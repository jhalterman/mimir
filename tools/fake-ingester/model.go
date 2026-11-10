// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
)

// Topology is what a fake knows about the cell it pretends to be part of.
type Topology struct {
	NumCompartments          int
	PartitionsPerCompartment int
}

// copyKind is how read-tee produced a request, recognized from its matchers.
type copyKind int

const (
	copyOriginal   copyKind = iota // original read, scoped with __amp__=""
	copyPerReplica                 // label values suffixed _amp{k}: one replica's data
	copyHeavy                      // matchers ending (?:_amp[0-9]+)?: every replica's data
)

var (
	ampSuffix    = regexp.MustCompile(`_amp[0-9]+$`)
	heavySuffix  = "(?:_amp[0-9]+)?"
	ampLabelName = "__amp__"
)

// selector is a request's matchers, reduced to what sizing needs.
type selector struct {
	key          string   // normalized text: _amp suffixes stripped, read-tee label dropped, sorted
	metricNames  []string // exact names the selector is limited to, if any
	kind         copyKind
	ampSuffixStr string // the _amp{k} suffix of a per-replica copy
	shards       int    // query-frontend shard count from __query_shard__="i_of_n"; 1 if unsharded
	shardIndex   int    // this request's shard, 0-based

	// Labels the selector pins, so returned series match it the way real ones would: fixed from
	// equality matchers, and choices from regex alternations (each series takes one value).
	fixed   []mimirpb.LabelAdapter
	choices []labelChoice
}

type labelChoice struct {
	name   string
	values []string
}

// queryShardLabel is the matcher the query-frontend adds to each shard of a sharded query.
const queryShardLabel = "__query_shard__"

func parseSelector(ms []*labels.Matcher) selector {
	s := selector{shards: 1}
	parts := make([]string, 0, len(ms))
	for _, m := range ms {
		if m.Name == ampLabelName {
			continue
		}
		if m.Name == queryShardLabel {
			var i, n int
			if _, err := fmt.Sscanf(m.Value, "%d_of_%d", &i, &n); err == nil && n > 0 && i >= 1 && i <= n {
				s.shards = n
				s.shardIndex = i - 1
			}
			continue
		}
		v := m.Value
		if strings.HasSuffix(v, heavySuffix) {
			s.kind = copyHeavy
			v = strings.TrimSuffix(v, heavySuffix)
		} else if loc := ampSuffix.FindStringIndex(v); loc != nil && (m.Type == labels.MatchEqual || m.Type == labels.MatchNotEqual) {
			if s.kind != copyHeavy {
				s.kind = copyPerReplica
			}
			s.ampSuffixStr = v[loc[0]:]
			v = v[:loc[0]]
		}
		parts = append(parts, m.Name+m.Type.String()+strconv.Quote(v))
		if m.Name != labels.MetricName {
			switch m.Type {
			case labels.MatchEqual:
				// Keep a per-replica copy's suffix: that replica's series carry it.
				if m.Value != "" {
					s.fixed = append(s.fixed, mimirpb.LabelAdapter{Name: strings.Clone(m.Name), Value: strings.Clone(m.Value)})
				}
			case labels.MatchRegexp:
				if rm, err := labels.NewMatcher(labels.MatchRegexp, m.Name, v); err == nil {
					var values []string
					for _, x := range rm.SetMatches() {
						if x != "" {
							values = append(values, strings.Clone(x))
						}
					}
					if len(values) > 0 {
						sort.Strings(values)
						s.choices = append(s.choices, labelChoice{name: strings.Clone(m.Name), values: values})
					}
				}
			}
		}
		if m.Name == labels.MetricName {
			switch m.Type {
			case labels.MatchEqual:
				s.metricNames = []string{v}
			case labels.MatchRegexp:
				if s.metricNames == nil {
					if rm, err := labels.NewMatcher(labels.MatchRegexp, m.Name, v); err == nil {
						if set := rm.SetMatches(); len(set) > 0 {
							s.metricNames = set
						}
					}
				}
			}
		}
	}
	sort.Strings(parts)
	s.key = "{" + strings.Join(parts, ",") + "}"
	return s
}

// compartmentsFor returns the compartments a selector reaches, matching the querier's routing.
func (t Topology) compartmentsFor(router *compartments.Router, tenant string, s selector) []int {
	if len(s.metricNames) == 0 {
		all := make([]int, t.NumCompartments)
		for i := range all {
			all[i] = i
		}
		return all
	}
	seen := map[int]bool{}
	var out []int
	for _, n := range s.metricNames {
		c := router.CompartmentForMetric(tenant, n)
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}
	sort.Ints(out)
	return out
}

// hashUnit maps strings to a deterministic value in [0, 1).
func hashUnit(parts ...string) float64 {
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}
	return float64(h.Sum64()>>11) / float64(1<<53)
}

// interpolate reads a quantile table at q, linearly in log space between positive points.
func interpolate(points []QuantilePoint, q float64) float64 {
	if len(points) == 0 {
		return 0
	}
	if q <= points[0].Q {
		return points[0].Value
	}
	for i := 1; i < len(points); i++ {
		a, b := points[i-1], points[i]
		if q > b.Q {
			continue
		}
		f := (q - a.Q) / (b.Q - a.Q)
		if a.Value <= 0 || b.Value <= 0 {
			return a.Value + f*(b.Value-a.Value)
		}
		return math.Exp(math.Log(a.Value) + f*(math.Log(b.Value)-math.Log(a.Value)))
	}
	return points[len(points)-1].Value
}

// seriesOnPartition is how many series one partition returns for a selector: the selector's
// cell-wide size, scaled by the copy kind, split evenly over the partitions it reaches.
func seriesOnPartition(st *runtimeState, topo Topology, tenant string, s selector, reached []int, compartment, partition int) int {
	in := false
	for _, c := range reached {
		if c == compartment {
			in = true
			break
		}
	}
	if !in {
		return 0
	}
	total, ok := st.table[s.key]
	if !ok {
		total = interpolate(st.cfg.Sizes.SeriesQuantiles, hashUnit(tenant, s.key))
	}
	if s.kind == copyHeavy && st.cfg.Sizes.WriteAmplification > 1 {
		total *= float64(st.cfg.Sizes.WriteAmplification)
	}
	if s.shards > 1 {
		total /= float64(s.shards)
	}
	parts := len(reached) * topo.PartitionsPerCompartment
	if parts <= 0 {
		return 0
	}
	base := int(total) / parts
	rem := int(total) % parts
	// Hand the remainder to a deterministic subset of partitions.
	if rem > 0 && hashUnit(s.key, strconv.Itoa(compartment), strconv.Itoa(partition)) < float64(rem)/float64(parts) {
		base++
	}
	if max := st.cfg.Sizes.MaxSeriesPerResponse; max > 0 && base > max {
		base = max
	}
	return base
}

// uniqueLabel carries every series' identity, so series stay distinct when a query ignores
// instance and job, or when the selector pins instance.
const uniqueLabel = "fake_series"

// seriesLabels builds a partition's i-th series for a selector, carrying the labels the selector
// pins. Series are disjoint across partitions and across query shards (the partition and the
// series' index in the unsharded selector are in the instance and fake_series labels), and identical
// between zones.
// Callers sort them, since a pinned label can vary from series to series.
func seriesLabels(s selector, metricName string, compartment, partition, i int) []mimirpb.LabelAdapter {
	g := i*s.shards + s.shardIndex
	id := fmt.Sprintf("c%03d-p%05d-%08d", compartment, partition, g)
	ls := map[string]string{
		labels.MetricName: metricName + s.ampSuffixStr,
		"instance":        id,
		"job":             "fake-job-" + strconv.Itoa(int(hashUnit(s.key, strconv.Itoa(g%997))*200)),
	}
	for _, c := range s.choices {
		ls[c.name] = c.values[g%len(c.values)]
	}
	for _, l := range s.fixed {
		ls[l.Name] = l.Value
	}
	ls[uniqueLabel] = id
	out := make([]mimirpb.LabelAdapter, 0, len(ls))
	for n, v := range ls {
		out = append(out, mimirpb.LabelAdapter{Name: n, Value: v})
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out
}

// sortSeries orders series the way the querier's stream merge requires.
func sortSeries(series [][]mimirpb.LabelAdapter) {
	sort.Slice(series, func(a, b int) bool { return mimirpb.CompareLabelAdapters(series[a], series[b]) < 0 })
}

// metricNameFor picks the metric a selector's series carry on a compartment: one of the selector's
// exact names that routes there, or a synthetic name for selectors without one.
func metricNameFor(router *compartments.Router, tenant string, s selector, compartment int) string {
	for _, n := range s.metricNames {
		if router.CompartmentForMetric(tenant, n) == compartment {
			return n
		}
	}
	return "fake_metric_" + strconv.Itoa(int(hashUnit(s.key)*1000))
}

// chunkCache holds encoded XOR chunks by aligned start time. Every series shares the same chunk
// bytes, so building them is a one-off cost.
type chunkCache struct {
	mtx    sync.Mutex
	chunks map[int64]client.Chunk
}

func newChunkCache() *chunkCache { return &chunkCache{chunks: map[int64]client.Chunk{}} }

// chunksFor returns the chunks covering [from, to], clamped to what an ingester would hold.
func (c *chunkCache) chunksFor(st *runtimeState, from, to time.Time, now time.Time) []client.Chunk {
	sc := st.cfg.Sizes
	if sc.ScrapeInterval <= 0 || sc.SamplesPerChunk <= 0 {
		return nil
	}
	if min := now.Add(-sc.QueryIngestersWithin); from.Before(min) {
		from = min
	}
	if to.After(now) {
		to = now
	}
	if !from.Before(to) {
		return nil
	}
	width := sc.ScrapeInterval * time.Duration(sc.SamplesPerChunk)
	start := from.Truncate(width)
	var out []client.Chunk
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for t := start; t.Before(to); t = t.Add(width) {
		ms := t.UnixMilli()
		ch, ok := c.chunks[ms]
		if !ok {
			ch = buildChunk(ms, sc.ScrapeInterval, sc.SamplesPerChunk)
			c.chunks[ms] = ch
		}
		out = append(out, ch)
	}
	// Drop chunks that fell out of the window.
	if len(c.chunks) > 4*int(sc.QueryIngestersWithin/width)+16 {
		cut := now.Add(-sc.QueryIngestersWithin - width).UnixMilli()
		for k := range c.chunks {
			if k < cut {
				delete(c.chunks, k)
			}
		}
	}
	return out
}

func buildChunk(startMs int64, interval time.Duration, n int) client.Chunk {
	xc := chunkenc.NewXORChunk()
	app, err := xc.Appender()
	if err != nil {
		panic(err)
	}
	step := interval.Milliseconds()
	for i := 0; i < n; i++ {
		app.Append(0, startMs+int64(i)*step, float64(i%60))
	}
	return client.Chunk{
		StartTimestampMs: startMs,
		EndTimestampMs:   startMs + int64(n-1)*step,
		Encoding:         int32(chunk.PrometheusXorChunk),
		Data:             xc.Bytes(),
	}
}

// labelValuesFor returns a label's values on one partition, following its LabelSpec.
func labelValuesFor(spec LabelSpec, compartment, partition int, topo Topology, limit int) []string {
	total := spec.Values
	var idx []int
	if spec.Shared {
		n := total
		if limit > 0 && n > limit {
			n = limit
		}
		idx = make([]int, n)
		for i := range idx {
			idx[i] = i
		}
	} else {
		parts := topo.NumCompartments * topo.PartitionsPerCompartment
		if parts <= 0 {
			parts = 1
		}
		self := compartment*topo.PartitionsPerCompartment + partition
		for i := self % parts; i < total; i += parts {
			idx = append(idx, i)
			if limit > 0 && len(idx) >= limit {
				break
			}
		}
	}
	out := make([]string, len(idx))
	for i, v := range idx {
		out[i] = fmt.Sprintf("%s-%08d", spec.Name, v)
	}
	sort.Strings(out)
	return out
}

// metricNamesOn returns the synthetic metric names that route to a compartment.
func metricNamesOn(router *compartments.Router, tenant string, n, compartment int) []string {
	var out []string
	for i := 0; i < n; i++ {
		name := "fake_metric_" + strconv.Itoa(i)
		if router.CompartmentForMetric(tenant, name) == compartment {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
