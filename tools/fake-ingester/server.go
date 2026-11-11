// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

const (
	queryStreamSeriesBatch = 128     // series per message, as the real ingester sends
	maxMessageBytes        = 1 << 20 // chunk batches are flushed before passing this, as the real ingester does
)

// shared is the state every fake in a process uses.
type shared struct {
	cfg     *configHolder
	topo    Topology
	router  *compartments.Router
	chunks  *chunkCache
	resps   *responseCache
	metrics *serverMetrics
	now     func() time.Time
}

// fake is one fake ingester: one instance ID, one partition, one gRPC server.
type fake struct {
	client.UnimplementedIngesterServer
	grpc_health_v1.UnimplementedHealthServer

	id          string
	zone        string
	compartment int
	partition   int
	addr        string

	inflight atomic.Int64
	s        *shared
}

type serverMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	series   *prometheus.CounterVec
	inflight prometheus.Gauge

	cacheRequests *prometheus.CounterVec
	cacheBytes    prometheus.Gauge
	cacheEntries  prometheus.Gauge
}

func newServerMetrics(reg prometheus.Registerer) *serverMetrics {
	m := &serverMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fake_ingester_requests_total", Help: "Requests served, by RPC and status code."}, []string{"rpc", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "fake_ingester_request_duration_seconds", Help: "Time spent serving requests, including injected delay.",
			NativeHistogramBucketFactor: 1.1, Buckets: prometheus.ExponentialBuckets(0.0001, 4, 10),
		}, []string{"rpc"}),
		series:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fake_ingester_series_returned_total", Help: "Series returned, by RPC."}, []string{"rpc"}),
		inflight: prometheus.NewGauge(prometheus.GaugeOpts{Name: "fake_ingester_inflight_requests", Help: "Requests in flight across all fakes in this process."}),

		cacheRequests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "fake_ingester_response_cache_requests_total", Help: "QueryStream response cache lookups, by result (hit or miss)."}, []string{"result"}),
		cacheBytes:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "fake_ingester_response_cache_bytes", Help: "Bytes held by the QueryStream response cache."}),
		cacheEntries:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "fake_ingester_response_cache_entries", Help: "Entries in the QueryStream response cache."}),
	}
	reg.MustRegister(m.requests, m.duration, m.series, m.inflight, m.cacheRequests, m.cacheBytes, m.cacheEntries)
	return m
}

// begin tracks a request and returns the in-flight count it sees and a func to call when done.
func (f *fake) begin(rpc string) (int, func(error)) {
	n := int(f.inflight.Add(1))
	f.s.metrics.inflight.Inc()
	start := time.Now()
	return n, func(err error) {
		f.inflight.Add(-1)
		f.s.metrics.inflight.Dec()
		f.s.metrics.duration.WithLabelValues(rpc).Observe(time.Since(start).Seconds())
		f.s.metrics.requests.WithLabelValues(rpc, status.Code(err).String()).Inc()
	}
}

func tenantOf(ctx context.Context) (string, error) {
	id, err := user.ExtractOrgID(ctx)
	if err != nil {
		return "", status.Error(codes.Unauthenticated, err.Error())
	}
	return id, nil
}

// --- QueryStream ---

// preEncoded is a QueryStreamResponse whose wire bytes were produced once and are replayed on
// every send. Mimir's gRPC codec calls Size and MarshalWithSize, which return the cached bytes.
type preEncoded struct {
	client.QueryStreamResponse
	b []byte
}

func (p *preEncoded) Size() int                           { return len(p.b) }
func (p *preEncoded) MarshalWithSize(int) ([]byte, error) { return p.b, nil }
func (p *preEncoded) Marshal() ([]byte, error)            { return p.b, nil }

func encode(m *client.QueryStreamResponse) *preEncoded {
	b, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	return &preEncoded{b: b}
}

type cachedStream struct {
	seriesMsgs []*preEncoded // the last one carries IsEndOfSeriesStream
	chunkMsgs  []*preEncoded
	series     int
	chunks     int
	bytes      int
}

// responseCache keeps built responses. It's cleared wholesale when it passes its byte budget,
// which is cheap and rare once the working set of selectors is warm.
type responseCache struct {
	mtx     sync.Mutex
	m       map[string]*cachedStream
	bytes   int
	budget  int
	metrics *serverMetrics
}

func newResponseCache(budget int, metrics *serverMetrics) *responseCache {
	return &responseCache{m: map[string]*cachedStream{}, budget: budget, metrics: metrics}
}

func (c *responseCache) getOrBuild(key string, build func() *cachedStream) *cachedStream {
	c.mtx.Lock()
	if v, ok := c.m[key]; ok {
		c.mtx.Unlock()
		c.metrics.cacheRequests.WithLabelValues("hit").Inc()
		return v
	}
	c.mtx.Unlock()
	c.metrics.cacheRequests.WithLabelValues("miss").Inc()
	v := build()
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.bytes+v.bytes > c.budget {
		c.m = map[string]*cachedStream{}
		c.bytes = 0
	}
	c.m[key] = v
	c.bytes += v.bytes
	c.metrics.cacheBytes.Set(float64(c.bytes))
	c.metrics.cacheEntries.Set(float64(len(c.m)))
	return v
}

func (f *fake) QueryStream(req *client.QueryRequest, stream client.Ingester_QueryStreamServer) (err error) {
	ctx := stream.Context()
	inflight, done := f.begin("QueryStream")
	defer func() { done(err) }()
	st := f.s.cfg.get()
	if err := injectedError(st, "QueryStream", f.id, inflight); err != nil {
		return err
	}
	tenant, err := tenantOf(ctx)
	if err != nil {
		return err
	}
	from, to, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	now := f.s.now()
	sel := parseSelector(matchers)
	reached := f.s.topo.compartmentsFor(f.s.router, tenant, sel)
	n := seriesOnPartition(st, f.s.topo, tenant, sel, reached, f.compartment, f.partition)
	chunks := f.s.chunks.chunksFor(st, from.Time(), to.Time(), now)
	if len(chunks) == 0 {
		// Real ingesters only return series with chunks in range, and queriers panic on a series without any.
		n = 0
	}
	batch := int(req.StreamingChunksBatchSize)
	if batch <= 0 {
		batch = 256
	}

	var firstChunk, lastChunk int64
	if len(chunks) > 0 {
		firstChunk, lastChunk = chunks[0].StartTimestampMs, chunks[len(chunks)-1].StartTimestampMs
	}
	key := fmt.Sprintf("%s|%s|%d|%d|%d|%d|%d|%d|%d|%d|%d", tenant, sel.key, sel.kind, f.compartment, f.partition, n, firstChunk, lastChunk, batch, sel.shards, sel.shardIndex) + sel.ampSuffixStr
	cs := f.s.resps.getOrBuild(key, func() *cachedStream {
		return buildStream(f.s.router, tenant, sel, f.compartment, f.partition, n, chunks, batch)
	})
	f.s.metrics.series.WithLabelValues("QueryStream").Add(float64(cs.series))

	total := delayFor(st, "QueryStream", sel.key+strconv.FormatInt(req.StartTimestampMs, 10)+strconv.FormatInt(req.EndTimestampMs, 10), f.id, cs.series, cs.chunks, now, inflight)
	lc := st.cfg.Latency
	first := time.Duration(float64(total) * lc.FirstBatchShare)
	seriesRest := time.Duration(float64(total) * lc.SeriesShare)
	chunkRest := total - first - seriesRest

	if err := sleepCtx(ctx, first); err != nil {
		return status.FromContextError(err).Err()
	}
	for i, m := range cs.seriesMsgs {
		if i > 0 && len(cs.seriesMsgs) > 1 {
			if err := sleepCtx(ctx, seriesRest/time.Duration(len(cs.seriesMsgs)-1)); err != nil {
				return status.FromContextError(err).Err()
			}
		}
		if err := stream.SendMsg(m); err != nil {
			return err
		}
	}
	for _, m := range cs.chunkMsgs {
		if err := sleepCtx(ctx, chunkRest/time.Duration(len(cs.chunkMsgs))); err != nil {
			return status.FromContextError(err).Err()
		}
		if err := stream.SendMsg(m); err != nil {
			return err
		}
	}
	return nil
}

// buildStream produces the two phases of the streaming protocol: series in batches of 128, the
// last flagged IsEndOfSeriesStream (sent even when empty), then one chunks entry per series in
// order, flushed every batch series or before a message passes 1 MiB.
func buildStream(router *compartments.Router, tenant string, sel selector, compartment, partition, n int, chunks []client.Chunk, batch int) *cachedStream {
	cs := &cachedStream{series: n, chunks: n * len(chunks)}
	name := metricNameFor(router, tenant, sel, compartment)

	all := make([][]mimirpb.LabelAdapter, n)
	for i := range all {
		all[i] = seriesLabels(sel, name, compartment, partition, i)
	}
	sortSeries(all)

	var cur []client.QueryStreamSeries
	for i := 0; i < n; i++ {
		cur = append(cur, client.QueryStreamSeries{Labels: all[i], ChunkCount: int64(len(chunks))})
		if len(cur) == queryStreamSeriesBatch && i < n-1 {
			cs.seriesMsgs = append(cs.seriesMsgs, encode(&client.QueryStreamResponse{StreamingSeries: cur}))
			cur = nil
		}
	}
	cs.seriesMsgs = append(cs.seriesMsgs, encode(&client.QueryStreamResponse{StreamingSeries: cur, IsEndOfSeriesStream: true}))

	chunkBytes := 0
	for _, c := range chunks {
		chunkBytes += len(c.Data) + 32
	}
	var entries []client.QueryStreamSeriesChunks
	size := 0
	for i := 0; i < n; i++ {
		if len(entries) > 0 && (len(entries) >= batch || size+chunkBytes > maxMessageBytes) {
			cs.chunkMsgs = append(cs.chunkMsgs, encode(&client.QueryStreamResponse{StreamingSeriesChunks: entries}))
			entries, size = nil, 0
		}
		entries = append(entries, client.QueryStreamSeriesChunks{SeriesIndex: uint64(i), Chunks: chunks})
		size += chunkBytes
	}
	if len(entries) > 0 {
		cs.chunkMsgs = append(cs.chunkMsgs, encode(&client.QueryStreamResponse{StreamingSeriesChunks: entries}))
	}
	for _, m := range cs.seriesMsgs {
		cs.bytes += len(m.b)
	}
	for _, m := range cs.chunkMsgs {
		cs.bytes += len(m.b)
	}
	return cs
}

// --- Series and label RPCs ---

// seriesFor returns the series a selector yields on this partition, for the series-returning
// unary and streaming RPCs.
func (f *fake) seriesFor(st *runtimeState, tenant string, matchers []*labels.Matcher, limit int) [][]mimirpb.LabelAdapter {
	sel := parseSelector(matchers)
	reached := f.s.topo.compartmentsFor(f.s.router, tenant, sel)
	n := seriesOnPartition(st, f.s.topo, tenant, sel, reached, f.compartment, f.partition)
	if limit > 0 && n > limit {
		n = limit
	}
	name := metricNameFor(f.s.router, tenant, sel, f.compartment)
	out := make([][]mimirpb.LabelAdapter, n)
	for i := range out {
		out[i] = seriesLabels(sel, name, f.compartment, f.partition, i)
	}
	sortSeries(out)
	return out
}

// unary runs the common wrapper for unary RPCs: tracking, error injection, tenant, then the
// handler, then the delay.
func (f *fake) unary(ctx context.Context, rpc string, h func(st *runtimeState, tenant string) (series int, err error)) (err error) {
	inflight, done := f.begin(rpc)
	defer func() { done(err) }()
	st := f.s.cfg.get()
	if err := injectedError(st, rpc, f.id, inflight); err != nil {
		return err
	}
	tenant, err := tenantOf(ctx)
	if err != nil {
		return err
	}
	n, err := h(st, tenant)
	if err != nil {
		return err
	}
	f.s.metrics.series.WithLabelValues(rpc).Add(float64(n))
	if err := sleepCtx(ctx, delayFor(st, rpc, rpc+tenant, f.id, n, 0, f.s.now(), inflight)); err != nil {
		return status.FromContextError(err).Err()
	}
	return nil
}

func (f *fake) MetricsForLabelMatchers(ctx context.Context, req *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	resp := &client.MetricsForLabelMatchersResponse{}
	err := f.unary(ctx, "MetricsForLabelMatchers", func(st *runtimeState, tenant string) (int, error) {
		_, sets, err := client.FromMetricsForLabelMatchersRequest(req)
		if err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		for _, ms := range sets {
			for _, ls := range f.seriesFor(st, tenant, ms, int(req.Limit)) {
				resp.Metric = append(resp.Metric, &mimirpb.Metric{Labels: ls})
			}
		}
		return len(resp.Metric), nil
	})
	return resp, err
}

func (f *fake) LabelNames(ctx context.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	resp := &client.LabelNamesResponse{}
	err := f.unary(ctx, "LabelNames", func(st *runtimeState, _ string) (int, error) {
		names := []string{labels.MetricName}
		for _, l := range st.cfg.Labels {
			names = append(names, l.Name)
		}
		sort.Strings(names)
		if req.Limit > 0 && int64(len(names)) > req.Limit {
			names = names[:req.Limit]
		}
		resp.LabelNames = names
		return 0, nil
	})
	return resp, err
}

func (f *fake) labelValues(st *runtimeState, tenant, name string, limit int) []string {
	if name == labels.MetricName {
		vs := f.s.metricNames(st, tenant, f.compartment)
		if limit > 0 && len(vs) > limit {
			vs = vs[:limit]
		}
		return vs
	}
	for _, l := range st.cfg.Labels {
		if l.Name == name {
			return labelValuesFor(l, f.compartment, f.partition, f.s.topo, limit)
		}
	}
	return nil
}

func (f *fake) LabelValues(ctx context.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	resp := &client.LabelValuesResponse{}
	err := f.unary(ctx, "LabelValues", func(st *runtimeState, tenant string) (int, error) {
		resp.LabelValues = f.labelValues(st, tenant, req.LabelName, int(req.Limit))
		return 0, nil
	})
	return resp, err
}

func (f *fake) LabelNamesAndValues(req *client.LabelNamesAndValuesRequest, stream client.Ingester_LabelNamesAndValuesServer) error {
	var items []*client.LabelValues
	err := f.unary(stream.Context(), "LabelNamesAndValues", func(st *runtimeState, tenant string) (int, error) {
		items = append(items, &client.LabelValues{LabelName: labels.MetricName, Values: f.labelValues(st, tenant, labels.MetricName, 0)})
		for _, l := range st.cfg.Labels {
			items = append(items, &client.LabelValues{LabelName: l.Name, Values: f.labelValues(st, tenant, l.Name, 0)})
		}
		return 0, nil
	})
	if err != nil {
		return err
	}
	return sendInBatches(len(items), 64, func(lo, hi int) error {
		return stream.Send(&client.LabelNamesAndValuesResponse{Items: items[lo:hi]})
	})
}

func (f *fake) LabelValuesCardinality(req *client.LabelValuesCardinalityRequest, stream client.Ingester_LabelValuesCardinalityServer) error {
	var items []*client.LabelValueSeriesCount
	err := f.unary(stream.Context(), "LabelValuesCardinality", func(st *runtimeState, tenant string) (int, error) {
		ms, err := client.FromLabelMatchers(req.Matchers)
		if err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		series := len(f.seriesFor(st, tenant, ms, 0))
		for _, name := range req.LabelNames {
			vs := f.labelValues(st, tenant, name, 0)
			counts := make(map[string]uint64, len(vs))
			for i, v := range vs {
				// Spread the selector's series over the values, at least one each.
				c := uint64(series / max(1, len(vs)))
				if i < series%max(1, len(vs)) {
					c++
				}
				counts[v] = max(c, 1)
			}
			items = append(items, &client.LabelValueSeriesCount{LabelName: name, LabelValueSeries: counts})
		}
		return series, nil
	})
	if err != nil {
		return err
	}
	return sendInBatches(len(items), 16, func(lo, hi int) error {
		return stream.Send(&client.LabelValuesCardinalityResponse{Items: items[lo:hi]})
	})
}

func (f *fake) ActiveSeries(req *client.ActiveSeriesRequest, stream client.Ingester_ActiveSeriesServer) error {
	var series [][]mimirpb.LabelAdapter
	err := f.unary(stream.Context(), "ActiveSeries", func(st *runtimeState, tenant string) (int, error) {
		ms, err := client.FromLabelMatchers(req.Matchers)
		if err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		series = f.seriesFor(st, tenant, ms, 0)
		return len(series), nil
	})
	if err != nil {
		return err
	}
	return sendInBatches(len(series), 1024, func(lo, hi int) error {
		resp := &client.ActiveSeriesResponse{}
		for _, ls := range series[lo:hi] {
			resp.Metric = append(resp.Metric, &mimirpb.Metric{Labels: ls})
			if req.Type == client.NATIVE_HISTOGRAM_SERIES {
				resp.BucketCount = append(resp.BucketCount, 20)
			}
		}
		return stream.Send(resp)
	})
}

func (f *fake) MetricsMetadata(ctx context.Context, req *client.MetricsMetadataRequest) (*client.MetricsMetadataResponse, error) {
	resp := &client.MetricsMetadataResponse{}
	err := f.unary(ctx, "MetricsMetadata", func(st *runtimeState, tenant string) (int, error) {
		names := f.s.metricNames(st, tenant, f.compartment)
		if req.Metric != "" {
			names = []string{req.Metric}
		}
		if req.Limit > 0 && len(names) > int(req.Limit) {
			names = names[:req.Limit]
		}
		for _, n := range names {
			resp.Metadata = append(resp.Metadata, &mimirpb.MetricMetadata{Type: mimirpb.COUNTER, MetricFamilyName: n, Help: "Synthetic metric from fake-ingester."})
		}
		return 0, nil
	})
	return resp, err
}

func (f *fake) UserStats(ctx context.Context, _ *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	resp := &client.UserStatsResponse{}
	err := f.unary(ctx, "UserStats", func(*runtimeState, string) (int, error) {
		resp.NumSeries = 2900000
		resp.IngestionRate = 200000
		resp.ApiIngestionRate = 200000
		return 0, nil
	})
	return resp, err
}

func (f *fake) AllUserStats(ctx context.Context, _ *client.UserStatsRequest) (*client.UsersStatsResponse, error) {
	resp := &client.UsersStatsResponse{}
	err := f.unary(ctx, "AllUserStats", func(*runtimeState, string) (int, error) { return 0, nil })
	return resp, err
}

func (f *fake) QueryExemplars(ctx context.Context, _ *client.ExemplarQueryRequest) (*client.ExemplarQueryResponse, error) {
	resp := &client.ExemplarQueryResponse{}
	err := f.unary(ctx, "QueryExemplars", func(*runtimeState, string) (int, error) { return 0, nil })
	return resp, err
}

func (f *fake) SearchLabelNames(req *client.SearchLabelNamesRequest, stream client.Ingester_SearchLabelNamesServer) error {
	var names []string
	err := f.unary(stream.Context(), "SearchLabelNames", func(st *runtimeState, _ string) (int, error) {
		names = append(names, labels.MetricName)
		for _, l := range st.cfg.Labels {
			names = append(names, l.Name)
		}
		if req.Limit > 0 && int64(len(names)) > req.Limit {
			names = names[:req.Limit]
		}
		return 0, nil
	})
	if err != nil {
		return err
	}
	return sendSearch(stream.Send, names)
}

func (f *fake) SearchLabelValues(req *client.SearchLabelValuesRequest, stream client.Ingester_SearchLabelValuesServer) error {
	var values []string
	err := f.unary(stream.Context(), "SearchLabelValues", func(st *runtimeState, tenant string) (int, error) {
		values = f.labelValues(st, tenant, req.Name, int(req.Limit))
		return 0, nil
	})
	if err != nil {
		return err
	}
	return sendSearch(stream.Send, values)
}

func sendSearch(send func(*client.SearchResultBatch) error, vals []string) error {
	return sendInBatches(len(vals), 1024, func(lo, hi int) error {
		b := &client.SearchResultBatch{}
		for _, v := range vals[lo:hi] {
			b.Results = append(b.Results, client.SearchResultBatch_Result{Value: v, Score: 1})
		}
		return send(b)
	})
}

func sendInBatches(n, size int, send func(lo, hi int) error) error {
	for lo := 0; lo < n; lo += size {
		if err := send(lo, min(n, lo+size)); err != nil {
			return err
		}
	}
	return nil
}

// --- Health ---

func (f *fake) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if isUnhealthy(f.s.cfg.get(), f.id) {
		return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// metricNames caches the synthetic metric names per (tenant, compartment).
func (s *shared) metricNames(st *runtimeState, tenant string, compartment int) []string {
	key := tenant + "|" + strconv.Itoa(compartment) + "|" + strconv.Itoa(st.cfg.Sizes.NumMetrics)
	if v, ok := metricNamesCache.Load(key); ok {
		return v.([]string)
	}
	v := metricNamesOn(s.router, tenant, st.cfg.Sizes.NumMetrics, compartment)
	metricNamesCache.Store(key, v)
	return v
}

var metricNamesCache sync.Map
