// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

const testTenant = "9960"

func newTestShared(t *testing.T, rc RuntimeConfig, topo Topology) *shared {
	h := &configHolder{}
	require.NoError(t, h.set(rc))
	metrics := newServerMetrics(prometheus.NewRegistry())
	return &shared{
		cfg:     h,
		topo:    topo,
		router:  compartments.NewRouter(topo.NumCompartments),
		chunks:  newChunkCache(),
		resps:   newResponseCache(64<<20, metrics),
		metrics: metrics,
		now:     time.Now,
	}
}

// noDelay keeps tests fast: zero latency, no slow instances, no errors.
func noDelay() RuntimeConfig {
	rc := DefaultRuntimeConfig()
	rc.Latency.Base = map[string][]QuantilePoint{"default": {{Q: 0, Value: 0}, {Q: 1, Value: 0}}}
	rc.Latency.PerSeries, rc.Latency.PerChunk = 0, 0
	rc.Latency.SlowInstanceFraction, rc.Latency.SlowWindowFactor, rc.Latency.InflightKnee = 0, 0, 0
	return rc
}

// startFake serves one fake on a loopback port and returns a real ingester gRPC client for it.
func startFake(t *testing.T, s *shared, zone string, compartment, partition int) (*fake, client.IngesterClient) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &fake{id: "f-" + zone, zone: zone, compartment: compartment, partition: partition, addr: lis.Addr().String(), s: s}
	srv := newGRPCServer(1000)
	client.RegisterIngesterServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(middleware.ClientUserHeaderInterceptor),
		grpc.WithStreamInterceptor(middleware.StreamClientUserHeaderInterceptor))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return f, client.NewIngesterClient(conn)
}

type readSeries struct {
	labels  labels.Labels
	samples int
}

// queryStream consumes a QueryStream the way the distributor does: series until
// IsEndOfSeriesStream, then each series' chunks in order through SeriesChunksStreamReader.
func queryStream(t *testing.T, c client.IngesterClient, from, to time.Time, ms ...*labels.Matcher) []readSeries {
	ctx := user.InjectOrgID(t.Context(), testTenant)
	req, err := client.ToQueryRequest(0, 0, ms)
	require.NoError(t, err)
	req.StartTimestampMs, req.EndTimestampMs = from.UnixMilli(), to.UnixMilli()
	req.StreamingChunksBatchSize = 256
	stream, err := c.QueryStream(ctx, req)
	require.NoError(t, err)

	var out []readSeries
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			require.Empty(t, out, "EOF before end of series stream is only valid for empty results")
			return nil
		}
		require.NoError(t, err)
		for _, s := range resp.StreamingSeries {
			out = append(out, readSeries{labels: mimirpb.FromLabelAdaptersToLabelsWithCopy(s.Labels)})
		}
		if resp.IsEndOfSeriesStream {
			break
		}
	}
	if len(out) == 0 {
		return nil
	}
	tracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	reader := client.NewSeriesChunksStreamReader(ctx, stream, "fake", len(out), limiter.NewQueryLimiter(0, 0, 0, 0, nil), tracker, func() {}, log.NewNopLogger())
	reader.StartBuffering()
	defer reader.Close()
	for i := range out {
		chunks, err := reader.GetChunks(uint64(i))
		require.NoError(t, err)
		for _, ch := range chunks {
			xc, err := chunkenc.FromData(chunkenc.EncXOR, ch.Data)
			require.NoError(t, err)
			it := xc.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, _ := it.At()
				require.GreaterOrEqual(t, ts, ch.StartTimestampMs)
				require.LessOrEqual(t, ts, ch.EndTimestampMs)
				out[i].samples++
			}
			require.NoError(t, it.Err())
		}
	}
	return out
}

func TestQueryStream_ProtocolAndShape(t *testing.T) {
	rc := noDelay()
	// Every selector is 3,000 series cell-wide; 3 compartments × 2 partitions.
	rc.Sizes.SeriesQuantiles = []QuantilePoint{{Q: 0, Value: 3000}, {Q: 1, Value: 3000}}
	topo := Topology{NumCompartments: 3, PartitionsPerCompartment: 2}
	s := newTestShared(t, rc, topo)
	now := time.Now()

	fanout := labels.MustNewMatcher(labels.MatchRegexp, "job", ".+")

	t.Run("fan-out selector splits over every partition, zones agree, partitions are disjoint", func(t *testing.T) {
		_, a0 := startFake(t, s, "zone-a", 1, 0)
		_, b0 := startFake(t, s, "zone-b", 1, 0)
		_, a1 := startFake(t, s, "zone-a", 1, 1)

		sa0 := queryStream(t, a0, now.Add(-time.Hour), now, fanout)
		sb0 := queryStream(t, b0, now.Add(-time.Hour), now, fanout)
		sa1 := queryStream(t, a1, now.Add(-time.Hour), now, fanout)

		require.Len(t, sa0, 500) // 3,000 / 6 partitions
		windows := 0
		for w := now.Add(-time.Hour).Truncate(30 * time.Minute); w.Before(now); w = w.Add(30 * time.Minute) {
			windows++
		}
		require.Len(t, sa1, 500)
		require.True(t, sort.SliceIsSorted(sa0, func(i, j int) bool { return labels.Compare(sa0[i].labels, sa0[j].labels) < 0 }), "series must be sorted")
		for i := range sa0 {
			require.Equal(t, sa0[i].labels, sb0[i].labels, "zones must return identical series")
			require.Equal(t, 120*windows, sa0[i].samples, "whole 30m chunks of 120 samples overlapping the range")
		}
		seen := map[string]bool{}
		for _, x := range sa0 {
			seen[x.labels.String()] = true
		}
		for _, x := range sa1 {
			require.False(t, seen[x.labels.String()], "partitions must not share series")
		}
	})

	t.Run("exact metric name only returns data from its compartment", func(t *testing.T) {
		name := "some_metric"
		home := compartments.NewRouter(3).CompartmentForMetric(testTenant, name)
		_, onHome := startFake(t, s, "zone-a", home, 0)
		_, offHome := startFake(t, s, "zone-a", (home+1)%3, 0)
		m := labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, name)
		home0 := queryStream(t, onHome, now.Add(-10*time.Minute), now, m)
		require.Len(t, home0, 1500) // 3,000 / 2 partitions of one compartment
		require.Equal(t, name, home0[0].labels.Get(labels.MetricName))
		require.Empty(t, queryStream(t, offHome, now.Add(-10*time.Minute), now, m))
	})

	t.Run("time range beyond the head returns no chunks", func(t *testing.T) {
		_, c := startFake(t, s, "zone-a", 0, 0)
		got := queryStream(t, c, now.Add(-48*time.Hour), now.Add(-24*time.Hour), fanout)
		require.Len(t, got, 500)
		require.Zero(t, got[0].samples)
	})

	t.Run("read-tee copies", func(t *testing.T) {
		rc := rc
		rc.Sizes.WriteAmplification = 4
		s := newTestShared(t, rc, topo)
		_, c := startFake(t, s, "zone-a", 0, 0)
		perReplica := queryStream(t, c, now.Add(-time.Minute), now, labels.MustNewMatcher(labels.MatchEqual, "job", "api_amp3"))
		heavy := queryStream(t, c, now.Add(-time.Minute), now, labels.MustNewMatcher(labels.MatchRegexp, "job", "api(?:_amp[0-9]+)?"))
		original := queryStream(t, c, now.Add(-time.Minute), now, labels.MustNewMatcher(labels.MatchEqual, "job", "api"), labels.MustNewMatcher(labels.MatchEqual, "__amp__", ""))
		require.Len(t, perReplica, 500)
		require.Len(t, original, 500)
		require.Len(t, heavy, 2000)
		require.Contains(t, perReplica[0].labels.Get(labels.MetricName), "_amp3")
	})

	t.Run("query-frontend shards split a selector's series", func(t *testing.T) {
		_, c := startFake(t, s, "zone-a", 2, 1)
		require.Equal(t, parseSelector([]*labels.Matcher{fanout}).key, parseSelector([]*labels.Matcher{fanout, labels.MustNewMatcher(labels.MatchEqual, queryShardLabel, "3_of_4")}).key)
		// Shards are disjoint: concatenating them, as the query-frontend does, never repeats a labelset.
		seen := map[string]bool{}
		for i := 1; i <= 4; i++ {
			shard := queryStream(t, c, now.Add(-time.Minute), now, fanout, labels.MustNewMatcher(labels.MatchEqual, queryShardLabel, fmt.Sprintf("%d_of_4", i)))
			require.Len(t, shard, 125)
			for _, x := range shard {
				k := x.labels.String()
				require.False(t, seen[k], "shards must not share series: %s", k)
				seen[k] = true
			}
		}
		require.Len(t, seen, 500)
	})

	t.Run("series carry the labels the selector pins", func(t *testing.T) {
		_, c := startFake(t, s, "zone-a", 0, 1)
		got := queryStream(t, c, now.Add(-time.Minute), now,
			labels.MustNewMatcher(labels.MatchEqual, "cluster", "prod-1"),
			labels.MustNewMatcher(labels.MatchRegexp, "app", "web|api"),
			labels.MustNewMatcher(labels.MatchNotEqual, "namespace", "x"))
		require.Len(t, got, 500)
		require.True(t, sort.SliceIsSorted(got, func(i, j int) bool { return labels.Compare(got[i].labels, got[j].labels) < 0 }), "series must be sorted")
		apps := map[string]int{}
		for _, x := range got {
			require.Equal(t, "prod-1", x.labels.Get("cluster"))
			apps[x.labels.Get("app")]++
			require.False(t, x.labels.Has("namespace"))
		}
		require.Equal(t, map[string]int{"web": 250, "api": 250}, apps)

		// Series stay distinct when a join ignores instance and job, through fake_series.
		ignoring := map[string]bool{}
		for _, x := range got {
			ignoring[labels.NewBuilder(x.labels).Del(labels.MetricName, "instance", "job").Labels().String()] = true
		}
		require.Len(t, ignoring, len(got))

		// Pinning instance keeps series unique through fake_series.
		pinned := queryStream(t, c, now.Add(-time.Minute), now, labels.MustNewMatcher(labels.MatchEqual, "instance", "host-1"))
		seen := map[string]bool{}
		for _, x := range pinned {
			require.Equal(t, "host-1", x.labels.Get("instance"))
			seen[x.labels.String()] = true
		}
		require.Len(t, seen, len(pinned))
	})
}

func TestQueryStream_LargeResponseFlushesBelow1MiB(t *testing.T) {
	rc := noDelay()
	rc.Sizes.SeriesQuantiles = []QuantilePoint{{Q: 0, Value: 5000}, {Q: 1, Value: 5000}}
	s := newTestShared(t, rc, Topology{NumCompartments: 1, PartitionsPerCompartment: 1})
	sel := parseSelector([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "job", ".+")})
	now := time.Now()
	chunks := s.chunks.chunksFor(s.cfg.get(), now.Add(-13*time.Hour), now, now)
	cs := buildStream(s.router, testTenant, sel, 0, 0, 5000, chunks, 256)
	require.Len(t, cs.seriesMsgs, int(math.Ceil(5000.0/128)))
	for _, m := range cs.chunkMsgs {
		require.LessOrEqual(t, len(m.b), 1<<20+64<<10)
	}

	_, c := startFake(t, s, "zone-a", 0, 0)
	got := queryStream(t, c, now.Add(-13*time.Hour), now, labels.MustNewMatcher(labels.MatchRegexp, "job", ".+"))
	require.Len(t, got, 5000)
}

func TestRingRegistration_QuerierSeesFakes(t *testing.T) {
	logger := log.NewNopLogger()
	instKV, closer1 := consul.NewInMemoryClient(ring.GetCodec(), logger, nil)
	t.Cleanup(func() { _ = closer1.Close() })
	partKV, closer2 := consul.NewInMemoryClient(ring.GetPartitionRingCodec(), logger, nil)
	t.Cleanup(func() { _ = closer2.Close() })

	s := newTestShared(t, noDelay(), Topology{NumCompartments: 2, PartitionsPerCompartment: 3})
	var fakes []*fake
	for _, zone := range []string{"zone-a", "zone-b"} {
		for c := 0; c < 2; c++ {
			for p := 0; p < 3; p++ {
				fakes = append(fakes, &fake{id: "fake-" + zone + "-rc-" + itoa(c) + "-" + itoa(p), zone: zone, compartment: c, partition: p, addr: "10.0.0.1:" + itoa(10000+p), s: s})
			}
		}
	}
	r := &registrar{instances: instKV, partitions: partKV, fakes: fakes, compartmentsOn: true, logger: logger}
	require.NoError(t, r.heartbeat(t.Context()))

	// Build the querier's view, as pkg/mimir/modules.go does.
	instRing, err := ring.NewWithStoreClientAndStrategy(ring.Config{HeartbeatTimeout: time.Minute, ReplicationFactor: 1}, "ingester", instanceRingKey, instKV, ring.NewDefaultReplicationStrategy(), nil, logger)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(t.Context(), instRing))
	t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), instRing) })

	readNoExtend := ring.NewOp([]ring.InstanceState{ring.ACTIVE, ring.PENDING}, nil)
	for c := 0; c < 2; c++ {
		w := ring.NewPartitionRingWatcher("ingester-partitions", partitionRingKey(true, c), partKV, logger, nil)
		require.NoError(t, services.StartAndAwaitRunning(t.Context(), w))
		t.Cleanup(func() { _ = services.StopAndAwaitTerminated(context.Background(), w) })
		pir := ring.NewPartitionInstanceRing(w, instRing, time.Minute)

		require.Eventually(t, func() bool {
			sets, err := pir.GetReplicationSetsForOperation(readNoExtend)
			if err != nil || len(sets) != 3 {
				return false
			}
			for _, set := range sets {
				if len(set.Instances) != 2 {
					return false
				}
			}
			return true
		}, 5*time.Second, 50*time.Millisecond, "compartment %d: want 3 partitions with 2 healthy owners each", c)
	}

	require.NoError(t, r.unregister(t.Context(), true))
}

func TestLatencyModel(t *testing.T) {
	rc := DefaultRuntimeConfig()
	rc.Latency.SlowInstanceFraction, rc.Latency.SlowWindowFactor, rc.Latency.PerSeries = 0, 0, 0
	rc.Latency.Correlation = 1
	h := &configHolder{}
	require.NoError(t, h.set(rc))
	st := h.get()
	now := time.Unix(1000, 0)

	// Fully correlated: every partition draws the same quantile for one request.
	d1 := delayFor(st, "QueryStream", "req-1", "a", 0, 0, now, 1)
	d2 := delayFor(st, "QueryStream", "req-1", "b", 0, 0, now, 1)
	require.Equal(t, d1, d2)

	// The median of many requests sits near the configured p50, and load past the knee inflates
	// the tail but not the median.
	sample := func(inflight int) (p50, p99 time.Duration) {
		var ds []time.Duration
		for i := 0; i < 20000; i++ {
			ds = append(ds, delayFor(st, "QueryStream", "req-"+itoa(i), "a", 0, 0, now, inflight))
		}
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[len(ds)/2], ds[len(ds)*99/100]
	}
	p50, p99 := sample(1)
	require.InDelta(t, 0.22, p50.Seconds()*1000, 0.05, "p50 ms")
	loadedP50, loadedP99 := sample(12)
	require.InDelta(t, p50.Seconds(), loadedP50.Seconds(), 0.002)
	require.Greater(t, loadedP99, p99)
}

func itoa(i int) string { return strconv.Itoa(i) }

func TestBuildSizeTable(t *testing.T) {
	in := `{"q":"sum(rate(http_requests_total{job=\"api\"}[5m]))","fs":100,"len":"","n":3}
{"q":"sum(rate(http_requests_total{job=\"api\"}[5m]))","fs":300,"len":"","n":1}
{"q":"a / b","fs":50,"len":"1h","n":1}
{"q":"up","fs":1000,"len":"72h","n":1}
{"q":"this is not promql(","fs":5,"len":"","n":1}
`
	table, st, err := buildSizeTable(strings.NewReader(in))
	require.NoError(t, err)
	require.Equal(t, 1, st.ParseErrors)

	key := func(ms ...*labels.Matcher) string { return parseSelector(ms).key }
	// Weighted median: three samples of 100 outweigh one of 300.
	require.Equal(t, 100.0, table[key(labels.MustNewMatcher(labels.MatchEqual, "job", "api"), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "http_requests_total"))])
	// Split evenly across selectors.
	require.Equal(t, 25.0, table[key(labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "a"))])
	// 72h spans three 24h splits; only the newest reaches ingesters.
	require.Equal(t, 333.0, table[key(labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, "up"))])
}
