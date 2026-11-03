// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// The fan-out benchmarks measure the distributor's read path (replication set lookup, per-ingester
// requests, stream handling and response merging) as the number of partitions a query reaches grows.
// Ingesters are replaced by fake clients that replay pre-built responses, so the measured cost is the
// querier side only. See fanout-load-test-plan.md (step 2).

// fanoutBenchPartitions returns the numbers of partitions a single query reaches. At 10B active series
// a targeted query reaches about 350 partitions (one compartment) and a fan-out query about 3,500.
func fanoutBenchPartitions() []int {
	return []int{10, 100, 350, 1000, 3500}
}

const (
	fanoutBenchTenant      = "user"
	fanoutBenchMetric      = "fanout_bench_metric"
	fanoutBenchBatchSize   = 128
	fanoutBenchChunkLength = 120
)

// fanoutBenchResponses holds the responses a fake ingester replays. Responses are shared by the fake
// ingesters of every zone owning the same partition, since they hold the same data.
type fanoutBenchResponses struct {
	queryStream            []*client.QueryStreamResponse
	activeSeries           []*client.ActiveSeriesResponse
	labelValues            *client.LabelValuesResponse
	labelNames             *client.LabelNamesResponse
	metricsForMatchers     *client.MetricsForLabelMatchersResponse
	labelNamesAndValues    []*client.LabelNamesAndValuesResponse
	labelValuesCardinality []*client.LabelValuesCardinalityResponse
	userStats              *client.UserStatsResponse
}

// fanoutBenchIngester is a fake ingester client replaying pre-built responses. Unimplemented RPCs
// panic through the nil embedded interfaces.
type fanoutBenchIngester struct {
	client.IngesterClient
	grpc_health_v1.HealthClient

	responses *fanoutBenchResponses
}

func (f *fanoutBenchIngester) Close() error { return nil }

// Check answers the client pool's periodic health checks, which long benchmark runs reach.
func (f *fanoutBenchIngester) Check(context.Context, *grpc_health_v1.HealthCheckRequest, ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (f *fanoutBenchIngester) QueryStream(ctx context.Context, _ *client.QueryRequest, _ ...grpc.CallOption) (client.Ingester_QueryStreamClient, error) {
	return &stream{ctx: ctx, results: f.responses.queryStream}, nil
}

func (f *fanoutBenchIngester) ActiveSeries(_ context.Context, _ *client.ActiveSeriesRequest, _ ...grpc.CallOption) (client.Ingester_ActiveSeriesClient, error) {
	return &activeSeriesStream{results: f.responses.activeSeries}, nil
}

func (f *fanoutBenchIngester) LabelValues(_ context.Context, _ *client.LabelValuesRequest, _ ...grpc.CallOption) (*client.LabelValuesResponse, error) {
	return f.responses.labelValues, nil
}

func (f *fanoutBenchIngester) LabelNames(_ context.Context, _ *client.LabelNamesRequest, _ ...grpc.CallOption) (*client.LabelNamesResponse, error) {
	return f.responses.labelNames, nil
}

func (f *fanoutBenchIngester) MetricsForLabelMatchers(_ context.Context, _ *client.MetricsForLabelMatchersRequest, _ ...grpc.CallOption) (*client.MetricsForLabelMatchersResponse, error) {
	return f.responses.metricsForMatchers, nil
}

func (f *fanoutBenchIngester) LabelNamesAndValues(_ context.Context, _ *client.LabelNamesAndValuesRequest, _ ...grpc.CallOption) (client.Ingester_LabelNamesAndValuesClient, error) {
	return &fanoutBenchStream[*client.LabelNamesAndValuesResponse]{msgs: f.responses.labelNamesAndValues}, nil
}

func (f *fanoutBenchIngester) LabelValuesCardinality(_ context.Context, _ *client.LabelValuesCardinalityRequest, _ ...grpc.CallOption) (client.Ingester_LabelValuesCardinalityClient, error) {
	return &fanoutBenchStream[*client.LabelValuesCardinalityResponse]{msgs: f.responses.labelValuesCardinality}, nil
}

func (f *fanoutBenchIngester) UserStats(_ context.Context, _ *client.UserStatsRequest, _ ...grpc.CallOption) (*client.UserStatsResponse, error) {
	return f.responses.userStats, nil
}

// fanoutBenchStream is a client stream replaying pre-built messages.
type fanoutBenchStream[T any] struct {
	grpc.ClientStream
	next int
	msgs []T
}

func (s *fanoutBenchStream[T]) Recv() (T, error) {
	if s.next >= len(s.msgs) {
		var zero T
		return zero, io.EOF
	}
	msg := s.msgs[s.next]
	s.next++
	return msg, nil
}

func (s *fanoutBenchStream[T]) CloseSend() error { return nil }

// fanoutBenchProfile describes the data each partition returns.
type fanoutBenchProfile struct {
	// seriesPerPartition is the number of series each partition returns for QueryStream and ActiveSeries.
	seriesPerPartition int
	// labelValuesPerPartition is the number of values each partition returns for LabelValues.
	labelValuesPerPartition int
	// labelValuesOverlap is true when every partition returns the same label values (like "job"), and
	// false when each partition returns its own (like "pod").
	labelValuesOverlap bool
	// labelNamesPerPartition is the number of label names each partition returns for LabelNames,
	// LabelNamesAndValues and LabelValuesCardinality. Label names are shared by every partition.
	labelNamesPerPartition int
}

// fanoutBenchSeriesLabels returns realistic labels for the i-th series of a partition. Series are
// disjoint across partitions.
func fanoutBenchSeriesLabels(partition, i int) []mimirpb.LabelAdapter {
	return mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
		model.MetricNameLabel, fanoutBenchMetric,
		"cluster", "prod-us-central-0",
		"namespace", "checkout-"+strconv.Itoa(i%20),
		"job", "checkout/api-"+strconv.Itoa(i%50),
		"pod", fmt.Sprintf("api-%d-%d-7f9c8d6b5-x2kqz", partition, i),
		"instance", fmt.Sprintf("10.%d.%d.%d:8080", partition%256, (i/256)%256, i%256),
		"container", "api",
		"status_code", strconv.Itoa(200+i%5),
		"method", "GET",
		"route", "/api/v1/orders/{id}",
	))
}

// buildFanoutBenchResponses builds the responses partition p replays for the given profile.
func buildFanoutBenchResponses(b testing.TB, p int, profile fanoutBenchProfile, wireChunk client.Chunk) *fanoutBenchResponses {
	b.Helper()
	resp := &fanoutBenchResponses{}

	// QueryStream: series labels in batches, the end-of-series marker, then chunks in batches. Ingesters
	// stream series sorted by labels.
	seriesLabels := make([][]mimirpb.LabelAdapter, 0, profile.seriesPerPartition)
	for i := 0; i < profile.seriesPerPartition; i++ {
		seriesLabels = append(seriesLabels, fanoutBenchSeriesLabels(p, i))
	}
	slices.SortFunc(seriesLabels, mimirpb.CompareLabelAdapters)
	series := make([]client.QueryStreamSeries, 0, len(seriesLabels))
	for _, lbls := range seriesLabels {
		series = append(series, client.QueryStreamSeries{Labels: lbls, ChunkCount: 1})
	}
	for start := 0; start < len(series); start += fanoutBenchBatchSize {
		end := min(start+fanoutBenchBatchSize, len(series))
		resp.queryStream = append(resp.queryStream, &client.QueryStreamResponse{StreamingSeries: series[start:end]})
	}
	resp.queryStream = append(resp.queryStream, &client.QueryStreamResponse{IsEndOfSeriesStream: true})
	for start := 0; start < len(series); start += fanoutBenchBatchSize {
		end := min(start+fanoutBenchBatchSize, len(series))
		batch := make([]client.QueryStreamSeriesChunks, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, client.QueryStreamSeriesChunks{SeriesIndex: uint64(i), Chunks: []client.Chunk{wireChunk}})
		}
		resp.queryStream = append(resp.queryStream, &client.QueryStreamResponse{StreamingSeriesChunks: batch})
	}

	// ActiveSeries: the same series, in batches.
	for start := 0; start < len(seriesLabels); start += fanoutBenchBatchSize {
		end := min(start+fanoutBenchBatchSize, len(seriesLabels))
		batch := &client.ActiveSeriesResponse{}
		for _, lbls := range seriesLabels[start:end] {
			batch.Metric = append(batch.Metric, &mimirpb.Metric{Labels: lbls})
		}
		resp.activeSeries = append(resp.activeSeries, batch)
	}

	// LabelValues: sorted values, shared by every partition or unique to this one.
	values := make([]string, 0, profile.labelValuesPerPartition)
	for i := 0; i < profile.labelValuesPerPartition; i++ {
		if profile.labelValuesOverlap {
			values = append(values, fmt.Sprintf("value-%08d", i))
		} else {
			values = append(values, fmt.Sprintf("value-%05d-%08d", p, i))
		}
	}
	resp.labelValues = &client.LabelValuesResponse{LabelValues: values}

	// LabelNames: names shared by every partition.
	names := make([]string, 0, profile.labelNamesPerPartition)
	for i := 0; i < profile.labelNamesPerPartition; i++ {
		names = append(names, fmt.Sprintf("label_%04d", i))
	}
	resp.labelNames = &client.LabelNamesResponse{LabelNames: names}

	// MetricsForLabelMatchers: the same series as QueryStream.
	resp.metricsForMatchers = &client.MetricsForLabelMatchersResponse{}
	for _, lbls := range seriesLabels {
		resp.metricsForMatchers.Metric = append(resp.metricsForMatchers.Metric, &mimirpb.Metric{Labels: lbls})
	}

	// LabelNamesAndValues and LabelValuesCardinality: every label name has the label values above. The
	// ingester sends one message per label name.
	for _, name := range names {
		resp.labelNamesAndValues = append(resp.labelNamesAndValues, &client.LabelNamesAndValuesResponse{
			Items: []*client.LabelValues{{LabelName: name, Values: values}},
		})
		counts := make(map[string]uint64, len(values))
		for _, v := range values {
			counts[v] = 1
		}
		resp.labelValuesCardinality = append(resp.labelValuesCardinality, &client.LabelValuesCardinalityResponse{
			Items: []*client.LabelValueSeriesCount{{LabelName: name, LabelValueSeries: counts}},
		})
	}

	resp.userStats = &client.UserStatsResponse{NumSeries: uint64(profile.seriesPerPartition), IngestionRate: 1}

	return resp
}

// makeFanoutBenchChunk returns a wire chunk holding fanoutBenchChunkLength samples, about two hours of
// data at a 1m scrape interval.
func makeFanoutBenchChunk(b testing.TB) client.Chunk {
	b.Helper()
	c, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(b, err)
	for i := 0; i < fanoutBenchChunkLength; i++ {
		overflow, err := c.Add(model.SamplePair{Timestamp: model.Time(i * 60_000), Value: model.SampleValue(i)})
		require.NoError(b, err)
		require.Nil(b, overflow)
	}
	return makeWireChunk(c)
}

// prepareFanoutBench returns a distributor whose queries reach the given number of partitions, each
// owned by one fake ingester per zone replaying the responses built for the profile.
func prepareFanoutBench(b *testing.B, partitions int, profile fanoutBenchProfile) *Distributor {
	b.Helper()
	wireChunk := makeFanoutBenchChunk(b)
	responses := make([]*fanoutBenchResponses, partitions)
	for p := range responses {
		responses[p] = buildFanoutBenchResponses(b, p, profile, wireChunk)
	}

	factory := ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
		// Instance addresses are "ingester-<zone>-<partition>".
		var zone string
		var partition int
		if _, err := fmt.Sscanf(inst.Addr, "ingester-zone-%1s-%d", &zone, &partition); err != nil {
			return nil, fmt.Errorf("unexpected ingester address %q: %w", inst.Addr, err)
		}
		return &fanoutBenchIngester{responses: responses[partition]}, nil
	})

	return prepareFanoutBenchWithClients(b, partitions, factory)
}

// prepareFanoutBenchWithClients returns a distributor whose queries reach the given number of
// partitions, with ingester clients created by the given factory.
func prepareFanoutBenchWithClients(b *testing.B, partitions int, factory ring_client.PoolFactory) *Distributor {
	b.Helper()
	distributors, _, _, _ := prepare(b, prepConfig{
		numDistributors:         1,
		ingestStorageEnabled:    true,
		ingestStoragePartitions: int32(partitions),
		// Generating spread-minimizing tokens for thousands of partitions takes minutes, and reads don't
		// depend on them.
		ingestStorageOneTokenPerPartition: true,
		ingesterStateByZone: map[string]ingesterZoneState{
			"zone-a": {numIngesters: partitions, happyIngesters: partitions},
			"zone-b": {numIngesters: partitions, happyIngesters: partitions},
		},
		replicationFactor: 2,
		// The benchmark doesn't write, so the mocked ingesters don't need to consume from Kafka.
		ingesterIngestionType: ingesterIngestionTypeGRPC,
		configure: func(cfg *Config) {
			cfg.IngesterClientFactory = factory
			cfg.StreamingChunksPerIngesterSeriesBufferSize = fanoutBenchBatchSize
			// Match prod queriers: query one zone per partition (-querier.minimize-ingester-requests
			// defaults to true), preferring the querier's own zone.
			cfg.MinimizeIngesterRequests = true
			cfg.PreferAvailabilityZones = []string{"zone-a"}
		},
	})
	d := distributors[0]

	test.Poll(b, 10*time.Second, partitions, func() interface{} {
		return d.partitionInstanceRings.Get(0).PartitionRing().ActivePartitionsCount()
	})
	return d
}

// fanoutBenchCPUTime returns the CPU time used by the process so far.
func fanoutBenchCPUTime(b *testing.B) time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

// runFanoutBench runs op b.N times and reports the CPU time per op alongside the wall time, since the
// query path fans out in parallel and wall time alone hides the CPU a querier spends per query.
func runFanoutBench(b *testing.B, op func()) {
	b.ReportAllocs()
	b.ResetTimer()
	start := fanoutBenchCPUTime(b)
	for n := 0; n < b.N; n++ {
		op()
	}
	b.StopTimer()
	b.ReportMetric(float64(fanoutBenchCPUTime(b)-start)/float64(b.N), "cpu-ns/op")
}

func fanoutBenchContext() context.Context {
	ctx := user.InjectOrgID(context.Background(), fanoutBenchTenant)
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	return limiter.ContextWithNewSeriesLabelsDeduplicator(ctx, limiter.NewSeriesDeduplicatorMetrics(prometheus.NewPedanticRegistry()))
}

// BenchmarkDistributor_QueryStreamFanout measures QueryStream, including reading every series' chunks
// the way the querier does, as the number of partitions grows. seriesPerPartition=0 measures the fixed
// cost of fanning out; prod data shows 69-95% of ingester requests return no series.
func BenchmarkDistributor_QueryStreamFanout(b *testing.B) {
	for _, partitions := range fanoutBenchPartitions() {
		for _, seriesPerPartition := range []int{0, 1, 10, 100} {
			b.Run(fmt.Sprintf("partitions=%d/series_per_partition=%d", partitions, seriesPerPartition), func(b *testing.B) {
				d := prepareFanoutBench(b, partitions, fanoutBenchProfile{seriesPerPartition: seriesPerPartition})
				queryMetrics := stats.NewQueryMetrics(prometheus.NewPedanticRegistry())
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, fanoutBenchMetric)}

				runFanoutBench(b, func() {
					resp, err := d.QueryStream(fanoutBenchContext(), queryMetrics, 0, model.Time(time.Hour.Milliseconds()), matchers...)
					if err != nil {
						b.Fatal(err)
					}
					if len(resp.StreamingSeries) != partitions*seriesPerPartition {
						b.Fatalf("expected %d series, got %d", partitions*seriesPerPartition, len(resp.StreamingSeries))
					}
					for _, s := range resp.StreamingSeries {
						for _, source := range s.Sources {
							if _, err := source.StreamReader.GetChunks(source.SeriesIndex); err != nil {
								b.Fatal(err)
							}
						}
					}
					for _, r := range resp.StreamReaders {
						r.Close()
					}
				})
			})
		}
	}
}

// BenchmarkDistributor_ActiveSeriesFanout measures the active series cardinality API, which the
// dedicated cells call about 3x as often as they run user queries.
func BenchmarkDistributor_ActiveSeriesFanout(b *testing.B) {
	for _, partitions := range fanoutBenchPartitions() {
		for _, seriesPerPartition := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("partitions=%d/series_per_partition=%d", partitions, seriesPerPartition), func(b *testing.B) {
				d := prepareFanoutBench(b, partitions, fanoutBenchProfile{seriesPerPartition: seriesPerPartition})
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, fanoutBenchMetric)}

				runFanoutBench(b, func() {
					series, err := d.ActiveSeries(fanoutBenchContext(), matchers)
					if err != nil {
						b.Fatal(err)
					}
					if len(series) != partitions*seriesPerPartition {
						b.Fatalf("expected %d series, got %d", partitions*seriesPerPartition, len(series))
					}
				})
			})
		}
	}
}

// BenchmarkDistributor_LabelValuesFanout measures label values merging, where fan-out and response size
// combine: values shared by every partition (like "job") are returned by each of them and deduplicated
// by the distributor.
func BenchmarkDistributor_LabelValuesFanout(b *testing.B) {
	for _, partitions := range fanoutBenchPartitions() {
		for _, valuesPerPartition := range []int{10, 100, 1000} {
			for _, overlap := range []bool{true, false} {
				b.Run(fmt.Sprintf("partitions=%d/values_per_partition=%d/overlap=%t", partitions, valuesPerPartition, overlap), func(b *testing.B) {
					d := prepareFanoutBench(b, partitions, fanoutBenchProfile{labelValuesPerPartition: valuesPerPartition, labelValuesOverlap: overlap})
					expected := valuesPerPartition
					if !overlap {
						expected *= partitions
					}

					runFanoutBench(b, func() {
						values, err := d.LabelValuesForLabelName(fanoutBenchContext(), 0, model.Time(time.Hour.Milliseconds()), "pod", nil)
						if err != nil {
							b.Fatal(err)
						}
						if len(values) != expected {
							b.Fatalf("expected %d values, got %d", expected, len(values))
						}
					})
				})
			}
		}
	}
}

// BenchmarkDistributor_LabelNamesFanout measures label names merging. Label names are almost entirely
// shared across partitions.
func BenchmarkDistributor_LabelNamesFanout(b *testing.B) {
	for _, partitions := range fanoutBenchPartitions() {
		for _, namesPerPartition := range []int{10, 100} {
			b.Run(fmt.Sprintf("partitions=%d/names_per_partition=%d", partitions, namesPerPartition), func(b *testing.B) {
				d := prepareFanoutBench(b, partitions, fanoutBenchProfile{labelNamesPerPartition: namesPerPartition})

				runFanoutBench(b, func() {
					names, err := d.LabelNames(fanoutBenchContext(), 0, model.Time(time.Hour.Milliseconds()), nil)
					if err != nil {
						b.Fatal(err)
					}
					if len(names) != namesPerPartition {
						b.Fatalf("expected %d names, got %d", namesPerPartition, len(names))
					}
				})
			})
		}
	}
}

// BenchmarkDistributor_MetricsForLabelMatchersFanout measures the series API, which deduplicates series
// by hashing their labels.
func BenchmarkDistributor_MetricsForLabelMatchersFanout(b *testing.B) {
	for _, partitions := range fanoutBenchPartitions() {
		for _, seriesPerPartition := range []int{1, 10, 100} {
			b.Run(fmt.Sprintf("partitions=%d/series_per_partition=%d", partitions, seriesPerPartition), func(b *testing.B) {
				d := prepareFanoutBench(b, partitions, fanoutBenchProfile{seriesPerPartition: seriesPerPartition})
				matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, fanoutBenchMetric)}

				runFanoutBench(b, func() {
					series, err := d.MetricsForLabelMatchers(fanoutBenchContext(), 0, model.Time(time.Hour.Milliseconds()), nil, matchers...)
					if err != nil {
						b.Fatal(err)
					}
					if len(series) != partitions*seriesPerPartition {
						b.Fatalf("expected %d series, got %d", partitions*seriesPerPartition, len(series))
					}
				})
			})
		}
	}
}

// BenchmarkDistributor_LabelNamesAndValuesFanout measures the cardinality label names API, which merges
// every label name's values into one set.
func BenchmarkDistributor_LabelNamesAndValuesFanout(b *testing.B) {
	const namesPerPartition = 10
	for _, partitions := range fanoutBenchPartitions() {
		for _, valuesPerPartition := range []int{10, 100} {
			for _, overlap := range []bool{true, false} {
				b.Run(fmt.Sprintf("partitions=%d/values_per_name=%d/overlap=%t", partitions, valuesPerPartition, overlap), func(b *testing.B) {
					d := prepareFanoutBench(b, partitions, fanoutBenchProfile{labelNamesPerPartition: namesPerPartition, labelValuesPerPartition: valuesPerPartition, labelValuesOverlap: overlap})
					matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, fanoutBenchMetric)}

					runFanoutBench(b, func() {
						resp, err := d.LabelNamesAndValues(fanoutBenchContext(), matchers, cardinality.InMemoryMethod)
						if err != nil {
							b.Fatal(err)
						}
						if len(resp.Items) != namesPerPartition {
							b.Fatalf("expected %d label names, got %d", namesPerPartition, len(resp.Items))
						}
					})
				})
			}
		}
	}
}

// BenchmarkDistributor_LabelValuesCardinalityFanout measures the cardinality label values API, which
// counts series per label value for each partition and zone, and also calls UserStats on every partition.
func BenchmarkDistributor_LabelValuesCardinalityFanout(b *testing.B) {
	const namesPerPartition = 1
	for _, partitions := range fanoutBenchPartitions() {
		for _, valuesPerPartition := range []int{10, 100} {
			for _, overlap := range []bool{true, false} {
				b.Run(fmt.Sprintf("partitions=%d/values_per_partition=%d/overlap=%t", partitions, valuesPerPartition, overlap), func(b *testing.B) {
					d := prepareFanoutBench(b, partitions, fanoutBenchProfile{labelNamesPerPartition: namesPerPartition, labelValuesPerPartition: valuesPerPartition, labelValuesOverlap: overlap})
					matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, fanoutBenchMetric)}
					expected := valuesPerPartition
					if !overlap {
						expected *= partitions
					}

					runFanoutBench(b, func() {
						_, resp, err := d.LabelValuesCardinality(fanoutBenchContext(), []model.LabelName{"label_0000"}, matchers, cardinality.InMemoryMethod)
						if err != nil {
							b.Fatal(err)
						}
						if len(resp.Items) != 1 || len(resp.Items[0].LabelValueSeries) != expected {
							b.Fatalf("expected 1 label name with %d values, got %v", expected, resp.Items)
						}
					})
				})
			}
		}
	}
}
