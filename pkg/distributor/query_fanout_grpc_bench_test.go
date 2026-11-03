// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/cardinality"
	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/grpcstats"
)

// The gRPC fan-out benchmarks run the same queries as the in-process ones, but over real gRPC
// connections to fake ingesters running in a child process, listening on a Unix socket. Running the
// fakes in their own process keeps their encoding and sending out of the benchmark's CPU time, so
// cpu-ns/op stays the querier's cost: the in-process cost plus gRPC, HTTP/2 and protobuf decoding.

const (
	// fanoutBenchServerEnv holds the child process's fanoutBenchServerConfig, as JSON.
	fanoutBenchServerEnv = "MIMIR_FANOUT_BENCH_SERVER"
	// fanoutBenchPartitionMetadata is the gRPC metadata key telling the fake ingesters which
	// partition a request is for, since every connection goes to the same process.
	fanoutBenchPartitionMetadata = "fanout-bench-partition"
)

type fanoutBenchServerConfig struct {
	Socket                  string `json:"socket"`
	Partitions              int    `json:"partitions"`
	SeriesPerPartition      int    `json:"series_per_partition"`
	LabelValuesPerPartition int    `json:"label_values_per_partition"`
	LabelValuesOverlap      bool   `json:"label_values_overlap"`
	LabelNamesPerPartition  int    `json:"label_names_per_partition"`
}

func (c fanoutBenchServerConfig) profile() fanoutBenchProfile {
	return fanoutBenchProfile{
		seriesPerPartition:      c.SeriesPerPartition,
		labelValuesPerPartition: c.LabelValuesPerPartition,
		labelValuesOverlap:      c.LabelValuesOverlap,
		labelNamesPerPartition:  c.LabelNamesPerPartition,
	}
}

// TestFanoutBenchServer is the fake ingesters' process for the gRPC fan-out benchmarks. It only runs
// when started by them, and serves until its stdin is closed.
func TestFanoutBenchServer(t *testing.T) {
	raw := os.Getenv(fanoutBenchServerEnv)
	if raw == "" {
		t.Skip("only runs as the gRPC fan-out benchmarks' fake ingester process")
	}
	var cfg fanoutBenchServerConfig
	require.NoError(t, json.Unmarshal([]byte(raw), &cfg))

	wireChunk := makeFanoutBenchChunk(t)
	responses := make([]*fanoutBenchResponses, cfg.Partitions)
	for p := range responses {
		responses[p] = buildFanoutBenchResponses(t, p, cfg.profile(), wireChunk)
	}

	lis, err := net.Listen("unix", cfg.Socket)
	require.NoError(t, err)
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(100<<20), grpc.MaxSendMsgSize(100<<20))
	client.RegisterIngesterServer(srv, &fanoutBenchServer{responses: responses})
	// The client pool health checks ingesters periodically.
	grpc_health_v1.RegisterHealthServer(srv, fanoutBenchHealthServer{})
	go func() {
		// Serve returns an error once Stop is called below; nothing to do about it.
		_ = srv.Serve(lis)
	}()

	_, err = io.Copy(io.Discard, os.Stdin)
	require.NoError(t, err)
	srv.Stop()
}

// fanoutBenchHealthServer reports the fake ingesters as always serving.
type fanoutBenchHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (fanoutBenchHealthServer) Check(context.Context, *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

// fanoutBenchServer is a fake ingester gRPC server replaying pre-built responses.
type fanoutBenchServer struct {
	client.UnimplementedIngesterServer
	responses []*fanoutBenchResponses
}

func (s *fanoutBenchServer) responsesFor(ctx context.Context) (*fanoutBenchResponses, error) {
	values := metadata.ValueFromIncomingContext(ctx, fanoutBenchPartitionMetadata)
	if len(values) != 1 {
		return nil, fmt.Errorf("expected one %s metadata value, got %v", fanoutBenchPartitionMetadata, values)
	}
	p, err := strconv.Atoi(values[0])
	if err != nil || p < 0 || p >= len(s.responses) {
		return nil, fmt.Errorf("invalid partition %q", values[0])
	}
	return s.responses[p], nil
}

func fanoutBenchSendAll[T any](msgs []T, send func(T) error) error {
	for _, msg := range msgs {
		if err := send(msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *fanoutBenchServer) QueryStream(_ *client.QueryRequest, stream client.Ingester_QueryStreamServer) error {
	r, err := s.responsesFor(stream.Context())
	if err != nil {
		return err
	}
	return fanoutBenchSendAll(r.queryStream, stream.Send)
}

func (s *fanoutBenchServer) ActiveSeries(_ *client.ActiveSeriesRequest, stream client.Ingester_ActiveSeriesServer) error {
	r, err := s.responsesFor(stream.Context())
	if err != nil {
		return err
	}
	return fanoutBenchSendAll(r.activeSeries, stream.Send)
}

func (s *fanoutBenchServer) LabelValues(ctx context.Context, _ *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	r, err := s.responsesFor(ctx)
	if err != nil {
		return nil, err
	}
	return r.labelValues, nil
}

func (s *fanoutBenchServer) LabelNames(ctx context.Context, _ *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	r, err := s.responsesFor(ctx)
	if err != nil {
		return nil, err
	}
	return r.labelNames, nil
}

func (s *fanoutBenchServer) MetricsForLabelMatchers(ctx context.Context, _ *client.MetricsForLabelMatchersRequest) (*client.MetricsForLabelMatchersResponse, error) {
	r, err := s.responsesFor(ctx)
	if err != nil {
		return nil, err
	}
	return r.metricsForMatchers, nil
}

func (s *fanoutBenchServer) LabelNamesAndValues(_ *client.LabelNamesAndValuesRequest, stream client.Ingester_LabelNamesAndValuesServer) error {
	r, err := s.responsesFor(stream.Context())
	if err != nil {
		return err
	}
	return fanoutBenchSendAll(r.labelNamesAndValues, stream.Send)
}

func (s *fanoutBenchServer) LabelValuesCardinality(_ *client.LabelValuesCardinalityRequest, stream client.Ingester_LabelValuesCardinalityServer) error {
	r, err := s.responsesFor(stream.Context())
	if err != nil {
		return err
	}
	return fanoutBenchSendAll(r.labelValuesCardinality, stream.Send)
}

func (s *fanoutBenchServer) UserStats(ctx context.Context, _ *client.UserStatsRequest) (*client.UserStatsResponse, error) {
	r, err := s.responsesFor(ctx)
	if err != nil {
		return nil, err
	}
	return r.userStats, nil
}

// startFanoutBenchServer starts the fake ingesters' process and returns the socket it listens on.
func startFanoutBenchServer(b *testing.B, partitions int, profile fanoutBenchProfile) string {
	b.Helper()

	// Unix socket paths are limited to about 100 bytes, which b.TempDir() can exceed.
	dir, err := os.MkdirTemp("", "fanout")
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, os.RemoveAll(dir)) })
	socket := filepath.Join(dir, "s")

	cfg, err := json.Marshal(fanoutBenchServerConfig{
		Socket:                  socket,
		Partitions:              partitions,
		SeriesPerPartition:      profile.seriesPerPartition,
		LabelValuesPerPartition: profile.labelValuesPerPartition,
		LabelValuesOverlap:      profile.labelValuesOverlap,
		LabelNamesPerPartition:  profile.labelNamesPerPartition,
	})
	require.NoError(b, err)

	cmd := exec.Command(os.Args[0], "-test.run=^TestFanoutBenchServer$", "-test.count=1", "-test.timeout=0")
	cmd.Env = append(os.Environ(), fanoutBenchServerEnv+"="+string(cfg))
	// The child's output would interleave with benchmark results, so it's only shown if it fails.
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	stdin, err := cmd.StdinPipe()
	require.NoError(b, err)
	require.NoError(b, cmd.Start())
	b.Cleanup(func() {
		require.NoError(b, stdin.Close())
		require.NoError(b, cmd.Wait(), "fake ingester process failed:\n%s", output.String())
	})

	require.Eventually(b, func() bool {
		conn, err := net.Dial("unix", socket)
		if err != nil {
			return false
		}
		require.NoError(b, conn.Close())
		return true
	}, 30*time.Second, 10*time.Millisecond, "fake ingester process didn't start listening")
	return socket
}

// fanoutBenchGRPCClient is an ingester client over a real gRPC connection.
type fanoutBenchGRPCClient struct {
	client.IngesterClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *fanoutBenchGRPCClient) Close() error { return c.conn.Close() }

// prepareFanoutBenchGRPC is prepareFanoutBench with ingester clients connected over gRPC to fake
// ingesters in a child process.
func prepareFanoutBenchGRPC(b *testing.B, partitions int, profile fanoutBenchProfile) *Distributor {
	b.Helper()
	socket := startFanoutBenchServer(b, partitions, profile)

	// Under go test, mimirpb instruments every unmarshalled buffer for reference leaks (see its init),
	// mapping and protecting memory per message. Prod doesn't, so disable it while benchmarking, and
	// restore it for anything that runs afterwards.
	mimirpb.CustomCodecConfig{}.RegisterGlobally(nil)
	b.Cleanup(func() {
		mimirpb.CustomCodecConfig{InstrumentRefLeaksConfig: mimirpb.InstrumentRefLeaksConfig{Percentage: 100}}.RegisterGlobally(prometheus.NewRegistry())
	})

	// Use the ingester client's defaults, interceptors and stats handlers, as client.MakeIngesterClient
	// does. MakeIngesterClient can't be used directly because it can't dial a Unix socket. Unlike it,
	// responses aren't decoded into pooled buffers, so allocations are slightly overstated.
	var clientCfg client.Config
	flagext.DefaultValues(&clientCfg)
	reg := prometheus.NewPedanticRegistry()
	requestDuration := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "request_duration_seconds"}, []string{"operation", "status_code"})
	transferredBytes := prometheus.NewCounter(prometheus.CounterOpts{Name: "transferred_bytes_total"})
	reg.MustRegister(requestDuration, transferredBytes)

	factory := ring_client.PoolInstFunc(func(inst ring.InstanceDesc) (ring_client.PoolClient, error) {
		var zone string
		var partition int
		if _, err := fmt.Sscanf(inst.Addr, "ingester-zone-%1s-%d", &zone, &partition); err != nil {
			return nil, fmt.Errorf("unexpected ingester address %q: %w", inst.Addr, err)
		}
		md := []string{fanoutBenchPartitionMetadata, strconv.Itoa(partition)}

		unary, stream := grpcclient.Instrument(requestDuration, middleware.ReportGRPCStatusOption)
		unary = append(unary, querierapi.ReadConsistencyClientUnaryInterceptor, client.OnlyReplicaClientUnaryInterceptor,
			func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				return invoker(metadata.AppendToOutgoingContext(ctx, md...), method, req, reply, cc, opts...)
			})
		stream = append(stream, querierapi.ReadConsistencyClientStreamInterceptor, client.OnlyReplicaClientStreamInterceptor,
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				return streamer(metadata.AppendToOutgoingContext(ctx, md...), desc, cc, method, opts...)
			})

		opts, err := clientCfg.GRPCClientConfig.DialOption(unary, stream, nil)
		if err != nil {
			return nil, err
		}
		opts = append(opts,
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
			grpc.WithStatsHandler(grpcstats.NewDataTransferStatsHandler(transferredBytes)),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			}),
		)
		conn, err := grpc.NewClient("passthrough:///"+inst.Addr, opts...)
		if err != nil {
			return nil, err
		}
		return &fanoutBenchGRPCClient{IngesterClient: client.NewIngesterClient(conn), HealthClient: grpc_health_v1.NewHealthClient(conn), conn: conn}, nil
	})

	d := prepareFanoutBenchWithClients(b, partitions, factory)

	// Open the connections before measuring.
	_, err := d.LabelNames(fanoutBenchContext(), 0, model.Time(time.Hour.Milliseconds()), nil)
	require.NoError(b, err)
	return d
}

// BenchmarkDistributor_FanoutGRPC runs a subset of the in-process fan-out benchmarks over gRPC.
func BenchmarkDistributor_FanoutGRPC(b *testing.B) {
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, fanoutBenchMetric)}
	end := model.Time(time.Hour.Milliseconds())

	type endpoint struct {
		name     string
		profiles []fanoutBenchProfile
		describe func(fanoutBenchProfile) string
		run      func(b *testing.B, d *Distributor, partitions int, profile fanoutBenchProfile)
	}
	endpoints := []endpoint{
		{
			name:     "query_stream",
			profiles: []fanoutBenchProfile{{seriesPerPartition: 0}, {seriesPerPartition: 10}, {seriesPerPartition: 100}},
			describe: func(p fanoutBenchProfile) string { return fmt.Sprintf("series_per_partition=%d", p.seriesPerPartition) },
			run: func(b *testing.B, d *Distributor, partitions int, profile fanoutBenchProfile) {
				queryMetrics := stats.NewQueryMetrics(prometheus.NewPedanticRegistry())
				runFanoutBench(b, func() {
					resp, err := d.QueryStream(fanoutBenchContext(), queryMetrics, 0, end, matchers...)
					if err != nil {
						b.Fatal(err)
					}
					if len(resp.StreamingSeries) != partitions*profile.seriesPerPartition {
						b.Fatalf("expected %d series, got %d", partitions*profile.seriesPerPartition, len(resp.StreamingSeries))
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
			},
		},
		{
			name:     "active_series",
			profiles: []fanoutBenchProfile{{seriesPerPartition: 1}, {seriesPerPartition: 100}},
			describe: func(p fanoutBenchProfile) string { return fmt.Sprintf("series_per_partition=%d", p.seriesPerPartition) },
			run: func(b *testing.B, d *Distributor, partitions int, profile fanoutBenchProfile) {
				runFanoutBench(b, func() {
					series, err := d.ActiveSeries(fanoutBenchContext(), matchers)
					if err != nil {
						b.Fatal(err)
					}
					if len(series) != partitions*profile.seriesPerPartition {
						b.Fatalf("expected %d series, got %d", partitions*profile.seriesPerPartition, len(series))
					}
				})
			},
		},
		{
			name:     "label_values",
			profiles: []fanoutBenchProfile{{labelValuesPerPartition: 10, labelValuesOverlap: true}, {labelValuesPerPartition: 1000, labelValuesOverlap: false}},
			describe: func(p fanoutBenchProfile) string {
				return fmt.Sprintf("values_per_partition=%d/overlap=%t", p.labelValuesPerPartition, p.labelValuesOverlap)
			},
			run: func(b *testing.B, d *Distributor, partitions int, profile fanoutBenchProfile) {
				expected := profile.labelValuesPerPartition
				if !profile.labelValuesOverlap {
					expected *= partitions
				}
				runFanoutBench(b, func() {
					values, err := d.LabelValuesForLabelName(fanoutBenchContext(), 0, end, "pod", nil)
					if err != nil {
						b.Fatal(err)
					}
					if len(values) != expected {
						b.Fatalf("expected %d values, got %d", expected, len(values))
					}
				})
			},
		},
		{
			name:     "series",
			profiles: []fanoutBenchProfile{{seriesPerPartition: 1}, {seriesPerPartition: 100}},
			describe: func(p fanoutBenchProfile) string { return fmt.Sprintf("series_per_partition=%d", p.seriesPerPartition) },
			run: func(b *testing.B, d *Distributor, partitions int, profile fanoutBenchProfile) {
				runFanoutBench(b, func() {
					series, err := d.MetricsForLabelMatchers(fanoutBenchContext(), 0, end, nil, matchers...)
					if err != nil {
						b.Fatal(err)
					}
					if len(series) != partitions*profile.seriesPerPartition {
						b.Fatalf("expected %d series, got %d", partitions*profile.seriesPerPartition, len(series))
					}
				})
			},
		},
		{
			name:     "label_values_cardinality",
			profiles: []fanoutBenchProfile{{labelNamesPerPartition: 1, labelValuesPerPartition: 10, labelValuesOverlap: true}},
			describe: func(p fanoutBenchProfile) string {
				return fmt.Sprintf("values_per_partition=%d/overlap=%t", p.labelValuesPerPartition, p.labelValuesOverlap)
			},
			run: func(b *testing.B, d *Distributor, _ int, profile fanoutBenchProfile) {
				runFanoutBench(b, func() {
					_, resp, err := d.LabelValuesCardinality(fanoutBenchContext(), []model.LabelName{"label_0000"}, matchers, cardinality.InMemoryMethod)
					if err != nil {
						b.Fatal(err)
					}
					if len(resp.Items) != 1 || len(resp.Items[0].LabelValueSeries) != profile.labelValuesPerPartition {
						b.Fatalf("unexpected response %v", resp.Items)
					}
				})
			},
		},
	}

	for _, e := range endpoints {
		for _, partitions := range []int{350, 3500} {
			for _, profile := range e.profiles {
				b.Run(fmt.Sprintf("endpoint=%s/partitions=%d/%s", e.name, partitions, e.describe(profile)), func(b *testing.B) {
					d := prepareFanoutBenchGRPC(b, partitions, profile)
					e.run(b, d, partitions, profile)
				})
			}
		}
	}
}
