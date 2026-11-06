// SPDX-License-Identifier: AGPL-3.0-only

// fake-ingester runs many fake ingesters in one process, so queriers can be tested against
// thousands of ingesters without the cost of real ones. Each fake has its own gRPC server and port,
// registers in the ingester ring and owns an ACTIVE partition, and serves the read RPCs from
// synthetic, deterministic, cached responses with a configurable latency and error model.
//
// See "Approach: a fleet of fake ingesters" in fanout-load-test-plan.md.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/codec"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/ingester/client"
)

type config struct {
	Fakes                    int
	FirstPartition           int
	PartitionFromHostname    bool
	Zone                     string
	Compartment              int
	CompartmentsEnabled      bool
	NumCompartments          int
	PartitionsPerCompartment int
	InstancePrefix           string
	AdvertiseAddr            string
	ListenAddr               string
	BasePort                 int
	HTTPListen               string
	HeartbeatPeriod          time.Duration
	RuntimeConfigFile        string
	RuntimeConfigReload      time.Duration
	ResponseCacheBytes       int
	MaxConcurrentStreams     uint
	UnregisterOnExit         bool
	RemovePartitionsOnExit   bool

	InstanceRing  kv.Config
	PartitionRing kv.Config
	Memberlist    memberlist.KVConfig
}

func (c *config) registerFlags(f *flag.FlagSet) {
	f.IntVar(&c.Fakes, "fakes", 100, "Fake ingesters in this process; each owns one partition.")
	f.IntVar(&c.FirstPartition, "first-partition", 0, "Partition ID of the first fake. The rest follow consecutively.")
	f.BoolVar(&c.PartitionFromHostname, "first-partition-from-hostname", false, "Set -first-partition to (StatefulSet ordinal of the hostname) × -fakes.")
	f.StringVar(&c.Zone, "zone", "zone-a", "Zone of every fake in this process.")
	f.IntVar(&c.Compartment, "compartment", 0, "Read compartment of every fake in this process.")
	f.BoolVar(&c.CompartmentsEnabled, "compartments.enabled", true, "Register under per-compartment partition-ring keys (ingester-partitions-rc-<N>).")
	f.IntVar(&c.NumCompartments, "compartments.num", 10, "Read compartments in the simulated cell, for routing and sizing.")
	f.IntVar(&c.PartitionsPerCompartment, "partitions-per-compartment", 350, "Partitions per compartment in the simulated cell, for splitting a selector's series.")
	f.StringVar(&c.InstancePrefix, "instance-prefix", "fake-ingester", "Instance ID prefix. IDs are <prefix>-<zone>-rc-<compartment>-<partition>.")
	f.StringVar(&c.AdvertiseAddr, "advertise-addr", os.Getenv("POD_IP"), "IP that queriers dial. Defaults to $POD_IP.")
	f.StringVar(&c.ListenAddr, "listen-addr", "0.0.0.0", "IP the gRPC servers listen on.")
	f.IntVar(&c.BasePort, "base-port", 10000, "Port of the first fake's gRPC server. The rest follow consecutively.")
	f.StringVar(&c.HTTPListen, "http-listen", ":8080", "Address for /metrics, /config and /ready.")
	f.DurationVar(&c.HeartbeatPeriod, "heartbeat-period", 15*time.Second, "Ring heartbeat period. Keep it well under the queriers' -ingester.ring.heartbeat-timeout.")
	f.StringVar(&c.RuntimeConfigFile, "runtime-config.file", "", "YAML runtime config: sizes, latency, errors, labels. Missing fields use defaults.")
	f.DurationVar(&c.RuntimeConfigReload, "runtime-config.reload-period", 10*time.Second, "How often to re-read the runtime config file.")
	f.IntVar(&c.ResponseCacheBytes, "response-cache-bytes", 1<<30, "Byte budget for cached QueryStream responses.")
	f.UintVar(&c.MaxConcurrentStreams, "grpc-max-concurrent-streams", 10000, "Max concurrent gRPC streams per connection.")
	f.BoolVar(&c.UnregisterOnExit, "unregister-on-exit", true, "Remove instances and partition owners from the rings on exit.")
	f.BoolVar(&c.RemovePartitionsOnExit, "remove-partitions-on-exit", false, "Also remove partitions on exit (final teardown only).")

	c.InstanceRing.RegisterFlagsWithPrefix("ring.", "fake-collectors/", f)
	c.PartitionRing.RegisterFlagsWithPrefix("partition-ring.", "fake-collectors/", f)
	c.Memberlist.RegisterFlags(f)
}

var ordinalRE = regexp.MustCompile(`-([0-9]+)$`)

func main() {
	var cfg config
	cfg.registerFlags(flag.CommandLine)
	flag.Parse()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(log.With(logger, "ts", log.DefaultTimestampUTC), level.AllowInfo())
	if err := run(cfg, logger); err != nil {
		level.Error(logger).Log("msg", "fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfg config, logger log.Logger) error {
	if cfg.PartitionFromHostname {
		host, _ := os.Hostname()
		m := ordinalRE.FindStringSubmatch(host)
		if m == nil {
			return fmt.Errorf("hostname %q has no ordinal suffix", host)
		}
		ord, _ := strconv.Atoi(m[1])
		cfg.FirstPartition = ord * cfg.Fakes
	}
	if cfg.AdvertiseAddr == "" {
		return errors.New("-advertise-addr (or $POD_IP) is required")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	holder := &configHolder{}
	if err := loadConfigFile(holder, cfg.RuntimeConfigFile); err != nil {
		return err
	}
	if cfg.RuntimeConfigFile != "" {
		go reloadLoop(ctx, holder, cfg.RuntimeConfigFile, cfg.RuntimeConfigReload, logger)
	}

	s := &shared{
		cfg:     holder,
		topo:    Topology{NumCompartments: cfg.NumCompartments, PartitionsPerCompartment: cfg.PartitionsPerCompartment},
		router:  compartments.NewRouter(cfg.NumCompartments),
		chunks:  newChunkCache(),
		resps:   newResponseCache(cfg.ResponseCacheBytes),
		metrics: newServerMetrics(reg),
		now:     time.Now,
	}

	// Start one gRPC server per fake.
	var fakes []*fake
	var servers []*grpc.Server
	for i := 0; i < cfg.Fakes; i++ {
		partition := cfg.FirstPartition + i
		port := cfg.BasePort + i
		f := &fake{
			id:          fmt.Sprintf("%s-%s-rc-%d-%d", cfg.InstancePrefix, cfg.Zone, cfg.Compartment, partition),
			zone:        cfg.Zone,
			compartment: cfg.Compartment,
			partition:   partition,
			addr:        net.JoinHostPort(cfg.AdvertiseAddr, strconv.Itoa(port)),
			s:           s,
		}
		lis, err := net.Listen("tcp", net.JoinHostPort(cfg.ListenAddr, strconv.Itoa(port)))
		if err != nil {
			return fmt.Errorf("listen for %s: %w", f.id, err)
		}
		srv := newGRPCServer(cfg.MaxConcurrentStreams)
		client.RegisterIngesterServer(srv, f)
		grpc_health_v1.RegisterHealthServer(srv, f)
		go func() { _ = srv.Serve(lis) }()
		fakes = append(fakes, f)
		servers = append(servers, srv)
	}
	level.Info(logger).Log("msg", "fake ingesters listening", "fakes", len(fakes), "first_partition", cfg.FirstPartition, "zone", cfg.Zone, "compartment", cfg.Compartment, "base_port", cfg.BasePort)

	// Ring registration.
	instKV, partKV, stopKV, err := newKVClients(ctx, cfg, reg, logger)
	if err != nil {
		return err
	}
	defer stopKV()
	r := &registrar{instances: instKV, partitions: partKV, fakes: fakes, compartmentsOn: cfg.CompartmentsEnabled, logger: logger}
	go r.run(ctx, cfg.HeartbeatPeriod)

	// HTTP: metrics, runtime config, readiness.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ready\n") })
	mux.HandleFunc("/config", configHandler(holder))
	httpSrv := &http.Server{Addr: cfg.HTTPListen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = httpSrv.ListenAndServe() }()

	<-ctx.Done()
	level.Info(logger).Log("msg", "shutting down")
	if cfg.UnregisterOnExit {
		uctx, ucancel := context.WithTimeout(context.Background(), 20*time.Second)
		if err := r.unregister(uctx, cfg.RemovePartitionsOnExit); err != nil {
			level.Warn(logger).Log("msg", "unregister failed", "err", err)
		}
		ucancel()
	}
	for _, srv := range servers {
		srv.Stop()
	}
	_ = httpSrv.Close()
	return nil
}

func newGRPCServer(maxStreams uint) *grpc.Server {
	return grpc.NewServer(
		grpc.ChainUnaryInterceptor(middleware.ServerUserHeaderInterceptor),
		grpc.ChainStreamInterceptor(middleware.StreamServerUserHeaderInterceptor),
		grpc.MaxConcurrentStreams(uint32(maxStreams)),
		grpc.MaxSendMsgSize(100<<20),
		grpc.MaxRecvMsgSize(100<<20),
		// Queriers ping idle connections; accept that rather than closing them.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: time.Minute, Timeout: 20 * time.Second}),
	)
}

// newKVClients builds the instance-ring and partition-ring KV clients. With the memberlist store,
// both share one memberlist KV that has both ring codecs registered.
func newKVClients(ctx context.Context, cfg config, reg prometheus.Registerer, logger log.Logger) (kv.Client, kv.Client, func(), error) {
	stop := func() {}
	if cfg.InstanceRing.Store == "memberlist" || cfg.PartitionRing.Store == "memberlist" {
		cfg.Memberlist.Codecs = []codec.Codec{ring.GetCodec(), ring.GetPartitionRingCodec()}
		dnsProvider := dns.NewProvider(dns.GolangResolverType, 0, logger, prometheus.WrapRegistererWithPrefix("fake_ingester_", reg))
		svc := memberlist.NewKVInitService(&cfg.Memberlist, log.With(logger, "component", "memberlist"), dnsProvider, reg)
		if err := services.StartAndAwaitRunning(ctx, svc); err != nil {
			return nil, nil, nil, fmt.Errorf("start memberlist: %w", err)
		}
		stop = func() { _ = services.StopAndAwaitTerminated(context.Background(), svc) }
		cfg.InstanceRing.MemberlistKV = svc.GetMemberlistKV
		cfg.PartitionRing.MemberlistKV = svc.GetMemberlistKV
	}
	inst, err := kv.NewClient(cfg.InstanceRing, ring.GetCodec(), kv.RegistererWithKVName(reg, "fake-ingester-instances"), logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("instance ring KV: %w", err)
	}
	part, err := kv.NewClient(cfg.PartitionRing, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(reg, "fake-ingester-partitions"), logger)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("partition ring KV: %w", err)
	}
	return inst, part, stop, nil
}

func loadConfigFile(h *configHolder, path string) error {
	var b []byte
	if path != "" {
		var err error
		if b, err = os.ReadFile(path); err != nil {
			return fmt.Errorf("read runtime config: %w", err)
		}
	}
	rc, err := loadRuntimeConfig(b)
	if err != nil {
		return fmt.Errorf("parse runtime config: %w", err)
	}
	return h.set(rc)
}

func reloadLoop(ctx context.Context, h *configHolder, path string, period time.Duration, logger log.Logger) {
	var last []byte
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		b, err := os.ReadFile(path)
		if err != nil || string(b) == string(last) {
			continue
		}
		last = b
		rc, err := loadRuntimeConfig(b)
		if err == nil {
			err = h.set(rc)
		}
		if err != nil {
			level.Warn(logger).Log("msg", "runtime config reload failed", "err", err)
			continue
		}
		level.Info(logger).Log("msg", "runtime config reloaded")
	}
}

// configHandler serves the current runtime config on GET and replaces it on PUT/POST (YAML or
// JSON body, merged on top of the defaults).
func configHandler(h *configHolder) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/yaml")
			b, err := marshalYAML(h.get().cfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, _ = w.Write(b)
		case http.MethodPut, http.MethodPost:
			b, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			rc, err := loadRuntimeConfig(b)
			if err == nil {
				err = h.set(rc)
			}
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			_, _ = io.WriteString(w, "ok\n")
		default:
			http.Error(w, "use GET or PUT", http.StatusMethodNotAllowed)
		}
	}
}
