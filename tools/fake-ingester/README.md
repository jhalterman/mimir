# fake-ingester

`fake-ingester` runs many fake ingesters in one process, so queriers can be load-tested against
thousands of ingesters (and partitions) without running real ones. It's the step 4 tool of the
querier ↔ ingester fan-out load test.

Each fake:

- Serves its own gRPC server on its own port (`-base-port` + index).
- Registers in the ingester instance ring (ACTIVE, heartbeat every `-heartbeat-period`) and owns one
  ACTIVE partition with stored tokens in its compartment's partition ring
  (`ingester-partitions-rc-<N>`). One CAS per ring key per heartbeat covers every fake in the process.
- Serves the read RPCs (`QueryStream`, `LabelNames`, `LabelValues`, `MetricsForLabelMatchers`,
  `LabelNamesAndValues`, `LabelValuesCardinality`, `ActiveSeries`, `MetricsMetadata`, `UserStats`,
  `AllUserStats`, `QueryExemplars`, `SearchLabelNames`, `SearchLabelValues`) and the gRPC health
  service. `Push` is not implemented.

## Keep the fakes away from the write path

Fakes register under their own KV prefix (default `fake-collectors/`). Point **only the queriers
under test** at it, with `-ingester.ring.prefix=fake-collectors/` and
`-ingester.partition-ring.prefix=fake-collectors/`. Distributors, real ingesters and block-builders
stay on the default prefix, so nothing writes to partitions nobody consumes.

The queriers under test also need a tenant shard size that covers every fake partition (for
example `ingestion_partitions_tenant_shard_size: 0`), because queriers only query the partitions in
a tenant's shuffle shard.

## Responses

Responses are synthetic and deterministic, built once per (selector, partition, chunk window) and
replayed from a byte-budgeted cache (`-response-cache-bytes`), with pre-encoded wire bytes:

- A selector's cell-wide series count comes from the size table (`sizes.table_file`, normalized
  selector → series) or, failing that, from `sizes.series_quantiles`, at a position chosen by a hash
  of the selector. It's split evenly over the partitions the selector reaches.
- Exact `__name__` selectors only return data from the compartment `CompartmentForMetric` maps the
  name to, so compartment targeting is exercised for real. Other selectors reach every compartment.
- Series are disjoint across partitions, identical between zones, and sorted.
- read-tee copies are recognized from their matchers: per-replica copies (`_amp{k}` suffix) and
  originals (`__amp__=""`) get one replica's data, heavy copies (`(?:_amp[0-9]+)?`) get
  `sizes.write_amplification` replicas' worth.
- Chunks are valid XOR chunks covering the request range, clamped to
  `sizes.query_ingesters_within`.
- Label APIs follow `labels`: shared labels return every value from every partition, the others are
  sharded across partitions.

## Latency and errors

`latency = (base(RPC, u) + per-series + per-chunk cost) × instance slowness × load`, where:

- `u` is drawn with a correlation (`latency.correlation`) across the partitions of one request, so
  an expensive request is expensive everywhere.
- A fraction of instances is persistently slow, and each instance has a periodic slow window.
- Past `latency.inflight_knee` requests in flight on one fake, a growing share of requests get an
  extra exponential delay: the tail grows while the median stays put, as real ingesters showed in
  step 3. Past `latency.inflight_limit`, requests are rejected.
- `QueryStream` splits its delay between the first series batch, later series batches and chunk
  batches. Waiting is a context-aware timer, so a cancelled or hedged request frees its slot at once.
- `errors` injects failures per RPC, and marks a fraction or a list of fakes unhealthy (failing
  health checks and every RPC).

Defaults are calibrated from the plan's step 1 and step 3 measurements. Everything under the runtime
config can change while running: edit the file (`-runtime-config.file`, re-read every
`-runtime-config.reload-period`) or `PUT` YAML to `/config`. `GET /config` shows the current values.

## Run

```
fake-ingester \
  -fakes=100 -first-partition-from-hostname -zone=zone-a -compartment=3 \
  -compartments.num=10 -partitions-per-compartment=350 \
  -ring.store=memberlist -partition-ring.store=memberlist \
  -memberlist.join=dns+memberlist-bridge.<namespace>.svc.cluster.local.:7946 \
  -memberlist.cluster-label=<cluster label> \
  -runtime-config.file=/etc/fake-ingester/runtime.yaml
```

`-first-partition-from-hostname` sets the first partition to the StatefulSet ordinal × `-fakes`, so
pod 3 with 100 fakes owns partitions 300–399. Add `-pods-per-compartment=N` to also derive the
compartment (ordinal / N), so one StatefulSet per zone covers every compartment. Metrics are on
`-http-listen` at `/metrics`.

## Size table

Build the size table from query-frontend `query stats` logs, one JSON object per line with `q`
(`param_query`), `fs` (`fetched_series_count`), `len` (`length`) and `n` (identical entries):

```
fake-ingester sizetable < samples.jsonl | gzip > size-table.json.gz
```

Each query's series are split evenly across its selectors and divided by its 24h time splits, and
the table keeps the weighted median per normalized selector. Shards (`__query_shard__`) and read-tee
rewrites are normalized away at lookup, and a sharded selector returns 1/n of its series. The
table holds one replica's data; heavy read-tee copies multiply it by `sizes.write_amplification`.
Query texts are internal, so keep the table out of git and ship it as a deploy-time file.
