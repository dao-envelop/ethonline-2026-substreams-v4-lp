# Substreams for Uniswap v4 LP managers

Typed events, operator changes and position deltas for the class **"LP manager on top of Uniswap v4"** —
a contract that owns v4 liquidity on someone's behalf and lets an operator rebalance it.

Built during [ETHOnline 2026](https://github.com/dao-envelop/ethonline-2026-unisafe) for
[unisafe](https://unisafe.envelop.is), but the package is deliberately not about our addresses: the
manager registry is built from the **factory's own deployment event**, so pointing it at any deployment
of the same factory, on any chain, fills the registry by itself. The factory address is the only
parameter.

## Why this exists

A manager's own events cannot describe a position:

- `Allocated` carries a **leg count** and nothing else,
- no manager event carries token amounts,
- and for the volatile product nothing on chain links a position's **salt to its pool** — the salt is
  chosen by the caller and lives only in storage.

So the position model has to be built on Uniswap's own `ModifyLiquidity` logs, where pool, range, exact
signed liquidity delta and salt all appear in one place, and where the `sender` is the manager. A
recenter then decomposes for free into a negative row and a positive row under one salt in one
transaction, with no special case anywhere.

The pipeline this replaces — a Python `eth_getLogs` poller — cannot filter by address at all: its event
table has no address column, so a signature matches network-wide. That is why it carries an explicit
do-not-index list, and why generic signatures like `OperatorSet(address,bool)` were dangerous to index.
Here the filter is one map step against the registry.

## Module graph

```
  block ──▶ map_raw_events ──▶ store_managers ──▶ map_events      manager events, emitter verified
                  │                     │
                  │                     └──▶ map_positions        v4 ModifyLiquidity made by a manager
                  └──▶ index_events                               block-filter keys
```

| Module | Kind | What it does |
|---|---|---|
| `map_raw_events` | map | Decodes every log matching one of our signatures. Deployments are filtered by the factory parameter; manager events are not yet filtered, because the registry is built from this module's own output. |
| `store_managers` | store, `set_if_not_exists` | Manager address → product type (3000 stable, 3001 volatile, 3002 open volatile). A manager is deployed once and never changes product, so a second write would be a bug rather than an update. |
| `map_events` | map | The same events, minus anything emitted by a contract the factory did not produce. |
| `map_positions` | map | `ModifyLiquidity` logs whose `sender` is a known manager: pool, range, signed liquidity delta, salt. |
| `index_events` | blockIndex | Keys a consumer can filter blocks by: `evt:<name>` and `mgr:<address>`. |
| `db_out` | map | `DatabaseChanges` for the Postgres sink — twelve tables, keyed by `(transaction_hash, log_index)`. |
| `store_position_totals` | store, `add` | Signed running liquidity per position plus its two fee counters, so a subgraph query does not fold every delta itself. |
| `store_position_open` | store, `set_if_not_exists` | Where and when a position began. The first `ModifyLiquidity` under a salt wins, so "opened at" survives every recenter without a special case. |
| `graph_out` | map | `EntityChanges` for a Substreams-powered subgraph — the model in [`schema.graphql`](./schema.graphql). |

Every amount is a **decimal string of base units**. `uint256` fits no protobuf integer and a float would
silently round — the same choice the existing Envelop history API made, for the same reason.

## Events decoded

| Event | Source | Note |
|---|---|---|
| `EnvelopV2Deployment` | factory | The only event that says what product a manager is: `EnvelopV2OracleType` is emitted by the implementation's constructor, not by the clone. |
| `Initialized` | manager | Owner, pool manager, pool count. |
| `OperatorSet` | manager | Also emitted with `allowed=false` for every operator when the ownership NFT is transferred. |
| `PriceOracleSet` | manager | Zero address means unset, and operator swaps then fail closed. |
| `Allocated` | manager | Leg count only — the amounts are in `PositionDelta`. |
| `Recentered` | manager | Liquidity is the **absolute** amount after the move, not a delta. |
| `LiquidityMoved` | manager | Cross-pool move. New in the implementation shipped during ETHOnline 2026; managers deployed before it never emit it. |
| `FeesCollected` | manager | Gross, before the protocol skim. Emitted by the claim path and, since the same change, by **every** pull — removing liquidity realises fees whether or not the caller asked. |
| `Reinvested`, `WithdrawnTo`, `ProtocolFeeTaken` | manager | |
| `ModifyLiquidity` | Uniswap v4 `PoolManager` | The position model. |

## Composition: a published index decides which blocks we open

The package imports [`ethereum-common`](https://substreams.dev/packages/ethereum-common/v0.3.3)
(StreamingFast) and uses its `index_events` module as a **block filter** on the two modules that read
raw blocks. That module keys every block by the event signatures and contract addresses it contains, so
a block holding none of our eleven signatures is never opened by `map_raw_events`, and one with no
`ModifyLiquidity` is never opened by `map_positions`.

This is the largest cost lever the platform offers — billing is per block processed, and these managers
are active in a tiny fraction of blocks. Measured on Unichain over the 30-block window that contains a
manager's creation: **32 processed blocks with the filter against 61 without**, for identical output. On
a backfill, where almost every block is empty of ours, the ratio is not close.

The filter lists **every** signature the decoder handles. A superset would be harmless — anything extra
is dropped in the map — but a missing one would silently skip blocks that hold our data, which is the
one way a block filter can be wrong.

This package is itself published to the registry: **[`envelop-lp-v4`](https://substreams.dev/packages/envelop-lp-v4)**,
so it can be imported the same way by anyone else.

## Two sinks, one decoder

The same modules feed two Graph products, which is the point rather than a convenience:

* **`db_out` → Postgres**, through `substreams-sink-sql`. Eleven event tables plus `position_delta`,
  matching the schema Envelop's existing indexer already serves in production, so rows from the two can
  be compared one against the other.
* **`graph_out` → a Substreams-powered subgraph**, deployed from [`subgraph.yaml`](./subgraph.yaml)
  against [`schema.graphql`](./schema.graphql).

The subgraph is deliberately **not** the SQL shape. A table dump is what a sink wants; a subgraph is
queried by people and by agents, so it gets a model: `Manager`, `Operator` (the current answer to "who
may act", already folded, because transferring the manager NFT revokes every operator at once),
`Position` with running liquidity and lifetime fees, an immutable `PositionDelta`, and one
`ManagerEvent` timeline covering all eleven event types.

Two decisions inside `graph_out` are worth knowing:

* **A manager is created once.** `EnvelopV2Deployment` and the clone's `Initialized` are two events in
  one transaction, and they are merged into a single entity change rather than a create followed by an
  update that would have to race it. `PriceOracleSet` can arrive months later, so that one is an update.
* **`openedAtBlock` comes from a store, not from the delta being processed.** For every log after the
  first, the current block is when the position *moved*, not when it began.

The entity types are generated from a local copy of the upstream proto
([`proto/sf/substreams/sink/entity/v1/entity.proto`](./proto/sf/substreams/sink/entity/v1/entity.proto)),
not from the `substreams-entity-change` crate: that crate's current release is built against
`substreams` 0.6 while this package is on 0.7, and linking both under `lto = true` fails to build at all.
The manifest still **imports the official `.spkg`**, so the descriptor a consumer reads is the canonical
one — the local copy exists only so Rust has types.

### Deploying the subgraph — and where that road now ends

```bash
substreams pack                  # refresh the .spkg the datasource points at
graph auth <deploy key>          # from Subgraph Studio
graph deploy <subgraph slug>
```

That is the documented route, and as of **8 September 2026 Subgraph Studio refuses it**:

> Substreams-powered Subgraphs, originally intended for non-EVM chains, are no longer supported. If you
> need help migrating to standalone Substreams, please reach out in the #substreams channel on Discord.

The build and the IPFS upload succeed; the rejection comes from the node. So `graph_out` and
[`schema.graphql`](./schema.graphql) stay in the package — the module is written, tested against a live
chain, and costs nothing unless a consumer asks for it — but the hosted deployment of it is closed. Any
graph-node that still accepts a substreams datasource will take this package as it is; The Graph's own
hosting will not, and this README says so rather than leaving a reader to find out at deploy time.

One deployment per chain either way: `network:` in `subgraph.yaml` and the factory parameter in
`substreams.yaml` change together.

## Building

Needs the Rust toolchain with the `wasm32-unknown-unknown` target (pinned in `rust-toolchain.toml`), the
`substreams` CLI, and an API key from [The Graph Market](https://thegraph.market/auth/signup) for
anything that talks to a hosted endpoint.

```bash
cargo build --target wasm32-unknown-unknown --release
substreams pack
substreams run -e mainnet.eth.streamingfast.io:443 map_events -s 25580292 -t +1000
```

Verified with `substreams` 1.22.0, `rustc` 1.98.1 and `protoc` 36.1: the package builds and packs with
no warnings, and `substreams info` lists all five modules.

### Verified against a live chain

Run against Arbitrum One, and cross-checked against the Envelop history API — the production indexer this
package is meant to eventually replace — on the same manager,
[`0x60723973…264b`](https://arbiscan.io/address/0x60723973ABF3BBC2ce7EB4400B728390D55e264b):

| What | Result |
|---|---|
| `map_raw_events` at block 487,466,603 | decoded the manager's `OperatorSet`: operator `0xd5228c94…`, `allowed: true` |
| `map_positions` at block 486,482,005 | decoded the first position: pool `0x70bf44c3…`, salt `0x8cd1b9d4…`, range `[58920, 70920]`, liquidity `+717932` |

Pool, salt and timestamp match the oracle's record of that position exactly, and the delta is positive
because it is an open. The `emitter` is the v4 `PoolManager` while the `manager` is the `sender` — which
is the whole reason positions are read from Uniswap's logs rather than the manager's own.

`graph_out` was checked the same way, on Unichain — the chain this package indexes from the head
alongside mainnet, while Arbitrum stays on the Envelop oracle because a block quota and 0.25-second
blocks are bad arithmetic:

| Block | Emitted |
|---:|---|
| 54,551,071 | `Manager` `0x9f7e19b7…` — deployment and `Initialized` merged into one entity: product `stable`, owner, pool manager, pool count — plus two `ManagerEvent` rows |
| 54,572,737 | first allocate: `Position` (range `[-89, 111]`, liquidity from the store), its `PositionDelta`, and a `ManagerEvent` of kind `allocated` |

Both runs started at the package's own `initialBlock` for that chain rather than mid-history, so the
stores needed no hosted backfill beyond the 21.7k blocks between the two events — 63k processed blocks
in total for the second one.

Backfilling the store from the factory's first block to that point processed ~340k blocks. Results are
cached, so later runs over the same range are free; the CLI also refuses to process more than 10,000
blocks unless `--limit-processed-blocks` says otherwise, which is a useful guard against an accidental
full-chain backfill.

Switch networks with `--network`; the manifest carries the factory address and the first block for each,
so there is one manifest rather than five copies of it:

```bash
substreams run -e arb-one.streamingfast.io:443 --network arbitrum-one \
  ./envelop-lp-v4-v0.2.3.spkg map_positions -s 486481990 -t +40
```

| Chain | Factory | First block |
|---|---|---:|
| 1 mainnet | `0x75e5d72D6971221b6332AaE8F59759d4Ba366dd0` | 25,580,292 |
| 130 unichain | `0x62D51DFF0c264a5aF8452A10789E4C98b7413A3c` | 53,877,933 |
| 1301 unichain-sepolia | `0xF813Bdc4de2658e2bC7Dd2c4afdeC4846Cfa7986` | 57,770,905 |
| 8453 base | `0x7A3c8F45b809078da58d17fb6Cd059334622838F` | 48,918,859 |
| 42161 arbitrum | `0x8A56c6be755aC385395E96234b553DB1B9B06bEa` | 486,143,228 |

`initialBlock` in the manifest is pinned to the mainnet factory. Pointing at another chain means moving
it too — a genesis pin turns every cold start into a full-chain backfill, because the store has to catch
up from `initialBlock` each time.

Note what this means for a start flag: `-s <recent block>` does **not** buy a saving. The stores still
catch up from `initialBlock` behind it, and those blocks are billed. Where a chain is indexed from is a
manifest decision, not a command-line one.

Both chains are indexed **from their factory block**. Unichain briefly was not — a 4.2M-block catch-up
on a one-second chain looked like more than a free tier's monthly quota. The block filter above removed
the reason: at the measured ratio that history costs on the order of 85k processed blocks, so there is
no longer anything to buy by starting late, and the index covers every manager the factory ever made.

## Filtering blocks, and why it matters

Billing is **per block processed**, and these managers are active in a tiny fraction of blocks. On a
chain with sub-second blocks that is the difference between a backfill that fits inside a free tier and
one that does not: Arbitrum alone is ~15.5M blocks of history and ~345k more per day.

`index_events` emits two key families — `evt:<name>` and `mgr:<address>` — so a consumer can declare a
`blockFilter` and have the engine skip everything else:

```yaml
  - name: my_consumer
    kind: map
    blockFilter:
      module: index_events
      query:
        params: true
    inputs:
      - params: string
      - map: map_events
```

Not wired into this package's own modules yet: which query is right depends on what the consumer wants,
and a hardcoded one would be wrong for everyone else.

## SQL sink

`schema.sql` in this repository is the schema — twelve tables, eleven event types plus `position_delta`.
Row identity is `(transaction_hash, log_index)`, the log's position in the chain, which is what makes a
replay idempotent: re-ingesting a block writes the same rows over the same keys rather than duplicating
them. Amounts land in `numeric(78,0)`, passed as decimal strings, because anything narrower cannot hold a
uint256 and any float rounds it silently.

The sink ships inside the CLI; the standalone `substreams-sink-sql` binary is deprecated.

```bash
export SUBSTREAMS_SINK_DSN="postgres://user:pass@host:5432/db?sslmode=require"
substreams sink postgres setup ./envelop-lp-v4-v0.2.3.spkg   # bookkeeping tables + schema.sql
substreams sink postgres       ./envelop-lp-v4-v0.2.3.spkg   # runs; no `run` subcommand
```

`setup` creates its own `cursors` and `substreams_history` tables alongside ours — that is how it resumes
and how it unwinds reorgs. Do not create them by hand and do not apply `schema.sql` yourself: `setup`
does it, and it fails against objects that already exist.

Note `postgresql://` is rejected as a DSN scheme even though `psql` accepts it. Use `postgres://` or
`psql://`.

Two things worth knowing before editing the manifest:

- **Do not import the SQL protodefs package.** The sink `Service` type is already inside the CLI, and
  importing it as well collides: *name conflict over `sf.substreams.sink.sql.v1.Service`*. Only the
  `DatabaseChanges` protos are imported here.
- **Changing `imports` invalidates the cache.** Module hashes cover the package's proto definitions, so
  adding the import made a 338k-block store backfill run again from scratch. Cheap to discover, less
  cheap to discover on a big chain.

## Not done yet

- `graph_out` (subgraph entities). A different sink contract — `EntityChanges`, not `DatabaseChanges` —
  and mixing the two produces a pipeline that builds and writes rubbish, so it gets its own module.

## License

MIT.
