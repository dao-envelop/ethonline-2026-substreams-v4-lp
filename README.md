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
                  └───────────────────  └──▶ map_positions        v4 ModifyLiquidity made by a manager
```

| Module | Kind | What it does |
|---|---|---|
| `map_raw_events` | map | Decodes every log matching one of our signatures. Deployments are filtered by the factory parameter; manager events are not yet filtered, because the registry is built from this module's own output. |
| `store_managers` | store, `set_if_not_exists` | Manager address → product type (3000 stable, 3001 volatile, 3002 open volatile). A manager is deployed once and never changes product, so a second write would be a bug rather than an update. |
| `map_events` | map | The same events, minus anything emitted by a contract the factory did not produce. |
| `map_positions` | map | `ModifyLiquidity` logs whose `sender` is a known manager: pool, range, signed liquidity delta, salt. |

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

## Building

Needs the Rust toolchain with the `wasm32-unknown-unknown` target (pinned in `rust-toolchain.toml`), the
`substreams` CLI, and an API key from [The Graph Market](https://thegraph.market/auth/signup) for
anything that talks to a hosted endpoint.

```bash
cargo build --target wasm32-unknown-unknown --release
substreams pack
substreams run -e mainnet.eth.streamingfast.io:443 map_events -s 25580292 -t +1000
```

Override the factory per network:

```bash
substreams run ... -p map_raw_events=0x8a56c6be755ac385395e96234b553db1b9b06bea   # arbitrum
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

## Not done yet

- `db_out` (SQL sink) and `graph_out` (subgraph entities). They are different sink contracts —
  `DatabaseChanges` and `EntityChanges` — and mixing them produces a pipeline that builds and emits
  rubbish, so each gets its own module.
- A `blockIndex` module. Managers are active in a small fraction of blocks and billing is per block
  processed, so this is the single biggest cost lever available. It lands once the toolchain is in place
  and the manifest can be validated by `substreams pack` rather than by reading.

## License

MIT.
