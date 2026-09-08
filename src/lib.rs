//! Substreams for the class "LP manager on top of Uniswap v4".
//!
//! Not "index our contracts": the manager registry is built from the factory's own deployment event, so
//! the package works for any manager the factory produced, on any chain it is deployed to, without a
//! hardcoded address list. The factory address is the only parameter.
//!
//! Module graph:
//!
//! ```text
//!   block ──▶ map_raw_events ──▶ store_managers ──▶ map_events      (manager events, emitter verified)
//!                   │                     │
//!                   │                     └──▶ map_positions        (v4 ModifyLiquidity by a manager)
//!                   └──▶ index_events                               (block filter keys, cost lever)
//! ```

mod abi;
mod entity;

mod pb {
    pub mod envelop {
        pub mod lp {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/envelop.lp.v1.rs"));
            }
        }
    }
    pub mod entity {
        include!(concat!(env!("OUT_DIR"), "/sf.substreams.sink.entity.v1.rs"));
    }
}

use substreams::errors::Error;
use substreams_database_change::pb::sf::substreams::sink::database::v1::DatabaseChanges;
use substreams_database_change::tables::Tables;
use substreams::pb::sf::substreams::index::v1::Keys;
use std::str::FromStr;
use substreams::scalar::BigInt;
use substreams::store::{
    StoreAdd, StoreAddBigInt, StoreGet, StoreGetBigInt, StoreGetString, StoreNew, StoreSetIfNotExists,
    StoreSetIfNotExistsString,
};
use entity::{Row as _, Tables as EntityTables};
use pb::entity::EntityChanges;
use substreams::Hex;
use substreams_ethereum::pb::eth::v2 as eth;
use substreams_ethereum::Event;

use pb::envelop::lp::v1 as lp;

/// `0x`-prefixed lowercase, the form every consumer of ours already uses.
fn addr(bytes: &[u8]) -> String {
    format!("0x{}", Hex::encode(bytes))
}

fn meta(block: &eth::Block, tx_hash: &str, log: &eth::Log) -> lp::Meta {
    lp::Meta {
        block_number: block.number,
        block_timestamp: block
            .header
            .as_ref()
            .and_then(|h| h.timestamp.as_ref())
            .map(|t| t.seconds)
            .unwrap_or(0),
        transaction_hash: tx_hash.to_string(),
        log_index: log.block_index,
        emitter: addr(&log.address),
    }
}

// ─────────────────────────── 1. decode ───────────────────────────

/// Decode every log that matches one of our event signatures, without yet asking whether the contract
/// that emitted it is really one of ours — that question needs the registry, which is built from this
/// module's own output. `map_events` answers it one step later.
///
/// The factory is the exception: its address is the parameter, and a deployment from anywhere else is
/// not a deployment of ours, so it is filtered here.
#[substreams::handlers::map]
pub fn map_raw_events(factory: String, block: eth::Block) -> Result<lp::Events, Error> {
    let factory = factory.trim().to_lowercase();
    let mut out = lp::Events::default();

    for trx in block.transactions() {
        let tx_hash = format!("0x{}", Hex::encode(&trx.hash));

        for (log, _call) in trx.logs_with_calls() {
            let emitter = addr(&log.address);
            let m = || meta(&block, &tx_hash, log);

            if emitter == factory {
                if let Some(e) = abi::lp_factory::events::EnvelopV2Deployment::match_and_decode(log)
                {
                    out.manager_deployed.push(lp::ManagerDeployed {
                        meta: Some(m()),
                        manager: addr(&e.proxy),
                        implementation: addr(&e.implementation),
                        oracle_type: e.envelop_oracle_type.to_u64(),
                    });
                }
                continue; // the factory emits nothing else we index
            }

            if let Some(e) = abi::lp_manager::events::Initialized::match_and_decode(log) {
                out.manager_initialized.push(lp::ManagerInitialized {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    owner: addr(&e.owner),
                    pool_manager: addr(&e.pool_manager),
                    pool_count: e.pool_count.to_u64(),
                });
            } else if let Some(e) = abi::lp_manager::events::OperatorSet::match_and_decode(log) {
                out.operator_set.push(lp::OperatorSet {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    operator: addr(&e.operator),
                    allowed: e.allowed,
                });
            } else if let Some(e) = abi::lp_manager::events::PriceOracleSet::match_and_decode(log) {
                out.price_oracle_set.push(lp::PriceOracleSet {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    oracle: addr(&e.oracle),
                });
            } else if let Some(e) = abi::lp_manager::events::Allocated::match_and_decode(log) {
                out.allocated.push(lp::Allocated {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    legs: e.legs.to_u64(),
                });
            } else if let Some(e) = abi::lp_manager::events::Recentered::match_and_decode(log) {
                out.recentered.push(lp::Recentered {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    salt: addr(&e.salt),
                    new_tick_lower: e.new_tick_lower.to_i32(),
                    new_tick_upper: e.new_tick_upper.to_i32(),
                    liquidity: e.liquidity.to_string(),
                });
            } else if let Some(e) = abi::lp_manager::events::LiquidityMoved::match_and_decode(log) {
                out.liquidity_moved.push(lp::LiquidityMoved {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    from_salt: addr(&e.from_salt),
                    to_salt: addr(&e.to_salt),
                    liquidity_pulled: e.liquidity_pulled.to_string(),
                });
            } else if let Some(e) = abi::lp_manager::events::FeesCollected::match_and_decode(log) {
                out.fees_collected.push(lp::FeesCollected {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    salt: addr(&e.salt),
                    fees0: e.fees0.to_string(),
                    fees1: e.fees1.to_string(),
                });
            } else if let Some(e) = abi::lp_manager::events::Reinvested::match_and_decode(log) {
                out.reinvested.push(lp::Reinvested {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    salt: addr(&e.salt),
                    added_liquidity: e.added_liquidity.to_string(),
                });
            } else if let Some(e) = abi::lp_manager::events::WithdrawnTo::match_and_decode(log) {
                out.withdrawn_to.push(lp::WithdrawnTo {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    recipient: addr(&e.recipient),
                    currency: addr(&e.stable),
                    amount: e.amount.to_string(),
                });
            } else if let Some(e) = abi::lp_manager::events::ProtocolFeeTaken::match_and_decode(log)
            {
                out.protocol_fee_taken.push(lp::ProtocolFeeTaken {
                    meta: Some(m()),
                    manager: emitter.clone(),
                    currency: addr(&e.currency),
                    amount: e.amount.to_string(),
                });
            }
        }
    }

    Ok(out)
}

// ─────────────────────────── 2. registry ───────────────────────────

/// Every manager the factory ever produced, and which product it is. `set_if_not_exists` because a
/// manager is deployed once and its product never changes — a later write would be a bug, not an update.
///
/// This is what makes the package reusable rather than a script about our addresses: point it at any
/// deployment of the same factory and the registry fills itself.
#[substreams::handlers::store]
pub fn store_managers(events: lp::Events, store: StoreSetIfNotExistsString) {
    for d in events.manager_deployed {
        let ord = d.meta.as_ref().map(|m| m.block_number).unwrap_or(0);
        store.set_if_not_exists(ord, &d.manager, &d.oracle_type.to_string());
    }
}

// ─────────────────────────── 3. verified events ───────────────────────────

/// The same events, minus anything emitted by a contract the factory did not produce.
///
/// The filter is not cosmetic. `OperatorSet(address,bool)` is a generic signature that other protocols
/// use for unrelated things, so topic0 alone would happily import strangers' logs. The existing poller
/// this package replaces cannot make this distinction at all — its event table has no address column, so
/// a signature matches network-wide, which is why it carries an explicit do-not-index list. Here the
/// filter is one map step.
#[substreams::handlers::map]
pub fn map_events(raw: lp::Events, managers: StoreGetString) -> Result<lp::Events, Error> {
    let known = |m: &String| managers.get_last(m).is_some();

    Ok(lp::Events {
        manager_deployed: raw.manager_deployed,
        manager_initialized: raw
            .manager_initialized
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        operator_set: raw
            .operator_set
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        price_oracle_set: raw
            .price_oracle_set
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        allocated: raw
            .allocated
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        recentered: raw
            .recentered
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        liquidity_moved: raw
            .liquidity_moved
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        fees_collected: raw
            .fees_collected
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        reinvested: raw
            .reinvested
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        withdrawn_to: raw
            .withdrawn_to
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
        protocol_fee_taken: raw
            .protocol_fee_taken
            .into_iter()
            .filter(|e| known(&e.manager))
            .collect(),
    })
}

// ─────────────────────────── 4. positions ───────────────────────────

/// Positions, from Uniswap's own logs rather than from the manager's.
///
/// The manager's events cannot describe a position: `Allocated` carries a leg count, no manager event
/// carries amounts, and for the volatile product nothing on chain links a salt to a pool — the salt is
/// caller-chosen and lives only in storage. `ModifyLiquidity` has all four facts in one log: pool, range,
/// exact signed liquidity delta, salt. Its `sender` is the manager, which is how we recognise ours.
///
/// A recenter therefore decomposes for free into a negative row and a positive row under one salt in one
/// transaction, with no special case for it anywhere.
#[substreams::handlers::map]
pub fn map_positions(
    block: eth::Block,
    managers: StoreGetString,
) -> Result<lp::PositionDeltas, Error> {
    let mut out = lp::PositionDeltas::default();

    for trx in block.transactions() {
        let tx_hash = format!("0x{}", Hex::encode(&trx.hash));

        for (log, _call) in trx.logs_with_calls() {
            let Some(e) = abi::pool_manager::events::ModifyLiquidity::match_and_decode(log) else {
                continue;
            };
            let manager = addr(&e.sender);
            if managers.get_last(&manager).is_none() {
                continue;
            }
            out.deltas.push(lp::PositionDelta {
                meta: Some(meta(&block, &tx_hash, log)),
                manager,
                pool_id: addr(&e.id),
                salt: addr(&e.salt),
                tick_lower: e.tick_lower.to_i32(),
                tick_upper: e.tick_upper.to_i32(),
                liquidity_delta: e.liquidity_delta.to_string(),
            });
        }
    }

    Ok(out)
}

// ─────────────────────────── 5. block index ───────────────────────────

/// Keys a consumer can filter blocks by, so the engine skips blocks that cannot contain anything of
/// ours. This is the largest cost lever the platform offers: billing is per block processed, and these
/// managers are active in a tiny fraction of blocks — on a sub-second-block chain that is the difference
/// between a backfill that fits a free tier and one that does not.
///
/// Two key families, both cheap to emit and precise enough to be worth filtering on:
/// `mgr:<address>` for the contract that logged it, and `evt:<name>` for what happened.
#[substreams::handlers::map]
pub fn index_events(events: lp::Events) -> Result<Keys, Error> {
    let mut keys: Vec<String> = Vec::new();
    let mut push = |name: &str, manager: &str| {
        keys.push(format!("evt:{name}"));
        keys.push(format!("mgr:{manager}"));
    };

    for e in &events.manager_deployed {
        push("manager_deployed", &e.manager);
    }
    for e in &events.manager_initialized {
        push("manager_initialized", &e.manager);
    }
    for e in &events.operator_set {
        push("operator_set", &e.manager);
    }
    for e in &events.price_oracle_set {
        push("price_oracle_set", &e.manager);
    }
    for e in &events.allocated {
        push("allocated", &e.manager);
    }
    for e in &events.recentered {
        push("recentered", &e.manager);
    }
    for e in &events.liquidity_moved {
        push("liquidity_moved", &e.manager);
    }
    for e in &events.fees_collected {
        push("fees_collected", &e.manager);
    }
    for e in &events.reinvested {
        push("reinvested", &e.manager);
    }
    for e in &events.withdrawn_to {
        push("withdrawn_to", &e.manager);
    }
    for e in &events.protocol_fee_taken {
        push("protocol_fee_taken", &e.manager);
    }

    keys.sort();
    keys.dedup();
    Ok(Keys { keys })
}

// ─────────────────────────── 6. SQL sink ───────────────────────────

/// Rows for the Postgres sink, one table per message type plus `position_delta`.
///
/// Identity is `(transaction_hash, log_index)` — the log's position in the chain, not a synthetic id.
/// That is what makes a replay idempotent: re-ingesting a block writes the same rows over the same keys
/// instead of duplicating them. The column names and their order in the key must match `schema.sql`
/// exactly; a mismatch here builds fine and produces a table nobody can upsert into.
///
/// Numeric columns are passed as decimal strings into `numeric(78,0)`. Going through any float on the
/// way would round a uint256 silently, which is the one failure mode that never announces itself.
#[substreams::handlers::map]
pub fn db_out(events: lp::Events, positions: lp::PositionDeltas) -> Result<DatabaseChanges, Error> {
    let mut tables = Tables::new();

    // Row keys have to outlive the borrow, hence the explicit bindings before every create_row.
    for e in events.manager_deployed {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "manager_deployed",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("implementation", e.implementation)
            .set("oracle_type", e.oracle_type);
    }

    for e in events.manager_initialized {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "manager_initialized",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("owner", e.owner)
            .set("pool_manager", e.pool_manager)
            .set("pool_count", e.pool_count);
    }

    for e in events.operator_set {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "operator_set",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("operator", e.operator)
            .set("allowed", e.allowed);
    }

    for e in events.price_oracle_set {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "price_oracle_set",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("oracle", e.oracle);
    }

    for e in events.allocated {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "allocated",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("legs", e.legs);
    }

    for e in events.recentered {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "recentered",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("salt", e.salt)
            .set("new_tick_lower", e.new_tick_lower)
            .set("new_tick_upper", e.new_tick_upper)
            .set("liquidity", e.liquidity);
    }

    for e in events.liquidity_moved {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "liquidity_moved",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("from_salt", e.from_salt)
            .set("to_salt", e.to_salt)
            .set("liquidity_pulled", e.liquidity_pulled);
    }

    for e in events.fees_collected {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "fees_collected",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("salt", e.salt)
            .set("fees0", e.fees0)
            .set("fees1", e.fees1);
    }

    for e in events.reinvested {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "reinvested",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("salt", e.salt)
            .set("added_liquidity", e.added_liquidity);
    }

    for e in events.withdrawn_to {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "withdrawn_to",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("recipient", e.recipient)
            .set("currency", e.currency)
            .set("amount", e.amount);
    }

    for e in events.protocol_fee_taken {
        let m = e.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "protocol_fee_taken",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("manager", e.manager)
            .set("currency", e.currency)
            .set("amount", e.amount);
    }

    for d in positions.deltas {
        let m = d.meta.unwrap_or_default();
        let idx = m.log_index.to_string();
        tables
            .create_row(
                "position_delta",
                [
                    ("transaction_hash", m.transaction_hash.as_str()),
                    ("log_index", idx.as_str()),
                ],
            )
            .set("block_number", m.block_number)
            .set("block_timestamp", m.block_timestamp)
            .set("emitter", m.emitter)
            .set("manager", d.manager)
            .set("pool_id", d.pool_id)
            .set("salt", d.salt)
            .set("tick_lower", d.tick_lower)
            .set("tick_upper", d.tick_upper)
            .set("liquidity_delta", d.liquidity_delta);
    }

    Ok(tables.to_database_changes())
}

// ─────────────────────────── 7. the subgraph side ───────────────────────────

/// A decimal string from the protobuf, as a number. These strings are produced by our own decoder from
/// on-chain integers, so a parse failure is a bug rather than bad input — but a panic here would stop a
/// backfill dead, and a zero is visible in the totals it lands in.
fn number(s: &str) -> BigInt {
    BigInt::from_str(s).unwrap_or_else(|_| BigInt::zero())
}

/// The key both position stores agree on. Salt is unique per manager, not globally.
fn position_key(manager: &str, salt: &str) -> String {
    format!("{manager}:{salt}")
}

/// What a manager holds right now, per position, so a subgraph query does not have to fold the deltas
/// itself. Liquidity is a signed running sum — removals are negative rows — and the two fee counters
/// accumulate what every pull and claim realised.
///
/// Read in the same block it is written: a store input in `get` mode sees this block's writes, which is
/// why `graph_out` can report the total *including* the delta it is currently emitting.
#[substreams::handlers::store]
pub fn store_position_totals(events: lp::Events, positions: lp::PositionDeltas, store: StoreAddBigInt) {
    for d in &positions.deltas {
        store.add(0, format!("liq:{}", position_key(&d.manager, &d.salt)), number(&d.liquidity_delta));
    }
    for e in &events.fees_collected {
        let key = position_key(&e.manager, &e.salt);
        store.add(0, format!("fee0:{key}"), number(&e.fees0));
        store.add(0, format!("fee1:{key}"), number(&e.fees1));
    }
}

/// When a position was first seen, and in which pool. `set_if_not_exists` is the whole mechanism: the
/// first `ModifyLiquidity` under a salt wins and later ones cannot overwrite it, so "opened at" survives
/// every recenter without a special case.
///
/// The pool travels in the same value because a salt outlives its pool only through `moveLiquidity`, and
/// that case is better answered by the deltas themselves than by a store that would have to be mutable.
#[substreams::handlers::store]
pub fn store_position_open(positions: lp::PositionDeltas, store: StoreSetIfNotExistsString) {
    for d in &positions.deltas {
        let m = d.meta.clone().unwrap_or_default();
        store.set_if_not_exists(
            0,
            position_key(&d.manager, &d.salt),
            &format!("{}:{}:{}", m.block_number, m.block_timestamp, d.pool_id),
        );
    }
}

fn product_of(oracle_type: u64) -> &'static str {
    match oracle_type {
        3000 => "stable",
        3001 => "volatile",
        3002 => "openVolatile",
        _ => "unknown",
    }
}

/// Entities for a Substreams-powered subgraph — the second Graph product this package feeds, next to the
/// SQL sink, from the same modules.
///
/// The shape is deliberately not the SQL one. Postgres gets eleven event tables plus `position_delta`,
/// because that schema already exists in production and rows there are compared against it. A subgraph
/// is queried by people and agents, so it gets a small model instead: `Manager`, `Operator`, `Position`
/// with running totals, an immutable `PositionDelta`, and one `ManagerEvent` timeline covering every
/// event type — which is the query the frontend and the MCP service actually ask.
///
/// Manager is created once, from the deployment. `Initialized` is emitted by the clone in the same
/// transaction, so its fields are folded into that same create rather than an update that would race it;
/// `PriceOracleSet` can arrive much later and is therefore an update.
#[substreams::handlers::map]
pub fn graph_out(
    events: lp::Events,
    positions: lp::PositionDeltas,
    totals: StoreGetBigInt,
    opened: StoreGetString,
) -> Result<EntityChanges, Error> {
    let mut tables = EntityTables::new();

    let event_row = |tables: &mut EntityTables, m: &lp::Meta, manager: &str, kind: &str| -> String {
        let id = format!("{}-{}", m.transaction_hash, m.log_index);
        tables
            .create_row("ManagerEvent", &id)
            .set("manager", manager)
            .set("kind", kind)
            .set_bigint("block", &m.block_number.to_string())
            .set_bigint("timestamp", &m.block_timestamp.to_string())
            .set_bytes("transaction", &m.transaction_hash)
            .set("logIndex", m.log_index as i32);
        id
    };

    for e in &events.manager_deployed {
        let m = e.meta.clone().unwrap_or_default();
        let init = events.manager_initialized.iter().find(|i| i.manager == e.manager);
        let row = tables
            .create_row("Manager", &e.manager)
            .set_bytes("address", &e.manager)
            .set_bytes("implementation", &e.implementation)
            .set("product", product_of(e.oracle_type))
            .set("oracleType", e.oracle_type as i32)
            .set_bigint("createdAtBlock", &m.block_number.to_string())
            .set_bigint("createdAtTimestamp", &m.block_timestamp.to_string());
        if let Some(i) = init {
            row.set_bytes("owner", &i.owner)
                .set_bytes("poolManager", &i.pool_manager)
                .set("poolCount", i.pool_count as i32);
        }
        event_row(&mut tables, &m, &e.manager, "deployed");
    }

    for e in &events.manager_initialized {
        let m = e.meta.clone().unwrap_or_default();
        event_row(&mut tables, &m, &e.manager, "initialized");
    }

    for e in &events.operator_set {
        let m = e.meta.clone().unwrap_or_default();
        // One row per (manager, operator), rewritten on every change: an NFT transfer revokes every
        // operator in one block, and the fold that produces "who may act now" is the entity itself.
        tables
            .create_row("Operator", format!("{}-{}", e.manager, e.operator))
            .set("manager", e.manager.clone())
            .set_bytes("address", &e.operator)
            .set("authorized", e.allowed)
            .set_bigint("updatedAtBlock", &m.block_number.to_string())
            .set_bigint("updatedAtTimestamp", &m.block_timestamp.to_string());
        event_row(&mut tables, &m, &e.manager, "operatorSet");
    }

    for e in &events.price_oracle_set {
        let m = e.meta.clone().unwrap_or_default();
        tables.update_row("Manager", &e.manager).set_bytes("priceOracle", &e.oracle);
        event_row(&mut tables, &m, &e.manager, "priceOracleSet");
    }

    for e in &events.allocated {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "allocated");
        tables.update_row("ManagerEvent", &id).set("legs", e.legs as i32);
    }

    for e in &events.recentered {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "recentered");
        tables
            .update_row("ManagerEvent", &id)
            .set("position", format!("{}-{}", e.manager, e.salt))
            .set_bytes("salt", &e.salt)
            .set("tickLower", e.new_tick_lower)
            .set("tickUpper", e.new_tick_upper)
            .set_bigint_or_zero("liquidity", &e.liquidity);
    }

    for e in &events.liquidity_moved {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "liquidityMoved");
        tables
            .update_row("ManagerEvent", &id)
            .set("position", format!("{}-{}", e.manager, e.from_salt))
            .set_bytes("salt", &e.from_salt)
            .set_bytes("toSalt", &e.to_salt)
            .set_bigint_or_zero("liquidity", &e.liquidity_pulled);
    }

    for e in &events.fees_collected {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "feesCollected");
        tables
            .update_row("ManagerEvent", &id)
            .set("position", format!("{}-{}", e.manager, e.salt))
            .set_bytes("salt", &e.salt)
            .set_bigint_or_zero("amount0", &e.fees0)
            .set_bigint_or_zero("amount1", &e.fees1);
    }

    for e in &events.reinvested {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "reinvested");
        tables
            .update_row("ManagerEvent", &id)
            .set("position", format!("{}-{}", e.manager, e.salt))
            .set_bytes("salt", &e.salt)
            .set_bigint_or_zero("liquidity", &e.added_liquidity);
    }

    for e in &events.withdrawn_to {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "withdrawnTo");
        tables
            .update_row("ManagerEvent", &id)
            .set_bytes("recipient", &e.recipient)
            .set_bytes("currency", &e.currency)
            .set_bigint_or_zero("amount0", &e.amount);
    }

    for e in &events.protocol_fee_taken {
        let m = e.meta.clone().unwrap_or_default();
        let id = event_row(&mut tables, &m, &e.manager, "protocolFeeTaken");
        tables
            .update_row("ManagerEvent", &id)
            .set_bytes("currency", &e.currency)
            .set_bigint_or_zero("amount0", &e.amount);
    }

    for d in &positions.deltas {
        let m = d.meta.clone().unwrap_or_default();
        let key = position_key(&d.manager, &d.salt);
        let id = format!("{}-{}", d.manager, d.salt);

        // Opened-at comes from the store rather than from this delta: for every log after the first, this
        // block is when the position moved, not when it began.
        let (opened_block, opened_time) = match opened.get_last(&key) {
            Some(v) => {
                let mut parts = v.split(':');
                (
                    parts.next().and_then(|x| x.parse::<u64>().ok()).unwrap_or(m.block_number),
                    parts.next().and_then(|x| x.parse::<i64>().ok()).unwrap_or(m.block_timestamp),
                )
            }
            None => (m.block_number, m.block_timestamp),
        };

        tables
            .create_row("Position", &id)
            .set("manager", d.manager.clone())
            .set_bytes("salt", &d.salt)
            .set_bytes("pool", &d.pool_id)
            .set("tickLower", d.tick_lower)
            .set("tickUpper", d.tick_upper)
            .set_bigint("liquidity", &totals.get_last(format!("liq:{key}")).unwrap_or_else(BigInt::zero).to_string())
            .set_bigint("lifetimeFees0", &totals.get_last(format!("fee0:{key}")).unwrap_or_else(BigInt::zero).to_string())
            .set_bigint("lifetimeFees1", &totals.get_last(format!("fee1:{key}")).unwrap_or_else(BigInt::zero).to_string())
            .set_bigint("openedAtBlock", &opened_block.to_string())
            .set_bigint("openedAtTimestamp", &opened_time.to_string())
            .set_bigint("updatedAtBlock", &m.block_number.to_string())
            .set_bigint("updatedAtTimestamp", &m.block_timestamp.to_string());

        tables
            .create_row("PositionDelta", format!("{}-{}", m.transaction_hash, m.log_index))
            .set("position", id)
            .set("manager", d.manager.clone())
            .set_bytes("pool", &d.pool_id)
            .set("tickLower", d.tick_lower)
            .set("tickUpper", d.tick_upper)
            .set_bigint_or_zero("liquidityDelta", &d.liquidity_delta)
            .set_bigint("block", &m.block_number.to_string())
            .set_bigint("timestamp", &m.block_timestamp.to_string())
            .set_bytes("transaction", &m.transaction_hash)
            .set("logIndex", m.log_index as i32);
    }

    Ok(tables.to_entity_changes())
}
