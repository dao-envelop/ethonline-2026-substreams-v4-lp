-- Schema for the Substreams SQL sink of https://github.com/dao-envelop/ethonline-2026-substreams-v4-lp
--
-- Applied by `substreams-sink-sql setup`, which also creates its own bookkeeping tables (`cursors`,
-- `substreams_history`) — those are the sink's, not ours, and are how it resumes and unwinds reorgs.
-- Do not declare or edit them here.
--
-- One schema per network (eth, arb, base, uni) rather than a chain_id column: the sink runs one process
-- per network with its own cursor, and separate schemas keep "whose block 486482005 is this" from ever
-- being a question. Set the schema per sink through the DSN (`?options=--search_path%3Darb`) or by
-- running setup once per schema.
--
-- Conventions, all inherited from the existing Envelop LP schema so the two can be compared row by row:
--   * amounts and liquidity are numeric(78,0) — uint256/int256 do not fit any narrower type, and a
--     float would silently round;
--   * addresses are lowercase varchar(42), hashes and 32-byte keys varchar(66), both 0x-prefixed;
--   * block_timestamp is unix seconds as bigint, not timestamptz: it is what the chain reports, and
--     converting on write hides which chains have no reliable clock feed;
--   * identity is (transaction_hash, log_index) — the position of the log in the chain. That is what
--     makes a replay idempotent: re-ingesting a block writes the same rows over the same keys.

-- ─────────────────────────── manager lifecycle ───────────────────────────

-- A manager clone was created. The only place that says which product a manager is: EnvelopV2OracleType
-- is emitted by the implementation's constructor, not by the clone.
CREATE TABLE IF NOT EXISTS manager_deployed (
    transaction_hash varchar(66)  NOT NULL,
    log_index        integer      NOT NULL,
    block_number     bigint       NOT NULL,
    block_timestamp  bigint       NOT NULL,
    manager          varchar(42)  NOT NULL,
    implementation   varchar(42)  NOT NULL,
    oracle_type      integer      NOT NULL, -- 3000 stable, 3001 volatile, 3002 open volatile
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS manager_deployed_manager_idx ON manager_deployed (manager);

CREATE TABLE IF NOT EXISTS manager_initialized (
    transaction_hash varchar(66)  NOT NULL,
    log_index        integer      NOT NULL,
    block_number     bigint       NOT NULL,
    block_timestamp  bigint       NOT NULL,
    manager          varchar(42)  NOT NULL,
    owner            varchar(42)  NOT NULL,
    pool_manager     varchar(42)  NOT NULL,
    pool_count       integer      NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS manager_initialized_manager_idx ON manager_initialized (manager);

-- Who may act on a manager. Also emitted with allowed=false for every operator when the ownership NFT
-- is transferred, so the current set is an ordered fold over (block_number, log_index) — never a
-- last-row-wins guess.
CREATE TABLE IF NOT EXISTS operator_set (
    transaction_hash varchar(66)  NOT NULL,
    log_index        integer      NOT NULL,
    block_number     bigint       NOT NULL,
    block_timestamp  bigint       NOT NULL,
    manager          varchar(42)  NOT NULL,
    operator         varchar(42)  NOT NULL,
    allowed          boolean      NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS operator_set_manager_idx  ON operator_set (manager, block_number, log_index);
CREATE INDEX IF NOT EXISTS operator_set_operator_idx ON operator_set (operator);

-- The oracle that gates operator swaps. The zero address means unset, and an operator swap then fails
-- closed — so an absent row and a zero row mean different things.
CREATE TABLE IF NOT EXISTS price_oracle_set (
    transaction_hash varchar(66)  NOT NULL,
    log_index        integer      NOT NULL,
    block_number     bigint       NOT NULL,
    block_timestamp  bigint       NOT NULL,
    manager          varchar(42)  NOT NULL,
    oracle           varchar(42)  NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS price_oracle_set_manager_idx ON price_oracle_set (manager, block_number);

-- ─────────────────────────── position operations ───────────────────────────

-- Carries a leg count and nothing else. Useful only to mark that an allocation happened in this
-- transaction; the amounts and the salt→pool link are in position_delta.
CREATE TABLE IF NOT EXISTS allocated (
    transaction_hash varchar(66)  NOT NULL,
    log_index        integer      NOT NULL,
    block_number     bigint       NOT NULL,
    block_timestamp  bigint       NOT NULL,
    manager          varchar(42)  NOT NULL,
    legs             integer      NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS allocated_manager_idx ON allocated (manager, block_number);

-- A position moved to a new range inside the same pool. `liquidity` is the ABSOLUTE amount after the
-- move, not a delta — the deltas of the same operation are two rows in position_delta.
CREATE TABLE IF NOT EXISTS recentered (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    manager          varchar(42)   NOT NULL,
    salt             varchar(66)   NOT NULL,
    new_tick_lower   integer       NOT NULL,
    new_tick_upper   integer       NOT NULL,
    liquidity        numeric(78,0) NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS recentered_salt_idx ON recentered (manager, salt, block_number);

-- A position moved to ANOTHER pool. Introduced during ETHOnline 2026; managers deployed before that
-- implementation never emit it, so an empty table is a real answer, not a gap.
CREATE TABLE IF NOT EXISTS liquidity_moved (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    manager          varchar(42)   NOT NULL,
    from_salt        varchar(66)   NOT NULL,
    to_salt          varchar(66)   NOT NULL,
    liquidity_pulled numeric(78,0) NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS liquidity_moved_from_idx ON liquidity_moved (manager, from_salt, block_number);
CREATE INDEX IF NOT EXISTS liquidity_moved_to_idx   ON liquidity_moved (manager, to_salt, block_number);

-- Fees realised, gross, before the protocol skim. Emitted by the claim path and — since the same change
-- that added liquidity_moved — by every pull as well, because removing liquidity realises fees whether
-- or not the caller asked for them. Summing this table is how lifetime fees are computed.
CREATE TABLE IF NOT EXISTS fees_collected (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    manager          varchar(42)   NOT NULL,
    salt             varchar(66)   NOT NULL,
    fees0            numeric(78,0) NOT NULL,
    fees1            numeric(78,0) NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS fees_collected_salt_idx ON fees_collected (manager, salt, block_number);

CREATE TABLE IF NOT EXISTS reinvested (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    manager          varchar(42)   NOT NULL,
    salt             varchar(66)   NOT NULL,
    added_liquidity  numeric(78,0) NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS reinvested_salt_idx ON reinvested (manager, salt, block_number);

-- The owner-only drain. Keyed by recipient and currency, not by position — a withdrawal can be sourced
-- from several positions at once, and which ones is visible in position_delta of the same transaction.
CREATE TABLE IF NOT EXISTS withdrawn_to (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    manager          varchar(42)   NOT NULL,
    recipient        varchar(42)   NOT NULL,
    currency         varchar(42)   NOT NULL,
    amount           numeric(78,0) NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS withdrawn_to_manager_idx ON withdrawn_to (manager, block_number);

CREATE TABLE IF NOT EXISTS protocol_fee_taken (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    manager          varchar(42)   NOT NULL,
    currency         varchar(42)   NOT NULL,
    amount           numeric(78,0) NOT NULL,
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS protocol_fee_taken_manager_idx ON protocol_fee_taken (manager, block_number);

-- ─────────────────────────── positions ───────────────────────────

-- One Uniswap v4 ModifyLiquidity log made by a known manager. This is the position model, and it has to
-- be: the manager's own events cannot describe a position — Allocated carries a leg count, no manager
-- event carries amounts, and for the volatile product nothing on chain links a salt to its pool.
--
-- One row per log, deliberately not an aggregated position. A recenter is a removal and an addition
-- under one salt in one transaction, so it decomposes into a negative row and a positive row with no
-- special case; current liquidity is the sum over (manager, salt).
--
-- `emitter` is the v4 PoolManager; `manager` is the log's `sender`.
CREATE TABLE IF NOT EXISTS position_delta (
    transaction_hash varchar(66)   NOT NULL,
    log_index        integer       NOT NULL,
    block_number     bigint        NOT NULL,
    block_timestamp  bigint        NOT NULL,
    emitter          varchar(42)   NOT NULL,
    manager          varchar(42)   NOT NULL,
    pool_id          varchar(66)   NOT NULL,
    salt             varchar(66)   NOT NULL,
    tick_lower       integer       NOT NULL,
    tick_upper       integer       NOT NULL,
    liquidity_delta  numeric(78,0) NOT NULL, -- negative on removal
    PRIMARY KEY (transaction_hash, log_index)
);
CREATE INDEX IF NOT EXISTS position_delta_salt_idx    ON position_delta (manager, salt, block_number, log_index);
CREATE INDEX IF NOT EXISTS position_delta_pool_idx    ON position_delta (pool_id, block_number);
CREATE INDEX IF NOT EXISTS position_delta_manager_idx ON position_delta (manager, block_number);
