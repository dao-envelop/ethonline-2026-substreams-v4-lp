//! The few entity-change setters `graph_out` needs.
//!
//! Upstream ships `substreams-entity-change` for this, but its current release is built against
//! `substreams` 0.6 while this package is on 0.7: its `ToValue` impls are for the other crate's
//! `BigInt`, and linking both versions under `lto = true` fails to build at all. The wire format is
//! what matters and it is small, so it is written out here instead of pinning this package to an old
//! runtime for the sake of a helper.
//!
//! One behaviour is worth knowing: `create_row` and `update_row` return the *same* row when called
//! twice for one entity in one block. Events arrive as parallel lists — a manager's deployment and its
//! initialization are two lists but one transaction — and merging them into a single change is both
//! cheaper and safer than emitting a create that a later update in the same block has to repair.

use std::collections::HashMap;

use crate::pb::entity::{
    entity_change::Operation, value::Typed, EntityChange, EntityChanges, Field, Value,
};

pub struct Tables {
    rows: Vec<EntityChange>,
    index: HashMap<(String, String), usize>,
}

impl Tables {
    pub fn new() -> Self {
        Tables { rows: Vec::new(), index: HashMap::new() }
    }

    pub fn create_row(&mut self, entity: &str, id: impl AsRef<str>) -> &mut EntityChange {
        self.row(entity, id.as_ref(), Operation::Create)
    }

    /// An update to an entity written in an earlier block. Within one block it lands on the row already
    /// created, so the operation stays a create and graph-node never sees an update for something that
    /// does not exist yet.
    pub fn update_row(&mut self, entity: &str, id: impl AsRef<str>) -> &mut EntityChange {
        self.row(entity, id.as_ref(), Operation::Update)
    }

    fn row(&mut self, entity: &str, id: &str, op: Operation) -> &mut EntityChange {
        let key = (entity.to_string(), id.to_string());
        let at = *self.index.entry(key).or_insert_with(|| {
            self.rows.push(EntityChange {
                entity: entity.to_string(),
                id: id.to_string(),
                ordinal: 0,
                operation: op as i32,
                fields: Vec::new(),
            });
            self.rows.len() - 1
        });
        &mut self.rows[at]
    }

    pub fn to_entity_changes(self) -> EntityChanges {
        EntityChanges { entity_changes: self.rows }
    }
}

/// What a field can hold. `Bytes` is a hex string on the wire despite its name, which is why an address
/// or a hash goes in as the 0x-prefixed string we already carry.
pub trait ToValue {
    fn to_value(self) -> Value;
}

fn typed(t: Typed) -> Value {
    Value { typed: Some(t) }
}

impl ToValue for String {
    fn to_value(self) -> Value {
        typed(Typed::String(self))
    }
}

impl ToValue for &str {
    fn to_value(self) -> Value {
        typed(Typed::String(self.to_string()))
    }
}

impl ToValue for i32 {
    fn to_value(self) -> Value {
        typed(Typed::Int32(self))
    }
}

impl ToValue for bool {
    fn to_value(self) -> Value {
        typed(Typed::Bool(self))
    }
}

pub trait Row {
    fn set<T: ToValue>(&mut self, name: &str, value: T) -> &mut Self;
    /// An address, hash or 32-byte key. `Bytes` is a hex string on the wire despite the name, so the
    /// 0x-prefixed value we already carry goes in unchanged — but it must be typed, or graph-node
    /// rejects a `Bytes!` column that arrives as a plain string.
    fn set_bytes(&mut self, name: &str, value: &str) -> &mut Self;
    fn set_bigint(&mut self, name: &str, value: &str) -> &mut Self;
    /// A number that may not have parsed upstream. An empty or malformed string becomes zero rather
    /// than a field graph-node rejects mid-backfill: the row is worth more than the one value.
    fn set_bigint_or_zero(&mut self, name: &str, value: &str) -> &mut Self;
}

impl Row for EntityChange {
    fn set<T: ToValue>(&mut self, name: &str, value: T) -> &mut Self {
        self.field(name, value.to_value())
    }

    fn set_bytes(&mut self, name: &str, value: &str) -> &mut Self {
        self.field(name, typed(Typed::Bytes(value.to_string())))
    }

    fn set_bigint(&mut self, name: &str, value: &str) -> &mut Self {
        self.field(name, typed(Typed::Bigint(value.to_string())))
    }

    fn set_bigint_or_zero(&mut self, name: &str, value: &str) -> &mut Self {
        let ok = !value.is_empty()
            && value.strip_prefix('-').unwrap_or(value).chars().all(|c| c.is_ascii_digit());
        self.set_bigint(name, if ok { value } else { "0" })
    }
}

trait SetField {
    fn field(&mut self, name: &str, value: Value) -> &mut Self;
}

impl SetField for EntityChange {
    /// Last write wins for a name already present: the callers below set a field at most once per row,
    /// and a duplicate would otherwise reach graph-node as two entries for one column.
    fn field(&mut self, name: &str, value: Value) -> &mut Self {
        let f = Field { name: name.to_string(), new_value: Some(value), old_value: None };
        match self.fields.iter_mut().find(|x| x.name == name) {
            Some(existing) => *existing = f,
            None => self.fields.push(f),
        }
        self
    }
}
