// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::PartitionKey;
use lsm_tree::{UserKey, UserValue, ValueType};

///
/// Inner Item
///
pub enum CompactItem<K, V> {
    /// Value
    Value {
        /// Key
        key: K,
        /// Value
        value: V,
    },
    /// Tombstone
    Tombstone(K),
    /// Weak tombstone
    WeakTombstone(K),
}

impl<K: Ord, V> Ord for CompactItem<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(other.key())
    }
}

impl<K: Ord, V> PartialOrd for CompactItem<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Eq, V> PartialEq for CompactItem<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl<K: Eq, V> Eq for CompactItem<K, V> {}

impl<K, V> CompactItem<K, V> {
    fn key(&self) -> &K {
        match self {
            Self::Value { key, .. } | Self::Tombstone(key) | Self::WeakTombstone(key) => key,
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Item {
    /// Partition key - an arbitrary byte array
    ///
    /// Supports up to 2^8 bytes
    pub partition: PartitionKey,

    /// User-defined key - an arbitrary byte array
    ///
    /// Supports up to 2^16 bytes
    pub key: UserKey,

    /// User-defined value - an arbitrary byte array
    ///
    /// Supports up to 65535 bytes
    pub value: UserValue,

    /// Tombstone marker - if this is true, the value has been deleted
    pub value_type: ValueType,
}

impl std::fmt::Debug for Item {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{:?}:{} => {:?}",
            self.partition,
            self.key,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
                ValueType::WeakTombstone => "W",
            },
            self.value
        )
    }
}

impl Item {
    ///
    /// New item
    ///
    pub fn new<P: Into<PartitionKey>, K: Into<UserKey>, V: Into<UserValue>>(
        partition: P,
        key: K,
        value: V,
        value_type: ValueType,
    ) -> Self {
        let p = partition.into();
        let k = key.into();
        let v = value.into();

        assert!(!p.is_empty());
        assert!(!k.is_empty());

        assert!(u8::try_from(p.len()).is_ok(), "Partition name too long");
        assert!(
            u16::try_from(k.len()).is_ok(),
            "Keys can be up to 65535 bytes long"
        );
        assert!(
            u32::try_from(v.len()).is_ok(),
            "Values can be up to 2^32 bytes long"
        );

        Self {
            partition: p,
            key: k,
            value: v,
            value_type,
        }
    }
}
