// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::key::{Key, KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl Block {
    fn get_first_key(&self) -> KeyVec {
        if self.offsets.is_empty() {
            return KeyVec::new();
        }
        let mut buf = &self.data[..];
        let key_len = buf.get_u16();
        let key_data = &buf[..key_len as usize];
        Key::from_vec(key_data.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_first_key(),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = BlockIterator::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        assert!(!self.is_valid(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        assert!(!self.is_valid(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_offset(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_offset(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0;
        let mut r = self.block.offsets.len() - 1;
        // find the leftmost position that the key is >= given key
        while l <= r {
            let mid_idx = l + (r - l) / 2;
            let offset = self.block.offsets[mid_idx];
            let mut entry = &self.block.data[offset as usize..];
            let key_len = entry.get_u16();
            let key_data = &entry[..key_len as usize];
            let key_slice = Key::from_slice(key_data);
            if key_slice >= key {
                r = mid_idx - 1;
            } else {
                l = mid_idx + 1;
            }
        }
        self.seek_to_offset(l);
    }

    fn seek_to_offset(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.idx = self.block.offsets.len();
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        self.idx = idx;
        let offset = self.block.offsets[idx];
        let mut entry = &self.block.data[offset as usize..];
        let key_len = entry.get_u16();
        let key_data = &entry[..key_len as usize];
        self.key = Key::from_vec(key_data.to_vec());
        entry.advance(key_len as usize);
        let value_len = entry.get_u16();
        self.value_range = (
            offset as usize + key_len as usize + SIZEOF_U16,
            offset as usize + key_len as usize + SIZEOF_U16 + value_len as usize,
        );
        entry.advance(value_len as usize);
    }
}
