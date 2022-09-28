//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      if (cmp(array_[i].first, key) == 0) {
        result->push_back(array_[i].second);
      }
    }
  }
  return !result->empty();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  size_t free_slot = -1;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      // 允许出现同样的 key 但是 value 不同的情况，但不能同 key 同 value
      if (cmp(array_[i].first, key) == 0 && array_[i].second == value) {
        return false;
      }
      // 不需要判断 occupied，这样数组元素才能重复利用
    } else if (free_slot == static_cast<size_t>(-1)) {
      // 不能找到就直接返回，因为后面可能还会出现重复的键值对
      free_slot = i;
    }
  }
  if (free_slot == static_cast<size_t>(-1)) {
    return false;
  }
  array_[free_slot].first = key;
  array_[free_slot].second = value;
  SetOccupied(free_slot);
  SetReadable(free_slot);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && IsOccupied(i)) {
      if (cmp(array_[i].first, key) == 0 && array_[i].second == value) {
        array_[i] = MappingType();
        // occupied 永远都是 1，不能改变
        RemoveAt(i);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CHAR_BIT_SIZE);
  readable_[char_pos] &= ~(1U << bit_pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CHAR_BIT_SIZE);
  return ((occupied_[char_pos] >> bit_pos) & 1) == 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CHAR_BIT_SIZE);
  occupied_[char_pos] |= (1U << bit_pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CHAR_BIT_SIZE);
  return ((readable_[char_pos] >> bit_pos) & 1) == 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  const auto [char_pos, bit_pos] = std::div(bucket_idx, CHAR_BIT_SIZE);
  readable_[char_pos] |= (1U << bit_pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t counter = 0;
  for (size_t i = 0; i < (BUCKET_ARRAY_SIZE - 1) / 8 + 1; i++) {
    char v = readable_[i];
    while (v) {
      v &= (v - 1);
      counter++;
    }
  }
  return counter;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
