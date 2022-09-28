//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  auto dir_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&this->directory_page_id_));
  dir_page->SetPageId(this->directory_page_id_);
  dir_page->IncrGlobalDepth();
  page_id_t page_id;
  buffer_pool_manager_->NewPage(&page_id);
  dir_page->SetBucketPageId(0, page_id);
  dir_page->SetLocalDepth(0, 1);
  buffer_pool_manager_->UnpinPage(page_id, false);
  buffer_pool_manager_->NewPage(&page_id);
  dir_page->SetBucketPageId(1, page_id);
  dir_page->SetLocalDepth(1, 1);
  buffer_pool_manager->UnpinPage(page_id, false);
  buffer_pool_manager_->UnpinPage(this->directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t global_depth_mask = dir_page->GetGlobalDepthMask();
  return Hash(key) & global_depth_mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t directory_index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(directory_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(this->directory_page_id_));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  bucket_page->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  return !static_cast<bool>(result->empty());
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  bool is_inserted = bucket_page->Insert(key, value, comparator_);
  // TODO 判断目录和 bucket 是否需要改变容量
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return is_inserted;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  bool is_deleted = bucket_page->Remove(key, value, comparator_);
  // TODO 判断目录和 bucket 是否需要改变容量
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  return is_deleted;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
