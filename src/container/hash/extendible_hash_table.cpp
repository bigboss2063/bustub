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
  auto dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&this->directory_page_id_)->GetData());
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
  uint32_t global_depth_mask = dir_page->GetGlobalDepthMask();
  uint32_t directory_index = Hash(key) & global_depth_mask;
  return dir_page->GetBucketPageId(directory_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->FetchPage(this->directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->RLatch();
  bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return !static_cast<bool>(result->empty());
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->WLatch();
  // 插入成功，直接返回 true
  if (bucket_page->Insert(key, value, comparator_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    table_latch_.RUnlock();
    return true;
  }
  // （如果 bucket 没满，就代表是重复的 KV，直接返回 false）括号中的说法是有问题的！满了的话不代表里面没有重复的kv，
  // 后面还需要再检查一遍是否有重复的 kv
  if (!bucket_page->IsFull()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    return false;
  }
  if (bucket_page->IsRepeat(key, value, comparator_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    return false;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  table_latch_.RUnlock();
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto directory_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  if (bucket_page->Insert(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    table_latch_.WUnlock();
    return true;
  }

  if (!bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return false;
  }
  if (bucket_page->IsRepeat(key, value, comparator_)) {
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return false;
  }
  auto global_depth = dir_page->GetGlobalDepth();
  dir_page->IncrLocalDepth(directory_idx);
  // 如果 local_depth == global_depth 则执行 Directory Expansion 和 Bucket Split，否则只进行 Bucket Split
  if (dir_page->GetLocalDepth(directory_idx) > global_depth) {
    // 进行 Directory Expansion
    dir_page->IncrGlobalDepth();
  }
  // 进行 Bucket Split 并将新的 bucket 放入目录中
  uint32_t split_image_index = dir_page->GetSplitImageIndex(directory_idx);
  page_id_t split_image_page_id;
  // 新建 page 之后可以直接转成 bucket_page 类型来使用，如果新建完之后再 fetch 一次那它就无法被替换出去了
  auto split_image_page =
      reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&split_image_page_id)->GetData());
  dir_page->SetLocalDepth(split_image_index, dir_page->GetLocalDepth(directory_idx));
  dir_page->SetBucketPageId(split_image_index, split_image_page_id);
  std::vector<MappingType> result;
  bucket_page->CopyMappingsAndResetPage(&result);
  auto local_depth_mask = dir_page->GetLocalDepthMask(directory_idx);
  for (auto mapping : result) {
    uint32_t idx = (Hash(mapping.first) & local_depth_mask);
    if (idx == split_image_index) {
      split_image_page->Insert(mapping.first, mapping.second, comparator_);
    } else {
      bucket_page->Insert(mapping.first, mapping.second, comparator_);
    }
  }
  /*
   * bucket 分裂之后新增了一个 page，这时不能仅仅在目录中修改 split_image_index 的指针，
   * 而是要整个遍历一遍，把所有和 split_image_index 同级的指针都指向新增出来的 page, 因为有可能新增出来的
   * page 有多个指针同时指向它，如果只改 split_image_index 那么剩下本应该指向新 page 的指针此时还是指向分裂之前的
   page
   */
  size_t step_length = 1 << dir_page->GetLocalDepth(directory_idx);
  for (size_t idx = split_image_index; idx >= 0; idx -= step_length) {
    dir_page->SetBucketPageId(idx, split_image_page_id);
    dir_page->SetLocalDepth(idx, dir_page->GetLocalDepth(split_image_index));
    if (idx < step_length) {
      break;
    }
  }
  for (size_t idx = split_image_index; idx < dir_page->Size(); idx += step_length) {
    dir_page->SetBucketPageId(idx, split_image_page_id);
    dir_page->SetLocalDepth(idx, dir_page->GetLocalDepth(split_image_index));
  }
  // 这里要注意, 分裂之后不是直接往老的 bucket_page 里插入，要重新计算一下 directory_idx 来判断往哪里插入
  auto new_directory_idx = Hash(key) & local_depth_mask;
  bool inserted = new_directory_idx == directory_idx ? bucket_page->Insert(key, value, comparator_)
                                                     : split_image_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  // 新分裂出来的页不要忘记 unpin
  buffer_pool_manager_->UnpinPage(split_image_page_id, true);
  table_latch_.WUnlock();
  return inserted;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->WLatch();
  if (!bucket_page->Remove(key, value, comparator_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  Merge(transaction, key, value);
  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto bucket_page_id = KeyToPageId(key, dir_page);
  auto directory_idx = KeyToDirectoryIndex(key, dir_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  while (true) {
    if (!bucket_page->IsEmpty()) {
      break;
    }
    auto local_depth = dir_page->GetLocalDepth(directory_idx);
    auto split_image_bucket_idx = dir_page->GetSplitImageIndex(directory_idx);
    if (local_depth <= 1 || local_depth != dir_page->GetLocalDepth(split_image_bucket_idx)) {
      break;
    }
    dir_page->DecrLocalDepth(split_image_bucket_idx);
    size_t step_length = 1 << dir_page->GetLocalDepth(split_image_bucket_idx);
    for (size_t idx = directory_idx; idx >= 0; idx -= step_length) {
      dir_page->SetBucketPageId(idx, dir_page->GetBucketPageId(split_image_bucket_idx));
      dir_page->SetLocalDepth(idx, dir_page->GetLocalDepth(split_image_bucket_idx));
      if (idx < step_length) {
        break;
      }
    }
    for (size_t idx = directory_idx; idx < dir_page->Size(); idx += step_length) {
      dir_page->SetBucketPageId(idx, dir_page->GetBucketPageId(split_image_bucket_idx));
      dir_page->SetLocalDepth(idx, dir_page->GetLocalDepth(split_image_bucket_idx));
    }
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->DeletePage(bucket_page_id);
    dir_page->DecrGlobalDepth();
    bucket_page_id = KeyToPageId(key, dir_page);
    directory_idx = KeyToDirectoryIndex(key, dir_page);
    bucket_page = FetchBucketPage(bucket_page_id);
  }
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  table_latch_.WUnlock();
}

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
