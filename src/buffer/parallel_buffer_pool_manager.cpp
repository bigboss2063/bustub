//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/parallel_buffer_pool_manager.h"
namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  num_instances_ = num_instances;
  next_index_ = 0;
  pool_size_ = pool_size;
  for (size_t i = 0; i < num_instances; ++i) {
    BufferPoolManagerInstance *buffer_pool_manager_instance =
        new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager);
    buffer_pool_manager_instances_[i] = buffer_pool_manager_instance;
  }
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() = default;

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return num_instances_ * pool_size_;
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  size_t instance_index = page_id % GetPoolSize();
  BufferPoolManagerInstance *buffer_pool_manager_instance = buffer_pool_manager_instances_[instance_index];
  return buffer_pool_manager_instance;
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  BufferPoolManagerInstance *buffer_pool_manager_instance =
      dynamic_cast<BufferPoolManagerInstance *>(GetBufferPoolManager(page_id));
  return buffer_pool_manager_instance->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager_instance = GetBufferPoolManager(page_id);
  return buffer_pool_manager_instance->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager_instance = GetBufferPoolManager(page_id);
  return buffer_pool_manager_instance->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  std::lock_guard<std::mutex> guard(latch_);
  size_t index = next_index_;
  size_t saved_index = index;
  while (true) {
    BufferPoolManagerInstance *buffer_pool_manager_instance =
        dynamic_cast<BufferPoolManagerInstance *>(buffer_pool_manager_instances_[index]);
    Page *page = buffer_pool_manager_instance->NewPage(page_id);
    if (page != nullptr) {
      next_index_ = (next_index_ + 1) % num_instances_;
      return page;
    }
    index = (index + 1) % num_instances_;
    if (index == saved_index) {
      break;
    }
  }
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  // Delete page_id from responsible BufferPoolManagerInstance
  BufferPoolManager *buffer_pool_manager_instance = GetBufferPoolManager(page_id);
  if (buffer_pool_manager_instance == nullptr) {
    return true;
  }
  return buffer_pool_manager_instance->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  // flush all pages from all BufferPoolManagerInstances
  for (auto &kv : buffer_pool_manager_instances_) {
    kv.second->FlushAllPages();
  }
}

}  // namespace bustub
