//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);
  // 只有一种情况返回 false 就是该页不在内存中
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  Page *page = &pages_[page_table_[page_id]];
  // 方法注释中并没有说明只有脏页才会写回，所以要全部都写入磁盘中
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> lock(latch_);
  Page *page;
  for (size_t i = 0; i < pool_size_; i++) {
    page = &pages_[i];
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  // 不需要循环遍历每个page是否是 pinned 状态，如果缓存池没满，那么其中所有页都是 pinned 状态也无所谓，用空闲状态的帧就行了
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  // 分配一个页 id
  *page_id = AllocatePage();
  frame_id_t frame_id;
  // 首先从空闲队列里拿一个帧
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    // 如果没有就找一个牺牲者
    replacer_->Victim(&frame_id);
  }
  // 拿到帧对应的页
  Page *page = &pages_[frame_id];
  // 如果是脏页就先把内容写出去
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
  page_table_.erase(page->page_id_);
  // 重置元数据
  page->ResetMemory();
  page->page_id_ = *page_id;
  // 创建完新页之后马上写入磁盘中，防止页号丢失
  disk_manager_->WritePage(page->GetPageId(), page->GetData());
  page->pin_count_ = 1;
  replacer_->Pin(frame_id);
  // insert 对于不存在的元素效率更高
  page_table_.insert(std::pair<page_id_t, frame_id_t>(*page_id, frame_id));
  return page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  // 先检查页表中是否有这一页，不要先检查能不能拉取新的页
  if (page_table_.count(page_id) != 0) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    return page;
  }
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else {
    replacer_->Victim(&frame_id);
  }
  Page *page = &pages_[frame_id];
  if (page->is_dirty_) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
    page->is_dirty_ = false;
  }
  page->ResetMemory();
  page_table_.insert(std::pair<page_id_t, frame_id_t>(page_id, frame_id));
  page_table_.erase(page->page_id_);
  page->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, page->GetData());
  page->pin_count_++;
  replacer_->Pin(frame_id);
  return page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() != 0) {
    return false;
  }
  page_table_.erase(page_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  // 一定不能直接把 is_dirty_ 赋值，如果参数是 false，那就会把需要写入磁盘的脏页中的数据丢掉
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  if (page->GetPinCount() <= 0) {
    return false;
  }
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
