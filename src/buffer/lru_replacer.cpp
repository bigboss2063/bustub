
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>
#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * 将双向列表的队头元素弹出，其实就是将最近最少使用的元素弹出
 * @param frame_id
 * @return
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (frame_id_list_.empty()) {
    return false;
  }
  *frame_id = frame_id_list_.front();
  frame_id_list_.pop_front();
  location_map_.erase(*frame_id);
  return true;
}

/**
 * BufferPool Pin 了某个页，代表这个页最近正在被使用，所以从队列中移出，防止被换入磁盘
 * @param frame_id
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (!frame_id_list_.empty()) {
    if (location_map_.count(frame_id) != 0) {
      frame_id_list_.erase(location_map_[frame_id]);
      location_map_.erase(frame_id);
    }
  }
}

/**
 * BufferPool Unpin 了某个页，说明这个页刚被使用完，那么把它插入队尾。
 * @param frame_id
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  mutex_.lock();
  bool found = (std::find(frame_id_list_.begin(), frame_id_list_.end(), frame_id) != frame_id_list_.end());
  if (!found) {
    frame_id_list_.push_back(frame_id);
    location_map_[frame_id] = std::prev(frame_id_list_.end());
  }
  mutex_.unlock();
}

size_t LRUReplacer::Size() { return frame_id_list_.size(); }

}  // namespace bustub
