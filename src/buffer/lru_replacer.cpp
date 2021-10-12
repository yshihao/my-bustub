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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  page_number = num_pages;
  lru_list.clear();
  lruMap.clear();
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mymutex.lock();
  if (lruMap.empty()) {
    mymutex.unlock();
    return false;
  }
  frame_id_t frame_ids = lru_list.back();
  lruMap.erase(frame_ids);
  lru_list.pop_back();
  *frame_id = frame_ids;
  mymutex.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mymutex.lock();
  if (lruMap.count(frame_id) != 0) {
    lru_list.erase(lruMap[frame_id]);
    lruMap.erase(frame_id);
  }
  mymutex.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mymutex.lock();
  if (lruMap.count(frame_id) != 0) {
    mymutex.unlock();
    return;
  }
  // while(lru_list.size()>=page_number) {
  //     frame_id_t del = lru_list.front();
  //     lru_list.pop_front();
  //     lruMap.erase(del);
  // }
  lru_list.push_front(frame_id);
  lruMap[frame_id] = lru_list.begin();
  mymutex.unlock();
}

size_t LRUReplacer::Size() { return lru_list.size(); }

}  // namespace bustub
