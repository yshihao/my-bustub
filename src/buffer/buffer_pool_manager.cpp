//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}
bool BufferPoolManager::find_replace(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }
  if (replacer_->Victim(frame_id)) {
    Page *replace_page = &pages_[*frame_id];
    if (replace_page->IsDirty()) {
      disk_manager_->WritePage(replace_page->GetPageId(),replace_page->GetData());
    }
    replace_page->pin_count_ = 0;
    replace_page->is_dirty_ = false;
    page_table_.erase(replace_page->page_id_);
    replace_page->page_id_ = INVALID_PAGE_ID;
    return true;
  }
  return false;
}
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  latch_.lock();
  auto p = page_table_.find(page_id);
  // 1.1    If P exists, pin it and return it immediately.??
  if (p != page_table_.end()) {
    frame_id_t frame_id = p->second;
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    latch_.unlock();
    return &pages_[frame_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.

  frame_id_t frame_ids;
  if (!find_replace(&frame_ids)) {
    latch_.unlock();
    return nullptr;
  }
  // 2.     If R is dirty, write it back to the disk.
  if (pages_[frame_ids].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_ids].GetPageId(), pages_[frame_ids].GetData());
  }
  // 3.     Delete R from the page table and insert P.
  if (page_table_.find(pages_[frame_ids].GetPageId()) != page_table_.end()) {
    page_table_.erase(pages_[frame_ids].GetPageId());
  }
  page_table_[page_id] = frame_ids;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[frame_ids].page_id_ = page_id;
  pages_[frame_ids].pin_count_ = 1;
  pages_[frame_ids].is_dirty_ = false;
  disk_manager_->ReadPage(page_id, pages_[frame_ids].GetData());
  replacer_->Pin(frame_ids);
  latch_.unlock();
  return &pages_[frame_ids];
  // return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    latch_.unlock();
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() <= 0) {
    latch_.unlock();
     return false;
  }
  if (!pages_[frame_id].IsDirty()) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  pages_[frame_id].pin_count_ = pages_[frame_id].pin_count_-1;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true;
  }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  auto p = page_table_.find(page_id);
  if (p != page_table_.end()) {
    frame_id_t frame_id = p->second;
    page_id_t page_id = p->first;
    pages_[frame_id].is_dirty_ = false;
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  latch_.lock(); 
  page_id_t disk_page_id = disk_manager_->AllocatePage();
  size_t i;
  for (i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() <= 0) { break;}
  }
  if (i >= pool_size_) { 
    latch_.unlock();
    return nullptr; 
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frame_ids;
  if (!find_replace(&frame_ids)) {
    latch_.unlock();
    return nullptr;
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // pages_[frame_ids].ResetMemory();
  // if (page_table_.find(pages_[frame_ids].GetPageId()) != page_table_.end()) {
  //   page_table_.erase(pages_[frame_ids].GetPageId());
  // }
  // if (pages_[frame_ids].is_dirty_) {
  //   disk_manager_->WritePage(pages_[frame_ids].GetPageId(), pages_[frame_ids].GetData());
  // }
  page_table_[disk_page_id] = frame_ids;
  pages_[frame_ids].page_id_ = disk_page_id;
  pages_[frame_ids].pin_count_ = 1;
  replacer_->Pin(frame_ids);
  pages_[frame_ids].is_dirty_ = false;
  // pages_[frame_ids].ResetMemory();
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = disk_page_id;
  // disk_manager_->WritePage(pages_[frame_ids].GetPageId(),pages_[frame_ids].GetData());
  latch_.unlock();
  return &pages_[frame_ids];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  latch_.lock();
  disk_manager_->DeallocatePage(page_id);
  frame_id_t frame_id;
  if (page_table_.find(page_id) == page_table_.end()) {
    latch_.unlock();
    return true;
  } 
  frame_id = page_table_[page_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  replacer_->Pin(frame_id);
  if (pages_[frame_id].IsDirty()) {
    FlushPageImpl(page_id);
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  free_list_.push_back(frame_id);
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (auto & p : page_table_) {
    page_id_t page_id = p.first;
    FlushPageImpl(page_id);
  }
}

}  // namespace bustub
