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

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  std::scoped_lock locks{latch_};
  auto p = page_table_.find(page_id);
  frame_id_t frame_id;
  if (p != page_table_.end()) {
    frame_id = p->second;
    Page *pages = &pages_[frame_id];
    pages->pin_count_ = pages->GetPinCount() + 1;
    replacer_->Pin(frame_id);  // 这里要不要Pin? 需要！！！
    return pages;
  }
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    Page *pages = &pages_[frame_id];  // 需不需要从磁盘拿？
    pages->page_id_ = page_id;
    pages->pin_count_ = 1;
    page_table_[page_id] = frame_id;
    replacer_->Pin(frame_id);
    disk_manager_->ReadPage(page_id, pages->GetData());
    return pages;
  } else if (replacer_->Victim(&frame_id)) {
    replacer_->Pin(frame_id);
    Page *pages = &pages_[frame_id];
    if (pages->is_dirty_) {
      pages->is_dirty_ = false;
      disk_manager_->WritePage(pages->GetPageId(), pages->GetData());
    }
    page_table_.erase(pages->GetPageId());
    disk_manager_->ReadPage(page_id, pages->GetData());
    pages->page_id_ = page_id;
    pages->pin_count_ = 1;
    page_table_[page_id] = frame_id;
    return pages;
  }
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  return nullptr;
}
// 进程不用这页数据 ---->unPin 到可替换区等待可能被替换 page_table 还是维持状态 重新要这页数据 ---> Pin 换出替换区
// replacer的 Pin 和 UnPin 只是用来标识是否在 replacer可控制的范围页内
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock locks{latch_};
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = p->second;
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() <= 0) {
    return false;
  }
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  page->pin_count_ = page->pin_count_ - 1;
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::scoped_lock locks{latch_};
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = p->second;
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page->GetPageId(), page->GetData());  //多个进程在用的时候可以刷新嘛
  page->is_dirty_ = false;
  // Make sure you call DiskManager::WritePage!
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  std::scoped_lock locks{latch_};
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    Page *pages = &pages_[frame_id];
    page_id_t page_ids = disk_manager_->AllocatePage();
    pages->pin_count_ = 1;
    pages->page_id_ = page_ids;
    pages->ResetMemory();
    replacer_->Pin(frame_id);
    page_table_[page_ids] = frame_id;
    *page_id = page_ids;
    return pages;
  } else if (replacer_->Victim(&frame_id)) {
    replacer_->Pin(frame_id);
    Page *pages = &pages_[frame_id];
    if (pages->is_dirty_) {
      pages->is_dirty_ = false;
      disk_manager_->WritePage(pages->GetPageId(), pages->GetData());
    }
    page_table_.erase(pages->GetPageId());
    page_id_t page_ids = disk_manager_->AllocatePage();
    pages->pin_count_ = 1;
    pages->page_id_ = page_ids;
    pages->ResetMemory();
    page_table_[page_ids] = frame_id;
    *page_id = page_ids;
    return pages;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  return nullptr;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  std::scoped_lock locks{latch_};
  auto p = page_table_.find(page_id);
  if (p == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = p->second;
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() > 0) {
    return false;
  }
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->is_dirty_) {
    page->is_dirty_ = false;
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  page_table_.erase(page_id);
  disk_manager_->DeallocatePage(page_id);
  replacer_->Pin(frame_id);
  page->ResetMemory();
  page->page_id_ = INVALID_PAGE_ID;
  free_list_.emplace_back(frame_id);
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  std::scoped_lock locks{latch_};
  for (auto p : page_table_) {
    page_id_t page_id = p.first;
    FlushPageImpl(page_id);
  }
}

}  // namespace bustub
