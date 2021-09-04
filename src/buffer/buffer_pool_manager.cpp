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
#include "include/buffer/clock_replacer.h"
#include "include/common/logger.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  // `pages_` is really a misleading name. `frames_` or `buffer_pool_` is much better
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

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
  std::lock_guard<std::mutex> latch(latch_);
  // Search the page table for the requested page (P).
  if (page_table_.find(page_id) != page_table_.end()) {
    // If P exists, pin it and return it immediately.
    frame_id_t frame_id = page_table_[page_id];
    Page *p = &pages_[frame_id];
    p->WLatch();
    p->pin_count_++;
    p->WUnlatch();
    replacer_->Pin(frame_id);
    return p;
  }

  // If P does not exist, find a replacement page (R) from either the free list or the replacer.
  if (free_list_.size() > 0) {
    // Note that pages are always found from the free list first.
    // read the requested page into a free frame
    frame_id_t frame_id = *free_list_.begin();
    free_list_.pop_front();
    page_table_[page_id] = frame_id;

    Page *p = &pages_[frame_id];
    p->WLatch();
    p->page_id_ = page_id;
    p->pin_count_ = 1;
    p->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, p->data_);
    p->WUnlatch();

    replacer_->Pin(frame_id);
    return p;
  } else {
    // evict a page in one of the frames
    frame_id_t vic_frame;
    if (!replacer_->Victim(&vic_frame)) return nullptr;

    Page *vic_page = &pages_[vic_frame];
    vic_page->RLatch();
    if (vic_page->IsDirty()) {
      // If R is dirty, write it back to the disk.
      disk_manager_->WritePage(vic_page->page_id_, vic_page->data_);
    }

    // Delete R from the page table and insert P.
    page_table_.erase(vic_page->page_id_);
    vic_page->RUnlatch();
    page_table_[page_id] = vic_frame;

    Page *p = &pages_[vic_frame];
    p->WLatch();
    p->page_id_ = page_id;
    p->pin_count_ = 1;
    p->is_dirty_ = false;
    disk_manager_->ReadPage(page_id, p->data_);
    p->WUnlatch();

    replacer_->Pin(vic_frame);
    return p;
  }

  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> latch(latch_);
  frame_id_t frame_id = page_table_[page_id];
  Page *p = &pages_[frame_id];

  p->WLatch();
  p->is_dirty_ = is_dirty;
  if (p->pin_count_ <= 0) {
    p->WUnlatch();
    return false;
  }
  p->pin_count_--;
  if (p->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  p->WUnlatch();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> latch(latch_);
  if (page_table_.find(page_id) == page_table_.end()) return false;

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].page_id_ == INVALID_PAGE_ID) {
    LOG_ERROR("frame # %d in page table: invalid page id", frame_id);
    return false;
  }

  Page *p = &pages_[frame_id];
  p->RLatch();
  disk_manager_->WritePage(page_id, p->data_);
  p->RUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> latch(latch_);
  if(free_list_.size() > 0) {
    // pick a frame from free list
    frame_id_t frame_id = *free_list_.begin();
    free_list_.pop_front();
    *page_id = disk_manager_->AllocatePage();
    page_table_[*page_id] = frame_id;

    Page *p = &pages_[frame_id];
    p->WLatch();
    p->page_id_ = *page_id;
    p->pin_count_ = 1;
    p->is_dirty_ = false;
    p->ResetMemory();
    p->WUnlatch();
    return p;
  } else {
    // evict a page in on of the frames
    frame_id_t vic_frame;
    // replacer_->Summary();
    if(!(replacer_->Victim(&vic_frame))) {
      return nullptr; // all pages are pinned
    }

    Page *vic_page = &pages_[vic_frame];
    vic_page->RLatch();
    if (vic_page->IsDirty()) {
      disk_manager_->WritePage(vic_page->page_id_, vic_page->data_);
    }
    page_table_.erase(vic_page->page_id_);
    vic_page->RUnlatch();

    *page_id = disk_manager_->AllocatePage();
    page_table_[*page_id] = vic_frame;
    Page *p = &pages_[vic_frame];
    p->WLatch();
    p->page_id_ = *page_id;
    p->pin_count_ = 1;
    p->is_dirty_ = false;
    p->ResetMemory();
    p->WUnlatch();
    return p;    
  }
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> latch(latch_);

  if(page_table_.find(page_id) == page_table_.end()) return true;

  frame_id_t frame_id = page_table_[page_id];
  Page *p = &pages_[frame_id];

  p->RLatch();
  if(p->GetPinCount() > 0) {
    p->RUnlatch();
    return false;
  }
  p->RUnlatch();

  page_table_.erase(page_id);
  free_list_.emplace_back(frame_id);

  p->WLatch();
  disk_manager_->DeallocatePage(page_id);
  p->page_id_ = INVALID_PAGE_ID;
  p->pin_count_ = 0;
  p->is_dirty_ = false;
  p->ResetMemory();
  p->RUnlatch();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // This is an inefficient implementation but it saves my time
  // because I can make use of `FlushPageImpl`
  std::vector<page_id_t> page_ids;
  {
    // avoid deadlock
    std::lock_guard<std::mutex> latch(latch_);
    for (auto kv : page_table_) {
      page_ids.push_back(kv.first);
    }
  }
  for (page_id_t page_id : page_ids) {
    FlushPageImpl(page_id);
  }
}

}  // namespace bustub
