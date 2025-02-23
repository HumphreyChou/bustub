//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_header_page.h"
#include "include/common/logger.h"

namespace bustub {
page_id_t HashTableHeaderPage::GetBlockPageId(size_t index) {
  if (index >= next_ind_) {
    LOG_ERROR("index: %ld but next_ind_: %ld ", index, next_ind_);
    return -1;
  }
  return block_page_ids_[index];
}

page_id_t HashTableHeaderPage::GetPageId() const { return page_id_; }

void HashTableHeaderPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; }

lsn_t HashTableHeaderPage::GetLSN() const { return lsn_; }

void HashTableHeaderPage::SetLSN(lsn_t lsn) { lsn_ = lsn; }

void HashTableHeaderPage::AddBlockPageId(page_id_t page_id) {
  if (next_ind_ >= MAX_BLOCK_NUM) {
    LOG_ERROR("hash table header page is full");
  } else {
    block_page_ids_[next_ind_++] = page_id;
  }
}

size_t HashTableHeaderPage::NumBlocks() { return next_ind_; }

void HashTableHeaderPage::SetSize(size_t size) { size_ = size; }

size_t HashTableHeaderPage::GetSize() const { return size_; }

}  // namespace bustub
