//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"
#include "include/storage/page/header_page.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : size_(num_buckets),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)) {
  block_latches_ = std::vector<ReaderWriterLatch>(size_ / BLOCK_ARRAY_SIZE + 1);

  Page *header_page = buffer_pool_manager_->NewPage(&header_page_id_);

  // a new hash table is created, add it to database header page
  HeaderPage *db_header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(0));
  db_header_page->InsertRecord(name, header_page_id_);

  // retrieve data from page as header, set metadata
  table_latch_.WLock();
  HashTableHeaderPage *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());
  header->SetPageId(header_page_id_);
  header->SetSize(size_);
  // header->SetLSN(0);

  // allocate enough blocks
  for (size_t i = 0; i < size_ / BLOCK_ARRAY_SIZE + 1; ++i) {
    page_id_t block_page_id;
    buffer_pool_manager_->NewPage(&block_page_id);
    header->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
  }
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  std::pair<size_t, slot_offset_t> idx = GetSlot(key);
  size_t block_idx = idx.first;
  slot_offset_t offset = idx.second;

  table_latch_.RLock();
  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  // probe in the whole slot array until find a non-tomb slot
  while (block_idx < block_latches_.size()) {
    page_id_t block_page_id = header->GetBlockPageId(block_idx);
    Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
    HashTableBlockPage<KeyType, ValueType, KeyComparator> *block =
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());

    block_latches_[block_idx].RLock();
    while (block->IsOccupied(offset) && offset < BLOCK_ARRAY_SIZE) {
      if (block->IsReadable(offset) && comparator_(key, block->KeyAt(offset)) == 0) {
        result->push_back(block->ValueAt(offset));
      }
      offset++;
    }
    block_latches_[block_idx].RUnlock();
    buffer_pool_manager_->UnpinPage(block_page_id, false);

    if (offset < BLOCK_ARRAY_SIZE) {
      // this means we have probed a non-tomb slot, query completed
      break;
    } else {
      // this means we have finished probing a block but yet to find a non-tomb slot
      // keep probing next block
      block_idx++;
    }
  }

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  table_latch_.RLock();
  std::pair<bool, bool> res = InsertImpl(header, key, value);
  table_latch_.RUnlock();

  while (res.second) {
    Resize(size_);
    // Resize() would hold `table_latch_` WriterLock until all key-value paors are re-arranged
    table_latch_.RLock();
    res = InsertImpl(header, key, value);
    table_latch_.RUnlock();
  }
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  return res.first;
}

/****************************************************************************
 * INSERTION IMPLEMENTATION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<bool, bool> HASH_TABLE_TYPE::InsertImpl(HashTableHeaderPage *header, const KeyType &key,
                                                  const ValueType &value) {
  std::pair<size_t, slot_offset_t> idx = GetSlot(key);
  size_t block_idx = idx.first;
  slot_offset_t offset = idx.second;

  // probe in the whole slot array until find a non-tomb slot
  while (block_idx < block_latches_.size()) {
    page_id_t block_page_id = header->GetBlockPageId(block_idx);
    Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
    HashTableBlockPage<KeyType, ValueType, KeyComparator> *block =
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());

    block_latches_[block_idx].WLock();
    while (block->IsOccupied(offset) && offset < BLOCK_ARRAY_SIZE) {
      if (block->IsReadable(offset) && comparator_(key, block->KeyAt(offset)) == 0 && value == block->ValueAt(offset)) {
        // duplicated key-value pair, clean up and return
        block_latches_[block_idx].WUnlock();
        buffer_pool_manager_->UnpinPage(block_page_id, false);
        return std::make_pair(false, false);
      }
      offset++;
    }
    if (offset < BLOCK_ARRAY_SIZE) {
      // this means we have probed a non-tomb slot, insert key-value pair and query completed
      block->Insert(offset, key, value);
      block_latches_[block_idx].WUnlock();
      buffer_pool_manager_->UnpinPage(block_page_id, true);
      return std::make_pair(true, false);
    } else {
      // this means we have finished probing a block but yet to find a non-tomb slot
      // keep probing next block
      block_latches_[block_idx].WUnlock();
      buffer_pool_manager_->UnpinPage(block_page_id, false);
      block_idx++;
    }
  }

  // if execution comes here, it means that the hash table is filled up and needs resize
  return std::make_pair(false, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  std::pair<size_t, slot_offset_t> idx = GetSlot(key);
  size_t block_idx = idx.first;
  slot_offset_t offset = idx.second;

  table_latch_.RLock();
  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  // probe in the whole slot array until find a non-tomb slot
  bool success = false;
  while (block_idx < block_latches_.size()) {
    page_id_t block_page_id = header->GetBlockPageId(block_idx);
    Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
    HashTableBlockPage<KeyType, ValueType, KeyComparator> *block =
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());

    block_latches_[block_idx].WLock();
    while (block->IsOccupied(offset) && offset < BLOCK_ARRAY_SIZE) {
      if (block->IsReadable(offset) && comparator_(key, block->KeyAt(offset)) == 0 && value == block->ValueAt(offset)) {
        block->Remove(offset);
        success = true;
        break;
      }
      offset++;
    }
    block_latches_[block_idx].WUnlock();
    buffer_pool_manager_->UnpinPage(block_page_id, true);

    if (offset < BLOCK_ARRAY_SIZE) {
      // this means we have probed a non-tomb slot, query completed
      break;
    } else {
      // this means we have finished probing a block but yet to find a non-tomb slot
      // keep probing next block
      block_idx++;
    }
  }

  table_latch_.RUnlock();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);

  return success;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  Page *header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  HashTableHeaderPage *header = reinterpret_cast<HashTableHeaderPage *>(header_page->GetData());

  table_latch_.WLock();
  // resizing thread manipulating metadata here exclusively
  std::vector<MappingType> all_pairs = CleanUp(header);

  // allocate new blocks to expand the hash table
  size_ *= 2;
  header->SetSize(size_);
  for (size_t i = header->NumBlocks(); i < size_ / BLOCK_ARRAY_SIZE + 1; ++i) {
    page_id_t block_page_id;
    buffer_pool_manager_->NewPage(&block_page_id);
    header->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false);
  }
  block_latches_ = std::vector<ReaderWriterLatch>(size_ / BLOCK_ARRAY_SIZE + 1);

  // re-insert all key-value pairs
  for (MappingType pr : all_pairs) {
    InsertImpl(header, pr.first, pr.second);
  }

  table_latch_.WUnlock();

  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::vector<MappingType> HASH_TABLE_TYPE::CleanUp(HashTableHeaderPage *header) {
  std::vector<MappingType> all;
  for (size_t i = 0; i < header->NumBlocks(); ++i) {
    page_id_t block_page_id = header->GetBlockPageId(i);
    Page *block_page = buffer_pool_manager_->FetchPage(block_page_id);
    HashTableBlockPage<KeyType, ValueType, KeyComparator> *block =
        reinterpret_cast<HashTableBlockPage<KeyType, ValueType, KeyComparator> *>(block_page->GetData());

    block_latches_[i].WLock();
    for (slot_offset_t offset = 0; offset < BLOCK_ARRAY_SIZE; ++offset) {
      if (block->IsReadable(offset)) {
        all.push_back(std::make_pair(block->KeyAt(offset), block->ValueAt(offset)));
      }
    }
    block_page->ResetMemory();  // clean all tombs (readable and occupied bits)
    block_latches_[i].WUnlock();
    buffer_pool_manager_->UnpinPage(block_page_id, true);
  }
  return all;
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();
  size_t size = size_;
  table_latch_.RUnlock();
  return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::pair<size_t, slot_offset_t> HASH_TABLE_TYPE::GetSlot(const KeyType &key) {
  uint64_t idx = hash_fn_.GetHash(key) % size_;
  return std::make_pair(static_cast<size_t>(idx / BLOCK_ARRAY_SIZE),
                        static_cast<slot_offset_t>(idx % BLOCK_ARRAY_SIZE));
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
