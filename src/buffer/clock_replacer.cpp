//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
#include "include/common/logger.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  capacity_ = num_pages;
  clock_ = std::vector<Slot>(capacity_);
  hand_ = 0;
  size_ = 0;
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> latch(mtx_);
  if (size_ == 0) {
    frame_id = nullptr;
    return false;
  }
  for (;;hand_ = (hand_ + 1) % capacity_) {
    if (!clock_[hand_].valid) {
      continue;
    }
    if (clock_[hand_].ref == false) {
      *frame_id = hand_;
      clock_[hand_].valid = false;
      size_--;
      return true;
    } else {
      clock_[hand_].ref = false;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(mtx_);
  if(clock_[frame_id].valid) {
    clock_[frame_id].valid = false;
    size_--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(mtx_);
  if(!clock_[frame_id].valid) {
    clock_[frame_id].valid = true;
    clock_[frame_id].ref = true;
    size_++;
  }
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> latch(mtx_);
  return size_;
}

}  // namespace bustub
