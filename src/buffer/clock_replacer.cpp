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
  clock_ = std::list<Slot>();
  hand_ = clock_.begin();
}

ClockReplacer::~ClockReplacer() = default;

void ClockReplacer::RemoveFrame(std::list<Slot>::iterator iter) {
  if (iter == hand_) {
    hand_ = clock_.erase(hand_);
    if (hand_ == clock_.end()) hand_ = clock_.begin();
  } else {
    clock_.erase(iter);
  }
}

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  if (clock_.size() == 0) {
    frame_id = nullptr;
    return false;
  }
  std::lock_guard<std::mutex> latch(mtx_);
  while (1) {
    if (hand_ == clock_.end()) {
      hand_ = clock_.begin();
      continue;
    }
    if (hand_->ref = false) {
      *frame_id = hand_->frame_id;
      RemoveFrame(hand_);
      return true;
    } else {
      hand_->ref = false;
      hand_++;
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(mtx_);
  for (auto iter = clock_.begin(); iter != clock_.end(); iter++) {
    if (iter->frame_id == frame_id) {
      RemoveFrame(iter);
      break;
    }
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(mtx_);
  hand_ = clock_.insert(hand_, Slot(frame_id, true));
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> latch(mtx_);
  if (clock_.size() > capacity_) {
    LOG_ERROR("clock has size %d but buffer has max size %d", clock_.size(), capacity_);
  }
  return clock_.size();
}

}  // namespace bustub
