//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2023/12/30.
// src/include/common/rwlatch.h
//
//===-----------------------------------------------------

#pragma once

#include <shared_mutex>

namespace qalsh {

/**
 * Reader-Writer latch backed by std::mutex
 */
class ReaderWriterLatch {
 public:
  /**
   * Acquire a write latch
   */
  void WLock() { mutex_.lock(); }

  /**
   * Release a write lock
   */
  void WUnlock() { mutex_.unlock(); }

  /**
   * Acquire a read latch
   */
  void RLock() { mutex_.lock_shared(); }

  /**
   * Release a read latch
   */
  void RUnlock() { mutex_.unlock_shared(); }

 private:
  std::shared_mutex mutex_;
};

}// namespace qalsh
