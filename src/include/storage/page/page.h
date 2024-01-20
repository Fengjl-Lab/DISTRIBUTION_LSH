//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/12/31.
// src/include/storage/page/page.h
//
//===-----------------------------------------------------

#pragma once

#include <cstring>
#include <iostream>

#include <common/config.h>
#include <common/rwlatch.h>

namespace distribution_lsh {
/**
 * Page is the basic unit of storage within the DISTRIBUTION_LSH algorithm.
 * Page provides a wrapper for actual data pages being held in
 * main memory.
 * Page contains information that is used by the buffer pool manager.
 */
class Page {
  // class that manages Pages
  friend class BufferPoolManager;

 public:
  /** Constructor. Zeros out the page data */
  Page() {
    data_ = new char[DISTRIBUTION_LSH_PAGE_SIZE];
    ResetMemory();
  }

  /** Default destructor. */
  ~Page() { delete[] data_; }

  /** @return the actual data contained within this page */
  inline auto GetData() -> char* { return data_; }

  /** @return the page id of this page */
  inline auto GetPageId() -> page_id_t { return page_id_; }

  /** @return the pin count of this page */
  inline auto GetPinCount() -> int { return pin_count_; }

  /** @return true if the page in memory has been modified from the page on disk, false otherwise */
  inline auto IsDirty() -> bool { return is_dirty_; }

  /** Acquire the page write latch. */
  inline void WLatch() { rwlatch_.WLock(); }

  /** Release the page write latch_. */
  inline void WUnlatch() { rwlatch_.WUnlock(); }

  /** Acquire the page read latch. */
  inline void RLatch() { rwlatch_.RLock(); }

  /** Release the page read latch. */
  inline void RUnlatch() { rwlatch_.RUnlock(); }

  /** @return the page LSN. */
  inline auto GetLSN() -> lsn_t { return *reinterpret_cast<lsn_t *>(GetData() + OFFSET_LSN); }

  /** Sets the page LSN. */
  inline void SetLSN(lsn_t lsn) { memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t)); }

 protected:
  static_assert(sizeof(page_id_t) == 4);
  static_assert(sizeof(lsn_t) == 4);

  static constexpr size_t SIZE_PAGE_HEADER = 8;
  static constexpr size_t OFFSET_PAGE_START = 0;
  static constexpr size_t OFFSET_LSN = 4;

 private:
  /**  Zeros out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, DISTRIBUTION_LSH_PAGE_SIZE); }

  /** The actual data that is stored within a page. */
  // Use ASAN to detect page overflow
  char* data_;
  /** The ID of this page. */
  page_id_t page_id_ = INVALID_PAGE_ID;
  /** The pin count of this page. */
  int pin_count_ = 0;
  /** True if the page is dirty */
  bool is_dirty_ = false;
  /** Page latch */
  ReaderWriterLatch rwlatch_;
};

}// namespace distribution_lsh

