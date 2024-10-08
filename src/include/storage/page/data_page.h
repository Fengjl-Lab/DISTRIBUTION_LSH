//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/6/19.
// src/include/storage/page/data_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>

namespace distribution_lsh {

#define COMMON_DATA_PAGE_HEADER_SIZE 4

/**
 * @brief the common data page contains the next slot page in the project
 */
class DataPage {
 public:
  /** Delete all constructor / destructor to ensure memory safety */
  DataPage() = delete;
  DataPage(const DataPage &other) = delete;
  ~DataPage() = delete;

  [[nodiscard]] auto GetNextSlotPageId() const -> page_id_t { return next_slot_page_id_; }
  void SetNextSlotPageId(page_id_t next_slot_page_id) { next_slot_page_id_ = next_slot_page_id; }

 private:
  page_id_t next_slot_page_id_{INVALID_PAGE_ID};
};


} // namespace distribution_lsh
