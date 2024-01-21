//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/20.
// src/include/storage/page/random_line_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <common/logger.h>

namespace distribution_lsh {

#define RANDOM_LINE_PAGE_TEMPLATE template<typename ValueType>
#define RANDOM_LINE_PAGE_TYPE RandomLinePage<ValueType>
#define RANDOM_LINE_PAGE_HEADER_SIZE 12
#define RANDOM_LINE_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_PAGE_HEADER_SIZE) / (sizeof(ValueType)))
/**
 * @brief page to install a random line
 * -----------------------------------------------------------------------
 * | SIZE (4) | MAX_SIZE (4) | NEXT_PAGE_ID (4) | ....DATA     .....     |
 * ----------------------------------------------------------------------
 */
RANDOM_LINE_PAGE_TEMPLATE
class RandomLinePage {
 public:
  RandomLinePage() = delete;
  RandomLinePage(const RandomLinePage& other) = delete;

  void Init(int max_size = RANDOM_LINE_PAGE_SIZE) {
    SetMaxSize(max_size);
    SetSize(0);
    SetNextPageId(INVALID_PAGE_ID);
  }

  auto GetValueAt(int &index) -> ValueType {
    if (index < 0 || index > max_size_ - 1) {
      return -1;
    }

    return array_[index];
  }

  void SetValueAt(int &index, ValueType &value) {
    if (index < 0 || index > max_size_ - 1) {
      return;
    }
    array_[index] = value;
  }

  auto GetSize() -> int { return size_; }
  void SetSize(int size) {  size_ = size;}

  auto GetMaxSize() -> int { return max_size_; }
  void SetMaxSize(int max_size) { max_size_ = max_size; }

  auto GetNextPageId() -> page_id_t { return next_page_id_; }
  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

 private:
  int size_;
  int max_size_{RANDOM_LINE_PAGE_SIZE};
  page_id_t next_page_id_{INVALID_PAGE_ID};
  ValueType array_[0];
};

template class RandomLinePage<float>;
} // namespace distribution_lsh
