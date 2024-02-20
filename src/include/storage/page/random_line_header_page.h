//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/20.
// src/include/storage/page/random_line_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>

namespace distribution_lsh {

#define RANDOM_LINE_HEADER_PAGE_HEADER_SIZE 24
#define RANDOM_LINE_HEADER_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - RANDOM_LINE_HEADER_PAGE_HEADER_SIZE) / (sizeof(page_id_t)))
/**
* @breif page that contain the start page of each random line
*/
class RandomLineHeaderPage {
  friend class RandomLineManager;
 public:
  RandomLineHeaderPage() = delete;
  RandomLineHeaderPage(const RandomLineHeaderPage &other) = delete;

  void Init(int dimension, float epsilon = EPSILON, int max_size = RANDOM_LINE_HEADER_PAGE_SIZE) {
    SetDimension(dimension);
    SetEpsilon(epsilon);
    SetMaxSize(max_size);
    SetSize(0);
    SetAverageRandomLinePageId(INVALID_PAGE_ID);
    SetNextPageId(INVALID_PAGE_ID);
  }

  auto GetDimension() const -> int { return dimension_; }
  void SetDimension(int dimension) { dimension_ = dimension; }

  auto GetSize() const -> int { return size_; }
  void SetSize(int size) { size_ = size; }

  auto GetMaxSize() const -> int { return max_size_; }
  void SetMaxSize(int max_size) { max_size_ = max_size; }

  auto GetEpsilon() const -> float { return epsilon_; }
  void SetEpsilon(float epsilon) { epsilon_ = epsilon; }

  auto GetAverageRandomLinePageId() const -> page_id_t { return average_random_line_page_id_; }
  void SetAverageRandomLinePageId(page_id_t average_random_line_page_id) { average_random_line_page_id_ = average_random_line_page_id; }

  auto GetNextPageId() const -> page_id_t { return next_page_id_; }
  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

  void IncreaseSize(int amount) { size_ += amount; }

 private:
  int dimension_{0};
  float epsilon_{EPSILON};
  int size_{0};
  int max_size_{0};
  page_id_t average_random_line_page_id_{INVALID_PAGE_ID};
  page_id_t next_page_id_{INVALID_PAGE_ID};
  page_id_t random_line_pages_start_[0];
};

}  // namespace distribution_lsh

