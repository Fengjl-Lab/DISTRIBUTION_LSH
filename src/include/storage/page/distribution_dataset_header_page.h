//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/24.
// src/include/storage/page/distribution_dataset_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_HEADER_PAGE_HEADER_SIZE 16
#define DISTRIBUTION_DATASET_HEADER_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - DISTRIBUTION_DATASET_HEADER_PAGE_HEADER_SIZE) / (sizeof(page_id_t)))
/**
* @breif page that contain the start page of each random line
*/
class DistributionDataSetHeaderPage {
 public:
  DistributionDataSetHeaderPage() = delete;
  DistributionDataSetHeaderPage(const DistributionDataSetHeaderPage &other) = delete;

  void Init(int dimension, float epsilon = EPSILON, int max_size = DISTRIBUTION_DATASET_HEADER_PAGE_SIZE) {
    SetDimension(dimension);
    SetMaxSize(max_size);
    SetSize(0);
    SetNextPageId(INVALID_PAGE_ID);
  }

  auto GetDimension() const -> int { return dimension_; }
  void SetDimension(int dimension) { dimension_ = dimension; }

  auto GetSize() const -> int { return size_; }
  void SetSize(int size) { size_ = size; }

  auto GetMaxSize() const -> int { return max_size_; }
  void SetMaxSize(int max_size) { max_size_ = max_size; }

  auto GetNextPageId() const -> page_id_t { return next_page_id_; }
  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

  void IncreaseSize(int amount) { size_ += amount; }

 private:
  int dimension_{0};
  int size_{0};
  int max_size_{0};
  page_id_t next_page_id_{INVALID_PAGE_ID};
  page_id_t distribution_dataset_pages_start_[0];
};

}  // namespace distribution_lsh
