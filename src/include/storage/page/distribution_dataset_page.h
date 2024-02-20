//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/24.
// src/include/storage/page/distribution_dataset_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <common/logger.h>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_PAGE_TEMPLATE template<typename ValueType>
#define DISTRIBUTION_DATASET_PAGE_TYPE DistributionDatasetPage<ValueType>
#define DISTRIBUTION_DATASET_PAGE_HEADER_SIZE 12
#define DISTRIBUTION_DATASET_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - DISTRIBUTION_DATASET_PAGE_HEADER_SIZE) / (sizeof(ValueType)))
/**
 * @brief page to install a random line
 * -----------------------------------------------------------------------
 * | SIZE (4) | MAX_SIZE (4) | NEXT_PAGE_ID (4) | ....DATA     .....     |
 * ----------------------------------------------------------------------
 */
DISTRIBUTION_DATASET_PAGE_TEMPLATE
class DistributionDatasetPage {
  friend class DistributionDatasetManager;
 public:
  DistributionDatasetPage() = delete;
  DistributionDatasetPage(const DistributionDatasetPage &other) = delete;

  void Init(int max_size = DISTRIBUTION_DATASET_PAGE_SIZE) {
    SetMaxSize(max_size);
    SetSize(0);
    SetNextPageId(INVALID_PAGE_ID);
  }

  auto GetValueAt(int &index) const -> ValueType {
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

  auto GetSize() const -> int { return size_; }
  void SetSize(int size) { size_ = size; }

  auto GetMaxSize() const -> int { return max_size_; }
  void SetMaxSize(int max_size) { max_size_ = max_size; }

  auto GetNextPageId() const -> page_id_t { return next_page_id_; }
  void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

 private:
  int size_;
  int max_size_{DISTRIBUTION_DATASET_PAGE_SIZE};
  page_id_t next_page_id_{INVALID_PAGE_ID};
  ValueType array_[0];
};

template
class DistributionDatasetPage<float>;

} // namespace distribution_lsh
