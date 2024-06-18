//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/include/storage/page/dataset/distribution_dataset_directory_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <fmt/format.h>
#include <storage/page/dataset/distribution_dataset_page.h>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_DIRECTORY_PAGE_HEADER_SIZE (8 + DISTRIBUTION_DATASET_PAGE_HEADER_SIZE)
#define DISTRIBUTION_DATASET_DIRECTORY_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - DISTRIBUTION_DATASET_DIRECTORY_PAGE_HEADER_SIZE) / sizeof(page_id_t))


class DistributionDataSetDirectoryPage : public distribution_lsh::DistributionDataSetPage {
  template<class ValueType>
  friend class DistributionDataSetManager;
 public:
  DistributionDataSetDirectoryPage() = delete;
  DistributionDataSetDirectoryPage(const DistributionDataSetDirectoryPage &other) = delete;

  /**
  * Init the directory page
  * @param max_size data the page can max hold
  */
 void Init(int max_size = DISTRIBUTION_DATASET_DIRECTORY_PAGE_SIZE);


 /**
  * Insert an entry into the page
  * @param data_page_id new data page start
  * @return successfully insert or not
  */
 auto Insert(page_id_t data_page_id, int *index) -> bool;

 /**
  * Delete an entry into the page
  */
 auto Delete(int index) -> bool;

 /**
  * Print the information of the page
  */
 auto ToString() -> std::string override;

 /**
  * Value of input index
  */
 [[nodiscard]] auto IndexAt(int index) const -> page_id_t;

 [[nodiscard]] auto GetNullSlotStart() const -> int;
 void  SetNullSlotStart(int null_slot_start);

 [[nodiscard]] auto GetEndOfArray() const -> int;
 void SetEndOfArray(int end_of_array);

 private:
  int null_slot_start_{0};
  int end_of_array_{0};
  page_id_t array_[0];
};

} // namespace distribution_lsh
