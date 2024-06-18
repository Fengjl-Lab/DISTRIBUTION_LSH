//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/24.
// src/include/storage/page/distribution_dataset_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <common/logger.h>
#include <fmt/format.h>
#include <storage/page/dataset/distribution_dataset_page.h>

namespace distribution_lsh {


#define DISTRIBUTION_DATASET_TEMPLATE template<typename ValueType>
#define DISTRIBUTION_DATASET_PAGE_TYPE DistributionDataSetDataPage<ValueType>
#define DISTRIBUTION_DATASET_DATA_PAGE_HEADER_SIZE DISTRIBUTION_DATASET_PAGE_HEADER_SIZE
#define DISTRIBUTION_DATASET_DATA_PAGE_SIZE ((DISTRIBUTION_LSH_PAGE_SIZE - DISTRIBUTION_DATASET_DATA_PAGE_HEADER_SIZE) / (sizeof(ValueType)))

DISTRIBUTION_DATASET_TEMPLATE
class DistributionDataSetManager;
/**
 * @brief page to install a random line
 * --------------------------------------------------------------------------------------
 * | SIZE (4) | MAX_SIZE (4) | NEXT_PAGE_ID (4) | null slot(4) | ... .DATA     .....     |
 * --------------------------------------------------------------------------------------
 */
DISTRIBUTION_DATASET_TEMPLATE
class DistributionDataSetDataPage : public DistributionDataSetPage {
  friend class DistributionDataSetManager<ValueType>;
 public:
  DistributionDataSetDataPage() = delete;
  DistributionDataSetDataPage(const DistributionDataSetDataPage &other) = delete;

  void Init(int max_size = DISTRIBUTION_DATASET_DATA_PAGE_SIZE);

  auto GetValueAt(int &index) const -> ValueType;

  void SetValueAt(int &index, ValueType &value);

  auto ToString() -> std::string override;

 private:
  ValueType array_[0];
};


} // namespace distribution_lsh
