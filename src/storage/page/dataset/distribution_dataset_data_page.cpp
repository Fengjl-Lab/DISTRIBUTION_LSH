//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/storage/page/dataset/distribution_dataset_data_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/dataset/distribution_dataset_data_page.h>

namespace distribution_lsh {

DISTRIBUTION_DATASET_TEMPLATE
void DISTRIBUTION_DATASET_PAGE_TYPE::Init(int max_size) {
  SetPageType(DistributionDatatSetPageType::DATA_PAGE);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}


DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_PAGE_TYPE::GetValueAt(int &index) const -> ValueType {
  if (index < 0 || index> GetMaxSize()- 1) {
    return -1;
  }

  return array_[index];
}

DISTRIBUTION_DATASET_TEMPLATE
void DISTRIBUTION_DATASET_PAGE_TYPE::SetValueAt(int &index, ValueType &value) {
  if (index < 0 || index > GetMaxSize() - 1) {
    return;
  }
  array_[index] = value;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_PAGE_TYPE::ToString() -> std::string {
  return fmt::format("\033[32mData Page:\t"
                     "size = {},\t"
                     "max size = {}.\033[9m",
                     GetSize(),
                     GetMaxSize());
}


template class DistributionDataSetDataPage<float>;
} // namespace distribution_lsh