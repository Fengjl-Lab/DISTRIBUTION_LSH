//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/12.
// src/include/storage/page/distribution_dataset_cifar10_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <storage/page/dataset/distribution_dataset_header_page.h>

namespace distribution_lsh {

class DistributionDataSetHeaderPage;


class DistributionDataSetCIFAR10HeaderPage : public DistributionDataSetHeaderPage {
  template<class ValueType>
  friend class DistributionDataSetManager;
 public:
  DistributionDataSetCIFAR10HeaderPage() =  delete;
  DistributionDataSetCIFAR10HeaderPage(const DistributionDataSetCIFAR10HeaderPage &other) = delete;

  void Init(file_id_t file_id, NormalizationType normalization_type, page_id_t directory_start_page_id);
};
} // namespace distribution_lsh
