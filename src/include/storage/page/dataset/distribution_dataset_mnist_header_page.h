//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/12.
// src/include/storage/page/distribution_dataset_mnist_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <storage/page/dataset/distribution_dataset_header_page.h>

namespace distribution_lsh {
template<class ValueType>
class DistributionDataSetManager;


class DistributionDataSetMNISTHeaderPage : public DistributionDataSetHeaderPage{
  template<class ValueType>
  friend class DistributionDataSetManager;
 public:
  DistributionDataSetMNISTHeaderPage() =  delete;
  DistributionDataSetMNISTHeaderPage(const DistributionDataSetMNISTHeaderPage &other) = delete;

  void Init(file_id_t file_id, NormalizationType normalization_type, page_id_t directory_start_page_id);

};

} // namespace distribution_lsh
