//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/12.
// src/include/storage/page/distribution_dataset_generation_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <memory>
#include <storage/page/dataset/distribution_dataset_header_page.h>

namespace distribution_lsh {

class DistributionDataSetHeaderPage;

/**
* @brief generation data set header page, inherit from distribution dataset header page
*/
class DistributionDataSetGenerationHeaderPage : public DistributionDataSetHeaderPage {
  template<class ValueType>
  friend class DistributionDataSetManager;
 public:
  DistributionDataSetGenerationHeaderPage() = delete;
  DistributionDataSetGenerationHeaderPage(const DistributionDataSetGenerationHeaderPage &other) = delete;

  void Init(
      file_id_t  file_id,
      DistributionType distribution_type,
      NormalizationType normalization_type,
      float param1,
      float param2,
      int dimension,
      page_id_t directory_start_page_id);

  auto GetDistributionType() const -> DistributionType;
  void SetDistributionType(DistributionType distribution_type);

  auto GetParameter() const -> std::shared_ptr<float [2]>;

 private:

  void SetParameter(float param1, float param2);

  DistributionType distribution_type_;                                                  // distribution type
  float parameter_[2];                                                                   // parameters needed to generate data
};
} // namespace distribution_lsh
