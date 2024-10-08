//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/24.
// src/include/storage/page/distribution_dataset_header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <fmt/format.h>

#include <common/config.h>
#include <common/exception.h>
#include <storage/page/header_page.h>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_HEADER_PAGE_HEADER_SIZE (6 + COMMON_HEADER_PAGE_HEADER_SIZE)

class HeaderPage;

// define enum
/*
 * GENERATION: dimension: unknown, distribution_datat_set_start:
 * MNIST: dimension: 784[training/test set(csv form)]
 * CIFAR10: dimension: 32*32*3
 * SIFT:
 */
enum class DataSetType : std::uint8_t { INVALID_DATA_SET_TYPE = 0, GENERATION, MNIST, CIFAR10};
enum class DistributionType : std::uint8_t { INVALID_DISTRIBUTION_TYPE = 0, UNIFORM, GAUSSIAN, CAUCHY};
enum class NormalizationType : std::uint8_t { INVALID_NORMALIZATION_TYPE = 0, SOFTMAX, MIN_MAX};

inline auto DataSetTypeToString(DataSetType type) noexcept -> std::string {
  switch (type) {
    case DataSetType::INVALID_DATA_SET_TYPE: return "INVALID DATA SET TYPE";
    case DataSetType::GENERATION: return "GENERATION";
    case DataSetType::MNIST: return "MNIST";
    case DataSetType::CIFAR10: return "CIFAR10";
    default: return "UNSUPPORTED DATA SET TYPE";
  }
}

inline auto DistributionTypeToString(DistributionType type) noexcept -> std::string {
  switch (type) {
    case DistributionType::INVALID_DISTRIBUTION_TYPE: return "INVALID DISTRIBUTION TYPE";
    case DistributionType::UNIFORM: return "UNIFORM";
    case DistributionType::GAUSSIAN: return "GAUSSIAN";
    case DistributionType::CAUCHY: return "CAUCHY";
    default: return "UNSUPPORTED DISTRIBUTION TYPE";
  }
}

inline auto NormalizationTypeToString(NormalizationType type) noexcept -> std::string {
  switch (type) {
    case NormalizationType::INVALID_NORMALIZATION_TYPE: return "INVALID NORMALIZATION TYPE";
    case NormalizationType::SOFTMAX: return "SOFTMAX";
    case NormalizationType::MIN_MAX: return "MIN_MAX";
    default: return "UNSUPPORTED DATA SET TYPE";
  }
}
/**
* @breif page that contain the start page of data
*/
class DistributionDataSetHeaderPage : public HeaderPage {
  template<class ValueType>
  friend class DistributionDataSetManager;
 public:
  DistributionDataSetHeaderPage() = delete;
  DistributionDataSetHeaderPage(const DistributionDataSetHeaderPage &other) = delete;
  ~DistributionDataSetHeaderPage() = delete;

  auto GetDataSetType() const -> DataSetType;
  void SetDataSetType(DataSetType data_set_type);

  auto GetNormalizationType() const -> NormalizationType;
  void SetNormalizationType(NormalizationType normalization_type);

  auto GetDimension() const -> int;
  void SetDimension(int dimension);

  auto IsEmpty() const -> bool;
  void SetDirectoryId(page_id_t directory_start_page_id);

 private:
  DataSetType data_set_type_;                    // data set type in this file
  NormalizationType normalization_type_;         // normalization type in this file
  int dimension_;                                // dimension of the data
  page_id_t directory_start_page_id_;              // directory start page id
};

}  // namespace distribution_lsh
