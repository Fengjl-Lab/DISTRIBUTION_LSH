//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/24.
// src/include/dataset/distribution/distribution_dataset_generator.h
//
//===-----------------------------------------------------

#pragma once

#include <random>
#include <vector>
#include <memory>

#include <common/config.h>
#include <storage/page/dataset/distribution_dataset_header_page.h>
#include <storage/page/dataset/distribution_dataset_data_page.h>

namespace distribution_lsh {
/**
 * @brief class for generate random distribution and provide
 * the interface to operate real world data set
 */

#define DISTRIBUTION_DATASET_PROCESSOR_TYPE DistributionDatasetProcessor<ValueType>

DISTRIBUTION_DATASET_TEMPLATE
class DistributionDatasetProcessor {
 public:
  DistributionDatasetProcessor() = default;

  // Generate a random line based on average_random_line and epsilon
  auto GenerationDistributionDataset(int dimension,
                                     int size,
                                     DistributionType distribution_type = DistributionType::UNIFORM,
                                     NormalizationType normalization_type = NormalizationType::SOFTMAX,
                                     float *param = nullptr) -> std::unique_ptr<ValueType[]>;

  auto MNISTDistributionDataset(int size,
                                const std::string& directory_name_,
                                NormalizationType normalization_type = NormalizationType::SOFTMAX) -> std::unique_ptr<ValueType[]>;

  auto CIFAR10DistributionDataset(int size,
                                  std::string directory_name_,
                                  NormalizationType normalization_type = NormalizationType::SOFTMAX) -> std::unique_ptr<ValueType[]>;

 private:
  // Normalization Method
  void Normalize(float *distribution_dataset,
                 int size,
                 int dimension,
                 NormalizationType normalization_type = NormalizationType::SOFTMAX);

};
} // namespace distribution_lsh