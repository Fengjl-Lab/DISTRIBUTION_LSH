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

namespace distribution_lsh {

// define distribution type enum
enum class DistributionType { INVALID_DISTRIBUTION_TYPE = 0, UNIFORM, GAUSSIAN};

/**
 * @brief class for generate random distribution and provide
 * the interface to operate real world data set
 */

class DistributionDatasetGenerator {
 public:
  DistributionDatasetGenerator() = default;
  ~DistributionDatasetGenerator() =  delete;

  // Generate a random line based on average_random_line and epsilon
  auto GenerateDistributionDataset(int dimension, int size, DistributionType type = DistributionType::UNIFORM) -> std::vector<std::unique_ptr<float *>>;
};
} // namespace distribution_lsh