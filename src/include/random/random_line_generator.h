//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/21.
// src/include/random/random_line_generator.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <storage/page/random_line/random_line_header_page.h>
#include <memory>

namespace distribution_lsh {

/**
 * @brief random line generator which based on gaussian distribution and epsilon
 */
template<typename RandomLineValueType>
class RandomLineGenerator {
 public:
  // Generate a random line based on average_random_line and epsilon
  auto GenerateRandomLine(RandomLineDistributionType distribution_type,
                          RandomLineNormalizationType normalization_type,
                          int dimension) -> std::shared_ptr<RandomLineValueType[]>;

  auto Normalization(std::shared_ptr<RandomLineValueType[]> data, RandomLineNormalizationType normalization_type, int dimension) -> std::shared_ptr<RandomLineValueType[]>;
};
} // namespace distribution_lsh

