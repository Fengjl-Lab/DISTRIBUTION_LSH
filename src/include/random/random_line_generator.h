//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/21.
// src/include/random/random_line_generator.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <memory>

namespace distribution_lsh {

/**
 * @brief random line generator which based on gaussian distribution and epsilon
 */
class RandomLineGenerator {
 public:
  // Generate a random line based on average_random_line and epsilon
  auto GenerateRandomLine(int dimension) -> std::unique_ptr<float[]>;
};
} // namespace distribution_lsh

