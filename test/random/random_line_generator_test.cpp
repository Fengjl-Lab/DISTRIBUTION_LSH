//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// test/random/random_line_generator_test.cpp
//
//===-----------------------------------------------------

#include <random/random_line_generator.h>
#include <fmt/format.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

TEST(RandomLineGeneratorTest, RandomLineGeneration) {
  RandomLineGenerator<float> random_line_generator;
  int dimension = 1000;
  std::shared_ptr<float[]> data =
      random_line_generator.GenerateRandomLine(distribution_lsh::RandomLineDistributionType::GAUSSIAN,
                                               distribution_lsh::RandomLineNormalizationType::NONE,
                                               dimension);
  for (int i = 0; i < dimension; ++i) {
    fmt::print("value at {} is : {:.10f}\n", i, data.get()[i]);
  }
}

} // namespace distribution_lsh
