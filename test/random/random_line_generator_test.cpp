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
  int dimension = 10;
  std::shared_ptr<float[]> data =
      random_line_generator.GenerateRandomLine(distribution_lsh::RandomLineDistributionType::GAUSSIAN,
                                               distribution_lsh::RandomLineNormalizationType::NONE,
                                               dimension);
  for (int i = 0; i < dimension; ++i) {
    fmt::print("value at {} is : {:.10f}\n", i, data.get()[i]);
  }
}

TEST(RandomLineGeneratorTest, RandomLineGenerationL1NORM) {
  RandomLineGenerator<float> random_line_generator;
  int dimension = 10;
  std::shared_ptr<float[]> data =
      random_line_generator.GenerateRandomLine(distribution_lsh::RandomLineDistributionType::GAUSSIAN,
                                               distribution_lsh::RandomLineNormalizationType::L1_NORM,
                                               dimension);
  auto sum = 0.0F;
  for (int i = 0; i < dimension; ++i) {
    fmt::print("value at {} is : {:.10f}\n", i, data.get()[i]);
    sum += std::abs(data.get()[i]);
  }

  EXPECT_TRUE(std::abs(sum - 1.0F) < 1e-6);
}

TEST(RandomLineGeneratorTest, RandomLineGenerationL2NORM) {
  RandomLineGenerator<float> random_line_generator;
  int dimension = 10;
  std::shared_ptr<float[]> data =
      random_line_generator.GenerateRandomLine(distribution_lsh::RandomLineDistributionType::GAUSSIAN,
                                               distribution_lsh::RandomLineNormalizationType::L2_NORM,
                                               dimension);
  auto sum = 0.0F;
  for (int i = 0; i < dimension; ++i) {
    fmt::print("value at {} is : {:.10f}\n", i, data.get()[i]);
    sum += data.get()[i] * data.get()[i];
  }

  EXPECT_TRUE(std::abs(sum - 1.0F) < 1e-6);
}

} // namespace distribution_lsh
