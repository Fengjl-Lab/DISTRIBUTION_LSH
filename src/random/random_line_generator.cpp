//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/21.
// src/random/random_line_generator.cpp
//
//===-----------------------------------------------------

#include <random/random_line_generator.h>
#include <random>

namespace distribution_lsh {

auto RandomLineGenerator::GenerateRandomLine(int dimension) -> std::unique_ptr<float> {
  std::random_device rd;
  std::mt19937 generator(rd());
  std::normal_distribution<float> distribution(0.0, 1.0);

  std::unique_ptr<float> data (new float[dimension]);
  for (int i = 0; i < dimension; ++i) {
    data.get()[i] = distribution(generator);
  }

  return data;
}
} // namespace distribution_lsh