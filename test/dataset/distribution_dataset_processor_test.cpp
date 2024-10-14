//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/7.
// test/dataset/distribution_dataset_generator_test.cpp
//
//===-----------------------------------------------------

#include <memory>
#include <gtest/gtest.h>
#include <dataset/distribution/distribution_dataset_processor.h>

namespace distribution_lsh {

TEST(DistributionDatasetProcessorTest, GenerationDataSetUNIFORM_SOFTMAX) {
  DistributionDatasetProcessor<float> ddp;
  auto dimension = 50;
  auto size = 100;
  float param[2] = {0, 1};
  std::unique_ptr<float[]> distribution_dataset =
      ddp.GenerationDistributionDataset(dimension, size, DistributionType::UNIFORM, NormalizationType::SOFTMAX, param);

  // Test distribution's property
  for (int i = 0; i < size; ++i) {
    float sum = 0;
    for (int j = 0; j < dimension; ++j) {
      sum += distribution_dataset[i * dimension + j];
      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
  }
}

TEST(DistributionDatasetProcessorTest, GenerationDataSetGAUSSIAN_SOFTMAX) {
  DistributionDatasetProcessor<float> ddp;
  auto dimension = 5000;
  auto size = 100;
  float param[2] = {0, 1};
  std::unique_ptr<float[]> distribution_dataset =
      ddp.GenerationDistributionDataset(dimension, size, DistributionType::GAUSSIAN, NormalizationType::SOFTMAX, param);

  // Test distribution's property
  for (int i = 0; i < size; ++i) {
    float sum = 0;
    for (int j = 0; j < dimension; ++j) {
      sum += distribution_dataset[i * dimension + j];
      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
  }
}

//TEST(DistributionDatasetProcessorTest, GenerationDataSetCAUCHY_SOFTMAX) {
//  DistributionDatasetProcessor<float> ddp;
//  auto dimension = 5000;
//  auto size = 100;
//  float param[2] = {0, 1};
//  std::unique_ptr<float[]> distribution_dataset =
//      ddp.GenerationDistributionDataset(dimension, size, DistributionType::CAUCHY, NormalizationType::SOFTMAX, param);
//
//  // Test distribution's property
//  for (int i = 0; i < size; ++i) {
//    float sum = 0;
//    for (int j = 0; j < dimension; ++j) {
//      sum += distribution_dataset[i * dimension + j];
//      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
//    }
//    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
//  }
//}

TEST(DistributionDatasetProcessorTest, GenerationDataSetUNIFORM_MINIMAX) {
  DistributionDatasetProcessor<float> ddp;
  auto dimension = 50;
  auto size = 100;
  float param[2] = {0, 1};
  std::unique_ptr<float[]> distribution_dataset =
      ddp.GenerationDistributionDataset(dimension, size, DistributionType::UNIFORM, NormalizationType::MIN_MAX, param);

  // Test distribution's property
  for (int i = 0; i < size; ++i) {
    float sum = 0;
    for (int j = 0; j < dimension; ++j) {
      sum += distribution_dataset[i * dimension + j];
      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
  }
}

TEST(DistributionDatasetProcessorTest, GenerationDataSetGAUSSIAN_MINIMAX) {
  DistributionDatasetProcessor<float> ddp;
  auto dimension = 50;
  auto size = 100;
  float param[2] = {0, 1};
  std::unique_ptr<float[]> distribution_dataset =
      ddp.GenerationDistributionDataset(dimension, size, DistributionType::GAUSSIAN, NormalizationType::MIN_MAX, param);

  // Test distribution's property
  for (int i = 0; i < size; ++i) {
    float sum = 0;
    for (int j = 0; j < dimension; ++j) {
      sum += distribution_dataset[i * dimension + j];
      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
  }
}

TEST(DistributionDatasetProcessorTest, GenerationDataSeCAUCHY_MINIMAX) {
  DistributionDatasetProcessor<float> ddp;
  auto dimension = 50;
  auto size = 100;
  float param[2] = {0, 1};
  std::unique_ptr<float[]> distribution_dataset =
      ddp.GenerationDistributionDataset(dimension, size, DistributionType::CAUCHY, NormalizationType::MIN_MAX, param);

  // Test distribution's property
  for (int i = 0; i < size; ++i) {
    float sum = 0;
    for (int j = 0; j < dimension; ++j) {
      sum += distribution_dataset[i * dimension + j];
      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
  }
}

TEST(DistributionDatasetProcessorTest, MNISTDataSet_MINIMAX) {
  DistributionDatasetProcessor<float> ddp;
  auto dimension = 784;
  auto size = 1000;
  std::unique_ptr<float[]> distribution_dataset =
      ddp.MNISTDistributionDataset(1000, "/Users/chenjunhao/LSH/DISTRIBUTION_LSH/raw_dataset/mnist", NormalizationType::MIN_MAX);

  // Test distribution's property
  for (int i = 0; i < size; ++i) {
    float sum = 0;
    for (int j = 0; j < dimension; ++j) {
      sum += distribution_dataset[i * dimension + j];
      ASSERT_TRUE(distribution_dataset[i * dimension + j] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1) < 1E-5);
  }
}

} // namespace distribution_lsh