//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/9/18.
// test/file/random_line_monitor_test.cpp
//
//===-----------------------------------------------------

#include <memory>
#include <filesystem>

#include <dataset/distribution/distribution_dataset_processor.h>
#include <file/random_line_monitor.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

class RandomLineMonitorTest : public ::testing::Test {
  protected:
    void SetUp() override {
      std::string b_plus_tree_directory_name("./distribution_lsh/b_plus_tree/test/");
      std::string random_line_directory_name("./distribution_lsh/random_line/test/");
      std::string relation_directory_name("./distribution_lsh/relation/test/");
      rlm_ = std::make_shared<RandomLineMonitor<float>>(b_plus_tree_directory_name, random_line_directory_name, relation_directory_name);
    }

    void TearDown() override {}

    std::shared_ptr<RandomLineMonitor<float>> rlm_;
};

TEST_F(RandomLineMonitorTest, RandomProjectionTest1) {
  auto params = std::make_shared<float []>(2);
  params[0] = 0.0F;
  params[1] = 1.0F;
  auto ddp = std::make_shared<DistributionDatasetProcessor<float>>();
  std::shared_ptr<float []> data = ddp->GenerationDistributionDataset(
      100,
      100,
      DistributionType::UNIFORM,
      NormalizationType::MIN_MAX,
      params.get());
  for (int i = 0; i < 100; ++i) {
    auto result = rlm_->RandomProjection(
        100,
        {data, data.get() + i * 100},
        RandomLineDistributionType::GAUSSIAN,
        RandomLineNormalizationType::NONE,
        1,
        20,
        0xFFFFUL,
        RID(0, i));
  }

  rlm_->List();
}

} // namespace distribution_lsh