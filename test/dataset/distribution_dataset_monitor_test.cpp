//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/7.
// test/dataset/distribution_dataset_monitor_test.cpp
//
//===-----------------------------------------------------

#include <memory>
#include <filesystem>
#include <dataset/distribution/distribution_dataset_monitor.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

class DistributionDataSetMonitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string raw_data_directory_name("RAW DATA VOID DIRECTORY");
    std::string training_set_directory_name("./dataset/test/training");
    std::string testing_set_directory_name("./dataset/test/testing");
    std::string relation_directory_name("./dataset/test/relation");
    monitor_ = std::make_shared<DistributionDataSetMonitor<float>>(raw_data_directory_name, training_set_directory_name, testing_set_directory_name, relation_directory_name);
  }

  void TearDown() override {}

  std::shared_ptr<DistributionDataSetMonitor<float>> monitor_;
};

TEST_F(DistributionDataSetMonitorTest, GenerationTest) {
  auto index1 = monitor_->GetDataSetIndex(DataSetType::GENERATION, DistributionType::GAUSSIAN, NormalizationType::MIN_MAX, 15, 0.0F, 1.0F, 1000, 0.7);
  ASSERT_EQ(index1, 0);
  auto index2 = monitor_->GetDataSetIndex(DataSetType::GENERATION, DistributionType::GAUSSIAN, NormalizationType::MIN_MAX, 1000, 0.0F, 1.0F, 1000, 0.7);
  ASSERT_EQ(index2, 1);
  auto index3 = monitor_->GetDataSetIndex(DataSetType::GENERATION, DistributionType::GAUSSIAN, NormalizationType::MIN_MAX, 15, 0.0F, 1.0F, 1000, 0.8);
  ASSERT_EQ(index3, 2);
}


TEST_F(DistributionDataSetMonitorTest, GetTest) {
  auto manager_index = monitor_->GetDataSetIndex(DataSetType::GENERATION, DistributionType::GAUSSIAN, NormalizationType::MIN_MAX, 15, 0.0F, 1.0F, 1000, 0.7);
  auto data_index = 0;
  while (true) {
    auto directory_page_id = INVALID_PAGE_ID;
    auto slot = -1;

    auto data = monitor_->GetDistributionData(manager_index, true, data_index, &directory_page_id, &slot);
    if (directory_page_id == INVALID_PAGE_ID || slot == -1) {
      break;
    }

    if (data == nullptr) {
      continue;
    }

    // Test distribution data attribution
    auto sum = 0.0F;
    for (int i = 0; i < 15; ++i) {
      sum += data[i];
      ASSERT_TRUE(data[i] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1.0F) < 1E-5);
    data_index ++;
  }

  manager_index = monitor_->GetDataSetIndex(DataSetType::GENERATION, DistributionType::GAUSSIAN, NormalizationType::MIN_MAX, 1000, 0.0F, 1.0F, 1000, 0.7);
  data_index = 0;
  while (true) {
    auto directory_page_id = INVALID_PAGE_ID;
    auto slot = -1;

    auto data = monitor_->GetDistributionData(manager_index, true, data_index, &directory_page_id, &slot);
    if (directory_page_id == INVALID_PAGE_ID || slot == -1) {
      break;
    }

    if (data == nullptr) {
      continue;
    }

    // Test distribution data attribution
    auto sum = 0.0F;
    for (int i = 0; i < 1000; ++i) {
      sum += data[i];
      ASSERT_TRUE(data[i] >= 0);
    }
    ASSERT_TRUE(std::abs(sum - 1.0F) < 1E-5);
    data_index ++;
  }
}

} // namespace distribution_lsh