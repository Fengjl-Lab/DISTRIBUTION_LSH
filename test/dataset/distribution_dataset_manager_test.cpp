//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/22.
// test/dataset/distribution_dataset_manager_test.cpp
//
//===-----------------------------------------------------

#include <memory>
#include <vector>
#include <common/util/file.h>
#include <storage/disk/disk_manager_memory.h>
#include <dataset/distribution/distribution_dataset_processor.h>
#include <dataset/distribution/distribution_dataset_manager.h>
#include <gtest/gtest.h>
#include <fmt/core.h>

namespace distribution_lsh {

class DistributionDataSetManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    std::string manager_name("manager");
    auto training_disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
    auto training_set_bpm = std::make_shared<BufferPoolManager>(10, training_disk_manager);
    auto testing_disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
    auto testing_set_bpm = std::make_shared<BufferPoolManager>(10, testing_disk_manager);
    auto ddp = std::make_unique<DistributionDatasetProcessor<float>>();
    auto dimension = 15;
    std::shared_ptr<float[2]> params(new float[2]);
    params[0] = 0.0;
    params[1] = 1.0;
    std::string directory_name("fake directory");
    file_id_t training_set_file_id = 0x8000'0000UL | GetHashValue("training set");
    file_id_t testing_set_file_id = 0x8000'0000UL | GetHashValue("testing set");
    auto directory_page_max_size = 10;
    auto data_page_max_size = 10;

    manager_ = std::make_shared<DistributionDataSetManager<float>>(
        manager_name,
        DataSetType::GENERATION,
        DistributionType::GAUSSIAN,
        NormalizationType::MIN_MAX,
        training_set_bpm,
        testing_set_bpm,
        std::move(ddp),
        INVALID_PAGE_ID,
        INVALID_PAGE_ID,
        dimension,
        params,
        directory_name,
        training_set_file_id,
        testing_set_file_id,
        directory_page_max_size,
        data_page_max_size);

    directory_page_max_size_ = directory_page_max_size;
    dimension_ = dimension;
  }

  void TearDown() override {}

  std::shared_ptr<DistributionDataSetManager<float>> manager_;
  int directory_page_max_size_;
  int dimension_;
};

TEST_F(DistributionDataSetManagerTest, GenerationTest) {
  // Test empty
  ASSERT_TRUE(manager_->IsEmpty());
  fmt::print("{}", manager_->ToString());

  // Test generation
  manager_->GenerateDistributionDataset(1000, 0.7);
  ASSERT_EQ(manager_->GetSize(true), 700);
  ASSERT_EQ(manager_->GetSize(false), 300);
}

TEST_F(DistributionDataSetManagerTest, DeleteTest1) {
  // Test empty
  ASSERT_TRUE(manager_->IsEmpty());
  fmt::print("{}", manager_->ToString());

  // Test generation
  manager_->GenerateDistributionDataset(100, 0.7);

  // Test Delete function
  std::vector<int> delete_array({10, 20, 30, 40, 50});
  for (int i : delete_array) {
    page_id_t directory_page_id = INVALID_PAGE_ID;
    int slot;
    auto data = manager_->GetDistributionData(true, i, &directory_page_id, &slot);
    ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
    ASSERT_TRUE(slot < directory_page_max_size_);
    int size = manager_->GetSize(true);
    manager_->Delete(true, directory_page_id, slot);
    ASSERT_EQ(manager_->GetSize(true), size - 1);
  }
}

TEST_F(DistributionDataSetManagerTest, DeleteTest2) {
  // Test empty
  ASSERT_TRUE(manager_->IsEmpty());
  fmt::print("{}", manager_->ToString());

  // Test generation
  manager_->GenerateDistributionDataset(100, 0.7);

  // Test Delete function in one directory page
  std::vector<int> delete_array({10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  for (int i : delete_array) {
    page_id_t directory_page_id = INVALID_PAGE_ID;
    int slot;
    auto data = manager_->GetDistributionData(true, i, &directory_page_id, &slot);
    ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
    ASSERT_TRUE(slot < directory_page_max_size_);
    int size = manager_->GetSize(true);
    manager_->Delete(true, directory_page_id, slot);
    ASSERT_EQ(manager_->GetSize(true), size - 1);
  }
}

TEST_F(DistributionDataSetManagerTest, DeleteTest3) {
  // Test empty
  ASSERT_TRUE(manager_->IsEmpty());
  fmt::print("{}", manager_->ToString());

  // Test generation
  manager_->GenerateDistributionDataset(100, 0.7);

  // Test Delete function in one directory page
  std::vector<int> delete_array({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  for (int i : delete_array) {
    auto size = manager_->GetSize(true);
    manager_->Delete(true, 1, i);
    ASSERT_EQ(manager_->GetSize(true), size - 1);
  }
}

TEST_F(DistributionDataSetManagerTest, GetTest1) {
  // Test empty
  ASSERT_TRUE(manager_->IsEmpty());
  fmt::print("{}", manager_->ToString());

  // Test generation
  manager_->GenerateDistributionDataset(200, 0.6);

  // Test Delete function in one directory page
  for (int index =  0; index < 120 ; index ++) {
    auto directory_page_id = INVALID_PAGE_ID;
    auto slot = NULL_SLOT_END;
    auto data = manager_->GetDistributionData(true, index, &directory_page_id, &slot);
    ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
    auto sum = 0.0F;
    for (int j = 0; j < manager_->GetDimension(); ++j) {
      sum += data[j];
      ASSERT_TRUE(data[j] >= 0);
    }

    ASSERT_TRUE(std::abs(sum - 1.0F) < 1E-5);
  }
}
} // namespace distribution_lsh