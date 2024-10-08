//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/2.
// test/dataset/distribution_dataset_manager_concurrent_test.cpp
//
//===-----------------------------------------------------


#include <chrono> // NOLINT
#include <cstdio>
#include <functional>
#include <thread>

#include <buffer/buffer_pool_manager.h>
#include <dataset/distribution/distribution_dataset_manager.h>
#include <storage/disk/disk_manager_memory.h>

#include <gtest/gtest.h>

namespace distribution_lsh {
using distribution_lsh::DiskManagerUnlimitedMemory;

template<typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&... args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// Function to generate
void GenerateFunction(DistributionDataSetManager<float> *manager,
                      int size,
                      float ratio,
                      __attribute__((unused)) uint64_t thread_itr) {
  manager->GenerateDistributionDataset(size, ratio);
}

// Function to generate split
void GenerateFunctionSplit(DistributionDataSetManager<float> *manager,
                           int size,
                           float ratio,
                           int total_threads,
                           __attribute__((unused)) uint64_t thread_itr = 0) {
  auto portion = size / total_threads;
  if (thread_itr == 0) {
    manager->GenerateDistributionDataset(portion + size - total_threads * portion, ratio);
  } else {
    manager->GenerateDistributionDataset(portion, ratio);
  }
}

// Function to delete
void DeleteFunction(DistributionDataSetManager<float> *manager,
                    const std::vector<int> &delete_indexes,
                    __attribute__((unused)) uint64_t thread_itr) {
  for (const auto &index : delete_indexes) {
    auto directory_page_id = INVALID_PAGE_ID;
    auto slot = NULL_SLOT_END;
    auto data = manager->GetDistributionData(true, index, &directory_page_id, &slot);
    ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
    int size = manager->GetSize(true);
    manager->Delete(true, directory_page_id, slot);
    ASSERT_EQ(manager->GetSize(true), size - 1);
  }
}

// Function to delete split
void DeleteFunctionSplit(DistributionDataSetManager<float> *manager,
                         const std::vector<page_id_t> &delete_indexes,
                         int total_threads,
                         __attribute__((unused)) uint64_t thread_itr) {
  for (size_t i = 0; i < delete_indexes.size(); ++i) {
    if (static_cast<uint64_t>(i) % total_threads == thread_itr) {
      auto directory_page_id = INVALID_PAGE_ID;
      auto slot = NULL_SLOT_END;
      auto data = manager->GetDistributionData(true, delete_indexes[i], &directory_page_id, &slot);
      ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
      manager->Delete(true, directory_page_id, slot);
    }
  }
}


// Function to get split
void GetFunctionSplit(DistributionDataSetManager<float> *manager,
                      const std::vector<int> &get_indexes,
                      int total_threads,
                      __attribute__((unused)) uint64_t thread_itr) {
  for (const auto &index : get_indexes) {
    if (static_cast<uint64_t>(index) % total_threads == thread_itr) {
      auto directory_page_id = INVALID_PAGE_ID;
      auto slot = NULL_SLOT_END;
      auto data = manager->GetDistributionData(true, index, &directory_page_id, &slot);
      ASSERT_NE(directory_page_id, INVALID_PAGE_ID);
      auto sum = 0.0F;
      for (int j = 0; j < manager->GetDimension(); ++j) {
        sum += data[j];
        ASSERT_TRUE(data[j] >= 0);
      }

      ASSERT_TRUE(std::abs(sum - 1.0F) < 1E-5);
    }
  }
}

class DistributionDataSetManagerConcurrentTest : public ::testing::Test {
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
    file_id_t training_set_file_id = 0x8000 | GetHashValue("training set");
    file_id_t testing_set_file_id = 0x8000 | GetHashValue("testing set");
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
  }

  void TearDown() override {}

  std::shared_ptr<DistributionDataSetManager<float>> manager_;
};

TEST_F(DistributionDataSetManagerConcurrentTest, GenerationTest) {
  LaunchParallelTest(10, GenerateFunctionSplit, manager_.get(), 1000, 0.7, 10);
  ASSERT_EQ(manager_->GetSize(true), 700);
  ASSERT_EQ(manager_->GetSize(false), 300);
}

TEST_F(DistributionDataSetManagerConcurrentTest, DeleteTest) {
  LaunchParallelTest(10, GenerateFunctionSplit, manager_.get(), 1000, 0.7, 10);
  ASSERT_EQ(manager_->GetSize(true), 700);
  ASSERT_EQ(manager_->GetSize(false), 300);

  // Test Delete function in one directory page
  std::vector<int> delete_array({10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
  auto size = manager_->GetSize(true);
  LaunchParallelTest(4, DeleteFunctionSplit, manager_.get(), delete_array, 4);
  ASSERT_EQ(manager_->GetSize(true), size - delete_array.size());
}

TEST_F(DistributionDataSetManagerConcurrentTest, GetTest) {
  LaunchParallelTest(10, GenerateFunctionSplit, manager_.get(), 1000, 0.7, 10);
  ASSERT_EQ(manager_->GetSize(true), 700);
  ASSERT_EQ(manager_->GetSize(false), 300);

  // Test Delete function in one directory page
  std::vector<int> get_array({0, 11, 10, 500, 29, 78, 100, 250, 213, 323});
  LaunchParallelTest(10, GetFunctionSplit, manager_.get(), get_array, 10);
}


} // namespace distribution_lsh