//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/25.
// test/storage/relation_manager_test.cpp
//
//===-----------------------------------------------------


#include <random>
#include <ctime>
#include <sstream>
#include <storage/index/relation_manager.h>
#include <storage/disk/disk_manager_memory.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

TEST(RelationManagerTest, InsertTest) {
  std::string manager_name("manager");
  std::string directory_name("fake directory");
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager);
  auto file_id = 0xC0 | GetHashValue("relation manager");
  auto relation_manager = std::make_shared<RelationManager<TrainingSetToTestingSetUnion>>(manager_name,
                                                                                          directory_name,
                                                                                          bpm,
                                                                                          RelationFileType::TRAINING_SET_TO_TESTING_SET,
                                                                                          file_id,
                                                                                          INVALID_PAGE_ID,
                                                                                          10);

  ASSERT_EQ(relation_manager->IsEmpty(), true);
  // Insert Test
  int total_size = 1000;
  for (int i = 0; i < total_size; ++i) {
    std::mt19937 rng(std::time(nullptr));
    std::uniform_int_distribution<int> dist(0, 9);

    // Generate a random training set file id
    std::stringstream ss;
    for (int j = 0; j < 10; ++j) {
      ss << dist(rng);
    }
    std::string training_set_string = ss.str();
    file_id_t training_set_file_id = 0x8000 | GetHashValue(training_set_string);

    ss.clear();
    for (int j = 0; j < 10; ++j) {
      ss << dist(rng);
    }
    std::string testing_set_string = ss.str();
    file_id_t testing_set_file_id = 0x8000 | GetHashValue(testing_set_string);

    TrainingSetToTestingSetUnion union_data{.map_ = {static_cast<file_id_t>(training_set_file_id), static_cast<file_id_t>(testing_set_file_id)}};
    auto index = 0;
    ASSERT_TRUE(relation_manager->Insert(union_data, &index));
  }

}


TEST(RelationManagerTest, GetTest) {
  std::string manager_name("manager");
  std::string directory_name("fake directory");
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager);
  auto file_id = 0xC0 | GetHashValue("relation manager");
  auto relation_manager = std::make_shared<RelationManager<TrainingSetToTestingSetUnion>>(manager_name,
                                                                                          directory_name,
                                                                                          bpm,
                                                                                          RelationFileType::TRAINING_SET_TO_TESTING_SET,
                                                                                          file_id,
                                                                                          INVALID_PAGE_ID,
                                                                                          10);

  ASSERT_EQ(relation_manager->IsEmpty(), true);
  // Insert Test
  int total_size = 1000;
  for (int i = 0; i < total_size; ++i) {
    TrainingSetToTestingSetUnion union_data{.map_ = {static_cast<file_id_t>(i), static_cast<file_id_t>(i + 1)}};
    auto index = 0;
    ASSERT_TRUE(relation_manager->Insert(union_data, &index));
  }

  // Get Test
  for (int i = 0; i < total_size; ++i) {
    auto page_id = INVALID_PAGE_ID;
    auto index = 0;
    ASSERT_EQ(relation_manager->Get(i, &page_id, &index).map_.training_set_file_id_, static_cast<file_id_t>(i));
  }
}
} // namespace distribution_lsh