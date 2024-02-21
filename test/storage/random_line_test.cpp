//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/2/19.
// test/storage/random_line_page_test.cpp
//
//===-----------------------------------------------------

#include <fmt/core.h>

#include <buffer/buffer_pool_manager.h>
#include <random/random_line_generator.h>
#include <storage/index/random_line_manager.h>
#include <storage/disk/disk_manager_memory.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

using distribution_lsh::DiskManagerUnlimitedMemory;
using distribution_lsh::RandomLineGenerator;

TEST(RandomLineManagerTest, GenerationTest1) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = new BufferPoolManager(50, disk_manager.get());
  auto rlg = std::make_unique<RandomLineGenerator>();
  auto dimension = 100;
  auto rlm = new RandomLineManager("random line manager", bpm, rlg.get(), INVALID_PAGE_ID, dimension);

  // Test if the current random line group is empty
  ASSERT_TRUE(rlm->IsEmpty());

  // Test generation process
  ASSERT_TRUE(rlm->GenerateRandomLineGroup(10));
  ASSERT_EQ(rlm->GetSize(), 10);

  delete rlm;
  delete bpm;
}

TEST(RandomLineManagerTest, GenerationTest2_LARGE_DIMENSION) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = new BufferPoolManager(50, disk_manager.get());
  auto rlg = std::make_unique<RandomLineGenerator>();
  auto dimension = 3000;
  auto rlm = new RandomLineManager("random line manager", bpm, rlg.get(), INVALID_PAGE_ID, dimension, 100);

  // Test if the current random line group is empty
  ASSERT_TRUE(rlm->IsEmpty());

  // Test generation process
  ASSERT_TRUE(rlm->GenerateRandomLineGroup(10));
  rlm->PrintRandomLineGroup();

  delete rlm;
  delete bpm;
}

} // namespace distribution_lsh
