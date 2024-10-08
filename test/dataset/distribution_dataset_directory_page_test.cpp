//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/22.
// test/dataset/distribution_dataset_directory_page_test.cpp
//
//===-----------------------------------------------------

#include <vector>
#include <common/config.h>
#include <storage/page/dataset/distribution_dataset_directory_page.h>
#include <gtest/gtest.h>

namespace distribution_lsh {
TEST(DistributionDataSetDirectoryPageTest, InsertTest) {
  char raw_data[DISTRIBUTION_LSH_PAGE_SIZE];
  auto directory_page = reinterpret_cast<DistributionDataSetDirectoryPage*>(raw_data);
  directory_page->Init(10);

  // Assert page type
  ASSERT_TRUE(directory_page->IsDirectoryPage());

  // Insert 10 data
  for (int i = 0; i < 10; ++i) {
    auto index = 0;
    directory_page->Insert(i, &index);
    ASSERT_EQ(directory_page->GetSize(), i + 1);
    ASSERT_EQ(directory_page->GetNullSlotStart(), i + 1);
    ASSERT_EQ(directory_page->GetEndOfArray(), i + 1);
  }

  // Test for over-stack
  auto  index = 0;
  ASSERT_EQ(directory_page->Insert(11, &index), false);
}

TEST(DistributionDataSetDirectoryPageTest, DeleteTest) {
  char raw_data[DISTRIBUTION_LSH_PAGE_SIZE];
  auto directory_page = reinterpret_cast<DistributionDataSetDirectoryPage*>(raw_data);
  directory_page->Init(10);

  // Insert 10 data
  for (int i = 0; i < 10; ++i) {
    auto index = 0;
    directory_page->Insert(i, &index);
  }

  // Delete Test
  // Test 1
  directory_page->Delete(7);
  ASSERT_EQ(directory_page->GetNullSlotStart(), 7);
  ASSERT_EQ(directory_page->GetEndOfArray(), 10);
  ASSERT_EQ(directory_page->GetSize(), 9);
  ASSERT_EQ(directory_page->IndexAt(7), -1);

  // Test 2
  directory_page->Delete(5);
  ASSERT_EQ(directory_page->GetNullSlotStart(), 5);
  ASSERT_EQ(directory_page->GetEndOfArray(), 10);
  ASSERT_EQ(directory_page->GetSize(), 8);
  ASSERT_EQ(directory_page->IndexAt(5), -1);

  // Test 3
  directory_page->Delete(9);
  ASSERT_EQ(directory_page->GetNullSlotStart(), 5);
  ASSERT_EQ(directory_page->GetEndOfArray(), 10);
  ASSERT_EQ(directory_page->GetSize(), 7);
  ASSERT_EQ(directory_page->IndexAt(7), -1);
}

} // namespace distribution_lsh