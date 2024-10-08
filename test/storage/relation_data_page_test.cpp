//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/28.
// test/storage/relation_data_page_test.cpp
//
//===-----------------------------------------------------

#include <common/config.h>
#include <storage/page/relation/relation_data_page.h>
#include <gtest/gtest.h>
#include <array>

namespace distribution_lsh {

TEST(RelationDataPageTest, InsertTest) {
  std::array<char, DISTRIBUTION_LSH_PAGE_SIZE> raw_data{};
  auto data_page = reinterpret_cast<RelationDataPage<TrainingSetToTestingSetUnion>*>(raw_data.data());
  data_page->Init(10);

  for (int i = 0; i < 10; ++i) {
    TrainingSetToTestingSetUnion union_data{.map_ = {static_cast<file_id_t>(i), static_cast<file_id_t>(i + 1)}};
    auto index = 0;
    data_page->Insert(union_data, &index);
    ASSERT_EQ(data_page->GetSize(), i + 1);
    ASSERT_EQ(data_page->GetNullSlotStart(), i + 1);
    ASSERT_EQ(data_page->GetEndOfArray(), i + 1);
  }
}

TEST(RelationDataPageTest, DeleteTest) {
  std::array<char, DISTRIBUTION_LSH_PAGE_SIZE> raw_data{};
  auto data_page = reinterpret_cast<RelationDataPage<TrainingSetToTestingSetUnion>*>(raw_data.data());
  data_page->Init(10);

  for (int i = 0; i < 10; ++i) {
    TrainingSetToTestingSetUnion union_data{.map_ = {static_cast<file_id_t>(i), static_cast<file_id_t>(i + 1)}};
    auto index = 0;
    data_page->Insert(union_data, &index);
    ASSERT_EQ(data_page->GetSize(), i + 1);
    ASSERT_EQ(data_page->GetNullSlotStart(), i + 1);
    ASSERT_EQ(data_page->GetEndOfArray(), i + 1);
  }

  // Delete Test
  // Test 1
  data_page->Delete(7);
  ASSERT_EQ(data_page->GetNullSlotStart(), 7);
  ASSERT_EQ(data_page->GetEndOfArray(), 10);
  ASSERT_EQ(data_page->GetSize(), 9);
  ASSERT_EQ(data_page->Get(7).next_null_slot_, -1);

  // Test 2
  data_page->Delete(5);
  ASSERT_EQ(data_page->GetNullSlotStart(), 5);
  ASSERT_EQ(data_page->GetEndOfArray(), 10);
  ASSERT_EQ(data_page->GetSize(), 8);
  ASSERT_EQ(data_page->Get(7).next_null_slot_, -1);

  // Test 3
  data_page->Delete(9);
  ASSERT_EQ(data_page->GetNullSlotStart(), 5);
  ASSERT_EQ(data_page->GetEndOfArray(), 10);
  ASSERT_EQ(data_page->GetSize(), 7);
  ASSERT_EQ(data_page->Get(7).next_null_slot_, -1);
}

} // namespace distribution_lsh