//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/19.
// test/storage/b_plus_tree_delete_test.cpp
//
//===-----------------------------------------------------

#include <algorithm>
#include <cstdio>

#include <buffer/buffer_pool_manager.h>
#include <gtest/gtest.h>
#include <storage/disk/disk_manager_memory.h>
#include <storage/index/b_plus_tree.h>

namespace distribution_lsh {

using distribution_lsh::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DeleteTest1) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(50, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm, 3, 3);
  float index_key;
  RID rid;

  // Test for empty tree
  float empty_key = 0;
  EXPECT_EQ(tree.Delete(empty_key), false);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = (float) key;
    tree.Insert(index_key, rid);
  }

  // Test for delete unexists keys
  std::vector<int64_t> unexist_keys = {6, 7, 8, 9, 10};
  EXPECT_EQ(tree.Delete(empty_key), false);
  for (auto key : unexist_keys) {
    index_key = (float) key;
    EXPECT_EQ(tree.Delete(index_key), false);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key = (float) key;
    tree.Get(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);

    rids.clear();
    tree.Delete(index_key);
    EXPECT_EQ(rids.size(), 0);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
}

TEST(BPlusTreeTests, DeleteTest2) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm, 3, 3);
  float index_key;
  RID rid;

  // Test for empty tree
  float empty_key = 0;
  EXPECT_EQ(tree.Delete(empty_key), false);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = (float) key;
    tree.Insert(index_key, rid);
  }

  // Test for delete unexists keys
  std::vector<int64_t> unexist_keys = {6, 7, 8, 9, 10};
  EXPECT_EQ(tree.Delete(empty_key), false);
  for (auto key : unexist_keys) {
    index_key = (float) key;
    EXPECT_EQ(tree.Delete(index_key), false);
  }

  std::vector<RID> rids;
  std::reverse(keys.begin(), keys.end());
  for (auto key : keys) {
    rids.clear();
    index_key = (float) key;
    tree.Get(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);

    rids.clear();
    tree.Delete(index_key);
    EXPECT_EQ(rids.size(), 0);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
}

TEST(BPlusTreeTests, DeleteTest3) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(50, disk_manager);

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm, 3, 3);
  float index_key;
  RID rid;

  // Test for empty tree
  float empty_key = 0;
  EXPECT_EQ(tree.Delete(empty_key), false);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = (float) key;
    tree.Insert(index_key, rid);
  }

  std::vector<RID> rids;
  // Test for random delete
  auto key = (float) 3;
  tree.Delete(key);
  tree.Get(key, &rids);
  EXPECT_EQ(rids.size(), 0);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
}
} // namespace distribution_lsh