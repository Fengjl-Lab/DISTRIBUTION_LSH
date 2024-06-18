//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/18.
// test/storage/b_plus_tree_insert_test.cpp
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

TEST(BPlusTreeTests, InsertTest1) {

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(50, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  ASSERT_EQ(page_id, HEADER_PAGE_ID);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm, 2, 3);
  float index_key;
  RID rid;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key = (float) key;
  tree.Insert(index_key, rid);

  auto root_page_id = tree.GetRootPageId();
  auto root_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id)->GetData());
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf = reinterpret_cast<BPlusTreeLeafPage<float, RID> *>(root_page);
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
}


TEST(BPlusTreeTests, InsertTest2) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(50, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID > tree("foo_pk", header_page->GetPageId(), bpm, 3, 3);
  float index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = (float)key;
    tree.Insert(index_key, rid);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key = (float)key;
    tree.Get(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t size = 0;

  for (auto key : keys) {
    rids.clear();
    index_key = (float)key;
    tree.Get(index_key, &rids);

    // EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    // EXPECT_EQ(rids[0].GetPageId(), 0);
    EXPECT_EQ(rids[0].GetSlotNum(), key);
    size = size + 1;
  }

  EXPECT_EQ(size, keys.size());
}


}  // namespace distribution_lsh