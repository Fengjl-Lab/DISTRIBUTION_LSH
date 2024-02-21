//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/21.
// test/storage/b_plus_tree_range_search_test.cpp
//
//===-----------------------------------------------------

#include <algorithm>
#include <cstdio>
#include <cmath>

#include <buffer/buffer_pool_manager.h>
#include <gtest/gtest.h>
#include <storage/disk/disk_manager_memory.h>
#include <storage/index/b_plus_tree.h>

namespace distribution_lsh {

using distribution_lsh::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, RangeTest1) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID > tree("foo_pk", header_page->GetPageId(), bpm, 3, 3);
  float index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = (float)key;
    tree.Insert(index_key, rid);
  }

  std::vector<RID> rids;
  for (size_t i = 0; i < keys.size() - 2; ++i) {
    float lkey  = (float)keys[i];
    float rkey = (float)keys[i + 2];
    rids.clear();
    EXPECT_EQ(tree.RangeRead(lkey, rkey, &rids), true);
    EXPECT_EQ(rids.size(), 3);

    for (size_t j = 0;  j < rids.size(); j++) {
      EXPECT_TRUE(rids[j].GetSlotNum() == keys[i + j]);
    }
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}


}  // namespace distribution_lsh