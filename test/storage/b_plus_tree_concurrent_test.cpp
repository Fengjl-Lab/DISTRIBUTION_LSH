//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/21.
// test/storage/b_plus_tree_concurrent_test.cpp
//
//===-----------------------------------------------------

#include <chrono> // NOLINT
#include <cstdio>
#include <functional>
#include <thread> // NOLINT

#include <buffer/buffer_pool_manager.h>
#include <gtest/gtest.h>
#include <storage/disk/disk_manager_memory.h>
#include <storage/index/b_plus_tree.h>

namespace distribution_lsh {
using distribution_lsh::DiskManagerUnlimitedMemory;

template <typename... Args>
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

// function to insert
void InsertHelper(BPlusTree<float, RID> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  float index_key;
  RID rid;

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = static_cast<float>(key);
    tree->Insert(index_key, rid);
  }
}

// function to separate insert
void InsertHelperSplit(BPlusTree<float, RID> *tree, const std::vector<int64_t> &keys,
                      int total_threads, __attribute__((unused)) uint64_t thread_itr = 0) {
  float index_key;
  RID rid;

  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key = static_cast<float>(key);
      tree->Insert(index_key, rid);
    }
  }
}

// function to delete
void DeleteHelper(BPlusTree<float, RID> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  float index_key;
  RID rid;

  for (auto key : keys) {
    index_key = static_cast<float>(key);
    tree->Delete(index_key);
  }
}

// function to separate delete
void DeleteHelperSplit(BPlusTree<float, RID> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr = 0) {
  float index_key;
  RID rid;

  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key = static_cast<float>(key);
      tree->Delete(index_key);
    }
  }
}

// function to look up
void LookupHelper(BPlusTree<float, RID> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  float index_key;
  RID rid;

  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key = static_cast<float>(key);
    std::vector<RID> result;
    bool res = tree->Get(index_key, &result);
    ASSERT_EQ(res, true);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0], rid);
  }
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);

  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm);
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  float index_key;
  for (auto key : keys) {
    rids.clear();
    index_key = static_cast<float>(key);
    tree.Get(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);

  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm);
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  float index_key;
  for (auto key : keys) {
    rids.clear();
    index_key = static_cast<float>(key);
    tree.Get(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  float index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm);
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  std::vector<RID> rids;
  index_key = static_cast<float>(keys[0]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[2]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[3]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[4]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  float index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm);
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  std::vector<RID> rids;
  index_key = static_cast<float>(keys[0]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[2]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[3]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[4]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, MixTest1) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", header_page->GetPageId(), bpm);
  float index_key;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  std::vector<RID> rids;
  keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  index_key = static_cast<float>(keys[0]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[1]);
  ASSERT_EQ(tree.Get(index_key, &rids), true);
  index_key = static_cast<float>(keys[2]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[3]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[4]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[5]);
  ASSERT_EQ(tree.Get(index_key, &rids), false);
  index_key = static_cast<float>(keys[6]);
  ASSERT_EQ(tree.Get(index_key, &rids), true);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, MixTest2) {
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<float, RID> tree("foo_pk", page_id, bpm);

  // Add perserved_keys
  std::vector<int64_t> perserved_keys;
  std::vector<int64_t> dynamic_keys;
  int64_t total_keys = 50;
  int64_t sieve = 5;
  for (int64_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.push_back(i);
    } else {
      dynamic_keys.push_back(i);
    }
  }
  InsertHelper(&tree, perserved_keys, 1);
  // Check there are 1000 keys in there
  size_t size;

  auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
  auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
  auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };

  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(delete_task);
  tasks.emplace_back(lookup_task);

  size_t num_threads = 6;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  // Check all reserved keys exist
  std::vector<RID> rids;
  float lkey = 1, rkey = 50;
  tree.RangeRead(lkey, rkey, &rids);
  EXPECT_TRUE(rids.size() >= 10);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}
} // namespace distribution_lsh