//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/5.
// test/storage/disk_scheduler.cpp
//
//===-----------------------------------------------------

#include <cstring>
#include <future> // NOLINT
#include <memory>

#include <common/exception.h>
#include <storage/disk/disk_scheduler.h>
#include <storage/disk/disk_manager_memory.h>

#include <gtest/gtest.h>

namespace qalsh {

using qalsh::DiskManagerUnlimitedMemory;

// NOLINTNEXTLINE
TEST(DiskSchedulerTest, ScheduleWriteReadPageTest) {
  char buf[QALSH_PAGE_SIZE] = {0};
  char data[QALSH_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, reinterpret_cast<char *>(&data), /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf), /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

} // namespace qalsh