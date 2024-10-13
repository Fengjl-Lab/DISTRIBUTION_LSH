//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/5.
// test/storage/disk_manager_test.cpp
//
//===-----------------------------------------------------

#include <cstring>

#include <common/exception.h>
#include <storage/disk/disk_manager.h>

#include <gtest/gtest.h>

namespace distribution_lsh {
class DiskManagerTest : public ::testing::Test {
 protected:
  // This function is called before every test.
  void SetUp() override {
    remove("test.db");
    remove("test.log");
  }

  // This function is called after every test.
  void TearDown() override {
    remove("test.db");
    remove("test.log");
  };
};

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ReadWritePageTest) {
  char buf[DISTRIBUTION_LSH_PAGE_SIZE] = {0};
  char data[DISTRIBUTION_LSH_PAGE_SIZE] = {0};
  std::string db_file("test.db");
  auto dm = DiskManager(db_file);
  std::strncpy(data, "A test string.", sizeof(data));

  dm.ReadPage(0, buf);  // tolerate empty read

  dm.WritePage(0, data);
  dm.ReadPage(0, buf);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  std::memset(buf, 0, sizeof(buf));
  dm.WritePage(5, data);
  dm.ReadPage(5, buf);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  dm.ShutDown();
}

// NOLINTNEXTLINE
#ifdef LOG_FILE_NEEDED
TEST_F(DiskManagerTest, ReadWriteLogTest) {
  char buf[16] = {0};
  char data[16] = {0};
  std::string db_file("test.db");
  auto dm = DiskManager(db_file);
  std::strncpy(data, "A test string.", sizeof(data));

  dm.ReadLog(buf, sizeof(buf), 0);  // tolerate empty read

  dm.WriteLog(data, sizeof(data));
  dm.ReadLog(buf, sizeof(buf), 0);
  EXPECT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  dm.ShutDown();
}
#endif

// NOLINTNEXTLINE
TEST_F(DiskManagerTest, ThrowBadFileTest) { EXPECT_THROW(DiskManager("dev/null\\/foo/bar/baz/test.db"), Exception); }
} // namespace distribution_lsh