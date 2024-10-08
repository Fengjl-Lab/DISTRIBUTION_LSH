//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/6/3.
// test/common/logger_test.cpp
//
//===-----------------------------------------------------

#include <common/logger.h>
#include <gtest/gtest.h>

namespace distribution_lsh {

TEST(LoggerSystemTest, DebugTest) {
  // Test log system
  LOG_DEBUG("TEST DEBUG INFO");
}


}// namespace distribution_lsh