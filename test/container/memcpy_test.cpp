//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/16.
// test/container/memcpy_test.cpp
//
//===-----------------------------------------------------

#include <iostream>
#include <cstdlib>
#include <gtest/gtest.h>

TEST(MemcpyTest, ByteCopyTest) {
  std::pair<int, float> origin_array[2];
  origin_array[0] = {1,0.1};
  origin_array[1] = {2, 0.2};
  std::pair<int, float> new_array[2];
  memcpy(new_array, origin_array, sizeof(std::pair<int, float>) * 2);
  EXPECT_EQ(new_array[0].first, 1);
  EXPECT_EQ(new_array[1].first, 2);
}