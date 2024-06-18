//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/14.
// test/container/hasher_test.cpp
//
//===-----------------------------------------------------

#include <iostream>
#include <functional>
#include <gtest/gtest.h>

uint8_t GetSixBitHash(const std::string& input) {
  std::hash<std::string> hasher;
  size_t hash_value = hasher(input);

  uint8_t six_bit_hash = (hash_value & 0x3F) | 0xC0;

  return six_bit_hash;
}

TEST(HashValueTest, InputInt) {
  uint8_t six_bit_hash = GetSixBitHash(std::to_string(0));

  std::cout << "6-bit Hash: " << static_cast<int>(six_bit_hash) << "\n";

  int head_two_number = static_cast<int>(six_bit_hash & 0xC0) >> 6;

  std::cout << "2-bit 3: " << head_two_number << "\n";
}