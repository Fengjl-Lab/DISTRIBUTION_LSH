//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/12.
// test/container/reinterpret_test.cpp
//
//===-----------------------------------------------------

#include <vector>
#include <iostream>
#include <gtest/gtest.h>

struct TestDataStructure {
  int size_;
  int array_[100];
};

class TestDataClass {
 public:
  void PrintVector() {
    for (int i = 0; i < size_; i++) {
      std::cout << array_[i] << " ";
    }
    std::cout << "\n";
  }

 private:
  int size_;
  int array_[0];
};

TEST(ReinterpretTest, DataTransferTest) {
  auto data_structure =  new TestDataStructure({5, {1, 2, 3, 4, 5}});
  auto data_pointer = reinterpret_cast<unsigned char*>(data_structure);
  auto data_class = reinterpret_cast<TestDataClass*>(data_pointer);

  data_class->PrintVector();
  free(data_structure);
}