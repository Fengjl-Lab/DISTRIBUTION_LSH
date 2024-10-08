//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/13.
// test/container/file_stream.cpp
//
//===-----------------------------------------------------

#include <filesystem>
#include <fmt/core.h>
#include <gtest/gtest.h>


auto FileFullExTension(const std::string& file_name) -> std::string {
  std::size_t first_dot_pos = file_name.find('.');
  if (first_dot_pos != std::string::npos && first_dot_pos < file_name.length() - 1) {
    return file_name.substr(first_dot_pos);
  }
  return "";
}

TEST(FileSystemTest, ListFileTest) {
  std::string directory_name("/Users/chenjunhao/LSH/DISTRBUTION_LSH/raw_dataset/cifar10/cifar-10-batches-bin");
  for (const auto & file : std::filesystem::directory_iterator(directory_name)) {
    if (file.path().extension() == ".bin") {
      fmt::print("\033[31m Bin file path: {}, name: {} \033[0m\n", file.path().string(), file.path().filename().string());
    }

    if (FileFullExTension(file.path().string()) == ".meta.txt") {
      fmt::print("\033[36m Txt file path: {}, name: {} \033[0m\n", file.path().string(), file.path().filename().string());
    }
  }
}