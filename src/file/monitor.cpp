//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/file/monitor.cpp
//
//===-----------------------------------------------------

#include <sstream>
#include <file/monitor.h>

namespace distribution_lsh {

auto Monitor::GenerateFileIdentification(const std::string &directory_name, FileType type) -> file_id_t {
  file_id_t file_identification  = 0;
  std::mt19937 rng(std::time(nullptr));
  std::uniform_int_distribution<int> dist(0, 9);

  while (true) {
    // Generate a random  file id
    std::stringstream ss;
    for (int j = 0; j < 10; ++j) {
      ss << dist(rng);
    }

    auto file_string = ss.str();
    auto hash_value = GetHashValue(file_string);
    switch (type) {
      case FileType::INVALID_FILE_TYPE: throw Exception("Invalid file type");
      case FileType::RANDOM_LINE_FILE: {
        file_identification = hash_value;
        break;
      }
      case FileType::B_PLUS_TREE_FILE: {
        file_identification = hash_value | 0x4000;
        break;
      }
      case FileType::DISTRIBUTION_DATASET_FILE: {
        file_identification = hash_value | 0x8000;
        break;
      }
      case FileType::RELATION_FILE: {
        file_identification = hash_value | 0xC000;
        break;
      }
      default: throw Exception("Unsupported file type");
    }

    // Check if there has existing file with the same file identification
    auto collision = false;
    for (const auto &entry : std::filesystem::directory_iterator{std::filesystem::path{directory_name}}) {
      if (file_identification == static_cast<file_id_t >(std::stoull(entry.path().filename()))) {
        collision = true;
        break;
      }
    }

    if (!collision) {
      break;
    }
  }


  return file_identification;
}
} // namespace distribution_lsh