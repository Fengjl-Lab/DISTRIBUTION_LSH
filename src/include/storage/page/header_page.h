//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/14.
// src/include/storage/page/header_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/exception.h>
#include <common/config.h>
#include <common/util/file.h>

namespace distribution_lsh {

#define COMMON_HEADER_PAGE_HEADER_SIZE 16

enum class FileType : std::uint8_t {INVALID_FILE_TYPE = 0, RANDOM_LINE_FILE, B_PLUS_TREE_FILE, DISTRIBUTION_DATASET_FILE, RELATION_FILE};

/**
 * @brief The header page for identification of the file type
 */
class HeaderPage {
 public:
  /** Delete all constructor / destructor to ensure memory safety */
  HeaderPage() = delete;
  HeaderPage(const HeaderPage &other) = delete;
  ~HeaderPage() = delete;

  [[nodiscard]] auto GetFileType() const -> FileType {
    switch (static_cast<uint8_t>(file_identification_ >> 30) & 0x03) {
      case 0x00: return FileType::RANDOM_LINE_FILE;
      case 0x01: return FileType::B_PLUS_TREE_FILE;
      case 0x02: return FileType::DISTRIBUTION_DATASET_FILE;
      case 0x03: return FileType::RELATION_FILE;
      default: return FileType::INVALID_FILE_TYPE;
    }
  }

  [[nodiscard]] auto GetFileIdentification() const -> file_id_t { return file_identification_; }

  void SetFileIdentification(file_id_t file_id) {
    file_identification_ = file_id;
  }

  auto SetFileIdentification(FileType file_type, int random_number) -> file_id_t {
    file_id_t hash_value = GetHashValue(std::to_string(random_number));

    switch (file_type) {
      case FileType::INVALID_FILE_TYPE: throw Exception("Invalid file type");
      case FileType::RANDOM_LINE_FILE: {
        file_identification_ = hash_value;
        break;
      }
      case FileType::B_PLUS_TREE_FILE: {
        file_identification_ = hash_value | 0x0000'0000'4000'0000UL;
        break;
      }
      case FileType::DISTRIBUTION_DATASET_FILE: {
        file_identification_ = hash_value | 0x0000'0000'8000'0000UL;
        break;
      }
      case FileType::RELATION_FILE: {
        file_identification_ = hash_value | 0x0000'0000'C000'0000UL;
        break;
      }
      default: throw Exception("Unsupported file type");
    }

    return file_identification_;
  }

  [[nodiscard]] auto GetNullPageSlotStart() const -> int {return null_page_slot_start_; }
  void SetNullPageSlotStart(int null_page_slot_start) { null_page_slot_start_ = null_page_slot_start; }

 private:
  file_id_t file_identification_;
  int null_page_slot_start_{0};
};
} // namespace distribution_lsh
