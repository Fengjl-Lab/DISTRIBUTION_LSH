//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/10.
// src/include/storage/page/random_line/random_line_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <fmt/format.h>

namespace distribution_lsh {

#define RANDOM_LINE_TEMPLATE template<typename ValueType>
#define RANDOM_LINE_PAGE_HEADER_SIZE 13

enum class RandomLinePageType : std::uint8_t {INVALID_DATA_SET_PAGE_TYPE = 0 , AVERAGE_RANDOM_LINE_PAGE, DIRECTORY_PAGE, DATA_PAGE};

inline auto RandomLinePageTypeToString(RandomLinePageType type) -> std::string {
  switch (type) {
    case RandomLinePageType::INVALID_DATA_SET_PAGE_TYPE:return "INVALID_DATA_SET_PAGE_TYPE";
    case RandomLinePageType::AVERAGE_RANDOM_LINE_PAGE:return "AVERAGE_RANDOM_LINE_PAGE";
    case RandomLinePageType::DIRECTORY_PAGE:return "DIRECTORY_PAGE";
    case RandomLinePageType::DATA_PAGE:return "DATA_PAGE";
    default:throw std::runtime_error("Invalid RandomLinePageType");
  }
}

class RandomLinePage {
 public:
  RandomLinePage() = delete;
  RandomLinePage(const RandomLinePage &other) = delete;
  ~RandomLinePage() = delete;

  // Getter and Setter methods
  auto GetSize() const -> int;
  void SetSize(int size);

  auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);

  auto GetPageType() const -> RandomLinePageType;
  void SetPageType(RandomLinePageType type);

  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);

  void IncreaseSize(int amount);

  virtual auto ToString() -> std::string = 0;

 private:
  RandomLinePageType type_;             // type of page
  int size_;
  int max_size_;
  page_id_t next_page_id_;              // next page id
};
} // namespace distribution_lsh
