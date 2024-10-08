//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/18.
// src/include/storage/page/datatset/distribution_dataset_page.h
//
//===-----------------------------------------------------

#pragma once

#include <common/config.h>
#include <storage/page/header_page.h>
#include <storage/page/data_page.h>

namespace distribution_lsh {

#define DISTRIBUTION_DATASET_PAGE_HEADER_SIZE 24

enum class DistributionDataSetPageType : std::uint8_t {INVALID_DATA_SET_PAGE_TYPE = 0 , DIRECTORY_PAGE, DATA_PAGE};

class DistributionDataSetPage : public DataPage {
  template<class ValueType>
  friend class DistributionDataSetManager;
 public:
  DistributionDataSetPage() = delete;
  DistributionDataSetPage(const DistributionDataSetPage &other) = delete;
  ~DistributionDataSetPage() = delete;

  [[nodiscard]] auto IsDirectoryPage() const -> bool;
  void SetPageType(DistributionDataSetPageType page_type);

  [[nodiscard]] auto GetSize() const -> int;
  void SetSize(int size);

  [[nodiscard]] auto GetMaxSize() const -> int;
  void SetMaxSize(int max_size);

  [[nodiscard]] auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);

  void IncreaseSize(int amount);

  virtual auto ToString() -> std::string = 0;

 private :
  int size_;
  int max_size_;
  page_id_t next_page_id_;
  DistributionDataSetPageType page_type_;
};

} // namespace distribution_lsh
