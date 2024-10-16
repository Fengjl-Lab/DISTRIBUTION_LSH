//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/6.
// src/include/common/rid.h
//
//===-----------------------------------------------------

#pragma once

#include <cstdint>
#include <sstream>
#include <string>

#include <common/config.h>

namespace distribution_lsh {

class RID {
 public:
  /** The Default constructor creates an invalid RID! */
  RID() = default;

  /**
   * Creates a new Record Identifier for the given page identifier and slot number.
   * @param page_id page identifier (high 32 bit)
   * @param slot_num slot number   (low 32 bit)
   */
  RID(page_id_t page_id, uint32_t slot_num) : page_id_(page_id), slot_num_(slot_num) {}

  explicit RID(int64_t rid) : page_id_(static_cast<page_id_t>(rid >> 32)), slot_num_(static_cast<uint32_t>(rid)) {}

  inline auto Get() const -> int64_t { return (static_cast<int64_t>(page_id_)) << 32 | slot_num_; }

  inline auto GetPageId() const -> int32_t { return page_id_; }

  inline auto GetSlotNum() const -> uint32_t { return slot_num_; }

  inline void Set(page_id_t page_id, uint32_t slot_num) {
    page_id_ = page_id;
    slot_num_ = slot_num;
  }

  inline auto ToString() const -> std::string {
    std::stringstream os;
    os << "page_id: " << page_id_;
    os << " slot_num: " << slot_num_ << "\n";

    return os.str();
  }

  friend auto operator<<(std::ostream &os, const RID &rid) -> std::ostream & {
    os << rid.ToString();
    return os;
  }

  auto operator==(const RID &other) const -> bool { return page_id_ == other.page_id_ && slot_num_ == other.slot_num_; }

  auto operator<(const RID &other) const -> bool {
    return page_id_ < other.page_id_ || (page_id_ == other.page_id_ && slot_num_ < other.slot_num_);
  }

 private:
  page_id_t page_id_{INVALID_PAGE_ID};
  uint32_t slot_num_{0}; // logical offset
};

template <typename T>
struct has_rid_feature : std::false_type {};

template <>
struct has_rid_feature<RID> : std::true_type {};

}// namespace distribution_lsh
