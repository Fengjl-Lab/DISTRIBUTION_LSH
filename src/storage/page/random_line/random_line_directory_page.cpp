//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/4/11.
// src/storage/page/random_line/random_line_directory_page.cpp
//
//===-----------------------------------------------------

#include <storage/page/random_line/random_line_directory_page.h>

namespace distribution_lsh {

void RandomLineDirectoryPage::Init(int max_size) {
  SetPageType(RandomLinePageType::DIRECTORY_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
  SetNullSlotStart(0);
  array_[null_slot_start_] = NULL_SLOT_END;
  SetEndOfArray(0);
}

auto RandomLineDirectoryPage::GetNullSlotStart() const -> int { return null_slot_start_; }
void RandomLineDirectoryPage::SetNullSlotStart(int null_slot_start) { null_slot_start_ = null_slot_start; }

auto RandomLineDirectoryPage::GetEndOfArray() const -> int { return end_of_array_; }
void RandomLineDirectoryPage::SetEndOfArray(int end_of_array) { end_of_array_ = end_of_array; }

auto RandomLineDirectoryPage::Insert(distribution_lsh::page_id_t data_page_id, int *index) -> bool {
  if (GetSize() >= GetMaxSize()) {
    *index = -1;
    return false;
  }
  // Search the null slot start
  int null_slot = GetNullSlotStart();

  // Judge if it has arrived at the end of array
  if (null_slot_start_ == GetEndOfArray()) {
    null_slot_start_ ++;
    end_of_array_ ++;
    array_[null_slot_start_] = NULL_SLOT_END;
  } else {
    null_slot_start_ = array_[null_slot_start_];
  }

  array_[null_slot] = data_page_id;

  *index = null_slot;
  IncreaseSize(1);
  return true;
}

auto RandomLineDirectoryPage::Delete(int index) -> bool {
  // Judge if the index is valid
  if (index >= GetEndOfArray()) {
    return false;
  }

  auto null_slot = GetNullSlotStart();
  auto location = -1;

  while (array_[null_slot] != NULL_SLOT_END && index > null_slot) {
    location = null_slot;
    null_slot = array_[null_slot];
  }

  if (index == null_slot) {
    return false;
  }

  location == -1 ? null_slot_start_ = index : array_[location] = index;
  array_[index] = null_slot;
  IncreaseSize(-1);

  return true;
}

auto RandomLineDirectoryPage::IndexAt(int index) const -> page_id_t {
  if (index >= GetEndOfArray()) {
    return INVALID_PAGE_ID;
  }

  auto null_slot = GetNullSlotStart();

  while (array_[null_slot] != NULL_SLOT_END && index > null_slot) {
    null_slot = array_[null_slot];
  }

  if (index == null_slot) {
    return INVALID_PAGE_ID;
  }

  return array_[index];
}

auto  RandomLineDirectoryPage::ToString() -> std::string {
  return fmt::format("random line directory page(size={}, max size={}, next page id={}, null slot starts at: {}, end of array at: {})",
      GetSize(), GetMaxSize(), GetNextPageId(), GetNullSlotStart(), GetEndOfArray());
}
} // namespace distribution_lsh