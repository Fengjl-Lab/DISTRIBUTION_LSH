//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/storage/page/relation/relation_data.cpp
//
//===-----------------------------------------------------

#include <storage/page/relation/relation_data_page.h>

namespace distribution_lsh {

RELATION_TEMPLATE
void RELATION_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetNullSlotStart(0);
  array_[null_slot_start_].next_null_slot_ = NULL_SLOT_END;
  SetEndOfArray(0);
}

RELATION_TEMPLATE
auto RELATION_TYPE::GetSize() const -> int { return size_; }

RELATION_TEMPLATE
void  RELATION_TYPE::SetSize(int size) { size_ = size; }

RELATION_TEMPLATE
auto RELATION_TYPE::GetMaxSize() const -> int { return max_size_; }

RELATION_TEMPLATE
void RELATION_TYPE::SetMaxSize(int max_size) { max_size_ = max_size; }

RELATION_TEMPLATE
auto RELATION_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

RELATION_TEMPLATE
void RELATION_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

RELATION_TEMPLATE
void RELATION_TYPE::IncreaseSize(int amount) {
  size_ += amount;
}

RELATION_TEMPLATE
auto RELATION_TYPE::GetNullSlotStart() const -> int { return null_slot_start_; }

RELATION_TEMPLATE
void RELATION_TYPE::SetNullSlotStart(int null_slot_start) { null_slot_start_ = null_slot_start; }


RELATION_TEMPLATE
auto RELATION_TYPE::GetEndOfArray() const -> int { return end_of_array_; }

RELATION_TEMPLATE
void RELATION_TYPE::SetEndOfArray(int end_of_array) { end_of_array_ = end_of_array; }


RELATION_TEMPLATE
auto RELATION_TYPE::Insert(ValueType value, int *index) -> bool {
  if (GetSize() >= GetMaxSize()) {
    return false;
  }
  // Search the null slot start
  int null_slot = GetNullSlotStart();

  // Judge if it has arrived at the end of array
  if (null_slot_start_ == GetEndOfArray()) {
    null_slot_start_ ++;
    end_of_array_ ++;
    array_[null_slot_start_].next_null_slot_ = NULL_SLOT_END;
  } else {
    null_slot_start_ = array_[null_slot_start_].next_null_slot_;
  }

  array_[null_slot] = value;
  *index = null_slot;
  IncreaseSize(1);
  return true;
}

RELATION_TEMPLATE
auto RELATION_TYPE::Delete(int index) -> bool {
  // Judge if the index is valid
  if (index >= GetEndOfArray()) {
    return false;
  }

  auto null_slot = GetNullSlotStart();
  auto location = -1;

  while (array_[null_slot].next_null_slot_ != NULL_SLOT_END && index > null_slot) {
    location = null_slot;
    null_slot = array_[null_slot].next_null_slot_;
  }

  if (index == null_slot) {
    return false;
  }

  location == -1 ? null_slot_start_ = index : array_[location].next_null_slot_ = index;
  array_[index].next_null_slot_ = null_slot;
  IncreaseSize(-1);

  return true;
}

RELATION_TEMPLATE
auto RELATION_TYPE::Get(int index) const -> ValueType {
  if (index >= GetEndOfArray()) {
    return {.next_null_slot_ = NULL_SLOT_END};
  }

  auto null_slot = GetNullSlotStart();

  while (array_[null_slot].next_null_slot_ != NULL_SLOT_END && index > null_slot) {
    null_slot = array_[null_slot].next_null_slot_;
  }

  if (index == null_slot) {
    return {.next_null_slot_ = INVALID_SLOT_VALUE};
  }

  return array_[index];
}

template class RelationDataPage<TrainingSetToTestingSetUnion>;
template class RelationDataPage<RandomLineFileToBPlusTreeFileUnion>;
} // namespace distribution_lsh