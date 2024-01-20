//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/6.
// src/include/type/Value.h
//
//===-----------------------------------------------------

#pragma once

#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <fmt/format.h>

#include <type/type.h>

namespace distribution_lsh {
inline auto GetCmpBool(bool boolean) -> CmpBool { return boolean ? CmpBool::CmpTrue : CmpBool::CmpFalse; }

// A value is an abstract class that represents a view for the algorithm
// All values have a type and comparison functions, but subclasses implement
// other type-specific functionality.
class Value {
  // Friend Type classed
  friend class Type;

 public:
 private:
  // The actual value item
  union Val {
   int32_t integer_;
   float float_;
   double double_;
 } value_;

 union {
   uint32_t len_;
   TypeId elem_type_id_;
 } size_;

 bool manage_data_;
 // The data type
 TypeId type_id_;
};

}// namespace distribution_lsh
