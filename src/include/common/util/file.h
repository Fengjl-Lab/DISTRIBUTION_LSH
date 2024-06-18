//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/14.
// src/include/common/util/file.h
//
//===-----------------------------------------------------

#pragma once

#include <string>
#include <common/config.h>

namespace distribution_lsh {

/**
* @brief Obtain the full extension of the file
* @param file_name
* @return the full extension of the file
*/
inline auto FileFullExTension(const std::string &file_name) -> std::string {
  std::size_t first_dot_pos = file_name.find('.');
  if (first_dot_pos != std::string::npos && first_dot_pos < file_name.length() - 1) {
    return file_name.substr(first_dot_pos);
  }
  return "";
}

inline auto GetHashValue(const std::string& input) -> file_id_t {
  std::hash<std::string> hasher;
  size_t origin_hash_value = hasher(input);

  file_id_t hash_value = origin_hash_value & 0x3FFF;

  return hash_value;
}


} // namespace distribution_lsh
