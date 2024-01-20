//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/5.
// src/include/storage/index/index_iterator.h
//
//===-----------------------------------------------------

#pragma

#include <storage/page/b_plus_tree_leaf_page.h>

namespace distribution_lsh {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  IndexIterator();
  ~IndexIterator();

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { throw std::runtime_error("unimplemented"); }

  auto operator!=(const IndexIterator &itr) const -> bool { throw std::runtime_error("unimplemented"); }

 private:
  // TODO add opaque variable here
};
} // namespace distribution_lsh
