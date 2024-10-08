//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2023/11/17.
// src/storage/index/b_plus_tree.cpp
//
//===-----------------------------------------------------

#include <iostream>
#include <cmath>
#include <stack>
#include <tuple>

#include <common/logger.h>
#include <common/macro.h>
#include <storage/index/b_plus_tree.h>

namespace distribution_lsh {

INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_TYPE::BPlusTree(std::string name,
                            distribution_lsh::page_id_t header_page_id,
                            std::shared_ptr<distribution_lsh::BufferPoolManager> buffer_pool_manager,
                            int leaf_max_size,
                            int internal_max_size) : \
                            index_name_(std::move(name)), bpm_(std::move(buffer_pool_manager)), leaf_max_size_(leaf_max_size), \
                            internal_max_size_(internal_max_size), header_page_id_(header_page_id) {
  // We need allocate the header_page
  if (IsEmpty()) {
    auto header_page_guard = bpm_->NewPageGuarded(&header_page_id_);
    if (header_page_id_ == INVALID_PAGE_ID) {
      LOG_DEBUG("Allocate header page failed");
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::IsEmpty(Context *ctx, bool is_read) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_page = ctx != nullptr && !is_read ? [&]() {
    if (ctx->header_page_.has_value()) {
      return ctx->header_page_.value().template As<BPlusTreeHeaderPage>();
    }
    ctx->header_page_ = bpm_->FetchPageWrite(header_page_id_);
    return ctx->header_page_.value().template As<BPlusTreeHeaderPage>();
  }() : [&]() {
    return ctx != nullptr && ctx->header_page_.has_value() ?
          ctx->header_page_.value().template As<BPlusTreeHeaderPage>()
          : bpm_->FetchPageRead(header_page_id_).template As<BPlusTreeHeaderPage>();
  }();
  return header_page->root_page_id_ == INVALID_PAGE_ID || header_page->root_page_id_ == HEADER_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::GetRootPageId() -> page_id_t {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::Get(const BPlusTreeKeyType &key, std::vector<BPlusTreeValueType> *result) -> bool {
  if (IsEmpty()) {
    return false;
  }

  Context ctx;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  ctx.read_set_.emplace_back(std::move(root_page_guard));
  auto current_page = ctx.read_set_.back().template As<BPlusTreePage>();

  while (!current_page->IsLeafPage()) {
    const InternalPage *internal_page = ctx.read_set_.back().template As<InternalPage>();
    auto pos = 0;
    while (pos < internal_page->GetSize() && internal_page->KeyAt(pos + 1) < key) { pos++; }
    // Obtain correct number
    pos = pos != internal_page->GetSize() && std::abs(key - internal_page->KeyAt(pos + 1)) <= 1E-10 ? pos + 1 : pos;

    // Traverse to the leaf child
    ctx.read_set_.emplace_back(std::move(bpm_->FetchPageRead(internal_page->ValueAt(pos))));
    current_page = ctx.read_set_.back().template As<BPlusTreePage>();
  }

  const auto leaf_page = reinterpret_cast<const LeafPage *>(current_page);
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (std::abs(key - leaf_page->array_[i].first) <= 1E-10) {
      result->emplace_back(leaf_page->array_[i].second);
      return true;
    }
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::Insert(const BPlusTreeKeyType &key, const BPlusTreeValueType &value) -> bool {
  Context ctx;
  // Create a new tree it is empty
  if (IsEmpty(&ctx, false)) {
    auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
    auto basic_page_guard = bpm_->NewPageGuarded(&header_page->root_page_id_);

    if (header_page->root_page_id_ == INVALID_PAGE_ID) {
      LOG_DEBUG("Create new tree failed");
      return false;
    }

    auto root_page_guard = basic_page_guard.UpgradeWrite();
    LeafPage *root_page = root_page_guard.template AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->array_[0] = {key, value};
    root_page->IncreaseSize(1);
    return true;
  }

  auto header_page = ctx.header_page_.value().template AsMut<BPlusTreeHeaderPage>();
  auto root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  ctx.write_set_.emplace_back(std::move(root_page_guard));

  auto current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    const InternalPage *internal_page = ctx.write_set_.back().template AsMut<InternalPage>();
    auto pos = 0;
    while (pos < internal_page->GetSize() && internal_page->KeyAt(pos + 1) < key) { pos++; }
    // Obtain correct number
    pos = pos != internal_page->GetSize() && std::abs(key - internal_page->KeyAt(pos + 1)) <= 1E-10 ? pos + 1 : pos;

    // Traverse to the leaf child
    ctx.write_set_.emplace_back(std::move(bpm_->FetchPageWrite(internal_page->ValueAt(pos))));
    current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  }

  auto leaf_page = reinterpret_cast<LeafPage *>(current_page);
  auto target_position = leaf_page->GetSize();
  auto end_position = leaf_page->GetSize();
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (std::abs(key - leaf_page->array_[i].first) <= 1E-10) {
      LOG_DEBUG("Need unique key value");
      return false;
    }

    if (key < leaf_page->array_[i].first) {
      target_position = i;
      break;
    }
  }

  while (end_position > target_position) {
    leaf_page->array_[end_position] = leaf_page->array_[end_position - 1];
    end_position--;
  }

  leaf_page->array_[target_position] = {key, value};
  leaf_page->IncreaseSize(1);

  // Recursive deal with the case with current_size > max_size
  current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  while ((current_page->IsLeafPage() && current_page->GetSize() >= leaf_max_size_) || \
      (!current_page->IsLeafPage() && current_page->GetSize() >= internal_max_size_)) {
    std::pair<BPlusTreeKeyType, page_id_t> upgrade_variable({-1, INVALID_PAGE_ID});
    // split the current to page to two child page
    if (current_page->IsLeafPage()) {
      // new page for [leaf_max_size/2, .)
      page_id_t new_leaf_page_id = INVALID_PAGE_ID;
      auto new_leaf_basic_page_guard = bpm_->NewPageGuarded(&new_leaf_page_id);
      auto new_leaf_page_guard = new_leaf_basic_page_guard.UpgradeWrite();

      if (new_leaf_page_id == INVALID_PAGE_ID) {
        LOG_DEBUG("Allocate new leaf page failed");
        return false;
      }

      LeafPage *new_leaf_page = new_leaf_page_guard.template AsMut<LeafPage>();
      new_leaf_page->Init(leaf_max_size_);
      memcpy(reinterpret_cast<char *>(new_leaf_page->array_),
             reinterpret_cast<char *>(&reinterpret_cast<LeafPage *>(current_page)->array_[leaf_max_size_ / 2]),
             sizeof(MappingType) * (leaf_max_size_ / 2 + 1));
      new_leaf_page->SetSize(leaf_max_size_ / 2 + 1);
      new_leaf_page->SetNextPageId(reinterpret_cast<LeafPage *>(current_page)->GetNextPageId());

      // current page for [0,leaf_max_size/2)
      LeafPage *old_leaf_page = reinterpret_cast<LeafPage *>(current_page);
      old_leaf_page->SetSize(leaf_max_size_ / 2);
      old_leaf_page->SetNextPageId(new_leaf_page_id);

      // Set upgrade variable
      upgrade_variable = {new_leaf_page->array_[0].first, new_leaf_page_id};
    } else {
      // new page for [internal_max_size/2, .)
      page_id_t new_internal_page_id = INVALID_PAGE_ID;
      auto new_internal_basic_page_guard = bpm_->NewPageGuarded(&new_internal_page_id);
      auto new_internal_page_guard = new_internal_basic_page_guard.UpgradeWrite();

      if (new_internal_page_id == INVALID_PAGE_ID) {
        LOG_DEBUG("Allocate new leaf page failed");
        return false;
      }

      InternalPage *new_internal_page = new_internal_page_guard.template AsMut<InternalPage>();
      new_internal_page->Init(internal_max_size_);
      memcpy(reinterpret_cast<char *>(&new_internal_page->array_[1]),
             reinterpret_cast<char *>(&reinterpret_cast<InternalPage *>(current_page)->array_[internal_max_size_ / 2
                 + 2]),
             sizeof(std::pair<BPlusTreeKeyType, page_id_t>) * (internal_max_size_ / 2));
      new_internal_page->SetSize(internal_max_size_ / 2);
      new_internal_page->array_[0].second =
          reinterpret_cast<InternalPage *>(current_page)->array_[internal_max_size_ / 2 + 1].second;

      // current page for [0,internal_max_size/2)
      InternalPage *old_internal_page = reinterpret_cast<InternalPage *>(current_page);
      old_internal_page->SetSize(internal_max_size_ / 2);

      // Set upgrade variable
      upgrade_variable = {old_internal_page->array_[internal_max_size_ / 2 + 1].first, new_internal_page_id};
    }

    // Update ctx
    ctx.write_set_.pop_back();
    // root page need to be updated
    if (ctx.write_set_.empty()) {
      auto new_root_page_id = INVALID_PAGE_ID;
      auto new_root_page_basic_guard = bpm_->NewPageGuarded(&new_root_page_id);

      if (new_root_page_id == INVALID_PAGE_ID) {
        LOG_DEBUG("Allocate new root page failed.");
        return false;
      }

      auto new_root_page_guard = new_root_page_basic_guard.UpgradeWrite();
      InternalPage *new_root_page = new_root_page_guard.template AsMut<InternalPage>();
      new_root_page->Init(internal_max_size_);
      new_root_page->array_[1] = upgrade_variable;
      new_root_page->array_[0].second = header_page->root_page_id_;
      new_root_page->SetSize(1);
      header_page->root_page_id_ = new_root_page_id;
      break;
    }

    // Upgrade data in parent node
    current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
    InternalPage *parent_page = reinterpret_cast<InternalPage *>(current_page);
    target_position = parent_page->GetSize() + 1;
    end_position = parent_page->GetSize() + 1;
    for (int i = 1; i < parent_page->GetSize() + 1; ++i) {
      if (key < parent_page->array_[i].first) {
        target_position = i;
        break;
      }
    }

    while (end_position > target_position) {
      parent_page->array_[end_position] = parent_page->array_[end_position - 1];
      end_position--;
    }

    parent_page->array_[target_position] = upgrade_variable;
    parent_page->IncreaseSize(1);
  }

  // For debug
//  auto root_page_basic_guard = bpm_->FetchPageBasic(header_page->root_page_id_);
//  this->PrintTree(root_page_basic_guard.PageId(), root_page_basic_guard.template As<BPlusTreePage>());
//  std::cout <<  "-------------------------------------------\n";
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::Delete(const BPlusTreeKeyType &key) -> bool {
  // If empty, return false directly
  if (IsEmpty()) {
    return false;
  }

  Context ctx;
  std::vector<int> trace;
  auto twice_delete_key = false;

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  auto root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto root_page = root_page_guard.template AsMut<BPlusTreePage>();
  auto current_page = root_page;
  ctx.write_set_.emplace_back(std::move(root_page_guard));

  // Delete target key-value pair
  while (!current_page->IsLeafPage()) {
    InternalPage *internal_page = ctx.write_set_.back().template AsMut<InternalPage>();
    auto pos = 0;
    while (pos < internal_page->GetSize() && internal_page->KeyAt(pos + 1) < key) { pos++; }
    // Obtain correct number
    pos = pos != internal_page->GetSize() && std::abs(key - internal_page->KeyAt(pos + 1)) <= 1E-10 ? pos + 1 : pos;
    trace.emplace_back(pos);

    // Trace the position need to be deleted
    if (std::abs(key - internal_page->KeyAt(pos)) <= 1E-10) {
      internal_page->array_[pos].first = TEMP_SLOT_VACANCY;
      twice_delete_key = true;
    }

    // Traverse to the leaf child
    ctx.write_set_.emplace_back(std::move(bpm_->FetchPageWrite(internal_page->ValueAt(pos))));
    current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  }

  auto leaf_page = reinterpret_cast<LeafPage *>(current_page);
  auto target_position = leaf_page->GetSize();
  auto end_position = leaf_page->GetSize();
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (std::abs(key - leaf_page->array_[i].first) <= 1E-10) {
      target_position = i;
      break;
    }
  }

  // Search input key failed
  if (target_position == end_position) {
    return false;
  }

  // Update leaf page data
  while (end_position > target_position + 1) {
    leaf_page->array_[target_position] = leaf_page->array_[target_position + 1];
    target_position++;
  }
  leaf_page->IncreaseSize(-1);


  // Deal with the case that leaf page least than min size
  auto replace_key = leaf_page->GetSize() != 0 ? leaf_page->array_[0].first : -1;
  ctx.write_set_.pop_back();
  auto update_success = false;
  while (!ctx.write_set_.empty() && current_page->GetSize() < current_page->GetMinSize()) {
    update_success = false;
    InternalPage *parent_page = ctx.write_set_.back().template AsMut<InternalPage>();

    // Try to borrow from left sibling
    if (!update_success && trace.back() != 0) {
      auto left_sibling_page_id = parent_page->array_[trace.back() - 1].second;
      auto left_sibling_page_guard = bpm_->FetchPageWrite(left_sibling_page_id);
      BPlusTreePage *left_sibling_page = left_sibling_page_guard.template AsMut<BPlusTreePage>();

      if (left_sibling_page->GetSize() > left_sibling_page->GetMinSize()) {
        if (left_sibling_page->IsLeafPage()) {
          LeafPage *current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
          LeafPage *left_sibling_leaf_page = reinterpret_cast<LeafPage *>(left_sibling_page);
          MappingType replacer_element = left_sibling_leaf_page->array_[left_sibling_leaf_page->GetSize() - 1];
          left_sibling_leaf_page->IncreaseSize(-1);

          end_position = current_leaf_page->GetSize();
          while (end_position > 0) {
            current_leaf_page->array_[end_position] = current_leaf_page->array_[end_position - 1];
            end_position--;
          }

          current_leaf_page->array_[0] = replacer_element;
          current_leaf_page->IncreaseSize(1);
          replace_key = replacer_element.first;

          if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
            twice_delete_key = false;
          }
          parent_page->array_[trace.back()].first = replacer_element.first;
        } else {
          InternalPage *current_internal_page = reinterpret_cast<InternalPage *>(current_page);
          InternalPage *left_sibling_internal_page = reinterpret_cast<InternalPage *>(left_sibling_page);
          auto replacer_element = left_sibling_internal_page->array_[left_sibling_internal_page->GetSize()];
          left_sibling_internal_page->IncreaseSize(-1);

          end_position = current_internal_page->GetSize() + 1;
          while (end_position > 1) {
            current_internal_page->array_[end_position] = current_internal_page->array_[end_position - 1];
            end_position--;
          }

          if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
            current_internal_page->array_[1] = {replace_key, current_internal_page->array_[0].second};
            twice_delete_key = false;
          } else {
            current_internal_page->array_[1] =
                {parent_page->array_[trace.back()].first, current_internal_page->array_[0].second};
          }
          current_internal_page->array_[0].second = replacer_element.second;
          current_internal_page->IncreaseSize(1);

          parent_page->array_[trace.back()].first = replacer_element.first;
        }

        update_success = true;
      }
    }

    // Try to borrow from right sibling
    if (!update_success && trace.back() != parent_page->GetSize()) {
      auto right_sibling_page_id = parent_page->array_[trace.back() + 1].second;
      auto right_sibling_page_guard = bpm_->FetchPageWrite(right_sibling_page_id);
      BPlusTreePage *right_sibling_page = right_sibling_page_guard.template AsMut<BPlusTreePage>();

      if (right_sibling_page->GetSize() > right_sibling_page->GetMinSize()) {
        if (right_sibling_page->IsLeafPage()) {
          LeafPage *current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
          LeafPage *right_sibling_leaf_page = reinterpret_cast<LeafPage *>(right_sibling_page);
          MappingType replacer_element = right_sibling_leaf_page->array_[0];
          for (int i = 0; i < right_sibling_leaf_page->GetSize() - 1; ++i) {
            right_sibling_leaf_page->array_[i] = right_sibling_leaf_page->array_[i + 1];
          }
          right_sibling_leaf_page->IncreaseSize(-1);

          current_leaf_page->array_[current_leaf_page->GetSize()] = replacer_element;
          current_leaf_page->IncreaseSize(1);
          replace_key = current_leaf_page->array_[0].first;

          parent_page->array_[trace.back() + 1].first = right_sibling_leaf_page->array_[1].first;
        } else {
          InternalPage *current_internal_page = reinterpret_cast<InternalPage *>(current_page);
          InternalPage *right_sibling_internal_page = reinterpret_cast<InternalPage *>(right_sibling_page);
          auto replacer_element = right_sibling_internal_page->array_[1];
          for (int i = 1; i < right_sibling_internal_page->GetSize(); ++i) {
            right_sibling_internal_page->array_[i] = right_sibling_internal_page->array_[i + 1];
          }
          right_sibling_internal_page->IncreaseSize(-1);

          if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
            current_internal_page->array_[current_internal_page->GetSize() + 1] =
                {replace_key, right_sibling_internal_page->array_[0].second};
            twice_delete_key = false;
          } else {
            current_internal_page->array_[current_internal_page->GetSize() + 1] =
                {parent_page->array_[trace.back()].first, right_sibling_internal_page->array_[0].second};
          }
          current_internal_page->IncreaseSize(1);

          right_sibling_internal_page->array_[0].second = replacer_element.second;
          parent_page->array_[trace.back() + 1].first = replacer_element.first;
        }

        update_success = true;
      }
    }

    // Compact with the left sibling
    if (!update_success && trace.back() != 0) {
      auto left_sibling_page_id = parent_page->array_[trace.back() - 1].second;
      auto left_sibling_page_guard = bpm_->FetchPageWrite(left_sibling_page_id);
      BPlusTreePage *left_sibling_page = left_sibling_page_guard.template AsMut<BPlusTreePage>();

      if (left_sibling_page->IsLeafPage()) {
        LeafPage *current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
        LeafPage *left_sibling_leaf_page = reinterpret_cast<LeafPage *>(left_sibling_page);
        memcpy(reinterpret_cast<char *>(&left_sibling_leaf_page->array_[left_sibling_leaf_page->GetSize()]),
               reinterpret_cast<char *>(current_leaf_page->array_),
               sizeof(MappingType) * current_leaf_page->GetSize());
        left_sibling_page->IncreaseSize(current_leaf_page->GetSize());
        left_sibling_leaf_page->SetNextPageId(current_leaf_page->GetNextPageId());

        bpm_->DeletePage(parent_page->array_[trace.back()].second);
        if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
          twice_delete_key = false;
        }

        auto slot = trace.back();
        while (slot < parent_page->GetSize()) {
          parent_page->array_[slot] = parent_page->array_[slot + 1];
          slot++;
        }

        parent_page->IncreaseSize(-1);
      } else {
        InternalPage *current_internal_page = reinterpret_cast<InternalPage *>(current_page);
        InternalPage *left_sibling_internal_page = reinterpret_cast<InternalPage *>(left_sibling_page);
        memcpy(reinterpret_cast<char *>(&left_sibling_internal_page->array_[left_sibling_internal_page->GetSize() + 2]),
               reinterpret_cast<char *>(&current_internal_page->array_[1]),
               sizeof(std::pair<BPlusTreeKeyType, page_id_t>) * current_internal_page->GetSize());
        left_sibling_internal_page->array_[left_sibling_internal_page->GetSize() + 1] =
            {parent_page->array_[trace.back()].first, current_internal_page->array_[0].second};
        left_sibling_internal_page->IncreaseSize(current_internal_page->GetSize() + 1);

        bpm_->DeletePage(parent_page->array_[trace.back()].second);
        if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
          twice_delete_key = false;
        }

        auto slot = trace.back();
        while (slot < parent_page->GetSize()) {
          parent_page->array_[slot] = parent_page->array_[slot + 1];
          slot++;
        }

        parent_page->IncreaseSize(-1);
      }

      update_success = true;
    }

    // Compact with right sibling
    if (!update_success && trace.back() != parent_page->GetSize()) {
      auto right_sibling_page_id = parent_page->array_[trace.back() + 1].second;
      auto right_sibling_page_guard = bpm_->FetchPageWrite(right_sibling_page_id);
      BPlusTreePage *right_sibling_page = right_sibling_page_guard.template AsMut<BPlusTreePage>();

      if (right_sibling_page->IsLeafPage()) {
        LeafPage *current_leaf_page = reinterpret_cast<LeafPage *>(current_page);
        LeafPage *right_sibling_leaf_page = reinterpret_cast<LeafPage *>(right_sibling_page);
        memcpy(reinterpret_cast<char *>(&current_leaf_page->array_[current_leaf_page->GetSize()]),
               reinterpret_cast<char *>(right_sibling_leaf_page->array_),
               sizeof(MappingType) * right_sibling_leaf_page->GetSize());
        current_leaf_page->IncreaseSize(right_sibling_leaf_page->GetSize());
        current_leaf_page->SetNextPageId(right_sibling_leaf_page->GetNextPageId());

        bpm_->DeletePage(parent_page->array_[trace.back() + 1].second);
        if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
          parent_page->array_[trace.back()].first = replace_key;
          twice_delete_key = false;
        }

        auto slot = trace.back() + 1;
        while (slot < parent_page->GetSize()) {
          parent_page->array_[slot] = parent_page->array_[slot + 1];
          slot++;
        }

        parent_page->IncreaseSize(-1);
      } else {
        InternalPage *current_internal_page = reinterpret_cast<InternalPage *>(current_page);
        InternalPage *right_sibling_internal_page = reinterpret_cast<InternalPage *>(right_sibling_page);
        memcpy(reinterpret_cast<char *>(&current_internal_page->array_[current_internal_page->GetSize() + 2]),
               reinterpret_cast<char *>(&right_sibling_internal_page->array_[1]),
               sizeof(std::pair<BPlusTreeKeyType, page_id_t>) * right_sibling_internal_page->GetSize());
        current_internal_page->array_[current_internal_page->GetSize() + 1] =
            {parent_page->array_[trace.back() + 1].first, right_sibling_internal_page->array_[0].second};
        current_internal_page->IncreaseSize(right_sibling_internal_page->GetSize() + 1);
        bpm_->DeletePage(parent_page->array_[trace.back() + 1].second);

        if (std::abs(parent_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
          parent_page->array_[trace.back()].first = replace_key;
          twice_delete_key = false;
        }

        auto slot = trace.back() + 1;
        while (slot < parent_page->GetSize()) {
          parent_page->array_[slot] = parent_page->array_[slot + 1];
          slot++;
        }

        parent_page->IncreaseSize(-1);
      }
    }

    // Update current_page, ctx and trace
    current_page = parent_page;
    if (current_page == root_page) {
      break;
    }
    ctx.write_set_.pop_back();
    trace.pop_back();
  }

  // If the target index is appeared in the index
  while (twice_delete_key && !trace.empty() && !ctx.write_set_.empty()) {
    auto current_internal_page = ctx.write_set_.back().template AsMut<InternalPage>();
    if (std::abs(current_internal_page->array_[trace.back()].first - TEMP_SLOT_VACANCY) < 1E-10) {
      current_internal_page->array_[trace.back()].first = replace_key;
    }

    trace.pop_back();
    ctx.write_set_.pop_back();
  }

  // For the case that need to decrease the height of B+ tree
  if (root_page->GetSize() == 0 && !root_page->IsLeafPage()) {
    InternalPage *root_internal_page = reinterpret_cast<InternalPage *>(root_page);
    auto old_root_page_id = header_page->root_page_id_;
    if (update_success) {
      header_page->root_page_id_ = root_internal_page->array_[trace[0] - 1].second;
    } else {
      header_page->root_page_id_ = root_internal_page->array_[trace[0]].second;
    }
    bpm_->DeletePage(old_root_page_id);
  }

  // For debug
//  auto root_page_basic_guard = bpm_->FetchPageBasic(header_page->root_page_id_);
//  this->PrintTree(root_page_basic_guard.PageId(), root_page_basic_guard.template As<BPlusTreePage>());
//  std::cout << "-------------------------------------------\n";
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::UpDate(const BPlusTreeKeyType &key, const BPlusTreeValueType &value) -> bool {
  if (IsEmpty()) {
    return false;
  }

  Context ctx;

  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();

  auto root_page_guard = bpm_->FetchPageWrite(header_page->root_page_id_);
  auto root_page = root_page_guard.template AsMut<BPlusTreePage>();
  auto current_page = root_page;
  ctx.write_set_.emplace_back(std::move(root_page_guard));

  while (!current_page->IsLeafPage()) {
    InternalPage *internal_page = ctx.write_set_.back().template AsMut<InternalPage>();
    auto pos = 0;
    while (pos < internal_page->GetSize() && internal_page->KeyAt(pos + 1) < key) { pos++; }
    // Obtain correct number
    pos = pos != internal_page->GetSize() && std::abs(key - internal_page->KeyAt(pos + 1)) <= 1E-10 ? pos + 1 : pos;

    // Traverse to the leaf child
    ctx.write_set_.emplace_back(std::move(bpm_->FetchPageWrite(internal_page->ValueAt(pos))));
    current_page = ctx.write_set_.back().template AsMut<BPlusTreePage>();
  }

  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(current_page);
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (std::abs(key - leaf_page->array_[i].first) <= 1E-10) {
      leaf_page->array_[i].second = value;
      return true;
    }
  }

  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::RangeRead(const BPlusTreeKeyType &lkey, const BPlusTreeKeyType &rkey, std::vector<BPlusTreeValueType> *result) -> bool {
  if (lkey >= rkey || IsEmpty()) {
    return false;
  }

  Context ctx;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.template As<BPlusTreeHeaderPage>();

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  ctx.read_set_.emplace_back(std::move(root_page_guard));
  auto current_page = ctx.read_set_.back().template As<BPlusTreePage>();

  // left key search
  while (!current_page->IsLeafPage()) {
    const InternalPage *internal_page = reinterpret_cast<const InternalPage*>(current_page);
    auto pos = 0;
    while (pos < internal_page->GetSize() && internal_page->KeyAt(pos + 1) < lkey) { pos++; }
    // Obtain correct number
    pos = pos != internal_page->GetSize() && std::abs(lkey - internal_page->KeyAt(pos + 1)) <= 1E-10 ? pos + 1 : pos;

    // Traverse to the leaf child
    ctx.read_set_.emplace_back(std::move(bpm_->FetchPageRead(internal_page->ValueAt(pos))));
    current_page = ctx.read_set_.back().template As<BPlusTreePage>();
  }

  const auto left_leaf_page = reinterpret_cast<const LeafPage *>(current_page);
  auto left_start_page_id =  ctx.read_set_.back().PageId();
  auto left_start_page_slot = left_leaf_page->GetSize();
  for (int i = 0; i < left_leaf_page->GetSize(); ++i) {
    if (lkey < left_leaf_page->array_[i].first || std::abs(lkey - left_leaf_page->array_[i].first) <= 1E-10) {
      left_start_page_slot = i;
      break;
    }
  }

  // right key search
  current_page = ctx.read_set_.begin()->template As<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    const InternalPage *internal_page = reinterpret_cast<const InternalPage*>(current_page);
    auto pos = 0;
    while (pos < internal_page->GetSize() && internal_page->KeyAt(pos + 1) < rkey) { pos++; }
    // Obtain correct number
    pos = pos != internal_page->GetSize() && std::abs(rkey - internal_page->KeyAt(pos + 1)) <= 1E-10 ? pos + 1 : pos;

    // Traverse to the leaf child
    ctx.read_set_.emplace_back(std::move(bpm_->FetchPageRead(internal_page->ValueAt(pos))));
    current_page = ctx.read_set_.back().template As<BPlusTreePage>();
  }

  const auto right_leaf_page = reinterpret_cast<const LeafPage *>(current_page);
  auto right_back_page_slot = right_leaf_page->GetSize();
  auto right_back_page_id = ctx.read_set_.back().PageId();
  auto right_end_page_id = right_leaf_page->GetNextPageId();
  for (int i = 0; i < right_leaf_page->GetSize(); ++i) {
    if (rkey < right_leaf_page->array_[i].first) {
      right_back_page_slot = std::abs(rkey - right_leaf_page->array_[i].first) <= 1E-10 ? i + 1 : i;
      break;
    }
  }

  auto current_search_page_id = left_start_page_id;
  while (current_search_page_id != right_end_page_id) {
    auto current_search_page_guard = bpm_->FetchPageRead(current_search_page_id);
    ctx.read_set_.emplace_back(std::move(current_search_page_guard));
    const LeafPage *current_search_page = ctx.read_set_.back().template As<LeafPage>();
    auto current_search_page_start_slot = current_search_page_id == left_start_page_id ? left_start_page_slot : 0;
    auto current_search_page_end_slot = current_search_page_id == right_back_page_id ? right_back_page_slot : current_search_page->GetSize();

    for (int i = current_search_page_start_slot; i < current_search_page_end_slot; ++i) {
      result->emplace_back(current_search_page->array_[i].second);
    }

    current_search_page_id = current_search_page->GetNextPageId();
  }
   // for debug
//  auto root_page_basic_guard = bpm_->FetchPageBasic(header_page->root_page_id_);
//  this->PrintTree(root_page_basic_guard.PageId(), root_page_basic_guard.template As<BPlusTreePage>());
//  std::cout << "-------------------------------------------\n";
  return !result->empty();
}


INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_TYPE::SubTreeToString(page_id_t page_id, const BPlusTreePage *page, std::stringstream &ss) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    ss << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    ss << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      ss << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        ss << ", ";
      }
    }
    ss << '\n';
    ss << '\n';

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    ss << "Internal Page: " << page_id << '\n';

    // Print the contents of the internal page.
    ss << "Contents: ";
    for (int i = 0; i < internal->GetSize() + 1; i++) {
      ss << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if (i < internal->GetSize()) {
        ss << ", ";
      }
    }
    ss << '\n';
    ss << '\n';
    for (int i = 0; i < internal->GetSize() + 1; i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      SubTreeToString(guard.PageId(), guard.template As<BPlusTreePage>(), ss);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_TYPE::ToString() -> std::string {
  if (IsEmpty()) {
    return "";
  }
  std::stringstream ss;
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page_guard = bpm_->FetchPageRead(header_page_guard.template As<BPlusTreeHeaderPage>()->root_page_id_);
  SubTreeToString(root_page_guard.PageId(), root_page_guard.template As<BPlusTreePage>(), ss);

  return ss.str();
}

template class BPlusTree<float, RID>;

template class BPlusTree<double, RID>;
} // namespace distribution_lsh