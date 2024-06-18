//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/24.
// src/storage/index/relation_manager.cpp
//
//===-----------------------------------------------------

#include <storage/index/relation_manager.h>

namespace distribution_lsh {

RELATION_TEMPLATE
RELATION_MANAGER_TYPE::RelationManager(std::string manager_name,
                                       std::string directory_name,
                                       std::shared_ptr<BufferPoolManager> bpm,
                                       RelationFileType type,
                                       distribution_lsh::file_id_t file_id,
                                       page_id_t header_page_id,
                                       int data_max_size) :
    manager_name_(std::move(manager_name)),
    directory_name_(std::move(directory_name)),
    bpm_(std::move(bpm)),
    type_(type),
    file_id_(file_id),
    header_page_id_(header_page_id),
    data_page_max_size_(data_max_size) {
  // Judge if the file empty
  if (IsEmpty()) {
    auto header_page_basic_guard = bpm_->NewPageGuarded(&header_page_id_);
    if (header_page_id_ == INVALID_PAGE_ID || header_page_id_ != HEADER_PAGE_ID) {
      throw Exception("Allocate header page failed.");
    }
    auto header_page = header_page_basic_guard.template AsMut<RelationHeaderPage>();
    header_page->SetRelationFileType(type_);
  } else {
    auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_page_guard.template As<RelationHeaderPage>();
    file_id_ = header_page->GetFileIdentification();
    type_ = header_page->GetRelationFileType();
  }
}

RELATION_TEMPLATE
auto RELATION_MANAGER_TYPE::IsEmpty() -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.template As<RelationHeaderPage>();
  return header_page->IsEmpty();
}

RELATION_TEMPLATE
auto RELATION_MANAGER_TYPE::Insert(ValueType value, int *index) -> bool {
  {
    std::scoped_lock empty_latch(lock_);

    if (IsEmpty()) {
      auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
      auto header_page = header_page_guard.template AsMut<RelationHeaderPage>();
      auto data_page_guard = bpm_->NewPageGuarded(&header_page->data_page_start_id_);
      if (header_page->data_page_start_id_ == INVALID_PAGE_ID) {
        LOG_DEBUG("Allocate data page failed.");
        return false;
      }
      DataPage* data_page = data_page_guard.template AsMut<DataPage>();
      data_page->Init(data_page_max_size_);
    }
  }

  // Locate data page
  RelationContext ctx;
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.template AsMut<RelationHeaderPage>();
  auto data_page_guard = bpm_->FetchPageWrite(header_page->data_page_start_id_);
  DataPage* data_page = data_page_guard.template AsMut<DataPage>();
  ctx.write_set_.emplace_back(std::move(data_page_guard));

  while (data_page->GetNextPageId() != INVALID_PAGE_ID) {
    data_page_guard = bpm_->FetchPageWrite(data_page->GetNextPageId());
    ctx.write_set_.pop_front();
    ctx.write_set_.emplace_back(std::move(data_page_guard));
    data_page = ctx.write_set_.back().template AsMut<DataPage>();
  }

  if (data_page->GetSize() == data_page->GetMaxSize()) {
    auto data_page_basic_guard = bpm_->NewPageGuarded(&data_page->next_page_id_);
    if (data_page->next_page_id_ == INVALID_PAGE_ID) {
      LOG_DEBUG("Allocate data page failed.");
      return false;
    }
    data_page_guard = data_page_basic_guard.UpgradeWrite();
    ctx.write_set_.emplace_back(std::move(data_page_guard));
    data_page = ctx.write_set_.back().template AsMut<DataPage>();
    data_page->Init(data_page_max_size_);
  }

  data_page->Insert(value, index);
  return true;
}

RELATION_TEMPLATE
auto RELATION_MANAGER_TYPE::Delete(page_id_t data_page_id, int slot) -> bool {
  if (data_page_id == header_page_id_) {
    LOG_DEBUG("Cannot delete in header page");
    return false;
  }

  RelationContext ctx;
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.template AsMut<RelationHeaderPage>();
  auto data_page_guard = bpm_->FetchPageWrite(data_page_id);
  DataPage *data_page = data_page_guard.template AsMut<DataPage>();
  data_page->Delete(slot);

  // Update the link
  if (data_page->GetSize() == 0) {
    // Locate before page
    auto before_page_guard = bpm_->FetchPageWrite(header_page->data_page_start_id_);
    DataPage* before_page = before_page_guard.template AsMut<DataPage>();
    ctx.write_set_.emplace_back(std::move(before_page_guard));
    while (before_page->GetNextPageId() != data_page_id) {
      if (before_page->GetNextPageId() == INVALID_PAGE_ID) {
        LOG_DEBUG("Page id out of range.");
        return false;
      }

      before_page_guard = bpm_->FetchPageWrite(before_page->GetNextPageId());
      ctx.write_set_.pop_front();
      ctx.write_set_.emplace_back(std::move(before_page_guard));
      before_page = ctx.write_set_.back().template AsMut<DataPage>();
    }

    before_page->next_page_id_ = data_page->next_page_id_;
    data_page_guard.Drop();
    bpm_->DeletePage(data_page_id);
  }

  return true;
}

RELATION_TEMPLATE
auto RELATION_MANAGER_TYPE::Get(page_id_t data_page_id, int slot) -> ValueType {
  if (data_page_id == header_page_id_) {
    LOG_DEBUG("Cannot Get in header page");
    return {.next_null_slot_ = NULL_SLOT_END};
  }

  auto data_page_guard = bpm_->FetchPageRead(data_page_id);
  const DataPage *data_page = data_page_guard.template As<DataPage>();
  return data_page->Get(slot);
}

RELATION_TEMPLATE
auto RELATION_MANAGER_TYPE::Get(int index, distribution_lsh::page_id_t *data_page_id, int *slot) -> ValueType {
  if (IsEmpty()) {
    throw Exception("The file is empty");
  }

  RelationContext ctx;
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.template As<RelationHeaderPage>();
  auto data_page_guard = bpm_->FetchPageRead(header_page->data_page_start_id_);
  const DataPage *data_page = data_page_guard.template As<DataPage>();
  ctx.read_set_.emplace_back(std::move(data_page_guard));
  auto current_size = 0;

  while (index >= current_size) {
    if (index - current_size >= data_page->GetMaxSize()) {
      if (data_page->GetNextPageId() == INVALID_PAGE_ID) {
        throw Exception("Index out of range", false);
      }

      current_size += data_page->GetMaxSize();
      data_page_guard = bpm_->FetchPageRead(data_page->GetNextPageId());
      ctx.read_set_.pop_front();
      ctx.read_set_.emplace_back(std::move(data_page_guard));
      data_page = ctx.read_set_.back().template As<DataPage>();
    } else {
      *data_page_id = ctx.read_set_.back().PageId();
      *slot = index - current_size;
      break;
    }
  }

  return data_page->Get(*slot);
}

template class RelationManager<TrainingSetToTestingSetUnion>;
template class RelationManager<RandomLineFileToBPlusTreeFileUnion>;

} // namespace distribution_lsh