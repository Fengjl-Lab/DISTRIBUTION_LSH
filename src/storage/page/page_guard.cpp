//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/3.
// src/storage/page/page_guard.cpp
//
//===-----------------------------------------------------

#include <storage/page/page_guard.h>
#include <buffer/buffer_pool_manager.h>

namespace qalsh {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (this->page_->GetPageId() != INVALID_PAGE_ID) {
    this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  }

  this->page_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.page_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard(){
  Drop();
  this->bpm_ = nullptr;
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  this->page_->RLatch();
  return this->bpm_->FetchPageRead(this->page_->GetPageId());
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  this->page_->WLatch();
  return this->bpm_->FetchPageWrite(this->page_->GetPageId());
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  this->guard_.page_->RUnlatch();
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
  this->guard_.page_->RUnlatch();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)){
  that.guard_.Drop();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  this->guard_.page_->WUnlatch();
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
  this->guard_.page_->WUnlatch();
}  // NOLINT

}  // namespace qalsh