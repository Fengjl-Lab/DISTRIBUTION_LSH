//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/3.
// src/storage/page/page_guard.cpp
//
//===-----------------------------------------------------

#include <storage/page/page_guard.h>
#include <buffer/buffer_pool_manager.h>

namespace distribution_lsh {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(std::move(that.page_)), is_dirty_(that.is_dirty_) {
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (this->page_ != nullptr && this->page_->GetPageId() != INVALID_PAGE_ID) {
    this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  }

  this->page_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;

  return *this;
}

BasicPageGuard::~BasicPageGuard(){
  Drop();
  this->bpm_ = nullptr;
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  return {this->bpm_, std::move(this->page_)};
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  return {this->bpm_, std::move(this->page_)};
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr && this->guard_.PageId() != INVALID_PAGE_ID) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
    this->guard_.page_->RUnlatch();
    guard_.page_ = nullptr;
  }
}

ReadPageGuard::~ReadPageGuard() {
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr && this->guard_.PageId() != INVALID_PAGE_ID) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
    this->guard_.page_->WUnlatch();
    guard_.page_ = nullptr;
  }
}

WritePageGuard::~WritePageGuard() {
  Drop();
}  // NOLINT

}  // namespace distribution_lsh