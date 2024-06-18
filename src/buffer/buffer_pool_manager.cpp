//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/4.
// src/buffer/buffer_pool_manager.cpp
//
//===-----------------------------------------------------

#include <buffer/buffer_pool_manager.h>

#include <common/exception.h>
#include <common/macro.h>
#include <common/logger.h>
#include <storage/page/page_guard.h>

namespace distribution_lsh {
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     std::shared_ptr<distribution_lsh::DiskManager> disk_manager,
                                     size_t replacer_k,
                                     distribution_lsh::LogManager *log_manager,
                                     page_id_t next_page_id)
    : pool_size_(pool_size), pages_(new Page[pool_size_]), disk_scheduler_(std::make_unique<DiskScheduler>(std::move(disk_manager))), log_manager_(log_manager), next_page_id_(next_page_id){
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  FlushAllPages();
  disk_scheduler_->request_queue_.Put(std::nullopt);
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> std::shared_ptr<Page> {
  std::unique_lock<std::mutex> page(latch_);

  // search in the free frame
  frame_id_t target_frame = !free_list_.empty() ? free_list_.front() : -1;
  if (target_frame == -1 && !replacer_->Evict(&target_frame)) {
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }

  // if the frame is searched in free list
  if (!free_list_.empty()) {
    free_list_.pop_front();
  } else {
    Page *replaced_page = &pages_[target_frame];
    if (replaced_page->IsDirty()) {
      // Write back to disk
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      std::optional<DiskRequest> disk_request
          ({true, reinterpret_cast<char *>(replaced_page->data_), replaced_page->page_id_, std::move(promise)});
      disk_scheduler_->request_queue_.Put(std::move(disk_request));
      DISTRIBUTION_LSH_ENSURE(future.get(), "Write Back Failure.")
    }

    page_table_.erase(replaced_page->page_id_);
  }

  // Set initial data
  pages_[target_frame].page_id_ = AllocatePage();
  *page_id = pages_[target_frame].page_id_;
  pages_[target_frame].ResetMemory();
  pages_[target_frame].pin_count_ = 1;
  pages_[target_frame].is_dirty_ = false;
  page_table_[pages_[target_frame].page_id_] = target_frame;

  // Update replacers
  replacer_->RecordAccess(target_frame);
  replacer_->SetEvictable(target_frame, false);

  return {pages_, pages_.get() + target_frame};
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> std::shared_ptr<Page> {
  std::unique_lock<std::mutex> page(latch_);

  // Page in the buffer
  if (page_table_.find(page_id) != page_table_.end()) {
    pages_[page_table_[page_id]].pin_count_ += 1;
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    return {pages_, pages_.get() + page_table_[page_id]};
  }

  // Page need to replace
  frame_id_t target_frame = !free_list_.empty() ? free_list_.front() : -1;
  if (target_frame == -1 && !replacer_->Evict(&target_frame)) {
    return nullptr;
  }

  // Deal the target page
  if (!free_list_.empty()) {
    free_list_.pop_front();
  } else {
    Page *replaced_page = &pages_[target_frame];
    if (replaced_page->IsDirty()) {
      // Write back to disk
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      std::optional<DiskRequest> disk_request
          ({true, reinterpret_cast<char *>(replaced_page->data_), replaced_page->page_id_, std::move(promise)});
      disk_scheduler_->request_queue_.Put(std::move(disk_request));
      DISTRIBUTION_LSH_ENSURE(future.get(), "Write Back Failure.")
    }

    page_table_.erase(replaced_page->page_id_);
  }

  // Read from disk
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  std::optional<DiskRequest>
      disk_request({false, reinterpret_cast<char *>(pages_[target_frame].data_), page_id, std::move(promise)});
  disk_scheduler_->request_queue_.Put(std::move(disk_request));
  // Wait for read in process
  DISTRIBUTION_LSH_ENSURE(future.get(), "Read In Failure.")

  // Set page information
  pages_[target_frame].page_id_ = page_id;
  pages_[target_frame].is_dirty_ = false;
  pages_[target_frame].pin_count_ = 1;
  page_table_[page_id] = target_frame;

  // Update replacers
  replacer_->RecordAccess(target_frame);
  replacer_->SetEvictable(target_frame, false);

  return {pages_, pages_.get() + target_frame};
}

auto BufferPoolManager::UnpinPage(distribution_lsh::page_id_t page_id, bool is_dirty, [[maybe_unused]] distribution_lsh::AccessType access_type) -> bool {
  std::unique_lock<std::mutex> page(latch_);
  // page_id is not in the buffer pool or its pin count is already 0
  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].pin_count_ == 0) {
    return false;
  }

  // Decrease the pin count and set evictable
  auto target_frame = page_table_[page_id];
  if (--pages_[target_frame].pin_count_ == 0) {
    replacer_->SetEvictable(target_frame, true);
  }
  pages_[target_frame].is_dirty_ = is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> page(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  auto target_frame = page_table_[page_id];
  // Write into disk(non-block)
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  std::optional<DiskRequest>
      disk_request({true, reinterpret_cast<char *>(pages_[target_frame].data_), page_id, std::move(promise)});
  disk_scheduler_->request_queue_.Put(std::move(disk_request));
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> page(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    // Write into disk(non-block)
    if (pages_[static_cast<int64_t>(i)].page_id_ != INVALID_PAGE_ID) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      std::optional<DiskRequest>
          disk_request({true, reinterpret_cast<char *>(pages_[static_cast<int64_t>(i)].data_), pages_[static_cast<int64_t>(i)].page_id_, std::move(promise)});
      disk_scheduler_->request_queue_.Put(std::move(disk_request));
      if (!future.get()) {
        LOG_DEBUG("Flush page failed.");
      }
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> page(latch_);

  if (page_table_.find(page_id) == page_table_.end() || pages_[page_table_[page_id]].pin_count_ != 0) {
    return page_table_.find(page_id) == page_table_.end();
  }

  auto target_frame = page_table_[page_id];
  if (page_table_.erase(page_id) == 0U) {
    throw Exception("Delete page failed");
  }

  // Stop trace and add to free list
  replacer_->Remove(target_frame);
  free_list_.emplace_back(target_frame);
  page_table_.erase(pages_[target_frame].page_id_);

  pages_[target_frame].page_id_ = INVALID_PAGE_ID;
  pages_[target_frame].ResetMemory();
  pages_[target_frame].pin_count_ = 0;
  pages_[target_frame].is_dirty_ = false;

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  if (!free_page_list_.empty()) {
    page_id_t free_page = free_page_list_.front();
    free_page_list_.pop_front();
    return free_page;
  }

  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}// namespace distribution_lsh