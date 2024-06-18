//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/storage/disk/disk_scheduler.cpp
//
//===-----------------------------------------------------

#include <storage/disk/disk_scheduler.h>
#include <common/exception.h>
#include <storage/disk/disk_manager.h>

namespace distribution_lsh {

DiskScheduler::DiskScheduler(std::shared_ptr<distribution_lsh::DiskManager> disk_manager) : disk_manager_(std::move(disk_manager)) {
  // Spawn the background thread
  background_thread_.emplace([&]{ StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }

  disk_manager_->ShutDown();
}

void DiskScheduler::Schedule(distribution_lsh::DiskRequest r) {
  if (r.is_write_) {
    disk_manager_->WritePage(r.page_id_, r.data_);
    r.callback_.set_value(true);
  } else {
    disk_manager_->ReadPage(r.page_id_, r.data_);
    r.callback_.set_value(true);
  }
}

void DiskScheduler::StartWorkerThread() {
  // loop to get the request
  std::optional<DiskRequest> disk_request;
  while ((disk_request = request_queue_.Get()).has_value() && disk_request != std::nullopt) {
    Schedule(std::move(disk_request.value()));
  }
}

}// namespace distribution_lsh