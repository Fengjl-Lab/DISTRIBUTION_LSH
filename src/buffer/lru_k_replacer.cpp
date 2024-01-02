//===----------------------------------------------------
//                          QALSH
// Created by chenjunhao on 2024/1/2.
// src/buffer/lru_k_replacer.cpp
//
//===-----------------------------------------------------

#include <buffer/lru_k_replacer.h>
#include <common/exception.h>

namespace qalsh {

auto LRUKNode::CalKDistance(size_t current_timestamp) -> size_t {
  return history_.size() < k_ ? \
      std::numeric_limits<size_t>::max() : \
      (LRUKReplacer::MAX_CURRENT_TIMESTAMP + current_timestamp - *history_.begin())
      % LRUKReplacer::MAX_CURRENT_TIMESTAMP;
}

auto LRUKNode::Cal1Distance(size_t current_timestamp) -> size_t {
  return history_.empty() ? \
      std::numeric_limits<size_t>::max() : \
      (LRUKReplacer::MAX_CURRENT_TIMESTAMP + current_timestamp - history_.back()) % LRUKReplacer::MAX_CURRENT_TIMESTAMP;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : num_frames_(num_frames), k_(k) {
  for (size_t frame_id = 0; frame_id < num_frames_; frame_id++) {
    std::unique_ptr<LRUKNode> node(new LRUKNode(this->k_, frame_id));
    node_store_.insert({frame_id, std::move(node)});
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> frame(latch_);

  if (replacer_size_ == 0) {
    return false;
  }
  size_t max_k_distance = 0;
  size_t max_last_distance = 0;
  auto max_k_distance_frame = -1;

  // Iterate the frame_id_t array
  for (auto &node : node_store_) {
    if (node.second->GetEvictable() && (node.second->CalKDistance(current_timestamp_) > max_k_distance \
 || (node.second->CalKDistance(current_timestamp_) == max_k_distance
        && node.second->Cal1Distance(current_timestamp_) > max_last_distance))) {
      max_k_distance = node.second->CalKDistance(current_timestamp_);
      max_last_distance = node.second->Cal1Distance(current_timestamp_);
      max_k_distance_frame = node.first;
      *frame_id = node.first;
    }
  }

  node_store_.erase(max_k_distance_frame);
  this->replacer_size_--;
  this->curr_size_--;
  this->current_timestamp_ = (this->current_timestamp_ + 1) % MAX_CURRENT_TIMESTAMP;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> frame(latch_);

  QALSH_ASSERT(frame_id >= 0 && frame_id <= static_cast<int>(num_frames_), "Invalid frame id");
  auto target_frame_iterator = node_store_.find(frame_id);
  if (target_frame_iterator != node_store_.end()) {
    node_store_.find(frame_id)->second->Access(this->current_timestamp_);
  } else {
    std::unique_ptr<LRUKNode> node(new LRUKNode(this->k_, frame_id));
    node->Access(this->current_timestamp_);
    node_store_.insert({frame_id, std::move(node)});
    this->curr_size_++;
  }

  this->current_timestamp_ = (this->current_timestamp_ + 1) % MAX_CURRENT_TIMESTAMP;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> frame(latch_);

  auto target_frame_iterator = node_store_.find(frame_id);
  if (target_frame_iterator != node_store_.end() && set_evictable != target_frame_iterator->second->GetEvictable()) {
    target_frame_iterator->second->SetEvictable(set_evictable);
    set_evictable ? [&]() {
      replacer_size_++;
      curr_size_++;
    }() : \
                    [&]() {
      replacer_size_--;
      curr_size_--;
    }();
  } else if (target_frame_iterator == node_store_.end()) {
    throw Exception("Set evict failed.");
  }

  this->current_timestamp_ = (this->current_timestamp_ + 1) % MAX_CURRENT_TIMESTAMP;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> frame(latch_);

  auto target_frame_iterator = node_store_.find(frame_id);
  if (target_frame_iterator == node_store_.end()) {
    return;
  }
  target_frame_iterator->second->GetEvictable() ? node_store_.erase(frame_id) : \
                                               throw Exception("Try to remove a non-evictable frame.");
  this->curr_size_--;
  this->replacer_size_--;
  this->current_timestamp_ = (this->current_timestamp_ + 1) % MAX_CURRENT_TIMESTAMP;
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> frame(latch_);
  return this->replacer_size_;
}

} // namespace qalsh