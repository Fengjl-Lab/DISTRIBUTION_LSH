//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/2.
// src/include/buffer/lru_k_replacer.h
//
//===-----------------------------------------------------

#pragma once

#include <limits>
#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <memory>

#include <common/config.h>
#include <common/macro.h>

namespace distribution_lsh {

class LRUKNode;
class LRUKReplacer;

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 public:
  /**
   *
   * @brief a new LRUKNode which is unique and cannot be moved
   */
  explicit LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}
  ~LRUKNode() = default;
  DISALLOW_COPY_AND_MOVE(LRUKNode);

  /** Remove the access history */
  void RemoveHistory() {
    SetEvictable(false);
    history_.clear();
  }

  /** Record the access */
  void Access(size_t current_time_stamp) {
    history_.emplace_back(current_time_stamp);
    history_.size() <= k_ ? void() : history_.pop_front();
  }

  /** Calculate the k-distance for the node*/
  auto CalKDistance(size_t current_timestamp) -> size_t;

  /** Calculate the 1-distance when size of history least than k */
  auto Cal1Distance(size_t current_timestamp) -> size_t;

  /** Get information for the node */
  auto GetFrameId() -> frame_id_t { return fid_; }
  auto GetEvictable() -> bool { return is_evictable_; }

  /** Set evictable for the node */
  void SetEvictable(bool is_evictable) { is_evictable_ = is_evictable; }

 private:
  /** History of last seen K timestamp of this page. Least recent timestamp stored in front */
  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum of
 * all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k-historical reference is given +inf as its k-distance,
 * When multiple frames has +inf backward k-distance, classical LRU algorithm is
 * used to choose victim.
 */
class LRUKReplacer {
  friend class LRUKNode;
 public:
  /**
   * @brief a new LRUKReplacer.
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * @param[out] frame_id id of frame that is evicted
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * @brief Record the event that the given frame id is accessed at current timestamp.
   *
   * Create a new entry for access history if frame id has not been seen before.
  *
  * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
  * also use DISTRIBUTION_LSH_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id  id of frame that received a new access.
   * @param access_type type of access that was received.
   */
  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  /**
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Not that size is equal to number of evictable entries.
   *
   *  If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not.
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
  * @brief Remove an evictable frame from replacer, along with its access history.
  * This function should also decrement replacer's size if removal is successful.
  *
  * Note that this is different from evicting a frame, which always remove the frame
  * with largest backward k-distance. This function removes specified frame id,
  * no matter what its backward k-distance is.
  *
  * If Remove is called on a non-evictable frame, throw an exception or abort the
  * process.
  *
  * If specified frame is not found, directly return from this function.
  *
  * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * @brief Return replacer's size,  which tracks the number of evictable frames.
   */
  auto Size() -> size_t;

 private:
  std::unordered_map<frame_id_t, std::unique_ptr<LRUKNode>> node_store_;
  size_t current_timestamp_{0};
  size_t num_frames_;
  size_t curr_size_{0};
  size_t replacer_size_{0};
  size_t k_;
  std::mutex latch_;
  static const size_t MAX_CURRENT_TIMESTAMP = 1e10;
};

} // namespace distribution_lsh

