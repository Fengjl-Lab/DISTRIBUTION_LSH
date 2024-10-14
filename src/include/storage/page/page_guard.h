//===----------------------------------------------------
//                          DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/3.
// src/include/storage/page/page_guard.h
//
//===-----------------------------------------------------

#pragma once

#include <storage/page/page.h>
#include <memory>

namespace distribution_lsh {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

class BasicPageGuard {
 public:
  BasicPageGuard() = default;

  BasicPageGuard(BufferPoolManager *bpm, std::shared_ptr<Page> page) : bpm_(bpm), page_(std::move(page)) { is_dirty_ = page_->IsDirty(); }

  BasicPageGuard(const BasicPageGuard &) = delete;
  auto operator=(const BasicPageGuard &) = delete;

  /**
   * @brief Move constructor for BasicGuard
   *
   * e.g. BasicGuard basic_guard(std::move(other))
   */
  BasicPageGuard(BasicPageGuard &&that) noexcept;

  /**
   * @brief Drop a page guard
   */
  void Drop();

  /**
   * @brief Move assignment for BasicPageGuard
   */
  auto operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard &;

  /**
   * @brief Destructor for BasicPageGuard
   */
  ~BasicPageGuard();

  /**
   *  @brief Upgrade a BasicPageGuard to a ReadPageGuard
   */
  auto UpgradeRead() -> ReadPageGuard;

  /**
   *  @brief Upgrade a BasicPageGuard to a WritedPageGuard
   */
  auto UpgradeWrite() -> WritePageGuard;

  auto PageId() -> page_id_t { return page_->GetPageId(); }

  auto GetData() -> const char* { return page_->GetData(); }

  template<class T>
  auto As() -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }

  auto GetDataMut() -> char * {
    is_dirty_ = true;
    return page_->GetData();
  }

  template<class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }

 private:
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

  BufferPoolManager *bpm_{nullptr};
  std::shared_ptr<Page> page_{nullptr};
  bool is_dirty_{false};
};

class ReadPageGuard {
 public:
  ReadPageGuard() = default;
  ReadPageGuard(BufferPoolManager *bpm, std::shared_ptr<Page> page) : guard_(bpm, std::move(page)) { guard_.page_->RLatch(); }
  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  /**
   * @brief Move constructor for ReadPageGuard
   */
  ReadPageGuard(ReadPageGuard&& that) noexcept;

   /**
    * @brief Move assignment for ReadPageGuard
    */
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;

   /**
    * @brief Drop a ReadPageGuard
    */
  void Drop();

   /**
    * @brief Destructor for ReadPageGuard
    */
  ~ReadPageGuard();

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char * { return guard_.GetData(); }

  template <class T>
  auto As() -> const T * {
    return guard_.As<T>();
  }

 private:
  BasicPageGuard guard_;
};

class WritePageGuard {
 public:
  WritePageGuard() = default;
  WritePageGuard(BufferPoolManager *bpm, std::shared_ptr<Page> page) : guard_(bpm, std::move(page)) { guard_.page_->WLatch(); }
  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;

  /**
   * @brief Move constructor for WritePageGuard
   */
  WritePageGuard(WritePageGuard &&that) noexcept;

  /**
   * @brief Move assignment for WritePageGuard
   */
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;

  /**
   * @brief Drop a WritePageGuard
   */
  void Drop();

  /**
   * @brief Destructor for WritePageGuard
   */
  ~WritePageGuard();

  auto PageId() -> page_id_t { return guard_.PageId(); }

  auto GetData() -> const char * { return guard_.GetData(); }

  template <class T>
  auto As() -> const T * {
    return guard_.As<T>();
  }

  auto GetDataMut() -> char * { return guard_.GetDataMut(); }

  template <class T>
  auto AsMut() -> T * {
    return guard_.AsMut<T>();
  }

 private:
  BasicPageGuard guard_;
};
}// namespace distribution_lsh