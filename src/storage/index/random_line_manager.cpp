//===----------------------------------------------------
//                     DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/21.
// src/random/random_line_manager.cpp
//
//===-----------------------------------------------------

#include <cmath>
#include <fmt/core.h>
#include <common/exception.h>
#include <storage/index/random_line_manager.h>


namespace distribution_lsh {
RandomLineManager::RandomLineManager(std::string manager_name,
                                     distribution_lsh::BufferPoolManager *bpm,
                                     distribution_lsh::RandomLineGenerator *rlg,
                                     distribution_lsh::page_id_t header_page_id,
                                     int dimension,
                                     float epsilon) :
    manager_name_(std::move(manager_name)), \

    bpm_(bpm),
    header_page_id_(header_page_id),
    dimension_(dimension),
    epsilon_(epsilon),
    rlg_(rlg) {}

auto RandomLineManager::IsEmpty() -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<RandomLineHeaderPage>();
  return header_page->GetSize() == 0;
}

auto RandomLineManager::GenerateRandomLineGroup(int group_size) -> bool {
  if (IsEmpty()) {
    page_id_t new_header_page_id;
    auto new_header_page_basic_guard = bpm_->NewPageGuarded(&new_header_page_id);
    if (new_header_page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto new_header_page_guard = new_header_page_basic_guard.UpgradeWrite();
    auto new_header_page = new_header_page_basic_guard.template AsMut<RandomLineHeaderPage>();
    new_header_page->Init(dimension_, epsilon_);
    header_page_id_ = new_header_page_id;
  }

  RandomLineContext ctx;
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_page_guard.AsMut<RandomLineHeaderPage>();
  // Create a new page to store average random line
  if (header_page->GetAverageRandomLinePageId() == INVALID_PAGE_ID || header_page->GetAverageRandomLinePageId() == HEADER_PAGE_ID) {
    std::unique_ptr<float[]> average_array(new float[dimension_]);
    memset(reinterpret_cast<void *>(average_array.get()), 0, sizeof(float) * dimension_);
    if (!StoreAverageRandomLine(average_array.get(), header_page)) {
      return false;
    }
  }

  // Generate random line group
  auto current_size = 0;
  while (current_size < group_size) {
    std::unique_ptr<float[]> new_array = rlg_->GenerateRandomLine(dimension_);
    if (fabsf(InnerProductByPageId(header_page->GetAverageRandomLinePageId(), new_array.get())) > epsilon_) {
      continue;
    }
    if (!Store(new_array.get(), header_page)) {
      return false;
    }
    UpdateAverageRandomLine(header_page->GetAverageRandomLinePageId(), header_page->random_line_pages_start_[header_page->GetSize() - 1], header_page);
    current_size ++;
  }

  return true;
}

auto RandomLineManager::StoreAverageRandomLine(float *array,
                                               distribution_lsh::RandomLineHeaderPage *header_page) -> bool {
  RandomLineContext random_line_ctx;
  auto new_random_line_page_id = INVALID_PAGE_ID;
  auto new_random_line_page_basic_guard = bpm_->NewPageGuarded(&new_random_line_page_id);
  if (new_random_line_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("Allocate new page for random line failed.");
    return false;
  }

  auto new_random_line_page_guard = new_random_line_page_basic_guard.UpgradeWrite();
  random_line_ctx.write_set_.emplace_back(std::move(new_random_line_page_guard));

  auto new_random_line_page = random_line_ctx.write_set_.back().AsMut<FloatRandomLinePage>();
  new_random_line_page->Init();
  auto current_random_line_page = new_random_line_page;

  // Update header page index
  header_page->average_random_line_page_id_ = new_random_line_page_id;

  for (int current_size = 0; current_size < dimension_; current_size += current_random_line_page->GetMaxSize()) {
    if (dimension_ - current_size > current_random_line_page->GetMaxSize()) {
      memcpy(reinterpret_cast<char*>(current_random_line_page->array_), reinterpret_cast<char*>(&array[current_size]),
             sizeof(float) * current_random_line_page->GetMaxSize());
      current_random_line_page->SetSize(current_random_line_page->GetMaxSize());

      // Allocate new page and update data
      new_random_line_page_basic_guard = bpm_->NewPageGuarded(&new_random_line_page_id);
      if (new_random_line_page_id == INVALID_PAGE_ID) {
        // roll back
        header_page->average_random_line_page_id_ = INVALID_PAGE_ID;
        LOG_DEBUG("Allocate new page for random line failed.");
        return false;
      }
      new_random_line_page_guard = new_random_line_page_basic_guard.UpgradeWrite();
      random_line_ctx.write_set_.emplace_back(std::move(new_random_line_page_guard));
      new_random_line_page = random_line_ctx.write_set_.back().AsMut<FloatRandomLinePage>();
      new_random_line_page->Init();
      current_random_line_page->SetNextPageId(new_random_line_page_id);
      current_random_line_page = new_random_line_page;
    } else {
      memcpy(reinterpret_cast<char*>(current_random_line_page->array_), reinterpret_cast<char*>(&array[current_size]),
             sizeof(float) * (dimension_ - current_size));
      current_random_line_page->SetSize(dimension_ - current_size);
    }
  }

  return true;

}


auto RandomLineManager::Store(float *array, RandomLineHeaderPage *header_page) -> bool {
  // Locate the latest header_page
  RandomLineContext header_page_ctx;
  RandomLineContext random_line_ctx;
  while (header_page->GetNextPageId() != INVALID_PAGE_ID) {
    auto next_header_page_guard = bpm_->FetchPageWrite(header_page->GetNextPageId());
    header_page_ctx.write_set_.emplace_back(std::move(next_header_page_guard));
    header_page = header_page_ctx.write_set_.back().AsMut<RandomLineHeaderPage>();
  }

  if (header_page->GetSize() ==  header_page->GetMaxSize()) {
    auto next_header_page_id = INVALID_PAGE_ID;
    auto next_header_page_basic_guard = bpm_->NewPageGuarded(&next_header_page_id);
    if (next_header_page_id == INVALID_PAGE_ID) {
      LOG_DEBUG("Allocate new page for random line failed.");
      return false;
    }

    auto next_header_page_guard = next_header_page_basic_guard.UpgradeWrite();
    header_page_ctx.write_set_.emplace_back(std::move(next_header_page_guard));
    auto next_header_page = header_page_ctx.write_set_.back().AsMut<RandomLineHeaderPage>();
    next_header_page->Init(dimension_);
    next_header_page->SetEpsilon(epsilon_);
    next_header_page->SetAverageRandomLinePageId(header_page->GetAverageRandomLinePageId());
    header_page->SetNextPageId(next_header_page_id);
    header_page = next_header_page;
  }

  auto new_random_line_page_id = INVALID_PAGE_ID;
  auto new_random_line_page_basic_guard = bpm_->NewPageGuarded(&new_random_line_page_id);
  if (new_random_line_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("Allocate new page for random line failed.");
    return false;
  }

  auto new_random_line_page_guard = new_random_line_page_basic_guard.UpgradeWrite();
  random_line_ctx.write_set_.emplace_back(std::move(new_random_line_page_guard));

  auto new_random_line_page = random_line_ctx.write_set_.back().AsMut<FloatRandomLinePage>();
  new_random_line_page->Init();
  auto current_random_line_page = new_random_line_page;

  // Update header page index
  header_page->random_line_pages_start_[header_page->GetSize()] = new_random_line_page_id;
  header_page->IncreaseSize(1);

  for (int current_size = 0; current_size < dimension_; current_size += current_random_line_page->GetMaxSize()) {
    if (dimension_ - current_size > current_random_line_page->GetMaxSize()) {
      memcpy(reinterpret_cast<char*>(current_random_line_page->array_), reinterpret_cast<char*>(&array[current_size]),
             sizeof(float) * current_random_line_page->GetMaxSize());
      current_random_line_page->SetSize(current_random_line_page->GetMaxSize());

      // Allocate new page and update data
      new_random_line_page_basic_guard = bpm_->NewPageGuarded(&new_random_line_page_id);
      if (new_random_line_page_id == INVALID_PAGE_ID) {
        // roll back
        header_page->IncreaseSize(-1);
        LOG_DEBUG("Allocate new page for random line failed.");
        return false;
      }
      new_random_line_page_guard = new_random_line_page_basic_guard.UpgradeWrite();
      random_line_ctx.write_set_.emplace_back(std::move(new_random_line_page_guard));
      new_random_line_page = random_line_ctx.write_set_.back().AsMut<FloatRandomLinePage>();
      new_random_line_page->Init();
      current_random_line_page->SetNextPageId(new_random_line_page_id);
      current_random_line_page = new_random_line_page;
    } else {
      memcpy(reinterpret_cast<char*>(current_random_line_page->array_), reinterpret_cast<char*>(&array[current_size]),
             sizeof(float) * (dimension_ - current_size));
      current_random_line_page->SetSize(dimension_ - current_size);
    }
  }

  return true;
}

auto RandomLineManager::InnerProduct(distribution_lsh::page_id_t header_page_id, int slot, const float *outer_array) -> float {
  if (slot < 0) {
    throw Exception(ExceptionType::INVALID, "Slot number must bigger than zero");
  }

  RandomLineContext ctx;
  auto header_page_guard  = bpm_->FetchPageRead(header_page_id);
  auto header_page = header_page_guard.As<RandomLineHeaderPage>();
  auto target_random_line_page_id = header_page->random_line_pages_start_[slot];

  return InnerProductByPageId(target_random_line_page_id, outer_array);
}

auto RandomLineManager::InnerProductByPageId(page_id_t random_line_page_id, const float* outer_array) -> float {
  RandomLineContext ctx;
  float result = 0;
  auto current_size = 0;
  while (random_line_page_id != INVALID_PAGE_ID) {
    auto left_random_page_guard = bpm_->FetchPageRead(random_line_page_id);
    auto left_random_page = left_random_page_guard.As<FloatRandomLinePage>();
    ctx.read_set_.emplace_back(std::move(left_random_page_guard));

    for (int i = 0; i < left_random_page->GetSize(); ++i) {
      result += left_random_page->array_[i] * outer_array[i + current_size];
    }

    current_size += left_random_page->GetSize();
    random_line_page_id = left_random_page->GetNextPageId();
  }

  return result;
}

void RandomLineManager::PrintRandomLine(distribution_lsh::page_id_t random_line_page_id) {
  RandomLineContext ctx;
  fmt::print("\033[31mRandom line page number starts at: {}\033[0m\n", random_line_page_id);
  while (random_line_page_id !=  INVALID_PAGE_ID) {
    auto random_line_page_guard = bpm_->FetchPageRead(random_line_page_id);
    auto random_line_page = random_line_page_guard.As<FloatRandomLinePage>();
    ctx.read_set_.emplace_back(std::move(random_line_page_guard));

    fmt::print("\033[33mCurrent page id: {}, random line content:\033[0m\n", random_line_page_id);
    for (int i = 0; i < random_line_page->GetSize(); ++i) {
      fmt::print("\033[36m{:.3f}\033[0m\t", random_line_page->array_[i]);
    }
    random_line_page_id = random_line_page->GetNextPageId();
    fmt::print("\n");
  }
  fmt::print("\n\n");
}

void RandomLineManager::PrintRandomLineGroup() {
  if (IsEmpty()) {
    return;
  }

  RandomLineContext header_page_ctx;
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  header_page_ctx.read_set_.emplace_back(std::move(header_page_guard));
  auto header_page = header_page_ctx.read_set_.back().template As<RandomLineHeaderPage>();
  for (int i = 0; i < header_page->GetSize(); ++i) {
    PrintRandomLine(header_page->random_line_pages_start_[i]);
  }

  while (header_page->GetNextPageId() != INVALID_PAGE_ID) {
    for (int i = 0; i < header_page->GetSize(); ++i) {
      PrintRandomLine(header_page->random_line_pages_start_[i]);
    }

    auto next_header_page_guard = bpm_->FetchPageRead(header_page->GetNextPageId());
    header_page_ctx.read_set_.emplace_back(std::move(next_header_page_guard));
    header_page = header_page_ctx.read_set_.back().As<RandomLineHeaderPage>();
  }
}

void RandomLineManager::UpdateAverageRandomLine(distribution_lsh::page_id_t average_random_line_page_id,
                                                distribution_lsh::page_id_t new_random_line_page_id,
                                                distribution_lsh::RandomLineHeaderPage *header_page) {
  RandomLineContext ctx;
  auto old_factor = header_page->GetSize() == 1 ? 0 : sqrt(static_cast<double>(header_page->GetSize() - 1));
  auto new_factor = sqrt(static_cast<double>(header_page->GetSize()));

  // Update average random line
  while (average_random_line_page_id !=  INVALID_PAGE_ID && new_random_line_page_id != INVALID_PAGE_ID) {
    auto average_random_page_guard = bpm_->FetchPageWrite(average_random_line_page_id);
    auto average_random_page = average_random_page_guard.AsMut<FloatRandomLinePage>();
    ctx.write_set_.emplace_back(std::move(average_random_page_guard));

    auto new_random_page_guard = bpm_->FetchPageRead(new_random_line_page_id);
    auto new_random_page = new_random_page_guard.As<FloatRandomLinePage>();
    ctx.read_set_.emplace_back(std::move(new_random_page_guard));

    for (int i = 0; i < average_random_page->GetSize(); ++i) {
      average_random_page->array_[i] = static_cast<float>((old_factor * average_random_page->array_[i] + new_random_page->array_[i]) / new_factor);
    }

    average_random_line_page_id = average_random_page->GetNextPageId();
    new_random_line_page_id = new_random_page->GetNextPageId();
  }
}

} // namespace distribution_lsh