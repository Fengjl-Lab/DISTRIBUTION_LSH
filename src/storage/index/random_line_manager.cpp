//===----------------------------------------------------
//                     DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/21.
// src/random/random_line_manager.cpp
//
//===-----------------------------------------------------

#include <cmath>
#include <sstream>
#include <omp.h>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/color.h>

#include <common/exception.h>
#include <storage/index/random_line_manager.h>

namespace distribution_lsh {

RANDOM_LINE_TEMPLATE
RANDOM_LINE_MANAGER_TYPE::RandomLineManager(
    std::string manager_name,
    file_id_t file_id,
    std::shared_ptr<distribution_lsh::BufferPoolManager> bpm,
    std::shared_ptr<distribution_lsh::RandomLineGenerator<ValueType>> rlg,
    distribution_lsh::page_id_t header_page_id,
    int dimension,
    int directory_page_max_size,
    int data_page_max_size,
    RandomLineDistributionType distribution_type,
    RandomLineNormalizationType normalization_type,
    float epsilon) :
    manager_name_(std::move(manager_name)),
    file_id_(file_id),
    bpm_(std::move(bpm)),
    rlg_(std::move(rlg)),
    header_page_id_(header_page_id),
    dimension_(dimension),
    directory_page_max_size_(directory_page_max_size),
    data_page_max_size_(data_page_max_size),
    distribution_type_(distribution_type),
    normalization_type_(normalization_type),
    epsilon_(epsilon) {
  // Check if there has exists an epsilon
  if (!IsEmpty()) {
    LOG_INFO("%s", fmt::format("use parameters in existing header page, header page id: {}", header_page_id_).data());
    auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_page_guard.As<RandomLineHeaderPage>();
    file_id_ = header_page->GetFileIdentification();
    dimension_ = header_page->GetDimension();
    distribution_type_ = header_page->GetDistributionType();
    normalization_type_ = header_page->GetNormalizationType();
    epsilon_ = header_page->GetEpsilon();
  } else {
    auto header_page_basic_guard = bpm_->NewPageGuarded(&header_page_id_);
    if (header_page_id_ == INVALID_PAGE_ID || header_page_id_ != HEADER_PAGE_ID) {
      throw Exception("Allocate header page failed.");
    }

    auto header_page_guard = header_page_basic_guard.UpgradeWrite();
    auto header_page = header_page_guard.template AsMut<RandomLineHeaderPage>();
    header_page->Init(file_id_,
                      dimension_,
                      distribution_type,
                      normalization_type,
                      epsilon_,
                      INVALID_PAGE_ID,
                      INVALID_PAGE_ID);
  }
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::IsEmpty(RandomLineContext *ctx) -> bool {
  if (header_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  if (header_page_id_ != HEADER_PAGE_ID) {
    throw Exception(fmt::format(fg(fmt::color::red), "[ERROR] RANDOM LINE MANAGER: header page id is not valid, current header page id is: {}.", header_page_id_));
  }

  RandomLineContext random_line_ctx;
  auto header_page = ctx != nullptr && ctx->header_page_ != std::nullopt? ctx->header_page_.value().As<RandomLineHeaderPage>() : [&]() {
    random_line_ctx.read_set_.emplace_back(bpm_->FetchPageRead(header_page_id_));
    return random_line_ctx.read_set_.back().As<RandomLineHeaderPage>();
  }();

  return header_page->IsEmpty();
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GenerateRandomLineGroup(int group_size) -> bool {
  {
    std::scoped_lock<std::mutex> header_page_latch(latch_);
    if (IsEmpty()) {
      RandomLineContext ctx;
      ctx.header_page_ = std::make_optional(bpm_->FetchPageWrite(header_page_id_));
      auto header_page = ctx.header_page_.value().AsMut<RandomLineHeaderPage>();
      auto directory_page_guard = bpm_->NewPageGuarded(&header_page->directory_page_start_page_id_);

      if (header_page->GetDirectoryPageStartPageId() == INVALID_PAGE_ID) {
        LOG_DEBUG("create new directory page failed");
        return false;
      }

      auto directory_page = directory_page_guard.template AsMut<RandomLineDirectoryPage>();
      directory_page->Init(directory_page_max_size_);

      // Use average random line
      if (epsilon_ > 0) {
        std::shared_ptr<ValueType[]> average_array(new ValueType[dimension_]);
        memset(reinterpret_cast<void *>(average_array.get()), 0, sizeof(ValueType) * dimension_);
        if (!StoreAverageRandomLine(average_array, &ctx)) {
          LOG_DEBUG("store average random line failed");
          return false;
        }
      }
    }
  }

  // Generate random line group
  auto current_size = 0;
  while (current_size < group_size) {
    std::shared_ptr<ValueType[]> array = rlg_->GenerateRandomLine(distribution_type_, normalization_type_, dimension_);
    if (array == nullptr) {
      LOG_DEBUG("generate new array failed.");
      return false;
    }

    if (epsilon_ > 0) {
      auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
      auto header_page = header_page_guard.As<RandomLineHeaderPage>();
      if (static_cast<float>(std::abs(InnerProduct(header_page->average_random_line_page_id_, array))) > epsilon_) {
        continue;
      }
    }


    // Store the random line
    RandomLineContext ctx;
    ctx.header_page_ = std::make_optional(bpm_->FetchPageWrite(header_page_id_));
    if (!Store(array, &ctx)) {
      return false;
    }

    if (epsilon_ > 0) {
      //  Update average random line
      UpdateAverageRandomLine(array, &ctx);
    }

    current_size++;
  }

  return true;
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::StoreAverageRandomLine(std::shared_ptr<ValueType[]> array, RandomLineContext *ctx) -> bool {
  RandomLineContext random_line_ctx;

  RandomLineHeaderPage* header_page = ctx != nullptr && ctx->header_page_ != std::nullopt?
      ctx->header_page_.value().AsMut<RandomLineHeaderPage>()
      : [&]() {
    random_line_ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
    return random_line_ctx.header_page_.value().AsMut<RandomLineHeaderPage>(); }();

  auto average_random_line_page_basic_guard = bpm_->NewPageGuarded(&header_page->average_random_line_page_id_);
  if (header_page->GetAverageRandomLinePageId() == INVALID_PAGE_ID) {
    LOG_DEBUG("create new average random line page failed.");
    return false;
  }

  random_line_ctx.write_set_.emplace_back(average_random_line_page_basic_guard.UpgradeWrite());

  auto average_random_line_page_id = header_page->average_random_line_page_id_;
  auto current_average_random_line_page_id = average_random_line_page_id;
  AverageRandomLinePage<ValueType> *average_random_line_page =
      random_line_ctx.write_set_.back().template AsMut<AverageRandomLinePage<ValueType>>();
  average_random_line_page->Init(data_page_max_size_);
  std::list<page_id_t > page_id_list;

  auto current_average_random_line_page = average_random_line_page;
  page_id_list.emplace_back(header_page->average_random_line_page_id_);

  for (int current_size = 0; current_size < dimension_;
       current_size += current_average_random_line_page->GetMaxSize()) {
    if (dimension_ - current_size > current_average_random_line_page->GetMaxSize()) {
      memcpy(reinterpret_cast<char *>(current_average_random_line_page->array_),
             reinterpret_cast<char *>(&array[current_size]),
             sizeof(ValueType) * current_average_random_line_page->GetMaxSize());
      current_average_random_line_page->SetSize(current_average_random_line_page->GetMaxSize());

      // Allocate new page and update data
      average_random_line_page_basic_guard = bpm_->NewPageGuarded(&average_random_line_page_id);
      if (average_random_line_page_id == INVALID_PAGE_ID) {
        // Roll back
        header_page->average_random_line_page_id_ = INVALID_PAGE_ID;

        // Delete page allocate before
        while (!page_id_list.empty()) {
          if (page_id_list.front() == current_average_random_line_page_id) {
            random_line_ctx.write_set_.pop_front();
          }

          bpm_->DeletePage(page_id_list.front());
          page_id_list.pop_front();
        }

        LOG_DEBUG("allocate new page for random line failed.");
        return false;
      }

      random_line_ctx.write_set_.emplace_back(average_random_line_page_basic_guard.UpgradeWrite());
      current_average_random_line_page->SetNextPageId(average_random_line_page_id);
      page_id_list.emplace_back(average_random_line_page_id);

      current_average_random_line_page_id = average_random_line_page_id;
      current_average_random_line_page = random_line_ctx.write_set_.back().AsMut<AverageRandomLinePage<ValueType>>();
      current_average_random_line_page->Init(data_page_max_size_);

      random_line_ctx.write_set_.pop_front();
    } else {
      memcpy(reinterpret_cast<char *>(current_average_random_line_page->array_), reinterpret_cast<char *>(&array[current_size]),
             sizeof(ValueType) * (dimension_ - current_size));
      current_average_random_line_page->SetSize(dimension_ - current_size);
    }
  }

  return true;

}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::Store(std::shared_ptr<ValueType[]> array, RandomLineContext *ctx) -> bool {
  RandomLineContext random_line_ctx;
  // Get header page
  RandomLineHeaderPage* header_page =
      ctx != nullptr && ctx->header_page_ != std::nullopt?
              ctx->header_page_.value().AsMut<RandomLineHeaderPage>()
              : [&]() {
        random_line_ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
        return random_line_ctx.header_page_.value().AsMut<RandomLineHeaderPage>(); }();

  // Locate the latest header_page
  RandomLineContext directory_page_ctx;
  RandomLineContext data_page_ctx;

  auto directory_page_guard = bpm_->FetchPageWrite(header_page->directory_page_start_page_id_);
  directory_page_ctx.write_set_.emplace_back(std::move(directory_page_guard));
  auto directory_page = directory_page_ctx.write_set_.back().AsMut<RandomLineDirectoryPage>();
  while (directory_page->GetNextPageId() != INVALID_PAGE_ID) {
    directory_page_guard = bpm_->FetchPageWrite(directory_page->GetNextPageId());
    directory_page_ctx.write_set_.emplace_back(std::move(directory_page_guard));
    directory_page = directory_page_ctx.write_set_.back().AsMut<RandomLineDirectoryPage>();
    directory_page_ctx.write_set_.pop_front();
  }

  if (directory_page->GetSize() == directory_page->GetMaxSize()) {
    auto next_directory_page_id = INVALID_PAGE_ID;
    auto next_directory_page_basic_guard = bpm_->NewPageGuarded(&next_directory_page_id);
    if (next_directory_page_id == INVALID_PAGE_ID) {
      LOG_DEBUG("allocate new page for random line failed.");
      return false;
    }

    directory_page_ctx.write_set_.emplace_back(next_directory_page_basic_guard.UpgradeWrite());
    auto next_directory_page = directory_page_ctx.write_set_.back().AsMut<RandomLineDirectoryPage>();
    next_directory_page->Init(directory_page_max_size_);
    directory_page->SetNextPageId(next_directory_page_id);
    directory_page = next_directory_page;
  }

  auto random_line_page_id = INVALID_PAGE_ID;
  auto random_line_page_basic_guard = bpm_->NewPageGuarded(&random_line_page_id);
  if (random_line_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("allocate new page for random line failed.");
    return false;
  }

  auto start_random_line_page_id = random_line_page_id;
  data_page_ctx.write_set_.emplace_back(random_line_page_basic_guard.UpgradeWrite());
  RandomLineDataPage<ValueType>
      *random_line_page = data_page_ctx.write_set_.back().AsMut<RandomLineDataPage<ValueType>>();
  random_line_page->Init(data_page_max_size_);
  auto current_random_line_page = random_line_page;
  auto start_random_line_page = random_line_page;

  // Update header page index
  auto slot = -1;
  directory_page->Insert(random_line_page_id, &slot);

  for (int current_size = 0; current_size < dimension_; current_size += current_random_line_page->GetMaxSize()) {
    if (dimension_ - current_size > current_random_line_page->GetMaxSize()) {
      memcpy(reinterpret_cast<char *>(current_random_line_page->array_), reinterpret_cast<char *>(&array[current_size]),
             sizeof(ValueType) * current_random_line_page->GetMaxSize());
      current_random_line_page->SetSize(current_random_line_page->GetMaxSize());

      // Allocate new page and update data
      random_line_page_id = INVALID_PAGE_ID;
      random_line_page_basic_guard = bpm_->NewPageGuarded(&random_line_page_id);
      if (random_line_page_id == INVALID_PAGE_ID) {
        // Roll back
        directory_page->Delete(start_random_line_page_id);

        // Delete page allocate before
        auto delete_random_line_page = start_random_line_page;
        auto delete_random_line_page_id = start_random_line_page_id;
        while (delete_random_line_page->GetNextPageId() != INVALID_PAGE_ID) {
          data_page_ctx.write_set_.pop_front();
          bpm_->DeletePage(delete_random_line_page_id);
          delete_random_line_page_id = delete_random_line_page->GetNextPageId();
          delete_random_line_page = data_page_ctx.write_set_.front().template AsMut<RandomLineDataPage<ValueType>>();
        }

        // Delete last page
        data_page_ctx.write_set_.pop_front();
        bpm_->DeletePage(delete_random_line_page_id);

        LOG_DEBUG("allocate new page for random line failed.");
        return false;
      }

      data_page_ctx.write_set_.emplace_back(random_line_page_basic_guard.UpgradeWrite());

      random_line_page = data_page_ctx.write_set_.back().AsMut<RandomLineDataPage<ValueType> >();
      random_line_page->Init(data_page_max_size_);

      current_random_line_page->SetNextPageId(random_line_page_id);
      current_random_line_page = random_line_page;

      data_page_ctx.write_set_.pop_front();
    } else {
      memcpy(reinterpret_cast<char *>(current_random_line_page->array_), reinterpret_cast<char *>(&array[current_size]),
             sizeof(ValueType) * (dimension_ - current_size));
      current_random_line_page->SetSize(dimension_ - current_size);
    }
  }

  return true;
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::InnerProduct(
    int index,
    distribution_lsh::page_id_t *directory_page_id,
    int *slot,
    std::shared_ptr<ValueType[]> outer_array) -> ValueType {

  if (index < 0 || IsEmpty()) {
    throw Exception(ExceptionType::OUT_OF_RANGE, fmt::format(fg(fmt::color::red), "[ERROR] RANDOM LINE MANAGER: current file is empty, invalid index {} input.", index));
  }

  RandomLineContext ctx;

  // Locate the target directory page
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.template As<RandomLineHeaderPage>();
  ctx.read_set_.emplace_back(bpm_->FetchPageRead(header_page->GetDirectoryPageStartPageId()));
  auto random_line_page = ctx.read_set_.back().template As<RandomLineDirectoryPage>();

  auto current_size = 0;
  while (current_size <= index) {
    if (random_line_page->GetMaxSize() > index - current_size) {
      *directory_page_id = ctx.read_set_.back().PageId();
      *slot = index - current_size;
      break;
    }

    if (random_line_page->GetNextPageId() == INVALID_PAGE_ID) {
      throw Exception(ExceptionType::OUT_OF_RANGE, fmt::format(fg(fmt::color::red), "[ERROR] RANDOM LINE MANAGER: index out of range, invalid index {} input.", index));
    }

    current_size += random_line_page->GetMaxSize();
    ctx.read_set_.emplace_back(bpm_->FetchPageRead(random_line_page->GetNextPageId()));
    ctx.read_set_.pop_front();
    random_line_page = ctx.read_set_.back().template As<RandomLineDirectoryPage>();
  }

  auto target_random_line_start_page_id = random_line_page->IndexAt(*slot);
  if (target_random_line_start_page_id == INVALID_PAGE_ID) {
    throw Exception(ExceptionType::NULL_SLOT, fmt::format(fg(fmt::color::red), "[ERROR] RANDOM LINE MANAGER: input slot {} is not valid", *slot), false);
  }

  return InnerProduct(target_random_line_start_page_id, outer_array);
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::InnerProduct(page_id_t random_line_page_start_id,
                                            std::shared_ptr<ValueType[]> outer_array) -> ValueType {
  RandomLineContext ctx;
  auto result = static_cast<ValueType>(0.0F);
  auto current_size = 0;
  auto random_line_page_id = random_line_page_start_id;

  while (random_line_page_id != INVALID_PAGE_ID) {
    ctx.read_set_.emplace_back(bpm_->FetchPageRead(random_line_page_id));
    auto random_page = ctx.read_set_.back().template As<RandomLineDataPage<ValueType>>();

    // Calculate inner product
#pragma omp parallel for reduction(+:result) default(none) shared(random_page, outer_array, current_size)
    for (int i = 0; i < random_page->GetSize(); ++i) {
      result += random_page->array_[i] * outer_array[i + current_size];
    }

    current_size += random_page->GetSize();
    random_line_page_id = random_page->GetNextPageId();
    ctx.read_set_.pop_front();
  }

  return result;
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::RandomLineInformation(distribution_lsh::page_id_t random_line_page_id) -> std::string {
  std::stringstream ss;

  ss << fmt::format(fg(fmt::color::orange) | fmt::emphasis::bold, "random line starts at {} [[", random_line_page_id) << "\n";
  while (random_line_page_id != INVALID_PAGE_ID) {
    auto random_line_page_guard = bpm_->FetchPageRead(random_line_page_id);
    auto random_line_page = random_line_page_guard.As<RandomLinePage>();
    if (random_line_page->GetPageType() != RandomLinePageType::DATA_PAGE) {
      LOG_DEBUG("%s", fmt::format("invalid page type, expected page type: DATA PAGE, current page type: {}.", RandomLinePageTypeToString(random_line_page->GetPageType())).data());
      return "";
    }

    auto random_line_data_page = random_line_page_guard.template As<RandomLineDataPage<ValueType>>();

    ss << fmt::format("current page id: {}, random line content [ ", random_line_page_id);
    for (int i = 0; i < random_line_page->GetSize(); ++i) {
      ss << fmt::format(fg(fmt::color::brown), "{:.3f}\t", random_line_data_page->array_[i]);
    }
    ss << "]\n";

    random_line_page_id = random_line_data_page->GetNextPageId();
  }

  ss << fmt::format(fg(fmt::color::orange) | fmt::emphasis::bold, "]]\n\n");

  return ss.str();
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::RandomLineGroupInformation() -> std::string {
  if (IsEmpty()) {
    return "";
  }

  std::stringstream ss;
  ss << fmt::format(fg(fmt::color::yellow) | fmt::emphasis::bold, "random line group information :") << "\n";
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<RandomLineHeaderPage>();
  auto directory_page_guard = bpm_->FetchPageRead(header_page->GetDirectoryPageStartPageId());
  auto directory_page = directory_page_guard.As<RandomLineDirectoryPage>();

  // Record random line in each directory page
  while (directory_page != nullptr) {
    for (int index = 0; index < directory_page_max_size_; ++index) {
      try {
        auto random_line_start_page_id = directory_page->IndexAt(index);
        if (random_line_start_page_id == INVALID_PAGE_ID) {
          continue;
        }

        ss << RandomLineInformation(random_line_start_page_id);
      } catch (Exception &exception) {
        break;
      }
    }


    if (directory_page->GetNextPageId() == INVALID_PAGE_ID) {
      break;
    }
    directory_page_guard.Drop();
    directory_page_guard = bpm_->FetchPageRead(directory_page->GetNextPageId());
    directory_page = directory_page_guard.As<RandomLineDirectoryPage>();
  }

  return ss.str();
}

RANDOM_LINE_TEMPLATE
void RANDOM_LINE_MANAGER_TYPE::UpdateAverageRandomLine(std::shared_ptr<ValueType[]> array, RandomLineContext *ctx) {
  RandomLineContext random_line_ctx;

  RandomLineHeaderPage* header_page = ctx != nullptr && ctx->header_page_ != std::nullopt?
                                      ctx->header_page_.value().AsMut<RandomLineHeaderPage>()
                                                                                         : [&]() {
        random_line_ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
        return random_line_ctx.header_page_.value().AsMut<RandomLineHeaderPage>(); }();

  auto random_line_page_guard = bpm_->FetchPageWrite(header_page->GetAverageRandomLinePageId());
  auto random_line_page = random_line_page_guard.AsMut<RandomLinePage>();

  if (random_line_page->GetPageType() != RandomLinePageType::AVERAGE_RANDOM_LINE_PAGE) {
    LOG_DEBUG("invalid page type.");
    return;
  }

  auto average_random_line_page = random_line_page_guard.AsMut<AverageRandomLinePage<ValueType>>();
  auto factor = GetSize(ctx) - 1;

  // Update average random line
  for (int current_size = 0; current_size < dimension_; current_size += average_random_line_page->GetSize()) {
#pragma omp parallel for default(none) shared(average_random_line_page, array, current_size, factor)
    for (int index = 0; index < average_random_line_page->GetSize(); ++index) {
      average_random_line_page->array_[index] = (average_random_line_page->array_[index] * factor + array[current_size + index]) / (factor + 1);
    }

    if (average_random_line_page->GetNextPageId() == INVALID_PAGE_ID && dimension_ - current_size > average_random_line_page->GetSize()) {
      throw Exception(fmt::format(fg(fmt::color::red), "average random line data is not completeness, current page id: {}", random_line_page_guard.PageId()));
    } else if (average_random_line_page->GetNextPageId() == INVALID_PAGE_ID) {
      return;
    }

    random_line_page_guard.Drop();
    random_line_page_guard = bpm_->FetchPageWrite(average_random_line_page->GetNextPageId());
    average_random_line_page = random_line_page_guard.AsMut<AverageRandomLinePage<ValueType>>();
  }
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetSize(RandomLineContext *ctx) -> int {
  if (IsEmpty(ctx)) {
    return 0;
  }

  RandomLineContext random_line_ctx;
  auto size = 0;

  auto header_page = ctx != nullptr && ctx->header_page_ != std::nullopt? ctx->header_page_.value().As<RandomLineHeaderPage>() : [&](){
    random_line_ctx.read_set_.emplace_back(bpm_->FetchPageRead(header_page_id_));
    return random_line_ctx.read_set_.back().As<RandomLineHeaderPage>();
  }();

  auto directory_page_guard = bpm_->FetchPageRead(header_page->GetDirectoryPageStartPageId());
  auto directory_page = directory_page_guard.template As<RandomLineDirectoryPage>();
  while (directory_page->GetNextPageId() != INVALID_PAGE_ID) {
    size += directory_page->GetSize();
    directory_page_guard.Drop();
    directory_page_guard = bpm_->FetchPageRead(directory_page->GetNextPageId());
    directory_page = directory_page_guard.template As<RandomLineDirectoryPage>();
  }

  size += directory_page->GetSize();

  return size;
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::Delete(page_id_t directory_page_id, int slot) -> bool {
  auto random_line_page_guard = bpm_->FetchPageWrite(directory_page_id);
  auto random_line_page = random_line_page_guard.AsMut<RandomLinePage>();
  if (random_line_page->GetPageType() != RandomLinePageType::DIRECTORY_PAGE) {
    LOG_DEBUG("%s", fmt::format("input page is not directory page, input page type: {}", RandomLinePageTypeToString(random_line_page->GetPageType())).data());
    return false;
  }

  auto directory_page = random_line_page_guard.AsMut<RandomLineDirectoryPage>();
  auto random_line_data_page_id = directory_page->IndexAt(slot);
  if (random_line_data_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("%s", fmt::format("invalid slot {}.", slot).data());
    return false;
  }

  // Delete entry in directory page
  directory_page->Delete(slot);

  // Delete actual data page
  RandomLineContext data_ctx;
  data_ctx.write_set_.emplace_back(bpm_->FetchPageWrite(random_line_data_page_id));
  auto random_line_data_page = data_ctx.write_set_.back().AsMut<RandomLineDataPage<ValueType>>();
  while (random_line_data_page->GetNextPageId()!= INVALID_PAGE_ID) {
    auto next_random_line_data_page_id = random_line_data_page->GetNextPageId();
    data_ctx.write_set_.pop_front();
    bpm_->DeletePage(random_line_data_page_id);

    random_line_data_page_id = next_random_line_data_page_id;
    data_ctx.write_set_.emplace_back(bpm_->FetchPageWrite(random_line_data_page_id));
    random_line_data_page = data_ctx.write_set_.back().AsMut<RandomLineDataPage<ValueType>>();
  }

  // Delete last page
  data_ctx.write_set_.pop_front();
  bpm_->DeletePage(random_line_data_page_id);

  // If directory page is empty, delete it
  if (directory_page->GetSize() == 0) {
    RandomLineContext directory_ctx;
    directory_ctx.header_page_ = std::make_optional(bpm_->FetchPageWrite(header_page_id_));
    auto header_page = directory_ctx.header_page_->AsMut<RandomLineHeaderPage>();

    // Delete directory page is the first directory page
    if (header_page->GetDirectoryPageStartPageId() == directory_page_id) {
      header_page->SetDirectoryPageStartPageId(directory_page->GetNextPageId());
      return true;
    }

    directory_ctx.write_set_.emplace_back(bpm_->FetchPageWrite(header_page->GetDirectoryPageStartPageId()));
    auto before_directory_page = directory_ctx.write_set_.back().AsMut<RandomLineDirectoryPage>();

    while (before_directory_page->GetNextPageId() != directory_page_id) {
      if (before_directory_page->GetNextPageId() == INVALID_PAGE_ID) {
        LOG_DEBUG("%s", fmt::format("invalid directory page list, current directory page id: {}, the next page is invalid.", directory_ctx.write_set_.back().PageId()).data());
        return false;
      }

      directory_ctx.write_set_.emplace_back(bpm_->FetchPageWrite(before_directory_page->GetNextPageId()));
      before_directory_page = directory_ctx.write_set_.back().AsMut<RandomLineDirectoryPage>();
      directory_ctx.write_set_.pop_front();
    }

    before_directory_page->SetNextPageId(directory_page->GetNextPageId());
    random_line_page_guard.Drop();
    bpm_->DeletePage(directory_page_id);
  }

  return true;
}

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetEpsilon() -> float { return epsilon_; }

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetDimension() -> int { return dimension_; }

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetDirectoryPageMaxSize() -> int { return directory_page_max_size_; }

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetDataPageMaxSize() -> int { return data_page_max_size_; }

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetDistributionType() -> RandomLineDistributionType { return distribution_type_; }

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::GetNormalizationType() -> RandomLineNormalizationType { return normalization_type_; }

RANDOM_LINE_TEMPLATE
auto RANDOM_LINE_MANAGER_TYPE::ToString() -> std::string {
  auto info = fmt::format(
      "Random Line Manager Information:(file id :{})\n"
      "├─────manager name: {}.\n"
      "├─────epsilon: {}.\n"
      "├─────dimension: {}.\n"
      "├─────directory page max size: {}.\n"
      "├─────data page max size: {}.\n"
      "├─────distribution type: {}.\n"
      "└─────normalization type: {}.\n"
      ,
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::underline, std::to_string(file_id_)),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, manager_name_),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, "{:.3f}", epsilon_),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, "{}", dimension_),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, "{}", directory_page_max_size_),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, "{}", data_page_max_size_),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, RandomLineDistributionTypeToString(distribution_type_)),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, RandomLineNormalizationTypeToString(normalization_type_))
      );

  return info;
}

template
class RandomLineManager<float>;

template
class RandomLineManager<double>;
} // namespace distribution_lsh