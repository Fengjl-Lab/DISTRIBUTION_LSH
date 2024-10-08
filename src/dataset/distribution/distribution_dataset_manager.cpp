//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/1/24.
// src/dataset/distribution/distribution_dataset_manager.cpp
//
//===-----------------------------------------------------

#include <memory>
#include <utility>
#include <dataset/distribution/distribution_dataset_manager.h>

namespace distribution_lsh {

DISTRIBUTION_DATASET_TEMPLATE
DISTRIBUTION_DATASET_MANAGER_TYPE::DistributionDataSetManager(
    std::string manager_name,
    distribution_lsh::DataSetType data_set_type,
    distribution_lsh::DistributionType distribution_type,
    distribution_lsh::NormalizationType normalization_type,
    std::shared_ptr<BufferPoolManager> training_set_bpm,
    std::shared_ptr<BufferPoolManager> testing_set_bpm,
    std::unique_ptr<DistributionDatasetProcessor<ValueType>> ddp,
    distribution_lsh::page_id_t training_set_header_page_id,
    distribution_lsh::page_id_t testing_set_header_page_id,
    int dimension,
    std::shared_ptr<float[2]> params,
    std::string directory_name,
    file_id_t training_set_file_id,
    file_id_t testing_set_file_id,
    int directory_page_max_size,
    int data_page_max_size) :
    manager_name_(std::move(manager_name)),
    data_set_type_(data_set_type),
    distribution_type_(distribution_type),
    normalization_type_(normalization_type),
    training_set_bpm_(std::move(training_set_bpm)),
    testing_set_bpm_(std::move(testing_set_bpm)),
    ddp_(std::move(ddp)),
    training_set_header_page_id_(training_set_header_page_id),
    testing_set_header_page_id_(testing_set_header_page_id),
    dimension_(dimension),
    params_(std::move(params)),
    directory_name_(std::move(directory_name)),
    directory_page_max_size_(directory_page_max_size),
    data_page_max_size_(data_page_max_size),
    training_set_file_id_(training_set_file_id),
    testing_set_file_id_(testing_set_file_id) {
  // Check if training set matches testing set
  DISTRIBUTION_LSH_ENSURE(IsTrainingSetMatchTestingSet(), "Training set and testing set not matched");

  // Select data from file
  if (!IsEmpty()) {
    auto training_set_header_page_guard = training_set_bpm_->FetchPageRead(HEADER_PAGE_ID);
    auto training_set_header_page = training_set_header_page_guard.As<DistributionDataSetHeaderPage>();

    auto testing_set_header_page_guard = testing_set_bpm_->FetchPageRead(HEADER_PAGE_ID);
    auto testing_set_header_page = testing_set_header_page_guard.As<DistributionDataSetHeaderPage>();

    this->training_set_file_id_ = training_set_header_page->GetFileIdentification();
    this->testing_set_file_id_ = testing_set_header_page->GetFileIdentification();
    this->data_set_type_ = training_set_header_page->GetDataSetType();
    this->dimension_ = training_set_header_page->GetDimension();
    this->normalization_type_ = training_set_header_page->GetNormalizationType();
    this->training_set_header_page_id_ = HEADER_PAGE_ID;
    this->testing_set_header_page_id_ = HEADER_PAGE_ID;
    switch (this->data_set_type_) {
      case DataSetType::INVALID_DATA_SET_TYPE: {
        throw Exception("Invalid data set type");
      }
      case DataSetType::GENERATION: {
        auto training_set_generation_header_page =
            reinterpret_cast<const DistributionDataSetGenerationHeaderPage *>(training_set_header_page);
        this->distribution_type_ = training_set_generation_header_page->GetDistributionType();
        this->params_ = training_set_generation_header_page->GetParameter();
        break;
      }
      case DataSetType::MNIST:
      case DataSetType::CIFAR10: break;
      default: {
        throw Exception("Unsupported data set type");
      }
    }
  } else {
    // Allocate header page for training set and testing set
    auto training_set_header_page_basic_guard = training_set_bpm_->NewPageGuarded(&training_set_header_page_id_);
    auto testing_set_header_page_basic_guard = testing_set_bpm_->NewPageGuarded(&testing_set_header_page_id_);
    if (training_set_header_page_id_ == INVALID_PAGE_ID || testing_set_header_page_id_ == INVALID_PAGE_ID) {
      throw Exception("Failed to allocate header page");
    }

    // Initialize header page
    auto training_set_header_page_guard = training_set_header_page_basic_guard.UpgradeWrite();
    auto training_set_header_page = training_set_header_page_guard.template AsMut<DistributionDataSetHeaderPage>();

    auto testing_set_header_page_guard = testing_set_header_page_basic_guard.UpgradeWrite();
    auto testing_set_header_page = testing_set_header_page_guard.template AsMut<DistributionDataSetHeaderPage>();

    switch (this->data_set_type_) {
      case DataSetType::INVALID_DATA_SET_TYPE: {
        throw Exception("Invalid data set type");
      }
      case DataSetType::GENERATION: {
        auto training_set_generation_header_page =
            reinterpret_cast<DistributionDataSetGenerationHeaderPage *>(training_set_header_page);
        training_set_generation_header_page->Init(this->training_set_file_id_, this->distribution_type_, this->normalization_type_, this->params_[0],
                                                  this->params_[1], this->dimension_, INVALID_PAGE_ID);

        auto testing_set_generation_header_page =
            reinterpret_cast<DistributionDataSetGenerationHeaderPage *>(testing_set_header_page);
        testing_set_generation_header_page->Init(this->testing_set_file_id_, this->distribution_type_, this->normalization_type_, this->params_[0],
                                                 this->params_[1], this->dimension_, INVALID_PAGE_ID);
        break;
      }
      case DataSetType::MNIST: {
        auto training_set_mnist_header_page =
            reinterpret_cast<DistributionDataSetMNISTHeaderPage *>(training_set_header_page);
        training_set_mnist_header_page->Init(this->training_set_file_id_, this->normalization_type_, INVALID_PAGE_ID);

        auto testing_set_mnist_header_page =
            reinterpret_cast<DistributionDataSetMNISTHeaderPage *>(testing_set_header_page);
        testing_set_mnist_header_page->Init(this->testing_set_file_id_, this->normalization_type_, INVALID_PAGE_ID);
        break;
      }
      case DataSetType::CIFAR10: {
        auto training_set_cifar10_header_page =
            reinterpret_cast<DistributionDataSetCIFAR10HeaderPage *>(training_set_header_page);
        training_set_cifar10_header_page->Init(this->training_set_file_id_, this->normalization_type_, INVALID_PAGE_ID);

        auto testing_set_cifar10_header_page =
            reinterpret_cast<DistributionDataSetCIFAR10HeaderPage *>(testing_set_header_page);
        testing_set_cifar10_header_page->Init(this->testing_set_file_id_, this->normalization_type_, INVALID_PAGE_ID);
        break;
      }
      default: {
        throw Exception("Unsupported data set type");
      }
    }
  }
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::IsEmpty(DistributionDataSetContext *ctx, bool is_read) -> bool {
  if (training_set_header_page_id_ == INVALID_PAGE_ID && testing_set_header_page_id_ == INVALID_PAGE_ID) {
    return true;
  }

  auto training_set_header_page = ctx != nullptr && !is_read ? [&]() {
    if (ctx->training_set_header_page_.has_value()) {
      return ctx->training_set_header_page_.value().template As<DistributionDataSetHeaderPage>();
    }
    ctx->training_set_header_page_ = training_set_bpm_->FetchPageWrite(training_set_header_page_id_);
    return ctx->training_set_header_page_.value().template As<DistributionDataSetHeaderPage>();
  }() : [&]() {
    return ctx != nullptr && ctx->training_set_header_page_.has_value() ?
           ctx->training_set_header_page_.value().template As<DistributionDataSetHeaderPage>()
           : training_set_bpm_->FetchPageRead(training_set_header_page_id_).template As<DistributionDataSetHeaderPage>();
  }();

  auto testing_set_header_page = ctx != nullptr && !is_read ? [&]() {
    if (ctx->testing_set_header_page_.has_value()) {
      return ctx->testing_set_header_page_.value().template As<DistributionDataSetHeaderPage>();
    }
    ctx->testing_set_header_page_ = testing_set_bpm_->FetchPageWrite(testing_set_header_page_id_);
    return ctx->testing_set_header_page_.value().template As<DistributionDataSetHeaderPage>();
  }() : [&]() {
    return ctx != nullptr && ctx->testing_set_header_page_.has_value() ?
           ctx->testing_set_header_page_.value().template As<DistributionDataSetHeaderPage>()
           : testing_set_bpm_->FetchPageRead(testing_set_header_page_id_).template As<DistributionDataSetHeaderPage>();
  }();

  return training_set_header_page->IsEmpty() && testing_set_header_page->IsEmpty();
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::IsTrainingSetMatchTestingSet() -> bool {
  if (IsEmpty()) {
    return true;
  }
  // If training set and testing set are matched, then we can check if they are empty
  auto training_set_header_page_guard = training_set_bpm_->FetchPageRead(HEADER_PAGE_ID);
  auto training_set_header_page = training_set_header_page_guard.As<DistributionDataSetHeaderPage>();

  auto testing_set_header_page_guard = testing_set_bpm_->FetchPageRead(HEADER_PAGE_ID);
  auto testing_set_header_page = testing_set_header_page_guard.As<DistributionDataSetHeaderPage>();

  DISTRIBUTION_LSH_ENSURE(training_set_header_page->GetFileType() == FileType::DISTRIBUTION_DATASET_FILE, "File type not matched");
  DISTRIBUTION_LSH_ASSERT(testing_set_header_page->GetFileType() == FileType::DISTRIBUTION_DATASET_FILE, "File type not matched");

  if (training_set_header_page->GetDataSetType() != testing_set_header_page->GetDataSetType()
      || training_set_header_page->GetDimension() != testing_set_header_page->GetDimension()) {
    return false;
  }

  // Check for parameters
  if (training_set_header_page->GetDataSetType() == DataSetType::GENERATION) {
    auto training_set_generation_header_page =
        reinterpret_cast<const DistributionDataSetGenerationHeaderPage *>(training_set_header_page);
    auto testing_set_generation_header_page =
        reinterpret_cast<const DistributionDataSetGenerationHeaderPage *>(testing_set_header_page);
    DISTRIBUTION_LSH_ENSURE(std::abs(
        training_set_generation_header_page->GetParameter()[0] - testing_set_generation_header_page->GetParameter()[0])
                                < 1E-5, "Parameter not matched");
    DISTRIBUTION_LSH_ENSURE(std::abs(
        training_set_generation_header_page->GetParameter()[1] - testing_set_generation_header_page->GetParameter()[1])
                                < 1E-5, "Parameter not matched");
  }

  return true;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::GenerateDistributionDataset(int size, float ratio) -> bool {
  DistributionDataSetContext ctx;
  if (IsEmpty(&ctx, false)) {
    // Allocate directory page for training set and testing set
    auto training_set_header_page = ctx.training_set_header_page_->template AsMut<DistributionDataSetHeaderPage>();
    auto training_set_directory_page_guard =
        training_set_bpm_->NewPageGuarded(&training_set_header_page->directory_start_page_id_);

    auto testing_set_header_page = ctx.testing_set_header_page_->template AsMut<DistributionDataSetHeaderPage>();
    auto testing_set_directory_page_guard =
        testing_set_bpm_->NewPageGuarded(&testing_set_header_page->directory_start_page_id_);
    if (training_set_header_page->directory_start_page_id_ == INVALID_PAGE_ID
    || testing_set_header_page->directory_start_page_id_ == INVALID_PAGE_ID) {
      LOG_DEBUG("Allocate directory page for training set and testing set failed.");
      return false;
    }

    // Initialize the directory page
    auto training_set_directory_page =
        training_set_directory_page_guard.template AsMut<DistributionDataSetDirectoryPage>();
    training_set_directory_page->Init(directory_page_max_size_);

    auto testing_set_directory_page =
        testing_set_directory_page_guard.template AsMut<DistributionDataSetDirectoryPage>();
    testing_set_directory_page->Init(directory_page_max_size_);
  }

  // Generate training set and testing set data
  auto training_set_data = [&]() {
    switch (this->data_set_type_) {
      case DataSetType::INVALID_DATA_SET_TYPE : throw Exception("Invalid data set type");
      case DataSetType::GENERATION: {
        return ddp_->GenerationDistributionDataset(this->dimension_,
                                                   static_cast<int>(static_cast<float>(size) * ratio),
                                                   this->distribution_type_,
                                                   this->normalization_type_,
                                                   this->params_.get());
      }
      case DataSetType::MNIST: {
        return ddp_->MNISTDistributionDataset(static_cast<int>(static_cast<float>(size) * ratio),
                                              this->directory_name_,
                                              this->normalization_type_);
      }
      case DataSetType::CIFAR10:
      default: throw Exception("Unsupported data set type");
    }
  }();

  auto testing_set_data = [&]() {
    switch (this->data_set_type_) {
      case DataSetType::INVALID_DATA_SET_TYPE : throw Exception("Invalid data set type");
      case DataSetType::GENERATION: {
        return ddp_->GenerationDistributionDataset(this->dimension_,
                                                   static_cast<int>(static_cast<float>(size) * (1 - ratio)),
                                                   this->distribution_type_,
                                                   this->normalization_type_,
                                                   this->params_.get());
      }
      case DataSetType::MNIST: {
        return ddp_->MNISTDistributionDataset(static_cast<int>(static_cast<float>(size) * (1 - ratio)),
                                              this->directory_name_,
                                              this->normalization_type_);
      }
      case DataSetType::CIFAR10:
      default: throw Exception("Unsupported data set type");
    }
  }();

  // Store training set data
  for (int current = 0; current < static_cast<int>(static_cast<float>(size) * ratio); ++current) {
    Store(true, &training_set_data[current * this->dimension_], &ctx);
  }

  // Store testing set data
  for (int current = 0; current < static_cast<int>(static_cast<float>(size) * (1 - ratio)); ++current) {
    Store(false, &testing_set_data[current * this->dimension_], &ctx);
  }

  return true;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::Store(bool is_training_set, ValueType *distribution, DistributionDataSetContext *ctx) -> RID {
  DistributionDataSetContext dataset_ctx;

  // Judge data set type and locate the target directory page
  auto bpm = is_training_set ? training_set_bpm_ : testing_set_bpm_;
  auto header_page_id = is_training_set ? training_set_header_page_id_ : testing_set_header_page_id_;
  auto header_page = is_training_set ?
      [&]() {
        if (ctx != nullptr && ctx->training_set_header_page_.has_value()) {
          return ctx->training_set_header_page_->template AsMut<DistributionDataSetHeaderPage>();
        }
        dataset_ctx.training_set_header_page_ = bpm->FetchPageWrite(header_page_id);
        return dataset_ctx.training_set_header_page_->template AsMut<DistributionDataSetHeaderPage>();
  }() :
      [&]() {
        if (ctx != nullptr && ctx->testing_set_header_page_.has_value()) {
          return ctx->testing_set_header_page_->template AsMut<DistributionDataSetHeaderPage>();
        }
        dataset_ctx.testing_set_header_page_ = bpm->FetchPageWrite(header_page_id);
        return dataset_ctx.testing_set_header_page_->template AsMut<DistributionDataSetHeaderPage>();
  }();

  auto directory_page_guard = bpm->FetchPageWrite(header_page->directory_start_page_id_);
  dataset_ctx.write_set_.emplace_back(std::move(directory_page_guard));

  // Locate the last of the directory page
  auto directory_page = dataset_ctx.write_set_.back().template AsMut<DistributionDataSetDirectoryPage>();
  while (directory_page->next_page_id_ != INVALID_PAGE_ID) {
    directory_page_guard = bpm->FetchPageWrite(directory_page->next_page_id_);
    dataset_ctx.write_set_.pop_front();
    dataset_ctx.write_set_.emplace_back(std::move(directory_page_guard));
    directory_page = dataset_ctx.write_set_.back().template AsMut<DistributionDataSetDirectoryPage>();
  }

  // Update the directory page
  // If it has full, update a new page
  if (directory_page->GetSize() == directory_page->GetMaxSize()) {
    auto directory_page_basic_guard = bpm->NewPageGuarded(&directory_page->next_page_id_);
    if (directory_page->next_page_id_ == INVALID_PAGE_ID) {
      throw Exception("Allocate directory page failed");
    }
    directory_page_guard = directory_page_basic_guard.UpgradeWrite();
    dataset_ctx.write_set_.emplace_back(std::move(directory_page_guard));
    directory_page = dataset_ctx.write_set_.back().AsMut<DistributionDataSetDirectoryPage>();
    directory_page->Init(directory_page_max_size_);
    dataset_ctx.write_set_.pop_front();
  }

  // Store the data with multi-pages
  auto data_page_id = INVALID_PAGE_ID;
  auto data_page_guard = bpm->NewPageGuarded(&data_page_id);
  if (data_page_id == INVALID_PAGE_ID) {
    throw Exception("Allocate data page for data set failed");
  }

  dataset_ctx.write_set_.emplace_front(data_page_guard.UpgradeWrite());
  DataPage *data_page = dataset_ctx.write_set_.front().template AsMut<DataPage>();
  data_page->Init(this->data_page_max_size_);
  
  auto current_data_page_id = data_page_id;
  DataPage *current_data_page = data_page;
  
  auto start_data_page_id = data_page_id;
  std::deque<page_id_t > data_page_id_list;
  data_page_id_list.emplace_front(data_page_id);


  auto slot = -1;
  directory_page->Insert(start_data_page_id, &slot);

  for (int current_size = 0; current_size < this->dimension_; current_size += this->data_page_max_size_) {
    if (dimension_ - current_size > this->data_page_max_size_) {
      memcpy(reinterpret_cast<char *>(current_data_page->array_), reinterpret_cast<char *>(&distribution[current_size]),
             sizeof(ValueType) * this->data_page_max_size_);

      current_data_page->SetSize(this->data_page_max_size_);

      // Allocate new data page
      data_page_id = INVALID_PAGE_ID;
      data_page_guard = bpm->NewPageGuarded(&data_page_id);
      if (data_page_id == INVALID_PAGE_ID) {
        // Roll back
        directory_page->Delete(start_data_page_id);

        //  Delete page allocated before
        while (!data_page_id_list.empty()) {
          // Delete current page
          if (data_page_id_list.front() == current_data_page_id) {
            dataset_ctx.write_set_.pop_front();
          }

          bpm->DeletePage(data_page_id_list.front());
          data_page_id_list.pop_front();
        }

        throw Exception("Allocate data page for data set failed");
      }

      current_data_page->SetNextPageId(data_page_id);
      dataset_ctx.write_set_.emplace_front(data_page_guard.UpgradeWrite());
      data_page = dataset_ctx.write_set_.front().template AsMut<DataPage>();
      data_page->Init(this->data_page_max_size_);
      
      // Update the current page
      current_data_page = data_page;
      current_data_page_id = data_page_id;
    } else {
      memcpy(reinterpret_cast<char *>(current_data_page->array_), reinterpret_cast<char *>(&distribution[current_size]),
             sizeof(ValueType) * (dimension_ - current_size));
      current_data_page->SetSize(dimension_ - current_size);
    }
  }

  return {dataset_ctx.write_set_.back().PageId(), static_cast<uint32_t>(slot)};
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::GetSize(bool is_training_set) -> int {
  if (IsEmpty()) {
    return 0;
  }

  DistributionDataSetContext context;
  auto distribution_dataset_size = 0;

  auto bpm = is_training_set ? training_set_bpm_ : testing_set_bpm_;
  auto header_page_id = is_training_set ? training_set_header_page_id_ : testing_set_header_page_id_;
  auto header_page_guard = bpm->FetchPageRead(header_page_id);
  auto header_page = header_page_guard.template As<DistributionDataSetHeaderPage>();

  auto directory_page_guard = bpm->FetchPageRead(header_page->directory_start_page_id_);
  auto directory_page = directory_page_guard.template As<DistributionDataSetDirectoryPage>();
  context.read_set_.emplace_back(std::move(directory_page_guard));

  while (directory_page->GetNextPageId() != INVALID_PAGE_ID) {
    distribution_dataset_size += directory_page->GetSize();
    directory_page_guard = bpm->FetchPageRead(directory_page->GetNextPageId());
    context.read_set_.pop_front();
    context.read_set_.emplace_back(std::move(directory_page_guard));
    directory_page = context.read_set_.back().template As<DistributionDataSetDirectoryPage>();
  }

  distribution_dataset_size += directory_page->GetSize();

  return distribution_dataset_size;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::GetDistributionData(bool is_training_set,
                                                            distribution_lsh::page_id_t directory_page_id,
                                                            int index) -> std::shared_ptr<ValueType[]> {
  std::shared_ptr<ValueType[]> distribution_data(new ValueType[this->dimension_]);

  auto bpm = is_training_set ? training_set_bpm_ : testing_set_bpm_;
  auto directory_page_guard = bpm->FetchPageRead(directory_page_id);
  auto data_set_page = directory_page_guard.template As<DistributionDataSetPage>();
  if (!data_set_page->IsDirectoryPage()) {
    throw Exception("The input page is not a directory page");
  }

  auto directory_page = reinterpret_cast<const DistributionDataSetDirectoryPage *>(data_set_page);
  auto data_page_id = directory_page->IndexAt(index);
  if (data_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("Invalid index page");
    return nullptr;
  }
  auto data_page_guard = bpm->FetchPageRead(data_page_id);
  const DataPage *data_page = data_page_guard.template As<DataPage>();

  auto current_size = 0;
  while (current_size < this->dimension_) {
    if (this->dimension_ - current_size > data_page_max_size_) {
      memcpy(distribution_data.get() + current_size, data_page->array_, data_page_max_size_ * sizeof(ValueType));
      current_size += data_page_max_size_;

      if (data_page->next_page_id_ == INVALID_PAGE_ID) {
        throw Exception("The data page is not a linked list");
      }

      data_page_guard.Drop();
      data_page_guard = bpm->FetchPageRead(data_page->next_page_id_);
      data_page = data_page_guard.template As<DataPage>();
    } else {
      memcpy(distribution_data.get() + current_size,
             data_page->array_,
             (this->dimension_ - current_size) * sizeof(ValueType));
      current_size = this->dimension_;
    }
  }

  return distribution_data;
}

// TODO return different exception type for different error cases
DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::GetDistributionData(bool is_training_set, int index, page_id_t *directory_page_id, int *slot) -> std::shared_ptr<ValueType[]> {
  if (IsEmpty()) {
    LOG_DEBUG("The file is empty");
    return nullptr;
  }

  DistributionDataSetContext directory_ctx;
  std::shared_ptr<ValueType[]> distribution_data(new ValueType[this->dimension_]);

  auto bpm = is_training_set ? training_set_bpm_ : testing_set_bpm_;
  auto header_page_id = is_training_set ? training_set_header_page_id_ : testing_set_header_page_id_;
  auto header_page_guard = bpm->FetchPageRead(header_page_id);
  auto header_page = header_page_guard.template As<DistributionDataSetHeaderPage>();
  auto directory_page_guard = bpm->FetchPageRead(header_page->directory_start_page_id_);
  auto directory_page = directory_page_guard.template As<DistributionDataSetDirectoryPage>();
  directory_ctx.read_set_.emplace_back(std::move(directory_page_guard));

  // Locate the target directory page
  auto directory_current_size = 0;
  while (directory_current_size <= index) {
    if (directory_page->GetMaxSize() > index - directory_current_size) {
      *directory_page_id = directory_ctx.read_set_.back().PageId();
      *slot = index - directory_current_size;
      break;
    }

    if (directory_page->GetNextPageId() == INVALID_PAGE_ID) {
      throw Exception("Invalid index");
    }

    directory_current_size += directory_page->GetMaxSize();
    directory_page_guard = bpm->FetchPageRead(directory_page->GetNextPageId());
    directory_ctx.read_set_.pop_front();
    directory_ctx.read_set_.emplace_back(std::move(directory_page_guard));
    directory_page = directory_ctx.read_set_.back().template As<DistributionDataSetDirectoryPage>();
  }

  // Locate the data
  auto data_page_id = directory_page->IndexAt(index - directory_current_size);
  if (data_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("Invalid index page");
    return nullptr;
  }
  auto data_page_guard = bpm->FetchPageRead(data_page_id);
  const DataPage *data_page = data_page_guard.template As<DataPage>();

  auto current_size = 0;
  while (current_size < this->dimension_) {
    if (this->dimension_ - current_size > data_page_max_size_) {
      memcpy(distribution_data.get() + current_size, data_page->array_, data_page_max_size_ * sizeof (ValueType));
      current_size += data_page_max_size_;

      if (data_page->next_page_id_ == INVALID_PAGE_ID) {
        throw Exception("The data page is not a linked list");
      }

      data_page_guard.Drop();
      data_page_guard = bpm->FetchPageRead(data_page->next_page_id_);
      data_page = data_page_guard.template As<DataPage>();
    } else {
      memcpy(distribution_data.get() + current_size,
             data_page->array_,
             (this->dimension_ - current_size) * sizeof(ValueType));
      current_size = this->dimension_;
    }
  }

  return distribution_data;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::Delete(bool is_training_set, page_id_t directory_page_id, int index) -> bool {
  DistributionDataSetContext directory_ctx;
  DistributionDataSetContext data_ctx;
  auto bpm = is_training_set ? training_set_bpm_ : testing_set_bpm_;
  auto header_page_id = is_training_set ? training_set_header_page_id_ : testing_set_header_page_id_;

  if (directory_page_id == header_page_id) {
    LOG_DEBUG("Cannot delete in header page");
    return false;
  }

  auto header_page_guard = bpm->FetchPageWrite(header_page_id);
  auto header_page = header_page_guard.template AsMut<DistributionDataSetHeaderPage>();
  auto directory_page_guard = bpm->FetchPageWrite(directory_page_id);
  auto data_set_page = directory_page_guard.template AsMut<DistributionDataSetPage>();
  if (!data_set_page->IsDirectoryPage()) {
    LOG_DEBUG("The input page is not a directory page");
    return false;
  }

  auto directory_page = reinterpret_cast<DistributionDataSetDirectoryPage *>(data_set_page);

  // Delete data page located by the index
  auto data_page_id = directory_page->IndexAt(index);
  auto data_page_guard = bpm->FetchPageWrite(data_page_id);
  data_ctx.write_set_.emplace_back(std::move(data_page_guard));
  auto data_page = data_ctx.write_set_.back().template AsMut<DataPage>();

  while (data_page->next_page_id_!= INVALID_PAGE_ID) {
    auto next_page_id = data_page->next_page_id_;
    data_ctx.write_set_.pop_front();
    bpm->DeletePage(data_page_id);
    data_ctx.write_set_.emplace_back(bpm->FetchPageWrite(next_page_id));
    data_page_id = next_page_id;
    data_page = data_ctx.write_set_.back().template AsMut<DataPage>();
  }

  data_ctx.write_set_.pop_front();
  bpm->DeletePage(data_page_id);

  // Delete the entry in the directory page
  if (!directory_page->Delete(index)) {
    LOG_DEBUG("Delete failed");
    return false;
  }

  // Compact the directory page if size is zero
  if (directory_page->GetSize() == 0) {
    // Locate the page before this page and maintain the list structure
    if (header_page->directory_start_page_id_ == directory_page_id) {
      header_page->directory_start_page_id_ = data_set_page->next_page_id_;
      directory_page_guard.Drop();
      bpm->DeletePage(directory_page_id);
    } else {
      auto before_directory_page_guard = bpm->FetchPageWrite(header_page->directory_start_page_id_);
      auto before_directory_page = before_directory_page_guard.template AsMut<DistributionDataSetDirectoryPage>();
      directory_ctx.write_set_.emplace_back(std::move(before_directory_page_guard));
      while (before_directory_page->GetNextPageId() != directory_page_id) {
        before_directory_page_guard = bpm->FetchPageWrite(before_directory_page->GetNextPageId());
        directory_ctx.write_set_.pop_front();
        directory_ctx.write_set_.emplace_back(std::move(before_directory_page_guard));
        before_directory_page = directory_ctx.write_set_.back().template AsMut<DistributionDataSetDirectoryPage>();
      }

      before_directory_page->SetNextPageId(directory_page->GetNextPageId());
      directory_page_guard.Drop();
      bpm->DeletePage(directory_page_id);
    }
  }

  return true;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MANAGER_TYPE::ToString() -> std::string {
  auto basic_info = fmt::format(
      "Distribution Dataset Manager Information:(training set file id: {}, testing set file id: {})\n"
      "├─────manager name: {}.\n"
      "├─────data set type: {}.\n"
      "├─────normalization set type: {}.\n"
      "├─────distribution set type: {}.\n"
      "├─────training set header page id: {}.\n"
      "├─────testing set header page id: {}.\n"
      "├─────dimension: {}.\n"
      "├─────directory page max size {}.\n"
      "├─────data page max size {}.\n"
      "├─────data set scale: training set scale {}, testing set scale {}.\n",
      fmt::format(fg(fmt::color::deep_pink) | fmt::emphasis::underline, std::to_string(training_set_file_id_)),
      fmt::format(fg(fmt::color::deep_pink) | fmt::emphasis::underline, std::to_string(testing_set_file_id_)),
      fmt::format(fg(fmt::color::light_pink) | fmt::emphasis::bold, manager_name_),
      fmt::format(fg(fmt::color::light_salmon) | fmt::emphasis::bold, DataSetTypeToString(data_set_type_)),
      fmt::format(fg(fmt::color::light_yellow) | fmt::emphasis::bold, NormalizationTypeToString(normalization_type_)),
      fmt::format(fg(fmt::color::peach_puff) | fmt::emphasis::bold, DistributionTypeToString(distribution_type_)),
      fmt::format(fg(fmt::color::medium_purple) | fmt::emphasis::bold, "{}", training_set_header_page_id_),
      fmt::format(fg(fmt::color::medium_purple) | fmt::emphasis::bold, "{}", testing_set_header_page_id_),
      fmt::format(fg(fmt::color::medium_violet_red) | fmt::emphasis::bold, "{}", dimension_),
      fmt::format(fg(fmt::color::misty_rose) | fmt::emphasis::bold, "{}", directory_page_max_size_),
      fmt::format(fg(fmt::color::misty_rose) | fmt::emphasis::bold, "{}", data_page_max_size_),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, "{}", GetSize(true)),
      fmt::format(fg(fmt::color::orange_red) | fmt::emphasis::bold, "{}", GetSize(false))
      );

  if (data_set_type_ == DataSetType::GENERATION) {
    auto aux_info = fmt::format(
        "├─────parameter: {}, {}\n",
        fmt::format(fg(fmt::color::orange) | fmt::emphasis::bold, "{:.3f}", params_[0]),
        fmt::format(fg(fmt::color::orange) | fmt::emphasis::bold, "{:.3f}", params_[1])
        );

    return basic_info + aux_info;
  }

  return basic_info;
}

template class DistributionDataSetManager<float>;

//template class DistributionDataSetManager<double>;
} // namespace distribution_lsh