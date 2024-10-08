//===----------------------------------------------------
//                    DISTRIBUTION_LSH
// Created by chenjunhao on 2024/3/13.
// src/dataset/distribution/distribution_dataset_monitor.cpp
//
//===-----------------------------------------------------

#include <string_view>

#include <fmt/format.h>

#include <dataset/distribution/distribution_dataset_manager.h>
#include <dataset/distribution/distribution_dataset_monitor.h>

namespace distribution_lsh {

using namespace indicators;

DISTRIBUTION_DATASET_TEMPLATE
DISTRIBUTION_DATASET_MONITOR_TYPE::DistributionDataSetMonitor(
    std::string raw_data_directory_name,
    std::string training_set_directory_name,
    std::string testing_set_directory_name,
    std::string relation_directory_name,
    int k,
    int pool_size,
    int directory_page_max_size,
    int data_page_max_size)
    : raw_data_directory_name_(std::move(raw_data_directory_name)),
      training_set_directory_name_(std::move(training_set_directory_name)),
      testing_set_directory_name_(std::move(testing_set_directory_name)),
      relation_directory_name_(std::move(relation_directory_name)),
      k_(k),
      pool_size_(pool_size),
      directory_page_max_size_(directory_page_max_size),
      data_page_max_size_(data_page_max_size) {
  // Indicators progress bar presentation

  // Progress bar for reading
  ProgressBar training_process {
      option::BarWidth{50},
      option::ForegroundColor{Color::cyan},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::PrefixText{"Training Set Files Reading"},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}
  };

  ProgressBar testing_process {
      option::BarWidth{50},
      option::ForegroundColor{Color::magenta},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::PrefixText{"Testing Set Files Reading"},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}
  };

  ProgressBar relation_process {
      option::BarWidth{50},
      option::ForegroundColor{Color::grey},
      option::ShowElapsedTime{true},
      option::ShowRemainingTime{true},
      option::PrefixText{"Relation Files Reading"},
      indicators::option::FontStyles{
          std::vector<indicators::FontStyle>{indicators::FontStyle::bold}}
  };


  // Read from training set directory and prepare bpm
  if (!std::filesystem::exists(std::filesystem::path{training_set_directory_name_})) {
    std::filesystem::create_directories(training_set_directory_name_);
  }

  auto training_set_directory_size = std::distance(std::filesystem::recursive_directory_iterator{std::filesystem::path{training_set_directory_name_}}, std::filesystem::recursive_directory_iterator{}) / 2;
  std::atomic<int> training_set_index{0};
  training_process.set_option(option::MaxProgress{training_set_directory_size});

  std::ranges::for_each(
      std::filesystem::directory_iterator{std::filesystem::path{training_set_directory_name_}},
      [&](const auto &entry) {
        if (std::string_view{FileFullExTension(entry.path().filename())} == DATASET_FILE_SUFFIX) {
          auto disk_manager = std::make_shared<DiskManager>(entry.path().string());
          auto next_page_id = GetNextPageId(disk_manager.get(), entry.path().string());
          auto bpm = std::make_shared<BufferPoolManager>(pool_size_, disk_manager, k_, nullptr, next_page_id);
          training_set_bpms_.insert({static_cast<file_id_t >(std::stoull(entry.path().filename())), bpm});

          training_process.tick();
          training_set_index++;
          training_process.set_option(option::PostfixText{std::to_string(training_set_index) + "/" + std::to_string(training_set_directory_size) + " completed."});
        }
      }
  );

  if (training_set_index == training_set_directory_size && training_set_index != 0 ) {
    training_process.mark_as_completed();
  }

  // Read from testing set directory and prepare bpm
  if (!std::filesystem::exists(std::filesystem::path{testing_set_directory_name_})) {
    std::filesystem::create_directories(testing_set_directory_name_);
  }
  auto testing_set_directory_size = std::distance(std::filesystem::recursive_directory_iterator{std::filesystem::path{testing_set_directory_name_}}, std::filesystem::recursive_directory_iterator{}) / 2;
  std::atomic<int> testing_set_index{0};

  std::ranges::for_each(
      std::filesystem::directory_iterator{std::filesystem::path{testing_set_directory_name_}},
      [&](const auto &entry) {
        if (std::string_view{FileFullExTension(entry.path().filename())} == DATASET_FILE_SUFFIX) {
          auto disk_manager = std::make_shared<DiskManager>(entry.path().string());
          auto next_page_id = GetNextPageId(disk_manager.get(), entry.path().string());
          auto bpm = std::make_shared<BufferPoolManager>(pool_size_, disk_manager, k_, nullptr, next_page_id);
          testing_set_bpms_.insert({static_cast<file_id_t >(std::stoull(entry.path().filename())), bpm});

          testing_process.tick();
          testing_set_index++;
          testing_process.set_option(option::PostfixText{std::to_string(testing_set_index) + "/" + std::to_string(testing_set_directory_size) + " completed."});
        }
      }
  );

  if (testing_set_index == testing_set_directory_size && testing_set_index != 0 ) {
    testing_process.mark_as_completed();
  }

  // Read from relation file directory and prepare bpm
  if (!std::filesystem::exists(std::filesystem::path{relation_directory_name_})) {
    std::filesystem::create_directories(relation_directory_name_);
  }
  auto relation_directory_size = std::distance(std::filesystem::recursive_directory_iterator{std::filesystem::path{relation_directory_name_}}, std::filesystem::recursive_directory_iterator{}) / 2;
  std::atomic<int> relation_index{0};
  relation_process.set_option(option::MaxProgress{relation_directory_size});

  std::ranges::for_each(
      std::filesystem::directory_iterator{std::filesystem::path{relation_directory_name_}},
      [&](const auto &entry) {
        if (std::string_view{FileFullExTension(entry.path().filename())} == RELATION_FILE_SUFFIX) {
          auto disk_manager = std::make_shared<DiskManager>(entry.path().string());
          auto next_page_id = GetNextPageId(disk_manager.get(), entry.path().string());
          auto bpm = std::make_shared<BufferPoolManager>(pool_size_, disk_manager, k_, nullptr, next_page_id);
          auto relation_manager = std::make_shared<RelationManager<TrainingSetToTestingSetUnion>>("relation manager",
                                                                                                  relation_directory_name_,
                                                                                                  bpm,
                                                                                                  RelationFileType::TRAINING_SET_TO_TESTING_SET,
                                                                                                  INVALID_FILE_ID,
                                                                                                  HEADER_PAGE_ID);
          if (relation_manager->GetType() != RelationFileType::TRAINING_SET_TO_TESTING_SET) {
            return;
          }

          relation_managers_.insert({static_cast<file_id_t >(std::stoull(entry.path().filename())), relation_manager});

          relation_process.tick();
          relation_index++;
          relation_process.set_option(option::PostfixText{std::to_string(relation_index) + "/" + std::to_string(relation_directory_size) + + " completed."});
        }
      }
  );

  if (relation_index == relation_directory_size && relation_index != 0 ) {
    relation_process.mark_as_completed();
  }

  // Show cursor
  // indicators::show_console_cursor(true);

  // Constructing dataset manager according to the relation file
  for (const auto &[relation_file_id, relation_manager] : relation_managers_) {
    while (true) {
      try {
        auto data_page_id = INVALID_PAGE_ID;
        auto slot = NULL_SLOT_END;
        auto relation_record = relation_manager->Get(current_index_, &data_page_id, &slot);

        if (relation_record.next_null_slot_ == NULL_SLOT_END) {
          break;
        }

        if (relation_record.next_null_slot_ == INVALID_SLOT_VALUE) {
          current_index_++;
          continue;
        }


        // Test if training set exist
        if (training_set_bpms_.find(relation_record.map_.training_set_file_id_) == training_set_bpms_.end()
            || testing_set_bpms_.find(relation_record.map_.testing_set_file_id_) == testing_set_bpms_.end()) {
          fmt::println("Data set file not found(training set file id: {}, testing set file id {})",
                       relation_record.map_.training_set_file_id_,
                       relation_record.map_.testing_set_file_id_);

          // Delete the corresponding relation slot for consistent
          relation_manager->Delete(data_page_id, slot);
          std::filesystem::remove(std::filesystem::path{
              training_set_directory_name_ + "/" + std::to_string(relation_record.map_.training_set_file_id_)
                  + DATASET_FILE_SUFFIX}) ? fmt::print("Training set file  {} has successfully deleted\n",
                                                       relation_record.map_.training_set_file_id_) : fmt::print(
              "Training set file not found\n");
          std::filesystem::remove(std::filesystem::path{
              testing_set_directory_name_ + "/" + std::to_string(relation_record.map_.testing_set_file_id_)
                  + DATASET_FILE_SUFFIX}) ? fmt::print("Testing set file  {} has successfully deleted\n",
                                                       relation_record.map_.testing_set_file_id_) : fmt::print(
              "Testing set file not found\n");

          current_index_++;
          continue;
        }

        // Constructing dataset manager
        auto ddp = std::make_unique<DistributionDatasetProcessor<ValueType>>();
        auto distribution_data_set_manager =
            std::make_unique<DistributionDataSetManager<ValueType>>(std::to_string(current_index_),
                                                                    DataSetType::INVALID_DATA_SET_TYPE,
                                                                    DistributionType::INVALID_DISTRIBUTION_TYPE,
                                                                    NormalizationType::INVALID_NORMALIZATION_TYPE,
                                                                    training_set_bpms_[relation_record.map_.training_set_file_id_],
                                                                    testing_set_bpms_[relation_record.map_.testing_set_file_id_],
                                                                    std::move(ddp),
                                                                    HEADER_PAGE_ID,
                                                                    HEADER_PAGE_ID,
                                                                    INVALID_DIMENSION,
                                                                    nullptr,
                                                                    raw_data_directory_name_,
                                                                    relation_record.map_.training_set_file_id_,
                                                                    relation_record.map_.testing_set_file_id_,
                                                                    directory_page_max_size_,
                                                                    data_page_max_size_);
        dataset_managers_.insert({current_index_, std::move(distribution_data_set_manager)});
        current_index_++;
      } catch (Exception &exception) {
        current_index_++;
        break;
      }

    }
  }

  // Print the information from managers
  fmt::println("DATA SET INFORMATION");
  for (const auto &[manager_index, manager] : dataset_managers_) {
    fmt::print("{}", manager->ToString());
  }
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MONITOR_TYPE::GenerateDataSet(distribution_lsh::DataSetType
                                                        data_set_type,
                                                        distribution_lsh::DistributionType distribution_type,
                                                        distribution_lsh::NormalizationType
                                                        normalization_type,
                                                        int dimension,
                                                        float param1, float param2,
                                                        int size, float ratio, file_id_t *training_set_file_id_ptr) -> int {

  auto training_set_file_id =
      GenerateFileIdentification(training_set_directory_name_, FileType::DISTRIBUTION_DATASET_FILE);
  auto training_set_disk_manager = std::make_shared<DiskManager>(
      training_set_directory_name_ + "/" + std::to_string(training_set_file_id) + DATASET_FILE_SUFFIX);
  auto training_set_next_page_id = GetNextPageId(training_set_disk_manager.get(),
                                                 training_set_directory_name_ + "/" + std::to_string(training_set_file_id)
                                                 + DATASET_FILE_SUFFIX);
  auto training_set_bpm = std::make_shared<BufferPoolManager>(pool_size_,
                                                              training_set_disk_manager,
                                                              k_,
                                                              nullptr,
                                                              training_set_next_page_id);
  training_set_bpms_.insert({training_set_file_id, training_set_bpm});

  auto testing_set_file_id =
      GenerateFileIdentification(testing_set_directory_name_, FileType::DISTRIBUTION_DATASET_FILE);
  auto testing_set_manager = std::make_shared<DiskManager>(
      testing_set_directory_name_ + "/" + std::to_string(testing_set_file_id) + DATASET_FILE_SUFFIX);
  auto testing_set_next_page_id = GetNextPageId(testing_set_manager.get(),
                                                testing_set_directory_name_ + "/" + std::to_string(testing_set_file_id)
                                                + DATASET_FILE_SUFFIX);
  auto testing_set_bpm =
      std::make_shared<BufferPoolManager>(pool_size_, testing_set_manager, k_, nullptr, testing_set_next_page_id);
  testing_set_bpms_.insert({testing_set_file_id, testing_set_bpm});

  auto ddp = std::make_unique<DistributionDatasetProcessor<ValueType>>();

  std::shared_ptr<float[2]> params(new float[2]);
  params[0] = param1;
  params[1] = param2;

  auto distribution_data_set_manager =
      std::make_unique<DistributionDataSetManager<ValueType>>(std::to_string(current_index_),
                                                              data_set_type,
                                                              distribution_type,
                                                              normalization_type,
                                                              training_set_bpms_[training_set_file_id],
                                                              testing_set_bpms_[testing_set_file_id],
                                                              std::move(ddp),
                                                              INVALID_PAGE_ID,
                                                              INVALID_PAGE_ID,
                                                              dimension,
                                                              params,
                                                              raw_data_directory_name_,
                                                              training_set_file_id,
                                                              testing_set_file_id,
                                                              directory_page_max_size_,
                                                              data_page_max_size_);

  distribution_data_set_manager.get()->GenerateDistributionDataset(size, ratio);
  dataset_managers_.insert({current_index_, std::move(distribution_data_set_manager)});

  // Update data in relation manager
  if (relation_managers_.empty()) {
    // If empty, create a new relation file
    auto relation_file_id = GenerateFileIdentification(relation_directory_name_, FileType::RELATION_FILE);
    auto relation_disk_manager = std::make_shared<DiskManager>(
        relation_directory_name_ + "/" + std::to_string(relation_file_id) + RELATION_FILE_SUFFIX);
    auto relation_next_page_id = GetNextPageId(relation_disk_manager.get(), relation_directory_name_ + "/" + std::to_string(relation_file_id) + RELATION_FILE_SUFFIX);
    auto relation_bpm = std::make_shared<BufferPoolManager>(pool_size_, relation_disk_manager, k_, nullptr, relation_next_page_id);
    auto relation_manager = std::make_shared<RelationManager<TrainingSetToTestingSetUnion>>("relation manager-" + std::to_string(relation_file_id) ,
                                                                           relation_directory_name_,
                                                                           relation_bpm,
                                                                           RelationFileType::TRAINING_SET_TO_TESTING_SET,
                                                                           relation_file_id,
                                                                           INVALID_PAGE_ID);
    relation_managers_.insert({relation_file_id, relation_manager});
  }

  auto index = 0;
  relation_managers_.begin()->second->Insert({.map_{training_set_file_id, testing_set_file_id}}, &index);

  // Set training set file id
  if (training_set_file_id_ptr != nullptr) {
    *training_set_file_id_ptr = training_set_file_id;
  }

  return current_index_++;
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MONITOR_TYPE::GetDataSetIndex(DataSetType data_set_type,
                                                        DistributionType distribution_type,
                                                        NormalizationType normalization_type,
                                                        int dimension,
                                                        float param1,
                                                        float param2,
                                                        int size,
                                                        float ratio,
                                                        file_id_t *training_set_file_id_ptr) -> int {
  std::unique_lock<std::mutex> lock(latch_);
  for (const auto &[index, manager] : dataset_managers_)  {
    if (manager->GetDataSetType() == data_set_type
    && manager->GetNormalizationType() == normalization_type
    && manager->GetDimension() == dimension
    && abs(manager->GetSize(true) + manager->GetSize(false) - size) < 10
    && std::abs(static_cast<float>(manager->GetSize(true)) / static_cast<float>(size) -  ratio) < 1E-5) {
      // TODO if there has more complex case
      if (manager->GetDataSetType() == DataSetType::GENERATION) {
        if (manager->GetNormalizationType() != normalization_type
        || std::abs(manager.get()->params_[0] - param1) > 1E-5
        || std::abs(manager.get()->params_[1] - param2) > 1E-5) {
          continue;
        }
      }

      // Set training set file id
      if (training_set_file_id_ptr != nullptr) {
        *training_set_file_id_ptr = manager->GetTrainingSetFileID();
      }

      return index;
    }
  }

  // Create a new dataset if not found
  return GenerateDataSet(data_set_type, distribution_type, normalization_type, dimension, param1, param2, size, ratio, training_set_file_id_ptr);
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MONITOR_TYPE::GetDistributionData(int manager_index,bool is_training_set , int index, page_id_t *directory_page_id, int *slot) -> std::shared_ptr<ValueType[]> {
  std::unique_lock<std::mutex> lock(latch_);
  if (dataset_managers_.find(manager_index) == dataset_managers_.end()) {
    return nullptr;
  }
  
  try {
    std::shared_ptr<ValueType[]> data = dataset_managers_[manager_index].get()->GetDistributionData(is_training_set, index, directory_page_id, slot);
    return data;
  } catch (Exception &exception) {
    *directory_page_id = INVALID_PAGE_ID;
    *slot = NULL_SLOT_END;
    return nullptr;
  }
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MONITOR_TYPE::GetDistributionData(int manager_index, bool is_training_set, page_id_t directory_page_id, int index) -> std::shared_ptr<ValueType[]> {
  std::unique_lock<std::mutex> lock(latch_);
  if (dataset_managers_.find(manager_index) == dataset_managers_.end()) {
    return nullptr;
  }

  return dataset_managers_[manager_index].get()->GetDistributionData(is_training_set, directory_page_id, index);
}

DISTRIBUTION_DATASET_TEMPLATE
auto DISTRIBUTION_DATASET_MONITOR_TYPE::DeleteDistributionData(int manager_index,
                                                               bool is_training_set,
                                                               distribution_lsh::page_id_t directory_page_id,
                                                               int index) -> std::shared_ptr<ValueType[]> {
  std::unique_lock<std::mutex> lock(latch_);
  if (dataset_managers_.find(manager_index) == dataset_managers_.end()) {
    return nullptr;
  }

  std::shared_ptr<ValueType[]> distribution_data = dataset_managers_[manager_index].get()->GetDistributionData(is_training_set, directory_page_id, index);
  dataset_managers_[manager_index].get()->Delete(is_training_set, directory_page_id, index);

  return distribution_data;
}

DISTRIBUTION_DATASET_TEMPLATE
void DISTRIBUTION_DATASET_MONITOR_TYPE::List() {
  fmt::println("DATA SET INFORMATION");
  for (const auto &[index, data_set_manager] : dataset_managers_) {
   fmt::print("{}", data_set_manager->ToString());
  }
}

template
class DistributionDataSetMonitor<float>;
} // namespace distribution_lsh