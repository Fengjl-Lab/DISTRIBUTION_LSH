add_library(
        distribution_lsh_dataset_distribution
        OBJECT
        distribution_dataset_manager.cpp
        distribution_dataset_monitor.cpp
        distribution_dataset_processor.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_dataset_distribution>
        PARENT_SCOPE
)

add_executable(distribution_dataset distribution_dataset.cpp)