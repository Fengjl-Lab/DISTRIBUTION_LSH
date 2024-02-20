add_library(
        distribution_lsh_dataset_distribution
        OBJECT
        distribution_dataset_manager.cpp
        distribution_dataset_generator.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_dataset_distribution>
        PARENT_SCOPE
)