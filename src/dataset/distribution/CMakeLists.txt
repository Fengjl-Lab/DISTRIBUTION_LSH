add_library(
        distribution_lsh_dataset_distribution
        OBJECT
        gen_distribution.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_dataset_distribution>
        PARENT_SCOPE
)