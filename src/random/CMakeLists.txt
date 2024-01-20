add_library(
        distribution_lsh_random
        OBJECT
        one_dimension_random_number.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_random>
        PARENT_SCOPE
)