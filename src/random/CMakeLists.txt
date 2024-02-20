add_library(
        distribution_lsh_random
        OBJECT
        random_line_generator.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_random>
        PARENT_SCOPE
)