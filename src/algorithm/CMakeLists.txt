add_library(
        distribution_lsh_algorithm
        OBJECT
        distribution_lsh.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_algorithm>
        PARENT_SCOPE
)