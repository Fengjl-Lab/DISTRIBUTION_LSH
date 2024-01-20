add_library(
        distribution_lsh_type
        OBJECT
        type.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_type>
        PARENT_SCOPE
)