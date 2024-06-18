add_library(
        distribution_lsh_file
        OBJECT
        monitor.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_file>
        PARENT_SCOPE
)