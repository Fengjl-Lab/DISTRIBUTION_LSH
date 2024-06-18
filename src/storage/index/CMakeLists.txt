add_library(
        distribution_lsh_storage_index
        OBJECT
        b_plus_tree.cpp
        random_line_manager.cpp
        relation_manager.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_storage_index>
        PARENT_SCOPE
)