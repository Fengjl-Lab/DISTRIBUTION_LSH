add_library(
        distribution_lsh_storage_page
        OBJECT
        b_plus_tree_internal_page.cpp
        b_plus_tree_leaf_page.cpp
        b_plus_tree_page.cpp
        page_guard.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_storage_page>
        PARENT_SCOPE
)