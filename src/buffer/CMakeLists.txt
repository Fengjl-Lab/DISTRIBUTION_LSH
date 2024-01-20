add_library(
        distribution_lsh_buffer
        OBJECT
        buffer_pool_manager.cpp
        lru_k_replacer.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_buffer>
        PARENT_SCOPE
)