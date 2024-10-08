add_library(
        distribution_lsh_storage_disk
        OBJECT
        disk_manager.cpp
        disk_manager_memory.cpp
        disk_scheduler.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_storage_disk>
        PARENT_SCOPE
)