# judge the system
if (UNIX)
    if(APPLE)
        add_definitions(-D__APPLE__)
    elseif (CMAKE_SYSTEM_NAME MATCHES "Linux")
        add_definitions(-D__LINUX__)
    else ()
        message(FATAL_ERROR "Unsupported operating system")
    endif ()
else()
    message(FATAL_ERROR "Unsupported operating system")
endif ()

add_subdirectory(algorithm)
add_subdirectory(buffer)
add_subdirectory(dataset)
add_subdirectory(random)
add_subdirectory(storage)
add_subdirectory(type)

add_library(distribution_lsh ${ALL_OBJECT_FILES})

set(DISTRIBUTION_LSH_LIBS
        distribution_lsh_algorithm
        distribution_lsh_buffer
        distribution_lsh_dataset_distribution
        distribution_lsh_random
        distribution_lsh_storage_disk
        distribution_lsh_storage_page
        distribution_lsh_storage_index
        distribution_lsh_type
)

set(DISTRIBUTION_LSH_THIRD_PARTY_LIBS
        fmt
)

target_link_libraries(distribution_lsh ${DISTRIBUTION_LSH_LIBS} ${DISTRIBUTION_LSH_THIRD_PARTY_LIBS})

target_include_directories(
        distribution_lsh PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

#file(GLOB_RECURSE DISTRIBUTION_LSH_sources ${PROJECT_SOURCE_DIR}/src/*/*.cpp)
#file(GLOB_RECURSE DISTRIBUTION_LSH_headers ${PROJECT_SOURCE_DIR}/src/include/*.h)
#add_library(DISTRIBUTION_LSH_static STATIC ${DISTRIBUTION_LSH_sources} ${DISTRIBUTION_LSH_headers})
#
##target_include_directories(DISTRIBUTION_LSH_static PRIVATE
##        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
##        $<INSTALL_INTERFACE:include>
##)
#
#target_include_directories(DISTRIBUTION_LSH_static PUBLIC
#        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
#        $<INSTALL_INTERFACE:include>
#)
#

#target_link_libraries(DISTRIBUTION_LSH_static fmt)
#
add_executable(DISTRIBUTION_LSH_main main.cpp)
target_link_libraries(DISTRIBUTION_LSH_main PUBLIC distribution_lsh fmt)