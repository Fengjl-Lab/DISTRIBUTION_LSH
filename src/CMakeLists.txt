file(GLOB_RECURSE QALSH_sources ${PROJECT_SOURCE_DIR}/src/*/*.cpp)
file(GLOB_RECURSE QALSH_headers ${PROJECT_SOURCE_DIR}/src/include/*.h)
add_library(QALSH_static STATIC ${QALSH_sources} ${QALSH_headers})

target_include_directories(QALSH_static PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_include_directories(QALSH_static PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

add_executable(QALSH main.cpp)
target_link_libraries(QALSH PUBLIC QALSH_shared)