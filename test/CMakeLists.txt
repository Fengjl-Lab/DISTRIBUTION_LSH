cmake_minimum_required(VERSION 3.10)
include_directories(${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})

add_executable(DistributionTests DistributionTests.cpp)

target_link_libraries(DistributionTests QALSH_static)
target_link_libraries(DistributionTests gtest gtest_main)
target_link_libraries(DistributionTests fmt)

include(GoogleTest)
file(GLOB_RECURSE QALSH_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

# ##########################################
# "make check-test"
# ##########################################
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)


# ##########################################
# "make XYZ_test"
# ##########################################
foreach (qalsh_test_source ${QALSH_TEST_SOURCES})
    # Create a human readable name
    get_filename_component(qalsh_test_filename ${qalsh_test_source} NAME)
    string(REPLACE ".cpp" "" qalsh_test_name ${qalsh_test_source})

    # Add the test target separately and as part of "make check-test"
    add_executable(${qalsh_test_name}  ${qalsh_test_source}) #"${PROJECT_SOURCE_DIR}/tools/backtrace.cpp"
    # add_backward(${qalsh_test_name})
    add_dependencies(build-tests ${qalsh_test_name})
    add_dependencies(check-tests ${qalsh_test_name})

    gtest_discover_tests(${qalsh_test_name}
            EXTRA_ARGS
            --gtest_color=auto
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${qalsh_test_name}.xml
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            PROPERTIES
            TIMEOUT 120
    )

    target_link_libraries(${qalsh_test_name} QALSH_static gtest gmock_main)

    # Set test target properties and dependencies.
    set_target_properties(${qalsh_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${qalsh_test_name}
    )
endforeach ()