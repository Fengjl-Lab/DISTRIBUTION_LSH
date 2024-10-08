cmake_minimum_required(VERSION 3.10)

include(GoogleTest)
file(GLOB_RECURSE DISTRIBUTION_LSH_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")

# ##########################################
# "make check-test"
# ##########################################
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)


# ##########################################
# "make XYZ_test"
# ##########################################
foreach (distribution_lsh_test_source ${DISTRIBUTION_LSH_TEST_SOURCES})
    # Create a human readable name
    get_filename_component(distribution_lsh_test_filename ${distribution_lsh_test_source} NAME)
    string(REPLACE ".cpp" "" distribution_lsh_test_name ${distribution_lsh_test_filename})

    # Add the test target separately and as part of "make check-test"
    add_executable(${distribution_lsh_test_name}  EXCLUDE_FROM_ALL ${distribution_lsh_test_source} "${PROJECT_SOURCE_DIR}/tools/backtrace.cpp")
    add_backward(${distribution_lsh_test_name})
    add_dependencies(build-tests ${distribution_lsh_test_name})
    add_dependencies(check-tests ${distribution_lsh_test_name})

    gtest_discover_tests(${distribution_lsh_test_name}
            EXTRA_ARGS
            --gtest_color=auto
            --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${distribution_lsh_test_name}.xml
            --gtest_catch_exceptions=0
            DISCOVERY_TIMEOUT 120
            PROPERTIES
            TIMEOUT 120
    )

    target_link_libraries(${distribution_lsh_test_name} distribution_lsh gtest gmock_main hwy)

    # Set test target properties and dependencies.
    set_target_properties(${distribution_lsh_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${distribution_lsh_test_name}
    )
endforeach ()