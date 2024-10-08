add_library(
        distribution_lsh_storage_page
        OBJECT
        b_plus_tree/b_plus_tree_internal_page.cpp
        b_plus_tree/b_plus_tree_leaf_page.cpp
        b_plus_tree/b_plus_tree_page.cpp
        dataset/distribution_dataset_page.cpp
        dataset/distribution_dataset_header_page.cpp
        dataset/distribution_dataset_data_page.cpp
        dataset/distribution_dataset_directory_page.cpp
        dataset/distribution_dataset_generation_header_page.cpp
        dataset/distribution_dataset_mnist_header_page.cpp
        dataset/distribution_dataset_cifar10_header_page.cpp
        random_line/random_line_page.cpp
        random_line/random_line_header_page.cpp
        random_line/random_line_directory_page.cpp
        random_line/random_line_data_page.cpp
        random_line/average_random_line_page.cpp
        relation/relation_header_page.cpp
        relation/relation_data_page.cpp
        page_guard.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_PROJECT_FILES} $<TARGET_OBJECTS:distribution_lsh_storage_page>
        PARENT_SCOPE
)