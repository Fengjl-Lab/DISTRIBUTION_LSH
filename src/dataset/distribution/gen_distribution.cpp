//
// Created by 陈俊皓 on 2023/11/19.
//

#include <iostream>
#include <random>
#include <vector>
#include <algorithm>
#include <stdexcept>
#include <omp.h>

template<typename T>
class GenerateDistribution {
private:
    int dimension;
    size_t numberOfDistribution;

public:
    GenerateDistribution() = delete;
    GenerateDistribution(int dimension, size_t numberOfDistribution) : dimension(dimension), numberOfDistribution(numberOfDistribution) {}


    std::vector<std::vector<T>> generate() {
        using value_type = T;
        static  std::uniform_real_distribution<value_type > distribution(
//                std::numeric_limits<value_type >::min(),
//                std::numeric_limits<value_type >::max()
                0, 1
        );

        static std::random_device rd;
        static std::default_random_engine generator(rd());

        std::vector<std::vector<value_type >> data(numberOfDistribution, std::vector<value_type >(dimension));
        for(auto i = 0; i< numberOfDistribution; i++) {
            std::generate(data[i].begin(), data[i].end(), []() { return distribution(generator); });
            normalize(data[i]);

            std::cout << "Distribution " << i << ":" << std::endl;
            for (auto& value : data[i]) {
                std::cout << value << " ";
            }

            std::cout << std::endl;
        }
        return data;
    }

    void normalize(std::vector<T>& distribution) {
        using value_type = T;
        // value_type sum = std::accumulate(distribution.begin(), distribution.end(), 0.0);
        value_type sum = 0;
        #pragma omp parallel for default(none) shared(distribution) reduction(+:sum)
        for (size_t i = 0; i < distribution.size() ; i++) {
            #pragma omp atomic
            sum += distribution[i];
        }

        #pragma omp parallel for default(none) shared(distribution, sum)
        for (size_t i = 0; i < distribution.size(); i++) {
            distribution[i] /= sum;
        }
    }
};
//int main(int argc, char* argv[]) {
//    if (argc < 3) {
//        std::cerr << "Need to input the dimension and number of distribution." << std::endl;
//        exit(1);
//    }
//    int dimension;
//    size_t numberOfDistribution;
//
//    try {
//        dimension = std::stoi(argv[1]);
//        numberOfDistribution = (size_t)std::stoi(argv[2]);
//    } catch (const std::invalid_argument& e) {
//        std::cerr << "Invalid Argument Exception：" << e.what() << std::endl;
//        exit(2);
//    } catch (const std::out_of_range& e) {
//        std::cerr << "Out Of Range: " << e.what() << std::endl;
//        exit(3);
//    }
//
//    GenerateDistribution<double> generateDistribution(dimension, numberOfDistribution);
//    for (auto i = 0u; i < 5; ++i) {
//        generateDistribution.generate();
//    }
//
//    return 0;
//}