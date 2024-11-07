# VSAG
- [VSAG](#vsag)
  - [What is VSAG](#what-is-vsag)
  - [Getting Started](#getting-started)
    - [Integrate with CMake](#integrate-with-cmake)
    - [Try the Example](#try-the-example)
  - [Developer Guide](#developer-guide)
    - [Dependencies](#dependencies)
    - [VSAG Build Tool](#vsag-build-tool)
    - [Project Structure](#project-structure)
  - [Roadmap](#roadmap)
  - [Contribution Guidelines](#contribution-guidelines)
  - [License](#license)


## What is VSAG
```
____    ____   _______.     ___       _______ 
\   \  /   /  /       |    /   \     /  _____|
 \   \/   /  |   (----`   /  ^  \   |  |  __  
  \      /    \   \      /  /_\  \  |  | |_ | 
   \    / .----)   |    /  _____  \ |  |__| | 
    \__/  |_______/    /__/     \__\ \______| 
```
VSAG is a vector indexing library used for similarity search. The indexing algorithm allows users to search through various sizes of vector sets, especially those that cannot fit in memory. The library also provides methods for generating parameters based on vector dimensions and data scale, allowing developers to use it without understanding the algorithm’s principles. VSAG is written in C++ and provides a Python wrapper package called pyvsag. Developed by the Vector Database Team at Ant Group.

## Getting Started
### Integrate with CMake
```cmake
# CMakeLists.txt
cmake_minimum_required(VERSION 3.11)

project (myproject)

set (CMAKE_CXX_STANDARD 11)

# download and compile vsag
include (FetchContent)
FetchContent_Declare (
  vsag
  GIT_REPOSITORY https://github.com/alipay/vsag
  GIT_TAG master
)
FetchContent_MakeAvailable (vsag)
include_directories (vsag-cmake-example PRIVATE ${vsag_SOURCE_DIR}/include)

# compile executable and link to vsag
add_executable (vsag-cmake-example src/main.cpp)
target_link_libraries (vsag-cmake-example PRIVATE vsag)

# add dependency
add_dependencies (vsag-cmake-example vsag)
```
### Try the Example
```cpp
#include <vsag/vsag.h>

#include <iostream>

int
main(int argc, char** argv) {
    vsag::init();

    int64_t num_vectors = 10000;
    int64_t dim = 128;

    // prepare ids and vectors
    auto ids = new int64_t[num_vectors];
    auto vectors = new float[dim * num_vectors];

    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    for (int64_t i = 0; i < num_vectors; ++i) {
        ids[i] = i;
    }
    for (int64_t i = 0; i < dim * num_vectors; ++i) {
        vectors[i] = distrib_real(rng);
    }

    // create index
    auto hnsw_build_paramesters = R"(
    {
        "dtype": "float32",
        "metric_type": "l2",
        "dim": 128,
        "hnsw": {
            "max_degree": 16,
            "ef_construction": 100
        }
    }
    )";
    auto index = vsag::Factory::CreateIndex("hnsw", hnsw_build_paramesters).value();
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)->Dim(dim)->Ids(ids)->Float32Vectors(vectors)->Owner(false);
    index->Build(base);

    // prepare a query vector
    auto query_vector = new float[dim];  // memory will be released by query the dataset
    for (int64_t i = 0; i < dim; ++i) {
        query_vector[i] = distrib_real(rng);
    }

    // search on the index
    auto hnsw_search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        }
    }
    )";
    int64_t topk = 10;
    auto query = vsag::Dataset::Make();
    query->NumElements(1)->Dim(dim)->Float32Vectors(query_vector)->Owner(true);
    auto result = index->KnnSearch(query, topk, hnsw_search_parameters).value();

    // print the results
    std::cout << "results: " << std::endl;
    for (int64_t i = 0; i < result->GetDim(); ++i) {
        std::cout << result->GetIds()[i] << ": " << result->GetDistances()[i] << std::endl;
    }

    // free memory
    delete[] ids;
    delete[] vectors;

    return 0;
}
```

## Developer Guide
### Dependencies
```bash
# for Debian/Ubuntu
$ ./scripts/deps/install_deps_ubuntu.sh

# for CentOS/AliOS
$ ./scripts/deps/install_deps_centos.sh
```
### VSAG Build Tool
```bash
Usage: make <target>

Targets:
help:                   ## Show the help.
debug:                  ## Build vsag with debug options.
release:                ## Build vsag with release options.
distribution:           ## Build vsag with distribution options.
fmt:                    ## Format codes.
test:                   ## Build and run unit tests.
asan:                   ## Build with AddressSanitizer option.
test_asan: asan         ## Run unit tests with AddressSanitizer option.
tsan:                   ## Build with ThreadSanitizer option.
test_tsan: tsan         ## Run unit tests with ThreadSanitizer option.
test_cov:               ## Build and run unit tests with code coverage enabled.
clean:                  ## Clear build/ directory.
install:                ## Build and install the release version of vsag.
```
### Project Structure
`benchs/`: benchmark script in Python</br>
`cmake/`: cmake util functions</br>
`docker/`: the dockerfile to build develop and ci image</br>
`examples/`: cpp and python example codes</br>
`externs/`: third-party libraries</br>
`include/`: export header files</br>
`mockimpl/`: the mock implementation that can be used in interface test</br>
`python_bindings/`: the python bindings</br>
`scripts/`: useful scripts</br>
`src/`: the source codes and unit tests</br>
`tests/`: the functional tests</br>

## Roadmap
- v0.11 (ETA: Jul. 2024)
  - support cosine distance type, with normalization
  - support EnhanceGraph on HNSW
- v1.0 (ETA: Aug. 2024)
  - support IVFFlat index
  - support int8 datatype
  - support DFS index
- v1.1 (ETA: Sep. 2024)
  - support FP16 datatype
  - support freshHNSW index
  - support automated parameter

## Contribution Guidelines
Contributions are welcomed and greatly appreciated. Please read our [contribution guidelines](./CONTRIBUTING.md) for detailed contribution workflow. 

## License
[Apache License 2.0](./LICENSE)
