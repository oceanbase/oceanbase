// Copyright 2024-present the vsag project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <filesystem>
#include <random>
#include <tuple>
#include <vector>

#include "vsag/vsag.h"

namespace fixtures {

std::vector<float>
generate_vectors(int64_t num_vectors, int64_t dim, bool need_normalize = true);

std::tuple<std::vector<int64_t>, std::vector<float>>
generate_ids_and_vectors(int64_t num_elements, int64_t dim, bool need_normalize = true);

vsag::IndexPtr
generate_index(const std::string& name,
               const std::string& metric_type,
               int64_t num_vectors,
               int64_t dim,
               std::vector<int64_t>& ids,
               std::vector<float>& vectors,
               bool use_conjugate_graph = false);

float
test_knn_recall(const vsag::IndexPtr& index,
                const std::string& search_parameters,
                int64_t num_vectors,
                int64_t dim,
                std::vector<int64_t>& ids,
                std::vector<float>& vectors);

float
test_range_recall(const vsag::IndexPtr& index,
                  const std::string& search_parameters,
                  int64_t num_vectors,
                  int64_t dim,
                  std::vector<int64_t>& ids,
                  std::vector<float>& vectors);

std::string
generate_hnsw_build_parameters_string(const std::string& metric_type, int64_t dim);

vsag::DatasetPtr
brute_force(const vsag::DatasetPtr& query,
            const vsag::DatasetPtr& base,
            int64_t k,
            const std::string& metric_type);

struct temp_dir {
    temp_dir(const std::string& name) {
        auto epoch_time = std::chrono::system_clock::now().time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch_time).count();

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(1000, 9999);
        int random_number = dist(gen);

        std::stringstream dirname;
        dirname << "vsagtest_" << std::setfill('0') << std::setw(14) << seconds << "_"
                << std::to_string(random_number);
        path = "/tmp/" + dirname.str() + "/";
        std::filesystem::create_directory(path);
    }

    ~temp_dir() {
        std::filesystem::remove_all(path);
    }

    std::string path;
};

struct comparable_float_t {
    comparable_float_t(float val) {
        this->value = val;
    }

    bool
    operator==(const comparable_float_t& d) const {
        return std::fabs(this->value - d.value) < epsilon;
    }

    friend std::ostream&
    operator<<(std::ostream& os, const comparable_float_t& obj) {
        os << obj.value;
        return os;
    }

    float value;
    const float epsilon = 2e-6;
};
using dist_t = comparable_float_t;
// The error epsilon between time_t and recall_t should be 1e-6; however, the error does not fall
// between 1e-6 and 2e-6 in actual situations. Therefore, to ensure compatibility with dist_t,
// we will limit the error to within 2e-6.
using time_t = comparable_float_t;
using recall_t = comparable_float_t;

}  // Namespace fixtures
