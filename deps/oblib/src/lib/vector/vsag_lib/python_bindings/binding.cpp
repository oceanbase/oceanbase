
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

#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <filesystem>
#include <fstream>
#include <map>

#include "iostream"
#include "vsag/dataset.h"
#include "vsag/vsag.h"

namespace py = pybind11;

class Index {
public:
    Index(std::string name, const std::string& parameters) {
        if (auto index = vsag::Factory::CreateIndex(name, parameters)) {
            index_ = index.value();
        } else {
            vsag::Error error_code = index.error();
            if (error_code.type == vsag::ErrorType::UNSUPPORTED_INDEX) {
                throw std::runtime_error("error type: UNSUPPORTED_INDEX");
            } else if (error_code.type == vsag::ErrorType::INVALID_ARGUMENT) {
                throw std::runtime_error("error type: invalid_parameter");
            } else {
                throw std::runtime_error("error type: unexpectedError");
            }
        }
    }

public:
    void
    Build(py::array_t<float> vectors, py::array_t<int64_t> ids, size_t num_elements, size_t dim) {
        auto dataset = vsag::Dataset::Make();
        dataset->Owner(false)
            ->Dim(dim)
            ->NumElements(num_elements)
            ->Ids(ids.mutable_data())
            ->Float32Vectors(vectors.mutable_data());
        index_->Build(dataset);
    }

    py::object
    KnnSearch(py::array_t<float> vector, size_t k, std::string& parameters) {
        auto query = vsag::Dataset::Make();
        size_t data_num = 1;
        query->NumElements(data_num)
            ->Dim(vector.size())
            ->Float32Vectors(vector.mutable_data())
            ->Owner(false);

        auto labels = py::array_t<int64_t>(k);
        auto dists = py::array_t<float>(k);
        if (auto result = index_->KnnSearch(query, k, parameters); result.has_value()) {
            auto labels_data = labels.mutable_data();
            auto dists_data = dists.mutable_data();
            auto ids = result.value()->GetIds();
            auto distances = result.value()->GetDistances();
            for (int i = 0; i < data_num * k; ++i) {
                labels_data[i] = ids[i];
                dists_data[i] = distances[i];
            }
        }

        return py::make_tuple(labels, dists);
    }

    py::object
    RangeSearch(py::array_t<float> point, float threshold, std::string& parameters) {
        auto query = vsag::Dataset::Make();
        size_t data_num = 1;
        query->NumElements(data_num)
            ->Dim(point.size())
            ->Float32Vectors(point.mutable_data())
            ->Owner(false);

        py::array_t<int64_t> labels;
        py::array_t<float> dists;
        if (auto result = index_->RangeSearch(query, threshold, parameters); result.has_value()) {
            auto ids = result.value()->GetIds();
            auto distances = result.value()->GetDistances();
            auto k = result.value()->GetDim();
            labels.resize({k});
            dists.resize({k});
            auto labels_data = labels.mutable_data();
            auto dists_data = dists.mutable_data();
            for (int i = 0; i < data_num * k; ++i) {
                labels_data[i] = ids[i];
                dists_data[i] = distances[i];
            }
        }

        return py::make_tuple(labels, dists);
    }

    std::map<std::string, size_t>
    Save(const std::string& dir_name) {
        auto serialize_result = index_->Serialize();
        if (not serialize_result.has_value()) {
            throw std::runtime_error("serialize error: " + serialize_result.error().message);
        }
        vsag::BinarySet& binary_set = serialize_result.value();
        std::filesystem::path dir(dir_name);
        std::map<std::string, size_t> file_sizes;
        for (const auto& key : binary_set.GetKeys()) {
            std::filesystem::path file_path(key);
            std::filesystem::path full_path = dir / file_path;
            vsag::Binary binary = binary_set.Get(key);
            std::ofstream file(full_path.string(), std::ios::binary);
            file.write(reinterpret_cast<char*>(binary.data.get()), binary.size);
            file_sizes[key] = binary.size;
            file.close();
        }

        return std::move(file_sizes);
    }

    void
    Load(const std::string& dir_name,
         const std::map<std::string, size_t>& file_sizes,
         bool load_memory) {
        std::filesystem::path dir(dir_name);
        if (load_memory) {
            vsag::BinarySet binary_set;
            for (const auto& single_file : file_sizes) {
                const std::string& key = single_file.first;
                size_t size = single_file.second;
                std::filesystem::path file_path(key);
                std::filesystem::path full_path = dir / file_path;
                std::ifstream file(full_path.string(), std::ios::binary);
                vsag::Binary binary;
                binary.size = size;
                binary.data.reset(new int8_t[binary.size]);
                file.read(reinterpret_cast<char*>(binary.data.get()), size);
                file.close();
                binary_set.Set(key, binary);
            }
            index_->Deserialize(binary_set);
        } else {
            vsag::ReaderSet reader_set;
            for (const auto& single_file : file_sizes) {
                const std::string& key = single_file.first;
                size_t size = single_file.second;
                std::filesystem::path file_path(key);
                std::filesystem::path full_path = dir / file_path;
                auto reader = vsag::Factory::CreateLocalFileReader(full_path.string(), 0, size);
                reader_set.Set(key, reader);
            }
            index_->Deserialize(reader_set);
        }
    }

private:
    std::shared_ptr<vsag::Index> index_;
};

PYBIND11_MODULE(pyvsag, m) {
    py::class_<Index>(m, "Index")
        .def(py::init<std::string, std::string&>(), py::arg("name"), py::arg("parameters"))
        .def("build",
             &Index::Build,
             py::arg("vectors"),
             py::arg("ids"),
             py::arg("num_elements"),
             py::arg("dim"))
        .def(
            "knn_search", &Index::KnnSearch, py::arg("vector"), py::arg("k"), py::arg("parameters"))
        .def("range_search",
             &Index::RangeSearch,
             py::arg("vector"),
             py::arg("threshold"),
             py::arg("parameters"))
        .def("save", &Index::Save, py::arg("dir_name"))
        .def("load",
             &Index::Load,
             py::arg("dir_name"),
             py::arg("file_sizes"),
             py::arg("load_memory"));
}
