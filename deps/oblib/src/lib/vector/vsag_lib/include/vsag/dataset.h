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
#include <memory>
#include <string>
#include <unordered_map>
#include <variant>

#include "allocator.h"
#include "constants.h"

namespace vsag {

class Dataset;
using DatasetPtr = std::shared_ptr<Dataset>;

class Dataset : public std::enable_shared_from_this<Dataset> {
public:
    static DatasetPtr
    Make();

    virtual ~Dataset() = default;

    virtual DatasetPtr
    Owner(bool is_owner, Allocator* allocator = nullptr) = 0;

public:
    virtual DatasetPtr
    NumElements(const int64_t num_elements) = 0;

    virtual int64_t
    GetNumElements() const = 0;

    virtual DatasetPtr
    Dim(const int64_t dim) = 0;

    virtual int64_t
    GetDim() const = 0;

    virtual DatasetPtr
    Ids(const int64_t* ids) = 0;

    virtual const int64_t*
    GetIds() const = 0;

    virtual DatasetPtr
    Distances(const float* dists) = 0;

    virtual const float*
    GetDistances() const = 0;

    virtual DatasetPtr
    Int8Vectors(const int8_t* vectors) = 0;

    virtual const int8_t*
    GetInt8Vectors() const = 0;

    virtual DatasetPtr
    Float32Vectors(const float* vectors) = 0;

    virtual const float*
    GetFloat32Vectors() const = 0;
};

};  // namespace vsag
