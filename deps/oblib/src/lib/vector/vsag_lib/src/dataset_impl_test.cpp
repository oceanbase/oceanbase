

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

#include "./dataset_impl.h"

#include <catch2/catch_test_macros.hpp>

#include "default_allocator.h"
#include "vsag/dataset.h"

TEST_CASE("test dataset", "[ut][dataset]") {
    vsag::DefaultAllocator allocator;
    SECTION("allocator") {
        auto dataset = vsag::Dataset::Make();
        auto* data = (float*)allocator.Allocate(sizeof(float) * 1);
        dataset->Float32Vectors(data)->Owner(true, &allocator);
    }

    SECTION("delete") {
        auto dataset = vsag::Dataset::Make();
        auto* data = new float[1];
        dataset->Float32Vectors(data);
    }

    SECTION("default") {
        auto dataset = vsag::Dataset::Make();
        auto* data = new float[1];
        dataset->Float32Vectors(data)->Owner(false);
        delete[] data;
    }
}
