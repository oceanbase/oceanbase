

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

#include "default_allocator.h"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("default allocator", "[ut]") {
    vsag::DefaultAllocator allocator;
    int number = 69278;
    auto p = (int*)allocator.Allocate(sizeof(int) * 1);

    REQUIRE(p);

    *p = number;
    auto p2 = (int*)allocator.Reallocate(p, sizeof(int) * 2);
    REQUIRE(*p2 == number);

    allocator.Deallocate(p2);
}

TEST_CASE("match of malloc and free", "[ut]") {
#ifndef NDBUG
    vsag::DefaultAllocator allocator;
    size_t alloc_size = 1024;
    auto p = malloc(alloc_size);
    REQUIRE_THROWS(allocator.Reallocate(p, alloc_size));
    REQUIRE_THROWS(allocator.Deallocate(p));
    free(p);

    p = allocator.Reallocate(nullptr, alloc_size);
    allocator.Deallocate(p);
    allocator.Deallocate(nullptr);
#endif
}
