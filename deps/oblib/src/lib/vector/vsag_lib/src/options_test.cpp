
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

#include "vsag/options.h"

#include <catch2/catch_test_macros.hpp>

#include "default_allocator.h"

TEST_CASE("option test", "[ut][option]") {
    size_t block_size_limit = 134217728;
    vsag::Options::Instance().set_block_size_limit(block_size_limit);
    REQUIRE(vsag::Option::Instance().block_size_limit() == block_size_limit);

    REQUIRE_THROWS(vsag::Option::Instance().set_block_size_limit(1));

    size_t num_threads_io = 24;
    vsag::Options::Instance().set_num_threads_io(num_threads_io);
    REQUIRE(vsag::Option::Instance().num_threads_io() == num_threads_io);

    REQUIRE_THROWS(vsag::Option::Instance().set_num_threads_io(0));
    REQUIRE_THROWS(vsag::Option::Instance().set_num_threads_io(201));

    size_t num_threads_building = 24;
    vsag::Options::Instance().set_num_threads_building(num_threads_building);
    REQUIRE(vsag::Option::Instance().num_threads_building() == num_threads_building);

    REQUIRE_THROWS(vsag::Option::Instance().set_num_threads_building(0));
    REQUIRE_THROWS(vsag::Option::Instance().set_num_threads_building(201));
}