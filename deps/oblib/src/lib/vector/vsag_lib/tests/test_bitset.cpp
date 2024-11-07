
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

#include <catch2/catch_test_macros.hpp>

#include "vsag/bitset.h"

TEST_CASE("test bitset", "[ft][bitset]") {
    auto bitset = vsag::Bitset::Make();

    // empty
    REQUIRE(bitset->Count() == 0);

    // set to true
    bitset->Set(100, true);
    REQUIRE(bitset->Test(100));
    REQUIRE(bitset->Count() == 1);

    // set to false
    bitset->Set(100, false);
    REQUIRE_FALSE(bitset->Test(100));
    REQUIRE(bitset->Count() == 0);

    // not set
    REQUIRE_FALSE(bitset->Test(1234567890));

    // dump
    bitset->Set(100, false);
    REQUIRE(bitset->Dump() == "{}");
    bitset->Set(100, true);
    auto dumped = bitset->Dump();
    REQUIRE(dumped == "{100}");
}
