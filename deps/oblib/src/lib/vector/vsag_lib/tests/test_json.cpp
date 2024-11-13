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
#include <nlohmann/json.hpp>

TEST_CASE("Json usage", "[ft][json]") {
    nlohmann::json j{};

    j["name"] = "Alice";
    j["age"] = 25;
    j["isMarried"] = false;

    std::string name = j["name"];
    int age = j["age"];
    bool isMarried = j["isMarried"];

    REQUIRE(name == "Alice");
    REQUIRE(age == 25);
    REQUIRE(isMarried == false);
}
