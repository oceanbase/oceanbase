
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

#include "footer.h"

#include <catch2/catch_test_macros.hpp>

TEST_CASE("footer basic usage", "[ut][footer]") {
    vsag::SerializationFooter footer;

    SECTION("successful case") {
        for (int i = 0; i < 10; i++) {
            footer.SetMetadata(std::to_string(i), std::to_string(i));
        }
        std::stringstream out_ss(std::ios::out | std::ios::binary);
        footer.Serialize(out_ss);
        std::string str = out_ss.str();

        std::stringstream in_ss(std::ios::in | std::ios::binary);
        in_ss.str(str);
        footer.Deserialize(in_ss);
        for (int i = 0; i < 10; i++) {
            REQUIRE(std::stoi(footer.GetMetadata(std::to_string(i))) == i);
        }
        REQUIRE_THROWS(std::stoi(footer.GetMetadata("11")));
        REQUIRE(str.size() == vsag::FOOTER_SIZE);
    }

    SECTION("error in SetMetadata: footer size exceeds 4KB") {
        for (int i = 0; i <= 355; i++) {
            footer.SetMetadata(std::to_string(i), std::to_string(i));
        }
        REQUIRE_THROWS(footer.SetMetadata(std::to_string(356), std::to_string(356)));
        REQUIRE_THROWS(footer.SetMetadata(std::to_string(355), std::to_string(55555555555555555)));
    }

    SECTION("error in Deserialize: less bits") {
        std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
        footer.Serialize(ss);
        std::string str = ss.str();
        str.resize(vsag::FOOTER_SIZE - 1);
        ss.str(str);
        REQUIRE_THROWS(footer.Deserialize(ss));
    }

    SECTION("error in Deserialize: less bits") {
        for (int i = 0; i < 10; i++) {
            footer.SetMetadata(std::to_string(i), std::to_string(i));
        }
        std::stringstream out_ss(std::ios::out | std::ios::binary);
        footer.Serialize(out_ss);
        std::string str = out_ss.str();

        std::stringstream in_ss(std::ios::in | std::ios::binary);
        str.resize(10);
        in_ss.str(str);
        REQUIRE_THROWS(footer.Deserialize(in_ss));
    }

    SECTION("error in GetMetadata: invalid key") {
        REQUIRE_THROWS(footer.GetMetadata("999"));
    }

    SECTION("error in Deserialize: invalid json") {
        std::string invalid_json_part = "12341234";
        for (int i = 0; i < 10; i++) {
            footer.SetMetadata(std::to_string(i), std::to_string(i));
        }
        std::stringstream out_ss(std::ios::out | std::ios::binary);
        footer.Serialize(out_ss);
        std::string str = out_ss.str();
        str.replace(str.find("\"3\":"), 6, invalid_json_part);

        std::stringstream in_ss(std::ios::in | std::ios::binary);
        in_ss.str(str);
        REQUIRE_THROWS(footer.Deserialize(in_ss));
    }

    SECTION("error in Deserialize: invalid footer size") {
        std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
        uint32_t serialized_data_size = vsag::FOOTER_SIZE + 1;
        ss << serialized_data_size;
        REQUIRE_THROWS(footer.Deserialize(ss));
    }

    SECTION("error in Deserialize: invalid magic num") {
        std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
        std::string invalid_value = "abcd";
        footer.SetMetadata(vsag::SERIALIZE_MAGIC_NUM, invalid_value);
        footer.Serialize(ss);
        REQUIRE_THROWS(footer.Deserialize(ss));
    }

    SECTION("error in Deserialize: invalid version") {
        std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
        std::string invalid_value = "1.0";
        footer.SetMetadata(vsag::SERIALIZE_VERSION, invalid_value);
        footer.Serialize(ss);
        REQUIRE_THROWS(footer.Deserialize(ss));
    }
}