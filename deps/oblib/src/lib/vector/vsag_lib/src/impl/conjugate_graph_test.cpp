
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

#include "conjugate_graph.h"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "../../tests/fixtures/fixtures.h"

TEST_CASE("build, add and memory usage", "[ut][conjugate_graph]") {
    std::shared_ptr<vsag::ConjugateGraph> conjugate_graph =
        std::make_shared<vsag::ConjugateGraph>();
    REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);
    REQUIRE(conjugate_graph->AddNeighbor(0, 0) == false);
    REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);

    REQUIRE(conjugate_graph->AddNeighbor(0, 1) == true);
    REQUIRE(conjugate_graph->GetMemoryUsage() == 28 + vsag::FOOTER_SIZE);

    REQUIRE(conjugate_graph->AddNeighbor(0, 1) == false);
    REQUIRE(conjugate_graph->GetMemoryUsage() == 28 + vsag::FOOTER_SIZE);

    REQUIRE(conjugate_graph->AddNeighbor(0, 2) == true);
    REQUIRE(conjugate_graph->GetMemoryUsage() == 36 + vsag::FOOTER_SIZE);

    REQUIRE(conjugate_graph->AddNeighbor(1, 0) == true);
    REQUIRE(conjugate_graph->GetMemoryUsage() == 60 + vsag::FOOTER_SIZE);
}

TEST_CASE("serialize and deserialize with binary", "[ut][conjugate_graph]") {
    std::shared_ptr<vsag::ConjugateGraph> conjugate_graph =
        std::make_shared<vsag::ConjugateGraph>();

    conjugate_graph->AddNeighbor(0, 2);
    conjugate_graph->AddNeighbor(0, 1);
    conjugate_graph->AddNeighbor(1, 0);

    SECTION("successful case") {
        vsag::Binary binary = *conjugate_graph->Serialize();
        REQUIRE(binary.size == 60 + vsag::FOOTER_SIZE);

        REQUIRE(conjugate_graph->Deserialize(binary).has_value());
        REQUIRE(conjugate_graph->GetMemoryUsage() == 60 + vsag::FOOTER_SIZE);
        REQUIRE(conjugate_graph->AddNeighbor(0, 2) == false);
        REQUIRE(conjugate_graph->AddNeighbor(0, 1) == false);
        REQUIRE(conjugate_graph->AddNeighbor(1, 0) == false);
    }

    SECTION("deserialize with less bits") {
        vsag::Binary binary = *conjugate_graph->Serialize();
        uint32_t invalid_memory_usage = 0;

        invalid_memory_usage = 0;
        std::memcpy((char*)binary.data.get(), &invalid_memory_usage, sizeof(invalid_memory_usage));
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        invalid_memory_usage = 60 + vsag::FOOTER_SIZE;
        std::memcpy((char*)binary.data.get(), &invalid_memory_usage, sizeof(invalid_memory_usage));

        binary.size = 0;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        binary.size = 0 + vsag::FOOTER_SIZE;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        binary.size = 3 + vsag::FOOTER_SIZE;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        binary.size = 9 + vsag::FOOTER_SIZE;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        binary.size = 27 + vsag::FOOTER_SIZE;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        binary.size = 35 + vsag::FOOTER_SIZE;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);

        binary.size = 59 + vsag::FOOTER_SIZE;
        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);
    }

    SECTION("deserialize with invalid magic_num") {
        vsag::Binary binary = *conjugate_graph->Serialize();

        nlohmann::json json;
        json[vsag::SERIALIZE_MAGIC_NUM] = std::to_string(0xABCD1234);
        json[vsag::SERIALIZE_VERSION] = vsag::VERSION;
        std::string json_str = json.dump();
        uint32_t serialized_data_size = json_str.size();
        std::memcpy(binary.data.get() + binary.size - vsag::FOOTER_SIZE,
                    reinterpret_cast<const char*>(&serialized_data_size),
                    sizeof(serialized_data_size));
        std::memcpy(
            binary.data.get() + binary.size - vsag::FOOTER_SIZE + sizeof(serialized_data_size),
            json_str.c_str(),
            json_str.size());

        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);
        REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);
        binary = *conjugate_graph->Serialize();
        REQUIRE(binary.size == 4 + vsag::FOOTER_SIZE);
        REQUIRE(conjugate_graph->Deserialize(binary));
        REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);
    }

    SECTION("deserialize with invalid version") {
        vsag::Binary binary = *conjugate_graph->Serialize();

        nlohmann::json json;
        json[vsag::SERIALIZE_MAGIC_NUM] = vsag::MAGIC_NUM;
        json[vsag::SERIALIZE_VERSION] = std::to_string(2);
        std::string json_str = json.dump();
        uint32_t serialized_data_size = json_str.size();
        std::memcpy(binary.data.get() + binary.size - vsag::FOOTER_SIZE,
                    reinterpret_cast<const char*>(&serialized_data_size),
                    sizeof(serialized_data_size));
        std::memcpy(
            binary.data.get() + binary.size - vsag::FOOTER_SIZE + sizeof(serialized_data_size),
            json_str.c_str(),
            json_str.size());

        REQUIRE(conjugate_graph->Deserialize(binary).error().type == vsag::ErrorType::READ_ERROR);
        REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);
        binary = *conjugate_graph->Serialize();
        REQUIRE(binary.size == 4 + vsag::FOOTER_SIZE);
        REQUIRE(conjugate_graph->Deserialize(binary));
        REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);
    }
}

TEST_CASE("serialize and deserialize with stream", "[ut][conjugate_graph]") {
    std::shared_ptr<vsag::ConjugateGraph> conjugate_graph =
        std::make_shared<vsag::ConjugateGraph>();

    conjugate_graph->AddNeighbor(0, 2);
    conjugate_graph->AddNeighbor(0, 1);
    conjugate_graph->AddNeighbor(1, 0);

    SECTION("successful case") {
        fixtures::temp_dir dir("conjugate_graph_test_deserialize_on_not_empty_index");
        std::fstream out_stream(dir.path + "conjugate_graph.bin", std::ios::out | std::ios::binary);
        auto serialize_result = conjugate_graph->Serialize(out_stream);
        REQUIRE(serialize_result.has_value());
        out_stream.close();

        std::fstream in_stream(dir.path + "conjugate_graph.bin", std::ios::in | std::ios::binary);
        in_stream.seekg(0, std::ios::end);
        REQUIRE(in_stream.tellg() == 60 + vsag::FOOTER_SIZE);
        in_stream.seekg(0, std::ios::beg);
        REQUIRE(conjugate_graph->Deserialize(in_stream).has_value());
        in_stream.close();

        REQUIRE(conjugate_graph->GetMemoryUsage() == 60 + vsag::FOOTER_SIZE);
        REQUIRE(conjugate_graph->AddNeighbor(0, 2) == false);
        REQUIRE(conjugate_graph->AddNeighbor(0, 1) == false);
        REQUIRE(conjugate_graph->AddNeighbor(1, 0) == false);
    }

    SECTION("invalid magic_num") {
        fixtures::temp_dir dir("conjugate_graph_test_deserialize_on_not_empty_index");
        std::fstream out_stream(dir.path + "conjugate_graph.bin", std::ios::out | std::ios::binary);
        auto serialize_result = conjugate_graph->Serialize(out_stream);
        REQUIRE(serialize_result.has_value());
        out_stream.seekg(conjugate_graph->GetMemoryUsage() - vsag::FOOTER_SIZE, std::ios::beg);

        nlohmann::json json;
        json[vsag::SERIALIZE_MAGIC_NUM] = std::to_string(0xABCD1234);
        json[vsag::SERIALIZE_VERSION] = vsag::VERSION;
        std::string json_str = json.dump();
        uint32_t serialized_data_size = json_str.size();
        out_stream.write(reinterpret_cast<const char*>(&serialized_data_size),
                         sizeof(serialized_data_size));
        out_stream.write(json_str.c_str(), json_str.size());
        out_stream.close();

        std::fstream in_stream(dir.path + "conjugate_graph.bin", std::ios::in | std::ios::binary);
        REQUIRE(conjugate_graph->Deserialize(in_stream).error().type ==
                vsag::ErrorType::READ_ERROR);
        in_stream.close();
    }

    SECTION("invalid version") {
        fixtures::temp_dir dir("conjugate_graph_test_deserialize_on_not_empty_index");
        std::fstream out_stream(dir.path + "conjugate_graph.bin", std::ios::out | std::ios::binary);
        auto serialize_result = conjugate_graph->Serialize(out_stream);
        REQUIRE(serialize_result.has_value());
        out_stream.seekg(conjugate_graph->GetMemoryUsage() - vsag::FOOTER_SIZE, std::ios::beg);

        nlohmann::json json;
        json[vsag::SERIALIZE_MAGIC_NUM] = vsag::MAGIC_NUM;
        json[vsag::SERIALIZE_VERSION] = std::to_string(2);
        std::string json_str = json.dump();
        uint32_t serialized_data_size = json_str.size();
        out_stream.write(reinterpret_cast<const char*>(&serialized_data_size),
                         sizeof(serialized_data_size));
        out_stream.write(json_str.c_str(), json_str.size());
        out_stream.close();

        std::fstream in_stream(dir.path + "conjugate_graph.bin", std::ios::in | std::ios::binary);
        REQUIRE(conjugate_graph->Deserialize(in_stream).error().type ==
                vsag::ErrorType::READ_ERROR);
        in_stream.close();
    }

    SECTION("less bits") {
        fixtures::temp_dir dir("conjugate_graph_test_deserialize_on_not_empty_index");
        std::fstream out_stream(dir.path + "conjugate_graph.bin", std::ios::out | std::ios::binary);
        auto serialize_result = conjugate_graph->Serialize(out_stream);
        REQUIRE(serialize_result.has_value());
        out_stream.close();

        std::filesystem::resize_file(dir.path + "conjugate_graph.bin", 55 + vsag::FOOTER_SIZE);

        std::fstream in_stream(dir.path + "conjugate_graph.bin", std::ios::in | std::ios::binary);
        REQUIRE(conjugate_graph->Deserialize(in_stream).error().type ==
                vsag::ErrorType::READ_ERROR);
        in_stream.close();
    }

    SECTION("invalid header") {
        uint32_t invalid_memory_usage = 0;
        fixtures::temp_dir dir("conjugate_graph_test_deserialize_on_not_empty_index");
        std::fstream out_stream(dir.path + "conjugate_graph.bin", std::ios::out | std::ios::binary);
        auto serialize_result = conjugate_graph->Serialize(out_stream);
        REQUIRE(serialize_result.has_value());
        out_stream.seekg(0);
        out_stream.write((char*)&invalid_memory_usage, sizeof(invalid_memory_usage));
        out_stream.close();

        std::fstream in_stream(dir.path + "conjugate_graph.bin", std::ios::in | std::ios::binary);
        REQUIRE(conjugate_graph->Deserialize(in_stream).error().type ==
                vsag::ErrorType::READ_ERROR);
        in_stream.close();
    }

    SECTION("failed deserialize and re-serialize") {
        fixtures::temp_dir dir("conjugate_graph_test_deserialize_on_not_empty_index");
        std::fstream out_stream(dir.path + "conjugate_graph.bin", std::ios::out | std::ios::binary);
        auto serialize_result = conjugate_graph->Serialize(out_stream);
        REQUIRE(serialize_result.has_value());
        out_stream.close();

        std::filesystem::resize_file(dir.path + "conjugate_graph.bin", 55 + vsag::FOOTER_SIZE);

        std::fstream in_stream(dir.path + "conjugate_graph.bin", std::ios::in | std::ios::binary);
        REQUIRE(conjugate_graph->Deserialize(in_stream).error().type ==
                vsag::ErrorType::READ_ERROR);
        in_stream.close();

        REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);

        std::fstream re_out_stream(dir.path + "conjugate_graph.bin",
                                   std::ios::out | std::ios::binary);
        auto re_serialize_result = conjugate_graph->Serialize(re_out_stream);
        REQUIRE(serialize_result.has_value());
        re_out_stream.close();

        std::fstream re_in_stream(dir.path + "conjugate_graph.bin",
                                  std::ios::in | std::ios::binary);
        REQUIRE(conjugate_graph->Deserialize(re_in_stream).has_value());
        REQUIRE(conjugate_graph->GetMemoryUsage() == 4 + vsag::FOOTER_SIZE);
        re_in_stream.close();
    }
}