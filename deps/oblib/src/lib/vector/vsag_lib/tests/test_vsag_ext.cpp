
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
#include <cstdint>
#include <fstream>

#include "catch2/catch_message.hpp"
#include "fixtures/fixtures.h"
#include "vsag/dataset.h"
#include "vsag/vsag_ext.h"

TEST_CASE("dataset handler", "[ft][ext]") {
    vsag::ext::DatasetHandler* dh = nullptr;
    SECTION("make") {
        dh = vsag::ext::DatasetHandler::Make();
    }

    SECTION("make from dataset") {
        auto ds = vsag::Dataset::Make();
        dh = vsag::ext::DatasetHandler::Make(ds);
    }

    auto ids = new int64_t[10];
    auto dists = new float[10];
    auto f32vectors = new float[10 * 128];
    auto i8vectors = new int8_t[10 * 128];

    dh->Owner(true);
    dh->NumElements(100);
    CHECK(dh->GetNumElements() == 100);
    dh->Dim(128);
    CHECK(dh->GetDim() == 128);
    dh->Dim(128);
    CHECK(dh->GetDim() == 128);
    dh->Ids(ids);
    CHECK(dh->GetIds() == ids);
    dh->Distances(dists);
    CHECK(dh->GetDistances() == dists);
    dh->Float32Vectors(f32vectors);
    CHECK(dh->GetFloat32Vectors() == f32vectors);
    dh->Int8Vectors(i8vectors);
    CHECK(dh->GetInt8Vectors() == i8vectors);

    delete dh;
}

TEST_CASE("bitset handler", "[ft][ext]") {
    vsag::ext::BitsetHandler* bh = vsag::ext::BitsetHandler::Make();

    CHECK_FALSE(bh->Test(12345678));

    bh->Set(12345678, true);
    CHECK(bh->Test(12345678));
    CHECK(bh->Count() == 1);

    bh->Set(12345678, false);
    CHECK_FALSE(bh->Test(12345678));

    delete bh;
}

TEST_CASE("index handler", "[ft][ext]") {
    auto parameters = R"(
    {
        "dtype": "float32",
        "metric_type": "ip",
        "dim": 16,
        "hnsw": {
            "max_degree": 16,
            "ef_construction": 100
        }
    }
    )";
    auto make_indexhandler = vsag::ext::IndexHandler::Make("hnsw", parameters);
    REQUIRE(make_indexhandler.has_value());
    vsag::ext::IndexHandler* index_handler = make_indexhandler.value();

    // build
    vsag::ext::DatasetHandler* base_handler = vsag::ext::DatasetHandler::Make();
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(100, 16);
    base_handler->NumElements(100)
        ->Dim(16)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    REQUIRE(index_handler->Build(base_handler).has_value());

    auto query_vector = fixtures::generate_vectors(1, 16);
    vsag::ext::DatasetHandler* query_handler = vsag::ext::DatasetHandler::Make();
    query_handler->NumElements(1)->Dim(16)->Float32Vectors(query_vector.data())->Owner(false);
    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        }
    }
    )";

    // search
    auto knn_search = index_handler->KnnSearch(query_handler, 10, search_parameters);
    REQUIRE(knn_search.has_value());
    vsag::ext::DatasetHandler* search_result_handler = knn_search.value();
    auto range_search = index_handler->RangeSearch(query_handler, 0.5, search_parameters);
    REQUIRE(range_search.has_value());
    delete search_result_handler;
    search_result_handler = range_search.value();

    // serialize/deserialize
    {
        auto serialize = index_handler->Serialize();
        REQUIRE(serialize.has_value());

        auto bs = serialize.value();

        auto new_make_indexhandler = vsag::ext::IndexHandler::Make("hnsw", parameters);
        REQUIRE(new_make_indexhandler.has_value());
        vsag::ext::IndexHandler* new_index_handler = new_make_indexhandler.value();
        REQUIRE(new_index_handler->Deserialize(bs).has_value());

        delete new_index_handler;
    }

    {
        fixtures::temp_dir dir("test_ext");
        std::fstream out_file(dir.path + "index.bin", std::ios::out | std::ios::binary);
        REQUIRE(index_handler->Serialize(out_file).has_value());
        out_file.close();

        std::fstream in_file(dir.path + "index.bin", std::ios::in | std::ios::binary);
        auto new_make_indexhandler = vsag::ext::IndexHandler::Make("hnsw", parameters);
        REQUIRE(new_make_indexhandler.has_value());
        vsag::ext::IndexHandler* new_index_handler = new_make_indexhandler.value();
        auto deserialize = new_index_handler->Deserialize(in_file);
        REQUIRE(deserialize.has_value());

        delete new_index_handler;
    }

    CHECK(index_handler->GetNumElements() == 100);
    CHECK(index_handler->GetMemoryUsage() > 0);

    // free memory
    delete search_result_handler;
    delete query_handler;
    delete base_handler;
    delete index_handler;
}
