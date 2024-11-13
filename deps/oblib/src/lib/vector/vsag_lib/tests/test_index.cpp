
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

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>
#include <unordered_set>

#include "fixtures/fixtures.h"
#include "vsag/dataset.h"
#include "vsag/errors.h"
#include "vsag/logger.h"
#include "vsag/options.h"
#include "vsag/vsag.h"

namespace vsag {

extern float
L2Sqr(const void* pVect1v, const void* pVect2v, const void* qty_ptr);

extern float
InnerProduct(const void* pVect1v, const void* pVect2v, const void* qty_ptr);

}  // namespace vsag

/////////////////////////////////////////////////////////
// index->build
/////////////////////////////////////////////////////////

TEST_CASE("hnsw build test", "[ft][index][hnsw]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 57;
    auto metric_type = GENERATE("l2", "ip", "cosine");
    bool need_normalize = metric_type != std::string("cosine");

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors * 2, dim, need_normalize);
    auto createindex = vsag::Factory::CreateIndex(
        "hnsw", fixtures::generate_hnsw_build_parameters_string(metric_type, dim));
    REQUIRE(createindex.has_value());
    auto index = createindex.value();

    // build index
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto buildindex = index->Build(base);
    REQUIRE(buildindex.has_value());

    // check the number of vectors in index
    REQUIRE(index->GetNumElements() == num_vectors);

    for (int64_t i = 0; i < num_vectors; ++i) {
        auto one_vector = vsag::Dataset::Make();
        one_vector->NumElements(1)
            ->Dim(dim)
            ->Ids(ids.data() + num_vectors + i)
            ->Float32Vectors(vectors.data() + (num_vectors + i) * dim)
            ->Owner(false);

        REQUIRE(index->Add(one_vector).has_value());
    }

    // check the number of vectors in index
    REQUIRE(index->GetNumElements() == num_vectors * 2);
}

TEST_CASE("diskann build test", "[ft][index][diskann]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 57;
    auto metric_type = GENERATE("l2", "ip");

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);

    constexpr auto build_parameter_json = R"(
        {{
            "dtype": "float32",
            "metric_type": "{}",
            "dim": {},
            "diskann": {{
                "max_degree": 16,
                "ef_construction": 200,
                "pq_dims": 32,
                "pq_sample_rate": {}
            }}
        }}
    )";

    SECTION("pq_sample_rate is too small") {
        auto build_parameters = fmt::format(build_parameter_json, metric_type, dim, -0.5);
        auto createindex = vsag::Factory::CreateIndex("diskann", build_parameters);
        REQUIRE(createindex.has_value());
        auto index = createindex.value();
        auto buildindex = index->Build(base);
        REQUIRE(buildindex.has_value());
    }

    SECTION("pq_sample_rate is zero") {
        auto build_parameters = fmt::format(build_parameter_json, metric_type, dim, 0);
        auto createindex = vsag::Factory::CreateIndex("diskann", build_parameters);
        REQUIRE(createindex.has_value());
        auto index = createindex.value();
        auto buildindex = index->Build(base);
        REQUIRE(buildindex.has_value());
    }

    SECTION("pq_sample_rate is too large") {
        auto build_parameters = fmt::format(build_parameter_json, metric_type, dim, 20);
        auto createindex = vsag::Factory::CreateIndex("diskann", build_parameters);
        REQUIRE(createindex.has_value());
        auto index = createindex.value();
        auto buildindex = index->Build(base);
        REQUIRE(buildindex.has_value());
    }
}

/////////////////////////////////////////////////////////
// index->add
/////////////////////////////////////////////////////////
TEST_CASE("hnsw add test", "[ft][index][hnsw]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 8000;
    int64_t dim = 57;
    auto metric_type = "ip";

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto createindex = vsag::Factory::CreateIndex(
        "hnsw", fixtures::generate_hnsw_build_parameters_string(metric_type, dim));
    REQUIRE(createindex.has_value());
    auto index = createindex.value();

    // build index
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);

    index->Add(base);

    // check the number of vectors in index
    REQUIRE(index->GetNumElements() == num_vectors);
}

/////////////////////////////////////////////////////////
// index->search
/////////////////////////////////////////////////////////

TEST_CASE("hnsw float32 recall", "[ft][index][hnsw]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 104;
    auto metric_type = GENERATE("l2", "ip", "cosine");
    bool need_normalize = metric_type != std::string("cosine");

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim, need_normalize);
    auto createindex = vsag::Factory::CreateIndex(
        "hnsw", fixtures::generate_hnsw_build_parameters_string(metric_type, dim));
    REQUIRE(createindex.has_value());
    auto index = createindex.value();

    // build index
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto buildindex = index->Build(base);
    REQUIRE(buildindex.has_value());

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        }
    }
    )";

    float recall =
        fixtures::test_knn_recall(index, search_parameters, num_vectors, dim, ids, vectors);
    REQUIRE(recall > 0.99);

    float range_recall =
        fixtures::test_range_recall(index, search_parameters, num_vectors, dim, ids, vectors);
    REQUIRE(range_recall > 0.99);
}

TEST_CASE("index search distance", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    size_t num_vectors = 1000;
    size_t dim = 256;
    auto metric_type = GENERATE("ip", "cosine", "l2");
    auto algorithm = GENERATE("hnsw", "diskann");

    if (algorithm == std::string("diskann") and metric_type == std::string("cosine")) {
        return;  // TODO: support cosine for diskann
    }

    bool need_normalize = metric_type != std::string("cosine");
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim, need_normalize);

    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "hnsw": {{
            "max_degree": 24,
            "ef_construction": 200
        }},
        "diskann": {{
            "max_degree": 24,
            "ef_construction": 200,
            "pq_dims": 32,
            "pq_sample_rate": 1,
            "use_pq_search": true,
            "use_bsa": true
        }}
    }}
    )";

    auto build_parameter = fmt::format(build_parameter_json, metric_type, dim);

    auto createindex = vsag::Factory::CreateIndex(algorithm, build_parameter);
    REQUIRE(createindex.has_value());
    auto index = createindex.value();

    // build index
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    auto buildindex = index->Build(base);
    REQUIRE(buildindex.has_value());

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        },
        "diskann": {
            "ef_search": 100,
            "io_limit": 100,
            "beam_search": 4,
            "use_reorder": true
        }
    }
    )";

    for (int i = 0; i < num_vectors; ++i) {
        vsag::DatasetPtr query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + dim * i)->Owner(false);

        int64_t k = 10;
        float max_score = 0;

        auto result = index->KnnSearch(query, k, search_parameters);
        REQUIRE(result.has_value());
        auto knn_result = result.value();
        for (int j = 0; j < knn_result->GetDim(); ++j) {
            auto id = knn_result->GetIds()[j];
            float score = 0;
            if (metric_type == std::string("l2")) {
                score = vsag::L2Sqr(vectors.data() + dim * i, vectors.data() + dim * id, &dim);
            } else if (metric_type == std::string("ip")) {
                score = 1 - vsag::InnerProduct(
                                vectors.data() + dim * i, vectors.data() + dim * id, &dim);
            } else if (metric_type == std::string("cosine")) {
                float mold_query =
                    vsag::InnerProduct(vectors.data() + dim * i, vectors.data() + dim * i, &dim);
                float mold_base =
                    vsag::InnerProduct(vectors.data() + dim * id, vectors.data() + dim * id, &dim);
                score = 1 - vsag::InnerProduct(
                                vectors.data() + dim * i, vectors.data() + dim * id, &dim) /
                                std::sqrt(mold_query * mold_base);
            }
            fixtures::dist_t return_score = knn_result->GetDistances()[j];
            REQUIRE(return_score == score);
            max_score = score;
        }

        result = index->RangeSearch(query, max_score, search_parameters);
        REQUIRE(result.has_value());
        auto range_result = result.value();
        REQUIRE(range_result->GetDim() >= k);
        for (int j = 0; j < range_result->GetDim(); ++j) {
            auto id = range_result->GetIds()[j];
            float score = 0;
            if (metric_type == std::string("l2")) {
                score = vsag::L2Sqr(vectors.data() + dim * i, vectors.data() + dim * id, &dim);
            } else if (metric_type == std::string("ip")) {
                score = 1 - vsag::InnerProduct(
                                vectors.data() + dim * i, vectors.data() + dim * id, &dim);
            } else if (metric_type == std::string("cosine")) {
                float mold_query =
                    vsag::InnerProduct(vectors.data() + dim * i, vectors.data() + dim * i, &dim);
                float mold_base =
                    vsag::InnerProduct(vectors.data() + dim * id, vectors.data() + dim * id, &dim);
                score = 1 - vsag::InnerProduct(
                                vectors.data() + dim * i, vectors.data() + dim * id, &dim) /
                                std::sqrt(mold_query * mold_base);
            }

            fixtures::dist_t return_score = range_result->GetDistances()[j];
            REQUIRE(return_score == score);
        }

        auto search_parameters_no_reorder = R"(
        {
            "hnsw": {
                "ef_search": 100
            },
            "diskann": {
                "ef_search": 100,
                "io_limit": 100,
                "beam_search": 4
            }
        }
        )";
        result = index->RangeSearch(query, max_score, search_parameters_no_reorder);
        REQUIRE(result.has_value());
        auto range_upper_result = result.value();
        std::unordered_set<uint32_t> candidates_results;
        for (int j = 0; j < range_upper_result->GetDim(); ++j) {
            candidates_results.insert(range_upper_result->GetIds()[j]);
        }
        for (int j = 0; j < range_result->GetDim(); ++j) {
            auto iter = candidates_results.find(range_result->GetIds()[j]);
            REQUIRE(iter != candidates_results.end());
        }
    }
}

TEST_CASE("index get distance by id", "[ft][index][hnsw]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);
    int64_t num_vectors = 1000;
    int64_t dim = 104;
    auto metric_type = GENERATE("l2", "ip", "cosine");
    auto index_name = GENERATE("hnsw", "diskann");

    if (index_name == std::string("diskann") and metric_type == std::string("cosine")) {
        return;  // TODO: support cosine for diskann
    }

    bool need_normalize = metric_type != std::string("cosine");
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim, need_normalize);
    auto index =
        fixtures::generate_index(index_name, metric_type, num_vectors, dim, ids, vectors, false);

    if (index_name == std::string("hnsw")) {
        for (int i = 1; i < num_vectors; ++i) {
            int64_t id_index = i - 1;
            auto result = index->CalcDistanceById(vectors.data() + i * dim, ids[id_index]);
            float score = 0;
            if (metric_type == std::string("l2")) {
                score =
                    vsag::L2Sqr(vectors.data() + dim * i, vectors.data() + dim * id_index, &dim);
            } else if (metric_type == std::string("ip")) {
                score = 1 - vsag::InnerProduct(
                                vectors.data() + dim * i, vectors.data() + dim * id_index, &dim);
            } else if (metric_type == std::string("cosine")) {
                float mold_query =
                    vsag::InnerProduct(vectors.data() + dim * i, vectors.data() + dim * i, &dim);
                float mold_base = vsag::InnerProduct(
                    vectors.data() + dim * id_index, vectors.data() + dim * id_index, &dim);
                score = 1 - vsag::InnerProduct(
                                vectors.data() + dim * i, vectors.data() + dim * id_index, &dim) /
                                std::sqrt(mold_query * mold_base);
            }
            fixtures::dist_t return_score = result.value();
            REQUIRE(return_score == score);
        }
    } else {
        REQUIRE_THROWS(index->CalcDistanceById(vectors.data(), ids[0]));
    }
}

TEST_CASE("create two hnsw index in the same time", "[ft][index][hnsw]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 49;
    auto metric_type = GENERATE("l2", "ip");

    auto [ids1, vectors1] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto [ids2, vectors2] = fixtures::generate_ids_and_vectors(num_vectors, dim);

    // index1
    auto createindex1 = vsag::Factory::CreateIndex(
        "hnsw", fixtures::generate_hnsw_build_parameters_string(metric_type, dim));
    REQUIRE(createindex1.has_value());
    auto index1 = createindex1.value();
    // index2
    auto createindex2 = vsag::Factory::CreateIndex(
        "hnsw", fixtures::generate_hnsw_build_parameters_string(metric_type, dim));
    REQUIRE(createindex2.has_value());
    auto index2 = createindex2.value();

    auto base1 = vsag::Dataset::Make();
    base1->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids1.data())
        ->Float32Vectors(vectors1.data())
        ->Owner(false);
    auto buildindex1 = index1->Build(base1);
    REQUIRE(buildindex1.has_value());
    auto base2 = vsag::Dataset::Make();
    base2->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids2.data())
        ->Float32Vectors(vectors2.data())
        ->Owner(false);
    auto buildindex2 = index2->Build(base1);
    REQUIRE(buildindex2.has_value());

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        }
    }
    )";

    float recall1 =
        fixtures::test_knn_recall(index1, search_parameters, num_vectors, dim, ids1, vectors1);
    REQUIRE(recall1 > 0.99);
    float recall2 =
        fixtures::test_knn_recall(index2, search_parameters, num_vectors, dim, ids2, vectors2);
    REQUIRE(recall2 > 0.99);
}

/////////////////////////////////////////////////////////
// index->serialize/deserialize
/////////////////////////////////////////////////////////

TEST_CASE("serialize/deserialize with file stream", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 64;
    // auto index_name = GENERATE("hnsw", "diskann");
    auto index_name = GENERATE("hnsw");
    auto metric_type = GENERATE("l2", "ip", "cosine");

    bool need_normalize = metric_type != std::string("cosine");
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim, metric_type);
    auto index =
        fixtures::generate_index(index_name, metric_type, num_vectors, dim, ids, vectors, true);

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        },
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": false
        }
    }
    )";

    SECTION("successful case") {
        fixtures::temp_dir dir("test_index_serialize_via_stream");

        // serialize to file stream
        std::fstream out_file(dir.path + "index.bin", std::ios::out | std::ios::binary);
        REQUIRE(index->Serialize(out_file).has_value());
        out_file.close();

        // deserialize from file stream
        std::fstream in_file(dir.path + "index.bin", std::ios::in | std::ios::binary);
        auto new_index =
            vsag::Factory::CreateIndex(
                index_name,
                vsag::generate_build_parameters(metric_type, num_vectors, dim, true).value())
                .value();
        REQUIRE(new_index->Deserialize(in_file).has_value());

        // compare recall
        auto before_serialize_recall =
            fixtures::test_knn_recall(index, search_parameters, num_vectors, dim, ids, vectors);
        auto after_serialize_recall =
            fixtures::test_knn_recall(new_index, search_parameters, num_vectors, dim, ids, vectors);
        REQUIRE(before_serialize_recall == after_serialize_recall);
    }

    SECTION("less bits") {
        fixtures::temp_dir dir("test_index_serialize_via_stream");

        // serialize to file stream
        std::fstream out_file(dir.path + "index.bin", std::ios::out | std::ios::binary);
        REQUIRE(index->Serialize(out_file).has_value());
        int size = out_file.tellg();
        out_file.close();

        // deserialize from file stream
        std::filesystem::resize_file(dir.path + "index.bin", size - 10);
        std::fstream in_file(dir.path + "index.bin", std::ios::in | std::ios::binary);

        auto new_index =
            vsag::Factory::CreateIndex(
                index_name,
                vsag::generate_build_parameters(metric_type, num_vectors, dim, true).value())
                .value();
        REQUIRE(new_index->Deserialize(in_file).error().type == vsag::ErrorType::READ_ERROR);
    }

    SECTION("diskann invalid") {
        fixtures::temp_dir dir("test_index_serialize_via_stream");

        // serialize to file stream
        std::fstream out_file(dir.path + "index.bin", std::ios::out | std::ios::binary);
        REQUIRE(index->Serialize(out_file).has_value());
        int size = out_file.tellg();
        out_file.close();

        // deserialize from file stream
        std::filesystem::resize_file(dir.path + "index.bin", size - 10);
        std::fstream in_file(dir.path + "index.bin", std::ios::in | std::ios::binary);

        if (metric_type == std::string("cosine")) {
            return;
        }
        auto new_index =
            vsag::Factory::CreateIndex(
                "diskann",
                vsag::generate_build_parameters(metric_type, num_vectors, dim, true).value())
                .value();
        REQUIRE_THROWS(new_index->Deserialize(in_file));
        REQUIRE_THROWS(new_index->Serialize(in_file));
    }
}

TEST_CASE("serialize/deserialize hnswstatic with file stream", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 64;
    auto index_name = GENERATE("hnsw");
    auto metric_type = GENERATE("l2");  // hnswstatic does not support ip

    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": 64,
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 100,
            "use_static": true,
            "use_conjugate_graph": true
        }},
        "diskann": {{
            "max_degree": 16,
            "ef_construction": 200,
            "pq_dims": 32,
            "pq_sample_rate": 0.5
        }}
    }}
    )";
    auto build_parameters = fmt::format(build_parameter_json, metric_type, dim);

    auto index = vsag::Factory::CreateIndex(index_name, build_parameters).value();

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    REQUIRE(index->Build(base).has_value());

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        },
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": false
        }
    }
    )";

    SECTION("successful case") {
        fixtures::temp_dir dir("test_index_serialize_via_stream");

        // serialize to file stream
        std::fstream out_file(dir.path + "index.bin", std::ios::out | std::ios::binary);
        REQUIRE(index->Serialize(out_file).has_value());
        out_file.close();

        // deserialize from file stream
        std::fstream in_file(dir.path + "index.bin", std::ios::in | std::ios::binary);
        auto new_index = vsag::Factory::CreateIndex(index_name, build_parameters).value();
        REQUIRE(new_index->Deserialize(in_file).has_value());

        // compare recall
        auto before_serialize_recall =
            fixtures::test_knn_recall(index, search_parameters, num_vectors, dim, ids, vectors);
        auto after_serialize_recall =
            fixtures::test_knn_recall(new_index, search_parameters, num_vectors, dim, ids, vectors);

        REQUIRE(before_serialize_recall == after_serialize_recall);
    }

    SECTION("less bits") {
        fixtures::temp_dir dir("test_index_serialize_via_stream");

        // serialize to file stream
        std::fstream out_file(dir.path + "index.bin", std::ios::out | std::ios::binary);
        REQUIRE(index->Serialize(out_file).has_value());
        int size = out_file.tellg();
        out_file.close();

        // deserialize from file stream
        std::filesystem::resize_file(dir.path + "index.bin", size - 10);
        std::fstream in_file(dir.path + "index.bin", std::ios::in | std::ios::binary);

        auto new_index = vsag::Factory::CreateIndex(index_name, build_parameters).value();
        REQUIRE(new_index->Deserialize(in_file).error().type == vsag::ErrorType::READ_ERROR);
    }
}

TEST_CASE("search on a deserialized empty index", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 64;
    auto index_name = GENERATE("hnsw", "diskann");
    auto metric_type = GENERATE("l2", "ip", "cosine");

    if (index_name == std::string("diskann") and metric_type == std::string("cosine")) {
        return;  // TODO: support cosine for diskann
    }
    auto index =
        vsag::Factory::CreateIndex(
            index_name, vsag::generate_build_parameters(metric_type, num_vectors, dim).value())
            .value();

    auto base = vsag::Dataset::Make();
    REQUIRE(index->Build(base).has_value());

    auto serializeindex = index->Serialize();
    REQUIRE(serializeindex.has_value());

    auto bs = serializeindex.value();

    index = nullptr;
    index = vsag::Factory::CreateIndex(
                index_name, vsag::generate_build_parameters(metric_type, num_vectors, dim).value())
                .value();
    auto deserializeindex = index->Deserialize(bs);
    REQUIRE(deserializeindex.has_value());

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(1, dim);
    auto one_vector = vsag::Dataset::Make();
    one_vector->NumElements(1)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        },
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": false
        }
    }
    )";

    auto knnsearch = index->KnnSearch(one_vector, 10, search_parameters);
    REQUIRE(knnsearch.has_value());
    REQUIRE(knnsearch.value()->GetNumElements() == 1);
    REQUIRE(knnsearch.value()->GetDim() == 0);

    auto rangesearch = index->RangeSearch(one_vector, 10, search_parameters);
    REQUIRE(rangesearch.has_value());
    REQUIRE(rangesearch.value()->GetNumElements() == 1);
    REQUIRE(rangesearch.value()->GetDim() == 0);
}

TEST_CASE("remove vectors from the index", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);
    int64_t num_vectors = 1000;
    int64_t dim = 64;
    auto index_name = GENERATE("fresh_hnsw", "diskann");
    auto metric_type = GENERATE("cosine", "ip", "l2");

    if (index_name == std::string("diskann") and metric_type == std::string("cosine")) {
        return;  // TODO: support cosine for diskann
    }

    bool need_normalize = metric_type != std::string("cosine");
    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim, need_normalize);
    auto index = fixtures::generate_index(index_name, metric_type, num_vectors, dim, ids, vectors);

    constexpr auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        },
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": false
        }
    }
    )";

    if (index_name != std::string("diskann")) {  // index that supports remove
        // remove half data

        int correct = 0;
        for (int i = 0; i < num_vectors; i++) {
            auto query = vsag::Dataset::Make();
            query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + i * dim)->Owner(false);

            int64_t k = 10;
            auto result = index->KnnSearch(query, k, search_parameters);
            REQUIRE(result.has_value());
            if (result.value()->GetIds()[0] == ids[i]) {
                correct += 1;
            }
        }
        float recall_before = ((float)correct) / num_vectors;

        for (int i = 0; i < num_vectors / 2; ++i) {
            auto result = index->Remove(ids[i]);
            REQUIRE(result.has_value());
            REQUIRE(result.value());
        }
        auto wrong_result = index->Remove(-1);
        REQUIRE(wrong_result.has_value());
        REQUIRE_FALSE(wrong_result.value());

        REQUIRE(index->GetNumElements() == num_vectors / 2);

        // test recall for half data
        correct = 0;
        for (int i = 0; i < num_vectors; i++) {
            auto query = vsag::Dataset::Make();
            query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + i * dim)->Owner(false);

            int64_t k = 10;
            auto result = index->KnnSearch(query, k, search_parameters);
            REQUIRE(result.has_value());
            if (i < num_vectors / 2) {
                REQUIRE(result.value()->GetIds()[0] != ids[i]);
            } else {
                if (result.value()->GetIds()[0] == ids[i]) {
                    correct += 1;
                }
            }
        }
        float recall = ((float)correct) / (num_vectors / 2);
        REQUIRE(recall >= 0.98);

        // remove all data
        for (int i = num_vectors / 2; i < num_vectors; ++i) {
            auto result = index->Remove(i);
            REQUIRE(result.has_value());
            REQUIRE(result.value());
        }

        // add data into index again
        correct = 0;
        auto dataset = vsag::Dataset::Make();
        dataset->NumElements(num_vectors)
            ->Dim(dim)
            ->Float32Vectors(vectors.data())
            ->Ids(ids.data())
            ->Owner(false);
        auto result = index->Add(dataset);

        for (int i = 0; i < num_vectors; i++) {
            auto query = vsag::Dataset::Make();
            query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + i * dim)->Owner(false);

            int64_t k = 10;
            auto result = index->KnnSearch(query, k, search_parameters);
            REQUIRE(result.has_value());
            if (result.value()->GetIds()[0] == ids[i]) {
                correct += 1;
            }
        }
        float recall_after = ((float)correct) / num_vectors;
        REQUIRE(abs(recall_before - recall_after) < 0.001);
    } else {  // index that does not supports remove
        REQUIRE_THROWS(index->Remove(-1));
    }
}

TEST_CASE("index with bsa", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);
    int64_t num_vectors = 1000;
    int64_t dim = 128;
    auto index_name = GENERATE("diskann");
    auto metric_type = "l2";

    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 100
        }},
        "diskann": {{
            "max_degree": 16,
            "ef_construction": 100,
            "pq_dims": 32,
            "pq_sample_rate": 0.5,
            "use_pq_search": true
        }}
    }}
    )";
    auto build_parameters = fmt::format(build_parameter_json, metric_type, dim);
    auto index = vsag::Factory::CreateIndex(index_name, build_parameters).value();

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    REQUIRE(index->Build(base).has_value());

    auto search_parameters = R"(
    {
        "hnsw": {
            "ef_search": 100
        },
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": true,
            "use_bsa": true
        }
    }
    )";
    float recall =
        fixtures::test_knn_recall(index, search_parameters, num_vectors, dim, ids, vectors);
    REQUIRE(recall > 0.99);
}

TEST_CASE("io for diskann", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);
    int64_t num_vectors = 1000;
    int64_t dim = 128;
    auto index_name = GENERATE("diskann");
    auto use_async_io = true;
    auto use_bsa = GENERATE(true, false);
    auto use_reorder = GENERATE(true, false);

    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "l2",
        "dim": {},
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 100
        }},
        "diskann": {{
            "max_degree": 16,
            "ef_construction": 100,
            "pq_dims": 32,
            "pq_sample_rate": 0.5,
            "use_pq_search": true,
            "use_async_io": {}
        }}
    }}
    )";
    auto build_parameters = fmt::format(build_parameter_json, dim, use_async_io);
    auto index = vsag::Factory::CreateIndex(index_name, build_parameters).value();

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    REQUIRE(index->Build(base).has_value());

    constexpr auto search_parameters_json = R"(
    {{
        "hnsw": {{
            "ef_search": 100
        }},
        "diskann": {{
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": {},
            "use_bsa": {}
        }}
    }}
    )";
    auto search_parameters = fmt::format(search_parameters_json, use_reorder, use_bsa);
    float recall =
        fixtures::test_knn_recall(index, search_parameters, num_vectors, dim, ids, vectors);
    REQUIRE(recall > 0.99);
}

TEST_CASE("different parameter of io_limit", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);
    int64_t num_vectors = 1000;
    int64_t dim = 128;
    auto index_name = "diskann";
    auto use_async_io = GENERATE(true, false);
    auto use_bsa = true;
    auto io_limit = GENERATE(47, 193);
    auto use_reorder = true;

    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "l2",
        "dim": {},
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 100
        }},
        "diskann": {{
            "max_degree": 16,
            "ef_construction": 100,
            "pq_dims": 32,
            "pq_sample_rate": 0.5,
            "use_pq_search": true,
            "use_async_io": {}
        }}
    }}
    )";
    auto build_parameters = fmt::format(build_parameter_json, dim, use_async_io);
    auto index = vsag::Factory::CreateIndex(index_name, build_parameters).value();

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    REQUIRE(index->Build(base).has_value());

    constexpr auto search_parameters_json = R"(
    {{
        "hnsw": {{
            "ef_search": 100
        }},
        "diskann": {{
            "ef_search": 97,
            "beam_search": 4,
            "io_limit": {},
            "use_reorder": {},
            "use_bsa": {}
        }}
    }}
    )";
    auto search_parameters = fmt::format(search_parameters_json, io_limit, use_reorder, use_bsa);
    float recall =
        fixtures::test_knn_recall(index, search_parameters, num_vectors, dim, ids, vectors);
    REQUIRE(recall > 0.99);
}

/////////////////////////////////////////////////////////
// utility functions
/////////////////////////////////////////////////////////

TEST_CASE("check correct build parameters", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    auto json_string = R"(
    {
        "dtype": "float32",
        "metric_type": "l2",
        "dim": 512,
        "hnsw": {
            "max_degree": 16,
            "ef_construction": 100
        },
        "diskann": {
            "max_degree": 16,
            "ef_construction": 200,
            "pq_dims": 32,
            "pq_sample_rate": 0.5
        }
    }
    )";
    auto res = vsag::check_diskann_hnsw_build_parameters(json_string);
    REQUIRE(res.has_value());
}

TEST_CASE("check incorrect build parameters", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    // dtype is missing
    auto json_string = R"(
    {
        "metric_type": "l2",
        "dim": 512,
        "hnsw": {
            "max_degree": 16,
            "ef_construction": 100
        },
        "diskann": {
            "max_degree": 16,
            "ef_construction": 200,
            "pq_dims": 32,
            "pq_sample_rate": 0.5
        }
    }
    )";
    auto res = vsag::check_diskann_hnsw_build_parameters(json_string);
    REQUIRE_FALSE(res.has_value());
    REQUIRE(res.error().type == vsag::ErrorType::INVALID_ARGUMENT);
}

TEST_CASE("check correct search parameters", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    auto json_string = R"(
        {
            "hnsw": {
                "ef_search": 100
            },
            "diskann": {
                "ef_search": 200,
                "beam_search": 4,
                "io_limit": 200,
                "use_reorder": true
           }
        }
        )";
    auto res = vsag::check_diskann_hnsw_search_parameters(json_string);
    REQUIRE(res.has_value());
}

TEST_CASE("check incorrect search parameters", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    auto json_string = R"(
        {
            "hhhhhhhhhhhhhh": {
                "ef_search": 100
            },
            "diskann": {
                "ef_search": 200,
                "beam_search": 4,
                "io_limit": 200,
                "use_reorder": true
           }
        }
        )";
    auto res = vsag::check_diskann_hnsw_search_parameters(json_string);
    REQUIRE_FALSE(res.has_value());
    REQUIRE(res.error().type == vsag::ErrorType::INVALID_ARGUMENT);
}

TEST_CASE("generate build parameters", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    auto metric_type = GENERATE("l2", "IP");
    auto num_elements = GENERATE(1'000'000,
                                 2'000'000,
                                 3'000'000,
                                 4'000'000,
                                 5'000'000,
                                 6'000'000,
                                 7'000'000,
                                 8'000'000,
                                 9'000'000,
                                 10'000'000,
                                 11'000'000);
    auto dim = GENERATE(32, 64, 96, 128, 256, 512, 768, 1024, 1536, 2048, 4096);

    auto parameters = vsag::generate_build_parameters(metric_type, num_elements, dim);

    REQUIRE(parameters.has_value());
    auto json = nlohmann::json::parse(parameters.value());
    REQUIRE(json["dim"] == dim);
    REQUIRE(json["diskann"]["pq_dims"] == dim / 4);
}

TEST_CASE("estimate search cost", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    constexpr auto search_parameters_json = R"(
    {{
        "hnsw": {{
            "ef_search": {}
        }}
    }}
    )";

    SECTION("small dataset") {
        auto dim = 128;
        auto ef_search = 100;
        auto data_num = 1'000'000;
        auto search_parameters = fmt::format(search_parameters_json, ef_search);
        auto result = vsag::estimate_search_time("hnsw", data_num, dim, search_parameters);
        REQUIRE(result.has_value());
        REQUIRE(fixtures::time_t(result.value()) == 1);
    }

    SECTION("large dataset") {
        auto dim = 512;
        auto ef_search = 300;
        auto data_num = 10'000'000;
        auto search_parameters = fmt::format(search_parameters_json, ef_search);
        auto result = vsag::estimate_search_time("hnsw", data_num, dim, search_parameters);
        REQUIRE(result.has_value());
        REQUIRE(fixtures::time_t(result.value()) == 24);
    }

    SECTION("unsupported index operation") {
        auto dim = 512;
        auto ef_search = 300;
        auto data_num = 10'000'000;
        auto search_parameters = fmt::format(search_parameters_json, ef_search);
        auto result = vsag::estimate_search_time("diskann", data_num, dim, search_parameters);
        REQUIRE(result.error().type == vsag::ErrorType::UNSUPPORTED_INDEX_OPERATION);
    }

    SECTION("invalid argument") {
        auto dim = 512;
        auto data_num = 10'000'000;
        auto search_parameters = fmt::format(search_parameters_json, "\"hhhhh\"");
        auto result = vsag::estimate_search_time("hnsw", data_num, dim, search_parameters);
        REQUIRE(result.error().type == vsag::ErrorType::INVALID_ARGUMENT);
    }
}

TEST_CASE("generate build parameters with invalid num_elements", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    auto metric_type = GENERATE("l2", "IP");
    auto num_elements = GENERATE(-1'000'000, -1, 0, 17'000'001, 1'000'000'000);
    int64_t dim = 128;

    auto parameters = vsag::generate_build_parameters(metric_type, num_elements, dim);

    REQUIRE(not parameters.has_value());
    REQUIRE(parameters.error().type == vsag::ErrorType::INVALID_ARGUMENT);
}

TEST_CASE("generate build parameters with invalid dim", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    auto metric_type = GENERATE("l2", "IP");
    int64_t num_elements = 1'000'000;
    int64_t dim = GENERATE(1, 3, 42, 61, 90);

    auto parameters = vsag::generate_build_parameters(metric_type, num_elements, dim);

    REQUIRE(not parameters.has_value());
    REQUIRE(parameters.error().type == vsag::ErrorType::INVALID_ARGUMENT);
}

TEST_CASE("build index with generated_build_parameters", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);

    int64_t num_vectors = 1000;
    int64_t dim = 64;

    auto index = vsag::Factory::CreateIndex(
                     "hnsw", vsag::generate_build_parameters("l2", num_vectors, dim).value())
                     .value();

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);

    auto base = vsag::Dataset::Make();
    base->NumElements(num_vectors)
        ->Dim(dim)
        ->Ids(ids.data())
        ->Float32Vectors(vectors.data())
        ->Owner(false);
    REQUIRE(index->Build(base).has_value());

    auto search_parameters = R"(
    {
	"hnsw": {
	    "ef_search": 100
	},
	"diskann": {
	    "ef_search": 100,
	    "beam_search": 4,
	    "io_limit": 100,
	    "use_reorder": false
	}
    }
    )";

    int64_t correct = 0;
    for (int64_t i = 0; i < num_vectors; ++i) {
        auto query = vsag::Dataset::Make();
        query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data() + i * dim)->Owner(false);
        auto result = index->KnnSearch(query, 10, search_parameters).value();
        for (int64_t j = 0; j < result->GetDim(); ++j) {
            if (i == result->GetIds()[j]) {
                ++correct;
                break;
            }
        }
    }

    float recall = 1.0 * correct / num_vectors;
    std::cout << "recall: " << recall << std::endl;
    REQUIRE(recall > 0.95);
}

TEST_CASE("hnsw + feedback with global optimum id", "[ft][index][hnsw]") {
    auto logger = vsag::Options::Instance().logger();
    logger->SetLevel(vsag::Logger::Level::kDEBUG);

    // parameters
    int dim = 128;
    int num_base = 1000;
    int num_query = 1000;
    int64_t k = 10;
    auto metric_type = GENERATE("l2");
    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 200,
            "use_conjugate_graph": true
        }}
    }}
    )";
    auto build_parameter = fmt::format(build_parameter_json, metric_type, dim);

    // create index
    auto createindex = vsag::Factory::CreateIndex("hnsw", build_parameter);
    REQUIRE(createindex.has_value());
    auto index = createindex.value();

    // generate dataset
    auto [base_ids, base_vectors] = fixtures::generate_ids_and_vectors(num_base, dim);
    auto base = vsag::Dataset::Make();
    auto queries = vsag::Dataset::Make();
    base->NumElements(num_base)
        ->Dim(dim)
        ->Ids(base_ids.data())
        ->Float32Vectors(base_vectors.data())
        ->Owner(false);

    auto [query_ids, query_vectors] = fixtures::generate_ids_and_vectors(num_query, dim);
    queries->NumElements(num_query)
        ->Dim(dim)
        ->Ids(query_ids.data())
        ->Float32Vectors(query_vectors.data())
        ->Owner(false);

    // build index
    auto buildindex = index->Build(base);
    REQUIRE(buildindex.has_value());

    // train and search
    float recall[2];
    int correct;
    uint32_t error_fix = 0;
    bool use_conjugate_graph_search = false;
    for (int round = 0; round < 2; round++) {
        correct = 0;

        if (round == 0) {
            logger->Debug("====train stage====");
        } else {
            logger->Debug("====test stage====");
        }

        logger->Debug(fmt::format(R"(Memory Usage: {:.3f} KB)", index->GetMemoryUsage() / 1024.0));

        use_conjugate_graph_search = (round != 0);
        constexpr auto search_parameters_json = R"(
        {{
            "hnsw": {{
                "ef_search": 100,
                "use_conjugate_graph_search": {}
            }}
        }}
        )";
        auto search_parameters = fmt::format(search_parameters_json, use_conjugate_graph_search);

        for (int i = 0; i < num_query; i++) {
            auto query = vsag::Dataset::Make();
            query->Dim(dim)
                ->Float32Vectors(queries->GetFloat32Vectors() + i * dim)
                ->NumElements(1)
                ->Owner(false);

            auto result = index->KnnSearch(query, k, search_parameters);
            REQUIRE(result.has_value());
            auto bf_result = fixtures::brute_force(query, base, 1, metric_type);
            int64_t global_optimum = bf_result->GetIds()[0];
            int64_t local_optimum = result.value()->GetIds()[0];

            if (local_optimum != global_optimum and round == 0) {
                error_fix += *index->Feedback(query, k, search_parameters, global_optimum);
                REQUIRE(*index->Feedback(query, k, search_parameters) == 0);
            }

            if (local_optimum == global_optimum) {
                correct++;
            }
        }
        recall[round] = correct / (1.0 * num_query);
        logger->Debug(fmt::format(R"(Recall: {:.4f})", recall[round]));
    }

    logger->Debug("====summary====");
    logger->Debug(fmt::format(R"(Error fix: {})", error_fix));

    REQUIRE(fixtures::time_t(recall[1]) == 1.0f);
}

TEST_CASE("static hnsw + feedback without global optimum id", "[ft][index][hnsw]") {
    auto logger = vsag::Options::Instance().logger();
    logger->SetLevel(vsag::Logger::Level::kDEBUG);

    // parameters
    int dim = 128;
    int num_base = 1000;
    int num_query = 1000;
    auto metric_type = GENERATE("l2");
    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 200,
            "use_conjugate_graph": true,
            "use_static": true
        }}
    }}
    )";
    auto build_parameter = fmt::format(build_parameter_json, metric_type, dim);

    // create index
    auto createindex = vsag::Factory::CreateIndex("hnsw", build_parameter);
    REQUIRE(createindex.has_value());
    auto index = createindex.value();

    // generate dataset
    auto [base_ids, base_vectors] = fixtures::generate_ids_and_vectors(num_base, dim);
    auto base = vsag::Dataset::Make(), queries = vsag::Dataset::Make();
    base->NumElements(num_base)
        ->Dim(dim)
        ->Ids(base_ids.data())
        ->Float32Vectors(base_vectors.data())
        ->Owner(false);

    auto [query_ids, query_vectors] = fixtures::generate_ids_and_vectors(num_query, dim);
    queries->NumElements(num_query)
        ->Dim(dim)
        ->Ids(query_ids.data())
        ->Float32Vectors(query_vectors.data())
        ->Owner(false);

    // build index
    auto buildindex = index->Build(base);
    REQUIRE(buildindex.has_value());

    // train and search
    float recall[2];
    int correct;
    uint32_t error_fix = 0;
    bool use_conjugate_graph_search = false;
    for (int round = 0; round < 2; round++) {
        correct = 0;

        if (round == 0) {
            logger->Debug("====train stage====");
        } else {
            logger->Debug("====test stage====");
        }

        logger->Debug(fmt::format(R"(Memory Usage: {:.3f} KB)", index->GetMemoryUsage() / 1024.0));

        use_conjugate_graph_search = (round != 0);
        constexpr auto search_parameters_json = R"(
        {{
            "hnsw": {{
                "ef_search": 100,
                "use_conjugate_graph_search": {}
            }}
        }}
        )";
        auto search_parameters = fmt::format(search_parameters_json, use_conjugate_graph_search);

        for (int i = 0; i < num_query; i++) {
            auto query = vsag::Dataset::Make();
            query->Dim(dim)
                ->Float32Vectors(queries->GetFloat32Vectors() + i * dim)
                ->NumElements(1)
                ->Owner(false);

            auto result = index->KnnSearch(query, 1, search_parameters);
            REQUIRE(result.has_value());
            auto bf_result = fixtures::brute_force(query, base, 1, metric_type);
            int64_t global_optimum = bf_result->GetIds()[0];
            int64_t local_optimum = result.value()->GetIds()[0];

            if (local_optimum != global_optimum and round == 0) {
                error_fix += *index->Feedback(query, 1, search_parameters);
                REQUIRE(*index->Feedback(query, 1, search_parameters, global_optimum) == 0);
            }

            if (local_optimum == global_optimum) {
                correct++;
            }
        }

        recall[round] = correct / (1.0 * num_query);
        logger->Debug(fmt::format(R"(Recall: {:.4f})", recall[round]));
    }

    logger->Debug("====summary====");
    logger->Debug(fmt::format(R"(Error fix: {})", error_fix));

    REQUIRE(std::fabs(recall[1] - 1.0) < 1e-7);
}

TEST_CASE("using indexes that do not support conjugate graph", "[ft][index]") {
    vsag::Options::Instance().logger()->SetLevel(vsag::Logger::Level::kDEBUG);
    int64_t num_vectors = 1000;
    int64_t dim = 64;
    auto index_name = GENERATE("diskann");
    auto metric_type = GENERATE("l2");

    auto [ids, vectors] = fixtures::generate_ids_and_vectors(num_vectors, dim);
    auto index = fixtures::generate_index(index_name, metric_type, num_vectors, dim, ids, vectors);

    auto search_parameters = R"(
    {
        "diskann": {
            "ef_search": 100,
            "beam_search": 4,
            "io_limit": 100,
            "use_reorder": false
        }
    }
    )";
    auto query = vsag::Dataset::Make();
    query->NumElements(1)->Dim(dim)->Float32Vectors(vectors.data())->Owner(false);
    int64_t k = 10;
    std::vector<int64_t> base_tag_ids;

    REQUIRE_THROWS(index->Feedback(query, k, search_parameters, -1));
    REQUIRE_THROWS(index->Feedback(query, k, search_parameters));

    REQUIRE_THROWS(index->Pretrain(base_tag_ids, k, search_parameters));
}

TEST_CASE("hnsw with pretrained by conjugate graph", "[ft][index][hnsw]") {
    auto logger = vsag::Options::Instance().logger();
    logger->SetLevel(vsag::Logger::Level::kDEBUG);

    // parameters
    int dim = 128;
    int base_elements = 10000;
    int query_elements = 1000;
    int ef_search = 10;
    int64_t k = 10;
    auto metric_type = GENERATE("l2");
    std::set<int64_t> failed_base_set;
    constexpr auto search_parameters_json = R"(
        {{
            "hnsw": {{
                "ef_search": {},
                "use_conjugate_graph_search": true
            }}
        }}
        )";
    auto search_parameters = fmt::format(search_parameters_json, ef_search);
    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "hnsw": {{
            "max_degree": 16,
            "ef_construction": 200,
            "use_conjugate_graph": true,
            "use_static": true
        }}
    }}
    )";
    auto build_parameter = fmt::format(build_parameter_json, metric_type, dim);

    // generate data (use base[0: query_num] as query)
    auto base = vsag::Dataset::Make();
    auto query = vsag::Dataset::Make();
    std::shared_ptr<int64_t[]> base_ids(new int64_t[base_elements]);
    std::shared_ptr<float[]> base_data(new float[dim * base_elements]);
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distribution_real(-1, 1);
    for (int i = 0; i < base_elements; i++) {
        base_ids[i] = i;

        for (int d = 0; d < dim; d++) {
            base_data[d + i * dim] = distribution_real(rng);
        }
    }
    base->Dim(dim)
        ->NumElements(base_elements)
        ->Ids(base_ids.get())
        ->Float32Vectors(base_data.get())
        ->Owner(false);
    query->Dim(dim)->NumElements(1)->Owner(false);

    // Create index
    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", build_parameter);
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Build index
    {
        auto build_result = hnsw->Build(base);
        REQUIRE(build_result.has_value());
    }

    // Search without empty conjugate graph
    {
        int correct = 0;
        logger->Debug("====Search Stage====");
        logger->Debug(fmt::format("Memory Usage: {:.3f} KB", hnsw->GetMemoryUsage() / 1024.0));

        for (int i = 0; i < query_elements; i++) {
            query->Float32Vectors(base_data.get() + i * dim);

            auto result = hnsw->KnnSearch(query, k, search_parameters);
            int64_t global_optimum = i;  // global optimum is itself
            int64_t local_optimum = result.value()->GetIds()[0];

            if (local_optimum != global_optimum) {
                failed_base_set.emplace(global_optimum);
            }

            if (local_optimum == global_optimum) {
                correct++;
            }
        }
        logger->Debug(fmt::format("Recall: {:.4f}", correct / (1.0 * query_elements)));
    }

    // Pretrain
    {
        logger->Debug("====Pretrain Stage====");
        logger->Debug(fmt::format("Before Pretrain, Memory Usage: {:.3f} KB",
                                  hnsw->GetMemoryUsage() / 1024.0));
        std::vector<int64_t> failed_base_vec(failed_base_set.begin(), failed_base_set.end());
        REQUIRE(hnsw->Pretrain(failed_base_vec, k, search_parameters).has_value());
        logger->Debug(fmt::format("After Pretrain, Memory Usage: {:.3f} KB",
                                  hnsw->GetMemoryUsage() / 1024.0));
    }

    // Search with pretrained conjugate graph
    {
        int correct = 0;
        logger->Debug("====Enhanced Search Stage====");
        logger->Debug(fmt::format("Memory Usage: {:.3f} KB", hnsw->GetMemoryUsage() / 1024.0));

        for (int i = 0; i < query_elements; i++) {
            query->Float32Vectors(base_data.get() + i * dim);

            auto result = hnsw->KnnSearch(query, k, search_parameters);
            int64_t global_optimum = i;  // global optimum is itself
            int64_t local_optimum = result.value()->GetIds()[0];

            if (local_optimum == global_optimum) {
                correct++;
            }
        }
        logger->Debug(fmt::format("Enhanced Recall: {:.4f}", correct / (1.0 * query_elements)));

        fixtures::recall_t recall = 1.0f * correct / query_elements;
        REQUIRE(recall == 1.0);
    }
}

TEST_CASE("HNSW filtered search", "[ft][index][hnsw]") {
    auto logger = vsag::Options::Instance().logger();
    logger->SetLevel(vsag::Logger::Level::kDEBUG);

    // Params
    int dim = 17;
    int max_elements = 1000;
    int max_degree = 16;
    int ef_construction = 100;
    int ef_search = max_elements;
    auto metric_type = GENERATE("l2");
    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "hnsw": {{
            "max_degree": {},
            "ef_construction": {}
        }}
    }}
    )";
    auto build_parameter =
        fmt::format(build_parameter_json, metric_type, dim, max_degree, ef_construction);
    constexpr auto search_parameters_json = R"(
        {{
            "hnsw": {{
                "ef_search": {}
            }}
        }}
        )";
    auto search_parameters = fmt::format(search_parameters_json, ef_search);

    // Build index
    std::shared_ptr<vsag::Index> hnsw;
    auto index = vsag::Factory::CreateIndex("hnsw", build_parameter);
    REQUIRE(index.has_value());
    hnsw = index.value();

    // Generate random data
    std::mt19937 rng(47);
    std::uniform_real_distribution<> distrib_real;
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];

    for (int64_t i = 0; i < max_elements; i++) {
        ids[i] = i;
        for (int d = 0; d < dim; d++) {
            data[d + i * dim] = distrib_real(rng);
        }
    }

    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);
    hnsw->Build(dataset);
    REQUIRE(hnsw->GetNumElements() == max_elements);

    // Tests
    auto query = vsag::Dataset::Make();
    int64_t k = max_elements;
    float radius = 100000;

    SECTION("no filter") {
        std::function<bool(int64_t)> null_func;
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = hnsw->KnnSearch(query, k, search_parameters);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == k);
            }

            {  // range search
                auto result = hnsw->RangeSearch(query, radius, search_parameters, -1);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }
        }
    }

    SECTION("valid functional filter") {
        auto filter = [](int64_t id) -> bool { return (id % 2 == 0); };
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = hnsw->KnnSearch(query, k, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements / 2);
                for (int j = 0; j < result.value()->GetDim(); j++) {
                    REQUIRE(not filter(result.value()->GetIds()[j]));
                }
            }

            {  // range search
                auto result = hnsw->RangeSearch(query, radius, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements / 2);
                for (int j = 0; j < result.value()->GetDim(); j++) {
                    REQUIRE(not filter(result.value()->GetIds()[j]));
                }
            }
        }
    }

    SECTION("null functional filter") {
        std::function<bool(int64_t)> null_func;
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = hnsw->KnnSearch(query, k, search_parameters, null_func);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == k);
            }

            {  // range search
                auto result = hnsw->RangeSearch(query, radius, search_parameters, null_func);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }
        }
    }

    SECTION("valid bitset filter") {
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            vsag::BitsetPtr filter = vsag::Bitset::Random(max_elements);
            int64_t num_deleted = filter->Count();

            {  // knn search
                auto result = hnsw->KnnSearch(query, k, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements - num_deleted);
                for (int64_t j = 0; j < result.value()->GetDim(); ++j) {
                    // deleted ids NOT in result
                    REQUIRE(filter->Test(result.value()->GetIds()[j] & 0xFFFFFFFFLL) == false);
                }
            }

            {  // range search
                auto result = hnsw->RangeSearch(query, radius, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements - num_deleted);
                for (int64_t j = 0; j < result.value()->GetDim(); ++j) {
                    // deleted ids NOT in result
                    REQUIRE(filter->Test(result.value()->GetIds()[j] & 0xFFFFFFFFLL) == false);
                }
            }
        }
    }

    SECTION("null bitset filter") {
        vsag::BitsetPtr filter = nullptr;
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = hnsw->KnnSearch(query, k, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }

            {  // range search
                auto result = hnsw->KnnSearch(query, radius, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }
        }
    }
}

TEST_CASE("DiskAnn filtered knn search", "[ft][index][diskann]") {
    auto logger = vsag::Options::Instance().logger();
    logger->SetLevel(vsag::Logger::Level::kDEBUG);

    // Params
    int dim = 65;
    int max_elements = 1000;
    int max_degree = 16;
    int ef_construction = 200;
    float pq_sample_rate = 0.5;
    int pq_dims = 9;
    int ef_search = 500;
    int beam_search = 4;
    int io_limit = 200;
    int k = 100;
    float radius = 100000;
    auto metric_type = GENERATE("l2");
    constexpr auto build_parameter_json = R"(
    {{
        "dtype": "float32",
        "metric_type": "{}",
        "dim": {},
        "diskann": {{
            "max_degree": {},
            "ef_construction": {},
            "pq_sample_rate": {},
            "pq_dims": {},
            "use_pq_search": {}
        }}
    }}
    )";
    auto build_parameter = fmt::format(build_parameter_json,
                                       metric_type,
                                       dim,
                                       max_degree,
                                       ef_construction,
                                       pq_sample_rate,
                                       pq_dims,
                                       true);
    constexpr auto search_parameters_json = R"(
        {{
            "diskann": {{
                "ef_search": {},
                "beam_search": {},
                "io_limit": {}
            }}
        }}
        )";
    auto search_parameters = fmt::format(search_parameters_json, ef_search, beam_search, io_limit);

    // Generate random data
    std::mt19937 rng;
    rng.seed(47);
    std::uniform_real_distribution<> distrib_real;
    std::uniform_int_distribution<> ids_random(0, max_elements - 1);
    int64_t* ids = new int64_t[max_elements];
    float* data = new float[dim * max_elements];
    for (int64_t i = 0; i < max_elements; i++) {
        ids[i] = i;
        for (int d = 0; d < dim; d++) {
            data[d + i * dim] = distrib_real(rng);
        }
    }
    auto dataset = vsag::Dataset::Make();
    dataset->Dim(dim)->NumElements(max_elements)->Ids(ids)->Float32Vectors(data);

    // Build index
    std::shared_ptr<vsag::Index> diskann;
    auto index = vsag::Factory::CreateIndex("diskann", build_parameter);
    REQUIRE(index.has_value());
    diskann = index.value();
    diskann->Build(dataset);
    REQUIRE(max_elements == diskann->GetNumElements());

    // Tests
    auto query = vsag::Dataset::Make();
    SECTION("valid functional filter") {
        auto filter = [](int64_t id) -> bool { return (id % 2 == 0); };
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = diskann->KnnSearch(query, k, search_parameters, filter);
                REQUIRE(result.has_value());
                for (int j = 0; j < result.value()->GetDim(); j++) {
                    REQUIRE(not filter(result.value()->GetIds()[j]));
                }
            }

            {  // range search
                auto result = diskann->RangeSearch(query, radius, search_parameters, filter);
                REQUIRE(result.has_value());
                for (int j = 0; j < result.value()->GetDim(); j++) {
                    REQUIRE(not filter(result.value()->GetIds()[j]));
                }
            }
        }
    }

    SECTION("no filter") {
        std::function<bool(int64_t)> null_func;
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = diskann->KnnSearch(query, k, search_parameters);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == k);
            }

            {  // range search
                auto result = diskann->RangeSearch(query, radius, search_parameters, -1);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }
        }
    }

    SECTION("null functional filter") {
        std::function<bool(int64_t)> null_func;
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = diskann->KnnSearch(query, k, search_parameters, null_func);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == k);
            }

            {  // range search
                auto result = diskann->RangeSearch(query, radius, search_parameters, null_func);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }
        }
    }

    SECTION("valid bitset filter") {
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            vsag::BitsetPtr filter = vsag::Bitset::Random(max_elements);
            int64_t num_deleted = filter->Count();

            {  // knn search
                auto result = diskann->KnnSearch(query, k, search_parameters, filter);
                REQUIRE(result.has_value());
                for (int64_t j = 0; j < result.value()->GetDim(); ++j) {
                    // deleted ids NOT in result
                    REQUIRE(filter->Test(result.value()->GetIds()[j] & 0xFFFFFFFFLL) == false);
                }
            }

            {  // range search
                auto result = diskann->RangeSearch(query, radius, search_parameters, filter);
                REQUIRE(result.has_value());
                for (int64_t j = 0; j < result.value()->GetDim(); ++j) {
                    // deleted ids NOT in result
                    REQUIRE(filter->Test(result.value()->GetIds()[j] & 0xFFFFFFFFLL) == false);
                }
            }
        }
    }

    SECTION("null bitset filter") {
        vsag::BitsetPtr filter = nullptr;
        for (int i = 0; i < max_elements; i++) {
            query->NumElements(1)->Dim(dim)->Float32Vectors(data + i * dim)->Owner(false);

            {  // knn search
                auto result = diskann->KnnSearch(query, k, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == k);
            }

            {  // range search
                auto result = diskann->RangeSearch(query, radius, search_parameters, filter);
                REQUIRE(result.has_value());
                REQUIRE(result.value()->GetDim() == max_elements);
            }
        }
    }
}
