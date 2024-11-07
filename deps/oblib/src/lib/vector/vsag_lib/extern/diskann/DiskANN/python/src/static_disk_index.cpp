// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "static_disk_index.h"

#include "pybind11/numpy.h"

namespace diskannpy
{

template <typename DT>
StaticDiskIndex<DT>::StaticDiskIndex(const diskann::Metric metric, const std::string &index_path_prefix,
                                     const uint32_t num_threads, const size_t num_nodes_to_cache,
                                     const uint32_t cache_mechanism)
    : _reader(std::make_shared<PlatformSpecificAlignedFileReader>()), _index(_reader, metric)
{
    int load_success = _index.load(num_threads, index_path_prefix.c_str());
    if (load_success != 0)
    {
        throw std::runtime_error("index load failed.");
    }
    if (cache_mechanism == 1)
    {
        std::string sample_file = index_path_prefix + std::string("_sample_data.bin");
        cache_sample_paths(num_nodes_to_cache, sample_file, num_threads);
    }
    else if (cache_mechanism == 2)
    {
        cache_bfs_levels(num_nodes_to_cache);
    }
}

template <typename DT> void StaticDiskIndex<DT>::cache_bfs_levels(const size_t num_nodes_to_cache)
{
    std::vector<uint32_t> node_list;
    _index.cache_bfs_levels(num_nodes_to_cache, node_list);
    _index.load_cache_list(node_list);
}

template <typename DT>
void StaticDiskIndex<DT>::cache_sample_paths(const size_t num_nodes_to_cache, const std::string &warmup_query_file,
                                             const uint32_t num_threads)
{
    if (!file_exists(warmup_query_file))
    {
        return;
    }

    std::vector<uint32_t> node_list;
    _index.generate_cache_list_from_sample_queries(warmup_query_file, 15, 4, num_nodes_to_cache, num_threads,
                                                   node_list);
    _index.load_cache_list(node_list);
}

template <typename DT>
NeighborsAndDistances<StaticIdType> StaticDiskIndex<DT>::search(
    py::array_t<DT, py::array::c_style | py::array::forcecast> &query, const uint64_t knn, const uint64_t complexity,
    const uint64_t beam_width)
{
    py::array_t<StaticIdType> ids(knn);
    py::array_t<float> dists(knn);

    std::vector<uint32_t> u32_ids(knn);
    std::vector<uint64_t> u64_ids(knn);
    diskann::QueryStats stats;

    _index.cached_beam_search(query.data(), knn, complexity, u64_ids.data(), dists.mutable_data(), beam_width, false,
                              &stats);

    auto r = ids.mutable_unchecked<1>();
    for (uint64_t i = 0; i < knn; ++i)
        r(i) = (unsigned)u64_ids[i];

    return std::make_pair(ids, dists);
}

template <typename DT>
NeighborsAndDistances<StaticIdType> StaticDiskIndex<DT>::batch_search(
    py::array_t<DT, py::array::c_style | py::array::forcecast> &queries, const uint64_t num_queries, const uint64_t knn,
    const uint64_t complexity, const uint64_t beam_width, const uint32_t num_threads)
{
    py::array_t<StaticIdType> ids({num_queries, knn});
    py::array_t<float> dists({num_queries, knn});

    omp_set_num_threads(num_threads);

    std::vector<uint64_t> u64_ids(knn * num_queries);

#pragma omp parallel for schedule(dynamic, 1) default(none)                                                            \
    shared(num_queries, queries, knn, complexity, u64_ids, dists, beam_width)
    for (int64_t i = 0; i < (int64_t)num_queries; i++)
    {
        _index.cached_beam_search(queries.data(i), knn, complexity, u64_ids.data() + i * knn, dists.mutable_data(i),
                                  beam_width);
    }

    auto r = ids.mutable_unchecked();
    for (uint64_t i = 0; i < num_queries; ++i)
        for (uint64_t j = 0; j < knn; ++j)
            r(i, j) = (uint32_t)u64_ids[i * knn + j];

    return std::make_pair(ids, dists);
}

template class StaticDiskIndex<float>;
template class StaticDiskIndex<uint8_t>;
template class StaticDiskIndex<int8_t>;
} // namespace diskannpy