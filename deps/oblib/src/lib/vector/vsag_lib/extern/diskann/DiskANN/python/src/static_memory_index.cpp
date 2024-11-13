// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include "static_memory_index.h"

#include "pybind11/numpy.h"

namespace diskannpy
{

template <class DT>
diskann::Index<DT, StaticIdType, filterT> static_index_builder(const diskann::Metric m, const size_t num_points,
                                                               const size_t dimensions,
                                                               const uint32_t initial_search_complexity)
{
    if (initial_search_complexity == 0)
    {
        throw std::runtime_error("initial_search_complexity must be a positive uint32_t");
    }

    return diskann::Index<DT>(m, dimensions, num_points,
                              false, // not a dynamic_index
                              false, // no enable_tags/ids
                              false, // no concurrent_consolidate,
                              false, // pq_dist_build
                              0,     // num_pq_chunks
                              false, // use_opq = false
                              0);    // num_frozen_points
}

template <class DT>
StaticMemoryIndex<DT>::StaticMemoryIndex(const diskann::Metric m, const std::string &index_prefix,
                                         const size_t num_points, const size_t dimensions, const uint32_t num_threads,
                                         const uint32_t initial_search_complexity)
    : _index(static_index_builder<DT>(m, num_points, dimensions, initial_search_complexity))
{
    const uint32_t _num_threads = num_threads != 0 ? num_threads : omp_get_num_threads();
    _index.load(index_prefix.c_str(), _num_threads, initial_search_complexity);
}

template <typename DT>
NeighborsAndDistances<StaticIdType> StaticMemoryIndex<DT>::search(
    py::array_t<DT, py::array::c_style | py::array::forcecast> &query, const uint64_t knn, const uint64_t complexity)
{
    py::array_t<StaticIdType> ids(knn);
    py::array_t<float> dists(knn);
    std::vector<DT *> empty_vector;
    _index.search(query.data(), knn, complexity, ids.mutable_data(), dists.mutable_data());
    return std::make_pair(ids, dists);
}

template <typename DT>
NeighborsAndDistances<StaticIdType> StaticMemoryIndex<DT>::batch_search(
    py::array_t<DT, py::array::c_style | py::array::forcecast> &queries, const uint64_t num_queries, const uint64_t knn,
    const uint64_t complexity, const uint32_t num_threads)
{
    const uint32_t _num_threads = num_threads != 0 ? num_threads : omp_get_num_threads();
    py::array_t<StaticIdType> ids({num_queries, knn});
    py::array_t<float> dists({num_queries, knn});
    std::vector<DT *> empty_vector;

    omp_set_num_threads(static_cast<int32_t>(_num_threads));

#pragma omp parallel for schedule(dynamic, 1) default(none) shared(num_queries, queries, knn, complexity, ids, dists)
    for (int64_t i = 0; i < (int64_t)num_queries; i++)
    {
        _index.search(queries.data(i), knn, complexity, ids.mutable_data(i), dists.mutable_data(i));
    }

    return std::make_pair(ids, dists);
}

template class StaticMemoryIndex<float>;
template class StaticMemoryIndex<uint8_t>;
template class StaticMemoryIndex<int8_t>;

} // namespace diskannpy