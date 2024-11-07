// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#include <vector>
#include <boost/dynamic_bitset.hpp>

#include "scratch.h"

namespace diskann
{
//
// Functions to manage scratch space for in-memory index based search
//
template <typename T>
InMemQueryScratch<T>::InMemQueryScratch(uint32_t search_l, uint32_t indexing_l, uint32_t r, uint32_t maxc, size_t dim,
                                        size_t aligned_dim, size_t alignment_factor, bool init_pq_scratch, size_t max_degree,
                                        size_t pq_chunk)
    : _L(0), _R(r), _maxc(maxc)
{
    if (search_l == 0 || indexing_l == 0 || r == 0 || dim == 0)
    {
        std::stringstream ss;
        ss << "In InMemQueryScratch, one of search_l = " << search_l << ", indexing_l = " << indexing_l
           << ", dim = " << dim << " or r = " << r << " is zero." << std::endl;
        throw diskann::ANNException(ss.str(), -1);
    }

    alloc_aligned(((void **)&_aligned_query), aligned_dim * sizeof(T), alignment_factor * sizeof(T));
    memset(_aligned_query, 0, aligned_dim * sizeof(T));

    if (init_pq_scratch)
        _pq_scratch = new PQScratch<T>(max_degree, aligned_dim, pq_chunk);
    else
        _pq_scratch = nullptr;

    _occlude_factor.reserve(maxc);
    _inserted_into_pool_bs = new boost::dynamic_bitset<>();
    _id_scratch.reserve((size_t)std::ceil(1.5 * GRAPH_SLACK_FACTOR * _R));
    _dist_scratch.reserve((size_t)std::ceil(1.5 * GRAPH_SLACK_FACTOR * _R));

    resize_for_new_L(std::max(search_l, indexing_l));
}

template <typename T> void InMemQueryScratch<T>::clear()
{
    _pool.clear();
    _best_l_nodes.clear();
    _occlude_factor.clear();

    _inserted_into_pool_rs.clear();
    _inserted_into_pool_bs->reset();

    _id_scratch.clear();
    _dist_scratch.clear();

    _expanded_nodes_set.clear();
    _expanded_nghrs_vec.clear();
    _occlude_list_output.clear();
}

template <typename T> void InMemQueryScratch<T>::resize_for_new_L(uint32_t new_l)
{
    if (new_l > _L)
    {
        _L = new_l;
        _pool.reserve(3 * _L + _R);
        _best_l_nodes.reserve(_L);

        _inserted_into_pool_rs.reserve(20 * _L);
    }
}

template <typename T> InMemQueryScratch<T>::~InMemQueryScratch()
{
    if (_aligned_query != nullptr)
    {
        aligned_free(_aligned_query);
    }

    delete _pq_scratch;
    delete _inserted_into_pool_bs;
}

//
// Functions to manage scratch space for SSD based search
//
template <typename T> void SSDQueryScratch<T>::reset()
{
    sector_idx = 0;
}

template <typename T> SSDQueryScratch<T>::SSDQueryScratch(size_t max_degree, size_t aligned_dim, size_t pq_chunk)
{
    size_t coord_alloc_size = ROUND_UP(sizeof(T) * aligned_dim, 256);

    diskann::alloc_aligned((void **)&coord_scratch, coord_alloc_size, 256);
    diskann::alloc_aligned((void **)&aligned_query_T, aligned_dim * sizeof(T), 8 * sizeof(T));

    _pq_scratch = new PQScratch<T>(max_degree, aligned_dim, pq_chunk);

    memset(coord_scratch, 0, coord_alloc_size);
    memset(aligned_query_T, 0, aligned_dim * sizeof(T));
}

template <typename T> SSDQueryScratch<T>::~SSDQueryScratch()
{
    diskann::aligned_free((void *)coord_scratch);
    diskann::aligned_free((void *)aligned_query_T);

    delete _pq_scratch;
}


template <typename T>
SSDThreadData<T>::SSDThreadData(size_t max_degree, size_t aligned_dim, size_t pq_chunk):
      scratch(max_degree, aligned_dim, pq_chunk)
{
}

template <typename T> void SSDThreadData<T>::clear()
{
    scratch.reset();
}

template DISKANN_DLLEXPORT class InMemQueryScratch<int8_t>;
template DISKANN_DLLEXPORT class InMemQueryScratch<uint8_t>;
template DISKANN_DLLEXPORT class InMemQueryScratch<float>;

template DISKANN_DLLEXPORT class SSDQueryScratch<int8_t>;
template DISKANN_DLLEXPORT class SSDQueryScratch<uint8_t>;
template DISKANN_DLLEXPORT class SSDQueryScratch<float>;

template DISKANN_DLLEXPORT class SSDThreadData<int8_t>;
template DISKANN_DLLEXPORT class SSDThreadData<uint8_t>;
template DISKANN_DLLEXPORT class SSDThreadData<float>;
} // namespace diskann
