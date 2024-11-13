// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <string>


#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#ifdef _WINDOWS
#include "windows_aligned_file_reader.h"
#else
#include "linux_aligned_file_reader.h"
#endif

#include "common.h"
#include "pq_flash_index.h"

namespace py = pybind11;

namespace diskannpy {

#ifdef _WINDOWS
typedef WindowsAlignedFileReader PlatformSpecificAlignedFileReader;
#else
typedef LinuxAlignedFileReader PlatformSpecificAlignedFileReader;
#endif

template <typename DT>
class StaticDiskIndex
{
  public:
    StaticDiskIndex(diskann::Metric metric, const std::string &index_path_prefix, uint32_t num_threads,
                    size_t num_nodes_to_cache, uint32_t cache_mechanism);

    void cache_bfs_levels(size_t num_nodes_to_cache);

    void cache_sample_paths(size_t num_nodes_to_cache, const std::string &warmup_query_file, uint32_t num_threads);

    NeighborsAndDistances<StaticIdType> search(py::array_t<DT, py::array::c_style | py::array::forcecast> &query, uint64_t knn,
                uint64_t complexity, uint64_t beam_width);

    NeighborsAndDistances<StaticIdType> batch_search(py::array_t<DT, py::array::c_style | py::array::forcecast> &queries, uint64_t num_queries,
                      uint64_t knn, uint64_t complexity, uint64_t beam_width, uint32_t num_threads);
  private:
    std::shared_ptr<AlignedFileReader> _reader;
    diskann::PQFlashIndex<DT> _index;
};
}
