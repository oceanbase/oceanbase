

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

#include "vsag/constants.h"

namespace vsag {

const char* const INDEX_DISKANN = "diskann";
const char* const INDEX_HNSW = "hnsw";
const char* const INDEX_FRESH_HNSW = "fresh_hnsw";
const char* const DIM = "dim";
const char* const NUM_ELEMENTS = "num_elements";
const char* const IDS = "ids";
const char* const DISTS = "dists";
const char* const FLOAT32_VECTORS = "f32_vectors";
const char* const INT8_VECTORS = "i8_vectors";
const char* const HNSW_DATA = "hnsw_data";
const char* const CONJUGATE_GRAPH_DATA = "conjugate_graph_data";
const char* const DISKANN_PQ = "diskann_qp";
const char* const DISKANN_COMPRESSED_VECTOR = "diskann_compressed_vector";
const char* const DISKANN_LAYOUT_FILE = "diskann_layout_file";
const char* const DISKANN_TAG_FILE = "diskann_tag_file";
const char* const DISKANN_GRAPH = "diskann_graph";
const char* const SIMPLEFLAT_VECTORS = "simpleflat_vectors";
const char* const SIMPLEFLAT_IDS = "simpleflat_ids";
const char* const METRIC_L2 = "l2";
const char* const METRIC_COSINE = "cosine";
const char* const METRIC_IP = "ip";
const char* const DATATYPE_FLOAT32 = "float32";
const char* const BLANK_INDEX = "blank_index";

// parameters
const char* const PARAMETER_DTYPE = "dtype";
const char* const PARAMETER_DIM = "dim";
const char* const PARAMETER_METRIC_TYPE = "metric_type";
const char* const PARAMETER_USE_CONJUGATE_GRAPH = "use_conjugate_graph";
const char* const PARAMETER_USE_CONJUGATE_GRAPH_SEARCH = "use_conjugate_graph_search";

const char* const DISKANN_PARAMETER_L = "ef_construction";
const char* const DISKANN_PARAMETER_R = "max_degree";
const char* const DISKANN_PARAMETER_P_VAL = "pq_sample_rate";
const char* const DISKANN_PARAMETER_DISK_PQ_DIMS = "pq_dims";
const char* const DISKANN_PARAMETER_PRELOAD = "use_pq_search";
const char* const DISKANN_PARAMETER_USE_REFERENCE = "use_reference";
const char* const DISKANN_PARAMETER_USE_OPQ = "use_opq";
const char* const DISKANN_PARAMETER_USE_ASYNC_IO = "use_async_io";
const char* const DISKANN_PARAMETER_USE_BSA = "use_bsa";

const char* const DISKANN_PARAMETER_BEAM_SEARCH = "beam_search";
const char* const DISKANN_PARAMETER_IO_LIMIT = "io_limit";
const char* const DISKANN_PARAMETER_EF_SEARCH = "ef_search";
const char* const DISKANN_PARAMETER_REORDER = "use_reorder";

const char* const HNSW_PARAMETER_EF_RUNTIME = "ef_search";
const char* const HNSW_PARAMETER_M = "max_degree";
const char* const HNSW_PARAMETER_CONSTRUCTION = "ef_construction";
const char* const HNSW_PARAMETER_USE_STATIC = "use_static";
const char* const HNSW_PARAMETER_REVERSED_EDGES = "use_reversed_edges";

// statstic key
const char* const STATSTIC_MEMORY = "memory";
const char* const STATSTIC_INDEX_NAME = "index_name";
const char* const STATSTIC_DATA_NUM = "data_num";

const char* const STATSTIC_KNN_TIME = "knn_time";
const char* const STATSTIC_KNN_IO = "knn_io";
const char* const STATSTIC_KNN_HOP = "knn_hop";
const char* const STATSTIC_KNN_IO_TIME = "knn_io_time";
const char* const STATSTIC_KNN_CACHE_HIT = "knn_cache_hit";
const char* const STATSTIC_RANGE_TIME = "range_time";
const char* const STATSTIC_RANGE_IO = "range_io";
const char* const STATSTIC_RANGE_HOP = "range_hop";
const char* const STATSTIC_RANGE_CACHE_HIT = "range_cache_hit";
const char* const STATSTIC_RANGE_IO_TIME = "range_io_time";

//Error message
const char* const MESSAGE_PARAMETER = "invalid parameter";

// Serialize key
const char* const SERIALIZE_MAGIC_NUM = "MAGIC_NUM";
const char* const SERIALIZE_VERSION = "VERSION";

};  // namespace vsag
