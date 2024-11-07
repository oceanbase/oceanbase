
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

#include "diskann_zparameters.h"

namespace vsag {

CreateDiskannParameters
CreateDiskannParameters::FromJson(const std::string& json_string) {
    nlohmann::json params = nlohmann::json::parse(json_string);

    CHECK_ARGUMENT(params.contains(PARAMETER_DTYPE),
                   fmt::format("parameters must contains {}", PARAMETER_DTYPE));
    CHECK_ARGUMENT(
        params[PARAMETER_DTYPE] == DATATYPE_FLOAT32,
        fmt::format("parameters[{}] supports {} only now", PARAMETER_DTYPE, DATATYPE_FLOAT32));
    CHECK_ARGUMENT(params.contains(PARAMETER_METRIC_TYPE),
                   fmt::format("parameters must contains {}", PARAMETER_METRIC_TYPE));
    CHECK_ARGUMENT(params.contains(PARAMETER_DIM),
                   fmt::format("parameters must contains {}", PARAMETER_DIM));

    CreateDiskannParameters obj;

    // set ojb.dim
    obj.dim = params[PARAMETER_DIM];

    // set ojb.dtype
    obj.dtype = params[PARAMETER_DTYPE];

    // set obj.metric
    CHECK_ARGUMENT(params.contains(INDEX_DISKANN),
                   fmt::format("parameters must contains {}", INDEX_DISKANN));
    if (params[PARAMETER_METRIC_TYPE] == METRIC_L2) {
        obj.metric = diskann::Metric::L2;
    } else if (params[PARAMETER_METRIC_TYPE] == METRIC_IP) {
        obj.metric = diskann::Metric::INNER_PRODUCT;
    } else {
        std::string metric = params[PARAMETER_METRIC_TYPE];
        throw std::invalid_argument(fmt::format("parameters[{}] must in [{}, {}], now is {}",
                                                PARAMETER_METRIC_TYPE,
                                                METRIC_L2,
                                                METRIC_IP,
                                                metric));
    }

    // set obj.max_degree
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_R),
        fmt::format("parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_R));
    obj.max_degree = params[INDEX_DISKANN][DISKANN_PARAMETER_R];

    // set obj.ef_construction
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_L),
        fmt::format("parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_L));
    obj.ef_construction = params[INDEX_DISKANN][DISKANN_PARAMETER_L];

    // set obj.pq_dims
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_DISK_PQ_DIMS),
        fmt::format(
            "parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_DISK_PQ_DIMS));
    obj.pq_dims = params[INDEX_DISKANN][DISKANN_PARAMETER_DISK_PQ_DIMS];

    // set obj.pq_sample_rate
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_P_VAL),
        fmt::format("parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_P_VAL));
    obj.pq_sample_rate = params[INDEX_DISKANN][DISKANN_PARAMETER_P_VAL];

    // optional
    // set obj.use_preload
    if (params[INDEX_DISKANN].contains(DISKANN_PARAMETER_PRELOAD)) {
        obj.use_preload = params[INDEX_DISKANN][DISKANN_PARAMETER_PRELOAD];
    }
    // set obj.use_reference
    if (params[INDEX_DISKANN].contains(DISKANN_PARAMETER_USE_REFERENCE)) {
        obj.use_reference = params[INDEX_DISKANN][DISKANN_PARAMETER_USE_REFERENCE];
    }
    // set obj.use_opq
    if (params[INDEX_DISKANN].contains(DISKANN_PARAMETER_USE_OPQ)) {
        obj.use_opq = params[INDEX_DISKANN][DISKANN_PARAMETER_USE_OPQ];
    }

    // set obj.use_bsa
    if (params[INDEX_DISKANN].contains(DISKANN_PARAMETER_USE_BSA)) {
        obj.use_bsa = params[INDEX_DISKANN][DISKANN_PARAMETER_USE_BSA];
    }

    // set obj.use_async_io
    if (params[INDEX_DISKANN].contains(DISKANN_PARAMETER_USE_ASYNC_IO)) {
        obj.use_async_io = params[INDEX_DISKANN][DISKANN_PARAMETER_USE_ASYNC_IO];
    }

    return obj;
}

DiskannSearchParameters
DiskannSearchParameters::FromJson(const std::string& json_string) {
    nlohmann::json params = nlohmann::json::parse(json_string);

    DiskannSearchParameters obj;

    // set obj.ef_search
    CHECK_ARGUMENT(params.contains(INDEX_DISKANN),
                   fmt::format("parameters must contains {}", INDEX_DISKANN));
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_EF_SEARCH),
        fmt::format("parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_EF_SEARCH));
    obj.ef_search = params[INDEX_DISKANN][DISKANN_PARAMETER_EF_SEARCH];
    CHECK_ARGUMENT((1 <= obj.ef_search) and (obj.ef_search <= 1000),
                   fmt::format("ef_search({}) must in range[1, 1000]", obj.ef_search));

    // set obj.beam_search
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_BEAM_SEARCH),
        fmt::format(
            "parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_BEAM_SEARCH));
    obj.beam_search = params[INDEX_DISKANN][DISKANN_PARAMETER_BEAM_SEARCH];
    CHECK_ARGUMENT((1 <= obj.beam_search) and (obj.beam_search <= 30),
                   fmt::format("beam_search({}) must in range[1, 30]", obj.beam_search));

    // set obj.io_limit
    CHECK_ARGUMENT(
        params[INDEX_DISKANN].contains(DISKANN_PARAMETER_IO_LIMIT),
        fmt::format("parameters[{}] must contains {}", INDEX_DISKANN, DISKANN_PARAMETER_IO_LIMIT));
    obj.io_limit = params[INDEX_DISKANN][DISKANN_PARAMETER_IO_LIMIT];
    CHECK_ARGUMENT((1 <= obj.io_limit) and (obj.io_limit <= 512),
                   fmt::format("io_limit({}) must in range[1, 512]", obj.io_limit));

    // optional
    // set obj.use_reorder
    if (params[INDEX_DISKANN].contains(DISKANN_PARAMETER_REORDER)) {
        obj.use_reorder = params[INDEX_DISKANN][DISKANN_PARAMETER_REORDER];
    }

    return obj;
}

}  // namespace vsag
