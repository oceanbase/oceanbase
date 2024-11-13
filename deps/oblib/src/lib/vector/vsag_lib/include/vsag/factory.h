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

#pragma once

#include <memory>

#include "allocator.h"
#include "index.h"
#include "readerset.h"

namespace vsag {

class Factory {
public:
    /*
     *  HNSW.parameters:
     *    - dtype: string, required, one of [float32]
     *    - metric_type: string, required, one of [l2, ip]
     *    - dim: integer, required
     *    - hnsw.max_degree: integer, required
     *    - hnsw.ef_construction: integer, required
     *  e.g.,
     *  {
     *      "dtype": "float32",
     *      "metric_type": "l2",
     *      "dim": 128,
     *      "hnsw": {
     *          "max_degree": 16,
     *          "ef_construction": 200
     *      }
     *  }
     *
     *  DiskANN.parameters:
     *    - dtype: string, required, one of [float32]
     *    - metric_type: string, required, one of [l2, ip]
     *    - dim: integer, required
     *    - diskann.max_degree: integer, required
     *    - diskann.ef_construction: integer, required
     *    - diskann.pq_dims: integer, required
     *    - diskann.pq_sample_rate: floating number, required, in range (0.0, 1.0]
     *  e.g.,
     *  {
     *      "dtype": "float32",
     *      "metric_type": "l2",
     *      "dim": 128,
     *      "diskann": {
     *          "max_degree": 16,
     *          "ef_construction": 200,
     *          "pq_dims": 64,
     *          "pq_sample_rate": 0.5
     *      }
     *  }
     */
    static tl::expected<std::shared_ptr<Index>, Error>
    CreateIndex(const std::string& name,
                const std::string& parameters,
                Allocator* allocator = nullptr);

    static std::shared_ptr<Reader>
    CreateLocalFileReader(const std::string& filename, int64_t base_offset, int64_t size);

private:
    Factory() = default;
};

}  // namespace vsag
