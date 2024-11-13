
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

#include <stdexcept>

#include "index/diskann_zparameters.h"
#include "index/hnsw_zparameters.h"
#include "utils.h"
#include "vsag/errors.h"
#include "vsag/expected.hpp"

namespace vsag {

tl::expected<bool, Error>
check_diskann_hnsw_build_parameters(const std::string& json_string) {
    if (auto ret = try_parse_parameters<CreateHnswParameters>(json_string); not ret.has_value()) {
        return tl::unexpected(ret.error());
    }
    if (auto ret = try_parse_parameters<CreateDiskannParameters>(json_string);
        not ret.has_value()) {
        return tl::unexpected(ret.error());
    }
    return true;
}

tl::expected<bool, Error>
check_diskann_hnsw_search_parameters(const std::string& json_string) {
    if (auto ret = try_parse_parameters<HnswSearchParameters>(json_string); not ret.has_value()) {
        return tl::unexpected(ret.error());
    }
    if (auto ret = try_parse_parameters<DiskannSearchParameters>(json_string);
        not ret.has_value()) {
        return tl::unexpected(ret.error());
    }
    return true;
}

}  // namespace vsag
