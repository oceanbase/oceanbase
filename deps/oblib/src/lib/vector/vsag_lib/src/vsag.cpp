
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

#include "vsag/vsag.h"

#include <cpuinfo.h>

#include <sstream>

#include "logger.h"
#include "simd/simd.h"
#include "version.h"

namespace vsag {

std::string
version() {
    return VSAG_VERSION;
}

bool
init() {
#ifndef NDEBUG
    // set debug level by default in debug version of VSAG
    logger::set_level(logger::level::debug);
#endif

    cpuinfo_initialize();
    std::stringstream ss;

    ss << std::boolalpha;
    ss << "\n====vsag start init test====";
    ss << "\nrunning on " << cpuinfo_get_package(0)->name;
    ss << "\ncores count: " << cpuinfo_get_cores_count();
    auto simd_status = setup_simd();
    ss << "\ncpu sse >> " << simd_status.sse();
    ss << "\ncpu avx >> " << simd_status.avx();
    ss << "\ncpu avx2 >> " << simd_status.avx2();
    ss << "\ncpu avx512f >> " << simd_status.avx512f();
    ss << "\ncpu avx512dq >> " << simd_status.avx512dq();
    ss << "\ncpu avx512bw >> " << simd_status.avx512bw();
    ss << "\ncpu avx512vl >> " << simd_status.avx512vl();
    ss << "\n====vsag init done====";
    logger::debug(ss.str());

    return true;
}

static bool _init = init();

}  // namespace vsag
