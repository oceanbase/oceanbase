
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

#include "simd.h"

#include <cpuinfo.h>

#include <iostream>

namespace vsag {

float (*L2SqrSIMD16Ext)(const void*, const void*, const void*);
float (*L2SqrSIMD16ExtResiduals)(const void*, const void*, const void*);
float (*L2SqrSIMD4Ext)(const void*, const void*, const void*);
float (*L2SqrSIMD4ExtResiduals)(const void*, const void*, const void*);

float (*InnerProductSIMD4Ext)(const void*, const void*, const void*);
float (*InnerProductSIMD16Ext)(const void*, const void*, const void*);
float (*InnerProductDistanceSIMD16Ext)(const void*, const void*, const void*);
float (*InnerProductDistanceSIMD16ExtResiduals)(const void*, const void*, const void*);
float (*InnerProductDistanceSIMD4Ext)(const void*, const void*, const void*);
float (*InnerProductDistanceSIMD4ExtResiduals)(const void*, const void*, const void*);

SimdStatus
setup_simd() {
    L2SqrSIMD16Ext = L2Sqr;
    L2SqrSIMD16ExtResiduals = L2Sqr;
    L2SqrSIMD4Ext = L2Sqr;
    L2SqrSIMD4ExtResiduals = L2Sqr;

    InnerProductSIMD4Ext = InnerProduct;
    InnerProductSIMD16Ext = InnerProduct;
    InnerProductDistanceSIMD16Ext = InnerProductDistance;
    InnerProductDistanceSIMD16ExtResiduals = InnerProductDistance;
    InnerProductDistanceSIMD4Ext = InnerProductDistance;
    InnerProductDistanceSIMD4ExtResiduals = InnerProductDistance;

    SimdStatus ret;

    if (cpuinfo_has_x86_sse()) {
        ret.runtime_has_sse = true;
#ifndef ENABLE_SSE
    }
#else
        L2SqrSIMD16Ext = L2SqrSIMD16ExtSSE;
        L2SqrSIMD16ExtResiduals = L2SqrSIMD16ExtResidualsSSE;
        L2SqrSIMD4Ext = L2SqrSIMD4ExtSSE;
        L2SqrSIMD4ExtResiduals = L2SqrSIMD4ExtResidualsSSE;

        InnerProductSIMD4Ext = InnerProductSIMD4ExtSSE;
        InnerProductSIMD16Ext = InnerProductSIMD16ExtSSE;
        InnerProductDistanceSIMD16Ext = InnerProductDistanceSIMD16ExtSSE;
        InnerProductDistanceSIMD16ExtResiduals = InnerProductDistanceSIMD16ExtResidualsSSE;
        InnerProductDistanceSIMD4Ext = InnerProductDistanceSIMD4ExtSSE;
        InnerProductDistanceSIMD4ExtResiduals = InnerProductDistanceSIMD4ExtResidualsSSE;
    }
    ret.dist_support_sse = true;
#endif

    if (cpuinfo_has_x86_avx()) {
        ret.runtime_has_avx = true;
#ifndef ENABLE_AVX
    }
#else
        L2SqrSIMD16Ext = L2SqrSIMD16ExtAVX;
        InnerProductSIMD4Ext = InnerProductSIMD4ExtAVX;
        InnerProductSIMD16Ext = InnerProductSIMD16ExtAVX;
    }
    ret.dist_support_avx = true;
#endif

    if (cpuinfo_has_x86_avx2()) {
        ret.runtime_has_avx2 = true;
#ifndef ENABLE_AVX2
    }
#else
    }
    ret.dist_support_avx2 = true;
#endif

    if (cpuinfo_has_x86_avx512f() && cpuinfo_has_x86_avx512dq() && cpuinfo_has_x86_avx512bw() &&
        cpuinfo_has_x86_avx512vl()) {
        ret.runtime_has_avx512f = true;
        ret.runtime_has_avx512dq = true;
        ret.runtime_has_avx512bw = true;
        ret.runtime_has_avx512vl = true;
#ifndef ENABLE_AVX512
    }
#else
        L2SqrSIMD16Ext = L2SqrSIMD16ExtAVX512;
        InnerProductSIMD16Ext = InnerProductSIMD16ExtAVX512;
    }
    ret.dist_support_avx512f = true;
    ret.dist_support_avx512dq = true;
    ret.dist_support_avx512bw = true;
    ret.dist_support_avx512vl = true;
#endif

    return ret;
}

DistanceFunc
GetInnerProductDistanceFunc(size_t dim) {
    if (dim % 16 == 0) {
        return vsag::InnerProductDistanceSIMD16Ext;
    } else if (dim % 4 == 0) {
        return vsag::InnerProductDistanceSIMD4Ext;
    } else if (dim > 16) {
        return vsag::InnerProductDistanceSIMD16ExtResiduals;
    } else if (dim > 4) {
        return vsag::InnerProductDistanceSIMD4ExtResiduals;
    } else {
        return vsag::InnerProductDistance;
    }
}

PQDistanceFunc
GetPQDistanceFunc() {
#ifdef ENABLE_AVX
    return PQDistanceAVXFloat256;
#endif
#ifdef ENABLE_SSE
    return PQDistanceSSEFloat256;
#endif
    return PQDistanceFloat256;
}

DistanceFunc
GetL2DistanceFunc(size_t dim) {
    if (dim % 16 == 0) {
        return vsag::L2SqrSIMD16Ext;
    } else if (dim % 4 == 0) {
        return vsag::L2SqrSIMD4Ext;
    } else if (dim > 16) {
        return vsag::L2SqrSIMD16ExtResiduals;
    } else if (dim > 4) {
        return vsag::L2SqrSIMD4ExtResiduals;
    } else {
        return vsag::L2Sqr;
    }
}

}  // namespace vsag
