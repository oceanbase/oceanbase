
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

#include <stdlib.h>

#include <string>

namespace vsag {

struct SimdStatus {
    bool dist_support_sse = false;
    bool dist_support_avx = false;
    bool dist_support_avx2 = false;
    bool dist_support_avx512f = false;
    bool dist_support_avx512dq = false;
    bool dist_support_avx512bw = false;
    bool dist_support_avx512vl = false;
    bool runtime_has_sse = false;
    bool runtime_has_avx = false;
    bool runtime_has_avx2 = false;
    bool runtime_has_avx512f = false;
    bool runtime_has_avx512dq = false;
    bool runtime_has_avx512bw = false;
    bool runtime_has_avx512vl = false;

    std::string
    sse() {
        return status_to_string(dist_support_sse, runtime_has_sse);
    }

    std::string
    avx() {
        return status_to_string(dist_support_avx, runtime_has_avx);
    }

    std::string
    avx2() {
        return status_to_string(dist_support_avx2, runtime_has_avx2);
    }

    std::string
    avx512f() {
        return status_to_string(dist_support_avx512f, runtime_has_avx512f);
    }

    std::string
    avx512dq() {
        return status_to_string(dist_support_avx512dq, runtime_has_avx512dq);
    }

    std::string
    avx512bw() {
        return status_to_string(dist_support_avx512bw, runtime_has_avx512bw);
    }

    std::string
    avx512vl() {
        return status_to_string(dist_support_avx512vl, runtime_has_avx512vl);
    }

    std::string
    boolean_to_string(bool value) {
        if (value) {
            return "Y";
        } else {
            return "N";
        }
    }

    std::string
    status_to_string(bool dist, bool runtime) {
        return "dist_support:" + boolean_to_string(dist) +
               " + platform:" + boolean_to_string(runtime) +
               " = using:" + boolean_to_string(dist & runtime);
    }
};

SimdStatus
setup_simd();

float
L2Sqr(const void* pVect1v, const void* pVect2v, const void* qty_ptr);

float
InnerProduct(const void* pVect1, const void* pVect2, const void* qty_ptr);
float
InnerProductDistance(const void* pVect1, const void* pVect2, const void* qty_ptr);

void
PQDistanceFloat256(const void* single_dim_centers, float single_dim_val, void* result);

#if defined(ENABLE_SSE)
float
L2SqrSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
L2SqrSIMD4ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
L2SqrSIMD4ExtResidualsSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
L2SqrSIMD16ExtResidualsSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);

float
InnerProductSIMD4ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductDistanceSIMD16ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductDistanceSIMD4ExtSSE(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductDistanceSIMD4ExtResidualsSSE(const void* pVect1v,
                                         const void* pVect2v,
                                         const void* qty_ptr);
float
InnerProductDistanceSIMD16ExtResidualsSSE(const void* pVect1v,
                                          const void* pVect2v,
                                          const void* qty_ptr);
void
PQDistanceSSEFloat256(const void* single_dim_centers, float single_dim_val, void* result);
#endif

#if defined(ENABLE_AVX)
float
L2SqrSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductSIMD4ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductSIMD16ExtAVX(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
void
PQDistanceAVXFloat256(const void* single_dim_centers, float single_dim_val, void* result);
#endif

#if defined(ENABLE_AVX512)
float
L2SqrSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
float
InnerProductSIMD16ExtAVX512(const void* pVect1v, const void* pVect2v, const void* qty_ptr);
#endif

typedef float (*DistanceFunc)(const void* pVect1, const void* pVect2, const void* qty_ptr);
DistanceFunc
GetL2DistanceFunc(size_t dim);
DistanceFunc
GetInnerProductDistanceFunc(size_t dim);

typedef void (*PQDistanceFunc)(const void* single_dim_centers, float single_dim_val, void* result);

PQDistanceFunc
GetPQDistanceFunc();

}  // namespace vsag
