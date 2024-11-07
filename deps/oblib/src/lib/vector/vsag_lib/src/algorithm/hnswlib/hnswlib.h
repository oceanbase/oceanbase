
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
#ifndef NO_MANUAL_VECTORIZATION
#if (defined(__SSE__) || _M_IX86_FP > 0 || defined(_M_AMD64) || defined(_M_X64))
#define USE_SSE
#ifdef __AVX__
#define USE_AVX
#ifdef __AVX512F__
#define USE_AVX512
#endif
#endif
#endif
#endif

#if defined(USE_AVX) || defined(USE_SSE)
#ifdef _MSC_VER
#include <intrin.h>

#include <stdexcept>
void
cpuid(int32_t out[4], int32_t eax, int32_t ecx) {
    __cpuidex(out, eax, ecx);
}
static __int64
xgetbv(unsigned int x) {
    return _xgetbv(x);
}
#else
#include <cpuid.h>
#include <stdint.h>
#include <x86intrin.h>

#include <future>
static void
cpuid(int32_t cpuInfo[4], int32_t eax, int32_t ecx) {
    __cpuid_count(eax, ecx, cpuInfo[0], cpuInfo[1], cpuInfo[2], cpuInfo[3]);
}
static uint64_t
xgetbv(unsigned int index) {
    uint32_t eax, edx;
    __asm__ __volatile__("xgetbv" : "=a"(eax), "=d"(edx) : "c"(index));
    return ((uint64_t)edx << 32) | eax;
}
#endif

#if defined(USE_AVX512)
#include <immintrin.h>
#endif

#if defined(__GNUC__)
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#define PORTABLE_ALIGN64 __attribute__((aligned(64)))
#else
#define PORTABLE_ALIGN32 __declspec(align(32))
#define PORTABLE_ALIGN64 __declspec(align(64))
#endif

// Adapted from https://github.com/Mysticial/FeatureDetector
#define _XCR_XFEATURE_ENABLED_MASK 0

static bool
AVXCapable() {
    int cpuInfo[4];

    // CPU support
    cpuid(cpuInfo, 0, 0);
    int nIds = cpuInfo[0];

    bool HW_AVX = false;
    if (nIds >= 0x00000001) {
        cpuid(cpuInfo, 0x00000001, 0);
        HW_AVX = (cpuInfo[2] & ((int)1 << 28)) != 0;
    }

    // OS support
    cpuid(cpuInfo, 1, 0);

    bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
    bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;

    bool avxSupported = false;
    if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
        uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
        avxSupported = (xcrFeatureMask & 0x6) == 0x6;
    }
    return HW_AVX && avxSupported;
}

static bool
AVX512Capable() {
    if (!AVXCapable())
        return false;

    int cpuInfo[4];

    // CPU support
    cpuid(cpuInfo, 0, 0);
    int nIds = cpuInfo[0];

    bool HW_AVX512F = false;
    if (nIds >= 0x00000007) {  //  AVX512 Foundation
        cpuid(cpuInfo, 0x00000007, 0);
        HW_AVX512F = (cpuInfo[1] & ((int)1 << 16)) != 0;
    }

    // OS support
    cpuid(cpuInfo, 1, 0);

    bool osUsesXSAVE_XRSTORE = (cpuInfo[2] & (1 << 27)) != 0;
    bool cpuAVXSuport = (cpuInfo[2] & (1 << 28)) != 0;

    bool avx512Supported = false;
    if (osUsesXSAVE_XRSTORE && cpuAVXSuport) {
        uint64_t xcrFeatureMask = xgetbv(_XCR_XFEATURE_ENABLED_MASK);
        avx512Supported = (xcrFeatureMask & 0xe6) == 0xe6;
    }
    return HW_AVX512F && avx512Supported;
}
#endif

#include <string.h>

#include <functional>
#include <iostream>
#include <queue>
#include <vector>

namespace hnswlib {
typedef size_t labeltype;

// This can be extended to store state for filtering (e.g. from a std::set)
class BaseFilterFunctor {
public:
    virtual bool
    operator()(hnswlib::labeltype id) {
        return true;
    }
};

template <typename T>
class pairGreater {
public:
    bool
    operator()(const T& p1, const T& p2) {
        return p1.first > p2.first;
    }
};

template <typename T>
static void
writeBinaryPOD(std::ostream& out, const T& podRef) {
    out.write((char*)&podRef, sizeof(T));
}

template <typename T>
static void
readBinaryPOD(std::istream& in, T& podRef) {
    in.read((char*)&podRef, sizeof(T));

    if (in.fail()) {
        throw std::runtime_error("Failed to read from stream.");
    }
}

using DISTFUNC = float (*)(const void*, const void*, const void*);

class SpaceInterface {
public:
    // virtual void search(void *);
    virtual size_t
    get_data_size() = 0;

    virtual DISTFUNC
    get_dist_func() = 0;

    virtual void*
    get_dist_func_param() = 0;

    virtual ~SpaceInterface() {
    }
};

template <typename dist_t>
class AlgorithmInterface {
public:
    virtual bool
    addPoint(const void* datapoint, labeltype label) = 0;

    virtual std::priority_queue<std::pair<dist_t, labeltype>>
    searchKnn(const void*, size_t, size_t, BaseFilterFunctor* isIdAllowed = nullptr) const = 0;

    virtual std::priority_queue<std::pair<dist_t, labeltype>>
    searchRange(const void*, float, size_t, BaseFilterFunctor* isIdAllowed = nullptr) const = 0;

    // Return k nearest neighbor in the order of closer fist
    virtual std::vector<std::pair<dist_t, labeltype>>
    searchKnnCloserFirst(const void* query_data,
                         size_t k,
                         size_t ef,
                         BaseFilterFunctor* isIdAllowed = nullptr) const;

    virtual void
    saveIndex(const std::string& location) = 0;

    virtual void
    saveIndex(void* d) = 0;

    virtual void
    saveIndex(std::ostream& out_stream) = 0;

    virtual size_t
    getMaxElements() = 0;

    virtual float
    getDistanceByLabel(labeltype label, const void* data_point) = 0;

    virtual const float*
    getDataByLabel(labeltype label) const = 0;

    virtual std::priority_queue<std::pair<float, labeltype>>
    bruteForce(const void* data_point, int64_t k) = 0;

    virtual void
    resizeIndex(size_t new_max_elements) = 0;

    virtual size_t
    calcSerializeSize() = 0;

    virtual void
    loadIndex(std::function<void(uint64_t, uint64_t, void*)> read_func,
              SpaceInterface* s,
              size_t max_elements_i = 0) = 0;

    virtual void
    loadIndex(std::istream& in_stream, SpaceInterface* s, size_t max_elements_i = 0) = 0;

    virtual size_t
    getCurrentElementCount() = 0;

    virtual size_t
    getDeletedCount() = 0;

    virtual bool
    isValidLabel(labeltype label) = 0;

    virtual bool
    init_memory_space() = 0;

    virtual ~AlgorithmInterface() {
    }
};

template <typename dist_t>
std::vector<std::pair<dist_t, labeltype>>
AlgorithmInterface<dist_t>::searchKnnCloserFirst(const void* query_data,
                                                 size_t k,
                                                 size_t ef,
                                                 BaseFilterFunctor* isIdAllowed) const {
    std::vector<std::pair<dist_t, labeltype>> result;

    // here searchKnn returns the result in the order of further first
    auto ret = searchKnn(query_data, k, ef, isIdAllowed);
    {
        size_t sz = ret.size();
        result.resize(sz);
        while (!ret.empty()) {
            result[--sz] = ret.top();
            ret.pop();
        }
    }

    return result;
}
}  // namespace hnswlib

#include "bruteforce.h"
#include "hnswalg.h"
#include "hnswalg_static.h"
#include "space_ip.h"
#include "space_l2.h"
