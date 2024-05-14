/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512_ARGSORT_64BIT
#define AVX512_ARGSORT_64BIT

#include "avx512-64bit-common.h"
#include "avx512-common-argsort.h"
#include "avx512-64bit-keyvalue-networks.hpp"

/* argsort using std::sort */
template <typename T>
void std_argsort_withnan(T *arr, int64_t *arg, int64_t left, int64_t right)
{
    std::sort(arg + left,
              arg + right,
              [arr](int64_t left, int64_t right) -> bool {
              if ((!std::isnan(arr[left])) && (!std::isnan(arr[right]))) {return arr[left] < arr[right];}
              else if (std::isnan(arr[left])) {return false;}
              else {return true;}
              });
}

/* argsort using std::sort */
template <typename T>
void std_argsort(T *arr, int64_t *arg, int64_t left, int64_t right)
{
    std::sort(arg + left,
              arg + right,
              [arr](int64_t left, int64_t right) -> bool {
                  // sort indices according to corresponding array element
                  return arr[left] < arr[right];
              });
}

template <typename vtype, typename type_t>
X86_SIMD_SORT_INLINE void argsort_8_64bit(type_t *arr, int64_t *arg, int32_t N)
{
    using zmm_t = typename vtype::zmm_t;
    typename vtype::opmask_t load_mask = (0x01 << N) - 0x01;
    argzmm_t argzmm = argtype::maskz_loadu(load_mask, arg);
    zmm_t arrzmm = vtype::template mask_i64gather<sizeof(type_t)>(
            vtype::zmm_max(), load_mask, argzmm, arr);
    arrzmm = sort_zmm_64bit<vtype, argtype>(arrzmm, argzmm);
    argtype::mask_storeu(arg, load_mask, argzmm);
}

template <typename vtype, typename type_t>
X86_SIMD_SORT_INLINE void argsort_16_64bit(type_t *arr, int64_t *arg, int32_t N)
{
    if (N <= 8) {
        argsort_8_64bit<vtype>(arr, arg, N);
        return;
    }
    using zmm_t = typename vtype::zmm_t;
    typename vtype::opmask_t load_mask = (0x01 << (N - 8)) - 0x01;
    argzmm_t argzmm1 = argtype::loadu(arg);
    argzmm_t argzmm2 = argtype::maskz_loadu(load_mask, arg + 8);
    zmm_t arrzmm1 = vtype::template i64gather<sizeof(type_t)>(argzmm1, arr);
    zmm_t arrzmm2 = vtype::template mask_i64gather<sizeof(type_t)>(
            vtype::zmm_max(), load_mask, argzmm2, arr);
    arrzmm1 = sort_zmm_64bit<vtype, argtype>(arrzmm1, argzmm1);
    arrzmm2 = sort_zmm_64bit<vtype, argtype>(arrzmm2, argzmm2);
    bitonic_merge_two_zmm_64bit<vtype, argtype>(
            arrzmm1, arrzmm2, argzmm1, argzmm2);
    argtype::storeu(arg, argzmm1);
    argtype::mask_storeu(arg + 8, load_mask, argzmm2);
}

template <typename vtype, typename type_t>
X86_SIMD_SORT_INLINE void argsort_32_64bit(type_t *arr, int64_t *arg, int32_t N)
{
    if (N <= 16) {
        argsort_16_64bit<vtype>(arr, arg, N);
        return;
    }
    using zmm_t = typename vtype::zmm_t;
    using opmask_t = typename vtype::opmask_t;
    zmm_t arrzmm[4];
    argzmm_t argzmm[4];

#pragma GCC unroll 2
    for (int ii = 0; ii < 2; ++ii) {
        argzmm[ii] = argtype::loadu(arg + 8 * ii);
        arrzmm[ii] = vtype::template i64gather<sizeof(type_t)>(argzmm[ii], arr);
        arrzmm[ii] = sort_zmm_64bit<vtype, argtype>(arrzmm[ii], argzmm[ii]);
    }

    uint64_t combined_mask = (0x1ull << (N - 16)) - 0x1ull;
    opmask_t load_mask[2] = {0xFF, 0xFF};
#pragma GCC unroll 2
    for (int ii = 0; ii < 2; ++ii) {
        load_mask[ii] = (combined_mask >> (ii * 8)) & 0xFF;
        argzmm[ii + 2] = argtype::maskz_loadu(load_mask[ii], arg + 16 + 8 * ii);
        arrzmm[ii + 2] = vtype::template mask_i64gather<sizeof(type_t)>(
                vtype::zmm_max(), load_mask[ii], argzmm[ii + 2], arr);
        arrzmm[ii + 2] = sort_zmm_64bit<vtype, argtype>(arrzmm[ii + 2],
                                                        argzmm[ii + 2]);
    }

    bitonic_merge_two_zmm_64bit<vtype, argtype>(
            arrzmm[0], arrzmm[1], argzmm[0], argzmm[1]);
    bitonic_merge_two_zmm_64bit<vtype, argtype>(
            arrzmm[2], arrzmm[3], argzmm[2], argzmm[3]);
    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm, argzmm);

    argtype::storeu(arg, argzmm[0]);
    argtype::storeu(arg + 8, argzmm[1]);
    argtype::mask_storeu(arg + 16, load_mask[0], argzmm[2]);
    argtype::mask_storeu(arg + 24, load_mask[1], argzmm[3]);
}

template <typename vtype, typename type_t>
X86_SIMD_SORT_INLINE void argsort_64_64bit(type_t *arr, int64_t *arg, int32_t N)
{
    if (N <= 32) {
        argsort_32_64bit<vtype>(arr, arg, N);
        return;
    }
    using zmm_t = typename vtype::zmm_t;
    using opmask_t = typename vtype::opmask_t;
    zmm_t arrzmm[8];
    argzmm_t argzmm[8];

#pragma GCC unroll 4
    for (int ii = 0; ii < 4; ++ii) {
        argzmm[ii] = argtype::loadu(arg + 8 * ii);
        arrzmm[ii] = vtype::template i64gather<sizeof(type_t)>(argzmm[ii], arr);
        arrzmm[ii] = sort_zmm_64bit<vtype, argtype>(arrzmm[ii], argzmm[ii]);
    }

    opmask_t load_mask[4] = {0xFF, 0xFF, 0xFF, 0xFF};
    uint64_t combined_mask = (0x1ull << (N - 32)) - 0x1ull;
#pragma GCC unroll 4
    for (int ii = 0; ii < 4; ++ii) {
        load_mask[ii] = (combined_mask >> (ii * 8)) & 0xFF;
        argzmm[ii + 4] = argtype::maskz_loadu(load_mask[ii], arg + 32 + 8 * ii);
        arrzmm[ii + 4] = vtype::template mask_i64gather<sizeof(type_t)>(
                vtype::zmm_max(), load_mask[ii], argzmm[ii + 4], arr);
        arrzmm[ii + 4] = sort_zmm_64bit<vtype, argtype>(arrzmm[ii + 4],
                                                        argzmm[ii + 4]);
    }

#pragma GCC unroll 4
    for (int ii = 0; ii < 8; ii = ii + 2) {
        bitonic_merge_two_zmm_64bit<vtype, argtype>(
                arrzmm[ii], arrzmm[ii + 1], argzmm[ii], argzmm[ii + 1]);
    }
    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm, argzmm);
    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm + 4, argzmm + 4);
    bitonic_merge_eight_zmm_64bit<vtype, argtype>(arrzmm, argzmm);

#pragma GCC unroll 4
    for (int ii = 0; ii < 4; ++ii) {
        argtype::storeu(arg + 8 * ii, argzmm[ii]);
    }
#pragma GCC unroll 4
    for (int ii = 0; ii < 4; ++ii) {
        argtype::mask_storeu(arg + 32 + 8 * ii, load_mask[ii], argzmm[ii + 4]);
    }
}

/* arsort 128 doesn't seem to make much of a difference to perf*/
//template <typename vtype, typename type_t>
//X86_SIMD_SORT_INLINE void
//argsort_128_64bit(type_t *arr, int64_t *arg, int32_t N)
//{
//    if (N <= 64) {
//        argsort_64_64bit<vtype>(arr, arg, N);
//        return;
//    }
//    using zmm_t = typename vtype::zmm_t;
//    using opmask_t = typename vtype::opmask_t;
//    zmm_t arrzmm[16];
//    argzmm_t argzmm[16];
//
//#pragma UNROLL_LOOP(8)
//    for (int ii = 0; ii < 8; ++ii) {
//        argzmm[ii] = argtype::loadu(arg + 8*ii);
//        arrzmm[ii] = vtype::template i64gather<sizeof(type_t)>(argzmm[ii], arr);
//        arrzmm[ii] = sort_zmm_64bit<vtype, argtype>(arrzmm[ii], argzmm[ii]);
//    }
//
//    opmask_t load_mask[8] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
//    if (N != 128) {
//    uint64_t combined_mask = (0x1ull << (N - 64)) - 0x1ull;
//#pragma UNROLL_LOOP(8)
//        for (int ii = 0; ii < 8; ++ii) {
//            load_mask[ii] = (combined_mask >> (ii*8)) & 0xFF;
//        }
//    }
//#pragma UNROLL_LOOP(8)
//    for (int ii = 0; ii < 8; ++ii) {
//        argzmm[ii+8] = argtype::maskz_loadu(load_mask[ii], arg + 64 + 8*ii);
//        arrzmm[ii+8] = vtype::template mask_i64gather<sizeof(type_t)>(vtype::zmm_max(), load_mask[ii], argzmm[ii+8], arr);
//        arrzmm[ii+8] = sort_zmm_64bit<vtype, argtype>(arrzmm[ii+8], argzmm[ii+8]);
//    }
//
//#pragma UNROLL_LOOP(8)
//    for (int ii = 0; ii < 16; ii = ii + 2) {
//        bitonic_merge_two_zmm_64bit<vtype, argtype>(arrzmm[ii], arrzmm[ii + 1], argzmm[ii], argzmm[ii + 1]);
//    }
//    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm, argzmm);
//    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm + 4, argzmm + 4);
//    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm + 8, argzmm + 8);
//    bitonic_merge_four_zmm_64bit<vtype, argtype>(arrzmm + 12, argzmm + 12);
//    bitonic_merge_eight_zmm_64bit<vtype, argtype>(arrzmm, argzmm);
//    bitonic_merge_eight_zmm_64bit<vtype, argtype>(arrzmm+8, argzmm+8);
//    bitonic_merge_sixteen_zmm_64bit<vtype, argtype>(arrzmm, argzmm);
//
//#pragma UNROLL_LOOP(8)
//    for (int ii = 0; ii < 8; ++ii) {
//        argtype::storeu(arg + 8*ii, argzmm[ii]);
//    }
//#pragma UNROLL_LOOP(8)
//    for (int ii = 0; ii < 8; ++ii) {
//        argtype::mask_storeu(arg + 64 + 8*ii, load_mask[ii], argzmm[ii + 8]);
//    }
//}

template <typename vtype, typename type_t>
type_t get_pivot_64bit(type_t *arr,
                       int64_t *arg,
                       const int64_t left,
                       const int64_t right)
{
    if (right - left >= vtype::numlanes) {
        // median of 8
        int64_t size = (right - left) / 8;
        using zmm_t = typename vtype::zmm_t;
        // TODO: Use gather here too:
        __m512i rand_index = _mm512_set_epi64(arg[left + size],
                                              arg[left + 2 * size],
                                              arg[left + 3 * size],
                                              arg[left + 4 * size],
                                              arg[left + 5 * size],
                                              arg[left + 6 * size],
                                              arg[left + 7 * size],
                                              arg[left + 8 * size]);
        zmm_t rand_vec
                = vtype::template i64gather<sizeof(type_t)>(rand_index, arr);
        // pivot will never be a nan, since there are no nan's!
        zmm_t sort = sort_zmm_64bit<vtype>(rand_vec);
        return ((type_t *)&sort)[4];
    }
    else {
        return arr[arg[right]];
    }
}

template <typename vtype, typename type_t>
inline void argsort_64bit_(type_t *arr,
                           int64_t *arg,
                           int64_t left,
                           int64_t right,
                           int64_t max_iters)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        std_argsort(arr, arg, left, right + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 64
     */
    if (right + 1 - left <= 64) {
        argsort_64_64bit<vtype>(arr, arg + left, (int32_t)(right + 1 - left));
        return;
    }
    type_t pivot = get_pivot_64bit<vtype>(arr, arg, left, right);
    type_t smallest = vtype::type_max();
    type_t biggest = vtype::type_min();
    int64_t pivot_index = partition_avx512_unrolled<vtype, 4>(
            arr, arg, left, right + 1, pivot, &smallest, &biggest);
    if (pivot != smallest)
        argsort_64bit_<vtype>(arr, arg, left, pivot_index - 1, max_iters - 1);
    if (pivot != biggest)
        argsort_64bit_<vtype>(arr, arg, pivot_index, right, max_iters - 1);
}

template <typename vtype, typename type_t>
bool has_nan(type_t* arr, int64_t arrsize)
{
    using opmask_t = typename vtype::opmask_t;
    using zmm_t = typename vtype::zmm_t;
    bool found_nan = false;
    opmask_t loadmask = 0xFF;
    zmm_t in;
    while (arrsize > 0) {
        if (arrsize < vtype::numlanes) {
            loadmask = (0x01 << arrsize) - 0x01;
            in = vtype::maskz_loadu(loadmask, arr);
        }
        else {
            in = vtype::loadu(arr);
        }
        opmask_t nanmask = vtype::template fpclass<0x01|0x80>(in);
        arr += vtype::numlanes;
        arrsize -= vtype::numlanes;
        if (nanmask != 0x00) {
            found_nan = true;
            break;
        }
    }
    return found_nan;
}

template <typename T>
void avx512_argsort(T* arr, int64_t *arg, int64_t arrsize)
{
    if (arrsize > 1) {
        argsort_64bit_<zmm_vector<T>>(
                arr, arg, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
    }
}

template <>
void avx512_argsort(double* arr, int64_t *arg, int64_t arrsize)
{
    if (arrsize > 1) {
        if (has_nan<zmm_vector<double>>(arr, arrsize)) {
            std_argsort_withnan(arr, arg, 0, arrsize);
        }
        else {
            argsort_64bit_<zmm_vector<double>>(
                    arr, arg, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
        }
    }
}


template <>
void avx512_argsort(int32_t* arr, int64_t *arg, int64_t arrsize)
{
    if (arrsize > 1) {
        argsort_64bit_<ymm_vector<int32_t>>(
                arr, arg, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
    }
}

template <>
void avx512_argsort(uint32_t* arr, int64_t *arg, int64_t arrsize)
{
    if (arrsize > 1) {
        argsort_64bit_<ymm_vector<uint32_t>>(
                arr, arg, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
    }
}

template <>
void avx512_argsort(float* arr, int64_t *arg, int64_t arrsize)
{
    if (arrsize > 1) {
        if (has_nan<ymm_vector<float>>(arr, arrsize)) {
            std_argsort_withnan(arr, arg, 0, arrsize);
        }
        else {
            argsort_64bit_<ymm_vector<float>>(
                    arr, arg, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
        }
    }
}

template <typename T>
std::vector<int64_t> avx512_argsort(T* arr, int64_t arrsize)
{
    std::vector<int64_t> indices(arrsize);
    std::iota(indices.begin(), indices.end(), 0);
    avx512_argsort<T>(arr, indices.data(), arrsize);
    return indices;
}

#endif // AVX512_ARGSORT_64BIT
