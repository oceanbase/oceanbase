/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Liu Zhuan <zhuan.liu@intel.com>
 *          Tang Xi <xi.tang@intel.com>
 * ****************************************************************/

#ifndef AVX512_QSORT_64BIT_KV
#define AVX512_QSORT_64BIT_KV

#include "avx512-64bit-common.h"
#include "avx512-64bit-keyvalue-networks.hpp"

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
sort_8_64bit(type1_t *keys, type2_t *indexes, int32_t N)
{
    typename vtype1::opmask_t load_mask = (0x01 << N) - 0x01;
    typename vtype1::zmm_t key_zmm
            = vtype1::mask_loadu(vtype1::zmm_max(), load_mask, keys);

    typename vtype2::zmm_t index_zmm
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask, indexes);
    vtype1::mask_storeu(keys,
                        load_mask,
                        sort_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm));
    vtype2::mask_storeu(indexes, load_mask, index_zmm);
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
sort_16_64bit(type1_t *keys, type2_t *indexes, int32_t N)
{
    if (N <= 8) {
        sort_8_64bit<vtype1, vtype2>(keys, indexes, N);
        return;
    }
    using zmm_t = typename vtype1::zmm_t;
    using index_type = typename vtype2::zmm_t;

    typename vtype1::opmask_t load_mask = (0x01 << (N - 8)) - 0x01;

    zmm_t key_zmm1 = vtype1::loadu(keys);
    zmm_t key_zmm2 = vtype1::mask_loadu(vtype1::zmm_max(), load_mask, keys + 8);

    index_type index_zmm1 = vtype2::loadu(indexes);
    index_type index_zmm2
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask, indexes + 8);

    key_zmm1 = sort_zmm_64bit<vtype1, vtype2>(key_zmm1, index_zmm1);
    key_zmm2 = sort_zmm_64bit<vtype1, vtype2>(key_zmm2, index_zmm2);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm1, key_zmm2, index_zmm1, index_zmm2);

    vtype2::storeu(indexes, index_zmm1);
    vtype2::mask_storeu(indexes + 8, load_mask, index_zmm2);

    vtype1::storeu(keys, key_zmm1);
    vtype1::mask_storeu(keys + 8, load_mask, key_zmm2);
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
sort_32_64bit(type1_t *keys, type2_t *indexes, int32_t N)
{
    if (N <= 16) {
        sort_16_64bit<vtype1, vtype2>(keys, indexes, N);
        return;
    }
    using zmm_t = typename vtype1::zmm_t;
    using opmask_t = typename vtype2::opmask_t;
    using index_type = typename vtype2::zmm_t;
    zmm_t key_zmm[4];
    index_type index_zmm[4];

    key_zmm[0] = vtype1::loadu(keys);
    key_zmm[1] = vtype1::loadu(keys + 8);

    index_zmm[0] = vtype2::loadu(indexes);
    index_zmm[1] = vtype2::loadu(indexes + 8);

    key_zmm[0] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[0], index_zmm[0]);
    key_zmm[1] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[1], index_zmm[1]);

    opmask_t load_mask1 = 0xFF, load_mask2 = 0xFF;
    uint64_t combined_mask = (0x1ull << (N - 16)) - 0x1ull;
    load_mask1 = (combined_mask)&0xFF;
    load_mask2 = (combined_mask >> 8) & 0xFF;
    key_zmm[2] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask1, keys + 16);
    key_zmm[3] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask2, keys + 24);

    index_zmm[2]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask1, indexes + 16);
    index_zmm[3]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask2, indexes + 24);

    key_zmm[2] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[2], index_zmm[2]);
    key_zmm[3] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[3], index_zmm[3]);

    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[0], key_zmm[1], index_zmm[0], index_zmm[1]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[2], key_zmm[3], index_zmm[2], index_zmm[3]);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm);

    vtype2::storeu(indexes, index_zmm[0]);
    vtype2::storeu(indexes + 8, index_zmm[1]);
    vtype2::mask_storeu(indexes + 16, load_mask1, index_zmm[2]);
    vtype2::mask_storeu(indexes + 24, load_mask2, index_zmm[3]);

    vtype1::storeu(keys, key_zmm[0]);
    vtype1::storeu(keys + 8, key_zmm[1]);
    vtype1::mask_storeu(keys + 16, load_mask1, key_zmm[2]);
    vtype1::mask_storeu(keys + 24, load_mask2, key_zmm[3]);
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
sort_64_64bit(type1_t *keys, type2_t *indexes, int32_t N)
{
    if (N <= 32) {
        sort_32_64bit<vtype1, vtype2>(keys, indexes, N);
        return;
    }
    using zmm_t = typename vtype1::zmm_t;
    using opmask_t = typename vtype1::opmask_t;
    using index_type = typename vtype2::zmm_t;
    zmm_t key_zmm[8];
    index_type index_zmm[8];

    key_zmm[0] = vtype1::loadu(keys);
    key_zmm[1] = vtype1::loadu(keys + 8);
    key_zmm[2] = vtype1::loadu(keys + 16);
    key_zmm[3] = vtype1::loadu(keys + 24);

    index_zmm[0] = vtype2::loadu(indexes);
    index_zmm[1] = vtype2::loadu(indexes + 8);
    index_zmm[2] = vtype2::loadu(indexes + 16);
    index_zmm[3] = vtype2::loadu(indexes + 24);
    key_zmm[0] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[0], index_zmm[0]);
    key_zmm[1] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[1], index_zmm[1]);
    key_zmm[2] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[2], index_zmm[2]);
    key_zmm[3] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[3], index_zmm[3]);

    opmask_t load_mask1 = 0xFF, load_mask2 = 0xFF;
    opmask_t load_mask3 = 0xFF, load_mask4 = 0xFF;
    // N-32 >= 1
    uint64_t combined_mask = (0x1ull << (N - 32)) - 0x1ull;
    load_mask1 = (combined_mask)&0xFF;
    load_mask2 = (combined_mask >> 8) & 0xFF;
    load_mask3 = (combined_mask >> 16) & 0xFF;
    load_mask4 = (combined_mask >> 24) & 0xFF;
    key_zmm[4] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask1, keys + 32);
    key_zmm[5] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask2, keys + 40);
    key_zmm[6] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask3, keys + 48);
    key_zmm[7] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask4, keys + 56);

    index_zmm[4]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask1, indexes + 32);
    index_zmm[5]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask2, indexes + 40);
    index_zmm[6]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask3, indexes + 48);
    index_zmm[7]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask4, indexes + 56);
    key_zmm[4] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[4], index_zmm[4]);
    key_zmm[5] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[5], index_zmm[5]);
    key_zmm[6] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[6], index_zmm[6]);
    key_zmm[7] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[7], index_zmm[7]);

    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[0], key_zmm[1], index_zmm[0], index_zmm[1]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[2], key_zmm[3], index_zmm[2], index_zmm[3]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[4], key_zmm[5], index_zmm[4], index_zmm[5]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[6], key_zmm[7], index_zmm[6], index_zmm[7]);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm + 4, index_zmm + 4);
    bitonic_merge_eight_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm);

    vtype2::storeu(indexes, index_zmm[0]);
    vtype2::storeu(indexes + 8, index_zmm[1]);
    vtype2::storeu(indexes + 16, index_zmm[2]);
    vtype2::storeu(indexes + 24, index_zmm[3]);
    vtype2::mask_storeu(indexes + 32, load_mask1, index_zmm[4]);
    vtype2::mask_storeu(indexes + 40, load_mask2, index_zmm[5]);
    vtype2::mask_storeu(indexes + 48, load_mask3, index_zmm[6]);
    vtype2::mask_storeu(indexes + 56, load_mask4, index_zmm[7]);

    vtype1::storeu(keys, key_zmm[0]);
    vtype1::storeu(keys + 8, key_zmm[1]);
    vtype1::storeu(keys + 16, key_zmm[2]);
    vtype1::storeu(keys + 24, key_zmm[3]);
    vtype1::mask_storeu(keys + 32, load_mask1, key_zmm[4]);
    vtype1::mask_storeu(keys + 40, load_mask2, key_zmm[5]);
    vtype1::mask_storeu(keys + 48, load_mask3, key_zmm[6]);
    vtype1::mask_storeu(keys + 56, load_mask4, key_zmm[7]);
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
sort_128_64bit(type1_t *keys, type2_t *indexes, int32_t N)
{
    if (N <= 64) {
        sort_64_64bit<vtype1, vtype2>(keys, indexes, N);
        return;
    }
    using zmm_t = typename vtype1::zmm_t;
    using index_type = typename vtype2::zmm_t;
    using opmask_t = typename vtype1::opmask_t;
    zmm_t key_zmm[16];
    index_type index_zmm[16];

    key_zmm[0] = vtype1::loadu(keys);
    key_zmm[1] = vtype1::loadu(keys + 8);
    key_zmm[2] = vtype1::loadu(keys + 16);
    key_zmm[3] = vtype1::loadu(keys + 24);
    key_zmm[4] = vtype1::loadu(keys + 32);
    key_zmm[5] = vtype1::loadu(keys + 40);
    key_zmm[6] = vtype1::loadu(keys + 48);
    key_zmm[7] = vtype1::loadu(keys + 56);

    index_zmm[0] = vtype2::loadu(indexes);
    index_zmm[1] = vtype2::loadu(indexes + 8);
    index_zmm[2] = vtype2::loadu(indexes + 16);
    index_zmm[3] = vtype2::loadu(indexes + 24);
    index_zmm[4] = vtype2::loadu(indexes + 32);
    index_zmm[5] = vtype2::loadu(indexes + 40);
    index_zmm[6] = vtype2::loadu(indexes + 48);
    index_zmm[7] = vtype2::loadu(indexes + 56);
    key_zmm[0] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[0], index_zmm[0]);
    key_zmm[1] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[1], index_zmm[1]);
    key_zmm[2] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[2], index_zmm[2]);
    key_zmm[3] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[3], index_zmm[3]);
    key_zmm[4] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[4], index_zmm[4]);
    key_zmm[5] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[5], index_zmm[5]);
    key_zmm[6] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[6], index_zmm[6]);
    key_zmm[7] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[7], index_zmm[7]);

    opmask_t load_mask1 = 0xFF, load_mask2 = 0xFF;
    opmask_t load_mask3 = 0xFF, load_mask4 = 0xFF;
    opmask_t load_mask5 = 0xFF, load_mask6 = 0xFF;
    opmask_t load_mask7 = 0xFF, load_mask8 = 0xFF;
    if (N != 128) {
        uint64_t combined_mask = (0x1ull << (N - 64)) - 0x1ull;
        load_mask1 = (combined_mask)&0xFF;
        load_mask2 = (combined_mask >> 8) & 0xFF;
        load_mask3 = (combined_mask >> 16) & 0xFF;
        load_mask4 = (combined_mask >> 24) & 0xFF;
        load_mask5 = (combined_mask >> 32) & 0xFF;
        load_mask6 = (combined_mask >> 40) & 0xFF;
        load_mask7 = (combined_mask >> 48) & 0xFF;
        load_mask8 = (combined_mask >> 56) & 0xFF;
    }
    key_zmm[8] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask1, keys + 64);
    key_zmm[9] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask2, keys + 72);
    key_zmm[10] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask3, keys + 80);
    key_zmm[11] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask4, keys + 88);
    key_zmm[12] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask5, keys + 96);
    key_zmm[13] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask6, keys + 104);
    key_zmm[14] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask7, keys + 112);
    key_zmm[15] = vtype1::mask_loadu(vtype1::zmm_max(), load_mask8, keys + 120);

    index_zmm[8]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask1, indexes + 64);
    index_zmm[9]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask2, indexes + 72);
    index_zmm[10]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask3, indexes + 80);
    index_zmm[11]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask4, indexes + 88);
    index_zmm[12]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask5, indexes + 96);
    index_zmm[13]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask6, indexes + 104);
    index_zmm[14]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask7, indexes + 112);
    index_zmm[15]
            = vtype2::mask_loadu(vtype2::zmm_max(), load_mask8, indexes + 120);
    key_zmm[8] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[8], index_zmm[8]);
    key_zmm[9] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[9], index_zmm[9]);
    key_zmm[10] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[10], index_zmm[10]);
    key_zmm[11] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[11], index_zmm[11]);
    key_zmm[12] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[12], index_zmm[12]);
    key_zmm[13] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[13], index_zmm[13]);
    key_zmm[14] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[14], index_zmm[14]);
    key_zmm[15] = sort_zmm_64bit<vtype1, vtype2>(key_zmm[15], index_zmm[15]);

    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[0], key_zmm[1], index_zmm[0], index_zmm[1]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[2], key_zmm[3], index_zmm[2], index_zmm[3]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[4], key_zmm[5], index_zmm[4], index_zmm[5]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[6], key_zmm[7], index_zmm[6], index_zmm[7]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[8], key_zmm[9], index_zmm[8], index_zmm[9]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[10], key_zmm[11], index_zmm[10], index_zmm[11]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[12], key_zmm[13], index_zmm[12], index_zmm[13]);
    bitonic_merge_two_zmm_64bit<vtype1, vtype2>(
            key_zmm[14], key_zmm[15], index_zmm[14], index_zmm[15]);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm + 4, index_zmm + 4);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm + 8, index_zmm + 8);
    bitonic_merge_four_zmm_64bit<vtype1, vtype2>(key_zmm + 12, index_zmm + 12);
    bitonic_merge_eight_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm);
    bitonic_merge_eight_zmm_64bit<vtype1, vtype2>(key_zmm + 8, index_zmm + 8);
    bitonic_merge_sixteen_zmm_64bit<vtype1, vtype2>(key_zmm, index_zmm);
    vtype2::storeu(indexes, index_zmm[0]);
    vtype2::storeu(indexes + 8, index_zmm[1]);
    vtype2::storeu(indexes + 16, index_zmm[2]);
    vtype2::storeu(indexes + 24, index_zmm[3]);
    vtype2::storeu(indexes + 32, index_zmm[4]);
    vtype2::storeu(indexes + 40, index_zmm[5]);
    vtype2::storeu(indexes + 48, index_zmm[6]);
    vtype2::storeu(indexes + 56, index_zmm[7]);
    vtype2::mask_storeu(indexes + 64, load_mask1, index_zmm[8]);
    vtype2::mask_storeu(indexes + 72, load_mask2, index_zmm[9]);
    vtype2::mask_storeu(indexes + 80, load_mask3, index_zmm[10]);
    vtype2::mask_storeu(indexes + 88, load_mask4, index_zmm[11]);
    vtype2::mask_storeu(indexes + 96, load_mask5, index_zmm[12]);
    vtype2::mask_storeu(indexes + 104, load_mask6, index_zmm[13]);
    vtype2::mask_storeu(indexes + 112, load_mask7, index_zmm[14]);
    vtype2::mask_storeu(indexes + 120, load_mask8, index_zmm[15]);

    vtype1::storeu(keys, key_zmm[0]);
    vtype1::storeu(keys + 8, key_zmm[1]);
    vtype1::storeu(keys + 16, key_zmm[2]);
    vtype1::storeu(keys + 24, key_zmm[3]);
    vtype1::storeu(keys + 32, key_zmm[4]);
    vtype1::storeu(keys + 40, key_zmm[5]);
    vtype1::storeu(keys + 48, key_zmm[6]);
    vtype1::storeu(keys + 56, key_zmm[7]);
    vtype1::mask_storeu(keys + 64, load_mask1, key_zmm[8]);
    vtype1::mask_storeu(keys + 72, load_mask2, key_zmm[9]);
    vtype1::mask_storeu(keys + 80, load_mask3, key_zmm[10]);
    vtype1::mask_storeu(keys + 88, load_mask4, key_zmm[11]);
    vtype1::mask_storeu(keys + 96, load_mask5, key_zmm[12]);
    vtype1::mask_storeu(keys + 104, load_mask6, key_zmm[13]);
    vtype1::mask_storeu(keys + 112, load_mask7, key_zmm[14]);
    vtype1::mask_storeu(keys + 120, load_mask8, key_zmm[15]);
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
void heapify(type1_t *keys, type2_t *indexes, int64_t idx, int64_t size)
{
    int64_t i = idx;
    while (true) {
        int64_t j = 2 * i + 1;
        if (j >= size || j < 0) { break; }
        int k = j + 1;
        if (k < size && keys[j] < keys[k]) { j = k; }
        if (keys[j] < keys[i]) { break; }
        std::swap(keys[i], keys[j]);
        std::swap(indexes[i], indexes[j]);
        i = j;
    }
}
template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
void heap_sort(type1_t *keys, type2_t *indexes, int64_t size)
{
    for (int64_t i = size / 2 - 1; i >= 0; i--) {
        heapify<vtype1, vtype2>(keys, indexes, i, size);
    }
    for (int64_t i = size - 1; i > 0; i--) {
        std::swap(keys[0], keys[i]);
        std::swap(indexes[0], indexes[i]);
        heapify<vtype1, vtype2>(keys, indexes, 0, i);
    }
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
void qsort_64bit_(type1_t *keys,
                  type2_t *indexes,
                  int64_t left,
                  int64_t right,
                  int64_t max_iters)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        //std::sort(keys+left,keys+right+1);
        heap_sort<vtype1, vtype2>(
                keys + left, indexes + left, right - left + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 128
     */
    if (right + 1 - left <= 128) {

        sort_128_64bit<vtype1, vtype2>(
                keys + left, indexes + left, (int32_t)(right + 1 - left));
        return;
    }

    type1_t pivot = get_pivot_64bit<vtype1>(keys, left, right);
    type1_t smallest = vtype1::type_max();
    type1_t biggest = vtype1::type_min();
    int64_t pivot_index = partition_avx512<vtype1, vtype2>(
            keys, indexes, left, right + 1, pivot, &smallest, &biggest);
    if (pivot != smallest) {
        qsort_64bit_<vtype1, vtype2>(
                keys, indexes, left, pivot_index - 1, max_iters - 1);
    }
    if (pivot != biggest) {
        qsort_64bit_<vtype1, vtype2>(
                keys, indexes, pivot_index, right, max_iters - 1);
    }
}

template <>
void avx512_qsort_kv<int64_t>(int64_t *keys, uint64_t *indexes, int64_t arrsize)
{
    if (arrsize > 1) {
        qsort_64bit_<zmm_vector<int64_t>, zmm_vector<uint64_t>>(
                keys, indexes, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
    }
}

template <>
void avx512_qsort_kv<uint64_t>(uint64_t *keys,
                               uint64_t *indexes,
                               int64_t arrsize)
{
    if (arrsize > 1) {
        qsort_64bit_<zmm_vector<uint64_t>, zmm_vector<uint64_t>>(
                keys, indexes, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
    }
}

template <>
void avx512_qsort_kv<double>(double *keys, uint64_t *indexes, int64_t arrsize)
{
    if (arrsize > 1) {
        int64_t nan_count = replace_nan_with_inf(keys, arrsize);
        qsort_64bit_<zmm_vector<double>, zmm_vector<uint64_t>>(
                keys, indexes, 0, arrsize - 1, 2 * (int64_t)log2(arrsize));
        replace_inf_with_nan(keys, arrsize, nan_count);
    }
}
#endif // AVX512_QSORT_64BIT_KV
