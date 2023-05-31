/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Raghuveer Devulapalli <raghuveer.devulapalli@intel.com>
 * ****************************************************************/

#ifndef AVX512_ARGSORT_COMMON
#define AVX512_ARGSORT_COMMON

#include "avx512-64bit-common.h"
#include <numeric>
#include <stdio.h>
#include <vector>

using argtype = zmm_vector<int64_t>;
using argzmm_t = typename argtype::zmm_t;

template <typename T>
void avx512_argsort(T *arr, int64_t *arg, int64_t arrsize);

template <typename T>
std::vector<int64_t> avx512_argsort(T *arr, int64_t arrsize);

/*
 * Parition one ZMM register based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype, typename type_t, typename zmm_t>
static inline int32_t partition_vec(type_t *arg,
                                    int64_t left,
                                    int64_t right,
                                    const argzmm_t arg_vec,
                                    const zmm_t curr_vec,
                                    const zmm_t pivot_vec,
                                    zmm_t *smallest_vec,
                                    zmm_t *biggest_vec)
{
    /* which elements are larger than the pivot */
    typename vtype::opmask_t gt_mask = vtype::ge(curr_vec, pivot_vec);
    int32_t amount_gt_pivot = _mm_popcnt_u32((int32_t)gt_mask);
    argtype::mask_compressstoreu(
            arg + left, vtype::knot_opmask(gt_mask), arg_vec);
    argtype::mask_compressstoreu(arg + right - amount_gt_pivot, gt_mask, arg_vec);
    *smallest_vec = vtype::min(curr_vec, *smallest_vec);
    *biggest_vec = vtype::max(curr_vec, *biggest_vec);
    return amount_gt_pivot;
}
/*
 * Parition an array based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype, typename type_t>
static inline int64_t partition_avx512(type_t *arr,
                                       int64_t *arg,
                                       int64_t left,
                                       int64_t right,
                                       type_t pivot,
                                       type_t *smallest,
                                       type_t *biggest)
{
    /* make array length divisible by vtype::numlanes , shortening the array */
    for (int32_t i = (right - left) % vtype::numlanes; i > 0; --i) {
        *smallest = std::min(*smallest, arr[arg[left]], comparison_func<vtype>);
        *biggest = std::max(*biggest, arr[arg[left]], comparison_func<vtype>);
        if (!comparison_func<vtype>(arr[arg[left]], pivot)) {
            std::swap(arg[left], arg[--right]);
        }
        else {
            ++left;
        }
    }

    if (left == right)
        return left; /* less than vtype::numlanes elements in the array */

    using zmm_t = typename vtype::zmm_t;
    zmm_t pivot_vec = vtype::set1(pivot);
    zmm_t min_vec = vtype::set1(*smallest);
    zmm_t max_vec = vtype::set1(*biggest);

    if (right - left == vtype::numlanes) {
        argzmm_t argvec = argtype::loadu(arg + left);
        zmm_t vec = vtype::template i64gather<sizeof(type_t)>(argvec, arr);
        int32_t amount_gt_pivot = partition_vec<vtype>(arg,
                                                       left,
                                                       left + vtype::numlanes,
                                                       argvec,
                                                       vec,
                                                       pivot_vec,
                                                       &min_vec,
                                                       &max_vec);
        *smallest = vtype::reducemin(min_vec);
        *biggest = vtype::reducemax(max_vec);
        return left + (vtype::numlanes - amount_gt_pivot);
    }

    // first and last vtype::numlanes values are partitioned at the end
    argzmm_t argvec_left = argtype::loadu(arg + left);
    zmm_t vec_left
            = vtype::template i64gather<sizeof(type_t)>(argvec_left, arr);
    argzmm_t argvec_right = argtype::loadu(arg + (right - vtype::numlanes));
    zmm_t vec_right
            = vtype::template i64gather<sizeof(type_t)>(argvec_right, arr);
    // store points of the vectors
    int64_t r_store = right - vtype::numlanes;
    int64_t l_store = left;
    // indices for loading the elements
    left += vtype::numlanes;
    right -= vtype::numlanes;
    while (right - left != 0) {
        argzmm_t arg_vec;
        zmm_t curr_vec;
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype::numlanes) - right < left - l_store) {
            right -= vtype::numlanes;
            arg_vec = argtype::loadu(arg + right);
            curr_vec = vtype::template i64gather<sizeof(type_t)>(arg_vec, arr);
        }
        else {
            arg_vec = argtype::loadu(arg + left);
            curr_vec = vtype::template i64gather<sizeof(type_t)>(arg_vec, arr);
            left += vtype::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        int32_t amount_gt_pivot
                = partition_vec<vtype>(arg,
                                       l_store,
                                       r_store + vtype::numlanes,
                                       arg_vec,
                                       curr_vec,
                                       pivot_vec,
                                       &min_vec,
                                       &max_vec);
        ;
        r_store -= amount_gt_pivot;
        l_store += (vtype::numlanes - amount_gt_pivot);
    }

    /* partition and save vec_left and vec_right */
    int32_t amount_gt_pivot = partition_vec<vtype>(arg,
                                                   l_store,
                                                   r_store + vtype::numlanes,
                                                   argvec_left,
                                                   vec_left,
                                                   pivot_vec,
                                                   &min_vec,
                                                   &max_vec);
    l_store += (vtype::numlanes - amount_gt_pivot);
    amount_gt_pivot = partition_vec<vtype>(arg,
                                           l_store,
                                           l_store + vtype::numlanes,
                                           argvec_right,
                                           vec_right,
                                           pivot_vec,
                                           &min_vec,
                                           &max_vec);
    l_store += (vtype::numlanes - amount_gt_pivot);
    *smallest = vtype::reducemin(min_vec);
    *biggest = vtype::reducemax(max_vec);
    return l_store;
}

template <typename vtype,
          int num_unroll,
          typename type_t = typename vtype::type_t>
static inline int64_t partition_avx512_unrolled(type_t *arr,
                                                int64_t *arg,
                                                int64_t left,
                                                int64_t right,
                                                type_t pivot,
                                                type_t *smallest,
                                                type_t *biggest)
{
    if (right - left <= 8 * num_unroll * vtype::numlanes) {
        return partition_avx512<vtype>(
                arr, arg, left, right, pivot, smallest, biggest);
    }
    /* make array length divisible by vtype::numlanes , shortening the array */
    for (int32_t i = ((right - left) % (num_unroll * vtype::numlanes)); i > 0;
         --i) {
        *smallest = std::min(*smallest, arr[arg[left]], comparison_func<vtype>);
        *biggest = std::max(*biggest, arr[arg[left]], comparison_func<vtype>);
        if (!comparison_func<vtype>(arr[arg[left]], pivot)) {
            std::swap(arg[left], arg[--right]);
        }
        else {
            ++left;
        }
    }

    if (left == right)
        return left; /* less than vtype::numlanes elements in the array */

    using zmm_t = typename vtype::zmm_t;
    zmm_t pivot_vec = vtype::set1(pivot);
    zmm_t min_vec = vtype::set1(*smallest);
    zmm_t max_vec = vtype::set1(*biggest);

    // first and last vtype::numlanes values are partitioned at the end
    zmm_t vec_left[num_unroll], vec_right[num_unroll];
    argzmm_t argvec_left[num_unroll], argvec_right[num_unroll];
#pragma UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        argvec_left[ii] = argtype::loadu(arg + left + vtype::numlanes * ii);
        vec_left[ii] = vtype::template i64gather<sizeof(type_t)>(
                argvec_left[ii], arr);
        argvec_right[ii] = argtype::loadu(
                arg + (right - vtype::numlanes * (num_unroll - ii)));
        vec_right[ii] = vtype::template i64gather<sizeof(type_t)>(
                argvec_right[ii], arr);
    }
    // store points of the vectors
    int64_t r_store = right - vtype::numlanes;
    int64_t l_store = left;
    // indices for loading the elements
    left += num_unroll * vtype::numlanes;
    right -= num_unroll * vtype::numlanes;
    while (right - left != 0) {
        argzmm_t arg_vec[num_unroll];
        zmm_t curr_vec[num_unroll];
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype::numlanes) - right < left - l_store) {
            right -= num_unroll * vtype::numlanes;
#pragma UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                arg_vec[ii] = argtype::loadu(arg + right + ii * vtype::numlanes);
                curr_vec[ii] = vtype::template i64gather<sizeof(type_t)>(
                        arg_vec[ii], arr);
            }
        }
        else {
#pragma UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                arg_vec[ii] = argtype::loadu(arg + left + ii * vtype::numlanes);
                curr_vec[ii] = vtype::template i64gather<sizeof(type_t)>(
                        arg_vec[ii], arr);
            }
            left += num_unroll * vtype::numlanes;
        }
        // partition the current vector and save it on both sides of the array
#pragma UNROLL_LOOP(8)
        for (int ii = 0; ii < num_unroll; ++ii) {
            int32_t amount_gt_pivot
                    = partition_vec<vtype>(arg,
                                           l_store,
                                           r_store + vtype::numlanes,
                                           arg_vec[ii],
                                           curr_vec[ii],
                                           pivot_vec,
                                           &min_vec,
                                           &max_vec);
            l_store += (vtype::numlanes - amount_gt_pivot);
            r_store -= amount_gt_pivot;
        }
    }

    /* partition and save vec_left and vec_right */
#pragma UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype>(arg,
                                       l_store,
                                       r_store + vtype::numlanes,
                                       argvec_left[ii],
                                       vec_left[ii],
                                       pivot_vec,
                                       &min_vec,
                                       &max_vec);
        l_store += (vtype::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
#pragma UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype>(arg,
                                       l_store,
                                       r_store + vtype::numlanes,
                                       argvec_right[ii],
                                       vec_right[ii],
                                       pivot_vec,
                                       &min_vec,
                                       &max_vec);
        l_store += (vtype::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    *smallest = vtype::reducemin(min_vec);
    *biggest = vtype::reducemax(max_vec);
    return l_store;
}
#endif // AVX512_ARGSORT_COMMON
