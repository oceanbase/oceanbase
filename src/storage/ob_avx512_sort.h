/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_STORAGE_OB_AVX512_SORT_H_
#define OCEANBASE_STORAGE_OB_AVX512_SORT_H_

#include <immintrin.h>
#include <climits>
#include <cfloat>
#include <cstring>

#include "share/ob_define.h"

namespace oceanbase {
namespace storage {

inline void CoreSmallSort(__m512i& input, __m512i& values){
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(4, 5, 6, 7, 0, 1, 2, 3);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }

    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
}

inline void CoreSmallSort(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ ptrVal){
    __m512i v = _mm512_loadu_si512(ptr1);
    __m512i v_val = _mm512_loadu_si512(ptrVal);
    CoreSmallSort(v, v_val);
    _mm512_storeu_si512(ptr1, v);
    _mm512_storeu_si512(ptrVal, v_val);
}


inline void CoreExchangeSort2V(__m512i& input, __m512i& input2,
                                 __m512i& input_val, __m512i& input2_val){
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i tmp_input = _mm512_min_epu64(input2, permNeigh);
        __m512i tmp_input2 = _mm512_max_epu64(input2, permNeigh);

        __m512i input_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input_val);
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, permNeigh, _MM_CMPINT_EQ ),
                                       input_val_perm);
        input2_val = _mm512_mask_mov_epi64(input_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                        input2_val);

         input = tmp_input;
         input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                        input2_val);

         input = tmp_input;
         input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                        input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
}



inline void CoreSmallSort2(__m512i& input, __m512i& input2,
                                 __m512i& input_val, __m512i& input2_val){
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(4, 5, 6, 7, 0, 1, 2, 3);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);

        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    CoreExchangeSort2V(input,input2,input_val,input2_val);
}


inline void CoreSmallSort2(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input1 = _mm512_loadu_si512(ptr1);
    __m512i input2 = _mm512_loadu_si512(ptr1+8);
    __m512i input1_val = _mm512_loadu_si512(values);
    __m512i input2_val = _mm512_loadu_si512(values+8);
    CoreSmallSort2(input1, input2, input1_val, input2_val);
    _mm512_storeu_si512(ptr1, input1);
    _mm512_storeu_si512(ptr1+8, input2);
    _mm512_storeu_si512(values, input1_val);
    _mm512_storeu_si512(values+8, input2_val);
}


inline void CoreSmallSort3(__m512i& input, __m512i& input2, __m512i& input3,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val){
    CoreSmallSort2(input, input2, input_val, input2_val);
    CoreSmallSort(input3, input3_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i tmp_input3 = _mm512_max_epu64(input2, permNeigh);
        __m512i tmp_input2 = _mm512_min_epu64(input2, permNeigh);
        __m512i input3_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input3_val);

        input3_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input3, permNeigh, _MM_CMPINT_EQ ),
                                       input3_val_perm);
        input2_val = _mm512_mask_mov_epi64(input3_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input3 = tmp_input3;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);

        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
    }
}


inline void CoreSmallSort3(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values){
    __m512i input1 = _mm512_loadu_si512(ptr1);
    __m512i input2 = _mm512_loadu_si512(ptr1+8);
    __m512i input3 = _mm512_loadu_si512(ptr1+16);
    __m512i input1_val = _mm512_loadu_si512(values);
    __m512i input2_val = _mm512_loadu_si512(values+8);
    __m512i input3_val = _mm512_loadu_si512(values+16);
    CoreSmallSort3(input1, input2, input3,
                         input1_val, input2_val, input3_val);
    _mm512_storeu_si512(ptr1, input1);
    _mm512_storeu_si512(ptr1+8, input2);
    _mm512_storeu_si512(ptr1+16, input3);
    _mm512_storeu_si512(values, input1_val);
    _mm512_storeu_si512(values+8, input2_val);
    _mm512_storeu_si512(values+16, input3_val);
}



inline void CoreSmallSort4(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val){
    CoreSmallSort2(input, input2, input_val, input2_val);
    CoreSmallSort2(input3, input4, input3_val, input4_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);

        __m512i tmp_input4 = _mm512_max_epu64(input, permNeigh4);
        __m512i tmp_input = _mm512_min_epu64(input, permNeigh4);

        __m512i tmp_input3 = _mm512_max_epu64(input2, permNeigh3);
        __m512i tmp_input2 = _mm512_min_epu64(input2, permNeigh3);
        __m512i input4_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input4_val);
        input4_val = _mm512_mask_mov_epi64(input_val, _mm512_cmp_epu64_mask(tmp_input4, permNeigh4, _MM_CMPINT_EQ ),
                                       input4_val_perm);
        input_val = _mm512_mask_mov_epi64(input4_val_perm, _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);

        __m512i input3_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input3_val);
        input3_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input3, permNeigh3, _MM_CMPINT_EQ ),
                                       input3_val_perm);
        input2_val = _mm512_mask_mov_epi64(input3_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);

        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);

        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);
        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
}


inline void CoreSmallSort4(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input1 = _mm512_loadu_si512(ptr1);
    __m512i input2 = _mm512_loadu_si512(ptr1+8);
    __m512i input3 = _mm512_loadu_si512(ptr1+16);
    __m512i input4 = _mm512_loadu_si512(ptr1+24);
    __m512i input1_val = _mm512_loadu_si512(values);
    __m512i input2_val = _mm512_loadu_si512(values+8);
    __m512i input3_val = _mm512_loadu_si512(values+16);
    __m512i input4_val = _mm512_loadu_si512(values+24);
    CoreSmallSort4(input1, input2, input3, input4,
                         input1_val, input2_val, input3_val, input4_val);
    _mm512_storeu_si512(ptr1, input1);
    _mm512_storeu_si512(ptr1+8, input2);
    _mm512_storeu_si512(ptr1+16, input3);
    _mm512_storeu_si512(ptr1+24, input4);
    _mm512_storeu_si512(values, input1_val);
    _mm512_storeu_si512(values+8, input2_val);
    _mm512_storeu_si512(values+16, input3_val);
    _mm512_storeu_si512(values+24, input4_val);
}


inline void CoreSmallSort5(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4, __m512i& input5,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val, __m512i& input5_val){
    CoreSmallSort4(input, input2, input3, input4,
                         input_val, input2_val, input3_val, input4_val);
    CoreSmallSort(input5, input5_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);

        __m512i tmp_input5 = _mm512_max_epu64(input4, permNeigh5);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh5);

        __m512i input5_val_copy = _mm512_permutexvar_epi64(idxNoNeigh, input5_val);
        input5_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input5, permNeigh5, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input4_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input5 = tmp_input5;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);

        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);

        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);

        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);

        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
    }
}


inline void CoreSmallSort5(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values){
    __m512i input1 = _mm512_loadu_si512(ptr1);
    __m512i input2 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input1_val = _mm512_loadu_si512(values);
    __m512i input2_val = _mm512_loadu_si512(values+1*8);
    __m512i input3_val = _mm512_loadu_si512(values+2*8);
    __m512i input4_val = _mm512_loadu_si512(values+3*8);
    __m512i input5_val = _mm512_loadu_si512(values+4*8);
    CoreSmallSort5(input1, input2, input3, input4, input5,
                    input1_val, input2_val, input3_val, input4_val, input5_val);
    _mm512_storeu_si512(ptr1, input1);
    _mm512_storeu_si512(ptr1+1*8, input2);
    _mm512_storeu_si512(ptr1+2*8, input3);
    _mm512_storeu_si512(ptr1+3*8, input4);
    _mm512_storeu_si512(ptr1+4*8, input5);
    _mm512_storeu_si512(values, input1_val);
    _mm512_storeu_si512(values+1*8, input2_val);
    _mm512_storeu_si512(values+2*8, input3_val);
    _mm512_storeu_si512(values+3*8, input4_val);
    _mm512_storeu_si512(values+4*8, input5_val);
}



inline void CoreSmallSort6(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                                             __m512i& input5_val, __m512i& input6_val){
    CoreSmallSort4(input, input2, input3, input4,
                         input_val, input2_val, input3_val, input4_val);
    CoreSmallSort2(input5, input6, input5_val, input6_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);

        __m512i tmp_input5 = _mm512_max_epu64(input4, permNeigh5);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh5);

        __m512i tmp_input6 = _mm512_max_epu64(input3, permNeigh6);
        __m512i tmp_input3 = _mm512_min_epu64(input3, permNeigh6);


        __m512i input5_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input5_val);
        input5_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input5, permNeigh5, _MM_CMPINT_EQ ),
                                       input5_val_perm);
        input4_val = _mm512_mask_mov_epi64(input5_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        __m512i input6_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input6_val);
        input6_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input6, permNeigh6, _MM_CMPINT_EQ ),
                                       input6_val_perm);
        input3_val = _mm512_mask_mov_epi64(input6_val_perm, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input5 = tmp_input5;
        input4 = tmp_input4;

        input6 = tmp_input6;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input6_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xF0, permNeighMax6);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);

        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xCC, permNeighMax6);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xAA, permNeighMax6);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
    }
}


inline void CoreSmallSort6(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    CoreSmallSort6(input0,input1,input2,input3,input4,input5,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
}



inline void CoreSmallSort7(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                __m512i& input5_val, __m512i& input6_val, __m512i& input7_val){
    CoreSmallSort4(input, input2, input3, input4,
                         input_val, input2_val, input3_val, input4_val);
    CoreSmallSort3(input5, input6, input7,
                         input5_val, input6_val, input7_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);

        __m512i tmp_input5 = _mm512_max_epu64(input4, permNeigh5);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh5);

        __m512i tmp_input6 = _mm512_max_epu64(input3, permNeigh6);
        __m512i tmp_input3 = _mm512_min_epu64(input3, permNeigh6);

        __m512i tmp_input7 = _mm512_max_epu64(input2, permNeigh7);
        __m512i tmp_input2 = _mm512_min_epu64(input2, permNeigh7);


        __m512i input5_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input5_val);
        input5_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input5, permNeigh5, _MM_CMPINT_EQ ),
                                       input5_val_perm);
        input4_val = _mm512_mask_mov_epi64(input5_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        __m512i input6_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input6_val);
        input6_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input6, permNeigh6, _MM_CMPINT_EQ ),
                                       input6_val_perm);
        input3_val = _mm512_mask_mov_epi64(input6_val_perm, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        __m512i input7_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input7_val);
        input7_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input7, permNeigh7, _MM_CMPINT_EQ ),
                                       input7_val_perm);
        input2_val = _mm512_mask_mov_epi64(input7_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);


        input5 = tmp_input5;
        input4 = tmp_input4;

        input6 = tmp_input6;
        input3 = tmp_input3;

        input7 = tmp_input7;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input7_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input5 = tmp_input5;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input6_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xF0, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xF0, permNeighMax7);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xCC, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xCC, permNeighMax7);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);

        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xAA, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xAA, permNeighMax7);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
    }
}


inline void CoreSmallSort7(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    CoreSmallSort7(input0,input1,input2,input3,input4,input5,input6,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
}



inline void CoreSmallSort8(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                 __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val){
    CoreSmallSort4(input, input2, input3, input4,
                         input_val, input2_val, input3_val, input4_val);
    CoreSmallSort4(input5, input6, input7, input8,
                         input5_val, input6_val, input7_val, input8_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);

        __m512i tmp_input5 = _mm512_max_epu64(input4, permNeigh5);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh5);

        __m512i tmp_input6 = _mm512_max_epu64(input3, permNeigh6);
        __m512i tmp_input3 = _mm512_min_epu64(input3, permNeigh6);

        __m512i tmp_input7 = _mm512_max_epu64(input2, permNeigh7);
        __m512i tmp_input2 = _mm512_min_epu64(input2, permNeigh7);

        __m512i tmp_input8 = _mm512_max_epu64(input, permNeigh8);
        __m512i tmp_input = _mm512_min_epu64(input, permNeigh8);


        __m512i input5_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input5_val);
        input5_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input5, permNeigh5, _MM_CMPINT_EQ ),
                                       input5_val_perm);
        input4_val = _mm512_mask_mov_epi64(input5_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        __m512i input6_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input6_val);
        input6_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input6, permNeigh6, _MM_CMPINT_EQ ),
                                       input6_val_perm);
        input3_val = _mm512_mask_mov_epi64(input6_val_perm, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        __m512i input7_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input7_val);
        input7_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input7, permNeigh7, _MM_CMPINT_EQ ),
                                       input7_val_perm);
        input2_val = _mm512_mask_mov_epi64(input7_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        __m512i input8_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input8_val);
        input8_val = _mm512_mask_mov_epi64(input_val, _mm512_cmp_epu64_mask(tmp_input8, permNeigh8, _MM_CMPINT_EQ ),
                                       input8_val_perm);
        input_val = _mm512_mask_mov_epi64(input8_val_perm, _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);


        input5 = tmp_input5;
        input4 = tmp_input4;

        input6 = tmp_input6;
        input3 = tmp_input3;

        input7 = tmp_input7;
        input2 = tmp_input2;

        input8 = tmp_input8;
        input = tmp_input;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input7_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input5 = tmp_input5;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input6;
        __m512i tmp_input6 = _mm512_min_epu64(input8, inputCopy);
        __m512i tmp_input8 = _mm512_max_epu64(input8, inputCopy);
        __m512i input6_val_copy = input6_val;
        input6_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input6, inputCopy, _MM_CMPINT_EQ ),
                                       input6_val_copy);
        input8_val = _mm512_mask_mov_epi64(input6_val_copy, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        input6 = tmp_input6;
        input8 = tmp_input8;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input6_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i inputCopy = input7;
        __m512i tmp_input7 = _mm512_min_epu64(input8, inputCopy);
        __m512i tmp_input8 = _mm512_max_epu64(input8, inputCopy);
        __m512i input7_val_copy = input7_val;
        input7_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input7, inputCopy, _MM_CMPINT_EQ ),
                                       input7_val_copy);
        input8_val = _mm512_mask_mov_epi64(input7_val_copy, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        input7 = tmp_input7;
        input8 = tmp_input8;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMin8 = _mm512_min_epu64(permNeigh8, input8);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i permNeighMax8 = _mm512_max_epu64(permNeigh8, input8);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xF0, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xF0, permNeighMax7);
        __m512i tmp_input8 = _mm512_mask_mov_epi64(permNeighMin8, 0xF0, permNeighMax8);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input8_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input8_val), _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
        input8_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
        input8 = tmp_input8;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMin8 = _mm512_min_epu64(permNeigh8, input8);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i permNeighMax8 = _mm512_max_epu64(permNeigh8, input8);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xCC, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xCC, permNeighMax7);
        __m512i tmp_input8 = _mm512_mask_mov_epi64(permNeighMin8, 0xCC, permNeighMax8);
        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input8_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input8_val), _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
        input8_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
        input8 = tmp_input8;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMin8 = _mm512_min_epu64(permNeigh8, input8);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i permNeighMax8 = _mm512_max_epu64(permNeigh8, input8);

        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xAA, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xAA, permNeighMax7);
        __m512i tmp_input8 = _mm512_mask_mov_epi64(permNeighMin8, 0xAA, permNeighMax8);
        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input8_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input8_val), _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
        input8_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
        input8 = tmp_input8;
    }
}

inline void CoreSmallSort8(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    CoreSmallSort8(input0,input1,input2,input3,input4,input5,input6,input7,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
}



inline void CoreSmallEnd1(__m512i& input, __m512i& values){
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);

        values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, values), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       values);

        input = tmp_input;
    }
}

inline void CoreSmallEnd2(__m512i& input, __m512i& input2,
                                   __m512i& input_val, __m512i& input2_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
}

inline void CoreSmallEnd3(__m512i& input, __m512i& input2, __m512i& input3,
                                   __m512i& input_val, __m512i& input2_val, __m512i& input3_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
    }
}

inline void CoreSmallEnd4(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                                   __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
    }
}

inline void CoreSmallEnd5(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5,
                                   __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                   __m512i& input5_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input5, inputCopy);
        __m512i tmp_input5 = _mm512_max_epu64(input5, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input5_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input5 = tmp_input5;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
    }
}

inline void CoreSmallEnd6(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6,
                                   __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                   __m512i& input5_val, __m512i& input6_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input5, inputCopy);
        __m512i tmp_input5 = _mm512_max_epu64(input5, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input5_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input5 = tmp_input5;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input6_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input2 = tmp_input2;
        input6 = tmp_input6;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input6_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xF0, permNeighMax6);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xCC, permNeighMax6);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xAA, permNeighMax6);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
    }
}



inline void CoreSmallEnd7(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7,
                                   __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                   __m512i& input5_val, __m512i& input6_val, __m512i& input7_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input5, inputCopy);
        __m512i tmp_input5 = _mm512_max_epu64(input5, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input5_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input5 = tmp_input5;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input6_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input2 = tmp_input2;
        input6 = tmp_input6;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input7_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input3 = tmp_input3;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input7_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input5 = tmp_input5;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input7_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input5 = tmp_input5;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input6_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xF0, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xF0, permNeighMax7);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xCC, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xCC, permNeighMax7);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xAA, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xAA, permNeighMax7);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
    }
}



inline void CoreSmallEnd8(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                                   __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                   __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val){
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input5, inputCopy);
        __m512i tmp_input5 = _mm512_max_epu64(input5, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input5_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input = tmp_input;
        input5 = tmp_input5;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input6_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input2 = tmp_input2;
        input6 = tmp_input6;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input7_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input3 = tmp_input3;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input4;
        __m512i tmp_input4 = _mm512_min_epu64(input8, inputCopy);
        __m512i tmp_input8 = _mm512_max_epu64(input8, inputCopy);
        __m512i input4_val_copy = input4_val;
        input4_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input4, inputCopy, _MM_CMPINT_EQ ),
                                       input4_val_copy);
        input8_val = _mm512_mask_mov_epi64(input4_val_copy, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        input4 = tmp_input4;
        input8 = tmp_input8;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input7_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input5 = tmp_input5;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input3, inputCopy);
        __m512i tmp_input3 = _mm512_max_epu64(input3, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input3_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input = tmp_input;
        input3 = tmp_input3;
    }
    {
        __m512i inputCopy = input2;
        __m512i tmp_input2 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input2_val_copy = input2_val;
        input2_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input2, inputCopy, _MM_CMPINT_EQ ),
                                       input2_val_copy);
        input4_val = _mm512_mask_mov_epi64(input2_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input2 = tmp_input2;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input;
        __m512i tmp_input = _mm512_min_epu64(input2, inputCopy);
        __m512i tmp_input2 = _mm512_max_epu64(input2, inputCopy);
        __m512i input_val_copy = input_val;
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, inputCopy, _MM_CMPINT_EQ ),
                                       input_val_copy);
        input2_val = _mm512_mask_mov_epi64(input_val_copy, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i inputCopy = input3;
        __m512i tmp_input3 = _mm512_min_epu64(input4, inputCopy);
        __m512i tmp_input4 = _mm512_max_epu64(input4, inputCopy);
        __m512i input3_val_copy = input3_val;
        input3_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input3, inputCopy, _MM_CMPINT_EQ ),
                                       input3_val_copy);
        input4_val = _mm512_mask_mov_epi64(input3_val_copy, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input3 = tmp_input3;
        input4 = tmp_input4;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input7, inputCopy);
        __m512i tmp_input7 = _mm512_max_epu64(input7, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input7_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        input5 = tmp_input5;
        input7 = tmp_input7;
    }
    {
        __m512i inputCopy = input6;
        __m512i tmp_input6 = _mm512_min_epu64(input8, inputCopy);
        __m512i tmp_input8 = _mm512_max_epu64(input8, inputCopy);
        __m512i input6_val_copy = input6_val;
        input6_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input6, inputCopy, _MM_CMPINT_EQ ),
                                       input6_val_copy);
        input8_val = _mm512_mask_mov_epi64(input6_val_copy, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        input6 = tmp_input6;
        input8 = tmp_input8;
    }
    {
        __m512i inputCopy = input5;
        __m512i tmp_input5 = _mm512_min_epu64(input6, inputCopy);
        __m512i tmp_input6 = _mm512_max_epu64(input6, inputCopy);
        __m512i input5_val_copy = input5_val;
        input5_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input5, inputCopy, _MM_CMPINT_EQ ),
                                       input5_val_copy);
        input6_val = _mm512_mask_mov_epi64(input5_val_copy, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        input5 = tmp_input5;
        input6 = tmp_input6;
    }
    {
        __m512i inputCopy = input7;
        __m512i tmp_input7 = _mm512_min_epu64(input8, inputCopy);
        __m512i tmp_input8 = _mm512_max_epu64(input8, inputCopy);
        __m512i input7_val_copy = input7_val;
        input7_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input7, inputCopy, _MM_CMPINT_EQ ),
                                       input7_val_copy);
        input8_val = _mm512_mask_mov_epi64(input7_val_copy, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        input7 = tmp_input7;
        input8 = tmp_input8;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMin8 = _mm512_min_epu64(permNeigh8, input8);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i permNeighMax8 = _mm512_max_epu64(permNeigh8, input8);

        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xF0, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xF0, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xF0, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xF0, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xF0, permNeighMax7);
        __m512i tmp_input8 = _mm512_mask_mov_epi64(permNeighMin8, 0xF0, permNeighMax8);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input8_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input8_val), _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
        input8_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
        input8 = tmp_input8;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMin8 = _mm512_min_epu64(permNeigh8, input8);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i permNeighMax8 = _mm512_max_epu64(permNeigh8, input8);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xCC, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xCC, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xCC, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xCC, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xCC, permNeighMax7);
        __m512i tmp_input8 = _mm512_mask_mov_epi64(permNeighMin8, 0xCC, permNeighMax8);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input8_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input8_val), _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
        input8_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
        input8 = tmp_input8;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeigh3 = _mm512_permutexvar_epi64(idxNoNeigh, input3);
        __m512i permNeigh4 = _mm512_permutexvar_epi64(idxNoNeigh, input4);
        __m512i permNeigh5 = _mm512_permutexvar_epi64(idxNoNeigh, input5);
        __m512i permNeigh6 = _mm512_permutexvar_epi64(idxNoNeigh, input6);
        __m512i permNeigh7 = _mm512_permutexvar_epi64(idxNoNeigh, input7);
        __m512i permNeigh8 = _mm512_permutexvar_epi64(idxNoNeigh, input8);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMin3 = _mm512_min_epu64(permNeigh3, input3);
        __m512i permNeighMin4 = _mm512_min_epu64(permNeigh4, input4);
        __m512i permNeighMin5 = _mm512_min_epu64(permNeigh5, input5);
        __m512i permNeighMin6 = _mm512_min_epu64(permNeigh6, input6);
        __m512i permNeighMin7 = _mm512_min_epu64(permNeigh7, input7);
        __m512i permNeighMin8 = _mm512_min_epu64(permNeigh8, input8);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i permNeighMax3 = _mm512_max_epu64(permNeigh3, input3);
        __m512i permNeighMax4 = _mm512_max_epu64(permNeigh4, input4);
        __m512i permNeighMax5 = _mm512_max_epu64(permNeigh5, input5);
        __m512i permNeighMax6 = _mm512_max_epu64(permNeigh6, input6);
        __m512i permNeighMax7 = _mm512_max_epu64(permNeigh7, input7);
        __m512i permNeighMax8 = _mm512_max_epu64(permNeigh8, input8);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);
        __m512i tmp_input3 = _mm512_mask_mov_epi64(permNeighMin3, 0xAA, permNeighMax3);
        __m512i tmp_input4 = _mm512_mask_mov_epi64(permNeighMin4, 0xAA, permNeighMax4);
        __m512i tmp_input5 = _mm512_mask_mov_epi64(permNeighMin5, 0xAA, permNeighMax5);
        __m512i tmp_input6 = _mm512_mask_mov_epi64(permNeighMin6, 0xAA, permNeighMax6);
        __m512i tmp_input7 = _mm512_mask_mov_epi64(permNeighMin7, 0xAA, permNeighMax7);
        __m512i tmp_input8 = _mm512_mask_mov_epi64(permNeighMin8, 0xAA, permNeighMax8);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
        input2_val);
        input3_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input3_val), _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
        input3_val);
        input4_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input4_val), _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
        input4_val);
        input5_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input5_val), _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
        input5_val);
        input6_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input6_val), _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
        input6_val);
        input7_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input7_val), _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
        input7_val);
        input8_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input8_val), _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
        input8_val);

        input = tmp_input;
        input2 = tmp_input2;
        input3 = tmp_input3;
        input4 = tmp_input4;
        input5 = tmp_input5;
        input6 = tmp_input6;
        input7 = tmp_input7;
        input8 = tmp_input8;
    }
}


inline void CoreSmallSort9(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9,
                                 __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                 __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                 __m512i& input9_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                         input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort(input9, input9_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);


        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        input9 = tmp_input9;
        input8 = tmp_input8;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd1(input9, input9_val);
}


inline void CoreSmallSort9(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    CoreSmallSort9(input0,input1,input2,input3,input4,input5,input6,input7,input8,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
}


inline void CoreSmallSort10(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10,
                             __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                             __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                             __m512i& input9_val, __m512i& input10_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                         input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort2(input9, input10, input9_val, input10_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);


        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);


        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                           input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd2(input9, input10, input9_val, input10_val);
}


inline void CoreSmallSort10(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    CoreSmallSort10(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
}



inline void CoreSmallSort11(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10, __m512i& input11,
                                  __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                  __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                  __m512i& input9_val, __m512i& input10_val, __m512i& input11_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                    input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort3(input9, input10, input11,
                    input9_val, input10_val, input11_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);
        __m512i permNeigh11 = _mm512_permutexvar_epi64(idxNoNeigh, input11);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);

        __m512i tmp_input11 = _mm512_max_epu64(input6, permNeigh11);
        __m512i tmp_input6 = _mm512_min_epu64(input6, permNeigh11);


        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        __m512i input11_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input11_val);
        input11_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input11, permNeigh11, _MM_CMPINT_EQ ),
                                       input11_val_perm);
        input6_val = _mm512_mask_mov_epi64(input11_val_perm, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);


        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;

        input11 = tmp_input11;
        input6 = tmp_input6;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd3(input9, input10, input11,
                      input9_val, input10_val, input11_val);
}

inline void CoreSmallSort11(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input10 = _mm512_loadu_si512(ptr1+10*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    __m512i input10_val = _mm512_loadu_si512(values+10*8);
    CoreSmallSort11(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,input10,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val,input10_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(ptr1+10*8, input10);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
    _mm512_storeu_si512(values+10*8, input10_val);
}

inline void CoreSmallSort12(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10, __m512i& input11, __m512i& input12,
                                  __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                  __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                  __m512i& input9_val, __m512i& input10_val, __m512i& input11_val ,
                                  __m512i& input12_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                    input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort4(input9, input10, input11, input12,
                    input9_val, input10_val, input11_val, input12_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);
        __m512i permNeigh11 = _mm512_permutexvar_epi64(idxNoNeigh, input11);
        __m512i permNeigh12 = _mm512_permutexvar_epi64(idxNoNeigh, input12);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);

        __m512i tmp_input11 = _mm512_max_epu64(input6, permNeigh11);
        __m512i tmp_input6 = _mm512_min_epu64(input6, permNeigh11);

        __m512i tmp_input12 = _mm512_max_epu64(input5, permNeigh12);
        __m512i tmp_input5 = _mm512_min_epu64(input5, permNeigh12);

        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        __m512i input11_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input11_val);
        input11_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input11, permNeigh11, _MM_CMPINT_EQ ),
                                       input11_val_perm);
        input6_val = _mm512_mask_mov_epi64(input11_val_perm, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        __m512i input12_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input12_val);
        input12_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input12, permNeigh12, _MM_CMPINT_EQ ),
                                       input12_val_perm);
        input5_val = _mm512_mask_mov_epi64(input12_val_perm, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;

        input11 = tmp_input11;
        input6 = tmp_input6;

        input12 = tmp_input12;
        input5 = tmp_input5;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd4(input9, input10, input11, input12,
                      input9_val, input10_val, input11_val, input12_val);
}


inline void CoreSmallSort12(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input10 = _mm512_loadu_si512(ptr1+10*8);
    __m512i input11 = _mm512_loadu_si512(ptr1+11*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    __m512i input10_val = _mm512_loadu_si512(values+10*8);
    __m512i input11_val = _mm512_loadu_si512(values+11*8);
    CoreSmallSort12(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,input10,input11,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val,input10_val,input11_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(ptr1+10*8, input10);
    _mm512_storeu_si512(ptr1+11*8, input11);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
    _mm512_storeu_si512(values+10*8, input10_val);
    _mm512_storeu_si512(values+11*8, input11_val);
}



inline void CoreSmallSort13(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10, __m512i& input11, __m512i& input12,
                            __m512i& input13,
                                  __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                  __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                  __m512i& input9_val, __m512i& input10_val, __m512i& input11_val ,
                                  __m512i& input12_val, __m512i& input13_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                    input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort5(input9, input10, input11, input12, input13,
                    input9_val, input10_val, input11_val, input12_val, input13_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);
        __m512i permNeigh11 = _mm512_permutexvar_epi64(idxNoNeigh, input11);
        __m512i permNeigh12 = _mm512_permutexvar_epi64(idxNoNeigh, input12);
        __m512i permNeigh13 = _mm512_permutexvar_epi64(idxNoNeigh, input13);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);

        __m512i tmp_input11 = _mm512_max_epu64(input6, permNeigh11);
        __m512i tmp_input6 = _mm512_min_epu64(input6, permNeigh11);

        __m512i tmp_input12 = _mm512_max_epu64(input5, permNeigh12);
        __m512i tmp_input5 = _mm512_min_epu64(input5, permNeigh12);

        __m512i tmp_input13 = _mm512_max_epu64(input4, permNeigh13);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh13);

        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        __m512i input11_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input11_val);
        input11_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input11, permNeigh11, _MM_CMPINT_EQ ),
                                       input11_val_perm);
        input6_val = _mm512_mask_mov_epi64(input11_val_perm, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        __m512i input12_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input12_val);
        input12_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input12, permNeigh12, _MM_CMPINT_EQ ),
                                       input12_val_perm);
        input5_val = _mm512_mask_mov_epi64(input12_val_perm, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        __m512i input13_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input13_val);
        input13_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input13, permNeigh13, _MM_CMPINT_EQ ),
                                       input13_val_perm);
        input4_val = _mm512_mask_mov_epi64(input13_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;

        input11 = tmp_input11;
        input6 = tmp_input6;

        input12 = tmp_input12;
        input5 = tmp_input5;

        input13 = tmp_input13;
        input4 = tmp_input4;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd5(input9, input10, input11, input12, input13,
                      input9_val, input10_val, input11_val, input12_val, input13_val);
}


inline void CoreSmallSort13(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input10 = _mm512_loadu_si512(ptr1+10*8);
    __m512i input11 = _mm512_loadu_si512(ptr1+11*8);
    __m512i input12 = _mm512_loadu_si512(ptr1+12*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    __m512i input10_val = _mm512_loadu_si512(values+10*8);
    __m512i input11_val = _mm512_loadu_si512(values+11*8);
    __m512i input12_val = _mm512_loadu_si512(values+12*8);
    CoreSmallSort13(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,input10,input11,input12,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val,input10_val,input11_val,input12_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(ptr1+10*8, input10);
    _mm512_storeu_si512(ptr1+11*8, input11);
    _mm512_storeu_si512(ptr1+12*8, input12);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
    _mm512_storeu_si512(values+10*8, input10_val);
    _mm512_storeu_si512(values+11*8, input11_val);
    _mm512_storeu_si512(values+12*8, input12_val);
}



inline void CoreSmallSort14(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10, __m512i& input11, __m512i& input12,
                            __m512i& input13, __m512i& input14,
                                  __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                  __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                  __m512i& input9_val, __m512i& input10_val, __m512i& input11_val ,
                                  __m512i& input12_val, __m512i& input13_val, __m512i& input14_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                    input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort6(input9, input10, input11, input12, input13, input14,
                    input9_val, input10_val, input11_val, input12_val, input13_val, input14_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);
        __m512i permNeigh11 = _mm512_permutexvar_epi64(idxNoNeigh, input11);
        __m512i permNeigh12 = _mm512_permutexvar_epi64(idxNoNeigh, input12);
        __m512i permNeigh13 = _mm512_permutexvar_epi64(idxNoNeigh, input13);
        __m512i permNeigh14 = _mm512_permutexvar_epi64(idxNoNeigh, input14);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);

        __m512i tmp_input11 = _mm512_max_epu64(input6, permNeigh11);
        __m512i tmp_input6 = _mm512_min_epu64(input6, permNeigh11);

        __m512i tmp_input12 = _mm512_max_epu64(input5, permNeigh12);
        __m512i tmp_input5 = _mm512_min_epu64(input5, permNeigh12);

        __m512i tmp_input13 = _mm512_max_epu64(input4, permNeigh13);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh13);

        __m512i tmp_input14 = _mm512_max_epu64(input3, permNeigh14);
        __m512i tmp_input3 = _mm512_min_epu64(input3, permNeigh14);

        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        __m512i input11_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input11_val);
        input11_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input11, permNeigh11, _MM_CMPINT_EQ ),
                                       input11_val_perm);
        input6_val = _mm512_mask_mov_epi64(input11_val_perm, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        __m512i input12_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input12_val);
        input12_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input12, permNeigh12, _MM_CMPINT_EQ ),
                                       input12_val_perm);
        input5_val = _mm512_mask_mov_epi64(input12_val_perm, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        __m512i input13_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input13_val);
        input13_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input13, permNeigh13, _MM_CMPINT_EQ ),
                                       input13_val_perm);
        input4_val = _mm512_mask_mov_epi64(input13_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        __m512i input14_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input14_val);
        input14_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input14, permNeigh14, _MM_CMPINT_EQ ),
                                       input14_val_perm);
        input3_val = _mm512_mask_mov_epi64(input14_val_perm, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;

        input11 = tmp_input11;
        input6 = tmp_input6;

        input12 = tmp_input12;
        input5 = tmp_input5;

        input13 = tmp_input13;
        input4 = tmp_input4;

        input14 = tmp_input14;
        input3 = tmp_input3;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd6(input9, input10, input11, input12, input13, input14,
                      input9_val, input10_val, input11_val, input12_val, input13_val, input14_val);
}


inline void CoreSmallSort14(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input10 = _mm512_loadu_si512(ptr1+10*8);
    __m512i input11 = _mm512_loadu_si512(ptr1+11*8);
    __m512i input12 = _mm512_loadu_si512(ptr1+12*8);
    __m512i input13 = _mm512_loadu_si512(ptr1+13*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    __m512i input10_val = _mm512_loadu_si512(values+10*8);
    __m512i input11_val = _mm512_loadu_si512(values+11*8);
    __m512i input12_val = _mm512_loadu_si512(values+12*8);
    __m512i input13_val = _mm512_loadu_si512(values+13*8);
    CoreSmallSort14(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,input10,input11,input12,input13,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val,input10_val,input11_val,input12_val,input13_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(ptr1+10*8, input10);
    _mm512_storeu_si512(ptr1+11*8, input11);
    _mm512_storeu_si512(ptr1+12*8, input12);
    _mm512_storeu_si512(ptr1+13*8, input13);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
    _mm512_storeu_si512(values+10*8, input10_val);
    _mm512_storeu_si512(values+11*8, input11_val);
    _mm512_storeu_si512(values+12*8, input12_val);
    _mm512_storeu_si512(values+13*8, input13_val);
}


inline void CoreSmallSort15(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10, __m512i& input11, __m512i& input12,
                            __m512i& input13, __m512i& input14, __m512i& input15,
                                  __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                  __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                  __m512i& input9_val, __m512i& input10_val, __m512i& input11_val ,
                                  __m512i& input12_val, __m512i& input13_val, __m512i& input14_val,
                                  __m512i& input15_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                    input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort7(input9, input10, input11, input12, input13, input14, input15,
                    input9_val, input10_val, input11_val, input12_val, input13_val, input14_val, input15_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);
        __m512i permNeigh11 = _mm512_permutexvar_epi64(idxNoNeigh, input11);
        __m512i permNeigh12 = _mm512_permutexvar_epi64(idxNoNeigh, input12);
        __m512i permNeigh13 = _mm512_permutexvar_epi64(idxNoNeigh, input13);
        __m512i permNeigh14 = _mm512_permutexvar_epi64(idxNoNeigh, input14);
        __m512i permNeigh15 = _mm512_permutexvar_epi64(idxNoNeigh, input15);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);

        __m512i tmp_input11 = _mm512_max_epu64(input6, permNeigh11);
        __m512i tmp_input6 = _mm512_min_epu64(input6, permNeigh11);

        __m512i tmp_input12 = _mm512_max_epu64(input5, permNeigh12);
        __m512i tmp_input5 = _mm512_min_epu64(input5, permNeigh12);

        __m512i tmp_input13 = _mm512_max_epu64(input4, permNeigh13);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh13);

        __m512i tmp_input14 = _mm512_max_epu64(input3, permNeigh14);
        __m512i tmp_input3 = _mm512_min_epu64(input3, permNeigh14);

        __m512i tmp_input15 = _mm512_max_epu64(input2, permNeigh15);
        __m512i tmp_input2 = _mm512_min_epu64(input2, permNeigh15);

        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        __m512i input11_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input11_val);
        input11_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input11, permNeigh11, _MM_CMPINT_EQ ),
                                       input11_val_perm);
        input6_val = _mm512_mask_mov_epi64(input11_val_perm, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        __m512i input12_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input12_val);
        input12_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input12, permNeigh12, _MM_CMPINT_EQ ),
                                       input12_val_perm);
        input5_val = _mm512_mask_mov_epi64(input12_val_perm, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        __m512i input13_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input13_val);
        input13_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input13, permNeigh13, _MM_CMPINT_EQ ),
                                       input13_val_perm);
        input4_val = _mm512_mask_mov_epi64(input13_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        __m512i input14_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input14_val);
        input14_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input14, permNeigh14, _MM_CMPINT_EQ ),
                                       input14_val_perm);
        input3_val = _mm512_mask_mov_epi64(input14_val_perm, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        __m512i input15_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input15_val);
        input15_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input15, permNeigh15, _MM_CMPINT_EQ ),
                                       input15_val_perm);
        input2_val = _mm512_mask_mov_epi64(input15_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;

        input11 = tmp_input11;
        input6 = tmp_input6;

        input12 = tmp_input12;
        input5 = tmp_input5;

        input13 = tmp_input13;
        input4 = tmp_input4;

        input14 = tmp_input14;
        input3 = tmp_input3;

        input15 = tmp_input15;
        input2 = tmp_input2;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd7(input9, input10, input11, input12, input13, input14, input15,
                      input9_val, input10_val, input11_val, input12_val, input13_val, input14_val, input15_val);
}


inline void CoreSmallSort15(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input10 = _mm512_loadu_si512(ptr1+10*8);
    __m512i input11 = _mm512_loadu_si512(ptr1+11*8);
    __m512i input12 = _mm512_loadu_si512(ptr1+12*8);
    __m512i input13 = _mm512_loadu_si512(ptr1+13*8);
    __m512i input14 = _mm512_loadu_si512(ptr1+14*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    __m512i input10_val = _mm512_loadu_si512(values+10*8);
    __m512i input11_val = _mm512_loadu_si512(values+11*8);
    __m512i input12_val = _mm512_loadu_si512(values+12*8);
    __m512i input13_val = _mm512_loadu_si512(values+13*8);
    __m512i input14_val = _mm512_loadu_si512(values+14*8);
    CoreSmallSort15(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,input10,input11,input12,input13,input14,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val,input10_val,input11_val,input12_val,input13_val,input14_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(ptr1+10*8, input10);
    _mm512_storeu_si512(ptr1+11*8, input11);
    _mm512_storeu_si512(ptr1+12*8, input12);
    _mm512_storeu_si512(ptr1+13*8, input13);
    _mm512_storeu_si512(ptr1+14*8, input14);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
    _mm512_storeu_si512(values+10*8, input10_val);
    _mm512_storeu_si512(values+11*8, input11_val);
    _mm512_storeu_si512(values+12*8, input12_val);
    _mm512_storeu_si512(values+13*8, input13_val);
    _mm512_storeu_si512(values+14*8, input14_val);
}



inline void CoreSmallSort16(__m512i& input, __m512i& input2, __m512i& input3, __m512i& input4,
                            __m512i& input5, __m512i& input6, __m512i& input7, __m512i& input8,
                            __m512i& input9, __m512i& input10, __m512i& input11, __m512i& input12,
                            __m512i& input13, __m512i& input14, __m512i& input15, __m512i& input16,
                                  __m512i& input_val, __m512i& input2_val, __m512i& input3_val, __m512i& input4_val,
                                  __m512i& input5_val, __m512i& input6_val, __m512i& input7_val, __m512i& input8_val,
                                  __m512i& input9_val, __m512i& input10_val, __m512i& input11_val ,
                                  __m512i& input12_val, __m512i& input13_val, __m512i& input14_val,
                                  __m512i& input15_val,__m512i& input16_val){
    CoreSmallSort8(input, input2, input3, input4, input5, input6, input7, input8,
                    input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallSort8(input9, input10, input11, input12, input13, input14, input15, input16,
                    input9_val, input10_val, input11_val, input12_val, input13_val, input14_val, input15_val, input16_val);
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh9 = _mm512_permutexvar_epi64(idxNoNeigh, input9);
        __m512i permNeigh10 = _mm512_permutexvar_epi64(idxNoNeigh, input10);
        __m512i permNeigh11 = _mm512_permutexvar_epi64(idxNoNeigh, input11);
        __m512i permNeigh12 = _mm512_permutexvar_epi64(idxNoNeigh, input12);
        __m512i permNeigh13 = _mm512_permutexvar_epi64(idxNoNeigh, input13);
        __m512i permNeigh14 = _mm512_permutexvar_epi64(idxNoNeigh, input14);
        __m512i permNeigh15 = _mm512_permutexvar_epi64(idxNoNeigh, input15);
        __m512i permNeigh16 = _mm512_permutexvar_epi64(idxNoNeigh, input16);

        __m512i tmp_input9 = _mm512_max_epu64(input8, permNeigh9);
        __m512i tmp_input8 = _mm512_min_epu64(input8, permNeigh9);

        __m512i tmp_input10 = _mm512_max_epu64(input7, permNeigh10);
        __m512i tmp_input7 = _mm512_min_epu64(input7, permNeigh10);

        __m512i tmp_input11 = _mm512_max_epu64(input6, permNeigh11);
        __m512i tmp_input6 = _mm512_min_epu64(input6, permNeigh11);

        __m512i tmp_input12 = _mm512_max_epu64(input5, permNeigh12);
        __m512i tmp_input5 = _mm512_min_epu64(input5, permNeigh12);

        __m512i tmp_input13 = _mm512_max_epu64(input4, permNeigh13);
        __m512i tmp_input4 = _mm512_min_epu64(input4, permNeigh13);

        __m512i tmp_input14 = _mm512_max_epu64(input3, permNeigh14);
        __m512i tmp_input3 = _mm512_min_epu64(input3, permNeigh14);

        __m512i tmp_input15 = _mm512_max_epu64(input2, permNeigh15);
        __m512i tmp_input2 = _mm512_min_epu64(input2, permNeigh15);

        __m512i tmp_input16 = _mm512_max_epu64(input, permNeigh16);
        __m512i tmp_input = _mm512_min_epu64(input, permNeigh16);


        __m512i input9_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input9_val);
        input9_val = _mm512_mask_mov_epi64(input8_val, _mm512_cmp_epu64_mask(tmp_input9, permNeigh9, _MM_CMPINT_EQ ),
                                       input9_val_perm);
        input8_val = _mm512_mask_mov_epi64(input9_val_perm, _mm512_cmp_epu64_mask(tmp_input8, input8, _MM_CMPINT_EQ ),
                                       input8_val);

        __m512i input10_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input10_val);
        input10_val = _mm512_mask_mov_epi64(input7_val, _mm512_cmp_epu64_mask(tmp_input10, permNeigh10, _MM_CMPINT_EQ ),
                                       input10_val_perm);
        input7_val = _mm512_mask_mov_epi64(input10_val_perm, _mm512_cmp_epu64_mask(tmp_input7, input7, _MM_CMPINT_EQ ),
                                       input7_val);

        __m512i input11_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input11_val);
        input11_val = _mm512_mask_mov_epi64(input6_val, _mm512_cmp_epu64_mask(tmp_input11, permNeigh11, _MM_CMPINT_EQ ),
                                       input11_val_perm);
        input6_val = _mm512_mask_mov_epi64(input11_val_perm, _mm512_cmp_epu64_mask(tmp_input6, input6, _MM_CMPINT_EQ ),
                                       input6_val);

        __m512i input12_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input12_val);
        input12_val = _mm512_mask_mov_epi64(input5_val, _mm512_cmp_epu64_mask(tmp_input12, permNeigh12, _MM_CMPINT_EQ ),
                                       input12_val_perm);
        input5_val = _mm512_mask_mov_epi64(input12_val_perm, _mm512_cmp_epu64_mask(tmp_input5, input5, _MM_CMPINT_EQ ),
                                       input5_val);

        __m512i input13_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input13_val);
        input13_val = _mm512_mask_mov_epi64(input4_val, _mm512_cmp_epu64_mask(tmp_input13, permNeigh13, _MM_CMPINT_EQ ),
                                       input13_val_perm);
        input4_val = _mm512_mask_mov_epi64(input13_val_perm, _mm512_cmp_epu64_mask(tmp_input4, input4, _MM_CMPINT_EQ ),
                                       input4_val);

        __m512i input14_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input14_val);
        input14_val = _mm512_mask_mov_epi64(input3_val, _mm512_cmp_epu64_mask(tmp_input14, permNeigh14, _MM_CMPINT_EQ ),
                                       input14_val_perm);
        input3_val = _mm512_mask_mov_epi64(input14_val_perm, _mm512_cmp_epu64_mask(tmp_input3, input3, _MM_CMPINT_EQ ),
                                       input3_val);

        __m512i input15_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input15_val);
        input15_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input15, permNeigh15, _MM_CMPINT_EQ ),
                                       input15_val_perm);
        input2_val = _mm512_mask_mov_epi64(input15_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);

        __m512i input16_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input16_val);
        input16_val = _mm512_mask_mov_epi64(input_val, _mm512_cmp_epu64_mask(tmp_input16, permNeigh16, _MM_CMPINT_EQ ),
                                       input16_val_perm);
        input_val = _mm512_mask_mov_epi64(input16_val_perm, _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                       input_val);

        input9 = tmp_input9;
        input8 = tmp_input8;

        input10 = tmp_input10;
        input7 = tmp_input7;

        input11 = tmp_input11;
        input6 = tmp_input6;

        input12 = tmp_input12;
        input5 = tmp_input5;

        input13 = tmp_input13;
        input4 = tmp_input4;

        input14 = tmp_input14;
        input3 = tmp_input3;

        input15 = tmp_input15;
        input2 = tmp_input2;

        input16 = tmp_input16;
        input = tmp_input;
    }
    CoreSmallEnd8(input, input2, input3, input4, input5, input6, input7, input8,
                      input_val, input2_val, input3_val, input4_val, input5_val, input6_val, input7_val, input8_val);
    CoreSmallEnd8(input9, input10, input11, input12, input13, input14, input15, input16,
                      input9_val, input10_val, input11_val, input12_val, input13_val, input14_val, input15_val, input16_val);
}


inline void CoreSmallSort16(uint64_t* __restrict__ ptr1, uint64_t* __restrict__ values ){
    __m512i input0 = _mm512_loadu_si512(ptr1+0*8);
    __m512i input1 = _mm512_loadu_si512(ptr1+1*8);
    __m512i input2 = _mm512_loadu_si512(ptr1+2*8);
    __m512i input3 = _mm512_loadu_si512(ptr1+3*8);
    __m512i input4 = _mm512_loadu_si512(ptr1+4*8);
    __m512i input5 = _mm512_loadu_si512(ptr1+5*8);
    __m512i input6 = _mm512_loadu_si512(ptr1+6*8);
    __m512i input7 = _mm512_loadu_si512(ptr1+7*8);
    __m512i input8 = _mm512_loadu_si512(ptr1+8*8);
    __m512i input9 = _mm512_loadu_si512(ptr1+9*8);
    __m512i input10 = _mm512_loadu_si512(ptr1+10*8);
    __m512i input11 = _mm512_loadu_si512(ptr1+11*8);
    __m512i input12 = _mm512_loadu_si512(ptr1+12*8);
    __m512i input13 = _mm512_loadu_si512(ptr1+13*8);
    __m512i input14 = _mm512_loadu_si512(ptr1+14*8);
    __m512i input15 = _mm512_loadu_si512(ptr1+15*8);
    __m512i input0_val = _mm512_loadu_si512(values+0*8);
    __m512i input1_val = _mm512_loadu_si512(values+1*8);
    __m512i input2_val = _mm512_loadu_si512(values+2*8);
    __m512i input3_val = _mm512_loadu_si512(values+3*8);
    __m512i input4_val = _mm512_loadu_si512(values+4*8);
    __m512i input5_val = _mm512_loadu_si512(values+5*8);
    __m512i input6_val = _mm512_loadu_si512(values+6*8);
    __m512i input7_val = _mm512_loadu_si512(values+7*8);
    __m512i input8_val = _mm512_loadu_si512(values+8*8);
    __m512i input9_val = _mm512_loadu_si512(values+9*8);
    __m512i input10_val = _mm512_loadu_si512(values+10*8);
    __m512i input11_val = _mm512_loadu_si512(values+11*8);
    __m512i input12_val = _mm512_loadu_si512(values+12*8);
    __m512i input13_val = _mm512_loadu_si512(values+13*8);
    __m512i input14_val = _mm512_loadu_si512(values+14*8);
    __m512i input15_val = _mm512_loadu_si512(values+15*8);
    CoreSmallSort16(input0,input1,input2,input3,input4,input5,input6,input7,input8,input9,input10,input11,input12,input13,input14,input15,
        input0_val,input1_val,input2_val,input3_val,input4_val,input5_val,input6_val,input7_val,input8_val,input9_val,input10_val,input11_val,input12_val,input13_val,input14_val,input15_val);
    _mm512_storeu_si512(ptr1+0*8, input0);
    _mm512_storeu_si512(ptr1+1*8, input1);
    _mm512_storeu_si512(ptr1+2*8, input2);
    _mm512_storeu_si512(ptr1+3*8, input3);
    _mm512_storeu_si512(ptr1+4*8, input4);
    _mm512_storeu_si512(ptr1+5*8, input5);
    _mm512_storeu_si512(ptr1+6*8, input6);
    _mm512_storeu_si512(ptr1+7*8, input7);
    _mm512_storeu_si512(ptr1+8*8, input8);
    _mm512_storeu_si512(ptr1+9*8, input9);
    _mm512_storeu_si512(ptr1+10*8, input10);
    _mm512_storeu_si512(ptr1+11*8, input11);
    _mm512_storeu_si512(ptr1+12*8, input12);
    _mm512_storeu_si512(ptr1+13*8, input13);
    _mm512_storeu_si512(ptr1+14*8, input14);
    _mm512_storeu_si512(ptr1+15*8, input15);
    _mm512_storeu_si512(values+0*8, input0_val);
    _mm512_storeu_si512(values+1*8, input1_val);
    _mm512_storeu_si512(values+2*8, input2_val);
    _mm512_storeu_si512(values+3*8, input3_val);
    _mm512_storeu_si512(values+4*8, input4_val);
    _mm512_storeu_si512(values+5*8, input5_val);
    _mm512_storeu_si512(values+6*8, input6_val);
    _mm512_storeu_si512(values+7*8, input7_val);
    _mm512_storeu_si512(values+8*8, input8_val);
    _mm512_storeu_si512(values+9*8, input9_val);
    _mm512_storeu_si512(values+10*8, input10_val);
    _mm512_storeu_si512(values+11*8, input11_val);
    _mm512_storeu_si512(values+12*8, input12_val);
    _mm512_storeu_si512(values+13*8, input13_val);
    _mm512_storeu_si512(values+14*8, input14_val);
    _mm512_storeu_si512(values+15*8, input15_val);
}



inline void SmallSort16V(uint64_t* __restrict__ ptr, uint64_t* __restrict__ values, const size_t length){
    // length is limited to 4 times size of a vec
    const int nbValuesInVec = 8;
    const int nbVecs = (length+nbValuesInVec-1)/nbValuesInVec;
    const int rest = nbVecs*nbValuesInVec-length;
    const int lastVecSize = nbValuesInVec-rest;
    switch(nbVecs){
    case 1:
    {
        __m512i v1 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr),
                        _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v1_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values),
                        _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort(v1, v1_val);
        _mm512_mask_compressstoreu_epi64(ptr, 0xFF>>rest, v1);
        _mm512_mask_compressstoreu_epi64(values, 0xFF>>rest, v1_val);
    }
        break;
    case 2:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+8),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v2_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+8),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort2(v1,v2,
                             v1_val,v2_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_mask_compressstoreu_epi64(ptr+8, 0xFF>>rest, v2);
        _mm512_mask_compressstoreu_epi64(values+8, 0xFF>>rest, v2_val);
    }
        break;
    case 3:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+16),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v3_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+16),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort3(v1,v2,v3,
                             v1_val,v2_val,v3_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_mask_compressstoreu_epi64(ptr+16, 0xFF>>rest, v3);
        _mm512_mask_compressstoreu_epi64(values+16, 0xFF>>rest, v3_val);
    }
        break;
    case 4:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+24),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v4_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+24),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort4(v1,v2,v3,v4,
                             v1_val,v2_val,v3_val,v4_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_mask_compressstoreu_epi64(ptr+24, 0xFF>>rest, v4);
        _mm512_mask_compressstoreu_epi64(values+24, 0xFF>>rest, v4_val);
    }
        break;
    case 5:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+32),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v5_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+32),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort5(v1,v2,v3,v4,v5,
                             v1_val,v2_val,v3_val,v4_val,v5_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_mask_compressstoreu_epi64(ptr+32, 0xFF>>rest, v5);
        _mm512_mask_compressstoreu_epi64(values+32, 0xFF>>rest, v5_val);
    }
        break;
    case 6:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+40),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v6_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+40),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort6(v1,v2,v3,v4,v5,v6,
                             v1_val,v2_val,v3_val,v4_val,v5_val,v6_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_mask_compressstoreu_epi64(ptr+40, 0xFF>>rest, v6);
        _mm512_mask_compressstoreu_epi64(values+40, 0xFF>>rest, v6_val);
    }
        break;
    case 7:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+48),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v7_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+48),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort7(v1,v2,v3,v4,v5,v6,v7,
                             v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_mask_compressstoreu_epi64(ptr+48, 0xFF>>rest, v7);
        _mm512_mask_compressstoreu_epi64(values+48, 0xFF>>rest, v7_val);
    }
        break;
    case 8:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+56),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v8_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+56),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort8(v1,v2,v3,v4,v5,v6,v7,v8,
                             v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_mask_compressstoreu_epi64(ptr+56, 0xFF>>rest, v8);
        _mm512_mask_compressstoreu_epi64(values+56, 0xFF>>rest, v8_val);
    }
        break;
    case 9:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+64),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v9_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+64),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort9(v1,v2,v3,v4,v5,v6,v7,v8,v9,
                             v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_mask_compressstoreu_epi64(ptr+64, 0xFF>>rest, v9);
        _mm512_mask_compressstoreu_epi64(values+64, 0xFF>>rest, v9_val);
    }
        break;
    case 10:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+72),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v10_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+72),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort10(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_mask_compressstoreu_epi64(ptr+72, 0xFF>>rest, v10);
        _mm512_mask_compressstoreu_epi64(values+72, 0xFF>>rest, v10_val);
    }
        break;
    case 11:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_loadu_si512(ptr+72);
        __m512i v10_val = _mm512_loadu_si512(values+72);
        __m512i v11 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+80),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v11_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+80),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort11(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val,v11_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_storeu_si512(ptr+72, v10);
        _mm512_storeu_si512(values+72, v10_val);
        _mm512_mask_compressstoreu_epi64(ptr+80, 0xFF>>rest, v11);
        _mm512_mask_compressstoreu_epi64(values+80, 0xFF>>rest, v11_val);
    }
        break;
    case 12:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_loadu_si512(ptr+72);
        __m512i v10_val = _mm512_loadu_si512(values+72);
        __m512i v11 = _mm512_loadu_si512(ptr+80);
        __m512i v11_val = _mm512_loadu_si512(values+80);
        __m512i v12 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+88),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v12_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+88),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort12(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val,v11_val,v12_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_storeu_si512(ptr+72, v10);
        _mm512_storeu_si512(values+72, v10_val);
        _mm512_storeu_si512(ptr+80, v11);
        _mm512_storeu_si512(values+80, v11_val);
        _mm512_mask_compressstoreu_epi64(ptr+88, 0xFF>>rest, v12);
        _mm512_mask_compressstoreu_epi64(values+88, 0xFF>>rest, v12_val);
    }
        break;
    case 13:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_loadu_si512(ptr+72);
        __m512i v10_val = _mm512_loadu_si512(values+72);
        __m512i v11 = _mm512_loadu_si512(ptr+80);
        __m512i v11_val = _mm512_loadu_si512(values+80);
        __m512i v12 = _mm512_loadu_si512(ptr+88);
        __m512i v12_val = _mm512_loadu_si512(values+88);
        __m512i v13 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+96),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v13_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+96),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort13(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val,v11_val,v12_val,v13_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_storeu_si512(ptr+72, v10);
        _mm512_storeu_si512(values+72, v10_val);
        _mm512_storeu_si512(ptr+80, v11);
        _mm512_storeu_si512(values+80, v11_val);
        _mm512_storeu_si512(ptr+88, v12);
        _mm512_storeu_si512(values+88, v12_val);
        _mm512_mask_compressstoreu_epi64(ptr+96, 0xFF>>rest, v13);
        _mm512_mask_compressstoreu_epi64(values+96, 0xFF>>rest, v13_val);
    }
        break;
    case 14:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_loadu_si512(ptr+72);
        __m512i v10_val = _mm512_loadu_si512(values+72);
        __m512i v11 = _mm512_loadu_si512(ptr+80);
        __m512i v11_val = _mm512_loadu_si512(values+80);
        __m512i v12 = _mm512_loadu_si512(ptr+88);
        __m512i v12_val = _mm512_loadu_si512(values+88);
        __m512i v13 = _mm512_loadu_si512(ptr+96);
        __m512i v13_val = _mm512_loadu_si512(values+96);
        __m512i v14 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+104),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v14_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+104),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort14(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val,v11_val,v12_val,v13_val,v14_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_storeu_si512(ptr+72, v10);
        _mm512_storeu_si512(values+72, v10_val);
        _mm512_storeu_si512(ptr+80, v11);
        _mm512_storeu_si512(values+80, v11_val);
        _mm512_storeu_si512(ptr+88, v12);
        _mm512_storeu_si512(values+88, v12_val);
        _mm512_storeu_si512(ptr+96, v13);
        _mm512_storeu_si512(values+96, v13_val);
        _mm512_mask_compressstoreu_epi64(ptr+104, 0xFF>>rest, v14);
        _mm512_mask_compressstoreu_epi64(values+104, 0xFF>>rest, v14_val);
    }
        break;
    case 15:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_loadu_si512(ptr+72);
        __m512i v10_val = _mm512_loadu_si512(values+72);
        __m512i v11 = _mm512_loadu_si512(ptr+80);
        __m512i v11_val = _mm512_loadu_si512(values+80);
        __m512i v12 = _mm512_loadu_si512(ptr+88);
        __m512i v12_val = _mm512_loadu_si512(values+88);
        __m512i v13 = _mm512_loadu_si512(ptr+96);
        __m512i v13_val = _mm512_loadu_si512(values+96);
        __m512i v14 = _mm512_loadu_si512(ptr+104);
        __m512i v14_val = _mm512_loadu_si512(values+104);
        __m512i v15 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+112),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v15_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+112),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort15(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val,v11_val,v12_val,v13_val,v14_val,v15_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_storeu_si512(ptr+72, v10);
        _mm512_storeu_si512(values+72, v10_val);
        _mm512_storeu_si512(ptr+80, v11);
        _mm512_storeu_si512(values+80, v11_val);
        _mm512_storeu_si512(ptr+88, v12);
        _mm512_storeu_si512(values+88, v12_val);
        _mm512_storeu_si512(ptr+96, v13);
        _mm512_storeu_si512(values+96, v13_val);
        _mm512_storeu_si512(ptr+104, v14);
        _mm512_storeu_si512(values+104, v14_val);
        _mm512_mask_compressstoreu_epi64(ptr+112, 0xFF>>rest, v15);
        _mm512_mask_compressstoreu_epi64(values+112, 0xFF>>rest, v15_val);
    }
        break;
    //case 16:
    default:
    {
        __m512i v1 = _mm512_loadu_si512(ptr);
        __m512i v1_val = _mm512_loadu_si512(values);
        __m512i v2 = _mm512_loadu_si512(ptr+8);
        __m512i v2_val = _mm512_loadu_si512(values+8);
        __m512i v3 = _mm512_loadu_si512(ptr+16);
        __m512i v3_val = _mm512_loadu_si512(values+16);
        __m512i v4 = _mm512_loadu_si512(ptr+24);
        __m512i v4_val = _mm512_loadu_si512(values+24);
        __m512i v5 = _mm512_loadu_si512(ptr+32);
        __m512i v5_val = _mm512_loadu_si512(values+32);
        __m512i v6 = _mm512_loadu_si512(ptr+40);
        __m512i v6_val = _mm512_loadu_si512(values+40);
        __m512i v7 = _mm512_loadu_si512(ptr+48);
        __m512i v7_val = _mm512_loadu_si512(values+48);
        __m512i v8 = _mm512_loadu_si512(ptr+56);
        __m512i v8_val = _mm512_loadu_si512(values+56);
        __m512i v9 = _mm512_loadu_si512(ptr+64);
        __m512i v9_val = _mm512_loadu_si512(values+64);
        __m512i v10 = _mm512_loadu_si512(ptr+72);
        __m512i v10_val = _mm512_loadu_si512(values+72);
        __m512i v11 = _mm512_loadu_si512(ptr+80);
        __m512i v11_val = _mm512_loadu_si512(values+80);
        __m512i v12 = _mm512_loadu_si512(ptr+88);
        __m512i v12_val = _mm512_loadu_si512(values+88);
        __m512i v13 = _mm512_loadu_si512(ptr+96);
        __m512i v13_val = _mm512_loadu_si512(values+96);
        __m512i v14 = _mm512_loadu_si512(ptr+104);
        __m512i v14_val = _mm512_loadu_si512(values+104);
        __m512i v15 = _mm512_loadu_si512(ptr+112);
        __m512i v15_val = _mm512_loadu_si512(values+112);
        __m512i v16 = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr+120),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        __m512i v16_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, values+120),
                                     _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
        CoreSmallSort16(v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,v11,v12,v13,v14,v15,v16,
                              v1_val,v2_val,v3_val,v4_val,v5_val,v6_val,v7_val,v8_val,v9_val,v10_val,v11_val,v12_val,v13_val,v14_val,v15_val,v16_val);
        _mm512_storeu_si512(ptr, v1);
        _mm512_storeu_si512(values, v1_val);
        _mm512_storeu_si512(ptr+8, v2);
        _mm512_storeu_si512(values+8, v2_val);
        _mm512_storeu_si512(ptr+16, v3);
        _mm512_storeu_si512(values+16, v3_val);
        _mm512_storeu_si512(ptr+24, v4);
        _mm512_storeu_si512(values+24, v4_val);
        _mm512_storeu_si512(ptr+32, v5);
        _mm512_storeu_si512(values+32, v5_val);
        _mm512_storeu_si512(ptr+40, v6);
        _mm512_storeu_si512(values+40, v6_val);
        _mm512_storeu_si512(ptr+48, v7);
        _mm512_storeu_si512(values+48, v7_val);
        _mm512_storeu_si512(ptr+56, v8);
        _mm512_storeu_si512(values+56, v8_val);
        _mm512_storeu_si512(ptr+64, v9);
        _mm512_storeu_si512(values+64, v9_val);
        _mm512_storeu_si512(ptr+72, v10);
        _mm512_storeu_si512(values+72, v10_val);
        _mm512_storeu_si512(ptr+80, v11);
        _mm512_storeu_si512(values+80, v11_val);
        _mm512_storeu_si512(ptr+88, v12);
        _mm512_storeu_si512(values+88, v12_val);
        _mm512_storeu_si512(ptr+96, v13);
        _mm512_storeu_si512(values+96, v13_val);
        _mm512_storeu_si512(ptr+104, v14);
        _mm512_storeu_si512(values+104, v14_val);
        _mm512_storeu_si512(ptr+112, v15);
        _mm512_storeu_si512(values+112, v15_val);
        _mm512_mask_compressstoreu_epi64(ptr+120, 0xFF>>rest, v16);
        _mm512_mask_compressstoreu_epi64(values+120, 0xFF>>rest, v16_val);
    }
    }
}

inline void Merge2V(__m512i& input, __m512i& input2, __m512i& input_val, 
        __m512i& input2_val){
    {
        __m512i idxNoNeigh = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i tmp_input = _mm512_min_epu64(input2, permNeigh);
        __m512i tmp_input2 = _mm512_max_epu64(input2, permNeigh);

        __m512i input_val_perm = _mm512_permutexvar_epi64(idxNoNeigh, input_val);
        input_val = _mm512_mask_mov_epi64(input2_val, _mm512_cmp_epu64_mask(tmp_input, permNeigh, _MM_CMPINT_EQ ),
                                       input_val_perm);
        input2_val = _mm512_mask_mov_epi64(input_val_perm, _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                       input2_val);
        input = tmp_input;
        input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xF0, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xF0, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                        input2_val);

         input = tmp_input;
         input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xCC, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xCC, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                        input2_val);

         input = tmp_input;
         input2 = tmp_input2;
    }
    {
        __m512i idxNoNeigh = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
        __m512i permNeigh = _mm512_permutexvar_epi64(idxNoNeigh, input);
        __m512i permNeigh2 = _mm512_permutexvar_epi64(idxNoNeigh, input2);
        __m512i permNeighMin = _mm512_min_epu64(permNeigh, input);
        __m512i permNeighMin2 = _mm512_min_epu64(permNeigh2, input2);
        __m512i permNeighMax = _mm512_max_epu64(permNeigh, input);
        __m512i permNeighMax2 = _mm512_max_epu64(permNeigh2, input2);
        __m512i tmp_input = _mm512_mask_mov_epi64(permNeighMin, 0xAA, permNeighMax);
        __m512i tmp_input2 = _mm512_mask_mov_epi64(permNeighMin2, 0xAA, permNeighMax2);

        input_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input_val), _mm512_cmp_epu64_mask(tmp_input, input, _MM_CMPINT_EQ ),
                                        input_val);
        input2_val = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idxNoNeigh, input2_val), _mm512_cmp_epu64_mask(tmp_input2, input2, _MM_CMPINT_EQ ),
                                        input2_val);

        input = tmp_input;
        input2 = tmp_input2;
    }
}

static inline void Merge(uint64_t* keys1, uint64_t* keys2, uint64_t* values1,
        uint64_t* values2, int64_t len1, int64_t len2, uint64_t* key_buf, 
        uint64_t* value_buf) {

    __m512i minv = _mm512_loadu_si512(keys1);
    __m512i minv_val = _mm512_loadu_si512(values1);
    __m512i maxv = _mm512_loadu_si512(keys2);
    __m512i maxv_val = _mm512_loadu_si512(values2);
    int64_t ids1 = 8, ids2 = 8, ids3 = 0;
    int32_t rest1 = ((len1 + 7) & 0xFFFFFFF8) - len1;
    int32_t rest2 = ((len2 + 7) & 0xFFFFFFF8) - len2;

    while (ids1 < len1 || ids2 < len2) {
        Merge2V(minv, maxv, minv_val, maxv_val);
        _mm512_storeu_si512(key_buf + ids3, minv);
        _mm512_storeu_si512(value_buf + ids3, minv_val);
        ids3 += 8;
        if (ids1 < len1) {
            if (ids2 < len2) {
                if (keys1[ids1] < keys2[ids2]) {
                    if (len1 - ids1 < 8) {
                        int32_t lastVecSize = 8 - rest1;
                        minv = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest1, keys1 + ids1),
                                _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                        minv_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest1, values1 + ids1),
                                _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                    } else {
                        minv = _mm512_loadu_si512(keys1 + ids1);
                        minv_val = _mm512_loadu_si512(values1 + ids1);
                    }
                    ids1 += 8;
                } else {
                    if (len2 - ids2 < 8) {
                        int32_t lastVecSize = 8 - rest2;
                        minv = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest2, keys2 + ids2),
                                _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                        minv_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest2, values2 + ids2),
                                _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                    } else {
                        minv = _mm512_loadu_si512(keys2 + ids2);
                        minv_val = _mm512_loadu_si512(values2 + ids2);
                    }
                    ids2 += 8;
                }
            } else {
                if (len1 - ids1 < 8) {
                    int32_t lastVecSize = 8 - rest1;
                    minv = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest1, keys1 + ids1),
                            _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                    minv_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest1, values1 + ids1),
                            _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                } else {
                    minv = _mm512_loadu_si512(keys1 + ids1);
                    minv_val = _mm512_loadu_si512(values1 + ids1);
                }
                ids1 += 8;
            }
        } else {
            if (len2 - ids2 < 8) {
                int32_t lastVecSize = 8 - rest2;
                minv = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest2, keys2 + ids2),
                        _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
                minv_val = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest2, values2 + ids2),
                        _mm512_maskz_set1_epi64(0xFF<<lastVecSize, ULLONG_MAX));
            } else {
                minv = _mm512_loadu_si512(keys2 + ids2);
                minv_val = _mm512_loadu_si512(values2 + ids2);
            }
            ids2 += 8;
        }
    }
    Merge2V(minv, maxv, minv_val, maxv_val);
    int32_t rest = rest1 + rest2;
    if (rest >= 8) {
        rest -= 8;
        _mm512_mask_compressstoreu_epi64(key_buf + ids3, 0xFF>>rest, minv);
        _mm512_mask_compressstoreu_epi64(value_buf + ids3, 0xFF>>rest, minv_val);
    } else {
       _mm512_storeu_si512(key_buf + ids3, minv);
       _mm512_storeu_si512(value_buf + ids3, minv_val);
       _mm512_mask_compressstoreu_epi64(key_buf + ids3 + 8, 0xFF>>rest, maxv);
       _mm512_mask_compressstoreu_epi64(value_buf + ids3 + 8, 0xFF>>rest, maxv_val);
    }

    memcpy(keys1, key_buf, len1 * sizeof(uint64_t));
    memcpy(keys2, key_buf + len1, len2 * sizeof(uint64_t));
    memcpy(values1, value_buf, len1 * sizeof(uint64_t));
    memcpy(values2, value_buf + len1, len2 * sizeof(uint64_t));
}

static inline void MergeSort(uint64_t* keys, uint64_t* values, int64_t len, uint64_t* key_buf, uint64_t* value_buf) {
    const int64_t nbValuesInVec = 8;
    const int64_t nbValuesInSegment = 16 * nbValuesInVec;

    if (len <= nbValuesInSegment) {
        SmallSort16V(keys, values, len);
    } else {
        const int64_t half = len / 2;
        MergeSort(keys, values, half, key_buf, value_buf);
        MergeSort(keys + half, values + half, len - half, key_buf, value_buf);
        Merge(keys, keys + half, values, values + half, half, len - half, key_buf, value_buf); 
    }
}

static inline int64_t CoreScalarPartition(uint64_t* array, uint64_t* values, int64_t left, int64_t right,
                                    const uint64_t pivot){

    for(; left <= right
         && array[left] <= pivot ; ++left){
    }

    for(int64_t idx = left ; idx <= right ; ++idx){
        if( array[idx] <= pivot ){
            std::swap(array[idx],array[left]);
            std::swap(values[idx],values[left]);
            left += 1;
        }
    }

    return left;
}


inline int popcount(__mmask16 mask){
    //    int res = int(mask);
    //    res = (0x5555 & res) + (0x5555 & (res >> 1));
    //    res = (res & 0x3333) + ((res>>2) & 0x3333);
    //    res = (res & 0x0F0F) + ((res>>4) & 0x0F0F);
    //    return (res & 0xFF) + ((res>>8) & 0xFF);
#ifdef __INTEL_COMPILER
    return _mm_countbits_32(mask);
#else
    return __builtin_popcount(mask);
#endif
}


static inline int64_t Partition512(uint64_t* array, uint64_t* values, int64_t left, int64_t right,
                                         const uint64_t pivot){
    const int64_t S = 8;//(512/8)/sizeof(uint64_t);

    if(right-left+1 < 2*S){
        return CoreScalarPartition(array, values, left, right, pivot);
    }

    __m512i pivotvec = _mm512_set1_epi64(pivot);

    __m512i left_val = _mm512_loadu_si512(&array[left]);
    __m512i left_val_val = _mm512_loadu_si512(&values[left]);
    int64_t left_w = left;
    left += S;

    int64_t right_w = right+1;
    right -= S-1;
    __m512i right_val = _mm512_loadu_si512(&array[right]);
    __m512i right_val_val = _mm512_loadu_si512(&values[right]);

    while(left + S <= right){
        const int64_t free_left = left - left_w;
        const int64_t free_right = right_w - right;

        __m512i val;
        __m512i val_val;
        if( free_left <= free_right ){
            val = _mm512_loadu_si512(&array[left]);
            val_val = _mm512_loadu_si512(&values[left]);
            left += S;
        }
        else{
            right -= S;
            val = _mm512_loadu_si512(&array[right]);
            val_val = _mm512_loadu_si512(&values[right]);
        }

        __mmask8 mask = _mm512_cmp_epu64_mask(val, pivotvec, _MM_CMPINT_LE);

        const int64_t nb_low = popcount(mask); // count mask
        const int64_t nb_high = S-nb_low; // S-nb_low

        _mm512_mask_compressstoreu_epi64(&array[left_w],mask,val);
        _mm512_mask_compressstoreu_epi64(&values[left_w],mask,val_val);
        left_w += nb_low;

        right_w -= nb_high;
        _mm512_mask_compressstoreu_epi64(&array[right_w],~mask,val);
        _mm512_mask_compressstoreu_epi64(&values[right_w],~mask,val_val);
    }

    {
        const int64_t remaining = right - left;
        __m512i val = _mm512_loadu_si512(&array[left]);
        __m512i val_val = _mm512_loadu_si512(&values[left]);
        left = right;

        __mmask8 mask = _mm512_cmp_epu64_mask(val, pivotvec, _MM_CMPINT_LE);

        __mmask8 mask_low = mask & ~(0xFF << remaining);
        __mmask8 mask_high = (~mask) & ~(0xFF << remaining);

        const int64_t nb_low = popcount(mask_low); // count mask
        const int64_t nb_high = popcount(mask_high); // S-nb_low

        _mm512_mask_compressstoreu_epi64(&array[left_w],mask_low,val);
        _mm512_mask_compressstoreu_epi64(&values[left_w],mask_low,val_val);
        left_w += nb_low;

        right_w -= nb_high;
        _mm512_mask_compressstoreu_epi64(&array[right_w],mask_high,val);
        _mm512_mask_compressstoreu_epi64(&values[right_w],mask_high,val_val);
    }
    {
        __mmask8 mask = _mm512_cmp_epu64_mask(left_val, pivotvec, _MM_CMPINT_LE);

        const int64_t nb_low = popcount(mask); // count mask
        const int64_t nb_high = S-nb_low; // S-nb_low

        _mm512_mask_compressstoreu_epi64(&array[left_w],mask,left_val);
        _mm512_mask_compressstoreu_epi64(&values[left_w],mask,left_val_val);
        left_w += nb_low;

        right_w -= nb_high;
        _mm512_mask_compressstoreu_epi64(&array[right_w],~mask,left_val);
        _mm512_mask_compressstoreu_epi64(&values[right_w],~mask,left_val_val);
    }
    {
        __mmask8 mask = _mm512_cmp_epu64_mask(right_val, pivotvec, _MM_CMPINT_LE);

        const int64_t nb_low = popcount(mask); // count mask
        const int64_t nb_high = S-nb_low; // S-nb_low

        _mm512_mask_compressstoreu_epi64(&array[left_w],mask,right_val);
        _mm512_mask_compressstoreu_epi64(&values[left_w],mask,right_val_val);
        left_w += nb_low;

        right_w -= nb_high;
        _mm512_mask_compressstoreu_epi64(&array[right_w],~mask,right_val);
        _mm512_mask_compressstoreu_epi64(&values[right_w],~mask,right_val_val);
    }
    return left_w;
}



////////////////////////////////////////////////////////////////////////////////
/// Main functions
////////////////////////////////////////////////////////////////////////////////
inline int64_t lg(int64_t n) {
    int64_t k;
    for (k = 0; n > 1; n >>= 1) ++k;
    return k;
}

static inline int64_t CoreSortGetPivot(const uint64_t* array, const int64_t left, const int64_t right)
{
    const int64_t middle = ((right-left)/2) + left;
    if(array[left] <= array[middle] && array[middle] <= array[right]){
        return middle;
    }
    else if(array[middle] <= array[left] && array[left] <= array[right]){
        return left;
    }
    else return right;
}

static inline int64_t CoreSortPivotPartition(uint64_t* array, uint64_t* values, const int64_t left, const int64_t right)
{
    if(right-left > 1){
        const int64_t pivotIdx = CoreSortGetPivot(array, left, right);
        std::swap(array[pivotIdx], array[right]);
        std::swap(values[pivotIdx], values[right]);
        const int64_t part = Partition512(array, values, left, right-1, array[right]);
        std::swap(array[part], array[right]);
        std::swap(values[part], values[right]);
        return part;
    }
    return left;
}

static inline void CoreSort(uint64_t* array, uint64_t* values, const int64_t left, 
        const int64_t right, int64_t depth_limit, uint64_t* key_buf, uint64_t* value_buf)
{
    static const int SortLimite = 16 * 64 / sizeof(uint64_t);
    if(right-left < SortLimite){
        SmallSort16V(array+left, values+left, right-left+1);
    } else {
        if (depth_limit == 0) {
            MergeSort(array + left, values + left, right - left + 1, key_buf, value_buf);
        } else {
            depth_limit--;
            const int64_t part = CoreSortPivotPartition(array, values, left, right);
            if(part + 1 < right) CoreSort(array, values, part + 1, right, depth_limit, key_buf, value_buf);
            if(part && left < part - 1)  CoreSort(array, values, left, part - 1, depth_limit, key_buf, value_buf);
        }
    }
}

template <typename T>
static int SortNextRound(uint64_t* keys, T** values, uint16_t round, 
        int64_t left, int64_t right, uint64_t* key_buf, uint64_t* value_buf)
{
    int ret = OB_SUCCESS;
    for (int64_t i = left; OB_SUCC(ret) && i < right; i++) {
        ret = values[i]->get_sort_key(&keys[i]);
    }
    if (OB_SUCC(ret)) {
        CoreSort(keys, (uint64_t*)values, left, right - 1, lg(right - left) * 2, key_buf, value_buf);
    }

    int64_t i = left;
    while (OB_SUCC(ret)) {
        while (i < right - 1 && keys[i] != keys[i + 1]) {
            i++;
        }
        int64_t next_left = i;
        i++;
        while (i < right - 1 && keys[i] == keys[i + 1]) {
            i++;
        }
        i++;
        while (next_left < i && next_left < right && values[next_left]->get_sort_key_end()) {
            next_left++;
        }
        if (next_left >= right - 1) {
            break;
        }
        ret = SortNextRound(keys, values, round + 1, next_left, i, key_buf, value_buf);
    }
    return ret;
}

template <typename T>
static int sort(uint64_t* keys, T** values, const int64_t size, uint64_t* key_buf, uint64_t* value_buf) {
    int ret = OB_SUCCESS;
    if (keys == NULL || values == NULL || key_buf == NULL || value_buf == NULL || size < 0) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      ret = SortNextRound<T>(keys, values, 0, 0, size, key_buf, value_buf);
    }
    return ret;
}

}  // end namespace storage
}  // end namespace oceanbase

#endif
