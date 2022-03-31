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
#include <climits>
#include "storage/ob_avx512_sort.h"
#include "share/ob_define.h"
#include "ob_i_store.h"

namespace oceanbase {
namespace storage {
/* 
 * This cpp file implements quick sorting and bitonic sort through 
 * vectorization. However, the sorting algorithm implemented by the vectorization
 * method can only sort numerical arrays.We use key-value to solve this 
 * problem. The key array is used for comparison and sorting, the value array 
 * holds pointers to sorted elements. When the elements of the key array are
 * exchanged, the elements of value are also exchanged.
 *
 * Main sort function as follows:
 *  1. bitonic_sort: vectorized bitonic, only numeric array with elements less 
 *  than 128 can be sorted.
 *  2. quick_sort: vectorized quick sort.
 *  3. do_sort: entry function.
 *
 * The flow of the `do_sort function` is as follows:
 *  1. use vector quick sort to sort the elements.
 *  2. However, quick sort may have uneven partitions. When the recursion
 *  reaches a certain depth, heap sort should be used.
 *  3. In the process of quick sorting, the interval keeps shrinking. When the 
 *  interval length is less than 128, the bitontic sorting realized by 
 *  vectorization is needed.
 *
 * */

/* Bitonic sort 
 *
 * Before reading the code, please learn the bitonic sorting.
 *
 * Main function:
 *  1. bitonic_sort_1v : sort the elements in 1 vector.
 *  2. bitonic_sort_2v : sort the elements in 2 vector.
 *  3. bitonic_sort_3v : sort the elements in 3 vector.
 *  4. bitonic_sort_4v : sort the elements in 4 vector.
 *  5. bitonic_sort_5v : sort the elements in 5 vector.
 *  6. bitonic_sort_6v : sort the elements in 6 vector.
 *  7. bitonic_sort_7v : sort the elements in 7 vector.
 *  8. bitonic_sort_8v : sort the elements in 8 vector.
 *  9. bitonic_sort_9v : sort the elements in 9 vector.
 *  10. bitonic_sort_10v : sort the elements in 10 vector.
 *  11. bitonic_sort_11v : sort the elements in 11 vector.
 *  12. bitonic_sort_12v : sort the elements in 12 vector.
 *  13. bitonic_sort_13v : sort the elements in 13 vector.
 *  14. bitonic_sort_14v : sort the elements in 14 vector.
 *  15. bitonic_sort_15v : sort the elements in 15 vector.
 *  16. bitonic_sort_16v : sort the elements in 16 vector.
 *  17. bitonic_sort: sort the elements in array by vectorized bitonic, but 
 *  only numeric array with elements less than 128 can be used.
 *
 * */

/* 
 * compare_and_exchange_1v : 
 * 1. Exchange elements in `keys` according to the index `idx` to get `perm_keys`.
 * 2. Compare the values of the corresponding elements of `keys` and 
 * `perm_keys` to get `min_keys` and `max_keys`.
 * 3. Get the required value from `min_keys` and `max_keys` according to the mask `m`. 
 *
 * example:
 * keys: 1 5 6 9 10 4 3 2
 * idx:  3 2 1 0 7 6 5 4
 * m: 0xF0
 *
 * 1. get `perm_keys: 10 4 3 2 1  5 6 9`
 * 2. get `min_keys:  1  4 3 2 1  4 3 2 
 *         max_keys:  10 5 6 9 10 5 6 9`
 * 3. use mask `m 0xF0` to get `tmp_keys: 1 4 3 2 10 5 6 9` 
 *
 * */
inline void compare_and_exchange_1v(__m512i& keys, __m512i& values, 
                                      __m512i& idx, __mmask8 m)
{
  __m512i perm_keys = _mm512_permutexvar_epi64(idx, keys);
  __m512i min_keys = _mm512_min_epu64(perm_keys, keys);
  __m512i max_keys = _mm512_max_epu64(perm_keys, keys);
  __m512i tmp_keys = _mm512_mask_mov_epi64(min_keys, m, max_keys);
  /* When exchanging the values of keys, also need to exchange values. */
  values = _mm512_mask_mov_epi64(_mm512_permutexvar_epi64(idx, values), 
                                    _mm512_cmp_epu64_mask(tmp_keys, keys, 
                                                            _MM_CMPINT_EQ),
                                    values);
  keys = tmp_keys;
}

/* 
 * compare_and_exchange_2v:
 *  1. Transpose the vector `keys2`.
 *  2. Compare the values of the corresponding elements of `keys1` and 
 * `perm_keys` to get `tmp_keys1` and `tmp_keys2`.
 *
 * example: 
 * keys1: 1 5 6 9 10 11 13 16
 * keys2: 2 3 4 7 8  12 14 15
 *
 *  1. get `perm_keys: 16 13 11 10 9 6  5  1` (now keys1 and perm_keys form a bitonic sequence)
 *  2. get `tmp_keys1: 2  3  4  7  8 6  5  1 (bitonic sequence)
 *          tmp_keys2: 16 13 11 10 9 12 14 15 (bitonic sequence)`
 *
 * */
inline void compare_and_exchange_2v(__m512i& keys1, __m512i& keys2, 
                                      __m512i& values1, __m512i& values2)
{
  __m512i idx = _mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7);
  __m512i perm_keys = _mm512_permutexvar_epi64(idx, keys2);
  __m512i tmp_keys1 = _mm512_min_epu64(keys1, perm_keys);
  __m512i tmp_keys2 = _mm512_max_epu64(keys1, perm_keys);
  __m512i perm_values = _mm512_permutexvar_epi64(idx, values2);
  values2 = _mm512_mask_mov_epi64(values1, 
                                    _mm512_cmp_epu64_mask(tmp_keys2, perm_keys, 
                                                            _MM_CMPINT_EQ),
                                    perm_values);
  values1 = _mm512_mask_mov_epi64(perm_values, 
                                    _mm512_cmp_epu64_mask(tmp_keys1, keys1, 
                                                            _MM_CMPINT_EQ),
                                   values1);
  keys1 = tmp_keys1;
  keys2 = tmp_keys2;
}

/*
 * compare_and_exchange_2v_without_perm:
 *  Compare the values of the corresponding elements of `keys1` and `keys2` to 
 *  get `tmp_keys1` and `tmp_keys2`.
 *
 * */
inline void compare_and_exchange_2v_without_perm(__m512i& keys1, __m512i& keys2, 
                                                   __m512i& values1,
                                                   __m512i& values2)
{
  __m512i tmp_keys1 = _mm512_min_epu64(keys2, keys1);
  __m512i tmp_keys2 = _mm512_max_epu64(keys2, keys1);
  __m512i copy_values1 = values1;
  values1 = _mm512_mask_mov_epi64(values2, 
                                    _mm512_cmp_epu64_mask(tmp_keys1, keys1,
                                                            _MM_CMPINT_EQ),
                                    copy_values1);
  values2 = _mm512_mask_mov_epi64(copy_values1, 
                                    _mm512_cmp_epu64_mask(tmp_keys2, keys2,
                                                            _MM_CMPINT_EQ),
                                    values2);
  keys1 = tmp_keys1;
  keys2 = tmp_keys2;
}

/*
 * sort_bitonic_sequence: sorting of a bitonal sequence in keys.
 *
 * example:
 * keys: 1 5 6 9 10 4 3 2 (bitonic sequence)
 *
 * */
inline void sort_bitonic_sequence(__m512i& keys, __m512i& values)
{
  // keys: 1  5 6 9 10 4 3 2
  //       10 4 3 2 1  5 6 9
  // min:  1  4 3 2 1  4 3 2 
  // max:  10 5 6 9 10 5 6 9
  __m512i idx = _mm512_set_epi64(3, 2, 1, 0, 7, 6, 5, 4);
  compare_and_exchange_1v(keys, values, idx, 0xF0);
  // keys: 1 4 3 2 10 5 6  9
  //       3 2 1 4 6  9 10 5
  // min:  1 2 1 2 6  5 6  5
  // max:  3 4 3 4 10 9 10 9
  idx = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
  compare_and_exchange_1v(keys, values, idx, 0xCC);
  // keys: 1 2 3 4 6 5 10 9
  //       2 1 4 3 5 6 9  10
  // min:  1 1 3 3 5 5 9  9
  // max:  2 2 4 4 6 6 10 10
  idx = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
  compare_and_exchange_1v(keys, values, idx, 0xAA);
  // keys: 1 2 3 4 5 6 9 10 
}

/*
 * sort_bitonic_sequence: sorting of a bitonal sequence in keys1 and keys2
 *
 * example:
 *  keys1: 1 5 6 9 10 11 13 16
 *  keys2: 15 14 12 8 7 4 3 2 
 *  (keys1 and keys2 form a bitonic sequence)
 *
 * */
inline void sort_bitonic_sequence_2v(__m512i& keys1, __m512i& keys2,
                                       __m512i& values1, __m512i& values2)
{
  compare_and_exchange_2v_without_perm(keys1, keys2, values1, values2);
  // keys1: 1  5  6  8 7  4  3   2 (bitonic sequence)
  // keys2: 15 14 12 9 10 11 13 16 (bitonic sequence)
  sort_bitonic_sequence(keys1, values1);
  sort_bitonic_sequence(keys2, values2);
}

inline void sort_bitonic_sequence_3v(__m512i& keys1, __m512i& keys2, 
                                       __m512i& keys3, __m512i& values1, 
                                       __m512i& values2, __m512i& values3)
{
  compare_and_exchange_2v_without_perm(keys1, keys3, values1, values3);
  sort_bitonic_sequence_2v(keys1, keys2, values1, values2);
  sort_bitonic_sequence(keys3, values3);
}

inline void sort_bitonic_sequence_4v(__m512i& keys1, __m512i& keys2, 
                                     __m512i& keys3, __m512i& keys4,
                                     __m512i& values1, __m512i& values2, 
                                     __m512i& values3, __m512i& values4)
{
  compare_and_exchange_2v_without_perm(keys1, keys3, values1, values3);
  compare_and_exchange_2v_without_perm(keys2, keys4, values2, values4);
  sort_bitonic_sequence_2v(keys1, keys2, values1, values2);
  sort_bitonic_sequence_2v(keys3, keys4, values3, values4);
}

inline void sort_bitonic_sequence_5v(__m512i& keys1, __m512i& keys2, 
                                       __m512i& keys3, __m512i& keys4,
                                       __m512i& keys5, __m512i& values1, 
                                       __m512i& values2, __m512i& values3, 
                                       __m512i& values4, __m512i& values5)
{
  compare_and_exchange_2v_without_perm(keys1, keys5, values1, values5);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4, 
                             values1, values2, values3, values4);
  sort_bitonic_sequence(keys5, values5);
}

inline void sort_bitonic_sequence_6v(__m512i& keys1, __m512i& keys2, 
                                       __m512i& keys3, __m512i& keys4,
                                       __m512i& keys5, __m512i& keys6,
                                       __m512i& values1, __m512i& values2,
                                       __m512i& values3, __m512i& values4,
                                       __m512i& values5, __m512i& values6)
{
  compare_and_exchange_2v_without_perm(keys1, keys5, values1, values5);
  compare_and_exchange_2v_without_perm(keys2, keys6, values2, values6);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4, 
                             values1, values2, values3, values4);
  sort_bitonic_sequence_2v(keys5, keys6, values5, values6);
}

inline void sort_bitonic_sequence_7v(__m512i& keys1, __m512i& keys2, 
                                       __m512i& keys3, __m512i& keys4,
                                       __m512i& keys5, __m512i& keys6, 
                                       __m512i& keys7, __m512i& values1,
                                       __m512i& values2, __m512i& values3, 
                                       __m512i& values4, __m512i& values5, 
                                       __m512i& values6, __m512i& values7)
{
  compare_and_exchange_2v_without_perm(keys1, keys5, values1, values5);
  compare_and_exchange_2v_without_perm(keys2, keys6, values2, values6);
  compare_and_exchange_2v_without_perm(keys3, keys7, values3, values7);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4, 
                             values1, values2, values3, values4);
  sort_bitonic_sequence_3v(keys5, keys6, keys7, values5, values6, values7);
}

inline void sort_bitonic_sequence_8v(__m512i& keys1, __m512i& keys2, 
                                       __m512i& keys3, __m512i& keys4,
                                       __m512i& keys5, __m512i& keys6, 
                                       __m512i& keys7, __m512i& keys8,
                                       __m512i& values1, __m512i& values2,
                                       __m512i& values3, __m512i& values4,
                                       __m512i& values5, __m512i& values6, 
                                       __m512i& values7, __m512i& values8)
{
  compare_and_exchange_2v_without_perm(keys1, keys5, values1, values5);
  compare_and_exchange_2v_without_perm(keys2, keys6, values2, values6);
  compare_and_exchange_2v_without_perm(keys3, keys7, values3, values7);
  compare_and_exchange_2v_without_perm(keys4, keys8, values4, values8);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4, 
                             values1, values2, values3, values4);
  sort_bitonic_sequence_4v(keys5, keys6, keys7, keys8, 
                             values5, values6, values7, values8);
}

/* 
 * bitonic_sort_1v: sort the elements in the vector keys using bitonal sorting.
 *
 * */
inline void bitonic_sort_1v(__m512i& keys, __m512i& values)
{
  // step1: the process of forming a bitonal sequence
  __m512i idx = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
  compare_and_exchange_1v(keys, values, idx, 0x66);
  idx = _mm512_set_epi64(5, 4, 7, 6, 1, 0, 3, 2);
  compare_and_exchange_1v(keys, values, idx, 0x3C);
  idx = _mm512_set_epi64(6, 7, 4, 5, 2, 3, 0, 1);
  compare_and_exchange_1v(keys, values, idx, 0x5A);
  // step2: invoke the bitonic sort on a bitonic sequence
  sort_bitonic_sequence(keys, values);
}

/* 
 * bitonic_sort_2v: sort the elements in the vector keys1 and keys2 using bitonal sorting.
 *
 * */
inline void bitonic_sort_2v(__m512i& keys1, __m512i& keys2,
                                 __m512i& values1, __m512i& values2)
{

  bitonic_sort_1v(keys1, values1);
  bitonic_sort_1v(keys2, values2);
  compare_and_exchange_2v(keys1, keys2, values1, values2); 
  sort_bitonic_sequence(keys1, values1);
  sort_bitonic_sequence(keys2, values2);
}

inline void bitonic_sort_3v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& values1, __m512i& values2, 
                              __m512i& values3)
{
  bitonic_sort_2v(keys1, keys2, values1, values2);
  bitonic_sort_1v(keys3, values3);
  compare_and_exchange_2v(keys2, keys3, values2, values3);
  sort_bitonic_sequence_2v(keys1, keys2, values1, values2);
  sort_bitonic_sequence(keys3, values3);
}


inline void bitonic_sort_4v(__m512i& keys1, __m512i& keys2, __m512i& keys3, 
                             __m512i& keys4, __m512i& values1, 
                             __m512i& values2, __m512i& values3, 
                             __m512i& values4)
{
  bitonic_sort_2v(keys1, keys2, values1, values2);
  bitonic_sort_2v(keys3, keys4, values3, values4);
  compare_and_exchange_2v(keys1, keys4, values1, values4);
  compare_and_exchange_2v(keys2, keys3, values2, values3);
  sort_bitonic_sequence_2v(keys1, keys2, values1, values2);
  sort_bitonic_sequence_2v(keys3, keys4, values3, values4);
}

inline void bitonic_sort_5v(__m512i& keys1, __m512i& keys2, __m512i& keys3, 
                             __m512i& keys4, __m512i& keys5, __m512i& values1, 
                             __m512i& values2, __m512i& values3, 
                             __m512i& values4, __m512i& values5)
{
  bitonic_sort_4v(keys1, keys2, keys3, keys4, values1, values2, values3,
                    values4);
  bitonic_sort_1v(keys5, values5);
  compare_and_exchange_2v(keys4, keys5, values4, values5);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4,
                             values1, values2, values3, values4);
  sort_bitonic_sequence(keys5, values5);
}

inline void bitonic_sort_6v(__m512i& keys1, __m512i& keys2, __m512i& keys3, 
                             __m512i& keys4, __m512i& keys5, __m512i& keys6,
                             __m512i& values1, __m512i& values2, 
                             __m512i& values3, __m512i& values4,
                             __m512i& values5, __m512i& values6)
{
  bitonic_sort_4v(keys1, keys2, keys3, keys4, values1, values2, values3,
                    values4);
  bitonic_sort_2v(keys5, keys6, values5, values6);
  compare_and_exchange_2v(keys3, keys6, values3, values6);
  compare_and_exchange_2v(keys4, keys5, values4, values5);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4,
                             values1, values2, values3, values4);
  sort_bitonic_sequence_2v(keys5, keys6, values5, values6);
}

inline void bitonic_sort_7v(__m512i& keys1, __m512i& keys2, __m512i& keys3, 
                              __m512i& keys4, __m512i& keys5, __m512i& keys6, 
                              __m512i& keys7, __m512i& values1, 
                              __m512i& values2, __m512i& values3, 
                              __m512i& values4, __m512i& values5, 
                              __m512i& values6, __m512i& values7)
{
  bitonic_sort_4v(keys1, keys2, keys3, keys4, values1, values2, values3,
                    values4);
  bitonic_sort_3v(keys5, keys6, keys7, values5, values6, values7);
  compare_and_exchange_2v(keys4, keys5, values4, values5);
  compare_and_exchange_2v(keys3, keys6, values3, values6);
  compare_and_exchange_2v(keys2, keys7, values2, values7);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4,
                             values1, values2, values3, values4);
  sort_bitonic_sequence_3v(keys5, keys6, keys7, values5, values6, values7);
}

inline void bitonic_sort_8v(__m512i& keys1, __m512i& keys2, __m512i& keys3, 
                              __m512i& keys4, __m512i& keys5, __m512i& keys6, 
                              __m512i& keys7, __m512i& keys8, __m512i& values1, 
                              __m512i& values2, __m512i& values3, 
                              __m512i& values4, __m512i& values5, 
                              __m512i& values6, __m512i& values7, 
                              __m512i& values8)
{
  bitonic_sort_4v(keys1, keys2, keys3, keys4, values1, values2, values3,
                    values4);
  bitonic_sort_4v(keys5, keys6, keys7, keys8, values5, values6, values7, 
                    values8);
  compare_and_exchange_2v(keys4, keys5, values4, values5);
  compare_and_exchange_2v(keys3, keys6, values3, values6);
  compare_and_exchange_2v(keys2, keys7, values2, values7);
  compare_and_exchange_2v(keys1, keys8, values1, values8);
  sort_bitonic_sequence_4v(keys1, keys2, keys3, keys4,
                             values1, values2, values3, values4);
  sort_bitonic_sequence_4v(keys5, keys6, keys7, keys8,
                             values5, values6, values7, values8);
}

inline void bitonic_sort_9v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_1v(keys9, values9);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence(keys9, values9);
}

inline void bitonic_sort_10v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_2v(keys9, keys10, values9, values10);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_2v(keys9, keys10, values9, values10);
}

inline void bitonic_sort_11v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10, __m512i& keys11,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10,
                              __m512i& values11)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_3v(keys9, keys10, keys11, values9, values10, values11);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  compare_and_exchange_2v(keys6, keys11, values6, values11);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_3v(keys9, keys10, keys11, values9, values10, 
                             values11);
}

inline void bitonic_sort_12v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10, __m512i& keys11, __m512i& keys12,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10,
                              __m512i& values11, __m512i& values12)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_4v(keys9, keys10, keys11, keys12, values9, values10, values11,
                    values12);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  compare_and_exchange_2v(keys6, keys11, values6, values11);
  compare_and_exchange_2v(keys5, keys12, values5, values12);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_4v(keys9, keys10, keys11, keys12, values9, values10, 
                             values11, values12);
}

inline void bitonic_sort_13v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10, __m512i& keys11, __m512i& keys12,
                              __m512i& keys13,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10,
                              __m512i& values11, __m512i& values12,
                              __m512i& values13)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_5v(keys9, keys10, keys11, keys12, keys13, values9, values10, 
                    values11, values12, values13);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  compare_and_exchange_2v(keys6, keys11, values6, values11);
  compare_and_exchange_2v(keys5, keys12, values5, values12);
  compare_and_exchange_2v(keys4, keys13, values4, values13);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_5v(keys9, keys10, keys11, keys12, keys13, values9, 
                             values10, values11, values12, values13);
}

inline void bitonic_sort_14v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10, __m512i& keys11, __m512i& keys12,
                              __m512i& keys13, __m512i& keys14,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10,
                              __m512i& values11, __m512i& values12,
                              __m512i& values13, __m512i& values14)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_6v(keys9, keys10, keys11, keys12, keys13, keys14, values9, 
                    values10, values11, values12, values13, values14);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  compare_and_exchange_2v(keys6, keys11, values6, values11);
  compare_and_exchange_2v(keys5, keys12, values5, values12);
  compare_and_exchange_2v(keys4, keys13, values4, values13);
  compare_and_exchange_2v(keys3, keys14, values3, values14);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_6v(keys9, keys10, keys11, keys12, keys13, keys14,
                            values9, values10, values11, values12, values13, 
                            values14);
}

inline void bitonic_sort_15v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10, __m512i& keys11, __m512i& keys12,
                              __m512i& keys13, __m512i& keys14, __m512i& keys15,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10,
                              __m512i& values11, __m512i& values12,
                              __m512i& values13, __m512i& values14,
                              __m512i& values15)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_7v(keys9, keys10, keys11, keys12, keys13, keys14, keys15,
                    values9, values10, values11, values12, values13,
                    values14, values15);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  compare_and_exchange_2v(keys6, keys11, values6, values11);
  compare_and_exchange_2v(keys5, keys12, values5, values12);
  compare_and_exchange_2v(keys4, keys13, values4, values13);
  compare_and_exchange_2v(keys3, keys14, values3, values14);
  compare_and_exchange_2v(keys2, keys15, values2, values15);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_7v(keys9, keys10, keys11, keys12, keys13, keys14, 
                             keys15, values9, values10, values11, values12, 
                             values13, values14, values15);
}

inline void bitonic_sort_16v(__m512i& keys1, __m512i& keys2, __m512i& keys3,
                              __m512i& keys4, __m512i& keys5, __m512i& keys6,
                              __m512i& keys7, __m512i& keys8, __m512i& keys9,
                              __m512i& keys10, __m512i& keys11, __m512i& keys12,
                              __m512i& keys13, __m512i& keys14, __m512i& keys15,
                              __m512i& keys16,
                              __m512i& values1, __m512i& values2,
                              __m512i& values3, __m512i& values4,
                              __m512i& values5, __m512i& values6,
                              __m512i& values7, __m512i& values8,
                              __m512i& values9, __m512i& values10,
                              __m512i& values11, __m512i& values12,
                              __m512i& values13, __m512i& values14,
                              __m512i& values15, __m512i& values16)
{
  bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                    values1, values2, values3, values4, values5, values6, 
                    values7, values8);
  bitonic_sort_8v(keys9, keys10, keys11, keys12, keys13, keys14, keys15, keys16,
                    values9, values10, values11, values12, values13,
                    values14, values15, values16);
  compare_and_exchange_2v(keys8, keys9, values8, values9);
  compare_and_exchange_2v(keys7, keys10, values7, values10);
  compare_and_exchange_2v(keys6, keys11, values6, values11);
  compare_and_exchange_2v(keys5, keys12, values5, values12);
  compare_and_exchange_2v(keys4, keys13, values4, values13);
  compare_and_exchange_2v(keys3, keys14, values3, values14);
  compare_and_exchange_2v(keys2, keys15, values2, values15);
  compare_and_exchange_2v(keys1, keys16, values1, values16);
  sort_bitonic_sequence_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                             keys8, values1, values2, values3, values4,
                             values5, values6, values7, values8);
  sort_bitonic_sequence_8v(keys9, keys10, keys11, keys12, keys13, keys14, 
                             keys15, keys16, values9, values10, values11, 
                             values12, values13, values14, values15, values16);
}

/*
 * load_last_vec:
 *
 *  A 512bit vector register can contain 8 64bit variables. If the number of 
 *  sorted elements is less than 8, for example, we have 5 sorted elements: 
 *  5 7 8 3 2. In order to fill a vector to sort, we need to add 3 the maximum 
 *  value of uint64_t.
 *
 * */
inline __m512i load_last_vec(uint64_t* ptr, int32_t last_vec_size, int32_t rest)
{
  __m512i last_vec = _mm512_or_si512(_mm512_maskz_loadu_epi64(0xFF>>rest, ptr),
                                       _mm512_maskz_set1_epi64(
                                         0xFF<<last_vec_size, 
                                         ULLONG_MAX));
  return last_vec;
}

/*
 * store_last_vec:
 *
 * As said in `load_last_vec`, there are 8 elements in a vector, but only the 
 * first 5 are valid, and the valid elements are stored back into the array.
 *
 * */
inline void store_last_vec(uint64_t* ptr, __m512i& vec, int32_t rest)
{
  _mm512_mask_compressstoreu_epi64(ptr, 0xFF>>rest, vec);
}

/*
 * load_1v: load a vector from the array
 *
 * */
inline void load_1v(uint64_t* ptr, __m512i& vec)
{
  vec  = _mm512_loadu_si512(ptr);
}

/*
 * store_1v: store a vector to the array
 *
 * */
inline void load_1v(uint64_t* ptr, __m512i& vec);
inline void store_1v(uint64_t* ptr, __m512i& vec)
{
  _mm512_storeu_si512(ptr, vec);
}

inline void load_2v(uint64_t* ptr, __m512i& vec1, __m512i& vec2)
{
  vec1  = _mm512_loadu_si512(ptr);
  vec2  = _mm512_loadu_si512(ptr + 8);
}

inline void store_2v(uint64_t* ptr, __m512i& vec1, __m512i& vec2)
{
  _mm512_storeu_si512(ptr, vec1);
  _mm512_storeu_si512(ptr + 8, vec2);
}

inline void load_3v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3)
{
  load_2v(ptr, vec1, vec2);
  vec3  = _mm512_loadu_si512(ptr + 16);
}

inline void store_3v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3)
{
  store_2v(ptr, vec1, vec2);
  _mm512_storeu_si512(ptr + 16, vec3);
}

inline void load_4v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4)
{
  load_3v(ptr, vec1, vec2, vec3);
  vec4  = _mm512_loadu_si512(ptr + 24);
}

inline void store_4v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4)
{
  store_3v(ptr, vec1, vec2, vec3);
  _mm512_storeu_si512(ptr + 24, vec4);
}

inline void load_5v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5)
{
  load_4v(ptr, vec1, vec2, vec3, vec4);
  vec5  = _mm512_loadu_si512(ptr + 32);
}
inline void store_5v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5)
{
  store_4v(ptr, vec1, vec2, vec3, vec4);
  _mm512_storeu_si512(ptr + 32, vec5);
}

inline void load_6v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6)
{
  load_5v(ptr, vec1, vec2, vec3, vec4, vec5);
  vec6  = _mm512_loadu_si512(ptr + 40);
}

inline void store_6v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6)
{
  store_5v(ptr, vec1, vec2, vec3, vec4, vec5);
  _mm512_storeu_si512(ptr + 40, vec6);
}

inline void load_7v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7)
{
  load_6v(ptr, vec1, vec2, vec3, vec4, vec5, vec6);
  vec7  = _mm512_loadu_si512(ptr + 48);
}

inline void store_7v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7)
{
  store_6v(ptr, vec1, vec2, vec3, vec4, vec5, vec6);
  _mm512_storeu_si512(ptr + 48, vec7);
}

inline void load_8v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8)
{
  load_7v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7);
  vec8  = _mm512_loadu_si512(ptr + 56);
}

inline void store_8v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8)
{
  store_7v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7);
  _mm512_storeu_si512(ptr + 56, vec8);
}

inline void load_9v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9)
{
  load_8v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8);
  vec9 = _mm512_loadu_si512(ptr + 64);
}

inline void store_9v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9)
{
  store_8v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8);
  _mm512_storeu_si512(ptr + 64, vec9);
}

inline void load_10v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10)
{
  load_9v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9);
  vec10 = _mm512_loadu_si512(ptr + 72);
}

inline void store_10v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10)
{
  store_9v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9);
  _mm512_storeu_si512(ptr + 72, vec10);
}

inline void load_11v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11)
{
  load_10v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10);
  vec11 = _mm512_loadu_si512(ptr + 80);
}

inline void store_11v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11)
{
  store_10v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10);
  _mm512_storeu_si512(ptr + 80, vec11);
}

inline void load_12v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12)
{
  load_11v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11);
  vec12 = _mm512_loadu_si512(ptr + 88);
}

inline void store_12v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12)
{
  store_11v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11);
  _mm512_storeu_si512(ptr + 88, vec12);
}

inline void load_13v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12, __m512i& vec13)
{
  load_12v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11, vec12);
  vec13 = _mm512_loadu_si512(ptr + 96);
}

inline void store_13v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12, __m512i& vec13)
{
  store_12v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11, vec12);
  _mm512_storeu_si512(ptr + 96, vec13);
}

inline void load_14v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12, __m512i& vec13, __m512i& vec14)
{
  load_13v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11, vec12, vec13);
  vec14 = _mm512_loadu_si512(ptr + 104);
}

inline void store_14v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12, __m512i& vec13, __m512i& vec14)
{
  store_13v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11, vec12, vec13);
  _mm512_storeu_si512(ptr + 104, vec14);
}

inline void load_15v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12, __m512i& vec13, __m512i& vec14,
                      __m512i& vec15)
{
  load_14v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11, vec12, vec13, vec14);
  vec15 = _mm512_loadu_si512(ptr + 112);
}

inline void store_15v(uint64_t* ptr, __m512i& vec1, __m512i& vec2, 
                      __m512i& vec3, __m512i& vec4, __m512i& vec5,
                      __m512i& vec6, __m512i& vec7, __m512i& vec8,
                      __m512i& vec9, __m512i& vec10, __m512i& vec11,
                      __m512i& vec12, __m512i& vec13, __m512i& vec14,
                      __m512i& vec15)
{
  store_14v(ptr, vec1, vec2, vec3, vec4, vec5, vec6, vec7, vec8, vec9, vec10,
             vec11, vec12, vec13, vec14);
  _mm512_storeu_si512(ptr + 112, vec15);
}

/*
 * bitonic_sort: sort the elements in array by vectorized bitonic, but only 
 * numeric array with elements less than 128 can be sorted.
 * 1. load elements of array to vectors.
 * 2. sort vectors using bitonic sort.
 * 3. store ordered elements in vectors to array.
 *
 * Since the length of the numeric array may not be a multiple of 8, the last 
 * vector sequence is processed separately.
 *
 * example:
 * ptr_keys: 8 2 9 7 5 6 1 7 4 3 9
 * len: 11
 *
 * 1. load `8 2 9 7 5 6 1 7` to keys1.
 *    laod `4 3 9 M M M M M` to keys2.
 *    (M represents the max of uint64_t)
 * 2. sort keys1 ane keys2 using sort_bitonic_sequence_2v
 * 3. store elements in `keys1` and `keys2` to `ptr_keys`
 *
 **/
inline void bitonic_sort(uint64_t* __restrict__ ptr_keys, 
                           uint64_t* __restrict__ ptr_values, 
                           const int64_t len)
{
  const int32_t vec_cap = 8;
  const int32_t n = (len + 7) / vec_cap;
  const int32_t rest = n * vec_cap - len;
  const int last_vec_size = vec_cap - rest;

  __m512i keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, keys9, keys10;
  __m512i keys11, keys12, keys13, keys14, keys15, keys16;

  __m512i values1, values2, values3, values4, values5, values6, values7; 
  __m512i values8, values9, values10, values11, values12, values13, values14;
  __m512i values15, values16;
  switch(n){
  case 1: {
    keys1 = load_last_vec(ptr_keys, last_vec_size, rest);
    values1 = load_last_vec(ptr_values, last_vec_size, rest);
    bitonic_sort_1v(keys1, values1);
    store_last_vec(ptr_keys, keys1, rest);
    store_last_vec(ptr_values, values1, rest);
    break;
  }
  case 2: {
    load_1v(ptr_keys, keys1);
    load_1v(ptr_values, values1);
    keys2 = load_last_vec(ptr_keys + 8, last_vec_size, rest);
    values2 = load_last_vec(ptr_values + 8, last_vec_size, rest);
    bitonic_sort_2v(keys1, keys2, values1, values2);
    store_1v(ptr_keys, keys1);
    store_1v(ptr_values, values1);
    store_last_vec(ptr_keys + 8, keys2, rest);
    store_last_vec(ptr_values + 8, values2, rest);
    break;
  }
  case 3: {
    load_2v(ptr_keys, keys1, keys2);
    load_2v(ptr_values, values1, values2);
    keys3 = load_last_vec(ptr_keys + 16, last_vec_size, rest);
    values3 = load_last_vec(ptr_values + 16, last_vec_size, rest);
    bitonic_sort_3v(keys1, keys2, keys3, values1, values2, values3);
    store_2v(ptr_keys, keys1, keys2);
    store_2v(ptr_values, values1, values2);
    store_last_vec(ptr_keys + 16, keys3, rest);
    store_last_vec(ptr_values + 16, values3, rest);
    break;
  }
  case 4: {
    load_3v(ptr_keys, keys1, keys2, keys3);
    load_3v(ptr_values, values1, values2, values3);
    keys4 = load_last_vec(ptr_keys + 24, last_vec_size, rest);
    values4 = load_last_vec(ptr_values + 24, last_vec_size, rest);
    bitonic_sort_4v(keys1, keys2, keys3, keys4, values1, values2, values3, 
                      values4);
    store_3v(ptr_keys, keys1, keys2, keys3);
    store_3v(ptr_values, values1, values2, values3);
    store_last_vec(ptr_keys + 24, keys4, rest);
    store_last_vec(ptr_values + 24, values4, rest);
    break;
  }
  case 5: {
    load_4v(ptr_keys, keys1, keys2, keys3, keys4);
    load_4v(ptr_values, values1, values2, values3, values4);
    keys5 = load_last_vec(ptr_keys + 32, last_vec_size, rest);
    values5 = load_last_vec(ptr_values + 32, last_vec_size, rest);
    bitonic_sort_5v(keys1, keys2, keys3, keys4, keys5, 
                      values1, values2, values3, values4, values5);
    store_4v(ptr_keys, keys1, keys2, keys3, keys4);
    store_4v(ptr_values, values1, values2, values3, values4);
    store_last_vec(ptr_keys + 32, keys5, rest);
    store_last_vec(ptr_values + 32, values5, rest);
    break;
  }
  case 6: {
    load_5v(ptr_keys, keys1, keys2, keys3, keys4, keys5);
    load_5v(ptr_values, values1, values2, values3, values4, values5);
    keys6 = load_last_vec(ptr_keys + 40, last_vec_size, rest);
    values6 = load_last_vec(ptr_values + 40, last_vec_size, rest);
    bitonic_sort_6v(keys1, keys2, keys3, keys4, keys5, keys6, 
                      values1, values2, values3, values4, values5, values6);
    store_5v(ptr_keys, keys1, keys2, keys3, keys4, keys5);
    store_5v(ptr_values, values1, values2, values3, values4, values5);
    store_last_vec(ptr_keys + 40, keys6, rest);
    store_last_vec(ptr_values + 40, values6, rest);
    break;
  }
  case 7: {
    load_6v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6);
    load_6v(ptr_values, values1, values2, values3, values4, values5, values6);
    keys7 = load_last_vec(ptr_keys + 48, last_vec_size, rest);
    values7 = load_last_vec(ptr_values + 48, last_vec_size, rest);
    bitonic_sort_7v(keys1, keys2, keys3, keys4, keys5, keys6, keys7,
                      values1, values2, values3, values4, values5, values6,
                      values7);
    store_6v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6);
    store_6v(ptr_values, values1, values2, values3, values4, values5, values6);
    store_last_vec(ptr_keys + 48, keys7, rest);
    store_last_vec(ptr_values + 48, values7, rest);
    break;
  }
  case 8: {
    load_7v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7);
    load_7v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7);
    keys8 = load_last_vec(ptr_keys + 56, last_vec_size, rest);
    values8 = load_last_vec(ptr_values + 56, last_vec_size, rest);
    bitonic_sort_8v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      values1, values2, values3, values4, values5, values6,
                      values7, values8);
    store_7v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7);
    store_7v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7);
    store_last_vec(ptr_keys + 56, keys8, rest);
    store_last_vec(ptr_values + 56, values8, rest);
    break;
  }
  case 9: {
    load_8v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8);
    load_8v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8);
    keys9 = load_last_vec(ptr_keys + 64, last_vec_size, rest);
    values9 = load_last_vec(ptr_values + 64, last_vec_size, rest);
    bitonic_sort_9v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, values1, values2, values3, values4, values5,
                      values6, values7, values8, values9);
    store_8v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8);
    store_8v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8);
    store_last_vec(ptr_keys + 64, keys9, rest);
    store_last_vec(ptr_values + 64, values9, rest);
    break;
  }
  case 10: {
    load_9v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9);
    load_9v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9);
    keys10 = load_last_vec(ptr_keys + 72, last_vec_size, rest);
    values10 = load_last_vec(ptr_values + 72, last_vec_size, rest);
    bitonic_sort_10v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, values1, values2, values3, values4,
                      values5, values6, values7, values8, values9, values10);
    store_9v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9);
    store_9v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9);
    store_last_vec(ptr_keys + 72, keys10, rest);
    store_last_vec(ptr_values + 72, values10, rest);
    break;
  }
  case 11: {
    load_10v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9, keys10);
    load_10v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10);
    keys11 = load_last_vec(ptr_keys + 80, last_vec_size, rest);
    values11 = load_last_vec(ptr_values + 80, last_vec_size, rest);
    bitonic_sort_11v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, keys11, values1, values2, values3, values4,
                      values5, values6, values7, values8, values9, values10,
                      values11);
    store_10v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9, keys10);
    store_10v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10);
    store_last_vec(ptr_keys + 80, keys11, rest);
    store_last_vec(ptr_values + 80, values11, rest);
    break;
  }
  case 12: {
    load_11v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9, keys10, keys11);
    load_11v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11);
    keys12 = load_last_vec(ptr_keys + 88, last_vec_size, rest);
    values12 = load_last_vec(ptr_values + 88, last_vec_size, rest);
    bitonic_sort_12v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, keys11, keys12, values1, values2, values3,
                      values4, values5, values6, values7, values8, values9,
                      values10, values11, values12);
    store_11v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9, keys10, keys11);
    store_11v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11);
    store_last_vec(ptr_keys + 88, keys12, rest);
    store_last_vec(ptr_values + 88, values12, rest);
    break;
  }
  case 13: {
    load_12v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9, keys10, keys11, keys12);
    load_12v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12);
    keys13 = load_last_vec(ptr_keys + 96, last_vec_size, rest);
    values13 = load_last_vec(ptr_values + 96, last_vec_size, rest);
    bitonic_sort_13v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, keys11, keys12, keys13, values1, values2, 
                      values3, values4, values5, values6, values7, values8,
                      values9, values10, values11, values12, values13);
    store_12v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9, keys10, keys11, keys12);
    store_12v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12);
    store_last_vec(ptr_keys + 96, keys13, rest);
    store_last_vec(ptr_values + 96, values13, rest);
    break;
  }
  case 14: {
    load_13v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9, keys10, keys11, keys12, keys13);
    load_13v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12,
              values13);
    keys14 = load_last_vec(ptr_keys + 104, last_vec_size, rest);
    values14 = load_last_vec(ptr_values + 104, last_vec_size, rest);
    bitonic_sort_14v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, keys11, keys12, keys13, keys14, values1,
                      values2, values3, values4, values5, values6, values7,
                      values8, values9, values10, values11, values12, values13,
                      values14);
    store_13v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9, keys10, keys11, keys12, keys13);
    store_13v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12, 
              values13);
    store_last_vec(ptr_keys + 104, keys14, rest);
    store_last_vec(ptr_values + 104, values14, rest);
    break;
  }
  case 15: {
    load_14v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9, keys10, keys11, keys12, keys13, keys14);
    load_14v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12,
              values13, values14);
    keys15 = load_last_vec(ptr_keys + 112, last_vec_size, rest);
    values15 = load_last_vec(ptr_values + 112, last_vec_size, rest);
    bitonic_sort_15v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, keys11, keys12, keys13, keys14, keys15,
                      values1, values2, values3, values4, values5, values6,
                      values7, values8, values9, values10, values11, values12,
                      values13, values14, values15);
    store_14v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9, keys10, keys11, keys12, keys13, keys14);
    store_14v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12,
              values13, values14);
    store_last_vec(ptr_keys + 112, keys15, rest);
    store_last_vec(ptr_values + 112, values15, rest);
    break;
  }
  default: {
    load_15v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8, 
              keys9, keys10, keys11, keys12, keys13, keys14, keys15);
    load_15v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12,
              values13, values14, values15);
    keys16 = load_last_vec(ptr_keys + 120, last_vec_size, rest);
    values16 = load_last_vec(ptr_values + 120, last_vec_size, rest);
    bitonic_sort_16v(keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
                      keys9, keys10, keys11, keys12, keys13, keys14, keys15,
                      keys16, values1, values2, values3, values4, values5,
                      values6, values7, values8, values9, values10, values11,
                      values12, values13, values14, values15, values16);

    store_15v(ptr_keys, keys1, keys2, keys3, keys4, keys5, keys6, keys7, keys8,
               keys9, keys10, keys11, keys12, keys13, keys14, keys15);
    store_15v(ptr_values, values1, values2, values3, values4, values5, values6,
              values7, values8, values9, values10, values11, values12,
              values13, values14, values15);
    store_last_vec(ptr_keys + 120, keys16, rest);
    store_last_vec(ptr_values + 120, values16, rest);
  }
  }
}

/* 
 * popcount : count the number of bit 1 in the mask
 *
 */
inline int popcount(__mmask16 mask)
{
#ifdef __INTEL_COMPILER
  return _mm_countbits_32(mask);
#else
  return __builtin_popcount(mask);
#endif
}

/*
 * compare_and_store :
 * Compare the `keys` and `pitvotvec`. If flag is true, write the part smaller 
 * than pivot into `keys_ptr+left_w`, and write the remaining part into 
 * `keys_ptr+right_w`. If flag is false, the part less than or equal to pivot 
 * is written into `keys_ptr+left_w`, and the remaining part is written into 
 * `keys_ptr+right_w`.
 * The `pivotvec` contains 8 same values, similar to pivot in quick sort.
 *
 * example:
 * keys:     8  10 17 6  5  12 7  10
 * pivotvec: 10 10 10 10 10 10 10 10
 * flag:     true
 *
 * 8  6  5  7  store into keys_ptr+left_w
 * 10 17 12 10 store into keys_ptr+right_w
 * 
 * */
inline void compare_and_store(uint64_t* keys_ptr, uint64_t* values_ptr,
                                __m512i& pivotvec, __m512i& keys,
                                __m512i& values, int64_t& left_w,
                                int64_t& right_w, bool flag) {
  const int32_t vec_cap = 8;
  __mmask8 mask;
  if (flag) {
    mask = _mm512_cmp_epu64_mask(keys, pivotvec, _MM_CMPINT_LT);
  } else {
    mask = _mm512_cmp_epu64_mask(keys, pivotvec, _MM_CMPINT_LE);
  }
  const int64_t nb_low = popcount(mask);
  const int64_t nb_high = vec_cap - nb_low;
  _mm512_mask_compressstoreu_epi64(keys_ptr + left_w, mask, keys);
  _mm512_mask_compressstoreu_epi64(values_ptr + left_w, mask, values);
  left_w += nb_low;
  right_w -= nb_high;
  _mm512_mask_compressstoreu_epi64(keys_ptr + right_w, ~mask, keys);
  _mm512_mask_compressstoreu_epi64(values_ptr + right_w, ~mask, values);
}

/*
 * compare_and_store:
 *  Like compare_and_store, but only the value of the previous `remaining` part 
 *  of the `keys` is valid.
 *
 * example:
 * keys:      8  10 17 6  5  12 7  10
 * pivotvec:  10 10 10 10 10 10 10 10
 * flag:      true
 * remaining: 6
 *
 * 8  6  5  store into keys_ptr+left_w
 * 10 17 12 store into keys_ptr+right_w
 * 
 * */
inline void compare_and_store(uint64_t* keys_ptr, uint64_t* values_ptr,
                                __m512i& pivotvec, __m512i& keys,
                                __m512i& values, int64_t& left_w,
                                int64_t& right_w, int64_t remaining)
{
  __mmask8 mask = _mm512_cmp_epu64_mask(keys, pivotvec, _MM_CMPINT_LE);
  __mmask8 mask_low = mask & ~(0xFF << remaining);
  __mmask8 mask_high = (~mask) & ~(0xFF << remaining);
  const int64_t nb_low = popcount(mask_low);
  const int64_t nb_high = popcount(mask_high);
  _mm512_mask_compressstoreu_epi64(keys_ptr + left_w, mask_low, keys);
  _mm512_mask_compressstoreu_epi64(values_ptr + left_w, mask_low, values);
  left_w += nb_low;
  right_w -= nb_high;
  _mm512_mask_compressstoreu_epi64(keys_ptr + right_w, mask_high, keys);
  _mm512_mask_compressstoreu_epi64(values_ptr + right_w, mask_high, values);
}

/*
 * do_partition: divide the data into two parts according to pivot.
 *
 * */
static inline int64_t do_partition(uint64_t* keys, uint64_t* values, 
        int64_t left, int64_t right, const uint64_t pivot)
{
  const int32_t vec_cap = 8;
  __m512i pivotvec = _mm512_set1_epi64(pivot);
  __m512i tmp_left = _mm512_loadu_si512(keys + left);
  __m512i tmp_left_val = _mm512_loadu_si512(values + left);
  int64_t left_w = left;
  left += vec_cap;

  int64_t right_w = right + 1;
  right -= vec_cap - 1;
  __m512i tmp_right = _mm512_loadu_si512(keys + right);
  __m512i tmp_right_val = _mm512_loadu_si512(values + right);
  while(left + vec_cap <= right) {
    const int64_t free_left = left - left_w;
    const int64_t free_right = right_w - right;
    __m512i tmp;
    __m512i tmp_val;
    if(free_left <= free_right) {
      tmp = _mm512_loadu_si512(keys + left);
      tmp_val = _mm512_loadu_si512(values + left);
      left += vec_cap;
      compare_and_store(keys, values, pivotvec, tmp, tmp_val, left_w,
                          right_w, true);
    } else {
      right -= vec_cap;
      tmp = _mm512_loadu_si512(keys + right);
      tmp_val = _mm512_loadu_si512(values + right);
      compare_and_store(keys, values, pivotvec, tmp, tmp_val, left_w,
                          right_w, false);
    }
  }
  const int64_t remaining = right - left;
  __m512i tmp = _mm512_loadu_si512(keys + left);
  __m512i tmp_val = _mm512_loadu_si512(values + left);
  left = right;
  compare_and_store(keys, values, pivotvec, tmp, tmp_val, left_w, right_w,
                      remaining);
  compare_and_store(keys, values, pivotvec, tmp_left, tmp_left_val, left_w, 
                      right_w, true);
  compare_and_store(keys, values, pivotvec, tmp_right, tmp_right_val, left_w, 
                      right_w, false);
  return left_w;
}

inline int64_t lg(int64_t n)
{
  int64_t k;
  for (k = 0; n > 1; n >>= 1) ++k;
  return k;
}

inline int64_t get_pivot(const uint64_t* ptr, const int64_t left,
                           const int64_t right)
{
  const int64_t middle = ((right-left) / 2) + left;
  int64_t ret=0;
  if((ptr[middle]>ptr[left] && ptr[left]>=ptr[right]) || (ptr[right]>=ptr[left] && ptr[left]>ptr[middle])) {
    ret = left;
  } else if((ptr[left]>=ptr[middle] && ptr[middle]>=ptr[right]) || (ptr[right]>=ptr[middle] && ptr[middle]>=ptr[left])) {
    ret = middle;
  } else if((ptr[left]>ptr[right] && ptr[right]>ptr[middle]) || (ptr[middle]>ptr[right] && ptr[right]>ptr[left])) {
    ret = right;
  }  

  return ret;
}

inline int64_t quick_sort(uint64_t* keys, uint64_t* values, const int64_t left,
                           const int64_t right)
{
  const int64_t idx = get_pivot(keys, left, right);
  std::swap(keys[idx], keys[right]);
  std::swap(values[idx], values[right]);
  const int64_t cut = do_partition(keys, values, left, right - 1, keys[right]);
  std::swap(keys[cut], keys[right]);
  std::swap(values[cut], values[right]);
  return cut;
}

inline void heapify(uint64_t* keys, uint64_t* values, int64_t idx, int64_t size)
{
  int i = idx;
  while(true) {
    int j = 2 * i + 1;
    if (j >= size || j < 0) {
      break;
    }
    int k = j + 1;
    if (k < size && keys[j] < keys[k]) {
      j = k;
    }
    if (keys[j] < keys[i]) {
      break;
    }
    std::swap(keys[i], keys[j]);
    std::swap(values[i], values[j]);
    i = j;
  }
}

inline void heap_sort(uint64_t* keys, uint64_t* values, int64_t size)
{
  for (int64_t i = size / 2 - 1; i >= 0; i--) {
    heapify(keys, values, i, size);
  }
  for (int64_t i = size - 1; i > 0; i--) {
    std::swap(keys[0], keys[i]);
    std::swap(values[0], values[i]);
    heapify(keys, values, 0, i);
  }
}

/*
 * The flow of the `do_sort function` is as follows:
 *  1. use vector quick sort to sort the elements.
 *  2. However, quick sort may have uneven partitions. When the recursion
 *  reaches a certain depth, heap sort should be used.
 *  3. In the process of quick sorting, the interval keeps shrinking. When the 
 *  interval length is less than 128, the bitontic sorting realized by 
 *  vectorization is needed.
 *  */
inline void do_sort(uint64_t* keys, uint64_t* values, const int64_t left, 
               const int64_t right, int64_t depth_limit)
{
  const int32_t limit = 128;
  if(right - left < limit){
    bitonic_sort(keys + left, values + left, right - left + 1);
  } else {
    if (depth_limit == 0) {
      heap_sort(keys + left, values + left, right - left + 1);
    } else {
      depth_limit--;
      const int64_t cut = quick_sort(keys, values, left, right);
      if(cut + 1 < right) {
        do_sort(keys, values, cut + 1, right, depth_limit);
      }
      if(cut && left < cut - 1) {
        do_sort(keys, values, left, cut - 1, depth_limit);
      }
    }
  }
}

/*
 * Due to the sorting algorithm implemented by the vectorization method can 
 * only sort numerical arrays.We use key-value to solve this problem. The key 
 * value array is used for comparison and sorting, the value array holds pointers 
 * to sorted elements. When the elements of the key exchanged, the elements 
 * of value are also exchanged.
 *
 * */
template <typename T>
int sort_loop(uint64_t* keys, T** values, int64_t round, 
                int64_t left, int64_t right)
{
  int ret = OB_SUCCESS;
  for (int64_t i = left; OB_SUCC(ret) && i < right; i++) {
    // Use the get_sort_key method provided by T to get the key needed for sorting
    ret = values[i]->get_sort_key(&keys[i]);
  }
  if (OB_SUCC(ret)) {
    // sort the keys array
    do_sort(keys, (uint64_t*)values, left, right - 1, lg(right - left) * 2);
  }
  /*
   * Process the same key value
   *
   * example:
   * If we use the sorting algorithm to sort the type T:
   *    struct T {
   *      uint64_t a,
   *      uint64_t b
   *      };
   * T's comparison function: 
   *    compare(T& t1, T& t2) {return t1.a == t2.a ? t1.b < t2.b : t1.a < t2.a;}
   *
   * Since the sorting algorithm implemented by vectorization can only sort 
   * numeric types, we first use T.a as the key to sort according to the 
   * comparison function. After a round of sorting with T.a, the same value 
   * of T.a will be sorted together, E.g: 
   *    1 1 1 2 2 3 3 3 3 3
   *
   * For the same T as T.a, we need to use T.b as the key for the next round of sorting.
   *
   * */
  int64_t idx = left;
  while (OB_SUCC(ret)) {
    while (idx < right - 1 && keys[idx] != keys[idx + 1]) {
      idx++;
    }
    int64_t next_left = idx;
    while (idx < right - 1 && keys[idx] == keys[idx + 1]) {
      idx++;
    }
    idx++;
    while (next_left < idx && next_left < right && 
             values[next_left]->get_sort_key_end()) {
      next_left++;
    }
    if (next_left >= right - 1) {
      break;
    }
    if (idx - next_left > 1) {
      ret = sort_loop(keys, values, round + 1, next_left, idx);
    }
  }
  return ret;
}

template <typename T>
int sort(uint64_t* keys, T** values, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (keys == NULL || values == NULL || size < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ret = sort_loop<T>(keys, values, 0, 0, size);
  }
  return ret;
}

template int sort(uint64_t* keys, ObSortRow** values,
                    const int64_t size);
template int sort(uint64_t* keys, ObStoreRow** values,
                    const int64_t size);
}  // end namespace storage
}  // end namespace oceanbase