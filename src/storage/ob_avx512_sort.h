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
#include <avx512fintrin.h>
#include <climits>
#include <cfloat>
#include <cstring>
#include <ctype.h>
#include <stdint.h>

namespace oceanbase {
namespace storage {

template <typename T>
int sort(uint64_t* keys, T** values, const int64_t size);

}  // end namespace storage
}  // end namespace oceanbase

#endif