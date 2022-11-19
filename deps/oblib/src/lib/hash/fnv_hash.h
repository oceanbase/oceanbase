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

#ifndef FNV_HASH_H_
#define FNV_HASH_H_

#include <cstdint>

namespace oceanbase
{
namespace common
{
namespace hash
{
#define fnv_offset_basis 0x811C9DC5 // 2166136261
#define fnv_prime        0x01000193 // 16777619

/*
fnv1_32(high 32bit) && fnv1a_32(low 32bit)
*/
constexpr static inline uint64_t
fnv1_32_and_fnv1a_32_compile_time_hash(char const* const str,
                                       const uint32_t fnv1 = fnv_offset_basis,
                                       const uint32_t fnv1a = fnv_offset_basis)
{
  return (str[0] == '\0') ? ((uint64_t)fnv1 << 32 | fnv1a) : fnv1_32_and_fnv1a_32_compile_time_hash(&str[1],
    (fnv1 * fnv_prime) ^ uint32_t(str[0]),
    (fnv1a ^ uint32_t(str[0])) * fnv_prime);
}

constexpr static inline uint64_t
fnv_hash_for_logger(char const* const str,
                    const int idx,
                    const uint32_t fnv1 = fnv_offset_basis,
                    const uint32_t fnv1a = fnv_offset_basis)
{
  return (idx < 0 || str[idx] == '/') ?
    ((uint64_t)fnv1 << 32 | fnv1a) :
    fnv_hash_for_logger(str, idx - 1, (fnv1 * fnv_prime) ^ uint32_t(str[idx]),
    (fnv1a ^ uint32_t(str[idx])) * fnv_prime);
}

template<int N>
constexpr static inline uint64_t
fnv_hash_for_logger(const char (&str)[N], const int FROM = N - 2)
{
  return fnv_hash_for_logger(str, FROM);
}

} // end of namespace hash
} // end of namespace common
} // end of namespace oceanbase

#endif // FNV_HASH_H_
