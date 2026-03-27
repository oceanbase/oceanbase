/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#ifndef OB_GENERATED_UNALIGN_SIMD_BP_FUNC_H_
#define OB_GENERATED_UNALIGN_SIMD_BP_FUNC_H_

#include <stdint.h>
#include <string.h>
#include "ob_sse_to_neon.h"

namespace oceanbase
{
namespace common
{
void uSIMD_fastpackwithoutmask_128_16(const uint16_t *__restrict__ in,
                                      __m128i *__restrict__ out, const uint32_t bit);
void uSIMD_fastunpack_128_16(const __m128i *__restrict__ in,
                             uint16_t *__restrict__ out, const uint32_t bit);


void uSIMD_fastpackwithoutmask_128_32(const uint32_t *__restrict__ in,
                                      __m128i *__restrict__ out, const uint32_t bit);
void uSIMD_fastunpack_128_32(const __m128i *__restrict__ in,
                             uint32_t *__restrict__ out, const uint32_t bit);


//void uSIMD_fastpackwithoutmask_256_32(const uint32_t *__restrict__ in,
//                                       __m256i *__restrict__ out, const uint32_t bit);
//void uSIMD_fastunpack_256_32(const __m256i *__restrict__ in,
//                             uint32_t *__restrict__ out, const uint32_t bit);

// TODO add avx512 and uint64_t packing method

} // end namespace common
} // end namespace oceanbase
#endif /* OB_GENERATED_UNALIGN_SIMD_BP_FUNC_H_ */
