/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#ifndef OB_GENERATED_SCALAR_BP_FUNC_H_
#define OB_GENERATED_SCALAR_BP_FUNC_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
// packing 8 uint8_t once
void scalar_fastpackwithoutmask_8(const uint8_t *__restrict__ in, uint8_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_8(const uint8_t*__restrict__ in, uint8_t *__restrict__ out, const uint32_t bit);

// packing 32 uint8_t once
void scalar_fastpackwithoutmask_8_32_count(const uint8_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit);
void scalar_fastunpack_8_32_count(const uint32_t *__restrict__ _in, uint8_t *__restrict__ out, const uint32_t bit);

// packing 16 uint16_t once
void scalar_fastpackwithoutmask_16(const uint16_t *__restrict__ in, uint16_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_16(const uint16_t*__restrict__ in, uint16_t *__restrict__ out, const uint32_t bit);

// packing 32 uint16_t once
void scalar_fastpackwithoutmask_16_32_count(const uint16_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit);
void scalar_fastunpack_16_32_count(const uint32_t *__restrict__ _in, uint16_t *__restrict__ out, const uint32_t bit);

// packing 32 uint32_t once
void scalar_fastpackwithoutmask_32(const uint32_t *__restrict__ in, uint32_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_32(const uint32_t*__restrict__ in, uint32_t *__restrict__ out, const uint32_t bit);

// packing 64 uint64_t once
void scalar_fastpackwithoutmask_64(const uint64_t *__restrict__ in, uint64_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_64(const uint64_t*__restrict__ in, uint64_t *__restrict__ out, const uint32_t bit);


} // end namespace common
} // end namespace oceanbase
#endif /* OB_GENERATED_SCALAR_BP_FUNC_H_ */
