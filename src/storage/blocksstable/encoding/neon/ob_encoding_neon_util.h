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

#ifndef OCEANBASE_ENCODING_OB_NEON_CMP_UTIL_H_
#define OCEANBASE_ENCODING_OB_NEON_CMP_UTIL_H_

#if defined ( __ARM_NEON )
#include <arm_neon.h>
#endif

#include "lib/ob_define.h"

namespace oceanbase
{
namespace blocksstable
{

template <typename T_OUT, typename T_IN, int CMP_TYPE>
static T_OUT neon_cmp_int(T_IN left, T_IN right)
{
  UNUSEDx(left, right);
  STORAGE_LOG_RET(ERROR, OB_NOT_SUPPORTED, "Unsupported neon compare", "lbt", lbt());
  return left;
}

#if defined ( __ARM_NEON )
const uint8_t NEON_CMP_MASK_POWER_16[16]
    = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};

const uint16_t NEON_CMP_MASK_POWER_8[8]
    = {1, 2, 4, 8, 16, 32, 64, 128};


// comparison wrap up for 16 1-byte data
template <>
uint8x16_t neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_EQ>(uint8x16_t left, uint8x16_t right)
{
  return vceqq_u8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, int8x16_t, sql::WHITE_OP_EQ>(int8x16_t left, int8x16_t right)
{
  return vceqq_s8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_LT>(uint8x16_t left, uint8x16_t right)
{
  return vcltq_u8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, int8x16_t, sql::WHITE_OP_LT>(int8x16_t left, int8x16_t right)
{
  return vcltq_s8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_LE>(uint8x16_t left, uint8x16_t right)
{
  return vcleq_u8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, int8x16_t, sql::WHITE_OP_LE>(int8x16_t left, int8x16_t right)
{
  return vcleq_s8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_GE>(uint8x16_t left, uint8x16_t right)
{
  return vcgeq_u8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, int8x16_t, sql::WHITE_OP_GE>(int8x16_t left, int8x16_t right)
{
  return vcgeq_s8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_GT>(uint8x16_t left, uint8x16_t right)
{
  return vcgtq_u8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, int8x16_t, sql::WHITE_OP_GT>(int8x16_t left, int8x16_t right)
{
  return vcgtq_s8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, uint8x16_t, sql::WHITE_OP_NE>(uint8x16_t left, uint8x16_t right)
{
  return ~vceqq_u8(left, right);
}

template <>
uint8x16_t neon_cmp_int<uint8x16_t, int8x16_t, sql::WHITE_OP_NE>(int8x16_t left, int8x16_t right)
{
  return ~vceqq_s8(left, right);
}

// comparison wrap up for 8 2-bytes data
template <>
uint16x8_t neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_EQ>(uint16x8_t left, uint16x8_t right)
{
  return vceqq_u16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, int16x8_t, sql::WHITE_OP_EQ>(int16x8_t left, int16x8_t right)
{
  return vceqq_s16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_LT>(uint16x8_t left, uint16x8_t right)
{
  return vcltq_u16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, int16x8_t, sql::WHITE_OP_LT>(int16x8_t left, int16x8_t right)
{
  return vcltq_s16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_LE>(uint16x8_t left, uint16x8_t right)
{
  return vcleq_u16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, int16x8_t, sql::WHITE_OP_LE>(int16x8_t left, int16x8_t right)
{
  return vcleq_s16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_GE>(uint16x8_t left, uint16x8_t right)
{
  return vcgeq_u16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, int16x8_t, sql::WHITE_OP_GE>(int16x8_t left, int16x8_t right)
{
  return vcgeq_s16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_GT>(uint16x8_t left, uint16x8_t right)
{
  return vcgtq_u16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, int16x8_t, sql::WHITE_OP_GT>(int16x8_t left, int16x8_t right)
{
  return vcgtq_s16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, uint16x8_t, sql::WHITE_OP_NE>(uint16x8_t left, uint16x8_t right)
{
  return ~vceqq_u16(left, right);
}

template <>
uint16x8_t neon_cmp_int<uint16x8_t, int16x8_t, sql::WHITE_OP_NE>(int16x8_t left, int16x8_t right)
{
  return ~vceqq_s16(left, right);
}

#endif


} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_NEON_CMP_UTIL_H_
