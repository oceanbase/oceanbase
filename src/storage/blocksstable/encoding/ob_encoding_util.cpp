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

#define USING_LOG_PREFIX STORAGE

#include "ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
const char* OB_ENCODING_LABEL_HASH_TABLE = "EncodeHashTable";
const char* OB_ENCODING_LABEL_HT_FACTORY = "EncodeHTFactory";
const char* OB_ENCODING_LABEL_PIVOT = "EncodePivot";
const char* OB_ENCODING_LABEL_DATA_BUFFER = "EncodeDataBuf";
const char* OB_ENCODING_LABEL_ROW_BUFFER = "EncodeRowBuffer";
const char* OB_ENCODING_LABEL_MULTI_PREFIX_TREE = "EncodeMulPreTree";
const char* OB_ENCODING_LABEL_PREFIX_TREE_FACTORY = "EncodeTreeFactory";
const char* OB_ENCODING_LABEL_STRING_DIFF = "EncodeStrDiff";

uint64_t INTEGER_MASK_TABLE[sizeof(int64_t) + 1] = {
  0x0, 0xff, 0xffff, 0xffffff, 0xffffffff,
  0xffffffffff, 0xffffffffffff, 0xffffffffffffff, 0xffffffffffffffff
};

int64_t get_packing_size(bool &bit_packing, const uint64_t v, bool enable_bit_packing)
{
  int64_t bit_size = 0;
  int64_t size = 0;
  if (enable_bit_packing) {
    if (0 == v) {
      // at least one bit
      bit_size = 1;
    } else {
      bit_size = sizeof(v) * CHAR_BIT - __builtin_clzl(v);
    }
    size = bit_size / CHAR_BIT;
    int64_t ext = bit_size % CHAR_BIT;
    if (0 == ext) {
      bit_packing = false;
    } else if (CHAR_BIT - ext < size / 2 + 1) {
      // do not bit packing if save bit count than half byte count
      size++;
      bit_packing = false;
    } else {
      bit_packing = true;
    }
  } else {
    // using byte packing
    bit_packing = false;
    if (v <= UINT8_MAX) {
      size = 1;
    } else if (v <= UINT16_MAX) {
      size = 2;
    } else if (v <= UINT32_MAX) {
      size = 4;
    } else {
      size = 8;
    }
  }
  return bit_packing ? bit_size : size;
}

int64_t get_int_size(const uint64_t v)
{
  int64_t bit_size = 1;
  if (v > 0) {
    bit_size = sizeof(v) * CHAR_BIT - __builtin_clzl(v);
  }
  return (bit_size + CHAR_BIT - 1) / CHAR_BIT;
}

int64_t get_byte_packed_int_size(const uint64_t v)
{
  int64_t size = 0;
  if (v <= UINT8_MAX) {
    size = 1;
  } else if (v <= UINT16_MAX) {
    size = 2;
  } else if (v <= UINT32_MAX) {
    size = 4;
  } else {
    size = 8;
  }
  return size;
}

int ObMapAttrOperator::set_row_id_byte(const int64_t byte, int8_t &attr)
{
  int ret = OB_SUCCESS;
  if (1 == byte) {
    attr |= ROW_ID_ONE_BYTE;
  } else if (2 == byte) {
    attr |= ROW_ID_TWO_BYTE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row id byte should be less than 2", K(ret), K(byte));
  }
  return ret;
}

int ObMapAttrOperator::set_ref_byte(const int64_t byte, int8_t &attr)
{
  int ret = OB_SUCCESS;
  if (1 == byte) {
    attr |= REF_ONE_BYTE;
  } else if (2 == byte) {
    attr |= REF_TWO_BYTE;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ref byte should be less than 2", K(ret), K(byte));
  }
  return ret;
}

int ObMapAttrOperator::get_row_id_byte(const int8_t attr, int64_t &byte)
{
  int ret = OB_SUCCESS;
  if (attr & ROW_ID_ONE_BYTE) {
    byte = 1;
  } else if (attr & ROW_ID_TWO_BYTE) {
    byte = 2;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get row id byte", K(ret), K(attr));
  }
  return ret;
}

int ObMapAttrOperator::get_ref_byte(const int8_t attr, int64_t &byte)
{
  int ret = OB_SUCCESS;
  if (attr & REF_ONE_BYTE) {
    byte = 1;
  } else if (attr & REF_TWO_BYTE) {
    byte = 2;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get ref byte", K(ret), K(attr));
  }
  return ret;
}

int ObEncodingRowBufHolder::init(const int64_t macro_block_size,
                                 const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    reset();
  }
  if (OB_UNLIKELY(macro_block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid block size", K(ret), K(macro_block_size));
  } else if (!is_valid_tenant_id(tenant_id)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    allocator_.set_tenant_id(tenant_id);
    buf_size_limit_ = macro_block_size * 3;
    is_inited_ = true;
  }
  return ret;
}

void ObEncodingRowBufHolder::reset()
{
  allocator_.reuse();
  buf_size_limit_ = 0;
  alloc_size_ = 0;
  alloc_buf_ = nullptr;
  is_inited_ = false;
}

int ObEncodingRowBufHolder::try_alloc(const int64_t required_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited", K(ret));
  } else if (OB_UNLIKELY(required_size < 0 || required_size > buf_size_limit_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid required size for micro block", K(ret), K(required_size));
  } else if (required_size <= alloc_size_) {
    // Reuse allocated buffer
  } else {
    // Need re-allocate buffer
    allocator_.reuse();
    const int64_t expand_size = required_size * EXTRA_MEM_FACTOR;
    const int64_t alloc_size = expand_size > buf_size_limit_ ? buf_size_limit_ : expand_size;
    if (OB_ISNULL(alloc_buf_ = reinterpret_cast<char *>(allocator_.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to allocate memory", K(ret), K(required_size), K_(alloc_size));
    } else {
      alloc_size_ = alloc_size;
    }
  }
  return ret;
}

} // end namespace blocksstable
} // end namespace oceanbase
