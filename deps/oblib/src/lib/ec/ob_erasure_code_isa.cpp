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

#include "lib/ec/ob_erasure_code_isa.h"

namespace oceanbase {
namespace common {

// static
int ObErasureCodeIsa::encode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
    unsigned char** data_blocks, unsigned char** parity_blocks)
{
  INIT_SUCC(ret);
  unsigned char* encode_gf_tables = nullptr;  // encoding table

  if (nullptr == data_blocks || 0 >= block_size || nullptr == parity_blocks) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", KP(data_blocks), K(block_size), KP(parity_blocks), K(ret));
  } else if (OB_FAIL(
                 ObECCacheManager::get_ec_cache_mgr().get_encoding_table(data_count, parity_count, encode_gf_tables))) {
    LIB_LOG(WARN, "failed to get encoding table", K(data_count), K(parity_count), K(ret));
  } else {
    ec_encode_data((int)block_size, (int)data_count, (int)parity_count, encode_gf_tables, data_blocks, parity_blocks);
  }

  return ret;
}

// static
int ObErasureCodeIsa::decode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
    const ObIArray<int64_t>& erase_indexes, unsigned char** data_blocks, unsigned char** recovery_blocks)
{
  INIT_SUCC(ret);

  int64_t all_count = data_count + parity_count;
  int64_t nerrs = erase_indexes.count();
  unsigned char* source_blocks[data_count];
  ObSEArray<int64_t, OB_MAX_EC_STRIPE_COUNT> decode_indexes;
  ObSEArray<bool, OB_MAX_EC_STRIPE_COUNT> src_in_err;

  if (nullptr == data_blocks || 0 >= block_size || nullptr == recovery_blocks || 0 >= nerrs) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", KP(data_blocks), K(block_size), KP(recovery_blocks), K(nerrs), K(ret));
  } else if (nerrs > parity_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "nerrs is more than parity count", K(nerrs), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    MEMSET(source_blocks, 0, sizeof(source_blocks));
    for (int64_t i = 0; OB_SUCC(ret) && i < all_count; i++) {
      bool found_flag = false;
      for (int64_t p = 0; p < nerrs; p++) {
        if (erase_indexes.at(p) == i) {
          found_flag = true;
          break;
        }
      }
      if (OB_FAIL(src_in_err.push_back(found_flag))) {
        LIB_LOG(WARN, "failed to push element into src_in_err", K(i), K(ret));
      }
    }

    // Construct decode indexes by removing error rows,get first data_block_count no error blocks
    for (int i = 0, r = 0; OB_SUCC(ret) && i < data_count; i++, r++) {
      while (src_in_err[r]) {
        r++;
      }

      if (OB_SUCC(decode_indexes.push_back(r))) {
        source_blocks[i] = data_blocks[i];
      }
    }
  }

  if (OB_SUCC(ret)) {
    unsigned char* decode_gf_tables = nullptr;
    if (OB_FAIL(ObECCacheManager::get_ec_cache_mgr().get_decoding_table(
            erase_indexes, decode_indexes, data_count, parity_count, decode_gf_tables))) {
      LIB_LOG(WARN,
          "failed to get decoding table",
          K(erase_indexes),
          K(decode_indexes),
          K(data_count),
          K(parity_count),
          K(ret));
    } else {
      ec_encode_data((int)block_size, (int)data_count, (int)nerrs, decode_gf_tables, source_blocks, recovery_blocks);
    }
  }
  return ret;
}

// static
int ObErasureCodeIsa::decode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
    const ObIArray<int64_t>& data_block_indexes, const ObIArray<int64_t>& erase_block_indexes,
    unsigned char** data_blocks, unsigned char** recovery_blocks)
{
  INIT_SUCC(ret);

  int64_t nerrs = erase_block_indexes.count();
  unsigned char* source_blocks[data_count];
  ObSEArray<int64_t, 16> decode_indexes;
  ObSEArray<bool, 16> src_in_err;

  MEMSET(source_blocks, 0, sizeof(source_blocks));

  if (nullptr == data_blocks || 0 >= block_size || nullptr == recovery_blocks || 0 >= nerrs) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", KP(data_blocks), K(block_size), KP(recovery_blocks), K(nerrs), K(ret));
  } else if (data_block_indexes.count() < data_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
        "data_block_indexes count is less than data_count!",
        K(data_block_indexes.count()),
        K(data_count),
        K(ret));
  } else if (nerrs > parity_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "parity_count is less than nerrs!", K(nerrs), K(parity_count), K(ret));
  }

  // Construct decode indexes by removing error rows,get first data_block_count no error blocks
  for (int i = 0; OB_SUCC(ret) && i < data_count; i++) {
    if (OB_SUCC(decode_indexes.push_back(data_block_indexes.at(i)))) {
      source_blocks[i] = data_blocks[i];
    }
  }

  if (OB_SUCC(ret)) {
    unsigned char* decode_gf_tables = nullptr;
    if (OB_FAIL(ObECCacheManager::get_ec_cache_mgr().get_decoding_table(
            erase_block_indexes, decode_indexes, data_count, parity_count, decode_gf_tables))) {
      LIB_LOG(WARN,
          "failed to get decoding table",
          K(erase_block_indexes),
          K(decode_indexes),
          K(data_count),
          K(parity_count),
          K(ret));
    } else {
      ec_encode_data((int)block_size, (int)data_count, (int)nerrs, decode_gf_tables, source_blocks, recovery_blocks);
    }
  }

  return ret;
}

// static
int ObErasureCodeIsa::append_encode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
    const int64_t block_index, unsigned char* data_block, unsigned char** parity_blocks)
{
  INIT_SUCC(ret);
  unsigned char* encode_gf_tables = nullptr;  // encoding table

  if (OB_ISNULL(data_block) || OB_ISNULL(parity_blocks) || (0 >= block_size) ||
      ((0 > block_index) || (block_index >= data_count))) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
        "arguments are invalid!",
        KP(data_block),
        KP(parity_blocks),
        K(data_count),
        K(parity_count),
        K(block_size),
        K(block_index),
        K(ret));
  } else if (OB_FAIL(
                 ObECCacheManager::get_ec_cache_mgr().get_encoding_table(data_count, parity_count, encode_gf_tables))) {
    LIB_LOG(WARN, "failed to get encoding table", K(data_count), K(parity_count), K(ret));
  } else {
    ec_encode_data_update((int)block_size,
        (int)data_count,
        (int)parity_count,
        (int)block_index,
        encode_gf_tables,
        data_block,
        parity_blocks);
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase*/