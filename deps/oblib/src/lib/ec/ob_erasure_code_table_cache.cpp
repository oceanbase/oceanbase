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

#include <mutex>
#include <isa-l/erasure_code.h>
#include "lib/container/ob_se_array.h"
#include "lib/ec/ob_erasure_code_table_cache.h"

namespace oceanbase {
namespace common {

ObECTableCache::ObECTableCache()
    : is_inited_(false), encoding_table_map_(), encoding_matrix_map_(), decoding_table_map_(), decoding_matrix_map_()
{}

ObECTableCache::~ObECTableCache()
{
  destroy();
}

int ObECTableCache::init(const int64_t bucket_size)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "ec cache has been inited!", K(ret));
  } else if (bucket_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bucket_size is invalid!", K(bucket_size), K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(encoding_table_map_.create(bucket_size, ObModIds::OB_EC_CALCULATE_BUFFER))) {
      LIB_LOG(WARN, "create encoding table map fail", K(ret));
    } else if (OB_FAIL(encoding_matrix_map_.create(bucket_size, ObModIds::OB_EC_CALCULATE_BUFFER))) {
      LIB_LOG(WARN, "create encoding matrix map fail", K(ret));
    } else if (OB_FAIL(decoding_table_map_.create(bucket_size, ObModIds::OB_EC_CALCULATE_BUFFER))) {
      LIB_LOG(WARN, "create decoding table map fail", K(ret));
    } else if (OB_FAIL(decoding_matrix_map_.create(bucket_size, ObModIds::OB_EC_CALCULATE_BUFFER))) {
      LIB_LOG(WARN, "create decoding matrix map fail", K(ret));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObECTableCache::destroy()
{
  encoding_table_map_.destroy();
  encoding_matrix_map_.destroy();
  decoding_table_map_.destroy();
  decoding_matrix_map_.destroy();

  is_inited_ = false;
}

int ObECTableCache::get_encoding_table(
    const int64_t data_count, const int64_t parity_count, bool& table_exist_flag, unsigned char*& encoding_table)
{
  INIT_SUCC(ret);
  table_exist_flag = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeEncodeKey ec_key(data_count, parity_count);
    if (OB_FAIL(encoding_table_map_.get_refactored(ec_key, encoding_table))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      table_exist_flag = true;
    }
  }

  return ret;
}

int ObECTableCache::set_encoding_table(const int64_t data_count, const int64_t parity_count, unsigned char* ec_in_table)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeEncodeKey ec_key(data_count, parity_count);
    ret = encoding_table_map_.set_refactored(ec_key, ec_in_table);
    if (OB_SUCCESS != ret) {
      if (OB_HASH_EXIST == ret) {
        LIB_LOG(WARN, "ec table insert repeatedly", K(data_count), K(parity_count), K(ret));
      }
    }
  }

  return ret;
}

int ObECTableCache::get_encoding_matrix(
    const int64_t data_count, const int64_t parity_count, bool& matrix_exist_flag, unsigned char*& encoding_matrix)
{
  INIT_SUCC(ret);
  matrix_exist_flag = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeEncodeKey ec_key(data_count, parity_count);
    if (OB_FAIL(encoding_matrix_map_.get_refactored(ec_key, encoding_matrix))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      matrix_exist_flag = true;
    }
  }

  return ret;
}

int ObECTableCache::set_encoding_matrix(const int64_t data_count, const int64_t parity_count, unsigned char* ec_matrix)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeEncodeKey ec_key(data_count, parity_count);
    ret = encoding_matrix_map_.set_refactored(ec_key, ec_matrix);
    if (OB_SUCCESS != ret) {
      if (OB_HASH_EXIST == ret) {
        LIB_LOG(WARN, "ec matrix insert repeatedly", K(data_count), K(parity_count), K(ret));
      }
    }
  }

  return ret;
}

int ObECTableCache::get_decoding_table(const ObString& signature, const int64_t data_count, const int64_t parity_count,
    bool& table_exist_flag, unsigned char*& decoding_table)
{
  INIT_SUCC(ret);
  table_exist_flag = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0 || signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(signature), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeDecodeKey ec_key(data_count, parity_count, signature);
    if (OB_FAIL(decoding_table_map_.get_refactored(ec_key, decoding_table))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      table_exist_flag = true;
    }
  }

  return ret;
}

int ObECTableCache::set_decoding_table(
    const ObString& signature, const int64_t data_count, const int64_t parity_count, unsigned char* decoding_table)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0 || signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(signature), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeDecodeKey ec_key(data_count, parity_count, signature);
    ret = decoding_table_map_.set_refactored(ec_key, decoding_table);
    if (OB_SUCCESS != ret) {
      if (OB_HASH_EXIST == ret) {
        LIB_LOG(WARN, "ec decoding table insert repeatedly", K(data_count), K(parity_count), K(ret));
      }
    }
  }

  return ret;
}

int ObECTableCache::get_decoding_matrix(const ObString& signature, const int64_t data_count, const int64_t parity_count,
    bool& matrix_exist_flag, unsigned char*& decoding_matrix)
{
  INIT_SUCC(ret);
  matrix_exist_flag = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0 || signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(signature), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeDecodeKey ec_key(data_count, parity_count, signature);
    if (OB_FAIL(decoding_matrix_map_.get_refactored(ec_key, decoding_matrix))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    } else {
      matrix_exist_flag = true;
    }
  }

  return ret;
}

int ObECTableCache::set_decoding_matrix(
    const ObString& signature, const int64_t data_count, const int64_t parity_count, unsigned char* decoding_matrix)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0 || signature.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(signature), K(ret));
  }

  if (OB_SUCC(ret)) {
    ObErasureCodeDecodeKey ec_key(data_count, parity_count, signature);
    ret = decoding_matrix_map_.set_refactored(ec_key, decoding_matrix);
    if (OB_SUCCESS != ret) {
      if (OB_HASH_EXIST == ret) {
        LIB_LOG(WARN, "ec decoding matrix insert repeatedly", K(data_count), K(parity_count), K(ret));
      }
    }
  }

  return ret;
}

/*-------------- ObECCacheManager implement --------------------*/

// static
ObECCacheManager& ObECCacheManager::get_ec_cache_mgr()
{
  static ObECCacheManager ec_cache_mgr;
  return ec_cache_mgr;
}

int ObECCacheManager::init(const int64_t bucket_size)
{
  INIT_SUCC(ret);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LIB_LOG(WARN, "ec cache manager has been inited!", K(ret));
  } else if (bucket_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "bucket size is invalid!", K(bucket_size), K(ret));
  } else if (OB_FAIL(cache_.init(bucket_size))) {
    LIB_LOG(WARN, "failed to init cache!", K(bucket_size), K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

void ObECCacheManager::destroy()
{
  is_inited_ = false;

  {
    lib::ObMutexGuard guard(table_allocator_mutex_);
    table_allocator_.clear();
  }

  {
    lib::ObMutexGuard guard(signature_allocator_mutex_);
    signature_allocator_.clear();
  }

  cache_.destroy();
}

int ObECCacheManager::get_encoding_table(
    const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_table)
{
  INIT_SUCC(ret);
  bool exist_flag = false;
  unsigned char* encoding_matrix = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  } else if (OB_FAIL(cache_.get_encoding_table(data_count, parity_count, exist_flag, encoding_table))) {
    LIB_LOG(WARN, "error occur when getting ec encoding table,", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret) && !exist_flag) {
    LIB_LOG(INFO, "don't hit ec table in cache , ", K(data_count), K(parity_count));

    if (OB_FAIL(get_encoding_matrix(data_count, parity_count, encoding_matrix))) {
      LIB_LOG(WARN, "error occur when getting encoding ec matrix,", K(data_count), K(parity_count), K(ret));
    } else if (OB_FAIL(alloc_table_mem(data_count, parity_count, encoding_table))) {
      LIB_LOG(WARN, "failed to alloc ec table memory!", K(data_count), K(parity_count), K(ret));
    } else {
      ec_init_tables((int)data_count, (int)parity_count, &encoding_matrix[data_count * data_count], encoding_table);

      if (OB_FAIL(cache_.set_encoding_table(data_count, parity_count, encoding_table))) {
        if (ret == OB_HASH_EXIST) {  // maybe another thread put one into cache
          ret = OB_SUCCESS;
          free_encoding_table_or_matrix_mem(encoding_table);
          if (OB_FAIL(cache_.get_encoding_table(data_count, parity_count, exist_flag, encoding_table))) {
            LIB_LOG(WARN, "error occur when getting ec encoding table,", K(data_count), K(parity_count), K(ret));
          } else if (!exist_flag) {
            ret = OB_ERR_UNEXPECTED;
            LIB_LOG(
                WARN, "unexpected error, ec encoding table should be exist,", K(data_count), K(parity_count), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObECCacheManager::set_encoding_table(
    const int64_t data_count, const int64_t parity_count, unsigned char* ec_in_table)
{
  return cache_.set_encoding_table(data_count, parity_count, ec_in_table);
}

int ObECCacheManager::get_encoding_matrix(
    const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_matrix)
{
  INIT_SUCC(ret);
  bool exist_flag = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  } else if (OB_FAIL(cache_.get_encoding_matrix(data_count, parity_count, exist_flag, encoding_matrix))) {
    LIB_LOG(WARN, "error occur when getting ec encoding matrix,", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret) && !exist_flag) {
    LIB_LOG(INFO, "don't hit ec matrix in cache, ", K(data_count), K(parity_count));
    if (OB_FAIL(alloc_matrix_mem(data_count, parity_count, encoding_matrix))) {
      LIB_LOG(WARN, "failed to alloc ec matrix memory!", K(data_count), K(parity_count), K(ret));
    } else {
      gf_gen_cauchy1_matrix(encoding_matrix, (int)(data_count + parity_count), (int)data_count);
      if (OB_FAIL(cache_.set_encoding_matrix(data_count, parity_count, encoding_matrix))) {
        if (ret == OB_HASH_EXIST) {  // maybe another thread put one into cache
          ret = OB_SUCCESS;          // reset to OB_SUCCESS
          free_encoding_table_or_matrix_mem(encoding_matrix);
          if (OB_FAIL(cache_.get_encoding_matrix(data_count, parity_count, exist_flag, encoding_matrix))) {
            LIB_LOG(WARN, "error occur when getting ec encoding matrix,", K(data_count), K(parity_count), K(ret));
          } else if (!exist_flag) {
            ret = OB_ERR_UNEXPECTED;
            LIB_LOG(WARN, "unexpected error, encoding matrix should be exist,", K(data_count), K(parity_count), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObECCacheManager::set_encoding_matrix(
    const int64_t data_count, const int64_t parity_count, unsigned char* ec_in_table)
{
  return cache_.set_encoding_matrix(data_count, parity_count, ec_in_table);
}

int ObECCacheManager::get_decoding_matrix(const common::ObIArray<int64_t>& erasure_indexes,
    const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
    unsigned char*& decoding_matrix)
{
  INIT_SUCC(ret);
  ObString erasure_signature;
  bool matrix_exist_flag = false;
  unsigned char* encode_matrix = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  } else if (erasure_indexes.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "erasure indexes is empty!", K(ret));
  } else if (erasure_indexes.count() > parity_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
        "erasure indexes count is more than parity count!",
        "erasure count",
        erasure_indexes.count(),
        K(parity_count),
        K(ret));
  } else if (ready_indexes.count() < data_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
        "ready indexes count is less than data count!",
        "ready count",
        ready_indexes.count(),
        K(data_count),
        K(ret));
  } else if (OB_FAIL(get_erasure_code_signature(erasure_indexes, ready_indexes, erasure_signature))) {
    LIB_LOG(WARN, "failed to get ec signature", K(erasure_indexes), K(ready_indexes), K(ret));
  } else if (OB_FAIL(cache_.get_decoding_matrix(
                 erasure_signature, data_count, parity_count, matrix_exist_flag, decoding_matrix))) {
    LIB_LOG(WARN, "failed to get ec decoding matrix", K(erasure_signature), K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret) && !matrix_exist_flag) {
    common::ObSEArray<int64_t, OB_MAX_EC_STRIPE_COUNT> decode_indexes;

    // if ready_indexes.count() is more than **data_count**, just get the first **data_count** indexes
    for (int i = 0; OB_SUCC(ret) && i < data_count; i++) {
      ret = decode_indexes.push_back(ready_indexes.at(i));
    }

    if (OB_FAIL(ret)) {
      LIB_LOG(WARN, "error occur when pushing element into array!", K(ret));
    } else if (OB_FAIL(alloc_matrix_mem(data_count, parity_count, decoding_matrix))) {
      LIB_LOG(WARN, "failed to alloc ec matrix memory!", K(data_count), K(parity_count), K(ret));
    } else if (OB_FAIL(get_encoding_matrix(data_count, parity_count, encode_matrix))) {
      LIB_LOG(WARN, "error occur when getting ec encoding matrix!", K(data_count), K(parity_count), K(ret));
    } else if (OB_FAIL(gen_decode_matrix(
                   encode_matrix, decode_indexes, erasure_indexes, data_count, parity_count, decoding_matrix))) {
      LIB_LOG(WARN,
          "failed to gen ec decode matrix!",
          K(data_count),
          K(parity_count),
          K(decode_indexes),
          K(erasure_indexes),
          K(ret));
    } else if (OB_FAIL(cache_.set_decoding_matrix(erasure_signature, data_count, parity_count, decoding_matrix))) {
      if (ret == OB_HASH_EXIST) {  // maybe another thread put one into cache
        ret = OB_SUCCESS;
        free_encoding_table_or_matrix_mem(decoding_matrix);
        if (OB_FAIL(cache_.get_decoding_matrix(
                erasure_signature, data_count, parity_count, matrix_exist_flag, decoding_matrix))) {
          LIB_LOG(
              WARN, "failed to get ec decoding matrix", K(erasure_signature), K(data_count), K(parity_count), K(ret));
        } else if (!matrix_exist_flag) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }
  }

  return ret;
}

int ObECCacheManager::set_decoding_matrix(const common::ObIArray<int64_t>& erasure_indexes,
    const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
    unsigned char* decoding_matrix)
{
  INIT_SUCC(ret);
  ObString erasure_signature;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  } else if (erasure_indexes.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "erasure indexes is empty!", K(ret));
  } else if (OB_FAIL(get_erasure_code_signature(erasure_indexes, ready_indexes, erasure_signature))) {
    LIB_LOG(WARN, "failed to get ec signature!", K(erasure_indexes), K(ready_indexes), K(ret));
  } else if (OB_FAIL(cache_.set_decoding_matrix(erasure_signature, data_count, parity_count, decoding_matrix))) {
    LIB_LOG(WARN, "failed to insert ec decoding matrix!", K(erasure_signature), K(data_count), K(parity_count), K(ret));
  }

  return ret;
}

int ObECCacheManager::get_decoding_table(const common::ObIArray<int64_t>& erasure_indexes,
    const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
    unsigned char*& decoding_table)
{
  INIT_SUCC(ret);
  ObString erasure_signature;
  bool table_exist_flag = false;
  unsigned char* decode_matrix = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  } else if (erasure_indexes.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "erasure indexes is empty!", K(ret));
  } else if (OB_FAIL(get_erasure_code_signature(erasure_indexes, ready_indexes, erasure_signature))) {
    LIB_LOG(WARN, "failed to get ec signature!", K(erasure_indexes), K(ready_indexes), K(ret));
  } else if (OB_FAIL(cache_.get_decoding_table(
                 erasure_signature, data_count, parity_count, table_exist_flag, decoding_table))) {
    LIB_LOG(WARN, "failed to get ec decoding table!", K(erasure_signature), K(data_count), K(parity_count), K(ret));
  } else if (!table_exist_flag) {
    LIB_LOG(INFO, "don't hit decode ec table in cache, ", K(data_count), K(parity_count), K(erasure_signature));

    if (OB_FAIL(get_decoding_matrix(erasure_indexes, ready_indexes, data_count, parity_count, decode_matrix))) {
      LIB_LOG(WARN,
          "failed to get ec decoding matrix!",
          K(erasure_indexes),
          K(ready_indexes),
          K(data_count),
          K(parity_count),
          K(ret));
    } else if (OB_FAIL(alloc_table_mem(data_count, parity_count, decoding_table))) {
      LIB_LOG(WARN, "failed to alloc ec table memory!", K(data_count), K(parity_count), K(ret));
    } else {
      ec_init_tables((int)data_count, (int)erasure_indexes.count(), decode_matrix, decoding_table);

      if (OB_FAIL(cache_.set_decoding_table(erasure_signature, data_count, parity_count, decoding_table))) {
        if (ret == OB_HASH_EXIST) {  // maybe another thread put one intocache
          ret = OB_SUCCESS;
          free_encoding_table_or_matrix_mem(decoding_table);
          if (OB_FAIL(cache_.get_decoding_table(
                  erasure_signature, data_count, parity_count, table_exist_flag, decoding_table))) {
            LIB_LOG(
                WARN, "failed to get ec decoding table!", K(erasure_signature), K(data_count), K(parity_count), K(ret));
          } else if (!table_exist_flag) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
  }

  return ret;
}

int ObECCacheManager::set_decoding_table(const common::ObIArray<int64_t>& erasure_indexes,
    const common::ObIArray<int64_t>& ready_indexes, const int64_t data_count, const int64_t parity_count,
    unsigned char* decoding_table)
{
  INIT_SUCC(ret);
  ObString erasure_signature;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  } else if (erasure_indexes.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "erasure indexes is empty!", K(ret));
  } else if (OB_FAIL(get_erasure_code_signature(erasure_indexes, ready_indexes, erasure_signature))) {
    LIB_LOG(WARN, "failed to get ec signature!", K(erasure_indexes), K(ready_indexes), K(ret));
  } else if (OB_FAIL(cache_.set_decoding_table(erasure_signature, data_count, parity_count, decoding_table))) {
    LIB_LOG(WARN, "failed to insert ec decoding table!", K(erasure_signature), K(data_count), K(parity_count), K(ret));
  }

  return ret;
}

ObECCacheManager::ObECCacheManager()
    : is_inited_(false),
      cache_(),
      table_allocator_(ObModIds::OB_EC_CALCULATE_BUFFER),
      table_allocator_mutex_(),
      signature_allocator_(ObModIds::OB_EC_CALCULATE_BUFFER),
      signature_allocator_mutex_()
{}

ObECCacheManager::~ObECCacheManager()
{
  destroy();
}

int ObECCacheManager::alloc_table_mem(
    const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_table)
{
  INIT_SUCC(ret);
  int64_t all_count = data_count + parity_count;
  int64_t matrix_size = data_count * all_count;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(table_allocator_mutex_);
    if (nullptr == (encoding_table = (unsigned char*)table_allocator_.alloc(matrix_size * 32))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LIB_LOG(WARN, "failed to alloc ec encoding table memory!", "alloc size", matrix_size * 32, K(ret));
    } else {
      MEMSET(encoding_table, 0, (size_t)matrix_size * 32);
    }
  }

  return ret;
}

int ObECCacheManager::alloc_matrix_mem(
    const int64_t data_count, const int64_t parity_count, unsigned char*& encoding_matrix)
{
  int ret = OB_SUCCESS;

  int64_t all_count = data_count + parity_count;
  int64_t matrix_size = data_count * all_count;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "ec cache manager has not been inited!", K(ret));
  } else if (data_count <= 0 || parity_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "arguments are invalid!", K(data_count), K(parity_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(table_allocator_mutex_);
    if (nullptr == (encoding_matrix = (unsigned char*)table_allocator_.alloc(matrix_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(encoding_matrix, 0, (size_t)matrix_size);
    }
  }

  return ret;
}

void ObECCacheManager::free_encoding_table_or_matrix_mem(unsigned char* ptr)
{
  lib::ObMutexGuard guard(table_allocator_mutex_);
  table_allocator_.free(ptr);
  ptr = nullptr;
}

int ObECCacheManager::get_erasure_code_signature(const common::ObIArray<int64_t>& erasure_indexes,
    const common::ObIArray<int64_t>& ready_indexes, ObString& signature)
{
  INIT_SUCC(ret);
  int64_t pos = 0;
  int64_t buf_length = OB_MAX_EC_STRIPE_COUNT * 8;
  char buf[buf_length];

  MEMSET(buf, 0, sizeof(buf));

  if (erasure_indexes.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "erasure indexes is empty", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < erasure_indexes.count(); i++) {
    if (erasure_indexes.at(i) >= OB_MAX_EC_STRIPE_COUNT) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid erasure index", K(i), K(erasure_indexes.at(i)), K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_length, pos, "-%ld", erasure_indexes.at(i)))) {
      LIB_LOG(WARN, "failed to databuf_printf", K(i), K(erasure_indexes.at(i)), K(ret));
    }
  }  // end for

  for (int64_t i = 0; OB_SUCC(ret) && i < ready_indexes.count(); i++) {
    if (ready_indexes.at(i) >= OB_MAX_EC_STRIPE_COUNT) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "invalid ready index", K(i), K(ready_indexes.at(i)), K(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_length, pos, "+%ld", ready_indexes.at(i)))) {
      LIB_LOG(WARN, "failed to databuf_printf", K(i), K(ready_indexes.at(i)), K(ret));
    }
  }  // end for

  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(ob_write_string(signature_allocator_, ObString(pos, buf), signature))) {
    LIB_LOG(WARN, "failed to write string", K(ret));
  }

  return ret;
}

bool ObECCacheManager::erasure_contains(const ObIArray<int64_t>& erasures, int64_t index)
{
  bool contain_flag = false;

  int64_t count = erasures.count();
  for (int i = 0; i < count; i++) {
    if (erasures.at(i) == index) {
      contain_flag = true;
      break;
    }
  }

  return contain_flag;
}

int ObECCacheManager::gen_decode_matrix(const unsigned char* encode_matrix, const ObIArray<int64_t>& decode_indexes,
    const ObIArray<int64_t>& erase_indexes, const int64_t data_block_count, const int64_t parity_block_count,
    unsigned char* decode_matrix)
{
  INIT_SUCC(ret);

  int64_t all_block_count = data_block_count + parity_block_count;
  unsigned char tmp_matrix[data_block_count * all_block_count];
  unsigned char invert_matrix[data_block_count * all_block_count];
  int64_t nerrs = erase_indexes.count();

  MEMSET(tmp_matrix, 0, sizeof(tmp_matrix));
  MEMSET(invert_matrix, 0, sizeof(invert_matrix));

  if (nullptr == encode_matrix || nullptr == decode_matrix) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "encode_matrix or decode_matrix is null", KP(encode_matrix), KP(decode_matrix));
  } else if (data_block_count <= 0 || all_block_count <= 0 || data_block_count > all_block_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "error block count", K(data_block_count), K(all_block_count), K(ret));
  } else if (nerrs > parity_block_count) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "nerrs count is more than parity count", K(nerrs), K(parity_block_count), K(ret));
  }

  if (OB_SUCC(ret)) {
    // Construct matrix tmp_matrix
    for (int i = 0; i < data_block_count; i++) {
      for (int j = 0; j < data_block_count; j++) {
        tmp_matrix[data_block_count * i + j] = encode_matrix[data_block_count * decode_indexes.at(i) + j];
      }
    }
  }

  if (OB_SUCC(ret)) {
    // get invert matirx(data_block_count* data_block_count)
    if (gf_invert_matrix(tmp_matrix, invert_matrix, (int)data_block_count) < 0) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "failed to get ec invert matrix", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // get decode matrix
    for (int p = 0; p < nerrs; p++) {
      if (erase_indexes.at(p) < data_block_count) {
        // decoding matrix elements for data blocks
        for (int j = 0; j < data_block_count; j++) {
          decode_matrix[data_block_count * p + j] = invert_matrix[data_block_count * erase_indexes.at(p) + j];
        }
      } else {
        // decoding matrix element for parity blocks
        for (int i = 0; i < data_block_count; i++) {
          unsigned char s = 0;
          for (int j = 0; j < data_block_count; j++) {
            s = s ^ gf_mul(invert_matrix[j * data_block_count + i],
                        encode_matrix[data_block_count * erase_indexes.at(p) + j]);
          }

          decode_matrix[data_block_count * p + i] = s;
        }
      }
    }
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase
