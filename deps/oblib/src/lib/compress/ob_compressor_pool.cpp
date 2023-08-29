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

#include "lib/compress/ob_compressor_pool.h"

namespace oceanbase
{
namespace common
{
ObCompressorPool::ObCompressorPool()
    :none_compressor(),
     lz4_compressor(),
     lz4_compressor_1_9_1(),
     snappy_compressor(),
     zlib_compressor(),
     zstd_compressor_1_3_8(),
     lz4_stream_compressor(),
     zstd_stream_compressor(),
     zstd_stream_compressor_1_3_8()
{
}
ObCompressorPool &ObCompressorPool::get_instance()
{
  static ObCompressorPool instance_;
  return instance_;
}

int ObCompressorPool::get_compressor(const char *compressor_name,
                                     ObCompressor *&compressor)
{
  int ret = OB_SUCCESS;
  ObCompressorType compressor_type = INVALID_COMPRESSOR;

  if (NULL == compressor_name) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compressor name argument, ", K(ret), KP(compressor_name));
  } else if (OB_FAIL(get_compressor_type(compressor_name, compressor_type))) {
    LIB_LOG(WARN, "fail to get compressor type, ", K(ret), KCSTRING(compressor_name));
  } else if (OB_FAIL(get_compressor(compressor_type, compressor))) {
    LIB_LOG(WARN, "fail to get compressor", K(ret), K(compressor_type));
  }
  return ret;
}

int ObCompressorPool::get_compressor(const ObCompressorType &compressor_type,
                                     ObCompressor *&compressor)
{
  int ret = OB_SUCCESS;
  switch(compressor_type) {
    case NONE_COMPRESSOR:
      compressor = &none_compressor;
      break;
    case LZ4_191_COMPRESSOR:
      compressor = &lz4_compressor_1_9_1;
      break;
    case LZ4_COMPRESSOR:
      compressor = &lz4_compressor;
      break;
    case SNAPPY_COMPRESSOR:
      compressor = &snappy_compressor;
      break;
    case ZLIB_COMPRESSOR:
      compressor = &zlib_compressor;
      break;
    case ZSTD_COMPRESSOR:
      compressor = &zstd_compressor;
      break;
    case ZSTD_1_3_8_COMPRESSOR:
      compressor = &zstd_compressor_1_3_8;
      break;
    default:
      compressor = NULL;
      ret = OB_NOT_SUPPORTED;
      LIB_LOG(WARN, "not support compress type, ", K(ret), K(compressor_type));
  }
  return ret;
}

int ObCompressorPool::get_compressor_type(const char *compressor_name,
                                          ObCompressorType &compressor_type) const
{
  int ret = OB_SUCCESS;
  if (NULL == compressor_name) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compressor name argument, ", K(ret), KP(compressor_name));
  } else if (!STRCASECMP(compressor_name, "none")) {
    compressor_type = NONE_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "lz4_1.0")) {
    compressor_type = LZ4_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "snappy_1.0")) {
    compressor_type = SNAPPY_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "zlib_1.0")) {
    compressor_type = ZLIB_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "zstd_1.0")) {
    compressor_type = ZSTD_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "zstd_1.3.8")) {
    compressor_type = ZSTD_1_3_8_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "lz4_1.9.1")) {
    compressor_type = LZ4_191_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "stream_lz4_1.0")) {
    compressor_type = STREAM_LZ4_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "stream_zstd_1.0")) {
    compressor_type = STREAM_ZSTD_COMPRESSOR;
  } else if (!STRCASECMP(compressor_name, "stream_zstd_1.3.8")) {
    compressor_type = STREAM_ZSTD_1_3_8_COMPRESSOR;
  } else {
    ret = OB_NOT_SUPPORTED;
    LIB_LOG(WARN, "no support compressor type, ", K(ret), KCSTRING(compressor_name));
  }
  return ret;
}

int ObCompressorPool::get_compressor_type(const ObString &compressor_name,
                                          ObCompressorType &compressor_type) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compressor_name.ptr()) || compressor_name.length() == 0) {
    compressor_type = ObCompressorType::NONE_COMPRESSOR;
  } else {
    int64_t len = compressor_name.length() + 1;
    char comp_name[len];
    MEMCPY(comp_name, compressor_name.ptr(), len - 1);
    comp_name[len - 1] = '\0';
    if (OB_FAIL(get_compressor_type(comp_name, compressor_type))) {
      LIB_LOG(ERROR, "no support compressor name", K(ret), K(compressor_name));
    }
  }
  return ret;
}

int ObCompressorPool::get_stream_compressor(const char *compressor_name,
                                            ObStreamCompressor *&stream_compressor)
{
  int ret = OB_SUCCESS;
  ObCompressorType compressor_type = INVALID_COMPRESSOR;

  if (OB_ISNULL(compressor_name)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compressor name argument, ", K(ret), KP(compressor_name));
  } else if (OB_FAIL(get_compressor_type(compressor_name, compressor_type))) {
    LIB_LOG(WARN, "fail to get compressor type, ", K(ret), KCSTRING(compressor_name));
  } else if (OB_FAIL(get_stream_compressor(compressor_type, stream_compressor))) {
    LIB_LOG(WARN, "fail to get stream compressor", K(ret), K(compressor_type));
  } else {/*do nothing*/}
  return ret;
}

int ObCompressorPool::get_stream_compressor(const ObCompressorType &compressor_type,
                                            ObStreamCompressor *&stream_compressor)
{
  int ret = OB_SUCCESS;
  switch(compressor_type) {
    case STREAM_LZ4_COMPRESSOR:
      stream_compressor = &lz4_stream_compressor;
      break;
    case STREAM_ZSTD_COMPRESSOR:
      stream_compressor = &zstd_stream_compressor;
      break;
    case STREAM_ZSTD_1_3_8_COMPRESSOR:
      stream_compressor = &zstd_stream_compressor_1_3_8;
      break;
    default:
      stream_compressor = NULL;
      ret = OB_NOT_SUPPORTED;
      LIB_LOG(WARN, "not support compress type for stream compress", K(ret), K(compressor_type));
  }
  return ret;
}

int ObCompressorPool::get_max_overflow_size(const int64_t src_data_size, int64_t &max_overflow_size)
{
  int ret = OB_SUCCESS;
  int64_t lz4_overflow_size = 0;
  int64_t lz4_191_overflow_size = 0;
  int64_t snappy_overflow_size = 0;
  int64_t zlib_overflow_size = 0;
  int64_t zstd_overflow_size = 0;
  int64_t zstd_138_overflow_size = 0;
  if (OB_FAIL(lz4_compressor.get_max_overflow_size(src_data_size, lz4_overflow_size))) {
      LIB_LOG(WARN, "failed to get_max_overflow_size of lz4", K(ret), K(src_data_size));
  } else if (OB_FAIL(lz4_compressor_1_9_1.get_max_overflow_size(src_data_size, lz4_191_overflow_size))) {
      LIB_LOG(WARN, "failed to get_max_overflow_size of lz4", K(ret), K(src_data_size));
  } else if (OB_FAIL(snappy_compressor.get_max_overflow_size(src_data_size, snappy_overflow_size))) {
      LIB_LOG(WARN, "failed to get_max_overflow_size of snappy", K(ret), K(src_data_size));
  } else if (OB_FAIL(zlib_compressor.get_max_overflow_size(src_data_size, zlib_overflow_size))) {
      LIB_LOG(WARN, "failed to get_max_overflow_size of zlib", K(ret), K(src_data_size));
  } else if (OB_FAIL(zstd_compressor.get_max_overflow_size(src_data_size, zstd_overflow_size))) {
      LIB_LOG(WARN, "failed to get_max_overflow_size of zstd", K(ret), K(src_data_size));
  } else if (OB_FAIL(zstd_compressor_1_3_8.get_max_overflow_size(src_data_size, zstd_138_overflow_size))) {
      LIB_LOG(WARN, "failed to get_max_overflow_size of zstd_138", K(ret), K(src_data_size));
  } else {
    max_overflow_size = std::max(lz4_overflow_size, lz4_191_overflow_size);
    max_overflow_size = std::max(max_overflow_size, snappy_overflow_size);
    max_overflow_size = std::max(max_overflow_size, zlib_overflow_size);
    max_overflow_size = std::max(max_overflow_size, zstd_overflow_size);
    max_overflow_size = std::max(max_overflow_size, zstd_138_overflow_size);
  }
  return ret;
}
} /* namespace common */
} /* namespace oceanbase */
