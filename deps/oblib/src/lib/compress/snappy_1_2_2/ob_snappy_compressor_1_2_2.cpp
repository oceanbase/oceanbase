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

#include "lib/compress/snappy_1_2_2/ob_snappy_compressor_1_2_2.h"
#include <apache-arrow/snappy/snappy.h>
#include "lib/utility/ob_backtrace.h"
#include "lib/oblog/ob_log.h"
using oceanbase::common::lbt;

using namespace oceanbase;
using namespace common;
using namespace oceanbase::common::snappy_1_2_2;

int ObSnappyCompressor1_2_2::compress(const char *src_buffer,
                                       const int64_t src_data_size,
                                       char *dst_buffer,
                                       const int64_t dst_buffer_size,
                                       int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ",
        K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else {
    size_t compressed_size = 0;
    ::snappy_1_2_2::RawCompress(src_buffer,
                                static_cast<size_t>(src_data_size),
                                dst_buffer,
                                &compressed_size);
    dst_data_size = compressed_size;
  }
  return ret;
}

int ObSnappyCompressor1_2_2::decompress(const char *src_buffer,
                                        const int64_t src_data_size,
                                        char *dst_buffer,
                                        const int64_t dst_buffer_size,
                                        int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  size_t uncompressed_size = 0;
  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid decompress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (!::snappy_1_2_2::GetUncompressedLength(src_buffer, static_cast<size_t>(src_data_size), &uncompressed_size)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
  } else if (uncompressed_size > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (!::snappy_1_2_2::RawUncompress(src_buffer, static_cast<size_t>(src_data_size), dst_buffer)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
  } else {
    dst_data_size = uncompressed_size;
  }
  return ret;
}

const char *ObSnappyCompressor1_2_2::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::SNAPPY_1_2_2_COMPRESSOR];
}

ObCompressorType ObSnappyCompressor1_2_2::get_compressor_type() const
{
  return ObCompressorType::SNAPPY_1_2_2_COMPRESSOR;
}

int ObSnappyCompressor1_2_2::get_max_overflow_size(const int64_t src_data_size,
                                                   int64_t &max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = 32 + src_data_size / 6;
  }
  return ret;
}

int ObSnappyCompressor1_2_2::get_uncompressed_length(const char *compressed,
                                                     const int64_t compressed_length,
                                                     size_t &uncompressed_length)
{
  int ret = OB_SUCCESS;
  if (NULL == compressed || 0 >= compressed_length) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!::snappy_1_2_2::GetUncompressedLength(compressed, static_cast<size_t>(compressed_length), &uncompressed_length)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
  }
  return ret;
}