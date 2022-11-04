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

#include "lib/compress/snappy/ob_snappy_compressor.h"
#include "lib/compress/snappy/snappy_src/snappy.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
int ObSnappyCompressor::compress(const char *src_buffer,
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
    snappy::RawCompress(src_buffer, static_cast<size_t>(src_data_size), dst_buffer,
                        reinterpret_cast<size_t*>(&dst_data_size));
  }
  return ret;
}

int ObSnappyCompressor::decompress(const char *src_buffer,
                                   const int64_t src_data_size,
                                   char *dst_buffer,
                                   const int64_t dst_buffer_size,
                                   int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;
  bool flag = true;
  size_t decom_size = 0;
  if (NULL == src_buffer
      || 0 >= src_data_size
      || NULL == dst_buffer
      || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid compress argument, ",
        K(ret), KP(src_buffer), K(src_data_size), KP(dst_buffer), K(dst_buffer_size));
  } else if (!snappy::GetUncompressedLength(src_buffer,
                                            static_cast<size_t>(src_data_size),
                                            &decom_size)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to get uncompressed length, ",
        K(ret), KP(src_buffer), K(src_data_size));
  } else if (decom_size > static_cast<size_t>(dst_buffer_size)) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ",
        K(ret), K(decom_size), K(dst_buffer_size));
  } else if (!snappy::RawUncompress(src_buffer,
                                    static_cast<size_t>(src_data_size),
                                    dst_buffer)) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN, "fail to decompress data by snappy, ",
        K(ret), KP(src_buffer), K(src_data_size));
  } else {
    dst_data_size = decom_size;
  }

  return ret;
}

const char *ObSnappyCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::SNAPPY_COMPRESSOR];
}

ObCompressorType ObSnappyCompressor::get_compressor_type() const
{
  return ObCompressorType::SNAPPY_COMPRESSOR;
}

int ObSnappyCompressor::get_max_overflow_size(const int64_t src_data_size,
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
}//namespace common
}//namespace oceanbase
