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

#include <new>
#include "lib/compress/none/ob_none_compressor.h"
#include "lib/ob_errno.h"

namespace oceanbase
{
namespace common
{
int ObNoneCompressor::compress(const char *src_buffer,
                               const int64_t src_data_size,
                               char *dst_buffer,
                               const int64_t dst_buffer_size,
                               int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;

  UNUSED(src_buffer);
  UNUSED(dst_buffer);
  UNUSED(dst_buffer_size);
  dst_data_size = src_data_size;

  return ret;
}

int ObNoneCompressor::decompress(const char *src_buffer,
                                 const int64_t src_data_size,
                                 char *dst_buffer,
                                 const int64_t dst_buffer_size,
                                 int64_t &dst_data_size)
{
  int ret = OB_SUCCESS;

  UNUSED(src_buffer);
  UNUSED(dst_buffer);
  UNUSED(dst_buffer_size);
  dst_data_size = src_data_size;

  return ret;
}

int ObNoneCompressor::get_max_overflow_size(const int64_t src_data_size,
                                            int64_t &max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = 0;
  }
  return ret;
}

const char *ObNoneCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::NONE_COMPRESSOR];
}

ObCompressorType ObNoneCompressor::get_compressor_type() const
{
  return ObCompressorType::NONE_COMPRESSOR;
}

}//namespace common
}//namespace oceanbase
