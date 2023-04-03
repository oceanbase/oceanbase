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

#ifndef  OCEANBASE_COMMON_STREAM_COMPRESS_OB_COMPRESSOR_H_
#define  OCEANBASE_COMMON_STREAM_COMPRESS_OB_COMPRESSOR_H_

#include <stdint.h>
#include "lib/string/ob_string.h"
#include "lib/compress/ob_compress_util.h"
namespace oceanbase
{
namespace common
{
class ObStreamCompressor
{
public:
  static const char *none_compressor_name;
public:
  ObStreamCompressor() {}
  virtual ~ObStreamCompressor() {}

  virtual int create_compress_ctx(void *&ctx) = 0;
  virtual int reset_compress_ctx(void *&ctx) = 0;
  virtual int free_compress_ctx(void *ctx) = 0;
  virtual int stream_compress(void *ctx,
                              const char *src_buffer,
                              const int64_t src_data_size,
                              char *dst_buffer,
                              const int64_t dst_buffer_size,
                              int64_t &dst_data_size) = 0;

  virtual int create_decompress_ctx(void *&ctx) = 0;
  virtual int reset_decompress_ctx(void *&ctx) = 0;
  virtual int free_decompress_ctx(void *ctx) = 0;
  virtual int stream_decompress(void *ctx,
                                const char *src_buffer,
                                const int64_t src_data_size,
                                char *dst_buffer,
                                const int64_t dst_buffer_size,
                                int64_t &dst_data_size) = 0;

  virtual int insert_uncompressed_block(void *dctx, const void *block, const int64_t block_size) = 0;
  virtual int get_compress_bound_size(const int64_t src_data_size, int64_t &bound_size) const = 0;

  virtual const char *get_compressor_name() const = 0;
  virtual ObCompressorType get_compressor_type() const = 0;
};

}//namespace common
}//namespace oceanbase

#endif
