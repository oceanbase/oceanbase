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

#ifndef OCEANBASE_COMMON_STREAM_COMPRESS_LZ4_COMPRESSOR_H_
#define OCEANBASE_COMMON_STREAM_COMPRESS_LZ4_COMPRESSOR_H_

#include "lib/compress/ob_stream_compressor.h"

namespace oceanbase
{
namespace common
{

class ObLZ4StreamCompressor : public ObStreamCompressor
{
public:
  //-----start of stream compress ralated interfaces//
  int create_compress_ctx(void *&ctx);
  int free_compress_ctx(void *ctx);
  int reset_compress_ctx(void *&ctx);
  int stream_compress(void *ctx, const char *src, const int64_t src_size,
                      char *dest, const int64_t dest_size, int64_t &compressed_size);

  int create_decompress_ctx(void *&ctx);
  int reset_decompress_ctx(void *&ctx);
  int free_decompress_ctx(void *ctx);
  int stream_decompress(void *ctx, const char *src, const int64_t src_size, char *dest,
                        const int64_t max_decompressed_size, int64_t &decompressed_size);

  int get_compress_bound_size(const int64_t src_data_size, int64_t &bound_size) const;
  int insert_uncompressed_block(void *dctx, const void *block, const int64_t block_size);

  const char *get_compressor_name() const;
  ObCompressorType get_compressor_type() const;
private:
  bool is_valid_original_data_length(int64_t origin_data_len) const;
};

class ObLZ4StreamCompressor191 : public ObStreamCompressor
{
public:
  //-----start of stream compress ralated interfaces//
  int create_compress_ctx(void *&ctx);
  int free_compress_ctx(void *ctx);
  int reset_compress_ctx(void *&ctx);
  int stream_compress(void *ctx, const char *src, const int64_t src_size,
                      char *dest, const int64_t dest_size, int64_t &compressed_size);

  int create_decompress_ctx(void *&ctx);
  int reset_decompress_ctx(void *&ctx);
  int free_decompress_ctx(void *ctx);
  int stream_decompress(void *ctx, const char *src, const int64_t src_size, char *dest,
                        const int64_t max_decompressed_size, int64_t &decompressed_size);

  int get_compress_bound_size(const int64_t src_data_size, int64_t &bound_size) const;
  int insert_uncompressed_block(void *dctx, const void *block, const int64_t block_size);

  const char *get_compressor_name() const;
  ObCompressorType get_compressor_type() const;
private:
  bool is_valid_original_data_length(int64_t origin_data_len) const;
};

}//namespace common
}//namespace oceanabse
#endif
