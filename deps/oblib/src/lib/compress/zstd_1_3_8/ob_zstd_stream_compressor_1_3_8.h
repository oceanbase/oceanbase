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

#ifndef OCEANBASE_COMMON_STREAM_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
#define OCEANBASE_COMMON_STREAM_COMPRESS_ZSTD_1_3_8_COMPRESSOR_

#include "lib/compress/ob_stream_compressor.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace common
{
namespace zstd_1_3_8
{

class ObZstdStreamCtxAllocator
{
public:
  ObZstdStreamCtxAllocator();
  virtual ~ObZstdStreamCtxAllocator();
  static ObZstdStreamCtxAllocator &get_thread_local_instance()
  {
    thread_local ObZstdStreamCtxAllocator allocator;
    return allocator;
  }
  void *alloc(size_t size);
  void free(void *addr);
private:
  ModulePageAllocator allocator_;
};

class ObZstdStreamCompressor_1_3_8 : public ObStreamCompressor
{
public:
  explicit ObZstdStreamCompressor_1_3_8() {}
  virtual ~ObZstdStreamCompressor_1_3_8() {}

  inline const char *get_compressor_name() const;
  inline ObCompressorType get_compressor_type() const;

  int create_compress_ctx(void *&ctx);
  int reset_compress_ctx(void *&ctx);
  int free_compress_ctx(void *ctx);

  // a block is considered not compressible enough,  compressed_size will be zero
  int stream_compress(void *ctx, const char *src, const int64_t src_size, char *dest, const int64_t dest_capacity, int64_t &compressed_size);

  int create_decompress_ctx(void *&ctx);
  int reset_decompress_ctx(void *&ctx);
  int free_decompress_ctx(void *ctx);

  int stream_decompress(void *ctx, const char *src, const int64_t src_size, char *dest, const int64_t dest_capacity, int64_t &decompressed_size);

  int get_compress_bound_size(const int64_t src_size, int64_t &bound_size) const;
  int insert_uncompressed_block(void *dctx, const void *block, const int64_t block_size);

};
} // namespace zstd_1_3_8
} //namespace common
} //namespace oceanbase
#endif //OCEANBASE_COMMON_STREAM_COMPRESS_ZSTD_1_3_8_COMPRESSOR_
