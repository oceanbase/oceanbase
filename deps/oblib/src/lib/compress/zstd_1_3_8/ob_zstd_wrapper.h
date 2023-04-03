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

#ifndef DEPS_OBLIB_SRC_LIB_COMPRESS_ZSTD_OB_ZSTD_WRAPPER_H_
#define DEPS_OBLIB_SRC_LIB_COMPRESS_ZSTD_OB_ZSTD_WRAPPER_H_

#include <stddef.h>

namespace oceanbase
{
namespace common
{
namespace zstd_1_3_8
{

/*= Custom memory allocation functions */
typedef void* (*OB_ZSTD_allocFunction) (void* opaque, size_t size);
typedef void  (*OB_ZSTD_freeFunction) (void* opaque, void* address);
typedef struct { OB_ZSTD_allocFunction customAlloc; OB_ZSTD_freeFunction customFree; void* opaque; } OB_ZSTD_customMem;

#define OB_PUBLIC_API __attribute__ ((visibility ("default")))


class OB_PUBLIC_API ObZstdWrapper final
{
public:
  // for normal
  static int compress(
      OB_ZSTD_customMem &zstd_mem,
      const char *src_buffer,
      const size_t src_data_size,
      char *dst_buffer,
      const size_t dst_buffer_size,
      size_t &compress_ret_size);
  static int decompress(
      OB_ZSTD_customMem &zstd_mem,
      const char *src_buffer,
      const size_t src_data_size,
      char *dst_buffer,
      const size_t dst_buffer_size,
      size_t &dst_data_size);

  // for stream
  static int create_cctx(OB_ZSTD_customMem &ob_zstd_mem, void *&ctx);
  static void free_cctx(void *&ctx);
  static int compress_block(void *ctx, const char *src, const size_t src_size,
      char *dest, const size_t dest_capacity, size_t &compressed_size);
  static int create_dctx(OB_ZSTD_customMem &ob_zstd_mem, void *&ctx);
  static void free_dctx(void *&ctx);
  static int decompress_block(void *ctx, const char *src, const size_t src_size,
      char *dest, const size_t dest_capacity, size_t &decompressed_size);
  static size_t compress_bound(const size_t src_size);
  static int insert_block(void *ctx, const void *block, const size_t block_size);
};

#undef OB_PUBLIC_API

} // namespace zstd_1_3_8
} //namespace common
} //namespace oceanbase

#endif /* DEPS_OBLIB_SRC_LIB_COMPRESS_ZSTD_OB_ZSTD_WRAPPER_H_ */
