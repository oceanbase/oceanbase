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

#ifndef OB_COMPRESSOR_UTIL_H_
#define OB_COMPRESSOR_UTIL_H_
#include "lib/ob_define.h"
#include "lib/utility/ob_template_utils.h"
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace common
{
enum ObCompressorType : uint8_t
{
  INVALID_COMPRESSOR             = 0 ,
  NONE_COMPRESSOR                = 1,
  LZ4_COMPRESSOR                 = 2,
  SNAPPY_COMPRESSOR              = 3,
  ZLIB_COMPRESSOR                = 4,
  ZSTD_COMPRESSOR                = 5,
  ZSTD_1_3_8_COMPRESSOR          = 6,
  LZ4_191_COMPRESSOR             = 7,
  STREAM_LZ4_COMPRESSOR          = 8,//used for clog rpc compress
  STREAM_ZSTD_COMPRESSOR         = 9,//used for clog rpc compress
  STREAM_ZSTD_1_3_8_COMPRESSOR   = 10,//used for clog rpc compress
  ZLIB_LITE_COMPRESSOR           = 11,//Composed of qpl+zlib

  MAX_COMPRESSOR
};

const char *const all_compressor_name[] =
{
  "",
  "none",
  "lz4_1.0",
  "snappy_1.0",
  "zlib_1.0",
  "zstd_1.0",
  "zstd_1.3.8",
  "lz4_1.9.1",
  "stream_lz4_1.0",
  "stream_zstd_1.0",
  "stream_zstd_1.3.8",
  "zlib_lite_1.0",
};

STATIC_ASSERT(ARRAYSIZEOF(all_compressor_name) == ObCompressorType::MAX_COMPRESSOR, "compressor count mismatch");

#define DISABLED_ZLIB_1_COMPRESS_IDX 3

const char *const compress_funcs[] =
{
  "lz4_1.0",
  "none",
  "snappy_1.0",
  "zlib_1.0", // temporarily disable zlib_1.0
  "zstd_1.0",
  "zstd_1.3.8",
  "lz4_1.9.1",
  "zlib_lite_1.0",
};

const char *const perf_compress_funcs[] =
{
  "lz4_1.0",
  "zstd_1.0",
  "zstd_1.3.8",
};

const char *const syslog_compress_funcs[] =
{
  "none",
  "zstd_1.0",
  "zstd_1.3.8",
};

const char *const sql_temp_store_compress_funcs[] =
{
  "none",
  "zstd",
  "lz4",
  "snappy",
  "zlib"
};

} /* namespace common */
} /* namespace oceanbase */

using oceanbase::common::ObIAllocator;
static void *ob_zstd_malloc(void *opaque, size_t size)
{
  void *buf = NULL;
  if (NULL != opaque) {
    ObIAllocator *allocator = reinterpret_cast<ObIAllocator*> (opaque);
    buf = allocator->alloc(size);
  }
  return buf;
}

static void ob_zstd_free(void *opaque, void *address)
{
  if (NULL != opaque) {
    ObIAllocator *allocator = reinterpret_cast<ObIAllocator*> (opaque);
    allocator->free(address);
  }
}

static void *ob_zlib_alloc(void *opaque, unsigned int items, unsigned int size)
{
  void *ret = NULL;
  ObIAllocator *allocator = static_cast<ObIAllocator *>(opaque);
  if (OB_NOT_NULL(allocator)) {
    ret = allocator->alloc(items * size);
  }
  return ret;
}

static void ob_zlib_free(void *opaque, void *address)
{
  ObIAllocator *allocator = static_cast<ObIAllocator *>(opaque);
  if (OB_NOT_NULL(allocator)) {
    allocator->free(address);
  }
}

#endif /* OB_COMPRESSOR_UTIL_H_ */
