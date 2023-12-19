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

#include "ob_zlib_lite_compressor.h"
#include "zlib_lite_adaptor.h"
#include "lib/ob_errno.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{
/*zlib_lite supports two algorithms. On the platform that supports qpl, the qpl compression algorithm will be used,
otherwise the zlib algorithm will be used.*/

ObZlibLiteCompressor::ObZlibLiteCompressor() : adaptor_(NULL)
{
}

ObZlibLiteCompressor::~ObZlibLiteCompressor()
{
}

void *qpl_allocate(int64_t size)
{
  return ob_malloc(size, ObModIds::OB_COMPRESSOR);
}

void qpl_deallocate(void *ptr)
{
  return ob_free(ptr);
}

int ObZlibLiteCompressor::init(int32_t io_thread_count)
{
  int ret = OB_SUCCESS;
  if (adaptor_ != NULL) {
    ret = OB_INIT_TWICE;
  } else if (FALSE_IT(adaptor_ = OB_NEW(ObZlibLiteAdaptor, ObModIds::OB_COMPRESSOR))) {
  } else if (NULL == adaptor_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LIB_LOG(WARN, "failed to create ObZlibLiteAdaptor");
  } else {
    ret = adaptor_->init(qpl_allocate, qpl_deallocate, io_thread_count);
  }

  LIB_LOG(INFO, "init zlib lite done.", K(ret), KCSTRING(adaptor_->compression_method()));
  return ret;
}

void ObZlibLiteCompressor::deinit()
{
  if (adaptor_ != NULL) {
    adaptor_->deinit();
    OB_DELETE(ObZlibLiteAdaptor, ObModIds::OB_COMPRESSOR, adaptor_);
    adaptor_ = NULL;
  }
}

int ObZlibLiteCompressor::compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  int64_t max_overflow_size = 0;
  if (NULL == adaptor_) {
    ret = OB_NOT_INIT;
    LIB_LOG(WARN, "zlib lite is not init");
  } else if (NULL == src_buffer || 0 >= src_data_size || NULL == dst_buffer || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
            "invalid compress argument, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            KP(dst_buffer),
            K(dst_buffer_size));
  } else if (OB_FAIL(get_max_overflow_size(src_data_size, max_overflow_size))) {
    LIB_LOG(WARN, "fail to get max_overflow_size, ", K(ret), K(src_data_size));
  } else if ((src_data_size + max_overflow_size) > dst_buffer_size) {
    ret = OB_BUF_NOT_ENOUGH;
    LIB_LOG(WARN, "dst buffer not enough, ", K(ret), K(src_data_size), K(max_overflow_size), K(dst_buffer_size));
  } else if (0 >= (dst_data_size = adaptor_->compress(src_buffer, src_data_size, dst_buffer, dst_buffer_size))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN,
            "fail to compress data by zlib_lite_compress, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            K(dst_buffer_size),
            K(dst_data_size));
  }

  return ret;
}

int ObZlibLiteCompressor::decompress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    const int64_t dst_buffer_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  int decompress_ret = 0;
  if (NULL == adaptor_) {
    ret = OB_NOT_INIT;
  } else if (NULL == src_buffer || 0 >= src_data_size || NULL == dst_buffer || 0 >= dst_buffer_size) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN,
            "invalid decompress argument, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            KP(dst_buffer),
            K(dst_buffer_size));
  } else if (0 >= (dst_data_size = adaptor_->decompress(src_buffer, src_data_size, dst_buffer, dst_buffer_size))) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LIB_LOG(WARN,
            "fail to decompress by zlib_lite_decompress, ",
            K(ret),
            KP(src_buffer),
            K(src_data_size),
            K(dst_buffer_size),
            K(dst_data_size));
  }

  return ret;
}

int ObZlibLiteCompressor::get_max_overflow_size(const int64_t src_data_size, int64_t& max_overflow_size) const
{
  int ret = OB_SUCCESS;
  if (src_data_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid argument, ", K(ret), K(src_data_size));
  } else {
    max_overflow_size = (src_data_size >> 12) + (src_data_size >> 14) + (src_data_size >> 25 ) + 13;
  }
  return ret;
}

const char* ObZlibLiteCompressor::get_compressor_name() const
{
  return all_compressor_name[ObCompressorType::ZLIB_LITE_COMPRESSOR];
}

ObCompressorType ObZlibLiteCompressor::get_compressor_type() const
{
  return ObCompressorType::ZLIB_LITE_COMPRESSOR;
}

const char* ObZlibLiteCompressor::compression_method() const
{
  const char *str = NULL;
  if (NULL == adaptor_) {
    str = "";
  } else {
    str = adaptor_->compression_method();
  }
  return str;
}
} // namespace ZLIB_LITE
} // namespace common
} // namespace oceanbase
