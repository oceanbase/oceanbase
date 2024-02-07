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

#include "zlib_lite_adaptor.h"
#include "codec_deflate_qpl.h"
#include "zlib_lite_src/deflate.h"
#include "zlib_lite_src/zlib.h"

namespace oceanbase
{
namespace common
{
namespace ZLIB_LITE
{
/*zlib_lite supports two algorithms. On the platform that supports qpl, the qpl compression algorithm will be used,
otherwise the zlib algorithm will be used.*/

ObZlibLiteAdaptor::ObZlibLiteAdaptor()
{
}

ObZlibLiteAdaptor::~ObZlibLiteAdaptor()
{
}

int ObZlibLiteAdaptor::init(allocator alloc, deallocator dealloc, int32_t io_thread_count)
{
  int ret = 0;
#ifdef ENABLE_QPL_COMPRESSION

  QplAllocator allocator;
  allocator.allocate = alloc;
  allocator.deallocate = dealloc;
  int qpl_ret = qpl_init(allocator, io_thread_count);
  if (0 != qpl_ret) {
    ret = -1;
  }
#endif

  return ret;
}

void ObZlibLiteAdaptor::deinit()
{
#ifdef ENABLE_QPL_COMPRESSION
  qpl_deinit();
#endif
}

int ObZlibLiteAdaptor::zlib_compress(char *dest, int64_t *dest_len, const char *source, int64_t source_len)
{
  z_stream stream;
  int err = Z_OK;

  stream.next_in = (z_const Bytef *)source;
  stream.avail_in = (uInt)source_len;
#ifdef MAXSEG_64K
  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != source_len) return Z_BUF_ERROR;
#endif
  stream.next_out = (Bytef *)dest;
  stream.avail_out = (uInt)*dest_len;
  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;
  stream.opaque = (voidpf)0;

  if ((uLong)stream.avail_out != *dest_len) {
    err = Z_BUF_ERROR;
  } else if (Z_OK != (err = deflateInit2_(&stream, compress_level, Z_DEFLATED, window_bits, DEF_MEM_LEVEL,
                                          Z_DEFAULT_STRATEGY, ZLIB_VERSION, (int)sizeof(z_stream)))) {
    // LIB_LOG(WARN, "deflateInit2_ failed", K(err));
  } else if (Z_STREAM_END != (err = deflate(&stream, Z_FINISH))) {
    deflateEnd(&stream);
    err = (err == Z_OK ? Z_BUF_ERROR : err);
  } else {
    *dest_len = stream.total_out;
    err = deflateEnd(&stream);
  }
  return err;
}

int ObZlibLiteAdaptor::zlib_decompress(char *dest, int64_t *dest_len, const char *source, int64_t source_len)
{
  int ret = 0; // just for log
  z_stream stream;
  int err = Z_OK;

  stream.next_in = (z_const Bytef *)source;
  stream.avail_in = (uInt)source_len;
  stream.next_out = (Bytef *)dest;
  stream.avail_out = (uInt)*dest_len;
  stream.zalloc = (alloc_func)0;
  stream.zfree = (free_func)0;

  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != source_len || (uLong)stream.avail_out != *dest_len) {
    err = Z_BUF_ERROR;
  } else if (Z_OK != (err = inflateInit2_(&stream, window_bits, ZLIB_VERSION,
                                          (int)sizeof(z_stream)))) {
    // LIB_LOG(WARN, "inflateInit2_ failed", K(err));
  } else {
    err = inflate(&stream, Z_FINISH);
    if (err != Z_STREAM_END) {
      inflateEnd(&stream);
      if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0)) {
        err = Z_DATA_ERROR;
      }
    } else {
      *dest_len = stream.total_out;
      err = inflateEnd(&stream);
    }
  }
  return err;
}

int64_t ObZlibLiteAdaptor::compress(const char* src_buffer,
                                const int64_t src_data_size,
                                char* dst_buffer,
                                const int64_t dst_buffer_size)
{
#ifdef ENABLE_QPL_COMPRESSION

  return qpl_compress(src_buffer, dst_buffer, static_cast<int>(src_data_size),
                      static_cast<int>(dst_buffer_size));

#endif // ENABLE_QPL_COMPRESSION

  int zlib_errno = Z_OK;
  int64_t compress_ret_size = dst_buffer_size;
  zlib_errno = zlib_compress(dst_buffer, &compress_ret_size, src_buffer, src_data_size);
  if (Z_OK != zlib_errno) {
    // LIB_LOG(WARN, "Compress data by zlib in zlib_lite algorithm faild, ", K(ret), K(zlib_errno));
    return -1;
  }

  return compress_ret_size;
}

int64_t ObZlibLiteAdaptor::decompress(const char* src_buffer,
                                  const int64_t src_data_size,
                                  char* dst_buffer,
                                  const int64_t dst_buffer_size)
{
#ifdef ENABLE_QPL_COMPRESSION

  return qpl_decompress(src_buffer, dst_buffer,
                        static_cast<int>(src_data_size),
                        static_cast<int>(dst_buffer_size));
#endif

  int64_t decompress_ret_size = dst_buffer_size;
  int zlib_errno = Z_OK;
  zlib_errno = zlib_decompress(dst_buffer, &decompress_ret_size, src_buffer, src_data_size);
  if (Z_OK != zlib_errno) {
    // LIB_LOG(WARN, "Decompress data by zlib in zlib_lite algorithm faild, ",K(ret), K(zlib_errno));
    return -1;
  }
  return decompress_ret_size;
}

const char *ObZlibLiteAdaptor::compression_method() const
{
#ifdef ENABLE_QPL_COMPRESSION
  return qpl_hardware_enabled() ? "qpl_hardware" : "qpl_software";
#endif

  return "zlib_native";
}

} // namespace ZLIB_LITE
} // namespace common
} // namespace oceanbase
