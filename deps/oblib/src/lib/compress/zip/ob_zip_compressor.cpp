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
#include "lib/compress/zip/zip.h"
#include "lib/compress/zip/ob_zip_compressor.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {
ObZipCompressor::ObZipCompressor() : zip_(nullptr)
{}
ObZipCompressor::~ObZipCompressor()
{
  if (NULL != zip_) {
    zip_stream_close(zip_, NULL, NULL);
  }
}

int ObZipCompressor::compress(const char* src_buffer, const int64_t src_data_size, char* dst_buffer,
    int64_t& dst_data_size, ObZipCompressFlag flag, const char* file_name)
{
  int ret = OB_SUCCESS;
  int zip_ret = 0;
  if (OB_ISNULL(src_buffer) || OB_ISNULL(dst_buffer)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "src_buffer or dst_buffer is nullptr", K(flag), K(ret));
  } else if (ObZipCompressFlag::FAKE_FILE_HEADER > flag || ObZipCompressFlag::TAIL < flag) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(ERROR, "invalid zip compress flag", K(flag), K(ret));
  } else {
    switch (flag) {
      case ObZipCompressFlag::FAKE_FILE_HEADER: {
        // get a fake file header and the real file name
        if (OB_ISNULL(file_name)) {
          ret = OB_INVALID_ARGUMENT;
          LIB_LOG(ERROR, "file name is nullptr", K(ret));
        } else if (OB_ISNULL(zip_ = zip_stream_open(NULL, 0, ZIP_DEFAULT_COMPRESSION_LEVEL, 'w'))) {
          ret = OB_INIT_FAIL;
          LIB_LOG(ERROR, "zip_ alloc failed", K(ret));
        } else if (0 != (zip_ret = zip_entry_open(
                             zip_, file_name, dst_buffer, reinterpret_cast<size_t*>(&dst_data_size)))) {
          ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
          LIB_LOG(ERROR, "zip compress: get fake file header failed", K(ret), K(zip_ret));
        }
        break;
      }
      case ObZipCompressFlag::DATA: {
        if (OB_ISNULL(zip_)) {
          ret = OB_NOT_INIT;
          LIB_LOG(ERROR, "zip_ is nullptr", K(ret));
        } else if (0 != (zip_ret = zip_entry_write(zip_,
                             src_buffer,
                             static_cast<size_t>(src_data_size),
                             dst_buffer,
                             reinterpret_cast<size_t*>(&dst_data_size)))) {
          ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
          LIB_LOG(ERROR, "zip compress: compress data failed", K(ret), K(zip_ret));
        }
        break;
      }
      case ObZipCompressFlag::LAST_DATA: {
        if (OB_ISNULL(zip_)) {
          ret = OB_NOT_INIT;
          LIB_LOG(ERROR, "zip_ is nullptr", K(ret));
        } else if (0 !=
                   (zip_ret = zip_entry_extra_write(zip_, dst_buffer, reinterpret_cast<size_t*>(&dst_data_size)))) {
          ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
          LIB_LOG(ERROR, "zip compress: compress last data failed", K(ret), K(zip_ret));
        }
        break;
      }
      case ObZipCompressFlag::FILE_HEADER: {
        // this real file header needs to be written in offset 0.
        if (OB_ISNULL(zip_)) {
          ret = OB_NOT_INIT;
          LIB_LOG(ERROR, "zip_ is nullptr", K(ret));
        } else if (0 != (zip_ret = zip_entry_close(zip_, dst_buffer, reinterpret_cast<size_t*>(&dst_data_size)))) {
          ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
          LIB_LOG(ERROR, "zip compress: get the real file header failed", K(ret), K(zip_ret));
        }
        break;
      }
      case ObZipCompressFlag::TAIL: {
        if (OB_ISNULL(zip_)) {
          ret = OB_NOT_INIT;
          LIB_LOG(ERROR, "zip_ is nullptr", K(ret));
        } else if (0 != (zip_ret = zip_stream_close(zip_, dst_buffer, reinterpret_cast<size_t*>(&dst_data_size)))) {
          ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
          LIB_LOG(ERROR, "zip compress: get the tail failed", K(ret), K(zip_ret));
        } else {
          zip_ = nullptr;
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LIB_LOG(ERROR, "invalid zip compress flag", K(flag), K(ret));
        break;
      }
    }
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
