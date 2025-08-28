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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_select_into_basic.h"
#include "lib/compress/ob_compress_util.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/ob_device_manager.h"
#include "lib/compress/zstd_1_3_8/ob_zstd_wrapper.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

static constexpr uint64_t OB_STORAGE_ID_EXPORT = 2002;

ObStorageAppender::ObStorageAppender()
    : is_opened_(false),
      offset_(0),
      fd_(),
      device_handle_(nullptr),
      access_type_(OB_STORAGE_ACCESS_MAX_TYPE)
{}

ObStorageAppender::~ObStorageAppender()
{
  reset();
}

void ObStorageAppender::reset()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  if (is_opened_) {
    if (OB_FAIL(adapter.close_device_and_fd(device_handle_, fd_))) {
      LOG_WARN("fail to close device and fd", KR(ret), K_(fd), KP_(device_handle));
    }
  }
  offset_ = 0;
  fd_.reset();
  device_handle_ = nullptr;
  access_type_ = OB_STORAGE_ACCESS_MAX_TYPE;
  is_opened_ = false;
}

int ObStorageAppender::open(const share::ObBackupStorageInfo *storage_info,
    const ObString &uri, const ObStorageAccessType &access_type)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObStorageAppender has been opened",
        KR(ret), K_(is_opened), KPC(storage_info), K(uri), K(access_type));
  } else if (OB_UNLIKELY(access_type != OB_STORAGE_ACCESS_APPENDER
      && access_type != OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("access type is not supported",
        KR(ret), K_(is_opened), KPC(storage_info), K(uri), K(access_type));
  // The validity of the input parameters is verified by the open_with_access_type function.
  } else if (OB_FAIL(adapter.open_with_access_type(device_handle_, fd_,
      storage_info, uri, access_type,
      ObStorageIdMod(OB_STORAGE_ID_EXPORT, ObStorageUsedMod::STORAGE_USED_EXPORT)))) {
    LOG_WARN("fail to open appender", KR(ret), KPC(storage_info), K(uri), K(access_type));
  } else {
    offset_ = 0;
    access_type_ = access_type;
    is_opened_ = true;
  }
  return ret;
}

int ObStorageAppender::append(const char *buf, const int64_t size, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  ObBackupIoAdapter adapter;
  CONSUMER_GROUP_FUNC_GUARD(share::PRIO_EXPORT);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObStorageAppender not opened", KR(ret), K_(is_opened));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(size));
  } else if (size == 0) {
    // do nothing
  } else if (OB_STORAGE_ACCESS_APPENDER == access_type_) {
    if (OB_FAIL(adapter.pwrite(*device_handle_, fd_,
        buf, offset_, size, write_size, false/*is_can_seal*/))) {
      LOG_WARN("fail to append data",
          KR(ret), KP_(device_handle), K_(fd), KP(buf), K_(offset), K(size));
    } else if (OB_UNLIKELY(size != write_size)) {
      ret = OB_IO_ERROR;
      LOG_WARN("write size not equal to expected size",
          KR(ret), K_(fd), K_(offset), K(size), K(write_size));
    }
  } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type_) {
    ObIOHandle io_handle;
    // The BUFFERED_MULTIPART_WRITER writes to the buffer first during upload,
    // and only uploads when the buffer is full. 
    // Therefore, the return value of io_handle.get_data_size() may be 0
    // or the total size of the buffer during upload.
    if (OB_FAIL(adapter.async_upload_data(*device_handle_, fd_, buf, offset_, size, io_handle))) {
      LOG_WARN("fail to upload data",
          KR(ret), KP_(device_handle), K_(fd), KP(buf), K_(offset), K(size));
    } else if (OB_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait uploading data",
          KR(ret), KP_(device_handle), K_(fd), KP(buf), K_(offset), K(size));
    } else {
      write_size = io_handle.get_data_size();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access type is invalid, please check open status",
        KR(ret), KP_(device_handle), K_(fd), K_(access_type));
  }
  
  if (OB_SUCC(ret)) {
    offset_ += size;
  }
  return ret;
}

int ObStorageAppender::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;
  CONSUMER_GROUP_FUNC_GUARD(share::PRIO_EXPORT);
  // ignore error if not opened
  if (OB_LIKELY(is_opened_)) {
    if (OB_STORAGE_ACCESS_APPENDER == access_type_) {
      if (OB_FAIL(device_handle_->seal_file(fd_))) {
        LOG_WARN("fail to seal file",
            KR(ret), K_(fd), KP_(device_handle), K_(offset), K_(access_type));
      }
    } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type_) {
      if (OB_FAIL(adapter.complete(*device_handle_, fd_))) {
        LOG_WARN("fail to complete",
            KR(ret), K_(fd), KP_(device_handle), K_(offset), K_(access_type));
      }

      // if complete failed, need to abort
      if (OB_FAIL(ret)) {
        if (OB_TMP_FAIL(adapter.abort(*device_handle_, fd_))) {
          LOG_WARN("fail to abort",
              KR(ret), KR(tmp_ret), K_(fd), KP_(device_handle), K_(offset), K_(access_type));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("access type is invalid, please check open status",
          KR(ret), KP_(device_handle), K_(fd), K_(access_type));
    }
    
    if (OB_TMP_FAIL(adapter.close_device_and_fd(device_handle_, fd_))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to close device and fd", KR(ret), KR(tmp_ret), K_(fd), KP_(device_handle));
    }
  }
  offset_ = 0;
  fd_.reset();
  device_handle_ = nullptr;
  is_opened_ = false;
  return ret;
}

const int64_t ObCompressStreamWriter::DEFAULT_COMPRESSED_BUFFER_SIZE = 1 * 1024 * 1024; // 1MB
const int64_t ObCompressStreamWriter::MIN_COMPRESSED_BUFFER_SIZE = 4 * 1024; // 4KB

int ObCompressStreamWriter::write(const char *src, size_t length, bool is_file_end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compressor is null", KR(ret));
  } else {
    bool compress_ended = false;
    size_t consumed_size = 0;
    do {
      int64_t compressed_size = 0;
      if (OB_FAIL(compressor_->compress(src, length, consumed_size, buf_ + curr_pos_, buf_len_ - curr_pos_,
                                        compressed_size, is_file_end, compress_ended))) {
        LOG_WARN("failed to compress data", K(ret), K(length), K(consumed_size));
      } else {
        curr_pos_ += compressed_size;
        total_compressed_bytes_ += compressed_size;
        if (curr_pos_ >= buf_len_) {
          if (OB_FAIL(flush_to_storage(buf_, curr_pos_))) {
            LOG_WARN("failed to flush data to storage", K(ret), K(curr_pos_));
          } else {
            curr_pos_ = 0;
          }
        }
      }
    } while (OB_SUCC(ret) && !compress_ended);
  }
  return ret;
}

int ObCompressStreamWriter::flush_to_storage(const char *data, size_t length)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (OB_FAIL(file_appender_->append(data, length, false))) {
      LOG_WARN("failed to append file", K(ret), K(length));
    }
  } else if (OB_FAIL(storage_appender_->append(static_cast<const char*>(data), length, write_size))) {
    LOG_WARN("fail to append data", K(ret), KP(data), K(length));
  }
  return ret;
}

int ObCompressStreamWriter::finish_file_compress()
{
  int ret = OB_SUCCESS;
  if (compress_stream_finished_) {
  } else if (OB_FAIL(finish_compress_stream())) {
    LOG_WARN("failed to finish compress stream", K(ret));
  } else {
    if (curr_pos_ > 0) {
      if (OB_FAIL(flush_to_storage(buf_, curr_pos_))) {
        LOG_WARN("failed to flush buf to storage", K(ret), K(curr_pos_));
      }
    }
    if (OB_SUCC(ret)) {
      compress_stream_finished_ = true;
    }
  }
  return ret;
}

int ObCompressStreamWriter::finish_compress_stream()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(compressor_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compressor is null", KR(ret));
  } else if (OB_FAIL(write(NULL, 0, true))) {
    LOG_WARN("failed to finish compress", K(ret));
  }
  return ret;
}

void ObCompressStreamWriter::reuse()
{
  if (OB_NOT_NULL(compressor_)) {
    compressor_->reuse();
  }
  curr_pos_ = 0;
  total_compressed_bytes_ = 0;
  compress_stream_finished_ = false;
}
int ObCompressStreamWriter::init(ObFileAppender *file_appender,
                                 ObStorageAppender *storage_appender,
                                 IntoFileLocation file_location,
                                 CsvCompressType compress_type,
                                 ObIAllocator &allocator,
                                 int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  file_appender_ = file_appender;
  storage_appender_ = storage_appender;
  file_location_ = file_location;
  compress_type_ = compress_type;
  allocator_ = &allocator;
  compress_stream_finished_ = false;
  // buffer_size used for stream compression is limited to between MIN_COMPRESSED_BUFFER_SIZE
  // and DEFAULT_COMPRESSED_BUFFER_SIZE, which ensures streaming compression works properly
  // and avoiding the memory usage of too many partitions when export outfile by partition.
  if (buffer_size > DEFAULT_COMPRESSED_BUFFER_SIZE) {
    buf_len_ = DEFAULT_COMPRESSED_BUFFER_SIZE;
  } else if (buffer_size < MIN_COMPRESSED_BUFFER_SIZE) {
    buf_len_ = MIN_COMPRESSED_BUFFER_SIZE;
  } else {
    buf_len_ = buffer_size;
  }
  if (compress_type_ == CsvCompressType::NONE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compress type", K(ret), K(compress_type_));
  } else {
    if (OB_FAIL(ObOutfileStreamCompressor::create(compress_type_, *allocator_, compressor_))) {
      LOG_WARN("failed to create decompressor", K(compress_type_), K(ret));
    } else if (OB_ISNULL(buf_ = (char *)allocator_->alloc(buf_len_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buffer.", K(buf_len_));
    }
  }
  return ret;
}

void ObCompressStreamWriter::reset()
{
  if (compressor_ != NULL) {
    compressor_->reset();
    ObOutfileStreamCompressor::destroy(compressor_);
    compressor_ = NULL;
  }
  if (buf_ != NULL) {
    allocator_->free(buf_);
    buf_ = NULL;
  }
  compress_stream_finished_ = false;
}

int ObOutfileStreamCompressor::create(CsvCompressType format, ObIAllocator &allocator, ObOutfileStreamCompressor *&compressor)
{
  int ret = OB_SUCCESS;
  compressor = NULL;

  switch (format) {
    case CsvCompressType::NONE: {
      ret = OB_INVALID_ARGUMENT;
    } break;

    case CsvCompressType::GZIP:
    case CsvCompressType::DEFLATE: {
      compressor = OB_NEW(ObOutfileGzipStreamCompressor, ObMemAttr(MTL_ID(), "ExportWriter"), allocator);
    } break;

    case CsvCompressType::ZSTD: {
      compressor = OB_NEW(ObOutfileZstdStreamCompressor, ObMemAttr(MTL_ID(), "ExportWriter"), allocator);
    } break;

    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported compression format", K(format));
    } break;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(compressor)) {
    if (OB_FAIL(compressor->init())) {
      LOG_WARN("failed to init decompressor", KR(ret));
      ObOutfileStreamCompressor::destroy(compressor);
      compressor = NULL;
    }
  }

  return ret;
}

void ObOutfileStreamCompressor::destroy(ObOutfileStreamCompressor *compressor)
{
  if (OB_NOT_NULL(compressor)) {
    compressor->reset();
    OB_DELETE(ObOutfileStreamCompressor, ObMemAttr(MTL_ID(), "ExportWriter"), compressor);
  }
}

using OB_ZSTD_customMem = oceanbase::common::zstd_1_3_8::OB_ZSTD_customMem;
using ObZstdWrapper = oceanbase::common::zstd_1_3_8::ObZstdWrapper;
int ObOutfileZstdStreamCompressor::init()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(zstd_cctx_)) {
    ret = OB_INIT_TWICE;
  } else {
    OB_ZSTD_customMem allocator;
    allocator.customAlloc = ob_zstd_malloc;
    allocator.customFree  = ob_zstd_free;
    allocator.opaque      = &allocator_;

    ret = ObZstdWrapper::create_cctx(allocator, zstd_cctx_);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to create zstd compress context", K(ret));
    }
  }

  return ret;
}

void ObOutfileZstdStreamCompressor::reset()
{
  if (OB_NOT_NULL(zstd_cctx_)) {
    ObZstdWrapper::free_cctx(zstd_cctx_);
    zstd_cctx_ = NULL;
  }
}

void ObOutfileZstdStreamCompressor::reuse()
{
  ObZstdWrapper::reset_cctx(zstd_cctx_, 1/*ZSTD_reset_session_only*/);
}

int ObOutfileZstdStreamCompressor::compress(const char *src,
                                            int64_t src_size,
                                            size_t &consumed_size,
                                            char *dest,
                                            int64_t dest_capacity,
                                            int64_t &compressed_size,
                                            bool is_file_end,
                                            bool &compress_ended)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(zstd_cctx_)) {
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(dest)
             || dest_capacity <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(dest), K(dest_capacity));
  } else {
    size_t tmp_compressed_size = 0;
    ret = ObZstdWrapper::compress_stream(zstd_cctx_,
                                         src, src_size, consumed_size,
                                         dest, dest_capacity, tmp_compressed_size, is_file_end, compress_ended);
    compressed_size = static_cast<int64_t>(tmp_compressed_size);
  }
  return ret;
}

int ObOutfileGzipStreamCompressor::init()
{
  int ret = OB_SUCCESS;
  zstr_.zalloc = ob_zlib_alloc;
  zstr_.zfree = ob_zlib_free;
  zstr_.opaque = static_cast<voidpf>(&allocator_);
  int zlib_ret = deflateInit2(&zstr_, 5/* compression level */, Z_DEFLATED,
                              15 + 16/* for gzip */, 8/* memLevel default value */,
                              Z_DEFAULT_STRATEGY);
  if (Z_OK != zlib_ret) {
    ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
    LOG_WARN("zlib failed to deflateInit2", K(zlib_ret));
  } else {
    z_stream_ended_ = false;
  }
  return ret;
}

void ObOutfileGzipStreamCompressor::reset()
{
  if (!z_stream_ended_) {
    deflateEnd(&zstr_);
    z_stream_ended_ = true;
  }
}

void ObOutfileGzipStreamCompressor::reuse()
{
  deflateReset(&zstr_);
}

int ObOutfileGzipStreamCompressor::compress(const char *src,
                                            int64_t src_size,
                                            size_t &consumed_size,
                                            char *dest,
                                            int64_t dest_capacity,
                                            int64_t &compressed_size,
                                            bool is_file_end,
                                            bool &compress_ended)
{
  int ret = OB_SUCCESS;
  if (NULL == dest
      || 0 >= dest_capacity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compress argument, ",
        K(ret), KP(dest), K(dest_capacity));
  } else {
    zstr_.next_in = (Bytef *)(src + consumed_size);
    zstr_.avail_in = src_size - consumed_size;
    zstr_.avail_out = dest_capacity;
    zstr_.next_out = (Bytef *)dest;
    int flush_flag = is_file_end ? Z_FINISH : Z_NO_FLUSH;
    int zlib_ret = deflate(&zstr_, flush_flag);
    if (zlib_ret == Z_STREAM_ERROR) {
      ret = OB_ERR_COMPRESS_DECOMPRESS_DATA;
      LOG_WARN("failed to deflate", KP(src), K(src_size), K(dest_capacity));
    }
    if (OB_SUCC(ret)) {
      compressed_size = dest_capacity - zstr_.avail_out;
      consumed_size = zstr_.next_in - (Bytef *)src;
      compress_ended = is_file_end ? (zlib_ret == Z_STREAM_END) : (zstr_.avail_in == 0 && zstr_.avail_out > 0);
    }
  }
  return ret;
}

}
}