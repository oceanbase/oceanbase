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

#ifndef SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_BASIC_H_
#define SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_BASIC_H_


#include "common/storage/ob_io_device.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "lib/file/ob_file.h"

namespace oceanbase
{
namespace sql
{
using CsvCompressType = ObCSVGeneralFormat::ObCSVCompression;
enum class IntoFileLocation {
  SERVER_DISK,
  REMOTE_OSS,
  REMOTE_COS,
  REMOTE_S3,
  REMOTE_HDFS,
  REMOTE_UNKNOWN,
  REMOTE_AZBLOB
};
struct ObStorageAppender
{
  ObStorageAppender();
  virtual ~ObStorageAppender();
  void reset();

  int open(const common::ObObjectStorageInfo *storage_info,
           const common::ObString &uri,
           const common::ObStorageAccessType &access_type);
  int append(const char *buf, const int64_t size, int64_t &write_size);
  int close();

  bool is_opened_;
  int64_t offset_;
  ObIOFd fd_;
  ObIODevice *device_handle_;
  ObStorageAccessType access_type_;
};

class ObOutfileStreamCompressor
{
public:
  ObOutfileStreamCompressor(ObIAllocator &allocator)
      : allocator_(allocator) {}
  virtual ~ObOutfileStreamCompressor() { }
  virtual int compress(const char *src, int64_t src_size, size_t &consumed_size,
                       char *dest, int64_t dest_capacity, int64_t &decompressed_size,
                       bool is_file_end, bool &compress_ended) = 0;

  virtual int init() = 0;
  virtual void reset() {};
  virtual void reuse() {};
  static int create(CsvCompressType format, ObIAllocator &allocator, ObOutfileStreamCompressor *&compressor);
  static void destroy(ObOutfileStreamCompressor *compressor);
protected:
  ObIAllocator &allocator_;
};

class ObOutfileZstdStreamCompressor : public ObOutfileStreamCompressor
{
public:
  ObOutfileZstdStreamCompressor(ObIAllocator &allocator)
      : ObOutfileStreamCompressor(allocator), zstd_cctx_(NULL) {}
  ~ObOutfileZstdStreamCompressor() { reset(); }
  int compress(const char *src, int64_t src_size, size_t &consumed_size,
               char *dest, int64_t dest_capacity, int64_t &decompressed_size,
               bool is_file_end, bool &compress_ended) override;
  int init() override;
  void reset() override;
  void reuse() override;
private:
  void *zstd_cctx_;
};

class ObOutfileGzipStreamCompressor : public ObOutfileStreamCompressor
{
public:
  ObOutfileGzipStreamCompressor(ObIAllocator &allocator)
      : ObOutfileStreamCompressor(allocator), zstr_(), z_stream_ended_(true) {}
  ~ObOutfileGzipStreamCompressor() { reset(); }
  int compress(const char *src, int64_t src_size, size_t &consumed_size,
               char *dest, int64_t dest_capacity, int64_t &decompressed_size,
               bool is_file_end, bool &compress_ended) override;
  int init() override;
  void reset() override;
  void reuse() override;
private:
  z_stream zstr_;
  bool z_stream_ended_;
};

class ObCompressStreamWriter
{
public:
  ObCompressStreamWriter()
  : file_appender_(NULL),
    storage_appender_(NULL),
    file_location_(IntoFileLocation::SERVER_DISK),
    compress_type_(CsvCompressType::NONE),
    buf_(NULL),
    buf_len_(0),
    curr_pos_(0),
    total_compressed_bytes_(0),
    compressor_(NULL),
    allocator_(NULL),
    compress_stream_finished_(true)
  {}

  int init(ObFileAppender *file_appender,
           ObStorageAppender *storage_appender,
           IntoFileLocation file_location,
           CsvCompressType compress_type,
           ObIAllocator &allocator,
           int64_t buffer_size);

  void reuse();

  void reset();

  ~ObCompressStreamWriter() { reset(); }

  int write(const char *src, size_t length, bool is_file_end = false);

  int finish_file_compress();

  int64_t get_write_bytes() {return total_compressed_bytes_; }
private:

  int flush_to_storage(const char *src, size_t length);

  int finish_compress_stream();

private:
  ObFileAppender *file_appender_;
  ObStorageAppender *storage_appender_;
  IntoFileLocation file_location_;
  CsvCompressType compress_type_;
  char *buf_;
  int64_t buf_len_;
  // compressed position in buf_
  int64_t curr_pos_;
  int64_t total_compressed_bytes_;

  ObOutfileStreamCompressor *compressor_;
  ObIAllocator *allocator_;
  bool compress_stream_finished_;
  static const int64_t DEFAULT_COMPRESSED_BUFFER_SIZE;
  static const int64_t MIN_COMPRESSED_BUFFER_SIZE;
};

}
}
#endif /* SRC_SQL_ENGINE_BASIC_OB_SELECT_INTO_BASIC_H_ */
