/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_PARQUET_BASIC_H
#define OB_PARQUET_BASIC_H

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <orc/OrcFile.hh>

#include "share/ob_device_manager.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/basic/ob_select_into_basic.h"
#include "sql/engine/table/ob_file_prefetch_buffer.h"
#include "sql/engine/table/ob_file_prebuffer.h"
#include "sql/engine/table/ob_external_file_access.h"

namespace oceanbase
{
namespace sql
{

class ObErrorCodeException : public std::runtime_error {
public:
  explicit ObErrorCodeException(const int error_code) :
    runtime_error("ObErrorCodeException"), error_code_(error_code) {}
  ~ObErrorCodeException() noexcept override {}
  ObErrorCodeException(const ObErrorCodeException& exception) : runtime_error(exception),
    error_code_(exception.error_code_) {}
  int get_error_code() const { return error_code_; }
private:
  ObErrorCodeException& operator=(const ObErrorCodeException&);
  int error_code_;
};

class ObOrcMemPool : public orc::MemoryPool {
public:
  void init(uint64_t tenant_id);

  virtual char* malloc(uint64_t size) override;

  virtual void free(char* p) override;

private:
  common::ObMemAttr mem_attr_;
};

class ObOrcOutputStream : public orc::OutputStream {
  public:
    ObOrcOutputStream(ObFileAppender *file_appender,
                      ObStorageAppender *storage_appender,
                      IntoFileLocation file_location,
                      const ObString &url)
    : file_appender_(file_appender),
      storage_appender_(storage_appender),
      file_location_(file_location),
      url_(url.ptr()),
      pos_(0)
    {}

    ~ObOrcOutputStream() {}

    virtual void write(const void *buf, size_t length) override;
    virtual uint64_t getLength() const { return pos_; }
    virtual uint64_t getNaturalWriteSize() const { return 128 * 1024; }

    virtual const std::string& getName() const override {
      return url_;
    }

    virtual void close() {
    }
  private:
    ObFileAppender *file_appender_;
    ObStorageAppender *storage_appender_;
    IntoFileLocation file_location_;
    std::string url_;
    int64_t pos_;
};

class ObArrowMemPool : public ::arrow::MemoryPool
{
public:
  ObArrowMemPool() : total_alloc_size_(0), total_hold_size_(0), num_allocations_(0) {}
  void init(uint64_t tenant_id);
  virtual arrow::Status Allocate(int64_t size, int64_t alignment, uint8_t** out) override;

  virtual arrow::Status Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                                   uint8_t **ptr) override;

  virtual void Free(uint8_t* buffer, int64_t size, int64_t alignment) override;

  virtual void ReleaseUnused() override;
  /// The number of bytes that were allocated.
  virtual int64_t total_bytes_allocated() const override;
  virtual int64_t num_allocations() const override;
  /// The number of bytes that were allocated and not yet free'd through
  /// this allocator.
  virtual int64_t bytes_allocated() const override;
  virtual int64_t max_memory() const override { return -1; }

  virtual std::string backend_name() const override { return "Arrow"; }
private:
  common::ObArenaAllocator alloc_;
  common::ObMemAttr mem_attr_;
  arrow::internal::MemoryPoolStats stats_;
  int64_t total_alloc_size_;
  int64_t total_hold_size_;
  int64_t num_allocations_;
};

class ObArrowFile : public arrow::io::RandomAccessFile {
public:
  ObArrowFile(ObExternalFileAccess &file_reader, const char *file_name, arrow::MemoryPool *pool) :
    file_reader_(file_reader), file_name_(file_name), pool_(pool), position_(0),
    timeout_ts_(INT64_MAX), file_prebuffer_(nullptr)
  {}
  ~ObArrowFile() override {
    file_reader_.close();
  }

  int open(const ObExternalFileUrlInfo &info, const ObExternalFileCacheOptions &cache_options);

  virtual arrow::Status Close() override;

  virtual bool closed() const override;

  virtual arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
  virtual arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes) override;

  virtual arrow::Status Seek(int64_t position) override;
  virtual arrow::Result<int64_t> Tell() const override;
  virtual arrow::Result<int64_t> GetSize() override;
  void set_file_prebuffer(ObFilePreBuffer *file_prebuffer);
  void set_timeout_timestamp(const int64_t timeout_ts);
  int read_from_cache(int64_t position, int64_t nbytes, void* out, bool &is_hit);

private:
  ObExternalFileAccess &file_reader_;
  const char* file_name_;
  arrow::MemoryPool *pool_;
  int64_t position_;
  int64_t timeout_ts_;
  ObFilePreBuffer *file_prebuffer_;
};

class ObParquetOutputStream : public arrow::io::OutputStream
{
public:
  ObParquetOutputStream (ObFileAppender *file_appender,
                         ObStorageAppender *storage_appender,
                         IntoFileLocation file_location,
                         const ObString &url)
    : file_appender_(file_appender),
      storage_appender_(storage_appender),
      file_location_(file_location),
      url_(url),
      position_(0)
    {}

  ~ObParquetOutputStream() override {}

  // Write methods
  // Virtual methods in `arrow::io::Writable`
  virtual arrow::Status Write(const void* data, int64_t nbytes) override;
  // virtual arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;
  // virtual arrow::Status Flush() override;

  // Virtual methods in `arrow::io::FileInterface`
  virtual arrow::Status Close() override;
  virtual bool closed() const override;
  virtual arrow::Result<int64_t> Tell() const override;

private:
  ObFileAppender *file_appender_;
  ObStorageAppender *storage_appender_;
  IntoFileLocation file_location_;
  const ObString &url_;
  int64_t position_;
};

class ObArrowUtil
{
public:
  static int get_location(const IntoFileLocation &file_location, const char *&location);
};

} // end of sql namespace
} // end of oceanbase namespace

#endif // OB_PARQUET_BASIC_H