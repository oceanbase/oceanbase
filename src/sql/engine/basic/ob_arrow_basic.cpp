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
#define USING_LOG_PREFIX SQL_ENG

#include "ob_arrow_basic.h"
#include <parquet/api/reader.h>


namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql {

void ObOrcMemPool::init(uint64_t tenant_id)
{
  mem_attr_ = ObMemAttr(tenant_id, "OrcMemPool");
}

char* ObOrcMemPool::malloc(uint64_t size)
{
  int ret = OB_SUCCESS;
  void *buf = ob_malloc_align(64, size, mem_attr_);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(size), K(lbt()));
    throw std::bad_alloc();
  }
  return (char*)buf;
}

void ObOrcMemPool::free(char* p)
{
  if (OB_ISNULL(p)) {
    throw std::bad_exception();
  }
  ob_free_align(p);
}

void ObOrcOutputStream::write(const void *buf, size_t length)
{
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (OB_FAIL(file_appender_->append(buf, length, false))) {
      LOG_WARN("failed to append file", K(ret), K(length), K(url_.c_str()));
      throw std::runtime_error("failed to append file");
    }
  } else {
    const char *location = nullptr;
    if (OB_FAIL(ObArrowUtil::get_location(file_location_, location)) || OB_ISNULL(location)) {
      LOG_WARN("failed to get location string", K(ret), K_(file_location));
      throw std::runtime_error("failed to get location string");
    } else {
      int64_t write_size = 0;
      int64_t begin_ts = ObTimeUtility::current_time();
      if (OB_FAIL(storage_appender_->append(static_cast<const char *>(buf), length, write_size))) {
        LOG_WARN("fail to append data", K(ret), KP(buf), K(length), K(url_.c_str()));
        throw std::runtime_error("fail to append data");
      } else {
        pos_ += length;
        int64_t end_ts = ObTimeUtility::current_time();
        int64_t cost_time = end_ts - begin_ts;
        long double speed = (cost_time <= 0) ? 0 : (long double)write_size * 1000.0 * 1000.0 / 1024.0 / 1024.0 / cost_time;
        long double total_write = (long double)pos_ / 1024.0 / 1024.0;
        _OB_LOG(TRACE, "write %s stat, time:%ld write_size:%ld speed:%.2Lf MB/s total_write:%.2Lf MB",
                location, cost_time, write_size, speed, total_write);
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos_ += length;
  }
}

/* ObArrowMemPool */
void ObArrowMemPool::init(uint64_t tenant_id)
{
  mem_attr_ = ObMemAttr(tenant_id, "ArrowMemPool");
}

arrow::Status ObArrowMemPool::Allocate(int64_t size, uint8_t** out)
{
  int ret = OB_SUCCESS;
  arrow::Status status_ret = arrow::Status::OK();
  if (0 == size) {
    *out = NULL;
  } else {
    void *buf = ob_malloc_align(64, size, mem_attr_);
    if (OB_ISNULL(buf)) {
      status_ret = arrow::Status::Invalid("allocate memory failed");
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(size), K(lbt()));
    } else {
      *out = static_cast<uint8_t*>(buf);
      total_alloc_size_ += size;
    }
  }
  LOG_DEBUG("ObArrowMemPool::Allocate", K(size), "stack", lbt());
  return status_ret;
}

arrow::Status ObArrowMemPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr)
{
  int ret = OB_SUCCESS;
  uint8_t* old = *ptr;
  arrow::Status status_ret = Allocate(new_size, ptr);
  if (arrow::Status::OK() == status_ret) {
    MEMCPY(*ptr, old, std::min(old_size, new_size));
    Free(old, old_size);
  }
  LOG_DEBUG("ObArrowMemPool::Reallocate", K(old_size), K(new_size), "stack", lbt());
  return status_ret;
}

void ObArrowMemPool::Free(uint8_t* buffer, int64_t size) {
  int ret = OB_SUCCESS;
  ob_free_align(buffer);
  total_alloc_size_ -= size;
  LOG_DEBUG("ObArrowMemPool::Free", K(size), "stack", lbt());
}

void ObArrowMemPool::ReleaseUnused() {
  LOG_DEBUG("ObArrowMemPool::ReleaseUnused", "stack", lbt());
}

int64_t ObArrowMemPool::bytes_allocated() const {
  LOG_DEBUG("ObArrowMemPool::bytes_allocated", "stack", lbt());
  return total_alloc_size_;
}

/* ObArrowFile */
int ObArrowFile::open(const ObExternalFileUrlInfo &info, const ObExternalFileCacheOptions &cache_options)
{
  return file_reader_.open(info, cache_options);
}

arrow::Status ObArrowFile::Seek(int64_t position) {
  position_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> ObArrowFile::Read(int64_t nbytes, void *out)
{
  int ret = OB_SUCCESS;
  bool is_hit_cache = false;
  arrow::Result<int64_t> ret_code;
  int64_t read_size = -1;
  const int64_t io_timeout_ms = (timeout_ts_ - ObTimeUtility::current_time()) / 1000;
  ObExternalReadInfo read_info(position_, out, nbytes, io_timeout_ms);
  if (OB_FAIL(read_from_cache(position_, nbytes, out, is_hit_cache))) {
    LOG_WARN("failed to read from cache", K(ret));
  } else if (is_hit_cache) {
    position_ += nbytes;
    ret_code = nbytes;
  } else if (OB_FAIL(file_reader_.pread(read_info, read_size))) {
    LOG_WARN("fail to read file", K(ret), K(nbytes));
    throw ObErrorCodeException(ret);
  } else {
    position_ += read_size;
    ret_code = read_size;
  }
  LOG_DEBUG("Read(int64_t nbytes, void *out)", K(nbytes));
  return ret_code;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ObArrowFile::Read(int64_t nbytes)
{
  ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, pool_));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
  }
  LOG_DEBUG("ObArrowFile::Read(int64_t nbytes)", K(nbytes));
  return std::move(buffer);
}


arrow::Result<int64_t> ObArrowFile::ReadAt(int64_t position, int64_t nbytes, void* out)
{
  int ret = OB_SUCCESS;
  bool is_hit_cache = false;
  arrow::Result<int64_t> ret_code;
  int64_t read_size = -1;
  const int64_t io_timeout_ms = (timeout_ts_ - ObTimeUtility::current_time()) / 1000;
  ObExternalReadInfo read_info(position, out, nbytes, io_timeout_ms);
  if (OB_FAIL(read_from_cache(position, nbytes, out, is_hit_cache))) {
    LOG_WARN("failed to read from cache", K(ret));
  } else if (is_hit_cache) {
    position_ += nbytes;
    ret_code = nbytes;
  } else if (OB_FAIL(file_reader_.pread(read_info, read_size))) {
    LOG_WARN("fail to read file", K(ret), K(position), K(nbytes));
    throw ObErrorCodeException(ret);
  } else {
    position_ = position + read_size;
    ret_code = read_size;
  }
  LOG_DEBUG("ObArrowFile::Read(int64_t nbytes)", K(nbytes));
  return ret_code;
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ObArrowFile::ReadAt(int64_t position, int64_t nbytes)
{
  ARROW_ASSIGN_OR_RAISE(auto buffer, AllocateResizableBuffer(nbytes, pool_));
  ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                        ReadAt(position, nbytes, buffer->mutable_data()));
  if (bytes_read < nbytes) {
    RETURN_NOT_OK(buffer->Resize(bytes_read));
    buffer->ZeroPadding();
  }
  LOG_DEBUG("ObArrowFile::ReadAt(int64_t position, int64_t nbytes)", K(nbytes));
  return std::move(buffer);
}


arrow::Result<int64_t> ObArrowFile::Tell() const
{
  return position_;
}

arrow::Result<int64_t> ObArrowFile::GetSize()
{
  int ret = OB_SUCCESS;
  arrow::Result<int64_t> ret_code;
  int64_t file_size = 0;
  if (OB_FAIL(file_reader_.get_file_size(file_size))) {
    LOG_WARN("fail to get file size", K(ret), K(file_name_));
    ret_code = arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, "get file size"));
  } else {
    ret_code = file_size;
  }
  return ret_code;
}

arrow::Status ObArrowFile::Close()
{
  file_reader_.close();
  return arrow::Status::OK();
}

bool ObArrowFile::closed() const
{
  return !file_reader_.is_opened();
}

void ObArrowFile::set_file_prebuffer(ObFilePreBuffer *file_prebuffer)
{
  if (nullptr != file_prebuffer) {
    file_prebuffer->reset();
  }
  file_prebuffer_ = file_prebuffer;
}

int ObArrowFile::read_from_cache(int64_t position, int64_t nbytes, void* out, bool &is_hit)
{
  int ret = OB_SUCCESS;
  is_hit = false;
  if (nullptr == file_prebuffer_) {
  } else if (OB_FAIL(file_prebuffer_->read(position, nbytes, out))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to read from prebuffer", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else {
    is_hit = true;
  }
  return ret;
}

void ObArrowFile::set_timeout_timestamp(const int64_t timeout_ts)
{
  timeout_ts_ = timeout_ts;
}


/*ObParquetOutputStream*/

arrow::Status ObParquetOutputStream::Write(const void* data, int64_t nbytes)
{
  arrow::Status status = arrow::Status::OK();
  int ret = OB_SUCCESS;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (OB_FAIL(file_appender_->append(data, nbytes, false))) {
      LOG_WARN("failed to append file", K(ret), K(nbytes), K(url_));
      status = arrow::Status(arrow::StatusCode::IOError, "write file failed");
    } else {
      position_ += nbytes;
    }
  } else {
    const char *location = nullptr;
    if (OB_FAIL(ObArrowUtil::get_location(file_location_, location))) {
      LOG_WARN("failed to get location string", K(ret), K_(file_location));
    } else if (OB_ISNULL(location)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid location string", K(ret));
    } else {
      int64_t write_size = 0;
      int64_t begin_ts = ObTimeUtility::current_time();
      if (OB_FAIL(storage_appender_->append(static_cast<const char *>(data),
                                            nbytes, write_size))) {
        LOG_WARN("fail to append data", K(ret), KP(data), K(nbytes), K(url_));
        status = arrow::Status(arrow::StatusCode::IOError, "write file failed");
      } else {
        position_ += nbytes;
        int64_t end_ts = ObTimeUtility::current_time();
        int64_t cost_time = end_ts - begin_ts;
        long double speed = (cost_time <= 0) ? 0 :
                        (long double) write_size * 1000.0 * 1000.0 / 1024.0 / 1024.0 / cost_time;
        long double total_write = (long double) position_ / 1024.0 / 1024.0;
        _OB_LOG(TRACE, "write %s stat, time:%ld write_size:%ld speed:%.2Lf MB/s total_write:%.2Lf MB", location,
                cost_time, write_size, speed, total_write);
      }
    }
  }
  return status;
}

// Virtual methods in `arrow::io::FileInterface`
arrow::Status ObParquetOutputStream::Close()
{
  arrow::Status status = arrow::Status::OK();
  int ret = OB_SUCCESS;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    file_appender_->close();
  } else if (OB_FAIL(storage_appender_->close())) {
    LOG_WARN("failed to close storage appender", K(ret));
    status = arrow::Status(arrow::StatusCode::IOError, "close file failed");
  }
  return status;
}

bool ObParquetOutputStream::closed() const
{
  bool ret = false;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    ret = !file_appender_->is_opened();
  } else {
    ret = !storage_appender_->is_opened_;
  }
  return ret;
}

arrow::Result<int64_t> ObParquetOutputStream::Tell() const
{
  return position_;
}
/*------------------------- ObArrowUtil -------------------------*/
int ObArrowUtil::get_location(const IntoFileLocation &file_location, const char *&location)
{
  int ret = OB_SUCCESS;
  switch (file_location) {
    case IntoFileLocation::SERVER_DISK: {
      location = "server";
      break;
    }
    case IntoFileLocation::REMOTE_OSS: {
      location = "oss";
      break;
    }
    case IntoFileLocation::REMOTE_COS: {
      location = "cos";
      break;
    }
    case IntoFileLocation::REMOTE_S3: {
      location = "s3";
      break;
    }
    case IntoFileLocation::REMOTE_HDFS: {
      location = "hdfs";
      break;
    }
    case IntoFileLocation::REMOTE_AZBLOB: {
      location = "azblob";
      break;
    }
    case IntoFileLocation::REMOTE_UNKNOWN:
    default: {
      ret = OB_INVALID_BACKUP_DEST;
      LOG_WARN("invalid destination", K(ret), K(file_location));
    }
  }
  return ret;
}

} // end of oceanbase namespace
} // end of oceanbase namespace