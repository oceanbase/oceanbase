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
  } else if (OB_FAIL(storage_appender_->append(static_cast<const char*>(buf), length, write_size))) {
    LOG_WARN("fail to append data", K(ret), KP(buf), K(length), K(url_.c_str()));
    throw std::runtime_error("fail to append data");
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
int ObArrowFile::open()
{
  return file_reader_.open(file_name_);
}

arrow::Status ObArrowFile::Seek(int64_t position) {
  position_ = position;
  return arrow::Status::OK();
}

arrow::Result<int64_t> ObArrowFile::Read(int64_t nbytes, void *out)
{
  int ret = OB_SUCCESS;
  arrow::Result<int64_t> ret_code;
  int64_t read_size = -1;
  if (file_prefetch_buffer_.in_prebuffer_range(position_, nbytes)) {
    file_prefetch_buffer_.fetch(position_, nbytes, out);
    position_ += nbytes;
    ret_code = nbytes;
  } else if (OB_FAIL(file_reader_.pread(out, nbytes, position_, read_size))) {
    LOG_WARN("fail to read file", K(ret), K(nbytes));
    ret_code =
      arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, "read file failed"));
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
  arrow::Result<int64_t> ret_code;
  int64_t read_size = -1;
  if (file_prefetch_buffer_.in_prebuffer_range(position, nbytes)) {
    file_prefetch_buffer_.fetch(position, nbytes, out);
    position_ = position + nbytes;
    ret_code = nbytes;
  } else if (OB_FAIL(file_reader_.pread(out, nbytes, position, read_size))) {
    LOG_WARN("fail to read file", K(ret), K(position), K(nbytes));
    ret_code =
      arrow::Result<int64_t>(arrow::Status(arrow::StatusCode::IOError, "read at file failed"));
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
  if (OB_FAIL(file_reader_.get_file_size(file_name_, file_size))) {
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

/*ObParquetOutputStream*/

arrow::Status ObParquetOutputStream::Write(const void* data, int64_t nbytes)
{
  arrow::Status status = arrow::Status::OK();
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (IntoFileLocation::SERVER_DISK == file_location_) {
    if (OB_FAIL(file_appender_->append(data, nbytes, false))) {
      LOG_WARN("failed to append file", K(ret), K(nbytes), K(url_));
      status = arrow::Status(arrow::StatusCode::IOError, "write file failed");
    }
  } else if (OB_FAIL(storage_appender_->append(static_cast<const char*>(data), nbytes, write_size))) {
    LOG_WARN("fail to append data", K(ret), KP(data), K(nbytes), K(url_));
    status = arrow::Status(arrow::StatusCode::IOError, "write file failed");
  }
  if (OB_SUCC(ret)) {
    position_ += nbytes;
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

} // end of oceanbase namespace
} // end of oceanbase namespace