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

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>
#include "lib/file/ob_file.h"

namespace oceanbase
{
namespace common
{
namespace FileComponent
{
template <class T>
int open(const ObString &fname, const T &file, int &fd)
{
  int ret = OB_SUCCESS;
  if (-1 != fd) {
    _OB_LOG(WARN, "file has been open fd=%d", fd);
    ret = OB_INIT_TWICE;
  } else if (NULL == fname.ptr()
             || 0 == fname.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const char *fname_ptr = NULL;
    char buffer[OB_MAX_FILE_NAME_LENGTH];
    if ('\0' != fname.ptr()[fname.length() - 1]) {
      if (OB_MAX_FILE_NAME_LENGTH > snprintf(buffer, OB_MAX_FILE_NAME_LENGTH, "%.*s", fname.length(),
                                             fname.ptr())) {
        fname_ptr = buffer;
      }
    } else {
      fname_ptr = fname.ptr();
    }
    if (NULL == fname_ptr) {
      _OB_LOG(WARN, "prepare fname string fail fname=[%.*s]", fname.length(), fname.ptr());
      ret = OB_INVALID_ARGUMENT;
    } else if (-1 == (fd = ::open(fname_ptr, file.get_open_flags(), file.get_open_mode()))) {
      if (ENOENT == errno) {
        ret = OB_FILE_NOT_EXIST;
      } else if (EEXIST == errno) {
        ret = OB_FILE_ALREADY_EXIST;
      } else {
        _OB_LOG(WARN, "open fname=[%s] fail errno=%u", fname_ptr, errno);
        ret = OB_IO_ERROR;
      }
    } else {
      _OB_LOG(INFO, "open fname=[%s] fd=%d flags=%d succ", fname_ptr, fd, file.get_open_flags());
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int IFileReader::open(const ObString &fname)
{
  return FileComponent::open(fname, *this, fd_);
}

void IFileReader::close()
{
  if (-1 != fd_) {
    if(0 != ::close(fd_)) {
      OB_LOG_RET(WARN, OB_ERR_SYS, "fail to close file ", K_(fd), K(errno), KERRMSG);
    }
    fd_ = -1;
  }
}

bool IFileReader::is_opened() const
{
  return fd_ != -1;
}

void IFileReader::revise(int64_t pos)
{
  if (-1 != fd_) {
    if(0 != ::ftruncate(fd_, pos)) {
       _OB_LOG_RET(WARN, OB_ERR_SYS, "ftruncate fail fd=%d file_pos=%ld errno=%u", fd_, pos, errno);
    }
  }
}

////////////////////////////////////////////////////////////////////////////////////////////////////

BufferFileReader::BufferFileReader()
{
}

BufferFileReader::~BufferFileReader()
{
  if (-1 != fd_) {
    this->close();
  }
}

int BufferFileReader::pread(void *buf, const int64_t count, const int64_t offset,
                            int64_t &read_size)
{
  return pread_by_fd(fd_, buf, count, offset, read_size);
}

int BufferFileReader::pread_by_fd(const int fd, void *buf, const int64_t count,
                                  const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t read_ret = 0;
  if (0 == count) {
    read_size = 0;
  } else if (NULL == buf || 0 > count) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == fd) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else if (0 > (read_ret = unintr_pread(fd, buf, count, offset))) {
    _OB_LOG(WARN, "read fail fd=%d count=%ld offset=%ld read_ret=%ld errno=%u",
              fd, count, offset, read_ret, errno);
    ret = OB_IO_ERROR;
  } else {
    read_size = read_ret;
  }
  return ret;
}

int BufferFileReader::pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf,
                            int64_t &read_size)
{
  return pread_by_fd(fd_, count, offset, file_buf, read_size);
}

int BufferFileReader::pread_by_fd(const int fd, const int64_t count, const int64_t offset,
                                  IFileBuffer &file_buf, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (0 == count) {
    read_size = 0;
  } else if (0 > count
             || OB_SUCCESS != (ret = file_buf.assign(count))
             || NULL == file_buf.get_buffer()) {
    _OB_LOG(WARN, "file_buf assign fail count=%ld ret=%d or get_buffer null pointer", count, ret);
    ret = (OB_SUCCESS == ret) ? OB_INVALID_ARGUMENT : ret;
  } else {
    file_buf.set_base_pos(0);
    ret = pread_by_fd(fd, file_buf.get_buffer(), count, offset, read_size);
  }
  return ret;
}

int BufferFileReader::get_open_flags() const
{
  return OPEN_FLAGS;
}

int BufferFileReader::get_open_mode() const
{
  return OPEN_MODE;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

DirectFileReader::DirectFileReader(const int64_t buffer_size,
                                   const int64_t align_size) : align_size_(align_size),
                                                               buffer_size_(buffer_size),
                                                               buffer_(NULL)
{
}

DirectFileReader::~DirectFileReader()
{
  if (-1 != fd_) {
    this->close();
  }
  if (NULL != buffer_) {
    ::free(buffer_);
    buffer_ = NULL;
  }
}

int DirectFileReader::pread(void *buf, const int64_t count, const int64_t offset,
                            int64_t &read_size)
{
  return pread_by_fd(fd_, buf, count, offset, read_size);
}

int DirectFileReader::pread_by_fd(const int fd, void *buf, const int64_t count,
                                  const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  bool param_align = is_align(buf, count, offset, align_size_);
  if (0 == count) {
    read_size = 0;
  } else if (NULL == buf || 0 > count) {
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == fd) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else if (!param_align
             && NULL == buffer_
             && NULL == (buffer_ = (char *)::memalign(align_size_, buffer_size_))) {
    _OB_LOG(WARN,
              "prepare buffer fail param_align=%s buffer=%p buf=%p count=%ld offset=%ld align_size=%ld buffer_size=%ld",
              STR_BOOL(param_align), buffer_, buf, count, offset, align_size_, buffer_size_);
    ret = OB_ERROR;
  } else {
    int64_t read_ret = 0;
    if (param_align) {
      read_ret = unintr_pread(fd, buf, count, offset);
    } else {
      read_ret = pread_align(fd, buf, count, offset, buffer_, buffer_size_, align_size_);
    }
    if (0 > read_ret) {
      _OB_LOG(WARN, "read fail fd=%d count=%ld offset=%ld read_ret=%ld errno=%u",
                fd_, count, offset, read_ret, errno);
      ret = OB_IO_ERROR;
    } else {
      read_size = read_ret;
    }
  }
  return ret;
}

int DirectFileReader::pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf,
                            int64_t &read_size)
{
  return pread_by_fd(fd_, count, offset, file_buf, read_size);
}

int DirectFileReader::pread_by_fd(const int fd, const int64_t count, const int64_t offset,
                                  IFileBuffer &file_buf, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int64_t offset2read = lower_align(offset, align_size_);
  int64_t size2read = upper_align(offset + count, align_size_) - offset2read;
  if (0 == count) {
    read_size = 0;
  } else if (0 > count
             || OB_SUCCESS != (ret = file_buf.assign(size2read, align_size_))
             || NULL == file_buf.get_buffer()) {
    _OB_LOG(WARN, "file_buf assign fail count=%ld ret=%d or get_buffer null pointer", count, ret);
    ret = (OB_SUCCESS == ret) ? OB_INVALID_ARGUMENT : ret;
  } else if (-1 == fd) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else {
    int64_t buffer_offset = offset - offset2read;
    int64_t read_ret = 0;
    if (0 > (read_ret = unintr_pread(fd, file_buf.get_buffer(), size2read, offset2read))) {
      _OB_LOG(WARN, "read fail fd=%d count=%ld offset=%ld read_ret=%ld errno=%u",
                fd, count, offset, read_ret, errno);
      ret = OB_IO_ERROR;
    } else {
      file_buf.set_base_pos(buffer_offset);
      read_size = (read_ret < buffer_offset) ? 0 : std::min(read_ret - buffer_offset, count);
    }
  }
  return ret;
}

int DirectFileReader::get_open_flags() const
{
  return OPEN_FLAGS;
}

int DirectFileReader::get_open_mode() const
{
  return OPEN_MODE;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int IFileAppender::open(const ObString &fname, const bool is_create, const bool is_trunc)
{
  int ret = OB_SUCCESS;
  if (is_trunc) {
    this->add_truncate_flags_();
  }
  if (is_create) {
    this->add_create_flags_();
  }
  ret = FileComponent::open(fname, *this, fd_);
  this->set_normal_flags_();

  if (OB_SUCC(ret)) {
    int64_t file_pos = get_file_size(fd_);
    this->set_file_pos_(file_pos);
    if (0 > file_pos
        || OB_SUCCESS != (ret = this->prepare_buffer_())) {
      this->close();
      ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
    }
  }
  return ret;
}

int IFileAppender::create(const ObString &fname)
{
  int ret = OB_SUCCESS;
  this->add_create_flags_();
  this->add_excl_flags_();
  if (OB_SUCCESS != (ret = FileComponent::open(fname, *this, fd_))) {
    _OB_LOG(WARN, "open file error:ret=%d,fname=%s,fd_=%d",
              ret, fname.ptr(), fd_);
  } else {
    this->set_normal_flags_();
    this->set_file_pos_(0);
    if (OB_SUCCESS != (ret = this->prepare_buffer_())) {
      this->close();
    }
  }
  return ret;
}

void IFileAppender::close()
{
  if (-1 != fd_) {
    this->fsync();
    ::close(fd_);
    fd_ = -1;
  }
}

bool IFileAppender::is_opened() const
{
  return fd_ != -1;
}

int IFileAppender::get_fd() const
{
  return fd_;
}


////////////////////////////////////////////////////////////////////////////////////////////////////

BufferFileAppender::BufferFileAppender(const int64_t buffer_size) : open_flags_(NORMAL_FLAGS),
                                                                    buffer_size_(buffer_size),
                                                                    buffer_pos_(0),
                                                                    file_pos_(0),
                                                                    buffer_(NULL)
{
}

BufferFileAppender::~BufferFileAppender()
{
  if (-1 != fd_) {
    this->close();
  }
  if (NULL != buffer_) {
    ::free(buffer_);
    buffer_ = NULL;
  }
}

void BufferFileAppender::close()
{
  IFileAppender::close();
  open_flags_ = NORMAL_FLAGS;
  buffer_pos_ = 0;
  file_pos_ = 0;
}

int BufferFileAppender::buffer_sync_()
{
  int ret = OB_SUCCESS;
  if (NULL != buffer_
      && 0 != buffer_pos_) {
    int64_t write_ret = 0;
    if (buffer_pos_ != (write_ret = unintr_pwrite(fd_, buffer_, buffer_pos_, file_pos_))) {
      _OB_LOG(WARN, "write buffer fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u",
                fd_, buffer_, buffer_pos_, file_pos_, write_ret, errno);
      ret = OB_IO_ERROR;
    } else {
      _OB_LOG(DEBUG, "write buffer succ fd=%d buffer_size=%ld file_pos=%ld", fd_, buffer_pos_,
                file_pos_);
      file_pos_ += buffer_pos_;
      buffer_pos_ = 0;
    }
  }
  return ret;
}

int BufferFileAppender::fsync()
{
  int ret = OB_SUCCESS;
  if (-1 == fd_) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else if (OB_SUCCESS == (ret = buffer_sync_())) {
    if (0 != ::fsync(fd_)) {
      _OB_LOG(WARN, "fsync fail fd=%d errno=%u", fd_, errno);
      ret = OB_IO_ERROR;
    }
  }
  return ret;
}

int BufferFileAppender::async_append(const void *buf, const int64_t count,
                                     IFileAsyncCallback *callback)
{
  UNUSED(buf);
  UNUSED(count);
  UNUSED(callback);
  return OB_NOT_SUPPORTED;
}

int BufferFileAppender::append(const void *buf, const int64_t count, const bool is_fsync)
{
  int ret = OB_SUCCESS;
  if (-1 == fd_) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else if (0 == count) {
    // do nothing
  } else if (NULL == buf || 0 > count) {
    _OB_LOG(WARN, "invalid param buf=%p count=%ld", buf, count);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if ((buffer_size_ - buffer_pos_) < count) {
      ret = buffer_sync_();
    }

    if (OB_SUCC(ret)) {
      if ((buffer_size_ - buffer_pos_) < count) {
        int64_t write_ret = 0;
        if (count != (write_ret = unintr_pwrite(fd_, buf, count, file_pos_))) {
          _OB_LOG(WARN, "write fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u",
                    fd_, buf, count, file_pos_, write_ret, errno);
          ret = OB_IO_ERROR;
        } else {
          file_pos_ += count;
        }
      } else {
        MEMCPY(buffer_ + buffer_pos_, buf, count);
        buffer_pos_ += count;
      }
    }
  }
  if (OB_SUCCESS == ret
      && is_fsync) {
    ret = this->fsync();
  }
  return ret;
}

int BufferFileAppender::prepare_buffer_()
{
  int ret = OB_SUCCESS;
  if (NULL == buffer_
      && NULL == (buffer_ = (char *)::malloc(buffer_size_))) {
    _OB_LOG(WARN, "prepare buffer fail buffer_size=%ld", buffer_size_);
    ret = OB_ERROR;
  }
  return ret;
}

void BufferFileAppender::set_normal_flags_()
{
  open_flags_ = NORMAL_FLAGS;
}

void BufferFileAppender::add_truncate_flags_()
{
  open_flags_ |= TRUNC_FLAGS;
}

void BufferFileAppender::add_create_flags_()
{
  open_flags_ |= CREAT_FLAGS;
}

void BufferFileAppender::add_excl_flags_()
{
  open_flags_ |= EXCL_FLAGS;
}

void BufferFileAppender::set_file_pos_(const int64_t file_pos)
{
  file_pos_ = file_pos;
}

int BufferFileAppender::get_open_flags() const
{
  return open_flags_;
}

int BufferFileAppender::get_open_mode() const
{
  return OPEN_MODE;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

DirectFileAppender::DirectFileAppender(const int64_t buffer_size,
                                       const int64_t align_size) : open_flags_(NORMAL_FLAGS),
                                                                   align_size_(align_size),
                                                                   buffer_size_(buffer_size),
                                                                   buffer_pos_(0),
                                                                   file_pos_(0),
                                                                   buffer_(NULL),
                                                                   buffer_length_(0),
                                                                   align_warn_(0)
{
}

DirectFileAppender::~DirectFileAppender()
{
  if (-1 != fd_) {
    this->close();
  }
  if (NULL != buffer_) {
    ::free(buffer_);
    buffer_ = NULL;
  }
}

void DirectFileAppender::close()
{
  IFileAppender::close();
  open_flags_ = NORMAL_FLAGS;
  buffer_pos_ = 0;
  file_pos_ = 0;
  buffer_length_ = 0;
  align_warn_ = 0;
}

int DirectFileAppender::buffer_sync_(bool *need_truncate)
{
  int ret = OB_SUCCESS;
  if (NULL != buffer_
      && 0 != buffer_length_) {
    int64_t offset2write = lower_align(file_pos_, align_size_);
    int64_t size2write = buffer_pos_;
    // If the buffer space is enough, write some debug information
    //if ((int64_t)(2 * sizeof(int64_t)) <= (buffer_size_ - buffer_pos_))
    //{
    //  *((int64_t*)&(buffer_[buffer_pos_])) = DEBUG_MAGIC;
    //  *((int64_t*)&(buffer_[buffer_pos_ + sizeof(int64_t)])) = buffer_pos_;
    //  size2write += (2 * sizeof(int64_t));
    //}
    size2write = upper_align(buffer_pos_, align_size_);
    int64_t write_ret = 0;
    memset(buffer_ + buffer_pos_, 0, size2write - buffer_pos_);
    if (size2write != (write_ret = unintr_pwrite(fd_, buffer_, size2write, offset2write))) {
      _OB_LOG(WARN, "write buffer fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u "
                "file_pos=%ld align_size=%ld buffer_pos=%ld",
                fd_, buffer_, size2write, offset2write, write_ret, errno,
                file_pos_, align_size_, buffer_pos_);
      ret = OB_IO_ERROR;
    } else {
      file_pos_ += buffer_length_;
      if (NULL != need_truncate) {
        *need_truncate = (file_pos_ != (offset2write + size2write));
      }
      buffer_length_ = 0;
      int64_t size2reserve = buffer_pos_ - lower_align(buffer_pos_, align_size_);
      if (0 != size2reserve) {
        memmove(buffer_, buffer_ + buffer_pos_ - size2reserve, size2reserve);
        buffer_pos_ = size2reserve;
      } else {
        buffer_pos_ = 0;
      }
    }
  }
  return ret;
}

int DirectFileAppender::fsync()
{
  int ret = OB_SUCCESS;
  bool need_truncate = false;
  if (-1 == fd_) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else if (OB_SUCCESS == (ret = buffer_sync_(&need_truncate))) {
    if (need_truncate
        && 0 != ::ftruncate(fd_, file_pos_)) {
      _OB_LOG(WARN, "ftruncate fail fd=%d file_pos=%ld errno=%u", fd_, file_pos_, errno);
      ret = OB_IO_ERROR;
    }
  }
  if (OB_SUCC(ret)) {
    if (0 != ::fsync(fd_)) {
      _OB_LOG(WARN, "fsync fail fd=%d errno=%u", fd_, errno);
      ret = OB_IO_ERROR;
    }
  }
  return ret;
}

int DirectFileAppender::async_append(const void *buf, const int64_t count,
                                     IFileAsyncCallback *callback)
{
  // TODO will support soon
  UNUSED(buf);
  UNUSED(count);
  UNUSED(callback);
  return OB_NOT_SUPPORTED;
}

int DirectFileAppender::append(const void *buf, const int64_t count, const bool is_fsync)
{
  int ret = OB_SUCCESS;
  if (-1 == fd_) {
    _OB_LOG(WARN, "file has not been open");
    ret = OB_ERROR;
  } else if (0 == count) {
    // do nothing
  } else if (NULL == buf || 0 > count) {
    _OB_LOG(WARN, "invalid param buf=%p count=%ld", buf, count);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (0 == align_warn_
        && 0 != file_pos_ % align_size_) {
      align_warn_++;
      _OB_LOG(WARN, "file_pos_=%ld do not match align_size=%ld", file_pos_, align_size_);
    }

    bool buffer_synced = false;
    if (buffer_size_ < (buffer_pos_ + count)) {
      ret = buffer_sync_();
      buffer_synced = true;
    }

    if (OB_SUCC(ret)) {
      if (buffer_synced
          && buffer_size_ < (buffer_pos_ + count)) {
        bool param_align = is_align(buf, count, file_pos_, align_size_);
        int64_t write_ret = 0;
        if (param_align) {
          write_ret = unintr_pwrite(fd_, buf, count, file_pos_);
        } else {
          write_ret = pwrite_align(fd_, buf, count, file_pos_, buffer_, buffer_size_, align_size_,
                                   buffer_pos_);
        }
        if (count != write_ret) {
          _OB_LOG(WARN, "write fail fd=%d buffer=%p count=%ld offset=%ld write_ret=%ld errno=%u "
                    "align_buffer=%p align_size=%ld buffer_pos=%ld",
                    fd_, buf, count, file_pos_, write_ret, errno,
                    buffer_, align_size_, buffer_pos_);
          ret = OB_IO_ERROR;
        } else {
          file_pos_ += count;
        }
        if (!param_align
            && 0 != ::ftruncate(fd_, file_pos_)) {
          _OB_LOG(WARN, "ftruncate fail fd=%d file_pos=%ld errno=%u", fd_, file_pos_, errno);
          ret = OB_IO_ERROR;
        }
      } else {
        MEMCPY(buffer_ + buffer_pos_, buf, count);
        buffer_pos_ += count;
        buffer_length_ += count;
      }
    }
  }
  if (OB_SUCCESS == ret
      && is_fsync) {
    ret = this->fsync();
  }
  return ret;
}

int DirectFileAppender::prepare_buffer_()
{
  int ret = OB_SUCCESS;
  if (NULL == buffer_
      && NULL == (buffer_ = (char *)::memalign(align_size_, buffer_size_))) {
    _OB_LOG(WARN, "prepare buffer fail align_size=%ld buffer_size=%ld", align_size_, buffer_size_);
    ret = OB_ERROR;
  } else if (0 != (file_pos_ % align_size_)) {
    int64_t offset2read = lower_align(file_pos_, align_size_);
    int64_t size2read = file_pos_ - offset2read;
    int64_t read_ret = 0;
    if (size2read != (read_ret = unintr_pread(fd_, buffer_, align_size_, offset2read))) {
      _OB_LOG(WARN,
                "read buffer fail fd=%d buffer=%p align_size=%ld offset2read=%ld size2read=%ld read_ret=%ld errno=%u",
                fd_, buffer_, align_size_, offset2read, size2read, read_ret, errno);
      ret = OB_IO_ERROR;
    } else {
      buffer_pos_ = read_ret;
    }
  }
  return ret;
}

void DirectFileAppender::set_normal_flags_()
{
  open_flags_ = NORMAL_FLAGS;
}

void DirectFileAppender::add_truncate_flags_()
{
  open_flags_ |= TRUNC_FLAGS;
}

void DirectFileAppender::add_create_flags_()
{
  open_flags_ |= CREAT_FLAGS;
}

void DirectFileAppender::add_excl_flags_()
{
  open_flags_ |= EXCL_FLAGS;
}

void DirectFileAppender::set_file_pos_(const int64_t file_pos)
{
  file_pos_ = file_pos;
}

int DirectFileAppender::get_open_flags() const
{
  return open_flags_;
}

int DirectFileAppender::get_open_mode() const
{
  return OPEN_MODE;
}

int DirectFileAppender::set_align_size(const int64_t align_size)
{
  int ret = OB_SUCCESS;
  if (align_size != align_size_) {
    _OB_LOG(INFO, "change align size from align_size=%ld to new_align_size=%ld",
              align_size_, align_size);
  }
  if (-1 != fd_) {
    _OB_LOG(WARN, "file is open cannot modify align_size");
    ret = OB_INIT_TWICE;
  } else {
    align_size_ = align_size;
    if (NULL != buffer_) {
      ::free(buffer_);
      buffer_ = NULL;
    }
  }
  return ret;
}
}

////////////////////////////////////////////////////////////////////////////////////////////////////

int atomic_rename(const char *oldpath, const char *newpath)
{
  int ret = 0;
  if (NULL == oldpath
      || NULL == newpath) {
    ret = -1;
    errno = EINVAL;
  } else {
    if (0 == (ret = ::link(oldpath, newpath))) {
      if (0 != unlink(oldpath)) {
        ret = -1;
      }
    }
  }
  return ret;
}

int64_t unintr_pwrite(const int fd, const void *buf, const int64_t count, const int64_t offset)
{
  int64_t length2write = count;
  int64_t offset2write = 0;
  int64_t write_ret = 0;
  while (length2write > 0) {
    for (int64_t retry = 0; retry < 3;) {
      write_ret = ::pwrite(fd, (char *)buf + offset2write, length2write, offset + offset2write);
      if (0 >= write_ret) {
        if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
          continue;
        }
        _OB_LOG_RET(ERROR, OB_ERR_SYS,
                    "pwrite fail ret=%ld errno=%u fd=%d buf=%p size2write=%ld offset2write=%ld retry_num=%ld",
                    write_ret, errno, fd, (char *)buf + offset2write, length2write, offset + offset2write, retry);
        retry++;
      } else {
        break;
      }
    }
    if (0 >= write_ret) {
      if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
        continue;
      }
      offset2write = -1;
      break;
    }
    length2write -= write_ret;
    offset2write += write_ret;
  }
  return offset2write;
}
int64_t unintr_write(const int fd, const void *buf, const int64_t count)
{
  int64_t length2write = count;
  int64_t offset2write = 0;
  int64_t write_ret = 0;
  while (length2write > 0) {
    for (int64_t retry = 0; retry < 3;) {
      write_ret = write(fd, (char *)buf + offset2write, length2write);
      if (0 >= write_ret) {
        if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
          continue;
        }
        _OB_LOG_RET(ERROR, OB_ERR_SYS,
                  "pwrite fail ret=%ld errno=%u fd=%d buf=%p size2write=%ld offset2write=%ld retry_num=%ld",
                  write_ret, errno, fd, (char *)buf + offset2write, length2write, offset2write, retry);
        retry++;
      } else {
        break;
      }
    }
    if (0 >= write_ret) {
      if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
        continue;
      }
      break;
    }
    length2write -= write_ret;
    offset2write += write_ret;
  }
  return offset2write;
}

int64_t unintr_pread(const int fd, void *buf, const int64_t count, const int64_t offset)
{
  int64_t length2read = count;
  int64_t offset2read = 0;
  int64_t read_ret = 0;
  while (length2read > 0) {
    for (int64_t retry = 0; retry < 3;) {
      read_ret = ::pread(fd, (char *)buf + offset2read, length2read, offset + offset2read);
      if (0 > read_ret) {
        if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
          continue;
        }
        _OB_LOG_RET(ERROR, OB_ERR_SYS,
                  "pread fail ret=%ld errno=%u fd=%d buf=%p size2read=%ld offset2read=%ld retry_num=%ld",
                  read_ret, errno, fd, (char *)buf + offset2read, length2read, offset + offset2read, retry);
        retry++;
      } else {
        break;
      }
    }
    if (0 >= read_ret) {
      if (errno == EINTR) { // Blocking IO does not need to judge EAGAIN
        continue;
      }
      if (0 > read_ret) {
        offset2read = -1;
      }
      break;
    }
    offset2read += read_ret;
    if (length2read > read_ret) {
      break;
    } else {
      length2read -= read_ret;
    }
  }
  return offset2read;
}

bool is_align(const void *buf, const int64_t count, const int64_t offset, const int64_t align_size)
{
  bool bret = false;
  if (0 == (int64_t)buf % align_size
      && 0 == count % align_size
      && 0 == offset % align_size) {
    bret = true;
  }
  return bret;
}

int64_t pread_align(const int fd, void *buf, const int64_t count, const int64_t offset,
                    void *align_buffer, const int64_t buffer_size, const int64_t align_size)
{
  int64_t ret = 0;
  int64_t offset2read = lower_align(offset, align_size);
  int64_t size2read = upper_align(offset + count, align_size) - offset2read;
  int64_t buffer_offset = offset - offset2read;

  int64_t read_ret = 0;
  int64_t size2copy = 0;
  while (size2read > 0
         && ret < count) {
    read_ret = unintr_pread(fd, align_buffer, buffer_size, offset2read);
    if (0 > read_ret) {
      ret = -1;
      break;
    } else if (read_ret <= buffer_offset) {
      // The amount of data read is less than or equal to the data used for alignment, that is, there is no data after the offset specified by the user.
      break;
    } else {
      size2copy = std::min(read_ret - buffer_offset, count - ret);
      MEMCPY((char *)buf + ret, (char *)align_buffer + buffer_offset, size2copy);
      ret += size2copy;

      offset2read += read_ret;
      size2read -= read_ret;
      buffer_offset = 0;

      // The amount of data read is less than the requested. To the end of the file, break out
      if (buffer_size > read_ret) {
        break;
      }
    }
  }
  return ret;
}

int64_t pwrite_align(const int fd, const void *buf, const int64_t count, const int64_t offset,
                     void *align_buffer, const int64_t buffer_size, const int64_t align_size, int64_t &buffer_pos)
{
  int64_t ret = 0;
  int64_t total2write = count;
  int64_t total_offset = offset;

  int64_t write_ret = 0;
  while (0 < total2write) {
    int64_t size2copy = std::min(buffer_size - buffer_pos, total2write);
    MEMCPY((char *)align_buffer + buffer_pos, (char *)buf + ret, size2copy);
    total2write -= size2copy;
    buffer_pos += size2copy;

    int64_t offset2write = lower_align(total_offset, align_size);
    int64_t size2write = upper_align(buffer_pos, align_size);
    memset((char *)align_buffer + buffer_pos, 0, size2write - buffer_pos);
    write_ret = unintr_pwrite(fd, align_buffer, size2write, offset2write);
    if (size2write != write_ret) {
      ret = -1;
      break;
    }
    total_offset += size2copy;
    ret = ret + size2copy;
    if (buffer_pos == size2write) {
      buffer_pos = 0;
    }
  }

  int64_t size2reserve = buffer_pos - lower_align(buffer_pos, align_size);
  if (0 != size2reserve) {
    memmove(align_buffer, (char *)align_buffer + buffer_pos - size2reserve, size2reserve);
    buffer_pos = size2reserve;
  } else {
    buffer_pos = 0;
  }
  return ret;
}

int64_t get_file_size(const int fd)
{
  int64_t ret = -1;
  struct stat st;
  if (-1 != fd
      && 0 == fstat(fd, &st)) {
    ret = st.st_size;
  }
  return ret;
}

int64_t get_file_size(const char *fname)
{
  int64_t ret = -1;
  struct stat st;
  if (NULL != fname
      && 0 == stat(fname, &st)) {
    ret = st.st_size;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObFileBuffer::ObFileBuffer() : buffer_(NULL), base_pos_(0), buffer_size_(0)
{
}

ObFileBuffer::~ObFileBuffer()
{
  if (NULL != buffer_) {
    ::free(buffer_);
    buffer_ = NULL;
  }
}

char *ObFileBuffer::get_buffer()
{
  return buffer_;
}

int64_t ObFileBuffer::get_base_pos()
{
  return base_pos_;
}

void ObFileBuffer::set_base_pos(const int64_t pos)
{
  if (pos > buffer_size_) {
    _OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "base_pos=%ld will be greater than buffer_size=%ld", pos, buffer_size_);
  }
  base_pos_ = pos;
}

int ObFileBuffer::assign(const int64_t size, const int64_t align)
{
  int ret = OB_SUCCESS;
  if (0 >= size
      || 0 >= align
      || !is2n(align)
      || 0 != size % align) {
    _OB_LOG(WARN, "invalid size=%ld or donot match align=%ld", size, align);
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == buffer_
             || buffer_size_ < size
             || (MIN_BUFFER_SIZE < buffer_size_ && MIN_BUFFER_SIZE > size)
             || 0 != (int64_t)buffer_ % align) {
    int64_t alloc_size = size;
    if (MIN_BUFFER_SIZE > size) {
      alloc_size = MIN_BUFFER_SIZE;
    }
    if (NULL != buffer_) {
      ::free(buffer_);
      buffer_ = NULL;
      base_pos_ = 0;
      buffer_size_ = 0;
    }
    if (NULL == (buffer_ = (char *)::memalign(align, alloc_size))) {
      _OB_LOG(WARN, "memalign fail align=%ld alloc_size=%ld size=%ld errno=%u", align, alloc_size, size,
                errno);
      ret = OB_ERROR;
    } else {
      buffer_size_ = alloc_size;
    }
  } else {
    base_pos_ = 0;
  }
  return ret;
}

int ObFileBuffer::assign(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (0 >= size) {
    _OB_LOG(WARN, "invalid size=%ld", size);
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == buffer_
             || buffer_size_ < size
             || (MIN_BUFFER_SIZE < buffer_size_ && MIN_BUFFER_SIZE > size)) {
    int64_t alloc_size = size;
    if (MIN_BUFFER_SIZE > size) {
      alloc_size = MIN_BUFFER_SIZE;
    }
    if (NULL != buffer_) {
      ::free(buffer_);
      buffer_ = NULL;
      base_pos_ = 0;
      buffer_size_ = 0;
    }
    if (NULL == (buffer_ = (char *)::malloc(alloc_size))) {
      _OB_LOG(WARN, "malloc fail alloc_size=%ld size=%ld errno=%u", alloc_size, size, errno);
      ret = OB_ERROR;
    } else {
      buffer_size_ = alloc_size;
    }
  } else {
    base_pos_ = 0;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObFileReader::ObFileReader() : file_(NULL)
{
}

ObFileReader::~ObFileReader()
{
  if (NULL != file_) {
    delete file_;
    file_ = NULL;
  }
}

int ObFileReader::open(const ObString &fname, const bool dio, const int64_t align_size)
{
  int ret = OB_SUCCESS;
  if (NULL != file_) {
    ret = OB_INIT_TWICE;
  } else if (!is2n(align_size)) {
    _OB_LOG(WARN, "invalid align_size=%ld", align_size);
    ret = OB_INVALID_ARGUMENT;
  } else {
    using namespace FileComponent;
    if (dio) {
      file_ = new(std::nothrow) DirectFileReader(DirectFileReader::DEFAULT_BUFFER_SIZE, align_size);
    } else {
      file_ = new(std::nothrow) BufferFileReader();
    }

    if (NULL == file_) {
      _OB_LOG(WARN, "construct file reader fail fname=[%.*s]", fname.length(), fname.ptr());
      ret = OB_ERROR;
    } else {
      ret = file_->open(fname);
    }
  }

  return ret;
}

void ObFileReader::close()
{
  if (NULL != file_) {
    file_->close();
    delete file_;
    file_ = NULL;
  }
}

bool ObFileReader::is_opened() const
{
  return NULL != file_ && file_->is_opened();
}

void ObFileReader::revise(int64_t pos)
{
  file_->revise(pos);
}

int ObFileReader::pread(void *buf, const int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (NULL == file_) {
    ret = OB_ERROR;
  } else {
    ret = file_->pread(buf, count, offset, read_size);
  }
  return ret;
}

int ObFileReader::pread(const int64_t count, const int64_t offset, IFileBuffer &file_buf,
                        int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (NULL == file_) {
    ret = OB_ERROR;
  } else {
    ret = file_->pread(count, offset, file_buf, read_size);
  }
  return ret;
}

int ObFileReader::read_record(IFileInfoMgr &fileinfo_mgr,
                              const uint64_t file_id,
                              const int64_t offset,
                              const int64_t size,
                              IFileBuffer &file_buf)
{
  int ret = OB_SUCCESS;
  const IFileInfo *file_info = fileinfo_mgr.get_fileinfo(file_id);
  if (NULL == file_info) {
    _OB_LOG(WARN, "get file info fail file_id=%lu offset=%ld size=%ld", file_id, offset, size);
    ret = OB_ERROR;
  } else {
    ret = read_record(*file_info, offset, size, file_buf);
    fileinfo_mgr.revert_fileinfo(file_info);
  }
  return ret;
}

int ObFileReader::read_record(const IFileInfo &file_info,
                              const int64_t offset,
                              const int64_t size,
                              IFileBuffer &file_buf)
{
  int ret = OB_SUCCESS;
  int fd = file_info.get_fd();
  if (OB_SUCCESS != (ret = read_record(fd, offset, size, file_buf))) {
    _OB_LOG(WARN, "read record fail: ret=[%d], fd=[%d], offset=[%ld], size=[%ld]", ret, fd, offset,
              size);
  }
  return ret;
}

int ObFileReader::read_record(const int fd,
                              const int64_t offset,
                              const int64_t size,
                              IFileBuffer &file_buf)
{
  int ret = OB_SUCCESS;
  int flags = 0;
  if (-1 == fd) {
    _OB_LOG(WARN, "invalid fd");
    ret = OB_INVALID_ARGUMENT;
  } else if (-1 == (flags = fcntl(fd, F_GETFL))) {
    _OB_LOG(WARN, "fcntl F_GETFL fail fd=%d errno=%u", fd, errno);
    ret = OB_ERROR;
  } else {
    FileComponent::BufferFileReader buffer_reader;
    FileComponent::DirectFileReader direct_reader;
    FileComponent::IFileReader *preader = NULL;
    if (O_DIRECT == (O_DIRECT & flags)) {
      preader = &direct_reader;
    } else {
      preader = &buffer_reader;
    }
    int64_t read_size = 0;
    ret = preader->pread_by_fd(fd, size, offset, file_buf, read_size);
    if (size != read_size) {
      _OB_LOG(WARN, "read_size=%ld do not equal size=%ld", read_size, size);
      ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

ObFileAppender::ObFileAppender() : file_(NULL)
{
}

ObFileAppender::~ObFileAppender()
{
  if (NULL != file_) {
    file_->close();
    file_ = NULL;
  }
}

int ObFileAppender::open(const ObString &fname, const bool dio, const bool is_create,
                         const bool is_trunc, const int64_t align_size)
{
  int ret = OB_SUCCESS;
  if (NULL != file_) {
    ret = OB_INIT_TWICE;
  } else if (!is2n(align_size)) {
    _OB_LOG(WARN, "invalid align_size=%ld", align_size);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (dio) {
      file_ = &direct_file_;
      ret = direct_file_.set_align_size(align_size);
    } else {
      file_ = &buffer_file_;
    }
  }
  if (NULL == file_) {
    _OB_LOG(WARN, "construct file appender fail fname=[%.*s]", fname.length(), fname.ptr());
    ret = OB_ERROR;
  } else if (OB_FAIL(ret)) {
    _OB_LOG(WARN, "set align_size=%ld fail", align_size);
  } else {
    ret = file_->open(fname, is_create, is_trunc);
  }
  if (OB_FAIL(ret)) {
    file_ = NULL;
  }
  return ret;
}

int ObFileAppender::create(const ObString &fname, const bool dio, const int64_t align_size)
{
  int ret = OB_SUCCESS;
  if (NULL != file_) {
    ret = OB_INIT_TWICE;
  } else if (!is2n(align_size)) {
    _OB_LOG(WARN, "invalid align_size=%ld", align_size);
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (dio) {
      file_ = &direct_file_;
      ret = direct_file_.set_align_size(align_size);
    } else {
      file_ = &buffer_file_;
    }
    if (NULL == file_) {
      _OB_LOG(WARN, "construct file appender fail fname=[%.*s]", fname.length(), fname.ptr());
      ret = OB_ERROR;
    } else if (OB_FAIL(ret)) {
      _OB_LOG(WARN, "set align_size=%ld fail", align_size);
    } else {
      ret = file_->create(fname);
    }
  }
  if (OB_FAIL(ret)) {
    file_ = NULL;
  }
  return ret;
}

void ObFileAppender::close()
{
  if (NULL != file_) {
    file_->close();
    file_ = NULL;
  }
}

bool ObFileAppender::is_opened() const
{
  return NULL != file_ && file_->is_opened();
}

int ObFileAppender::append(const void *buf, const int64_t count, const bool is_fsync)
{
  int ret = OB_SUCCESS;
  if (NULL == file_) {
    ret = OB_ERROR;
  } else {
    ret = file_->append(buf, count, is_fsync);
  }
  return ret;
}

int ObFileAppender::async_append(const void *buf, const int64_t count, IFileAsyncCallback *callback)
{
  int ret = OB_SUCCESS;
  if (NULL == file_) {
    ret = OB_ERROR;
  } else {
    ret = file_->async_append(buf, count, callback);
  }
  return ret;
}

int ObFileAppender::fsync()
{
  int ret = OB_SUCCESS;
  if (NULL == file_) {
    ret = OB_ERROR;
  } else {
    ret = file_->fsync();
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __USE_AIO_FILE
ObFileAsyncAppender::ObFileAsyncAppender() : pool_(),
                                             fd_(-1),
                                             file_pos_(0),
                                             align_size_(0),
                                             cur_iocb_(NULL)
{
  memset(&ctx_, 0, sizeof(ctx_));
  int tmp_ret = 0;
  if (0 != (tmp_ret = io_setup(AIO_MAXEVENTS, &ctx_))) {
    _OB_LOG_RET(ERROR, OB_ERR_SYS, "io_setup fail ret=%d", tmp_ret);
  }
}

ObFileAsyncAppender::~ObFileAsyncAppender()
{
  io_destroy(ctx_);
  if (-1 != fd_) {
    close();
  }
}

int ObFileAsyncAppender::open(const ObString &fname,
                              const bool dio,
                              const bool is_create,
                              const bool is_trunc,
                              const int64_t align_size)
{
  int ret = OB_SUCCESS;
  const bool is_excl = false;
  OpenParam op(dio, is_create, is_trunc, is_excl);
  if (-1 != fd_) {
    ret = OB_INIT_TWICE;
  } else if (!dio
             || !is2n(align_size)) {
    _OB_LOG(WARN, "invalid param dio=%s align_size=%ld", STR_BOOL(dio), align_size);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = FileComponent::open(fname, op, fd_))) {
    _OB_LOG(WARN, "open file fail, ret=%d dio=%s is_create=%s is_trunc=%s is_excl=%s fname=[%.*s]",
              ret, STR_BOOL(dio), STR_BOOL(is_create), STR_BOOL(is_trunc), STR_BOOL(is_excl), fname.length(),
              fname.ptr());
  } else {
    file_pos_ = get_file_size(fd_);
    if (0 != (file_pos_ % align_size)) {
      _OB_LOG(WARN, "file_size=%ld do not align, align_size=%ld fd=%d", file_pos_, align_size, fd_);
      close();
    } else {
      align_size_ = align_size;
    }
  }
  return ret;
}

int ObFileAsyncAppender::create(const ObString &fname,
                                const bool dio,
                                const int64_t align_size)
{
  int ret = OB_SUCCESS;
  const bool is_create = true;
  const bool is_trunc = false;
  const bool is_excl = true;
  OpenParam op(dio, is_create, is_trunc, is_excl);
  if (-1 != fd_) {
    ret = OB_INIT_TWICE;
  } else if (!dio
             || !is2n(align_size)) {
    _OB_LOG(WARN, "invalid param dio=%s align_size=%ld", STR_BOOL(dio), align_size);
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = FileComponent::open(fname, op, fd_))) {
    _OB_LOG(WARN, "open file fail, ret=%d dio=%s is_create=%s is_trunc=%s is_excl=%s fname=[%.*s]",
              ret, STR_BOOL(dio), STR_BOOL(is_create), STR_BOOL(is_trunc), STR_BOOL(is_excl), fname.length(),
              fname.ptr());
  } else {
    file_pos_ = 0;
    align_size_ = align_size;
  }
  return ret;
}

void ObFileAsyncAppender::close()
{
  if (-1 != fd_) {
    if (OB_SUCCESS != fsync()) {
      // Fatal error
      _OB_LOG_RET(ERROR, OB_ERR_SYS, "fsync fail fd=%d, will set fd=-1, and the fd will leek", fd_);
    } else {
      if (0 != ftruncate(fd_, file_pos_)) {
        OB_LOG_RET(WARN, OB_ERR_SYS, "fail to truncate file ", K_(fd), K(errno), KERRMSG);
      }
      if (0 != ::close(fd_)) {
        OB_LOG_RET(WARN, OB_ERR_SYS, "fail to close file ", K_(fd), K(errno), KERRMSG);
      }


    }
    fd_ = -1;
    cur_iocb_ = NULL;
  }
}

int64_t ObFileAsyncAppender::get_file_pos() const
{
  return file_pos_;
}

int ObFileAsyncAppender::append(const void *buf,
                                const int64_t count,
                                const bool is_fsync)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(count);
  UNUSED(is_fsync);
  AIOCB *iocb = NULL;
  if (-1 == fd_) {
    ret = OB_NOT_INIT;
  } else if (NULL == buf
             || 0 >= count) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t size2append = count;
    while (OB_SUCC(ret) && 0 < size2append) {
      if (NULL == (iocb = get_iocb_())) {
        ret = OB_AIO_TIMEOUT;
      } else {
        int64_t buffer_available = AIO_BUFFER_SIZE - iocb->buffer_pos;
        int64_t size2copy = std::min(buffer_available, size2append);
        MEMCPY(iocb->buffer + iocb->buffer_pos, (char *)buf + (count - size2append), size2copy);
        iocb->buffer_pos += size2copy;
        size2append -= size2copy;
        if (AIO_BUFFER_SIZE == iocb->buffer_pos
            && OB_FAIL(submit_iocb_(iocb))) {
        }
      }
    }
    if (OB_SUCCESS != ret
        && 0 != size2append
        && count != size2append) {
      // Fatal error
      _OB_LOG(ERROR, "iocb timeout, to protect the file, will set fd=-1, and the fd will leek");
      fd_ = -1;
      cur_iocb_ = NULL;
    }
    if (OB_SUCCESS == ret
        && is_fsync) {
      ret = fsync();
    }
  }
  return ret;
}

int ObFileAsyncAppender::fsync()
{
  int ret = OB_SUCCESS;
  if (-1 == fd_) {
    ret = OB_NOT_INIT;
  } else if (NULL != cur_iocb_) {
    ret = submit_iocb_(cur_iocb_);
  }

  if (OB_SUCC(ret)) {
    int64_t pool_used = pool_.used();
    for (int64_t i = 0; i < pool_used; i++) {
      wait();
    }
    if (0 != pool_.used()) {
      ret = OB_AIO_TIMEOUT;
    }
  }
  return ret;
}

int ObFileAsyncAppender::submit_iocb_(AIOCB *iocb)
{
  int ret = OB_SUCCESS;
  io_prep_pwrite(&(iocb->cb), fd_, iocb->buffer, upper_align(iocb->buffer_pos, align_size_),
                 file_pos_);
  iocb->cb.data = iocb;
  struct iocb *cb_ptr = &(iocb->cb);
  int tmp_ret = 0;
  if (1 != (tmp_ret = io_submit(ctx_, 1, &cb_ptr))) {
    _OB_LOG(WARN, "io_submit fail ret=%d", tmp_ret);
    ret = OB_ERR_UNEXPECTED;
  } else {
    file_pos_ += iocb->buffer_pos;
    cur_iocb_ = NULL;
  }
  return ret;
}

ObFileAsyncAppender::AIOCB *ObFileAsyncAppender::get_iocb_()
{
  AIOCB *ret = NULL;
  if (NULL != cur_iocb_) {
    if (AIO_BUFFER_SIZE != cur_iocb_->buffer_pos) {
      ret = cur_iocb_;
    } else {
      if (OB_SUCCESS  == submit_iocb_(cur_iocb_)) {
        pool_.alloc_obj(cur_iocb_, *this);
        ret = cur_iocb_;
      }
    }
  } else {
    pool_.alloc_obj(cur_iocb_, *this);
    ret = cur_iocb_;
  }
  if (NULL != ret
      && NULL == ret->buffer) {
    if (NULL == (ret->buffer = (char *)memalign(align_size_, AIO_BUFFER_SIZE))) {
      _OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "alloc async buffer fail");
      pool_.free_obj(ret);
      cur_iocb_ = NULL;
      ret = NULL;
    } else {
      memset(ret->buffer, 0, AIO_BUFFER_SIZE);
    }
  }
  return ret;
}

void ObFileAsyncAppender::wait()
{
  struct timespec ts;
  ts.tv_sec = AIO_WAIT_TIME_US / 1000000;
  ts.tv_nsec = (AIO_WAIT_TIME_US % 1000000) * 1000;
  struct io_event ioe[1];
  int64_t ret_num = io_getevents(ctx_, 1, 1, ioe, &ts);
  for (int64_t i = 0; i < ret_num; i++) {
    AIOCB *iocb = (AIOCB *)ioe[i].data;
    if (NULL != iocb
        && 0 == ioe[i].res2
        && upper_align(iocb->buffer_pos, align_size_) == (int64_t)ioe[i].res) {
      iocb->buffer_pos = 0;
      pool_.free_obj(iocb);
    } else {
      // Fatal error
      _OB_LOG_RET(ERROR, OB_ERR_SYS, "iocb return fail iocb=%p res=%ld res2=%ld, will set fd=-1, and the fd will leek",
                ioe[i].data, ioe[i].res, ioe[i].res2);
      fd_ = -1;
      cur_iocb_ = NULL;
    }
  }
}
#endif // __USE_AIO_FILE
}
}

