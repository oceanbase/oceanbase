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

#include "common/log/ob_log_data_writer.h"
#include <sys/vfs.h>
#include "lib/file/ob_file.h"
#include "common/log/ob_log_dir_scanner.h"
#include "common/log/ob_log_constants.h"
#include "common/log/ob_log_cursor.h"
#include "common/log/ob_log_generator.h"

namespace oceanbase
{
namespace common
{
ObLogDataWriter::AppendBuffer::AppendBuffer()
    : file_pos_(-1),
      buf_(NULL),
      buf_end_(0),
      buf_limit_(DEFAULT_BUF_SIZE)
{}

ObLogDataWriter::AppendBuffer::~AppendBuffer()
{
  if (NULL != buf_) {
    free(buf_);
    buf_ = NULL;
  }
}

int ObLogDataWriter::AppendBuffer::write(const char *buf, int64_t len, int64_t pos)
{
  int ret = OB_SUCCESS;
  int sys_err = 0;
  int64_t file_pos = 0;
  if (OB_ISNULL(buf) || len < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KP(buf), K(len), K(pos));
  } else if (NULL == buf_
             && 0 != (sys_err = posix_memalign((void **)&buf_,
                                               ObLogConstants::LOG_FILE_ALIGN_SIZE,
                                               buf_limit_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(ERROR, "posix_memalign", "align", LITERAL(ObLogConstants::LOG_FILE_ALIGN_SIZE), 
              "size", buf_limit_, KERRNOMSG(sys_err));
  } else if (len > buf_limit_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "write_size overflow", K(len), K(buf_limit_), K(ret));
  } else {
    file_pos = file_pos_ >= 0 ? file_pos_ : pos;
  }
  if (OB_SUCCESS != ret) {
    // do nothing
  } else if (pos < file_pos || pos > file_pos + buf_end_) {
    ret = OB_DISCONTINUOUS_LOG;
    SHARE_LOG(ERROR, "write error", K(file_pos), K(buf_end_), K(pos), K(len), K(ret));
  } else if (file_pos + buf_limit_ < pos + len) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_LOG(DEBUG, "AppendBuf not ENOUGH: file_pos + buf_limit < pos + len",
              K(file_pos), K(buf_limit_), K(pos), K(len), K(ret));
  } else {
    MEMCPY(buf_ + (pos - file_pos), buf, len);
    buf_end_ = pos + len - file_pos;
    file_pos_ = file_pos;
  }
  return ret;
}

int ObLogDataWriter::AppendBuffer::flush(int fd)
{
  int ret = OB_SUCCESS;
  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", K(fd), K(ret));
  } else if (buf_end_ <= 0) {
    // do nothing
  } else if (unintr_pwrite(fd, buf_, buf_end_, file_pos_) != buf_end_) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "uniter_pwrite error", K(fd), KP(buf_), K(buf_end_),
              K(file_pos_), KERRMSG, K(ret));
  } else {
    file_pos_ = -1;
    buf_end_ = 0;
  }
  return ret;
}

void ObLogDataWriter::AppendBuffer::destroy()
{
  file_pos_ = -1;
  buf_end_ = 0;
  buf_limit_ = DEFAULT_BUF_SIZE;
  if (NULL != buf_) {
    free(buf_);
    buf_ = NULL;
  }
}

ObLogDataWriter::ObLogDataWriter():
    write_buffer_(),
    log_dir_(NULL),
    file_size_(0),
    end_cursor_(),
    log_sync_type_(OB_LOG_SYNC),
    fd_(-1), cur_file_id_(-1),
    num_file_to_add_(-1), min_file_id_(0),
    min_avail_file_id_(-1),
    min_avail_file_id_getter_(NULL)
{
}

ObLogDataWriter::~ObLogDataWriter()
{
  int ret = OB_SUCCESS;
  if (NULL != log_dir_) {
    free((void *)log_dir_);
    log_dir_ = NULL;
  }
  if (fd_ > 0) {
    if (OB_LOG_NOSYNC == log_sync_type_ && OB_FAIL(write_buffer_.flush(fd_))) {
      SHARE_LOG(ERROR, "write_buffer_ flush error", K(fd_), K(ret));
    }
    if (0 != close(fd_)) {
      SHARE_LOG(ERROR, "close error", K(fd_), KERRMSG);
    }
  }
}

int64_t ObLogDataWriter::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  databuff_printf(buf, len, pos, "DataWriter(%s, sync_type=%ld)",
                  to_cstring(end_cursor_), log_sync_type_);
  return pos;
}

int ObLogDataWriter::init(const char *log_dir,
                          const int64_t file_size,
                          int64_t du_percent,
                          const int64_t log_sync_type,
                          MinAvailFileIdGetter *min_avail_file_id_getter // maybe NULL
                         )
{
  int ret = OB_SUCCESS;
  ObLogDirScanner scanner;
  struct statfs fsst;
  if (NULL != log_dir_) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(ERROR, "log dir already exists", K(ret));
  } else if (NULL == log_dir
             || file_size <= 0
             || du_percent < 0
             || du_percent > 100) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", KCSTRING(log_dir), K(file_size), K(du_percent),
              K(min_avail_file_id_getter), K(ret));
  } else if (OB_FAIL(scanner.init(log_dir))) {
    SHARE_LOG(ERROR, "scanner init failed", KP(log_dir), K(ret));
  } else if (OB_FAIL(scanner.get_min_log_id((uint64_t &)min_file_id_))
             && OB_ENTRY_NOT_EXIST != ret) {
    SHARE_LOG(ERROR, "scanner.get_min_log_file_id() failed", K(ret));
  } else {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (0 != statfs(log_dir, &fsst)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "statfs error", KP(log_dir), KERRMSG);
  } else {
    log_dir_ = strdup(log_dir);
    file_size_ = file_size;
    log_sync_type_ = log_sync_type;
    num_file_to_add_ = (int64_t)fsst.f_bsize
                       * ((int64_t)fsst.f_blocks * du_percent / 100LL
                          - (int64_t)(fsst.f_blocks - fsst.f_bavail)
                         ) / file_size;
    min_avail_file_id_getter_ = min_avail_file_id_getter;
    SHARE_LOG(INFO, "log_data_writer init", KCSTRING(log_dir), K(file_size),
              K(fsst.f_bsize * fsst.f_bavail),
              K(fsst.f_bsize * fsst.f_blocks),
              K(du_percent), K(num_file_to_add_));
  }
  return ret;
}

void ObLogDataWriter::destroy()
{
  int ret = OB_SUCCESS;
  if (NULL != log_dir_) {
    free((void *)log_dir_);
    log_dir_ = NULL;
  }
  if (fd_ > 0) {
    if (OB_LOG_NOSYNC == log_sync_type_ && OB_FAIL(write_buffer_.flush(fd_))) {
      SHARE_LOG(ERROR, "write_buffer_ flush error", K(fd_), K(ret));
    }
    if (0 != close(fd_)) {
      SHARE_LOG(ERROR, "close error", K(fd_), KERRMSG);
    }
  }
  write_buffer_.destroy();
  file_size_ = 0;
  end_cursor_.reset();
  log_sync_type_ = OB_LOG_SYNC;
  fd_ = -1;
  cur_file_id_ = -1;
  num_file_to_add_ = -1;
  min_file_id_ = 0;
  min_avail_file_id_ = -1;
  min_avail_file_id_getter_ = NULL;
}

int myfallocate_by_append(int fd, int mode, off_t offset, off_t len)
{
  int ret = 0;
  static char buf[1 << 20] __attribute__((aligned(ObLogConstants::LOG_FILE_ALIGN_SIZE)));
  int64_t count = 0;
  UNUSED(mode);
  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", K(fd), K(ret));
  } else if (offset < 0 || len <= 0) {
    errno = -EINVAL;
    ret = -1;
  }
  for (int64_t pos = offset; 0 == ret && pos < offset + len; pos += count) {
    count = min(offset + len - pos, static_cast<int64_t>(sizeof(buf)));
    if (unintr_pwrite(fd, buf, count, pos) != count) {
      ret = -1;
      SHARE_LOG(ERROR, "uniter_pwrite fail", K(pos), K(count), K(ret));
      break;
    }
  }
  return ret;
}
# if __linux && (__GLIBC__ > 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ >= 10))
int myfallocate(int fd, int mode, off_t offset, off_t len)
{
  int ret = 0;
  static bool syscall_supported = true;
  if (syscall_supported && 0 != (ret = fallocate(fd, mode, offset, len))) {
    syscall_supported = false;
    SHARE_LOG(WARN, "glibc support fallocate(), but fallocate still fail, "
              "fallback to call myfallocate_by_append()",
              KERRMSG);
  }
  if (!syscall_supported) {
    ret = myfallocate_by_append(fd, mode, offset, len);
  }
  return ret;
}
#else
int myfallocate(int fd, int mode, off_t offset, off_t len)
{
  return myfallocate_by_append(fd, mode, offset, len);
}
#endif
int file_expand_by_fallocate(const int fd, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  struct stat st;
  if (fd < 0 || file_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", K(fd), K(file_size), K(ret));
  } else if (0 != fstat(fd, &st)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "fstat failed", K(fd), KERRMSG);
  } else if (0 != (st.st_size & ObLogConstants::LOG_FILE_ALIGN_MASK)
             || 0 != (file_size & ObLogConstants::LOG_FILE_ALIGN_MASK)) {
    ret = OB_LOG_NOT_ALIGN;
    _SHARE_LOG(ERROR, "file_size[%ld] or file_size[%ld] not align by %lx",
               st.st_size, file_size,
               ObLogConstants::LOG_FILE_ALIGN_MASK);
  } else if (st.st_size >= file_size) {
    //do nothing
  } else if (0 != myfallocate(fd, 0, st.st_size, file_size - st.st_size)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "fallocate error", K(fd), KERRMSG);
  } else if (0 != fsync(fd)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "fsync error", K(fd), KERRMSG);
  } else {}
  return ret;
}

// The original size of the file needs to be 512-byte aligned
int file_expand_by_append(const int fd, const int64_t file_size)
{
  int ret = OB_SUCCESS;
  struct stat st;
  static char buf[1 << 20] __attribute__((aligned(ObLogConstants::LOG_FILE_ALIGN_SIZE)));
  int64_t count = 0;
  bool need_fsync = false;
  if (fd < 0 || file_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", K(fd), K(file_size), K(ret));
  } else if (0 != fstat(fd, &st)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "fstat error", K(fd), KERRMSG);
  } else if (0 != (st.st_size & ObLogConstants::LOG_FILE_ALIGN_MASK)
             || 0 != (file_size & ObLogConstants::LOG_FILE_ALIGN_MASK)) {
    ret = OB_LOG_NOT_ALIGN;
    _SHARE_LOG(ERROR, "file_size[%ld] or file_size[%ld] not align by %lx",
               st.st_size, file_size,
               ObLogConstants::LOG_FILE_ALIGN_MASK);
  }
  if (OB_SUCCESS == ret) {
    for (int64_t pos = st.st_size; OB_SUCCESS == ret && pos < file_size; pos += count) {
      need_fsync = true;
      count = min(file_size - pos, static_cast<int64_t>(sizeof(buf)));
      if (unintr_pwrite(fd, buf, count, pos) != count) {
        ret = OB_IO_ERROR;
        SHARE_LOG(ERROR, "uniter_pwrite error", K(pos), K(count));
        break;
      }
    }
  }
  if (OB_SUCCESS == ret && need_fsync && 0 != fsync(fd)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "fsync error", K(fd), KERRMSG);
  }
  return ret;
}

int ObLogDataWriter::write(const ObLogCursor &start_cursor,
                           const ObLogCursor &end_cursor,
                           const char *data, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (NULL == log_dir_) {
    ret = OB_NOT_INIT;
    SHARE_LOG(ERROR, "log data writer is not initialized", K(ret));
  } else if (OB_ISNULL(data)
             || len < 0
             || !start_cursor.is_valid()
             || !end_cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", K(start_cursor), K(end_cursor),
              KP(data), K(len), K(ret));
  } else if (0 != (((uint64_t)data) & ObLogConstants::LOG_FILE_ALIGN_MASK)
             || 0 != (len & ObLogConstants::LOG_FILE_ALIGN_MASK)) {
    ret = OB_LOG_NOT_ALIGN;
    SHARE_LOG(ERROR, "write_log  NOT_ALIGN", KP(data), K(len), K(ret));
  } else if (!start_cursor.equal(end_cursor_)) {
    ret = OB_DISCONTINUOUS_LOG;
  } else if (OB_LOG_NOT_PERSISTENT == log_sync_type_) {
    for (int64_t i = 0; i < 4000; i++) {
      PAUSE();
    }
  } else if (OB_FAIL(prepare_fd(start_cursor.file_id_))) {
    SHARE_LOG(ERROR, "prepare fd error", K(start_cursor.file_id_), K(ret));
  } else if (OB_LOG_NOSYNC == log_sync_type_) {
    if (OB_FAIL(write_buffer_.write(data, len, start_cursor.offset_))
        && OB_BUF_NOT_ENOUGH != ret) {
      SHARE_LOG(ERROR, "writer_buffer_ write error", KP(data), K(len),
                K(start_cursor.offset_), K(ret));
    } else if (OB_SUCCESS == ret) {
      // do nothing
    } else if (OB_FAIL(write_buffer_.flush(fd_))) {
      SHARE_LOG(ERROR, "write_buffer_ flush error", K(fd_), K(ret));
    } else if (OB_FAIL(write_buffer_.write(data, len, start_cursor.offset_))) {
      SHARE_LOG(ERROR, "send write fail", K(start_cursor), KP(data), K(len), K(ret));
    } else {}
  } else {
    STORAGE_REDO_LOG(INFO, "unintr_pwrite begin");
    if (unintr_pwrite(fd_, data, len, start_cursor.offset_) != len) {
      ret = OB_IO_ERROR;
      SHARE_LOG(ERROR, "pwrite error", K(fd_), KP(data), K(len), K(start_cursor),
                KERRMSG, K(ret));
    } else {
      STORAGE_REDO_LOG(INFO, "fsync begin");
      if (0 != fsync(fd_)) {
        ret = OB_IO_ERROR;
        SHARE_LOG(ERROR, "fdatasync error", K(fd_), KERRMSG);
      } else {
        STORAGE_REDO_LOG(INFO, "fsync end");
      }
    }
  }
  if (OB_SUCC(ret)) {
    end_cursor_ = end_cursor;
  }
  return ret;
}

int ObLogDataWriter::prepare_fd(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  char pool_file[OB_MAX_FILE_NAME_LENGTH];
  int64_t count = 0;
  if (file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "invalid argument", K(file_id), K(ret));
  } else if (cur_file_id_ == file_id) {
    if (fd_ < 0) {
      cur_file_id_ = -1;
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(ERROR, "fd < 0", K(fd_), K(file_id), K(ret));
    }
  } else {
    cur_file_id_ = -1;
    if (fd_ >= 0) {
      if (OB_LOG_NOSYNC == log_sync_type_ && OB_FAIL(write_buffer_.flush(fd_))) {
        SHARE_LOG(ERROR, "write_buffer_.flush error", K(fd_), K(ret));
      } else if (NULL != min_avail_file_id_getter_ &&
                 OB_FAIL(file_expand_by_fallocate(fd_, file_size_))) {
        SHARE_LOG(ERROR, "file_expand_by_fallocate error", K(fd_), K(file_size_),
                  KERRMSG);
      }
      if (0 != close(fd_)) {
        ret = OB_IO_ERROR;
        SHARE_LOG(ERROR, "close error", K(fd_), KERRMSG);
      }
    }
    fd_ = -1;
  }
  if (OB_FAIL(ret) || fd_ >= 0) {
    // do nothing
  } else if ((count = snprintf(fname, sizeof(fname), "%s/%ld", log_dir_, file_id)) < 0
             || count >= (int64_t)sizeof(fname)) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_LOG(ERROR, "file name too long", KCSTRING(log_dir_), K(file_id), K(sizeof(fname)));
  } else if ((fd_ = open(fname, OPEN_FLAG, OPEN_MODE)) >= 0) {
    SHARE_LOG(INFO, "file exist, append clog to it", KCSTRING(fname));
  } else if ((NULL == select_pool_file(pool_file, sizeof(pool_file))
              || (fd_ = reuse(pool_file, fname)) < 0)
             && (fd_ = open(fname, CREATE_FLAG, OPEN_MODE)) < 0) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "open file error", KCSTRING(fname), KERRMSG, K(ret));
  }
  if (OB_SUCCESS == ret) {
    cur_file_id_ = file_id;
    if (0 == min_file_id_) {
      min_file_id_ = cur_file_id_;
    }
  }
  return ret;
}

int ObLogDataWriter::reuse(const char *pool_file, const char *fname)
{
  int ret = OB_SUCCESS;
  char tmp_pool_file[OB_MAX_FILE_NAME_LENGTH];
  int64_t len = 0;
  int fd = -1;
  // Open and rename are allowed to fail, but pwrite() is not allowed to fail.
  if (OB_ISNULL(pool_file) || OB_ISNULL(fname)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", KP(pool_file), KP(fname), K(ret));
  } else if ((len = snprintf(tmp_pool_file,
                             sizeof(tmp_pool_file),
                             "%s.tmp", pool_file)) < 0
             || len >= (int64_t)sizeof(tmp_pool_file)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (0 != rename(pool_file, tmp_pool_file)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(WARN, "rename error", KCSTRING(pool_file), KCSTRING(tmp_pool_file), KERRMSG);
  } else if ((fd = open(tmp_pool_file, OPEN_FLAG, OPEN_MODE)) < 0) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "open file failed", KCSTRING(tmp_pool_file), KERRMSG);
  } else if (unintr_pwrite(fd, ObLogGenerator::eof_flag_buf_,
                           sizeof(ObLogGenerator::eof_flag_buf_),
                           0) != sizeof(ObLogGenerator::eof_flag_buf_)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "write_eof_flag fail", KCSTRING(tmp_pool_file), KERRMSG);
  } else if (0 != fsync(fd)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "fdatasync error", KCSTRING(tmp_pool_file), KERRMSG);
  } else if (0 != rename(tmp_pool_file, fname)) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "rename error", KCSTRING(pool_file), KCSTRING(fname), KERRMSG);
  }
  if (OB_SUCCESS != ret && fd > 0) {
    if (0 != close(fd)) {
      SHARE_LOG(ERROR, "close error", K(fd), KERRMSG);
    } else {
      fd = -1;
    }
  }
  return fd;
}

const char *ObLogDataWriter::select_pool_file(char *buf, const int64_t buf_len)
{
  char *result = NULL;
  int64_t len = 0;
  if (OB_ISNULL(buf) || buf_len < 0) {
    SHARE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument", KP(buf), K(buf_len));
  } else if (OB_ISNULL(min_avail_file_id_getter_)) {
    // do nothing
  } else if (0 == min_file_id_) {
    num_file_to_add_--;
    SHARE_LOG(INFO, "min_file_id has not inited, can not reuse file, "
              "will create new file num_file_to_add", K(num_file_to_add_));
  } else if (min_file_id_ < 0) {
    SHARE_LOG_RET(ERROR, OB_ERROR, "min_file_id < 0", K(min_file_id_));
  } else if (num_file_to_add_ > 0) {
    num_file_to_add_--;
    SHARE_LOG(INFO, "num_file_to_add >= 0 will create new file.", K(num_file_to_add_));
  } else if (min_file_id_ > min_avail_file_id_
             && min_file_id_ > (min_avail_file_id_ = min_avail_file_id_getter_->get())) {
    SHARE_LOG_RET(WARN, OB_ERROR, "can not select pool_file", K(min_file_id_), K(min_avail_file_id_));
  } else if ((len = snprintf(buf, buf_len, "%s/%ld", log_dir_, min_file_id_)) < 0
             || len >= buf_len) {
    SHARE_LOG_RET(ERROR, OB_ERROR, "gen fname fail", K(buf_len), KCSTRING(log_dir_), K(min_file_id_));
  } else {
    result = buf;
    min_file_id_++;
  }
  SHARE_LOG(INFO, "select_pool_file", K(num_file_to_add_), K(min_file_id_),
            K(min_avail_file_id_), KCSTRING(result));
  return result;
}

int ObLogDataWriter::check_eof_after_log_cursor(const ObLogCursor &cursor)
{
  int ret = OB_SUCCESS;
  int sys_err = 0;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int64_t fname_len = 0;
  int fd = -1;
  char *buf = NULL;
  int64_t len = ObLogConstants::LOG_FILE_ALIGN_SIZE;
  int64_t read_count = 0;
  if (!cursor.is_valid()) {
    SHARE_LOG(ERROR, "invalid argument", K(cursor), K(ret));
  } else if (0 != (sys_err = posix_memalign((void **)&buf,
                                            ObLogConstants::LOG_FILE_ALIGN_SIZE,
                                            len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(ERROR, "posix_memalign", "align", LITERAL(ObLogConstants::LOG_FILE_ALIGN_SIZE), "size", len,
              KERRNOMSG(sys_err));
  } else if ((fname_len = snprintf(fname, sizeof(fname), "%s/%ld", log_dir_,
                                   cursor.file_id_)) < 0
             || fname_len >= (int64_t)sizeof(fname)) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_LOG(ERROR, "gen fname fail", K(sizeof(fname)), KCSTRING(log_dir_), K(cursor.file_id_));
  } else if ((fd = open(fname, O_RDONLY | O_DIRECT | O_CREAT, OPEN_MODE)) < 0) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "open file failed", KCSTRING(fname), KERRMSG);
  } else if (common::get_file_size(fd) == cursor.offset_) {
    SHARE_LOG(WARN, "no eof mark after log_cursor, but this is the end of file, "
              "maybe replay old version log_file", K(cursor));
  } else if (0 > (read_count = unintr_pread(fd, buf, len, cursor.offset_))) {
    ret = OB_IO_ERROR;
    SHARE_LOG(ERROR, "pread error", KCSTRING(fname), K(fd), KP(buf), K(len),
              K(cursor.offset_), KERRMSG);
  } else if (!ObLogGenerator::is_eof(buf, read_count)) {
    ret = OB_LAST_LOG_RUINNED;
    SHARE_LOG(ERROR, "not follow by eof", K(cursor), K(read_count));
  }

  if (fd >= 0) {
    int tmp_ret = 0;
    if (0 != (tmp_ret = close(fd))) {
      SHARE_LOG(ERROR, "close failed", K(tmp_ret));
    } else {
      fd = -1;
    }
  }
  if (NULL != buf) {
    free(buf);
    buf = NULL;
  }
  return ret;
}

int ObLogDataWriter::start_log(const ObLogCursor &cursor)
{
  int ret = OB_SUCCESS;
  const bool check_eof = false;
  if (!cursor.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(ERROR, "cursor is_invalid", K(cursor), K(ret));
  } else if (end_cursor_.is_valid()) {
    ret = OB_INIT_TWICE;
    SHARE_LOG(ERROR, "ObLogWriter is init already init", K(end_cursor_));
  } else if (0 != (cursor.offset_ & ObLogConstants::LOG_FILE_ALIGN_MASK)) {
    ret = OB_LOG_NOT_ALIGN;
    SHARE_LOG(WARN, "start_log: LOG_NOT_ALIGNED", K(cursor), K(ret));
  } else if (check_eof && OB_FAIL(check_eof_after_log_cursor(cursor))) {
    SHARE_LOG(ERROR, "no eof after cursor, maybe commitlog corrupt",
              K(cursor), K(ret));
    ret = OB_START_LOG_CURSOR_INVALID;
  } else {
    end_cursor_ = cursor;
  }
  return ret;
}

int ObLogDataWriter::reset()
{
  int ret = OB_SUCCESS;
  end_cursor_.reset();
  return ret;
}

int ObLogDataWriter::get_cursor(ObLogCursor &cursor) const
{
  int ret = OB_SUCCESS;
  cursor = end_cursor_;
  return ret;
}
}; // end namespace common
}; // end namespace oceanbase
