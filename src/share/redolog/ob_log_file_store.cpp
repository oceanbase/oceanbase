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

#include <dirent.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include "ob_log_file_store.h"
#include "lib/file/file_directory_utils.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
using namespace clog;
using namespace share;
namespace common {
ObLogFileDescriptor::ObLogFileDescriptor()
    : flag_(0), disk_mgr_(NULL), file_id_(OB_INVALID_FILE_ID), fd_infos_(), is_inited_(false)
{}

ObLogFileDescriptor::~ObLogFileDescriptor()
{
  reset();
}

void ObLogFileDescriptor::reset()
{
  flag_ = 0;
  disk_mgr_ = NULL;
  file_id_ = OB_INVALID_FILE_ID;
  fd_infos_.reset();
  is_inited_ = false;
}

bool ObLogFileDescriptor::is_valid() const
{
  bool b_ret = (ObILogFileStore::is_valid_file_id(file_id_));

  b_ret = fd_infos_.count() > 0 && OB_NOT_NULL(disk_mgr_);
  if (b_ret) {
    for (int32_t i = 0; b_ret && i < fd_infos_.count(); i++) {
      b_ret = fd_infos_[i].is_valid();
    }
  }
  return b_ret;
}

int ObLogFileDescriptor::init(ObLogDiskManager* disk_mgr, const int8_t flag, const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "already inited", K(ret));
  } else if (OB_ISNULL(disk_mgr) || !ObILogFileStore::is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_mgr), K(file_id), K(flag));
  } else if ((flag & READ_FLAG) > 0 && (flag & TEMP_FLAG) > 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid flag", K(ret), K(flag));
  } else {
    disk_mgr_ = disk_mgr;
    flag_ = flag & FLAG_MASK;
    file_id_ = file_id;
    is_inited_ = true;
  }
  return ret;
}

int ObLogFileDescriptor::sync(const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(offset));
  } else {
    int fd_idx = -1;
    int64_t disk_id = -1;
    ObLogDiskManager::BaseDiskIterator iter = disk_mgr_->begin_all();
    for (; OB_SUCC(ret) && disk_mgr_->end_all() != iter; ++iter) {
      disk_id = iter->get_disk_id();
      fd_idx = get_log_fd(disk_id);
      if (!is_valid_state(iter->get_state())) {
        // disk become invalid, remove fd if exist, otherwise do nothing
        if (-1 != fd_idx && OB_FAIL(fd_infos_.remove(fd_idx))) {
          COMMON_LOG(ERROR, "fail to remove bad disk fd", K(ret), K(fd_idx), K(fd_infos_[fd_idx]));
        }
      } else {
        // disk is valid
        if (-1 == fd_idx) {  // add fd if not exist
          ObLogFdInfo new_fd_info;
          if (OB_FAIL(disk_mgr_->sync_system_fd(file_id_, disk_id, is_tmp(), enable_write(), offset, new_fd_info))) {
            COMMON_LOG(ERROR, "fail to refresh fd", K(ret), K(file_id_), K(disk_id));
          } else if (new_fd_info.is_valid() && OB_FAIL(fd_infos_.push_back(new_fd_info))) {
            COMMON_LOG(ERROR, "fail to add new fd", K(ret), K(new_fd_info));
          }
        } else {  // refresh current fd if need
          if (OB_FAIL(disk_mgr_->sync_system_fd(
                  file_id_, disk_id, is_tmp(), enable_write(), offset, fd_infos_.at(fd_idx)))) {
            COMMON_LOG(ERROR, "fail to refresh fd", K(ret), K(file_id_), K(disk_id));
          } else if (!fd_infos_.at(fd_idx).is_valid() && OB_FAIL(fd_infos_.remove(fd_idx))) {
            // disk become invalid during refresh
            COMMON_LOG(ERROR, "fail to remove bad disk fd", K(ret), K(fd_idx), K(fd_infos_[fd_idx]));
          }
        }
      }
    }

    if (OB_SUCC(ret) && 0 == fd_infos_.count()) {
      ret = OB_EMPTY_RESULT;
      COMMON_LOG(WARN, "all disks are bad", K(ret), K(*this));
    }
  }
  return ret;
}

int ObLogFileDescriptor::get_log_fd(const int64_t disk_id)
{
  int idx = -1;
  for (int32_t i = 0; - 1 == idx && i < fd_infos_.count(); i++) {
    if (disk_id == fd_infos_[i].disk_id_) {
      idx = i;
    }
  }
  return idx;
}

bool ObLogFileDescriptor::is_valid_state(const ObLogDiskState state)
{
  bool valid = true;
  if (enable_read() && OB_LDS_GOOD != state) {
    valid = false;
  } else if (enable_write() && OB_LDS_GOOD != state && OB_LDS_RESTORE != state) {
    valid = false;
  }
  return valid;
}

int ObLogFileReader::get_fd(
    const char* log_dir, const int64_t file_id, const ObRedoLogType log_type, ObLogFileDescriptor& log_fd)
{
  int ret = OB_SUCCESS;
  ObLogDiskManager* disk_mgr = NULL;
  char fname[MAX_PATH_SIZE] = {};
  int fd = OB_INVALID_FD;
  log_fd.reset();

  if (!ObILogFileStore::is_valid_file_id(file_id) || OB_REDO_TYPE_INVALID == log_type || OB_ISNULL(log_dir)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(file_id), K(log_type), KP(log_dir));
  } else {
    if (NULL == (disk_mgr = ObLogDiskManager::get_disk_manager(log_type))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "can't get disk manager", K(ret), K(log_type));
    } else if (OB_FAIL(log_fd.init(disk_mgr, ObLogFileDescriptor::READ_FLAG, file_id))) {
      COMMON_LOG(ERROR, "log fd init fail", K(ret), K(file_id));
    } else if (OB_FAIL(log_fd.sync())) {
      COMMON_LOG(WARN, "log fd sync fail", K(ret), K(log_fd));
    } else {
      COMMON_LOG(DEBUG, "inner open fd", K(log_fd));
    }
  }
  return ret;
}

int64_t ObLogFileReader::pread(ObLogFileDescriptor& log_fd, void* buf, const int64_t count, const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t read_size = -1;
  if (!log_fd.is_valid() || count <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), K(log_fd), K(count), K(offset));
  } else {
    if (OB_FAIL(log_fd.sync())) {
      COMMON_LOG(ERROR, "log fd sync fail", K(ret), K(log_fd));
    } else {
      for (int32_t i = 0; read_size < 0 && i < log_fd.count(); i++) {
        if (-1 == log_fd.fd_infos_[i].fd_) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "invalid fd.", K(ret), K(log_fd.fd_infos_[i]));
        } else {
          read_size = ob_pread(log_fd.fd_infos_[i].fd_, (char*)buf, count, offset);
          if (-1 == read_size) {
            ret = OB_IO_ERROR;
            COMMON_LOG(WARN, "ob_pread fail.", K(ret), K(log_fd.fd_infos_[i]), K(errno), KERRMSG);
          }
        }
      }
    }
  }
  return read_size;
}

int ObLogFileReader::close_fd(ObLogFileDescriptor& log_fd)
{
  int ret = OB_SUCCESS;
  if (!log_fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(log_fd));
  }

  if (OB_FAIL(ret)) {
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < log_fd.count(); i++) {
      if (-1 != log_fd.fd_infos_.at(i).fd_ && 0 != ::close(log_fd.fd_infos_.at(i).fd_)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "close file failed ", K(ret), K(log_fd.fd_infos_.at(i)));
      }
    }
    log_fd.reset();
  }
  return ret;
}

/*static*/ bool ObILogFileStore::is_valid_file_id(int64_t file_id)
{
  return (file_id > 0 && file_id < OB_INVALID_FILE_ID);
}

int ObILogFileStore::format_file_path(
    char* buf, const int64_t size, const char* log_dir, const int64_t file_id, const bool is_tmp)
{
  int ret = OB_SUCCESS;
  int pret = 0;

  if (OB_ISNULL(buf) || size <= 0 || OB_ISNULL(log_dir) || !is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(buf), KP(log_dir), K(size), K(file_id));
  } else if (STRLEN(log_dir) <= 0 || STRLEN(log_dir) >= size) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid log_dir", K(ret), K(log_dir), K(size));
  } else {
    pret = snprintf(buf, size, "%s/%ld%s", log_dir, file_id, is_tmp ? TMP_SUFFIX : "\0");
    if (pret <= 0 || pret >= size) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(WARN, "file name too long", K(ret), K(file_id), K(log_dir));
    }
  }
  return ret;
}

ObLogFileStore::ObLogFileStore() : is_inited_(false), disk_mgr_(NULL), write_fd_(), io_ctx_(NULL)
{
  for (int32_t i = 0; i < MAX_DISK_COUNT; i++) {
    memset(&io_reqs_[i], 0, sizeof(io_reqs_[i]));
    io_req_ptrs_[i] = NULL;
  }

  for (int32_t i = 0; i < MAX_IO_COUNT; i++) {
    memset(&io_events_[i], 0, sizeof(io_events_[i]));
  }
}

ObLogFileStore::~ObLogFileStore()
{
  destroy();
}

int ObLogFileStore::init(const char* log_dir, const int64_t file_size, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "already inited", K(ret));
  } else {
    switch (type) {
      case CLOG_WRITE_POOL:
        log_type_ = OB_REDO_TYPE_CLOG;
        break;
      case ILOG_WRITE_POOL:
        log_type_ = OB_REDO_TYPE_ILOG;
        break;
      case SLOG_WRITE_POOL:
        log_type_ = OB_REDO_TYPE_SLOG;
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid write pool type", K(ret), K(type));
        break;
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (disk_mgr_ = ObLogDiskManager::get_disk_manager(log_type_))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "fail to get disk manager", K(ret), K_(log_type));
    } else if (OB_FAIL(disk_mgr_->init(log_dir, file_size))) {
      COMMON_LOG(ERROR, "init mul_file_pool_ failed", K(ret));
    } else {
      MEMSET(&io_ctx_, 0, sizeof(io_ctx_));
      if (0 != (ret = ob_io_setup(MAX_IO_COUNT, &io_ctx_))) {
        COMMON_LOG(ERROR, "io_setup fail", K(ret), K(errno));
        ret = OB_IO_ERROR;
      } else {
        for (int32_t i = 0; i < MAX_DISK_COUNT; i++) {
          io_req_ptrs_[i] = &(io_reqs_[i]);
        }
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObLogFileStore::open(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id));
  } else if (write_fd_.is_valid()) {
    ret = OB_FILE_ALREADY_EXIST;
    COMMON_LOG(WARN, "previous file still opened ", K(ret), K_(write_fd));
  } else if (OB_FAIL(inner_open(file_id, ObLogFileDescriptor::WRITE_FLAG, write_fd_))) {
    COMMON_LOG(ERROR, "inner open write fd fail", K(ret), K_(write_fd));
  }
  return ret;
}

int ObLogFileStore::inner_open(const int64_t file_id, const int8_t flag, ObLogFileDescriptor& log_fd)
{
  int ret = OB_SUCCESS;
  log_fd.reset();
  if (OB_FAIL(log_fd.init(disk_mgr_, flag, file_id))) {
    COMMON_LOG(ERROR, "log fd init fail", K(ret), K(file_id));
  } else if (OB_FAIL(log_fd.sync())) {
    COMMON_LOG(WARN, "log fd sync fail", K(ret), K(log_fd));
  }

  if (OB_FAIL(ret)) {
    log_fd.reset();
  } else {
    COMMON_LOG(INFO, "inner open fd", K(log_fd));
  }
  return ret;
}

int ObLogFileStore::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!write_fd_.is_valid()) {
    ret = OB_FILE_NOT_OPENED;
    COMMON_LOG(WARN, "no file has opened ", K(ret), K_(write_fd));
  } else if (OB_FAIL(inner_close(write_fd_))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(WARN, "close file failed ", K(ret), K_(write_fd));
  } else {
    write_fd_.reset();
  }
  return ret;
}

int ObLogFileStore::write(void* buf, int64_t count, int64_t offset)
{
  int ret = OB_SUCCESS;
  const int64_t fd_cnt = write_fd_.count();
  int64_t new_req_cnt = 0;
  int64_t submitted = 0;
  int64_t retry_cnt = 0;
  bool need_retry = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(buf) || count <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(buf), K(count), K(offset));
  } else if (!write_fd_.is_valid()) {
    ret = OB_FILE_NOT_OPENED;
    COMMON_LOG(WARN, "no file has opened ", K(ret), K_(write_fd), K(fd_cnt));
  } else if (OB_FAIL(write_fd_.sync(offset))) {
    COMMON_LOG(ERROR, "log fd sync fail", K(ret), K_(write_fd));
  } else if (OB_FAIL(prepare_write_info(buf, count, offset))) {
    COMMON_LOG(ERROR, "prepare io info fail", K(ret));
  } else {
    while (need_retry) {
      ret = OB_SUCCESS;
      new_req_cnt = 0;
      if (OB_FAIL(process_io_prep_pwrite(submitted, new_req_cnt))) {
        COMMON_LOG(ERROR, "prepare io requests fail", K(ret), K(new_req_cnt), K(submitted), K(retry_cnt), K_(write_fd));
      } else if (OB_FAIL(process_io_submit(io_ctx_, io_req_ptrs_, new_req_cnt, submitted))) {
        COMMON_LOG(ERROR, "process io submit fail", K(ret), K(new_req_cnt), K(submitted), K(retry_cnt), K_(write_fd));
      } else if (OB_FAIL(process_io_getevents(submitted, io_ctx_, io_events_))) {
        COMMON_LOG(ERROR, "process get events fail", K(ret), K(new_req_cnt), K(submitted), K(retry_cnt), K_(write_fd));
      }
      need_retry = process_retry(ret, retry_cnt);
    }

    // whatever success or failure, reset write requests, check and mark bad disk
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = process_failed_write())) {
      COMMON_LOG(WARN, "fail to process uncompleted write", K(tmp_ret));
    }

    if (OB_SUCCESS == tmp_ret) {  // at least 1 disk success, ignore previous ret
      ret = OB_SUCCESS;
    } else {
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObLogFileStore::read(void* buf, int64_t count, int64_t offset, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(buf) || count <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(buf), K(count), K(offset));
  } else if (!write_fd_.is_valid()) {
    ret = OB_FILE_NOT_OPENED;
    COMMON_LOG(WARN, "no file has opened ", K(ret), K_(write_fd));
  }

  if (OB_SUCC(ret)) {
    int aio_ret = 0;
    int64_t event_sz = 0;
    char* rd_buf = NULL;
    int64_t rd_size = 0;
    int64_t rd_offset = 0;
    int64_t event_res = 0;
    int retry = 0;
    struct timespec timeout;

    for (int32_t i = 0; OB_SUCC(ret) && event_sz < count && i < write_fd_.count(); i++) {
      event_sz = 0;
      retry = 0;
      while (OB_SUCC(ret) && event_sz < count && retry < MAX_IO_RETRY) {
        retry++;
        timeout.tv_sec = AIO_TIMEOUT_SECOND;
        timeout.tv_nsec = 0;
        rd_buf = reinterpret_cast<char*>(buf) + event_sz;
        rd_size = count - event_sz;
        rd_offset = offset + event_sz;
        io_prep_pread(io_req_ptrs_[0], write_fd_.fd_infos_[i].fd_, rd_buf, rd_size, rd_offset);
        if (1 != (aio_ret = ob_io_submit(io_ctx_, 1, &(io_req_ptrs_[0])))) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "io_submit fail", K(aio_ret), K(write_fd_.file_id_), K(ret));
        } else if (1 != (aio_ret = ob_io_getevents(io_ctx_, 1, 1, io_events_, &timeout))) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "io_getevents mismatch ", K(aio_ret), K(write_fd_.file_id_), K(ret));
        } else {
          aio_ret = static_cast<int>(io_events_[0].res2);
          event_res = static_cast<int64_t>(io_events_[0].res);
          if (0 == aio_ret && event_res == rd_size) {  // full complete
            event_sz += rd_size;
          } else if (0 == aio_ret && event_res == 0) {  // read nothing from file
            ret = OB_READ_NOTHING;
          } else if (0 == aio_ret && event_res > 0 && event_res < rd_size &&
                     (0 == event_res % DIO_ALIGN_SIZE)) {  // partial complete
            event_sz += event_res;
            COMMON_LOG(INFO, "re-submit read", K(i), K(event_res), K(rd_size), K(event_sz), K(count));
          } else {
            ret = OB_IO_ERROR;
            COMMON_LOG(ERROR,
                "read error ",
                K(i),
                K(event_res),
                K(aio_ret),
                K(errno),
                KERRNOMSG(errno),
                K(rd_size),
                K(event_sz),
                K(count),
                K(offset));
          }
        }
      }

      if (OB_FAIL(ret) && OB_READ_NOTHING != ret) {
        COMMON_LOG(ERROR, "read fail on disk ", K(ret), K(i), K(event_sz), K(write_fd_.file_id_));
        ret = OB_SUCCESS;  // try next disk
      }
    }

    // all data read or meet end of file
    if ((OB_SUCC(ret) && event_sz == count) || OB_READ_NOTHING == ret) {
      read_size = event_sz;
      ret = OB_SUCCESS;
    } else {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "read fail on all disk ", K(ret), K(buf), K(count), K(offset), K(errno));
    }
  }

  return ret;
}

void ObLogFileStore::destroy()
{
  if (write_fd_.is_valid()) {
    inner_close(write_fd_);
  }
  if (OB_NOT_NULL(io_ctx_)) {
    ob_io_destroy(io_ctx_);
  }
  disk_mgr_ = NULL;
  is_inited_ = false;
}

int ObLogFileStore::fsync()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!write_fd_.is_valid()) {
    ret = OB_FILE_NOT_OPENED;
    COMMON_LOG(WARN, "no file has opened ", K(ret), K_(write_fd));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < write_fd_.count(); i++) {
      if (0 != ::fsync(write_fd_.fd_infos_[i].fd_)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "fsync file failed ", K(ret), K(i), K(write_fd_.fd_infos_[i]), K(errno));
      }
    }
  }
  return ret;
}

int ObLogFileStore::write_file(const int64_t file_id, void* buf, const int64_t count)
{
  int ret = OB_SUCCESS;
  int close_ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id) || OB_ISNULL(buf) || count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id), KP(buf), K(count));
  } else if (OB_FAIL(
                 inner_open(file_id, ObLogFileDescriptor::WRITE_FLAG | ObLogFileDescriptor::TEMP_FLAG, write_fd_))) {
    COMMON_LOG(ERROR, "open tmp file failed", K(ret), K(file_id));
  } else if (OB_FAIL(write(buf, count, 0))) {
    COMMON_LOG(ERROR, "write file failed", K(ret), KP(buf), K(count), K(file_id), K(errno), KERRMSG);
  }
  // ensure file is closed
  if (write_fd_.is_valid() && (OB_SUCCESS != (close_ret = inner_close(write_fd_)))) {
    COMMON_LOG(ERROR, "close file fail", K(ret), K(close_ret), K_(write_fd));
  }
#ifdef ERRSIM
  ret = E(EventTable::EN_CLOG_DUMP_ILOG_MEMSTORE_RENAME_FAILURE) OB_SUCCESS;
#endif
  // only rename file after all succeed
  if (OB_SUCC(ret) && OB_SUCCESS == close_ret && OB_FAIL(rename(file_id))) {
    COMMON_LOG(ERROR, "rename tmp file failed ", K(ret), K(close_ret), K(file_id), K(errno));
  }
  ret = OB_SUCC(ret) ? close_ret : ret;
  return ret;
}

int ObLogFileStore::rename(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  char tmp_file[OB_MAX_FILE_NAME_LENGTH];
  char dest_file[OB_MAX_FILE_NAME_LENGTH];
  int tmp_n = 0;
  int dest_n = 0;
  int succ_cnt = 0;

  ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr_->begin();
  for (; OB_SUCC(ret) && disk_mgr_->end() != iter; ++iter) {
    tmp_n = snprintf(tmp_file, sizeof(tmp_file), "%s/%ld%s", iter->get_disk_path(), file_id, TMP_SUFFIX);
    dest_n = snprintf(dest_file, sizeof(dest_file), "%s/%ld", iter->get_disk_path(), file_id);
    if (tmp_n <= 0 || tmp_n >= sizeof(tmp_file) || dest_n <= 0 || dest_n >= sizeof(dest_file)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(WARN, "file name too long, ", K(ret), K(file_id), "dir_path", iter->get_disk_path());
    } else if (0 != ::rename(tmp_file, dest_file)) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "rename file failed, ", K(ret), K(tmp_file), K(dest_file), K(errno));
    } else {
      succ_cnt++;
    }

    // do next disk
    if (OB_FAIL(ret)) {
      if (OB_FAIL(disk_mgr_->set_bad_disk(iter->get_disk_id()))) {
        COMMON_LOG(WARN, "set bad disk fail", K(ret), K(iter->get_disk_id()));
      }
    }
  }

  if (0 == succ_cnt) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "all disk failed", K(ret));
  }
  return ret;
}

int ObLogFileStore::delete_all_files()
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  bool is_exist = false;
  uint32_t min_file_id = OB_INVALID_FILE_ID;
  uint32_t max_file_id = OB_INVALID_FILE_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(disk_mgr_->get_file_id_range(min_file_id, max_file_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // dir is empty
      ret = OB_SUCCESS;
    } else {
      COMMON_LOG(WARN, "get file id range fail.", K(ret));
    }
  } else {
    for (uint32_t file_id = min_file_id; OB_INVALID_FILE_ID != file_id && file_id <= max_file_id; ++file_id) {
      ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr_->begin();
      for (; OB_SUCC(ret) && disk_mgr_->end() != iter; ++iter) {
        n = snprintf(fname, sizeof(fname), "%s/%u", iter->get_disk_path(), file_id);
        if (n <= 0 || n >= sizeof(fname)) {
          ret = OB_BUF_NOT_ENOUGH;
          COMMON_LOG(ERROR, "file name too long, ", K(ret), K(file_id), "dir_path", iter->get_disk_path());
        } else if (OB_FAIL(FileDirectoryUtils::is_exists(fname, is_exist))) {
          COMMON_LOG(ERROR, "check file failed, ", K(ret), K(fname));
        } else if (is_exist && (0 != ::unlink(fname))) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "unlink file failed", K(ret), K(fname), K(errno));
        } else {
          COMMON_LOG(INFO, "unlink file success", K(ret), K(fname));
        }
      }
    }
  }

  return ret;
}

int ObLogFileStore::delete_file(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  bool is_exist = false;
  int succ_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id));
  } else {
    ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr_->begin();
    for (; OB_SUCC(ret) && disk_mgr_->end() != iter; ++iter) {
      n = snprintf(fname, sizeof(fname), "%s/%ld", iter->get_disk_path(), file_id);
      if (n <= 0 || n >= sizeof(fname)) {
        ret = OB_BUF_NOT_ENOUGH;
        COMMON_LOG(ERROR, "file name too long, ", K(ret), K(file_id), "dir_path", iter->get_disk_path());
      } else if (OB_FAIL(FileDirectoryUtils::is_exists(fname, is_exist))) {
        COMMON_LOG(ERROR, "check file failed, ", K(ret), K(fname));
      } else if (is_exist && (0 != ::unlink(fname))) {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "unlink file failed", K(ret), K(fname), K(errno));
      } else {
        succ_cnt++;
      }

      // do next disk
      if (OB_FAIL(ret)) {
        if (OB_FAIL(disk_mgr_->set_bad_disk(iter->get_disk_id()))) {
          COMMON_LOG(WARN, "set bad disk fail", K(ret), K(iter->get_disk_id()));
        }
      }
    }

    if (0 == succ_cnt) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(ERROR, "all disk failed", K(ret));
    }
  }

  return ret;
}

int ObLogFileStore::ftruncate(const int64_t file_id, const int64_t length)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  bool is_exist = false;
  int succ_cnt = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id) || length <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id), K(length));
  } else {
    ObLogDiskManager::ReadWriteDiskIterator iter = disk_mgr_->begin();
    for (; OB_SUCC(ret) && disk_mgr_->end() != iter; ++iter) {
      n = snprintf(fname, sizeof(fname), "%s/%ld", iter->get_disk_path(), file_id);
      if (n <= 0 || n >= sizeof(fname)) {
        ret = OB_BUF_NOT_ENOUGH;
        COMMON_LOG(ERROR, "file name too long, ", K(ret), K(file_id), "dir_path", iter->get_disk_path());
      } else if (OB_FAIL(FileDirectoryUtils::is_exists(fname, is_exist))) {
        COMMON_LOG(ERROR, "check file failed, ", K(ret), K(fname));
      } else if (is_exist && (0 != ::truncate(fname, length))) {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "truncate file failed", K(ret), K(fname), K(length), K(errno));
      } else {
        succ_cnt++;
      }

      // do next disk
      if (OB_FAIL(ret)) {
        if (OB_FAIL(disk_mgr_->set_bad_disk(iter->get_disk_id()))) {
          COMMON_LOG(WARN, "set bad disk fail", K(ret), K(iter->get_disk_id()));
        }
      }
    }

    if (0 == succ_cnt) {
      ret = OB_ENTRY_NOT_EXIST;
      COMMON_LOG(ERROR, "all disk failed", K(ret));
    }
  }
  return ret;
}

int ObLogFileStore::exist(const int64_t file_id, bool& is_exist) const
{
  int ret = OB_SUCCESS;
  is_exist = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id));
  } else if (OB_FAIL(disk_mgr_->exist(file_id, is_exist))) {
    COMMON_LOG(WARN, "check file exist from disk mgr fail", K(ret), K(file_id));
  }
  return ret;
}

int ObLogFileStore::get_file_st_size(const int64_t file_id, int64_t& st_size) const
{
  int ret = OB_SUCCESS;
  struct stat file_stat;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id));
  } else if (OB_FAIL(this->fstat(file_id, &file_stat))) {
    COMMON_LOG(WARN, "stat fail", K(ret), K(file_id));
  } else {
    st_size = static_cast<int64_t>(file_stat.st_size);
  }

  return ret;
}

int ObLogFileStore::get_file_st_time(const int64_t file_id, time_t& st_time) const
{
  int ret = OB_SUCCESS;
  struct stat file_stat;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id));
  } else if (OB_FAIL(this->fstat(file_id, &file_stat))) {
    COMMON_LOG(WARN, "stat fail", K(ret), K(file_id));
  } else {
    st_time = file_stat.st_mtime;
  }

  return ret;
}

int ObLogFileStore::fstat(const int64_t file_id, struct stat* file_stat) const
{
  int ret = OB_SUCCESS;

  if (file_id <= 0 || OB_ISNULL(file_stat)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments ", K(ret), K(file_id), KP(file_stat));
  } else if (OB_FAIL(disk_mgr_->fstat(file_id, file_stat))) {
    if (OB_FILE_NOT_EXIST == ret) {
      COMMON_LOG(WARN, "file not exist", K(ret), K(file_id), KP(file_stat));
    } else {
      COMMON_LOG(ERROR, "fstat fail", K(ret), K(file_id), KP(file_stat));
    }
  }

  return ret;
}

int ObLogFileStore::get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(disk_mgr_->get_file_id_range(min_file_id, max_file_id)) && OB_ENTRY_NOT_EXIST != ret) {
    COMMON_LOG(WARN, "get file id range fail.", K(ret));
  }
  return ret;
}

int ObLogFileStore::get_total_used_size(int64_t& total_size) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    ret = disk_mgr_->get_total_used_size(total_size);
  }
  return ret;
}

void ObLogFileStore::update_max_file_id(const uint32_t file_id)
{
  if (ObRedoLogType::OB_REDO_TYPE_ILOG != log_type_) {
    COMMON_LOG(ERROR, "only ilog interface can call update_max_file_id", K(log_type_), K(file_id));
  } else {
    disk_mgr_->update_max_file_id(file_id);
  }
}

const char* ObLogFileStore::get_dir_name() const
{
  int ret = OB_SUCCESS;
  const char* dir_name = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
    dir_name = "";
  } else {
    dir_name = disk_mgr_->get_dir_name();
  }

  return dir_name;
}

int ObLogFileStore::get_total_disk_space(int64_t& total_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(disk_mgr_->get_total_disk_space(total_space))) {
    COMMON_LOG(ERROR, "get total disk space fail", K(ret));
  }
  return ret;
}

int ObLogFileStore::inner_close(ObLogFileDescriptor& log_fd)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < log_fd.count(); i++) {
    if (-1 != log_fd.fd_infos_.at(i).fd_ && 0 != ::close(log_fd.fd_infos_.at(i).fd_)) {
      ret = OB_IO_ERROR;
      COMMON_LOG(WARN, "close file failed ", K(ret), K(log_fd.fd_infos_.at(i)));
    }
  }
  log_fd.reset();
  return ret;
}

int ObLogFileStore::prepare_write_info(void* buf, int64_t count, int64_t offset)
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < write_fd_.count(); i++) {
    if (!pending_wr_[i].complete_) {  // defense code
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "previous IO not complete ", K(ret), K(pending_wr_[i]));
    } else {
      pending_wr_[i].disk_id_ = write_fd_.fd_infos_[i].disk_id_;
      pending_wr_[i].buf_ = reinterpret_cast<char*>(buf);
      pending_wr_[i].fd_ = write_fd_.fd_infos_[i].fd_;
      pending_wr_[i].size_ = count;
      pending_wr_[i].offset_ = offset;
      pending_wr_[i].complete_ = false;
      pending_wr_[i].ret_ = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogFileStore::process_io_prep_pwrite(const int64 submitted, int64_t& req_cnt)
{
  int ret = OB_SUCCESS;
  req_cnt = 0;
  for (int32_t i = 0; i < MAX_DISK_COUNT; i++) {
    if (pending_wr_[i].complete_) {
      // request succeed, do nothing
    } else {
      // if 0 == submitted, means no flying events, all uncompleted IO need prepare
      // otherwise, means already waiting some events, only prepare those retriable ones
      if (0 == submitted || (submitted > 0 && OB_EAGAIN == pending_wr_[i].ret_)) {
        io_prep_pwrite(io_req_ptrs_[req_cnt],
            pending_wr_[i].fd_,
            pending_wr_[i].buf_,
            pending_wr_[i].size_,
            pending_wr_[i].offset_);
        io_req_ptrs_[req_cnt]->data = reinterpret_cast<void*>(&(pending_wr_[i]));
        req_cnt++;
      }
    }
  }
  return ret;
}

int ObLogFileStore::process_io_submit(io_context_t ctx, struct iocb** requests, const int64_t cnt, int64_t& submitted)
{
  int ret = OB_SUCCESS;
  int64_t new_submitted = 0;
  if (cnt <= 0) {  // defense code
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid argument", K(ret), K(cnt), K(write_fd_.file_id_));
  } else {
    while (0 > (new_submitted = ob_io_submit(ctx, cnt, requests)) && EAGAIN == errno) {
      COMMON_LOG(WARN, "retry io_submit, sleep", K(ret), K(write_fd_.file_id_), K(errno), KERRMSG);
      sleep(AIO_RETRY_INTERVAL_US);
    }

    if (new_submitted < 0) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "io_submit fail", K(ret), K(cnt), K(submitted), K(write_fd_.file_id_), K(errno), KERRMSG);
    } else {
      submitted += new_submitted;
    }

    if (submitted > 0) {
      // ignore error as we still have flying event
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLogFileStore::process_io_getevents(int64_t& submitted, io_context_t ctx, struct io_event* events)
{
  int ret = OB_SUCCESS;
  bool partial_write = false;
  int aio_ret = 0;
  int64_t event_res = 0;
  ObLogFileIOInfo* wr_info = NULL;
  int gotten = 0;
  struct timespec timeout;

  while (submitted > 0 && OB_SUCC(ret) && !partial_write) {
    timeout.tv_sec = (OB_REDO_TYPE_CLOG == log_type_ ? CLOG_AIO_TIMEOUT_SECOND : AIO_TIMEOUT_SECOND);
    timeout.tv_nsec = 0;
    if (0 >= (gotten = ob_io_getevents(ctx, 1, submitted, events, &timeout))) {
      // timeout or io error
      if (0 == gotten) {
        COMMON_LOG(WARN,
            "io_getevents timeout",
            K(ret),
            K(gotten),
            K(submitted),
            K(write_fd_.file_id_),
            LITERAL_K(AIO_TIMEOUT_SECOND));
      } else {
        ret = OB_IO_ERROR;
        COMMON_LOG(
            ERROR, "io_getevents fail", K(ret), K(gotten), K(submitted), K(write_fd_.file_id_), K(errno), KERRMSG);
      }
    } else {
      submitted -= gotten;
      for (int32_t i = 0; i < gotten; i++) {
        aio_ret = static_cast<int>(events[i].res2);
        event_res = static_cast<int64_t>(events[i].res);
        wr_info = reinterpret_cast<ObLogFileIOInfo*>(events[i].data);
        if (0 == aio_ret && event_res == wr_info->size_) {  // full complete
          wr_info->complete_ = true;
          wr_info->ret_ = OB_SUCCESS;
        } else if (0 == aio_ret && event_res > 0 && event_res < wr_info->size_ &&
                   (0 == event_res % DIO_ALIGN_SIZE)) {  // partial complete
          wr_info->buf_ = wr_info->buf_ + event_res;
          wr_info->size_ -= event_res;
          wr_info->offset_ += event_res;
          wr_info->complete_ = false;
          wr_info->ret_ = OB_EAGAIN;
          partial_write = true;
          COMMON_LOG(WARN, "re-submit", K(wr_info->ret_), K(i), K(event_res), K(*wr_info));
        } else {  // fail write, check if can retry
          wr_info->complete_ = (-EAGAIN != aio_ret);
          wr_info->ret_ = (-EAGAIN == aio_ret)   ? OB_EAGAIN
                          : (-ENOSPC == aio_ret) ? OB_CS_OUTOF_DISK_SPACE
                                                 : OB_IO_ERROR;
          partial_write = (-EAGAIN == aio_ret) ? true : partial_write;
          COMMON_LOG(WARN, "write error", K(wr_info->ret_), K(i), K(event_res), K(*wr_info), K(aio_ret));
        }
      }
    }
  }

  return ret;
}

bool ObLogFileStore::process_retry(const int result, int64_t& retry)
{
  int ret = OB_SUCCESS;
  bool b_ret = false;
  if (OB_FAIL(result) || ++retry >= MAX_IO_RETRY) {
    // other fatal error or reach maximum retry count
    b_ret = false;
  } else {
    for (int32_t i = 0; !b_ret && i < MAX_DISK_COUNT; i++) {  // check if all request complete
      if (!pending_wr_[i].complete_) {
        b_ret = true;
      }
    }
  }

  if (b_ret) {
    COMMON_LOG(WARN, "sleep for retry", K(ret), K(retry));
    usleep(AIO_RETRY_INTERVAL_US);
  }
  return b_ret;
}

int ObLogFileStore::process_failed_write()
{
  int ret = OB_SUCCESS;
  const int64_t fd_cnt = write_fd_.count();
  int32_t succ_cnt = 0;
  int last_fail_ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < fd_cnt; i++) {
    if (!pending_wr_[i].complete_ || OB_SUCCESS != pending_wr_[i].ret_) {
      COMMON_LOG(WARN, "IO not complete or failed on disk, mark bad", K(pending_wr_[i]));
      last_fail_ret = pending_wr_[i].ret_;
      if (OB_FAIL(disk_mgr_->set_bad_disk(pending_wr_[i].disk_id_))) {
        COMMON_LOG(ERROR, "fail to set bad disk", K(ret), K(pending_wr_[i]));
      }
    } else {
      ++succ_cnt;
    }
    // whatever write succeed or fail, reset write request
    pending_wr_[i].reset();
  }

  // there exists disk failed
  if (OB_SUCC(ret) && fd_cnt != succ_cnt) {
    if (OB_FAIL(write_fd_.sync())) {
      COMMON_LOG(ERROR, "fail to sync fd", K(ret), K_(write_fd));
    }
  }

  if (OB_SUCC(ret) && 0 == succ_cnt) {
    if (OB_CS_OUTOF_DISK_SPACE == last_fail_ret) {
      ret = OB_CS_OUTOF_DISK_SPACE;
    } else {
      ret = OB_IO_ERROR;
    }
    COMMON_LOG(ERROR, "write on all disk failed", K(ret), K(fd_cnt));
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase
