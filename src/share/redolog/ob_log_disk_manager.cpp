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

#include "ob_log_disk_manager.h"
#include "lib/thread/thread_mgr.h"
#include "lib/checksum/ob_crc64.h"
#include "share/ob_thread_mgr.h"
#include <sys/statfs.h>
#include "common/log/ob_log_dir_scanner.h"
#include "share/ob_thread_define.h"
#include "clog/ob_log_file_pool.h"

namespace oceanbase {
using namespace clog;
namespace common {
bool ObLogFdInfo::is_valid() const
{
  return fd_ >= 0 && disk_id_ >= 0 && disk_id_ < ObLogDiskManager::MAX_DISK_COUNT && file_id_ >= 0 &&
         file_id_ != OB_INVALID_FILE_ID && refresh_time_ > 0;
}

int ObLogDiskInfo::init(const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "already init", K(ret));
  } else if (OB_ISNULL(log_dir) || file_size <= 0 || ObLogWritePoolType::INVALID_WRITE_POOL == type) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(log_dir), K(file_size), K(type));
  } else if (OB_FAIL(log_dir_.init(log_dir))) {
    COMMON_LOG(ERROR, "init ObLogDir fail", K(ret), K(log_dir));
  } else if (OB_FAIL(file_pool_.init(&log_dir_, file_size, type))) {
    COMMON_LOG(ERROR, "init ObLogWriteFilePool fail", K(ret), K(log_dir));
  } else if (STRLEN(log_dir) > OB_MAX_FILE_NAME_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "dir path too long", K(ret), K(log_dir));
  } else {
    set_disk_path(log_dir);
    set_state(OB_LDS_NEW);
    is_inited_ = true;
  }
  return ret;
}

void ObLogDiskInfo::reuse()
{
  set_state(OB_LDS_INVALID);
  set_disk_path("\0");
  restore_start_file_id_ = -1;
  restore_start_offset_ = -1;
  file_pool_.destroy();
  log_dir_.destroy();
  is_inited_ = false;
}

void ObLogDiskInfo::restore_start(const int64_t file_id, const int64_t offset)
{
  // Currently write API is all single thread, but this function is designed for multiple threads
  // Only the first thread successfully set file id can continue setting the offset
  if (OB_LDS_RESTORE == get_state() && file_id >= 0 && offset >= 0 &&
      -1 == ATOMIC_CAS(&restore_start_file_id_, -1, file_id)) {
    COMMON_LOG(INFO, "restore start", K(file_id), K(offset));
    ATOMIC_STORE(&restore_start_offset_, offset);
  }
}

int ObLogDiskManager::MonitorTask::init(ObLogDiskManager* disk_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(disk_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(disk_mgr));
  } else {
    disk_mgr_ = disk_mgr;
  }
  return ret;
}

void ObLogDiskManager::MonitorTask::runTimerTask()
{
  if (OB_ISNULL(disk_mgr_)) {
    COMMON_LOG(ERROR, "ObLogDiskManager is not inited");

  } else {
    disk_mgr_->run_monitor_task();
  }
}

void ObLogDiskManager::LogRestoreProgress::reset()
{
  length_ = sizeof(LogRestoreProgress);
  version_ = LogRestoreProgress::DATA_VERSION;
  catchup_complete_ = 0;
  copy_complete_ = 0;
  catchup_file_id_ = -1;
  catchup_offset_ = -1;
  copy_start_file_id_ = -1;
  copied_file_id_ = -1;
  checksum_ = 0;
}

int ObLogDiskManager::LogRestoreProgress::check()
{
  int ret = OB_SUCCESS;
  if (sizeof(LogRestoreProgress) != length_ || LogRestoreProgress::DATA_VERSION != version_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid length or version", K(ret), K(*this));
  } else {
    const int64_t checksum = ob_crc64(&length_, get_serialize_size() - sizeof(checksum_));
    if (checksum != checksum_) {
      ret = OB_CHECKSUM_ERROR;
      COMMON_LOG(ERROR, "checksum error", K(ret), K(checksum), K(*this));
    }
  }
  return ret;
}

int ObLogDiskManager::LogRestoreProgress::serialize(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (buf_len <= 0) || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid argument", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    checksum_ = ob_crc64(&length_, get_serialize_size() - sizeof(checksum_));
    LogRestoreProgress* ptr = reinterpret_cast<LogRestoreProgress*>(buf + pos);
    ptr->checksum_ = checksum_;
    ptr->length_ = length_;
    ptr->version_ = version_;
    ptr->catchup_complete_ = catchup_complete_;
    ptr->copy_complete_ = copy_complete_;
    ptr->catchup_file_id_ = catchup_file_id_;
    ptr->catchup_offset_ = catchup_offset_;
    ptr->copy_start_file_id_ = copy_start_file_id_;
    ptr->copied_file_id_ = copied_file_id_;
    pos += get_serialize_size();
  }
  return ret;
}

int ObLogDiskManager::LogRestoreProgress::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    const LogRestoreProgress* ptr = reinterpret_cast<const LogRestoreProgress*>(buf + pos);
    checksum_ = ptr->checksum_;
    length_ = ptr->length_;
    version_ = ptr->version_;
    catchup_complete_ = ptr->catchup_complete_;
    copy_complete_ = ptr->copy_complete_;
    catchup_file_id_ = ptr->catchup_file_id_;
    catchup_offset_ = ptr->catchup_offset_;
    copy_start_file_id_ = ptr->copy_start_file_id_;
    copied_file_id_ = ptr->copied_file_id_;
    pos += get_serialize_size();

    if (OB_FAIL(check())) {
      COMMON_LOG(ERROR, "deserialize fail", K(ret), K(*this));
    }
  }
  return ret;
}

int64_t ObLogDiskManager::LogRestoreProgress::get_serialize_size(void) const
{
  return sizeof(LogRestoreProgress);
}

ObLogDiskManager::ObLogDiskManager()
    : is_inited_(false),
      file_size_(0),
      total_disk_space_(0),
      pool_type_(ObLogWritePoolType::INVALID_WRITE_POOL),
      init_lock_(),
      tg_id_(-1),
      monitor_task_(),
      log_copy_buffer_(NULL),
      rst_pro_buffer_(NULL),
      rst_pro_(),
      rst_fd_(-1)
{
  MEMSET(log_dir_, 0, sizeof(log_dir_));
  for (int32_t i = 0; i < MAX_DISK_COUNT; i++) {
    disk_slots_[i].reuse();
    // disk id is also the index of array, it is decided during construction
    disk_slots_[i].set_disk_id(i);
  }
}

ObLogDiskManager::~ObLogDiskManager()
{
  destroy();
}

int ObLogDiskManager::init(const char* log_dir, const int64_t file_size)
{
  UNUSED(log_dir);
  UNUSED(file_size);
  return OB_NOT_SUPPORTED;
}

int ObLogDiskManager::init(const char* log_dir, const int64_t file_size, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    COMMON_LOG(INFO, "already inited", K(ret));
  } else if (OB_ISNULL(log_dir) || file_size <= 0 || ObLogWritePoolType::INVALID_WRITE_POOL == type) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(log_dir), K(file_size), K(type));
  } else if (STRLEN(log_dir) >= OB_MAX_FILE_NAME_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "log dir name too long", K(ret), KP(log_dir));
  } else {
    ObSpinLockGuard guard(init_lock_);
    if (IS_NOT_INIT) {
      ObMemAttr attr(OB_SERVER_TENANT_ID, ObModIds::OB_LOG_DISK_MANAGER);
      rst_fd_ = -1;
      if (NULL ==
          (log_copy_buffer_ = static_cast<char*>(ob_malloc_align(DIO_READ_ALIGN_SIZE, LOG_COPY_BUF_SIZE, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "alloc log copy buffer fail", K(ret), KP_(log_copy_buffer));
      } else if (NULL == (rst_pro_buffer_ = static_cast<char*>(
                              ob_malloc_align(DIO_ALIGN_SIZE, RESTORE_PROGRESS_BUF_SIZE, attr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(ERROR, "alloc restore progress buffer fail", K(ret), KP_(rst_pro_buffer));
      } else if (OB_FAIL(detect_disks(log_dir, file_size, type))) {
        COMMON_LOG(ERROR, "detect disks failed", K(ret), K(log_dir));
      } else if (OB_FAIL(single_disk_check(log_dir, file_size, type))) {
        COMMON_LOG(ERROR, "single disk check fail", K(ret), K(log_dir));
      } else if (OB_FAIL(startup())) {
        COMMON_LOG(ERROR, "startup disks fail", K(ret));
      } else if (OB_FAIL(monitor_task_.init(this))) {
        COMMON_LOG(ERROR, "init monitor task fail", K(ret));
      } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::LogDiskMon, tg_id_))) {
        COMMON_LOG(ERROR, "tg create failed", K(ret));
      } else if (OB_FAIL(TG_START(tg_id_))) {
        COMMON_LOG(ERROR, "init timer fail", K(ret));
      } else if (OB_FAIL(get_total_disk_space_(total_disk_space_))) {
        COMMON_LOG(ERROR, "failed to get total_disk_space", K(ret));
      } else {
        STRNCPY(log_dir_, log_dir, sizeof(log_dir_) - 1);
        file_size_ = file_size;
        pool_type_ = type;
        if (OB_FAIL(TG_SCHEDULE(tg_id_,
                monitor_task_,
                MONITOR_TASK_INTERVAL_US,
                /*repeat*/ true))) {
          COMMON_LOG(ERROR, "schedule monitor task fail", K(ret));
        } else {
          COMMON_LOG(INFO, "schedule monitor task succeed", K(total_disk_space_), K(file_size_), K(pool_type_));
          is_inited_ = true;
        }
      }
    }

    if (IS_NOT_INIT) {
      destroy();
    }
  }
  return ret;
}

int ObLogDiskManager::sync_system_fd(const int64_t file_id, const int64_t disk_id, const bool is_tmp,
    const bool enable_write, const int64_t offset, ObLogFdInfo& fd_info)
{
  int ret = OB_SUCCESS;
  int tmp_fd = -1;
  int64_t disk_timestamp = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (file_id <= 0 || OB_INVALID_FILE_ID == file_id || disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(file_id), K(disk_id));
  } else if (!enable_write && is_tmp) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "unexpected read temp file case", K(ret), K(enable_write), K(is_tmp));
  } else if (fd_info.is_valid() && (fd_info.disk_id_ != disk_id || fd_info.file_id_ != file_id)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "log fd info not match", K(ret), K(disk_id), K(file_id), K(fd_info));
  } else if (fd_info.is_valid() && fd_info.refresh_time_ >= disk_slots_[disk_id].get_state_timestamp()) {
    // fd info are up-to-date, do nothing
  } else {
    // get timestamp first and then open fd. This is to ensure that state change from other
    // thread won't overwrite timestamp and can be aware later.
    disk_timestamp = disk_slots_[disk_id].get_state_timestamp();
    if (fd_info.fd_ > 0) {
      if (OB_LDS_GOOD == disk_slots_[disk_id].get_state()) {
        // when a RESTORE disk finish catching up log files, its state become GOOD and timestamp
        // be updated. There's no need to open new fd.
        COMMON_LOG(INFO, "disk state RESTORE become GOOD", K(disk_id), K(file_id), K(fd_info), K(disk_slots_[disk_id]));
        tmp_fd = fd_info.fd_;
      } else {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "unexpected scenario", K(ret), K(disk_id), K(file_id), K(fd_info), K(disk_slots_[disk_id]));
      }
    } else if (enable_write && !is_tmp) {  // clog write, may reuse old log file
      if (OB_FAIL(disk_slots_[disk_id].get_file_pool()->get_fd((uint32_t)file_id, tmp_fd))) {
        COMMON_LOG(ERROR, "file pool get fd fail", K(ret), K(file_id));
      }
    } else if (enable_write && is_tmp) {  // ilog write tmp file
      if (OB_FAIL(inner_open_fd(disk_id, file_id, OPEN_FLAG_WRITE, is_tmp, tmp_fd))) {
        COMMON_LOG(ERROR, "open fd fail", K(ret), K(enable_write), K(is_tmp));
      }
    } else if (!enable_write && !is_tmp) {  // normal read
      if (OB_FAIL(inner_open_fd(disk_id, file_id, OPEN_FLAG_READ, is_tmp, tmp_fd))) {
        COMMON_LOG(WARN, "open fd fail", K(ret), K(enable_write), K(is_tmp));
      }
    }

    if (OB_FAIL(ret)) {
      // post check disk state, disk may already become bad
      if (OB_LDS_BAD == disk_slots_[disk_id].get_state()) {
        ret = OB_SUCCESS;
        fd_info.reset();
        COMMON_LOG(WARN, "disk become bad", K(ret), K(disk_id));
      } else if (enable_write && OB_FAIL(set_bad_disk(disk_id))) {
        COMMON_LOG(ERROR, "fail set bad disk", K(ret), K(disk_id));
      } else {
        ret = OB_SUCCESS;
        fd_info.reset();
      }
    } else {
      if (enable_write) {
        // update file id and offset for log catch up
        disk_slots_[disk_id].restore_start(file_id, offset);
      }
      fd_info.disk_id_ = disk_id;
      fd_info.file_id_ = file_id;
      fd_info.fd_ = tmp_fd;
      fd_info.refresh_time_ = disk_timestamp;
    }
  }

  return ret;
}

void ObLogDiskManager::destroy()
{
  if (-1 != tg_id_) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
  }
  tg_id_ = -1;
  monitor_task_.destroy();
  for (int32_t i = 0; i < MAX_DISK_COUNT; i++) {
    disk_slots_[i].reuse();
    disk_slots_[i].set_disk_id(i);
  }
  log_dir_[0] = '\0';
  file_size_ = 0;
  total_disk_space_ = 0;
  pool_type_ = ObLogWritePoolType::INVALID_WRITE_POOL;
  if (OB_NOT_NULL(log_copy_buffer_)) {
    ob_free_align(log_copy_buffer_);
    log_copy_buffer_ = NULL;
  }
  if (OB_NOT_NULL(rst_pro_buffer_)) {
    ob_free_align(rst_pro_buffer_);
    rst_pro_buffer_ = NULL;
  }
  rst_pro_.reset();
  if (rst_fd_ >= 0) {
    ::close(rst_fd_);
    rst_fd_ = -1;
  }
  is_inited_ = false;
}

int ObLogDiskManager::set_bad_disk(const int64_t disk_id)
{
  int ret = OB_SUCCESS;
  bool has_good_disk = false;
  const char* disk_path = NULL;
  char bad_disk_name[OB_MAX_FILE_NAME_LENGTH];
  int tmp_n = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id));
  } else if (disk_id != disk_slots_[disk_id].get_disk_id()) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "target disk not found", K(ret), K(disk_id));
  } else {
    for (int32_t i = 0; !has_good_disk && i < MAX_DISK_COUNT; ++i) {
      if (i != disk_id && i == disk_slots_[i].get_disk_id() && OB_LDS_GOOD == disk_slots_[i].get_state()) {
        has_good_disk = true;
      }
    }

    if (!has_good_disk && OB_LDS_GOOD == disk_slots_[disk_id].get_state()) {
      // Skip set bad state for single disk mode OR the last disk in multiple disk mode.
      // For restore thread, disk state couldn't be GOOD so that it won't be skipped.
      COMMON_LOG(INFO, "skip setting bad disk", K(disk_slots_[disk_id]), K(has_good_disk));
    } else {
      if (OB_LDS_BAD != disk_slots_[disk_id].get_state()) {
        disk_path = disk_slots_[disk_id].get_disk_path();
        tmp_n = snprintf(bad_disk_name, sizeof(bad_disk_name), "%s%s", disk_path, BAD_DISK_SUFFIX);
        if (tmp_n <= 0 || tmp_n > OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_BUF_NOT_ENOUGH;
          COMMON_LOG(ERROR, "disk dir too long, ", K(ret), K(disk_path));
        } else if (0 != ::rename(disk_path, bad_disk_name)) {
          if (ENOENT == errno) {
            // disk already set to bad by other thread
            ret = OB_SUCCESS;
          } else {
            ret = OB_IO_ERROR;
            COMMON_LOG(ERROR, "rename fail", K(ret), K(bad_disk_name), K(errno), KERRMSG);
          }
        }
      }
      // whatever rename succeed, mark state bad
      disk_slots_[disk_id].set_state(OB_LDS_BAD);
      COMMON_LOG(INFO, "disk set to bad", K(disk_slots_[disk_id]), K(has_good_disk));
    }
  }

  return ret;
}

int ObLogDiskManager::detect_disks(const char* log_dir, const int64_t file_size, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  int func_ret = 0;

  DIR* plog_dir = opendir(log_dir);
  if (OB_ISNULL(plog_dir)) {
    func_ret = mkdir(log_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    if (0 != func_ret && EEXIST != errno) {
      ret = OB_ERROR;
      COMMON_LOG(ERROR, "mkdir error", K(ret), K(log_dir), KERRMSG);
    } else {
      plog_dir = opendir(log_dir);
      if (OB_ISNULL(plog_dir)) {
        ret = OB_ERROR;
        COMMON_LOG(ERROR, "opendir error", K(ret), K(log_dir), KERRMSG);
      }
    }
  }

  // scan disk dir
  if (OB_SUCC(ret)) {
    struct dirent entry;
    memset(&entry, 0x00, sizeof(entry));
    struct dirent* pentry = &entry;
    char fname[OB_MAX_FILE_NAME_LENGTH];
    int n = 0;
    func_ret = ::readdir_r(plog_dir, pentry, &pentry);
    while (OB_SUCC(ret) && 0 == func_ret && NULL != pentry) {
      if (is_valid_disk_entry(pentry)) {
        n = snprintf(fname, sizeof(fname), "%s/%s", log_dir, pentry->d_name);
        if (n < 0 || n > sizeof(fname)) {
          ret = OB_BUF_NOT_ENOUGH;
          COMMON_LOG(ERROR, "file name too long", K(ret), K(log_dir), "d_name", pentry->d_name);
        } else if (OB_FAIL(add_disk(fname, file_size, type))) {
          COMMON_LOG(ERROR, "add disk failed", K(ret));
        }
      }
      if (0 > (func_ret = ::readdir_r(plog_dir, pentry, &pentry))) {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "read dir fail", K(ret), K(log_dir), K(errno), KERRMSG);
      }
    }
  }
  if (0 > (func_ret = ::closedir(plog_dir))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(ERROR, "closedir error", K(ret), K(log_dir), K(errno), KERRMSG);
  }
  return ret;
}

int ObLogDiskManager::single_disk_check(
    const char* log_dir, const int64_t file_size, const clog::ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  int64_t total_disk = 0;
  for (int32_t i = 0; i < MAX_DISK_COUNT; i++) {
    if (OB_LDS_NEW == disk_slots_[i].get_state()) {
      total_disk++;
    }
  }
  if (0 == total_disk) {
    // fallback to single disk
    COMMON_LOG(INFO, "single log disk", K(log_dir));
    if (OB_FAIL(add_disk(log_dir, file_size, type))) {
      COMMON_LOG(ERROR, "add disk failed", K(ret), K(log_dir));
    }
  } else {
    COMMON_LOG(INFO, "multiple log disks", K(total_disk));
  }
  return ret;
}

int ObLogDiskManager::add_disk(const char* log_dir, const int64_t file_size, const ObLogWritePoolType type)
{
  int ret = OB_SUCCESS;
  int disk_index = 0;
  bool is_exist = false;

  // find first empty slot
  // assume log_dir must be a valid disk name
  for (; disk_index < MAX_DISK_COUNT; disk_index++) {
    if (0 == STRCMP(log_dir, disk_slots_[disk_index].get_disk_path())) {
      is_exist = true;
      break;
    } else if (OB_LDS_INVALID == disk_slots_[disk_index].get_state()) {
      // If a disk was set to BAD just before add_disk, but acquired log_dir was still the
      // original name which hasn't ".bad" prefix, no worry, it won't be added back again
      // because the state is BAD.
      COMMON_LOG(INFO, "find disk slot", K(log_dir), K(disk_index));
      break;
    }
  }

  if (is_exist) {
    // disk already loaded, do nothing
  } else if (disk_index >= MAX_DISK_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(ERROR, "too many disk", K(ret), K(log_dir), K(disk_index));
  } else if (OB_FAIL(disk_slots_[disk_index].init(log_dir, file_size, type))) {
    COMMON_LOG(ERROR, "init log disk info fail", K(ret));
  } else {
    COMMON_LOG(INFO, "find new disk", K(disk_slots_[disk_index]));
  }
  return ret;
}

bool ObLogDiskManager::is_valid_disk_entry(struct dirent* pentry) const
{
  bool b_ret = false;
  bool is_bad_disk = false;
  bool is_valid_prefix = false;
  if (OB_ISNULL(pentry)) {
    b_ret = false;
  } else {
    if (STRLEN(pentry->d_name) > STRLEN(BAD_DISK_SUFFIX)) {
      is_bad_disk = (0 == STRCMP(pentry->d_name + STRLEN(pentry->d_name) - STRLEN(BAD_DISK_SUFFIX), BAD_DISK_SUFFIX));
    }
    if (STRLEN(pentry->d_name) >= STRLEN(VALID_DISK_PREFIX)) {
      is_valid_prefix = (0 == STRNCMP(pentry->d_name, VALID_DISK_PREFIX, STRLEN(VALID_DISK_PREFIX)));
    }
    b_ret = !is_bad_disk && is_valid_prefix && (pentry->d_type == DT_DIR || pentry->d_type == DT_LNK);
  }
  return b_ret;
}

bool ObLogDiskManager::is_tmp_filename(const char* filename) const
{
  bool b_ret = false;
  if (OB_ISNULL(filename)) {
    b_ret = false;
  } else if (STRLEN(filename) < STRLEN(TMP_SUFFIX)) {
    b_ret = false;
  } else {
    b_ret = (0 == STRCMP(filename + STRLEN(filename) - STRLEN(TMP_SUFFIX), TMP_SUFFIX));
  }
  return b_ret;
}

const ObLogDiskInfo* ObLogDiskManager::get_first_good_disk() const
{
  const ObLogDiskInfo* ret_ptr = NULL;
  for (int32_t i = 0; OB_ISNULL(ret_ptr) && i < MAX_DISK_COUNT; i++) {
    if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
      ret_ptr = &disk_slots_[i];
    }
  }
  return ret_ptr;
}

int ObLogDiskManager::startup()
{
  int ret = OB_SUCCESS;
  ObSEArray<DirScanResult, MAX_DISK_COUNT> scan_results;
  int64_t min_log_id = OB_INVALID_FILE_ID;
  int64_t max_log_id = 0;
  int64_t src_disk_id = -1;
  int64_t good_disk_cnt = 0;

  if (OB_FAIL(clear_tmp_files())) {
    COMMON_LOG(ERROR, "fail to clear tmp files", K(ret));
  } else if (OB_FAIL(get_disk_file_range(scan_results, min_log_id, max_log_id, src_disk_id))) {
    COMMON_LOG(ERROR, "fail to get disks file range", K(ret));
  } else if (OB_FAIL(conform_disk_files(scan_results, min_log_id, max_log_id, src_disk_id))) {
    COMMON_LOG(ERROR, "fail to conform disk files", K(ret));
  }

  if (OB_SUCC(ret)) {
    // The initial state of a disk is NEW
    // Any incomplete disks will be set to INVALID state
    // Any restoring disks will be set to RESTORE state
    // Any failed disks during startup will be set to BAD state
    // After all, NEW state disks has full log files and ready to change state to GOOD.
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_NEW == disk_slots_[i].get_state()) {
        disk_slots_[i].set_state(OB_LDS_GOOD);
        good_disk_cnt++;
      }
    }
    if (0 == good_disk_cnt) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "no disk is good.", K(ret));
    }
  }

  return ret;
}

int ObLogDiskManager::clear_tmp_files()
{
  int ret = OB_SUCCESS;
  int io_ret = 0;
  DIR* plog_dir = NULL;
  struct dirent entry;
  struct dirent* pentry = NULL;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;

  for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
    if (OB_LDS_NEW == disk_slots_[i].get_state()) {
      plog_dir = NULL;
      memset(&entry, 0x00, sizeof(entry));
      pentry = &entry;
      if (OB_ISNULL(plog_dir = opendir(disk_slots_[i].get_disk_path()))) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "open disk dir fail", K(disk_slots_[i]));
      } else if (0 != (io_ret = readdir_r(plog_dir, pentry, &pentry))) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "readdir_r fail", K(disk_slots_[i]));
      } else {
        while (OB_SUCC(ret) && NULL != pentry) {
          if (is_tmp_filename(pentry->d_name)) {
            n = snprintf(fname, sizeof(fname), "%s/%s", disk_slots_[i].get_disk_path(), pentry->d_name);
            if (n < 0 || n > sizeof(fname)) {
              ret = OB_BUF_NOT_ENOUGH;
              COMMON_LOG(WARN, "file name too long", K(ret), K(disk_slots_[i]), "d_name", pentry->d_name);
            } else if (0 > ::unlink(fname) && ENOENT != errno) {
              ret = OB_IO_ERROR;
              COMMON_LOG(WARN, "delete tmp file fail", K(ret), K(fname), K(errno), KERRMSG);
            } else {
              COMMON_LOG(INFO, "delete tmp file", K(fname));
            }
          }
          if (OB_SUCC(ret) && 0 != (io_ret = readdir_r(plog_dir, pentry, &pentry))) {
            ret = OB_IO_ERROR;
            COMMON_LOG(WARN, "readdir_r fail", K(disk_slots_[i]));
          }
        }
      }

      if (OB_NOT_NULL(plog_dir)) {
        // ignore ret, close the dir
        ::closedir(plog_dir);
        plog_dir = NULL;
      }

      if (OB_FAIL(ret)) {
        if (OB_FAIL(set_bad_disk(disk_slots_[i].get_disk_id()))) {
          COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(disk_slots_[i]));
        }
      }
    }
  }
  return ret;
}

int ObLogDiskManager::get_disk_file_range(
    ObIArray<DirScanResult>& results, int64_t& min_log_id, int64_t& max_log_id, int64_t& src_disk_id)
{
  int ret = OB_SUCCESS;
  ObLogDirScanner scanner;
  bool is_restoring = false;
  DirScanResult tmp_res;
  uint64_t scan_min_id = 0;
  uint64_t scan_max_id = 0;
  min_log_id = OB_INVALID_FILE_ID;
  max_log_id = 0;
  src_disk_id = -1;
  // scan each NEW disk log files
  for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
    if (OB_LDS_NEW == disk_slots_[i].get_state()) {
      scanner.reset();
      scan_min_id = 0;
      scan_max_id = 0;
      // Check whether previous log sync task complete and scan the log file range
      if (OB_FAIL(is_log_restoring(disk_slots_[i].get_disk_id(), is_restoring))) {
        COMMON_LOG(ERROR, "check log restore fail", K(ret), K(disk_slots_[i]));
      } else if (OB_FAIL(scanner.init(disk_slots_[i].get_disk_path())) && OB_DISCONTINUOUS_LOG != ret) {
        COMMON_LOG(WARN, "scan disk fail", K(ret), K(disk_slots_[i]));
        if (OB_FAIL(set_bad_disk(disk_slots_[i].get_disk_id()))) {
          COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(disk_slots_[i]));
        }
      } else {
        tmp_res.disk_id_ = disk_slots_[i].get_disk_id();
        tmp_res.continuous_ = (OB_SUCCESS == ret);
        tmp_res.has_log_ = scanner.has_log();
        tmp_res.is_restoring_ = is_restoring;
        scanner.get_min_log_id(scan_min_id);
        scanner.get_max_log_id(scan_max_id);
        tmp_res.min_log_id_ = (int64_t)scan_min_id;
        tmp_res.max_log_id_ = (int64_t)scan_max_id;
        if (OB_FAIL(results.push_back(tmp_res))) {
          COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(disk_slots_[i]));
        } else if (tmp_res.continuous_ && !tmp_res.is_restoring_) {
          // find src disk
          src_disk_id = (tmp_res.max_log_id_ > max_log_id) || (-1 == src_disk_id) ? tmp_res.disk_id_ : src_disk_id;
          if (tmp_res.has_log_) {
            max_log_id = MAX(tmp_res.max_log_id_, max_log_id);
            min_log_id = MIN(tmp_res.min_log_id_, min_log_id);
            if ((max_log_id == tmp_res.max_log_id_ && min_log_id != tmp_res.min_log_id_) ||
                (max_log_id != tmp_res.max_log_id_ && min_log_id == tmp_res.min_log_id_)) {
              // defense code
              ret = OB_ERR_UNEXPECTED;
              COMMON_LOG(ERROR, "incomplete log id range", K(ret), K(tmp_res), K(max_log_id), K(min_log_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogDiskManager::conform_disk_files(const ObIArray<DirScanResult>& results, const int64_t min_log_id,
    const int64_t max_log_id, const int64_t src_disk_id)
{
  int ret = OB_SUCCESS;
  if (0 == results.count()) {
    ret = OB_IO_ERROR;
    COMMON_LOG(ERROR, "all disk scan fail", K(ret));
  } else if (-1 == src_disk_id) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "no disk contains complete logs", K(ret));
  } else if (0 == max_log_id) {
    for (int32_t i = 0; OB_SUCC(ret) && i < results.count(); i++) {  // defense code
      if (results.at(i).has_log_) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "not all disks are empty", K(ret), K(results.at(i)));
      }
    }
    // disks are empty, do nothing
    COMMON_LOG(INFO, "all disk are empty", "disk_cnt", results.count());
  } else if (OB_FAIL(tackle_incomplete_disk(results, min_log_id, max_log_id, src_disk_id))) {
    COMMON_LOG(ERROR, "fail to tackle incomplete disks", K(ret));
  } else if (OB_FAIL(clone_max_log_file(results, max_log_id, src_disk_id))) {
    COMMON_LOG(ERROR, "fail to clone max log file", K(ret));
  }
  return ret;
}

int ObLogDiskManager::tackle_incomplete_disk(const ObIArray<DirScanResult>& results, const int64_t min_log_id,
    const int64_t max_log_id, const int64_t src_disk_id)
{
  int ret = OB_SUCCESS;
  // empty src disk is considered in conform_disk_files, here it must contain log files
  for (int32_t i = 0; OB_SUCC(ret) && i < results.count(); i++) {
    if (src_disk_id != results.at(i).disk_id_) {
      if (results.at(i).is_restoring_) {
        disk_slots_[results.at(i).disk_id_].set_state(OB_LDS_RESTORE);
        COMMON_LOG(INFO, "find restoring disk", K(disk_slots_[results.at(i).disk_id_]));
      } else if (!results.at(i).continuous_ || max_log_id != results.at(i).max_log_id_) {
        COMMON_LOG(WARN, "purge disk dir", K(results.at(i)), K(min_log_id), K(max_log_id), K(src_disk_id));
        if (results.at(i).has_log_ && OB_FAIL(purge_disk_dir(results.at(i).disk_id_, min_log_id, max_log_id))) {
          COMMON_LOG(WARN, "purge disk dir fail", K(ret), K(results.at(i)));
        }

        if (OB_FAIL(ret)) {
          if (OB_FAIL(set_bad_disk(results.at(i).disk_id_))) {
            COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(results.at(i)));
          }
        } else if (OB_FAIL(set_invalid_disk(results.at(i).disk_id_))) {
          COMMON_LOG(ERROR, "set invalid disk fail", K(ret), K(results.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObLogDiskManager::purge_disk_dir(const int64_t disk_id, const int64_t min_log_id, const int64_t max_log_id)
{
  int ret = OB_SUCCESS;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT || max_log_id <= 0 || min_log_id <= 0 || max_log_id < min_log_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id), K(max_log_id), K(min_log_id));
  } else if (disk_id != disk_slots_[disk_id].get_disk_id() || 0 >= STRLEN(disk_slots_[disk_id].get_disk_path()) ||
             OB_LDS_NEW != disk_slots_[disk_id].get_state()) {  // defense code
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "invalid disk", K(ret), K(disk_id), K(disk_slots_[disk_id]));
  } else {
    for (int64_t file_id = min_log_id; OB_SUCC(ret) && file_id <= max_log_id; file_id++) {
      if (OB_FAIL(inner_unlink(disk_id, file_id, false))) {
        COMMON_LOG(ERROR, "unlink file failed", K(ret), K(disk_id), K(file_id));
      }
    }
  }
  return ret;
}

int ObLogDiskManager::set_invalid_disk(const int64_t disk_id)
{
  int ret = OB_SUCCESS;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id));
  } else {
    disk_slots_[disk_id].reuse();
  }
  return ret;
}

int ObLogDiskManager::clone_max_log_file(
    const ObIArray<DirScanResult>& results, const int64_t max_log_id, const int64_t src_disk_id)
{
  int ret = OB_SUCCESS;
  ObLogFdInfo tmp_fd_info;
  ObLogFdInfo src_fd;
  src_fd.disk_id_ = src_disk_id;
  src_fd.file_id_ = max_log_id;
  src_fd.refresh_time_ = ObTimeUtility::current_time();

  // open all target disks' file descriptor
  ObSEArray<ObLogFdInfo, MAX_DISK_COUNT> target_fds;
  for (int32_t i = 0; OB_SUCC(ret) && i < results.count(); i++) {
    // for now, all valid target disks' state must be OB_LDS_NEW or OB_LDS_RESTORE
    tmp_fd_info.reset();
    if (src_disk_id != results.at(i).disk_id_ && OB_LDS_NEW == disk_slots_[results.at(i).disk_id_].get_state()) {
      if (OB_FAIL(inner_open_fd(results.at(i).disk_id_, max_log_id, OPEN_FLAG_WRITE, true, tmp_fd_info.fd_))) {
        COMMON_LOG(ERROR, "open target fd fail", K(ret), K(max_log_id), K(results.at(i)));
      } else {
        tmp_fd_info.disk_id_ = results.at(i).disk_id_;
        tmp_fd_info.file_id_ = max_log_id;
        if (OB_FAIL(target_fds.push_back(tmp_fd_info))) {
          COMMON_LOG(ERROR, "push back fd fail", K(ret), K(tmp_fd_info));
        } else {
          COMMON_LOG(INFO, "get target fd", K(tmp_fd_info));
        }
      }

      if (OB_FAIL(ret)) {
        if (OB_FAIL(set_bad_disk(results.at(i).disk_id_))) {
          COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(results.at(i)));
        }
      }
    }
  }

  if (OB_SUCC(ret) && target_fds.count() > 0) {
    if (OB_FAIL(inner_open_fd(src_disk_id, max_log_id, OPEN_FLAG_READ, false, src_fd.fd_))) {
      COMMON_LOG(WARN, "open src file fail", K(ret), K(src_disk_id), K(max_log_id));
    } else if (OB_FAIL(copy_file_content(src_fd, target_fds, 0))) {
      COMMON_LOG(ERROR, "copy files fail", K(ret), K(max_log_id));
    } else if (OB_LDS_BAD == disk_slots_[src_disk_id].get_state()) {
      ret = OB_ERR_SYS;
      COMMON_LOG(ERROR, "src disk become BAD", K(ret), K(disk_slots_[src_disk_id]));
    } else {
      for (int32_t i = 0; OB_SUCC(ret) && i < target_fds.count(); i++) {
        if (OB_FAIL(remove_tmp_suffix(target_fds[i].disk_id_, max_log_id))) {
          COMMON_LOG(WARN, "rename max log file fail", K(ret), K(target_fds[i]));
          if (OB_FAIL(set_bad_disk(target_fds[i].disk_id_))) {
            COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(target_fds[i]));
          }
        }
      }
    }
  }

  // close fd, ignore return
  for (int32_t i = 0; i < target_fds.count(); i++) {
    if (target_fds[i].fd_ >= 0) {
      ::close(target_fds[i].fd_);
    }
  }
  if (src_fd.fd_ >= 0) {
    ::close(src_fd.fd_);
  }
  return ret;
}

int ObLogDiskManager::copy_file_content(
    const ObLogFdInfo src_fd, const ObIArray<ObLogFdInfo>& targets, const int64_t file_offset)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t read_len = 0;
  int64_t write_len = 0;
  int64_t offset = 0;
  // CLOG and ILOG DIO aligned size are all CLOG_DIO_ALIGN_SIZE
  // SLOG DIO aligned size is DIO_ALIGN_SIZE
  int64_t align_size = SLOG_WRITE_POOL == pool_type_ ? DIO_ALIGN_SIZE : CLOG_DIO_ALIGN_SIZE;

  if (!src_fd.is_valid() || file_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid argument", K(ret), K(src_fd), K(file_offset));
  } else if (targets.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "target disk fds is empty", K(ret));
  } else if (0 != (file_offset % align_size)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "file offset is not aligned", K(ret), K_(pool_type), K(file_offset), K(align_size));
  } else {
    count = (0 == file_offset) ? LOG_COPY_BUF_SIZE : MIN(file_offset - offset, LOG_COPY_BUF_SIZE);
    while (OB_SUCC(ret) && (0 == file_offset || offset < file_offset) &&
           (read_len = ob_pread(src_fd.fd_, log_copy_buffer_, count, offset)) > 0) {
      for (int32_t i = 0; OB_SUCC(ret) && i < targets.count(); i++) {
        if ((OB_LDS_NEW == disk_slots_[targets.at(i).disk_id_].get_state() ||
                OB_LDS_RESTORE == disk_slots_[targets.at(i).disk_id_].get_state()) &&
            read_len != (write_len = ob_pwrite(targets.at(i).fd_, log_copy_buffer_, read_len, offset))) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR,
              "write error",
              K(ret),
              K(targets.at(i)),
              K(read_len),
              K(write_len),
              K(offset),
              K(file_offset),
              K(errno),
              KERRMSG);
        }
        if (OB_FAIL(ret)) {
          if (OB_FAIL(set_bad_disk(targets.at(i).disk_id_))) {
            COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(targets.at(i)));
          }
        }
      }
      offset += count;
      count = (0 == file_offset) ? LOG_COPY_BUF_SIZE : MIN(file_offset - offset, LOG_COPY_BUF_SIZE);
    }
    if (read_len < 0) {
      COMMON_LOG(WARN, "src disk pread fail", K(ret), K(src_fd), K(read_len), K(errno), KERRMSG);
      if (OB_FAIL(set_bad_disk(src_fd.disk_id_))) {
        COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(src_fd));
      }
    }
    if (file_offset != 0 && offset > file_offset) {  // defense code
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "offset exceed file offset", K(ret), K(offset), K(file_offset));
    }
  }
  return ret;
}

int ObLogDiskManager::inner_exist(const int64_t disk_id, const int64_t file_id, bool& exist) const
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  exist = false;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT || file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id), K(file_id));
  } else {
    n = ::snprintf(fname, sizeof(fname), "%s/%ld", disk_slots_[disk_id].get_disk_path(), file_id);
    if (n <= 0 || n >= sizeof(fname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(disk_slots_[disk_id]));
    } else if (0 > ::access(fname, F_OK)) {
      if (errno == ENOENT) {
        exist = false;
        ret = OB_SUCCESS;
      } else {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "access flag file fail", K(ret), K(fname), K(errno), KERRMSG);
      }
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObLogDiskManager::inner_unlink(const int64_t disk_id, const int64_t file_id, const bool is_tmp) const
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  const ObLogDiskInfo* disk = NULL;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT || file_id < 0 || OB_INVALID_FILE_ID == file_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id), K(file_id));
  } else {
    disk = &disk_slots_[disk_id];
    n = snprintf(fname, sizeof(fname), "%s/%ld%s", disk->get_disk_path(), file_id, is_tmp ? TMP_SUFFIX : "\0");
    if (n <= 0 || n >= sizeof(fname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(file_id), K(*disk));
    } else if (0 != ::unlink(fname) && ENOENT != errno) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "unlink file failed", K(ret), K(fname), K(errno), KERRMSG);
    }
  }
  return ret;
}

int ObLogDiskManager::inner_open_fd(
    const int64_t disk_id, const int64_t file_id, const int flag, const bool is_tmp, int& fd)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  ObLogDiskInfo* disk = NULL;
  fd = -1;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT || file_id < 0 || OB_INVALID_FILE_ID == file_id ||
      (OPEN_FLAG_READ != flag && OPEN_FLAG_WRITE != flag && OPEN_FLAG_WRITE_WITHOUT_CREATE != flag)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id), K(file_id), K(flag));
  } else {
    disk = &disk_slots_[disk_id];
    n = snprintf(fname, sizeof(fname), "%s/%ld%s", disk->get_disk_path(), file_id, is_tmp ? TMP_SUFFIX : "\0");
    if (n <= 0 || n >= sizeof(fname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(file_id), K(*disk));
    } else if (0 > (fd = ::open(fname, flag, OPEN_MODE))) {
      if (ENOENT == errno) {
        ret = OB_FILE_NOT_EXIST;
        COMMON_LOG(WARN, "file not exist", K(ret), K(fname), K(flag));
      } else {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "open file fail", K(ret), K(fname), K(flag), K(errno), KERRMSG);
      }
    }
  }
  return ret;
}

int ObLogDiskManager::log_restore_begin(const int64_t disk_id)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  char tname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  int tn = 0;
  ObLogDiskInfo* disk = NULL;
  bool exist = true;
  int64_t p_ret = 0;
  int64_t pos = 0;

  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id));
  } else if (-1 != rst_fd_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "previous LOG_RESTORE file still open", K(ret), K_(rst_fd), K_(rst_pro));
  } else {
    disk = &disk_slots_[disk_id];
    n = snprintf(fname, sizeof(fname), "%s/%s", disk->get_disk_path(), LOG_RESTORE_FILENAME);
    tn = snprintf(tname, sizeof(tname), "%s/%s%s", disk->get_disk_path(), LOG_RESTORE_FILENAME, TMP_SUFFIX);
    if (n <= 0 || n >= sizeof(fname) || tn <= 0 || tn >= sizeof(tname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(*disk));
    } else if (0 > ::access(fname, F_OK)) {
      if (ENOENT == errno) {
        exist = false;
      } else {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "check LOG_RESTORE file fail", K(ret), K(fname));
      }
    }

    if (OB_SUCC(ret)) {
      if (exist) {
        if (0 > (rst_fd_ = ::open(fname, O_RDWR | O_DIRECT | O_SYNC, OPEN_MODE))) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "open LOG_RESTORE file fail", K(ret), K(fname), K(errno), KERRMSG);
        } else if ((p_ret = ob_pread(rst_fd_, rst_pro_buffer_, RESTORE_PROGRESS_BUF_SIZE, 0)) <= 0) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "read LOG_RESTORE file fail", K(ret), K(p_ret), K_(rst_fd), K(errno), KERRMSG);
        } else if (OB_FAIL(rst_pro_.deserialize(rst_pro_buffer_, RESTORE_PROGRESS_BUF_SIZE, pos))) {
          COMMON_LOG(ERROR, "deserialize LOG_RESTORE file fail", K(ret));
        }
      } else {
        rst_pro_.reset();
        if (0 > (rst_fd_ = ::open(tname, O_RDWR | O_CREAT | O_DIRECT | O_SYNC | O_TRUNC, OPEN_MODE))) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "open LOG_RESTORE file fail", K(ret), K(fname), K(errno), KERRMSG);
        } else if (OB_FAIL(write_restore_progress())) {
          COMMON_LOG(ERROR, "write restore progress fail", K(ret), K(disk_id), K_(rst_pro));
        } else if (0 != ::rename(tname, fname)) {
          ret = OB_IO_ERROR;
          COMMON_LOG(ERROR, "rename LOG_RESTORE file fail", K(ret), K(tname), K(errno), KERRMSG);
        }
      }
    }

    if (OB_SUCC(ret)) {
      COMMON_LOG(INFO, "log restore begin", K_(rst_pro), K(exist), K(fname));
    }
  }
  return ret;
}

int ObLogDiskManager::log_restore_end(const int64_t disk_id)
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  ObLogDiskInfo* disk = NULL;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id));
  } else if (-1 == rst_fd_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "restore not begin", K(ret), K_(rst_fd), K_(rst_pro));
  } else {
    // ignore return
    ::close(rst_fd_);
    rst_fd_ = -1;
    rst_pro_.reset();
    disk = &disk_slots_[disk_id];
    n = snprintf(fname, sizeof(fname), "%s/%s", disk->get_disk_path(), LOG_RESTORE_FILENAME);
    if (n <= 0 || n >= sizeof(fname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(*disk));
    } else if (0 > ::unlink(fname)) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "delete flag file fail", K(ret), K(fname), K(errno), KERRMSG);
    } else {
      COMMON_LOG(INFO, "log restore end", K(disk_id), K(fname));
    }
  }
  return ret;
}

int ObLogDiskManager::is_log_restoring(const int64_t disk_id, bool& restoring) const
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id));
  } else {
    n = snprintf(fname, sizeof(fname), "%s/%s", disk_slots_[disk_id].get_disk_path(), LOG_RESTORE_FILENAME);
    if (n <= 0 || n >= sizeof(fname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(disk_slots_[disk_id]));
    } else if (0 > ::access(fname, F_OK)) {
      if (errno == ENOENT) {
        restoring = false;
        ret = OB_SUCCESS;
      } else {
        ret = OB_IO_ERROR;
        COMMON_LOG(ERROR, "access flag file fail", K(ret), K(fname), K(errno), KERRMSG);
      }
    } else {
      restoring = true;
    }
  }
  return ret;
}

int ObLogDiskManager::write_restore_progress()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t p_ret = 0;
  if (rst_fd_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "log restore not begin", K(ret), K_(rst_fd));
  } else if (OB_FAIL(rst_pro_.serialize(rst_pro_buffer_, RESTORE_PROGRESS_BUF_SIZE, pos))) {
    COMMON_LOG(ERROR, "serialize LOG_RESTORE file fail", K(ret), K_(rst_pro));
  } else if ((p_ret = ob_pwrite(rst_fd_, rst_pro_buffer_, RESTORE_PROGRESS_BUF_SIZE, 0)) <= 0) {
    ret = OB_IO_ERROR;
    COMMON_LOG(ERROR, "write LOG_RESTORE file fail", K(ret), K_(rst_fd), K(errno), KERRMSG);
  } else {
    COMMON_LOG(INFO, "write restore progress", K_(rst_pro), K_(rst_fd));
  }
  return ret;
}

void ObLogDiskManager::run_monitor_task()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(detect_disks(log_dir_, file_size_, pool_type_))) {
    COMMON_LOG(ERROR, "detect disks fail", K(ret), K_(log_dir));
  } else if (OB_FAIL(load_new_disks())) {
    COMMON_LOG(ERROR, "load new disks fail", K(ret));
  }

  // whatever return, sweep BAD disk slots for future use
  if (OB_FAIL(sweep_bad_disk_slots())) {
    COMMON_LOG(ERROR, "sweep bad disk slots fail", K(ret));
  }
}

int ObLogDiskManager::load_new_disks()
{
  int ret = OB_SUCCESS;
  ObLogDiskState state = OB_LDS_INVALID;
  for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
    state = disk_slots_[i].get_state();
    if (OB_LDS_NEW == state || OB_LDS_RESTORE == state) {
      COMMON_LOG(INFO, "start load disk", K(disk_slots_[i]));
      bool is_empty = false;
      if (OB_LDS_NEW == state && OB_FAIL(is_disk_dir_empty(disk_slots_[i].get_disk_path(), is_empty))) {
        COMMON_LOG(ERROR, "check dir empty fail", K(ret), K(disk_slots_[i]));
      } else if (OB_LDS_NEW == state && !is_empty) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "new disk is not empty", K(ret), K(disk_slots_[i]));
      } else if (OB_FAIL(log_restore_begin(disk_slots_[i].get_disk_id()))) {
        COMMON_LOG(ERROR, "begin log restore fail", K(ret), K(disk_slots_[i]));
      } else {
        // Change to RESTORE if it is not
        disk_slots_[i].set_state(OB_LDS_NEW, OB_LDS_RESTORE);
        if (OB_FAIL(restore_log_files(disk_slots_[i].get_disk_id()))) {
          COMMON_LOG(ERROR, "restore log files fail", K(ret), K(disk_slots_[i]));
        } else if (rst_pro_.is_catchup_complete() && rst_pro_.is_copy_complete()) {
          if (OB_FAIL(clear_orphan_files(disk_slots_[i].get_disk_id()))) {
            COMMON_LOG(ERROR, "clear orphan files fail", K(ret), K(disk_slots_[i]));
          } else {
            // disk may become BAD by write thread, use CAS here
            disk_slots_[i].set_state(OB_LDS_RESTORE, OB_LDS_GOOD);
            if (OB_LDS_GOOD == disk_slots_[i].get_state() && OB_FAIL(log_restore_end(disk_slots_[i].get_disk_id()))) {
              COMMON_LOG(ERROR, "end log sync fail", K(ret), K(disk_slots_[i]));
            }
          }
        }
      }
      if (rst_fd_ >= 0) {  // whatever close
        ::close(rst_fd_);
        rst_fd_ = -1;
      }
    }
    // whatever reason fail, mark disk BAD for safe
    if (OB_FAIL(ret)) {
      if (OB_FAIL(set_bad_disk(disk_slots_[i].get_disk_id()))) {
        COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(disk_slots_[i]));
      }
    }
  }
  return ret;
}

int ObLogDiskManager::is_disk_dir_empty(const char* dir_path, bool& is_empty) const
{
  int ret = OB_SUCCESS;
  struct dirent entry;
  memset(&entry, 0x00, sizeof(entry));
  struct dirent* pentry = &entry;
  DIR* plog_dir = NULL;
  int sys_ret = 0;

  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), KP(dir_path));
  } else if (NULL == (plog_dir = ::opendir(dir_path))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(ERROR, "open dir fail", K(ret), K(dir_path));
  } else if (0 != (sys_ret = ::readdir_r(plog_dir, pentry, &pentry))) {
    ret = OB_IO_ERROR;
    COMMON_LOG(ERROR, "read dir fail", K(ret), K(dir_path));
  } else {
    is_empty = true;
    while (is_empty && 0 == sys_ret && NULL != pentry) {
      if ((0 != STRCMP(pentry->d_name, ".")) && (0 != STRCMP(pentry->d_name, ".."))) {
        is_empty = false;
      } else {
        sys_ret = ::readdir_r(plog_dir, pentry, &pentry);
      }
    }
    if (0 != sys_ret) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "read dir fail", K(ret), K(dir_path));
    }
  }

  if (OB_NOT_NULL(plog_dir)) {
    // ignore return
    ::closedir(plog_dir);
    plog_dir = NULL;
  }
  return ret;
}

int ObLogDiskManager::restore_log_files(const int64_t disk_id)
{
  int ret = OB_SUCCESS;
  const ObLogDiskInfo* src_disk = NULL;
  ObLogDirScanner scanner;
  uint64_t scan_min_id = 0;
  uint64_t scan_max_id = 0;
  int64_t cur_log_id = 0;
  int64_t copy_start_id = 0;
  int64_t copy_end_id = 0;

  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id));
  } else {
    ObLogDiskInfo* disk = &disk_slots_[disk_id];
    bool copied = true;
    src_disk = get_first_good_disk();
    scanner.reset();
    if (OB_ISNULL(src_disk)) {
      ret = OB_ERR_SYS;
      COMMON_LOG(WARN, "no good disk", K(ret));
    } else if (OB_FAIL(scanner.init(src_disk->get_disk_path()))) {
      COMMON_LOG(WARN, "src disk log not complete", K(ret), K(*src_disk));
      if (OB_FAIL(set_bad_disk(src_disk->get_disk_id()))) {
        COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(*src_disk));
      }
    } else if (!scanner.has_log()) {
      rst_pro_.catchup_complete_ = 1;
    } else if (OB_FAIL(scanner.get_min_log_id(scan_min_id)) || OB_FAIL(scanner.get_max_log_id(scan_max_id))) {
      COMMON_LOG(ERROR, "get min/max log id fail", K(ret));
    } else {
      // decided copy range and persist it to file
      if (rst_pro_.is_copy_start()) {
        if ((int64_t)scan_min_id > rst_pro_.copy_start_file_id_ ||
            (rst_pro_.copied_file_id_ > 0 && (int64_t)scan_min_id >= rst_pro_.copied_file_id_)) {
          // the minimum log file in source disk is greater than copy id, no need to continue copy
          rst_pro_.copy_complete_ = 1;
        } else {
          copy_start_id = rst_pro_.copied_file_id_ > 1 ? rst_pro_.copied_file_id_ - 1 : rst_pro_.copy_start_file_id_;
          copy_end_id = (int64_t)scan_min_id;
        }
      } else {
        rst_pro_.copy_start_file_id_ = (int64_t)scan_max_id - 1;
        copy_start_id = (int64_t)scan_max_id - 1;
        copy_end_id = (int64_t)scan_min_id;
        if (OB_FAIL(write_restore_progress())) {
          COMMON_LOG(ERROR, "write restore progress fail", K(ret), K(disk_id), K_(rst_pro));
        }
      }

      if (OB_SUCC(ret) && !rst_pro_.is_copy_complete()) {
        if (copy_start_id > 0 && copy_end_id > 0 && copy_start_id >= copy_end_id) {
          COMMON_LOG(INFO, "start copy log files", K(copy_start_id), K(copy_end_id), K_(rst_pro));
          cur_log_id = copy_start_id;
          for (; OB_SUCC(ret) && copied && cur_log_id >= copy_end_id; cur_log_id--) {
            // try catch last log file as early as possible
            if (OB_FAIL(catchup_log_files(src_disk->get_disk_id(), disk_id))) {
              COMMON_LOG(ERROR, "catch log file fail", K(ret), K(*src_disk), K(*disk), K_(rst_pro));
            } else if (OB_FAIL(copy_single_log_file(src_disk->get_disk_id(), disk_id, cur_log_id, copied))) {
              COMMON_LOG(ERROR, "copy log file fail", K(ret), K(*src_disk), K(disk_id), K(cur_log_id));
            } else if (copied) {
              rst_pro_.copied_file_id_ = cur_log_id;
              if (OB_FAIL(write_restore_progress())) {
                COMMON_LOG(ERROR, "write restore progress fail", K(ret), K(disk_id), K_(rst_pro));
              }
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      rst_pro_.copy_complete_ = 1;
      if (OB_FAIL(write_restore_progress())) {
        COMMON_LOG(ERROR, "write restore progress fail", K(ret), K(disk_id), K_(rst_pro));
      } else if (OB_FAIL(catchup_log_files(src_disk->get_disk_id(), disk_id))) {
        COMMON_LOG(ERROR, "catch write file fail", K(ret), K(*src_disk), K(*disk));
      }
    }
  }
  return ret;
}

int ObLogDiskManager::copy_single_log_file(
    const int64_t src_disk_id, const int64_t dest_disk_id, const int64_t file_id, bool& copied)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLogFdInfo, 1> targets;
  ObLogFdInfo dest_fd;
  ObLogFdInfo src_fd;
  dest_fd.disk_id_ = dest_disk_id;
  dest_fd.file_id_ = file_id;
  dest_fd.refresh_time_ = ObTimeUtility::current_time();
  src_fd.disk_id_ = src_disk_id;
  src_fd.file_id_ = file_id;
  src_fd.refresh_time_ = ObTimeUtility::current_time();
  bool src_exist = true;

  if (src_disk_id < 0 || src_disk_id >= MAX_DISK_COUNT || dest_disk_id < 0 || dest_disk_id >= MAX_DISK_COUNT ||
      file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(src_disk_id), K(dest_disk_id), K(file_id));
  } else if (OB_FAIL(inner_open_fd(src_disk_id, file_id, OPEN_FLAG_READ, false, src_fd.fd_))) {
    COMMON_LOG(WARN, "open src file fail", K(ret), K(src_disk_id), K(file_id));
    if (OB_FILE_NOT_EXIST != ret && OB_FAIL(set_bad_disk(src_disk_id))) {
      COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(src_disk_id), K(file_id));
    } else {
      ret = OB_SUCCESS;
      copied = false;
      COMMON_LOG(INFO, "src log file is renamed or deleted", K(ret), K(copied));
    }
  } else if (OB_FAIL(inner_open_fd(dest_disk_id, file_id, OPEN_FLAG_WRITE, true, dest_fd.fd_))) {
    COMMON_LOG(ERROR, "open target fd fail", K(ret), K(file_id), K(dest_disk_id), K(dest_fd));
  } else if (CLOG_WRITE_POOL == pool_type_ && 0 != ::fallocate(dest_fd.fd_, 0, 0, file_size_)) {
    if (ENOSPC == errno) {
      ret = OB_SUCCESS;
      copied = false;
      COMMON_LOG(WARN, "not enough disk space", K(ret), K(copied));
    } else {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "fallocate fail", K(ret), K(dest_fd), K_(file_size), K(errno), KERRMSG);
    }
  } else if (OB_FAIL(targets.push_back(dest_fd))) {
    COMMON_LOG(ERROR, "push back fd fail", K(ret), K(dest_fd));
  } else if (OB_FAIL(copy_file_content(src_fd, targets, 0))) {
    COMMON_LOG(ERROR, "copy file fail", K(ret), K(src_fd), K(dest_fd));
  } else if (OB_FAIL(inner_exist(src_disk_id, file_id, src_exist))) {
    COMMON_LOG(ERROR, "check src file exist fail", K(ret), K(src_disk_id), K(file_id));
  } else if (!src_exist) {
    // src file is renamed or deleted
    if (OB_FAIL(inner_unlink(dest_disk_id, file_id, true))) {
      COMMON_LOG(ERROR, "delete tmp copied file fail", K(ret), K(dest_disk_id), K(file_id));
    } else {
      copied = false;
      COMMON_LOG(INFO, "log file is renamed or deleted", K(ret), K(copied), K(src_exist));
    }
  } else if (OB_FAIL(remove_tmp_suffix(dest_disk_id, file_id))) {
    COMMON_LOG(ERROR, "rename tmp file fail", K(ret), K(dest_disk_id), K(file_id));
  } else {
    copied = true;
    COMMON_LOG(INFO, "copy log succeed", K(file_id), K(dest_fd), K(src_fd));
  }

  // ignore return
  if (dest_fd.fd_ >= 0) {
    ::close(dest_fd.fd_);
  }
  if (src_fd.fd_ >= 0) {
    ::close(src_fd.fd_);
  }
  return ret;
}

int ObLogDiskManager::clear_orphan_files(const int64_t disk_id)
{
  int ret = OB_SUCCESS;
  ObLogDirScanner scanner;
  int64_t clear_start_id = -1;
  int64_t clear_end_id = -1;
  uint64_t scan_min_id = 0;

  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid disk id", K(ret), K(disk_id));
  } else if (OB_SUCC(scanner.init(disk_slots_[disk_id].get_disk_path()))) {
    COMMON_LOG(INFO, "no orphan file", K(disk_slots_[disk_id]));
  } else if (OB_DISCONTINUOUS_LOG != ret) {
    COMMON_LOG(ERROR, "scan disk dir fail", K(ret), K(disk_slots_[disk_id]));
  } else if (!scanner.has_log()) {  // defense code
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "disk should not be empty", K(ret), K(disk_slots_[disk_id]));
  } else {
    COMMON_LOG(INFO, "start clear orphan file", K(disk_slots_[disk_id]));
    if (OB_FAIL(scanner.get_min_log_id(scan_min_id))) {
      COMMON_LOG(ERROR, "get min log id fail", K(ret), K(disk_slots_[disk_id]));
    } else if ((int64_t)scan_min_id <= rst_pro_.copied_file_id_) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(ERROR, "unexpected orphan file id", K(scan_min_id), K_(rst_pro));
    } else {
      clear_start_id = (int64_t)scan_min_id - 1;
      clear_end_id = rst_pro_.copied_file_id_;
      for (int64_t log_id = clear_start_id; OB_SUCC(ret) && log_id >= clear_end_id; log_id--) {
        if (OB_FAIL(inner_unlink(disk_id, log_id, false))) {
          COMMON_LOG(
              ERROR, "unlink orphan file fail", K(ret), K(disk_id), K(log_id), K(clear_start_id), K(clear_end_id));
        } else {
          COMMON_LOG(INFO, "unlink orphan file", K(disk_id), K(log_id), K(clear_start_id), K(clear_end_id));
        }
      }
    }
  }
  return ret;
}

int ObLogDiskManager::catchup_log_files(const int64_t src_disk_id, const int64_t dest_disk_id)
{
  int ret = OB_SUCCESS;

  if (src_disk_id < 0 || src_disk_id >= MAX_DISK_COUNT || dest_disk_id < 0 || dest_disk_id >= MAX_DISK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(src_disk_id), K(dest_disk_id));
  } else if (rst_pro_.is_catchup_complete()) {
    // catchup complete, do nothing
  } else {
    ObLogDiskInfo* dest_disk = NULL;
    int64_t file_id = 0;
    int64_t file_offset = 0;
    ObSEArray<ObLogFdInfo, 1> targets;
    ObLogFdInfo dest_fd;
    ObLogFdInfo src_fd;
    dest_disk = &disk_slots_[dest_disk_id];

    // Get and persist catchup file id and offset if hasn't
    if (!rst_pro_.is_catchup_start() && dest_disk->get_restore_start_file_id() > 0) {
      rst_pro_.catchup_file_id_ = dest_disk->get_restore_start_file_id();
      rst_pro_.catchup_offset_ = dest_disk->get_restore_start_offset();
      if (OB_FAIL(write_restore_progress())) {
        COMMON_LOG(ERROR, "write restore progress fail", K(ret), K(dest_disk_id), K_(rst_pro));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!rst_pro_.is_catchup_start()) {
      COMMON_LOG(INFO, "catchup hasn't been set", K(dest_disk_id), K(rst_pro_));
    } else {
      COMMON_LOG(INFO, "start catchup log files", K(dest_disk_id), K(rst_pro_));
      // 1) copy content of last log file
      if (rst_pro_.catchup_offset_ > 0) {
        file_id = rst_pro_.catchup_file_id_;
        file_offset = rst_pro_.catchup_offset_;
        dest_fd.disk_id_ = dest_disk_id;
        dest_fd.file_id_ = file_id;
        dest_fd.refresh_time_ = ObTimeUtility::current_time();
        src_fd.disk_id_ = src_disk_id;
        src_fd.file_id_ = file_id;
        src_fd.refresh_time_ = ObTimeUtility::current_time();
        if (OB_FAIL(inner_open_fd(src_disk_id, file_id, OPEN_FLAG_READ, false, src_fd.fd_))) {
          COMMON_LOG(WARN, "open src file fail", K(ret), K(src_disk_id), K(file_id));
          if (OB_FAIL(set_bad_disk(src_disk_id))) {
            COMMON_LOG(ERROR, "set bad disk fail", K(ret), K(src_disk_id), K(file_id));
          }
        } else if (OB_FAIL(inner_open_fd(dest_disk_id, file_id, OPEN_FLAG_WRITE_WITHOUT_CREATE, false, dest_fd.fd_))) {
          COMMON_LOG(ERROR, "open target fd fail", K(ret), K(file_id), K(dest_disk_id), K(dest_fd));
        } else if (OB_FAIL(targets.push_back(dest_fd))) {
          COMMON_LOG(ERROR, "push back fd fail", K(ret), K(dest_fd));
        } else if (OB_FAIL(copy_file_content(src_fd, targets, file_offset))) {
          COMMON_LOG(ERROR, "copy file fail", K(ret), K(src_fd), K(dest_fd), K(file_offset));
        }
        // ignore return
        if (dest_fd.fd_ >= 0) {
          ::close(dest_fd.fd_);
        }
        if (src_fd.fd_ >= 0) {
          ::close(src_fd.fd_);
        }
      }
      // 2) copy log file between (copy_start_file_id_, catchup_file_id_)
      if (OB_SUCC(ret)) {
        int64_t copy_start_id = rst_pro_.catchup_file_id_ - 1;
        int64_t copy_end_id = rst_pro_.copy_start_file_id_ + 1;
        int64_t cur_log_id = copy_start_id;
        bool copied = true;
        if (copy_start_id > 0 && copy_end_id > 0 && copy_start_id >= copy_end_id) {
          COMMON_LOG(INFO, "copy log files", K(copy_start_id), K(copy_end_id));
          for (; OB_SUCC(ret) && copied && cur_log_id >= copy_end_id; cur_log_id--) {
            if (OB_FAIL(copy_single_log_file(src_disk_id, dest_disk_id, cur_log_id, copied))) {
              COMMON_LOG(ERROR, "copy log file fail", K(ret), K(src_disk_id), K(dest_disk_id), K(cur_log_id));
            }
          }
        }
      }
      // 3) persist complete to file
      if (OB_SUCC(ret)) {
        rst_pro_.catchup_complete_ = 1;
        if (OB_FAIL(write_restore_progress())) {
          COMMON_LOG(ERROR, "write restore progress fail", K(ret), K(dest_disk_id), K_(rst_pro));
        } else {
          COMMON_LOG(INFO, "finish catchup log files", K(dest_disk_id), K_(rst_pro));
        }
      }
    }
  }
  return ret;
}

int ObLogDiskManager::sweep_bad_disk_slots()
{
  int ret = OB_SUCCESS;
  for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
    if (OB_LDS_BAD == disk_slots_[i].get_state() &&
        disk_slots_[i].get_state_timestamp() + BAD_DISK_RETENTION_US < ObTimeUtility::current_time()) {
      if (OB_FAIL(set_invalid_disk(i))) {
        COMMON_LOG(ERROR, "set invalid disk fail", K(ret), K(disk_slots_[i]));
      } else {
        COMMON_LOG(INFO, "release BAD disk slot", K(disk_slots_[i]));
      }
    }
  }
  return ret;
}

int ObLogDiskManager::remove_tmp_suffix(const int64_t disk_id, const int64_t file_id) const
{
  int ret = OB_SUCCESS;
  char fname[OB_MAX_FILE_NAME_LENGTH];
  char tname[OB_MAX_FILE_NAME_LENGTH];
  int n = 0;
  int tn = 0;
  const ObLogDiskInfo* disk = NULL;
  if (disk_id < 0 || disk_id >= MAX_DISK_COUNT || file_id < 0 || OB_INVALID_FILE_ID == file_id) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(disk_id), K(file_id));
  } else {
    disk = &disk_slots_[disk_id];
    n = snprintf(fname, sizeof(fname), "%s/%ld", disk->get_disk_path(), file_id);
    tn = snprintf(tname, sizeof(tname), "%s/%ld%s", disk->get_disk_path(), file_id, TMP_SUFFIX);
    if (n <= 0 || n >= sizeof(fname) || tn <= 0 || tn >= sizeof(tname)) {
      ret = OB_BUF_NOT_ENOUGH;
      COMMON_LOG(ERROR, "file name too long", K(ret), K(file_id), K(*disk));
    } else if (0 != ::rename(tname, fname)) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "rename file failed", K(ret), K(fname), K(tname), K(errno), KERRMSG);
    }
  }
  return ret;
}

uint32_t ObLogDiskManager::get_min_using_file_id()
{
  int ret = OB_SUCCESS;
  uint32_t min_using_id = OB_INVALID_FILE_ID;
  const ObLogDiskInfo* disk = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (NULL == (disk = get_first_good_disk())) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "no disk found", K(ret));
  } else {
    min_using_id = (disk->get_file_pool())->get_min_using_file_id();
  }

  return min_using_id;
}

uint32_t ObLogDiskManager::get_min_file_id()
{
  int ret = OB_SUCCESS;
  uint32_t min_file_id = OB_INVALID_FILE_ID;
  const ObLogDiskInfo* disk = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (NULL == (disk = get_first_good_disk())) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "no disk found", K(ret));
  } else {
    min_file_id = (disk->get_file_pool())->get_min_file_id();
  }

  return min_file_id;
}

int64_t ObLogDiskManager::get_free_quota() const
{
  int ret = OB_SUCCESS;
  int64_t free_quota = 0;
  const ObLogDiskInfo* disk = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (NULL == (disk = get_first_good_disk())) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "no disk found", K(ret));
  } else {
    free_quota = disk->get_file_pool()->get_free_quota();
  }

  return free_quota;
}

bool ObLogDiskManager::is_disk_space_enough() const
{
  return get_free_quota() >= 0;
}

void ObLogDiskManager::update_min_using_file_id(const uint32_t file_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        disk_slots_[i].get_file_pool()->update_min_using_file_id(file_id);
      }
    }
  }
}

void ObLogDiskManager::update_min_file_id(const uint32_t file_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        disk_slots_[i].get_file_pool()->update_min_file_id(file_id);
      }
    }
  }
}

void ObLogDiskManager::update_max_file_id(const uint32_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        disk_slots_[i].get_file_pool()->update_max_file_id(file_id);
      }
    }
  }
}

void ObLogDiskManager::try_recycle_file()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        disk_slots_[i].get_file_pool()->try_recycle_file();
      }
    }
  }
}

int ObLogDiskManager::update_free_quota()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    int32_t succ_cnt = 0;
    for (int32_t i = 0; OB_SUCC(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        if (OB_FAIL(disk_slots_[i].get_file_pool()->update_free_quota())) {
          COMMON_LOG(WARN, "update quota failed.", K(ret), K(disk_slots_[i]));
          if (OB_FAIL(set_bad_disk(disk_slots_[i].get_disk_id()))) {
            COMMON_LOG(ERROR, "fail to set bad disk", K(ret), K(disk_slots_[i]));
          }
        } else {
          ++succ_cnt;
        }
      }
    }

    if (OB_SUCC(ret) && 0 == succ_cnt) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "update quota on all disk failed", K(ret));
    }
  }

  return ret;
}

bool ObLogDiskManager::free_quota_warn()
{
  int ret = OB_SUCCESS;
  bool b_warn = false;
  const ObLogDiskInfo* disk = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (NULL == (disk = get_first_good_disk())) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(ERROR, "no disk found", K(ret));
  } else {
    b_warn = disk->get_file_pool()->free_quota_warn();
  }

  return b_warn;
}

int ObLogDiskManager::get_file_id_range(uint32_t& min_file_id, uint32_t& max_file_id)
{
  int ret = OB_SUCCESS;
  bool try_next_disk = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    ret = OB_EAGAIN;
    // try every disk until succeed
    for (int32_t i = 0; try_next_disk && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        if (OB_FAIL(disk_slots_[i].get_file_pool()->get_file_id_range(min_file_id, max_file_id)) &&
            (OB_ENTRY_NOT_EXIST != ret)) {
          COMMON_LOG(WARN, "get_file_id_range fail", K(ret), K(disk_slots_[i]));
        } else if (OB_ENTRY_NOT_EXIST == ret) {
          try_next_disk = (OB_LDS_GOOD != disk_slots_[i].get_state());
        } else {
          try_next_disk = false;
        }
      }
    }

    if (try_next_disk) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "all disks fail", K(ret));
    }
  }

  return ret;
}

int ObLogDiskManager::get_total_used_size(int64_t& total_size)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    ret = OB_EAGAIN;
    // try every disk until succeed
    for (int32_t i = 0; OB_FAIL(ret) && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        if (OB_FAIL(disk_slots_[i].get_file_pool()->get_total_used_size(total_size))) {
          COMMON_LOG(WARN, "get_total_used_size fail", K(ret), K(disk_slots_[i]));
        }
      }
    }

    if (OB_FAIL(ret)) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "all disks fail", K(ret));
    }
  }

  return ret;
}

int ObLogDiskManager::exist(const int64_t file_id, bool& is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  bool try_next_disk = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    // try every disk until succeed
    for (int32_t i = 0; try_next_disk && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        if (OB_FAIL(inner_exist(disk_slots_[i].get_disk_id(), file_id, is_exist))) {
          COMMON_LOG(WARN, "exist fail", K(ret), K(disk_slots_[i]));
        } else {
          try_next_disk = false;
        }
      }
    }
    if (try_next_disk) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "all disks fail", K(ret));
    }
  }

  return ret;
}

int ObLogDiskManager::fstat(const int64_t file_id, struct stat* file_stat)
{
  int ret = OB_SUCCESS;
  char file_name[OB_MAX_FILE_NAME_LENGTH];
  int tmp_n = 0;
  bool try_next_disk = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    // try every disk until succeed
    for (int32_t i = 0; try_next_disk && i < MAX_DISK_COUNT; i++) {
      if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
        tmp_n = snprintf(file_name, OB_MAX_FILE_NAME_LENGTH, "%s/%lu", disk_slots_[i].get_disk_path(), file_id);
        if (tmp_n <= 0 || tmp_n >= OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_ERROR;
          COMMON_LOG(WARN, "file name error", K(ret), K(file_id), K(disk_slots_[i]));
        } else if (0 != ::stat(file_name, file_stat)) {
          if (ENOENT == errno) {
            ret = OB_FILE_NOT_EXIST;
            COMMON_LOG(WARN, "file not found.", K(ret), K(file_name), K(errno));
            // in case disk is bad, double check
            try_next_disk = (OB_LDS_GOOD != disk_slots_[i].get_state());
          } else {
            ret = OB_IO_ERROR;
            COMMON_LOG(WARN, "stat error", K(ret), K(file_name), K(errno));
          }
        } else {
          ret = OB_SUCCESS;
          try_next_disk = false;
        }
      }
    }

    if (try_next_disk) {
      ret = OB_IO_ERROR;
      COMMON_LOG(ERROR, "all disks fail", K(ret));
    }
  }

  return ret;
}

const char* ObLogDiskManager::get_dir_name()
{
  int ret = OB_SUCCESS;
  const ObLogDiskInfo* disk = NULL;
  const char* dir_name = "";

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else if (NULL == (disk = get_first_good_disk())) {
    ret = OB_ENTRY_NOT_EXIST;
    COMMON_LOG(WARN, "no disk found", K(ret));
  } else {
    dir_name = disk->get_disk_path();
  }

  return dir_name;
}

int ObLogDiskManager::get_total_disk_space(int64_t& total_space) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not inited", K(ret));
  } else {
    total_space = total_disk_space_;
  }
  return ret;
}

int ObLogDiskManager::get_total_disk_space_(int64_t& total_space) const
{
  int ret = OB_EAGAIN;
  struct statfs fsst;
  // try every disk until succeed
  for (int32_t i = 0; OB_FAIL(ret) && i < MAX_DISK_COUNT; i++) {
    if (OB_LDS_GOOD == disk_slots_[i].get_state()) {
      if (0 != ::statfs(disk_slots_[i].get_disk_path(), &fsst)) {
        ret = OB_IO_ERROR;
        COMMON_LOG(WARN, "statfs error", K(ret), K(errno), KERRMSG);
      } else {
        ret = OB_SUCCESS;
        total_space = (int64_t)fsst.f_bsize * (int64_t)fsst.f_blocks;
      }
    }
  }

  if (OB_FAIL(ret)) {
    ret = OB_IO_ERROR;
    COMMON_LOG(ERROR, "all disks fail", K(ret));
  }
  return ret;
}

ObLogDiskManager::ReadWriteDiskIterator ObLogDiskManager::begin() const
{
  ObLogDiskManager::ReadWriteDiskIterator ret_iter(this, -1);
  ++ret_iter;
  return ret_iter;
}

ObLogDiskManager::ReadWriteDiskIterator ObLogDiskManager::end() const
{
  return ObLogDiskManager::ReadWriteDiskIterator(this, MAX_DISK_COUNT);
}

ObLogDiskManager::BaseDiskIterator ObLogDiskManager::begin_all() const
{
  ObLogDiskManager::BaseDiskIterator ret_iter(this, -1);
  ++ret_iter;
  return ret_iter;
}

ObLogDiskManager::BaseDiskIterator ObLogDiskManager::end_all() const
{
  return ObLogDiskManager::BaseDiskIterator(this, MAX_DISK_COUNT);
}

/*static*/ ObLogDiskManager* ObLogDiskManager::get_disk_manager(const ObRedoLogType log_type)
{
  ObLogDiskManager* disk_mgr = NULL;
  switch (log_type) {
    case OB_REDO_TYPE_CLOG:
      disk_mgr = &OB_CLOG_DISK_MGR;
      break;
    case OB_REDO_TYPE_ILOG:
      disk_mgr = &OB_ILOG_DISK_MGR;
      break;
    case OB_REDO_TYPE_SLOG:
      disk_mgr = &OB_SLOG_DISK_MGR;
      break;
    default:
      disk_mgr = NULL;
      break;
  }
  return disk_mgr;
}
}  // namespace common
}  // namespace oceanbase
