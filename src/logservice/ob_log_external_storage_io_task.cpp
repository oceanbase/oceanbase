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
#include "ob_log_external_storage_io_task.h"
#include "lib/string/ob_string.h"                             // ObString
#include "lib/restore/ob_object_device.h"                     // ObObjectDevice
#include "share/ob_device_manager.h"                          // ObDeviceManager
namespace oceanbase
{
using namespace share;
namespace common
{
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}
namespace logservice
{
RunningStatus::RunningStatus()
{
  reset();
}

RunningStatus::~RunningStatus()
{
  reset();
}

void RunningStatus::reset()
{
  ret_ = -1;
  main_thread_id_ = -1;
  thread_id_ = -1;
  logical_thread_id_ = -1;
  status_ = EnumRunningStatus::INVALID_STATUS;
}

ObLogExternalStorageIOTaskCtx::ObLogExternalStorageIOTaskCtx()
  : flying_task_count_(-1), total_task_count_(-1), running_status_(NULL), is_inited_(false)
{}

ObLogExternalStorageIOTaskCtx::~ObLogExternalStorageIOTaskCtx()
{
  destroy();
}

int ObLogExternalStorageIOTaskCtx::init(const int64_t total_task_count)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogExternalStorageIOTaskCtx inited twice", K(total_task_count), KPC(this));
  } else if (0 >= total_task_count) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(total_task_count));
  } else if (OB_FAIL(condition_.init(ObWaitEventIds::LOG_EXTERNAL_STORAGE_IO_TASK_WAIT))) {
    CLOG_LOG(WARN, "init thread conditions failed", K(total_task_count));
    // set 'total_task_count_' at here
  } else if (OB_FAIL(construct_running_status_(total_task_count))) {
    CLOG_LOG(WARN, "construct_running_status_ failed", K(total_task_count));
  } else {
    flying_task_count_ = total_task_count;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogExternalStorageIOTaskCtx init success", KPC(this), K(total_task_count));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

void ObLogExternalStorageIOTaskCtx::destroy()
{
  is_inited_ = false;
  flying_task_count_ = -1;
  deconstruct_running_status_();
  CLOG_LOG(INFO, "ObLogExternalStorageIOTaskCtx destroy success", KPC(this));
}

void ObLogExternalStorageIOTaskCtx::signal()
{
  ObThreadCondGuard guard(condition_);
  if (0 >= flying_task_count_) {
    CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected ERROR, flying_task_count_ is less than 0", KPC(this));
  } else {
    flying_task_count_--;
  }
  if (0 == flying_task_count_) {
    condition_.broadcast();
  }
}

int ObLogExternalStorageIOTaskCtx::wait(const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(condition_);
  while (flying_task_count_ > 0 && OB_SUCC(ret)) {
    ret = condition_.wait_us(timeout_us);
  }
  return ret;
}

int ObLogExternalStorageIOTaskCtx::get_running_status(const int64_t idx,
                                                      RunningStatus *&running_status) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageIOTaskCtx not init", KPC(this));
  } else if (0 > idx && total_task_count_ <= idx) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KPC(this), K(idx));
  } else {
    running_status = running_status_ + idx;
    running_status->logical_thread_id_ = idx;
    running_status->main_thread_id_ = gettid();
    CLOG_LOG(TRACE, "get_running_status success", KPC(this), K(idx), KP(running_status), KPC(running_status));
  }
  return ret;
}

int ObLogExternalStorageIOTaskCtx::get_ret_code() const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < total_task_count_; i++) {
    if (0 == i && OB_SUCCESS != running_status_[i].ret_) {
      ret = running_status_[i].ret_;
      CLOG_LOG(WARN, "asyn task execute failed", KPC(this));
      break;
    }
  }
  return ret;
}

bool ObLogExternalStorageIOTaskCtx::has_flying_async_task() const
{
  ObThreadCondGuard guard(condition_);
  return flying_task_count_ > 0;
}

DEF_TO_STRING(ObLogExternalStorageIOTaskCtx)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(is_inited),
       K_(total_task_count),
       K_(flying_task_count),
       KP(running_status_));
  J_COMMA();
  if (OB_NOT_NULL(running_status_)) {
    J_ARRAY_START();
    for (int64_t i = 0; i < total_task_count_; i++) {
      J_OBJ_START();
      J_KV("idx", i);
      J_COMMA();
      J_KV("data", running_status_[i]);
      J_OBJ_END();
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

int ObLogExternalStorageIOTaskCtx::construct_running_status_(const int64_t total_task_count)
{
  int ret = OB_SUCCESS;
  if (NULL == (running_status_ =
      reinterpret_cast<RunningStatus*>(mtl_malloc(sizeof(RunningStatus)*total_task_count, "ObLogEXT")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed", K(total_task_count), KP(running_status_));
  } else {
    total_task_count_ = total_task_count;
    for (int64_t i = 0; i < total_task_count; i++) {
      RunningStatus *ptr = running_status_ + i;
      new (ptr)RunningStatus;
    }
  }
  return ret;
}

void ObLogExternalStorageIOTaskCtx::deconstruct_running_status_()
{
  if (OB_NOT_NULL(running_status_)) {
    for (int64_t i = 0; i < total_task_count_; i++) {
      RunningStatus *ptr = running_status_ + i;
      ptr->~RunningStatus();
    }
    mtl_free(running_status_);
    running_status_ = NULL;
    total_task_count_ = -1;
    CLOG_LOG(TRACE, "deconstruct_running_status_ success", KPC(this));
  }
}

int get_and_init_io_device(const ObString &uri,
                           const ObString &storage_info,
                           ObIODevice *&io_device)
{
  int ret = OB_SUCCESS;
  ObIODOpts opts;
  ObIODOpt opt;
  opts.opts_ = &opt;
  opts.opt_cnt_ = 1;
  opt.key_ = "storage_info";
  opt.value_.value_str = storage_info.ptr();
  if (OB_FAIL(ObDeviceManager::get_instance().get_device(storage_info, uri, io_device))) {
    CLOG_LOG(WARN, "get_device from ObDeviceManager failed", K(uri), KP(io_device));
  } else if (OB_FAIL(io_device->start(opts))) {
    CLOG_LOG(WARN, "start io device failed", K(uri), KP(io_device));
  } else {
    CLOG_LOG(TRACE, "get_io_device success", K(uri), KP(io_device));
  }
  return ret;
}

void release_io_device(ObIODevice *&io_device)
{
  if (NULL != io_device) {
    (void)ObDeviceManager::get_instance().release_device(io_device);
  }
  io_device = NULL;
}

enum class OPEN_FLAG{
  INVALID_FLAG = 0,
  READ_FLAG = 1,
  WRITE_FLAG = 2,
  MAX_FLAG = 3
};

int convert_to_storage_access_type(const OPEN_FLAG &open_flag,
                                   ObStorageAccessType &storage_access_type)
{
  int ret = OB_SUCCESS;
  if (OPEN_FLAG::READ_FLAG == open_flag) {
    storage_access_type = ObStorageAccessType::OB_STORAGE_ACCESS_ADAPTIVE_READER;
  } else {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "not supported flag", K(open_flag));
  }
  return ret;
}

int open_io_fd(const ObString &uri,
               const OPEN_FLAG open_flag,
               ObIODevice *io_device,
               ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  ObIODOpt opt;
  ObIODOpts iod_opts;
  iod_opts.opts_ = &opt;
  iod_opts.opt_cnt_ = 1;
  // TODO by runlin: support write
  ObStorageAccessType access_type;
  if (OB_FAIL(convert_to_storage_access_type(open_flag, access_type))) {
    CLOG_LOG(WARN, "convert_to_storage_access_type failed", K(open_flag));
  } else if (FALSE_IT(opt.set("AccessType", OB_STORAGE_ACCESS_TYPES_STR[access_type]))) {
    // flag=-1 and mode=0 are invalid, because ObObjectDevice does not use flag and mode;
  } else if (OB_FAIL(io_device->open(uri.ptr(), -1, 0, io_fd, &iod_opts))) {
    CLOG_LOG(WARN, "open fd failed", K(uri), K(open_flag));
  } else {
    CLOG_LOG(TRACE, "open fd success", K(uri), K(open_flag));
  }
  return ret;
}

int close_io_fd(ObIODevice *io_device,
                const ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (NULL == io_device || !io_fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "io device is empty");
  } else if (OB_FAIL(io_device->close(io_fd))) {
    CLOG_LOG(WARN, "fail to close fd!", K(io_fd));
  } else {
    CLOG_LOG(TRACE, "close_io_fd success", KP(io_device), K(io_fd));
  }
  return ret;
}

ObLogExternalStorageIOTaskHandleAdapter::ObLogExternalStorageIOTaskHandleAdapter() {}

ObLogExternalStorageIOTaskHandleAdapter::~ObLogExternalStorageIOTaskHandleAdapter() {}

int ObLogExternalStorageIOTaskHandleAdapter::exist(const ObString &uri,
                                                   const ObString &storage_info,
                                                   bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObIODevice *io_device = NULL;
  if (OB_FAIL(get_and_init_io_device(uri, storage_info, io_device))) {
    CLOG_LOG(WARN, "get_io_device failed", K(uri), KP(io_device));
  } else if (OB_FAIL(io_device->adaptive_exist(uri.ptr(), exist))) {
    CLOG_LOG(WARN, "exist failed", K(uri), KP(io_device), K(exist));
  } else {
    CLOG_LOG(TRACE, "exist success", K(uri), KP(io_device), K(exist));
  }
  release_io_device(io_device);
  return ret;
}

int ObLogExternalStorageIOTaskHandleAdapter::get_file_size(const ObString &uri,
                                                           const ObString &storage_info,
                                                           int64_t &file_size)
{
  int ret = OB_SUCCESS;
  file_size = 0;
  ObIODFileStat file_stat;
  ObIODevice *io_device = NULL;
  if (OB_FAIL(get_and_init_io_device(uri, storage_info, io_device))) {
    CLOG_LOG(WARN, "get_io_device failed", K(uri), KP(io_device));
  } else if (OB_FAIL(io_device->adaptive_stat(uri.ptr(), file_stat))) {
    CLOG_LOG(WARN, "stat io deveice failed", K(uri));
  } else {
    file_size = file_stat.size_;
    CLOG_LOG(TRACE, "get_file_size success", K(uri), KP(io_device), K(file_size));
  }
  release_io_device(io_device);
  return ret;
}

int ObLogExternalStorageIOTaskHandleAdapter::pread(const ObString &uri,
                                                   const ObString &storage_info,
                                                   const int64_t offset,
                                                   char *buf,
                                                   const int64_t read_buf_size,
                                                   int64_t &real_read_size)
{
  ObTimeGuard time_guard("storage pread", 100 * 1000);
  int ret = OB_SUCCESS;
  real_read_size = 0;
  ObIODevice *io_device = NULL;
  ObIOFd io_fd;
  if (OB_FAIL(get_and_init_io_device(uri, storage_info, io_device))) {
    CLOG_LOG(WARN, "get_io_device failed", K(io_fd), K(offset), K(read_buf_size), KP(buf), K(real_read_size));
  } else if (FALSE_IT(time_guard.click("after get_io_device"))) {
  } else if (OB_FAIL(open_io_fd(uri, OPEN_FLAG::READ_FLAG, io_device, io_fd))) {
  } else if (FALSE_IT(time_guard.click("after open_io_fd"))) {
    CLOG_LOG(WARN, "open_io_fd failed", K(io_fd), K(offset), K(read_buf_size), KP(buf), K(real_read_size));
  } else if (OB_FAIL(io_device->pread(io_fd, offset, read_buf_size, buf, real_read_size))) {
    CLOG_LOG(WARN, "pread failed", K(io_fd), K(offset), K(read_buf_size), KP(buf), K(real_read_size), K(time_guard));
  } else if (FALSE_IT(time_guard.click("after pread"))) {
  } else {
    CLOG_LOG(TRACE, "pread success", K(time_guard), K(io_fd), K(offset), K(read_buf_size), KP(buf), K(real_read_size));
  }
  (void)close_io_fd(io_device, io_fd);
  (void)release_io_device(io_device);
  return ret;
}

ObLogExternalStorageIOTask::ObLogExternalStorageIOTask(const ObString &uri,
                                                       const ObString &storage_info,
                                                       RunningStatus *running_status,
                                                       ObLogExternalStorageIOTaskCtx *io_task_ctx,
                                                       ObLogExternalStorageIOTaskHandleIAdapter *adapter)
    : uri_ob_str_(OB_MAX_URI_LENGTH, 0, uri_str_),
      storage_info_ob_str_(OB_MAX_BACKUP_STORAGE_INFO_LENGTH, 0, storage_info_str_),
      generate_ts_(OB_INVALID_TIMESTAMP),
      do_task_ts_(OB_INVALID_TIMESTAMP),
      running_status_(running_status),
      io_task_ctx_(io_task_ctx),
      type_(ObLogExternalStorageIOTaskType::INVALID_TYPE),
      adapter_(adapter)
{
  memset(uri_str_, '\0', OB_MAX_URI_LENGTH);
  memset(storage_info_str_, '\0', OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
  uri_ob_str_.write(uri.ptr(), uri.length());
  storage_info_ob_str_.write(storage_info.ptr(), storage_info.length());
  generate_ts_ = ObTimeUtility::current_time();
}

ObLogExternalStorageIOTask::~ObLogExternalStorageIOTask()
{
  memset(uri_str_, '\0', OB_MAX_URI_LENGTH);
  memset(storage_info_str_, '\0', OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
  uri_ob_str_.reset();
  storage_info_ob_str_.reset();
  generate_ts_ = OB_INVALID_TIMESTAMP;
  do_task_ts_ = OB_INVALID_TIMESTAMP;
  running_status_ = NULL;
  io_task_ctx_ = NULL;
  type_ = ObLogExternalStorageIOTaskType::INVALID_TYPE;
  adapter_ = NULL;
}

int ObLogExternalStorageIOTask::do_task()
{
  int ret = OB_SUCCESS;
  do_task_ts_ = ObTimeUtility::current_time();
  running_status_->thread_id_ = gettid();
  running_status_->status_ = EnumRunningStatus::START_STATUS;
  int64_t io_delay = do_task_ts_ - generate_ts_;
  if (io_delay >= 500 * 1000) {
    CLOG_LOG(WARN, "[io delay] handle io task delay too much", K(io_delay), KPC(this));
  }
  if (OB_FAIL(do_task_())) {
    CLOG_LOG(WARN, "do_task_ failed", KP(this));
  }
  return ret;
}

ObLogExternalStoragePreadTask::ObLogExternalStoragePreadTask(const ObString &uri,
                                                             const ObString &storage_info,
                                                             RunningStatus *running_status,
                                                             ObLogExternalStorageIOTaskCtx *io_task_ctx,
                                                             ObLogExternalStorageIOTaskHandleIAdapter *adapter,
                                                             const int64_t offset,
                                                             const int64_t read_buf_size,
                                                             char *read_buf,
                                                             int64_t &real_read_size)
  : ObLogExternalStorageIOTask(uri, storage_info, running_status, io_task_ctx, adapter),
    offset_(offset),
    read_buf_size_(read_buf_size),
    read_buf_(read_buf),
    real_read_size_(real_read_size)
{
  type_ = ObLogExternalStorageIOTaskType::PREAD_TYPE;
}

ObLogExternalStoragePreadTask::~ObLogExternalStoragePreadTask()
{
  offset_ = -1;
  read_buf_size_ = -1;
  read_buf_ = NULL;
}

int ObLogExternalStoragePreadTask::do_task_()
{
  int ret = OB_SUCCESS;
  int64_t tmp_real_read_size = 0;
  if (OB_FAIL(adapter_->pread(uri_ob_str_, storage_info_ob_str_, offset_,
                              read_buf_, read_buf_size_, tmp_real_read_size))) {
    CLOG_LOG(WARN, "pread failed", KPC(this));
  } else {
    ATOMIC_AAF(&real_read_size_, tmp_real_read_size);
  }
  running_status_->ret_ = ret;
  running_status_->status_ = EnumRunningStatus::FINSHED_STATUS;
  CLOG_LOG(TRACE, "finished pread", KPC(this), K(tmp_real_read_size));
  // NB: don't use io_task_ctx_ or KPC(this) after signal.
  // consider used shared ptr to manage memory release of ObLogExternalStorageIOTaskCtx
  io_task_ctx_->signal();
  return ret;
}

} // end namespace logservice
} // end namespace oceanbase
