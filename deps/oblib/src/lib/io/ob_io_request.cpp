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

#define USING_LOG_PREFIX COMMON
#include "lib/io/ob_io_request.h"
#include "lib/io/ob_io_manager.h"

namespace oceanbase {
using namespace lib;
namespace common {
/**
 * -----------------------------------------------ObIORequest-------------------------------------
 */

bool ObIORequestDesc::is_valid() const
{
  return desc_.is_valid() && fd_.is_valid() && io_size_ > 0 && io_size_ % DIO_READ_ALIGN_SIZE == 0 &&
         reinterpret_cast<const int64_t>(io_buf_) % DIO_READ_ALIGN_SIZE == 0 && io_offset_ >= 0 &&
         io_offset_ % DIO_READ_ALIGN_SIZE == 0;
}

void ObIORequest::destroy()
{
  reset();  // clear list pointer
  inited_ = false;
  need_submit_ = true;
  finished_ = false;
  MEMSET(&iocb_, 0, sizeof(iocb));
  desc_ = ObIODesc();
  fd_.reset();
  io_buf_ = NULL;
  io_offset_ = 0;
  io_size_ = 0;
  deadline_time_ = 0;
  io_time_.reset();
  ret_code_ = ObIORetCode();
  master_ = NULL;
  disk_guard_.reset();
  channel_ = NULL;
  retry_cnt_ = 0;
}

int ObIORequest::open(const ObIOMode mode, const ObIORequestDesc& req_param, ObIOMaster& master, ObDisk& disk)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (mode >= IO_MODE_MAX || !req_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(mode), K(req_param));
  } else {
    desc_ = req_param.desc_;
    io_buf_ = req_param.io_buf_;
    fd_ = req_param.fd_;
    io_offset_ = req_param.io_offset_;
    io_size_ = req_param.io_size_;
    master_ = &master;
    disk_guard_.set_disk(&disk);
    switch (mode) {
      case IO_MODE_READ:
        io_prep_pread(&iocb_, fd_.fd_, io_buf_, io_size_, io_offset_);
        break;
      case IO_MODE_WRITE:
        io_prep_pwrite(&iocb_, fd_.fd_, io_buf_, io_size_, io_offset_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(ERROR, "invalid mode", K(ret), K(mode), K(req_param));
    }

    if (OB_SUCC(ret)) {
      iocb_.data = this;
      inited_ = true;
    }
  }
  return ret;
}

int ObIORequest::finish(const int io_ret, const int sys_errno)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else if (finished_) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "finish twice", K(ret));
  } else {
    ret_code_.io_ret_ = io_ret;
    ret_code_.sys_errno_ = sys_errno;
    finished_ = true;
    io_time_.end_time_ = ObTimeUtility::current_time();
    report_diagnose_info();
    master_->recv_request();
    master_->dec_ref();
  }
  // COMMON_LOG(INFO, "req finish called", K(ret), K(this), "req", *this);
  return ret;
}

void ObIORequest::report_diagnose_info()
{
  if (finished_) {
    ObDisk* disk = get_disk();

    if (OB_ISNULL(disk)) {
      COMMON_LOG(ERROR, "disk is null", K(*this));
    } else if (OB_ISNULL(master_)) {
      COMMON_LOG(ERROR, "io master is null", K(*this));
    } else {
      if (OB_IO_ERROR == ret_code_.io_ret_) {
        disk->record_io_failure(*this, DEFAULT_IO_WAIT_TIME_MS);
      }

      if (io_time_.os_submit_time_ > 0 && io_time_.os_return_time_ > io_time_.os_submit_time_) {
        const int64_t disk_delay = io_time_.os_return_time_ - io_time_.os_submit_time_;
        disk->update_io_stat(desc_.category_, master_->mode_, io_size_, disk_delay);
      }
    }
  }
}

bool ObIORequest::can_retry() const
{
  const int16_t MAX_RETRY_COUNT = 10;
  return need_submit_ && retry_cnt_ >= 0 && retry_cnt_ < MAX_RETRY_COUNT;
}

/**
 * ------------------------------------------------- ObIOMaster ------------------------------------------
 */

ObIOMaster::ObIOMaster()
    : inited_(false),
      need_callback_(true),
      has_finished_(false),
      has_estimated_(false),
      io_info_(),
      callback_(nullptr),
      buf_(nullptr),
      buf_size_(0),
      aligned_offset_(0),
      io_ret_(0),
      finish_count_(0),
      time_(),
      ref_cnt_(0),
      out_ref_cnt_(0),
      cond_(),
      resource_mgr_(nullptr),
      trace_id_(),
      allocator_(nullptr),
      io_error_handler_(nullptr),
      parent_io_master_holder_(),
      recover_io_master_holder_(),
      mode_(IO_MODE_MAX)
{
  MEMSET(requests_, 0, sizeof(requests_));
}

ObIOMaster::~ObIOMaster()
{
  reset();
}

int ObIOMaster::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    COMMON_LOG(WARN, "fail to init thread condition", K(ret));
  }
  return ret;
}

int ObIOMaster::open(const ObIOMode mode, const ObIOInfo& info, ObIOCallback* read_callback)
{
  int ret = OB_SUCCESS;

  if (mode >= IO_MODE_MAX || !info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mode), K(info));
  } else if (OB_FAIL(open(mode, info, read_callback, resource_mgr_, allocator_))) {
    LOG_WARN("failed to init master", K(ret));
  }
  return ret;
}

int ObIOMaster::open(const ObIOMode mode, const ObIOInfo& info, ObIOCallback* callback,
    ObIOResourceManager* resource_mgr, ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  trace_id_ = *ObCurTraceId::get_trace_id();
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret), KP(this), K(*this));
  } else if (mode >= IO_MODE_MAX || !info.is_valid() || OB_ISNULL(resource_mgr) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(mode), K(info), KP(resource_mgr), KP(allocator));
  } else {
    io_info_ = info;
    resource_mgr_ = resource_mgr;
    allocator_ = allocator;
    if (IO_MODE_READ == mode) {  // thread-safe, require concurrent allocator
      if (OB_FAIL(alloc_read_buf(callback))) {
        COMMON_LOG(WARN, "fail to alloc read io buf", K(ret), K(mode));
      }
    } else {  // thread-safe, require concurrent allocator
      if (NULL == callback) {
        if (OB_FAIL(alloc_write_buf())) {
          COMMON_LOG(WARN, "fail to alloc write io buf", K(ret), K(mode));
        }
      } else {
        if (OB_FAIL(alloc_write_buf(callback))) {
          COMMON_LOG(WARN, "fail to alloc write io buf", K(ret), K(mode));
        }
      }
    }
  }

  if (OB_SUCC(ret) && NULL != info.io_error_handler_) {
    if (OB_FAIL(alloc_io_error_handler(info.io_error_handler_))) {
      LOG_WARN("failed to alloc io error handler", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (io_info_.fail_disk_count_ > 0) {
      if (IO_MODE_READ != mode) {
        ret = OB_ERR_SYS;
        LOG_ERROR("only read mode could use recovery", K(ret), K(mode), K_(io_info));
      } else if (OB_FAIL(init_recover_io_master())) {
        COMMON_LOG(WARN, "failed to send recover io request", K(ret), KP(allocator_));
      }
    } else if (OB_FAIL(prepare_request(mode))) {  // thread-safe
      COMMON_LOG(WARN, "fail to prepare request", K(ret), K(info), K(mode));
    }
  }

  if (OB_SUCC(ret)) {
    time_.prepare_delay_ = ObTimeUtility::current_time() - begin_time;
    mode_ = mode;
    inited_ = true;
  }

  return ret;
}

void ObIOMaster::reset()
{
  for (int32_t i = 0; i < MAX_IO_BATCH_NUM; ++i) {
    if (OB_NOT_NULL(requests_[i])) {
      requests_[i]->destroy();
      requests_[i] = NULL;
    }
  }

  if (OB_NOT_NULL(buf_) && OB_ISNULL(callback_)) {
    if (OB_ISNULL(resource_mgr_)) {
      LOG_ERROR("resource_mgr_ is NULL", K(*this));
    } else {
      resource_mgr_->free_memory(buf_);
    }
    buf_ = NULL;
  }

  if (OB_NOT_NULL(callback_)) {
    callback_->~ObIOCallback();
    callback_ = NULL;
    buf_ = NULL;
  }

  if (NULL != io_error_handler_) {
    io_error_handler_->~ObIIOErrorHandler();
    if (NULL != allocator_) {
      allocator_->free(io_error_handler_);
    }
  }

  // ignore ret
  if (OB_SUCCESS != io_info_.notice_finish()) {
    LOG_ERROR("failed to notice finish", K(*this));
  }

  inited_ = false;
  need_callback_ = true;
  has_finished_ = false;
  has_estimated_ = false;
  io_info_.reset();
  // callback_ has been reset
  buf_ = NULL;
  buf_size_ = 0;
  aligned_offset_ = 0;
  io_ret_ = 0;
  finish_count_ = 0;
  // requests_ has been reset
  time_.reset();
  ref_cnt_ = 0;
  out_ref_cnt_ = 0;

  resource_mgr_ = NULL;
  trace_id_.reset();
  allocator_ = NULL;
  io_error_handler_ = NULL;
  parent_io_master_holder_.reset();
  recover_io_master_holder_.reset();
  mode_ = IO_MODE_MAX;
  cond_.destroy();
}

int ObIOMaster::alloc_read_buf(ObIOCallback* callback)
{
  int ret = OB_SUCCESS;

  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(resource_mgr_) || !io_info_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "invalid status", K(ret), K(io_info_), KP(resource_mgr_));
  } else if (OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "read callback must not null", K(ret), K_(io_info));
  } else if (callback->size() > ObIOCallback::CALLBACK_BUF_SIZE) {
    ret = OB_ERR_SYS;
    COMMON_LOG(ERROR, "callback buf not enough", K(ret), "size", callback->size());
  } else {  // has callback, copy callback and use it to allocate buf
    if (OB_FAIL(callback->deep_copy(callback_buf_, callback->size(), callback_))) {
      callback_ = NULL;
      COMMON_LOG(WARN, "Fail to deep copy callback function, ", K(ret));
    } else if (OB_ISNULL(callback_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("callback must not null", K(ret));
    } else {
      if (OB_FAIL(callback_->alloc_io_buf(buf_, buf_size_, aligned_offset_))) {
        COMMON_LOG(WARN, "Fail to alloc io buf, ", K(ret), K(buf_));
      } else if (OB_ISNULL(buf_)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "buf is null", K(ret));
      } else if (0 != ((int64_t)(buf_) % DIO_READ_ALIGN_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "The io buffer is not aligned, ", K(ret));
      }
    }
  }
  return ret;
}

int ObIOMaster::alloc_io_error_handler(const ObIIOErrorHandler* io_error_handler)
{
  int ret = OB_SUCCESS;
  char* buf = NULL;

  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(resource_mgr_) || !io_info_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "invalid status", K(ret), K(io_info_), KP(resource_mgr_));
  } else if (OB_ISNULL(io_error_handler)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "io_error_handler must not null", K(ret), K_(io_info));
  } else {
    const int64_t copy_size = io_error_handler->get_deep_copy_size();
    if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(copy_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "failed to alloc callback buf ", K(ret), K(copy_size));
    } else if (OB_FAIL(io_error_handler->deep_copy(buf, copy_size, io_error_handler_))) {
      COMMON_LOG(WARN, "Fail to deep copy io_error_handler", K(ret));
    } else if (OB_ISNULL(io_error_handler_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("io error handler must not null", K(ret));
    } else {
      buf = NULL;
      if (OB_FAIL(io_error_handler_->set_read_io_buf(buf_, buf_size_, aligned_offset_))) {
        LOG_WARN("failed to set read io buf", K(ret), KP(buf_), K(buf_size_), K(aligned_offset_));
      }
    }
  }

  if (NULL != buf) {
    allocator_->free(buf);
    buf = NULL;
  }
  return ret;
}

int ObIOMaster::alloc_write_buf()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(resource_mgr_) || !io_info_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "invalid status", K(ret), K(io_info_), KP(resource_mgr_));
  } else {
    buf_size_ = io_info_.size_ + DIO_READ_ALIGN_SIZE;
    if (OB_UNLIKELY(OB_ISNULL(buf_ = (char*)resource_mgr_->alloc_memory(buf_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "Fail to allocate memory, ", K(ret));
    } else {
      // align memory
      char* io_buf = (char*)upper_align((int64_t)buf_, DIO_READ_ALIGN_SIZE);
      for (int32_t i = 0; OB_SUCC(ret) && i < io_info_.batch_count_; ++i) {
        const ObIOPoint& io_point = io_info_.io_points_[i];
        if (0 != io_info_.io_points_[i].offset_ % DIO_READ_ALIGN_SIZE ||
            0 != io_info_.io_points_[i].size_ % DIO_READ_ALIGN_SIZE) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(WARN, "io info is not aligned", K(ret), "io_point", io_info_.io_points_[i]);
        } else {
          MEMCPY(io_buf, io_point.write_buf_, io_point.size_);
          io_buf += io_point.size_;
        }
      }
    }
  }
  return ret;
}

int ObIOMaster::alloc_write_buf(ObIOCallback* callback)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(resource_mgr_) || !io_info_.is_valid()) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "invalid status", K(ret), K(io_info_), KP(resource_mgr_));
  } else if (OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "write callback is null", K(ret), K_(io_info));
  } else if (callback->size() > ObIOCallback::CALLBACK_BUF_SIZE) {
    ret = OB_ERR_SYS;
    COMMON_LOG(ERROR, "callback buf not enough", K(ret), "size", callback->size());
  } else {  // has callback, copy callback and use it to allocate buf
    if (OB_FAIL(callback->deep_copy(callback_buf_, callback->size(), callback_))) {
      callback_ = NULL;
      COMMON_LOG(WARN, "Fail to deep copy callback function, ", K(ret));
    } else if (OB_ISNULL(callback_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("callback is null", K(ret));
    } else {
      if (OB_FAIL(callback_->alloc_io_buf(buf_, buf_size_, aligned_offset_))) {
        COMMON_LOG(WARN, "Fail to alloc io buf, ", K(ret), K(buf_));
      } else if (OB_ISNULL(buf_)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "buf is null", K(ret));
      } else if (0 != ((int64_t)(buf_) % DIO_READ_ALIGN_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "The io buffer is not aligned, ", K(ret));
      } else {
        // align memory
        char* io_buf = (char*)upper_align((int64_t)buf_, DIO_READ_ALIGN_SIZE);
        for (int32_t i = 0; OB_SUCC(ret) && i < io_info_.batch_count_; ++i) {
          const ObIOPoint& io_point = io_info_.io_points_[i];
          if (0 != io_info_.io_points_[i].offset_ % DIO_READ_ALIGN_SIZE ||
              0 != io_info_.io_points_[i].size_ % DIO_READ_ALIGN_SIZE) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "io info is not aligned", K(ret), "io_point", io_info_.io_points_[i]);
          } else {
            MEMCPY(io_buf, io_point.write_buf_, io_point.size_);
            io_buf += io_point.size_;
          }
        }
      }
    }
  }
  return ret;
}

int ObIOMaster::prepare_request(const ObIOMode mode)
{
  int ret = OB_SUCCESS;

  const ObIOInfo& info = io_info_;
  char* io_buf = (char*)upper_align((int64_t)buf_, DIO_READ_ALIGN_SIZE);  // io buf always aligned
  ObDisk* disk = NULL;
  for (int32_t i = 0; OB_SUCC(ret) && i < info.batch_count_; ++i) {
    const ObIOPoint& current_point = info.io_points_[i];
    // calculate real offset and size for request
    int64_t io_offset = 0;
    int64_t io_size = 0;
    if (IO_MODE_READ == mode) {
      if (1 == info.batch_count_) {  // for only 1 point
        align_offset_size(current_point.offset_, current_point.size_, io_offset, io_size);
      } else {  // for 2 or more points
        /*
         *   |<------------------------------------total_io_size-------------------------------------->|
         *                      |<---------------------info.size--------------------->|
         *   |<--delta_offset-->|<---size_1--->|<---size_2(aligned)--->|<---size_3--->|<--delta_size-->|
         *   |----------------- |--------------|-----------------------|-------------------------------|------
         * io_offset         offset_1       offset_2               offset_3
         * ALIGNED                          ALIGNED                ALIGNED                          ALIGNED
         */
        io_offset = lower_align(current_point.offset_, DIO_READ_ALIGN_SIZE);
        io_size = current_point.offset_ - io_offset + current_point.size_;
        if ((info.batch_count_ - 1) == i) {  // last point
          const int64_t delta_offset =
              info.io_points_[0].offset_ - lower_align(info.io_points_[0].offset_, DIO_READ_ALIGN_SIZE);
          const int64_t delta_size =
              upper_align(info.size_ + delta_offset, DIO_READ_ALIGN_SIZE) - info.size_ - delta_offset;
          io_size = current_point.size_ /* size_3 */ + delta_size;
        }
      }
    } else {  // mode is checked by public method for write and append, always set the true offset and size
      io_offset = current_point.offset_;
      io_size = current_point.size_;
    }

    // allocate request and init it
    ObIORequest* request = NULL;
    ObDiskManager& disk_mgr = OB_IO_MANAGER.get_disk_manager();
    ObDiskGuard guard;
    if (OB_FAIL(disk_mgr.get_disk_with_guard(current_point.fd_, guard))) {  // thread-safe, even add or delete disk
      COMMON_LOG(WARN, "get_disk failed", K(ret), K(current_point), K(disk));
    } else if (OB_ISNULL(disk = guard.get_disk())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "disk is null", K(ret));
    } else if (disk->get_admin_status() != DISK_USING) {  // thread-safe, only send req when disk is ready
      ret = OB_STATE_NOT_MATCH;
      COMMON_LOG(WARN, "fail to check admin status", K(ret));
    } else if (OB_ISNULL(requests_[i])) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "request is null", K(ret), K(i));
    } else {
      request = requests_[i];
      request->io_time_.begin_time_ = ObTimeUtility::current_time();
      ObIORequestDesc req_param;
      req_param.desc_ = info.io_desc_;
      req_param.fd_ = current_point.fd_;
      req_param.io_size_ = static_cast<int32_t>(io_size);
      req_param.io_buf_ = io_buf;
      req_param.io_offset_ = io_offset;
      if (OB_FAIL(request->open(mode, req_param, *this, *disk))) {  // thread-safe
        COMMON_LOG(WARN, "fail to init request", K(ret), KP(io_buf), K(current_point));
      } else {
        io_buf += io_size;
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < info.batch_count_; ++i) {
      requests_[i]->master_->inc_ref();
    }
  }
  return ret;
}

int ObIOMaster::init_recover_io_master()
{
  int ret = OB_SUCCESS;
  ObBitSet<OB_MAX_DISK_NUMBER> recover_disk_idx_set;
  ObIOMaster* recover_io_master = NULL;
  int64_t recover_request_num = 0;

  // not inited, so just check needed variables
  if (OB_ISNULL(resource_mgr_) || OB_ISNULL(io_error_handler_) || OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL != parent_io_master_holder_.get_ptr()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cannot alloc recover_io_master_ twice", K(ret), K(parent_io_master_holder_));
  } else if (OB_FAIL(io_error_handler_->get_recover_request_num(recover_request_num))) {
    LOG_WARN("fail to get recover request num", K(ret));
  } else if (OB_FAIL(resource_mgr_->alloc_master(recover_request_num, recover_io_master))) {  // thread-safe //todo
    COMMON_LOG(WARN, "fail to alloc master", K(ret), KP(recover_io_master));
  } else if (OB_ISNULL(recover_io_master)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "master is null", K(ret));
  } else {
    recover_io_master_holder_.hold(recover_io_master);
    recover_io_master->time_.begin_time_ = ObTimeUtility::current_time();
    recover_io_master->parent_io_master_holder_.hold(this);
    recover_io_master->resource_mgr_ = resource_mgr_;
    recover_io_master->allocator_ = allocator_;
    recover_io_master->trace_id_ = *ObCurTraceId::get_trace_id();
    recover_io_master->mode_ = IO_MODE_READ;  // only support read recover

    for (int64_t i = 0; OB_SUCC(ret) && i < io_info_.fail_disk_count_; ++i) {
      if (OB_FAIL(recover_disk_idx_set.add_member(io_info_.fail_disk_ids_[i]))) {
        LOG_WARN("failed to add recover index", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(io_error_handler_->init_recover_io_master(recover_disk_idx_set, allocator_, recover_io_master))) {
        LOG_WARN("failed to init recover io master", K(ret), KP(allocator_), KP(recover_io_master));
      }
    }
  }

  return ret;
}

int ObIOMaster::send_request()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "request not ready", K(ret), KP(this));
  } else if (NULL != recover_io_master_holder_.get_ptr()) {
    finish_count_ = io_info_.batch_count_;  // no request need send, set it all done
    if (OB_FAIL(recover_io_master_holder_.get_ptr()->send_request())) {
      LOG_WARN("failed to send recover io request", K(ret));
    }
  } else {
    time_.send_time_ = ObTimeUtility::current_time();
    int first_err_code = OB_SUCCESS;
    for (int32_t i = 0; i < io_info_.batch_count_; ++i) {
      ObIORequest* req = requests_[i];
      ObDisk* disk = req->get_disk();
      DiskHolder holder(disk);
      if (OB_FAIL(ret)) {
        ret = OB_CANCELED;
        req->finish(ret, 0);  // thread-safe
      } else if (disk->get_admin_status() != DISK_USING) {
        ret = OB_CANCELED;
        first_err_code = ret;
        COMMON_LOG(WARN, "disk status not match", K(ret), K(disk->get_admin_status()));
        req->finish(ret, 0);  // thread-safe
      } else if (OB_FAIL(disk->send_request(*req))) {
        first_err_code = ret;
        COMMON_LOG(WARN, "fail to send request", K(ret));
      }
    }
    ret = first_err_code == ret ? ret : first_err_code;
  }
  return ret;
}

int ObIOMaster::recv_request()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "request not ready", K(ret));
  } else if (ATOMIC_AAF(&finish_count_, 1) == io_info_.batch_count_) {
    time_.recv_time_ = ObTimeUtility::current_time();
    for (int32_t i = 0; i < io_info_.batch_count_; ++i) {
      if (OB_UNLIKELY(OB_SUCCESS != requests_[i]->ret_code_.io_ret_)) {
        io_ret_ = requests_[i]->ret_code_.io_ret_;
        if (OB_CANCELED != io_ret_) {
          LOG_WARN("recv request error", K(io_ret_), K(i), K(io_info_), "request", *requests_[i]);
        }
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_IO_ERROR == io_ret_ && NULL == parent_io_master_holder_.get_ptr() && NULL != io_error_handler_) {
        if (OB_FAIL((init_recover_io_master()))) {
          LOG_WARN("failed to init recover io master", K(ret), K_(io_info));
        }
      } else {
        if (OB_FAIL(send_callback())) {
          LOG_WARN("failed to send callback", K(ret), K_(io_info));
        }
      }
    }
  } else if (finish_count_ > io_info_.batch_count_) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "Wrong status", K(ret), K(finish_count_), "batch_count", io_info_.batch_count_);
  }
  COMMON_LOG(DEBUG, "master get one finished req", K(ret), KP(this), "master", *this);
  return ret;
}

int ObIOMaster::send_callback()
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (finish_count_ != io_info_.batch_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("not all io request finished, cannot send callback", K(ret), K_(finish_count), K_(io_info));
  } else if (need_callback_ &&
             can_callback()) {  // if need callback, the finish signal will be sent in the callback thread.
    inc_ref();
    time_.callback_enqueue_time_ = ObTimeUtility::current_time();
    if (OB_FAIL(OB_IO_MANAGER.get_callback_runner().enqueue_callback(*this))) {
      if (OB_CANCELED != ret) {
        COMMON_LOG(WARN, "Fail to enqueue callback", K(ret));
      }
      dec_ref();
    }
  } else {  // finish directly
    if (OB_FAIL(notify_finished())) {
      COMMON_LOG(WARN, "fail to notify finish", K(ret));
    }
  }
  return ret;
}

int ObIOMaster::notify_finished()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "not init", K(ret));
  } else {
    time_.end_time_ = ObTimeUtility::current_time();
    has_finished_ = true;
    if (NULL != parent_io_master_holder_.get_ptr()) {
      if (OB_FAIL(parent_io_master_holder_.get_ptr()->send_callback())) {
        LOG_WARN("failed to notice parent io master recv send_callback", K(ret), K(*this));
      }
      parent_io_master_holder_.reset();
    } else {
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(guard.get_ret())) {
        COMMON_LOG(ERROR, "Fail to lock master condition, ", K(ret));
      } else if (OB_FAIL(cond_.signal())) {
        COMMON_LOG(ERROR, "Fail to signal master condition, ", K(ret));
      }
    }
  }
  // COMMON_LOG(INFO, "notify finished");
  return ret;
}

void ObIOMaster::cancel()
{
  if (inited_ && !has_finished_) {
    int ret = OB_SUCCESS;
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(guard.get_ret())) {
      COMMON_LOG(WARN, "fail to guard condition", K(ret));
    } else if (has_finished_) {
      // do nothing
    } else {
      need_callback_ = false;
      for (int32_t i = 0; i < io_info_.batch_count_; ++i) {
        ObIORequest* req = requests_[i];
        if (OB_NOT_NULL(req) && !req->finished_) {
          req->need_submit_ = false;
          if (req->io_time_.os_submit_time_ != 0) {  // cancel io already submitted
            req->channel_->cancel(*req);
          }
        }
      }  // end for-loop
    }

    if (NULL != recover_io_master_holder_.get_ptr()) {
      recover_io_master_holder_.get_ptr()->cancel();
    }
  }
}

void ObIOMaster::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObIOMaster::dec_ref()
{
  int ret = OB_SUCCESS;
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(ERROR, "bug: ref_cnt < 0", K(ret), K(tmp_ref), K(lbt()));
    ob_abort();
  } else if (0 == tmp_ref) {
    reset();
    if (OB_FAIL(OB_IO_MANAGER.get_resource_manager().free_master(this))) {
      COMMON_LOG(WARN, "fail to free master", K(ret));
    }
  }
}

void ObIOMaster::inc_out_ref()
{
  ATOMIC_INC(&out_ref_cnt_);
}

void ObIOMaster::dec_out_ref()
{
  if (0 == ATOMIC_SAF(&out_ref_cnt_, 1)) {
    cancel();
  }
}

} /* namespace common */
} /* namespace oceanbase */
