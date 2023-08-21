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

#include "lib/stat/ob_diagnose_info.h"
#include "share/io/ob_io_define.h"
#include "share/io/ob_io_struct.h"
#include "share/io/ob_io_manager.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;


/******************             IOMode              **********************/
static const char *read_mode_name = "READ";
static const char *read_mode_name_short = "R";
static const char *write_mode_name = "WRITE";
static const char *write_mode_name_short = "W";
const char *oceanbase::common::get_io_mode_string(const ObIOMode mode)
{
  const char *ret_name = "UNKNOWN";
  switch (mode) {
    case ObIOMode::READ:
      ret_name = read_mode_name;
      break;
    case ObIOMode::WRITE:
      ret_name = write_mode_name;
      break;
    default:
      break;
  }
  return ret_name;
}

ObIOMode oceanbase::common::get_io_mode_enum(const char *mode_string)
{
  ObIOMode mode = ObIOMode::MAX_MODE;
  if (0 == strncasecmp(mode_string, read_mode_name, strlen(mode_string))
      || 0 == strncasecmp(mode_string, read_mode_name_short, strlen(read_mode_name_short))) {
    mode = ObIOMode::READ;
  } else if (0 == strncasecmp(mode_string, write_mode_name, strlen(write_mode_name))
      || 0 == strncasecmp(mode_string, write_mode_name_short, strlen(write_mode_name_short))) {
    mode = ObIOMode::WRITE;
  }
  return mode;
}

const char *oceanbase::common::get_io_sys_group_name(ObIOModule module)
{
  const char *ret_name = "UNKNOWN";
  switch (module) {
    case ObIOModule::SLOG_IO:
      ret_name = "SLOG_IO";
      break;
    case ObIOModule::CALIBRATION_IO:
      ret_name = "CALIBRATION_IO";
      break;
    case ObIOModule::DETECT_IO:
      ret_name = "DETECT_IO";
      break;
    case ObIOModule::DIRECT_LOAD_IO:
      ret_name = "DIRECT_LOAD_IO";
      break;
    case ObIOModule::SHARED_BLOCK_RW_IO:
      ret_name = "SHARED_BLOCK_ReaderWriter_IO";
      break;
    case ObIOModule::SSTABLE_WHOLE_SCANNER_IO:
      ret_name = "SSTABLE_WHOLE_SCANNER_IO";
      break;
    case ObIOModule::INSPECT_BAD_BLOCK_IO:
      ret_name = "INSPECT_BAD_BLOCK_IO";
      break;
    case ObIOModule::SSTABLE_INDEX_BUILDER_IO:
      ret_name = "SSTABLE_INDEX_BUILDER_IO";
      break;
    case ObIOModule::BACKUP_READER_IO:
      ret_name = "BACKUP_READER_IO";
      break;
    case ObIOModule::BLOOM_FILTER_IO:
      ret_name = "BLOOM_FILTER_IO";
      break;
    case ObIOModule::SHARED_MACRO_BLOCK_MGR_IO:
      ret_name = "SHARED_MACRO_BLOCK_MGR_IO";
      break;
    case ObIOModule::INDEX_BLOCK_TREE_CURSOR_IO:
      ret_name = "INDEX_BLOCK_TREE_CURSOR_IO";
      break;
    case ObIOModule::MICRO_BLOCK_CACHE_IO:
      ret_name = "MICRO_BLOCK_CACHE_IO";
      break;
    case ObIOModule::ROOT_BLOCK_IO:
      ret_name = "ROOT_BLOCK_IO";
      break;
    case ObIOModule::TMP_PAGE_CACHE_IO:
      ret_name = "TMP_PAGE_CACHE_IO";
      break;
    case ObIOModule::INDEX_BLOCK_MICRO_ITER_IO:
      ret_name = "INDEX_BLOCK_MICRO_ITER_IO";
      break;
    case ObIOModule::HA_COPY_MACRO_BLOCK_IO:
      ret_name = "HA_COPY_MACRO_BLOCK_IO";
      break;
    case ObIOModule::LINKED_MACRO_BLOCK_IO:
      ret_name = "LINKED_MACRO_BLOCK_IO";
      break;
    case ObIOModule::HA_MACRO_BLOCK_WRITER_IO:
      ret_name = "HA_MACRO_BLOCK_WRITER_IO";
      break;
    case ObIOModule::TMP_TENANT_MEM_BLOCK_IO:
      ret_name = "TMP_TENANT_MEM_BLOCK_IO";
      break;
    default:
      break;
  }
  return ret_name;
}

/******************             IOFlag              **********************/
ObIOFlag::ObIOFlag()
  : flag_(0)
{

}

ObIOFlag::~ObIOFlag()
{

}

void ObIOFlag::reset()
{
  flag_ = 0;
}

bool ObIOFlag::is_valid() const
{
  return mode_ >= 0 && mode_ < static_cast<int>(ObIOMode::MAX_MODE)
    && group_id_ >= 0
    && wait_event_id_ > 0;
}

void ObIOFlag::set_mode(ObIOMode mode)
{
  mode_ = static_cast<int>(mode);
}

ObIOMode ObIOFlag::get_mode() const
{
  return static_cast<ObIOMode>(mode_);
}

void ObIOFlag::set_group_id(ObIOModule module)
{
  group_id_ = THIS_WORKER.get_group_id() != 0 ? THIS_WORKER.get_group_id() : static_cast<int64_t>(module);
}

void ObIOFlag::set_group_id(int64_t group_id)
{
  group_id_ = group_id;
}

void ObIOFlag::set_wait_event(int64_t wait_event_id)
{
  wait_event_id_ = wait_event_id;
}

int64_t ObIOFlag::get_group_id() const
{
  return group_id_;
}

ObIOModule ObIOFlag::get_io_module() const
{
  return static_cast<ObIOModule>(group_id_);
}

int64_t ObIOFlag::get_wait_event() const
{
  return wait_event_id_;
}

void ObIOFlag::set_read()
{
  set_mode(ObIOMode::READ);
}

bool ObIOFlag::is_read() const
{
  return static_cast<int>(ObIOMode::READ) == mode_;
}

void ObIOFlag::set_write()
{
  set_mode(ObIOMode::WRITE);
}

bool ObIOFlag::is_write() const
{
  return static_cast<int>(ObIOMode::WRITE) == mode_;
}

void ObIOFlag::set_sync()
{
  is_sync_ = true;
}

void ObIOFlag::set_async()
{
  is_sync_ = false;
}

bool ObIOFlag::is_sync() const
{
  return is_sync_;
}

void ObIOFlag::set_unlimited(const bool is_unlimited)
{
  is_unlimited_ = is_unlimited;
}

void ObIOFlag::set_detect(const bool is_detect)
{
  is_detect_ = is_detect;
}

void ObIOFlag::set_time_detect(const bool is_time_detect)
{
  is_time_detect_ = is_time_detect;
}

bool ObIOFlag::is_unlimited() const
{
  return is_unlimited_;
}

bool ObIOFlag::is_detect() const
{
  return is_detect_;
}

bool ObIOFlag::is_time_detect() const
{
  return is_time_detect_;
}

/******************             IOCallback              **********************/
ObIOCallback::ObIOCallback()
  : compat_mode_(static_cast<lib::Worker::CompatMode>(lib::get_compat_mode()))
{

}

ObIOCallback::~ObIOCallback()
{

}

int ObIOCallback::process(const char *data_buffer, const int64_t size)
{
  lib::set_compat_mode(compat_mode_);
  return inner_process(data_buffer, size);
}

int ObIOCallback::deep_copy(char *buf, const int64_t buf_len, ObIOCallback *&copied_callback) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(inner_deep_copy(buf, buf_len, copied_callback))) {
    copied_callback->compat_mode_ = compat_mode_;
  }
  return ret;
}

/******************             IOInfo              **********************/
ObIOInfo::ObIOInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    fd_(),
    offset_(0),
    size_(0),
    flag_(),
    buf_(nullptr),
    callback_(nullptr)
{

}

ObIOInfo::~ObIOInfo()
{

}

void ObIOInfo::reset()
{
  tenant_id_ = 0;
  fd_.reset();
  offset_ = 0;
  size_ = 0;
  flag_.reset();
  buf_ = nullptr;
  callback_ = nullptr;
}

bool ObIOInfo::is_valid() const
{
  return tenant_id_ > 0
    && fd_.is_valid()
    && offset_ >= 0
    && size_ > 0
    && flag_.is_valid()
    && (flag_.is_read() || nullptr != buf_);
}

/******************             IOTimeLog              **********************/

ObIOTimeLog::ObIOTimeLog()
  : begin_ts_(0),
    enqueue_ts_(0),
    dequeue_ts_(0),
    submit_ts_(0),
    return_ts_(0),
    callback_enqueue_ts_(0),
    callback_dequeue_ts_(0),
    callback_finish_ts_(0),
    end_ts_(0)
{
}

ObIOTimeLog::~ObIOTimeLog()
{

}

void ObIOTimeLog::reset()
{
  begin_ts_ = 0;
  enqueue_ts_ = 0;
  dequeue_ts_ = 0;
  submit_ts_ = 0;
  return_ts_ = 0;
  callback_enqueue_ts_ = 0;
  callback_dequeue_ts_ = 0;
  callback_finish_ts_ = 0;
  end_ts_ = 0;
}

int64_t oceanbase::common::get_io_interval(const int64_t &end_time, const int64_t &begin_time)
{
  int64_t ret_time = end_time - begin_time;
  static const int64_t max_io_time = 600L * 1000L * 1000L; // 600s
  ret_time = max(ret_time, 0);
  ret_time = min(ret_time, max_io_time);
  return ret_time;
}

/******************             IORetCode              **********************/

ObIORetCode::ObIORetCode()
  : io_ret_(OB_SUCCESS),
    fs_errno_(0)
{

}

ObIORetCode::ObIORetCode(int io_ret, int fs_errno)
  : io_ret_(io_ret),
    fs_errno_(fs_errno)
{

}

ObIORetCode::~ObIORetCode()
{

}

void ObIORetCode::reset()
{
  io_ret_ = OB_SUCCESS;
  fs_errno_ = 0;
}

/******************             IORequest              **********************/

ObIORequest::ObIORequest()
  : is_inited_(false),
    is_finished_(false),
    is_canceled_(false),
    has_estimated_(false),
    io_info_(),
    deadline_ts_(0),
    sender_index_(0),
    control_block_(nullptr),
    raw_buf_(nullptr),
    io_buf_(nullptr),
    io_offset_(0),
    io_size_(0),
    complete_size_(0),
    time_log_(),
    channel_(nullptr),
    sender_(nullptr),
    ref_cnt_(0),
    out_ref_cnt_(0),
    cond_(),
    trace_id_(),
    ret_code_(),
    retry_count_(0),
    tenant_io_mgr_(),
    copied_callback_(nullptr),
    callback_buf_size_(0),    
    callback_buf_()
{

}

ObIORequest::~ObIORequest()
{
  destroy();
}

int ObIORequest::init(const ObIOInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io request init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    LOG_WARN("init request condition failed", K(ret));
  } else {
    time_log_.begin_ts_ = ObTimeUtility::fast_current_time();
    io_info_ = info;
    if (nullptr == io_info_.fd_.device_handle_) {
      io_info_.fd_.device_handle_ = THE_IO_DEVICE; // for test
    }
    trace_id_ = *ObCurTraceId::get_trace_id();
    if (nullptr != info.callback_ && OB_FAIL(info.callback_->deep_copy(
            callback_buf_, callback_buf_size_, copied_callback_))) {
      LOG_WARN("deep copy io callback failed", K(ret), K(*info.callback_));
    } else if (io_info_.flag_.is_write() && OB_FAIL(alloc_io_buf())) {
      // alloc buffer for write request when ObIORequest init
      LOG_WARN("alloc io buffer for write failed", K(ret));
    } else {
      // read buf allocation is delayed to before_submit
      // but aligned offset and size need calculate here for calculating deadline_ts
      align_offset_size(info.offset_, info.size_, io_offset_, io_size_);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(tenant_io_mgr_.get_ptr())) {
      tenant_io_mgr_.get_ptr()->io_usage_.record_request_start(*this);
    }
    is_inited_ = true;
  }
  return ret;
}

void ObIORequest::destroy()
{
  is_inited_ = false;
  is_finished_ = false;
  is_canceled_ = false;
  has_estimated_ = false;
  deadline_ts_ = 0;
  sender_index_ = 0;
  if (nullptr != control_block_ && nullptr != io_info_.fd_.device_handle_) {
    io_info_.fd_.device_handle_->free_iocb(control_block_);
    control_block_ = nullptr;
  }
  io_info_.reset();
  free_io_buffer();
  io_buf_ = nullptr;
  io_offset_ = 0;
  io_size_ = 0;
  complete_size_ = 0;
  time_log_.reset();
  channel_ = nullptr;
  sender_ = nullptr;
  ref_cnt_ = 0;
  out_ref_cnt_ = 0;
  cond_.destroy();
  trace_id_.reset();
  ret_code_.reset();
  retry_count_ = 0;
  if (nullptr != copied_callback_) {
    copied_callback_->~ObIOCallback();
    copied_callback_ = nullptr;
  }
  callback_buf_size_ = 0;
  tenant_io_mgr_.reset();
  // callback_buf_ needn't reset
}

int64_t ObIORequest::get_data_size() const
{
  int64_t data_size = 0;
  if (io_info_.flag_.is_sync()) {
    data_size = complete_size_;
  } else {
    const int64_t aligned_offset = lower_align(io_info_.offset_, DIO_READ_ALIGN_SIZE);
    data_size = min(io_info_.size_, max(0, complete_size_ - (io_info_.offset_ - aligned_offset)));
  }
  return data_size;
}

int64_t ObIORequest::get_group_id() const
{
  return io_info_.flag_.get_group_id();
}

uint64_t ObIORequest::get_io_usage_index()
{
  uint64_t index = 0;
  if (!is_user_group(get_group_id())) {
    //other group or sys group , do nothing
  } else {
    index = tenant_io_mgr_.get_ptr()->get_usage_index(get_group_id());
  }
  return index;
}

const char *ObIORequest::get_data()
{
  const char *buf = nullptr;
  if (nullptr != copied_callback_) {
    buf = copied_callback_->get_data();
  } else if (io_info_.flag_.is_sync()) {
    buf = io_buf_;
  } else {
    // re-calculate with const parameters, in case of partial return change aligned_buf and so on.

    //todo QILU: buf = get_user_data_buf()，完成no_callback memcpy改造后替换
    buf = get_io_data_buf();
  }
  return buf;
}

const char *ObIORequest::get_io_data_buf()
{
  char *buf = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == raw_buf_)) {
    LOG_ERROR("raw buf is null, maybe has been recycle");
  } else {
    const int64_t aligned_offset = lower_align(io_info_.offset_, DIO_READ_ALIGN_SIZE);
    char *aligned_buf = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), DIO_READ_ALIGN_SIZE));
    buf = aligned_buf + io_info_.offset_ - aligned_offset;
  }
  return buf;
}

const ObIOFlag &ObIORequest::get_flag() const
{
  return io_info_.flag_;
}

ObIOMode ObIORequest::get_mode() const
{
  return io_info_.flag_.get_mode();
}

int ObIORequest::alloc_io_buf()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!io_info_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io info", K(ret), K(io_info_));
  } else if (io_info_.flag_.is_sync()) { // for pread and pwrite, reuse user buf
    io_offset_ = io_info_.offset_;
    io_size_ = io_info_.size_;
    io_buf_ = const_cast<char *>(io_info_.buf_);
  } else {
    ret = alloc_aligned_io_buf();
  }
  return ret;
}

int ObIORequest::alloc_aligned_io_buf()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!io_info_.is_valid()
      || (!io_info_.flag_.is_read() && !io_info_.flag_.is_write()))) { // only for read and write
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io info", K(ret), K(io_info_));
  } else if (io_info_.flag_.is_write() // write io must aligned
      && OB_UNLIKELY(!is_io_aligned(io_info_.offset_) || !is_io_aligned(io_info_.size_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write io info not aligned", K(ret), K(io_info_));
  } else {
    align_offset_size(io_info_.offset_, io_info_.size_, io_offset_, io_size_);
    const int64_t io_buffer_size = io_size_ + DIO_READ_ALIGN_SIZE;
    if (OB_ISNULL(tenant_io_mgr_.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant io manager is null", K(ret));
    } else if (OB_ISNULL(raw_buf_ = tenant_io_mgr_.get_ptr()->io_allocator_.alloc(io_buffer_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(io_size_));
    } else {
      io_buf_ = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), DIO_READ_ALIGN_SIZE));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(io_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf is null", K(ret));
    } else if (!is_io_aligned((int64_t)io_buf_)
        || !is_io_aligned(io_size_)
        || !is_io_aligned(io_offset_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The io buffer is not aligned", K(ret), K(io_info_), KP(copied_callback_),
          KP(io_buf_), K(io_offset_), K(io_offset_));
    } else if (io_info_.flag_.is_write()) {
      MEMCPY(io_buf_, io_info_.buf_, io_info_.size_);
    }
  }
  return ret;
}

int ObIORequest::prepare()
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg("prepare", 100000); //100ms
  if (OB_ISNULL(io_info_.fd_.device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle is null", K(ret), K(*this));
  } else if (OB_ISNULL(control_block_) && OB_ISNULL(control_block_ = io_info_.fd_.device_handle_->alloc_iocb())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc io control block failed", K(ret), K(*this));
  } else if (FALSE_IT(tg.click("alloc_iocb"))) {
  } else if (OB_ISNULL(io_buf_) && OB_FAIL(alloc_io_buf())) {
    // delayed alloc buffer for read request here to reduce memory usage when io request enqueue
    LOG_WARN("alloc io buffer for read failed", K(ret), K(*this));
  } else if (FALSE_IT(tg.click("alloc_buf"))) {
  } else {
    if (io_info_.flag_.is_read()) {
      if (OB_FAIL(io_info_.fd_.device_handle_->io_prepare_pread(
              io_info_.fd_,
              io_buf_,
              io_size_,
              io_offset_,
              control_block_,
              this/*data*/))) {
        LOG_WARN("prepare io read failed", K(ret), K(*this));
      }
      tg.click("prepare_read");
    } else if (io_info_.flag_.is_write()) {
      if (OB_FAIL(io_info_.fd_.device_handle_->io_prepare_pwrite(
              io_info_.fd_,
              io_buf_,
              io_size_,
              io_offset_,
              control_block_,
              this/*data*/))) {
        LOG_WARN("prepare io write failed", K(ret), K(*this));
      }
      tg.click("prepare_write");
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported io mode", K(ret), K(*this));
    }
  }
  return ret;
}

bool ObIORequest::can_callback() const
{
  return nullptr != copied_callback_ && nullptr != io_buf_;
}

void ObIORequest::free_io_buffer()
{
  if (nullptr != raw_buf_ && nullptr != tenant_io_mgr_.get_ptr()) {
    tenant_io_mgr_.get_ptr()->io_allocator_.free(raw_buf_);
    raw_buf_ = nullptr;
  }
}

void ObIORequest::cancel()
{
  int ret = OB_SUCCESS;
  if (is_inited_ && !is_finished_) {
    { // must check finished and set cancel in guard
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(guard.get_ret())) {
        LOG_WARN("fail to guard condition", K(ret));
      } else if (is_finished_){
        // do nothing
      } else {
        is_canceled_ = true;
        if (time_log_.submit_ts_ > 0 && 0 == time_log_.return_ts_) {
          channel_->cancel(*this);
        }
      }
    }
    finish(OB_CANCELED);
  }
}

void ObIORequest::finish(const ObIORetCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_LIKELY(!is_finished_)) {
      ret_code_ = ret_code;
      is_finished_ = true;
      if (OB_NOT_NULL(tenant_io_mgr_.get_ptr())) {
        if (get_group_id() > USER_RESOURCE_GROUP_END_ID) {
          tenant_io_mgr_.get_ptr()->io_backup_usage_.accumulate(*this);
        } else {
          tenant_io_mgr_.get_ptr()->io_usage_.accumulate(*this);
        }
        tenant_io_mgr_.get_ptr()->io_usage_.record_request_finish(*this);
      }
      if (OB_UNLIKELY(OB_SUCCESS != ret_code_.io_ret_)) {
        OB_IO_MANAGER.get_device_health_detector().record_failure(*this);
      }
      time_log_.end_ts_ = ObTimeUtility::fast_current_time();
      if (OB_FAIL(guard.get_ret())) {
        LOG_ERROR("lock io request condition failed", K(ret), K(*this));
      } else if (OB_FAIL(cond_.signal())) {
        LOG_ERROR("signal io request condition failed", K(ret), K(*this));
      }
    }
  }
}

void ObIORequest::inc_ref(const char *msg)
{
  int64_t old_ref_cnt = ATOMIC_FAA(&ref_cnt_, 1);
  if (nullptr != msg && OB_NOT_NULL(tenant_io_mgr_.get_ptr())) {
    int tmp_ret = OB_SUCCESS;
    ObIOTracer::TraceType trace_type = 0 == old_ref_cnt ? ObIOTracer::TraceType::IS_FIRST : ObIOTracer::TraceType::OTHER;
    if (OB_TMP_FAIL(tenant_io_mgr_.get_ptr()->trace_request_if_need(this, msg, trace_type))) {
      LOG_WARN_RET(tmp_ret, "add trace for io request failed", K(tmp_ret), KP(this), K(trace_type));
    }
  }
}

void ObIORequest::dec_ref(const char *msg)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder(tenant_io_mgr_.get_ptr());
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bug: ref_cnt < 0", K(ret), K(tmp_ref), KCSTRING(lbt()));
    abort();
  } else if (nullptr != msg && OB_NOT_NULL(tenant_holder.get_ptr())) {
    int tmp_ret = OB_SUCCESS;
    ObIOTracer::TraceType trace_type = 0 == tmp_ref ? ObIOTracer::TraceType::IS_LAST : ObIOTracer::TraceType::OTHER;
    if (OB_TMP_FAIL(tenant_holder.get_ptr()->trace_request_if_need(this, msg, trace_type))) {
      LOG_WARN("remove trace for io request failed", K(tmp_ret), KP(this), K(trace_type));
    }
  }
  if (0 == tmp_ref) {
    // destroy will be called when free
    if (OB_ISNULL(tenant_holder.get_ptr())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tenant io manager is null, memory leak", K(ret));
    } else {
      tenant_holder.get_ptr()->io_allocator_.free(this);
    }
  }
}

void ObIORequest::inc_out_ref()
{
  ATOMIC_INC(&out_ref_cnt_);
}

void ObIORequest::dec_out_ref()
{
  if (0 == ATOMIC_SAF(&out_ref_cnt_, 1)) {
    cancel();
  }
}

/******************             IOPhyQueue              **********************/

ObPhyQueue::ObPhyQueue()
  : is_inited_(false),
    stop_accept_(false),
    reservation_ts_(INT_MAX64),
    group_limitation_ts_(INT_MAX64),
    tenant_limitation_ts_(INT_MAX64),
    proportion_ts_(INT_MAX64),
    is_group_ready_(false),
    is_tenant_ready_(false),
    queue_index_(-1),
    reservation_pos_(-1),
    group_limitation_pos_(-1),
    tenant_limitation_pos_(-1),
    proportion_pos_(-1),
    req_list_()
{

}

ObPhyQueue::~ObPhyQueue()
{
  destroy();
}

int ObPhyQueue::init(const int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("phy queue init twice", K(ret), K(is_inited_));
  } else if (index < 0 || index > INT64_MAX){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of boundary", K(ret), K(index));
  } else {
    queue_index_ = index;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObPhyQueue::destroy()
{
  is_inited_ = false;
  stop_accept_ = true;
  reservation_ts_ = INT_MAX64;
  group_limitation_ts_ = INT_MAX64;
  tenant_limitation_ts_ = INT_MAX64;
  proportion_ts_ = INT_MAX64;
  is_group_ready_ = false;
  is_tenant_ready_ = false;
  reservation_pos_ = -1;
  group_limitation_pos_ = -1;
  tenant_limitation_pos_ = -1;
  proportion_pos_ = -1;
  queue_index_ = -1;
}

void ObPhyQueue::reset_time_info()
{
  reservation_ts_ = INT_MAX64;
  group_limitation_ts_ = INT_MAX64;
  tenant_limitation_ts_ = INT_MAX64;
  proportion_ts_ = INT_MAX64;
}

void ObPhyQueue::reset_queue_info()
{
  is_group_ready_ = false;
  is_tenant_ready_ = false;
  reservation_pos_ = -1;
  group_limitation_pos_ = -1;
  tenant_limitation_pos_ = -1;
  proportion_pos_ = -1;
}

/******************             IOHandle              **********************/
ObIOHandle::ObIOHandle()
  : req_(nullptr)
{
}

ObIOHandle::~ObIOHandle()
{
  reset();
}

ObIOHandle::ObIOHandle(const ObIOHandle &other)
  : req_(nullptr)
{
  *this = other;
}

ObIOHandle& ObIOHandle::operator=(const ObIOHandle &other)
{
  if (&other != this) {
  	int ret = OB_SUCCESS;
    if (OB_NOT_NULL(other.req_)) {
    	if (OB_FAIL(set_request(*other.req_))) {
    		LOG_ERROR("set io request failed", K(ret));
    	}
    }
  }
  return *this;
}

int ObIOHandle::set_request(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  reset();
  req.inc_ref("handle_inc"); // ref for handle
  req.inc_out_ref();
  req_ = &req;
  return ret;
}

bool ObIOHandle::is_empty() const
{
  return nullptr == req_;
}

bool ObIOHandle::is_valid() const
{
  return nullptr != req_;
}

int ObIOHandle::wait(const int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The IOHandle has not been inited, ", K(ret));
  } else if (timeout_ms < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(timeout_ms), K(ret));
  } else if (!req_->is_finished_) {
    ObWaitEventGuard wait_guard(req_->io_info_.flag_.get_wait_event(),
                                timeout_ms,
                                req_->io_info_.size_);
    const int64_t real_wait_timeout = min(OB_IO_MANAGER.get_io_config().data_storage_io_timeout_ms_, timeout_ms);

    if (real_wait_timeout > 0) {
      ObThreadCondGuard guard(req_->cond_);
      if (OB_FAIL(guard.get_ret())) {
        LOG_ERROR("fail to guard request condition", K(ret));
      } else {
        int64_t wait_ms = real_wait_timeout;
        int64_t begin_ms = ObTimeUtility::fast_current_time();
        while (OB_SUCC(ret) && !req_->is_finished_ && wait_ms > 0) {
          if (OB_FAIL(req_->cond_.wait(wait_ms))) {
            LOG_WARN("fail to wait request condition", K(ret), K(wait_ms), K(*req_));
          } else if (!req_->is_finished_) {
            int64_t duration_ms = ObTimeUtility::fast_current_time() - begin_ms;
            wait_ms = real_wait_timeout - duration_ms;
          }
        }
        if (OB_UNLIKELY(wait_ms <= 0)) { // rarely happen
          ret = OB_TIMEOUT;
          LOG_WARN("fail to wait request condition due to spurious wakeup", 
              K(ret), K(wait_ms), K(*req_));
        }
        if (OB_TIMEOUT == ret) {
          OB_IO_MANAGER.get_device_health_detector().record_failure(*req_);
        }
      }
    } else {
      ret = OB_TIMEOUT;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(req_->ret_code_.io_ret_)) {
      LOG_WARN("IO error, ", K(ret), K(*req_));
    }
  } else if (OB_TIMEOUT == ret) {
    LOG_WARN("IO wait timeout", K(timeout_ms), K(ret), K(*req_));
  }
  estimate();

  return ret;
}

void ObIOHandle::estimate()
{
  if (OB_NOT_NULL(req_) && req_->is_finished_ && !ATOMIC_CAS(&req_->has_estimated_, false, true)) {
    ObIOTimeLog &time_log = req_->time_log_;
    const int64_t prepare_delay = get_io_interval(time_log.enqueue_ts_, time_log.begin_ts_);
    const int64_t schedule_queue_delay = get_io_interval(time_log.dequeue_ts_, time_log.enqueue_ts_);
    const int64_t submit_delay = get_io_interval(time_log.submit_ts_, time_log.dequeue_ts_);
    const int64_t device_delay = get_io_interval(time_log.return_ts_, time_log.submit_ts_);
    const int64_t start_callback_delay = get_io_interval(time_log.callback_enqueue_ts_, time_log.return_ts_);
    const int64_t callback_queue_delay = get_io_interval(time_log.callback_dequeue_ts_, time_log.callback_enqueue_ts_);
    const int64_t callback_process_delay = get_io_interval(time_log.callback_finish_ts_, time_log.callback_dequeue_ts_);
    const int64_t finish_notify_delay = get_io_interval(time_log.end_ts_, max(time_log.callback_finish_ts_, time_log.return_ts_));
    const int64_t request_delay = get_io_interval(time_log.end_ts_, time_log.begin_ts_);
    if (req_->io_info_.flag_.is_read()) {
      EVENT_INC(ObStatEventIds::IO_READ_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_BYTES, req_->io_info_.size_);
      EVENT_ADD(ObStatEventIds::IO_READ_DELAY, request_delay);
      EVENT_ADD(ObStatEventIds::IO_READ_QUEUE_DELAY, schedule_queue_delay);
      EVENT_ADD(ObStatEventIds::IO_READ_CB_QUEUE_DELAY, callback_queue_delay);
      EVENT_ADD(ObStatEventIds::IO_READ_CB_PROCESS_DELAY, callback_process_delay);
    } else {
      EVENT_INC(ObStatEventIds::IO_WRITE_COUNT);
      EVENT_ADD(ObStatEventIds::IO_WRITE_BYTES, req_->io_info_.size_);
      EVENT_ADD(ObStatEventIds::IO_WRITE_DELAY, request_delay);
      EVENT_ADD(ObStatEventIds::IO_WRITE_QUEUE_DELAY, schedule_queue_delay);
    }
    static const int64_t LONG_IO_PRINT_TRIGGER_US = 1000L * 1000L * 3L; // 3s
    if (request_delay > LONG_IO_PRINT_TRIGGER_US) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "io request wait too long", KPC(req_),
          K(prepare_delay),
          K(schedule_queue_delay),
          K(submit_delay),
          K(device_delay),
          K(start_callback_delay),
          K(callback_queue_delay),
          K(callback_process_delay),
          K(finish_notify_delay),
          K(request_delay));
    }
  }
}

int ObIOHandle::get_fs_errno(int &io_errno) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("empty io handle", K(ret), KP(req_));
  } else {
    io_errno = req_->ret_code_.fs_errno_;
  }
  return ret;
}

const char *ObIOHandle::get_buffer()
{
  const char *buf = nullptr;
  if (OB_NOT_NULL(req_) && req_->is_finished_) {
    buf = req_->get_data();
  }
  return buf;
}

const char *ObIOHandle::get_physical_buffer()
{
  const char *buf = nullptr;
  if (OB_NOT_NULL(req_)) {
    buf = req_->io_buf_;
  }
  return buf;
}

int64_t ObIOHandle::get_data_size() const
{
  return OB_NOT_NULL(req_) ? req_->get_data_size() : 0;
}

int64_t ObIOHandle::get_rt() const
{
  return OB_NOT_NULL(req_) ? get_io_interval(req_->time_log_.end_ts_, req_->time_log_.begin_ts_) : -1;
}

void ObIOHandle::reset()
{
  if (OB_NOT_NULL(req_)) {
    req_->dec_out_ref();
    req_->dec_ref("handle_dec"); // ref for handle
    req_ = nullptr;
  }
}

void ObIOHandle::cancel()
{
  if (OB_NOT_NULL(req_)) {
    req_->cancel();
  }
}

/******************             TenantIOConfig              **********************/
ObTenantIOConfig::UnitConfig::UnitConfig()
  : min_iops_(0), max_iops_(0), weight_(0)
{

}

bool ObTenantIOConfig::UnitConfig::is_valid() const
{
  return min_iops_ > 0 && max_iops_ >= min_iops_ && weight_ >= 0;
}

ObTenantIOConfig::GroupConfig::GroupConfig()
  : deleted_(false), cleared_(false), min_percent_(0), max_percent_(0), weight_percent_(0)
{

}

ObTenantIOConfig::GroupConfig::~GroupConfig()
{

}

bool ObTenantIOConfig::GroupConfig::is_valid() const
{
  return min_percent_ >= 0 && min_percent_ <= 100 &&
         max_percent_ >= 0 && max_percent_ <= 100 &&
         weight_percent_ >= 0 && weight_percent_ <= 100 &&
         max_percent_ >= min_percent_;
}

ObTenantIOConfig::ObTenantIOConfig()
  : memory_limit_(0), callback_thread_count_(0), group_num_(0), group_ids_(), group_configs_(),
    other_group_config_(), group_config_change_(false), enable_io_tracer_(false)
{

}

ObTenantIOConfig::~ObTenantIOConfig()
{
  destroy();
}

void ObTenantIOConfig::destroy()
{
  group_ids_.destroy();
  group_configs_.destroy();
}

const ObTenantIOConfig &ObTenantIOConfig::default_instance()
{
  static ObTenantIOConfig instance;
  instance.memory_limit_ = 512L * 1024L * 1024L; // min_tenant_memory: 512M
  instance.callback_thread_count_ = 8;
  instance.group_num_ = 0;
  instance.unit_config_.min_iops_ = 10000;
  instance.unit_config_.max_iops_ = 50000;
  instance.unit_config_.weight_ = 10000;
  instance.other_group_config_.min_percent_ = 100;
  instance.other_group_config_.max_percent_ = 100;
  instance.other_group_config_.weight_percent_ = 100;
  instance.group_config_change_ = false;
  instance.enable_io_tracer_ = false;
  return instance;
}

bool ObTenantIOConfig::is_valid() const
{
  bool bret = memory_limit_ > 0 && callback_thread_count_ > 0 && unit_config_.is_valid();
  if (bret) {
    int64_t sum_min_percent = 0;
    int64_t sum_weight_percent = 0;
    for (int64_t i = 0; i < group_num_; ++i) {
      if (group_configs_.at(i).is_valid() && !group_configs_.at(i).deleted_) {
        sum_min_percent += group_configs_.at(i).min_percent_;
        sum_weight_percent += group_configs_.at(i).weight_percent_;
      }
    }
    sum_min_percent += other_group_config_.min_percent_;
    sum_weight_percent += other_group_config_.weight_percent_;
    bret = 100 >= sum_min_percent && 100 == sum_weight_percent;
  }
  return bret;
}

bool ObTenantIOConfig::operator ==(const ObTenantIOConfig &other) const
{
  //for basic config, group config is judged by flag
  bool bret = false;
  if (memory_limit_ != other.memory_limit_) {
    LOG_INFO("memory limit not equal", K(memory_limit_), K(other.memory_limit_));
  } else if (callback_thread_count_ != other.callback_thread_count_) {
    LOG_INFO("callback thread count not equal", K(callback_thread_count_), K(other.callback_thread_count_));
  } else if (unit_config_.weight_ != other.unit_config_.weight_
      || unit_config_.max_iops_ != other.unit_config_.max_iops_
      || unit_config_.min_iops_ != other.unit_config_.min_iops_) {
    LOG_INFO("unit config not equal", K(unit_config_), K(other.unit_config_));
  } else if (enable_io_tracer_ != other.enable_io_tracer_) {
    LOG_INFO("enable io tracer not equal", K(enable_io_tracer_), K(other.enable_io_tracer_));
  }
  return bret;
}

int ObTenantIOConfig::deep_copy(const ObTenantIOConfig &other_config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_configs_.assign(other_config.group_configs_))) {
    LOG_WARN("fail to assign group_configs", K(ret));
  } else if (OB_FAIL(group_ids_.assign(other_config.group_ids_))) {
    LOG_WARN("fail to assign group_ids", K(ret));
  } else {
    group_num_ = other_config.group_num_;
    other_group_config_ = other_config.other_group_config_;
  }

  if (OB_SUCC(ret)) {
    memory_limit_ = other_config.memory_limit_;
    callback_thread_count_ = other_config.callback_thread_count_;
    unit_config_ = other_config.unit_config_;
    group_config_change_ = other_config.group_config_change_;
    enable_io_tracer_ = other_config.enable_io_tracer_;
  }
  return ret;
}

//for unittest and performance test script
int ObTenantIOConfig::parse_group_config(const char *config_str)
{
  int ret = OB_SUCCESS;
  const int64_t max_config_length = 512;
  char copied_str[max_config_length] = { 0 };
  if (OB_ISNULL(config_str) || strlen(config_str) > max_config_length) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KCSTRING(config_str));
  } else if (0 > snprintf(copied_str, max_config_length, "%s", config_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy config string failed", K(ret), KCSTRING(config_str));
  } else {
    str_trim(copied_str);
    int pos = 0;
    const int64_t len = strlen(copied_str);
    for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (';' == copied_str[i]) {
        copied_str[i] = '\0';
      } else if (':' == copied_str[i]) {
        copied_str[i] = ' ';
      }
    }
    while (OB_SUCC(ret) && pos < len) {
      const char *tmp_config_str = copied_str + pos;
      int64_t group_id = 0;
      char group_idx[max_config_length] = { 0 };
      char group_namex[max_config_length] = { 0 };
      ObTenantIOConfig::GroupConfig tmp_group_config;
      int scan_count = sscanf(tmp_config_str, "%s %s %ld,%ld,%ld",
                              group_idx,
                              group_namex,
                              &tmp_group_config.min_percent_,
                              &tmp_group_config.max_percent_,
                              &tmp_group_config.weight_percent_);
      if (5 != scan_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan current group config failed", K(ret), K(scan_count), KCSTRING(tmp_config_str), K(tmp_group_config));
      } else {
        const int64_t group_id = atoi(group_idx);
        if (0 == group_id) {
          // other group
          other_group_config_ = tmp_group_config;
          pos += strlen(tmp_config_str) + 1;
        } else {
          // add single group
          if (OB_FAIL(group_configs_.push_back(tmp_group_config))) {
            LOG_WARN("push back group config failed", K(ret), K(tmp_group_config));
          } else if (OB_FAIL(group_ids_.push_back(group_id))) {
            LOG_WARN("push back group id failed", K(ret), K(group_id));
          } else {
            ATOMIC_INC(&group_num_);
            pos += strlen(tmp_config_str) + 1;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // decide the config of other group
      int64_t sum_min_percent = 0;
      int64_t sum_weight_percent = 0;
      for (int64_t i = 0; i < group_configs_.count(); ++i) {
        if (group_configs_.at(i).is_valid()) {
          sum_min_percent += group_configs_.at(i).min_percent_;
          sum_weight_percent += group_configs_.at(i).weight_percent_;
        }
      }
      if (0 == other_group_config_.min_percent_
          && 0 == other_group_config_.max_percent_
          && 0 == other_group_config_.weight_percent_
          && sum_min_percent < 100
          && sum_weight_percent < 100) {
        other_group_config_.min_percent_ = 100 - sum_min_percent;
        other_group_config_.max_percent_ = 100;
        other_group_config_.weight_percent_ = 100 - sum_weight_percent;
      }
    }
  }
  return ret;
}

int ObTenantIOConfig::add_single_group_config(const uint64_t tenant_id, const int64_t group_id, int64_t min_percent, int64_t max_percent, int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_group(group_id)) || !is_valid_tenant_id(tenant_id) ||
      min_percent < 0 || min_percent > 100 ||
      max_percent < 0 || max_percent > 100 ||
      weight_percent < 0 || weight_percent > 100 ||
      min_percent > max_percent) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(ret), K(tenant_id), K(group_id), K(min_percent), K(max_percent), K(weight_percent));
  } else {
    ObTenantIOConfig::GroupConfig tmp_group_config;
    tmp_group_config.min_percent_ = min_percent;
    tmp_group_config.max_percent_ = max_percent;
    tmp_group_config.weight_percent_ = weight_percent;
    if (OB_UNLIKELY(!tmp_group_config.is_valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid group config", K(ret), K(tmp_group_config));
    } else if (OB_FAIL(group_configs_.push_back(tmp_group_config))) {
      LOG_WARN("push back group config failed", K(ret), K(tmp_group_config));
    } else if (OB_FAIL(group_ids_.push_back(group_id))) {
      group_configs_.pop_back();
      LOG_WARN("push back group id failed", K(ret), K(tmp_group_config));
    }
  }
  return ret;
}

int ObTenantIOConfig::get_group_config(const uint64_t index, int64_t &min_iops, int64_t &max_iops, int64_t &iops_weight) const
{
  int ret = OB_SUCCESS;
  min_iops = 0;
  max_iops = 0;
  iops_weight = 0;
  if (OB_UNLIKELY(index < 0 || (index >= group_configs_.count() && index != INT64_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index));
  } else if (index == INT64_MAX) {
    //OTHER GROUPS
    const GroupConfig &group_config = other_group_config_;
    min_iops = unit_config_.min_iops_ * group_config.min_percent_ / 100;
    max_iops = unit_config_.max_iops_ * group_config.max_percent_ / 100;
    iops_weight = unit_config_.weight_ * group_config.weight_percent_ / 100;
  } else {
    const GroupConfig &group_config = group_configs_.at(index);
    min_iops = unit_config_.min_iops_ * group_config.min_percent_ / 100;
    max_iops = unit_config_.max_iops_ * group_config.max_percent_ / 100;
    iops_weight = unit_config_.weight_ * group_config.weight_percent_ / 100;
  }
  return ret;
}

int64_t ObTenantIOConfig::get_all_group_num() const
{
  int64_t all_group_num = ATOMIC_LOAD(&group_num_) +1;
  //include other_groups
  return all_group_num;
}

int64_t ObTenantIOConfig::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(group_num_), K(memory_limit_), K(callback_thread_count_), K(unit_config_), K_(enable_io_tracer));
  // if self invalid, print all group configs, otherwise, only print valid group configs
  const bool self_valid = is_valid();
  BUF_PRINTF(", group_configs:[");
  bool need_comma = false;
  for (int64_t i = 0; i < group_configs_.count(); ++i) {
    if (group_configs_.at(i).deleted_ || group_configs_.at(i).cleared_) {
      continue;
    } else if (!self_valid || group_configs_.at(i).is_valid()) {
      if (need_comma) {
        J_COMMA();
      }
      BUF_PRINTF("group_id = ");
      char group_id[8];
      snprintf(group_id, sizeof(group_id), "%ld", group_ids_.at(i));
      J_KV(group_id, group_configs_.at(i));
      need_comma = true;
    }
  }
  if (!self_valid || other_group_config_.is_valid()) {
    if (need_comma) {
      J_COMMA();
    }
    J_KV("other_groups", other_group_config_);
  }
  BUF_PRINTF("]");
  J_OBJ_END();
  return pos;
}

/******************             IOClock              **********************/
void ObAtomIOClock::atom_update(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts)
{
  int64_t tmp_ts = 0;
  if (0 == iops_) {
    deadline_ts = INT64_MAX;
  } else {
    const int64_t delta_ns = 1000L * 1000L * 1000L / (iops_scale * iops_);
    do {
      tmp_ts = last_ns_;
      deadline_ts = max(current_ts * 1000L, tmp_ts + delta_ns);
    } while (!ATOMIC_BCAS(&last_ns_, tmp_ts, deadline_ts));
    deadline_ts /= 1000;
  }
}

void ObAtomIOClock::reset()
{
  iops_ = 0;
  last_ns_ = 0;
}

/******************             MClockQueue              **********************/
ObMClockQueue::ObMClockQueue()
  : is_inited_(false),
    r_heap_(r_cmp_),
    gl_heap_(gl_cmp_),
    tl_heap_(tl_cmp_),
    ready_heap_(p_cmp_)
{

}

ObMClockQueue::~ObMClockQueue()
{
  destroy();
}

int ObMClockQueue::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObMClockQueue::destroy()
{
  is_inited_ = false;
}

int ObMClockQueue::get_time_info(int64_t &reservation_ts,
                                  int64_t &group_limitation_ts,
                                  int64_t &tenant_limitation_ts,
                                  int64_t &proportion_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || r_heap_.empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret), K(is_inited_));
  } else {
    ObPhyQueue *r_phy_queue = r_heap_.top();
    if (OB_ISNULL(r_phy_queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_queue is null", K(ret));
    } else {
      reservation_ts = r_phy_queue->reservation_ts_;
      group_limitation_ts = gl_heap_.empty() ? INT64_MAX : gl_heap_.top()->group_limitation_ts_;
      tenant_limitation_ts = tl_heap_.empty() ? INT64_MAX : tl_heap_.top()->tenant_limitation_ts_;
      proportion_ts = ready_heap_.empty() ? INT64_MAX : ready_heap_.top()->proportion_ts_;
    }
  }
  return ret;
}

int ObMClockQueue::push_phyqueue(ObPhyQueue *phy_queue)
{
  int ret = OB_SUCCESS;
  phy_queue->reset_queue_info();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(phy_queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_queue is null", K(ret), KP(phy_queue));
  } else if (OB_FAIL(r_heap_.push(phy_queue))) {
    LOG_WARN("push r heap failed", K(ret));
  } else if (OB_FAIL(gl_heap_.push(phy_queue))) {
    LOG_WARN("push cl heap failed", K(ret));
    int tmp_ret = r_heap_.remove(phy_queue);
    if (OB_SUCCESS != tmp_ret) {
      LOG_ERROR("remove queue from reservation queue failed", K(ret));
    }
  } 
  return ret;
}

int ObMClockQueue::pop_phyqueue(ObIORequest *&req, int64_t &deadline_ts)
{
  int ret = OB_SUCCESS;
  req = nullptr;
  deadline_ts = 0;
  const int64_t current_ts = ObTimeUtility::fast_current_time();
  
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (r_heap_.empty()){
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    ObTimeGuard time_guard("pop_phyqueue", 100000); //100ms
    ObPhyQueue *tmp_phy_queue = r_heap_.top();
    if (OB_ISNULL(tmp_phy_queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_queue is null", K(ret), KP(tmp_phy_queue));
    } else if (tmp_phy_queue->req_list_.is_empty()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (tmp_phy_queue->reservation_ts_ <= current_ts) {
      //R schedule
      if(OB_FAIL(remove_from_heap(tmp_phy_queue))) {
        LOG_WARN("remove phy queue from heap failed(R schedule)", K(ret));
      } else if (FALSE_IT(time_guard.click("R_leave_heap"))) {
      } else {
        req = tmp_phy_queue->req_list_.remove_first();
        if (OB_ISNULL(req)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("request is null", K(ret), KP(req));
        } else {
          req->time_log_.dequeue_ts_ = ObTimeUtility::fast_current_time();
          if (tmp_phy_queue->req_list_.is_empty()) {
            tmp_phy_queue->reset_time_info();
          } else if (OB_NOT_NULL(req->tenant_io_mgr_.get_ptr())) {
            ObTenantIOClock *io_clock = static_cast<ObTenantIOClock *>(req->tenant_io_mgr_.get_ptr()->get_io_clock());       
            ObIORequest *next_req = tmp_phy_queue->req_list_.get_first();
            if (OB_ISNULL(next_req)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null next_req", KP(next_req));
            } else if (OB_FAIL(io_clock->calc_phyqueue_clock(tmp_phy_queue, *next_req))) {
              LOG_WARN("calc phyqueue clock failed", K(ret), KPC(next_req));
            } else if (FALSE_IT(time_guard.click("R_calc_clock"))) {
            }
          }
        }
        int tmp_ret = push_phyqueue(tmp_phy_queue);
        time_guard.click("R_into_heap");
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          LOG_WARN("re_into heap failed(R schedule)", K(tmp_ret));
          abort();
        }
      }
    } else {
      // P schedule, find candidate requests with limitation_ts < current_ts and pop the request with min proportion_ts
      if (OB_FAIL(pop_with_ready_queue(current_ts, req, deadline_ts))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("pop with ready queue failed", K(ret));
        }
      } else {
        time_guard.click("P_schedule");
      }
    }
  }
  return ret;  
}

int ObMClockQueue::remove_from_heap(ObPhyQueue *phy_queue)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_queue)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_queue is null", K(ret), KP(phy_queue));
  } else if (OB_FAIL(r_heap_.remove(phy_queue))) {
    LOG_WARN("remove phy queue from r heap failed", K(ret));
  } else if (!phy_queue->is_group_ready_ && !phy_queue->is_tenant_ready_) {
    if (OB_FAIL(gl_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from cl heap failed", K(ret));
    }
  } else if (phy_queue->is_group_ready_ && !phy_queue->is_tenant_ready_) {
    if (OB_FAIL(tl_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from tl heap failed", K(ret));
    }
  } else if (phy_queue->is_group_ready_ && phy_queue->is_tenant_ready_) {
    if (OB_FAIL(ready_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from ready heap failed", K(ret));
    }
  }
  // ignore ret
  return ret;
}

int ObMClockQueue::pop_with_ready_queue(const int64_t current_ts, ObIORequest *&req, int64_t &deadline_ts)
{
  int ret = OB_SUCCESS;
  deadline_ts = 0;
  int64_t iter_count = 0;
  ObPhyQueue *tmp_phy_queue = nullptr;
  req = nullptr;
  while (OB_SUCC(ret) && !gl_heap_.empty() && !gl_heap_.top()->req_list_.is_empty()) {
    tmp_phy_queue = gl_heap_.top();
    deadline_ts = 0 == iter_count ? tmp_phy_queue->tenant_limitation_ts_ : deadline_ts;
    ++iter_count;
    if (tmp_phy_queue->group_limitation_ts_ > current_ts) {
      break;
    } else if (OB_FAIL(gl_heap_.pop())) {
      LOG_WARN("remove PhyQueue from c_limitation queue failed", K(ret));
    } else {
      tmp_phy_queue->is_group_ready_ = true;
      if (tmp_phy_queue->tenant_limitation_ts_ <= current_ts) {
        tmp_phy_queue->is_tenant_ready_ = true;
        if (OB_FAIL(ready_heap_.push(tmp_phy_queue))) {
          LOG_WARN("push phy_queue from gl_heap to ready_heap failed", K(ret));
        }
      } else {
        if (OB_FAIL(tl_heap_.push(tmp_phy_queue))) {
          LOG_WARN("push phy_queue from gl_heap to tl_heap failed", K(ret));
        }
      }
    }
  }
  iter_count = 0;
  while (OB_SUCC(ret) && !tl_heap_.empty() && !tl_heap_.top()->req_list_.is_empty()) {
    tmp_phy_queue = tl_heap_.top();
    if (0 == iter_count) {
      if (0 == deadline_ts) {
        deadline_ts = tmp_phy_queue->tenant_limitation_ts_;
      } else {
        deadline_ts = std::min(tmp_phy_queue->tenant_limitation_ts_, deadline_ts);
      }
    }
    ++iter_count;
    if (tmp_phy_queue->tenant_limitation_ts_ > current_ts) {
      break;
    } else if (OB_FAIL(tl_heap_.pop())) {
      LOG_WARN("remove PhyQueue from t_limitation queue failed", K(ret));
    } else {
      tmp_phy_queue->is_tenant_ready_ = true;
      if (OB_FAIL(ready_heap_.push(tmp_phy_queue))) {
        LOG_WARN("push phy_queue from tl_heap to ready_heap failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ready_heap_.empty()) {
    tmp_phy_queue = ready_heap_.top();
    if (OB_ISNULL(tmp_phy_queue)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_queue is null", K(ret));
    } else if (!tmp_phy_queue->req_list_.is_empty()) {
      if (OB_FAIL(remove_from_heap(tmp_phy_queue))) {
        LOG_WARN("remove phy queue from heap failed(P schedule)", K(ret));
      } else {
        req = tmp_phy_queue->req_list_.remove_first();
        if (OB_ISNULL(req)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("req is null", K(ret), KP(req));
        } else {
          req->time_log_.dequeue_ts_ = ObTimeUtility::fast_current_time();
          LOG_DEBUG("req pop from phy queue succcess(P schedule)", KP(req), K(iter_count), "time_cost", ObTimeUtility::fast_current_time() - current_ts, K(ready_heap_.count()), K(current_ts));
          if (tmp_phy_queue->req_list_.is_empty()) {
            tmp_phy_queue->reset_time_info();
          } else if (OB_NOT_NULL(req->tenant_io_mgr_.get_ptr())) {
            ObTenantIOClock *io_clock = static_cast<ObTenantIOClock *>(req->tenant_io_mgr_.get_ptr()->get_io_clock());
            ObIORequest *next_req = tmp_phy_queue->req_list_.get_first();
            if (OB_ISNULL(next_req)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null next_req", KP(next_req));
            } else {
              int tmp_ret = io_clock->adjust_reservation_clock(tmp_phy_queue, *next_req);
              if (OB_FAIL(io_clock->calc_phyqueue_clock(tmp_phy_queue, *next_req))) {
                LOG_WARN("calc phyqueue clock failed", K(ret));
              } else if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
                LOG_WARN("adjust reservation clock failed", K(tmp_ret), KPC(next_req));
              }
            }
          }
        }
        int tmp_ret = push_phyqueue(tmp_phy_queue);
        if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
          LOG_WARN("re_into heap failed(P schedule)", K(tmp_ret));
          abort();
        }
      }
    }
  } else {
    ret = OB_EAGAIN;
    if (!r_heap_.empty() && !r_heap_.top()->req_list_.is_empty()) {
      ObPhyQueue *next_tmp_phy_queue = r_heap_.top();
      if (0 == deadline_ts) {
        deadline_ts = next_tmp_phy_queue->reservation_ts_;
      } else {
        deadline_ts = std::min(next_tmp_phy_queue->reservation_ts_, deadline_ts);
      }
    }
  }
  return ret;
}

