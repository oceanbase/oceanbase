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

/******************             IOCategory              **********************/
static const char *log_category_name = "LOG";
static const char *user_category_name = "USER";
static const char *sys_category_name = "SYS";
static const char *prewarm_category_name = "PREWARM";
static const char *large_query_category_name = "LARGE";
const char *oceanbase::common::get_io_category_name(ObIOCategory category)
{
  const char *ret_name = "UNKNOWN";
  switch (category) {
    case ObIOCategory::LOG_IO:
      ret_name = log_category_name;
      break;
    case ObIOCategory::USER_IO:
      ret_name = user_category_name;
      break;
    case ObIOCategory::SYS_IO:
      ret_name = sys_category_name;
      break;
    case ObIOCategory::PREWARM_IO:
      ret_name = prewarm_category_name;
      break;
    case ObIOCategory::LARGE_QUERY_IO:
      ret_name = large_query_category_name;
      break;
    default:
      break;
  }
  return ret_name;
}

ObIOCategory oceanbase::common::get_io_category_enum(const char *category_name)
{
  ObIOCategory io_category = ObIOCategory::MAX_CATEGORY;
  if (0 == strncasecmp(category_name, log_category_name, strlen(log_category_name))) {
    io_category = ObIOCategory::LOG_IO;
  } else if (0 == strncasecmp(category_name, user_category_name, strlen(user_category_name))) {
    io_category = ObIOCategory::USER_IO;
  } else if (0 == strncasecmp(category_name, sys_category_name, strlen(sys_category_name))) {
    io_category = ObIOCategory::SYS_IO;
  } else if (0 == strncasecmp(category_name, prewarm_category_name, strlen(prewarm_category_name))) {
    io_category = ObIOCategory::PREWARM_IO;
  } else if (0 == strncasecmp(category_name, large_query_category_name, strlen(large_query_category_name))) {
    io_category = ObIOCategory::LARGE_QUERY_IO;
  }
  return io_category;
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
    && category_ >= 0 && category_ < static_cast<int>(ObIOCategory::MAX_CATEGORY)
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

void ObIOFlag::set_category(ObIOCategory category)
{
  category_ = static_cast<int>(category);
}

ObIOCategory ObIOFlag::get_category() const
{
  return static_cast<ObIOCategory>(category_);
}

void ObIOFlag::set_wait_event(int64_t wait_event_id)
{
  wait_event_id_ = wait_event_id;
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

bool ObIOFlag::is_unlimited() const
{
  return is_unlimited_;
}

/******************             IOCallback              **********************/
ObIOCallback::ObIOCallback()
  : compat_mode_(static_cast<lib::Worker::CompatMode>(lib::get_compat_mode()))
{

}

ObIOCallback::~ObIOCallback()
{

}

int ObIOCallback::process(const bool is_success)
{
  lib::set_compat_mode(compat_mode_);
  return inner_process(is_success);
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
  if (nullptr != control_block_ && nullptr != io_info_.fd_.device_handle_) {
    io_info_.fd_.device_handle_->free_iocb(control_block_);
    control_block_ = nullptr;
  }
  io_info_.reset();
  if (nullptr != raw_buf_ && nullptr != tenant_io_mgr_.get_ptr()) {
    tenant_io_mgr_.get_ptr()->io_allocator_.free(raw_buf_);
    raw_buf_ = nullptr;
  }
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

const char *ObIORequest::get_data()
{
  const char *buf = nullptr;
  if (nullptr != copied_callback_) {
    buf = copied_callback_->get_data();
  } else if (io_info_.flag_.is_sync()) {
    buf = io_buf_;
  } else {
    // re-calculate with const parameters, in case of partial return change aligned_buf and so on.
    const int64_t aligned_offset = lower_align(io_info_.offset_, DIO_READ_ALIGN_SIZE);
    const char *aligned_buf = reinterpret_cast<const char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), DIO_READ_ALIGN_SIZE));
    buf = aligned_buf + io_info_.offset_ - aligned_offset;
  }
  return buf;
}

const ObIOFlag &ObIORequest::get_flag() const
{
  return io_info_.flag_;
}

ObIOCategory ObIORequest::get_category() const
{
  return io_info_.flag_.get_category();
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
  } else if (nullptr == copied_callback_) {
    align_offset_size(io_info_.offset_, io_info_.size_, io_offset_, io_size_);
    if (OB_ISNULL(tenant_io_mgr_.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant io manager is null", K(ret));
    } else if (OB_ISNULL(raw_buf_ = tenant_io_mgr_.get_ptr()->io_allocator_.alloc(io_size_ + DIO_READ_ALIGN_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(io_size_));
    } else {
      io_buf_ = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), DIO_READ_ALIGN_SIZE));
    }
  } else {
    if (OB_FAIL(copied_callback_->alloc_io_buf(io_buf_, io_size_, io_offset_))) {
      LOG_WARN("callback allocate memory failed", K(ret), K(io_info_));
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
  ObTimeGuard tg("prepare");
  if (OB_ISNULL(control_block_)
      && (OB_ISNULL(io_info_.fd_.device_handle_) || OB_ISNULL(control_block_ = io_info_.fd_.device_handle_->alloc_iocb()))) {
    ret = OB_ERR_UNEXPECTED;
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
        tenant_io_mgr_.get_ptr()->io_usage_.accumulate(*this);
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
      LOG_WARN("add trace for io request failed", K(tmp_ret), KP(this), K(trace_type));
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
    reservation_ts_(INT_MAX64),
    category_limitation_ts_(INT_MAX64),
    tenant_limitation_ts_(INT_MAX64),
    proportion_ts_(INT_MAX64),
    is_category_ready_(false),
    is_tenant_ready_(false),
    category_index_(-1),
    reservation_pos_(-1),
    category_limitation_pos_(-1),
    tenant_limitation_pos_(-1),
    proportion_pos_(-1),
    req_list_()
{

}

ObPhyQueue::~ObPhyQueue()
{
  destroy();
}

int ObPhyQueue::init(const int index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("phy queue init twice", K(ret), K(is_inited_));
  } else if (index < 0 || index > static_cast<int>(ObIOCategory::MAX_CATEGORY)){
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of boundary", K(ret), K(index));
  } else {
    category_index_=index;
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
  reservation_ts_ = INT_MAX64;
  category_limitation_ts_ = INT_MAX64;
  tenant_limitation_ts_ = INT_MAX64;
  proportion_ts_ = INT_MAX64;
  is_category_ready_ = false;
  is_tenant_ready_ = false;
  reservation_pos_ = -1;
  category_limitation_pos_ = -1;
  tenant_limitation_pos_ = -1;
  proportion_pos_ = -1;
  category_index_ = -1;
}

void ObPhyQueue::reset_time_info()
{
  reservation_ts_ = INT_MAX64;
  category_limitation_ts_ = INT_MAX64;
  tenant_limitation_ts_ = INT_MAX64;
  proportion_ts_ = INT_MAX64;
}

void ObPhyQueue::reset_queue_info()
{
  is_category_ready_ = false;
  is_tenant_ready_ = false;
  reservation_pos_ = -1;
  category_limitation_pos_ = -1;
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
    int real_wait_timeout = min(OB_IO_MANAGER.get_io_config().data_storage_io_timeout_ms_, timeout_ms);

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
    LOG_WARN("IO wait timeout, ", K(timeout_ms), K(ret), K(*req_));
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
      LOG_WARN("io request wait too long", KPC(req_),
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

ObTenantIOConfig::CategoryConfig::CategoryConfig()
  : min_percent_(0), max_percent_(0), weight_percent_(0)
{

}

bool ObTenantIOConfig::CategoryConfig::is_valid() const
{
  return min_percent_ > 0 && max_percent_ >= min_percent_ && max_percent_ <= 100 && weight_percent_ >= 0 && weight_percent_ <= 100;
}

ObTenantIOConfig::ObTenantIOConfig()
  : memory_limit_(0), callback_thread_count_(0), enable_io_tracer_(false)
{

}

const ObTenantIOConfig &ObTenantIOConfig::default_instance()
{
  static ObTenantIOConfig instance;
  instance.memory_limit_ = 512L * 1024L * 1024L; // min_tenant_memory: 512M
  instance.callback_thread_count_ = 8;
  instance.other_config_.min_percent_ = 100;
  instance.other_config_.max_percent_ = 100;
  instance.other_config_.weight_percent_ = 100;
  instance.unit_config_.min_iops_ = 10000;
  instance.unit_config_.max_iops_ = 50000;
  instance.unit_config_.weight_ = 10000;
  instance.enable_io_tracer_ = false;
  return instance;
}

bool ObTenantIOConfig::is_valid() const
{
  bool bret = memory_limit_ > 0 && callback_thread_count_ > 0 && unit_config_.is_valid();
  if (bret) {
    int64_t sum_min_percent = 0;
    int64_t sum_weight_percent = 0;
    for (int64_t i = 0; i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
      if (category_configs_[i].is_valid()) {
        sum_min_percent += category_configs_[i].min_percent_;
        sum_weight_percent += category_configs_[i].weight_percent_;
      }
    }
    sum_min_percent += other_config_.min_percent_;
    sum_weight_percent += other_config_.weight_percent_;
    bret = 100 == sum_min_percent && 100 == sum_weight_percent;
  }
  return bret;
}

bool ObTenantIOConfig::operator ==(const ObTenantIOConfig &other) const
{
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
  } else {
    bret = true;
    const int64_t max_category_count = static_cast<int64_t>(ObIOCategory::MAX_CATEGORY);
    for (int64_t i = 0; bret && i < max_category_count + 1; ++i) {
      const CategoryConfig &category_config = max_category_count == i ? other_config_ : category_configs_[i];
      const CategoryConfig &other_category_config = max_category_count == i ? other.other_config_ : other.category_configs_[i];
      if (category_config.weight_percent_ != other_category_config.weight_percent_
          || category_config.max_percent_ != other_category_config.max_percent_
          || category_config.min_percent_ != other_category_config.min_percent_) {
        bret = false;
        LOG_INFO("category config not equal", K(category_config), K(other_category_config), K(i), K(max_category_count));
      }
    }
  }
  return bret;
}

int ObTenantIOConfig::parse_category_config(const char *config_str)
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
    int len = strlen(copied_str);
    for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (';' == copied_str[i]) {
        copied_str[i] = '\0';
      } else if (':' == copied_str[i]) {
        copied_str[i] = ' ';
      }
    }
    while (OB_SUCC(ret) && pos < len) {
      const char *tmp_config_str = copied_str + pos;
      char category_name[max_config_length] = { 0 };
      ObTenantIOConfig::CategoryConfig tmp_category_config;
      int scan_count = sscanf(tmp_config_str, "%s %ld,%ld,%ld",
                              category_name,
                              &tmp_category_config.min_percent_,
                              &tmp_category_config.max_percent_,
                              &tmp_category_config.weight_percent_);
      if (4 != scan_count) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan current category config failed", K(ret), K(scan_count), KCSTRING(tmp_config_str), K(tmp_category_config));
      } else {
        ObIOCategory io_category = get_io_category_enum(category_name);
        ObTenantIOConfig::CategoryConfig &target_category_config = ObIOCategory::MAX_CATEGORY == io_category ? other_config_ : category_configs_[static_cast<int>(io_category)];
        target_category_config = tmp_category_config;
        pos += strlen(tmp_config_str) + 1;
      }
    }
    if (OB_SUCC(ret)) {
      // decide the config of other category
      int64_t sum_min_percent = 0;
      int64_t sum_weight_percent = 0;
      for (int64_t i = 0; i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
        if (category_configs_[i].is_valid()) {
          sum_min_percent += category_configs_[i].min_percent_;
          sum_weight_percent += category_configs_[i].weight_percent_;
        }
      }
      if (0 == other_config_.min_percent_
          && 0 == other_config_.max_percent_
          && 0 == other_config_.weight_percent_
          && sum_min_percent < 100
          && sum_weight_percent < 100) {
        other_config_.min_percent_ = 100 - sum_min_percent;
        other_config_.max_percent_ = 100;
        other_config_.weight_percent_ = 100 - sum_weight_percent;
      }
    }
  }
  return ret;
}

int ObTenantIOConfig::get_category_config(const ObIOCategory category, int64_t &min_iops, int64_t &max_iops, int64_t &iops_weight) const
{
  int ret = OB_SUCCESS;
  min_iops = 0;
  max_iops = 0;
  iops_weight = 0;
  if (OB_UNLIKELY(category >= ObIOCategory::MAX_CATEGORY)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const CategoryConfig &category_config = category_configs_[static_cast<int64_t>(category)].is_valid()
      ? category_configs_[static_cast<int64_t>(category)] : other_config_;
    min_iops = unit_config_.min_iops_ * category_config.min_percent_ / 100;
    max_iops = unit_config_.max_iops_ * category_config.max_percent_ / 100;
    iops_weight = unit_config_.weight_ * category_config.weight_percent_ / 100;
  }
  return ret;
}

int64_t ObTenantIOConfig::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K(memory_limit_), K(callback_thread_count_), K(unit_config_), K_(enable_io_tracer));
  // if self invalid, print all category configs, otherwise, only print valid category configs
  const bool self_valid = is_valid();
  BUF_PRINTF(", category_configs:[");
  bool need_comma = false;
  for (int64_t i = 0; i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
    if (!self_valid || category_configs_[i].is_valid()) {
      if (need_comma) {
        J_COMMA();
      }
      J_KV(get_io_category_name(static_cast<ObIOCategory>(i)), category_configs_[i]);
      need_comma = true;
    }
  }
  if (!self_valid || other_config_.is_valid()) {
    if (need_comma) {
      J_COMMA();
    }
    J_KV("other", other_config_);
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
    cl_heap_(cl_cmp_),
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
  } else if (OB_FAIL(cl_heap_.push(phy_queue))) {
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
    LOG_WARN("r heap is empty, phy queue not init", K(ret));
  } else {
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
            }
          }
        }
        int tmp_ret = push_phyqueue(tmp_phy_queue);
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
  } else if (!phy_queue->is_category_ready_ && !phy_queue->is_tenant_ready_) {
    if (OB_FAIL(cl_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from cl heap failed", K(ret));
    }
  } else if (phy_queue->is_category_ready_ && !phy_queue->is_tenant_ready_) {
    if (OB_FAIL(tl_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from tl heap failed", K(ret));
    }
  } else if (phy_queue->is_category_ready_ && phy_queue->is_tenant_ready_) {
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
  while (OB_SUCC(ret) && !cl_heap_.empty() && !cl_heap_.top()->req_list_.is_empty()) {
    tmp_phy_queue = cl_heap_.top();
    deadline_ts = 0 == iter_count ? tmp_phy_queue->tenant_limitation_ts_ : deadline_ts;
    ++iter_count;
    if (tmp_phy_queue->category_limitation_ts_ > current_ts) {
      break;
    } else if (OB_FAIL(cl_heap_.pop())) {
      LOG_WARN("remove PhyQueue from c_limitation queue failed", K(ret));
    } else {
      tmp_phy_queue->is_category_ready_ = true;
      if (tmp_phy_queue->tenant_limitation_ts_ <= current_ts) {
        tmp_phy_queue->is_tenant_ready_ = true;
        if (OB_FAIL(ready_heap_.push(tmp_phy_queue))) {
          LOG_WARN("push phy_queue from cl_heap to ready_heap failed", K(ret));
        }
      } else {
        if (OB_FAIL(tl_heap_.push(tmp_phy_queue))) {
          LOG_WARN("push phy_queue from cl_heap to tl_heap failed", K(ret));
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

