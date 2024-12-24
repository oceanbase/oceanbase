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
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/restore/ob_object_device.h"
#include "storage/backup/ob_backup_factory.h"
#include "deps/oblib/src/lib/stat/ob_session_stat.h"

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

const char *oceanbase::common::get_io_mode_string(const ObIOGroupMode group_mode)
{
  const char *str = "INVALID";
  switch (group_mode) {
    case ObIOGroupMode::LOCALREAD:   str = "LOCAL READ";   break;
    case ObIOGroupMode::LOCALWRITE:  str = "LOCAL WRITE";  break;
    case ObIOGroupMode::REMOTEREAD:  str = "REMOTE READ";  break;
    case ObIOGroupMode::REMOTEWRITE: str = "REMOTE WRITE"; break;
    default: str = "EXCEPTION"; break;
  }
  return str;
}

int oceanbase::common::transform_usage_index_to_group_config_index(const uint64_t &usage_index, uint64_t &group_config_index)
{
  int ret = OB_SUCCESS;
  uint64_t MODE_CNT = static_cast<uint64_t>(ObIOMode::MAX_MODE) + 1;
  uint64_t GROUP_MODE_CNT = static_cast<uint64_t>(ObIOGroupMode::MODECNT);
  uint64_t quot = usage_index / GROUP_MODE_CNT;
  uint64_t mod = usage_index % GROUP_MODE_CNT;
  if (mod < 0) {
  } else if (mod <= static_cast<uint64_t>(ObIOGroupMode::LOCALWRITE)) {
    group_config_index = quot * MODE_CNT + static_cast<uint64>(ObIOMode::MAX_MODE);
  } else if (mod < static_cast<uint64_t>(ObIOGroupMode::MODECNT)) {
    group_config_index = quot * MODE_CNT + (mod - static_cast<uint64_t>(ObIOGroupMode::REMOTEREAD));
  }
  return ret;
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
    case ObIOModule::SSTABLE_MACRO_BLOCK_WRITE_IO:
      ret_name = "SSTABLE_MACRO_BLOCK_WRITE_IO";
      break;
    case ObIOModule::CLOG_WRITE_IO:
      ret_name = "CLOG_WRITE_IO";
      break;
    case ObIOModule::CLOG_READ_IO:
      ret_name = "CLOG_READ_IO";
      break;
    default:
      break;
  }
  return ret_name;
}

/******************             IOFlag              **********************/
ObIOFlag::ObIOFlag()
    : mode_(0),
      func_type_(oceanbase::share::ObFunctionType::DEFAULT_FUNCTION),
      wait_event_id_(0),
      is_sync_(false),
      is_unlimited_(false),
      is_detect_(false),
      is_write_through_(false),
      is_sealed_(true),
      need_close_dev_and_fd_(false),
      reserved_(0),
      group_id_(USER_RESOURCE_OTHER_GROUP_ID),
      sys_module_id_(OB_INVALID_ID)
{
}

ObIOFlag::~ObIOFlag()
{

}

void ObIOFlag::reset()
{
  mode_ = 0;
  group_id_ = USER_RESOURCE_OTHER_GROUP_ID;
  sys_module_id_ = OB_INVALID_ID;
  wait_event_id_ = 0;
  is_sync_ = false;
  is_unlimited_ = false;
  is_detect_ = false;
  is_write_through_ = false;
  is_sealed_ = true;
  need_close_dev_and_fd_ = false;
  reserved_ = 0;
  group_id_ = USER_RESOURCE_OTHER_GROUP_ID;
  sys_module_id_ = OB_INVALID_ID;
}

bool ObIOFlag::is_valid() const
{
  return mode_ >= 0 && mode_ < static_cast<int>(ObIOMode::MAX_MODE)
    && is_valid_group(group_id_)
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

void ObIOFlag::set_resource_group_id(const uint64_t group_id)
{
  if (!is_valid_resource_group(group_id)) {
    group_id_ = USER_RESOURCE_OTHER_GROUP_ID;
  } else {
    group_id_ = group_id;
  }
}

void ObIOFlag::set_func_type(const uint8_t func_type)
{
  func_type_ = func_type;
}

uint8_t ObIOFlag::get_func_type() const
{
  return func_type_;
}

void ObIOFlag::set_wait_event(int64_t wait_event_id)
{
  wait_event_id_ = wait_event_id;
}

uint64_t ObIOFlag::get_resource_group_id() const
{
  return group_id_;
}

uint64_t ObIOFlag::get_sys_module_id() const
{
  return sys_module_id_;
}

void ObIOFlag::set_sys_module_id(const uint64_t sys_module_id)
{
  sys_module_id_ = sys_module_id;
}

bool ObIOFlag::is_sys_module() const
{
  return USER_RESOURCE_OTHER_GROUP_ID == group_id_
          && sys_module_id_ >= SYS_MODULE_START_ID
          && sys_module_id_ < SYS_MODULE_END_ID;
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

bool ObIOFlag::is_time_detect() const
{
  return is_time_detect_;
}

bool ObIOFlag::is_unlimited() const
{
  return is_unlimited_;
}

bool ObIOFlag::is_detect() const
{
  return is_detect_;
}

void ObIOFlag::set_write_through(const bool is_write_through)
{
  is_write_through_ = is_write_through;
}

bool ObIOFlag::is_write_through() const
{
  return is_write_through_;
}

void ObIOFlag::set_unsealed()
{
  is_sealed_ = false;
}

void ObIOFlag::set_sealed()
{
  is_sealed_ = true;
}

bool ObIOFlag::is_sealed() const
{
  return is_sealed_;
}

void ObIOFlag::set_need_close_dev_and_fd()
{
  need_close_dev_and_fd_ = true;
}

void ObIOFlag::set_no_need_close_dev_and_fd()
{
  need_close_dev_and_fd_ = false;
}

bool ObIOFlag::is_need_close_dev_and_fd() const
{
  return need_close_dev_and_fd_;
}


bool oceanbase::common::is_atomic_write_callback(const ObIOCallbackType type)
{
  return (ObIOCallbackType::ATOMIC_WRITE_CALLBACK == type);
}

/******************             IOCallback              **********************/
ObIOCallback::ObIOCallback(const ObIOCallbackType type)
  : type_(type), compat_mode_(static_cast<lib::Worker::CompatMode>(lib::get_compat_mode()))
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

int ObIOCallback::alloc_and_copy_data(const char *io_data_buffer, const int64_t data_size, common::ObIAllocator *allocator, char *&data_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator) || nullptr == io_data_buffer) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid data, the allocator is nullptr", K(ret), K(io_data_buffer), K(allocator));
  } else if (OB_UNLIKELY(data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data buffer size", K(ret), K(data_size));
  } else if (OB_UNLIKELY(NULL == (data_buffer = (char*) (allocator->alloc(data_size))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to allocate memory", K(ret), K(data_size));
  } else {
    MEMCPY(data_buffer, io_data_buffer, data_size);
  }
  return ret;
}

/******************             SNIOInfo              **********************/
ObSNIOInfo::ObSNIOInfo()
  : tenant_id_(OB_INVALID_TENANT_ID),
    fd_(),
    offset_(0),
    size_(0),
    timeout_us_(DEFAULT_IO_WAIT_TIME_US),
    flag_(),
    callback_(nullptr),
    buf_(nullptr),
    user_data_buf_(nullptr),
    part_id_(-1)
{
}

ObSNIOInfo::ObSNIOInfo(const ObSNIOInfo &other)
{
  *this = other;
}


ObSNIOInfo::~ObSNIOInfo()
{

}

void ObSNIOInfo::reset()
{
  tenant_id_ = 0;
  fd_.reset();
  offset_ = 0;
  size_ = 0;
  timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  flag_.reset();
  callback_ = nullptr;
  buf_ = nullptr;
  user_data_buf_ = nullptr;
  part_id_ = -1;
}

bool ObSNIOInfo::is_valid() const
{
  return tenant_id_ > 0
    && fd_.is_valid()
    && offset_ >= 0
    // in order to address concurrent write issues, archive checkpoint module would write
    // multiple non-content objects (it stores content in object name) whose size = 0
    && (flag_.is_sync() ? size_ >= 0 : size_ > 0)
    && timeout_us_ >= 0 //todo qilu: reopen after column_store steady
    && flag_.is_valid()
    && (flag_.is_read() || nullptr != buf_);
}

ObSNIOInfo &ObSNIOInfo::operator=(const ObSNIOInfo &other)
{
  if (&other != this) {
    reset();
    tenant_id_ = other.tenant_id_;
    fd_ = other.fd_;
    offset_ = other.offset_;
    size_ = other.size_;
    timeout_us_ = other.timeout_us_;
    flag_ = other.flag_;
    callback_ = other.callback_;
    buf_ = other.buf_;
    user_data_buf_ = other.user_data_buf_;
    part_id_ = other.part_id_;
  }
  return (*this);
}


/******************             S2IOInfo              **********************/
#ifdef OB_BUILD_SHARED_STORAGE
ObSSIOInfo::ObSSIOInfo() : ObSNIOInfo(), phy_block_handle_(), fd_cache_handle_(), tmp_file_valid_length_(0)
{
}

ObSSIOInfo::ObSSIOInfo(const ObSSIOInfo &other)
{
  *this = other;
}

ObSSIOInfo::~ObSSIOInfo()
{
}

void ObSSIOInfo::reset()
{
  ObSNIOInfo::reset();
  phy_block_handle_.reset();
  fd_cache_handle_.reset();
  tmp_file_valid_length_ = 0;
}

ObSSIOInfo &ObSSIOInfo::operator=(const ObSSIOInfo &other)
{
  int ret = OB_SUCCESS;
  if (&other != this) {
    reset();
    tenant_id_ = other.tenant_id_;
    fd_ = other.fd_;
    offset_ = other.offset_;
    size_ = other.size_;
    timeout_us_ = other.timeout_us_;
    flag_ = other.flag_;
    callback_ = other.callback_;
    buf_ = other.buf_;
    user_data_buf_ = other.user_data_buf_;
    part_id_ = other.part_id_;
    tmp_file_valid_length_ = other.tmp_file_valid_length_;
    // ignore ret, cuz assign fails only when other.phy_block_handle_/fd_cache_handle_ is invalid.
    // in case when other.phy_block_handle_/fd_cache_handle_ is invalid, ret is unnecessary.
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(phy_block_handle_.assign(other.phy_block_handle_))) {
      LOG_WARN("fail to assign phy block handle", KR(tmp_ret), KPC(this), K(other));
    }
    if (OB_TMP_FAIL(fd_cache_handle_.assign(other.fd_cache_handle_))) {
      LOG_WARN("fail to assign fd cache handle", KR(tmp_ret), KPC(this), K(other));
    }
  }
  return (*this);
}
#endif

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

/******************             ObIOResult              **********************/
ObIOResult::ObIOResult()
  : is_inited_(false),
    is_finished_(false),
    is_canceled_(false),
    has_estimated_(false),
    is_object_device_req_(false),
    result_ref_cnt_(0),
    out_ref_cnt_(0),
    complete_size_(0),
    offset_(0),
    size_(0),
    timeout_us_(DEFAULT_IO_WAIT_TIME_US),
    aligned_size_(DIO_ALIGN_SIZE),
    tenant_io_mgr_(),
    buf_(nullptr),
    user_data_buf_(nullptr),
    io_callback_(nullptr),
    flag_(),
    cond_(),
    time_log_(),
    ret_code_()
{

}

ObIOResult::~ObIOResult()
{
  destroy();
}

bool ObIOResult::is_valid() const
{
  return offset_ >= 0
    // in order to address concurrent write issues, archive checkpoint module would write
    // multiple non-content objects (it stores content in object name) whose size = 0
    && (flag_.is_sync() ? size_ >= 0 : size_ > 0)
    && timeout_us_ >= 0 //todo qilu: reopen after column_store steady
    && flag_.is_valid()
    && (flag_.is_read() ? (nullptr != io_callback_ || nullptr != user_data_buf_ || flag_.is_detect()) : nullptr != buf_);
}

int ObIOResult::basic_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
    LOG_WARN("init result condition failed", K(ret));
  }
  return ret;
}

int ObIOResult::init(const ObIOInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io result init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!cond_.is_inited())) {
    ret = OB_NOT_INIT;
    LOG_WARN("thread_cond not init yet", K(ret));
  } else if (OB_ISNULL(info.fd_.device_handle_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (info.flag_.is_sync()) {
      if (OB_FAIL(info.fd_.device_handle_->get_io_aligned_size(aligned_size_))) {
        LOG_WARN("get io aligned size failed", K(ret));
      }
    } else {
      aligned_size_ = DIO_ALIGN_SIZE;
    }
  }
  if (OB_SUCC(ret)) {
    //init info and check valid
    tenant_id_ = info.tenant_id_;
    offset_ = info.offset_;
    size_ = info.size_;
    flag_ = info.flag_;
    flag_.set_func_type(GET_FUNC_TYPE());
    flag_.set_resource_group_id(GET_GROUP_ID());
    timeout_us_ = info.timeout_us_;
    if (timeout_us_ <= 0) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {  // 10s
        LOG_WARN("io result timeout_us is unexpected", K(lbt()), K(*this));
      }
    }
    buf_ = info.buf_;
    user_data_buf_ = info.user_data_buf_;
    time_log_.begin_ts_ = ObTimeUtility::fast_current_time();
    is_object_device_req_ = info.fd_.device_handle_->is_object_device();
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(*this));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObIOResult::reset()
{
  is_finished_ = false;
  is_canceled_ = false;
  has_estimated_ = false;
  is_object_device_req_ = false;
  complete_size_ = 0;
  offset_ = 0;
  size_ = 0;
  result_ref_cnt_ = 0;
  out_ref_cnt_ = 0;
  timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  buf_ = nullptr;
  user_data_buf_ = nullptr;
  io_callback_ = nullptr;
  flag_.reset();
  ret_code_.reset();
  time_log_.reset();
  tenant_io_mgr_.reset();
  //do not destroy thread_cond
  is_inited_ = false;
}

void ObIOResult::destroy()
{
  is_finished_ = false;
  is_canceled_ = false;
  has_estimated_ = false;
  is_object_device_req_ = false;
  complete_size_ = 0;
  offset_ = 0;
  size_ = 0;
  result_ref_cnt_ = 0;
  out_ref_cnt_ = 0;
  timeout_us_ = DEFAULT_IO_WAIT_TIME_US;
  buf_ = nullptr;
  user_data_buf_ = nullptr;
  io_callback_ = nullptr;
  flag_.reset();
  ret_code_.reset();
  time_log_.reset();
  cond_.destroy();
  tenant_io_mgr_.reset();
  is_inited_ = false;
}

int64_t ObIOResult::get_data_size() const
{
  int64_t data_size = 0;
  const int64_t aligned_offset = lower_align(offset_, aligned_size_);
  data_size = min(size_, max(0, complete_size_ - (offset_ - aligned_offset)));
  return data_size;
}

ObIOMode ObIOResult::get_mode() const
{
  return flag_.get_mode();
}

ObIOGroupKey ObIOResult::get_group_key() const
{
  return ObIOGroupKey(flag_.get_resource_group_id(), is_object_device_req_ ? get_mode() : ObIOMode::MAX_MODE);
}

uint64_t ObIOResult::get_sys_module_id() const
{
  return flag_.get_sys_module_id();
}

bool ObIOResult::is_sys_module() const
{
  return flag_.is_sys_module();
}

uint64_t ObIOResult::get_io_usage_index()
{
  uint64_t index = 0;
  const ObIOGroupKey key = get_group_key();
  transform_group_config_index_to_usage_index(key, index);
  return index;
}

uint64_t ObIOResult::get_tenant_id() const
{
  return this->tenant_id_;
}
void ObIOResult::cancel()
{
  int ret = OB_SUCCESS;
  if (is_inited_ && !is_finished_) {
    { // must check finished and set cancel in guard
      ObThreadCondGuard guard(cond_);
      if (OB_FAIL(guard.get_ret())) {
        LOG_WARN("fail to guard condition", K(ret));
      } else if (is_finished_) {
        // do nothing
      } else {
        is_canceled_ = true;
        //note: not support channel cancel now
      }
    }
    finish(OB_CANCELED);
  }
}

int ObIOResult::wait(int64_t wait_ms)
{
  int ret = OB_SUCCESS;
  ObThreadCondGuard guard(cond_);
  if (OB_FAIL(guard.get_ret())) {
    LOG_ERROR("fail to guard result condition", K(ret));
  } else {
    int64_t begin_ms = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !is_finished_ && wait_ms > 0) {
      if (OB_FAIL(cond_.wait(wait_ms))) {
        LOG_WARN("fail to wait result condition", K(ret), K(wait_ms), K(*this));
      } else if (!is_finished_) {
        int64_t duration_ms = ObTimeUtility::current_time() - begin_ms;
        wait_ms -= duration_ms;
      }
    }
    if (OB_UNLIKELY(wait_ms <= 0)) { // rarely happen
      ret = OB_TIMEOUT;
    }
  }
  return ret;
}

void ObIOResult::inc_ref(const char *msg)
{
  (void)ATOMIC_FAA(&result_ref_cnt_, 1);
}

void ObIOResult::dec_ref(const char *msg)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder(tenant_io_mgr_.get_ptr());
  int64_t tmp_ref = ATOMIC_SAF(&result_ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bug: result_ref_cnt < 0", K(ret), K(tmp_ref), KCSTRING(lbt()));
    abort();
  } else if (0 == tmp_ref) {
    if (OB_ISNULL(tenant_holder.get_ptr())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tenant io manager is null, memory leak", K(ret));
    } else {
      if (tenant_holder.get_ptr()->io_result_pool_.contain(this)) {
        reset();
        tenant_holder.get_ptr()->io_result_pool_.recycle(this);
      } else {
        // destroy will be called when free
        tenant_holder.get_ptr()->io_allocator_.free(this);
      }
    }
  }
}

void ObIOResult::finish(const ObIORetCode &ret_code, ObIORequest *req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_LIKELY(!is_finished_)) {
      ret_code_ = ret_code;
      ATOMIC_STORE(&is_finished_, true);
      if (OB_NOT_NULL(tenant_io_mgr_.get_ptr()) && OB_NOT_NULL(req)) {
        if (is_sys_module()) {
          // sys group accumulate
          tenant_io_mgr_.get_ptr()->io_sys_usage_.accumulate(*req);
        } else {
          tenant_io_mgr_.get_ptr()->io_usage_.accumulate(*req);
        }
        time_log_.end_ts_ = ObTimeUtility::fast_current_time();
        if (req->fd_.device_handle_->is_object_device()) {
          const ObStorageIdMod &storage_info = ((ObObjectDevice*)(req->fd_.device_handle_))->get_storage_id_mod();
          if (OB_UNLIKELY(!storage_info.is_valid())) {
            LOG_WARN("invalid storage id", K(storage_info));
          } else {
            OB_IO_MANAGER.get_tc().calc_usage(*req);
          }
        }
        tenant_io_mgr_.get_ptr()->io_func_infos_.accumulate(*req);
        // do not detect backup io
        if (!req->fd_.is_backup_block_file()) {
          // record io error
          if (OB_UNLIKELY(OB_IO_ERROR == ret_code_.io_ret_)) {
            OB_IO_MANAGER.get_device_health_detector().record_io_error(*this, *req);
          }
          // record timeout
          if (OB_UNLIKELY(ObTimeUtility::current_time() > req->timeout_ts())) {
            OB_IO_MANAGER.get_device_health_detector().record_io_timeout(*this, *req);
          }
	      }
      }
      if (OB_FAIL(guard.get_ret())) {
        LOG_ERROR("lock io result condition failed", K(ret), K(*this));
      } else if (OB_FAIL(cond_.signal())) {
        LOG_ERROR("signal io result condition failed", K(ret), K(*this));
      }
    }
  }
}

void ObIOResult::inc_out_ref()
{
  ATOMIC_INC(&out_ref_cnt_);
}

void ObIOResult::dec_out_ref()
{
  int ret =OB_SUCCESS;
  if (0 == ATOMIC_SAF(&out_ref_cnt_, 1)) {
    cancel();

    if (OB_NOT_NULL(io_callback_) && OB_NOT_NULL(io_callback_->get_allocator())) {
      ObIAllocator *tmp_allocator = io_callback_->get_allocator();
      io_callback_->~ObIOCallback(); //allocator will set null
      tmp_allocator->free(io_callback_);
      io_callback_ = nullptr;
    }
  }
}

void ObIOResult::finish_without_accumulate(const ObIORetCode &ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_LIKELY(!is_finished_)) {
      ret_code_ = ret_code;
      ATOMIC_STORE(&is_finished_, true);
      time_log_.end_ts_ = ObTimeUtility::fast_current_time();
      if (OB_FAIL(guard.get_ret())) {
        LOG_ERROR("lock io result condition failed", K(ret), K(*this));
      } else if (OB_FAIL(cond_.signal())) {
        LOG_ERROR("signal io result condition failed", K(ret), K(*this));
      }
    }
  }
}

int ObIOResult::transform_group_config_index_to_usage_index(const ObIOGroupKey &key, uint64_t &usage_index)
{
  int ret = OB_SUCCESS;
  uint64_t tmp_index = 0;
  uint64_t offset = 0;
  uint64_t MODE_CNT = static_cast<uint64_t>(ObIOMode::MAX_MODE) + 1;
  uint64_t GROUP_MODE_CNT = static_cast<uint64_t>(ObIOGroupMode::MODECNT);
  // LOCAL READ = 0,
  // LOCAL WRITE = 1,
  // REMOTE READ = 2,
  // REMOTE WRITE = 3
  if (is_sys_module()) {
    offset = is_object_device_req_ ? 1 : 0;
    offset = offset * 2 + static_cast<uint64_t>(get_mode());
    tmp_index = get_sys_module_id() - SYS_MODULE_START_ID;
    usage_index = tmp_index * GROUP_MODE_CNT + offset;
  } else {
    if (OB_FAIL(tenant_io_mgr_.get_ptr()->get_group_index(key, tmp_index))) {
      // do nothing
    }
    uint64_t quot = tmp_index / MODE_CNT;
    usage_index = quot * GROUP_MODE_CNT +
                  (key.mode_ == ObIOMode::MAX_MODE
                          ? static_cast<uint64_t>(get_mode())
                          : static_cast<uint64_t>(key.mode_) + static_cast<uint64_t>(ObIOGroupMode::REMOTEREAD));
  }
  return ret;
}

int ObIOResult::cal_delay_us(int64_t &prepare_delay, int64_t &schedule_delay, int64_t &submit_delay, int64_t &device_delay, int64_t &total_delay)
{
  int ret = OB_SUCCESS;
  prepare_delay = get_io_interval(this->time_log_.enqueue_ts_, this->time_log_.begin_ts_);
  schedule_delay = get_io_interval(this->time_log_.dequeue_ts_, this->time_log_.enqueue_ts_);
  submit_delay = get_io_interval(this->time_log_.submit_ts_, this->time_log_.dequeue_ts_);
  device_delay = get_io_interval(this->time_log_.return_ts_, this->time_log_.submit_ts_);
  total_delay = get_io_interval(this->time_log_.return_ts_, this->time_log_.begin_ts_);
  return ret;
}

/******************             IORequest              **********************/

ObIORequest::ObIORequest()
  : io_result_(nullptr),
    is_inited_(false),
    retry_count_(0),
    ref_cnt_(0),
    raw_buf_(nullptr),
    control_block_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    tenant_io_mgr_(),
    storage_accesser_(),
    fd_(),
    trace_id_(),
    part_id_(-1)
{

}

ObIORequest::~ObIORequest()
{
  destroy();
}

bool ObIORequest::is_valid() const
{
  return nullptr != io_result_
      && io_result_->is_valid()
      && tenant_id_ > 0
      && fd_.is_valid();
}

int ObIORequest::basic_init()
{
  return OB_SUCCESS;
}

int ObIORequest::init(const ObIOInfo &info, ObIOResult *result)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io request init twice", K(ret), K(is_inited_));
  } else if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else {
    io_result_ = result;
    io_result_->inc_ref("request");
    trace_id_ = *ObCurTraceId::get_trace_id();
    //init info and check valid
    tenant_id_ = info.tenant_id_;
    fd_ = info.fd_;
    part_id_ = info.part_id_;
    char *io_buf = nullptr;
    buf_size_ = 0;
    if (OB_FAIL(set_block_handle(info))) {
      LOG_WARN("fail to set block handle", K(ret), K(info));
    } else if (OB_FAIL(set_fd_cache_handle(info))) {
      LOG_WARN("fail to fd cache handle", K(ret), K(info));
    } else if (OB_FAIL(calc_io_offset_and_size_())) {
      LOG_WARN("fail to calc io offset and size", K(ret), K(info));
    } else if (OB_ISNULL(fd_.device_handle_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("device handle is null", KR(ret), K(*this));
    } else if (fd_.device_handle_->is_object_device()) {
      ObObjectDevice *obj_device_handle = static_cast<ObObjectDevice *>(fd_.device_handle_);
      int flag = -1;
      ObFdSimulator::get_fd_flag(fd_, flag);

      io_result_->flag_.set_sync();
      // alloc buffer for sync read/write request when ObIORequest init
      if (OB_UNLIKELY(!is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(*this));
      } else if ((info.flag_.is_read() || info.flag_.is_write())
                 && (io_result_->size_ > 0) // size == 0 does not need to alloc io buf
                 && (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER != flag)
                 && OB_FAIL(alloc_io_buf(io_buf))) {
        LOG_WARN("fail to alloc io buffer for sync read or write", K(ret), K(info));
      } else if (OB_FAIL(hold_storage_accesser(fd_, *obj_device_handle))) {
        LOG_WARN("fail to hold storage accesser", K(ret), K_(fd), KP(obj_device_handle));
      }
    } else if (OB_UNLIKELY(!is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(*this));
    } else if (info.flag_.is_write() && OB_FAIL(alloc_io_buf(io_buf))) {
      // alloc buffer for write request when ObIORequest init
      LOG_WARN("alloc io buffer for write failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

void ObIORequest::set_result(ObIOResult &io_result)
{
  io_result.inc_ref("request");
  io_result_ = &io_result;
}

void ObIORequest::free()
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder(tenant_io_mgr_.get_ptr());
  if (OB_ISNULL(tenant_holder.get_ptr())) {
    //not set yet, do nothing
  } else {
    if (tenant_holder.get_ptr()->io_request_pool_.contain(this)) {
      destroy();
      tenant_holder.get_ptr()->io_request_pool_.recycle(this);
    } else {
      // destroy will be called when free
      tenant_holder.get_ptr()->io_allocator_.free(this);
    }
  }
}

void ObIORequest::reset() //only for test, not dec resut_ref
{
  int ret = OB_SUCCESS;
  retry_count_ = 0;
  // only read need destroy here
  // TODO(yanfeng): works now, need refactor
  if (fd_.is_backup_block_file() && nullptr != io_result_ && io_result_->flag_.is_read()) {
    backup::ObLSBackupFactory::free(static_cast<backup::ObBackupWrapperIODevice *>(fd_.device_handle_));
  }
  if (nullptr != control_block_ && nullptr != fd_.device_handle_) {
    fd_.device_handle_->free_iocb(control_block_);
    control_block_ = nullptr;
  }

  tenant_id_ = 0;
  free_io_buffer();
  ref_cnt_ = 0;
  trace_id_.reset();
  io_result_ = nullptr;
  fd_.reset();
  tenant_io_mgr_.reset();
  storage_accesser_.reset();
  is_inited_ = false;
  part_id_ = -1;
}

void ObIORequest::destroy()
{
  int ret = OB_SUCCESS;
  retry_count_ = 0;
  if (nullptr != control_block_ && nullptr != fd_.device_handle_) {
    fd_.device_handle_->free_iocb(control_block_);
    control_block_ = nullptr;
  }

  fd_.reset();
  tenant_id_ = 0;
  free_io_buffer();
  ref_cnt_ = 0;
  trace_id_.reset();
  if (nullptr != io_result_) {
    io_result_->dec_ref("request");
    io_result_ = nullptr;
  }
  tenant_io_mgr_.reset();
  storage_accesser_.reset();
  is_inited_ = false;
  part_id_ = -1;
}

bool ObIORequest::is_canceled()
{
  //result = null should not happen, no need to continue request
  return nullptr == io_result_ ? true : io_result_->is_canceled_;
}

int64_t ObIORequest::get_data_size() const
{
  return nullptr == io_result_ ? 0 : io_result_->get_data_size();
}

ObIOGroupKey ObIORequest::get_group_key() const
{
  ObIOGroupKey ret;
  if (OB_ISNULL(io_result_)) {
    // do nothing
  } else {
    ret = io_result_->get_group_key();
  }
  return ret;
}

int64_t ObIORequest::timeout_ts() const
{
  return nullptr == io_result_ ? 0 : io_result_->time_log_.begin_ts_ > 0 ? io_result_->time_log_.begin_ts_ + io_result_->timeout_us_ : 0;
}

uint64_t ObIORequest::get_sys_module_id() const
{
  return nullptr == io_result_ ? OB_INVALID_ID : io_result_->flag_.get_sys_module_id();
}

bool ObIORequest::is_sys_module() const
{
  return nullptr == io_result_ ? false : io_result_->flag_.is_sys_module();
}

char *ObIORequest::calc_io_buf()
{
  int ret = OB_SUCCESS;
  char *ret_buf = nullptr;
  if (OB_NOT_NULL(io_result_)) {
    ret_buf = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), io_result_->aligned_size_));
  }
  return ret_buf;
}

int ObIORequest::calc_io_offset_and_size_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else if (OB_ISNULL(fd_.device_handle_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("device handle is null", K(ret));
  } else {
    align_offset_size(io_result_->offset_, io_result_->size_, io_result_->aligned_size_, align_offset_, align_size_);
  }
  return ret;
}

const char *ObIORequest::get_io_data_buf()
{
  const char *buf = nullptr;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == raw_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("raw buf is null, maybe has been recycle", K(ret));
  } else if (OB_ISNULL(io_result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", K(ret));
  } else {
    // re-calculate with const parameters, in case of partial return change aligned_buf and so on.
    const int64_t aligned_offset = lower_align(io_result_->offset_, io_result_->aligned_size_);
    char *aligned_buf = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), io_result_->aligned_size_));
    buf = aligned_buf + io_result_->offset_ - aligned_offset;
  }
  return buf;
}

int64_t ObIORequest::get_align_size() const
{
  return std::max(1L, align_size_);
}

int64_t ObIORequest::get_align_offset() const
{
  return align_offset_;
}
const ObIOFlag &ObIORequest::get_flag() const
{
  return io_result_->flag_;
}

ObIOMode ObIORequest::get_mode() const
{
  return io_result_->flag_.get_mode(); // 2 mode : read and write
}

ObIOGroupMode ObIORequest::get_group_mode() const
{
  ObIOGroupMode group_mode = ObIOGroupMode::MODECNT;
  if (fd_.device_handle_->is_object_device()) {
    if (get_mode() == ObIOMode::READ) {
      group_mode = ObIOGroupMode::REMOTEREAD;
    } else if (get_mode() == ObIOMode::WRITE) {
      group_mode = ObIOGroupMode::REMOTEWRITE;
    }
  } else {
    if (get_mode() == ObIOMode::READ) {
      group_mode = ObIOGroupMode::LOCALREAD;
    } else if (get_mode() == ObIOMode::WRITE) {
      group_mode = ObIOGroupMode::LOCALWRITE;
    }
  }
  if (group_mode == ObIOGroupMode::MODECNT) {
    int ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get wrong mode", K(ret), K(*this), K(group_mode));
  }
  return group_mode;
}

ObIOCallback *ObIORequest::get_callback() const
{
  return io_result_->io_callback_;
}

int ObIORequest::get_buf_size() const
{
  return buf_size_;
}

int ObIORequest::alloc_io_buf(char *&io_buf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io info", K(ret), K(*this));
  } else if (nullptr != io_result_) {
    ret = alloc_aligned_io_buf(io_buf);
  }
  return ret;
}

int ObIORequest::alloc_aligned_io_buf(char *&io_buf)
{
  int ret = OB_SUCCESS;
  int64_t io_offset = 0;
  int64_t io_size = 0;
  int64_t aligned_size = 0;
  if (OB_ISNULL(io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else if (OB_UNLIKELY(!io_result_->flag_.is_read() && !io_result_->flag_.is_write())) { // only for read and write
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io info, io mode is wrong", K(ret), K(*this));
  } else if (OB_ISNULL(fd_.device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle is null", K(ret));
  } else if (FALSE_IT(aligned_size = io_result_->aligned_size_)) {
  } else if (io_result_->flag_.is_write() // write io must aligned
      && OB_UNLIKELY(!is_io_aligned(io_result_->offset_, aligned_size)
                     || !is_io_aligned(io_result_->size_, aligned_size))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("write io info not aligned", K(ret), K(*this));
  } else {
    align_offset_size(io_result_->offset_, io_result_->size_, aligned_size, io_offset, io_size);
    const int64_t io_buffer_size = ((1 == aligned_size) ? io_size : (io_size + aligned_size));
    if (OB_ISNULL(tenant_io_mgr_.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant io manager is null", K(ret));
    } else if (OB_ISNULL(raw_buf_ = tenant_io_mgr_.get_ptr()->io_allocator_.alloc(io_buffer_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(io_size));
    } else {
      io_buf = reinterpret_cast<char *>(upper_align(reinterpret_cast<int64_t>(raw_buf_), aligned_size));
      buf_size_ = io_buffer_size;
      tenant_io_mgr_.get_ptr()->io_mem_stats_.inc(*this);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(raw_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io raw buf is null", K(ret));
    } else if (!is_io_aligned((int64_t)io_buf, aligned_size)
        || !is_io_aligned(io_size, aligned_size)
        || !is_io_aligned(io_offset, aligned_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The io buffer is not aligned", K(ret), K(*this), KP(io_buf), K(io_offset), K(io_size));
    } else if (io_result_->flag_.is_write()) {
      MEMCPY(io_buf, io_result_->buf_, io_result_->size_);
    }
  }
  return ret;
}

int ObIORequest::prepare(char *next_buffer, int64_t next_size, int64_t next_offset)
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg("prepare", 100000); //100ms
  int64_t io_offset = 0;
  int64_t io_size = 0;
  if (next_size != 0 || next_offset != 0) {
    io_offset = next_offset;
    io_size = next_size;
  } else {
    io_offset = this->get_align_offset();
    io_size = this->get_align_size();
  }
  char *io_buf = next_buffer != nullptr ? next_buffer : calc_io_buf();

  if (OB_ISNULL(fd_.device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle is null", K(ret), K(*this));
  } else if (fd_.device_handle_->is_object_device()) {
    // do nothing
  } else if (OB_ISNULL(control_block_) && OB_ISNULL(control_block_ = fd_.device_handle_->alloc_iocb(tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc io control block failed", K(ret), K(*this));
  } else if (FALSE_IT(tg.click("alloc_iocb"))) {
  } else if (OB_ISNULL(io_buf) && OB_FAIL(alloc_io_buf(io_buf))) {
    // delayed alloc buffer for read request here to reduce memory usage when io request enqueue
    LOG_WARN("alloc io buffer for read failed", K(ret), K(*this));
  } else if (FALSE_IT(tg.click("alloc_buf"))) {
  } else if (OB_ISNULL(io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else {
    if (fd_.is_backup_block_file()) {
      // ignore
    } else if (io_result_->flag_.is_read()) {
      if (OB_FAIL(fd_.device_handle_->io_prepare_pread(
              fd_,
              io_buf,
              io_size,
              io_offset,
              control_block_,
              this/*data*/))) {
        LOG_WARN("prepare io read failed", K(ret), K(*this));
      }
      tg.click("prepare_read");
    } else if (io_result_->flag_.is_write()) {
      if (OB_FAIL(fd_.device_handle_->io_prepare_pwrite(
              fd_,
              io_buf,
              io_size,
              io_offset,
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
  if (OB_UNLIKELY(tg.get_diff() > 100000)) {// 100ms
    //print req
    LOG_INFO("prepare_request cost too much time", K(ret), K(tg), K(*this));
  }
  return ret;
}

int ObIORequest::recycle_buffer()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else {
    ObThreadCondGuard guard(io_result_->cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("fail to guard IOresult condition", K(ret));
    } else if (io_result_->flag_.is_detect()) {
      free_io_buffer();
    } else {
      if (nullptr == io_result_->user_data_buf_) {
        // do nothing，无需memcpy，也不能free io buffer
      } else if (!is_canceled() && nullptr != get_io_data_buf()) {
        MEMCPY(io_result_->user_data_buf_, get_io_data_buf(), get_data_size());
        free_io_buffer();
      }
    }
  }
  return ret;
}

int ObIORequest::re_prepare()
{
  int ret = OB_SUCCESS;
  ObTimeGuard tg("re_prepare", 100000); //100ms
  char *io_buf = calc_io_buf();
  if (OB_ISNULL(control_block_)
      && (OB_ISNULL(fd_.device_handle_) || OB_ISNULL(control_block_ = fd_.device_handle_->alloc_iocb(tenant_id_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc io control block failed", K(ret), K(*this));
  } else if (FALSE_IT(tg.click("alloc_iocb"))) {
  } else if (OB_ISNULL(io_buf) && OB_FAIL(try_alloc_buf_until_timeout(io_buf))) {
    // delayed alloc buffer for read request here to reduce memory usage when io request enqueue
    LOG_WARN("alloc io buffer for read failed", K(ret), K(*this));
  } else if (FALSE_IT(tg.click("try_alloc_buf"))) {
  } else if (OB_ISNULL(io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else {
    if (io_result_->flag_.is_read()) {
      if (OB_FAIL(fd_.device_handle_->io_prepare_pread(
              fd_,
              io_buf,
              this->get_align_size(),
              this->get_align_offset(),
              control_block_,
              this/*data*/))) {
        LOG_WARN("prepare io read failed", K(ret), K(*this));
      }
      tg.click("prepare_read");
    } else if (io_result_->flag_.is_write()) {
      if (OB_FAIL(fd_.device_handle_->io_prepare_pwrite(
              fd_,
              io_buf,
              this->get_align_size(),
              this->get_align_offset(),
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
  if (OB_UNLIKELY(tg.get_diff() > 100000)) {// 100ms
    //print req
    LOG_INFO("prepare_request cost too much time", K(ret), K(tg), K(*this));
  }
  return ret;
}

int ObIORequest::try_alloc_buf_until_timeout(char *&io_buf)
{
  int ret = OB_SUCCESS;
  int64_t retry_alloc_count = 0;
  while (OB_SUCC(ret)) {
    const int64_t current_ts = ObTimeUtility::current_time();
    ++retry_alloc_count;
    if (current_ts > timeout_ts()) {
      ret = OB_TIMEOUT;
      LOG_WARN("current time is larger than the timeout timestamp", K(ret), K(current_ts), K(timeout_ts()), K(retry_alloc_count));
    } else if (OB_FAIL(alloc_io_buf(io_buf))) {
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        const int64_t remain_time = timeout_ts() - current_ts;
        const int64_t sleep_time = min(remain_time, 1000L);
        if (TC_REACH_TIME_INTERVAL(1000L * 1000L)) {
          LOG_INFO("alloc memory failed, retry later", K(ret), K(remain_time), K(sleep_time), K(retry_alloc_count));
        }
        ob_usleep((useconds_t)sleep_time);
        ret = OB_SUCCESS;
      }
    } else {
      LOG_INFO("retry alloc io_buf success", K(retry_alloc_count));
      break;
    }
  }
  return ret;
}

bool ObIORequest::can_callback() const
{
  // both async_io and sync_io can callback
  return nullptr != io_result_ && nullptr != get_callback() && nullptr != raw_buf_;
}

void ObIORequest::free_io_buffer()
{
  if (nullptr != raw_buf_ && nullptr != tenant_io_mgr_.get_ptr()) {
    tenant_io_mgr_.get_ptr()->io_mem_stats_.dec(*this);
    tenant_io_mgr_.get_ptr()->io_allocator_.free(raw_buf_);
    raw_buf_ = nullptr;
  }
}

void ObIORequest::inc_ref(const char *msg)
{
  int64_t old_ref_cnt = ATOMIC_FAA(&ref_cnt_, 1);
  if (nullptr != msg && OB_NOT_NULL(tenant_io_mgr_.get_ptr())) {
    int tmp_ret = OB_SUCCESS;
    ObIOTracer::TraceType trace_type = 0 == old_ref_cnt ? ObIOTracer::TraceType::IS_FIRST : ObIOTracer::TraceType::OTHER;
    if (OB_TMP_FAIL(tenant_io_mgr_.get_ptr()->trace_request_if_need(this, msg, trace_type))) {
      LOG_WARN_RET(tmp_ret, "add trace for io request failed", K(tmp_ret), KPC(this), K(trace_type));
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
    LOG_ERROR("bug: ref_cnt < 0", K(ret), K(tmp_ref), KCSTRING(lbt()), K(*this));
    abort();
  } else if (nullptr != msg && OB_NOT_NULL(tenant_holder.get_ptr())) {
    int tmp_ret = OB_SUCCESS;
    ObIOTracer::TraceType trace_type = 0 == tmp_ref ? ObIOTracer::TraceType::IS_LAST : ObIOTracer::TraceType::OTHER;
    if (OB_TMP_FAIL(tenant_holder.get_ptr()->trace_request_if_need(this, msg, trace_type))) {
      LOG_WARN("remove trace for io request failed", K(tmp_ret), KPC(this), K(trace_type));
    }
  }
  if (0 == tmp_ref) {
    if (OB_ISNULL(tenant_holder.get_ptr())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("tenant io manager is null, memory leak", K(ret), KCSTRING(lbt()), K(*this));
    } else {
      if (tenant_holder.get_ptr()->io_request_pool_.contain(this)) {
        destroy();
        tenant_holder.get_ptr()->io_request_pool_.recycle(this);
      } else {
        // destroy will be called when free
        tenant_holder.get_ptr()->io_allocator_.free(this);
      }
    }
  }
}

int64_t ObIORequest::get_remained_io_timeout_us()
{
  int64_t cur_remained_timeout_us = 0;
  if (OB_NOT_NULL(io_result_)) {
    cur_remained_timeout_us = io_result_->timeout_us_
                            - (ObTimeUtility::current_time() - io_result_->time_log_.begin_ts_);
  }
  return MAX(0, cur_remained_timeout_us);
}

int ObIORequest::set_block_handle(const ObIOInfo &info)
{
  // do nothing
  return OB_SUCCESS;
}

int ObIORequest::set_fd_cache_handle(const ObIOInfo &info)
{
  // do nothing
  return OB_SUCCESS;
}

int ObIORequest::hold_storage_accesser(const ObIOFd &fd, ObObjectDevice &object_device)
{
  int ret = OB_SUCCESS;
  void *ctx = NULL;
  ObStorageAccesser *storage_accesser = nullptr;
  if (OB_FAIL(object_device.get_fd_mng().fd_to_ctx(fd, ctx))) {
    LOG_WARN("fail to get ctx accroding fd", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else {
    storage_accesser = static_cast<ObStorageAccesser *>(ctx);
    storage_accesser_.hold(storage_accesser);
  }
  return ret;
}


/******************             IOPhyQueue              **********************/

ObPhyQueue::ObPhyQueue()
  : is_inited_(false),
    stop_accept_(false),
    reservation_ts_(INT_MAX64),
    limitation_ts_(INT_MAX64),
    proportion_ts_(INT_MAX64),
    last_empty_ts_(INT_MAX64),
    queue_index_(-1),
    reservation_pos_(-1),
    limitation_pos_(-1),
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
    last_empty_ts_ = ObTimeUtility::fast_current_time();
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
  limitation_ts_ = INT_MAX64;
  proportion_ts_ = INT_MAX64;
  last_empty_ts_ = INT_MAX64;
  reservation_pos_ = -1;
  limitation_pos_ = -1;
  proportion_pos_ = -1;
  queue_index_ = -1;
}

void ObPhyQueue::reset_time_info()
{
  reservation_ts_ = INT_MAX64;
  limitation_ts_ = INT_MAX64;
  proportion_ts_ = INT_MAX64;
}

void ObPhyQueue::reset_queue_info()
{
  reservation_pos_ = -1;
  limitation_pos_ = -1;
  proportion_pos_ = -1;
}

bool ObPhyQueue::reach_adjust_interval()
{
  int64_t cur_ts = ObTimeUtility::fast_current_time();
  return cur_ts - last_empty_ts_ > CLOCK_IDLE_THRESHOLD_US;
}

/******************             IOHandle              **********************/
ObIOHandle::ObIOHandle()
  : result_(nullptr)
{
}

ObIOHandle::~ObIOHandle()
{
  reset();
}

ObIOHandle::ObIOHandle(const ObIOHandle &other)
  : result_(nullptr)
{
  *this = other;
}

ObIOHandle& ObIOHandle::operator=(const ObIOHandle &other)
{
  if (&other != this) {
  	int ret = OB_SUCCESS;
    if (OB_NOT_NULL(other.result_)) {
	if (OB_FAIL(set_result(*other.result_))) {
    		LOG_ERROR("set io request failed", K(ret));
    	}
    }
  }
  return *this;
}

int ObIOHandle::set_result(ObIOResult &result)
{
  int ret = OB_SUCCESS;
  reset();
  result.inc_ref("handle_inc"); // ref for handle
  result.inc_out_ref();
  result_ = &result;
#ifdef ENABLE_DEBUG_LOG
  storage::ObStorageLeakChecker::get_instance().handle_hold(this, storage::ObStorageCheckID::IO_HANDLE);
#endif
  return ret;
}

bool ObIOHandle::is_empty() const
{
  return nullptr == result_;
}

bool ObIOHandle::is_valid() const
{
  return nullptr != result_;
}

int ObIOHandle::get_io_ret() const
{
  int ret = OB_NOT_INIT;
  if (nullptr != result_) {
    ret = result_->ret_code_.io_ret_;
  }
  return ret;
}

int ObIOHandle::get_io_flag(ObIOFlag &flag) const
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(result_)) {
    flag = result_->flag_;
  } else {
    ret = OB_NOT_INIT;
    LOG_WARN("io result is null", KR(ret));
  }
  return ret;
}

int ObIOHandle::get_io_time_us(int64_t &io_time_us) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io result is null", KR(ret));
  } else {
    io_time_us = get_io_interval(result_->time_log_.end_ts_, result_->time_log_.begin_ts_);
  }
  return ret;
}

int ObIOHandle::check_is_finished(bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The IOHandle has not been inited", KR(ret));
  } else if (true == ATOMIC_LOAD(&result_->is_finished_)) {
    is_finished = true;
  }
  return ret;
}

int ObIOHandle::wait(const int64_t wait_timeout_ms)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The IOHandle has not been inited, ", K(ret));
  } else if (OB_FAIL(result_->ret_code_.io_ret_)) {
    LOG_WARN("IO error, ", K(ret), K(*result_));
  } else if (result_->is_finished_) {
    // do nothing
  } else if (0 == wait_timeout_ms) {
    ret = OB_EAGAIN;
  } else if (UINT64_MAX == wait_timeout_ms) {
    const int64_t timeout_ms =
        ((result_->time_log_.begin_ts_ > 0
                ? result_->time_log_.begin_ts_ + result_->timeout_us_ - ObTimeUtility::current_time()
                : 0)) /
        1000L;
    ObWaitEventGuard wait_guard(result_->flag_.get_wait_event(),
                                timeout_ms,
                                result_->size_);
    const int64_t real_wait_timeout = (result_->is_object_device_req_
                                       ? timeout_ms
                                       : min(OB_IO_MANAGER.get_io_config().data_storage_io_timeout_ms_, timeout_ms));

    if (real_wait_timeout > 0) {
      int64_t wait_ms = real_wait_timeout;
      if (OB_FAIL(result_->wait(wait_ms))) {
        if (OB_TIMEOUT == ret) {
          LOG_WARN("fail to wait result condition due to spurious wakeup", K(ret), K(wait_ms), K(*result_));
        }
      }
    } else if (result_->is_finished_) {
      // do nothing
    } else {
      ret = OB_TIMEOUT;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {  // 10s
        LOG_WARN(
            "real_wait_timeout is unexpected < 0", K(ret), K(real_wait_timeout), K(timeout_ms), K(result_), K(lbt()));
      }
    }
  } else {
    int64_t wait_ms = wait_timeout_ms;
    if (OB_FAIL(result_->wait(wait_ms))) {
      if (OB_TIMEOUT == ret) {
        const int64_t real_wait_timeout = min(OB_IO_MANAGER.get_io_config().data_storage_io_timeout_ms_, result_->timeout_us_ / 1000L);
        if ((ObTimeUtility::current_time() - result_->time_log_.begin_ts_) / 1000L < real_wait_timeout) {
          ret = OB_EAGAIN;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ATOMIC_LOAD(&result_->ret_code_.io_ret_))) {
      LOG_WARN("IO error, ", K(ret), K(*result_));
    }
  } else if (OB_TIMEOUT == ret || OB_IO_TIMEOUT == ret) {
    LOG_WARN("IO wait timeout", K(ret), K(*result_));
  }
  estimate();

  // TODO(binifei.bnf) just for debug, remove this log later
  if (OB_UNLIKELY(NULL != (strstr(GETTNAME(), "DiskCB")))) {
    FLOG_INFO("wait io in DiskCB thread", K(lbt()));
  }

  return ret;
}

void ObIOHandle::estimate()
{
  if (OB_NOT_NULL(result_) && result_->is_finished_ && !ATOMIC_CAS(&result_->has_estimated_, false, true)) {
    oceanbase::common::ObTenantDiagnosticInfoSummaryGuard guard(result_->get_tenant_id(), result_->flag_.get_resource_group_id());
    const int64_t result_delay = get_io_interval(result_->time_log_.end_ts_, result_->time_log_.begin_ts_);
    if (result_->flag_.is_read()) {
      EVENT_INC(ObStatEventIds::IO_READ_COUNT);
      EVENT_ADD(ObStatEventIds::IO_READ_BYTES, result_->size_);
      EVENT_ADD(ObStatEventIds::IO_READ_DELAY, result_delay);
    } else {
      EVENT_INC(ObStatEventIds::IO_WRITE_COUNT);
      EVENT_ADD(ObStatEventIds::IO_WRITE_BYTES, result_->size_);
      EVENT_ADD(ObStatEventIds::IO_WRITE_DELAY, result_delay);
    }
    static const int64_t LONG_IO_PRINT_TRIGGER_US = 1000L * 1000L * 3L; // 3s
    if (result_delay > LONG_IO_PRINT_TRIGGER_US) {
      LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "io result wait too long", KPC(result_), K(result_delay));
    }
  }
}

int ObIOHandle::get_fs_errno(int &io_errno) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(result_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("empty io handle", K(ret), KP(result_));
  } else {
    io_errno = result_->ret_code_.fs_errno_;
  }
  return ret;
}

const char *ObIOHandle::get_buffer()
{
  const char *buf = nullptr;
  if (OB_NOT_NULL(result_) && result_->is_finished_) {
    if (nullptr != result_->io_callback_) {
      buf = result_->io_callback_->get_data();
    } else {
      buf = result_->user_data_buf_;
    }
  }
  return buf;
}

int64_t ObIOHandle::get_data_size() const
{
  return OB_NOT_NULL(result_) ? result_->get_data_size() : 0;
}

int64_t ObIOHandle::get_rt() const
{
  return OB_NOT_NULL(result_) ? get_io_interval(result_->time_log_.end_ts_, result_->time_log_.begin_ts_) : -1;
}

void ObIOHandle::reset()
{
  if (OB_NOT_NULL(result_)) {
#ifdef ENABLE_DEBUG_LOG
    storage::ObStorageLeakChecker::get_instance().handle_reset(this, storage::ObStorageCheckID::IO_HANDLE);
#endif
    result_->dec_out_ref();
    result_->dec_ref("handle_dec"); // ref for handle
    result_ = nullptr;
  }
}

void ObIOHandle::cancel()
{
  if (OB_NOT_NULL(result_)) {
    result_->cancel();
  }
}

void ObIOHandle::clear_io_callback()
{
  if (OB_NOT_NULL(result_)) {
    result_->io_callback_ = nullptr;
  }
}

ObIOCallback *ObIOHandle::get_io_callback()
{
  ObIOCallback *callback = nullptr;
  if (OB_NOT_NULL(result_)) {
    callback = result_->io_callback_;
  }
  return callback;
}

/******************             TenantIOConfig              **********************/
ObTenantIOConfig::UnitConfig::UnitConfig()
  : min_iops_(0), max_iops_(0), weight_(0), max_net_bandwidth_(0), net_bandwidth_weight_(0)
{

}

bool ObTenantIOConfig::UnitConfig::is_valid() const
{
  return min_iops_ > 0 && max_iops_ >= min_iops_ && weight_ >= 0 && max_net_bandwidth_ > 0 && net_bandwidth_weight_ >= 0;
}

ObTenantIOConfig::GroupConfig::GroupConfig()
  : deleted_(false),
    cleared_(false),
    min_percent_(0),
    max_percent_(0),
    weight_percent_(0),
    group_id_(0),
    mode_(ObIOMode::MAX_MODE)
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
  : memory_limit_(0), callback_thread_count_(0), group_configs_(),
    group_config_change_(false), enable_io_tracer_(false),
    object_storage_io_timeout_ms_(DEFAULT_OBJECT_STORAGE_IO_TIMEOUT_MS)
{
  int ret = OB_SUCCESS;
  GroupConfig tmp_group_config;
  tmp_group_config.min_percent_ = 100;
  tmp_group_config.max_percent_ = 100;
  tmp_group_config.weight_percent_ = 100;
  for (uint8_t i = (uint8_t)ObIOMode::READ; i <= (uint8_t)ObIOMode::MAX_MODE; ++i) {
    tmp_group_config.mode_ = (ObIOMode)i;
    memcpy(tmp_group_config.group_name_, other_group_name.ptr(), other_group_name.length());
    tmp_group_config.group_name_[other_group_name.length()] = '\0';
    if (OB_FAIL(group_configs_.push_back(tmp_group_config))) {
      LOG_WARN("push back group config failed", K(ret));
    }
  }
}

ObTenantIOConfig::ObTenantIOConfig(const share::ObUnitConfig &unit_config) : ObTenantIOConfig()
{
  memory_limit_ = unit_config.memory_size();
  unit_config_.min_iops_ = unit_config.min_iops();
  unit_config_.max_iops_ = unit_config.max_iops();
  unit_config_.weight_ = unit_config.iops_weight();
  unit_config_.max_net_bandwidth_ =unit_config.max_net_bandwidth();
  unit_config_.net_bandwidth_weight_ = unit_config.net_bandwidth_weight();
  object_storage_io_timeout_ms_ = DEFAULT_OBJECT_STORAGE_IO_TIMEOUT_MS;
}

ObTenantIOConfig::~ObTenantIOConfig()
{
  destroy();
}

void ObTenantIOConfig::destroy()
{
  group_configs_.destroy();
}

const ObTenantIOConfig &ObTenantIOConfig::default_instance()
{
  static ObTenantIOConfig instance;
  instance.memory_limit_ = 512L * 1024L * 1024L; // min_tenant_memory: 512M
  instance.callback_thread_count_ = 0;
  instance.unit_config_.min_iops_ = 10000;
  instance.unit_config_.max_iops_ = 50000;
  instance.unit_config_.weight_ = 10000;
  instance.unit_config_.max_net_bandwidth_ = INT64_MAX;
  instance.unit_config_.net_bandwidth_weight_ = 100;
  instance.group_config_change_ = false;
  instance.enable_io_tracer_ = false;
  instance.object_storage_io_timeout_ms_ = DEFAULT_OBJECT_STORAGE_IO_TIMEOUT_MS;
  return instance;
}

bool ObTenantIOConfig::is_valid() const
{
  bool bret = memory_limit_ > 0
              && callback_thread_count_ >= 0
              && unit_config_.is_valid();
  for (uint8_t i = (uint8_t)ObIOMode::READ; i <= (uint8_t)ObIOMode::MAX_MODE; ++i) {
    int64_t sum_min_percent = 0;
    int64_t sum_weight_percent = 0;
    ObIOMode mode = (ObIOMode)i;
    for (int64_t j = 0; j < group_configs_.count(); ++j) {
      if (group_configs_.at(j).is_valid()
          && !group_configs_.at(j).deleted_
          && !group_configs_.at(j).cleared_
          && group_configs_.at(j).mode_ == mode) {
        sum_min_percent += group_configs_.at(j).min_percent_;
        sum_weight_percent += group_configs_.at(j).weight_percent_;
      }
    }
    bret = bret && 100 >= sum_min_percent && 100 == sum_weight_percent;
  }
  return bret;
}

int ObTenantIOConfig::deep_copy(const ObTenantIOConfig &other_config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_configs_.assign(other_config.group_configs_))) {
    LOG_WARN("fail to assign group_configs", K(ret));
  }

  if (OB_SUCC(ret)) {
    memory_limit_ = other_config.memory_limit_;
    callback_thread_count_ = other_config.callback_thread_count_;
    unit_config_ = other_config.unit_config_;
    group_config_change_ = other_config.group_config_change_;
    enable_io_tracer_ = other_config.enable_io_tracer_;
    object_storage_io_timeout_ms_ = other_config.object_storage_io_timeout_ms_;
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
        tmp_group_config.group_id_ = atoi(group_idx);
        if (0 == tmp_group_config.group_id_) {
          group_configs_.at((uint8_t)ObIOMode::MAX_MODE) = tmp_group_config;
        } else if (OB_FAIL(group_configs_.push_back(tmp_group_config))) {
          LOG_WARN("push back group config failed", K(ret), K(tmp_group_config));
        }
        pos += strlen(tmp_config_str) + 1;
      }
    }
    if (OB_SUCC(ret)) {
      // decide the config of other group
      int64_t sum_min_percent = 0;
      int64_t sum_weight_percent = 0;
      for (uint8_t i = (uint8_t)ObIOMode::READ; i <= (uint8_t)ObIOMode::MAX_MODE; ++i) {
        for (int64_t j = 0; j < group_configs_.count(); ++j) {
          if (group_configs_.at(j).is_valid() && group_configs_.at(j).group_id_ != 0) {
            sum_min_percent += group_configs_.at(j).min_percent_;
            sum_weight_percent += group_configs_.at(j).weight_percent_;
          }
        }
        if (0 == group_configs_.at(i).group_id_
            && 0 == group_configs_.at(i).min_percent_
            && 0 == group_configs_.at(i).max_percent_
            && 0 == group_configs_.at(i).weight_percent_
            && sum_weight_percent < 100) {
          group_configs_.at(i).min_percent_ = 100 - sum_min_percent;
          group_configs_.at(i).max_percent_ = 100;
          group_configs_.at(i).weight_percent_ = 100 - sum_weight_percent;
        }
      }
    }
  }
  return ret;
}

int ObTenantIOConfig::add_single_group_config(const uint64_t tenant_id,
                                              const ObIOGroupKey &key,
                                              const char *group_name,
                                              int64_t min_percent,
                                              int64_t max_percent,
                                              int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_resource_manager_group(key.group_id_)) || !is_valid_tenant_id(tenant_id) ||
      min_percent < 0 || min_percent > 100 ||
      max_percent < 0 || max_percent > 100 ||
      weight_percent < 0 || weight_percent > 100 ||
      min_percent > max_percent) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(ret), K(tenant_id), K(key.group_id_), K(min_percent), K(max_percent), K(weight_percent));
  } else {
    ObTenantIOConfig::GroupConfig tmp_group_config;
    strncpy(tmp_group_config.group_name_, group_name, common::OB_MAX_RESOURCE_PLAN_NAME_LENGTH);
    tmp_group_config.min_percent_ = min_percent;
    tmp_group_config.max_percent_ = max_percent;
    tmp_group_config.weight_percent_ = weight_percent;
    tmp_group_config.group_id_ = key.group_id_;
    tmp_group_config.mode_ = key.mode_;
    if (OB_UNLIKELY(!tmp_group_config.is_valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid group config", K(ret), K(tmp_group_config));
    } else if (OB_FAIL(group_configs_.push_back(tmp_group_config))) {
      LOG_WARN("push back group config failed", K(ret), K(tmp_group_config));
    }
  }
  return ret;
}

int ObTenantIOConfig::get_group_config(const uint64_t index, int64_t &min, int64_t &max, int64_t &weight) const
{
  int ret = OB_SUCCESS;
  min = 0;
  max = 0;
  weight = 0;
  if (OB_UNLIKELY(index < 0 || index >= group_configs_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index));
  } else {
    const GroupConfig &group_config = group_configs_.at(index);
    if (group_config.mode_ == ObIOMode::MAX_MODE) {
      min = unit_config_.min_iops_ == INT64_MAX ? INT64_MAX : unit_config_.min_iops_ * group_config.min_percent_ / 100;
      max = unit_config_.max_iops_ == INT64_MAX ? INT64_MAX : unit_config_.max_iops_ * group_config.max_percent_ / 100;
      weight = unit_config_.weight_ * group_config.weight_percent_ / 100;
    } else {
      min = 0;
      max = unit_config_.max_net_bandwidth_ == INT64_MAX ? INT64_MAX : unit_config_.max_net_bandwidth_ * group_config.max_percent_ / 100;
      weight = unit_config_.net_bandwidth_weight_ * group_config.weight_percent_ / 100;
    }
  }
  return ret;
}

int64_t ObTenantIOConfig::get_callback_thread_count() const
{
  int64_t memory_benchmark = 4L * 1024L * 1024L * 1024L; //4G memory
  //Based on 4G memory, one thread will be added for each additional 4G of memory, and the maximum number of callback_thread_count is 16
  int64_t callback_thread_num = 0 == callback_thread_count_? min(16, (memory_limit_ / memory_benchmark) + 1) : callback_thread_count_;
  LOG_INFO("get callback thread by memory success", K(memory_limit_), K(callback_thread_num));
  return callback_thread_num;
}

int64_t ObTenantIOConfig::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  int64_t group_configs_cnt = 0;
  for (int64_t i = 0; i < group_configs_.count(); ++i) {
    if (group_configs_.at(i).deleted_ || group_configs_.at(i).cleared_){
    } else {
      group_configs_cnt++;
    }
  }
  J_KV(K(group_configs_cnt), K(memory_limit_), K(callback_thread_count_), K(unit_config_),
       K_(enable_io_tracer), K_(object_storage_io_timeout_ms));
  // if self invalid, print all group configs, otherwise, only print valid group configs
  const bool self_valid = is_valid();
  BUF_PRINTF(", group_configs:[");
  bool need_comma = false;
  for (int64_t i = 0; i < group_configs_.count(); ++i) {
    if (group_configs_.at(i).deleted_ || group_configs_.at(i).cleared_){
    } else {
      if (need_comma) {
        J_COMMA();
      }
      BUF_PRINTF("group_id = ");
      char group_id[8];
      snprintf(group_id, sizeof(group_id), "%ld", group_configs_.at(i).group_id_);
      J_KV(group_id, group_configs_.at(i));
      need_comma = true;
    }
  }
  BUF_PRINTF("]");
  J_OBJ_END();
  return pos;
}

/******************             IOClock              **********************/
void ObAtomIOClock::atom_update(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts)
{
  if (0 == iops_scale * iops_) {
    deadline_ts = current_ts;
  } else {
    const int64_t delta_ns = 1000L * 1000L * 1000L / (iops_scale * iops_);
    int64_t ov = ATOMIC_LOAD(&last_ns_);
    int64_t nv = 0;
    int64_t tv = max(current_ts * 1000L, ov + delta_ns);
    while (ov != (nv = ATOMIC_VCAS(&last_ns_, ov, tv))) {
      ov = nv;
      tv = max(current_ts * 1000L, ov + delta_ns);
    }
    deadline_ts = tv / 1000L;
  }
}

void ObAtomIOClock::compare_and_update(const int64_t current_ts, const double iops_scale, int64_t &deadline_ts)
{
  int64_t tmp = 0;
  atom_update(current_ts, iops_scale, tmp);
  deadline_ts = (INT64_MAX == deadline_ts) ? current_ts : max(deadline_ts, tmp);
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
    l_heap_(l_cmp_),
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
                                  int64_t &limitation_ts,
                                  int64_t &proportion_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || r_heap_.empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init yet", K(ret), K(is_inited_));
  } else {
    reservation_ts = r_heap_.top()->reservation_ts_;
    limitation_ts = l_heap_.empty() ? INT64_MAX : l_heap_.top()->limitation_ts_;
    proportion_ts = ready_heap_.empty() ? INT64_MAX : ready_heap_.top()->proportion_ts_;
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
  } else if (OB_FAIL(l_heap_.push(phy_queue))) {
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
          if (OB_NOT_NULL(req->io_result_)) {
            req->io_result_->time_log_.dequeue_ts_ = ObTimeUtility::fast_current_time();
          }
          if (tmp_phy_queue->req_list_.is_empty()) {
            tmp_phy_queue->reset_time_info();
            tmp_phy_queue->last_empty_ts_ = ObTimeUtility::fast_current_time();
          } else if (OB_NOT_NULL(req->tenant_io_mgr_.get_ptr())) {
            ObTenantIOClock *io_clock = static_cast<ObTenantIOClock *>(req->tenant_io_mgr_.get_ptr()->get_io_clock());       
            ObIORequest *next_req = tmp_phy_queue->req_list_.get_first();
            if (OB_ISNULL(next_req)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null next_req", KP(next_req));
            } else if (OB_SUCCESS != io_clock->calc_phyqueue_clock(tmp_phy_queue, *next_req)) {
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
  } else if (-1 == phy_queue->reservation_pos_
             || (-1 == phy_queue->limitation_pos_ && -1 == phy_queue->proportion_pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_queue is not in heaps of reservation, proportion and limitation",
        K(phy_queue),
        K(*this),
        K(ret),
        K(phy_queue->reservation_pos_),
        K(phy_queue->limitation_pos_),
        K(phy_queue->proportion_pos_));
  } else if (OB_FAIL(r_heap_.remove(phy_queue))) {
    LOG_WARN("remove phy queue from r heap failed", K(ret));
  } else if (FALSE_IT(phy_queue->reservation_pos_ = -1)) {
  } else if (-1 != phy_queue->limitation_pos_) {
    if (OB_FAIL(l_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from cl heap failed", K(ret));
    } else {
      phy_queue->limitation_pos_ = -1;
    }
  } else if (-1 != phy_queue->proportion_pos_) {
    if (OB_FAIL(ready_heap_.remove(phy_queue))) {
      LOG_WARN("remove phy queue from ready heap failed", K(ret));
    } else {
      phy_queue->proportion_pos_ = -1;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObMClockQueue::pop_with_ready_queue(const int64_t current_ts, ObIORequest *&req, int64_t &deadline_ts)
{
  int ret = OB_SUCCESS;
  ObPhyQueue *tmp_phy_queue = nullptr;
  req = nullptr;
  deadline_ts = l_heap_.empty() ? 0 : l_heap_.top()->limitation_ts_;
  // push more phy queue to tenant heap
  while (OB_SUCC(ret)
         && !l_heap_.empty()
         && l_heap_.top()->limitation_ts_ - POP_MORE_PHY_QUEUE_USEC <= current_ts) {
    tmp_phy_queue = l_heap_.top();
    if (OB_FAIL(l_heap_.pop())) {
      LOG_WARN("remove PhyQueue from c_limitation queue failed", K(ret));
    } else {
      tmp_phy_queue->limitation_pos_= -1;
      if (OB_FAIL(ready_heap_.push(tmp_phy_queue))) {
        LOG_WARN("push phy_queue from l_heap to ready_heap failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !ready_heap_.empty()) {
    tmp_phy_queue = ready_heap_.top();
    if (OB_FAIL(remove_from_heap(tmp_phy_queue))) {
      LOG_WARN("remove phy queue from heap failed(P schedule)", K(ret));
    } else {
      req = tmp_phy_queue->req_list_.remove_first();
      if (OB_ISNULL(req)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("req is null", K(ret), KP(req));
      } else {
        if (OB_NOT_NULL(req->io_result_)) {
          req->io_result_->time_log_.dequeue_ts_ = ObTimeUtility::fast_current_time();
        }
        LOG_DEBUG("req pop from phy queue succcess(P schedule)", KP(tmp_phy_queue), KP(req), "time_cost", ObTimeUtility::fast_current_time() - current_ts, K(ready_heap_.count()), K(current_ts));
        if (tmp_phy_queue->req_list_.is_empty()) {
          tmp_phy_queue->reset_time_info();
          tmp_phy_queue->last_empty_ts_ = ObTimeUtility::fast_current_time();
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
      }
    }
  } else {
    ret = OB_EAGAIN;
    if (!r_heap_.empty() && !r_heap_.top()->req_list_.is_empty()) {
      ObPhyQueue *next_tmp_phy_queue = r_heap_.top();
      if (OB_UNLIKELY(next_tmp_phy_queue->reservation_ts_ == INT64_MAX &&
                      next_tmp_phy_queue->limitation_ts_ == INT64_MAX &&
                      next_tmp_phy_queue->proportion_ts_== INT64_MAX)) {
        // 对应min = max = 0的极端场景
        const int64_t current_ts = ObTimeUtility::fast_current_time();
        next_tmp_phy_queue->reservation_ts_ = current_ts;
        next_tmp_phy_queue->limitation_ts_ = current_ts;
        next_tmp_phy_queue->proportion_ts_ = current_ts;
      }
      if (0 == deadline_ts) {
        deadline_ts = next_tmp_phy_queue->reservation_ts_;
      } else {
        deadline_ts = std::min(next_tmp_phy_queue->reservation_ts_, deadline_ts);
      }
    }
  }
  return ret;
}

