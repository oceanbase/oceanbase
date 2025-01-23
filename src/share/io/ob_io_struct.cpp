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

#include "share/io/ob_io_struct.h"

#include "lib/time/ob_time_utility.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/file/file_directory_utils.h"
#include "share/io/ob_io_manager.h"
#include "observer/ob_server.h"
#include "lib/ash/ob_active_session_guard.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

static int clear_io_hang_errsim;

/******************             IOConfig              **********************/

ObIOConfig::ObIOConfig()
{
  set_default_value();
}

ObIOConfig::~ObIOConfig()
{

}

const ObIOConfig &ObIOConfig::default_config()
{
  static ObIOConfig default_config;
  return default_config;
}

void ObIOConfig::set_default_value()
{
  write_failure_detect_interval_ = 60 * 1000 * 1000; // 1 min
  read_failure_black_list_interval_ = 60 * 1000 * 1000; // Cooperate with the adjustment of tolerance_time to 1min
  data_storage_warning_tolerance_time_ = 5L * 1000L * 1000L; // 5s, same as parameter seed
  data_storage_error_tolerance_time_ = 300L * 1000L * 1000L; // 300s
  disk_io_thread_count_ = 8;
  data_storage_io_timeout_ms_ = 120L * 1000L; // 120s
}

bool ObIOConfig::is_valid() const
{
  return write_failure_detect_interval_ > 0
      && read_failure_black_list_interval_ > 0
      && data_storage_warning_tolerance_time_ > 0
      && data_storage_error_tolerance_time_ >= data_storage_warning_tolerance_time_
      && disk_io_thread_count_ > 0 && disk_io_thread_count_ % 2 == 0 && disk_io_thread_count_ <= MAX_IO_THREAD_COUNT
      && data_storage_io_timeout_ms_ > 0;
}

void ObIOConfig::reset()
{
  write_failure_detect_interval_ = 0;
  read_failure_black_list_interval_ = 0;
  data_storage_warning_tolerance_time_ = 0;
  data_storage_error_tolerance_time_ = 0;
  disk_io_thread_count_ = 0;
  data_storage_io_timeout_ms_ = 0;
}

/******************             IOMemoryPool              **********************/
template<int64_t SIZE>
ObIOMemoryPool<SIZE>::ObIOMemoryPool()
  : is_inited_(false), capacity_(0), free_count_(0), allocator_(nullptr), begin_ptr_(nullptr)
{

}

template<int64_t SIZE>
ObIOMemoryPool<SIZE>::~ObIOMemoryPool()
{
  destroy();
}

template<int64_t SIZE>
int ObIOMemoryPool<SIZE>::init(const int64_t block_count, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(block_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    allocator_ = &allocator;
    capacity_ = block_count;
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(pool_.init(capacity_, allocator_))) {
    LOG_WARN("fail to init memory pool", K(ret));
  } else if (OB_ISNULL(begin_ptr_ = reinterpret_cast<char *>(allocator_->alloc(capacity_ * SIZE)))){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate block memory", K(ret));
  } else {
    char *buf = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < capacity_; ++i) {
      buf = begin_ptr_ + i * SIZE;
      if (OB_FAIL(pool_.push(buf))) {
        LOG_WARN("fail to push memory block to pool", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    free_count_ = capacity_;
    is_inited_ = true;
  } else {
    destroy();
  }
  return ret;
}

template<int64_t SIZE>
void ObIOMemoryPool<SIZE>::destroy()
{
  is_inited_ = false;
  free_count_ = 0;
  pool_.destroy();

  if (nullptr != allocator_) {
    if (nullptr != begin_ptr_) {
      allocator_->free(begin_ptr_);
      begin_ptr_ = nullptr;
    }
  }
  allocator_ = nullptr;
}

template<int64_t SIZE>
int ObIOMemoryPool<SIZE>::alloc(void *&ptr)
{
  int ret = OB_SUCCESS;
  char *ret_ptr = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(pool_.pop(ret_ptr))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to pop memory block", K(ret));
    }
  } else {
    const int64_t idx = (ret_ptr - begin_ptr_) / SIZE;
    if (OB_UNLIKELY(idx < 0 || idx >= capacity_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid block index", K(ret), K(idx), KP(ret_ptr), KP(begin_ptr_));
    } else {
      ATOMIC_DEC(&free_count_);
      ptr = ret_ptr;
    }
  }
  return ret;
}

template<int64_t SIZE>
int ObIOMemoryPool<SIZE>::free(void *ptr)
{
  int ret = OB_SUCCESS;
  const int64_t idx = ((char *) ptr - begin_ptr_) / SIZE;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (nullptr == ptr || idx < 0 || idx >= capacity_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), KP(ptr));
  } else {
    if (OB_FAIL(pool_.push((char *)ptr))) {
      LOG_WARN("fail to pop memory block", K(ret));
    } else {
      ATOMIC_INC(&free_count_);
    }
  }
  return ret;
}

template<int64_t SIZE>
bool ObIOMemoryPool<SIZE>::contain(void *ptr)
{
  bool bret = (uint64_t) ptr > 0
      && ((char *) ptr >= begin_ptr_)
      && ((char *) ptr <  begin_ptr_ + capacity_ * SIZE)
      && ((char *) ptr - begin_ptr_) % SIZE == 0;
  return bret;
}


/******************             IOAllocator              **********************/
ObIOAllocator::ObIOAllocator()
  : is_inited_(false),
    memory_limit_(0),
    block_count_(0),
    inner_allocator_()
{

}

ObIOAllocator::~ObIOAllocator()
{
  destroy();
}

int ObIOAllocator::init(const uint64_t tenant_id, const int64_t memory_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io allocator init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || memory_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(memory_limit));
  } else if (OB_FAIL(inner_allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                           ObModIds::OB_IO_CONTROL,
                                           tenant_id,
                                           memory_limit))) {
    LOG_WARN("init inner allocator failed", K(ret), K(tenant_id), K(memory_limit));
  } else if (OB_FAIL(init_macro_pool(memory_limit))) {
    LOG_WARN("init macro pool failed", K(ret), K(memory_limit));
  } else {
    memory_limit_ = memory_limit;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIOAllocator::destroy()
{
  is_inited_ = false;
  macro_pool_.destroy();
  inner_allocator_.destroy();
}

int ObIOAllocator::update_memory_limit(const int64_t memory_limit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(memory_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(memory_limit));
  } else if (memory_limit == memory_limit_) {
    // do nothing
  } else if (FALSE_IT(inner_allocator_.set_total_limit(memory_limit))) {
  } else {
    // TODO@wenqu: dynamic ajust the pool count
  }
  return ret;
}

int64_t ObIOAllocator::get_allocated_size() const
{
  return inner_allocator_.allocated();
}

void *ObIOAllocator::alloc(const int64_t size, const lib::ObMemAttr &attr)
{
  UNUSED(attr);
  return alloc(size);
}

void *ObIOAllocator::alloc(const int64_t size)
{
  int ret = OB_SUCCESS;
  void *ret_buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io allocator is not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(size));
  } else {
    // try pop from cache block pool if size equals cache block size
    if (size == macro_pool_.get_block_size()) {
      if (OB_FAIL(macro_pool_.alloc(ret_buf))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_ERROR("failed to alloc buf from fixed size pool", K(ret), K(size));
        } else {
          ret = OB_SUCCESS;
        }
      }
    }

    // get buf from normal allocator
    if (OB_SUCC(ret) && OB_ISNULL(ret_buf)) {
      if (OB_ISNULL(ret_buf = inner_allocator_.alloc(size))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate io mem block", K(ret), K(size));
      }
    }
  }
  return ret_buf;
}

void ObIOAllocator::free(void *ptr)
{
  if (macro_pool_.contain(ptr)) {
    macro_pool_.free(ptr);
  } else {
    inner_allocator_.free(ptr);
  }
}

int ObIOAllocator::calculate_pool_block_count(const int64_t memory_limit, int64_t &block_count)
{
  int ret = OB_SUCCESS;
  block_count = 0;
  if (OB_UNLIKELY(memory_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(memory_limit));
  } else {
    const double DEFAULT_MACRO_POOL_RATIO = 0.01;
    const int64_t MIN_MACRO_POOL_COUNT = 1;
    const int64_t MAX_MACRO_POOL_COUNT = OB_MAX_SYS_BKGD_THREAD_NUM * 4;
    int64_t macro_pool_count = memory_limit * DEFAULT_MACRO_POOL_RATIO / MACRO_POOL_BLOCK_SIZE;
    macro_pool_count = max(macro_pool_count, MIN_MACRO_POOL_COUNT);
    macro_pool_count = min(macro_pool_count, MAX_MACRO_POOL_COUNT);
    block_count = macro_pool_count;
  }
  return ret;
}

int ObIOAllocator::init_macro_pool(const int64_t memory_limit)
{
  int ret = OB_SUCCESS;
  int64_t block_count = 0;
  if (OB_FAIL(calculate_pool_block_count(memory_limit, block_count))) {
    LOG_WARN("calculate pool block count failed", K(ret));
  } else if (OB_FAIL(macro_pool_.init(block_count, inner_allocator_))) {
    LOG_WARN("failed to init macro block memory pool", K(ret), K(block_count));
  } else {
    block_count_ = block_count;
    LOG_INFO("succ to init io macro pool", K(memory_limit), K(block_count));
  }
  return ret;
}

/******************             IOStat              **********************/

ObIOStat::ObIOStat()
  : io_count_(0),
    io_bytes_(0),
    io_rt_us_(0)
{

}

ObIOStat::~ObIOStat()
{

}

void ObIOStat::accumulate(const uint64_t io_count, const uint64_t io_bytes, const uint64_t io_rt_us)
{
  ATOMIC_AAF(&io_count_, io_count);
  ATOMIC_AAF(&io_bytes_, io_bytes);
  ATOMIC_AAF(&io_rt_us_, io_rt_us);
}

void ObIOStat::reset()
{
  io_count_ = 0;
  io_bytes_ = 0;
  io_rt_us_ = 0;
}

/******************             IOStatDiff              **********************/

ObIOStatDiff::ObIOStatDiff()
  : last_stat_(),
    last_ts_(0)
{
  last_ts_ = ObTimeUtility::fast_current_time();
}

ObIOStatDiff::~ObIOStatDiff()
{

}

void ObIOStatDiff::diff(const ObIOStat &io_stat, double &avg_iops, double &avg_bytes, double &avg_rt_us)
{
  const int64_t new_ts = ObTimeUtility::fast_current_time();
  const ObIOStat new_stat = io_stat; // copy to prevent accumulating
  const int64_t delta_io_count = (new_stat.io_count_ - last_stat_.io_count_);
  if (delta_io_count > 0) {
    avg_bytes = 1.0 * (new_stat.io_bytes_ - last_stat_.io_bytes_) / delta_io_count;
    avg_rt_us = 1.0 * (new_stat.io_rt_us_ - last_stat_.io_rt_us_) / delta_io_count;
  } else {
    avg_bytes = 0;
    avg_rt_us = 0;
  }
  if (last_ts_ > 0 && new_ts > last_ts_) {
    const double delta_interval_s = 1.0 * (new_ts - last_ts_) / (1000L * 1000L);
    avg_iops = 1.0 * delta_io_count / delta_interval_s;
  } else {
    avg_iops = 0;
  }
  last_stat_ = new_stat;
  last_ts_ = new_ts;
}

void ObIOStatDiff::reset()
{
  last_stat_.reset();
  last_ts_ = 0;
}

/******************        Function Group Usage      **********************/
ObIOFuncUsages::ObIOFuncUsages()
{
  int FUNC_NUM = static_cast<uint8_t>(share::ObFunctionType::MAX_FUNCTION_NUM);
  int GROUP_MODE_NUM = static_cast<uint8_t>(ObIOGroupMode::MODECNT);
  for (int i = 0; i < FUNC_NUM; ++i) {
    ObIOFuncUsage func_usage;
    func_usage.reserve(GROUP_MODE_NUM);
    for (int j = 0; j < GROUP_MODE_NUM; ++j) {
      func_usage.push_back(ObIOFuncUsageByMode());
    }
    func_usages_.push_back(func_usage);
  }
}

int ObIOFuncUsages::accumulate(ObIORequest &req) {
  int ret = OB_SUCCESS;
  int64_t io_offset = 0;
  int64_t prepare_delay = 0;
  int64_t schedule_delay = 0;
  int64_t submit_delay = 0;
  int64_t device_delay = 0;
  int64_t total_delay = 0;
  uint64_t idx = 0;
  ObIOGroupMode mode = ObIOGroupMode::MODECNT;
  if (OB_FAIL(req.cal_delay_us(
                 prepare_delay, schedule_delay, submit_delay, device_delay, total_delay))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("failed to cal delay", K(ret));
  } else {
    idx = static_cast<uint8_t>(req.get_flag().get_func_type());
    mode = req.get_group_mode();
    if (idx < 0 || idx >= func_usages_.count()) {
      LOG_ERROR("invalid io usage index", K(idx), K(func_usages_.count()));
    } else if (mode == ObIOGroupMode::MODECNT) {
      LOG_ERROR("invalid io usage mode", K(idx), K(mode), K(func_usages_.count()));
    } else {
      func_usages_.at(idx).at(static_cast<uint8_t>(mode)).inc(req.get_data_size(), prepare_delay, schedule_delay, submit_delay, device_delay, total_delay);
    }
  }
  return ret;
}

/******************             IOUsage              **********************/
ObIOUsage::ObIOUsage()
  : group_throttled_time_us_(),
    io_stats_(),
    io_estimators_(),
    group_avg_iops_(),
    group_avg_byte_(),
    group_avg_rt_us_(),
    group_num_(0),
    doing_request_count_()
{

}

ObIOUsage::~ObIOUsage()
{
  lock_.destroy();
}

int ObIOUsage::init(const uint64_t tenant_id, const int64_t group_num)
{
  int ret =OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  //push other group into array
  } else if (OB_FAIL(refresh_group_num(group_num))) {
    LOG_WARN("refresh io usage array failed", K(ret), K(group_num));
  } else if (io_stats_.count() != group_num_ ||
             io_estimators_.count() != group_num_ ||
             group_avg_iops_.count() != group_num_ ||
             group_avg_byte_.count() != group_num_ ||
             group_avg_rt_us_.count() != group_num_ ||
             group_throttled_time_us_.count() != group_num_ ||
             doing_request_count_.count() != group_num_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init io usage failed", K(group_num_));
  } else if (OB_FAIL(lock_.init(lib::ObMemAttr(tenant_id, "IOUsage")))) {
    LOG_WARN("init lock failed", K(ret));
  }
  return ret;
}

int ObIOUsage::refresh_group_num(const int64_t group_num)
{
  int ret = OB_SUCCESS;
  ObQSyncLockWriteGuard guard(lock_);
  if (group_num < 0) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group num", K(ret), K(group_num));
  } else if (OB_FAIL(io_stats_.reserve(group_num + 1)) ||
             OB_FAIL(io_estimators_.reserve(group_num + 1)) ||
             OB_FAIL(group_avg_iops_.reserve(group_num + 1)) ||
             OB_FAIL(group_avg_byte_.reserve(group_num + 1)) ||
             OB_FAIL(group_avg_rt_us_.reserve(group_num + 1)) ||
             OB_FAIL(group_throttled_time_us_.reserve(group_num + 1)) ||
             OB_FAIL(doing_request_count_.reserve(group_num + 1))) {
    LOG_WARN("reserver group failed", K(ret), K(group_num));
  } else {
    for (int64_t i = group_num_; OB_SUCC(ret) && i < group_num + 1; ++i) {
      ObSEArray<ObIOStat, GROUP_START_NUM> cur_stat_array;
      ObSEArray<ObIOStatDiff, GROUP_START_NUM> cur_estimators_array;
      ObSEArray<double, GROUP_START_NUM> cur_avg_iops;
      ObSEArray<double, GROUP_START_NUM> cur_avg_byte;
      ObSEArray<double, GROUP_START_NUM> cur_avg_rt_us;
      ObSEArray<int64_t, GROUP_START_NUM> cur_throttled_time_us;

      if (OB_FAIL(cur_stat_array.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_estimators_array.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_avg_iops.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_avg_byte.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_avg_rt_us.reserve(static_cast<int>(ObIOMode::MAX_MODE)))) {
        LOG_WARN("reserver group failed", K(ret), K(group_num));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < static_cast<int>(ObIOMode::MAX_MODE); ++j) {
          ObIOStat cur_stat;
          ObIOStatDiff cur_diff;
          if (OB_FAIL(cur_stat_array.push_back(cur_stat))) {
            LOG_WARN("push stat failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_estimators_array.push_back(cur_diff))) {
            LOG_WARN("push estimator failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_avg_iops.push_back(0))) {
            LOG_WARN("push avg_iops failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_avg_byte.push_back(0))) {
            LOG_WARN("push avg_byte failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_avg_rt_us.push_back(0))) {
            LOG_WARN("push avg_rt failed", K(ret), K(i), K(j));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(io_stats_.push_back(cur_stat_array))) {
          LOG_WARN("push stat array failed", K(ret), K(i));
        } else if (OB_FAIL(io_estimators_.push_back(cur_estimators_array))) {
          LOG_WARN("push estimator array failed", K(ret), K(i));
        } else if (OB_FAIL(group_avg_iops_.push_back(cur_avg_iops))) {
          LOG_WARN("push avg_iops array failed", K(ret), K(i));
        } else if (OB_FAIL(group_avg_byte_.push_back(cur_avg_byte))) {
          LOG_WARN("push avg_byte array failed", K(ret), K(i));
        } else if (OB_FAIL(group_avg_rt_us_.push_back(cur_avg_rt_us))) {
          LOG_WARN("push avg_rt array failed", K(ret), K(i));
        } else if (OB_FAIL(group_throttled_time_us_.push_back(0))) {
          LOG_WARN("push throttled_time_us array failed", K(ret), K(i));
        } else if (OB_FAIL(doing_request_count_.push_back(0))) {
          LOG_WARN("push group_doing_req failed", K(ret), K(i));
        } else {
          ATOMIC_INC(&group_num_);
        }
      }
    }
  }
  return ret;
}

void ObIOUsage::accumulate(ObIORequest &req)
{
  int ret = OB_ERR_UNEXPECTED;
  ObQSyncLockReadGuard guard(lock_);
  if (req.time_log_.return_ts_ > 0) {
    const int64_t device_delay = get_io_interval(req.time_log_.return_ts_, req.time_log_.submit_ts_);
    const int64_t usage_index = req.get_io_usage_index();
    const int mode_index = static_cast<int>(req.get_mode());
    if (usage_index >= io_stats_.count() || mode_index >= static_cast<int>(ObIOMode::MAX_MODE)) {
      if (REACH_TIME_INTERVAL(100000)) {
        LOG_WARN("invalid index or mode", K(usage_index), K(mode_index), K(req));
      }
    } else {
      io_stats_.at(usage_index).at(mode_index).accumulate(1, req.io_size_, device_delay);
    }
  }
}

void ObIOUsage::calculate_io_usage()
{
  ObQSyncLockReadGuard guard(lock_);
  for (int64_t i = 0; i < group_num_; ++i) {
    for (int64_t j = 0; j < static_cast<int>(ObIOMode::MAX_MODE); ++j) {
      ObIOStatDiff &cur_io_estimator = io_estimators_.at(i).at(j);
      ObIOStat &cur_io_stat = io_stats_.at(i).at(j);
      cur_io_estimator.diff(cur_io_stat,
                            group_avg_iops_.at(i).at(j),
                            group_avg_byte_.at(i).at(j),
                            group_avg_rt_us_.at(i).at(j));
    }
  }
}

int ObIOUsage::get_io_usage(AvgItems &avg_iops, AvgItems &avg_bytes, AvgItems &avg_rt_us)
{
  int ret = OB_SUCCESS;
  ObQSyncLockReadGuard guard(lock_);
  if (OB_FAIL(avg_iops.assign(group_avg_iops_))) {
    LOG_ERROR("fail to assign avg_iops", K(ret));
  } else if(OB_FAIL(avg_bytes.assign(group_avg_byte_))) {
    LOG_ERROR("fail to assign avg_bytes", K(ret));
  } else if (OB_FAIL(avg_rt_us.assign(group_avg_rt_us_))) {
    LOG_ERROR("fail to assign avg_rt_us", K(ret));
  }
  return ret;
}

void ObIOUsage::record_request_start(ObIORequest &req)
{
  ObQSyncLockReadGuard guard(lock_);
  if (req.get_io_usage_index() < doing_request_count_.count()) {
    ATOMIC_INC(&doing_request_count_.at(req.get_io_usage_index()));
  }
}

void ObIOUsage::record_request_finish(ObIORequest &req)
{
  ObQSyncLockReadGuard guard(lock_);
  if (req.get_io_usage_index() < doing_request_count_.count()) {
    ATOMIC_DEC(&doing_request_count_.at(req.get_io_usage_index()));
  }
}

bool ObIOUsage::is_request_doing(const int64_t index) const
{
  bool ret = false;
  ObQSyncLockReadGuard guard(lock_);
  if (index < doing_request_count_.count()) {
    ret = (ATOMIC_LOAD(&doing_request_count_.at(index)) > 0);
  }
  return ret;
}

int64_t ObIOUsage::get_io_usage_num() const
{
  return ATOMIC_LOAD(&group_num_);
}

int64_t ObIOUsage::to_string(char* buf, const int64_t buf_len) const
{
  ObQSyncLockReadGuard guard(lock_);
  int64_t pos = 0;
  J_OBJ_START();
  BUF_PRINTF("doing_request_count:[");
  bool need_comma = false;
  for (int64_t i = 0; i < group_num_; ++i) {
    if (need_comma) {
      J_COMMA();
    }
    char ret[8];
    snprintf(ret, sizeof(ret), "%ld", i);
    J_KV(ret, doing_request_count_.at(i));
    need_comma = true;
  }
  BUF_PRINTF("]");
  J_OBJ_END();
  return pos;
}

/******************             ObSysIOUsage              **********************/
ObSysIOUsage::ObSysIOUsage()
  : io_stats_(),
    io_estimators_(),
    group_avg_iops_(),
    group_avg_byte_(),
    group_avg_rt_us_()
{

}

ObSysIOUsage::~ObSysIOUsage()
{

}

int ObSysIOUsage::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_stats_.reserve(SYS_MODULE_CNT)) ||
             OB_FAIL(io_estimators_.reserve(SYS_MODULE_CNT)) ||
             OB_FAIL(group_avg_iops_.reserve(SYS_MODULE_CNT)) ||
             OB_FAIL(group_avg_byte_.reserve(SYS_MODULE_CNT)) ||
             OB_FAIL(group_avg_rt_us_.reserve(SYS_MODULE_CNT))) {
    LOG_WARN("reserver group failed", K(ret), K(SYS_MODULE_CNT));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < SYS_MODULE_CNT; ++i) {
      ObSEArray<ObIOStat, SYS_MODULE_CNT> cur_stat_array;
      ObSEArray<ObIOStatDiff, SYS_MODULE_CNT> cur_estimators_array;
      ObSEArray<double, SYS_MODULE_CNT> cur_avg_iops;
      ObSEArray<double, SYS_MODULE_CNT> cur_avg_byte;
      ObSEArray<double, SYS_MODULE_CNT> cur_avg_rt_us;

      if (OB_FAIL(cur_stat_array.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_estimators_array.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_avg_iops.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_avg_byte.reserve(static_cast<int>(ObIOMode::MAX_MODE))) ||
          OB_FAIL(cur_avg_rt_us.reserve(static_cast<int>(ObIOMode::MAX_MODE)))) {
        LOG_WARN("reserver group failed", K(ret), K(SYS_MODULE_CNT));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < static_cast<int>(ObIOMode::MAX_MODE); ++j) {
          ObIOStat cur_stat;
          ObIOStatDiff cur_diff;
          if (OB_FAIL(cur_stat_array.push_back(cur_stat))) {
            LOG_WARN("push stat failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_estimators_array.push_back(cur_diff))) {
            LOG_WARN("push estimator failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_avg_iops.push_back(0))) {
            LOG_WARN("push avg_iops failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_avg_byte.push_back(0))) {
            LOG_WARN("push avg_byte failed", K(ret), K(i), K(j));
          } else if (OB_FAIL(cur_avg_rt_us.push_back(0))) {
            LOG_WARN("push avg_rt failed", K(ret), K(i), K(j));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(io_stats_.push_back(cur_stat_array))) {
          LOG_WARN("push stat array failed", K(ret), K(i));
        } else if (OB_FAIL(io_estimators_.push_back(cur_estimators_array))) {
          LOG_WARN("push estimator array failed", K(ret), K(i));
        } else if (OB_FAIL(group_avg_iops_.push_back(cur_avg_iops))) {
          LOG_WARN("push avg_iops array failed", K(ret), K(i));
        } else if (OB_FAIL(group_avg_byte_.push_back(cur_avg_byte))) {
          LOG_WARN("push avg_byte array failed", K(ret), K(i));
        } else if (OB_FAIL(group_avg_rt_us_.push_back(cur_avg_rt_us))) {
          LOG_WARN("push avg_rt array failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

void ObSysIOUsage::accumulate(ObIORequest &req)
{
  if (OB_UNLIKELY(!req.is_sys_module())) {
    // ignore
  } else if (req.time_log_.return_ts_ > 0) {
    const int64_t idx = req.get_sys_module_id() - SYS_MODULE_START_ID;
    const int64_t device_delay = get_io_interval(req.time_log_.return_ts_, req.time_log_.submit_ts_);
    io_stats_.at(idx).at(static_cast<int>(req.get_mode()))
      .accumulate(1, req.io_size_, device_delay);
  }
}

void ObSysIOUsage::calculate_io_usage()
{
  for (int64_t i = 0; i < SYS_MODULE_CNT; ++i) {
    for (int64_t j = 0; j < static_cast<int>(ObIOMode::MAX_MODE); ++j) {
      ObIOStatDiff &cur_io_estimator = io_estimators_.at(i).at(j);
      ObIOStat &cur_io_stat = io_stats_.at(i).at(j);
      cur_io_estimator.diff(cur_io_stat,
                            group_avg_iops_.at(i).at(j),
                            group_avg_byte_.at(i).at(j),
                            group_avg_rt_us_.at(i).at(j));
    }
  }
}

int ObSysIOUsage::get_io_usage(SysAvgItems &avg_iops, SysAvgItems &avg_bytes, SysAvgItems &avg_rt_us)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(avg_iops.assign(group_avg_iops_))) {
    LOG_ERROR("fail to assign avg_iops", K(ret));
  } else if(OB_FAIL(avg_bytes.assign(group_avg_byte_))) {
    LOG_ERROR("fail to assign avg_bytes", K(ret));
  } else if (OB_FAIL(avg_rt_us.assign(group_avg_rt_us_))) {
    LOG_ERROR("fail to assign avg_rt_us", K(ret));
  }
  return ret;
}

int ObIOGroupUsage::calc(double &avg_size, double &avg_iops, int64_t &avg_bw,
    int64_t &avg_prepare_delay, int64_t &avg_schedule_delay, int64_t &avg_submit_delay, int64_t &avg_device_delay,
    int64_t &avg_total_delay)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::fast_current_time();
  int64_t last_ts = ATOMIC_LOAD(&last_ts_);
  if (0 != last_ts && now - last_ts > 0 && ATOMIC_BCAS(&last_ts_, last_ts, 0)) {
    int64_t size = 0;
    int64_t io_count = 0;
    const int64_t diff = now - last_ts;
    io_count = ATOMIC_SET(&io_count_, 0);
    size = ATOMIC_SET(&size_, 0);
    ATOMIC_STORE(&last_io_ps_record_, io_count * 1000L * 1000L / diff);
    ATOMIC_STORE(&last_io_bw_record_, size * 1000L * 1000L / diff);
    ATOMIC_STORE(&last_ts_, now);
    if (io_count != 0) {
      avg_size = static_cast<double>(size) / io_count;
      avg_prepare_delay = total_prepare_delay_ / io_count;
      avg_schedule_delay = total_schedule_delay_ / io_count;
      avg_submit_delay = total_submit_delay_ / io_count;
      avg_device_delay = total_device_delay_ / io_count;
      avg_total_delay = total_total_delay_ / io_count;
    }
    avg_iops = static_cast<double>(io_count * 1000L * 1000L) / diff;
    avg_bw = size * 1000L * 1000L / diff;
    clear();
  }
  return ret;
}


/******************             CpuUsage              **********************/
ObCpuUsage::ObCpuUsage()
  : last_usage_(),
    last_ts_(0)
{
  MEMSET(&last_usage_, 0, sizeof(last_usage_));
}

ObCpuUsage::~ObCpuUsage()
{

}

void ObCpuUsage::get_cpu_usage(double &avg_usage_percentage)
{
  const int64_t new_ts = ObTimeUtility::fast_current_time();
  struct rusage new_usage;
  int sys_errno = 0;
  if (0 != (sys_errno = getrusage(RUSAGE_SELF, &new_usage))) {
    if (REACH_TIME_INTERVAL(1000L * 1000L * 10)) {
      LOG_WARN_RET(OB_ERR_SYS, "get cpu usage failed", K(sys_errno));
    }
  } else {
    if (last_ts_ > 0 && new_ts > last_ts_) {
      const int64_t sched_period_us = new_ts - last_ts_;
      const int64_t cpu_time_us = (new_usage.ru_utime.tv_sec - last_usage_.ru_utime.tv_sec) * 1000000L
                             + (new_usage.ru_utime.tv_usec - last_usage_.ru_utime.tv_usec)
                             + (new_usage.ru_stime.tv_sec - last_usage_.ru_stime.tv_sec) * 1000000L
                             + (new_usage.ru_stime.tv_usec - last_usage_.ru_stime.tv_usec);
      avg_usage_percentage = 1.0 * cpu_time_us / sched_period_us * 100;
    } else {
      avg_usage_percentage = 0;
    }
    last_usage_ = new_usage;
    last_ts_ = new_ts;
  }
}

void ObCpuUsage::reset()
{
  MEMSET(&last_usage_, 0, sizeof(last_usage_));
  last_ts_ = 0;
}

/******************             IOTuner              **********************/
ObIOTuner::ObIOTuner(ObIOScheduler &io_scheduler)
  : is_inited_(false), cpu_usage_(), io_scheduler_(io_scheduler)
{

}

ObIOTuner::~ObIOTuner()
{
  destroy();
}

int ObIOTuner::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::IO_TUNING, *this))) {
    LOG_WARN("start io scheduler failed", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObIOTuner::send_detect_task()
{
  int ret = OB_SUCCESS;
  ObArray<MacroBlockId> macro_ids;
  if (OB_FAIL(OB_SERVER_BLOCK_MGR.get_all_macro_ids(macro_ids))) {
    LOG_WARN("fail to get macro ids", K(ret) ,K(macro_ids));
  } else if (OB_UNLIKELY(0 == macro_ids.count())) {
    // skip
  } else {
    MacroBlockId &rand_id = macro_ids.at(ObRandom::rand(0, macro_ids.count() - 1));
    if (OB_FAIL(OB_IO_MANAGER.get_device_health_detector().record_timing_task(rand_id.first_id(), rand_id.second_id()))) {
      LOG_WARN("fail to record timing task", K(ret), K(rand_id));
    }
  }
  return ret;
}

void ObIOTuner::stop()
{
  TG_STOP(lib::TGDefIDs::IO_TUNING);
}

void ObIOTuner::wait()
{
  TG_WAIT(lib::TGDefIDs::IO_TUNING);
}

void ObIOTuner::destroy()
{
  stop();
  wait();
  is_inited_ = false;
}

void ObIOTuner::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = get_thread_idx();
  set_thread_name("IO_TUNING", thread_id);
  LOG_INFO("io tuner thread started");
  while (!has_set_stop()) {
    // print interval must <= 1s, for ensuring real_iops >= 1 in gv$ob_io_quota.
    if (REACH_TIME_INTERVAL(1000L * 1000L * 1L)) {
      print_io_status();
      print_sender_status();
      if (OB_FAIL(send_detect_task())) {
        LOG_WARN("fail to send detect task", K(ret));
      }
    }
    ob_usleep(100 * 1000, true/*is_idle_sleep*/); // 100ms
  }
  LOG_INFO("io tuner thread stopped");
}

int64_t ObIOTuner::to_string(char *buf, const int64_t len) const
{
  int64_t pos = 0;
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCCESS == tmp_ret && i < io_scheduler_.senders_.count(); ++i) {
    ObIOSender *sender = io_scheduler_.senders_.at(i);
    if (OB_ISNULL(sender)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN_RET(tmp_ret, "io sender is null", K(i));
    } else {
      int64_t reservation_ts = 0;
      int64_t group_limitation_ts = 0;
      int64_t tenant_limitation_ts = 0;
      int64_t proportion_ts = 0;

      tmp_ret = sender->get_sender_info(reservation_ts, group_limitation_ts, tenant_limitation_ts, proportion_ts);
      if (OB_NOT_INIT != tmp_ret) {
        databuff_printf(buf,
            len,
            pos,
            "send_index: %ld, req_count: %ld, reservation_ts: %ld, group_limitation_ts: %ld, tenant_limitation_ts: "
            "%ld, proportion_ts: %ld; ",
            sender->sender_index_,
            sender->get_queue_count(),
            reservation_ts,
            group_limitation_ts,
            tenant_limitation_ts,
            proportion_ts);
      }
    }
  }
  return pos;
}

void ObIOTuner::print_sender_status()
{
  LOG_INFO("[IO STATUS SENDER]", K(*this));
}
void ObIOTuner::print_io_status()
{
  int ret = OB_SUCCESS;
  ObVector<uint64_t> tenant_ids;
  if (OB_NOT_NULL(GCTX.omt_)) {
    GCTX.omt_->get_tenant_ids(tenant_ids);
  }
  if (tenant_ids.size() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (is_virtual_tenant_id(cur_tenant_id)) {
        // do nothing
      } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        tenant_holder.get_ptr()->print_io_status();
      }
    }
  }
}

/******************             ObIOGroupQueues              **********************/
ObIOGroupQueues::ObIOGroupQueues(ObIAllocator &allocator)
  : is_inited_(false),
    allocator_(allocator),
    group_phy_queues_(),
    other_phy_queue_()
{

}

ObIOGroupQueues::~ObIOGroupQueues()
{
  destroy();
}

int ObIOGroupQueues::init(const int64_t group_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("phy queue init twice", K(ret), K(is_inited_));
  } else if (group_num < 0) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group num", K(ret), K(group_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < group_num; ++i) {
      void *buf = nullptr;
      ObPhyQueue *tmp_phyqueue = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObPhyQueue)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(tmp_phyqueue = new (buf) ObPhyQueue())) {
      } else if (OB_FAIL(tmp_phyqueue->init(i))) {
        LOG_WARN("init io phy_queue failed", K(ret), K(i), K(*tmp_phyqueue));
      } else if (OB_FAIL(group_phy_queues_.push_back(tmp_phyqueue))) {
        LOG_WARN("push back io sender failed", K(ret), K(i), K(*tmp_phyqueue));
      }
      if (OB_FAIL(ret) && nullptr != tmp_phyqueue) {
        tmp_phyqueue->~ObPhyQueue();
        allocator_.free(tmp_phyqueue);
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObIOGroupQueues::destroy()
{
  for (int64_t i = 0; i < group_phy_queues_.count(); ++i) {
    ObPhyQueue *tmp_phyqueue = group_phy_queues_.at(i);
    if (nullptr != tmp_phyqueue) {
      tmp_phyqueue->destroy();
      allocator_.free(tmp_phyqueue);
    }
  }
  other_phy_queue_.destroy();
  group_phy_queues_.destroy();
  is_inited_ = false;
}

/******************             IOSenderInfo              **********************/
ObSenderInfo::ObSenderInfo()
  : queuing_count_(0),
    reservation_ts_(INT_MAX64),
    group_limitation_ts_(INT_MAX64),
    tenant_limitation_ts_(INT_MAX64),
    proportion_ts_(INT_MAX64)
{

}
ObSenderInfo::~ObSenderInfo()
{

}

/******************             IOScheduleQueue              **********************/
ObIOSender::ObIOSender(ObIAllocator &allocator)
  : sender_req_count_(0),
    sender_index_(0),
    tg_id_(-1),
    is_inited_(false),
    stop_submit_(false),
    allocator_(allocator),
    io_queue_(nullptr),
    queue_cond_()
{

}

ObIOSender::~ObIOSender()
{
  destroy();
}

int ObIOSender::init(const int64_t sender_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(queue_cond_.init(ObWaitEventIds::IO_QUEUE_COND_WAIT))) {
    LOG_WARN("init queue condition failed", K(ret));
  } else if (OB_FAIL(alloc_mclock_queue(allocator_, io_queue_))) {
    LOG_WARN("alloc io queue failed", K(ret));
  } else if (OB_FAIL(io_queue_->init())) {
    LOG_WARN("init io queue failed", K(ret));
  } else if (OB_FAIL(tenant_groups_map_.create(7, "IO_GROUP_MAP"))) {
    LOG_WARN("create tenant group map failed", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::IO_SCHEDULE, tg_id_))) {
    LOG_WARN("create thread group id failed", K(ret));
  } else {
    is_inited_ = true;
    sender_req_count_ = 0;
    sender_index_ = sender_index;
    LOG_INFO("io sender init succ", KCSTRING(lbt()), K(sender_index));
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

struct DestroyGroupqueueMapFn
{
public:
  DestroyGroupqueueMapFn() {}
  int operator () (hash::HashMapPair<uint64_t, ObIOGroupQueues *> &entry) {
    int ret = OB_SUCCESS;
    if (nullptr != entry.second) {
      if (is_valid_tenant_id(entry.first)) {
        ObRefHolder<ObTenantIOManager> tenant_holder;
        if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(entry.first, tenant_holder))) {
          LOG_WARN("get tenant io manager failed", K(ret), K(entry.first));
        } else {
          entry.second->~ObIOGroupQueues();
          if (OB_NOT_NULL(tenant_holder.get_ptr()) && OB_NOT_NULL(tenant_holder.get_ptr()->get_tenant_io_allocator())) {
            tenant_holder.get_ptr()->get_tenant_io_allocator()->free(entry.second);
          }
        }
      }
    }
    return ret;
  }
};

void ObIOSender::stop()
{
  stop_submit();
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
  }
}

void ObIOSender::wait()
{
  if (tg_id_ >= 0) {
    TG_WAIT(tg_id_);
  }
}

void ObIOSender::destroy()
{
  // wait flying request
  stop_submit();
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  DestroyGroupqueueMapFn destry_groupqueue_map_fn;
  tenant_groups_map_.foreach_refactored(destry_groupqueue_map_fn);
  tenant_groups_map_.destroy();
  queue_cond_.destroy();
  if (nullptr != io_queue_) {
    io_queue_->destroy();
    allocator_.free(io_queue_);
    io_queue_ = nullptr;
  }
  is_inited_ = false;
  stop_submit_ = false;
  sender_req_count_ = 0;
  sender_index_ = 0;
  LOG_INFO("io sender destroyed", KCSTRING(lbt()));
}

int ObIOSender::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_WARN("start sender thread failed", K(ret), K(tg_id_));
  }
  return ret;
}

void ObIOSender::stop_submit()
{
  stop_submit_ = true;
}

void ObIOSender::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = sender_index_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    set_thread_name("IO_SCHEDULE", thread_id);
    LOG_INFO("io schedule thread started", K(thread_id));
    while (!has_set_stop() && !stop_submit_) {
      pop_and_submit();
    }
    LOG_INFO("io schedule thread stopped", K(thread_id));
  }
}

int ObIOSender::alloc_mclock_queue(ObIAllocator &allocator, ObMClockQueue *&io_queue)
{
  int ret = OB_SUCCESS;
  io_queue = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMClockQueue)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    io_queue = new (buf) ObMClockQueue;
  }
  return ret;
}

int ObIOSender::enqueue_request(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  ObIORequest *tmp_req = &req;
  ObPhyQueue *tmp_phy_queue = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else if (OB_NOT_NULL(req.tenant_io_mgr_.get_ptr())) {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      req.sender_index_ = sender_index_;
      ObIOGroupQueues *io_group_queues = nullptr;
      if (OB_FAIL(tenant_groups_map_.get_refactored(tmp_req->io_info_.tenant_id_, io_group_queues))) {
        LOG_WARN("get_refactored tenant_map failed", K(ret), K(req));
      } else {
        uint64_t index = INT_MAX64;
        const int64_t group_id = tmp_req->get_resource_group_id();
        if (!is_resource_manager_group(group_id)) { //other
          tmp_phy_queue = &(io_group_queues->other_phy_queue_);
        } else if (OB_FAIL(req.tenant_io_mgr_.get_ptr()->get_group_index(group_id, index))) {
          // 防止删除group、新建group等情况发生时在途req无法找到对应的group
          if (ret == OB_HASH_NOT_EXIST || ret == OB_STATE_NOT_MATCH) {
            ret = OB_SUCCESS;
            tmp_phy_queue = &(io_group_queues->other_phy_queue_);
          } else {
            LOG_WARN("get group index failed", K(ret), K(group_id), K(index));
          }
        } else if (index < 0 || index >= io_group_queues->group_phy_queues_.count()) {
          tmp_phy_queue = &(io_group_queues->other_phy_queue_);
        } else {
          tmp_phy_queue = io_group_queues->group_phy_queues_.at(index);
          if (OB_UNLIKELY(tmp_phy_queue->is_stop_accept())) {
            ret = OB_STATE_NOT_MATCH;
            LOG_WARN("runner is quit, stop accept new req", K(ret), K(req));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (tmp_phy_queue->req_list_.is_empty()) {
          //new request
          if (OB_FAIL(io_queue_->remove_from_heap(tmp_phy_queue))) {
            LOG_WARN("remove phy queue from heap failed", K(ret), K(index));
          } else {
            req.inc_ref("phyqueue_inc"); //ref for phy_queue
            if (OB_UNLIKELY(!tmp_phy_queue->req_list_.add_last(tmp_req))) {
              ret = OB_ERR_UNEXPECTED;
              req.dec_ref("phyqueue_dec"); //ref for phy_queue
              tmp_phy_queue->reset_time_info();
              LOG_WARN("push new req into phy queue failed", K(ret));
            } else {
              ATOMIC_INC(&sender_req_count_);
              req.time_log_.enqueue_ts_ = ObTimeUtility::fast_current_time();
              //calc ts_
              if (OB_NOT_NULL(req.tenant_io_mgr_.get_ptr())) {
                ObTenantIOClock *io_clock = static_cast<ObTenantIOClock *>(req.tenant_io_mgr_.get_ptr()->get_io_clock());
                //phy_queue from idle to active
                // TODO（QILU):不要每次是新请求就调一次，因为可能就是发的很快，需要增加一个判断机制空闲了一段时间才触发
                int tmp_ret = OB_SUCCESS;
                tmp_ret = io_clock->try_sync_tenant_clock(io_clock);
                if (OB_FAIL(io_clock->calc_phyqueue_clock(tmp_phy_queue, req))) {
                  LOG_WARN("calc phyqueue clock failed", K(ret), K(tmp_phy_queue->queue_index_));
                } else if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
                  LOG_WARN("sync tenant clock failed", K(tmp_ret));
                }
              }
            }
            int tmp_ret = io_queue_->push_phyqueue(tmp_phy_queue);
            if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
              LOG_WARN("re_into heap failed", K(tmp_ret));
              abort();
            }
          }
        } else {
          //not new req, into phy_queue and line up
          req.inc_ref("phyqueue_inc"); //ref for phy_queue
          if (OB_UNLIKELY(!tmp_phy_queue->req_list_.add_last(tmp_req))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("req line up failed", K(req));
            req.dec_ref("phyqueue_dec"); //ref for phy_queue
          } else {
            ATOMIC_INC(&sender_req_count_);
            req.time_log_.enqueue_ts_ = ObTimeUtility::fast_current_time();
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(queue_cond_.signal())) {
            LOG_ERROR("signal queue condition failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObIOSender::enqueue_phy_queue(ObPhyQueue &phyqueue)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      if (OB_FAIL(io_queue_->push_phyqueue(&phyqueue))) {
        LOG_WARN("push phyqueue into queue failed", K(ret));
      } else {
        if (OB_FAIL(queue_cond_.signal())) {
          LOG_ERROR("signal queue condition failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIOSender::dequeue_request(ObIORequest *&req)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      int64_t queue_deadline_ts = 0;
      ret = io_queue_->pop_phyqueue(req, queue_deadline_ts);
      if (OB_SUCC(ret)) {
        ATOMIC_DEC(&sender_req_count_);
      } else if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
        const int64_t timeout_us = calc_wait_timeout(queue_deadline_ts);
        int tmp_ret = OB_SUCCESS;
        ObBKGDSessInActiveGuard inactive_guard;
        if (timeout_us > 0 && OB_SUCCESS != (tmp_ret = queue_cond_.wait_us(timeout_us))) {
          if (OB_TIMEOUT == tmp_ret) {
            // normal case, ignore
          } else {
            LOG_ERROR("fail to wait queue condition", K(tmp_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObIOSender::update_group_queue(const uint64_t tenant_id, const int64_t group_num)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || group_num < 0)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id), K(group_num));
  } else {
    ObIOGroupQueues *io_group_queues = nullptr;
    if (OB_FAIL(tenant_groups_map_.get_refactored(tenant_id, io_group_queues))) {
      LOG_WARN("get_refactored form tenant_group_map failed", K(ret), K(tenant_id));
    } else if (OB_ISNULL(io_group_queues)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io group queues is null", K(ret), KP(io_group_queues));
    } else if (OB_UNLIKELY(!io_group_queues->is_inited_)) {
      LOG_WARN("io_group_queues not init", K(ret), K(*io_group_queues));
    } else if (io_group_queues->group_phy_queues_.count() > group_num || group_num < 0) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("invalid group num", K(ret), K(group_num));
    } else if (io_group_queues->group_phy_queues_.count() == group_num) {
      // do nothing
    } else if (io_group_queues->group_phy_queues_.count() < group_num) {
      // add phyqueue
      int64_t cur_num = io_group_queues->group_phy_queues_.count();
      for (int64_t i = cur_num; OB_SUCC(ret) && i < group_num; ++i) {
        void *buf = nullptr;
        ObPhyQueue *tmp_phyqueue = nullptr;
        if (OB_ISNULL(buf = io_group_queues->allocator_.alloc(sizeof(ObPhyQueue)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (FALSE_IT(tmp_phyqueue = new (buf) ObPhyQueue())) {
        } else if (OB_FAIL(tmp_phyqueue->init(i))) {
          LOG_WARN("init io phy_queue failed", K(ret), K(i), K(*tmp_phyqueue));
        } else if (OB_FAIL(io_group_queues->group_phy_queues_.push_back(tmp_phyqueue))) {
          LOG_WARN("push back io sender failed", K(ret), K(i), K(*tmp_phyqueue));
        } else if (OB_FAIL(enqueue_phy_queue(*tmp_phyqueue))) {
          LOG_WARN("new queue into heap failed", K(ret));
        } else {
          LOG_INFO("add phy queue success", K(tenant_id), K(cur_num), K(group_num));
        }
        if (OB_FAIL(ret) && nullptr != tmp_phyqueue) {
          tmp_phyqueue->~ObPhyQueue();
          io_group_queues->allocator_.free(tmp_phyqueue);
        }
      }
    }
  }
  return ret;
}

int ObIOSender::remove_group_queues(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      ObIOGroupQueues *io_group_queues = nullptr;
      if (OB_FAIL(tenant_groups_map_.erase_refactored(tenant_id, &io_group_queues))) {
        LOG_WARN("erase phy_queues failed", K(ret), K(tenant_id));
      } else if (nullptr != io_group_queues) {
        for (int64_t j = 0; OB_SUCC(ret) && j < io_group_queues->group_phy_queues_.count(); ++j) {
          ObPhyQueue *tmp_phy_queue = io_group_queues->group_phy_queues_.at(j);
          if (OB_FAIL(io_queue_->remove_from_heap(tmp_phy_queue))) {
            LOG_WARN("remove phy queue from heap failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if(OB_FAIL(io_queue_->remove_from_heap(&(io_group_queues->other_phy_queue_)))) {
            LOG_WARN("remove other phy queue from heap failed", K(ret));
          } else {
            ObRefHolder<ObTenantIOManager> tenant_holder;
            if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
              LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
            } else {
              io_group_queues->~ObIOGroupQueues();
              tenant_holder.get_ptr()->get_tenant_io_allocator()->free(io_group_queues);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObIOSender::stop_phy_queue(const uint64_t tenant_id, const uint64_t index)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || index < 0 || INT64_MAX == index)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id), K(index));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      ObIOGroupQueues *io_group_queues = nullptr;
      if (OB_FAIL(tenant_groups_map_.get_refactored(tenant_id, io_group_queues))) {
        LOG_WARN("get_refactored tenant_map failed", K(ret), K(tenant_id));
      } else if (nullptr != io_group_queues && index < io_group_queues->group_phy_queues_.count()) {
        io_group_queues->group_phy_queues_.at(index)->set_stop_accept();
        //TODO (QILU) tuner regularly checks whether the memory can be released
      }
    }
  }
  return ret;
}
int ObIOSender::notify()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else if (OB_FAIL(queue_cond_.signal())) {
      LOG_ERROR("signal queue condition failed", K(ret));
    }
  }
  return ret;
}

int64_t ObIOSender::get_queue_count() const
{
  return OB_ISNULL(io_queue_) ?  0 : ATOMIC_LOAD(&sender_req_count_);
}

int ObIOSender::get_sender_info(int64_t &reservation_ts,
                                int64_t &group_limitation_ts,
                                int64_t &tenant_limitation_ts,
                                int64_t &proportion_ts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      ret = io_queue_->get_time_info(reservation_ts, group_limitation_ts, tenant_limitation_ts, proportion_ts);
    }
  }
  return ret;
}

int ObIOSender::get_sender_status(const uint64_t tenant_id, const uint64_t index, ObSenderInfo &sender_info)
{
  int ret = OB_SUCCESS;
  ObIOGroupQueues *io_group_queues = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || index < 0)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid index", K(ret), K(index));
  } else {
    ObThreadCondGuard cond_guard(queue_cond_);
    if (OB_FAIL(cond_guard.get_ret())) {
      LOG_ERROR("guard queue condition failed", K(ret));
    } else {
      if (OB_FAIL(tenant_groups_map_.get_refactored(tenant_id, io_group_queues))) {
        LOG_WARN("get io_group_queues from map failed", K(ret), K(tenant_id));
      } else if (OB_UNLIKELY((index >= io_group_queues->group_phy_queues_.count() && INT64_MAX != index))) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("invalid index", K(ret), K(index));
      } else {
        ObPhyQueue *tmp_phy_queue = index == INT64_MAX ?
                   &(io_group_queues->other_phy_queue_) : io_group_queues->group_phy_queues_.at(index);
        sender_info.queuing_count_ = tmp_phy_queue->req_list_.get_size();
        sender_info.reservation_ts_ = tmp_phy_queue->reservation_ts_;
        sender_info.group_limitation_ts_ = tmp_phy_queue->group_limitation_ts_;
        sender_info.tenant_limitation_ts_ = tmp_phy_queue->tenant_limitation_ts_;
        sender_info.proportion_ts_ = tmp_phy_queue->proportion_ts_;
      }
    }
  }
  return ret;
}

void ObIOSender::pop_and_submit()
{
  int ret = OB_SUCCESS;
  ObIORequest *req = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(dequeue_request(req))) {
    if (OB_EAGAIN == ret || OB_ENTRY_NOT_EXIST == ret) {
      // ignore
    } else {
      LOG_WARN("pop request from send queue failed", K(ret));
    }
  } else if (OB_ISNULL(req)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("request is null", K(ret));
  } else {
    RequestHolder req_holder(req);
    req->sender_ = this;
    bool is_retry = false;
    ObTraceIDGuard trace_guard(req->trace_id_);
    if (req->is_canceled_) {
      ret = OB_CANCELED;
    } else {
      if (OB_FAIL(submit(*req))) {
        if (OB_EAGAIN == ret) {
          LOG_INFO("IOChannel submit failed, re_submit req", K(ret), K(*req));
          req->dec_ref("phyqueue_dec"); // ref for io queue
          ObIORequest &re_req = *req;
          if (OB_FAIL(enqueue_request(re_req))) {
            LOG_WARN("retry push request to queue failed", K(ret), K(re_req));
          } else {
            is_retry = true;
          }
        } else if (OB_CANCELED != ret) {
          LOG_WARN("submit io request failed", K(ret));
        }
      }
    }
    // the request has only three result here: submitted, failed, retrying
    if (OB_FAIL(ret)) {
      req->finish(ret);
    }
    if (OB_LIKELY(!is_retry)) {
      req->dec_ref("phyqueue_dec"); // ref for io queue
    }
  }
}

int64_t ObIOSender::calc_wait_timeout(const int64_t queue_deadline)
{
  static const int64_t DEFAULT_WAIT_US = 1000L * 1000L; // 1s
  const int64_t current_time = ObTimeUtility::fast_current_time();
  int64_t wait_us = 0;
  if (queue_deadline <= 0) {
    wait_us = DEFAULT_WAIT_US;
  } else if (queue_deadline <= current_time) {
    wait_us = 0;
  } else if (queue_deadline > current_time){
    wait_us = queue_deadline - current_time;
    if (wait_us > DEFAULT_WAIT_US) {
      wait_us = DEFAULT_WAIT_US;
    }
  }
  return wait_us;
}

int ObIOSender::submit(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  ObTimeGuard time_guard("submit_req", 100000); //100ms
  if (OB_UNLIKELY(stop_submit_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("sender stop submit", K(ret), K(stop_submit_));
  } else if (OB_FAIL(req.prepare())) {
    LOG_WARN("prepare io request failed", K(ret), K(req));
  } else if (FALSE_IT(time_guard.click("prepare_req"))) {
  } else if (OB_FAIL(OB_IO_MANAGER.get_device_channel(req.io_info_.fd_.device_handle_, device_channel))) {
    LOG_WARN("get device channel failed", K(ret), K(req));
  } else {
    // lock request condition to prevent canceling halfway
    ObThreadCondGuard guard(req.cond_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to guard master condition", K(ret));
    } else if (req.is_canceled_) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(device_channel->submit(req))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("submit io request failed", K(ret), K(req), KPC(device_channel));
      }
    } else {
      time_guard.click("device_submit");
    }
  }
  if (time_guard.get_diff() > 100000) {// 100ms
    //print req
    LOG_INFO("submit_request cost too much time", K(ret), K(time_guard), K(req));
  }
  return ret;
}


/******************             IOScheduler              **********************/

ObIOScheduler::ObIOScheduler(const ObIOConfig &io_config, ObIAllocator &allocator)
  : is_inited_(false),
    io_config_(io_config),
    allocator_(allocator),
    io_tuner_(*this),
    schedule_media_id_(0)
{
}

ObIOScheduler::~ObIOScheduler()
{
  destroy();
}

int ObIOScheduler::init(const int64_t queue_count, const int64_t schedule_media_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io scheduler init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(queue_count <= 0 || queue_count <= 0 || schedule_media_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(queue_count), K(schedule_media_id));
  } else if (OB_FAIL(io_tuner_.init())) {
    LOG_WARN("init io tuner failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < queue_count; ++i) {
      void *buf = nullptr;
      ObIOSender *tmp_sender = nullptr;
      int64_t sender_index = i + 1;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObIOSender)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(tmp_sender = new (buf) ObIOSender(allocator_))) {
      } else if (OB_FAIL(tmp_sender->init(sender_index))) {
        LOG_WARN("init io sender failed", K(ret), K(i), K(*tmp_sender));
      } else if (OB_FAIL(senders_.push_back(tmp_sender))) {
        LOG_WARN("push back io sender failed", K(ret), K(i), K(*tmp_sender));
      }
      if (OB_FAIL(ret) && nullptr != tmp_sender) {
        tmp_sender->~ObIOSender();
        allocator_.free(tmp_sender);
      }
    }
    if (OB_SUCC(ret)) {
      schedule_media_id_ = schedule_media_id;
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIOScheduler::destroy()
{
  io_tuner_.destroy();
  for (int64_t i = 0; i < senders_.count(); ++i) {
    senders_.at(i)->stop();
  }
  for (int64_t i = 0; i < senders_.count(); ++i) {
    senders_.at(i)->wait();
  }
  for (int64_t i = 0; i < senders_.count(); ++i) {
    ObIOSender *&tmp_sender = senders_.at(i);
    if (OB_NOT_NULL(tmp_sender)) {
      tmp_sender->~ObIOSender();
      allocator_.free(tmp_sender);
      tmp_sender = nullptr;
    }
  }
  senders_.destroy();
  is_inited_ = false;
}

int ObIOScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < senders_.count(); ++i) {
      ObIOSender *cur_sender = senders_.at(i);
      if (OB_FAIL(cur_sender->start())) {
        LOG_WARN("start io sender failed", K(ret), K(i), K(*cur_sender));
      }
    }
  }
  return ret;
}

void ObIOScheduler::stop()
{
  for (int64_t i = 0; i < senders_.count(); ++i) {
    senders_.at(i)->stop_submit();
  }
  io_tuner_.stop();
}

void ObIOScheduler::wait()
{
  io_tuner_.wait();
}

int ObIOScheduler::schedule_request(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  RequestHolder holder(&req);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    int64_t idx = 0;
    for (idx = 0; idx < senders_.count(); idx++) {
      if (senders_.at(idx)->get_queue_count() < SENDER_QUEUE_WATERLEVEL) {
        break;
      }
    }
    if (idx == senders_.count()) {
      idx = ObRandom::rand(0, senders_.count() - 1);
    }
    ObIOSender *sender = senders_.at(idx);
    if (req.io_info_.fd_.device_handle_->media_id_ != schedule_media_id_) {
      // direct submit
      if (OB_FAIL(sender->submit(req))) {
        LOG_WARN("direct submit request failed", K(ret));
      } else {
        LOG_INFO("direct submit request success", K(req));
      }
    } else {
      if (OB_FAIL(sender->enqueue_request(req))) {
        req.finish(ObIORetCode(ret));
        LOG_WARN("enqueue request failed", K(ret), K(req));
      }
    }
  }
  return ret;
}

int ObIOScheduler::init_group_queues(const uint64_t tenant_id, const int64_t group_num, ObIOAllocator *io_allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || group_num < 0 || OB_ISNULL(io_allocator))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id), K(group_num), KP(io_allocator));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < senders_.count(); ++i) {
      ObIOSender *cur_sender = senders_.at(i);
      ObIOGroupQueues *io_group_queues = nullptr;
      void *buf_queues = nullptr;
      if (OB_ISNULL(buf_queues = io_allocator->alloc(sizeof(ObIOGroupQueues)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate phyqueues memory failed", K(ret));
      } else if (FALSE_IT(io_group_queues = new (buf_queues) ObIOGroupQueues(*io_allocator))) {
      } else if (OB_FAIL(io_group_queues->other_phy_queue_.init(INT64_MAX))) { //other group index
        LOG_WARN("init other group queue failes", K(ret));
      } else if (OB_FAIL(io_group_queues->init(group_num))) {
        LOG_WARN("init phyqueues failed", K(ret));
      } else if (OB_FAIL(cur_sender->enqueue_phy_queue(io_group_queues->other_phy_queue_))){ //other groups queue
        LOG_WARN("other phy queue into send_queue failed", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < group_num; ++j) {
          if (OB_FAIL(cur_sender->enqueue_phy_queue(*(io_group_queues->group_phy_queues_.at(j))))) {
            LOG_WARN("new phy_queue into send_queue failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(cur_sender->tenant_groups_map_.set_refactored(tenant_id, io_group_queues))) {
            LOG_WARN("init tenant group map failed", K(tenant_id), K(ret), K(i));
          }
        } else {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          }
          io_group_queues->~ObIOGroupQueues();
          io_allocator->free(io_group_queues);
        }
      }
    }
  }
  return ret;
}

int ObIOScheduler::update_group_queues(const uint64_t tenant_id, const int64_t group_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || group_num < 0)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id), K(group_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < senders_.count(); ++i) {
      ObIOSender *cur_sender = senders_.at(i);
      if (OB_FAIL(cur_sender->update_group_queue(tenant_id, group_num))) {
        LOG_WARN("serder update group queue num failed", K(ret), K(tenant_id), K(group_num));
      }
    }
  }
  return ret;
}

int ObIOScheduler::remove_phyqueues(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < senders_.count(); ++i) {
      ObIOSender *cur_sender = senders_.at(i);
      if (OB_FAIL(cur_sender->remove_group_queues(tenant_id))) {
        LOG_WARN("remove phy queue failed", K(ret), K(i), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObIOScheduler::stop_phy_queues(const uint64_t tenant_id, const int64_t index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || index < 0 || INT64_MAX == index)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id), K(index));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < senders_.count(); ++i) {
      ObIOSender *cur_sender = senders_.at(i);
      if (OB_FAIL(cur_sender->stop_phy_queue(tenant_id, index))) {
        LOG_WARN("stop phy queue failed", K(ret), K(index));
      }
    }
  }
  return ret;
}

/******************             IOChannel              **********************/
ObIOChannel::ObIOChannel()
  : is_inited_(false),
    tg_id_(-1),
    device_handle_(nullptr),
    device_channel_(nullptr)
{

}

ObIOChannel::~ObIOChannel()
{

}

int ObIOChannel::base_init(ObDeviceChannel *device_channel)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == device_channel)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_channel));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::IO_CHANNEL, tg_id_))) {
    LOG_WARN("create thread group id failed", K(ret));
  } else {
    device_channel_ = device_channel;
    device_handle_ = device_channel->device_handle_;
  }
  return ret;
}

int ObIOChannel::start_thread()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
    LOG_WARN("start channel thread failed", K(ret), K(tg_id_));
  }
  return ret;
}

void ObIOChannel::destroy_thread()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
}

int ObIOChannel::convert_sys_errno(const int system_errno)
{
  int ret = OB_IO_ERROR;
  // system_errno is a positive number.
  bool use_warn_log = false;
  switch (system_errno) {
    case EACCES:
      ret = OB_FILE_OR_DIRECTORY_PERMISSION_DENIED;
      LOG_ERROR("file or directory permission denied", K(ret), K(system_errno));
      break;
    case ENOENT:
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      LOG_ERROR("no such file or directory", K(ret), K(system_errno));
      break;
    case EEXIST:
    case ENOTEMPTY:
      ret = OB_FILE_OR_DIRECTORY_EXIST;
      LOG_ERROR("file or directory exist", K(ret), K(system_errno));
      break;
    case ETIMEDOUT:
      ret = OB_TIMEOUT;
      LOG_ERROR("io timeout", K(ret), K(system_errno));
      break;
    case EAGAIN:
      ret = OB_EAGAIN;
      LOG_WARN("io eagain", K(ret), K(system_errno));
      break;
    case ENOSPC:
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_ERROR("server out of disk space", K(ret), K(system_errno));
      break;
    case EDQUOT:
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_ERROR("server out of disk space", K(ret), K(system_errno));
      break;
    default:
      use_warn_log = true;
      break;
  }
  if (use_warn_log) {
    LOG_INFO("convert sys errno", K(ret), K(system_errno));
  } else {
    LOG_WARN("convert sys errno", K(ret), K(system_errno));
  }
  return ret;
}

void switch_check_io_hang_errsim()
{
  ATOMIC_FAA(&clear_io_hang_errsim, 1);
}

static inline int check_io_hang_errsim()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_E(EventTable::EN_IO_HANG_ERROR) OB_SUCCESS;

  STATIC_ASSERT(0 == OB_SUCCESS, "OB_SUCCESS is not 0");

  if (OB_SUCCESS != tmp_ret) {
    int errsim = -tmp_ret;
    int hang_ms = (errsim / 10) * 10;
    int errcode = (errsim % 10);
    if (0 == errcode) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_IO_ERROR;
    }
    if (0 == clear_io_hang_errsim % 2) {
      LOG_WARN("errsim: EN_IO_HANG_ERROR", K(ret), K(tmp_ret), K(hang_ms));
    } else {
      LOG_WARN("errsim: EN_IO_HANG_ERROR is ignored", K(ret), K(tmp_ret), K(hang_ms));
    }
    while (hang_ms > 0 && 0 == ATOMIC_LOAD(&clear_io_hang_errsim) % 2) {
      oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
      ObClockGenerator::msleep(10);
      hang_ms = hang_ms - 10;
    }
  }
  return ret;
}

/******************             AsyncIOChannel              **********************/
ObAsyncIOChannel::ObAsyncIOChannel()
  : io_context_(nullptr),
    io_events_(nullptr),
    polling_timeout_({0, 0}),
    submit_count_(0)
{

}

ObAsyncIOChannel::~ObAsyncIOChannel()
{
  destroy();
}

int ObAsyncIOChannel::init(ObDeviceChannel *device_channel)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(base_init(device_channel))) {
    LOG_WARN("base init failed", K(ret), KP(device_channel));
  } else if (OB_FAIL(depth_cond_.init(ObWaitEventIds::IO_CHANNEL_COND_WAIT))) {
    LOG_WARN("init thread cond failed", K(ret));
  } else if (OB_FAIL(device_handle_->io_setup(MAX_AIO_EVENT_CNT, io_context_))) {
    LOG_ERROR("io setup failed, check config aio-max-nr of operating system", K(ret), KP(io_context_));
  } else if (OB_ISNULL(io_events_ = device_handle_->alloc_io_events(MAX_AIO_EVENT_CNT))) {
    ret = OB_ERR_SYS;
    LOG_WARN("alloc io events failed", K(ret), KP(io_events_));
  } else {
    polling_timeout_.tv_sec = 0;
    polling_timeout_.tv_nsec = AIO_POLLING_TIMEOUT_NS;
    submit_count_ = 0;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObAsyncIOChannel::stop()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
  }
}

void ObAsyncIOChannel::wait()
{
  if (tg_id_ >= 0) {
    TG_WAIT(tg_id_);
  }
}

void ObAsyncIOChannel::destroy()
{
    // wait flying request
  const int64_t max_wait_ts = ObTimeUtility::fast_current_time() + 1000L * 1000L * 30L; // 30s
  while (submit_count_ > 0 && ObTimeUtility::fast_current_time() < max_wait_ts) {
    ob_usleep(1000 * 10);
  }
  if (submit_count_ > 0) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "some request have not returned from file system", K(submit_count_));
  }
  destroy_thread();
  if (nullptr != io_context_) {
    device_handle_->io_destroy(io_context_);
    io_context_ = nullptr;
  }
  if (nullptr != io_events_) {
    device_handle_->free_io_events(io_events_);
    io_events_ = nullptr;
  }
  polling_timeout_.tv_sec = 0;
  polling_timeout_.tv_nsec = 0;
  submit_count_ = 0;
  device_handle_ = nullptr;
  depth_cond_.destroy();
  is_inited_ = false;
}

void ObAsyncIOChannel::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = get_thread_idx();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    set_thread_name("IO_GETEVENT", thread_id);
    LOG_INFO("io get_events thread started", K(thread_id), K(tg_id_));
    while (!has_set_stop()) {
      get_events();
    }
    LOG_INFO("io get_events thread stopped", K(thread_id), K(tg_id_));
  }
}

static int64_t get_io_depth(const int64_t io_size)
{
  const int64_t IO_SPLIT_SIZE = 512L * 1024L; // 512KB
  return upper_align(io_size, IO_SPLIT_SIZE) / IO_SPLIT_SIZE;
}

int ObAsyncIOChannel::submit(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(device_handle_ != req.io_info_.fd_.device_handle_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(req), KP(device_handle_));
  } else if (submit_count_ >= MAX_AIO_EVENT_CNT) {
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(1000000L)) {
      LOG_WARN("too many io requests", K(ret), K(submit_count_));
    }
  } else if (device_channel_->used_io_depth_ > device_channel_->max_io_depth_) {
    ret = OB_EAGAIN;
    FLOG_INFO("reach max io depth", K(ret), K(device_channel_->used_io_depth_), K(device_channel_->max_io_depth_));
  } else {
    ATOMIC_INC(&submit_count_);
    ATOMIC_FAA(&device_channel_->used_io_depth_, get_io_depth(req.io_size_));
    req.channel_ = this;
    req.time_log_.submit_ts_ = ObTimeUtility::fast_current_time();
    req.inc_ref("os_inc"); // ref for file system
    if (OB_FAIL(device_handle_->io_submit(io_context_, req.control_block_))) {
      ATOMIC_DEC(&submit_count_);
      req.dec_ref("os_dec"); // ref for file system
      LOG_WARN("io_submit failed", K(ret), K(submit_count_), K(req));
    } else {
      LOG_DEBUG("Success to submit io request, ", K(ret), K(submit_count_), KP(&req), KP(io_context_));
    }
  }
  return ret;
}

void ObAsyncIOChannel::cancel(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (0 != req.time_log_.submit_ts_ && 0 == req.time_log_.return_ts_) {
    // Note: here if ob_io_cancel failed (possibly due to kernel not supporting io_cancel),
    // neither we or the get_events thread would call control.callback_->process(),
    // as we previously set need_callback to false.
    if (OB_FAIL(device_handle_->io_cancel(io_context_, req.control_block_))) {
      LOG_DEBUG("cancel io request failed", K(ret), K(req), KP(io_context_));
    } else {
      RequestHolder holder(&req);
      ATOMIC_DEC(&submit_count_);
      ATOMIC_FAS(&device_channel_->used_io_depth_, get_io_depth(req.io_size_));
      req.dec_ref("os_dec"); // ref for file system
      LOG_DEBUG("The IO Request has been canceled!");
      LOG_WARN("Shouldn't go here, io cancel not supported", K(ret), K(req));
    }
  }
}

int64_t ObAsyncIOChannel::get_queue_count() const
{
  return submit_count_;
}

void ObAsyncIOChannel::get_events()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(device_handle_->io_getevents(io_context_, 1/*min_nr*/, io_events_, &polling_timeout_))) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
      LOG_ERROR("io get_events failed", K(ret));
    }
  } else if (io_events_->get_complete_cnt() > 0) {
    const int64_t io_return_time = ObTimeUtility::fast_current_time();
    ObIORequest *req = nullptr;
    for (int64_t i = 0; i < io_events_->get_complete_cnt(); ++i) { // ignore ret
      if (OB_ISNULL(req = reinterpret_cast<ObIORequest *>(io_events_->get_ith_data(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("req is null", K(ret));
      } else {
        RequestHolder holder(req);
        req->dec_ref("os_dec"); // ref for file system
        req->time_log_.return_ts_ = io_return_time;
        ATOMIC_FAS(&device_channel_->used_io_depth_, req->io_size_);
        const int system_errno = io_events_->get_ith_ret_code(i);
        const int complete_size = io_events_->get_ith_ret_bytes(i);
        if (OB_LIKELY(0 == system_errno)) { // io succ
          if (complete_size == req->io_size_) { // full complete
            LOG_DEBUG("Success to get io event", K(*req), K(complete_size));
            if (OB_FAIL(on_full_return(*req))) {
              LOG_WARN("process full return io request failed", K(ret), K(*req));
            }
          } else if (complete_size >= 0 && complete_size < req->io_size_) { // partial complete
            LOG_WARN("io request partial finished", K(*req), K(complete_size));
            if (0 == complete_size || !is_io_aligned(complete_size)) { // reach end of file
              if (OB_FAIL(on_partial_return(*req, complete_size))) {
                LOG_WARN("process partial return io request failed", K(ret), K(complete_size), K(*req));
              }
            } else {
              if (OB_FAIL(on_partial_retry(*req, complete_size))) { // partial retry
                LOG_WARN("partial retry io request failed", K(ret), K(complete_size), K(*req));
              }
            }
          } else { // invalid complete size
            LOG_WARN("invalid complete size", K(*req), K(complete_size));
            if (OB_FAIL(on_failed(*req, ObIORetCode(OB_IO_ERROR, complete_size)))) { // use complete_size as errno here
              LOG_WARN("process failed io request failed", K(ret), K(*req));
            }
          }
        } else { // io failed
          int tmp_ret = convert_sys_errno(system_errno);
          LOG_ERROR("io request failed", K(ret), K(tmp_ret), K(system_errno), K(complete_size), K(*req));
          if (EAGAIN == system_errno) { //retry
            if (OB_FAIL(on_full_retry(*req))) {
              LOG_WARN("retry io request failed", K(ret), K(tmp_ret), K(system_errno), K(*req));
            }
          } else {
            if (OB_FAIL(on_failed(*req, ObIORetCode(tmp_ret, system_errno)))) {
              LOG_WARN("process failed io request failed", K(ret), K(tmp_ret), K(system_errno), K(*req));
            }
          }
        }
      }
      ATOMIC_DEC(&submit_count_);
    }
  }
}

int ObAsyncIOChannel::on_full_return(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  req.complete_size_ = req.io_size_;
  if (!req.is_canceled_ && req.can_callback()) {
    if (OB_FAIL(req.tenant_io_mgr_.get_ptr()->enqueue_callback(req))) {
      LOG_WARN("push io request into callback queue failed", K(ret), K(req));
      req.finish(ret);
    }
  } else {
    req.finish(OB_SUCCESS);
  }
  return ret;
}

int ObAsyncIOChannel::on_partial_return(ObIORequest &req, const int64_t complete_size)
{
  int ret = OB_SUCCESS;
  // partial return ignore callback
  req.complete_size_ += complete_size;
  if (req.get_data_size() >= req.io_info_.size_) {
    // in case of aligned_size > file_size > user_need_size
    if (!req.is_canceled_ && req.can_callback()) {
      // the callback is not aware of complete size, not supported for now
      req.finish(OB_NOT_SUPPORTED);
    } else {
      req.finish(OB_SUCCESS);
    }
  } else {
    req.finish(OB_DATA_OUT_OF_RANGE);
  }
  return ret;
}

int ObAsyncIOChannel::on_partial_retry(ObIORequest &req, const int64_t complete_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_io_aligned(complete_size))) {
    ret = OB_ERR_SYS;
    LOG_WARN("complete size not aligned", K(ret), K(complete_size));
  } else {
    req.complete_size_ += complete_size;
    req.io_buf_ += complete_size;
    req.io_offset_ += complete_size;
    req.io_size_ -= complete_size;
    if (OB_FAIL(req.prepare())) {
      LOG_WARN("prepare io request failed", K(ret), K(req));
    } else if (OB_FAIL(submit(req))) {
      LOG_WARN("submit io request failed", K(ret), K(req));
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = on_failed(req, ObIORetCode(ret)))) {
      LOG_WARN("deal with failed request failed", K(tmp_ret), K(ret), K(req));
    }
  }
  return ret;
}

int ObAsyncIOChannel::on_full_retry(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  if (++req.retry_count_ > MAX_RETRY_COUNT) {
    ret = OB_IO_ERROR;
    LOG_WARN("retry too many times", K(ret), K(req));
  } else if (FALSE_IT(req.complete_size_ = 0)) {
  } else if (OB_FAIL(req.prepare())) {
    LOG_WARN("prepare io request failed", K(ret), K(req));
  } else if (OB_FAIL(submit(req))) {
    LOG_WARN("submit io request failed", K(ret));
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = on_failed(req, ObIORetCode(ret)))) {
      LOG_WARN("deal with failed request failed", K(tmp_ret), K(ret), K(req));
    }
  }
  return ret;
}

int ObAsyncIOChannel::on_failed(ObIORequest &req, const ObIORetCode &ret_code)
{
  int ret = OB_SUCCESS;
  req.finish(ret_code);
  return ret;
}


/******************             SyncIOChannel              **********************/
ObSyncIOChannel::ObSyncIOChannel()
  : req_queue_(),
    submit_count_(0),
    is_wait_(false)
{

}

ObSyncIOChannel::~ObSyncIOChannel()
{
  destroy();
}

int ObSyncIOChannel::init(ObDeviceChannel *device_channel)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(base_init(device_channel))) {
    LOG_WARN("base init failed", K(ret), KP(device_channel));
  } else if (OB_FAIL(req_queue_.init(MAX_SYNC_IO_QUEUE_COUNT))) {
    LOG_WARN("init requeust queue failed", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CHANNEL_COND_WAIT))) {
    LOG_WARN("init queue condition failed", K(ret));
  } else {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;

}

void ObSyncIOChannel::destroy()
{
  destroy_thread();
  req_queue_.destroy();
  cond_.destroy();
  device_handle_ = nullptr;
  is_inited_ = false;
}

void ObSyncIOChannel::run1()
{
  int ret = OB_SUCCESS;
  const int64_t thread_id = get_thread_idx();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    set_thread_name("IO_SYNC_CH", thread_id);
    LOG_INFO("sync io thread started", K(thread_id), K(tg_id_));
    while (!has_set_stop()) {
      ObIORequest *req = nullptr;
      if (OB_FAIL(req_queue_.pop(req))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("pop request failed", K(ret));
        } else {
          const int64_t DEFAULT_WAIT_TIMEOUT_MS = 50;
          ObThreadCondGuard guard(cond_);
          is_wait_ = true;
          ObBKGDSessInActiveGuard inactive_guard;
          if (OB_FAIL(cond_.wait(DEFAULT_WAIT_TIMEOUT_MS))) {
            if (OB_TIMEOUT != ret) {
              LOG_WARN("thread condition wait failed", K(ret));
            }
          }
        }
      } else if (OB_ISNULL(req)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("request is null", K(ret), KP(req));
      } else {
        RequestHolder holder(req);
        if (OB_FAIL(do_sync_io(*req))) {
          LOG_WARN("do sync io failed", K(ret), KPC(req));
        }
        req->dec_ref("sync_dec"); // ref for file system
        ATOMIC_DEC(&submit_count_);
      }
    }
    LOG_INFO("sync io thread stopped", K(thread_id), K(tg_id_));
  }
}

int ObSyncIOChannel::submit(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    req.inc_ref("sync_inc"); // ref for file system
    req.time_log_.submit_ts_ = ObTimeUtility::fast_current_time();
    ATOMIC_INC(&submit_count_);
    if (OB_FAIL(req_queue_.push(&req))) {
      req.dec_ref("sync_dec"); // ref for file system
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("push queue failed", K(ret), K(req));
      } else {
        if (REACH_TIME_INTERVAL(1000000L)) {
          LOG_WARN("too many io request in the sync channel", K(ret), K(req_queue_.get_total()));
        }
        ret = OB_EAGAIN;
      }
      ATOMIC_DEC(&submit_count_);
    } else {
      ObThreadCondGuard guard(cond_);
      if (is_wait_ && OB_FAIL(cond_.signal())) {
        LOG_WARN("thread condition signal failed", K(ret));
      } else {
        is_wait_ = false;
      }
    }
  }
  return ret;
}

void ObSyncIOChannel::cancel(ObIORequest &req)
{
  int ret = OB_NOT_SUPPORTED;
  LOG_WARN("cancel is not supported on sync io channel", K(ret), K(req));
}

int64_t ObSyncIOChannel::get_queue_count() const
{
  // submit_count_ include request in req_queue_ and request is submitting
  return ATOMIC_LOAD(&submit_count_);
}

int ObSyncIOChannel::do_sync_io(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(device_handle_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("device handle is null", K(ret));
  } else if (req.get_flag().is_read()) {
    if (OB_FAIL(check_io_hang_errsim())) {
    } else if (OB_FAIL(device_handle_->pread(req.io_info_.fd_, req.io_info_.offset_, req.io_info_.size_, req.io_buf_, req.complete_size_))) {
      LOG_WARN("pread failed", K(ret), K(req));
    }
  } else if (req.get_flag().is_write()) {
    if (OB_FAIL(check_io_hang_errsim())) {
    } else if (OB_FAIL(device_handle_->pwrite(req.io_info_.fd_, req.io_info_.offset_, req.io_info_.size_, req.io_buf_, req.complete_size_))) {
      LOG_WARN("pread failed", K(ret), K(req));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported io mode", K(ret), K(req));
  }
  if (OB_SUCC(ret)) {
    req.time_log_.return_ts_ = ObTimeUtility::fast_current_time();
    if (!req.is_canceled_ && req.can_callback()) {
      if (OB_FAIL(req.tenant_io_mgr_.get_ptr()->enqueue_callback(req))) {
        LOG_WARN("push io request into callback queue failed", K(ret), K(req));
        req.finish(ret);
      }
    } else {
      req.finish(OB_SUCCESS);
    }
  } else {
    req.finish(ret);
  }
  return ret;
}

/******************             DeviceChannel              **********************/
ObDeviceChannel::ObDeviceChannel()
  : is_inited_(false),
    allocator_(nullptr),
    device_handle_(nullptr),
    used_io_depth_(0),
    max_io_depth_(0)
{

}

ObDeviceChannel::~ObDeviceChannel()
{
  destroy();
}

int ObDeviceChannel::init(ObIODevice *device_handle,
                          const int64_t async_channel_count,
                          const int64_t sync_channel_count,
                          const int64_t max_io_depth,
                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(nullptr == device_handle
        || async_channel_count < 0
        || sync_channel_count <= 0
        || max_io_depth <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle), K(async_channel_count), K(sync_channel_count), K(max_io_depth));
  } else {
    device_handle_ = device_handle;
    used_io_depth_ = 0;
    max_io_depth_ = max_io_depth;
    allocator_ = &allocator;
    for (int64_t i = 0; OB_SUCC(ret) && i < async_channel_count; ++i) {
      ObAsyncIOChannel *ch = nullptr;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObAsyncIOChannel)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc async channel failed", K(ret), K(i), K(async_channel_count));
      } else if (FALSE_IT(ch = new (buf) ObAsyncIOChannel())) {
      } else if (OB_FAIL(ch->init(this))) {
        LOG_WARN("init async channel failed", K(ret));
      } else if (OB_FAIL(ch->start_thread())) {
        LOG_WARN("start thread failed", K(ret), KPC(ch));
      } else if (OB_FAIL(async_channels_.push_back(ch))) {
        LOG_WARN("push back async channel failed", K(ret), KPC(ch));
      } else {
        ch = nullptr;
      }
      if (OB_UNLIKELY(nullptr != ch)) {
        ch->~ObAsyncIOChannel();
        allocator.free(ch);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sync_channel_count; ++i) {
      ObSyncIOChannel *ch = nullptr;
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSyncIOChannel)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc sync channel failed", K(ret), K(i), K(sync_channel_count));
      } else if (FALSE_IT(ch = new (buf) ObSyncIOChannel())) {
      } else if (OB_FAIL(ch->init(this))) {
        LOG_WARN("init async channel failed", K(ret));
      } else if (OB_FAIL(ch->start_thread())) {
        LOG_WARN("start thread failed", K(ret), KPC(ch));
      } else if (OB_FAIL(sync_channels_.push_back(ch))) {
        LOG_WARN("push back async channel failed", K(ret), KPC(ch));
      } else {
        ch = nullptr;
      }
      if (OB_UNLIKELY(nullptr != ch)) {
        ch->~ObSyncIOChannel();
        allocator.free(ch);
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObDeviceChannel::destroy()
{
  is_inited_ = false;
  for (int64_t i = 0; i < async_channels_.count(); ++i) {
    ObAsyncIOChannel *ch = static_cast<ObAsyncIOChannel *>(async_channels_.at(i));
    ch->stop();
  }
  for (int64_t i = 0; i < async_channels_.count(); ++i) {
    ObAsyncIOChannel *ch = static_cast<ObAsyncIOChannel *>(async_channels_.at(i));
    ch->wait();
  }
  for (int64_t i = 0; i < async_channels_.count(); ++i) {
    ObIOChannel *ch = async_channels_.at(i);
    if (nullptr != allocator_ && nullptr != ch) {
      ch->~ObIOChannel();
      allocator_->free(ch);
    }
  }
  async_channels_.destroy();
  for (int64_t i = 0; i < sync_channels_.count(); ++i) {
    ObIOChannel *ch = sync_channels_.at(i);
    if (nullptr != allocator_ && nullptr != ch) {
      ch->~ObIOChannel();
      allocator_->free(ch);
    }
  }
  sync_channels_.destroy();
  allocator_ = nullptr;
}

int ObDeviceChannel::submit(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  ObIOChannel *ch = nullptr;
  RequestHolder holder(&req);
  const bool is_sync = req.get_flag().is_sync();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    if (is_sync) {
      lock_for_sync_io_.lock();
    }
    if (OB_FAIL(get_random_io_channel(is_sync ? sync_channels_ : async_channels_, ch))) {
      LOG_WARN("get random io channel failed", K(ret), K(sync_channels_.count()), K(is_sync));
    } else if (OB_FAIL(ch->submit(req))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("submit request failed", K(ret), K(req));
      }
    }
    if (is_sync) {
      lock_for_sync_io_.unlock();
    }
  }
  return ret;
}

int ObDeviceChannel::get_random_io_channel(ObIArray<ObIOChannel *> &io_channels, ObIOChannel *&ch)
{
  int ret = OB_SUCCESS;
  ch = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(io_channels.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(io_channels.count()));
  } else {
    int64_t idx = 0;
    for (idx = 0; idx < io_channels.count(); idx++) {
      if (0 == io_channels.at(idx)->get_queue_count()) {
        break;
      }
    }
    if (idx == io_channels.count()) {
      const int64_t idx1 = ObRandom::rand(0, io_channels.count() - 1);
      const int64_t idx2 = ObRandom::rand(0, io_channels.count() - 1);
      idx = (io_channels.at(idx1)->get_queue_count() < io_channels.at(idx2)->get_queue_count() ? idx1 : idx2);
    }
    ch = io_channels.at(idx);
  }
  return ret;
}

/******************             IORunner              **********************/
ObIORunner::ObIORunner()
  : is_inited_(false),
    tg_id_(-1)
{

}

ObIORunner::~ObIORunner()
{
  destroy();
}


int ObIORunner::init(const int64_t queue_capacity)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(queue_capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid", K(ret), K(queue_capacity));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::IO_CALLBACK, tg_id_, queue_capacity))) {
    LOG_WARN("create runner thread failed", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    LOG_WARN("start runner thread failed", K(ret), K(tg_id_));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIORunner::stop()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
  }
}

void ObIORunner::wait()
{
  if (tg_id_ >= 0) {
    TG_WAIT(tg_id_);
  }
}

void ObIORunner::destroy()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    TG_DESTROY(tg_id_);
    tg_id_ = -1;
  }
  is_inited_ = false;
}

int ObIORunner::push(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  req.time_log_.callback_enqueue_ts_ = ObTimeUtility::fast_current_time();
  req.inc_ref("cb_inc"); // ref for callback queue
  if (OB_FAIL(TG_PUSH_TASK(tg_id_, &req))) {
    LOG_WARN("Fail to enqueue callback", K(ret));
    req.dec_ref("cb_dec"); // ref for callback queue
  }
  return ret;
}

void ObIORunner::handle(void *task)
{
  int ret = OB_SUCCESS;
  ObIORequest *req = (ObIORequest *)task;
  RequestHolder holder(req);
  if (OB_ISNULL(req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(req));
  } else {
    const int64_t begin_time = ObTimeUtility::fast_current_time();
    req->dec_ref("cb_dec"); // ref for callback queue
    req->time_log_.callback_dequeue_ts_ = begin_time;
    ObTraceIDGuard trace_guard(req->trace_id_);
    { // callback must execute in guard, in case of cancel halfway
      ObThreadCondGuard guard(req->cond_);
      if (OB_FAIL(guard.get_ret())) {
        LOG_ERROR("Fail to lock req condition, ", K(ret), K(*req));
      } else if (req->is_canceled_) {
        // canceled, do nothing
      } else if (!req->can_callback()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("io request can not do callback", K(ret), K(*req));
      } else if (OB_FAIL(req->ret_code_.io_ret_)) {
        //failed, ignore
      } else if (OB_FAIL(req->copied_callback_->process(req->get_io_data_buf(), req->io_info_.size_))) {
        LOG_WARN("fail to callback", K(ret), K(*req), K(MTL_ID()));
      }
      req->time_log_.callback_finish_ts_ = ObTimeUtility::fast_current_time();
      //recycle buffer after process
      req->free_io_buffer();
    }
    req->finish(ret);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("handle request failed", K(ret), KPC(req));
  }
}

void ObIORunner::handle_drop(void *task)
{
  ObIORequest *req = (ObIORequest *)task;
  if (OB_NOT_NULL(req)) {
    req->finish(OB_CANCELED);
    req->dec_ref("cb_dec"); // ref for callback queue
  }
}

int64_t ObIORunner::get_queue_count()
{
  int64_t num = 0;
  TG_GET_QUEUE_NUM(tg_id_, num);
  if (num < 0) { num = 0; }
  return num;
}

int ObIORunner::set_thread_cnt(int64_t thread_count)
{
  return TG_SET_THREAD_CNT(tg_id_, thread_count);
}

/******************             IOCallbackManager              **********************/

ObIOCallbackManager::ObIOCallbackManager()
  : is_inited_(false),
    runner_()
{

}

ObIOCallbackManager::~ObIOCallbackManager()
{
  destroy();
}

int ObIOCallbackManager::init(const int64_t tenant_id, const int64_t thread_count,
                              const int64_t queue_depth)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(thread_count <= 0 || queue_depth <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(thread_count), K(queue_depth));
  } else if (OB_FAIL(runner_.init(queue_depth))) {
    LOG_WARN("init callback runner failed", K(ret), K(queue_depth));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIOCallbackManager::destroy()
{
  runner_.destroy();
  is_inited_ = false;
}

int ObIOCallbackManager::enqueue_callback(ObIORequest &req)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (req.is_canceled_) {
    ret = OB_CANCELED;
  } else if (!req.can_callback()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(req));
  } else if (OB_FAIL(runner_.push(req))) {
    LOG_WARN("push callback failed", K(ret), K(req));
  }
  return ret;
}

int ObIOCallbackManager::update_thread_count(const int64_t thread_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", K(ret));
  } else if (OB_UNLIKELY(thread_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(thread_count));
  } else if (OB_FAIL(runner_.set_thread_cnt(thread_count))) {
    LOG_WARN("invalid argument", K(ret), K(thread_count));
  }
  return ret;
}

int64_t ObIOCallbackManager::get_queue_count()
{
  return runner_.get_queue_count();
}

const char *oceanbase::common::device_health_status_to_str(const ObDeviceHealthStatus dhs)
{
  const char *hstr = "UNKNOWN";
  switch (dhs) {
    case DEVICE_HEALTH_NORMAL:
      hstr = "NORMAL";
      break;
    case DEVICE_HEALTH_WARNING:
      hstr = "WARNING";
      break;
    case DEVICE_HEALTH_ERROR:
      hstr = "ERROR";
      break;
    default:
      hstr = "UNKNOWN";
      break;
  }
  if (STRLEN(hstr) > OB_MAX_DEVICE_HEALTH_STATUS_STR_LENGTH) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid device health status str", K(hstr),
        K(OB_MAX_DEVICE_HEALTH_STATUS_STR_LENGTH), K(dhs));
  }
  return hstr;
}

/******************             IOFaultDetector              **********************/

ObIOFaultDetector::ObIOFaultDetector(const ObIOConfig &io_config)
  : is_inited_(false),
    lock_(ObLatchIds::IO_FAULT_DETECTOR_LOCK),
    io_config_(io_config),
    is_device_warning_(false),
    last_device_warning_ts_(0),
    is_device_error_(false),
    begin_device_error_ts_(0),
    last_device_error_ts_(0)
{

}

ObIOFaultDetector::~ObIOFaultDetector()
{
  destroy();
}

int ObIOFaultDetector::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io fault detector init twice", K(ret), K(!is_inited_));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIOFaultDetector::destroy()
{
  TG_STOP(TGDefIDs::IO_HEALTH);
  TG_WAIT(TGDefIDs::IO_HEALTH);
  is_device_warning_ = false;
  is_device_error_ = false;
  begin_device_error_ts_ = 0;
  last_device_error_ts_ = 0;
  is_inited_ = false;
}

int ObIOFaultDetector::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io fault detector not init", K(ret), KP(is_inited_));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(TGDefIDs::IO_HEALTH, *this))) {
    LOG_WARN("start thread pool failed", K(ret));
  }
  return ret;
}

struct RetryTask
{
  ObIOInfo io_info_;
  int64_t timeout_ms_;
};

void ObIOFaultDetector::handle(void *task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io fault detector not init", K(ret), KP(is_inited_));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else {
    const int64_t LONG_AIO_TIMEOUT_MS = 30000; // 30s
    RetryTask *retry_task = reinterpret_cast<RetryTask *>(task);
    retry_task->io_info_.flag_.set_unlimited();
    retry_task->io_info_.flag_.set_detect();
    if ((is_device_warning_ || is_device_error_) && retry_task->io_info_.flag_.is_time_detect()) {
      //ignore
    } else {
      int64_t timeout_ms = retry_task->timeout_ms_;
      // remain 1s to avoid race condition for retry_black_list_interval
      const int64_t retry_black_list_interval_ms = io_config_.read_failure_black_list_interval_ / 1000L - 1000L;
      // rety_io_timeout must less than black_list_interval
      const int64_t MIN_IO_RETRY_TIMEOUT_MS = min(10L * 1000L/* 10s */, retry_black_list_interval_ms);
      const int64_t MAX_IO_RETRY_TIMEOUT_MS = min(180L * 1000L/* 180s*/, retry_black_list_interval_ms);
      const int64_t diagnose_begin_ts = ObTimeUtility::fast_current_time();
      bool is_retry_succ = false;
      int64_t fs_error_times = 0;
      while (OB_SUCC(ret) && !OB_IO_MANAGER.is_stopped() && !is_retry_succ && !is_device_error_) {
        ObIOHandle handle;
        const int64_t current_retry_ts = ObTimeUtility::fast_current_time();
        const int64_t warn_ts = diagnose_begin_ts + io_config_.data_storage_warning_tolerance_time_;
        const int64_t error_ts = diagnose_begin_ts + io_config_.data_storage_error_tolerance_time_;
        const int64_t left_timeout_ms = !is_device_warning_ ?
          (warn_ts - current_retry_ts) / 1000 : (error_ts - current_retry_ts) / 1000;
        // timeout of retry io increase exponentially
        timeout_ms = min(left_timeout_ms, min(MAX_IO_RETRY_TIMEOUT_MS, max(timeout_ms * 2, MIN_IO_RETRY_TIMEOUT_MS)));
        int sys_io_errno = 0;
        if (timeout_ms > 0) {
          // do retry io
          if (OB_FAIL(OB_IO_MANAGER.detect_read(retry_task->io_info_, handle, timeout_ms))) {
            int tmp_ret = OB_SUCCESS;
            if (OB_TMP_FAIL(handle.get_fs_errno(sys_io_errno))) {
              LOG_WARN("get fs errno num failed", K(ret), K(sys_io_errno));
            }
            if (OB_TIMEOUT == ret || OB_IO_TIMEOUT == ret) {
              LOG_WARN("ObIOManager::read failed", K(ret), K(retry_task->io_info_), K(timeout_ms));
              ret = OB_SUCCESS;
            } else if (OB_EAGAIN == ret) { //maybe channel is busy, wait and retry
              ob_usleep(100 * 1000); // 100ms
              ret = OB_SUCCESS;
            } else if (sys_io_errno != 0) {
              ++ fs_error_times;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("ObIOManager::retry read request failed", K(ret), K(retry_task->io_info_));
            }
          } else {
            is_retry_succ = true;
          }
        }
        if (OB_SUCC(ret) && !is_retry_succ) {
          const int64_t current_ts = ObTimeUtility::fast_current_time();
          if (current_ts >= error_ts || (sys_io_errno != 0 && fs_error_times >= MAX_DETECT_READ_ERROR_TIMES)) {
            set_device_error();
            LOG_WARN("ObIOManager::detect IO retry timeout, device error", K(ret), K(current_ts), K(error_ts), K(retry_task->io_info_));
          } else if (current_ts >= warn_ts || (sys_io_errno != 0 && fs_error_times >= MAX_DETECT_READ_WARN_TIMES)) {
            set_device_warning();
            LOG_WARN("ObIOManager::detect IO retry reach limit, device warning", K(ret), K(sys_io_errno), K(current_ts), K(current_ts), K(fs_error_times), K(retry_task->io_info_));
          }
        }
      }
    }
    op_free(const_cast<RetryTask *>(retry_task));
    retry_task = nullptr;
  }
}

int ObIOFaultDetector::get_device_health_status(ObDeviceHealthStatus &dhs,
    int64_t &device_abnormal_time)
{
  int ret = OB_SUCCESS;
  dhs = DEVICE_HEALTH_NORMAL;
  device_abnormal_time = 0;

  if (is_device_warning_ && last_device_warning_ts_ > 0 && !is_device_error_) {
    const int64_t period = ObTimeUtility::fast_current_time() - last_device_warning_ts_;
    if (period > io_config_.read_failure_black_list_interval_) {
      last_device_warning_ts_ = 0;
      is_device_warning_ = false;
    }
  }

  if (is_device_error_) {
    dhs = DEVICE_HEALTH_ERROR;
    device_abnormal_time = begin_device_error_ts_;
  } else if (is_device_warning_) {
    dhs = DEVICE_HEALTH_WARNING;
    device_abnormal_time = last_device_warning_ts_;
  } else {
    dhs = DEVICE_HEALTH_NORMAL;
    device_abnormal_time = 0;
  }

  return ret;
}

void ObIOFaultDetector::reset_device_health()
{
  is_device_warning_ = false;
  last_device_warning_ts_ = 0;
  is_device_error_ = false;
  begin_device_error_ts_ = 0;
  last_device_error_ts_ = 0;
}

int ObIOFaultDetector::record_timing_task(const int64_t first_id, const int64_t second_id)
{
  int ret = OB_SUCCESS;
  RetryTask *retry_task = nullptr;
  if (OB_ISNULL(retry_task = op_alloc(RetryTask))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc RetryTask failed", K(ret));
  } else {
    retry_task->io_info_.tenant_id_ = OB_SERVER_TENANT_ID;
    retry_task->io_info_.size_ = 4096;
    //todo qilu: after column store merge, add allocator for user_data_buf
    retry_task->io_info_.buf_ = nullptr;
    retry_task->io_info_.flag_.set_mode(ObIOMode::READ);
    retry_task->io_info_.flag_.set_sys_module_id(OB_INVALID_ID);
    retry_task->io_info_.flag_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    retry_task->io_info_.flag_.set_time_detect();
    retry_task->io_info_.fd_.first_id_ = first_id;
    retry_task->io_info_.fd_.second_id_ = second_id;
    retry_task->io_info_.offset_ = ObRandom::rand(0, OB_DEFAULT_MACRO_BLOCK_SIZE - 4096);
    retry_task->io_info_.callback_ = nullptr;
    retry_task->timeout_ms_ = io_config_.data_storage_warning_tolerance_time_; // default 5s
    if (OB_FAIL(TG_PUSH_TASK(TGDefIDs::IO_HEALTH, retry_task))) {
      LOG_WARN("io fault detector push task failed", K(ret), KP(retry_task));
    }
    if (OB_FAIL(ret)) {
      op_free(retry_task);
      retry_task = nullptr;
    }
  }
  return ret;
}

void ObIOFaultDetector::record_failure(const ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io fault detector not init", K(ret), KP(is_inited_));
  } else if (req.get_flag().is_detect()) {
    //reach max retry time, ignore
  } else if (req.get_flag().is_sync()) {
    LOG_INFO("ignore fault detect for sync io", K(req));
  } else if (req.is_finished_ && OB_IO_ERROR != req.ret_code_.io_ret_) {
    // ignore, do nothing here
  } else if (req.get_flag().is_read()) {
    if (OB_FAIL(record_read_failure(req))) {
      LOG_WARN("record read failure failed", K(ret), K(req));
    }
  } else if (req.get_flag().is_write()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported io write detect", K(ret), K(req));
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported io mode", K(ret), K(req));
  }
}

int ObIOFaultDetector::record_read_failure(const ObIORequest &req)
{
  int ret = OB_SUCCESS;
  RetryTask *retry_task = nullptr;
  if (OB_ISNULL(retry_task = op_alloc(RetryTask))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc RetryTask failed", K(ret));
  } else {
    retry_task->io_info_ = req.io_info_;
    retry_task->io_info_.flag_.set_sys_module_id(ObIOModule::DETECT_IO);
    // fix bug2024101100104667912, detect io failure asynchronously
    retry_task->io_info_.flag_.set_async();
    retry_task->io_info_.callback_ = nullptr;
    retry_task->timeout_ms_ = 5000L; // 5s
    if (OB_FAIL(TG_PUSH_TASK(TGDefIDs::IO_HEALTH, retry_task))) {
      LOG_WARN("io fault detector push task failed", K(ret), KP(retry_task));
    } else {
      LOG_INFO("io fault detector push task", KP(retry_task));
    }
    if (OB_FAIL(ret)) {
      op_free(retry_task);
      retry_task = nullptr;
    }
  }
  return ret;
}

// set disk warning and record warn_ts
// until warn_ts + io_config.read_failure_black_list_interval, this server is not allowed to be partition leader
void ObIOFaultDetector::set_device_warning()
{
  last_device_warning_ts_ = ObTimeUtility::fast_current_time();
  is_device_warning_ = true;
  LOG_WARN_RET(OB_IO_ERROR, "disk maybe corrupted");
}

// set disk error and record error_ts
// if the disk is confirmed normal, the administrator can reset disk status by:
// alter system set disk valid server [=] 'ip:port'
void ObIOFaultDetector::set_device_error()
{
  if (!is_device_warning_) {
    set_device_warning();
  }
  if (!is_device_error_) {
    begin_device_error_ts_ = ObTimeUtility::fast_current_time();
  }
  last_device_error_ts_ = ObTimeUtility::fast_current_time();
  is_device_error_ = true;
  LOG_ERROR_RET(OB_IO_ERROR, "set_disk_error: attention!!!");
  LOG_DBA_ERROR_V2(OB_COMMON_DISK_INVALID, OB_DISK_ERROR,
                    "The disk may be corrupted. ",
                    "[suggestion] check disk.");
}

ObIOTracer::ObIOTracer()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), trace_map_()
{

}

ObIOTracer::~ObIOTracer()
{
  destroy();
}

int ObIOTracer::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const ObMemAttr attr = SET_USE_500("io_trace_map");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(trace_map_.create(1009, attr))) {
    LOG_WARN("create trace map failed", K(ret));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIOTracer::destroy()
{
  trace_map_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObIOTracer::reuse()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(trace_map_.reuse())) {
    LOG_WARN_RET(tmp_ret, "reuse trace map failed", K(tmp_ret));
  }
}

ObIOTracer::RefLog::RefLog()
  : click_count_(0)
{
  memset(click_str_, 0, sizeof(click_str_));
}

void ObIOTracer::RefLog::click(const char *mod)
{
  int64_t old_index = ATOMIC_FAA(&click_count_, 1);
  if (OB_LIKELY(old_index < MAX_CLICK_COUNT)) {
    click_str_[old_index] = mod;
  }
}

ObIOTracer::TraceInfo::TraceInfo()
  : ref_log_()
{
  memset(bt_str_, 0, sizeof(bt_str_));
}

uint64_t ObIOTracer::TraceInfo::hash() const
{
  return murmurhash(bt_str_, sizeof(bt_str_), 0);
}

bool ObIOTracer::TraceInfo::operator== (const TraceInfo &param) const
{
  return 0 == memcmp(bt_str_, param.bt_str_, sizeof(bt_str_));
}

int ObIOTracer::trace_request(const ObIORequest *req, const char *msg, const TraceType trace_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the io tracer is not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(req), KCSTRING(msg), K(trace_type));
  } else if (ObIOTracer::TraceType::IS_FIRST == trace_type) {
    TraceInfo trace_info;
    char *bt_str = lbt();
    strncpy(trace_info.bt_str_, bt_str, sizeof(trace_info.bt_str_) - 1);
    trace_info.ref_log_.click(msg);
    if (OB_FAIL(trace_map_.set_refactored(reinterpret_cast<int64_t>(req), trace_info))) {
      LOG_WARN("add trace failed", K(ret));
    }
  } else {
    struct ModifyFn {
      ModifyFn(const char *msg) : msg_(msg) {}
      int operator () (hash::HashMapPair<int64_t, TraceInfo> &entry) {
        entry.second.ref_log_.click(msg_);
        return OB_SUCCESS;
      }
      const char *msg_;
    };
    ModifyFn modify_fn(msg);
    if (OB_FAIL(trace_map_.atomic_refactored(reinterpret_cast<int64_t>(req), modify_fn))) {
      LOG_WARN("modify trace failed", K(ret));
    }
    if (OB_SUCC(ret) && ObIOTracer::TraceType::IS_LAST == trace_type) {
      if (OB_FAIL(trace_map_.erase_refactored(reinterpret_cast<int64_t>(req)))) {
        LOG_WARN("remove trace failed", K(ret));
      }
    }
  }
  return ret;
}

int64_t ObIOTracer::to_string(char *buf, const int64_t len) const
{
  struct UpdateFn {
    int operator () (hash::HashMapPair<TraceInfo, int64_t> &entry) {
      ++entry.second;
      return OB_SUCCESS;
    }
  };

  struct CountFn {
    CountFn() : req_count_(0) {}
    int init() { return bt_count_.create(97, "count_fn"); }
    int operator () (hash::HashMapPair<int64_t, TraceInfo> &entry) {
      int ret = OB_SUCCESS;
      ++req_count_;
      if (OB_FAIL(bt_count_.set_refactored(entry.second, 1))) {
        if (OB_HASH_EXIST == ret) {
          UpdateFn update_fn;
          if (OB_FAIL(bt_count_.atomic_refactored(entry.second, update_fn))) {
            LOG_WARN("update backtrace count failed", K(ret));
          }
        } else {
          LOG_WARN("insert backtrace count failed", K(ret));
        }
      }
      return ret;
    }
    int64_t req_count_;
    hash::ObHashMap<TraceInfo, int64_t> bt_count_;
  };

  struct TraceItem
  {
  public:
    TraceItem() : trace_info_(), count_(0) {}
    TraceItem(const TraceInfo &trace, const int64_t count) : trace_info_(trace), count_(count) {}
    TO_STRING_KV(K(trace_info_), K(count_));
    TraceInfo trace_info_;
    int64_t count_;
  };

  struct StoreFn {
    StoreFn(ObIArray<TraceItem> &trace_array) : trace_array_(trace_array) {}
    int operator () (hash::HashMapPair<TraceInfo, int64_t> &entry) {
      TraceItem item(entry.first, entry.second);
      return trace_array_.push_back(item);
    }
    ObIArray<TraceItem> &trace_array_;
  };

  struct {
    bool operator()(const TraceItem &left, const TraceItem &right) const
    {
      return left.count_ < right.count_;
    }
  } sort_fn;

  int64_t pos = 0;
  int ret = OB_SUCCESS;
  CountFn counter;
  if (OB_FAIL(counter.init())) {
    LOG_WARN("init trace counter failed", K(ret));
  } else if (OB_FAIL(trace_map_.foreach_refactored(counter))) {
    LOG_WARN("count io trace failed", K(ret));
  } else if (counter.req_count_ > 0) {
    ObArray<TraceItem> trace_array;
    StoreFn store_fn(trace_array);
    if (OB_FAIL(trace_array.reserve(counter.bt_count_.size()))) {
      LOG_WARN("reserve trace array failed", K(ret));
    } else if (OB_FAIL(counter.bt_count_.foreach_refactored(store_fn))) {
      LOG_WARN("get max backtrace count failed", K(ret));
    } else {
      lib::ob_sort(trace_array.begin(), trace_array.end(), sort_fn);
      databuff_printf(buf, len, pos, "trace_request_count: %ld, distinct_backtrace_count: %ld; ", counter.req_count_, trace_array.count());
      const int64_t print_count = min(5, trace_array.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < print_count; ++i) {
        const TraceItem &item = trace_array.at(i);
        ObCStringHelper helper;
        databuff_printf(buf, len, pos, "top: %ld, count: %ld, ref_log: %s, backtrace: %s; ", i + 1, item.count_, helper.convert(item.trace_info_.ref_log_), item.trace_info_.bt_str_);
      }
    }
  }
  return pos;
}
void ObIOTracer::print_status()
{
  LOG_INFO("[IO STATUS TRACER]", K_(tenant_id), K(*this));
}
