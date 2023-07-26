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

#include "share/io/ob_io_calibration.h"

#include "lib/time/ob_time_utility.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/config/ob_config_helper.h"
#include "share/io/ob_io_struct.h"
#include "share/io/ob_io_manager.h"
#include "observer/ob_server.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;


/******************             IOBenchLoad              **********************/

ObIOBenchLoad::ObIOBenchLoad()
  : mode_(ObIOMode::MAX_MODE),
    size_(0)
{

}

ObIOBenchLoad::~ObIOBenchLoad()
{

}

void ObIOBenchLoad::reset()
{
  mode_ = ObIOMode::MAX_MODE;
  size_ = 0;
}

bool ObIOBenchLoad::is_valid() const
{
  return mode_ < ObIOMode::MAX_MODE && size_ > 0 && size_ <= OB_DEFAULT_MACRO_BLOCK_SIZE;
}

/******************             IOBenchResult              **********************/

OB_SERIALIZE_MEMBER(ObIOBenchResult, mode_, size_, iops_, rt_us_);

ObIOBenchResult::ObIOBenchResult()
  : mode_(ObIOMode::MAX_MODE),
    size_(0),
    iops_(0),
    rt_us_(0)
{

}

ObIOBenchResult::~ObIOBenchResult()
{

}

void ObIOBenchResult::reset()
{
  mode_ = ObIOMode::MAX_MODE;
  size_ = 0;
  iops_ = 0;
  rt_us_ = 0;
}

bool ObIOBenchResult::is_valid() const
{
  return mode_ < ObIOMode::MAX_MODE
    && size_ > 0
    && iops_ > std::numeric_limits<double>::epsilon()
    && rt_us_ > std::numeric_limits<double>::epsilon();
}

bool ObIOBenchResult::operator==(const ObIOBenchResult &other) const
{
  return mode_ == other.mode_
    && size_ == other.size_
    && fabs(iops_ - other.iops_) < std::numeric_limits<double>::epsilon()
    && fabs(rt_us_ - other.rt_us_) < std::numeric_limits<double>::epsilon();
}

/******************             IOAbility              **********************/


ObIOAbility::ObIOAbility()
  : measure_items_()
{

}

ObIOAbility::~ObIOAbility()
{

}

void ObIOAbility::reset()
{
  for (int64_t i = 0; i < static_cast<int>(ObIOMode::MAX_MODE); ++i) {
    measure_items_[i].reset();
  }
}

bool ObIOAbility::is_valid() const
{
  bool bret = true;
  for (int64_t i = 0; bret && i < static_cast<int>(ObIOMode::MAX_MODE); ++i) {
    const MeasureItemArray &cur_array = measure_items_[i];
    bret = cur_array.count() > 0;
    for (int64_t j = 0; bret && j < cur_array.count(); ++j) {
      bret = cur_array.at(j).is_valid();
    }
  }
  return bret;
}


int ObIOAbility::assign(const ObIOAbility &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int>(ObIOMode::MAX_MODE); ++i) {
    if (OB_FAIL(measure_items_[i].assign(other.measure_items_[i]))) {
      LOG_WARN("assign measure items failed", K(ret), K(other));
    }
  }
  return ret;
}

bool ObIOAbility::operator==(const ObIOAbility &other) const
{
  bool is_equal = true;
  for (int64_t i = 0; is_equal && i < static_cast<int>(ObIOMode::MAX_MODE); ++i) {
    const MeasureItemArray &other_item_array = other.measure_items_[i];
    const MeasureItemArray &local_item_array = measure_items_[i];
    if (local_item_array.count() != other_item_array.count()) {
      is_equal = false;
    } else {
      for (int64_t j = 0; is_equal && j < local_item_array.count(); ++j) {
        is_equal = local_item_array.at(j) == other_item_array.at(j);
      }
    }
  }
  return is_equal;
}

int ObIOAbility::add_measure_item(const ObIOBenchResult &item)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (OB_FAIL(measure_items_[static_cast<int>(item.mode_)].push_back(item))) {
    LOG_WARN("push back measure_items failed", K(ret), K(item));
  } else {
    std::sort(measure_items_[static_cast<int>(item.mode_)].begin(), measure_items_[static_cast<int>(item.mode_)].end(),
        [](const ObIOBenchResult &left, const ObIOBenchResult &right) {
        return left.size_ < right.size_;
        });
  }
  return ret;
}

const ObIOAbility::MeasureItemArray &ObIOAbility::get_measure_items(const ObIOMode mode) const
{
  static MeasureItemArray dummy_items;
  const MeasureItemArray *ret_items = nullptr;
  if (mode < ObIOMode::MAX_MODE) {
    ret_items = &measure_items_[static_cast<int64_t>(mode)];
  } else {
    ret_items = &dummy_items;
  }
  return *ret_items;
}

int ObIOAbility::get_iops(const ObIOMode mode, const int64_t size, double &iops) const
{
  int ret = OB_SUCCESS;
  int64_t found_item_idx = -1;
  if (OB_UNLIKELY(mode < ObIOMode::READ || mode >= ObIOMode::MAX_MODE || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mode), K(size));
  } else if (OB_FAIL(find_item(mode, size, found_item_idx))) {
    LOG_WARN("find measure item failed", K(ret), K(mode), K(size));
  } else if (OB_UNLIKELY(found_item_idx < 0)) {
    // there is no measure item of bigger size, assume fixed bandwith
    const ObIOBenchResult &tail_item = measure_items_[static_cast<int>(mode)].at(measure_items_[static_cast<int>(mode)].count() - 1);
    iops = tail_item.iops_ * tail_item.size_ / size;
  } else {
    const ObIOBenchResult &found_item = measure_items_[static_cast<int>(mode)].at(found_item_idx);
    if (size == found_item.size_ // exactly match
        || 0 == found_item_idx) { // size smaller than smallest measure item
      iops = found_item.iops_;
    } else {
      const ObIOBenchResult &prev_item = measure_items_[static_cast<int>(mode)].at(found_item_idx - 1);
      const double avg_bandwidth = (prev_item.iops_ * prev_item.size_ + found_item.iops_ * found_item.size_) / 2.0;
      iops = avg_bandwidth / (double)size;
    }
  }
  return ret;
}

int ObIOAbility::get_rt(const ObIOMode mode, const int64_t size, double &rt_us) const
{
  int ret = OB_SUCCESS;
  int64_t found_item_idx = -1;
  if (OB_UNLIKELY(mode < ObIOMode::READ || mode >= ObIOMode::MAX_MODE || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mode), K(size));
  } else if (OB_FAIL(find_item(mode, size, found_item_idx))) {
    LOG_WARN("find measure item failed", K(ret), K(mode), K(size));
  } else if (OB_UNLIKELY(found_item_idx < 0)) {
    // there is no measure item of bigger size, assume linear growth for rt
    const ObIOBenchResult &tail_item = measure_items_[static_cast<int>(mode)].at(measure_items_[static_cast<int>(mode)].count() - 1);
    rt_us = (double)size / tail_item.size_ * tail_item.rt_us_;
  } else {
    const ObIOBenchResult &found_item = measure_items_[static_cast<int>(mode)].at(found_item_idx);
    if (size == found_item.size_ // exactly match
        || 0 == found_item_idx) { // size smaller than smallest measure item
      rt_us = found_item.rt_us_;
    } else {
      const ObIOBenchResult &prev_item = measure_items_[static_cast<int>(mode)].at(found_item_idx - 1);
      const double slope = (found_item.rt_us_ - prev_item.rt_us_) / static_cast<double>(found_item.size_ - prev_item.size_);
      rt_us = prev_item.rt_us_ + slope * (size - prev_item.size_);
    }
  }
  return ret;
}

int ObIOAbility::find_item(const ObIOMode mode, const int64_t size, int64_t &item_idx) const
{
  int ret = OB_SUCCESS;
  const MeasureItemArray &item_array = measure_items_[static_cast<int>(mode)];
  if (OB_UNLIKELY(item_array.count() <= 0)) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid measure_items", K(ret), K(mode), K(item_array.count()));
  } else {
    MeasureItemArray::const_iterator found_it = std::lower_bound(item_array.begin(), item_array.end(), size,
        [](const ObIOBenchResult &left, const int64_t size) {
          return left.size_ < size;
        });
    if (found_it != item_array.end()) {
      item_idx = found_it - item_array.begin();
    } else {
      item_idx = -1;
    }
  }
  return ret;
}

/******************             IOBenchRunner              **********************/

ObIOBenchRunner::ObIOBenchRunner()
  : is_inited_(false),
    block_handles_(),
    load_(),
    tg_id_(-1),
    io_count_(0),
    rt_us_(0),
    write_buf_(nullptr)
{

}

ObIOBenchRunner::~ObIOBenchRunner()
{
  destroy();
}

int ObIOBenchRunner::init(const int64_t block_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(block_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(block_count));
  } else if (OB_ISNULL(write_buf_ = static_cast<char *>(ob_malloc(OB_DEFAULT_MACRO_BLOCK_SIZE, "io_calibration")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    // prepare macro blocks
    for (int64_t i = 0; OB_SUCC(ret) && i < block_count; ++i) {
      blocksstable::ObMacroBlockHandle block_handle;
      if (OB_FAIL(OB_SERVER_BLOCK_MGR.alloc_block(block_handle))) {
        LOG_WARN("alloc macro block failed", K(ret), K(block_count), K(i));
      } else if (OB_FAIL(block_handles_.push_back(block_handle))) {
        LOG_WARN("push back block handle failed", K(ret), K(block_count), K(i), K(block_handle));
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

int ObIOBenchRunner::do_benchmark(const ObIOBenchLoad &load, const int64_t thread_count, ObIOBenchResult &result)
{
  int ret = OB_SUCCESS;
  result.reset();
  const int64_t BENCHMARK_TIMEOUT_S = 5L; // 5s
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!load.is_valid() || thread_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(load), K(thread_count));
  } else {
    load_ = load;
    io_count_ = 0;
    rt_us_ = 0;
    if (OB_FAIL(TG_CREATE(TGDefIDs::IO_BENCHMARK, tg_id_))) {
      LOG_WARN("create thread group failed", K(ret));
    } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
      LOG_WARN("set tg_runnable failed", K(ret), K(tg_id_));
    } else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_count))) {
      LOG_WARN("set thread count failed", K(ret), K(thread_count));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("start thread failed", K(ret));
    } else {
      sleep(BENCHMARK_TIMEOUT_S);
      TG_STOP(tg_id_);
      TG_WAIT(tg_id_);
      if (io_count_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid io count", K(ret), K(io_count_));
      } else {
        result.mode_ = load_.mode_;
        result.size_ = load_.size_;
        result.iops_ = io_count_ / BENCHMARK_TIMEOUT_S;
        result.rt_us_ = rt_us_ / io_count_;

      }
      LOG_INFO("IO BENCHMARK finished", K(ret), K_(load), K(result));
    }
  }
  return ret;
}

void ObIOBenchRunner::destroy()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    tg_id_ = -1;
  }
  if (nullptr != write_buf_) {
    ob_free(write_buf_);
    write_buf_ = nullptr;
  }
  is_inited_ = false;
  block_handles_.reset();
  load_.reset();
  io_count_ = 0;
  rt_us_ = 0;
}

void ObIOBenchRunner::run1()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(block_handles_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("block not ready", K(ret), K(block_handles_.count()));
  } else {
    ObIOInfo io_info;
    io_info.tenant_id_ = OB_SERVER_TENANT_ID;
    io_info.size_ = load_.size_;
    io_info.buf_ = ObIOMode::READ == load_.mode_ ? nullptr : write_buf_;
    io_info.flag_.set_mode(load_.mode_);
    io_info.flag_.set_group_id(ObIOModule::CALIBRATION_IO);
    io_info.flag_.set_wait_event(ObIOMode::READ == load_.mode_ ?
        ObWaitEventIds::DB_FILE_DATA_READ : ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    io_info.flag_.set_unlimited(true);
    ObIOHandle io_handle;
    while (!has_set_stop()) {
      int64_t block_idx = ObRandom::rand(0, block_handles_.count() - 1);
      io_info.fd_.first_id_ = block_handles_[block_idx].get_macro_id().first_id();
      io_info.fd_.second_id_ = block_handles_[block_idx].get_macro_id().second_id();
      io_info.offset_ = ObRandom::rand(0, OB_DEFAULT_MACRO_BLOCK_SIZE - load_.size_);
      if (ObIOMode::WRITE == load_.mode_) {
        io_info.offset_ = lower_align(io_info.offset_, DIO_READ_ALIGN_SIZE);
      }
      const int64_t begin_ts = ObTimeUtility::fast_current_time();
      if (ObIOMode::READ == load_.mode_) {
        if (OB_FAIL(OB_IO_MANAGER.read(io_info, io_handle))) {
          LOG_WARN("io benchmark read failed", K(ret), K(io_info));
        }
      } else {
        if (OB_FAIL(OB_IO_MANAGER.write(io_info))) {
          LOG_WARN("io benchmark write failed", K(ret), K(io_info));
        }
      }
      if (OB_SUCC(ret)) {
        const int64_t rt = ObTimeUtility::fast_current_time() - begin_ts;
        ATOMIC_INC(&io_count_);
        ATOMIC_AAF(&rt_us_, rt);
      }
    }
  }
}

/******************             IOBenchController              **********************/

ObIOBenchController::ObIOBenchController()
  : tg_id_(-1), running_mutex_(), start_ts_(0), finish_ts_(0), ret_code_(OB_SUCCESS)
{

}

ObIOBenchController::~ObIOBenchController()
{
  if (tg_id_ >= 0) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
    tg_id_ = -1;
  }
}

int ObIOBenchController::start_io_bench()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(running_mutex_.trylock())) {
    if (OB_UNLIKELY(OB_EAGAIN != ret)) {
      LOG_WARN("try lock failed", K(ret));
    } else {
      // benchmark is running, ignore this request
      ret = OB_SUCCESS;
    }
  } else {
    if (OB_FAIL(TG_CREATE(TGDefIDs::IO_BENCHMARK, tg_id_))) {
      LOG_WARN("create tg failed", K(ret));
    } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(tg_id_, *this))) {
      LOG_WARN("start thread failed", K(ret), K(tg_id_));
    }
    int tmp_ret = running_mutex_.unlock();
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("unlock running_mutex failed", K(ret));
    }
  }
  return ret;
}

void ObIOBenchController::run1()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(running_mutex_);
  ObIOBenchRunner runner;
  start_ts_ = ObTimeUtility::fast_current_time();
  finish_ts_ = 0;
  ret_code_ = OB_SUCCESS;
  // prepare io bench runner
  const double MIN_FREE_SPACE_PERCENTAGE = 0.1; // if auto extend is on, _datafile_usage_upper_bound_percentage maybe less than (1 - 0.1 = 0.9), may cause OB_SERVER_OUTOF_DISK_SPACE
  const int64_t MIN_CALIBRATION_BLOCK_COUNT = 1024L * 1024L * 1024L / OB_DEFAULT_MACRO_BLOCK_SIZE;
  const int64_t MAX_CALIBRATION_BLOCK_COUNT = 20L * 1024L * 1024L * 1024L / OB_DEFAULT_MACRO_BLOCK_SIZE;
  const int64_t free_block_count = OB_SERVER_BLOCK_MGR.get_free_macro_block_count();
  const int64_t total_block_count = OB_SERVER_BLOCK_MGR.get_total_macro_block_count();
  if (free_block_count <= MIN_CALIBRATION_BLOCK_COUNT
      || 1.0 * free_block_count / total_block_count < MIN_FREE_SPACE_PERCENTAGE) {
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_WARN("out of space", K(ret), K(free_block_count), K(total_block_count));
  } else {
    int64_t benchmark_block_count = free_block_count * 0.2;
    benchmark_block_count = min(benchmark_block_count, MAX_CALIBRATION_BLOCK_COUNT);
    benchmark_block_count = max(benchmark_block_count, MIN_CALIBRATION_BLOCK_COUNT);
    if (OB_FAIL(runner.init(benchmark_block_count))) {
      LOG_WARN("init benchmark runner failed", K(ret), K(benchmark_block_count));
    }
  }
  // execute io benchmark
  const int64_t bench_start_size = 4096;
  const int64_t bench_thread_count = 16;
  ObIOAbility io_ability;
  for (int64_t i = 0; OB_SUCC(ret) && !has_set_stop() && i < static_cast<int64_t>(ObIOMode::MAX_MODE); ++i) {
    for (int64_t size = bench_start_size; OB_SUCC(ret) && !has_set_stop() && size <= OB_DEFAULT_MACRO_BLOCK_SIZE; size *= 2) {
      LOG_INFO("execute disk io benchmark", K(size), "mode", i);
      ObIOBenchLoad load;
      load.mode_ = static_cast<ObIOMode>(i);
      load.size_ = size;
      ObIOBenchResult result;
      if (OB_FAIL(runner.do_benchmark(load, bench_thread_count, result))) {
        LOG_WARN("run io benchmark failed", K(ret), K(load), K(bench_thread_count), K(result));
      } else if (OB_UNLIKELY(!result.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_WARN("benchmark result it invalid", K(ret), K(result));
      } else if (OB_FAIL(io_ability.add_measure_item(result))) {
        LOG_WARN("add benchmark result failed", K(ret), K(result));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObMySQLTransaction trans;
    if (!io_ability.is_valid()) {
      ret = OB_ERR_SYS;
      LOG_WARN("io ability from benchmark is invalid", K(ret), K(io_ability));
    } else if (OB_FAIL(trans.start(&OBSERVER.get_mysql_proxy(), OB_SYS_TENANT_ID))) {
      LOG_WARN("start transaction failed", K(ret));
    } else {
      const ObAddr &self_addr = OBSERVER.get_self();
      if (OB_FAIL(ObIOCalibration::get_instance().write_into_table(trans, self_addr, io_ability))) {
        LOG_WARN("write io calibration data failed", K(ret), K(self_addr), K(io_ability));
      }
      bool is_commit = OB_SUCCESS == ret;
      int tmp_ret = trans.end(is_commit);
      if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(is_commit));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIOCalibration::get_instance().update_io_ability(io_ability))) {
      LOG_WARN("update io ability failed", K(ret));
    }
  }
  ret_code_ = ret;
  finish_ts_ = ObTimeUtility::fast_current_time();
}

int64_t ObIOBenchController::get_start_timestamp()
{
  return start_ts_;
}

int64_t ObIOBenchController::get_finish_timestamp()
{
  return finish_ts_;
}

int ObIOBenchController::get_ret_code()
{
  return ret_code_;
}

/******************             IOCalibration              **********************/

ObIOCalibration::ObIOCalibration()
  : is_inited_(false),
    baseline_iops_(0),
    io_ability_(),
    lock_()
{
}

ObIOCalibration::~ObIOCalibration()
{
  destroy();
}

ObIOCalibration &ObIOCalibration::get_instance()
{
  static ObIOCalibration instance;
  return instance;
}

int ObIOCalibration::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("io calibration init twice", K(ret), K(is_inited_));
  } else {
    is_inited_ = true;
    (void)read_from_table();//ignore ret
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObIOCalibration::destroy()
{
  is_inited_ = false;
  baseline_iops_ = 0;
  io_ability_.reset();
}

int ObIOCalibration::update_io_ability(const ObIOAbility &io_ability)
{
  int ret = OB_SUCCESS;
  double tmp_baseline_iops = baseline_iops_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io calibration not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!io_ability.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(io_ability));
  } else if (OB_FAIL(io_ability.get_iops(BASELINE_IO_MODE, BASELINE_IO_SIZE, tmp_baseline_iops))) {
    LOG_WARN("get baseline iops failed", K(ret));
  } else if (tmp_baseline_iops < std::numeric_limits<double>::epsilon()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid baseline iops", K(ret), K(tmp_baseline_iops));
  } else {
    DRWLock::WRLockGuard guard(lock_);
    if (OB_FAIL(io_ability_.assign(io_ability))) {
      LOG_WARN("assign io ability failed", K(ret));
    } else {
      baseline_iops_ = tmp_baseline_iops;
    }
  }
  return ret;
}

int ObIOCalibration::reset_io_ability()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io calibration not init", K(ret), K(is_inited_));
  } else {
    DRWLock::WRLockGuard guard(lock_);
    io_ability_.reset();
  }
  return ret;
}

int ObIOCalibration::get_io_ability(ObIOAbility &io_ability)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io calibration not init", K(ret), K(is_inited_));
  } else {
    DRWLock::RDLockGuard guard(lock_);
    if (OB_FAIL(io_ability.assign(io_ability_))) {
      LOG_WARN("assign io ability failed", K(ret), K(io_ability_));
    }
  }
  return ret;
}

int ObIOCalibration::get_iops_scale(const ObIOMode mode, const int64_t size, double &iops_scale, bool &is_io_ability_valid)
{
  int ret = OB_SUCCESS;
  iops_scale = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(mode >= ObIOMode::MAX_MODE || size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mode), K(size));
  } else {
    DRWLock::RDLockGuard guard(lock_);
    if (!io_ability_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      is_io_ability_valid = false;
    } else {
      double iops = 0;
      if (OB_FAIL(io_ability_.get_iops(mode, size, iops))) {
        LOG_WARN("get iops failed", K(ret), K(mode), K(size));
      } else {
        iops_scale = iops / baseline_iops_;
      }
    }
  }
  if (OB_FAIL(ret)) {
    iops_scale = 1.0 * BASELINE_IO_SIZE / size; // assume fixed bandwidth
  }
  return OB_SUCCESS; // always success
}

int ObIOCalibration::read_from_table()
{
  int ret = OB_SUCCESS;
  ObIOAbility tmp_ability;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io calibration not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(parse_calibration_table(tmp_ability))) {
    LOG_WARN("parse calibration data failed", K(ret));
  } else if (tmp_ability.is_valid()) {
    if (OB_FAIL(update_io_ability(tmp_ability))) {
      LOG_WARN("update io ability failed", K(ret), K(tmp_ability));
    }
  } else {
    if (OB_FAIL(reset_io_ability())) {
      LOG_WARN("reset io ability failed", K(ret));
    }
  }
  return ret;
}

int ObIOCalibration::write_into_table(ObMySQLTransaction &trans, const ObAddr &addr, const ObIOAbility &io_ability)
{
  int ret = OB_SUCCESS;
  // if the io_ability is invalid, only delete the calibration data,
  // otherwise replace the calibration data
  ObSqlString delete_sql, insert_sql;
  int64_t affected_rows = 0;
  char ip_str[32] = { 0 };
  if (OB_UNLIKELY(!trans.is_started() || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(trans.is_started()), K(addr));
  } else if (OB_UNLIKELY(!addr.ip_to_string(ip_str, sizeof(ip_str)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get self ip string failed", K(ret));
  } else if (OB_FAIL(delete_sql.append_fmt(
          "delete from %s where svr_ip = \"%s\" and svr_port = %d and storage_name = \"DATA\"",
          share::OB_ALL_DISK_IO_CALIBRATION_TNAME, ip_str, addr.get_port()))) {
    LOG_WARN("append delete sql failed", K(ret), K(addr));
  } else if (OB_FAIL(trans.write(delete_sql.ptr(), affected_rows))) {
    LOG_WARN("execute delete sql failed", K(ret), K(delete_sql));
  } else if (!io_ability.is_valid()) {
    // no need to execute insert sql, skip
  } else if (OB_FAIL(insert_sql.append_fmt("insert into %s (svr_ip, svr_port, storage_name, mode, size, latency, iops) values ", share::OB_ALL_DISK_IO_CALIBRATION_TNAME))) {
    LOG_WARN("append insert sql failed", K(ret));
  } else {
    bool need_comma = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(ObIOMode::MAX_MODE); ++i) {
      const ObIArray<ObIOBenchResult> &bench_items = io_ability.get_measure_items(static_cast<ObIOMode>(i));
      for (int64_t j = 0; OB_SUCC(ret) && j < bench_items.count(); ++j) {
        const ObIOBenchResult &item = bench_items.at(j);
        if (OB_FAIL(insert_sql.append_fmt("%s (\"%s\", %d, \"%s\", \"%s\", %ld, %ld, %ld)",
                need_comma ? ", ": " ", ip_str, addr.get_port(),
                "DATA", common::get_io_mode_string(static_cast<ObIOMode>(i)),
                item.size_, static_cast<int64_t>(item.rt_us_), static_cast<int64_t>(item.iops_)))) {
          LOG_WARN("append write sql failed", K(ret), K(item));
        }
        need_comma = true;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.write(insert_sql.ptr(), affected_rows))) {
        LOG_WARN("execute insert sql failed", K(ret));
      }
    }
  }
  return ret;
}

int ObIOCalibration::refresh(const bool only_refresh, const ObIArray<ObIOBenchResult> &items)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io calibration not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(only_refresh && items.count() > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(only_refresh), K(items.count()));
  } else if (only_refresh) {
    if (OB_FAIL(read_from_table())) {
      LOG_WARN("read io calibration table failed", K(ret));
    }
  } else if (items.count() > 0) {
    ObIOAbility io_ability;
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      const ObIOBenchResult &item = items.at(i);
      if (OB_FAIL(io_ability.add_measure_item(item))) {
        LOG_WARN("add item failed", K(ret), K(i), K(item));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!io_ability.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(io_ability));
      } else if (OB_FAIL(update_io_ability(io_ability))) {
        LOG_WARN("update io ability failed", K(ret), K(io_ability));
      }
    }
  } else {
    if (OB_FAIL(reset_io_ability())) {
      LOG_WARN("reset io ability failed", K(ret));
    }
  }
  LOG_INFO("refresh io calibration", K(ret), K(only_refresh), K(items), K(io_ability_));
  return ret;
}

int ObIOCalibration::execute_benchmark()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(benchmark_controller_.start_io_bench())) {
    LOG_WARN("start io benchmark failed", K(ret));
  }
  return ret;
}

int ObIOCalibration::get_benchmark_status(int64_t &start_ts, int64_t &finish_ts, int &ret_code)
{
  int ret = OB_SUCCESS;
  start_ts = benchmark_controller_.get_start_timestamp();
  finish_ts = benchmark_controller_.get_finish_timestamp();
  ret_code = benchmark_controller_.get_ret_code();
  return ret;
}

int ObIOCalibration::parse_calibration_table(ObIOAbility &io_ability)
{
  int ret = OB_SUCCESS;
  io_ability.reset();
  sqlclient::ObMySQLResult *result = nullptr;
  SMART_VAR(ObISQLClient::ReadResult, res) {
    ObSqlString sql_string;
    char ip_str[INET6_ADDRSTRLEN] = { 0 };
    const ObAddr &self_addr = OBSERVER.get_self();
    if (OB_UNLIKELY(!self_addr.ip_to_string(ip_str, sizeof(ip_str)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get self ip string failed", K(ret));
    } else if (OB_FAIL(sql_string.append_fmt(
            "select mode, size, latency, iops from %s where svr_ip = \"%s\" and svr_port = %d and storage_name = \"DATA\"",
            share::OB_ALL_DISK_IO_CALIBRATION_TNAME, ip_str, self_addr.get_port()))) {
      LOG_WARN("generate sql string failed", K(ret), K(self_addr));
    } else if (OB_FAIL(OBSERVER.get_mysql_proxy().read(res, sql_string.ptr()))) {
      LOG_WARN("query failed", K(ret), K(sql_string));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret), KP(result));
    } else {
      while (OB_SUCC(result->next())) {
        ObIOBenchResult item;
        ObString mode_string;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "mode", mode_string);
        EXTRACT_INT_FIELD_MYSQL(*result, "size", item.size_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "latency", item.rt_us_, double);
        EXTRACT_INT_FIELD_MYSQL(*result, "iops", item.iops_, double);
        if (OB_FAIL(ret)) {
        } else if (FALSE_IT(item.mode_ = get_io_mode_enum(mode_string.ptr()))) {
        } else if (OB_UNLIKELY(!item.is_valid())) {
          ret = OB_ERR_SYS;
          LOG_WARN("calibration data is invalid", K(ret), K(item), K(mode_string));
        } else if (OB_FAIL(io_ability.add_measure_item(item))) {
          LOG_WARN("add item failed", K(ret), K(item));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObIOCalibration::parse_calibration_string(const ObString &calibration_string, ObIOBenchResult &item)
{
  int ret = OB_SUCCESS;
  // format: mode:size:latency:iops
  char mode_str[64] = { 0 };
  char size_str[64] = { 0 };
  char latency_str[64] = { 0 };
  bool is_valid = false;
  const int64_t MAX_IO_CALIBRAITON_STRING_LENGTH = 256;
  if (OB_UNLIKELY(calibration_string.empty()
        || calibration_string.length() >= MAX_IO_CALIBRAITON_STRING_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calibration string is empty", K(ret), K(calibration_string));
  } else {
    // duplicate and replace ':' with ' '
    char dup_str[MAX_IO_CALIBRAITON_STRING_LENGTH] = { 0 };
    strncpy(dup_str, calibration_string.ptr(), sizeof(dup_str) - 1);
    for (int64_t i = 0; i < sizeof(dup_str); ++i) {
      if (':' == dup_str[i]) {
        dup_str[i] = ' ';
      }
    }
    int scan_ret = sscanf(dup_str, "%s %s %s %lf", mode_str, size_str, latency_str, &item.iops_);
    if (OB_UNLIKELY(4 != scan_ret)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(scan_ret), K(calibration_string));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (item.iops_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid iops string", K(ret), K(calibration_string), K(item.iops_));
  } else if (FALSE_IT(item.mode_ = get_io_mode_enum(mode_str))) {
  } else if (item.mode_ >= ObIOMode::MAX_MODE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mode name", K(ret), K(mode_str), K(item.mode_));
  } else if (FALSE_IT(item.size_ = ObConfigCapacityParser::get(size_str, is_valid))) {
  } else if (!is_valid) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid size string", K(ret), K(calibration_string), K(size_str));
  } else if (FALSE_IT(item.rt_us_ = ObConfigTimeParser::get(latency_str, is_valid))) {
  } else if (!is_valid) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid latency string", K(ret), K(calibration_string), K(latency_str));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(calibration_string), K(item));
  }
  return ret;
}


