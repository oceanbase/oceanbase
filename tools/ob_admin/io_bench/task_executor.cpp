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

#include "share/backup/ob_backup_io_adapter.h"
#include "task_executor.h"
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{

/*--------------------------------TaskConfig--------------------------------*/
TaskConfig::TaskConfig()
    : thread_num_(-1), max_task_runs_(-1), time_limit_s_(-1),
      obj_size_(-1), obj_num_(-1), fragment_size_(-1), is_adaptive_(false),
      type_(BenchmarkTaskType::BENCHMARK_TASK_MAX_TYPE)
{
}

int TaskConfig::assign(const TaskConfig &other)
{
  int ret = OB_SUCCESS;
  thread_num_ = other.thread_num_;
  max_task_runs_ = other.max_task_runs_;
  time_limit_s_ = other.time_limit_s_;
  obj_size_ = other.obj_size_;
  obj_num_ = other.obj_num_;
  fragment_size_ = other.fragment_size_;
  is_adaptive_ = other.is_adaptive_;
  type_ = other.type_;
  return ret;
}

/*--------------------------------Metrics--------------------------------*/
TimeMap::TimeMap() : total_entry_(0)
{
}

int TimeMap::log_entry(const int64_t start_time_us)
{
  int ret = OB_SUCCESS;
  const int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time_us) / 1000;
  time_ms_map_[cost_time_ms]++;
  total_entry_++;
  return ret;
}

int TimeMap::add(const TimeMap &other)
{
  int ret = OB_SUCCESS;
  std::map<int64_t, int64_t>::const_iterator it = other.time_ms_map_.begin();
  while (OB_SUCC(ret) && it != other.time_ms_map_.end()) {
    time_ms_map_[it->first] += it->second;
    ++it;
  }
  if (OB_SUCC(ret)) {
    total_entry_ += other.total_entry_;
  }
  return ret;
}

void TimeMap::summary(const char *map_name_str)
{
  const char *map_name = "Anonymous Time Map";
  if (OB_NOT_NULL(map_name_str)) {
    map_name = map_name_str;
  }
  if (total_entry_ <= 0) {
    PrintHelper::print_dump_line(map_name, "Empty Time Map");
  } else {
    const int64_t th_50_count = total_entry_ * 0.5;
    const int64_t th_90_count = total_entry_ * 0.9;
    const int64_t th_99_count = total_entry_ * 0.99;
    const int64_t th_999_count = total_entry_ * 0.999;
    int64_t min_ms = 0;
    int64_t th_50_ms = 0;
    int64_t th_90_ms = 0;
    int64_t th_99_ms = 0;
    int64_t th_999_ms = 0;
    int64_t cur_count = 0;
    int64_t max_ms = 0;

    std::map<int64_t, int64_t>::const_iterator it = time_ms_map_.begin();
    min_ms = it->first;
    while (it != time_ms_map_.end()) {
      cur_count += it->second;
      if (th_50_ms == 0 && cur_count >= th_50_count) {
        th_50_ms = it->first;
      }
      if (th_90_ms == 0 && cur_count >= th_90_count) {
        th_90_ms = it->first;
      }
      if (th_99_ms == 0 && cur_count >= th_99_count) {
        th_99_ms = it->first;
      }
      if (th_999_ms == 0 && cur_count >= th_999_count) {
        th_999_ms = it->first;
      }
      if (max_ms == 0 && cur_count == total_entry_) {
        max_ms = it->first;
      }

      ++it;
    }

    char buf[2048];
    int ret = OB_SUCCESS;
    if (OB_FAIL(databuff_printf(buf, sizeof(buf),
        "total_entry=%ld, min_ms=%ld, th_50_ms=%ld, th_90_ms=%ld, th_99_ms=%ld, th_999_ms=%ld, max_ms=%ld",
        total_entry_, min_ms, th_50_ms, th_90_ms,th_99_ms, th_999_ms, max_ms))) {
      OB_LOG(WARN, "fail to set log str", K(ret), K_(total_entry),
          K(min_ms), K(th_50_ms), K(th_90_ms), K(th_99_ms), K(th_999_ms), K(max_ms));
    } else {
      OB_LOG(INFO, "time map status", K(ret), K_(total_entry),
          K(min_ms), K(th_50_ms), K(th_90_ms), K(th_99_ms), K(th_999_ms), K(max_ms));
      PrintHelper::print_dump_line(map_name, buf);
    }
  }
}

int TimeMap::assign(const TimeMap &other)
{
  int ret = OB_SUCCESS;
  total_entry_ = other.total_entry_;
  time_ms_map_ = other.time_ms_map_;
  return ret;
}

Metrics::Metrics()
    : status_(OB_SUCCESS), throughput_bytes_(0), operation_num_(0),
      total_op_time_ms_map_(), open_time_ms_map_(), close_time_ms_map_()
{
}

int Metrics::assign(const Metrics &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(total_op_time_ms_map_.assign(other.total_op_time_ms_map_))) {
    OB_LOG(WARN, "fail to assign total_op_time_ms_map", K(ret));
  } else if (OB_FAIL(open_time_ms_map_.assign(other.open_time_ms_map_))) {
    OB_LOG(WARN, "fail to assign open_time_ms_map", K(ret));
  } else if (OB_FAIL(close_time_ms_map_.assign(other.close_time_ms_map_))) {
    OB_LOG(WARN, "fail to assign close_time_ms_map", K(ret));
  } else {
    status_ = other.status_;
    throughput_bytes_ = other.throughput_bytes_;
    operation_num_ = other.operation_num_;
  }
  return ret;
}

int Metrics::add(const Metrics &other)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(status_)) {
    if (OB_SUCC(other.status_)) {
      if (OB_FAIL(total_op_time_ms_map_.add(other.total_op_time_ms_map_))) {
        OB_LOG(WARN, "fail to add total_op_time_ms_map", K(ret));
      } else if (OB_FAIL(open_time_ms_map_.add(other.open_time_ms_map_))) {
        OB_LOG(WARN, "fail to add open_time_ms_map", K(ret));
      } else if (OB_FAIL(close_time_ms_map_.add(other.close_time_ms_map_))) {
        OB_LOG(WARN, "fail to add close_time_ms_map", K(ret));
      } else {
        throughput_bytes_ += other.throughput_bytes_;
        operation_num_ += other.operation_num_;
      }
   } else {
     status_ = other.status_;
   }
  }
  return ret;
}

static double cal_time_diff(const timeval& start, const timeval& end)
{
  return (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1e6;
}

void Metrics::summary(
    const struct timeval &start_real_time,
    const struct rusage &start_usage,
    const int64_t thread_num)
{
  PrintHelper::print_dump_line("Status", status_ == OB_SUCCESS ? "SUCCESS" : "FAIL");
  if (OB_LIKELY(status_ == OB_SUCCESS)) {
    if (OB_UNLIKELY(operation_num_ <= 0)) {
      PrintHelper::print_dump_line("Operation num is unexpected", operation_num_);
    } else {
      struct rusage end_usage;
      struct timeval end_real_time;
      getrusage(RUSAGE_SELF, &end_usage);
      gettimeofday(&end_real_time, nullptr);

      const double cost_time_s = cal_time_diff(start_real_time, end_real_time);
      const double user_cpu_time_s = cal_time_diff(start_usage.ru_utime, end_usage.ru_utime);
      const double sys_cpu_time_s = cal_time_diff(start_usage.ru_stime, end_usage.ru_stime);
      const double cpu_usage = (user_cpu_time_s + sys_cpu_time_s) / (cost_time_s) * 100.0;
      const double QPS = ((double)operation_num_) / cost_time_s;
      const double BW = ( ((double)throughput_bytes_) / 1024.0 / 1024.0 ) / cost_time_s;

      PrintHelper::print_dump_line("Total operation num", operation_num_);
      PrintHelper::print_dump_line("Total execution time", (std::to_string(cost_time_s) + " s").c_str());
      PrintHelper::print_dump_line("Total user time", (std::to_string(user_cpu_time_s) + " s").c_str());
      PrintHelper::print_dump_line("Total system time", (std::to_string(sys_cpu_time_s) + " s").c_str());
      if (BW > 1e-6) {
        const double cpu_usage_for_100MB_bw = (100.0 / BW) * cpu_usage;
        PrintHelper::print_dump_line("CPU usage for 100MB/s BW",
            (std::to_string(cpu_usage_for_100MB_bw) + "% per 100MB/s").c_str());
      } else {
        PrintHelper::print_dump_line("Total CPU usage", (std::to_string(cpu_usage) + "%").c_str());
      }
      PrintHelper::print_dump_line("Total throughput bytes", throughput_bytes_);
      PrintHelper::print_dump_line("Total QPS", std::to_string(QPS).c_str());
      PrintHelper::print_dump_line("Per Thread QPS", std::to_string(QPS / thread_num).c_str());
      PrintHelper::print_dump_line("Total BW", (std::to_string(BW) + " MB/s").c_str());
      PrintHelper::print_dump_line("Per Thread BW", (std::to_string(BW / thread_num) + " MB/s").c_str());
      total_op_time_ms_map_.summary("Total Op Time Map");
      open_time_ms_map_.summary("Open Time Map");
      close_time_ms_map_.summary("Close Op Time Map");
    }
  }
}

/*--------------------------------ITaskExecutor--------------------------------*/
ITaskExecutor::ITaskExecutor()
    : is_inited_(false), base_uri_len_(-1), storage_info_(nullptr), metrics_()
{
  base_uri_[0] = '\0';
}

void ITaskExecutor::reset()
{
  is_inited_ = false;
  base_uri_len_ = -1;
  storage_info_ = nullptr;
  base_uri_[0] = '\0';
}

int ITaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "Task Executor init twice", K(ret));
  } else if (OB_ISNULL(base_uri) || OB_ISNULL(storage_info)
      || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(base_uri), KPC(storage_info));
  } else if (OB_FAIL(databuff_printf(base_uri_, sizeof(base_uri_), "%s", base_uri))) {
    OB_LOG(WARN, "fail to deep copy base uri", K(ret), K(base_uri));
  } else {
    base_uri_len_ = strlen(base_uri_);
    storage_info_ = storage_info;
    is_inited_ = true;
  }
  return ret;
}

int ITaskExecutor::prepare_(const int64_t object_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = base_uri_len_;
  if (OB_FAIL(databuff_printf(base_uri_, sizeof(base_uri_), pos, "/%ld", object_id))) {
    OB_LOG(WARN, "fail to construct object name", K(ret), K_(base_uri), K(object_id));
  }
  return ret;
}

void ITaskExecutor::finish_(const int64_t ob_errcode)
{
  metrics_.status_ = ob_errcode;
  base_uri_[base_uri_len_] = '\0';
}

template<typename Executor>
typename std::enable_if<std::is_base_of<ITaskExecutor, Executor>::value, int>::type
alloc_executor(ITaskExecutor *&executor, ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = ob_malloc(sizeof(Executor), attr))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN,"fail to alloc buf for task executor", K(ret));
  } else {
    executor = new(buf) Executor();
  }
  return ret;
}

int init_task_executor(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config, ITaskExecutor *&executor)
{
  int ret = OB_SUCCESS;
  executor = nullptr;
  void *buf = nullptr;
  ObMemAttr attr;
  attr.label_ = "TMPSTR";
  if (OB_ISNULL(base_uri) || OB_ISNULL(storage_info)
      || OB_UNLIKELY(!storage_info->is_valid() || config.type_ == BenchmarkTaskType::BENCHMARK_TASK_MAX_TYPE)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (config.type_ == BenchmarkTaskType::BENCHMARK_TASK_NORMAL_WRITE) {
    if (OB_FAIL(alloc_executor<WriteTaskExecutor>(executor, attr))) {
      OB_LOG(WARN, "fail to alloc and construct executor", K(ret), K(config));
    }
  } else if (config.type_ == BenchmarkTaskType::BENCHMARK_TASK_READ) {
    if (OB_FAIL(alloc_executor<ReadTaskExecutor>(executor, attr))) {
      OB_LOG(WARN, "fail to alloc and construct executor", K(ret), K(config));
    }
  } else if (config.type_ == BenchmarkTaskType::BENCHMARK_TASK_APPEND_WRITE) {
    if (OB_FAIL(alloc_executor<AppendWriteTaskExecutor>(executor, attr))) {
      OB_LOG(WARN, "fail to alloc and construct executor", K(ret), K(config));
    }
  } else if (config.type_ == BenchmarkTaskType::BENCHMARK_TASK_MULTIPART_WRITE) {
    if (OB_FAIL(alloc_executor<MultipartWriteTaskExecutor>(executor, attr))) {
      OB_LOG(WARN, "fail to alloc and construct executor", K(ret), K(config));
    }
  } else if (config.type_ == BenchmarkTaskType::BENCHMARK_TASK_DEL) {
    if (OB_FAIL(alloc_executor<DelTaskExecutor>(executor, attr))) {
      OB_LOG(WARN, "fail to alloc and construct executor", K(ret), K(config));
    }
  } else if (config.type_ == BenchmarkTaskType::BENCHMARK_TASK_IS_EXIST) {
    if (OB_FAIL(alloc_executor<IsExistTaskExecutor>(executor, attr))) {
      OB_LOG(WARN, "fail to alloc and construct executor", K(ret), K(config));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "unknown benchmark task type", K(ret), K(config));
  }

  if (FAILEDx(executor->init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init executor", K(ret), K(base_uri), KPC(storage_info), K(config));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(executor)) {
      executor->~ITaskExecutor();
      ob_free(executor);
      executor = nullptr;
    }
  }
  return ret;
}

/*--------------------------------Write Task Executor--------------------------------*/
static const int64_t MAX_RANDOM_CONTENT_LEN = 128 * 1024 * 1024L;
static char RANDOM_CONTENT[MAX_RANDOM_CONTENT_LEN + 1];
void init_random_content()
{
  static bool is_inited = false;
  if (!is_inited) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    for (int64_t i = 0; i < MAX_RANDOM_CONTENT_LEN; i++) {
      RANDOM_CONTENT[i] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
    }
    RANDOM_CONTENT[MAX_RANDOM_CONTENT_LEN] = '\0';
    is_inited = true;
  }
}

WriteTaskExecutor::WriteTaskExecutor()
    : ITaskExecutor(), obj_size_(-1), write_buf_(nullptr), allocator_()
{
}

void WriteTaskExecutor::reset()
{
  obj_size_ = -1;
  write_buf_ = nullptr;
  allocator_.clear();
  ITaskExecutor::reset();
}

int WriteTaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ITaskExecutor::init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init ITaskExecutor", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (OB_UNLIKELY(config.obj_size_ <= 0 || config.obj_size_ >= MAX_RANDOM_CONTENT_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(config));
  } else if (FALSE_IT(obj_size_ = config.obj_size_)) {
  } else if (OB_ISNULL(write_buf_ = (char *)allocator_.alloc(obj_size_ + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory for write buf", K(ret), K_(obj_size));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int WriteTaskExecutor::execute()
{
  int ret = OB_SUCCESS;
  const int64_t object_id = metrics_.operation_num_;
  const int64_t start_time_us = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "WriteTaskExecutor not init", K(ret), K_(base_uri));
  } else if (OB_FAIL(prepare_(object_id))) {
    OB_LOG(WARN, "fail to prepare", K(ret), K_(base_uri), K(object_id));
  } else {
    ObBackupIoAdapter adapter;
    MEMCPY(write_buf_,
           RANDOM_CONTENT + ObRandom::rand(0, MAX_RANDOM_CONTENT_LEN - obj_size_),
           obj_size_);
    write_buf_[obj_size_] = '\0';

    if (OB_FAIL(adapter.write_single_file(base_uri_, storage_info_, write_buf_, obj_size_))) {
      OB_LOG(WARN, "fail to write file",
          K(ret), K_(base_uri), KPC_(storage_info), K_(obj_size), K(object_id));
    } else {
      metrics_.operation_num_++;
      metrics_.throughput_bytes_ += obj_size_;
      metrics_.total_op_time_ms_map_.log_entry(start_time_us);
    }
  }

  finish_(ret);
  return ret;
}

AppendWriteTaskExecutor::AppendWriteTaskExecutor()
    : ITaskExecutor(), obj_size_(-1), fragment_size_(-1), write_buf_(nullptr), allocator_()
{
}

void AppendWriteTaskExecutor::reset()
{
  obj_size_ = -1;
  fragment_size_ = -1;
  write_buf_ = nullptr;
  allocator_.clear();
  ITaskExecutor::reset();
}

int AppendWriteTaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ITaskExecutor::init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init ITaskExecutor", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (OB_UNLIKELY(config.fragment_size_ <= 0 || config.fragment_size_ > config.obj_size_
      || config.fragment_size_ >= MAX_RANDOM_CONTENT_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(config));
  } else if (FALSE_IT(obj_size_ = config.obj_size_)) {
  } else if (FALSE_IT(fragment_size_ = config.fragment_size_)) {
  } else if (OB_ISNULL(write_buf_ = (char *)allocator_.alloc(fragment_size_ + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory for write buf", K(ret), K_(fragment_size));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int AppendWriteTaskExecutor::execute()
{
  int ret = OB_SUCCESS;
  const int64_t object_id = metrics_.operation_num_;
  const int64_t start_time_us = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "AppendWriteTaskExecutor not init", K(ret), K_(base_uri));
  } else if (OB_FAIL(prepare_(object_id))) {
    OB_LOG(WARN, "fail to prepare", K(ret), K_(base_uri), K(object_id));
  } else {
    ObBackupIoAdapter adapter;
    ObIOFd fd;
    ObIODevice *device_handle = nullptr;
    ObStorageAccessType access_type = OB_STORAGE_ACCESS_RANDOMWRITER;

    const int64_t open_start_time_us = ObTimeUtility::current_time();
    if (OB_FAIL(adapter.open_with_access_type(
        device_handle, fd, storage_info_, base_uri_, access_type))) {
      OB_LOG(WARN, "failed to open device with access type",
          K(ret), K_(base_uri), KPC_(storage_info), K(access_type));
    } else if (OB_ISNULL(device_handle)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "device handle is NULL", K(ret), KP(device_handle), K_(base_uri));
    } else {
      metrics_.open_time_ms_map_.log_entry(open_start_time_us);
    }

    int64_t cur_offset = 0;
    int64_t actual_write_size = -1;
    int64_t cur_append_size = -1;
    while (OB_SUCC(ret) && cur_offset < obj_size_) {
      cur_append_size = MIN(obj_size_ - cur_offset, fragment_size_);
      MEMCPY(write_buf_,
             RANDOM_CONTENT + ObRandom::rand(0, MAX_RANDOM_CONTENT_LEN - cur_append_size),
             cur_append_size);
      write_buf_[cur_append_size] = '\0';

      if (OB_FAIL(device_handle->pwrite(fd, cur_offset, cur_append_size,
                                        write_buf_, actual_write_size))) {
        OB_LOG(WARN, "fail to append object",
            K(ret), K_(base_uri), K(cur_offset), K(cur_append_size));
      } else {
        cur_offset += cur_append_size;
      }
    }

    const int64_t close_start_time_us = ObTimeUtility::current_time();
    if (FAILEDx(device_handle->seal_file(fd))) {
      OB_LOG(WARN, "fail to seal file", K(ret), K_(base_uri));
    } else if (OB_FAIL(adapter.close_device_and_fd(device_handle, fd))) {
      OB_LOG(WARN, "fail to close device handle", K(ret), K_(base_uri));
    } else {
      metrics_.close_time_ms_map_.log_entry(close_start_time_us);
    }

    if (OB_SUCC(ret)) {
      metrics_.operation_num_++;
      metrics_.throughput_bytes_ += obj_size_;
      metrics_.total_op_time_ms_map_.log_entry(start_time_us);
    }
  }

  finish_(ret);
  return ret;
}

MultipartWriteTaskExecutor::MultipartWriteTaskExecutor()
    : ITaskExecutor(), obj_size_(-1), part_size_(-1), write_buf_(nullptr), allocator_()
{
}

void MultipartWriteTaskExecutor::reset()
{
  obj_size_ = -1;
  part_size_ = -1;
  write_buf_ = nullptr;
  allocator_.clear();
  ITaskExecutor::reset();
}

int MultipartWriteTaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ITaskExecutor::init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init ITaskExecutor", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (OB_UNLIKELY(config.fragment_size_ <= 0 || config.fragment_size_ > config.obj_size_
      || config.fragment_size_ >= MAX_RANDOM_CONTENT_LEN)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(config));
  } else if (FALSE_IT(obj_size_ = config.obj_size_)) {
  } else if (FALSE_IT(part_size_ = config.fragment_size_)) {
  } else if (OB_ISNULL(write_buf_ = (char *)allocator_.alloc(part_size_ + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory for write buf", K(ret), K_(part_size));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int MultipartWriteTaskExecutor::execute()
{
  int ret = OB_SUCCESS;
  const int64_t object_id = metrics_.operation_num_;
  const int64_t start_time_us = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "MultipartWriteTaskExecutor not init", K(ret), K_(base_uri));
  } else if (OB_FAIL(prepare_(object_id))) {
    OB_LOG(WARN, "fail to prepare", K(ret), K_(base_uri), K(object_id));
  } else {
    ObBackupIoAdapter adapter;
    ObIOFd fd;
    ObIODevice *device_handle = nullptr;
    ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;

    const int64_t open_start_time_us = ObTimeUtility::current_time();
    if (OB_FAIL(adapter.open_with_access_type(
        device_handle, fd, storage_info_, base_uri_, access_type))) {
      OB_LOG(WARN, "failed to open device with access type",
          K(ret), K_(base_uri), KPC_(storage_info), K(access_type));
    } else if (OB_ISNULL(device_handle)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "device handle is NULL", K(ret), KP(device_handle), K_(base_uri));
    } else {
      metrics_.open_time_ms_map_.log_entry(open_start_time_us);
    }

    int64_t cur_offset = 0;
    int64_t actual_write_size = -1;
    int64_t cur_part_size = -1;
    while (OB_SUCC(ret) && cur_offset < obj_size_) {
      cur_part_size = MIN(obj_size_ - cur_offset, part_size_);
      MEMCPY(write_buf_,
             RANDOM_CONTENT + ObRandom::rand(0, MAX_RANDOM_CONTENT_LEN - cur_part_size),
             cur_part_size);
      write_buf_[cur_part_size] = '\0';

      if (OB_FAIL(device_handle->pwrite(fd, cur_offset, cur_part_size,
                                        write_buf_, actual_write_size))) {
        OB_LOG(WARN, "fail to upload part",
            K(ret), K_(base_uri), K(cur_offset), K(cur_part_size), K(fd));
      } else {
        cur_offset += cur_part_size;
      }
    }

    const int64_t close_start_time_us = ObTimeUtility::current_time();
    if (FAILEDx(device_handle->complete(fd))) {
      OB_LOG(WARN, "fail to complete multipart writer", K(ret), K_(base_uri), K(fd));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(adapter.close_device_and_fd(device_handle, fd))) {
      OB_LOG(WARN, "fail to close device handle", K(ret), K(tmp_ret), K_(base_uri));
    }
    metrics_.close_time_ms_map_.log_entry(close_start_time_us);

    if (OB_SUCC(ret)) {
      metrics_.operation_num_++;
      metrics_.throughput_bytes_ += obj_size_;
      metrics_.total_op_time_ms_map_.log_entry(start_time_us);
    }
  }

  finish_(ret);
  return ret;
}

/*--------------------------------Read Task Executor--------------------------------*/
ReadTaskExecutor::ReadTaskExecutor()
    : ITaskExecutor(), obj_size_(-1), obj_num_(-1), is_adaptive_(false),
      expected_read_size_(-1), read_buf_(nullptr), allocator_()
{
}

void ReadTaskExecutor::reset()
{
  obj_size_ = -1;
  obj_num_ = -1;
  is_adaptive_ = false;
  expected_read_size_ = -1;
  read_buf_ = nullptr;
  allocator_.clear();
  ITaskExecutor::reset();
}

int ReadTaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ITaskExecutor::init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init ITaskExecutor", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (OB_UNLIKELY(config.fragment_size_ <= 0 || config.fragment_size_ > config.obj_size_
      || config.obj_num_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(config));
  } else if (FALSE_IT(obj_size_ = config.obj_size_)) {
  } else if (FALSE_IT(expected_read_size_ = config.fragment_size_)) {
  } else if (OB_ISNULL(read_buf_ = (char *)allocator_.alloc(expected_read_size_ + 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory for read buf", K(ret), K_(obj_size), K_(expected_read_size));
  } else {
    obj_num_ = config.obj_num_;
    is_adaptive_ = config.is_adaptive_;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ReadTaskExecutor::execute()
{
  int ret = OB_SUCCESS;
  const int64_t object_id = ObRandom::rand(0, obj_num_ - 1);
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ReadTaskExecutor not init", K(ret), K_(base_uri));
  } else if (OB_FAIL(prepare_(object_id))) {
    OB_LOG(WARN, "fail to prepare", K(ret), K_(base_uri), K(object_id));
  } else {
    ObBackupIoAdapter adapter;
    const int64_t offset = (ObRandom::rand(0, obj_size_ - expected_read_size_) / ALIGNMENT) * ALIGNMENT;
    int64_t read_size = -1;

    if (is_adaptive_ && OB_FAIL(adapter.adaptively_read_part_file(base_uri_,
        storage_info_, read_buf_, expected_read_size_, offset, read_size))) {
      OB_LOG(WARN, "fail to read adaptive file", K(ret), K_(base_uri),
          KPC_(storage_info), K_(expected_read_size), K_(obj_size), K(offset), K(object_id));
    } else if (!is_adaptive_ && OB_FAIL(adapter.read_part_file(base_uri_,
        storage_info_, read_buf_, expected_read_size_, offset, read_size))) {
      OB_LOG(WARN, "fail to read file", K(ret), K_(base_uri),
          KPC_(storage_info), K_(expected_read_size), K_(obj_size), K(offset), K(object_id));
    } else {
      metrics_.operation_num_++;
      metrics_.throughput_bytes_ += read_size;
      metrics_.total_op_time_ms_map_.log_entry(start_time_us);
    }
  }

  finish_(ret);
  return ret;
}

/*--------------------------------Del Task Executor--------------------------------*/
DelTaskExecutor::DelTaskExecutor()
    : ITaskExecutor(), is_adaptive_(false)
{
}

int DelTaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ITaskExecutor::init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init ITaskExecutor", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else {
    is_adaptive_ = config.is_adaptive_;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int DelTaskExecutor::execute()
{
  int ret = OB_SUCCESS;
  const int64_t object_id = metrics_.operation_num_;
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "DelTaskExecutor not init", K(ret), K_(base_uri));
  } else if (OB_FAIL(prepare_(object_id))) {
    OB_LOG(WARN, "fail to prepare", K(ret), K_(base_uri), K(object_id));
  } else {
    ObBackupIoAdapter adapter;

    if (is_adaptive_ && OB_FAIL(adapter.adaptively_del_file(base_uri_, storage_info_))) {
      OB_LOG(WARN, "fail to delete adaptive file",
          K(ret), K_(base_uri), KPC_(storage_info), K(object_id));
    } else if (!is_adaptive_ && OB_FAIL(adapter.del_file(base_uri_, storage_info_))) {
      OB_LOG(WARN, "fail to delete file", K(ret), K_(base_uri), KPC_(storage_info), K(object_id));
    } else {
      metrics_.operation_num_++;
      metrics_.total_op_time_ms_map_.log_entry(start_time_us);
    }
  }

  finish_(ret);
  return ret;
}

/*--------------------------------Is Exist Task Executor--------------------------------*/
IsExistTaskExecutor::IsExistTaskExecutor()
    : ITaskExecutor(), obj_num_(-1)
{
}

int IsExistTaskExecutor::init(const char *base_uri,
    share::ObBackupStorageInfo *storage_info, const TaskConfig &config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ITaskExecutor::init(base_uri, storage_info, config))) {
    OB_LOG(WARN, "fail to init ITaskExecutor", K(ret), K(base_uri), KPC(storage_info), K(config));
  } else if (OB_UNLIKELY(config.obj_num_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), K(config));
  } else {
    obj_num_ = config.obj_num_;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int IsExistTaskExecutor::execute()
{
  int ret = OB_SUCCESS;
  const int64_t object_id = ObRandom::rand(0, obj_num_ - 1);
  const int64_t start_time_us = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "ReadTaskExecutor not init", K(ret), K_(base_uri));
  } else if (OB_FAIL(prepare_(object_id))) {
    OB_LOG(WARN, "fail to prepare", K(ret), K_(base_uri), K(object_id));
  } else {
    ObBackupIoAdapter adapter;
    bool is_exist = false;
    if (OB_FAIL(adapter.is_exist(base_uri_, storage_info_, is_exist))) {
      OB_LOG(WARN, "fail to check is exist",
          K(ret), K_(base_uri), KPC_(storage_info), K(object_id));
    } else {
      metrics_.operation_num_++;
      metrics_.total_op_time_ms_map_.log_entry(start_time_us);
    }
  }

  finish_(ret);
  return ret;
}

}   //tools
}   //oceanbase