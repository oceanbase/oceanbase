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

#include "ob_admin_object_storage_driver_quality.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{
double cal_time_diff(const timeval &start, const timeval &end)
{
  return (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1e6;
}

std::string to_string_with_precision(const double value, const int precision)
{

  std::ostringstream out;
  out << std::fixed << std::setprecision(precision) << value;
  return out.str();
}

int generate_content_by_object_id(char *buf, const int64_t buf_len, const int64_t object_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len < 0 || object_id < 0)
      || OB_UNLIKELY(buf_len > 0 && buf == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the argument is invalid", KR(ret), K(buf), K(buf_len), K(object_id));
  }
  for (int i = 0; i < buf_len && OB_SUCC(ret); i++) {
    buf[i] = (object_id + i) % INT8_MAX;
  }
  return ret;
}

int check_content_by_object_id(const char *buf, const int64_t buf_len, const int64_t object_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len < 0 || object_id < 0) || OB_UNLIKELY(buf_len > 0 && buf == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the argument is invalid", KR(ret), K(buf));
  }

  for (int i = 0; i < buf_len && OB_SUCC(ret); i++) {
    if (OB_UNLIKELY(buf[i] != (object_id + i) % INT8_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "check content failed!", KR(ret), K(i), K(buf[i]), K(object_id), K(buf_len));
    }
  }
  return ret;
}

/**
 *  @brief Get a random length for performing a write operation or a read operation
 *
 *  @return content_length Indicates the content length generated
 */
int64_t get_random_content_length(const OSDQOpType op_type)
{
  int min_object_size = MIN_OBJECT_SIZE;
  if (op_type == APPEND_WRITE) {
    min_object_size = 4;
  }
  int64_t content_length = 0;
  double rnd = 1.0 * ObRandom::rand(1, 100) / 100;
  if (rnd <= SMALL_OBJECT_SIZE_RATE) {
    content_length = ObRandom::rand(min_object_size, SMALL_OBJECT_SIZE_LIMIT);
  } else if (rnd <= SMALL_OBJECT_SIZE_RATE + NORMAL_OBJECT_SIZE_RATE) {
    content_length = ObRandom::rand(SMALL_OBJECT_SIZE_LIMIT + 1, NORMAL_OBJECT_SIZE_LIMIT);
  } else {
    content_length = ObRandom::rand(NORMAL_OBJECT_SIZE_LIMIT + 1, LARGE_OBJECT_SIZE_LIMIT);
  }
  return content_length;
}

int construct_object_name(const int64_t object_id, char *object_name, const int64_t object_name_len)
{
  static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "@=:$";
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(object_name) || OB_UNLIKELY(object_id < 0 || object_name_len != MAX_OBJECT_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument!", KR(ret), K(object_id), K(object_name_len));
  } else if (OB_FAIL(databuff_printf(object_name, object_name_len, pos, "object_%ld_", object_id))) {
    OB_LOG(WARN, "failed to databuff_printf object_name", KR(ret), K(object_id), K(object_name_len), K(pos));
  } else {
    for (int i = 0; i < MAX_OBJECT_NAME_SUFFIX_LENGTH && OB_SUCC(ret); i++) {
      if (OB_LIKELY(pos < object_name_len)) {
        object_name[pos] = alphanum[ObRandom::rand(0, sizeof(alphanum) - 2)];
        pos++;
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "pos should be smaller than MAX_OBJECT_NAME_LENGTH", KR(ret), K(i), K(pos), K(object_name_len));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_LIKELY(pos < object_name_len)) {
        object_name[pos] = '\0';
      } else {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "pos should be smaller than MAX_OBJECT_NAME_LENGTH", KR(ret), K(pos), K(object_name_len));
      }
    }
  }
  return ret;
}

int construct_file_path(
    const char *base_uri,
    const char *object_name,
    char *file_path,
    const int64_t file_path_length)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(base_uri) || OB_ISNULL(object_name) || OB_ISNULL(file_path)
      || OB_UNLIKELY(file_path_length < OB_MAX_URI_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(base_uri), K(object_name), K(file_path), K(file_path_length));
  } else if (OB_FAIL(databuff_printf(file_path, file_path_length, "%s/%s", base_uri, object_name))) {
    OB_LOG(WARN, "failed to construct object uri", KR(ret), K(base_uri), K(object_name), K(file_path_length));
  }
  return ret;
}

ObVSliceAlloc& get_vslice_alloc_instance()
{
  static ObBlockAllocMgr alloc_mgr;
  static ObMemAttr attr;
  static const int64_t DEFAULT_BLOCK_SIZE = 8 * 1024 * 1024;
  static ObVSliceAlloc allocator(attr, DEFAULT_BLOCK_SIZE, alloc_mgr);
  return allocator;
}

int64_t allocator_cnt = 0;

//============================ OSDQLogEntry ==============================

OSDQLogEntry::OSDQLogEntry()
  : is_inited_(false),
    prefix_(),
    head_holder_(),
    content_()
{
}

int OSDQLogEntry::init(const std::string &title, const std::string &color)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", KR(ret));
  } else {
    prefix_ = get_time_prefix_() + " ";
    head_holder_ = std::string(prefix_.size(), ' ');
    prefix_ += color + std::string("[") + title + std::string("]") + NONE_COLOR_PREFIX;
    is_inited_ = true;
  }
  return ret;
}

void OSDQLogEntry::print_log(const std::string &title, const std::string &content, const std::string &color)
{
  const std::string log = get_time_prefix_() + color + " [" + title + "] " + NONE_COLOR_PREFIX + content + "\n";
  std::cout << log;
}

int get_time_formatted(char *time_str, const int time_str_len, const char *format = "%Y-%m-%d %H:%M:%S") {
  int ret = OB_SUCCESS;
  struct timeval current_real_time;
  struct tm current_timeinfo;
  gettimeofday(&current_real_time, nullptr);
  ob_localtime(const_cast<time_t *>(&current_real_time.tv_sec), &current_timeinfo);

  if (OB_ISNULL(time_str) || OB_UNLIKELY(time_str_len <= 0) || OB_ISNULL(format)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(time_str), K(time_str_len), KP(format));
  } else if (0 == strftime(time_str, time_str_len, format, &current_timeinfo)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "the len of time_str is not enough", KR(ret));
  }
  return ret;
}

std::string OSDQLogEntry::get_time_prefix_()
{
  int ret = OB_SUCCESS;
  char time_str[TIME_STR_LENGTH] = { 0 };
  int64_t pos = 0;
  std::string time_prefix = "";
  if (OB_FAIL(get_time_formatted(time_str, TIME_STR_LENGTH))) {
    OB_LOG(WARN, "failed get time formatted", KR(ret));
  } else if (FALSE_IT(pos = strlen(time_str))) {
  } else if (snprintf(time_str + pos, sizeof(time_str) - pos, ".%06ld", ObTimeUtility::current_time() % 1000000) < 0) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed append microsecond for time_str", KR(ret));
  } else {
    time_prefix = std::string("[") + std::string(time_str) + std::string("]");
  }
  return time_prefix;
}

void OSDQLogEntry::log_entry_kv(const std::string &key, const std::string &value, const std::string &color)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else {
    content_ += head_holder_ + color + key + NONE_COLOR_PREFIX + ": " + value + "\n";
  }
}

void OSDQLogEntry::log_entry(const std::string &content, const std::string &color)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else {
    content_ += head_holder_ + color + content + NONE_COLOR_PREFIX + "\n";
  }
}

void OSDQLogEntry::print()
{
  std::string log = prefix_ + "\n" + content_;
  std::cout << log;
  reset();
}

void OSDQLogEntry::reset()
{
  prefix_ = "";
  head_holder_ = "";
  content_ = "";
  is_inited_ = false;
}

//================== OSDQTimeMap ===========================
OSDQTimeMap::OSDQTimeMap()
  : total_entry_(0)
{
}

int OSDQTimeMap::log_entry(const int64_t cost_time_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cost_time_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(cost_time_us));
  } else {
    const int64_t cost_time_ms = cost_time_us / 1000;
    time_map_[cost_time_ms]++;
    total_entry_++;
  }
  return ret;
}

int OSDQTimeMap::get_latency_quantile_vals(ObIArray<int64_t> &latency_quantile_vals) const
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> latency_quantile_counts;

  const int64_t latency_quantile_cnt = sizeof(LATENCY_QUANTILES) / sizeof(double);
  for (int i = 0; i < latency_quantile_cnt && OB_SUCC(ret); ++i) {
    int64_t latency_quantile_val = static_cast<int64_t>(total_entry_ * LATENCY_QUANTILES[i] + 0.5);
    latency_quantile_val = max(1, latency_quantile_val);
    latency_quantile_val = min(total_entry_, latency_quantile_val);
    if (OB_FAIL(latency_quantile_counts.push_back(latency_quantile_val))) {
      OB_LOG(WARN, "failed push back latency_quantile_val", KR(ret), K(latency_quantile_val));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t current_cnt = 0;
    int64_t boundary_index = 0;
    std::map<int64_t, int64_t>::const_iterator it = time_map_.begin();
    while (boundary_index < latency_quantile_cnt && it != time_map_.end() && OB_SUCC(ret)) {
      current_cnt += it->second;
      while (boundary_index < latency_quantile_cnt && current_cnt >= latency_quantile_counts[boundary_index] && OB_SUCC(ret)) {
        if (OB_FAIL(latency_quantile_vals.push_back(it->first))) {
          OB_LOG(WARN, "failed push back latency_quantile_val", KR(ret), K(it->first));
        }
        boundary_index++;
      }
      ++it;
    }
  }

  return ret;
}

int OSDQTimeMap::summary(const char *map_name_str, OSDQLogEntry &log) const
{
  int ret = OB_SUCCESS;
  const char *map_name = "Anonymous Time Map";
  if (OB_NOT_NULL(map_name_str)) {
    map_name = map_name_str;
  }
  if (total_entry_ <= 0) {
    log.log_entry(std::string(map_name) + "      0");
  } else {
    ObArray<int64_t> latency_quantile_vals;
    if (OB_FAIL(get_latency_quantile_vals(latency_quantile_vals))) {
      OB_LOG(WARN, "failed get latency_quantile_vals", KR(ret));
    } else {
      char buf[2048] = {0};
      int64_t pos = 0;

      if (FAILEDx(databuff_printf(buf, sizeof(buf), pos, "%s%7ld ", map_name, total_entry_))) {
        OB_LOG(WARN, "failed set log str", KR(ret));
      }

      for (int i = 0; i < latency_quantile_vals.count() && OB_SUCC(ret); ++i) {
        if (FAILEDx(databuff_printf(buf, sizeof(buf), pos, "%7ld ",
                                    latency_quantile_vals[i]))) {
          OB_LOG(WARN, "failed set log str", KR(ret), K(i), K(latency_quantile_vals[i]));
        }
      }
      if (OB_SUCC(ret)) {
        log.log_entry(std::string(buf));
      }
    }
  }
  return ret;
}

//================== OSDQMetric ===========================

//================== OSDQMetric::ReqStatisticalsInfo =============

OSDQMetric::ReqStatisticalsInfo::ReqStatisticalsInfo()
  : total_operation_num_(0),
    total_queued_num_(0),
    average_qps_(0),
    average_bw_mb_(0)
{}

//================== OSDQMetric::CpuInfo =========================
OSDQMetric::CpuInfo::CpuInfo()
  : cpu_usage_for_100MB_bw_(0),
    total_cpu_usage_(0),
    total_user_time_(0),
    total_system_time_(0),
    real_cpu_usage_(0)
{}

//================== OSDQMetric::MemInfo =========================
OSDQMetric::MemInfo::MemInfo()
  : start_vm_size_kb_(0),
    start_vm_rss_kb_(0),
    object_storage_hold_kb_(0),
    object_storage_used_kb_(0),
    total_hold_kb_(0),
    total_used_kb_(0),
    vm_peak_kb_(0),
    vm_size_kb_(0),
    vm_hwm_kb_(0),
    vm_rss_kb_(0),
    ob_vslice_alloc_used_memory_kb_(0),
    ob_vslice_alloc_allocator_cnt_(0)
{}

int OSDQMetric::print_csv_title_()
{
  int ret = OB_SUCCESS;
  std::ofstream ofs;
  if (FALSE_IT(ofs.open(metric_csv_path_))) {
  } else if (OB_UNLIKELY(!ofs.is_open())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to open metric_csv file", KR(ret), KP(metric_csv_path_));
  } else {
    ofs << "id, time, "
      << "total_operation_num, "
      << "total_queued_num, "
      << "total_throughput_mb, "
      << "average_qps, "
      << "average_bw_mb, "
      << "real_qps, "
      << "real_bw_mb,"
      << "cpu_usage_for_100MB_bw, "
      << "total_cpu_usage, "
      << "total_user_time, "
      << "total_system_time, "
      << "real_cpu_usage,"
      << "start_vm_size_kb, "
      << "start_vm_rss_kb, "
      << "object_storage_hold_kb, "
      << "object_storage_used_kb, "
      << "total_hold_kb, "
      << "total_used_kb, "
      << "vm_peak_kb, "
      << "vm_size_kb, "
      << "vm_hwm_kb, "
      << "vm_rss_kb, "
      << "ob_vslice_alloc_used_memory_kb, "
      << "ob_vslice_alloc_allocator_cnt, "
      << std::endl;
    ofs.close();
  }
  return ret;
}

int OSDQMetric::print_csv_dump_()
{
  int ret = OB_SUCCESS;
  char time[TIME_STR_LENGTH];
  std::ofstream ofs;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(get_time_formatted(time, TIME_STR_LENGTH))) {
    OB_LOG(WARN, "failed get time formatted", KR(ret));
  } else if (FALSE_IT(ofs.open(metric_csv_path_, std::ios::app))) {
  } else if (OB_UNLIKELY(!ofs.is_open())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to open metric_csv file", KR(ret), KP(metric_csv_path_));
  } else {
    ofs << std::fixed << std::setprecision(6)
      << summary_cnt_ << "," << time << ","
      << last_req_statistical_info_.total_operation_num_ << ","
      << last_req_statistical_info_.total_queued_num_ << ","
      << last_req_statistical_info_.total_throughput_mb_ << ","
      << last_req_statistical_info_.average_qps_ << ","
      << last_req_statistical_info_.average_bw_mb_ << ","
      << last_req_statistical_info_.real_qps_ << ","
      << last_req_statistical_info_.real_bw_mb_ << ","
      << last_cpu_info_.cpu_usage_for_100MB_bw_ << ","
      << last_cpu_info_.total_cpu_usage_ << ","
      << last_cpu_info_.total_user_time_ << ","
      << last_cpu_info_.total_system_time_ << ","
      << last_cpu_info_.real_cpu_usage_ << ","
      << last_mem_info_.start_vm_size_kb_ << ","
      << last_mem_info_.start_vm_rss_kb_ << ","
      << last_mem_info_.object_storage_hold_kb_ << ","
      << last_mem_info_.object_storage_used_kb_ << ","
      << last_mem_info_.total_hold_kb_ << ","
      << last_mem_info_.total_used_kb_ << ","
      << last_mem_info_.vm_peak_kb_ << ","
      << last_mem_info_.vm_size_kb_ << ","
      << last_mem_info_.vm_hwm_kb_ << ","
      << last_mem_info_.vm_rss_kb_ << ","
      << last_mem_info_.ob_vslice_alloc_used_memory_kb_ << ","
      << last_mem_info_.ob_vslice_alloc_allocator_cnt_ << ","
      << std::endl;
    ofs.close();
  }
  return ret;
}

OSDQMetric::OSDQMetric()
  : is_inited_(false),
    summary_cnt_(0),
    start_real_time_us_(ObTimeUtility::current_time()),
    last_real_time_us_(start_real_time_us_),
    total_operation_num_(0),
    total_queued_num_(0),
    total_throughput_mb_(0),
    last_req_statistical_info_(),
    last_cpu_info_(),
    start_mem_info_(),
    last_mem_info_()
{
  metric_csv_path_[0] = '\0';
}

OSDQMetric::~OSDQMetric()
{

}

int get_metric_csv_path(char *metric_csv_path, const int metric_csv_path_len)
{
  int ret = OB_SUCCESS;
  char time_str[TIME_STR_LENGTH] = { 0 };

  const char *ob_admin_log_dir = getenv("OB_ADMIN_LOG_DIR");
  if (OB_ISNULL(metric_csv_path) || OB_UNLIKELY(metric_csv_path_len < OB_MAX_FILE_NAME_LENGTH)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(metric_csv_path), K(metric_csv_path_len));
  } else if (OB_FAIL(get_time_formatted(time_str, TIME_STR_LENGTH, "%Y%m%d_%H%M%S"))) {
    OB_LOG(WARN, "failed to get time str", KR(ret));
  } else if (OB_ISNULL(ob_admin_log_dir)) {
    if (OB_FAIL(databuff_printf(metric_csv_path, metric_csv_path_len, "metric_csv_%s.csv", time_str))) {
      OB_LOG(WARN, "failed databuff printf metric csv path", KR(ret), K(ob_admin_log_dir), K(time_str));
    }
  } else if (OB_FAIL(databuff_printf(metric_csv_path, metric_csv_path_len, "%s/metric_csv_%s.csv",
          ob_admin_log_dir, time_str))) {
    OB_LOG(WARN, "failed databuff printf metric csv path", KR(ret), K(ob_admin_log_dir), K(time_str));
  }
  return ret;
}

int OSDQMetric::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", KR(ret), K(is_inited_));
  } else if (OB_FAIL(get_memory_usage(start_mem_info_))) {
    OB_LOG(WARN, "failed get start memory info", KR(ret));
  } else if (OB_FAIL(get_metric_csv_path(metric_csv_path_, OB_MAX_FILE_NAME_LENGTH))) {
    OB_LOG(WARN, "failed get metric csv path", KR(ret));
  } else if (OB_FAIL(print_csv_title_())) {
    OB_LOG(WARN, "faild print title to metric csv file", KR(ret), KP(metric_csv_path_));
  } else {
    getrusage(RUSAGE_SELF, &start_usage_);
    last_usage_ = start_usage_;
    is_inited_ = true;
  }
  return ret;
}

int OSDQMetric::add_latency_metric(
    const int64_t op_start_time_us,
    const OSDQOpType op_type,
    const int64_t object_size)
{
  lib::ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(op_start_time_us <= 0 || op_type == MAX_OPERATE_TYPE || object_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(op_start_time_us), K(op_type), K(object_size));
  } else {
    const int64_t op_cost_time_us = ObTimeUtility::current_time() - op_start_time_us;
    ObjectSizeType object_size_type = ObjectSizeType::MAX_OJBECT_SIZE_TYPE;
    if (object_size <= SMALL_OBJECT_SIZE_LIMIT) {
      object_size_type = ObjectSizeType::SMALL_OBJECT;
    } else if (object_size <= NORMAL_OBJECT_SIZE_LIMIT) {
      object_size_type = ObjectSizeType::NORMAL_OBJECT_SIZE;
    } else if (object_size <= LARGE_OBJECT_SIZE_LIMIT) {
      object_size_type = ObjectSizeType::LARGE_OBJECT_SIZE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "object size is too large", KR(ret), K(object_size));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_LIKELY(object_size_type != ObjectSizeType::MAX_OJBECT_SIZE_TYPE)) {
      if (OB_FAIL(latency_maps_[op_type][object_size_type].log_entry(op_cost_time_us))) {
        OB_LOG(WARN, "failed to log entry", KR(ret), K(op_type), K(op_cost_time_us));
      } else {
        total_operation_num_++;
        total_throughput_mb_ += 1.0 * object_size / 1024 / 1024;
      }
    }
  }
  return ret;
}

int OSDQMetric::add_queued_entry()
{
  lib::ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else {
    total_queued_num_++;
  }
  return ret;
}

int OSDQMetric::sub_queued_entry()
{
  lib::ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "not init", KR(ret));
  } else {
    total_queued_num_--;
  }
  return ret;
}

int OSDQMetric::get_memory_usage(MemInfo &mem_info)
{
  int ret = OB_SUCCESS;
  std::ifstream status_file("/proc/self/status");
  std::string line;
  if (OB_UNLIKELY(!status_file.is_open())) {
    ret = OB_FILE_NOT_OPENED;
    OB_LOG(WARN, "failed to open /proc/self/status", KR(ret));
  }

  int write_state = 0;
  while (std::getline(status_file, line) && OB_SUCC(ret)) {
   if (line.find("VmPeak:") == 0) {
     if (OB_UNLIKELY(sscanf(line.c_str(), "VmPeak: %lf kB", &mem_info.vm_peak_kb_) != 1)) {
       ret = OB_ERR_UNEXPECTED;
       OB_LOG(WARN, "faield to get VmPeak", KR(ret), K(line.c_str()));
     } else {
       write_state |= 1 << 0;
     }
    } else if (line.find("VmSize:") == 0) {
      if (OB_UNLIKELY(sscanf(line.c_str(), "VmSize: %lf kB", &mem_info.vm_size_kb_) != 1)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to get VmSize", KR(ret), K(line.c_str()));
      } else {
        write_state |= 1 << 1;
      }
    } else if (line.find("VmHWM:") == 0) {
      if (OB_UNLIKELY(sscanf(line.c_str(), "VmHWM: %lf kB", &mem_info.vm_hwm_kb_) != 1)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to get VmHWM", KR(ret), K(line.c_str()));
      } else {
        write_state |= 1 << 2;
      }
    } else if (line.find("VmRSS:") == 0) {
      if (OB_UNLIKELY(sscanf(line.c_str(), "VmRSS: %lf kB", &mem_info.vm_rss_kb_) != 1)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "failed to get VmRSS", KR(ret), K(line.c_str()));
      } else {
        write_state |= 1 << 3;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(write_state != 15)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "some memory statistics are missing", KR(ret), K(write_state));
  }
  return ret;
}

int OSDQMetric::get_req_statistical_info_(OSDQLogEntry &log)
{
  int ret = OB_SUCCESS;
  const double cost_time_s = 1.0 * (ObTimeUtility::current_time() - start_real_time_us_) / 1000000;
  log.log_entry("===== PART 1 REQ STATISTICAL INFO =====", DARY_GRAY_PREFIX);
  if (cost_time_s > EPS) {
    last_req_statistical_info_.average_qps_ = total_operation_num_ / cost_time_s;
    last_req_statistical_info_.average_bw_mb_ = total_throughput_mb_ / cost_time_s;
  }
  double real_qps = 0;
  double real_bw_mb = 0;
  const double interval_time_s = (ObTimeUtility::current_time() - last_real_time_us_) / 1000000;
  if (interval_time_s > EPS) {
    real_qps = static_cast<double>(total_operation_num_ - last_req_statistical_info_.total_operation_num_) / interval_time_s;
    real_bw_mb = static_cast<double>(total_throughput_mb_ - last_req_statistical_info_.total_throughput_mb_) / interval_time_s;
  }
  last_req_statistical_info_.total_operation_num_ = total_operation_num_;
  last_req_statistical_info_.total_throughput_mb_ = total_throughput_mb_;
  last_req_statistical_info_.total_queued_num_ = total_queued_num_;
  last_req_statistical_info_.real_qps_ = real_qps;
  last_req_statistical_info_.real_bw_mb_ = real_bw_mb;

  log.log_entry_kv("Total operation num", std::to_string(last_req_statistical_info_.total_operation_num_));
  log.log_entry_kv("Total queued num", std::to_string(last_req_statistical_info_.total_queued_num_));
  log.log_entry_kv("Total throughput bytes(MB)",
      to_string_with_precision(last_req_statistical_info_.total_throughput_mb_, PRECISION));
  log.log_entry_kv("Average QPS",
      to_string_with_precision(last_req_statistical_info_.average_qps_, PRECISION));
  log.log_entry_kv("Average BW(MB/s)",
      to_string_with_precision(last_req_statistical_info_.average_bw_mb_, PRECISION));
  log.log_entry_kv("Real QPS", to_string_with_precision(real_qps, PRECISION));
  log.log_entry_kv("Real BW(MB/s)", to_string_with_precision(real_bw_mb, PRECISION));
  return ret;
}

int OSDQMetric::get_req_latency_map_(OSDQLogEntry &log)
{
  int ret = OB_SUCCESS;
  log.log_entry("===== PART 2 REQ LATENCY MAP =====", DARY_GRAY_PREFIX);
  static const int latency_maps_num = MAX_OPERATE_TYPE;
  static const char *object_size_type_time_map_str[] = {
    "S |", // SMALL
    "N |", // NORMAL
    "L |"  // LARGE
  };

  log.log_entry(" req |  cnt  |  min  |  P50  |  P90  |  P95  |  P99  |  max   ");
  log.log_entry("--------------------------------------------------------------");
  char time_map_str[24] = {0};
  int64_t pos = 0;
  int64_t cur_pos = 0;
  for (int i = 0; i < latency_maps_num && OB_SUCC(ret); i++) {
    pos = 0;
    if (FAILEDx(databuff_printf(time_map_str, sizeof(time_map_str), pos, "%s ", OSDQ_OP_TYPE_NAMES[i]))) {
      OB_LOG(WARN, "failed to printf latency map str", KR(ret), K(OSDQ_OP_TYPE_NAMES[i]));
    } else {
      for (int type = 0; type < ObjectSizeType::MAX_OJBECT_SIZE_TYPE && OB_SUCC(ret); type++) {
        cur_pos = pos;
        if (OB_FAIL(databuff_printf(time_map_str, sizeof(time_map_str), cur_pos, "%s", object_size_type_time_map_str[type]))) {
          OB_LOG(WARN, "failed to printf object size type time map str",
              KR(ret), K(object_size_type_time_map_str[type]));
        } else if (OB_FAIL(latency_maps_[i][type].summary(time_map_str, log))) {
          OB_LOG(WARN, "failed summary latency map", KR(ret), K(i), K(type), K(time_map_str));
        }
      }
    }
    log.log_entry("--------------------------------------------------------------");
  }
  return ret;
}

int OSDQMetric::get_cpu_info_(OSDQLogEntry &log)
{
  int ret = OB_SUCCESS;
  const double cost_time_s = 1.0 * (ObTimeUtility::current_time() - start_real_time_us_) / 1000000;
  const double interval_time_s = 1.0 * (ObTimeUtility::current_time() - last_real_time_us_) / 1000000;
  log.log_entry("===== PART 3 CPU INFO =====", DARY_GRAY_PREFIX);
  struct rusage current_usage;
  getrusage(RUSAGE_SELF, &current_usage);

  const double user_cpu_time_s = cal_time_diff(start_usage_.ru_utime, current_usage.ru_utime);
  const double sys_cpu_time_s = cal_time_diff(start_usage_.ru_stime, current_usage.ru_stime);
  double avg_cpu_usage = 0;
  if (cost_time_s > EPS) {
    avg_cpu_usage = (user_cpu_time_s + sys_cpu_time_s) / cost_time_s * 100;
  }
  double cpu_usage_for_100MB_bw = 0;
  if (last_req_statistical_info_.average_bw_mb_ > EPS) {
    cpu_usage_for_100MB_bw = (100.0 / last_req_statistical_info_.average_bw_mb_) * avg_cpu_usage;
  }
  double real_cpu_time_s = cal_time_diff(last_usage_.ru_utime, current_usage.ru_utime) +
    cal_time_diff(last_usage_.ru_stime, current_usage.ru_stime);
  double real_cpu_usage_ = 0;
  if (interval_time_s > EPS) {
    real_cpu_usage_ = real_cpu_time_s / interval_time_s * 100;
  }

  log.log_entry_kv("CPU usage for 100MB/s BW",
      (to_string_with_precision(cpu_usage_for_100MB_bw, PRECISION) + "% per 100MB/s"));
  log.log_entry_kv("Total CPU usage", (to_string_with_precision(avg_cpu_usage, PRECISION) + "%"));
  log.log_entry_kv("Total user time", (to_string_with_precision(user_cpu_time_s, PRECISION) + " s"));
  log.log_entry_kv("Total system time", (to_string_with_precision(sys_cpu_time_s, PRECISION) + " s"));
  log.log_entry_kv("Real CPU usage", (to_string_with_precision(real_cpu_usage_, PRECISION) + "%"));
  last_usage_ = current_usage;
  last_cpu_info_.cpu_usage_for_100MB_bw_ = cpu_usage_for_100MB_bw;
  last_cpu_info_.total_system_time_ = sys_cpu_time_s;
  last_cpu_info_.total_user_time_ = user_cpu_time_s;
  last_cpu_info_.total_cpu_usage_ = avg_cpu_usage;
  last_cpu_info_.real_cpu_usage_ = real_cpu_time_s;
  return ret;
}

static int iter_memory_label(
    lib::ObLabel &label,
    LabelItem *l_item,
    int64_t &object_storage_hold_bytes,
    int64_t &object_storage_used_bytes,
    int64_t &total_hold_bytes,
    int64_t &total_used_bytes)
{
  // OSS_SDK or StorageOss
  const char *oss_label = "OSS";
  // COS_SDK or StorageCos
  const char *cos_label = "COS";
  // S3_SDK or StorageS3
  const char *s3_label = "S3";
  const char *obdal_label = "OBDAL";
  const char *default_label = "OBJECT_STORAGE";
  const char *object_label = "OBJECT_DEVICE";

  int ret = OB_SUCCESS;
  if (strstr(label.str_, oss_label) != nullptr
      || strstr(label.str_, cos_label) != nullptr
      || strstr(label.str_, s3_label) != nullptr
      || strstr(label.str_, obdal_label) != nullptr
      || strstr(label.str_, default_label) != nullptr
      || strstr(label.str_, object_label) != nullptr) {
    object_storage_hold_bytes += l_item->hold_;
    object_storage_used_bytes += l_item->used_;
  }
  total_hold_bytes += l_item->hold_;
  total_used_bytes += l_item->used_;
  return ret;
}

int OSDQMetric::get_memory_info_(OSDQLogEntry &log, const bool is_final)
{
  int ret = OB_SUCCESS;
  log.log_entry("===== PART 4 MEMORY INFO =====", DARY_GRAY_PREFIX);
  if (FAILEDx(get_memory_usage(last_mem_info_))) {
    OB_LOG(WARN, "failed to get memory usage", KR(ret));
  } else {
    if (is_final) {
      last_mem_info_.start_vm_size_kb_ = start_mem_info_.vm_size_kb_;
      last_mem_info_.start_vm_rss_kb_ = start_mem_info_.vm_rss_kb_;
      log.log_entry_kv("start VmSize", (std::to_string(start_mem_info_.vm_size_kb_) + " kB"));
      log.log_entry_kv("start VmRSS", (std::to_string(start_mem_info_.vm_rss_kb_) + " kB"));
    }

    int64_t total_hold_bytes = 0;
    int64_t total_used_bytes = 0;
    int64_t object_storage_hold_bytes = 0;
    int64_t object_storage_used_bytes = 0;
    uint64_t tenant_ids[OB_MAX_SERVER_TENANT_CNT];
    int tenant_cnt = 0;
    // default get all tenant memory info
    get_tenant_ids(tenant_ids, OB_MAX_SERVER_TENANT_CNT, tenant_cnt);

    for (int tenant_idx = 0; OB_SUCC(ret) && tenant_idx < tenant_cnt; tenant_idx++) {
      uint64_t tenant_id = tenant_ids[tenant_idx];
      for (int ctx_id = 0; ctx_id < ObCtxIds::MAX_CTX_ID; ctx_id++) {
        lib::ObTenantCtxAllocatorGuard it = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(tenant_id, ctx_id);
        if (nullptr == it) {
          it = ObMallocAllocator::get_instance()->get_tenant_ctx_allocator_unrecycled(tenant_id, ctx_id);
        }
        if (nullptr == it) {
          continue;
        }

        if (OB_SUCC(ret)) {
          std::function<int(lib::ObLabel &label, LabelItem *l_item)> iter_label_func = std::bind(
              &iter_memory_label,
              std::placeholders::_1,
              std::placeholders::_2,
              std::ref(object_storage_hold_bytes),
              std::ref(object_storage_used_bytes),
              std::ref(total_hold_bytes),
              std::ref(total_used_bytes));
          ret = it->iter_label(iter_label_func);
        }
      }
    }

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "failed get alloc bytes", KR(ret));
    } else {
      last_mem_info_.object_storage_hold_kb_ = object_storage_hold_bytes / 1024;
      last_mem_info_.object_storage_used_kb_ = object_storage_used_bytes / 1024;
      last_mem_info_.total_hold_kb_ = total_hold_bytes / 1024;
      last_mem_info_.total_used_kb_ = total_used_bytes / 1024;
      last_mem_info_.ob_vslice_alloc_used_memory_kb_ = get_vslice_alloc_instance().hold() / 1024;
      last_mem_info_.ob_vslice_alloc_allocator_cnt_ = allocator_cnt;
      log.log_entry_kv("ObjectStorageHold", (std::to_string(last_mem_info_.object_storage_hold_kb_) + " kB"));
      log.log_entry_kv("ObjectStorageUsed", (std::to_string(last_mem_info_.object_storage_used_kb_) + " kB"));
      log.log_entry_kv("TotalHold", (std::to_string(last_mem_info_.total_hold_kb_) + " kB"));
      log.log_entry_kv("TotalUsed", (std::to_string(last_mem_info_.total_used_kb_) + " kB"));
      log.log_entry_kv("VmPeak", (std::to_string(last_mem_info_.vm_peak_kb_) + " kB"));
      log.log_entry_kv("VmSize", (std::to_string(last_mem_info_.vm_size_kb_) + " kB"));
      log.log_entry_kv("VmHWM", (std::to_string(last_mem_info_.vm_hwm_kb_) + " kB"));
      log.log_entry_kv("VmRSS", (std::to_string(last_mem_info_.vm_rss_kb_) + " kB"));
      log.log_entry_kv("ObVSliceAlloc used memory",
          (std::to_string(last_mem_info_.ob_vslice_alloc_used_memory_kb_) + " kB"));
      log.log_entry_kv("ObVSliceAlloc allocator cnt",
          (std::to_string(last_mem_info_.ob_vslice_alloc_allocator_cnt_) + " times"));
    }

    if (OB_FAIL(ret)) {
    } else if (is_final) {
      if (OB_UNLIKELY(last_mem_info_.object_storage_hold_kb_ > FINAL_OBJECT_STORAGE_MEMORY_LIMIT)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(ERROR, "Object storage memory is out of limit!", K(last_mem_info_.object_storage_hold_kb_));
        log.print_log("[ERROR]", "Object Storage memory is out of limit! " + std::to_string(last_mem_info_.object_storage_hold_kb_));
      }
    }
  }

  return ret;
}

int OSDQMetric::final_dump_()
{
  int ret = OB_SUCCESS;
  std::ofstream ofs;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "metric not init", KR(ret), K(is_inited_));
  } else if (FALSE_IT(ofs.open(metric_final_dump_file_name, std::ios::app))) {
  } else if (OB_UNLIKELY(!ofs.is_open())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to open file", KR(ret), K(metric_final_dump_file_name));
  } else {
    ofs << "[request statistical info]" << "\n";
    ofs << "total_operation_num = " << last_req_statistical_info_.total_operation_num_ << "\n";
    ofs << "total_queued_num = " << last_req_statistical_info_.total_queued_num_ << "\n";
    ofs << "total_throughput_mb = " << last_req_statistical_info_.total_throughput_mb_ << "\n";
    ofs << "average_qps = " << last_req_statistical_info_.average_qps_ << "\n";
    ofs << "average_bw_mb = " << last_req_statistical_info_.average_bw_mb_ << "\n";
    ofs << "real_qps = " << last_req_statistical_info_.real_qps_ << "\n";
    ofs << "real_bw_mb = " << last_req_statistical_info_.real_bw_mb_ << "\n";
    ofs << "[cpu info]" << "\n";
    ofs << "cpu_usage_for_100MB_bw = " << to_string_with_precision(last_cpu_info_.cpu_usage_for_100MB_bw_, PRECISION) << "\n";
    ofs << "total_cpu_usage = " << last_cpu_info_.total_cpu_usage_ << "\n";
    ofs << "total_user_time = " << last_cpu_info_.total_user_time_ << "\n";
    ofs << "total_system_time = " << last_cpu_info_.total_system_time_ << "\n";
    ofs << "real_cpu_usage = " << last_cpu_info_.real_cpu_usage_ << "\n";
    ofs << "[memory info]" << "\n";
    ofs << "start_vm_size_kb = " << last_mem_info_.start_vm_size_kb_ << "\n";
    ofs << "start_vm_rss_kb = " << last_mem_info_.start_vm_rss_kb_ << "\n";
    ofs << "object_storage_hold_kb = " << last_mem_info_.object_storage_hold_kb_ << "\n";
    ofs << "object_storage_used_kb = " << last_mem_info_.object_storage_used_kb_ << "\n";
    ofs << "total_hold_kb = " << last_mem_info_.total_hold_kb_ << "\n";
    ofs << "total_used_kb = " << last_mem_info_.total_used_kb_ << "\n";
    ofs << "vm_peak_kb = " << last_mem_info_.vm_peak_kb_ << "\n";
    ofs << "vm_size_kb = " << last_mem_info_.vm_size_kb_ << "\n";
    ofs << "vm_hwm_kb = " << last_mem_info_.vm_hwm_kb_ << "\n";
    ofs << "vm_rss_kb = " << last_mem_info_.vm_rss_kb_ << "\n";
    ofs << "ob_vslice_alloc_used_memory_kb = " << last_mem_info_.ob_vslice_alloc_used_memory_kb_ << "\n";
    ofs << "ob_vslice_alloc_allocator_cnt = " << last_mem_info_.ob_vslice_alloc_allocator_cnt_ << "\n";

    ofs << "[latency info]" << "\n";
    ofs << "latency_quantile = ";
    int64_t latency_quantile_cnt = sizeof(LATENCY_QUANTILES_TITLE) / sizeof(char *);
    for (int i = 0; i < latency_quantile_cnt; i++) {
      ofs << LATENCY_QUANTILES_TITLE[i] << ",";
    }
    ofs << "\n";
    static const char *object_size_type_time_map_str[] = {
      "small", // SMALL
      "normal", // NORMAL
      "large"  // LARGE
    };
    for (int i = 0; i < MAX_OPERATE_TYPE && OB_SUCC(ret); i++) {
      for (int type = 0; type < ObjectSizeType::MAX_OJBECT_SIZE_TYPE && OB_SUCC(ret); type++) {
        ofs << OSDQ_OP_TYPE_NAMES_FOR_FINAL_DUMP[i] << "_" << object_size_type_time_map_str[type] << " = ";
        ObArray<int64_t> latency;
        if (OB_FAIL(latency_maps_[i][type].get_latency_quantile_vals(latency))) {
          OB_LOG(WARN, "failed to get latency", KR(ret), K(i), K(type));
        } else {
          for (int j = 0; j < latency.count() && OB_SUCC(ret); j++) {
            ofs << latency[j] << ",";
          }
          ofs << "\n";
        }
      }
    }
  }
  return ret;
}

int OSDQMetric::summary(const bool is_final)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  summary_cnt_++;
  OSDQLogEntry log;
  std::string title = std::to_string(summary_cnt_) + "-th METRIC SUMMARY" + (is_final ? " FINAL" : "");
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "metric not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(log.init(title, LIGHT_BLUE_PREFIX))) {
    OB_LOG(WARN, "failed init log", KR(ret), K(summary_cnt_));
  } else if (OB_FAIL(get_req_statistical_info_(log))) {
    OB_LOG(WARN, "failed get req statistical info", KR(ret));
  } else if (OB_FAIL(get_req_latency_map_(log))) {
    OB_LOG(WARN, "failed to get req latency map", KR(ret));
  } else if (OB_FAIL(get_cpu_info_(log))) {
    OB_LOG(WARN, "failed get cpu info", KR(ret));
  } else if (OB_FAIL(get_memory_info_(log, is_final))) {
    OB_LOG(WARN, "failed get memory info", KR(ret));
  } else if (OB_FAIL(print_csv_dump_())) {
    OB_LOG(WARN, "failed print csv dump", KR(ret));
  } else {
    log.print();
    last_real_time_us_ = ObTimeUtility::current_time();
  }

  if (is_final) {
    if (FAILEDx(final_dump_())) {
      OB_LOG(WARN, "failed to final dump", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
    summary_cnt_--;
  }
  return ret;
}

//=========================== OSDQMonitor ==============================

OSDQMonitor::OSDQMonitor()
  : is_inited_(false),
    is_started_(false),
    metric_(nullptr),
    interval_us_(0),
    tg_id_(-1)
{}

OSDQMonitor::~OSDQMonitor() {}

int OSDQMonitor::init(const int64_t interval_s, OSDQMetric *metric) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "OSDQMonitor init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(metric) || OB_UNLIKELY(interval_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), K(metric));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::COMMON_TIMER_THREAD, tg_id_))) {
    OB_LOG(WARN, "failed create timer thread", KR(ret));
  } else {
    metric_ = metric;
    interval_us_ = interval_s * 1000 * 1000;
    is_inited_ = true;
  }
  return ret;
}

int OSDQMonitor::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "monitor not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(is_started_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "monitor start twice", KR(ret), K(is_started_));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    OB_LOG(WARN, "failed start timer thread", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id_, *this, interval_us_, true/*repeat*/, true/*immediate*/))) {
    OB_LOG(WARN, "failed schedule summary task", KR(ret), K(tg_id_), K(interval_us_));
  } else {
    is_started_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

void OSDQMonitor::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "metric monitor not init", KR(ret), K(is_inited_));
  } else {
    if (tg_id_ != -1) {
      TG_STOP(tg_id_);
      TG_WAIT(tg_id_);
      TG_DESTROY(tg_id_);
    }
    metric_ = nullptr;
    interval_us_ = 0;
    is_inited_ = false;
    is_started_ = false;
  }
}

void OSDQMonitor::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "metric monitor not init", KR(ret), K(is_inited_));
  } else if (OB_FAIL(metric_->summary())) {
    OB_LOG(WARN, "failed exec metric summary", KR(ret));
  }
}

//============================ OSDQParameters ===============================
OSDQParameters::OSDQParameters()
  : scene_type_(-1),
    run_time_s_(DEFAULT_RUN_TIME_S),
    interval_s_(DEFAULT_INTERVAL_S),
    thread_cnt_(DEFAULT_THREAD_CNT),
    resource_limited_type_(-1),
    limit_run_time_s_(DEFAULT_LIMIT_RUN_TIME_S),
    limit_memory_mb_(DEFAULT_LIMIT_MEMORY_MB),
    limit_cpu_(DEFAULT_LIMIT_CPU),
    use_obdal_(false)
{
}

//============================ ObAdminObjectStorageDriverQualityExecutor ===========================
ObAdminObjectStorageDriverQualityExecutor::ObAdminObjectStorageDriverQualityExecutor()
  : params_(),
    metric_(),
    monitor_()
{
}

int ObAdminObjectStorageDriverQualityExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  OSDQScene *scene = nullptr;
  const int64_t memory_limit = 16 * 1024 * 1024 * 1024LL;  // 16 GB
  if (OB_FAIL(parse_cmd_(argc, argv))) {
    OB_LOG(WARN, "failed to parse cmd", KR(ret), K(argc), K(argv));
  } else if (OB_FAIL(set_environment_())) {
    OB_LOG(WARN, "failed set environment", KR(ret));
  } else if (OB_FAIL(param_dump())) {
    OB_LOG(WARN, "failed to dump args", KR(ret));
  } else if (OB_FAIL(metric_.init())) {
    OB_LOG(WARN, "failed init metric", KR(ret));
  } else if (OB_ISNULL(scene = create_scene_())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to create scene", K(ret));
  } else if (OB_FAIL(scene->init(&params_, &metric_))) {
    if (ret == OB_INVALID_ARGUMENT) {
      print_usage_();
    }
    OB_LOG(WARN, "failed to init scene", KR(ret), K(params_));
  } else if (OB_FAIL(scene->set_thread_cnt(params_.thread_cnt_))) {
    OB_LOG(WARN, "failed set thread cnt", KR(ret));
  } else if (OB_FAIL(monitor_.init(params_.interval_s_, &metric_))) {
    OB_LOG(WARN, "failed init monitor", KR(ret), K(params_.interval_s_));
  } else if (OB_FAIL(monitor_.start())) {
    OB_LOG(WARN, "failed start monitor", KR(ret));
  } else if (OB_FAIL(scene->execute())) {
    OB_LOG(WARN, "failed to execute scene", KR(ret), K(params_));
    OSDQLogEntry::print_log("TEST RESULT FAILED", "", RED_COLOR_PREFIX);
  }
  monitor_.destroy();
  malloc_trim(0);
  free_scene_(scene);
  if (OB_SUCC(ret)) {
    OSDQLogEntry::print_log("TEST RESULT SUCCESS", "");
    OSDQLogEntry::print_log("WAIT 60 seconds for MemoryDump to refresh", "");
    // wait 60 seconds for MemoryDump to refresh
    ::sleep(60);
    if (OB_FAIL(metric_.summary(true/*is_final*/))) {
      OB_LOG(WARN, "failed to execute the last summary", KR(ret));
    }
  }
  return ret;
}

int ObAdminObjectStorageDriverQualityExecutor::param_dump()
{
  int ret = OB_SUCCESS;
  std::ofstream ofs;
  ofs.open(metric_final_dump_file_name, std::ios::out);
  if (OB_UNLIKELY(!ofs.is_open())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to open file", K(ret), K(metric_final_dump_file_name));
  } else {
    ofs << "[parameter]" << "\n";
    ofs << "base_path = " << params_.base_path_ << "\n";
    ofs << "interval_s = " << params_.interval_s_ << "\n";
    ofs << "limit_cpu = " << params_.limit_cpu_ << "\n";
    ofs << "limit_memory_mb = " << params_.limit_memory_mb_ << "\n";
    ofs << "limit_run_time_s = " << params_.limit_run_time_s_<< "\n";
    ofs << "resource_limited_type = " << params_.resource_limited_type_ << "\n";
    ofs << "run_time_s = " << params_.run_time_s_ << "\n";
    ofs << "scene_type = " << params_.scene_type_ << "\n";
    ofs << "thread_cnt = " << params_.thread_cnt_ << "\n";
    ofs << "use_obdal = " << params_.use_obdal_ << "\n";
  }
  return ret;
}

int ObAdminObjectStorageDriverQualityExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  int index = -1;
  const char *opt_str = "h:d:s:S:R:r:i:t:a";
  struct option longopts[] = {
    {"help", 0, NULL, 'h'},
    {"file-path-prefix", 1, NULL, 'd'},
    {"storage-info", 1, NULL, 's'},
    {"scene-type", 1, NULL, 'S'},
    {"resource-limited-type", 1, NULL, 'R'},
    {"limit-run-time", 1, NULL, '0'},
    {"limit-memory", 1, NULL, '0'},
    {"limit-cpu", 1, NULL, '0'},
    {"run-time", 1, NULL, 'r'},
    {"interval", 1, NULL, 'i'},
    {"thread_cnt", 1, NULL, 't'},
    {"enable_obdal", 0, NULL, 'a'},
    {NULL, 0, NULL, 0},
  };
  ObClusterStateBaseMgr::get_instance().set_enable_obdal(false);
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        ret = OB_INVALID_ARGUMENT;
        print_usage_();
        break;
      }
      case 'd': {
        time_t timestamp = time(NULL);
        struct tm *timeinfo = localtime(&timestamp);
        char buf[OB_MAX_TIME_STR_LENGTH];
        strftime(buf, sizeof(buf), "%Y-%m-%d-%H:%M:%S", timeinfo);
        if (OB_FAIL(databuff_printf(params_.base_path_, sizeof(params_.base_path_), "%s", optarg))) {
          OB_LOG(WARN, "failed to construct base path", KR(ret), K((char *)optarg), K(buf));
        }
        break;
      }
      case 's': {
        if (OB_FAIL(databuff_printf(params_.storage_info_str_, sizeof(params_.storage_info_str_), "%s", optarg))) {
          OB_LOG(WARN, "failed to copy storage info str", KR(ret), K((char *)optarg));
        }
        break;
      }
      case 'S': {
        if (OB_FAIL(c_str_to_int(optarg, params_.scene_type_))) {
          OB_LOG(WARN, "failed to parse scene type", KR(ret), K((char *) optarg));
        }
        break;
      }
      case 'R': {
        if (OB_FAIL(c_str_to_int(optarg, params_.resource_limited_type_))) {
          OB_LOG(WARN, "failed to parse resource limited type", KR(ret), K((char *)optarg));
        }
        break;
      }
      case 'r': {
        if (OB_FAIL(c_str_to_int(optarg, params_.run_time_s_))) {
          OB_LOG(WARN, "failed to parse run time", KR(ret), K((char *)optarg));
        }
        break;
      }
      case 'i': {
        if (OB_FAIL(c_str_to_int(optarg, params_.interval_s_))) {
          OB_LOG(WARN, "failed to parse interval", KR(ret), K((char *)optarg));
        }
        break;
      }
      case 't': {
        if (OB_FAIL(c_str_to_int(optarg, params_.thread_cnt_))) {
          OB_LOG(WARN, "failed to parse thread cnt", KR(ret), K((char *)optarg));
        }
        break;
      }
      case 'a': {
        ObClusterStateBaseMgr::get_instance().set_enable_obdal(true);
        params_.use_obdal_ = true;
        break;
      }
      case '0': {
        if (index >= 0) {
          const char *opt_name = longopts[index].name;
          if (strcmp(opt_name, "limit-run-time") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, params_.limit_run_time_s_))) {
              OB_LOG(WARN, "failed to parse limit run time", KR(ret), K((char *) optarg));
            }
          } else if (strcmp(opt_name, "limit-memory") == 0) {
            if (OB_FAIL(c_str_to_int(optarg, params_.limit_memory_mb_))) {
              OB_LOG(WARN, "failed to parse limit memory", KR(ret), K((char *) optarg));
            }
          } else if (strcmp(opt_name, "limit-cpu") == 0) {
            try {
              params_.limit_cpu_ = std::stod(optarg);
            } catch (...) {
              ret = OB_INVALID_ARGUMENT;
              OB_LOG(WARN, "failed to parse limit cpu", KR(ret), K((char *) optarg));
            }
          }
          break;
        } else {
          ret = OB_INVALID_ARGUMENT;
          print_usage_();
          break;
        }
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        print_usage_();
        break;
      }
    }
  }
  return ret;
}

int ObAdminObjectStorageDriverQualityExecutor::set_environment_()
{
  int ret = OB_SUCCESS;
  init_malloc_hook();
  lib::set_memory_limit(MEMORY_LIMITED_SIZE);
  lib::set_tenant_memory_limit(OB_SERVER_TENANT_ID, MEMORY_LIMITED_SIZE);
  mallopt(M_MMAP_THRESHOLD, 128 * 1024);
  OB_LOGGER.set_log_level("INFO");

  ObTenantBase *tenant_base = new ObTenantBase(OB_SERVER_TENANT_ID);
  ObMallocAllocator *malloc = ObMallocAllocator::get_instance();

  // set tenant memory limit
  if (OB_ISNULL(malloc->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, 0))) {
    if (OB_FAIL(malloc->create_and_add_tenant_allocator(OB_SERVER_TENANT_ID))) {
      OB_LOG(WARN, "failed to create_and_add_tenant_allocator", KR(ret));
    }
  }

  // init tenant and tenant io manager
  if (FAILEDx(tenant_base->init())) {
    OB_LOG(WARN, "failed to init tenant base", KR(ret));
  } else if (FALSE_IT(ObTenantEnv::set_tenant(tenant_base))) {
  } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
    OB_LOG(WARN, "init device manager failed", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().init(MEMORY_LIMITED_SIZE))) {
    OB_LOG(WARN, "failed to init io manager", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    OB_LOG(WARN, "failed to start io manager", KR(ret));
  } else if (OB_FAIL(ObObjectStorageInfo::register_cluster_state_mgr(&ObClusterStateBaseMgr::get_instance()))) {
    STORAGE_LOG(WARN, "fail to register cluster state mgr", KR(ret));
  }

  // set tenant io manager memory limit;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (FAILEDx(OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, tenant_holder))) {
    OB_LOG(WARN, "failed get tenant io manager", KR(ret));
  } else if (OB_FAIL(tenant_holder.get_ptr()->update_memory_pool(MEMORY_LIMITED_SIZE))) {
    OB_LOG(WARN, "failed update memory limit", KR(ret), K(MEMORY_LIMITED_SIZE));
  }

  if (FAILEDx(ObClockGenerator::get_instance().init())) {
    OB_LOG(WARN, "failed init clock generate", KR(ret));
  } else if (OB_FAIL(ObMemoryDump::get_instance().init())) {
    OB_LOG(WARN, "failed init MemoryDump", KR(ret));
  }
  return ret;
}

int ObAdminObjectStorageDriverQualityExecutor::print_usage_()
{
  int ret = OB_SUCCESS;
  printf("\n");
  printf("Usage: io_driver_quality command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h, --help", "display this message.");
  printf("options:\n");
  printf(HELP_FMT, "-d, --file-path-prefix", "absolute file path with file prefix");
  printf(HELP_FMT, "-s, --storage-info", "oss/cos should provide storage info");
  printf(HELP_FMT, "-a", "enable obdal");
  printf(HELP_FMT, "-S, --scene-type", "indicate the Scene to be run");
  printf(HELP_FMT, "", "0, Hybrid Scene");
  printf(HELP_FMT, "", "1, Resource Limited Scene");
  printf(HELP_FMT, "", "2, ErrSim Scene");
  printf(HELP_FMT, "-r, --run-time", "scene run time(s)");
  printf(HELP_FMT, "-i, --interval", "the interval(in seconds) between echo print monitor statistics, the default is 1s");
  printf(HELP_FMT, "-t --thread_cnt", "indicate the thread num");
  printf(HELP_FMT, "-R, --resource-limited-type", "indicate the resource limited type");
  printf(HELP_FMT, "", "0, network packet loss limited type");
  printf(HELP_FMT, "", "1, network bandwidth limited type");
  printf(HELP_FMT, "", "2, memory limited type");
  printf(HELP_FMT, "", "3, cpu limited type");
  printf(HELP_FMT, "--limit-run-time", "indicate the time(s) of resource limitation, dafault is 10s");
  printf(HELP_FMT, "--limit-memory", "indicate the limit meory size(MB), default is 64MB");
  printf(HELP_FMT, "--limit-cpu", "indicate the limit of cpu rate(0~1.0), default is 0.2");
  return ret;
}

OSDQScene *ObAdminObjectStorageDriverQualityExecutor::create_scene_()
{
  int ret = OB_SUCCESS;
  OSDQScene *scene = nullptr;
  ObVSliceAlloc &allocator = get_vslice_alloc_instance();
  if (params_.scene_type_ == HYBRID_TEST_SCENE) {
    if (OB_ISNULL(scene = static_cast<OSDQHybridTestScene *>(allocator.alloc(sizeof(OSDQHybridTestScene))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc memory for hybrid test scene", KR(ret), K(sizeof(OSDQHybridTestScene)));
    } else {
      scene = new (scene) OSDQHybridTestScene();
    }
  } else if (params_.scene_type_ == RESOURCE_LIMITED_SCENE) {
    if (OB_ISNULL(scene = static_cast<OSDQResourceLimitedScene *>(allocator.alloc(sizeof(OSDQResourceLimitedScene))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc memory for resource limited scene", KR(ret), K(sizeof(OSDQResourceLimitedScene)));
    } else {
      scene = new (scene) OSDQResourceLimitedScene();
    }
  } else if (params_.scene_type_ == ERRSIM_SCENE) {
    if (OB_ISNULL(scene = static_cast<OSDQErrSimScene*>(allocator.alloc(sizeof(OSDQErrSimScene))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc memory for errsim scene", KR(ret), K(sizeof(OSDQErrSimScene)));
    } else {
      scene = new (scene) OSDQErrSimScene();
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "error scene type", KR(ret));
    print_usage_();
  }
  return scene;
}

void ObAdminObjectStorageDriverQualityExecutor::free_scene_(OSDQScene *&scene)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(scene)) {
    if (params_.scene_type_ == HYBRID_TEST_SCENE) {
      dynamic_cast<OSDQHybridTestScene *>(scene)->~OSDQHybridTestScene();
    } else if (params_.scene_type_ == RESOURCE_LIMITED_SCENE) {
      dynamic_cast<OSDQResourceLimitedScene *>(scene)->~OSDQResourceLimitedScene();
    } else if (params_.scene_type_ == ERRSIM_SCENE) {
      dynamic_cast<OSDQErrSimScene *>(scene)->~OSDQErrSimScene();
    } else {
    }
    get_vslice_alloc_instance().free(scene);
    scene = nullptr;
  }
}

//=========================== OSDQFileSet ===============================

OSDQFileSet::OSDQFileSet() {}
OSDQFileSet::~OSDQFileSet()
{
  FilePathMap::iterator it = file_path_map_.begin();
  while (it != file_path_map_.end()) {
    if (it->second != nullptr) {
      free(it->second);
      it->second = nullptr;
    }
    it++;
  }
}

int OSDQFileSet::add_file(const int64_t object_id, const char *file_path)
{
  lib::ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  char *file_path_copy = nullptr;
  if (OB_UNLIKELY(object_id < 0 || file_path == nullptr))  {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the argument is invalid", KR(ret), K(object_id), K(file_path));
  } else if (OB_UNLIKELY(file_set_.find(object_id) != file_set_.end())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object_id is already in the file set", KR(ret));
  } else if (OB_ISNULL(file_path_copy = static_cast<char *>(malloc(OB_MAX_URI_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed allocate memory for file path");
  } else {
    MEMCPY(file_path_copy, file_path, OB_MAX_URI_LENGTH);
    file_set_.insert(object_id);
    file_path_map_[object_id] = file_path_copy;
  }
  return ret;
}

int OSDQFileSet::fetch_and_delete_file(int64_t &object_id, char *&file_path)
{
  lib::ObMutexGuard guard(mutex_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_set_.size() <= 0)) {
    ret = OB_FILE_NOT_EXIST;
    OB_LOG(WARN, "the file set is empty", KR(ret), K(file_set_.size()));
  } else {
    // To quickly fetch a random element from file_set, we first generate a random value from [min, max]
    // and then look for lower_bound in file_set based on it.
    // Note that the probability that each element will be fetched is not the same when the file_set are
    // sparse, but this can be allowed to happen for now.
    int64_t min_object_id = *file_set_.begin();
    int64_t max_object_id = *file_set_.rbegin();
    int64_t random_object_id = ObRandom::rand(min_object_id, max_object_id);
    std::set<int64_t>::iterator it = file_set_.lower_bound(random_object_id);
    if (OB_UNLIKELY(it == file_set_.end() || file_path_map_.find(*it) == file_path_map_.end())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "failed to get random object id", KR(ret));
    } else {
      object_id = *it;
      file_path = file_path_map_[object_id];
      file_path_map_.erase(object_id);
      file_set_.erase(it);
    }
  }
  return ret;
}

size_t OSDQFileSet::size() const
{
  return file_set_.size();
}


} // namespace tools
} // namespace oceanbase
