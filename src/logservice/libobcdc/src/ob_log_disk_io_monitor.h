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
 *
 * Disk IO bandwidth monitor
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_DISK_IO_MONITOR_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_DISK_IO_MONITOR_H__

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"         // TO_STRING_KV
#include "lib/utility/ob_macro_utils.h"         // DISALLOW_COPY_AND_ASSIGN, CACHE_ALIGNED

namespace oceanbase
{
namespace libobcdc
{

/**
 * Disk IO statistics information
 * Monitor process-level disk IO bandwidth by reading /proc/self/io
 */
struct DiskIOStatInfo
{
  uint64_t read_bytes_ CACHE_ALIGNED;              // Total bytes read from storage
  uint64_t write_bytes_ CACHE_ALIGNED;             // Total bytes written to storage
  uint64_t read_ios_ CACHE_ALIGNED;                // Total read I/O operations
  uint64_t write_ios_ CACHE_ALIGNED;               // Total write I/O operations
  uint64_t last_read_bytes_ CACHE_ALIGNED;         // Last read bytes for rate calculation
  uint64_t last_write_bytes_ CACHE_ALIGNED;       // Last write bytes for rate calculation
  uint64_t last_read_ios_ CACHE_ALIGNED;           // Last read IOs for IOPS calculation
  uint64_t last_write_ios_ CACHE_ALIGNED;          // Last write IOs for IOPS calculation
  int64_t last_stat_time_ CACHE_ALIGNED;          // Last statistics time

  DiskIOStatInfo() { reset(); }
  ~DiskIOStatInfo() { reset(); }

  void reset();
  int update_stat();
  double calc_read_bandwidth_mbps(const int64_t delta_time);
  double calc_write_bandwidth_mbps(const int64_t delta_time);
  double calc_read_iops(const int64_t delta_time);
  double calc_write_iops(const int64_t delta_time);
  double get_total_read_bytes_gb() const;
  double get_total_write_bytes_gb() const;

  TO_STRING_KV(K_(read_bytes),
               K_(write_bytes),
               K_(read_ios),
               K_(write_ios),
               K_(last_read_bytes),
               K_(last_write_bytes),
               K_(last_read_ios),
               K_(last_write_ios),
               K_(last_stat_time));
};

/**
 * Disk IO monitor class
 * Provides interface to monitor and print disk IO statistics
 */
class ObLogDiskIOMonitor
{
public:
  ObLogDiskIOMonitor();
  ~ObLogDiskIOMonitor();

public:
  int init();
  void destroy();
  void reset();

  // Update statistics from /proc/self/io
  int update_stat();

  // Calculate and print disk IO statistics
  void print_stat();

  // Get current statistics
  DiskIOStatInfo& get_stat_info() { return stat_info_; }
  const DiskIOStatInfo& get_stat_info() const { return stat_info_; }

private:
  static const int64_t PRINT_DISK_IO_STAT_INTERVAL = 10 * 1000 * 1000;  // 10 seconds
  static const char *PROC_SELF_IO_PATH;

  int parse_proc_io_(const char *buf, const int64_t buf_len);
  int64_t get_timestamp() const;

private:
  bool inited_;
  int64_t last_print_time_;
  DiskIOStatInfo stat_info_;
  DISALLOW_COPY_AND_ASSIGN(ObLogDiskIOMonitor);
};

} // namespace libobcdc
} // namespace oceanbase

#endif /* OCEANBASE_LIBOBCDC_OB_LOG_DISK_IO_MONITOR_H__ */
