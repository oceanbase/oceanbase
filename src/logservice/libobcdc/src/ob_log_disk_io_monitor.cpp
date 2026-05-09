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
 * Disk IO bandwidth monitor implementation
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_disk_io_monitor.h"
#include "ob_log_utils.h"                     // _G_, _M_, get_timestamp
#include "lib/oblog/ob_log.h"                 // ObLogger
#include "lib/oblog/ob_log_module.h"          // LOG_*
#include "lib/ob_errno.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

namespace oceanbase
{
namespace libobcdc
{

const char *ObLogDiskIOMonitor::PROC_SELF_IO_PATH = "/proc/self/io";

void DiskIOStatInfo::reset()
{
  read_bytes_ = 0;
  write_bytes_ = 0;
  read_ios_ = 0;
  write_ios_ = 0;
  last_read_bytes_ = 0;
  last_write_bytes_ = 0;
  last_read_ios_ = 0;
  last_write_ios_ = 0;
  last_stat_time_ = 0;
}

int DiskIOStatInfo::update_stat()
{
  int ret = OB_SUCCESS;
  // This method is called by ObLogDiskIOMonitor::update_stat()
  // The actual update is done in ObLogDiskIOMonitor
  UNUSED(ret);
  return ret;
}

double DiskIOStatInfo::calc_read_bandwidth_mbps(const int64_t delta_time)
{
  double bandwidth = 0.0;
  const uint64_t local_read_bytes = ATOMIC_LOAD(&read_bytes_);
  const uint64_t local_last_read_bytes = ATOMIC_LOAD(&last_read_bytes_);
  const uint64_t delta_read_bytes = local_read_bytes - local_last_read_bytes;
  double delta_read_bytes_mb = (double)delta_read_bytes / (double)_M_;

  if (delta_time > 0) {
    bandwidth = (double)(delta_read_bytes_mb) * 1000000.0 / (double)delta_time;
  }

  // Update last statistics
  ATOMIC_STORE(&last_read_bytes_, local_read_bytes);

  return bandwidth;
}

double DiskIOStatInfo::calc_write_bandwidth_mbps(const int64_t delta_time)
{
  double bandwidth = 0.0;
  const uint64_t local_write_bytes = ATOMIC_LOAD(&write_bytes_);
  const uint64_t local_last_write_bytes = ATOMIC_LOAD(&last_write_bytes_);
  const uint64_t delta_write_bytes = local_write_bytes - local_last_write_bytes;
  double delta_write_bytes_mb = (double)delta_write_bytes / (double)_M_;

  if (delta_time > 0) {
    bandwidth = (double)(delta_write_bytes_mb) * 1000000.0 / (double)delta_time;
  }

  // Update last statistics
  ATOMIC_STORE(&last_write_bytes_, local_write_bytes);

  return bandwidth;
}

double DiskIOStatInfo::calc_read_iops(const int64_t delta_time)
{
  double iops = 0.0;
  const uint64_t local_read_ios = ATOMIC_LOAD(&read_ios_);
  const uint64_t local_last_read_ios = ATOMIC_LOAD(&last_read_ios_);
  const uint64_t delta_read_ios = local_read_ios - local_last_read_ios;

  if (delta_time > 0) {
    iops = (double)(delta_read_ios) * 1000000.0 / (double)delta_time;
  }

  // Update last statistics
  ATOMIC_STORE(&last_read_ios_, local_read_ios);

  return iops;
}

double DiskIOStatInfo::calc_write_iops(const int64_t delta_time)
{
  double iops = 0.0;
  const uint64_t local_write_ios = ATOMIC_LOAD(&write_ios_);
  const uint64_t local_last_write_ios = ATOMIC_LOAD(&last_write_ios_);
  const uint64_t delta_write_ios = local_write_ios - local_last_write_ios;

  if (delta_time > 0) {
    iops = (double)(delta_write_ios) * 1000000.0 / (double)delta_time;
  }

  // Update last statistics
  ATOMIC_STORE(&last_write_ios_, local_write_ios);

  return iops;
}

double DiskIOStatInfo::get_total_read_bytes_gb() const
{
  double total_size = 0.0;
  const uint64_t local_read_bytes = ATOMIC_LOAD(&read_bytes_);
  total_size = (double)local_read_bytes / (double)_G_;
  return total_size;
}

double DiskIOStatInfo::get_total_write_bytes_gb() const
{
  double total_size = 0.0;
  const uint64_t local_write_bytes = ATOMIC_LOAD(&write_bytes_);
  total_size = (double)local_write_bytes / (double)_G_;
  return total_size;
}

ObLogDiskIOMonitor::ObLogDiskIOMonitor() : inited_(false), last_print_time_(0), stat_info_()
{
}

ObLogDiskIOMonitor::~ObLogDiskIOMonitor()
{
  destroy();
}

int ObLogDiskIOMonitor::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogDiskIOMonitor has been initialized");
    ret = OB_INIT_TWICE;
  } else {
    stat_info_.reset();
    inited_ = true;
    LOG_INFO("ObLogDiskIOMonitor init succ");
  }

  return ret;
}

void ObLogDiskIOMonitor::destroy()
{
  if (inited_) {
    reset();
    inited_ = false;
    LOG_INFO("ObLogDiskIOMonitor destroyed");
  }
}

void ObLogDiskIOMonitor::reset()
{
  last_print_time_ = 0;
  stat_info_.reset();
}

int ObLogDiskIOMonitor::update_stat()
{
  int ret = OB_SUCCESS;
  int fd = -1;
  char buf[1024] = {0};
  ssize_t read_len = 0;

  if (OB_UNLIKELY(!inited_)) {
    LOG_ERROR("ObLogDiskIOMonitor has not been initialized");
    ret = OB_NOT_INIT;
  } else {
    // Open /proc/self/io
    fd = open(PROC_SELF_IO_PATH, O_RDONLY);
    if (fd < 0) {
      ret = OB_IO_ERROR;
      LOG_WARN("open /proc/self/io failed", KR(ret), K(errno));
    } else {
      // Read file content
      read_len = read(fd, buf, sizeof(buf) - 1);
      if (read_len < 0) {
        ret = OB_IO_ERROR;
        LOG_WARN("read /proc/self/io failed", KR(ret), K(errno));
      } else if (read_len == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read /proc/self/io returned 0 bytes");
      } else {
        buf[read_len] = '\0';
        // Parse the content
        if (OB_FAIL(parse_proc_io_(buf, read_len))) {
          LOG_WARN("parse_proc_io_ failed", KR(ret));
        }
      }

      close(fd);
      fd = -1;
    }
  }

  return ret;
}

int ObLogDiskIOMonitor::parse_proc_io_(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const char *line_end = NULL;
  uint64_t rchar = 0, wchar = 0, syscr = 0, syscw = 0;
  uint64_t read_bytes = 0, write_bytes = 0, cancelled_write_bytes = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    // Parse each line
    // Format: "key: value\n"
    const char *p = buf;
    const char *end = buf + buf_len;

    while (p < end && OB_SUCC(ret)) {
      // Find line end
      line_end = strchr(p, '\n');
      if (NULL == line_end) {
        line_end = end;
      }

      // Parse line, check sscanf return value to guard against format changes
      if (strncmp(p, "rchar:", 6) == 0) {
        if (1 != sscanf(p, "rchar: %lu", &rchar)) {
          LOG_WARN("failed to parse rchar from /proc/self/io");
        }
      } else if (strncmp(p, "wchar:", 6) == 0) {
        if (1 != sscanf(p, "wchar: %lu", &wchar)) {
          LOG_WARN("failed to parse wchar from /proc/self/io");
        }
      } else if (strncmp(p, "syscr:", 6) == 0) {
        if (1 != sscanf(p, "syscr: %lu", &syscr)) {
          LOG_WARN("failed to parse syscr from /proc/self/io");
        }
      } else if (strncmp(p, "syscw:", 6) == 0) {
        if (1 != sscanf(p, "syscw: %lu", &syscw)) {
          LOG_WARN("failed to parse syscw from /proc/self/io");
        }
      } else if (strncmp(p, "read_bytes:", 11) == 0) {
        if (1 != sscanf(p, "read_bytes: %lu", &read_bytes)) {
          LOG_WARN("failed to parse read_bytes from /proc/self/io");
        }
      } else if (strncmp(p, "write_bytes:", 12) == 0) {
        if (1 != sscanf(p, "write_bytes: %lu", &write_bytes)) {
          LOG_WARN("failed to parse write_bytes from /proc/self/io");
        }
      } else if (strncmp(p, "cancelled_write_bytes:", 22) == 0) {
        if (1 != sscanf(p, "cancelled_write_bytes: %lu", &cancelled_write_bytes)) {
          LOG_WARN("failed to parse cancelled_write_bytes from /proc/self/io");
        }
      }

      // Move to next line
      if (line_end < end) {
        p = line_end + 1;
      } else {
        p = end;
      }
    }

    // Update statistics
    // Note: read_bytes and write_bytes are the actual bytes read/written from/to storage
    // syscr and syscw are the number of read/write system calls
    ATOMIC_STORE(&stat_info_.read_bytes_, read_bytes);
    ATOMIC_STORE(&stat_info_.write_bytes_, write_bytes);
    ATOMIC_STORE(&stat_info_.read_ios_, syscr);
    ATOMIC_STORE(&stat_info_.write_ios_, syscw);
  }

  return ret;
}

int64_t ObLogDiskIOMonitor::get_timestamp() const
{
  return ::oceanbase::libobcdc::get_timestamp();
}

// NOTE: This function must only be called from a single thread (the storager stat timer thread).
// It is not thread-safe due to non-atomic read/write of last_print_time_ and stat_info_ internal state.
void ObLogDiskIOMonitor::print_stat()
{
  int ret = OB_SUCCESS;
  int64_t current_time = get_timestamp();
  int64_t delta_time = current_time - last_print_time_;

  if (delta_time >= PRINT_DISK_IO_STAT_INTERVAL || last_print_time_ == 0) {
    // Update statistics first
    if (OB_FAIL(update_stat())) {
      LOG_WARN("update_stat failed", KR(ret));
    } else {
      // Calculate bandwidth and IOPS
      double read_bandwidth = stat_info_.calc_read_bandwidth_mbps(delta_time);
      double write_bandwidth = stat_info_.calc_write_bandwidth_mbps(delta_time);
      double read_iops = stat_info_.calc_read_iops(delta_time);
      double write_iops = stat_info_.calc_write_iops(delta_time);
      double total_read_gb = stat_info_.get_total_read_bytes_gb();
      double total_write_gb = stat_info_.get_total_write_bytes_gb();

      // Print statistics
      _LOG_INFO("[DISK_IO] [STAT] READ_BW=%.3fMB/s WRITE_BW=%.3fMB/s "
                "READ_IOPS=%.2f WRITE_IOPS=%.2f "
                "TOTAL_READ=%.3fGB TOTAL_WRITE=%.3fGB",
                read_bandwidth, write_bandwidth,
                read_iops, write_iops,
                total_read_gb, total_write_gb);
    }

    last_print_time_ = current_time;
  }
}

} // namespace libobcdc
} // namespace oceanbase
