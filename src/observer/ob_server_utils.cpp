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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_file_system_router.h"
#include <sys/utsname.h>
#include "share/ob_version.h"

namespace oceanbase
{
using namespace common;

namespace observer
{

int ObServerUtils::get_server_ip(ObIAllocator *allocator, ObString &ipstr)
{
  int ret = OB_SUCCESS;
  ObAddr addr = GCTX.self_addr();
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid alloctor pointer is NULL", K(ret));
  } else if (!addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip to string failed", K(ret));
  } else {
    ObString ipstr_tmp = ObString::make_string(ip_buf);
    if (OB_FAIL(ob_write_string (*allocator, ipstr_tmp, ipstr))) {
      SERVER_LOG(WARN, "ob write string failed", K(ret));
    } else if (ipstr.empty()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "host ip is empty", K(ret));
    }
  }
  return ret;
}

int ObServerUtils::get_log_disk_info_in_config(int64_t& log_disk_size,
                                               int64_t& log_disk_percentage,
                                               int64_t& total_log_disk_size)
{
  int ret = OB_SUCCESS;
  int64_t suggested_data_disk_size = GCONF.datafile_size;
  int64_t suggested_data_disk_percentage = GCONF.datafile_disk_percentage;
  int64_t suggested_clog_disk_size = GCONF.log_disk_size;
  int64_t suggested_clog_disk_percentage = GCONF.log_disk_percentage;
  int64_t data_default_disk_percentage = 0;
  int64_t clog_default_disk_percentage = 0;
  int64_t data_disk_total_size = 0;
  int64_t clog_disk_total_size = 0;
  bool shared_mode = false;
  const char* data_dir = OB_FILE_SYSTEM_ROUTER.get_sstable_dir();
  const char* clog_dir = OB_FILE_SYSTEM_ROUTER.get_clog_dir();
  if (OB_FAIL(cal_all_part_disk_default_percentage(data_disk_total_size,
                                                   data_default_disk_percentage,
                                                   clog_disk_total_size,
                                                   clog_default_disk_percentage,
                                                   shared_mode))) {
    LOG_ERROR("cal all part disk default percentage failed",
        KR(ret), K(data_dir), K(suggested_data_disk_size), K(suggested_data_disk_percentage),
        K(data_default_disk_percentage), K(shared_mode));
  } else if (OB_FAIL(decide_disk_size(clog_disk_total_size,
                                      suggested_clog_disk_size,
                                      suggested_clog_disk_percentage,
                                      clog_default_disk_percentage,
                                      log_disk_size,
                                      log_disk_percentage))) {
    LOG_ERROR("decide disk size failed",
        KR(ret), K(data_dir), K(suggested_data_disk_size), K(suggested_data_disk_percentage),
        K(data_default_disk_percentage), K(shared_mode));
  } else {
    total_log_disk_size = clog_disk_total_size;
    LOG_INFO("get_log_disk_info_in_config", K(suggested_data_disk_size), K(suggested_clog_disk_size),
             K(suggested_data_disk_percentage), K(suggested_clog_disk_percentage), K(log_disk_size),
             K(log_disk_percentage), K(total_log_disk_size));
  }
  return ret;
}

int ObServerUtils::get_data_disk_info_in_config(int64_t& data_disk_size,
                                                int64_t& data_disk_percentage)
{
  int ret = OB_SUCCESS;
  int64_t suggested_data_disk_size = GCONF.datafile_size;
  int64_t suggested_data_disk_percentage = GCONF.datafile_disk_percentage;
  int64_t suggested_clog_disk_size = GCONF.log_disk_size;
  int64_t suggested_clog_disk_percentage = GCONF.log_disk_percentage;
  int64_t data_default_disk_percentage = 0;
  int64_t clog_default_disk_percentage = 0;
  int64_t data_disk_total_size = 0;
  int64_t clog_disk_total_size = 0;
  bool shared_mode = false;
  const char* data_dir = OB_FILE_SYSTEM_ROUTER.get_sstable_dir();
  const char* clog_dir = OB_FILE_SYSTEM_ROUTER.get_clog_dir();
  if (OB_FAIL(cal_all_part_disk_default_percentage(data_disk_total_size,
                                                   data_default_disk_percentage,
                                                   clog_disk_total_size,
                                                   clog_default_disk_percentage,
                                                   shared_mode))) {
    LOG_ERROR("cal all part disk default percentage failed",
        KR(ret), K(data_dir), K(suggested_data_disk_size), K(suggested_data_disk_percentage),
        K(data_default_disk_percentage), K(shared_mode));
  } else if (OB_FAIL(decide_disk_size(data_disk_total_size,
                                      suggested_data_disk_size,
                                      suggested_data_disk_percentage,
                                      data_default_disk_percentage,
                                      data_disk_size,
                                      data_disk_percentage))) {
    LOG_ERROR("decide data disk size failed",
        KR(ret), K(data_dir), K(suggested_data_disk_size), K(suggested_data_disk_percentage),
        K(data_default_disk_percentage), K(shared_mode));
  } else {
    LOG_INFO("get_data_disk_info_in_config", K(suggested_data_disk_size), K(suggested_clog_disk_size),
             K(suggested_data_disk_percentage), K(suggested_clog_disk_percentage), K(data_disk_size),
             K(data_disk_percentage));
  }
  return ret;
}

int ObServerUtils::cal_all_part_disk_size(const int64_t suggested_data_disk_size,
                                          const int64_t suggested_clog_disk_size,
                                          const int64_t suggested_data_disk_percentage,
                                          const int64_t suggested_clog_disk_percentage,
                                          int64_t& data_disk_size,
                                          int64_t& log_disk_size,
                                          int64_t& data_disk_percentage,
                                          int64_t& log_disk_percentage)
{
  int ret = OB_SUCCESS;
  int64_t total_log_disk_space = 0;
  if (OB_FAIL(get_data_disk_info_in_config(data_disk_size, data_disk_percentage))) {
    LOG_ERROR("get_data_disk_info_in_config failed", K(data_disk_size), K(data_disk_percentage));
  } else if (OB_FAIL(get_log_disk_info_in_config(log_disk_size, log_disk_percentage, total_log_disk_space))) {
    LOG_ERROR("get_log_disk_info_in_config failed", K(log_disk_size), K(log_disk_percentage));
  } else {
    LOG_INFO("cal_all_part_disk_size success", K(suggested_data_disk_size), K(suggested_clog_disk_size),
             K(suggested_data_disk_percentage), K(suggested_clog_disk_percentage), K(data_disk_size),
             K(log_disk_size), K(data_disk_percentage), K(log_disk_percentage));
  }
  return ret;
}

int ObServerUtils::check_slog_data_binding(
    const char *sstable_dir,
    const char *slog_dir)
{
  int ret = OB_SUCCESS;
  struct statvfs sstable_svfs;
  struct statvfs slog_svfs;
  if (OB_ISNULL(sstable_dir) || OB_ISNULL(slog_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), KP(sstable_dir), KP(slog_dir));
  } else if (OB_UNLIKELY(0 != statvfs(sstable_dir, &sstable_svfs))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to get sstable directory vfs", K(ret), K(sstable_dir));
  } else if (OB_UNLIKELY(0 != statvfs(slog_dir, &slog_svfs))) {
    ret = OB_IO_ERROR;
    LOG_WARN("fail to get slog directory vfs", K(ret), K(slog_dir));
  } else if (sstable_svfs.f_fsid != slog_svfs.f_fsid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("SLOG and datafile must be on the same disk", K(ret), K(sstable_svfs.f_fsid),
        K(slog_svfs.f_fsid));
  }
  return ret;
}

const char *ObServerUtils::build_syslog_file_info(const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  const static int64_t max_info_len = 512;
  static char info[max_info_len];

  // self address
  const char *self_addr = addr.is_valid() ? to_cstring(addr) : "";

  // OS info
  struct utsname uts;
  if (0 != ::uname(&uts)) {
    ret = OB_ERR_SYS;
    LOG_WARN("call uname failed");
  }

  // time zone info
  int gmtoff_hour = 0;
  int gmtoff_minute = 0;
  if (OB_SUCC(ret)) {
    time_t t = time(NULL);
    struct tm lt;
    if (NULL == localtime_r(&t, &lt)) {
      ret = OB_ERR_SYS;
      LOG_WARN("call localtime failed");
    } else {
      gmtoff_hour = (std::abs(lt.tm_gmtoff) / 3600) * (lt.tm_gmtoff < 0 ? -1 : 1);
      gmtoff_minute = std::abs(lt.tm_gmtoff) % 3600 / 60;
    }
  }

  if (OB_SUCC(ret)) {
    int n = snprintf(info, max_info_len,
                     "address: %s, observer version: %s, revision: %s, "
                     "sysname: %s, os release: %s, machine: %s, tz GMT offset: %02d:%02d",
                     self_addr, PACKAGE_STRING, build_version(),
                     uts.sysname, uts.release, uts.machine, gmtoff_hour, gmtoff_minute);
    if (n <= 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("snprintf failed");
    }
  }

  return OB_SUCCESS == ret ? info : nullptr;
}

/*
 * calc actual_extend_size, following the rules:
 *  1. if datafile_next less than 1G, actual_extend_size equal to min(1G, datafile_maxsize * 10%)
 *  2. if datafile_next large than 1G, actual_extend_size equal to min(datafile_next, max_extend_file)
*/
int ObServerUtils::calc_auto_extend_size(int64_t &actual_extend_size)
{
  int ret = OB_SUCCESS;

  const int64_t datafile_maxsize = GCONF.datafile_maxsize;
  const int64_t datafile_next = GCONF.datafile_next;
  const int64_t datafile_size =
    OB_SERVER_BLOCK_MGR.get_total_macro_block_count() * OB_SERVER_BLOCK_MGR.get_macro_block_size();

  if (OB_UNLIKELY(datafile_maxsize <= 0) ||
      OB_UNLIKELY(datafile_size) <= 0 ||
      OB_UNLIKELY(datafile_next) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument",
      K(ret), K(datafile_maxsize), K(datafile_size), K(datafile_next));
  } else {
    // attention: max_extend_file maybe equal to zero in the following situations:
    // 1. alter datafile_size as A, alter datafile_maxsize as B, and A < B
    // 2. auto extend to size to C ( A < C < B )
    // 3. alter datafile_maxsize as D ( A < D < C )
    int64_t max_extend_file = datafile_maxsize - datafile_size;
    const int64_t datafile_next_minsize = 1 * 1024 * 1024 * 1024; // 1G
    if (datafile_next < datafile_next_minsize) {
      int64_t min_extend_size = datafile_maxsize * 10 / 100;
      actual_extend_size =
        min_extend_size < datafile_next_minsize ? min_extend_size : datafile_next_minsize;
      if (actual_extend_size > max_extend_file) { // take the smaller
        actual_extend_size = max_extend_file;
      }
    } else {
      actual_extend_size =
        datafile_next < max_extend_file ? datafile_next : max_extend_file;
    }
    if (actual_extend_size <= 0) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      if (REACH_TIME_INTERVAL(300 * 1000 * 1000L)) { // 5 min
        LOG_INFO("No more disk space to extend, is full",
          K(ret), K(datafile_maxsize), K(datafile_size));
      }
    } else {
      actual_extend_size += datafile_size; // suggest block file size
    }
  }
  return ret;
}

int ObServerUtils::cal_all_part_disk_default_percentage(int64_t& data_disk_total_size,
                                                        int64_t& data_disk_default_percentage,
                                                        int64_t& clog_disk_total_size,
                                                        int64_t& clog_disk_default_percentage,
                                                        bool &shared_mode)
{
  int ret = OB_SUCCESS;

// background information about default disk percentage:
// If not in shared mode, disk will be used up to 90%.
// If in shared mode, data and clog disk usage will be up to 60% and 30%
  const int64_t DEFAULT_DISK_PERCENTAGE_IN_SEPRATE_MODE = 90;
  const int64_t DEFAULT_DATA_DISK_PERCENTAGE_IN_SHARED_MODE = 60;
  const int64_t DEFAULT_CLOG_DISK_PERCENTAGE_IN_SHARED_MODE = 30;

  // We use sstable_dir as the data disk directory to identify whether the log and data are located
  // on the same file system, and the storage module will ensure that sstable_dir and slog_dir are
  // located on the same file system;
  const char* data_dir = OB_FILE_SYSTEM_ROUTER.get_sstable_dir();
  const char* clog_dir = OB_FILE_SYSTEM_ROUTER.get_clog_dir();

  struct statvfs data_statvfs;
  struct statvfs clog_statvfs;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(0 != statvfs(data_dir, &data_statvfs))) {
      LOG_ERROR("Failed to get data disk space ", KR(ret), K(data_dir), K(errno));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_UNLIKELY(0 != statvfs(clog_dir, &clog_statvfs))) {
      LOG_ERROR("Failed to get clog disk space ", KR(ret), K(clog_dir), K(errno));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  if (OB_SUCC(ret)) {
    if (data_statvfs.f_fsid == clog_statvfs.f_fsid) {
      shared_mode = true;
      data_disk_default_percentage = DEFAULT_DATA_DISK_PERCENTAGE_IN_SHARED_MODE;
      clog_disk_default_percentage = DEFAULT_CLOG_DISK_PERCENTAGE_IN_SHARED_MODE;
    } else {
      shared_mode = false;
      data_disk_default_percentage = DEFAULT_DISK_PERCENTAGE_IN_SEPRATE_MODE;
      clog_disk_default_percentage = DEFAULT_DISK_PERCENTAGE_IN_SEPRATE_MODE;
    }
    data_disk_total_size = (data_statvfs.f_blocks + data_statvfs.f_bavail - data_statvfs.f_bfree) * data_statvfs.f_bsize;
    clog_disk_total_size = (clog_statvfs.f_blocks + clog_statvfs.f_bavail - clog_statvfs.f_bfree) * clog_statvfs.f_bsize;
    LOG_INFO("cal_all_part_disk_default_percentage succ",
        K(data_dir), K(clog_dir),
        K(shared_mode), K(data_disk_total_size), K(data_disk_default_percentage),
        K(clog_disk_total_size), K(clog_disk_default_percentage));
  }

  return ret;
}

int ObServerUtils::decide_disk_size(const int64_t total_space,
                                    const int64_t suggested_disk_size,
                                    const int64_t suggested_disk_percentage,
                                    const int64_t default_disk_percentage,
                                    int64_t& disk_size,
                                    int64_t& disk_percentage)
{
  int ret = OB_SUCCESS;

  if (suggested_disk_size <= 0) {
    int64_t disk_percentage = 0;
    if (suggested_disk_percentage <= 0) {
      disk_percentage = default_disk_percentage;
    } else {
      disk_percentage = suggested_disk_percentage;
    }
    disk_size = total_space * disk_percentage / 100;
  } else {
    disk_size = suggested_disk_size;
  }
  if (disk_size > total_space) {
    LOG_WARN("disk_size is greater than total disk space", KR(OB_SERVER_OUTOF_DISK_SPACE),
          K(suggested_disk_size), K(suggested_disk_percentage),
          K(default_disk_percentage),
          K(total_space), K(disk_size));
  }

  LOG_INFO("decide disk size finished",
        K(suggested_disk_size), K(suggested_disk_percentage),
        K(default_disk_percentage),
        K(total_space), K(disk_size));
  return ret;
}

} // namespace observer
} // namespace oceanbase

