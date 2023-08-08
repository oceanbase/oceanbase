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

#ifndef OCEANBASE_OB_SERVER_UTILS_
#define OCEANBASE_OB_SERVER_UTILS_

#include <sys/statvfs.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
}
using common::ObIAllocator;
using common::ObString;

namespace observer
{
class ObServerUtils
{
public:
  // Get the server's ipstr.
  // @param [in] allocator.
  // @param [out] server's ip str.
  // @return the error code.
  static int get_server_ip(ObIAllocator *allocator, ObString &ipstr);

  static int get_log_disk_info_in_config(int64_t& log_disk_size,
                                         int64_t& log_disk_percentage,
                                         int64_t& total_log_disk_size);

  static int get_data_disk_info_in_config(int64_t& data_disk_size,
                                          int64_t& data_disk_percentage);

  static int cal_all_part_disk_size(const int64_t suggested_data_disk_size,
                                    const int64_t suggested_log_disk_size,
                                    const int64_t suggested_data_disk_percentage,
                                    const int64_t suggested_log_disk_percentage,
                                    int64_t& data_disk_size,
                                    int64_t& log_disk_size,
                                    int64_t& data_disk_percentage,
                                    int64_t& log_disk_percentage);
  static int check_slog_data_binding(const char *sstable_dir,
                                     const char *slog_dir);

  // Build info for syslog files which is logged when new file created.
  // The following infors are included:
  // - self address
  // - observer version
  // - OS info
  // - timezone info
  static const char *build_syslog_file_info(const common::ObAddr &addr);
  static int calc_auto_extend_size(int64_t &actual_extend_size);

private:
  static int decide_disk_size(const struct statvfs& svfs,
                              const int64_t suggested_disk_size,
                              const int64_t suggested_disk_percentage,
                              const int64_t default_disk_percentage,
                              const char* dir,
                              int64_t& disk_size,
                              int64_t& disk_percentage);
  static int decide_disk_size(const int64_t disk_total_size,
                              const int64_t suggested_disk_size,
                              const int64_t suggested_disk_percentage,
                              const int64_t default_disk_percentage,
                              int64_t& disk_size,
                              int64_t& disk_percentage);
  static int cal_all_part_disk_default_percentage(int64_t &data_disk_total_size,
                                                  int64_t& data_disk_percentage,
                                                  int64_t &log_disk_total_size,
                                                  int64_t& log_disk_percentage,
                                                  bool &shared_mode);
  DISALLOW_COPY_AND_ASSIGN(ObServerUtils);
};
} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OB_SERVER_UTILS_
