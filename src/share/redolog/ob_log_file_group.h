/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_LOG_FILE_GROUP_H_
#define OCEANBASE_COMMON_OB_LOG_FILE_GROUP_H_

#include <stdint.h>
#include "lib/oblog/ob_log.h"
#include "lib/ob_define.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "common/storage/ob_io_device.h"
#include "share/redolog/ob_log_definition.h"

namespace oceanbase
{
namespace common
{
// TODO: remove this definition
const int64_t CLOG_FILE_SIZE = 1 << 26;
class ObLogFileGroup
{
public:
  ObLogFileGroup();
  ~ObLogFileGroup();
public:
  int init(const char *log_dir);
  void destroy();
  // interface
  int get_file_id_range(int64_t &min_file_id, int64_t &max_file_id);
  int get_total_disk_space(int64_t &total_space) const;
  int get_total_used_size(int64_t &total_size) const;
  void update_min_file_id(const int64_t file_id);
  void update_max_file_id(const int64_t file_id);
private:
  static int check_file_existence(const char *dir, const int64_t file_id, bool &b_exist);
private:
  bool is_inited_;
  int64_t min_file_id_;
  int64_t min_using_file_id_;
  int64_t max_file_id_;
  const char *log_dir_;
  int64_t total_disk_size_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_FILE_GROUP_H_
