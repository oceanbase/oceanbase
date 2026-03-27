/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OB_CHECK_PARAMS_
#define OCEANBASE_OBSERVER_OB_CHECK_PARAMS_

#include "deps/oblib/src/lib/file/file_directory_utils.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include <fstream>
#include <sys/resource.h>
namespace oceanbase
{
using namespace common;
namespace observer
{
using namespace common;
class CheckAllParams
{
public:
  static int check_all_params(bool strict_check);

private:
  static int read_one_line(const char* file_path, char* buffer, size_t buffer_size);
  static bool is_path_valid(const char *file_name);
  static int check_vm_max_map_count(bool strict_check);         // 1
  static int check_vm_min_free_kbytes(bool strict_check);    // 2
  static int check_vm_overcommit_memory(bool strict_check);  // 3
  static int check_fs_file_max(bool strict_check);
  static int check_ulimit_open_files(bool strict_check);
  static int check_ulimit_max_user_processes(bool strict_check);
  static int check_ulimit_core_file_size(bool strict_check);
  static int check_ulimit_stack_size(bool strict_check);  // 8
  static int check_current_clocksource(bool strict_check);  // 9
};

int check_os_params(bool strict_check_params);

}  // namespace observer
}  // namespace oceanbase

#endif
