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
#include "observer/ob_check_params.h"

namespace oceanbase
{
using namespace common;
namespace observer
{
int CheckAllParams::check_all_params(bool strict_check = true)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_FAIL(check_vm_max_map_count(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_vm_min_free_kbytes(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_vm_overcommit_memory(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_fs_file_max(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_ulimit_open_files(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_ulimit_max_user_processes(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_ulimit_core_file_size(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_ulimit_stack_size(strict_check))) {}

  if (OB_SUCC(ret) && OB_FAIL(check_current_clocksource(strict_check))) {}
  return ret;
}

int CheckAllParams::read_one_int(const char *file_name, int64_t &value)
{
  int ret = OB_SUCCESS;
  FILE *fp = fopen(file_name, "r");
  if (fp != nullptr) {
    if (1 != fscanf(fp, "%ld", &value)) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Failed to read integer from file", K(ret));
    }
    fclose(fp);
  } else {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("File does not exist", K(ret));
  }
  return ret;
}

bool CheckAllParams::is_path_valid(const char *file_name)
{
  return (access(file_name, F_OK) != -1);
}

int CheckAllParams::check_vm_max_map_count(bool strict_check)
{
  int ret = OB_SUCCESS;
  const char *file_path = "/proc/sys/vm/max_map_count";
  if (is_path_valid(file_path)) {
    int64_t max_map_count = 0;
    if (OB_FAIL(read_one_int(file_path, max_map_count))) {
      LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL,
          OB_ERR_UNEXPECTED,
          "[check OS params]:read file failed. ",
          "file_path is ",
          file_path,
          ". [suggestion] Please ensure vm.max_map_count is readable.");
    } else if (max_map_count >= 327600) {
      LOG_INFO("[check OS params]:vm.max_map_count is within the range", K(max_map_count));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_MAX_MAP_COUNT_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:vm.max_map_count is less than 327600. ",
            "max_map_count is ",
            max_map_count,
            ". [suggestion] Please modify the value of vm.max_map_count to ensure that it is greater than 327600.");
      } else {
        LOG_WARN("[check OS params]:vm.max_map_count is less than 327600", K(max_map_count));
      }
    }
  } else {
    LOG_WARN("file path does not exist", K(file_path));
  }
  return ret;
}

int CheckAllParams::check_vm_min_free_kbytes(bool strict_check)
{
  int ret = OB_SUCCESS;
  const char *file_path = "/proc/sys/vm/min_free_kbytes";
  if (is_path_valid(file_path)) {
    int64_t vm_min_free_kbytes = 0;
    if (OB_FAIL(read_one_int(file_path, vm_min_free_kbytes))) {
      LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL,
          OB_ERR_UNEXPECTED,
          "[check OS params]:read file failed. ",
          "file_path is ",
          file_path,
          ". [suggestion] Please ensure vm.min_free_kbytes is readable.");
    } else if (vm_min_free_kbytes >= 32768 && vm_min_free_kbytes <= 2097152) {
      LOG_INFO("[check OS params]:vm.min_free_kbytes is within the range", K(vm_min_free_kbytes));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_MIN_FREE_KBYTES_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:vm.min_free_kbytes is not within the allowed range [32768, 2097152]. ",
            "vm.min_free_kbytes is ",
            vm_min_free_kbytes,
            ". [suggestion] Please modify the value of vm.min_free_kbytes to ensure that it is in [32768, 2097152].");
      } else {
        LOG_WARN("[check OS params]:vm.min_free_kbytes is not within the allowed range [32768, 2097152]",
            K(vm_min_free_kbytes));
      }
    }
  } else {
    LOG_WARN("file path does not exist", K(file_path));
  }
  return ret;
}

int CheckAllParams::check_vm_overcommit_memory(bool strict_check)
{
  int ret = OB_SUCCESS;
  const char *file_path = "/proc/sys/vm/overcommit_memory";
  if (is_path_valid(file_path)) {
    int64_t vm_overcommit_memory = 0;
    if (OB_FAIL(read_one_int(file_path, vm_overcommit_memory))) {
      LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL,
          OB_ERR_UNEXPECTED,
          "[check OS params]:read file failed. ",
          "file_path is ",
          file_path,
          ". [suggestion] Please ensure vm.overcommit_memory is readable.");
    } else if (vm_overcommit_memory == 0) {
      LOG_INFO("[check OS params]:vm.overcommit_memory is equal to 0", K(vm_overcommit_memory));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_OVERCOMMIT_MEMORY_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:vm.overcommit_memory is not the value:0. ",
            "vm.overcommit_memory is ",
            vm_overcommit_memory,
            ". [suggestion] Please modify the value of vm.overcommit_memory to ensure that it is 0.");
      } else {
        LOG_WARN("[check OS params]:vm.overcommit_memory is not within the allowed value:0", K(vm_overcommit_memory));
      }
    }
  } else {
    LOG_WARN("file path does not exist", K(file_path));
  }
  return ret;
}

int CheckAllParams::check_fs_file_max(bool strict_check)
{
  int ret = OB_SUCCESS;
  const char *file_path = "/proc/sys/fs/file-max";
  if (is_path_valid(file_path)) {
    int64_t fs_file_max = 0;
    if (OB_FAIL(read_one_int(file_path, fs_file_max))) {
      LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL,
          OB_ERR_UNEXPECTED,
          "[check OS params]:read file failed. ",
          "file_path is ",
          file_path,
          ". [suggestion] Please ensure fs.file-max is readable.");
    } else if (fs_file_max >= 6573688) {
      LOG_INFO("[check OS params]:fs.file-max is greater than or equal to 6573688", K(fs_file_max));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_FS_FILE_MAX_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:fs.file-max is less than 6573688. ",
            "fs.file-max is ",
            fs_file_max,
            ". [suggestion] Please modify the value of fs.file-max to ensure that it is greater than or equal to 6573688.");
      } else {
        LOG_WARN("[check OS params]:fs.file-max is less than 6573688", K(fs_file_max));
      }
    }
  } else {
    LOG_WARN("file path does not exist", K(file_path));
  }
  return ret;
}

int CheckAllParams::check_ulimit_open_files(bool strict_check)
{
  int ret = OB_SUCCESS;
  // Check open files limit
  struct rlimit rlim;
  if (getrlimit(RLIMIT_NOFILE, &rlim) == 0) {
    if (rlim.rlim_cur >= 655300 && rlim.rlim_max >= 655300) {
      LOG_INFO(
          "[check OS params]:open files limit is greater than or equal to 655300", K(rlim.rlim_cur), K(rlim.rlim_max));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_ULIMIT_OPEN_FILES_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:open files limit's soft nofile or hard nofile is less than 655300.",
            " soft nofile is ",
            rlim.rlim_cur,
            " hard nofile is ",
            rlim.rlim_max,
            ". [suggestion] Please run 'ulimit -n' to confirm the current limit and find a way to set it to the proper value.");
      } else {
        LOG_WARN("[check OS params]:open files limit's soft nofile or hard nofile is less than 655300, please run "
                 "'ulimit -n' to confirm the current limit and find a way to set it to the proper value",
            K(rlim.rlim_cur),
            K(rlim.rlim_max));
      }
    }
  } else {
    LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL, OB_FILE_NOT_EXIST, "read ulimit.open_file_limit file failed ");
  }
  return ret;
}

int CheckAllParams::check_ulimit_max_user_processes(bool strict_check)
{
  int ret = OB_SUCCESS;
  struct rlimit rlim;
  // Check process limit
  if (getrlimit(RLIMIT_NPROC, &rlim) == 0) {
    if (rlim.rlim_cur >= 655300 && rlim.rlim_max >= 655300) {
      LOG_INFO("[check OS params]:ulimit.max_user_processes is greater than or equal to 655300",
          K(rlim.rlim_cur),
          K(rlim.rlim_max));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_ULIMIT_MAX_USER_PROCESSES_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:ulimit.max_user_processes's soft nofile or hard nofile is less than 655300.",
            " soft nofile is ",
            rlim.rlim_cur,
            " hard nofile is ",
            rlim.rlim_max,
            ". [suggestion] Please run 'ulimit -u' to confirm the current limit and find a way to set it to the proper "
            "value");
      } else {
        LOG_WARN("[check OS params]:ulimit.max_user_processes's soft nofile or hard nofile is less than 655300, please "
                 "run 'ulimit -u' to confirm the current limit and find a way to set it to the proper value",
            K(rlim.rlim_cur),
            K(rlim.rlim_max));
      }
    }
  } else {
    LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL, OB_FILE_NOT_EXIST, "read ulimit.max_user_processes file failed ");
  }
  return ret;
}
int CheckAllParams::check_ulimit_core_file_size(bool strict_check)
{
  int ret = OB_SUCCESS;
  struct rlimit rlim;
  // Check core file size limit
  if (getrlimit(RLIMIT_CORE, &rlim) == 0) {
    if (rlim.rlim_cur == RLIM_INFINITY && rlim.rlim_max == RLIM_INFINITY) {
      LOG_INFO("[check OS params]:core file size limit is unlimited");
    } else {
      // Violations of the recommended range will only trigger a warning, regardless of strict or non-strict checking.
      LOG_WARN("[check OS params]:ulimit.core_file_size limit's soft nofile or hard nofile is limited",
          K(rlim.rlim_cur),
          K(rlim.rlim_max));
    }
  } else {
    LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL, OB_FILE_NOT_EXIST, "read ulimit.core_file_size file failed ");
  }
  return ret;
}

int CheckAllParams::check_ulimit_stack_size(bool strict_check)
{
  int ret = OB_SUCCESS;
  struct rlimit rlim;
  // Check stack size limit
  if (getrlimit(RLIMIT_STACK, &rlim) == 0) {
    if (rlim.rlim_cur >= (1 << 20) && rlim.rlim_max >= (1 << 20)) {
      LOG_INFO("[check OS params]:stack size limit is larger than 1M", K(rlim.rlim_cur), K(rlim.rlim_max));
    } else {
      if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_ULIMIT_STACK_SIZE_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:ulimit.stack_size limit's soft nofile or hard nofile is smaller than 1M.",
            " soft nofile is ",
            rlim.rlim_cur,
            " hard nofile is ",
            rlim.rlim_max,
            ". [suggestion] Please run 'ulimit -s' to confirm the current limit and find a way to set it to the proper."
            "value");
      } else {
        LOG_WARN("[check OS params]: ulimit.stack_size limit's soft nofile or hard nofile is smaller than 1M, please "
                 "run 'ulimit -s' to confirm the current limit and find a way to set it to the proper value",
            K(rlim.rlim_cur),
            K(rlim.rlim_max));
      }
    }
  } else {
    LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL, OB_FILE_NOT_EXIST, "read ulimit.stack_size file failed ");
  }
  return ret;
}

int CheckAllParams::read_one_line(const char *file_path, char *buffer, size_t buffer_size)
{
  int ret = OB_SUCCESS;
  FILE *file = fopen(file_path, "r");
  if (file != nullptr) {
    if (fgets(buffer, buffer_size, file) == nullptr) {
      ret = OB_IO_ERROR;
      LOG_ERROR("Failed to read line from file", K(file_path), K(ret));
    }
    fclose(file);
  } else {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("File does not exist", K(ret));
  }
  return ret;
}

int CheckAllParams::check_current_clocksource(bool strict_check)
{
  int ret = OB_SUCCESS;
  const char *file_path = "/sys/devices/system/clocksource/clocksource0/current_clocksource";
  if (is_path_valid(file_path)) {
    char clocksource[256];
    if (OB_FAIL(read_one_line(file_path, clocksource, sizeof(clocksource)))) {
      LOG_DBA_ERROR_V2(OB_SERVER_READ_FILE_FAIL,
          OB_ERR_UNEXPECTED,
          "[check OS params]:read file failed. ",
          "file_path is ",
          file_path,
          ". [suggestion] Please ensure system parameter is readable.");
    } else {
      if (clocksource[strlen(clocksource) - 1] == '\n') {
        clocksource[strlen(clocksource) - 1] = '\0';
      }
      if (strcmp(clocksource, "tsc") == 0 || strcmp(clocksource, "kvm-clock") == 0 ||
          strcmp(clocksource, "arch_sys_counter") == 0) {
        LOG_INFO("[check OS params]:current_clocksource is in proper range", K(clocksource), K(ret));
      } else if (strict_check) {
        ret = OB_IMPROPER_OS_PARAM;
        LOG_DBA_ERROR_V2(OB_SERVER_CHECK_CURRENT_CLOCKSOURCE_FAIL,
            OB_IMPROPER_OS_PARAM,
            "[check OS params]:current_clocksource is not [tsc], [kvm-clock], [arch_sys_counter]. ",
            "clocksource is ",
            clocksource,
            ". [suggestion] Please change current_clocksource to [tsc], [kvm-clock], [arch_sys_counter].");
      } else {
        LOG_WARN("[check OS params]:current_clocksource is not [tsc], [kvm-clock], [arch_sys_counter]",
            K(clocksource),
            K(ret));
      }
    }
  } else {
    LOG_WARN("file path does not exist", K(file_path));
  }
  return ret;
}

int check_os_params(bool strict_check_params = false)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(CheckAllParams::check_all_params(strict_check_params))) {
    LOG_DBA_ERROR_V2(OB_SERVER_CHECK_ALL_PARAMS_FAIL, OB_IMPROPER_OS_PARAM, "check os params failed ");
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
