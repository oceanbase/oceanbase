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

#include "ob_admin_io_executor.h"
#include "share/io/ob_io_manager.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

namespace oceanbase
{
namespace tools
{

ObAdminIOExecutor::ObAdminIOExecutor()
  : conf_dir_(NULL),
    data_dir_(NULL),
    file_size_(NULL)
{
}

ObAdminIOExecutor::~ObAdminIOExecutor()
{
}

int ObAdminIOExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(parse_cmd(argc - 1, argv + 1))) {
    COMMON_LOG(ERROR, "Fail to parse cmd, ", K(ret));
  } else if (OB_UNLIKELY(NULL == conf_dir_ || NULL == data_dir_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(ERROR, "invalid argument", K(ret), K(data_dir_), K(conf_dir_));
  } else {
    file_size_ = NULL == file_size_ ? "100G" : file_size_;
    ObArenaAllocator arena;
    const int64_t max_cmd_length = OB_MAX_DIRECTORY_PATH_LENGTH * 3L;
    char *bench_cmd = reinterpret_cast<char *>(arena.alloc(max_cmd_length));
    char *exe_path = reinterpret_cast<char *>(arena.alloc(OB_MAX_DIRECTORY_PATH_LENGTH));
    if (OB_ISNULL(bench_cmd) || OB_ISNULL(exe_path)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "allocate memory for bench command failed", K(ret));
    } else {
      int exe_path_len = readlink("/proc/self/exe", exe_path, OB_MAX_DIRECTORY_PATH_LENGTH);
      if (exe_path_len < 0 || exe_path_len >= OB_MAX_DIRECTORY_PATH_LENGTH) {
        ret = OB_ERR_INVALID_PATH;
        COMMON_LOG(ERROR, "executable path is invalid", K(ret), K(exe_path));
      } else {
        for (int64_t i = exe_path_len - 1; i >= 0 ; --i) { // ignore ret
          if (i >= 0 && '/' == exe_path[i]) {
            exe_path[i] = 0;
            break;
          }
        }
        int len = snprintf(bench_cmd, max_cmd_length, "bash %s/bench_io.sh %s %s %s", exe_path, data_dir_, file_size_, conf_dir_);
        if (len < 0 || len >= max_cmd_length) {
          ret = OB_ERR_UNEXPECTED;
          COMMON_LOG(ERROR, "generate bench command failed", K(ret), K(len), K(bench_cmd));
        } else {
          int sys_ret = system(bench_cmd);
          if (sys_ret < 0 || !WIFEXITED(sys_ret) || 0 != WEXITSTATUS(sys_ret)) {
            ret = OB_ERR_UNEXPECTED;
            COMMON_LOG(WARN, "unexpected exit status of bench command", K(sys_ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObAdminIOExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "hc:d:f:";
  struct option longopts[] =
    {{"help", 0, NULL, 'h' },
     {"conf_dir", 1, NULL, 'c'},
     {"data_dir", 1, NULL, 'd'},
     {"file_size", 1, NULL, 'f'}};

  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'h': {
        print_usage();
        break;
      }
      case 'c': {
        conf_dir_ = optarg;
        break;
      }
      case 'd': {
        data_dir_ = optarg;
        break;
      }
      case 'f': {
        file_size_ = optarg;
        break;
      }
      default: {
        print_usage();
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }
  return ret;
}

void ObAdminIOExecutor::print_usage()
{
  fprintf(stderr, "\nUsage: ob_tool io_bench -c conf_dir -d data_dir\n");
}

void ObAdminIOExecutor::reset()
{
  conf_dir_ = NULL;
  data_dir_ = NULL;
  file_size_ = NULL;
}

}
}
