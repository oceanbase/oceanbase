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

#include <iostream>
#include <sstream>
#include <iterator>
#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <malloc.h>
#include "share/ob_define.h"
#include "dumpsst/ob_admin_dumpsst_executor.h"
#include "io_bench/ob_admin_io_executor.h"
#include "server_tool/ob_admin_server_executor.h"
#include "backup_tool/ob_admin_dump_backup_data_executor.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "tools/ob_admin/dump_key/ob_admin_dump_key_executor.h"
#endif
#include "dump_enum_value/ob_admin_dump_enum_value_executor.h"
#include "log_tool/ob_admin_log_tool_executor.h"
#include "slog_tool/ob_admin_slog_executor.h"
#include "dump_ckpt/ob_admin_dump_ckpt_executor.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

void print_usage()
{
  fprintf(stderr, "\nUsage: ob_admin io_bench\n"
         "       ob_admin slog_tool\n"
         "       ob_admin dump_ckpt ## dump slog checkpoint, only support for 4.x\n"
         "       ob_admin dumpsst\n"
         "       ob_admin dump_enum_value\n"
#ifdef OB_BUILD_TDE_SECURITY
         "       ob_admin dump_key\n"
#endif
         "       ob_admin log_tool ## './ob_admin log_tool' for more detail\n"
         "       ob_admin -h127.0.0.1 -p2883 xxx\n"
         "       ob_admin -h127.0.0.1 -p2883 (-sintl/-ssm -mbkmi/-mlocal) [command]\n"
         "              ## The options in parentheses take effect when ssl enabled.\n"
         "       ob_admin -S unix_domain_socket_path xxx");
}

int get_log_base_directory(char *log_file_name, const int64_t log_file_name_len,
                           char *log_file_rs_name, const int64_t log_file_rs_name_len)
{
  int ret = OB_SUCCESS;
  const char *log_file_name_ptr = "ob_admin.log";
  const char *log_file_rs_name_ptr = "ob_admin_rs.log";
  // the format of log file name  is 'ob_admin_log_dir' + "/" + ob_admin.log + '\0'
  const int tmp_log_file_name_len = 1 + strlen(log_file_name_ptr) + 1;
  const int tmp_log_file_rs_name_len = 1 + strlen(log_file_rs_name_ptr) + 1;

  if (NULL == log_file_name || 0 >= log_file_name_len
      || NULL == log_file_rs_name || 0 >= log_file_rs_name_len) {
    ret = OB_INVALID_ARGUMENT;
    fprintf(stderr, "\ninvalid argument, errno:%d\n", errno);
  } else {
    const char *ob_admin_log_dir = getenv("OB_ADMIN_LOG_DIR");
    int64_t ob_admin_log_dir_len = 0;
    bool is_directory = false;
    if (NULL == ob_admin_log_dir) {
      fprintf(stderr, "\nThe OB_ADMIN_LOG_DIR environment variable not found, we will not generate ob_admin.log\n"
                      "If log files are required, please notice that log files should not be outputted to\n"
                      "OceanBase's clog directory.(for example, export OB_ADMIN_LOG_DIR=/tmp)\n");
      ret = OB_ENTRY_NOT_EXIST;
    } else if (FALSE_IT(ob_admin_log_dir_len = strlen(ob_admin_log_dir))) {
    } else if (OB_FAIL(FileDirectoryUtils::is_directory(ob_admin_log_dir, is_directory))) {
      fprintf(stderr, "\nCheck is_directory failed, we will not generate ob_admin.log(errno:%d)\n", ret);
    } else if (!is_directory) {
      fprintf(stderr, "\nThe OB_ADMIN_LOG_DIR(%s) environment variable is not a directory, we will not generate ob_admin.log\n"
                      "If log files are required, please notice that log files should not be outputted to\n"
                      "OceanBase's clog directory.\n", ob_admin_log_dir);
      ret = OB_ENTRY_NOT_EXIST;
    } else if (0 != access(ob_admin_log_dir, W_OK)) {
      fprintf(stderr, "\nPermission denied, currently OB_ADMIN_LOG_DIR(%s) environment variable, we will not generate ob_admin.log\n"
                      "If log files are required, please notice that log files should not be outputted to\n"
                      "OceanBase's clog directory.\n", ob_admin_log_dir);
      ret = OB_ENTRY_NOT_EXIST;
    } else if (ob_admin_log_dir_len + tmp_log_file_name_len > log_file_name_len
               || ob_admin_log_dir_len + tmp_log_file_rs_name_len > log_file_rs_name_len) {
      fprintf(stderr, "\nLog file name length too longer, please modify log's directory via export $OB_ADMIN_LOG_DIR=xxx\n"
                      "If log files are required, please notice that log files should not be outputted to\n"
                      "OceanBase's clog directory, currently OB_ADMIN_LOG_DIR(%s) environment variable.\n", ob_admin_log_dir);
      ret = OB_SIZE_OVERFLOW;
    } else if (OB_FAIL(databuff_printf(log_file_name, log_file_name_len, "%s/%s", ob_admin_log_dir, log_file_name_ptr))) {
      fprintf(stderr, "\nUnexpected error, databuff_printf failed\n");
    } else if (OB_FAIL(databuff_printf(log_file_rs_name, log_file_rs_name_len, "%s/%s", ob_admin_log_dir, log_file_rs_name_ptr))) {
      fprintf(stderr, "\nUnexpected error, databuff_printf failed\n");
    } else {
    }
  }
  return ret;
}

int main(int argc, char *argv[])
{
  int ret = 0;
  char log_file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  char log_file_rs_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (OB_FAIL(get_log_base_directory(log_file_name, sizeof(log_file_name), log_file_rs_name, sizeof(log_file_rs_name)))) {
  } else {
    OB_LOGGER.set_log_level("INFO");
    OB_LOGGER.set_file_name(log_file_name, true, false, log_file_rs_name);
  }
  const char *log_level = getenv("OB_ADMIN_LOG_LEVEL");
  if (NULL != log_level) {
    OB_LOGGER.set_log_level(log_level);
  }
  std::ostringstream ss;
  copy(argv, argv + argc, std::ostream_iterator<char*>(ss, " "));
  _OB_LOG(INFO, "cmd: [%s]", ss.str().c_str());

  ObAdminExecutor *executor = NULL;
  if (argc < 2) {
    print_usage();
  } else {
    if (0 == strcmp("io_bench", argv[1])) {
      executor = new ObAdminIOExecutor();
#ifdef OB_BUILD_TDE_SECURITY
    } else if (0 == strcmp("dump_key", argv[1])) {
    executor = new ObAdminDumpKeyExecutor();
#endif
    } else if (0 == strcmp("dump_enum_value", argv[1])) {
      executor = new ObAdminDumpEnumValueExecutor();
    } else if (0 == strcmp("dumpsst", argv[1])) {
      executor = new ObAdminDumpsstExecutor();
    } else if (0 == strcmp("log_tool", argv[1])) {
      executor = new ObAdminLogExecutor();
    } else if (0 == strcmp("dump_backup", argv[1])) {
      executor = new ObAdminDumpBackupDataExecutor();
    } else if (0 == strcmp("slog_tool", argv[1])) {
      executor = new ObAdminSlogExecutor();
    } else if (0 == strcmp("dump_ckpt", argv[1])) {
      executor = new ObAdminDumpCkptExecutor();
    } else if (0 == strncmp("-h", argv[1], 2) || 0 == strncmp("-S", argv[1], 2)) {
      executor = new ObAdminServerExecutor();
    } else {
      print_usage();
    }

    if (NULL != executor) {
      if (OB_FAIL(executor->execute(argc, argv))) {
        COMMON_LOG(WARN, "Fail to executor cmd, ", K(ret));
      }
      delete executor;
    }
  }
  return ret;
}
