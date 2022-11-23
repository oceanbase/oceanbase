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
#include "share/ob_define.h"
#include "dumpsst/ob_admin_dumpsst_executor.h"
#include "io_bench/ob_admin_io_executor.h"
#include "server_tool/ob_admin_server_executor.h"
#include "backup_tool/ob_admin_dump_backup_data_executor.h"
#include "dump_enum_value/ob_admin_dump_enum_value_executor.h"
#include "log_tool/ob_admin_log_tool_executor.h"
#include "slog_tool/ob_admin_slog_executor.h"
#include "dump_ckpt/ob_admin_dump_ckpt_executor.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

void print_usage()
{
  fprintf(stderr, "\nUsage: ob_admin io_bench\n"
         "       ob_admin slog_tool\n"
         "       ob_admin dump_ckpt ## dump slog checkpoint, only support for 4.x\n"
         "       ob_admin dumpsst\n"
         "       ob_admin dump_enum_value\n"
         "       ob_admin log_tool ## './ob_admin log_tool' for more detail\n"
         "       ob_admin -h127.0.0.1 -p2883 xxx\n"
         "       ob_admin -h127.0.0.1 -p2883 (-sintl/-ssm -mbkmi/-mlocal) [command]\n"
         "              ## The options in parentheses take effect when ssl enabled.\n"
         "       ob_admin -S unix_domain_socket_path xxx");
}

int main(int argc, char *argv[])
{
  int ret = 0;
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("ob_admin.log", true, false);
  OB_LOGGER.set_file_name("ob_admin.log", true, false, "ob_admin_rs.log");
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
