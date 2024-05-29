/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "gtest/gtest.h"

#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string_buffer.h"
#include "ob_log_miner_args.h"
#include "lib/oblog/ob_log.h"


using namespace oceanbase;

namespace oceanbase
{
namespace oblogminer
{

struct CmdArgs {
  explicit CmdArgs(ObIAllocator &alloc):
      allocator(alloc),
      argc(0) {}

  ~CmdArgs() {
    reset();
  }

  void build_cmd_args(const char* prog_name, ...) {
    reset();

    va_list ap;
    va_start(ap, prog_name);

    argv[argc] = (char*)allocator.alloc(strlen(prog_name)+1);
    strncpy(argv[argc], prog_name, strlen(prog_name));
    argv[argc][strlen(prog_name)] = '\0';
    argc++;

    const char* next = nullptr;
    while ((next = va_arg(ap, const char*)) != nullptr && argc < 100) {
        argv[argc] = (char*)allocator.alloc(strlen(next)+1);
        strncpy(argv[argc], next, strlen(next));
        argv[argc][strlen(next)] = '\0';
        argc++;
    }

    va_end(ap);
  }

  void reset() {
    for (int i = 0; i < argc; i++) {
      allocator.free(argv[i]);
      argv[i] = nullptr;
    }
    argc = 0;
    optind = 1;
  }

  ObIAllocator &allocator;
  int   argc;
  char  *argv[100];
};



TEST(ObLogMinerArgs, test_log_miner_args)
{
  ObMalloc allocator("TestMnrCmdArgs");
  CmdArgs *args = new CmdArgs(allocator);
  ObLogMinerCmdArgs miner_cmd_args;
  ObLogMinerArgs miner_args;
  EXPECT_NE(nullptr, args);

  // test commands for analysis mode
  // set analyze archvie
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // set analyze online
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // unknown arguments, unexpected
  args->build_cmd_args("oblogminer",
      "-x", "unknown_arg",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // unknown mode, OB_INVALID_ARGUMENT
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "unknown_mode",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // both archive dest and online are set
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack online params
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // archive_dest and part online arguments are given
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-c", "cluster_url",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack output
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack table-list, default not filter any table
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack start time
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // recovery path is set in analysis mode
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-r", "recovery_path",
      nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // source_db1.table is set in analysis mode
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-S", "db1.table1",
      nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // target table is set in analysis mode
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-t", "db1.tmp_table1",
      nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();

  // test commands for analysis mode
  // args are correctly set
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack cluster_url
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack recovery_path
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack source table
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-r", "recovery_path",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // lack output_dest
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  // archvie_dest is set in flashback mode
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-a", "archvie_dest",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, miner_cmd_args.init(args->argc, args->argv));

  // table_list is set in flashback mode
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-l", "table-list",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();

  // verbose
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-v",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();

  // timezone
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-z", "+8:00",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();

  // timezone
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-z", "-8:00",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();

  // unspecified end time
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-o", "output_dest",
      "-z", "+0:00",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_cmd_args.init(args->argc, args->argv));
  miner_cmd_args.reset();
  delete args;
}

TEST(ObLogMinerArgs, test_log_miner_analyzer_flashbacker_args) {
  ObMalloc allocator("TestMnrCmdArgs");
  CmdArgs *args = new CmdArgs(allocator);
  ObLogMinerCmdArgs miner_args;
  AnalyzerArgs analyzer_args;
  FlashbackerArgs flashbacker_args;
  EXPECT_NE(nullptr, args);

  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  EXPECT_EQ(OB_ERR_UNEXPECTED, flashbacker_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();
  flashbacker_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "aaaaa",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_DATE_FORMAT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  // incorrect time range
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "22222",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-c", "cluster_addr",
      "-u", "user_name@tenant",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "22221",
      "-e", "22222",
      "-o", "output_progress",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, flashbacker_args.init(miner_args));
  EXPECT_EQ(OB_ERR_UNEXPECTED, analyzer_args.init(miner_args));
  miner_args.reset();
  flashbacker_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-20 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-20 16:35:20",
      "-o", "output_dest",
      "-z", "-8:00",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-20 16:35:20",
      "-o", "output_dest",
      "-z", "xxx",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_ERR_UNKNOWN_TIME_ZONE, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-20 16:35:20",
      "-e", "2023-12-20 16:35:20",
      "-o", "output_dest",
      "-z", "+8:00",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-20 16:35:20",
      "-e", "2023-12-20 16:35:20.000001",
      "-o", "output_dest",
      "-z", "+8:00",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-20 16:35:20",
      "-e", "2023-12-20 16:35:20.000001",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-32 16:35:20",
      "-e", "2023-12-32 16:35:20.000001",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_DATE_VALUE, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  // check user-name
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  // check password
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  // table list check
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "tenant.db1.table1",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.*.*",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "fake.db1.*",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*..",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.*|tenant.db2.*",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "*.db1.*|fake.db2.*",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-O", "insert|delete",
      "-m", "analysis",
      "-l", "tenant.db1.*",
      "-s", "2023-12-31 16:35:20",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  miner_args.reset();
  analyzer_args.reset();

  // incorrect time range
  args->build_cmd_args("oblogminer",
      "-m", "flashback",
      "-c", "cluster_addr",
      "-u", "user_name",
      "-p", "password",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.tmp_table1",
      "-s", "22222",
      "-e", "22222",
      "-o", "output_progress",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_INVALID_ARGUMENT, flashbacker_args.init(miner_args));
  miner_args.reset();
  flashbacker_args.reset();
  if (nullptr != args) {
    delete args;
  }
}

TEST(ObLogMinerArgs, test_ob_log_miner_args)
{
  ObMalloc allocator("TestMnrCmdArgs");
  CmdArgs *args = new CmdArgs(allocator);
  ObLogMinerArgs miner_args;
  EXPECT_NE(nullptr, args);

 // test commands for analysis mode
  // set analyze archvie
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  miner_args.reset();
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_mod_log_levels("ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:WARN");
  // set analyze online
  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  miner_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "flashback",
      "-r", "recovery_path",
      "-S", "db1.table1",
      "-t", "db1.table1_tmp",
      "-s", "11111",
      "-e", "22222",
      "-o", "progress_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  miner_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "flashback",
      "-S", "db1.table1",
      "-t", "db1.table1_tmp",
      "-s", "23333",
      "-e", "22222",
      "-o", "progress_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_args.init(args->argc, args->argv));
  miner_args.reset();
  // unknown arguments, unexpected
  args->build_cmd_args("oblogminer",
      "-x", "unknown_arg",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_args.init(args->argc, args->argv));
  miner_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-r", "recovery_path",
      nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED, miner_args.init(args->argc, args->argv));
  miner_args.reset();

  args->build_cmd_args("oblogminer",
      "-c", "cluster_url",
      "-u", "username@tenant#cluster",
      "-p", "password",
      "-m", "flashback",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      "-r", "recovery_path",
      nullptr);
  EXPECT_EQ(OB_INVALID_ARGUMENT, miner_args.init(args->argc, args->argv));
  miner_args.reset();
  if (nullptr != args) {
    delete args;
  }
}

TEST(ObLogMinerArgs, test_log_miner_analyzer_args_serialize)
{
  ObMalloc allocator("TestMnrCmdArgs");
  CmdArgs *args = new CmdArgs(allocator);
  ObLogMinerCmdArgs miner_args;
  AnalyzerArgs analyzer_args;
  EXPECT_NE(nullptr, args);

  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.init(miner_args));
  int64_t pos = 0;
  const char *expect_buf = "cluster_addr=\nuser_name=\npassword=\narchive_dest=archive_dest\ntable_list=*.db1.table1\n"
      "column_cond=\noperations=insert|delete|update\noutput_dest=output_dest\nlog_level=\n"
      "start_time_us=11111\nend_time_us=22222\ntimezone=+8:00\nrecord_format=CSV\n";
  char tmp_buf[1000] = {0};
  EXPECT_EQ(OB_SUCCESS, analyzer_args.serialize(tmp_buf, 1000, pos));
  EXPECT_STREQ(tmp_buf, expect_buf);
  analyzer_args.reset();
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, analyzer_args.deserialize(expect_buf, strlen(expect_buf), pos));
  EXPECT_STREQ(analyzer_args.archive_dest_, "archive_dest");
  EXPECT_STREQ(analyzer_args.table_list_, "*.db1.table1");
  EXPECT_EQ(analyzer_args.start_time_us_, 11111);
  EXPECT_EQ(analyzer_args.end_time_us_, 22222);
  EXPECT_STREQ(analyzer_args.output_dst_, "output_dest");
  pos = 0;
  memset(tmp_buf, 0, sizeof(tmp_buf));
  EXPECT_EQ(OB_SUCCESS, analyzer_args.serialize(tmp_buf, 1000, pos));
  tmp_buf[pos+1] = '\0';
  EXPECT_STREQ(expect_buf, tmp_buf);
  pos = 0;
  const char *buf2 = "cluster_addr=\nuser_name=\n";
  analyzer_args.reset();
  EXPECT_EQ(OB_SIZE_OVERFLOW, analyzer_args.deserialize(buf2, strlen(buf2), pos));
  pos = 0;
  const char *buf3 = "cluster_addr=\nuser_name=\npassword=\narchive_dest=archive_dest\ntable_list=db1.table1\n"
      "operations=insert|delete|update\noutput_dest=output_dest\nlog_level=ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:WARN\n"
      "start_time_us=11a11\nend_time_us=22222\ntimezone=+8:00\nrecord_format=CSV\n";
  EXPECT_EQ(OB_INVALID_DATA, analyzer_args.deserialize(buf3, strlen(buf3), pos));
  analyzer_args.reset();
  miner_args.reset();
  if (nullptr != args) {
    delete args;
  }
}

TEST(ObLogMinerArgs, test_log_miner_args_serialize)
{
  ObMalloc allocator("TestMnrArgs");
  CmdArgs *args = new CmdArgs(allocator);
  ObLogMinerArgs miner_args;
  EXPECT_NE(nullptr, args);

 // test commands for analysis mode
  // set analyze archvie
  args->build_cmd_args("oblogminer",
      "-a", "archive_dest",
      "-m", "analysis",
      "-l", "*.db1.table1",
      "-s", "11111",
      "-e", "22222",
      "-o", "output_dest",
      nullptr);
  EXPECT_EQ(OB_SUCCESS, miner_args.init(args->argc, args->argv));
  int64_t pos = 0;
  const char *expect_buf = "mode=ANALYSIS\ncluster_addr=\nuser_name=\npassword=\n"
      "archive_dest=archive_dest\ntable_list=*.db1.table1\ncolumn_cond=\noperations=insert|delete|update\n"
      "output_dest=output_dest\nlog_level=ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:WARN\nstart_time_us=11111\n"
      "end_time_us=22222\ntimezone=+8:00\nrecord_format=CSV\n";
  char tmp_buf[1000] = {0};
  EXPECT_EQ(OB_SUCCESS, miner_args.serialize(tmp_buf, 1000, pos));
  EXPECT_STREQ(tmp_buf, expect_buf);
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, miner_args.deserialize(expect_buf, strlen(expect_buf), pos));
  EXPECT_EQ(miner_args.mode_, LogMinerMode::ANALYSIS);
  EXPECT_STREQ(miner_args.analyzer_args_.archive_dest_, "archive_dest");
  EXPECT_STREQ(miner_args.analyzer_args_.table_list_, "*.db1.table1");
  EXPECT_EQ(miner_args.analyzer_args_.start_time_us_, 11111);
  EXPECT_EQ(miner_args.analyzer_args_.end_time_us_, 22222);
  EXPECT_STREQ(miner_args.analyzer_args_.output_dst_, "output_dest");
  pos = 0;
  memset(tmp_buf, 0, sizeof(tmp_buf));
  EXPECT_EQ(OB_SUCCESS, miner_args.serialize(tmp_buf, 1000, pos));
  tmp_buf[pos+1] = '\0';
  EXPECT_STREQ(expect_buf, tmp_buf);

  if (nullptr != args) {
    delete args;
  }
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_log_miner_args.log");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_log_miner_args.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
