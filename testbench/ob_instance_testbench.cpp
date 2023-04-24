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

#include "ob_workload_scheduler.h"
#include "lib/file/file_directory_utils.h"
#include "getopt.h"

using namespace oceanbase;
using namespace oceanbase::testbench;

static void print_help()
{
  MPRINT("testbench [OPTIONS]");
  MPRINT("-h, --help, print this help");
  MPRINT("-H,--home_path, home path of the benchmark process");
  MPRINT("-d,--distributed_transaction, option string of distributed transaction workload");
  MPRINT("-c,--contention, option string of contention workload");
  MPRINT("-D,--deadlock, option string of deadlock workload");
}

static void get_opts_setting(struct option long_opts[], char short_opts[], const size_t size)
{
  static struct
  {
    const char *long_name_;
    char short_name_;
    bool has_arg_;
  } tb_opts[] = {
      {"help", 'h', 0},
      {"home_path", 'H', 1},
      {"distributed_transaction", 'd', 1},
      {"contention", 'c', 1},
      {"deadlock", 'D', 1},
  };
  size_t opts_cnt = sizeof(tb_opts) / sizeof(tb_opts[0]);

  if (opts_cnt > size)
  {
    MPRINTx("parse option fail: opts array is too small");
  }

  int short_idx = 0;
  for (size_t i = 0; i < opts_cnt; ++i)
  {
    long_opts[i].name = tb_opts[i].long_name_;
    long_opts[i].has_arg = tb_opts[i].has_arg_;
    long_opts[i].flag = NULL;
    long_opts[i].val = tb_opts[i].short_name_;

    short_opts[short_idx++] = tb_opts[i].short_name_;
    if (tb_opts[i].has_arg_)
    {
      short_opts[short_idx++] = ':';
    }
  }
}

// process long only option
static void
parse_long_opt(const char *name, const char *value, ObSchedulerOptions &opts)
{
  MPRINT("long: %s %s", name, value);
  UNUSED(name);
  UNUSED(value);
  UNUSED(opts);
}

static void
parse_short_opt(const int c, const char *value, ObSchedulerOptions &opts)
{
  switch (c)
  {
  case 'H':
    MPRINT("home path: %s", value);
    opts.home_path_ = value;
    break;
  case 'l':
    MPRINT("log level: %s", value);
    if (OB_SUCCESS != OB_LOGGER.level_str2int(value, opts.log_level_))
    {
      MPRINT("malformed log level, candicates are: "
             "    ERROR,USER_ERR,WARN,INFO,TRACE,DEBUG");
      MPRINT("!! Back to INFO log level.");
      opts.log_level_ = OB_LOG_LEVEL_WARN;
    }
    break;
  case 'd':
    MPRINT("distributed transaction options: %s", value);
    ADD_WORKLOAD_OPTS(opts.workloads_, DISTRIBUTED_TRANSACTION, value);
    opts.opts_cnt_++;
    break;
  case 'h':
    print_help();
    break;
  default:
    print_help();
    MPRINTx("parse workload with unknown type: %s", value);
    break;
  }
}

static void parse_opts(int argc, char *argv[], ObSchedulerOptions &opts)
{
  static const int MAX_OPTS_CNT = 128;
  static char short_opts[MAX_OPTS_CNT * 2 + 1];
  static struct option long_opts[MAX_OPTS_CNT];
  get_opts_setting(long_opts, short_opts, MAX_OPTS_CNT);
  int long_opts_idx = 0;

  bool end = false;
  while (!end)
  {
    int c = getopt_long(argc, argv, short_opts, long_opts, &long_opts_idx);
    if (-1 == c || long_opts_idx >= MAX_OPTS_CNT)
    { // end
      end = true;
    }
    else if (0 == c)
    {
      parse_long_opt(long_opts[long_opts_idx].name, optarg, opts);
    }
    else
    {
      parse_short_opt(c, optarg, opts);
    }
  }
}
int main(int argc, char *argv[])
{
  ObSchedulerOptions opts;
  opts.log_level_ = OB_LOG_LEVEL_WARN;
  parse_opts(argc, argv, opts);
  const char *LOG_DIR = "log";
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::create_full_path(LOG_DIR)))
  {
    MPRINT("create log dir fail: ./log/");
  }
  static const int LOG_FILE_SIZE = 256 * 1024 * 1024;
  const char *const LOG_FILE_NAME = "log/scheduler.log";
  OB_LOGGER.set_log_level(opts.log_level_);
  OB_LOGGER.set_max_file_size(LOG_FILE_SIZE);
  OB_LOGGER.set_file_name(LOG_FILE_NAME, true, true);
  ObWorkloadScheduler scheduler;
  fprintf(stdout, "scheduler init"
                  "\n");
  scheduler.init(opts);
  return 0;
}