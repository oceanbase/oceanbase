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

#include "ob_testbench_builder.h"
#include "lib/file/file_directory_utils.h"
#include "getopt.h"

using namespace oceanbase;
using namespace oceanbase::testbench;

static void print_help()
{
  MPRINT("testbench [OPTIONS]");
  MPRINT("-h, --help, print this help");
  MPRINT("-H,--home_path, home path of the benchmark process");
  MPRINT("-i,--cluster_host, ip address of the cluster");
  MPRINT("-P,--cluster_port, mysql port of the cluster");
  MPRINT("-U,--cluster_user, user name of the cluster");
  MPRINT("-p,--cluster_password, password of the cluster");
  MPRINT("-n,--cluster_db_name, database name of the cluster");
  MPRINT("-D,--duration, time duration of the benchmark process");
  MPRINT("-t,--distributed_transaction, option string of distributed transaction workload");
  MPRINT("-c,--contention, option string of contention workload");
  MPRINT("-d,--deadlock, option string of deadlock workload");
}

static bool to_int64(const char *sval, int64_t &ival)
{
  char *endp = NULL;
  ival = static_cast<int64_t>(strtol(sval, &endp, 10));
  return NULL != endp && *endp == '\0';
}

static int to_port_x(const char *sval)
{
  int64_t port = 0;
  if (!to_int64(sval, port))
  {
    MPRINTx("need port number: %s", sval);
  }
  else if (port <= 1024 || port >= 65536)
  {
    MPRINTx(
        "port number should greater than 1024 and less than 65536: %ld", port);
  }
  return static_cast<int>(port);
}

static int to_duration(const char *sval)
{
  int64_t duration = 0;
  if (!to_int64(sval, duration))
  {
    MPRINTx("need benchmark time duration: %s", sval);
  }
  else if (duration <= 0)
  {
    MPRINTx("benchmark time duration should greater than 0: %ld", duration);
  }
  return static_cast<int>(duration);
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
      {"cluster_host", 'i', 1},
      {"cluster_port", 'P', 1},
      {"cluster_user", 'U', 1},
      {"cluster_password", 'p', 1},
      {"cluster_db_name", 'n', 1},
      {"distributed_transaction", 't', 1},
      {"contention", 'c', 1},
      {"deadlock", 'd', 1},
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
parse_long_opt(const char *name, const char *value, ObTestbenchOptions &opts)
{
  MPRINT("long: %s %s", name, value);
  UNUSED(name);
  UNUSED(value);
  UNUSED(opts);
}

static void
parse_short_opt(const int c, const char *value, ObTestbenchOptions &opts)
{
  int w_len = 0;
  switch (c)
  {
  case 'H':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid home path param");
    }
    else if (FALSE_IT(w_len = snprintf(opts.home_path_, OB_MAX_CONTEXT_STRING_LENGTH, "%s", value)))
    {
      // impossible
    }
    else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_CONTEXT_STRING_LENGTH))
    {
      MPRINTx("set home_path error: %s", value);
    }
    MPRINT("set home path: %s", value);
    break;
  case 'U':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid cluster user param");
    }
    else if (FALSE_IT(w_len = snprintf(opts.cluster_user_, OB_MAX_USER_NAME_BUF_LENGTH, "%s", value)))
    {
      // impossible
    }
    else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_USER_NAME_BUF_LENGTH))
    {
      MPRINTx("set cluster user error: %s", value);
    }
    MPRINT("set cluster user: %s", value);
    break;
  case 'i':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid cluster host param");
    }
    else if (FALSE_IT(w_len = snprintf(opts.cluster_host_, OB_MAX_HOST_NAME_LENGTH, "%s", value)))
    {
      // impossible
    }
    else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_PASSWORD_BUF_LENGTH))
    {
      MPRINTx("set cluster host error %s", value);
    }
    MPRINT("set cluster host: %s", value);
    break;
  case 'P':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid cluster port param");
    }
    else if (FALSE_IT((opts.cluster_port_ = to_port_x(value))))
    {
      MPRINTx("set cluster port error: %s", value);
    }
    MPRINT("set cluster port: %s", value);
    break;
  case 'p':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid cluster password param");
    }
    else if (FALSE_IT(w_len = snprintf(opts.cluster_pass_, OB_MAX_PASSWORD_BUF_LENGTH, "%s", value)))
    {
      // impossible
    }
    else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_PASSWORD_BUF_LENGTH))
    {
      MPRINTx("set cluster password error: %s", value);
    }
    MPRINT("set cluster password: %s", value);
    break;
  case 'n':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid cluster db name param");
    }
    else if (FALSE_IT(w_len = snprintf(opts.cluster_db_name_, OB_MAX_DATABASE_NAME_BUF_LENGTH, "%s", value)))
    {
      // impossible
    }
    else if (OB_UNLIKELY(w_len <= 0) || OB_UNLIKELY(w_len > OB_MAX_DATABASE_NAME_BUF_LENGTH))
    {
      MPRINTx("set cluster db name error: %s", value);
    }
    MPRINT("set cluster db name: %s", value);
    break;
  case 'D':
    if (OB_ISNULL(value))
    {
      MPRINTx("invalid time duration param");
    }
    else if (FALSE_IT(opts.duration_ = to_duration(value)))
    {
      MPRINTx("set benchmark time duration error: %s", value);
    }
    MPRINT("set benchmark time duration: %s", value);
    break;
  case 'l':
    if (OB_SUCCESS != OB_LOGGER.level_str2int(value, opts.log_level_))
    {
      MPRINT("malformed log level, candicates are: "
             "    ERROR,USER_ERR,WARN,INFO,TRACE,DEBUG");
      MPRINT("!! Back to INFO log level.");
      opts.log_level_ = OB_LOG_LEVEL_WARN;
    }
    MPRINT("log level: %s", value);
    break;
  case 't':
    MPRINT("distributed transaction options: %s", value);
    ADD_WORKLOAD_OPTS(opts.workloads_, DISTRIBUTED_TRANSACTION, value);
    opts.opts_cnt_++;
    break;
  case 'c':
    MPRINT("contention options: %s", value);
    break;
  case 'd':
    MPRINT("deadlock options: %s", value);
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

static void parse_opts(int argc, char *argv[], ObTestbenchOptions &opts)
{
  static const int MAX_OPTS_CNT = 16;
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
  ObTestbenchOptions opts;
  parse_opts(argc, argv, opts);
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::create_full_path(opts.log_dir_)))
  {
    MPRINT("create log dir fail: %s", opts.log_dir_);
  }
  int64_t version = ::oceanbase::common::ObTimeUtility::current_time();
  OB_LOGGER.set_log_level(opts.log_level_, version);
  OB_LOGGER.set_max_file_size(opts.log_file_size_);
  OB_LOGGER.set_file_name(opts.log_file_, true, false);
  OB_LOGGER.set_enable_log_limit(false);
  ObTestbenchBuilder &builder = ObTestbenchBuilder::get_instance();
  builder.init(opts);
  builder.start_service();
  ob_usleep(opts.duration_ * 1000 * 1000);
  builder.stop_service();
  OB_LOGGER.set_stop_append_log();
  return ret;
}