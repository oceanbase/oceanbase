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

#define USING_LOG_PREFIX LOGMNR

#include "lib/container/ob_array_wrap.h"
#include "ob_log_miner_utils.h"                       // logminer_str2ll
#include "ob_log_miner_args.h"                        // ObLogMinerCmdArgs/ObLogMinerArgs/AnalyzerArgs/FlashbackerArgs
#include "ob_log_miner_logger.h"                      // LOGMINER_LOGGER
#include "lib/ob_define.h"                            // OB_SUCCESS
#include "lib/oblog/ob_log.h"                         // LOG_XX
#include "lib/timezone/ob_time_convert.h"             // ObTimeConverter
#include "lib/timezone/ob_timezone_info.h"            // ObTimeZoneInfo
#include <getopt.h>                                   // getopt_long

namespace oceanbase
{
namespace oblogminer
{

/////////////////////////////////////// ObLogMinerCmdArgs ///////////////////////////////////////

void ObLogMinerCmdArgs::print_usage(const char *prog_name)
{
  fprintf(stderr,
  "OceanBase LogMiner(ObLogMiner) is a tool that converts CLOG in OceanBase to\n"
  "a human-readable format.\n"
  "The following are examples demonstrating how to use:\n"
  "%s -c \"ip:port\" -u \"user@tenant\" -p \"***\" -s \"2023-01-01 10:00:00\" -o \"file:///data/output/\"\n"
  "%s -a \"file:///data/archive/\" -s \"2023-01-01 10:00:00\" -o \"file:///data/output/\"\n"
  "Options are list below:\n"
  "   -m, --mode             Specifies the working mode of ObLogMiner, which currently\n"
  "                          only supports \"analysis\" mode. The default is \"analysis\".\n"
  "   -c, --cluster-addr     A list of IPs and ports separated by '|' (e.g., ip1:port1|ip2:port2),\n"
  "                          where the IP is the observer's IP and the port is the observer's SQL port.\n"
  "   -u, --user-name        The tenant's username, including the tenant name(e.g., user@tenant).\n"
  "   -p, --password         The password corresponding to the user-name.\n"
  "   -a, --archive-dest     The location for the tenant's archive log(e.g., file:///data/xxx/xx).\n"
  "   -l, --table-list       Specifies the table list for analysis, corresponding to\n"
  "                          the `tb_white_list` parameter in OBCDC.\n"
  "   -C, --column-cond      Specifies the column filter for analysis.\n"
  "   -s, --start-time       Specifies the start time in microsecond timestamp or datetime format.\n"
  "   -e, --end-time         Specifies the end time in microsecond timestamp or datetime format.\n"
  "   -O, --operations       Specifies the type of operations, options including insert,\n"
  "                          update, and delete. Operations can be combined using '|'.\n"
  "                          The default is \"insert|update|delete\".\n"
  "   -o, --output           The location for the analysis results.\n"
  "   -h, --help             Displays the help message.\n"
  "   -v, --verbose          Determines whether to output detailed logs to the command line.\n"
  "   -z, --timezone         Specifies the timezone, default is \"+8:00\".\n"
  "   -f, --record_format    Specifies the record format, default is \"CSV\".\n"
  "                          Options including CSV, REDO_ONLY, UNDO_ONLY, JSON.\n"
  "   -L  --log_level        Specifies the log level, default is \"ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:WARN\".\n"
  "\n",
  prog_name, prog_name
  );
}

void ObLogMinerCmdArgs::reset()
{
  mode_ = LogMinerMode::UNKNOWN;
  cluster_addr_ = nullptr;
  user_name_ = nullptr;
  password_ = nullptr;
  archive_dest_ = nullptr;
  table_list_ = nullptr;
  column_cond_ = nullptr;
  operations_ = nullptr;
  output_ = nullptr;
  recovery_path_ = nullptr;
  target_table_ = nullptr;
  source_table_ = nullptr;
  start_time_ = nullptr;
  end_time_ = nullptr;
  log_level_ = nullptr;
  timezone_ = nullptr;
  record_format_ = nullptr;
  verbose_ = false;
  print_usage_ = false;
}

int ObLogMinerCmdArgs::init(int argc, char **argv)
{
  int ret = OB_SUCCESS;
  int opt = -1;
  const char *short_opts = "m:c:u:p:a:l:C:s:e:O:o:r:t:S:hL:z:f:v";
  const struct option long_opts[] = {
    {"mode", 1, NULL, 'm'},
    {"cluster-addr", 1, NULL, 'c'},
    {"user-name", 1, NULL, 'u'},
    {"password", 1, NULL, 'p'},
    {"archive-dest", 1, NULL, 'a'},
    {"table-list", 1, NULL, 'l'},
    {"column-cond", 1, NULL, 'C'},
    {"start-time", 1, NULL, 's'},
    {"end-time", 1, NULL, 'e'},
    {"operations", 1, NULL, 'O'},
    {"output", 1, NULL, 'o'},
    {"recovery-path", 1, NULL, 'r'},
    {"target-table", 1, NULL, 't'},
    {"source-table", 1, NULL, 'S'},
    {"help", 0, NULL, 'h'},
    {"log_level", 1, NULL, 'L'},
    {"timezone", 1, NULL, 'z'},
    {"record_format", 1, NULL, 'f'},
    {"verbose", 0, NULL, 'v'},
    {0, 0, 0, 0},
  };

  if (argc <= 1) {
    // print usage, just as -h
    print_usage(argv[0]);
    ret = OB_IN_STOP_STATE;
  }
  // set logminer default mode.
  if (OB_SUCC(ret)) {
    mode_ = LogMinerMode::ANALYSIS;
  }

  while (OB_SUCC(ret) && (-1 != (opt = getopt_long(argc, argv, short_opts, long_opts, NULL)))) {
    _LOG_TRACE("get opt %d, val is \"%s\"", opt, optarg);
    switch (opt) {
      case 'm': {
        mode_ = get_logminer_mode(optarg);
        break;
      }

      case 'c': {
        cluster_addr_ = optarg;
        break;
      }

      case 'u': {
        user_name_ = optarg;
        break;
      }

      case 'p': {
        password_ = optarg;
        break;
      }

      case 'a': {
        archive_dest_ = optarg;
        break;
      }

      case 'l': {
        table_list_ = optarg;
        break;
      }

      case 'C': {
        column_cond_ = optarg;
        break;
      }

      case 's': {
        start_time_ = optarg;
        break;
      }

      case 'e': {
        end_time_ = optarg;
        break;
      }

      case 'o': {
        output_ = optarg;
        break;
      }

      case 'O': {
        operations_ = optarg;
        break;
      }

      case 'r': {
        recovery_path_ = optarg;
        break;
      }

      case 't': {
        target_table_ = optarg;
        break;
      }

      case 'S': {
        source_table_ = optarg;
        break;
      }

      case 'h': {
        print_usage_ = true;
        break;
      }

      case 'L': {
        log_level_ = optarg;
        break;
      }

      case 'z': {
        timezone_ = optarg;
        break;
      }

      case 'f': {
        record_format_ = optarg;
        break;
      }

      case 'v': {
        verbose_ = true;
        break;
      }

      default: {
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }
  }
  LOG_INFO("parse cmd args finished", KPC(this));
  if (OB_SUCC(ret) && OB_FAIL(validate_())) {
    LOG_ERROR("logminer command arguments are invalid", KPC(this));
    LOGMINER_STDOUT("logminer command arguments are invalid\n");
  }

  return ret;
}

int ObLogMinerCmdArgs::validate_() const
{
  int ret = OB_SUCCESS;
  if (print_usage_) {
    // do nothing
  } else if (! is_logminer_mode_valid(mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("logminer mode is invalid", K(mode_));
    LOGMINER_STDOUT("logminer mode is invalid\n");
  } else if (is_analysis_mode(mode_)) {
    const bool server_args_set = (nullptr != cluster_addr_) || (nullptr != user_name_) || (nullptr != password_);
    const bool server_args_complete = (nullptr != cluster_addr_) && (nullptr != user_name_) && (nullptr != password_);
    const bool archive_args_set = (nullptr != archive_dest_);
    const bool is_other_required_args_set = (nullptr != start_time_) && (nullptr != output_);
    const bool is_flashback_spec_args_set = (nullptr != recovery_path_) || (nullptr != target_table_) ||
                                            (nullptr != source_table_);
    if (archive_args_set && server_args_set) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("server_args and archive_args are both set", K(server_args_set), K(archive_args_set));
      LOGMINER_STDOUT("(cluster_addr, user_name, password) and (archive_dest) are both specified\n");
    } else if (!server_args_complete && !archive_args_set) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("server_args and archive_args are both empty or incomplete", K(server_args_set), K(archive_args_set));
      LOGMINER_STDOUT("(cluster_addr, user_name, password) and (archive_dest) are both empty or incomplete\n");
    } else if (! is_other_required_args_set) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("start_time or output must be set in analysis mode");
      LOGMINER_STDOUT("start_time or output are empty\n");
    } else if (is_flashback_spec_args_set) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("some flashback specified arguments are set in analysis mode",
          KCSTRING(target_table_), KCSTRING(source_table_));
      LOGMINER_STDOUT("some flashback specified arguments are set in analysis mode\n");
    }
  } else if (is_flashback_mode(mode_)) {
    const bool server_args_set = (nullptr != cluster_addr_) && (nullptr != user_name_) && (nullptr != password_);
    const bool is_other_required_args_set = (nullptr != recovery_path_) &&
                                            (nullptr != target_table_) && (nullptr != source_table_) &&
                                            (nullptr != start_time_) && (nullptr != end_time_) &&
                                            (nullptr != output_);
    const bool is_analysis_spec_args_set = (nullptr != table_list_) || (nullptr != operations_) ||
                                           (nullptr != archive_dest_);

    if (!server_args_set || !is_other_required_args_set) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("some of the required arguments are not set in flashback mode", KCSTRING(cluster_addr_), KCSTRING(user_name_),
          KCSTRING(target_table_), KCSTRING(source_table_), KCSTRING(start_time_), KCSTRING(end_time_));
    } else if (is_analysis_spec_args_set) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("some analysis specified arguments are set in flashback mode", KCSTRING(table_list_), KCSTRING(operations_));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("logminer mode is invalid", K(mode_));
    LOGMINER_STDOUT("logminer mode is invalid\n");
  }

  return ret;
}

/////////////////////////////////////// AnalyzerArgs ///////////////////////////////////////

const char *AnalyzerArgs::CLUSTER_ADDR_KEY = "cluster_addr";
const char *AnalyzerArgs::USER_NAME_KEY = "user_name";
const char *AnalyzerArgs::PASSWORD_KEY = "password";
const char *AnalyzerArgs::ARCHIVE_DEST_KEY = "archive_dest";
const char *AnalyzerArgs::TABLE_LIST_KEY = "table_list";
const char *AnalyzerArgs::COLUMN_COND_KEY = "column_cond";
const char *AnalyzerArgs::OPERATIONS_KEY = "operations";
const char *AnalyzerArgs::OUTPUT_DST_KEY = "output_dest";
const char *AnalyzerArgs::LOG_LEVEL_KEY = "log_level";
const char *AnalyzerArgs::START_TIME_US_KEY = "start_time_us";
const char *AnalyzerArgs::END_TIME_US_KEY = "end_time_us";
const char *AnalyzerArgs::TIMEZONE_KEY = "timezone";
const char *AnalyzerArgs::RECORD_FORMAT_KEY = "record_format";
void AnalyzerArgs::reset()
{
  cluster_addr_ = nullptr;
  user_name_ = nullptr;
  password_ = nullptr;
  archive_dest_ = nullptr;
  table_list_ = nullptr;
  column_cond_ = nullptr;
  operations_ = nullptr;
  output_dst_ = nullptr;
  log_level_ = nullptr;
  start_time_us_ = OB_INVALID_TIMESTAMP;
  end_time_us_ = OB_INVALID_TIMESTAMP;
  timezone_ = nullptr;
  record_format_ = RecordFileFormat::INVALID;
  alloc_.reset();
}

int AnalyzerArgs::init(const ObLogMinerCmdArgs &args)
{
  int ret = OB_SUCCESS;
  if (! is_analysis_mode(args.mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("incorrect cmd args to init analyzer args", K(args));
  } else {
    const char *operations = nullptr == args.operations_ ? ObLogMinerArgs::DEFAULT_LOGMNR_OPERATIONS : args.operations_;
    const char *timezone = nullptr == args.timezone_ ? ObLogMinerArgs::DEFAULT_LOGMNR_TIMEZONE : args.timezone_;
    const char *table_list = nullptr == args.table_list_ ? ObLogMinerArgs::DEFAULT_LOGMNR_TABLE_LIST : args.table_list_;
    const char *record_format_str = nullptr == args.record_format_ ? ObLogMinerArgs::DEFAULT_LOGMNR_FORMAT : args.record_format_;
    ObTimeZoneInfo tz_info;
    ObTimeConvertCtx cvrt_ctx(&tz_info, true);
    if (OB_FAIL(deep_copy_cstring(alloc_, args.cluster_addr_, cluster_addr_))) {
      LOG_ERROR("failed to deep copy cluster addr", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, args.user_name_, user_name_))) {
      LOG_ERROR("failed to deep copy user name", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, args.password_, password_))) {
      LOG_ERROR("failed to deep copy password", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, args.archive_dest_, archive_dest_))) {
      LOG_ERROR("failed to deep copy archive dest", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, table_list, table_list_))) {
      LOG_ERROR("failed to deep copy table list", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, args.column_cond_, column_cond_))) {
      LOG_ERROR("failed to deep copy column cond", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, operations, operations_))) {
      LOG_ERROR("failed to deep copy operations", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, args.output_, output_dst_))) {
      LOG_ERROR("failed to deep copy output dest", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, args.log_level_, log_level_))) {
      LOG_ERROR("failed to deep copy log level", K(args));
    } else if (OB_FAIL(deep_copy_cstring(alloc_, timezone, timezone_))) {
      LOG_ERROR("failed to deep copy timezone", K(args));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tz_info.set_timezone(ObString(timezone_)))) {
        LOG_ERROR("parse timezone failed", K(ret), KCSTRING(timezone_));
        LOGMINER_STDOUT("parse timezone failed\n");
      } else {
        record_format_ = get_record_file_format(record_format_str);
        if (RecordFileFormat::INVALID == record_format_) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("parse record_format failed", K(ret), KCSTRING(record_format_str));
          LOGMINER_STDOUT("parse record_format failed\n");
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (is_number(args.start_time_)) {
        if (OB_FAIL(logminer_str2ll(args.start_time_, start_time_us_))) {
          LOG_ERROR("parse start time for AnalyzerArgs failed", K(ret), K(args),
              K(start_time_us_), K(errno), K(strerror(errno)));
        }
      } else if (OB_FAIL(ObTimeConverter::str_to_datetime(ObString(args.start_time_), cvrt_ctx, start_time_us_))) {
        LOG_ERROR("parse start time for AnalyzerArgs failed", K(ret), K(args), K(start_time_us_));
      }
      if (OB_FAIL(ret)) {
        LOGMINER_STDOUT("parse start time for AnalyzerArgs failed\n");
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr == args.end_time_) {
        end_time_us_ = ObTimeUtility::current_time();
      } else if (is_number(args.end_time_)) {
        if (OB_FAIL(logminer_str2ll(args.end_time_, end_time_us_))) {
          LOG_ERROR("parse end time for AnalyzerArgs failed", K(ret), K(args), K(end_time_us_), K(errno),
              K(strerror(errno)));
        }
      } else if (OB_FAIL(ObTimeConverter::str_to_datetime(ObString(args.end_time_), cvrt_ctx, end_time_us_))) {
        LOG_ERROR("parse end time for AnalyzerArgs failed", K(ret), K(args), K(end_time_us_));
      }
      if (OB_FAIL(ret)) {
        LOGMINER_STDOUT("parse end time for AnalyzerArgs failed\n");
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(validate())) {
        LOG_ERROR("validate AnalyzerArgs failed after initialize all arguments", K(args), KPC(this));
      } else {
        // succ
        LOG_INFO("finished to init analyzer_args", KPC(this));
      }
    }
  }
  return ret;
}

int AnalyzerArgs::validate() const
{
  int ret = OB_SUCCESS;
  bool is_server_avail = (cluster_addr_ != nullptr) && (user_name_ != nullptr) && (password_ != nullptr);
  bool is_server_arg_set = (cluster_addr_ != nullptr) || (user_name_ != nullptr) || (password_ != nullptr);
  bool is_archive_avail = (archive_dest_ != nullptr);
  if ((is_archive_avail && is_server_arg_set) || (!is_archive_avail && !is_server_avail)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("(cluster_addr, user_name, password) and (archive_dest) are both"
        " specified or not completely specified", K(is_server_avail), K(is_archive_avail));
    LOGMINER_STDOUT("(cluster_addr, user_name, password) and (archive_dest) are both"
        " specified or not completely specified\n");
  } else if (OB_INVALID_TIMESTAMP == start_time_us_ ||
             OB_INVALID_TIMESTAMP == end_time_us_ ||
             start_time_us_ >= end_time_us_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("start_time or end_time is not valid or start_time exceeds end_time",
        K(start_time_us_), K(end_time_us_));
    LOGMINER_STDOUT("start_time or end_time is not valid or start_time exceeds end_time\n");
  } else if (nullptr == output_dst_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("output_dst is null which is not expected", KPC(this));
    LOGMINER_STDOUT("output_dst should be specified\n");
  } else {
    const char *user_name_delim = "@#";
    char *user_name_buf = nullptr;
    char *strtok_ptr = nullptr;
    char *user = nullptr;
    char *tenant = nullptr;
    int user_name_size = 0;
    if (is_server_avail) {
      // only support user tenant
      user_name_size = strlen(user_name_) + 1;
      if (OB_ISNULL(user_name_buf = reinterpret_cast<char*>(ob_malloc(user_name_size, "LogMnrUsrBuf")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("err alloc string buffer", K(ret), K(user_name_size));
      } else {
        MEMCPY(user_name_buf, user_name_, user_name_size-1);
        user_name_buf[user_name_size-1] = '\0';
      }
      user = STRTOK_R(user_name_buf, user_name_delim, &strtok_ptr);
      tenant = STRTOK_R(nullptr, user_name_delim, &strtok_ptr);
      if (nullptr == tenant || 0 == strcmp(tenant, OB_SYS_TENANT_NAME)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("oblogminer only support user tenant(e.g., user@tenant).", K(tenant));
        LOGMINER_STDOUT("oblogminer only support user tenant(e.g., user@tenant).\n");
      }
      // check password not empty
      if (OB_SUCC(ret)) {
        if (0 == strcmp(password_, "")) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("password should not be empty string", KCSTRING(password_));
          LOGMINER_STDOUT("password should not be empty string\n");
        }
      }
    }
    if (OB_SUCC(ret) && nullptr != tenant) {
      bool validate = true;
      if (OB_FAIL(validate_table_list_(tenant, validate))) {
        LOG_ERROR("validate table list failed", K(ret), KCSTRING(tenant), KCSTRING(table_list_));
        LOGMINER_STDOUT("parse table_list failed\n");
      } else {
        if (!validate) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("tenant in table_list must match the tenant specified in user-name", KCSTRING(tenant));
          LOGMINER_STDOUT("tenant in table_list must match the tenant specified in user-name\n");
        }
      }
    }
    if (OB_NOT_NULL(user_name_buf)) {
      ob_free(user_name_buf);
    }
  }

  return ret;
}

// check tenant name in table_list
int AnalyzerArgs::validate_table_list_(const char *tenant_name, bool &validate) const
{
  int ret = OB_SUCCESS;
  const char pattern_delimiter = '|';
  const char name_delimiter = '.';
  bool done = false;
  int buffer_size = strlen(table_list_) + 1;
  char *buffer = nullptr;
  ObString remain;
  ObString cur_pattern;
  ObString tenant_pattern;
  ObString database_pattern;
  ObString table_pattern;
  if (OB_ISNULL(buffer = reinterpret_cast<char*>(ob_malloc(buffer_size, "LogMnrTableList")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("err alloc string buffer", K(ret), K(buffer_size));
  } else {
    MEMCPY(buffer, table_list_, buffer_size-1);
    buffer[buffer_size-1] = '\0';
    remain.assign_ptr(buffer, buffer_size);
  }
  while (OB_SUCCESS == ret && !done) {
    // Split Pattern & get current pattern.
    cur_pattern = remain.split_on(pattern_delimiter);
    if (cur_pattern.empty()) {
      cur_pattern = remain;
      done = true;
    }
    if (OB_SUCC(ret)) {
      tenant_pattern = cur_pattern.split_on(name_delimiter);
      if (tenant_pattern.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argment", K(ret), K(cur_pattern));
        break;
      }
      if (OB_SUCC(ret)) {
        if (0 != tenant_pattern.compare(tenant_name) && 0 != tenant_pattern.compare("*")) {
          validate = false;
          LOG_INFO("tenant in table_list must match the tenant specified in user-name",
              KCSTRING(tenant_name), K(tenant_pattern));
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      database_pattern = cur_pattern.split_on(name_delimiter);
      if (database_pattern.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argument", KR(ret), K(cur_pattern));
        break;
      }
    }
    // Table name.
    if (OB_SUCC(ret)) {
      table_pattern = cur_pattern;
      if (table_pattern.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("invalid argment", KR(ret), K(cur_pattern));
        break;
      }
    }
  } // while
  if (OB_NOT_NULL(buffer)) {
    ob_free(buffer);
  }
  return ret;
}

int AnalyzerArgs::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // make sure there is no nullptr among class members
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", CLUSTER_ADDR_KEY,
      empty_str_wrapper(cluster_addr_)))) {
    LOG_ERROR("failed to print cluster_addr", KCSTRING(cluster_addr_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", USER_NAME_KEY,
      empty_str_wrapper(user_name_)))) {
    LOG_ERROR("failed to print user_name", KCSTRING(user_name_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", PASSWORD_KEY,
      empty_str_wrapper(password_)))) {
    LOG_ERROR("failed to print password", K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", ARCHIVE_DEST_KEY,
      empty_str_wrapper(archive_dest_)))) {
    LOG_ERROR("failed to print archive_dest", K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", TABLE_LIST_KEY,
      empty_str_wrapper(table_list_)))) {
    LOG_ERROR("failed to print table_list", KCSTRING(table_list_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", COLUMN_COND_KEY,
      empty_str_wrapper(column_cond_)))) {
    LOG_ERROR("failed to print column cond", KCSTRING(column_cond_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", OPERATIONS_KEY,
      empty_str_wrapper(operations_)))) {
    LOG_ERROR("failed to print operations", KCSTRING(operations_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", OUTPUT_DST_KEY,
      empty_str_wrapper(output_dst_)))) {
    LOG_ERROR("failed to print output_dst", K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", LOG_LEVEL_KEY,
      empty_str_wrapper(log_level_)))) {
    LOG_ERROR("failed to print log_level", KCSTRING(log_level_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n", START_TIME_US_KEY,
      start_time_us_))) {
    LOG_ERROR("failed to print start_time_us", K(start_time_us_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%ld\n", END_TIME_US_KEY,
      end_time_us_))) {
    LOG_ERROR("failed to print end_time_us", K(end_time_us_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", TIMEZONE_KEY,
      timezone_))) {
    LOG_ERROR("failed to print timezone", KCSTRING(timezone_), K(buf_len), K(pos));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s=%s\n", RECORD_FORMAT_KEY,
      record_file_format_str(record_format_)))) {
    LOG_ERROR("failed to print record format", KCSTRING(record_file_format_str(record_format_)),
        K(buf_len), K(pos));
  }
  return ret;
}

int AnalyzerArgs::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObString record_format_str;
  if (OB_FAIL(parse_line(CLUSTER_ADDR_KEY, buf, data_len, pos, alloc_, cluster_addr_))) {
    LOG_ERROR("failed to get cluster_addr", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(USER_NAME_KEY, buf, data_len, pos, alloc_, user_name_))) {
    LOG_ERROR("failed to get user_name", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(PASSWORD_KEY, buf, data_len, pos, alloc_, password_))) {
    LOG_ERROR("failed to get password", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(ARCHIVE_DEST_KEY, buf, data_len, pos, alloc_, archive_dest_))) {
    LOG_ERROR("failed to get archive_dest", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(TABLE_LIST_KEY, buf, data_len, pos, alloc_, table_list_))) {
    LOG_ERROR("failed to get table_list", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(COLUMN_COND_KEY, buf, data_len, pos, alloc_, column_cond_))) {
    LOG_ERROR("failed to get column_cond", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(OPERATIONS_KEY, buf, data_len, pos, alloc_, operations_))) {
    LOG_ERROR("failed to get operations", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(OUTPUT_DST_KEY, buf, data_len, pos, alloc_, output_dst_))) {
    LOG_ERROR("failed to get output_dst", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(LOG_LEVEL_KEY, buf, data_len, pos, alloc_, log_level_))) {
    LOG_ERROR("failed to get log_level", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(START_TIME_US_KEY, buf, data_len, pos, start_time_us_))) {
    LOG_ERROR("failed to get start_time_us", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(END_TIME_US_KEY, buf, data_len, pos, end_time_us_))) {
    LOG_ERROR("failed to get end_time_us", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(TIMEZONE_KEY, buf, data_len, pos, alloc_, timezone_))) {
    LOG_ERROR("failed to get timezone", K(data_len), K(pos));
  } else if (OB_FAIL(parse_line(RECORD_FORMAT_KEY, buf, data_len, pos, record_format_str))) {
    LOG_ERROR("failed to ger record_format", K(data_len), K(pos));
  }
  if (OB_SUCC(ret)) {
    record_format_ = get_record_file_format(record_format_str);
  }
  return ret;
}

int64_t AnalyzerArgs::get_serialize_size() const
{
  int64_t size = 0;
  char buf[30] = {0};
  int64_t start_time_us_len = snprintf(buf, sizeof(buf), "%ld", start_time_us_);
  int64_t end_time_us_len = snprintf(buf, sizeof(buf), "%ld", end_time_us_);
  // need add "=" and "\n"
  size += strlen(CLUSTER_ADDR_KEY) + 1 + strlen(empty_str_wrapper(cluster_addr_)) + 1;
  size += strlen(USER_NAME_KEY) + 1 + strlen(empty_str_wrapper(user_name_)) + 1;
  size += strlen(PASSWORD_KEY) + 1 + strlen(empty_str_wrapper(password_)) + 1;
  size += strlen(ARCHIVE_DEST_KEY) + 1 + strlen(empty_str_wrapper(archive_dest_)) + 1;
  size += strlen(TABLE_LIST_KEY) + 1 + strlen(empty_str_wrapper(table_list_)) + 1;
  size += strlen(COLUMN_COND_KEY) + 1 + strlen(empty_str_wrapper(column_cond_)) + 1;
  size += strlen(OPERATIONS_KEY) + 1 + strlen(empty_str_wrapper(operations_)) + 1;
  size += strlen(OUTPUT_DST_KEY) + 1 + strlen(empty_str_wrapper(output_dst_)) + 1;
  size += strlen(LOG_LEVEL_KEY) + 1 + strlen(empty_str_wrapper(log_level_)) + 1;
  size += strlen(START_TIME_US_KEY) + 1 + start_time_us_len + 1;
  size += strlen(END_TIME_US_KEY) + 1 + end_time_us_len + 1;
  size += strlen(TIMEZONE_KEY) + 1 + strlen(empty_str_wrapper(timezone_)) + 1;
  size += strlen(RECORD_FORMAT_KEY) + 1 + strlen(record_file_format_str(record_format_)) + 1;
  return size;
}

/////////////////////////////////////// FlashbackerArgs ///////////////////////////////////////

void FlashbackerArgs::reset()
{
  cluster_addr_ = nullptr;
  user_name_ = nullptr;
  password_ = nullptr;
  recovery_path_ = nullptr;
  tgt_table_ = nullptr;
  src_table_ = nullptr;
  progress_dst_ = nullptr;
  log_level_ = nullptr;
  start_time_us_ = OB_INVALID_TIMESTAMP;
  end_time_us_ = OB_INVALID_TIMESTAMP;
}

int FlashbackerArgs::init(const ObLogMinerCmdArgs &args)
{
  int ret = OB_SUCCESS;
  if (!is_flashback_mode(args.mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("incorrect cmd args to init flashbacker args", K(args));
  } else {
    cluster_addr_ = args.cluster_addr_;
    user_name_ = args.user_name_;
    password_ = args.password_;
    recovery_path_ = args.recovery_path_;
    tgt_table_ = args.target_table_;
    src_table_ = args.source_table_;
    progress_dst_ = args.output_;
    log_level_ = args.log_level_;

    if (OB_FAIL(logminer_str2ll(args.start_time_, start_time_us_))) {
      LOG_ERROR("parse start time for FlashbackerArgs failed", K(args), K(start_time_us_));
    } else if (OB_FAIL(logminer_str2ll(args.end_time_, end_time_us_))) {
      LOG_ERROR("parse end time for FlashbackerArgs failed", K(args), K(end_time_us_));
    } else if (OB_FAIL(validate())) {
      LOG_ERROR("validate FlashbackerArgs failed after initialize all arguments", K(args), KPC(this));
    } else {
      // succ
      LOG_INFO("finished to init flashbacker args", KPC(this));
    }
  }

  return ret;
}

int FlashbackerArgs::validate() const
{
  int ret = OB_SUCCESS;

  if (nullptr == cluster_addr_ || nullptr == user_name_ || nullptr == password_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("cluster_addr, user_name or password is empty", KPC(this));
  } else if (nullptr == tgt_table_ || nullptr == src_table_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("target-table or source-table is empty", KPC(this));
  } else if (nullptr == progress_dst_ || nullptr == recovery_path_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("recovery path or progress_dst is empty", KPC(this));
  } else if (OB_INVALID_TIMESTAMP == start_time_us_ ||
             OB_INVALID_TIMESTAMP == end_time_us_ ||
             start_time_us_ >= end_time_us_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("start_time or end_time is not valid", K(start_time_us_), K(end_time_us_), KPC(this));
  } else {
    // succ
  }

  return ret;
}

int FlashbackerArgs::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int FlashbackerArgs::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int64_t FlashbackerArgs::get_serialize_size() const
{
  int64_t size = 0;
  // TODO
  return size;
}

/////////////////////////////////////// ObLogMinerArgs ///////////////////////////////////////
const char *ObLogMinerArgs::DEFAULT_LOG_LEVEL = "ALL.*:INFO;PALF.*:WARN;SHARE.SCHEMA:WARN";
const char *ObLogMinerArgs::LOGMINER_LOG_FILE = "oblogminer.log";
const char *ObLogMinerArgs::DEFAULT_LOGMNR_TIMEZONE = "+8:00";
const char *ObLogMinerArgs::DEFAULT_LOGMNR_OPERATIONS = "insert|delete|update";
const char *ObLogMinerArgs::DEFAULT_LOGMNR_TABLE_LIST = "*.*.*";
const char *ObLogMinerArgs::DEFAULT_LOGMNR_FORMAT = "csv";


void ObLogMinerArgs::reset()
{
  mode_ = LogMinerMode::UNKNOWN;
  verbose_ = false;
  print_usage_ = false;
  timezone_ = nullptr;
  analyzer_args_.reset();
  flashbacker_args_.reset();
}

int ObLogMinerArgs::validate() const
{
  int ret = OB_SUCCESS;
  if (! is_logminer_mode_valid(mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("logminer mode is invalid", K(mode_));
  } else if (is_analysis_mode(mode_)) {
    ret = analyzer_args_.validate();
  } else if (is_flashback_mode(mode_)) {
    ret = flashbacker_args_.validate();
  }

  if (OB_FAIL(ret)) {
    LOG_ERROR("ObLogMiner found some arguments invalid", KPC(this));
  }

  return ret;
}

int ObLogMinerArgs::init(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ObLogMinerCmdArgs args;
  if (OB_FAIL(args.init(argc, argv))) {
    LOG_ERROR("init ObLogMinerCmdArgs failed", K(argc));
  } else if (args.print_usage_) {
    print_usage_ = args.print_usage_;
  } else if (! is_logminer_mode_valid(args.mode_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ObLogMinerArgs got invalid mode argument", K(args.mode_),
        "mode", logminer_mode_to_str(args.mode_));
  } else {
    mode_ = args.mode_;
    verbose_ = args.verbose_;
    timezone_ = args.timezone_ == nullptr ? DEFAULT_LOGMNR_TIMEZONE : args.timezone_;

    if (nullptr == args.log_level_) {
      args.log_level_ = DEFAULT_LOG_LEVEL;
    }

    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to set mod log level previously, unexpected");
    } else if (is_analysis_mode(args.mode_)) {
      if (OB_FAIL(analyzer_args_.init(args))) {
        LOG_ERROR("failed to ini analyzer args", K(args.mode_), K(analyzer_args_), K(args));
      }
    } else if (is_flashback_mode(args.mode_)) {
      if (OB_FAIL(flashbacker_args_.init(args))) {
        LOG_ERROR("failed to ini flashbacker args", K(args.mode_), K(flashbacker_args_), K(args));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("vaild logminer mode is ignored, which is unexpected", K(args.mode_),
          "mode", logminer_mode_to_str(args.mode_));
    }
  }

  return ret;
}

int ObLogMinerArgs::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf + pos, buf_len, pos, "mode=%s\n", logminer_mode_to_str(mode_)))) {
    LOG_ERROR("failed to serialize mode", K(mode_));
  } else if (is_analysis_mode(mode_)) {
    if (OB_FAIL(analyzer_args_.serialize(buf, buf_len, pos))) {
      LOG_ERROR("analyzer_args failed to serialize", K(buf), K(buf_len), K(pos), K(analyzer_args_));
    }
  } else if (is_flashback_mode(mode_)) {
    if (OB_FAIL(flashbacker_args_.serialize(buf, buf_len, pos))) {
      LOG_ERROR("flashbacker_args failed to serialize", K(buf), K(buf_len), K(pos), K(flashbacker_args_));
    }
  }
  return ret;
}

int ObLogMinerArgs::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObString mode_str;
  if (OB_FAIL(parse_line("mode", buf, data_len, pos, mode_str))) {
    LOG_ERROR("failed to get mode string", K(data_len), K(pos));
  } else {
    mode_ = get_logminer_mode(mode_str);
    if (is_analysis_mode(mode_)) {
      if (OB_FAIL(analyzer_args_.deserialize(buf, data_len, pos))) {
        LOG_ERROR("failed to deserialize analyzer arguments", K(data_len), K(pos));
      }
    } else {
      ret = OB_INVALID_DATA;
      LOG_ERROR("get invalid mode when deserializing logminer args", K(mode_str), K(mode_));
    }
  }
  return ret;
}

int64_t ObLogMinerArgs::get_serialize_size() const
{
  int64_t size = 0;

  size += strlen("mode=") + strlen(logminer_mode_to_str(mode_)) + strlen("\n");

  if (is_analysis_mode(mode_)) {
    size += analyzer_args_.get_serialize_size();
  } else if (is_flashback_mode(mode_)) {
    size += flashbacker_args_.get_serialize_size();
  }

  return size;
}


int64_t ObLogMinerArgs::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_print_kv(buf, buf_len, pos, "mode", mode_);
  if (is_analysis_mode(mode_)) {
    databuff_print_kv(buf, buf_len, pos, "analyzer_args", analyzer_args_);
  } else if (is_flashback_mode(mode_)) {
    databuff_print_kv(buf, buf_len, pos, "flashbacker_args", flashbacker_args_);
  } else {
    // invalid mode
  }
  return pos;
}



}
}