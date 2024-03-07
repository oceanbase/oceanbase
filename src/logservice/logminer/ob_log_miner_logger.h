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

#ifndef OCEANBASE_LOG_MINER_LOGGER_H_
#define OCEANBASE_LOG_MINER_LOGGER_H_

namespace oceanbase
{
namespace oblogminer
{
#define LOGMINER_STDOUT(...) \
::oceanbase::oblogminer::LogMinerLogger::get_logminer_logger_instance().log_stdout(__VA_ARGS__)
#define LOGMINER_STDOUT_V(...) \
::oceanbase::oblogminer::LogMinerLogger::get_logminer_logger_instance().log_stdout_v(__VA_ARGS__)
#define LOGMINER_LOGGER \
::oceanbase::oblogminer::LogMinerLogger::get_logminer_logger_instance()
#define PB_STR ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
#define PB_WIDTH 50
class LogMinerLogger {
public:
  LogMinerLogger();
  static LogMinerLogger &get_logminer_logger_instance();
  void set_verbose(bool verbose) {
    verbose_ = verbose;
  }
  void log_stdout(const char *format, ...);
  void log_stdout_v(const char *format, ...);
  int log_progress(int64_t record_num, int64_t current_ts, int64_t begin_ts, int64_t end_ts);

private:
  bool verbose_;
};

}
}

#endif