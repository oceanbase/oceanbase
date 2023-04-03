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

#ifndef OCEANBASE_COMMON_OB_PROFILE_LOG_H
#define OCEANBASE_COMMON_OB_PROFILE_LOG_H

#include "lib/profile/ob_trace_id.h"
#include "lib/thread_local/ob_tsi_factory.h"
namespace oceanbase
{
namespace common
{

// Print ip, port, seq, chid, default
class ObProfileLogger
{
public:
  enum LogLevel
  {
    INFO = 0,
    DEBUG
  };
  ~ObProfileLogger();

  static ObProfileLogger *getInstance();
  LogLevel getLogLevel() { return log_level_; }
  void setLogLevel(const char *log_level);
private:
  static const char *const errmsg_[2];
  static LogLevel log_levels_[2];
  static ObProfileLogger *logger_;
  char *log_dir_;
  char *log_filename_;
  LogLevel log_level_;
 int log_fd_;
private:
  ObProfileLogger();
  DISALLOW_COPY_AND_ASSIGN(ObProfileLogger);

};
}// common
}// oceanbase
#endif
