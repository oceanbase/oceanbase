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
namespace oceanbase {
namespace common {
#define PROFILE_LOGGER oceanbase::common::ObProfileLogger::getInstance()
#define PROFILE_LOG(level, fmt, args...)                                                                               \
  do {                                                                                                                 \
    if (ObProfileLogger::level <= PROFILE_LOGGER->getLogLevel()) {                                                     \
      PROFILE_LOGGER->printlog(ObProfileLogger::level, __FILE__, __LINE__, __FUNCTION__, pthread_self(), fmt, ##args); \
    }                                                                                                                  \
  } while (0);

inline uint32_t& get_src_chid()
{
  static __thread uint32_t src_chid = 0;
  return src_chid;
}
inline uint32_t& get_chid()
{
  static __thread uint32_t chid = 0;
  return chid;
}
#define SRC_CHID get_src_chid()
#define CHID get_chid()

#define INIT_PROFILE_LOG_TIMER() int64_t __PREV_TIME__ = ::oceanbase::common::ObTimeUtility::current_time();

#define PROFILE_LOG_TIME(level, fmt, args...)                                                                          \
  do {                                                                                                                 \
    int64_t now = ::oceanbase::common::ObTimeUtility::current_time();                                                  \
    if (ObProfileLogger::level <= PROFILE_LOGGER->getLogLevel()) {                                                     \
      PROFILE_LOGGER->printlog(                                                                                        \
          ObProfileLogger::level, __FILE__, __LINE__, __FUNCTION__, pthread_self(), now - __PREV_TIME__, fmt, ##args); \
    }                                                                                                                  \
    __PREV_TIME__ = now;                                                                                               \
  } while (0);

// Print ip, port, seq, chid, default
class ObProfileLogger {
public:
  enum LogLevel { INFO = 0, DEBUG };
  ~ObProfileLogger();
  int printlog(LogLevel level, const char* file, int line, const char* function, pthread_t tid, const char* fmt, ...);
  int printlog(LogLevel level, const char* file, int line, const char* function, pthread_t tid, const int64_t time,
      const char* fmt, ...);
  static ObProfileLogger* getInstance();
  LogLevel getLogLevel()
  {
    return log_level_;
  }
  void setLogLevel(const char* log_level);
  void setLogDir(const char* log_dir);
  void setFileName(const char* log_filename);
  int init();

private:
  static const char* const errmsg_[2];
  static LogLevel log_levels_[2];
  static ObProfileLogger* logger_;
  char* log_dir_;
  char* log_filename_;
  LogLevel log_level_;
  int log_fd_;

private:
  ObProfileLogger();
  DISALLOW_COPY_AND_ASSIGN(ObProfileLogger);
};
}  // namespace common
}  // namespace oceanbase
#endif
