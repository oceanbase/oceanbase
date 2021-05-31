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

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <new>
#include <pthread.h>
#include "lib/profile/ob_profile_log.h"
#include "lib/ob_define.h"

namespace oceanbase {
namespace common {
const char* const ObProfileLogger::errmsg_[2] = {"INFO", "DEBUG"};
ObProfileLogger* ObProfileLogger::logger_ = NULL;
ObProfileLogger::LogLevel ObProfileLogger::log_levels_[2] = {INFO, DEBUG};

ObProfileLogger::ObProfileLogger() : log_dir_(strdup("./")), log_filename_(strdup("")), log_level_(INFO), log_fd_(2)
{}

ObProfileLogger* ObProfileLogger::getInstance()
{
  if (NULL == logger_) {
    logger_ = new (std::nothrow) ObProfileLogger();
  }
  return logger_;
}

ObProfileLogger::~ObProfileLogger()
{
  if (log_fd_ >= 0) {
    if (0 != close(log_fd_)) {
      fprintf(stderr, "fail to close file\n");
    }
    log_fd_ = -1;
  }
  if (NULL != log_dir_) {
    free(log_dir_);
    log_dir_ = NULL;
  }
  if (NULL != log_filename_) {
    free(log_filename_);
    log_filename_ = NULL;
  }
}

void ObProfileLogger::setLogLevel(const char* log_level)
{
  for (int i = 0; i < 2; ++i) {
    if (0 == STRCASECMP(log_level, errmsg_[i])) {
      log_level_ = log_levels_[i];
      break;
    }
  }
}

void ObProfileLogger::setLogDir(const char* log_dir)
{
  if (OB_LIKELY(log_dir != NULL)) {
    free(log_dir_);
    log_dir_ = strdup(log_dir);
  }
}

void ObProfileLogger::setFileName(const char* log_filename)
{
  if (OB_LIKELY(log_filename != NULL)) {
    free(log_filename_);
    log_filename_ = strdup(log_filename);
  }
}

int ObProfileLogger::init()
{
  int ret = OB_SUCCESS;
  char buf[128];
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos, "%s/%s", log_dir_, log_filename_))) {
    ret = OB_ERROR;
    fprintf(stderr, "fail to snprintf to buf\n");
  } else {
    int fd = open(buf, O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd != -1) {
      // stderr
      // dup2(fd,2);
      log_fd_ = fd;
    } else {
      ret = OB_ERROR;
      fprintf(stderr, "open log file error\n");
    }
  }
  return ret;
}

int ObProfileLogger::printlog(
    LogLevel level, const char* file, int line, const char* function, pthread_t tid, const char* fmt, ...)
{
  int ret = OB_SUCCESS;
  if (level > log_level_) {
    // nothing to do
  } else {
    time_t t;
    time(&t);
    struct tm tm;
    localtime_r(&t, &tm);
    const int log_len = 512;
    const int buf_len = 256;
    char log[log_len];
    char buf[buf_len];
    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(log, log_len, fmt, args);
    if (OB_UNLIKELY(0 > written)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (written >= log_len) {
      written = log_len - 1;
    }
    if (OB_SUCC(ret)) {
      log[written] = '\n';
      const uint64_t* trace_id = ObCurTraceId::get();
      if (OB_ISNULL(trace_id)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ssize_t size = static_cast<ssize_t>(snprintf(buf,
            256,
            "[%04d-%02d-%02d %02d:%02d:%02d] %-5s %s (%s:%d) [%ld] trace_id=[" TRACE_ID_FORMAT
            "] source_chid=[%u] chid=[%u]",
            tm.tm_year + 1900,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec,
            errmsg_[level],
            function,
            file,
            line,
            tid,
            trace_id[0],
            trace_id[1],
            SRC_CHID,
            CHID));
        struct iovec iov[2];
        iov[0].iov_base = buf;
        iov[0].iov_len = size;
        iov[1].iov_base = log;
        iov[1].iov_len = written + 1;
        if (-1 == writev(log_fd_, iov, 2)) {
          ret = OB_ERR_UNEXPECTED;
          fprintf(stderr, "fail to writev\n");
        }
      }
    }
    va_end(args);
  }
  return ret;
}

int ObProfileLogger::printlog(LogLevel level, const char* file, int line, const char* function, pthread_t tid,
    const int64_t consume, const char* fmt, ...)
{
  int ret = OB_SUCCESS;
  if (level > log_level_) {
    // nothing to do
  } else {
    time_t t = time(NULL);
    time(&t);
    struct tm tm;
    localtime_r(&t, &tm);
    const int log_len = 512;
    const int buf_len = 256;
    char log[log_len];
    char buf[buf_len];
    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(log, log_len, fmt, args);
    if (OB_UNLIKELY(0 > written)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (written >= log_len) {
      written = log_len - 1;
    }

    if (OB_SUCC(ret)) {
      log[written] = '\n';
      const uint64_t* trace_id = ObCurTraceId::get();
      if (OB_ISNULL(trace_id)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ssize_t size = static_cast<ssize_t>(snprintf(buf,
            256,
            "[%04d-%02d-%02d %02d:%02d:%02d] %-5s %s (%s:%d) [%ld] trace_id=[" TRACE_ID_FORMAT
            "] source_chid=[%u] chid=[%u], consume=[%ld] ",
            tm.tm_year + 1900,
            tm.tm_mon + 1,
            tm.tm_mday,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec,
            errmsg_[level],
            function,
            file,
            line,
            tid,
            trace_id[0],
            trace_id[1],
            SRC_CHID,
            CHID,
            consume));
        struct iovec iov[2];
        iov[0].iov_base = buf;
        iov[0].iov_len = size;
        iov[1].iov_base = log;
        iov[1].iov_len = written + 1;
        if (-1 == writev(log_fd_, iov, 2)) {
          ret = OB_ERR_UNEXPECTED;
          fprintf(stderr, "fail to writev\n");
        }
      }
    }
    va_end(args);
  }
  return ret;
}
}  // namespace common
}  // namespace oceanbase
