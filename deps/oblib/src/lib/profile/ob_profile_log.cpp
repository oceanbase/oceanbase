/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/profile/ob_profile_log.h"

namespace oceanbase
{
namespace common
{
const char *const ObProfileLogger::errmsg_[2] = {"INFO", "DEBUG"};
ObProfileLogger *ObProfileLogger::logger_ = NULL;
ObProfileLogger::LogLevel ObProfileLogger::log_levels_[2] = {INFO, DEBUG};

ObProfileLogger::ObProfileLogger()
    : log_dir_(strdup("./")), log_filename_(strdup(""))
    , log_level_(INFO), log_fd_(2)
{
}

ObProfileLogger *ObProfileLogger::getInstance()
{
  if (NULL == logger_) {
    logger_ = new(std::nothrow) ObProfileLogger();
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

void ObProfileLogger::setLogLevel(const char *log_level)
{
  for (int i = 0; i < 2; ++i) {
    if (0 == STRCASECMP(log_level, errmsg_[i])) {
      log_level_ = log_levels_[i];
      break;
    }
  }
}

} //end common
} //end oceanbase
