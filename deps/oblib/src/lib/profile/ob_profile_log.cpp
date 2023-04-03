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
