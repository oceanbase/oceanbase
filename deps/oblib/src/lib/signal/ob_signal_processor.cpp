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

#define USING_LOG_PREFIX COMMON

#include "lib/signal/ob_signal_processor.h"
#include <unistd.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/signal/ob_signal_utils.h"
#include "lib/signal/ob_libunwind.h"

namespace oceanbase
{
namespace common
{
ObSigBTOnlyProcessor::ObSigBTOnlyProcessor()
  : fd_(-1), pos_(-1)
{
  char *buf = filename_;
  int64_t len = sizeof(filename_);
  int64_t pos = safe_snprintf(buf, len, "stack.%d.", getpid());
  int64_t count = 0;
  safe_current_datetime_str(buf + pos, len - pos, count);
  pos += count;
  buf_[pos] = '\0';
  fd_ = ::open(filename_, O_CREAT | O_WRONLY | O_APPEND, S_IRUSR | S_IWUSR);
}

ObSigBTOnlyProcessor::~ObSigBTOnlyProcessor()
{
  if (fd_ != -1) {
    CLOSE(fd_);
  }
}

int ObSigBTOnlyProcessor::start()
{
  int64_t len = 0;
  const char *buf = ObProcMaps::get_instance().get_maps(len);
  ::write(fd_, buf, static_cast<int32_t>(len));
  return OB_SUCCESS;
}

int ObSigBTOnlyProcessor::prepare()
{
  int ret = OB_SUCCESS;
  pos_ = 0;
  int64_t len = sizeof(buf_) - 1;
  int64_t tid = syscall(SYS_gettid);
  char tname[16];
  prctl(PR_GET_NAME, tname);
  int64_t count = 0;
  count = safe_snprintf(buf_ + pos_, len - pos_, "tid: %ld, tname: %s, lbt: ", tid, tname);
  pos_ += count;
#ifdef __x86_64__
  safe_backtrace(buf_ + pos_, len - pos_, &count);
#endif
  pos_ += count;
  buf_[pos_++] = '\n';
  return ret;
}

int ObSigBTOnlyProcessor::process()
{
  int ret = OB_SUCCESS;
  if (pos_ > 0) {
    buf_[pos_] = '\0';
    ::write(fd_, buf_, (int)pos_);
  }
  return ret;
}

int ObSigBTSQLProcessor::prepare()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSigBTOnlyProcessor::prepare())) {
  } else {
    // TODO: save sql_
    int64_t len = sizeof(buf_) - 1;
    pos_--;
    int64_t count = safe_snprintf(buf_ + pos_, len - pos_, ", sql: TODO");
    pos_ += count;
    buf_[pos_++] = '\n';
  }
  return ret;
}

int ObSigBTSQLProcessor::process()
{
  int ret = ObSigBTOnlyProcessor::process();
  return ret;
}

} // namespace common
} // namespace oceanbase
