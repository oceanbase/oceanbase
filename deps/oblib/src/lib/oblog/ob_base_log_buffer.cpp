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

#include "ob_base_log_buffer.h"
#include <string.h>
#include <errno.h>
#include <new>
#include <sys/mman.h>
#include <sys/sysinfo.h>
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/file/file_directory_utils.h"

namespace oceanbase
{
namespace common
{
ObBaseLogBufferMgr::ObBaseLogBufferMgr() : log_buf_cnt_(0)
{
}

ObBaseLogBufferMgr::~ObBaseLogBufferMgr()
{
  destroy();
}

void ObBaseLogBufferMgr::destroy()
{
  for (int64_t i = 0; i < log_buf_cnt_; ++i) {
    if (NULL != log_ctrls_[i].base_buf_) {
      munmap(log_ctrls_[i].base_buf_, SHM_BUFFER_SIZE);
      log_ctrls_[i].base_buf_ = NULL;
      log_ctrls_[i].data_buf_ = NULL;
    }
  }
  log_buf_cnt_ = 0;
}

int ObBaseLogBufferMgr::get_abs_dir(const char *log_dir, const char *&abs_log_dir)
{
  int ret = OB_SUCCESS;
  if (NULL == log_dir || strlen(log_dir) >= ObBaseLogBuffer::MAX_LOG_DIR_LENGTH || strlen(log_dir) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "Invalid argument, ", K(ret), KP(log_dir));
  } else {
    if ('/' == log_dir[0]) {
      //absolute dir
      abs_log_dir = log_dir;
    } else if (NULL == (abs_log_dir = realpath(log_dir, NULL))) {
      ret = OB_ERR_SYS;
      LIB_LOG(WARN, "Fail to get real path, ", K(ret), KERRMSG);
    } else if (strlen(abs_log_dir) >= ObBaseLogBuffer::MAX_LOG_DIR_LENGTH) {
      ret = OB_INVALID_ARGUMENT;
      LIB_LOG(WARN, "The log path is too long, ", K(ret), KCSTRING(log_dir));
    }

    if (OB_SUCC(ret)) {
      LIB_LOG(INFO, "Success to get abs dir, ", KCSTRING(abs_log_dir));
    }
  }
  return ret;
}

}
}
