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

#include "ob_i_disk_log_buffer.h"

namespace oceanbase {
using namespace common;
namespace clog {
void ObDiskBufferTask::reset()
{
  ObIBufferTask::reset();
  proposal_id_.reset();
  buf_ = NULL;
  len_ = 0;
  offset_ = 0;
  need_callback_ = true;
}

int ObDiskBufferTask::fill_buffer(char* buf, const offset_t offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_IS_INVALID_OFFSET(offset)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    offset_ = offset;
    MEMCPY(buf + offset, buf_, len_);
  }
  return ret;
}

void ObDiskBufferTask::set(char* buf, const int64_t len)
{
  buf_ = buf;
  len_ = len;
  offset_ = 0;
}
};  // end namespace clog
};  // end namespace oceanbase
