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
#include "share/redolog/ob_clog_switch_write_callback.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{
ObCLogSwitchWriteCallback::ObCLogSwitchWriteCallback()
  : is_inited_(false),  buffer_(nullptr), buffer_size_(0),
    info_block_len_(0)
{
}

ObCLogSwitchWriteCallback::~ObCLogSwitchWriteCallback()
{
  destroy();
}

int ObCLogSwitchWriteCallback::init()
{
  return OB_NOT_SUPPORTED;
}

void ObCLogSwitchWriteCallback::destroy()
{
  if (buffer_ != nullptr) {
    ob_free(buffer_);
    buffer_ = nullptr;
    buffer_size_ = 0;
  }
  is_inited_ = false;
}

int ObCLogSwitchWriteCallback::handle(
    const char *input_buf,
    const int64_t input_size,
    char *&output_buf,
    int64_t &output_size)
{
  return OB_NOT_SUPPORTED;
}

} // namespace common
} // namespace oceanbase
