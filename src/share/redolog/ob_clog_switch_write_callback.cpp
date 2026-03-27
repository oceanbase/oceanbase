/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "share/redolog/ob_clog_switch_write_callback.h"
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
