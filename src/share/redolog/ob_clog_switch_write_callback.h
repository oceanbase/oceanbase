/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_CLOG_SWITCH_WRITE_CALLBACK_H_
#define OCEANBASE_COMMON_OB_CLOG_SWITCH_WRITE_CALLBACK_H_

#include <stdint.h>
#include "share/redolog/ob_log_write_callback.h"

namespace oceanbase
{
namespace common
{
class ObCLogSwitchWriteCallback : public ObLogWriteCallback
{
public:
  ObCLogSwitchWriteCallback();
  virtual ~ObCLogSwitchWriteCallback();
public:
  int init();
  void destroy();
  virtual int handle(
      const char *input_buf,
      const int64_t input_size,
      char *&output_buf,
      int64_t &output_size) override;
  int64_t get_info_block_len() const { return info_block_len_; }
  const char* get_buffer() const { return buffer_; }
private:
  bool is_inited_;
  char *buffer_;
  int64_t buffer_size_;
  int64_t info_block_len_;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_CLOG_SWITCH_WRITE_CALLBACK_H_
