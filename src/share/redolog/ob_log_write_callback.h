/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_LOG_WRITE_CALLBACK_H_
#define OCEANBASE_COMMON_OB_LOG_WRITE_CALLBACK_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObLogWriteCallback
{
public:
  ObLogWriteCallback() = default;
  virtual ~ObLogWriteCallback() = default;
  virtual int handle(const char *input_buf, const int64_t input_size,
      char *&output_buf, int64_t &output_size) = 0;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_WRITE_CALLBACK_H_
