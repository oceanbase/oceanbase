/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_OB_LOG_OPEN_CALLBACK_H_
#define OCEANBASE_COMMON_OB_LOG_OPEN_CALLBACK_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
class ObLogOpenCallback
{
public:
  ObLogOpenCallback() = default;
  virtual ~ObLogOpenCallback() = default;
  virtual int handle(const int64_t file_id) = 0;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_OPEN_CALLBACK_H_