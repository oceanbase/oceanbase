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
