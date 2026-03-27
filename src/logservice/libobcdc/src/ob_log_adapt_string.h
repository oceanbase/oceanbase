/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * Adaptive strings
 * 1. manage your own memory
 * 2. double size if memory is not enough
 */

#ifndef OCEANBASE_OB_LOG_ADAPT_STRING_H__
#define OCEANBASE_OB_LOG_ADAPT_STRING_H__

#include "lib/alloc/alloc_struct.h"   // ObMemAttr
#include "common/data_buffer.h"       // ObDataBuffer

#include "ob_log_utils.h"             // _K_

namespace oceanbase
{
namespace libobcdc
{
class ObLogAdaptString
{
public:
  explicit ObLogAdaptString(const char *label);
  virtual ~ObLogAdaptString();

  int append(const char *data);
  int append_int64(const int64_t int_val);

  // Supports calling append function again after cstr to fill
  // If the user has not called the append function, the empty string is returned, for compatibility with std::string
  int cstr(const char *&str);

public:
  TO_STRING_KV(K_(buf));

private:
  int alloc_buf_(const int64_t data_size, char *&data_buf);

private:
  lib::ObMemAttr        attr_;
  common::ObDataBuffer  buf_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogAdaptString);
};

}
}
#endif
