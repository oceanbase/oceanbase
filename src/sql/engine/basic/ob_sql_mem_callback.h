/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_BASIC_OB_SQL_MEMM_CALLBACK_H_
#define OCEANBASE_BASIC_OB_SQL_MEMM_CALLBACK_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

class ObSqlMemoryCallback
{
public:
  virtual ~ObSqlMemoryCallback() = default;

  virtual void alloc(int64_t size) = 0;
  virtual void free(int64_t size) = 0;
  virtual void dumped(int64_t size) = 0;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_SQL_MEMM_CALLBACK_H_
