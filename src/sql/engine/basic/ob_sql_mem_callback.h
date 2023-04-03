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
