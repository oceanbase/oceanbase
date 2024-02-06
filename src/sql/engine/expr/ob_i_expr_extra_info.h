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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_I_EXPR_EXTRA_INFO_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_I_EXPR_EXTRA_INFO_
#include <stdint.h>
#include "sql/engine/expr/ob_expr_extra_info_factory.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{
struct ObIExprExtraInfo
{
  ObIExprExtraInfo(common::ObIAllocator &alloc, const ObExprOperatorType &type)
    : type_(type)
  {
    UNUSED(alloc);
  }

public:
  virtual int serialize(char *buf, const int64_t len, int64_t &pos) const = 0;

  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos) = 0;

  virtual int64_t get_serialize_size() const = 0;

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const = 0;
public:
  ObExprOperatorType type_;
};

} // end namespace sql
} // end namespace oceanbase
#endif
