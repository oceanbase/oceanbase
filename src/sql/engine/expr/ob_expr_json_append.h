/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_APPEND_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_APPEND_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_json_array_append.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonAppend : public ObExprJsonArrayAppend
{
public:
  explicit ObExprJsonAppend(common::ObIAllocator &alloc);
  virtual ~ObExprJsonAppend();
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonAppend);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_APPEND_H_