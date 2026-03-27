/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 * This file is for define of func json_merge
 */


#ifndef OCEANBASE_SQL_OB_EXPR_JSON_MERGE_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_MERGE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_json_merge_preserve.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprJsonMerge : public ObExprJsonMergePreserve
{
public:
  explicit ObExprJsonMerge(common::ObIAllocator &alloc);
  virtual ~ObExprJsonMerge();
private:
    DISALLOW_COPY_AND_ASSIGN(ObExprJsonMerge);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_MERGE_H_