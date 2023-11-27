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
 * This file is for define of func json_append
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