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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_MID_
#define OCEANBASE_SQL_ENGINE_EXPR_MID_

#include "sql/engine/expr/ob_expr_substr.h"

namespace oceanbase
{
namespace sql
{

class ObExprMid : public ObExprSubstr
{
public:
  explicit  ObExprMid(common::ObIAllocator &alloc)
    : ObExprSubstr(alloc)
  {
    *(const_cast<ObExprOperatorType*>(&type_)) = T_FUN_SYS_MID;
    *(const_cast<const char**>(&name_)) = N_MID;
  };
  virtual ~ObExprMid() {};
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprMid);
};

} // namespace sql
} // namespace oceanbase
#endif
