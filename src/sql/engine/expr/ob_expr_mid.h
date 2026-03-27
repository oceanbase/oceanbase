/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
