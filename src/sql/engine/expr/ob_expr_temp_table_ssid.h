/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// Copyright (c) 2011-2023 Alibaba Inc. All Rights Reserved.
// Author:
//    jim.wjh <>


#ifndef _OB_EXPR_TEMP_TABLE_SSID_H
#define _OB_EXPR_TEMP_TABLE_SSID_H
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{

class ObExprTempTableSSID : public ObFuncExprOperator
{
public:
  enum {GTT_SESSION_SCOPE, GTT_TRANS_SCOPE, MAX_GTT_SCOPTE};
public:
  explicit  ObExprTempTableSSID(common::ObIAllocator &alloc);
  virtual ~ObExprTempTableSSID();
  int calc_result_type1(ObExprResType &type, ObExprResType &type1, common::ObExprTypeCtx &type_ctx) const override;
  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static int calc_temp_table_ssid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprTempTableSSID);
};

}
}

#endif // _OB_EXPR_TEMP_TABLE_SSID_H
