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
 * This file contains implementation for json_member_of.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_MEMBER_OF_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_MEMBER_OF_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_wrapper.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExprJsonMemberOf : public ObFuncExprOperator
{
public:
  explicit ObExprJsonMemberOf(common::ObIAllocator &alloc);
  virtual ~ObExprJsonMemberOf();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprTypeCtx &type_ctx) const override;
  static int eval_json_member_of(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  // need a runtime ctx (ObJsonParamCacheCtx) to cache the constant candidate
  // wrapper across rows; this also allocates expr_ctx_id_ at code-gen time.
  virtual bool need_rt_ctx() const override { return true; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonMemberOf);
  static int check_json_member_of_array(const ObJsonWrapper &candidate,
                                        const ObJsonWrapper &array,
                                        bool &is_member);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_MEMBER_OF_H_