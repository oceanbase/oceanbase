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
 * This file contains implementation for json_valid.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_IS_JSON_H_
#define OCEANBASE_SQL_OB_EXPR_IS_JSON_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace sql
{
class ObExprIsJson : public ObFuncExprOperator
{
public:
  explicit ObExprIsJson(common::ObIAllocator &alloc);
  virtual ~ObExprIsJson();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int get_is_json_option(const ObExpr &expr, ObEvalCtx &ctx, int64_t idx, uint8_t& is_json_mode);
  static int check_is_json(const ObExpr &expr, ObEvalCtx &ctx,
                           const ObDatum &data, ObObjType type,
                           ObCollationType cs_type, ObArenaAllocator &allocator,
                           uint8_t strict_opt, uint8_t scalar_opt, uint8_t unique_opt,
                           bool check_for_is_json, ObDatum &res);
  static int eval_is_json(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  const static uint8_t OB_IS_JSON_MODE_DEFAULT     = 0;
  const static uint8_t OB_IS_JSON_MODE_UNDEFAULT   = 1;
  const static uint8_t OB_IS_JSON_MODE_MAX         = 2;
  const static uint8_t OB_JSON_MODE_STRICT         = 1;
  const static uint8_t OB_JSON_MODE_UNIQUE_KEYS    = 1;
  const static uint32_t RESERVE_MIN_BUFF_SIZE = 32;
  // const static uint8_t OB_JSON_MODE_NO_UNIQUE_KEYS = 0;
  DISALLOW_COPY_AND_ASSIGN(ObExprIsJson);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_IS_JSON_H_