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
 * This file contains implementation for json_exists.
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_EXISTS_H
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_EXISTS_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_base.h"

namespace oceanbase
{
namespace sql
{

class ObExprJsonExists : public ObFuncExprOperator
{
public:
  explicit ObExprJsonExists(common::ObIAllocator &alloc);
  virtual ~ObExprJsonExists();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx) const override;
  static int get_path(const ObExpr &expr, ObEvalCtx &ctx, ObJsonPath* &j_path,
                      common::ObArenaAllocator &allocator,
                      ObJsonPathCache &ctx_cache, ObJsonPathCache* &path_cache);
  static int get_var_data(const ObExpr &expr, ObEvalCtx &ctx, common::ObArenaAllocator &allocator,
                           uint16_t index, ObIJsonBase*& j_base);
  static int get_json_data(const ObExpr &expr, ObEvalCtx &ctx, common::ObArenaAllocator &allocator,
                           uint16_t index, ObIJsonBase*& j_base, bool &is_null, bool need_to_tree,
                           bool need_quote);
  static int get_passing(const ObExpr &expr, ObEvalCtx &ctx, PassingMap &pass_map,
                        uint32_t param_num, common::ObArenaAllocator &temp_allocator);
  static int get_error_or_empty(const ObExpr &expr, ObEvalCtx &ctx, uint32_t idx, uint8_t &result);
  static int set_result(ObDatum &res, const ObJsonBaseVector& hit,
                        const uint8_t option_on_error, const uint8_t option_on_empty,
                        const bool is_cover_by_error, const bool is_null_json);
  static int eval_json_exists(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
  const static uint8_t OB_JSON_FALSE_ON_ERROR = 0;
  const static uint8_t OB_JSON_TRUE_ON_ERROR  = 1;
  const static uint8_t OB_JSON_ERROR_ON_ERROR = 2;
  const static uint8_t OB_JSON_DEFAULT_ON_ERROR = 3;
  const static uint8_t OB_JSON_FALSE_ON_EMPTY = 0;
  const static uint8_t OB_JSON_TRUE_ON_EMPTY  = 1;
  const static uint8_t OB_JSON_ERROR_ON_EMPTY = 2;
  const static uint8_t OB_JSON_DEFAULT_ON_EMPTY = 3;
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonExists);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_JSON_EXISTS_H