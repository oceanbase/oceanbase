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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_HASH_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_HASH_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObTaskExecutorCtx;
class ObExprFuncPartHashBase : public ObFuncExprOperator
{
public:
  ObExprFuncPartHashBase(common::ObIAllocator &alloc, ObExprOperatorType type,
          const char *name, int32_t param_num, int32_t dimension,
          bool is_internal_for_mysql = false,
          bool is_internal_for_oracle = false);
  template<typename T>
  static int calc_value_for_mysql(const T &input, T &output, const common::ObObjType input_type);
};

class ObTaskExecutorCtx;
class ObExprFuncPartHash : public ObExprFuncPartHashBase
{
public:
  explicit  ObExprFuncPartHash(common::ObIAllocator &alloc);
  virtual ~ObExprFuncPartHash();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc_value(common::ObExprCtx &expr_ctx,
                        const common::ObObj *objs_stack,
                        int64_t param_num,
                        common::ObObj &result);
  static int calc_hash_value_with_seed(const common::ObObj &obj,
                                        int64_t seed,
                                        uint64_t &res);
  static int calc_value_for_oracle(const common::ObObj *objs_stack,
                                   int64_t param_num,
                                   common::ObObj &result);
  static bool is_virtual_part_for_oracle(const ObTaskExecutorCtx *task_ec);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_part_hash(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_oracle_part_hash(const ObExpr &expr, ObEvalCtx &ctx,
                                   ObDatum &expr_datum, uint64_t seed);
private:
  static bool is_oracle_supported_type(const common::ObObjType type);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncPartHash);

};

}  // namespace sql
}  // namespace oceanbase
#endif //OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_HASH_
