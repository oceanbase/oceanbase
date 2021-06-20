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

namespace oceanbase {
namespace sql {
class ObTaskExecutorCtx;
class ObExprFuncPartOldHash : public ObFuncExprOperator {
public:
  explicit ObExprFuncPartOldHash(common::ObIAllocator& alloc);
  virtual ~ObExprFuncPartOldHash();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  static int calc_value(
      common::ObExprCtx& expr_ctx, const common::ObObj* objs_stack, int64_t param_num, common::ObObj& result);
  static uint64_t calc_hash_value_with_seed(const common::ObObj& obj, int64_t seed);
  static int calc_value_for_oracle(const common::ObObj* objs_stack, int64_t param_num, common::ObObj& result);
  static int calc_value_for_mysql(const common::ObObj& obj1, common::ObObj& result);
  static bool is_virtual_part_for_oracle(const ObTaskExecutorCtx* task_ec);
  static int calc_oracle_vt_part_id(ObTaskExecutorCtx& task_exec_ctx, uint64_t table_id,
      const common::ObObj* objs_stack, int64_t param_num, common::ObObj& result);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int eval_old_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int eval_oracle_old_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, uint64_t seed);

private:
  static int eval_vt_old_part_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum,
      ObTaskExecutorCtx& task_exec_ctx, const uint64_t table_id);
  static bool is_oracle_supported_type(const common::ObObjType type);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncPartOldHash);
};

///////////////////////////////////////////////////////////////////////////////
// call new hash functions, solve partition row skew problem
class ObTaskExecutorCtx;
class ObExprFuncPartHash : public ObFuncExprOperator {
public:
  explicit ObExprFuncPartHash(common::ObIAllocator& alloc);
  virtual ~ObExprFuncPartHash();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  static int calc_value(
      common::ObExprCtx& expr_ctx, const common::ObObj* objs_stack, int64_t param_num, common::ObObj& result);
  static uint64_t calc_hash_value_with_seed(const common::ObObj& obj, int64_t seed);
  static int calc_value_for_oracle(const common::ObObj* objs_stack, int64_t param_num, common::ObObj& result);
  static int calc_value_for_mysql(const common::ObObj& obj1, common::ObObj& result);
  static bool is_virtual_part_for_oracle(const ObTaskExecutorCtx* task_ec);
  static int calc_oracle_vt_part_id(ObTaskExecutorCtx& task_exec_ctx, uint64_t table_id,
      const common::ObObj* objs_stack, int64_t param_num, common::ObObj& result);

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int eval_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int eval_oracle_part_hash(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, uint64_t seed);

private:
  static int eval_vt_part_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum, ObTaskExecutorCtx& task_exec_ctx,
      const uint64_t table_id);
  static bool is_oracle_supported_type(const common::ObObjType type);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprFuncPartHash);
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FUNC_HASH_
