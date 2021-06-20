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

#ifndef _OB_SQL_EXPR_GREATEST_H_
#define _OB_SQL_EXPR_GREATEST_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_least.h"

namespace oceanbase {
namespace sql {
class ObExprBaseGreatest : public ObExprBaseLeastGreatest {
public:
  explicit ObExprBaseGreatest(common::ObIAllocator& alloc, int32_t param_num,
      ObExprOperatorType type = T_FUN_SYS_GREATEST, const char* name = N_GREATEST);
  virtual ~ObExprBaseGreatest();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num,
      const ObExprResType& expected_type, common::ObExprCtx& expr_ctx);
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_greatest(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprBaseGreatest);
};

class ObExprGreatestMySQL : public ObExprBaseGreatest {
public:
  explicit ObExprGreatestMySQL(common::ObIAllocator& alloc);
  virtual ~ObExprGreatestMySQL();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprGreatestMySQL);
};

class ObExprGreatestMySQLInner : public ObExprBaseGreatest {
public:
  explicit ObExprGreatestMySQLInner(common::ObIAllocator& alloc);
  virtual ~ObExprGreatestMySQLInner();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprGreatestMySQLInner);
};

class ObExprOracleGreatest : public ObExprBaseGreatest {
public:
  explicit ObExprOracleGreatest(common::ObIAllocator& alloc);
  virtual ~ObExprOracleGreatest();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleGreatest);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_EXPR_GREATEST_H_ */
