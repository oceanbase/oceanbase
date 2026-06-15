/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTR_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTR_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprInstr: public ObLocationExprOperator
{
public:
  ObExprInstr();
  explicit  ObExprInstr(common::ObIAllocator &alloc);
  virtual ~ObExprInstr();
  // 支持MySQL模式下INSTR函数的第3、4个参数
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *type_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const;
  static int calc_mysql_instr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int calc_mysql_instr_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  // 提取INSTR函数参数的辅助函数
  static int calc_mysql_instr_arg(const ObExpr &expr, ObEvalCtx &ctx,
                                  bool &is_null,
                                  common::ObDatum *&haystack,
                                  common::ObDatum *&needle,
                                  int64_t &pos_int,
                                  int64_t &occ_int,
                                  common::ObCollationType &calc_cs_type);

  template <typename HaystackVec, typename NeedleVec, typename ResVec>
  static int vector_mysql_instr(const ObExpr &expr,
                         ObEvalCtx &ctx,
                         const ObBitVector &skip,
                         const EvalBound &bound);
  DISALLOW_COPY_AND_ASSIGN(ObExprInstr);
};


class ObExprOracleInstr: public ObLocationExprOperator
{
public:
  ObExprOracleInstr();
  explicit  ObExprOracleInstr(common::ObIAllocator &alloc);
  virtual ~ObExprOracleInstr();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *type_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int slow_reverse_search(common::ObIAllocator &alloc,
                          const common::ObCollationType &cs_type,
                          const common::ObString &str1, const common::ObString &str2,
                          int64_t neg_start, int64_t occ, uint32_t &idx);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const;
  static int calc_oracle_instr_arg(const ObExpr &expr, ObEvalCtx &ctx,
                                   bool &is_null,
                                   common::ObDatum *&haystack, common::ObDatum *&needle,
                                   int64_t &pos_int, int64_t &occ_int,
                                   common::ObCollationType &calc_cs_type);
  static int calc_oracle_instr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
  static int calc_oracle_instr_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  static int calc(common::ObObj &result,
                  const common::ObObj &heystack,
                  const common::ObObj &needle,
                  const common::ObObj &position,
                  const common::ObObj &occurrence,
                  common::ObExprCtx &expr_ctx);
private:
  template <typename HaystackVec, typename NeedleVec, typename ResVec>
  static int vector_oracle_instr(const ObExpr &expr,
                                 ObEvalCtx &ctx,
                                 const ObBitVector &skip,
                                 const EvalBound &bound);

  template <typename HaystackVec, typename NeedleVec>
  static int calc_oracle_instr_vector_arg(const ObExpr &expr, ObEvalCtx &ctx,
                                          int64_t idx, bool &is_null,
                                          int64_t &pos_int, int64_t &occ_int,
                                          const ObIVector *pos = NULL, const ObIVector *occ = NULL);
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleInstr);
};

}//end of namespace sql
}//end of namespace oceanbase

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTR_H_ */
