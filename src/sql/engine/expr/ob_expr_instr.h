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
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const;
  static int calc_mysql_instr_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
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
  static int calc(common::ObObj &result,
                  const common::ObObj &heystack,
                  const common::ObObj &needle,
                  const common::ObObj &position,
                  const common::ObObj &occurrence,
                  common::ObExprCtx &expr_ctx);
private:

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleInstr);
};

}//end of namespace sql
}//end of namespace oceanbase

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_INSTR_H_ */
