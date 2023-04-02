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

#ifndef OCEANBASE_SQL_OB_PL_EXPR_SUBQUERY_H_
#define OCEANBASE_SQL_OB_PL_EXPR_SUBQUERY_H_

#include "common/object/ob_object.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_type.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace sql
{

struct ObExprPlSubQueryInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprPlSubQueryInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
      id_(common::OB_INVALID_ID),
      ps_sql_(ObString()),
      type_(stmt::T_NONE),
      route_sql_(ObString()),
      result_type_(),
      is_ignore_fail_(false) {}

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr, ObIAllocator &alloc);

  ObPsStmtId id_; //prepare的语句id, 保留id，兼容老版本
  common::ObString ps_sql_;
  stmt::StmtType type_; //prepare的语句类型

  common::ObString route_sql_;
  sql::ObExprResType result_type_;
  bool is_ignore_fail_;
};

class ObExprOpSubQueryInPl : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprOpSubQueryInPl(common::ObIAllocator &alloc);
  virtual ~ObExprOpSubQueryInPl();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual inline void reset() {
    route_sql_.reset();
    ObFuncExprOperator::reset();
  }

  virtual int assign(const ObExprOperator &other);

  int deep_copy_route_sql(const common::ObString &v)
  {
    return ob_write_string(allocator_, v, route_sql_);
  }
  int deep_copy_ps_sql(const common::ObString &v)
  {
    return ob_write_string(allocator_, v, ps_sql_);
  }

  inline void set_ps_sql(common::ObString sql) { ps_sql_ = sql; }
  inline void set_stmt_type(stmt::StmtType type) { type_ = type; }
  inline void set_route_sql(common::ObString sql) { route_sql_ = sql; }
  inline void set_result_type(ObExprResType type) { result_type_ = type; }
  inline void set_ignore_fail() { is_ignore_fail_ = true; }

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;

  static int eval_subquery(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

private:
  static int fill_obj_stack(const ObExpr &expr, ObEvalCtx &ctx, common::ObObj *objs);
  static int fill_param_store(const ObObj *objs_stack, int64_t param_num, ParamStore& params);
  static int fetch_row(void *result_ret, int64_t &row_count, ObNewRow &cur_row);
  static int get_result(void *result_set, ObObj &result, ObIAllocator &alloc);

  ObPsStmtId id_; //prepare的语句id
  common::ObString ps_sql_;
  stmt::StmtType type_; //prepare的语句类型

  common::ObString route_sql_;
  ObExprResType result_type_;
  bool is_ignore_fail_;
  common::ObIAllocator &allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOpSubQueryInPl);
};

} //sql
} //oceanbase
#endif //OCEANBASE_SQL_OB_PL_EXPR_SUBQUERY_H_
