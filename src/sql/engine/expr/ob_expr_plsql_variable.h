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

#ifndef SRC_SQL_ENGINE_EXPR_OB_EXPR_PLSQL_VARIABLE_H_
#define SRC_SQL_ENGINE_EXPR_OB_EXPR_PLSQL_VARIABLE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace sql
{

struct ObPLSQLVariableInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPLSQLVariableInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr, common::ObIAllocator &alloc);
  int64_t plsql_line_;
  common::ObString plsql_variable_;
  ObExprResType result_type_;
};
class ObExprPLSQLVariable : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprPLSQLVariable(common::ObIAllocator &alloc);
  virtual ~ObExprPLSQLVariable() {}

  virtual int calc_result_type0(
    ObExprResType &type, common::ObExprTypeCtx &type_ctx) const;

  virtual int assign(const ObExprOperator &other);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_plsql_variable(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);

  void set_plsql_line(int64_t v) { plsql_line_ = v; }
  int64_t get_plsql_line() { return plsql_line_; }

  int deep_copy_plsql_variable(const common::ObString &v)
  {
    return ob_write_string(allocator_, v, plsql_variable_);
  }
  common::ObString& get_plsql_variable() { return plsql_variable_; }

  static int check_plsql_ccflags(
    const common::ObString &v,
    common::ObIArray<std::pair<common::ObString, common::ObObj> > *result = NULL);

  static int add_to_array(
    const common::ObString &key,
    common::ObObj &val,
    common::ObIArray<std::pair<common::ObString, common::ObObj> > &result);
  static int check_value(
    const common::ObString &val, common::ObObj &val_obj);
  static int check_key(const common::ObString &key);

private:
  static int get_plsql_unit(common::ObObj &result,
                            common::ObIAllocator &alloc,
                            ObSQLSessionInfo &session,
                            const ObExprResType &result_type,
                            const ObString &plsql_variable);
  static int get_key_value(
    const common::ObString &plsql_ccflags,
    const common::ObString &key, common::ObObj &value);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprPLSQLVariable);

private:
  int64_t plsql_line_;
  common::ObString plsql_variable_;
  common::ObIAllocator &allocator_;
};

}
}

#endif /* SRC_SQL_ENGINE_EXPR_OB_EXPR_PLSQL_VARIABLE_H_ */
