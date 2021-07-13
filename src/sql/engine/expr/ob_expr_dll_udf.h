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

#ifndef OB_EXPR_UDF_H_
#define OB_EXPR_UDF_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_sql_expression_factory.h"
#include "sql/engine/expr/ob_expr_operator_factory.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"
#include "sql/engine/user_defined_function/ob_user_defined_function.h"
#include "sql/code_generator/ob_expr_generator_impl.h"

namespace oceanbase {
namespace sql {
class ObRawExpr;
class ObSqlExpression;

class ObExprDllUdf : public ObFuncExprOperator {
  OB_UNIS_VERSION_V(1);

public:
  explicit ObExprDllUdf(ObIAllocator& alloc);
  ObExprDllUdf(ObIAllocator& alloc, ObExprOperatorType type, const char* name);
  virtual ~ObExprDllUdf();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;

  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;

public:
  int set_udf_meta(const share::schema::ObUDFMeta& udf);
  int init_udf(const common::ObIArray<ObRawExpr*>& para_exprs);
  common::ObIArray<common::ObString>& get_udf_attribute()
  {
    return udf_attributes_;
  };
  common::ObIArray<ObExprResType>& get_udf_attributes_types()
  {
    return udf_attributes_types_;
  };
  ObNormalUdfFunction& get_udf_func()
  {
    return udf_func_;
  };
  int add_const_expression(ObSqlExpression* sql_calc, int64_t idx_in_udf_arg);

protected:
  common::ObIAllocator& allocator_;
  ObNormalUdfFunction udf_func_;
  ObUdfFunction::ObUdfCtx udf_ctx_;
  share::schema::ObUDFMeta udf_meta_;
  common::ObSEArray<common::ObString, 16> udf_attributes_;    /* udf's input args' name */
  common::ObSEArray<ObExprResType, 16> udf_attributes_types_; /* udf's attribute type */
  /*
   * mysql require the calculable expression got result before we invoke the xxx_init() function.
   * it seems no other choice.
   * just like table location, the udf expr must have the capable of calculating the inner expression.
   * so we set a ObSqlExpressionFactory as his data member.
   * the overhead seems unavoidable. all our calculation tight coupling with SQL execution.
   * as we can see, all the behaviors of mysql shows that it don't split the expression operator into
   * logical and physical stage.
   * */
  common::ObSEArray<ObUdfConstArgs, 16> calculable_results_; /* calculable input expr' param idx */
  ObSqlExpressionFactory sql_expression_factory_;
  ObExprOperatorFactory expr_op_factory_;
};

}  // namespace sql
}  // namespace oceanbase

#endif
