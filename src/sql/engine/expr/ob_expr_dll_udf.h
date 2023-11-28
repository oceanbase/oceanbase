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
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlExpression;

class ObExprDllUdf : public ObFuncExprOperator
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObExprDllUdf(ObIAllocator &alloc);
  ObExprDllUdf(ObIAllocator &alloc, ObExprOperatorType type, const char *name);
  virtual ~ObExprDllUdf();
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
public:
  int set_udf_meta(const share::schema::ObUDFMeta &udf);
  static int deep_copy_udf_meta(share::schema::ObUDFMeta &dst,
                                common::ObIAllocator &alloc,
                                const share::schema::ObUDFMeta &src);
  int init_udf(const common::ObIArray<ObRawExpr*> &para_exprs);
  common::ObIArray<common::ObString> &get_udf_attribute() { return udf_attributes_; };
  common::ObIArray<ObExprResType> &get_udf_attributes_types() { return udf_attributes_types_; };
  ObNormalUdfFunction &get_udf_func() { return udf_func_; };
  int add_const_expression(ObSqlExpression *sql_calc, int64_t idx_in_udf_arg);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }

  static int eval_dll_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
protected :
  common::ObIAllocator &allocator_;
  ObNormalUdfFunction udf_func_;
  share::schema::ObUDFMeta udf_meta_;
  common::ObSEArray<common::ObString, 16> udf_attributes_; /* udf's input args' name */
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

template <typename UF>
struct ObDllUdfInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDllUdfInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type), allocator_(alloc),
      udf_attributes_(alloc), udf_attributes_types_(alloc), args_const_attr_(alloc)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

  template <typename RE>
  int from_raw_expr(RE &expr);

  common::ObIAllocator &allocator_;
  UF udf_func_;
  share::schema::ObUDFMeta udf_meta_;
  // udf's input args' name
  common::ObFixedArray<common::ObString, common::ObIAllocator> udf_attributes_;
  // udf's attribute type
  common::ObFixedArray<ObExprResType, common::ObIAllocator> udf_attributes_types_;
  // indicate the argument is const expr or not
  common::ObFixedArray<bool, common::ObIAllocator> args_const_attr_;
  TO_STRING_KV(K(udf_meta_), K(udf_attributes_), K(udf_attributes_types_), K(args_const_attr_));
};

OB_DEF_SERIALIZE(ObDllUdfInfo<UF>, template<typename UF>)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              udf_meta_,
              udf_attributes_,
              udf_attributes_types_,
              args_const_attr_);
  return ret;
}

OB_DEF_DESERIALIZE(ObDllUdfInfo<UF>, template<typename UF>)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              udf_meta_,
              udf_attributes_,
              udf_attributes_types_,
              args_const_attr_);
  OZ(udf_func_.init(udf_meta_));
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDllUdfInfo<UF>, template<typename UF>)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              udf_meta_,
              udf_attributes_,
              udf_attributes_types_,
              args_const_attr_);
  return len;
}

template <typename UF>
int ObDllUdfInfo<UF>::deep_copy(common::ObIAllocator &allocator,
                                const ObExprOperatorType type,
                                ObIExprExtraInfo *&copied_info) const
{
  int ret = common::OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ObDllUdfInfo &other = *static_cast<ObDllUdfInfo *>(copied_info);
  OZ(ObExprDllUdf::deep_copy_udf_meta(other.udf_meta_, allocator, udf_meta_));

  OZ(other.udf_attributes_.assign(udf_attributes_));
  if (OB_SUCC(ret)) {
    FOREACH_CNT_X(it, other.udf_attributes_, OB_SUCC(ret)) {
      ObString s;
      OZ(ob_write_string(allocator, *it, s));
      if (OB_SUCC(ret)) {
        *it = s;
      }
    }
  }
  OZ(other.udf_attributes_types_.assign(udf_attributes_types_));
  OZ(other.args_const_attr_.assign(args_const_attr_));

  OZ(other.udf_func_.init(other.udf_meta_));

  return ret;
}

template <typename UF>
template <typename RE>
int ObDllUdfInfo<UF>::from_raw_expr(RE &raw_expr)
{
  int ret = OB_SUCCESS;
  OZ(ObExprDllUdf::deep_copy_udf_meta(udf_meta_, allocator_, raw_expr.get_udf_meta()));

  OZ(udf_attributes_.init(raw_expr.get_param_count()));
  OZ(udf_attributes_types_.init(raw_expr.get_param_count()));
  OZ(args_const_attr_.init(raw_expr.get_param_count()));

  // init_udf()
  for (int64_t i = 0; OB_SUCC(ret) && i < raw_expr.get_param_count(); i++) {

    const ObRawExpr *e = raw_expr.get_param_expr(i);
    CK(NULL != e);

    OZ(args_const_attr_.push_back(e->is_const_expr()));
    if (OB_SUCC(ret)) {
      ObString name = e->get_expr_name();
      if (e->is_column_ref_expr()) {
        //if the input expr is a column, we should set the column name as the expr name.
        const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(e);
        name = col_expr->get_alias_column_name().empty()
            ? col_expr->get_column_name()
            : col_expr->get_alias_column_name();
      }
      ObString deep_copy_name;
      OZ(ob_write_string(allocator_, name, deep_copy_name));
      OZ(udf_attributes_.push_back(deep_copy_name));
    }
    OZ(udf_attributes_types_.push_back(e->get_result_type()));
  }
  OZ(udf_func_.init(udf_meta_));
  return ret;
}

typedef ObDllUdfInfo<ObNormalUdfFunction> ObNormalDllUdfInfo;
typedef ObDllUdfInfo<ObAggUdfFunction> ObAggDllUdfInfo;


}
}

#endif
