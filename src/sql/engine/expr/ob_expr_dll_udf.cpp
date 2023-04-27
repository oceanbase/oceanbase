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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/user_defined_function/ob_user_defined_function.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprDllUdf::ObExprDllUdf(ObIAllocator &alloc) :
    ObFuncExprOperator(alloc, T_FUN_NORMAL_UDF, N_NORMAL_UDF, PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, INTERNAL_IN_MYSQL_MODE),
    allocator_(alloc),
    udf_func_(),
    udf_meta_(),
    udf_attributes_(),
    udf_attributes_types_(),
    calculable_results_(),
    sql_expression_factory_(allocator_),
    expr_op_factory_(allocator_)
{
}

ObExprDllUdf::ObExprDllUdf(ObIAllocator &alloc, ObExprOperatorType type, const char *name) :
    ObFuncExprOperator(alloc, type, name, PARAM_NUM_UNKNOWN, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
    allocator_(alloc),
    udf_func_(),
    udf_meta_(),
    udf_attributes_(),
    udf_attributes_types_(),
    calculable_results_(),
    sql_expression_factory_(allocator_),
    expr_op_factory_(allocator_)
{
}

ObExprDllUdf::~ObExprDllUdf()
{
  sql_expression_factory_.destroy();
  expr_op_factory_.destroy();
}

/*
 * UDF的结果是强制性的，UDF的ret仅仅只有三种类型，分别是STRING，DOUBLE，LONG LONG。
 * 这里不存类型的推导。
 * UDF的输入则是可以是任何类型，
 * 比如select udf_sum(t1.c1) from t1
 *     select udf_sum(t2.c1) from t2
 * t1.c1是varchar，t2.c1是int，只要udf_sum的执行函数里面写清楚执行逻辑即可。
 * */
int ObExprDllUdf::calc_result_typeN(ObExprResType &type,
                                    ObExprResType *types,
                                    int64_t param_num,
                                    ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  /* the result type of udf is defined by user, no matter what the inputs' type or count. */
  UNUSED(param_num);
  UNUSED(types);
  if (OB_FAIL(ObUdfUtil::calc_udf_result_type(
              allocator_, &udf_func_, udf_meta_,
              udf_attributes_, udf_attributes_types_,
              type, types, param_num, type_ctx))) {
    LOG_WARN("failed to cale udf result type");
  }
  if (OB_SUCC(ret)) {
    type_ctx.set_cast_mode(type_ctx.get_cast_mode() | CM_WARN_ON_FAIL);
  }
  return ret;
}

int ObExprDllUdf::deep_copy_udf_meta(share::schema::ObUDFMeta &dst,
                                     common::ObIAllocator &alloc,
                                     const share::schema::ObUDFMeta &src)
{
  int ret = OB_SUCCESS;
  dst.ret_ = src.ret_;
  dst.type_ = src.type_;
  if (OB_FAIL(ob_write_string(alloc, src.name_, dst.name_))) {
    LOG_WARN("fail to write string", K(src.name_), K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, src.dl_, dst.dl_))) {
    LOG_WARN("fail to write string", K(src.name_), K(ret));
  } else { }
  LOG_DEBUG("set udf meta", K(src), K(dst));
  return ret;
}

int ObExprDllUdf::set_udf_meta(const share::schema::ObUDFMeta &udf)
{
  return deep_copy_udf_meta(udf_meta_, allocator_, udf);
}

int ObExprDllUdf::init_udf(const common::ObIArray<ObRawExpr*> &param_exprs)
{
  int ret = OB_SUCCESS;
  // this function may be invoke many times
  udf_attributes_.reset();
  udf_attributes_types_.reset();
  calculable_results_.reset();
  ARRAY_FOREACH_X(param_exprs, idx, cnt, OB_SUCC(ret)) {
    ObRawExpr *expr = param_exprs.at(idx);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the expr is null", K(ret));
    } else if (expr->is_column_ref_expr()) {
      //if the input expr is a column, we should set the column name as the expr name.
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
      const ObString &real_expr_name = col_expr->get_alias_column_name().empty() ? col_expr->get_column_name() : col_expr->get_alias_column_name();
      expr->set_expr_name(real_expr_name);
    } else if (expr->is_const_expr()) {
      // TODO shengle use static engine
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("static engine not support", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "static engine for const udf expr");
      // generate the sql expression
     // ObObj tmp_res;
     // ObNewRow empty_row;
     // RowDesc row_desc; //空的行描述符，可计算的表达式，不需要行描述符
     // ObSqlExpression *sql_expr = NULL;
     // ObExprGeneratorImpl expr_generator(expr_op_factory_, 0, 0, NULL, row_desc);
     // if (OB_FAIL(sql_expression_factory_.alloc(sql_expr))) {
     //   LOG_WARN("fail to alloc sql-expr", K(ret));
     // } else if (OB_ISNULL(sql_expr)) {
     //   ret = OB_ERR_UNEXPECTED;
     //   LOG_ERROR("alloc invalid expr", K(ret), K(sql_expr));
     // } else if (OB_FAIL(expr_generator.generate(*expr, *sql_expr))) {
     //   LOG_WARN("generate sql expr failed", K(ret));
     // } else if (OB_FAIL(add_const_expression(sql_expr, idx))) {
     //   LOG_WARN("failed to calculate", K(ret), "sql_expr", *sql_expr);
     // }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(udf_attributes_.push_back(expr->get_expr_name()))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(udf_attributes_types_.push_back(expr->get_result_type()))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(udf_func_.init(udf_meta_))) {
      LOG_WARN("udf function init failed", K(ret));
    }
  }
  return ret;
}

int ObExprDllUdf::add_const_expression(ObSqlExpression *sql_calc, int64_t idx_in_udf_arg)
{
  int ret = OB_SUCCESS;
  ObUdfConstArgs const_args;
  const_args.sql_calc_ = sql_calc;
  const_args.idx_in_udf_arg_ = idx_in_udf_arg;
  if (OB_FAIL(calculable_results_.push_back(const_args))) {
    LOG_WARN("push back const args failed", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprDllUdf)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(udf_meta_);
  OB_UNIS_ENCODE(udf_attributes_);
  OB_UNIS_ENCODE(udf_attributes_types_);
  OB_UNIS_ENCODE(calculable_results_);
  for (int64_t j = 0; j < calculable_results_.count() && OB_SUCC(ret); ++j) {
    const ObUdfConstArgs &args = calculable_results_.at(j);
    const ObSqlExpression *expr = args.sql_calc_;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null");
    } else if (OB_FAIL(expr->serialize(buf, buf_len, pos))) {
      LOG_WARN("serialize aggregate column expression failed", K(ret));
    }
  }
  OZ(ObFuncExprOperator::serialize(buf, buf_len, pos));
  return ret;
}

OB_DEF_DESERIALIZE(ObExprDllUdf)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(udf_meta_);
  OB_UNIS_DECODE(udf_attributes_);
  OB_UNIS_DECODE(udf_attributes_types_);
  OB_UNIS_DECODE(calculable_results_);
  for (int64_t j = 0; j < calculable_results_.count() && OB_SUCC(ret); ++j) {
    ObSqlExpression *sql_expr = nullptr;
    if (OB_FAIL(sql_expression_factory_.alloc(sql_expr))) {
      LOG_WARN("fail to alloc sql-expr", K(ret));
    } else if (OB_ISNULL(sql_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("alloc invalid expr", K(ret), K(sql_expr));
    } else if (OB_FAIL(sql_expr->deserialize(buf, data_len, pos))) {
      LOG_WARN("fail to deserialize expression", K(ret));
    }
  }
  OZ(ObFuncExprOperator::deserialize(buf, data_len, pos));
  OZ(udf_func_.init(udf_meta_));
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprDllUdf)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(udf_meta_);
  OB_UNIS_ADD_LEN(udf_attributes_);
  OB_UNIS_ADD_LEN(udf_attributes_types_);
  OB_UNIS_ADD_LEN(calculable_results_);
  for (int64_t j = 0; j < calculable_results_.count(); ++j) {
    const ObUdfConstArgs &args = calculable_results_.at(j);
    const ObSqlExpression *expr = args.sql_calc_;
    if (OB_ISNULL(expr)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "udf normal expr is null");
    } else {
      len += expr->get_serialize_size();
    }
  }
  len += ObFuncExprOperator::get_serialize_size();
  return len;
}

int ObExprDllUdf::cg_expr(ObExprCGCtx &expr_cg_ctx,
                          const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIAllocator &alloc = *expr_cg_ctx.allocator_;
  const ObNormalDllUdfRawExpr &fun_sys = static_cast<const ObNormalDllUdfRawExpr &>(raw_expr);

  ObNormalDllUdfInfo *info = OB_NEWx(ObNormalDllUdfInfo, (&alloc),
                                   alloc, T_FUN_NORMAL_UDF);
  if (NULL == info) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    OZ(info->from_raw_expr(fun_sys));
    rt_expr.extra_info_ = info;
  }

  rt_expr.eval_func_ = eval_dll_udf;
  return ret;
}

class ObExprDllUdfCtx : public ObExprOperatorCtx
{
public:
  ObExprDllUdfCtx() : udf_func_(NULL) {}
  virtual ~ObExprDllUdfCtx()
  {
    if (NULL != udf_func_) {
      IGNORE_RETURN udf_func_->process_deinit_func(udf_ctx_);
    }
  }


  const ObUdfFunction *udf_func_;
  ObUdfFunction::ObUdfCtx udf_ctx_;
};


int ObExprDllUdf::eval_dll_udf(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  const ObNormalDllUdfInfo *info = static_cast<ObNormalDllUdfInfo *>(expr.extra_info_);
  ObExprDllUdfCtx *expr_udf_ctx = static_cast<ObExprDllUdfCtx *>(
      ctx.exec_ctx_.get_expr_op_ctx(expr.expr_ctx_id_));
  CK(NULL != info);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("evaluate parameters' value failed", K(ret));
  } else if (OB_ISNULL(expr_udf_ctx)) {
    if (OB_FAIL(ctx.exec_ctx_.create_expr_op_ctx(expr.expr_ctx_id_, expr_udf_ctx))) {
      LOG_WARN("create expr op ctx failed", K(ret));
    } else if (OB_ISNULL(expr_udf_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr udf ctx is NULL", K(ret));
    } else {
      ObUdfFunction::ObUdfCtx &udf_ctx = expr_udf_ctx->udf_ctx_;
      OZ(ObUdfUtil::init_udf_args(ctx.exec_ctx_.get_allocator(),
                                  info->udf_attributes_,
                                  info->udf_attributes_types_,
                                  udf_ctx.udf_args_));
      CK(info->args_const_attr_.count() == expr.arg_cnt_);
      // set const argument values
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
        if (info->args_const_attr_.at(i)) {
          OZ(ObUdfUtil::set_udf_arg(ctx.exec_ctx_.get_allocator(),
                                    expr.args_[i]->locate_expr_datum(ctx),
                                    *expr.args_[i],
                                    udf_ctx.udf_args_,
                                    i));
        }
      }
      if (OB_SUCC(ret)) {
        expr_udf_ctx->udf_func_ = &info->udf_func_;
        OZ(info->udf_func_.process_init_func(udf_ctx));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObUdfFunction::ObUdfCtx &udf_ctx = expr_udf_ctx->udf_ctx_;
    for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
      OZ(ObUdfUtil::set_udf_arg(tmp_alloc_g.get_allocator(),
                                expr.args_[i]->locate_expr_datum(ctx),
                                *expr.args_[i],
                                udf_ctx.udf_args_,
                                i));
    }
    ObObj obj_res;
    OZ(ObUdfUtil::process_udf_func(info->udf_meta_.ret_,
                                   tmp_alloc_g.get_allocator(),
                                   udf_ctx.udf_init_,
                                   udf_ctx.udf_args_,
                                   info->udf_func_.func_origin_,
                                   obj_res));
    if (OB_SUCC(ret)) {
      OZ(res.from_obj(obj_res));
      OZ(expr.deep_copy_datum(ctx, res));
    }
  }
  return ret;
}

}
}


