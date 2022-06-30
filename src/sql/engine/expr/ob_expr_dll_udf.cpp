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

namespace oceanbase {
using namespace common;
namespace sql {

ObExprDllUdf::ObExprDllUdf(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_NORMAL_UDF, N_NORMAL_UDF, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION),
      allocator_(alloc),
      udf_func_(),
      udf_ctx_(),
      udf_meta_(),
      udf_attributes_(),
      udf_attributes_types_(),
      calculable_results_(),
      sql_expression_factory_(allocator_),
      expr_op_factory_(allocator_)
{}

ObExprDllUdf::ObExprDllUdf(ObIAllocator& alloc, ObExprOperatorType type, const char* name)
    : ObFuncExprOperator(alloc, type, name, PARAM_NUM_UNKNOWN, NOT_ROW_DIMENSION),
      allocator_(alloc),
      udf_func_(),
      udf_ctx_(),
      udf_meta_(),
      udf_attributes_(),
      udf_attributes_types_(),
      calculable_results_(),
      sql_expression_factory_(allocator_),
      expr_op_factory_(allocator_)
{}

ObExprDllUdf::~ObExprDllUdf()
{
  sql_expression_factory_.destroy();
  expr_op_factory_.destroy();
}

/*
 * ret of UDF can only be three types, including: STRING, DOUBLE and LONG LONG.
 * input of UDF can be any type.
 * such as select udf_sum(t1.c1) from t1
 *     select udf_sum(t2.c1) from t2
 * t1.c1 is varchar, t2.c1 is int
 * */
int ObExprDllUdf::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  /* the result type of udf is defined by user, no matter what the inputs' type or count. */
  UNUSED(param_num);
  UNUSED(types);
  UNUSED(type_ctx);
  if (OB_FAIL(ObUdfUtil::calc_udf_result_type(
          allocator_, &udf_func_, udf_meta_, udf_attributes_, udf_attributes_types_, type))) {
    LOG_WARN("failed to cale udf result type");
  }
  return ret;
}

int ObExprDllUdf::calc_resultN(ObObj& result, const ObObj* objs_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObUdfFunction::ObUdfCtx* udf_ctx = nullptr;
  ObNormalUdfExeUnit* udf_exec_unit = nullptr;
  ObUdfCtxMgr* udf_ctx_mgr = nullptr;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(expr_ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the calc buf is null", K(ret), K(expr_ctx.calc_buf_), K(expr_ctx.exec_ctx_), K(lbt()));
  } else if (OB_FAIL(expr_ctx.exec_ctx_->get_udf_ctx_mgr(udf_ctx_mgr))) {
    LOG_WARN("Failed to get udf ctx map", K(ret));
  } else if (OB_FAIL(udf_ctx_mgr->get_udf_ctx(get_id(), udf_exec_unit))) {
    if (ret == OB_HASH_NOT_EXIST) {
      if (OB_FAIL(udf_ctx_mgr->register_udf_expr(this, &udf_func_, udf_exec_unit))) {
        LOG_WARN("failed to register this op to udf ctx mgr", K(ret));
      } else if (OB_ISNULL(udf_exec_unit) || OB_ISNULL(udf_exec_unit->udf_ctx_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("the udf ctx is null", K(ret));
      } else if (FALSE_IT(udf_ctx = udf_exec_unit->udf_ctx_)) {
      } else if (OB_FAIL(ObUdfUtil::init_udf_args(
                     udf_ctx_mgr->get_allocator(), udf_attributes_, udf_attributes_types_, udf_ctx->udf_args_))) {
        LOG_WARN("failed to set udf args", K(ret));
      } else if (OB_FAIL(ObUdfUtil::init_const_args(
                     udf_ctx_mgr->get_allocator(), calculable_results_, udf_ctx->udf_args_, expr_ctx))) {
        LOG_WARN("failed to set udf args", K(ret));
      } else if (OB_FAIL(udf_func_.process_init_func(*udf_ctx))) {
        LOG_WARN("do agg init func failed", K(ret));
      }
    } else {
      LOG_WARN("get udf ctx failed", K(ret));
    }
  } else if (OB_ISNULL(udf_exec_unit) || OB_ISNULL(udf_exec_unit->udf_ctx_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("the udf ctx is null", K(ret));
  }
  /* process row */
  if (OB_SUCC(ret)) {
    udf_ctx = udf_exec_unit->udf_ctx_;
    if (OB_FAIL(udf_func_.process_origin_func(result, objs_stack, param_num, expr_ctx, *udf_ctx))) {
      LOG_WARN("failed to calc row", K(ret));
    }
  }
  return ret;
}

int ObExprDllUdf::set_udf_meta(const share::schema::ObUDFMeta& udf)
{
  int ret = OB_SUCCESS;
  udf_meta_.ret_ = udf.ret_;
  udf_meta_.type_ = udf.type_;
  if (OB_FAIL(ob_write_string(allocator_, udf.name_, udf_meta_.name_))) {
    LOG_WARN("fail to write string", K(udf.name_), K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, udf.dl_, udf_meta_.dl_))) {
    LOG_WARN("fail to write string", K(udf.name_), K(ret));
  } else {
  }
  LOG_DEBUG("set udf meta", K(udf_meta_), K(udf));
  return ret;
}

int ObExprDllUdf::init_udf(const common::ObIArray<ObRawExpr*>& param_exprs)
{
  int ret = OB_SUCCESS;
  // this function may be invoke many times
  udf_attributes_.reset();
  udf_attributes_types_.reset();
  calculable_results_.reset();
  ARRAY_FOREACH_X(param_exprs, idx, cnt, OB_SUCC(ret))
  {
    ObRawExpr* expr = param_exprs.at(idx);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the expr is null", K(ret));
    } else if (expr->is_column_ref_expr()) {
      // if the input expr is a column, we should set the column name as the expr name.
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
      const ObString& real_expr_name =
          col_expr->get_alias_column_name().empty() ? col_expr->get_column_name() : col_expr->get_alias_column_name();
      expr->set_expr_name(real_expr_name);
    } else if (expr->has_flag(IS_CALCULABLE_EXPR) || expr->has_flag(IS_CONST_EXPR) || expr->has_flag(IS_CONST)) {
      // generate the sql expression
      ObObj tmp_res;
      ObNewRow empty_row;
      RowDesc row_desc;
      ObSqlExpression* sql_expr = NULL;
      ObExprGeneratorImpl expr_generator(expr_op_factory_, 0, 0, NULL, row_desc);
      if (OB_FAIL(sql_expression_factory_.alloc(sql_expr))) {
        LOG_WARN("fail to alloc sql-expr", K(ret));
      } else if (OB_ISNULL(sql_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("alloc invalid expr", K(ret), K(sql_expr));
      } else if (OB_FAIL(expr_generator.generate(*expr, *sql_expr))) {
        LOG_WARN("generate sql expr failed", K(ret));
      } else if (OB_FAIL(add_const_expression(sql_expr, idx))) {
        LOG_WARN("failed to calculate", K(ret), "sql_expr", *sql_expr);
      }
    }
    if (OB_SUCC(ret)) {
      ObString str;
      if (OB_FAIL(ob_write_string(allocator_, expr->get_expr_name(), str))) {
        LOG_WARN("copy string failed", K(ret));
      } else if (OB_FAIL(udf_attributes_.push_back(str))) {
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

int ObExprDllUdf::add_const_expression(ObSqlExpression* sql_calc, int64_t idx_in_udf_arg)
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
    const ObUdfConstArgs& args = calculable_results_.at(j);
    const ObSqlExpression* expr = args.sql_calc_;
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
    ObSqlExpression* sql_expr = nullptr;
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
    const ObUdfConstArgs& args = calculable_results_.at(j);
    const ObSqlExpression* expr = args.sql_calc_;
    if (OB_ISNULL(expr)) {
      LOG_ERROR("udf normal expr is null");
    } else {
      len += expr->get_serialize_size();
    }
  }
  len += ObFuncExprOperator::get_serialize_size();
  return len;
}

}  // namespace sql
}  // namespace oceanbase
