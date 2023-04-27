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

#define USING_LOG_PREFIX  SQL_ENG

#include "sql/engine/expr/ob_expr_assign.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprAssign::ObExprAssign(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_ASSIGN, N_ASSIGN, 2, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprAssign::~ObExprAssign()
{
}

int ObExprAssign::calc_result_type2(ObExprResType &type,
                                            ObExprResType &key,
                                            ObExprResType &value,
                                            common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  UNUSED(key);
  ObObjType val_type = value.get_type();
  if (ob_is_temporal_type(val_type)){
    type.set_varchar();
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(common::ObCharset::get_default_collation(common::ObCharset::get_default_charset()));
  } else if (ob_is_bit_tc(val_type)) {
    type.set_uint64();
    value.set_calc_type(ObUInt64Type);
  } else if (OB_UNLIKELY(value.is_null())) {
    type.set_varchar();
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(common::CS_TYPE_BINARY);
  } else {
    type.set_type(val_type);
    type.set_collation_level(common::CS_LEVEL_IMPLICIT);
    type.set_collation_type(value.get_collation_type());
  }
  type.set_precision(value.get_precision());
  type.set_scale(value.get_scale());
  //set length
  if (ob_is_string_type(type.get_type())) {
   type.set_full_length(value.get_length(), value.get_length_semantics());
  }

  value.set_calc_meta(type.get_obj_meta());
  value.set_calc_accuracy(type.get_accuracy());
  value.set_calc_collation_type(type.get_collation_type());
  value.set_calc_collation_level(type.get_collation_level());
  key.set_calc_type(ObVarcharType);
  key.set_calc_collation_type(ObCharset::get_system_collation());

  return ret;
}

int calc_assign_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *key_res = NULL;
  ObDatum *val_res = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, key_res)) ||
             OB_FAIL(expr.args_[1]->eval(ctx, val_res))) {
    LOG_WARN("eval arg failed", K(ret));
  } else {
    ObObj obj;
    ObObjMeta obj_meta;
    ObSessionVariable sess_var;
    if (val_res->is_null()) {
      obj.set_null();
      obj_meta.set_null();
      // same as old engine. but why...
      obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
      obj_meta.set_collation_type(ObCharset::get_default_collation(
                                  ObCharset::get_default_charset()));
    } else {
      obj_meta.set_type(expr.datum_meta_.type_);
      obj_meta.set_scale(expr.datum_meta_.scale_);
      obj_meta.set_collation_type(expr.datum_meta_.cs_type_);
      obj_meta.set_collation_level(common::CS_LEVEL_IMPLICIT);
      if (expr.args_[1]->obj_meta_.has_lob_header()) {
         // use temp meta to session var，ensure flag in expr.args_[1](val_res) copy to dest
        obj_meta.set_has_lob_header();
      }
      if (OB_FAIL(val_res->to_obj(obj, obj_meta))) {
        LOG_WARN("to_obj failed", K(ret), K(expr), K(obj_meta));
      }
    }
    if (OB_SUCC(ret)) {
      sess_var.value_ = obj;
      sess_var.meta_ = obj_meta;
    }
    if (OB_SUCC(ret) && OB_FAIL(session->replace_user_variable(
            key_res->get_string(), sess_var))) {
      LOG_WARN("replace user val failed", K(ret), K(key_res->get_string()));
    } else {
      res_datum.set_datum(*val_res);
    }
  }
  return ret;
}

int ObExprAssign::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                          ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = calc_assign_expr;
  return ret;
}

} //namespace sql
} //namespace oceanbase
