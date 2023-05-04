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

#include "sql/engine/expr/ob_expr_get_user_var.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "lib/ob_name_def.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprGetUserVar::ObExprGetUserVar(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_OP_GET_USER_VAR, N_GET_USER_VAR, 1, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                         INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprGetUserVar::~ObExprGetUserVar()
{
}

int ObExprGetUserVar::calc_result_type1(ObExprResType &type,
                                        ObExprResType &type1,
                                        ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  const ObObj &obj = type1.get_param();
  ObString key = obj.get_varchar();//should NOT use const reference here !
  ObSessionVariable session_var;
  CK( OB_NOT_NULL(type_ctx.get_session()) );
  OC( (type_ctx.get_session()->get_user_variable)(key, session_var) );
  if (OB_SUCC(ret)) {
    if (session_var.value_.is_null()) {
      type.set_varchar();//blob in mysql. use varchar instead in ob.
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_collation_type(CS_TYPE_BINARY);
    } else {
      type.set_type(session_var.meta_.get_type());
      type.set_scale(session_var.meta_.get_scale());
      type.set_collation_level(session_var.meta_.get_collation_level());
      type.set_collation_type(session_var.meta_.get_collation_type());
    }
  } else if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
    type.set_varchar();//blob in mysql. use varchar instead in ob.
    type.set_collation_level(CS_LEVEL_IMPLICIT);
    type.set_collation_type(CS_TYPE_BINARY);
    ret = OB_SUCCESS;//always return success no matter found or not
  } else {
    LOG_WARN("Unexpected ret code", K(ret), K(key), K(session_var));
  }
  if (OB_SUCC(ret) && ob_is_string_type(type.get_type())) {
    //set length
    int64_t mbmaxlen = 0;
    if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(type.get_collation_type(), mbmaxlen))) {
      SQL_RESV_LOG(WARN, "fail to get mbmaxlen", K(ret), K(type.get_collation_type()));
    } else {
      type.set_length(static_cast<ObLength>(OB_MAX_VARCHAR_LENGTH / mbmaxlen));
      type.set_length_semantics(type_ctx.get_session()->get_actual_nls_length_semantics());
    }
  }
  return ret;
}

int ObExprGetUserVar::eval_get_user_var(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *key = NULL;
  if (OB_FAIL(expr.eval_param_value(ctx, key))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (key->is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user var name is NULL", K(ret));
  } else {
    ObSQLSessionInfo *my_session = ctx.exec_ctx_.get_my_session();
    CK(OB_NOT_NULL(my_session));
    if (OB_SUCC(ret)) {
      const ObString &key_str = key->get_string();
      ObSessionVariable osv;
      ret = my_session->get_user_variable(key_str, osv);
      if (OB_SUCC(ret)) {
        ObObj sess_obj = osv.value_;
        sess_obj.set_meta_type(osv.meta_);
        if (expr.datum_meta_.type_ != sess_obj.get_type() ||
            (sess_obj.is_string_type() &&
             expr.datum_meta_.cs_type_ != sess_obj.get_collation_type())) {
          // eg: set @a = 10, @b = 1;
          //     select @a := '10', @b := '1', @a < @b;
          // expr.datum_meta_ is type of the origin var(int),
          // osv.value_ may be updated(string)
          ObObj res_obj;
          ObEvalCtx::TempAllocGuard alloc_guard(ctx);
          ObIAllocator &calc_alloc = alloc_guard.get_allocator();
          //TODO @peihan.dph
          //incompatible with mysql, return errcode controled by cast_mode
          //pl.sp-vars_mysql
          OZ(ObDatumCast::cast_obj(ctx, calc_alloc, expr.datum_meta_.type_,
                                  expr.datum_meta_.cs_type_, sess_obj, res_obj));
          OZ(res.from_obj(res_obj));
          if (is_lob_storage(res_obj.get_type())) {
            OZ(ob_adjust_lob_datum(res_obj, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), res));
          }
        } else {
          OZ(res.from_obj(sess_obj));
          if (is_lob_storage(sess_obj.get_type())) {
            OZ(ob_adjust_lob_datum(sess_obj, expr.obj_meta_, ctx.exec_ctx_.get_allocator(), res));
          }
        }
        // res.ptr_ may allocated by temporary allocator, need deep copy.
        OZ(expr.deep_copy_datum(ctx, res));
      } else if (OB_ERR_USER_VARIABLE_UNKNOWN == ret) {
        res.set_null();
        ret = OB_SUCCESS;//always return success no matter found or not
      } else {
        LOG_WARN("Unexpected ret code", K(ret), K(key_str), K(osv));
      }
    }
  }
  return ret;
}

int ObExprGetUserVar::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  CK(1 == expr.arg_cnt_);
  CK(ObStringTC == ob_obj_type_class(expr.args_[0]->datum_meta_.type_));
  OX(expr.eval_func_ = eval_get_user_var);
  return ret;
}

} //namespace sql
} //namespace oceanbase
