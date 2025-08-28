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
#include "sql/engine/expr/ob_expr_inner_info_cols_printer.h"
#include <string.h>
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_printer.h"
#include "common/object/ob_obj_type.h"
//#include "sql/engine/expr/ob_expr_promotion_util.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

ObExprInnerInfoColsColumnDefPrinter::ObExprInnerInfoColsColumnDefPrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_COLUMN_DEF_PRINTER, N_INNER_INFO_COLS_COLUMN_DEF_PRINTER, 3,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsColumnDefPrinter::~ObExprInnerInfoColsColumnDefPrinter()
{
}

inline int ObExprInnerInfoColsColumnDefPrinter::calc_result_type3(ObExprResType &type,
                                                                  ObExprResType &type1,
                                                                  ObExprResType &type2,
                                                                  ObExprResType &type3,
                                                                  common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type3.set_calc_type(ObIntType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(OB_MAX_DEFAULT_VALUE_LENGTH);
  return ret;
}

int ObExprInnerInfoColsColumnDefPrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsColumnDefPrinter::eval_column_def;
  return ret;
}

int ObExprInnerInfoColsColumnDefPrinter::eval_column_def(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *tenant_id = nullptr;
  ObDatum *table_id = nullptr;
  ObDatum *column_id = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, tenant_id, table_id, column_id))) {
    LOG_WARN("failed to eval tenant id", K(ret));
  } else if (tenant_id->is_null() || table_id->is_null() || column_id->is_null()) {
    expr_datum.set_null();
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema_service", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id->get_int(), schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id->get_int(), table_id->get_int(), table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      expr_datum.set_null();
    } else {
      const ObColumnSchemaV2 *tmp_column_schema = NULL;
      if (OB_ISNULL(tmp_column_schema = table_schema->get_column_schema(column_id->get_int()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(ret), K(table_schema->get_table_id()), K(column_id->get_int()));
      } else {
        ObEvalCtx::TempAllocGuard alloc_guard(ctx);
        ObIAllocator &calc_alloc = alloc_guard.get_allocator();
        ObObj def_obj = tmp_column_schema->get_cur_default_value();
        if (IS_DEFAULT_NOW_OBJ(def_obj)) {
          expr_datum.set_string(ObString::make_string(N_UPPERCASE_CUR_TIMESTAMP));
        } else if (def_obj.is_bit() || ob_is_enum_or_set_type(def_obj.get_type())) {
          char *buf = NULL;
          int64_t buf_len = number::ObNumber::MAX_PRINTABLE_SIZE;
          int64_t pos = 0;
          if (OB_UNLIKELY(NULL == (buf = static_cast<char*>(calc_alloc.alloc(buf_len))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret));
          } else if (def_obj.is_bit()) {
            if (OB_FAIL(def_obj.print_varchar_literal(buf, buf_len, pos, TZ_INFO(ctx.exec_ctx_.get_my_session())))) {
              LOG_WARN("fail to print varchar literal", K(ret), K(def_obj), K(buf_len), K(pos), K(buf));
            } else {
              expr_datum.set_string(ObString(static_cast<int32_t>(pos), buf));
            }
          } else {
            if (OB_FAIL(def_obj.print_plain_str_literal(tmp_column_schema->get_extended_type_info(), buf, buf_len, pos))) {
              LOG_WARN("fail to print plain str literal", K(buf), K(buf_len), K(pos), K(ret));
            } else {
              expr_datum.set_string(ObString(static_cast<int32_t>(pos), buf));
            }
          }
        } else {
          const ObDataTypeCastParams dtc_params = sql::ObBasicSessionInfo::create_dtc_params(ctx.exec_ctx_.get_my_session());
          ObCastCtx cast_ctx(&calc_alloc, &dtc_params, CM_NONE, ObCharset::get_system_collation());
          ObObj casted_cell;
          const ObObj *res_cell = NULL;
          if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx,
                                           def_obj,
                                           casted_cell, res_cell))) {
            LOG_WARN("failed to cast to object", K(ret));
          } else {
            if (!res_cell->is_null()) {
              expr_datum.set_string(res_cell->get_string());
            } else {
              expr_datum.set_null();
            }
          }
        }
      }
      if (OB_SUCC(ret) && !expr_datum.is_null()) {
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObString res;
        OZ(deep_copy_ob_string(res_alloc, expr_datum.get_string(), res));
        expr_datum.set_string(res);
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsColumnDefPrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsCharLenPrinter::ObExprInnerInfoColsCharLenPrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_CHAR_LEN_PRINTER, N_INNER_INFO_COLS_CHAR_LEN_PRINTER, 3,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsCharLenPrinter::~ObExprInnerInfoColsCharLenPrinter()
{
}

inline int ObExprInnerInfoColsCharLenPrinter::calc_result_type3(ObExprResType &type,
                                                                ObExprResType &type1,
                                                                ObExprResType &type2,
                                                                ObExprResType &type3,
                                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type3.set_calc_type(ObIntType);
  type.set_type(ObUInt64Type);
  return ret;
}

int ObExprInnerInfoColsCharLenPrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsCharLenPrinter::eval_column_char_len;
  return ret;
}

int ObExprInnerInfoColsCharLenPrinter::eval_column_char_len(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *data_type = nullptr;
  ObDatum *collation_type = nullptr;
  ObDatum *data_length = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, data_type, collation_type, data_length))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (data_type->is_null() || collation_type->is_null() || data_length->is_null()) {
    expr_datum.set_null();
  } else {
    if (ob_is_string_tc(static_cast<ObObjType> (data_type->get_int()))) {
      ObCollationType coll = static_cast<ObCollationType> (collation_type->get_int());
      int64_t mbmaxlen = 0;
      if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(coll, mbmaxlen))) {
        LOG_WARN("failed to get mbmaxlen", K(ret), K(coll));
      } else {
        expr_datum.set_int(static_cast<uint64_t>(
                mbmaxlen * data_length->get_int()));
      }
    } else if (ob_is_text_tc(static_cast<ObObjType> (data_type->get_int())) ||
        ob_is_json(static_cast<ObObjType> (data_type->get_int())) || ob_is_geometry(static_cast<ObObjType> (data_type->get_int()))) {
      expr_datum.set_int(static_cast<uint64_t>(data_length->get_int()));
    } else {
      expr_datum.set_null();
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsCharLenPrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsCharNamePrinter::ObExprInnerInfoColsCharNamePrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_CHAR_NAME_PRINTER, N_INNER_INFO_COLS_CHAR_NAME_PRINTER, 2,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsCharNamePrinter::~ObExprInnerInfoColsCharNamePrinter()
{
}

inline int ObExprInnerInfoColsCharNamePrinter::calc_result_type2(ObExprResType &type,
                                                                ObExprResType &type1,
                                                                ObExprResType &type2,
                                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(MAX_CHARSET_LENGTH);
  return ret;
}

int ObExprInnerInfoColsCharNamePrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsCharNamePrinter::eval_column_char_name;
  return ret;
}

int ObExprInnerInfoColsCharNamePrinter::eval_column_char_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *data_type = nullptr;
  ObDatum *collation_type = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, data_type))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, collation_type))) {
    LOG_WARN("failed to eval collation type", K(ret));
  } else if (data_type->is_null() || collation_type->is_null()) {
    expr_datum.set_null();
  } else {
    ObCharsetType charset = ObCharset::charset_type_by_coll(static_cast<ObCollationType> (collation_type->get_int()));
    if(ob_is_varchar_char_type(static_cast<ObObjType> (data_type->get_int()),
                               static_cast<ObCollationType> (collation_type->get_int())) 
      || ob_is_enum_or_set_type(static_cast<ObObjType> (data_type->get_int()))
      || ob_is_text(static_cast<ObObjType> (data_type->get_int()),
                    static_cast<ObCollationType> (collation_type->get_int()))) {
        expr_datum.set_string(common::ObCharset::charset_name(charset));
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObString res;
        OZ(deep_copy_ob_string(res_alloc, expr_datum.get_string(), res));
        expr_datum.set_string(res);
    } else {
      expr_datum.set_null();           
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsCharNamePrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsCollNamePrinter::ObExprInnerInfoColsCollNamePrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_COLL_NAME_PRINTER, N_INNER_INFO_COLS_COLL_NAME_PRINTER, 2,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsCollNamePrinter::~ObExprInnerInfoColsCollNamePrinter()
{
}

inline int ObExprInnerInfoColsCollNamePrinter::calc_result_type2(ObExprResType &type,
                                                                ObExprResType &type1,
                                                                ObExprResType &type2,
                                                                common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(MAX_COLLATION_LENGTH);
  return ret;
}

int ObExprInnerInfoColsCollNamePrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsCollNamePrinter::eval_column_collation_name;
  return ret;
}

int ObExprInnerInfoColsCollNamePrinter::eval_column_collation_name(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *data_type = nullptr;
  ObDatum *collation_type = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, data_type))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, collation_type))) {
    LOG_WARN("failed to eval collation type", K(ret));
  } else if (data_type->is_null() || collation_type->is_null()) {
    expr_datum.set_null();
  } else {
    ObCharsetType charset = ObCharset::charset_type_by_coll(static_cast<ObCollationType> (collation_type->get_int()));
    if(ob_is_varchar_char_type(static_cast<ObObjType> (data_type->get_int()),
                               static_cast<ObCollationType> (collation_type->get_int())) 
      || ob_is_enum_or_set_type(static_cast<ObObjType> (data_type->get_int()))
      || ob_is_text(static_cast<ObObjType> (data_type->get_int()),
                    static_cast<ObCollationType> (collation_type->get_int()))) {
        expr_datum.set_string(common::ObCharset::collation_name(static_cast<ObCollationType> (collation_type->get_int())));
        ObExprStrResAlloc res_alloc(expr, ctx);
        ObString res;
        OZ(deep_copy_ob_string(res_alloc, expr_datum.get_string(), res));
        expr_datum.set_string(res);
    } else {
      expr_datum.set_null();           
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsCollNamePrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsPrivPrinter::ObExprInnerInfoColsPrivPrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_PRIV_PRINTER, N_INNER_INFO_COLS_PRIV_PRINTER, 2,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsPrivPrinter::~ObExprInnerInfoColsPrivPrinter()
{
}

inline int ObExprInnerInfoColsPrivPrinter::calc_result_type2(ObExprResType &type,
                                                             ObExprResType &type1,
                                                             ObExprResType &type2,
                                                             common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObVarcharType);
  type2.set_calc_type(ObVarcharType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(MAX_COLUMN_PRIVILEGE_LENGTH);
  return ret;
}

int ObExprInnerInfoColsPrivPrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsPrivPrinter::eval_column_priv;
  return ret;
}

int ObExprInnerInfoColsPrivPrinter::eval_column_priv(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *database_name = nullptr;
  ObDatum *table_name = nullptr;
  if (OB_FAIL(expr.args_[0]->eval(ctx, database_name))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, table_name))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (database_name->is_null() || table_name->is_null()) {
    expr_datum.set_null();
  } else {
    char *buf = NULL;
    int64_t buf_len = 200;
    int64_t pos = 0;
    ObSessionPrivInfo session_priv;
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                                                              schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(ctx.exec_ctx_.get_my_session()->get_session_priv_info(session_priv))) {
      LOG_WARN( "fail to get session priv info", K(ret));
    } else if (OB_UNLIKELY(!session_priv.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN( "session priv is invalid", "tenant_id", session_priv.tenant_id_,
                  "user_id", session_priv.user_id_, K(ret));
    } else if (OB_ISNULL(buf = static_cast<char*>(calc_alloc.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN( "fail to allocate memory", K(ret));
    } else {
      const common::ObIArray<uint64_t> &enable_role_id_array = ctx.exec_ctx_.get_my_session()->get_enable_role_array();
      ObNeedPriv need_priv(database_name->get_string(), table_name->get_string(),
                            OB_PRIV_TABLE_LEVEL, OB_PRIV_SELECT, false);
      if (OB_FAIL(fill_col_privs(schema_guard, session_priv, enable_role_id_array, need_priv, OB_PRIV_SELECT,
                                 "select,", buf, buf_len, pos))) {
        LOG_WARN("fail to fill col priv", K(need_priv), K(ret));
      } else if (OB_FAIL(fill_col_privs(schema_guard, session_priv, enable_role_id_array, need_priv, OB_PRIV_INSERT,
                                        "insert,", buf, buf_len, pos))) {
        LOG_WARN("fail to fill col priv", K(need_priv), K(ret));
      } else if (OB_FAIL(fill_col_privs(schema_guard, session_priv, enable_role_id_array, need_priv, OB_PRIV_UPDATE,
                                        "update,", buf, buf_len, pos))) {
        LOG_WARN("fail to fill col priv", K(need_priv), K(ret));
      } else if (OB_FAIL(fill_col_privs(schema_guard, session_priv, enable_role_id_array, need_priv, OB_PRIV_REFERENCES,
                                        "reference,", buf, buf_len, pos))) {
        LOG_WARN("fail to fill col priv", K(need_priv), K(ret));
      } else {
        if (pos > 0) {
          expr_datum.set_string(ObString(0, pos - 1, buf));
          ObExprStrResAlloc res_alloc(expr, ctx);
          ObString res;
          OZ(deep_copy_ob_string(res_alloc, expr_datum.get_string(), res));
          expr_datum.set_string(res);
        } else {
          expr_datum.set_string(ObString(""));
        }
      }
    }
  }
  return ret;
}

int ObExprInnerInfoColsPrivPrinter::fill_col_privs(share::schema::ObSchemaGetterGuard &schema_guard,
                                                   const ObSessionPrivInfo &session_priv,
                                                   const common::ObIArray<uint64_t> &enable_role_id_array,
                                                   ObNeedPriv &need_priv, 
                                                   ObPrivSet priv_set, 
                                                   const char *priv_str,
                                                   char* buf,
                                                   const int64_t buf_len,
                                                   int64_t &pos)
{
  int ret = OB_SUCCESS;
  need_priv.priv_set_ = priv_set;
  if (OB_SUCC(schema_guard.check_single_table_priv(session_priv, enable_role_id_array, need_priv))) {
    ret = databuff_printf(buf, buf_len, pos, "%s", priv_str);
  } else if (OB_ERR_NO_TABLE_PRIVILEGE == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsPrivPrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsExtraPrinter::ObExprInnerInfoColsExtraPrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_EXTRA_PRINTER, N_INNER_INFO_COLS_EXTRA_PRINTER, 4,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsExtraPrinter::~ObExprInnerInfoColsExtraPrinter()
{
}

int ObExprInnerInfoColsExtraPrinter::calc_result_typeN(ObExprResType &type,
                                                       ObExprResType *types_array,
                                                       int64_t param_num,
                                                       ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < param_num && OB_SUCC(ret); i++) {
    types_array[i].set_calc_type(ObIntType);
  }
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(COLUMN_EXTRA_LENGTH);
  return ret;
}

int ObExprInnerInfoColsExtraPrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(4 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsExtraPrinter::eval_column_extra;
  return ret;
}

int ObExprInnerInfoColsExtraPrinter::eval_column_extra(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *auto_inc = nullptr;
  ObDatum *on_update_current_timestamp = nullptr;
  ObDatum *data_scale = nullptr;
  ObDatum *column_flag = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, auto_inc, on_update_current_timestamp, data_scale, column_flag))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (auto_inc->is_null() || on_update_current_timestamp->is_null()
             || data_scale->is_null() || column_flag->is_null()) {
    expr_datum.set_null();
  } else {
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    ObString extra = ObString::make_string("");
    // auto_increment 和 on update current_timestamp 不会同时出现在同一列上
    if (auto_inc->get_int()) {
      extra = ObString::make_string("auto_increment");
    } else if (on_update_current_timestamp->get_int()) {
      int16_t scale = data_scale->get_int();
      if (0 == scale) {
        extra = ObString::make_string("on update current_timestamp");
      } else {
        char* buf = NULL;
        int64_t buf_len = 32;
        int64_t pos = 0;
        if (OB_UNLIKELY(NULL == (buf = static_cast<char*> (calc_alloc.alloc(buf_len))))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN( "fail to allocate memory", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "on update current_timestamp(%d)", scale))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print on update current_tiemstamp", K(ret));
        } else {
          extra = ObString(static_cast<int32_t>(pos), buf);
        }
      }
    } else if (column_flag->get_int() & VIRTUAL_GENERATED_COLUMN_FLAG) {
      extra = ObString::make_string("VIRTUAL GENERATED");
    } else if (column_flag->get_int() & STORED_GENERATED_COLUMN_FLAG) {
      extra = ObString::make_string("STORED GENERATED");
    }
    if (OB_SUCC(ret) && (column_flag->get_int() & INVISIBLE_COLUMN_FLAG)) {
      int64_t append_len = sizeof("INVISIBLE");
      if (extra.length() > 0) {
        append_len += 1;
      }
      int64_t buf_len = extra.length() + append_len;
      int64_t cur_pos = extra.length();
      char *buf = NULL;
      if (OB_ISNULL(buf = static_cast<char *>(calc_alloc.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to allocate memory", K(ret));
      } else if (FALSE_IT(MEMCPY(buf, extra.ptr(), extra.length()))) {
      } else if (extra.length() == 0
          && OB_FAIL(databuff_printf(buf, buf_len, cur_pos, "%s", "INVISIBLE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print on Mysql invisible column", K(ret));
      } else if (extra.length() > 0
          && OB_FAIL(databuff_printf(buf, buf_len, cur_pos, "%s", " INVISIBLE"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print on Mysql invisible column", K(ret));
      } else {
        extra = ObString(cur_pos, buf);
      }
    }
    if (OB_SUCC(ret)) {
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObString res;
      OZ(deep_copy_ob_string(res_alloc, extra, res));
      expr_datum.set_string(res);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsExtraPrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsDataTypePrinter::ObExprInnerInfoColsDataTypePrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_DATA_TYPE_PRINTER, N_INNER_INFO_COLS_DATA_TYPE_PRINTER, 4,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsDataTypePrinter::~ObExprInnerInfoColsDataTypePrinter()
{
}

int ObExprInnerInfoColsDataTypePrinter::calc_result_typeN(ObExprResType &type,
                                                          ObExprResType *types_array,
                                                          int64_t param_num,
                                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  types_array[0].set_calc_type(ObIntType);
  types_array[1].set_calc_type(ObIntType);
  types_array[2].set_calc_type(ObVarcharType);
  types_array[3].set_calc_type(ObIntType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(OB_MAX_EXTENDED_TYPE_INFO_LENGTH);
  return ret;
}

int ObExprInnerInfoColsDataTypePrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(4 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsDataTypePrinter::eval_column_data_type;
  return ret;
}

int ObExprInnerInfoColsDataTypePrinter::eval_column_data_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *data_type = nullptr;
  ObDatum *collation_type = nullptr;
  ObDatum *extended_type_info = nullptr;
  ObDatum *srs_id = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, data_type, collation_type, extended_type_info, srs_id))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (data_type->is_null() || collation_type->is_null() || srs_id->is_null()) {
    expr_datum.set_null();
  } else {
    static const int64_t SRS_ID_MASK = UINT64_MAX >> (64 - 5);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    ObObjType column_type = ObMaxType;
    char *data_type_str = nullptr;
    ObArrayHelper<ObString> extended_infos;
    int64_t pos = 0;
    if (OB_UNLIKELY(NULL == (data_type_str = static_cast<char *>(calc_alloc.alloc(
                            OB_MAX_SYS_PARAM_NAME_LENGTH))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (!extended_type_info->is_null() && 0 != extended_type_info->get_string().length()
               && OB_FAIL(ObSchema::deserialize_string_array(extended_type_info->get_string().ptr(),
                                                             extended_type_info->get_string().length(),
                                                             pos, extended_infos, &calc_alloc))) {
      LOG_WARN("failed to get extended infos", K(ret));
    } else if (OB_FAIL(ob_sql_type_str(data_type_str,
                                       OB_MAX_SYS_PARAM_NAME_LENGTH,
                                       static_cast<ObObjType> (data_type->get_int()),
                                       static_cast<ObCollationType> (collation_type->get_int()),
                                       extended_infos,
                                       static_cast<common::ObGeoType> (srs_id->get_int() & SRS_ID_MASK)))) {
      LOG_WARN("fail to get data type str",K(ret), K(static_cast<ObObjType> (data_type->get_int())));
    } else {
      ObString type_val(static_cast<int32_t>(strlen(data_type_str)), data_type_str);
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObString res;
      OZ(deep_copy_ob_string(res_alloc, type_val, res));
      expr_datum.set_string(res);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsDataTypePrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

ObExprInnerInfoColsColumnTypePrinter::ObExprInnerInfoColsColumnTypePrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_COLUMN_TYPE_PRINTER, N_INNER_INFO_COLS_COLUMN_TYPE_PRINTER, 10,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsColumnTypePrinter::~ObExprInnerInfoColsColumnTypePrinter()
{
}

int ObExprInnerInfoColsColumnTypePrinter::calc_result_typeN(ObExprResType &type,
                                                          ObExprResType *types_array,
                                                          int64_t param_num,
                                                          ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < 8 && OB_SUCC(ret); i++) {
    types_array[i].set_calc_type(ObIntType);
  }
  types_array[8].set_calc_type(ObVarcharType);
  types_array[9].set_calc_type(ObIntType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(OB_MAX_EXTENDED_TYPE_INFO_LENGTH);
  return ret;
}

int ObExprInnerInfoColsColumnTypePrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(10 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsColumnTypePrinter::eval_column_column_type;
  return ret;
}

int ObExprInnerInfoColsColumnTypePrinter::eval_column_column_type(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *data_type = nullptr;
  ObDatum *sub_data_type = nullptr;
  ObDatum *srs_id = nullptr;
  ObDatum *collation_type = nullptr;
  ObDatum *data_scale = nullptr;
  ObDatum *data_length = nullptr;
  ObDatum *data_precision = nullptr;
  ObDatum *zero_fill = nullptr;
  ObDatum *extended_type_info = nullptr;
  ObDatum *is_string_lob = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, data_type, sub_data_type,
                                    srs_id, collation_type, data_scale,
                                    data_length, data_precision, zero_fill,
                                    extended_type_info, is_string_lob))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (data_type->is_null() || sub_data_type->is_null() || srs_id->is_null()
             || collation_type->is_null() || data_scale->is_null() || data_length->is_null()
             || data_precision->is_null() || zero_fill->is_null() || is_string_lob->is_null()) {
    expr_datum.set_null();
  } else {
    static const int64_t SRS_ID_MASK = UINT64_MAX >> (64 - 5);
    ObEvalCtx::TempAllocGuard alloc_guard(ctx);
    ObIAllocator &calc_alloc = alloc_guard.get_allocator();
    ObObjType column_type = ObMaxType;
    char *data_type_str = nullptr;
    ObArrayHelper<ObString> extended_infos;
    int64_t pos = 0;
    ObAccuracy accuracy;
    accuracy.length_ = data_length->get_int();
    accuracy.precision_ = data_precision->get_int();
    accuracy.scale_ = data_scale->get_int();
    ObObjMeta meta;
    meta.set_type(static_cast<ObObjType> (data_type->get_int()));
    meta.set_collation_type(static_cast<ObCollationType> (collation_type->get_int()));
    meta.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
    meta.set_scale(data_scale->get_int());
    if (OB_UNLIKELY(NULL == (data_type_str = static_cast<char *>(calc_alloc.alloc(
                            OB_MAX_SYS_PARAM_NAME_LENGTH))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret));
    } else if (!extended_type_info->is_null() && 0 != extended_type_info->get_string().length()
               && OB_FAIL(ObSchema::deserialize_string_array(extended_type_info->get_string().ptr(),
                                                             extended_type_info->get_string().length(),
                                                             pos, extended_infos, &calc_alloc))) {
      LOG_WARN("failed to get extended infos", K(ret));
    } else {
      int64_t pos = 0;
      const ObLengthSemantics default_length_semantics = ctx.exec_ctx_.get_my_session()->get_local_nls_length_semantics();
      const uint64_t sub_type = ObExtendType == static_cast<ObObjType> (data_type->get_int()) ?
                                sub_data_type->get_int() : static_cast<uint64_t>(srs_id->get_int() & SRS_ID_MASK);
      ObObjType column_type = ObMaxType;
      const ObColumnSchemaV2 *tmp_column_schema = NULL;
      if (OB_FAIL(ob_sql_type_str(meta,
                                  accuracy,
                                  extended_infos,
                                  default_length_semantics,
                                  data_type_str,
                                  OB_MAX_SYS_PARAM_NAME_LENGTH,
                                  pos, sub_type, is_string_lob->get_int()))) {
        if (OB_SIZE_OVERFLOW == ret) {
          pos = 0;
          if (OB_UNLIKELY(NULL == (data_type_str = static_cast<char *>(calc_alloc.alloc(
                                  OB_MAX_EXTENDED_TYPE_INFO_LENGTH))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory", K(ret));
          } else if (OB_FAIL(ob_sql_type_str(meta,
                                             accuracy,
                                             extended_infos,
                                             default_length_semantics,
                                             data_type_str,
                                             OB_MAX_EXTENDED_TYPE_INFO_LENGTH,
                                             pos, sub_type, is_string_lob->get_int()))) {
            LOG_WARN("fail to get column type str",K(ret));
          }
        } else {
          LOG_WARN("fail to get column type str",K(ret));
        }
      }
      if (OB_SUCC(ret) && zero_fill->get_int()) {
        // zerofill, only for int, float, decimal
        if (OB_FAIL(databuff_printf(data_type_str, OB_MAX_SYS_PARAM_NAME_LENGTH,
                                    pos, " zerofill"))) {
          LOG_WARN("fail to print zerofill", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString type_val(static_cast<int32_t>(strlen(data_type_str)), data_type_str);
      ObExprStrResAlloc res_alloc(expr, ctx);
      ObString res;
      OZ(deep_copy_ob_string(res_alloc, type_val, res));
      expr_datum.set_string(res);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsColumnTypePrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}


ObExprInnerInfoColsColumnKeyPrinter::ObExprInnerInfoColsColumnKeyPrinter(ObIAllocator &alloc)
    : ObExprOperator(alloc, T_FUN_SYS_INNER_INFO_COLS_COLUMN_KEY_PRINTER, N_INNER_INFO_COLS_COLUMN_KEY_PRINTER, 3,
                           VALID_FOR_GENERATED_COL, INTERNAL_IN_MYSQL_MODE, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprInnerInfoColsColumnKeyPrinter::~ObExprInnerInfoColsColumnKeyPrinter()
{
}

inline int ObExprInnerInfoColsColumnKeyPrinter::calc_result_type3(ObExprResType &type,
                                                                  ObExprResType &type1,
                                                                  ObExprResType &type2,
                                                                  ObExprResType &type3,
                                                                  common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type1.set_calc_type(ObIntType);
  type2.set_calc_type(ObIntType);
  type3.set_calc_type(ObIntType);
  type.set_varchar();
  type.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  type.set_collation_level(ObCollationLevel::CS_LEVEL_IMPLICIT);
  const ObLengthSemantics def_ls = type_ctx.get_session()->get_actual_nls_length_semantics();
  type.set_length_semantics(def_ls);
  type.set_length(MAX_COLUMN_KEY_LENGTH);
  return ret;
}

int ObExprInnerInfoColsColumnKeyPrinter::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(3 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = &ObExprInnerInfoColsColumnKeyPrinter::eval_column_column_key;
  return ret;
}

int ObExprInnerInfoColsColumnKeyPrinter::eval_column_column_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *tenant_id = nullptr;
  ObDatum *table_id = nullptr;
  ObDatum *column_id = nullptr;
  if (OB_FAIL(expr.eval_param_value(ctx, tenant_id, table_id, column_id))) {
    LOG_WARN("failed to eval data type", K(ret));
  } else if (tenant_id->is_null() || table_id->is_null() || column_id->is_null()) {
    expr_datum.set_null();
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schema_service", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id->get_int(), schema_guard))) {
      LOG_WARN("failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id->get_int(), table_id->get_int(), table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(table_schema)) {
      expr_datum.set_string("");
    } else {
      const ObColumnSchemaV2 *column_schema = NULL;
      bool is_unique = false;
      bool is_multiple = false;
      bool is_first_not_null_unique = true;
      bool has_primary_key = false;
      const common::ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
      if (rowkey_info.get_size() > 0) {
        // if rowkey_info[0] is pk_increment, means there is no primary key
        uint64_t cid = OB_INVALID_ID;
        if (OB_FAIL(rowkey_info.get_column_id(0, cid))) {
          LOG_WARN("failed to column id");
        } else {
          if (cid != OB_HIDDEN_PK_INCREMENT_COLUMN_ID &&
              cid >= OB_APP_MIN_COLUMN_ID) {
            has_primary_key = true;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_id->get_int()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column schema", K(ret), K(table_schema->get_table_id()), K(column_id->get_int()));
      } else if (column_schema->is_original_rowkey_column() || column_schema->is_heap_table_primary_key_column()) {
        expr_datum.set_string("PRI");
      } else if (OB_FAIL(table_schema->
        is_unique_key_column(schema_guard, column_schema->get_column_id(),
                              is_unique, is_first_not_null_unique))) {
        LOG_WARN("failed to check unique key", K(ret));
      } else if (is_unique) {
        if (column_schema->is_nullable() || has_primary_key || !is_first_not_null_unique) {
          expr_datum.set_string("UNI");
        } else {
          // is mysql mode && is unique key && no_primary_key && is_first_not_null_unique, set as PRI
          expr_datum.set_string("PRI");
        }
      } else if (OB_FAIL(table_schema->
          is_multiple_key_column(schema_guard, column_schema->get_column_id(), is_multiple))) {
        LOG_WARN("judge multiple key fail", K(ret));
      } else if (is_multiple) {
        expr_datum.set_string("MUL");
      } else {
        expr_datum.set_string("");
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInnerInfoColsColumnKeyPrinter, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(SYS_VAR_COLLATION_CONNECTION);
  }
  return ret;
}

}
}