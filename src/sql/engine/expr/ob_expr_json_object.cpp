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
 * This file contains implementation of json_object.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_object.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_cast.h"

#include "lib/hash/ob_hashset.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonObject::ObExprJsonObject(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_OBJECT, N_JSON_OBJECT, OCCUR_AS_PAIR, NOT_ROW_DIMENSION)
{
}

ObExprJsonObject::~ObExprJsonObject()
{
}

int ObExprJsonObject::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (lib::is_mysql_mode() && OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is NULL", K(ret));
  } else if (OB_UNLIKELY(param_num < 0 || (lib::is_mysql_mode() && param_num % 2 != 0 || (lib::is_oracle_mode() && param_num < 4)))) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name = "json_object";
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (lib::is_mysql_mode() && exec_ctx->is_ps_prepare_stage()) {
    // the ps prepare stage does not do type deduction, and directly gives a default type.
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  } else {
    if (lib::is_oracle_mode()) {
      // type.set_json();
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num - 4; i += 3) {
        if ((types_stack[i].get_type() == ObNullType)) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
        } else if (types_stack[i].get_type() != ObCharType
                    && types_stack[i].get_type() != ObVarcharType
                    && types_stack[i].get_type() != ObNCharType
                    && types_stack[i].get_type() != ObNVarchar2Type) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(types_stack[i].get_type()), "CHAR");
        } else if (ob_is_string_type(types_stack[i].get_type())) {
          if (types_stack[i].get_charset_type() == CHARSET_BINARY) {
            ret = OB_ERR_INVALID_JSON_CHARSET;
            LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else {
          types_stack[i].set_calc_type(ObCharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }

        if (OB_SUCC(ret)) {
          ObObjType doc_type = types_stack[i + 1].get_type();
          if (types_stack[i + 1].get_type() == ObNullType) {
          } else if (ob_is_string_type(doc_type)) {
            if (types_stack[i + 1].get_collation_type() == CS_TYPE_BINARY) {
              if (lib::is_mysql_mode()) {
                // unsuport string type with binary charset
                ret = OB_ERR_INVALID_JSON_CHARSET;
                LOG_WARN("Unsupport for string type with binary charset input.", K(ret), K(doc_type));
              } else {
                types_stack[i + 1].set_calc_collation_type(CS_TYPE_BINARY);
              }
            } else if (types_stack[i + 1].get_charset_type() != CHARSET_UTF8MB4) {
              types_stack[i + 1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            }
          } else if (doc_type == ObJsonType) {
            types_stack[i + 1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          } else {
            types_stack[i + 1].set_calc_type(types_stack[i + 1].get_type());
            types_stack[i + 1].set_calc_collation_type(types_stack[i + 1].get_collation_type());
          }
        }

        if (OB_SUCC(ret)) {
          if (types_stack[i + 2].get_type() == ObNullType) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("<format type> param type is unexpected", K(types_stack[i + 2].get_type()), K(ret));
          } else if (types_stack[i + 2].get_type() != ObIntType) {
            types_stack[i + 2].set_calc_type(ObIntType);
          }
        }
      }

      // null type  : param_num - 4
      if (OB_SUCC(ret)) {
        if (types_stack[param_num - 4].get_type() == ObNullType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("<empty type> param type is unexpected", K(types_stack[param_num - 4].get_type()), K(ret));
        } else if (types_stack[param_num - 4].get_type() != ObIntType) {
          types_stack[param_num - 4].set_calc_type(ObIntType);
        }
      }

      // returning type : param_num - 3
      ObExprResType dst_type;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObJsonExprHelper::get_cast_type(types_stack[param_num - 3], dst_type))) {
          LOG_WARN("get cast dest type failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[param_num - 3], type, dst_type, type_ctx))) {
          LOG_WARN("set dest type failed", K(ret));
        } else {
          type.set_calc_collation_type(type.get_collation_type());
        }
      }

      // strict type  : param_num - 2,
      if (OB_SUCC(ret)) {
        if (types_stack[param_num - 2].get_type() == ObNullType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("<empty type> param type is unexpected", K(types_stack[param_num - 2].get_type()), K(ret));
        } else if (types_stack[param_num - 2].get_type() != ObIntType) {
          types_stack[param_num - 2].set_calc_type(ObIntType);
        }
      }

      // with unique keys type  : param_num - 1,
      if (OB_SUCC(ret)) {
        if (types_stack[param_num - 1].get_type() == ObNullType) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("<empty type> param type is unexpected", K(types_stack[param_num - 1].get_type()), K(ret));
        } else if (types_stack[param_num - 1].get_type() != ObIntType) {
          types_stack[param_num - 1].set_calc_type(ObIntType);
        }
      }
    } else {
      type.set_json();
      type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
      for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i += 2) {
        if ((types_stack[i].get_type() == ObNullType)) {
          ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
          LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
        } else if (ob_is_string_type(types_stack[i].get_type())) {
          if (types_stack[i].get_charset_type() == CHARSET_BINARY) {
            ret = OB_ERR_INVALID_JSON_CHARSET;
            LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else {
          types_stack[i].set_calc_type(ObLongTextType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }

        if (OB_SUCC(ret)) {
          if (ob_is_string_type(types_stack[i+1].get_type())) {
            if (types_stack[i+1].get_charset_type() != CHARSET_UTF8MB4) {
              types_stack[i+1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            }
          } else if (types_stack[i+1].get_type() == ObJsonType) {
            types_stack[i+1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      }
    }

  }

  return ret;
}

// for new sql engine
int ObExprJsonObject::eval_json_object(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonObject j_obj(&temp_allocator);
  ObIJsonBase *j_base = &j_obj;

  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  }

  for (int32 i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 2) {
    ObExpr *arg = expr.args_[i];
    ObDatum *json_datum = NULL;  
    if (OB_FAIL(arg->eval(ctx, json_datum))) {
      LOG_WARN("failed: eval json args datum failed", K(ret));
    } else if (json_datum->is_null()) {
      ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
      LOG_WARN("failed:json key is null", K(ret));
    } else {
      ObString key = json_datum->get_string();
      ObIJsonBase *j_val = NULL;
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(arg, ctx, temp_allocator, key, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(key));
      } else if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, j_val))) {
        ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
      } else if (OB_FAIL(j_obj.add(key, static_cast<ObJsonNode*>(j_val), false, true, false))) {
        if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
          LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
        }
        LOG_WARN("failed: append json object kv", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObString raw_bin;
    j_obj.stable_sort();
    j_obj.unique();
    if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("failed: get json raw binary", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }

  return ret;
}

int ObExprJsonObject::eval_ora_json_object(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBuffer res_str(&temp_allocator);
  ObObjType val_type;
  ObCollationType value_cs_type;
  uint8_t format_type = OB_JSON_ON_STRICT_IMPLICIT;
  bool is_column = false;
  common::hash::ObHashSet<ObString> view_key_names;
  ObJsonObject j_obj(&temp_allocator);
  ObIJsonBase *j_base = &j_obj;
  ObJsonBuffer jbuf(&temp_allocator);

  // parse null type
  uint8_t null_type = OB_JSON_ON_NULL_IMPLICIT;
  if (OB_SUCC(ret)) {
    ret = get_clause(expr, ctx, expr.arg_cnt_ - 4, null_type, OB_JSON_ON_NULL_NUM);
  }

  // parse returning type
  ObObjType dst_type;
  int32_t dst_len = OB_MAX_TEXT_LENGTH;
  if (OB_SUCC(ret)) {
    ret = ObJsonExprHelper::get_dest_type(expr, expr.arg_cnt_ - 3, ctx, dst_type, dst_len);
  }

    // parse strict type
  uint8_t strict_type = OB_JSON_ON_STRICT_IMPLICIT;
  if (OB_SUCC(ret)) {
    ret = get_clause(expr, ctx, expr.arg_cnt_ - 2, strict_type, OB_JSON_ON_STRICT_NUM);
  }

  // parse unique type
  uint8_t unique_type = OB_JSON_ON_UNIQUE_IMPLICIT;
  if (OB_SUCC(ret)) {
    ret = get_clause(expr, ctx, expr.arg_cnt_ - 1, unique_type, OB_JSON_ON_UNIQUE_NUM);
  }

  if (OB_FAIL(res_str.append("{"))) {
    LOG_WARN("symbol write fail");
  }
  int64_t size_set= (expr.arg_cnt_ - 4) / 3;
  int64_t bucket_num = (size_set / 2) + 1;
  if (size_set > 0 && OB_FAIL(view_key_names.create(bucket_num))) {
    LOG_WARN("init hash failed", K(ret), K(bucket_num));
  }
  for (int32 i = 0; OB_SUCC(ret) && i < expr.arg_cnt_ - 4; i += 3) {
    ObExpr *arg = expr.args_[i];
    ObDatum *json_datum = NULL;
    ObObjType key_type = expr.args_[i]->datum_meta_.type_;
    if (OB_FAIL(arg->eval(ctx, json_datum))) {
      LOG_WARN("failed: eval json args datum failed", K(ret));
    } else if (json_datum->is_null()) {
      ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
      LOG_WARN("failed:json key is null", K(ret));
    } else if (!ob_is_string_type(key_type) && key_type != ObJsonType) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
    }
    if (OB_SUCC(ret)) {
      ret = get_clause(expr, ctx, i + 2, format_type, OB_JSON_ON_STRICT_NUM);
    }
    ObExpr *val_expr = expr.args_[i + 1];
    val_type = val_expr->datum_meta_.type_;
    bool can_only_be_null = false;
    if (OB_SUCC(ret) && format_type
      && (!ObJsonExprHelper::is_convertible_to_json(val_type) || val_type == ObJsonType)) {
      if (val_type == ObObjType::ObNumberType) {
        can_only_be_null = true;
      } else {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type));
      }
    }
    if (OB_SUCC(ret)) {
      ObString key = json_datum->get_string();
      bool is_null = false;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(arg, ctx, temp_allocator, key, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(key));
      } else if (dst_type == ObJsonType) {
        ObIJsonBase *j_val = NULL;
        uint32_t parse_flag = (strict_type == OB_JSON_ON_STRICT_IMPLICIT) ?
                                ObJsonParser::JSN_STRICT_FLAG : ObJsonParser::JSN_RELAXED_FLAG;
        if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, j_val, false, format_type, parse_flag))) {
          ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
        } else if (can_only_be_null && j_val->json_type() != ObJsonNodeType::J_NULL) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type));
        } else if (j_val->json_type() == ObJsonNodeType::J_NULL && null_type == OB_JSON_ON_NULL_ABSENT) {
          ObString key = json_datum->get_string();
          if (unique_type == OB_JSON_ON_UNIQUE_USE && OB_FAIL(check_key_valid(view_key_names, key))) {
            LOG_WARN("duplicate key fail");
          }
        // do nothing
        } else if (OB_FAIL(check_key_valid(view_key_names, key))) {
          LOG_WARN("duplicate key fail");
        } else if (OB_FAIL(j_base->object_add(key, j_val))) {
          if (ret == OB_ERR_JSON_DOCUMENT_NULL_KEY) {
            LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
          }
          LOG_WARN("failed: append json object kv", K(ret));
        }
      } else {
        ObJsonString ob_str(key.ptr(), key.length());
        ObDatum *j_datum = NULL;
        bool t_is_null = false;
        if (OB_FAIL(get_ora_json_doc(expr, ctx, i+1, j_datum, t_is_null))) {
          LOG_WARN("get value json doc fail", K(ret), K(j_datum));
        } else if (can_only_be_null && !t_is_null) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "CHAR", ob_obj_type_str(val_type));
        } else if (t_is_null && null_type == OB_JSON_ON_NULL_ABSENT) {
          ObString key = json_datum->get_string();
          if (unique_type == OB_JSON_ON_UNIQUE_USE && OB_FAIL(check_key_valid(view_key_names, key))) {
            LOG_WARN("duplicate key fail");
          }
          // do nothing only check
        } else if (t_is_null && null_type <= 1) {
          if (OB_SUCC(ret) && i > 0 && res_str.length() > 1) {
            if (OB_FAIL(res_str.append(","))) {
              LOG_WARN("comma write fail");
            }
          }
          if (OB_SUCC(ret)) {
            if (unique_type == OB_JSON_ON_UNIQUE_USE && OB_FAIL(check_key_valid(view_key_names, key))) {
              LOG_WARN("duplicate key fail");
            } else if (OB_FAIL(ob_str.print(res_str, true))) {
              LOG_WARN("fail to print json node", K(ret));
            } else if (OB_FAIL(res_str.append(":"))) {
              LOG_WARN("colon write fail");
            } else if (OB_FAIL(res_str.append("null"))) {
              LOG_WARN("null write fail");
            }
          }
        } else {
          if (OB_SUCC(ret) && i > 0 && res_str.length() > 1) {
            if (res_str.append(",")) {
              LOG_WARN("comma write fail");
            }
          }
          if (OB_SUCC(ret)) {
            if (unique_type == OB_JSON_ON_UNIQUE_USE && OB_FAIL(check_key_valid(view_key_names, key))) {
              LOG_WARN("duplicate key fail");
            } else if (OB_FAIL(ob_str.print(res_str, true))) {
              LOG_WARN("fail to print json node", K(ret));
            } else if (OB_FAIL(res_str.append(":"))) {
              LOG_WARN("colon write fail");
            }
          }
          if (OB_SUCC(ret)) {
            value_cs_type = expr.args_[i + 1]->datum_meta_.cs_type_;
            if (ObJsonExprHelper::is_convertible_to_json(val_type) || ob_is_raw(val_type)) {
              if (OB_FAIL(ObJsonExprHelper::transform_convertible_2String(*expr.args_[i + 1], ctx, *j_datum,
                                                           val_type, value_cs_type, res_str,
                                                           expr.args_[i + 1]->obj_meta_.has_lob_header(), format_type,
                                                           strict_type == OB_JSON_ON_STRICT_USE, i + 1))) {
                LOG_WARN("fail to transfrom to string");
              }
            } else {
              ObScale scale = expr.args_[i + 1]->datum_meta_.scale_;
              const ObTimeZoneInfo *tz_info = get_timezone_info(ctx.exec_ctx_.get_my_session());
              if (OB_FAIL(ObJsonExprHelper::transform_scalar_2String(ctx, *j_datum, val_type, scale, tz_info, res_str))) {
                LOG_WARN("Fail to transformer string");
              }
            }
          }
          if (OB_SUCC(ret) && res_str.length() > OB_MAX_PACKET_LENGTH) {
            ret = OB_ERR_TOO_LONG_STRING_IN_CONCAT;
            LOG_WARN("result of json_objectagg is too long", K(ret), K(res_str.length()),
                                                            K(OB_MAX_PACKET_LENGTH));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("return value fail", K(ret));
  } else if (OB_FAIL(res_str.append("}"))) {
    LOG_WARN("symbol write fail");
  } else {
    if (dst_type == ObJsonType) {
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("failed: get json raw binary", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
        LOG_WARN("fail to pack json result", K(ret), K(raw_bin));
      }
    } else {
      if (dst_type == ObVarcharType && res_str.string().length()  > dst_len) {
        char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
        if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
          LOG_WARN("dst_len fail to string.", K(ret));
        }
        ret = OB_OPERATE_OVERFLOW;
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, "json_object");
      } else {
        ret = set_result(dst_type, res_str.string(), &temp_allocator, ctx, expr, res, strict_type, unique_type);
      }
    }
  }
  return ret;
}

int ObExprJsonObject::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_oracle_mode()) {
    rt_expr.eval_func_ = eval_ora_json_object;
  } else {
    rt_expr.eval_func_ = eval_json_object;
  }
  return OB_SUCCESS;
}

int ObExprJsonObject::check_key_valid(common::hash::ObHashSet<ObString> &view_key_names, const ObString &key_name)
{
  INIT_SUCC(ret);
  if (OB_HASH_EXIST == view_key_names.exist_refactored(key_name)) {
    ret = OB_ERR_DUPLICATE_KEY;
    LOG_WARN("duplicate key", K(ret));
  } else if (OB_FAIL(view_key_names.set_refactored(key_name, 0))) {
    LOG_WARN("store key to vector failed", K(ret), K(view_key_names.size()));
  }
  return ret;
}

int ObExprJsonObject::set_result(ObObjType dst_type, ObString str_res, common::ObIAllocator *allocator, ObEvalCtx &ctx, const ObExpr &expr, ObDatum &res, uint8_t strict_type, uint8_t unique_type) {
  INIT_SUCC(ret);
  if (dst_type == ObVarcharType || dst_type == ObLongTextType) {
    if (strict_type == OB_JSON_ON_STRICT_USE && OB_FAIL(ObJsonParser::check_json_syntax(str_res, allocator, ObJsonParser::JSN_STRICT_FLAG))) {
      ret = OB_ERR_JSON_SYNTAX_ERROR;
      LOG_WARN("fail to parse json text strict", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, str_res))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  } else {
    ret = OB_ERR_INVALID_DATA_TYPE_RETURNING;
    LOG_USER_ERROR(OB_ERR_INVALID_DATA_TYPE_RETURNING);
  }
  return ret;
}

int ObExprJsonObject::get_ora_json_doc(const ObExpr &expr, ObEvalCtx &ctx,
                          uint16_t index, ObDatum*& j_datum,
                          bool &is_null)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr.args_[index];
  j_datum = NULL;
  ObObjType val_type = json_arg->datum_meta_.type_;
  if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, j_datum)))) {
    LOG_WARN("eval json arg failed", K(ret), K(val_type));
  } else if (j_datum->is_null()) {
    is_null = true;
  }
  return ret;
}

int ObExprJsonObject::get_clause(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                uint8_t index,
                                uint8_t &type,
                                int64_t size_para)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObDatum *json_datum = NULL;
  if (OB_FAIL(json_arg->eval(ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type != ObIntType) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input type error", K(val_type));
  } else {
    int64_t option_type = json_datum->get_int();
    if (option_type < 0 ||
        option_type >= size_para) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input option type error", K(option_type));
    } else {
      type = static_cast<uint8_t>(option_type);
    }
  }
  return ret;
}

} // sql
} // oceanbase