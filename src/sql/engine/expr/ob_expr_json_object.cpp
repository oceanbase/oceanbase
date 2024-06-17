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
#include "share/ob_json_access_utils.h"
#include "lib/hash/ob_hashset.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonObject::ObExprJsonObject(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_OBJECT, N_JSON_OBJECT, OCCUR_AS_PAIR, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
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
  if (OB_UNLIKELY(param_num < 0
      || (lib::is_mysql_mode() && param_num % 2 != 0
      || (lib::is_oracle_mode() && param_num < 4)))) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name = "json_object";
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else if (lib::is_oracle_mode()) {
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
      if (OB_FAIL(ObJsonExprHelper::get_cast_type(types_stack[param_num - 3], dst_type, type_ctx))) {
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

    ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
    ObExecContext* ctx = nullptr;

    bool is_deduce_input = true;
    if (OB_NOT_NULL(session)) {
      is_deduce_input = (!session->is_varparams_sql_prepare());
    }

    for (int64_t i = 0; OB_SUCC(ret) && is_deduce_input && i < param_num; i += 2) {
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
    if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
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
  ObJsonBuffer string_buffer(&temp_allocator);
  ObObjType val_type;
  ObCollationType value_cs_type;

  bool is_column = false;
  ObJsonObject j_obj(&temp_allocator);
  ObIJsonBase *j_base = &j_obj;
  ObJsonBuffer jbuf(&temp_allocator);

  // parse null type
  uint8_t null_type = OB_JSON_ON_NULL_IMPLICIT;
  if (OB_SUCC(ret)
      && OB_FAIL(eval_option_clause_value(expr.args_[expr.arg_cnt_ - 4], ctx, null_type, OB_JSON_ON_NULL_NUM))) {
    LOG_WARN("fail to eval option", K(ret), K(expr.arg_cnt_));
  }

  // parse returning type
  ObObjType dst_type;
  int32_t dst_len = OB_MAX_TEXT_LENGTH;
  int64_t opt_res_type = 0;
  ObDatum *datum_res_type = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_FAIL(expr.args_[expr.arg_cnt_ - 3]->eval(ctx, datum_res_type)))) {
      LOG_WARN("eval json arg failed", K(ret));
  } else {
    opt_res_type = datum_res_type->get_int();
    if (opt_res_type == 0) {
      dst_type = ObJsonType;
    } else if (OB_FAIL(ObJsonExprHelper::eval_and_check_res_type(opt_res_type, dst_type, dst_len))) {
      LOG_WARN("fail to check returning type", K(ret));
    }
  }

  // parse strict type
  uint8_t strict_type = OB_JSON_ON_STRICT_IMPLICIT;
  if (OB_SUCC(ret)
      && OB_FAIL(eval_option_clause_value(expr.args_[expr.arg_cnt_ - 2], ctx, strict_type, OB_JSON_ON_STRICT_NUM))) {
    LOG_WARN("fail to eval option", K(ret), K(expr.arg_cnt_));
  }

  // parse unique type
  uint8_t unique_type = OB_JSON_ON_UNIQUE_IMPLICIT;
  if (OB_SUCC(ret)
      && OB_FAIL(eval_option_clause_value(expr.args_[expr.arg_cnt_ - 1], ctx, unique_type, OB_JSON_ON_UNIQUE_NUM))) {
    LOG_WARN("fail to eval option", K(ret), K(expr.arg_cnt_));
  }

  bool is_strict = (strict_type == OB_JSON_ON_STRICT_USE);
  bool is_null_absent = (null_type == OB_JSON_ON_NULL_ABSENT);
  bool is_key_unique = (unique_type == OB_JSON_ON_UNIQUE_USE || (dst_type == ObJsonType && opt_res_type > 0));

  for (int32 i = 0; OB_SUCC(ret) && i < expr.arg_cnt_ - 4; i += 3) {
    uint8_t format_type = OB_JSON_ON_STRICT_IMPLICIT;

    ObExpr *arg_key = expr.args_[i];
    ObExpr *arg_value = expr.args_[i + 1];
    ObExpr *arg_opt = expr.args_[i + 2];

    ObObjType key_data_type = arg_key->datum_meta_.type_;
    ObObjType value_data_type = arg_value->datum_meta_.type_;

    ObDatum *datum_key = nullptr;
    if (OB_FAIL(arg_key->eval(ctx, datum_key))) {
      LOG_WARN("failed: eval json args datum failed", K(ret));
    } else if (datum_key->is_null() || key_data_type == ObNullType) {
      ret = OB_ERR_JSON_DOCUMENT_NULL_KEY;
      LOG_USER_ERROR(OB_ERR_JSON_DOCUMENT_NULL_KEY);
    } else if (!(ob_is_extend(key_data_type)
                 || ob_is_json(key_data_type)
                 || ob_is_raw(key_data_type)
                 || ob_is_string_type(key_data_type))) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("data type not legal for key type", K(ret), K(i), K(key_data_type));
    } else if ((!ob_is_string_type(key_data_type) && key_data_type != ObJsonType)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("data type not legal for key type", K(ret), K(i), K(key_data_type));
    } else if (OB_FAIL(eval_option_clause_value(arg_opt, ctx, format_type, OB_JSON_ON_STRICT_NUM))) {
      LOG_WARN("fail to eval option", K(ret), K(i));
    } else {
      ObIJsonBase *j_val = nullptr;
      ObString key = datum_key->get_string();
      bool is_format_json = format_type > 0;

      if (OB_FAIL(ObJsonExprHelper::eval_oracle_json_val(
                    arg_value, ctx, &temp_allocator, j_val, is_format_json, is_strict, false, is_null_absent))) {
        LOG_WARN("failed to get json value node.", K(ret), K(value_data_type), K(i));
      } else {
        bool is_key_already_exist = (j_obj.get_value(key) != nullptr);
        bool is_overwrite = (is_key_unique || dst_type == ObJsonType);
        if (is_key_already_exist && is_key_unique) {
          ret = OB_ERR_DUPLICATE_KEY;
          LOG_WARN("Found duplicate key inserted before!", K(key), K(ret));
        } else if (OB_ISNULL(j_val)) {
        } else if (OB_FAIL(j_obj.add(key, static_cast<ObJsonNode*>(j_val), false, false, is_overwrite))) {
          LOG_WARN("failed to get json value node.", K(ret), K(val_type));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (dst_type == ObJsonType) {
      ObString raw_bin;
      if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
        LOG_WARN("failed: get json raw binary", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
        LOG_WARN("fail to pack json result", K(ret), K(raw_bin));
      }
    } else {

      ObString temp_str;
      ObString result_str;

      if (OB_FAIL(string_buffer.reserve(j_obj.get_serialize_size()))) {
        LOG_WARN("fail to reserve string.", K(ret), K(j_obj.get_serialize_size()));
      } else if (OB_FAIL(j_base->print(string_buffer, false, false))) {
        LOG_WARN("fail to transform to string.", K(ret), K(string_buffer.length()));
      } else {
        ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN;
        ObCollationType dst_cs_type = expr.obj_meta_.get_collation_type();
        temp_str = string_buffer.string();
        result_str = temp_str;

        if (OB_FAIL(ObJsonExprHelper::convert_string_collation_type(in_cs_type,
                                                                    dst_cs_type,
                                                                    &temp_allocator,
                                                                    temp_str,
                                                                    result_str))) {
          LOG_WARN("fail to convert string result", K(ret));
        }
      }


      if (dst_type == ObVarcharType && result_str.length()  > dst_len) {
        char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
        if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
          LOG_WARN("dst_len fail to string.", K(ret));
        }
        ret = OB_OPERATE_OVERFLOW;
        LOG_USER_ERROR(OB_OPERATE_OVERFLOW, res_ptr, "json_object");
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, result_str))) {
        LOG_WARN("fail to pack json result", K(ret));
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

int ObExprJsonObject::eval_option_clause_value(ObExpr *expr,
                                               ObEvalCtx &ctx,
                                               uint8_t &type,
                                               int64_t size_para)
{
  INIT_SUCC(ret);
  ObExpr *json_arg = expr;
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

ObExprJsonObjectStar::ObExprJsonObjectStar(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_OBJECT_WILD_STAR, N_JSON_OBJECT_STAR, OCCUR_AS_PAIR, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonObjectStar::~ObExprJsonObjectStar()
{
}

int ObExprJsonObjectStar::calc_result_typeN(ObExprResType& type,
                                            ObExprResType* types_stack,
                                            int64_t param_num,
                                            ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);
  if (param_num != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("incorrect num of param", K(ret));
  } else {
    types_stack[0].set_calc_type(types_stack[0].get_type());
    types_stack[0].set_calc_collation_type(types_stack[0].get_collation_type());
    ObExprResType dst_type;
    dst_type.set_type(ObObjType::ObVarcharType);
    dst_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
    dst_type.set_full_length(4000, 1);
    if (OB_FAIL(ObJsonExprHelper::set_dest_type(types_stack[0], type, dst_type, type_ctx))) {
      LOG_WARN("set dest type failed", K(ret));
    } else {
      type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }
  return ret;
}

int ObExprJsonObjectStar::eval_ora_json_object_star(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ret = OB_ERR_UNEXPECTED;
  LOG_WARN("can not be use this expr, should transform to real column", K(ret));
  return ret;
}

int ObExprJsonObjectStar::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                  ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_ora_json_object_star;
  return OB_SUCCESS;
}

} // sql
} // oceanbase