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
 * This file is for implementation of func json_merge_patch
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_merge_patch.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonMergePatch::ObExprJsonMergePatch(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_MERGE_PATCH,
      N_JSON_MERGE_PATCH, 
      PARAM_NUM_UNKNOWN,
      VALID_FOR_GENERATED_COL,
      NOT_ROW_DIMENSION)
{
}

ObExprJsonMergePatch::~ObExprJsonMergePatch()
{
}

int ObExprJsonMergePatch::calc_result_typeN(ObExprResType& type,
                                            ObExprResType* types_stack,
                                            int64_t param_num,
                                            ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (lib::is_mysql_mode()) {
    const ObString name(N_JSON_MERGE_PATCH);
    if (param_num < 2) {
      ret = OB_ERR_PARAM_SIZE;
      LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
    }

    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, i, N_JSON_MERGE_PRESERVE))) {
        LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[i].get_type()));
      }
    }
  } else {
    for (size_t i = 0; i < 2 && OB_SUCC(ret); ++i) {
      ObObjType doc_type = types_stack[i].get_type();
      if (types_stack[i].get_type() == ObNullType) {
        if (i == 1) {
          ret = OB_ERR_JSON_PATCH_INVALID;
          LOG_USER_ERROR(OB_ERR_JSON_PATCH_INVALID);
        }
      } else if (!ObJsonExprHelper::is_convertible_to_json(doc_type)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, ob_obj_type_str(types_stack[i].get_type()), "JSON");
      } else if (ob_is_string_type(doc_type)) {
        if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
          types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
        } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else if (doc_type == ObJsonType) {
        types_stack[i].set_calc_type(ObJsonType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {
        types_stack[i].set_calc_type(types_stack[i].get_type());
        types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
      }
    }

    // returning type : 2
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObJsonExprHelper::parse_res_type(types_stack[0], types_stack[2], type, type_ctx))) {
       LOG_WARN("fail to parse res type.", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::parse_asc_option(types_stack[4], types_stack[0], type, type_ctx))) {
      LOG_WARN("fail to parse asc option.", K(ret));
    }

    for (size_t i = 2; OB_SUCC(ret) && i < param_num; ++i) {
      if (!ob_is_integer_type(types_stack[i].get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to calc type param type should be int type", K(types_stack[i].get_type()));
      } else {
        types_stack[i].set_calc_collation_type(types_stack[i].get_collation_type());
        types_stack[i].set_calc_type(types_stack[i].get_type());
      }
    }
  }

  return ret;
}

int ObExprJsonMergePatch::eval_json_merge_patch(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObIJsonBase *j_base = NULL;
  ObIJsonBase *j_patch_node = NULL;
  bool has_null = false;
  ObJsonNull j_null;
  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, j_base, has_null))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (has_null) {
    j_base = &j_null;
  }

  for (int32 i = 1; OB_SUCC(ret) && i < expr.arg_cnt_; i ++) {
    bool is_null = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, i, j_patch_node, is_null))) {
      LOG_WARN("get_json_doc failed", K(ret));
    } else if (is_null) {
      has_null= true;
    } else if (j_patch_node->json_type() != ObJsonNodeType::J_OBJECT) {
      j_base = j_patch_node;
      has_null = false;
    } else if (has_null) {
      // do nothing
    } else {
      ObJsonObject *j_obj = NULL;
      if (j_base->json_type() == ObJsonNodeType::J_OBJECT) {
        j_obj = static_cast<ObJsonObject *>(j_base);
      } else {
        void *buf = temp_allocator.alloc(sizeof(ObJsonObject));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("error, json merge patch allocate jsonobject buffer failed", K(ret));
        } else {
          j_obj = new (buf) ObJsonObject(&temp_allocator);
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(j_obj->merge_patch(&temp_allocator, static_cast<ObJsonObject*>(j_patch_node)))) {
          LOG_WARN("error, json merge patch failed", K(ret));
        } else {
          j_base = j_obj;
          has_null = false; 
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObString raw_bin;
    if (has_null) {
      res.set_null();
    } else if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("failed: get json raw binary", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }
  return ret;
}

#define SET_COVER_ERROR(error) \
{ \
  if (!is_cover_error) { \
    is_cover_error = true; \
    err_code = (error); \
  } \
}

int ObExprJsonMergePatch::eval_ora_json_merge_patch(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();

  bool is_cover_error = false;
  int err_code = 0;

  // eval option original int64 type value
  int64_t opt_array[OPT_MAX_ID] = {0};
  for (size_t i = 2; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    ObDatum *opt_datum = NULL;
    ObExpr *opt_expr = expr.args_[i];
    ObObjType val_type = opt_expr->datum_meta_.type_;
    ObCollationType cs_type = opt_expr->datum_meta_.cs_type_;
    if (OB_UNLIKELY(OB_FAIL(opt_expr->eval(ctx, opt_datum)))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || opt_datum->is_null()) {
    } else if (!ob_is_integer_type(val_type)) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("input type error", K(val_type));
    } else {
      opt_array[i-2] = opt_datum->get_int();
    }
  }

  const int64_t& is_pretty = opt_array[OPT_PRETTY_ID];
  const int64_t& is_trunc = opt_array[OPT_TRUNC_ID];
  const int64_t& is_asc = opt_array[OPT_ASCII_ID];
  const int64_t& err_type = opt_array[OPT_ERROR_ID];
  const int64_t& return_type = opt_array[OPT_RES_TYPE_ID];

  // some constraint check
  ObObjType dst_type;
  int32_t dst_len;
  if (OB_FAIL(ret)) {
  } else if (return_type == 0) {
    dst_type = ObJsonType;
    const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType];
    dst_len = default_accuracy.get_length();
    // dst_type = expr.args_[0]->datum_meta_.type_;
    // const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[dst_type];
    // dst_len = dst_type == ObVarcharType ? OB_MAX_ORACLE_VARCHAR_LENGTH : default_accuracy.get_length();
  } else if (OB_FAIL(ObJsonExprHelper::eval_and_check_res_type(return_type, dst_type, dst_len))) {
    LOG_WARN("fail to check returning type", K(ret));
  } else if ((expr.datum_meta_.cs_type_ == CS_TYPE_BINARY || dst_type == ObJsonType) && (opt_array[OPT_PRETTY_ID] > 0 || opt_array[OPT_ASCII_ID] > 0)) {
    // ascii or pretty only support text
    ret = OB_ERR_NON_TEXT_RET_NOTSUPPORT;
    LOG_WARN("ASCII or PRETTY not supported for non-textual return data type", K(ret));
  }

  ObIJsonBase *j_base = NULL;
  bool has_null = false;
  ObJsonNull j_null;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, j_base, has_null))) {
    LOG_WARN("get_json_doc failed", K(ret));
    SET_COVER_ERROR(ret);
  } else if (has_null) {
    j_base = &j_null;
  }

  ObIJsonBase *j_patch_node = NULL;
  int tmp_ret = OB_SUCCESS;
  if ((!is_cover_error && OB_FAIL(ret)) || has_null) {
    // do nothing
  } else if ((tmp_ret = ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 1, j_patch_node, has_null, true, false)) != OB_SUCCESS) {
    ret = tmp_ret;
    is_cover_error = false;
    if (tmp_ret == OB_ERR_JSON_SYNTAX_ERROR) {
      ret = OB_ERR_JSON_PATCH_INVALID;
    }
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (has_null) {
    ret = OB_ERR_JSON_PATCH_INVALID;
    LOG_USER_ERROR(OB_ERR_JSON_PATCH_INVALID);
  } else if (j_patch_node->json_type() != ObJsonNodeType::J_OBJECT) {
    j_base = j_patch_node;
  } else if (OB_FAIL(ret)) {
  } else {
    ObJsonObject *j_obj = NULL;
    if (j_base->json_type() == ObJsonNodeType::J_OBJECT) {
      j_obj = static_cast<ObJsonObject *>(j_base);
    } else {
      void *buf = temp_allocator.alloc(sizeof(ObJsonObject));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SET_COVER_ERROR(ret);
        LOG_WARN("error, json merge patch allocate jsonobject buffer failed", K(ret));
      } else {
        j_obj = new (buf) ObJsonObject(&temp_allocator);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(j_obj->merge_patch(&temp_allocator, static_cast<ObJsonObject*>(j_patch_node)))) {
        LOG_WARN("error, json merge patch failed", K(ret));
        SET_COVER_ERROR(ret);
      } else {
        j_base = j_obj;
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (is_cover_error && err_type) {
      res.set_null();
      ret = OB_SUCCESS;
    }
  } else if (OB_SUCC(ret)) {
    if (has_null) {
      res.set_null();
    } else {
      ObJsonBuffer* jbuf = nullptr;
      ObString res_string;
      bool is_res_blob = expr.datum_meta_.cs_type_ == CS_TYPE_BINARY && dst_type == ObLongTextType;

      if (OB_ISNULL( jbuf = OB_NEWx(ObJsonBuffer, &temp_allocator, &temp_allocator))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to construct jbuf", K(ret));
      } else if (dst_type == ObJsonType) {
        if (OB_FAIL(j_base->get_raw_binary(res_string, &temp_allocator))) {
          LOG_WARN("failed: get json raw binary", K(ret));
        }
      } else {
        ObString tmp_val;
        ObString result_str;
        bool is_quote = j_base->json_type() == ObJsonNodeType::J_STRING;

        if (OB_FAIL(j_base->print(*jbuf, is_quote, is_pretty > 0))) {
          LOG_WARN("json binary to string failed", K(ret));
        } else if (jbuf->empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("jbuf should not empty", K(ret));
        } else {
          tmp_val = jbuf->string();
          ObCollationType in_cs_type = CS_TYPE_UTF8MB4_BIN;
          ObCollationType dst_cs_type = expr.obj_meta_.get_collation_type();
          result_str = tmp_val;

          if (OB_FAIL(ObJsonExprHelper::convert_string_collation_type(in_cs_type,
                                                                      dst_cs_type,
                                                                      &temp_allocator,
                                                                      tmp_val,
                                                                      result_str))) {
            LOG_WARN("fail to convert string result", K(ret));
          } else {
            tmp_val = result_str;
          }
        }


        if (OB_SUCC(ret) && is_asc && !is_res_blob /* clob varchar */ ) {
          if (OB_FAIL(ObJsonExprHelper::character2_ascii_string(&temp_allocator, expr, ctx, tmp_val, 1))) {
            LOG_WARN("fail to transform string 2 ascii character", K(ret));
          }
        }


        if (is_trunc && dst_type != ObLongTextType) {
          if (tmp_val.length() > dst_len) {
            if (ob_is_string_type(dst_type)) {
              int64_t char_len; // not used
              int64_t real_dst_len;
              real_dst_len = ObCharset::max_bytes_charpos(expr.datum_meta_.cs_type_, tmp_val.ptr(),
                                                          tmp_val.length(), dst_len, char_len);

              tmp_val.assign_ptr(tmp_val.ptr(), real_dst_len);
              // compact with oracle:
              if (real_dst_len != dst_len && real_dst_len > 0) {
                // get last char
                // must be utf8
                bool append_quote = false;
                int64_t last_char_len = 0;
                if (OB_FAIL(ObCharset::last_valid_char(expr.datum_meta_.cs_type_,
                                                       tmp_val.ptr(), tmp_val.length(),
                                                       last_char_len))) {
                  LOG_WARN("failed to get last char", K(ret), K(expr.datum_meta_.cs_type_), K(tmp_val));
                } else if (last_char_len == 1) {
                  if (real_dst_len > 1) {
                    if (OB_FAIL(ObCharset::last_valid_char(expr.datum_meta_.cs_type_,
                                                          tmp_val.ptr(), tmp_val.length() - 1,
                                                          last_char_len))) {
                      LOG_WARN("failed to get second last char", K(ret), K(expr.datum_meta_.cs_type_), K(tmp_val));
                    } else if (last_char_len == 1) {
                      if ((tmp_val.ptr()[real_dst_len - 1] == '"') && (tmp_val.ptr()[real_dst_len - 2] != '"')) {
                        append_quote = true;
                      }
                    }
                  } else {
                    append_quote = true;
                  }
                }
                if (OB_FAIL(ret)) {
                } else if (append_quote) {
                  // already reserve 1 byte calling character2_ascii_string
                  *(tmp_val.ptr() + tmp_val.length()) = '\"';
                  tmp_val.assign_ptr(tmp_val.ptr(), tmp_val.length() + 1);
                }
              }
            } else {
              tmp_val.assign_ptr(tmp_val.ptr(), dst_len);
            }
          }
        }
        res_string.assign_ptr(tmp_val.ptr(), tmp_val.length());
      }

      if (OB_SUCC(ret)) {
        uint64_t length = res_string.length();
        if (dst_type == ObVarcharType && length > dst_len) {
          char res_ptr[OB_MAX_DECIMAL_PRECISION] = {0};
          if (OB_ISNULL(ObCharset::lltostr(dst_len, res_ptr, 10, 1))) {
            LOG_WARN("failed to lltostr", K(ret), K(dst_len));
          }
          if (!err_type) {
            ret = OB_ERR_VALUE_EXCEEDED_MAX;
            LOG_USER_ERROR(OB_ERR_VALUE_EXCEEDED_MAX, static_cast<int>(length), static_cast<int>(dst_len));
          } else {
            ret = OB_SUCCESS;
            res.set_null();
          }
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, res_string))) {
          LOG_WARN("fail to pack ressult.", K(ret));
        }
      }
    }
  }


  return ret;
}

int ObExprJsonMergePatch::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_mysql_mode()) {
    rt_expr.eval_func_ = eval_json_merge_patch;
  } else {
    rt_expr.eval_func_ = eval_ora_json_merge_patch;
  }
  return OB_SUCCESS;
}

}
}
