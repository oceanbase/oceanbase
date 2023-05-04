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
 * This file contains implementation for json_extract.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_extract.h"
#include "ob_expr_json_func_helper.h"
#include "lib/json_type/ob_json_tree.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonExtract::ObExprJsonExtract(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_EXTRACT, N_JSON_EXTRACT, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonExtract::~ObExprJsonExtract()
{
}

int ObExprJsonExtract::calc_result_typeN(ObExprResType& type,
                                         ObExprResType* types_stack,
                                         int64_t param_num,
                                         ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 2)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid argument number", K(ret), K(param_num));
  } else {
    // 1st param is json doc
    ObObjType in_type = types_stack[0].get_type();
    bool is_null_result = false;

    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_EXTRACT))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(in_type));
    } else if (in_type == ObNullType) {
      is_null_result = true;
    } else if (in_type == ObJsonType) {
      // do nothing
    } else if (ob_is_string_type(in_type) && types_stack[0].get_collation_type() != CS_TYPE_BINARY) {
      if (types_stack[0].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[0].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }

    // following params are path strings
    for (int64_t i = 1; i < param_num && OB_SUCC(ret); i++) {
      if (types_stack[i].get_type() == ObNullType) {
        is_null_result = true;
      } else if (ob_is_string_type(types_stack[i].get_type())) {
        if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else {
        types_stack[i].set_calc_type(ObLongTextType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }

    if (OB_SUCC(ret)) {
      if (is_null_result) {
        type.set_null();
      } else {
        type.set_json();
        type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
      }
    }
  }
  return ret;
}

int ObExprJsonExtract::eval_json_extract_null(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  UNUSED(expr);
  UNUSED(ctx);
  res.set_null();
  return OB_SUCCESS;
}

int ObExprJsonExtract::eval_json_extract(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[0];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
  ObIJsonBase *j_base = NULL;
  bool is_null_result = false;
  bool may_match_many = (expr.arg_cnt_ > 2);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (json_datum->is_null()) {
    is_null_result = true; // mysql return NULL result
  } else if (val_type != ObJsonType && ob_is_string_type(val_type) == false) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_WARN("input type error", K(val_type));
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(val_type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(val_type), K(cs_type));
  } else {
    ObString j_str = json_datum->get_string();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(val_type);
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, allocator, j_str, is_null_result))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_str, j_in_type, j_in_type, j_base))) {
      LOG_WARN("fail to get json base", K(ret), K(j_in_type));
      ret = OB_ERR_INVALID_JSON_TEXT;
    }
  }

  if (OB_UNLIKELY(OB_FAIL(ret))) {
    if (ret == OB_ERR_INVALID_TYPE_FOR_JSON) {
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, "json_extract");
    } else if (ret == OB_ERR_INVALID_JSON_CHARSET) {
    } else {
      ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
    }
    LOG_WARN("fail to handle json param 0 in json extract in new sql engine", K(ret));
  } else if (is_null_result ==  false) {
    ObJsonBaseVector hit;
    ObJsonPathCache ctx_cache(&allocator);
    ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
    for (int64_t i = 1; OB_SUCC(ret) && (!is_null_result) && i < expr.arg_cnt_; i++) {
      ObDatum *path_data = NULL;
      if (OB_FAIL(expr.args_[i]->eval(ctx, path_data))) {
        LOG_WARN("eval json path datum failed", K(ret));
      } else if (path_data->is_null()) {
        is_null_result = true;
      } else {
        ObString path_text = path_data->get_string();
        ObJsonPath *j_path = NULL;
        if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx, allocator, path_text, is_null_result))) {
          LOG_WARN("fail to get real data.", K(ret), K(path_text));
        } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, path_text, i, true))) {
          LOG_WARN("parse text to path failed", K(path_text), K(ret));
        } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, false, hit))) {
          LOG_WARN("json seek failed", K(path_text), K(ret));
        } else {
          if (j_path->can_match_many()) {
            may_match_many = true;
          }
        }
      }
    }

    int32_t hit_size = hit.size();
    ObJsonArray j_arr_res(&allocator);
    ObIJsonBase *jb_res = NULL;
    if (OB_UNLIKELY(OB_FAIL(ret))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit_size == 0 || is_null_result) {
      res.set_null();
    } else {
      if (hit_size == 1 && (may_match_many == false)) {
        jb_res = hit[0];
      } else {
        jb_res = &j_arr_res;
        ObJsonNode *j_node = NULL;
        ObIJsonBase *jb_node = NULL;
        for (int32_t i = 0; OB_SUCC(ret) && i < hit_size; i++) {
          if (ObJsonBaseFactory::transform(&allocator, hit[i], ObJsonInType::JSON_TREE, jb_node)) { // to tree
            LOG_WARN("fail to transform to tree", K(ret), K(i), K(*(hit[i])));
          } else {
            j_node = static_cast<ObJsonNode *>(jb_node);
            if (OB_FAIL(jb_res->array_append(j_node->clone(&allocator)))) {
              LOG_WARN("result array append failed", K(ret), K(i), K(*j_node));
            }
          }
        }
      }

      ObString raw_str;
      if (OB_FAIL(ret)) {
        LOG_WARN("json extarct get results failed", K(ret));
      } else if (OB_FAIL(jb_res->get_raw_binary(raw_str, &allocator))) {
        LOG_WARN("json extarct get result binary failed", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
        LOG_WARN("fail to pack json result", K(ret));
      }
    }
  } else if (OB_SUCC(ret) && is_null_result) {
    res.set_null();
  }

  return ret;
}

int ObExprJsonExtract::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (rt_expr.datum_meta_.type_ == ObNullType) {
      rt_expr.eval_func_ = eval_json_extract_null;
  } else {
      rt_expr.eval_func_ = eval_json_extract;
  }
  return OB_SUCCESS;
}

}
}
