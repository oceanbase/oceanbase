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
 * This file is for implement of func json_array_array_insert
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_array_insert.h"
#include "share/ob_json_access_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonArrayInsert::ObExprJsonArrayInsert(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_ARRAY_INSERT,
      N_JSON_ARRAY_INSERT, 
      MORE_THAN_TWO,
      VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonArrayInsert::~ObExprJsonArrayInsert()
{
}

int ObExprJsonArrayInsert::calc_result_typeN(ObExprResType& type, ObExprResType* types_stack,
    int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);

  // param_num is odd，JSON_ARRAY_INSERT(json_doc, path, val[, path, val] ...)
  if (OB_UNLIKELY(param_num <= 1 || (param_num & 1 ) == 0)) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name("json_array_insert");
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_ARRAY_INSERT))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i += 2) {
        //path type
        if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, i))) {
          LOG_WARN("wrong type for json path.", K(ret), K(types_stack[i].get_type()));
        }
        
        if (OB_SUCC(ret)) {
            // value type
          if (types_stack[i+1].get_type() == ObNullType) {
            // do nothing
          } else if (ob_is_string_type(types_stack[i+1].get_type())) {
            if (types_stack[i+1].get_charset_type() != CHARSET_UTF8MB4) {
              types_stack[i+1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
            }
          } else if  (types_stack[i+1].get_type() == ObJsonType) {
            types_stack[i+1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      }
    }
  }
  return ret;
}

int ObExprJsonArrayInsert::eval_json_array_insert(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObIJsonBase *j_base = NULL;
  bool is_null = false;
  ObJsonSeekResult hit;

  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, j_base, is_null))) {
    LOG_WARN("get_json_doc failed", K(ret));
  }

  ObJsonPathCache ctx_cache(&temp_allocator);
  ObJsonPathCache* path_cache = NULL;
  if (OB_SUCC(ret) && !is_null) {
    path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
  }

  for (int32 i = 1; OB_SUCC(ret) && i < expr.arg_cnt_ && !is_null; i += 2) {
    hit.reset();
    ObExpr *arg = expr.args_[i];
    ObDatum *json_datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, json_datum))) {
      LOG_WARN("failed: eval json path datum.", K(ret));
    } else if (json_datum->is_null() || arg->datum_meta_.type_ == ObNullType) {
      is_null = true;
    } else {
      ObString j_path_text = json_datum->get_string();
      ObJsonPath *j_path;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *json_datum,
                  arg->datum_meta_, arg->obj_meta_.has_lob_header(), j_path_text))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, i, false))) {
        LOG_WARN("parse text to path failed", K(j_path_text), K(ret));
      } else if (j_path->path_node_cnt() == 0
          || j_path->last_path_node()->get_node_type() != JPN_ARRAY_CELL) {
        ret = OB_ERR_INVALID_JSON_PATH_ARRAY_CELL;
        LOG_WARN("error, path illegal, last path isn't array member", K(ret), K(j_path_text));
      } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt() - 1, false, true, hit))) {
        LOG_WARN("json seek failed", K(j_path_text), K(ret));
      } else if (hit.size() == 0) {
        // do nothing
      } else {
        ObJsonPathBasicNode* path_node = j_path->last_path_node();
        ObIJsonBase *j_pos_node = hit[0];
        if (j_pos_node->json_type() == ObJsonNodeType::J_ARRAY) {
          ObIJsonBase *j_val = NULL;
          if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, j_val))) {
            ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
            LOG_WARN("failed: get_json_val", K(ret));
          } else {
            ObJsonArrayIndex array_index;
            if (OB_FAIL(path_node->get_first_array_index(j_pos_node->element_count(), array_index))) {
              LOG_WARN("failed: get first array index", K(ret), K(j_pos_node->element_count()));
            } else if (OB_FAIL(j_pos_node->array_insert(array_index.get_array_index(), j_val))) {
              LOG_WARN("failed: array insert", K(ret), K(array_index.get_array_index()));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObString raw_bin;
    if (is_null) {
      res.set_null();
    } else if (OB_FAIL(ObJsonWrapper::get_raw_binary(j_base, raw_bin, &temp_allocator))) {
      LOG_WARN("failed: get json raw binary", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }

  return ret;
}

int ObExprJsonArrayInsert::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_array_insert;
  return OB_SUCCESS;
}

}
}
