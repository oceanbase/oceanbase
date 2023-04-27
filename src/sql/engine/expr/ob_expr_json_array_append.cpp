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
 * This file is for implementation of func json_array_append
 */


#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_array_append.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonArrayAppend::ObExprJsonArrayAppend(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_ARRAY_APPEND,
      N_JSON_ARRAY_APPEND, 
      MORE_THAN_TWO,
      VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonArrayAppend::~ObExprJsonArrayAppend()
{
}

int ObExprJsonArrayAppend::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);

  // param_num is odd，JSON_ARRAY_APPEND(json_doc, path, val[, path, val] ...)
  if (OB_UNLIKELY(param_num <= 1 || (param_num & 1 ) == 0)) {
    ret = OB_ERR_PARAM_SIZE;
    const ObString name(N_JSON_ARRAY_APPEND);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_ARRAY_APPEND))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
    } else {
      for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i += 2) {
        //path type
        if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, i))) {
          LOG_WARN("wrong type for json path.", K(ret), K(types_stack[i].get_type()));
        }
        if (OB_SUCC(ret)) {
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


int ObExprJsonArrayAppend::eval_json_array_append(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObIJsonBase *j_base = NULL;
  bool is_null = false;
  ObJsonBaseVector hit;

  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator,
      0, j_base, is_null))) {
    LOG_WARN("get_json_doc failed", K(ret));
  }

  ObJsonPathCache ctx_cache(&temp_allocator);
  ObJsonPathCache *path_cache = NULL;
  if (OB_SUCC(ret) && !is_null) {
    path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
  }

  for (int32 i = 1; OB_SUCC(ret) && i < expr.arg_cnt_ && !is_null; i += 2) {
    ObExpr *arg = expr.args_[i];
    ObDatum *json_datum = NULL;
    hit.reset();
    if (OB_FAIL(expr.args_[i]->eval(ctx, json_datum))) {
      LOG_WARN("failed: eval json path datum.", K(ret));
    } else if (arg->datum_meta_.type_ == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else {
      ObString j_path_text = json_datum->get_string();
      ObJsonPath *j_path = NULL;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(arg, ctx, temp_allocator, j_path_text, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, i, false))) {
        LOG_WARN("failed: parse text to path.", K(j_path_text), K(ret));
      } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, true, hit))) {
        LOG_WARN("failed: json seek failed", K(j_path_text), K(ret));
      } else if (hit.size() == 0) {
        // do nothing
      } else {
        ObIJsonBase *j_val = NULL;
        if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, j_val))) {
          ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
          LOG_WARN("failed: get_json_val.", K(ret), K(i));
        } else {
          // if added position's father is not array(is scaler or object), need pack into array
          // 1. create a new array.
          void *buf = NULL;
          if (OB_ISNULL(buf = temp_allocator.alloc(sizeof(ObJsonArray)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc a new json array", K(ret));
          } else {
            ObIJsonBase *jb_pos_node = hit[0];
            ObJsonNode *j_pos_node = static_cast<ObJsonNode *>(jb_pos_node);
            if (j_pos_node->json_type() == ObJsonNodeType::J_ARRAY) {
              // set new node parent
              if (OB_FAIL(j_pos_node->array_append(j_val))) {
                LOG_WARN("failed: append array value", K(ret), K(*j_val));
              }
            } else {
              // if added position's father is not array(is scaler or object), need pack into array
              // 1. create a new array.
              void *buf = NULL;
              if (OB_ISNULL(buf = temp_allocator.alloc(sizeof(ObJsonArray)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to alloc a new json array", K(ret));
              } else {
                ObJsonArray *j_new_arr = new (buf) ObJsonArray(&temp_allocator);
                ObIJsonBase *jb_new_arr = j_new_arr;
                // 2. append old node and new node to new array.
                ObJsonNode *j_parent = j_pos_node->get_parent();
                ObIJsonBase *jb_parent = j_parent;
                if (OB_FAIL(jb_new_arr->array_append(jb_pos_node))) {
                  LOG_WARN("fail to append pos node to new array", K(ret), K(*jb_pos_node));
                } else if (OB_FAIL(jb_new_arr->array_append(j_val))) {
                  LOG_WARN("fail to append new node to new array", K(ret), K(*j_val));
                } else if (OB_ISNULL(jb_parent)) { // 3.1 root
                  j_base = jb_new_arr;
                } else if (OB_FAIL(jb_parent->replace(jb_pos_node, jb_new_arr))){ // 3.2 not root, replace pos node with new array
                  LOG_WARN("fail to replace pos node with new array", K(ret), K(*jb_new_arr));
                }
              }
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
    } else if (OB_FAIL(j_base->get_raw_binary(raw_bin, &temp_allocator))) {
      LOG_WARN("failed: get json raw binary", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }
  return ret;
}

int ObExprJsonArrayAppend::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_array_append;
  return OB_SUCCESS;
}

}
}
