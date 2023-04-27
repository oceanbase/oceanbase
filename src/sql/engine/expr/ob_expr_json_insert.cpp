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
 * This file is for implement of func json_insert
 */

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_json_insert.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonInsert::ObExprJsonInsert(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_INSERT,
      N_JSON_INSERT, 
      MORE_THAN_TWO,
      VALID_FOR_GENERATED_COL,
      NOT_ROW_DIMENSION)
{
}

ObExprJsonInsert::~ObExprJsonInsert()
{
}

int ObExprJsonInsert::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); // type_ctx session, collation, raw expr, oracle模式下可能需要从type_ctx来判断字符集
  INIT_SUCC(ret);
  const ObString name("json_insert");

  // param_num is odd, and param > 1
  if (OB_UNLIKELY((param_num & 1 ) == 0)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, name.length(), name.ptr());
  } else {
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_INSERT))) {
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

int ObExprJsonInsert::eval_json_insert(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObIJsonBase *j_base = NULL;
  ObDatum *json_datum = NULL;
  bool is_null = false;

  // transform to tree node
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
    ObExpr *arg = expr.args_[i];
    json_datum = NULL;
    if (OB_FAIL(expr.args_[i]->eval(ctx, json_datum))) {
      LOG_WARN("failed: eval json path datum.", K(ret));
    } else if (arg->datum_meta_.type_ == ObNullType || json_datum->is_null()) {
      is_null = true;
    } else {
      ObString j_path_text = json_datum->get_string();
      ObJsonPath *j_path;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(arg, ctx, temp_allocator, j_path_text, is_null))) {
        LOG_WARN("fail to get real data.", K(ret), K(j_path_text));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, i, false))) {
        LOG_WARN("failed: parse text to path.", K(j_path_text), K(ret));
      } else if (j_path->path_node_cnt() == 0) {
        // do nothing
      } else {
        ObJsonBaseVector hit;
        // if target exists continue, don't replace
        if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, true, hit))) {
          LOG_WARN("failed: json seek.", K(j_path_text), K(ret));
        } else if (hit.size()) {
          // do nothing
        } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt() - 1, true, true, hit))) {
          LOG_WARN("failed: json seek.", K(j_path_text), K(ret));
        } else if (hit.size() == 0) {
          // do nothing
        } else {
          ObIJsonBase *j_val;
          if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, j_val))) {
            ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
            LOG_WARN("failed: get_json_val.", K(ret));
          } else {
            ObIJsonBase *j_pos_node = *hit.last();
            ObJsonPathBasicNode *path_last = j_path->last_path_node();
            if (path_last->get_node_type() == JPN_ARRAY_CELL) {
              if (j_pos_node->json_type() == ObJsonNodeType::J_ARRAY) {
                ObJsonArrayIndex array_index;
                size_t length = j_pos_node->element_count();
                if (OB_FAIL(path_last->get_first_array_index(length, array_index))) {
                  LOG_WARN("failed: get array insert.", K(ret), K(length));
                } else if (OB_FAIL(j_pos_node->array_insert(array_index.get_array_index(), j_val))) {
                  LOG_WARN("failed: insert array node.", K(ret));
                }
              } else if (!path_last->is_autowrap()) {
                void *buf = temp_allocator.alloc(sizeof(ObJsonArray));
                if (OB_ISNULL(buf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed: alloc jsonarray node.", K(ret));
                } else {
                  ObJsonArray *j_new_arr = new (buf) ObJsonArray(&temp_allocator);
                  ObIJsonBase *jb_new_arr = j_new_arr;
                  ObJsonNode *j_parent = static_cast<ObJsonNode *>(j_pos_node)->get_parent();
                  ObIJsonBase *jb_parent = j_parent;
                  if (OB_FAIL(jb_new_arr->array_append(j_pos_node))
                      || OB_FAIL(jb_new_arr->array_append(j_val))) {
                    LOG_WARN("failed: array append node.", K(ret), K(*j_pos_node), K(*j_val));
                  } else if (OB_ISNULL(jb_parent)) { // root
                    j_base = jb_new_arr;
                  } else if (OB_FAIL(jb_parent->replace(j_pos_node, jb_new_arr))){ //  not root, replace pos node with new array
                    LOG_WARN("fail to replace pos node with new array", K(ret), K(*jb_new_arr));
                  }
                }
              }
            } else if (path_last->get_node_type() == JPN_MEMBER
                && j_pos_node->json_type() == ObJsonNodeType::J_OBJECT) {
              ObString key;
              key.assign_ptr(path_last->get_object().object_name_, path_last->get_object().len_);
              if (OB_FAIL(j_pos_node->object_add(key, j_val))) {
                LOG_WARN("error, json object add kv pair failed", K(ret), K(*j_val));
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

int ObExprJsonInsert::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_insert;
  return OB_SUCCESS;
}

}
}
