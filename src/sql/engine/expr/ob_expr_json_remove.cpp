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
 * This file contains implementation for json_remove.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "deps/oblib/src/lib/json_type/ob_json_path.h"
#include "ob_expr_json_remove.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace oceanbase
{
namespace sql
{
ObExprJsonRemove::ObExprJsonRemove(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_REMOVE, N_JSON_REMOVE, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonRemove::~ObExprJsonRemove()
{
}

int ObExprJsonRemove::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;

  // json doc
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_REMOVE))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
  }
  // json path
  for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, i))) {
      LOG_WARN("wrong type for json path.", K(ret), K(types_stack[i].get_type()));
    }
  }
  type.set_json();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  return ret;
}

static int remove_from_json(ObJsonPath *path_node, ObIJsonBase *child)
{
  INIT_SUCC(ret);
  // remove item in hits
  ObJsonPathBasicNode* last_node = path_node->last_path_node();
  // get node to be removed
  ObJsonNodeType type;
  ObIJsonBase* parent = nullptr;
  if (OB_FAIL(child->get_parent(parent)) || OB_ISNULL(parent)) {
    // may be null parent
    ret = OB_SUCCESS;
  } else if (FALSE_IT(type = parent->json_type())) {
  } else if (type == ObJsonNodeType::J_OBJECT && last_node->get_node_type() == JPN_MEMBER) {
    ObPathMember member = last_node->get_object();
    ObString key(member.len_, member.object_name_);
    if (OB_FAIL(parent->object_remove(key))) {
      LOG_WARN("fail to remove json_object node", K(ret));
    }
  } else if (type == ObJsonNodeType::J_ARRAY && last_node->get_node_type() == JPN_ARRAY_CELL) {
    ObJsonArrayIndex array_index;
    if (OB_FAIL(last_node->get_first_array_index(parent->element_count(), array_index))) {
      LOG_WARN("error, get array index failed", K(ret));
    } else if (array_index.is_within_bounds() && OB_FAIL(parent->array_remove(array_index.get_array_index()))) {
      LOG_WARN("fail to remove json_array node", K(ret));
    }
  }
  return ret;
}

int ObExprJsonRemove::eval_json_remove(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *json_doc = NULL;

  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  if (expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid out put charset", K(ret), K(expr.datum_meta_.cs_type_));
  } else if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0,
                                                    json_doc, is_null_result))) {
    LOG_WARN("get_json_doc failed", K(ret));
  }

  ObJsonPathCache ctx_cache(&temp_allocator);
  ObJsonPathCache* path_cache = NULL;
  if (OB_SUCC(ret) && !is_null_result) {
    path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
  }
  
  ObJsonSeekResult hits;
  for (int64_t i = 1; OB_SUCC(ret) && !is_null_result && i < expr.arg_cnt_; i++) {
    hits.clear();
    ObDatum *path_data = NULL;
    if (expr.args_[i]->datum_meta_.type_ == ObNullType) {
      is_null_result = true;
    } else if (OB_FAIL(expr.args_[i]->eval(ctx, path_data))) {
      ret = OB_ERR_INVALID_JSON_PATH;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
    } else {
      ObString path_val = path_data->get_string();
      ObJsonPath *json_path;
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx, temp_allocator, path_val, is_null_result))) {
        LOG_WARN("fail to get real data.", K(ret), K(path_val));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, json_path, path_val, i, false))) {
        LOG_WARN("parse text to path failed", K(path_data->get_string()), K(ret));
      } else if (json_path->path_node_cnt() == 0) {
        ret = OB_ERR_JSON_VACUOUS_PATH;
        LOG_USER_ERROR(OB_ERR_JSON_VACUOUS_PATH); 
      } else if (OB_FAIL(json_doc->seek(*json_path, json_path->path_node_cnt(), true, false, hits))) {
        LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
      } else if (hits.size() == 0){
        continue;
      } else if (hits.size() > 1){
        ret = OB_INVALID_ERROR;
        LOG_WARN("More than one results after seek with only_need_one mode.", K(ret));
      } else {
        if (OB_FAIL(remove_from_json(json_path, hits[0]))) {
          LOG_WARN("remove_from_json failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::refresh_root_when_bin_rebuild_all(json_doc))) {
          LOG_WARN("refresh_root_when_bin_rebuild_all fail", K(ret));
        }
      }
    }
  }

  // set result
  if (OB_FAIL(ret)) {
    LOG_WARN("json_remove failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_res(expr, ctx, temp_allocator, json_doc, res))) {
    LOG_WARN("pack fail", K(ret));
  }
  if (OB_NOT_NULL(json_doc)) {
    json_doc->reset();
  }
  return ret;
}

int ObExprJsonRemove::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObJsonExprHelper::init_json_expr_extra_info(expr_cg_ctx.allocator_, raw_expr, type_, rt_expr))) {
    LOG_WARN("init_json_partial_update_extra_info fail", K(ret));
  } else {
    rt_expr.eval_func_ = eval_json_remove;
  }
  return ret;
}

}
}