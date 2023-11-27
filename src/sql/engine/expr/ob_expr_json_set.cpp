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
 * This file is for implementation of func json_set
 */

#define USING_LOG_PREFIX SQL_ENG
#include "deps/oblib/src/lib/json_type/ob_json_path.h"
#include "ob_expr_json_func_helper.h"
#include "ob_expr_json_set.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonSet::ObExprJsonSet(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_SET, N_JSON_SET, MORE_THAN_ONE, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonSet::~ObExprJsonSet()
{
}

int ObExprJsonSet::calc_result_typeN(ObExprResType& type,
                                     ObExprResType* types_stack,
                                     int64_t param_num,
                                     ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3 || param_num % 2 == 0)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString func_name_(N_JSON_SET);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_SET))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
    }

    for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i+=2) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, i))) {
        LOG_WARN("wrong type for json path.", K(ret), K(types_stack[i].get_type()));
      } else {
        ObJsonExprHelper::set_type_for_value(types_stack, i+1);
      }
    }
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  }
  return ret;
}                   

int ObExprJsonSet::set_value(ObJsonSeekResult &hit, ObIJsonBase *&json_doc, ObIJsonBase* json_val,
                             ObJsonPath *json_path, ObIAllocator *allocator)
{
  INIT_SUCC(ret);

  int32_t hit_cnt = hit.size();
  if (hit_cnt == 1) {
    if (OB_FAIL(ObJsonExprHelper::json_base_replace(hit[0], json_val, json_doc))) {
      LOG_WARN("json_base_replace failed", K(ret));
    }
  } else if (hit_cnt == 0) {
    // seek again
    if (OB_FAIL(json_doc->seek(*json_path, json_path->path_node_cnt() - 1, true, true, hit))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit.size() != 0) {
      ObIJsonBase* pos_node = hit.last();
      ObJsonPathBasicNode* path_last = json_path->last_path_node();
      if (path_last->get_node_type() == JPN_ARRAY_CELL) {
        if (pos_node->json_type() == ObJsonNodeType::J_ARRAY) {
          uint64_t arr_len = pos_node->element_count();
          ObJsonArrayIndex array_index;
          if (OB_FAIL(path_last->get_first_array_index(arr_len, array_index))) {
            LOG_WARN("error, get array index failed", K(ret), K(arr_len));
          } else if (json_doc->is_bin() && ! json_val->is_bin() && OB_FAIL(ObJsonBaseFactory::transform(allocator, json_val, ObJsonInType::JSON_BIN, json_val))) {
            LOG_WARN("json tree to bin fail", K(ret));
          } else if (OB_FAIL(pos_node->array_insert(array_index.get_array_index(), json_val))) {
            LOG_WARN("error, insert array node failed", K(ret), K(array_index.get_array_index()));
          }
        } else if (!path_last->is_autowrap()) {
          void* array_buf = allocator->alloc(sizeof(ObJsonArray));
          if (OB_ISNULL(array_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("error, alloc jsonarray node failed", K(ret));
          } else {
            ObJsonArray* json_array = (ObJsonArray*)new(array_buf)ObJsonArray(allocator);
            ObIJsonBase *j_parent = nullptr;
            ObIJsonBase *j_array = json_array;
            ObIJsonBase *j_pos_node = pos_node;
            bool is_idx_from_end = path_last->node_content_.array_cell_.is_index_from_end_;
            if (OB_FAIL(pos_node->get_parent(j_parent))) {
              LOG_WARN("get_parent fail", K(ret), KPC(pos_node));
            } else if (! pos_node->is_tree() && OB_FAIL(ObJsonBaseFactory::transform(allocator, pos_node, ObJsonInType::JSON_TREE, j_pos_node))) {
              LOG_WARN("json tree to bin fail", K(ret));
            } else if (!is_idx_from_end && (OB_FAIL(json_array->array_append(j_pos_node))
                || OB_FAIL(json_array->array_append(json_val)))) {
              LOG_WARN("error, array append node failed", K(ret));
            } else if (is_idx_from_end && (OB_FAIL(json_array->array_append(json_val))
                || OB_FAIL(json_array->array_append(j_pos_node)))) {
              LOG_WARN("error, array append node failed", K(ret));
            } else if (OB_ISNULL(j_parent)){
              json_doc->reset();
              json_doc = json_array;
            } else if (j_parent->is_bin() && OB_FAIL(ObJsonBaseFactory::transform(allocator, j_array, ObJsonInType::JSON_BIN, j_array))) {
              LOG_WARN("json tree to bin fail", K(ret));
            } else if (OB_FAIL(j_parent->replace(pos_node, j_array))) {
              LOG_WARN("replace fail", K(ret), KPC(pos_node), KPC(j_array));
            }
          }
        }
      } else if (json_doc->is_bin() && ! json_val->is_bin() && OB_FAIL(ObJsonBaseFactory::transform(allocator, json_val, ObJsonInType::JSON_BIN, json_val))) {
        LOG_WARN("json tree to bin fail", K(ret));
      } else if (path_last->get_node_type() == JPN_MEMBER
                 && pos_node->json_type() == ObJsonNodeType::J_OBJECT) {
        ObString key_name;
        key_name.assign_ptr(path_last->get_object().object_name_, path_last->get_object().len_);
        if (OB_FAIL(pos_node->object_add(key_name, json_val))) {
          LOG_WARN("error, json object add kv pair failed", K(ret));
        }
      } else {}
      if (OB_SUCC(ret) && OB_FAIL(ObJsonExprHelper::refresh_root_when_bin_rebuild_all(json_doc))) {
        LOG_WARN("refresh_root_when_bin_rebuild_all fail", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Input path seek failed", K(ret));
  }

  return ret;
}

int ObExprJsonSet::eval_json_set(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
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
  if (OB_SUCC(ret)) {
    path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
  }
  
  for (int64_t i = 1; OB_SUCC(ret) && !is_null_result && i < expr.arg_cnt_; i+=2) {
    ObJsonSeekResult hit;
    ObDatum *path_data = NULL;
    ObJsonPath *json_path = NULL;
    if (expr.args_[i]->datum_meta_.type_ == ObNullType) {
      is_null_result = true;
      break;
    } else if (OB_FAIL(expr.args_[i]->eval(ctx, path_data))) {
      LOG_WARN("eval json path datum failed", K(ret));
    } else {
      ObString path_val = path_data->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx, temp_allocator, path_val, is_null_result))) {
        LOG_WARN("fail to get real data.", K(ret), K(path_val));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, json_path, path_val, i, false))) {
        LOG_WARN("get json path cache failed", K(path_data->get_string()), K(ret));
      } else if (OB_FAIL(json_doc->seek(*json_path, json_path->path_node_cnt(), true, false, hit))) {
        LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
      }
    }

    if (OB_SUCC(ret) && !is_null_result) {
      ObIJsonBase *json_val = NULL;
      if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator, i+1, json_val))) {
        LOG_WARN("get_json_val failed", K(ret));
      } else if (OB_FAIL(set_value(hit, json_doc, json_val, json_path, &temp_allocator))) {
        LOG_WARN("set_json_value failed", K(ret));
      }
    }
  }

  // set result
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    LOG_WARN("Json parse and seek failed", K(ret));
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

int ObExprJsonSet::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                           ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObJsonExprHelper::init_json_expr_extra_info(expr_cg_ctx.allocator_, raw_expr, type_, rt_expr))) {
    LOG_WARN("init_json_expr_extra_info fail", K(ret));
  } else {
    rt_expr.eval_func_ = eval_json_set;
  }
  return ret;
}

}
}