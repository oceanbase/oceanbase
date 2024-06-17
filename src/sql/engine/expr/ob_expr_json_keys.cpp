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
 * This file contains implementation for json_keys.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_keys.h"
#include "ob_expr_json_func_helper.h"
#include "share/ob_json_access_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonKeys::ObExprJsonKeys(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_KEYS, N_JSON_KEYS, ONE_OR_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonKeys::~ObExprJsonKeys()
{
}

int ObExprJsonKeys::calc_result_typeN(ObExprResType& type,
                                      ObExprResType* types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx); 
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num > 2)) {
    ObString func_name_(N_JSON_KEYS);
    ret = OB_ERR_PARAM_SIZE;
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    // set result to json
    type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
    // set type of json_doc
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_KEYS))) {
      LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
    }

    // set type of json_path
    if (OB_SUCC(ret) && param_num == 2) {
      if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, 1))) {
        LOG_WARN("wrong type for json path.", K(ret), K(types_stack[1].get_type()));
      }
    }

  }
  return ret;
}

int ObExprJsonKeys::get_keys_from_wrapper(ObIJsonBase *json_doc, 
                                          ObIAllocator *allocator,
                                          ObString &str)
{
  INIT_SUCC(ret);
  ObJsonArray res_array(allocator);
  JsonObjectIterator iter = json_doc->object_iterator();
  while (!iter.end() && OB_SUCC(ret)) {
    ObString key;
    if (OB_FAIL(iter.get_key(key))) {
      LOG_WARN("fail to get key from iterator", K(ret));
    } else {
      void *node = allocator->alloc(sizeof(ObJsonString));
      if (OB_ISNULL(node)){
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("json_keys alloc jsonString failed", K(ret), K(str.length()));
      } else {
        ObJsonString *jstr = new (node) ObJsonString(key.ptr(), key.length());
        if (OB_FAIL(res_array.append(jstr))) {
          LOG_WARN("json_keys array append failed", K(ret), K(str.length()));
        }
      }
    }
    iter.next();
  }
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObJsonWrapper::get_raw_binary(&res_array, str, allocator))) {
    LOG_WARN("json_keys get result binary failed", K(ret));
  }
  return ret;
}

int ObExprJsonKeys::eval_json_keys(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
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
                                                    json_doc, is_null_result, false))) {
    LOG_WARN("get_json_doc failed", K(ret));
  }

  if (!is_null_result && OB_SUCC(ret) && expr.arg_cnt_ == 2) {
    // get json_path
    ObJsonPathCache ctx_cache(&temp_allocator);
    ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

    ObDatum *path_data = NULL;
    if (expr.args_[1]->datum_meta_.type_ == ObNullType) {
      is_null_result = true;
    } else if (OB_FAIL(expr.args_[1]->eval(ctx, path_data))) {
      LOG_WARN("eval json path datum failed", K(ret));
    } else {
      ObJsonSeekResult sub_json_targets;
      ObJsonPath *json_path;
      ObString path_val = path_data->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[1], ctx, temp_allocator, path_val, is_null_result))) {
        LOG_WARN("fail to get real data.", K(ret), K(path_val));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, json_path, path_val, 1, false))) {
        LOG_WARN("json parse failed", K(path_data->get_string()), K(ret));
      } else if (OB_FAIL(json_doc->seek(*json_path, json_path->path_node_cnt(),
                                        false, true, sub_json_targets))) {
        LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
      } else {
        if (sub_json_targets.size() != 1) {
          is_null_result = true;
        } else {
          json_doc = sub_json_targets[0];
        }
      }
    }
  }

  if (!is_null_result && OB_SUCC(ret) && !OB_ISNULL(json_doc) && json_doc->json_type() != ObJsonNodeType::J_OBJECT) {
    is_null_result = true;
  }

  // set result
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    LOG_WARN("json_keys failed", K(ret));
  } else if (is_null_result){
    res.set_null();
  } else {
    ObString str;
    if (OB_FAIL(get_keys_from_wrapper(json_doc, &temp_allocator, str))) {
      LOG_WARN("get_keys_from_wrapper failed", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, str))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }

  return ret;
}

int ObExprJsonKeys::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_keys;
  return OB_SUCCESS;
}


}
}