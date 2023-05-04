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
 * This file contains implementation for json_replace.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_replace.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonReplace::ObExprJsonReplace(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_REPLACE, N_JSON_REPLACE, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonReplace::~ObExprJsonReplace()
{
}

int ObExprJsonReplace::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3 || param_num % 2 == 0)) {
    ret = OB_ERR_PARAM_SIZE;
    ObString func_name_(N_JSON_REPLACE);
    LOG_USER_ERROR(OB_ERR_PARAM_SIZE, func_name_.length(), func_name_.ptr());
  } else {
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_REPLACE))) {
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

int ObExprJsonReplace::eval_json_replace(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
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
  if (OB_SUCC(ret)) {
    path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);
  }
  
  for (int64_t i = 1; OB_SUCC(ret) && !is_null_result && i < expr.arg_cnt_; i+=2) {
    ObJsonBaseVector hit;
    ObDatum *path_data = NULL;
    if (expr.args_[i]->datum_meta_.type_ == ObNullType) {
      is_null_result = true;
      break;
    } else if (OB_FAIL(expr.args_[i]->eval(ctx, path_data))) {
      LOG_WARN("eval json path datum failed", K(ret));
    } else {
      ObString path_val = path_data->get_string();
      ObJsonPath *json_path;
      if (OB_FAIL(ObTextStringHelper::read_real_string_data(temp_allocator, *path_data,
                  expr.args_[i]->datum_meta_, expr.args_[i]->obj_meta_.has_lob_header(), path_val))) {
        LOG_WARN("fail to get real data.", K(ret), K(path_val));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, json_path, path_val, i, false))) {
        ret = OB_ERR_INVALID_JSON_PATH;
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
      } else if (OB_FAIL(json_doc->seek(*json_path, json_path->path_node_cnt(),
                                        true, false, hit))) {
        LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
      }
    }

    if (OB_SUCC(ret) && !is_null_result) {
      ObIJsonBase *json_val = NULL;
      if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &temp_allocator,
                                                 i+1, json_val))) {
        LOG_WARN("get_json_val failed", K(ret));
      }

      // replace 
      int32_t hits = hit.size();
      if (hits == 0) {
        // do nothing
      } else if (hits != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Input path seek failed", K(ret));
      } else {
        if (OB_FAIL(ObJsonExprHelper::json_base_replace(hit[0], json_val, json_doc))) {
          LOG_WARN("json_base_replace failed", K(ret));
        }
      }
    }
  }

  // set result
  if (OB_UNLIKELY(OB_FAIL(ret))) {
    LOG_WARN("Json parse and seek failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    ObString str;
    if (OB_FAIL(json_doc->get_raw_binary(str, &temp_allocator))) {
      LOG_WARN("json_replace result to binary failed", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, str))) {
      LOG_WARN("fail to pack json result", K(ret));
    }
  }
  return ret;
}

int ObExprJsonReplace::cg_expr(ObExprCGCtx &expr_cg_ctx,
                               const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_replace;
  return OB_SUCCESS;
}

}
}