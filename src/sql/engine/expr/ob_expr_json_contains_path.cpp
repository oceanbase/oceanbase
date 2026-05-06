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
 * This file contains implementation for json_contains_path.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_contains_path.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "lib/container/ob_array_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonContainsPath::ObExprJsonContainsPath(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_CONTAINS_PATH, N_JSON_CONTAINS_PATH, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonContainsPath::~ObExprJsonContainsPath()
{
}

int ObExprJsonContainsPath::calc_result_typeN(ObExprResType& type,
                                              ObExprResType* types_stack,
                                              int64_t param_num,
                                              ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  // set result to bool
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);

  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_CONTAINS_PATH))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
  }

  // set type of one or all flag
  if (OB_SUCC(ret)) {
    if (types_stack[1].get_type() == ObNullType) {
    } else if (ob_is_string_type(types_stack[1].get_type())) {
      if (types_stack[1].get_charset_type() != CHARSET_UTF8MB4) {
        types_stack[1].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    } else {
      ret = OB_ERR_JSON_BAD_ONE_OR_ALL_ARG;
      LOG_USER_ERROR(OB_ERR_JSON_BAD_ONE_OR_ALL_ARG);
    }
  }

  for (int64_t i = 1; OB_SUCC(ret) && i < param_num; i++) {
    if (OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, i))) {
      LOG_WARN("wrong type for json path.", K(ret), K(types_stack[i].get_type()));
    }
  }

  return ret;
}

static int validate_contains_path_fast_path(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObJsonPathCache *path_cache,
                                            ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 2; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
    ObString path_str;
    bool is_null_path = false;
    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx, allocator, path_str,
                                                       is_null_path))) {
      LOG_WARN("eval path arg failed", K(ret), K(i));
    } else if (is_null_path) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("null path is not supported", K(ret), K(i));
    } else if (OB_FAIL(ObJsonExprHelper::parse_and_cache_path_keys(path_cache, path_str,
                                                                    allocator, i))) {
      LOG_WARN("parse and cache path keys failed", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
    path_cache->set_fast_path_not_supported();
    ret = OB_SUCCESS;
  } else {
    path_cache->set_fast_path_supported();
  }
  return ret;
}

int ObExprJsonContainsPath::eval_json_contains_path_fast_path(const ObExpr &expr,
                                                               ObEvalCtx &ctx,
                                                               ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *json_datum = nullptr;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();

  if (OB_FAIL(expr.args_[0]->eval(ctx, json_datum))) {
    LOG_WARN("eval json doc failed", K(ret));
  } else if (json_datum->is_null()) {
    res.set_null();
  } else {
    const ObLobCommon &lob = json_datum->get_lob_data();
    ObJsonPathCache *path_cache =
        ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);

    if (!(json_datum->len_ != 0 && !lob.is_mem_loc_ && lob.in_row_) || OB_ISNULL(path_cache)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported fast path", K(ret));
    } else if (path_cache->is_fast_path_unchecked()
               && OB_FAIL(validate_contains_path_fast_path(expr, ctx, path_cache, tmp_allocator))) {
      LOG_WARN("validate fast path failed", K(ret));
    } else if (path_cache->is_fast_path_unsupported()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported fast path because path is complex", K(ret));
    } else {
      ObDatum *one_all_datum = nullptr;
      bool one_flag = true;
      if (OB_FAIL(expr.args_[1]->eval(ctx, one_all_datum))) {
        LOG_WARN("eval one_or_all arg failed", K(ret));
      } else if (one_all_datum->is_null()) {
        res.set_null();
      } else {
        ObString target_str = one_all_datum->get_string();
        if (0 == target_str.case_compare("one")) {
          one_flag = true;
        } else if (0 == target_str.case_compare("all")) {
          one_flag = false;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid one_or_all flag", K(ret), K(target_str));
        }
      }

      if (OB_SUCC(ret) && !one_all_datum->is_null()) {
        ObString json_data;
        json_data.assign_ptr(lob.get_inrow_data_ptr(),
                             static_cast<int32_t>(lob.get_byte_size(json_datum->len_)));

        bool is_contains = false;

        ObJsonBinFastLocator fast_locator;
        if (OB_FAIL(fast_locator.init(json_data.ptr(), json_data.length()))) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("fast locator init failed, fall back", K(ret));
        }
        const ObSEArray<ObJsonPathCache::ObMultiPathEntry, 4> &path_keys_arr =
            path_cache->get_multi_path_keys();
        for (int64_t i = 0; OB_SUCC(ret) && i < path_keys_arr.count(); ++i) {
          if (i > 0 && OB_FAIL(fast_locator.reset_to_root())) {
            LOG_WARN("fast locator reset_to_root failed", K(ret));
          } else {
            const ObJsonPathCache::ObMultiPathEntry &entry = path_keys_arr.at(i);
            ObArrayHelper<ObString> keys(entry.key_cnt, entry.keys, entry.key_cnt);
            char *res_ptr = nullptr;
            int64_t res_len = 0;
            uint8_t res_type = 0;
            int seek_ret = fast_locator.seek(keys, res_ptr, res_len, res_type);
            if (seek_ret == OB_SEARCH_NOT_FOUND) {
              if (!one_flag) {
                is_contains = false;
                break;
              }
            } else if (OB_FAIL(seek_ret)) {
              LOG_WARN("fast locator seek failed", K(ret));
            } else {
              is_contains = true;
              if (one_flag) {
                break;
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          res.set_int(static_cast<int64_t>(is_contains));
        }
      }
    }
  }
  return ret;
}

int ObExprJsonContainsPath::eval_json_contains_path_general_path(const ObExpr &expr,
                                                                  ObEvalCtx &ctx,
                                                                  ObDatum &res)
{
  // get json doc
  int ret = OB_SUCCESS;
  ObIJsonBase *json_target = NULL;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor temp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
  if (OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 0, json_target, is_null_result, false))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else {
    // get one_or_all flag
    bool one_flag = true;
    ObDatum *json_datum = NULL;
    ObExpr *json_arg = expr.args_[1];
    ObObjType val_type = json_arg->datum_meta_.type_;
    if (OB_FAIL(temp_allocator.eval_arg(json_arg, ctx, json_datum))) {
      LOG_WARN("eval json arg failed", K(ret));
    } else if (val_type == ObNullType || json_datum->is_null()) {
      is_null_result = true;
    } else {
      ObString target_str = json_datum->get_string();
      if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(json_arg, ctx, temp_allocator, target_str, is_null_result))) {
        LOG_WARN("fail to get real data.", K(ret), K(target_str));
      } else if (0 == target_str.case_compare("one")) {
        one_flag = true;
      } else if (0 == target_str.case_compare("all")) {
        one_flag = false;
      } else {
        ret = OB_ERR_JSON_BAD_ONE_OR_ALL_ARG;
        LOG_USER_ERROR(OB_ERR_JSON_BAD_ONE_OR_ALL_ARG);
      }
    }

    bool is_contains = false;
    if ((OB_SUCC(ret)) && !is_null_result) {
      ObJsonPathCache ctx_cache(&temp_allocator);
      ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
      path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

      for (int64_t i = 2; OB_SUCC(ret) && i < expr.arg_cnt_ && !is_null_result; i++) {
        ObJsonSeekResult hit;
        ObDatum *path_data = NULL;
        if (OB_FAIL(temp_allocator.eval_arg(expr.args_[i], ctx, path_data))) {
          LOG_WARN("eval json path datum failed", K(ret));
        } else if (expr.args_[i]->datum_meta_.type_ == ObNullType || path_data->is_null()) {
          is_null_result = true;
        } else {
          ObString path_val = path_data->get_string();
          ObJsonPath *json_path = nullptr;
          bool is_const = expr.args_[i]->is_const_expr();
          if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], ctx, temp_allocator, path_val, is_null_result))) {
            LOG_WARN("fail to get real data.", K(ret), K(path_val));
          } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(temp_allocator, path_cache, json_path, path_val, i, true, is_const))) {
            LOG_WARN("failed: parse path", K(path_data->get_string()), K(ret));
          } else if (OB_FAIL(json_target->seek(*json_path, json_path->path_node_cnt(),
                                               true, false, hit))) {
            LOG_WARN("json seek failed", K(path_data->get_string()), K(ret));
          }

          if (hit.size() == 0) {
            if (!one_flag) {
              is_contains = false;
              break;
            }
          } else {
            is_contains = true;
            if (one_flag) {
              break;
            }
          }
        }
      }
    }

    // set result
    if (OB_FAIL(ret)) {
      LOG_WARN("json_contains_path failed", K(ret));
    } else if (is_null_result) {
      res.set_null();
    } else {
      res.set_int(static_cast<int64_t>(is_contains));
    }

  }

  return ret;
}

int ObExprJsonContainsPath::eval_json_contains_path(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &res)
{
  INIT_SUCC(ret);
  ObObjType val_type = expr.args_[0]->datum_meta_.type_;
  bool enable_fast_path = false;

  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_NOT_NULL(session) && session->is_enable_fast_json_path_lookup()) {
    bool all_paths_const = true;
    for (int64_t i = 2; i < expr.arg_cnt_; ++i) {
      if (!expr.args_[i]->is_static_const_expr()) {
        all_paths_const = false;
        break;
      }
    }
    enable_fast_path = (val_type == ObJsonType) && all_paths_const;
  }
  if (enable_fast_path) {
    ObJsonPathCache *path_cache =
        ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    if (OB_NOT_NULL(path_cache) && path_cache->is_fast_path_unsupported()) {
      enable_fast_path = false;
    }
  }

  if (enable_fast_path && OB_FAIL(eval_json_contains_path_fast_path(expr, ctx, res))) {
    LOG_WARN("eval json_contains_path fast path failed", K(ret));
  }

  if (!enable_fast_path || OB_FAIL(ret)) {
    ret = OB_SUCCESS;
    if (OB_FAIL(eval_json_contains_path_general_path(expr, ctx, res))) {
      LOG_WARN("eval json_contains_path general path failed", K(ret));
    }
  }

  return ret;
}

int ObExprJsonContainsPath::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                    const ObRawExpr &raw_expr,
                                    ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_contains_path;
  return OB_SUCCESS;
}

}
}
