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
#include "lib/xml/ob_binary_aggregate.h"
#include "lib/container/ob_array_helper.h"
#include "lib/json_type/ob_json_wrapper.h"
#include "sql/engine/expr/ob_expr.h"

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

// Pack all elements of hits into a JSON array result.
static int pack_json_hits(const ObExpr &expr, ObEvalCtx &ctx,
                          MultimodeAlloctor &allocator,
                          ObIArray<ObJsonWrapper> &hits,
                          ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObBinAggSerializer bin_agg(&allocator, AGG_JSON, static_cast<uint8_t>(ObJsonNodeType::J_ARRAY));
  ObStringBuffer value(&allocator);
  for (int32_t i = 0; OB_SUCC(ret) && i < hits.count(); i++) {
    if (OB_FAIL(hits.at(i).append_to_bin_agg(allocator, bin_agg, value))) {
      LOG_WARN("failed to append hit to bin_agg", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(bin_agg.serialize())) {
    LOG_WARN("failed to serialize bin_agg", K(ret));
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, bin_agg.get_buffer()->string()))) {
    LOG_WARN("failed to pack json result", K(ret));
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

int ObExprJsonExtract::eval_json_extract_fast_path(const ObExpr &expr,
                                                    ObEvalCtx &ctx,
                                                    ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *json_datum = NULL;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();

  if (OB_FAIL(expr.args_[0]->eval(ctx, json_datum))) {
    LOG_WARN("fail to get real data.", K(ret));
  } else if (json_datum->is_null()) {
    res.set_null();
  } else {
    ObString path_str;
    bool is_null_path = false;
    ObString json_data;
    common::ObJsonPathCache *path_cache =
      ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);

    if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[1], ctx, tmp_allocator, path_str, is_null_path))) {
      LOG_WARN("fail to get path string", K(ret));
    } else if (is_null_path) {
      res.set_null();
    } else if (OB_ISNULL(path_cache)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported fast path", K(ret));
    } else if (path_cache->is_fast_path_unchecked()
        && OB_FAIL(ObJsonExprHelper::validate_and_cache_simple_path(path_cache, path_str, tmp_allocator, 1))) {
      LOG_WARN("fail to validate and cache simple path", K(ret));
    } else if (path_cache->is_fast_path_unsupported()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported fast path because path cache is not inited", K(ret));
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(
        tmp_allocator, *json_datum, expr.args_[0]->datum_meta_,
        expr.args_[0]->obj_meta_.has_lob_header(), json_data, &ctx.exec_ctx_))) {
      LOG_WARN("fail to get real data.", K(ret));
    } else {
      ObJsonBinFastLocator fast_locator;
      char *res_ptr = nullptr;
      int64_t res_len = 0;
      uint8_t res_type = 0;

      const ObSEArray<ObJsonPathCache::ObMultiPathEntry, 4> &path_keys_arr = path_cache->get_multi_path_keys();
      if (path_keys_arr.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("path keys not found", K(ret));
      } else if (OB_FAIL(fast_locator.init(json_data.ptr(), json_data.length()))) {
        LOG_WARN("fail to init fast path", K(ret));
      } else {
        const ObJsonPathCache::ObMultiPathEntry &entry = path_keys_arr.at(0);
        ObArrayHelper<ObString> path_keys(entry.key_cnt, entry.keys, entry.key_cnt);
        if (OB_FAIL(fast_locator.seek(path_keys, res_ptr, res_len, res_type))) {
          if (ret == OB_SEARCH_NOT_FOUND) {
            res.set_null();
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to lookup", K(ret));
          }
        } else if (OB_FAIL(fast_locator.pack_json_str_res(expr, ctx, res, res_ptr, res_len, res_type))) {
          LOG_WARN("fail to pack json str res", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprJsonExtract::eval_json_extract_general_path(const ObExpr &expr,
                                                       ObEvalCtx &ctx,
                                                       ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[0];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObJsonWrapper wrapper;
  bool is_null_result = false;

  ObItemType pick_type = static_cast<ObItemType>(expr.extra_);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  MultimodeAlloctor allocator(tmp_alloc_g.get_allocator(), expr.type_, ret, ctx, "json_extract");
  if (OB_FAIL(allocator.eval_arg(json_arg, ctx, json_datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (json_datum->is_null() || val_type == ObNullType) {
    is_null_result = true; // mysql return NULL result
  } else if (OB_FAIL(ObJsonExprHelper::get_json_wrapper(json_arg, json_datum, allocator,
                                                                 ctx, wrapper, true))) {
    LOG_WARN("get json bin view wrapper failed", K(ret));
  }

  if (OB_UNLIKELY(OB_FAIL(ret))) {
    if (ret == OB_ERR_INVALID_TYPE_FOR_JSON) {
      LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, "json_extract");
    }
    LOG_WARN("fail to handle json param 0 in json extract", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    ObJsonPathCache *path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    if (OB_ISNULL(path_cache)) {
      path_cache = OB_NEWx(ObJsonPathCache, (&allocator), &allocator);
      if (OB_ISNULL(path_cache)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc path cache failed", K(ret));
      }
    }
    bool may_match_many = (expr.arg_cnt_ > 2);
    ObSEArray<ObJsonWrapper, 4> hits;

    for (int64_t i = 1; OB_SUCC(ret) && !is_null_result && i < expr.arg_cnt_; i++) {
      ObDatum *path_data = NULL;
      ObString path_text;
      ObJsonPath *j_path = NULL;
      if (OB_FAIL(allocator.eval_arg(expr.args_[i], ctx, path_data))) {
        LOG_WARN("eval json path datum failed", K(ret));
      } else if (path_data->is_null()) {
        is_null_result = true;
      } else if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[i], path_data, allocator, path_text, ctx))) {
        LOG_WARN("fail to get path data.", K(ret), K(path_text));
      } else if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(allocator, path_cache, j_path, path_text,
                                                               i, true, expr.args_[i]->is_static_const_expr()))) {
        LOG_WARN("parse path failed", K(path_text), K(ret));
      } else {
        may_match_many |= j_path->can_match_many();
        if (may_match_many) {
          // Multi-path: seek into a per-path temp array to avoid cross-path dedup.
          // Intra-path dedup (for wildcards/ellipsis) is still handled by push_unique_hit inside seek.
          ObSEArray<ObJsonWrapper, 4> path_hits;
          if (OB_FAIL(wrapper.seek(*j_path, allocator, path_hits, true, false))) {
            LOG_WARN("seek failed", K(path_text), K(ret));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < path_hits.count(); j++) {
              if (OB_FAIL(hits.push_back(path_hits.at(j)))) {
                LOG_WARN("push_back hit failed", K(ret), K(j));
              }
            }
          }
        } else if (OB_FAIL(wrapper.seek(*j_path, allocator, hits, true, true))) {
          LOG_WARN("seek failed", K(path_text), K(ret));
        }
      }
    }

    // Unified result processing.
    int32_t hit_size = static_cast<int32_t>(hits.count());
    if (OB_UNLIKELY(OB_FAIL(ret))) {
      LOG_WARN("json seek failed", K(ret));
    } else if (hit_size == 0 || is_null_result) {
      res.set_null();
    } else if (hit_size == 1 && !may_match_many) {
      if (hits.at(0).is_bin()) {
        bool has_lob = expr.obj_meta_.has_lob_header();
        ObExprStrResAlloc result_alloc(expr, ctx);
        ObString result_str;
        if (OB_FAIL(hits.at(0).get_raw_binary(result_alloc, result_str, has_lob))) {
          LOG_WARN("get_raw_binary failed", K(ret));
        } else {
          res.set_string(result_str);
        }
      } else {
        // DOM path: pack_json_str_res handles LOB header
        ObString raw_str;
        if (OB_FAIL(hits.at(0).get_raw_binary(allocator, raw_str))) {
          LOG_WARN("get_raw_binary failed", K(ret));
        } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_str))) {
          LOG_WARN("pack_json_str_res failed", K(ret));
        }
      }
    } else if (OB_FAIL(pack_json_hits(expr, ctx, allocator, hits, res))) {
      LOG_WARN("pack hits failed", K(ret));
    }
  }

  return ret;
}

int ObExprJsonExtract::eval_json_extract(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObObjType val_type = expr.args_[0]->datum_meta_.type_;
  bool may_match_many = (expr.arg_cnt_ > 2);
  bool is_static_const = expr.args_[1]->is_static_const_expr();
  bool enable_fast_path = false;

  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  if (OB_NOT_NULL(session)) {
    enable_fast_path = session->is_enable_fast_json_path_lookup() && !may_match_many
                       && is_static_const && (val_type == ObJsonType);
  }
  if (enable_fast_path) {
    common::ObJsonPathCache *path_cache =
        ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    if (OB_NOT_NULL(path_cache) && path_cache->is_fast_path_unsupported()) {
      enable_fast_path = false;
    }
  }

  // Try fast path first
  if (enable_fast_path && OB_FAIL(ObExprJsonExtract::eval_json_extract_fast_path(expr, ctx, res))) {
    LOG_WARN("fail to eval json extract fast path", K(ret));
  }

  if (!enable_fast_path || OB_FAIL(ret)) {
    ret = OB_SUCCESS; // Reset ret for general path
    if (OB_FAIL(ObExprJsonExtract::eval_json_extract_general_path(expr, ctx, res))) {
      LOG_WARN("fail to eval json extract general path", K(ret));
    }
  }

  return ret;
}

int ObExprJsonExtract::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                               ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  rt_expr.extra_ = static_cast<uint64_t>(T_NULL);
  ObObjType val_type = rt_expr.args_[0]->datum_meta_.type_;
  ObCollationType cs_type = rt_expr.args_[0]->datum_meta_.cs_type_;
  if (rt_expr.datum_meta_.type_ == ObNullType) {
    rt_expr.eval_func_ = eval_json_extract_null;
  } else if (rt_expr.datum_meta_.cs_type_ != CS_TYPE_UTF8MB4_BIN) {
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_WARN("invalid output charset", K(ret), K(rt_expr.datum_meta_.cs_type_));
  } else if (val_type != ObJsonType && val_type != ObNullType
             && ob_is_string_type(val_type) == false) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_WARN("input type error", K(val_type));
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(val_type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(val_type), K(cs_type));
  } else {
    rt_expr.eval_func_ = eval_json_extract;
  }
  return ret;
}

}
}
