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
 */

// This file is for implement of func json expr helper
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/ob_exec_context.h"
#include "ob_expr_json_func_helper.h"
#include "lib/json_type/ob_json_bin.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {
ObJsonInType ObJsonExprHelper::get_json_internal_type(ObObjType type)
{
  return type == ObJsonType ? ObJsonInType::JSON_BIN : ObJsonInType::JSON_TREE;
}

int ObJsonExprHelper::ensure_collation(ObObjType type, ObCollationType cs_type)
{
  INIT_SUCC(ret);
  if (ob_is_string_type(type) && (ObCharset::charset_type_by_coll(cs_type) != CHARSET_UTF8MB4)) {
    LOG_WARN("unsuport json string type with binary charset", K(type), K(cs_type));
    ret = OB_ERR_INVALID_JSON_CHARSET;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_CHARSET);
  }

  return ret;
}

int ObJsonExprHelper::get_json_doc(const ObExpr &expr, ObEvalCtx &ctx, common::ObArenaAllocator &allocator,
    uint16_t index, ObIJsonBase *&j_base, bool &is_null, bool need_to_tree)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  ObCollationType cs_type = json_arg->datum_meta_.cs_type_;

  if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (val_type == ObNullType || json_datum->is_null()) {
    is_null = true;
  } else if (val_type != ObJsonType && !ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(val_type));
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(val_type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(val_type), K(cs_type));
  } else {
    ObString j_str = json_datum->get_string();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(val_type);
    ObJsonInType expect_type = need_to_tree ? ObJsonInType::JSON_TREE : j_in_type;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, j_str, j_in_type, expect_type, j_base))) {
      LOG_WARN("fail to get json base", K(ret), K(j_in_type));
      ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
    }
  }
  return ret;
}

int ObJsonExprHelper::get_json_doc(const ObObj *objs, common::ObIAllocator *allocator, uint16_t index,
    ObIJsonBase *&j_base, bool &is_null, bool need_to_tree)
{
  INIT_SUCC(ret);
  ObObjType val_type = objs[index].get_type();
  ObCollationType cs_type = objs[index].get_collation_type();

  if (objs[index].is_null() || val_type == ObNullType) {
    is_null = true;
  } else if (val_type != ObJsonType && !ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(val_type));
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(val_type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(val_type), K(cs_type));
  } else {
    ObString j_str = objs[index].get_string();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(val_type);
    ObJsonInType expect_type = need_to_tree ? ObJsonInType::JSON_TREE : j_in_type;
    if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type, expect_type, j_base))) {
      LOG_WARN("fail to get json base", K(ret), K(j_in_type));
      ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT_IN_PARAM);
    }
  }
  return ret;
}

int ObJsonExprHelper::get_json_val(const common::ObObj &data, ObExprCtx &ctx, bool is_bool,
    common::ObIAllocator *allocator, ObIJsonBase *&j_base, bool to_bin)
{
  INIT_SUCC(ret);
  ObObjType val_type = data.get_type();
  if (data.is_null()) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonNull));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonNull *null_node = static_cast<ObJsonNull *>(new (json_node_buf) ObJsonNull());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, null_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = null_node;
      }
    }
  } else if (is_bool) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonBoolean));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonBoolean *bool_node = (ObJsonBoolean *)new (json_node_buf) ObJsonBoolean(data.get_bool());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, bool_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = bool_node;
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ObCollationType cs_type = data.get_collation_type();
    if (OB_FAIL(
            ObJsonExprHelper::transform_convertible_2jsonBase(data, val_type, allocator, cs_type, j_base, to_bin))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  } else {
    ObBasicSessionInfo *session = ctx.exec_ctx_->get_my_session();
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(
                   data, val_type, allocator, data.get_scale(), session->get_timezone_info(), j_base, to_bin))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }

  return ret;
}

int ObJsonExprHelper::get_json_val(const ObExpr &expr, ObEvalCtx &ctx, common::ObIAllocator *allocator, uint16_t index,
    ObIJsonBase *&j_base, bool to_bin)
{
  INIT_SUCC(ret);
  ObDatum *json_datum = NULL;
  ObExpr *json_arg = expr.args_[index];
  ObObjType val_type = json_arg->datum_meta_.type_;
  if (OB_UNLIKELY(OB_FAIL(json_arg->eval(ctx, json_datum)))) {
    LOG_WARN("eval json arg failed", K(ret), K(val_type));
  } else if (json_datum->is_null()) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonNull));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonNull *null_node = static_cast<ObJsonNull *>(new (json_node_buf) ObJsonNull());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, null_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = null_node;
      }
    }
  } else if (json_arg->is_boolean_ == 1) {
    void *json_node_buf = allocator->alloc(sizeof(ObJsonBoolean));
    if (OB_ISNULL(json_node_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed: alloscate jsonboolean", K(ret));
    } else {
      ObJsonBoolean *bool_node = (ObJsonBoolean *)new (json_node_buf) ObJsonBoolean(json_datum->get_bool());
      if (to_bin) {
        if (OB_FAIL(ObJsonBaseFactory::transform(allocator, bool_node, ObJsonInType::JSON_BIN, j_base))) {
          LOG_WARN("failed: json tree to bin", K(ret));
        }
      } else {
        j_base = bool_node;
      }
    }
  } else if (ObJsonExprHelper::is_convertible_to_json(val_type)) {
    ObCollationType cs_type = json_arg->datum_meta_.cs_type_;
    if (OB_FAIL(ObJsonExprHelper::transform_convertible_2jsonBase(
            *json_datum, val_type, allocator, cs_type, j_base, to_bin))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  } else {
    ObBasicSessionInfo *session = ctx.exec_ctx_.get_my_session();
    ObScale scale = json_arg->datum_meta_.scale_;
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_FAIL(ObJsonExprHelper::transform_scalar_2jsonBase(
                   *json_datum, val_type, allocator, scale, session->get_timezone_info(), j_base, to_bin))) {
      LOG_WARN("failed: parse value to jsonBase", K(ret), K(val_type));
    }
  }
  return ret;
}

int ObJsonExprHelper::json_base_replace(ObIJsonBase *json_old, ObIJsonBase *json_new, ObIJsonBase *&json_doc)
{
  INIT_SUCC(ret);
  if (json_old == json_doc) {
    json_doc = json_new;
  } else {
    ObIJsonBase *json_old_tree = json_old;
    ObIAllocator *allocator = json_doc->get_allocator();
    if (!json_old->is_tree() &&
        ObJsonBaseFactory::transform(allocator, json_old, ObJsonInType::JSON_TREE, json_old_tree)) {
      LOG_WARN("fail to transform to tree", K(ret), K(*json_old));
    } else {
      ObIJsonBase *parent = static_cast<ObJsonNode *>(json_old_tree)->get_parent();
      if (OB_NOT_NULL(parent) && parent != json_doc) {
        if (OB_FAIL(parent->replace(json_old_tree, json_new))) {
          LOG_WARN("json base replace failed", K(ret));
        }
      } else {
        if (OB_FAIL(json_doc->replace(json_old_tree, json_new))) {
          LOG_WARN("json base replace failed", K(ret));
        }
      }
    }
  }
  return ret;
}

// get json expr path cache context, if not exists cache context do nothing
ObJsonPathCache *ObJsonExprHelper::get_path_cache_ctx(const uint64_t &id, ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  ObJsonPathCacheCtx *cache_ctx = NULL;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<ObJsonPathCacheCtx *>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if pathcache not exist, create one
      void *cache_ctx_buf = NULL;
      ret = exec_ctx->create_expr_op_ctx(id, sizeof(ObJsonPathCacheCtx), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf) ObJsonPathCacheCtx(&exec_ctx->get_allocator());
      }
    }
  }
  return (cache_ctx == NULL) ? NULL : cache_ctx->get_path_cache();
}

int ObJsonExprHelper::find_and_add_cache(
    ObJsonPathCache *path_cache, ObJsonPath *&res_path, ObString &path_str, int arg_idx, bool enable_wildcard)
{
  INIT_SUCC(ret);
  if (OB_FAIL(path_cache->find_and_add_cache(res_path, path_str, arg_idx))) {
    ret = OB_ERR_INVALID_JSON_PATH;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
  } else if (!enable_wildcard && res_path->can_match_many()) {
    ret = OB_ERR_INVALID_JSON_PATH_WILDCARD;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH_WILDCARD);
  }
  return ret;
}

bool ObJsonExprHelper::is_convertible_to_json(ObObjType &type)
{
  bool val = false;
  switch (type) {
    case ObNullType:
    case ObJsonType:
    case ObVarcharType:
    case ObCharType:
    case ObTinyTextType:
    case ObTextType:
    case ObMediumTextType:
    case ObLongTextType: {
      val = true;
      break;
    }
    default:
      break;
  }
  return val;
}

int ObJsonExprHelper::is_valid_for_json(ObExprResType *types_stack, uint32_t index, const char *func_name)
{
  INIT_SUCC(ret);
  ObObjType in_type = types_stack[index].get_type();

  if (!is_convertible_to_json(in_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, index + 1, func_name);
  } else if (ob_is_string_type(in_type) && types_stack[index].get_collation_type() != CS_TYPE_BINARY) {
    if (types_stack[index].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }

  return ret;
}

int ObJsonExprHelper::is_valid_for_json(ObExprResType &type, uint32_t index, const char *func_name)
{
  INIT_SUCC(ret);
  ObObjType in_type = type.get_type();

  if (!is_convertible_to_json(in_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, index, func_name);
  } else if (ob_is_string_type(in_type) && type.get_collation_type() != CS_TYPE_BINARY) {
    if (type.get_charset_type() != CHARSET_UTF8MB4) {
      type.set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  }

  return ret;
}

int ObJsonExprHelper::is_valid_for_path(ObExprResType *types_stack, uint32_t index)
{
  INIT_SUCC(ret);
  ObObjType in_type = types_stack[index].get_type();
  if (in_type == ObNullType || in_type == ObJsonType) {
  } else if (ob_is_string_type(in_type)) {
    if (types_stack[index].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else {
    ret = OB_ERR_INVALID_JSON_PATH;
    LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
  }
  return ret;
}

void ObJsonExprHelper::set_type_for_value(ObExprResType *types_stack, uint32_t index)
{
  ObObjType in_type = types_stack[index].get_type();
  if (in_type == ObNullType) {
  } else if (ob_is_string_type(in_type)) {
    if (types_stack[index].get_charset_type() != CHARSET_UTF8MB4) {
      types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
    }
  } else if (in_type == ObJsonType) {
    types_stack[index].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
}

int ObJsonExprHelper::is_json_zero(const ObString &data, int &result)
{
  INIT_SUCC(ret);
  int tmp_result = 0;
  ObJsonBin j_bin(data.ptr(), data.length());
  if (data.length() == 0) {
    result = 1;
  } else if (OB_FAIL(j_bin.reset_iter())) {
    LOG_WARN("failed: reset iter", K(ret));
  } else if (OB_FAIL(ObJsonBaseUtil::compare_int_json(0, &j_bin, tmp_result))) {
    LOG_WARN("failed: cmp json", K(ret));
  } else {
    result = (tmp_result == 0) ? 0 : 1;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
