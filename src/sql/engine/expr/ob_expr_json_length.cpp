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

// This file contains implementation for json_length.
#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_length.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "sql/parser/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"
#include "ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonLength::ObExprJsonLength(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_LENGTH, N_JSON_LENGTH,
                         ONE_OR_TWO, NOT_ROW_DIMENSION)
{
}

ObExprJsonLength::~ObExprJsonLength()
{
}

int ObExprJsonLength::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  INIT_SUCC(ret);
  UNUSED(type_ctx);

  // set result type to int32
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

  // 0 position is json doc
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(types_stack, 0, N_JSON_LENGTH))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(types_stack[0].get_type()));
  } else if (param_num > 1 && OB_FAIL(ObJsonExprHelper::is_valid_for_path(types_stack, 1))) {
    LOG_WARN("wrong type for json path.", K(ret), K(types_stack[1].get_type()));
  }

  return ret;
}

template <typename T>
int ObExprJsonLength::calc(const T &data1, ObObjType type1, ObCollationType cs_type1, const T *data2, 
                           ObObjType type2, ObIAllocator *allocator, T &res,
                           ObJsonPathCache* path_cache)
{
  INIT_SUCC(ret);
  bool is_null = false;
  uint32_t res_len = 0;
  ObIJsonBase *j_base = NULL;

  // handle data1(json text)
  if (type1 == ObNullType || data1.is_null()) { // null should display "NULL"
    is_null = true;
  } else if (type1 != ObJsonType && !ob_is_string_type(type1)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("input type error", K(type1));
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type1, cs_type1))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type1), K(cs_type1));
  } else {
    ObString j_doc = data1.get_string();
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type1);
    if (j_doc.length() == 0) {
      ret = OB_ERR_INVALID_JSON_TEXT;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_doc, j_in_type,
        j_in_type, j_base))) {
      LOG_WARN("fail to get json base", K(ret), K(type1), K(j_doc), K(j_in_type));
    }
  }

  // handle data2(path text)
  if (OB_SUCC(ret) && OB_LIKELY(!is_null)) {
    if (OB_ISNULL(data2)) { // have no path
      res_len = j_base->element_count();
    } else { // handle json path
      if (type2 == ObNullType) { // null should display "NULL"
        is_null = true;
      } else { // ObLongTextType
        ObJsonBaseVector hit;
        ObString j_path_text = data2->get_string();
        ObJsonPath *j_path = NULL;
        if (OB_FAIL(ObJsonExprHelper::find_and_add_cache(path_cache, j_path, j_path_text, 1, false))) {
          LOG_USER_ERROR(OB_ERR_INVALID_JSON_PATH);
          LOG_WARN("fail to parse json path", K(ret), K(type2), K(j_path_text));
        } else if (OB_FAIL(j_base->seek(*j_path, j_path->path_node_cnt(), true, true, hit))) {
          LOG_WARN("fail to seek json node", K(ret), K(j_path_text));
        } else if (hit.size() != 1) { // not found node by path, display "NULL"
          is_null = true;
        } else {
          res_len = hit[0]->element_count();
        }
      }
    }
  }

  // set result
  if (OB_SUCC(ret)) {
    if (is_null) {
      res.set_null();
    } else {
      res.set_int32(res_len);
    }
  }

  return ret;
}

// for old sql engine
int ObExprJsonLength::calc_resultN(common::ObObj &result,
                                   const common::ObObj *objs,
                                   int64_t param_num,
                                   common::ObExprCtx &expr_ctx) const
{
  INIT_SUCC(ret);
  ObIAllocator *allocator = expr_ctx.calc_buf_;

  if (OB_ISNULL(objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(objs), K(param_num));
  } else if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("varchar buffer not init", K(ret));
  } else {
    ObObjType path_type = ObMaxType;
    const common::ObObj *path_obj_ptr = NULL;
    if (param_num > 1) {
      path_obj_ptr = &objs[1];
      path_type = objs[1].get_type();
    }

    if (OB_SUCC(ret)) {
      ObJsonPathCache path_cache(allocator);
      if (OB_FAIL(calc(objs[0], objs[0].get_type(), objs[0].get_collation_type(), path_obj_ptr,
          path_type, allocator, result, &path_cache))) {
        LOG_WARN("fail to calc json length result", K(ret), K(objs[0]), K(param_num));
      }
    }
  }

  return ret;
}

// for new sql engine 
int ObExprJsonLength::eval_json_length(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObDatum *datum1 = NULL;
  ObObjType type1 = ObMaxType;
  ObDatum *datum0 = NULL;
  ObExpr *arg0 = expr.args_[0];
  ObObjType type0 = arg0->datum_meta_.type_;
  ObCollationType cs_type0 = arg0->datum_meta_.cs_type_;
  common::ObArenaAllocator &tmp_allocator = ctx.get_reset_tmp_alloc();

  if (OB_FAIL(arg0->eval(ctx, datum0))) { // json doc
    LOG_WARN("fail to eval json arg", K(ret), K(type1));
  } else {
    if (expr.arg_cnt_ > 1) { // json path
      ObExpr *arg1 = expr.args_[1];
      type1 = arg1->datum_meta_.type_;
      if (OB_FAIL(arg1->eval(ctx, datum1))) {
        LOG_WARN("fail to eval path arg", K(ret), K(type1));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObJsonPathCache ctx_cache(&tmp_allocator);
    ObJsonPathCache* path_cache = ObJsonExprHelper::get_path_cache_ctx(expr.expr_ctx_id_, &ctx.exec_ctx_);
    path_cache = ((path_cache != NULL) ? path_cache : &ctx_cache);

    if (OB_FAIL(calc(*datum0, type0, cs_type0, datum1, type1, &tmp_allocator, res, path_cache))) {
      LOG_WARN("fail to calc json length result", K(ret), K(datum0), K(expr.arg_cnt_));
    }
  }

  return ret;
}

int ObExprJsonLength::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_length;
  return OB_SUCCESS;
}


}
}