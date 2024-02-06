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
 * This file contains implementation for json_storage_size.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_storage_size.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "share/object/ob_obj_cast.h"
#include "objit/common/ob_item_type.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/json_type/ob_json_tree.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprJsonStorageSize::ObExprJsonStorageSize(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_STORAGE_SIZE, N_JSON_STORAGE_SIZE, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonStorageSize::~ObExprJsonStorageSize()
{
}

int ObExprJsonStorageSize::calc_result_type1(ObExprResType &type,
                                            ObExprResType &type1,
                                            common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); 
  INIT_SUCC(ret);

  // set result type to int32
  type.set_int32();
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);

  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_STORAGE_SIZE))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  }
  return ret;
}

int ObExprJsonStorageSize::calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta,
                                bool has_lob_header, ObIAllocator *allocator, ObDatum &res)
{
  INIT_SUCC(ret);
  ObObjType type = meta.type_;
  ObCollationType cs_type = meta.cs_type_;

  if (type == ObNullType || data.is_null()) {
    res.set_null();
  } else if (type != ObJsonType && !ob_is_string_type(type)) { // invalid type
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid input type", K(type));
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else {
    uint64_t size = 0;
    common::ObString j_str = data.get_string();
    ObIJsonBase *j_base = NULL;
    ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
    if (j_str.length() == 0) {
      ret = OB_ERR_INVALID_JSON_TEXT;
      LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(*allocator, data, meta, has_lob_header, j_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(j_str));
    } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type,
        j_in_type, j_base))) {
      if (ret == OB_ERR_INVALID_JSON_TEXT) {
        LOG_USER_ERROR(OB_ERR_INVALID_JSON_TEXT);
      }
      LOG_WARN("fail to get json base", K(ret), K(type), K(j_str), K(j_in_type));
    } else if (OB_FAIL(j_base->get_used_size(size))) {
      LOG_WARN("fail to get used size", K(ret), K(type), K(j_str), K(j_in_type));
    } else {
      res.set_int32(size);
    }
  }

  return ret;
}

// for new sql engine
int ObExprJsonStorageSize::eval_json_storage_size(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);

  ObDatum *datum = NULL;
  ObExpr *arg = expr.args_[0];

  if (OB_FAIL(arg->eval(ctx, datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
    common::ObIAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
    if (OB_FAIL(calc(ctx, *datum, arg->datum_meta_, arg->obj_meta_.has_lob_header(), &tmp_allocator, res))) {
      LOG_WARN("fail to calc json storage free result", K(ret), K(arg->datum_meta_));
    }
  }

  return ret;
}

int ObExprJsonStorageSize::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_storage_size;
  return OB_SUCCESS;
}


}
}