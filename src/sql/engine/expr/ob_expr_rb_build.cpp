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
 * This file contains implementation for rb_build expression.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_rb_build.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/engine/expr/ob_expr_rb_func_helper.h"
#include "lib/roaringbitmap/ob_rb_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprRbBuild::ObExprRbBuild(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUNC_SYS_RB_BUILD, N_RB_BUILD, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprRbBuild::~ObExprRbBuild()
{
}

int ObExprRbBuild::calc_result_type1(ObExprResType &type,
                                    ObExprResType &type1,
                                    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  ObExecContext *exec_ctx = NULL;
  ObSubSchemaValue arr_meta;
  const ObSqlCollectionInfo *coll_info = NULL;
  uint16_t subschema_id;

  if (OB_ISNULL(session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSQLSessionInfo is null", K(ret));
  } else if (OB_ISNULL(exec_ctx = session->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObExecContext is null", K(ret));
  } else if (ob_is_null(type1.get_type())) {
    // do nothing
  } else if (!ob_is_collection_sql_type(type1.get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_OP, "ARRAY", ob_obj_type_str(type1.get_type()));
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(type1.get_subschema_id(), arr_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(type1.get_subschema_id()));
  } else if (arr_meta.type_ != ObSubSchemaType::OB_SUBSCHEMA_COLLECTION_TYPE) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid subschema type", K(ret), K(arr_meta.type_));
  } else {
    ObDataType data_type;
    uint32_t depth = 0;
    bool is_vec = false;
    if (OB_FAIL(ObArrayExprUtils::get_array_element_type(exec_ctx, type1.get_subschema_id(), data_type, depth, is_vec))) {
      LOG_WARN("failed to get array element type", K(ret));
    } else if (!ob_is_integer_type(data_type.get_obj_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("basic element type of array is not integer", K(ret), K(data_type.get_obj_type()));
    }
  }

  if (OB_SUCC(ret)) {
    type.set_roaringbitmap();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObRoaringBitmapType]).get_length());
  }

  return ret;
}

int ObExprRbBuild::eval_rb_build(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObDatum *arr_datum = NULL;
  ObRoaringBitmap *rb = NULL;
  ObString rb_bin;
  ObIArrayType *arr_obj = NULL;
  const uint16_t subschema_id = expr.args_[0]->obj_meta_.get_subschema_id();
  bool is_null_res = false;

  if (OB_FAIL(expr.args_[0]->eval(ctx, arr_datum))) {
    LOG_WARN("failed to eval source array arg", K(ret));
  } else if (arr_datum->is_null()) {
    is_null_res = true;
  } else if (OB_FAIL(ObArrayExprUtils::get_array_obj(tmp_allocator, ctx, subschema_id, arr_datum->get_string(), arr_obj))) {
    LOG_WARN("construct array obj failed", K(ret));
  } else if (OB_ISNULL(rb = OB_NEWx(ObRoaringBitmap, &tmp_allocator, (&tmp_allocator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create alloc memory to roaringbitmap", K(ret));
  } else {
    for (uint32_t i = 0; OB_SUCC(ret) && i < arr_obj->cardinality(); ++i) {
      ObObj elem_obj;
      bool is_null_elem = false;
      if (OB_FAIL(ObArrayExprUtils::get_basic_elem(arr_obj, i, elem_obj, is_null_elem))) {
        LOG_WARN("failed to cast get element", K(ret));
      } else if (is_null_elem) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("array contains null basic element", K(ret));
      } else if (OB_UNLIKELY(!elem_obj.is_integer_type())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("element is not integer", K(ret), K(elem_obj.get_type()));
      } else if (elem_obj.is_signed_integer() && elem_obj.get_int() < 0) {
        if (elem_obj.get_int() < INT32_MIN) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("negative integer not in the range of int32", K(ret), K(elem_obj.get_int()));
        } else {
          uint32_t uint32_val = static_cast<uint32_t>(elem_obj.get_int());
          if (OB_FAIL(rb->value_add(static_cast<uint64_t>(uint32_val)))) {
            LOG_WARN("failed to add value to roaringbtimap", K(ret), K(uint32_val));
          }
        }
      } else if (OB_FAIL(rb->value_add(elem_obj.get_uint64()))) {
        LOG_WARN("failed to add value to roaringbtimap", K(ret), K(elem_obj.get_uint64()));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    res.set_null();
  } else if (OB_FAIL(ObRbUtils::rb_serialize(tmp_allocator, rb_bin, rb))) {
    LOG_WARN("failed to serialize roaringbitmap", K(ret));
  } else if (OB_FAIL(ObRbExprHelper::pack_rb_res(expr, ctx, res, rb_bin))) {
    LOG_WARN("fail to pack roaringbitmap res", K(ret));
  }
  ObRbUtils::rb_destroy(rb);

  return ret;
}

int ObExprRbBuild::cg_expr(ObExprCGCtx &expr_cg_ctx,
                         const ObRawExpr &raw_expr,
                         ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = ObExprRbBuild::eval_rb_build;
  return OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
