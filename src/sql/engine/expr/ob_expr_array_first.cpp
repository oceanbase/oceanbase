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
 * This file contains implementation for array.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_array_first.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/udt/ob_array_type.h"
#include "lib/udt/ob_array_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace sql
{
ObExprArrayFirst::ObExprArrayFirst(ObIAllocator &alloc)
    : ObExprArrayMapCommon(alloc, T_FUNC_SYS_ARRAY_FIRST, N_ARRAY_FIRST, MORE_THAN_TWO, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprArrayFirst::ObExprArrayFirst(ObIAllocator &alloc,
                                   ObExprOperatorType type,
                                   const char *name,
                                   int32_t param_num,
                                   int32_t dimension) : ObExprArrayMapCommon(alloc, type, name, param_num, NOT_VALID_FOR_GENERATED_COL, dimension)
{
}

ObExprArrayFirst::~ObExprArrayFirst()
{
}

int ObExprArrayFirst::calc_result_typeN(ObExprResType& type,
                                        ObExprResType* types_stack,
                                        int64_t param_num,
                                        ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = const_cast<ObSQLSessionInfo *>(type_ctx.get_session());
  ObExecContext *exec_ctx = OB_ISNULL(session) ? NULL : session->get_cur_exec_ctx();
  ObDataType elem_type;
  ObSubSchemaValue arr_meta;
  const ObSqlCollectionInfo *coll_info = NULL;
  ObCollectionArrayType *arr_type = NULL;
  bool is_null_res = false;

  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec ctx is null", K(ret));
  } else if (!ob_is_int_uint_tc(types_stack[0].get_type())) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid data type", K(ret), K(types_stack[0].get_type()));
  }

  for (int64_t i = 1; i < param_num && OB_SUCC(ret) && !is_null_res; i++) {
    if (types_stack[i].is_null()) {
      is_null_res = true;
    } else if (!ob_is_collection_sql_type(types_stack[i].get_type())) {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("invalid data type", K(ret), K(types_stack[i].get_type()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (is_null_res) {
    type.set_null();
  } else if (OB_FAIL(exec_ctx->get_sqludt_meta_by_subschema_id(types_stack[1].get_subschema_id(), arr_meta))) {
    LOG_WARN("failed to get elem meta.", K(ret), K(types_stack[1].get_subschema_id()));
  } else if (OB_ISNULL(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(arr_meta.value_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array collection info is null", K(ret));
  } else if (OB_ISNULL(arr_type = static_cast<ObCollectionArrayType *>(coll_info->collection_meta_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array collection array type is null", K(ret), K(*coll_info));
  } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_BASIC_TYPE) {
    ObCollectionBasicType *elem_type = static_cast<ObCollectionBasicType *>(arr_type->element_type_);
    type.set_meta(elem_type->basic_meta_.get_meta_type());
    type.set_accuracy(elem_type->basic_meta_.get_accuracy());
  } else if (arr_type->element_type_->type_id_ == ObNestedType::OB_ARRAY_TYPE
             || arr_type->element_type_->type_id_ == ObNestedType::OB_VECTOR_TYPE) {
    ObString child_def;
    uint16_t child_subschema_id = 0;
    if (OB_FAIL(coll_info->get_child_def_string(child_def))) {
      LOG_WARN("failed to get child define", K(ret), K(*coll_info));
    } else if (OB_FAIL(session->get_cur_exec_ctx()->get_subschema_id_by_type_string(child_def, child_subschema_id))) {
      LOG_WARN("failed to get child subschema id", K(ret), K(*coll_info), K(child_def));
    } else {
      type.set_collection(child_subschema_id);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ObNestedType type", K(ret), K(arr_type->element_type_->type_id_));
  }

  return ret;
}

int ObExprArrayFirst::eval_array_first(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExprArrayMapInfo *info = static_cast<ObExprArrayMapInfo *>(expr.extra_info_);
  ObIArrayType *src_arrs[expr.arg_cnt_ - 1];
  ObDatum *datum_val = NULL;
  bool is_null_res = false;
  uint32_t arr_dim = 0;

  if (ob_is_null(expr.obj_meta_.get_type())) {
    is_null_res = true;
  } else if (OB_UNLIKELY(OB_ISNULL(info))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr extra info is null", K(ret));
  } else if (OB_FAIL(eval_src_arrays(expr, ctx, tmp_allocator, src_arrs, arr_dim, is_null_res))) {
    LOG_WARN("failed to eval src arrays", K(ret));
  } else if (is_null_res) {
    res.set_null();
  } else {
    ObExprArrayMapInfo *info = static_cast<ObExprArrayMapInfo *>(expr.extra_info_);
    bool found_res = false;
    for (uint32_t i = 0; i < arr_dim && !found_res && OB_SUCC(ret); i++) {
      ObSQLUtils::clear_expr_eval_flags(*expr.args_[0], ctx);
      if (OB_FAIL(set_lambda_para(tmp_allocator, ctx, info, src_arrs, i))) {
        LOG_WARN("failed to set lambda para", K(ret), K(i));
      } else if (OB_FAIL(expr.args_[0]->eval(ctx, datum_val))) {
        LOG_WARN("failed to eval args", K(ret));
      } else if (datum_val->is_null()) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("invalid data type", K(ret));
      } else if (datum_val->get_bool()) {
        found_res = true;
        if (src_arrs[0]->is_null(i)) {
          res.set_null();
        } else if (src_arrs[0]->is_nested_array()) {
          ObIArrayType* child_arr = NULL;
          ObString child_arr_str;
          if (OB_FAIL(ObArrayTypeObjFactory::construct(tmp_allocator, *src_arrs[0]->get_array_type()->element_type_, child_arr))) {
            LOG_WARN("failed to add null to array", K(ret));
          } else if (OB_FAIL(static_cast<ObArrayNested*>(src_arrs[0])->at(i, *child_arr))) {
            LOG_WARN("failed to get elem", K(ret), K(i));
          } else if (OB_FAIL(ObArrayExprUtils::set_array_res(child_arr, child_arr->get_raw_binary_len(), expr, ctx, child_arr_str))) {
            LOG_WARN("get array binary string failed", K(ret));
          } else {
            res.set_string(child_arr_str);
          }
        } else {
          ObObj elem_obj;
          if (OB_FAIL(src_arrs[0]->elem_at(i, elem_obj))) {
            LOG_WARN("failed to get element", K(ret), K(i));
          } else {
            res.from_obj(elem_obj);
          }
        }
      }
    } // end for
    if (OB_SUCC(ret) && !found_res) {
      res.set_null();
    }
  }
  return ret;
}

int ObExprArrayFirst::cg_expr(ObExprCGCtx &expr_cg_ctx,
                            const ObRawExpr &raw_expr,
                            ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  ObIExprExtraInfo *extra_info = nullptr;

  if (OB_FAIL(construct_extra_info(expr_cg_ctx, raw_expr, rt_expr, extra_info))) {
    LOG_WARN("failed to construct extra info", K(ret));
  } else {
    rt_expr.extra_info_ = extra_info;
    rt_expr.eval_func_ = eval_array_first;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
