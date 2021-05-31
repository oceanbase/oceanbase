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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_calc_urowid.h"

#include "lib/container/ob_array_helper.h"
#include "common/data_buffer.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {
ObExprCalcURowID::ObExprCalcURowID(ObIAllocator& alloc)
    : ObExprOperator(alloc, T_FUN_SYS_CALC_UROWID, N_CALC_UROWID, MORE_THAN_ZERO, NOT_ROW_DIMENSION)
{}

ObExprCalcURowID::~ObExprCalcURowID()
{}

int ObExprCalcURowID::calc_result_typeN(
    ObExprResType& type, ObExprResType* types, int64_t param_num, ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num < 1) || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for calc_result_typeN", K(ret), K(param_num), K(types), K(share::is_mysql_mode()));
  } else if (share::is_mysql_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("calc_urowid is not supported in mysql mode", K(ret));
  } else {
    type.set_type(ObURowIDType);
  }
  return ret;
}

int ObExprCalcURowID::calc_resultN(ObObj& result, const ObObj* obj_stack, int64_t param_num, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  int64_t version = -1;
  if (OB_ISNULL(expr_ctx.calc_buf_) || OB_ISNULL(obj_stack) || OB_UNLIKELY(param_num < 2)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(expr_ctx.calc_buf_), K(obj_stack), K(param_num));
  } else if (OB_UNLIKELY(share::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("calc_urowid is not supported in mysql mode", K(ret));
  } else if (OB_UNLIKELY(!obj_stack[0].is_number()) ||
             OB_UNLIKELY(!obj_stack[0].get_number().is_valid_int64(version)) ||
             OB_UNLIKELY(!ObURowIDData::is_valid_version(version))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first arg must be version info(number type)", K(ret), K(obj_stack[0]));
  } else {
    ObArrayHelper<ObObj> pk_vals(param_num, const_cast<ObObj*>(obj_stack) + 1, param_num - 1);
    LOG_TRACE("calc urowid", K(pk_vals), K(param_num));
    ObURowIDData urowid_data;
    if (OB_FAIL(urowid_data.set_rowid_content(pk_vals, version, *expr_ctx.calc_buf_))) {
      LOG_WARN("failed to set rowid content", K(ret), K(pk_vals));
    } else {
      result.set_urowid(urowid_data);
    }
  }
  return ret;
}

int ObExprCalcURowID::cg_expr(ObExprCGCtx& cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_) ||
      OB_UNLIKELY(ObURowIDType != rt_expr.datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = calc_urowid;
  }
  return ret;
}

int ObExprCalcURowID::calc_urowid(const ObExpr& rt_expr, ObEvalCtx& eval_ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(share::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("calc_urowid is not supported in mysql mode", K(ret));
  } else if (OB_UNLIKELY(2 > rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(rt_expr.arg_cnt_));
  } else {
    int64_t version = -1;
    ObDatum* param = NULL;
    if (OB_FAIL(rt_expr.args_[0]->eval(eval_ctx, param))) {
      LOG_WARN("eval version arg failed", K(ret));
    } else {
      number::ObNumber ver_nmb(param->get_number());
      if (OB_UNLIKELY(!ver_nmb.is_valid_int64(version)) || OB_UNLIKELY(!ObURowIDData::is_valid_version(version))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("first arg must be version info(number type)", K(ret), K(ver_nmb));
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObObj> pk_vals;
      ObURowIDData urowid_data;
      if (OB_FAIL(pk_vals.prepare_allocate(rt_expr.arg_cnt_ - 1))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      }
      for (int i = 1; OB_SUCC(ret) && i < rt_expr.arg_cnt_; i++) {
        if (OB_FAIL(rt_expr.args_[i]->eval(eval_ctx, param))) {
          LOG_WARN("failed to eval", K(ret));
        } else if (OB_FAIL(param->to_obj(pk_vals.at(i - 1), rt_expr.args_[i]->obj_meta_))) {
          LOG_WARN("failed to transform datum to obj", K(ret));
        } else {
          // do nothing
        }
      }  // end for
      ObDataBuffer data_alloc;
      int64_t data_size = ObURowIDData::needed_content_buf_size(pk_vals);
      char* data_buf = NULL;
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(data_buf = rt_expr.get_str_res_mem(eval_ctx, data_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          data_alloc.set_data(data_buf, data_size);
          if (OB_FAIL(urowid_data.set_rowid_content(pk_vals, version, data_alloc))) {
            LOG_WARN("failed to set rowid content", K(ret));
          } else {
            expr_datum.set_urowid(urowid_data);
          }
        }
      }
    }
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase
