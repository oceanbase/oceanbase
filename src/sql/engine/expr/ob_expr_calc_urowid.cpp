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
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/container/ob_array_helper.h"
#include "common/data_buffer.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprCalcURowID::ObExprCalcURowID(ObIAllocator &alloc)
  : ObExprOperator(alloc, T_FUN_SYS_CALC_UROWID, N_CALC_UROWID, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION,
                   false, INTERNAL_IN_ORACLE_MODE)
{
}

ObExprCalcURowID::~ObExprCalcURowID() {}

int ObExprCalcURowID::calc_result_typeN(ObExprResType &type,
                                        ObExprResType *types,
                                        int64_t param_num,
                                        ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num < 1)
      || OB_ISNULL(types)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for calc_result_typeN",
             K(ret), K(param_num), K(types), K(lib::is_mysql_mode()));
  } else if (lib::is_mysql_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("calc_urowid is not supported in mysql mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "urowid in mysql mode");
  } else {
    type.set_type(ObURowIDType);
    type.set_length(OB_MAX_USER_ROW_KEY_LENGTH);
  }
  return ret;
}

int ObExprCalcURowID::cg_expr(ObExprCGCtx &cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(raw_expr);
  UNUSED(cg_ctx);
  if (OB_UNLIKELY(rt_expr.arg_cnt_ < 1) || OB_ISNULL(rt_expr.args_)
      || OB_UNLIKELY(ObURowIDType != rt_expr.datum_meta_.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(rt_expr.arg_cnt_), K(rt_expr.args_), K(rt_expr.type_));
  } else {
    rt_expr.eval_func_ = calc_urowid;
  }
  return ret;
}

int ObExprCalcURowID::calc_urowid(const ObExpr &rt_expr,
                                  ObEvalCtx &eval_ctx,
                                  ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(lib::is_mysql_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("calc_urowid is not supported in mysql mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "urowid in mysql mode");
  } else if (OB_UNLIKELY(2 > rt_expr.arg_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected arg cnt", K(ret), K(rt_expr.arg_cnt_));
  } else {
    int64_t version = -1;
    ObDatum *param = NULL;
    if (OB_FAIL(rt_expr.args_[0]->eval(eval_ctx, param))) {
      LOG_WARN("eval version arg failed", K(ret));
    } else {
      number::ObNumber ver_nmb(param->get_number());
      if (OB_UNLIKELY(!ver_nmb.is_valid_int64(version))) {
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
      } // end for
      ObDataBuffer data_alloc;
      int64_t data_size = ObURowIDData::needed_content_buf_size(pk_vals, version);
      char *data_buf = NULL;
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(data_buf = rt_expr.get_str_res_mem(eval_ctx, data_size))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret));
        } else {
          data_alloc.set_data(data_buf, data_size);
          if (all_pk_is_null(version, pk_vals)) {
            // trigger before-row or outer join add extra null for rowid column will occur pks all are null.
            expr_datum.set_null();
          } else if (OB_FAIL(urowid_data.set_rowid_content(pk_vals, version, data_alloc))) {
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

bool ObExprCalcURowID::all_pk_is_null(int64_t version, ObIArray<ObObj> &pk_vals)
{
  bool is_all = !pk_vals.empty();
  if (ObURowIDData::HEAP_TABLE_ROWID_VERSION == version ||
      ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION == version) {
    is_all = pk_vals.at(0).is_null();
  } else {
    for (int64_t i = 0; is_all && i < pk_vals.count(); ++i) {
      is_all &= pk_vals.at(i).is_null();
    }
  }
  return is_all;
}

} // end namespace sql
} // end namespace oceanbase


