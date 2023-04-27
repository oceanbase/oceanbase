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

#include "sql/engine/expr/ob_expr_tablet_autoinc_nextval.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{
ObExprTabletAutoincNextval::ObExprTabletAutoincNextval(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_TABLET_AUTOINC_NEXTVAL, N_TABLET_AUTOINC_NEXTVAL, 0, NOT_VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
   disable_operand_auto_cast();
}

ObExprTabletAutoincNextval::~ObExprTabletAutoincNextval()
{
}

int ObExprTabletAutoincNextval::calc_result_type0(ObExprResType &type,
                                                  common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  type.set_result_flag(NOT_NULL_FLAG);
  return ret;
}

int ObExprTabletAutoincNextval::calc_result_typeN(ObExprResType &type,
                                     ObExprResType *types_array,
                                     int64_t param_num,
                                     ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  type.set_uint64();
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].scale_);
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObUInt64Type].precision_);
  type.set_result_flag(NOT_NULL_FLAG);

  CK(NULL != type_ctx.get_session());
  if (OB_SUCC(ret)) {
    CK(types_array[0].is_uint64());
    if (OB_SUCC(ret) && 2 == param_num) {
      // column_conv() is add before nextval() in static tying engine. Parameter 2 is converted
      // to defined type, only uint/int allowed.
      ObObjTypeClass tc = ob_obj_type_class(type.get_type());
      if (!(ObNullTC == tc || ObIntTC == tc || ObUIntTC == tc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only int/uint type class supported for tablet auto_increment column",
                 K(ret));
      } else {
        static_cast<ObObjMeta &>(type) = types_array[1];
      }
    }
  }

  return ret;
}

int ObExprTabletAutoincNextval::cg_expr(ObExprCGCtx &, const ObRawExpr &, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  rt_expr.eval_func_ = eval_nextval;
  return ret;
}

int ObExprTabletAutoincNextval::eval_nextval(
    const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
  UNUSED(expr);
  UNUSED(ctx);
  expr_datum.set_null();
//  ObDatum *col_id = NULL;
//  ObDatum *tablet_id_datum = NULL; // TODO(shuangcan): currently it is partition id
//  ObPhysicalPlanCtx *plan_ctx = ctx.exec_ctx_.get_physical_plan_ctx();
//  ObSQLSessionInfo *my_session = ctx.exec_ctx_.get_my_session();
//  if (OB_ISNULL(plan_ctx) || OB_ISNULL(my_session)) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("no phy plan context", K(ret));
//  } else if (OB_FAIL(expr.eval_param_value(ctx, col_id, tablet_id_datum))) {
//    LOG_WARN("evaluate parameter failed", K(ret));
//  } else if (col_id->is_null() || tablet_id_datum->is_null()) {
//    ret = OB_ERR_UNEXPECTED;
//    LOG_WARN("id is null", K(ret));
//  } else {
//    // TODO(shuangcan): add logic to calculate tablet seq
//    ObTabletAutoincrementService &auto_service = ObTabletAutoincrementService::get_instance();
//    ObTabletAutoincParam &autoinc_param = plan_ctx->get_tablet_autoinc_param();
//    // common::ObTabletID tablet_id(tablet_id_datum->get_int());
//    common::ObTabletID tablet_id(1); // TODO(shuangcan): change to calc real tablet id
//    ObTabletCacheInterval &interval = autoinc_param.cache_interval_;
//    if (interval.tablet_id_ != tablet_id) {
//      if (OB_FAIL(auto_service.get_tablet_cache_interval(autoinc_param, tablet_id, interval))) {
//        if (OB_EAGAIN == ret) {
//          if (OB_FAIL(auto_service.get_tablet_cache_interval(autoinc_param, tablet_id, interval))) {
//            LOG_WARN("failed to get auto_increment handle", K(ret));
//          }
//        } else {
//          LOG_WARN("failed to get auto_increment handle", K(ret));
//        }
//      }
//    }
//    uint64_t value = 0;
//    if (OB_SUCC(ret)) {
//      // get auto-increment value
//      if (OB_FAIL(interval.next_value(value))) {
//        if (OB_EAGAIN == ret) {
//          if (OB_FAIL(auto_service.get_tablet_cache_interval(autoinc_param, tablet_id, interval))) {
//            LOG_WARN("failed to get auto_increment handle", K(ret));
//          } else if (OB_FAIL(interval.next_value(value))) {
//            LOG_WARN("failed to get next auto increment value", K(ret), K(interval));
//          } else {
//            expr_datum.set_int(value);
//          }
//        } else {
//          LOG_WARN("failed to get next auto increment value", K(ret), K(interval));
//        }
//      } else {
//        expr_datum.set_int(value);
//      }
//    }
//  }
  return ret;
}
}//end namespace sql
}//end namespace oceanbase
