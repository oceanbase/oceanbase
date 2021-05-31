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
#include "sql/engine/expr/ob_expr_func_addr_to_part_id.h"
#include "sql/engine/ob_exec_context.h"
#include "share/object/ob_obj_cast.h"
using namespace oceanbase::common;
namespace oceanbase {
namespace sql {

ObExprFuncAddrToPartId::ObExprFuncAddrToPartId(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_ADDR_TO_PART_ID, N_ADDR_TO_PARTITION_ID, 2, NOT_ROW_DIMENSION)
{}

ObExprFuncAddrToPartId::~ObExprFuncAddrToPartId()
{}

int ObExprFuncAddrToPartId::calc_result_type2(
    ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  type.set_int();
  type.set_precision(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].precision_);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);
  // set calc type
  type1.set_calc_type(ObVarcharType);
  type1.set_calc_collation_type(ObCharset::get_system_collation());
  type1.set_calc_collation_level(CS_LEVEL_COERCIBLE);
  type2.set_calc_type(ObIntType);
  return ret;
}

int ObExprFuncAddrToPartId::calc_result2(ObObj& result, const ObObj& obj1, const ObObj& obj2, ObExprCtx& expr_ctx) const
{
  int ret = OB_SUCCESS;
  ObTaskExecutorCtx* task_exec_ctx = NULL;
  if (OB_ISNULL(expr_ctx.exec_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("exec ctx in expr ctx is NULL", K(ret));
  } else if (OB_ISNULL(task_exec_ctx = expr_ctx.exec_ctx_->get_task_executor_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task_exec_ctx is NULL", K(ret));
  } else if (OB_UNLIKELY(obj1.is_null() || obj2.is_null())) {
    result.set_null();
  } else {
    TYPE_CHECK(obj1, ObVarcharType);
    TYPE_CHECK(obj2, ObIntType);
    ObString ip_str = obj1.get_string();
    int64_t port = obj2.get_int();
    int64_t virtual_part_id = OB_INVALID_ID;
    ObAddr tmp_addr;
    tmp_addr.set_ip_addr(ip_str, static_cast<int32_t>(port));
    const ObTaskExecutorCtx::CalcVirtualPartitionIdParams& calc_params =
        task_exec_ctx->get_calc_virtual_part_id_params();
    if (false == calc_params.is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("calc params is not inited", K(ret), K(calc_params));
    } else if (OB_FAIL(task_exec_ctx->calc_virtual_partition_id(
                   calc_params.get_ref_table_id(), tmp_addr, virtual_part_id))) {
      LOG_WARN("fail to calc virtual partition id", K(ret), K(tmp_addr));
    } else {
      result.set_int(virtual_part_id);
    }
  }
  UNUSED(expr_ctx);
  return ret;
}

int ObExprFuncAddrToPartId::cg_expr(ObExprCGCtx&, const ObRawExpr&, ObExpr& rt_expr) const
{
  int ret = OB_SUCCESS;
  CK(2 == rt_expr.arg_cnt_);
  rt_expr.eval_func_ = eval_addr_to_part_id;
  return ret;
}

int ObExprFuncAddrToPartId::eval_addr_to_part_id(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  ObDatum* ip = NULL;
  ObDatum* port = NULL;
  ObTaskExecutorCtx* tec = NULL;
  if (OB_ISNULL(tec = ctx.exec_ctx_.get_task_executor_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("task executor ctx is NULL", K(ret));
  } else if (OB_FAIL(expr.eval_param_value(ctx, ip, port))) {
    LOG_WARN("evaluate parameter failed", K(ret));
  } else if (ip->is_null() || port->is_null()) {
    expr_datum.set_null();
  } else {
    int64_t part_id = OB_INVALID_ID;
    ObAddr addr;
    addr.set_ip_addr(ip->get_string(), static_cast<int32_t>(port->get_int()));
    const ObTaskExecutorCtx::CalcVirtualPartitionIdParams& calc_params = tec->get_calc_virtual_part_id_params();
    if (false == calc_params.is_inited()) {
      ret = OB_NOT_INIT;
      LOG_WARN("calc params is not inited", K(ret), K(calc_params));
    } else if (OB_FAIL(tec->calc_virtual_partition_id(calc_params.get_ref_table_id(), addr, part_id))) {
      LOG_WARN("fail to calc virtual partition id", K(ret), K(addr));
    } else {
      expr_datum.set_int(part_id);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
