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

#include "ob_temp_table_transformation_op.h"
#include "ob_temp_table_insert_op.h"
#include "sql/engine/ob_operator_reg.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace share;
using namespace share::schema;
using namespace obrpc;
namespace sql
{
#define USE_MULTI_GET_ARRAY_BINDING 1

DEF_TO_STRING(ObTempTableTransformationOpSpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("op_spec");
  J_COLON();
  pos += ObOpSpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER_INHERIT(ObTempTableTransformationOpSpec, ObOpSpec);

int ObTempTableTransformationOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan the operator.", K(ret));
  } else { /*do nothing.*/ }
  return ret;
}

int ObTempTableTransformationOp::inner_open()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObTempTableTransformationOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  ObExecContext &ctx = get_exec_ctx();
  if (init_temp_table_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_child_cnt() - 1; ++i) {
      int64_t temp_table_count = ctx.get_temp_table_ctx().count();
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child op is null");
      } else if (OB_FALSE_IT(ret = children_[i]->get_next_row())) {
      } else if (ret != OB_ITER_END) {
        LOG_WARN("failed to get next row.", K(ret));
      } else {
        ret = OB_SUCCESS;
        while(OB_SUCC(ret) && ctx.get_temp_table_ctx().count() <= temp_table_count) {
          if (OB_FAIL(check_status())) {
            LOG_WARN("failed to wait temp table finish msg", K(ret));
          } else {
            ob_usleep(1000);
          }
        }
      }
    }
    init_temp_table_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(children_[get_child_cnt() - 1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(children_[get_child_cnt() - 1]->get_next_row())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next row.", K(ret));
    } else { /*do nothing.*/ }
  } else { /*do nothing.*/ }
  return ret;
}

int ObTempTableTransformationOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  clear_datum_eval_flag();
  if (init_temp_table_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_child_cnt() - 1; ++i) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child op is null");
      } else if (OB_FAIL(children_[i]->get_next_batch(max_row_cnt, child_brs))) {
        LOG_WARN("failed to get next row batch.", K(ret));
      }
    }
    init_temp_table_ = false;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(children_[get_child_cnt() - 1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null");
  } else if (OB_FAIL(children_[get_child_cnt() - 1]->get_next_batch(
                 max_row_cnt, child_brs))) {
    LOG_WARN("failed to get next batch.", K(ret));
  } else { /*do nothing.*/
  }
  (void)brs_.copy(child_brs);
  return ret;
}

int ObTempTableTransformationOp::inner_close()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destory_interm_results())) {
    LOG_WARN("failed to destory interm results.", K(ret));
  }
  return ret;
}

int ObTempTableTransformationOp::destory_interm_results()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 2> svrs;
  ObSEArray<ObEraseDtlIntermResultArg, 2> args;
  ObEraseDtlIntermResultArg interm_ids;
  ObExecContext &ctx = get_exec_ctx();
  const int64_t temp_table_count = ctx.get_temp_table_ctx().count();
  int64_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_count; ++i) {
    ObSqlTempTableCtx &temp_table_ctx = ctx.get_temp_table_ctx().at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < temp_table_ctx.interm_result_infos_.count(); ++j) {
      ObTempTableResultInfo &result_info = temp_table_ctx.interm_result_infos_.at(j);
      if (result_info.addr_ == ctx.get_addr() || temp_table_ctx.is_local_interm_result_) {
        if (OB_FAIL(destory_local_interm_results(result_info.interm_result_ids_))) {
          LOG_WARN("failed to destory interm results.", K(ret));
        }
      } else if (has_exist_in_array(svrs, result_info.addr_, &idx)) {
        if (OB_UNLIKELY(idx < 0 || idx >= args.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected idx.", K(ret), K(idx), K(args.count()));
        } else if (OB_FAIL(append(args.at(idx).interm_result_ids_,
                                  result_info.interm_result_ids_))) {
          LOG_WARN("failed to append args", K(ret));
        }
      } else if (OB_FAIL(svrs.push_back(result_info.addr_))) {
        LOG_WARN("failed to push back svr", K(ret));
      } else if (OB_FAIL(interm_ids.interm_result_ids_.assign(result_info.interm_result_ids_))) {
        LOG_WARN("failed to assign interm result ids", K(ret));
      } else if (OB_FAIL(args.push_back(interm_ids))) {
        LOG_WARN("failed to push back args", K(ret));
      }
    }
  }

#ifdef ERRSIM
  int ecode = EventTable::EN_PX_TEMP_TABLE_NOT_DESTROY_REMOTE_INTERM_RESULT;
  if (OB_SUCCESS != ecode && OB_SUCC(ret)) {
    LOG_WARN("ObTempTableTransformationOp not destory_remote_interm_results by design", K(ret));
    return ret;
  }
#endif

  if (OB_SUCC(ret) && !svrs.empty() &&
      OB_FAIL(destory_remote_interm_results(svrs, args))) {
    LOG_WARN("failed to destory interm results", K(ret));
  }
  return ret;
}

int ObTempTableTransformationOp::destory_remote_interm_results(ObIArray<ObAddr> &svrs,
                                                               ObIArray<ObEraseDtlIntermResultArg> &args)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("destory_interm_results use rpc", K(svrs));
  ObExecContext &ctx = get_exec_ctx();
  ObSQLSessionInfo *session = ctx.get_my_session();
  ObPhysicalPlanCtx *plan_ctx = ctx.get_physical_plan_ctx();
  ObExecutorRpcImpl *rpc = NULL;
  ObExecutorRpcProxy *proxy = NULL;
  if (OB_ISNULL(session) || OB_ISNULL(plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session snap or plan ctx snap is NULL", K(ret), K(session), K(plan_ctx));
  } else if (OB_FAIL(ObTaskExecutorCtxUtil::get_task_executor_rpc(ctx, rpc))) {
    LOG_ERROR("fail get rpc", K(ret));
  } else if (OB_ISNULL(rpc) || OB_ISNULL(proxy = rpc->get_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc is NULL", K(ret), K(rpc), K(proxy));
  } else if (OB_UNLIKELY(svrs.count() != args.count())) {
    LOG_WARN("unexpected array count", K(ret), K(svrs), K(args));
  } else {
    uint64_t tenant_id = THIS_WORKER.get_rpc_tenant() > 0 ? THIS_WORKER.get_rpc_tenant()
                                                          : session->get_rpc_tenant_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < svrs.count(); ++i) {
      int64_t timeout_timestamp = plan_ctx->get_timeout_timestamp();
      int64_t timeout = timeout_timestamp - ::oceanbase::common::ObTimeUtility::current_time();
      if (OB_UNLIKELY(timeout <= 0)) {
        ret = OB_TIMEOUT;
        LOG_WARN("task_execute timeout before rpc", K(ret), K(svrs.at(i)), K(timeout),
                                                    K(timeout_timestamp));
      } else if (OB_FAIL(proxy->to(svrs.at(i))
                                .by(tenant_id)
                                .timeout(timeout)
                                .erase_dtl_interm_result(args.at(i), NULL))) {
        LOG_WARN("rpc close_result fail", K(ret), K(svrs.at(i)), K(tenant_id),
                                          K(args.at(i)), K(timeout), K(timeout_timestamp));
      }
    }
  }
  return ret;
}

int ObTempTableTransformationOp::destory_local_interm_results(ObIArray<uint64_t> &result_ids)
{
  int ret = OB_SUCCESS;
  dtl::ObDTLIntermResultKey dtl_int_key;
  LOG_TRACE("destory interm results", K(get_exec_ctx().get_addr()), K(result_ids));
  for (int64_t i = 0; OB_SUCC(ret) && i < result_ids.count(); ++i) {
    dtl_int_key.channel_id_ = result_ids.at(i);
    if (OB_FAIL(MTL(dtl::ObDTLIntermResultManager*)->erase_interm_result_info(
                                                                            dtl_int_key))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("interm result may erased by DM", K(ret));
      } else {
        LOG_WARN("failed to erase interm result info in manager.", K(ret));
      }
    }
  }
  return ret;
}

void ObTempTableTransformationOp::destroy()
{
  ObOperator::destroy();
}

} // end namespace sql
} // end namespace oceanbase

