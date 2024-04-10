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
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/engine/px/ob_dfo_scheduler.h"
#include "sql/engine/px/ob_px_scheduler.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_rpc_processor.h"
#include "sql/engine/px/ob_px_sqc_async_proxy.h"
#include "share/ob_server_blacklist.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "ob_px_coord_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

ObDfoSchedulerBasic::ObDfoSchedulerBasic(ObPxCoordInfo &coord_info,
                      ObPxRootDfoAction &root_dfo_action,
                      ObIPxCoordEventListener &listener)
  : coord_info_(coord_info),
    root_dfo_action_(root_dfo_action),
    listener_(listener)
{
}

int ObDfoSchedulerBasic::dispatch_root_dfo_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  ObPxTaskChSets parent_ch_sets;
  int64_t child_dfo_id = child.get_dfo_id();
  if (parent.is_root_dfo()) {
    ObDtlChTotalInfo *ch_info = nullptr;
    if (OB_FAIL(child.get_dfo_ch_info(0, ch_info))) {
      LOG_WARN("failed to get task receive chs", K(ret));
    } else if (!parent.check_root_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr", K(parent), K(ret));
    } else if (OB_FAIL(root_dfo_action_.receive_channel_root_dfo(ctx, parent, *ch_info))) {
      LOG_WARN("failed to receive channel for root dfo", K(ret));
    }
  }
  return ret;
}

int ObDfoSchedulerBasic::init_all_dfo_channel(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  /*do nothings*/
  UNUSED(ctx);
  return ret;
}

int ObDfoSchedulerBasic::prepare_schedule_info(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  // for temp table
  if (!coord_info_.p2p_temp_table_info_.temp_access_ops_.empty()) {
    ObIArray<ObSqlTempTableCtx>& temp_ctx = exec_ctx.get_temp_table_ctx();
    ObSqlTempTableCtx *ctx = nullptr;
    CK(coord_info_.p2p_temp_table_info_.temp_access_ops_.count() ==
       coord_info_.p2p_temp_table_info_.dfos_.count());
    for (int i = 0; i < coord_info_.p2p_temp_table_info_.dfos_.count() && OB_SUCC(ret); ++i) {
      ObDfo *parent_dfo = coord_info_.p2p_temp_table_info_.dfos_.at(i);
      ctx = nullptr;
      if (parent_dfo->need_p2p_info() && parent_dfo->get_p2p_dh_addrs().empty()) {
        for (int64_t j = 0; nullptr == ctx && j < temp_ctx.count(); j++) {
          if (parent_dfo->get_temp_table_id() == temp_ctx.at(j).temp_table_id_) {
            ctx = &temp_ctx.at(j);
          }
        }
        if (OB_NOT_NULL(ctx) && !ctx->interm_result_infos_.empty()) {
          for (int j = 0; OB_SUCC(ret) && j < ctx->interm_result_infos_.count(); ++j) {
            if (OB_FAIL(parent_dfo->get_p2p_dh_addrs().push_back(ctx->interm_result_infos_.at(j).addr_))) {
              LOG_WARN("fail to push back p2p dh addrs", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDfoSchedulerBasic::on_sqc_threads_inited(ObExecContext &ctx, ObDfo &dfo) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (OB_FAIL(dfo.prepare_channel_info())) {
    LOG_WARN("failed to prepare channel info", K(ret));
  }
  LOG_TRACE("on_sqc_threads_inited: dfo data xchg ch allocated", K(ret));
  return ret;
}

// 构建m * n的网络shuffle
int ObDfoSchedulerBasic::build_data_mn_xchg_ch(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  if (SM_NONE != parent.get_slave_mapping_type()
      || ObPQDistributeMethod::Type::PARTITION == child.get_dist_method()
      || ObPQDistributeMethod::Type::PARTITION_RANDOM == child.get_dist_method()
      || ObPQDistributeMethod::Type::PARTITION_HASH == child.get_dist_method()
      || ObPQDistributeMethod::Type::PARTITION_RANGE == child.get_dist_method()) {
    // 构建对应的channel map：目前channel map分为三种类型
    // 1. slave-mapping类型：会按照slave mapping的具体类型决定
    // 2. affinity+pw类型(PARTITION)：pkey类型，按照partition粒度匹配
    // 3. PARTITION_RANDOM：pkey类型，按照sqc粒度匹配
    uint64_t tenant_id = OB_INVALID_ID;
    if (OB_FAIL(get_tenant_id(ctx, tenant_id))) {
    } else if (OB_FAIL(ObSlaveMapUtil::build_mn_ch_map(
        ctx, child, parent, tenant_id))) {
      LOG_WARN("fail to build slave mapping group", K(ret));
    }
  } else {
    // 其他普通场景下的channel创建
    int64_t child_dfo_idx = -1;
    ObPxChTotalInfos *transmit_mn_ch_info = &child.get_dfo_ch_total_infos();
    uint64_t tenant_id = -1;
    if (OB_FAIL(ObDfo::check_dfo_pair(parent, child, child_dfo_idx))) {
      LOG_WARN("failed to check dfo pair", K(ret));
    } else if (OB_FAIL(get_tenant_id(ctx, tenant_id))) {
    } else if (OB_FAIL(ObSlaveMapUtil::build_mn_channel(
        transmit_mn_ch_info, child, parent, tenant_id))) {
      LOG_WARN("failed to build mn channel", K(ret));
    }
  }
  return ret;
}

int ObDfoSchedulerBasic::build_data_xchg_ch(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  ret = build_data_mn_xchg_ch(ctx, child, parent);
  return ret;
}

int ObDfoSchedulerBasic::dispatch_receive_channel_info_via_sqc(ObExecContext &ctx,
                                                                       ObDfo &child,
                                                                       ObDfo &parent,
                                                                       bool is_parallel_scheduler) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObPxTaskChSets parent_ch_sets;
  int64_t child_dfo_id = child.get_dfo_id();
  if (parent.is_root_dfo()) {
    if (OB_FAIL(dispatch_root_dfo_channel_info(ctx, child, parent))) {
      LOG_WARN("fail dispatch root dfo receive channel info", K(ret), K(parent), K(child));
    }
  } else {
    // 将 receive channels sets 按照 sqc 维度拆分并发送给各个 SQC
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(parent.get_sqcs(sqcs))) {
      LOG_WARN("fail get sqcs", K(parent), K(ret));
    } else {
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        int64_t sqc_id = sqcs.at(idx)->get_sqc_id();
        ObPxReceiveDataChannelMsg &receive_data_channel_msg = sqcs.at(idx)->get_receive_channel_msg();
        if (OB_INVALID_INDEX == sqc_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected param", K(sqc_id), K(ret));
        } else {
          ObDtlChTotalInfo *ch_info = nullptr;
          if (OB_FAIL(child.get_dfo_ch_info(idx, ch_info))) {
            LOG_WARN("failed to get task receive chs", K(ret));
          } else if (OB_FAIL(receive_data_channel_msg.set_payload(child_dfo_id, *ch_info))) {
            LOG_WARN("fail init msg", K(ret));
          } else if (!receive_data_channel_msg.is_valid()) {
            LOG_WARN("receive data channel msg is not valid", K(ret));
          } else if (!is_parallel_scheduler &&
              OB_FAIL(sqcs.at(idx)->add_serial_recieve_channel(receive_data_channel_msg))) {
            LOG_WARN("fail to add recieve channel", K(ret), K(receive_data_channel_msg));
          } else {
            LOG_TRACE("ObPxCoord::MsgProc::dispatch_receive_channel_info_via_sqc done.",
                      K(idx), K(cnt), K(sqc_id), K(child_dfo_id), K(parent_ch_sets));
          }
        }
      }
    }
  }
  return ret;
}

int ObDfoSchedulerBasic::set_temp_table_ctx_for_sqc(ObExecContext &ctx,
                                                    ObDfo &child) const
{
  int ret = OB_SUCCESS;
  ObArray<ObPxSqcMeta *> sqcs;
  if (OB_FAIL(child.get_sqcs(sqcs))) {
    LOG_WARN("failed to get sqcs from child.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); i++) {
      ObPxSqcMeta *sqc = sqcs.at(i);
      if (OB_ISNULL(sqc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null sqc", K(ret));
      } else if (OB_FAIL(sqc->get_temp_table_ctx().assign(ctx.get_temp_table_ctx()))) {
        LOG_WARN("failed to assign temp table ctx", K(ret));
      }
    }
  }
  return ret;
}

int ObDfoSchedulerBasic::get_tenant_id(ObExecContext &ctx, uint64_t &tenant_id) const
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else {
    tenant_id = session->get_effective_tenant_id();
  }
  return ret;
}

int ObDfoSchedulerBasic::dispatch_transmit_channel_info_via_sqc(ObExecContext &ctx,
                                                                        ObDfo &child,
                                                                        ObDfo &parent) const
{
  UNUSED(ctx);
  UNUSED(parent);
  int ret = OB_SUCCESS;
  ObPxTaskChSets child_ch_sets;
  ObPxPartChMapArray &map = child.get_part_ch_map();
  if (child.is_root_dfo()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("a child dfo should not be root dfo", K(child), K(ret));
  } else {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(child.get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        int64_t sqc_id = sqcs.at(idx)->get_sqc_id();
        ObPxTransmitDataChannelMsg &transmit_data_channel_msg = sqcs.at(idx)->get_transmit_channel_msg();
        ObDtlChTotalInfo *ch_info = nullptr;
        if (OB_FAIL(child.get_dfo_ch_info(sqc_id, ch_info))) {
          LOG_WARN("fail get child tasks", K(ret));
        } else if (OB_FAIL(transmit_data_channel_msg.set_payload(*ch_info, map))) {
          LOG_WARN("fail init msg", K(ret));
        }

        LOG_TRACE("ObPxCoord::MsgProc::dispatch_transmit_channel_info_via_sqc done."
                  "sent transmit_data_channel_msg to child task",
                  K(transmit_data_channel_msg), K(child), K(idx), K(cnt), K(ret));
      }
    }
  }
  return ret;
}

// -------------分割线-----------
int ObSerialDfoScheduler::init_all_dfo_channel(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  ObIArray<ObDfo *> &dfos = coord_info_.dfo_mgr_.get_all_dfos_for_update();
  for (int i = 0; OB_SUCC(ret) && i < dfos.count(); ++i) {
    ObDfo *child = dfos.at(i);
    ObDfo *parent = child->parent();
    if (OB_ISNULL(child) || OB_ISNULL(parent)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dfo is null", K(ret));
    } else if (!child->has_child_dfo() && !child->is_thread_inited()) {
      if (child->has_temp_table_scan()) {
        if (OB_FAIL(ObPXServerAddrUtil::alloc_by_temp_child_distribution(ctx,
                                                                         *child))) {
          LOG_WARN("fail alloc addr by temp child distribution", K(child), K(ret));
        } else { /*do nothing.*/ }
      } else if (OB_FAIL(ObPXServerAddrUtil::alloc_by_data_distribution(
          coord_info_.pruning_table_location_,
          ctx, *child))) {
        LOG_WARN("fail to alloc data distribution", K(ret));
      } else { /*do nothing.*/ }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_temp_table_ctx_for_sqc(ctx, *child))) {
          LOG_WARN("failed to set temp table ctx", K(ret));
        }
      }
    } else {
      /*do nothing*/
    }
    if (OB_SUCC(ret)) {
      if (parent->has_temp_table_scan() && !parent->is_thread_inited()) {
        if (OB_FAIL(ObPXServerAddrUtil::alloc_by_temp_child_distribution(ctx,
                                                                         *parent))) {
          LOG_WARN("fail alloc addr by data distribution", K(parent), K(ret));
        } else { /*do nohting.*/ }
      } else if (parent->is_root_dfo() && !parent->is_thread_inited() &&
          OB_FAIL(ObPXServerAddrUtil::alloc_by_local_distribution(ctx, *parent))) {
        LOG_WARN("fail to alloc local distribution", K(ret));
      } else if (!parent->is_root_dfo() &&
                 ObPQDistributeMethod::PARTITION_HASH == child->get_dist_method()) {
        if (OB_FAIL(ObPXServerAddrUtil::alloc_by_reference_child_distribution(
            coord_info_.pruning_table_location_,
            ctx,
            *child, *parent))) {
          LOG_WARN("fail alloc addr by data distribution", K(parent), K(child), K(ret));
        }
      } else if (!parent->is_root_dfo() && !parent->is_thread_inited() &&
          OB_FAIL(ObPXServerAddrUtil::alloc_by_data_distribution(
          coord_info_.pruning_table_location_, ctx, *parent))) {
        LOG_WARN("fail to alloc data distribution", K(ret));
      }
      if (OB_SUCC(ret) && !parent->is_scheduled()) {
        if (OB_FAIL(set_temp_table_ctx_for_sqc(ctx, *parent))) {
          LOG_WARN("failed to set temp table ctx", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_dfo_channel(ctx, child, parent))) {
        LOG_WARN("fail to init dfo channel", K(ret));
      }
    }
  }

  return ret;
}

int ObSerialDfoScheduler::init_data_xchg_ch(ObExecContext &ctx, ObDfo *dfo) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObArray<ObPxSqcMeta *> sqcs;
  if (OB_ISNULL(dfo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dfo is null", K(ret));
  } else if (OB_FAIL(dfo->get_sqcs(sqcs))) {
    LOG_WARN("fail get qc-sqc channel for QC", K(ret));
  } else {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      ObPxSqcMeta *sqc = sqcs.at(idx);
      if (OB_ISNULL(sqc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected sqc", K(ret));
      } else {
        sqc->set_task_count(1);
        sqc->set_thread_inited(true);
      }
    }
    if (OB_SUCC(ret)) {
      dfo->set_thread_inited(true);
      if (OB_FAIL(on_sqc_threads_inited(ctx, *dfo))) {
        LOG_WARN("failed to sqc thread init", K(ret));
      }
    }
  }
  return ret;
}
int ObSerialDfoScheduler::init_dfo_channel(ObExecContext &ctx, ObDfo *child, ObDfo *parent) const
{
  int ret = OB_SUCCESS;
  if (!child->is_thread_inited() && OB_FAIL(init_data_xchg_ch(ctx, child))) {
    LOG_WARN("fail to build data xchg ch", K(ret));
  } else if (!parent->is_thread_inited() && OB_FAIL(init_data_xchg_ch(ctx, parent))) {
    LOG_WARN("fail to build parent xchg ch", K(ret));
  } else if (OB_FAIL(build_data_xchg_ch(ctx, *child, *parent))) {
    LOG_WARN("fail to build data xchg ch", K(ret));
  } else if (OB_FAIL(dispatch_dtl_data_channel_info(ctx, *child, *parent))) {
    LOG_WARN("fail to dispatch dtl data channel", K(ret));
  }
  return ret;
}

int ObSerialDfoScheduler::dispatch_dtl_data_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dispatch_receive_channel_info_via_sqc(ctx, child,
      parent, /*is_parallel_scheduler*/false))) {
    LOG_WARN("fail to dispatch recieve channel", K(ret));
  } else if (OB_FAIL(dispatch_transmit_channel_info_via_sqc(ctx, child, parent))) {
    LOG_WARN("fail to dispatch transmit channel", K(ret));
  }
  return ret;
}

int ObSerialDfoScheduler::try_schedule_next_dfo(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(px_schedule);
  ObDfo *dfo = NULL;
  if (OB_FAIL(coord_info_.dfo_mgr_.get_ready_dfo(dfo))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail get ready dfos", K(ret));
    } else {
      LOG_TRACE("No more dfos to schedule", K(ret));
    }
  } else if (OB_ISNULL(dfo)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dfo is null, unexpected schedule", K(ret));
  } else if (OB_FAIL(do_schedule_dfo(ctx, *dfo))) {
    LOG_WARN("fail to do schedule dfo", K(ret));
  }
  return ret;
}

int ObSerialDfoScheduler::dispatch_sqcs(ObExecContext &exec_ctx,
                                        ObDfo &dfo,
                                        ObArray<ObPxSqcMeta *> &sqcs) const
{
  int ret = OB_SUCCESS;
  const ObPhysicalPlan *phy_plan = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSQLSessionInfo *session = NULL;
  ObCurTraceId::TraceId *cur_thread_id = NULL;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (phy_plan = dfo.get_phy_plan()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL plan ptr unexpected", K(ret));
    } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx NULL", K(ret));
    } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else if (OB_ISNULL(cur_thread_id = ObCurTraceId::get_trace_id())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory", K(ret));
    }
  }
  bool ignore_vtable_error = coord_info_.should_ignore_vtable_error();
  int64_t cluster_id = GCONF.cluster_id;
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    ObPxSqcMeta &sqc = *sqcs.at(idx);
    const ObAddr &addr = sqc.get_exec_addr();
    auto proxy = coord_info_.rpc_proxy_.to(addr);
    if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(addr, session->get_process_query_time()))) {
      if (!ignore_vtable_error) {
        ret = OB_RPC_CONNECT_ERROR;
        LOG_WARN("peer no in communication, maybe crashed", K(ret), K(sqc), K(cluster_id),
                K(session->get_process_query_time()));
      } else {
        LOG_WARN("ignore the black server list with virtual table", K(addr), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      SMART_VAR(ObPxRpcInitSqcArgs, args) {
        int64_t timeout_us = phy_plan_ctx->get_timeout_timestamp() - ObTimeUtility::current_time();
        ObFastInitSqcCB sqc_cb(addr,
                              *cur_thread_id,
                              &session->get_retry_info_for_update(),
                              phy_plan_ctx->get_timeout_timestamp(),
                              coord_info_.interrupt_id_,
                              &sqc);
        args.set_serialize_param(exec_ctx, const_cast<ObOpSpec &>(*dfo.get_root_op_spec()), *phy_plan);
        if ((NULL != dfo.parent() && !dfo.parent()->is_root_dfo()) ||
          coord_info_.enable_px_batch_rescan()) {
          sqc.set_transmit_use_interm_result(true);
        }
        if (NULL != dfo.parent() && dfo.parent()->is_root_dfo()) {
          sqc.set_adjoining_root_dfo(true);
        }
        if (dfo.has_child_dfo()) {
          sqc.set_recieve_use_interm_result(true);
        }
        if (ignore_vtable_error) {
          sqc.set_ignore_vtable_error(true);
        }
        if (coord_info_.enable_px_batch_rescan()) {
          OZ(sqc.set_rescan_batch_params(coord_info_.batch_rescan_ctl_->params_));
        }
        if (timeout_us <= 0) {
          ret = OB_TIMEOUT;
          LOG_WARN("dispatch sqc timeout", K(ret));
        } else if (OB_FAIL(args.sqc_.assign(sqc))) {
          LOG_WARN("fail assign sqc", K(ret));
        } else if (FALSE_IT(sqc.set_need_report(true))) {
          // 必须发 rpc 之前设置为 true
          // 原因见
        } else if (OB_FAIL(OB_E(EventTable::EN_PX_SQC_INIT_FAILED) OB_SUCCESS)) {
          sqc.set_need_report(false);
          LOG_WARN("[SIM] server down. fail to init sqc", K(ret));
          if (ignore_vtable_error && ObVirtualTableErrorWhitelist::should_ignore_vtable_error(ret)) {
            ObFastInitSqcReportQCMessageCall call(&sqc, ret, phy_plan_ctx->get_timeout_timestamp(), true);
            call.mock_sqc_finish_msg();
            ret = OB_SUCCESS;
            sqc.set_server_not_alive(true);
          }
        } else if (OB_FAIL(proxy
                          .by(THIS_WORKER.get_rpc_tenant()?: session->get_effective_tenant_id())
                          .timeout(timeout_us)
                          .fast_init_sqc(args, &sqc_cb))) {
          if (ignore_vtable_error && ObVirtualTableErrorWhitelist::should_ignore_vtable_error(ret)) {
            LOG_WARN("ignore error when init sqc with virtual table failed", K(ret), K(sqc));
            ObFastInitSqcReportQCMessageCall call(&sqc, ret, phy_plan_ctx->get_timeout_timestamp(), true);
            call.mock_sqc_finish_msg();
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to init sqc", K(ret), K(sqc));
          }
          sqc.set_need_report(false);
          sqc.set_server_not_alive(true);
        }
      }
    }
  }
  return ret;
}

int ObSerialDfoScheduler::do_schedule_dfo(ObExecContext &ctx, ObDfo &dfo) const
{
  int ret = OB_SUCCESS;
  ObArray<ObPxSqcMeta *> sqcs;
  if (OB_FAIL(dfo.get_sqcs(sqcs))) {
    LOG_WARN("fail get qc-sqc channel for QC", K(ret));
  } else {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      ObPxSqcMeta *sqc = sqcs.at(idx);
      if (OB_ISNULL(sqc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected sqc", K(ret));
      }
    }
  }
  LOG_TRACE("Dfo's sqcs count", K(dfo), "sqc_count", sqcs.count());

  ObSQLSessionInfo *session = NULL;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (session = ctx.get_my_session()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr session", K(ret));
    }
  }

  // 0. 分配 QC-SQC 通道信息
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    ObPxSqcMeta &sqc = *sqcs.at(idx);
    ObDtlChannelInfo &qc_ci = sqc.get_qc_channel_info();
    ObDtlChannelInfo &sqc_ci = sqc.get_sqc_channel_info();
    const ObAddr &sqc_exec_addr = sqc.get_exec_addr();
    const ObAddr &qc_exec_addr = sqc.get_qc_addr();
    if (OB_FAIL(ObDtlChannelGroup::make_channel(session->get_effective_tenant_id(),
                                                sqc_exec_addr, /* producer exec addr */
                                                qc_exec_addr, /* consumer exec addr */
                                                sqc_ci /* producer */,
                                                qc_ci /* consumer */))) {
      LOG_WARN("fail make channel for QC-SQC", K(ret));
    } else {
      LOG_TRACE("Make a new channel for qc & sqc",
                K(idx), K(cnt), K(sqc_ci), K(qc_ci), K(sqc_exec_addr), K(qc_exec_addr));
    }
  }

  int64_t thread_id = GETTID();
  // 1. 链接 QC-SQC 通道
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    ObPxSqcMeta &sqc = *sqcs.at(idx);
    ObDtlChannelInfo &ci = sqc.get_qc_channel_info();
    ObDtlChannel *ch = NULL;
    // ObDtlChannelGroup::make_channel 中已经填充好了 ci 的属性
    // 所以 link_channel 知道应该以何种方式建立 channel
    if (OB_FAIL(ObDtlChannelGroup::link_channel(ci, ch))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail add qc channel", K(ret));
    } else {
      ch->set_qc_owner();
      ch->set_thread_id(thread_id);
      (void)coord_info_.msg_loop_.register_channel(*ch);
      sqc.set_qc_channel(ch);
      LOG_TRACE("link qc-sqc channel and registered to qc msg loop. ready to receive sqc ctrl msg",
                K(idx), K(cnt), K(*ch), K(dfo), K(sqc));
    }
  }

  // 2. allocate branch_id for DML: replace, insert update, select for update
  if (OB_SUCC(ret) && dfo.has_need_branch_id_op()) {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      int16_t branch_id = 0;
      const int64_t max_task_count = sqcs.at(idx)->get_max_task_count();
      if (OB_FAIL(ObSqlTransControl::alloc_branch_id(ctx, max_task_count, branch_id))) {
        LOG_WARN("alloc branch id fail", KR(ret), K(max_task_count));
      } else {
        sqcs.at(idx)->set_branch_id_base(branch_id);
        LOG_TRACE("alloc branch id", K(max_task_count), K(branch_id), KPC(sqcs.at(idx)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dispatch_sqcs(ctx, dfo, sqcs))) {
      LOG_WARN("fail to dispatch sqc", K(ret));
    }
  }

  dfo.set_scheduled();
  return ret;
}

bool ObSerialDfoScheduler::CleanDtlIntermRes::operator()(const ObAddr &attr,
                                                         ObPxCleanDtlIntermResArgs *arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(coord_info_.rpc_proxy_.to(attr).by(tenant_id_).clean_dtl_interm_result(*arg, NULL))) {
    LOG_WARN("send clean dtl interm result rpc failed", K(ret), K(attr), KPC(arg));
  }
  LOG_TRACE("clean dtl res map", K(attr), K(*arg));
  arg->~ObPxCleanDtlIntermResArgs();
  arg = NULL;

  return true;
}

void ObSerialDfoScheduler::clean_dtl_interm_result(ObExecContext &exec_ctx)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObDfo *> &all_dfos = coord_info_.dfo_mgr_.get_all_dfos();
  ObDfo *last_dfo = all_dfos.at(all_dfos.count() - 1);
  int clean_ret = OB_E(EventTable::EN_ENABLE_CLEAN_INTERM_RES) OB_SUCCESS;
  if (clean_ret != OB_SUCCESS) {
    // Fault injection: Do not clean up interm results.
  } else if (OB_NOT_NULL(last_dfo) && last_dfo->is_scheduled() && OB_NOT_NULL(last_dfo->parent())
      && last_dfo->parent()->is_root_dfo()) {
    // all dfo scheduled, do nothing.
    LOG_TRACE("all dfo scheduled.");
  } else {
    const ObDfo *root = coord_info_.dfo_mgr_.get_root_dfo();
    int64_t batch_size = coord_info_.get_rescan_param_count();
    ObSEArray<ObDfo*, 8> dfos;
    ObLinearHashMap<ObAddr, ObPxCleanDtlIntermResArgs *> map;
    ObIAllocator &allocator = exec_ctx.get_allocator();
    for (int64_t i = 0; i < all_dfos.count(); i++) {
      ObDfo *dfo = all_dfos.at(i);
      ObDfo *parent = NULL;
      if (OB_NOT_NULL(dfo) && dfo->is_scheduled() && NULL != (parent = dfo->parent())
          && !parent->is_root_dfo() && !parent->is_scheduled()) {
        // if current dfo is scheduled but parent dfo is not scheduled.
        for (int64_t j = 0; j < parent->get_sqcs_count(); j++) {
          ObPxSqcMeta &sqc = parent->get_sqcs().at(j);
          int64_t msg_idx = 0;
          for (; msg_idx < sqc.get_serial_receive_channels().count(); msg_idx++) {
            if (sqc.get_serial_receive_channels().at(msg_idx).get_child_dfo_id() == dfo->get_dfo_id()) {
              break;
            }
          }
          if (OB_LIKELY(msg_idx < sqc.get_serial_receive_channels().count())) {
            ObPxCleanDtlIntermResArgs *arg = NULL;
            if (!map.is_inited() && OB_FAIL(map.init("CleanDtlRes", OB_SYS_TENANT_ID))) {
              LOG_WARN("init map failed", K(ret));
            } else if (OB_FAIL(map.get(sqc.get_exec_addr(), arg))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                void *buf = NULL;
                if (OB_ISNULL(buf = allocator.alloc(sizeof(ObPxCleanDtlIntermResArgs)))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("alloc failed", K(ret));
                } else {
                  arg = new(buf) ObPxCleanDtlIntermResArgs();
                  arg->batch_size_ = coord_info_.get_rescan_param_count();
                  if (OB_FAIL(map.insert(sqc.get_exec_addr(), arg))) {
                    LOG_WARN("insert failed", K(ret));
                  }
                }
              } else {
                LOG_WARN("get refactored failed", K(ret));
              }
            }
            if (OB_SUCC(ret) && OB_NOT_NULL(arg)) {
              ObPxReceiveDataChannelMsg &msg = sqc.get_serial_receive_channels().at(msg_idx);
              if (OB_FAIL(arg->info_.push_back(ObPxCleanDtlIntermResInfo(msg.get_ch_total_info(),
                          sqc.get_sqc_id(), sqc.get_task_count())))) {
                LOG_WARN("push back failed", K(ret));
              }
            }
          }
        }
      }
    }
    // ignore allocate, set_refactored and push_back failure.
    //  send rpc to addrs inserted into the map successfully.
    if (OB_UNLIKELY(map.count() != 0)) {
      LOG_TRACE("clean dtl res map", K(map.count()));
      ObSQLSessionInfo *session = exec_ctx.get_my_session();
      uint64_t tenant_id = OB_NOT_NULL(session) ? session->get_effective_tenant_id() : OB_SYS_TENANT_ID;
      CleanDtlIntermRes clean_dtl_interm_res(coord_info_, tenant_id);
      if (OB_FAIL(map.for_each(clean_dtl_interm_res))) {
        LOG_WARN("map for each clean_dtl_interm_res fail", KR(ret));
      }
    }
  }
}
// -------------分割线-----------

// 启动 DFO 的 SQC 线程
int ObParallelDfoScheduler::do_schedule_dfo(ObExecContext &exec_ctx, ObDfo &dfo) const
{
  int ret = OB_SUCCESS;
  ObArray<ObPxSqcMeta *> sqcs;
  if (OB_FAIL(dfo.get_sqcs(sqcs))) {
    LOG_WARN("fail get qc-sqc channel for QC", K(ret));
  } else {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      ObPxSqcMeta *sqc = sqcs.at(idx);
      if (OB_ISNULL(sqc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected sqc", K(ret));
      }
    }
  }

  LOG_TRACE("Dfo's sqcs count", K(dfo), "sqc_count", sqcs.count());

  ObSQLSessionInfo *session = NULL;
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (session = exec_ctx.get_my_session()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL ptr session", K(ret));
    }
  }
  // 0. 分配 QC-SQC 通道信息
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    ObPxSqcMeta &sqc = *sqcs.at(idx);
    ObDtlChannelInfo &qc_ci = sqc.get_qc_channel_info();
    ObDtlChannelInfo &sqc_ci = sqc.get_sqc_channel_info();
    const ObAddr &sqc_exec_addr = sqc.get_exec_addr();
    const ObAddr &qc_exec_addr = sqc.get_qc_addr();
    if (OB_FAIL(ObDtlChannelGroup::make_channel(session->get_effective_tenant_id(),
                                                sqc_exec_addr, /* producer exec addr */
                                                qc_exec_addr, /* consumer exec addr */
                                                sqc_ci /* producer */,
                                                qc_ci /* consumer */))) {
      LOG_WARN("fail make channel for QC-SQC", K(ret));
    } else {
      LOG_TRACE("Make a new channel for qc & sqc",
                K(idx), K(cnt), K(sqc_ci), K(qc_ci), K(sqc_exec_addr), K(qc_exec_addr));
    }
  }

  int64_t thread_id = GETTID();
  // 1. 链接 QC-SQC 通道
  ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
    ObPxSqcMeta &sqc = *sqcs.at(idx);
    ObDtlChannelInfo &ci = sqc.get_qc_channel_info();
    ObDtlChannel *ch = NULL;
    // ObDtlChannelGroup::make_channel 中已经填充好了 ci 的属性
    // 所以 link_channel 知道应该以何种方式建立 channel
    if (OB_FAIL(ObDtlChannelGroup::link_channel(ci, ch))) {
      LOG_WARN("fail link channel", K(ci), K(ret));
    } else if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail add qc channel", K(ret));
    } else {
      ch->set_qc_owner();
      ch->set_thread_id(thread_id);
      (void)coord_info_.msg_loop_.register_channel(*ch);
      sqc.set_qc_channel(ch);
      sqc.set_sqc_count(sqcs.count());
      LOG_TRACE("link qc-sqc channel and registered to qc msg loop. ready to receive sqc ctrl msg",
                K(idx), K(cnt), K(*ch), K(dfo), K(sqc));
    }
  }

  // 2. allocate branch_id for DML: replace, insert update, select for update
  if (OB_SUCC(ret) && dfo.has_need_branch_id_op()) {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      int16_t branch_id = 0;
      const int64_t max_task_count = sqcs.at(idx)->get_max_task_count();
      if (OB_FAIL(ObSqlTransControl::alloc_branch_id(exec_ctx, max_task_count, branch_id))) {
        LOG_WARN("alloc branch id fail", KR(ret), K(max_task_count));
      } else {
        sqcs.at(idx)->set_branch_id_base(branch_id);
        LOG_TRACE("alloc branch id", K(max_task_count), K(branch_id), KPC(sqcs.at(idx)));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // 下面的逻辑处理握手阶段超时的情况
    //  - 目的： 为了防止死锁
    //  - 方式： 一旦超时，则终止掉全部 sqc，等待一段事件后，整个 dfo 重试
    //  - 问题： init sqc 是异步的，其中部分 sqc 已经汇报了获取 task 的信息
    //           突然被终止，QC 方面的状态需要重新维护。但是存在下面的问题：
    //           场景举例：
    //            1. sqc1 成功，sqc2 超时
    //            2. dfo abort, clean sqc state
    //            3. sqc1 汇报已经分配好 task (old news)
    //            4. sqc1, sqc2 收到中断信息
    //            5. sqc1 重新调度
    //            6. sqc2 汇报已经分配好 task (latest news)
    //            7. qc 认为 dfo 都已全部调度成功 (实际上没有)
    //            8. sqc1 汇报分配好的 task (too late msg)
    //
    ret = dispatch_sqc(exec_ctx, dfo, sqcs);
  }
  return ret;
}

int ObParallelDfoScheduler::dispatch_dtl_data_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  /* 注意设置顺序：先设置 receive channel，再设置 transmit channel。
   * 这么做可以尽可能保证 transmit 发数据的时候 receive 端有人已经在监听，
   * 否则可能出现 transmit 发出的数据一段时间内无人接收，DTL 会疯狂重试影响系统性能
   */

  if (OB_SUCC(ret)) {
    if (parent.is_prealloc_receive_channel() && !parent.is_scheduled()) {
      // 因为 parent 中可以包含多个 receive 算子，仅仅对于调度时的场景
      // 才能通过 sqc 捎带，后继的 receive 算子 channel 信息都要走 dtl
      if (OB_FAIL(dispatch_receive_channel_info_via_sqc(ctx, child, parent))) {
        LOG_WARN("fail dispatch receive channel info", K(child), K(parent), K(ret));
      }
    } else {
      if (OB_FAIL(dispatch_receive_channel_info(ctx, child, parent))) {
        LOG_WARN("fail dispatch receive channel info", K(child), K(parent), K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (child.is_prealloc_transmit_channel() && !child.is_scheduled()) {
      if (OB_FAIL(dispatch_transmit_channel_info_via_sqc(ctx, child, parent))) {
        LOG_WARN("fail dispatch transmit channel info", K(child), K(ret));
      }
    } else {
      if (OB_FAIL(dispatch_transmit_channel_info(ctx, child, parent))) {
        LOG_WARN("fail dispatch transmit channel info", K(child), K(ret));
      }
    }
  }

  return ret;
}


int ObParallelDfoScheduler::dispatch_transmit_channel_info(ObExecContext &ctx, ObDfo &child, ObDfo &parent) const
{
  UNUSED(ctx);
  UNUSED(parent);
  int ret = OB_SUCCESS;
  ObPxTaskChSets child_ch_sets;
  ObPxPartChMapArray &map = child.get_part_ch_map();
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  // TODO: abort here to test transmit wait for channel info when inner_open.
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx NULL", K(ret));
  } else if (child.is_root_dfo()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("a child dfo should not be root dfo", K(child), K(ret));
  } else {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(child.get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
        int64_t sqc_id = sqcs.at(idx)->get_sqc_id();
        ObPxTransmitDataChannelMsg transmit_data_channel_msg;
        if (OB_ISNULL(ch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("qc channel should not be null", K(ret));
        } else {
          ObDtlChTotalInfo *ch_info = nullptr;
          if (OB_FAIL(child.get_dfo_ch_info(idx, ch_info))) {
            LOG_WARN("fail get child tasks", K(ret));
          } else if (OB_FAIL(transmit_data_channel_msg.set_payload(*ch_info, map))) {
            LOG_WARN("fail init msg", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ch->send(transmit_data_channel_msg,
              phy_plan_ctx->get_timeout_timestamp()))) { // 尽力而为，如果 push 失败就由其它机制处理
          LOG_WARN("fail push data to channel", K(ret));
        } else if (OB_FAIL(ch->flush(true, false))) {
          LOG_WARN("fail flush dtl data", K(ret));
        }
        LOG_TRACE("ObPxCoord::MsgProc::dispatch_transmit_channel_info done."
                  "sent transmit_data_channel_msg to child task",
                  K(transmit_data_channel_msg), K(child), K(idx), K(cnt), K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
        LOG_WARN("failed to wait for sqcs", K(ret));
      }
    }
  }
  return ret;
}

int ObParallelDfoScheduler::dispatch_receive_channel_info(ObExecContext &ctx,
                                                                  ObDfo &child,
                                                                  ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  ObPxTaskChSets parent_ch_sets;
  int64_t child_dfo_id = child.get_dfo_id();
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx NULL", K(ret));
  } else if (parent.is_root_dfo()) {
    if (OB_FAIL(dispatch_root_dfo_channel_info(ctx, child, parent))) {
      LOG_WARN("fail dispatch root dfo receive channel info", K(ret), K(parent), K(child));
    }
  } else {
    // 将 receive channels sets 按照 sqc 维度拆分并发送给各个 SQC
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(parent.get_sqcs(sqcs))) {
      LOG_WARN("fail get sqcs", K(parent), K(ret));
    } else {
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObDtlChannel *ch = sqcs.at(idx)->get_qc_channel();
        int64_t sqc_id = sqcs.at(idx)->get_sqc_id();
        ObPxReceiveDataChannelMsg receive_data_channel_msg;
        if (OB_ISNULL(ch) || OB_INVALID_INDEX == sqc_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected param", KP(ch), K(parent), K(sqc_id), K(ret));
        } else {
          ObDtlChTotalInfo *ch_info = nullptr;
          if (OB_FAIL(child.get_dfo_ch_info(idx, ch_info))) {
            LOG_WARN("failed to get task receive chs", K(ret));
          } else if (OB_FAIL(receive_data_channel_msg.set_payload(child_dfo_id, *ch_info))) {
            LOG_WARN("fail init msg", K(ret));
          } else if (!receive_data_channel_msg.is_valid()) {
            LOG_WARN("receive data channel msg is not valid", K(ret), K(receive_data_channel_msg));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(ch->send(receive_data_channel_msg,
              phy_plan_ctx->get_timeout_timestamp()))) { // 尽力而为，如果 push 失败就由其它机制处理
            LOG_WARN("fail push data to channel", K(ret));
          } else if (OB_FAIL(ch->flush(true, false))) {
            LOG_WARN("fail flush dtl data", K(ret));
          } else {
            LOG_TRACE("dispatched receive ch",
                      K(idx), K(cnt), K(*ch), K(sqc_id), K(child_dfo_id), K(parent_ch_sets));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ObPxChannelUtil::sqcs_channles_asyn_wait(sqcs))) {
        LOG_WARN("failed to wait for sqcs", K(ret));
      }
    }
  }
  return ret;
}

// 优化点：can prealloc 可以很早就预先计算好
int ObParallelDfoScheduler::check_if_can_prealloc_xchg_ch(ObDfo &child,
                                                                  ObDfo &parent,
                                                                  bool &bret) const
{
  int ret = OB_SUCCESS;
  bret = true;
  ObSEArray<const ObPxSqcMeta *, 16> sqcs;

  if (child.is_scheduled() || parent.is_scheduled()) {
    bret = false;
  } else if (OB_FAIL(child.get_sqcs(sqcs))) {
    LOG_WARN("fail to get child sqcs", K(ret));
  } else {
    ARRAY_FOREACH_X(sqcs, idx, cnt, true == bret) {
      const ObPxSqcMeta &sqc = *sqcs.at(idx);
      if (1 < sqc.get_max_task_count() ||
          1 < sqc.get_min_task_count()) {
        bret = false;
      }
    }
  }
  if (bret && OB_SUCC(ret)) {
    sqcs.reuse();
    if (OB_FAIL(parent.get_sqcs(sqcs))) {
      LOG_WARN("fail to get parent sqcs", K(ret));
    } else {
      ARRAY_FOREACH_X(sqcs, idx, cnt, true == bret) {
        const ObPxSqcMeta &sqc = *sqcs.at(idx);
        if (1 < sqc.get_max_task_count() ||
            1 < sqc.get_min_task_count()) {
          bret = false;
        }
      }
    }
  }
  return ret;
}

int ObParallelDfoScheduler::do_fast_schedule(ObExecContext &exec_ctx,
                                                     ObDfo &child,
                                                     ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  // 启用数据通道预分配模式，提示后面的逻辑不要再分配数据通道
  // 下面三个函数的调用顺序不能错，因为：
  //  1. 调用 root dfo 后，root dfo 会被标记为调度成功状态
  //  2. 然后假装 child dfo 也调度成功，
  //  经过上面两步，可以认为 child 和 parent 的线程数都确定，
  //  可以将 parent-child 的 channel 信息分配好
  //  3. 最后调度 child 的时候就能将 channel 信息捎带到 sqc 端
  if (OB_SUCC(ret) && !parent.is_scheduled()) {
    parent.set_prealloc_receive_channel(true);
    if (parent.has_parent() && parent.parent()->is_thread_inited()) {
      parent.set_prealloc_transmit_channel(true);
    }
    if (OB_FAIL(mock_on_sqc_init_msg(exec_ctx, parent))) {
      LOG_WARN("fail mock init parent dfo", K(parent), K(child), K(ret));
    }
  }
  if (OB_SUCC(ret) && !child.is_scheduled()) {
    child.set_prealloc_transmit_channel(true);
    if (OB_FAIL(mock_on_sqc_init_msg(exec_ctx, child))) {
      LOG_WARN("fail mock init child dfo", K(parent), K(child), K(ret));
    }
  }
  if (OB_SUCC(ret) && !parent.is_scheduled()) {
    if (OB_FAIL(schedule_dfo(exec_ctx, parent))) {
      LOG_WARN("fail schedule root dfo", K(parent), K(ret));
    }
  }
  if (OB_SUCC(ret) && !child.is_scheduled()) {
    if (OB_FAIL(schedule_dfo(exec_ctx, child))) {
      LOG_WARN("fail schedule child dfo", K(child), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("fast schedule ok", K(parent), K(child));
  }
  return ret;
}

int ObParallelDfoScheduler::mock_on_sqc_init_msg(ObExecContext &ctx, ObDfo &dfo) const
{
  int ret = OB_SUCCESS;
  ObArray<ObPxSqcMeta *> sqcs;
  if (dfo.is_root_dfo()) {
    // root dfo 无需 mock 这个消息
  } else if (OB_FAIL(dfo.get_sqcs(sqcs))) {
    LOG_WARN("fail get qc-sqc channel for QC", K(ret));
  } else {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      ObPxSqcMeta *sqc = sqcs.at(idx);
      if (OB_ISNULL(sqc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected sqc", K(ret));
      } else if (1 != sqc->get_max_task_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("only if all sqc task cnt is one", K(*sqc), K(ret));
      } else {
        ObPxInitSqcResultMsg pkt;
        pkt.dfo_id_ = sqc->get_dfo_id();
        pkt.sqc_id_ = sqc->get_sqc_id();
        pkt.rc_ = OB_SUCCESS;
        pkt.task_count_ = sqc->get_max_task_count();
        if (OB_FAIL(proc_.on_sqc_init_msg(ctx, pkt))) {
          LOG_WARN("fail mock sqc init msg", K(pkt), K(*sqc), K(ret));
        }
      }
    }
  }
  return ret;
}


int ObParallelDfoScheduler::schedule_dfo(ObExecContext &exec_ctx,
    ObDfo &dfo) const
{
  int ret = OB_SUCCESS;
  int retry_times = 0;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  /* 异常处理：
   * 1. Timeout Msg: 超时，不确定是否成功，此时可能会建立链路，如何处理？
   * 2. No Msg: DFO 链接 DTL 失败，无法返回消息
   * 3. DFO Msg: 线程不足导致 dispatch 失败
   *
   * 统一处理方式：
   * 1. QC 读 DTL，直到超时
   */
  NG_TRACE_EXT(dfo_start, OB_ID(dfo_id), dfo.get_dfo_id());
  if (dfo.is_root_dfo()) {
    if (OB_FAIL(on_root_dfo_scheduled(exec_ctx, dfo))) {
      LOG_WARN("fail setup root dfo", K(ret));
    }
  } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx NULL", K(ret));
  } else if (OB_FAIL(do_schedule_dfo(exec_ctx, dfo))) {
    LOG_WARN("fail dispatch dfo", K(ret));
  }
  // 无论成功失败，都标记为已调度。
  // 当 schedule 失败时，整个 query 就失败了。
  dfo.set_scheduled();
  LOG_TRACE("schedule dfo ok", K(dfo), K(retry_times), K(ret));
  return ret;
}


int ObParallelDfoScheduler::on_root_dfo_scheduled(ObExecContext &ctx, ObDfo &root_dfo) const
{
  int ret = OB_SUCCESS;
  ObPxSqcMeta *sqc = NULL;

  LOG_TRACE("on_root_dfo_scheduled", K(root_dfo));

  if (OB_FAIL(root_dfo.get_sqc(0, sqc))) {
    LOG_WARN("fail find sqc", K(root_dfo), K(ret));
  } else if (OB_ISNULL(sqc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(root_dfo), KP(sqc), K(ret));
  } else {
    sqc->set_task_count(1);
    sqc->set_thread_inited(true);
    root_dfo.set_thread_inited(true);
    root_dfo.set_used_worker_count(0);
    ret = on_sqc_threads_inited(ctx, root_dfo);
  }

  if (OB_SUCC(ret)) {
    if (root_dfo.is_thread_inited()) {
      // 尝试调度 self-child 对
      if (OB_SUCC(ret)) {
        int64_t cnt = root_dfo.get_child_count();
        ObDfo *child= NULL;
        if (1 != cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("root dfo should has only 1 child dfo", K(cnt), K(ret));
        } else if (OB_FAIL(root_dfo.get_child_dfo(0, child))) {
          LOG_WARN("fail get child dfo", K(cnt), K(ret));
        } else if (OB_ISNULL(child)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected", K(ret));
        } else if (child->is_thread_inited()) {
          // 因为 root-child 对中谁先 schedule 成功的时序是不定的
          // 任何一个后 schedule 成功的 dfo 都有义务推进 on_dfo_pair_thread_inited 消息
          // 例如，root-A-B 的调度里，A、B 已经调度成功，并且已经收到 thread init msg
          // 这时候调度 root，就需要 root 来推进 on_dfo_pair_thread_inited
          ret = proc_.on_dfo_pair_thread_inited(ctx, *child, root_dfo);
        }
      }
    }
  }
  return ret;
}

// 批量分发 DFO 到各个 server，构建 SQC
int ObParallelDfoScheduler::dispatch_sqc(ObExecContext &exec_ctx,
                                         ObDfo &dfo,
                                         ObArray<ObPxSqcMeta *> &sqcs) const
{
  int ret = OB_SUCCESS;
  bool fast_sqc = dfo.is_fast_dfo();

  const ObPhysicalPlan *phy_plan = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  ObSQLSessionInfo *session = NULL;

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(NULL == (phy_plan = dfo.get_phy_plan()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL plan ptr unexpected", K(ret));
    } else if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(exec_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy plan ctx NULL", K(ret));
    } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr != dfo.parent() && dfo.parent()->is_root_dfo()) {
    ARRAY_FOREACH(sqcs, idx) {
      ObPxSqcMeta &sqc = *sqcs.at(idx);
      sqc.set_adjoining_root_dfo(true);
    }
  }
  ObSEArray<ObPeerTaskState, 8> peer_states;
  ObSEArray<dtl::ObDtlChannel *, 8> dtl_channels;

  // 分发 sqc 可能需要重试，
  // 分发 sqc 的 rpc 成功，但 sqc 上无法分配最小个数的 worker 线程，`dispatch_sqc`内部进行重试，
  // 如果多次重试（达到超时时间）都无法成功，不需要再重试整个DFO（因为已经超时）
  ObPxSqcAsyncProxy proxy(coord_info_.rpc_proxy_, dfo, exec_ctx, phy_plan_ctx, session, phy_plan, sqcs);
  auto process_failed_proxy = [&]() {
    if (is_data_not_readable_err(ret) || is_server_down_error(ret)) {
      ObPxSqcMeta &sqc = *sqcs.at(proxy.get_error_index());
      LOG_WARN("fail to init sqc with proxy", K(ret), K(sqc), K(exec_ctx));
      int temp_ret = deal_with_init_sqc_error(exec_ctx, sqc, ret);
      if (temp_ret != OB_SUCCESS) {
        LOG_WARN("fail to deal with init sqc error", K(exec_ctx), K(sqc), K(temp_ret));
      }
    }
    // 对于正确process的sqc, 是需要sqc report的, 否则在后续的wait_running_dfo逻辑中不会等待此sqc结束
    const ObSqcAsyncCB *cb = NULL;
    const ObArray<ObSqcAsyncCB *> &callbacks = proxy.get_callbacks();
    for (int i = 0; i < callbacks.count(); ++i) {
      cb = callbacks.at(i);
      ObPxSqcMeta &sqc = *sqcs.at(i);
      if (OB_NOT_NULL(cb) && cb->is_processed() &&
          OB_SUCCESS == cb->get_ret_code().rcode_ &&
          OB_SUCCESS == cb->get_result().rc_) {
        sqc.set_need_report(true);
        ObPeerTaskState peer_state(sqc.get_sqc_addr());
        int push_ret = OB_SUCCESS;
        if (OB_SUCCESS != (push_ret = peer_states.push_back(peer_state))) {
          LOG_WARN("[DM] fail to push back", K(push_ret), K(sqc.get_sqc_addr()),
              K(dfo.get_px_detectable_ids().sqc_detectable_id_));
        } else if (OB_SUCCESS != (push_ret = dtl_channels.push_back(sqc.get_qc_channel()))) {
          LOG_WARN("[DM] fail to push back dtl channels", K(push_ret), K(sqc.get_sqc_addr()),
              K(dfo.get_px_detectable_ids().sqc_detectable_id_));
        }
      } else {
        // if init_sqc_msg is not processed and the msg may be sent successfully, set server not alive.
        // then when qc waiting_all_dfo_exit, it will push sqc.access_table_locations into trans_result,
        // and the query can be retried.
        sqc.set_server_not_alive(true);
      }
    }
  };
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy.launch_all_rpc_request())) {
    process_failed_proxy();
    LOG_WARN("fail to send all init async sqc", K(exec_ctx), K(ret));
  } else if (OB_FAIL(proxy.wait_all())) {
    // ret 可是能是 is_data_not_readable_err错误类型，需要通过`deal_with_init_sqc_error`进行处理
    process_failed_proxy();
    LOG_WARN("fail to wait all async init sqc", K(ret), K(exec_ctx));
  } else {
    const ObArray<ObSqcAsyncCB *> &callbacks = proxy.get_callbacks();
    ARRAY_FOREACH(callbacks, idx) {
      const ObSqcAsyncCB *cb = callbacks.at(idx);
      const ObPxRpcInitSqcResponse &resp = (*cb).get_result();
      ObPxSqcMeta &sqc = *sqcs.at(idx);
      sqc.set_need_report(true);
      ObPeerTaskState peer_state(sqc.get_sqc_addr());
      int push_ret = OB_SUCCESS;
      if (OB_SUCCESS != (push_ret = peer_states.push_back(peer_state))) {
        LOG_WARN("[DM] fail to push back", K(push_ret), K(sqc.get_sqc_addr()),
            K(dfo.get_px_detectable_ids().sqc_detectable_id_));
      } else if (OB_SUCCESS != (push_ret = dtl_channels.push_back(sqc.get_qc_channel()))) {
        LOG_WARN("[DM] fail to push back dtl channels", K(push_ret), K(sqc.get_sqc_addr()),
            K(dfo.get_px_detectable_ids().sqc_detectable_id_));
      }

      if (!fast_sqc) {
        ObPxInitSqcResultMsg pkt;
        pkt.dfo_id_ = sqc.get_dfo_id();
        pkt.sqc_id_ = sqc.get_sqc_id();
        pkt.rc_ = resp.rc_;
        pkt.task_count_ = resp.reserved_thread_count_;
        pkt.sqc_order_gi_tasks_ = resp.sqc_order_gi_tasks_;
        if (resp.reserved_thread_count_ < sqc.get_max_task_count()) {
          LOG_TRACE("SQC don`t have enough thread or thread auto scaling, Downgraded thread allocation",
              K(resp), K(sqc));
        }
        if (OB_FAIL(pkt.tablets_info_.assign(resp.partitions_info_))) {
          LOG_WARN("Failed to assign partition info", K(ret));
        } else if (OB_FAIL(proc_.on_sqc_init_msg(exec_ctx, pkt))) {
          LOG_WARN("fail to do sqc init callback", K(resp), K(pkt), K(ret));
        }
      }
    }
  }

  if (OB_NOT_NULL(phy_plan) && phy_plan->is_enable_px_fast_reclaim() && !peer_states.empty()
      && peer_states.count() == dtl_channels.count()) {
    int reg_ret = ObDetectManagerUtils::qc_register_check_item_into_dm(dfo, peer_states, dtl_channels);
    if (OB_SUCCESS != reg_ret) {
      LOG_WARN("[DM] qc failed to register_check_item_into_dm", K(reg_ret),
          K(dfo.get_px_detectable_ids().qc_detectable_id_));
    }
  }
  return ret;
}

int ObParallelDfoScheduler::deal_with_init_sqc_error(ObExecContext &exec_ctx,
                                                 const ObPxSqcMeta &sqc,
                                                 int rc) const
{
  int ret = OB_SUCCESS;
  if (is_data_not_readable_err(rc) || is_server_down_error(ret)) {
    // 分布式执行读到落后太多的备机或者正在回放日志的副本了，
    // 将远端的这个observer加进retry info的invalid servers中。
    const ObAddr &invalid_server = sqc.get_exec_addr();
    ObSQLSessionInfo *session = NULL;
    if (OB_ISNULL(session = GET_MY_SESSION(exec_ctx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is NULL", K(ret));
    } else { }
  }
  return ret;
}

/* 当发送 sqc 超时时，可能是遇到了死锁。
 * 应对策略是：终止 dfo 下所有 sqc，清空 qc-sqc 通道，
 * 等待一段时间，然后重新调度整个 dfo
 */
int ObParallelDfoScheduler::do_cleanup_dfo(ObDfo &dfo) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = ObInterruptUtil::broadcast_dfo(&dfo, OB_GOT_SIGNAL_ABORTING);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("fail broadcast interrupt to dfo", K(dfo), K(tmp_ret));
  } else {
    LOG_INFO("succ broadcast OB_GOT_SIGNAL_ABORTING to dfo sqcs", K(dfo));
  }
  tmp_ret = ObInterruptUtil::regenerate_interrupt_id(dfo);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("fail regenerate interrupt id for next round retry", K(dfo), K(tmp_ret));
  }
  // cleanup qc-sqc channel
  ObArray<ObPxSqcMeta *> sqcs;
  if (OB_FAIL(dfo.get_sqcs(sqcs))) {
    LOG_WARN("fail get dfo sqc", K(dfo), K(ret));
  } else {
    ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
      ObPxSqcMeta &sqc = *sqcs.at(idx);
      sqc.set_need_report(false);
      ObDtlChannel *ch = NULL;
      if (NULL != (ch = sqc.get_qc_channel())) {
        (void)coord_info_.msg_loop_.unregister_channel(*ch);
        sqc.set_qc_channel(NULL);
        ObDtlChannelInfo &ci = sqc.get_qc_channel_info();
        if (OB_FAIL(ObDtlChannelGroup::unlink_channel(ci))) {
          LOG_WARN("fail unlink channel", K(ci), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObParallelDfoScheduler::try_schedule_next_dfo(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  FLTSpanGuard(px_schedule);
  ObSEArray<ObDfo *, 3> dfos;
  while (OB_SUCC(ret)) {
    // 每次只迭代出一对 DFO，parent & child
    if (OB_FAIL(coord_info_.dfo_mgr_.get_ready_dfos(dfos))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail get ready dfos", K(ret));
      } else {
        LOG_TRACE("No more dfos to schedule", K(ret));
      }
    } else if (0 == dfos.count()) {
      LOG_TRACE("No dfos to schedule for now. wait");
      break;
    } else if (2 != dfos.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get_ready_dfo should output a pair of dfo",
               "actual", dfos.count(), "expect", 2, K(ret));
    } else if (OB_ISNULL(dfos.at(0)) || OB_ISNULL(dfos.at(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpetected", K(ret));
    } else {
      /*
       * 和 get_ready_dfo() 约定，0 号是 child，1 号是 parent：
       *
       *   parent  <-- 1
       *   /
       * child  <-- 0
       */
      ObDfo &child = *dfos.at(0);
      ObDfo &parent = *dfos.at(1);
      LOG_TRACE("to schedule", K(parent), K(child));
      if (OB_FAIL(schedule_pair(ctx, child, parent))) {
        LOG_WARN("fail schedule parent and child", K(ret));
      }
      FLT_SET_TAG(dfo_id, parent.get_dfo_id(),
                  qc_id, parent.get_qc_id(),
                  used_worker_cnt, parent.get_used_worker_count());
    }
  }
  return ret;
}

int ObParallelDfoScheduler::schedule_pair(ObExecContext &exec_ctx,
                                                  ObDfo &child,
                                                  ObDfo &parent) const
{
  int ret = OB_SUCCESS;
  //
  // for scan dfo:  dop + ranges -> dop + svr -> (svr1, th_cnt1), (svr2, th_cnt2), ...
  // for other dfo: dop + child svr -> (svr1, th_cnt1), (svr2, th_cnt2), ...
  //
  if (OB_SUCC(ret)) {
    // 调度一定是成对调度的，任何一个 child 调度起来时，它的 parent 一定已经调度成功
    if (!child.is_scheduled() && child.has_child_dfo()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("a interm node with child running should not be in state of unscheduled",
               K(child), K(ret));
    } else if (!child.is_scheduled()) {
      if (child.has_temp_table_scan()) {
        if (OB_FAIL(ObPXServerAddrUtil::alloc_by_temp_child_distribution(exec_ctx,
                                                                         child))) {
          LOG_WARN("fail alloc addr by temp table distribution", K(child), K(ret));
        } else { /*do nohting.*/ }
      } else if (OB_FAIL(ObPXServerAddrUtil::alloc_by_data_distribution(
            coord_info_.pruning_table_location_,
            exec_ctx,
            child))) {
        LOG_WARN("fail alloc addr by data distribution", K(child), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_temp_table_ctx_for_sqc(exec_ctx, child))) {
          LOG_WARN("failed to set temp table ctx", K(ret));
        }
      }
      LOG_TRACE("alloc_by_data_distribution", K(child));
    } else {
      // already in schedule, pass
    }
  }
  if (OB_SUCC(ret)) {
    if (!parent.is_scheduled()) {
      if (parent.has_temp_table_scan()) {
        if (OB_FAIL(ObPXServerAddrUtil::alloc_by_temp_child_distribution(exec_ctx,
                                                                         parent))) {
          LOG_WARN("fail alloc addr by data distribution", K(parent), K(ret));
        } else { /*do nohting.*/ }
      } else if (parent.is_root_dfo()) {
        // QC/local dfo，直接在本机本线程执行，无需计算执行位置
        if (OB_FAIL(ObPXServerAddrUtil::alloc_by_local_distribution(exec_ctx,
                                                                    parent))) {
          LOG_WARN("alloc SQC on local failed", K(parent), K(ret));
        }
      } else {
        // DONE (xiaochu): 如果 parent dfo 里面自带了 scan，那么存在一种情况：dfo 按照 scan
        // 数据所在的位置分配， 而 child dfo 的数据需要主动 shuffle 到 parent 所在的机器。
        // 一般来说，有三种情况：
        // (1) parent 向 child 靠，适合于 parent 为纯计算节点的场景
        // (2) parent 独立，child 数据向 parent shuffle，适合于 parent 自己也要读盘的场景；
        //     其中pdml global index maintain符合这种情况。
        // (3) parent、child 完全独立，各自根据各自的情况分配位置，适合于需要扩展计算能力的场景
        // (4) parent、child是特殊的slave mapping关系，需要将parent按照reference table的分布来分配
        // sqc。
        //
        // 下面只实现了第一种、第二种情况，第三种需求不明确，列为 TODO
        if (parent.has_scan_op() || parent.has_dml_op()) { // 参考 Partial Partition Wise Join
          // 当DFO中存在TSC或者pdml中的global index maintain op：
          // 1. 当存在TSC情况下，sqcs的location信息使用tsc表的location信息
          // 2. 当是pdml、dml+px情况下，sqcs的locations信息使用DML对应的表的locations
          if (OB_FAIL(ObPXServerAddrUtil::alloc_by_data_distribution(
            coord_info_.pruning_table_location_, exec_ctx, parent))) {
            LOG_WARN("fail alloc addr by data distribution", K(parent), K(ret));
          }
          LOG_TRACE("alloc_by_data_distribution", K(parent));
        } else if (parent.is_single()) {
          // parent 可能是一个 scalar group by，会被标记为 is_local，此时
          // 走 alloc_by_data_distribution，内部会分配一个 QC 本地线程来执行
          if (OB_FAIL(ObPXServerAddrUtil::alloc_by_data_distribution(
            coord_info_.pruning_table_location_, exec_ctx, parent))) {
            LOG_WARN("fail alloc addr by data distribution", K(parent), K(ret));
          }
          LOG_TRACE("alloc_by_local_distribution", K(parent));
        } else if (ObPQDistributeMethod::PARTITION_HASH == child.get_dist_method()) {
          if (OB_FAIL(ObPXServerAddrUtil::alloc_by_reference_child_distribution(
                  coord_info_.pruning_table_location_,
                  exec_ctx,
                  child, parent))) {
            LOG_WARN("fail alloc addr by data distribution", K(parent), K(child), K(ret));
          }
        } else if (child.is_slave_mapping()) {
          if (OB_UNLIKELY(ObPQDistributeMethod::HASH != child.get_dist_method())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected dist method for slave mapping", K(ret), K(parent), K(child));
          } else if (OB_FAIL(ObPXServerAddrUtil::alloc_by_child_distribution(child, parent))) {
            LOG_WARN("alloc by child distribution failed", K(ret));
          }
        } else if (OB_FAIL(ObPXServerAddrUtil::alloc_by_random_distribution(exec_ctx, child, parent))) {
          LOG_WARN("fail alloc addr by data distribution", K(parent), K(child), K(ret));
        }
        LOG_TRACE("alloc_by_child_distribution", K(child), K(parent));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(set_temp_table_ctx_for_sqc(exec_ctx, parent))) {
          LOG_WARN("failed to set temp table ctx", K(ret));
        }
      }
    } else {
      // already in schedule, pass
    }
  }



  // 优化分支：QC 和它的 child dfo 之前的数据通道在满足一定条件时尽早分配
  bool can_prealloc = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_if_can_prealloc_xchg_ch(child, parent, can_prealloc))) {
      LOG_WARN("fail check can prealloc xchange, ingore", K(ret));
    } else if (can_prealloc) {
      if (OB_FAIL(do_fast_schedule(exec_ctx, child, parent))) {
        LOG_WARN("fail do fast schedule", K(parent), K(child), K(ret));
      }
    }
  }

  // 注意：不用担心重复调度，原因如下：
  // 如果上面的 do_fast_schedule 成功调度了 parent / child
  // 那么它的 is_schedule 状态会被更新为 true，下面的 schedule_dfo
  // 显然就不会被调度。
  //
    // schedule child first
    // because child can do some useful (e.g. scan) work while parent is scheduling
  if (OB_SUCC(ret)) {
    if (!child.is_scheduled()) {
      if (OB_FAIL(schedule_dfo(exec_ctx, child))) { // 发送 DFO 到各个 server
        LOG_WARN("fail schedule dfo", K(child), K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (!parent.is_scheduled()) {
      if (OB_FAIL(schedule_dfo(exec_ctx, parent))) { // 发送 DFO 到各个 server
        LOG_WARN("fail schedule dfo", K(parent), K(ret));
      }
    }
  }

  return ret;
}
