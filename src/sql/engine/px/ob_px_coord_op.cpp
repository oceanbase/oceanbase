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

#include "ob_px_coord_op.h"
#include "lib/random/ob_random.h"
#include "share/ob_rpc_share.h"
#include "share/schema/ob_part_mgr_util.h"
#include "sql/ob_sql.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#include "share/config/ob_server_config.h"
#include "sql/engine/px/ob_px_sqc_async_proxy.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "sql/engine/px/datahub/components/ob_dh_range_dist_wf.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "sql/dtl/ob_dtl_utils.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/px/exchange/ob_px_ms_coord_op.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
#include "share/detect/ob_detect_manager_utils.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace sql::dtl;
namespace sql
{
OB_SERIALIZE_MEMBER(ObPxCoordOp::ObPxBatchOpInfo, op_type_, op_id_);

OB_DEF_SERIALIZE(ObPxCoordSpec)
{
  int ret = OB_SUCCESS;
  BASE_SER((ObPxCoordSpec, ObPxReceiveSpec));
  LST_DO_CODE(OB_UNIS_ENCODE,
              px_expected_worker_count_,
              qc_id_,
              batch_op_info_,
              table_locations_,
              sort_exprs_,
              sort_collations_,
              sort_cmp_funs_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPxCoordSpec)
{
  int ret = OB_SUCCESS;
  BASE_DESER((ObPxCoordSpec, ObPxReceiveSpec));
  LST_DO_CODE(OB_UNIS_DECODE, px_expected_worker_count_, qc_id_, batch_op_info_);
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret) && count > 0) {
    if (OB_ISNULL(table_locations_.get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected fix array", K(ret));
    } else if (OB_FAIL(table_locations_.prepare_allocate(count, *table_locations_.get_allocator()))) {
      LOG_WARN("fail to init table location array item", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i ++) {
        OB_UNIS_DECODE(table_locations_.at(i));
      }
    }
  }
  OB_UNIS_DECODE(sort_exprs_);
  OB_UNIS_DECODE(sort_collations_);
  OB_UNIS_DECODE(sort_cmp_funs_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxCoordSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObPxCoordSpec, ObPxReceiveSpec));
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              px_expected_worker_count_,
              qc_id_,
              batch_op_info_,
              table_locations_,
              sort_exprs_,
              sort_collations_,
              sort_cmp_funs_);
  return len;
}

OB_INLINE const ObPxCoordSpec &get_my_spec(const ObPxCoordOp &op)
{
  return static_cast<const ObPxCoordSpec &>(op.get_spec());
}

ObPxCoordOp::ObPxCoordOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
: ObPxReceiveOp(exec_ctx, spec, input),
  allocator_(common::ObModIds::OB_SQL_PX),
  row_allocator_(common::ObModIds::OB_SQL_PX),
  coord_info_(*this, allocator_, msg_loop_, interrupt_id_),
  root_dfo_(NULL),
  root_receive_ch_provider_(),
  first_row_fetched_(false),
  first_row_sent_(false),
  qc_id_(common::OB_INVALID_ID),
  first_buffer_cache_(allocator_),
  register_interrupted_(false),
  px_sequence_id_(0),
  interrupt_id_(0),
  register_detectable_id_(false),
  detectable_id_(),
  px_dop_(1),
  time_recorder_(0),
  batch_rescan_param_version_(0),
  server_alive_checker_(coord_info_.dfo_mgr_, exec_ctx.get_my_session()->get_process_query_time()),
  last_px_batch_rescan_size_(0)
{}


int ObPxCoordOp::init_dfc(ObDfo &dfo, dtl::ObDtlChTotalInfo *ch_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(dfc_.init(ctx_.get_my_session()->get_effective_tenant_id(),
                        task_ch_set_.count()))) {
    LOG_WARN("Fail to init dfc", K(ret));
  } else if (OB_INVALID_ID == dfo.get_qc_id() || OB_INVALID_ID == dfo.get_dfo_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: dfo or qc id is invalid", K(dfo),
      K(dfo.get_qc_id()), K(dfo.get_dfo_id()));
  } else {
    ObDtlDfoKey dfo_key;
    dfo_key.set(GCTX.server_id_, dfo.get_px_sequence_id(), dfo.get_qc_id(), dfo.get_dfo_id());
    dfc_.set_timeout_ts(phy_plan_ctx->get_timeout_timestamp());
    dfc_.set_receive();
    dfc_.set_qc_coord();
    dfc_.set_dfo_key(dfo_key);
    dfc_.set_dtl_channel_watcher(&msg_loop_);
    dfc_.set_total_ch_info(ch_info);
    DTL.get_dfc_server().register_dfc(dfc_);
    dfc_.set_op_metric(&metric_);
    bool force_block = false;
#ifdef ERRSIM
    int ret = OB_SUCCESS;
    ret = OB_E(EventTable::EN_FORCE_DFC_BLOCK) ret;
    force_block = (OB_HASH_NOT_EXIST == ret);
    LOG_TRACE("Worker init dfc", K(dfo_key), K(dfc_.is_receive()), K(force_block), K(ret));
    ret = OB_SUCCESS;
#endif
    ObDtlLocalFirstBufferCache *buf_cache = nullptr;
    if (OB_FAIL(DTL.get_dfc_server().get_buffer_cache(
                ctx_.get_my_session()->get_effective_tenant_id(), dfo_key, buf_cache))) {
      LOG_WARN("failed to get buffer cache", K(dfo_key));
    } else {
      dfc_.set_first_buffer_cache(buf_cache);
    }
    LOG_TRACE("QC init dfc", K(dfo_key), K(dfc_.is_receive()), K(force_block));
  }
  return ret;
}

void ObPxCoordOp::debug_print(ObDfo &root)
{
  // print plan tree
  const ObPhysicalPlan *phy_plan = root.get_phy_plan();
  if (NULL != phy_plan) {
    LOG_TRACE("ObPxCoord PLAN", "plan", *phy_plan);
  }
  // print dfo tree
  debug_print_dfo_tree(0, root);
}

void ObPxCoordOp::debug_print_dfo_tree(int level, ObDfo &dfo)
{
  int ret = OB_SUCCESS;
  const ObOpSpec *op_spec = dfo.get_root_op_spec();
  if (OB_ISNULL(op_spec)) {
    LOG_TRACE("DFO",
              K(level),
              K(dfo));
  } else {
    LOG_TRACE("DFO",
              K(level),
              "dfo_root_op_type", op_spec->type_,
              "dfo_root_op_id", op_spec->id_,
              K(dfo));
  }
  level++;
  int64_t cnt = dfo.get_child_count();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < cnt; ++idx) {
    ObDfo *child_dfo = NULL;
    if (OB_FAIL(dfo.get_child_dfo(idx, child_dfo))) {
      LOG_WARN("fail get child_dfo", K(idx), K(cnt), K(ret));
    } else if (OB_ISNULL(child_dfo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_dfo is null", K(ret));
    } else {
      debug_print_dfo_tree(level, *child_dfo);
    }
  }
}

int ObPxCoordOp::inner_rescan()
{
  return ObOperator::inner_rescan();
}

//PX coord has its own rescan
int ObPxCoordOp::rescan()
{
  int ret = OB_SUCCESS;
  if (NULL == coord_info_.batch_rescan_ctl_
      || batch_rescan_param_version_ != coord_info_.batch_rescan_ctl_->param_version_) {
    ObDfo *root_dfo = NULL;
    if (OB_FAIL(inner_rescan())) {
      LOG_WARN("failed to do inner rescan", K(ret));
    } else {
      int terminate_ret = OB_SUCCESS;
      if (OB_SUCCESS != (terminate_ret = terminate_running_dfos(coord_info_.dfo_mgr_))) {
        if (OB_GOT_SIGNAL_ABORTING == terminate_ret) {
          LOG_WARN("fail to release px resources in QC rescan, ignore ret", K(ret));
        } else {
          ret = terminate_ret;
          LOG_WARN("fail to release px resources in QC rescan", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(ObDetectManagerUtils::qc_unregister_detectable_id_from_dm(detectable_id_, register_detectable_id_))) {
    } else if (FALSE_IT(clear_interrupt())) {
    } else if (OB_FAIL(destroy_all_channel())) {
      LOG_WARN("release dtl channel failed", K(ret));
    } else if (FALSE_IT(unregister_first_buffer_cache())) {
      LOG_WARN("failed to register first buffer cache", K(ret));
    } else if (OB_FAIL(free_allocator())) {
      LOG_WARN("failed to free allocator", K(ret));
    } else if (FALSE_IT(reset_for_rescan())) {
      // nop
    } else if (MY_SPEC.batch_op_info_.is_inited() && OB_FAIL(init_batch_info())) {
      LOG_WARN("fail to init batch info", K(ret));
    } else if (FALSE_IT(px_sequence_id_ = GCTX.sql_engine_->get_px_sequence_id())) {
    } else if (OB_FAIL(register_interrupt())) {
      LOG_WARN("fail to register interrupt", K(ret));
    } else if (OB_NOT_NULL(get_spec().get_phy_plan()) && get_spec().get_phy_plan()->is_enable_px_fast_reclaim()
        && OB_FAIL(ObDetectManagerUtils::qc_register_detectable_id_into_dm(detectable_id_, register_detectable_id_,
                                                                           GET_TENANT_ID(), coord_info_))) {
      LOG_WARN("fail to register detectable_id", K(ret));
    } else if (OB_FAIL(init_dfo_mgr(
                ObDfoInterruptIdGen(interrupt_id_,
                                    (uint32_t)GCTX.server_id_,
                                    (uint32_t)MY_SPEC.qc_id_,
                                    px_sequence_id_),
                coord_info_.dfo_mgr_))) {
      LOG_WARN("fail parse dfo tree",
               "server_id", GCTX.server_id_,
               "qc_id", MY_SPEC.qc_id_,
               "execution_id", ctx_.get_my_session()->get_current_execution_id(),
               K(ret));
    } else if (OB_ISNULL(root_dfo = coord_info_.dfo_mgr_.get_root_dfo())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL root dfo", K(ret));
    } else if (OB_FAIL(setup_op_input(*root_dfo))) {
      LOG_WARN("fail setup all receive/transmit op input", K(ret));
    } else if (OB_FAIL(setup_loop_proc())) {
      LOG_WARN("fail setup loop proc", K(ret));
    } else if (OB_FAIL(register_first_buffer_cache(root_dfo))) {
      LOG_WARN("failed to register first buffer cache", K(ret));
    }
  } else {
    if (OB_FAIL(ObPxCoordOp::inner_rescan())) {
      LOG_WARN("failed to do inner rescan", K(ret));
    } else if (OB_FAIL(batch_rescan())) {
      LOG_WARN("fail to do batch rescan", K(ret));
    }
  }

#ifndef NDEBUG
  OX(OB_ASSERT(false == brs_.end_));
#endif

  return ret;
}

int ObPxCoordOp::register_first_buffer_cache(ObDfo *root_dfo)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect status: physical plan context is null", K(ret));
  } else if (OB_ISNULL(phy_plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect status: physical plan is null", K(ret));
  } else {
    int64_t dop = phy_plan_ctx->get_phy_plan()->get_px_dop();
    if (OB_FAIL(first_buffer_cache_.init(dop, dop))) {
      LOG_WARN("failed to init first buffer cache", K(ret));
    } else {
      ObDtlDfoKey dfo_key;
      dfo_key.set(GCTX.server_id_, root_dfo->get_px_sequence_id(),
          root_dfo->get_qc_id(), root_dfo->get_dfo_id());
      first_buffer_cache_.set_first_buffer_key(dfo_key);
      msg_loop_.set_first_buffer_cache(&first_buffer_cache_);
      if (OB_FAIL(DTL.get_dfc_server().register_first_buffer_cache(
                  ctx_.get_my_session()->get_effective_tenant_id(), get_first_buffer_cache()))) {
        LOG_WARN("failed to register first buffer cache", K(ret));
      }
      LOG_TRACE("trace QC register first buffer cache", K(dfo_key), K(dop));
    }
  }
  return ret;
}

void ObPxCoordOp::unregister_first_buffer_cache()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(DTL.get_dfc_server().unregister_first_buffer_cache(
    ctx_.get_my_session()->get_effective_tenant_id(),
    first_buffer_cache_.get_first_buffer_key(),
    &first_buffer_cache_))) {
    LOG_WARN("failed to register first buffer cache", K(ret));
  }
  LOG_TRACE("trace QC unregister first buffer cache", K(first_buffer_cache_.get_first_buffer_key()));
}

int ObPxCoordOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObDfo *root_dfo = NULL;
  if (OB_FAIL(ObPxReceiveOp::inner_open())) {
  } else if (!is_valid_server_id(GCTX.server_id_)) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("Server is initializing", K(ret), K(GCTX.server_id_));
  } else if (OB_FAIL(post_init_op_ctx())) {
    LOG_WARN("init operator context failed", K(ret));
  } else if (OB_FAIL(coord_info_.init())) {
    LOG_WARN("fail to init coord info", K(ret));
  } else if (OB_FAIL(register_interrupt())) {
    LOG_WARN("fail to register interrupt", K(ret));
  } else if (OB_NOT_NULL(get_spec().get_phy_plan()) && get_spec().get_phy_plan()->is_enable_px_fast_reclaim()
      && OB_FAIL(ObDetectManagerUtils::qc_register_detectable_id_into_dm(detectable_id_, register_detectable_id_,
                                                                         GET_TENANT_ID(), coord_info_))) {
    LOG_WARN("fail to register detectable_id", K(ret));
  } else if (OB_FAIL(init_dfo_mgr(
              ObDfoInterruptIdGen(interrupt_id_,
                                  (uint32_t)GCTX.server_id_,
                                  (uint32_t)(static_cast<const ObPxCoordSpec*>(&get_spec()))->qc_id_,
                                  px_sequence_id_),
                                  coord_info_.dfo_mgr_))) {
    LOG_WARN("fail parse dfo tree",
             "server_id", GCTX.server_id_,
             "qc_id", (static_cast<const ObPxCoordSpec*>(&get_spec()))->qc_id_,
             "execution_id", ctx_.get_my_session()->get_current_execution_id(),
             K(ret));
  } else if (OB_ISNULL(root_dfo = coord_info_.dfo_mgr_.get_root_dfo())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL root dfo", K(ret));
  } else if (OB_FAIL(setup_op_input(*root_dfo))) {
    LOG_WARN("fail setup all receive/transmit op input", K(ret));
  } else if (OB_FAIL(register_first_buffer_cache(root_dfo))) {
    LOG_WARN("failed to register first buffer cache", K(ret));
  } else {
    ctx_.add_extra_check(server_alive_checker_);
    debug_print(*root_dfo);
  }
  if (OB_SUCC(ret)) {
    if (static_cast<const ObPxCoordSpec&>(get_spec()).batch_op_info_.is_inited() &&
        OB_FAIL(init_batch_info())) {
      LOG_WARN("fail to init batch info", K(ret));
    }
  }
  return ret;
}

int ObPxCoordOp::setup_loop_proc()
{
  return OB_ERR_UNEXPECTED;
}

int ObPxCoordOp::post_init_op_ctx()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_obrpc_proxy(coord_info_.rpc_proxy_))) {
    LOG_WARN("fail init rpc proxy", K(ret));
  } else {
    px_dop_ = GET_PHY_PLAN_CTX(ctx_)->get_phy_plan()->get_px_dop();
    set_pruning_table_locations(&(static_cast<const ObPxCoordSpec&>(get_spec()).table_locations_));
  }

  return ret;
}

int ObPxCoordOp::init_dfo_mgr(const ObDfoInterruptIdGen &dfo_id_gen, ObDfoMgr &dfo_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dfo_mgr.init(ctx_, get_spec(), dfo_id_gen, coord_info_))) {
    LOG_WARN("fail init dfo mgr", K(ret));
  }
  return ret;
}

int ObPxCoordOp::terminate_running_dfos(ObDfoMgr &dfo_mgr)
{
  int ret = OB_SUCCESS;
  // notify all running dfo exit
  ObSEArray<ObDfo *, 32> dfos;
  if (OB_FAIL(dfo_mgr.get_running_dfos(dfos))) {
    LOG_WARN("fail find dfo", K(ret));
  } else if (OB_FAIL(ObInterruptUtil::broadcast_px(dfos, OB_GOT_SIGNAL_ABORTING))) {
    LOG_WARN("fail broadcast interrupt to all of a px", K(ret));
  } else if (!dfos.empty() && OB_FAIL(wait_all_running_dfos_exit())) {
    LOG_WARN("fail to exit dfo", K(ret));
  }
  if (OB_NOT_NULL(get_spec().get_phy_plan()) && get_spec().get_phy_plan()->is_enable_px_fast_reclaim()) {
    (void)ObDetectManagerUtils::qc_unregister_all_check_items_from_dm(dfos);
  }
  return ret;
}

int ObPxCoordOp::setup_op_input(ObDfo &root)
{
  int ret = OB_SUCCESS;
  int64_t cnt = root.get_child_count();
  for (int64_t idx = 0; idx < cnt && OB_SUCC(ret); ++idx) {
    ObDfo *child_dfo = NULL;
    if (OB_FAIL(root.get_child_dfo(idx, child_dfo))) {
      LOG_WARN("fail get child_dfo", K(idx), K(cnt), K(ret));
    } else if (OB_ISNULL(child_dfo)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_dfo is null", K(ret));
    } else if (OB_ISNULL(child_dfo->get_root_op_spec())
               || OB_ISNULL(child_dfo->get_root_op_spec()->get_parent())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_dfo op is null", K(ret));
    } else {
      ObOpSpec *receive = child_dfo->get_root_op_spec()->get_parent();
      if (IS_PX_RECEIVE(receive->type_)) {
        ObOperatorKit *kit = ctx_.get_operator_kit(receive->id_);
        if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit), K(receive->id_));
        } else {
          ObPxReceiveOpInput *input = static_cast<ObPxReceiveOpInput*>(kit->input_);
          if (OB_ISNULL(input)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("op input is null, maybe not created", "id", receive->get_id(), K(ret));
          } else {
            input->set_child_dfo_id(child_dfo->get_dfo_id());
            if (root.is_root_dfo()) {
              input->set_task_id(0); // root dfo 只有一个 task
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      // 递归设置 child 的 receive input
      if (OB_FAIL(setup_op_input(*child_dfo))) {
        LOG_WARN("fail setup op input", K(ret));
      }
    }
  }
  return ret;
}

int ObPxCoordOp::try_clear_p2p_dh_info()
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ObSQLSessionInfo *session = ctx_.get_my_session();
  int64_t query_timeout = 0;
  session->get_query_timeout(query_timeout);
  if (OB_FAIL(OB_E(EventTable::EN_PX_NOT_ERASE_P2P_DH_MSG) OB_SUCCESS)) {
    LOG_WARN("qc not clear p2p dh info by design", K(ret), K(query_timeout));
    return OB_SUCCESS;
  }
#endif

  if (!coord_info_.p2p_dfo_map_.empty()) {
    hash::ObHashMap<ObAddr, ObSArray<int64_t> *, hash::NoPthreadDefendMode> dh_map;
    if (OB_FAIL(dh_map.create(coord_info_.p2p_dfo_map_.size() * 2,
        "ClearP2PDhMap",
        "ClearP2PDhMap"))) {
      LOG_WARN("fail to create dh map", K(ret));
    }
    ObSArray<int64_t> *p2p_ids = nullptr;
    void *ptr = nullptr;
    common::ObArenaAllocator allocator;
    int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
    allocator.set_tenant_id(tenant_id);
    FOREACH_X(entry, coord_info_.p2p_dfo_map_, OB_SUCC(ret)) {
      for (int i = 0; OB_SUCC(ret) && i < entry->second.addrs_.count(); ++i) {
        ptr = nullptr;
        p2p_ids = nullptr;
        if (OB_FAIL(dh_map.get_refactored(entry->second.addrs_.at(i), p2p_ids))) {
          if (OB_HASH_NOT_EXIST == ret) {
            if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObSArray<int64_t>)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc memory", K(ret));
            } else {
              p2p_ids = new(ptr) ObSArray<int64_t>();
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail to get array", K(ret));
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(p2p_ids)) {
          if (OB_FAIL(p2p_ids->push_back(entry->first))) {
            LOG_WARN("fail to push back array ptr", K(ret));
          } else if (OB_FAIL(dh_map.set_refactored(entry->second.addrs_.at(i), p2p_ids, 1))) {
            LOG_WARN("fail to set p2p sequence ids", K(ret));
          }
        }
      }
    }
    FOREACH_X(entry, dh_map, true) {
      ObPxP2PClearMsgArg arg;
      arg.px_seq_id_ = px_sequence_id_;
      int tmp_ret = arg.p2p_dh_ids_.assign(*entry->second);
      if (OB_SUCCESS == tmp_ret && !arg.p2p_dh_ids_.empty()) {
        if (OB_FAIL(PX_P2P_DH.get_proxy().to(entry->first).
            by(tenant_id).
            clear_dh_msg(arg, nullptr))) {
          LOG_WARN("fail to clear dh msg", K(ret));
          ret = OB_SUCCESS;
        }
      }
      entry->second->reset();
    }
    allocator.reset();
  }
  return ret;
}

int ObPxCoordOp::inner_close()
{
  int ret = OB_SUCCESS;
  // close过程中忽略terminate错误码
  int terminate_ret = OB_SUCCESS;
  bool should_terminate_running_dfos = true;

#ifdef ERRSIM
  ObSQLSessionInfo *session = ctx_.get_my_session();
  int64_t query_timeout = 0;
  session->get_query_timeout(query_timeout);
  if (OB_FAIL(OB_E(EventTable::EN_PX_QC_EARLY_TERMINATE, query_timeout) OB_SUCCESS)) {
    LOG_WARN("qc not interrupt qc by design", K(ret), K(query_timeout));
    should_terminate_running_dfos = false;
  }
#endif

  if (should_terminate_running_dfos) {
    if (OB_SUCCESS != (terminate_ret = terminate_running_dfos(coord_info_.dfo_mgr_))) {
      // #issue/44180396
      if (OB_NOT_NULL(ctx_.get_my_session()) &&
          ctx_.get_my_session()->get_trans_result().is_incomplete()) {
        ret = terminate_ret;
      } else {
        LOG_WARN("fail to terminate running dfo, ignore ret", K(terminate_ret));
      }
    }
  }

  unregister_first_buffer_cache();
  (void)ObDetectManagerUtils::qc_unregister_detectable_id_from_dm(detectable_id_, register_detectable_id_);
  const ObIArray<ObDfo *> &dfos = coord_info_.dfo_mgr_.get_all_dfos();
  if (OB_NOT_NULL(get_spec().get_phy_plan()) && get_spec().get_phy_plan()->is_enable_px_fast_reclaim()) {
    (void)ObDetectManagerUtils::qc_unregister_all_check_items_from_dm(dfos);
  }
  (void)try_clear_p2p_dh_info();
  (void)clear_interrupt();
  int release_channel_ret = OB_SUCCESS;
  if (OB_SUCCESS != (release_channel_ret = destroy_all_channel())) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }
  ctx_.del_extra_check(server_alive_checker_);
  clean_dfos_dtl_interm_result();
  LOG_TRACE("byebye. exit QC Coord");
  return ret;
}

int ObPxCoordOp::inner_drain_exch()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("drain QC", K(get_spec().id_), K(iter_end_), K(exch_drained_),
            K(enable_px_batch_rescan()), K(lbt()));
  /**
    * why different from receive operator?
    * 1. Why not need try link channel.
    * There are two situations when qc call drain_exch.
    * The first is qc return iter_end when inner_get_next_row, in this case, it will not reach here.
    * The second is plan like this:
    *               merge join
    *          QC1              QC2
    * If QC1 ends, the main thread will call drain exch of QC2.
    * In this situation, no action is required because the upper operator has already got enough rows
    * and will call inner_close to terminate all dfos soon.
    * Therefore, there is no need to send a termination message
    * 2. Why not drain channels if enable px batch rescan?
    * If use px batch rescan, drain channel may lead to missing results of other params in the batch.
  */
  if (enable_px_batch_rescan()) {
    // do nothing
  } else if (iter_end_) {
    exch_drained_ = true;
  } else if (!exch_drained_) {
    dfc_.drain_all_channels();
    exch_drained_ = true;
  }
  return ret;
}

int ObPxCoordOp::destroy_all_channel()
{
  int ret = OB_SUCCESS;
  int64_t recv_cnt = 0;
  ObDtlBasicChannel *ch = nullptr;
  for (int i = 0; i < task_channels_.count(); ++i) {
    ch = static_cast<ObDtlBasicChannel *>(task_channels_.at(i));
    recv_cnt += ch->get_recv_buffer_cnt();
  }
  op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::DTL_SEND_RECV_COUNT;
  op_monitor_info_.otherstat_3_value_ = recv_cnt;
  // 注意：首先必须将 channel 从 msg_loop 中 unregister 掉
  // 这样才能安全地做 unlink_channel
  // 否则，在 unlink_channel 的时候 channel 上还可能收到数据
  // 导致内存泄漏等未知问题
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = msg_loop_.unregister_all_channel())) {
    LOG_WARN("fail unregister channels from msg_loop. ignore", KR(tmp_ret));
  }

  const ObIArray<ObDfo *> & dfos = coord_info_.dfo_mgr_.get_all_dfos();
  /* even if one channel unlink faild, we still continue destroy other channel */
  ARRAY_FOREACH_X(dfos, idx, cnt, true) {
    const ObDfo *edge = dfos.at(idx);
    ObSEArray<const ObPxSqcMeta *, 16> sqcs;
    if (OB_FAIL(edge->get_sqcs(sqcs))) {
      LOG_WARN("fail to get sqcs", K(ret));
    } else {
      /* one channel unlink faild still continue destroy other channel */
      ARRAY_FOREACH_X(sqcs, sqc_idx, sqc_cnt, true) {
        const ObDtlChannelInfo &qc_ci = sqcs.at(sqc_idx)->get_qc_channel_info_const();
        const ObDtlChannelInfo &sqc_ci = sqcs.at(sqc_idx)->get_sqc_channel_info_const();
        if (OB_FAIL(ObDtlChannelGroup::unlink_channel(qc_ci))) {
          LOG_WARN("fail unlink channel", K(qc_ci), K(ret));
        }
        /*
         * actually, the qc and sqc can see the channel id of sqc.
         * sqc channel's owner is SQC, not QC.
         * if we release there, all these channel will be release twice.
         * So, sqc channel will be release by sqc, not qc.
         *
         * */
        UNUSED(sqc_ci);
      }
    }
  }
  /*
   * release root task channel here.
   * */
  if (OB_FAIL(ObPxChannelUtil::unlink_ch_set(task_ch_set_, &dfc_, true))) {
    LOG_WARN("unlink channel failed", K(ret));
  }

  // must erase after unlink channel
  tmp_ret = erase_dtl_interm_result();
  if (tmp_ret != common::OB_SUCCESS) {
    LOG_TRACE("release interm result failed", KR(tmp_ret));
  }
  return ret;
}

int ObPxCoordOp::try_link_channel()
{
  int ret = OB_SUCCESS;
  // qc最后收数据的channel link是在自己get next的
  // 调度循环中进行的，这里不用做任何事情。
  return ret;
}

int ObPxCoordOp::wait_all_running_dfos_exit()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObSEArray<ObDfo *, 32> active_dfos;
  bool all_dfo_terminate = false;
  int64_t nth_channel = OB_INVALID_INDEX_INT64;
  bool collect_trans_result_ok = false;
  if (OB_FAIL(coord_info_.dfo_mgr_.get_running_dfos(active_dfos))) {
    LOG_WARN("fail find dfo", K(ret));
  } else if (OB_UNLIKELY(!first_row_fetched_)) {
    // 一个dfo都没有发出去，不用做任何处理了。
    collect_trans_result_ok = true;
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    // 专用于等待各个活跃sqc的msg proc.
    ObDtlChannelLoop &loop = msg_loop_;
    ObIPxCoordEventListener &listener = get_listenner();
    ObPxTerminateMsgProc terminate_msg_proc(coord_info_, listener);
    ObPxFinishSqcResultP sqc_finish_msg_proc(ctx_, terminate_msg_proc);
    ObPxInitSqcResultP sqc_init_msg_proc(ctx_, terminate_msg_proc);
    ObPxQcInterruptedP interrupt_proc(ctx_, terminate_msg_proc);
    dtl::ObDtlPacketEmptyProc<ObBarrierPieceMsg>  barrier_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObWinbufPieceMsg> winbuf_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObDynamicSamplePieceMsg> sample_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObRollupKeyPieceMsg> rollup_key_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObRDWFPieceMsg> rd_wf_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObInitChannelPieceMsg> init_channel_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObReportingWFPieceMsg> reporting_wf_piece_msg_proc;
    dtl::ObDtlPacketEmptyProc<ObOptStatsGatherPieceMsg> opt_stats_gather_piece_msg_proc;

    // 这个注册会替换掉旧的proc.
    (void)msg_loop_.clear_all_proc();
    (void)msg_loop_
      .register_processor(sqc_finish_msg_proc)
      .register_processor(sqc_init_msg_proc)
      .register_processor(px_row_msg_proc_)
      .register_interrupt_processor(interrupt_proc)
      .register_processor(barrier_piece_msg_proc)
      .register_processor(winbuf_piece_msg_proc)
      .register_processor(sample_piece_msg_proc)
      .register_processor(rollup_key_piece_msg_proc)
      .register_processor(rd_wf_piece_msg_proc)
      .register_processor(init_channel_piece_msg_proc)
      .register_processor(reporting_wf_piece_msg_proc)
      .register_processor(opt_stats_gather_piece_msg_proc);
    loop.ignore_interrupt();

    ObPxControlChannelProc control_channels;
    int64_t times_offset = 0;
    int64_t last_timestamp = 0;
    bool wait_msg = true;
    int64_t start_wait_time = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && wait_msg) {
      ObDtlChannelLoop &loop = msg_loop_;
      /**
       * 开始收下一个消息。
       */
      if (OB_FAIL(check_all_sqc(active_dfos, times_offset, all_dfo_terminate,
                                last_timestamp))) {
        LOG_WARN("fail to check sqc");
      } else if (all_dfo_terminate) {
        wait_msg = false;
        collect_trans_result_ok = true;
        LOG_TRACE("all dfo has been terminate", K(ret));
        break;
      } else if (OB_FAIL(ctx_.fast_check_status_ignore_interrupt())) {
        if (OB_TIMEOUT == ret) {
          LOG_WARN("fail check status, px query timeout", K(ret),
              K(start_wait_time), K(phy_plan_ctx->get_timeout_timestamp()));
        } else if (ObTimeUtility::current_time() - start_wait_time < 50 * 1000000) {
          // 即使query被kill了, 也希望能够等到分布式任务的report结束
          // 在被kill的情况下最多等待50秒.
          // 预期这个时间已经足够.
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail check status, px query killed", K(ret),
          K(start_wait_time), K(phy_plan_ctx->get_timeout_timestamp()));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(loop.process_one_if(&control_channels, nth_channel))) {
        if (OB_EAGAIN == ret) {
          LOG_DEBUG("no message, waiting sqc report", K(ret));
          ret = OB_SUCCESS;
        } else if (OB_ITER_END != ret) {
          LOG_WARN("fail process message", K(ret));
        }
      } else {
        ObDtlMsgType msg_type = loop.get_last_msg_type();
        /**
         * sqc finish的消息已经在回调process中处理了.
         * 对所有消息都不处理，直接丢掉.
         */
        switch (msg_type) {
          case ObDtlMsgType::PX_NEW_ROW:
          case ObDtlMsgType::INIT_SQC_RESULT:
          case ObDtlMsgType::FINISH_SQC_RESULT:
          case ObDtlMsgType::DH_BARRIER_PIECE_MSG:
          case ObDtlMsgType::DH_WINBUF_PIECE_MSG:
          case ObDtlMsgType::DH_DYNAMIC_SAMPLE_PIECE_MSG:
          case ObDtlMsgType::DH_ROLLUP_KEY_PIECE_MSG:
          case ObDtlMsgType::DH_RANGE_DIST_WF_PIECE_MSG:
          case ObDtlMsgType::DH_INIT_CHANNEL_PIECE_MSG:
          case ObDtlMsgType::DH_SECOND_STAGE_REPORTING_WF_PIECE_MSG:
          case ObDtlMsgType::DH_OPT_STATS_GATHER_PIECE_MSG:
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Unexpected msg type", K(msg_type));
            break;
        }
      }
    }
  }
  //过滤掉4662的原因是，在QC希望所有dfo退出时会向所有dfo广播4662中断，这个错误码可能会被sqc report回来
  if (OB_SUCCESS != coord_info_.first_error_code_
      && OB_GOT_SIGNAL_ABORTING != coord_info_.first_error_code_
      && OB_ERR_SIGNALED_IN_PARALLEL_QUERY_SERVER != coord_info_.first_error_code_) {
    ret = coord_info_.first_error_code_;
  }
  if (!collect_trans_result_ok) {
    ObSQLSessionInfo *session = ctx_.get_my_session();
    session->get_trans_result().set_incomplete();
    LOG_WARN("collect trans_result fail", K(ret),
             "session_id", session->get_sessid(),
             "trans_result", session->get_trans_result());

  }
  return ret;
}

int ObPxCoordOp::check_all_sqc(ObIArray<ObDfo *> &active_dfos,
                               int64_t &times_offset,
                               bool &all_dfo_terminate,
                               int64_t &last_timestamp)
{
  int ret = OB_SUCCESS;
  all_dfo_terminate = true;
  for (int64_t i = 0; i < active_dfos.count() && all_dfo_terminate && OB_SUCC(ret); ++i) {
    ObArray<ObPxSqcMeta *> sqcs;
    if (OB_FAIL(active_dfos.at(i)->get_sqcs(sqcs))) {
      LOG_WARN("fail get qc-sqc channel for QC", K(ret));
    } else {
      ARRAY_FOREACH_X(sqcs, idx, cnt, OB_SUCC(ret)) {
        ObPxSqcMeta *sqc = sqcs.at(idx);
        if (OB_ISNULL(sqc)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected sqc", K(ret));
        } else if (sqc->need_report()) {
          LOG_DEBUG("wait for sqc", K(sqc));
          int64_t cur_timestamp = ObTimeUtility::current_time();
          // > 1s, increase gradually
          // In order to get the dfo to propose as soon as possible and
          // In order to avoid the interruption that is not received,
          // So the interruption needs to be sent repeatedly
          if (cur_timestamp - last_timestamp > (1000000 + min(times_offset, 10) * 1000000)) {
            last_timestamp = cur_timestamp;
            times_offset++;
            ObInterruptUtil::broadcast_dfo(active_dfos.at(i), OB_GOT_SIGNAL_ABORTING);
          }
          all_dfo_terminate = false;
          break;
        } else if (sqc->is_server_not_alive() || sqc->is_interrupt_by_dm()) {
          if (sqc->is_interrupt_by_dm()) {
            ObRpcResultCode err_msg;
            ObPxErrorUtil::update_qc_error_code(coord_info_.first_error_code_, OB_RPC_CONNECT_ERROR, err_msg);
          }
          sqc->set_server_not_alive(false);
          sqc->set_interrupt_by_dm(false);
          const DASTabletLocIArray &access_locations = sqc->get_access_table_locations();
          for (int64_t i = 0; i < access_locations.count() && OB_SUCC(ret); i++) {
            if (OB_FAIL(ctx_.get_my_session()->get_trans_result().add_touched_ls(access_locations.at(i)->ls_id_))) {
              LOG_WARN("add touched ls failed", K(ret));
            }
          }
          LOG_WARN("server not alive", K(access_locations), K(sqc->get_access_table_location_keys()));
        }
      }
    }
  }
  return ret;
}

int ObPxCoordOp::register_interrupt()
{
  int ret = OB_SUCCESS;
  px_sequence_id_ = GCTX.sql_engine_->get_px_sequence_id();
  ObInterruptUtil::generate_query_interrupt_id((uint32_t)GCTX.server_id_,
      px_sequence_id_,
      interrupt_id_);
  if (OB_FAIL(SET_INTERRUPTABLE(interrupt_id_))) {
    LOG_WARN("fail to register interrupt", K(ret));
  } else {
    register_interrupted_ = true;
  }
  LOG_TRACE("QC register interrupt", K(ret));
  return ret;
}

void ObPxCoordOp::clear_interrupt()
{
  if (register_interrupted_) {
    UNSET_INTERRUPTABLE(interrupt_id_);
    register_interrupted_ = false;
  }
  LOG_TRACE("unregister interrupt");
}

int ObPxCoordOp::receive_channel_root_dfo(
  ObExecContext &ctx, ObDfo &parent_dfo, ObPxTaskChSets &parent_ch_sets)
{
  int ret = OB_SUCCESS;
  uint64_t min_cluster_version = 0;
  CK (OB_NOT_NULL(ctx.get_physical_plan_ctx()) && OB_NOT_NULL(ctx.get_physical_plan_ctx()->get_phy_plan()));
  OX (min_cluster_version = ctx.get_physical_plan_ctx()->get_phy_plan()->get_min_cluster_version());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(task_ch_set_.assign(parent_ch_sets.at(0)))) {
    LOG_WARN("fail assign data", K(ret));
  } else if (OB_FAIL(init_dfc(parent_dfo, nullptr))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(ObPxReceiveOp::link_ch_sets(task_ch_set_, task_channels_, &dfc_))) {
    LOG_WARN("fail link px coord data channels with its only child dfo", K(ret));
  } else {
    if (OB_FAIL(get_listenner().on_root_data_channel_setup())) {
      LOG_WARN("fail notify listener", K(ret));
    }
    bool enable_audit = GCONF.enable_sql_audit && ctx.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    msg_loop_.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    msg_loop_.set_interm_result(enable_px_batch_rescan());
    msg_loop_.set_process_query_time(ctx_.get_my_session()->get_process_query_time());
    msg_loop_.set_query_timeout_ts(ctx_.get_physical_plan_ctx()->get_timeout_timestamp());
    // root dfo 的 receive channel sets 在本机使用，不需要通过  DTL 发送
    // 直接注册到 msg_loop 中收取数据即可
    int64_t cnt = task_channels_.count();
    int64_t thread_id = GETTID();
    // FIXME:  msg_loop_ 不要 unregister_channel，避免这个 ...start_ 下标变动 ?
    for (int64_t idx = 0; idx < cnt && OB_SUCC(ret); ++idx) {
      dtl::ObDtlChannel *ch = task_channels_.at(idx);
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ch), K(ret));
      } else {
        ch->set_audit(enable_audit);
        ch->set_is_px_channel(true);
        ch->set_operator_owner();
        ch->set_thread_id(thread_id);
        ch->set_enable_channel_sync(min_cluster_version >= CLUSTER_VERSION_4_1_0_0);
        if (enable_px_batch_rescan()) {
          ch->set_interm_result(true);
          ch->set_batch_id(get_batch_id());
          last_px_batch_rescan_size_ = max(get_batch_id() + 1, get_rescan_param_count());
        }
      }
      LOG_TRACE("link qc-task channel and registered to qc msg loop. ready to receive task data msg",
                K(idx), K(cnt), "ch", *ch, KP(ch->get_id()), K(ch->get_peer()));
    }
  }
  return ret;
}

int ObPxCoordOp::notify_peers_mock_eof(ObDfo *dfo,
                                       int64_t timeout_ts,
                                       common::ObAddr addr) const
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(dfo));
  if (OB_SUCC(ret) && dfo->parent()->is_root_dfo()) {
    // need drain task channel
    for (int i = 0; i < task_channels_.count() && OB_SUCC(ret); ++i) {
      if (task_channels_.at(i)->get_peer() == addr) {
        OZ(reinterpret_cast<ObDtlBasicChannel *>(task_channels_.at(i))->
           mock_eof_buffer(timeout_ts));
      }
    }
  }
  return ret;
}

int ObPxCoordOp::receive_channel_root_dfo(
  ObExecContext &ctx, ObDfo &parent_dfo, ObDtlChTotalInfo &ch_info)
{
  int ret = OB_SUCCESS;
  ObPxTaskChSets tmp_ch_sets;
  uint64_t min_cluster_version = 0;
  CK (OB_NOT_NULL(ctx.get_physical_plan_ctx()) && OB_NOT_NULL(ctx.get_physical_plan_ctx()->get_phy_plan()));
  OX (min_cluster_version = ctx.get_physical_plan_ctx()->get_phy_plan()->get_min_cluster_version());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObPxChProviderUtil::inner_get_data_ch(
      tmp_ch_sets, ch_info, 0, 0, task_ch_set_, false))) {
    LOG_WARN("fail get data ch set", K(ret));
  } else if (OB_FAIL(init_dfc(parent_dfo, &ch_info))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(ObPxReceiveOp::link_ch_sets(task_ch_set_, task_channels_, &dfc_))) {
    LOG_WARN("fail link px coord data channels with its only child dfo", K(ret));
  } else {
    if (OB_FAIL(get_listenner().on_root_data_channel_setup())) {
      LOG_WARN("fail notify listener", K(ret));
    }
    bool enable_audit = GCONF.enable_sql_audit && ctx.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    msg_loop_.set_tenant_id(ctx.get_my_session()->get_effective_tenant_id());
    msg_loop_.set_interm_result(enable_px_batch_rescan());
    msg_loop_.set_process_query_time(ctx_.get_my_session()->get_process_query_time());
    msg_loop_.set_query_timeout_ts(ctx_.get_physical_plan_ctx()->get_timeout_timestamp());
    // root dfo 的 receive channel sets 在本机使用，不需要通过  DTL 发送
    // 直接注册到 msg_loop 中收取数据即可
    int64_t cnt = task_channels_.count();
    int64_t thread_id = GETTID();
    // FIXME:  msg_loop_ 不要 unregister_channel，避免这个 ...start_ 下标变动 ?
    for (int64_t idx = 0; idx < cnt && OB_SUCC(ret); ++idx) {
      dtl::ObDtlChannel *ch = task_channels_.at(idx);
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ch), K(ret));
      } else {
        ch->set_operator_owner();
        ch->set_thread_id(thread_id);
        ch->set_audit(enable_audit);
        ch->set_is_px_channel(true);
        ch->set_enable_channel_sync(min_cluster_version >= CLUSTER_VERSION_4_1_0_0);
        if (enable_px_batch_rescan()) {
          ch->set_interm_result(true);
          ch->set_batch_id(get_batch_id());
          last_px_batch_rescan_size_ = max(get_batch_id() + 1, get_rescan_param_count());
        }
      }
      LOG_TRACE("link qc-task channel and registered to qc msg loop. ready to receive task data msg",
                K(idx), K(cnt), "ch", *ch, KP(ch->get_id()), K(ch->get_peer()));
    }
  }
  return ret;
}

int ObPxCoordOp::init_batch_info()
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = ctx_.get_operator_kit(MY_SPEC.batch_op_info_.op_id_);
  if (OB_ISNULL(kit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is unexpected", K(ret));
  } else if (OB_ISNULL((kit->op_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("op is unexpected", K(ret));
  } else {
    if (PHY_NESTED_LOOP_JOIN == MY_SPEC.batch_op_info_.op_type_) {
      coord_info_.batch_rescan_ctl_
          = &static_cast<ObNestedLoopJoinOp *>(kit->op_)->get_batch_rescan_ctl();
    } else if (PHY_SUBPLAN_FILTER == MY_SPEC.batch_op_info_.op_type_) {
      coord_info_.batch_rescan_ctl_
          = &static_cast<ObSubPlanFilterOp*>(kit->op_)->get_batch_rescan_ctl();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op type is unexpected", K(ret));
    }
    if (OB_SUCC(ret)) {
      batch_rescan_param_version_ = coord_info_.batch_rescan_ctl_->param_version_;
    }
  }
  return ret;
}

int ObPxCoordOp::batch_rescan()
{
  int ret = OB_SUCCESS;
  for (int64_t idx = 0; idx < task_channels_.count() && OB_SUCC(ret); ++idx) {
    dtl::ObDtlChannel *ch = task_channels_.at(idx);
    if (OB_ISNULL(ch)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", K(ch), K(ret));
    } else {
      ch->set_batch_id(get_batch_id());
      ch->set_channel_is_eof(false);
      ch->reset_state();
      ch->reset_px_row_iterator();
    }
  }
  if (OB_SUCC(ret)) {
    msg_loop_.reset_eof_cnt();
    row_reader_.reset();
    iter_end_ = false;
    if (PHY_PX_MERGE_SORT_COORD == get_spec().get_type()) {
      reinterpret_cast<ObPxMSCoordOp *>(this)->reset_finish_ch_cnt();
      reinterpret_cast<ObPxMSCoordOp *>(this)->reset_readers();
      reinterpret_cast<ObPxMSCoordOp *>(this)->reuse_heap();
    }
  }
  return ret;
}

int ObPxCoordOp::erase_dtl_interm_result()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ObSQLSessionInfo *session = ctx_.get_my_session();
  int64_t query_timeout = 0;
  session->get_query_timeout(query_timeout);
  if (OB_FAIL(OB_E(EventTable::EN_PX_SINGLE_DFO_NOT_ERASE_DTL_INTERM_RESULT) OB_SUCCESS)) {
    LOG_WARN("ObPxCoordOp not erase_dtl_interm_result by design", K(ret), K(query_timeout));
    return OB_SUCCESS;
  }
#endif
  if (static_cast<const ObPxCoordSpec&>(get_spec()).batch_op_info_.is_inited()) {
    ObDTLIntermResultKey key;
    ObDtlChannelInfo ci;
    // no need OB_SUCCESS in for-loop.
    for (int64_t idx = 0; idx < task_ch_set_.count(); ++idx) {
      if (OB_FAIL(task_ch_set_.get_channel_info(idx, ci))) {
        LOG_WARN("fail get channel info", K(ret));
      } else {
        key.channel_id_ = ci.chid_;
        for (int j = 0; j < last_px_batch_rescan_size_; ++j) {
          key.batch_id_ = j;
          if (OB_FAIL(MTL(ObDTLIntermResultManager*)->erase_interm_result_info(key))) {
            LOG_TRACE("fail to release recieve internal result", K(ret));
          }
        }
        last_px_batch_rescan_size_ = 0;
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
