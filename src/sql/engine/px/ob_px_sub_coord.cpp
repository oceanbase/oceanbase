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

#include "lib/coro/co.h"
#include "common/ob_smart_call.h"
#include "sql/engine/px/ob_px_sub_coord.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/exchange/ob_px_receive.h"
#include "sql/engine/px/exchange/ob_px_transmit.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/px/exchange/ob_px_transmit_op.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/engine/basic/ob_expr_values.h"
#include "sql/engine/px/ob_granule_iterator.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/ob_px_worker.h"
#include "sql/engine/px/ob_px_admission.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/basic/ob_temp_table_insert.h"
#include "sql/engine/basic/ob_temp_table_access.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/basic/ob_temp_table_access_op.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/executor/ob_task_spliter.h"
#include "share/ob_rpc_share.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "sql/ob_sql_trans_control.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::sql::dtl;

#define ENG_OP typename ObEngineOpTraits<NEW_ENG>

int ObPxSubCoord::pre_process()
{
  int ret = OB_SUCCESS;
  // 1. Register interruption
  // 2. Construct ObGranuleIterator input, to task
  // 3. Setup SQC-QC channel
  // 4. Acquire workers and send Task. Report worker count to  QC
  // 5. Setup SQC-Task channels
  // 6. SQC get Task Channel Map and deliver to Task
  // 7. Wati Task execute complete
  // 8. Send result to QC
  // 9. Un-register interruption

  LOG_TRACE("begin ObPxSubCoord process", K(ret));
  int64_t dfo_id = sqc_arg_.sqc_.get_dfo_id();
  int64_t sqc_id = sqc_arg_.sqc_.get_sqc_id();
  ObPhysicalPlanCtx* phy_plan_ctx = NULL;
  int64_t tsc_locations_idx = 0;
  int64_t dml_op_count = 0;
  LOG_TRACE("TIMERECORD ",
      "reserve:=0 name:=SQC dfoid:",
      dfo_id,
      "sqcid:",
      sqc_id,
      "taskid:=-1 start:",
      ObTimeUtility::current_time());

  NG_TRACE(tag1);
  if (OB_ISNULL(sqc_arg_.exec_ctx_) || (OB_ISNULL(sqc_arg_.op_root_) && OB_ISNULL(sqc_arg_.op_spec_root_)) ||
      OB_ISNULL(sqc_arg_.des_phy_plan_) || OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(*sqc_arg_.exec_ctx_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc args should not be NULL", K(ret));
  } else if (OB_FAIL(sqc_ctx_.sqc_proxy_.init())) {
    LOG_WARN("fail to setup loop proc", K(ret));
  } else if (OB_FAIL(try_prealloc_data_channel(sqc_ctx_, sqc_arg_.sqc_))) {
    LOG_WARN("fail try prealloc data channel", K(ret));
  } else if (sqc_arg_.des_phy_plan_->is_new_engine()) {
    // ObOperator *op = NULL;
    ObSEArray<const ObTableScanSpec*, 1> all_scan_ops;
    if (OB_ISNULL(sqc_arg_.op_spec_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: op root is null", K(ret));
    } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(all_scan_ops, *sqc_arg_.op_spec_root_))) {
      LOG_WARN("fail get scan ops", K(ret));
    } else {
      // set mock schema guard
      ObSQLMockSchemaGuard mock_schema_guard;
      // prepare mock schemas
      if (OB_FAIL(ObSQLMockSchemaUtils::prepare_mocked_schemas(sqc_arg_.des_phy_plan_->get_mock_rowid_tables()))) {
        LOG_WARN("failed to prepare mocked schemas", K(ret));
      } else if (OB_FAIL(setup_op_input(*sqc_arg_.exec_ctx_,
                     *sqc_arg_.op_spec_root_,
                     sqc_ctx_,
                     sqc_arg_.sqc_.get_access_table_locations(),
                     tsc_locations_idx,
                     all_scan_ops,
                     dml_op_count))) {
        LOG_WARN("fail to setup receive/transmit op input", K(ret));
      }
    }
  } else {
    ObSEArray<const ObTableScan*, 1> all_scan_ops;
    if (OB_ISNULL(sqc_arg_.op_root_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: op root is null", K(ret));
    } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(all_scan_ops, *sqc_arg_.op_root_))) {
      LOG_WARN("fail get scan ops", K(ret));
    } else {
      // set mock schema guard
      ObSQLMockSchemaGuard mock_schema_guard;
      // prepare mock schemas
      if (OB_FAIL(ObSQLMockSchemaUtils::prepare_mocked_schemas(sqc_arg_.des_phy_plan_->get_mock_rowid_tables()))) {
        LOG_WARN("failed to prepare mocked schemas", K(ret));
      } else if (OB_FAIL(setup_op_input(*sqc_arg_.exec_ctx_,
                     *sqc_arg_.op_root_,
                     sqc_ctx_,
                     sqc_arg_.sqc_.get_access_table_locations(),
                     tsc_locations_idx,
                     all_scan_ops,
                     dml_op_count))) {
        LOG_WARN("fail to setup receive/transmit op input", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (IS_INTERRUPTED()) {
      // interrupt by QC
    } else {
      (void)ObInterruptUtil::interrupt_qc(sqc_arg_.sqc_, ret);
    }
  }

  return ret;
}

// Schedule task execution when receive TRANSMIT CHANNEL, RECEIVE CHANNEL message.
int ObPxSubCoord::try_start_tasks(int64_t& dispatch_worker_count, bool is_fast_sqc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tasks(sqc_arg_, sqc_ctx_))) {
    LOG_WARN("fail create tasks", K(ret));
  } else if (OB_FAIL(dispatch_tasks(sqc_arg_, sqc_ctx_, dispatch_worker_count, is_fast_sqc))) {
    LOG_WARN("fail to dispatch tasks to working threads", K(ret));
  }
  return ret;
}

void ObPxSubCoord::notify_dispatched_task_exit(int64_t dispatched_worker_count)
{
  (void)thread_worker_factory_.join();
  auto& tasks = sqc_ctx_.get_tasks();
  for (int64_t idx = 0; idx < dispatched_worker_count && dispatched_worker_count <= tasks.count(); ++idx) {
    int tick = 1;
    ObPxTask& task = tasks.at(idx);
    while (false == task.is_task_state_set(SQC_TASK_EXIT)) {
      if (tick % 1000 == 100) {
        ObPxSqcMeta& sqc = sqc_arg_.sqc_;
        (void)ObInterruptUtil::interrupt_tasks(sqc, OB_GOT_SIGNAL_ABORTING);
      }
      // log wait message after waiting 10 second.
      if (tick++ % 10000 == 0) {
        LOG_INFO("waiting for task exit", K(idx), K(dispatched_worker_count), K(tick));
      }
      usleep(1000);
    }
    LOG_TRACE("task exit",
        K(idx),
        K(tasks.count()),
        K(dispatched_worker_count),
        "dfo_id",
        task.dfo_id_,
        "sqc_id",
        task.sqc_id_,
        "task_id",
        task.task_id_);
  }
}

int ObPxSubCoord::init_exec_env(ObExecContext& exec_ctx)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = GET_MY_SESSION(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized exec ctx without phy plan session set. Unexpected", K(ret));
  } else if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized exec ctx without phy plan ctx set. Unexpected", K(ret));
  } else if (OB_FAIL(init_first_buffer_cache(sqc_arg_.sqc_.is_rpc_worker(), sqc_arg_.des_phy_plan_->get_px_dop()))) {
    LOG_WARN("failed to init first buffer cache", K(ret));
  } else {
    exec_ctx.get_task_exec_ctx().set_partition_service(gctx_.par_ser_);
    exec_ctx.set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
    session->set_cur_phy_plan(sqc_arg_.des_phy_plan_);
    plan_ctx->set_phy_plan(sqc_arg_.des_phy_plan_);
    THIS_WORKER.set_timeout_ts(plan_ctx->get_timeout_timestamp());
  }
  return ret;
}

template <bool NEW_ENG>
int ObPxSubCoord::get_tsc_or_dml_op_partition_key(ENG_OP::Root& root, ObPartitionReplicaLocationIArray& tsc_locations,
    int64_t& tsc_location_idx, common::ObIArray<const ENG_OP::TSC*>& scan_ops,
    common::ObIArray<common::ObPartitionArray>& partition_keys_array, int64_t& td_op_count, int64_t part_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, root))) {
    LOG_WARN("fail get scan ops", K(ret));
  } else if (FALSE_IT(td_op_count += scan_ops.count())) {
  } else if (0 == td_op_count) {
    /*do nothing*/
  } else if (OB_FAIL(ObPxPartitionLocationUtil::get_all_tables_partitions(
                 td_op_count, tsc_locations, tsc_location_idx, partition_keys_array, part_count))) {
    LOG_WARN("get all table scan's partition failed", K(ret));
  }
  return ret;
}

int ObPxSubCoord::setup_op_input(ObExecContext& ctx, ObPhyOperator& root, ObSqcCtx& sqc_ctx,
    ObPartitionReplicaLocationIArray& tsc_locations, int64_t& tsc_location_idx,
    ObIArray<const ObTableScan*>& all_scan_ops, int64_t& dml_op_count)
{
  int ret = OB_SUCCESS;
  if (IS_PX_RECEIVE(root.get_type())) {
    ObPxReceive* receive_op = reinterpret_cast<ObPxReceive*>(&root);
    ObPxReceiveInput* receive_input = NULL;
    if (OB_ISNULL(receive_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op with px receive type fail cast to ObPxReceive. Unexpected!", "type", root.get_type(), K(ret));
    } else if (OB_ISNULL(receive_input = GET_PHY_OP_INPUT(ObPxReceiveInput, ctx, receive_op->get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't get receive op input", K(ret));
    } else {
      receive_input->set_sqc_proxy(sqc_ctx.sqc_proxy_);
      LOG_TRACE("setup op input", "op_id", root.get_id(), KP(receive_input));
    }
  } else if (IS_PX_TRANSMIT(root.get_type())) {
    ObPxTransmit* transmit_op = reinterpret_cast<ObPxTransmit*>(&root);
    ObPxTransmitInput* transmit_input = NULL;
    if (OB_ISNULL(transmit_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op with px transmit type fail cast to ObPxTransmit. Unexpected!", "type", root.get_type(), K(ret));
    } else if (OB_ISNULL(transmit_input = GET_PHY_OP_INPUT(ObPxTransmitInput, ctx, transmit_op->get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't get transmit op input", K(ret));
    } else {
      transmit_input->set_sqc_proxy(sqc_ctx.sqc_proxy_);
      LOG_TRACE("setup op input", "op_id", root.get_id(), KP(transmit_input));
    }
  } else if (PHY_EXPR_VALUES == root.get_type()) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    if (sqc.get_partition_id_values().count() > 0) {
      ObExprValues* expr_op = reinterpret_cast<ObExprValues*>(&root);
      ObExprValuesInput* expr_op_input = NULL;
      if (OB_ISNULL(expr_op)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("operator is NULL", K(ret));
      } else if (OB_ISNULL(expr_op_input = GET_PHY_OP_INPUT(ObExprValuesInput, ctx, expr_op->get_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't get expr op input", K(ret));
      } else {
        expr_op_input->partition_id_values_ = reinterpret_cast<int64_t>(&sqc.get_partition_id_values());
      }
    }
  } else if (IS_DML(root.get_type())) {
    if (0 == tsc_location_idx) {
      dml_op_count++;
      int64_t each_table_part_count = tsc_locations.count() / (all_scan_ops.count() + dml_op_count);
      tsc_location_idx += each_table_part_count;
      LOG_TRACE(
          "Current sqc serves following partition", K(dml_op_count), K(each_table_part_count), K(tsc_location_idx));
    }
  } else if (IS_PX_GI(root.get_type())) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    common::ObSEArray<common::ObPartitionArray, 4> partition_keys_array;
    ObSEArray<const ObTableScan*, 1> scan_ops;
    ObTableModify* dml_op = NULL;
    int64_t td_op_count = 0;
    int64_t each_table_part_count = 0;
    if (root.get_child_num() == 1 && IS_DML(root.get_child(0)->get_type())) {
      // ....
      //   GI
      //     INSERT/REPLACE
      //      ....
      dml_op = static_cast<ObTableModify*>(root.get_child(0));
      td_op_count++;
      dml_op_count++;
    }
    // pre query range and init scan input (for compatible)
    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(0 == all_scan_ops.count() + dml_op_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("all scan ops count is unexpected", K(ret));
        } else {
          each_table_part_count = tsc_locations.count() / (all_scan_ops.count() + dml_op_count);
          LOG_TRACE("Current sqc serves following partition");
        }
      }
      if (OB_FAIL(get_tsc_or_dml_op_partition_key<false>(root,
              tsc_locations,
              tsc_location_idx,
              scan_ops,
              partition_keys_array,
              td_op_count,
              each_table_part_count))) {
        LOG_WARN("fail get scan ops", K(ret));
      } else {
        ObGranuleIterator* gi_op = reinterpret_cast<ObGranuleIterator*>(&root);
        ObGIInput* gi_input = NULL;
        if (OB_ISNULL(gi_op)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("op with px gi type fail cast to ObGranuleIterator. Unexpected!", "type", root.get_type(), K(ret));
        } else if (OB_ISNULL(gi_input = GET_PHY_OP_INPUT(ObGIInput, ctx, gi_op->get_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("can't get gi op input", K(ret));
        } else if (OB_ISNULL(GCTX.par_ser_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr unexpected", K(ret));
        } else {
          ObGranulePumpArgs gp_args(ctx, partition_keys_array, sqc_ctx.partitions_info_, *GCTX.par_ser_);
          gp_args.tablet_size_ = gi_op->get_tablet_size();
          gp_args.parallelism_ = sqc.get_task_count();
          gp_args.gi_attri_flag_ = gi_op->get_gi_flags();
          if (OB_FAIL(gp_args.partitions_info_.assign(sqc_ctx.partitions_info_))) {
            LOG_WARN("Failed to assign partitions info", K(ret));
          } else if (OB_FAIL(sqc_ctx.gi_pump_.add_new_gi_task(gp_args, scan_ops, dml_op))) {
            LOG_WARN("fail init granule iter pump", K(ret));
          } else {
            gi_input->set_granule_pump(&sqc_ctx.gi_pump_);
            gi_input->set_parallelism(sqc.get_task_count());
            LOG_TRACE("setup gi op input", K(gi_input), K(&sqc_ctx.gi_pump_));
          }
        }
      }
    }
  } else if (root.get_type() == PHY_TEMP_TABLE_ACCESS) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    uint64_t* access_count_ptr = NULL;
    uint64_t* closed_count_ptr = NULL;
    ObTempTableAccessInput* access_input = NULL;
    ObTempTableAccess* phy_op = reinterpret_cast<ObTempTableAccess*>(&root);
    if (OB_ISNULL(phy_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast.", K(root.get_type()), K(ret));
    } else if (OB_ISNULL(access_input = GET_PHY_OP_INPUT(ObTempTableAccessInput, ctx, phy_op->get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get phy op input.", K(ret));
    } else {
      access_count_ptr = (uint64_t*)ctx.get_allocator().alloc(sizeof(uint64_t));
      closed_count_ptr = (uint64_t*)ctx.get_allocator().alloc(sizeof(uint64_t));
      if (OB_ISNULL(access_count_ptr) || OB_ISNULL(closed_count_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null insert count ptr or closed count ptr.", K(ret));
      } else if (OB_FAIL(access_input->assign_ids(sqc.get_interm_results()))) {
        LOG_WARN("failed to assign to interm result ids.", K(ret));
      } else {
        *access_count_ptr = sqc.get_interm_results().count();
        *closed_count_ptr = sqc.get_task_count();
        access_input->closed_count_ = reinterpret_cast<uint64_t>(closed_count_ptr);
        access_input->unfinished_count_ptr_ = reinterpret_cast<uint64_t>(access_count_ptr);
      }
    }
  } else if (root.get_type() == PHY_TEMP_TABLE_INSERT) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    uint64_t* insert_count_ptr = NULL;
    ObTempTableInsertInput* insert_input = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc.get_task_count(); i++) {
      const uint64_t interm_id = ObDtlChannel::generate_id();
      if (OB_FAIL(sqc_ctx.interm_result_ids_.push_back(interm_id))) {
        LOG_WARN("failed to push back into sqc ctx result ids.", K(ret));
      } else { /*do nothing.*/
      }
    }
    ObTempTableInsert* phy_op = reinterpret_cast<ObTempTableInsert*>(&root);
    if (OB_ISNULL(phy_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast op.", K(root.get_type()), K(ret));
    } else if (OB_ISNULL(insert_input = GET_PHY_OP_INPUT(ObTempTableInsertInput, ctx, phy_op->get_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get phy op input.", K(ret));
    } else {
      insert_count_ptr = (uint64_t*)ctx.get_allocator().alloc(sizeof(uint64_t));
      if (OB_ISNULL(insert_count_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null insert count ptr.", K(ret));
      } else if (OB_FAIL(insert_input->assign_ids(sqc_ctx.interm_result_ids_))) {
        LOG_WARN("failed to assign to interm result ids.", K(ret));
      } else {
        *insert_count_ptr = sqc.get_task_count();
        insert_input->unfinished_count_ptr_ = reinterpret_cast<uint64_t>(insert_count_ptr);
        sqc_ctx.set_temp_table_id(phy_op->get_table_id());
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root.register_to_datahub(ctx))) {
      LOG_WARN("fail register op to datahub", K(ret));
    }
  }

  if (IS_PX_RECEIVE(root.get_type())) {
  } else if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < root.get_child_num() && OB_SUCC(ret); ++i) {
      ObPhyOperator* child = root.get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child op unexpected", K(ret));
      } else {
        ret = SMART_CALL(
            setup_op_input(ctx, *child, sqc_ctx, tsc_locations, tsc_location_idx, all_scan_ops, dml_op_count));
      }
    }
  }

  // after all operator setup_op_input processed
  if (OB_SUCC(ret) && sqc_arg_.op_root_->get_id() == root.get_id()) {
    ARRAY_FOREACH_X(all_scan_ops, idx, cnt, OB_SUCC(ret))
    {
      ObTableScanInput* tsc_input = NULL;
      if (OB_ISNULL(all_scan_ops.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL scan op ptr unexpected", K(ret));
      } else if (OB_ISNULL(tsc_input = GET_PHY_OP_INPUT(ObTableScanInput, ctx, all_scan_ops.at(idx)->get_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't get tsc op input", K(ret));
      } else {
        // TSC need this in open()
        // hard code idx to 0
        tsc_input->set_location_idx(0);
      }
    }
  }
  return ret;
}

int ObPxSubCoord::setup_op_input(ObExecContext& ctx, ObOpSpec& root, ObSqcCtx& sqc_ctx,
    ObPartitionReplicaLocationIArray& tsc_locations, int64_t& tsc_location_idx,
    ObIArray<const ObTableScanSpec*>& all_scan_ops, int64_t& dml_op_count)
{
  int ret = OB_SUCCESS;
  if (IS_PX_RECEIVE(root.get_type())) {
    ObPxReceiveSpec* receive_op = reinterpret_cast<ObPxReceiveSpec*>(&root);
    ObOperatorKit* kit = ctx.get_operator_kit(receive_op->id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObPxReceiveOpInput* receive_input = static_cast<ObPxReceiveOpInput*>(kit->input_);
      receive_input->set_sqc_proxy(sqc_ctx.sqc_proxy_);
      LOG_TRACE("setup op input", "op_id", root.get_id(), KP(receive_input));
    }
  } else if (PHY_EXPR_VALUES == root.get_type()) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    if (sqc.get_partition_id_values().count() > 0) {
      ObExprValuesSpec* expr_op_spec = reinterpret_cast<ObExprValuesSpec*>(&root);
      ObOperatorKit* kit = ctx.get_operator_kit(expr_op_spec->id_);
      if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("operator is NULL", K(ret), KP(kit));
      } else {
        ObExprValuesOpInput* expr_op_input = static_cast<ObExprValuesOpInput*>(kit->input_);
        expr_op_input->partition_id_values_ = reinterpret_cast<int64_t>(&sqc.get_partition_id_values());
      }
    }
  } else if (IS_PX_TRANSMIT(root.get_type())) {
    ObPxTransmitSpec* transmit_op = reinterpret_cast<ObPxTransmitSpec*>(&root);
    ObOperatorKit* kit = ctx.get_operator_kit(transmit_op->id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else {
      ObPxTransmitOpInput* transmit_input = static_cast<ObPxTransmitOpInput*>(kit->input_);
      transmit_input->set_sqc_proxy(sqc_ctx.sqc_proxy_);
      LOG_TRACE("setup op input", "op_id", root.get_id(), KP(transmit_input));
    }
  } else if (IS_DML(root.get_type())) {
    if (0 == tsc_location_idx) {
      dml_op_count++;
      int64_t each_table_part_count = tsc_locations.count() / (all_scan_ops.count() + dml_op_count);
      tsc_location_idx += each_table_part_count;
      LOG_TRACE(
          "Current sqc serves following partition", K(dml_op_count), K(each_table_part_count), K(tsc_location_idx));
    }
  } else if (IS_PX_GI(root.get_type())) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    common::ObSEArray<common::ObPartitionArray, 4> partition_keys_array;
    ObSEArray<const ObTableScanSpec*, 1> scan_ops;
    ObTableModifySpec* dml_op = NULL;
    int64_t td_op_count = 0;
    int64_t each_table_part_count = 0;
    if (OB_SUCC(ret)) {
      if (root.get_child_num() == 1 && IS_DML(root.get_child(0)->get_type())) {
        // ....
        //   GI
        //     INSERT/REPLACE
        //      ....
        dml_op = static_cast<ObTableModifySpec*>(root.get_child(0));
        dml_op_count++;
        td_op_count++;
      }
    }
    // pre query range and init scan input (for compatible)
    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(0 == all_scan_ops.count() + dml_op_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("all scan ops and dml op count is unexpected", K(ret));
        } else {
          each_table_part_count = tsc_locations.count() / (all_scan_ops.count() + dml_op_count);
          LOG_TRACE("Current sqc serves following partition");
        }
      }
      if (OB_FAIL(get_tsc_or_dml_op_partition_key<true>(root,
              tsc_locations,
              tsc_location_idx,
              scan_ops,
              partition_keys_array,
              td_op_count,
              each_table_part_count))) {
        LOG_WARN("fail get scan ops", K(ret));
      } else {
        ObGranuleIteratorSpec* gi_op = reinterpret_cast<ObGranuleIteratorSpec*>(&root);
        ObOperatorKit* kit = ctx.get_operator_kit(gi_op->id_);
        if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit));
        } else if (OB_ISNULL(GCTX.par_ser_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL ptr unexpected", K(ret));
        } else {
          ObGIOpInput* gi_input = static_cast<ObGIOpInput*>(kit->input_);
          ObGranulePumpArgs gp_args(ctx, partition_keys_array, sqc_ctx.partitions_info_, *GCTX.par_ser_);
          gp_args.tablet_size_ = gi_op->get_tablet_size();
          gp_args.parallelism_ = sqc.get_task_count();
          gp_args.gi_attri_flag_ = gi_op->get_gi_flags();
          if (OB_FAIL(gp_args.partitions_info_.assign(sqc_ctx.partitions_info_))) {
            LOG_WARN("Failed to assign partitions info", K(ret));
          } else if (OB_FAIL(sqc_ctx.gi_pump_.add_new_gi_task(gp_args, scan_ops, dml_op))) {
            LOG_WARN("fail init granule iter pump", K(ret));
          } else {
            gi_input->set_granule_pump(&sqc_ctx.gi_pump_);
            gi_input->set_parallelism(sqc.get_task_count());
            LOG_TRACE("setup gi op input", K(gi_input), K(&sqc_ctx.gi_pump_));
          }
        }
      }
    }
  } else if (root.get_type() == PHY_TEMP_TABLE_ACCESS) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    ObTempTableAccessOpInput* access_input = NULL;
    uint64_t* insert_count_ptr = NULL;
    uint64_t* closed_count_ptr = NULL;
    ObOperatorKit* kit = ctx.get_operator_kit(root.id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (OB_ISNULL(insert_count_ptr = (uint64_t*)ctx.get_allocator().alloc(sizeof(uint64_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc count_ptr", K(ret));
    } else if (OB_ISNULL(closed_count_ptr = (uint64_t*)ctx.get_allocator().alloc(sizeof(uint64_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc count_ptr", K(ret));
    } else {
      access_input = static_cast<ObTempTableAccessOpInput*>(kit->input_);
      if (OB_FAIL(access_input->interm_result_ids_.assign(sqc.get_interm_results()))) {
        LOG_WARN("failed to assign to interm result ids.", K(ret));
      } else {
        *insert_count_ptr = sqc.get_interm_results().count();
        *closed_count_ptr = sqc.get_task_count();
        access_input->unfinished_count_ptr_ = reinterpret_cast<uint64_t>(insert_count_ptr);
        access_input->closed_count_ = reinterpret_cast<uint64_t>(closed_count_ptr);
      }
    }
  } else if (root.get_type() == PHY_TEMP_TABLE_INSERT) {
    ObPxSqcMeta& sqc = sqc_arg_.sqc_;
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc.get_task_count(); i++) {
      const uint64_t interm_id = ObDtlChannel::generate_id();
      if (OB_FAIL(sqc_ctx.interm_result_ids_.push_back(interm_id))) {
        LOG_WARN("failed to push back into sqc ctx result ids.", K(ret));
      } else { /*do nothing.*/
      }
    }
    uint64_t* insert_count_ptr = NULL;
    ObTempTableInsertOpSpec* insert_spec = reinterpret_cast<ObTempTableInsertOpSpec*>(&root);
    ObOperatorKit* kit = ctx.get_operator_kit(root.id_);
    if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("operator is NULL", K(ret), KP(kit));
    } else if (OB_ISNULL(insert_count_ptr = (uint64_t*)ctx.get_allocator().alloc(sizeof(uint64_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc count_ptr", K(ret));
    } else {
      ObTempTableInsertOpInput* insert_input = NULL;
      insert_input = static_cast<ObTempTableInsertOpInput*>(kit->input_);
      if (OB_FAIL(insert_input->interm_result_ids_.assign(sqc_ctx.interm_result_ids_))) {
        LOG_WARN("failed to assign to interm result ids.", K(ret));
      } else {
        *insert_count_ptr = sqc.get_task_count();
        insert_input->unfinished_count_ptr_ = reinterpret_cast<uint64_t>(insert_count_ptr);
        sqc_ctx.set_temp_table_id(insert_spec->get_table_id());
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(root.register_to_datahub(ctx))) {
      LOG_WARN("fail register op to datahub", K(ret));
    }
  }
  if (IS_PX_RECEIVE(root.get_type())) {
  } else if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < root.get_child_cnt() && OB_SUCC(ret); ++i) {
      ObOpSpec* child = root.get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child op unexpected", K(ret));
      } else {
        ret = SMART_CALL(
            setup_op_input(ctx, *child, sqc_ctx, tsc_locations, tsc_location_idx, all_scan_ops, dml_op_count));
      }
    }
  }

  // after all operator setup_op_input processed
  if (OB_SUCC(ret) && sqc_arg_.op_spec_root_->get_id() == root.get_id()) {
    ARRAY_FOREACH_X(all_scan_ops, idx, cnt, OB_SUCC(ret))
    {
      if (OB_ISNULL(all_scan_ops.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL scan op ptr unexpected", K(ret));
      } else {
        ObOperatorKit* kit = ctx.get_operator_kit(all_scan_ops.at(idx)->get_id());
        if (OB_ISNULL(kit) || OB_ISNULL(kit->input_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("operator is NULL", K(ret), KP(kit));
        } else {
          ObTableScanOpInput* tsc_input = static_cast<ObTableScanOpInput*>(kit->input_);
          // TSC need this in open()
          // hard code idx to 0
          tsc_input->set_location_idx(0);
        }
      }
    }
  }
  return ret;
}

int ObPxSubCoord::create_tasks(ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx)
{
  int ret = OB_SUCCESS;
  ObPxSqcMeta& sqc = sqc_arg.sqc_;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(sqc_arg.exec_ctx_) || (OB_ISNULL(sqc_arg.op_root_) && OB_ISNULL(sqc_arg.op_spec_root_)) ||
      OB_ISNULL(sqc_arg.des_phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc args should not be NULL", K(ret));
  } else if (OB_FAIL(sqc_ctx.reserve_task_mem(sqc.get_task_count()))) {
    LOG_WARN("fail pre alloc memory", K(sqc), K(ret));
  } else if (OB_UNLIKELY(NULL == (session = sqc_arg.exec_ctx_->get_my_session()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr session", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sqc.get_task_count(); ++i) {
    ObPxTask task;
    const ObAddr& sqc_exec_addr = sqc.get_exec_addr();
    const ObAddr& task_exec_addr = sqc.get_exec_addr();
    const ObAddr& qc_exec_addr = sqc.get_qc_addr();
    task.set_task_id(i);
    task.set_sqc_addr(sqc_exec_addr);
    task.set_exec_addr(task_exec_addr);
    task.set_qc_addr(qc_exec_addr);
    task.set_sqc_id(sqc.get_sqc_id());
    task.set_dfo_id(sqc.get_dfo_id());
    task.set_execution_id(sqc.get_execution_id());
    task.set_qc_id(sqc.get_qc_id());
    task.set_interrupt_id(sqc.get_interrupt_id());
    task.set_fulltree(sqc.is_fulltree());

    if (OB_SUCC(ret)) {
      ObPxTask* task_ptr = nullptr;
      if (OB_FAIL(sqc_ctx.add_task(task, task_ptr))) {
        LOG_ERROR("fail add task. should always SUCC as mem reserved", K(ret));
      } else if (OB_ISNULL(task_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task ptr should not be null", KP(task_ptr), K(ret));
      }
    }
  }
  return ret;
}

int ObPxSubCoord::dispatch_tasks(
    ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx, int64_t& dispatch_worker_count, bool is_fast_sqc)
{
  int ret = OB_SUCCESS;
  dispatch_worker_count = 0;
  ObPxSqcMeta& sqc = sqc_arg.sqc_;
  if (OB_ISNULL(sqc_arg.exec_ctx_) || OB_ISNULL(sqc_arg.sqc_handler_) ||
      (OB_ISNULL(sqc_arg.op_root_) && OB_ISNULL(sqc_arg.op_spec_root_)) || OB_ISNULL(sqc_arg.des_phy_plan_) ||
      (sqc.is_rpc_worker() && 1 != sqc.get_task_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid sqc args", K(ret), K(sqc));
  } else if (sqc.is_rpc_worker() || is_fast_sqc) {
    dispatch_worker_count = 0;
    ret = dispatch_task_to_local_thread(sqc_arg, sqc_ctx, sqc);
  } else {
    // execute all task in thread pool, non-block call
    for (int64_t i = 0; OB_SUCC(ret) && i < sqc.get_task_count(); ++i) {
      sqc_arg.sqc_handler_->inc_ref_count();
      ret = dispatch_task_to_thread_pool(sqc_arg, sqc_ctx, sqc, i);
      if (OB_SUCC(ret)) {
        ++dispatch_worker_count;
      } else {
        sqc_arg.sqc_handler_->dec_ref_count();
      }
    }
  }
  return ret;
}

int ObPxSubCoord::dispatch_task_to_local_thread(ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  ObPxRpcInitTaskArgs args;
  ObPxTask* task_ptr = nullptr;
  int64_t task_idx = 0;  // only 1 task
  if (OB_FAIL(sqc_ctx.get_task(task_idx, task_ptr))) {
    LOG_ERROR("fail add task. should always SUCC as mem reserved", K(ret));
  } else if (OB_ISNULL(task_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task ptr should not be null", KP(task_ptr), K(ret));
  } else {
    args.exec_ctx_ = sqc_arg.exec_ctx_;
    args.op_root_ = sqc_arg.op_root_;
    args.op_spec_root_ = sqc_arg.op_spec_root_;
    args.static_engine_root_ = sqc_arg.static_engine_root_;
    args.des_phy_plan_ = sqc_arg.des_phy_plan_;
    args.task_ = *task_ptr;
    args.sqc_task_ptr_ = task_ptr;
    args.sqc_task_ptr_->task_monitor_info_.record_sched_exec_time_begin();
    args.sqc_handler_ = sqc_arg.sqc_handler_;
  }

  ObPxWorkerRunnable* worker = nullptr;
  if (OB_FAIL(ret)) {
    // fail
  } else if (OB_ISNULL(worker = local_worker_factory_.create_worker())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail create new worker", K(sqc), K(ret));
  } else if (OB_FAIL(worker->run(args))) {
    LOG_WARN("fail run task", K(sqc), K(ret));
  } else {
    LOG_TRACE("success execute local root task", K(task_idx), "task", args.task_);
  }
  return ret;
}

int ObPxSubCoord::dispatch_task_to_thread_pool(
    ObPxRpcInitSqcArgs& sqc_arg, ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc, int64_t task_idx)

{
  int ret = OB_SUCCESS;
  ObPxRpcInitTaskArgs args;
  ObPxTask* task_ptr = nullptr;
  ObPxWorkerRunnable* worker = nullptr;
  if (OB_FAIL(sqc_ctx.get_task(task_idx, task_ptr))) {
    LOG_ERROR("fail add task. should always SUCC as mem reserved", K(ret));
  } else if (OB_ISNULL(task_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task ptr should not be null", KP(task_ptr), K(ret));
  } else {
    if (nullptr != sqc_arg.op_spec_root_) {
      args.set_serialize_param(*sqc_arg.exec_ctx_, *sqc_arg.op_spec_root_, *sqc_arg.des_phy_plan_);
    } else if (nullptr != sqc_arg.op_root_) {
      args.set_serialize_param(*sqc_arg.exec_ctx_, *sqc_arg.op_root_, *sqc_arg.des_phy_plan_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: op root is null", K(ret));
    }
    args.task_ = *task_ptr;
    args.sqc_task_ptr_ = task_ptr;
    args.sqc_task_ptr_->task_monitor_info_.record_sched_exec_time_begin();
    args.sqc_handler_ = sqc_arg.sqc_handler_;
  }

  if (OB_FAIL(ret)) {
    // fail
  } else if (OB_ISNULL(worker = static_cast<ObPxWorkerRunnable*>(thread_worker_factory_.create_worker()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail create new worker", K(ret));
  } else if (OB_FAIL(worker->run(args))) {
    auto stat = MTL_GET(ObPxPoolStat*);
    task_ptr->set_result(ret);
    if (OB_ISNULL(stat)) {
      LOG_ERROR("can't alloc task thread."
                "reservation logic behavior unexpected. can't get pool stat",
          "request_dop",
          sqc.get_task_count(),
          K(task_idx),
          K(sqc),
          K(ret));
    } else {
      LOG_ERROR("can't alloc task thread. reservation logic behavior unexpected",
          "request_dop",
          sqc.get_task_count(),
          "pool_stat",
          *stat,
          K(task_idx),
          K(sqc),
          K(ret));
    }
  } else {
    LOG_TRACE("success issue one task", K(task_idx), K(sqc));
  }
  return ret;
}

int ObPxSubCoord::try_prealloc_data_channel(ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_prealloc_transmit_channel(sqc_ctx, sqc))) {
    LOG_WARN("fail preallocate transmit channel", K(ret));
  } else if (OB_FAIL(try_prealloc_receive_channel(sqc_ctx, sqc))) {
    LOG_WARN("fail preallocate receive channel", K(ret));
  }
  return ret;
}

int ObPxSubCoord::try_prealloc_transmit_channel(ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  if (sqc.is_prealloc_transmit_channel()) {
    if (OB_FAIL(sqc_ctx.transmit_data_ch_provider_.add_msg(sqc.get_transmit_channel_msg()))) {
      LOG_WARN("fail set transmit data ch msg", K(ret));
    }
  }
  return ret;
}

int ObPxSubCoord::try_prealloc_receive_channel(ObSqcCtx& sqc_ctx, ObPxSqcMeta& sqc)
{
  int ret = OB_SUCCESS;
  if (sqc.is_prealloc_receive_channel()) {
    if (sqc.recieve_use_interm_result()) {
      auto& msgs = sqc.get_serial_receive_channels();
      ARRAY_FOREACH_X(msgs, idx, cnt, OB_SUCC(ret))
      {
        if (OB_FAIL(sqc_ctx.receive_data_ch_provider_.add_msg(msgs.at(idx)))) {
          LOG_WARN("fail set receive data ch msg", K(ret));
        }
      }
    } else {
      if (OB_FAIL(sqc_ctx.receive_data_ch_provider_.add_msg(sqc.get_receive_channel_msg()))) {
        LOG_WARN("fail set receive data ch msg", K(ret));
      }
    }
  }
  return ret;
}

// the last worker will invoke this function
int ObPxSubCoord::end_process()
{
  LOG_TRACE("start sqc end process");
  int ret = OB_SUCCESS;
  int64_t dfo_id = sqc_arg_.sqc_.get_dfo_id();
  int64_t sqc_id = sqc_arg_.sqc_.get_sqc_id();
  ObPhysicalPlanCtx* phy_plan_ctx = nullptr;

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sqc_arg_.exec_ctx_) || (OB_ISNULL(sqc_arg_.op_root_) && OB_ISNULL(sqc_arg_.op_spec_root_)) ||
        OB_ISNULL(sqc_arg_.des_phy_plan_) || OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(*sqc_arg_.exec_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sqc args should not be NULL", K(ret));
    } else if (OB_FAIL(sqc_ctx_.sqc_proxy_.check_task_finish_status(phy_plan_ctx->get_timeout_timestamp()))) {
      LOG_WARN("fail check task finish status", K(ret));
    }
  }

  sqc_ctx_.sqc_proxy_.destroy();

  NG_TRACE(tag3);
  LOG_TRACE("exit ObPxSubCoord process", K(ret));
  LOG_TRACE("TIMERECORD ",
      "reserve:=0 name:=SQC dfoid:",
      dfo_id,
      "sqcid:",
      sqc_id,
      "taskid:=-1 end:",
      ObTimeUtility::current_time());
  return ret;
}

int ObPxSubCoord::init_first_buffer_cache(bool is_rpc_worker, int64_t px_dop)
{
  int ret = OB_SUCCESS;
  int64_t dop = is_rpc_worker ? 1 * px_dop : px_dop * px_dop;
  if (OB_ISNULL(sqc_arg_.exec_ctx_) || OB_ISNULL(sqc_arg_.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else if (OB_FAIL(first_buffer_cache_.init(dop, dop))) {
    LOG_WARN("failed to init first buffer cache", K(ret));
  } else {
    ObDtlDfoKey dfo_key;
    sqc_ctx_.sqc_proxy_.get_self_dfo_key(dfo_key);
    first_buffer_cache_.set_first_buffer_key(dfo_key);
    if (OB_FAIL(DTL.get_dfc_server().register_first_buffer_cache(
            sqc_arg_.exec_ctx_->get_my_session()->get_effective_tenant_id(), get_first_buffer_cache()))) {
      if (OB_HASH_EXIST == ret) {
        first_buffer_cache_.destroy();
      }
      LOG_WARN("failed to register first buffer cache", K(ret), K(dfo_key));
    } else {
      sqc_ctx_.sqc_proxy_.set_first_buffer_cache(&first_buffer_cache_);
    }
    LOG_TRACE("trace register first buffer cache", K(ret), K(dfo_key), K(dop), KP(&first_buffer_cache_));
  }
  return ret;
}

void ObPxSubCoord::destroy_first_buffer_cache()
{
  int ret = OB_SUCCESS;
  ObDtlDfoKey& dfo_key = first_buffer_cache_.get_first_buffer_key();
  if (OB_ISNULL(sqc_arg_.exec_ctx_) || OB_ISNULL(sqc_arg_.exec_ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else if (OB_FAIL(DTL.get_dfc_server().unregister_first_buffer_cache(
                 sqc_arg_.exec_ctx_->get_my_session()->get_effective_tenant_id(), dfo_key, &first_buffer_cache_))) {
    LOG_WARN("failed to register first buffer cache", K(ret), K(first_buffer_cache_.get_first_buffer_key()));
  }
  LOG_TRACE("trace unregister first buffer cache", K(ret), K(dfo_key));
}

//////////// END /////////
