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
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/basic/ob_statistics_collector_op.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "common/ob_smart_call.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER((ObStatisticsCollectorSpec, ObOpSpec), row_threshold_, hj_op_ids_, nlj_op_ids_);

int ObStatisticsCollectorSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    void *buf = ctx.get_allocator().alloc(
      sizeof(ObStatisticsCollectorWholeMsg::WholeMsgProvider));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("buf is null", K(ret));
    } else {
      ObStatisticsCollectorWholeMsg::WholeMsgProvider *provider =
        new (buf)ObStatisticsCollectorWholeMsg::WholeMsgProvider();
      ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
      if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(),
                  dtl::DH_STATISTICS_COLLECTOR_WHOLE_MSG, *provider))) {
        LOG_WARN("fail add whole msg provider", K(ret));
      }
    }
  }
  return ret;
}

ObStatisticsCollectorOp::ObStatisticsCollectorOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input)
  : ObByPassOperator(ctx_, spec, input),
    read_row_cnt_(0),
    statistics_collect_done_(false),
    read_store_end_(false),
    material_impl_(op_monitor_info_)
{}

int ObStatisticsCollectorOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left is null", K(ret));
  } else if (is_vectorized() && OB_FAIL(brs_holder_.init(left_->get_spec().output_, eval_ctx_))) {
    LOG_WARN("init brs holder failed", K(ret));
  }
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::ROW_THRESHOLD;
  op_monitor_info_.otherstat_1_value_ = MY_SPEC.row_threshold_;
  LOG_TRACE("statistics collector open", K(spec_.id_), K(MY_SPEC.row_threshold_),
            K(MY_SPEC.hj_op_ids_), K(MY_SPEC.nlj_op_ids_));
  return ret;
}

int ObStatisticsCollectorOp::inner_close()
{
  int ret = OB_SUCCESS;
  material_impl_.reset();
  return ret;
}

int ObStatisticsCollectorOp::inner_rescan()
{
  // Every time a rescan is performed,
  // the adaptive join will re-evaluate the join method.
  int ret = OB_SUCCESS;
  read_row_cnt_ = 0;
  statistics_collect_done_ = false;
  read_store_end_ = false;
  brs_holder_.reset();
  if (OB_FAIL(material_impl_.rescan())) {
    LOG_WARN("material_impl_ rescan failed", K(ret));
  } else if (OB_FAIL(ObByPassOperator::inner_rescan())) {
    LOG_WARN("ObOperator::inner_rescan failed", K(ret));
  } else {
    reset_batchrows();
  }
  return ret;
}

void ObStatisticsCollectorOp::destroy()
{
  material_impl_.unregister_profile_if_necessary();
  material_impl_.~ObMaterialOpImpl();
  ObOperator::destroy();
}

int ObStatisticsCollectorOp::init_material_impl()
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (OB_FAIL(material_impl_.init(tenant_id, &eval_ctx_, &ctx_,
                                  &io_event_observer_,
                                  ObChunkDatumStore::BLOCK_SIZE,
                                  "StatisticsCollector"))) {
    LOG_WARN("failed to init material impl", K(tenant_id));
  } else {
    // Used for setting the initial cache size for automatic memory management.
    material_impl_.set_input_rows(MY_SPEC.row_threshold_);
    material_impl_.set_input_width(MY_SPEC.width_);
    material_impl_.set_operator_type(MY_SPEC.type_);
    material_impl_.set_operator_id(MY_SPEC.id_);
  }
  return ret;
}

int ObStatisticsCollectorOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!statistics_collect_done_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("statistics_collect_done_ unexpected", K(ret), K(MY_SPEC.id_), K(lbt()));
  } else {
    // Read materialized data from the store
    if (OB_FAIL(material_impl_.get_next_row(left_->get_spec().output_))) {
      if (OB_ITER_END == ret) {
        // If the materialized data in the store is completely read,
        // continue reading data from the lower-level operator
        ret = OB_SUCCESS;
        read_store_end_ = true;
      } else {
        LOG_WARN("failed to get next row from datum store");
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(read_store_end_)) {
    // Stream data from the lower-level operator
    if (OB_FAIL(left_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("child_op failed to get next row", K(ret));
      } else {
        set_by_pass(true);
      }
    } else {
      set_by_pass(true);
    }
  }

  return ret;
}

int ObStatisticsCollectorOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (OB_UNLIKELY(!statistics_collect_done_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("statistics_collect_done_ unexpected", K(ret), K(MY_SPEC.id_), K(lbt()));
  } else {
    int64_t read_rows = 0;
    // Read materialized data from the store
    if (OB_FAIL(material_impl_.get_next_batch(left_->get_spec().output_,
                                              batch_cnt,
                                              read_rows))) {
      if (OB_ITER_END == ret) {
        // If the materialized data in the store is completely read,
        // continue reading data from the lower-level operator
        ret = OB_SUCCESS;
        read_store_end_ = true;
      } else {
        LOG_WARN("failed to get next batch from datum store");
      }
    } else {
      brs_.size_ = read_rows;
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(read_store_end_)) {
    // Stream data from the lower-level operator
    const ObBatchRows *child_brs = nullptr;
    brs_holder_.restore();
    if (OB_FAIL(left_->get_next_batch(batch_cnt, child_brs))) {
      LOG_WARN("child_op failed to get next batch", K(ret));
    } else if (OB_FAIL(brs_.copy(child_brs))) {
      LOG_WARN("copy child_brs to brs_ failed", K(ret));
    } else {
      set_by_pass(true);
    }
  }
  return ret;
}

int ObStatisticsCollectorOp::set_op_passed(int64_t op_id)
{
  int ret = OB_SUCCESS;
  ObOperatorKit *kit = ctx_.get_operator_kit(op_id);
  if (OB_NOT_NULL(kit) && OB_NOT_NULL(kit->op_)) {
    ObByPassOperator *by_pass_op = static_cast<ObByPassOperator*>(kit->op_);
    by_pass_op->set_by_pass(true);
    if (OB_FAIL(by_pass_op->process_after_set_passed())) {
      LOG_WARN("by_pass_op fail to process after set passed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to visit op", K(ret), K(kit), K(op_id));
  }
  return ret;
}

int ObStatisticsCollectorOp::set_join_passed(bool use_hash_join)
{
  int ret = OB_SUCCESS;
  if (use_hash_join) {
    for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.nlj_op_ids_.count(); ++i) {
      if (OB_FAIL(set_op_passed(MY_SPEC.nlj_op_ids_.at(i)))) {
        LOG_WARN("Fail to set hash join op pass", K(ret));
      }
    }
  } else {
    for (int i = 0; OB_SUCC(ret) && i < MY_SPEC.hj_op_ids_.count(); ++i) {
      if (OB_FAIL(set_op_passed(MY_SPEC.hj_op_ids_.at(i)))) {
        LOG_WARN("Fail to set hash join op pass", K(ret));
      }
    }
  }
  statistics_collect_done_ = true;
  return ret;
}

int ObStatisticsCollectorOp::global_decide_join_method(bool &use_hash_join)
{
  // If local chooses hash_join, then globally it must also be hash_join,
  // no need to wait for the response
  return global_decide_join_method(use_hash_join, !use_hash_join);
}

int ObStatisticsCollectorOp::global_decide_join_method(bool &use_hash_join,
                                                  bool need_wait_whole_msg)
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_NOT_NULL(handler)) {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObStatisticsCollectorPieceMsg piece_msg;
    piece_msg.op_id_ = MY_SPEC.id_;
    piece_msg.thread_id_ = GETTID();
    piece_msg.source_dfo_id_ = proxy.get_dfo_id();
    piece_msg.target_dfo_id_ =  proxy.get_dfo_id();
    piece_msg.use_hash_join_ = use_hash_join;
    const ObStatisticsCollectorWholeMsg *whole_msg = nullptr;
    if (OB_FAIL(proxy.get_dh_msg(get_spec().id_,
                                dtl::DH_STATISTICS_COLLECTOR_WHOLE_MSG,
                                piece_msg,
                                whole_msg,
                                ctx_.get_physical_plan_ctx()->get_timeout_timestamp(),
                                true,
                                need_wait_whole_msg))) {
      LOG_WARN("failed to send piece msg", K(ret));
    } else if (need_wait_whole_msg) {
      if (OB_ISNULL(whole_msg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("whole_msg is null", K(ret));
      } else {
        use_hash_join = whole_msg->use_hash_join_;
      }
    }
  }
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::JOIN_METHOD;
  op_monitor_info_.otherstat_2_value_ = use_hash_join;
  return ret;
}

int ObStatisticsCollectorOp::do_row_statistics_collect()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(statistics_collect_done_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set pass unexpected", K(ret));
  } else {
    if (OB_FAIL(init_material_impl())) {
      LOG_WARN("failed to init material impl");
    }
    while (OB_SUCC(ret) && OB_LIKELY(read_row_cnt_ <= MY_SPEC.row_threshold_)) {
      if (OB_FAIL(left_->get_next_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("child_op failed to get next row", K(ret));
        }
      } else if (OB_FAIL(material_impl_.add_row(left_->get_spec().output_))) {
        LOG_WARN("add store row failed", K(ret));
      } else {
        ++read_row_cnt_;
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      // Reading is completed, yet the threshold has not been exceeded;
      // locally determine to use the nested-loop join.
      bool use_hash_join = false;
      if (OB_FAIL(global_decide_join_method(use_hash_join))) {
        LOG_WARN("Fail to decide join method globally", K(ret), K(use_hash_join));
      } else if (OB_FAIL(set_join_passed(use_hash_join))) {
        LOG_WARN("Fail to set join passed", K(ret), K(use_hash_join));
      }
    } else if (OB_SUCC(ret) && read_row_cnt_ > MY_SPEC.row_threshold_) {
      // Threshold exceeded, switch to hash join.
      bool use_hash_join = true;
      if (OB_FAIL(global_decide_join_method(use_hash_join))) {
        LOG_WARN("Fail to decide join method globally", K(ret), K(use_hash_join));
      } else if (OB_FAIL(set_join_passed(use_hash_join))) {
        LOG_WARN("Fail to set join passed", K(ret), K(use_hash_join));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(material_impl_.finish_add_row())) {
        LOG_WARN("failed to finish add row to row store");
      }
    }
  }
  return ret;
}

int ObStatisticsCollectorOp::do_batch_statistics_collect()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(statistics_collect_done_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set pass unexpected", K(ret));
  } else {
    const ObBatchRows *child_brs = nullptr;
    int64_t stored_rows_cnt = 0;
    if (OB_FAIL(init_material_impl())) {
      LOG_WARN("failed to init material impl");
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(left_->get_next_batch(MY_SPEC.max_batch_size_, child_brs))) {
        LOG_WARN("child_op failed to get next row", K(ret));
      } else {
        if (child_brs->size_ > 0) {
          if (OB_FAIL(material_impl_.add_batch(child_->get_spec().output_,
                                                  *child_brs->skip_,
                                                  child_brs->size_,
                                                  stored_rows_cnt))) {
            LOG_WARN("add store row failed", K(ret));
          } else {
            read_row_cnt_ += stored_rows_cnt;
          }
        }
        if (OB_SUCC(ret)) {
          if (read_row_cnt_ > MY_SPEC.row_threshold_) {
            // Threshold exceeded, switch to hash join.
            bool use_hash_join = true;
            if (OB_FAIL(global_decide_join_method(use_hash_join))) {
              LOG_WARN("Fail to decide join method globally", K(ret), K(use_hash_join));
            } else if (OB_FAIL(set_join_passed(use_hash_join))) {
              LOG_WARN("Fail to set join passed", K(ret), K(use_hash_join));
            } else if (OB_FAIL(brs_holder_.save(MY_SPEC.max_batch_size_))) {
              LOG_WARN("save failed", K(ret));
            }
            break;
          } else if (child_brs->end_) {
            // Reading is completed, yet the threshold has not been exceeded;
            // locally determine to use the nested-loop join.
            bool use_hash_join = false;
            if (OB_FAIL(global_decide_join_method(use_hash_join))) {
              LOG_WARN("Fail to decide join method globally", K(ret), K(use_hash_join));
            } else if (OB_FAIL(set_join_passed(use_hash_join))) {
              LOG_WARN("Fail to set join passed", K(ret), K(use_hash_join));
            } else if (OB_FAIL(brs_holder_.save(MY_SPEC.max_batch_size_))) {
              LOG_WARN("save failed", K(ret));
            }
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(material_impl_.finish_add_row())) {
        LOG_WARN("failed to finish add row to row store");
      }
    }
  }
  return ret;
}

int ObStatisticsCollectorOp::inner_drain_exch()
{
  int ret = OB_SUCCESS;
  if (!statistics_collect_done_) {
    bool use_hash_join = false;
    if (OB_FAIL(global_decide_join_method(use_hash_join, false))) {
      LOG_WARN("Failed to global decide join method", K(ret));
    }
    statistics_collect_done_ = true;
  }
  return ret;
}