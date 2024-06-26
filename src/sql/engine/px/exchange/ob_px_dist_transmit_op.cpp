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

#include "ob_px_dist_transmit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

namespace oceanbase
{
using namespace common;
namespace sql
{


OB_SERIALIZE_MEMBER((ObPxDistTransmitOpInput, ObPxTransmitOpInput));

OB_SERIALIZE_MEMBER((ObPxDistTransmitSpec, ObPxTransmitSpec), dist_exprs_,
    dist_hash_funcs_, sort_cmp_funs_, sort_collations_, calc_tablet_id_expr_, popular_values_hash_);

int ObPxDistTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxTransmitOp::inner_open())) {
    LOG_WARN("PX transmit open failed", K(ret));
  } else if (!MY_SPEC.sampling_saving_row_.empty()) {
    if (MY_SPEC.is_vectorized()) {
      if (get_spec().use_rich_format_) {
        OZ(brs_holder_.init(MY_SPEC.sampling_saving_row_, eval_ctx_));
      } else {
        OZ(vec_holder_.init(MY_SPEC.sampling_saving_row_, eval_ctx_));
      }
    } else {
      OZ(last_row_.init(ctx_.get_allocator(), MY_SPEC.sampling_saving_row_.count()));
    }
  }
  return ret;
}

int ObPxDistTransmitOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (sample_done_ && NULL != cur_transmit_sampled_rows_) {
    clear_evaluated_flag();
    const ObRADatumStore::StoredRow *sr = NULL;
    OZ(sampled_input_rows_.get_row(cur_transmit_sampled_rows_->first, sr));
    OZ(sr->to_expr(MY_SPEC.sampling_saving_row_, eval_ctx_));
    if (OB_SUCC(ret)) {
      cur_transmit_sampled_rows_->first += 1;
      if (cur_transmit_sampled_rows_->first >= cur_transmit_sampled_rows_->second) {
        if (cur_transmit_sampled_rows_
            == &sampled_rows2transmit_.at(sampled_rows2transmit_.count() - 1)) {
          cur_transmit_sampled_rows_ = NULL;
        } else {
          cur_transmit_sampled_rows_ += 1;
        }
      }
    }
  } else {
    if (last_row_.is_saved()) {
      last_row_.reuse();
      OZ(last_row_.restore(MY_SPEC.sampling_saving_row_, eval_ctx_));
    }
    OZ(ObPxTransmitOp::inner_get_next_row());
  }
  return ret;
}

int ObPxDistTransmitOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  return get_spec().use_rich_format_ ? next_vector(max_row_cnt) : next_batch(max_row_cnt);
}

int ObPxDistTransmitOp::next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (sample_done_ && NULL != cur_transmit_sampled_rows_) {
    clear_evaluated_flag();
    brs_.size_ = std::min(std::min(max_row_cnt, MY_SPEC.max_batch_size_),
                          cur_transmit_sampled_rows_->second - cur_transmit_sampled_rows_->first);
    brs_.end_ = false;
    brs_.skip_->reset(brs_.size_);
    ObEvalCtx::BatchInfoScopeGuard g(eval_ctx_);
    g.set_batch_size(brs_.size_);
    sampled_input_rows_it_age_.inc();
    for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
      g.set_batch_idx(i);
      const ObRADatumStore::StoredRow *sr = NULL;
      OZ(sampled_input_rows_.get_row(cur_transmit_sampled_rows_->first + i, sr));
      OZ(sr->to_expr(MY_SPEC.sampling_saving_row_, eval_ctx_));
      if (OB_SUCC(ret)) {
        LOG_DEBUG("fetch row for transmit", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.sampling_saving_row_));
      }
    }
    if (OB_SUCC(ret)) {
      cur_transmit_sampled_rows_->first += brs_.size_;
      if (cur_transmit_sampled_rows_->first >= cur_transmit_sampled_rows_->second) {
        if (cur_transmit_sampled_rows_
            == &sampled_rows2transmit_.at(sampled_rows2transmit_.count() - 1)) {
          cur_transmit_sampled_rows_ = NULL;
        } else {
          cur_transmit_sampled_rows_ += 1;
        }
      }
    }
  } else {
    if (brs_holder_.is_saved()) {
      OZ(brs_holder_.restore());
      brs_holder_.reset();
    }
    OZ(ObPxTransmitOp::inner_get_next_batch(max_row_cnt));
  }
  return ret;
}

int ObPxDistTransmitOp::next_vector(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  if (sample_done_ && NULL != cur_transmit_sampled_rows_) {
    clear_evaluated_flag();
    int64_t size = std::min(std::min(max_row_cnt, MY_SPEC.max_batch_size_),
                            cur_transmit_sampled_rows_->second - cur_transmit_sampled_rows_->first);
    for (int64_t i = 0; i < MY_SPEC.sampling_saving_row_.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(MY_SPEC.sampling_saving_row_.at(i)->init_vector(eval_ctx_, VEC_UNIFORM, size))) {
        LOG_WARN("init vector failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      brs_.size_ = size;
      brs_.end_ = false;
      brs_.skip_->reset(brs_.size_);
      ObEvalCtx::BatchInfoScopeGuard g(eval_ctx_);
      g.set_batch_size(brs_.size_);
      sampled_input_rows_it_age_.inc();
      for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
        g.set_batch_idx(i);
        const ObRADatumStore::StoredRow *sr = NULL;
        OZ(sampled_input_rows_.get_row(cur_transmit_sampled_rows_->first + i, sr));
        OZ(sr->to_expr(MY_SPEC.sampling_saving_row_, eval_ctx_));
        if (OB_SUCC(ret)) {
          LOG_DEBUG("fetch row for transmit", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.sampling_saving_row_));
        }
      }
    }
    if (OB_SUCC(ret)) {
      cur_transmit_sampled_rows_->first += brs_.size_;
      if (cur_transmit_sampled_rows_->first >= cur_transmit_sampled_rows_->second) {
        if (cur_transmit_sampled_rows_
            == &sampled_rows2transmit_.at(sampled_rows2transmit_.count() - 1)) {
          cur_transmit_sampled_rows_ = NULL;
        } else {
          cur_transmit_sampled_rows_ += 1;
        }
      }
    }
  } else {
    OZ(vec_holder_.restore());
    vec_holder_.reset();
    OZ(ObPxTransmitOp::inner_get_next_batch(max_row_cnt));
  }
  return ret;
}

int ObPxDistTransmitOp::do_transmit()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  int64_t use_shared_bcast_msg = ObBcastOptimization::BC_TO_WORKER;
  if (ObPQDistributeMethod::LOCAL == MY_SPEC.dist_method_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid PX distribution method",  K(ret), K(MY_SPEC.dist_method_));
  } else if (OB_FAIL(ctx_.get_my_session()->get_sys_variable(share::SYS_VAR__OB_PX_BCAST_OPTIMIZATION, use_shared_bcast_msg))) {
    LOG_WARN("failed to get system variable", K(ret));
  } else if (ObPQDistributeMethod::BROADCAST == MY_SPEC.dist_method_) {
    use_bcast_opt_ = (ObBcastOptimization::BC_TO_SERVER == use_shared_bcast_msg);
    if (use_shared_bcast_msg && OB_FAIL(chs_agent_.init(
        dfc_,
        task_ch_set_,
        task_channels_,
        ctx_.get_my_session()->get_effective_tenant_id(),
        phy_plan_ctx->get_timeout_timestamp()))) {
      LOG_WARN("failed to init chs agent", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("do distribution", K(MY_SPEC.dist_method_),
        "method", ObPQDistributeMethod::get_type_string(MY_SPEC.dist_method_),
        "consumer PX worker count", task_channels_.count());
    switch (MY_SPEC.dist_method_) {
      case ObPQDistributeMethod::HASH: {
        if (OB_FAIL(do_hash_dist())) {
          LOG_WARN("do hash distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::BC2HOST: {
        if (OB_FAIL(do_bc2host_dist())) {
          LOG_WARN("do BC2HOST distribution failed", K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::RANDOM: {
        if (OB_FAIL(do_random_dist())) {
          LOG_WARN("do random distribution failed",  K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::BROADCAST: {
        if (OB_FAIL(do_broadcast_dist())) {
          LOG_WARN("do broadcast distribution failed",  K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::SM_BROADCAST: {
        if (OB_FAIL(do_sm_broadcast_dist())) {
          LOG_WARN("do broadcast distribution failed",  K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::PARTITION_HASH: {
        if (OB_FAIL(do_sm_pkey_hash_dist())) {
          LOG_WARN("do broadcast distribution failed",  K(ret));
        }
        break;
      }
       case ObPQDistributeMethod::RANGE: {
        if (OB_FAIL(do_range_dist())) {
          LOG_WARN("do broadcast distribution failed",  K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::HYBRID_HASH_BROADCAST: {
        if (OB_FAIL(do_hybrid_hash_broadcast_dist())) {
          LOG_WARN("do broadcast distribution failed",  K(ret));
        }
        break;
      }
      case ObPQDistributeMethod::HYBRID_HASH_RANDOM: {
        if (OB_FAIL(do_hybrid_hash_random_dist())) {
          LOG_WARN("do broadcast distribution failed",  K(ret));
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "this transmit distribution method");
        LOG_WARN("distribution method not supported right now", K(ret), K(MY_SPEC.dist_method_));
      }
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_hash_dist()
{
  int ret = OB_SUCCESS;
  if (MY_SPEC.need_null_aware_shuffle_) {
    ObNullAwareHashSliceIdCalc slice_id_calc(ctx_.get_allocator(),
                                             task_channels_.count(),
                                             &MY_SPEC.dist_exprs_,
                                             &MY_SPEC.dist_hash_funcs_);
    if (MY_SPEC.is_rollup_hybrid_ || MY_SPEC.is_wf_hybrid_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: is_rollup_hybrid or MY_SPEC.is_wf_hybrid_ is true",
               K(ret), K(MY_SPEC.is_rollup_hybrid_), K(MY_SPEC.is_wf_hybrid_));
    } else if (OB_FAIL(send_rows<ObSliceIdxCalc::NULL_AWARE_HASH>(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  } else if (MY_SPEC.is_wf_hybrid_) {
    ObWfHybridDistSliceIdCalc wf_hybrid_slice_id_calc(
        ctx_.get_allocator(), task_channels_.count(),
        MY_SPEC.null_row_dist_method_,
        &MY_SPEC.dist_exprs_, &MY_SPEC.dist_hash_funcs_);
    if (OB_FAIL(send_rows<ObSliceIdxCalc::WF_HYBRID>(wf_hybrid_slice_id_calc))) {
      LOG_WARN("row wf hybrid distribution failed", K(ret));
    }
  } else {
    ObHashSliceIdCalc slice_id_calc(
                    ctx_.get_allocator(), task_channels_.count(),
                    MY_SPEC.null_row_dist_method_,
                    &MY_SPEC.dist_exprs_, &MY_SPEC.dist_hash_funcs_);
    if (OB_FAIL(send_rows<ObSliceIdxCalc::HASH>(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_bc2host_dist()
{
  int ret = OB_SUCCESS;
  ObBc2HostSliceIdCalc::ChannelIdxArray channel_idx;
  ObBc2HostSliceIdCalc::HostIdxArray host_idx;
  auto &channels = task_channels_;
  for (int64_t i = 0; i < task_channels_.count() && OB_SUCC(ret); i++) {
    if (OB_FAIL(channel_idx.push_back(i))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(channel_idx.begin(), channel_idx.end(), [&channels](int64_t l, int64_t r) {
        return channels.at(l)->get_peer() < channels.at(r)->get_peer(); });
  }
  ObBc2HostSliceIdCalc::HostIndex hi;
  uint64_t idx = 0;
  while (OB_SUCC(ret) && idx < channel_idx.count()) {
    hi.begin_ = idx;
    while (idx < channel_idx.count()
        && task_channels_.at(channel_idx.at(hi.begin_))->get_peer()
        == task_channels_.at(channel_idx.at(idx))->get_peer()) {
      idx++;
    }
    hi.end_ = idx;
    if (OB_FAIL(host_idx.push_back(hi))) {
      LOG_WARN("array push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_TRACE("broadcast to host distribution",
        "channel_cnt", channel_idx.count(), "host_cnt", host_idx.count());
    ObBc2HostSliceIdCalc slice_id_calc(ctx_.get_allocator(),
                                       channel_idx,
                                       host_idx,
                                       MY_SPEC.null_row_dist_method_);
    if (OB_FAIL(send_rows<ObSliceIdxCalc::BC2HOST>(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_random_dist()
{
  int ret = OB_SUCCESS;
  ObRandomSliceIdCalc slice_id_calc(ctx_.get_allocator(), task_channels_.count());
  if (OB_FAIL(send_rows<ObSliceIdxCalc::RANDOM>(slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_broadcast_dist()
{
  int ret = OB_SUCCESS;
  ObBroadcastSliceIdCalc slice_id_calc(ctx_.get_allocator(),
                                       task_channels_.count(),
                                       MY_SPEC.null_row_dist_method_);
  if (!use_bcast_opt_) {
    if (OB_FAIL(send_rows<ObSliceIdxCalc::BROADCAST>(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  } else if (get_spec().use_rich_format_) {
    if (OB_FAIL(broadcast_rows<true>(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  } else {
    if (OB_FAIL(broadcast_rows<false>(slice_id_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_hybrid_hash_random_dist()
{
  int ret = OB_SUCCESS;
  ObHybridHashRandomSliceIdCalc slice_id_calc(
      ctx_.get_allocator(), task_channels_.count(),
      MY_SPEC.null_row_dist_method_,
      &MY_SPEC.dist_exprs_, &MY_SPEC.dist_hash_funcs_,
      &MY_SPEC.popular_values_hash_);
  if (OB_FAIL(send_rows<ObSliceIdxCalc::HYBRID_HASH_RANDOM>(slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_hybrid_hash_broadcast_dist()
{
  int ret = OB_SUCCESS;
  ObHybridHashBroadcastSliceIdCalc slice_id_calc(
      ctx_.get_allocator(), task_channels_.count(),
      MY_SPEC.null_row_dist_method_,
      &MY_SPEC.dist_exprs_, &MY_SPEC.dist_hash_funcs_,
      &MY_SPEC.popular_values_hash_);
  if (OB_FAIL(send_rows<ObSliceIdxCalc::HYBRID_HASH_BROADCAST>(slice_id_calc))) {
    LOG_WARN("row distribution failed", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::do_range_dist()
{
  int ret = OB_SUCCESS;
  const ObPxTabletRange *range = NULL;
  if (!sample_done_) {
    ObDynamicSamplePieceMsg piece_msg;
    if (OB_FAIL(build_ds_piece_msg(task_channels_.count(), piece_msg))) {
      LOG_WARN("fail to buil ds piece msg", K(ret));
    } else if (OB_FAIL(do_datahub_dynamic_sample(MY_SPEC.id_, piece_msg))) {
      LOG_WARN("fail to do dynamic sample");
    } else {
      sample_done_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    ObPxSqcHandler *handler = ctx_.get_sqc_handler();
    range = handler->get_partition_ranges().empty() ? NULL : &handler->get_partition_ranges().at(0);
    ObRangeSliceIdCalc slice_id_calc(ctx_.get_allocator(), task_channels_.count(),
      range, &MY_SPEC.dist_exprs_, MY_SPEC.sort_cmp_funs_, MY_SPEC.sort_collations_);
    if (ObPxSampleType::OBJECT_SAMPLE == MY_SPEC.sample_type_) {
      if (OB_FAIL(child_->rescan())) {
        LOG_WARN("fail to rescan child", K(ret));
      } else if (OB_FAIL(ObOperator::inner_rescan())) {
        LOG_WARN("fail to inner rescan", K(ret));
      } else {
        iter_end_ = false;
        consume_first_row_ = true;
      }
    }
    OZ(send_rows<ObSliceIdxCalc::RANGE>(slice_id_calc));
  }
  return ret;
}

int ObPxDistTransmitOp::do_sm_broadcast_dist()
{
  int ret = OB_SUCCESS;
  ObPxDistTransmitOpInput *trans_input = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  uint64_t repart_ref_table_id = MY_SPEC.repartition_ref_table_id_;
  if (OB_ISNULL(trans_input = static_cast<ObPxDistTransmitOpInput *>(get_input()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
      ctx_.get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
    ctx_.get_my_session()->get_effective_tenant_id(),
    repart_ref_table_id, table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K(repart_ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null. repart sharding requires a table in dfo",
             K(repart_ref_table_id), K(ret));
  } else if (OB_FAIL(trans_input->get_part_ch_map(part_ch_info_,
                                          ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
    LOG_WARN("fail to get channel affinity map", K(ret));
  } else {
    ObSlaveMapBcastIdxCalc slice_idx_calc(ctx_,
                                          *table_schema,
                                          MY_SPEC.calc_tablet_id_expr_,
                                          MY_SPEC.unmatch_row_dist_method_,
                                          MY_SPEC.null_row_dist_method_,
                                          task_channels_.count(),
                                          part_ch_info_,
                                          MY_SPEC.repartition_type_);
    if (OB_FAIL(send_rows<ObSliceIdxCalc::SM_BROADCAST>(slice_idx_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
  }
  return ret;
}

int ObPxDistTransmitOp::do_sm_pkey_hash_dist()
{
  int ret = OB_SUCCESS;
  // TODO bin.lb: to be implement
  ObPxDistTransmitOpInput *trans_input = NULL;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  uint64_t repart_ref_table_id = MY_SPEC.repartition_ref_table_id_;
  if (OB_ISNULL(trans_input = static_cast<ObPxDistTransmitOpInput *>(get_input()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input is null", K(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
      ctx_.get_my_session()->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("faile to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(
    ctx_.get_my_session()->get_effective_tenant_id(),
    repart_ref_table_id, table_schema))) {
    LOG_WARN("faile to get table schema", K(ret), K(repart_ref_table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is null. repart sharding requires a table in dfo",
             K(repart_ref_table_id), K(ret));
  } else if (OB_FAIL(trans_input->get_part_ch_map(part_ch_info_,
                                          ctx_.get_physical_plan_ctx()->get_timeout_timestamp()))) {
    LOG_WARN("fail to get channel affinity map", K(ret));
  } else {
    ObSlaveMapPkeyHashIdxCalc slice_idx_calc(ctx_,
                                             *table_schema,
                                             MY_SPEC.calc_tablet_id_expr_,
                                             MY_SPEC.unmatch_row_dist_method_,
                                             MY_SPEC.null_row_dist_method_,
                                             part_ch_info_,
                                             task_channels_.count(),
                                             MY_SPEC.dist_exprs_,
                                             MY_SPEC.dist_hash_funcs_,
                                             MY_SPEC.repartition_type_);
    if (OB_FAIL(slice_idx_calc.init())) {
      LOG_WARN("failed to init slice idx calc", K(ret));
    } else if (OB_FAIL(send_rows<ObSliceIdxCalc::SM_REPART_HASH>(slice_idx_calc))) {
      LOG_WARN("row distribution failed", K(ret));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = slice_idx_calc.destroy())) {
      LOG_WARN("failed to destroy", K(tmp_ret));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

void ObPxDistTransmitOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  sampled_input_rows_.set_mem_stat(NULL);
  ObPxTransmitOp::destroy();
}

int ObPxDistTransmitOp::inner_close()
{
  sql_mem_processor_.unregister_profile();
  return ObPxTransmitOp::inner_close();
}

int ObPxDistTransmitOp::build_ds_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg)
{
  int ret = OB_SUCCESS;
  piece_msg.reset();
  if (OB_FAIL(piece_msg.tablet_ids_.push_back(0))) {
    // just generate one partition
    LOG_WARN("fail to push back parititon ids", K(ret));
  } else if (FALSE_IT(piece_msg.sample_type_ = MY_SPEC.sample_type_)) {
  } else if (is_row_sample()) {
    OZ(build_row_sample_piece_msg(expected_range_count, piece_msg));
  } else if (is_object_sample()) {
    OZ(build_object_sample_piece_msg(expected_range_count, piece_msg));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sample type", K(ret));
  }
  return ret;
}

int ObPxDistTransmitOp::build_row_sample_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy &proxy = ctx_.get_sqc_handler()->get_sqc_proxy();
  piece_msg.expect_range_count_ = expected_range_count;
  piece_msg.source_dfo_id_ = proxy.get_dfo_id();
  piece_msg.target_dfo_id_ = proxy.get_dfo_id();
  piece_msg.op_id_ = MY_SPEC.id_;;

  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();

  ObChunkDatumStore *sample_store = OB_NEWx(ObChunkDatumStore, &ctx_.get_allocator(), "DYN_SAMPLE_CTX");
  OV(NULL != sample_store, OB_ALLOCATE_MEMORY_FAILED);

  bool sample_store_dump = false;
  OZ(sample_store->init(0, tenant_id,
                        ObCtxIds::DEFAULT_CTX_ID, "DYN_SAMPLE_CTX", sample_store_dump));

  OZ(piece_msg.row_stores_.push_back(sample_store));
  OZ(sample_stores_.push_back(sample_store));


  int64_t row_count = MY_SPEC.rows_;
  OZ(ObPxEstimateSizeUtil::get_px_size(
          &ctx_, MY_SPEC.px_est_size_factor_, row_count, row_count));

  lib::ContextParam param;
  param.set_mem_attr(tenant_id, "PxSampleRow", ObCtxIds::WORK_AREA);
  OZ(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param));
  sampled_input_rows_.set_mem_stat(&sql_mem_processor_);
  OZ(sql_mem_processor_.init(
          &mem_context_->get_malloc_allocator(),
          tenant_id,
          row_count * MY_SPEC.width_, MY_SPEC.type_, MY_SPEC.id_, &ctx_));
  bool updated = false;
  OZ(sql_mem_processor_.update_max_available_mem_size_periodically(
          &mem_context_->get_malloc_allocator(), [](int64_t) { return true; }, updated));
  OZ(sampled_input_rows_.init(
          sql_mem_processor_.get_mem_bound(), tenant_id,
          ObCtxIds::WORK_AREA, "PxSampleRow"));
  sampled_input_rows_.set_io_observer(&io_event_observer_);
  if (is_vectorized()) {
    if (get_spec().use_rich_format_) {
      OZ(add_batch_row_for_piece_msg_vec(*sample_store));
    } else {
      OZ(add_batch_row_for_piece_msg(*sample_store));
    }
  } else {
    OZ(add_row_for_piece_msg(*sample_store));
  }
  return ret;
}

int ObPxDistTransmitSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPxTransmitSpec::register_to_datahub(ctx))) {
    LOG_WARN("failed to register init channel msg", K(ret));
  } else if (ObPQDistributeMethod::RANGE == dist_method_) {
    if (OB_ISNULL(ctx.get_sqc_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else {
      void *buf = ctx.get_allocator().alloc(sizeof(ObDynamicSampleWholeMsg::WholeMsgProvider));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObDynamicSampleWholeMsg::WholeMsgProvider *provider =
          new (buf)ObDynamicSampleWholeMsg::WholeMsgProvider();
        ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
        if (OB_FAIL(sqc_ctx.add_whole_msg_provider(id_, dtl::DH_DYNAMIC_SAMPLE_WHOLE_MSG, *provider))) {
          LOG_WARN("fail add whole msg provider", K(ret));
        } else {
          char *chunk_buf = (char *)ctx.get_allocator().alloc(sizeof(ObChunkDatumStore));
          if (OB_ISNULL(chunk_buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret));
          } else {
            ObChunkDatumStore *sample_store = new (chunk_buf) ObChunkDatumStore("DYN_SAMPLE_CTX");
            if (OB_FAIL(sample_store->init(0,
              ctx.get_my_session()->get_effective_tenant_id(),
                  ObCtxIds::DEFAULT_CTX_ID, "DYN_SAMPLE_CTX", false/*enable dump*/))) {
              LOG_WARN("init sample chunk store failed", K(ret));
            } else if (OB_FAIL(ctx.get_sqc_handler()->get_sqc_proxy().
                  get_piece_sample_msg().row_stores_.push_back(sample_store))) {
              LOG_WARN("fail to push back sample store", K(ret));
            }
            if (OB_FAIL(ret) && nullptr != sample_store) {
              sample_store->~ObChunkDatumStore();
              ctx.get_allocator().free(sample_store);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPxDistTransmitOp::add_row_for_piece_msg(ObChunkDatumStore &sample_store)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    // do nothing
  } else {
    int64_t max_input_rows = HEADER_INPUT_SAMPLE == MY_SPEC.sample_type_
        ? DYNAMIC_SAMPLE_ROW_COUNT
        : INT64_MAX;
    int64_t mem_hold = 0;
    do {
      // For auto memory manage, ObRADatumStore can not shrink memory used right now,
      // no need to update memory statistics after dumped
      if (!sampled_input_rows_.is_file_open()) {
        bool updated = false;
        OZ(sql_mem_processor_.update_max_available_mem_size_periodically(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t loop_cnt) { return sampled_input_rows_.get_row_cnt() > loop_cnt; },
                updated));
        if (OB_SUCC(ret) && updated) {
          sampled_input_rows_.set_mem_limit(sql_mem_processor_.get_mem_bound());
        }
        if (sampled_input_rows_.get_mem_hold() != mem_hold) {
          mem_hold = sampled_input_rows_.get_mem_hold();
          // try extend memory bound when used memory close to memory bound.
          if (GCONF.is_sql_operator_dump_enabled()
              && mem_hold >= sql_mem_processor_.get_mem_bound() - ObRADatumStore::BIG_BLOCK_SIZE) {
            bool dumped = false;
            OZ(sql_mem_processor_.extend_max_memory_size(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t mem_bould) { return mem_hold > mem_bould; },
                dumped, mem_hold));
            if (OB_SUCC(ret)) {
              sampled_input_rows_.set_mem_limit(sql_mem_processor_.get_mem_bound());
            }
          }
        }
      } else {
        if (profile_.get_number_pass() == 0) {
          profile_.set_number_pass(1);
        }
      }

      // add row to sampled input row store.
      OZ(sampled_input_rows_.add_row(MY_SPEC.sampling_saving_row_, &eval_ctx_));
      if (OB_SUCC(ret)) {
        ret = inner_get_next_row();
      }
    } while (OB_SUCC(ret) && sampled_input_rows_.get_row_cnt() < max_input_rows);

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      OZ(last_row_.shadow_copy(MY_SPEC.sampling_saving_row_, eval_ctx_));
    }

    OZ(setup_sampled_rows_output());

    if (OB_SUCC(ret)) {
      const int64_t input_rows = sampled_input_rows_.get_row_cnt();
      int64_t sample_rows = input_rows / DYNAMIC_SAMPLE_INTERVAL;
      sample_rows = std::max(sample_rows, std::min(input_rows, (int64_t)DYNAMIC_SAMPLE_ROW_COUNT));
      sample_rows = std::min(sample_rows, (int64_t)MAX_DYNAMIC_SAMPLE_ROW_COUNT);
      const int64_t step = input_rows / sample_rows;

      for (int64_t i = 0; OB_SUCC(ret) && i < input_rows; i += step) {
        clear_evaluated_flag();
        const ObRADatumStore::StoredRow *sr = NULL;
        OZ(sampled_input_rows_.get_row(i, sr));
        OZ(sr->to_expr(MY_SPEC.sampling_saving_row_, eval_ctx_));
        OZ(sample_store.add_row(MY_SPEC.dist_exprs_, &eval_ctx_));
      }
    }
  }
  return ret;
}

int ObPxDistTransmitOp::add_batch_row_for_piece_msg(ObChunkDatumStore &sample_store)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    // do nothing
  } else {
    int64_t max_input_rows = HEADER_INPUT_SAMPLE == MY_SPEC.sample_type_
        ? DYNAMIC_SAMPLE_ROW_COUNT
        : INT64_MAX;
    int64_t mem_hold = 0;
    do {
      // For auto memory manage, ObRADatumStore can not shrink memory used right now,
      // no need to update memory statistics after dumped
      if (!sampled_input_rows_.is_file_open()) {
        bool updated = false;
        OZ(sql_mem_processor_.update_max_available_mem_size_periodically(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t loop_cnt) { return sampled_input_rows_.get_row_cnt() > loop_cnt; },
                updated));
        if (OB_SUCC(ret) && updated) {
          sampled_input_rows_.set_mem_limit(sql_mem_processor_.get_mem_bound());
        }
        if (sampled_input_rows_.get_mem_hold() != mem_hold) {
          mem_hold = sampled_input_rows_.get_mem_hold();
          // try extend memory bound when used memory close to memory bound.
          if (GCONF.is_sql_operator_dump_enabled()
              && mem_hold >= sql_mem_processor_.get_mem_bound() - ObRADatumStore::BIG_BLOCK_SIZE) {
            bool dumped = false;
            OZ(sql_mem_processor_.extend_max_memory_size(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t mem_bould) { return mem_hold > mem_bould; },
                dumped, mem_hold));
            if (OB_SUCC(ret)) {
              sampled_input_rows_.set_mem_limit(sql_mem_processor_.get_mem_bound());
            }
          }
        }
      } else {
        if (profile_.get_number_pass() == 0) {
          profile_.set_number_pass(1);
        }
      }

      // add batch rows to sampled input row store.
      {
        ObEvalCtx::BatchInfoScopeGuard g(eval_ctx_);
        g.set_batch_size(brs_.size_);
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
          if (brs_.skip_->at(i)) {
            continue;
          }
          g.set_batch_idx(i);
          OZ(sampled_input_rows_.add_row(MY_SPEC.sampling_saving_row_, &eval_ctx_));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t cnt = std::min(max_input_rows - sampled_input_rows_.get_row_cnt(),
                               MY_SPEC.max_batch_size_);
        if (cnt > 0) {
          ret = inner_get_next_batch(cnt);
        }
      }
    } while (OB_SUCC(ret)
             && sampled_input_rows_.get_row_cnt() < max_input_rows
             && !(brs_.end_ && brs_.size_ == 0));

    if (OB_FAIL(ret)) {
    } else if (!brs_.end_) {
      OZ(brs_holder_.save(std::min((int64_t)DYNAMIC_SAMPLE_ROW_COUNT, MY_SPEC.max_batch_size_)));
    } else {
      brs_.end_ = false;
    }
    OZ(setup_sampled_rows_output());

    if (OB_SUCC(ret)) {
      const int64_t input_rows = sampled_input_rows_.get_row_cnt();
      int64_t sample_rows = input_rows / DYNAMIC_SAMPLE_INTERVAL;
      sample_rows = std::max(sample_rows, std::min(input_rows, (int64_t)DYNAMIC_SAMPLE_ROW_COUNT));
      sample_rows = std::min(sample_rows, (int64_t)MAX_DYNAMIC_SAMPLE_ROW_COUNT);
      const int64_t step = input_rows / sample_rows;

      ObEvalCtx::BatchInfoScopeGuard g(eval_ctx_);
      g.set_batch_size(1);
      g.set_batch_idx(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < input_rows; i += step) {
        clear_evaluated_flag();
        const ObRADatumStore::StoredRow *sr = NULL;
        OZ(sampled_input_rows_.get_row(i, sr));
        OZ(sr->to_expr(MY_SPEC.sampling_saving_row_, eval_ctx_));
        OZ(sample_store.add_row(MY_SPEC.dist_exprs_, &eval_ctx_));
      }
      clear_evaluated_flag();
    }
  }
  return ret;
}

int ObPxDistTransmitOp::add_batch_row_for_piece_msg_vec(ObChunkDatumStore &sample_store)
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    // do nothing
  } else {
    int64_t max_input_rows = HEADER_INPUT_SAMPLE == MY_SPEC.sample_type_
        ? DYNAMIC_SAMPLE_ROW_COUNT
        : INT64_MAX;
    int64_t mem_hold = 0;
    do {
      // For auto memory manage, ObRADatumStore can not shrink memory used right now,
      // no need to update memory statistics after dumped
      if (!sampled_input_rows_.is_file_open()) {
        bool updated = false;
        OZ(sql_mem_processor_.update_max_available_mem_size_periodically(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t loop_cnt) { return sampled_input_rows_.get_row_cnt() > loop_cnt; },
                updated));
        if (OB_SUCC(ret) && updated) {
          sampled_input_rows_.set_mem_limit(sql_mem_processor_.get_mem_bound());
        }
        if (sampled_input_rows_.get_mem_hold() != mem_hold) {
          mem_hold = sampled_input_rows_.get_mem_hold();
          // try extend memory bound when used memory close to memory bound.
          if (GCONF.is_sql_operator_dump_enabled()
              && mem_hold >= sql_mem_processor_.get_mem_bound() - ObRADatumStore::BIG_BLOCK_SIZE) {
            bool dumped = false;
            OZ(sql_mem_processor_.extend_max_memory_size(
                &mem_context_->get_malloc_allocator(),
                [&](int64_t mem_bould) { return mem_hold > mem_bould; },
                dumped, mem_hold));
            if (OB_SUCC(ret)) {
              sampled_input_rows_.set_mem_limit(sql_mem_processor_.get_mem_bound());
            }
          }
        }
      } else {
        if (profile_.get_number_pass() == 0) {
          profile_.set_number_pass(1);
        }
      }

      // add batch rows to sampled input row store.
      {
        ObEvalCtx::BatchInfoScopeGuard g(eval_ctx_);
        g.set_batch_size(brs_.size_);
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
          if (brs_.skip_->at(i)) {
            continue;
          }
          g.set_batch_idx(i);
          OZ(sampled_input_rows_.add_row(MY_SPEC.sampling_saving_row_, &eval_ctx_));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t cnt = std::min(max_input_rows - sampled_input_rows_.get_row_cnt(),
                               MY_SPEC.max_batch_size_);
        if (cnt > 0) {
          ret = inner_get_next_batch(cnt);
          FOREACH_CNT_X(e, MY_SPEC.sampling_saving_row_, OB_SUCC(ret)) {
            if (OB_FAIL((*e)->cast_to_uniform(brs_.size_, eval_ctx_))) {
              LOG_WARN("cast expr to uniform failed", K(ret), KPC(*e), K_(eval_ctx));
            }
          }
        }
      }
    } while (OB_SUCC(ret)
             && sampled_input_rows_.get_row_cnt() < max_input_rows
             && !(brs_.end_ && brs_.size_ == 0));

    if (OB_FAIL(ret)) {
    } else if (!brs_.end_) {
      OZ(vec_holder_.save(std::min((int64_t)DYNAMIC_SAMPLE_ROW_COUNT, MY_SPEC.max_batch_size_)));
    } else {
      brs_.end_ = false;
    }
    OZ(setup_sampled_rows_output());

    if (OB_SUCC(ret)) {
      const int64_t input_rows = sampled_input_rows_.get_row_cnt();
      int64_t sample_rows = input_rows / DYNAMIC_SAMPLE_INTERVAL;
      sample_rows = std::max(sample_rows, std::min(input_rows, (int64_t)DYNAMIC_SAMPLE_ROW_COUNT));
      sample_rows = std::min(sample_rows, (int64_t)MAX_DYNAMIC_SAMPLE_ROW_COUNT);
      const int64_t step = input_rows / sample_rows;

      ObEvalCtx::BatchInfoScopeGuard g(eval_ctx_);
      g.set_batch_size(1);
      g.set_batch_idx(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < input_rows; i += step) {
        clear_evaluated_flag();
        const ObRADatumStore::StoredRow *sr = NULL;
        OZ(sampled_input_rows_.get_row(i, sr));
        OZ(sr->to_expr(MY_SPEC.sampling_saving_row_, eval_ctx_));
        OZ(sample_store.add_row(MY_SPEC.dist_exprs_, &eval_ctx_));
      }
      clear_evaluated_flag();
    }
  }
  return ret;
}

int ObPxDistTransmitOp::setup_sampled_rows_output()
{
  int ret = OB_SUCCESS;
  int64_t rows = sampled_input_rows_.get_row_cnt();
  if (rows <= 0) {
    // do nothing
  } else if (HEADER_INPUT_SAMPLE == MY_SPEC.sample_type_) {
    OZ(sampled_rows2transmit_.push_back(std::make_pair(0L, rows)));
    if (OB_SUCC(ret)) {
      cur_transmit_sampled_rows_ = &sampled_rows2transmit_.at(0);
    }
  } else if (FULL_INPUT_SAMPLE == MY_SPEC.sample_type_) {
    // split inputs rows in multi ranges, then transmit each range in random order
    const int64_t MIN_RANGE_ROWS = 16L << 10;
    const int64_t dop = task_channels_.count();
    const int64_t range_cnt = std::min((rows + MIN_RANGE_ROWS - 1)/MIN_RANGE_ROWS, dop);
    const int64_t range_rows = (rows + range_cnt - 1) / range_cnt;
    sampled_rows2transmit_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < rows; i+= range_rows) {
      // set second to random number for sort, will be updated to end position later.
      OZ(sampled_rows2transmit_.push_back(std::make_pair(i, nrand48(rand48_buf_))));
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(sampled_rows2transmit_.begin(), sampled_rows2transmit_.end(),
                [](const  std::pair<int64_t, int64_t> &l, const std::pair<int64_t, int64_t> &r)
                { return l.second < r.second; });
      FOREACH(range, sampled_rows2transmit_) {
        range->second = std::min(range->first + range_rows, rows);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sample type", K(ret), K(MY_SPEC.sample_type_));
  }

  if (OB_SUCC(ret) && !sampled_rows2transmit_.empty()) {
    if (MY_SPEC.is_vectorized()) {
      sampled_input_rows_.set_iteration_age(&sampled_input_rows_it_age_);
    }
    cur_transmit_sampled_rows_ = &sampled_rows2transmit_.at(0);
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
