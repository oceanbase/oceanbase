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

#include "ob_px_transmit_op.h"
#include "sql/dtl/ob_dtl_channel_group.h"
#include "sql/dtl/ob_dtl_utils.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/aggregate/ob_merge_groupby_op.h"
#include "sql/engine/aggregate/ob_merge_groupby_vec_op.h"
#include "share/detect/ob_detect_manager_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace sql::dtl;
namespace sql
{

OB_SERIALIZE_MEMBER(ObPxTransmitOpInput, ch_provider_ptr_);

OB_INLINE const ObPxTransmitSpec &get_my_spec(const ObPxTransmitOp &op)
{
  return static_cast<const ObPxTransmitSpec &>(op.get_spec());
}

int ObPxTransmitOpInput::get_part_ch_map(ObPxPartChInfo &map, int64_t timeout_ts)
{
  int ret = OB_SUCCESS;
  map.part_ch_array_.reset();
  ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else if (OB_FAIL(ch_provider->get_part_ch_map(map, timeout_ts))) {
    LOG_WARN("fail get affinity map from provider", K(ret));
  }
  return ret;
}

int ObPxTransmitOpInput::get_parent_dfo_key(ObDtlDfoKey &key)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else {
    ch_provider->get_parent_dfo_key(key);
    if (!ObDfo::is_valid_dfo_id(key.get_dfo_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(key.get_dfo_id()));
    }
  }
  return ret;
}

int ObPxTransmitOpInput::get_self_sqc_info(ObDtlSqcInfo &sqc_info)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else {
    ch_provider->get_self_sqc_info(sqc_info);
    if (!ObDfo::is_valid_dfo_id(sqc_info.get_dfo_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status", K(sqc_info.get_dfo_id()));
    }
  }
  return ret;
}

int ObPxTransmitOpInput::get_data_ch(ObPxTaskChSet &task_ch_set, int64_t timeout_ts, ObDtlChTotalInfo *&ch_info)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy *ch_provider = reinterpret_cast<ObPxSQCProxy *>(ch_provider_ptr_);
  if (OB_ISNULL(ch_provider)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ch provider not init", K(ret));
  } else if (OB_FAIL(ch_provider->get_transmit_data_ch(
      get_sqc_id(), get_task_id(), timeout_ts, task_ch_set, &ch_info))) {
    LOG_WARN("fail get data ch sets from provider", K(ret));
  }
  return ret;
}
//------------- end ObPxTransmitOpInput -------
OB_SERIALIZE_MEMBER((ObPxTransmitSpec, ObTransmitSpec),
    sample_type_, need_null_aware_shuffle_, tablet_id_expr_,
    random_expr_, sampling_saving_row_, repartition_table_id_,
    wf_hybrid_aggr_status_expr_, wf_hybrid_pby_exprs_cnt_array_, ddl_slice_id_expr_);

ObPxTransmitSpec::ObPxTransmitSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTransmitSpec(alloc, type),
      sample_type_(ObPxSampleType::NOT_INIT_SAMPLE_TYPE),
      need_null_aware_shuffle_(false),
      tablet_id_expr_(NULL),
      random_expr_(NULL),
      sampling_saving_row_(alloc),
      repartition_table_id_(0),
      wf_hybrid_aggr_status_expr_(NULL),
      wf_hybrid_pby_exprs_cnt_array_(alloc),
      ddl_slice_id_expr_(NULL)
{
}

ObPxTransmitOp::ObPxTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
: ObTransmitOp(exec_ctx, spec, input),
  px_row_allocator_(common::ObModIds::OB_SQL_PX),
  transmited_(false),
  // first_row_(),
  iter_end_(false),
  consume_first_row_(false),
  dfc_unblock_msg_proc_(dfc_),
  loop_(op_monitor_info_),
  chs_agent_(),
  use_bcast_opt_(false),
  part_ch_info_(),
  ch_info_(nullptr),
  sample_done_(false),
  sample_stores_(),
  cur_transmit_sampled_rows_(NULL),
  has_set_hybrid_key_(false),
  batch_param_remain_(false),
  receive_channel_ready_(false),
  data_msg_type_(dtl::ObDtlMsgType::PX_DATUM_ROW),
  params_(px_row_allocator_)
{
  MEMSET(rand48_buf_, 0, sizeof(rand48_buf_));
}

void ObPxTransmitOp::destroy()
{
  task_ch_set_.reset();
  px_row_allocator_.reset();
  ch_blocks_.reset();
  blk_bufs_.reset();
  task_channels_.reset();
  dfc_.destroy();
  loop_.reset();
  chs_agent_.~ObDtlChanAgent();
  part_ch_info_.~ObPxPartChInfo();
  has_set_hybrid_key_ = false;
  receive_channel_ready_ = false;
  for (int i = 0; i < sample_stores_.count(); ++i) {
    if (OB_NOT_NULL(sample_stores_.at(i))) {
      sample_stores_.at(i)->reset();
    }
  }
  sample_stores_.reset();
  cur_transmit_sampled_rows_ = NULL;
  sampled_rows2transmit_.reset();
  sampled_input_rows_.~ObRADatumStore();
  params_.meta_.reset();
  ObTransmitOp::destroy();
}

int ObPxTransmitOp::inner_open()
{
  int ret = OB_SUCCESS;
  // 这里是一个特殊处理逻辑，本质上是先inner_open然后open最后面打上tag
  // 但是现在transmit有个特殊地方在于inner_open会调用get_next_row，这样导致如果iter_end
  // 就会走到drain_exec，这个会判断，如果没有open，会先open
  // 所以如果inner_open里面嵌套了get_next_row，则opened_ flag就没有设置，这样在drain逻辑里面就会一直open
  // 导致函数栈溢出core掉
  opened_ = true;
  ObPxTransmitOpInput *trans_input = static_cast<ObPxTransmitOpInput*>(input_);
  metric_.set_id(get_spec().id_);
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is NULL", K(ret));
  } else if (OB_UNLIKELY(get_spec().filters_.count() > 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("filter exprs should be empty", K(ret), K(get_spec().filters_.count()));
  } else if (OB_FAIL(ObTransmitOp::inner_open())) {
    LOG_WARN("initialize operator context failed", K(ret));
  } else {
    if (get_spec().use_rich_format_) {
      if (OB_FAIL(init_data_msg_type(get_spec().output_))) {
        LOG_WARN("init data_msg_type failed", K(ret));
      } else if (OB_FAIL(params_.init_basic_params(get_spec().max_batch_size_,
                  task_channels_.count(),
                  get_spec().output_.count(),
                  px_row_allocator_,
                  ctx_))) {
        LOG_WARN("failed to init basic params", K(ret));
      }
    }
    rand48_buf_[0] = 0x330E; // 0x330E is the arbitrary value of srand48
    rand48_buf_[1] = trans_input->get_sqc_id();
    rand48_buf_[2] = trans_input->get_task_id();
    OZ(params_.meta_.init(get_spec().output_, 0, params_.reorder_fixed_expr_));
    if (is_object_sample()) {
      OZ(init_channel(*trans_input));
      OZ(set_expect_range_count());
      OZ(fetch_first_row());
    } else {
      OZ(fetch_first_row());
      OZ(init_channel(*trans_input));
    }
    chs_agent_.set_row_meta(params_.meta_);
    if (OB_SUCC(ret) && get_spec().use_rich_format_) {
      if (dtl::ObDtlMsgType::PX_VECTOR_FIXED == data_msg_type_) {
        int64_t size_per_buffer = GCONF.dtl_buffer_size;
        chs_agent_.set_size_per_buffer(size_per_buffer);
        for (int64_t i = 0; i < task_channels_.count(); ++i) {
          task_channels_.at(i)->set_send_buffer_size(size_per_buffer);
        }
      }
      if (OB_FAIL(params_.init_keep_order_params(get_spec().max_batch_size_,
                                                 task_channels_.count(),
                                                 get_spec().output_.count(),
                                                 px_row_allocator_))) {
        LOG_WARN("failed to init keep order params", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::transmit()
{
  begin_cpu_time_counting();
  int ret = do_transmit();
  end_cpu_time_counting();
  return ret;
}

int ObPxTransmitOp::fetch_first_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = NULL;
  const ObPhysicalPlan *phy_plan = NULL;
  if (OB_ISNULL(phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_)) ||
      OB_ISNULL(phy_plan = phy_plan_ctx->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null phy_plan or phy_plan_ctx", K(phy_plan_ctx), K(phy_plan), K(ret));
  } else {
    const ObBatchRows *brs = NULL;
    if (is_vectorized()) {
      int64_t batch_size = spec_.max_batch_size_;
      if (is_row_sample()) {
        if (ObPQDistributeMethod::RANGE ==
            static_cast<const ObPxTransmitSpec &>(spec_).dist_method_) {
          batch_size = std::min((int64_t)DYNAMIC_SAMPLE_ROW_COUNT, batch_size);
        }
      }
      if (OB_FAIL(ObOperator::get_next_batch(batch_size, brs))) {
        LOG_WARN("get next batch failed", K(ret));
      } else {
        if (brs->end_ && 0 == brs->size_) {
          iter_end_ = true;
          LOG_TRACE("transmit iter end", K(ret), K(iter_end_));
        }
      }
    } else {
      if (OB_FAIL(ObOperator::get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed", K(ret));
        } else {
          iter_end_ = true;
          LOG_TRACE("transmit iter end", K(ret), K(iter_end_));
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

bool ObPxTransmitOp::is_object_sample()
{
  return OBJECT_SAMPLE == MY_SPEC.sample_type_;
}
bool ObPxTransmitOp::is_row_sample()
{
  return HEADER_INPUT_SAMPLE == MY_SPEC.sample_type_
      || FULL_INPUT_SAMPLE == MY_SPEC.sample_type_;
}

int ObPxTransmitOp::set_expect_range_count()
{
  int ret = OB_SUCCESS;
  if (task_channels_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task channels is empty", K(ret));
  } else {
    ctx_.set_expect_range_count(task_channels_.count());
  }
  return ret;
}

int ObPxTransmitOp::init_dfc(ObDtlDfoKey &parent_key, ObDtlSqcInfo &child_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_FAIL(dfc_.init(ctx_.get_my_session()->get_effective_tenant_id(),
                        task_ch_set_.count()))) {
    LOG_WARN("Fail to init dfc", K(ret));
  } else {
    dfc_.set_timeout_ts(phy_plan_ctx->get_timeout_timestamp());
    dfc_.set_transmit();
    dfc_.set_dfo_key(parent_key);
    dfc_.set_sender_sqc_info(child_info);
    dfc_.set_op_metric(&metric_);
    dfc_.set_dtl_channel_watcher(&loop_);
    DTL.get_dfc_server().register_dfc(dfc_);
    LOG_TRACE("Worker init dfc", K(parent_key), K(child_info), K(dfc_.is_receive()),
              K(&dfc_), K(get_spec().get_id()));
  }
  return ret;
}

int ObPxTransmitOp::init_channel(ObPxTransmitOpInput &trans_input)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("TIMERECORD ", "reserve:=1 name:=TASK dfoid:", trans_input.get_dfo_id(),
      "sqcid:", trans_input.get_sqc_id(),
      "taskid:", trans_input.get_task_id(),
      "start:", ObTimeUtility::current_time());
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  ObDtlDfoKey parent_key;
  ObDtlSqcInfo self_info;
  LOG_TRACE("Try to get channel information from SQC", K(lbt()));
  CK (OB_NOT_NULL(ctx_.get_physical_plan_ctx()) && OB_NOT_NULL(ctx_.get_physical_plan_ctx()->get_phy_plan()));
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans_input.get_data_ch(
              task_ch_set_, phy_plan_ctx->get_timeout_timestamp(), ch_info_))) {
    LOG_WARN("Fail to get data dtl channel", K(ret));
  } else if (OB_FAIL(trans_input.get_parent_dfo_key(parent_key))) {
    LOG_WARN("Failed to get parent dfo key", K(ret));
  } else if (OB_FAIL(trans_input.get_self_sqc_info(self_info))) {
    LOG_WARN("Failed to get parent dfo key", K(ret));
  } else if (OB_FAIL(init_dfc(parent_key, self_info))) {
    LOG_WARN("Failed to init dfc", K(ret));
  } else if (OB_FAIL(ObPxTransmitOp::link_ch_sets(task_ch_set_, task_channels_, &dfc_))) {
    LOG_WARN("Fail to link data channel", K(ret));
  } else if (is_vectorized() && OB_FAIL(init_channels_cur_block(task_channels_))) {
    LOG_WARN("fail to init channels block info", K(ret));
  } else {
    uint64_t min_cluster_version = ctx_.get_physical_plan_ctx()->get_phy_plan()->get_min_cluster_version();
    bool enable_audit = GCONF.enable_sql_audit && ctx_.get_my_session()->get_local_ob_enable_sql_audit();
    metric_.init(enable_audit);
    common::ObIArray<dtl::ObDtlChannel*> &channels = task_channels_;
    loop_.set_tenant_id(ctx_.get_my_session()->get_effective_tenant_id());
    loop_.register_processor(dfc_unblock_msg_proc_)
        .register_interrupt_processor(interrupt_proc_);
    loop_.set_process_query_time(ctx_.get_my_session()->get_process_query_time());
    loop_.set_query_timeout_ts(ctx_.get_physical_plan_ctx()->get_timeout_timestamp());
    bool use_interm_result = false;
    int64_t px_batch_id = ctx_.get_px_batch_id();
    ObPxSQCProxy *sqc_proxy = NULL;
    if (OB_ISNULL(sqc_proxy = reinterpret_cast<ObPxSQCProxy *>(
        trans_input.get_ch_provider_ptr()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get ch provider ptr", K(ret));
    } else {
      use_interm_result = sqc_proxy->get_transmit_use_interm_result();
      if (!need_wait_sync_msg(*sqc_proxy)) {
        receive_channel_ready_ = true;
      }
    }
    loop_.set_interm_result(use_interm_result);
    int64_t thread_id = GETTID();

    ObPxSqcHandler *handler = ctx_.get_sqc_handler();
    bool should_reg_dm = use_interm_result && OB_NOT_NULL(handler) && handler->get_phy_plan().is_enable_px_fast_reclaim();
    common::ObRegisterDmInfo register_dm_info;
    if (should_reg_dm) {
      ObDetectManagerUtils::prepare_register_dm_info(register_dm_info, handler);
    }

    ARRAY_FOREACH_X(channels, idx, cnt, OB_SUCC(ret)) {
      dtl::ObDtlBasicChannel *ch = static_cast<dtl::ObDtlBasicChannel *> (channels.at(idx));
      if (OB_ISNULL(ch)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ch), K(ret));
      } else {
        ch->set_audit(enable_audit);
        ch->set_interm_result(use_interm_result);
        // if use_interm_result, set register_dm_info in dtl channel
        // so that the peer rpc processor can use this information to register check item into dm
        if (should_reg_dm) {
          ch->set_register_dm_info(register_dm_info);
        }
        ch->set_enable_channel_sync(true);
        ch->set_send_by_tenant(min_cluster_version >= CLUSTER_VERSION_4_3_5_0);
        ch->set_batch_id(px_batch_id);
        ch->set_compression_type(dfc_.get_compressor_type());
        ch->set_operator_owner();
        ch->set_thread_id(thread_id);
        ch->set_row_meta(params_.meta_);
      }
      LOG_TRACE("Transmit channel", K(ch), KP(ch->get_id()), K(ch->get_peer()));
    }
    LOG_TRACE("Get transmit channel ok",
              "task_id", trans_input.get_task_id(),
              "ch_cnt", channels.count(),
              K(ret));
    LOG_TRACE("TIMERECORD ", "reserve:=1 name:=TASK dfoid:", trans_input.get_dfo_id(),
      "sqcid:", trans_input.get_sqc_id(),
      "taskid:", trans_input.get_task_id(),
      "end:", ObTimeUtility::current_time());
  }
  return ret;
}

int ObPxTransmitOp::init_channels_cur_block(common::ObIArray<dtl::ObDtlChannel*> &dtl_chs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ch_blocks_.reserve(dtl_chs.count()))) {
    LOG_WARN("fail reserve channel blocks failed", K(ret), K(dtl_chs.count()));
  } else if (OB_FAIL(blk_bufs_.prepare_allocate(dtl_chs.count()))) {
    LOG_WARN("fail reserve channel blocks failed", K(ret), K(dtl_chs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dtl_chs.count(); i++) {
    if (OB_FAIL(ch_blocks_.push_back(NULL))) {
      LOG_WARN("fail to push back", K(ret), K(i));
    } else {
      static_cast<dtl::ObDtlBasicChannel *>(dtl_chs.at(i))
        ->get_datum_writer()
        .set_register_block_buf_ptr(&blk_bufs_.at(i));
      static_cast<dtl::ObDtlBasicChannel *>(dtl_chs.at(i))
        ->get_datum_writer()
        .set_register_block_ptr(&ch_blocks_.at(i));
    }
  }

  return ret;
}

int ObPxTransmitOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL(child_->get_next_row()) && OB_ITER_END != ret) {
    LOG_WARN("get next row from child failed", K(ret));
  } else if (NULL != MY_SPEC.random_expr_) {
    MY_SPEC.random_expr_->locate_datum_for_write(eval_ctx_).set_int(get_random_seq());
  }
  return ret;
}

int ObPxTransmitOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  return get_spec().use_rich_format_ ? next_vector(max_row_cnt) : next_batch(max_row_cnt);
}

int ObPxTransmitOp::next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const ObBatchRows *brs = NULL;
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, brs))) {
    LOG_WARN("get next batch row failed", K(ret));
  } else if (OB_FAIL(brs_.copy(brs))) {
    LOG_WARN("copy batch result failed", K(ret));
  } else if (NULL != MY_SPEC.random_expr_ && brs->size_ > 0) {
    ObDatum *datums = MY_SPEC.random_expr_->locate_datums_for_update(eval_ctx_, brs->size_);
    for (int64_t i = 0; i < brs->size_; i++) {
      datums[i].set_int(get_random_seq());
    }
  }
  return ret;
}

int ObPxTransmitOp::next_vector(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  const ObBatchRows *brs = NULL;
  if (OB_FAIL(child_->get_next_batch(max_row_cnt, brs))) {
    LOG_WARN("get next batch row failed", K(ret));
  } else if (OB_FAIL(brs_.copy(brs))) {
    LOG_WARN("copy batch result failed", K(ret));
  } else if (NULL != MY_SPEC.random_expr_ && brs->size_ > 0) {
    //ObDatum *datums = MY_SPEC.random_expr_->locate_datums_for_update(eval_ctx_, brs->size_);
    if (OB_FAIL(MY_SPEC.random_expr_->init_vector(eval_ctx_, VEC_FIXED, brs->size_))) {
      LOG_WARN("init vector failed", K(ret));
    } else {
      int64_t *value = MY_SPEC.random_expr_->get_fixed_vector_data<int64_t>(eval_ctx_);
      for (int64_t i = 0; i < brs->size_; i++) {
        value[i] = get_random_seq();
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::inner_close()
{
  int ret = OB_SUCCESS;
  /* we must release channel even if there is some error happen before */
  if (OB_FAIL(chs_agent_.destroy())) {
    LOG_WARN("failed to destroy ch agent", K(ret));
  }
  ObDtlBasicChannel *ch = nullptr;
  int64_t recv_cnt = 0;
  for (int i = 0; i < task_channels_.count(); ++i) {
    ch = static_cast<ObDtlBasicChannel *>(task_channels_.at(i));
    recv_cnt += ch->get_send_buffer_cnt();
  }
  op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::DTL_SEND_RECV_COUNT;
  op_monitor_info_.otherstat_3_value_ = recv_cnt;
  int release_channel_ret = loop_.unregister_all_channel();
  if (release_channel_ret != common::OB_SUCCESS) {
    // the following unlink actions is not safe is any unregister failure happened
    LOG_ERROR("fail unregister all channel from msg_loop", KR(release_channel_ret));
  }

  release_channel_ret = ObPxChannelUtil::unlink_ch_set(task_ch_set_, &dfc_, true);
  if (release_channel_ret != common::OB_SUCCESS) {
    LOG_WARN("release dtl channel failed", K(release_channel_ret));
  }
  // 注意：不能再 inner_open 中调用 flush rows，因为它会阻塞 inner_open 执行完成
  // 最好不要在inner_close中flush data，这会导致出错情况下，也send数据，应该send_rows中send_eof_row直接flush掉数据
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = ObTransmitOp::inner_close())) {
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
    LOG_WARN("fail close op", K(ret));
  }
  return ret;
}

int ObPxTransmitOp::set_wf_hybrid_slice_id_calc_type(ObSliceIdxCalc &slice_calc)
{
  int ret = OB_SUCCESS;

  const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
  if (spec.is_wf_hybrid_) {
    int64_t aggr_status = INT64_MIN;
    if (spec.use_rich_format_) {
      VectorFormat fmt = MY_SPEC.wf_hybrid_aggr_status_expr_->get_format(eval_ctx_);
      if (OB_UNLIKELY(VEC_FIXED != fmt && fmt != VEC_UNIFORM)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected format", K(ret), K(fmt));
      } else if (OB_LIKELY(fmt == VEC_FIXED)) {
        ObIVector *data = MY_SPEC.wf_hybrid_aggr_status_expr_->get_vector(eval_ctx_);
        int64_t batch_idx = eval_ctx_.get_batch_idx();
        aggr_status = *reinterpret_cast<const int64_t *>(
          static_cast<ObFixedLengthFormat<int64_t> *>(data)->get_payload(batch_idx));
      } else {
        ObIVector *data = MY_SPEC.wf_hybrid_aggr_status_expr_->get_vector(eval_ctx_);
        int64_t batch_idx = eval_ctx_.get_batch_idx();
        aggr_status = *reinterpret_cast<const int64_t *>(
          static_cast<ObUniformFormat<false> *>(data)->get_payload(batch_idx));
      }
    } else {
      ObDatum &wf_hybrid_aggr_status =
        MY_SPEC.wf_hybrid_aggr_status_expr_->locate_expr_datum(eval_ctx_);
      if (OB_ISNULL(wf_hybrid_aggr_status.ptr().int_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("wf_hybrid_aggr_status_expr_expr_ is null ptr", K(ret));
      } else {
        aggr_status = wf_hybrid_aggr_status.get_int();
      }
    }
    if (OB_SUCC(ret)) {
      ObWfHybridDistSliceIdCalc &wf_hybrid_slice_calc =
          static_cast< ObWfHybridDistSliceIdCalc &>(slice_calc);
      // distribute method is calculate by aggr_status
      if (aggr_status > 0 && aggr_status > spec.wf_hybrid_pby_exprs_cnt_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr_status > spec.wf_hybrid_pby_exprs_cnt_array_.count()"
            , K(ret), K(aggr_status), K(spec.wf_hybrid_pby_exprs_cnt_array_.count()));
      } else if (0 > aggr_status) {
        wf_hybrid_slice_calc.set_slice_id_calc_type(
            ObWfHybridDistSliceIdCalc::SliceIdCalcType::BROADCAST);
      } else if (0 == aggr_status) {
        wf_hybrid_slice_calc.set_slice_id_calc_type(
            ObWfHybridDistSliceIdCalc::SliceIdCalcType::RANDOM);
      } else {
        // n_keys is calculate by aggr_status
        wf_hybrid_slice_calc.set_slice_id_calc_type(
            ObWfHybridDistSliceIdCalc::SliceIdCalcType::HASH);
        int64_t n_keys = spec.wf_hybrid_pby_exprs_cnt_array_.at(aggr_status - 1);
        wf_hybrid_slice_calc.set_calc_hash_keys(n_keys);
      }
    }
  }
  return ret;
}

void ObPxTransmitOp::set_wf_hybrid_exprs(ObSliceIdxCalc &slice_calc)
{
  const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
  if (spec.is_wf_hybrid_) {
    ObWfHybridDistSliceIdCalc &wf_hybrid_slice_calc =
        static_cast< ObWfHybridDistSliceIdCalc &>(slice_calc);
    wf_hybrid_slice_calc.set_wf_hybrid_aggr_status_expr(spec.wf_hybrid_aggr_status_expr_);
    wf_hybrid_slice_calc.set_wf_hybrid_pby_exprs_cnt_array(&spec.wf_hybrid_pby_exprs_cnt_array_);
  }
}

int ObPxTransmitOp::set_rollup_hybrid_keys(ObSliceIdxCalc &slice_calc)
{
  int ret = OB_SUCCESS;
  const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
  if (spec.is_rollup_hybrid_ && !has_set_hybrid_key_) {
    ObOperator *child = get_child(0);
    // codegen has already check
    while (OB_NOT_NULL(child) && OB_SUCC(ret)) {
      if (ObPhyOperatorType::PHY_MERGE_GROUP_BY == child->get_spec().type_) {
        ObMergeGroupByOp *merge_groupby = static_cast<ObMergeGroupByOp *>(child);
        int64_t n_keys = 0;
        if (OB_FAIL(merge_groupby->get_n_shuffle_keys_for_exchange(n_keys))) {
          LOG_WARN("failed to get shuffle keys for exchange", K(ret));
        } else {
          slice_calc.set_calc_hash_keys(n_keys);
        }
        break;
      } else if (ObPhyOperatorType::PHY_VEC_MERGE_GROUP_BY == child->get_spec().type_) {
        ObMergeGroupByVecOp *merge_groupby = static_cast<ObMergeGroupByVecOp *>(child);
        int64_t n_keys = 0;
        if (OB_FAIL(merge_groupby->get_n_shuffle_keys_for_exchange(n_keys))) {
          LOG_WARN("failed to get shuffle keys for exchange", K(ret));
        } else {
          slice_calc.set_calc_hash_keys(n_keys);
        }
        break;
      } else {
        child = child->get_child(0);
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: child is null", K(ret));
    }
    has_set_hybrid_key_ = true;
  }
  return ret;
}

void ObPxTransmitOp::fill_batch_ptrs(const int64_t *indexes)
{
  for (int64_t i = 0; i < brs_.size_; ++i) {
    if (brs_.skip_->at(i)) {
      continue;
    }
    if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == indexes[i]) {
      op_monitor_info_.otherstat_1_value_++;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
    } else {
      int64_t slice_idx = indexes[i];
      const int64_t row_size = params_.row_size_array_[i];
      const int64_t head_pos = params_.heads_[slice_idx];
      ObTempRowStore::DtlRowBlock *block = params_.blocks_[slice_idx];
      if (nullptr == block
          || !params_.channel_unobstructeds_[slice_idx]
          || row_size > params_.tails_[slice_idx] - head_pos) {
        params_.fallback_array_[params_.fallback_cnt_++] = i;
        params_.channel_unobstructeds_[slice_idx] = false;
      } else {
        ObCompactRow *ptr = reinterpret_cast<ObCompactRow *> (reinterpret_cast<char *> (block) + head_pos);
        params_.return_rows_[params_.selector_cnt_] = ptr;
        params_.heads_[slice_idx] += row_size;
        const static int64_t MEMSET_SIZE = 128;
        while (params_.heads_[slice_idx] > params_.init_pos_[slice_idx]) {
          if (params_.init_pos_[slice_idx] + MEMSET_SIZE < params_.tails_[slice_idx]) {
            memset(reinterpret_cast<char *> (block) + params_.init_pos_[slice_idx], 0, MEMSET_SIZE);
            params_.init_pos_[slice_idx] += MEMSET_SIZE;
          } else {
            memset(ptr, 0, row_size);
            params_.init_pos_[slice_idx] = params_.heads_[slice_idx];
          }
        }
        block->cnt_ += 1;
        params_.selector_array_[params_.selector_cnt_++] = i;
      }
    }
  }
}

void ObPxTransmitOp::fill_batch_ptrs_fixed(const int64_t *indexes)
{
  for (int64_t i = 0; i < brs_.size_; ++i) {
    if (brs_.skip_->at(i)) {
      continue;
    }
    if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == indexes[i]) {
      op_monitor_info_.otherstat_1_value_++;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
    } else {
      int64_t slice_idx = indexes[i];
      char *header = params_.fixed_payload_headers_[slice_idx];
      if (nullptr == header
          || params_.row_cnts_[slice_idx] >= params_.row_limit_) {
        params_.fallback_array_[params_.fallback_cnt_++] = i;
      } else {
        params_.fixed_rows_[params_.selector_cnt_] = header;
        params_.row_idx_[params_.selector_cnt_] = params_.row_cnts_[slice_idx];
        params_.row_cnts_[slice_idx] += 1;
        params_.selector_array_[params_.selector_cnt_++] = i;
      }
    }
  }
  for (int64_t col_idx = 0; col_idx < get_spec().output_.count(); ++col_idx) {
    int64_t fixed_len = params_.column_lens_[col_idx];
    switch (params_.vectors_.at(col_idx)->get_format()) {
      case VEC_FIXED : {
        const char *payload = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_data();
        ObBitVector *nulls = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_nulls();
        if (!params_.vectors_.at(col_idx)->has_null()) {
          if (8 == fixed_len) {
            for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * 8,
                    payload + fixed_len * params_.selector_array_[i], 8);
            }
          } else {
            for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                    payload + fixed_len * params_.selector_array_[i], fixed_len);
            }
          }
        } else {
          ObBitVector *nulls = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_nulls();
          for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
            if (nulls->at(params_.selector_array_[i])) {
              ObDtlBasicChannel *channel =
                        static_cast<ObDtlBasicChannel *> (task_channels_.at(indexes[params_.selector_array_[i]]));
              ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
              row_writer.set_null(col_idx, params_.row_idx_[i]);
            } else {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                  payload + fixed_len * params_.selector_array_[i], fixed_len);
            }
          }
        }
        break;
      }
      case VEC_UNIFORM : {
        ObDatum *datums = (static_cast<ObUniformBase *> (params_.vectors_.at(col_idx)))->get_datums();
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          if (datums[params_.selector_array_[i]].is_null()) {
            ObDtlBasicChannel *channel =
                      static_cast<ObDtlBasicChannel *> (task_channels_.at(indexes[params_.selector_array_[i]]));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            row_writer.set_null(col_idx, params_.row_idx_[i]);
          } else {
            memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                datums[params_.selector_array_[i]].ptr_, fixed_len);
          }
        }
        break;
      }
      default : {
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          if (params_.vectors_.at(col_idx)->is_null(params_.selector_array_[i])) {
            ObDtlBasicChannel *channel =
                      static_cast<ObDtlBasicChannel *> (task_channels_.at(indexes[params_.selector_array_[i]]));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            row_writer.set_null(col_idx, params_.row_idx_[i]);
          } else {
            memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * params_.column_lens_[col_idx],
                  params_.vectors_.at(col_idx)->get_payload(params_.selector_array_[i]), params_.column_lens_[col_idx]);
          }
        }
      }
    }
  }
}

void ObPxTransmitOp::fill_batch_ptrs_fixed(ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                             ObSliceIdxCalc::EndIdxArray &end_idx_array)
{
  int64_t start_idx = 0;
  int64_t end_idx;
  for (int64_t i = 0; i < brs_.size_; ++i) {
    end_idx = end_idx_array.at(i);
    if (!brs_.skip_->at(i)) {
      for (int64_t j = start_idx; j < end_idx; ++j) {
        int64_t slice_idx = slice_idx_flatten_array.at(j);
        if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == slice_idx) {
          op_monitor_info_.otherstat_1_value_++;
          op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
        } else {
          char *header = params_.fixed_payload_headers_[slice_idx];
          if (nullptr == header
              || params_.row_cnts_[slice_idx] >= params_.row_limit_) {
            // 使用slice_idx_flatten_array本身进行覆盖，之后该值表示了需要fallback的行的slice_idx
            slice_idx_flatten_array[params_.fallback_cnt_++] = slice_idx;
          } else {
            params_.fixed_rows_[params_.selector_cnt_] = header;
            params_.row_idx_[params_.selector_cnt_] = params_.row_cnts_[slice_idx];
            params_.row_cnts_[slice_idx] += 1;
            params_.selector_slice_idx_array_[params_.selector_cnt_] = slice_idx;
            params_.selector_array_[params_.selector_cnt_++] = i;
          }
        }
      }
    }
    start_idx = end_idx;
    // 使用fallback_array_来作为后面每行进行fallbacl时结尾的下标数组，和slice_idx_flatten_array一块进行fallback操作
    params_.fallback_array_[i] = params_.fallback_cnt_;
  }
  for (int64_t col_idx = 0; col_idx < get_spec().output_.count(); ++col_idx) {
    int64_t fixed_len = params_.column_lens_[col_idx];
    switch (params_.vectors_.at(col_idx)->get_format()) {
      case VEC_FIXED : {
        const char *payload = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_data();
        ObBitVector *nulls = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_nulls();
        if (!params_.vectors_.at(col_idx)->has_null()) {
          if (8 == fixed_len) {
            for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * 8,
                    payload + fixed_len * params_.selector_array_[i], 8);
            }
          } else {
            for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                    payload + fixed_len * params_.selector_array_[i], fixed_len);
            }
          }
        } else {
          ObBitVector *nulls = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_nulls();
          for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
            if (nulls->at(params_.selector_array_[i])) {
              ObDtlBasicChannel *channel =
                        static_cast<ObDtlBasicChannel *> (task_channels_.at(params_.selector_slice_idx_array_[i]));
              ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
              row_writer.set_null(col_idx, params_.row_idx_[i]);
            } else {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                  payload + fixed_len * params_.selector_array_[i], fixed_len);
            }
          }
        }
        break;
      }
      case VEC_UNIFORM : {
        ObDatum *datums = (static_cast<ObUniformBase *> (params_.vectors_.at(col_idx)))->get_datums();
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          if (datums[params_.selector_array_[i]].is_null()) {
            ObDtlBasicChannel *channel =
                      static_cast<ObDtlBasicChannel *> (task_channels_.at(params_.selector_slice_idx_array_[i]));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            row_writer.set_null(col_idx, params_.row_idx_[i]);
          } else {
            memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                datums[params_.selector_array_[i]].ptr_, fixed_len);
          }
        }
        break;
      }
      default : {
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          if (params_.vectors_.at(col_idx)->is_null(params_.selector_array_[i])) {
            ObDtlBasicChannel *channel =
                      static_cast<ObDtlBasicChannel *> (task_channels_.at(params_.selector_slice_idx_array_[i]));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            row_writer.set_null(col_idx, params_.row_idx_[i]);
          } else {
            memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * params_.column_lens_[col_idx],
                  params_.vectors_.at(col_idx)->get_payload(params_.selector_array_[i]), params_.column_lens_[col_idx]);
          }
        }
      }
    }
  }
}

void ObPxTransmitOp::fill_broad_cast_ptrs_fixed(int64_t slice_idx)
{
  for (int64_t i = 0; i < brs_.size_; ++i) {
    if (brs_.skip_->at(i)) {
      continue;
    }
    char *header = params_.fixed_payload_headers_[slice_idx];
    if (nullptr == header
        || params_.row_cnts_[slice_idx] >= params_.row_limit_) {
      params_.fallback_array_[params_.fallback_cnt_++] = i;
    } else {
      params_.fixed_rows_[params_.selector_cnt_] = header;
      params_.row_idx_[params_.selector_cnt_] = params_.row_cnts_[slice_idx];
      params_.row_cnts_[slice_idx] += 1;
      params_.selector_array_[params_.selector_cnt_++] = i;
    }
  }
  for (int64_t col_idx = 0; col_idx < get_spec().output_.count(); ++col_idx) {
    int64_t fixed_len = params_.column_lens_[col_idx];
    switch (params_.vectors_.at(col_idx)->get_format()) {
      case VEC_FIXED : {
        const char *payload = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_data();
        ObBitVector *nulls = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_nulls();
        if (!params_.vectors_.at(col_idx)->has_null()) {
          if (8 == fixed_len) {
            for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * 8,
                    payload + fixed_len * params_.selector_array_[i], 8);
            }
          } else {
            for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                    payload + fixed_len * params_.selector_array_[i], fixed_len);
            }
          }
        } else {
          ObBitVector *nulls = (static_cast<ObFixedLengthBase *> (params_.vectors_.at(col_idx)))->get_nulls();
          for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
            if (nulls->at(params_.selector_array_[i])) {
              ObDtlBasicChannel *channel =
                        static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
              ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
              row_writer.set_null(col_idx, params_.row_idx_[i]);
            } else {
              memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                  payload + fixed_len * params_.selector_array_[i], fixed_len);
            }
          }
        }
        break;
      }
      case VEC_UNIFORM : {
        ObDatum *datums = (static_cast<ObUniformBase *> (params_.vectors_.at(col_idx)))->get_datums();
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          if (datums[params_.selector_array_[i]].is_null()) {
            ObDtlBasicChannel *channel =
                      static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            row_writer.set_null(col_idx, params_.row_idx_[i]);
          } else {
            memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * fixed_len,
                datums[params_.selector_array_[i]].ptr_, fixed_len);
          }
        }
        break;
      }
      default : {
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          if (params_.vectors_.at(col_idx)->is_null(params_.selector_array_[i])) {
            ObDtlBasicChannel *channel =
                      static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            row_writer.set_null(col_idx, params_.row_idx_[i]);
          } else {
            memcpy(params_.fixed_rows_[i] + params_.column_offsets_[col_idx] + params_.row_idx_[i] * params_.column_lens_[col_idx],
                  params_.vectors_.at(col_idx)->get_payload(params_.selector_array_[i]), params_.column_lens_[col_idx]);
          }
        }
      }
    }
  }
}

int ObPxTransmitOp::prepare_for_nested_expr()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_spec().output_.count(); ++i) {
    ObExpr *expr = get_spec().output_.at(i);
    if (expr->is_nested_expr() && !is_uniform_format(expr->get_format(eval_ctx_))) {
      if (OB_FAIL(expr->nested_cast_to_uniform(brs_.size_, eval_ctx_, brs_.skip_))) {
        LOG_WARN("failed to cast nested expr to uniform", K(ret));
      } else if (!params_.vectors_.empty()) { // params_.vectors_ might be not used
        params_.vectors_.at(i) = expr->get_vector(eval_ctx_);
      }
    }
  }
  return ret;
}

void ObPxTransmitOp::fill_batch_ptrs(ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                                     ObSliceIdxCalc::EndIdxArray &end_idx_array)
{
  for (int64_t i = 0; i < brs_.size_; ++i) {
    if (!brs_.skip_->at(i)) {
      int64_t start_idx = i == 0 ? 0 : end_idx_array.at(i-1);
      int64_t end_idx = end_idx_array.at(i);
      const int64_t row_size = params_.row_size_array_[i];
      for (int64_t j = start_idx; j < end_idx; ++j) {
        int64_t slice_idx = slice_idx_flatten_array.at(j);
        if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == slice_idx) {
          op_monitor_info_.otherstat_1_value_++;
          op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
        } else {
          const int64_t head_pos = params_.heads_[slice_idx];
          ObTempRowStore::DtlRowBlock *block = params_.blocks_[slice_idx];
          if (nullptr == block
              || !params_.channel_unobstructeds_[slice_idx]
              || row_size > params_.tails_[slice_idx] - head_pos) {
            // 使用slice_idx_flatten_array本身进行覆盖
            slice_idx_flatten_array[params_.fallback_cnt_++] = slice_idx;
            params_.channel_unobstructeds_[slice_idx] = false;
          } else {
            ObCompactRow *ptr = reinterpret_cast<ObCompactRow *> (reinterpret_cast<char *> (block) + head_pos);
            params_.return_rows_[params_.selector_cnt_] = ptr;
            params_.heads_[slice_idx] += row_size;
            const static int64_t MEMSET_SIZE = 128;
            while (params_.heads_[slice_idx] > params_.init_pos_[slice_idx]) {
              if (params_.init_pos_[slice_idx] + MEMSET_SIZE < params_.tails_[slice_idx]) {
                memset(reinterpret_cast<char *> (block) + params_.init_pos_[slice_idx], 0, MEMSET_SIZE);
                params_.init_pos_[slice_idx] += MEMSET_SIZE;
              } else {
                memset(ptr, 0, row_size);
                params_.init_pos_[slice_idx] = params_.heads_[slice_idx];
              }
            }
            block->cnt_ += 1;
            params_.selector_array_[params_.selector_cnt_++] = i;
          }
        }
      }
    }
    // 使用fallback_array_来作为后面每行进行fallbacl时结尾的下标数组，和slice_idx_flatten_array一块进行fallback操作
    params_.fallback_array_[i] = params_.fallback_cnt_;
  }
}

void ObPxTransmitOp::fill_broad_cast_ptrs(int64_t slice_idx)
{
  // 固定slice_idx，将所有行添加进来
  int64_t &head_pos = params_.heads_[slice_idx];
  ObTempRowStore::DtlRowBlock *block = params_.blocks_[slice_idx];
  for (int64_t i = 0; i < brs_.size_; ++i) {
    if (brs_.skip_->at(i)) {
      continue;
    }
    const int64_t row_size = params_.row_size_array_[i];
    if (nullptr == block
        || !params_.channel_unobstructeds_[slice_idx]
        || row_size > params_.tails_[slice_idx] - head_pos) {
      params_.fallback_array_[params_.fallback_cnt_++] = i;
      params_.channel_unobstructeds_[slice_idx] = false;
    } else {
      ObCompactRow *ptr = reinterpret_cast<ObCompactRow *> (reinterpret_cast<char *> (block) + head_pos);
      params_.return_rows_[params_.selector_cnt_] = ptr;
      head_pos += row_size;
      const static int64_t MEMSET_SIZE = 128;
      while (head_pos > params_.init_pos_[slice_idx]) {
        if (params_.init_pos_[slice_idx] + MEMSET_SIZE < params_.tails_[slice_idx]) {
          memset(reinterpret_cast<char *> (block) + params_.init_pos_[slice_idx], 0, MEMSET_SIZE);
          params_.init_pos_[slice_idx] += MEMSET_SIZE;
        } else {
          memset(ptr, 0, row_size);
          params_.init_pos_[slice_idx] = head_pos;
        }
      }
      block->cnt_ += 1;
      params_.selector_array_[params_.selector_cnt_++] = i;
    }
  }
}

int ObPxTransmitOp::try_extend_selector_array(int64_t target_count, bool is_fixed)
{
  int ret = OB_SUCCESS;
  if (params_.selector_array_max_size_ < target_count) {
    // calc size
    while(params_.selector_array_max_size_ < target_count) {
      params_.selector_array_max_size_ *= 2;
    }
    if (is_fixed) {
      // extend selector_array, fixed_rows_, row_idx_, selector_slice_idx_array_
      params_.selector_array_ = NULL;
      params_.fixed_rows_ = NULL;
      params_.row_idx_ = NULL;
      params_.selector_slice_idx_array_ = NULL;
      if (OB_ISNULL(params_.selector_array_ = (uint16_t *)
            ctx_.get_allocator().alloc(sizeof(uint16_t) * params_.selector_array_max_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc params_.selector_array_", K(ret), K(params_.selector_array_max_size_));
      } else if (OB_ISNULL(params_.fixed_rows_ = static_cast<char **>
                (ctx_.get_allocator().alloc(params_.selector_array_max_size_ * sizeof(char *))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc params_.fixed_rows_", K(ret), K(params_.selector_array_max_size_));
      } else if (OB_ISNULL(params_.row_idx_ = static_cast<int64_t *>
                (ctx_.get_allocator().alloc(params_.selector_array_max_size_ * sizeof(int64_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc params_.row_idx_", K(ret), K(params_.selector_array_max_size_));
      } else if (OB_ISNULL(params_.selector_slice_idx_array_ = (uint16_t *)
            ctx_.get_allocator().alloc(sizeof(uint16_t) * params_.selector_array_max_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc params_.selector_slice_idx_array_", K(ret), K(params_.selector_array_max_size_));
      }
    } else {
      // extend selector_array, return_rows_
      params_.selector_array_ = NULL;
      params_.return_rows_ = NULL;
      if (OB_ISNULL(params_.selector_array_ = (uint16_t *)
            ctx_.get_allocator().alloc(sizeof(uint16_t) * params_.selector_array_max_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc params_.selector_array_", K(ret), K(params_.selector_array_max_size_));
      } else if (OB_ISNULL(params_.return_rows_ = (ObCompactRow **)
            ctx_.get_allocator().alloc(sizeof(ObCompactRow *) * params_.selector_array_max_size_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc params_.return_rows_", K(ret), K(params_.selector_array_max_size_));
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::keep_order_send_batch(ObEvalCtx::BatchInfoScopeGuard &batch_info_guard,
                            ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                            ObSliceIdxCalc::EndIdxArray &end_idx_array,
                            bool is_broad_cast_calc_type)
{
  int ret = OB_SUCCESS;
  int64_t send_row_time_recorder = 0;
  ObObj tablet_id; //not used
  params_.fallback_cnt_ = 0;
  params_.selector_cnt_ = 0;
  if (params_.vectors_.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_spec().output_.count(); ++i) {
      OZ (params_.vectors_.push_back(get_spec().output_.at(i)->get_vector(eval_ctx_)));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_for_nested_expr())) {
    LOG_WARN("failed to prepare for nested expr", K(ret));
  } else if (OB_FAIL(ObTempRowStore::DtlRowBlock::calc_rows_size(params_.vectors_, params_.meta_,
                                                    brs_, params_.row_size_array_))) {
    LOG_WARN("failed to calc size", K(ret));
  } else {
    if (is_broad_cast_calc_type) {
      // 对于broad_cast对每一channel都进行一次计算，将所有的行填充到该channel中去,可以直接复用selector_array_
      for(int j = 0; OB_SUCC(ret) && j < slice_idx_flatten_array.count(); j++) {
        int64_t slice_idx = slice_idx_flatten_array[j];
        params_.selector_cnt_ = 0;
        params_.fallback_cnt_ = 0;
        fill_broad_cast_ptrs(slice_idx);
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          params_.return_rows_[i]->set_row_size(params_.row_size_array_[params_.selector_array_[i]]);
        }
        for (int64_t idx = 0; idx < get_spec().output_.count(); ++idx) {
          params_.vectors_.at(idx)->to_rows(params_.meta_, params_.return_rows_,
                                params_.selector_array_, params_.selector_cnt_, idx);
        }
        for (int64_t idx = 0; idx < task_channels_.count(); ++idx) {
          if (nullptr != params_.blocks_[idx]) {
            params_.blocks_[idx]->get_buffer()->fast_update_head(params_.heads_[idx]);
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < params_.fallback_cnt_; i++) {
          batch_info_guard.set_batch_idx(params_.fallback_array_[i]);
          metric_.count();
          ObDtlBasicChannel *channel =
                          static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
          ObDtlVectorRowMsgWriter &row_writer = channel->get_vector_row_writer();
          if (nullptr != row_writer.get_write_buffer()) {
            row_writer.get_write_buffer()->pos() = row_writer.used();
          }
          if (OB_FAIL(send_row(slice_idx, send_row_time_recorder, tablet_id.get_int(),
              params_.fallback_array_[i]))) {
            LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx));
          } else {
            params_.blocks_[slice_idx] = static_cast<ObDtlBasicChannel *>
              (task_channels_.at(slice_idx))->get_vector_row_writer().get_block();
            if (nullptr != params_.blocks_[slice_idx]) {
              params_.heads_[slice_idx] = params_.blocks_[slice_idx]->get_buffer()->head_pos();
              params_.tails_[slice_idx] = params_.blocks_[slice_idx]->get_buffer()->tail_pos();
              params_.init_pos_[slice_idx] = params_.heads_[slice_idx];
              params_.channel_unobstructeds_[slice_idx] = true;
            }
          }
        }
      }
    } else {
      if (OB_FAIL(try_extend_selector_array(slice_idx_flatten_array.count(), false))) {
        LOG_WARN("failed to try_extend_selector_array", K(ret));
      } else {
        fill_batch_ptrs(slice_idx_flatten_array, end_idx_array);
        for (int64_t i = 0; i < params_.selector_cnt_; ++i) {
          params_.return_rows_[i]->set_row_size(params_.row_size_array_[params_.selector_array_[i]]);
        }
        for (int64_t idx = 0; idx < get_spec().output_.count(); ++idx) {
          params_.vectors_.at(idx)->to_rows(params_.meta_, params_.return_rows_,
                                params_.selector_array_, params_.selector_cnt_, idx);
        }
        for (int64_t idx = 0; idx < task_channels_.count(); ++idx) {
          if (nullptr != params_.blocks_[idx]) {
            params_.blocks_[idx]->get_buffer()->fast_update_head(params_.heads_[idx]);
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_ && params_.fallback_cnt_ > 0; i++) {
          batch_info_guard.set_batch_idx(i);
          metric_.count();
          int64_t start_idx = i == 0 ? 0 : params_.fallback_array_[i-1];
          int64_t end_idx = params_.fallback_array_[i];
          for (int64_t j = start_idx; OB_SUCC(ret) && j < end_idx && params_.fallback_cnt_ > 0;
                j++, params_.fallback_cnt_--) {
            int64_t slice_idx = slice_idx_flatten_array[j];
            ObDtlBasicChannel *channel =
                            static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
            ObDtlVectorRowMsgWriter &row_writer = channel->get_vector_row_writer();
            if (nullptr != row_writer.get_write_buffer()) {
              row_writer.get_write_buffer()->pos() = row_writer.used();
            }
            if (OB_FAIL(send_row(slice_idx, send_row_time_recorder, tablet_id.get_int(), i))) {
              LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx));
            } else {
              params_.blocks_[slice_idx] = static_cast<ObDtlBasicChannel *>
                (task_channels_.at(slice_idx))->get_vector_row_writer().get_block();
              if (nullptr != params_.blocks_[slice_idx]) {
                params_.heads_[slice_idx] = params_.blocks_[slice_idx]->get_buffer()->head_pos();
                params_.tails_[slice_idx] = params_.blocks_[slice_idx]->get_buffer()->tail_pos();
                params_.init_pos_[slice_idx] = params_.heads_[slice_idx];
                params_.channel_unobstructeds_[slice_idx] = true;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::keep_order_send_batch_fixed(ObEvalCtx::BatchInfoScopeGuard &batch_info_guard,
                            ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                            ObSliceIdxCalc::EndIdxArray &end_idx_array,
                            bool is_broad_cast_calc_type)
{
  int ret = OB_SUCCESS;
  int64_t send_row_time_recorder = 0;
  ObObj tablet_id; //not used
  params_.fallback_cnt_ = 0;
  params_.selector_cnt_ = 0;
  if (params_.vectors_.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_spec().output_.count(); ++i) {
      OZ (params_.vectors_.push_back(get_spec().output_.at(i)->get_vector(eval_ctx_)));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    if (is_broad_cast_calc_type) {
      // 对于broad_cast对每一channel都进行一次计算，将所有的行填充到该channel中去,可以直接复用selector_array_
      for(int j = 0; OB_SUCC(ret) && j < slice_idx_flatten_array.count(); j++) {
        int64_t slice_idx = slice_idx_flatten_array[j];
        params_.selector_cnt_ = 0;
        params_.fallback_cnt_ = 0;
        fill_broad_cast_ptrs_fixed(slice_idx);
        for (int64_t i = 0; OB_SUCC(ret) && i < params_.fallback_cnt_; i++) {
          batch_info_guard.set_batch_idx(params_.fallback_array_[i]);
          metric_.count();
          ObDtlBasicChannel *channel =
                          static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
          ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
          if (row_writer.is_inited()) {
            row_writer.update_row_cnt(params_.row_cnts_[slice_idx]);
            row_writer.update_buffer_used();
          }
          if (OB_FAIL(send_row(slice_idx, send_row_time_recorder, tablet_id.get_int(),
              params_.fallback_array_[i]))) {
            LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx));
          } else if (static_cast<ObDtlBasicChannel *>
                    (task_channels_.at(slice_idx))->get_vector_fixed_msg_writer().is_inited()) {
            params_.fixed_payload_headers_[slice_idx] = static_cast<ObDtlBasicChannel *>
              (task_channels_.at(slice_idx))->get_vector_fixed_msg_writer().get_header(0);
            if (nullptr != params_.fixed_payload_headers_[slice_idx]) {
              if (!params_.offset_inited_) {
                for (int64_t i = 1; i < get_spec().output_.count(); ++i) {
                  params_.column_offsets_[i] = row_writer.get_header(i) - row_writer.get_header(0);
                }
                for (int64_t i = 0; i < get_spec().output_.count(); ++i) {
                  params_.column_lens_[i] = row_writer.get_fixed_len(i);
                }
                params_.offset_inited_ = true;
              }
              if (params_.row_limit_ < 0) {
                params_.row_limit_ = row_writer.get_row_limit();
              }
              params_.row_cnts_[slice_idx] = row_writer.get_row_cnt();
            }
          }
        }
      }
    } else {
      if (OB_FAIL(try_extend_selector_array(slice_idx_flatten_array.count(), true))) {
        LOG_WARN("failed to try_extend_selector_array", K(ret));
      } else {
        fill_batch_ptrs_fixed(slice_idx_flatten_array, end_idx_array);
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_ && params_.fallback_cnt_ > 0; i++) {
          batch_info_guard.set_batch_idx(i);
          metric_.count();
          int64_t start_idx = i == 0 ? 0 : params_.fallback_array_[i-1];
          int64_t end_idx = params_.fallback_array_[i];
          for (int64_t j = start_idx; OB_SUCC(ret) && j < end_idx && params_.fallback_cnt_ > 0;
              j++, params_.fallback_cnt_--) {
            int64_t slice_idx = slice_idx_flatten_array[j];
            ObDtlBasicChannel *channel =
                          static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            if (row_writer.is_inited()) {
              row_writer.update_row_cnt(params_.row_cnts_[slice_idx]);
              row_writer.update_buffer_used();
            }
            if (OB_FAIL(send_row(slice_idx, send_row_time_recorder, tablet_id.get_int(), i))) {
              LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx));
            } else if (static_cast<ObDtlBasicChannel *>
                (task_channels_.at(slice_idx))->get_vector_fixed_msg_writer().is_inited()) {
              params_.fixed_payload_headers_[slice_idx] = static_cast<ObDtlBasicChannel *>
                (task_channels_.at(slice_idx))->get_vector_fixed_msg_writer().get_header(0);
              if (nullptr != params_.fixed_payload_headers_[slice_idx]) {
                if (!params_.offset_inited_) {
                  for (int64_t k = 1; k < get_spec().output_.count(); ++k) {
                    params_.column_offsets_[k] = row_writer.get_header(k) - row_writer.get_header(0);
                  }
                  for (int64_t k = 0; k < get_spec().output_.count(); ++k) {
                    params_.column_lens_[k] = row_writer.get_fixed_len(k);
                  }
                  params_.offset_inited_ = true;
                }
                if (params_.row_limit_ < 0) {
                  params_.row_limit_ = row_writer.get_row_limit();
                }
                params_.row_cnts_[slice_idx] = row_writer.get_row_cnt();
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::send_eof_row()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  LOG_TRACE("Send eof row",
          "op_id", get_spec().id_,
          "ch_cnt", task_channels_.count(),
          K(ret));
  if (OB_ISNULL(ch_info_) ||
      ch_info_->receive_exec_server_.total_task_cnt_ != task_channels_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ch info is null", K(ret),
      KP(ch_info_), K(task_channels_.count()));
  } else {
    ObTransmitEofAsynSender eof_asyn_sender(task_channels_, ch_info_, true, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_, data_msg_type_);
    if (OB_FAIL(eof_asyn_sender.asyn_send())) {
      LOG_WARN("failed to asyn send drain", K(ret), K(lbt()));
    } else if (GCONF.enable_sql_audit) {
      op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::EXCHANGE_EOF_TIMESTAMP;
      op_monitor_info_.otherstat_2_value_ = oceanbase::common::ObClockGenerator::getClock();
      // It's the end time of sending all data
      // if not, the sql plan monitor can't show the end of eof
      op_monitor_info_.last_row_time_ = oceanbase::common::ObClockGenerator::getClock();
    }
  }
  return ret;
}

int ObPxTransmitOp::send_row(int64_t slice_idx,
                             int64_t &time_recorder,
                             int64_t tablet_id,
                             int64_t vector_row_idx)
{
  UNUSED(time_recorder);
  int ret = OB_SUCCESS;
  const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
  bool is_send_row_normal = false;
  if (OB_FAIL(try_wait_channel())) {
    LOG_WARN("failed to wait channel init", K(ret));
  } else {
    if (ObSliceIdxCalc::DEFAULT_CHANNEL_IDX_TO_DROP_ROW == slice_idx) {
      op_monitor_info_.otherstat_1_value_++;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::EXCHANGE_DROP_ROW_COUNT;
    // TODO: shanting2.0. use opt when DTL 2.0 implemented.
    } else if (!is_vectorized()) {
      is_send_row_normal = true;
    } else if (get_spec().use_rich_format_) {
      if (NULL != spec.tablet_id_expr_ && !has_set_tablet_id_vector_) {
        update_row(spec.tablet_id_expr_, tablet_id);
      }
      ObDtlBasicChannel *channel = static_cast<ObDtlBasicChannel *> (task_channels_.at(slice_idx));
      bool is_rpc_channel = channel->get_channel_type() == ObDtlChannel::DtlChannelType::RPC_CHANNEL;
      switch (data_msg_type_) {
        case dtl::ObDtlMsgType::PX_VECTOR_FIXED: {
          ObDtlVectorFixedMsgWriter &fixed_writer = channel->get_vector_fixed_msg_writer();
          if (!fixed_writer.is_inited()) {
            is_send_row_normal = true;
          } else if (OB_FAIL(fixed_writer.append_row(spec.output_, vector_row_idx, eval_ctx_))) {
            if (OB_BUF_NOT_ENOUGH != ret) {
              LOG_WARN("failed to append row", K(ret));
            } else {
              is_send_row_normal = true;
              fixed_writer.update_write_ret();
              ret = OB_SUCCESS;
            }
          } else {
            fixed_writer.update_buffer_used();
          }
          break;
        }
        case dtl::ObDtlMsgType::PX_VECTOR_ROW: {
          is_send_row_normal = true;
          break;
        }
        case dtl::ObDtlMsgType::PX_VECTOR: {
          if (is_rpc_channel) {
            is_send_row_normal = true;
          } else {
            ObDtlVectorRowMsgWriter &row_writer = channel->get_vector_row_writer();
            if (!row_writer.is_inited()) {
              is_send_row_normal = true;
            } else if (OB_FAIL(row_writer.try_append_row(spec.output_, eval_ctx_))) {
              if (OB_BUF_NOT_ENOUGH != ret) {
                LOG_WARN("failed to append row", K(ret));
              } else {
                is_send_row_normal = true;
                ret = OB_SUCCESS;
              }
            }
          }
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid msg type", K(ret), K(data_msg_type_));
      }
    } else {
      OB_ASSERT(slice_idx >= 0 && slice_idx < ch_blocks_.count());
      ObChunkDatumStore::BlockBufferWrap &blk_buf = blk_bufs_.at(slice_idx);
      if (!blk_buf.is_inited()) {
        is_send_row_normal = true;
      } else {
        if (NULL != spec.tablet_id_expr_) {
          update_row(spec.tablet_id_expr_, tablet_id);
        }
        if (OB_FAIL(blk_buf.append_row(spec.output_, &eval_ctx_, 0, vector_row_idx))) {
          if (OB_BUF_NOT_ENOUGH != ret) {
            SQL_DTL_LOG(WARN, "failed to add row", K(ret));
          } else {
            ch_blocks_.at(slice_idx)->rows_ += blk_buf.rows_;
            *(ch_blocks_.at(slice_idx)->get_buffer()) =
                              static_cast<ObChunkDatumStore::BlockBuffer &>(blk_buf);
            blk_buf.reset();
            is_send_row_normal = true;
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  LOG_DEBUG("[VEC2.0 PX] px transmit send row", K(get_spec().get_id()), K(get_spec().use_rich_format_),
          K(slice_idx), K(tablet_id), K(vector_row_idx), K(is_send_row_normal));
  if (OB_SUCC(ret) && is_send_row_normal) {
    if (OB_FAIL(send_row_normal(slice_idx, time_recorder, tablet_id, vector_row_idx))) {
      LOG_WARN("fail to send row normal", K(ret));
    }
  }
  LOG_DEBUG("Send row", K(slice_idx), K(ret), K(is_vectorized()));
  return ret;
}

int ObPxTransmitOp::send_row_normal(int64_t slice_idx,
                           int64_t &time_recorder,
                           int64_t tablet_id,
                           int64_t vector_row_idx)
{
  UNUSED(time_recorder);
  int ret = OB_SUCCESS;
  dtl::ObDtlChannel *ch = NULL;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
  common::ObIArray<dtl::ObDtlChannel*> &channels = task_channels_;
  OB_ASSERT(slice_idx >= 0 && slice_idx < channels.count());
  if (NULL == (ch = channels.at(slice_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (ch->is_drain()) {
    // if drain, don't send again
    LOG_TRACE("drain channel", KP(ch->get_id()));
  } else {
    if (NULL != spec.tablet_id_expr_ && !has_set_tablet_id_vector_) {
      update_row(spec.tablet_id_expr_, tablet_id);
    }
    ObPxNewRow px_row(get_spec().output_, vector_row_idx, data_msg_type_);
    if (OB_FAIL(ch->send(px_row, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail send row to slice channel", K(px_row), K(slice_idx), K(ret));
      }
    }
  }
  LOG_DEBUG("Send row", K(slice_idx), K(ret));
  return ret;
}

int ObPxTransmitOp::broadcast_eof_row()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("broadcast eof row",
          "op_id", get_spec().id_,
          "ch_cnt", task_channels_.count(),
          K(ret));
  ObPxNewRow px_eof_row;
  px_eof_row.set_eof_row();
  px_eof_row.set_data_type(data_msg_type_);
  if (OB_FAIL(chs_agent_.broadcast_row(px_eof_row, &eval_ctx_, true))) {
    LOG_WARN("unexpected NULL ptr", K(ret));
  } else if (OB_FAIL(chs_agent_.flush())) {
    LOG_WARN("fail flush row to slice channel", K(ret));
  } else if (GCONF.enable_sql_audit) {
    op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::EXCHANGE_EOF_TIMESTAMP;
    op_monitor_info_.otherstat_2_value_ = oceanbase::common::ObClockGenerator::getClock();
    // It's the end time of sending all data
    // if not, the sql plan monitor can't show the end of eof
    op_monitor_info_.last_row_time_ = oceanbase::common::ObClockGenerator::getClock();
  }
  return ret;
}

int ObPxTransmitOp::next_row()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    if (is_vectorized()) {
      brs_.end_ = true;
      brs_.size_ = 0;
    } else {
      ret = OB_ITER_END;
    }
    consume_first_row_ = true;
    LOG_TRACE("transmit iter end", K(ret), K(iter_end_));
  } else if (!consume_first_row_ && !sample_done_) {
    consume_first_row_ = true;
  } else {
    if (is_vectorized()) {
      const ObBatchRows *brs = NULL;
      ret = ObOperator::get_next_batch(spec_.max_batch_size_, brs);
    } else {
      ret = ObOperator::get_next_row();
    }
  }
  return ret;
}

int ObPxTransmitOp::link_ch_sets(ObPxTaskChSet &ch_set,
                                common::ObIArray<dtl::ObDtlChannel*> &channels,
                                ObDtlFlowControl *dfc)
{
  int ret = OB_SUCCESS;
  dtl::ObDtlChannelInfo ci;
  int64_t hash_val = 0;
  int64_t offset = 0;
  const int64_t DTL_CHANNEL_SIZE = sizeof(ObDtlRpcChannel) > sizeof(ObDtlLocalChannel) ? sizeof(ObDtlRpcChannel) : sizeof(ObDtlLocalChannel);
  if (OB_FAIL(channels.reserve(ch_set.count()))) {
    LOG_WARN("fail reserve channels", K(ret), K(ch_set.count()));
  } else if (OB_FAIL(dfc->reserve(ch_set.count()))) {
    LOG_WARN("fail reserve dfc channels", K(ret), K(ch_set.count()));
  } else if (ch_set.count() > 0) {
    ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(), "SqlDtlTxChan");
    void *buf = oceanbase::common::ob_malloc(DTL_CHANNEL_SIZE * ch_set.count(), attr);
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("malloc channel buf failed", K(ret));
    } else {
      uint16_t seed[3] = {0, 0, 0};
      int64_t time = ObTimeUtility::current_time();
      if (0 == seed[0] && 0 == seed[1] && 0 == seed[2]) {
        seed[0] = static_cast<uint16_t>(GETTID());
        seed[1] = static_cast<uint16_t>(time & 0x0000FFFF);
        seed[2] = static_cast<uint16_t>((time & 0xFFFF0000) >> 16);
        seed48(seed);
      }
      bool failed_in_push_back_to_channels = false;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < ch_set.count(); ++idx) {
        dtl::ObDtlChannel *ch = NULL;
        hash_val = jrand48(seed);
        if (OB_FAIL(ch_set.get_channel_info(idx, ci))) {
          LOG_WARN("fail get channel info", K(idx), K(ret));
        } else if (nullptr != dfc && ci.type_ == DTL_CT_LOCAL) {
          ch = new((char*)buf + offset) ObDtlLocalChannel(ci.tenant_id_, ci.chid_, ci.peer_, hash_val, ObDtlChannel::DtlChannelType::LOCAL_CHANNEL);
        } else {
          ch = new((char*)buf + offset) ObDtlRpcChannel(ci.tenant_id_, ci.chid_, ci.peer_, hash_val, ObDtlChannel::DtlChannelType::RPC_CHANNEL);
        }
        if (OB_FAIL(ret)) {
        } else if (nullptr == ch) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("create channel fail", K(ret), K(ci.tenant_id_), K(ci.chid_));
        } else if (OB_FAIL(ObDtlChannelGroup::link_channel(ci, ch, dfc))) {
          LOG_WARN("fail link channel", K(ci), K(ret));
        } else if (OB_ISNULL(ch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail add qc channel", K(ret));
        } else if (OB_FAIL(channels.push_back(ch))) {
          failed_in_push_back_to_channels = true;
          LOG_WARN("fail push back channel ptr", K(ci), K(ret));
        } else {
          offset += DTL_CHANNEL_SIZE;
        }
      }
      if (0 == channels.count() && !failed_in_push_back_to_channels) {
        ob_free(buf);
      }
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_DYNAMIC_SAMPLE_FAIL)
int ObPxTransmitOp::do_datahub_dynamic_sample(int64_t op_id, ObDynamicSamplePieceMsg &piece_msg)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("before dynamic sample", K(ctx_.get_partition_ranges()));
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dynimic sample only supported in parallel execution mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynimic sample in non-px mode");
  } else if (OB_SUCCESS != ERRSIM_DYNAMIC_SAMPLE_FAIL &&
             ctx_.get_px_task_id() == 0) {
    ret = ERRSIM_DYNAMIC_SAMPLE_FAIL;
  } else {
    const ObDynamicSampleWholeMsg *temp_whole_msg = NULL;
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    bool send_piece = true;
    if (OB_FAIL(proxy.make_sqc_sample_piece_msg(piece_msg, send_piece))) {
      LOG_WARN("fail to make sqc sample piece msg", K(ret));
    } else if (OB_FAIL(proxy.get_dh_msg_sync(op_id,
        DH_DYNAMIC_SAMPLE_WHOLE_MSG,
        proxy.get_piece_sample_msg(),
        temp_whole_msg,
        ctx_.get_physical_plan_ctx()->get_timeout_timestamp(),
        send_piece))) {
      LOG_WARN("fail get dynamic sample msg", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(temp_whole_msg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("whole msg is unexpected", K(ret));
    } else if (OB_FAIL(handler->set_partition_ranges(temp_whole_msg->part_ranges_))) {
      LOG_WARN("set partition ranges failed", K(ret), K(piece_msg), K(*temp_whole_msg));
    } else {
      LOG_INFO("dynamic sample succ", K(ret), K(piece_msg), K(*temp_whole_msg),
                                      K(handler->get_partition_ranges()));
    }
  }
  return ret;
}

int ObPxTransmitOp::build_ds_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg)
{
  UNUSED(expected_range_count);
  UNUSED(piece_msg);
  int ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "build ds picec msg");
  return ret;
}

int ObPxTransmitOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  iter_end_ = false;
  transmited_ = false;
  has_set_hybrid_key_ = false;
  dtl::ObDtlChannel *ch = NULL;
  common::ObIArray<dtl::ObDtlChannel*> &channels = task_channels_;
  for (int i = 0; i < channels.count(); ++i) {
    ch = channels.at(i);
    ch->reset_state();
    ch->set_channel_is_eof(false);
    ch->set_batch_id(ctx_.get_px_batch_id());
  }
  sampled_input_rows_.reuse();
  cur_transmit_sampled_rows_ = NULL;
  OZ(ObTransmitOp::inner_rescan());
  return ret;
}

int ObPxTransmitOp::build_object_sample_piece_msg(
    int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg)
{
  int ret = OB_SUCCESS;
  ObPxSQCProxy &proxy = ctx_.get_sqc_handler()->get_sqc_proxy();
  piece_msg.expect_range_count_ = expected_range_count;
  piece_msg.source_dfo_id_ = proxy.get_dfo_id();
  piece_msg.target_dfo_id_ = proxy.get_dfo_id();
  piece_msg.op_id_ = get_spec().id_;
  iter_end_ = ctx_.get_partition_ranges().empty();
  OZ(piece_msg.part_ranges_.assign(ctx_.get_partition_ranges()));
  return ret;
}

int ObPxTransmitSpec::register_to_datahub(ObExecContext &ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    void *buf = ctx.get_allocator().alloc(sizeof(ObInitChannelWholeMsg::WholeMsgProvider));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      ObInitChannelWholeMsg::WholeMsgProvider *provider =
        new (buf)ObInitChannelWholeMsg::WholeMsgProvider();
      ObSqcCtx &sqc_ctx = ctx.get_sqc_handler()->get_sqc_ctx();
      if (OB_FAIL(sqc_ctx.add_whole_msg_provider(get_id(), DH_INIT_CHANNEL_WHOLE_MSG, *provider))) {
        LOG_WARN("fail add whole msg provider", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::wait_channel_ready_msg()
{
  int ret = OB_SUCCESS;
  bool send_piece = false;
  bool need_wait_whole_msg = true;
  ObPxSqcHandler *handler = ctx_.get_sqc_handler();
  if (OB_ISNULL(handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get sqc handler", K(ret));
  } else {
    ObPxSQCProxy &proxy = handler->get_sqc_proxy();
    ObInitChannelPieceMsg piece;
    const ObInitChannelWholeMsg *whole_msg = nullptr;
    if (OB_FAIL(proxy.get_dh_msg(get_spec().id_,
                                 dtl::DH_INIT_CHANNEL_WHOLE_MSG,
                                 piece,
                                 whole_msg,
                                 ctx_.get_physical_plan_ctx()->get_timeout_timestamp(),
                                 send_piece,
                                 need_wait_whole_msg))) {
      LOG_WARN("failed to wait whole msg", K(ret), K(spec_.id_), K(GETTID()));
    } else {
      receive_channel_ready_ = true;
      LOG_TRACE("get channel msg, start to transmit", K(spec_.id_), K(GETTID()));
    }
  }
  return ret;
}

int ObPxTransmitOp::try_wait_channel()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!receive_channel_ready_) && OB_FAIL(wait_channel_ready_msg())) {
    LOG_WARN("failed to wait channel ready msg", K(ret));
  }
  return ret;
}

int ObPxTransmitOp::init_data_msg_type(const common::ObIArray<ObExpr *> &output)
{
  int ret = OB_SUCCESS;
  bool all_fixed = true;
  int64_t col_cnt = output.count();
  for (int64_t i = 0; all_fixed && i < col_cnt; ++i) {
    if (!output.at(i)->is_fixed_length_data_) {
      all_fixed = false;
    }
  }
  if (MY_SPEC.get_px_dop() == 1 || col_cnt > ObDtlVectorsBuffer::MAX_COL_CNT) {
    data_msg_type_ = dtl::ObDtlMsgType::PX_VECTOR_ROW;
  } else if (all_fixed) {
    data_msg_type_ = dtl::ObDtlMsgType::PX_VECTOR_FIXED;
  } else {
    data_msg_type_ = dtl::ObDtlMsgType::PX_VECTOR_ROW;
  }
  int err_sim = OB_E(EventTable::EN_DTL_OPTION) 0;
  if (0 != err_sim) {
    if (1 == std::abs(err_sim)) {
      data_msg_type_ = dtl::ObDtlMsgType::PX_VECTOR_ROW;
    } else if (2 == std::abs(err_sim)) {
      data_msg_type_ = dtl::ObDtlMsgType::PX_VECTOR_FIXED;
      if (MY_SPEC.get_px_dop() == 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Trasmit op does not support PX_VECTOR_FIXED.", K(ret));
      }
    } else if (3 == std::abs(err_sim)) {
      data_msg_type_ = dtl::ObDtlMsgType::PX_VECTOR;
      if (MY_SPEC.get_px_dop() == 1) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Trasmit op does not support PX_VECTOR.", K(ret));
      }
    }
  }
  return ret;
}

int ObPxTransmitOp::VectorSendParams::init_basic_params(const int64_t max_batch_size,
                                                        const int64_t channel_cnt,
                                                        const int64_t output_cnt,
                                                        ObIAllocator &alloc,
                                                        ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_size_array_
                        = static_cast<uint32_t *>
                            (alloc.alloc(max_batch_size * sizeof(uint32_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc row size array", K(ret), K(max_batch_size));
  } else if (OB_ISNULL(return_rows_
                        = static_cast<ObCompactRow **>
                            (alloc.alloc(max_batch_size * sizeof(ObCompactRow *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc return rows", K(ret), K(max_batch_size));
  } else if (OB_FAIL(vectors_.init(output_cnt))) {
    LOG_WARN("failed to init vector array", K(ret));
  } else if (OB_ISNULL(fallback_array_ = static_cast<uint16_t *>
              (alloc.alloc(max_batch_size * sizeof(uint16_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc row size array", K(ret), K(max_batch_size));
  } else if (OB_ISNULL(selector_array_ = static_cast<uint16_t *>
              (alloc.alloc(max_batch_size * sizeof(uint16_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc row size array", K(ret), K(max_batch_size));
  } else if (OB_ISNULL(selector_slice_idx_array_ = static_cast<uint16_t *>
              (alloc.alloc(max_batch_size * sizeof(uint16_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc selector_slice_idx_array_", K(ret), K(max_batch_size));
  } else if (OB_ISNULL(ctx.get_physical_plan_ctx()) || OB_ISNULL(ctx.get_physical_plan_ctx()->get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get phy plan", K(ret));
  } else {
    selector_array_max_size_ = max_batch_size;
    reorder_fixed_expr_ = ctx.get_physical_plan_ctx()->get_phy_plan()
                                                     ->get_min_cluster_version()
                          >= CLUSTER_VERSION_4_3_3_0;
  }
  return ret;
}

int ObPxTransmitOp::VectorSendParams::init_keep_order_params(const int64_t max_batch_size,
                                                             const int64_t channel_cnt,
                                                             const int64_t output_cnt,
                                                             ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(blocks_ =
            static_cast<ObTempRowStore::DtlRowBlock  **> (alloc.alloc(channel_cnt
                                                          * sizeof(ObTempRowStore::DtlRowBlock *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buffers", K(ret), K(channel_cnt));
  } else if (OB_ISNULL(heads_ = static_cast<int64_t *> (alloc.alloc(channel_cnt
                                                                        * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc heads", K(ret), K(channel_cnt));
  } else if (OB_ISNULL(tails_ = static_cast<int64_t *> (alloc.alloc(channel_cnt
                                                                        * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc tails", K(ret), K(channel_cnt));
  } else if (OB_ISNULL(init_pos_ = static_cast<int64_t *> (alloc.alloc(channel_cnt
                                                                        * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc init sizes", K(ret), K(channel_cnt));
  } else if (OB_ISNULL(channel_unobstructeds_ = static_cast<bool *> (alloc.alloc(channel_cnt
                                                                        * sizeof(bool))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc can adds", K(ret), K(channel_cnt));
  } else if (OB_ISNULL(fixed_payload_headers_ = static_cast<char **> (alloc.alloc(channel_cnt
                                                                        * sizeof(char *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc headers", K(ret), K(channel_cnt));
  } else if (OB_ISNULL(row_cnts_ = static_cast<int64_t *> (alloc.alloc(channel_cnt
                                                                        * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc row cnts", K(ret), K(channel_cnt));
  } else if (output_cnt > 0
              && OB_ISNULL(column_offsets_ = static_cast<int64_t *> (alloc.alloc(output_cnt
                                                                        * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc offsets", K(ret), K(output_cnt));
  } else if (output_cnt > 0
              && OB_ISNULL(column_lens_ = static_cast<int64_t *> (alloc.alloc(output_cnt
                                                                        * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc lens", K(ret), K(output_cnt));
  } else if (OB_ISNULL(row_idx_ = static_cast<int64_t *> (alloc.alloc(max_batch_size * sizeof(int64_t))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc row idx", K(ret), K(max_batch_size));
  } else if (OB_ISNULL(fixed_rows_ = static_cast<char **>
                            (alloc.alloc(max_batch_size
                                                      * sizeof(char *))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc fixed rows", K(ret), K(max_batch_size));
  } else {
    memset(blocks_, 0, channel_cnt * sizeof(ObTempRowStore::DtlRowBlock *));
    memset(heads_, 0, channel_cnt * sizeof(int64_t));
    memset(tails_, 0, channel_cnt * sizeof(int64_t));
    memset(init_pos_, 0, channel_cnt * sizeof(int64_t));
    memset(channel_unobstructeds_, 0, channel_cnt * sizeof(bool));
    memset(fixed_payload_headers_, 0, channel_cnt * sizeof(char *));
    memset(row_cnts_, 0, channel_cnt * sizeof(int64_t));
    if (output_cnt > 0) {
      memset(column_offsets_, 0, output_cnt * sizeof(int64_t));
      memset(column_lens_, 0, output_cnt * sizeof(int64_t));
    }
    memset(row_idx_, 0, max_batch_size * sizeof(int64_t));
    memset(fixed_rows_, 0, max_batch_size * sizeof(char *));
  }
  return ret;
}

int ObPxTransmitOp::update_tabletid_batch(const ObExpr *expr,ObRepartSliceIdxCalc& slice_calc)
{
  int ret = OB_SUCCESS;
  int64_t *tablet_ids = slice_calc.get_tablet_ids_ptr();
  if (OB_FAIL(slice_calc.get_tablet_ids<true>(eval_ctx_, *brs_.skip_,
    brs_.size_, tablet_ids))) {
    LOG_WARN("get tablet ids failed", K(ret));
  } else if (OB_FAIL(expr->init_vector_for_write(eval_ctx_, VectorFormat::VEC_FIXED, brs_.size_))) {
    LOG_WARN("init_vector failed", K(ret));
  } else {
    ObIVector *dst_vec = expr->get_vector(eval_ctx_);
    for (int i = 0; i < brs_.size_; i++) {
      if (brs_.skip_->at(i)) {
        continue;
      }
      dst_vec->set_int(i, tablet_ids[i]);
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
