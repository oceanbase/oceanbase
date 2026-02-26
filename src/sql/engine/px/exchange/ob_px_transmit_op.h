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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_TRANSMIT_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_TRANSMIT_OP_H_

#include "ob_transmit_op.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/engine/px/ob_slice_calc.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/basic/ob_ra_datum_store.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace sql
{
using namespace dtl;
#define DO_TRANSMIT_FUNC(func) \
  if (OB_FAIL(MY_SPEC.use_rich_format_ ? func<true>() : func<false>()))

class ObPxTransmitOpInput : public ObPxExchangeOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxTransmitOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxExchangeOpInput(ctx, spec),
      ch_provider_ptr_(0)
  {}
  virtual ~ObPxTransmitOpInput() {}
  virtual void reset() override {}
  virtual int init(ObTaskInfo &task_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }
  // 由 sqc 设置好后发给 task，该指针会序列化给 task
  void set_sqc_proxy(ObPxSQCProxy &sqc_proxy)
  {
    ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  int get_part_ch_map(ObPxPartChInfo &map, int64_t timeout_ts);
  int get_data_ch(ObPxTaskChSet &task_ch_set, int64_t timeout_ts, dtl::ObDtlChTotalInfo *&ch_info);
  int get_parent_dfo_key(dtl::ObDtlDfoKey &key);
  int get_self_sqc_info(dtl::ObDtlSqcInfo &sqc_info);

  uint64_t get_ch_provider_ptr() { return  ch_provider_ptr_; }
  uint64_t ch_provider_ptr_; // 因为要从 sqc 序列化到本机 task 端，只能先用整数表示，才能序列化
};

class ObPxTransmitSpec : public ObTransmitSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxTransmitSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObPxTransmitSpec() {}
  virtual int register_to_datahub(ObExecContext &ctx) const override;
  ObPxSampleType sample_type_;  //for range/pkey range
  // for null aware anti join, broadcast first line && null join key
  bool need_null_aware_shuffle_;
  // Fill the partition id to this expr for the above pdml or nlj partition pruning,
  ObExpr *tablet_id_expr_;

  // produce random value to split big ranges in range distribution.
  ObExpr *random_expr_;
  // saving rows when sampling
  ExprFixedArray sampling_saving_row_;
  int64_t repartition_table_id_; // for pkey, target table location id
  ObExpr *wf_hybrid_aggr_status_expr_;
  common::ObFixedArray<int64_t, common::ObIAllocator> wf_hybrid_pby_exprs_cnt_array_;
  // for calc ddl sice idx
  ObExpr *ddl_slice_id_expr_;
  ObExprFrameInfo *dfo_expr_frame_info_;
  ExprFixedArray real_trans_exprs_;
};

class ObPxTransmitOp : public ObTransmitOp
{
public:
  // sample DYNAMIC_SAMPLE_ROW_COUNT to MAX_DYNAMIC_SAMPLE_ROW_COUNT for dynamic sampling
  // with DYNAMIC_SAMPLE_INTERVAL interval.
  const static int64_t DYNAMIC_SAMPLE_ROW_COUNT = 90;
  const static int64_t MAX_DYNAMIC_SAMPLE_ROW_COUNT = 500;
  const static int64_t DYNAMIC_SAMPLE_INTERVAL = 10000;
  const static int64_t MAX_BKT_FOR_REORDER = 1 << 16;

  ObPxTransmitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxTransmitOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int transmit();
public:
  int init_channel(ObPxTransmitOpInput &trans_input);
  int init_dfc(dtl::ObDtlDfoKey &parent_key, dtl::ObDtlSqcInfo &child_info);

  ObOpMetric &get_op_metric() { return metric_; }
  common::ObIArray<dtl::ObDtlChannel *> &get_task_channels() { return task_channels_; }
  void set_batch_param_remain(bool batch_param_remain) { batch_param_remain_ = batch_param_remain; }
protected:
  virtual int do_transmit() = 0;
  int init_channels_cur_block(common::ObIArray<dtl::ObDtlChannel*> &dtl_chs);
protected:
  int link_ch_sets(ObPxTaskChSet &ch_set,
                   common::ObIArray<dtl::ObDtlChannel *> &channels,
                   dtl::ObDtlFlowControl *dfc = nullptr);
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
  int send_rows(ObSliceIdxCalc &sc)
  {
    return is_vectorized()
            ? (get_spec().use_rich_format_
                ? send_rows_in_vector<CALC_TYPE>(sc)
                : send_rows_in_batch<CALC_TYPE>(sc))
            : send_rows_one_by_one<CALC_TYPE>(sc);
  }
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
  int send_rows_one_by_one(ObSliceIdxCalc &sc);
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
  int send_rows_in_batch(ObSliceIdxCalc &sc);
  template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
  int send_rows_in_vector(ObSliceIdxCalc &slice_calc) {
    int ret = OB_SUCCESS;
    int64_t send_row_time_recorder = 0;
    ObObj tablet_id;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    ObSliceIdxCalc::SliceIdxArray slice_idx_array;
    ObSliceIdxCalc::SliceIdxFlattenArray slice_idx_flatten_array;
    ObSliceIdxCalc::EndIdxArray end_idx_array;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(next_row())) {
        LOG_WARN("fetch next rows failed", K(ret));
        break;
      }
      if (dfc_.all_ch_drained()) {
        int tmp_ret = child_->drain_exch();
        if (OB_SUCCESS != tmp_ret) {
          LOG_WARN("drain exchange data failed", K(tmp_ret));
        }
        LOG_TRACE("all channel has been drained");
        break;
      }
      const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
      has_set_tablet_id_vector_ = false;
      batch_info_guard.set_batch_size(brs_.size_);
      if (OB_FAIL(ret) || brs_.size_ == 0) {
      } else if (OB_FAIL(set_rollup_hybrid_keys(slice_calc))) {
        LOG_WARN("failed to set rollup hybrid keys", K(ret));
      } else if (!slice_calc.support_vectorized_calc() && !slice_calc.is_multi_slice_calc_type()) {
        for (int64_t i = 0; i < spec_.output_.count() && OB_SUCC(ret); i++) {
          ObExpr *expr = spec_.output_.at(i);
          if (T_TABLET_AUTOINC_NEXTVAL == expr->type_ || T_PSEUDO_HIDDEN_CLUSTERING_KEY == expr->type_) {
            ObIVector *vec = expr->get_vector(eval_ctx_);
            const char *payload = vec->get_payload(0);
            if (NULL != payload) {
            } else if (OB_FAIL(expr->init_vector(eval_ctx_, VEC_UNIFORM_CONST, 1 /*size*/))) {
              LOG_WARN("init vector failed", K(ret));
            } else {
              vec->set_null(0);
            }
          } else if (OB_FAIL(expr->eval_vector(eval_ctx_, brs_))) {
            LOG_WARN("eval expr failed", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
          if (brs_.skip_->at(i)) {
            continue;
          }
          batch_info_guard.set_batch_idx(i);
          metric_.count();
          //TODO: shanting2.0 支持wf2.0时做一下简单适配
          if (OB_FAIL(set_wf_hybrid_slice_id_calc_type(slice_calc))) {
            LOG_WARN("failed to set wf hybrid keys", K(ret));
          } else if (OB_FAIL((slice_calc.get_slice_indexes<CALC_TYPE, true>(get_spec().output_,
                              eval_ctx_, slice_idx_array, brs_.skip_)))) {
            LOG_WARN("fail get slice idx", K(ret));
          } else if (NULL != spec.tablet_id_expr_
          // 为什么要获取上一次的tablet_id
                    && OB_FAIL(slice_calc.get_previous_row_tablet_id(tablet_id))) {
            LOG_WARN("failed to get previous row tablet_id", K(ret));
          }
          LOG_DEBUG("[VEC2.0 PX] send rows vec without prefetch", K(i), K(slice_idx_array), K(tablet_id.get_int()));
          FOREACH_CNT_X(slice_idx, slice_idx_array, OB_SUCC(ret)) {
            if (OB_FAIL(send_row(*slice_idx, send_row_time_recorder, tablet_id.get_int(), i))) {
              LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx_array));
            }
          }
        }
      } else if (slice_calc.is_multi_slice_calc_type()) {
        if (NULL != spec.tablet_id_expr_) {
          ObRepartSliceIdxCalc &repart_slice_calc =
            static_cast<ObRepartSliceIdxCalc &>(slice_calc);
          if (OB_FAIL(update_tabletid_batch(spec.tablet_id_expr_, repart_slice_calc))) {
            LOG_WARN("failed to update_tabletid_batch", K(ret));
          } else {
            has_set_tablet_id_vector_ = true;
          }
        }
        set_wf_hybrid_exprs(slice_calc);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL((slice_calc.get_multi_slice_idx_vector<CALC_TYPE>(spec_.output_, eval_ctx_,
                                                *brs_.skip_, brs_.size_,
                                                slice_idx_flatten_array, end_idx_array)))) {
          LOG_WARN("calc slice indexes failed", K(ret));
        } else {
          bool is_broad_cast_calc_type =
            CALC_TYPE == ObSliceIdxCalc::SliceCalcType::BROADCAST ||
            CALC_TYPE == ObSliceIdxCalc::SliceCalcType::BC2HOST;
          for (int64_t i = 0; i < spec_.output_.count() && OB_SUCC(ret); i++) {
            ObExpr *expr = spec_.output_.at(i);
            if (T_TABLET_AUTOINC_NEXTVAL == expr->type_ || T_PSEUDO_HIDDEN_CLUSTERING_KEY == expr->type_) {
              ObIVector *vec = expr->get_vector(eval_ctx_);
              const char *payload = vec->get_payload(0);
              if (NULL != payload) {
              } else if (OB_FAIL(expr->init_vector(eval_ctx_, VEC_UNIFORM_CONST, 1 /*size*/))) {
                LOG_WARN("init vector failed", K(ret));
              } else {
                vec->set_null(0);
              }
            } else if (OB_FAIL(spec_.output_.at(i)->eval_vector(eval_ctx_, brs_))) {
              LOG_WARN("eval expr failed", K(ret));
            }
          }
          if (OB_FAIL(ret)) {
          } else {
            if (PX_VECTOR_FIXED == data_msg_type_) {
              if (OB_FAIL(keep_order_send_batch_fixed(batch_info_guard, slice_idx_flatten_array,
                  end_idx_array, is_broad_cast_calc_type))) {
                LOG_WARN("failed to send batch", K(ret));
              }
            } else {
              if (OB_FAIL(keep_order_send_batch(batch_info_guard, slice_idx_flatten_array,
                  end_idx_array, is_broad_cast_calc_type))) {
                LOG_WARN("failed to send batch", K(ret));
              }
            }
          }
        }
      } else {
        int64_t *indexes = NULL;
        if (NULL != spec.tablet_id_expr_) {
          ObRepartSliceIdxCalc &repart_slice_calc =
            static_cast<ObRepartSliceIdxCalc &>(slice_calc);
          if (OB_FAIL(update_tabletid_batch(spec.tablet_id_expr_, repart_slice_calc))) {
            LOG_WARN("failed to update_tabletid_batch", K(ret));
          } else {
            has_set_tablet_id_vector_ = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL((slice_calc.get_slice_idx_batch<CALC_TYPE, true>(spec_.output_, eval_ctx_,
                                                *brs_.skip_, brs_.size_,
                                                indexes)))) {
          LOG_WARN("calc slice indexes failed", K(ret));
        } else {
          for (int64_t i = 0; i < spec_.output_.count() && OB_SUCC(ret); i++) {
            ObExpr *expr = spec_.output_.at(i);
            if (T_TABLET_AUTOINC_NEXTVAL == expr->type_ || T_PSEUDO_HIDDEN_CLUSTERING_KEY == expr->type_) {
              ObIVector *vec = expr->get_vector(eval_ctx_);
              const char *payload = vec->get_payload(0);
              if (NULL != payload) {
              } else if (OB_FAIL(expr->init_vector(eval_ctx_, VEC_UNIFORM_CONST, 1 /*size*/))) {
                LOG_WARN("init vector failed", K(ret));
              } else {
                vec->set_null(0);
              }
            } else if (OB_FAIL(spec_.output_.at(i)->eval_vector(eval_ctx_, brs_))) {
              LOG_WARN("eval expr failed", K(ret));
            }
          }
          LOG_DEBUG("[VEC2.0 PX] send rows vec with prefetch", K(CALC_TYPE),
                  K(ObArrayWrap<int64_t>(indexes, brs_.size_)));
          if (dtl::ObDtlMsgType::PX_VECTOR_ROW == data_msg_type_) {
            for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
              if (brs_.skip_->at(i) || indexes[i] < 0) { continue; }
                ObDtlBasicChannel *channel = static_cast<ObDtlBasicChannel *> (task_channels_.at(indexes[i]));
                channel->get_vector_row_writer().prefetch();
            }
          }
          if (OB_FAIL(ret)) {
          } else {
            slice_idx_flatten_array.reuse();
            end_idx_array.reuse();
            for (int i = 0; i < brs_.size_ && OB_SUCC(ret); i++) {
              if (!brs_.skip_->at(i) && indexes[i] >= 0) {
                if (OB_FAIL(slice_idx_flatten_array.push_back(indexes[i]))) {
                  LOG_WARN("pushback slice_idx_flatten_array failed", K(i), K(ret));
                }
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(end_idx_array.push_back(slice_idx_flatten_array.count()))) {
                  LOG_WARN("pushback end_idx_array failed", K(i), K(ret));
                }
              }
            }

            if (OB_FAIL(ret)) {
            } else if (PX_VECTOR_FIXED == data_msg_type_) {
              if (OB_FAIL(keep_order_send_batch_fixed(batch_info_guard, slice_idx_flatten_array,
                  end_idx_array, false))) {
                LOG_WARN("failed to send batch", K(ret));
              }
            } else if (OB_FAIL(keep_order_send_batch(batch_info_guard, slice_idx_flatten_array,
                  end_idx_array, false))) {
              LOG_WARN("failed to send batch", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && brs_.end_) {
        if (get_spec().use_rich_format_ && PX_VECTOR_FIXED == data_msg_type_) {
          for (int64_t idx = 0; idx < task_channels_.count(); ++idx) {
            ObDtlBasicChannel *channel =
                            static_cast<ObDtlBasicChannel *> (task_channels_.at(idx));
            ObDtlVectorFixedMsgWriter &row_writer = channel->get_vector_fixed_msg_writer();
            if (row_writer.is_inited() && params_.row_cnts_[idx] > 0) {
              row_writer.update_row_cnt(params_.row_cnts_[idx]);
              row_writer.update_buffer_used();
              params_.row_cnts_[idx] = 0;
            }
          }
        }
        if (OB_FAIL(try_wait_channel())) {
          LOG_WARN("failed to wait channel init", K(ret));
        } else if (batch_param_remain_) {
          ObPxNewRow px_eof_row;
          px_eof_row.set_eof_row();
          px_eof_row.set_data_type(data_msg_type_);
          ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
          if (OB_ISNULL(phy_plan_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy plan ctx is null", K(ret));
          }
          for (int i = 0; i < task_channels_.count() && OB_SUCC(ret); i++) {
            dtl::ObDtlChannel *ch = task_channels_.at(i);
            if (OB_FAIL(ch->send(px_eof_row, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_, false))) {
              LOG_WARN("fail send eof row to slice channel", K(px_eof_row), K(ret));
            } else if (OB_FAIL(ch->push_buffer_batch_info())) {
              LOG_WARN("channel push back batch failed", K(ret));
            }
          }
        } else if (OB_FAIL(send_eof_row())) {
          LOG_WARN("fail send eof rows to channels", K(ret));
        }
        break;
      }
      // for those break out ops
    }
    LOG_TRACE("Transmit time record, send rows in vector", K(ret));
    return ret;
  }
  template <bool USE_VEC>
  int broadcast_rows(ObSliceIdxCalc &slice_calc);

  // for dynamic sample
  virtual int build_ds_piece_msg(int64_t expected_range_count,
      ObDynamicSamplePieceMsg &piece_msg);
  virtual int do_datahub_dynamic_sample(int64_t op_id,
      ObDynamicSamplePieceMsg &piece_msg);
  virtual int build_object_sample_piece_msg(int64_t expected_range_count,
    ObDynamicSamplePieceMsg &piece_msg);
  bool is_object_sample();
  bool is_row_sample();
private:
  int next_vector(const int64_t max_row_cnt);
  int next_batch(const int64_t max_row_cnt);
  inline void update_row(const ObExpr *expr, int64_t tablet_id);
  int update_tabletid_batch(const ObExpr *expr, ObRepartSliceIdxCalc& slice_calc);
  int send_row_normal(int64_t slice_idx,
               int64_t &time_recorder,
               int64_t tablet_id,
               int64_t vector_row_idx);
  int send_row(int64_t slice_idx,
               int64_t &time_recorder,
               int64_t tablet_id,
               int64_t vecotor_row_idx = OB_INVALID_ID);
  int send_eof_row();
  int broadcast_eof_row();
  int next_row();
  //template<bool USE_VEC>
  int set_rollup_hybrid_keys(ObSliceIdxCalc &slice_calc);
  //template<bool USE_VEC>
  int set_wf_hybrid_slice_id_calc_type(ObSliceIdxCalc &slice_calc);
  int fetch_first_row();
  int set_expect_range_count();
  int wait_channel_ready_msg();
  int try_extend_selector_array(int64_t target_count, bool is_fixed);
  int keep_order_send_batch(ObEvalCtx::BatchInfoScopeGuard &batch_info_guard,
                            ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                            ObSliceIdxCalc::EndIdxArray &end_idx_array,
                            bool is_broad_cast_calc_type);
  int keep_order_send_batch_fixed(ObEvalCtx::BatchInfoScopeGuard &batch_info_guard,
                            ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                            ObSliceIdxCalc::EndIdxArray &end_idx_array,
                            bool is_broad_cast_calc_type);
  int64_t get_random_seq()
  {
    return nrand48(rand48_buf_) % INT16_MAX;
  }
  /*should wait sync msg before send rows. but 3 exceptions:
      1. already received the channel ready msg
      2. first DFO under PX (which is adjoin to PX/rootdfo)
      3. using single DFO scheduling policy (parallel = 1)*/
  bool need_wait_sync_msg(const ObPxSQCProxy &proxy) const { return !receive_channel_ready_
                                                                    && !proxy.adjoining_root_dfo()
                                                                    && !proxy.get_transmit_use_interm_result(); }
  int try_wait_channel();
  int init_data_msg_type(const common::ObIArray<ObExpr *> &output);
  void fill_batch_ptrs(const int64_t *indexes);
  void fill_batch_ptrs_fixed(const int64_t *indexes);
  void fill_batch_ptrs(ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                       ObSliceIdxCalc::EndIdxArray &end_idx_array);
  void fill_batch_ptrs_fixed(ObSliceIdxCalc::SliceIdxFlattenArray &slice_idx_flatten_array,
                             ObSliceIdxCalc::EndIdxArray &end_idx_array);
  void fill_broad_cast_ptrs(int64_t slice_idx);
  void fill_broad_cast_ptrs_fixed(int64_t slice_idx);
  dtl::ObDtlMsgType get_data_msg_type() const { return data_msg_type_; }
  void set_wf_hybrid_exprs(ObSliceIdxCalc &slice_calc);
protected:
  ObArray<ObChunkDatumStore::Block *> ch_blocks_;
  ObArray<ObChunkDatumStore::BlockBufferWrap> blk_bufs_;
  common::ObArray<dtl::ObDtlChannel*> task_channels_;
  common::ObArenaAllocator px_row_allocator_;
  ObPxTaskChSet task_ch_set_;
  bool transmited_;
  // const ObChunkDatum::LastStoredRow first_row_;
  bool iter_end_;
  bool consume_first_row_;
  dtl::ObDtlUnblockingMsgP dfc_unblock_msg_proc_;
  dtl::ObDtlFlowControl dfc_;
  dtl::ObDtlChannelLoop loop_;
  ObPxInterruptP interrupt_proc_;
  ObOpMetric metric_;
  dtl::ObDtlChanAgent chs_agent_;
  bool use_bcast_opt_;
  ObPxPartChInfo part_ch_info_;
  dtl::ObDtlChTotalInfo *ch_info_;
  bool sample_done_;
  common::ObSEArray<ObChunkDatumStore *, 1> sample_stores_;
  // Row store to hold all sampled input rows
  ObRADatumStore sampled_input_rows_;
  // Transmit sampled input rows ranges， pair is <start_pos, len>
  common::ObSEArray<std::pair<int64_t, int64_t>, 1> sampled_rows2transmit_;
  // Current transmit sampled rows
  std::pair<int64_t, int64_t> *cur_transmit_sampled_rows_;
  // row store iteration age control to iterate multiple rows in vectorized execution.
  ObRADatumStore::IterationAge sampled_input_rows_it_age_;
  bool has_set_hybrid_key_;
  // px batch rescan is used and this is not the last parameter, so do not force flush dtl buffer.
  bool batch_param_remain_;

  unsigned short rand48_buf_[3];
  bool receive_channel_ready_;
  dtl::ObDtlMsgType data_msg_type_;
  bool has_set_tablet_id_vector_ = false;
  //slice_idx, batch_idx
  struct VectorSendParams {
    VectorSendParams(common::ObIAllocator &alloc) : slice_info_bkts_(nullptr),
                                                    slice_bkt_item_cnts_(nullptr),
                                                    vectors_(&alloc),
                                                    selector_array_(nullptr),
                                                    selector_slice_idx_array_(nullptr),
                                                    selector_cnt_(0),
                                                    row_size_array_(nullptr),
                                                    return_rows_(nullptr),
                                                    meta_(),
                                                    fallback_array_(nullptr),
                                                    fallback_cnt_(0),
                                                    blocks_(nullptr),
                                                    heads_(nullptr),
                                                    tails_(nullptr),
                                                    init_pos_(nullptr),
                                                    channel_unobstructeds_(nullptr),
                                                    init_hash_reorder_struct_(false),
                                                    fixed_payload_headers_(nullptr),
                                                    column_offsets_(nullptr),
                                                    column_lens_(nullptr),
                                                    row_cnts_(nullptr),
                                                    row_idx_(nullptr),
                                                    offset_inited_(false),
                                                    row_limit_(-1),
                                                    fixed_rows_(nullptr),
                                                    reorder_fixed_expr_(false),
                                                    selector_array_max_size_(0) {}
    int init_basic_params(const int64_t max_batch_size, const int64_t channel_cnt,
                          const int64_t output_cnt, ObIAllocator &alloc, ObExecContext &ctx);
    int init_keep_order_params(const int64_t max_batch_size, const int64_t channel_cnt,
                               const int64_t output_cnt, ObIAllocator &alloc);
    uint16_t **slice_info_bkts_;
    uint16_t *slice_bkt_item_cnts_;
    ObFixedArray<ObIVector *, common::ObIAllocator> vectors_;
    uint16_t *selector_array_;
    uint16_t *selector_slice_idx_array_;
    int64_t selector_cnt_;
    uint32_t *row_size_array_;
    ObCompactRow **return_rows_;
    RowMeta meta_;
    uint16_t *fallback_array_;
    int64_t fallback_cnt_;
    ObTempRowStore::DtlRowBlock **blocks_;
    int64_t *heads_;
    int64_t *tails_;
    int64_t *init_pos_; //memset from this pos
    bool *channel_unobstructeds_;
    bool init_hash_reorder_struct_;
    char **fixed_payload_headers_;
    int64_t *column_offsets_;
    int64_t *column_lens_;
    int64_t *row_cnts_;
    int64_t *row_idx_;
    bool offset_inited_;
    int64_t row_limit_;
    char **fixed_rows_;
    bool reorder_fixed_expr_;
    int64_t selector_array_max_size_;
  };
  VectorSendParams params_;
};

inline void ObPxTransmitOp::update_row(const ObExpr *expr, int64_t tablet_id)
{
  OB_ASSERT(OB_NOT_NULL(expr));
  OB_ASSERT(expr->type_ == T_PDML_PARTITION_ID);
  if (get_spec().use_rich_format_) {
    expr->init_vector(eval_ctx_, VectorFormat::VEC_UNIFORM, 1);
  }
  expr->locate_datum_for_write(eval_ctx_).set_int(tablet_id);
  expr->set_evaluated_projected(eval_ctx_);
}


template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
int ObPxTransmitOp::send_rows_one_by_one(ObSliceIdxCalc &slice_calc)
{
  int ret = OB_SUCCESS;
  int64_t send_row_time_recorder = 0;
  int64_t row_count = 0;
  ObObj tablet_id;

  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_ISNULL(phy_plan_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is null", K(ret));
  }
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
    ret = next_row();
    if (OB_FAIL(ret)) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next row from child op",
                 K(ret), K(child_->get_spec().get_type()));
      } else {
        // iter end
        const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
        if (OB_FAIL(try_wait_channel())) {
          LOG_WARN("failed to wait channel init", K(ret));
        } else if (batch_param_remain_) {
          ret = OB_SUCCESS;
          ObPxNewRow px_eof_row;
          px_eof_row.set_eof_row();
          px_eof_row.set_data_type(data_msg_type_);
          for (int i = 0; i < task_channels_.count() && OB_SUCC(ret); i++) {
            dtl::ObDtlChannel *ch = task_channels_.at(i);
            if (OB_FAIL(ch->send(px_eof_row, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_, false))) {
              LOG_WARN("fail send eof row to slice channel", K(px_eof_row), K(ret));
            } else if (OB_FAIL(ch->push_buffer_batch_info())) {
              LOG_WARN("channel push back batch failed", K(ret));
            }
          }
        } else if (OB_FAIL(send_eof_row())) { // overwrite err code
          LOG_WARN("fail send eof rows to channels", K(ret));
        }
        break;
      }
    }
    row_count++;
    metric_.count();
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (OB_FAIL(set_rollup_hybrid_keys(slice_calc))) {
      LOG_WARN("failed to set rollup hybrid keys", K(ret));
    } else if (OB_FAIL(set_wf_hybrid_slice_id_calc_type(slice_calc))) {
      LOG_WARN("failed to set rollup hybrid keys", K(ret));
    } else if (OB_FAIL((slice_calc.get_slice_indexes<CALC_TYPE, false>(
                       get_spec().output_, eval_ctx_, slice_idx_array)))) {
      LOG_WARN("fail get slice idx", K(ret));
    } else if (dfc_.all_ch_drained()) {
      int tmp_ret = child_->drain_exch();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("drain exchange data failed", K(tmp_ret));
      }
      ret = OB_ITER_END;
      LOG_DEBUG("all channel has been drained");
    } else if (NULL != spec.tablet_id_expr_
               && OB_FAIL(slice_calc.get_previous_row_tablet_id(tablet_id))) {
      LOG_WARN("failed to get previous row tablet_id", K(ret));
    }
    FOREACH_CNT_X(slice_idx, slice_idx_array, OB_SUCC(ret)) {
      if (OB_FAIL(send_row(*slice_idx, send_row_time_recorder, tablet_id.get_int()))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx_array));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    LOG_TRACE("transmit meet a iter end");
  }
  LOG_TRACE("Transmit time record", K(row_count), K(ret));
  return ret;
}

template <ObSliceIdxCalc::SliceCalcType CALC_TYPE>
int ObPxTransmitOp::send_rows_in_batch(ObSliceIdxCalc &slice_calc)
{
  int ret = OB_SUCCESS;
  int64_t send_row_time_recorder = 0;
  int64_t row_count = 0;
  ObObj tablet_id;
  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  while (OB_SUCC(ret)) {
    if (OB_FAIL(next_row())) {
      LOG_WARN("fetch next rows failed", K(ret));
      break;
    }
    if (dfc_.all_ch_drained()) {
      int tmp_ret = child_->drain_exch();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("drain exchange data failed", K(tmp_ret));
      }
      LOG_TRACE("all channel has been drained");
      break;
    }
    const ObPxTransmitSpec &spec = static_cast<const ObPxTransmitSpec &>(get_spec());
    batch_info_guard.set_batch_size(brs_.size_);
    if (OB_FAIL(ret) || brs_.size_ <= 0) {
    } else if (OB_FAIL(set_rollup_hybrid_keys(slice_calc))) {
      LOG_WARN("failed to set rollup hybrid keys", K(ret));
    } else if ((!slice_calc.support_vectorized_calc() || slice_calc.is_multi_slice_calc_type() || NULL != spec.tablet_id_expr_)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
        if (brs_.skip_->at(i)) {
          continue;
        }
        batch_info_guard.set_batch_idx(i);
        row_count += 1;
        metric_.count();
        if (OB_FAIL(set_wf_hybrid_slice_id_calc_type(slice_calc))) {
          LOG_WARN("failed to set wf hybrid keys", K(ret));
        } else if (OB_FAIL((slice_calc.get_slice_indexes<CALC_TYPE, false>(get_spec().output_, eval_ctx_, slice_idx_array)))) {
          LOG_WARN("fail get slice idx", K(ret));
        } else if (NULL != spec.tablet_id_expr_
                   && OB_FAIL(slice_calc.get_previous_row_tablet_id(tablet_id))) {
          LOG_WARN("failed to get previous row tablet_id", K(ret));
        }
        LOG_DEBUG("[VEC2.0 PX] send rows batch without prefetch", K(i), K(slice_idx_array), K(tablet_id.get_int()));
        FOREACH_CNT_X(slice_idx, slice_idx_array, OB_SUCC(ret)) {
          if (OB_FAIL(send_row(*slice_idx, send_row_time_recorder, tablet_id.get_int()))) {
            LOG_WARN("fail emit row to interm result", K(ret), K(slice_idx_array));
          }
        }
      }
    } else {
      int64_t *indexes = NULL;
      if (OB_FAIL((slice_calc.get_slice_idx_batch<CALC_TYPE, false>(spec_.output_, eval_ctx_,
                                               *brs_.skip_, brs_.size_,
                                               indexes)))) {
        LOG_WARN("calc slice indexes failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
          if (brs_.skip_->at(i) || indexes[i] < 0) { continue; }
            __builtin_prefetch(&blk_bufs_.at(indexes[i]),
                               0, // for read
                               1); // low temporal locality
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
          if (brs_.skip_->at(i) || indexes[i] < 0) { continue; }
          if (blk_bufs_.at(indexes[i]).is_inited()) {
            __builtin_prefetch(blk_bufs_.at(indexes[i]).head(),
                               1, // for write
                               1); // low temporal locality
          }
        }
        LOG_DEBUG("[VEC2.0 PX] send rows batch with prefetch", K(CALC_TYPE),
                 K(ObArrayWrap<int64_t>(indexes, brs_.size_)));
        for (int64_t i = 0; OB_SUCC(ret) && i < brs_.size_; i++) {
          if (brs_.skip_->at(i)) {
            continue;
          }
          batch_info_guard.set_batch_idx(i);
          row_count += 1;
          metric_.count();
          if (OB_FAIL(send_row(indexes[i], send_row_time_recorder, tablet_id.get_int()))) {
            LOG_WARN("fail emit row to interm result", K(ret), K(indexes[i]));
          }
        }
      }
    }
    if (OB_SUCC(ret) && brs_.end_) {
      if (OB_FAIL(try_wait_channel())) {
        LOG_WARN("failed to wait channel init", K(ret));
      } else if (batch_param_remain_) {
        ObPxNewRow px_eof_row;
        px_eof_row.set_eof_row();
        px_eof_row.set_data_type(data_msg_type_);
        ObPhysicalPlanCtx *phy_plan_ctx = GET_PHY_PLAN_CTX(ctx_);
        if (OB_ISNULL(phy_plan_ctx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy plan ctx is null", K(ret));
        }
        for (int i = 0; i < task_channels_.count() && OB_SUCC(ret); i++) {
          dtl::ObDtlChannel *ch = task_channels_.at(i);
          if (OB_FAIL(ch->send(px_eof_row, phy_plan_ctx->get_timeout_timestamp(), &eval_ctx_, false))) {
            LOG_WARN("fail send eof row to slice channel", K(px_eof_row), K(ret));
          } else if (OB_FAIL(ch->push_buffer_batch_info())) {
            LOG_WARN("channel push back batch failed", K(ret));
          }
        }
      } else if (OB_FAIL(send_eof_row())) {
        LOG_WARN("fail send eof rows to channels", K(ret));
      }
      break;
    }
    // for those break out ops
  }
  LOG_TRACE("Transmit time record", K(row_count), K(ret));
  return ret;
}

template <bool USE_VEC>
int ObPxTransmitOp::broadcast_rows(ObSliceIdxCalc &slice_calc)
{
  int ret = OB_SUCCESS;
  UNUSED(slice_calc);
  int64_t row_count = 0;

  ObSliceIdxCalc::SliceIdxArray slice_idx_array;
  while (OB_SUCC(ret)) {
    int64_t rows = 0;
    bool reach_end = false;
    if (OB_FAIL(next_row())) {
      if (OB_ITER_END == ret) {
        reach_end = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next row from child op",
                 K(ret), K(child_->get_spec().get_type()));
      }
    } else {
      if (is_vectorized()) {
        rows = brs_.size_;
        reach_end = brs_.end_;
      } else {
        rows = 1;
      }
    }
    row_count++;
    metric_.count();
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to get next row", K(ret));
    } else if (dfc_.all_ch_drained()) {
      LOG_DEBUG("all channel has been drained");
      break;
    } else if (OB_FAIL(try_wait_channel())) {
      LOG_WARN("failed to wait channel", K(ret));
    } else if (USE_VEC) {
      ObPxNewRow px_row(get_spec().output_, OB_INVALID_ID, data_msg_type_);
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_size(rows);
      for (int64_t i = 0; OB_SUCC(ret) && i < rows; i++) {
        if (brs_.skip_->at(i)) {
          continue;
        }
        row_count++;
        metric_.count();
        batch_info_guard.set_batch_idx(i);
        px_row.set_vector_row_idx(i);
        ret = chs_agent_.broadcast_row(px_row, &eval_ctx_);
      }
    } else {
      ObPxNewRow px_row(get_spec().output_, OB_INVALID_ID, data_msg_type_);
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_size(rows);
      for (int64_t i = 0; OB_SUCC(ret) && i < rows; i++) {
        if (is_vectorized() && brs_.skip_->at(i)) {
          continue;
        }
        row_count++;
        metric_.count();
        batch_info_guard.set_batch_idx(i);
        ret = chs_agent_.broadcast_row(px_row, &eval_ctx_);
      }
    }

    if (OB_SUCC(ret) && reach_end) {
      if (OB_FAIL(try_wait_channel())) {
        LOG_WARN("failed to wait channel", K(ret));
      } else if (OB_FAIL(broadcast_eof_row())) {
        LOG_WARN("fail send eof rows to channels", K(ret));
      }
      break;
    }
  }
  LOG_TRACE("Transmit time record", K(row_count), K(ret));
  return ret;
}


} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_TRANSMIT_OP_H_
