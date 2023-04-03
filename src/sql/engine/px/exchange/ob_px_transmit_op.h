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
#include "sql/executor/ob_slice_calc.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/basic/ob_ra_datum_store.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"

namespace oceanbase
{
namespace sql
{

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
};

class ObPxTransmitOp : public ObTransmitOp
{
public:
  // sample DYNAMIC_SAMPLE_ROW_COUNT to MAX_DYNAMIC_SAMPLE_ROW_COUNT for dynamic sampling
  // with DYNAMIC_SAMPLE_INTERVAL interval.
  const static int64_t DYNAMIC_SAMPLE_ROW_COUNT = 90;
  const static int64_t MAX_DYNAMIC_SAMPLE_ROW_COUNT = 500;
  const static int64_t DYNAMIC_SAMPLE_INTERVAL = 10000;

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
  int send_rows(ObSliceIdxCalc &sc)
  {
    return is_vectorized() ? send_rows_in_batch(sc) : send_rows_one_by_one(sc);
  }
  int send_rows_one_by_one(ObSliceIdxCalc &sc);
  int send_rows_in_batch(ObSliceIdxCalc &sc);

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
  inline void update_row(const ObExpr *expr, int64_t tablet_id);
  int send_row_normal(int64_t slice_idx,
               int64_t &time_recorder,
               int64_t tablet_id);
  int send_row(int64_t slice_idx,
               int64_t &time_recorder,
               int64_t tablet_id);
  int send_eof_row();
  int broadcast_eof_row();
  int next_row();
  int set_rollup_hybrid_keys(ObSliceIdxCalc &slice_calc);
  int set_wf_hybrid_slice_id_calc_type(ObSliceIdxCalc &slice_calc);
  int fetch_first_row();
  int set_expect_range_count();
  int wait_channel_ready_msg();
  int64_t get_random_seq()
  {
    return nrand48(rand48_buf_) % INT16_MAX;
  }
  /*should wait sync msg before send rows. but 3 exceptions:
      1. already received the channel ready msg
      2. first DFO under PX (which is adjoin to PX/rootdfo)
      3. using single DFO scheduling policy (parallel = 1)*/
  bool need_wait_sync_msg(const ObPxSQCProxy &proxy, const uint64_t curr_cluster_version) const { return ObInitChannelPieceMsgCtx::enable_dh_channel_sync(curr_cluster_version >= CLUSTER_VERSION_4_1_0_0)
                                                                    && !receive_channel_ready_
                                                                    && !proxy.adjoining_root_dfo()
                                                                    && !proxy.get_transmit_use_interm_result(); }
  int try_wait_channel();
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
  common::ObSEArray<ObPxTabletRange, 1> ranges_;
  common::ObSEArray<ObChunkDatumStore *, 1> sample_stores_;
  // Row store to hold all sampled input rows
  ObRADatumStore sampled_input_rows_;
  // Transmit sampled input rows ranges
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
};

inline void ObPxTransmitOp::update_row(const ObExpr *expr, int64_t tablet_id)
{
  OB_ASSERT(OB_NOT_NULL(expr));
  OB_ASSERT(expr->type_ == T_PDML_PARTITION_ID);
  expr->locate_datum_for_write(eval_ctx_).set_int(tablet_id);
  expr->set_evaluated_projected(eval_ctx_);
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_TRANSMIT_OP_H_
