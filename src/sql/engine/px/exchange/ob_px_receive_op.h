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

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_RECEIVE_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_RECEIVE_OP_H_

#include "sql/engine/px/exchange/ob_receive_op.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/px/ob_px_bloom_filter.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"

namespace oceanbase
{
namespace sql
{

class ObPxReceiveOpInput : public ObPxExchangeOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxReceiveOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxExchangeOpInput(ctx, spec),
      child_dfo_id_(common::OB_INVALID_ID),
      ch_provider_ptr_(0),
      ignore_vtable_error_(false),
      init_channel_count_(0)
  {}
  virtual ~ObPxReceiveOpInput() {}
  virtual int init(ObTaskInfo &task_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }
  void set_child_dfo_id(int64_t child_dfo_id) { child_dfo_id_ = child_dfo_id; }
  int64_t get_child_dfo_id() const { return child_dfo_id_; }
  // 由 sqc 设置好后发给 task，该指针会序列化给 task
  void set_sqc_proxy(ObPxSQCProxy &sqc_proxy)
  {
    ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  int get_data_ch(ObPxTaskChSet &ch_set, int64_t timeout_ts, dtl::ObDtlChTotalInfo &ch_info);
  int get_dfo_key(dtl::ObDtlDfoKey &key);
  uint64_t get_ch_provider() { return ch_provider_ptr_; }
  void set_ignore_vtable_error(bool flag) { ignore_vtable_error_ = flag; };
  bool is_ignore_vtable_error() { return ignore_vtable_error_; }
  int64_t inc_init_channel_count() { return ATOMIC_AAF(&init_channel_count_, 1); }
  int64_t get_init_channel_count() const { return ATOMIC_LOAD(&init_channel_count_); }
protected:
  int64_t child_dfo_id_;
  uint64_t ch_provider_ptr_;
  bool ignore_vtable_error_;
  int64_t init_channel_count_;
};

class ObPxReceiveSpec : public ObReceiveSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxReceiveSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObPxReceiveSpec() {}
  virtual const common::ObIArray<ObExpr *> *get_all_exprs() const { return NULL; }
  virtual int register_to_datahub(ObExecContext &ctx) const override;
  virtual int register_init_channel_msg(ObExecContext &ctx) override;
  // 保存child是为了获取child的数据，由于数据是shuffle过来，所以拿数据只能全部拿过来
  ExprFixedArray child_exprs_;
  int64_t repartition_table_id_;
  ExprFixedArray dynamic_const_exprs_; // const expr which contain dynamic param
  common::ObFixedArray<int64_t, common::ObIAllocator> bloom_filter_id_array_; //record bloom filter id will send by this pxreceive op.
};

class ObPxReceiveOp : public ObReceiveOp
{
public:
  ObPxReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxReceiveOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual void destroy() override;
  virtual int inner_close() override { return ObOperator::inner_close(); }
  virtual void do_clear_datum_eval_flag() override;

  ObPxTaskChSet &get_ch_set() { return task_ch_set_; };
  virtual int try_link_channel() = 0;
  /**
   * This function will block the thread until receive the task channel
   * info from SQC.
   */
  int init_channel(ObPxReceiveOpInput &recv_input,
                    ObPxTaskChSet &task_ch_set,
                    common::ObIArray<dtl::ObDtlChannel *> &task_channels,
                    dtl::ObDtlChannelLoop &loop,
                    ObPxReceiveRowP &px_row_msg_proc,
                    ObPxInterruptP &interrupt_proc);
  virtual int init_dfc(dtl::ObDtlDfoKey &key);
  bool channel_linked() { return channel_linked_; }

  virtual int inner_drain_exch() override;
  int active_all_receive_channel();
  int link_ch_sets(ObPxTaskChSet &ch_set,
                    common::ObIArray<dtl::ObDtlChannel*> &channels,
                    dtl::ObDtlFlowControl *dfc);
  ObOpMetric &get_op_metric() { return metric_; }
  common::ObIArray<dtl::ObDtlChannel *> &get_task_channels() { return task_channels_; }

  int64_t get_sqc_id();
  int get_bf_ctx(int64_t idx, ObJoinFilterDataCtx &bf_ctx);

  int wrap_get_next_batch(const int64_t max_row_cnt);
  int prepare_send_bloom_filter();
  int try_send_bloom_filter();

  int erase_dtl_interm_result();
  int send_channel_ready_msg(int64_t child_dfo_id);
public:
  // clear dynamic const expr parent evaluate flag, because when dynmaic param datum
  // changed, if we don't clear dynamic const expr parent expr evaluate flag, the
  // parent expr datum ptr may point to the last dynamic param datum memory which
  // is invalid now
  OB_INLINE void clear_dynamic_const_parent_flag()
  {
    const ObPxReceiveSpec &spec = static_cast<const ObPxReceiveSpec &>(spec_);
    for (int64_t i = 0; i < spec.dynamic_const_exprs_.count(); i++) {
      ObDynamicParamSetter::clear_parent_evaluated_flag(
                          eval_ctx_, *spec.dynamic_const_exprs_.at(i));
    }
  }
  void reset_for_rescan()
  {
    iter_end_ = false;
    channel_linked_ = false;
    row_reader_.reset();
    task_ch_set_.reset();
    msg_loop_.destroy();
    px_row_msg_proc_.destroy();
    task_channels_.reset();
    ts_cnt_ = 0;
    ts_ = 0;
    dfc_.destroy();
    ch_info_.reset();
  }
  OB_INLINE int64_t get_timestamp()
  {
    if (0 == ts_cnt_ % 1000) {
      ts_ = common::ObTimeUtility::current_time();
      ++ts_cnt_;
    }
    return ts_;
  }
protected:
  ObPxTaskChSet task_ch_set_;
  bool iter_end_;
  bool channel_linked_;
  ObTMArray<dtl::ObDtlChannel *> task_channels_;
  ObReceiveRowReader row_reader_;
  ObPxReceiveRowP px_row_msg_proc_;
  dtl::ObDtlFlowControl dfc_;
  dtl::ObDtlChannelLoop msg_loop_;
  int64_t ts_cnt_;
  int64_t ts_;
  ObOpMetric metric_;
  dtl::ObDtlChTotalInfo ch_info_;
  // stored rows used for get batch rows from DTL reader.
  const ObChunkDatumStore::StoredRow **stored_rows_;
  const ObCompactRow **vector_rows_;
  obrpc::ObPxBFProxy bf_rpc_proxy_;
  int64_t bf_ctx_idx_; // the idx of bloom_filter_id_array_ in spec
  int64_t bf_send_idx_; // the idx of bf_send_ctx_array_ in sqc proxy
  // each_group_size_ only used in ObPxMsgProc::mark_rpc_filter()(this func will calc group_size, then each_group_size_ keep it for next use)
  // means how many node(sqc level) in this group
  int64_t each_group_size_;
};

class ObPxFifoReceiveOpInput : public ObPxReceiveOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxFifoReceiveOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxFifoReceiveOpInput()
  {}
};

class ObPxFifoReceiveSpec : public ObPxReceiveSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxFifoReceiveSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxReceiveSpec(alloc, type)
  {}
  ~ObPxFifoReceiveSpec() {}
};

class ObPxFifoReceiveOp : public ObPxReceiveOp
{
public:
  ObPxFifoReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxFifoReceiveOp() {}
protected:
  virtual void destroy() { ObPxReceiveOp::destroy(); }
  virtual int inner_open();
  virtual int inner_close();
  virtual int inner_get_next_row();
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int try_link_channel() override;
  int fetch_rows(const int64_t row_cnt);
private:
  // try get %row_cnt rows from channels
  int get_rows_from_channels(const int64_t row_cnt, int64_t timeout_us);
  int get_rows_from_channels_vec(const int64_t row_cnt, int64_t timeout_us);
private:
  ObPxInterruptP interrupt_proc_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_RECEIVE_OP_H_
