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
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"

namespace oceanbase {
namespace sql {

class ObPxReceiveOpInput : public ObPxExchangeOpInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxReceiveOpInput(ObExecContext& ctx, const ObOpSpec& spec)
      : ObPxExchangeOpInput(ctx, spec), child_dfo_id_(common::OB_INVALID_ID), ch_provider_ptr_(0)
  {}
  virtual ~ObPxReceiveOpInput()
  {}
  virtual int init(ObTaskInfo& task_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }
  void set_child_dfo_id(int64_t child_dfo_id)
  {
    child_dfo_id_ = child_dfo_id;
  }
  void set_sqc_proxy(ObPxSQCProxy& sqc_proxy)
  {
    ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  int get_data_ch(ObPxTaskChSet& ch_set, int64_t timeout_ts, dtl::ObDtlChTotalInfo& ch_info);
  int get_dfo_key(dtl::ObDtlDfoKey& key);
  int get_first_buffer_cache(dtl::ObDtlLocalFirstBufferCache*& first_buffer_cache);
  uint64_t get_ch_provider()
  {
    return ch_provider_ptr_;
  }

protected:
  int64_t child_dfo_id_;
  uint64_t ch_provider_ptr_;
};

class ObPxReceiveSpec : public ObReceiveSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxReceiveSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObPxReceiveSpec()
  {}

  ExprFixedArray child_exprs_;
};

class ObPxReceiveOp : public ObReceiveOp {
public:
  ObPxReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxReceiveOp()
  {}

  virtual int inner_open() override
  {
    return ObOperator::inner_open();
  }
  virtual int rescan() override;
  virtual void destroy() override;
  virtual int inner_close() override
  {
    return ObOperator::inner_close();
  }

  ObPxTaskChSet& get_ch_set()
  {
    return task_ch_set_;
  };
  virtual int try_link_channel() = 0;
  /**
   * This function will block the thread until receive the task channel
   * info from SQC.
   */
  int init_channel(ObPxReceiveOpInput& recv_input, ObPxTaskChSet& task_ch_set,
      common::ObIArray<dtl::ObDtlChannel*>& task_channels, dtl::ObDtlChannelLoop& loop,
      ObPxReceiveRowP& px_row_msg_proc, ObPxInterruptP& interrupt_proc);
  virtual int init_dfc(dtl::ObDtlDfoKey& key);
  bool channel_linked()
  {
    return channel_linked_;
  }

  int drain_exch();
  int active_all_receive_channel();
  int link_ch_sets(ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, dtl::ObDtlFlowControl* dfc);
  ObOpMetric& get_op_metric()
  {
    return metric_;
  }
  common::ObIArray<dtl::ObDtlChannel*>& get_task_channels()
  {
    return task_channels_;
  }

  int64_t get_sqc_id();

public:
  void reset_for_rescan()
  {
    iter_end_ = false;
    channel_linked_ = false;
    finish_ch_cnt_ = 0;
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
  common::ObArray<dtl::ObDtlChannel*> task_channels_;
  ObPxNewRow px_row_;
  ObPxReceiveRowP px_row_msg_proc_;
  dtl::ObDtlFlowControl dfc_;
  dtl::ObDtlChannelLoop msg_loop_;
  int64_t finish_ch_cnt_;
  int64_t ts_cnt_;
  int64_t ts_;
  ObOpMetric metric_;
  dtl::ObDtlLocalFirstBufferCache* proxy_first_buffer_cache_;
  dtl::ObDtlChTotalInfo ch_info_;
};

class ObPxFifoReceiveOpInput : public ObPxReceiveOpInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxFifoReceiveOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxFifoReceiveOpInput()
  {}
};

class ObPxFifoReceiveSpec : public ObPxReceiveSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxFifoReceiveSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObPxReceiveSpec(alloc, type)
  {}
  ~ObPxFifoReceiveSpec()
  {}
};

class ObPxFifoReceiveOp : public ObPxReceiveOp {
public:
  ObPxFifoReceiveOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxFifoReceiveOp()
  {}

protected:
  virtual void destroy()
  {
    ObPxReceiveOp::destroy();
  }
  virtual int inner_open();
  virtual int inner_close();
  virtual int inner_get_next_row();
  virtual int try_link_channel() override;

private:
  int get_one_row_from_channels(int64_t timeout_us);

private:
  ObPxInterruptP interrupt_proc_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_RECEIVE_OP_H_
