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

#ifndef _OB_SQ_OB_PX_RECEIVE_H_
#define _OB_SQ_OB_PX_RECEIVE_H_

#include "sql/executor/ob_receive.h"
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

class ObPxReceiveInput : public ObPxExchangeInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxReceiveInput() : child_dfo_id_(common::OB_INVALID_ID), ch_provider_ptr_(0)
  {}
  virtual ~ObPxReceiveInput()
  {}
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
  {
    int ret = OB_SUCCESS;
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(op);
    return ret;
  }
  void set_child_dfo_id(int64_t child_dfo_id)
  {
    child_dfo_id_ = child_dfo_id;
  }
  // After being set by sqc and sent to task, this pointer will be serialized to task
  void set_sqc_proxy(ObPxSQCProxy& sqc_proxy)
  {
    ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  int get_data_ch(ObPxTaskChSet& ch_set, int64_t timeout_ts);
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

// ObPxReceive is pure base class, no get_type() info provided
class ObPxReceive : public ObReceive {
public:
  class ObPxReceiveCtx : public ObReceiveCtx {
    friend class ObPxReceive;

  public:
    explicit ObPxReceiveCtx(ObExecContext& ctx)
        : ObReceiveCtx(ctx),
          task_ch_set_(),
          iter_end_(false),
          channel_linked_(false),
          task_channels_(),
          px_row_(),
          px_row_msg_proc_(px_row_),
          msg_loop_(),
          finish_ch_cnt_(0),
          ts_cnt_(0),
          ts_(0),
          proxy_first_buffer_cache_(nullptr){};
    virtual ~ObPxReceiveCtx() = default;
    virtual void destroy()
    {
      ObReceiveCtx::destroy();
      task_ch_set_.reset();
      msg_loop_.destroy();
      px_row_msg_proc_.destroy();
      // no need to reset px_row_
      task_channels_.reset();
      ts_cnt_ = 0;
      ts_ = 0;
      dfc_.destroy();
    }
    void reset_for_rescan()
    {
      iter_end_ = false;
      channel_linked_ = false;
      finish_ch_cnt_ = 0;
      ts_cnt_ = 0;
      ts_ = 0;
      task_ch_set_.reset();
      msg_loop_.destroy();
      px_row_msg_proc_.destroy();
      // no need to reset px_row_
      task_channels_.reset();
      ts_cnt_ = 0;
      ts_ = 0;
      dfc_.destroy();
    }

    OB_INLINE int64_t get_timestamp()
    {
      if (0 == ts_cnt_ % 1000) {
        ts_ = common::ObTimeUtility::current_time();
        ++ts_cnt_;
      }
      return ts_;
    }
    ObPxTaskChSet& get_ch_set()
    {
      return task_ch_set_;
    };
    /**
     * This function will block the thread until receive the task channel
     * info from SQC.
     */
    int init_channel(ObExecContext& ctx, ObPxReceiveInput& recv_input, ObPxTaskChSet& task_ch_set,
        common::ObIArray<dtl::ObDtlChannel*>& task_channels, dtl::ObDtlChannelLoop& loop,
        ObPxReceiveRowP& px_row_msg_proc, ObPxInterruptP& interrupt_proc);
    virtual int init_dfc(ObExecContext& ctx, dtl::ObDtlDfoKey& key);
    bool channel_linked()
    {
      return channel_linked_;
    }

    ObOpMetric& get_op_metric()
    {
      return metric_;
    }
    common::ObIArray<dtl::ObDtlChannel*>& get_task_channels()
    {
      return task_channels_;
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
  };

public:
  explicit ObPxReceive(common::ObIAllocator& alloc);
  virtual ~ObPxReceive();
  virtual int rescan(ObExecContext& ctx) const override;
  virtual int drain_exch(ObExecContext& ctx) const override;
  virtual int try_link_channel(ObExecContext& ctx) const = 0;
  virtual int active_all_receive_channel(ObPxReceiveCtx& recv_ctx, ObExecContext& ctx) const;

protected:
  // helper func
  static int link_ch_sets(
      ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, dtl::ObDtlFlowControl* dfc = nullptr);
  int get_sqc_id(ObExecContext& ctx, int64_t& sqc_id) const;

private:
  /* functions */
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPxReceive);
};

class ObPxFifoReceiveInput : public ObPxReceiveInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxFifoReceiveInput() : ObPxReceiveInput()
  {}
  virtual ~ObPxFifoReceiveInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_PX_FIFO_RECEIVE;
  }
};

class ObPxFifoReceive : public ObPxReceive {
public:
  class ObPxFifoReceiveCtx : public ObPxReceiveCtx {
  public:
    friend class ObPxFifoReceive;

  public:
    explicit ObPxFifoReceiveCtx(ObExecContext& ctx);
    virtual ~ObPxFifoReceiveCtx();
    virtual void destroy()
    {
      ObPxReceiveCtx::destroy();
      // no need to reset interrupt_proc_
    }

  private:
    ObPxInterruptP interrupt_proc_;
  };

public:
  explicit ObPxFifoReceive(common::ObIAllocator& alloc);
  virtual ~ObPxFifoReceive();

protected:
  virtual int create_operator_input(ObExecContext& ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int try_link_channel(ObExecContext& ctx) const override;

private:
  int get_one_row_from_channels(ObPxFifoReceiveCtx& recv_ctx, dtl::ObDtlChannelLoop& loop, int64_t timeout_us,
      const common::ObNewRow*& row) const;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQ_OB_PX_RECEIVE_H_ */
//// end of header file
