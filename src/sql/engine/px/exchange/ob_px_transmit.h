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

#ifndef _OB_SQ_OB_PX_TRANSMIT_H_
#define _OB_SQ_OB_PX_TRANSMIT_H_

#include "sql/executor/ob_transmit.h"
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"

namespace oceanbase {

namespace common {
class ObNewRow;
}

namespace share {
namespace schema {
class ObTableSchema;
}
}  // namespace share

namespace sql {
class ObSliceIdxCalc;
class ObPxTransmitInput : public ObPxExchangeInput {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObPxTransmitInput() : ObPxExchangeInput(), ch_provider_ptr_(0)
  {}
  virtual ~ObPxTransmitInput()
  {}
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
  {
    int ret = OB_SUCCESS;
    UNUSED(ctx);
    UNUSED(task_info);
    UNUSED(op);
    return ret;
  }
  void set_sqc_proxy(ObPxSQCProxy& sqc_proxy)
  {
    ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  int get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts);
  int get_data_ch(ObPxTaskChSet& task_ch_set, int64_t timeout_ts);
  int get_parent_dfo_key(dtl::ObDtlDfoKey& key);
  uint64_t get_ch_provider_ptr()
  {
    return ch_provider_ptr_;
  }

protected:
  uint64_t ch_provider_ptr_;
};

// ObPxTransmit is pure base class, no get_type() info provided
class ObPxTransmit : public ObTransmit {
public:
  class ObPxTransmitCtx : public ObTransmitCtx {
  public:
    friend class ObPxTransmit;

  public:
    explicit ObPxTransmitCtx(ObExecContext& ctx);
    virtual ~ObPxTransmitCtx();
    virtual void destroy();
    int init_channel(ObExecContext& ctx, ObPxTransmitInput& trans_input);
    int init_dfc(ObExecContext& ctx, dtl::ObDtlDfoKey& key);

    ObOpMetric& get_op_metric()
    {
      return metric_;
    }
    common::ObIArray<dtl::ObDtlChannel*>& get_task_channels()
    {
      return task_channels_;
    }

  public:
    common::ObArray<dtl::ObDtlChannel*> task_channels_;
    common::ObArenaAllocator px_row_allocator_;
    ObPxTaskChSet task_ch_set_;
    bool transmited_;
    const ObNewRow* first_row_;
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
  };

public:
  explicit ObPxTransmit(common::ObIAllocator& alloc);
  virtual ~ObPxTransmit();
  // This interface only used to calculate the pseudo column of the part_id to which the current row belongs
  void set_partition_id_column_idx(int idx)
  {
    partition_id_idx_ = idx;
  }

protected:
  OB_INLINE bool has_partition_id_column_idx() const
  {
    return partition_id_idx_ != OB_INVALID_INDEX;
  }
  OB_INLINE int get_partition_id_column_idx() const
  {
    return partition_id_idx_;
  }
  virtual int do_transmit(ObExecContext& ctx) const = 0;
  int inner_open(ObExecContext& exec_ctx) const;
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const override;
  int inner_close(ObExecContext& exec_ctx) const;
  static int link_ch_sets(
      ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, dtl::ObDtlFlowControl* dfc = nullptr);
  int send_rows(ObExecContext& exec_ctx, ObPxTransmitCtx& transmit_ctx, ObSliceIdxCalc& slice_calc) const;
  int broadcast_rows(ObExecContext& exec_ctx, ObPxTransmitCtx& transmit_ctx, ObSliceIdxCalc& slice_calc) const;

private:
  int update_row(common::ObNewRow& row, int partition_id_column_idx, common::ObObj& partition_id) const;
  int send_row(ObExecContext& ctx, ObPxTransmitCtx& transmit_ctx, int64_t slice_idx, common::ObObj& partition_id,
      const common::ObNewRow& row, int64_t& time_recorder) const;
  int send_eof_row(ObPxTransmitCtx& transmit_ctx, ObExecContext& ctx) const;
  int broadcast_eof_row(ObPxTransmitCtx& transmit_ctx) const;
  int next_row(ObExecContext& ctx, const common::ObNewRow*& row, ObPxTransmitCtx& transmit_ctx) const;

private:
};

}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQ_OB_PX_TRANSMIT_H_ */
//// end of header file
