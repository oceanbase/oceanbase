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
#include "sql/engine/px/ob_px_exchange.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_op_metric.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/executor/ob_slice_calc.h"

namespace oceanbase {
namespace sql {

class ObPxTransmitOpInput : public ObPxExchangeOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObPxTransmitOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObPxExchangeOpInput(ctx, spec), ch_provider_ptr_(0)
  {}
  virtual ~ObPxTransmitOpInput()
  {}
  virtual void reset() override
  {}
  virtual int init(ObTaskInfo& task_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }
  void set_sqc_proxy(ObPxSQCProxy& sqc_proxy)
  {
    ch_provider_ptr_ = reinterpret_cast<uint64_t>(&sqc_proxy);
  }
  int get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts);
  int get_data_ch(ObPxTaskChSet& task_ch_set, int64_t timeout_ts, dtl::ObDtlChTotalInfo*& ch_info);
  int get_parent_dfo_key(dtl::ObDtlDfoKey& key);
  uint64_t get_ch_provider_ptr()
  {
    return ch_provider_ptr_;
  }
  uint64_t ch_provider_ptr_;  // use integer for serialize
};

class ObPxTransmitSpec : public ObTransmitSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObPxTransmitSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObPxTransmitSpec()
  {}

  void set_partition_id_column_idx(int idx)
  {
    partition_id_idx_ = idx;
  }

  OB_INLINE bool has_partition_id_column_idx() const
  {
    return partition_id_idx_ != OB_INVALID_INDEX;
  }

  OB_INLINE int32_t get_partition_id_column_idx() const
  {
    return partition_id_idx_;
  }

private:
  // in pdm, partition_id_exprs position of output_exprs
  int32_t partition_id_idx_;
};

class ObPxTransmitOp : public ObTransmitOp {
public:
  ObPxTransmitOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObPxTransmitOp()
  {}

  virtual int inner_open() override;
  virtual int rescan() override
  {
    return ObTransmitOp::rescan();
  }
  virtual void destroy() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;

public:
  int init_channel(ObPxTransmitOpInput& trans_input);
  int init_dfc(dtl::ObDtlDfoKey& key);

  ObOpMetric& get_op_metric()
  {
    return metric_;
  }
  common::ObIArray<dtl::ObDtlChannel*>& get_task_channels()
  {
    return task_channels_;
  }

protected:
  virtual int do_transmit() = 0;
  static int link_ch_sets(
      ObPxTaskChSet& ch_set, common::ObIArray<dtl::ObDtlChannel*>& channels, dtl::ObDtlFlowControl* dfc = nullptr);
  int send_rows(ObSliceIdxCalc& slice_calc);
  int broadcast_rows(ObSliceIdxCalc& slice_calc);

private:
  int update_row(int partition_id_column_idx, int64_t partition_id);
  int send_row(int64_t slice_idx, int64_t& time_recorder, int64_t partition_id);
  int send_eof_row();
  int broadcast_eof_row();
  int next_row();

protected:
  common::ObArray<dtl::ObDtlChannel*> task_channels_;
  common::ObArenaAllocator px_row_allocator_;
  ObPxTaskChSet task_ch_set_;
  bool transmited_;
  // const ObChunkDatum::LastStoredRow<> first_row_;
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
  dtl::ObDtlChTotalInfo* ch_info_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_TRANSMIT_OP_H_
