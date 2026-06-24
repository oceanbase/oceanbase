/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_RECEIVE_OP_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_RECEIVE_OP_H_

#include "sql/engine/px/exchange/ob_px_ordered_receive_filter.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
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
class ObPxOrderedReceiveOpInput : public ObPxReceiveOpInput
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObPxOrderedReceiveOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxReceiveOpInput(ctx, spec)
  {}
  virtual ~ObPxOrderedReceiveOpInput()
  {}
};

class ObPxOrderedReceiveSpec : public ObPxReceiveSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxOrderedReceiveSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObPxReceiveSpec(alloc, type)
  {}
  ~ObPxOrderedReceiveSpec() {}
};

class ObPxOrderedReceiveOp : public ObPxReceiveOp
{
public:
  ObPxOrderedReceiveOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObPxOrderedReceiveOp() {}

protected:
  virtual int inner_open() override;
  virtual void destroy() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int try_link_channel() override;

private:
  int setup_readers();
  void reset_readers();
  void destroy_readers();
  int next_row(ObReceiveRowReader &reader, bool &wait_next_msg);
  int next_rows(ObReceiveRowReader &reader, int64_t max_row_cnt, int64_t &read_rows);

private:
  ObReceiveRowReader *readers_;
  ObOrderedReceiveFilter receive_order_;
  int64_t reader_cnt_;
  int64_t channel_idx_;
  int64_t finish_ch_cnt_;
  bool all_rows_finish_;
  ObPxInterruptP interrupt_proc_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_RECEIVE_OP_H_
