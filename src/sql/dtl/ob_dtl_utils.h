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

#ifndef OB_DTL_UTILS_H
#define OB_DTL_UTILS_H

#include "sql/dtl/ob_dtl_channel.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase {
namespace sql {
namespace dtl {


class ObPxControlChannelProc : public ObIDltChannelLoopPred
{
public:
  ObPxControlChannelProc()
  {}
  virtual bool pred_process(int64_t idx, ObDtlChannel *chan) override
  {
    UNUSED(idx);
    return nullptr == chan->get_dfc();
  }
};

class ObDtlAsynSender
{
public:
  ObDtlAsynSender(ObIArray<ObDtlChannel*> &channels, ObDtlChTotalInfo *ch_info, bool is_transmit) :
    channels_(channels), ch_info_(ch_info), is_transmit_(is_transmit)
  {}

  int asyn_send();
  int syn_send();
  virtual int action(ObDtlChannel* ch) = 0;

private:
  int calc_batch_buffer_cnt(int64_t &max_batch_size, int64_t &max_loop_cnt);
private:
  ObIArray<ObDtlChannel*> &channels_;
  ObDtlChTotalInfo *ch_info_;
  bool is_transmit_;
};

class ObTransmitEofAsynSender : public ObDtlAsynSender
{
public:
  ObTransmitEofAsynSender(ObIArray<ObDtlChannel*> &channels,
                          ObDtlChTotalInfo *ch_info,
                          bool is_transmit,
                          int64_t timeout_ts,
                          sql::ObEvalCtx *eval_ctx) :
    ObDtlAsynSender(channels, ch_info, is_transmit),
    timeout_ts_(timeout_ts),
    eval_ctx_(eval_ctx)
  {}

 virtual int action(ObDtlChannel *ch);

private:
  int64_t timeout_ts_;
  sql::ObEvalCtx *eval_ctx_;
};


class ObDfcDrainAsynSender : public ObDtlAsynSender
{
public:
  ObDfcDrainAsynSender(ObIArray<ObDtlChannel*> &channels,
                      ObDtlChTotalInfo *ch_info,
                      bool is_transmit,
                      int64_t timeout_ts) :
    ObDtlAsynSender(channels, ch_info, is_transmit),
    timeout_ts_(timeout_ts)
  {}

 virtual int action(ObDtlChannel *ch);

private:
  int64_t timeout_ts_;
};

class ObDfcUnblockAsynSender : public ObDtlAsynSender
{
public:
  ObDfcUnblockAsynSender(ObIArray<ObDtlChannel*> &channels,
                      ObDtlChTotalInfo *ch_info,
                      bool is_transmit,
                      ObDtlFlowControl &dfc) :
    ObDtlAsynSender(channels, ch_info, is_transmit),
    dfc_(dfc),
    unblock_cnt_(0)
  {}

 virtual int action(ObDtlChannel *ch);

 int64_t get_unblocked_cnt() { return unblock_cnt_; }

private:
  ObDtlFlowControl &dfc_;
  int64_t unblock_cnt_;
};

}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_UTILS_H */
