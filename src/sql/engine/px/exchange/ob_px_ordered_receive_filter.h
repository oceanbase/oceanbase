/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_RECEIVE_FILTER_H_
#define OCEANBASE_ENGINE_PX_EXCHANGE_OB_PX_ORDERED_RECEIVE_FILTER_H_

#include "sql/dtl/ob_dtl_channel_loop.h"

namespace oceanbase
{
namespace sql
{

class ObOrderedReceiveFilter : public dtl::ObIDltChannelLoopPred
{
public:
  ObOrderedReceiveFilter()
      : data_ch_idx_start_(-1), data_ch_idx_end_(-1), curent_ch_idx_(-1) {}
  ~ObOrderedReceiveFilter() = default;
  // idx range 范围左闭右开: [start_idx, end_idx)
  void set_data_channel_idx_range(int64_t start_idx, int64_t end_idx)
  {
    data_ch_idx_start_ = start_idx;
    data_ch_idx_end_ = end_idx;
    curent_ch_idx_ = 0;
  }
  bool pred_process(int64_t ch_idx, dtl::ObDtlChannel *ch) override
  {
    UNUSED(ch);
    // NOTE: 多个 DFO 的控制信息 channel 创建时间不同，某些可能晚于 ROOT DFO 被调度起来
    //       所以 heap 的范围可能是中间的一段
    return (-1 == data_ch_idx_start_) || /* 还没到接收 ROOT DFO 数据，只接受控制消息阶段 */
        (ch_idx < data_ch_idx_start_) || /* 控制消息 */
        (ch_idx >= data_ch_idx_end_)  || /* 控制消息 */
        (curent_ch_idx_ + data_ch_idx_start_ == ch_idx); /* 预期数据消息 */
  }
  OB_INLINE int64_t get_data_channel_start_idx() { return data_ch_idx_start_; }
  void set_current_ch_idx(int64_t ch_idx) { curent_ch_idx_ = ch_idx; }
private:
  int64_t data_ch_idx_start_;
  int64_t data_ch_idx_end_;
  int64_t curent_ch_idx_;
};

} // end namespace sql
} // end namespace oceanbase

#endif