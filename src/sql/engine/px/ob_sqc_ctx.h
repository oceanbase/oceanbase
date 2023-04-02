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

#ifndef __OB_SQL_PX_SQC_CTX_H__ 
#define __OB_SQL_PX_SQC_CTX_H__


#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_coord_msg_proc.h"
#include "sql/engine/px/ob_px_data_ch_provider.h"
#include "sql/engine/px/ob_granule_pump.h"
#include "sql/engine/px/ob_px_dtl_proc.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/datahub/components/ob_dh_barrier.h"
#include "sql/engine/px/datahub/components/ob_dh_winbuf.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"
#include "sql/engine/px/datahub/components/ob_dh_sample.h"
#include "sql/engine/px/datahub/components/ob_dh_range_dist_wf.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"
#include "sql/engine/px/datahub/components/ob_dh_second_stage_reporting_wf.h"
#include "sql/dtl/ob_dtl_msg_type.h"
#include "sql/engine/px/datahub/components/ob_dh_opt_stats_gather.h"

namespace oceanbase
{
namespace sql
{

// SQC 状态
class ObSqcCtx
{
public:
  ObSqcCtx(ObPxRpcInitSqcArgs &sqc_arg);
  ~ObSqcCtx() {  reset(); }
  common::ObIArray<ObPxTask> &get_tasks() { return tasks_; }
  // 为了确保 add_task 不会因为内存问题失败。因为一旦失败可能导致启动的 task 未记录
  int reserve_task_mem(int64_t cnt) { return tasks_.reserve(cnt); }
  int add_task(ObPxTask &task, ObPxTask *&task_ptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(tasks_.push_back(task))) {
      task_ptr = &tasks_.at(tasks_.count() - 1);
    }
    return ret;
  }
  int get_task(int64_t idx, ObPxTask *&task_ptr)
  {
    int ret = common::OB_SUCCESS;
    if (idx >= tasks_.count() || idx < 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      task_ptr = &tasks_.at(idx);
    }
    return ret;
  }
  // 更新最后一个 task 的协程 id
  void revert_last_task() { tasks_.pop_back(); }
  int64_t get_task_count() const { return tasks_.count(); }
  void reset() 
  {
    for (int i = 0; i < whole_msg_provider_list_.count(); ++i) {
      if (OB_NOT_NULL(whole_msg_provider_list_.at(i))) {
        whole_msg_provider_list_.at(i)->reset();
      }
    }
  }

public:
  // sqc 启动时将子计划中所有用到 datahub 的算子都注册到这里，用于监听 whole 消息
  int add_whole_msg_provider(uint64_t op_id, dtl::ObDtlMsgType msg_type, ObPxDatahubDataProvider &provider);
  int get_whole_msg_provider(uint64_t op_id, dtl::ObDtlMsgType msg_type, ObPxDatahubDataProvider *&provider);
  // when sqc init, register all init channel msg operator id in it
  int get_init_channel_msg_cnt(uint64_t op_id, int64_t *&curr_piece_cnt);
public:
  common::ObArray<ObPxTask> tasks_;
  ObGranulePump gi_pump_;
  dtl::ObDtlChannelLoop msg_loop_;
  ObPxSubCoordMsgProc msg_proc_;
  ObPxReceiveDataChannelMsgP receive_data_ch_msg_proc_;
  ObPxTransmitDataChannelMsgP transmit_data_ch_msg_proc_;
  ObBarrierWholeMsgP barrier_whole_msg_proc_;
  ObWinbufWholeMsgP winbuf_whole_msg_proc_;
  ObDynamicSampleWholeMsgP sample_whole_msg_proc_;
  ObRollupKeyWholeMsgP rollup_key_whole_msg_proc_;
  ObRDWFWholeMsgP rd_wf_whole_msg_proc_;
  ObInitChannelWholeMsgP init_channel_whole_msg_proc_;
  ObReportingWFWholeMsgP reporting_wf_piece_msg_proc_;
  ObPxSqcInterruptedP interrupt_proc_;
  ObPxSQCProxy sqc_proxy_; // 给各个 worker 提供控制消息通信服务
  ObPxReceiveChProvider receive_data_ch_provider_;
  ObPxTransmitChProvider transmit_data_ch_provider_;
  bool all_tasks_finish_;
  bool interrupted_; // 标记当前 SQC 是否被 QC 中断
  common::ObSEArray<ObPxTabletInfo, 8> partitions_info_;
  ObPxBloomfilterChProvider bf_ch_provider_;
  ObPxCreateBloomFilterChannelMsgP px_bloom_filter_msg_proc_;
  ObOptStatsGatherWholeMsgP opt_stats_gather_whole_msg_proc_;
  // 用于 datahub 中保存 whole msg provider，一般情况下一个子计划里不会
  // 超过一个算子会使用 datahub，所以大小默认为 1 即可
  common::ObSEArray<ObPxDatahubDataProvider *, 1> whole_msg_provider_list_;
  common::ObSEArray<std::pair<int64_t, int64_t>, 1> init_channel_msg_cnts_; // <op_id, piece_cnt>
private:
  DISALLOW_COPY_AND_ASSIGN(ObSqcCtx);
};

}
}
#endif /* __OB_SQL_PX_SQC_CTX_H__ */
//// end of header file


