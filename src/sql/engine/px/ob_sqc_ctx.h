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
namespace oceanbase {
namespace sql {

// SQC status
class ObSqcCtx {
public:
  ObSqcCtx(ObPxRpcInitSqcArgs& sqc_arg)
      : msg_proc_(sqc_arg, *this),
        receive_data_ch_msg_proc_(msg_proc_),
        transmit_data_ch_msg_proc_(msg_proc_),
        barrier_whole_msg_proc_(msg_proc_),
        winbuf_whole_msg_proc_(msg_proc_),
        interrupt_proc_(msg_proc_),
        sqc_proxy_(*this, sqc_arg),
        all_tasks_finish_(false),
        interrupted_(false),
        temp_table_id_(common::OB_INVALID_ID)
  {}
  ~ObSqcCtx()
  {
    reset();
  }
  common::ObIArray<ObPxTask>& get_tasks()
  {
    return tasks_;
  }
  int reserve_task_mem(int64_t cnt)
  {
    return tasks_.reserve(cnt);
  }
  int add_task(ObPxTask& task, ObPxTask*& task_ptr)
  {
    int ret = common::OB_SUCCESS;
    if (OB_SUCC(tasks_.push_back(task))) {
      task_ptr = &tasks_.at(tasks_.count() - 1);
    }
    return ret;
  }
  int get_task(int64_t idx, ObPxTask*& task_ptr)
  {
    int ret = common::OB_SUCCESS;
    if (idx >= tasks_.count() || idx < 0) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      task_ptr = &tasks_.at(idx);
    }
    return ret;
  }
  void revert_last_task()
  {
    tasks_.pop_back();
  }
  int64_t get_task_count() const
  {
    return tasks_.count();
  }
  void reset()
  {
    for (int i = 0; i < whole_msg_provider_list_.count(); ++i) {
      if (OB_NOT_NULL(whole_msg_provider_list_.at(i))) {
        whole_msg_provider_list_.at(i)->reset();
      }
    }
  }

  void set_temp_table_id(uint64_t temp_table_id)
  {
    temp_table_id_ = temp_table_id;
  }
  uint64_t get_temp_table_id()
  {
    return temp_table_id_;
  }

public:
  int add_whole_msg_provider(uint64_t op_id, ObPxDatahubDataProvider& provider);
  int get_whole_msg_provider(uint64_t op_id, ObPxDatahubDataProvider*& provider);

public:
  common::ObArray<ObPxTask> tasks_;
  ObPxReceiveChProvider receive_data_ch_provider_;
  ObPxTransmitChProvider transmit_data_ch_provider_;
  ObGranulePump gi_pump_;
  dtl::ObDtlChannelLoop msg_loop_;
  ObPxSubCoordMsgProc msg_proc_;
  ObPxReceiveDataChannelMsgP receive_data_ch_msg_proc_;
  ObPxTransmitDataChannelMsgP transmit_data_ch_msg_proc_;
  ObBarrierWholeMsgP barrier_whole_msg_proc_;
  ObWinbufWholeMsgP winbuf_whole_msg_proc_;
  ObPxSqcInterruptedP interrupt_proc_;
  ObPxSQCProxy sqc_proxy_;  // provide message control for each worker
  bool all_tasks_finish_;
  bool interrupted_;  // used for SQC
  common::ObSEArray<ObPxPartitionInfo, 8> partitions_info_;
  common::ObSEArray<ObPxDatahubDataProvider*, 1> whole_msg_provider_list_;
  uint64_t temp_table_id_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqcCtx);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_PX_SQC_CTX_H__ */
//// end of header file
