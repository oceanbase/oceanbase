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

#ifndef __OB_SQL_ENGINE_PX_DATA_CH_PROVIDER_H__
#define __OB_SQL_ENGINE_PX_DATA_CH_PROVIDER_H__

#include "lib/lock/ob_thread_cond.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/dtl/ob_dtl_task.h"

namespace oceanbase
{
namespace sql
{

class ObPxChProviderUtil
{
public:
  static int inner_get_data_ch(
        ObPxTaskChSets &ch_sets,
        dtl::ObDtlChTotalInfo &ch_total_info,
        const int64_t sqc_id,
        const int64_t task_id,
        ObPxTaskChSet &ch_set,
        bool is_transmit);
  static int check_status(int64_t timeout_ts,
                          const common::ObAddr &qc_addr,
                          int64_t query_start_time);
};

class ObPxTransmitChProvider
{
public:
  ObPxTransmitChProvider(ObThreadCond &msg_ready_cond) : msg_set_(false), msg_ready_cond_(msg_ready_cond) {}
  virtual ~ObPxTransmitChProvider() = default;
  int init();
  int get_data_ch(const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts, ObPxTaskChSet &ch_set,
                  dtl::ObDtlChTotalInfo **ch_info);
  int get_data_ch_nonblock(const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts,
            ObPxTaskChSet &ch_set, dtl::ObDtlChTotalInfo **ch_info, const common::ObAddr &qc_addr,
            int64_t query_start_time);
  int get_part_ch_map(ObPxPartChInfo &map, int64_t timeout_ts);
  int get_part_ch_map_nonblock(ObPxPartChInfo &map, int64_t timeout_ts,
                               const common::ObAddr &qc_addr, int64_t query_start_time);
  int add_msg(const ObPxTransmitDataChannelMsg &msg);
private:
  int wait_msg(int64_t timeout_ts);
  int inner_get_part_ch_map(ObPxPartChInfo &map);
private:
  bool msg_set_;
  ObPxTransmitDataChannelMsg msg_;
  common::ObThreadCond &msg_ready_cond_;
};

class ObPxReceiveChProvider
{
public:
  ObPxReceiveChProvider(ObThreadCond &msg_ready_cond)
  : msg_ready_cond_(msg_ready_cond),
    lock_(common::ObLatchIds::DTL_RECV_CHANNEL_PROVIDER_LOCK)
  {
  }
  virtual ~ObPxReceiveChProvider() = default;
  int init();
  // 如果 child_dfo_id 的 ch sets 不存在，则等待，由 add_msg 唤醒。
  int get_data_ch(const int64_t child_dfo_id,
                  const int64_t sqc_id,
                  const int64_t task_id,
                  int64_t timeout_ts,
                  ObPxTaskChSet &ch_set,
                  dtl::ObDtlChTotalInfo *ch_info);
  int get_data_ch_nonblock(const int64_t child_dfo_id,
                          const int64_t sqc_id,
                          const int64_t task_id,
                          int64_t timeout_ts,
                          ObPxTaskChSet &ch_set,
                          dtl::ObDtlChTotalInfo *ch_info,
                          const common::ObAddr &qc_addr,
                          int64_t query_start_time);
  int add_msg(const ObPxReceiveDataChannelMsg &msg);
private:
  /* functions */
  int wait_msg(int64_t child_dfo_id, int64_t timeout_ts);
  int reserve_msg_set_array_size(int64_t size);
private:
  static const int64_t MSG_SET_DEFAULT_SIZE = 16;
private:
  /* variables */
  common::ObSEArray<ObPxReceiveDataChannelMsg, 2> msgs_;
  common::ObThreadCond &msg_ready_cond_;
  common::ObArray<bool> msg_set_;
  common::ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(ObPxReceiveChProvider);
};

// Root Dfo 专用的 Provider
class ObPxRootReceiveChProvider
{
public:
  ObPxRootReceiveChProvider() : root_dfo_(NULL) {}
  ~ObPxRootReceiveChProvider() = default;
  void set_root_dfo(ObDfo &root_dfo)
  {
    root_dfo_ = &root_dfo;
  }
  int get_data_ch(const int64_t child_dfo_id,
                  ObPxTaskChSets &ch_sets,
                  int64_t timeout_ts)
  {
    int ret = OB_SUCCESS;
    UNUSED(timeout_ts);
    if (OB_ISNULL(root_dfo_)) {
      ret = common::OB_NOT_INIT;
    } else {
      ret = root_dfo_->get_task_receive_chs(child_dfo_id, ch_sets);
    }
    return ret;
  }
  void reset() { root_dfo_ = nullptr; }
private:
  ObDfo *root_dfo_;
};

class ObPxBloomfilterChProvider
{
public:
  ObPxBloomfilterChProvider(ObThreadCond &msg_ready_cond) : msg_set_(false), msg_ready_cond_(msg_ready_cond) {}
  virtual ~ObPxBloomfilterChProvider() = default;
  int init();
  int get_data_ch_nonblock(
        ObPxBloomFilterChSet &ch_set,
        int64_t &sqc_count,
        int64_t timeout_ts,
        bool is_transmit,
        const common::ObAddr &qc_addr,
        int64_t query_start_time);
  int add_msg(const ObPxCreateBloomFilterChannelMsg &msg);
  int wait_msg(int64_t timeout_ts);
private:
  bool msg_set_;
  ObPxCreateBloomFilterChannelMsg msg_;
  common::ObThreadCond &msg_ready_cond_;
};

}
}
#endif /* __OB_SQL_ENGINE_PX_DATA_CH_PROVIDER_H__ */
//// end of header file
