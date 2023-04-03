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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/ob_sqc_ctx.h"
#include "lib/lock/ob_spin_lock.h"
#include "sql/engine/px/datahub/components/ob_dh_init_channel.h"

using namespace oceanbase::sql;

ObSqcCtx::ObSqcCtx(ObPxRpcInitSqcArgs &sqc_arg) : msg_loop_(),
      msg_proc_(sqc_arg, *this),
      receive_data_ch_msg_proc_(msg_proc_),
      transmit_data_ch_msg_proc_(msg_proc_),
      barrier_whole_msg_proc_(msg_proc_),
      winbuf_whole_msg_proc_(msg_proc_),
      sample_whole_msg_proc_(msg_proc_),
      rollup_key_whole_msg_proc_(msg_proc_),
      rd_wf_whole_msg_proc_(msg_proc_),
      init_channel_whole_msg_proc_(msg_proc_),
      reporting_wf_piece_msg_proc_(msg_proc_),
      interrupt_proc_(msg_proc_),
      sqc_proxy_(*this, sqc_arg),
      receive_data_ch_provider_(sqc_proxy_.get_msg_ready_cond()),
      transmit_data_ch_provider_(sqc_proxy_.get_msg_ready_cond()),
      all_tasks_finish_(false),
      interrupted_(false),
      bf_ch_provider_(sqc_proxy_.get_msg_ready_cond()),
      px_bloom_filter_msg_proc_(msg_proc_),
      opt_stats_gather_whole_msg_proc_(msg_proc_){}

int ObSqcCtx::add_whole_msg_provider(uint64_t op_id, dtl::ObDtlMsgType msg_type, ObPxDatahubDataProvider &provider)
{
  provider.op_id_ = op_id;
  provider.msg_type_ = msg_type;
  return whole_msg_provider_list_.push_back(&provider);
}

int ObSqcCtx::get_whole_msg_provider(uint64_t op_id, dtl::ObDtlMsgType msg_type, ObPxDatahubDataProvider *&provider)
{
  int ret = OB_SUCCESS;
  provider = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < whole_msg_provider_list_.count(); ++i) {
    if (OB_ISNULL(whole_msg_provider_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should never be nullptr, unexpected", K(ret));
    } else if (op_id == whole_msg_provider_list_.at(i)->op_id_
               && msg_type == whole_msg_provider_list_.at(i)->msg_type_) {
      provider = whole_msg_provider_list_.at(i);
      break;
    }
  }
  // 预期在 sqc 启动时就要遍历算子并注册好 provider
  if (nullptr == provider) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have a whole msg provider for op", K(op_id), K(ret), K(msg_type));
  }
  return ret;
}

int ObSqcCtx::get_init_channel_msg_cnt(uint64_t op_id, int64_t *&curr_piece_cnt)
{
  int ret = OB_SUCCESS;
  curr_piece_cnt = nullptr;
  for (int64_t i = 0; i < init_channel_msg_cnts_.count(); ++i) {
    if (op_id == init_channel_msg_cnts_.at(i).first) {
      curr_piece_cnt = &init_channel_msg_cnts_.at(i).second;
      break;
    }
  }
  if (nullptr == curr_piece_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have a init channel msg cnt for op", K(ret), K(op_id));
  }
  return ret;
}
