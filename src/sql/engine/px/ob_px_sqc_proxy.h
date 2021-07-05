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

#ifndef __OB_SQL_PX_SQC_PROXY_H__
#define __OB_SQL_PX_SQC_PROXY_H__

#include "lib/lock/ob_spin_lock.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_task.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"
#include "sql/engine/px/datahub/ob_dh_msg_provider.h"
#include "sql/engine/px/datahub/ob_dh_dtl_proc.h"

namespace oceanbase {
namespace sql {
namespace dtl {
class ObDtlLocalFirstBufferCache;
}  // namespace dtl

class ObSqcLeaderTokenGuard {
public:
  ObSqcLeaderTokenGuard(common::ObSpinLock& lock) : lock_(lock), hold_lock_(false)
  {
    if (common::OB_SUCCESS == lock_.trylock()) {
      hold_lock_ = true;
    }
  }
  ~ObSqcLeaderTokenGuard()
  {
    if (hold_lock_) {
      lock_.unlock();
    }
  }
  bool hold_token() const
  {
    return hold_lock_;
  }

private:
  common::ObSpinLock& lock_;
  bool hold_lock_;
};

class ObSqcCtx;
class ObPxSQCProxy {
public:
  ObPxSQCProxy(ObSqcCtx& sqc_ctx, ObPxRpcInitSqcArgs& arg);
  virtual ~ObPxSQCProxy();

  // basics
  int init();
  void destroy();

  // for transmit op
  int get_part_ch_map(ObPxPartChInfo& map, int64_t timeout_ts);
  int get_transmit_data_ch(const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts, ObPxTaskChSet& task_ch_set,
      dtl::ObDtlChTotalInfo** ch_info);

  // for receive op
  int get_receive_data_ch(int64_t child_dfo_id, const int64_t sqc_id, const int64_t task_id, int64_t timeout_ts,
      ObPxTaskChSet& task_ch_set, dtl::ObDtlChTotalInfo* ch_info);

  // for peek datahub whole msg
  template <class PieceMsg, class WholeMsg>
  int get_dh_msg(uint64_t op_id, const PieceMsg& piece, const WholeMsg*& whole, int64_t timeout_ts);

  int report_task_finish_status(int64_t task_idx, int rc);

  // for root thread
  int check_task_finish_status(int64_t timeout_ts);

  void get_self_dfo_key(dtl::ObDtlDfoKey& key);
  void get_parent_dfo_key(dtl::ObDtlDfoKey& key);

  void set_first_buffer_cache(dtl::ObDtlLocalFirstBufferCache* first_buffer_cache)
  {
    first_buffer_cache_ = first_buffer_cache;
  }
  dtl::ObDtlLocalFirstBufferCache* get_first_buffer_cache()
  {
    return first_buffer_cache_;
  }

  int report(int end_ret) const;

  bool get_transmit_use_interm_result()
  {
    return sqc_arg_.sqc_.transmit_use_interm_result();
  }
  bool get_recieve_use_interm_result()
  {
    return sqc_arg_.sqc_.recieve_use_interm_result();
  }
  int64_t get_dfo_id()
  {
    return sqc_arg_.sqc_.get_dfo_id();
  }

private:
  /* functions */
  int setup_loop_proc(ObSqcCtx& sqc_ctx) const;
  int process_dtl_msg(int64_t timeout_ts);
  int do_process_dtl_msg(int64_t timeout_ts);
  int link_sqc_qc_channel(ObPxRpcInitSqcArgs& sqc_arg);
  int unlink_sqc_qc_channel(ObPxRpcInitSqcArgs& sqc_arg);
  bool need_transmit_channel_map_via_dtl();
  bool need_receive_channel_map_via_dtl(int64_t child_dfo_id);
  int get_whole_msg_provider(uint64_t op_id, ObPxDatahubDataProvider*& provider);
  /* variables */
  ObSqcCtx& sqc_ctx_;
  ObPxRpcInitSqcArgs& sqc_arg_;
  common::ObSpinLock leader_token_lock_;
  dtl::ObDtlLocalFirstBufferCache* first_buffer_cache_;

  common::ObSpinLock dtl_lock_;

  DISALLOW_COPY_AND_ASSIGN(ObPxSQCProxy);
};

template <class PieceMsg, class WholeMsg>
int ObPxSQCProxy::get_dh_msg(uint64_t op_id, const PieceMsg& piece, const WholeMsg*& whole, int64_t timeout_ts)
{
  int ret = common::OB_SUCCESS;
  ObPxDatahubDataProvider* provider = nullptr;
  if (OB_FAIL(get_whole_msg_provider(op_id, provider))) {
    SQL_LOG(WARN, "fail get provider", K(ret));
  } else {
    {
      ObLockGuard<ObSpinLock> lock_guard(dtl_lock_);
      // TODO: LOCK sqc channel
      dtl::ObDtlChannel* ch = sqc_arg_.sqc_.get_sqc_channel();
      if (OB_ISNULL(ch)) {
        ret = common::OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "empty channel", K(ret));
      } else if (OB_FAIL(ch->send(piece, timeout_ts))) {
        SQL_LOG(WARN, "fail push data to channel", K(ret));
      } else if (OB_FAIL(ch->flush())) {
        SQL_LOG(WARN, "fail flush dtl data", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      typename WholeMsg::WholeMsgProvider* p = static_cast<typename WholeMsg::WholeMsgProvider*>(provider);
      int64_t wait_count = 0;
      do {
        ObSqcLeaderTokenGuard guard(leader_token_lock_);
        if (guard.hold_token()) {
          ret = process_dtl_msg(timeout_ts);
          SQL_LOG(DEBUG, "process dtl msg done", K(ret));
        }
        if (OB_SUCC(ret)) {
          const dtl::ObDtlMsg* msg = nullptr;
          if (OB_FAIL(p->get_msg_nonblock(msg, timeout_ts))) {
            SQL_LOG(WARN, "fail get msg", K(timeout_ts), K(ret));
          } else {
            whole = static_cast<const WholeMsg*>(msg);
          }
        }
        if (common::OB_EAGAIN == ret) {
          if (0 == (++wait_count) % 100) {
            SQL_LOG(TRACE, "try to get datahub data repeatly", K(timeout_ts), K(wait_count), K(ret));
          }
          // wait 1000us
          usleep(1000);
        }
      } while (common::OB_EAGAIN == ret);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_PX_SQC_PROXY_H__ */
//// end of header file
