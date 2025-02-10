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

#ifndef __OB_SQL_ENGINE_PX_DH_MSG_PROVIDER_H__
#define __OB_SQL_ENGINE_PX_DH_MSG_PROVIDER_H__

#include "lib/lock/ob_thread_cond.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/datahub/ob_dh_msg.h"

namespace oceanbase
{
namespace sql
{

class ObPxDatahubDataProvider
{
public:
  ObPxDatahubDataProvider()
      : op_id_(-1), msg_type_(dtl::TESTING), whole_msg_set_(false), dh_msg_cnt_(0), rescan_cnt_(0)
  {
  }
  virtual int get_msg_nonblock(const dtl::ObDtlMsg *&msg, int64_t timeout_ts) = 0;
  inline int init(uint64_t op_id, dtl::ObDtlMsgType msg_type) {
    op_id_ = op_id;
    msg_type_ = msg_type;
    return sqc_level_sync_cond_.init(common::ObWaitEventIds::DH_LOCAL_SYNC_COND_WAIT);
  }
  virtual void reset() {}
  TO_STRING_KV(K_(op_id), K_(msg_type));
  uint64_t op_id_; // 注册本 provider 的算子 id，用于 provder 数组里寻址对应 provider
  dtl::ObDtlMsgType msg_type_;
  bool whole_msg_set_;
  volatile int64_t dh_msg_cnt_;
  volatile int64_t rescan_cnt_;
  common::ObThreadCond sqc_level_sync_cond_;
public:
  static constexpr uint64_t SYNC_WAIT_USEC = 100; // 100 us
} CACHE_ALIGNED;

template <typename PieceMsg, typename WholeMsg>
class ObWholeMsgProvider : public ObPxDatahubDataProvider
{
public:
  ObWholeMsgProvider() {}
  virtual ~ObWholeMsgProvider() = default;
  virtual void reset() override
  {
    whole_msg_.reset();
    sqc_piece_msg_.reset();
  }

  int aggregate_sqc_piece_msgs_and_directly_return_whole(const PieceMsg &piece,
                                                         const WholeMsg *&whole, int64_t timeout_ts,
                                                         int64_t task_cnt, bool need_sync,
                                                         bool need_wait_whole_msg)
  {
    int ret = OB_SUCCESS;
    {
      ObThreadCondGuard guard(sqc_level_sync_cond_);
      if (OB_FAIL(whole_msg_.aggregate_piece(piece))) {
        SQL_ENG_LOG(WARN, "failed to aggregate_piece");
      } else {
        ++dh_msg_cnt_;
        whole_msg_set_ = (dh_msg_cnt_ == task_cnt);
        if (whole_msg_set_ && OB_FAIL(whole_msg_.after_aggregate_piece())) {
          SQL_ENG_LOG(WARN, "fail to do some work after whole msg aggregate piece msg");
        }
      }
    }

    if (OB_FAIL(ret)) {
      // For local datahub message, we also need to do sync_wait if need_wait_whole_msg here,
      // because the whole message will be ready once all local piece gathered.
      // While for rpc datahub message, there is a wait_whole_msg action outside to wait rpc back.
    } else if ((need_sync || need_wait_whole_msg) && OB_FAIL(sync_wait(timeout_ts, task_cnt))) {
      SQL_ENG_LOG(WARN, "failed to sync_wait");
    } else if (need_wait_whole_msg) {
      whole = &whole_msg_;
    }

    return ret;
  }

  int aggregate_sqc_piece_msgs(const PieceMsg &piece, const PieceMsg *&sqc_piece,
                               int64_t timeout_ts, int64_t task_cnt, bool need_sync,
                               bool &is_last_piece)
  {
    int ret = OB_SUCCESS;
    {
      ObThreadCondGuard guard(sqc_level_sync_cond_);
      if (OB_FAIL(sqc_piece_msg_.aggregate_piece(piece))) {
        SQL_ENG_LOG(WARN, "failed to aggregate_piece");
      } else {
        ++dh_msg_cnt_;
      }
      if (OB_SUCC(ret) && (dh_msg_cnt_ == task_cnt)) {
        // only last piece is under obligation to send rpc message
        is_last_piece = true;
        sqc_piece = &sqc_piece_msg_;
      }
    }

    if (OB_FAIL(ret)) {
    } else if ((need_sync) && OB_FAIL(sync_wait(timeout_ts, task_cnt))) {
      SQL_ENG_LOG(WARN, "failed to sync_wait");
    }
    return ret;
  }

  int get_msg_nonblock(const dtl::ObDtlMsg *&msg, int64_t timeout_ts)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(check_status(timeout_ts))) {
      // nop
    } else if (!whole_msg_set_) {
      ret = OB_DTL_WAIT_EAGAIN;
    } else {
      msg = &whole_msg_;
    }
    return ret;
  }
  int add_msg(const WholeMsg &msg)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(whole_msg_.assign(msg))) {
      SQL_ENG_LOG(WARN,"fail assign msg", K(msg), K(ret));
    } else {
      whole_msg_set_ = true;
    }
    return ret;
  }
  bool whole_msg_set()
  {
    return whole_msg_set_;
  }
  TO_STRING_KV(K_(whole_msg_set), K_(whole_msg));
private:
  int check_status(int64_t timeout_ts)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(IS_INTERRUPTED())) {
      // 中断错误处理
      // overwrite ret
      common::ObInterruptCode code = GET_INTERRUPT_CODE();
      ret = code.code_;
      SQL_ENG_LOG(WARN,"received a interrupt", K(code), K(ret));
    } else if (timeout_ts <= common::ObTimeUtility::current_time()) {
      ret = common::OB_TIMEOUT;
      SQL_ENG_LOG(WARN,"timeout and abort", K(timeout_ts), K(ret));
    }
    return ret;
  }

  int sync_wait(int64_t timeout_ts, int64_t task_cnt)
  {
    int ret = common::OB_SUCCESS;
    int64_t loop_time = 0;
    while (OB_SUCC(ret)) {
      {
        ObThreadCondGuard guard(sqc_level_sync_cond_);
        if (dh_msg_cnt_ == task_cnt) {
          break;
        }
        // wait for timeout or until notified.
        sqc_level_sync_cond_.wait_us(SYNC_WAIT_USEC);
        if (dh_msg_cnt_ == task_cnt) {
          break;
        }
      }
      ++loop_time;
      // check status each 256 loops
      if (((loop_time & 0xFF) == 0) && OB_FAIL(check_status(timeout_ts))) {
        loop_time = 0;
      }
    }
    return ret;
  }

private:
  WholeMsg whole_msg_;
  PieceMsg sqc_piece_msg_;
};


}
}
#endif /* __OB_SQL_ENGINE_PX_DH_MSG_PROVIDER_H__ */
//// end of header file
