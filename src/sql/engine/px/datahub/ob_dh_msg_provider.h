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
      : op_id_(-1), msg_type_(dtl::TESTING), msg_set_(false), dh_msg_cnt_(0), rescan_cnt_(0)
  {
  }
  virtual int get_msg_nonblock(const dtl::ObDtlMsg *&msg, int64_t timeout_ts) = 0;
  virtual void reset() {}
  TO_STRING_KV(K_(op_id), K_(msg_type));
  uint64_t op_id_; // 注册本 provider 的算子 id，用于 provder 数组里寻址对应 provider
  dtl::ObDtlMsgType msg_type_;
  bool msg_set_;
  volatile int64_t dh_msg_cnt_;
  volatile int64_t rescan_cnt_;
};

template <typename T>
class ObWholeMsgProvider : public ObPxDatahubDataProvider
{
public:
  ObWholeMsgProvider() {}
  virtual ~ObWholeMsgProvider() = default;
  virtual void reset() override { msg_.reset();}
  int get_msg_nonblock(const dtl::ObDtlMsg *&msg, int64_t timeout_ts)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(check_status(timeout_ts))) {
      // nop
    } else if (!msg_set_) {
      ret = OB_EAGAIN;
    } else {
      msg = &msg_;
    }
    return ret;
  }
  int add_msg(const T &msg)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(msg_.assign(msg))) {
      SQL_ENG_LOG(WARN,"fail assign msg", K(msg), K(ret));
    } else {
      msg_set_ = true;
    }
    return ret;
  }
  bool msg_set()
  {
    return msg_set_;
  }
  TO_STRING_KV(K_(msg_set), K_(msg));
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
private:
  T msg_;
  common::ObThreadCond msg_ready_cond_;
};


}
}
#endif /* __OB_SQL_ENGINE_PX_DH_MSG_PROVIDER_H__ */
//// end of header file
