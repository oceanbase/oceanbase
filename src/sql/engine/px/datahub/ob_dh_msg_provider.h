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

namespace oceanbase {
namespace sql {

class ObPxDatahubDataProvider {
public:
  virtual int get_msg_nonblock(const dtl::ObDtlMsg*& msg, int64_t timeout_ts) = 0;
  virtual void reset()
  {}
  TO_STRING_KV(K_(op_id));
  uint64_t op_id_;
};

template <typename T>
class ObWholeMsgProvider : public ObPxDatahubDataProvider {
public:
  ObWholeMsgProvider() : msg_set_(false)
  {}
  virtual ~ObWholeMsgProvider() = default;
  virtual void reset() override
  {
    msg_.reset();
  }
  int get_msg_nonblock(const dtl::ObDtlMsg*& msg, int64_t timeout_ts)
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
  int add_msg(const T& msg)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(msg_.assign(msg))) {
      SQL_ENG_LOG(WARN, "fail assign msg", K(msg), K(ret));
    } else {
      msg_set_ = true;
    }
    return ret;
  }
  TO_STRING_KV(K_(msg_set), K_(msg));

private:
  int check_status(int64_t timeout_ts)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(IS_INTERRUPTED())) {
      // overwrite ret
      common::ObInterruptCode code = GET_INTERRUPT_CODE();
      ret = code.code_;
      SQL_ENG_LOG(WARN, "received a interrupt", K(code), K(ret));
    } else if (timeout_ts <= common::ObTimeUtility::current_time()) {
      ret = common::OB_TIMEOUT;
      SQL_ENG_LOG(WARN, "timeout and abort", K(timeout_ts), K(ret));
    }
    return ret;
  }

private:
  bool msg_set_;
  T msg_;
  common::ObThreadCond msg_ready_cond_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_ENGINE_PX_DH_MSG_PROVIDER_H__ */
//// end of header file
