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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_RECONFIRM_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_RECONFIRM_
#include "logservice/palf/log_reconfirm.h"

namespace oceanbase
{
namespace palf
{

class MockLogReconfirm : public LogReconfirm
{
public:
  MockLogReconfirm() : mock_ret_(OB_SUCCESS) {}
  ~MockLogReconfirm() {}

  int init(const int64_t palf_id,
           const ObAddr &self,
           LogSlidingWindow *sw,
           LogStateMgr *state_mgr,
           LogConfigMgr *mm,
           LogEngine *log_engine)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_id);
    UNUSED(self);
    UNUSED(sw);
    UNUSED(state_mgr);
    UNUSED(mm);
    UNUSED(log_engine);
    return ret;
  }
  void destroy() {}
  void reset_state() {}
  bool need_start_up()
  {
    return false;
  }
  int reconfirm()
  {
    return mock_ret_;
  }
  int handle_prepare_response(const common::ObAddr &server,
                              const int64_t &src_proposal_id,
                              const int64_t &accept_proposal_id,
                              const LSN &last_lsn)
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(src_proposal_id);
    UNUSED(accept_proposal_id);
    UNUSED(last_lsn);
    return ret;
  }
  int receive_log(const common::ObAddr &server,
                  const LSN &prev_lsn,
                  const int64_t &prev_proposal_id,
                  const LSN &lsn,
                  const char *buf,
                  const int64_t buf_len)
  {
    int ret = OB_SUCCESS;
    UNUSED(server);
    UNUSED(prev_lsn);
    UNUSED(prev_proposal_id);
    UNUSED(lsn);
    UNUSED(buf);
    UNUSED(buf_len);
    return ret;
  }
public:
  int mock_ret_;
};

} // end of palf
} // end of oceanbase

#endif
