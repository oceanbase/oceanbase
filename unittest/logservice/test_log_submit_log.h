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

#include "logservice/palf/i_log_role_change_cb.h"
#include "logservice/palf/i_log_replay_engine.h"
#include "logservice/palf/i_log_apply_service.h"
#include "logservice/palf/i_log_role_change_cb.h"
#include "lib/ob_define.h"
#include <gtest/gtest.h>
namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;
class MockLogCtx : public palf::ILogCtx
{
public:
  explicit MockLogCtx(int64_t cb_id) :cb_id_(cb_id)
  {}
  ~MockLogCtx() {}
  virtual int on_success();
  // 日志未形成多数派时会调用此函数，调用此函数后对象不再使用
  virtual int on_failure();
private:
  int64_t cb_id_;
};
class MockRoleChangeCB : public palf::ILogRoleChangeCB
{
public:
  MockRoleChangeCB() {}
  virtual ~MockRoleChangeCB() {}
public:
  virtual int on_leader_revoke(const int64_t palf_id)
  {
    UNUSED(palf_id);
    return OB_SUCCESS;
  }
  virtual int is_leader_revoke_done(const int64_t palf_id,
                                    bool &is_done) const
  {
    UNUSED(palf_id);
    UNUSED(is_done);
    return OB_SUCCESS;
  }
  virtual int on_leader_takeover(const int64_t palf_id)
  {
    UNUSED(palf_id);
    return OB_SUCCESS;
  }
  virtual int is_leader_takeover_done(const int64_t palf_id,
                                      bool &is_done) const
  {
    UNUSED(palf_id);
    UNUSED(is_done);
    return OB_SUCCESS;
  }
  virtual int on_leader_active(const int64_t palf_id)
  {
    UNUSED(palf_id);
    return OB_SUCCESS;
  }
};
class MockLogReplayEngine : public palf::ILogReplayEngine
{
public:
  MockLogReplayEngine() {}
  virtual ~MockLogReplayEngine() {}
public:
  virtual int is_replay_done(const int64_t palf_id,
                             bool &is_done) const
  {
    UNUSED(palf_id);
    is_done = true;
    return OB_SUCCESS;
  }
  virtual int update_lsn_to_replay(const int64_t palf_id,
                                          const LSN &lsn)
  {
    UNUSED(palf_id);
    UNUSED(lsn);
    return OB_SUCCESS;
  }
};

class MockLogApplyEngine : public palf::ILogApplyEngine
{
public:
  MockLogApplyEngine() {}
  virtual ~MockLogApplyEngine() {}
public:
  virtual int submit_apply_task(logservice::AppendCb *cb)
  {
    UNUSED(cb);
    return OB_SUCCESS;
  }
};
} // end of unittest
} // end of oceanbase
