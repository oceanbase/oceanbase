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

#ifndef OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_MODE_MGR_
#define OCEANBASE_UNITTEST_LOGSERVICE_MOCK_CONTAINER_LOG_MODE_MGR_

#define private public
#include "logservice/palf/log_mode_mgr.h"
#undef private

namespace oceanbase
{
namespace palf
{

class MockLogModeMgr : public LogModeMgr
{
public:
  MockLogModeMgr() {}
  virtual ~MockLogModeMgr() {}

  int init(const int64_t palf_id,
           const common::ObAddr &self,
           const LogModeMeta &log_mode_meta,
           LogStateMgr *state_mgr,
           LogEngine *log_engine,
           LogConfigMgr *config_mgr,
           LogSlidingWindow *sw)
  {
    int ret = OB_SUCCESS;
    UNUSED(palf_id);
    UNUSED(self);
    UNUSED(log_mode_meta);
    UNUSED(state_mgr);
    UNUSED(log_engine);
    UNUSED(config_mgr);
    UNUSED(sw);
    return ret;
  }
  void destroy() {}
  void reset_status()
  {}
  int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const
  {
    int ret = OB_SUCCESS;
    access_mode = applied_mode_meta_.access_mode_;
    mode_version = applied_mode_meta_.mode_version_;
    return ret;
  }
  int get_ref_ts_ns(int64_t &mode_version, int64_t &ref_ts_ns) const
  {
    int ret = OB_SUCCESS;
    UNUSEDx(ref_ts_ns, mode_version);
    return ret;
  }
  bool is_state_changed() const
  {
    return false;
  }
  LogModeMeta get_accepted_mode_meta() const
  {
    return mock_accepted_mode_meta_;
  }
  LogModeMeta get_last_submit_mode_meta() const
  {
    return mock_last_submit_mode_meta_;
  }
  int reconfirm_mode_meta()
  {
    int ret = OB_SUCCESS;
    return ret;
  }
  int change_access_mode(const int64_t proposal_id,
                         const AccessMode &access_mode,
                         const int64_t ref_ts_ns,
                         bool &need_role_change)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(proposal_id, access_mode, ref_ts_ns, need_role_change);
    return ret;
  }
  int handle_prepare_response(const common::ObAddr &server,
                              const int64_t proposal_id,
                              const LogModeMeta &mode_meta)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(server, proposal_id, mode_meta);
    return ret;
  }
  bool can_receive_mode_meta(const int64_t proposal_id,
                            const LogModeMeta &mode_meta,
                            bool &has_accepted)
  {
    UNUSEDx(proposal_id, mode_meta, has_accepted);
    return false;
  }
  int receive_mode_meta(const common::ObAddr &server,
                        const int64_t proposal_id,
                        const LogModeMeta &mode_meta)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(server, proposal_id, mode_meta);
    return ret;
  }
  int after_flush_mode_meta(const LogModeMeta &mode_meta)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(mode_meta);
    return ret;
  }
  int ack_mode_meta(const common::ObAddr &server, const int64_t proposal_id)
  {
    int ret = OB_SUCCESS;
    UNUSEDx(server, proposal_id);
    return ret;
  }
public:
  LogModeMeta mock_applied_mode_meta_;
  LogModeMeta mock_accepted_mode_meta_;
  LogModeMeta mock_last_submit_mode_meta_;
};

} // end of palf
} // end of oceanbase

#endif
