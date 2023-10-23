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

#ifndef OCEANBASE_LOGSERVICE_LOG_MODE_MGR_
#define OCEANBASE_LOGSERVICE_LOG_MODE_MGR_

#include "lib/lock/ob_spin_lock.h"              // SpinRWLock
#include "log_define.h"                         // utils
#include "log_meta_info.h"                      // LogMembershipMeta
#include "log_req.h"                            // LogLearnerReqType
#include "log_simple_member_list.h"             // LogAckList

namespace oceanbase
{
namespace common
{
class ObAddr;
}

namespace palf
{
class LogStateMgr;
class LogEngine;
class LogConfigMgr;
class LogSlidingWindow;

enum ModeChangeState
{
  MODE_INIT = 0,
  MODE_PREPARE = 1,
  MODE_ACCEPT = 2,
  MODE_DONE = 3,
};

class LogModeMgr
{
public:
  LogModeMgr();
  virtual ~LogModeMgr() { destroy(); }
  int init(const int64_t palf_id,
           const common::ObAddr &self,
           const LogModeMeta &log_mode_meta,
           LogStateMgr *state_mgr,
           LogEngine *log_engine,
           LogConfigMgr *config_mgr,
           LogSlidingWindow *sw);
  virtual void destroy();
  virtual void reset_status();
  virtual int get_access_mode(AccessMode &access_mode) const;
  virtual int get_mode_version(int64_t &mode_version) const;
  virtual int get_access_mode(int64_t &mode_version, AccessMode &access_mode) const;
  virtual int get_access_mode_ref_scn(int64_t &mode_version,
                                      AccessMode &access_mode,
                                      share::SCN &ref_scn) const;
  bool can_append() const;
  bool can_raw_write() const;
  bool can_receive_log() const;
  bool is_in_pending_state() const;
  bool can_do_paxos_accept() const;
  virtual bool is_state_changed() const;
  virtual bool need_skip_log_barrier() const;
  virtual LogModeMeta get_accepted_mode_meta() const;
  virtual LogModeMeta get_last_submit_mode_meta() const;
  virtual int reconfirm_mode_meta();
  virtual int change_access_mode(const int64_t mode_version,
                                 const AccessMode &access_mode,
                                 const share::SCN &ref_scn);
  virtual int handle_prepare_response(const common::ObAddr &server,
                                      const int64_t msg_proposal_id,
                                      const int64_t accept_log_proposal_id,
                                      const LSN &last_lsn,
                                      const LogModeMeta &mode_meta);
  virtual bool can_receive_mode_meta(const int64_t proposal_id,
                                     const LogModeMeta &mode_meta,
                                     bool &has_accepted);
  virtual int receive_mode_meta(const common::ObAddr &server,
                                const int64_t proposal_id,
                                const bool is_applied_mode_meta,
                                const LogModeMeta &mode_meta);
  virtual int after_flush_mode_meta(const bool is_applied_mode_meta, const LogModeMeta &mode_meta);
  virtual int ack_mode_meta(const common::ObAddr &server, const int64_t proposal_id);
  virtual int submit_fetch_mode_meta_resp(const common::ObAddr &server,
                                          const int64_t msg_proposal_id,
                                          const int64_t accepted_mode_pid);
  int leader_do_loop_work();
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    common::ObSpinLockGuard guard(lock_);
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(palf_id), K_(self), K_(applied_mode_meta), K_(accepted_mode_meta),
        K_(last_submit_mode_meta), "state", state2str_(state_), K_(new_proposal_id), K_(local_max_lsn),
        K_(local_max_log_pid), K_(max_majority_accepted_pid), K_(max_majority_lsn),
        K_(max_majority_accepted_mode_meta), K_(follower_list), K_(ack_list), K_(majority_cnt),
        K_(last_submit_req_ts), K_(resend_mode_meta_list));
    J_OBJ_END();
    return pos;
  }

private:
  void reset_status_();
  int init_change_mode_();
  int can_change_access_mode_(const int64_t mode_version) const;
  bool is_reach_majority_() const;
  bool can_finish_change_mode_() const;
  bool is_need_retry_() const;
  int switch_state_(const AccessMode &access_mode,
                    const share::SCN &ref_scn,
                    const bool is_reconfirm);
  int submit_prepare_req_(const bool need_inc_pid, const bool need_send_and_handle_prepare);
  int submit_accept_req_(const int64_t proposal_id,
                         const bool is_applied_mode_meta,
                         const LogModeMeta &mode_meta);
  int receive_mode_meta_(const common::ObAddr &server,
                         const int64_t proposal_id,
                         const bool is_applied_mode_meta,
                         const LogModeMeta &mode_meta);
  int set_resend_mode_meta_list_();
  int resend_applied_mode_meta_();

private:
  static const int64_t PREPARE_RETRY_INTERVAL_US = 2 * 1000 * 1000;   // 2s
  const char *state2str_(const ModeChangeState state) const
  {
    #define MODE_CHANGE_TYPE_STR(x) case(ModeChangeState::x): return #x
    switch(state)
    {
      MODE_CHANGE_TYPE_STR(MODE_INIT);
      MODE_CHANGE_TYPE_STR(MODE_PREPARE);
      MODE_CHANGE_TYPE_STR(MODE_ACCEPT);
      MODE_CHANGE_TYPE_STR(MODE_DONE);
      default:
        return "Invalid";
    }
    #undef MODE_CHANGE_TYPE_STR
  }

private:
  mutable common::ObSpinLock lock_;
  bool is_inited_;
  int64_t palf_id_;
  common::ObAddr self_;
  // applied_mode_meta, it has been flushed in majority,
  // external modules could see this meta
  LogModeMeta applied_mode_meta_;
  // NB: protected by SpinLock
  // log_mode_meta has been accepted/flushed
  LogModeMeta accepted_mode_meta_;
  // NB: protected by SpinLock
  // log_mode_meta has been submitted to I/O Worker
  LogModeMeta last_submit_mode_meta_;
  // above LogModeMetas are protected by lock_ in PalfHandleImpl
  // =========access_mode changing state============
  // mode change state
  ModeChangeState state_;
  int64_t leader_epoch_;
  int64_t new_proposal_id_;
  // ack server list
  LogSimpleMemberList ack_list_;
  // all paxos folowers, not include self
  common::ObMemberList follower_list_;
  int64_t majority_cnt_;
  int64_t last_submit_req_ts_;
  LogModeMeta max_majority_accepted_mode_meta_;
  LSN local_max_lsn_;
  int64_t local_max_log_pid_;
  int64_t max_majority_accepted_pid_;
  LSN max_majority_lsn_;
  ResendConfigLogList resend_mode_meta_list_;
  mutable int64_t wait_committed_log_slide_warn_ts_;
  // =========access_mode changing state============
  LogStateMgr *state_mgr_;
  LogEngine *log_engine_;
  LogConfigMgr *config_mgr_;
  LogSlidingWindow *sw_;
};
} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_LOG_MODE_MGR_
