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

#ifndef OCEANBASE_PALF_CLUSTER_ROLE_CHANGE_SERVICE_
#define OCEANBASE_PALF_CLUSTER_ROLE_CHANGE_SERVICE_
#include "lib/function/ob_function.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_ls_id.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/palf/palf_options.h"
#include "logservice/ob_log_handler.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/applyservice/ob_log_apply_service.h"
#include "logservice/rcservice/ob_role_change_handler.h"
#include "mittest/palf_cluster/logservice/ob_log_client.h"
namespace oceanbase
{
namespace palfcluster
{
enum class RoleChangeEventType {
  INVALID_RC_EVENT_TYPE = 0,
  CHANGE_LEADER_EVENT_TYPE = 1,
  ROLE_CHANGE_CB_EVENT_TYPE = 2,
  MAX_RC_EVENT_TYPE = 3
};

struct RoleChangeEvent {
  RoleChangeEvent() { reset(); }
  RoleChangeEvent(const RoleChangeEventType &event_type,
                  const share::ObLSID &ls_id);
  RoleChangeEvent(const RoleChangeEventType &event_type,
                  const share::ObLSID &ls_id,
                  const common::ObAddr &dst_addr);
  bool is_valid() const;
  void reset();
  bool operator == (const RoleChangeEvent &event) const;
  RoleChangeEventType event_type_;
  share::ObLSID ls_id_;
  ObAddr dst_addr_;
  TO_STRING_KV(K_(event_type), K_(ls_id), K_(dst_addr));
};

class RoleChangeEventSet {
public:
  RoleChangeEventSet();
  ~RoleChangeEventSet();
  int insert(const RoleChangeEvent &event);
  int remove(const RoleChangeEvent &event);
  static constexpr int64_t  MAX_ARRAY_SIZE = 128;
private:
  // Assumed there are sixteen log streams at most.
  // 64 for normal role change events.
  // 64 for leader change events.
  RoleChangeEvent events_[MAX_ARRAY_SIZE];
  mutable ObSpinLock lock_;
  DISALLOW_COPY_AND_ASSIGN(RoleChangeEventSet);
};

class RoleCoordinator : public lib::TGTaskHandler , public palf::PalfRoleChangeCb {
public:
  RoleCoordinator();
  ~RoleCoordinator();
  int init(palfcluster::LogClientMap *log_client_map,
           logservice::ObLogApplyService *apply_service);
  int start();
  void wait();
  void stop();
  void destroy();
  void handle(void *task);
  int on_role_change(const int64_t id) final override;
  int on_need_change_leader(const int64_t ls_id, const common::ObAddr &dst_addr) final override;

private:
  enum class RoleChangeOptType {
    INVALID_RC_OPT_TYPE = 0,
    FOLLOWER_2_LEADER = 1,
    LEADER_2_FOLLOWER = 2,
    FOLLOWER_2_FOLLOWER = 3,
    LEADER_2_LEADER = 4,
    MAX_RC_OPT_TYPE = 5
  };
  enum class RetrySubmitRoleChangeEventReason {
    INVALID_TYPE = 0,
    WAIT_REPLAY_DONE_TIMEOUT = 1,
    MAX_TYPE = 2
  };
  class RetrySubmitRoleChangeEventCtx {
  public:
    RetrySubmitRoleChangeEventCtx() : reason_(RetrySubmitRoleChangeEventReason::INVALID_TYPE) {}
    ~RetrySubmitRoleChangeEventCtx()
    {
      reason_ = RetrySubmitRoleChangeEventReason::INVALID_TYPE;
    }
    bool need_retry() const
    {
      return RetrySubmitRoleChangeEventReason::WAIT_REPLAY_DONE_TIMEOUT == reason_;
    }
    void set_retry_reason(const RetrySubmitRoleChangeEventReason &reason)
    {
      reason_ = reason;
    }
    TO_STRING_KV(K_(reason));
  private:
    RetrySubmitRoleChangeEventReason reason_;
  };
private:
  int submit_role_change_event_(const RoleChangeEvent &event);
  int push_event_into_queue_(const RoleChangeEvent &event);
  int handle_role_change_event_(const RoleChangeEvent &event,
                                RetrySubmitRoleChangeEventCtx &retry_ctx);

  int handle_role_change_cb_event_for_log_handler_(const palf::AccessMode &curr_access_mode,
                                                   ObLogClient *ls,
                                                   RetrySubmitRoleChangeEventCtx &retry_ctx);
  int handle_change_leader_event_for_log_handler_(const common::ObAddr &dst_addr,
                                                  ObLogClient *ls);

  // retval
  //   - OB_SUCCESS
  //   - OB_TIMEOUT, means wait replay finish timeout.
  int switch_follower_to_leader_(const int64_t new_proposal_id,
                                 ObLogClient *ls,
                                 RetrySubmitRoleChangeEventCtx &retry_ctx);
  int switch_leader_to_follower_forcedly_(const int64_t new_proposal_id,
                                          ObLogClient *ls);
  int switch_leader_to_follower_gracefully_(const int64_t new_proposal_id,
                                            const int64_t curr_proposal_id,
                                            const common::ObAddr &dst_addr,
                                            ObLogClient *ls);
  int switch_follower_to_follower_(const int64_t new_proposal_id, ObLogClient *ls);
  int switch_leader_to_leader_(const int64_t new_proposal_id,
                               const int64_t curr_proposal_id,
                               ObLogClient *ls,
                               RetrySubmitRoleChangeEventCtx &retry_ctx);

  // wait replay finish with timeout.
  int wait_replay_service_replay_done_(const share::ObLSID &ls_id,
                                       const palf::LSN &end_lsn,
                                       const int64_t timeout_us);
  int wait_apply_service_apply_done_(const share::ObLSID &ls_id,
                                     palf::LSN &end_lsn);
  int wait_apply_service_apply_done_when_change_leader_(const logservice::ObLogHandler *log_handler,
                                                        const int64_t proposal_id,
                                                        const share::ObLSID &ls_id,
                                                        palf::LSN &end_lsn);
  bool need_execute_role_change(const int64_t curr_proposal_id,
                                const common::ObRole curr_role,
                                const int64_t new_proposal_id,
                                const common::ObRole new_role,
                                const bool is_pending_state,
                                const bool is_offline) const;

  bool is_append_mode(const palf::AccessMode &access_mode) const;
  bool is_raw_write_or_flashback_mode(const palf::AccessMode &access_mode) const;
private:
  RoleChangeOptType get_role_change_opt_type_(const common::ObRole &old_role,
                                              const common::ObRole &new_role,
                                              const bool need_transform_by_access_mode) const;
  // retry submit role change event
  // NB: nowdays, we only support retry submit role change event when wait replay finished
  //     timeout.
  bool need_retry_submit_role_change_event_(int ret) const;
public:
  static const int64_t MAX_THREAD_NUM = 1;
  static const int64_t MAX_RC_EVENT_TASK = 1024 * 1024;
private:
  DISALLOW_COPY_AND_ASSIGN(RoleCoordinator);
private:
  static constexpr int64_t EACH_ROLE_CHANGE_COST_MAX_TIME = 1 * 1000 * 1000;
  static constexpr int64_t WAIT_REPLAY_DONE_TIMEOUT_US = 2 * 1000 * 1000;
  palfcluster::LogClientMap *log_client_map_;
  logservice::ObLogApplyService *apply_service_;
  RoleChangeEventSet rc_set_;
  int tg_id_;
  bool is_inited_;
};
} // end namespace palfcluster
} // end namespce oceanbase
#endif
