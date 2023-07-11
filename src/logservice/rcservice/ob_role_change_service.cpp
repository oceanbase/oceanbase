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

#include "ob_role_change_service.h"
#include "common/ob_role.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/thread/thread_mgr.h"
#include "logservice/palf/log_define.h"
#include "share/ob_errno.h"
#include "share/ob_ls_id.h"
#include "share/ob_thread_define.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ob_occam_time_guard.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace logservice;
using namespace share;
namespace logservice
{

RoleChangeEvent::RoleChangeEvent(const RoleChangeEventType &event_type,
                                 const share::ObLSID &ls_id) : event_type_(event_type),
                                                               ls_id_(ls_id)
{
}
RoleChangeEvent::RoleChangeEvent(const RoleChangeEventType &event_type,
                                 const share::ObLSID &ls_id,
                                 const common::ObAddr &dst_addr) : event_type_(event_type),
                                                                   ls_id_(ls_id),
                                                                   dst_addr_(dst_addr)
{
}

bool RoleChangeEvent::is_valid() const
{
  return RoleChangeEventType::INVALID_RC_EVENT_TYPE != event_type_
         && false != ls_id_.is_valid();
}

void RoleChangeEvent::reset()
{
  event_type_ = RoleChangeEventType::INVALID_RC_EVENT_TYPE;
  ls_id_.reset();
  dst_addr_.reset();
}

bool RoleChangeEvent::operator==(const RoleChangeEvent &rhs) const
{
  // for change leader event, we just check 'ls_id'.
  return event_type_ == rhs.event_type_ && ls_id_ == rhs.ls_id_;
}

RoleChangeEventSet::RoleChangeEventSet()
{}

RoleChangeEventSet::~RoleChangeEventSet()
{}

int RoleChangeEventSet::insert(const RoleChangeEvent &event)
{
  int ret = OB_SUCCESS;
  int64_t free_idx = -1;
  ObSpinLockGuard guard(lock_);
  for (int64_t i = 0; i < MAX_ARRAY_SIZE; i++) {
    if (event == events_[i]) {
      ret = OB_ENTRY_EXIST;
    }
  }
  for (int64_t i = 0; i < MAX_ARRAY_SIZE && -1 == free_idx && OB_SUCC(ret); i++) {
    if (false == events_[i].is_valid()) {
      free_idx = i;
    }
  }
  if (OB_ENTRY_EXIST == ret) {
  } else if (-1 != free_idx) {
    events_[free_idx] = event;
  } else {
    ret = OB_SIZE_OVERFLOW;
  }
  CLOG_LOG(INFO, "insert event into set success", K(ret), K(event), K(free_idx));
  return ret;
}

int RoleChangeEventSet::remove(const RoleChangeEvent &event)
{
  int ret = OB_SUCCESS;
  int64_t delete_idx = -1;
  ObSpinLockGuard guard(lock_);
  for (int64_t i = 0; i < MAX_ARRAY_SIZE && -1 == delete_idx; i++) {
    if (event == events_[i]) {
      delete_idx = i;
    }
  };
  if (-1 != delete_idx) {
    events_[delete_idx].reset();
  } else {
    ret = OB_ENTRY_NOT_EXIST;
  }
  CLOG_LOG(INFO, "remove slog from set success", K(ret), K(delete_idx), K(event));
  return ret;
}

ObRoleChangeService::ObRoleChangeService() : ls_service_(NULL),
                                             apply_service_(NULL),
                                             replay_service_(NULL),
                                             tg_id_(-1),
                                             cur_task_info_(),
                                             is_inited_(false)
{
}

ObRoleChangeService::~ObRoleChangeService()
{
  if (IS_INIT) {
    destroy();
  }
}

int ObRoleChangeService::init(storage::ObLSService *ls_service,
                              logservice::ObLogApplyService *apply_service,
                              logservice::ObILogReplayService *replay_service)
{
  int ret = OB_SUCCESS;
  const int tg_id = lib::TGDefIDs::RCService;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(ls_service) || OB_ISNULL(apply_service) || OB_ISNULL(replay_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(ls_service), KP(apply_service), KP(replay_service));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    CLOG_LOG(WARN, "ObRoleChangeService TG_CREATE failed", K(ret));
  } else {
    cur_task_info_.reset();
    ls_service_ = ls_service;
    apply_service_ = apply_service;
    replay_service_ = replay_service;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObRoleChangeService init success", K(ret), K(tg_id_), KP(ls_service), KP(apply_service), KP(replay_service));
  }
  return ret;
}

int ObRoleChangeService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    CLOG_LOG(WARN, "ObRoleChangeService start failed", K(ret), K(tg_id_));
  } else {
    CLOG_LOG(INFO, "ObRoleChangeService start success", K(ret), K(tg_id_));
  }
  return ret;
}

void ObRoleChangeService::wait()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
    TG_WAIT(tg_id_);
  }
  CLOG_LOG(INFO, "ObRoleChangeService wait finish", K(tg_id_));
}

void ObRoleChangeService::stop()
{
  if (IS_INIT) {
    TG_STOP(tg_id_);
  }
  CLOG_LOG(INFO, "ObRoleChangeService stop finish", K(tg_id_));
}

void ObRoleChangeService::destroy()
{
  if (IS_INIT) {
    (void)stop();
    (void)wait();
    TG_DESTROY(tg_id_);
    is_inited_ = false;
    cur_task_info_.reset();
    tg_id_ = -1;
    ls_service_ = NULL;
    apply_service_ = NULL;
    replay_service_ = NULL;
    CLOG_LOG(INFO, "ObRoleChangeService destroy success");
  }
}

void ObRoleChangeService::handle(void *task)
{
  int ret = OB_SUCCESS;
  // When role chage service hang exceeds 30 seconds, we think there is dead lock in 'handle_role_change_event_',
  // TIMEGUARD will pring lbt().
  TIMEGUARD_INIT(CLOG, 30_s, 30_s);
  RoleChangeEvent *event = reinterpret_cast<RoleChangeEvent*>(task);
  const int64_t ls_id = event->ls_id_.id();
  const int64_t start_ts = ObTimeUtility::current_time();
  RetrySubmitRoleChangeEventCtx retry_ctx;
  CLOG_LOG(INFO, "begin handle_role_change_event_", "sequence:", start_ts, KPC(event));
  if (NULL == event) {
    CLOG_LOG(WARN, "unexpected error, task is nullptr", KP(event));
  } else if (OB_FAIL(handle_role_change_event_(*event, retry_ctx))) {
    CLOG_LOG(WARN, "handle_role_change_event_ failed", K(ret), KPC(event), K(retry_ctx));
  } else {
    CLOG_LOG(INFO, "end handle_role_change_event_", "sequence:", start_ts, KPC(event));
  }
  if (NULL != event) {
    OB_DELETE(RoleChangeEvent, "RCService", event);
  }
  if (retry_ctx.need_retry() && OB_FAIL(on_role_change(ls_id))) {
    CLOG_LOG(WARN, "retry submit role change event failed", K(ls_id), K(retry_ctx));
  }
}

int ObRoleChangeService::on_role_change(const int64_t id)
{
  int ret = OB_SUCCESS;
  share::ObLSID ls_id(id);
  RoleChangeEvent event(RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE, ls_id);
  // TODO by runlin: if task queue has been full, push task will be failed, the role change event
  // will be lost.
  if (OB_FAIL(submit_role_change_event_(event))) {
    CLOG_LOG(WARN, "submit_role_change_event_ failed", K(ret), K(event));
  } else {
    CLOG_LOG(INFO, "on_role_change success", K(ret), K(event));
  }
  return ret;
}

int ObRoleChangeService::on_need_change_leader(const int64_t ls_id, const common::ObAddr &dst_addr)
{
  int ret = OB_SUCCESS;
  RoleChangeEvent event(RoleChangeEventType::CHANGE_LEADER_EVENT_TYPE, share::ObLSID(ls_id), dst_addr);
  if (OB_FAIL(submit_role_change_event_(event))) {
    CLOG_LOG(WARN, "submit_role_change_event_ failed", K(ret), K(event));
  } else {
    CLOG_LOG(INFO, "change_leader success", K(ret), K(event));
  }
  return ret;
}

int ObRoleChangeService::submit_role_change_event_(const RoleChangeEvent &event)
{
  int ret = OB_SUCCESS;
  if(OB_FAIL(rc_set_.insert(event)) && OB_ENTRY_EXIST != ret) {
    CLOG_LOG(ERROR, "insert into rc_set failed", K(ret), K(event));
  } else if (OB_ENTRY_EXIST == ret) {
    CLOG_LOG(INFO, "repeat role change event, filter it", K(ret), K(event));
    ret = OB_SUCCESS;
  } else if (OB_FAIL(push_event_into_queue_(event))) {
    CLOG_LOG(WARN, "push_event_into_queue_ failed", K(ret), K(event));
  } else {
    CLOG_LOG(INFO, "submit_role_change_event_ success", K(ret), K(event));
  }
  return ret;
}

// TODO: use poll to avoid alloc memory failed.
int ObRoleChangeService::push_event_into_queue_(const RoleChangeEvent &event)
{
  int ret = OB_SUCCESS;
  RoleChangeEvent *rc_event = NULL;

  int64_t warn_time = OB_INVALID_TIMESTAMP;
  do {
    if (NULL == (rc_event =
          MTL_NEW(RoleChangeEvent, "RCService", event.event_type_, event.ls_id_, event.dst_addr_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (palf_reach_time_interval(1 * 1000 * 1000, warn_time)) {
        CLOG_LOG(WARN, "allocate memory failed", K(ret), K(event));
      }
      usleep(1 * 1000);
    } else {
      ret = OB_SUCCESS;
    }
  } while(OB_FAIL(ret));

  if (OB_FAIL(TG_PUSH_TASK(tg_id_, rc_event))) {
    CLOG_LOG(WARN, "ObRoleChangeTask push task failed", K(ret), K(event));
  }
  if (OB_FAIL(ret) && NULL != rc_event) {
    MTL_DELETE(RoleChangeEvent, "RCService", rc_event);
  }
  return ret;
}

int ObRoleChangeService::handle_role_change_event_(const RoleChangeEvent &event,
                                                   RetrySubmitRoleChangeEventCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  AccessMode curr_access_mode;
  int64_t unused_mode_version;
  OB_ASSERT(OB_SUCCESS == rc_set_.remove(event));
  if (false == event.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(event));
  } else if (OB_FAIL(ls_service_->get_ls(event.ls_id_, ls_handle, ObLSGetMod::LOG_MOD))
             || NULL == (ls = ls_handle.get_ls())) {
    ret = OB_ENTRY_NOT_EXIST;
    CLOG_LOG(WARN, "get log stream from ObLSService failed", K(ret), K(event));
  } else if (OB_FAIL(ls->get_log_handler()->get_access_mode(unused_mode_version, curr_access_mode))) {
    CLOG_LOG(WARN, "ObLogHandler get_access_mode failed", K(ret));
  } else {
    switch (event.event_type_) {
      case RoleChangeEventType::CHANGE_LEADER_EVENT_TYPE:
        CLOG_LOG(INFO, "begin change leader", K(curr_access_mode), K(event), KPC(ls));
        if (is_append_mode(curr_access_mode)
            && OB_FAIL(handle_change_leader_event_for_log_handler_(event.dst_addr_, ls))) {
          CLOG_LOG(WARN, "ObLogHandler change leader failed", K(ret), K(event), KPC(ls));
        } else if (is_raw_write_or_flashback_mode(curr_access_mode)
            && OB_FAIL(handle_change_leader_event_for_restore_handler_(event.dst_addr_, ls))) {
          CLOG_LOG(WARN, "ObRestoreHandler change leader failed", K(ret), K(event), KPC(ls));
        }
        CLOG_LOG(INFO, "end change leader", K(ret), K(curr_access_mode), K(event), KPC(ls));
        break;
      case RoleChangeEventType::ROLE_CHANGE_CB_EVENT_TYPE:
        CLOG_LOG(INFO, "begin log handler role change", K(curr_access_mode), K(event), KPC(ls));
        if (OB_FAIL(handle_role_change_cb_event_for_log_handler_(curr_access_mode, ls, retry_ctx))) {
          CLOG_LOG(WARN, "handle_role_change_cb_event_for_log_handler_ failed", K(ret),
              K(curr_access_mode), KPC(ls));
        }
        CLOG_LOG(INFO, "end log handler role change", K(ret), K(curr_access_mode), K(event), KPC(ls), K(retry_ctx));
        CLOG_LOG(INFO, "begin restore handler role change", K(curr_access_mode), K(event), KPC(ls));
        if (!retry_ctx.need_retry() && OB_FAIL(handle_role_change_cb_event_for_restore_handler_(curr_access_mode, ls))) {
          CLOG_LOG(WARN, "handle_role_change_cb_event_for_restore_handler_ failed", K(ret),
              K(curr_access_mode), KPC(ls));
        }
        CLOG_LOG(INFO, "end restore handler role change", K(ret), K(curr_access_mode), K(event), KPC(ls));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(WARN, "unexpected role change event type", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(ls) && !retry_ctx.need_retry()) {
    (void)ls->report_replica_info();
  }
  return ret;
}

int ObRoleChangeService::handle_role_change_cb_event_for_restore_handler_(
    const AccessMode &curr_access_mode,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  const bool log_handler_is_offline = ls->get_log_handler()->is_offline();

  // If log handler is offline, need execute LEADER_2_FOLLOWER or FOLLOWER_2_FOLLOWER.
  //
  // when access mode is RAW_WRITE or FLASHBACK, restore_handler need execute leader to
  // follower or follower to leader. otherwise, only need execute leader to follower or
  // follower to follower, therefore, we set 'need_transform_by_access_mode' to false
  // when 'curr_access_mode' is RAW_WRITE or FLASHBACK
  const bool only_need_change_to_follower =
    !is_raw_write_or_flashback_mode(curr_access_mode) || log_handler_is_offline;

  RoleChangeOptType opt_type;
  ObRole curr_role = ObRole::INVALID_ROLE;
  ObRole new_role  = ObRole::INVALID_ROLE;
  bool is_pending_state = false;
  int64_t curr_proposal_id = -1;
  int64_t new_proposal_id = -1;
  ObLogRestoreHandler *log_restore_handler = ls->get_log_restore_handler();
  ObRoleChangeHandler *restore_role_change_handler = ls->get_restore_role_change_handler();
  if (OB_FAIL(log_restore_handler->prepare_switch_role(curr_role, curr_proposal_id, new_role,
          new_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "ObRestoreHandler prepare_switch_role failed", K(ret), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id));
  } else if (false == need_execute_role_change(curr_proposal_id, curr_role, new_proposal_id,
        new_role, is_pending_state, log_handler_is_offline)) {
    CLOG_LOG(INFO, "no need change role", K(ret), K(is_pending_state), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id), K(is_pending_state), K(log_handler_is_offline));
  } else if (FALSE_IT(opt_type = get_role_change_opt_type_(curr_role, new_role, only_need_change_to_follower))) {
  } else {
    switch (opt_type) {
      // leader -> follower
      case RoleChangeOptType::LEADER_2_FOLLOWER:
      {
        ret = switch_leader_to_follower_forcedly_restore_(new_proposal_id, ls);
        break;
      }
      // follower -> follower
      case RoleChangeOptType::FOLLOWER_2_LEADER:
      {
        ret = switch_follower_to_leader_restore_(new_proposal_id, ls);
        break;
      }
      // leader -> leader
      case RoleChangeOptType::LEADER_2_LEADER:
      {
        ret = switch_leader_to_leader_restore_(new_proposal_id, curr_proposal_id, ls);
        break;
      }
      // follower -> follower
      case RoleChangeOptType::FOLLOWER_2_FOLLOWER:
      {
        ret = switch_follower_to_follower_restore_();
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error, can not handle role change", K(ret), K(curr_role),
            K(curr_proposal_id), K(new_role), K(new_proposal_id), KPC(ls));
    }
  }
  return ret;
}

int ObRoleChangeService::handle_role_change_cb_event_for_log_handler_(
    const AccessMode &curr_access_mode,
    ObLS *ls,
    RetrySubmitRoleChangeEventCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  const bool log_handler_is_offline = ls->get_log_handler()->is_offline();

  // If log handler is offline, need execute LEADER_2_FOLLOWER or FOLLOWER_2_FOLLOWER
  //
  // when access mode is APPEND, log_handler need execute leader to follower or
  // follower to leader. otherwise, only need execute leader to follower or  follower
  // to follower, therefore, we set 'need_transform_by_access_mode' to false when
  // 'curr_access_mode' is APPEND.
  const bool only_need_change_to_follower = !is_append_mode(curr_access_mode) || log_handler_is_offline;
  RoleChangeOptType opt_type;
  ObRole curr_role = ObRole::INVALID_ROLE;
  ObRole new_role  = ObRole::INVALID_ROLE;
  bool is_pending_state = false;
  int64_t curr_proposal_id = -1;
  int64_t new_proposal_id = -1;
  if (OB_FAIL(ls->get_log_handler()->prepare_switch_role(curr_role,
        curr_proposal_id, new_role, new_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "ObLogHandler prepare_switch_role failed", K(ret), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id));
  } else if (false == need_execute_role_change(curr_proposal_id, curr_role, new_proposal_id,
        new_role, is_pending_state, log_handler_is_offline)) {
    CLOG_LOG(INFO, "no need change role", K(ret), K(is_pending_state), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id), K(is_pending_state), K(log_handler_is_offline));
  } else if (FALSE_IT(opt_type = get_role_change_opt_type_(curr_role, new_role, only_need_change_to_follower))) {
  } else {
    switch (opt_type) {
      // leader -> follower
      case RoleChangeOptType::LEADER_2_FOLLOWER:
        if (OB_FAIL(switch_leader_to_follower_forcedly_(new_proposal_id, ls))) {
          CLOG_LOG(WARN, "switch_leader_to_follower_forcedly_ failed", K(ret), K(curr_role),
          K(curr_proposal_id), K(new_role), K(curr_access_mode), K(new_proposal_id));
        }
        break;
      // follower -> follower
      case RoleChangeOptType::FOLLOWER_2_LEADER:
        if (OB_FAIL(switch_follower_to_leader_(new_proposal_id, ls, retry_ctx))) {
          CLOG_LOG(WARN, "switch_follower_to_leader_ failed", K(ret), K(curr_role),
              K(curr_proposal_id), K(new_role), K(curr_access_mode), K(new_proposal_id));
        }
        break;
      // leader -> leader
      case RoleChangeOptType::LEADER_2_LEADER:
        if (OB_FAIL(switch_leader_to_leader_(new_proposal_id, curr_proposal_id, ls, retry_ctx))) {
          CLOG_LOG(WARN, "switch_leader_to_leader_ failed", K(ret), K(curr_role),
              K(curr_proposal_id), K(new_role), K(curr_access_mode), K(new_proposal_id));
        }
        break;
      // follower -> follower
      case RoleChangeOptType::FOLLOWER_2_FOLLOWER:
        if (OB_FAIL(switch_follower_to_follower_(new_proposal_id, ls))) {
          CLOG_LOG(WARN, "switch_follower_to_follower_ failed", K(ret), K(curr_role),
              K(curr_proposal_id), K(new_role), K(curr_access_mode), K(new_proposal_id));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error, can not handle role change", K(ret), K(curr_role),
            K(curr_proposal_id), K(new_role), K(new_proposal_id), KPC(ls));
    }
  }
  return ret;
}

int ObRoleChangeService::handle_change_leader_event_for_restore_handler_(
    const common::ObAddr &dst_addr,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObRole curr_role = ObRole::INVALID_ROLE;
  ObRole new_role  = ObRole::INVALID_ROLE;
  bool is_pending_state = false;
  int64_t curr_proposal_id = -1;
  int64_t new_proposal_id = -1;
  ObLogRestoreHandler *log_restore_handler = ls->get_log_restore_handler();
  if (OB_FAIL(log_restore_handler->prepare_switch_role(curr_role,
        curr_proposal_id, new_role, new_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "ObRestoreHandler prepare_switch_role failed", K(ret), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id));
  } else if (true == is_pending_state
      || curr_proposal_id != new_proposal_id || LEADER != curr_role || LEADER != new_role) {
    ls->get_log_restore_handler()->change_leader_to(dst_addr);
    CLOG_LOG(INFO, "no need execute switch_leader_to_follower_gracefully_restore_, change leader directlly",
        K(ret), K(is_pending_state), K(curr_proposal_id), K(new_proposal_id), K(curr_role), K(new_role));
  } else if (OB_FAIL(switch_leader_to_follower_gracefully_restore_(dst_addr, curr_proposal_id, ls))) {
    CLOG_LOG(WARN, "switch_leader_to_follower_gracefully_restore_ failed", K(ret), K(ls), K(dst_addr));
  } else {
    CLOG_LOG(INFO, "handle_change_leader_event_for_raw_write_ success", K(ret), K(curr_role),
        K(curr_proposal_id), K(new_role), K(new_proposal_id), K(dst_addr));
  }
  return ret;
}

int ObRoleChangeService::handle_change_leader_event_for_log_handler_(
    const common::ObAddr &dst_addr,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObRole curr_role = ObRole::INVALID_ROLE;
  ObRole new_role  = ObRole::INVALID_ROLE;
  bool is_pending_state = false;
  int64_t curr_proposal_id = -1;
  int64_t new_proposal_id = -1;
  if (OB_FAIL(ls->get_log_handler()->prepare_switch_role(curr_role,
        curr_proposal_id, new_role, new_proposal_id, is_pending_state))) {
    CLOG_LOG(WARN, "ObLogHandler prepare_switch_role failed", K(ret), K(curr_role), K(curr_proposal_id),
        K(new_role), K(new_proposal_id));
  } else if (true == is_pending_state
      || curr_proposal_id != new_proposal_id || LEADER != curr_role || LEADER != new_role) {
    // when log handler is not LEDAER, we also need execute change_leader_to, otherwise, the leader can not be changed by election.
    ls->get_log_handler()->change_leader_to(dst_addr);
    CLOG_LOG(INFO, "no need execute switch_leader_to_follower_gracefully, change leader directlly",
        K(ret), K(is_pending_state), K(curr_proposal_id), K(new_proposal_id), K(curr_role), K(new_role));
  } else if (OB_FAIL(switch_leader_to_follower_gracefully_(new_proposal_id, curr_proposal_id,
                                                           dst_addr, ls))) {
    CLOG_LOG(WARN, "switch_leader_to_follower_gracefully_ failed", K(ret), KPC(ls),
        K(curr_role), K(curr_proposal_id), K(new_role), K(new_proposal_id));
  } else {
    CLOG_LOG(INFO, "handle_change_leader_event_for_log_handler_ success", K(ret), K(curr_role),
        K(curr_proposal_id), K(new_role), K(new_proposal_id), K(dst_addr));
  }
  return ret;
}

int ObRoleChangeService::switch_follower_to_leader_(
    const int64_t new_proposal_id,
    ObLS *ls,
    RetrySubmitRoleChangeEventCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  const ObRole new_role = LEADER;
  const share::ObLSID &ls_id = ls->get_ls_id();
  palf::LSN end_lsn;
  ObTimeGuard time_guard("switch_to_leader", EACH_ROLE_CHANGE_COST_MAX_TIME);
  ObLogHandler *log_handler = ls->get_log_handler();
  ObRoleChangeHandler *role_change_handler = ls->get_role_change_handler();
  ATOMIC_SET(&cur_task_info_.state_, TakeOverState::WAIT_REPLAY_DONE);
  ATOMIC_SET(&cur_task_info_.id_, ls->get_ls_id().id());
  if (OB_FAIL(log_handler->get_end_lsn(end_lsn))) {
    CLOG_LOG(WARN, "get_end_lsn failed", K(ret), KPC(ls));
  // NB: order is vital!!!
  //     We must guarantee that 'replay_service_' has replayed complete data, and before
  //     stop 'replay_service_', other components can not submit log.
  } else if (FALSE_IT(time_guard.click("wait_replay_service_apply_done_"))
      || OB_FAIL(wait_replay_service_replay_done_(ls_id, end_lsn, WAIT_REPLAY_DONE_TIMEOUT_US))) {
    if (need_retry_submit_role_change_event_(ret)) {
      retry_ctx.set_retry_reason(RetrySubmitRoleChangeEventReason::WAIT_REPLAY_DONE_TIMEOUT);
    } else {
      CLOG_LOG(WARN, "wait_replay_service_replay_done_ failed", K(ret), K(end_lsn));
    }
  } else if (FALSE_IT(time_guard.click("apply_service->switch_to_leader"))
      || OB_FAIL(apply_service_->switch_to_leader(ls_id, new_proposal_id))) {
    CLOG_LOG(WARN, "apply_service_ switch_to_leader failed", K(ret), K(new_role), K(new_proposal_id));
  } else if (FALSE_IT(time_guard.click("replay_service->switch_to_leader"))
      || OB_FAIL(replay_service_->switch_to_leader(ls_id))) {
  } else if (FALSE_IT(log_handler->switch_role(new_role, new_proposal_id))) {
    CLOG_LOG(WARN, "ObLogHandler switch role failed", K(ret), K(new_role), K(new_proposal_id));
  } else if (FALSE_IT(ATOMIC_SET(&cur_task_info_.state_, TakeOverState::WAIT_RC_HANDLER_DONE))) {
  } else if (FALSE_IT(time_guard.click("role_change_handler->switch_to_leader"))
      || OB_FAIL(role_change_handler->switch_to_leader(cur_task_info_))) {
    CLOG_LOG(WARN, "ObRoleChangeHandler switch_to_leader failed", K(ret), KPC(ls));
  } else {
    ATOMIC_SET(&cur_task_info_.state_, TakeOverState::TAKE_OVER_FINISH);
    ATOMIC_SET(&cur_task_info_.log_type_, ObLogBaseType::INVALID_LOG_BASE_TYPE);
    CLOG_LOG(INFO, "switch_follower_to_leader_ success", K(ret), KPC(ls));
  }
  if (OB_FAIL(ret) && !retry_ctx.need_retry()) {
    log_handler->revoke_leader();
    CLOG_LOG(WARN, "switch_follower_to_leader_ failed", K(ret), KPC(ls));
  }
  return ret;
}

int ObRoleChangeService::switch_leader_to_follower_forcedly_(
    const int64_t new_proposal_id,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  const ObRole new_role = FOLLOWER;
  const share::ObLSID &ls_id = ls->get_ls_id();
  ObLogHandler *log_handler = ls->get_log_handler();
  ObRoleChangeHandler *role_change_handler = ls->get_role_change_handler();
  palf::LSN end_lsn;
  ObTimeGuard time_guard("switch_leader_to_follower_forcedly_", EACH_ROLE_CHANGE_COST_MAX_TIME);

	// Why need wait_apply_sync?
	//
	// when we can execute 'switch_to_follower_forcedly', means that there is no possibility to submit log via log handler successfully.
	// however, the flying callback may have not been pushed into apply service, and then, 'switch_to_follower' will be executed, for trans,
	// if the callback be executed after 'switch_to_follower', will cause abort.
  if (OB_FAIL(apply_service_->wait_append_sync(ls_id))) {
    CLOG_LOG(WARN, "wait_apply_sync failed", K(ret), K(ls_id));
  } else if (FALSE_IT(time_guard.click("apply_service->wait_apply_sync"))
      || OB_FAIL(apply_service_->switch_to_follower(ls_id))) {
    CLOG_LOG(WARN, "apply_service_ switch_to_follower failed", K(ret), K(new_role), K(new_proposal_id));
  } else if (FALSE_IT(time_guard.click("apply_service->switch_to_follower"))
      || OB_FAIL(wait_apply_service_apply_done_(ls_id, end_lsn))) {
    CLOG_LOG(WARN, "wait_apply_service_apply_done_ failed", K(ret), K(end_lsn));
  } else {
	  time_guard.click("wait_apply_service_apply_done_");
    role_change_handler->switch_to_follower_forcedly();
    // NB: order is vital
    //     We must guarantee that this replica will not submit any logs after switch_role.
    log_handler->switch_role(new_role, new_proposal_id);
    // NB: in case of leader reovke, do we no need retry.
    (void)replay_service_->switch_to_follower(ls_id, end_lsn);
    CLOG_LOG(INFO, "switch_leader_to_follower_forcedly_ success", K(ret), KPC(ls));
  }
  if (OB_FAIL(ret)) {
    log_handler->revoke_leader();
    CLOG_LOG(WARN, "switch_leader_to_follower_forcedly_ failed", K(ret), K(new_proposal_id), K(new_role));
  }
  return ret;
}

int ObRoleChangeService::switch_leader_to_follower_gracefully_(
    const int64_t new_proposal_id,
    const int64_t curr_proposal_id,
    const common::ObAddr &dst_addr,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObRole new_role = FOLLOWER;
  const share::ObLSID &ls_id = ls->get_ls_id();
  ObLogHandler *log_handler = ls->get_log_handler();
  ObRoleChangeHandler *role_change_handler = ls->get_role_change_handler();
  LSN end_lsn;
  ObTimeGuard time_guard("switch_leader_to_follower_gracefully_", EACH_ROLE_CHANGE_COST_MAX_TIME);
  // 1. OB_SUCCESS means execute transaction successfully, we need execute follow steps.
  // 2. OB_LS_NEED_REVOKE means the transaction execute failed, and can't been rollback, need revoke LS.
  // 3. OTHERS, switch_to_follower_gracefully failed, and 'role_change_handler' has rollback success,
  //    no need to execute follow steps.
  if (FALSE_IT(time_guard.click("role_change_handler->switch_to_follower_gracefully"))
      || OB_SUCCESS != (tmp_ret = role_change_handler->switch_to_follower_gracefully())) {
    CLOG_LOG(WARN, "switch_to_follower_gracefully failed, need revoke leader", K(tmp_ret),
				K(new_role), K(new_proposal_id), K(dst_addr));
  // NB: order is vital!!!
  //     we must ensure that the 'end_lsn' provid by 'apply_service_' is correctly.
  // just switch_role to follower firstly, avoid sync log failed because palf has changed leader.
  } else if (FALSE_IT(log_handler->switch_role(new_role, curr_proposal_id))) {
  // apply service will not update end_lsn after switch_to_follower, so wait apply done first here
  } else if (FALSE_IT(time_guard.click("wait_apply_service_apply_done_when_change_leader_"))
      || OB_FAIL(wait_apply_service_apply_done_when_change_leader_(log_handler, curr_proposal_id, ls_id, end_lsn))) {
    CLOG_LOG(WARN, "wait_apply_service_apply_done_when_change_leader_ failed", K(ret),
				K(new_role), K(new_proposal_id), K(dst_addr));
    // wait apply service done my fail, we need :
    // 1. switch log handler to origin status.
    // 2. resume role change handler
    log_handler->switch_role(LEADER, curr_proposal_id);
    if (OB_FAIL(role_change_handler->resume_to_leader())) {
      CLOG_LOG(WARN, "resume to leader failed", K(ret), KPC(ls));
    }
  // NB: the following steps mustn't be failed.
  } else if (FALSE_IT(time_guard.click("apply_service->switch_to_follower"))
      || OB_FAIL(apply_service_->switch_to_follower(ls_id))) {
    CLOG_LOG(WARN, "apply_service_ switch_to_follower failed", K(ret), K(new_role), K(new_proposal_id), K(dst_addr));
  } else if (FALSE_IT(time_guard.click("replay_service->switch_to_follower"))
      || OB_FAIL(replay_service_->switch_to_follower(ls_id, end_lsn))) {
    CLOG_LOG(WARN, "replay_service_ switch_to_follower failed", K(ret), KPC(ls), K(new_role), K(new_proposal_id));
    // NB: execute 'change_leader_to' lastly, can make 'wait_apply_service_apply_done_when_change_leader_' finish quickly.
  } else if (OB_FAIL(log_handler->change_leader_to(dst_addr))) {
    CLOG_LOG(WARN, "ObLogHandler change_leader failed", K(ret), K(new_role), K(new_proposal_id), K(dst_addr));
  } else {
    CLOG_LOG(INFO, "switch_to_follower_gracefully success", K(ret), K(new_role), K(new_proposal_id), K(dst_addr));
  }
  if (OB_FAIL(ret) || OB_LS_NEED_REVOKE == tmp_ret) {
    log_handler->revoke_leader();
    CLOG_LOG(WARN, "switch_leader_to_follower_gracefully failed, revoke leader", K(ret), K(tmp_ret), K(dst_addr),
				K(new_role), K(new_proposal_id));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObRoleChangeService::switch_leader_to_leader_(
    const int64_t new_proposal_id,
    const int64_t curr_proposal_id,
    ObLS *ls,
    RetrySubmitRoleChangeEventCtx &retry_ctx)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("switch_leader_to_leader", EACH_ROLE_CHANGE_COST_MAX_TIME);
  if (FALSE_IT(time_guard.click("switch_leader_to_follower_forcedly_"))
      || OB_FAIL(switch_leader_to_follower_forcedly_(curr_proposal_id, ls))) {
    CLOG_LOG(WARN, "switch_leader_to_leader_, switch leader to follower failed", K(ret), KPC(ls));
  } else if (FALSE_IT(time_guard.click("switch_follower_to_leader_"))
      || OB_FAIL(switch_follower_to_leader_(new_proposal_id, ls, retry_ctx))) {
    CLOG_LOG(WARN, "switch_follower_to_leader_ failed", K(ret), K(new_proposal_id));
  } else {
    CLOG_LOG(INFO, "switch_leader_to_leader_ success", K(ret), KPC(ls));
  }
  return ret;
}

int ObRoleChangeService::switch_follower_to_follower_(const int64_t new_proposal_id, ObLS *ls)
{
  int ret = OB_SUCCESS;
  // need update proposal_id
  const share::ObLSID &ls_id = ls->get_ls_id();
  ObLogHandler *log_handler = ls->get_log_handler();
  (void) log_handler->switch_role(common::ObRole::FOLLOWER, new_proposal_id);
  CLOG_LOG(INFO, "switch_follower_to_follower_");
  return ret;
}

int ObRoleChangeService::switch_follower_to_leader_restore_(
    const int64_t new_proposal_id,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  const ObRole new_role = LEADER;
  ObLogRestoreHandler *log_restore_handler = ls->get_log_restore_handler();
  ObRoleChangeHandler *restore_role_change_handler = ls->get_restore_role_change_handler();
  ATOMIC_SET(&cur_task_info_.state_, TakeOverState::WAIT_RC_HANDLER_DONE);
  ATOMIC_SET(&cur_task_info_.id_, ls->get_ls_id().id());
  ObTimeGuard time_guard("switch_follower_to_leader_restore_", EACH_ROLE_CHANGE_COST_MAX_TIME);
  if (FALSE_IT(log_restore_handler->switch_role(new_role, new_proposal_id))) {
  } else if (FALSE_IT(time_guard.click("restore_role_change_handler->switch_to_leader"))
      || OB_FAIL(restore_role_change_handler->switch_to_leader(cur_task_info_))) {
    CLOG_LOG(WARN, "restore_role_change_handler switch_to_leader failed", K(ret), K(new_role),
				K(new_proposal_id), K(ls));
  } else {
    ATOMIC_SET(&cur_task_info_.state_, TakeOverState::TAKE_OVER_FINISH);
    ATOMIC_SET(&cur_task_info_.log_type_, ObLogBaseType::INVALID_LOG_BASE_TYPE);
  }
  if (OB_FAIL(ret)) {
    log_restore_handler->revoke_leader();
  }
  return ret;
}

int ObRoleChangeService::switch_leader_to_follower_forcedly_restore_(
    const int64_t new_proposal_id,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObLogRestoreHandler *log_restore_handler = ls->get_log_restore_handler();
  ObRoleChangeHandler *restore_role_change_handler = ls->get_restore_role_change_handler();
  (void)restore_role_change_handler->switch_to_follower_forcedly();
  (void)log_restore_handler->switch_role(FOLLOWER, new_proposal_id);
  return ret;
}

int ObRoleChangeService::switch_leader_to_follower_gracefully_restore_(
    const common::ObAddr &dst_addr,
    const int64_t curr_proposal_id,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLogRestoreHandler *log_restore_handler = ls->get_log_restore_handler();
  ObRoleChangeHandler *restore_role_change_handler = ls->get_restore_role_change_handler();
  if (OB_SUCCESS != (tmp_ret = restore_role_change_handler->switch_to_follower_gracefully())) {
    CLOG_LOG(WARN, "ObRoleChangeHandler switch_to_follower_gracefully failed", K(ret), KPC(ls));
  } else if (FALSE_IT(log_restore_handler->switch_role(FOLLOWER, curr_proposal_id))) {
  } else if (OB_FAIL(log_restore_handler->change_leader_to(dst_addr))) {
    CLOG_LOG(WARN, "ObLogRestoreHandler change_leader_to failed", K(ret), K(ls), K(dst_addr));
  } else {
  }
  if (OB_FAIL(ret) || OB_LS_NEED_REVOKE == tmp_ret) {
    log_restore_handler->revoke_leader();
    CLOG_LOG(WARN, "switch_leader_to_follower_gracefully failed, revoke leader", K(ret), K(tmp_ret), K(dst_addr));
    ret = (OB_SUCCESS == ret ? tmp_ret : ret);
  }
  return ret;
}

int ObRoleChangeService::switch_follower_to_follower_restore_()
{
  return OB_SUCCESS;
}

int ObRoleChangeService::switch_leader_to_leader_restore_(
    const int64_t new_proposal_id,
    const int64_t curr_proposal_id,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("switch_leader_to_leader", EACH_ROLE_CHANGE_COST_MAX_TIME);
  if (FALSE_IT(time_guard.click("switch_leader_to_follower_forcedly_restore_"))
      || OB_FAIL(switch_leader_to_follower_forcedly_restore_(curr_proposal_id, ls))) {
    CLOG_LOG(WARN, "switch_leader_to_follower_forcedly_restore_ failed", K(ret),
        K(new_proposal_id), K(ls));
  } else if (FALSE_IT(time_guard.click("switch_follower_to_leader_restore_"))
      || OB_FAIL(switch_follower_to_leader_restore_(new_proposal_id, ls))) {
    CLOG_LOG(WARN, "switch_follower_to_leader_restore_ failed", K(ret),
        K(new_proposal_id), K(ls));
  } else {
    CLOG_LOG(INFO, "switch_leader_to_leader_restore_ success", K(ls));
  }
  return ret;
}

int ObRoleChangeService::wait_replay_service_replay_done_(
    const share::ObLSID &ls_id,
    const palf::LSN &end_lsn,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool is_done = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  palf::TimeoutChecker not_timeout(timeout_us);
  while (OB_SUCC(ret) && false == is_done && OB_SUCC(not_timeout())) {
    if (OB_FAIL(replay_service_->is_replay_done(ls_id, end_lsn, is_done))) {
      CLOG_LOG(WARN, "replay_service_ is_replay_done failed", K(ret), K(is_done), K(end_lsn));
    } else if (false == is_done) {
      ob_usleep(50*1000);
      CLOG_LOG(INFO, "wait replay done return false, need retry", K(ls_id), K(end_lsn), K(start_ts));
    } else {
    }
  }
  CLOG_LOG(INFO, "wait_replay_service_replay_done_ finish", K(ret), K(ls_id), K(end_lsn), K(is_done));
  return ret;
}

int ObRoleChangeService::wait_apply_service_apply_done_(
    const share::ObLSID &ls_id,
    palf::LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  bool is_done = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  while (OB_SUCC(ret) && false == is_done) {
    if (OB_FAIL(apply_service_->is_apply_done(ls_id, is_done, end_lsn))) {
      CLOG_LOG(WARN, "apply_service_ is_apply_done failed", K(ret), K(is_done), K(end_lsn));
    } else if (false == is_done) {
      ob_usleep(5*1000);
      CLOG_LOG(WARN, "wait apply done return false, need retry", K(ls_id), K(is_done), K(end_lsn), K(start_ts));
    } else {
    }
  }
  return ret;
}

int ObRoleChangeService::wait_apply_service_apply_done_when_change_leader_(
    const ObLogHandler *log_handler,
    const int64_t proposal_id,
    const share::ObLSID &ls_id,
    palf::LSN &end_lsn)
{
  int ret = OB_SUCCESS;
  bool is_done = false;
  palf::LSN max_lsn;
  common::ObRole unused_curr_role;
  int64_t unused_curr_proposal_id;
  common::ObRole new_role;
  int64_t new_proposal_id;
  bool is_pending_state = false;
  while (OB_SUCC(ret) && (false == is_done || end_lsn != max_lsn)) {
    if (OB_FAIL(apply_service_->is_apply_done(ls_id, is_done, end_lsn))) {
      CLOG_LOG(WARN, "apply_service_ is_apply_done failed", K(ret), K(is_done), K(end_lsn));
      // NB: ApplyService execute on_failure only when it's FOLLOWER, therefore ApplyService my not return apply done
      //     when it's LEADER, we need check the role of palf when has changed.
    } else if (OB_FAIL(log_handler->get_max_lsn(max_lsn))) {
      CLOG_LOG(WARN, "get_end_lsn from palf failed", K(ret), K(ls_id), K(end_lsn));
    } else if (OB_FAIL(log_handler->prepare_switch_role(unused_curr_role, unused_curr_proposal_id,
            new_role, new_proposal_id, is_pending_state))) {
      CLOG_LOG(WARN, "failed prepare_switch_role", K(ret), K(new_role), K(proposal_id), K(ls_id));
      // if palf has changed role, return OB_STATE_NOT_MATCH, change leader failed.
    } else if (LEADER != new_role || proposal_id != new_proposal_id) {
      ret = OB_STATE_NOT_MATCH;
      CLOG_LOG(WARN, "palf has changed leader, wait_apply_service_apply_done_when_change_leader_ failed", K(ret), K(proposal_id),
          K(new_proposal_id));
    } else if (false == is_done || end_lsn != max_lsn) {
      CLOG_LOG(INFO, "wait apply done return false, need retry", K(ls_id), K(is_done),
          K(end_lsn), K(max_lsn));
      ob_usleep(5*1000);
    } else {
    }
  }
  return ret;
}

ObRoleChangeService::RoleChangeOptType ObRoleChangeService::get_role_change_opt_type_(
    const ObRole &old_role,
    const ObRole &new_role,
    const bool need_transform_by_access_mode) const
{
  RoleChangeOptType rc_opt_type = RoleChangeOptType::INVALID_RC_OPT_TYPE;
  if (FOLLOWER == old_role && LEADER == new_role) {
    rc_opt_type = RoleChangeOptType::FOLLOWER_2_LEADER;
  } else if (LEADER == old_role && FOLLOWER == new_role) {
    rc_opt_type = RoleChangeOptType::LEADER_2_FOLLOWER;
  } else if (FOLLOWER == old_role && FOLLOWER == new_role) {
    rc_opt_type = RoleChangeOptType::FOLLOWER_2_FOLLOWER;
  } else if (LEADER == old_role && LEADER == new_role) {
    rc_opt_type = RoleChangeOptType::LEADER_2_LEADER;
  } else {
  }
  if (true == need_transform_by_access_mode) {
    if (LEADER == old_role) {
      rc_opt_type = RoleChangeOptType::LEADER_2_FOLLOWER;
    } else {
      rc_opt_type = RoleChangeOptType::FOLLOWER_2_FOLLOWER;
    }
  }
  return rc_opt_type;
}

// NB:
// 1. when log handler is offline, need execute role change;
// 2. when palf is not in pending:
//   1. proposal_id is not same or
//   2. role is not same.(If there are no pending logs in sliding window,
//      leader to follower will not advance proposal_id)
bool ObRoleChangeService::need_execute_role_change(const int64_t curr_proposal_id,
                                                   const common::ObRole curr_role,
                                                   const int64_t new_proposal_id,
                                                   const common::ObRole new_role,
                                                   const bool is_pending_state,
                                                   const bool is_offline) const
{
  return is_offline
         || (!is_pending_state
             && (curr_proposal_id != new_proposal_id || curr_role != new_role));
}

int ObRoleChangeService::diagnose(RCDiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  if (diagnose_info.id_ == ATOMIC_LOAD(&cur_task_info_.id_)) {
    // 当前日志流的切主任务正在被处理
    diagnose_info.state_ = ATOMIC_LOAD(&cur_task_info_.state_);
    diagnose_info.log_type_ = ATOMIC_LOAD(&cur_task_info_.log_type_);
  } else {
    // 当前日志流切主任务尚未被处理, 可能是因为其他日志流的切主任务卡住
    diagnose_info.state_ = logservice::TakeOverState::UNKNOWN_TAKE_OVER_STATE;
    diagnose_info.log_type_ = logservice::ObLogBaseType::INVALID_LOG_BASE_TYPE;
  }
  return ret;
}

bool ObRoleChangeService::is_append_mode(const AccessMode &mode) const
{
  return (AccessMode::APPEND == mode);
}

bool ObRoleChangeService::is_raw_write_or_flashback_mode(const AccessMode &mode) const
{
  return (AccessMode::RAW_WRITE == mode || AccessMode::FLASHBACK == mode ||
      AccessMode::PREPARE_FLASHBACK == mode);
}

bool ObRoleChangeService::need_retry_submit_role_change_event_(int ret) const
{
  bool bool_ret = false;
  if (OB_TIMEOUT == ret) {
    bool_ret = true;
  }
  return bool_ret;
}

} // end namespace logservice
} // end namespace oceanbase
