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

#define USING_LOG_PREFIX STORAGE
#include "ob_i_ls_restore_state.h"
#include "storage/ls/ob_ls.h"
#include "logservice/ob_log_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/restore/ob_ls_restore_handler.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_tablet_group_restore.h"
#include "storage/high_availability/ob_ls_restore.h"
#include "share/restore/ob_physical_restore_table_operator.h"
using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace backup;
using namespace logservice;


ERRSIM_POINT_DEF(EN_INSERT_GET_FOLLOWER_SERVER_FAILED);
//================================ObILSRestoreState=======================================

ObILSRestoreState::ObILSRestoreState(const share::ObLSRestoreStatus::Status &status)
  : is_inited_(false),
    cluster_id_(),
    ls_(nullptr),
    ls_restore_status_(status),
    ls_restore_arg_(nullptr),
    role_(),
    proposal_id_(0),
    tablet_mgr_(),
    location_service_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    proxy_(nullptr),
    self_addr_(),
    need_report_clog_lsn_(true)
{
}

ObILSRestoreState::~ObILSRestoreState()
{
}

int ObILSRestoreState::init(storage::ObLS &ls, logservice::ObLogService &log_srv, ObTenantRestoreCtx &restore_args)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(tablet_mgr_.init(this, ls.get_ls_id()))) {
    LOG_WARN("fail to init tablet mgr", K(ret), K(ls));
  } else if (OB_FAIL(log_srv.get_palf_role(ls.get_ls_id(), role_, proposal_id_))) {
    LOG_WARN("fail to get role", K(ret), "ls_id", ls.get_ls_id());
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else {
    ls_ = &ls;
    ls_restore_arg_ = &restore_args;
    cluster_id_ = GCONF.cluster_id;
    location_service_ = GCTX.location_service_;
    bandwidth_throttle_ = GCTX.bandwidth_throttle_;
    proxy_ = GCTX.sql_proxy_;
    self_addr_ = GCTX.self_addr();
    svr_rpc_proxy_ = ls_service->get_storage_rpc_proxy();
    storage_rpc_ = ls_service->get_storage_rpc();
    tablet_mgr_.set_force_reload();
    is_inited_ = true;
  }
  return ret;
}

void ObILSRestoreState::destroy()
{
  tablet_mgr_.destroy();
  ls_ = nullptr;
  ls_restore_arg_ = nullptr;
}

int ObILSRestoreState::handle_pull_tablet(
    const ObIArray<common::ObTabletID> &tablet_ids,
    const share::ObLSRestoreStatus &leader_restore_status,
    const int64_t leader_proposal_id)
{
  int ret = OB_SUCCESS;
  bool all_finish = false;
  LOG_INFO("success received handle pull tablet rpc");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_follower(role_)) {
    ret = OB_NOT_FOLLOWER;
    LOG_WARN("not follower", K(ret), KPC(ls_));
  } else if (leader_proposal_id != proposal_id_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("follower's proposal_id is not equal to leader proposal id", K(ret), K(leader_proposal_id), K(proposal_id_));
  } else if ((leader_restore_status.is_restore_tablets_meta() && ls_restore_status_.is_restore_tablets_meta())
     || (leader_restore_status.is_quick_restore() && ls_restore_status_.is_quick_restore())
     || (leader_restore_status.is_restore_major_data() && ls_restore_status_.is_restore_major_data())) {
    if (OB_FAIL(tablet_mgr_.add_tablet_in_wait_set(tablet_ids))) {
      LOG_WARN("fail to add tablet in wait set", K(tablet_ids), KPC(ls_));
    } else {
      LOG_INFO("succeed add tablets into tablet mgr", K(tablet_ids));
    }
  }
  return ret;
}

int ObILSRestoreState::check_leader_restore_finish(bool &finish)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus leader_restore_status;
  if (OB_FAIL(request_leader_status_(leader_restore_status))) {
    LOG_WARN("fail to request leader tablets and status", K(ret), KPC_(ls));
  } else {
    finish = check_leader_restore_finish_(leader_restore_status, ls_restore_status_);
  }

  return ret;
}

int ObILSRestoreState::update_restore_status_(
    storage::ObLS &ls, const share::ObLSRestoreStatus &next_status)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = ls.get_ls_restore_handler();
  int64_t rebuild_seq = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls_restore_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore handler can't be nullptr", K(ret));
  } else if (FALSE_IT(rebuild_seq = ls_restore_handler->get_rebuild_seq())) {
  } else if (OB_FAIL(ls.set_restore_status(next_status, rebuild_seq))) {
    LOG_WARN("fail to advance ls meta status", K(ret), K(next_status), K(ls), K(rebuild_seq));
    if (OB_STATE_NOT_MATCH == ret) {
      if (OB_SUCCESS != (tmp_ret = ls_restore_handler->update_rebuild_seq())) {
        LOG_ERROR("failed to update rebuild seq", K(ret), K(tmp_ret), K(rebuild_seq));
      }
      //overwrite ret
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObILSRestoreState::deal_failed_restore(const ObLSRestoreResultMgr &result_mgr)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_FAILED);
  ObHAResultInfo::Comment comment;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(result_mgr.get_comment_str(ls_->get_ls_id(), self_addr_, comment))) {
    LOG_WARN("fail to get comment str", K(ret));
  } else if (OB_FAIL(report_ls_restore_progress_(*ls_, next_status, result_mgr.get_trace_id(),
      result_mgr.get_result(), comment.ptr()))) {
    LOG_WARN("fail to report ls restore progress", K(ret));
  } else if (OB_FAIL(update_restore_status_(*ls_, next_status))) {
    LOG_WARN("failed to update restore status", K(ret), KPC(ls_), K(next_status));
  } else if (OB_FAIL(report_ls_restore_status_(*ls_, next_status))) {
    LOG_WARN("fail to report ls restore progress", K(ret));
  }
  return ret;
}

int ObILSRestoreState::advance_status_(
    storage::ObLS &ls, const share::ObLSRestoreStatus &next_status)
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = ls.get_ls_restore_handler();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(ls_restore_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore handler can't be nullptr", K(ret));
  } else if (OB_FAIL(update_restore_status_(ls, next_status))) {
    LOG_WARN("failed to update restore status", K(ret), K(ls), K(next_status));
  } else {
    ls_restore_handler->wakeup();
    LOG_INFO("success advance status", K(ls), K(next_status));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = report_ls_restore_status_(ls, next_status))) {
    LOG_WARN("fail to advance ls meta table status", K(tmp_ret), K(ls));
  }

  if (OB_SUCCESS != (tmp_ret = report_ls_restore_progress_(ls, next_status, *ObCurTraceId::get_trace_id()))) {
    LOG_WARN("fail to report ls restore progress", K(tmp_ret), K(ls), K(next_status));
  }

  if (need_notify_rs_restore_finish_(next_status)) {
    notify_rs_restore_finish_();
  }
  return ret;
}

bool ObILSRestoreState::need_notify_rs_restore_finish_(const ObLSRestoreStatus &ls_restore_status)
{
  return ObLSRestoreStatus::WAIT_RESTORE_TO_CONSISTENT_SCN  == ls_restore_status
         || ObLSRestoreStatus::QUICK_RESTORE_FINISH == ls_restore_status
         || ObLSRestoreStatus::NONE == ls_restore_status
         || ObLSRestoreStatus::RESTORE_FAILED == ls_restore_status;
}

int ObILSRestoreState::report_ls_restore_progress_(
    storage::ObLS &ls, const share::ObLSRestoreStatus &status,
    const share::ObTaskId &trace_id, const int result, const char *comment)
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = ls_->get_ls_restore_handler();
  ObLSRestoreStat &restore_stat = ls_restore_handler->restore_stat();
  int64_t finished_tablet_cnt = 0;
  share::ObRestorePersistHelper helper;
  ObLSRestoreJobPersistKey ls_key;
  ls_key.tenant_id_ = ls.get_tenant_id();
  ls_key.job_id_ = ls_restore_arg_->get_job_id();
  ls_key.ls_id_ = ls.get_ls_id();
  ls_key.addr_ = self_addr_;

  if (!GCTX.is_shared_storage_mode() && OB_FAIL(restore_stat.get_finished_tablet_cnt(finished_tablet_cnt))) {
    LOG_WARN("fail to get finished tablet cnt", K(ret));
  } else if (OB_FAIL(helper.init(ls_key.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key.tenant_id_);
  } else if (OB_FAIL(helper.update_ls_restore_status(*proxy_, ls_key, trace_id, status, finished_tablet_cnt, result, comment))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // this ls may created by migrate.
      LOG_INFO("ls restore progress not exist. this ls may created by migrate", K(ret), KPC(ls_));
      ObLSRestoreProgressPersistInfo ls_restore_info;
      ls_restore_info.key_ = ls_key;
      ls_restore_info.restore_scn_ = ls_restore_arg_->get_restore_scn();
      ls_restore_info.status_ = status;
      ls_restore_info.result_ = result;
      ls_restore_info.trace_id_ = trace_id;
      if (OB_FAIL(ls_restore_info.comment_.assign(comment))) {
        LOG_WARN("failed to assign comment", K(ret));
      } else if (OB_FAIL(helper.insert_initial_ls_restore_progress(*proxy_, ls_restore_info))) {
        LOG_WARN("failed to insert initial ls restore progress", K(ret));
      }
    } else {
      LOG_WARN("fail to update log restore status", K(ret));
    }
  }
  return ret;
}

int ObILSRestoreState::insert_initial_ls_restore_progress_()
{
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  ObLSRestoreProgressPersistInfo ls_restore_info;
  ls_restore_info.key_.tenant_id_ = ls_->get_tenant_id();
  ls_restore_info.key_.job_id_ = ls_restore_arg_->get_job_id();
  ls_restore_info.key_.ls_id_ = ls_->get_ls_id();
  ls_restore_info.key_.addr_ = self_addr_;
  ls_restore_info.restore_scn_ = ls_restore_arg_->get_restore_scn();
  ls_restore_info.status_ = ObLSRestoreStatus::Status::RESTORE_START;
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    ls_restore_info.status_ = ObLSRestoreStatus::Status::SS_RESTORE_START;
  }
#endif
  if (OB_FAIL(helper.init(ls_restore_info.key_.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_restore_info.key_.tenant_id_);
  } else if (OB_FAIL(helper.insert_initial_ls_restore_progress(*proxy_, ls_restore_info))) {
    LOG_WARN("fail to insert initial ls restore progress info", K(ret), K(ls_restore_info));
  } else {
    LOG_INFO("succeed insert ls restore progress info", K(ls_restore_info));
  }
  return ret;
}

int ObILSRestoreState::report_ls_restore_status_(const storage::ObLS &ls, const share::ObLSRestoreStatus &next_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_->report_replica_info())) {
    LOG_WARN("fail to report replica info", K(ret), KPC(ls_));
  } else {
    SERVER_EVENT_ADD("storage_ha", "restore_ls",
      "tenant_id", MTL_ID(),
      "ls_id", ls.get_ls_id().id(),
      "curr_status", ls_restore_status_.get_status(),
      "next_status", next_status.get_status(),
      "curr_status_str", ObLSRestoreStatus::get_restore_status_str(ls_restore_status_),
      "next_status_str", ObLSRestoreStatus::get_restore_status_str(next_status));
  }
  return ret;
}

int ObILSRestoreState::update_role_()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_srv = nullptr;
  int64_t proposal_id = 0;
  ObRole new_role;
  if (nullptr == (log_srv = MTL(logservice::ObLogService*))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log service is nullptr", K(ret));
  } else if (OB_FAIL(log_srv->get_palf_role(ls_->get_ls_id(), new_role, proposal_id))) {
    LOG_WARN("fail to get role", K(ret), "ls_id", ls_->get_ls_id());
  } else if (is_switch_to_leader_(new_role)) {
    LOG_WARN("change role from follower to leader", K(ret), "new role", new_role, "old role", role_);
    role_ = new_role;
    tablet_mgr_.switch_to_leader();
  } else if (is_switch_to_follower_(new_role)) {
    LOG_WARN("change role from leader to follower", K(ret), "new role", new_role, "old role", role_);
    role_ = new_role;
    tablet_mgr_.switch_to_follower();
  } else if (ObRole::FOLLOWER == role_ && proposal_id != proposal_id_) {
    tablet_mgr_.leader_switched();
  }
  if (OB_SUCC(ret)) {
    proposal_id_ = proposal_id;
  }
  return ret;
}

bool ObILSRestoreState::is_switch_to_leader_(const ObRole &new_role) {
  bool bret= false;
  if (!is_follower(new_role) && is_follower(role_)) {
    bret = true;
  }
  return bret;
}

bool ObILSRestoreState::is_switch_to_follower_(const ObRole &new_role) {
  bool bret= false;
  if (is_follower(new_role) && !is_follower(role_)) {
    bret = true;
  }
  return bret;
}

int ObILSRestoreState::check_new_election_(bool &is_changed) const
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_srv = nullptr;
  int64_t proposal_id = 0;
  ObRole new_role;
  is_changed = false;
  if (nullptr == (log_srv = MTL(logservice::ObLogService*))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log service is nullptr", K(ret));
  } else if (OB_FAIL(log_srv->get_palf_role(ls_->get_ls_id(), new_role, proposal_id))) {
    LOG_WARN("fail to get role", K(ret), "ls_id", ls_->get_ls_id());
  } else if (new_role != role_ || proposal_id != proposal_id_) {
    is_changed = true;
  }
  return ret;
}

int ObILSRestoreState::request_leader_status_(ObLSRestoreStatus &leader_restore_status)
{
  int ret = OB_SUCCESS;
  ObStorageHASrcInfo leader;
  ObLSService *ls_service = nullptr;
  ObStorageRpc *storage_rpc = nullptr;
  obrpc::ObInquireRestoreResp restore_resp;
  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_rpc should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(get_leader_(leader))) {
    LOG_WARN("fail to get follower server", K(ret));
  } else if (OB_FAIL(storage_rpc->inquire_restore(ls_restore_arg_->get_tenant_id(), leader, ls_->get_ls_id(), ls_restore_status_, restore_resp))) {
    LOG_WARN("fail to inquire restore status", K(ret), K(leader), KPC(ls_));
  } else if (!restore_resp.is_leader_) {
    LOG_INFO("ls may switch leader", K(ret), K(leader), KPC(ls_));
    leader_restore_status = ObLSRestoreStatus::Status::LS_RESTORE_STATUS_MAX;
  } else {
    leader_restore_status = restore_resp.restore_status_;
    LOG_INFO("get leader restore status", K(leader_restore_status), KPC(ls_));
  }
  return ret;
}

int ObILSRestoreState::get_leader_(ObStorageHASrcInfo &leader)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ls_restore_arg_->get_tenant_id();
  leader.cluster_id_ = cluster_id_;
  if (OB_FAIL(location_service_->get_leader_with_retry_until_timeout(leader.cluster_id_, tenant_id, ls_->get_ls_id(), leader.src_addr_))) {
    LOG_WARN("fail to get ls leader server", K(ret), K(leader), K(tenant_id), KPC(ls_));
  }
  return ret;
}

int ObILSRestoreState::leader_fill_tablet_group_restore_arg_(
    const ObIArray<ObTabletID> &tablet_need_restore,
    const ObTabletRestoreAction::ACTION &action,
    ObTabletGroupRestoreArg &tablet_group_restore_arg)
{
  int ret = OB_SUCCESS;
  tablet_group_restore_arg.reset();
  if (tablet_need_restore.empty() || !ObTabletRestoreAction::is_valid(action)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet need restore can not empty", K(ret), K(tablet_need_restore), K(action));
  } else {
    tablet_group_restore_arg.tenant_id_ = ls_restore_arg_->get_tenant_id();
    tablet_group_restore_arg.ls_id_ = ls_->get_ls_id();
    tablet_group_restore_arg.is_leader_ = true;
    if (OB_FAIL(append(tablet_group_restore_arg.tablet_id_array_, tablet_need_restore))) {
      LOG_WARN("fail to append tablet id", K(ret), K(tablet_need_restore));
    } else if (OB_FAIL(tablet_group_restore_arg.restore_base_info_.copy_from(*ls_restore_arg_))) {
      LOG_WARN("fail to fill restore base info from ls restore args", K(ret), KPC(ls_restore_arg_));
    } else {
      tablet_group_restore_arg.action_ = action;
    }
  }
  return ret;
}

int ObILSRestoreState::follower_fill_tablet_group_restore_arg_(
    const ObIArray<ObTabletID> &tablet_need_restore,
    const ObTabletRestoreAction::ACTION &action,
    ObTabletGroupRestoreArg &tablet_group_restore_arg)
{
  int ret = OB_SUCCESS;
  tablet_group_restore_arg.reset();
  uint64_t tenant_id = ls_restore_arg_->get_tenant_id();
  bool is_cache_hit = false;
  const int64_t expire_renew_time = INT64_MAX;
  share::ObLSLocation location;
  if (tablet_need_restore.empty() || !ObTabletRestoreAction::is_valid(action)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet need restore can not empty", K(ret), K(tablet_need_restore), K(action));
  } else {
    share::ObLSReplicaLocation leader;
    tablet_group_restore_arg.tenant_id_ = ls_restore_arg_->get_tenant_id();
    tablet_group_restore_arg.ls_id_ = ls_->get_ls_id();
    tablet_group_restore_arg.is_leader_ = false;
    if (OB_FAIL(location_service_->get(cluster_id_, tenant_id, ls_->get_ls_id(), expire_renew_time, is_cache_hit, location))) {
      LOG_WARN("fail to get location", K(ret), KPC(ls_));
    } else if (OB_FAIL(location.get_leader(leader))) {
      LOG_WARN("fail to get leader location", K(ret), K(location));
    } else if (OB_FAIL(tablet_group_restore_arg.src_.init(
                        leader.get_server(), 0/*invalid timestamp is ok*/, leader.get_replica_type()))) {
      LOG_WARN("fail to init src_", K(ret), K(leader));
    } else if (OB_FAIL(tablet_group_restore_arg.dst_.init(
                        GCTX.self_addr(), 0/*invalid timestamp is ok*/, REPLICA_TYPE_FULL))) {
      LOG_WARN("fail to init dst_", K(ret), K(GCTX.self_addr()));
    } else if (OB_FAIL(append(tablet_group_restore_arg.tablet_id_array_, tablet_need_restore))) {
      LOG_WARN("fail to append tablet id", K(ret), K(tablet_need_restore));
    } else if (OB_FAIL(tablet_group_restore_arg.restore_base_info_.copy_from(*ls_restore_arg_))) {
      LOG_WARN("fail to fill restore base info from ls restore args", K(ret), KPC(ls_restore_arg_));
    } else {
      tablet_group_restore_arg.action_ = action;
    }
  }
  return ret;
}

int ObILSRestoreState::notify_follower_restore_tablet_(const ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObStorageHASrcInfo> follower;
  ObLSService *ls_service = nullptr;
  ObStorageRpc *storage_rpc = nullptr;
  obrpc::ObNotifyRestoreTabletsResp restore_resp;
  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_rpc should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(get_follower_server_(follower))) {
    LOG_WARN("fail to get follower server", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < follower.count(); ++i) {
      const ObStorageHASrcInfo &follower_info = follower.at(i);
      if (OB_FAIL(storage_rpc->notify_restore_tablets(ls_restore_arg_->get_tenant_id(), follower_info, ls_->get_ls_id(),
          proposal_id_, tablet_ids, ls_restore_status_, restore_resp))) {
        LOG_WARN("fail to notify follower restore tablets", K(ret), K(follower_info), K(tablet_ids), KPC(ls_));
      }
    }
  }

  return ret;
}

int ObILSRestoreState::get_follower_server_(ObIArray<ObStorageHASrcInfo> &follower)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ls_restore_arg_->get_tenant_id();
  bool is_cache_hit = false;
  share::ObLSLocation location;
  ObStorageHASrcInfo follower_info;
  follower_info.cluster_id_ = cluster_id_;
  const int64_t expire_renew_time = INT64_MAX;
  logservice::ObLogHandler *log_handler = nullptr;
  int64_t paxos_replica_num = 0;
  common::ObMemberList member_list;
  GlobalLearnerList learner_list;
  int64_t full_replica_count = 0;
  int64_t non_paxos_replica_count = 0;
  if (OB_ISNULL(log_handler = ls_->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler should not be NULL", K(ret));
  } else if (OB_FAIL(log_handler->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, learner_list))) {
    LOG_WARN("failed to get paxos member list and learner list", K(ret));
  } else if (OB_FAIL(location_service_->get(follower_info.cluster_id_, tenant_id, ls_->get_ls_id(), expire_renew_time, is_cache_hit, location))) {
    LOG_WARN("fail to get location", K(ret), KPC(ls_));
  } else if (OB_FAIL(location.get_replica_count(full_replica_count, non_paxos_replica_count))) {
    LOG_WARN("fail to get replica count in location", KR(ret), K(location), K(full_replica_count), K(non_paxos_replica_count));
  } else if (full_replica_count != paxos_replica_num || non_paxos_replica_count != learner_list.get_member_number()) {
    ret = OB_REPLICA_NUM_NOT_MATCH;
    LOG_WARN("replica num not match, ls may in migration", K(ret), K(location), K(full_replica_count),
             K(non_paxos_replica_count), K(member_list), K(paxos_replica_num), K(learner_list));
  } else {
    const ObIArray<share::ObLSReplicaLocation> &replica_locations = location.get_replica_locations();
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_locations.count(); ++i) {
      const ObLSReplicaLocation &replica = replica_locations.at(i);
      follower_info.src_addr_ = replica.get_server();
      if (GCTX.self_addr() == replica.get_server()) { // just skip
      } else if (OB_FAIL(follower.push_back(follower_info))) {
        LOG_WARN("fail to push back follower info", K(ret), K(follower_info));
      }
    }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_INSERT_GET_FOLLOWER_SERVER_FAILED ? : OB_SUCCESS;
  }
#endif
  }
  return ret;
}
int ObILSRestoreState::check_all_follower_restore_finish_(bool &finish)
{
  int ret = OB_SUCCESS;
  ObArray<ObStorageHASrcInfo> follower;
  ObLSService *ls_service = nullptr;
  ObStorageRpc *storage_rpc = nullptr;
  obrpc::ObInquireRestoreResp restore_resp;
  finish = true;
  ObArray<ObTabletID> tablet_ids;
  if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_rpc should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(get_follower_server_(follower))) {
    if (ret == OB_REPLICA_NUM_NOT_MATCH) {
      finish = false;
      ret = OB_SUCCESS;
      LOG_INFO("replica num not match, wait add replica.", K(follower), KPC(ls_));
    } else {
      LOG_WARN("fail to get follower server", K(ret));
    }
  } else {
    bool is_finish = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < follower.count(); ++i) {
      const ObStorageHASrcInfo &follower_info = follower.at(i);
      is_finish = false;
      if (OB_FAIL(storage_rpc->inquire_restore(ls_restore_arg_->get_tenant_id(), follower_info, ls_->get_ls_id(),
          ls_restore_status_, restore_resp))) {
        LOG_WARN("fail to inquire restore status", K(ret), K(follower_info), K(tablet_ids), KPC(ls_));
      } else if (OB_FAIL(check_follower_restore_finish(ls_restore_status_, restore_resp.restore_status_, is_finish))) {
        LOG_WARN("fail to check follower restore finish", K(ret), KPC(ls_), K(ls_restore_status_), K(restore_resp));
      } else if (is_finish) { // do nothing
      } else {
        finish = false;
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("follower restore status not match leader status", K(ls_restore_status_), K(restore_resp));
        }
        break;
      }
    }
  }
  return ret;
}

int ObILSRestoreState::check_follower_restore_finish(const share::ObLSRestoreStatus &leader_status,
    const share::ObLSRestoreStatus &follower_status, bool &is_finish)
{
  int ret = OB_SUCCESS;
  is_finish = false;
  if (!leader_status.is_wait_status() && !leader_status.is_quick_restore_finish()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only leader wait status and quick restore finish status need to check follower restore status",
        K(ret), K(leader_status));
  } else if (leader_status == follower_status) {
    is_finish = true;
  } else if (leader_status.is_wait_restore_major_data() && follower_status.is_none()) {
    is_finish = true;
  } else if (leader_status.get_status() < follower_status.get_status()) {
    // when switch leader, follower state may ahead leader
    is_finish = true;
  }
  return ret;
}

bool ObILSRestoreState::check_leader_restore_finish_(
    const share::ObLSRestoreStatus &leader_status,
    const share::ObLSRestoreStatus &follower_status) const
{
  bool ret = false;
  if (!leader_status.is_valid_restore_status() || leader_status.is_failed()) {
    // leader may restore failed or switch leader
  } else if (leader_status.is_none()) {
    ret= true;
  } else if (leader_status.get_status() > follower_status.get_status()) {
    ret = true;
  }
  return ret;
}

int ObILSRestoreState::check_restore_concurrency_limit_(bool &reach_limit)
{
  int ret = OB_SUCCESS;
  const ObDagNetType::ObDagNetTypeEnum restore_type = ObDagNetType::DAG_NET_TYPE_RESTORE;
  ObTenantDagScheduler *scheduler = nullptr;
  int64_t restore_concurrency = 0;
  reach_limit = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    restore_concurrency = tenant_config->ha_high_thread_score;
  }
  if (0 == restore_concurrency) {
    restore_concurrency = OB_DEFAULT_RESTORE_CONCURRENCY;
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS restore state do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (0 == restore_concurrency) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore concurrency should not be 0", K(ret), K(restore_concurrency));
  } else {
    const int64_t restore_dag_net_count = scheduler->get_dag_net_count(restore_type);

    if (restore_dag_net_count >= restore_concurrency) {
      reach_limit = true;
      if (REACH_THREAD_TIME_INTERVAL(1000 * 1000 * 60 * 5)) {
        LOG_INFO("ls restore reach limit", K(ret), K(restore_concurrency), K(restore_dag_net_count));
      }
    }
  }
  return ret;
}

int ObILSRestoreState::inner_check_can_do_restore_(bool &can_do_restore)
{
  int ret = OB_SUCCESS;
  bool reach_limit = false;
  if (tablet_mgr_.has_no_tablets_to_restore()) {
    // If all tablets have been restored, then we need push the
    // restore state to restore done.
    can_do_restore = true;
  } else if (OB_FAIL(check_restore_concurrency_limit_(reach_limit))) {
    LOG_WARN("failed to check restore concurrency limit", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (reach_limit) {
    can_do_restore = false;
  } else {
    can_do_restore = true;
  }
  return ret;
}

int ObILSRestoreState::online_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_->online())) {
    LOG_WARN("online ls failed", K(ret), KPC(ls_));
  }
  return ret;
}

int ObILSRestoreState::schedule_tablet_group_restore_(
    const ObTabletGroupRestoreArg &arg,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool reach_limit = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS restore state do not init", K(ret));
  } else if (!arg.is_valid() || task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule tablet group restore get invalid argument", K(ret), K(arg), K(task_id));
  } else if (OB_FAIL(check_restore_concurrency_limit_(reach_limit))) {
    LOG_WARN("failed to check restore concurrency limit", K(ret), K(arg), K(task_id));
  } else if (reach_limit) { // wait next schedule.
  } else if (OB_FAIL(schedule_tablet_group_restore_dag_net_(arg, task_id))) {
    LOG_WARN("failed to schedule tablet group restore dag net", K(ret), K(arg), K(task_id));
  } else {
    STORAGE_LOG(INFO, "succeed to schedule tablet group restore", K(arg), K(task_id));
  }
  return ret;
}

int ObILSRestoreState::schedule_tablet_group_restore_dag_net_(
    const ObTabletGroupRestoreArg &arg,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ObTGRDagNetInitParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS restore state do not init", K(ret));
  } else if (OB_FAIL(param.arg_.assign(arg))) {
    LOG_WARN("failed to assign ls restore arg", K(ret), K(arg), K(task_id));
  } else {
    param.task_id_ = task_id;
    param.bandwidth_throttle_ = bandwidth_throttle_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;

    ObTenantDagScheduler *scheduler = nullptr;
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObTabletGroupRestoreDagNet>(&param))) {
      LOG_WARN("failed to create and add tablet group restore dag net", K(ret), K(arg), K(task_id));
    } else {
      SERVER_EVENT_ADD("storage_ha", "restore_tablet_group",
        "tenant_id", MTL_ID(),
        "ls_id", arg.ls_id_.id(),
        "status", ls_restore_status_.get_status(),
        "task_id", task_id,
        "action", arg.action_,
        "tablet_count", arg.tablet_id_array_.count());
      LOG_INFO("success to create tablet group restore dag net", K(ret), K(arg), K(task_id));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_SKIP_RESTORE_SYS_TABLETS_DAG_NET);
int ObILSRestoreState::schedule_ls_restore_(
    const ObLSRestoreArg &arg,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  bool reach_limit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS restore state do not init", K(ret));
  } else if (!arg.is_valid() || task_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schedule ls restore get invalid argument", K(ret), K(arg), K(task_id));
#ifdef ERRSIM
  } else if (OB_SUCCESS != EN_SKIP_RESTORE_SYS_TABLETS_DAG_NET) {
    LOG_ERROR("errsim EN_SKIP_RESTORE_SYS_TABLETS_DAG_NET");
#endif
  } else if (OB_FAIL(check_restore_concurrency_limit_(reach_limit))) {
    LOG_WARN("failed to check restore concurrency limit", K(ret), K(arg), K(task_id));
  } else if (reach_limit) { // wait next schedule.
  } else if (OB_FAIL(schedule_ls_restore_dag_net_(arg, task_id))) {
    LOG_WARN("failed to schedule tablet group restore dag net", K(ret), K(arg), K(task_id));
  } else {
    STORAGE_LOG(INFO, "succeed to schedule tablet group restore", K(arg), K(task_id));
  }
  return ret;
}

int ObILSRestoreState::schedule_ls_restore_dag_net_(
    const ObLSRestoreArg &arg,
    const share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNetInitParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS restore state do not init", K(ret));
  } else if (OB_FAIL(param.arg_.assign(arg))) {
    LOG_WARN("failed to assign ls restore arg", K(ret), K(arg), K(task_id));
  } else {
    param.task_id_ = task_id;
    param.bandwidth_throttle_ = bandwidth_throttle_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;

    ObTenantDagScheduler *scheduler = nullptr;
    if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret), KP(scheduler));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObLSRestoreDagNet>(&param))) {
      LOG_WARN("failed to create and add ls restore dag net", K(ret), K(arg), K(task_id));
    } else {
      LOG_INFO("success to create ls restore dag net", K(ret), K(arg), K(task_id));
    }
  }
  return ret;
}

int ObILSRestoreState::check_replay_to_target_scn_(
    const share::SCN &target_scn,
    bool &replayed) const
{
  int ret = OB_SUCCESS;
  replayed = false;
  rootserver::ObLSRecoveryStatHandler *ls_recovery_stat_handler = nullptr;
  share::SCN readable_scn;
  if (!target_scn.is_valid_and_not_min()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target scn", K(ret), K(target_scn));
  } else if (OB_ISNULL(ls_recovery_stat_handler = ls_->get_ls_recovery_stat_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls recovery stat handler must not be null", K(ret));
  } else if (OB_FAIL(ls_recovery_stat_handler->get_ls_replica_readable_scn(readable_scn))) {
    LOG_WARN("failed to get ls replica readable scn", K(ret), KPC(ls_));
  } else if (target_scn <= readable_scn) {
    replayed = true;
    LOG_INFO("clog replay to target scn finish", K(target_scn), K(readable_scn), KPC(ls_));
  }
  return ret;
}

int ObILSRestoreState::report_start_replay_clog_lsn_()
{
  int ret = OB_SUCCESS;
  LSN lsn;
  if (need_report_clog_lsn_) {
    if (OB_FAIL(ls_->get_log_handler()->get_end_lsn(lsn))) {
      LOG_WARN("failed to get end lsn", K(ret), KPC_(ls));
    } else {
      SERVER_EVENT_ADD("storage_ha", "log_restore_start_lsn",
        "tenant_id", MTL_ID(),
        "ls_id", ls_->get_ls_id().id(),
        "lsn", lsn.val_,
        "curr_status_str", ObLSRestoreStatus::get_restore_status_str(ls_restore_status_));
      ATOMIC_SET(&need_report_clog_lsn_, false);
    }
  }
  return ret;
}

int ObILSRestoreState::report_finish_replay_clog_lsn_()
{
  int ret = OB_SUCCESS;
  LSN lsn;
  if (need_report_clog_lsn_) {
    if (OB_FAIL(ls_->get_log_handler()->get_end_lsn(lsn))) {
      LOG_WARN("failed to get end lsn", K(ret), KPC_(ls));
    } else {
      SERVER_EVENT_ADD("storage_ha", "log_restore_finish_lsn",
        "tenant_id", MTL_ID(),
        "ls_id", ls_->get_ls_id().id(),
        "lsn", lsn.val_,
        "curr_status_str", ObLSRestoreStatus::get_restore_status_str(ls_restore_status_));
      ATOMIC_SET(&need_report_clog_lsn_, false);
    }
  }
  return ret;
}

int ObILSRestoreState::add_finished_tablet_cnt(const int64_t inc_finished_tablet_cnt)
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FALSE_IT(ls_restore_handler = ls_->get_ls_restore_handler())) {
  } else if (OB_FAIL(ls_restore_handler->restore_stat().add_finished_tablet_cnt(inc_finished_tablet_cnt))) {
    LOG_WARN("failed to add finished tablet cnt", K(ret), KPC_(ls), K(inc_finished_tablet_cnt));
  }

  return ret;
}

int ObILSRestoreState::report_unfinished_tablet_cnt(const int64_t unfinished_tablet_cnt)
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FALSE_IT(ls_restore_handler = ls_->get_ls_restore_handler())) {
  } else if (OB_FAIL(ls_restore_handler->restore_stat().report_unfinished_tablet_cnt(unfinished_tablet_cnt))) {
    LOG_WARN("failed to report unfinished tablet cnt", K(ret), KPC_(ls), K(unfinished_tablet_cnt));
  }

  return ret;
}

int ObILSRestoreState::add_finished_bytes(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FALSE_IT(ls_restore_handler = ls_->get_ls_restore_handler())) {
  } else if (OB_FAIL(ls_restore_handler->restore_stat().add_finished_bytes(bytes))) {
    LOG_WARN("failed to add finished bytes", K(ret), KPC_(ls), K(bytes));
  }

  return ret;
}

int ObILSRestoreState::report_unfinished_bytes(const int64_t bytes)
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FALSE_IT(ls_restore_handler = ls_->get_ls_restore_handler())) {
  } else if (OB_FAIL(ls_restore_handler->restore_stat().report_unfinished_bytes(bytes))) {
    LOG_WARN("failed to report unfinished bytes", K(ret), KPC_(ls), K(bytes));
  }

  return ret;
}

void ObILSRestoreState::notify_rs_restore_finish_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ls_restore_arg_->tenant_id_;
  common::ObAddr leader_addr;
  obrpc::ObNotifyLSRestoreFinishArg arg;
  arg.set_tenant_id(tenant_id);
  arg.set_ls_id(ls_->get_ls_id());

  if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy or location service is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
              GCONF.cluster_id, gen_meta_tenant_id(tenant_id), ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("failed to get meta tenant leader address", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).by(tenant_id).notify_ls_restore_finish(arg))) {
    LOG_WARN("failed to notify tenant restore scheduler", KR(ret), K(leader_addr), K(arg));
  }
}

int ObILSRestoreState::check_restore_pre_finish_(bool &is_finish) const
{
  int ret = OB_SUCCESS;
  is_finish = false;
  share::ObPhysicalRestoreTableOperator restore_table_operator;
  const uint64_t tenant_id = ls_->get_tenant_id();
  if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table operator", K(ret), K(tenant_id));
  } else {
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job_info))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else if (share::PhysicalRestoreStatus::PHYSICAL_RESTORE_PRE >= job_info.get_status()) {
        is_finish = false;
      } else {
        is_finish = true;
      }
    }
  }
  return ret;
}

int ObILSRestoreState::check_ls_leader_ready_(bool &is_ready)
{
  int ret = OB_SUCCESS;
  is_ready = false;
  uint64_t tenant_id = ls_->get_tenant_id();
  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    ObSqlString sql;
    common::sqlclient::ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql.assign_fmt("select count(*) ls_count from %s where ls_id=%ld and role = 1",
        OB_ALL_LS_META_TABLE_TNAME, ls_->get_ls_id().id()))) {
      LOG_WARN("fail to assign sql", K(ret));
    } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(tenant_id), sql.ptr(), share::OBCG_STORAGE))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is nullptr", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("no result", K(ret), K(sql));
    } else {
      int64_t count = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, OB_STR_LS_COUNT, count, int64_t);
      if (OB_FAIL(ret)) {
      } else {
        is_ready = 1 == count ? true : false;
      }
    }
  }
  return ret;
}

int ObILSRestoreState::advance_restore_status(const ObLSRestoreStatus &next_status)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("restore state not init", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  }
  return ret;
}

int ObILSRestoreState::check_clog_replay_finish_(bool &is_finish)
{
  int ret = OB_SUCCESS;
  bool done = false;
  logservice::ObLogRestoreHandler *log_restore_handle = nullptr;
  if (OB_ISNULL(log_restore_handle = ls_->get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore handler can't nullptr", K(ret));
  } else if (OB_FAIL(log_restore_handle->check_restore_done(ls_restore_arg_->get_restore_scn(), done))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to check clog replay done", K(ret), KPC(ls_));
    }
  } else if (done) {
    is_finish = true;
  }
  return ret;
}