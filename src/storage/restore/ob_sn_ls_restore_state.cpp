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
#include "ob_sn_ls_restore_state.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"
#include "storage/ls/ob_ls.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
using namespace oceanbase;

//================================ObLSRestoreStartState=======================================
ObLSRestoreStartState::ObLSRestoreStartState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_START)
{
}

ObLSRestoreStartState::~ObLSRestoreStartState()
{
}

int ObLSRestoreStartState::do_restore()
{
  DEBUG_SYNC(BEFORE_RESTORE_START);
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS);
  logservice::ObLogRestoreHandler *log_restore_handle = nullptr;
  bool is_created = false;
  bool is_exist = true;
  bool is_ready = false;
  bool is_finish = false;
  LOG_INFO("ready to start restore ls", K(ls_restore_status_), KPC(ls_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(log_restore_handle = ls_->get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore handle can't nullptr", K(ret), K(log_restore_handle));
  } else if (OB_FAIL(check_restore_pre_finish_(is_finish))) {
    LOG_WARN("fail to check can advance status", K(ret), KPC(ls_));
  } else if (!is_finish) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("restore_pre has not finished, wait later", KPC(ls_));
    }
  } else if (OB_FAIL(check_ls_created_(is_created))) {
    LOG_WARN("fail to check ls created", K(ret), KPC(ls_));
  } else if (!is_created) {
    if (OB_FAIL(do_with_uncreated_ls_())) {
      LOG_WARN("fail to do with uncreated ls", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(check_ls_leader_ready_(is_ready))) {
    LOG_WARN("fail to check is ls leader ready", K(ret), KPC(ls_));
  } else if (!is_ready) {
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("ls leader is not ready now, wait later", KPC(ls_));
    }
  } else if (OB_FAIL(insert_initial_ls_restore_progress_())) {
    LOG_WARN("fail to insert initial ls restore progress", K(ret), KPC(ls_));
  } else if (OB_FAIL(check_ls_meta_exist_(is_exist))) {
    LOG_WARN("fail to check ls meta exist", K(ret));
  } else if (!is_exist) {
    if (OB_FAIL(do_with_no_ls_meta_())) {
      LOG_WARN("fail to do with no meta ls", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  }
  return ret;
}

int ObLSRestoreStartState::do_with_no_ls_meta_()
{
  int ret = OB_SUCCESS;
  // ls with no ls meta means it created after backup ls_attr_infos.
  // this ls doesn't have ls meta and tablet in backup, it only needs to replay clog.
  ObLSRestoreStatus next_status;
  bool is_finish = false;
  if (OB_FAIL(online_())) {
    LOG_WARN("fail to online ls", K(ret), KPC_(ls));
  } else if (OB_FAIL(check_replay_to_target_scn_(ls_restore_arg_->get_consistent_scn(), is_finish))) {
    LOG_WARN("failed to check clog replay to consistent scn", K(ret));
  } else if (!is_finish) {
    // the ls is created before consistent scn
    next_status = ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN;
  } else {
    // the ls is created after consistent scn
    next_status = ObLSRestoreStatus::Status::WAIT_RESTORE_TO_CONSISTENT_SCN;
  }

  if (FAILEDx(report_start_replay_clog_lsn_())) {
    LOG_WARN("fail to report start replay clog lsn", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), K(*ls_), K(next_status));
  }

  return ret;
}

int ObLSRestoreStartState::do_with_uncreated_ls_()
{
  int ret = OB_SUCCESS;
  bool restore_finish = false;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::NONE);
  bool is_created = false;
  if (OB_FAIL(check_sys_ls_restore_finished_(restore_finish))) {
    LOG_WARN("fail to check sys ls restore finished", K(ret), KPC(this));
  } else if (!restore_finish) {
  } else if (OB_FAIL(check_ls_created_(is_created))) { // double check ls created
    LOG_WARN("fail to check ls created", K(ret), KPC(ls_));
  } else if (is_created) {
    // creating ls finished after sys ls restored. cur ls need to do restore.
  } else if (OB_FAIL(online_())) {
    LOG_WARN("fail to enable log", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  } else {
    // creating ls not finished after sys ls restored. cur ls no need to do restore.
    LOG_INFO("no need to restore when sys ls has been restored and the ls doesn't created.", KPC(ls_));
  }

  return ret;
}

int ObLSRestoreStartState::inc_need_restore_ls_cnt_()
{
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  common::ObMySQLTransaction trans;
  ObLSRestoreJobPersistKey key;
  key.job_id_ = ls_restore_arg_->job_id_;
  key.tenant_id_ = ls_restore_arg_->tenant_id_;
  key.ls_id_ = ls_->get_ls_id();
  key.addr_ = self_addr_;
  if (OB_FAIL(helper.init(key.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init helper", K(ret), K(key.tenant_id_));
  } else if (OB_FAIL(trans.start(proxy_, gen_meta_tenant_id(key.tenant_id_)))) {
    LOG_WARN("fail to start trans", K(ret), K(key.tenant_id_));
  } else {
    if (OB_FAIL(helper.inc_need_restore_ls_count_by_one(trans, key, ret))) {
      LOG_WARN("fail to inc finished ls cnt", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("fail to commit trans", KR(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("fail to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObLSRestoreStartState::check_ls_created_(bool &is_created)
{
  int ret = OB_SUCCESS;
  is_created = false;
  uint64_t tenant_id = MTL_ID();
  uint64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObLSStatusInfo status_info;
  ObLSStatusOperator ls_status_operator;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is nullptr is unexpected", K(ret));
  } else if (OB_FAIL(ls_status_operator.get_ls_status_info(user_tenant_id, ls_->get_ls_id(), status_info, *sql_proxy, share::OBCG_STORAGE))) {
    LOG_WARN("fail to get ls status info", K(ret), K(user_tenant_id), "ls_id", ls_->get_ls_id());
  } else if (!status_info.ls_is_create_abort() && !status_info.ls_is_creating()) {
    is_created = true;
  } else {
    LOG_WARN("ls has not been created, wait later", KPC(ls_));
  }
  return ret;
}

int ObLSRestoreStartState::check_ls_meta_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_backup_set_info_exist = false;
  ObBackupDataStore store;
  const ObArray<share::ObBackupSetBriefInfo> &backup_set_array = ls_restore_arg_->get_backup_set_list();
  int idx = backup_set_array.count() - 1;
  ObLSMetaPackage ls_meta_packge;
  is_exist = false;
  if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_set_array can't empty", K(ret), K(ls_restore_arg_));
  } else if (OB_FAIL(store.init(backup_set_array.at(idx).backup_set_path_.ptr()))) {
    LOG_WARN("fail to init backup data store", K(ret));
  } else if (OB_FAIL(store.read_ls_meta_infos(ls_->get_ls_id(), ls_meta_packge))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else if (OB_OBJECT_NOT_EXIST == ret
      && OB_TMP_FAIL(store.is_backup_set_info_file_exist(is_backup_set_info_exist))) {
      LOG_WARN("fail to check backup set info file exist", K(tmp_ret));
    } else if ((OB_OBJECT_NOT_EXIST == ret && !is_backup_set_info_exist)) {
      ret = OB_CANNOT_ACCESS_BACKUP_SET; // overwrite ret
      LOG_WARN("cannot access backup set file, please check backup media connectivity.",
        K(ret), K(is_backup_set_info_exist));
    } else {
      LOG_WARN("fail to read backup set info", K(ret));
    }
  } else if (ls_meta_packge.is_valid()) {
    is_exist = true;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta package", K(ret), K(ls_meta_packge));
  }
  return ret;
}

int ObLSRestoreStartState::check_sys_ls_restore_finished_(bool &restore_finish)
{
  int ret = OB_SUCCESS;
  // check all sys ls replicas are restore finished.
  restore_finish = false;
  uint64_t tenant_id = ls_restore_arg_->get_tenant_id();
  bool is_cache_hit = false;
  share::ObLSLocation location;
  const int64_t expire_renew_time = INT64_MAX;
  if (OB_FAIL(location_service_->get(cluster_id_, tenant_id, ObLSID(ObLSID::SYS_LS_ID), expire_renew_time,
      is_cache_hit, location))) {
    LOG_WARN("fail to get sys ls location", K(ret), KPC(ls_));
  } else {
    bool tmp_finish = true;
    const ObIArray<share::ObLSReplicaLocation> &replica_locations = location.get_replica_locations();
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_locations.count(); ++i) {
      const ObLSReplicaLocation &replica = replica_locations.at(i);
      if (replica.get_restore_status().is_none()) {
      } else {
        tmp_finish = false;
      }
    }

    if (OB_SUCC(ret) && tmp_finish) {
      restore_finish = true;
    }
  }
  return ret;
}

//================================ObLSRestoreSysTabletState=======================================

ObLSRestoreSysTabletState::ObLSRestoreSysTabletState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS),
    retry_flag_(false)
{
}

ObLSRestoreSysTabletState::~ObLSRestoreSysTabletState()
{
}

int ObLSRestoreSysTabletState::do_restore()
{
  DEBUG_SYNC(BEFORE_RESTORE_SYS_TABLETS);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(update_role_())) {
    LOG_WARN("fail to update role and status", K(ret), KPC(this));
  } else if (!is_follower(role_) && OB_FAIL(leader_restore_sys_tablet_())) {
    LOG_WARN("fail to do leader restore sys tablet", K(ret), KPC(this));
  } else if (is_follower(role_) && OB_FAIL(follower_restore_sys_tablet_())) {
    LOG_WARN("fail to do follower restore sys tablet", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
    set_retry_flag();
  }
  return ret;
}

int ObLSRestoreSysTabletState::leader_restore_sys_tablet_()
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS);
  ObArray<common::ObTabletID> no_use_tablet_ids;
  LOG_INFO("ready to restore leader sys tablet", K(ls_restore_status_), KPC(ls_));
  if (tablet_mgr_.has_no_tablets_restoring()) {
    if (OB_FAIL(do_restore_sys_tablet())) {
      LOG_WARN("fail to do restore sys tablet", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(tablet_mgr_.remove_restored_tablets(no_use_tablet_ids))) {
    LOG_WARN("fail to pop restored tablets", K(ret));
  } else if (!tablet_mgr_.has_no_tablets_restoring()) {// TODO: check restore finish, should read from extern. fix later
  } else if (is_need_retry_()) {
    // next term to retry
  } else if (OB_FAIL(online_())) {
    LOG_WARN("fail to load ls inner tablet", K(ret));
  } else if (OB_FAIL(ls_->get_ls_restore_handler()->update_rebuild_seq())) {
    LOG_WARN("failed to update rebuild seq", K(ret), KPC(ls_));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  } else {
    LOG_INFO("leader succ to restore sys tablet", KPC(ls_));
  }

  return ret;
}

int ObLSRestoreSysTabletState::follower_restore_sys_tablet_()
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS);
  ObArray<common::ObTabletID> no_use_tablet_ids;
  LOG_INFO("ready to restore follower sys tablet", K(ls_restore_status_), KPC(ls_));
  if (tablet_mgr_.has_no_tablets_restoring()) {
    bool finish = false;
    if (OB_FAIL(check_leader_restore_finish(finish))) {
      LOG_WARN("fail to check leader restore finish", K(ret), KPC(ls_));
    } else if (!finish) {
    } else if (OB_FAIL(do_restore_sys_tablet())) {
      LOG_WARN("fail to do restore sys tablet", K(ret), KPC(ls_));
    }
  } else if (OB_FAIL(tablet_mgr_.remove_restored_tablets(no_use_tablet_ids))) {
    LOG_WARN("fail to handle restoring tablets", K(ret), KPC(ls_));
  } else if (!tablet_mgr_.has_no_tablets_restoring()) {
  } else if (is_need_retry_()) {
    // next term to retry
  } else if (OB_FAIL(online_())) {
    LOG_WARN("fail to load ls inner tablet", K(ret));
  } else if (OB_FAIL(ls_->get_ls_restore_handler()->update_rebuild_seq())) {
    LOG_WARN("failed to update rebuild seq", K(ret), KPC(ls_));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
  } else {
    LOG_INFO("follower succ to restore sys tablet", KPC(ls_));
  }

  return ret;
}

int ObLSRestoreSysTabletState::do_restore_sys_tablet()
{
  int ret = OB_SUCCESS;
  ObLSRestoreArg arg;
  uint64_t tenant_id = arg.tenant_id_;
  ObTaskId task_id;
  task_id.init(GCONF.self_addr_);
  // always restore from backup.
  if (OB_FAIL(leader_fill_ls_restore_arg_(arg))) {
    LOG_WARN("fail to fill ls restore arg", K(ret));
  }
#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.
  if (!is_follower(role_) && OB_FAIL(leader_fill_ls_restore_arg_(arg))) {
    LOG_WARN("fail to fill ls restore arg", K(ret));
  } else if (is_follower(role_) && OB_FAIL(follower_fill_ls_restore_arg_(arg))) {
    LOG_WARN("fail to fill ls restore arg", K(ret));
  }
#endif
  if (FAILEDx(tablet_mgr_.schedule_ls_restore(task_id))) {
    LOG_WARN("fail to schedule tablet", K(ret), KPC(ls_));
  } else if (OB_FAIL(schedule_ls_restore_(arg, task_id))) {
    LOG_WARN("fail to schedule restore sys tablet", KR(ret), K(arg), K(task_id));
  } else {
    LOG_INFO("success to schedule restore sys tablet", K(ret));
  }
  return ret;
}

int ObLSRestoreSysTabletState::leader_fill_ls_restore_arg_(ObLSRestoreArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  arg.tenant_id_ = ls_restore_arg_->get_tenant_id();
  arg.ls_id_ = ls_->get_ls_id();
  arg.is_leader_ = true;
  if (OB_FAIL(arg.restore_base_info_.copy_from(*ls_restore_arg_))) {
    LOG_WARN("fail to fill restore base info from ls restore args", K(ret), KPC(ls_restore_arg_));
  }
  return ret;
}

int ObLSRestoreSysTabletState::follower_fill_ls_restore_arg_(ObLSRestoreArg &arg)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ls_restore_arg_->get_tenant_id();
  bool is_cache_hit = false;
  share::ObLSLocation location;
  arg.reset();
  arg.tenant_id_ = ls_restore_arg_->get_tenant_id();
  arg.ls_id_ = ls_->get_ls_id();
  arg.is_leader_ = false;
  share::ObLSReplicaLocation leader;
  const int64_t expire_renew_time = INT64_MAX;
  if (OB_FAIL(location_service_->get(cluster_id_, tenant_id, ls_->get_ls_id(), expire_renew_time,
      is_cache_hit, location))) {
    LOG_WARN("fail to get location", K(ret), KPC(ls_));
  } else if (OB_FAIL(location.get_leader(leader))) {
    LOG_WARN("fail to get leader location", K(ret), K(location));
  } else if (OB_FAIL(arg.src_.init(leader.get_server(), 0/*invalid timestamp is ok*/, leader.get_replica_type()))) {
    LOG_WARN("fail to init src_", K(ret), K(leader));
  } else if (OB_FAIL(arg.dst_.init(GCTX.self_addr(), 0/*invalid timestamp is ok*/, REPLICA_TYPE_FULL))) {
    LOG_WARN("fail to init dst_", K(ret), K(GCTX.self_addr()));
  } else if (OB_FAIL(arg.restore_base_info_.copy_from(*ls_restore_arg_))) {
    LOG_WARN("fail to fill restore base info from ls restore args", K(ret), KPC(ls_restore_arg_));
  }

  return ret;
}

bool ObLSRestoreSysTabletState::is_need_retry_()
{
  bool bret = retry_flag_;
  retry_flag_ = false;
  return bret;
}

//================================ObLSRestoreCreateUserTabletState=======================================

ObLSRestoreCreateUserTabletState::ObLSRestoreCreateUserTabletState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_TABLETS_META)
{
}

ObLSRestoreCreateUserTabletState::~ObLSRestoreCreateUserTabletState()
{
}

int ObLSRestoreCreateUserTabletState::do_restore()
{
  DEBUG_SYNC(BEFORE_RESTORE_TABLETS_META);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(update_role_())) {
    LOG_WARN("fail to update role and status", K(ret), KPC(this));
  } else if (!is_follower(role_) && OB_FAIL(leader_create_user_tablet_())) {
    LOG_WARN("fail to do leader create user tablet", K(ret), KPC(this));
  } else if (is_follower(role_) && OB_FAIL(follower_create_user_tablet_())) {
    LOG_WARN("fail to do follower create user tablet", K(ret), KPC(this));
  }
  return ret;
}

int ObLSRestoreCreateUserTabletState::leader_create_user_tablet_()
{
  int ret = OB_SUCCESS;
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  bool can_do_restore = false;
  LOG_INFO("ready to create leader user tablet", K(ls_restore_status_), KPC(ls_));
  if (OB_FAIL(tablet_mgr_.remove_restored_tablets(restored_tablets))) {
    LOG_WARN("fail to pop restored tablets", K(ret), KPC(ls_));
  } else if (OB_FAIL(inner_check_can_do_restore_(can_do_restore))) {
    LOG_WARN("failed to check can do restore", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (!can_do_restore) {
    // wait next schedule.
  } else if (OB_FAIL(tablet_mgr_.choose_tablets_to_restore(tablet_need_restore))) {
    LOG_WARN("fail to pop need restore tablets", K(ret), KPC(ls_));
  } else if (tablet_need_restore.empty()) {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META);
    if (!tablet_mgr_.is_restore_completed()) {
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
    } else {
      LOG_INFO("success create leader user tablets", KPC(ls_));
    }
  } else if (OB_FAIL(do_create_user_tablet_(tablet_need_restore))) {
    LOG_WARN("fail to do quick restore", K(ret), K(tablet_need_restore), KPC(ls_));
  }

#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.

  int tmp_ret = OB_SUCCESS; // try rpc's best
  if (restored_tablets.empty()) {
  } else if (OB_SUCCESS != (tmp_ret = notify_follower_restore_tablet_(restored_tablets))) {
    LOG_WARN("fail to notify follower restore tablet", K(tmp_ret), KPC(ls_));
  } else {
    LOG_INFO("success send tablets to follower for restore", K(restored_tablets));
  }
#endif

  return ret;
}

int ObLSRestoreCreateUserTabletState::follower_create_user_tablet_()
{
  int ret = OB_SUCCESS;
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  bool can_do_restore = false;
  LOG_INFO("ready to create follower user tablet", K(ls_restore_status_), KPC(ls_));
  if (OB_FAIL(tablet_mgr_.remove_restored_tablets(restored_tablets))) {
    LOG_WARN("fail to pop restored tablets", K(ret), KPC(ls_));
  } else if (OB_FAIL(inner_check_can_do_restore_(can_do_restore))) {
    LOG_WARN("failed to check can do restore", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (!can_do_restore) {
    // wait next schedule.
  } else if (OB_FAIL(tablet_mgr_.choose_tablets_to_restore(tablet_need_restore))) {
    LOG_WARN("fail to choose need restore tablets", K(ret), KPC(ls_));
  } else if (tablet_need_restore.empty()) {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META);
    ObLSRestoreStatus leader_restore_status;
    bool finish = false;
    if (!tablet_mgr_.is_restore_completed()) {
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
    } else {
      LOG_INFO("success create follower user tablets", KPC(ls_));
    }
  } else if (OB_FAIL(do_create_user_tablet_(tablet_need_restore))) {
    LOG_WARN("fail to do quick restore", K(ret), K(tablet_need_restore), KPC(ls_));
  }
  return ret;
}

int ObLSRestoreCreateUserTabletState::do_create_user_tablet_(
    const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore)
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreArg arg;
  ObTaskId task_id;
  task_id.init(GCONF.self_addr_);
  bool reach_dag_limit = false;
  bool is_new_election = false;
  // always restore from backup.
  if (OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill tablet group restore arg", K(ret));
  }

#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.
  if (!is_follower(role_) && OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill tablet group restore arg", K(ret));
  } else if (is_follower(role_) && OB_FAIL(follower_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill tablet group restore arg", K(ret));
  }
#endif

  if (FAILEDx(check_new_election_(is_new_election))) {
    LOG_WARN("fail to check change role", K(ret));
  } else if (is_new_election) {
    ret = OB_EAGAIN;
    LOG_WARN("new election, role may changed, retry later", K(ret), KPC(ls_));
  } else if (OB_FAIL(tablet_mgr_.schedule_tablet_group_restore(task_id, tablet_need_restore, reach_dag_limit))) {
    LOG_WARN("fail to schedule tablet", K(ret), K(tablet_need_restore), KPC(ls_));
  } else if (reach_dag_limit) {
    LOG_INFO("reach restore dag net max limit, wait later");
  } else if (OB_FAIL(schedule_tablet_group_restore_(arg, task_id))) {
    LOG_WARN("fail to schedule tablet group restore", KR(ret), K(arg));
  } else {
    LOG_INFO("success schedule create user tablet", K(ret), K(arg), K(ls_restore_status_));
  }
  return ret;
}


//================================ObLSRestoreConsistentScnState=======================================
int ObLSRestoreConsistentScnState::do_restore()
{
  int ret = OB_SUCCESS;
  LOG_INFO("ready to restore to consistent scn", K(ls_restore_status_), KPC(ls_));
  bool is_finish = false;
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_TO_CONSISTENT_SCN);
  if (OB_FAIL(update_role_())) {
    LOG_WARN("failed to update role", K(ret));
  } else if (OB_FAIL(check_recover_to_consistent_scn_finish(is_finish))) {
    LOG_WARN("failed to check clog replay to consistent scn", K(ret));
  } else if (!is_finish) { // do nothing
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
      LOG_INFO("clog replay not finish, wait later", KPC_(ls));
    }
  } else if (OB_FAIL(set_empty_for_transfer_tablets_())) {
    LOG_WARN("fail to set empty for transfer tablets", K(ret), KPC_(ls));
  } else if (OB_FAIL(report_finish_replay_clog_lsn_())) {
    LOG_WARN("fail to report finish replay clog lsn", K(ret));
  } else if (OB_FAIL(report_total_tablet_cnt_())) {
    LOG_WARN("fail to report total tablet cnt", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), KPC_(ls), K(next_status));
  } else {
    LOG_INFO("restore to consistent scn success", KPC_(ls));
  }

  return ret;
}

int ObLSRestoreConsistentScnState::check_recover_to_consistent_scn_finish(bool &is_finish) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_replay_to_target_scn_(ls_restore_arg_->get_consistent_scn(), is_finish))) {
    LOG_WARN("failed to check clog replay to consistent scn", K(ret));
  }

  return ret;
}

int ObLSRestoreConsistentScnState::set_empty_for_transfer_tablets_()
{
  int ret = OB_SUCCESS;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  const ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::EMPTY;

  if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_tablet_svr is nullptr", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->build_tablet_iter(iterator))) {
    LOG_WARN("fail to build tablet iterator", K(ret), KPC_(ls));
  }

  total_tablet_cnt_ = 0;
  while (OB_SUCC(ret)) {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletCreateDeleteMdsUserData user_data;
    mds::MdsWriter writer;// will be removed later
    mds::TwoPhaseCommitState trans_stat;// will be removed later
    share::SCN trans_version;// will be removed later
    if (OB_FAIL(iterator.get_next_tablet(tablet_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next tablet", K(ret));
      }
      break;
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is nullptr", K(ret), K(tablet_handle));
    } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
    } else if (tablet->is_empty_shell()) {
      LOG_INFO("skip empty shell", "tablet_id", tablet->get_tablet_meta().tablet_id_);
    } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_undefined()) {
      ++total_tablet_cnt_;
    } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_empty()) {
      ++total_tablet_cnt_;
    } else if (!tablet->get_tablet_meta().has_transfer_table()) {
    } else if (OB_FAIL(tablet->get_latest_tablet_status(user_data, writer, trans_stat, trans_version))) {
      LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
    } else if (mds::TwoPhaseCommitState::ON_COMMIT != trans_stat
        && ObTabletStatus::TRANSFER_IN == user_data.tablet_status_.get_status()) {
      LOG_INFO("skip tablet which transfer in not commit", "tablet_id", tablet->get_tablet_meta().tablet_id_, K(user_data));
    } else if (OB_FAIL(ls_->update_tablet_restore_status(tablet->get_reorganization_scn(),
                                                         tablet->get_tablet_meta().tablet_id_,
                                                         restore_status,
                                                         true/* need reset tranfser flag */,
                                                         false/*need_to_set_split_data_complete*/))) {
      LOG_WARN("failed to update tablet restore status to EMPTY", K(ret), KPC(tablet));
    } else {
      ++total_tablet_cnt_;
      LOG_INFO("update tablet restore status to EMPTY",
               "tablet_meta", tablet->get_tablet_meta());
    }
  }

  return ret;
}

int ObLSRestoreConsistentScnState::report_total_tablet_cnt_()
{
  int ret = OB_SUCCESS;
  storage::ObLSRestoreHandler *ls_restore_handler = ls_->get_ls_restore_handler();
  ObLSRestoreStat &restore_stat = ls_restore_handler->restore_stat();
  share::ObRestorePersistHelper helper;
  ObLSRestoreJobPersistKey ls_key;
  ls_key.tenant_id_ = ls_->get_tenant_id();
  ls_key.job_id_ = ls_restore_arg_->get_job_id();
  ls_key.ls_id_ = ls_->get_ls_id();
  ls_key.addr_ = self_addr_;
  if (OB_FAIL(helper.init(ls_key.tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key.tenant_id_);
  } else if (OB_FAIL(helper.set_ls_total_tablet_cnt(*proxy_, ls_key, total_tablet_cnt_))) {
    LOG_WARN("fail to set ls total tablet cnt", K(ret));
  } else {
    restore_stat.set_total_tablet_cnt(total_tablet_cnt_);
  }

  return ret;
}


//================================ObLSQuickRestoreState=======================================

ObLSQuickRestoreState::ObLSQuickRestoreState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::QUICK_RESTORE)
{
  has_rechecked_after_clog_recovered_ = false;
}

ObLSQuickRestoreState::~ObLSQuickRestoreState()
{
}

int ObLSQuickRestoreState::check_recover_finish(bool &is_finish) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_replay_to_target_scn_(ls_restore_arg_->get_restore_scn(), is_finish))) {
    LOG_WARN("failed to check clog replay to restore scn", K(ret));
  }

  return ret;
}

int ObLSQuickRestoreState::do_restore()
{
  DEBUG_SYNC(BEFORE_RESTORE_MINOR);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(update_role_())) {
    LOG_WARN("fail to update role and status", K(ret), KPC(this));
  } else if (!is_follower(role_) && OB_FAIL(leader_quick_restore_())) {
    LOG_WARN("fail to do leader quick restore", K(ret), KPC(this));
  } else if (is_follower(role_) && OB_FAIL(follower_quick_restore_())) {
    LOG_WARN("fail to do follower quick restore", K(ret), KPC(this));
  }
  return ret;
}

int ObLSQuickRestoreState::leader_quick_restore_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  logservice::ObLogRestoreHandler *log_restore_handle = ls_->get_log_restore_handler();
  bool can_do_restore = false;
  LOG_INFO("ready to leader quick restore", K(ls_restore_status_), KPC(ls_));
  if (OB_ISNULL(log_restore_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore handle can't nullptr", K(ret), K(log_restore_handle));
  } else if (OB_FAIL(tablet_mgr_.remove_restored_tablets(restored_tablets))) {
    LOG_WARN("fail to pop restored tablets", K(ret), KPC(ls_));
  } else if (OB_FAIL(inner_check_can_do_restore_(can_do_restore))) {
    LOG_WARN("failed to check can do restore", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (!can_do_restore) {
    // wait next schedule.
  } else if (OB_FAIL(tablet_mgr_.choose_tablets_to_restore(tablet_need_restore))) {
    LOG_WARN("fail to choose need restore tablets", K(ret), KPC(ls_));
  } else if (tablet_need_restore.empty()) {
    bool is_finish = false;
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE);
    if (OB_FAIL(check_clog_replay_finish_(is_finish))) {
      LOG_WARN("fail to check clog replay finish", K(ret), KPC(ls_));
    } else if (!is_finish) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_INFO("clog replay not finish, wait later", KPC(ls_));
      }
    } else if (OB_FAIL(report_finish_replay_clog_lsn_())) {
      LOG_WARN("fail to report finish replay clog lsn", K(ret));
    } else if (!tablet_mgr_.is_restore_completed()) {
    } else if (!has_rechecked_after_clog_recovered_) {
      // Force reload all tablets, ensure all transfer tablets has no transfer table.
      tablet_mgr_.set_force_reload();
      has_rechecked_after_clog_recovered_ = true;
    } else if (OB_FAIL(check_tablet_checkpoint_())) {
      LOG_WARN("fail to check tablet clog checkpoint ts", K(ret), KPC(ls_));
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
    } else {
      LOG_INFO("leader quick restore success", KPC(ls_));
    }
  } else if (OB_FAIL(do_quick_restore_(tablet_need_restore))) {
    LOG_WARN("fail to do quick restore", K(ret), K(tablet_need_restore), KPC(ls_));
  } else {
#ifdef ERRSIM
    if (!ls_->get_ls_id().is_sys_ls()) {
      DEBUG_SYNC(AFTER_SCHEDULE_RESTORE_MINOR_DAG_NET);
    }
#endif
  }

#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.

  int tmp_ret = OB_SUCCESS; // try rpc's best
  if (restored_tablets.empty()) {
  } else if (OB_SUCCESS != (tmp_ret = notify_follower_restore_tablet_(restored_tablets))) {
    LOG_WARN("fail to notify follower restore tablet", K(tmp_ret), KPC(ls_));
  } else {
    LOG_INFO("success send tablets to follower for restore", K(restored_tablets));
  }
#endif

  return ret;
}

int ObLSQuickRestoreState::follower_quick_restore_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  logservice::ObLogRestoreHandler *log_restore_handle = ls_->get_log_restore_handler();
  bool can_do_restore = false;
  LOG_INFO("ready to follower quick restore", K(ls_restore_status_), KPC(ls_));
  if (OB_ISNULL(log_restore_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore handle can't nullptr", K(ret), K(log_restore_handle));
  } else if (OB_FAIL(tablet_mgr_.remove_restored_tablets(restored_tablets))) {
    LOG_WARN("fail to pop restored tablets", K(ret), KPC(ls_));
  } else if (OB_FAIL(inner_check_can_do_restore_(can_do_restore))) {
    LOG_WARN("failed to check can do restore", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (!can_do_restore) {
    // wait next schedule.
  } else if (OB_FAIL(tablet_mgr_.choose_tablets_to_restore(tablet_need_restore))) {
    LOG_WARN("fail to choose need restore tablets", K(ret), KPC(ls_));
  } else if (tablet_need_restore.empty()) {
    bool is_finish = false;
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE);
    if (OB_FAIL(check_clog_replay_finish_(is_finish))) {
      LOG_WARN("fail to check clog replay finish", K(ret), KPC(ls_));
    } else if (!is_finish) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_INFO("clog replay not finish, wait later", KPC(ls_));
      }
    } else if (OB_FAIL(report_finish_replay_clog_lsn_())) {
      LOG_WARN("fail to report finish replay clog lsn", K(ret));
    } else if (!tablet_mgr_.is_restore_completed()) {
    } else if (!has_rechecked_after_clog_recovered_) {
      // Force reload all tablets, ensure all transfer tablets has no transfer table.
      tablet_mgr_.set_force_reload();
      has_rechecked_after_clog_recovered_ = true;
    } else if (OB_FAIL(check_tablet_checkpoint_())) {
      LOG_WARN("fail to check tablet clog checkpoint ts", K(ret), KPC(ls_));
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
    } else {
      LOG_INFO("follower quick restore success", KPC(ls_));
    }
  } else if (OB_FAIL(do_quick_restore_(tablet_need_restore))) {
    LOG_WARN("fail to do quick restore", K(ret), K(tablet_need_restore), KPC(ls_));
  }
  return ret;
}

int ObLSQuickRestoreState::do_quick_restore_(const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore)
{
  int ret = OB_SUCCESS;
  ObTaskId task_id;
  task_id.init(GCONF.self_addr_);
  ObTabletGroupRestoreArg arg;
  bool reach_dag_limit = false;
  bool is_new_election = false;
  // No matter is leader or follower, always restore data from backup.
  if (OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill leader ls restore arg", K(ret));
  }

#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.
  if (!is_follower(role_)
      || tablet_need_restore.action() == ObTabletRestoreAction::ACTION::RESTORE_TABLET_META) {
    if (OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
      LOG_WARN("fail to fill leader ls restore arg", K(ret));
    }
  } else {
    if (OB_FAIL(follower_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
      LOG_WARN("fail to fill follower ls restore arg", K(ret));
    }
  }
#endif

  if (FAILEDx(check_new_election_(is_new_election))) {
    LOG_WARN("fail to check change role", K(ret));
  } else if (is_new_election) {
    ret = OB_EAGAIN;
    LOG_WARN("new election, role may changed, retry later", K(ret), KPC(ls_));
  } else if (OB_FAIL(tablet_mgr_.schedule_tablet_group_restore(task_id, tablet_need_restore, reach_dag_limit))) {
    LOG_WARN("fail to schedule tablet", K(ret), K(tablet_need_restore), KPC(ls_));
  } else if (reach_dag_limit) {
    LOG_INFO("reach restore dag net max limit, wait later");
  } else if (OB_FAIL(schedule_tablet_group_restore_(arg, task_id))) {
    LOG_WARN("fail to schedule tablet group restore", KR(ret), K(arg), K(ls_restore_status_));
  } else {
    LOG_INFO("success schedule quick restore", K(ret), K(arg), K(ls_restore_status_));
  }
  return ret;
}

int ObLSQuickRestoreState::check_tablet_checkpoint_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ObTablet *tablet = nullptr;
  int64_t total_size = 0;

  if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_tablet_svr is nullptr", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->build_tablet_iter(iterator))) {
    LOG_WARN("fail to get tablet iterator", K(ret), KPC(ls_));
  } else {
    while (OB_SUCC(ret)) {
      int64_t tablet_size = 0;
      if (OB_FAIL(iterator.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next tablet", K(ret));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tablet is nullptr", K(ret));
      } else {
        const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
        bool can_restore = true;
        if (!tablet_meta.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet meta", K(ret), K(tablet_meta));
        } else if (tablet_meta.clog_checkpoint_scn_ > ls_restore_arg_->get_restore_scn()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet clog checkpoint ts should less than restore end ts", K(ret), K(tablet_meta),
              "ls restore end ts", ls_restore_arg_->get_restore_scn());
        } else if (tablet->is_empty_shell()) {
        } else if (!ls_restore_arg_->get_progress_display_mode().is_bytes()) {
        } else if (ls_->is_sys_ls() && tablet->get_tablet_meta().ha_status_.is_restore_status_full()) {
          // sys ls's tablets have all been fully restore, take occupy size as total bytes
          if (OB_FAIL(ObStorageHAUtils::get_tablet_occupy_size_in_bytes(
                  ls_->get_ls_id(), tablet->get_tablet_id(), tablet_size))) {
            LOG_WARN("fail to get tablet size", K(ret), KPC(tablet));
          }
        } else if (!ls_->is_sys_ls() && tablet->get_tablet_meta().ha_status_.is_restore_status_remote()) {
          if (OB_FAIL(ObStorageHAUtils::get_tablet_backup_size_in_bytes(
                  ls_->get_ls_id(), tablet->get_tablet_id(), tablet_size))) {
            LOG_WARN("fail to get tablet size", K(ret), KPC(tablet));
          }
        }
      }
      if (OB_SUCC(ret)) {
        total_size += tablet_size;
      }
    }
    // report ls total_bytes
    if (OB_SUCC(ret) && ls_restore_arg_->get_progress_display_mode().is_bytes()) {
      storage::ObLSRestoreHandler *ls_restore_handler = ls_->get_ls_restore_handler();
      ObLSRestoreStat &restore_stat = ls_restore_handler->restore_stat();
      share::ObRestorePersistHelper helper;
      ObLSRestoreJobPersistKey ls_key;
      ls_key.tenant_id_ = ls_->get_tenant_id();
      ls_key.job_id_ = ls_restore_arg_->get_job_id();
      ls_key.ls_id_ = ls_->get_ls_id();
      ls_key.addr_ = self_addr_;
      if (OB_FAIL(helper.init(ls_key.tenant_id_, share::OBCG_STORAGE))) {
        LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key.tenant_id_);
      } else if (OB_FAIL(helper.set_ls_total_bytes(*proxy_, ls_key, total_size))) {
        LOG_WARN("fail to set ls total bytes", K(ret), K(ls_key), K(total_size));
      } else if (ls_key.ls_id_.is_sys_ls() && OB_FAIL(helper.set_ls_finish_bytes(*proxy_, ls_key, total_size))) {
        LOG_WARN("fail to increase ls total bytes", K(ret), K(ls_key), K(total_size));
      } else {
        restore_stat.set_total_bytes(total_size);
      }
    }
  }
  return ret;
}

//================================ObLSQuickRestoreFinishState=======================================

ObLSQuickRestoreFinishState::ObLSQuickRestoreFinishState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH)
{
}

ObLSQuickRestoreFinishState::~ObLSQuickRestoreFinishState()
{
}

int ObLSQuickRestoreFinishState::do_restore()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(update_role_())) {
    LOG_WARN("fail to update role and status", K(ret), KPC(this));
  } else if (!is_follower(role_) && OB_FAIL(leader_quick_restore_finish_())) {
    LOG_WARN("fail to do leader quick restore finish", K(ret), KPC(this));
  } else if (is_follower(role_) && OB_FAIL(follower_quick_restore_finish_())) {
    LOG_WARN("fail to do follower quick restore finish", K(ret), KPC(this));
  }
  return ret;
}

int ObLSQuickRestoreFinishState::leader_quick_restore_finish_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("leader quick restore finish", K(ls_restore_status_), KPC(ls_));
  if (ls_restore_arg_->get_restore_type().is_quick_restore()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("quick restore is not supported now", K(ret), KPC(ls_));
  } else if (ls_restore_arg_->get_restore_type().is_full_restore()) {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA);
    if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), K(next_status), KPC(ls_));
    } else {
      LOG_INFO("succ to advance leader restore status to restore major from quick restore finish", K(ret));
    }
  }
  return ret;
}


int ObLSQuickRestoreFinishState::follower_quick_restore_finish_()
{
	int ret = OB_SUCCESS;
  LOG_INFO("follower quick restore finish", K(ls_restore_status_), KPC(ls_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ls_restore_arg_->get_restore_type().is_quick_restore()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("quick restore is not supported now", K(ret), KPC(ls_));
  } else {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA);
    if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance statsu", K(ret), KPC(ls_), K(next_status));
    }
  }
  return ret;
}

//================================ObLSRestoreMajorState=======================================

ObLSRestoreMajorState::ObLSRestoreMajorState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA)
{
}

ObLSRestoreMajorState::~ObLSRestoreMajorState()
{
}

ERRSIM_POINT_DEF(EN_REBUILD_BEFORE_RESTORE_MAJOR)
int ObLSRestoreMajorState::do_restore()
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  if (OB_SUCCESS != EN_REBUILD_BEFORE_RESTORE_MAJOR && !ls_->is_sys_ls() && is_follower(role_)) {
    // trigger follower rebuild
    ObRebuildService *rebuild_service = MTL(ObRebuildService *);
    const ObLSRebuildType rebuild_type(ObLSRebuildType::TRANSFER);
    if (OB_FAIL(rebuild_service->add_rebuild_ls(ls_->get_ls_id(), rebuild_type))) {
      LOG_WARN("[ERRSIM] failed to add rebuild ls", K(ret), K(ls_->get_ls_id()), K(rebuild_type));
    } else {
      LOG_INFO("fake EN_REBUILD_BEFORE_RESTORE_MAJOR", K(ls_->get_ls_id()), K(rebuild_type));
    }
  }
#endif

  if (!ls_->is_sys_ls()) {
    DEBUG_SYNC(BEFORE_RESTORE_MAJOR);
  }

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
#ifdef ERRSIM
  } else if (OB_FAIL(errsim_rebuild_before_restore_major_())) {
    LOG_WARN("[ERRSIM] fail to errsim rebuild before restore major", K(ret), KPC(this));
#endif
  } else if (OB_FAIL(update_role_())) {
    LOG_WARN("fail to update role and status", K(ret), KPC(this));
  } else if (!is_follower(role_) && OB_FAIL(leader_restore_major_data_())) {
    LOG_WARN("fail to do leader restore sys tablet", K(ret), KPC(this));
  } else if (is_follower(role_) && OB_FAIL(follower_restore_major_data_())) {
    LOG_WARN("fail to do follower restore sys tablet", K(ret), KPC(this));
  }
  return ret;
}

int ObLSRestoreMajorState::leader_restore_major_data_()
{
  int ret = OB_SUCCESS;
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  bool can_do_restore = false;
  LOG_INFO("ready to restore leader major data", K(ls_restore_status_), KPC(ls_));
  if (OB_FAIL(tablet_mgr_.remove_restored_tablets(restored_tablets))) {
    LOG_WARN("fail to pop restored tablets", K(ret), KPC(ls_));
  } else if (OB_FAIL(inner_check_can_do_restore_(can_do_restore))) {
    LOG_WARN("failed to check can do restore", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (!can_do_restore) {
    // wait next schedule.
  } else if (OB_FAIL(tablet_mgr_.choose_tablets_to_restore(tablet_need_restore))) {
    LOG_WARN("fail to choose need restore tablets", K(ret), KPC(ls_));
  } else if (tablet_need_restore.empty()) {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA);
    if (!tablet_mgr_.is_restore_completed()) {
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status to WAIT_RESTORE_MAJOR_DATA from RESTORE_MAJOR_DATA", K(ret), KPC(ls_), K(next_status));
    } else {
      LOG_INFO("leader restore major data finish", KPC(ls_));
    }
  } else if (OB_FAIL(do_restore_major_(tablet_need_restore))) {
    LOG_WARN("fail to do restore major", K(ret), K(tablet_need_restore), KPC(ls_));
  }

#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.

  int tmp_ret = OB_SUCCESS; // try rpc's best
  if (restored_tablets.empty()) {
  } else if (OB_SUCCESS != (tmp_ret = notify_follower_restore_tablet_(restored_tablets))) {
    LOG_WARN("fail to notify follower restore tablet", K(tmp_ret), KPC(ls_));
  } else {
    LOG_INFO("success send tablets to follower for restore", K(restored_tablets));
  }
#endif

  return ret;
}

int ObLSRestoreMajorState::follower_restore_major_data_()
{
  int ret = OB_SUCCESS;
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  bool can_do_restore = false;
  LOG_INFO("ready to restore follower major data", K(ls_restore_status_), KPC(ls_));
  if (OB_FAIL(tablet_mgr_.remove_restored_tablets(restored_tablets))) {
    LOG_WARN("fail to pop restored tablets", K(ret), KPC(ls_));
  } else if (OB_FAIL(inner_check_can_do_restore_(can_do_restore))) {
    LOG_WARN("failed to check can do restore", K(ret), "ls_id", ls_->get_ls_id().id());
  } else if (!can_do_restore) {
    // wait next schedule.
  } else if (OB_FAIL(tablet_mgr_.choose_tablets_to_restore(tablet_need_restore))) {
    LOG_WARN("fail to choose need restore tablets", K(ret), KPC(ls_));
  } else if (tablet_need_restore.empty()) {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA);
    if (!tablet_mgr_.is_restore_completed()) {
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), K(next_status), KPC(ls_));
    } else {
      LOG_INFO("follower restore major data finish", KPC(ls_));
    }
  } else if (OB_FAIL(do_restore_major_(tablet_need_restore))) {
    LOG_WARN("fail to do restore major", K(ret), K(tablet_need_restore), KPC(ls_));
  }
  return ret;
}

int ObLSRestoreMajorState::do_restore_major_(
    const ObLSRestoreTaskMgr::ToRestoreTabletGroup &tablet_need_restore)
{
  int ret = OB_SUCCESS;
  ObTaskId task_id;
  task_id.init(GCONF.self_addr_);
  ObTabletGroupRestoreArg arg;
  bool reach_dag_limit = false;
  bool is_new_election = false;
  const bool is_shared_storage_mode = GCTX.is_shared_storage_mode();

  if (!is_shared_storage_mode) {
    // No matter is leader or follower, always restore data from backup.
    if (OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
      LOG_WARN("fail to fill leader ls restore arg", K(ret));
    }
  } else {
    if (!is_follower(role_) && OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
      LOG_WARN("fail to fill ls restore arg", K(ret));
    } else if (is_follower(role_) && OB_FAIL(follower_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
      LOG_WARN("fail to fill ls restore arg", K(ret));
    }
  }

  if (FAILEDx(check_new_election_(is_new_election))) {
    LOG_WARN("fail to check change role", K(ret));
  } else if (is_new_election) {
    ret = OB_EAGAIN;
    LOG_WARN("new election, role may changed, retry later", K(ret), KPC(ls_));
  } else if (OB_FAIL(tablet_mgr_.schedule_tablet_group_restore(task_id, tablet_need_restore, reach_dag_limit))) {
    LOG_WARN("fail to schedule tablet", K(ret), K(tablet_need_restore), KPC(ls_));
  } else if (reach_dag_limit) {
    LOG_INFO("reach restore dag net max limit, wait later");
  } else if (OB_FAIL(schedule_tablet_group_restore_(arg, task_id))) {
    LOG_WARN("fail to schedule schedule tablet group restore", KR(ret), K(arg), K(ls_restore_status_));
  } else {
    LOG_INFO("success schedule restore major", K(ret), K(arg), K(ls_restore_status_));
  }
  return ret;
}

#ifdef ERRSIM
int ObLSRestoreMajorState::errsim_rebuild_before_restore_major_()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != EN_REBUILD_BEFORE_RESTORE_MAJOR && !ls_->is_sys_ls() && is_follower(role_)) {
    // trigger follower rebuild
    ObRebuildService *rebuild_service = MTL(ObRebuildService *);
    const ObLSRebuildType rebuild_type(ObLSRebuildType::TRANSFER);
    if (OB_FAIL(rebuild_service->add_rebuild_ls(ls_->get_ls_id(), rebuild_type))) {
      LOG_WARN("[ERRSIM] failed to add rebuild ls", K(ret), K(ls_->get_ls_id()), K(rebuild_type));
    } else {
      LOG_INFO("fake EN_REBUILD_BEFORE_RESTORE_MAJOR", K(ls_->get_ls_id()), K(rebuild_type));
    }
  }
  return ret;
}
#endif

//================================ObLSRestoreFinishState=======================================
ObLSRestoreFinishState::ObLSRestoreFinishState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::NONE)
{
}

ObLSRestoreFinishState::~ObLSRestoreFinishState()
{
}

int ObLSRestoreFinishState::do_restore()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_finish_())) {
    LOG_WARN("fail to do restore finish", K(ret), KPC(this));
  }
  return ret;
}

int ObLSRestoreFinishState::restore_finish_()
{
  int ret = OB_SUCCESS;
  LOG_INFO("leader restore finish");
  return ret;
}

//================================ObLSRestoreWaitState=======================================

ObLSRestoreWaitState::ObLSRestoreWaitState(const share::ObLSRestoreStatus::Status &status, const bool require_multi_replica_sync)
  : ObILSRestoreState(status), has_confirmed_(false), require_multi_replica_sync_(require_multi_replica_sync)
{
}

ObLSRestoreWaitState::~ObLSRestoreWaitState()
{
}

int ObLSRestoreWaitState::do_restore()
{
  int ret = OB_SUCCESS;
  bool all_finished = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(update_role_())) {
    LOG_WARN("fail to update role and status", K(ret), KPC(this));
  } else if (has_confirmed_) {
  } else if (OB_FAIL(check_all_tablets_has_finished_(all_finished))) {
    LOG_WARN("fail to check all tablets finished", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
  } else if (!all_finished) {
    // fatal error
    ret = OB_ERR_SYS;
    LOG_ERROR("not all tablets finished", K(ret), KPC(this));
  } else if (OB_FAIL(report_restore_stat_())) {
    LOG_WARN("fail to report restore stat", K(ret), KPC(this));
  } else if (!is_follower(role_) && OB_FAIL(leader_wait_follower_())) {
    LOG_WARN("fail to do leader restore sys tablet", K(ret), KPC(this));
  } else if(is_follower(role_) && OB_FAIL(follower_wait_leader_())) {
    LOG_WARN("fail to do follower restore sys tablet", K(ret), KPC(this));
  }

  return ret;
}

int ObLSRestoreWaitState::check_can_advance_status_(bool &can) const
{
  int ret = OB_SUCCESS;
  can = true;
  return ret;
}

int ObLSRestoreWaitState::report_restore_stat_()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObLSRestoreWaitState::check_all_tablets_has_finished_(bool &all_finished)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> unfinished_high_pri_tablets;
  ObArray<ObTabletID> unfinished_tablets;
  if (ls_restore_status_.is_wait_restore_sys_tablets()) {
    all_finished = true;
  } else if (ls_restore_status_.is_wait_restore_consistent_scn()) {
    all_finished = true;
  } else if (OB_FAIL(tablet_mgr_.reload_get_unfinished_tablets(unfinished_high_pri_tablets, unfinished_tablets))) {
    LOG_WARN("fail to get unfinished tablets", K(ret), KPC(this));
  } else if (!unfinished_high_pri_tablets.empty() || !unfinished_tablets.empty()) {
    all_finished = false;
    LOG_INFO("still have tablets not restored", K(ret), KPC(this), K(unfinished_high_pri_tablets), K(unfinished_tablets));
  } else {
    all_finished = true;
  }

  has_confirmed_ = true;

  return ret;
}

int ObLSRestoreWaitState::leader_wait_follower_()
{
  int ret = OB_SUCCESS;
  bool all_finish = false;
  bool can_advance = false;
  ObLSRestoreStatus next_status;
  if (ls_restore_status_.is_wait_restore_sys_tablets()) {
    DEBUG_SYNC(BEFORE_WAIT_RESTORE_SYS_TABLETS);
    next_status = ObLSRestoreStatus::Status::RESTORE_TABLETS_META;
  } else if (ls_restore_status_.is_wait_restore_tablets_meta()) {
    DEBUG_SYNC(BEFORE_WAIT_RESTORE_TABLETS_META);
    next_status = ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN;
  } else if (ls_restore_status_.is_wait_restore_consistent_scn()) {
    DEBUG_SYNC(BEFORE_WAIT_LS_RESTORE_TO_CONSISTENT_SCN);
    next_status = ObLSRestoreStatus::Status::QUICK_RESTORE;
  } else if (ls_restore_status_.is_wait_quick_restore()) {
    DEBUG_SYNC(BEFORE_WAIT_QUICK_RESTORE);
    if (ls_restore_arg_->restore_type_.is_quick_restore()) {
      // quick restore
      if (ls_->get_ls_id().is_sys_ls()) {
        next_status = ObLSRestoreStatus::Status::NONE;
      } else {
        next_status = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
      }
    } else {
      // full restore
      next_status = ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA;
    }
  } else if (ls_restore_status_.is_wait_restore_major_data()) {
    DEBUG_SYNC(BEFORE_WAIT_MAJOR_RESTORE);
    next_status = ObLSRestoreStatus::Status::NONE;
  }
  LOG_INFO("leader is wait follower", "leader current status", ls_restore_status_, "next status", next_status, KPC(ls_));
  if (require_multi_replica_sync_ && OB_FAIL(check_all_follower_restore_finish_(all_finish))) {
    LOG_WARN("fail to request follower restore meta result", K(ret), KPC(ls_));
  } else if (require_multi_replica_sync_ && !all_finish) {
  } else if (OB_FAIL(check_can_advance_status_(can_advance))) {
    LOG_WARN("fail to check can advance status", K(ret), KPC(ls_));
  } else if (!can_advance) {
    // do nothing
  } else if ((next_status.is_quick_restore() || next_status.is_restore_to_consistent_scn()) && OB_FAIL(report_start_replay_clog_lsn_())) {
    LOG_WARN("fail to report start replay clog lsn", K(ret));
  } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
    LOG_WARN("fail to advance status", K(ret), K(next_status), KPC(ls_));
  }
  return ret;
}

int ObLSRestoreWaitState::follower_wait_leader_()
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus next_status(ls_restore_status_);
  if (ls_restore_status_.is_wait_restore_sys_tablets()) {
    next_status = ObLSRestoreStatus::Status::RESTORE_TABLETS_META;
  } else if (ls_restore_status_.is_wait_restore_tablets_meta()) {
    next_status = ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN;
  } else if (ls_restore_status_.is_wait_restore_consistent_scn()) {
    next_status = ObLSRestoreStatus::Status::QUICK_RESTORE;
  } else if (ls_restore_status_.is_wait_quick_restore()) {
    if (ls_restore_arg_->restore_type_.is_quick_restore()) {
      // quick restore
      if (ls_->get_ls_id().is_sys_ls()) {
        next_status = ObLSRestoreStatus::Status::NONE;
      } else {
        next_status = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
      }
    } else {
      // full restore
      next_status = ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA;
    }
  } else if (ls_restore_status_.is_wait_restore_major_data()) {
    next_status = ObLSRestoreStatus::Status::NONE;
  }

  LOG_INFO("follower is wait leader", "follower current status", ls_restore_status_, "next status", next_status, KPC(ls_));
  ObLSRestoreStatus leader_restore_status(ObLSRestoreStatus::Status::LS_RESTORE_STATUS_MAX);
  if (require_multi_replica_sync_ && OB_FAIL(request_leader_status_(leader_restore_status))) {
    LOG_WARN("fail to request leader tablets and status", K(ret), KPC(ls_));
  } else if (!require_multi_replica_sync_ || check_leader_restore_finish_(leader_restore_status, ls_restore_status_)) {
    bool can_advance = false;
    if (OB_FAIL(check_can_advance_status_(can_advance))) {
      LOG_WARN("fail to check can advance status", K(ret), KPC(ls_));
    } else if (!can_advance) {
      // do nothing
    } else if ((next_status.is_quick_restore() || next_status.is_restore_to_consistent_scn()) && OB_FAIL(report_start_replay_clog_lsn_())) {
      LOG_WARN("fail to report start replay clog lsn", K(ret));
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
      LOG_WARN("fail to advance status", K(ret), KPC(ls_), K(next_status));
    } else {
      LOG_INFO("follower success advance status", K(next_status), K(leader_restore_status), KPC(ls_));
    }
  }
  return ret;
}


//================================ObLSWaitRestoreConsistentScnState=======================================
int ObLSWaitRestoreConsistentScnState::check_can_advance_status_(bool &can) const
{
  int ret = OB_SUCCESS;
  share::ObPhysicalRestoreTableOperator restore_table_operator;
  const uint64_t tenant_id = ls_->get_tenant_id();
  if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table operator", K(ret), K(tenant_id));
  } else {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::QUICK_RESTORE);
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job_info))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else if (ls_restore_arg_->get_progress_display_mode().is_bytes()) {
        can = share::PhysicalRestoreStatus::PHYSICAL_RESTORE_WAIT_QUICK_RESTORE_FINISH == job_info.get_status();
      } else {
        can = share::PhysicalRestoreStatus::PHYSICAL_RESTORE_WAIT_LS == job_info.get_status();
      }
    }
  }
  return ret;
}

//================================ObLSRestoreWaitQuickRestoreState=======================================
int ObLSRestoreWaitQuickRestoreState::check_can_advance_status_(bool &can) const
{
  int ret = OB_SUCCESS;
  if (ls_restore_arg_->get_progress_display_mode().is_tablet_cnt()) {
    can = true;
  } else {
    share::ObPhysicalRestoreTableOperator restore_table_operator;
    const uint64_t tenant_id = ls_->get_tenant_id();
    if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id, share::OBCG_STORAGE))) {
      LOG_WARN("fail to init restore table operator", K(ret), K(tenant_id));
    } else {
      HEAP_VAR(ObPhysicalRestoreJob, job_info) {
        if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job_info))) {
          LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
        } else if (share::PhysicalRestoreStatus::PHYSICAL_RESTORE_WAIT_LS != job_info.get_status()) {
          can = false;
        } else {
          can = true;
        }
      }
    }
  }
  return ret;
}

//================================ObLSRestoreWaitRestoreMajorDataState=======================================
int ObLSRestoreWaitRestoreMajorDataState::report_restore_stat_()
{
  int ret = OB_SUCCESS;

  if (!has_reported_) {
    share::ObRestorePersistHelper helper;
    ObLSRestoreJobPersistKey key;
    key.job_id_ = ls_restore_arg_->job_id_;
    key.tenant_id_ = ls_restore_arg_->tenant_id_;
    key.ls_id_ = ls_->get_ls_id();
    key.addr_ = self_addr_;
    if (OB_FAIL(helper.init(key.tenant_id_, share::OBCG_STORAGE))) {
      LOG_WARN("fail to init helper", K(ret), K(key.tenant_id_));
    } else if (OB_FAIL(helper.force_correct_restore_stat(*proxy_, key))) {
      LOG_WARN("fail to force correct restore stat", K(ret), K(key));
    } else {
      has_reported_ = true;
      LOG_INFO("force correct restore stat", K(key));
    }
  }

  return ret;
}
