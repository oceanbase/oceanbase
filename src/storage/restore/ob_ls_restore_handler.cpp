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
#include "ob_ls_restore_handler.h"
#include "logservice/ob_log_service.h"
#include "storage/high_availability/ob_ls_restore.h"
#include "storage/high_availability/ob_storage_ha_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_rebuild_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/high_availability/ob_ss_ls_restore_state.h"
#endif
#include "share/backup/ob_backup_connectivity.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "src/storage/restore/ob_tenant_restore_info_mgr.h"

using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace backup;
using namespace logservice;

ObLSRestoreHandler::ObLSRestoreHandler()
  : is_inited_(false),
    is_stop_(false),
    is_online_(false),
    rebuild_seq_(0),
    mtx_(common::ObLatchIds::OB_LS_RESTORE_HANDLER_LOCK),
    result_mgr_(),
    ls_(nullptr),
    ls_restore_arg_(),
    state_handler_(nullptr),
    allocator_(),
    restore_stat_(),
    trace_id_(),
    succeed_set_dest_info_(false),
    restore_job_id_(-1)
{
}

ObLSRestoreHandler::~ObLSRestoreHandler()
{
  destroy();
}

int ObLSRestoreHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  const char *OB_LS_RESTORE_HANDLER = "lsRestoreHandler";
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("nullptr ls", K(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                     ObMemAttr(common::OB_SERVER_TENANT_ID, OB_LS_RESTORE_HANDLER)))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else {
    ls_ = ls;
    rebuild_seq_ = ls->get_rebuild_seq();
    trace_id_.init(GCONF.self_addr_);
    is_inited_ = true;
  }
  return ret;
}

void ObLSRestoreHandler::destroy()
{
  if (nullptr != state_handler_) {
    state_handler_->destroy();
    state_handler_->~ObILSRestoreState();
    allocator_.free(state_handler_);
    state_handler_ = nullptr;
  }
  ls_ = nullptr;
  trace_id_.reset();
  succeed_set_dest_info_ = false;
  restore_job_id_ = -1;
}

int ObLSRestoreHandler::offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_online()) {
    LOG_INFO("ls restore handler is already offline");
  } else {
    int retry_cnt = 0;
    do {
      // if lock failed, retry 3 times.
      if (OB_FAIL(mtx_.trylock())) {
        LOG_WARN("lock restore handler failed, retry later", K(ret), KPC(ls_));
        sleep(1);
      } else {
        if (OB_FAIL(cancel_task_())) {
          LOG_WARN("failed to cancel task", K(ret), KPC(ls_));
        } else {
          set_is_online(false);
          LOG_INFO("ls restore handler offline finish");
        }
        mtx_.unlock();
      }
    } while (retry_cnt ++ < 3/*max retry cnt*/ && OB_EAGAIN == ret);
  }
  return ret;
}

int ObLSRestoreHandler::online()
{
  int ret = OB_SUCCESS;
  share::ObLSRestoreStatus new_status;
  ObILSRestoreState *new_state_handler = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_online()) {
    // do nothing
    LOG_INFO("ls restore handler is already online");
  } else if (OB_FAIL(ls_->get_restore_status(new_status))) {
    LOG_WARN("fail to get_restore_status", K(ret), KPC(ls_));
  } else if (!new_status.is_in_restoring_or_failed()) {
    set_is_online(true);
  } else {
    lib::ObMutexGuard guard(mtx_);
    if (nullptr != state_handler_) {
      // when online, the old task should be cancel.
      if (OB_FAIL(state_handler_->get_tablet_mgr().cancel_task())) {
        LOG_WARN("failed to cancel task", K(ret));
      } else {
        state_handler_->~ObILSRestoreState();
        allocator_.free(state_handler_);
        state_handler_ = nullptr;
      }
    }
    if (OB_SUCC(ret)) {
       set_is_online(true);
      LOG_INFO("ls restore handler online finish");
    }
  }
  return ret;
}

int ObLSRestoreHandler::record_clog_failed_info(
    const share::ObTaskId &trace_id, const share::ObLSID &ls_id, const int &result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (trace_id.is_invalid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(trace_id), K(ls_id));
  } else if (OB_SUCCESS == result) {
  } else {
    result_mgr_.set_result(result, trace_id, ObLSRestoreResultMgr::RestoreFailedType::CLOG_RESTORE_FAILED_TYPE);
    LOG_WARN("fail to restore clog", K(ret), K(ls_id), K(result), K(trace_id));
  }
  return ret;
}

void ObLSRestoreHandler::try_record_one_tablet_to_restore(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(state_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("state_handler_ is null", K(ret));
  } else if (OB_FAIL(state_handler_->get_tablet_mgr().record_one_tablet_to_restore(tablet_id))) {
    LOG_WARN("fail to record one tablet to restore", K(ret), KPC_(ls), K(tablet_id));
  } else {
    LOG_INFO("succeed record one tablet to restore", KPC_(ls), K(tablet_id));
  }
}

int ObLSRestoreHandler::get_consistent_scn(share::SCN &consistent_scn)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    consistent_scn = ls_restore_arg_.get_consistent_scn();
  }

  return ret;
}

int ObLSRestoreHandler::handle_execute_over(
    const share::ObTaskId &task_id,
    const ObIArray<common::ObTabletID> &restore_succeed_tablets,
    const ObIArray<common::ObTabletID> &restore_failed_tablets,
    const share::ObLSID &ls_id,
    const int &result)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (task_id.is_invalid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(task_id), K(ls_id));
  } else if (OB_SUCCESS == result) {
    wakeup();
    LOG_INFO("succeed restore dag net task", K(result), K(task_id), K(ls_id), K(restore_succeed_tablets));
  } else if (OB_CANCELED == result) {
    //do nothing
    LOG_WARN("task has been canceled", KPC(ls_), K(task_id));
  } else if (result != OB_SUCCESS) {
    share::ObLSRestoreStatus status;
    common::ObRole role;
    lib::ObMutexGuard guard(mtx_);
    if (nullptr != state_handler_) {
      status = state_handler_->get_restore_status();
      role = state_handler_->get_role();
    }

  #ifdef ERRSIM
    SERVER_EVENT_ADD("storage_ha", "handle_execute_over_errsim", "result", result);
  #endif

    if (status.is_restore_sys_tablets()) {
      state_handler_->set_retry_flag();
      result_mgr_.set_result(result, task_id, ObLSRestoreResultMgr::RestoreFailedType::DATA_RESTORE_FAILED_TYPE);
      LOG_WARN("restore sys tablets dag failed, need retry", K(ret));
    } else if (OB_TABLET_NOT_EXIST == result) {
      LOG_INFO("tablet has been deleted, no need to record err info", K(restore_failed_tablets));
    } else if (common::ObRole::FOLLOWER == role && result_mgr_.can_retrieable_err(result)) {
      LOG_INFO("follower met retrieable err, no need to record", K(result), K(task_id));
    } else {
      result_mgr_.set_result(result, task_id, ObLSRestoreResultMgr::RestoreFailedType::DATA_RESTORE_FAILED_TYPE);
      LOG_WARN("failed restore dag net task", K(result), K(task_id), K(ls_id), K(restore_succeed_tablets), K(restore_failed_tablets), KPC_(ls));
    }
  }
  return ret;
}

int ObLSRestoreHandler::handle_pull_tablet(
    const ObIArray<common::ObTabletID> &tablet_ids,
    const share::ObLSRestoreStatus &leader_restore_status,
    const int64_t leader_proposal_id)
{
  int ret = OB_SUCCESS;
  bool all_finish = false;
  LOG_INFO("succeed receive handle pull tablet from leader", K(ret));
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(state_handler_)) {
    ret = OB_ERR_SELF_IS_NULL;
    LOG_WARN("need restart, wait later", KPC(ls_), K(is_stop()), K(is_online()));
  } else if (OB_FAIL(state_handler_->handle_pull_tablet(tablet_ids, leader_restore_status, leader_proposal_id))) {
    LOG_WARN("fail to handl pull tablet", K(ret), K(leader_restore_status), K(leader_proposal_id));
  }
  return ret;
}

int ObLSRestoreHandler::process()
{
  int ret = OB_SUCCESS;
  ObCurTraceId::set(trace_id_);
  bool can_do_restore;
  ObCurTraceId::init(GCONF.self_addr_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_stop() || !is_online()) {
      LOG_INFO("ls stopped or disabled", KPC(ls_));
  } else if (OB_FAIL(check_before_do_restore_(can_do_restore))) {
    LOG_WARN("fail to check before do restore", K(ret), KPC(ls_));
  } else if (OB_FAIL(refresh_restore_info_())) {
    LOG_WARN("fail to refresh restore info", K(ret), KPC(ls_));
  } else if (!can_do_restore) {
  } else if (OB_FAIL(update_state_handle_())) {
    LOG_WARN("fail to update state handle", K(ret));
  } else if (!result_mgr_.can_retry()) {
    if (OB_FAIL(deal_failed_restore_())) {
      LOG_WARN("fail to deal failed restore", K(ret), KPC(ls_));
    }
  } else if (result_mgr_.is_met_retry_time_interval()) {
    // Some retrieable errors may be frequent in a short time，such as :
    // it tasks a period of time for the ls leader is ready after the shutdown and restart of observer usually,
    // and an ls leader not exist error will be returned before leader is ready.
    // so in order to improve availability, we need control the retry frequency and the default retry time interval is 10s.
    lib::ObMutexGuard guard(mtx_);
    if (is_stop() || !is_online()) {
      LOG_INFO("ls stopped or disabled", KPC(ls_));
  #ifdef ERRSIM
    } else if (ls_->get_ls_id().id() == GCONF.errsim_restore_ls_id
               && state_handler_->get_restore_status() == GCONF.errsim_ls_restore_status) {
      ret = OB_EAGAIN;
  #endif
    } else if (OB_FAIL(state_handler_->do_restore())) {
      result_mgr_.set_result(ret, trace_id_, ObLSRestoreResultMgr::RestoreFailedType::DATA_RESTORE_FAILED_TYPE);
      LOG_WARN("fail to do restore", K(ret), KPC(state_handler_));
    }
  }
  return ret;
}

int ObLSRestoreHandler::check_before_do_restore_(bool &can_do_restore)
{
  int ret = OB_SUCCESS;
  share::ObLSRestoreStatus restore_status;
  ObMigrationStatus migration_status;
  can_do_restore = false;
  bool is_normal = false;
  bool is_exist = true;
  bool is_in_member_or_learner_list = false;
  if (is_stop() || !is_online()) {
      LOG_INFO("ls stopped or disabled", KPC(ls_));
  } else if (OB_FAIL(check_meta_tenant_normal_(is_normal))) {
    LOG_WARN("fail to get meta tenant status", K(ret));
  } else if (!is_normal) {
  } else if (OB_FAIL(ls_->get_restore_status(restore_status))) {
    LOG_WARN("fail to get_restore_status", K(ret), KPC(ls_));
  } else if (!restore_status.is_in_restoring_or_failed()) {
    lib::ObMutexGuard guard(mtx_);
    if (OB_NOT_NULL(state_handler_)) {
      state_handler_->~ObILSRestoreState();
      allocator_.free(state_handler_);
      state_handler_ = nullptr;
    }
  } else if (restore_status.is_failed()) {
  } else if (OB_FAIL(check_restore_job_exist_(is_exist))) {
  } else if (!is_exist) {
    if (OB_FAIL(ls_->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_FAILED), get_rebuild_seq()))) {
      LOG_WARN("failed to set restore status", K(ret), KPC(ls_));
      if (OB_STATE_NOT_MATCH == ret && OB_FAIL(update_rebuild_seq())) {
        LOG_WARN("failed to update rebuild seq", K(ret));
      }
    }
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    can_do_restore = true;
#endif
  } else if (OB_FAIL(ls_->get_migration_status(migration_status))) {
    LOG_WARN("fail to get migration status", K(ret));
  } else if (!ObMigrationStatusHelper::check_can_restore(migration_status)) {
  } else if (OB_FAIL(check_in_member_or_learner_list_(is_in_member_or_learner_list))) {
    LOG_WARN("failed to check in member or learner list", K(ret));
  } else if (!is_in_member_or_learner_list) {
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      LOG_INFO("ls is not in member or learner list", KPC(ls_));
    }
  } else {
    can_do_restore = true;
  }
  return ret;
}

int ObLSRestoreHandler::check_in_member_or_learner_list_(bool &is_in_member_or_learner_list) const
{
  int ret = OB_SUCCESS;
  int64_t paxos_replica_num = 0;
  common::ObMemberList member_list;
  GlobalLearnerList learner_list;
  ObAddr self_addr = GCTX.self_addr();
  is_in_member_or_learner_list = false;
  if (OB_FAIL(ls_->get_log_handler()->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, learner_list, true/*filter_logonly_replica*/))) {
    LOG_WARN("failed to get paxos_member_list_and_learner_list", K(ret));
  } else {
    is_in_member_or_learner_list = member_list.contains(self_addr) || learner_list.contains(self_addr);
  }
  return ret;
}

int ObLSRestoreHandler::check_restore_job_exist_(bool &is_exist)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy_ = nullptr;
  share::ObPhysicalRestoreTableOperator restore_table_operator;
  const uint64_t tenant_id = ls_->get_tenant_id();
  ObPhysicalRestoreJob job_info;
  is_exist = true;
  if (OB_ISNULL(sql_proxy_ = GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql prxoy must not be null", K(ret));
  } else if (OB_FAIL(restore_table_operator.init(sql_proxy_, tenant_id, share::OBCG_STORAGE))) {
    LOG_WARN("failed to init restore table operator", K(ret), K(tenant_id));
  } else if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job_info))) {
    if (ret == OB_ENTRY_NOT_EXIST) {
      is_exist = false;
      ret = OB_SUCCESS;
      LOG_INFO("restore job is not exist", K(tenant_id), K(is_exist));
    } else {
      LOG_WARN("failed to get job by tenant", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObLSRestoreHandler::check_meta_tenant_normal_(bool &is_normal)
{
  int ret = OB_SUCCESS;
  is_normal = false;
  uint64_t meta_tenant_id = gen_meta_tenant_id(MTL_ID());
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = nullptr;
  share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service must not be null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(meta_tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(meta_tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(meta_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant info", K(ret), K(meta_tenant_id));
  } else if (OB_NOT_NULL(tenant_schema) && tenant_schema->is_normal()) {
    is_normal = true;
  }
  return ret;
}

int ObLSRestoreHandler::refresh_restore_info_()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  const uint64_t tenant_id = ls_->get_tenant_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSRestoreHandler is not init", K(ret));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't be null", K(ret), KP(sql_proxy));
  } else if (MTL_TENANT_ROLE_CACHE_IS_INVALID()) {
    // wait tenant role refresh
  } else if (MTL_TENANT_ROLE_CACHE_IS_CLONE()) {
    // do nothing for clone tenant
  } else if (MTL_TENANT_ROLE_CACHE_IS_RESTORE()) {
    // tenant in restore, get backup set list from table __all_restore_job
    share::ObPhysicalRestoreTableOperator restore_table_operator;
    const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
    int64_t dest_id = 0;
    common::ObArray<share::ObBackupSetBriefInfo> backup_set_list;
    ObTenantBackupDestInfoMgr *mgr = MTL(ObTenantBackupDestInfoMgr *);
    HEAP_VAR(ObPhysicalRestoreJob, job) {
      if (succeed_set_dest_info_) {
        // do nothing for succeed set dest info
      } else if (OB_ISNULL(mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mgr is null", K(ret), K(tenant_id));
      } else if (OB_FAIL(restore_table_operator.init(sql_proxy, tenant_id, share::OBCG_STORAGE))) {
        LOG_WARN("fail to init restore table operator", K(ret));
      } else if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(tenant_id, job))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_restore_dest_id(
            *sql_proxy, tenant_id, backup_dest_type, dest_id))) {
        LOG_WARN("failed to get restore dest id", K(ret), K(tenant_id), K(backup_dest_type));
      } else if (OB_FAIL(job.get_multi_restore_path_list().get_backup_set_brief_info_list(backup_set_list))) {
        LOG_WARN("fail to get backup set brief info list", K(ret), K(job));
      } else if (backup_set_list.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup set list is empty", K(ret), K(tenant_id));
      } else if (OB_FAIL(mgr->set_backup_set_info(backup_set_list, dest_id, ObTenantBackupDestInfoMgr::InfoType::RESTORE))) {
        LOG_WARN("failed to set restore info", K(ret), K(job));
      } else {
        succeed_set_dest_info_ = true;
        restore_job_id_ = job.get_job_id();
        LOG_INFO("refresh restore info for restore tenant", K(tenant_id), K(backup_set_list));
      }
    }
  } else {
    ObAllTenantInfo tenant_info;
    ObRestorePersistHelper persist_helper;
    ObPhysicalRestoreBackupDestList backup_dest_list;
    common::ObArray<share::ObBackupSetBriefInfo> backup_set_list;
    const ObBackupDestType::TYPE backup_dest_type = ObBackupDestType::DEST_TYPE_RESTORE_DATA;
    int64_t dest_id = 0;
    ObTenantBackupDestInfoMgr *mgr = MTL(ObTenantBackupDestInfoMgr *);
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, sql_proxy, false/*for_update*/, tenant_info))) {
      LOG_WARN("failed to load tenant info", K(ret), K(tenant_id));
    } else if (!tenant_info.get_restore_data_mode().is_remote_mode()) {
      // do nothing
    } else if (succeed_set_dest_info_) {
      // do nothing for succeed set dest info
    } else if (OB_FAIL(persist_helper.init(tenant_id, share::OBCG_STORAGE))) {
      LOG_WARN("failed to init persist helper", K(ret), K(tenant_id));
    } else if (OB_FAIL(persist_helper.get_backup_dest_list_from_restore_info(
                       *sql_proxy,
                       restore_job_id_,
                       backup_dest_list))) {
      LOG_WARN("failed to get backup dest list from restore info", K(ret), K(tenant_id));
    } else if (OB_FAIL(backup_dest_list.get_backup_set_brief_info_list(backup_set_list))) {
      LOG_WARN("fail to get backup set brief info list", K(ret), K(backup_dest_list));
    } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_restore_dest_id(*sql_proxy, tenant_id,
                                                                                  backup_dest_type, dest_id))) {
      LOG_WARN("failed to get restore dest id", K(ret), K(tenant_id), K(backup_dest_type));
    } else if (backup_set_list.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup set list is empty", K(ret), K(tenant_id));
    } else if (OB_FAIL(mgr->set_backup_set_info(backup_set_list, dest_id, ObTenantBackupDestInfoMgr::InfoType::RESTORE))) {
        LOG_WARN("failed to set restore info", K(ret));
    } else {
      succeed_set_dest_info_ = true;
      LOG_INFO("refresh restore info for normal tenant", K(tenant_id), K(backup_set_list));
    }
  }
  return ret;
}

int ObLSRestoreHandler::update_state_handle_()
{
  int ret = OB_SUCCESS;
  share::ObLSRestoreStatus new_status;
  ObILSRestoreState *new_state_handler = nullptr;
  if (OB_FAIL(ls_->get_restore_status(new_status))) {
    LOG_WARN("fail to get_restore_status", K(ret), KPC(ls_));
  } else if (!need_update_state_handle_(new_status)) { // no need update state handler
  } else if (OB_FAIL(fill_restore_arg())) {
    LOG_WARN("fail to fill restore arg", K(ret));
  } else {
    lib::ObMutexGuard guard(mtx_);
    if (OB_FAIL(get_restore_state_handler_(new_status, new_state_handler))) {
      LOG_WARN("fail to get restore state handler", K(ret), K(new_status));
    } else {
      restore_stat_.reset();
      ObLSRestoreJobPersistKey ls_key;
      ls_key.tenant_id_ = ls_->get_tenant_id();
      ls_key.job_id_ = ls_restore_arg_.get_job_id();
      ls_key.ls_id_ = ls_->get_ls_id();
      ls_key.addr_ = GCTX.self_addr();
      if (OB_FAIL(restore_stat_.init(ls_key))) {
        LOG_WARN("fail to init restore stat", K(ret), KPC_(ls));
      }

      // need reload restore stat after total_tablet_cnt has been reported.
      if (OB_FAIL(ret)) {
      } else if (!GCTX.is_shared_storage_mode()
                  && !new_status.is_before_restore_to_consistent_scn()
                  && OB_FAIL(restore_stat_.load_restore_stat())) {
        LOG_WARN("fail to load restore stat", K(ret), K(new_status), KPC_(ls));
      }

      if (OB_SUCC(ret)) {
        if (nullptr != state_handler_) {
          state_handler_->~ObILSRestoreState();
          allocator_.free(state_handler_);
          state_handler_ = nullptr;
        }
        state_handler_ = new_state_handler;
        result_mgr_.reset();
      }
    }

    if (OB_FAIL(ret) && nullptr != new_state_handler) {
      new_state_handler->~ObILSRestoreState();
      allocator_.free(new_state_handler);
    }
  }
  return ret;
}

bool ObLSRestoreHandler::need_update_state_handle_(share::ObLSRestoreStatus &new_status)
{
  lib::ObMutexGuard guard(mtx_);
  return nullptr == state_handler_ || new_status != state_handler_->get_restore_status();
}

int ObLSRestoreHandler::get_restore_state_handler_(const share::ObLSRestoreStatus &new_status, ObILSRestoreState *&new_state_handler)
{
  int ret = OB_SUCCESS;
  switch(new_status) {
    case ObLSRestoreStatus::Status::RESTORE_START: {
      ObLSRestoreStartState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreStartState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::RESTORE_SYS_TABLETS: {
      ObLSRestoreSysTabletState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreSysTabletState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::RESTORE_TABLETS_META: {
      ObLSRestoreCreateUserTabletState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreCreateUserTabletState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN: {
      ObLSRestoreConsistentScnState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreConsistentScnState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::WAIT_RESTORE_TO_CONSISTENT_SCN: {
      ObLSWaitRestoreConsistentScnState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSWaitRestoreConsistentScnState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::QUICK_RESTORE: {
      ObLSQuickRestoreState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSQuickRestoreState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH: {
      ObLSQuickRestoreFinishState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSQuickRestoreFinishState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA: {
      ObLSRestoreMajorState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreMajorState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::NONE: {
      ObLSRestoreFinishState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreFinishState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::WAIT_RESTORE_SYS_TABLETS: {
      ObLSRestoreWaitRestoreSysTabletState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreWaitRestoreSysTabletState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::WAIT_RESTORE_TABLETS_META: {
      ObLSRestoreWaitCreateUserTabletState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreWaitCreateUserTabletState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::WAIT_QUICK_RESTORE: {
      ObLSRestoreWaitQuickRestoreState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreWaitQuickRestoreState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::WAIT_RESTORE_MAJOR_DATA: {
      ObLSRestoreWaitRestoreMajorDataState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreWaitRestoreMajorDataState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
#ifdef OB_BUILD_SHARED_STORAGE
    case ObLSRestoreStatus::Status::SS_RESTORE_START: {
      ObSSLSRestoreStartState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObLSRestoreStartState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::SS_RESTORE_LS: {
      ObSSLSRestoreLSState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObSSLSRestoreLSState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::SS_RESTORE_WAIT_LS: {
      ObSSWaitLSRestoreState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObSSWaitLSRestoreState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
    case ObLSRestoreStatus::Status::SS_RESTORE_CLOG: {
      ObSSLSRestoreCLogState *tmp_ptr = nullptr;
      if (OB_FAIL(construct_state_handler_(tmp_ptr))) {
        LOG_WARN("fail to construct ObSSLSRestoreRestoreCLogState", K(ret), K(new_status));
      } else {
        new_state_handler = tmp_ptr;
      }
      break;
    }
#endif
    default: {
      ret = OB_ERR_SYS;
      LOG_ERROR("unknown ls restore status", K(ret), K(new_status));
    }
  }
  logservice::ObLogService *log_srv = nullptr;
  if (OB_FAIL(ret)) {
  } else if (nullptr == (log_srv = MTL(logservice::ObLogService*))) {
	  ret = OB_INVALID_ARGUMENT;
    LOG_WARN("log service is nullptr", K(ret));
  } else if (OB_FAIL(new_state_handler->init(*ls_, *log_srv, ls_restore_arg_))) {
    LOG_WARN("fail to init new state handler", K(ret), KPC(ls_));
  } else {
    LOG_INFO("success get new state handler", KPC(new_state_handler));
  }
  return ret;
}

template <typename T>
int ObLSRestoreHandler::construct_state_handler_(T *&new_handler)
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = nullptr;
  if (OB_ISNULL(tmp_ptr = allocator_.alloc(sizeof(T)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    new_handler = new (tmp_ptr) T();
  }
  return ret;
}

int ObLSRestoreHandler::deal_failed_restore_()
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mtx_);
  if (OB_ISNULL(state_handler_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("state handler is nullptr!", K(ret));
  } else if (OB_FAIL(state_handler_->deal_failed_restore(result_mgr_))) {
    LOG_WARN("fail to deal failed restore", K(ret), K(result_mgr_), KPC(state_handler_));
  }
  return ret;
}

void ObLSRestoreHandler::wakeup()
{
  int ret = OB_SUCCESS;
  ObStorageHAService *storage_ha_service = MTL(ObStorageHAService*);
  if (OB_ISNULL(storage_ha_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("storage ha handler service should not be NULL", K(ret), KP(storage_ha_service));
  } else {
    storage_ha_service->wakeup();
  }
}

int ObLSRestoreHandler::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  is_safe = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore handler do not init", K(ret));
  } else {
    lib::ObMutexGuard guard(mtx_);
    if (OB_FAIL(cancel_task_())) {
      LOG_WARN("failed to cancel tasks", K(ret), KPC(ls_));
    } else {
      is_safe = true;
      stop();
    }
  }
  LOG_INFO("wait ls restore stop", K(ret), K(is_safe), KPC(ls_));
  return ret;
}

int ObLSRestoreHandler::cancel_task_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(state_handler_)) {
  } else {
    ObLSRestoreTaskMgr &restore_tablet_mgr = state_handler_->get_tablet_mgr();
    if (OB_FAIL(restore_tablet_mgr.cancel_task())) {
      LOG_WARN("fail to check all task done", K(ret));
    }
  }
  return ret;
}

int ObLSRestoreHandler::update_rebuild_seq()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore handler do not init", K(ret));
  } else {
    ATOMIC_STORE(&rebuild_seq_, ls_->get_rebuild_seq());
  }
  return ret;
}

int64_t ObLSRestoreHandler::get_rebuild_seq()
{
  return ATOMIC_LOAD(&rebuild_seq_);
}

int ObLSRestoreHandler::fill_restore_arg()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy_ = GCTX.sql_proxy_;
  share::ObPhysicalRestoreTableOperator restore_table_operator;
  const uint64_t tenant_id = MTL_ID();
  int64_t job_id = 1;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't null", K(ret), K(sql_proxy_));
  } else if (OB_FAIL(restore_table_operator.init(sql_proxy_, tenant_id, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table operator", K(ret));
  } else {
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(
              tenant_id, job_info))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else {
        lib::ObMutexGuard guard(mtx_);
        ls_restore_arg_.job_id_ =  job_info.get_job_id();
        ls_restore_arg_.restore_type_ = job_info.get_restore_type();
        ls_restore_arg_.tenant_id_ = tenant_id;
        ls_restore_arg_.restore_scn_ = job_info.get_restore_scn();
        ls_restore_arg_.consistent_scn_ = job_info.get_consistent_scn();
        ls_restore_arg_.backup_cluster_version_ = job_info.get_source_cluster_version();
        ls_restore_arg_.backup_data_version_ = job_info.get_source_data_version();
        ls_restore_arg_.backup_compatible_ = static_cast<share::ObBackupSetFileDesc::Compatible>(job_info.get_backup_compatible());
        ls_restore_arg_.progress_display_mode_ = job_info.get_progress_display_mode();
        ls_restore_arg_.backup_set_list_.reset();
        ls_restore_arg_.backup_piece_list_.reset();
        if (OB_FAIL(ls_restore_arg_.backup_piece_list_.assign(
            job_info.get_multi_restore_path_list().get_backup_piece_path_list()))) {
          LOG_WARN("fail to get backup piece list", K(ret));
        } else if (OB_FAIL(job_info.get_multi_restore_path_list().get_backup_set_brief_info_list(
            ls_restore_arg_.backup_set_list_))) {
          LOG_WARN("fail to get backup set brief info list", K(ret), K(job_info));
        }
      }
    }
  }
  return ret;
}
