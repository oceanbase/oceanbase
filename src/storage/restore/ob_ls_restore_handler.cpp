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
#include <utility>
#include "storage/restore/ob_ls_restore_handler.h"
#include "lib/lock/ob_mutex.h"
#include "common/ob_member.h"
#include "storage/ls/ob_ls.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "share/backup/ob_backup_path.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/ls/ob_ls_status_operator.h"
#include "logservice/ob_log_service.h"
#include "storage/high_availability/ob_ls_restore.h"
#include "storage/high_availability/ob_tablet_group_restore.h"
#include "storage/high_availability/ob_storage_ha_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "share/restore/ob_physical_restore_table_operator.h"
#include "storage/backup/ob_backup_data_store.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/restore/ob_restore_persist_helper.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase;
using namespace share;
using namespace common;
using namespace storage;
using namespace backup;
using namespace logservice;

ObLSRestoreHandler::ObLSRestoreHandler()
  : is_inited_(false),
    is_stop_(false),
    is_online_(true),
    rebuild_seq_(0),
    result_mgr_(),
    ls_(nullptr),
    ls_restore_arg_(),
    state_handler_(nullptr),
    allocator_()
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
}

int ObLSRestoreHandler::offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!is_online_) {
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
          is_online_ = false;
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
  } else if (is_online_) {
    // do nothing
    LOG_INFO("ls restore handler is already online");
  } else if (OB_FAIL(ls_->get_restore_status(new_status))) {
    LOG_WARN("fail to get_restore_status", K(ret), KPC(ls_));
  } else if (new_status.is_restore_none()) {
    is_online_ = true;
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
      is_online_ = true;
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
  LOG_INFO("succeed recieve handle pull tablet from leader", K(ret));
  lib::ObMutexGuard guard(mtx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(state_handler_)) {
    ret = OB_ERR_SELF_IS_NULL;
    LOG_WARN("need restart, wait later", KPC(ls_), K(is_stop_), K(is_online_));
  } else if (OB_FAIL(state_handler_->handle_pull_tablet(tablet_ids, leader_restore_status, leader_proposal_id))) {
    LOG_WARN("fail to handl pull tablet", K(ret), K(leader_restore_status), K(leader_proposal_id));
  }
  return ret;
}

int ObLSRestoreHandler::process()
{
  int ret = OB_SUCCESS;

  bool can_do_restore;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_before_do_restore_(can_do_restore))) {
    LOG_WARN("fail to check before do restore", K(ret), KPC(ls_));
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
    if (is_stop_ || !is_online_) {
      LOG_INFO("ls stopped or disabled", KPC(ls_));
    } else if (OB_FAIL(state_handler_->do_restore())) {
      ObTaskId trace_id(*ObCurTraceId::get_trace_id());
      result_mgr_.set_result(ret, trace_id, ObLSRestoreResultMgr::RestoreFailedType::DATA_RESTORE_FAILED_TYPE);
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
  if (is_stop()) { 
  } else if (OB_FAIL(check_meta_tenant_normal_(is_normal))) {
    LOG_WARN("fail to get meta tenant status", K(ret));
  } else if (!is_normal) {
  } else if (OB_FAIL(ls_->get_restore_status(restore_status))) {
    LOG_WARN("fail to get_restore_status", K(ret), KPC(ls_));
  } else if (restore_status.is_restore_none()) {
    lib::ObMutexGuard guard(mtx_);
    if (OB_NOT_NULL(state_handler_)) {
      state_handler_->~ObILSRestoreState();
      allocator_.free(state_handler_);
      state_handler_ = nullptr;
    }
  } else if (restore_status.is_restore_failed()) {
  } else if (OB_FAIL(check_restore_job_exist_(is_exist))) {
  } else if (!is_exist) {
    if (OB_FAIL(ls_->set_restore_status(ObLSRestoreStatus(ObLSRestoreStatus::RESTORE_FAILED), get_rebuild_seq()))) {
      LOG_WARN("failed to set restore status", K(ret), KPC(ls_));
      if (OB_STATE_NOT_MATCH == ret && OB_FAIL(update_rebuild_seq())) {
        LOG_WARN("failed to update rebuild seq", K(ret));
      }
    }
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
  ObAddr self_addr = GCONF.self_addr_;
  is_in_member_or_learner_list = false;
  if (OB_FAIL(ls_->get_log_handler()->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, learner_list))) {
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
  } else if (OB_FAIL(restore_table_operator.init(sql_proxy_, tenant_id))) {
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

int ObLSRestoreHandler::update_state_handle_()
{
  int ret = OB_SUCCESS;
  share::ObLSRestoreStatus new_status;
  ObILSRestoreState *new_state_handler = nullptr;
  if (OB_FAIL(ls_->get_restore_status(new_status))) {
    LOG_WARN("fail to get_restore_status", K(ret), KPC(ls_));
  } else if (nullptr != state_handler_
      && new_status == state_handler_->get_restore_status()) { // no need update state handler
  } else if (OB_FAIL(fill_restore_arg())) {
    LOG_WARN("fail to fill restore arg", K(ret));
  } else {
    lib::ObMutexGuard guard(mtx_);
    if (OB_FAIL(get_restore_state_handler_(new_status, new_state_handler))) {
      LOG_WARN("fail to get restore state handler", K(ret), K(new_status));
    } else {
      if (nullptr != state_handler_) {
        state_handler_->~ObILSRestoreState();
        allocator_.free(state_handler_);
        state_handler_ = nullptr;
      }
      state_handler_ = new_state_handler;
    }

    if (OB_FAIL(ret) && nullptr != new_state_handler) {
      new_state_handler->~ObILSRestoreState();
      allocator_.free(new_state_handler);
    }
  }
  return ret;
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
    case ObLSRestoreStatus::Status::RESTORE_NONE: {
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
  if (OB_FAIL(state_handler_->deal_failed_restore(result_mgr_))) {
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
      is_stop_ = true;
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
  } else if (OB_FAIL(update_restore_status_(*ls_, next_status))) {
    LOG_WARN("failed to update restore status", K(ret), KPC(ls_), K(next_status));
  } else if (OB_FAIL(result_mgr.get_comment_str(ls_->get_ls_id(), self_addr_, comment))) {
    LOG_WARN("fail to get comment str", K(ret));
  } else if (OB_FAIL(report_ls_restore_progress_(*ls_, next_status, result_mgr.get_trace_id(),
      result_mgr.get_result(), comment.ptr()))) {
    LOG_WARN("fail to report ls restore progress", K(ret));
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
    LOG_WARN("fail to reprot ls restore progress", K(tmp_ret), K(ls), K(next_status));
  }
  return ret;
}

int ObILSRestoreState::report_ls_restore_progress_(
    storage::ObLS &ls, const share::ObLSRestoreStatus &status,
    const share::ObTaskId &trace_id, const int result, const char *comment)
{
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  ObLSRestoreJobPersistKey ls_key;
  ls_key.tenant_id_ = ls.get_tenant_id();
  ls_key.job_id_ = ls_restore_arg_->get_job_id();
  ls_key.ls_id_ = ls.get_ls_id();
  ls_key.addr_ = self_addr_;
  if (OB_FAIL(helper.init(ls_key.tenant_id_))) {
    LOG_WARN("fail to init restore table helper", K(ret), "tenant_id", ls_key.tenant_id_);
  } else if (OB_FAIL(helper.update_ls_restore_status(*proxy_, ls_key, trace_id, status, result, comment))) {
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
  if (OB_FAIL(helper.init(ls_restore_info.key_.tenant_id_))) {
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
    } else if (OB_FAIL(tablet_group_restore_arg.src_.set_replica_type(leader.get_replica_type()))) {
      LOG_WARN("fail to set src replica type", K(ret), K(leader));
    } else if (OB_FAIL(tablet_group_restore_arg.src_.set_member(ObMember(leader.get_server(), 0/*invalid timestamp is ok*/)))) {
      LOG_WARN("fail to set src member", K(ret));
    } else if (OB_FAIL(tablet_group_restore_arg.dst_.set_replica_type(REPLICA_TYPE_FULL))) {
      LOG_WARN("fail to set dst replica type", K(ret));
    } else if (OB_FAIL(tablet_group_restore_arg.dst_.set_member(ObMember(GCTX.self_addr(), 0/*invalid timestamp is ok*/)))) {
      LOG_WARN("fail to set dst member", K(ret), "server", GCTX.self_addr());
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
  int64_t readonly_replica_count = 0;
  if (OB_ISNULL(log_handler = ls_->get_log_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler should not be NULL", K(ret));
  } else if (OB_FAIL(log_handler->get_paxos_member_list_and_learner_list(member_list, paxos_replica_num, learner_list))) {
    LOG_WARN("failed to get paxos member list and learner list", K(ret));
  } else if (OB_FAIL(location_service_->get(follower_info.cluster_id_, tenant_id, ls_->get_ls_id(), expire_renew_time, is_cache_hit, location))) {
    LOG_WARN("fail to get location", K(ret), KPC(ls_));
  } else if (OB_FAIL(location.get_replica_count(full_replica_count, readonly_replica_count))) {
    LOG_WARN("fail to get replica count in location", KR(ret), K(location), K(full_replica_count), K(readonly_replica_count));
  } else if (full_replica_count != paxos_replica_num || readonly_replica_count != learner_list.get_member_number()) {
    ret = OB_REPLICA_NUM_NOT_MATCH;
    LOG_WARN("replica num not match, ls may in migration", K(ret), K(location), K(full_replica_count),
             K(readonly_replica_count), K(member_list), K(paxos_replica_num), K(learner_list));
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
  } else if (leader_status.is_wait_restore_major_data() && follower_status.is_restore_none()) {
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
  if (!leader_status.is_valid() || leader_status.is_restore_failed()) {
    // leader may restore failed or switch leader
  } else if (leader_status.is_restore_none()) {
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
      if (REACH_TENANT_TIME_INTERVAL(1000 * 1000 * 60 * 5)) {
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
  ObLogRestoreHandler *log_restore_handle = nullptr;
  bool is_created = false;
  bool is_exist = true;
  bool is_ready = false;
  LOG_INFO("ready to start restore ls", K(ls_restore_status_), KPC(ls_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(log_restore_handle = ls_->get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log restore handle can't nullptr", K(ret), K(log_restore_handle));
  } else if (OB_FAIL(check_ls_created_(is_created))) {
    LOG_WARN("fail to check ls created", K(ret), KPC(ls_));
  } else if (!is_created) {
    if (OB_FAIL(do_with_uncreated_ls_())) {
      LOG_WARN("fail to do with uncreadted ls", K(ret), KPC(ls_));
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

int ObLSRestoreStartState::check_ls_leader_ready_(bool &is_ready)
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
    } else if (OB_FAIL(proxy_->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
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

int ObLSRestoreStartState::do_with_no_ls_meta_()
{
  int ret = OB_SUCCESS;
  // ls with no ls meta means it created after backup ls_attr_infos.
  // this ls doesn't have ls meta and tablet in backup, it only needs to replay clog.
  // so just advance to restore to consistent_scn and start replay clog.
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_TO_CONSISTENT_SCN);
  if (OB_FAIL(online_())) {
    LOG_WARN("fail to enable log", K(ret));
  } else if (OB_FAIL(report_start_replay_clog_lsn_())) {
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
  ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_NONE);
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
  if (OB_FAIL(helper.init(key.tenant_id_))) {
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
  } else if (OB_FAIL(restore_table_operator.init(sql_proxy_, tenant_id))) {
    LOG_WARN("fail to init restore table operator", K(ret));
  } else {
    HEAP_VAR(ObPhysicalRestoreJob, job_info) {
      if (OB_FAIL(restore_table_operator.get_job_by_tenant_id(
              tenant_id, job_info))) {
        LOG_WARN("fail to get restore job", K(ret), K(tenant_id));
      } else {
        lib::ObMutexGuard guard(mtx_);
        ls_restore_arg_.job_id_ =  job_info.get_job_id();
        ls_restore_arg_.restore_type_ = share::ObRestoreType::NORMAL_RESTORE; // quick restore or normal restore
        ls_restore_arg_.tenant_id_ = tenant_id;
        ls_restore_arg_.restore_scn_ = job_info.get_restore_scn();
        ls_restore_arg_.consistent_scn_ = job_info.get_consistent_scn();
        ls_restore_arg_.backup_cluster_version_ = job_info.get_source_cluster_version();
        ls_restore_arg_.backup_data_version_ = job_info.get_source_data_version();
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
  } else if (OB_FAIL(ls_status_operator.get_ls_status_info(user_tenant_id, ls_->get_ls_id(), status_info, *sql_proxy))) {
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
  storage::ObBackupDataStore store;
  const ObArray<share::ObRestoreBackupSetBriefInfo> &backup_set_array = ls_restore_arg_->get_backup_set_list();
  int idx = backup_set_array.count() - 1;
  ObLSMetaPackage ls_meta_packge;
  is_exist = false;
  if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup_set_array can't empty", K(ret), K(ls_restore_arg_));
  } else if (OB_FAIL(store.init(backup_set_array.at(idx).backup_set_path_.ptr()))) {
    LOG_WARN("fail to init backup data store", K(ret));
  } else if (OB_FAIL(store.read_ls_meta_infos(ls_->get_ls_id(), ls_meta_packge))) {
    if (OB_ENTRY_NOT_EXIST) {
      is_exist = false;
      ret = OB_SUCCESS;
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
      if (replica.get_restore_status().is_restore_none()) {
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
  ObCurTraceId::init(GCONF.self_addr_);
  ObTaskId task_id(*ObCurTraceId::get_trace_id());
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
  } else if (OB_FAIL(arg.src_.set_replica_type(leader.get_replica_type()))) {
    LOG_WARN("fail to set src replica type", K(ret), K(leader));
  } else if (OB_FAIL(arg.src_.set_member(ObMember(leader.get_server(), 0/*invalid timestamp is ok*/)))) {
    LOG_WARN("fail to set src member", K(ret));
  } else if (OB_FAIL(arg.dst_.set_replica_type(REPLICA_TYPE_FULL))) {
    LOG_WARN("fail to set dst replica type", K(ret));
  } else if (OB_FAIL(arg.dst_.set_member(ObMember(GCTX.self_addr(), 0/*invalid timestamp is ok*/)))) {
    LOG_WARN("fail to set dst member", K(ret), "server", GCTX.self_addr());
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
  ObCurTraceId::init(GCONF.self_addr_);
  ObTaskId task_id(*ObCurTraceId::get_trace_id());
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

  while (OB_SUCC(ret)) {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ObTabletCreateDeleteMdsUserData user_data;
    bool is_commited = false;
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
    } else if (!tablet->get_tablet_meta().has_transfer_table()) {
    } else if (OB_FAIL(tablet->get_latest_tablet_status(user_data, is_commited))) {
      LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
    } else if (!is_commited && ObTabletStatus::TRANSFER_IN == user_data.tablet_status_.get_status()) {
      LOG_INFO("skip tablet which transfer in not commit", "tablet_id", tablet->get_tablet_meta().tablet_id_, K(user_data));
    } else if (!tablet->get_tablet_meta().ha_status_.is_restore_status_full()) {
      LOG_INFO("skip tablet which restore status is not full",
               "tablet_id", tablet->get_tablet_meta().tablet_id_,
               "ha_status", tablet->get_tablet_meta().ha_status_);
    } else if (OB_FAIL(ls_->update_tablet_restore_status(tablet->get_tablet_meta().tablet_id_, restore_status))) {
      LOG_WARN("failed to update tablet restore status to EMPTY", K(ret), KPC(tablet));
    } else {
      LOG_INFO("update tablet restore status to EMPTY",
               "tablet_meta", tablet->get_tablet_meta());
    }
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
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  ObLogRestoreHandler *log_restore_handle = ls_->get_log_restore_handler();
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
  ObSArray<ObTabletID> restored_tablets;
  ObLSRestoreTaskMgr::ToRestoreTabletGroup tablet_need_restore;
  ObLogRestoreHandler *log_restore_handle = ls_->get_log_restore_handler();
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
  ObCurTraceId::init(GCONF.self_addr_);
  ObTaskId task_id(*ObCurTraceId::get_trace_id());
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

int ObLSQuickRestoreState::check_clog_replay_finish_(bool &is_finish)
{
  int ret = OB_SUCCESS;
  bool done = false;
  ObLogRestoreHandler *log_restore_handle = nullptr;
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

int ObLSQuickRestoreState::check_tablet_checkpoint_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLSTabletIterator iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ObTablet *tablet = nullptr;

  if (OB_ISNULL(ls_tablet_svr = ls_->get_tablet_svr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls_tablet_svr is nullptr", K(ret));
  } else if (OB_FAIL(ls_tablet_svr->build_tablet_iter(iterator))) {
    LOG_WARN("fail to get tablet iterator", K(ret), KPC(ls_));
  } else {
    while (OB_SUCC(ret)) {
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
        }
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
  } else if (ls_restore_arg_->get_restore_type().is_normal_restore()) {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA);
    bool all_finish = false;
    if (OB_FAIL(check_all_follower_restore_finish_(all_finish))) {
      LOG_WARN("fail to request follower restore meta result", K(ret), KPC(ls_));
    } else if (!all_finish) {
    } else if (OB_FAIL(advance_status_(*ls_, next_status))) {
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
    ObLSRestoreStatus leader_restore_status;
    ObLSRestoreStatus next_status(ObLSRestoreStatus::Status::RESTORE_MAJOR_DATA);
    if (OB_FAIL(request_leader_status_(leader_restore_status))) {
      LOG_WARN("fail to request leader tablets and status", K(ret), KPC(ls_));
    } else if (check_leader_restore_finish_(leader_restore_status, ls_restore_status_)
        && OB_FAIL(advance_status_(*ls_, next_status))) {
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

int ObLSRestoreMajorState::do_restore()
{
  DEBUG_SYNC(BEFORE_RESTORE_MAJOR);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
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
  ObCurTraceId::init(GCONF.self_addr_);
  ObTaskId task_id(*ObCurTraceId::get_trace_id());
  ObTabletGroupRestoreArg arg;
  bool reach_dag_limit = false;
  bool is_new_election = false;
  // No matter is leader or follower, always restore data from backup.
  if (OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill leader ls restore arg", K(ret));
  }

#if 0
  // TODO(wangxiaohui.wxh): 4.3, let leader restore from backup and follower restore from leader.
  if (!is_follower(role_) && OB_FAIL(leader_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill ls restore arg", K(ret));
  } else if (is_follower(role_) && OB_FAIL(follower_fill_tablet_group_restore_arg_(tablet_need_restore.get_tablet_list(), tablet_need_restore.action(), arg))) {
    LOG_WARN("fail to fill ls restore arg", K(ret));
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
    LOG_WARN("fail to schedule schedule tablet group restore", KR(ret), K(arg), K(ls_restore_status_));
  } else {
    LOG_INFO("success schedule restore major", K(ret), K(arg), K(ls_restore_status_));
  }
  return ret;
}

//================================ObLSRestoreFinishState=======================================
ObLSRestoreFinishState::ObLSRestoreFinishState()
  : ObILSRestoreState(ObLSRestoreStatus::Status::RESTORE_NONE)
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

ObLSRestoreWaitState::ObLSRestoreWaitState(const share::ObLSRestoreStatus::Status &status)
  : ObILSRestoreState(status), has_confirmed_(false)
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
    next_status = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
  } else if (ls_restore_status_.is_wait_restore_major_data()) {
    DEBUG_SYNC(BEFORE_WAIT_MAJOR_RESTORE);
    next_status = ObLSRestoreStatus::Status::RESTORE_NONE;
  }
  LOG_INFO("leader is wait follower", "leader current status", ls_restore_status_, "next status", next_status, KPC(ls_));
  if (OB_FAIL(check_all_follower_restore_finish_(all_finish))) {
    LOG_WARN("fail to request follower restore meta result", K(ret), KPC(ls_));
  } else if (!all_finish) {
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
    next_status = ObLSRestoreStatus::Status::QUICK_RESTORE_FINISH;
  } else if (ls_restore_status_.is_wait_restore_major_data()) {
    next_status = ObLSRestoreStatus::Status::RESTORE_NONE;
  }

  LOG_INFO("follower is wait leader", "follower current status", ls_restore_status_, "next status", next_status, KPC(ls_));
  ObLSRestoreStatus leader_restore_status(ObLSRestoreStatus::Status::LS_RESTORE_STATUS_MAX);
  if (OB_FAIL(request_leader_status_(leader_restore_status))) {
    LOG_WARN("fail to request leader tablets and status", K(ret), KPC(ls_));
  } else if (check_leader_restore_finish_(leader_restore_status, ls_restore_status_)) {
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
  if (OB_FAIL(restore_table_operator.init(proxy_, tenant_id))) {
    LOG_WARN("fail to init restore table operator", K(ret), K(tenant_id));
  } else {
    ObLSRestoreStatus next_status(ObLSRestoreStatus::QUICK_RESTORE);
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
  return ret;
}


ObLSRestoreResultMgr::ObLSRestoreResultMgr()
  : mtx_(),
    result_(OB_SUCCESS),
    retry_cnt_(0),
    last_err_ts_(0),
    trace_id_(),
    failed_type_(RestoreFailedType::MAX_FAILED_TYPE)
{
}

bool ObLSRestoreResultMgr::can_retry() const
{
  int64_t max_retry_cnt = OB_MAX_RESTORE_RETRY_TIMES;
#ifdef ERRSIM
  if (0 != GCONF.errsim_max_restore_retry_count) {
    max_retry_cnt = GCONF.errsim_max_restore_retry_count;
  }
#endif
  return retry_cnt_ < max_retry_cnt &&  can_retrieable_err(result_);
}

bool ObLSRestoreResultMgr::is_met_retry_time_interval()
{
  lib::ObMutexGuard guard(mtx_);
  bool bret = false;
  int64_t cur_ts = ObTimeUtility::current_time();
  if (last_err_ts_ + OB_MAX_LS_RESTORE_RETRY_TIME_INTERVAL <= cur_ts) {
    bret = true;
  }
  return bret;
}

void ObLSRestoreResultMgr::set_result(const int result, const share::ObTaskId &trace_id,
     const RestoreFailedType &failed_type)
{
  // update result_ conditions:
  // 1. result_ is OB_SUCCESS;
  // 2. result_ is retrieable err, but input result is non retrieable err.
  lib::ObMutexGuard guard(mtx_);
  if (OB_EAGAIN == result) {
  } else {
    if (retry_cnt_ >= OB_MAX_RESTORE_RETRY_TIMES) { // avoiding overwrite error code
    } else if ((!can_retrieable_err(result) && can_retrieable_err(result_))
        || OB_SUCCESS == result_) {
      result_ = result;
      trace_id_.set(trace_id);
      failed_type_ = failed_type;
    }
    retry_cnt_++;
  }
  last_err_ts_ = ObTimeUtility::current_time();
}

int ObLSRestoreResultMgr::get_comment_str(const ObLSID &ls_id, const ObAddr &addr, ObHAResultInfo::Comment &comment) const
{
  ObHAResultInfo::FailedType type = RestoreFailedType::DATA_RESTORE_FAILED_TYPE == failed_type_ ?
                                    ObHAResultInfo::RESTORE_DATA : ObHAResultInfo::RESTORE_CLOG;
  ObHAResultInfo result_info(type, ls_id, addr, trace_id_, result_);
  return result_info.get_comment_str(comment);
}

bool ObLSRestoreResultMgr::can_retrieable_err(const int err) const
{
  bool bret = true;
  switch (err) {
    case OB_NOT_INIT :
    case OB_INVALID_ARGUMENT :
    case OB_ERR_UNEXPECTED :
    case OB_ERR_SYS :
    case OB_INIT_TWICE :
    case OB_CANCELED :
    case OB_NOT_SUPPORTED :
    case OB_TENANT_HAS_BEEN_DROPPED :
    case OB_SERVER_OUTOF_DISK_SPACE :
    case OB_BACKUP_FILE_NOT_EXIST :
    case OB_ARCHIVE_ROUND_NOT_CONTINUOUS :
    case OB_HASH_NOT_EXIST:
    case OB_TOO_MANY_PARTITIONS_ERROR:
      bret = false;
      break;
    default:
      break;
  }
  return bret;
}
