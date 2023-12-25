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

#include "lib/lock/mutex.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"
#include "share/restore/ob_ls_restore_status.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_task.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"
#include "storage/tenant_snapshot/ob_tenant_clone_service.h"

namespace oceanbase
{
using namespace share;
using namespace observer;
namespace storage
{

int ObTenantCloneService::init(ObTenantMetaSnapshotHandler* meta_handler)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(meta_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_handler is unexpected null", KR(ret));
  } else if (OB_FAIL(startup_accel_handler_.init(ObStartupAccelType::TENANT_ACCEL))) {
    LOG_WARN("fail to init startup_accel_handler_", KR(ret));
  } else {
    meta_handler_ = meta_handler;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantCloneService::start()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(startup_accel_handler_.start())) {
    LOG_WARN("fail to start startup_accel_handler_", KR(ret));
  } else {
    is_started_ = true;
  }
  return ret;
}

void ObTenantCloneService::stop()
{
  if (is_started_) {
    startup_accel_handler_.stop();
    is_started_ = false;
  }
}

void ObTenantCloneService::wait()
{
  int ret = OB_SUCCESS;
  while(OB_FAIL(wait_())) {
    usleep(100000);
  }
}

int ObTenantCloneService::wait_()
{
  int ret = OB_SUCCESS;

  if (is_started_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObTenantCloneService is running when wait function is called", KR(ret), KPC(this));
    stop();
  } else {
    startup_accel_handler_.wait();
  }
  return ret;
}

void ObTenantCloneService::destroy()
{
  if (IS_INIT) {
    startup_accel_handler_.destroy();
    is_inited_ = false;
  }
}

int ObTenantCloneService::get_clone_job_(ObArray<ObCloneJob>& clone_jobs)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTenantCloneTableOperator clone_op;
  clone_jobs.reset();

  if (OB_FAIL(clone_op.init(tenant_id, GCTX.sql_proxy_))) {
    LOG_WARN("fail init", KR(ret));
  } else if (OB_FAIL(clone_op.get_all_clone_jobs(clone_jobs))) {
    LOG_WARN("fail to get all clone jobs", KR(ret));
  } else if (clone_jobs.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_INFO("clone jobs in inner table not exist", KR(ret));
  } else if (clone_jobs.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("clone job count not validate", KR(ret), K(clone_jobs));
  } else if (!clone_jobs.at(0).is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("clone job is not valid", KR(ret), K(clone_jobs.at(0)));
  } else {
    LOG_INFO("get clone jobs succ", K(clone_jobs));
  }

  return ret;
}

int ObTenantCloneService::try_clone_(const ObCloneJob& job)
{
  int ret = OB_SUCCESS;
  ObTraceIDGuard trace_guard(job.get_trace_id());

  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLSIterator *ls_iter = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantCloneService does not init", KR(ret));
  } else if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get ls iter", KR(ret));
  } else if (OB_ISNULL(ls_iter = ls_iter_guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls iter should not be NULL", KR(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObLS *ls = nullptr;
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", KR(ret));
      } else {
        try_clone_one_ls_(job, ls);
      }
    }
  }

  return ret;
}

void ObTenantCloneService::try_clone_one_ls_(const ObCloneJob& job, ObLS* ls)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  } else {
    bool next_loop = false;
    do {
      next_loop = false;
      ObLSRestoreStatus restore_status;
      ObMigrationStatus migration_status = OB_MIGRATION_STATUS_MAX;
      if (OB_FAIL(ls->get_restore_status(restore_status))) {
        LOG_WARN("fail to get_restore_status", KR(ret), KPC(ls));
      } else if (!restore_status.is_in_clone()) {
        if (restore_status.is_none()) {
          LOG_INFO("the ls is in none status", KR(ret), KPC(ls));
        } else {
          LOG_WARN("the ls is in unexpected restore status", KR(ret), KPC(ls), K(restore_status));
        }
      } else if (OB_FAIL(ls->get_migration_status(migration_status))) {
        LOG_WARN("fail to get migration status", KR(ret), KPC(ls));
      } else if (OB_MIGRATION_STATUS_NONE != migration_status) {
        LOG_INFO("the ls is in migrate status, will not be executed", KR(ret), KPC(ls));
      } else {
        LOG_INFO("the ls is in clone status", KR(ret), KPC(ls), K(restore_status));

        share::ObLSRestoreStatus next_status = restore_status;
        drive_clone_ls_sm_(job, restore_status, ls, next_status, next_loop);

        if (next_status != restore_status) {
          if (OB_FAIL(advance_status_(ls, next_status))) {
            LOG_WARN("advance_status_ failed", KR(ret), KPC(ls), K(restore_status));
          } else {
            if (OB_TMP_FAIL(ls->report_replica_info())) {
              LOG_WARN("fail to report replica info", KR(tmp_ret), KPC(ls));
            }
            SERVER_EVENT_ADD("clone", "clone_ls",
              "tenant_id", MTL_ID(),
              "ls_id", ls->get_ls_id().id(),
              "curr_status", ObLSRestoreStatus::get_restore_status_str(restore_status),
              "next_status", ObLSRestoreStatus::get_restore_status_str(next_status));
          }
        }
      }
    } while(next_loop);
  }
}

int ObTenantCloneService::advance_status_(ObLS* ls, const share::ObLSRestoreStatus &next_status)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  } else if (OB_FAIL(ls->set_restore_status(next_status, ls->get_rebuild_seq()))) {
    LOG_WARN("failed to update restore status", KR(ret), KPC(ls), K(next_status));
  } else {
    LOG_INFO("advance clone status success", "ls_id", ls->get_ls_id(), K(next_status));
  }

  return ret;
}

void ObTenantCloneService::drive_clone_ls_sm_(const ObCloneJob& job,
                                              const ObLSRestoreStatus& restore_status,
                                              ObLS* ls,
                                              ObLSRestoreStatus& next_status,
                                              bool& next_loop)
{
  int ret = OB_SUCCESS;
  next_status = restore_status;
  next_loop = false;

  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  } else {
    switch (restore_status)
    {
    case ObLSRestoreStatus::Status::CLONE_START:
      handle_clone_start_(job, ls, next_status, next_loop);
      break;
    case ObLSRestoreStatus::Status::CLONE_COPY_ALL_TABLET_META:
      handle_copy_all_tablet_meta_(job, ls, next_status, next_loop);
      break;
    case ObLSRestoreStatus::Status::CLONE_COPY_LS_META:
      handle_copy_ls_meta_(job, ls, next_status, next_loop);
      break;
    case ObLSRestoreStatus::Status::CLONE_CLOG_REPLAY:
      handle_clog_replay_(job, ls, next_status, next_loop);
      break;
    case ObLSRestoreStatus::Status::CLONE_FAILED:
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid clone status", KR(ret), K(restore_status), KPC(ls));
      break;
    }
  }
}

int ObTenantCloneService::check_ls_status_valid_(const ObLSID& ls_id)
{
  int ret = OB_SUCCESS;

  ObLSStatusInfo status_info;
  ObLSStatusOperator ls_status_operator;
  if (OB_FAIL(ls_status_operator.get_ls_status_info(MTL_ID(), ls_id, status_info, *GCTX.sql_proxy_))) {
    LOG_WARN("fail to get ls status info", KR(ret), K(ls_id));
  } else if (!status_info.ls_is_created() &&
             !status_info.ls_is_normal() &&
             !status_info.ls_is_dropping()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_INFO("ls status info is not valid", KR(ret), K(ls_id), K(status_info));
  } else {
    LOG_INFO("ls status info is valid", K(ls_id), K(status_info));
  }
  return ret;
}

void ObTenantCloneService::handle_clone_start_(const ObCloneJob& job,
                                               ObLS* ls,
                                               ObLSRestoreStatus& next_status,
                                               bool& next_loop)
{
  int ret = OB_SUCCESS;
  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  } else if (OB_FAIL(check_ls_status_valid_(ls->get_ls_id()))) {
    LOG_INFO("fail to check_ls_status_valid_", KR(ret), KPC(ls));
  } else {
    next_status = ObLSRestoreStatus::Status::CLONE_COPY_ALL_TABLET_META;
    next_loop = true;
    FLOG_INFO("handle_clone_start_ succ", KPC(ls));
  }
}

void ObTenantCloneService::handle_copy_all_tablet_meta_(const ObCloneJob& job,
                                                        ObLS* ls,
                                                        ObLSRestoreStatus& next_status,
                                                        bool& next_loop)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  uint64_t source_tenant_id = OB_INVALID_TENANT_ID;
  ObTenantSnapshotID tenant_snapshot_id;
  ObLSID ls_id;

  blocksstable::MacroBlockId tablet_meta_entry;
  bool has_inc_clone_ref = false;

  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  } else if (ls->get_tablet_svr()->get_tablet_count() != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls tablet count is not zero", KR(ret), KPC(ls), KPC(ls->get_tablet_svr()));
  } else {
    source_tenant_id = job.get_source_tenant_id();
    tenant_snapshot_id = job.get_tenant_snapshot_id();
    ls_id = ls->get_ls_id();
  }

  if (OB_SUCC(ret)) {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_FAIL(guard.switch_to(source_tenant_id, false))) {
      LOG_WARN("fail to switch to tenant",
          KR(ret), K(source_tenant_id), K(tenant_snapshot_id), K(ls_id));
    } else if (OB_FAIL(MTL(ObTenantSnapshotService*)->start_clone(tenant_snapshot_id,
                                                                  ls_id,
                                                                  tablet_meta_entry))) {
      LOG_WARN("fail to start_clone", KR(ret), K(source_tenant_id), K(tenant_snapshot_id), K(ls_id));
    } else {
      has_inc_clone_ref = true;
      FLOG_INFO("inc snapshot clone ref succ",
          K(source_tenant_id), K(tenant_snapshot_id), K(ls_id));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(meta_handler_->create_all_tablet(&startup_accel_handler_, tablet_meta_entry))) {
      LOG_WARN("fail to create_all_tablet",
          KR(ret), K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
    } else {
      FLOG_INFO("create_all_tablet succ",
          K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
    }
  }

  if (has_inc_clone_ref) {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_TMP_FAIL(guard.switch_to(job.get_source_tenant_id(), false))) {
      LOG_WARN("fail to switch to tenant",
          KR(ret), K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
    } else if (OB_TMP_FAIL(MTL(ObTenantSnapshotService*)->end_clone(job.get_tenant_snapshot_id()))) {
      LOG_WARN("fail to end_clone",
          KR(ret), K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
    } else {
      FLOG_INFO("dec snapshot clone ref succ",
          K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
    }
  }

  if (OB_SUCC(ret)) {
    next_status = ObLSRestoreStatus::Status::CLONE_COPY_LS_META;
    next_loop = true;
    FLOG_INFO("handle_copy_all_tablet_meta_ succ",
        K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
  } else if (OB_EAGAIN == ret) {
    FLOG_INFO("handle_copy_all_tablet_meta_ eagain",
        K(source_tenant_id), K(tenant_snapshot_id), K(ls_id), K(tablet_meta_entry));
  } else {
    next_status = ObLSRestoreStatus::Status::CLONE_FAILED;
    next_loop = false;
  }
  return ;
}

void ObTenantCloneService::handle_copy_ls_meta_(const ObCloneJob& job,
                                                ObLS* ls,
                                                ObLSRestoreStatus& next_status,
                                                bool& next_loop)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;

  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  } else if (!ls->is_offline()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is unexpected online", KR(ret), KPC(ls));
  } else {
    ls_id = ls->get_ls_id();
  }

  if (OB_SUCC(ret)) {
    HEAP_VAR(ObLSMetaPackage, ls_meta_package) {
      {
        ObTenantSnapshotTableOperator snap_op;

        MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
        if (OB_FAIL(guard.switch_to(job.get_source_tenant_id(), false))) {
          LOG_WARN("fail to switch to tenant", KR(ret), K(job.get_source_tenant_id()));
        } else if (OB_FAIL(ObTenantSnapshotMetaTable::acquire_clone_ls_meta_package(job.get_tenant_snapshot_id(),
                                                                                    ls_id,
                                                                                    ls_meta_package))) {
          LOG_WARN("fail to acquire_clone_ls_meta_package", KR(ret), K(job), K(ls_id));
        } else if (!ls_meta_package.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls_meta_package is not valid",
              KR(ret), K(ls_meta_package), K(job.get_tenant_snapshot_id()), K(ls_id));
        } else {
          LOG_INFO("acquire_clone_ls_meta_package succ", K(job), K(ls_id), K(ls_meta_package.ls_meta_));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ls->update_ls_meta(false, ls_meta_package.ls_meta_))) {
          LOG_WARN("fail to update ls meta", KR(ret), K(job), KPC(ls));
        } else if (OB_FAIL(ls->set_dup_table_ls_meta(ls_meta_package.dup_ls_meta_, true))) {
          LOG_WARN("fail to set dup table ls meta", KR(ret), K(job), KPC(ls));
        } else if (OB_FAIL(ls->online())) {
          LOG_WARN("fail to online ls", KR(ret), KPC(ls));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    next_status = ObLSRestoreStatus::Status::CLONE_CLOG_REPLAY;
    next_loop = false;
    FLOG_INFO("handle_copy_ls_meta_ succ",
        K(job.get_source_tenant_id()), K(job.get_tenant_snapshot_id()), K(ls_id));
  } else {
    next_status = ObLSRestoreStatus::Status::CLONE_FAILED;
    next_loop = false;
  }
  return ;
}

void ObTenantCloneService::handle_clog_replay_(const ObCloneJob& job,
                                               ObLS* ls,
                                               ObLSRestoreStatus& next_status,
                                               bool& next_loop)
{
  int ret = OB_SUCCESS;

  if (!job.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("clone job is not valid", KR(ret), K(job));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is unexpected nullptr", KR(ret));
  }

  if (OB_SUCC(ret)) {
    bool restore_clog_done = false;
    logservice::ObLogRestoreHandler *log_restore_handle = nullptr;
    if (OB_ISNULL(log_restore_handle = ls->get_log_restore_handler())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log restore handler is unexpected nullptr", KR(ret));
    } else if (OB_FAIL(log_restore_handle->check_restore_done(job.get_restore_scn(), restore_clog_done))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to check log restore done", KR(ret), K(job), KPC(ls), K(restore_clog_done));
      } else {
        LOG_INFO("fail to check log restore done", KR(ret), K(job), KPC(ls), K(restore_clog_done));
      }
    } else {
      next_status = ObLSRestoreStatus::Status::NONE;
      next_loop = false;
      FLOG_INFO("handle_clog_replay_ succ", KPC(ls));
    }
  }
  return ;
}

void ObTenantCloneService::run()
{
  int ret = OB_SUCCESS;

  ObArray<ObCloneJob> clone_jobs;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_clone_job_(clone_jobs))) {
      LOG_WARN("fail to get_clone_job_", KR(ret));
    } else {
      LOG_INFO("get clone job succ", K(clone_jobs));
    }
  }

  if (OB_SUCC(ret)) {
    ObCloneJob &job = clone_jobs.at(0);
    if (OB_FAIL(try_clone_(job))) {
      LOG_INFO("fail to try_clone_", KR(ret), K(job));
    }
  }

  return ;
}

}
}
