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

#define USING_LOG_PREFIX RS

#include "ob_restore_service.h"
#include "rootserver/backup/ob_backup_task_scheduler.h"
#include "logservice/ob_log_service.h"

using namespace oceanbase;
using namespace rootserver;
using namespace share;


ObRestoreService::ObRestoreService()
  : inited_(false),
    schema_service_(NULL),
    sql_proxy_(NULL),
    rpc_proxy_(NULL),
    srv_rpc_proxy_(NULL),
    self_addr_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    idle_time_us_(1),
    wakeup_cnt_(0),
    restore_scheduler_(),
    recover_table_scheduler_(),
    import_table_scheduler_()
#ifdef OB_BUILD_SHARED_STORAGE
    , ss_restore_scheduler_(),
    proposal_id_(0)
#endif
{
}

ObRestoreService::~ObRestoreService()
{
  if (!has_set_stop()) {
    stop();
    wait();
  }
}

void ObRestoreService::destroy()
{
  ObTenantThreadHelper::destroy();
  inited_ = false;
}

int ObRestoreService::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_),
        KP(GCTX.rs_rpc_proxy_), KP(GCTX.srv_rpc_proxy_), KP(GCTX.lst_operator_));
  } else if (OB_FAIL(ObTenantThreadHelper::create("REST_SER", lib::TGDefIDs::SimpleLSService, *this))) {
    LOG_WARN("failed to create thread", KR(ret));
  } else if (OB_FAIL(ObTenantThreadHelper::start())) {
    LOG_WARN("fail to start thread", KR(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    ObBackupTaskScheduler *backup_task_scheduler = MTL(ObBackupTaskScheduler *);
    if (OB_FAIL(ss_restore_scheduler_.init(*this))) {
      LOG_WARN("failed to init restore scheduler", K(ret));
    } else if (OB_ISNULL(backup_task_scheduler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup_task_scheduler should not be NULL", K(ret), KP(backup_task_scheduler));
    } else if (OB_FAIL(backup_task_scheduler->register_restore_srv(this))) {
      LOG_WARN("failed to register restore srv", K(ret));
    }
#endif
  } else if (OB_FAIL(restore_scheduler_.init(*this))) {
    LOG_WARN("failed to init restore scheduler", K(ret));
  }

  if (FAILEDx(recover_table_scheduler_.init(
      *GCTX.schema_service_, *GCTX.sql_proxy_, *GCTX.rs_rpc_proxy_, *GCTX.srv_rpc_proxy_))) {
    LOG_WARN("failed to init recover table scheduler", K(ret));
  } else if (OB_FAIL(import_table_scheduler_.init(*GCTX.schema_service_, *GCTX.sql_proxy_))) {
    LOG_WARN("failed to init import table scheduler", K(ret));
  } else {
    schema_service_ = GCTX.schema_service_;
    sql_proxy_ = GCTX.sql_proxy_;
    rpc_proxy_ = GCTX.rs_rpc_proxy_;
    srv_rpc_proxy_ = GCTX.srv_rpc_proxy_;
    tenant_id_ = is_sys_tenant(MTL_ID()) ? MTL_ID() : gen_user_tenant_id(MTL_ID());
    self_addr_ = GCTX.self_addr();

    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObRestoreService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t proposal_id = 0;

  // Call base class switch_to_leader to start thread
  if (OB_FAIL(ObTenantThreadHelper::switch_to_leader())) {
    LOG_WARN("failed to switch to leader in base class", KR(ret));
  } else if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::SYS_LS, role, proposal_id))) {
    LOG_WARN("get restore service role fail", K(ret));
  } else {
    proposal_id_ = proposal_id;
    wakeup();
    LOG_INFO("[RESTORE_SERVICE]switch to leader finish", K(proposal_id_));
  }
  return ret;
}
#endif

int ObRestoreService::idle()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!has_set_stop() && 0 < ATOMIC_LOAD(&wakeup_cnt_)) {
     // skip next idle
  } else {
    ObTenantThreadHelper::idle(idle_time_us_);
    idle_time_us_ = GCONF._restore_idle_time;
  }
  ATOMIC_SET(&wakeup_cnt_, 0);
  return ret;
}

int ObRestoreService::check_stop() const
{
  int ret = OB_SUCCESS;
  if (has_set_stop()) {
    ret = OB_CANCELED;
    LOG_WARN("restore service stopped", K(ret));
  }
  return ret;
}

void ObRestoreService::wakeup()
{
  ATOMIC_INC(&wakeup_cnt_);
  ObTenantThreadHelper::wakeup();
}

void ObRestoreService::do_work()
{
  LOG_INFO("[RESTORE] restore service start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObRSThreadFlag rs_work;
    // avoid using default idle time when observer restarts.
    idle_time_us_ = GCONF._restore_idle_time;
    const uint64_t tenant_id = MTL_ID();
    while (!has_set_stop()) {
      {
        ObCurTraceId::init(GCTX.self_addr());
        ObArray<ObPhysicalRestoreJob> job_infos;
        share::schema::ObSchemaGetterGuard schema_guard;
        const share::schema::ObTenantSchema *tenant_schema = NULL;
        if (OB_ISNULL(GCTX.schema_service_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", KR(ret));
        } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                OB_SYS_TENANT_ID, schema_guard))) {
          LOG_WARN("fail to get schema guard", KR(ret));
        } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
          LOG_WARN("failed to get tenant ids", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
        } else if (!tenant_schema->is_normal()) {
          //tenant not normal, maybe meta or sys tenant
          //while meta tenant not ready, cannot process tenant restore job
        } else {
#ifdef OB_BUILD_SHARED_STORAGE
          if (GCTX.is_shared_storage_mode()) {
            ss_restore_scheduler_.do_work();
          }
#endif
          if (!GCTX.is_shared_storage_mode()) {
            restore_scheduler_.do_work();
          }
          recover_table_scheduler_.do_work();
          import_table_scheduler_.do_work();
          idle_time_us_ = 10;
        }
      }//for schema guard, must be free
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
      idle();
    }
  }
  LOG_INFO("[RESTORE] restore service quit");
  return;
}

int ObRestoreService::do_reload_task(ObBackupTaskSchedulerQueue &queue)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ss_restore_scheduler_.reload_task(queue))) {
    LOG_WARN("failed to get need reload task", K(ret));
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObRestoreService::check_leader()
{
  int ret = OB_SUCCESS;
  common::ObRole role;
  int64_t proposal_id = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore service not inited", K(ret));
  } else if (OB_FAIL(MTL(logservice::ObLogService *)->get_palf_role(share::SYS_LS, role, proposal_id))) {
    if (OB_LS_NOT_EXIST == ret) {
      ret = OB_NOT_MASTER;
      LOG_WARN("sys ls is not exist, service can't be leader", K(ret));
    } else {
      LOG_WARN("get ObRestoreService role fail", K(ret));
    }
  } else if (common::ObRole::LEADER == role && proposal_id == proposal_id_) {
  } else {
    ret = OB_NOT_MASTER;
  }
  return ret;
}

int ObRestoreService::end_transaction(
    common::ObMySQLTransaction &trans,
    const int upstream_ret)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (trans.is_started()) {
    if (OB_SUCCESS == upstream_ret) {
      // Upper layer succeeded, check leader and commit
      if (OB_FAIL(check_leader())) {
        LOG_WARN("failed to recheck leader before commit", K(ret));
        if (OB_TMP_FAIL(trans.end(false))) {
          LOG_WARN("failed to rollback after leader check fail", K(ret), K(tmp_ret));
        }
      } else if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      // Upper layer failed, rollback (preserve original error)
      if (OB_FAIL(trans.end(false))) {
        LOG_WARN("failed to rollback trans", K(upstream_ret), K(ret));
      }
    }
  }

  return ret;
}

#endif // OB_BUILD_SHARED_STORAGE