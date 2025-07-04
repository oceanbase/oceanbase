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
#include "ob_disaster_recovery_sslog_service.h"
#include "ob_disaster_recovery_task.h"
#include "ob_disaster_recovery_worker.h"
#include "ob_disaster_recovery_task_utils.h"
#include "common/ob_member_list.h"
#include "share/ob_rpc_struct.h"
#include "src/share/ls/ob_ls_table_operator.h"
#include "src/share/ob_ls_id.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace rootserver
{
ObDRCreateSSLOGTask::ObDRCreateSSLOGTask() : is_inited_(false),
                                             tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObDRCreateSSLOGTask::~ObDRCreateSSLOGTask()
{
  destroy();
}

int ObDRCreateSSLOGTask::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init sslog task twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    is_inited_ = true;
    tenant_id_ = tenant_id;
  }
  LOG_INFO("finish to init create sslog task", KR(ret));
  return ret;
}

int ObDRCreateSSLOGTask::start(const int tg_id)
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("start create sslog task", K_(tenant_id), K(tg_id));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create sslog task is not init", KR(ret));
  } else if (OB_UNLIKELY(INVALID_TG_ID == tg_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tg id", KR(ret), K(tg_id));
  } else if (OB_FAIL(TG_SCHEDULE(tg_id, *this, SCHEDULE_INTERVAL_US, true/*repeat*/))) {
    LOG_WARN("fail to schedule create sslog task", KR(ret), K_(tenant_id), K(tg_id));
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  FLOG_INFO("finish to start create sslog task", KR(ret), K_(tenant_id), K(tg_id), K(cost));
  return ret;
}

void ObDRCreateSSLOGTask::destroy()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_inited_ = false;
}

void ObDRCreateSSLOGTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create sslog task is not init", KR(ret));
  } else {
    LOG_TRACE("create sslog timer start work", K_(tenant_id));
    bool is_exist = false;
    bool is_hidden_sys = false;
    bool tenant_schema_is_ready = false;
    ObLSService *ls_service = MTL(ObLSService*);
    int64_t orig_paxos_replica_number = 0;
    common::ObAddr leader_server;
    bool is_tenant_will_be_deleted = false;
    ObNotifyTenantThreadArg notify_dr_arg;
    if (OB_ISNULL(GCTX.config_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("config_ is null", KR(ret), KP(GCTX.config_));
    } else if (!GCTX.config_->is_rereplication_enabled()) {
      LOG_TRACE("enable rereplication is disabled", K_(tenant_id));
    } else if (OB_ISNULL(GCTX.omt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("omt is null", KR(ret), KP(GCTX.omt_));
    } else if (OB_UNLIKELY(GCTX.omt_->check_if_hidden_sys(tenant_id_, is_hidden_sys))) {
      LOG_WARN("check if hidden sys failed", KR(ret), K_(tenant_id));
    } else if (is_hidden_sys) {
      LOG_TRACE("hiden sys tenant skip create sslog", K_(tenant_id));
    } else if (OB_FAIL(check_tenant_schema_is_ready_(tenant_schema_is_ready))) {
      LOG_WARN("failed to get tenant schema", KR(ret), K_(tenant_id));
    } else if (!tenant_schema_is_ready) {
      LOG_INFO("tenant schema not ready, no need create sslog", K_(tenant_id));
    } else if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mtl ObLSService should not be null", KR(ret));
    } else if (OB_FAIL(ls_service->check_ls_exist(SSLOG_LS, is_exist))) {
      LOG_WARN("failed to check ls exist", KR(ret));
    } else if (is_exist) {
      LOG_TRACE("local has sslog ls, skip create sslog ls", K_(tenant_id));
    } else if (OB_FAIL(ObStorageHAUtils::check_tenant_will_be_deleted(is_tenant_will_be_deleted))) {
      // Unit in WAIT_GC or DELETING status, skip create sslog LS.
      LOG_WARN("check tenant will be deleted failed", KR(ret));
    } else if (is_tenant_will_be_deleted) {
      LOG_INFO("tenant will be deleted, does not create sslog ls", K_(tenant_id));
    } else if (OB_FAIL(get_params_for_add_sslog_(orig_paxos_replica_number, leader_server))) {
      LOG_WARN("fail to get params for add sslog", KR(ret), K_(tenant_id));
    } else {
      FLOG_INFO("start to create sslog ls", K_(tenant_id));
      ObArray<ObDRTask*> dr_tasks;
      ObAddLSReplicaTask add_replica_task;
      ObDRTaskKey task_key;
      share::ObTaskId task_id;
      task_id.init(GCONF.self_addr_);
      ObDRWorker dr_worker;
      const char* commment = drtask::LOCAL_SSLOG_SERVICE_ADD_SSLOG_READONLY_REPLICA;
      ObReplicaMember dst_member(GCONF.self_addr_, ObTimeUtility::current_time(), REPLICA_TYPE_READONLY);
      ObReplicaMember data_source_member(leader_server, 0/*timstamp*/, REPLICA_TYPE_FULL);
      if (OB_FAIL(task_key.init(tenant_id_, SSLOG_LS, GCONF.zone.str(), ObDRTaskType::LS_ADD_REPLICA))) {
        LOG_WARN("fail to init task key", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(add_replica_task.build(
                                  task_key,
                                  task_id,
                                  commment,
                                  dst_member,
                                  data_source_member,
                                  ObReplicaMember()/*empty force_data_source*/,
                                  orig_paxos_replica_number,
                                  orig_paxos_replica_number))) {
        LOG_WARN("fail to build add replica task", KR(ret), K(task_key), K(task_id),
                      K(dst_member), K(orig_paxos_replica_number));
      } else if (OB_FAIL(dr_tasks.push_back(&add_replica_task))) {
        LOG_WARN("fail to build push back task", KR(ret), K(add_replica_task));
      } else if (OB_FAIL(dr_worker.persist_tasks_into_inner_table(dr_tasks, tenant_id_, SSLOG_LS, false/*is_manual*/))) {
        LOG_WARN("fail to persist tasks into table", KR(ret), K(add_replica_task));
      } else if (OB_FAIL(notify_dr_arg.init(tenant_id_, obrpc::ObNotifyTenantThreadArg::DISASTER_RECOVERY_SERVICE))) {
        LOG_WARN("failed to init arg", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(DisasterRecoveryUtils::wakeup_tenant_service(notify_dr_arg))) {
        LOG_WARN("fail to wake up", KR(ret), K(notify_dr_arg));
      }
    }
  }
}

int ObDRCreateSSLOGTask::get_params_for_add_sslog_(
    int64_t &orig_paxos_replica_number,
    common::ObAddr &leader_server)
{
  int ret = OB_SUCCESS;
  share::ObLSInfo ls_info;
  const share::ObLSReplica *leader_replica = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create sslog task is not init", KR(ret));
  } else if (OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls table operator", KR(ret));
  } else if (OB_FAIL(GCTX.lst_operator_->get(GCONF.cluster_id, tenant_id_, SSLOG_LS,
                     share::ObLSTable::COMPOSITE_MODE, ls_info))) {
    LOG_WARN("fail to get ls info", KR(ret), K_(tenant_id), K(ls_info));
  } else if (OB_FAIL(ls_info.find_leader(leader_replica))) {
    LOG_WARN("fail to get ls leader replica", KR(ret), K(ls_info));
  } else if (OB_ISNULL(leader_replica)) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("invalid leader replica", KR(ret), K(ls_info));
  } else {
    orig_paxos_replica_number = leader_replica->get_paxos_replica_number();
    leader_server = leader_replica->get_server();
  }
  return ret;
}

int ObDRCreateSSLOGTask::check_tenant_schema_is_ready_(
    bool &tenant_schema_is_ready)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  const ObSimpleTenantSchema *tenant_schema = NULL;
  tenant_schema_is_ready = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("create sslog task is not init", KR(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    LOG_INFO("tenant is creating", K_(tenant_id));
  } else if (tenant_schema->is_normal()) {
    tenant_schema_is_ready = true;
  }
  return ret;
}
ObDRSSLOGService::ObDRSSLOGService() : tg_id_(INVALID_TG_ID),
                                       is_inited_(false),
                                       is_stopped_(false),
                                       create_sslog_task_() {}

int ObDRSSLOGService::mtl_init(ObDRSSLOGService *&sslog_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sslog_service->init())) {
    LOG_WARN("fail to init sslog_service", KR(ret));
  }
  return ret;
}

int ObDRSSLOGService::init()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("init service", K(MTL_ID()));
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (is_user_tenant(MTL_ID())) {
    LOG_INFO("dr sslog service no need run on user tenant", K(MTL_ID()));
  } else if (OB_FAIL(create_sslog_task_.init(MTL_ID()))) {
    LOG_WARN("fail to init create sslog task", KR(ret));
  } else {
    is_inited_ = true;
  }
  FLOG_INFO("finish init service", K(MTL_ID()));
  return ret;
}

int ObDRSSLOGService::start()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("start sslog service", K(MTL_ID()));
  if (is_user_tenant(MTL_ID())) {
    LOG_INFO("dr sslog service no need run on user tenant", K(MTL_ID()));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::DRCreateSSLOGTimer, tg_id_))) {
    LOG_WARN("fail to create timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer thread", KR(ret), K_(tg_id));
  } else if (OB_FAIL(create_sslog_task_.start(tg_id_))) {
    LOG_WARN("fail to start prewarm map clean task", KR(ret), K_(tg_id));
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  FLOG_INFO("finish start sslog service", K(MTL_ID()), K(cost));
  return ret;
}

void ObDRSSLOGService::stop()
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("start to stop sslog service", K(MTL_ID()));
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  FLOG_INFO("finish to stop sslog service", K(MTL_ID()), K(cost));
}

int ObDRSSLOGService::wait()
{
  int ret = OB_SUCCESS;
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("start to wait sslog service", K(MTL_ID()));
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
  const int64_t cost = ObTimeUtility::fast_current_time() - start_time;
  FLOG_INFO("finish to wait sslog service", K(MTL_ID()), K(cost));
  return ret;
}

void ObDRSSLOGService::destroy()
{
  FLOG_INFO("start to destroy sslog service", K(MTL_ID()));
  is_stopped_ = true;
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  tg_id_ = INVALID_TG_ID;
  if (IS_INIT) {
    create_sslog_task_.destroy();
    is_inited_ = false;
  }
  FLOG_INFO("finish to destroy sslog service", K(MTL_ID()));
}

} // end namespace rootserver
} // end namespace oceanbase
