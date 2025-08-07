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

#include "logservice/ob_log_service.h" // for ObLogService
#include "lib/lock/ob_spin_rwlock.h" // for SpinRWLock
#include "ob_ddl_service_launcher.h"
#include "observer/ob_server_event_history_table_operator.h" // for SERVER_EVENT_ADD
#include "share/ob_server_struct.h"     // for GCTX
#include "share/rc/ob_tenant_base.h"    // for MTL_ID
#include "rootserver/ob_root_service.h" // for ObRootService
#include "share/scheduler/ob_partition_auto_split_helper.h" // for ObRsAutoSplitScheduler reset

namespace oceanbase
{
namespace rootserver
{
bool ObDDLServiceLauncher::is_ddl_service_started_ = false;
ObDDLServiceLauncher::ObDDLServiceLauncher()
  : inited_(false)
{
}

int ObDDLServiceLauncher::mtl_init(ObDDLServiceLauncher *&ddl_service_launcher)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin mtl_init for ddl_service_launcher");
  if (!is_sys_tenant(MTL_ID())) {
    LOG_INFO("ddl service launcher should run on SYS tenant", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_NOT_NULL(ddl_service_launcher)) {
    if (OB_FAIL(ddl_service_launcher->init())) {
      LOG_WARN("failed to init ddl_service_launcher", KR(ret));
    }
  }
  int64_t duration_time = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish mtl_init for ddl_service_launcher",
            KR(ret), "tenant_id", MTL_ID(), K(duration_time));
  return ret;
}

int ObDDLServiceLauncher::init()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin init for ddl_service_launcher");
  if (!is_sys_tenant(MTL_ID())) {
    // do nothing, only sys tenant should create this launcher
    LOG_INFO("ddl service launcher should start on sys tenant", "tenant_id", MTL_ID());
  } else if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else {
    inited_ = true;
  }
  int64_t duration_time = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish init for ddl_service_launcher", KR(ret),
            "tenant_id", MTL_ID(), K(duration_time));
  return ret;
}

void ObDDLServiceLauncher::destroy()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin destroy for ddl_service_launcher");
  if (!is_sys_tenant(MTL_ID())) {
    LOG_INFO("new ddl scheduler should run on SYS tenant", "tenant_id", MTL_ID());
  } else {
    inited_ = false;
  }
  int64_t duration_time = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish destroy for ddl_service_launcher", KR(ret),
            "tenant_id", MTL_ID(), K(duration_time));
}

int ObDDLServiceLauncher::switch_to_leader()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin switch_to_leader for ddl_service_launcher");
  if (!is_sys_tenant(MTL_ID())) {
    LOG_INFO("ddl service launcher should run on SYS tenant", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl service launcher is not inited", KR(ret), K_(inited));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_5_2) {
    // we only activate ddl service by new logic with cluster version up to 4.3.5.2
    // for compatible reason. We add new variables for ObRefreshSchemaInfo to replace
    // previous use of sequence_id_, that is ObDDLSequenceID. We use seq_id_ in
    // ObDDLSequenceID to serialize previous sequence_id_.
    // In order to let new ObRefreshSchemaInfo correctly serialized,
    // we have to make sure cluster version up to 4.3.5.2
    LOG_INFO("cluster version is below 4.3.5.2, use old logic");
  } else if (OB_FAIL(inner_start_ddl_service_with_lock_(
                        true/*with_new_mode*/,
                        0/*proposal_id_to_check: not used with new_mode*/,
                        0/*new_rs_epoch: not used with new_mode*/))) {
    LOG_WARN("fail to inner start ddl service with lock", KR(ret));
  }
  int64_t duration_time = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish switch_to_leader for ddl_service_launcher", KR(ret),
            "tenant_id", MTL_ID(), K(duration_time));
  return ret;
}

int ObDDLServiceLauncher::get_sys_palf_role_and_epoch(
    common::ObRole &role,
    int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  role = FOLLOWER;
  proposal_id = 0;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.omt_));
  } else if (OB_UNLIKELY(!GCTX.omt_->has_tenant(OB_SYS_TENANT_ID))) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("local server does not have SYS tenant resource", KR(ret));
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      if (OB_ISNULL(log_service)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("mtl ObLogService should not be null", KR(ret), KP(log_service));
      } else if (OB_FAIL(log_service->get_palf_role(SYS_LS, role, proposal_id))) {
        LOG_WARN("failed to get role from palf", KR(ret));
      }
    }
  }
  return ret;
}

int ObDDLServiceLauncher::start_ddl_service_with_old_logic(
    const int64_t new_rs_epoch,
    const int64_t proposal_id_to_check)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin start ddl service by old logic");
  if (OB_UNLIKELY(OB_INVALID_ID == new_rs_epoch)
      || OB_UNLIKELY(OB_INVALID_ID == proposal_id_to_check)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_rs_epoch), K(proposal_id_to_check));
  } else if (OB_FAIL(inner_start_ddl_service_with_lock_(
                         false/*with_new_mode*/,
                         proposal_id_to_check,
                         new_rs_epoch))) {
    LOG_WARN("fail to inner start ddl service with lock",
             KR(ret), K(new_rs_epoch), K(proposal_id_to_check));
  }
  int64_t duration_time = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish start ddl service by old logic", KR(ret),
            "tenant_id", MTL_ID(), K(duration_time));
  return ret;
}

int ObDDLServiceLauncher::resume_leader()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin resume_leader for ddl_service_launcher");
  if (OB_FAIL(switch_to_leader())) {
    LOG_WARN("fail to switch to leader", KR(ret));
  }
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish resume_leader for ddl_service_launcher", KR(ret));
  return ret;
}

void ObDDLServiceLauncher::switch_to_follower_forcedly()
{
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin switch_to_follower_forcedly for ddl_service_launcher");
  switch_to_follower_gracefully();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish switch_to_follower_forcedly for ddl_service_launcher");
}

int ObDDLServiceLauncher::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] begin switch_to_follower_gracefully for ddl_service_launcher");
  if (!is_sys_tenant(MTL_ID())) {
    LOG_INFO("ddl service launcher should run on SYS tenant", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl service launcher is not inited", KR(ret), K_(inited));
  } else {
    SpinWLockGuard guard(rw_lock_);
    if (OB_NOT_NULL(GCTX.root_service_)) {
      // try reset cache for schema refresh
      GCTX.root_service_->get_ddl_service().get_index_name_checker().reset_all_cache();
      FLOG_INFO("reset index name checker success");
      GCTX.root_service_->get_ddl_service().get_non_partitioned_tablet_allocator().reset_all_cache();
      FLOG_INFO("reset non partitioned tablet allocator success");
      ObRsAutoSplitScheduler::get_instance().reset_direct_cache();
      FLOG_INFO("reset RS auto split scheduler direct cache success");
    }
    ATOMIC_SET(&is_ddl_service_started_, false);
  }
  // rw_lock_ is released now
  int64_t duration_time = ObTimeUtility::current_time() - start_time;
  FLOG_INFO("[DDL_SERVICE_LAUNCHER] finish switch_to_follower_gracefully for ddl_service_launcher", KR(ret),
            "tenant_id", MTL_ID(), K(duration_time));
  return ret;
}

int ObDDLServiceLauncher::init_sequence_id_(
    bool with_new_mode,
    const int64_t proposal_id,
    const int64_t new_rs_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == proposal_id)
     || OB_UNLIKELY(!with_new_mode && OB_INVALID_ID == new_rs_epoch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("proposal id not valid", KR(ret), K(with_new_mode), K(proposal_id), K(new_rs_epoch));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.root_service_));
  } else {
    ObRefreshSchemaInfo schema_info;
    int64_t schema_version = OB_INVALID_VERSION;
    ObSchemaService *schema_service = GCTX.root_service_->get_schema_service().get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(schema_service));
    } else if (!with_new_mode) {
      // init sequence id with old logic
      if (OB_FAIL(schema_service->init_sequence_id_by_rs_epoch(new_rs_epoch))) {
        LOG_WARN("init sequence id by rootservice epoch failed", KR(ret), K(new_rs_epoch));
      } else if (OB_FAIL(GCTX.root_service_->get_schema_service().get_tenant_refreshed_schema_version(
                             OB_SYS_TENANT_ID, schema_version))) {
        LOG_WARN("fail to get sys tenant refreshed schema version", KR(ret));
      } else if (schema_version <= OB_CORE_SCHEMA_VERSION + 1) {
        // in bootstrap and new schema mode, to avoid write failure while schema_version change,
        // only actively refresh schema at the end of bootstrap, and make heartbeat's refresh_schema_info effective.
        // There are two things we have to pay attention to:
        // (1) sequence_id whether is OB_INVALID_ID
        //     before 4.3.5.2 sequence_id is OB_INVALID_ID when bootstrap or observer restart
        //     so observer could decide whether to refresh schema by checking sequence_id
        //     if sequence_id = OB_INVALID_ID, observer should decide whether refresh schema
        //     by checking baseline_schema_version.
        //     in 4.3.5.2, sequence id is valid after sys leader elected which before bootstrap finished,
        //     so we remove the logic to init_sequence_id when bootstrap
        //     and let observer ignore schema refresh request by checking GCTX.is_bootstrap_
        //     GCTX.is_bootstrap_ setted true only if sys tenant full schema is refreshed
        // (2) the moment sys tenant full schema refreshed successfully
        //     When RS start ddl service with old mode, RS must refreshed sys tenant schema successfully.
        //     observer could begin refreshing schema when sys tenant schema refreshed by checking GCTX.is_bootstrap_
        //     Although this check seams useeless, we still reserve it.
        LOG_INFO("sys tenant schema version not refreshed, do not trigger schema refresh",
                 KR(ret), K(schema_version));
      } else if (OB_FAIL(schema_service->set_refresh_schema_info(schema_info))) {
        LOG_WARN("fail to set refresh schema info", KR(ret), K(schema_info));
      }
    } else {
      // init sequence id with new logic
      if (OB_FAIL(schema_service->init_sequence_id_by_sys_leader_epoch(proposal_id))) {
        LOG_WARN("fail to init sequence id by sys leader epoch", KR(ret), K(proposal_id));
      } else if (OB_FAIL(schema_service->set_refresh_schema_info(schema_info))) {
        LOG_WARN("fail to set refresh schema info", K(ret), K(schema_info));
      }
    }
  }
  return ret;
}

int ObDDLServiceLauncher::inner_start_ddl_service_with_lock_(
    bool with_new_mode,
    const int64_t proposal_id_to_check,
    const int64_t new_rs_epoch)
{
  int ret = OB_SUCCESS;
  common::ObRole role = FOLLOWER;
  int64_t proposal_id = 0;
  // 0. hold lock before do pre-checking
  SpinWLockGuard guard(rw_lock_);
  // 1. precheck input parameters
  //    proposal id used to double check should be valid with old mode
  if (OB_UNLIKELY((!with_new_mode && OB_INVALID_ID == proposal_id_to_check)
      || OB_UNLIKELY(!with_new_mode && OB_INVALID_ID == new_rs_epoch))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("proposal id should be valid with old mode", KR(ret),
             K(with_new_mode), K(proposal_id_to_check), K(new_rs_epoch));
  // 2. check leader: make sure local is leader and get proposal id from palf
  //    For new logic: just check leaader is ok
  //    For old logic: need to double check proposal id not changed
  //    consider this case:
  //      a. server_A is previous leader and this old RS trigger start
  //         ddl service with old proposal_id
  //      b. then leader switched to server_B and then switched back to server_A
  //         we should prevent this start ddl service request triggered from old RS
  //         by double checking proposal id not changed
  } else if (OB_FAIL(get_sys_palf_role_and_epoch(role, proposal_id))) {
    LOG_WARN("fail to get role and proposal id", KR(ret));
  } else if (!is_strong_leader(role)) {
    ret = OB_LS_NOT_LEADER;
    LOG_WARN("local is not sys leader", KR(ret), K(role));
  } else if (!with_new_mode && proposal_id != proposal_id_to_check) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("leader already switched", KR(ret), K(proposal_id), K(proposal_id_to_check));
  // 3. init sequence id and set refresh schema info
  } else if (OB_FAIL(init_sequence_id_(with_new_mode, proposal_id, new_rs_epoch))) {
    LOG_WARN("fail to init sequence id", KR(ret), K(with_new_mode), K(proposal_id), K(new_rs_epoch));
  // 4. reset ddl epoch for all tenant
  //    ddl epoch should reset to trigger ddl_epoch to promote
  //    when ddl operation performing ObDDLSQLTransaction::lock_all_ddl_operation,
  //    ddl epoch will promoted and persist new ddl epoch both in __all_core_table and memory
  } else if (OB_FAIL(GCTX.root_service_->get_schema_service().get_ddl_epoch_mgr().remove_all_ddl_epoch())) {
    LOG_WARN("fail to remove ddl epoch", KR(ret));
  // 5. set is_ddl_service_started_ only if operations above all correctly done.
  //    this static variable means whether ddl service can provide service
  //    it has two meanings:
  //      (1) ddl service is ready with old logic
  //          ddl operations rely on RS full service
  //      (2) ddl service is ready with new logic
  //          from 4.3.5.2 ddl just rely on sys leader, do not concern status of RS
  } else {
    ATOMIC_SET(&is_ddl_service_started_, true);
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase
