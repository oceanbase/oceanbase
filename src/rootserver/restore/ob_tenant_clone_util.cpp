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

#define USING_LOG_PREFIX RS_RESTORE

#include "ob_tenant_clone_util.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_table_operator.h"
#include "share/restore/ob_tenant_clone_table_operator.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_global_stat_proxy.h" // for ObGlobalStatProxy
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::share;

//a source tenant only has one clone_job at the same time
int ObTenantCloneUtil::check_source_tenant_has_clone_job(
    common::ObISQLClient &sql_client,
    const uint64_t source_tenant_id,
    bool &has_job)
{
  int ret = OB_SUCCESS;
  has_job = false;
  ObTenantCloneTableOperator clone_op;
  ObCloneJob clone_job;

  if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
    LOG_WARN("fail to init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.get_clone_job_by_source_tenant_id(
                            source_tenant_id, clone_job))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get job", KR(ret), K(source_tenant_id));
    }
  } else {
    has_job = true;
  }

  return ret;
}

int ObTenantCloneUtil::check_clone_tenant_exist(common::ObISQLClient &sql_client,
                                                const ObString &clone_tenant_name,
                                                bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObTenantCloneTableOperator clone_op;
  ObCloneJob clone_job;

  if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
    LOG_WARN("fail to init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.get_clone_job_by_clone_tenant_name(
                            clone_tenant_name, false/*need lock*/, clone_job))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get job", KR(ret), K(clone_tenant_name));
    }
  } else {
    is_exist = true;
    LOG_INFO("clone job exist", KR(ret), K(clone_tenant_name));
  }

  return ret;
}

int ObTenantCloneUtil::fill_clone_job(const int64_t job_id,
                                      const obrpc::ObCloneTenantArg &arg,
                                      const uint64_t source_tenant_id,
                                      const ObString &source_tenant_name,
                                      const ObTenantSnapItem &snapshot_item,
                                      ObCloneJob &clone_job)
{
  int ret = OB_SUCCESS;
  clone_job.reset();
  ObTenantCloneJobType job_type = ObTenantCloneJobType::CLONE_JOB_MAX_TYPE;
  common::ObCurTraceId::TraceId trace_id;
  uint64_t data_version = 0;
  uint64_t min_cluster_version = 0;

  if (OB_UNLIKELY(job_id < 0
                  || !arg.is_valid()
                  || !is_user_tenant(source_tenant_id)
                  || source_tenant_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(arg), K(source_tenant_id),
                                 K(source_tenant_name), K(snapshot_item));
  } else if (OB_FAIL(construct_data_version_to_record_(source_tenant_id, data_version, min_cluster_version))) {
    LOG_WARN("fail to construct data version to record", KR(ret), K(source_tenant_id));
  } else {
    job_type = snapshot_item.is_valid() ?
               ObTenantCloneJobType::RESTORE :
               ObTenantCloneJobType::FORK;
    ObCurTraceId::TraceId *cur_trace_id = ObCurTraceId::get_trace_id();
    if (nullptr != cur_trace_id) {
      trace_id = *cur_trace_id;
    } else {
      trace_id.init(GCONF.self_addr_);
    }
    const ObCloneJob::ObCloneJobInitArg init_arg = {
          .trace_id_                   = trace_id,
          .tenant_id_                  = OB_SYS_TENANT_ID,
          .job_id_                     = job_id,
          .source_tenant_id_           = source_tenant_id,
          .source_tenant_name_         = source_tenant_name,
          .clone_tenant_id_            = OB_INVALID_TENANT_ID,
          .clone_tenant_name_          = arg.get_new_tenant_name(),
          .tenant_snapshot_id_         = snapshot_item.get_tenant_snapshot_id(),
          .tenant_snapshot_name_       = snapshot_item.get_snapshot_name(),
          .resource_pool_id_           = OB_INVALID_ID,
          .resource_pool_name_         = arg.get_resource_pool_name(),
          .unit_config_name_           = arg.get_unit_config_name(),
          .restore_scn_                = snapshot_item.get_snapshot_scn(),
          .status_                     = ObTenantCloneStatus(ObTenantCloneStatus::Status::CLONE_SYS_LOCK),
          .job_type_                   = job_type,
          .ret_code_                   = OB_SUCCESS,
          .data_version_               = data_version,
          .min_cluster_version_        = min_cluster_version,
    };
    if (OB_FAIL(clone_job.init(init_arg))) {
      LOG_WARN("fail to init clone job", KR(ret), K(init_arg));
    }
  }

  return ret;
}

int ObTenantCloneUtil::construct_data_version_to_record_(
    const uint64_t tenant_id,
    uint64_t &data_version,
    uint64_t &min_cluster_version)
{
  int ret = OB_SUCCESS;
  data_version = 0;
  min_cluster_version = 0;
  bool need_to_record_data_version = false;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::check_compat_version_for_clone_standby_tenant(
                         tenant_id, need_to_record_data_version))) {
    LOG_WARN("fail to check whether need to record data version", KR(ret), K(tenant_id));
  } else if (need_to_record_data_version) {
    ObGlobalStatProxy proxy(*GCTX.sql_proxy_, tenant_id);
    if (OB_FAIL(proxy.get_current_data_version(data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
    } else {
      min_cluster_version = GET_MIN_CLUSTER_VERSION();
    }
  } else {
    // data version not promoted, make sure data_version and min_cluster_version are 0
    data_version = 0;
    min_cluster_version = 0;
  }
  return ret;
}

int ObTenantCloneUtil::record_clone_job(common::ObISQLClient &sql_client,
                                        const share::ObCloneJob &clone_job)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;
  uint64_t source_tenant_id = clone_job.get_source_tenant_id();
  ObString clone_tenant_name = clone_job.get_clone_tenant_name();
  bool has_job = false;
  bool tenant_exist_in_clone_job = false;

  if (OB_UNLIKELY(!clone_job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clone_job));
  } else if (OB_FAIL(check_source_tenant_has_clone_job(sql_client, source_tenant_id, has_job))) {
    LOG_WARN("fail to check source tenant has clone job", KR(ret), K(source_tenant_id));
  } else if (has_job) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("source tenant already has clone job", KR(ret), K(source_tenant_id));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "source tenant has a running clone job, clone tenant now");
  } else if (OB_FAIL(ObTenantCloneUtil::check_clone_tenant_exist(sql_client,
                              clone_tenant_name, tenant_exist_in_clone_job))) {
    LOG_WARN("failed to check clone tenant exist in clone job", KR(ret), K(clone_tenant_name));
  } else if (tenant_exist_in_clone_job) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("duplicate clone tenant name in clone job", KR(ret), K(clone_tenant_name));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "clone tenant name exists in a running clone job, clone tenant now");
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
    LOG_WARN("fail to init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.insert_clone_job(clone_job))) {
    LOG_WARN("fail to insert clone job", KR(ret), K(clone_job));
  }

  return ret;
}

int ObTenantCloneUtil::update_resource_pool_id_of_clone_job(common::ObISQLClient &sql_client,
                                                            const int64_t job_id,
                                                            const uint64_t resource_pool_id)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;

  if (OB_UNLIKELY(job_id < 0 || OB_INVALID_ID == resource_pool_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(resource_pool_id));
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
    LOG_WARN("fail to init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.update_job_resource_pool_id(job_id, resource_pool_id))) {
    LOG_WARN("fail to update clone job resource pool id", KR(ret), K(job_id), K(resource_pool_id));
  }

  return ret;
}

int ObTenantCloneUtil::update_snapshot_info_for_fork_job(common::ObISQLClient &sql_client,
                                                         const int64_t job_id,
                                                         const ObTenantSnapshotID tenant_snapshot_id,
                                                         const ObString &tenant_snapshot_name)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;

  if (OB_UNLIKELY(job_id < 0 || !tenant_snapshot_id.is_valid() ||
                  tenant_snapshot_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(tenant_snapshot_id), K(tenant_snapshot_name));
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
    LOG_WARN("fail to init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.update_job_snapshot_info(job_id,
                                                       tenant_snapshot_id,
                                                       tenant_snapshot_name))) {
    LOG_WARN("fail to update clone job id and name", KR(ret), K(job_id), K(tenant_snapshot_id), K(tenant_snapshot_name));
  }

  return ret;
}

int ObTenantCloneUtil::update_restore_scn_for_fork_job(common::ObISQLClient &sql_client,
                                                       const int64_t job_id,
                                                       const SCN &restore_scn)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;

  if (OB_UNLIKELY(OB_INVALID_ID == job_id || !restore_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(restore_scn));
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
    LOG_WARN("fail to init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.update_job_snapshot_scn(job_id,
                                                      restore_scn))) {
    LOG_WARN("fail to update clone job snapshot scn", KR(ret), K(job_id), K(restore_scn));
  }

  return ret;
}

int ObTenantCloneUtil::insert_user_tenant_clone_job(common::ObISQLClient &sql_client,
                                                    const ObString &clone_tenant_name,
                                                    const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(clone_tenant_name.empty() || !is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clone_tenant_name), K(user_tenant_id));
  } else {
    ObTenantCloneTableOperator clone_op;
    ObCloneJob clone_job;
    ObCloneJob user_clone_job;
    if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &sql_client))) {
      LOG_WARN("fail init clone op", KR(ret));
    } else if (OB_FAIL(clone_op.get_clone_job_by_clone_tenant_name(
                              clone_tenant_name, false/*need_lock*/, clone_job))) {
      LOG_WARN("fail to get clone job", KR(ret), K(clone_tenant_name));
    } else if (OB_FAIL(user_clone_job.assign(clone_job))) {
      LOG_WARN("fail to assign clone job", KR(ret), K(clone_job));
    } else {
      user_clone_job.set_tenant_id(user_tenant_id);
      user_clone_job.set_clone_tenant_id(user_tenant_id);
      user_clone_job.set_status(ObTenantCloneStatus::Status::CLONE_USER_PREPARE);
      ObTenantCloneTableOperator user_clone_op;
      if (OB_FAIL(user_clone_op.init(user_tenant_id, &sql_client))) {
        LOG_WARN("fail init clone op", KR(ret), K(user_tenant_id));
      } else if (OB_FAIL(user_clone_op.insert_clone_job(user_clone_job))) {
        LOG_WARN("fail to insert clone job", KR(ret), K(user_clone_job));
      }
    }
  }
  return ret;
}

int ObTenantCloneUtil::recycle_clone_job(common::ObISQLClient &sql_client,
                                         const ObCloneJob &job)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;
  ObMySQLTransaction trans;
  const uint64_t tenant_id = job.get_tenant_id();

  if (OB_UNLIKELY(!job.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job));
  } else if (OB_FAIL(trans.start(&sql_client, gen_meta_tenant_id(tenant_id)))) {
    LOG_WARN("fail to start trans", KR(ret), K(gen_meta_tenant_id(tenant_id)));
  } else if (OB_FAIL(clone_op.init(tenant_id, &trans))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(clone_op.insert_clone_job_history(job))) {
    LOG_WARN("fail to insert clone job history", KR(ret), K(job));
  } else if (OB_FAIL(clone_op.remove_clone_job(job))) {
    LOG_WARN("fail to remove clone job", KR(ret), K(job));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, KR(tmp_ret), KR(ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  return ret;
}

int ObTenantCloneUtil::notify_clone_scheduler(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader_addr;
  obrpc::ObNotifyCloneSchedulerArg arg;
  arg.set_tenant_id(tenant_id);
  obrpc::ObNotifyCloneSchedulerResult res;

  if (OB_UNLIKELY(!is_sys_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
                  GCONF.cluster_id, tenant_id, ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("failed to get leader address", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).by(tenant_id).notify_clone_scheduler(arg, res))) {
    LOG_WARN("failed to notify clone scheduler", KR(ret), K(leader_addr), K(arg));
  } else {
    int res_ret = res.get_result();
    if (OB_SUCCESS != res_ret) {
      ret = res_ret;
      LOG_WARN("the result of notify clone scheduler failed", KR(res_ret), K(leader_addr), K(arg));
    }
  }
  return ret;
}

int ObTenantCloneUtil::release_clone_tenant_resource_of_clone_job(const ObCloneJob &clone_job)
{
  int ret = OB_SUCCESS;
  const int64_t timeout = GCONF._ob_ddl_timeout;
  const uint64_t resource_pool_id = clone_job.get_resource_pool_id();
  const uint64_t clone_tenant_id = clone_job.get_clone_tenant_id();
  const ObString &clone_tenant_name = clone_job.get_clone_tenant_name();

  if (!clone_job.get_status().is_sys_failed_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("try to release resource of a processing or success job", KR(ret), K(clone_job));
  } else if (OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret));
  } else if (OB_INVALID_ID == resource_pool_id) {
    // clone tenant and resource pool have not been created
    // it's no need to release resource
  } else {
    if (OB_INVALID_TENANT_ID != clone_tenant_id) {
      obrpc::ObDropTenantArg arg;
      ObArenaAllocator allocator;
      arg.exec_tenant_id_ = clone_job.get_tenant_id();
      arg.if_exist_ = false;
      arg.delay_to_drop_ = false;
      arg.force_drop_ = true;
      arg.drop_only_in_restore_ = true;
      arg.tenant_id_ = clone_tenant_id;

      if (OB_FAIL(deep_copy_ob_string(allocator, clone_tenant_name, arg.tenant_name_))) {
        LOG_WARN("fail to assign", KR(ret), K(clone_job));
      } else if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(timeout).drop_tenant(arg))) {
        if (ret == OB_TENANT_NOT_EXIST) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to drop tenant", KR(ret), K(clone_job), K(arg));
        }
      }
      LOG_INFO("recycle clone tenant", KR(ret), K(clone_job));
    }
    if (OB_SUCC(ret)) {
      obrpc::ObDropResourcePoolArg arg;
      arg.exec_tenant_id_ = clone_job.get_tenant_id();
      arg.pool_id_ = resource_pool_id;
      arg.if_exist_ = true;
      if (OB_FAIL(GCTX.rs_rpc_proxy_->timeout(timeout).drop_resource_pool(arg))) {
        LOG_WARN("drop_resource_pool failed", KR(ret), K(clone_job));
      }
      LOG_INFO("recycle clone resource pool", KR(ret), K(clone_job));
    }
  }

  return ret;
}

int ObTenantCloneUtil::release_source_tenant_resource_of_clone_job(common::ObISQLClient &sql_client,
                                                                   const ObCloneJob &clone_job)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObTenantSnapshotTableOperator table_op;
  const int64_t job_id = clone_job.get_job_id();
  const uint64_t source_tenant_id = clone_job.get_source_tenant_id();
  const ObTenantSnapshotID tenant_snapshot_id = clone_job.get_tenant_snapshot_id();
  const ObTenantCloneJobType job_type = clone_job.get_job_type();
  const ObTenantCloneStatus status = clone_job.get_status();
  bool is_already_unlocked = false;
  bool is_source_tenant_exist = true;
  bool need_notify_tenant_snapshot_scheduler = false;

  share::schema::ObSchemaGetterGuard schema_guard;

  if (!clone_job.get_status().is_sys_release_resource_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("try to release resource of a processing or success job", KR(ret), K(clone_job));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(schema_guard.check_tenant_exist(source_tenant_id, is_source_tenant_exist))) {
    LOG_WARN("get tenant ids failed", K(ret));
  } else if (OB_UNLIKELY(!is_source_tenant_exist)) {
    LOG_INFO("source tenant doesn't exist while release source tenant resource", KR(ret), K(source_tenant_id));
  } else if (OB_FAIL(schema_guard.reset())) {
    LOG_WARN("fail to reset schema guard", KR(ret));
  } else if (OB_FAIL(trans.start(&sql_client, gen_meta_tenant_id(source_tenant_id)))) {
    LOG_WARN("trans start failed", KR(ret), K(clone_job));
  } else if (OB_FAIL(table_op.init(source_tenant_id, &trans))) {
    LOG_WARN("failed to init table op", KR(ret), K(clone_job));
  } else { // is_source_tenant_exist == true
    // release global lock
    ObTenantSnapItem global_lock;
    if (OB_FAIL(table_op.get_tenant_snap_item(
                     ObTenantSnapshotID(ObTenantSnapshotTableOperator::GLOBAL_STATE_ID),
                     true /*for update*/,
                     global_lock))) {
      if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("global lock has not been created", KR(ret), K(clone_job));
        is_already_unlocked = true;
      } else {
        LOG_WARN("fail to get global_lock", KR(ret), K(clone_job));
      }
    } else if (ObTenantSnapStatus::CLONING != global_lock.get_status()) {
      is_already_unlocked = true;
      LOG_INFO("global lock has been released", KR(ret), K(clone_job));
    } else if (OB_FAIL(ObTenantSnapshotUtil::unlock_tenant_snapshot_simulated_mutex_from_clone_release_task(
                                                trans,
                                                source_tenant_id,
                                                job_id,
                                                ObTenantSnapStatus::CLONING,
                                                is_already_unlocked))) {
      LOG_WARN("fail to unlock", KR(ret), K(clone_job), K(global_lock));
    }

    // release snapshot
    // "is_already_unlocked == true" means the release trans of this clone job has been committed,
    // or clone job is failed at the first status (CLONE_SYS_LOCK)
    // thus, no need to handle with the snapshot
    if (OB_SUCC(ret) && !is_already_unlocked) {
      ObTenantSnapItem tenant_snapshot_item;
      ObTenantSnapStatus next_snap_status = ObTenantCloneJobType::RESTORE == job_type ?
                                            ObTenantSnapStatus::NORMAL :
                                            ObTenantSnapStatus::DELETING;
      if (!tenant_snapshot_id.is_valid()) {
        if (ObTenantCloneJobType::FORK == job_type &&
            !status.is_sys_valid_snapshot_status_for_fork()) {
          LOG_INFO("fork tenant snapshot has not been created", K(clone_job));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant snapshot is invalid", KR(ret), K(clone_job));
        }
      } else if (OB_FAIL(ObTenantSnapshotUtil::get_tenant_snapshot_info(trans,
                                                                        source_tenant_id,
                                                                        tenant_snapshot_id,
                                                                        tenant_snapshot_item))) {
        if (OB_TENANT_SNAPSHOT_NOT_EXIST == ret && ObTenantCloneJobType::FORK == job_type) {
          // fork clone job will generate tenant_snapshot_id at first, and then create snapshot.
          // thus, it is possible that job has valid tenant_snapshot_id, but the snapshot doesn't exist
          ret = OB_SUCCESS;
          LOG_INFO("tenant snapshot has not been created", K(clone_job));
        } else {
          LOG_WARN("fail to get tenant snapshot", KR(ret), K(clone_job));
        }
      } else if (tenant_snapshot_item.get_status() == next_snap_status) {
        LOG_INFO("tenant snapshot item already in next status",
            K(clone_job), K(tenant_snapshot_item), K(next_snap_status));
      } else if (OB_FAIL(table_op.update_tenant_snap_item(tenant_snapshot_id,
                                                          tenant_snapshot_item.get_status(),
                                                          next_snap_status))) {
        LOG_WARN("failed to update snapshot status", KR(ret), K(clone_job));
      } else if (ObTenantSnapStatus::DELETING == next_snap_status) {
        ObTenantSnapJobItem job_item(source_tenant_id,
                                     tenant_snapshot_id,
                                     ObTenantSnapOperation::DELETE,
                                     clone_job.get_trace_id());
        if (OB_FAIL(table_op.insert_tenant_snap_job_item(job_item))) {
          LOG_WARN("fail to insert tenant snapshot job", KR(ret), K(job_item));
        } else if (OB_FAIL(ObTenantSnapshotUtil::recycle_tenant_snapshot_ls_replicas(trans, source_tenant_id,
                                                                              clone_job.get_tenant_snapshot_name()))) {
          LOG_WARN("fail to recycle tenant snapshot ls replicas", KR(ret), K(clone_job));
        } else {
          need_notify_tenant_snapshot_scheduler = true;
          LOG_INFO("release source tenant resource", KR(ret), K(clone_job));
        }
      }
    }
  }

  if (trans.is_started()) {
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (OB_SUCC(ret) && need_notify_tenant_snapshot_scheduler) {
    if (OB_TMP_FAIL(ObTenantSnapshotUtil::notify_scheduler(source_tenant_id))) {
      LOG_WARN("notify tenant snapshot scheduler failed", KR(tmp_ret), K(clone_job));
    }
  }

  return ret;
}

int ObTenantCloneUtil::get_clone_job_failed_message(common::ObISQLClient &sql_client,
                                                    const int64_t job_id,
                                                    const uint64_t tenant_id,
                                                    ObIAllocator &allocator,
                                                    ObString &err_msg)
{
  int ret = OB_SUCCESS;
  ObTenantCloneTableOperator clone_op;

  if (OB_UNLIKELY(job_id < 0 || !is_sys_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(job_id), K(tenant_id));
  } else if (OB_FAIL(clone_op.init(tenant_id, &sql_client))) {
    LOG_WARN("fail to init", KR(ret), K(tenant_id));
  } else if (OB_FAIL(clone_op.get_job_failed_message(job_id, allocator, err_msg))) {
    LOG_WARN("fail to get clone job failed message", KR(ret), K(job_id), K(tenant_id));
  }
  return ret;
}

int ObTenantCloneUtil::inner_cancel_clone_job_(
    ObTenantCloneTableOperator &clone_op,
    const ObCloneJob &clone_job,
    const ObCancelCloneJobReason &reason,
    bool &clone_already_finish)
{
  int ret = OB_SUCCESS;
  clone_already_finish = false;
  ObSqlString err_msg;
  const ObTenantCloneStatus next_status(ObTenantCloneStatus::Status::CLONE_SYS_CANCELING);

  if (OB_UNLIKELY(!clone_op.is_inited())
      || OB_UNLIKELY(!clone_job.is_valid())
      || OB_UNLIKELY(!reason.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clone_job), K(reason));
  } else if (clone_job.get_status().is_user_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected sys clone job status", KR(ret), K(clone_job));
  } else if (ObTenantCloneStatus::Status::CLONE_SYS_RELEASE_RESOURCE == clone_job.get_status()
             || !clone_job.get_status().is_sys_processing_status()) {
    clone_already_finish = true;
  } else if (OB_FAIL(clone_op.update_job_status(clone_job.get_job_id(),
                                                clone_job.get_status(), /*old_status*/
                                                next_status))) {
    LOG_WARN("fail to update job status", KR(ret), K(clone_job));
  } else if (OB_FAIL(err_msg.append_fmt("clone job has been canceled in %s status %s",
                                        ObTenantCloneStatus::get_clone_status_str(clone_job.get_status()),
                                        reason.get_reason_str()))) {
    LOG_WARN("fail to construct error message", KR(ret), K(reason), K(clone_job));
  } else if (OB_FAIL(clone_op.update_job_failed_info(clone_job.get_job_id(), OB_CANCELED, err_msg.string()))) {
    LOG_WARN("fail to update job failed info", KR(ret), K(clone_job));
  }
  return ret;
}

void ObTenantCloneUtil::try_to_record_clone_status_change_rs_event(
     const ObCloneJob &clone_job,
     const share::ObTenantCloneStatus &prev_clone_status,
     const share::ObTenantCloneStatus &cur_clone_status,
     const int ret_code,
     const ObCancelCloneJobReason &reason)
{
  int ret = OB_SUCCESS;
  ObSqlString execute_result;
  if (OB_UNLIKELY(!clone_job.is_valid())
      || OB_UNLIKELY(!prev_clone_status.is_valid())
      || OB_UNLIKELY(!cur_clone_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clone_job), K(prev_clone_status), K(cur_clone_status));
  } else if (OB_FAIL(execute_result.assign_fmt("%s(%d)", common::ob_error_name(ret_code), ret_code))) {
    LOG_WARN("fail to build execute result", KR(ret), K(ret_code));
  } else {
    ROOTSERVICE_EVENT_ADD("clone", "change_clone_status",
                          "job_id", clone_job.get_job_id(),
                          K(execute_result), K(prev_clone_status), K(cur_clone_status),
                          reason.is_valid() ? "reason" : "",
                          reason.is_valid() ? reason.get_reason_str() : "");
  }
  LOG_INFO("[CLONE] switch job status", KR(ret), K(clone_job), K(prev_clone_status),
           K(cur_clone_status), K(ret_code), K(reason));
}

int ObTenantCloneUtil::cancel_clone_job_by_source_tenant_id(
    common::ObISQLClient &sql_client,
    const uint64_t source_tenant_id,
    const ObCancelCloneJobReason &reason,
    bool &clone_already_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  clone_already_finish = false;
  ObTenantCloneTableOperator clone_op;
  ObCloneJob clone_job;
  ObMySQLTransaction trans;
  FLOG_INFO("begin to cancel clone job", K(source_tenant_id), K(reason));

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == source_tenant_id)
      || OB_UNLIKELY(!reason.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(source_tenant_id), K(reason));
  } else if (OB_FAIL(trans.start(&sql_client, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &trans))) {
    LOG_WARN("fail init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.get_clone_job_by_source_tenant_id(source_tenant_id, clone_job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get clone job", KR(ret), K(source_tenant_id));
    } else {
      ret = OB_SUCCESS;
      clone_already_finish = true;
      LOG_INFO("clone job has already finished", KR(ret), K(source_tenant_id));
    }
  } else if (OB_FAIL(inner_cancel_clone_job_(clone_op, clone_job, reason, clone_already_finish))) {
    LOG_WARN("fail to cancel clone job", KR(ret), K(clone_job), K(reason));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    (void)try_to_record_clone_status_change_rs_event(
          clone_job, clone_job.get_status(), ObTenantCloneStatus(ObTenantCloneStatus::Status::CLONE_SYS_CANCELING), ret, reason);
  }
  return ret;
}

//This function is called by the user executing "cancel clone" sql.
int ObTenantCloneUtil::cancel_clone_job_by_name(
    common::ObISQLClient &sql_client,
    const ObString &clone_tenant_name,
    bool &clone_already_finish,
    const ObCancelCloneJobReason &reason)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  clone_already_finish = false;
  ObTenantCloneTableOperator clone_op;
  ObCloneJob clone_job;
  ObMySQLTransaction trans;

  if (OB_UNLIKELY(clone_tenant_name.empty()) || OB_UNLIKELY(!reason.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(clone_tenant_name), K(reason));
  } else if (OB_FAIL(trans.start(&sql_client, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else if (OB_FAIL(clone_op.init(OB_SYS_TENANT_ID, &trans))) {
    LOG_WARN("fail init clone op", KR(ret));
  } else if (OB_FAIL(clone_op.get_clone_job_by_clone_tenant_name(
                            clone_tenant_name, true/*need_lock*/, clone_job))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get clone job", KR(ret), K(clone_tenant_name));
    } else {
      ret = OB_SUCCESS;
      clone_already_finish = true;
    }
  } else if (OB_FAIL(inner_cancel_clone_job_(clone_op, clone_job, reason, clone_already_finish))) {
    LOG_WARN("fail to cancel clone job", KR(ret), K(clone_job), K(reason));
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", (OB_SUCCESS == ret), KR(tmp_ret));
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    (void)try_to_record_clone_status_change_rs_event(
              clone_job, clone_job.get_status(), ObTenantCloneStatus(ObTenantCloneStatus::Status::CLONE_SYS_CANCELING), ret, reason);
  }
  return ret;
}
