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

#include "ob_restore_scheduler.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_balance_info.h"
#include "rootserver/ob_alloc_replica_strategy.h"
#include "rootserver/ob_root_inspection.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_upgrade_utils.h"
#include "storage/transaction/ob_i_ts_source.h"
#include "share/backup/ob_backup_struct.h"      // ObPhysicalRestoreInfo
#include "archive/ob_archive_restore_engine.h"  // ObTenantPhysicalRestoreMeta

namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;

int64_t ObRestoreIdling::get_idle_interval_us()
{
  return idle_us_;
}

void ObRestoreIdling::set_idle_interval_us(int64_t idle_us)
{
  idle_us_ = idle_us;
}

void ObRestoreIdling::reset()
{
  idle_us_ = GCONF._restore_idle_time;
}

ObRestoreScheduler::ObRestoreScheduler()
    : inited_(false),
      idling_(stop_),
      schema_service_(NULL),
      sql_proxy_(NULL),
      oracle_sql_proxy_(NULL),
      rpc_proxy_(NULL),
      srv_rpc_proxy_(NULL),
      freeze_info_mgr_(NULL),
      pt_operator_(NULL),
      task_mgr_(NULL),
      server_manager_(NULL),
      zone_mgr_(NULL),
      unit_mgr_(NULL),
      ddl_service_(NULL),
      upgrade_processors_(),
      self_addr_()

{}

ObRestoreScheduler::~ObRestoreScheduler()
{
  if (!stop_) {
    stop();
    wait();
  }
}

int ObRestoreScheduler::init(ObMultiVersionSchemaService &schema_service, ObMySQLProxy &sql_proxy,
    ObOracleSqlProxy &oracle_sql_proxy, ObCommonRpcProxy &rpc_proxy, obrpc::ObSrvRpcProxy &srv_rpc_proxy,
    ObFreezeInfoManager &freeze_info_mgr, ObPartitionTableOperator &pt_operator, ObRebalanceTaskMgr &task_mgr,
    ObServerManager &server_manager, ObZoneManager &zone_manager, ObUnitManager &unit_manager,
    ObDDLService &ddl_service, const common::ObAddr &self_addr)
{
  int ret = OB_SUCCESS;
  const int restore_scheduler_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(create(restore_scheduler_thread_cnt, "RestoreSche"))) {
    LOG_WARN("create thread failed", KR(ret), K(restore_scheduler_thread_cnt));
  } else if (OB_FAIL(upgrade_processors_.init(ObBaseUpgradeProcessor::UPGRADE_MODE_PHYSICAL_RESTORE,
                 sql_proxy,
                 srv_rpc_proxy,
                 schema_service,
                 *this))) {
    LOG_WARN("fail to init upgrade processors", KR(ret));
  } else {
    schema_service_ = &schema_service;
    sql_proxy_ = &sql_proxy;
    oracle_sql_proxy_ = &oracle_sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    srv_rpc_proxy_ = &srv_rpc_proxy;
    freeze_info_mgr_ = &freeze_info_mgr;
    pt_operator_ = &pt_operator;
    task_mgr_ = &task_mgr;
    server_manager_ = &server_manager;
    zone_mgr_ = &zone_manager;
    unit_mgr_ = &unit_manager;
    ddl_service_ = &ddl_service;
    self_addr_ = self_addr;
    inited_ = true;
  }
  return ret;
}

int ObRestoreScheduler::idle()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(idling_.idle())) {
    LOG_WARN("idle failed", K(ret));
  } else {
    idling_.reset();
  }
  return ret;
}

void ObRestoreScheduler::wakeup()
{
  if (!inited_) {
    LOG_WARN("not init");
  } else {
    idling_.wakeup();
  }
}

void ObRestoreScheduler::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    idling_.wakeup();
  }
}

void ObRestoreScheduler::run3()
{
  LOG_INFO("[RESTORE] restore scheduler start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    // avoid using default idle time when observer restarts.
    idling_.reset();
    while (!stop_) {
      ObCurTraceId::init(GCTX.self_addr_);
      LOG_INFO("[RESTORE] try process restore job");
      typedef ObSEArray<ObPhysicalRestoreJob, 10> T;
      HEAP_VAR(T, job_infos)
      {
        ObPhysicalRestoreTableOperator restore_op;
        if (OB_FAIL(restore_op.init(sql_proxy_))) {
          LOG_WARN("fail init", K(ret));
        } else if (OB_FAIL(restore_op.get_jobs(job_infos))) {
          LOG_WARN("fail to get jobs", K(ret));
        } else {
          FOREACH_CNT_X(job_info, job_infos, !stop_)
          {  // ignore ret
            if (OB_ISNULL(job_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("job info is null", K(ret));
            } else if (OB_FAIL(process_restore_job(*job_info))) {
              LOG_WARN("fail to process restore job", K(ret), KPC(job_info));
            }
          }
        }
      }
      // retry until stopped, reset ret to OB_SUCCESS
      ret = OB_SUCCESS;
      idle();
    }
  }
  LOG_INFO("[RESTORE] restore scheduler quit");
  return;
}

int ObRestoreScheduler::process_restore_job(const ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    switch (job.status_) {
      case PHYSICAL_RESTORE_CREATE_TENANT:
        ret = restore_tenant(job);
        break;
      case PHYSICAL_RESTORE_SYS_REPLICA:
        ret = restore_sys_replica(job);
        break;
      case PHYSICAL_RESTORE_UPGRADE_PRE:
        ret = upgrade_pre(job);
        break;
      case PHYSICAL_RESTORE_UPGRADE_POST:
        ret = upgrade_post(job);
        break;
      case PHYSICAL_RESTORE_MODIFY_SCHEMA:
        ret = modify_schema(job);
        break;
      case PHYSICAL_RESTORE_CREATE_USER_PARTITIONS:
        ret = create_user_partitions(job);
        break;
      case PHYSICAL_RESTORE_USER_REPLICA:
        ret = restore_user_replica(job);
        break;
      case PHYSICAL_RESTORE_REBUILD_INDEX:
        ret = rebuild_index(job);
        break;
      case PHYSICAL_RESTORE_POST_CHECK:
        ret = post_check(job);
        break;
      case PHYSICAL_RESTORE_SUCCESS:
        ret = restore_success(job);
        break;
      case PHYSICAL_RESTORE_FAIL:
        ret = restore_fail(job);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status not match", K(ret), K(job));
        break;
    }
    if (PHYSICAL_RESTORE_FAIL != job.status_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_recycle_job(job))) {
        LOG_WARN("fail to recycle job", K(tmp_ret), K(job));
      }
    }
    LOG_INFO("[RESTORE] doing restore", K(ret), K(job));
  }
  return ret;
}

// restore_tenant is not reentrant
int ObRestoreScheduler::restore_tenant(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObCreateTenantArg arg;
  UInt64 tenant_id = OB_INVALID_TENANT_ID;
  ObPhysicalRestoreJob new_job;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_TENANT);
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
  const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // default time is 2s
  const int64_t PARTITION_CNT_PER_RPC = 5;
  const int64_t sys_table_num = ObSysTableChecker::instance().get_tenant_space_sys_table_num();
  int64_t timeout = (sys_table_num / PARTITION_CNT_PER_RPC) * TIMEOUT_PER_RPC;
  timeout = max(timeout, DEFAULT_TIMEOUT);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_INVALID_TENANT_ID != job_info.tenant_id_) {
    // restore_tenant can only be executed once.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore tenant already exist", K(ret), K(job_info));
  } else if (OB_FAIL(new_job.assign(job_info))) {
    LOG_WARN("fail to assign job", K(ret), K(job_info));
  } else if (OB_FAIL(fill_job_info(new_job, arg))) {
    LOG_WARN("fail to fill job info", K(ret), K(new_job));
  } else if (OB_FAIL(fill_create_tenant_arg(new_job, arg))) {
    LOG_WARN("fail to fill create tenant arg", K(ret), K(new_job));
  } else if (OB_FAIL(update_sys_table_schema_version_())) {
    LOG_WARN("fail to update sys table schema version", KR(ret));
  } else if (OB_FAIL(rpc_proxy_->timeout(timeout).create_tenant(arg, tenant_id))) {
    LOG_WARN("fail to create tenant", K(ret), K(arg));
  } else {
    new_job.tenant_id_ = tenant_id;
    new_job.status_ = PHYSICAL_RESTORE_SYS_REPLICA;
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(sql_proxy_))) {
      LOG_WARN("fail init", K(ret));
    } else if (OB_FAIL(restore_op.init_restore_progress(new_job))) {
      LOG_WARN("fail to init restore progress", K(ret), K(new_job));
    } else if (OB_FAIL(restore_op.replace_job(new_job))) {
      LOG_WARN("fail insert job and partitions", K(ret), K(new_job));
    } else {
      idling_.set_idle_interval_us(0);  // wakeup immediately
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
    }
  } else {
    (void)record_rs_event(job_info, PHYSICAL_RESTORE_SYS_REPLICA);
  }
  LOG_INFO("[RESTORE] restore tenant", K(ret), K(arg), K(job_info), K(new_job));
  return ret;
}

int ObRestoreScheduler::update_sys_table_schema_version_()
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
  const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // default time is 2s
  const int64_t PARTITION_CNT_PER_RPC = 5;
  const int64_t sys_table_num = ObSysTableChecker::instance().get_tenant_space_sys_table_num();
  int64_t timeout = (sys_table_num / PARTITION_CNT_PER_RPC) * TIMEOUT_PER_RPC;
  timeout = max(timeout, DEFAULT_TIMEOUT);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    ObUpdateTableSchemaVersionArg arg;
    // make other member is invalid to differ from other rpc
    arg.init(OB_INVALID_TENANT_ID, /* tenant_id */
        OB_INVALID_ID,             /* table_id  */
        OB_INVALID_VERSION,        /* schema_version */
        false,                     /* is_replay_schema */
        ObUpdateTableSchemaVersionArg::UPDATE_SYS_TABLE_IN_TENANT_SPACE);
    if (OB_FAIL(rpc_proxy_->timeout(timeout).update_table_schema_version(arg))) {
      LOG_WARN("fail to update table schema version", KR(ret), K(timeout), K(arg));
    }
  }
  return ret;
}

int ObRestoreScheduler::fill_job_info(ObPhysicalRestoreJob &job, ObCreateTenantArg &arg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(fill_backup_info(job, arg))) {
    LOG_WARN("fail to fill backup info", K(ret), K(job));
  } else if (OB_FAIL(fill_rs_info(job))) {
    LOG_WARN("fail to fill rs info", K(ret), K(job));
  }
  return ret;
}
int ObRestoreScheduler::fill_create_tenant_arg(const ObPhysicalRestoreJob &job, ObCreateTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else {
    /*
     * restore_tenant will only run trans one when create tenant.
     * Consider the following tenant options:
     * 1) need backup: tenant_name,compatibility_mode
     * 2) need backup and replace(maybe): zone_list,primary_zone,locality,previous_locality
     * 3) not backup yet:locked,default_tablegroup_id,info  TODO: ()
     * 4) no need to backup:drop_tenant_time,status,collation_type
     * 6) abandoned: replica_num,read_only,rewrite_merge_version,logonly_replica_num,
     *                storage_format_version,storage_format_work_version
     */
    ObCompatibilityMode mode = lib::Worker::CompatMode::ORACLE == job.compat_mode_ ? ObCompatibilityMode::ORACLE_MODE
                                                                                   : ObCompatibilityMode::MYSQL_MODE;
    arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
    arg.tenant_schema_.set_tenant_name(job.tenant_name_);
    arg.tenant_schema_.set_compatibility_mode(mode);
    arg.if_not_exist_ = false;
    arg.is_restore_ = true;
    arg.restore_frozen_status_.frozen_version_ = job.restore_data_version_;
    arg.restore_frozen_status_.frozen_timestamp_ = job.frozen_snapshot_version_;
    if (OB_FAIL(assign_pool_list(job.pool_list_, arg.pool_list_))) {
      LOG_WARN("fail to get pool list", K(ret), K(job));
    }
    ObTenantSchema &tenant_schema = arg.tenant_schema_;
    // check locality
    if (OB_SUCC(ret)) {
      bool specific_locality = 0 != strlen(job.locality_);
      if (specific_locality) {
        tenant_schema.set_locality(ObString::make_string(job.locality_));
      } else {
        tenant_schema.set_locality(ObString::make_string(job.backup_locality_));
      }
      // check & parser locality
      int tmp_ret = OB_SUCCESS;
      ObArray<share::ObZoneReplicaAttrSet> locality;
      if (OB_SUCCESS !=
          (tmp_ret = ddl_service_->check_create_tenant_locality(arg.pool_list_, tenant_schema, schema_guard))) {
        LOG_WARN("locality not match", K(tmp_ret), K(arg));
      } else if (OB_SUCCESS != (tmp_ret = tenant_schema.get_zone_replica_attr_array(locality))) {
        LOG_WARN("fail to get locality array", K(ret), K(tenant_schema));
      } else if (OB_SUCCESS != (tmp_ret = check_locality_valid(locality))) {
        LOG_WARN(
            "locality not supported", K(tmp_ret), "locality", job.locality_, "backup_locality", job.backup_locality_);
      }
      if (OB_SUCCESS == tmp_ret) {
      } else if (specific_locality) {
        ret = tmp_ret;
        LOG_WARN("invalid locality", K(ret), "locality", job.locality_);
      } else {
        tenant_schema.set_locality(ObString(""));
        LOG_INFO("backup locality not match, just reset", K(ret), "backup_locality", job.backup_locality_);
      }
    }
    // check primary_zone
    if (OB_FAIL(ret)) {
    } else if (0 != strlen(job.primary_zone_)) {
      // specific primary_zone
      tenant_schema.set_primary_zone(ObString::make_string(job.primary_zone_));
    } else {
      // use primary_zone from oss
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = ddl_service_->check_create_tenant_replica_options(tenant_schema, schema_guard))) {
        LOG_WARN("fail to check schema primary_zone", K(ret), K(arg));
      }
      if (OB_SUCCESS != tmp_ret) {
        LOG_INFO("backup primary_zone not match, just reset", K(tmp_ret), K(arg));
      } else {
        tenant_schema.set_primary_zone(ObString::make_string(job.backup_primary_zone_));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::assign_pool_list(const char *str, common::ObIArray<ObString> &pool_list)
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  while (OB_SUCC(ret)) {
    item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ",", &save_ptr);
    if (NULL != item_str) {
      if (OB_FAIL(pool_list.push_back(ObString::make_string(item_str)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObRestoreScheduler::fill_restore_backup_info_param(
    share::ObPhysicalRestoreJob &job, share::ObRestoreBackupInfoUtil::GetRestoreBackupInfoParam &param)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreBackupDestList &dest_list = job.multi_restore_path_list_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(param.backup_set_path_list_.assign(dest_list.get_backup_set_path_list()))) {
    LOG_WARN("failed to assign backup set path list", KR(ret), K(job));
  } else if (OB_FAIL(param.backup_piece_path_list_.assign(dest_list.get_backup_piece_path_list()))) {
    LOG_WARN("failed to assign backup piece path list", KR(ret), K(job));
  } else {
    param.backup_dest_ = job.backup_dest_;
    param.backup_cluster_name_ = job.backup_cluster_name_;
    param.cluster_id_ = job.cluster_id_;
    param.incarnation_ = job.incarnation_;
    param.backup_tenant_name_ = job.backup_tenant_name_;
    param.restore_timestamp_ = job.restore_timestamp_;
    param.passwd_array_ = job.passwd_array_;
  }
  return ret;
}

int ObRestoreScheduler::fill_backup_info(ObPhysicalRestoreJob &job, ObCreateTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObRestoreBackupInfo backup_info;
  ObRestoreBackupInfoUtil::GetRestoreBackupInfoParam param;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(fill_restore_backup_info_param(job, param))) {
    LOG_WARN("fail to fill restore backup info param", KR(ret), K(job));
  } else if (OB_FAIL(ObRestoreBackupInfoUtil::get_restore_backup_info(param, backup_info))) {
    LOG_WARN("fail to get backup info", KR(ret), K(job));
  } else if (OB_FAIL(check_source_cluster_version(backup_info.physical_restore_info_.cluster_version_))) {
    LOG_WARN("fail to check source cluster version", KR(ret), K(job));
  } else if (OB_FAIL(job.assign(backup_info))) {
    LOG_WARN("fail to assign backup info", KR(ret), K(job), K(backup_info));
  }
  if (OB_SUCC(ret)) {
    // fill restore_pkeys/restore_log_pkeys
    if (OB_FAIL(arg.restore_pkeys_.assign(backup_info.sys_pg_key_list_))) {
      LOG_WARN("fail to assign pkeys", KR(ret), K(job));
    } else if (backup_info.snapshot_version_ == param.restore_timestamp_ &&
               backup_info.physical_restore_info_.cluster_version_ > CLUSTER_VERSION_3000) {
      LOG_INFO("backup snapshot equal to restore timestamp, no need get pkeys from log",
          K(backup_info), K(param.restore_timestamp_));
    } else if (OB_FAIL(fill_pkeys_for_physical_restore_log(job, arg))) {
      LOG_WARN("fail to fill pkeys for physical restore log", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObRestoreScheduler::fill_pkeys_for_physical_restore_log(const ObPhysicalRestoreJob &job, ObCreateTenantArg &arg)
{
  int ret = OB_SUCCESS;
  common::hash::ObHashSet<uint64_t> restore_pure_ids;
  int64_t tenant_space_tables_cnt = ARRAYSIZEOF(tenant_space_tables);
  if (OB_FAIL(restore_pure_ids.create(hash::cal_next_prime(tenant_space_tables_cnt), "ResPureIds", "ResPureIds"))) {
    LOG_WARN("failed to create restore pure ids", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.restore_pkeys_.count(); i++) {
    const uint64_t pure_id = extract_pure_id(arg.restore_pkeys_.at(i).get_table_id());
    if (OB_FAIL(restore_pure_ids.set_refactored(pure_id, 0 /* won't overwrite */))) {
      LOG_WARN("fail to set pure_id", KR(ret), K(pure_id));
    }
  }
  share::ObPhysicalRestoreInfo restore_info;
  archive::ObTenantPhysicalRestoreMeta restore_meta;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(job.copy_to(restore_info))) {
    LOG_WARN("fail to gen restore info", KR(ret), K(job));
  } else if (OB_FAIL(restore_meta.init(restore_info))) {
    LOG_WARN("fail to init restore meta", KR(ret), K(job));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    const uint64_t tid = extract_pure_id(tenant_space_tables[i]);
    if (is_inner_table_with_partition(tid) && !ObSysTableChecker::is_backup_private_tenant_table(tid)) {
      int hash_ret = restore_pure_ids.exist_refactored(tid);
      if (OB_HASH_EXIST == hash_ret) {
        // skip
      } else if (OB_HASH_NOT_EXIST == hash_ret) {
        // check if partition has archived log
        const uint64_t table_id = combine_id(job.backup_tenant_id_, tid);
        common::ObPartitionKey pkey(table_id, 0, 0);
        bool exist = false;
        if (OB_FAIL(restore_meta.check_need_fetch_archived_log(pkey, exist))) {
          LOG_WARN("fail to check if archived log exist", KR(ret), K(pkey));
        } else if (exist && OB_FAIL(arg.restore_log_pkeys_.push_back(pkey))) {
          LOG_WARN("fail to push back pkey", KR(ret), K(pkey));
        } else {
          // partition has no archived log
        }
      } else {
        ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
        LOG_WARN("fail to check pure id exist", KR(ret), K(tid));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::fill_rs_info(ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  ObRestoreBackupInfo backup_info;
  share::ObSimpleFrozenStatus frozen_status;
  int64_t frozen_version = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(freeze_info_mgr_->get_freeze_info(frozen_version, frozen_status))) {
    LOG_WARN("fail to get freeze info", K(ret), K(frozen_status));
  } else {
    /* fill job */
    job.restore_data_version_ = frozen_status.frozen_version_;
  }
  return ret;
}

int ObRestoreScheduler::restore_sys_replica(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_SYS_REPLICA);
    bool sys_only = true;
    ObPhysicalRestoreStat stat(*schema_service_, *sql_proxy_, *pt_operator_, job_info, sys_only, stop_);
    int64_t task_cnt = 0;
    if (OB_FAIL(stat.gather_stat())) {
      LOG_WARN("fail to gather stat", K(ret), K(job_info));
    } else if (OB_FAIL(schedule_restore_task(job_info, stat, task_cnt))) {
      LOG_WARN("fail to schedule restore task", K(ret), K(job_info));
    } else if (0 == task_cnt) {
      if (OB_FAIL(set_member_list(job_info, stat))) {
        LOG_WARN("fail to set member_list", K(ret), K(job_info));
      } else if (OB_FAIL(update_restore_schema_version(job_info))) {
        LOG_WARN("fail to update restore schema version", KR(ret), K(job_info));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
          LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
        }
      }
    }
  }
  LOG_INFO("[RESTORE] restore sys replica", K(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::update_restore_schema_version(const ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = job.tenant_id_;
  int64_t schema_version = OB_INVALID_VERSION;
  ObPhysicalRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(restore_op.init(sql_proxy_))) {
    LOG_WARN("fail init", KR(ret));
  } else if (OB_FAIL(schema_service_->get_schema_version_in_inner_table(*sql_proxy_, schema_status, schema_version))) {
    LOG_WARN("fail to get schema version from inner table", KR(ret), K(schema_status));
  } else if (OB_FAIL(restore_op.update_restore_option(job.job_id_, "restore_schema_version", schema_version))) {
    LOG_WARN("fail to update restore option", KR(ret), K(schema_status), K(schema_version));
  }
  return ret;
}

int ObRestoreScheduler::schedule_restore_task(
    const ObPhysicalRestoreJob &job_info, ObPhysicalRestoreStat &stat, int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_REPLICA);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    int bret = OB_SUCCESS;
    FOREACH_CNT_X(partition, stat.partitions_, OB_SUCC(ret))
    {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (task_cnt >= MAX_RESTORE_TASK_CNT) {
        break;  // do it next round
      } else if (OB_SUCCESS != (tmp_ret = schedule_restore_task(job_info, stat, partition, task_cnt))) {
        LOG_WARN("fail to schedule restore task", K(tmp_ret), K(job_info), KPC(partition));
        bret = OB_SUCCESS == bret ? tmp_ret : bret;
      }
    }
    {
      // always update __all_restore_progress
      int tmp_ret = OB_SUCCESS;
      ObPhysicalRestoreTableOperator restore_op;
      if (OB_SUCCESS != (tmp_ret = restore_op.init(sql_proxy_))) {
        LOG_WARN("fail init", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = restore_op.update_restore_progress(job_info, stat.statistic_))) {
        LOG_WARN("fail to update progress", K(tmp_ret), K(job_info), "statistic", stat.statistic_);
      } else {
        LOG_INFO("update progress success", "statistic", stat.statistic_);
      }
    }
    ret = OB_SUCC(ret) ? bret : ret;
  }
  return ret;
}

int ObRestoreScheduler::schedule_restore_task(const ObPhysicalRestoreJob &job_info, ObPhysicalRestoreStat &stat,
    const PhysicalRestorePartition *partition, int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition is null", K(ret));
  } else if (partition->get_replica_count() <= 0) {
    ret = OB_EAGAIN;
    LOG_WARN("partition has no replica", K(ret), KPC(partition));
  } else {
    int64_t paxos_restore_cnt = 0;
    int64_t need_restore_cnt = 0;
    FOR_BEGIN_END_E(r, *partition, stat.replicas_, OB_SUCC(ret))
    {
      if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is null", K(ret), KPC(partition));
      }
      if (OB_SUCC(ret)) {
        // check paxos already restored replica cnt
        if (!r->need_schedule_restore_task() && ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
          paxos_restore_cnt++;
        }
        // check need restore replica cnt
        if (r->need_schedule_restore_task()) {
          need_restore_cnt++;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (0 == need_restore_cnt && paxos_restore_cnt >= partition->schema_paxos_cnt_) {
      // 1.paxos_restore_cnt is enough
      // 2.no replica need restore
      stat.statistic_.finish_pg_cnt_ += 1;
      stat.statistic_.finish_partition_cnt_ += partition->table_cnt_;
    } else {
      const ObPartitionReplica *restore_replica = NULL;
      FOR_BEGIN_END_E(r, *partition, stat.replicas_, OB_SUCC(ret))
      {
        if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica is null", K(ret), KPC(partition));
        } else if (REPLICA_RESTORE_DATA == r->is_restore_ || REPLICA_RESTORE_CUT_DATA == r->is_restore_ ||
                   REPLICA_RESTORE_ARCHIVE_DATA == r->is_restore_) {
          // leader first, follower restore should choose leader
          if (r->is_leader_like()) {
            restore_replica = r;
          } else if (OB_ISNULL(restore_replica)) {
            restore_replica = r;
          }
        }
      }
      if (OB_ISNULL(restore_replica)) {
        // 1.wait restore clog
        // 2.paxos replica not enough
        ret = OB_EAGAIN;
        LOG_WARN("partition still restore clog or replica num is not enough, "
                 "check it next round",
            K(ret),
            KPC(partition));
      } else if (task_cnt >= MAX_RESTORE_TASK_CNT) {
        ret = OB_EAGAIN;
        LOG_WARN("once schedule task cnt reach limit, do it next round", K(ret), K(task_cnt));
      } else {
        if (restore_replica->is_leader_like()) {
          if (OB_FAIL(schedule_leader_restore_task(*partition, *restore_replica, job_info, stat, task_cnt))) {
            LOG_WARN("fail to schedule leader restore task", K(ret), KPC(partition), K(restore_replica));
          }
        } else {
          if (OB_FAIL(schedule_follower_restore_task(*partition, *restore_replica, job_info, stat, task_cnt))) {
            LOG_WARN("fail to schedule leader restore task", K(ret), KPC(partition), K(restore_replica));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::schedule_leader_restore_task(const PhysicalRestorePartition &partition,
    const ObPartitionReplica &replica, const ObPhysicalRestoreJob &job_info, const ObPhysicalRestoreStat &stat,
    int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  UNUSED(stat);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    ObPartitionKey pkey = partition.get_key();
    ObPhysicalRestoreArg restore_arg;
    ObPhysicalRestoreTask task;
    ObPhysicalRestoreTaskInfo task_info;
    common::ObArray<ObPhysicalRestoreTaskInfo> task_info_array;
    const char *comment = balancer::PHYSICAL_RESTORE_REPLICA;
    int64_t tmp_cnt = 0;
    // fill dst
    OnlineReplica dst;
    dst.unit_id_ = replica.unit_id_;
    dst.zone_ = replica.zone_;
    dst.member_ = ObReplicaMember(
        replica.server_, replica.member_time_us_, replica.replica_type_, replica.get_memstore_percent());
    if (OB_FAIL(fill_restore_arg(pkey, job_info, restore_arg))) {
      LOG_WARN("fail to assign restore arg", K(ret), K(pkey), K(job_info));
    } else if (OB_FAIL(task_info.build(pkey, restore_arg, dst))) {
      LOG_WARN("fail build restore task", K(ret), K(pkey));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail push task info", K(pkey), K(ret));
    } else if (OB_FAIL(task.build(task_info_array, dst.get_server(), comment))) {
      LOG_WARN("fail build restore task", K(pkey), K(ret));
    } else if (OB_FAIL(task_mgr_->add_task(task, tmp_cnt))) {
      LOG_WARN("fail add task to task_mgr", K(ret), K(task));
    } else {
      task_cnt++;
    }
  }
  return ret;
}

int ObRestoreScheduler::schedule_follower_restore_task(const PhysicalRestorePartition &partition,
    const ObPartitionReplica &replica, const ObPhysicalRestoreJob &job_info, const ObPhysicalRestoreStat &stat,
    int64_t &task_cnt)
{
  int ret = OB_SUCCESS;
  UNUSED(job_info);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    ObPartitionKey pkey = partition.get_key();
    ObCopySSTableTask task;
    ObCopySSTableTaskInfo task_info;
    common::ObArray<ObCopySSTableTaskInfo> task_info_array;
    OnlineReplica dst;
    const char *comment = balancer::RESTORE_FOLLOWER_REPLICA;
    const ObCopySSTableType type = OB_COPY_SSTABLE_TYPE_RESTORE_FOLLOWER;
    ObReplicaMember data_src;
    int64_t tmp_cnt = 0;
    // fill dst
    dst.unit_id_ = replica.unit_id_;
    dst.zone_ = replica.zone_;
    dst.member_ = ObReplicaMember(
        replica.server_, replica.member_time_us_, replica.replica_type_, replica.get_memstore_percent());
    if (OB_FAIL(choose_restore_data_source(partition, stat, data_src))) {
      LOG_WARN("fail to fill restore data source", K(ret), K(partition), K(replica));
    } else if (OB_FAIL(task_info.build(pkey, dst, data_src))) {
      LOG_WARN("fail build restore task", K(ret), K(pkey));
    } else if (OB_FAIL(task_info_array.push_back(task_info))) {
      LOG_WARN("fail push task info", K(pkey), K(ret));
    } else if (OB_FAIL(task.build(task_info_array, type, dst.member_.get_server(), comment))) {
      LOG_WARN("fail build restore task", K(pkey), K(ret));
    } else if (OB_FAIL(task_mgr_->add_task(task, tmp_cnt))) {
      LOG_WARN("fail add task to task_mgr", K(ret), K(task));
    } else {
      task_cnt++;
    }
  }
  return ret;
}

int ObRestoreScheduler::fill_restore_arg(
    const common::ObPartitionKey &pg_key, const ObPhysicalRestoreJob &job, ObPhysicalRestoreArg &arg)
{
  int ret = OB_SUCCESS;
  arg.pg_key_ = pg_key;
  arg.restore_data_version_ = job.restore_data_version_;
  // fill restore_info
  if (OB_FAIL(job.copy_to(arg.restore_info_))) {
    LOG_WARN("fail to fill restore info", K(ret), K(job));
  }
  return ret;
}

int ObRestoreScheduler::choose_restore_data_source(
    const PhysicalRestorePartition &partition, const ObPhysicalRestoreStat &stat, ObReplicaMember &data_src)
{
  int ret = OB_SUCCESS;
  bool found = false;
  FOR_BEGIN_END_E(r, partition, stat.replicas_, OB_SUCC(ret) && !found)
  {
    if (OB_ISNULL(r)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replica is null", K(ret), K(partition));
    } else if (!r->need_schedule_restore_task() && REPLICA_TYPE_FULL == r->replica_type_) {
      data_src = ObReplicaMember(r->server_, r->member_time_us_, r->replica_type_, r->get_memstore_percent());
      found = true;
    }
  }
  if (!found && OB_SUCC(ret)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find available data source, maybe leader is still restoring", KR(ret), K(partition));
  }
  return ret;
}

int ObRestoreScheduler::set_member_list(const ObPhysicalRestoreJob &job_info, const ObPhysicalRestoreStat &stat)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  const int64_t bucket_num = 1024;
  common::hash::ObHashMap<ObPartitionKey, ObPartitionReplica::MemberList> member_list_map;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_SET_MEMBER_LIST);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(member_list_map.create(
                 bucket_num, ObModIds::OB_RESTORE_SET_MEMBER_LIST, ObModIds::OB_RESTORE_SET_MEMBER_LIST))) {
    LOG_WARN("fail to create hashmap", K(ret), K(tenant_id));
  } else if (OB_FAIL(build_member_list_map(tenant_id, member_list_map))) {
    LOG_WARN("fail to build member_list_map", K(ret), K(tenant_id));
  } else {
    bool all_done = true;
    ObArray<const PhysicalRestorePartition *> persist_partitions;
    ObRecoveryHelper::ObMemberListPkeyList partition_infos;
    SetMemberListAction action = SetMemberListAction::BALANCE;
    FOREACH_CNT_X(partition, stat.partitions_, OB_SUCC(ret))
    {
      int tmp_ret = OB_SUCCESS;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition is null", K(ret));
      } else if (OB_FAIL(get_set_member_list_action(*partition, stat, action))) {
        LOG_WARN("fail to check partition", K(ret), KPC(partition));
      } else if (SetMemberListAction::DONE == action) {
        LOG_DEBUG("partition no need to set member_list", K(ret), KPC(partition));
      } else if (SetMemberListAction::BALANCE == action) {
        LOG_INFO("partition need balance", K(ret), KPC(partition));
      } else if (SetMemberListAction::SET_MEMBER_LIST == action) {
        ObPartitionKey pkey(partition->schema_id_, partition->partition_id_, 0);
        ObPartitionReplica::MemberList member_list;
        ret = member_list_map.get_refactored(pkey, member_list);
        if (OB_SUCC(ret)) {
          // get member_list from __all_tenant_member_list
          if (OB_FAIL(add_member_list_pkey(pkey, member_list, partition_infos))) {
            LOG_WARN("fail to add member list pkey", K(ret), K(pkey), K(member_list));
          }
        } else if (OB_HASH_NOT_EXIST == ret) {  // overwrite ret
          // get member_list from meta table
          if (OB_FAIL(add_member_list_pkey(*partition, stat, partition_infos))) {
            LOG_WARN("fail to add member list pkey", K(ret), KPC(partition));
          } else if (OB_FAIL(persist_partitions.push_back(partition))) {
            LOG_WARN("fail to push back partition", K(ret), KPC(partition));
          }
        } else {
          LOG_WARN("fail to get member_list from map", K(ret), K(pkey));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid action", K(ret), K(action), KPC(partition));
      }
      if (OB_SUCC(ret) && all_done) {
        all_done = SetMemberListAction::DONE == action;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(batch_persist_member_list(persist_partitions, stat))) {
        LOG_WARN("fail to persistent member list", K(ret), "cnt", persist_partitions.count());
      } else if (OB_FAIL(batch_set_member_list(partition_infos))) {
        LOG_WARN("fail to batch set member_list",
            K(ret),
            "pkey_cnt",
            partition_infos.pkey_array_.count(),
            "ml_pk_cnt",
            partition_infos.ml_pk_array_.count(),
            "epoch",
            partition_infos.epoch_);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!all_done) {
      ret = OB_EAGAIN;
      LOG_INFO("check set_member_list next round",
          K(ret),
          "persist_cnt",
          persist_partitions.count(),
          "pkey_cnt",
          partition_infos.pkey_array_.count(),
          "ml_pk_cnt",
          partition_infos.ml_pk_array_.count());
      idling_.set_idle_interval_us(10 * 1000 * 1000);  // wakeup
    } else {
      if (OB_FAIL(clear_member_list_table(tenant_id))) {
        LOG_WARN("fail to clear __all_partition_member_list", K(ret), K(tenant_id));
      }
    }
  }
  LOG_INFO("finish batch set member_list", K(ret), K(tenant_id), K(job_info));
  return ret;
}

// To simplify logic, when replicas match locality, we thought set_member_list() should be run.
// There are three values of SetMemberListAction:
// 1. BALANCE: replicas match locality.
// 2. DONE: replicas match locality, and all replicas' is_restore = REPLICA_NOT_RESTORE.
// 3. SET_MEMBER_LIST: replicas match locality, and all replicas' is_restore =
// REPLICA_RESTORE_WAIT_ALL_DUMPED/REPLICA_RESTORE_MEMBER_LIST.
int ObRestoreScheduler::get_set_member_list_action(
    const PhysicalRestorePartition &partition, const ObPhysicalRestoreStat &stat, SetMemberListAction &action)
{
  int ret = OB_SUCCESS;
  bool locality_match = false;
  action = SetMemberListAction::BALANCE;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (partition.get_replica_count() <= 0) {
    LOG_INFO("partition has no replica", K(ret), K(partition));
  } else if (OB_FAIL(check_locality_match(partition, stat, locality_match))) {
    LOG_WARN("fail to check locality match", K(ret), K(partition));
  } else if (!locality_match) {
    LOG_DEBUG("locality not match, need balance", K(ret), K(partition));
  } else {
    action = SetMemberListAction::DONE;
    FOR_BEGIN_END_E(r, partition, stat.replicas_, OB_SUCC(ret))
    {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is null", K(ret), K(partition));
      } else if (!ObReplicaTypeCheck::is_paxos_replica_V2(r->replica_type_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("non paxos replica set member list is not supported", K(ret), KPC(r));
      } else if (r->need_schedule_restore_task()) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("replica restore status not match", K(ret), KPC(r));
      } else if (REPLICA_NOT_RESTORE == r->is_restore_ && (r->member_list_.count() <= 0 || r->is_restore_leader())) {
        // check replica normal
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is invalid", K(ret), KPC(r));
      } else if (REPLICA_NOT_RESTORE == r->is_restore_) {
        // pass
      } else if (REPLICA_RESTORE_WAIT_ALL_DUMPED == r->is_restore_ || REPLICA_RESTORE_MEMBER_LIST == r->is_restore_) {
        action = SetMemberListAction::SET_MEMBER_LIST;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected case", K(ret), KPC(r));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::check_locality_valid(const share::schema::ZoneLocalityIArray &locality)
{
  int ret = OB_SUCCESS;
  int64_t cnt = locality.count();
  if (cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cnt", K(ret), K(cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
      const share::ObZoneReplicaAttrSet &attr = locality.at(i);
      if (attr.is_specific_readonly_replica() || attr.is_allserver_readonly_replica()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("locality with readonly replica is not supported", K(ret), K(locality));
      } else if (attr.is_mixed_locality()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("mixed locality is not supported", K(ret), K(locality));
      } else if (attr.is_specific_replica_attr()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("locality with memstore_percent is not supported", K(ret), K(locality));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::check_locality_match(
    const PhysicalRestorePartition &partition, const ObPhysicalRestoreStat &stat, bool &locality_match)
{
  int ret = OB_SUCCESS;
  locality_match = false;
  uint64_t tenant_id = extract_tenant_id(partition.schema_id_);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    // mock partition_info
    ObPartitionInfo partition_info;
    if (OB_SUCC(ret)) {
      partition_info.set_table_id(partition.schema_id_);
      partition_info.set_partition_id(partition.partition_id_);
      FOR_BEGIN_END_E(r, partition, stat.replicas_, OB_SUCC(ret))
      {
        if (OB_FAIL(check_stop())) {
          LOG_WARN("restore scheduler stopped", K(ret));
        } else if (OB_ISNULL(r)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replica is null", K(ret), K(partition));
        } else if (OB_FAIL(partition_info.add_replica(*r))) {
          LOG_WARN("fail to add replica", K(ret), KPC(r));
        }
      }
    }
    // get locality & zone_list
    common::ObArray<share::ObZoneReplicaNumSet> zone_locality;
    common::ObArray<common::ObZone> zone_list;
    if (OB_SUCC(ret)) {
      ObSchemaGetterGuard schema_guard;
      if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
      } else if (is_new_tablegroup_id(partition.schema_id_)) {
        // PG
        const ObTablegroupSchema *tablegroup_schema = NULL;
        if (OB_FAIL(schema_guard.get_tablegroup_schema(partition.schema_id_, tablegroup_schema))) {
          LOG_WARN("fail to get tablegroup schema", K(ret), K(partition));
        } else if (OB_ISNULL(tablegroup_schema)) {
          ret = OB_TABLEGROUP_NOT_EXIST;
          LOG_WARN("tablegroup not exist", K(ret), K(partition));
        } else if (OB_FAIL(tablegroup_schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
          LOG_WARN("fail to get zone replica num array", K(ret));
        } else if (OB_FAIL(tablegroup_schema->get_zone_list(schema_guard, zone_list))) {
          LOG_WARN("fail to get zone list", K(ret));
        }
      } else {
        // standalone partition
        const ObSimpleTableSchemaV2 *table_schema = NULL;
        if (OB_FAIL(schema_guard.get_table_schema(partition.schema_id_, table_schema))) {
          LOG_WARN("fail to get table schema", K(ret), K(partition));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("table not exist", K(ret), K(partition));
        } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(schema_guard, zone_locality))) {
          LOG_WARN("fail to get zone replica num array", K(ret));
        } else if (OB_FAIL(table_schema->get_zone_list(schema_guard, zone_list))) {
          LOG_WARN("fail to get zone list", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObFilterLocalityUtility filter_locality_utility(
          *zone_mgr_, zone_locality, *unit_mgr_, partition_info.get_tenant_id(), zone_list);
      if (OB_FAIL(filter_locality_utility.init())) {
        LOG_WARN("fail to init filter locality utility", K(ret));
      } else if (OB_FAIL(filter_locality_utility.filter_locality(partition_info))) {
        LOG_WARN("fail to filter locality", K(ret), K(partition_info));
      } else {
        locality_match = filter_locality_utility.get_filter_result().count() <= 0;
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::add_member_list_pkey(const PhysicalRestorePartition &partition,
    const ObPhysicalRestoreStat &stat, ObRecoveryHelper::ObMemberListPkeyList &partition_infos)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (partition.get_replica_count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("partition has no replica", K(ret), K(partition));
  } else {
    ObPartitionInfo partition_info;
    partition_info.set_table_id(partition.schema_id_);
    partition_info.set_partition_id(partition.partition_id_);
    FOR_BEGIN_END_E(r, partition, stat.replicas_, OB_SUCC(ret))
    {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is null", K(ret), K(partition));
      } else if (OB_FAIL(partition_info.add_replica(*r))) {
        LOG_WARN("fail to add replica", K(ret), KPC(r));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_infos.add_partition(partition_info))) {
        LOG_WARN("fail to add partition info", K(ret), K(partition_info));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::add_member_list_pkey(const ObPartitionKey &pkey,
    const ObPartitionReplica::MemberList &member_list, ObRecoveryHelper::ObMemberListPkeyList &partition_infos)
{
  int ret = OB_SUCCESS;
  int64_t member_cnt = member_list.count();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (member_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid member cnt", K(ret), K(member_cnt));
  } else {
    ObPartitionInfo partition_info;
    partition_info.set_table_id(pkey.get_table_id());
    partition_info.set_partition_id(pkey.get_partition_id());
    for (int64_t i = 0; OB_SUCC(ret) && i < member_cnt; i++) {
      ObPartitionReplica replica;
      replica.table_id_ = pkey.get_table_id();
      replica.partition_id_ = pkey.get_partition_id();
      replica.partition_cnt_ = 0;
      replica.server_ = member_list.at(i).server_;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_FAIL(replica.zone_.assign("fake_zone"))) {  // not used
        LOG_WARN("fail to assign zone", K(ret));
      } else if (OB_FAIL(partition_info.add_replica(replica))) {
        LOG_WARN("fail to add replica", K(ret), K(replica));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_infos.add_partition(partition_info))) {
        LOG_WARN("fail to add partition info", K(ret), K(partition_info));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::batch_set_member_list(const ObRecoveryHelper::ObMemberListPkeyList &partition_infos)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (0 == partition_infos.ml_pk_array_.count()) {
    // nohing todo
  } else {
    obrpc::ObSetMemberListBatchArg arg;
    int64_t timestamp = ObTimeUtility::current_time();
    arg.timestamp_ = timestamp;
    ObSetMemberListBatchProxy batch_rpc_proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::batch_set_member_list);
    int64_t rpc_count = 0;
    for (int64_t i = 0; i < partition_infos.ml_pk_array_.count() && OB_SUCC(ret); i++) {
      const ObMemberList &member_list = partition_infos.ml_pk_array_.at(i).member_list_;
      for (int64_t j = 0; j < partition_infos.ml_pk_array_.at(i).pkey_info_.count() && OB_SUCC(ret); j++) {
        int64_t index = partition_infos.ml_pk_array_.at(i).pkey_info_.at(j).pkey_index_;
        const ObPartitionKey &key = partition_infos.pkey_array_.at(index);
        if (OB_FAIL(check_stop())) {
          LOG_WARN("restore scheduler stopped", K(ret));
        } else if (OB_FAIL(arg.add_arg(key, member_list))) {
          LOG_WARN("fail to add partition key", K(ret));
        } else if (!arg.reach_concurrency_limit()) {
          // nothing todo
        } else if (OB_FAIL(send_batch_set_member_list_rpc(arg, batch_rpc_proxy))) {
          LOG_WARN("fail to send batch set_member_list rpc", K(ret));
        } else {
          arg.reset();
          arg.timestamp_ = timestamp;
          rpc_count += member_list.get_member_number();
        }
      }
      if (OB_FAIL(ret) || !arg.has_task()) {
        // nothing todo
      } else if (OB_FAIL(send_batch_set_member_list_rpc(arg, batch_rpc_proxy))) {
        LOG_WARN("fail to send batch set_member_list rpc", K(ret));
      } else {
        arg.reset();
        arg.timestamp_ = timestamp;
        rpc_count += member_list.get_member_number();
      }
    }
    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = batch_rpc_proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (return_code_array.count() != rpc_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect return code", K(ret), "return_count", return_code_array.count(), K(rpc_count));
    } else {
      for (int64_t i = 0; i < return_code_array.count() && OB_SUCC(ret); i++) {
        if (OB_SUCCESS != return_code_array.at(i)) {
          ret = return_code_array.at(i);
          LOG_WARN("get failed return code", K(ret), K(i), K(return_code_array));
        }
      }
      for (int64_t i = 0; i < batch_rpc_proxy.get_results().count() && OB_SUCC(ret); i++) {
        const ObCreatePartitionBatchRes *res = batch_rpc_proxy.get_results().at(i);
        if (OB_ISNULL(res)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get result is null", K(ret));
        } else if (res->timestamp_ != timestamp) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid response", K(ret), KPC(res), K(timestamp));
        } else {
          for (int64_t j = 0; j < res->ret_list_.count() && OB_SUCC(ret); j++) {
            if (OB_SUCCESS != res->ret_list_.at(j)) {
              ret = res->ret_list_.at(j);
              LOG_WARN("set member_list failed", K(ret), K(j));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::send_batch_set_member_list_rpc(
    ObSetMemberListBatchArg &arg, ObSetMemberListBatchProxy &batch_rpc_proxy)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (arg.args_.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(arg));
  } else {
    ObAddr server;
    const ObMemberList &server_list = arg.args_.at(0).member_list_;
    const int64_t MAX_WAIT_TIMEOUT = 10 * 1000 * 1000;
    int64_t abs_timeout = ObTimeUtility::current_time() + MAX_WAIT_TIMEOUT;
    for (int64_t i = 0; i < server_list.get_member_number() && OB_SUCC(ret); i++) {
      int64_t now = ObTimeUtility::current_time();
      int64_t timeout_us = abs_timeout - now;
      if (timeout_us <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("already timeout", K(ret), K(abs_timeout));
      } else if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (0 == server_list.get_member_number()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(server_list));
      } else if (OB_FAIL(server_list.get_server_by_index(i, server))) {
        LOG_WARN("fail to get server by index", K(ret), K(i), K(server_list));
      } else if (OB_FAIL(batch_rpc_proxy.call(server, timeout_us, arg))) {
        LOG_WARN("fail to send async rpc", K(ret));
      } else {
        int64_t cost = ObTimeUtility::current_time() - now;
        LOG_INFO("end send batch set_member_list rpc", K(server), K(arg), "rpc_cost", cost);
      }
    }
  }
  LOG_INFO("end send batch tranlate rpc", K(ret), K(arg));
  return ret;
}

int ObRestoreScheduler::build_member_list_map(
    const uint64_t tenant_id, common::hash::ObHashMap<ObPartitionKey, ObPartitionReplica::MemberList> &member_list_map)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    ObSqlString sql;
    HEAP_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt(
              "select * from %s where tenant_id = %ld", OB_ALL_PARTITION_MEMBER_LIST_TNAME, tenant_id))) {
        LOG_WARN("failed to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret));
      } else {
        ObString member_list_str;
        ObPartitionReplica::MemberList member_list;
        int64_t partition_id = OB_INVALID_ID;
        const int64_t partition_cnt = 0;
        int64_t table_id = OB_INVALID_ID;
        ObPartitionKey key;
        int64_t total_cnt = 0;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          member_list_str.reset();
          member_list.reset();
          EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", partition_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, int64_t);
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "member_list", member_list_str);
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to get result", K(ret), K(sql));
          } else if (OB_FAIL(check_stop())) {
            LOG_WARN("restore scheduler stopped", K(ret));
          } else if (OB_FAIL(ObPartitionReplica::text2member_list(to_cstring(member_list_str), member_list))) {
            LOG_WARN("failed to get member_list", K(ret), K(member_list_str));
          } else if (OB_FAIL(key.init(table_id, partition_id, partition_cnt))) {
            LOG_WARN("failed to init partition key", K(ret), K(table_id), K(partition_id));
          } else if (member_list.count() <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid member_list", K(ret), K(key));
          } else if (OB_FAIL(member_list_map.set_refactored(key, member_list))) {
            LOG_WARN("fail to set member_list", K(ret), K(key), K(member_list));
          } else {
            total_cnt++;
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("build member_list map failed", K(ret), K(tenant_id));
        }
        LOG_INFO("build member_list map", K(ret), K(total_cnt));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::batch_persist_member_list(
    ObArray<const PhysicalRestorePartition *> &persist_partitions, const ObPhysicalRestoreStat &stat)
{
  int ret = OB_SUCCESS;
  int64_t total_cnt = persist_partitions.count();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (total_cnt <= 0) {
    // skip
  } else {
    int64_t start = 0;
    int64_t end = 0;
    const int64_t STEP = 1024;
    for (int64_t start = 0; OB_SUCC(ret) && start < total_cnt; start = end + 1) {
      end = min(start + STEP, total_cnt - 1);
      ObDMLSqlSplicer dml;
      for (int64_t i = start; OB_SUCC(ret) && i <= end; i++) {
        const PhysicalRestorePartition *partition = persist_partitions.at(i);
        if (OB_ISNULL(partition)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition is null", K(ret));
        } else if (OB_FAIL(check_stop())) {
          LOG_WARN("restore scheduler stopped", K(ret));
        } else if (OB_FAIL(fill_dml_splicer(*partition, stat, dml))) {
          LOG_WARN("fail to fill dml splicer", K(ret), KPC(partition));
        } else if (OB_FAIL(dml.finish_row())) {
          LOG_WARN("failed to finish row", K(ret), KPC(partition));
        }
      }  // end for
      int64_t affected_row = 0;
      int64_t partition_cnt = end - start + 1;
      ObSqlString sql;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_PARTITION_MEMBER_LIST_TNAME, sql))) {
        LOG_WARN("failed to splice batch insert sql", K(ret), K(sql));
      } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_row))) {
        LOG_WARN("failed to execute", K(ret), K(affected_row), K(sql));
      } else if (partition_cnt != affected_row) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_row not equal to partition count", K(ret), K(affected_row), K(partition_cnt));
      }
    }  // end for
    LOG_INFO("batch persist member_list", K(ret), K(total_cnt));
  }
  return ret;
}

int ObRestoreScheduler::fill_dml_splicer(
    const PhysicalRestorePartition &partition, const ObPhysicalRestoreStat &stat, ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  // fill member_list
  ObPartitionReplica::MemberList member_list;
  ObArenaAllocator allocator(ObModIds::OB_RS_PARTITION_TABLE_TEMP);
  const int64_t length = MAX_MEMBER_LIST_LENGTH;
  char *member_list_str = NULL;
  if (NULL == (member_list_str = static_cast<char *>(allocator.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(length));
  } else {
    FOR_BEGIN_END_E(r, partition, stat.replicas_, OB_SUCC(ret))
    {
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(r)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica is null", K(ret), K(partition));
      } else if (OB_FAIL(member_list.push_back(ObPartitionReplica::Member(r->server_, 0)))) {
        LOG_WARN("fail to add member", K(ret), KPC(r));
      }
    }  // end for
    if (OB_FAIL(ret)) {
    } else if (member_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition has no replica", K(ret), K(partition));
    } else if (OB_FAIL(ObPartitionReplica::member_list2text(member_list, member_list_str, length))) {
      LOG_WARN("failed to member list to str", K(ret), K(partition), K(member_list));
    }
  }
  // fill dml
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(partition.schema_id_))) ||
             OB_FAIL(dml.add_pk_column("table_id", partition.schema_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", partition.partition_id_)) ||
             OB_FAIL(dml.add_column("member_list", member_list_str))) {
    LOG_WARN("add column failed", K(ret), K(partition), K(member_list_str));
  }
  return ret;
}

int ObRestoreScheduler::clear_member_list_table(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    int64_t affected_rows = -1;
    const int64_t BATCH_NUM = 10000;
    while (OB_SUCC(ret) && 0 != affected_rows) {
      ObSqlString sql;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu LIMIT %ld",
                     OB_ALL_PARTITION_MEMBER_LIST_TNAME,
                     tenant_id,
                     BATCH_NUM))) {
        LOG_WARN("fail to assign sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
        LOG_WARN("failed to execute", K(ret), K(affected_rows), K(sql));
      } else {
        LOG_INFO("delete from __all_partition_member_list", K(ret), K(affected_rows));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::refresh_schema(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
    // TODO:() should refator in ver 3.3.0
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    ObRefreshSchemaStatus schema_status(tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION, OB_INVALID_VERSION);
    ObArray<uint64_t> tenant_ids;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_status_proxy->set_refresh_schema_status(schema_status))) {
      LOG_WARN("init tenant schema status failed", K(ret), K(schema_status));
    } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
      LOG_WARN("fail to push back tenant_id", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service_->refresh_and_add_schema(tenant_ids))) {
      LOG_WARN("fail to refresh schema", K(ret), K(tenant_id));
    } else {
      obrpc::ObBroadcastSchemaArg arg;
      arg.tenant_id_ = tenant_id;
      arg.schema_version_ = OB_INVALID_VERSION;
      if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).broadcast_schema(arg))) {
        LOG_WARN("fail to broadcast_schema", K(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::check_gts(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  int64_t ts_type = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_timestamp_service_type(tenant_id, ts_type))) {
    LOG_WARN("fail to get ts_type", KR(ret), K(tenant_id));
  } else if (transaction::TS_SOURCE_GTS != ts_type) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't restore tenant which timestamp service is not gts", KR(ret), K(job_info), K(ts_type));

    // mark restore job failed
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), K(tmp_ret), K(job_info));
    }
  }
  return ret;
}

int ObRestoreScheduler::modify_schema(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  bool need_log_nop = true;
#ifdef ERRSIM
  need_log_nop = false;
#endif

  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_MODIFY_SCHEMA);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(force_drop_schema(tenant_id))) {
    LOG_WARN("fail to force drop schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(filter_schema(job_info))) {
    LOG_WARN("fail to filter schema", KR(ret), K(job_info));
  } else if (OB_FAIL(convert_schema_options(tenant_id))) {
    LOG_WARN("fail to convert table options", K(ret), K(tenant_id));
  } else if (OB_FAIL(convert_index_status(job_info))) {
    LOG_WARN("fail to convert index status", K(ret), K(job_info));
  } else if (OB_FAIL(convert_parameters(job_info))) {
    LOG_WARN("fail to convert parameters", K(ret), K(job_info));
  } else if (need_log_nop && OB_FAIL(log_nop_operation(job_info))) {
    LOG_WARN("fail to log nop operation", KR(ret), K(job_info));
  } else if (OB_FAIL(convert_column_statistic(job_info.tenant_id_))) {
    LOG_WARN("failed to convert column statistic", K(ret), K(job_info));
  } else {
    // reset __all_restore_progress
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(sql_proxy_))) {
      LOG_WARN("fail init", KR(ret));
    } else if (OB_FAIL(restore_op.reset_restore_progress(job_info))) {
      LOG_WARN("fail to update progress", KR(ret), K(job_info));
    } else {
      LOG_INFO("reset progress success");
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
    }
  }
  LOG_INFO("[RESTORE] modify schema", K(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::force_drop_schema(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret));
  } else {
    ObForceDropSchemaChecker drop_schema_checker(*(GCTX.root_service_),
        *schema_service_,
        *rpc_proxy_,
        *sql_proxy_,
        ObForceDropSchemaChecker::RESTORE_MODE /*restore_mode*/);
    int64_t recycle_schema_version = 0;  // delete all delay dropped schema
    int64_t task_cnt = 0;
    bool exist = false;
    if (OB_FAIL(drop_schema_checker.force_drop_schema(tenant_id, recycle_schema_version, task_cnt))) {
      LOG_WARN("force drop schema", K(ret), K(tenant_id), K(recycle_schema_version), K(task_cnt));
    } else if (OB_FAIL(drop_schema_checker.check_dropped_schema_exist(tenant_id, exist))) {
      LOG_WARN("fail to check dropped schema exist", K(ret), K(tenant_id));
    } else if (exist) {
      ret = OB_EAGAIN;
      LOG_WARN("dropped schema exist, wait for next round", K(ret), K(tenant_id));
    }
  }
  return ret;
}

// If white_list is not empty, we try drop the following schemas:
// 1. view : all views in tenant
// 2. index : index which related data_table is not in white list
// 3. table : user table which is not in white list
// 4. foreign key : foreign key which related child_table or related parent table is not in white list
// 5. tablegroup : tablegroups except table's tablegroup in white list and tenant/database's default tablegroup
// 6. recyclebin objects in tenant
int ObRestoreScheduler::filter_schema(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  const ObSArray<obrpc::ObTableItem> &table_items = job_info.white_list_.get_table_white_list();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (table_items.count() <= 0) {
    // white_list is empty, it's no need to filter schema
  } else {
    common::hash::ObHashSet<uint64_t> table_white_list;
    common::hash::ObHashSet<uint64_t> tablegroup_white_list;
    const int64_t BUCKET_NUM = 1024;
    if (OB_FAIL(table_white_list.create(BUCKET_NUM))) {
      LOG_WARN("fail to init table_white_list", KR(ret));
    } else if (OB_FAIL(tablegroup_white_list.create(BUCKET_NUM))) {
      LOG_WARN("fail to init tablegroup_white_list", KR(ret));
    } else if (OB_FAIL(gen_white_list(job_info, table_items, table_white_list, tablegroup_white_list))) {
      LOG_WARN("fail to gen white list", KR(ret), K(table_items));
    } else if (OB_FAIL(filter_recyclebin_objects(tenant_id))) {
      LOG_WARN("fail to filter recyclebin objects", KR(ret), K(tenant_id));
    } else if (OB_FAIL(filter_view_and_foreign_key(tenant_id, table_white_list))) {
      LOG_WARN("fail to filter view/foreign key", KR(ret), K(tenant_id));
    } else if (OB_FAIL(filter_table(tenant_id, table_white_list))) {
      LOG_WARN("fail to filter table", KR(ret), K(tenant_id));
    } else if (OB_FAIL(filter_tablegroup(tenant_id, tablegroup_white_list))) {
      LOG_WARN("fail to filter tablegroup", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObRestoreScheduler::gen_white_list(const ObPhysicalRestoreJob &job_info,
    const ObIArray<obrpc::ObTableItem> &table_items, common::hash::ObHashSet<uint64_t> &table_white_list,
    common::hash::ObHashSet<uint64_t> &tablegroup_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const uint64_t tenant_id = job_info.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); i++) {
      const obrpc::ObTableItem &item = table_items.at(i);
      const ObDatabaseSchema *db = NULL;
      const ObSimpleTableSchemaV2 *tb = NULL;
      if (OB_FAIL(schema_guard.get_database_schema(tenant_id, item.database_name_, db))) {
        LOG_WARN("fail to get database schema", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(db)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("database not exist", KR(ret), K(item));
      } else if (db->is_or_in_recyclebin()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("restore table in recyclebin is not supported", KR(ret), K(item));
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id,
                     db->get_database_id(),
                     item.table_name_,
                     false, /*is_index*/
                     tb))) {
        LOG_WARN("fail to get table schema", KR(ret), K(item));
      } else if (OB_ISNULL(tb)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("table not exist", KR(ret), K(item));
      } else if (OB_FAIL(table_white_list.set_refactored(tb->get_table_id()))) {
        LOG_WARN("fail to set table white list", KR(ret), K(item));
      } else if (OB_INVALID_ID != tb->get_tablegroup_id() &&
                 OB_FAIL(tablegroup_white_list.set_refactored_1(tb->get_tablegroup_id(), true /*overwrite*/))) {
        LOG_WARN("fail to set tablegroup_id", KR(ret), K(item));
      }
    }
    // TODO:(yanmu.ztl) We can reset database/tenant's default tablegroup so we won't restore such tablegroup.
    if (OB_SUCC(ret)) {
      ObArray<const ObDatabaseSchema *> databases;
      if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, databases))) {
        LOG_WARN("fail to get databases", KR(ret), K(tenant_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < databases.count(); i++) {
          const ObDatabaseSchema *database = databases.at(i);
          if (OB_ISNULL(database)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("database is null", KR(ret));
          } else if (OB_INVALID_ID != database->get_default_tablegroup_id()) {
            if (OB_FAIL(tablegroup_white_list.set_refactored_1(
                    database->get_default_tablegroup_id(), true /*overwrite*/))) {
              LOG_WARN("fail to set default tablegroup id", KR(ret), KPC(database));
            }
          }
        }
      }
      const ObTenantSchema *tenant = NULL;
      if (FAILEDx(schema_guard.get_tenant_info(tenant_id, tenant))) {
        LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("tenant not exist", KR(ret), K(tenant_id));
      } else if (OB_INVALID_ID != tenant->get_default_tablegroup_id()) {
        if (OB_FAIL(tablegroup_white_list.set_refactored_1(tenant->get_default_tablegroup_id(), true /*overwrite*/))) {
          LOG_WARN("fail to set default tablegroup id", KR(ret), KPC(tenant));
        }
      }
    }
    // restore fail when:
    // 1. table/database/tenant doesn't exist.
    // 2. table is in recyclebin.
    // 3. table is oracle temp table.
    if (OB_NOT_SUPPORTED == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
        LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::filter_recyclebin_objects(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    const int64_t PURGE_EACH_RPC = 100;
    const int64_t SLEEP_INTERVAL = 1 * 1000 * 1000L;    // 1s
    const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // default 2s
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;  // 10s
    int64_t rpc_timeout = max(TIMEOUT_PER_RPC, DEFAULT_TIMEOUT);
    obrpc::ObPurgeRecycleBinArg arg;
    arg.tenant_id_ = tenant_id;
    arg.exec_tenant_id_ = tenant_id;
    arg.expire_time_ = ObTimeUtility::current_time();
    arg.auto_purge_ = true;
    arg.purge_num_ = PURGE_EACH_RPC;
    while (OB_SUCC(ret)) {
      obrpc::Int64 affected_rows = 0;
      if (OB_FAIL(rpc_proxy_->timeout(rpc_timeout).purge_expire_recycle_objects(arg, affected_rows))) {
        LOG_WARN("purge reyclebin objects failed", KR(ret), K(arg));
      } else if (affected_rows < PURGE_EACH_RPC) {
        LOG_INFO("purge recyclebin objects done", KR(ret), K(arg), K(affected_rows));
        break;
      } else {
        LOG_INFO("purge recyclebin objects, need continue", KR(ret), K(arg), K(affected_rows));
        usleep(SLEEP_INTERVAL);
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::filter_view_and_foreign_key(
    const uint64_t tenant_id, const common::hash::ObHashSet<uint64_t> &table_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2 *> tables;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObSimpleTableSchemaV2 *table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", KR(ret));
      } else if (is_inner_table(table->get_table_id())) {
        // skip
      } else if (table->is_view_table()) {
        // user view
        if (OB_FAIL(try_drop_table(schema_guard, *table))) {
          LOG_WARN("fail to drop view", KR(ret), KPC(table));
        }
      } else if (table->is_user_table() && table->get_simple_foreign_key_info_array().count() > 0) {
        // foreign key's child table
        if (OB_FAIL(try_drop_foreign_key(schema_guard, *table, table_white_list))) {
          LOG_WARN("fail to drop foreign key", KR(ret), KPC(table));
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::try_drop_table(
    ObSchemaGetterGuard &schema_guard, const share::schema::ObSimpleTableSchemaV2 &table)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    const uint64_t tenant_id = table.get_tenant_id();
    const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // default 2s
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;  // 10s
    int64_t rpc_timeout = max(TIMEOUT_PER_RPC, DEFAULT_TIMEOUT);
    obrpc::ObDropTableArg arg;
    arg.if_exist_ = true;
    arg.tenant_id_ = tenant_id;
    arg.exec_tenant_id_ = tenant_id;
    arg.to_recyclebin_ = false;
    arg.table_type_ = table.get_table_type();
    const ObSimpleDatabaseSchema *database = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(table.get_database_id(), database))) {
      LOG_WARN("fail to get database schema", KR(ret), K(table));
    } else if (OB_ISNULL(database)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("database not exist", KR(ret), K(table));
    } else {
      ObTableItem table_item;
      table_item.database_name_ = database->get_database_name();
      table_item.table_name_ = table.get_table_name();
      if (OB_FAIL(arg.tables_.push_back(table_item))) {
        LOG_WARN("fail to add table item", KR(ret), K(table_item));
      } else if (OB_FAIL(rpc_proxy_->timeout(rpc_timeout).drop_table(arg))) {
        LOG_WARN("drop table failed", KR(ret), K(arg));
      } else {
        LOG_INFO("drop table/view", KR(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::try_drop_foreign_key(ObSchemaGetterGuard &schema_guard,
    const share::schema::ObSimpleTableSchemaV2 &table, const common::hash::ObHashSet<uint64_t> &table_white_list)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!table.is_user_table() || table.get_simple_foreign_key_info_array().count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table should has foreign keys", KR(ret), K(table));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    const uint64_t tenant_id = table.get_tenant_id();
    const ObTableSchema *full_table = NULL;
    const ObSimpleDatabaseSchema *database = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(table.get_database_id(), database))) {
      LOG_WARN("fail to get database schema", KR(ret), K(table));
    } else if (OB_ISNULL(database)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("database not exist", KR(ret), K(table));
    } else if (OB_FAIL(schema_guard.get_table_schema(table.get_table_id(), full_table))) {
      LOG_WARN("fail to get full table schema", KR(ret), K(table));
    } else if (OB_ISNULL(full_table)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("full table should exist", KR(ret), K(table));
    } else {
      const ObIArray<ObForeignKeyInfo> &foreign_keys = full_table->get_foreign_key_infos();
      for (int i = 0; OB_SUCC(ret) && i < foreign_keys.count(); i++) {
        const ObForeignKeyInfo &foreign_key = foreign_keys.at(i);
        const uint64_t child_table_id = foreign_key.child_table_id_;
        const uint64_t parent_table_id = foreign_key.parent_table_id_;
        int hash_ret_1 = table_white_list.exist_refactored(child_table_id);
        int hash_ret_2 = table_white_list.exist_refactored(parent_table_id);
        if (child_table_id != full_table->get_table_id()) {
          // Only child table drop foreign key.
        } else if (OB_SUCCESS == hash_ret_1 || OB_SUCCESS == hash_ret_2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hash ret should not be OB_SUCCESS", KR(ret), KR(hash_ret_1), KR(hash_ret_2));
        } else if (OB_HASH_EXIST == hash_ret_1 && OB_HASH_EXIST == hash_ret_2) {
          // child_table and parent_table are both in white_list. We keep such foreign key.
        } else if (OB_HASH_NOT_EXIST == hash_ret_1 || OB_HASH_NOT_EXIST == hash_ret_2) {
          // drop foreign key
          ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;
          bool is_oracle_mode = false;
          if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
            LOG_WARN("fail to get tenant mode", KR(ret), K(tenant_id));
          } else {
            is_oracle_mode = (ObWorker::CompatMode::ORACLE == compat_mode);
          }
          ObSqlString sql;
          const ObString &database_name = database->get_database_name();
          const ObString &table_name = full_table->get_table_name();
          const ObString &foreign_key_name = foreign_key.foreign_key_name_;
          int64_t affected_rows = 0;
          if (FAILEDx(sql.append_fmt(is_oracle_mode ? "ALTER TABLE \"%.*s\".\"%.*s\" DROP CONSTRAINT \"%.*s\""
                                                    : "ALTER TABLE `%.*s`.`%.*s` DROP FOREIGN KEY `%.*s`",
                  database_name.length(),
                  database_name.ptr(),
                  table_name.length(),
                  table_name.ptr(),
                  foreign_key_name.length(),
                  foreign_key_name.ptr()))) {
            LOG_WARN("fail to append sql", KR(ret), K(database_name), K(table_name), K(foreign_key_name));
          } else if (is_oracle_mode && OB_FAIL(oracle_sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", KR(ret), K(sql));
          } else if (!is_oracle_mode && OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", KR(ret), K(sql));
          } else {
            LOG_INFO("drop foreign key", KR(ret), K(sql));
          }
        } else {
          ret = hash_ret_1;
          LOG_WARN("fail to check table exist", KR(ret), K(child_table_id), K(parent_table_id));
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::filter_table(
    const uint64_t tenant_id, const common::hash::ObHashSet<uint64_t> &table_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTableSchemaV2 *> tables;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("fail to get table schemas", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObSimpleTableSchemaV2 *table = tables.at(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", KR(ret));
      } else if (is_inner_table(table->get_table_id())) {
        // skip
      } else if (table->is_user_table()) {
        int hash_ret = table_white_list.exist_refactored(table->get_table_id());
        if (OB_HASH_EXIST == hash_ret) {
          // skip
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          // drop table
          if (OB_FAIL(try_drop_table(schema_guard, *table))) {
            LOG_WARN("fail to drop table", KR(ret), KPC(table));
          }
        } else {
          ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
          LOG_WARN("fail to check table exsit", KR(ret), KPC(table));
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::filter_tablegroup(
    const uint64_t tenant_id, const common::hash::ObHashSet<uint64_t> &tablegroup_white_list)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTablegroupSchema *> tablegroups;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    LOG_WARN("fail to get tablegroups", KR(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablegroups.count(); i++) {
      const ObSimpleTablegroupSchema *tablegroup = tablegroups.at(i);
      if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup is null", KR(ret), K(tenant_id));
      } else if (extract_pure_id(tablegroup->get_tablegroup_id()) <= OB_USER_TABLEGROUP_ID) {
        // skip system tablegroup
      } else {
        int hash_ret = tablegroup_white_list.exist_refactored(tablegroup->get_tablegroup_id());
        if (OB_HASH_EXIST == hash_ret) {
          // skip
        } else if (OB_HASH_NOT_EXIST == hash_ret) {
          // drop tablegroup
          if (OB_FAIL(try_drop_tablegroup(*tablegroup))) {
            LOG_WARN("fail to drop tablegroup", KR(ret), KPC(tablegroup));
          }
        } else {
          ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
          LOG_WARN("fail to check tablegroup exsit", KR(ret), KPC(tablegroup));
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::try_drop_tablegroup(const share::schema::ObSimpleTablegroupSchema &tablegroup)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    const uint64_t tenant_id = tablegroup.get_tenant_id();
    const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // default 2s
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;  // 10s
    int64_t rpc_timeout = max(TIMEOUT_PER_RPC, DEFAULT_TIMEOUT);
    obrpc::ObDropTablegroupArg arg;
    arg.if_exist_ = true;
    arg.tenant_id_ = tenant_id;
    arg.exec_tenant_id_ = tenant_id;
    arg.tablegroup_name_ = tablegroup.get_tablegroup_name();
    if (OB_FAIL(rpc_proxy_->timeout(rpc_timeout).drop_tablegroup(arg))) {
      LOG_WARN("drop tablegroup failed", KR(ret), K(arg));
    } else {
      LOG_INFO("drop tablegroup", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObRestoreScheduler::convert_schema_options(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(convert_database_options(tenant_id))) {
    LOG_WARN("fail to convert database options", K(ret), K(tenant_id));
  } else if (OB_FAIL(convert_tablegroup_options(tenant_id))) {
    LOG_WARN("fail to convert tablegroup options", K(ret), K(tenant_id));
  } else if (OB_FAIL(convert_table_options(tenant_id))) {
    LOG_WARN("fail to convert table options", K(ret), K(tenant_id));
  }
  return ret;
}

int ObRestoreScheduler::convert_database_options(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObArray<const ObDatabaseSchema *> databases;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_database_schemas_in_tenant(tenant_id, databases))) {
    LOG_WARN("fail to get databases", K(ret), K(tenant_id));
  } else {
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
    ObRestoreModifySchemaArg arg;
    arg.exec_tenant_id_ = tenant_id;
    arg.type_ = ObRestoreModifySchemaArg::RESET_DATABASE_PRIMARY_ZONE;
    for (int64_t i = 0; OB_SUCC(ret) && i < databases.count(); i++) {
      const ObDatabaseSchema *&database = databases.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(database)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("database is null", K(ret), K(tenant_id));
      } else if (database->get_primary_zone().empty()) {
        // skip
      } else {
        int tmp_ret = OB_SUCCESS;
        ObDatabaseSchema new_database;
        if (OB_FAIL(new_database.assign(*database))) {
          LOG_WARN("fail to assign new database", K(ret), KPC(database));
        } else if (OB_SUCCESS !=
                   (tmp_ret = ddl_service_->check_create_database_replica_options(new_database, schema_guard))) {
          LOG_INFO("backup primary_zone not match, just reset",
              K(tmp_ret),
              "database_id",
              database->get_database_id(),
              "primary_zone",
              database->get_primary_zone().ptr());
          arg.schema_id_ = database->get_database_id();
          if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
            LOG_WARN("fail to modify database's primary_zone", K(ret), K(arg));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::convert_tablegroup_options(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArray<const ObTablegroupSchema *> tablegroups;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    LOG_WARN("fail to get tablegroups", K(ret), K(tenant_id));
  } else {
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablegroups.count(); i++) {
      const ObTablegroupSchema *&tablegroup = tablegroups.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup is null", K(ret), K(tenant_id));
      } else if (!tablegroup->get_primary_zone().empty() || !tablegroup->get_locality_str().empty() ||
                 !tablegroup->get_previous_locality_str().empty()) {
        const uint64_t tablegroup_id = tablegroup->get_tablegroup_id();
        ObRestoreModifySchemaArg arg;
        arg.exec_tenant_id_ = tenant_id;
        arg.schema_id_ = tablegroup_id;
        // check primary_zone
        if (OB_FAIL(ret)) {
        } else if (!tablegroup->get_primary_zone().empty()) {
          int tmp_ret = OB_SUCCESS;
          ObTablegroupSchema new_tablegroup;
          if (OB_FAIL(new_tablegroup.assign(*tablegroup))) {
            LOG_WARN("fail to assign new tablegroup", K(ret), KPC(tablegroup));
          } else if (OB_SUCCESS !=
                     (tmp_ret = ddl_service_->check_create_tablegroup_replica_options(new_tablegroup, schema_guard))) {
            LOG_INFO("backup primary_zone not match, just reset",
                K(tmp_ret),
                "tablegroup_id",
                tablegroup->get_tablegroup_id(),
                "primary_zone",
                tablegroup->get_primary_zone().ptr());
            arg.type_ = ObRestoreModifySchemaArg::RESET_TABLEGROUP_PRIMARY_ZONE;
            if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
              LOG_WARN("fail to modify tablegroup's primary_zone", K(ret), K(arg));
            }
          }
        }
        // check locality
        if (OB_FAIL(ret)) {
        } else if (!tablegroup->get_locality_str().empty()) {
          int tmp_ret = OB_SUCCESS;
          ObArray<share::ObZoneReplicaAttrSet> locality;
          const ObString &locality_str = tablegroup->get_locality_str();
          if (OB_SUCCESS != (tmp_ret = ddl_service_->check_tablegroup_locality_with_tenant(
                                 schema_guard, *tenant_schema, *tablegroup))) {
            LOG_WARN("locality not match", K(tmp_ret), K(tablegroup_id), K(locality_str));
          } else if (OB_SUCCESS != (tmp_ret = tablegroup->get_zone_replica_attr_array(locality))) {
            LOG_WARN("fail to get locality array", K(tmp_ret), K(tablegroup_id), K(locality_str));
          } else if (OB_SUCCESS != (tmp_ret = check_locality_valid(locality))) {
            LOG_WARN("locality not supported", K(tmp_ret), K(tablegroup_id), K(locality_str));
          }
          if (OB_SUCCESS != tmp_ret) {
            LOG_INFO("backup locality not match, just reset", K(tmp_ret), K(tablegroup_id), K(locality_str));
            arg.type_ = ObRestoreModifySchemaArg::RESET_TABLEGROUP_LOCALITY;
            if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
              LOG_WARN("fail to modify tablegroup's locality", K(ret), K(arg));
            }
          }
        }
        // check previous_locality
        if (OB_FAIL(ret)) {
        } else if (!tablegroup->get_previous_locality_str().empty()) {
          LOG_INFO("backup previous_locality not match, just reset",
              "tablegroup_id",
              tablegroup->get_tablegroup_id(),
              "previous_locality",
              tablegroup->get_previous_locality_str().ptr());
          arg.type_ = ObRestoreModifySchemaArg::RESET_TABLEGROUP_PREVIOUS_LOCALITY;
          if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
            LOG_WARN("fail to modify tablegroup's previous locality", K(ret), K(arg));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::convert_table_options(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  ObArray<const ObSimpleTableSchemaV2 *> tables;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("tenant not exist", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("fail to get tables", K(ret), K(tenant_id));
  } else {
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObSimpleTableSchemaV2 *&table = tables.at(i);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null", K(ret), K(tenant_id));
      } else if (!table->get_primary_zone().empty() || !table->get_locality_str().empty() ||
                 !table->get_previous_locality_str().empty()) {
        const uint64_t table_id = table->get_table_id();
        ObRestoreModifySchemaArg arg;
        arg.exec_tenant_id_ = tenant_id;
        arg.schema_id_ = table_id;
        // check primary_zone
        if (OB_FAIL(ret)) {
        } else if (!table->get_primary_zone().empty()) {
          int tmp_ret = OB_SUCCESS;
          ObSimpleTableSchemaV2 new_table;
          const ObString &locality_str = table->get_locality_str();
          if (OB_FAIL(new_table.assign(*table))) {
            LOG_WARN("fail to assign new table", K(ret), KPC(table));
          } else if (OB_SUCCESS !=
                     (tmp_ret = ddl_service_->check_create_table_replica_options(new_table, schema_guard))) {
            LOG_WARN("locality not match", K(tmp_ret), K(table_id), K(locality_str));
          }
          if (OB_SUCCESS != tmp_ret) {
            LOG_INFO("backup primary_zone not match, just reset", K(tmp_ret), K(table_id), K(locality_str));
            arg.type_ = ObRestoreModifySchemaArg::RESET_TABLE_PRIMARY_ZONE;
            if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
              LOG_WARN("fail to modify table's primary_zone", K(ret), K(arg));
            }
          }
        }
        // check locality
        if (OB_FAIL(ret)) {
        } else if (!table->get_locality_str().empty()) {
          int tmp_ret = OB_SUCCESS;
          ObArray<share::ObZoneReplicaAttrSet> locality;
          const ObString &locality_str = table->get_locality_str();

          if (OB_SUCCESS !=
              (tmp_ret = ddl_service_->check_table_locality_with_tenant(schema_guard, *tenant_schema, *table))) {
            LOG_INFO("backup locality not match, just reset",
                K(tmp_ret),
                "table_id",
                table->get_table_id(),
                "locality",
                table->get_locality_str().ptr());
          } else if (OB_SUCCESS != (tmp_ret = table->get_zone_replica_attr_array(locality))) {
            LOG_WARN("fail to get locality array", K(tmp_ret), K(table_id), K(locality_str));
          } else if (OB_SUCCESS != (tmp_ret = check_locality_valid(locality))) {
            LOG_WARN("locality not supported", K(tmp_ret), K(table_id), K(locality_str));
          }

          if (OB_SUCCESS != tmp_ret) {
            arg.type_ = ObRestoreModifySchemaArg::RESET_TABLE_LOCALITY;
            if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
              LOG_WARN("fail to modify table's locality", K(ret), K(arg));
            }
          }
        }
        // check previous_locality
        if (OB_FAIL(ret)) {
        } else if (!table->get_previous_locality_str().empty()) {
          LOG_INFO("backup previous_locality not match, just reset",
              "table_id",
              table->get_table_id(),
              "previous_locality",
              table->get_previous_locality_str().ptr());
          arg.type_ = ObRestoreModifySchemaArg::RESET_TABLE_PREVIOUS_LOCALITY;
          if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).modify_schema_in_restore(arg))) {
            LOG_WARN("fail to modify table's previous locality", K(ret), K(arg));
          }
        }
      }
    }
  }
  return ret;
}

/*
 * If index's index_staus is:
 * case 1. unavaliable/restore_error: should be reset to error.
 * case 2. error/unusable : do nothing.
 * case 3. avaliable:
 * - case 3.1. If index is avaliable when data backup, we do nothing.
 * - case 3.2. If index is created or is avaliable when clog backup,
 *             index_status should be reset to unavaliable, and index should be rebuilded later.
 * Since we have dropped delay-deleted schemas before, we don't consider delay-deleted indexes here.
 */
int ObRestoreScheduler::convert_index_status(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard base_guard;    // schema_guard with schema_version using by data backup
  ObSchemaGetterGuard schema_guard;  // schema_guard with local latest schema version
  ObArray<uint64_t> error_index_ids;
  ObArray<uint64_t> unavaliable_index_ids;
  ObMultiVersionSchemaService::RefreshSchemaMode mode = ObMultiVersionSchemaService::FORCE_FALLBACK;
  uint64_t tenant_id = job_info.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
                 tenant_id, base_guard, job_info.schema_version_, OB_INVALID_VERSION /*latest*/, mode))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id), "schema_version", job_info.schema_version_);
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else {
    ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      LOG_WARN("fail to get table schemas in tenant", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, table schema is NULL", KR(ret));
        } else if (table_schema->is_index_table()) {
          const uint64_t index_id = table_schema->get_table_id();
          const ObIndexStatus index_status = table_schema->get_index_status();
          if (INDEX_STATUS_UNAVAILABLE == index_status || INDEX_STATUS_RESTORE_INDEX_ERROR == index_status) {  // case 1
            if (OB_FAIL(error_index_ids.push_back(index_id))) {
              LOG_WARN("fail to push back index id", KR(ret), K(index_id));
            }
          } else if (INDEX_STATUS_INDEX_ERROR == index_status || INDEX_STATUS_UNUSABLE == index_status) {
            // case 2, just skip
          } else if (INDEX_STATUS_AVAILABLE == index_status) {
            const ObSimpleTableSchemaV2 *index_schema = NULL;
            if (OB_FAIL(base_guard.get_table_schema(index_id, index_schema))) {
              LOG_WARN("fail to get index schema", KR(ret), K(index_id));
            } else if (OB_ISNULL(index_schema)) {
              // case 3.2 index is created when clog backup.
              if (OB_FAIL(unavaliable_index_ids.push_back(index_id))) {
                LOG_WARN("fail to push back index id", KR(ret), K(index_id));
              }
            } else if (INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
              // case 3.2 index is avaliable when clog backup.
              if (OB_FAIL(unavaliable_index_ids.push_back(index_id))) {
                LOG_WARN("fail to push back index id", KR(ret), K(index_id));
              }
            } else {
              // case 3.1, just skip
            }
          }
        }
      }
    }
    if (FAILEDx(update_index_status(error_index_ids, INDEX_STATUS_INDEX_ERROR))) {
      LOG_WARN("fail to update index status", KR(ret), K(tenant_id));
    } else if (OB_FAIL(update_index_status(unavaliable_index_ids, INDEX_STATUS_UNAVAILABLE))) {
      LOG_WARN("fail to update index status", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObRestoreScheduler::update_index_status(const common::ObIArray<uint64_t> &index_ids, ObIndexStatus index_status)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (INDEX_STATUS_UNAVAILABLE != index_status && INDEX_STATUS_INDEX_ERROR != index_status) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid index status", K(ret), K(index_status));
  } else if (index_ids.count() <= 0) {
    // skip
  } else {
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
    obrpc::ObUpdateIndexStatusArg arg;
    arg.exec_tenant_id_ = extract_tenant_id(index_ids.at(0));
    arg.status_ = index_status;
    arg.convert_status_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_ids.count(); i++) {
      arg.index_table_id_ = index_ids.at(i);
      arg.exec_tenant_id_ = extract_tenant_id(arg.index_table_id_);
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).update_index_status(arg))) {
        LOG_WARN("fail to update index status", K(ret), K(arg));
      } else {
        LOG_INFO("set index status success", K(ret), K(arg));
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::convert_parameters(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  }

  if (OB_SUCC(ret)) {
    // Broadcast tenant's config version after system tables are restored.
    int64_t force_refresh_version = ObSystemConfig::INIT_VERSION;
    if (OB_FAIL(OTC_MGR.set_tenant_config_version(tenant_id, force_refresh_version))) {
      LOG_WARN("fail set tenant config version", K(tenant_id), K(force_refresh_version));
    } else {
      //  Because clog module can't distinguish whether tenant's config is not set or refreshed,
      //  it will cause error when clog is encrypted and tenant's config isn't reloaded.
      //  Here, we wait a while to reduce the possibility of badcase.
      usleep(5 * 1000 * 1000L);  // 5s
    }
  }
  return ret;
}

/*
 * In ver 3.1.x/3.2.x, add_partition_to_pg may enhance memtable's schema_version,
 * which may be greator than broadcasted schema version. Because user tables' clog replay
 * is still limited by broadcasted schema version, it may cause deadlock when cluster is restoring.
 * To avoid this, we should broadcast new schema version before restore of user tables.
 */
int ObRestoreScheduler::log_nop_operation(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    obrpc::ObDDLNopOpreatorArg arg;
    arg.schema_operation_.tenant_id_ = tenant_id;
    arg.exec_tenant_id_ = tenant_id;
    arg.schema_operation_.op_type_ = OB_DDL_FINISH_PHYSICAL_RESTORE_MODIFY_SCHEMA;
    int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;  // 10s
    int64_t timeout = max(GCONF.rpc_timeout, DEFAULT_TIMEOUT);
    if (OB_ISNULL(rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc_proxy is null", KR(ret), KP_(rpc_proxy));
    } else if (OB_FAIL(rpc_proxy_->timeout(timeout).log_nop_operation(arg))) {
      LOG_WARN("fail to log nop operation", KR(ret), K(arg));
    } else {
      LOG_INFO("success to log nop operation", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObRestoreScheduler::convert_column_statistic(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObColumnStatisticOperator op;
  const int64_t version = 1;
  if (OB_FAIL(op.init(sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(op.update_column_statistic_version(tenant_id, version))) {
    LOG_WARN("fail to get jobs", K(ret), K(tenant_id));
  }
  return ret;
}

// not reentrant
int ObRestoreScheduler::create_user_partitions(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  ObSchemaGetterGuard latest_guard;
  ObSchemaGetterGuard base_guard;
  int64_t local_schema_version = OB_INVALID_VERSION;
  ObMultiVersionSchemaService::RefreshSchemaMode mode = ObMultiVersionSchemaService::FORCE_FALLBACK;
  ObArray<const ObSimpleTableSchemaV2 *> tables;
  ObArray<const ObTablegroupSchema *> tablegroups;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_USER_PARTITIONS);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (job_info.schema_version_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema version is invalid", KR(ret), K(job_info));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, latest_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(latest_guard.get_schema_version(tenant_id, local_schema_version))) {
    LOG_WARN("fail to get schema version", KR(ret), K(tenant_id));
  } else if (job_info.schema_version_ > local_schema_version) {
    ret = OB_EAGAIN;
    LOG_WARN("local schema is old, try again", KR(ret), K(local_schema_version), K(job_info));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
                 tenant_id, base_guard, job_info.schema_version_, OB_INVALID_VERSION /*latest*/, mode))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id), "schema_version", job_info.schema_version_);
  } else if (OB_FAIL(latest_guard.get_user_table_schemas_in_tenant(tenant_id, tables))) {
    LOG_WARN("get tenant table schemas failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(latest_guard.get_tablegroup_schemas_in_tenant(tenant_id, tablegroups))) {
    LOG_WARN("fail to get tenant tablegroup schemas", KR(ret), K(tenant_id));
  } else {
    // Only non-delay-deleted PG/Standalone partitions should be created.
    DEBUG_SYNC(BEFORE_SEND_RESTORE_PARTITIONS_RPC);
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;  // 10s
    const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // 2s
    const int64_t PARTITION_CNT_PER_RPC = 5;
    for (int64_t i = 0; i < tablegroups.count() && OB_SUCC(ret); ++i) {
      const ObTablegroupSchema *tablegroup = tablegroups.at(i);
      const ObTablegroupSchema *base_tablegroup = NULL;
      uint64_t tablegroup_id = OB_INVALID_ID;
      ObRestorePartitionsArg arg;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stop", KR(ret));
      } else if (OB_UNLIKELY(nullptr == tablegroup)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablegroup schema ptr is null", KR(ret));
      } else if (FALSE_IT(tablegroup_id = tablegroup->get_tablegroup_id())) {
      } else if (!tablegroup->has_self_partition()) {
        // bypass
      } else if (OB_FAIL(base_guard.get_tablegroup_schema(tablegroup_id, base_tablegroup))) {
        LOG_WARN("fail to get base tablegroup schema", KR(ret), K(tablegroup_id));
      } else if (OB_FAIL(fill_restore_partition_arg(tablegroup_id, base_tablegroup, arg))) {
        LOG_WARN("fail to fill restore partition arg", KR(ret), K(tablegroup_id));
      } else {
        int64_t timeout = (tablegroup->get_all_part_num() / PARTITION_CNT_PER_RPC) * TIMEOUT_PER_RPC;
        timeout = max(timeout, DEFAULT_TIMEOUT);
        if (OB_FAIL(rpc_proxy_->timeout(timeout).restore_partitions(arg))) {
          LOG_WARN("fail to create pg", KR(ret), K(arg), K(timeout));
        } else {
          LOG_INFO("physical restore create pg", KR(ret), K(arg), K(timeout));
        }
      }
    }
    for (int64_t i = 0; i < tables.count() && OB_SUCC(ret); ++i) {
      const ObSimpleTableSchemaV2 *table = tables.at(i);
      const ObSimpleTableSchemaV2 *base_table = NULL;
      uint64_t table_id = OB_INVALID_ID;
      ObRestorePartitionsArg arg;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stop", KR(ret));
      } else if (OB_UNLIKELY(nullptr == table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema ptr is null", KR(ret));
      } else if (!table->has_self_partition()) {
        // bypass
      } else if (FALSE_IT(table_id = table->get_table_id())) {
      } else if (is_inner_table(table_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("should not be inner table", KR(ret), K(table_id));
      } else if (OB_FAIL(base_guard.get_table_schema(table_id, base_table))) {
        LOG_WARN("fail to get base table schema", KR(ret), K(table_id));
      } else if (OB_FAIL(fill_restore_partition_arg(table_id, base_table, arg))) {
        LOG_WARN("fail to fill restore partition arg", KR(ret), K(table_id));
      } else {
        int64_t timeout = (table->get_all_part_num() / PARTITION_CNT_PER_RPC) * TIMEOUT_PER_RPC;
        timeout = max(timeout, DEFAULT_TIMEOUT);
        if (OB_FAIL(rpc_proxy_->timeout(timeout).restore_partitions(arg))) {
          LOG_WARN("fail to create standalone partition", KR(ret), K(arg), K(timeout));
        } else {
          LOG_INFO("physical restore create standalone partition", KR(ret), K(arg), K(timeout));
        }
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), K(tmp_ret), K(job_info));
    }
  }
  LOG_INFO("[RESTORE] create user partitions", KR(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::fill_restore_partition_arg(
    const uint64_t schema_id, const ObPartitionSchema *schema, obrpc::ObRestorePartitionsArg &arg)
{
  int ret = OB_SUCCESS;
  arg.schema_id_ = schema_id;
  arg.mode_ = OB_CREATE_TABLE_MODE_PHYSICAL_RESTORE;
  bool skip = false;
  if (OB_ISNULL(schema)) {
    // schema doesn't exist for data restore, and log restore is needed.
    skip = true;
  } else if (!is_new_tablegroup_id(arg.schema_id_)) {
    // unavaliable index for data restore
    const ObTableSchema *table = static_cast<const ObTableSchema *>(schema);
    if (table->has_self_partition() && table->is_global_index_table() &&
        (table->is_dropped_schema() || INDEX_STATUS_AVAILABLE != table->get_index_status())) {
      skip = true;
    }
  }

  if (OB_SUCC(ret) && !skip) {
    bool check_dropped_schema = false;
    ObPartitionKeyIter iter(arg.schema_id_, *schema, check_dropped_schema);
    int64_t partition_id = OB_INVALID_ID;
    while (OB_SUCC(ret) && OB_SUCC(iter.next_partition_id_v2(partition_id))) {
      if (OB_FAIL(arg.partition_ids_.push_back(partition_id))) {
        LOG_WARN("fail to push back partition_id", K(ret), K(schema_id), K(partition_id));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      ret = OB_FAIL(ret) ? ret : OB_ERR_UNEXPECTED;
      LOG_WARN("iter failed", K(ret));
    }
  }
  return ret;
}

int ObRestoreScheduler::restore_user_replica(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else {
    DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_USER_REPLICA);
    bool sys_only = false;
    ObPhysicalRestoreStat stat(*schema_service_, *sql_proxy_, *pt_operator_, job_info, sys_only, stop_);
    int64_t task_cnt = 0;
    if (OB_FAIL(stat.gather_stat())) {
      LOG_WARN("fail to gather stat", K(ret), K(job_info));
    } else if (OB_FAIL(schedule_restore_task(job_info, stat, task_cnt))) {
      LOG_WARN("fail to schedule restore task", K(ret), K(job_info));
    } else if (0 == task_cnt) {
      if (OB_FAIL(set_member_list(job_info, stat))) {
        LOG_WARN("fail to set member_list", K(ret), K(job_info));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
          LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
        }
      }
    }
  }
  LOG_INFO("[RESTORE] restore user replica", K(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::rebuild_index(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = job_info.tenant_id_;
  bool exist = true;
  const ObTenantSchema *tenant_schema = NULL;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_REBUILD_INDEX);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), K(tenant_id));
  } else if (!tenant_schema->is_restore()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant schema state not match", K(ret), KPC(tenant_schema));
  } else if (OB_FAIL(schema_guard.check_restore_error_index_exist(tenant_id, exist))) {
    LOG_WARN("fail to check unavailable index exist", K(ret), K(tenant_id));
  } else if (exist) {
    ret = OB_RESTORE_INDEX_FAILED;
    LOG_WARN("index restore failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.check_unavailable_index_exist(tenant_id, exist))) {
    LOG_WARN("fail to check unavailable index exist", K(ret), K(tenant_id));
  } else if (!exist) {
    // skip
  } else {
    LOG_INFO("unavailable index exist", K(ret), K(tenant_id));
    const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
    // try schedule build index task
    obrpc::ObRebuildIndexInRestoreArg arg;
    arg.tenant_id_ = tenant_id;
    if (OB_FAIL(check_stop())) {
      LOG_WARN("restore scheduler stopped", K(ret));
    } else if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).rebuild_index_in_restore(arg))) {
      LOG_WARN("fail to schedule build index task", K(ret), K(arg));
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
    }
  }
  LOG_INFO("[RESTORE] rebuild index", K(ret), K(job_info));
  return ret;
}

// TODO:() restore job should fail when rebuild index failed
int ObRestoreScheduler::post_check(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = job_info.tenant_id_;
  bool exist = true;
  const ObTenantSchema *tenant_schema = NULL;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_POST_CHECK);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant schema is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.check_unavailable_index_exist(tenant_id, exist))) {
    LOG_WARN("fail to check unavailable index exist", K(ret), K(tenant_id));
  } else if (exist) {
    // skip and wait
    LOG_INFO("unavailable index exist, just wait", K(ret), K(tenant_id));
  } else if (tenant_schema->is_restore() || tenant_schema->is_normal()) {
    if (tenant_schema->is_restore()) {
      const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
      // try finish restore status
      obrpc::ObCreateTenantEndArg arg;
      arg.tenant_id_ = tenant_id;
      arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", K(ret));
      } else if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT).create_tenant_end(arg))) {
        LOG_WARN("fail to create tenant end", K(ret), K(arg));
      }
    }
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
        LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
      }
    }
  } else {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant status not match", K(ret), KPC(tenant_schema));
  }
  LOG_INFO("[RESTORE] post check", K(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::restore_success(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  ObBackupInfoManager backup_info_manager;
  ObArray<uint64_t> tenant_ids;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(tenant_ids.push_back(job_info.tenant_id_))) {
    LOG_WARN("failed to push tenant id into array", K(ret), K(job_info));
  } else if (OB_FAIL(backup_info_manager.init(tenant_ids, *sql_proxy_))) {
    LOG_WARN("failed to init backup info manager", K(ret), K(tenant_ids));
  } else if (OB_FAIL(backup_info_manager.insert_restore_tenant_base_backup_version(
                 job_info.tenant_id_, job_info.restore_data_version_))) {
    LOG_WARN("failed to insert restore tenant base backup version", K(ret), K(job_info));
  } else if (OB_FAIL(restore_op.init(sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.recycle_job(job_info.job_id_, PHYSICAL_RESTORE_SUCCESS))) {
    LOG_WARN("finish restore tasks failed", K(job_info), K(ret));
  } else {
    LOG_INFO("[RESTORE] restore tenant success", K(ret), K(job_info));
  }
  ROOTSERVICE_EVENT_ADD("physical_restore", "restore_success", "tenant", job_info.tenant_name_);
  return ret;
}

int ObRestoreScheduler::restore_fail(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(drop_tenant_force_if_necessary(job_info))) {
    LOG_WARN("failed to drop_tenant_force_if_necessary", K(ret), K(job_info));
  } else if (OB_FAIL(restore_op.init(sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.recycle_job(job_info.job_id_, PHYSICAL_RESTORE_FAIL))) {
    LOG_WARN("finish restore tasks failed", K(job_info), K(ret));
  } else {
    LOG_INFO("[RESTORE] restore tenant failed", K(ret), K(job_info));
  }
  ROOTSERVICE_EVENT_ADD("physical_restore", "restore_failed", "tenant", job_info.tenant_name_);
  return ret;
}

int ObRestoreScheduler::check_stop() const
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("restore scheduler stopped", K(ret));
  }
  return ret;
}

/*
 * 1. Physical restore is not allowed when cluster is in upgrade mode or is standby.
 * 2. Physical restore jobs will be recycled asynchronously when restore tenant has been dropped.
 * 3. Physical restore jobs will be used to avoid duplicate tenant_name when tenant is creating.
 */
int ObRestoreScheduler::try_recycle_job(const ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  bool is_dropped = false;
  int failed_ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_RECYCLE_PHYSICAL_RESTORE_JOB);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (GCTX.is_standby_cluster()) {
    // 1. check standby cluster
    failed_ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[RESTORE] switch to standby cluster, try recycle job", KR(ret), "tenant_id", job.tenant_id_);
  } else if (GCONF.in_upgrade_mode()) {
    // 2. check in upgrade mode
    failed_ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[RESTORE] cluster is upgrading, try recycle job", KR(ret), "tenant_id", job.tenant_id_);
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_SUCCESS != schema_guard.check_formal_guard()) {
    // skip
  } else if (OB_INVALID_TENANT_ID == job.tenant_id_) {
    // restore job has already been recycled if restore tenant is not created successfully.
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(job.tenant_id_, is_dropped))) {
    LOG_WARN("fail to get tenant id", KR(ret), K(job));
  } else if (!is_dropped) {
    // skip
  } else {
    // 3. tenant has been dropped
    failed_ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("[RESTORE] tenant has been dropped, try recycle job", KR(ret), "tenant_id", job.tenant_id_);
  }
  if (OB_SUCC(ret) && OB_SUCCESS != failed_ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(failed_ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), K(tmp_ret), K(failed_ret), K(job));
    }
  }
  return ret;
}

int ObRestoreScheduler::mark_job_failed(int64_t job_id, int return_ret, PhysicalRestoreMod mod,
    const common::ObCurTraceId::TraceId &trace_id, const common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  bool exist = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (job_id < 0 || OB_SUCCESS == return_ret || PHYSICAL_RESTORE_MOD_MAX_NUM == mod) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(job_id), K(return_ret), K(mod));
  } else if (OB_FAIL(restore_op.init(sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else if (OB_FAIL(restore_op.check_job_exist(job_id, exist))) {
    LOG_WARN("fail to check job exist", K(ret), K(job_id));
  } else if (!exist) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("job not exist", K(ret), K(job_id));
  } else if (OB_FAIL(restore_op.update_job_error_info(job_id, return_ret, mod, trace_id, addr))) {
    LOG_WARN("fail to update job error info", K(ret), K(job_id), K(return_ret), K(mod));
  } else if (OB_FAIL(restore_op.update_job_status(job_id, PHYSICAL_RESTORE_FAIL))) {
    LOG_WARN("fail update job status", K(ret), K(job_id));
  } else {
    idling_.set_idle_interval_us(0);  // wakeup immediately
    LOG_INFO("[RESTORE] mark job failed", K(ret), K(job_id), K(return_ret));
  }
  return ret;
}

int ObRestoreScheduler::try_update_job_status(
    int return_ret, const ObPhysicalRestoreJob &job, share::PhysicalRestoreMod mod)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(restore_op.init(sql_proxy_))) {
    LOG_WARN("fail init", K(ret));
  } else {
    PhysicalRestoreStatus next_status = get_next_status(return_ret, job.status_);
    const common::ObCurTraceId::TraceId trace_id = *ObCurTraceId::get_trace_id();

    if (PHYSICAL_RESTORE_FAIL == next_status &&
        OB_FAIL(restore_op.update_job_error_info(job.job_id_, return_ret, mod, trace_id, self_addr_))) {
      LOG_WARN("fail to update job error info", K(ret), K(job), K(return_ret), K(mod));
    } else if (OB_FAIL(restore_op.update_job_status(job.job_id_, next_status))) {
      LOG_WARN("fail update job status", K(ret), K(job), K(next_status));
    } else {
      idling_.set_idle_interval_us(0);  // wakeup immediately
      LOG_INFO("[RESTORE] switch job status", K(ret), K(job), K(next_status));
      (void)record_rs_event(job, next_status);
    }
  }
  return ret;
}

void ObRestoreScheduler::record_rs_event(const ObPhysicalRestoreJob &job, const PhysicalRestoreStatus status)
{
  const char *status_str =
      ObPhysicalRestoreTableOperator::get_restore_status_str(static_cast<PhysicalRestoreStatus>(status));
  ROOTSERVICE_EVENT_ADD("physical_restore",
      "change_restore_status",
      "job_id",
      job.job_id_,
      "tenant",
      job.tenant_name_,
      "status",
      status_str);
}

PhysicalRestoreStatus ObRestoreScheduler::get_next_status(int return_ret, PhysicalRestoreStatus current_status)
{
  PhysicalRestoreStatus next_status = PHYSICAL_RESTORE_MAX_STATUS;
  if (OB_SUCCESS != return_ret) {
    next_status = PHYSICAL_RESTORE_FAIL;
  } else {
    switch (current_status) {
      case PHYSICAL_RESTORE_CREATE_TENANT: {
        next_status = PHYSICAL_RESTORE_SYS_REPLICA;
        break;
      }
      case PHYSICAL_RESTORE_SYS_REPLICA: {
        next_status = PHYSICAL_RESTORE_UPGRADE_PRE;
        break;
      }
      case PHYSICAL_RESTORE_UPGRADE_PRE: {
        next_status = PHYSICAL_RESTORE_UPGRADE_POST;
        break;
      }
      case PHYSICAL_RESTORE_UPGRADE_POST: {
        next_status = PHYSICAL_RESTORE_MODIFY_SCHEMA;
        break;
      }
      case PHYSICAL_RESTORE_MODIFY_SCHEMA: {
        next_status = PHYSICAL_RESTORE_CREATE_USER_PARTITIONS;
        break;
      }
      case PHYSICAL_RESTORE_CREATE_USER_PARTITIONS: {
        next_status = PHYSICAL_RESTORE_USER_REPLICA;
        break;
      }
      case PHYSICAL_RESTORE_USER_REPLICA: {
        next_status = PHYSICAL_RESTORE_REBUILD_INDEX;
        break;
      }
      case PHYSICAL_RESTORE_REBUILD_INDEX: {
        next_status = PHYSICAL_RESTORE_POST_CHECK;
        break;
      }
      case PHYSICAL_RESTORE_POST_CHECK: {
        next_status = PHYSICAL_RESTORE_SUCCESS;
        break;
      }
      default: {
        // do nothing
      }
    }
  }
  return next_status;
}

int ObRestoreScheduler::check_source_cluster_version(const uint64_t cluster_version)
{
  int ret = OB_SUCCESS;
  if (CLUSTER_VERSION_2260 > cluster_version || cluster_version > GET_MIN_CLUSTER_VERSION() ||
      !ObUpgradeChecker::check_cluster_version_exist(cluster_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can't restore data from such cluster version", KR(ret), K(cluster_version));
  }
  return ret;
}

/*
 * upgrade_pre() will execute actions below:
 * 1. modify system variable schema.
 * 2. run upgrade_pre() by version.
 * 3. refresh tenant's schema.
 */
int ObRestoreScheduler::upgrade_pre(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_UPGRADE_PRE);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(ObUpgradeUtils::upgrade_sys_variable(*sql_proxy_, tenant_id))) {
    LOG_WARN("fail to upgrade sys variable", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObUpgradeUtils::upgrade_sys_stat(*sql_proxy_, tenant_id))) {
    LOG_WARN("fail to upgrade sys stat", KR(ret), K(tenant_id));
  } else if (OB_FAIL(do_upgrade_pre(job_info))) {
    LOG_WARN("fail to do upgrade pre", KR(ret), K(job_info));
  } else if (OB_FAIL(refresh_schema(job_info))) {
    LOG_WARN("fail to refresh schema", KR(ret), K(job_info));
  } else if (OB_FAIL(check_gts(job_info))) {
    LOG_WARN("fail to check gts", KR(ret), K(job_info));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job_info));
    }
  }
  LOG_INFO("[RESTORE] upgrade pre finish", KR(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::upgrade_post(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_UPGRADE_POST);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(do_upgrade_post(job_info))) {
    LOG_WARN("fail to do upgrade post", KR(ret), K(job_info));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(ret, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), K(job_info));
    }
  }
  LOG_INFO("[RESTORE] upgrade post finish", KR(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::do_upgrade_pre(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_DO_UPGRADE_PRE);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (job_info.pre_cluster_version_ <= 0 || job_info.pre_cluster_version_ > GET_MIN_CLUSTER_VERSION()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pre_cluster_version is invalid", KR(ret), K(job_info));
  } else if (job_info.pre_cluster_version_ == GET_MIN_CLUSTER_VERSION()) {
    // pre upgrade job already done, just skip
  } else {
    const int64_t start_version = job_info.pre_cluster_version_;
    const int64_t end_version = GET_MIN_CLUSTER_VERSION();
    int64_t start_idx = OB_INVALID_INDEX;
    int64_t end_idx = OB_INVALID_INDEX;
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(sql_proxy_))) {
      LOG_WARN("fail init", KR(ret));
    } else if (OB_FAIL(
                   upgrade_processors_.get_processor_idx_by_range(start_version, end_version, start_idx, end_idx))) {
      LOG_WARN("fail to get processor idx by range", KR(ret), K(start_version), K(end_version));
    }
    for (int64_t idx = start_idx + 1; OB_SUCC(ret) && idx <= end_idx; idx++) {
      ObBaseUpgradeProcessor *processor = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", KR(ret));
      } else if (OB_FAIL(upgrade_processors_.get_processor_by_idx(idx, processor))) {
        LOG_WARN("fail to get processor by idx", KR(ret), K(idx), K(start_version));
      } else {
        int64_t start_ts = ObTimeUtility::current_time();
        int64_t current_version = processor->get_version();
        processor->set_tenant_id(tenant_id);
        // process
        LOG_INFO(
            "[RESTORE] start to run pre upgrade job by version", K(tenant_id), K(start_version), K(current_version));
        if (OB_FAIL(processor->pre_upgrade())) {
          LOG_WARN("run pre upgrade by version failed", KR(ret), K(tenant_id), K(start_version), K(current_version));
        }
        LOG_INFO("[RESTORE] finish pre upgrade job by version",
            KR(ret),
            K(tenant_id),
            K(start_version),
            K(current_version),
            "cost",
            ObTimeUtility::current_time() - start_ts);
        // update pre_cluster_version
        if (OB_SUCC(ret)) {
          char version[common::ObClusterVersion::MAX_VERSION_ITEM] = {0};
          int64_t len =
              ObClusterVersion::print_version_str(version, common::ObClusterVersion::MAX_VERSION_ITEM, current_version);
          if (OB_FAIL(
                  restore_op.update_restore_option(job_info.job_id_, "pre_cluster_version", ObString(len, version)))) {
            LOG_WARN("fail to update restore option", KR(ret), K(current_version), K(job_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::do_upgrade_post(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = job_info.tenant_id_;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_DO_UPGRADE_POST);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (job_info.post_cluster_version_ <= 0 || job_info.post_cluster_version_ > GET_MIN_CLUSTER_VERSION()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("post_cluster_version is invalid", KR(ret), K(job_info));
  } else if (job_info.post_cluster_version_ == GET_MIN_CLUSTER_VERSION()) {
    // post upgrade job already done, just skip
  } else {
    const int64_t start_version = job_info.post_cluster_version_;
    const int64_t end_version = GET_MIN_CLUSTER_VERSION();
    int64_t start_idx = OB_INVALID_INDEX;
    int64_t end_idx = OB_INVALID_INDEX;
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(sql_proxy_))) {
      LOG_WARN("fail init", KR(ret));
    } else if (OB_FAIL(
                   upgrade_processors_.get_processor_idx_by_range(start_version, end_version, start_idx, end_idx))) {
      LOG_WARN("fail to get processor idx by range", KR(ret), K(start_version), K(end_version));
    }
    for (int64_t idx = start_idx + 1; OB_SUCC(ret) && idx <= end_idx; idx++) {
      ObBaseUpgradeProcessor *processor = NULL;
      if (OB_FAIL(check_stop())) {
        LOG_WARN("restore scheduler stopped", KR(ret));
      } else if (OB_FAIL(upgrade_processors_.get_processor_by_idx(idx, processor))) {
        LOG_WARN("fail to get processor by idx", KR(ret), K(idx), K(start_version));
      } else {
        int64_t start_ts = ObTimeUtility::current_time();
        int64_t current_version = processor->get_version();
        processor->set_tenant_id(tenant_id);
        // process
        LOG_INFO(
            "[RESTORE] start to run post upgrade job by version", K(tenant_id), K(start_version), K(current_version));
        if (OB_FAIL(processor->post_upgrade())) {
          LOG_WARN("run post upgrade by version failed", KR(ret), K(tenant_id), K(start_version), K(current_version));
        }
        LOG_INFO("[RESTORE] finish post upgrade job by version",
            KR(ret),
            K(tenant_id),
            K(start_version),
            K(current_version),
            "cost",
            ObTimeUtility::current_time() - start_ts);
        // update post_cluster_version
        if (OB_SUCC(ret)) {
          char version[common::ObClusterVersion::MAX_VERSION_ITEM] = {0};
          int64_t len =
              ObClusterVersion::print_version_str(version, common::ObClusterVersion::MAX_VERSION_ITEM, current_version);
          if (OB_FAIL(
                  restore_op.update_restore_option(job_info.job_id_, "post_cluster_version", ObString(len, version)))) {
            LOG_WARN("fail to update restore option", KR(ret), K(current_version), K(job_info));
          }
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::drop_tenant_force_if_necessary(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  const bool need_force_drop = GCONF._auto_drop_tenant_if_restore_failed;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (need_force_drop) {
    ObSchemaGetterGuard schema_guard;
    ObString tenant_name(job_info.tenant_name_);
    const ObTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_name, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_name));
    } else if (OB_ISNULL(tenant_schema) || !tenant_schema->is_restore()) {
      LOG_INFO("tenant not exist or tenant is not in physical restore status, just skip", K(tenant_name));
    } else {
      obrpc::ObDropTenantArg arg;
      arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      arg.tenant_name_ = tenant_name;
      arg.if_exist_ = true;
      arg.delay_to_drop_ = false;
      ObSqlString sql;
      const int64_t TIMEOUT_PER_RPC = GCONF.rpc_timeout;  // default 2s
      const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;  // 10s
      int64_t rpc_timeout = max(TIMEOUT_PER_RPC, DEFAULT_TIMEOUT);
      if (OB_FAIL(sql.append_fmt("DROP TENANT IF EXISTS %s FORCE", arg.tenant_name_.ptr()))) {
        LOG_WARN("fail to generate sql", KR(ret), K(arg));
      } else if (FALSE_IT(arg.ddl_stmt_str_ = sql.string())) {
      } else if (OB_FAIL(rpc_proxy_->timeout(rpc_timeout).drop_tenant(arg))) {
        LOG_WARN("fail to drop tenant", KR(ret), K(arg));
      } else {
        LOG_INFO("drop_tenant_force after restore fail", K(job_info));
      }
    }
  } else {
    LOG_INFO("no need to drop tenant after restore fail", K(job_info));
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
