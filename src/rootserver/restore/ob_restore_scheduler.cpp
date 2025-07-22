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
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_unit_manager.h"//convert_pool_name_lis
#include "rootserver/ob_ls_service_helper.h"//create_new_ls_in_trans
#include "rootserver/ob_common_ls_service.h"//do_create_user_ls
#include "rootserver/standby/ob_tenant_role_transition_service.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "share/ob_schema_status_proxy.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/ob_upgrade_utils.h"
#include "lib/mysqlclient/ob_mysql_transaction.h" //ObMySQLTransaction
#include "share/ls/ob_ls_status_operator.h" //ObLSStatusOperator
#include "share/ls/ob_ls_operator.h"//ObLSAttr
#include "storage/backup/ob_backup_data_store.h"//ObBackupDataLSAttrDesc
#include "share/restore/ob_physical_restore_info.h"//ObPhysicalRestoreInfo
#include "share/restore/ob_physical_restore_table_operator.h"//ObPhysicalRestoreTableOperator
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "share/restore/ob_log_restore_source_mgr.h"
#include "share/ls/ob_ls_recovery_stat_operator.h"//ObLSRecoveryStatOperator
#include "share/ob_rpc_struct.h"
#include "rootserver/standby/ob_standby_service.h"
#include "logservice/palf/log_define.h"//scn
#include "share/scn.h"
#include "ob_restore_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace palf;

ObRestoreScheduler::ObRestoreScheduler()
  : inited_(false), schema_service_(NULL),
    sql_proxy_(NULL), rpc_proxy_(NULL),
    srv_rpc_proxy_(NULL), lst_operator_(NULL),
    restore_service_(nullptr), self_addr_(),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObRestoreScheduler::~ObRestoreScheduler()
{
}

int ObRestoreScheduler::init(ObRestoreService &restore_service)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)
      || OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.srv_rpc_proxy_)
      || OB_ISNULL(GCTX.lst_operator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_),
        KP(GCTX.rs_rpc_proxy_), KP(GCTX.srv_rpc_proxy_), KP(GCTX.lst_operator_));
  } else {
    schema_service_ = GCTX.schema_service_;
    sql_proxy_ = GCTX.sql_proxy_;
    rpc_proxy_ = GCTX.rs_rpc_proxy_;
    srv_rpc_proxy_ = GCTX.srv_rpc_proxy_;
    lst_operator_ = GCTX.lst_operator_;
    restore_service_ = &restore_service;
    tenant_id_ = is_sys_tenant(MTL_ID()) ? MTL_ID() : gen_user_tenant_id(MTL_ID());
    self_addr_ = GCTX.self_addr();
    inited_ = true;
  }
  return ret;
}
void ObRestoreScheduler::do_work()
{
  LOG_INFO("[RESTORE] restore scheduler start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObCurTraceId::init(GCTX.self_addr());
    LOG_INFO("[RESTORE] try process restore job");
    ObArray<ObPhysicalRestoreJob> job_infos;
    ObPhysicalRestoreTableOperator restore_op;
    if (OB_FAIL(restore_op.init(sql_proxy_, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
      LOG_WARN("fail init", K(ret), K(tenant_id_));
    } else if (OB_FAIL(restore_op.get_jobs(job_infos))) {
      LOG_WARN("fail to get jobs", KR(ret), K(tenant_id_));
    } else {
      FOREACH_CNT_X(job_info, job_infos, !restore_service_->has_set_stop()) { // ignore ret
        if (OB_ISNULL(job_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("job info is null", K(ret));
        } else if (is_sys_tenant(tenant_id_)) {
          if (OB_FAIL(process_sys_restore_job(*job_info))) {
            LOG_WARN("failed to process sys restore job", KR(ret), KPC(job_info));
          }
        } else if (OB_FAIL(process_restore_job(*job_info))) {
          LOG_WARN("fail to process restore job", K(ret), KPC(job_info));
        }
      }
    }
    ret = OB_SUCCESS;
    restore_service_->idle();
  }
  LOG_INFO("[RESTORE] restore scheduler quit");
  return;
}

int ObRestoreScheduler::process_sys_restore_job(const ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(MTL_ID()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not sys tenant", KR(ret));
  } else {
    switch (job.get_status()) {
      case PHYSICAL_RESTORE_CREATE_TENANT:
        ret = restore_tenant(job);
        break;
      case PHYSICAL_RESTORE_WAIT_TENANT_RESTORE_FINISH:
        ret = restore_wait_tenant_finish(job);
        break;
      case PHYSICAL_RESTORE_SUCCESS:
        ret = tenant_restore_finish(job);
        break;
      case PHYSICAL_RESTORE_FAIL:
        ret = tenant_restore_finish(job);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status not match", K(ret), K(job));
        break;
    }
    if (PHYSICAL_RESTORE_FAIL != job.get_status()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_recycle_job(job))) {
        LOG_WARN("fail to recycle job", K(tmp_ret), K(job));
      }
    }
    LOG_INFO("[RESTORE] doing restore", K(ret), K(job));
  }
  return ret;
}


int ObRestoreScheduler::process_restore_job(const ObPhysicalRestoreJob &job)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(is_sys_tenant(MTL_ID()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not sys tenant", KR(ret));
  } else {
    switch (job.get_status()) {
      case PHYSICAL_RESTORE_PRE:
        ret = restore_pre(job);
        break;
      case PHYSICAL_RESTORE_CREATE_INIT_LS:
        ret = restore_init_ls(job);
        break;
      case PHYSICAL_RESTORE_WAIT_CONSISTENT_SCN:
        ret = restore_wait_to_consistent_scn(job);
        break;
      case PHYSICAL_RESTORE_WAIT_LS:
        ret = restore_wait_ls_finish(job);
        break;
      case PHYSICAL_RESTORE_POST_CHECK:
        ret = post_check(job);
        break;
      case PHYSICAL_RESTORE_UPGRADE:
        ret = restore_upgrade(job);
        break;
      case PHYSICAL_RESTORE_SUCCESS:
        ret = restore_finish(job);
        break;
      case PHYSICAL_RESTORE_FAIL:
        ret = restore_finish(job);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("status not match", K(ret), K(job));
        break;
    }
    //TODO, table restore
    LOG_INFO("[RESTORE] doing restore", K(ret), K(job));
  }
  return ret;
}

// restore_tenant is not reentrant
int ObRestoreScheduler::restore_tenant(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObCreateTenantArg arg;
  //the pool list of job_info is obstring without '\0'
  ObSqlString pool_list;
  UInt64 tenant_id = OB_INVALID_TENANT_ID;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_TENANT);
  int64_t timeout =  GCONF._ob_ddl_timeout;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_INVALID_TENANT_ID != job_info.get_tenant_id()) {
    // restore_tenant can only be executed once.
    // only update job status 
  } else if (OB_FAIL(pool_list.assign(job_info.get_pool_list()))) {
    LOG_WARN("failed to assign pool list", KR(ret), K(job_info));
  } else if (OB_FAIL(fill_create_tenant_arg(job_info, pool_list, arg))) {
    LOG_WARN("fail to fill create tenant arg", K(ret), K(pool_list), K(job_info));
  } else if (OB_FAIL(rpc_proxy_->timeout(timeout).create_tenant(arg, tenant_id))) {
    LOG_WARN("fail to create tenant", K(ret), K(arg));
  } else {
    ObPhysicalRestoreTableOperator restore_op;
    const int64_t job_id = job_info.get_job_id();
    const uint64_t new_tenant_id = tenant_id;
    if (OB_FAIL(restore_op.init(sql_proxy_, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
      LOG_WARN("fail init", K(ret), K(tenant_id_));
    } else if (OB_FAIL(restore_op.update_restore_option(
            job_id, "tenant_id", new_tenant_id))) {
      LOG_WARN("update restore option", K(ret), K(new_tenant_id), K(job_id), K(tenant_id_));
    } else if (OB_FAIL(may_update_restore_concurrency_(new_tenant_id, job_info))) {
      LOG_WARN("failed to update restore concurrency", K(ret), K(new_tenant_id), K(job_info));
    } else if (!job_info.get_sts_credential().empty()
      && OB_FAIL(set_tenant_sts_crendential_config_(*sql_proxy_, new_tenant_id, job_info))) {
      LOG_WARN("fail to set tenant sts credential config", K(ret), K(new_tenant_id));
    } else {
      restore_service_->wakeup();
    }
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
    LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
  }
  LOG_INFO("[RESTORE] restore tenant", K(ret), K(arg), K(job_info));
  return ret;
}

int ObRestoreScheduler::fill_create_tenant_arg(
    const ObPhysicalRestoreJob &job,
    const ObSqlString &pool_list,
    ObCreateTenantArg &arg)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
                     OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", K(ret));
  } else if(lib::Worker::CompatMode::ORACLE != job.get_compat_mode() && lib::Worker::CompatMode::MYSQL != job.get_compat_mode()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid compat mode", K(ret));
  } else {
    /*
     * restore_tenant will only run trans one when create tenant.
     * Consider the following tenant options:
     * 1) need backup: tenant_name,compatibility_mode
     * 2) need backup and replace(maybe): zone_list,primary_zone,locality,previous_locality
     * 3) not backup yet:locked,default_tablegroup_id,info  TODO: (yanmu.ztl)
     * 4) no need to backup:drop_tenant_time,status,collation_type
     * 6) abandoned: replica_num,read_only,rewrite_merge_version,logonly_replica_num,
     *                storage_format_version,storage_format_work_version
     */
     ObCompatibilityMode mode = lib::Worker::CompatMode::ORACLE == job.get_compat_mode() ?
                                ObCompatibilityMode::ORACLE_MODE :
                                ObCompatibilityMode::MYSQL_MODE;
     arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
     arg.tenant_schema_.set_tenant_name(job.get_tenant_name());
     arg.tenant_schema_.set_compatibility_mode(mode);
     arg.if_not_exist_ = false;
     arg.is_restore_ = true;
     //  create tmp tenant for recover table
     arg.is_tmp_tenant_for_recover_ = job.get_recover_table();
     // Physical restore is devided into 2 stages. Recover to 'consistent_scn' which was recorded during
     // data backup first, then to user specified scn.
     arg.recovery_until_scn_ = job.get_consistent_scn();
     arg.compatible_version_ = job.get_source_data_version();
     if (OB_FAIL(assign_pool_list(pool_list.ptr(), arg.pool_list_))) {
       LOG_WARN("fail to get pool list", K(ret), K(pool_list));
     }

     if (OB_SUCC(ret)) {
       ObTenantSchema &tenant_schema = arg.tenant_schema_;
       const ObString& locality_str = job.get_locality();
       const ObString &primary_zone = job.get_primary_zone();
       if (!primary_zone.empty()) {
         // specific primary_zone
         tenant_schema.set_primary_zone(primary_zone);
       }
       if (!locality_str.empty()) {
         tenant_schema.set_locality(locality_str);
       }
     }
     if (FAILEDx(ObRestoreUtil::get_restore_ls_palf_base_info(job, SYS_LS, arg.palf_base_info_))) {
       LOG_WARN("failed to get sys ls palf base info", KR(ret), K(job));
     }
  }
  return ret;
}

int ObRestoreScheduler::assign_pool_list(
    const char *str,
    common::ObIArray<ObString> &pool_list)
{
  int ret = OB_SUCCESS;
  char *item_str = NULL;
  char *save_ptr = NULL;
  while (OB_SUCC(ret)) {
    item_str = strtok_r((NULL == item_str ? const_cast<char *>(str) : NULL), ",", &save_ptr);
    if (NULL != item_str) {
      ObString pool(item_str);
      if (OB_FAIL(pool_list.push_back(pool))) {
        LOG_WARN("push_back failed", K(ret), K(pool));
      }
    } else {
      break;
    }
  }
  return ret;
}

int ObRestoreScheduler::check_locality_valid(
    const share::schema::ZoneLocalityIArray &locality)
{
  int ret = OB_SUCCESS;
  int64_t cnt = locality.count();
  if (cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cnt", KR(ret), K(cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
      const share::ObZoneReplicaAttrSet &attr = locality.at(i);
      if (attr.is_specific_readonly_replica()
          || attr.is_allserver_readonly_replica()
          || attr.get_encryption_logonly_replica_num() > 0) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("locality with readonly/encrytion_logonly replica is not supported",
                 KR(ret), K(locality));
      } else if (attr.is_mixed_locality()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("mixed locality is not supported", KR(ret), K(locality));
      } else if (attr.is_specific_replica_attr()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("locality with memstore_percent is not supported", KR(ret), K(locality));
      }
    }
  }
  return ret;
}


int ObRestoreScheduler::check_tenant_can_restore_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id invalid", KR(ret), K(tenant_id));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (GCONF.in_upgrade_mode()) {
    // 2. check in upgrade mode
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[RESTORE] cluster is upgrading, try recycle job",
             KR(ret), K(tenant_id));
  }
  return ret;

}

//restore pre :modify parameters
int ObRestoreScheduler::restore_pre(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id_
             || OB_SYS_TENANT_ID == tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(restore_root_key(job_info))) {
    LOG_WARN("fail to restore root key", K(ret));
  } else if (OB_FAIL(restore_keystore(job_info))) {
    LOG_WARN("fail to restore keystore", K(ret), K(job_info));
  } else if (OB_FAIL(fill_restore_statistics(job_info))) {
    LOG_WARN("fail to fill restore statistics", K(ret), K(job_info));
  } else if (OB_FAIL(convert_parameters(job_info))) {
    LOG_WARN("fail to convert parameters", K(ret), K(job_info));
  }

  if (OB_IO_ERROR == ret || OB_KMS_SERVER_CONNECT_ERROR == ret || OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(try_update_job_status(*sql_proxy_, ret, job_info))) {
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
    }

    ret = COVER_SUCC(tmp_ret);
  }

  LOG_INFO("[RESTORE] restore pre", K(ret), K(job_info));

  return ret;
}
int ObRestoreScheduler::set_tenant_sts_crendential_config_(
    common::ObISQLClient &proxy, const uint64_t tenant_id, const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  int64_t affected_row = 0;
  ObSqlString sql;
  const ObString &sts_credential = job_info.get_sts_credential();
  bool is_exist = false;
  if (!is_user_tenant(tenant_id) || sts_credential.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not user tenant or sts credential is empty", K(job_info));
  } else if (OB_FAIL(sql.assign_fmt("alter system set sts_credential='%.*s'", sts_credential.length(), sts_credential.ptr()))) {
    LOG_WARN("failed to assign fmt", K(ret));
  } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_row))) {
    LOG_WARN("failed to set sts credential", K(ret), K(tenant_id));
  } else {
    LOG_INFO("update restore tenant sts credential", K(tenant_id));
  }
  return ret;
}

int ObRestoreScheduler::fill_restore_statistics(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObRestoreProgressPersistInfo restore_progress_info;
  restore_progress_info.key_.job_id_ = job_info.get_job_id();
  restore_progress_info.key_.tenant_id_ = job_info.get_tenant_id();
  restore_progress_info.restore_scn_ = job_info.get_restore_scn();
  int64_t idx = job_info.get_multi_restore_path_list().get_backup_set_path_list().count() - 1;
  ObBackupDataLSAttrDesc ls_info;
  if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job info", K(ret), K(idx), K(job_info));
  } else {
    storage::ObBackupDataStore store;
    storage::ObExternBackupSetInfoDesc backup_set_info;
    const share::ObBackupSetPath &backup_set_path = job_info.get_multi_restore_path_list().get_backup_set_path_list().at(idx);
    if (OB_FAIL(store.init(backup_set_path.ptr()))) {
      LOG_WARN("fail to init backup data store", K(backup_set_path));
    } else if (OB_FAIL(store.read_backup_set_info(backup_set_info))) {
      LOG_WARN("fail to read backup set info", K(ret));
    } else if (OB_FAIL(store.read_ls_attr_info(backup_set_info.backup_set_file_.meta_turn_id_, ls_info))) {
      LOG_WARN("fail to read ls attr info", K(ret));
    } else {
      restore_progress_info.ls_count_ = ls_info.ls_attr_array_.count();
      restore_progress_info.tablet_count_ = 0;
      restore_progress_info.total_bytes_ = backup_set_info.backup_set_file_.stats_.output_bytes_;
    }
  }
  if (OB_SUCC(ret)) {
    share::ObRestorePersistHelper helper;
    if (OB_FAIL(helper.init(job_info.get_tenant_id(), share::OBCG_STORAGE /*group_id*/))) {
      LOG_WARN("fail to init heler", K(ret));
    } else if (OB_FAIL(helper.insert_initial_restore_progress(*sql_proxy_, restore_progress_info))) {
      LOG_WARN("fail to insert initail ls restore progress", K(ret));
    }
  }
  return ret;
}

int ObRestoreScheduler::convert_parameters(
    const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  uint64_t tenant_id = tenant_id_;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
             || OB_SYS_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (job_info.get_kms_dest().empty()) {
    // do nothing
  } else {
    ObArenaAllocator allocator;
    int64_t affected_row = 0;
    ObString tde_method;
    ObString kms_info;
    ObSqlString sql;
    // set tde_method
    if (OB_FAIL(ObMasterKeyUtil::restore_encrypt_params(allocator, job_info.get_kms_dest(),
                                          job_info.get_kms_encrypt_key(), tde_method, kms_info))) {
      LOG_WARN("failed to restore encrypt params", K(ret));
    } else if (OB_UNLIKELY(tde_method.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tde_method is empty", K(ret));
    } else if (!job_info.get_kms_info().empty()) {
      kms_info = job_info.get_kms_info();
    }
    if (OB_FAIL(ret)) {
    } else if (!ObTdeMethodUtil::is_valid(tde_method)) {
      // do nothing
    } else if (OB_FAIL(sql.assign_fmt("ALTER SYSTEM SET tde_method = '%s'", tde_method.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_row))) {
      LOG_WARN("failed to execute", K(ret), K(affected_row), K(sql));
    } else if (ObTdeMethodUtil::is_internal(tde_method)) {
      // do nothing
    } else if (FALSE_IT(sql.reset())) {
    } else if (OB_FAIL(sql.assign_fmt("ALTER SYSTEM SET external_kms_info= '%s'", kms_info.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret), K(sql));
    } else if (OB_FAIL(sql_proxy_->write(tenant_id, sql.ptr(), affected_row))) {
      LOG_WARN("failed to execute", K(ret), K(affected_row), K(sql));
    }
  }
#endif
  return ret;
}

ERRSIM_POINT_DEF(EN_RESTORE_ROOT_KEY_FAILED);
int ObRestoreScheduler::restore_root_key(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = EN_RESTORE_ROOT_KEY_FAILED ? : OB_SUCCESS;
  if (OB_FAIL(ret)) {
    LOG_WARN("fake EN_RESTORE_ROOT_KEY_FAILED", K(ret));
  }
#endif

#ifdef OB_BUILD_TDE_SECURITY
  int64_t idx = 0;
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(idx = job_info.get_multi_restore_path_list().get_backup_set_path_list().count() - 1)) {
  } else if (idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid job info", K(ret), K(idx), K(job_info));
  } else if (OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null svr rpc proxy or sql proxy", K(ret));
  } else {
    storage::ObBackupDataStore store;
    const share::ObBackupSetPath &backup_set_path = job_info.get_multi_restore_path_list().get_backup_set_path_list().at(idx);
    ObRootKey root_key;
    if (OB_FAIL(store.init(backup_set_path.ptr()))) {
      LOG_WARN("fail to init backup data store", K(ret));
    } else if (OB_FAIL(store.read_root_key_info(tenant_id_))) {
      LOG_WARN("fail to read root key info", K(ret));
    } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(tenant_id_, root_key))) {
      LOG_WARN("fail to get root key", K(ret));
    } else if (obrpc::RootKeyType::INVALID == root_key.key_type_) {
      // do nothing
    } else {
      obrpc::ObRootKeyArg arg;
      obrpc::ObRootKeyResult result;
      ObUnitTableOperator unit_operator;
      ObArray<ObUnit> units;
      ObArray<ObAddr> addrs;
      arg.tenant_id_ = tenant_id_;
      arg.is_set_ = true;
      arg.key_type_ = root_key.key_type_;
      arg.root_key_ = root_key.key_;
      if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
        LOG_WARN("failed to init unit operator", KR(ret));
      } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id_, units))) {
        LOG_WARN("failed to get tenant unit", KR(ret), K_(tenant_id));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
        const ObUnit &unit = units.at(i);
        if (OB_FAIL(addrs.push_back(unit.server_))) {
          LOG_WARN("failed to push back addr", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDDLService::notify_root_key(*srv_rpc_proxy_, arg, addrs, result))) {
        LOG_WARN("failed to notify root key", K(ret));
      }
    }
  }
#endif
  return ret;
}

int ObRestoreScheduler::restore_keystore(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  const int64_t DEFAULT_TIMEOUT = 10 * 1000 * 1000L;
  ObUnitTableOperator unit_operator;
  common::ObArray<ObUnit> units;
  ObArray<int> return_code_array;
  obrpc::ObRestoreKeyArg arg;
  if (job_info.get_kms_dest().empty()) {
    // do nothing
  } else if (OB_ISNULL(srv_rpc_proxy_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null svr rpc proxy or sql proxy", K(ret));
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_units_by_tenant(tenant_id_, units))) {
    LOG_WARN("failed to get tenant unit", KR(ret), K_(tenant_id));
  } else {
    ObRestoreKeyProxy proxy(*srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::restore_key);
    arg.tenant_id_ = job_info.get_tenant_id();
    arg.backup_dest_ = job_info.get_kms_dest();
    arg.encrypt_key_ = job_info.get_kms_encrypt_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < units.count(); i++) {
      const ObUnit &unit = units.at(i);
      if (OB_FAIL(proxy.call(unit.server_, DEFAULT_TIMEOUT, arg))) {
        LOG_WARN("failed to send rpc", KR(ret));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("fail to check return cnt", KR(ret), "return_cnt", return_code_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
        ret = return_code_array.at(i);
        const ObAddr &addr = proxy.get_dests().at(i);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to restore key", KR(ret), K(addr));
        }
      }
    }
  }
#endif
  return ret;
}

int ObRestoreScheduler::post_check(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_POST_CHECK);
  ObAllTenantInfo all_tenant_info; 
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
  ObTenantRoleTransitionService role_transition_service;
  ObTenantRoleTransCostDetail cost_detail;
  ObTenantRoleTransAllLSInfo all_ls;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id_
             || OB_SYS_TENANT_ID == tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_,
          false, /*for_update*/all_tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (all_tenant_info.is_restore()) {
    //update tenant role to standby tenant
    int64_t new_switch_ts = 0;
    if (all_tenant_info.get_sync_scn() != job_info.get_restore_scn()) {
      ret = OB_NEED_WAIT;
      LOG_WARN("tenant sync scn not equal to restore scn, need wait", KR(ret), K(all_tenant_info), K(job_info));
    } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_role(
            tenant_id_, sql_proxy_, all_tenant_info.get_switchover_epoch(),
            share::STANDBY_TENANT_ROLE, all_tenant_info.get_switchover_status(),
            share::NORMAL_SWITCHOVER_STATUS, new_switch_ts))) {
      LOG_WARN("failed to update tenant role", KR(ret), K(tenant_id_), K(all_tenant_info));
    } else if (OB_FAIL(all_ls.init())) {
      LOG_WARN("fail to init all_ls", KR(ret));
    } else if (OB_FAIL(role_transition_service.init(
        tenant_id_,
        ObSwitchTenantArg::OpType::INVALID,
        false, /* is_verify */
        sql_proxy_,
        GCTX.srv_rpc_proxy_,
        &cost_detail,
        &all_ls))) {
      LOG_WARN("fail to init role_transition_service", KR(ret), K(tenant_id_),
          KP(sql_proxy_), KP(GCTX.srv_rpc_proxy_));
    } else {
      (void)role_transition_service.broadcast_tenant_info(
            ObTenantRoleTransitionConstants::RESTORE_TO_STANDBY_LOG_MOD_STR);
    }
  }

  if (FAILEDx(reset_schema_status(tenant_id_, sql_proxy_))) {
    LOG_WARN("failed to reset schema status", KR(ret));
  }

  if (OB_SUCC(ret)) {
    ObBroadcastSchemaArg arg;
    arg.tenant_id_ = tenant_id_;
    if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc proxy is null", KR(ret), KP(GCTX.rs_mgr_), KP(GCTX.rs_rpc_proxy_));
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to_rs(*GCTX.rs_mgr_).broadcast_schema(arg))) {
      LOG_WARN("failed to broadcast schema", KR(ret), K(arg));
    }
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
    }
  }
  LOG_INFO("[RESTORE] post check", K(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::restore_finish(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(ObRestoreUtil::recycle_restore_job(tenant_id_, *sql_proxy_,
                                                job_info))) {
    LOG_WARN("finish restore tasks failed", K(job_info), K(ret), K(tenant_id_));
  } else {
    LOG_INFO("[RESTORE] restore tenant success", K(ret), K(job_info));
  }
  ROOTSERVICE_EVENT_ADD("physical_restore", "restore_finish",
                        "restore_stauts", job_info.get_status(),
                        "tenant", job_info.get_tenant_name());
  return ret;
}

int ObRestoreScheduler::tenant_restore_finish(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObHisRestoreJobPersistInfo history_info;
  bool restore_tenant_exist = true;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(try_get_tenant_restore_history_(job_info, history_info, restore_tenant_exist))) {
    LOG_WARN("failed to get user tenant restory info", KR(ret), K(job_info));
  } else if (restore_tenant_exist && OB_FAIL(reset_restore_concurrency_(job_info.get_tenant_id(), job_info))) {
    LOG_WARN("failed to reset restore concurrency", K(ret), K(job_info));
  } else if (share::PHYSICAL_RESTORE_SUCCESS == job_info.get_status()) {
    //restore success
  } else {
    int tmp_ret = OB_SUCCESS;
    ObRestoreFailureChecker checker;
    bool is_concurrent_with_clean = false;
    if (OB_TMP_FAIL(checker.init(job_info))) {
      LOG_WARN("failed to init restore failure checker", K(tmp_ret), K(job_info));
    } else if (OB_TMP_FAIL(checker.check_is_concurrent_with_clean(is_concurrent_with_clean))) {
      LOG_WARN("failed to check is clean concurrency failure", K(tmp_ret));
    }
    if (OB_SUCC(ret) && is_concurrent_with_clean) {
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(history_info.comment_.ptr(), history_info.comment_.capacity(), pos,
                                  "%s;", "physical restore run concurrently with backup data clean, please check backup and archive jobs"))) {
        if (OB_SIZE_OVERFLOW == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to databuff printf comment", K(ret));
        }
      }
    }
  }

  if (FAILEDx(ObRestoreUtil::recycle_restore_job(*sql_proxy_,
                                                job_info, history_info))) {
    LOG_WARN("finish restore tasks failed", KR(ret), K(job_info), K(history_info), K(tenant_id_));
  } else {
    LOG_INFO("[RESTORE] restore tenant finish", K(ret), K(job_info));
  }
  ROOTSERVICE_EVENT_ADD("physical_restore", "restore_finish",
                        "restore_status", job_info.get_status(),
                        "tenant", job_info.get_tenant_name());
  return ret;
}

int ObRestoreScheduler::try_get_tenant_restore_history_(
    const ObPhysicalRestoreJob &job_info,
    ObHisRestoreJobPersistInfo &history_info,
    bool &restore_tenant_exist)
{
  int ret = OB_SUCCESS;
  restore_tenant_exist = true;
  ObSchemaGetterGuard schema_guard;
  bool tenant_dropped = false;
  ObHisRestoreJobPersistInfo user_history_info; 
  const uint64_t restore_tenant_id = job_info.get_tenant_id();
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_INVALID_TENANT_ID == restore_tenant_id) {
    //maybe failed to create tenant
    restore_tenant_exist = false;
    LOG_INFO("tenant maybe failed to create", KR(ret), K(job_info));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_SUCCESS != schema_guard.check_formal_guard()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("failed to check formal gurad", KR(ret), K(job_info));
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(restore_tenant_id, tenant_dropped))) {
    LOG_WARN("failed to check tenant is beed dropped", KR(ret), K(restore_tenant_id));
  } else if (tenant_dropped) {
    restore_tenant_exist = false;
    LOG_INFO("restore tenant has been dropped", KR(ret), K(job_info));
  } else {
    //check restore tenant's meta tenant is valid to read
    const share::schema::ObTenantSchema *tenant_schema = NULL;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(restore_tenant_id);
    if (OB_FAIL(schema_guard.get_tenant_info(meta_tenant_id, tenant_schema))) {
      LOG_WARN("failed to get tenant ids", KR(ret), K(meta_tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant not exist", KR(ret), K(meta_tenant_id));
    } else if (tenant_schema->is_normal()) {
      restore_tenant_exist = true;
    } else {
      //other status cannot get result from meta
      restore_tenant_exist = false;
      LOG_INFO("meta tenant of restore tenant not normal", KR(ret), KPC(tenant_schema));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!restore_tenant_exist) {
    if (OB_FAIL(history_info.init_with_job(job_info))) {
      LOG_WARN("failed to init with job", KR(ret), K(job_info));
    }
  } else if (OB_FAIL(ObRestoreUtil::get_user_restore_job_history(
                 *sql_proxy_, job_info.get_tenant_id(),
                 job_info.get_restore_key().tenant_id_, job_info.get_job_id(),
                 user_history_info))) {
    LOG_WARN("failed to get user restore job history", KR(ret), K(job_info));
  } else if (OB_FAIL(history_info.init_initiator_job_history(job_info, user_history_info))) {
    LOG_WARN("failed to init restore job history", KR(ret), K(job_info), K(user_history_info));
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
  } else if (OB_FAIL(check_tenant_can_restore_(tenant_id_))) {
    LOG_WARN("tenant cannot restore", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_SUCCESS != schema_guard.check_formal_guard()) {
    // skip
  } else if (OB_INVALID_TENANT_ID == job.get_tenant_id()) {
    //restore tenant may be failed to create, will to restore failed
  } else if (OB_FAIL(schema_guard.check_if_tenant_has_been_dropped(job.get_tenant_id(), is_dropped))) {
    LOG_WARN("fail to get tenant id", KR(ret), K(job));
  } else if (!is_dropped) {
    // skip
  } else {
    // 3. tenant has been dropped
    failed_ret = OB_TENANT_NOT_EXIST;
    LOG_WARN("[RESTORE] tenant has been dropped, try recycle job",
             KR(ret), K(tenant_id_));
  }
  if (OB_SUCC(ret) && OB_SUCCESS != failed_ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, failed_ret, job))) {
      LOG_WARN("fail to update job status", KR(ret), K(tmp_ret), K(failed_ret), K(job));
    }
  }
  return ret;
}

int ObRestoreScheduler::try_update_job_status(
    common::ObISQLClient &sql_client,
    int return_ret,
    const ObPhysicalRestoreJob &job,
    share::PhysicalRestoreMod mod)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", K(ret));
  } else if (OB_FAIL(restore_op.init(&sql_client, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail init", K(ret), K(tenant_id_));
  } else {
    PhysicalRestoreStatus next_status = get_next_status(return_ret, job.get_status());
    const common::ObCurTraceId::TraceId trace_id = *ObCurTraceId::get_trace_id();

    if (PHYSICAL_RESTORE_FAIL == next_status && OB_LS_RESTORE_FAILED != return_ret
        && OB_FAIL(restore_op.update_job_error_info(job.get_job_id(), return_ret, mod, trace_id, self_addr_))) {
      // if restore failed at wait ls, observer has record error info,
      // rs no need to record error info again.
      LOG_WARN("fail to update job error info", K(ret), K(job), K(return_ret), K(mod), K(tenant_id_));
    } else if (OB_FAIL(restore_op.update_job_status(job.get_job_id(), next_status))) {
      LOG_WARN("fail update job status", K(ret), K(job), K(next_status), K(tenant_id_));
    } else {
      //can not be zero
      restore_service_->wakeup();
      LOG_INFO("[RESTORE] switch job status", K(ret), K(job), K(next_status));
      (void)record_rs_event(job, next_status);
    }
  }
  return ret;
}

void ObRestoreScheduler::record_rs_event(
  const ObPhysicalRestoreJob &job,
  const PhysicalRestoreStatus status)
{
  const char *status_str = ObPhysicalRestoreTableOperator::get_restore_status_str(
                             static_cast<PhysicalRestoreStatus>(status));
  ROOTSERVICE_EVENT_ADD("physical_restore", "change_restore_status",
                        "job_id", job.get_job_id(),
                        "tenant", job.get_tenant_name(),
                        "status", status_str);
}

PhysicalRestoreStatus ObRestoreScheduler::get_sys_next_status(
  PhysicalRestoreStatus current_status)
{
  PhysicalRestoreStatus next_status = PHYSICAL_RESTORE_MAX_STATUS;
  switch (current_status) {
    case PHYSICAL_RESTORE_CREATE_TENANT : {
      next_status = PHYSICAL_RESTORE_WAIT_TENANT_RESTORE_FINISH;
      break;
    }
    case PHYSICAL_RESTORE_WAIT_TENANT_RESTORE_FINISH : {
       next_status = PHYSICAL_RESTORE_SUCCESS;
       break;
    }
    default : {
      // do nothing
    }
  }
  return next_status;
}



PhysicalRestoreStatus ObRestoreScheduler::get_next_status(
  int return_ret,
  PhysicalRestoreStatus current_status)
{
  PhysicalRestoreStatus next_status = PHYSICAL_RESTORE_MAX_STATUS;
  if (OB_SUCCESS != return_ret) {
    next_status = PHYSICAL_RESTORE_FAIL;
  } else if (is_sys_tenant(MTL_ID())) {
    next_status = get_sys_next_status(current_status);
  } else {
    switch (current_status) {
      case PHYSICAL_RESTORE_PRE : {
        next_status = PHYSICAL_RESTORE_CREATE_INIT_LS;
        break;
      }
      case PHYSICAL_RESTORE_CREATE_INIT_LS : {
        next_status = PHYSICAL_RESTORE_WAIT_CONSISTENT_SCN;
        break;
      }
      case PHYSICAL_RESTORE_WAIT_CONSISTENT_SCN : {
        next_status = PHYSICAL_RESTORE_WAIT_LS;
        break;
      }
      case PHYSICAL_RESTORE_WAIT_LS : {
        next_status = PHYSICAL_RESTORE_POST_CHECK;
        break;
      }
      case PHYSICAL_RESTORE_POST_CHECK : {
        next_status = PHYSICAL_RESTORE_UPGRADE;
        break;
      }
      case PHYSICAL_RESTORE_UPGRADE : {
        next_status = PHYSICAL_RESTORE_SUCCESS;
        break;
      }
      default : {
        // do nothing
      }
    }
  }
  return next_status;
}

int ObRestoreScheduler::restore_upgrade(const ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_UPGRADE_PRE);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id_
             || OB_SYS_TENANT_ID == tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else {
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
        LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
      }
    }
  }
  LOG_INFO("[RESTORE] upgrade pre finish", KR(ret), K(job_info));
  return ret;
}

int ObRestoreScheduler::restore_init_ls(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_INIT_LS);
  ObSchemaGetterGuard schema_guard;
  const share::schema::ObTenantSchema *tenant_schema = NULL;
  const common::ObSArray<share::ObBackupSetPath> &backup_set_path_array = 
    job_info.get_multi_restore_path_list().get_backup_set_path_list();
  const common::ObSArray<share::ObBackupPathString> &log_path_array = job_info.get_multi_restore_path_list().get_log_path_list();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_tenant_can_restore_(tenant_id_))) {
    LOG_WARN("tenant can not restore", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(job_info));
  } else if (OB_ISNULL(tenant_schema) || !tenant_schema->is_restore_tenant_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant not exist or tenant is not in physical restore status", KR(ret),
        K(tenant_schema));
  } else if (OB_UNLIKELY(0 == backup_set_path_array.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup piece path not expected", KR(ret), K(job_info));
  } else {
    const int64_t backup_path_count = backup_set_path_array.count();
    const ObString &backup_set_path = backup_set_path_array.at(backup_path_count - 1).ptr();
    storage::ObBackupDataLSAttrDesc backup_ls_attr;
    ObLogRestoreSourceMgr restore_source_mgr;
    storage::ObBackupDataStore store;
    if (OB_FAIL(store.init(backup_set_path.ptr()))) {
      LOG_WARN("fail to ini backup extern mgr", K(ret));
    } else if (OB_FAIL(store.read_ls_attr_info(backup_ls_attr))) {
      LOG_WARN("failed to read ls info", KR(ret));
    } else {
      const SCN &sync_scn = backup_ls_attr.backup_scn_;
      ObLSRecoveryStatOperator ls_recovery;
      const uint64_t exec_tenant_id = get_private_table_exec_tenant_id(tenant_id_);
      START_TRANSACTION(sql_proxy_, exec_tenant_id)
      LOG_INFO("start to create ls and set sync scn", K(sync_scn), K(backup_ls_attr), KR(ret));
      if (FAILEDx(ls_recovery.update_sys_ls_sync_scn(tenant_id_, trans, sync_scn))) {
        LOG_WARN("failed to update sync ls sync scn", KR(ret), K(sync_scn));
      }
       END_TRANSACTION(trans)
    }
    if (FAILEDx(create_all_ls_(*tenant_schema, backup_ls_attr.ls_attr_array_))) {
      LOG_WARN("failed to create all ls", KR(ret), K(backup_ls_attr), KPC(tenant_schema));
    } else if (OB_FAIL(wait_all_ls_created_(*tenant_schema, job_info))) {
      LOG_WARN("failed to wait all ls created", KR(ret), KPC(tenant_schema));
    } else if (OB_FAIL(finish_create_ls_(*tenant_schema, backup_ls_attr.ls_attr_array_))) {
      LOG_WARN("failed to finish create ls", KR(ret), KPC(tenant_schema));
    } else if (OB_FAIL(restore_source_mgr.init(tenant_id_, sql_proxy_))) {
      LOG_WARN("failed to init restore_source_mgr", KR(ret));
    } else if (1 == log_path_array.count() 
      && OB_FAIL(restore_source_mgr.add_location_source(job_info.get_restore_scn(), log_path_array.at(0).str()))) {
      LOG_WARN("failed to add log restore source", KR(ret), K(job_info), K(log_path_array));
    } else if (0 == log_path_array.count()) /*add restore source*/ {
      DirArray piece_dir_array;
      const common::ObSArray<share::ObBackupPiecePath> piece_array = job_info.get_multi_restore_path_list().get_backup_piece_path_list();
      ARRAY_FOREACH_X(piece_array, i, cnt, OB_SUCC(ret)) {
        ObBackupPiecePath piece_path = piece_array.at(i);
        if (OB_FAIL(piece_dir_array.push_back(piece_path))) {
          LOG_WARN("fail to push back", K(ret), K(piece_path), K(piece_dir_array));
        }
      }
      if (FAILEDx(restore_source_mgr.add_rawpath_source(job_info.get_restore_scn(), piece_dir_array))) {
        LOG_WARN("fail to add raw path source", K(ret), K(job_info), K(piece_dir_array));
      }
    }
  }

#ifdef ERRSIM
    ret = OB_E(EventTable::EN_RESTORE_CREATE_LS_FAILED) OB_SUCCESS;
#endif

  TenantRestoreStatus tenant_restore_status;
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(check_all_ls_restore_to_consistent_scn_finish_(tenant_id_, tenant_restore_status))) {
      LOG_WARN("failed to check all ls restore to consistent scn finish", K(ret));
    }
  }

  if (OB_SUCC(ret) || is_tenant_restore_failed(tenant_restore_status)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
      tmp_ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret), K(job_info));
    }
  }
  if (OB_FAIL(ret)) {
    restore_service_->wakeup();
  }
  LOG_INFO("[RESTORE] create init ls", KR(ret), K(job_info));

  return ret;
}

int ObRestoreScheduler::set_restore_to_target_scn_(
    common::ObMySQLTransaction &trans, const share::ObPhysicalRestoreJob &job_info, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = job_info.get_tenant_id();
  ObAllTenantInfo tenant_info;
  ObLSRecoveryStatOperator ls_recovery_operator;
  ObLSRecoveryStat sys_ls_recovery;
  ObLogRestoreSourceMgr restore_source_mgr;
  if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, &trans, true/*for_update*/, tenant_info))) {
    LOG_WARN("failed to load all tenant info", KR(ret), "tenant_id", job_info.get_tenant_id());
  } else if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, share::SYS_LS,
                     true /*for_update*/, sys_ls_recovery, trans))) {
    LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id));
  } else if (scn < tenant_info.get_sync_scn() || scn < sys_ls_recovery.get_sync_scn()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("recover before tenant sync_scn or SYS LS sync_scn is not allow", KR(ret), K(tenant_info),
             K(tenant_id), K(scn), K(sys_ls_recovery));
  } else if (tenant_info.get_recovery_until_scn() == scn) {
    LOG_INFO("recovery_until_scn is same with original", K(tenant_info), K(tenant_id), K(scn));
  } else if (OB_FAIL(restore_source_mgr.init(tenant_id, &trans))) {
    LOG_WARN("failed to init restore_source_mgr", KR(ret), K(tenant_id), K(scn));
  } else if (OB_FAIL(restore_source_mgr.update_recovery_until_scn(scn))) {
    LOG_WARN("failed to update_recovery_until_scn", KR(ret), K(tenant_id), K(scn));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_recovery_until_scn(
                  tenant_id, trans, tenant_info.get_switchover_epoch(), scn))) {
    LOG_WARN("failed to update_tenant_recovery_until_scn", KR(ret), K(tenant_id), K(scn));
  } else {
    LOG_INFO("succeed to set recover until scn", K(scn));
  }
  return ret;
}
int ObRestoreScheduler::create_all_ls_(
    const share::schema::ObTenantSchema &tenant_schema,
    const common::ObIArray<ObLSAttr> &ls_attr_array)
{
  int ret = OB_SUCCESS;
  ObLSStatusOperator status_op;
  ObLSStatusInfo status_info;
  ObAllTenantInfo tenant_info;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid()
        || 0 >= ls_attr_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema), K(ls_attr_array));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, sql_proxy_, false, tenant_info))) {
    LOG_WARN("failed to load tenant info", KR(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!tenant_info.is_normal_status())) {
    ret = OB_NEED_RETRY;
    LOG_WARN("the tenant's switchover status should be NORMAL", KR(ret), K(tenant_info));
  } else {
    common::ObMySQLTransaction trans;
    const int64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
    } else {
      //must be in trans
      //Multiple LS groups will be created here.
      //In order to ensure that each LS group can be evenly distributed in the unit group,
      //it is necessary to read the distribution of LS groups within the transaction.
      ObTenantLSInfo tenant_stat(sql_proxy_, &tenant_schema, tenant_id_, &trans);
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_attr_array.count(); ++i) {
        const ObLSAttr &ls_info = ls_attr_array.at(i);
        ObLSFlag ls_flag = ls_info.get_ls_flag();
        if (ls_info.get_ls_id().is_sys_ls()) {
        } else if (OB_SUCC(status_op.get_ls_status_info(tenant_id_, ls_info.get_ls_id(),
                status_info, trans))) {
          LOG_INFO("[RESTORE] ls already exist", K(ls_info), K(tenant_id_));
        } else if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get ls status info", KR(ret), K(tenant_id_), K(ls_info));
        } else if (OB_FAIL(ObLSServiceHelper::create_new_ls_in_trans(
                   ls_info.get_ls_id(), ls_info.get_ls_group_id(), ls_info.get_create_scn(),
                   tenant_info.get_switchover_epoch(), tenant_stat, trans, ls_flag))) {
          LOG_WARN("failed to add new ls status info", KR(ret), K(ls_info), K(tenant_info));
        }
        LOG_INFO("create init ls", KR(ret), K(ls_info));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

int ObRestoreScheduler::wait_all_ls_created_(const share::schema::ObTenantSchema &tenant_schema,
      const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    ObLSStatusOperator status_op;
    ObLSStatusInfoArray ls_array;
    palf::PalfBaseInfo palf_base_info;
    ObLSRecoveryStat recovery_stat;
    ObLSRecoveryStatOperator ls_recovery_operator;
    
    if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array,
                                                     *sql_proxy_))) {
      LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
  }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); ++i) {
      const ObLSStatusInfo &info = ls_array.at(i);
      if (info.ls_is_creating()) {
        recovery_stat.reset();
        if (OB_FAIL(ls_recovery_operator.get_ls_recovery_stat(tenant_id, info.ls_id_,
              false/*for_update*/, recovery_stat, *sql_proxy_))) {
          LOG_WARN("failed to get ls recovery stat", KR(ret), K(tenant_id), K(info));
        } else if (OB_FAIL(ObRestoreUtil::get_restore_ls_palf_base_info(
                job_info, info.ls_id_, palf_base_info))) {
          LOG_WARN("failed to get restore ls palf info", KR(ret), K(info),
                   K(job_info));
        } else if (OB_FAIL(ObCommonLSService::do_create_user_ls(
                       tenant_schema, info, recovery_stat.get_create_scn(),
                       true, /*create with palf*/
                       palf_base_info))) {
          LOG_WARN("failed to create ls with palf", KR(ret), K(info), K(tenant_schema),
                   K(palf_base_info));
        }
      }
    }// end for
    LOG_INFO("[RESTORE] wait ls created", KR(ret), K(tenant_id), K(ls_array));
  }
  return ret;
}

int ObRestoreScheduler::finish_create_ls_(
    const share::schema::ObTenantSchema &tenant_schema,
    const common::ObIArray<share::ObLSAttr> &ls_attr_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_schema));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else {
    const uint64_t tenant_id = tenant_schema.get_tenant_id();
    const int64_t exec_tenant_id = ObLSLifeIAgent::get_exec_tenant_id(tenant_id);
    common::ObMySQLTransaction trans;
    ObLSStatusOperator status_op;
    ObLSStatusInfoArray ls_array;
    ObLSStatus ls_info = share::OB_LS_EMPTY;//ls status in __all_ls
    if (OB_FAIL(status_op.get_all_ls_status_by_order(tenant_id, ls_array,
                                                     *sql_proxy_))) {
      LOG_WARN("failed to get all ls status", KR(ret), K(tenant_id));
    } else if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); ++i) {
        const ObLSStatusInfo &status_info = ls_array.at(i);
        if (OB_UNLIKELY(status_info.ls_is_creating())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls should be created", KR(ret), K(status_info));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          for (int64_t j = 0; OB_ENTRY_NOT_EXIST == ret && j < ls_attr_array.count(); ++j) {
            if (ls_attr_array.at(i).get_ls_id() == status_info.ls_id_) {
              ret = OB_SUCCESS;
              ls_info = ls_attr_array.at(i).get_ls_status();
            }
          }
          if (OB_FAIL(ret)) {
            LOG_WARN("failed to find ls in attr", KR(ret), K(status_info), K(ls_attr_array));
          } else if (share::OB_LS_CREATING == ls_info) {
            //no need to update
          } else if (ls_info == status_info.status_) {
            //no need update
          } else if (OB_FAIL(status_op.update_ls_status_in_trans(
                  tenant_id, status_info.ls_id_, status_info.status_,
                  ls_info, share::NORMAL_SWITCHOVER_STATUS, trans))) {
            LOG_WARN("failed to update status", KR(ret), K(tenant_id), K(status_info), K(ls_info));
          } else {
            LOG_INFO("[RESTORE] update ls status", K(tenant_id), K(status_info), K(ls_info));
          }
        }
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

int ObRestoreScheduler::restore_wait_to_consistent_scn(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  TenantRestoreStatus tenant_restore_status;
  const uint64_t tenant_id = job_info.get_tenant_id();
  const ObTenantSchema *tenant_schema = NULL;
  ObSchemaGetterGuard schema_guard;
  bool is_replay_finish = false;
  DEBUG_SYNC(BEFORE_WAIT_RESTORE_TO_CONSISTENT_SCN);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_tenant_can_restore_(tenant_id))) {
    LOG_WARN("failed to check tenant can restore", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema) || !tenant_schema->is_restore()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant not exist or tenant is not in physical restore status", KR(ret),
               KPC(tenant_schema));
  } else if (OB_FAIL(check_all_ls_restore_to_consistent_scn_finish_(tenant_id, tenant_restore_status))) {
    LOG_WARN("fail to check all ls restore finish", KR(ret), K(job_info));
  } else if (is_tenant_restore_finish(tenant_restore_status)) {
    LOG_INFO("[RESTORE] restore wait all ls restore to consistent scn done", K(tenant_id), K(tenant_restore_status));
    int tmp_ret = OB_SUCCESS;
    ObMySQLTransaction trans;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(job_info.get_tenant_id());
    if (is_tenant_restore_failed(tenant_restore_status)) {
      ret = OB_LS_RESTORE_FAILED;
      LOG_INFO("[RESTORE]restore wait all ls restore to consistent scn failed", K(ret));
      if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
        LOG_WARN("fail to update job status", KR(ret), K(job_info));
      }
    } else if (OB_FAIL(check_tenant_replay_to_consistent_scn(tenant_id, job_info.get_consistent_scn(), is_replay_finish))) {
      LOG_WARN("fail to check tenant replay to consistent scn", K(ret));
    } else if (!is_replay_finish) {
    } else if (FALSE_IT(DEBUG_SYNC(AFTER_WAIT_RESTORE_TO_CONSISTENT_SCN))) {
    } else if (OB_FAIL(trans.start(sql_proxy_, exec_tenant_id))) {
      LOG_WARN("fail to start trans", K(ret));
    } else if (OB_FAIL(stat_restore_progress_(trans, job_info, true/*is_restore_stat_start*/, false/*is_restore_finish*/))) {
      LOG_WARN("fail to stat restore progress", K(ret));
    } else if (OB_FAIL(set_restoring_start_ts_(trans, job_info))) {
      LOG_WARN("fail to set restoring start ts", K(ret));
    } else if (OB_FAIL(set_restore_to_target_scn_(trans, job_info, job_info.get_restore_scn()))) {
      LOG_WARN("fail to set restore to target scn", KR(ret));
    } else if (OB_FAIL(try_update_job_status(trans, ret, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), K(job_info));
    }
    if (trans.is_started()) {
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("fail to rollback trans", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}



int ObRestoreScheduler::check_tenant_replay_to_consistent_scn(const uint64_t tenant_id, const share::SCN &scn, bool &is_replay_finish)
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id, sql_proxy_, false/*no update*/, tenant_info))) {
    LOG_WARN("failed to load tenant info", K(ret));
  } else if (tenant_info.get_recovery_until_scn() != scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected recovery until scn", K(ret), K(tenant_info), K(scn));
  } else {
    is_replay_finish = (tenant_info.get_recovery_until_scn() <= tenant_info.get_readable_scn());
    LOG_INFO("[RESTORE]tenant replay to consistent_scn", K(is_replay_finish));
  }
  return ret;
}

int ObRestoreScheduler::restore_wait_ls_finish(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_PHYSICAL_RESTORE_WAIT_LS_FINISH);
  TenantRestoreStatus tenant_restore_status;
  const uint64_t tenant_id = job_info.get_tenant_id();
  const ObTenantSchema *tenant_schema = NULL;
  ObSchemaGetterGuard schema_guard;

  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(check_tenant_can_restore_(tenant_id))) {
    LOG_WARN("failed to check tenant can restore", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema) || !tenant_schema->is_restore_tenant_status()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant not exist or tenant is not in physical restore status", KR(ret),
               KPC(tenant_schema));
  } else if (OB_FAIL(check_all_ls_restore_finish_(tenant_id, tenant_restore_status))) {
    LOG_WARN("failed to check all ls restore finish", KR(ret), K(job_info));
  } else if (OB_FAIL(stat_restore_progress_(*sql_proxy_, job_info, false/*is_restore_stat_start*/, is_tenant_restore_success(tenant_restore_status)))) {
    LOG_WARN("fail to stat restore progress", K(ret));
  } else if (is_tenant_restore_finish(tenant_restore_status)) {
    LOG_INFO("[RESTORE] restore wait all ls finish done", K(tenant_id), K(tenant_restore_status));
    int tmp_ret = OB_SUCCESS;
    int tenant_restore_result = OB_LS_RESTORE_FAILED;
    if (is_tenant_restore_success(tenant_restore_status)) {
      tenant_restore_result = OB_SUCCESS;
    } 
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, tenant_restore_result, job_info))) {
      LOG_WARN("fail to update job status", KR(ret), KR(tmp_ret), KR(tenant_restore_result), K(job_info));
    }
  }
  return ret;
}

int ObRestoreScheduler::check_all_ls_restore_finish_(
    const uint64_t tenant_id,
    TenantRestoreStatus &tenant_restore_status)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else {
    tenant_restore_status = TenantRestoreStatus::SUCCESS;
    SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select a.ls_id, b.restore_status from %s as a "
              "left join %s as b on a.ls_id = b.ls_id",
              OB_ALL_LS_STATUS_TNAME, OB_ALL_LS_META_TABLE_TNAME))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(sql_proxy_->read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", KR(ret), K(sql));
      } else {
        int64_t ls_id = 0;
        share::ObLSRestoreStatus ls_restore_status;
        int32_t restore_status = -1;
        //TODO no ls in ls_meta
        //if one of ls restore failed, make tenant restore failed
        //
        while (OB_SUCC(ret) && OB_SUCC(result->next())
            && !is_tenant_restore_failed(tenant_restore_status)) {
          EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "restore_status", restore_status, int32_t);

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ls_restore_status.set_status(restore_status))) {
            LOG_WARN("failed to set status", KR(ret), K(restore_status));
          } else if (ls_restore_status.is_restore_failed()) {
            //restore failed
            tenant_restore_status = TenantRestoreStatus::FAILED;
          } else if (!ls_restore_status.is_restore_none()
                     && is_tenant_restore_success(tenant_restore_status)) {
            tenant_restore_status = TenantRestoreStatus::IN_PROGRESS;
          }
        } // while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (TenantRestoreStatus::SUCCESS != tenant_restore_status) {
          LOG_INFO("check all ls restore not finish, just wait", KR(ret),
              K(tenant_id), K(ls_id), K(tenant_restore_status));
        }
      }
    }
  }
  return ret;
}

int ObRestoreScheduler::check_all_ls_restore_to_consistent_scn_finish_(
    const uint64_t tenant_id,
    TenantRestoreStatus &tenant_restore_status)
{
  int ret = OB_SUCCESS;
  bool is_finished = false;
  bool is_success = false;
  ObPhysicalRestoreTableOperator restore_op;
  if (OB_FAIL(restore_op.init(sql_proxy_, tenant_id, share::OBCG_STORAGE/*group_id*/))) {
    LOG_WARN("fail init", K(ret), K(tenant_id_));
  } else if (OB_FAIL(restore_op.check_finish_restore_to_consistent_scn(is_finished, is_success))) {
    LOG_WARN("fail to check finish restore to consistent_scn", K(ret), K(tenant_id));
  } else if (!is_finished) {
    tenant_restore_status = TenantRestoreStatus::IN_PROGRESS;
  } else if (is_success) {
    tenant_restore_status = TenantRestoreStatus::SUCCESS;
  } else {
    tenant_restore_status = TenantRestoreStatus::FAILED;
  }

  if (OB_FAIL(ret)) {
  } else if (TenantRestoreStatus::SUCCESS != tenant_restore_status) {
    LOG_INFO("check all ls restore to consistent_scn not finish, just wait", KR(ret),
        K(tenant_id), K(tenant_restore_status));
  }

  return ret;
}

int ObRestoreScheduler::restore_wait_tenant_finish(const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_WAIT_RESTORE_TENANT_FINISH);
  //read tenant restore status from __all_restore_job_history
  ObPhysicalRestoreTableOperator restore_op;
  ObPhysicalRestoreJob tenant_job;
  ObHisRestoreJobPersistInfo user_job_history;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_FAIL(restore_service_->check_stop())) {
    LOG_WARN("restore scheduler stopped", KR(ret));
  } else if (OB_FAIL(restore_op.init(sql_proxy_, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail init", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObRestoreUtil::get_user_restore_job_history(
                 *sql_proxy_, job_info.get_tenant_id(),
                 job_info.get_restore_key().tenant_id_, job_info.get_job_id(),
                 user_job_history))) {
    LOG_WARN("failed to get user restore job", KR(ret), K(job_info));
  } else if (user_job_history.is_restore_success()) {
    const int64_t tenant_id = job_info.get_tenant_id();
    ObSchemaGetterGuard schema_guard;
    const ObTenantSchema *tenant_schema = NULL;

    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant schema is null", K(ret), K(tenant_id));
    } else if (tenant_schema->is_restore_tenant_status() || tenant_schema->is_normal()) {
      if (tenant_schema->is_restore_tenant_status()) {
        const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
        // try finish restore status
        obrpc::ObCreateTenantEndArg arg;
        arg.tenant_id_ = tenant_id;
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        if (OB_FAIL(restore_service_->check_stop())) {
          LOG_WARN("restore scheduler stopped", K(ret));
        } else if (OB_FAIL(rpc_proxy_->timeout(DEFAULT_TIMEOUT)
                               .create_tenant_end(arg))) {
          LOG_WARN("fail to create tenant end", K(ret), K(arg), K(DEFAULT_TIMEOUT));
        }
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
          LOG_WARN("fail to update job status", K(ret), K(tmp_ret),
                   K(job_info));
        }
      }
    } else {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("tenant status not match", K(ret), KPC(tenant_schema));
    }
  } else {
    //restore failed
    int tmp_ret = OB_SUCCESS;
    ret = OB_ERROR;
    if (OB_SUCCESS != (tmp_ret = try_update_job_status(*sql_proxy_, ret, job_info))) {
      LOG_WARN("fail to update job status", K(ret), K(tmp_ret),
          K(job_info));
    }
  }

  return ret;
}

int ObRestoreScheduler::reset_schema_status(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy));
  } else {
    ObSchemaStatusProxy proxy(*sql_proxy);
    ObRefreshSchemaStatus schema_status(tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);
    if (OB_FAIL(proxy.init())) {
      LOG_WARN("failed to init schema proxy", KR(ret));
    } else if (OB_FAIL(proxy.set_tenant_schema_status(schema_status))) {
      LOG_WARN("failed to update schema status", KR(ret), K(schema_status));
    }
  }
  return ret;
}

int ObRestoreScheduler::may_update_restore_concurrency_(const uint64_t new_tenant_id, const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  double cpu_count = 0;
  int64_t ha_high_thread_score = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(new_tenant_id));
  // restore concurrency controls the number of threads used by restore dag.
  // if cpu number is less than 10, use the default value.
  // if cpu number is between 10 ~ 100, let concurrency equals to the cpu number.
  // if cpu number is exceed 100,  let concurrency equals to 100.
  const int64_t LOW_CPU_LIMIT = 10;
  const int64_t MAX_CPU_LIMIT = 100;
  if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(job_info));
  } else if (tenant_config.is_valid() && OB_FALSE_IT(ha_high_thread_score = tenant_config->ha_high_thread_score)) {
  } else if (0 != ha_high_thread_score) {
    LOG_INFO("ha high thread score has been set", K(ha_high_thread_score));
  } else if (OB_FAIL(ObRestoreUtil::get_restore_tenant_cpu_count(*sql_proxy_, new_tenant_id, cpu_count))) {
    LOG_WARN("failed to get restore tenant cpu count", K(ret), K(new_tenant_id));
  } else {
    int64_t concurrency = job_info.get_concurrency();
    if (LOW_CPU_LIMIT < cpu_count && MAX_CPU_LIMIT >= cpu_count) {
      concurrency = std::max(static_cast<int64_t>(cpu_count), concurrency);
    } else if (MAX_CPU_LIMIT < cpu_count) {
      concurrency = MAX_CPU_LIMIT;
    }
    if (OB_FAIL(update_restore_concurrency_(job_info.get_tenant_name(), new_tenant_id, concurrency))) {
      LOG_WARN("failed to update restore concurrency", K(ret), K(job_info));
    }
  }
  return ret;
}

int ObRestoreScheduler::reset_restore_concurrency_(const uint64_t new_tenant_id, const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  const int64_t concurrency = 0;
  const ObString &tenant_name = job_info.get_tenant_name();
  if (!job_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(job_info));
  } else if (OB_FAIL(update_restore_concurrency_(tenant_name, new_tenant_id, concurrency))) {
    LOG_WARN("failed to update restore concurrency", K(ret), K(job_info));
  }
  return ret;
}

int ObRestoreScheduler::update_restore_concurrency_(const common::ObString &tenant_name,
    const uint64_t tenant_id, const int64_t concurrency)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(sql.append_fmt(
      "ALTER SYSTEM SET ha_high_thread_score = %ld TENANT = '%.*s'",
      concurrency, tenant_name.length(), tenant_name.ptr()))) {
    LOG_WARN("failed to append fmt", K(ret), K(tenant_name));
  } else if (OB_FAIL(sql_proxy_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(sql));
  } else {
    LOG_INFO("update restore concurrency", K(tenant_name), K(concurrency), K(sql));
  }
  return ret;
}

int ObRestoreScheduler::stat_restore_progress_(
    common::ObISQLClient &proxy,
    const share::ObPhysicalRestoreJob &job_info,
    const bool is_restore_stat_start,
    const bool is_restore_finish)
{
  int ret = OB_SUCCESS;
  share::ObRestorePersistHelper helper;
  ObArray<share::ObLSRestoreProgressPersistInfo> progress_array;
  int64_t total_tablet_cnt = 0;
  int64_t finished_tablet_cnt = 0;
  ObRestoreJobPersistKey job_key;
  ObRestoreProgressPersistInfo restore_progress;
  job_key.tenant_id_ = tenant_id_;
  job_key.job_id_ = job_info.get_job_id();

  if (OB_FAIL(helper.init(tenant_id_, share::OBCG_STORAGE))) {
    LOG_WARN("fail to init restore table helper", K(ret), K_(tenant_id));
  } else if (OB_FAIL(helper.get_all_ls_restore_progress(proxy, progress_array))) {
    LOG_WARN("fail to get all ls restore progress", K(ret));
  } else if (OB_FAIL(helper.get_restore_process(proxy, job_key, restore_progress))) {
    LOG_WARN("failed to get restore progress", K(ret), K(job_key));
  }

  for(int64_t i = 0; OB_SUCC(ret) && i < progress_array.count();) {
    share::ObLSRestoreProgressPersistInfo &ls_progress = progress_array.at(i);
    share::ObLSInfo ls_info;
    const ObLSReplica *leader_replica = nullptr;
    int64_t ls_replica_cnt = 0;
    int64_t total_tablet_replica_cnt = 0;
    int64_t finish_tablet_replica_cnt = 0;
    if (OB_FAIL(lst_operator_->get(GCONF.cluster_id,
                                   tenant_id_,
                                   ls_progress.key_.ls_id_,
                                   share::ObLSTable::COMPOSITE_MODE,
                                   ls_info))) {
      LOG_WARN("fail to get log stream info", K(ret), K(ls_progress));
    } else if (OB_FAIL(ls_info.find_leader(leader_replica))) {
      // the ls was deleted.
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to find leader replica", K(ret), K(ls_info));
      }
    }

    for(int64_t j = i; OB_SUCC(ret) && j < progress_array.count(); j++, i++) {
      share::ObLSRestoreProgressPersistInfo &cur = progress_array.at(j);
      if (cur.key_.ls_id_ != ls_progress.key_.ls_id_) {
        break;
      }

      const ObLSReplica *replica = nullptr;
      if (OB_ISNULL(leader_replica)) {
        // the ls was deleted.
        ++ls_replica_cnt;
        total_tablet_replica_cnt += cur.tablet_count_;
        // treat tablets on deleted ls as all finished.
        finish_tablet_replica_cnt += cur.tablet_count_;
      } else if (OB_FAIL(ls_info.find(cur.key_.addr_, replica))) {
        // the replica was not in member list, ignore the progress.
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to find replica", K(ret), K(cur.key_), K(ls_info));
        }
      } else if (replica->get_in_member_list()) {
        ++ls_replica_cnt;
        total_tablet_replica_cnt += cur.tablet_count_;
        finish_tablet_replica_cnt += cur.finish_tablet_count_;
      } else if (replica->get_in_learner_list()) {
        // filter learner replicas with flag
        common::ObMember learner_in_learner_list;
        if (OB_FAIL(leader_replica->get_learner_list().get_learner_by_addr(replica->get_server(), learner_in_learner_list))) {
          LOG_WARN("fail to get learner from leader learner_list", K(ret), KPC(leader_replica), KPC(replica));
        } else if (learner_in_learner_list.is_migrating()) {
          LOG_TRACE("ignore migrating replica", KPC(replica));
        } else {
          ++ls_replica_cnt;
          total_tablet_replica_cnt += cur.tablet_count_;
          finish_tablet_replica_cnt += cur.finish_tablet_count_;
        }
      }
    }

    if (ls_replica_cnt > 0) {
      total_tablet_cnt += total_tablet_replica_cnt / ls_replica_cnt;
      finished_tablet_cnt += finish_tablet_replica_cnt / ls_replica_cnt;
    }
  }

  if (is_restore_stat_start) {
    finished_tablet_cnt = 0;
  } else if (is_restore_finish) {
    total_tablet_cnt = restore_progress.tablet_count_;
    // correct result, force finished_tablet_cnt equal to total_tablet_cnt.
    finished_tablet_cnt = total_tablet_cnt;
  } else {
    total_tablet_cnt = restore_progress.tablet_count_;
    if (finished_tablet_cnt >= total_tablet_cnt) {
      LOG_INFO("finished_tablet_cnt is bigger than total_tablet_cnt.", K(job_key), K(total_tablet_cnt), K(finished_tablet_cnt));
      // If something wrong with the restore stat, let it keep to 99%.
      finished_tablet_cnt = total_tablet_cnt - 1;
    }
  }
  if (FAILEDx(helper.update_restore_process(proxy, job_key, total_tablet_cnt, finished_tablet_cnt))) {
    LOG_WARN("fail to update restore progress", K(ret), K(job_key), K(total_tablet_cnt), K(finished_tablet_cnt));
  }
  return ret;
}

int ObRestoreScheduler::set_restoring_start_ts_(common::ObISQLClient &proxy, const share::ObPhysicalRestoreJob &job_info)
{
  int ret = OB_SUCCESS;
  ObPhysicalRestoreTableOperator restore_op;
  const int64_t job_id = job_info.get_job_id();
  if (OB_FAIL(restore_op.init(&proxy, tenant_id_, share::OBCG_STORAGE /*group_id*/))) {
    LOG_WARN("fail to init", K(ret), K(tenant_id_));
  } else if (OB_FAIL(restore_op.update_restore_option(
          job_id, "restoring_start_ts", ObTimeUtility::current_time()))) {
    LOG_WARN("fail to update restoring_start_ts", K(ret), K(job_id), K(tenant_id_));
  }

  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
