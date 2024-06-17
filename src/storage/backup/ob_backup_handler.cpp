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

#include "lib/oblog/ob_log_module.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/backup/ob_backup_task.h"
#include "storage/backup/ob_backup_handler.h"
#include "share/backup/ob_backup_connectivity.h"

using namespace oceanbase::share;
using namespace oceanbase::storage;

namespace oceanbase {
namespace backup {

int ObBackupHandler::schedule_backup_meta_dag(const ObBackupJobDesc &job_desc, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const int64_t turn_id, const int64_t retry_id, const SCN &start_scn)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObBackupReportCtx report_ctx;
  report_ctx.location_service_ = GCTX.location_service_;
  report_ctx.sql_proxy_ = GCTX.sql_proxy_;
  report_ctx.rpc_proxy_ = GCTX.srv_rpc_proxy_;
  ObTenantDagScheduler *dag_scheduler = NULL;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy) || !job_desc.is_valid() || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id ||
      !backup_set_desc.is_valid() || !ls_id.is_valid() || turn_id <= 0 || retry_id < 0 || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(job_desc),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(ls_id),
        K(turn_id),
        K(retry_id),
        K(start_scn));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("failed to switch to tenant", K(ret), K(tenant_id));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else {
    ObLSBackupDagNetInitParam param;
    param.job_desc_ = job_desc;
    param.tenant_id_ = tenant_id;
    param.backup_set_desc_ = backup_set_desc;
    param.ls_id_ = ls_id;
    param.turn_id_ = turn_id;
    param.retry_id_ = retry_id;
    param.start_scn_ = start_scn;
    param.report_ctx_ = report_ctx;
    param.backup_data_type_.set_sys_data_backup();
    if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy, tenant_id, backup_dest, param.dest_id_))) {
      LOG_WARN("failed to get dest id", K(ret), K(backup_dest));
    } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObLSBackupMetaDagNet>(&param))) {
      LOG_WARN("failed to create log stream backup dag net", K(ret), K(param));
    } else {
      FLOG_INFO("success to create log stream backup dag net", K(ret), K(param));
    }
  }
  return ret;
}

int ObBackupHandler::schedule_backup_data_dag(const ObBackupJobDesc &job_desc, const ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const int64_t turn_id, const int64_t retry_id, const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObBackupReportCtx report_ctx;
  report_ctx.location_service_ = GCTX.location_service_;
  report_ctx.sql_proxy_ = GCTX.sql_proxy_;
  report_ctx.rpc_proxy_ = GCTX.srv_rpc_proxy_;
  ObTenantDagScheduler *dag_scheduler = NULL;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy) || !job_desc.is_valid() || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id
      || !backup_set_desc.is_valid() || turn_id <= 0 || retry_id < 0 || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(job_desc),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(turn_id),
        K(retry_id));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("failed to switch to tenant", K(ret), K(tenant_id));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else {
    ObLSBackupDagNetInitParam param;
    param.job_desc_ = job_desc;
    param.tenant_id_ = tenant_id;
    param.backup_set_desc_ = backup_set_desc;
    param.ls_id_ = ls_id;
    param.turn_id_ = turn_id;
    param.retry_id_ = retry_id;
    param.backup_data_type_ = backup_data_type;
    param.report_ctx_ = report_ctx;
    if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy, tenant_id, backup_dest, param.dest_id_))) {
      LOG_WARN("failed to get dest id", K(ret), K(backup_dest));
    } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObLSBackupDataDagNet>(&param))) {
      LOG_WARN("failed to create log stream backup dag net", K(ret), K(param));
    } else {
      FLOG_INFO("success to create log stream backup dag net", K(ret), K(param));
    }
  }
  return ret;
}

int ObBackupHandler::schedule_build_tenant_level_index_dag(const ObBackupJobDesc &job_desc,
    const share::ObBackupDest &backup_dest, const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc,
    const int64_t turn_id, const int64_t retry_id, const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObBackupReportCtx report_ctx;
  report_ctx.location_service_ = GCTX.location_service_;
  report_ctx.sql_proxy_ = GCTX.sql_proxy_;
  report_ctx.rpc_proxy_ = GCTX.srv_rpc_proxy_;
  ObTenantDagScheduler *dag_scheduler = NULL;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy) || !job_desc.is_valid() || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id ||
      !backup_set_desc.is_valid() || turn_id <= 0 || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(job_desc),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(turn_id),
        K(backup_data_type));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("failed to switch to tenant", K(ret), K(tenant_id));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  } else {
    ObLSBackupDagNetInitParam param;
    param.job_desc_ = job_desc;
    param.tenant_id_ = tenant_id;
    param.backup_set_desc_ = backup_set_desc;
    param.ls_id_ = ObLSID(0);
    param.turn_id_ = turn_id;
    param.retry_id_ = retry_id;
    param.backup_data_type_ = backup_data_type;
    param.report_ctx_ = report_ctx;
    if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy, tenant_id, backup_dest, param.dest_id_))) {
      LOG_WARN("failed to get dest id", K(ret), K(backup_dest));
    } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObBackupBuildTenantIndexDagNet>(&param))) {
      LOG_WARN("failed to create log stream backup dag net", K(ret), K(param));
    } else {
      FLOG_INFO("success to create log stream backup dag net", K(ret), K(param));
    }
  }
  return ret;
}

int ObBackupHandler::schedule_backup_complement_log_dag(const ObBackupJobDesc &job_desc,
    const share::ObBackupDest &backup_dest, const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc,
    const share::ObLSID &ls_id, const SCN &start_scn, const SCN &end_scn, const bool is_only_calc_stat)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObBackupReportCtx report_ctx;
  report_ctx.location_service_ = GCTX.location_service_;
  report_ctx.sql_proxy_ = GCTX.sql_proxy_;
  report_ctx.rpc_proxy_ = GCTX.srv_rpc_proxy_;
  ObLSBackupDagNetInitParam param;
  ObTenantDagScheduler *dag_scheduler = NULL;
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy) ||!job_desc.is_valid() || !backup_dest.is_valid() || OB_INVALID_ID == tenant_id ||
      !backup_set_desc.is_valid() || !ls_id.is_valid() || !start_scn.is_valid() || !end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args",
        K(ret),
        K(job_desc),
        K(backup_dest),
        K(tenant_id),
        K(backup_set_desc),
        K(ls_id),
        K(start_scn),
        K(end_scn));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("failed to switch to tenant", K(ret), K(tenant_id));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag scheduler must not be NULL", K(ret));
  }else {
    ObLSBackupDagNetInitParam param;
    param.job_desc_ = job_desc;
    param.tenant_id_ = tenant_id;
    param.backup_set_desc_ = backup_set_desc;
    param.ls_id_ = ls_id;
    param.turn_id_ = 1;   // turn_id no use for complement log
    param.retry_id_ = 0;  // retry id no use for complement log
    param.compl_start_scn_ = start_scn;
    param.compl_end_scn_ = end_scn;
    param.is_only_calc_stat_ = is_only_calc_stat;
    param.report_ctx_ = report_ctx;
    if (OB_FAIL(param.backup_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to deep copy backup dest", K(ret), K(backup_dest));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy, tenant_id, backup_dest, param.dest_id_))) {
      LOG_WARN("failed to get dest id", K(ret), K(backup_dest));
    } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObLSBackupComplementLogDagNet>(&param))) {
      LOG_WARN("failed to create log stream backup dag net", K(ret), K(param));
    } else {
      FLOG_INFO("success to create log stream backup dag net", K(ret), K(param));
    }
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
