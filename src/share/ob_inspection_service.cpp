/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_inspection_service.h"

#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_share_util.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_tenant_ddl_service.h"
#include "share/ob_global_stat_proxy.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_zone_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
using namespace schema;

int ObInspectionCancelCheckerMTL::check_cancel()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObAddr leader_addr;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.location_service_ is null", KR(ret));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(
                     GCONF.cluster_id,
                     tenant_id,
                     SYS_LS,
                     false/*force_renew*/,
                     leader_addr))) {
    LOG_WARN("failed to get leader", KR(ret), K(tenant_id));
  } else if (leader_addr != GCONF.self_addr_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("leader changed", KR(ret), K(leader_addr));
  }
  return ret;
}

int ObInspectionCancelCheckerSelfRS::check_cancel()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (!GCTX.root_service_->is_full_service()) {
    ret = OB_CANCELED;
  }
  return ret;
}

ObInspectionCancelCheckerRemoteRS::ObInspectionCancelCheckerRemoteRS(const obrpc::ObCheckSysTableSchemaArg &arg)
  : arg_(arg)
{
  origin_rs_epoch_id_ = ObHeartbeatHandler::get_rs_epoch_id();
}

ERRSIM_POINT_DEF(ERRSIM_SKIP_CHECK_LEADER);
int ObInspectionCancelCheckerRemoteRS::check_cancel()
{
  int ret = OB_SUCCESS;
  ObAddr rs_addr;
  ObAddr leader_addr;
  DEBUG_SYNC(BEFORE_INSPECTION_CHECK_CANCEL);
  int64_t current_rs_epoch_id = ObHeartbeatHandler::get_rs_epoch_id();
  if (OB_ISNULL(GCTX.rs_mgr_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.rs_mgr_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("failed to get master root server", KR(ret));
  } else if (rs_addr != arg_.get_rs_addr()) {
    ret = OB_RS_SHUTDOWN;
    LOG_WARN("rs changed", KR(ret), K(rs_addr), K(arg_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, arg_.get_tenant_id(), SYS_LS,
        false/*force_renew*/, leader_addr))) {
    LOG_WARN("failed to get leader", KR(ret), K(arg_));
  } else if (leader_addr != GCONF.self_addr_) {
    ret = OB_NOT_MASTER;
    LOG_WARN("leader changed", KR(ret), K(arg_), K(leader_addr));
  }
  if (ERRSIM_SKIP_CHECK_LEADER) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else if (current_rs_epoch_id != palf::INVALID_PROPOSAL_ID) {
    // origin_rs_epoch_id_ is used to check rs not changed since receiving RPC which is used for hotfix commit
    // arg_.get_rs_epoch_id is used to check rs not changed since rs send RPC
    if ((origin_rs_epoch_id_ != palf::INVALID_PROPOSAL_ID && origin_rs_epoch_id_ != current_rs_epoch_id)
        || (arg_.get_rs_epoch_id() != palf::INVALID_PROPOSAL_ID && arg_.get_rs_epoch_id() != current_rs_epoch_id)) {
      ret = OB_RS_SHUTDOWN;
      LOG_WARN("leader changed", KR(ret), K(arg_), K(current_rs_epoch_id), K_(origin_rs_epoch_id));
    }
  }
  return ret;
}

ObInspectionService::ObInspectionService()
  : is_inited_(false),
    mutex_(common::ObLatchIds::OB_INSPECTION_SERVICE_LOCK),
    sys_stat_passed_(false),
    sys_param_passed_(false),
    sys_table_schema_passed_(false),
    data_version_passed_(false),
    all_checked_(false)
{
}

int ObInspectionService::mtl_init(ObInspectionService *&inspection_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(inspection_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inspection_service is null", KR(ret));
  } else {
    ret = inspection_service->init();
  }
  return ret;
}

int ObInspectionService::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inspection service is already init", KR(ret));
  } else {
    is_inited_ = true;
    ATOMIC_STORE(&sys_stat_passed_, false);
    ATOMIC_STORE(&sys_param_passed_, false);
    ATOMIC_STORE(&sys_table_schema_passed_, false);
    ATOMIC_STORE(&data_version_passed_, false);
    ATOMIC_STORE(&all_checked_, false);
  }
  return ret;
}

int ObInspectionService::run_inspection()
{
  int ret = OB_SUCCESS;
  static constexpr int64_t INSPECTION_LOCK_TIMEOUT_US = 10 * 1000 * 1000L; // 10s
  const uint64_t tenant_id = MTL_ID();
  bool is_primary = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("inspection service is not init", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.schema_service_ is null", KR(ret));
  } else if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
    LOG_WARN("fail to check tenant role is primary", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!GCTX.schema_service_->is_tenant_full_schema(OB_SYS_TENANT_ID))) {
    ret = OB_EAGAIN;
    LOG_WARN("sys tenant schema is not ready, try again", KR(ret));
  } else if (OB_UNLIKELY(!GCTX.schema_service_->is_tenant_full_schema(tenant_id))) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant schema is not ready, try again", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mutex_.lock(ObTimeUtility::current_time() + INSPECTION_LOCK_TIMEOUT_US))) {
    LOG_WARN("run_inspection lock failed", KR(ret), K(tenant_id), K(INSPECTION_LOCK_TIMEOUT_US));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObInspectionCancelCheckerMTL cancel_checker;
    ObInspector inspector(tenant_id, cancel_checker);

    // check sys stat
    tmp_ret = OB_SUCCESS;
    if (is_primary) {
      if (OB_TMP_FAIL(inspector.check_sys_stat())) {
        LOG_WARN("check_sys_stat failed", KR(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
    ATOMIC_STORE(&sys_stat_passed_, (OB_SUCCESS == tmp_ret));

    // check sys param
    tmp_ret = OB_SUCCESS;
    if (is_primary) {
      if (OB_TMP_FAIL(inspector.check_sys_param())) {
        LOG_WARN("check_sys_param failed", KR(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
    ATOMIC_STORE(&sys_param_passed_, (OB_SUCCESS == tmp_ret));

    // check sys table schemas
    tmp_ret = OB_SUCCESS;
    if (is_primary) {
      if (OB_TMP_FAIL(inspector.check_sys_table_schemas())) {
        LOG_WARN("check_sys_table_schemas failed", KR(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
    ATOMIC_STORE(&sys_table_schema_passed_, (OB_SUCCESS == tmp_ret));

    // check data version
    tmp_ret = OB_SUCCESS;
    if (is_primary && !GCONF.in_upgrade_mode()) {
      if (OB_TMP_FAIL(inspector.check_data_version())) {
        LOG_WARN("check_data_version failed", KR(tmp_ret));
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
    ATOMIC_STORE(&data_version_passed_, (OB_SUCCESS == tmp_ret));

    ATOMIC_STORE(&all_checked_, true);

    const bool sys_stat_passed = ATOMIC_LOAD(&sys_stat_passed_);
    const bool sys_param_passed = ATOMIC_LOAD(&sys_param_passed_);
    const bool sys_table_schema_passed = ATOMIC_LOAD(&sys_table_schema_passed_);
    const bool data_version_passed = ATOMIC_LOAD(&data_version_passed_);
    LOG_INFO("inspection service run finished", KR(ret), K(tenant_id),
             K(sys_stat_passed), K(sys_param_passed), K(sys_table_schema_passed),
             K(data_version_passed));

    mutex_.unlock();
  }
  return ret;
}

bool ObInspectionService::is_sys_stat_passed() const
{
  return ATOMIC_LOAD(&sys_stat_passed_);
}

bool ObInspectionService::is_sys_param_passed() const
{
  return ATOMIC_LOAD(&sys_param_passed_);
}

bool ObInspectionService::is_sys_table_schema_passed() const
{
  return ATOMIC_LOAD(&sys_table_schema_passed_);
}

bool ObInspectionService::is_data_version_passed() const
{
  return ATOMIC_LOAD(&data_version_passed_);
}

bool ObInspectionService::is_all_checked() const
{
  return ATOMIC_LOAD(&all_checked_);
}

void ObInspectionService::destroy()
{
  is_inited_ = false;
  ATOMIC_STORE(&sys_stat_passed_, false);
  ATOMIC_STORE(&sys_param_passed_, false);
  ATOMIC_STORE(&sys_table_schema_passed_, false);
  ATOMIC_STORE(&data_version_passed_, false);
  ATOMIC_STORE(&all_checked_, false);
}

int ObInspector::check_sys_stat()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_;
  ObArray<const char *> sys_stat_names;
  ObSqlString extra_cond;
  rootserver::ObSysStat sys_stat;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(check_tenant_status(tenant_id))) {
    LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sys_stat.set_initial_values(tenant_id))) {
    LOG_WARN("set initial values failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(extra_cond.assign_fmt("tenant_id = %lu",
             ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("extra_cond assign_fmt failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_names(sys_stat.item_list_, sys_stat_names))) {
    LOG_WARN("get sys stat names failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_names(tenant_id, OB_ALL_SYS_STAT_TNAME, sys_stat_names, extra_cond))) {
    LOG_WARN("check all sys stat names failed", KR(ret), K(tenant_id),
             "table_name", OB_ALL_SYS_STAT_TNAME, K(sys_stat_names));
  }
  return ret;
}

int ObInspector::check_sys_param()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_;
  ObArray<const char *> sys_param_names;
  ObSqlString extra_cond;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(check_tenant_status(tenant_id))) {
    LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_FAIL(extra_cond.assign_fmt("tenant_id = %lu",
             ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("extra_cond assign_fmt failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_sys_param_names(sys_param_names))) {
    LOG_WARN("get sys param names failed", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_names(tenant_id, OB_ALL_SYS_VARIABLE_TNAME,
             sys_param_names, extra_cond))) {
    LOG_WARN("check all sys params names failed", KR(ret), K(tenant_id),
             "table_name", OB_ALL_SYS_VARIABLE_TNAME, K(sys_param_names), K(extra_cond));
  }
  if (OB_SCHEMA_ERROR != ret) {
  } else if (need_ignore_error_message(tenant_id)) {
    LOG_WARN("check sys_variable failed", KR(ret));
  } else {
    LOG_DBA_ERROR(OB_ERR_ROOT_INSPECTION, "msg", "system variables are unmatched", KR(ret));
  }
  return ret;
}

int ObInspector::check_sys_table_schemas()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> error_table_ids;
  if (OB_FAIL(check_sys_table_schemas(error_table_ids))) {
    LOG_WARN("failed to check sys table schemas", KR(ret));
  } else if (OB_FAIL(check_error_table_ids(tenant_id_, error_table_ids))) {
    LOG_WARN("failed to check error table ids", KR(ret));
  }
  return ret;
}

#define PRINT_TABLE_INFO(table) "table_id", table.get_table_id(), "table_name", table.get_table_name()
int ObInspector::check_sys_table_schemas(ObIArray<uint64_t> &error_table_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_;
  int64_t schema_version = OB_INVALID_VERSION;
  const schema_create_func *creator_ptr_array[] = {
    share::all_core_table_schema_creator,
    share::core_table_schema_creators,
    share::sys_table_schema_creators,
    share::virtual_table_schema_creators,
    share::sys_view_schema_creators,
    share::core_index_table_schema_creators,
    share::sys_index_table_schema_creators,
    NULL };

  ObTableSchema table_schema;
  bool exist = false;
  for (const schema_create_func **creator_ptr_ptr = creator_ptr_array;
        OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr_ptr); ++creator_ptr_ptr) {
    for (const schema_create_func *creator_ptr = *creator_ptr_ptr;
          OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
      table_schema.reset();
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_FAIL(check_tenant_status(tenant_id))) {
        LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
      } else if (OB_FAIL((*creator_ptr)(table_schema))) {
        LOG_WARN("create table schema failed", KR(ret));
      } else if (!is_sys_tenant(tenant_id)
                  && OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                            tenant_id, table_schema))) {
        LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
                  tenant_id, table_schema, exist))) {
        LOG_WARN("fail to check inner table exist",
                  KR(ret), K(tenant_id), K(table_schema));
      } else if (!exist) {
        // skip
        // OB_SCHEMA_ERROR will not be returned here
      } else if (OB_FAIL(check_single_table(tenant_id, table_schema, error_table_ids))) {
        LOG_WARN("failed to check_single_table", KR(ret), PRINT_TABLE_INFO(table_schema));
      }
    } // end for
  } // end for
  if (error_table_ids.empty()) {
  } else if (need_ignore_error_message(tenant_id)) {
    LOG_WARN("check sys table schema failed", KR(ret), K(tenant_id), K(error_table_ids));
  } else {
    LOG_ERROR("check sys table schema failed", KR(ret), K(tenant_id), K(error_table_ids));
    LOG_DBA_ERROR(OB_ERR_ROOT_INSPECTION, "msg", "inner tables are unmatched", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObInspector::check_data_version()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_id_;
  if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(check_tenant_status(tenant_id))) {
    LOG_WARN("fail to check tenant status", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret));
  } else {
    ObGlobalStatProxy proxy(*GCTX.sql_proxy_, tenant_id);
    uint64_t target_data_version = 0;
    uint64_t current_data_version = 0;
    uint64_t compatible_version = 0;
    bool for_update = false;
    if (OB_FAIL(proxy.get_target_data_version(for_update, target_data_version))) {
      LOG_WARN("fail to get target data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.get_current_data_version(current_data_version))) {
      LOG_WARN("fail to get current data version", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compatible_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    } else if (target_data_version != current_data_version
               || target_data_version != compatible_version
               || target_data_version != DATA_CURRENT_VERSION) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("data_version not match, upgrade process should be run",
               KR(ret), K(tenant_id), K(target_data_version),
               K(current_data_version), K(compatible_version));
    }
  }
  return ret;
}

int ObInspector::check_tenant_status(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  const ObSimpleTenantSchema *tenant = NULL;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(guard.get_tenant_info(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant)) {
    // tenant may has been dropped;
    ret = OB_EAGAIN;
    LOG_WARN("tenant may be dropped, don't continue", KR(ret), K(tenant_id));
  } else if (!tenant->is_normal()) {
    ret = OB_EAGAIN;
    LOG_WARN("tenant status is not noraml, should check next round", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_refreshed_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
  } else if (!ObSchemaService::is_formal_version(schema_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("schema version is not formal, observer may be restarting or inner table schema changed, "
             "should check next round", KR(ret), K(tenant_id), K(schema_version));
  }
  return ret;
}

template<typename Item>
int ObInspector::get_names(const ObDList<Item> &list, ObIArray<const char*> &names)
{
  int ret = OB_SUCCESS;
  if (list.get_size() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("list is empty", KR(ret));
  } else {
    const Item *it = list.get_first();
    while (OB_SUCCESS == ret && it != list.get_header()) {
      if (OB_FAIL(names.push_back(it->name_))) {
        LOG_WARN("push_back failed", KR(ret));
      } else {
        it = it->get_next();
      }
    }
  }
  return ret;
}

int ObInspector::get_sys_param_names(ObIArray<const char *> &names)
{
  int ret = OB_SUCCESS;
  const int64_t param_count = ObSysVariables::get_amount();
  for (int64_t i = 0; OB_SUCC(ret) && i < param_count; ++i) {
    if (OB_FAIL(names.push_back(ObSysVariables::get_name(i).ptr()))) {
      LOG_WARN("push_back failed", KR(ret));
    }
  }
  return ret;
}

int ObInspector::check_names(
  const uint64_t tenant_id,
  const char *table_name,
  const ObIArray<const char *> &names,
  const ObSqlString &extra_cond)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_name) || names.count() <= 0) {
    // extra_cond can be empty, so wo don't check it here
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is null or names is empty", KP(table_name), K(names), KR(ret));
  } else {
    ObArray<Name> fetch_names; // Get the data of the internal table
    ObArray<Name> extra_names; // data inner table more than hard code
    ObArray<Name> miss_names; // data inner table less than hard code
    if (OB_FAIL(calc_diff_names(tenant_id, table_name, names, extra_cond,
        fetch_names, extra_names, miss_names))) {
      LOG_WARN("fail to calc diff names", KR(ret), KP(table_name), K(names), K(extra_cond));
    } else {
      if (fetch_names.count() <= 0) {
        // don't need to set ret
        LOG_WARN("maybe tenant or zone has been deleted, ignore it",
                 K(tenant_id), K(table_name), K(extra_cond));
      } else {
        if (extra_names.count() > 0) {
          // don't need to set ret
          LOG_WARN("some item exist in table, but not hard coded",
                   K(tenant_id), K(table_name), K(extra_names));
        }
        if (miss_names.count() > 0) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("some item exist in hard code, but not exist in inner table",
                   KR(ret), K(tenant_id), K(table_name), K(miss_names));
        }
      }
    }
  }
  return ret;
}

int ObInspector::calc_diff_names(
  const uint64_t tenant_id,
  const char *table_name,
  const ObIArray<const char *> &names,
  const ObSqlString &extra_cond,
  ObIArray<Name> &fetch_names, /* data reading from inner table*/
  ObIArray<Name> &extra_names, /* data inner table more than hard code*/
  ObIArray<Name> &miss_names /* data inner table less than hard code*/)
{
  int ret = OB_SUCCESS;
  fetch_names.reset();
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(table_name) || names.count() <= 0) {
    // extra_cond can be empty, don't need to check it
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is null or names is empty",
             KR(ret), K(tenant_id), KP(table_name), K(names));
  }

  if (OB_SUCC(ret)) {
    const uint64_t exec_tenant_id = tenant_id;
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt("SELECT name FROM %s%s%s", table_name,
        (extra_cond.empty()) ? "" : " WHERE ", extra_cond.ptr()))) {
      LOG_WARN("append_fmt failed", KR(ret), K(tenant_id), K(table_name), K(extra_cond));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(GCTX.sql_proxy_->read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is not expected to be NULL",
                   KR(ret), K(tenant_id), "result", OB_P(result));
        } else {
          //only for filling the out parameter,
          //Ensure that there is no '\ 0' character in the middle of the corresponding string
          int64_t tmp_real_str_len = 0;
          Name name;
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("get next result failed", KR(ret), K(tenant_id));
              }
            } else {
              EXTRACT_STRBUF_FIELD_MYSQL(*result, "name", name.ptr(),
                  static_cast<int64_t>(NAME_BUF_LEN), tmp_real_str_len);
              (void) tmp_real_str_len; // make compiler happy
              if (OB_FAIL(fetch_names.push_back(name))) {
                LOG_WARN("push_back failed", KR(ret), K(tenant_id));
              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (fetch_names.count() <= 0) {
        LOG_WARN("maybe tenant or zone has been deleted, ignore it",
                 KR(ret), K(table_name), K(extra_cond));
      } else {
        extra_names.reset();
        miss_names.reset();
        FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCCESS == ret) {
          bool found = false;
          FOREACH_CNT_X(name, names, OB_SUCC(ret)) {
            if (Name(*name) == *fetch_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (OB_FAIL(extra_names.push_back(*fetch_name))) {
              LOG_WARN("fail to push name into fetch_names",
                       KR(ret), K(tenant_id), K(*fetch_name), K(fetch_names));
            }
          }
        }
        FOREACH_CNT_X(name, names, OB_SUCCESS == ret) {
          bool found = false;
          FOREACH_CNT_X(fetch_name, fetch_names, OB_SUCCESS == ret) {
            if (Name(*name) == *fetch_name) {
              found = true;
              break;
            }
          }
          if (!found) {
            if (OB_FAIL(miss_names.push_back(Name(*name)))) {
              LOG_WARN("fail to push name into miss_names",
                       KR(ret), K(tenant_id), K(*name), K(miss_names));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObInspector::check_error_table_ids(
  const uint64_t tenant_id,
  const ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  if (table_ids.empty()) {
  } else {
    ret = OB_SCHEMA_ERROR;
    if (ObInspector::need_ignore_error_message(tenant_id)) {
      LOG_WARN("tenant schema error", KR(ret), K(tenant_id), K(table_ids));
    } else {
      LOG_ERROR("check sys table schema failed", KR(ret), K(tenant_id), K(table_ids));
      LOG_DBA_ERROR(OB_ERR_ROOT_INSPECTION, "msg", "inner tables are unmatched", KR(ret),
          K(tenant_id), K(table_ids));
    }
  }
  return ret;
}

bool ObInspector::need_ignore_error_message(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool ignore = false;
  bool in_compatibility_mode = false;
  if (GCONF.in_upgrade_mode()) {
    LOG_INFO("in upgrade mode, ignore root inspection error message", KR(ret),
        K(GCONF.in_upgrade_mode()));
    ignore = true;
  } else if (OB_FAIL(check_in_compatibility_mode(tenant_id, in_compatibility_mode))) {
    LOG_WARN("failed to check compatible", KR(ret), K(tenant_id));
  } else if (in_compatibility_mode) {
    LOG_INFO("compatible not change to DATA_CURRENT_VERSION, ignore root inspection error message",
        KR(ret), K(tenant_id), K(in_compatibility_mode));
    ignore = true;
  }
  return ignore;
}

int ObInspector::check_in_compatibility_mode(
  const uint64_t tenant_id,
  bool &in_compatibility_mode)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get data version", KR(ret), K(tenant_id));
  } else {
    in_compatibility_mode = (data_version < DATA_CURRENT_VERSION);
  }
  return ret;
}

int ObInspector::check_single_table(
  const uint64_t tenant_id,
  const ObTableSchema &hard_code_table,
  ObIArray<uint64_t> &error_table_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(!hard_code_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(hard_code_table), K(tenant_id));
  } else {
    if (OB_TMP_FAIL(check_table_schema(tenant_id, hard_code_table))) {
      // don't print table_schema, otherwise log will be too much
      LOG_WARN("check table schema failed", KR(tmp_ret), K(tenant_id), PRINT_TABLE_INFO(hard_code_table));
      if (OB_SCHEMA_ERROR != tmp_ret) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(error_table_ids.push_back(hard_code_table.get_table_id()))) {
        LOG_WARN("failed to push_back error table id", KR(ret), PRINT_TABLE_INFO(hard_code_table));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_TMP_FAIL(check_sys_view(tenant_id, hard_code_table))) {
      LOG_WARN("check sys view failed", KR(tmp_ret), K(tenant_id), PRINT_TABLE_INFO(hard_code_table));
      // sql may has occur other error except OB_SCHEMA_ERROR, we should not continue is such situation.
      if (OB_SCHEMA_ERROR != tmp_ret) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_FAIL(error_table_ids.push_back(hard_code_table.get_table_id()))) {
        LOG_WARN("failed to push_back error table id", KR(ret), PRINT_TABLE_INFO(hard_code_table));
      }
    }
  }
  return ret;
}

int ObInspector::check_table_schema(
  const uint64_t tenant_id,
  const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table = NULL;
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_UNLIKELY(
             is_virtual_tenant_id(tenant_id)
             || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!hard_code_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", KR(ret), K(tenant_id), K(hard_code_table));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
             tenant_id, hard_code_table.get_table_id(), table))) {
    LOG_WARN("get_table_schema failed", KR(ret), K(tenant_id), PRINT_TABLE_INFO(hard_code_table));
  } else if (OB_ISNULL(table)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table should not be null", KR(ret), K(tenant_id), KP(table), PRINT_TABLE_INFO(hard_code_table));
  } else if (OB_FAIL(check_table_schema(hard_code_table, *table))) {
    LOG_WARN("fail to check table schema", KR(ret), K(tenant_id), K(hard_code_table), KPC(table));
  }
  return ret;
}

int ObInspector::check_table_schema(
  const ObTableSchema &hard_code_table,
  const ObTableSchema &inner_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!hard_code_table.is_valid()
      || !inner_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", K(hard_code_table), K(inner_table), KR(ret));
  } else if (OB_FAIL(check_table_options(inner_table, hard_code_table))) {
    LOG_WARN("check_table_options failed", PRINT_TABLE_INFO(hard_code_table), KR(ret));
  } else if (!inner_table.is_view_table()) { //view table do not check column info
    if (hard_code_table.get_column_count() != inner_table.get_column_count()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("column count mismatch", PRINT_TABLE_INFO(inner_table),
          "table_column_cnt",inner_table.get_column_count(),
          "hard_code_table_column_cnt", hard_code_table.get_column_count(), KR(ret));
    } else {
      int back_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_table.get_column_count(); ++i) {
        const ObColumnSchemaV2 *hard_code_column = hard_code_table.get_column_schema_by_idx(i);
        const ObColumnSchemaV2 *column = NULL;
        if (OB_ISNULL(hard_code_column)) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("hard_code_column is null", "hard_code_column", OB_P(hard_code_column), KR(ret));
        } else if (OB_ISNULL(column = inner_table.get_column_schema(
            hard_code_column->get_column_name()))) {
          ret = OB_SCHEMA_ERROR;
          LOG_WARN("hard code column not found", PRINT_TABLE_INFO(hard_code_table),
              "column", hard_code_column->get_column_name(), KR(ret));
        } else {
          if (OB_FAIL(check_column_schema(hard_code_table.get_table_name(),
              *column, *hard_code_column))) {
            LOG_WARN("column schema mismatch with hard code column schema",
                PRINT_TABLE_INFO(inner_table), "column", *column,
                "hard_code_column", *hard_code_column, KR(ret));
          }
        }
        back_ret = OB_SUCCESS == back_ret ? ret : back_ret;
        ret = OB_SUCCESS;
      }
      ret = back_ret;
    }
  }
  return ret;
}

#undef PRINT_TABLE_INFO

int ObInspector::check_sys_view(
  const uint64_t tenant_id,
  const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.oracle_sql_proxy_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(GCTX.oracle_sql_proxy_), KP(GCTX.sql_proxy_));
  } else if (hard_code_table.is_view_table()) {
    // check view definition
    const ObString &table_name = hard_code_table.get_table_name();
    const uint64_t table_id = hard_code_table.get_table_id();
    const uint64_t database_id = hard_code_table.get_database_id();
    bool is_oracle = is_oracle_sys_database_id(database_id);
    bool check_lower_case = !is_mysql_database_id(database_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      ObASHSetInnerSqlWaitGuard ash_inner_sql_guard(ObInnerSqlWaitTypeId::RS_CHECK_SYS_VIEW_EXPANSION);
      // case 0: check expansion of sys view definition
      if (is_oracle) {
        if (OB_FAIL(sql.assign_fmt("SELECT FIELD FROM \"%s\".\"%s\" WHERE TABLE_ID = %lu",
                                   OB_ORA_SYS_SCHEMA_NAME,
                                   OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TNAME,
                                   table_id))) {
          LOG_WARN("failed to assign sql", KR(ret), K(sql));
        } else if (OB_FAIL(GCTX.oracle_sql_proxy_->read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(table_name), K(sql));
        }
      } else {
        if (OB_FAIL(sql.assign_fmt("SELECT FIELD FROM `%s`.`%s` WHERE TABLE_ID = %lu",
                                   OB_SYS_DATABASE_NAME,
                                   OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME,
                                   table_id))) {
          LOG_WARN("failed to assign sql", KR(ret), K(sql));
        } else if (!is_oracle && OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(table_name), K(sql));
        }
      }
      if (OB_FAIL(ret)) {
        if (OB_ERR_VIEW_INVALID == ret) {
          ret = OB_SCHEMA_ERROR;
          LOG_ERROR("check sys view: expand failed", KR(ret), K(tenant_id), K(table_name));
        } else {
          LOG_WARN("check sys view: expand failed", KR(ret), K(tenant_id), K(table_name));
        }
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", KR(ret), K(tenant_id));
      } else if (check_lower_case) {
        // case 1: check column name with lower case
        ObString col_name;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          if (OB_FAIL(result->get_varchar(0L, col_name))) {
            LOG_WARN("fail to get filed", KR(ret), K(tenant_id), K(table_name));
          } else if (check_str_with_lower_case(col_name)) {
            ret = OB_SCHEMA_ERROR;
            LOG_ERROR("check sys view: column name should be uppercase",
                      KR(ret), K(tenant_id), K(table_name), K(col_name));
          }
        } // end while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
          LOG_WARN("iterate failed", KR(ret));
        }
        // case 2: check view name with lower case
        if (OB_SUCC(ret) && check_str_with_lower_case(hard_code_table.get_table_name())) {
          ret = OB_SCHEMA_ERROR;
          LOG_ERROR("check sys view: table name should be uppercase", KR(ret), K(tenant_id), K(table_name));
        }
      }
    }
  }
  return ret;
}

int ObInspector::check_table_options(
  const ObTableSchema &table,
  const ObTableSchema &hard_code_table)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!table.is_valid() || !hard_code_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table or invalid hard_code_table", K(table), K(hard_code_table), KR(ret));
  } else if (table.get_table_id() != hard_code_table.get_table_id()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table id not match", "table_id", table.get_table_id(),
        "hard_code table_id", hard_code_table.get_table_id(), KR(ret));
  } else if (table.get_table_name_str() != hard_code_table.get_table_name_str()) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table name mismatch with hard code table",
        "table_id", table.get_table_id(), "table_name", table.get_table_name(),
        "hard_code_table name", hard_code_table.get_table_name(), KR(ret));
  } else {
    const ObString &table_name = table.get_table_name_str();

    if (table.get_tenant_id() != hard_code_table.get_tenant_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tenant_id mismatch", K(table_name), "in_memory", table.get_tenant_id(),
          "hard_code", hard_code_table.get_tenant_id(), KR(ret));
    } else if (table.get_database_id() != hard_code_table.get_database_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("database_id mismatch", K(table_name), "in_memory", table.get_database_id(),
          "hard_code", hard_code_table.get_database_id(), KR(ret));
    } else if (table.get_tablegroup_id() != hard_code_table.get_tablegroup_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tablegroup_id mismatch", K(table_name), "in_memory", table.get_tablegroup_id(),
          "hard_code", hard_code_table.get_tablegroup_id(), KR(ret));
    } else if (table.get_auto_increment() != hard_code_table.get_auto_increment()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("auto_increment mismatch", K(table_name), "in_memory", table.get_auto_increment(),
          "hard_code", hard_code_table.get_auto_increment(), KR(ret));
    } else if (table.is_read_only() != hard_code_table.is_read_only()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("read_only mismatch", K(table_name), "in_memory", table.is_read_only(),
          "hard code", hard_code_table.is_read_only(), KR(ret));
    } else if (table.get_load_type() != hard_code_table.get_load_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("load_type mismatch", K(table_name), "in_memory", table.get_load_type(),
          "hard_code", hard_code_table.get_load_type(), KR(ret));
    } else if (table.get_table_type() != hard_code_table.get_table_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("table_type mismatch", K(table_name), "in_memory", table.get_table_type(),
          "hard_code", hard_code_table.get_table_type(), KR(ret));
    } else if (table.get_index_type() != hard_code_table.get_index_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_type mismatch", K(table_name), "in_memory", table.get_index_type(),
          "hard_code", hard_code_table.get_index_type(), KR(ret));
    } else if (table.get_index_using_type() != hard_code_table.get_index_using_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_using_type mismatch", K(table_name), "in_memory", table.get_index_using_type(),
          "hard_code", hard_code_table.get_index_using_type(), KR(ret));
    } else if (table.get_def_type() != hard_code_table.get_def_type()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("def_type mismatch", K(table_name), "in_memory", table.get_def_type(),
          "hard_code", hard_code_table.get_def_type(), KR(ret));
    } else if (table.get_data_table_id() != hard_code_table.get_data_table_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("data_table_id mismatch", K(table_name), "in_memory", table.get_data_table_id(),
          "hard_code", hard_code_table.get_data_table_id(), KR(ret));
    } else if (table.get_tablegroup_name() != hard_code_table.get_tablegroup_name()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tablegroup_name mismatch", K(table_name), "in_memory", table.get_tablegroup_name(),
          "hard_code", hard_code_table.get_tablegroup_name(), KR(ret));
    } else if (table.get_view_schema() != hard_code_table.get_view_schema()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("view_schema mismatch", K(table_name), "in_memory", table.get_view_schema(),
          "hard_code", hard_code_table.get_view_schema(), KR(ret));
    } else if (table.get_part_level() != hard_code_table.get_part_level()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("part_level mismatch", K(table_name), "in_memory", table.get_part_level(),
          "hard_code", hard_code_table.get_part_level(), KR(ret));
    } else if ((table.get_part_option().get_part_func_expr_str()
        != hard_code_table.get_part_option().get_part_func_expr_str())
        || (table.get_part_option().get_part_func_type()
        != hard_code_table.get_part_option().get_part_func_type())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("part_expr mismatch", K(table_name), "in_memory",
          table.get_part_option(), "hard_code", hard_code_table.get_part_option(), KR(ret));
    } else if ((table.get_sub_part_option().get_part_func_expr_str()
        != hard_code_table.get_sub_part_option().get_part_func_expr_str())
        || (table.get_sub_part_option().get_part_func_type()
        != hard_code_table.get_sub_part_option().get_part_func_type())) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("sub_part_expr mismatch", K(table_name), "in_memory",
          table.get_sub_part_option(), "hard_code", hard_code_table.get_sub_part_option(), KR(ret));
    } else if (table.is_view_table()) {
      // view table do not check column info
    } else if (table.get_max_used_column_id() < hard_code_table.get_max_used_column_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("max_used_column_id mismatch", K(table_name), "in_memory",
          table.get_max_used_column_id(), "hard_code",
          hard_code_table.get_max_used_column_id(), KR(ret));
    } else if (table.get_rowkey_column_num() != hard_code_table.get_rowkey_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("rowkey_column_num mismatch", K(table_name), "in_memory",
          table.get_rowkey_column_num(), "hard_code",
          hard_code_table.get_rowkey_column_num(), KR(ret));
    } else if (table.get_index_column_num() != hard_code_table.get_index_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("index_column_num mismatch", K(table_name), "in_memory",
          table.get_index_column_num(), "hard_code",
          hard_code_table.get_index_column_num(), KR(ret));
    } else if (table.get_rowkey_split_pos() != hard_code_table.get_rowkey_split_pos()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("rowkey_split_pos mismatch", K(table_name), "in_memory",
          table.get_rowkey_split_pos(), "hard_code",
          hard_code_table.get_rowkey_split_pos(), KR(ret));
    } else if (table.get_partition_key_column_num()
        != hard_code_table.get_partition_key_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition_key_column_num mismatch", K(table_name), "in_memory",
          table.get_partition_key_column_num(), "hard_code",
          hard_code_table.get_partition_key_column_num(), KR(ret));
    } else if (table.get_subpartition_key_column_num()
        != hard_code_table.get_subpartition_key_column_num()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("partition_key_column_num mismatch", K(table_name), "in_memory",
          table.get_subpartition_key_column_num(), "hard_code",
          hard_code_table.get_subpartition_key_column_num(), KR(ret));
    } else if (table.get_autoinc_column_id() != hard_code_table.get_autoinc_column_id()) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("autoinc_column_id mismatch", K(table_name), "in_memory",
          table.get_autoinc_column_id(), "hard_code",
          hard_code_table.get_autoinc_column_id(), KR(ret));
    }

    // options may be different between different ob instance, don't check
    // block_size
    // is_user_bloomfilter
    // progressive_merge_num
    // replica_num
    // index_status
    // name_case_mode
    // charset_type
    // collation_type
    // schema_version
    // comment
    // compress_func_name
    // expire_info
    // zone_list
    // primary_zone
    // part_expr.part_num_
    // sub_part_expr.part_num_
    // store_format
    // row_store_type
    // progressive_merge_round
    // storage_format_version
  }
  return ret;
}

int ObInspector::check_column_schema(
  const ObString &table_name,
  const ObColumnSchemaV2 &column,
  const ObColumnSchemaV2 &hard_code_column)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(table_name.empty() || !column.is_valid() || !hard_code_column.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_name is empty or invalid column or invalid hard_code_column",
             KR(ret), K(table_name), K(column), K(hard_code_column));
  } else {
#define CMP_COLUMN_ATTR(attr) \
  if (OB_SUCC(ret)) { \
    if (column.get_##attr() != hard_code_column.get_##attr()) { \
      ret = OB_SCHEMA_ERROR; \
      LOG_WARN(#attr " mismatch", KR(ret), K(table_name), "column_name", column.get_column_name(), \
               "in_memory", column.get_##attr(), "hard_code", hard_code_column.get_##attr()); \
    } \
  }

#define CMP_COLUMN_IS_ATTR(attr) \
  if (OB_SUCC(ret)) { \
    if (column.is_##attr() != hard_code_column.is_##attr()) { \
      ret = OB_SCHEMA_ERROR; \
      LOG_WARN(#attr " mismatch", KR(ret), K(table_name), "column_name", column.get_column_name(), \
               "in_memory", column.is_##attr(), "hard_code", hard_code_column.is_##attr()); \
    } \
  }
    if (OB_SUCC(ret)) {
      if (column.get_column_name_str() != hard_code_column.get_column_name_str()) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("column_name mismatch", KR(ret), K(table_name),
                 "in_memory", column.get_column_name(),
                 "hard_code", hard_code_column.get_column_name());
      }
    }

    CMP_COLUMN_ATTR(column_id);
    CMP_COLUMN_ATTR(tenant_id);
    CMP_COLUMN_ATTR(table_id);
    // don't need to check schema version
    CMP_COLUMN_ATTR(rowkey_position);
    CMP_COLUMN_ATTR(index_position);
    CMP_COLUMN_ATTR(order_in_rowkey);
    CMP_COLUMN_ATTR(tbl_part_key_pos);
    CMP_COLUMN_ATTR(meta_type);
    CMP_COLUMN_ATTR(accuracy);
    CMP_COLUMN_ATTR(data_length);
    CMP_COLUMN_IS_ATTR(nullable);
    CMP_COLUMN_IS_ATTR(zero_fill);
    CMP_COLUMN_IS_ATTR(autoincrement);
    CMP_COLUMN_IS_ATTR(hidden);
    CMP_COLUMN_IS_ATTR(on_update_current_timestamp);
    CMP_COLUMN_ATTR(charset_type);
    // don't need to check orig default value
    if (ObString("row_store_type") == column.get_column_name()
        && (ObString("__all_table") == table_name || ObString("__all_table_history") == table_name)) {
      // row_store_type may have two possible default values
    } else {
      CMP_COLUMN_ATTR(cur_default_value);
    }
    CMP_COLUMN_ATTR(comment);

  }

#undef CMP_COLUMN_IS_ATTR
#undef CMP_COLUMN_INT_ATTR
  return ret;
}

bool ObInspector::check_str_with_lower_case(const ObString &str)
{
  bool bret = false;
  if (str.length() > 0) {
    for (int64_t i = 0; !bret && i < str.length(); i++) {
      if (str.ptr()[i] >= 'a' && str.ptr()[i] <= 'z') {
        bret = true;
      }
    }
  }
  return bret;
}

// explicit template instantiation
template int ObInspector::get_names<ObZoneInfoItem>(
  const ObDList<ObZoneInfoItem> &list, ObIArray<const char*> &names);

} // namespace share
} // namespace oceanbase
