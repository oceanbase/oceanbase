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


#include "ob_root_inspection.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_global_stat_proxy.h"//ObGlobalStatProxy
#include "share/location_cache/ob_location_service.h"
#include "share/ob_heartbeat_handler.h"
#include "share/ob_inspection_service.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
using namespace sql;
namespace rootserver
{
ObRootInspection::ObRootInspection()
  : inited_(false), stopped_(false), zone_passed_(false),
    sys_param_passed_(false), sys_stat_passed_(false),
    sys_table_schema_passed_(false), data_version_passed_(false),
    all_checked_(false), all_passed_(false),
    sql_proxy_(NULL), rpc_proxy_(NULL), schema_service_(NULL),
    zone_mgr_(NULL)
{
}

ObRootInspection::~ObRootInspection()
{
}

int ObRootInspection::init(ObMultiVersionSchemaService &schema_service,
                           ObZoneManager &zone_mgr,
                           ObMySQLProxy &sql_proxy,
                           obrpc::ObCommonRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    zone_mgr_ = &zone_mgr;
    sql_proxy_ = &sql_proxy;
    stopped_ = false;
    zone_passed_ = false;
    sys_param_passed_ = false;
    sys_stat_passed_ = false;
    sys_table_schema_passed_ = false;
    data_version_passed_ = false;
    all_checked_ = false;
    all_passed_ = false;
    rpc_proxy_ = rpc_proxy;
    inited_ = true;
  }
  return ret;
}

int ObRootInspection::check_all()
{
  int ret = OB_SUCCESS;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (cluster_version >= CLUSTER_VERSION_4_4_2_1 && cluster_version < CLUSTER_VERSION_4_5_0_0) {
    if (OB_FAIL(check_all_on_tenants_())) {
      LOG_WARN("check_all_on_tenants_ failed", KR(ret));
    }
    FLOG_INFO("[ROOT_INSPECTION] check_all_on_tenants", KR(ret), KCV(cluster_version));
  } else {
    if (OB_FAIL(check_all_on_rs_())) {
      LOG_WARN("check_all_on_rs_ failed", KR(ret));
    }
    FLOG_INFO("[ROOT_INSPECTION] check_all_on_rs", KR(ret), KCV(cluster_version));
  }
  return ret;
}

int ObRootInspection::check_all_on_rs_()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (!schema_service_->is_tenant_full_schema(OB_SYS_TENANT_ID)) {
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready, try again", K(ret));
  } else {
    int tmp = OB_SUCCESS;

    // check __all_zone
    if (OB_SUCCESS != (tmp = check_zone())) {
      LOG_WARN("check_zone failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    zone_passed_ = (OB_SUCCESS == tmp);

    // check sys stat
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_stat_())) {
      LOG_WARN("check_sys_stat failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_stat_passed_ = (OB_SUCCESS == tmp);

    // check sys param
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_param_())) {
      LOG_WARN("check_sys_param failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_param_passed_ = (OB_SUCCESS == tmp);

    // check sys schema
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_sys_table_schemas_())) {
      LOG_WARN("check_sys_table_schemas failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    sys_table_schema_passed_ = (OB_SUCCESS == tmp);

    // check tenant's data version
    tmp = OB_SUCCESS;
    if (OB_SUCCESS != (tmp = check_data_version_())) {
      LOG_WARN("check_data_version failed", K(tmp));
      ret = (OB_SUCCESS == ret) ? tmp : ret;
    }
    data_version_passed_ = (OB_SUCCESS == tmp);

    // upgrade job may still running, in order to avoid the upgrade process error stuck,
    // ignore the 4674 error
    for (int64_t i = 0; i < UPGRADE_JOB_TYPE_COUNT; i++) {
      tmp = OB_SUCCESS;
      ObRsJobType job_type = upgrade_job_type_array[i];
      if (job_type > JOB_TYPE_INVALID && job_type < JOB_TYPE_MAX) {
        if (OB_SUCCESS != (tmp = check_cancel())) {
          LOG_WARN("check_cancel failed", KR(ret), K(tmp));
          ret = (OB_SUCCESS == ret) ? tmp : ret;
          break;
        } else if (OB_SUCCESS != (tmp = ObUpgradeUtils::check_upgrade_job_passed(job_type))) {
          LOG_WARN("fail to check upgrade job passed", K(tmp), K(job_type));
          if (OB_RUN_JOB_NOT_SUCCESS != tmp) {
            ret = (OB_SUCCESS == ret) ? tmp : ret;
          } else {
            LOG_WARN("upgrade job may still running, check with __all_virtual_uprade_inspection",
                     K(ret), K(tmp), "job_type", ObRsJobTableOperator::get_job_type_str(job_type));
          }
        }
      }
    }

    all_checked_ = true;
    all_passed_ = OB_SUCC(ret);
  }
  return ret;
}

int ObRootInspection::check_all_on_tenants_()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (!schema_service_->is_tenant_full_schema(OB_SYS_TENANT_ID)) {
    ret = OB_EAGAIN;
    LOG_WARN("schema is not ready, try again", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObArray<uint64_t> tenant_ids;

    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", KR(ret), KP(schema_service_));
    } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
      LOG_WARN("get_tenant_ids failed", KR(ret));
    } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("GCTX.srv_rpc_proxy_ is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
    } else {
      int backup_ret = OB_SUCCESS;
      ObAsyncRunInspectionProxy proxy(*GCTX.srv_rpc_proxy_, &ObSrvRpcProxy::async_run_inspection);
      for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed", KR(ret));
        } else if (OB_TMP_FAIL(async_run_inspection_(tenant_id, proxy))) {
          LOG_WARN("fail to run inspection by tenant", KR(tmp_ret), K(tenant_id));
          backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
        }
      }
      if (OB_TMP_FAIL(wait_and_check_async_run_inspection_response(proxy))) {
        LOG_WARN("wait run inspection rpc failed", KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
      ret = OB_SUCC(ret) ? backup_ret : ret;
    }
  }
  return ret;
}

int ObRootInspection::check_tenant_in_upgrade(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    ObInspectionCancelCheckerSelfRS checker;
    ObInspector inspector(tenant_id, checker);
    if (OB_TMP_FAIL(inspector.check_sys_stat())) {
      LOG_WARN("fail to check sys stat", KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
    if (OB_TMP_FAIL(inspector.check_sys_param())) {
      LOG_WARN("fail to check param", KR(tmp_ret), K(tenant_id));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

// standby tenant may stay at lower data version,
// root_inspection won't check standby tenant's schema.
int ObRootInspection::construct_tenant_ids_(
    common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();
  ObArray<uint64_t> standby_tenants;
  ObArray<uint64_t> tmp_tenants;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tmp_tenants))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    bool is_standby = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_tenants.count(); i++) {
      const uint64_t tenant_id = tmp_tenants.at(i);
      if (OB_FAIL(ObAllTenantInfoProxy::is_standby_tenant(sql_proxy_, tenant_id, is_standby))) {
        LOG_WARN("fail to check is standby tenant", KR(ret), K(tenant_id));
      } else if (is_standby) {
        // skip
      } else if (OB_FAIL(tenant_ids.push_back(tenant_id))) {
        LOG_WARN("fail to push back tenant_id", KR(ret), K(tenant_id));
      }
    } // end for
  }
  return ret;
}

int ObRootInspection::check_zone()
{
  int ret = OB_SUCCESS;
  ObSqlString extra_cond;
  HEAP_VAR(ObGlobalInfo, global_zone_info) {
    ObArray<ObZoneInfo> zone_infos;
    ObArray<const char *> global_zone_item_names;
    if (!inited_) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(check_cancel())) {
      LOG_WARN("check_cancel failed", K(ret));
    } else if (OB_FAIL(extra_cond.assign_fmt("zone = '%s'", global_zone_info.zone_.ptr()))) {
      LOG_WARN("extra_cond assign_fmt failed", K(ret));
    } else if (OB_FAIL(ObInspector::get_names(global_zone_info.list_, global_zone_item_names))) {
      LOG_WARN("get global zone item names failed", K(ret));
    } else if (OB_FAIL(ObInspector::check_names(OB_SYS_TENANT_ID, OB_ALL_ZONE_TNAME, global_zone_item_names, extra_cond))) {
      LOG_WARN("check global zone item names failed", "table_name", OB_ALL_ZONE_TNAME,
          K(global_zone_item_names), K(extra_cond), K(ret));
    } else if (OB_FAIL(zone_mgr_->get_zone(zone_infos))) {
      LOG_WARN("zone manager get_zone failed", K(ret));
    } else {
      ObArray<const char *> zone_item_names;
      FOREACH_CNT_X(zone_info, zone_infos, OB_SUCCESS == ret) {
        zone_item_names.reuse();
        extra_cond.reset();
        if (OB_FAIL(check_cancel())) {
          LOG_WARN("check_cancel failed", K(ret));
        } else if (OB_FAIL(extra_cond.assign_fmt("zone = '%s'", zone_info->zone_.ptr()))) {
          LOG_WARN("extra_cond assign_fmt failed", K(ret));
        } else if (OB_FAIL(ObInspector::get_names(zone_info->list_, zone_item_names))) {
          LOG_WARN("get zone item names failed", K(ret));
        } else if (OB_FAIL(ObInspector::check_names(OB_SYS_TENANT_ID, OB_ALL_ZONE_TNAME, zone_item_names, extra_cond))) {
          LOG_WARN("check zone item names failed", "table_name", OB_ALL_ZONE_TNAME,
              K(zone_item_names), "zone_info", *zone_info, K(extra_cond), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootInspection::check_sys_stat_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      ObInspectionCancelCheckerSelfRS checker;
      ObInspector inspector(*tenant_id, checker);
      if (OB_TMP_FAIL(inspector.check_sys_stat())) {
        LOG_WARN("fail to check sys stat", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_param_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_CNT_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      ObInspectionCancelCheckerSelfRS checker;
      ObInspector inspector(*tenant_id, checker);
      if (OB_TMP_FAIL(inspector.check_sys_param())) {
        LOG_WARN("fail to check sys param", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_table_schemas_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
    // not check standby in rs, standby will be checked on tenant leader
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else if (OB_FAIL(check_sys_table_schemas(tenant_ids))) {
    LOG_WARN("failed to check sys table schemas", KR(ret), K(tenant_ids));
  }
  return ret;
}

int ObRootInspection::wait_and_check_rpc_response(rootserver::ObCheckSysTableSchemaProxy &proxy)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int backup_ret = OB_SUCCESS;
  ObArray<int> return_code;
  const ObIArray<const ObCheckSysTableSchemaResult *> &results = proxy.get_results();
  const ObIArray<ObCheckSysTableSchemaArg> &args = proxy.get_args();
  if (OB_FAIL(proxy.wait_all(return_code))) {
    LOG_WARN("wait_all failed", KR(ret));
  } else if (OB_FAIL(proxy.check_return_cnt(results.count()))) {
    LOG_WARN("check_return_cnt failed", KR(ret), K(results.count()));
  } else {
    for (int64_t i = 0; i < return_code.count() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(results.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("results is null", KR(ret), K(i), K(results.count()), K(args.count()));
      } else {
        const ObCheckSysTableSchemaResult &result = *results.at(i);
        const uint64_t tenant_id = args.at(i).get_tenant_id();
        int result_code = return_code.at(i);
        const ObIArray<uint64_t> &error_table_ids = result.get_error_table_ids();
        if (OB_TMP_FAIL(result_code)) {
          LOG_WARN("failed to check sys table schema", KR(tmp_ret), K(tenant_id));
          backup_ret = backup_ret == OB_SUCCESS ? tmp_ret : backup_ret;
        } else if (OB_TMP_FAIL(ObInspector::check_error_table_ids(tenant_id, error_table_ids))) {
          LOG_WARN("check error table ids fail", KR(ret), K(tenant_id), K(error_table_ids));
          backup_ret = backup_ret == OB_SUCCESS ? tmp_ret : backup_ret;
        }
      }
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_table_schemas(const ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int backup_ret = OB_SUCCESS;
  uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.srv_rpc_proxy_ is null", KR(ret), KP(GCTX.srv_rpc_proxy_));
  } else if (cluster_version < MOCK_CLUSTER_VERSION_4_3_5_4 ||
      (cluster_version >= CLUSTER_VERSION_4_4_0_0 && cluster_version < CLUSTER_VERSION_4_4_1_0)) {
    ObInspectionCancelCheckerSelfRS checker;
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      ObInspector inspector(tenant_id, checker);
      ObArray<uint64_t> table_ids;
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_TMP_FAIL(inspector.check_sys_table_schemas(table_ids))) {
        LOG_WARN("fail to check sys table schemas by tenant", KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(ObInspector::check_error_table_ids(tenant_id, table_ids))) {
        LOG_WARN("fail to check error table ids", KR(ret), K(tenant_id), K(table_ids));
      }
      backup_ret = backup_ret == OB_SUCCESS ? tmp_ret : backup_ret;
    }
    ret = OB_SUCC(ret) ? backup_ret : ret;
  } else {
    ObCheckSysTableSchemaProxy proxy(*GCTX.srv_rpc_proxy_, &ObSrvRpcProxy::check_sys_table_schema);
    ObArray<int> return_code;
    for (int64_t i = 0; i < tenant_ids.count() && OB_SUCC(ret); i++) {
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_FAIL(check_cancel())) {
        LOG_WARN("check_cancel failed", KR(ret));
      } else if (OB_FAIL(check_sys_table_schema_(tenant_id, proxy))) {
        LOG_WARN("fail to check sys table schemas by tenant", KR(ret), K(tenant_id));
      }
    } // end for
    if (OB_TMP_FAIL(wait_and_check_rpc_response(proxy))) {
      LOG_WARN("check rpc failed", KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret;
}

int ObRootInspection::check_sys_table_schema_(const uint64_t tenant_id,
      ObCheckSysTableSchemaProxy &proxy)
{
  int ret = OB_SUCCESS;
  ObCheckSysTableSchemaArg arg;
  ObAddr tenant_leader;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
        tenant_id, SYS_LS, tenant_leader))) {
    LOG_WARN("get_leader_with_retry_until_timeout failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_leader.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_leader is not valid", KR(ret), K(tenant_id), K(tenant_leader));
  } else if (OB_FAIL(arg.init(tenant_id))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF._ob_ddl_timeout))) {
      LOG_WARN("set_default_timeout_ctx failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.call(tenant_leader, ctx.get_timeout(), tenant_id, arg))) {
      LOG_WARN("call failed", KR(ret), K(tenant_id), K(tenant_leader), K(ctx), K(arg));
    } else {
      LOG_INFO("async check sys table schema for tenant", KR(ret), K(tenant_leader), K(arg), K(ctx));
    }
  }
  return ret;
}

int ObRootInspection::async_run_inspection_(const uint64_t tenant_id,
    ObAsyncRunInspectionProxy &proxy)
{
  int ret = OB_SUCCESS;
  ObRunInspectionArg arg;
  ObAddr tenant_leader;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
        tenant_id, SYS_LS, tenant_leader))) {
    LOG_WARN("get_leader_with_retry_until_timeout failed", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!tenant_leader.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_leader is not valid", KR(ret), K(tenant_id), K(tenant_leader));
  } else if (OB_FAIL(arg.init(tenant_id))) {
    LOG_WARN("init failed", KR(ret), K(tenant_id));
  } else {
    ObTimeoutCtx ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF._ob_ddl_timeout))) {
      LOG_WARN("set_default_timeout_ctx failed", KR(ret), K(tenant_id));
    } else if (OB_FAIL(proxy.call(tenant_leader, ctx.get_timeout(), tenant_id, arg))) {
      LOG_WARN("call failed", KR(ret), K(tenant_id), K(tenant_leader), K(ctx), K(arg));
    } else {
      LOG_INFO("async run inspection for tenant", KR(ret), K(tenant_leader), K(arg), K(ctx));
    }
  }
  return ret;
}

int ObRootInspection::wait_and_check_async_run_inspection_response(
    ObAsyncRunInspectionProxy &proxy)
{
  int ret = OB_SUCCESS;
  int backup_ret = OB_SUCCESS;
  ObArray<int> return_code;
  const ObIArray<obrpc::ObRunInspectionArg> &args = proxy.get_args();
  if (OB_FAIL(proxy.wait_all(return_code))) {
    LOG_WARN("wait_all failed", KR(ret));
  } else if (OB_FAIL(proxy.check_return_cnt(args.count()))) {
    LOG_WARN("check_return_cnt failed", KR(ret), K(args.count()));
  } else {
    for (int64_t i = 0; i < return_code.count() && OB_SUCC(ret); i++) {
      int64_t tmp_ret = return_code.at(i);
      const uint64_t tenant_id = args.at(i).get_tenant_id();
      if (OB_TMP_FAIL(tmp_ret)) {
        LOG_WARN("run inspection rpc failed", KR(tmp_ret), K(tenant_id));
        backup_ret = (backup_ret == OB_SUCCESS) ? tmp_ret : backup_ret;
      }
    } // end for
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_sys_table_schema(const obrpc::ObCheckSysTableSchemaArg &arg,
    obrpc::ObCheckSysTableSchemaResult &result)
{
  int ret = OB_SUCCESS;
  bool need_check = true;
  const uint64_t tenant_id = arg.get_tenant_id();
  ObArray<uint64_t> error_table_ids;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else {
    bool is_standby = false;
    if (MTL_ID() != arg.get_tenant_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("this function should be called on tenant leader", KR(ret), K(arg), K(MTL_ID()));
    } else if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
      need_check = false;
      LOG_INFO("is standby tenant, no need to check", KR(ret), K(is_standby), K(arg));
    }
  }
  if (!need_check || OB_FAIL(ret)) {
  } else {
    // check_all
    ObInspectionCancelCheckerRemoteRS checker(arg);
    ObInspector inspector(tenant_id, checker);
    if (OB_FAIL(inspector.check_sys_table_schemas(error_table_ids))) {
      LOG_WARN("failed to check sys table schemas", KR(ret), K(tenant_id));
    } else if (OB_FAIL(result.init(error_table_ids))) {
      LOG_WARN("failed to init result", KR(ret), K(tenant_id), K(error_table_ids));
    }
  }
  return ret;
}

#define PRINT_TABLE_INFO(table) "table_id", table.get_table_id(), "table_name", table.get_table_name()

int ObRootInspection::check_and_get_system_table_column_diff(
    const share::schema::ObTableSchema &table_schema,
    const share::schema::ObTableSchema &hard_code_schema,
    common::ObIArray<uint64_t> &add_column_ids,
    common::ObIArray<uint64_t> &alter_column_ids)
{
  int ret = OB_SUCCESS;
  add_column_ids.reset();
  alter_column_ids.reset();
  if (OB_UNLIKELY(!table_schema.is_valid() || !hard_code_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", KR(ret), K(table_schema), K(hard_code_schema));
  } else if (table_schema.get_tenant_id() != hard_code_schema.get_tenant_id()
             || table_schema.get_table_id() != hard_code_schema.get_table_id()
             || 0 != table_schema.get_table_name_str().compare(hard_code_schema.get_table_name_str())
             || !is_system_table(table_schema.get_table_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table_schema", KR(ret),
             "tenant_id", table_schema.get_tenant_id(),
             PRINT_TABLE_INFO(table_schema),
             "hard_code_tenant_id", hard_code_schema.get_tenant_id(),
             "hard_code_table_id", hard_code_schema.get_table_id(),
             "hard_code_table_name", hard_code_schema.get_table_name());
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const ObColumnSchemaV2 *column = NULL;
    const ObColumnSchemaV2 *hard_code_column = NULL;
    ObColumnSchemaV2 tmp_column; // check_column_can_be_altered_online() may change dst_column, is ugly.

    // case 1. check if columns should be dropped.
    // case 2. check if column can be altered online.
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schema.get_column_count(); i++) {
      column = table_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", KR(ret), K(tenant_id), K(table_id), K(i));
      } else if (OB_ISNULL(hard_code_column = hard_code_schema.get_column_schema(column->get_column_id()))) {
        ret = OB_NOT_SUPPORTED; // case 1
        LOG_WARN("can't drop system table's column", KR(ret),
                 K(tenant_id), K(table_id),
                 "table_name", table_schema.get_table_name(),
                 "column_id", column->get_column_id(),
                 "column_name", column->get_column_name());
      } else {
        // case 2
        int tmp_ret = ObInspector::check_column_schema(table_schema.get_table_name_str(),
                                                       *column,
                                                       *hard_code_column);
        if (OB_SUCCESS == tmp_ret) {
          // not changed
        } else if (OB_SCHEMA_ERROR != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to check column schema", KR(ret),
                   K(tenant_id), K(table_id), KPC(column), KPC(hard_code_column));
        } else if (OB_FAIL(tmp_column.assign(*hard_code_column))) {
          LOG_WARN("fail to assign hard code column schema", KR(ret),
                   K(tenant_id), K(table_id),  "column_id", hard_code_column->get_column_id());
        } else if (OB_FAIL(table_schema.check_column_can_be_altered_online(column, &tmp_column))) {
          LOG_WARN("fail to check alter column online", KR(ret),
                   K(tenant_id), K(table_id),
                   "table_name", table_schema.get_table_name(),
                   "column_id", column->get_column_id(),
                   "column_name", column->get_column_name());
        } else if (OB_FAIL(alter_column_ids.push_back(column->get_column_id()))) {
          LOG_WARN("fail to push back column_id", KR(ret), K(tenant_id), K(table_id),
                   "column_id", column->get_column_id());
        }
      }
    } // end for

    // case 3: check if columns should be added.
    for (int64_t i = 0; OB_SUCC(ret) && i < hard_code_schema.get_column_count(); i++) {
      hard_code_column = hard_code_schema.get_column_schema_by_idx(i);
      if (OB_ISNULL(hard_code_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", KR(ret), K(tenant_id), K(table_id), K(i));
      } else if (OB_NOT_NULL(column = table_schema.get_column_schema(hard_code_column->get_column_id()))) {
        // column exist, just skip
      } else {
        const uint64_t hard_code_column_id = hard_code_column->get_column_id();
        const ObColumnSchemaV2 *last_column = NULL;
        if (table_schema.get_column_count() <= 0
            || OB_ISNULL(last_column = table_schema.get_column_schema_by_idx(
                         table_schema.get_column_count() - 1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column count or column", KR(ret), K(table_schema));
        } else if (table_schema.get_max_used_column_id() >= hard_code_column_id
                  || last_column->get_column_id() >= hard_code_column_id) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("column should be added at last", KR(ret), KPC(hard_code_column), K(table_schema));
        } else if (OB_FAIL(add_column_ids.push_back(hard_code_column_id))) {
          LOG_WARN("fail to push back column_id", KR(ret), K(tenant_id), K(table_id),
                   "column_id", hard_code_column_id);
        }
      }
    } // end for
  }
  return ret;
}

#undef PRINT_TABLE_INFO

int ObRootInspection::check_data_version_()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(check_cancel())) {
    LOG_WARN("check_cancel failed", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(construct_tenant_ids_(tenant_ids))) {
    LOG_WARN("get_tenant_ids failed", KR(ret));
  } else {
    int backup_ret = OB_SUCCESS;
    int tmp_ret = OB_SUCCESS;
    FOREACH_X(tenant_id, tenant_ids, OB_SUCC(ret)) {
      ObInspectionCancelCheckerSelfRS checker;
      ObInspector inspector(*tenant_id, checker);
      if (OB_TMP_FAIL(inspector.check_data_version())) {
        LOG_WARN("fail to check data version", KR(tmp_ret), K(*tenant_id));
        backup_ret = OB_SUCCESS == backup_ret ? tmp_ret : backup_ret;
      }
    } // end foreach
    ret = OB_SUCC(ret) ? backup_ret : ret;
  }
  return ret;
}

int ObRootInspection::check_cancel()
{
  int ret = OB_SUCCESS;
  if (stopped_) {
    ret = OB_CANCELED;
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (!GCTX.root_service_->is_full_service()) {
    ret = OB_CANCELED;
  }
  return ret;
}

ObUpgradeInspection::ObUpgradeInspection()
  : inited_(false), schema_service_(NULL), root_inspection_(NULL)
{
}

ObUpgradeInspection::~ObUpgradeInspection()
{
}

void ObUpgradeInspection::merge_check_result_(ObInspectionItem &item, const bool passed)
{
  if (!passed) {
    item.any_failed_ = true;
  }
}

void ObUpgradeInspection::mark_checking_(ObInspectionItem &item)
{
  item.any_checking_ = true;
}

const char *ObUpgradeInspection::get_status_(const ObInspectionItem &item)
{
  return item.any_failed_ ? "failed" : (item.any_checking_ ? "checking" : "succeed");
}

int ObUpgradeInspection::init(ObMultiVersionSchemaService &schema_service,
                              ObRootInspection &root_inspection)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    root_inspection_ = &root_inspection;
    inited_ = true;
  }
  return ret;
}

int ObUpgradeInspection::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (cluster_version >= CLUSTER_VERSION_4_4_2_1 && cluster_version < CLUSTER_VERSION_4_5_0_0) {
    if (OB_FAIL(inner_get_next_row_on_tenants_(row))) {
      LOG_WARN("inner_get_next_row_on_tenants_ failed", KR(ret));
    }
  } else {
    if (OB_FAIL(inner_get_next_row_on_rs_(row))) {
      LOG_WARN("inner_get_next_row_on_rs_ failed", KR(ret));
    }
  }
  return ret;
}

// old interface, get results from rs memory
int ObUpgradeInspection::inner_get_next_row_on_rs_(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (!start_to_read_) {
    const ObTableSchema *table_schema = NULL;
    const uint64_t table_id = OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID;
    if (OB_FAIL(schema_guard.get_table_schema(OB_SYS_TENANT_ID, table_id, table_schema))) {
      LOG_WARN("get_table_schema failed", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), K(ret));
    } else {
      ObArray<Column> columns;

#define ADD_ROW(name, info) \
  do { \
    columns.reuse(); \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(get_full_row(table_schema, name, info, columns))) { \
      LOG_WARN("get_full_row failed", "table_schema", *table_schema, \
          K(name), K(info), K(ret)); \
    } else if (OB_FAIL(project_row(columns, cur_row_))) { \
      LOG_WARN("project_row failed", K(columns), K(ret)); \
    } else if (OB_FAIL(scanner_.add_row(cur_row_))) { \
      LOG_WARN("add_row failed", K(cur_row_), K(ret)); \
    } \
  } while (false)

#define CHECK_RESULT(checked, value) (checked ? (value ? "succeed" : "failed") : "checking")

      ADD_ROW("zone_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_zone_passed()));
      ADD_ROW("sys_stat_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_sys_stat_passed()));
      ADD_ROW("sys_param_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_sys_param_passed()));
      ADD_ROW("sys_table_schema_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_sys_table_schema_passed()));
      ADD_ROW("data_version_check", CHECK_RESULT(root_inspection_->is_all_checked(),
          root_inspection_->is_data_version_passed()));

      bool upgrade_job_passed = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < UPGRADE_JOB_TYPE_COUNT; i++) {
        int tmp = OB_SUCCESS;
        ObRsJobType job_type = upgrade_job_type_array[i];
        if (job_type > JOB_TYPE_INVALID && job_type < JOB_TYPE_MAX) {
          if (OB_SUCCESS != (tmp = ObUpgradeUtils::check_upgrade_job_passed(job_type))) {
            LOG_WARN("fail to check upgrade job passed", K(tmp), K(job_type));
            upgrade_job_passed = false;
          }
          ADD_ROW(ObRsJobTableOperator::get_job_type_str(job_type),
                  CHECK_RESULT(root_inspection_->is_all_checked(), (OB_SUCCESS == tmp)));
        }
      }

      ADD_ROW("all_check", CHECK_RESULT(root_inspection_->is_all_checked(),
              (root_inspection_->is_all_passed() && upgrade_job_passed)));

#undef CHECK_RESULT
#undef ADD_ROW
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}


// new interface, get results from tenant leaders by rpc
int ObUpgradeInspection::inner_get_next_row_on_tenants_(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is null", KR(ret), KP(GCTX.srv_rpc_proxy_), KP(GCTX.location_service_));
  } else if (!start_to_read_) {
    const ObTableSchema *table_schema = NULL;
    const uint64_t table_id = OB_ALL_VIRTUAL_UPGRADE_INSPECTION_TID;
    if (OB_FAIL(schema_guard.get_table_schema(OB_SYS_TENANT_ID, table_id, table_schema))) {
      LOG_WARN("get_table_schema failed", K(table_id), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is null", KP(table_schema), K(ret));
    } else {
      ObArray<Column> columns;

#define ADD_ROW(name, info) \
  do { \
    columns.reuse(); \
    if (OB_FAIL(ret)) { \
    } else if (OB_FAIL(get_full_row(table_schema, name, info, columns))) { \
      LOG_WARN("get_full_row failed", "table_schema", *table_schema, \
          K(name), K(info), K(ret)); \
    } else if (OB_FAIL(project_row(columns, cur_row_))) { \
      LOG_WARN("project_row failed", K(columns), K(ret)); \
    } else if (OB_FAIL(scanner_.add_row(cur_row_))) { \
      LOG_WARN("add_row failed", K(cur_row_), K(ret)); \
    } \
  } while (false)

      ObInspectionItem sys_stat;
      ObInspectionItem sys_param;
      ObInspectionItem sys_table_schema;
      ObInspectionItem data_version;
      ObArray<uint64_t> tenant_ids;
      if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
        LOG_WARN("get tenant ids failed", KR(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
          ObTimeoutCtx ctx;
          const uint64_t tenant_id = tenant_ids.at(i);
          ObAddr tenant_leader;
          obrpc::ObGetInspectionStatusArg arg;
          obrpc::ObGetInspectionStatusResult result;
          if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
                      GCONF.cluster_id, tenant_id, SYS_LS, tenant_leader))) {
            LOG_WARN("get leader failed", KR(ret), K(tenant_id));
          } else if (OB_UNLIKELY(!tenant_leader.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tenant leader invalid", KR(ret), K(tenant_id), K(tenant_leader));
          } else if (OB_FAIL(arg.init(tenant_id))) {
            LOG_WARN("init arg failed", KR(ret), K(tenant_id));
          } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF._ob_ddl_timeout))) {
            LOG_WARN("set timeout failed", KR(ret), K(tenant_id));
          } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(tenant_leader)
                                                   .by(tenant_id)
                                                   .timeout(ctx.get_timeout())
                                                   .get_inspection_status(arg, result))) {
            LOG_WARN("get inspection status failed", KR(ret), K(tenant_id), K(tenant_leader));
            // tenant rpc failed, ignore error and mark as checking
            ret = OB_SUCCESS;
            mark_checking_(sys_stat);
            mark_checking_(sys_param);
            mark_checking_(sys_table_schema);
            mark_checking_(data_version);
          } else {
            if (result.all_checked_) {
              merge_check_result_(sys_stat, result.sys_stat_passed_);
              merge_check_result_(sys_param, result.sys_param_passed_);
              merge_check_result_(sys_table_schema, result.sys_table_schema_passed_);
              merge_check_result_(data_version, result.data_version_passed_);
            } else {
              mark_checking_(sys_stat);
              mark_checking_(sys_param);
              mark_checking_(sys_table_schema);
              mark_checking_(data_version);
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        const bool all_checked = !sys_stat.any_checking_
                                 && !sys_param.any_checking_
                                 && !sys_table_schema.any_checking_
                                 && !data_version.any_checking_;
        const bool any_failed = sys_stat.any_failed_
                                || sys_param.any_failed_
                                || sys_table_schema.any_failed_
                                || data_version.any_failed_;
        const bool all_passed = all_checked && !any_failed;

        ADD_ROW("sys_stat_check", get_status_(sys_stat));
        ADD_ROW("sys_param_check", get_status_(sys_param));
        ADD_ROW("sys_table_schema_check", get_status_(sys_table_schema));
        ADD_ROW("data_version_check", get_status_(data_version));
        ADD_ROW("all_check", all_checked ? (all_passed ? "succeed" : "failed") : "checking");
      }

#undef ADD_ROW
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get_next_row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObUpgradeInspection::get_full_row(const share::schema::ObTableSchema *table,
                                      const char *name, const char *info,
                                      ObIArray<Column> &columns)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table || NULL == name || NULL == info) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(table), KP(name), KP(info), K(ret));
  } else {
    ADD_COLUMN(set_varchar, table, "name", name, columns);
    ADD_COLUMN(set_varchar, table, "info", info, columns);
  }

  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
