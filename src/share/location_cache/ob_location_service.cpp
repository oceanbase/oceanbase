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

#define USING_LOG_PREFIX SHARE_LOCATION

#include "share/location_cache/ob_location_service.h"
#include "share/config/ob_server_config.h" // GCONF
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "observer/ob_server_struct.h" // GCTX
#include "lib/utility/ob_tracepoint.h" // ERRSIM_POINT_DEF
#include "share/ls/ob_ls_status_operator.h" // ObLSStatus

namespace oceanbase
{
using namespace common;

namespace share
{
ObLocationService::ObLocationService()
    : inited_(false),
      stopped_(false),
      ls_location_service_(),
      tablet_ls_service_()
{
}

int ObLocationService::get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.get(
      cluster_id,
      tenant_id,
      ls_id,
      expire_renew_time,
      is_cache_hit,
      location))) {
    LOG_WARN("fail to get log stream location",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id),
        K(expire_renew_time), K(is_cache_hit), K(location));
  }
  return ret;
}

int ObLocationService::get_leader(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const bool force_renew,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.get_leader(
      cluster_id,
      tenant_id,
      ls_id,
      force_renew,
      leader))) {
    LOG_WARN("fail to get log stream location leader",
        KR(ret), K(cluster_id), K(tenant_id),
        K(ls_id), K(leader), K(force_renew));
  }
  return ret;
}

int ObLocationService::get_leader_with_retry_until_timeout(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    common::ObAddr &leader,
    const int64_t abs_retry_timeout,
    const int64_t retry_interval)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.get_leader_with_retry_until_timeout(
      cluster_id,
      tenant_id,
      ls_id,
      leader,
      abs_retry_timeout,
      retry_interval))) {
    LOG_WARN("fail to get log stream location leader with retry until_timeout",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(leader),
        K(abs_retry_timeout), K(retry_interval));
  }
  return ret;
}

int ObLocationService::nonblock_get(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObLSLocation &location)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.nonblock_get(
      cluster_id,
      tenant_id,
      ls_id,
      location))) {
    LOG_WARN("fail to nonblock get log stream location",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(location));
  }
  return ret;
}

int ObLocationService::nonblock_get_leader(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.nonblock_get_leader(
      cluster_id,
      tenant_id,
      ls_id,
      leader))) {
    LOG_WARN("fail to nonblock get log stream location leader",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id), K(leader));
  }
  return ret;
}

int ObLocationService::nonblock_renew(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.nonblock_renew(
      cluster_id,
      tenant_id,
      ls_id))) {
    LOG_WARN("fail to nonblock_renew log stream location",
        KR(ret), K(cluster_id), K(tenant_id), K(ls_id));
  }
  return ret;
}

int ObLocationService::get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.get(
      tenant_id,
      tablet_id,
      expire_renew_time,
      is_cache_hit,
      ls_id))) {
    LOG_WARN("fail to get tablet to log stream",
        KR(ret), K(tenant_id), K(tablet_id),
        K(expire_renew_time), K(is_cache_hit), K(ls_id));
  }
  return ret;
}

int ObLocationService::nonblock_get(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.nonblock_get(
      tenant_id,
      tablet_id,
      ls_id))) {
    LOG_WARN("fail to nonblock get tablet to log stream",
        KR(ret), K(tenant_id), K(tablet_id), K(ls_id));
  }
  return ret;
}

int ObLocationService::nonblock_renew(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.nonblock_renew(
      tenant_id,
      tablet_id))) {
    LOG_WARN("fail to nonblock renew tablet to log stream",
        KR(ret), K(tenant_id), K(tablet_id));
  }
  return ret;
}

int ObLocationService::vtable_get(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t expire_renew_time,
    bool &is_cache_hit,
    ObIArray<common::ObAddr> &locations)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.vtable_get(
      tenant_id,
      table_id,
      expire_renew_time,
      is_cache_hit,
      locations))) {
    LOG_WARN("fail to get location for virtual table", KR(ret), K(tenant_id),
        K(table_id), K(expire_renew_time), K(is_cache_hit), K(locations));
  }
  return ret;
}

int ObLocationService::external_table_get(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObIArray<ObAddr> &locations)
{
  UNUSED(table_id);
  bool is_cache_hit = false;
  //using the locations from any distributed virtual table
  return vtable_get(tenant_id, OB_ALL_VIRTUAL_PROCESSLIST_TID, 0, is_cache_hit, locations);
}

int ObLocationService::vtable_nonblock_renew(
    const uint64_t tenant_id,
    const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.vtable_nonblock_renew(
      tenant_id,
      table_id))) {
    LOG_WARN("fail to nonblock renew location for virtual table",
        KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObLocationService::init(
    ObLSTableOperator &ls_pt,
    schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    ObIAliveServerTracer &server_tracer,
    ObRsMgr &rs_mgr,
    obrpc::ObCommonRpcProxy &rpc_proxy,
    obrpc::ObSrvRpcProxy &srv_rpc_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("location service init twice", KR(ret));
  } else if (OB_FAIL(ls_location_service_.init(ls_pt, schema_service, rs_mgr, srv_rpc_proxy))) {
    LOG_WARN("ls_location_service init failed", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.init(sql_proxy))) {
    LOG_WARN("tablet_ls_service init failed", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.init(server_tracer, rs_mgr, rpc_proxy))) {
    LOG_WARN("vtable_location_service init failed", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObLocationService::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location service not init", KR(ret));
  } else if (OB_FAIL(ls_location_service_.start())) {
    LOG_WARN("ls_location_service start failed", KR(ret));
  }
  // tablet_ls_service_ and vtable_location_service_ have no threads need to be started
  return ret;
}

void ObLocationService::stop()
{
  ls_location_service_.stop();
  tablet_ls_service_.stop();
  vtable_location_service_.stop();
}

void ObLocationService::wait()
{
  ls_location_service_.wait();
  tablet_ls_service_.wait();
  vtable_location_service_.wait();
}

int ObLocationService::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ls_location_service_.destroy())) {
    LOG_WARN("destroy ls_location_service failed", KR(ret));
  } else if (OB_FAIL(tablet_ls_service_.destroy())) {
    LOG_WARN("destroy tablet_ls_service failed", KR(ret));
  } else if (OB_FAIL(vtable_location_service_.destroy())) {
    LOG_WARN("destroy vtable_location_service failed", KR(ret));
  } else {
    stopped_ = true;
    inited_ = false;
  }
  return ret;
}

int ObLocationService::reload_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    if (OB_FAIL(ls_location_service_.reload_config())) {
      LOG_WARN("ls_location_service reload config failed", KR(ret));
    } else if (OB_FAIL(tablet_ls_service_.reload_config())) {
      LOG_WARN("tablet_ls_service reload config failed", KR(ret));
    } else if (OB_FAIL(vtable_location_service_.reload_config())) {
      LOG_WARN("vtable_location_service reload config failed", KR(ret));
    }
  }
  return ret;
}

int ObLocationService::batch_renew_tablet_locations(
    const uint64_t tenant_id,
    const ObList<common::ObTabletID, common::ObIAllocator> &tablet_list,
    const int error_code,
    const bool is_nonblock)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (0 == tablet_list.size()) {
    // do nothing
  } else {
    ObArray<ObLSID> ls_ids;
    RenewType renew_type = DEFAULT_RENEW_BOTH;
    renew_type = gen_renew_type_(error_code);
    ObArray<ObTabletLSCache> tablet_ls_caches;
    ObArray<ObLSLocation> ls_locations;
    // 1. renew tablet ls mapping
    if (OB_FAIL(ret)) {
    } else if (ONLY_RENEW_LS_LOCATION != renew_type) {
      if (is_nonblock) {
        FOREACH_X(tablet_id, tablet_list, OB_SUCC(ret)) {
          if (OB_FAIL(tablet_ls_service_.nonblock_renew(tenant_id, *tablet_id))) {
            LOG_WARN("nonblock renew failed", KR(ret), K(tenant_id), K(*tablet_id));
          }
        }
      } else if (OB_FAIL(tablet_ls_service_.batch_renew_tablet_ls_cache( // block
          tenant_id,
          tablet_list,
          tablet_ls_caches))) {
        LOG_WARN("batch renew cache failed", KR(ret), K(tenant_id), K(tablet_list));
      }
    }
    // 2. renew ls location
    if (OB_FAIL(ret)) {
    } else if (ONLY_RENEW_TABLET_LS_MAPPING != renew_type) {
      if (is_nonblock) {
        if (OB_FAIL(ls_location_service_.nonblock_renew(GCONF.cluster_id, tenant_id))) {
          LOG_WARN("nonblock renew tenant ls locations failed", KR(ret), K(tenant_id));
        }
      } else if (!tablet_ls_caches.empty()) { // block renew both
        ls_ids.reset();
        ARRAY_FOREACH(tablet_ls_caches, idx) {
          const ObTabletLSCache &tablet_ls = tablet_ls_caches.at(idx);
          if (!common::has_exist_in_array(ls_ids, tablet_ls.get_ls_id())) {
            if (OB_FAIL(ls_ids.push_back(tablet_ls.get_ls_id()))) {
              LOG_WARN("push back failed", KR(ret), K(tablet_ls));
            }
          }
        }
        if (FAILEDx(ls_location_service_.batch_renew_ls_locations(
            GCONF.cluster_id,
            tenant_id,
            ls_ids,
            ls_locations))) {
          LOG_WARN("batch renew cache failed", KR(ret), K(tenant_id), K(tablet_list), K(ls_ids));
        }
      } else { // block only renew ls location or block renew both with empty tablet_ls_caches
        if (OB_FAIL(ls_location_service_.renew_location_for_tenant(
            GCONF.cluster_id,
            tenant_id,
            ls_locations))) {
          LOG_WARN("renew location for tenant failed", KR(ret), K(tenant_id), K(ls_locations));
        }
      }
    }
    FLOG_INFO("[TABLET_LOCATION] batch renew tablet locations finished",
        KR(ret), K(tenant_id), K(renew_type), K(is_nonblock), K(tablet_list), K(ls_ids),
        K(error_code));
  }
  return ret;
}

// ONLY_RENEW_TABLET_LS_MAPPING is not used yet
ObLocationService::RenewType ObLocationService::gen_renew_type_(const int error) const
{
  RenewType renew_type = DEFAULT_RENEW_BOTH;

  // ALL error need renew both (tablet/LS) locations
  //
  // OB_NOT_MASTER also need renew tablet locations. SQL may request wrong Tablet-LS location.
  renew_type = DEFAULT_RENEW_BOTH;
  /*
  switch (error) {
    case OB_NOT_MASTER: {
      renew_type = ONLY_RENEW_LS_LOCATION;
      break;
    }
    default: {
      renew_type = DEFAULT_RENEW_BOTH;
      break;
    }
  }
  */
  return renew_type;
}

int ObLocationService::renew_tablet_location(
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    const int error_code,
    const bool is_nonblock)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObList<ObTabletID, ObIAllocator> tablet_list(allocator);
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(tablet_list.push_back(tablet_id))) {
    LOG_WARN("push back failed", KR(ret), K(tablet_id));
  } else if (OB_FAIL(batch_renew_tablet_locations(tenant_id, tablet_list, error_code, is_nonblock))) {
    LOG_WARN("renew tablet locations failed", KR(ret),
        K(tenant_id), K(tablet_list), K(error_code), K(is_nonblock));
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_CHECK_LS_EXIST_WITH_TENANT_NOT_NORMAL);

int ObLocationService::check_ls_exist(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObLSExistState &state)
{
  int ret = OB_SUCCESS;
  state.reset();
  ObSqlString sql;
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX has null ptr", KR(ret), KP(GCTX.schema_service_), KP(GCTX.sql_proxy_));
  } else {
    schema::ObSchemaGetterGuard schema_guard;
    const ObSimpleTenantSchema *tenant_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      LOG_WARN("tenant does not exist", KR(ret), K(tenant_id));
    } else if ((is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id))
        && (tenant_schema->is_normal() || tenant_schema->is_dropping())) {
      // sys and meta tenants only have sys ls. If tenant is in normal or dropping status, sys ls exists.
      state.set_existing();
    }
  } // release schema_guard as soon as possible

  // errsim for test
  if (EN_CHECK_LS_EXIST_WITH_TENANT_NOT_NORMAL) {
    state.reset();
  }

  if (OB_FAIL(ret) || state.is_valid()) {
  } else if (OB_FAIL(construct_check_ls_exist_sql_(tenant_id, ls_id, sql))) {
    LOG_WARN("construct check ls exist sql failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      int64_t ls_state = -1;
      common::sqlclient::ObMySQLResult *res = NULL;
      const uint64_t exec_tenant_id = get_private_table_exec_tenant_id(tenant_id);
      if (OB_FAIL(GCTX.sql_proxy_->read(result, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret),
            K(tenant_id), K(ls_id), K(exec_tenant_id), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret),
            K(tenant_id), K(ls_id), K(exec_tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        LOG_WARN("next failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
      } else if (OB_FAIL(res->get_int("ls_state", ls_state))) {
        LOG_WARN("fail to get ls_state", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_UNLIKELY(ls_state <= ObLSExistState::INVALID_STATE
          || ls_state >= ObLSExistState::MAX_STATE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected state value", KR(ret), K(ls_state), K(tenant_id), K(ls_id), K(sql));
      } else {
        state = ObLSExistState(ObLSExistState::State(ls_state));
      }
      LOG_INFO("check ls exist finished", KR(ret), K(tenant_id), K(ls_id), K(ls_state), K(state), K(sql));
    }
  }
  return ret;
}

// 1.relationship between ObLSStatus and ObLSExistState
//         <ObLSStatus>     <ObLSExistState>
//           CREATING   -->   UNCREATED
//         CREATE_ABORT -->   DELETED
//            OTHER     -->   EXISTING
//
// 2.check ls exist for sys and meta tenants only accesses __all_ls_status;
//   for user tenant need access __all_ls_status and __all_tenant_info;
int ObLocationService::construct_check_ls_exist_sql_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();
  if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(sql.append_fmt(
      "SELECT CASE WHEN exist = 1 THEN "
      "(CASE WHEN status = '%s' THEN %d WHEN status = '%s' THEN %d ELSE %d END) ",
      ls_status_to_str(ObLSStatus::OB_LS_CREATING),
      ObLSExistState::UNCREATED,
      ls_status_to_str(ObLSStatus::OB_LS_CREATE_ABORT),
      ObLSExistState::DELETED,
      ObLSExistState::EXISTING))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
  } else if (is_sys_tenant(tenant_id) || is_meta_tenant(tenant_id)) {
    if (OB_FAIL(sql.append_fmt("ELSE %d END AS ls_state ", ObLSExistState::UNCREATED))) {
      LOG_WARN("assign sql failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
    }
  } else if (is_user_tenant(tenant_id)) { // need max_ls_id
    if (OB_FAIL(sql.append_fmt(
        "ELSE (SELECT CASE WHEN max_ls_id < %ld THEN %d ELSE %d END FROM %s WHERE tenant_id = %lu) END AS ls_state ",
        ls_id.id(),
        ObLSExistState::UNCREATED,
        ObLSExistState::DELETED,
        OB_ALL_TENANT_INFO_TNAME,
        tenant_id))) {
      LOG_WARN("assign sql failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can't be here", KR(ret), K(tenant_id), K(ls_id), K(sql));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(sql.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql can't be empty", KR(ret), K(tenant_id), K(ls_id), K(sql));
  } else if (OB_FAIL(sql.append_fmt(
      "FROM (SELECT COUNT(*) > 0 as exist, status FROM %s WHERE tenant_id = %lu AND ls_id = %ld)",
      OB_ALL_LS_STATUS_TNAME,
      tenant_id,
      ls_id.id()))) {
    LOG_WARN("assign sql failed", KR(ret), K(tenant_id), K(ls_id), K(sql));
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
