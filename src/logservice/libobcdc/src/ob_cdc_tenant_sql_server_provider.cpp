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
 *
 * ObCDCTenantSQLServerProvider Impl
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_cdc_tenant_sql_server_provider.h"

#include "ob_log_config.h"                              // ObLogConfig
#include "ob_log_systable_helper.h"                     // ObLogSysTableHelper
#include "ob_log_svr_blacklist.h"                       // ObLogSvrBlacklist

namespace oceanbase
{
namespace libobcdc
{

ObCDCTenantSQLServerProvider::ObCDCTenantSQLServerProvider()
  : is_inited_(false),
    systable_helper_(NULL),
    tenant_list_(ObModIds::OB_LOG_SERVER_PROVIDER ,OB_MALLOC_NORMAL_BLOCK_SIZE),
    server_list_(ObModIds::OB_LOG_SERVER_PROVIDER ,OB_MALLOC_NORMAL_BLOCK_SIZE),
    tenant_server_map_(),
    server_blacklist_(),
    refresh_server_lock_()
{}

int ObCDCTenantSQLServerProvider::init(IObLogSysTableHelper &systable_helper)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant_sql_server_provider already inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tenant_server_map_.init(ObModIds::OB_SQL_CONNECTION_POOL))) {
    LOG_ERROR("tenant_server_map_ init failed", KR(ret));
  } else {
    systable_helper_ = &systable_helper;
    is_inited_ = true;
  }

  return ret;
}

void ObCDCTenantSQLServerProvider::destroy()
{
  if (IS_INIT) {
    is_inited_ = false;
    systable_helper_ = NULL;
    tenant_list_.destroy();
    server_list_.destroy();
    tenant_server_map_.reset();
    server_blacklist_.destroy();
  }
}

void ObCDCTenantSQLServerProvider::configure(const ObLogConfig &config)
{
  const char *sql_server_blacklist = config.sql_server_blacklist.str();
  LOG_INFO("[CONFIG]", KCSTRING(sql_server_blacklist));
  server_blacklist_.refresh(sql_server_blacklist);
}

int ObCDCTenantSQLServerProvider::del_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("cdc_tenant_server_provider not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_server_map_.del(tenant_id))) {
    LOG_WARN("del tenant_server_info from tenant_server_map_ failed", KR(ret), K(tenant_id));
  } else {
    // success
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::prepare_refresh()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant_sql_server_provider not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(refresh_server_lock_.wrlock())) { // lock with default timeout(INT64_MAX)
    LOG_ERROR("lock refresh_server_lock_ failed", KR(ret));
  } else if (OB_FAIL(query_all_server_())) {
    LOG_WARN("query_all_server_ failed", KR(ret));
  } else if (OB_FAIL(query_all_tenant_())) {
    LOG_WARN("query_all_tenant_ failed", KR(ret));
  } else if (OB_FAIL(query_tenant_server_())) {
    LOG_WARN("query_tenant_server_ failed", KR(ret));
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::end_refresh()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant_sql_server_provider not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(refresh_server_lock_.unlock())) {
    LOG_ERROR("release refresh_server_lock_ failed", KR(ret), K(this));
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::get_server(const int64_t svr_idx, common::ObAddr &server)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(svr_idx >= get_server_count())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("request server is not exist", KR(ret),
        K(svr_idx), "server_count", get_server_count());
  } else if (OB_FAIL(server_list_.at(svr_idx, server))) {
    LOG_ERROR("get server at specified position failed", KR(ret), K(svr_idx), K_(server_list));
  } else {
    // success
  }

  return ret;
}

int64_t ObCDCTenantSQLServerProvider::get_server_count() const
{
  int64_t server_count = 0;

  if (IS_INIT) {
    server_count = server_list_.count();
  }

  return server_count;
}

int ObCDCTenantSQLServerProvider::get_tenant_ids(common::ObIArray<uint64_t> &tenant_ids)
{
  int ret = OB_SUCCESS;
  tenant_ids.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tenant_ids.assign(tenant_list_))) {
    LOG_WARN("assign tenant_list_ to tenant_ids failed", KR(ret), K(tenant_list_), K(tenant_ids));
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::get_tenant_servers(
    const uint64_t tenant_id,
    common::ObIArray<common::ObAddr> &tenant_servers)
{
  int ret = OB_SUCCESS;
  tenant_servers.reset();
  TenantServerList *tenant_server_list = NULL;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(tenant_server_map_.get(tenant_id, tenant_server_list))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("tenant not exist in tenant_server_map_", KR(ret), K(tenant_id));
    }
  } else if (OB_FAIL(tenant_servers.assign(tenant_server_list->get_server_list()))) {
    LOG_ERROR("assign tenant_server_list failed", KR(ret), K(tenant_id), K(tenant_server_list));
  }

  if (OB_NOT_NULL(tenant_server_list)) {
    tenant_server_map_.revert(tenant_server_list);
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::refresh_server_list(void)
{
  return OB_SUCCESS;
}

int ObCDCTenantSQLServerProvider::query_all_tenant_()
{
  int ret = OB_SUCCESS;
  tenant_list_.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ should not be null", KR(ret));
  } else if (OB_FAIL(systable_helper_->query_tenant_id_list(tenant_list_))) {
    LOG_ERROR("get tenant_id_list by systable_helper_ failed", KR(ret));
  } else {
    // success
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::query_all_server_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ should not be null", KR(ret));
  } else {
    server_list_.reset();

    if (OB_FAIL(systable_helper_->query_sql_server_list(server_blacklist_, server_list_))) {
      LOG_ERROR("query_server_list failed", KR(ret));
    } else if (OB_UNLIKELY(0 == server_list_.count())) {
      ret = OB_NEED_RETRY;
      LOG_WARN("server_list query from cluster is empty", KR(ret));
    }
  }

  return ret;
}

int ObCDCTenantSQLServerProvider::query_tenant_server_()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("tenant_sql_server_provider is not inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(systable_helper_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("systable_helper_ should not be null", KR(ret));
  } else if (OB_UNLIKELY(0 == server_list_.count() || 0 == tenant_list_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect valid server_list and tenant_list");
  } else {
    for (int tenant_idx = 0; tenant_idx < tenant_list_.count(); tenant_idx++) {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      TenantServerList *tenant_server_list = NULL;

      if (OB_FAIL(tenant_list_.at(tenant_idx, tenant_id))) {
        LOG_WARN("get_tenant_id failed", KR(ret), K_(tenant_list), K(tenant_idx));
      } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expect valid tenant_id", KR(ret), K_(tenant_list), K(tenant_idx), K(tenant_id));
      } else if (OB_FAIL(tenant_server_map_.get(tenant_id, tenant_server_list))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          if (OB_FAIL(tenant_server_map_.create(tenant_id, tenant_server_list))) {
            LOG_ERROR("create tenant_server_list failed");
          }
        } else {
          LOG_ERROR("get tenant_server_list from tenant_server_map_ failed", KR(ret), K(tenant_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(tenant_server_list)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("tenant_server_list get/created from tenant_server_map_ should not be null", KR(ret), K(tenant_id));
        } else if (OB_FAIL(systable_helper_->query_tenant_sql_server_list(tenant_id, tenant_server_list->get_server_list()))) {
          LOG_ERROR("query_tenant_sql_server_list by systable_helper failed", KR(ret), K(tenant_id));
        } else if (OB_UNLIKELY(0 == tenant_server_list->get_server_count())) {
          share::schema::TenantStatus tenant_status = share::schema::TenantStatus::TENANT_STATUS_INVALID;

          if (OB_FAIL(systable_helper_->query_tenant_status(tenant_id, tenant_status))) {
            LOG_ERROR("query_tenant_status failed", KR(ret), K(tenant_id));
          } else if (share::schema::TenantStatus::TENANT_DELETED == tenant_status) {
            LOG_INFO("tenant_already dropped", K(tenant_id), K(tenant_status));
            ret = OB_ENTRY_NOT_EXIST;
          } else {
            LOG_WARN("tenant_server_list is empty but tenant is not dropped", K(tenant_id), K(tenant_server_list));
          }
        } else {
          LOG_DEBUG("find tenant servers", K(tenant_id), K(tenant_server_list));
        }

        if (OB_NOT_NULL(tenant_server_list)) {
          // revertt tenant_server_list anyway cause already get from tenant_server_map_ succ.
          tenant_server_map_.revert(tenant_server_list);
        }
      }
    }
  }

  return ret;
}

}// end namespace libobcdc
} // end namespace oceanbase
