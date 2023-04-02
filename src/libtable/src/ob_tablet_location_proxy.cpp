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

#define USING_LOG_PREFIX CLIENT
#include "ob_tablet_location_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/location_cache/ob_tablet_ls_service.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::table;

// select A.svr_ip, A.sql_port, A.table_id, A.role, B.svr_port from oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port where tenant_name = 'sys' and database_name='test' and table_name = 't2' and partition_id=0;

OB_INLINE bool ObTabletLocationProxy::inited() const
{
  return NULL != sql_client_;
}

int ObTabletLocationProxy::init(common::ObMySQLProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (inited()) {
    ret = OB_INIT_TWICE;
  } else {
    sql_client_ = &proxy;
  }
  return ret;
}

int ObTabletLocationProxy::get_tablet_location(const ObString &tenant,
                                               const uint64_t tenant_id,
                                               const ObString &db,
                                               const ObString &table,
                                               const uint64_t the_table_id,
                                               const ObTabletID tablet_id,
                                               bool force_renew,
                                               ObTabletLocation &location)
{
  int ret = OB_SUCCESS;
  UNUSED(the_table_id);
  UNUSED(force_renew);
  ObSqlString query_text;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(query_text.assign_fmt(
                "SELECT A.svr_ip, A.sql_port, A.table_id, A.role, A.replica_type, B.svr_port "
                "FROM oceanbase.%s A inner join oceanbase.%s B "
                "on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                "WHERE tenant_name = '%.*s' and database_name='%.*s' and table_name = '%.*s' and tablet_id=%ld",
                OB_ALL_VIRTUAL_PROXY_SCHEMA_TNAME,
                OB_ALL_SERVER_TNAME,
                tenant.length(), tenant.ptr(),
                db.length(), db.ptr(),
                table.length(), table.ptr(), tablet_id.id()))) {
      LOG_WARN("failed to format query text", K(ret), K(tenant), K(table));
    } else {
      LOG_INFO("query partition location", K(query_text));
      if (OB_FAIL(sql_client_->read(res, query_text.ptr()))) {
        LOG_WARN("execute sql failed", K(query_text), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql failed", K(query_text), K(ret));
      } else {
        location.reset();
        ObTabletReplicaLocation replica_location;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          replica_location.reset();
          if (OB_FAIL(cons_replica_location(*result, replica_location))) {
            LOG_WARN("failed to extract data", K(ret));
          } else if (OB_FAIL(location.add_replica_location(replica_location))) {
            LOG_WARN("failed to add", K(ret));
          }
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next result", K(ret));
        } else {
          location.set_tablet_id(tablet_id);
          location.set_tenant_id(tenant_id);
          location.set_renew_time(ObTimeUtility::current_time());
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTabletLocationProxy::cons_replica_location(const sqlclient::ObMySQLResult &res, ObTabletReplicaLocation &replica_location)
{
  int ret = OB_SUCCESS;
  ObAddr server;
  int64_t tmp_real_str_len = 0; // only used to fill output parameter，guarantee there is not any '\0' in the string
  char svr_ip[OB_IP_STR_BUFF] = "";
  int64_t svr_port = 0;
  EXTRACT_STRBUF_FIELD_MYSQL(res, "svr_ip", svr_ip, OB_IP_STR_BUFF, tmp_real_str_len);
  EXTRACT_INT_FIELD_MYSQL(res, "svr_port", svr_port, int64_t);
  (void)server.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port));
  int64_t sql_port;
  EXTRACT_INT_FIELD_MYSQL(res, "sql_port", sql_port, int64_t);
  int64_t role_value = 0;
  EXTRACT_INT_FIELD_MYSQL(res, "role", role_value, int64_t);
  int64_t replica_type = 0;
  EXTRACT_INT_FIELD_MYSQL(res, "replica_type", replica_type, int64_t);
  ObReplicaProperty replica_property;
  ObLSRestoreStatus restore_status;
  int64_t proposal_id = (LEADER == role_value ? 1 : 0);

  if (OB_SUCC(ret) && OB_FAIL(replica_location.init(server, static_cast<ObRole>(role_value),
                                                    sql_port, static_cast<ObReplicaType>(replica_type),
                                                    replica_property, restore_status, proposal_id))) {
    LOG_WARN("fail to init replica location", K(ret));
  }
  return ret;
}
////////////////////////////////////////////////////////////////
ObTabletLocationCache::~ObTabletLocationCache()
{
  LOG_INFO("destruct ObTabletLocationCache");
}

bool ObTabletLocationCache::is_valid_key(const uint64_t tenant_id, const common::ObTabletID tablet_id)
{
  return tablet_id.is_valid_with_tenant(tenant_id) && !tablet_id.is_reserved_tablet();
}

int ObTabletLocationCache::init(ObTabletLocationProxy &location_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet location cache has already inited", K(ret));
  } else if (OB_FAIL(location_cache_.init(CACHE_NAME, CACHE_PRIORITY))) {
    LOG_WARN("user tablet location cache init failed", K(ret));
  } else {
    sem_.set_max_count(LOCATION_RENEW_CONCURRENCY);
    location_proxy_ = &location_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletLocationCache::get_from_cache(const uint64_t tenant_id,
                                          const ObTabletID tablet_id,
                                          ObTabletLocation &location)
{
  int ret = OB_SUCCESS;
  ObTabletLocationCacheKey cache_key(tenant_id, tablet_id);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!cache_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KT(tenant_id), K(tablet_id));
  } else {
    ObKVCacheHandle handle;
    const ObTabletLocation *cache_value = NULL;
    if (OB_FAIL(location_cache_.get(cache_key, cache_value, handle))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        ret = OB_CACHE_NOT_HIT;
        LOG_WARN("get location from user cache failed", KR(ret), K(cache_key));
      } else {
        LOG_INFO("[LOCATION] cache miss", K(cache_key), KR(ret));
      }
    } else if (OB_FAIL(location.assign(*cache_value))) {
      LOG_WARN("assign tablet location failed", KR(ret), KP(cache_value), K(location));
    } else {
      LOG_TRACE("location hit in user cache", KR(ret), K(cache_key), K(location));
    }
  }
  return ret;
}

int ObTabletLocationCache::put_to_cache(const uint64_t tenant_id,
                                        const ObTabletID tablet_id,
                                        const ObTabletLocation &location)
{
  int ret = OB_SUCCESS;
  ObTabletLocationCacheKey cache_key(tenant_id, tablet_id);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!cache_key.is_valid() || !location.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cache_key), K(location));
  } else if (OB_FAIL(location_cache_.put(cache_key, location))) {
    LOG_WARN("put location to user location cache failed",
              K(cache_key), K(location), K(ret));
  } else {
    LOG_TRACE("renew location in user_cache succeed", K(cache_key), K(location));
  }
  return ret;
}

int ObTabletLocationCache::renew_get_tablet_location(const common::ObString &tenant,
                                                     const uint64_t tenant_id,
                                                     const common::ObString &db,
                                                     const common::ObString &table,
                                                     const uint64_t table_id,
                                                     const common::ObTabletID tablet_id,
                                                     ObTabletLocation &location)
{
  int ret = OB_SUCCESS;
  // timeout control
  int64_t start_ts = ObTimeUtility::current_time();
  int64_t abs_timeout_ts = start_ts + DEFAULT_FETCH_LOCATION_TIMEOUT_US;
  ObTimeoutCtx timeout_ctx;  // link self
  timeout_ctx.set_abs_timeout(abs_timeout_ts);
  abs_timeout_ts = ObTimeoutCtx::get_ctx().get_abs_timeout();
  // ignore acquire fail
  int tmp_ret = sem_.acquire(abs_timeout_ts);
  bool sem_acquired = true;
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("acquire failed", K(tmp_ret));
    if (OB_TIMEOUT == tmp_ret) {
      ret = OB_TIMEOUT;
      LOG_WARN("already timeout", K(ret), K(abs_timeout_ts));
    }
  } else {
    sem_acquired = true;
  }
  if (OB_SUCC(ret)) {
    // check cache to see whether other threads have already fill the cache
    bool did_fetch_and_renew = true;
    if (OB_FAIL(get_from_cache(tenant_id, tablet_id, location))) {
      // continue
      ret = OB_SUCCESS;
      did_fetch_and_renew = true;
    } else {
      did_fetch_and_renew = (location.get_renew_time() <= start_ts);
    }
    if (did_fetch_and_renew) {
      // renew and put it to the cache
      if (OB_FAIL(location_proxy_->get_tablet_location(tenant, tenant_id, db, table, table_id, tablet_id, true, location))) {
        LOG_WARN("failed to get location from proxy", K(ret));
      } else {
        LOG_INFO("[LOCATION] get from proxy", K(location));
        if (OB_FAIL(put_to_cache(tenant_id, tablet_id, location))) {
          LOG_WARN("failed to put to cache", K(ret), K(location));
          // ignore error
          ret = OB_SUCCESS;
        } else {
          LOG_DEBUG("[LOCATION] put to cache", K(location));
        }
      }
    }
  }
  if (sem_acquired) {
    // ignore release fail
    int tmp_ret = sem_.release();
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("release failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObTabletLocationCache::get_tablet_location(const common::ObString &tenant,
                                               const uint64_t tenant_id,
                                               const common::ObString &db,
                                               const common::ObString &table,
                                               const uint64_t table_id,
                                               const common::ObTabletID tablet_id,
                                               bool force_renew,
                                               share::ObTabletLocation &location)
{
  int ret = OB_SUCCESS;
  if (force_renew) {
    if (OB_FAIL(renew_get_tablet_location(tenant, tenant_id, db, table, table_id, tablet_id, location))) {
      LOG_WARN("failed to renew location", K(ret), K(tenant), K(db), K(table));
    }
  } else {
    if (OB_FAIL(get_from_cache(tenant_id, tablet_id, location))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(renew_get_tablet_location(tenant, tenant_id, db, table, table_id, tablet_id, location))) {
          LOG_WARN("failed to renew location", K(ret), K(tenant), K(db), K(table));
        }
      } else {
        LOG_WARN("failed to get from cache", K(ret), K(tenant), K(db), K(table));
      }
    }
  }
  return ret;
}
