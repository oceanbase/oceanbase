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

#include "storage/ob_locality_manager.h"
#include "storage/ob_partition_service.h"
#include "share/ob_locality_priority.h"
#include "observer/ob_server_struct.h"
#include "clog/ob_partition_log_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_remote_sql_proxy.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace storage {
ObLocalityManager::ObLocalityManager() : rwlock_(), ssl_invited_nodes_buf_(NULL)
{
  reset();
  ssl_invited_nodes_buf_ = new (std::nothrow) char[common::OB_MAX_CONFIG_VALUE_LEN];
  ssl_invited_nodes_buf_[0] = '\0';
}

void ObLocalityManager::reset()
{
  is_inited_ = false;
  self_.reset();
  sql_proxy_ = NULL;
  locality_info_.reset();
  server_locality_cache_.reset();
  remote_sql_proxy_ = NULL;
  remote_server_locality_cache_.reset();
  is_loaded_ = false;
}

void ObLocalityManager::destroy()
{
  if (NULL != ssl_invited_nodes_buf_) {
    delete[] ssl_invited_nodes_buf_;
    ssl_invited_nodes_buf_ = NULL;
  }
  if (is_inited_) {
    is_inited_ = false;
    locality_info_.destroy();
    server_locality_cache_.destroy();
    remote_server_locality_cache_.destroy();
    STORAGE_LOG(INFO, "ObLocalityManager destroy finished");
  }
}

int ObLocalityManager::init(const ObAddr& self, ObMySQLProxy* sql_proxy, ObRemoteSqlProxy* remote_sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLocalityManager init twice", K(ret));
  } else if (!self.is_valid() || OB_ISNULL(sql_proxy) || OB_ISNULL(remote_sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(self), KP(sql_proxy));
  } else if (OB_FAIL(server_locality_cache_.init())) {
    STORAGE_LOG(WARN, "server_locality_cache_ init failed", K(ret), K(self));
  } else if (OB_FAIL(remote_server_locality_cache_.init())) {
    STORAGE_LOG(WARN, "remote server locality cache init failed", KR(ret));
  } else {
    self_ = self;
    sql_proxy_ = sql_proxy;
    remote_sql_proxy_ = remote_sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObLocalityManager::is_server_legitimate(const ObAddr& addr, bool& is_valid)
{
  int ret = OB_SUCCESS;
  ObArray<ObServerLocality> server_locality_array;
  bool has_readonly_zone = false;
  SpinRLockGuard guard(rwlock_);
  is_valid = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(server_locality_cache_.get_server_locality_array(server_locality_array, has_readonly_zone))) {
    STORAGE_LOG(WARN, "fail to get server locality array", K(ret));
  } else if (server_locality_array.count() <= 0) {
    STORAGE_LOG(INFO, "check server legitimate, wait load server list");
  } else {
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < server_locality_array.count(); i++) {
      if (addr.is_equal_except_port(server_locality_array.at(i).get_addr())) {
        find = true;
      }
    }
    if (OB_SUCC(ret) && !find) {
      is_valid = false;
    }
  }
  return ret;
}

int ObLocalityManager::check_ssl_invited_nodes(easy_connection_t& c)
{
  int ret = OB_SUCCESS;
  bool use_ssl = false;
  if (OB_UNLIKELY(!is_inited_) || OB_ISNULL(c.handler)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(is_inited_), KP(c.handler), K(ret));
  } else {
    SpinRLockGuard guard(rwlock_);
    ObString ssl_invited_nodes(ssl_invited_nodes_buf_);
    if (1 == c.handler->is_ssl && 0 == c.handler->is_ssl_opt) {
      if (ssl_invited_nodes.empty() || 0 == ssl_invited_nodes.case_compare("NONE")) {
        // do nothing
      } else if (0 == ssl_invited_nodes.case_compare("ALL")) {
        use_ssl = true;
      } else {
        char ip_buffer[MAX_IP_ADDR_LENGTH] = {};
        easy_addr_t tmp_addr = c.addr;
        tmp_addr.port = 0;  // mark it invalied, not care it
        char* clinet_ip = easy_inet_addr_to_str(&tmp_addr, ip_buffer, 32);
        if (NULL != strstr(ssl_invited_nodes.ptr(), clinet_ip) && self_.ip_to_string(ip_buffer, MAX_IP_ADDR_LENGTH) &&
            NULL != strstr(ssl_invited_nodes.ptr(), ip_buffer))
          use_ssl = true;
      }
    }
    STORAGE_LOG(INFO,
        "rpc connection accept",
        "local_addr",
        self_,
        "dest",
        easy_connection_str(&c),
        K(use_ssl),
        K(ssl_invited_nodes));
  }
  c.ssl_sm_ = use_ssl ? SSM_USE_SSL : SSM_NONE;
  return ret;
}

void ObLocalityManager::set_ssl_invited_nodes(const common::ObString& new_value)
{
  if (OB_LIKELY(NULL != (ssl_invited_nodes_buf_)) && 0 != new_value.case_compare(ssl_invited_nodes_buf_)) {
    SpinWLockGuard guard(rwlock_);
    if (new_value.empty() || new_value.length() >= common::OB_MAX_CONFIG_VALUE_LEN - 1) {
      ssl_invited_nodes_buf_[0] = '\0';
    } else {
      MEMCPY(ssl_invited_nodes_buf_, new_value.ptr(), new_value.length());
      ssl_invited_nodes_buf_[new_value.length()] = '\0';
    }
  }
  STORAGE_LOG(INFO, "set_ssl_invited_nodes", K(new_value));
}

int ObLocalityManager::load_region()
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService*& schema_service = GCTX.schema_service_;
  int64_t schema_version = OB_INVALID_VERSION;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "schema service is null", K(ret));
  } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(OB_SYS_TENANT_ID, schema_version))) {
    STORAGE_LOG(WARN, "failed to get schema guard", K(ret));
  } else {
    // Firstly, check it's need to warn
    check_if_locality_has_been_loaded();
    ObLocalityInfo locality_info;
    ObAddr empty_addr;
    ObLocalityInfo empty_locality_info;
    bool is_self_cluster = true;
    if (OB_FAIL(locality_operator_.load_region(
            self_, is_self_cluster, *sql_proxy_, locality_info, server_locality_cache_))) {
      STORAGE_LOG(WARN, "localitity operator load region error", K(ret));
    } else if (GCTX.is_standby_cluster() && OB_FAIL(locality_operator_.load_region(empty_addr,
                                                !is_self_cluster,
                                                *remote_sql_proxy_,
                                                empty_locality_info,
                                                remote_server_locality_cache_))) {
      STORAGE_LOG(WARN, "fail to load region", KR(ret));
    } else if (OB_FAIL(set_locality_info(locality_info))) {
      STORAGE_LOG(WARN, "set locality_info fail", K(ret), K(locality_info));
    } else if (OB_FAIL(set_partition_region_priority())) {
      STORAGE_LOG(WARN, "set region priority  fail", K(ret), K_(locality_info));
    } else if (OB_FAIL(set_version(schema_version))) {
      STORAGE_LOG(WARN, "set version fail", K(ret));
    } else if (!is_loaded_) {
      is_loaded_ = true;
    }
  }
  return ret;
}

int ObLocalityManager::set_partition_region_priority()
{
  int ret = OB_SUCCESS;
  int first_error_code = OB_SUCCESS;
  ObIPartitionGroupIterator* iter = NULL;
  ObIPartitionGroup* partition = NULL;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!locality_info_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "locality_info is invalid", K(ret));
  } else if (NULL == (iter = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "fail to alloc partition scan iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(partition))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "scan next partition failed", K(ret));
        }
        break;
      } else if (NULL == partition) {
        ret = OB_PARTITION_NOT_EXIST;
      } else {
        clog::ObIPartitionLogService* pls = partition->get_log_service();
        const uint64_t tenant_id = partition->get_partition_key().get_tenant_id();
        uint64_t region_priority = UINT64_MAX;
        if (OB_ISNULL(pls)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "partition log service is NULL", KP(pls));
        } else if (OB_FAIL(locality_info_.get_region_priority(tenant_id, region_priority))) {
          STORAGE_LOG(WARN, "ObLocalityInfo get_region_priority error", K(ret), K(tenant_id));
        } else if (!locality_info_.local_region_.is_empty() && OB_FAIL(pls->set_region(locality_info_.local_region_))) {
          STORAGE_LOG(WARN, "ObPartitionService set region error", K(ret), K_(locality_info));
        } else if (!locality_info_.local_idc_.is_empty() && OB_FAIL(pls->set_idc(locality_info_.local_idc_))) {
          STORAGE_LOG(WARN, "ObPartitionService set idc error", K(ret), K_(locality_info));
        } else {
          pls->set_zone_priority(region_priority);
          STORAGE_LOG(DEBUG,
              "ObPartitionService set_zone_priority success",
              "pkey",
              partition->get_partition_key(),
              K(tenant_id),
              K(region_priority));
        }
        // In order to avoid set single partition's region failed, can skip temporarily,
        // and continue to execute, and return to the version of the error code when the first error was reported
        if (OB_FAIL(ret)) {
          if (OB_SUCCESS == first_error_code) {
            first_error_code = ret;
          }
          ret = OB_SUCCESS;
        }
      }
    }  // while
  }
  if (NULL != iter) {
    ObPartitionService::get_instance().revert_pg_iter(iter);
  }
  if (OB_SUCCESS != first_error_code) {
    ret = first_error_code;
  }
  return ret;
}

int ObLocalityManager::load_zone()
{
  int ret = OB_SUCCESS;
  ObLocalityInfo locality_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(locality_operator_.load_zone(self_, *sql_proxy_, locality_info))) {
    STORAGE_LOG(WARN, "localitity operator load zone error", K(ret));
  } else if (OB_FAIL(set_locality_info(locality_info))) {
    STORAGE_LOG(WARN, "set locality_info fail", K(ret), K(locality_info));
  } else {
    STORAGE_LOG(INFO, "localitity operator load zone success", K_(locality_info));
  }

  return ret;
}

int ObLocalityManager::get_region_priority(const uint64_t tenant_id, uint64_t& region_priority)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  region_priority = UINT64_MAX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(locality_info_.get_region_priority(tenant_id, region_priority))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      STORAGE_LOG(WARN, "get_region_priority error", K(ret));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObLocalityManager::get_local_region(ObRegion& region) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  region.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init");
  } else {
    region = locality_info_.local_region_;
  }

  return ret;
}

int ObLocalityManager::get_local_zone_type(ObZoneType& zone_type)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  zone_type = ObZoneType::ZONE_TYPE_INVALID;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init");
  } else {
    zone_type = locality_info_.get_local_zone_type();
  }

  return ret;
}
int ObLocalityManager::get_locality_zone(const uint64_t tenant_id, ObLocalityZone& locality_zone)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(locality_info_.get_locality_zone(tenant_id, locality_zone))) {
    // STORAGE_LOG(WARN, "get locality_item error", K(ret), K(tenant_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObLocalityManager::get_version(int64_t& version) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else {
    version = locality_info_.get_version();
  }
  return ret;
}

int ObLocalityManager::get_server_locality_array(
    ObIArray<ObServerLocality>& server_locality_array, bool& has_readonly_zone) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(server_locality_cache_.get_server_locality_array(server_locality_array, has_readonly_zone))) {
    STORAGE_LOG(WARN, "fail to get server locality array", K(ret));
  }
  return ret;
}

int ObLocalityManager::get_server_region(const common::ObAddr& server, common::ObRegion& region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.get_server_region(server, region))) {
    STORAGE_LOG(WARN, "fail to get server region", K(ret), K(server));
  }
  return ret;
}

int ObLocalityManager::get_server_region_across_cluster(const common::ObAddr& server, common::ObRegion& region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.get_server_region(server, region))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "fail to get server region", K(ret), K(server));
    } else if (!GCTX.is_standby_cluster()) {
      // nothing todo
    } else if (OB_FAIL(remote_server_locality_cache_.get_server_region(server, region))) {
      STORAGE_LOG(WARN, "fail to get server region from remote_server_locality_cache", KR(ret), K(server));
    }
  }
  return ret;
}

int ObLocalityManager::get_noempty_zone_region(const common::ObZone& zone, common::ObRegion& region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(server_locality_cache_.get_noempty_zone_region(zone, region))) {
    STORAGE_LOG(WARN, "fail to get server zone", K(ret), K(zone));
  }
  return ret;
}

int ObLocalityManager::get_server_zone(const common::ObAddr& server, common::ObZone& zone) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.get_server_zone(server, zone))) {
    STORAGE_LOG(WARN, "fail to get server zone", K(ret), K(server));
  }
  return ret;
}

int ObLocalityManager::record_server_region(const common::ObAddr& server, const common::ObRegion& region)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid() || region.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(region));
  } else if (OB_FAIL(server_locality_cache_.record_server_region(server, region))) {
    STORAGE_LOG(WARN, "record_server_region failed", K(ret), K(server), K(region));
  } else {
    // do nothing
  }
  return ret;
}

int ObLocalityManager::get_server_idc(const common::ObAddr& server, common::ObIDC& idc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.get_server_idc(server, idc))) {
    STORAGE_LOG(WARN, "fail to get server idc", K(ret), K(server));
  } else {
    // do nothing
  }
  return ret;
}

int ObLocalityManager::record_server_idc(const common::ObAddr& server, const common::ObIDC& idc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid() || idc.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server), K(idc));
  } else if (OB_FAIL(server_locality_cache_.record_server_idc(server, idc))) {
    STORAGE_LOG(WARN, "record_server_idc failed", K(ret), K(server), K(idc));
  } else {
    // do nothing
  }
  return ret;
}

int ObLocalityManager::get_server_cluster_id(const common::ObAddr& server, int64_t& cluster_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.get_server_cluster_id(server, cluster_id))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "get_server_cluster_id failed", K(ret), K(server));
    }
  }
  return ret;
}

int ObLocalityManager::record_server_cluster_id(const common::ObAddr& server, const int64_t& cluster_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.record_server_cluster_id(server, cluster_id))) {
    STORAGE_LOG(WARN, "record_server_cluster_id failed", K(ret), K(server));
  } else {
    // do nothing
  }
  return ret;
}

int ObLocalityManager::is_local_server(const ObAddr& server, bool& is_local)
{
  int ret = OB_SUCCESS;
  is_local = false;
  ObArray<ObServerLocality> server_locality_array;
  ObRegion local_region;
  bool has_readonly_zone = false;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(server_locality_cache_.get_server_locality_array(server_locality_array, has_readonly_zone))) {
    STORAGE_LOG(WARN, "fail to get server locality array", K(ret));
  } else {
    local_region = locality_info_.local_region_;
    bool find = false;
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < server_locality_array.count(); i++) {
      if (server == server_locality_array.at(i).get_addr()) {
        find = true;
        if (server_locality_array.at(i).get_region() == local_region) {
          is_local = true;
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ENTRY_NOT_EXIST;
      STORAGE_LOG(DEBUG, "fail to find server locality info", K(ret), K(server));
    }
  }
  return ret;
}

int ObLocalityManager::is_same_zone(const common::ObAddr& server, bool& is_same_zone)
{
  int ret = OB_SUCCESS;
  ObZone self_zone;
  ObZone svr_zone;
  is_same_zone = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(server));
  } else if (OB_FAIL(get_server_zone(self_, self_zone))) {
    STORAGE_LOG(WARN, "fail to get self zone", K(ret), K(self_));
  } else if (OB_FAIL(get_server_zone(server, svr_zone))) {
    STORAGE_LOG(WARN, "fail to get server zone", K(ret), K(server));
  } else if (self_zone == svr_zone) {
    is_same_zone = true;
  }
  return ret;
}

int ObLocalityManager::check_if_locality_has_been_loaded()
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!is_loaded_) {
    const int64_t start_service_time = GCTX.start_service_time_;
    if (start_service_time > 0) {
      const int64_t now = ObTimeUtility::current_time();
      // When the observer does not flash out the cache for a long time after starting,
      // you need to report an error
      if (now - start_service_time > FAIL_TO_LOAD_LOCALITY_CACHE_TIMEOUT) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN,
            "fail to load first cache since service started!",
            K(ret),
            K(now),
            K(start_service_time),
            K_(locality_info));
      }
    }
  }
  return ret;
}

int ObLocalityManager::get_locality_info(share::ObLocalityInfo& locality_info)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(locality_info_.copy_to(locality_info))) {
    STORAGE_LOG(WARN, "get locality_info fail", K(ret), K_(locality_info));
  }
  return ret;
}

int ObLocalityManager::set_locality_info(share::ObLocalityInfo& locality_info)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (!locality_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "locality_info is invalid", K(ret));
  } else if (OB_FAIL(locality_info.copy_to(locality_info_))) {
    STORAGE_LOG(WARN, "set locality_info fail", K(ret), K_(locality_info));
  }
  return ret;
}

int ObLocalityManager::is_local_zone_read_only(bool& is_readonly)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  is_readonly = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (common::ObZoneType::ZONE_TYPE_READONLY == locality_info_.get_local_zone_type()) {
    is_readonly = true;
  }
  return ret;
}

int ObLocalityManager::set_version(int64_t version)
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else {
    locality_info_.set_version(version);
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
