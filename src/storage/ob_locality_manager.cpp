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
#include "share/ob_locality_priority.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "rpc/obmysql/obsm_struct.h"                // easy_connection_str
#include "share/schema/ob_multi_version_schema_service.h"
#ifdef OB_BUILD_ARBITRATION
#include "share/arbitration_service/ob_arbitration_service_info.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{
ObLocalityManager::ObLocalityManager()
  : rwlock_(ObLatchIds::SERVER_LOCALITY_MGR_LOCK),
    ssl_invited_nodes_buf_(NULL)
{
  reset();
  ssl_invited_nodes_buf_ = new (std::nothrow) char[common::OB_MAX_CONFIG_VALUE_LEN];
  ssl_invited_nodes_buf_[0]  = '\0';
}

void ObLocalityManager::reset()
{
  is_inited_ = false;
  self_.reset();
#ifdef OB_BUILD_ARBITRATION
  arb_service_addr_.reset();
#endif
  sql_proxy_ = NULL;
  locality_info_.reset();
  server_locality_cache_.reset();
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
    TG_DESTROY(lib::TGDefIDs::LocalityReload);
    locality_info_.destroy();
    server_locality_cache_.destroy();
    refresh_locality_task_queue_.destroy();
    STORAGE_LOG(INFO, "ObLocalityManager destroy finished");
  }
}

int ObLocalityManager::init(const ObAddr &self, ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLocalityManager init twice", K(ret));
  } else if (!self.is_valid() || OB_ISNULL(sql_proxy)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(self), KP(sql_proxy));
  } else if (OB_FAIL(server_locality_cache_.init())) {
    STORAGE_LOG(WARN, "server_locality_cache_ init failed", K(ret), K(self));
  } else if (OB_FAIL(refresh_locality_task_queue_.init(1,
                                                       "LocltyRefTask",
                                                       REFRESH_LOCALITY_TASK_NUM,
                                                       REFRESH_LOCALITY_TASK_NUM))) {
    STORAGE_LOG(WARN, "fail to initialize refresh locality task queue", K(ret));
  } else if (OB_FAIL(reload_locality_task_.init(this))) {
    STORAGE_LOG(WARN, "init reload locality task failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::LocalityReload))) {
    STORAGE_LOG(WARN, "fail to initialize locality timer");
  } else {
    self_ = self;
    sql_proxy_ = sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObLocalityManager::start()
{
  int ret = OB_SUCCESS;
  bool repeat = true;
  STORAGE_LOG(INFO, "start locality manager");
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(ERROR, "locality manager not inited, cannot start.");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::LocalityReload,
                                 reload_locality_task_,
                                 RELOAD_LOCALITY_INTERVAL,
                                 repeat))) {
    STORAGE_LOG(ERROR, "fail to schedule reload locality task");
  }
  return ret;
}

int ObLocalityManager::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "locality manager not inited, cannot stop.", K(ret));
  } else {
    TG_STOP(lib::TGDefIDs::LocalityReload);
    refresh_locality_task_queue_.stop();
  }
  return ret;
}

int ObLocalityManager::wait()
{
  int ret = OB_SUCCESS;
  TG_WAIT(lib::TGDefIDs::LocalityReload);
  refresh_locality_task_queue_.wait();
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
  } else if (OB_FAIL(server_locality_cache_.get_server_locality_array(
                     server_locality_array,
                     has_readonly_zone))) {
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

int ObLocalityManager::check_ssl_invited_nodes(easy_connection_t &c)
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
        //do nothing
      } else if (0 == ssl_invited_nodes.case_compare("ALL")) {
        use_ssl = true;
      } else {
        char ip_buffer[MAX_IP_ADDR_LENGTH] = {};
        easy_addr_t tmp_addr = c.addr;
        tmp_addr.port = 0;//mark it invalied, not care it
        char *clinet_ip = easy_inet_addr_to_str(&tmp_addr, ip_buffer, sizeof(ip_buffer));
        if (NULL != strstr(ssl_invited_nodes.ptr(), clinet_ip)
            && self_.ip_to_string(ip_buffer, MAX_IP_ADDR_LENGTH)
            && NULL != strstr(ssl_invited_nodes.ptr(), ip_buffer))
        use_ssl = true;
      }
    }
    STORAGE_LOG(INFO, "rpc connection accept", "local_addr", self_, "dest", easy_connection_str(&c),
             K(use_ssl), K(ssl_invited_nodes));
  }
  c.ssl_sm_ = use_ssl ? SSM_USE_SSL: SSM_NONE;
  return ret;
}

void ObLocalityManager::set_ssl_invited_nodes(const common::ObString &new_value)
{
  if (OB_LIKELY(NULL != (ssl_invited_nodes_buf_))
      && 0 != new_value.case_compare(ssl_invited_nodes_buf_)) {
    SpinWLockGuard guard(rwlock_);
    if (new_value.empty() || new_value.length() >= common::OB_MAX_CONFIG_VALUE_LEN - 1) {
      ssl_invited_nodes_buf_[0] = '\0';
    } else {
      MEMCPY(ssl_invited_nodes_buf_, new_value.ptr(), new_value.length());
      ssl_invited_nodes_buf_[new_value.length()] =  '\0';
    }
  }
  STORAGE_LOG(INFO, "set_ssl_invited_nodes", K(new_value));
}

int ObLocalityManager::load_region()
{
  int ret = OB_SUCCESS;
  share::schema::ObMultiVersionSchemaService *&schema_service = GCTX.schema_service_;
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
    //Firstly, check it's need to warn
    check_if_locality_has_been_loaded();
    HEAP_VARS_2((ObLocalityInfo, locality_info),
                (ObLocalityInfo, empty_locality_info)) {
      ObAddr empty_addr;
      bool is_self_cluster = true;
      if (OB_FAIL(locality_operator_.load_region(self_,
                                                 is_self_cluster,
                                                 *sql_proxy_,
                                                 locality_info,
                                                 server_locality_cache_))) {
        STORAGE_LOG(WARN, "localitity operator load region error", K(ret));
      } else if (OB_FAIL(set_locality_info(locality_info))) {
        STORAGE_LOG(WARN, "set locality_info fail", K(ret), K(locality_info));
      } else if (OB_FAIL(set_version(schema_version))) {
        STORAGE_LOG(WARN, "set version fail", K(ret));
      } else if (!is_loaded_) {
        is_loaded_ = true;
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_ARBITRATION
int ObLocalityManager::load_arb_service_info()
{
  int ret = OB_SUCCESS;
  const ObString arbitration_service_key("default");
  const bool lock_line = false;
  ObArbitrationServiceInfo arb_service_info;
  ObAddr arb_service_addr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(arbitration_service_table_operator_.get(
                         *sql_proxy_,
                         arbitration_service_key,
                         lock_line,
                         arb_service_info))) {
    STORAGE_LOG(WARN, "fail to get arbitration service info", KR(ret), K(arbitration_service_key),
             K(lock_line), K(arb_service_info));
    if (OB_ARBITRATION_SERVICE_NOT_EXIST == ret) {
      SpinWLockGuard guard(rwlock_);
      arb_service_addr_.reset();
    }
  } else if (OB_FAIL(arb_service_addr.parse_from_string(arb_service_info.get_arbitration_service_string()))) {
    STORAGE_LOG(WARN, "parse_from_string failed", K(ret));
  } else if (arb_service_addr == arb_service_addr_) {
    // no need update
  } else {
    SpinWLockGuard guard(rwlock_);
    arb_service_addr_ = arb_service_addr;
  }
  STORAGE_LOG(INFO, "load_arb_service_info finshed", K(ret), K_(arb_service_addr));
  return ret;
}

int ObLocalityManager::get_arb_service_addr(common::ObAddr &arb_service_addr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    STORAGE_LOG(ERROR, "locality manager not inited, cannot start.");
    ret = OB_NOT_INIT;
  } else {
    SpinRLockGuard guard(rwlock_);
    arb_service_addr = arb_service_addr_;
  }
  return ret;
}
#endif

int ObLocalityManager::get_local_region(ObRegion &region) const
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

int ObLocalityManager::get_local_zone_type(ObZoneType &zone_type)
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
int ObLocalityManager::get_locality_zone(const uint64_t tenant_id, ObLocalityZone &locality_zone)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(locality_info_.get_locality_zone(tenant_id, locality_zone))) {
    //STORAGE_LOG(WARN, "get locality_item error", K(ret), K(tenant_id));
  } else {
    // do nothing
  }

  return ret;
}

int ObLocalityManager::get_version(int64_t &version) const
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
    ObIArray<ObServerLocality> &server_locality_array,
    bool &has_readonly_zone) const
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (OB_FAIL(server_locality_cache_.get_server_locality_array(
                     server_locality_array,
                     has_readonly_zone))) {
    STORAGE_LOG(WARN, "fail to get server locality array", K(ret));
  }
  return ret;
}

int ObLocalityManager::get_server_zone_type(const common::ObAddr &server,
                                         common::ObZoneType &zone_type) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", KR(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(server));
  } else if (OB_FAIL(server_locality_cache_.get_server_zone_type(server, zone_type))) {
    STORAGE_LOG(WARN, "fail to get server zone type", KR(ret), K(server));
  }
  return ret;
}

int ObLocalityManager::get_server_region(const common::ObAddr &server,
                                         common::ObRegion &region) const
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

int ObLocalityManager::get_noempty_zone_region(
    const common::ObZone &zone,
    common::ObRegion &region) const
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

int ObLocalityManager::get_server_zone(const common::ObAddr &server,
                                       common::ObZone &zone) const
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

int ObLocalityManager::record_server_region(const common::ObAddr &server, const common::ObRegion &region)
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

int ObLocalityManager::get_server_idc(const common::ObAddr &server,
                                      common::ObIDC &idc) const
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

int ObLocalityManager::record_server_idc(const common::ObAddr &server, const common::ObIDC &idc)
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

int ObLocalityManager::get_server_cluster_id(const common::ObAddr &server,
                                             int64_t &cluster_id) const
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

int ObLocalityManager::record_server_cluster_id(const common::ObAddr &server, const int64_t &cluster_id)
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

int ObLocalityManager::is_local_server(const ObAddr &server, bool &is_local)
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
  } else if (OB_FAIL(server_locality_cache_.get_server_locality_array(
                     server_locality_array,
                     has_readonly_zone))) {
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

int ObLocalityManager::is_same_zone(const common::ObAddr &server, bool &is_same_zone)
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
      //当observer启动后很长一段时间没刷出来cache，需要报error
      if (now - start_service_time > FAIL_TO_LOAD_LOCALITY_CACHE_TIMEOUT) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to load first cache since service started!",
                    K(ret), K(now), K(start_service_time), K_(locality_info));
      }
    }
  }
  return ret;
}

int ObLocalityManager::get_locality_info(share::ObLocalityInfo &locality_info)
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

int ObLocalityManager::set_locality_info(share::ObLocalityInfo &locality_info)
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

int ObLocalityManager::is_local_zone_read_only(bool &is_readonly)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard guard(rwlock_);
  is_readonly = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLocalityManager not init", K(ret));
  } else if (common::ObZoneType::ZONE_TYPE_READONLY == locality_info_.get_local_zone_type()){
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

int ObLocalityManager::add_refresh_locality_task()
{
  int ret = OB_SUCCESS;
  ObRefreshLocalityTask task(this);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "locality is not initialized", K(ret));
  } else if (OB_FAIL(refresh_locality_task_queue_.add_task(task))) {
    if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "add refresh locality task failed", K(ret));
    }
  }
  return ret;
}

ObLocalityManager::ReloadLocalityTask::ReloadLocalityTask()
  : is_inited_(false),
    locality_mgr_(NULL)
{
}

int ObLocalityManager::ReloadLocalityTask::init(ObLocalityManager *locality_mgr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ReloadLocalityTask init twice", K(ret));
  } else if (OB_ISNULL(locality_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(locality_mgr));
  } else {
    is_inited_ = true;
    locality_mgr_ = locality_mgr;
  }

  return ret;
}

void ObLocalityManager::ReloadLocalityTask::destroy()
{
  is_inited_ = false;
  locality_mgr_ = NULL;
}

void ObLocalityManager::ReloadLocalityTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ReloadLocalityTask not init", K(ret));
  } else if (OB_FAIL(locality_mgr_->add_refresh_locality_task())) {
    STORAGE_LOG(WARN, "runTimer to refresh locality_info fail", K(ret));
  } else {
    STORAGE_LOG(INFO, "runTimer to refresh locality_info", K(ret));
  }
}

ObLocalityManager::ObRefreshLocalityTask::ObRefreshLocalityTask(
    ObLocalityManager *locality_mgr)
    : IObDedupTask(T_REFRESH_LOCALITY),
      locality_mgr_(locality_mgr)
{
}

ObLocalityManager::ObRefreshLocalityTask::~ObRefreshLocalityTask()
{
}

int64_t ObLocalityManager::ObRefreshLocalityTask::hash() const
{
  uint64_t hash_val = 0;
  return static_cast<int64_t>(hash_val);
}

bool ObLocalityManager::ObRefreshLocalityTask::operator ==(const IObDedupTask &other) const
{
  UNUSED(other);
  bool b_ret = true;
  return b_ret;
}

int64_t ObLocalityManager::ObRefreshLocalityTask::get_deep_copy_size() const
{
  return sizeof(*this);
}

IObDedupTask *ObLocalityManager::ObRefreshLocalityTask::deep_copy(
    char *buffer,
    const int64_t buf_size) const
{
  ObRefreshLocalityTask *task = NULL;
  if (OB_UNLIKELY(OB_ISNULL(buffer))
      || OB_UNLIKELY(buf_size < get_deep_copy_size())) {
    STORAGE_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument", KP(buffer), K(buf_size));
  } else {
    task = new(buffer) ObRefreshLocalityTask(locality_mgr_);
  }
  return task;
}

int ObLocalityManager::ObRefreshLocalityTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(locality_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "locality manager is null", K(ret));
  } else if (OB_FAIL(locality_mgr_->load_region())) {
    STORAGE_LOG(WARN, "process refresh locality task fail", K(ret));
#ifdef OB_BUILD_ARBITRATION
  } else if (OB_FAIL(locality_mgr_->load_arb_service_info())) {
    STORAGE_LOG(WARN, "load_arb_service_info fail", K(ret));
#endif
  }
  return ret;
}

}// storage
}// oceanbase
