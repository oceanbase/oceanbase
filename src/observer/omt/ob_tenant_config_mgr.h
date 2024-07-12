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

#ifndef OCEANBASE_TENANT_CONFIG_MGR_H_
#define OCEANBASE_TENANT_CONFIG_MGR_H_

#include "lib/function/ob_function.h"
#include "observer/omt/ob_tenant_config.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_drw_lock.h"
#include "share/config/ob_reload_config.h"
#include "share/config/ob_config_helper.h"
#include "share/config/ob_config_manager.h"
#include "share/ob_lease_struct.h"
#include "share/rc/ob_context.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase {
namespace obrpc
{
class ObTenantConfigArg;
}
namespace omt {

class ObTenantConfigGuard {
public:
  ObTenantConfigGuard();
  ObTenantConfigGuard(ObTenantConfig *config);
  virtual ~ObTenantConfigGuard();

  ObTenantConfigGuard(const ObTenantConfigGuard &guard) = delete;
  ObTenantConfigGuard & operator=(const ObTenantConfigGuard &guard) = delete;

  void set_config(ObTenantConfig *config);
  bool is_valid() { return nullptr != config_; }
  /*
   * Requires: check is_valid().
   */
  ObTenantConfig *operator->() { return config_; }
  void trace_all_config() const;
private:
  ObTenantConfig *config_;
};

struct TenantConfigInfo {
  using ConfigString = common::ObSqlString;

  TenantConfigInfo() : TenantConfigInfo(common::OB_INVALID_TENANT_ID) {}
  TenantConfigInfo(uint64_t tenant_id) : tenant_id_(tenant_id),
    name_(), data_type_(), value_(), info_(), section_(),
    scope_(), source_(), edit_level_() {}
  virtual ~TenantConfigInfo() {}
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int set_name(const char *name) { return name_.assign(name); }
  int set_data_type(const char *data_type) { return data_type_.assign(data_type); }
  int set_value(const char *value) { return value_.assign(value); }
  int set_info(const char *info) { return info_.assign(info); }
  int set_section(const char *section) { return section_.assign(section); }
  int set_scope(const char *scope) { return scope_.assign(scope); }
  int set_source(const char *source) { return source_.assign(source); }
  int set_edit_level(const char *edit_level) { return edit_level_.assign(edit_level); }
  int assign(const TenantConfigInfo &rhs);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  uint64_t tenant_id_;
  ConfigString name_;
  ConfigString data_type_;
  ConfigString value_;
  ConfigString info_;
  ConfigString section_;
  ConfigString scope_;
  ConfigString source_;
  ConfigString edit_level_;
};

struct ObTenantID {
  ObTenantID() : tenant_id_(common::OB_INVALID_TENANT_ID) {}
  ObTenantID(uint64_t tenant_id) : tenant_id_(tenant_id) {}
  bool operator == (const ObTenantID &other) const
  {
    return tenant_id_ == other.tenant_id_;
  }
  uint64_t hash() const { return tenant_id_; }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  uint64_t tenant_id_;
};
using TenantConfigMap = common::__ObConfigContainer<ObTenantID, ObTenantConfig, common::OB_MAX_SERVER_TENANT_CNT>;
using TenantConfigVersionMap = common::hash::ObHashMap<ObTenantID, int64_t>;

using SuccessFunctor = std::function<void(const ObTenantConfig &config)>;
using FailureFunctor = std::function<void()>;
using UpdateTenantConfigCb = common::ObFunction<void(uint64_t tenant_id)>;

class ObTenantConfigMgr
{
public:
  static ObTenantConfigMgr &get_instance();
  virtual ~ObTenantConfigMgr();
  ObTenantConfigMgr(const ObTenantConfigMgr &config) = delete;
  ObTenantConfigMgr & operator=(const ObTenantConfigMgr &) = delete;

  int init(common::ObMySQLProxy &sql_proxy,
           const common::ObAddr &server,
           common::ObConfigManager *config_mgr,
           const UpdateTenantConfigCb &update_tenant_config_cb);
  int refresh_tenants(const common::ObIArray<uint64_t> &tenants);
  int add_tenant_config(uint64_t tenant_id);
  int del_tenant_config(uint64_t tenant_id);
  int init_tenant_config(const obrpc::ObTenantConfigArg &arg);

  ObTenantConfig *get_tenant_config(uint64_t tenant_id) const;
  // lock to guarantee this will not be deleted by calling del_tenant_config.
  // Fallback to get tenant config of %fallback_tenant_id if %tenant_id config not exist.
  // NOTICE: remember to unlock
  ObTenantConfig *get_tenant_config_with_lock(
      const uint64_t tenant_id,
      const uint64_t fallback_tenant_id = 0,
      const uint64_t timeout_us = 0) const;
  int read_tenant_config(
      const uint64_t tenant_id,
      const uint64_t fallback_tenant_id,
      const SuccessFunctor &on_success,
      const FailureFunctor &on_failure) const;
  int64_t get_tenant_newest_version(uint64_t tenant_id) const;
  int64_t get_tenant_current_version(uint64_t tenant_id) const;
  void print() const;
  int dump2file();

  void refresh_config_version_map(const common::ObIArray<uint64_t> &tenants);
  void reset_version_has_refreshed() { version_has_refreshed_ = false; }
  int set_tenant_config_version(uint64_t tenant_id, int64_t version);
  int64_t get_tenant_config_version(uint64_t tenant_id);
  void get_lease_request(share::ObLeaseRequest &lease_request);
  int get_lease_response(share::ObLeaseResponse &lease_response);
  int get_all_tenant_config_info(common::ObArray<TenantConfigInfo> &config_info,
                                 common::ObIAllocator *allocator);
  int got_versions(const common::ObIArray<std::pair<uint64_t, int64_t> > &versions);
  int got_version(uint64_t tenant_id, int64_t version, const bool remove_repeat = true);
  int update_local(uint64_t tenant_id, int64_t expected_version);
  void notify_tenant_config_changed(uint64_t tenatn_id);
  int add_config_to_existing_tenant(const char *config_str);
  int add_extra_config(const obrpc::ObTenantConfigArg &arg);
  int schedule(ObTenantConfig::TenantConfigUpdateTask &task, const int64_t delay);
  int cancel(const ObTenantConfig::TenantConfigUpdateTask &task);
  int wait(const ObTenantConfig::TenantConfigUpdateTask &task);
  bool inited() { return inited_; }

  static uint64_t default_fallback_tenant_id()
  {
    const auto tenant = CURRENT_ENTITY(TENANT_SPACE)->get_tenant();;
    uint64_t id = 0;
    if (NULL == tenant || 0 == tenant->id() || common::OB_INVALID_ID == tenant->id()) {
      // for background thread, fallback to sys tenant
      id = common::OB_SYS_TENANT_ID;
    } else {
      id = tenant->id();
    }
    return id;
  }
  // protect config_map_
  mutable common::DRWLock rwlock_;
  OB_UNIS_VERSION(1);

private:
  static const int64_t RECYCLE_LATENCY = 30L * 60L * 1000L * 1000L;
  ObTenantConfigMgr();
  bool inited_;
  common::ObAddr self_;
  common::ObMySQLProxy *sql_proxy_;
  // 租户配置项的映射
  TenantConfigMap config_map_;
  TenantConfigVersionMap config_version_map_;
  common::ObConfigManager *sys_config_mgr_;
  bool version_has_refreshed_;
  UpdateTenantConfigCb update_tenant_config_cb_;
  // reload cb

};

} // omt
} // oceanbase

#define OTC_MGR (::oceanbase::omt::ObTenantConfigMgr::get_instance())
/*
 * use ObTenantConfigGuard to unlock automatically, otherwise remember to unlock:
 *   ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
 */
#define TENANT_CONF_TIL(tenant_id, timeout_us) (OTC_MGR.get_tenant_config_with_lock( \
        tenant_id, oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(), timeout_us))

#define TENANT_CONF(tenant_id) (OTC_MGR.get_tenant_config_with_lock( \
        tenant_id, oceanbase::omt::ObTenantConfigMgr::default_fallback_tenant_id(), 0))
#endif
