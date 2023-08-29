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

#ifndef OCEANBASE_TENANT_CONFIG_H_
#define OCEANBASE_TENANT_CONFIG_H_

#include "share/config/ob_config.h"
#include "share/config/ob_system_config.h"
#include "share/config/ob_common_config.h"
#include "share/config/ob_config_helper.h"
#include "lib/lock/ob_drw_lock.h"

namespace oceanbase {

namespace omt {

using common::ObCommonConfig;
using common::Section;
using common::Scope;
using common::Source;
using common::EditLevel;
using common::ObParameterAttr;
class ObTenantConfigMgr;

class ObTenantConfig : public ObCommonConfig
{
public:
  static const int64_t INITIAL_TENANT_CONF_VERSION = 1;
public:
  class TenantConfigUpdateTask : public common::ObTimerTask
  {
  public:
    TenantConfigUpdateTask() : config_mgr_(nullptr), tenant_config_(nullptr),
                               version_(0), scheduled_time_(0),
                               update_local_(false),
                               running_task_count_(0) {}
    int init(ObTenantConfigMgr *config_mgr, ObTenantConfig *config)
    {
      config_mgr_ = config_mgr;
      tenant_config_ = config;
      return common::OB_SUCCESS;
    }
    virtual ~TenantConfigUpdateTask() {}
    TenantConfigUpdateTask(const TenantConfigUpdateTask &) = delete;
    TenantConfigUpdateTask &operator=(const TenantConfigUpdateTask &) = delete;
    void set_tenant_config(ObTenantConfig *config) { tenant_config_ = config; }
    void runTimerTask(void) override;
    ObTenantConfigMgr *config_mgr_;
    ObTenantConfig *tenant_config_;
    volatile int64_t version_;
    volatile int64_t scheduled_time_;
    bool update_local_;
    volatile int64_t running_task_count_;
  };
  friend class TenantConfigUpdateTask;
  static const int64_t LOCK_TIMEOUT = 1 * 1000 * 1000;
public:
  ObTenantConfig();
  ObTenantConfig(uint64_t tenant_id);
  int init(ObTenantConfigMgr *config_mgr);
  virtual ~ObTenantConfig() {};
  void set_tenant_config_mgr(ObTenantConfigMgr *config_mgr) { config_mgr_ = config_mgr; };
  void set_deleting(bool deleting = true) { is_deleting_ = deleting; }
  ObTenantConfig(const ObTenantConfig &)=delete;
  ObTenantConfig &operator=(const ObTenantConfig &)=delete;

  void print() const override;
  int check_all() const override { return OB_SUCCESS; }
  common::ObServerRole get_server_type() const override { return common::OB_SERVER; }
  void ref() { ATOMIC_INC(&ref_); }
  void unref() { ATOMIC_DEC(&ref_); }
  bool is_ref_clear() { return 0 == ATOMIC_LOAD(&ref_); }

  int read_config();
  int publish_special_config_after_dump();
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_current_version() const { return current_version_; }
  const TenantConfigUpdateTask &get_update_task() const { return  update_task_; }
  int64_t get_create_timestamp() const { return create_timestamp_; }
  int got_version(int64_t version, const bool remove_repeat);
  int update_local(int64_t expected_version, common::ObMySQLProxy::MySQLResult &result,
                   bool save2file = true);
  int add_extra_config(const char *config_str,
                       int64_t version = 0 ,
                       bool check_config = true);

  OB_UNIS_VERSION(1);
private:
#ifdef ERRSIM
  int build_errsim_module_();
#endif
private:
  uint64_t tenant_id_;
  int64_t current_version_; // 当前 tenant config 正在被 task 更新中的版本
  obutil::Mutex mutex_;
  TenantConfigUpdateTask update_task_;
  common::ObSystemConfig system_config_;
  ObTenantConfigMgr *config_mgr_;
  int64_t ref_;
  bool is_deleting_;
  int64_t create_timestamp_;

public:
///////////////////////////////////////////////////////////////////////////////
// use MACRO 'OB_TENANT_PARAMETER' to define new tenant parameters
// in ob_parameter_seed.ipp
///////////////////////////////////////////////////////////////////////////////
#undef OB_TENANT_PARAMETER
#define OB_TENANT_PARAMETER(args...) args
#include "share/parameter/ob_parameter_seed.ipp"
#undef OB_TENANT_PARAMETER

};

} // omt
} // oceanbase

#endif
