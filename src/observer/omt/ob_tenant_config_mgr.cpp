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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_tenant_config_mgr.h"
#include "lib/thread/thread_mgr.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_common_config.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace omt {

ObTenantConfigGuard::ObTenantConfigGuard() : ObTenantConfigGuard(nullptr)
{}

ObTenantConfigGuard::ObTenantConfigGuard(ObTenantConfig* config)
{
  config_ = config;
}

ObTenantConfigGuard::~ObTenantConfigGuard()
{
  if (OB_NOT_NULL(config_)) {
    config_->unlock();
  }
}

void ObTenantConfigGuard::set_config(ObTenantConfig* config)
{
  if (OB_NOT_NULL(config_)) {
    config_->unlock();
  }
  config_ = config;
}

int TenantConfigInfo::assign(const TenantConfigInfo& rhs)
{
  int ret = OB_SUCCESS;
  tenant_id_ = rhs.tenant_id_;
  if (OB_FAIL(name_.assign(rhs.name_))) {
    LOG_WARN("assign name fail", K_(name), K(ret));
  } else if (OB_FAIL(value_.assign(rhs.value_))) {
    LOG_WARN("assign value fail", K_(value), K(ret));
  } else if (OB_FAIL(info_.assign(rhs.info_))) {
    LOG_WARN("assign info fail", K_(info), K(ret));
  } else if (OB_FAIL(section_.assign(rhs.section_))) {
    LOG_WARN("assign section fail", K_(section), K(ret));
  } else if (OB_FAIL(scope_.assign(rhs.scope_))) {
    LOG_WARN("assign scope fail", K_(scope), K(ret));
  } else if (OB_FAIL(source_.assign(rhs.source_))) {
    LOG_WARN("assign source fail", K_(source), K(ret));
  } else if (OB_FAIL(edit_level_.assign(rhs.edit_level_))) {
    LOG_WARN("assign edit_level fail", K_(edit_level), K(ret));
  }
  return ret;
}

int64_t TenantConfigInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buf) && buf_len > 0) {
    if (OB_FAIL(databuff_printf(buf,
            buf_len,
            pos,
            "tenant_id: [%lu], name: [%s],"
            "value: [%s], info: [%s]",
            tenant_id_,
            name_.ptr(),
            value_.ptr(),
            info_.ptr()))) {
      pos = 0;
      LOG_WARN("to_string buff fail", K(ret));
    }
  }
  return pos;
}

ObTenantConfigMgr::ObTenantConfigMgr()
    : inited_(false),
      self_(),
      sql_proxy_(nullptr),
      rwlock_(),
      config_map_(),
      config_version_map_(),
      sys_config_mgr_(nullptr)
{}

ObTenantConfigMgr::~ObTenantConfigMgr()
{}

ObTenantConfigMgr& ObTenantConfigMgr::get_instance()
{
  static ObTenantConfigMgr ob_tenant_config_mgr;
  return ob_tenant_config_mgr;
}

int ObTenantConfigMgr::init(ObMySQLProxy& sql_proxy, const ObAddr& server, ObConfigManager* config_mgr)
{
  int ret = OB_SUCCESS;
  sql_proxy_ = &sql_proxy;
  self_ = server;
  sys_config_mgr_ = config_mgr;
  ret = config_version_map_.create(oceanbase::common::OB_MAX_SERVER_TENANT_CNT,
      oceanbase::common::ObModIds::OB_HASH_BUCKET_CONF_CONTAINER,
      oceanbase::common::ObModIds::OB_HASH_NODE_CONF_CONTAINER);
  inited_ = true;
  return ret;
}

int ObTenantConfigMgr::refresh_tenants(const ObIArray<uint64_t>& tenants)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> new_tenants;
  ObSEArray<uint64_t, 1> del_tenants;

  if (tenants.count() > 0) {
    // Probe the config to be added
    {
      ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
      for (int i = 0; i < tenants.count(); ++i) {
        uint64_t tenant_id = tenants.at(i);
        ObTenantConfig* const* config = nullptr;
        if (NULL == (config = config_map_.get(ObTenantID(tenant_id)))) {
          if (OB_FAIL(new_tenants.push_back(tenant_id))) {
            LOG_WARN("fail add tenant config", K(tenant_id), K(ret));
          }
        }
      }
    }
    // Probe the config to be added
    {
      ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
      TenantConfigMap::const_iterator it = config_map_.begin();
      for (; it != config_map_.end(); ++it) {
        uint64_t tenant_id = it->first.tenant_id_;
        if (OB_SYS_TENANT_ID == tenant_id) {
          continue;
        }
        bool need_del = true;
        for (int i = 0; i < tenants.count(); ++i) {
          if (tenant_id == tenants.at(i)) {
            need_del = false;
            break;
          }
        }
        if (need_del) {
          if (OB_FAIL(del_tenants.push_back(tenant_id))) {
            LOG_WARN("fail add tenant config", K(tenant_id), K(ret));
          }
        }
      }
    }
  }

  // add config
  for (int i = 0; i < new_tenants.count(); ++i) {
    if (OB_FAIL(add_tenant_config(new_tenants.at(i)))) {
      LOG_WARN("fail add tenant config", K(i), K(new_tenants.at(i)), K(ret));
    } else {
      LOG_INFO("add created tenant config succ", K(i), K(new_tenants.at(i)));
    }
  }
  // delete config
  for (int i = 0; i < del_tenants.count(); ++i) {
    if (OB_FAIL(del_tenant_config(del_tenants.at(i)))) {
      LOG_WARN("fail add tenant config", K(i), K(del_tenants.at(i)), K(ret));
    } else {
      LOG_INFO("del dropped tenant config succ.", K(i), K(del_tenants.at(i)));
    }
  }

  return ret;
}

int ObTenantConfigMgr::add_tenant_config(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantConfig* const* config = nullptr;
  ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);
  if (is_virtual_tenant_id(tenant_id) || OB_NOT_NULL(config = config_map_.get(ObTenantID(tenant_id)))) {
    if (nullptr != config) {
      ObTenantConfig* new_config = *config;
      new_config->set_deleting(false);
    }
  } else {
    ObTenantConfig* new_config = nullptr;
    new_config = OB_NEW(ObTenantConfig, ObModIds::OMT, tenant_id);
    if (OB_NOT_NULL(new_config)) {
      if (OB_FAIL(new_config->init(this))) {
        LOG_WARN("new tenant config init failed", K(ret));
      } else if (OB_FAIL(config_map_.set_refactored(ObTenantID(tenant_id), new_config, 1))) {
        LOG_WARN("add new tenant config failed", K(ret));
      }
      if (OB_FAIL(ret)) {
        ob_delete(new_config);
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc new tenant config failed", K(ret));
    }
  }
  LOG_INFO("tenant config added", K(tenant_id), K(ret));
  return ret;
}

int ObTenantConfigMgr::del_tenant_config(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t DELAY_DEL_US = 10 * 60 * 1000 * 1000;  // 10min
  ObTenantConfig* config = nullptr;
  ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);
  if (is_virtual_tenant_id(tenant_id)) {
  } else if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
    LOG_WARN("get tenant config failed", K(tenant_id), K(ret));
  } else if (config->get_current_version() + DELAY_DEL_US < ObTimeUtility::current_time()) {
  } else {
    static const int DEL_TRY_TIMES = 30;
    static const int64_t TIME_SLICE_PERIOD = 10000;
    for (int i = 0; i < DEL_TRY_TIMES; ++i) {
      if (OB_SUCC(config->try_wrlock())) {
        break;
      }
      usleep(TIME_SLICE_PERIOD);
    }  // for
    if (OB_SUCC(ret)) {
      config->set_deleting();
      if (OB_FAIL(cancel(config->get_update_task()))) {
        LOG_WARN("cancel tenant config update task failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(config_map_.erase_refactored(ObTenantID(tenant_id)))) {
        LOG_WARN("delete tenant config failed", K(ret), K(tenant_id));
      } else {
        ob_delete(config);
      }
      if (OB_FAIL(ret)) {
        config->unlock();
      }
    }
  }
  LOG_INFO("tenant config deleted", K(tenant_id), K(ret));
  return ret;
}

ObTenantConfig* ObTenantConfigMgr::get_tenant_config(uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  ObTenantConfig* config = nullptr;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
    LOG_ERROR("failed to get tenant config", K(tenant_id), K(ret));
  }
  return config;
}

ObTenantConfig* ObTenantConfigMgr::get_tenant_config_with_lock(
    const uint64_t tenant_id, const uint64_t fallback_tenant_id /* = 0 */) const
{
  int ret = OB_SUCCESS;
  ObTenantConfig* config = nullptr;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
    if (fallback_tenant_id > 0 && OB_INVALID_ID != fallback_tenant_id) {
      if (OB_FAIL(config_map_.get_refactored(ObTenantID(fallback_tenant_id), config))) {
        LOG_WARN("failed to get tenant config", K(fallback_tenant_id), K(ret));
      }
    } else {
      LOG_WARN("failed to get tenant config", K(tenant_id), K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(config) && OB_FAIL(config->rdlock())) {  // remember to unlock outside
    config = nullptr;
    LOG_ERROR("lock tenant config failed", K(tenant_id), K(ret));
  }
  return config;
}

int ObTenantConfigMgr::read_tenant_config(const uint64_t tenant_id, const uint64_t fallback_tenant_id,
    const SuccessFunctor& on_success, const FailureFunctor& on_failure) const
{
  int ret = OB_SUCCESS;
  ObTenantConfig* config = nullptr;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
    if (fallback_tenant_id > 0 && OB_INVALID_ID != fallback_tenant_id) {
      if (OB_FAIL(config_map_.get_refactored(ObTenantID(fallback_tenant_id), config))) {
        LOG_WARN("failed to get tenant config", K(fallback_tenant_id), K(ret), K(lbt()));
      }
    } else {
      LOG_WARN("failed to get tenant config", K(tenant_id), K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(config)) {
    on_success(*config);
  } else {
    on_failure();
    LOG_WARN("fail read tenant config", K(tenant_id), K(ret));
  }
  return ret;
}

void ObTenantConfigMgr::print() const
{
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  TenantConfigMap::const_iterator it = config_map_.begin();
  for (; it != config_map_.end(); ++it) {
    if (OB_NOT_NULL(it->second)) {
      it->second->print();
    }
  }  // for
}

int ObTenantConfigMgr::dump2file(const char* path) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(sys_config_mgr_->dump2file(path))) {
    ret = sys_config_mgr_->config_backup();
  }
  return ret;
}

int ObTenantConfigMgr::set_tenant_config_version(uint64_t tenant_id, int64_t version)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard wr_guard(rwlock_, ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(config_version_map_.set_refactored(ObTenantID(tenant_id), version, 1))) {
    LOG_WARN("set tenant config version fail", K(tenant_id), K(version), K(ret));
  }
  return ret;
}

int64_t ObTenantConfigMgr::get_tenant_config_version(uint64_t tenant_id)
{
  int64_t version = ObSystemConfig::INIT_VERSION;
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(config_version_map_.get_refactored(ObTenantID(tenant_id), version))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get tenant config version fail", K(tenant_id), K(ret));
    }
  }
  return version;
}

void ObTenantConfigMgr::get_lease_response(share::ObLeaseResponse& lease_response)
{
  int ret = OB_SUCCESS;
  std::pair<uint64_t, int64_t> pair;
  lease_response.tenant_config_version_.reset();
  lease_response.tenant_config_version_.reserve(config_version_map_.size());
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  TenantConfigVersionMap::const_iterator it = config_version_map_.begin();
  for (; it != config_version_map_.end() && OB_SUCC(ret); ++it) {
    pair.first = it->first.tenant_id_;
    pair.second = it->second;
    if (OB_FAIL(lease_response.tenant_config_version_.push_back(pair))) {
      LOG_WARN("push back tenant config fail", "tenant_id", pair.first, "version", pair.second, K(ret));
    } else {
      LOG_DEBUG("push back tenant config response succ", "tenant", pair.first, "version", pair.second);
    }
  }
  if (OB_FAIL(ret)) {
    lease_response.tenant_config_version_.reset();
  }
}

void ObTenantConfigMgr::get_lease_request(share::ObLeaseRequest& lease_request)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  std::pair<uint64_t, int64_t> pair;
  TenantConfigMap::const_iterator it = config_map_.begin();
  for (; it != config_map_.end(); ++it) {
    if (OB_NOT_NULL(it->second)) {
      pair.first = it->first.tenant_id_;
      pair.second = it->second->get_current_version();
      if (OB_FAIL(lease_request.tenant_config_version_.push_back(pair))) {
        LOG_WARN("push back tenant config fail", K(ret));
      }
    }
  }  // for
}

int ObTenantConfigMgr::get_all_tenant_config_info(common::ObArray<TenantConfigInfo>& all_config)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  TenantConfigMap::const_iterator it = config_map_.begin();
  for (; OB_SUCC(ret) && it != config_map_.end(); ++it) {
    uint64_t tenant_id = it->first.tenant_id_;
    ObTenantConfig* tenant_config = it->second;
    for (ObConfigContainer::const_iterator iter = tenant_config->get_container().begin();
         iter != tenant_config->get_container().end();
         iter++) {
      TenantConfigInfo config_info(tenant_id);
      if (OB_FAIL(config_info.set_name(iter->first.str()))) {
        LOG_WARN("set name fail", K(iter->first.str()), K(ret));
      } else if (OB_FAIL(config_info.set_value(iter->second->str()))) {
        LOG_WARN("set value fail", K(iter->second->str()), K(ret));
      } else if (OB_FAIL(config_info.set_info(iter->second->info()))) {
        LOG_WARN("set info fail", K(iter->second->info()), K(ret));
      } else if (OB_FAIL(config_info.set_section(iter->second->section()))) {
        LOG_WARN("set section fail", K(iter->second->section()), K(ret));
      } else if (OB_FAIL(config_info.set_scope(iter->second->scope()))) {
        LOG_WARN("set scope fail", K(iter->second->scope()), K(ret));
      } else if (OB_FAIL(config_info.set_source(iter->second->source()))) {
        LOG_WARN("set source fail", K(iter->second->source()), K(ret));
      } else if (OB_FAIL(config_info.set_edit_level(iter->second->edit_level()))) {
        LOG_WARN("set edit_level fail", K(iter->second->edit_level()), K(ret));
      } else if (OB_FAIL(all_config.push_back(config_info))) {
        LOG_WARN("push to array fail", K(config_info), K(ret));
      }
    }  // for
  }    // for
  return ret;
}

int ObTenantConfigMgr::got_versions(common::ObIArray<std::pair<uint64_t, int64_t>>& versions)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  for (int i = 0; i < versions.count(); ++i) {
    uint64_t tenant_id = versions.at(i).first;
    int64_t version = versions.at(i).second;
    if (OB_FAIL(got_version(tenant_id, version))) {
      LOG_WARN("fail got version", K(tenant_id), K(version), K(ret));
    }
  }
  return ret;
}

int ObTenantConfigMgr::got_version(uint64_t tenant_id, int64_t version, const bool remove_repeat /* = true */)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  ObTenantConfig* config = nullptr;
  if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
    LOG_WARN("No tenant config found", K(tenant_id), K(ret));
  } else {
    ret = config->got_version(version, remove_repeat);
  }
  return ret;
}

int ObTenantConfigMgr::update_local(uint64_t tenant_id, int64_t expected_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObTenantConfig* config = nullptr;
    bool did_use_weak = false;
    ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, did_use_weak);
    SMART_VAR(ObMySQLProxy::MySQLResult, result)
    {
      const char* sqlstr = "select config_version, zone, svr_type, svr_ip, svr_port, name, "
                           "data_type, value, info, section, scope, source, edit_level "
                           "from __tenant_parameter";
      if (OB_FAIL(sql_client_retry_weak.read(result, tenant_id, sqlstr))) {
        LOG_WARN("read config from __tenant_parameter failed", K(sqlstr), K(ret));
      } else {
        ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
        if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
          LOG_ERROR("failed to get tenant config", K(tenant_id), K(ret));
        } else {
          ret = config->update_local(expected_version, result);
        }
      }
    }
  }
  return ret;
}

int ObTenantConfigMgr::schedule(
    ObTenantConfig::TenantConfigUpdateTask& task, const int64_t delay, const bool repeat /* = false */)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(TG_SCHEDULE(lib::TGDefIDs::CONFIG_MGR, task, delay, repeat))) {
    task.is_running_ = true;
  }
  return ret;
}

int ObTenantConfigMgr::cancel(const ObTenantConfig::TenantConfigUpdateTask& task)
{
  int ret = OB_SUCCESS;
  const int try_times = 300;
  const int64_t period = 10000;
  if (OB_FAIL(TG_CANCEL_R(lib::TGDefIDs::CONFIG_MGR, task))) {
    LOG_WARN("cancel tenant config update task failed", K(ret));
  } else {
    for (int i = 0; i < try_times; ++i) {
      if (task.task_lock_.tryLock()) {
        if (!task.is_running_) {
          task.task_lock_.unlock();
          break;
        }
        task.task_lock_.unlock();
      }
      ret = OB_EAGAIN;
      usleep(period);
    }  // for
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTenantConfigMgr)
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  int64_t expect_data_len = get_serialize_size_();
  int64_t saved_pos = pos;
  TenantConfigMap::const_iterator it = config_map_.begin();
  for (; OB_SUCC(ret) && it != config_map_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = it->second->serialize(buf, buf_len, pos);
    }
  }  // for
  if (OB_SUCC(ret)) {
    int64_t writen_len = pos - saved_pos;
    if (writen_len != expect_data_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data size", K(writen_len), K(expect_data_len));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTenantConfigMgr)
{
  int ret = OB_SUCCESS;
  if (data_len == 0 || pos >= data_len) {
  } else {
    while (OB_SUCC(ret) && pos < data_len) {
      int64_t saved_pos = pos;
      int64_t ignore_version = 0, ignore_len = 0;
      OB_UNIS_DECODE(ignore_version);
      OB_UNIS_DECODE(ignore_len);
      if (OB_FAIL(ret)) {
      } else if ('[' != *(buf + pos)) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("invalid tenant config", K(ret));
      } else {
        int64_t cur = pos + 1;
        while (cur < data_len - 1 && ']' != *(buf + cur)) {
          ++cur;
        }  // while
        if (cur >= data_len - 1 || '\n' != *(buf + cur + 1)) {
          ret = OB_INVALID_DATA;
          LOG_ERROR("invalid tenant config", K(ret));
        } else {
          uint64_t tenant_id = OB_INVALID_TENANT_ID;
          char tenant_str[100];
          char* p_end = nullptr;
          MEMSET(tenant_str, '\0', 100);
          if (cur - pos - 1 < 100) {
            MEMCPY(tenant_str, buf + pos + 1, cur - pos - 1);
            tenant_id = strtoul(tenant_str, &p_end, 0);
          } else {
            ret = OB_INVALID_DATA;
            LOG_ERROR("invalid tenant id", K(ret));
            break;
          }
          if ('\0' != *p_end) {
            ret = OB_INVALID_CONFIG;
            LOG_ERROR("invalid tenant id", K(ret));
          } else {
            pos = cur + 2;
            ObTenantConfig* config = nullptr;
            if (OB_FAIL(config_map_.get_refactored(ObTenantID(tenant_id), config))) {
              if (ret != OB_HASH_NOT_EXIST || OB_FAIL(add_tenant_config(tenant_id))) {
                LOG_ERROR("get tenant config failed", K(tenant_id), K(ret));
                break;
              }
              ret = config_map_.get_refactored(ObTenantID(tenant_id), config);
            }
            if (OB_SUCC(ret)) {
              pos = saved_pos;
              ret = config->deserialize(buf, data_len, pos);
            }
          }
        }
      }  // else
    }    // while
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTenantConfigMgr)
{
  int64_t len = 0;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::CONFIG_LOCK);
  TenantConfigMap::const_iterator it = config_map_.begin();
  for (; it != config_map_.end(); ++it) {
    if (OB_NOT_NULL(it->second)) {
      len += it->second->get_serialize_size();
    }
  }  // for
  return len;
}

}  // namespace omt
}  // namespace oceanbase
