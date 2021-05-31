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

#include "ob_tenant_config.h"
#include "common/ob_common_utility.h"
#include "lib/net/tbnetutil.h"
#include "lib/oblog/ob_log.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace omt {

ObTenantConfig::ObTenantConfig() : ObTenantConfig(OB_INVALID_TENANT_ID)
{}

ObTenantConfig::ObTenantConfig(uint64_t tenant_id)
    : tenant_id_(tenant_id),
      current_version_(1),
      newest_version_(1),
      running_task_count_(0),
      mutex_(),
      update_task_(),
      system_config_(),
      config_mgr_(nullptr),
      lock_(),
      is_deleting_(false)
{}

int ObTenantConfig::init(ObTenantConfigMgr* config_mgr)
{
  int ret = OB_SUCCESS;
  config_mgr_ = config_mgr;
  if (OB_FAIL(system_config_.init())) {
    LOG_ERROR("init system config failed", K(ret));
  } else if (OB_FAIL(update_task_.init(config_mgr, this))) {
    LOG_ERROR("init tenant config updata task failed", K_(tenant_id), K(ret));
  }
  return ret;
}

void ObTenantConfig::print() const
{
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
  OB_LOG(INFO, "===================== * begin tenant config report * =====================", K(tenant_id_));
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      OB_LOG(WARN, "config item is null", "name", it->first.str());
    } else {
      _OB_LOG(INFO, "| %-36s = %s", it->first.str(), it->second->str());
    }
  }
  OB_LOG(INFO, "===================== * stop tenant config report * =======================", K(tenant_id_));
}

int ObTenantConfig::check_all() const
{
  int ret = OB_SUCCESS;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
  ObConfigContainer::const_iterator it = container_.begin();
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "config item is null", "name", it->first.str(), K(ret));
    } else if (!it->second->check()) {
      ret = OB_INVALID_CONFIG;
      OB_LOG(WARN, "Configure setting invalid", "name", it->first.str(), "value", it->second->str(), K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTenantConfig::rdlock()
{
  return lock_.rdlock(ObLatchIds::CONFIG_LOCK) == OB_SUCCESS ? OB_SUCCESS : OB_EAGAIN;
}

int ObTenantConfig::wrlock()
{
  return lock_.wrlock(ObLatchIds::CONFIG_LOCK) == OB_SUCCESS ? OB_SUCCESS : OB_EAGAIN;
}

int ObTenantConfig::try_rdlock()
{
  return lock_.try_rdlock(ObLatchIds::CONFIG_LOCK) == OB_SUCCESS ? OB_SUCCESS : OB_EAGAIN;
}

int ObTenantConfig::try_wrlock()
{
  return lock_.try_wrlock(ObLatchIds::CONFIG_LOCK) == OB_SUCCESS ? OB_SUCCESS : OB_EAGAIN;
}

int ObTenantConfig::unlock()
{
  return lock_.unlock() == OB_SUCCESS ? OB_SUCCESS : OB_EAGAIN;
}

int ObTenantConfig::read_config()
{
  int ret = OB_SUCCESS;
  ObSystemConfigKey key;
  ObAddr server;
  char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  ObLatchWGuard wr_guard(lock_, ObLatchIds::CONFIG_LOCK);
  if (!server.set_ipv4_addr(ntohl(obsys::CNetUtil::getLocalAddr(GCONF.devname)), 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(true != server.ip_to_string(local_ip, sizeof(local_ip)))) {
    ret = OB_CONVERT_ERROR;
  } else {
    key.set_varchar(ObString::make_string("svr_type"), print_server_role(get_server_type()));
    key.set_int(ObString::make_string("svr_port"), GCONF.rpc_port);
    key.set_varchar(ObString::make_string("svr_ip"), local_ip);
    key.set_varchar(ObString::make_string("zone"), GCONF.zone);
    ObConfigContainer::const_iterator it = container_.begin();
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      key.set_name(it->first.str());
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(ERROR, "config item is null", "name", it->first.str(), K(ret));
      } else {
        key.set_version(it->second->version());
        int temp_ret = system_config_.read_config(key, *(it->second));
        if (OB_SUCCESS != temp_ret) {
          OB_LOG(DEBUG, "Read config error", "name", it->first.str(), K(temp_ret));
        }
      }
    }  // for
  }
  return ret;
}

void ObTenantConfig::TenantConfigUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(config_mgr), K(ret));
  } else if (OB_ISNULL(tenant_config_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(tenant_config), K(ret));
  } else {
    const int64_t saved_current_version = tenant_config_->current_version_;
    const int64_t version = version_;
    THIS_WORKER.set_timeout_ts(INT64_MAX);
    if (tenant_config_->current_version_ == version) {
      ret = OB_ALREADY_DONE;
    } else if (update_local_) {
      task_lock_.lock();
      tenant_config_->current_version_ = version;
      if (OB_FAIL(tenant_config_->system_config_.clear())) {
        LOG_WARN("Clear system config map failed", K(ret));
      } else {
        ret = config_mgr_->update_local(tenant_config_->tenant_id_, version);
      }
      is_running_ = false;
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        uint64_t tenant_id = tenant_config_->tenant_id_;
        tenant_config_->current_version_ = saved_current_version;
        share::schema::ObSchemaGetterGuard schema_guard;
        share::schema::ObMultiVersionSchemaService* schema_service = GCTX.schema_service_;
        bool tenant_dropped = false;
        if (OB_ISNULL(schema_service)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_service is null", K(ret), K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
          LOG_WARN("fail to get schema guard", K(ret), K(tmp_ret), K(tenant_id));
        } else if (OB_SUCCESS != (tmp_ret = schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_dropped))) {
          LOG_WARN("fail to check if tenant has been dropped", K(ret), K(tmp_ret), K(tenant_id));
        } else {
          tenant_dropped = tenant_dropped || tenant_config_->is_deleting_;
          if (tenant_dropped) {
            LOG_INFO("tenant under deleting", K(tenant_id));
          } else if (OB_FAIL(config_mgr_->schedule(*this, 1000 * 1000L, false))) {
            LOG_WARN("Reschedule update tenant local config failed", K(ret));
          }
        }
      } else {
        const int64_t read_version = tenant_config_->system_config_.get_version();
        LOG_INFO("loaded new tenant config",
            "tenant_id",
            tenant_config_->tenant_id_,
            "read_version",
            read_version,
            "old_version",
            saved_current_version,
            "current_version",
            tenant_config_->current_version_,
            "newest_version",
            tenant_config_->newest_version_,
            "expected_version",
            version);
        tenant_config_->print();
      }
      task_lock_.unlock();
    }
  }

  task_lock_.lock();
  tenant_config_->running_task_count_--;
  task_lock_.unlock();
}

// scenario:
// s1: hundreds of alter system commands rush in, each trigger a got_version
//     which will use up all 32 timer slots
// s2: heartbeat with same version, reponse only once
int ObTenantConfig::got_version(int64_t version, const bool remove_repeat)
{
  UNUSED(remove_repeat);
  int ret = OB_SUCCESS;
  bool schedule_task = false;
  if (version < 0) {
    update_task_.update_local_ = false;
    schedule_task = true;
  } else if (0 == version) {
    LOG_DEBUG("root server restarting");
  } else if (current_version_ == version) {
  } else if (version < current_version_) {
    LOG_WARN("Local tenant config is newer than rs, weird", K_(current_version), K(version));
  } else if (version > current_version_) {
    if (!mutex_.tryLock()) {
      LOG_DEBUG("Processed by other thread!");
    } else {
      if (version > newest_version_) {
        LOG_INFO("Got new tenant config version", K_(tenant_id), K_(current_version), K_(newest_version), K(version));
        newest_version_ = version;
        update_task_.update_local_ = true;
        update_task_.version_ = version;
        update_task_.scheduled_time_ = obsys::CTimeUtil::getMonotonicTime();
        schedule_task = true;
      } else if (version < newest_version_) {
        LOG_WARN("Receive weird tenant config version", K_(current_version), K_(newest_version), K(version));
      }
      mutex_.unlock();
    }
  }
  if (schedule_task) {
    bool schedule = true;
    if (!config_mgr_->inited()) {
      newest_version_ = current_version_;
      schedule = false;
      ret = OB_NOT_INIT;
      LOG_WARN("Couldn't update config because timer is NULL", K(ret));
    }
    if (schedule && !is_deleting_) {
      update_task_.task_lock_.lock();
      // avoid use up timer slots (32)
      //
      // t----> time direction
      // '~' represents timer wait sched in queue
      // '-' represents timer scheduling task
      //
      // case1: task3 in queue after task1 finish
      // |~~~~|--- task1 ----|
      //            |~~~~~~~~|--- task2 ----|
      //                      |~~~~~~~~~~~~~|--- task3 ----|
      //
      // case2: task3 in queue before task1 finish
      // |~~~~|--- task1 ----|
      //            |~~~~~~~~|--- task2 ----|
      //                    |~~~~~~~~~~~~~~~|--- task3 ----|
      //
      //
      if (running_task_count_ < 2) {
        running_task_count_++;
        if (OB_FAIL(config_mgr_->schedule(update_task_, 0, false))) {
          LOG_ERROR("Update tenant local config failed", K_(tenant_id), K(ret));
        } else {
          LOG_INFO("Schedule update tenant config task successfully!");
        }
      }
      update_task_.task_lock_.unlock();
    }
  }
  return ret;
}

int ObTenantConfig::update_local(
    int64_t expected_version, ObMySQLProxy::MySQLResult& result, bool save2file /* = true */)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(system_config_.update(result))) {
    LOG_WARN("failed to load system config", K(ret));
  } else if (expected_version != ObSystemConfig::INIT_VERSION &&
             (system_config_.get_version() < current_version_ || system_config_.get_version() < expected_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("__tenant_parameter is older than the expected version",
        K(ret),
        "read_version",
        system_config_.get_version(),
        "current_version",
        current_version_,
        "newest_version",
        newest_version_,
        "expected_version",
        expected_version);
  } else {
    LOG_INFO("read tenant config from __tenant_parameter succeed", K_(tenant_id));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(read_config())) {
      LOG_ERROR("Read tenant config failed", K_(tenant_id), K(ret));
    } else if (save2file && OB_FAIL(config_mgr_->dump2file())) {
      LOG_WARN("Dump to file failed", K(ret));
    }
    print();
  } else {
    LOG_WARN("Read tenant config from inner table error", K_(tenant_id), K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTenantConfig)
{
  int ret = OB_SUCCESS;
  int64_t expect_data_len = get_serialize_size_();
  int64_t saved_pos = pos;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, "[%lu]\n", tenant_id_))) {
  } else {
    ret = ObCommonConfig::serialize(buf, buf_len, pos);
  }
  if (OB_SUCC(ret)) {
    int64_t writen_len = pos - saved_pos;
    if (writen_len != expect_data_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data size", K(writen_len), K(expect_data_len));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTenantConfig)
{
  int ret = OB_SUCCESS;
  ObLatchWGuard wr_guard(lock_, ObLatchIds::CONFIG_LOCK);
  if ('[' != *(buf + pos)) {
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
        pos = cur + 2;
        if ('\0' != *p_end) {
          ret = OB_INVALID_CONFIG;
          LOG_ERROR("invalid tenant id", K(ret));
        } else if (tenant_id != tenant_id_) {
          LOG_ERROR("wrong tenant id", K(ret));
        } else {
          ret = ObCommonConfig::deserialize(buf, data_len, pos);
        }
      } else {
        ret = OB_INVALID_DATA;
        LOG_ERROR("invalid tenant id", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTenantConfig)
{
  int64_t len = 0, tmp_pos = 0;
  int ret = OB_SUCCESS;
  char tenant_str[100] = {'\0'};
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(lock_), ObLatchIds::CONFIG_LOCK);
  if (OB_FAIL(databuff_printf(tenant_str, 100, tmp_pos, "[%lu]\n", tenant_id_))) {
    LOG_WARN("write data buff failed", K(ret));
  } else {
    len += tmp_pos;
  }
  len += ObCommonConfig::get_serialize_size();
  return len;
}

}  // namespace omt
}  // namespace oceanbase
