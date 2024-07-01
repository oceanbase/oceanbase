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
#include "lib/net/ob_net_util.h"
#include "lib/oblog/ob_log.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"
#include "share/errsim_module/ob_errsim_module_interface_imp.h"
#include "src/sql/ob_optimizer_trace_impl.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace omt {

ObTenantConfig::ObTenantConfig() : ObTenantConfig(OB_INVALID_TENANT_ID)
{
}

ObTenantConfig::ObTenantConfig(uint64_t tenant_id)
    : tenant_id_(tenant_id), current_version_(INITIAL_TENANT_CONF_VERSION),
      mutex_(),
      update_task_(), system_config_(), config_mgr_(nullptr),
      ref_(0L), is_deleting_(false), create_timestamp_(0L)
{
}

int ObTenantConfig::init(ObTenantConfigMgr *config_mgr)
{
  int ret = OB_SUCCESS;
  config_mgr_ = config_mgr;
  create_timestamp_ = ObTimeUtility::current_time();
  if (OB_FAIL(system_config_.init())) {
    LOG_ERROR("init system config failed", K(ret));
  } else if (OB_FAIL(update_task_.init(config_mgr, this))) {
    LOG_ERROR("init tenant config updata task failed", K_(tenant_id), K(ret));
  }
  return ret;
}

void ObTenantConfig::print() const
{
  OB_LOG(INFO, "===================== * begin tenant config report * =====================", K(tenant_id_));
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "config item is null", "name", it->first.str());
    } else {
      _OB_LOG(INFO, "| %-36s = %s", it->first.str(), it->second->str());
    }
  }
  OB_LOG(INFO, "===================== * stop tenant config report * =======================", K(tenant_id_));
}

void ObTenantConfig::trace_all_config() const
{
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
    } else if (it->second->case_compare(it->second->default_str()) != 0) {
      OPT_TRACE("  ", it->first.str(), " = ", it->second->str());
    }
  }
}

int ObTenantConfig::read_config()
{
  int ret = OB_SUCCESS;
  ObSystemConfigKey key;
  ObAddr server;
  char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  server = GCTX.self_addr();
  if (OB_UNLIKELY(true != server.ip_to_string(local_ip, sizeof(local_ip)))) {
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
        int temp_ret = system_config_.read_config(get_tenant_id(), key, *(it->second));
        if (OB_SUCCESS != temp_ret) {
          OB_LOG(DEBUG, "Read config error", "name", it->first.str(), K(temp_ret));
        }
      }
    } // for
  }
  return ret;
}

void ObTenantConfig::TenantConfigUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(config_mgr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(config_mgr), K(ret));
  } else if (OB_ISNULL(tenant_config_)){
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K_(tenant_config), K(ret));
  } else {
    const int64_t saved_current_version = tenant_config_->current_version_;
    const int64_t version = version_;
    THIS_WORKER.set_timeout_ts(INT64_MAX);
    if (tenant_config_->current_version_ >= version) {
      ret = OB_ALREADY_DONE;
    } else if (update_local_) {
      tenant_config_->current_version_ = version;
      if (OB_FAIL(tenant_config_->system_config_.clear())) {
        LOG_WARN("Clear system config map failed", K(ret));
      } else if (OB_FAIL(config_mgr_->update_local(tenant_config_->tenant_id_, version))) {
        LOG_WARN("ObTenantConfigMgr update_local failed", K(ret), K(tenant_config_));
      } else {
        config_mgr_->notify_tenant_config_changed(tenant_config_->tenant_id_);
      }

      sql::ObFLTControlInfoManager mgr(tenant_config_->tenant_id_);
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(mgr.init())) {
        LOG_WARN("fail to init", KR(ret));
      } else if (OB_FAIL(mgr.apply_control_info())) {
        LOG_WARN("fail to apply control info", KR(ret));
      } else {
        LOG_TRACE("apply control info", K(tenant_config_->tenant_id_));
      }

      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        uint64_t tenant_id = tenant_config_->tenant_id_;
        tenant_config_->current_version_ = saved_current_version;
        share::schema::ObSchemaGetterGuard schema_guard;
        share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
        bool tenant_dropped = false;
        // 租户如果正在删除过程中,schema失效，则返回添加反复失败，会导致定时器任务超量
        if (OB_ISNULL(schema_service)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_service is null", K(ret), K(tmp_ret));
        } else if (OB_SUCCESS != (tmp_ret = schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
          LOG_WARN("fail to get schema guard", K(ret), K(tmp_ret), K(tenant_id));
        } else if (OB_SUCCESS != (tmp_ret = schema_guard.check_if_tenant_has_been_dropped(tenant_id, tenant_dropped))) {
          LOG_WARN("fail to check if tenant has been dropped", K(ret), K(tmp_ret), K(tenant_id));
        } else {
          if (tenant_dropped) {
            LOG_INFO("tenant has dropped", K(tenant_id));
          } else if ((ATOMIC_FAA(&running_task_count_, 1) < 2)) {
            if (OB_FAIL(config_mgr_->schedule(*this, 0))) {
              LOG_WARN("schedule task failed", K(tenant_id), K(ret));
              ATOMIC_DEC(&running_task_count_);
            } else if (tenant_config_->is_deleting_) {
              LOG_INFO("tenant under deleting, cancel task", K(tenant_id));
              if (OB_FAIL(config_mgr_->cancel(*this))) {
                LOG_WARN("cancel task failed", K(tenant_id), K(ret));
              } else {
                ATOMIC_DEC(&running_task_count_);
              }
            } else {
              LOG_INFO("Schedule a retry task!", K(tenant_id));
            }
          } else {
            ATOMIC_DEC(&running_task_count_);
            LOG_INFO("already 2 running task, temporory no need more", K(tenant_id));
          }
        }
      } else {
        const int64_t read_version = tenant_config_->system_config_.get_version();
        LOG_INFO("loaded new tenant config",
                 "tenant_id", tenant_config_->tenant_id_,
                 "read_version", read_version,
                 "old_version", saved_current_version,
                 "current_version", tenant_config_->current_version_,
                 "expected_version", version);
        tenant_config_->print();
      }
    }
  }

  ATOMIC_DEC(&running_task_count_);
}

// 需要解决的场景：
// 场景1：脚本中更新上百个参数，每个参数触发一次 got_version，如果不加以防范，
//        会导致 timer 线程 32 个槽位被耗尽，导致参数更新丢失。
// 场景2: heartbeat 始终广播相同的 version，只需要响应最多一次
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
    if (!mutex_.trylock()) {
      LOG_DEBUG("Processed by other thread!");
    } else {
      LOG_INFO("Got new tenant config version", K_(tenant_id),
                K_(current_version), K(version));
      update_task_.update_local_ = true;
      update_task_.version_ = version;
      update_task_.scheduled_time_ = ObClockGenerator::getClock();
      schedule_task = true;
      mutex_.unlock();
    }
  }
  if (schedule_task) {
    bool schedule = true;
    if (!config_mgr_->inited()) {
      schedule = false;
      ret = OB_NOT_INIT;
      LOG_WARN("Couldn't update config because timer is NULL", K(ret));
    }
    if (schedule && !is_deleting_) {
      // 为了避免极短时间内上百个变量被更新导致生成上百个update_task
      // 占满 timer slots （32个），我们需要限定短时间内生成的 update_task
      // 数量。考虑到每个 update_task 的任务都是同质的（不区分version，都是
      // 负责把 parameter 刷到最新），我们只需要为这数百个变量更新生成
      // 1个 update task 即可。但是，考虑到 update  task 执行过程中还会有
      // 新的变量更新到来，为了让这些更新不丢失，需要另外在生成一个任务负责
      // “扫尾”。整个时序如下图：
      //
      // t----> 时间增长的方向
      // '~' 表示在 timer 队列中等待调度
      // '-' 表示 task 被 timer 调度执行中
      //
      // case1: task3 在 task1 结束后进入 timer 队列
      // |~~~~|--- task1 ----|
      //            |~~~~~~~~|--- task2 ----|
      //                      |~~~~~~~~~~~~~|--- task3 ----|
      //
      // case2: task3 在 task1 结束前就进入了 timer 队列
      // |~~~~|--- task1 ----|
      //            |~~~~~~~~|--- task2 ----|
      //                    |~~~~~~~~~~~~~~~|--- task3 ----|
      //
      //

      // running_task_count_ 可以
      // 不甚精确地限定同一时刻只能有两个 update_task，要么两个都在
      // 调度队列中等待，要么一个在运行另一个在调度队列中等待（上图 case1)。
      // 之所以说“不甚精确”，是因为 running_task_count_-- 操作是在
      // runTimerTask() 的结尾调用的，那么存在一种小概率的情况，某一很短的
      // 时刻，有大于 2 个 update_task 在调度队列中等待（上图的case2）。
      // 不过我们认为，这种情况可以接受，不会影响正确性。
      if ((ATOMIC_FAA(&update_task_.running_task_count_, 1) < 2)) {
        if (OB_FAIL(config_mgr_->schedule(update_task_, 0))) {
          LOG_WARN("schedule task failed", K_(tenant_id), K(ret));
          ret = OB_SUCCESS; // if task reach 32 limit, it has chance to retry later
          ATOMIC_DEC(&update_task_.running_task_count_);
        } else {
          LOG_INFO("Schedule update tenant config task successfully!", K_(tenant_id));
        }
      } else {
        ATOMIC_DEC(&update_task_.running_task_count_);
        LOG_INFO("already 2 running task, temporory no need more", K_(tenant_id));
      }
    }
  }
  return ret;
}

int ObTenantConfig::update_local(int64_t expected_version, ObMySQLProxy::MySQLResult &result,
                                 bool save2file /* = true */)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(system_config_.update(result))) {
    LOG_WARN("failed to load system config", K(ret));
  } else if (expected_version != ObSystemConfig::INIT_VERSION && (system_config_.get_version() < current_version_
          || system_config_.get_version() < expected_version)) {
    ret = OB_EAGAIN;
    LOG_WARN("__tenant_parameter is older than the expected version", K(ret),
             "read_version", system_config_.get_version(),
             "current_version", current_version_,
             "expected_version", expected_version);
  } else {
    LOG_INFO("read tenant config from __tenant_parameter succeed", K_(tenant_id));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(read_config())) {
      LOG_ERROR("Read tenant config failed", K_(tenant_id), K(ret));
    } else if (save2file && OB_FAIL(config_mgr_->dump2file())) {
      LOG_WARN("Dump to file failed", K(ret));
    } else if (OB_FAIL(publish_special_config_after_dump())) {
      LOG_WARN("publish special config after dump failed", K(tenant_id_), K(ret));
    }
#ifdef ERRSIM
    else if (OB_FAIL(build_errsim_module_())) {
      LOG_WARN("failed to build errsim module", K(ret), K(tenant_id_));
    }
#endif
    print();
  } else {
    LOG_WARN("Read tenant config from inner table error", K_(tenant_id), K(ret));
  }
  return ret;
}

int ObTenantConfig::publish_special_config_after_dump()
{
  int ret = OB_SUCCESS;
  ObConfigItem *const *pp_item = NULL;
  if (OB_ISNULL(pp_item = container_.get(ObConfigStringKey(COMPATIBLE)))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("Invalid config string", K(tenant_id_), K(ret));
  } else if (!(*pp_item)->dump_value_updated()) {
    LOG_INFO("config dump value is not set, no need read", K(tenant_id_), K((*pp_item)->spfile_str()));
  } else {
    uint64_t new_data_version = 0;
    uint64_t old_data_version = 0;
    bool value_updated = (*pp_item)->value_updated();
    if (OB_FAIL(ObClusterVersion::get_version((*pp_item)->spfile_str(), new_data_version))) {
      LOG_ERROR("parse data_version failed", KR(ret), K((*pp_item)->spfile_str()));
    } else if (OB_FAIL(ObClusterVersion::get_version((*pp_item)->str(), old_data_version))) {
      LOG_ERROR("parse data_version failed", KR(ret), K((*pp_item)->str()));
    } else if (!value_updated && old_data_version != DATA_CURRENT_VERSION) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_LOG(ERROR, "unexpected data_version", KR(ret), K(old_data_version));
    } else if (value_updated && new_data_version <= old_data_version) {
      LOG_INFO("[COMPATIBLE] [DATA_VERSION] no need to update", K(tenant_id_),
               "old_data_version", DVP(old_data_version),
               "new_data_version", DVP(new_data_version));
      // do nothing
    } else {
      if (!(*pp_item)->set_value((*pp_item)->spfile_str())) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("Invalid config value", K(tenant_id_), K((*pp_item)->spfile_str()), K(ret));
      } else {
        FLOG_INFO("[COMPATIBLE] [DATA_VERSION] read data_version after dump",
                  KR(ret), K_(tenant_id), "version", (*pp_item)->version(),
                  "value", (*pp_item)->str(), "value_updated",
                  (*pp_item)->value_updated(), "dump_version",
                  (*pp_item)->dumped_version(), "dump_value",
                  (*pp_item)->spfile_str(), "dump_value_updated",
                  (*pp_item)->dump_value_updated());
      }
    }
  }
  return ret;
}

int ObTenantConfig::add_extra_config(const char *config_str,
                                     int64_t version /* = 0 */ ,
                                     bool check_config /* = true */)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_OPTS_LENGTH = sysconf(_SC_ARG_MAX);
  int64_t config_str_length = 0;
  char *buf = NULL;
  char *saveptr = NULL;
  char *token = NULL;
  if (OB_ISNULL(config_str)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config str is null", K(ret));
  } else if ((config_str_length = static_cast<int64_t>(STRLEN(config_str))) >= MAX_OPTS_LENGTH) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("Extra config is too long", K(ret));
  } else if (OB_ISNULL(buf = new (std::nothrow) char[config_str_length + 1])) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("ob tc malloc memory for buf fail", K(ret));
  } else {
    MEMCPY(buf, config_str, config_str_length);
    buf[config_str_length] = '\0';
    token = STRTOK_R(buf, ",\n", &saveptr);
    const ObString compatible_cfg(COMPATIBLE);
    const ObString enable_compatible_monotonic_cfg(ENABLE_COMPATIBLE_MONOTONIC);
    while (OB_SUCC(ret) && OB_LIKELY(NULL != token)) {
      char *saveptr_one = NULL;
      char *name = NULL;
      const char *value = NULL;
      ObConfigItem *const *pp_item = NULL;
      if (OB_ISNULL(name = STRTOK_R(token, "=", &saveptr_one))) {
        ret = OB_INVALID_CONFIG;
        LOG_ERROR("Invalid config string", K(token), K(ret));
      } else if (OB_ISNULL(saveptr_one) || OB_UNLIKELY('\0' == *(value = saveptr_one))) {
        LOG_INFO("Empty config string", K(token), K(name));
        // ret = OB_INVALID_CONFIG;
        name = NULL;
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(name)) {
          // do nothing, just skip this parameter
        } else {
          char curr_tid_str[32] = {'\0'}; // 32 is enougth for uint64_t
          snprintf(curr_tid_str, sizeof(curr_tid_str), "%lu", tenant_id_);
          char *tid_in_arg = NULL;
          name = STRTOK_R(name, "@", &tid_in_arg);
          if (OB_ISNULL(name) || OB_UNLIKELY('\0' == *name)) {
            // skip this parameter because name is invalid
          } else if (OB_NOT_NULL(tid_in_arg) && ('\0' != *tid_in_arg) &&
                     0 != strcmp(tid_in_arg, curr_tid_str)) {
            // skip this parameter because the tenant_id does bot match
          } else {
            const int value_len = strlen(value);
            // hex2cstring -> value_len / 2 + 1
            // '\0' -> 1
            const int external_info_val_len = value_len / 2 + 1 + 1;
            char *external_info_val = (char*)ob_malloc(external_info_val_len, "temp");
            DEFER(if (external_info_val != nullptr) ob_free(external_info_val););
            if (OB_ISNULL(external_info_val)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc", K(ret));
            } else if (FALSE_IT(external_info_val[0] = '\0')) {
            } else if (OB_ISNULL(pp_item = container_.get(ObConfigStringKey(name)))) {
              ret = OB_SUCCESS;
              LOG_WARN("Invalid config string, no such config item", K(name), K(value), K(ret));
            }
            if (OB_FAIL(ret) || OB_ISNULL(pp_item)) {
            } else if (0 == compatible_cfg.case_compare(name)) {
              // init tenant and observer reload with -o will use this interface to update tenant's
              // config. considering the -o situation, we need to ensure the new_data_version won't
              // go back
              uint64_t new_data_version = 0;
              uint64_t old_data_version = 0;
              if (OB_FAIL(ObClusterVersion::get_version(value, new_data_version))) {
                LOG_ERROR("parse data_version failed", KR(ret), K(value));
              } else if (((*pp_item)->value_updated() || (*pp_item)->dump_value_updated()) &&
                         OB_FAIL(ObClusterVersion::get_version(
                             (*pp_item)->spfile_str(), old_data_version))) {
                LOG_ERROR("parse data_version failed", KR(ret), K((*pp_item)->spfile_str()));
              } else if (new_data_version <= old_data_version) {
                // do nothing
                LOG_INFO("[COMPATIBLE] DATA_VERSION no need to update",
                         K(tenant_id_), K(old_data_version),
                         K(new_data_version));
              } else {
                if (!(*pp_item)->set_dump_value(value)) {
                  ret = OB_INVALID_CONFIG;
                  LOG_WARN("Invalid config value", K(name), K(value), K(ret));
                } else {
                  (*pp_item)->set_dump_value_updated();
                  (*pp_item)->set_version(version);
                  int tmp_ret = 0;
                  if (OB_TMP_FAIL(ODV_MGR.set(tenant_id_, new_data_version))) {
                    LOG_WARN("fail to set data_version", KR(tmp_ret),
                             K(tenant_id_), K(new_data_version));
                  }
                  FLOG_INFO("[COMPATIBLE] [DATA_VERSION] init data_version before dump",
                            KR(ret), K_(tenant_id), "version",
                            (*pp_item)->version(), "value", (*pp_item)->str(),
                            "value_updated", (*pp_item)->value_updated(),
                            "dump_version", (*pp_item)->dumped_version(),
                            "dump_value", (*pp_item)->spfile_str(),
                            "dump_value_updated", (*pp_item)->dump_value_updated(),
                            K(old_data_version), K(new_data_version));
                }
              }
            } else if (!(*pp_item)->set_value(value)) {
              ret = OB_INVALID_CONFIG;
              LOG_WARN("Invalid config value", K(name), K(value), K(ret));
            } else if (check_config && (!(*pp_item)->check_unit(value) || !(*pp_item)->check())) {
              ret = OB_INVALID_CONFIG;
              const char* range = (*pp_item)->range();
              if (OB_ISNULL(range) || strlen(range) == 0) {
                LOG_ERROR("Invalid config, value out of range", K(name), K(value), K(ret));
              } else {
                _LOG_ERROR("Invalid config, value out of %s (for reference only). name=%s, value=%s, ret=%d", range, name, value, ret);
              }
            } else {
              (*pp_item)->set_version(version);
              LOG_INFO("Load tenant config succ", K(name), K(value));
              if (0 == enable_compatible_monotonic_cfg.case_compare(name)) {
                ObString v_str((*pp_item)->str());
                ODV_MGR.set_enable_compatible_monotonic(0 == v_str.case_compare("True") ? true
                                                                                        : false);
              }
            }
          }
        }
        token = STRTOK_R(NULL, ",\n", &saveptr);
      }
    }
  }
  if (NULL != buf) {
    delete [] buf;
    buf = NULL;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTenantConfig)
{
  int ret = OB_SUCCESS;
  int64_t expect_data_len = get_serialize_size_();
  int64_t saved_pos = pos;
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
  if ('[' != *(buf + pos)) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid tenant config", K(ret));
  } else {
    int64_t cur = pos + 1;
    while (cur < data_len - 1 && ']' != *(buf + cur)) {
      ++cur;
    } // while
    if (cur >= data_len - 1 || '\n' != *(buf + cur + 1)) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("invalid tenant config", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      char tenant_str[100];
      char *p_end = nullptr;
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
  if (OB_FAIL(databuff_printf(tenant_str, 100, tmp_pos, "[%lu]\n", tenant_id_))) {
    LOG_WARN("write data buff failed", K(ret));
  } else {
    len += tmp_pos;
  }
  len += ObCommonConfig::get_serialize_size();
  return len;
}

#ifdef ERRSIM
int ObTenantConfig::build_errsim_module_()
{
  int ret = OB_SUCCESS;
  char buf[ObErrsimModuleTypeHelper::MAX_TYPE_NAME_LENGTH] = "";
  ObTenantErrsimModuleMgr::ModuleArray module_array;
  ObTenantErrsimModuleMgr::ErrsimModuleString string;

  for (int64_t i = 0; OB_SUCC(ret) && i < this->errsim_module_types.size(); ++i) {
    if (OB_FAIL(this->errsim_module_types.get(
        static_cast<int>(i), buf, sizeof(buf)))) {
      LOG_WARN("get rs failed", K(ret), K(i));
    } else if (OB_FAIL(string.assign(buf))) {
      LOG_WARN("failed to assign buffer", K(ret));
    } else if (OB_FAIL(module_array.push_back(string))) {
      LOG_WARN("failed to push string into array", K(ret), K(string));
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t percentage = this->errsim_module_error_percentage;

    if (OB_FAIL(build_tenant_errsim_moulde(tenant_id_, current_version_, module_array, percentage))) {
      LOG_WARN("failed to build tenant module", K(ret), K(tenant_id_));
    }
  }
  return ret;
}
#endif


} // omt
} // oceanbase
