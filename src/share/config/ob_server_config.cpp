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

#define USING_LOG_PREFIX SHARE_CONFIG
#include <string>
#include <sstream>

#include "share/config/ob_server_config.h"
#include "common/ob_common_utility.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/utility/utility.h"
#include "lib/net/ob_net_util.h"
#include "common/ob_record_header.h"
#include "common/ob_zone.h"
#include "observer/ob_server.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/unit/ob_unit_resource.h"     // ObUnitResource
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_rpc_struct.h"
#include "observer/ob_server_struct.h"
extern "C" {
#include "ussl-hook.h"
#include "auth-methods.h"
}
namespace oceanbase
{
namespace common
{

int64_t get_cpu_count()
{
  int64_t cpu_cnt = GCONF.cpu_count;
  return cpu_cnt > 0 ? cpu_cnt : get_cpu_num();
}

using namespace share;

ObServerConfig::ObServerConfig()
    : disk_actual_space_(0), self_addr_(), system_config_(NULL)
{
}

ObServerConfig::~ObServerConfig()
{
}

ObServerConfig &ObServerConfig::get_instance()
{
  static ObServerConfig config;
  return config;
}

int ObServerConfig::init(const ObSystemConfig &config)
{
  int ret = OB_SUCCESS;
  system_config_ = &config;
  if (OB_ISNULL(system_config_)) {
    ret = OB_INIT_FAIL;
  }
  return ret;
}

bool ObServerConfig::in_upgrade_mode() const
{
  bool bret = false;
  if (enable_upgrade_mode) {
    bret = true;
  } else {
    obrpc::ObUpgradeStage stage = GCTX.get_upgrade_stage();
    bret = (stage >= obrpc::OB_UPGRADE_STAGE_PREUPGRADE
            && stage <= obrpc::OB_UPGRADE_STAGE_POSTUPGRADE);
  }
  return bret;
}

bool ObServerConfig::in_dbupgrade_stage() const
{
  return obrpc::OB_UPGRADE_STAGE_DBUPGRADE == GCTX.get_upgrade_stage();
}

int ObServerConfig::read_config()
{
  int ret = OB_SUCCESS;
  int temp_ret = OB_SUCCESS;
  ObSystemConfigKey key;
  char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  if (OB_UNLIKELY(true != self_addr_.ip_to_string(local_ip, sizeof(local_ip)))) {
    ret = OB_CONVERT_ERROR;
  } else {
    key.set_varchar(ObString::make_string("svr_type"), print_server_role(get_server_type()));
    key.set_int(ObString::make_string("svr_port"), rpc_port);
    key.set_varchar(ObString::make_string("svr_ip"), local_ip);
    key.set_varchar(ObString::make_string("zone"), zone);
    ObConfigContainer::const_iterator it = container_.begin();
    for (; OB_SUCC(ret) && it != container_.end(); ++it) {
      key.set_name(it->first.str());
      if (OB_ISNULL(it->second)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(ERROR, "config item is null", "name", it->first.str(), K(ret));
      } else {
        key.set_version(it->second->version());
        temp_ret = system_config_->read_config(key, *(it->second));
        if (OB_SUCCESS != temp_ret) {
          OB_LOG(DEBUG, "Read config error", "name", it->first.str(), K(temp_ret));
        }
      }
    }
  }
  return ret;
}

int ObServerConfig::check_all() const
{
  int ret = OB_SUCCESS;
  ObConfigContainer::const_iterator it = container_.begin();
  for (; OB_SUCC(ret) && it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "config item is null", "name", it->first.str(), K(ret));
    } else if (!it->second->check()) {
      int temp_ret = OB_INVALID_CONFIG;
      OB_LOG_RET(WARN, temp_ret, "Configure setting invalid",
             "name", it->first.str(), "value", it->second->str(), K(temp_ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObServerConfig::strict_check_special() const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (!cluster_id.check()) {
      ret = OB_INVALID_CONFIG;
      SHARE_LOG(WARN, "invalid cluster id", K(ret), K(cluster_id.str()));
    } else if (strlen(zone.str()) <= 0) {
      ret = OB_INVALID_CONFIG;
      SHARE_LOG(WARN, "config zone cannot be empty", KR(ret), K(zone.str()));
    }
  }
  return ret;
}

void ObServerConfig::print() const
{
  OB_LOG(INFO, "===================== *begin server config report * =====================");
  ObConfigContainer::const_iterator it = container_.begin();
  for (; it != container_.end(); ++it) {
    if (OB_ISNULL(it->second)) {
      OB_LOG_RET(WARN, OB_ERROR, "config item is null", "name", it->first.str());
    } else {
      _OB_LOG(INFO, "| %-36s = %s", it->first.str(), it->second->str());
    }
  }
  OB_LOG(INFO, "===================== *stop server config report* =======================");
}

double ObServerConfig::get_sys_tenant_default_min_cpu()
{
  double min_cpu = server_cpu_quota_min;
  if (0 == min_cpu) {
    int64_t cpu_count = get_cpu_count();
    if (cpu_count < 8) {
      min_cpu = 1;
    } else {
      min_cpu = 2;
    }
  }
  return min_cpu;
}

double ObServerConfig::get_sys_tenant_default_max_cpu()
{
  double max_cpu = server_cpu_quota_max;
  if (0 == max_cpu) {
    int64_t cpu_count = get_cpu_count();
    if (cpu_count < 8) {
      max_cpu = 1;
    } else {
      max_cpu = 2;
    }
  }
  return max_cpu;
}

int ObServerConfig::deserialize_with_compat(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (data_len - pos < MIN_LENGTH) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    /*
     * Extra 'OB_UNIS_VERSION' and 'len' field are added by using new version
     * serialization framework, which makes it hard to use header.version_ for
     * compatible distinguish, also makes codes bellow ugly.
     * TODO: remove this code after 2.2
     */
    const int64_t saved_pos = pos;
    if (OB_FAIL(deserialize(buf, data_len, pos))) {
      /* try old version */
      pos = saved_pos;
      ObRecordHeader header;
      const char *const p_header = buf + pos;
      const char *const p_data = p_header + header.get_serialize_size();
      const int64_t pos_data = pos + header.get_serialize_size();
      if (OB_FAIL(header.deserialize(buf, data_len, pos))) {
        LOG_ERROR("deserialize header failed", K(ret));
      } else if (OB_FAIL(header.check_header_checksum())) {
        LOG_ERROR("check header checksum failed", K(ret));
      } else if (OB_CONFIG_MAGIC != header.magic_) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("check magic number failed", K_(header.magic), K(ret));
      } else if (data_len - pos_data != header.data_zlength_) {
        ret = OB_INVALID_DATA;
        LOG_ERROR("check data len failed",
                  K(data_len), K(pos_data), K_(header.data_zlength), K(ret));
      } else if (OB_FAIL(header.check_payload_checksum(p_data, data_len - pos_data))) {
        LOG_ERROR("check data checksum failed", K(ret));
      } else if (OB_FAIL(add_extra_config(buf + pos, 0, false))) {
        LOG_ERROR("Read server config failed", K(ret));
      } else {
        pos += header.data_length_;
      }
    } // if
  }
  return ret;
}

ObServerMemoryConfig::ObServerMemoryConfig()
  : memory_limit_(0), system_memory_(0), hidden_sys_memory_(0)
{}

ObServerMemoryConfig &ObServerMemoryConfig::get_instance()
{
  static ObServerMemoryConfig memory_config;
  return memory_config;
}

int64_t ObServerMemoryConfig::get_adaptive_memory_config(const int64_t memory_size,
                                                         DependentMemConfig dep_mem_config,
                                                         AdaptiveMemConfig adap_mem_config)
{
  // According to different memory_limit, the kernel can provide adaptive memory_size for default capacity.
  static const int64_t      memory_limit_array[] = {4LL<<30, 12LL<<30, 20LL<<30, 40LL<<30, 60LL<<30, 80LL<<30, 100LL<<30, 130LL<<30};
  static const int64_t     system_memory_array[] = {1LL<<30,  5LL<<30,  6LL<<30,  7LL<<30,  8LL<<30,  9LL<<30,  10LL<<30,  11LL<<30};
  static const int64_t hidden_sys_memory_array[] = {1LL<<30,  2LL<<30,  2LL<<30,  3LL<<30,  3LL<<30,  4LL<<30,   4LL<<30,   4LL<<30};
  static const int64_t array_size = ARRAYSIZEOF(memory_limit_array);

  int64_t adap_memory_size = 1LL<<30;
  int64_t dep_memory_limit = 0;
  const int64_t *dep_array = NULL;
  switch (dep_mem_config) {
    case MEMORY_LIMIT:
      dep_array = memory_limit_array;
      dep_memory_limit = memory_size;
      break;
    case SYSTEM_MEMORY:
      dep_array = system_memory_array;
      dep_memory_limit = memory_size / 0.08;
      break;
  }
  if (memory_size < dep_array[array_size - 1]) {
    // When memory_limit < 130G, adaptive memory is calculated by array.
    // For example, memory_limit = 16G, adaptive system_memory and hidden_sys_memory are 5G and 2G.
    for (int i = array_size - 1; i >= 0; --i) {
      if (memory_size >= dep_array[i]) {
        switch (adap_mem_config) {
          case ADAPTIVE_SYSTEM_MEMORY:
            adap_memory_size = system_memory_array[i];
            break;
          case ADAPTIVE_HIDDEN_SYS_MEMORY:
            adap_memory_size = hidden_sys_memory_array[i];
            break;
        }
        break;
      }
    }
  } else {
    // When memory_limit >= 130G or system_memory >= 11G, system_memory = memory_limit*0.08, hidden_sys_memory = memory_limit*0.03.
    // For example, memory_limit = 200G, adaptive system_memory and hidden_sys_memory are 16G and 6G.
    switch (adap_mem_config) {
      case ADAPTIVE_SYSTEM_MEMORY:
        adap_memory_size = dep_memory_limit * 0.08;
        break;
      case ADAPTIVE_HIDDEN_SYS_MEMORY:
        adap_memory_size = dep_memory_limit * 0.03;
        break;
    }
  }
  return adap_memory_size;
}
int64_t ObServerMemoryConfig::get_extra_memory()
{
  return memory_limit_ < lib::ObRunningModeConfig::MINI_MEM_UPPER ? 0 : hidden_sys_memory_;
}
int ObServerMemoryConfig::reload_config(const ObServerConfig& server_config)
{
  int ret = OB_SUCCESS;
  const bool is_arbitration_mode = OBSERVER.is_arbitration_mode();
  int64_t memory_limit = server_config.memory_limit;

  if (0 == memory_limit) {
    memory_limit = get_phy_mem_size() * server_config.memory_limit_percentage / 100;
  }

  if (is_arbitration_mode) {
    if (memory_limit < (1LL << 30) ) {
      // The memory_limit should not be less than 1G for arbitration mode
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("memory_limit with unexpected value", K(ret), K(memory_limit), "phy mem", get_phy_mem_size());
    } else {
      memory_limit_ = memory_limit;
      LOG_INFO("update observer memory config success", K_(memory_limit));
    }
  } else if (memory_limit < (4LL << 30)) {
    // The memory_limit should not be less than 4G for observer
    ret = OB_INVALID_CONFIG;
    LOG_ERROR("memory_limit with unexpected value", K(ret), K(memory_limit), "phy mem", get_phy_mem_size());
  } else {
    // update observer memory config
    int64_t system_memory = server_config.system_memory;
    int64_t hidden_sys_memory = server_config._hidden_sys_tenant_memory;
    int64_t min_server_avail_memory = max(ObUnitResource::UNIT_MIN_MEMORY, server_config.__min_full_resource_pool_memory);

    if (0 != system_memory && 0 != hidden_sys_memory) {
      // do-nothing
    } else if (0 == system_memory && 0 == hidden_sys_memory) {
      system_memory = get_adaptive_memory_config(memory_limit, MEMORY_LIMIT, ADAPTIVE_SYSTEM_MEMORY);
      hidden_sys_memory = get_adaptive_memory_config(memory_limit, MEMORY_LIMIT, ADAPTIVE_HIDDEN_SYS_MEMORY);
    } else if (0 == hidden_sys_memory) {
      hidden_sys_memory = get_adaptive_memory_config(system_memory, SYSTEM_MEMORY, ADAPTIVE_HIDDEN_SYS_MEMORY);
    } else {
      system_memory = get_adaptive_memory_config(memory_limit, MEMORY_LIMIT, ADAPTIVE_SYSTEM_MEMORY);
    }
    if (memory_limit - system_memory >= min_server_avail_memory &&
        system_memory >= hidden_sys_memory) {
      memory_limit_ = memory_limit;
      system_memory_ = system_memory;
      hidden_sys_memory_ = hidden_sys_memory;
      LOG_INFO("update observer memory config success",
                K_(memory_limit), K_(system_memory), K_(hidden_sys_memory));
    } else {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("update observer memory config failed",
                K(memory_limit), K(system_memory), K(hidden_sys_memory), K(min_server_avail_memory));
    }
  }

#ifdef ENABLE_500_MEMORY_LIMIT

  if (OB_FAIL(ret)) {
    // do-nothing
  } else if (is_arbitration_mode) {
    // do-nothing
  } else if (OB_FAIL(set_500_tenant_limit(server_config._system_tenant_limit_mode))) {
    LOG_ERROR("set the limit of tenant 500 failed", KR(ret));
  }
#endif
  return ret;
}

void ObServerMemoryConfig::check_500_tenant_hold(bool ignore_error)
{
  //check the hold memory of tenant 500
  int ret = OB_SUCCESS;
  int64_t hold = lib::get_tenant_memory_hold(OB_SERVER_TENANT_ID);
  int64_t reserved = system_memory_ - get_extra_memory();
  if (hold > reserved) {
    if (ignore_error) {
      LOG_WARN("the hold memory of tenant_500 is over the reserved memory",
              K(hold), K(reserved));
    } else {
      LOG_ERROR("the hold memory of tenant_500 is over the reserved memory",
              K(hold), K(reserved));
    }
  }
}

#ifdef ENABLE_500_MEMORY_LIMIT
int ObServerMemoryConfig::set_500_tenant_limit(const int64_t limit_mode)
{
  const int64_t UNLIMIT_MODE = 0;
  const int64_t CTX_LIMIT_MODE = 1;
  const int64_t TENANT_LIMIT_MODE = 2;

  int ret = OB_SUCCESS;
  bool unlimited = false;
  auto ma = ObMallocAllocator::get_instance();
  if (UNLIMIT_MODE == limit_mode) {
    unlimited = true;
    ObTenantMemoryMgr::error_log_when_tenant_500_oversize = false;
  } else if (CTX_LIMIT_MODE == limit_mode) {
    ObTenantMemoryMgr::error_log_when_tenant_500_oversize = false;
  } else if (TENANT_LIMIT_MODE == limit_mode) {
    ObTenantMemoryMgr::error_log_when_tenant_500_oversize = true;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid limit mode", K(ret), K(limit_mode));
  }
  for (int ctx_id = 0; OB_SUCC(ret) && ctx_id < ObCtxIds::MAX_CTX_ID; ++ctx_id) {
    if (ObCtxIds::SCHEMA_SERVICE == ctx_id ||
        ObCtxIds::PKT_NIO == ctx_id ||
        ObCtxIds::CO_STACK == ctx_id ||
        ObCtxIds::LIBEASY == ctx_id ||
        ObCtxIds::GLIBC == ctx_id ||
        ObCtxIds::LOGGER_CTX_ID== ctx_id ||
        ObCtxIds::RPC_CTX_ID == ctx_id ||
        ObCtxIds::UNEXPECTED_IN_500 == ctx_id) {
      continue;
    }
    auto ta = ma->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, ctx_id);
    const char *ctx_name = get_global_ctx_info().get_ctx_name(ctx_id);
    if (OB_NOT_NULL(ta)) {
      int64_t ctx_limit = ObCtxIds::DEFAULT_CTX_ID == ctx_id ? (2LL<<30) : (50LL<<20);
      if (unlimited) {
        ctx_limit = INT64_MAX;
      }
      if (OB_FAIL(ta->set_limit(ctx_limit))) {
        LOG_WARN("set ctx limit of 500 tenant failed", K(ret), K(ctx_limit),
                 "ctx_name", ctx_name);
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant ctx allocator is not exist", K(ret),
               "ctx_name", ctx_name);
    }
  }
  return ret;
}
#endif

int ObServerConfig::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObRecordHeader header;
  // 这里 header 的序列化方法用的是非变长序列化，不对数字做编码
  int64_t header_len = header.get_serialize_size();
  int64_t expect_data_len = get_serialize_size_() - header_len;

  char *const p_header = buf + pos;
  char *const p_data   = p_header + header_len;
  const int64_t data_pos = pos + header_len;
  int64_t saved_header_pos     = pos;
  pos += header_len;

  // data first
  if (OB_FAIL(ObCommonConfig::serialize(buf, buf_len, pos))) {
  } else if (OB_FAIL(OTC_MGR.serialize(buf, buf_len, pos))) {
  } else {
    header.magic_ = OB_CONFIG_MAGIC;
    header.header_length_ = static_cast<int16_t>(header_len);
    header.version_ = OB_CONFIG_VERSION;
    header.data_length_ = static_cast<int32_t>(pos - data_pos);
    header.data_zlength_ = header.data_length_;
    if (header.data_zlength_ != expect_data_len) {
      LOG_WARN("unexpected data size", K_(header.data_zlength),
                                          K(expect_data_len));
    } else {
      header.data_checksum_ = ob_crc64(p_data, pos - data_pos);
      header.set_header_checksum();
      ret = header.serialize(buf, buf_len, saved_header_pos);
    }
  }
  return ret;
}

int ObServerConfig::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_FAIL(serialize_(buf, buf_len, pos))) {
      LOG_WARN("ObServerConfig serialize fail", K(ret));
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    if (OB_SUCC(ret)) {
      ret = NS_::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes,
        size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObServerConfig)
{
  int ret = OB_SUCCESS;
  if (data_len == 0 || pos >= data_len) {
  } else {
    // header
    ObRecordHeader header;
    int64_t header_len = header.get_serialize_size();
    const char *const p_header = buf + pos;
    const char *const p_data = p_header + header_len;
    const int64_t data_pos = pos + header_len;
    if (OB_FAIL(header.deserialize(buf, data_len, pos))) {
      LOG_ERROR("deserialize header failed", K(ret));
    } else if (OB_FAIL(header.check_header_checksum())) {
      LOG_ERROR("check header checksum failed", K(ret));
    } else if (OB_CONFIG_MAGIC != header.magic_) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("check magic number failed", K_(header.magic), K(ret));
    } else if (data_len - data_pos != header.data_zlength_) {
      ret = OB_INVALID_DATA;
      LOG_ERROR("check data len failed",
                K(data_len), K(data_pos), K_(header.data_zlength), K(ret));
    } else if (OB_FAIL(header.check_payload_checksum(p_data,
                                                     data_len - data_pos))) {
      LOG_ERROR("check data checksum failed", K(ret));
    } else if (OB_FAIL(ObCommonConfig::deserialize(buf, data_len, pos))) {
      LOG_ERROR("deserialize cluster config failed", K(ret));
    } else if (OB_FAIL(OTC_MGR.deserialize(buf, data_len, pos))){
      LOG_ERROR("deserialize tenant config failed", K(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObServerConfig)
{
  int64_t len = 0;
  ObRecordHeader header;
  // 1) header size
  len += header.get_serialize_size();
  // 2) cluster config size
  len += ObCommonConfig::get_serialize_size();
  // 3) tenant config size
  len += OTC_MGR.get_serialize_size();
  return len;
}

} // end of namespace common
namespace obrpc {
bool enable_pkt_nio(bool start_as_client) {
  bool bool_ret = false;
  if (OB_UNLIKELY(start_as_client || OBSERVER.is_arbitration_mode())) {
    bool enable_client_auth = (get_client_auth_methods() != USSL_AUTH_NONE);
    bool_ret = GCONF._enable_pkt_nio && enable_client_auth;
  } else {
    bool_ret =  GCONF._enable_pkt_nio && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0;
  }
  return bool_ret;
}

int64_t get_max_rpc_packet_size()
{
  return GCONF._max_rpc_packet_size;
}
} // end of namespace obrpc
} // end of namespace oceanbase

namespace easy
{
int64_t get_easy_per_dest_memory_limit()
{
  return GCONF.__easy_memory_limit; // no global memory limit for easy, just use GCONF.__easy_memory_limit as per server dest limit
}
};
