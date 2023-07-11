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
      } else if (OB_FAIL(add_extra_config(buf + pos, 0, false, false))) {
        LOG_ERROR("Read server config failed", K(ret));
      } else {
        pos += header.data_length_;
      }
    } // if
  }
  return ret;
}

ObServerMemoryConfig::ObServerMemoryConfig()
  : memory_limit_(0), system_memory_(0)
{}

ObServerMemoryConfig &ObServerMemoryConfig::get_instance()
{
  static ObServerMemoryConfig memory_config;
  return memory_config;
}

int64_t ObServerMemoryConfig::get_capacity_default_memory(CapacityType type, int64_t memory_limit)
{
  // According to different memory_limit, the kernel can provide adaptive memory_size for default capacity.
  // For example, memory_limit = 16G, adaptive system_memory and hidden_sys_memory are 6G and 2G.
  static const int64_t      memory_limit_array[] = {4LL<<30, 8LL<<30, 14LL<<30, 28LL<<30, 48LL<<30, 56LL<<30, 65LL<<30, 96LL<<30, 128LL<<30};
  static const int64_t     system_memory_array[] = {2LL<<30, 3LL<<30,  6LL<<30, 10LL<<30, 12LL<<30, 13LL<<30, 15LL<<30, 18LL<<30,  20LL<<30};
  static const int64_t hidden_sys_memory_array[] = {1LL<<30, 2LL<<30,  2LL<<30,  2LL<<30,  4LL<<30,  4LL<<30,  6LL<<30,  7LL<<30,   8LL<<30};

  int64_t memory_size = 0;
  for (int i = ARRAYSIZEOF(memory_limit_array) - 1; i >= 0; --i) {
    if (memory_limit_array[i] <= memory_limit) {
      switch (type) {
        case SYSTEM_MEMORY:
          memory_size = system_memory_array[i];
          break;
        case HIDDEN_SYS_MEMORY:
          memory_size = hidden_sys_memory_array[i];
          break;
      }
      break;
    }
  }
  return memory_size;
}

int ObServerMemoryConfig::reload_config(const ObServerConfig& server_config)
{
  int ret = OB_SUCCESS;
  const bool is_arbitration_mode = OBSERVER.is_arbitration_mode();
  int64_t memory_limit = server_config.memory_limit;
  if (0 == memory_limit) {
    memory_limit = get_phy_mem_size() * server_config.memory_limit_percentage / 100;
  }
  int64_t system_memory = server_config.system_memory;
  if (memory_limit < (1 << 30) ) {
    // The memory_limit should not be less than 1G for arbitration mode
    ret = OB_INVALID_CONFIG;
    LOG_ERROR("memory_limit with unexpected value", K(ret), K(memory_limit), "phy mem", get_phy_mem_size());
  } else if (is_arbitration_mode) {
    // do nothing
  } else if (0 == system_memory) {
    system_memory = get_capacity_default_memory(SYSTEM_MEMORY, memory_limit);
    if (0 == system_memory) {
      ret = OB_INVALID_CONFIG;
      LOG_ERROR("memory_limit with unexpected value", K(ret), K(memory_limit), "phy mem", get_phy_mem_size());
    }
  }

  if (OB_FAIL(ret)) {
  // do nothing
  } else if (memory_limit > system_memory) {
    memory_limit_ = memory_limit;
    system_memory_ = system_memory;
    LOG_INFO("update memory_limit or system_memory success",
              K(memory_limit_), K(system_memory_));
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_ERROR("update memory_limit or system_memory failed",
              K(memory_limit), K(system_memory));
  }

  int64_t observer_tenant_hold = lib::get_tenant_memory_hold(OB_SERVER_TENANT_ID);
  if (observer_tenant_hold > system_memory_ && false == is_arbitration_mode) {
    if (server_config._ignore_system_memory_over_limit_error) {
      LOG_WARN("the hold of tenant_500 is over the system_memory",
               K(observer_tenant_hold), K_(system_memory));
    } else {
      LOG_ERROR("the hold of tenant_500 is over the system_memory",
                K(observer_tenant_hold), K_(system_memory));
    }
  }

#ifdef ENABLE_500_MEMORY_LIMIT
  if (OB_FAIL(ret)) {
    // do-nothing
  } else if (is_arbitration_mode) {
    // do-nothing
  } else if (OB_FAIL(ObMallocAllocator::get_instance()->set_500_tenant_limit(
      !server_config._enable_system_tenant_memory_limit))) {
    LOG_ERROR("set the limit of tenant 500 failed", KR(ret));
  }
#endif
  return ret;
}

void ObServerMemoryConfig::set_server_memory_limit(int64_t memory_limit)
{
  if (memory_limit > system_memory_) {
    LOG_INFO("update memory_limit success", K(memory_limit), K(system_memory_));
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "update memory_limit failed", K(memory_limit), K(system_memory_));
  }
}

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
bool enable_pkt_nio() {
  return GCONF._enable_pkt_nio
      && (!OBSERVER.is_arbitration_mode())
      && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0;
}
int64_t get_max_rpc_packet_size()
{
  return GCONF._max_rpc_packet_size;
}
}
} // end of namespace oceanbase

namespace easy
{
int64_t get_easy_per_dest_memory_limit()
{
  return GCONF.__easy_memory_limit; // no global memory limit for easy, just use GCONF.__easy_memory_limit as per server dest limit
}
};
