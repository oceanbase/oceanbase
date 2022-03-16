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

#define USING_LOG_PREFIX BOOTSTRAP

#include "rootserver/ob_bootstrap.h"

#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "lib/string/ob_sql_string.h"
#include "lib/list/ob_dlist.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/utility/ob_print_utils.h"
#include "common/data_buffer.h"
#include "common/ob_role.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_srv_rpc_proxy.h"
#include "common/ob_member_list.h"
#include "share/ob_max_id_fetcher.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_ddl_sql_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_server_status.h"
#include "share/ob_worker.h"
#include "share/config/ob_server_config.h"
#include "share/ob_primary_zone_util.h"
#include "share/ob_schema_status_proxy.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_file_system_util.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "rootserver/ob_partition_creator.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_locality_util.h"
#include "rootserver/ob_rs_gts_manager.h"
#include "rootserver/ob_rs_gts_monitor.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"
#include "rootserver/ob_freeze_info_manager.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;
namespace rootserver {

ObBaseBootstrap::ObBaseBootstrap(
    ObSrvRpcProxy& rpc_proxy, const ObServerInfoList& rs_list, common::ObServerConfig& config)
    : step_id_(0), rpc_proxy_(rpc_proxy), rs_list_(rs_list), config_(config)
{
  std::sort(rs_list_.begin(), rs_list_.end());
}

int ObBaseBootstrap::gen_sys_unit_ids(const ObIArray<ObZone>& zones, ObIArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> sorted_zones;
  unit_ids.reuse();
  if (zones.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zones is empty", K(zones), K(ret));
  } else if (OB_FAIL(sorted_zones.assign(zones))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    std::sort(sorted_zones.begin(), sorted_zones.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < sorted_zones.count(); ++j) {
        if (sorted_zones.at(j) == zones.at(i)) {
          if (OB_FAIL(unit_ids.push_back(OB_SYS_UNIT_ID + static_cast<uint64_t>(j)))) {
            LOG_WARN("push_back failed", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (rs_list_.count() <= 0) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("rs_list is empty", K_(rs_list), K(ret));
  }
  return ret;
}

int ObBaseBootstrap::check_multiple_zone_deployment_rslist(const ObServerInfoList& rs_list)
{
  int ret = OB_SUCCESS;
  // In the multi zone deployment mode,
  // each server must come from a different zone,
  // and it will throw exception if there are duplicate zones
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list.count(); ++i) {
    const ObZone& zone = rs_list[i].zone_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rs_list.count(); ++j) {
      if (i != j) {
        if (zone == rs_list[j].zone_) {
          ret = OB_PARTITION_ZONE_DUPLICATED;
          LOG_WARN("should not choose two rs in same zone",
              "server1",
              to_cstring(rs_list[i].server_),
              "server2",
              to_cstring(rs_list[j].server_),
              K(zone),
              K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::check_bootstrap_rs_list(const ObServerInfoList& rs_list)
{
  int ret = OB_SUCCESS;
  if (rs_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_list size must larger than 0", K(ret));
  } else {
    if (OB_FAIL(check_multiple_zone_deployment_rslist(rs_list))) {
      LOG_WARN("fail to check multiple zone deployment rslist", K(ret));
    }
  }
  // BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBaseBootstrap::gen_sys_unit_ids(ObIArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  unit_ids.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(unit_ids.push_back(OB_SYS_UNIT_ID + static_cast<uint64_t>(i)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_sys_zone_list(ObIArray<ObZone>& zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs list count unexpected", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(zone_list.push_back(rs_list_.at(i).zone_))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_gts_unit_ids(ObIArray<uint64_t>& unit_ids)
{
  int ret = OB_SUCCESS;
  unit_ids.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(unit_ids.push_back(OB_GTS_UNIT_ID + static_cast<uint64_t>(i)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_gts_units(ObIArray<share::ObUnit>& units)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> unit_ids;
  units.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_gts_unit_ids(unit_ids))) {
    LOG_WARN("gen gts unit ids failed", K(ret));
  } else {
    ObUnit unit;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count() && i < ObRsGtsMonitor::GTS_QUORUM; ++i) {
      unit.reset();
      unit.unit_id_ = unit_ids.at(i);
      unit.resource_pool_id_ = OB_GTS_RESOURCE_POOL_ID;
      unit.group_id_ = 0;
      unit.zone_ = rs_list_.at(i).zone_;
      unit.server_ = rs_list_.at(i).server_;
      unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
      if (OB_FAIL(units.push_back(unit))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_sys_units(ObIArray<share::ObUnit>& units)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> unit_ids;
  units.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_unit_ids(unit_ids))) {
    LOG_WARN("gen_sys_unit_ids failed", K(ret));
  } else {
    ObUnit unit;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      unit.reset();
      unit.unit_id_ = unit_ids.at(i);
      unit.resource_pool_id_ = OB_SYS_RESOURCE_POOL_ID;
      unit.group_id_ = 0;
      unit.zone_ = rs_list_.at(i).zone_;
      unit.server_ = rs_list_.at(i).server_;
      unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
      if (OB_FAIL(units.push_back(unit))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::fill_sys_unit_config(const share::ObUnitConfig& sample_config, share::ObUnitConfig& target_config)
{
  int ret = OB_SUCCESS;
  if (!sample_config.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnt", K(ret), K(sample_config));
  } else {
    target_config.reset();
    target_config.unit_config_id_ = OB_SYS_UNIT_CONFIG_ID;
    target_config.name_ = OB_SYS_UNIT_CONFIG_NAME;
    double cpu = std::min(DEFAULT_MAX_SYS_CPU, sample_config.max_cpu_);
    int64_t factor = static_cast<int64_t>(sample_config.max_cpu_ / cpu);
    if (!config_.enable_rootservice_standalone) {
      target_config.max_cpu_ = cpu;
      target_config.min_cpu_ = std::max(1.0, cpu / 2);
      target_config.max_memory_ = sample_config.max_memory_ / factor;
      target_config.min_memory_ = sample_config.min_memory_ / factor;
      target_config.max_disk_size_ = sample_config.max_disk_size_ / factor;
      target_config.max_iops_ = sample_config.max_iops_ / factor;
      target_config.min_iops_ = target_config.max_iops_ / 2;
      target_config.max_session_num_ = INT64_MAX;
    } else {
      target_config.max_cpu_ = sample_config.max_cpu_;
      target_config.min_cpu_ = sample_config.max_cpu_;
      target_config.max_memory_ = sample_config.max_memory_;
      target_config.min_memory_ = sample_config.max_memory_;
      target_config.max_disk_size_ = sample_config.max_disk_size_;
      target_config.max_iops_ = sample_config.max_iops_;
      target_config.min_iops_ = sample_config.max_iops_;
      target_config.max_session_num_ = INT64_MAX;
    }
  }
  return ret;
}

ObPreBootstrap::ObPreBootstrap(ObSrvRpcProxy& rpc_proxy, const ObServerInfoList& rs_list,
    ObPartitionTableOperator& pt_operator, common::ObServerConfig& config, const ObBootstrapArg& arg,
    obrpc::ObCommonRpcProxy& rs_rpc_proxy)
    : ObBaseBootstrap(rpc_proxy, rs_list, config),
      stop_(false),
      leader_waiter_(pt_operator, stop_),
      begin_ts_(0),
      arg_(arg),
      common_proxy_(rs_rpc_proxy)
{}

int ObPreBootstrap::prepare_bootstrap(ObAddr& master_rs, int64_t& initial_frozen_version,
    int64_t& initial_schema_version, ObIArray<storage::ObFrozenStatus>& frozen_status,
    ObIArray<share::TenantIdAndSchemaVersion>& freeze_schemas)
{
  int ret = OB_SUCCESS;
  bool is_empty = false;
  bool match = false;
  begin_ts_ = ObTimeUtility::current_time();
  int64_t initial_frozen_ts = ObFreezeInfoManager::ORIGIN_FROZEN_VERSION;
  initial_frozen_version = ObFreezeInfoManager::ORIGIN_FROZEN_VERSION;
  initial_schema_version = ObFreezeInfoManager::ORIGIN_SCHEMA_VERSION;
  UNUSEDx(frozen_status, freeze_schemas);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(check_bootstrap_rs_list(rs_list_))) {
    LOG_WARN("failed to check_bootstrap_rs_list", K_(rs_list), K(ret));
  } else if (OB_FAIL(check_bootstrap_sys_tenant_primary_zone())) {
    LOG_WARN("fail to check bootstrap sys tenant primary zone", K(ret));
  } else if (OB_FAIL(check_all_server_bootstrap_mode_match(match))) {
    LOG_WARN("fail to check all server bootstrap mode match", K(ret));
  } else if (!match) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot do bootstrap with different bootstrap mode on servers", K(ret));
  } else if (OB_FAIL(check_is_all_server_empty(is_empty))) {
    LOG_WARN("failed to check bootstrap stat", K(ret));
  } else if (!is_empty) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot do bootstrap on not empty server", K(ret));
  } else if (OB_FAIL(notify_sys_tenant_server_unit_resource())) {
    LOG_WARN("fail to notify sys tenant server unit resource", K(ret));
  } else if (OB_FAIL(create_partition(initial_frozen_version, initial_frozen_ts))) {
    LOG_WARN("failed to create core table partition", K(ret));
  } else if (OB_FAIL(wait_elect_master_partition(master_rs))) {
    LOG_WARN("failed to wait elect master partition", K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBaseBootstrap::check_bootstrap_sys_tenant_primary_zone()
{
  int ret = OB_SUCCESS;
  common::ObZone zone;
  if (OB_FAIL(pick_sys_tenant_primary_zone(zone))) {
    LOG_WARN("fail to check bootstrap sys tenant primary zone", K(ret));
  }
  return ret;
}

int ObBaseBootstrap::get_zones_in_primary_region(
    const common::ObRegion& primary_region, common::ObIArray<common::ObZone>& zones_in_primary_region)
{
  int ret = OB_SUCCESS;
  zones_in_primary_region.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
    ObRegion this_region;
    if (rs_list_.at(i).region_.is_empty()) {
      this_region = DEFAULT_REGION_NAME;
    } else {
      this_region = rs_list_.at(i).region_;
    }
    if (this_region == primary_region) {
      if (OB_FAIL(zones_in_primary_region.push_back(rs_list_.at(i).zone_))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::pick_sys_tenant_primary_zone(common::ObZone& primary_zone)
{
  int ret = OB_SUCCESS;
  common::ObRegion dummy_region;
  if (OB_FAIL(pick_bootstrap_primary_zone_and_region(primary_zone, dummy_region))) {
    LOG_WARN("fail to pick bootstrap primary zone and region", K(ret));
  } else {
    LOG_INFO("succeed to pick sys tenant primary zone", K(ret), K(primary_zone));
  }
  return ret;
}

int ObBaseBootstrap::pick_gts_tenant_primary_region(common::ObRegion& primary_region)
{
  int ret = OB_SUCCESS;
  common::ObZone dummy_zone;
  if (OB_FAIL(pick_bootstrap_primary_zone_and_region(dummy_zone, primary_region))) {
    LOG_WARN("fail to pick bootstrap primary zone and region", K(ret));
  } else {
    LOG_INFO("succeed to pick gts tenant primary region", K(ret), K(primary_region));
  }
  return ret;
}

int ObBaseBootstrap::pick_bootstrap_primary_zone_and_region(
    common::ObZone& primary_zone, common::ObRegion& primary_region)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 2 * common::OB_MAX_MEMBER_NUMBER;  // the max replica num is 7
  common::hash::ObHashMap<common::ObRegion, int64_t, common::hash::NoPthreadDefendMode> region_cnt_map;
  if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("rs_list_ count unexpected", K(ret), "rs list count", rs_list_.count());
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "rootserver list is empty");
  } else if (1 == rs_list_.count()) {
    primary_zone = rs_list_.at(0).zone_;
    if (rs_list_.at(0).region_.is_empty()) {
      primary_region = DEFAULT_REGION_NAME;
    } else {
      primary_region = rs_list_.at(0).region_;
    }
  } else if (OB_FAIL(region_cnt_map.create(bucket_num, ObModIds::OB_HASH_BUCKET))) {
    LOG_WARN("fail to create region cnt map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      ObRegion region;
      if (rs_list_[i].region_.is_empty()) {
        if (OB_FAIL(region.assign(DEFAULT_REGION_NAME))) {
          LOG_WARN("fai lto assign default region info", K(ret));
        }
      } else {
        if (OB_FAIL(region.assign(rs_list_.at(i).region_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        int64_t cnt = 0;
        const int overwrite = 1;
        ret = region_cnt_map.get_refactored(region, cnt);
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(region_cnt_map.set_refactored(region, 1, overwrite))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        } else if (OB_SUCCESS == ret) {  // rewrite ret
          if (OB_FAIL(region_cnt_map.set_refactored(region, cnt + 1, overwrite))) {
            LOG_WARN("fail to set refactored", K(ret));
          }
        } else {
          LOG_WARN("fail to get from map", K(ret));
        }
      }
    }
    int64_t max_index = -1;
    int64_t max_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      ObRegion region;
      if (rs_list_[i].region_.is_empty()) {
        if (OB_FAIL(region.assign(DEFAULT_REGION_NAME))) {
          LOG_WARN("fai lto assign default region info", K(ret));
        }
      } else {
        if (OB_FAIL(region.assign(rs_list_.at(i).region_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        int64_t cnt = 0;
        if (OB_FAIL(region_cnt_map.get_refactored(region, cnt))) {
          LOG_WARN("fail to get refactored", K(ret));
        } else if (cnt <= 1) {
          // not the one we want
        } else if (cnt <= max_cnt) {
          // ignore the one not bigger than the current
        } else {
          max_index = i;
          max_cnt = cnt;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (max_index >= rs_list_.count() || max_index < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("no qualified primary zone for system", K(ret));
      LOG_USER_ERROR(OB_ERR_UNEXPECTED, "no qualified primary zone for system");
    } else {
      primary_zone = rs_list_[max_index].zone_;
      if (rs_list_[max_index].region_.is_empty()) {
        primary_region = DEFAULT_REGION_NAME;
      } else {
        primary_region = rs_list_[max_index].region_;
      }
    }
  }
  return ret;
}

int ObPreBootstrap::notify_sys_tenant_server_unit_resource()
{
  int ret = OB_SUCCESS;
  ObUnitConfig c;

  c.name_ = OB_SYS_UNIT_CONFIG_NAME;
  c.max_cpu_ = DEFAULT_MAX_SYS_CPU;
  c.max_memory_ = config_.get_max_sys_tenant_memory();
  c.min_memory_ = config_.get_min_sys_tenant_memory();
  c.max_disk_size_ = 512L * 1024L * 1024L * 1024L;  // dummy
  c.max_iops_ = 10000;
  c.max_session_num_ = INT64_MAX;

  ObUnitConfig unit_config;

  if (OB_FAIL(fill_sys_unit_config(c, unit_config))) {
    LOG_WARN("fail to fill sys unit config", K(ret));
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(rpc_proxy_, &ObSrvRpcProxy::notify_tenant_server_unit_resource);
    obrpc::TenantServerUnitConfig tenant_unit_server_config;
    tenant_unit_server_config.tenant_id_ = OB_SYS_TENANT_ID;
    tenant_unit_server_config.compat_mode_ = share::ObWorker::CompatMode::MYSQL;
    tenant_unit_server_config.unit_config_ = unit_config;
    tenant_unit_server_config.replica_type_ = ObReplicaType::REPLICA_TYPE_FULL;
    tenant_unit_server_config.if_not_grant_ = false;
    tenant_unit_server_config.is_delete_ = false;  // create new unit, set is_delete to false
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(notify_proxy.call(rs_list_[i].server_, rpc_timeout, tenant_unit_server_config))) {
        LOG_WARN("fail to call notify resource to server", K(ret), "dst", rs_list_[i].server_, K(rpc_timeout));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::create_partition(const int64_t initial_frozen_version, const int64_t initial_frozen_ts)
{
  int ret = OB_SUCCESS;
  ObCreatePartitionArg arg;
  common::ObZone primary_zone;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  ObTableSchema all_core_table_schema;
  ObArray<uint64_t> unit_ids;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_unit_ids(unit_ids))) {
    LOG_WARN("gen sys unit ids failed", K(ret));
  } else if (OB_FAIL(ObInnerTableSchema::all_core_table_schema(all_core_table_schema))) {
    LOG_WARN("fail to get all core table schema", K(ret));
  } else if (OB_FAIL(arg.table_schemas_.push_back(all_core_table_schema))) {
    LOG_WARN("fail to push back all core table schema", K(ret));
  } else if (OB_FAIL(pick_sys_tenant_primary_zone(primary_zone))) {
    LOG_WARN("fail to pick sys tenant primary zone", K(ret));
  } else {
    ObAddr leader;
    const int64_t frozen_version = initial_frozen_version;
    const int64_t memstore_version = frozen_version + 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      const int64_t now = ObTimeUtility::current_time();
      if (OB_FAIL(arg.member_list_.add_member(ObMember(rs_list_[i].server_, now)))) {
        LOG_WARN("failed to add server to member_list", "server", rs_list_[i].server_, K(now), K(ret));
      } else if (rs_list_[i].zone_ == primary_zone) {
        leader = rs_list_[i].server_;
      }
    }
    const int64_t now = ObTimeUtility::current_time();
    if (OB_FAIL(ret)) {
      // failed
    } else if (!leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("leader not found", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      arg.zone_ = rs_list_[i].zone_;
      // FIXME, to jiage, change to real schema_version
      arg.schema_version_ = 0;
      arg.memstore_version_ = memstore_version;  // frozen version + 1
      arg.replica_num_ = rs_list_.count();
      arg.leader_ = leader;
      arg.lease_start_ = now;
      arg.logonly_replica_num_ = 0;
      arg.backup_replica_num_ = 0;
      arg.readonly_replica_num_ = 0;
      arg.replica_type_ = common::REPLICA_TYPE_FULL;
      arg.last_submit_timestamp_ = now;
      arg.frozen_timestamp_ = initial_frozen_ts;
      if (OB_FAIL(arg.partition_key_.init(table_id,
              ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
              ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM))) {
        LOG_WARN("partition_key init failed",
            KT(table_id),
            K(ret),
            "partition_id",
            static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID),
            "partition_num",
            static_cast<int64_t>(ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM));
      } else {
        arg.pg_key_ = arg.partition_key_;
        LOG_INFO("start create partition", K(arg));
        if (OB_FAIL(ObPartitionCreator::create_partition_sync(
                rpc_proxy_, leader_waiter_.get_pt_operator(), rs_list_[i].server_, arg))) {
          LOG_WARN("failed to create partition", "server", rs_list_[i].server_, K(arg), K(ret));
        }
        LOG_INFO("finish create partition", K(arg));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to create partition", K(table_id));
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

int ObPreBootstrap::check_all_server_bootstrap_mode_match(bool& match)
{
  int ret = OB_SUCCESS;
  match = true;
  Bool is_match(false);

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else {
    ObCheckDeploymentModeArg arg;
    arg.single_zone_deployment_on_ = false;
    for (int64_t i = 0; OB_SUCC(ret) && match && i < rs_list_.count(); ++i) {
      if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_).check_deployment_mode_match(arg, is_match))) {
        LOG_WARN("fail to check deployment mode match", K(ret));
      } else if (!is_match) {
        LOG_WARN("server deployment mode not match", "server", rs_list_[i].server_);
        match = false;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

int ObPreBootstrap::check_is_all_server_empty(bool& is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  Bool is_server_empty;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObCheckServerEmptyArg arg;
    arg.mode_ = ObCheckServerEmptyArg::BOOTSTRAP;
    for (int64_t i = 0; OB_SUCC(ret) && is_empty && i < rs_list_.count(); ++i) {
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_).timeout(rpc_timeout).is_empty_server(arg, is_server_empty))) {
        LOG_WARN("failed to check if server is empty", "server", rs_list_[i].server_, K(rpc_timeout), K(ret));
      } else if (!is_server_empty) {
        // don't need to set ret
        LOG_WARN("server is not empty", "server", rs_list_[i].server_);
        is_empty = false;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

int ObPreBootstrap::wait_elect_master_partition(ObAddr& master_rs)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID);
  const int64_t partition_id = 0;
  int64_t rpc_timeout = WAIT_ELECT_SYS_LEADER_TIMEOUT_US;
  if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
    rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
  }
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(leader_waiter_.wait(table_id, partition_id, rpc_timeout, master_rs))) {
    LOG_WARN("leader_waiter_ wait failed", KT(table_id), K(partition_id), K(rpc_timeout), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

bool ObBootstrap::TableIdCompare::operator()(const ObTableSchema* left, const ObTableSchema* right)
{
  bool bret = false;

  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(ret), KP(left), KP(right));
  } else {
    bool left_is_sys_index = left->is_index_table() && is_sys_table(left->get_table_id());
    bool right_is_sys_index = right->is_index_table() && is_sys_table(right->get_table_id());
    uint64_t left_table_id = left->get_table_id();
    uint64_t left_data_table_id = left->get_data_table_id();
    uint64_t right_table_id = right->get_table_id();
    uint64_t right_data_table_id = right->get_data_table_id();
    if (!left_is_sys_index && !right_is_sys_index) {
      bret = left_table_id < right_table_id;
    } else if (left_is_sys_index && right_is_sys_index) {
      bret = left_data_table_id < right_data_table_id;
    } else if (left_is_sys_index) {
      if (left_data_table_id == right_table_id) {
        bret = true;
      } else {
        bret = left_data_table_id < right_table_id;
      }
    } else {
      if (left_table_id == right_data_table_id) {
        bret = false;
      } else {
        bret = left_table_id < right_data_table_id;
      }
    }
  }
  return bret;
}

ObBootstrap::ObBootstrap(ObSrvRpcProxy& rpc_proxy, ObDDLService& ddl_service, ObUnitManager& unit_mgr,
    ObILeaderCoordinator& leader_coordinator, ObServerConfig& config, const obrpc::ObBootstrapArg& arg,
    ObRsGtsManager& rs_gts_manager, obrpc::ObCommonRpcProxy& rs_rpc_proxy)
    : ObBaseBootstrap(rpc_proxy, arg.server_list_, config),
      ddl_service_(ddl_service),
      unit_mgr_(unit_mgr),
      leader_coordinator_(leader_coordinator),
      arg_(arg),
      rs_gts_manager_(rs_gts_manager),
      common_proxy_(rs_rpc_proxy),
      begin_ts_(0)
{}

int ObBootstrap::execute_bootstrap()
{
  int ret = OB_SUCCESS;
  bool already_bootstrap = true;
  uint64_t server_id = OB_INIT_SERVER_ID;
  ObSArray<ObTableSchema> table_schemas;
  ObSArray<ObTableSchema> sorted_table_schemas;
  begin_ts_ = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(check_is_already_bootstrap(already_bootstrap))) {
    LOG_WARN("failed to check_is_already_bootstrap", K(ret));
  } else if (already_bootstrap) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ob system is already bootstrap, cannot bootstrap again", K(ret));
  } else if (OB_FAIL(check_bootstrap_rs_list(rs_list_))) {
    LOG_WARN("failed to check_bootstrap_rs_list", K_(rs_list), K(ret));
  } else if (OB_FAIL(check_bootstrap_sys_tenant_primary_zone())) {
    LOG_WARN("fail to check sys tenant primary zone", K(ret));
  } else if (OB_FAIL(add_rs_list(server_id))) {
    LOG_WARN("failed to add rs list to server manager", K(ret));
  } else if (OB_FAIL(wait_all_rs_online())) {
    LOG_WARN("failed to wait all rs online", K(ret));
  } else if (OB_FAIL(set_in_bootstrap())) {
    LOG_WARN("failed to set in bootstrap", K(ret));
  } else if (OB_FAIL(init_global_stat())) {
    LOG_WARN("failed to init_global_stat", K(ret));
  } else if (OB_FAIL(insert_first_freeze_info())) {
    LOG_WARN("failed to insert_first_freeze_info", K(ret));
  } else if (OB_FAIL(ddl_service_.get_freeze_info_mgr().reload())) {
    LOG_WARN("fail to reload freeze info manager", KR(ret));
  } else if (OB_FAIL(construct_all_schema(table_schemas))) {
    LOG_WARN("construct all schema fail", K(ret));
  } else if (OB_FAIL(sort_schema(table_schemas, sorted_table_schemas))) {
    LOG_WARN("fail to sort table schemas", KR(ret));
  } else if (OB_FAIL(broadcast_sys_schema(sorted_table_schemas))) {
    LOG_WARN("broadcast_sys_schemas failed", K(sorted_table_schemas), K(ret));
  } else if (OB_FAIL(create_all_partitions())) {
    LOG_WARN("create all partitions fail", K(ret));
  } else if (OB_FAIL(create_all_schema(ddl_service_, sorted_table_schemas))) {
    LOG_WARN("create_all_schema failed", K(sorted_table_schemas), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("create_all_schema");
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService& schema_service = ddl_service_.get_schema_service();

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_system_data(server_id))) {
    LOG_WARN("failed to init system data", K(server_id), K(ret));
  } else if (OB_FAIL(ddl_service_.refresh_schema(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to refresh_schema", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(check_schema_version(schema_guard, sorted_table_schemas, false /*need_update*/))) {
    LOG_WARN("fail to check schema version", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("refresh_schema");

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(wait_all_rs_in_service())) {
    LOG_WARN("wait_all_rs_in_service failed", K(ret));
  } else if (GCONF._enable_ha_gts_full_service && OB_FAIL(init_gts_service_data())) {
    LOG_WARN("fail to init gts service data", K(ret));
  } else if (OB_FAIL(init_backup_inner_table())) {
    LOG_WARN("failed tro init backup inner table", K(ret));
  } else {
    ROOTSERVICE_EVENT_ADD("bootstrap", "bootstrap_succeed");
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::sort_schema(
    const ObIArray<ObTableSchema>& table_schemas, ObIArray<ObTableSchema>& sorted_table_schemas)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObTableSchema*> ptr_table_schemas;
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "count", table_schemas.count());
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ptr_table_schemas.push_back(&table_schemas.at(i)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      TableIdCompare compare;
      std::sort(ptr_table_schemas.begin(), ptr_table_schemas.end(), compare);
      if (OB_FAIL(compare.get_ret())) {
        LOG_WARN("fail to sort schema", KR(ret));
      } else {
        for (int64_t i = 0; i < ptr_table_schemas.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(sorted_table_schemas.push_back(*ptr_table_schemas.at(i)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBootstrap::prepare_create_partition(
    ObPartitionCreator& creator, const share::schema_create_func func, ObTableSchema& tschema)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  const bool set_primary_zone = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (NULL == func) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func is null", K(ret));
  } else if (OB_FAIL(func(tschema))) {
    LOG_WARN("failed to create table schema", K(ret));
  } else if (tschema.has_self_partition()) {
    if (OB_FAIL(gen_sys_units(units))) {
      LOG_WARN("gen_sys_units failed", K(ret));
    }
    BOOTSTRAP_CHECK_SUCCESS_V2("gen_sys_unit");
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_replica_options(set_primary_zone, tschema))) {
      LOG_WARN("set replica options failed", K(set_primary_zone), K(ret));
    } else {
      const bool is_standby = common::STANDBY_CLUSTER == arg_.cluster_type_;
      tschema.set_table_id(combine_id(OB_SYS_TENANT_ID, tschema.get_table_id()));
      const ObAddr leader = ddl_service_.get_server_manager().get_rs_addr();
      const int64_t frozen_version = arg_.initial_frozen_version_;
      if (OB_FAIL(ddl_service_.prepare_create_partition(
              creator, tschema, rs_list_.count(), units, leader, frozen_version, is_standby))) {
        LOG_WARN("failed prepare create partition",
            "table_id",
            tschema.get_table_id(),
            "table_name",
            tschema.get_table_name(),
            K(units),
            K(leader),
            K(frozen_version),
            K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed prepare create table partition",
        "table_id",
        tschema.get_table_id(),
        "table_name",
        tschema.get_table_name(),
        "frozen_version",
        arg_.initial_frozen_version_,
        "cluster_type",
        cluster_type_to_str(arg_.cluster_type_));

    if (OB_ALL_ROOT_TABLE_TID == extract_pure_id(tschema.get_table_id())) {
      ObArray<uint64_t> tids;
      ObArray<int64_t> nums;
      if (OB_FAIL(creator.execute())) {
        LOG_WARN("execute create partition failed", K(ret));
      } else {
        creator.reuse();
      }
      LOG_INFO("wait all_root_table partition leader election");
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tids.push_back(tschema.get_table_id()))) {
        LOG_WARN("add table id to array fail", K(ret));
      } else if (OB_FAIL(nums.push_back(tschema.get_all_part_num()))) {
        LOG_WARN("add replica num to array fail", K(ret));
      } else if (OB_FAIL(ddl_service_.wait_elect_sys_leaders(tids, nums))) {
        LOG_WARN("wait_elect_sys_leaders failed", K(tids), K(nums), K(ret));
      }
      LOG_INFO("wait all_root_table partition leader election done", K(ret));
    }
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_partitions()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> sys_table_ids;
  ObArray<int64_t> partition_nums;
  ObTableSchema table_schema;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObPartitionCreator creator(rpc_proxy_, ddl_service_.get_pt_operator(), &ddl_service_.get_server_manager());

    // create core table partition
    for (int64_t i = 0; OB_SUCC(ret) && NULL != core_table_schema_creators[i]; ++i) {
      table_schema.reset();
      if (OB_FAIL(prepare_create_partition(creator, core_table_schema_creators[i], table_schema))) {
        LOG_WARN("prepare create partition fail", K(ret));
      } else if (OB_FAIL(sys_table_ids.push_back(table_schema.get_table_id()))) {
        LOG_WARN("failed to add table id", K(ret));
      } else if (OB_FAIL(partition_nums.push_back(table_schema.get_all_part_num()))) {
        LOG_WARN("failed to add part num", K(ret));
      }
    }

    // create sys table partition
    for (int64_t i = 0; OB_SUCC(ret) && NULL != sys_table_schema_creators[i]; ++i) {
      table_schema.reset();
      if (OB_FAIL(prepare_create_partition(creator, sys_table_schema_creators[i], table_schema))) {
        LOG_WARN("prepare create partition fail", K(ret));
      } else if (table_schema.has_self_partition()) {
        if (OB_FAIL(sys_table_ids.push_back(table_schema.get_table_id()))) {
          LOG_WARN("failed to add table id", K(ret));
        } else if (OB_FAIL(partition_nums.push_back(table_schema.get_all_part_num()))) {
          LOG_WARN("failed to add part num", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(creator.execute())) {
        LOG_WARN("execute create partition failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_service_.wait_elect_sys_leaders(sys_table_ids, partition_nums))) {
        LOG_WARN("wait_elect_sys_leaders failed", K(sys_table_ids), K(partition_nums), K(ret));
      } else {
        LOG_INFO("wait_elect_sys_leaders succeed");
      }
    }
  }

  LOG_INFO("finish creating system tables", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::construct_all_schema(ObIArray<ObTableSchema>& table_schemas)
{
  int ret = OB_SUCCESS;
  if (PRIMARY_CLUSTER == arg_.cluster_type_) {
    if (OB_FAIL(primary_construct_all_schema(table_schemas))) {
      LOG_WARN("fail to primary construct all schema", KR(ret));
    }
  } else {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("get invalid cluster type", KR(ret));
  }
  return ret;
}

int ObBootstrap::primary_construct_all_schema(ObIArray<ObTableSchema>& table_schemas)
{
  int ret = OB_SUCCESS;
  const schema_create_func* creator_ptr_arrays[] = {
      core_table_schema_creators, sys_table_schema_creators, virtual_table_schema_creators, sys_view_schema_creators};

  ObTableSchema table_schema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(table_schemas.reserve(OB_SYS_TABLE_COUNT))) {
    LOG_WARN("reserve failed", "capacity", OB_SYS_TABLE_COUNT, K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func* creator_ptr = creator_ptr_arrays[i]; OB_SUCCESS == ret && NULL != *creator_ptr;
           ++creator_ptr) {
        table_schema.reset();
        if (OB_FAIL(construct_schema(*creator_ptr, table_schema))) {
          LOG_WARN("construct_schema failed", K(table_schema), K(ret));
        } else if (OB_ALL_TABLE_HISTORY_TID == extract_pure_id(table_schema.get_table_id())) {
          // To ensure the index_tid_array of data table is valid, the index of __all_table_history needs special
          // handling construct schema using the schema_version of data table, it can see the index schema
          ObTableSchema index_schema;
          if (OB_FAIL(ObInnerTableSchema::all_table_history_idx_data_table_id_schema(index_schema))) {
            LOG_WARN("fail to create index schema", K(ret), K(table_schema));
          } else if (OB_FAIL(table_schema.add_simple_index_info(ObAuxTableMetaInfo(index_schema.get_table_id(),
                         index_schema.get_table_type(),
                         index_schema.get_drop_schema_version())))) {
            LOG_WARN("fail to add simple_index_info", K(ret), K(table_schema));
          } else if (OB_FAIL(table_schemas.push_back(index_schema))) {
            LOG_WARN("push_back failed", K(ret), K(index_schema));
          } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
            LOG_WARN("push_back failed", K(ret), K(table_schema));
          } else {
            LOG_INFO("create sys index schema", K(ret), K(table_schema));
          }
        } else if (OB_ALL_TABLE_V2_HISTORY_TID == extract_pure_id(table_schema.get_table_id())) {
          // To ensure the index_tid_array of data table is valid, the index of __all_table_v2_history needs special
          // handling construct schema using the schema_version of data table, it can see the index schema
          ObTableSchema index_schema;
          if (OB_FAIL(ObInnerTableSchema::all_table_v2_history_idx_data_table_id_schema(index_schema))) {
            LOG_WARN("fail to create index schema", K(ret), K(table_schema));
          } else if (OB_FAIL(table_schema.add_simple_index_info(ObAuxTableMetaInfo(index_schema.get_table_id(),
                         index_schema.get_table_type(),
                         index_schema.get_drop_schema_version())))) {
            LOG_WARN("fail to add index", K(ret), K(table_schema));
          } else if (OB_FAIL(table_schemas.push_back(index_schema))) {
            LOG_WARN("push_back failed", K(ret), K(index_schema));
          } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
            LOG_WARN("push_back failed", K(ret), K(table_schema));
          } else {
            LOG_INFO("create sys index schema", K(ret), K(table_schema));
          }
        } else if (OB_ALL_BACKUP_PIECE_FILES_TID == extract_pure_id(table_schema.get_table_id())) {
          // index for __all_backup_piece_files
          ObTableSchema index_schema;
          if (OB_FAIL(ObInnerTableSchema::all_backup_piece_files_idx_data_table_id_schema(index_schema))) {
            LOG_WARN("fail to create index schema", K(ret), K(table_schema));
          } else if (OB_FAIL(table_schema.add_simple_index_info(ObAuxTableMetaInfo(index_schema.get_table_id(),
                         index_schema.get_table_type(),
                         index_schema.get_drop_schema_version())))) {
            LOG_WARN("fail to add simple_index_info", K(ret), K(table_schema));
          } else if (OB_FAIL(table_schemas.push_back(index_schema))) {
            LOG_WARN("push_back failed", K(ret), K(index_schema));
          } else if (OB_FAIL(table_schemas.push_back(table_schema))) {
            LOG_WARN("push_back failed", K(ret), K(table_schema));
          } else {
            LOG_INFO("create sys index schema", K(ret), K(table_schema));
          }
        } else {
          if (OB_FAIL(table_schemas.push_back(table_schema))) {
            LOG_WARN("push_back failed", K(ret), K(table_schema));
          }
        }
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::broadcast_sys_schema(const ObSArray<ObTableSchema>& table_schemas)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", K(table_schemas), K(ret));
  } else {
    FOREACH_CNT_X(rs, rs_list_, OB_SUCCESS == ret)
    {
      bool is_active = false;
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(ddl_service_.get_server_manager().check_server_active(rs->server_, is_active))) {
        LOG_WARN("check_server_active failed", "server", rs->server_, K(ret));
      } else if (!is_active) {
        ret = OB_SERVER_NOT_ACTIVE;
        LOG_WARN("server not active", "server", rs->server_, K(ret));
      } else if (OB_FAIL(rpc_proxy_.to(rs->server_).timeout(rpc_timeout).broadcast_sys_schema(table_schemas))) {
        LOG_WARN("broadcast_sys_schema failed", K(rpc_timeout), "server", rs->server_, K(ret));
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_schema(ObDDLService& ddl_service, ObIArray<ObTableSchema>& table_schemas)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("start create all schemas", "table count", table_schemas.count());
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", K(table_schemas), K(ret));
  } else {
    int64_t begin = 0;
    int64_t batch_count = BATCH_INSERT_SCHEMA_CNT;
    const int64_t MAX_RETRY_TIMES = 3;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      if (table_schemas.count() == (i + 1) || (i + 1 - begin) >= batch_count) {
        int64_t retry_times = 1;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(batch_create_schema(ddl_service, table_schemas, begin, i + 1))) {
            LOG_WARN("batch create schema failed", K(ret), "table count", i + 1 - begin);
            // bugfix:https://work.aone.alibaba-inc.com/issue/34030283
            if ((OB_SCHEMA_EAGAIN == ret || OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret) &&
                retry_times <= MAX_RETRY_TIMES) {
              retry_times++;
              ret = OB_SUCCESS;
              LOG_INFO("schema error while create table, need retry", KR(ret), K(retry_times));
              usleep(1 * 1000 * 1000L);  // 1s
            }
          } else {
            break;
          }
        }
        if (OB_SUCC(ret)) {
          begin = i + 1;
        }
      }
    }
  }
  LOG_INFO("end create all schemas",
      K(ret),
      "table count",
      table_schemas.count(),
      "time_used",
      ObTimeUtility::current_time() - begin_time);
  return ret;
}

int ObBootstrap::batch_create_schema(
    ObDDLService& ddl_service, ObIArray<ObTableSchema>& table_schemas, const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObDDLSQLTransaction trans(&(ddl_service.get_schema_service()));
  if (begin < 0 || begin >= end || end > table_schemas.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin), K(end), "table count", table_schemas.count());
  } else {
    ObDDLOperator ddl_operator(ddl_service.get_schema_service(), ddl_service.get_sql_proxy());
    if (OB_FAIL(trans.start(&ddl_service.get_sql_proxy()))) {
      LOG_WARN("start transaction failed", K(ret));
    } else {
      bool is_truncate_table = false;
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        ObTableSchema& table = table_schemas.at(i);
        const ObString* ddl_stmt = NULL;
        bool need_sync_schema_version =
            extract_pure_id(table.get_table_id()) != OB_ALL_TABLE_HISTORY_IDX_DATA_TABLE_ID_TID &&
            extract_pure_id(table.get_table_id()) != OB_ALL_TABLE_V2_HISTORY_IDX_DATA_TABLE_ID_TID &&
            extract_pure_id(table.get_table_id()) != OB_ALL_BACKUP_PIECE_FILES_IDX_DATA_TABLE_ID_TID;
        if (OB_FAIL(ddl_operator.create_table(table, trans, ddl_stmt, need_sync_schema_version, is_truncate_table))) {
          LOG_WARN("add table schema failed",
              K(ret),
              "table_id",
              table.get_table_id(),
              "table_name",
              table.get_table_name());
        } else {
          LOG_INFO(
              "add table schema succeed", K(i), "table_id", table.get_table_id(), "table_name", table.get_table_name());
        }
      }
    }
  }

  const int64_t begin_commit_time = ObTimeUtility::current_time();
  if (trans.is_started()) {
    const bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end trans failed", K(tmp_ret), K(is_commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  const int64_t now = ObTimeUtility::current_time();
  LOG_INFO("batch create schema finish",
      K(ret),
      "table count",
      end - begin,
      "total_time_used",
      now - begin_time,
      "end_transaction_time_used",
      now - begin_commit_time);
  // BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::construct_schema(const share::schema_create_func func, ObTableSchema& tschema)
{
  int ret = OB_SUCCESS;
  const bool set_primary_zone = false;
  BOOTSTRAP_CHECK_SUCCESS_V2("before construct schema");
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (NULL == func) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func is null", K(ret));
  } else if (OB_FAIL(func(tschema))) {
    LOG_WARN("failed to create table schema", K(ret));
  } else if (OB_FAIL(set_replica_options(set_primary_zone, tschema))) {
    LOG_WARN("set replica options failed", K(set_primary_zone), K(ret));
  } else {
  }  // no more to do
  return ret;
}

int ObBootstrap::add_rs_list(uint64_t& server_id)
{
  int ret = OB_SUCCESS;
  ObServerManager& server_mgr = ddl_service_.get_server_manager();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(server_mgr.add_server_list(rs_list_, server_id))) {
    LOG_WARN("add_server_list failed", K_(rs_list), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::wait_all_rs_online()
{
  int ret = OB_SUCCESS;
  int64_t left_time_can_sleep = 0;
  ObServerManager& server_mgr = ddl_service_.get_server_manager();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(server_mgr.get_lease_duration(left_time_can_sleep))) {
    LOG_WARN("get_lease_duration failed", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      bool all_alive = true;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        left_time_can_sleep = max(left_time_can_sleep, THIS_WORKER.get_timeout_remain());
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
        bool is_alive = false;
        if (OB_FAIL(server_mgr.check_server_alive(rs_list_.at(i).server_, is_alive))) {
          LOG_WARN("check_server_alive failed", "server", rs_list_.at(i).server_, K(ret));
        } else if (!is_alive) {
          LOG_WARN("server is not alive", "server", rs_list_.at(i).server_, K(is_alive));
          all_alive = false;
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (all_alive) {
        break;
      } else if (left_time_can_sleep > 0) {
        const int64_t time_to_sleep = min(HEAT_BEAT_INTERVAL_US, left_time_can_sleep);
        usleep(static_cast<uint32_t>(time_to_sleep));
        left_time_can_sleep -= time_to_sleep;
      } else {
        ret = OB_WAIT_ALL_RS_ONLINE_TIMEOUT;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::wait_all_rs_in_service()
{
  int ret = OB_SUCCESS;
  const int64_t check_interval = 500 * 1000;
  int64_t left_time_can_sleep = WAIT_RS_IN_SERVICE_TIMEOUT_US;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  }
  while (OB_SUCC(ret)) {
    bool all_in_service = true;
    FOREACH_CNT_X(rs, rs_list_, all_in_service && OB_SUCCESS == ret)
    {
      bool in_service = false;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        left_time_can_sleep = max(left_time_can_sleep, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(ddl_service_.get_server_manager().check_in_service(rs->server_, in_service))) {
        LOG_WARN("check_in_service failed", "server", rs->server_, K(ret));
      } else if (!in_service) {
        LOG_WARN("server is not in_service ", "server", rs->server_);
        all_in_service = false;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (all_in_service) {
      break;
    } else if (left_time_can_sleep > 0) {
      const int64_t time_to_sleep = min(check_interval, left_time_can_sleep);
      LOG_WARN("fail to wait all rs in service. wait a while", K(time_to_sleep), K(left_time_can_sleep));
      usleep(static_cast<uint32_t>(time_to_sleep));
      left_time_can_sleep -= time_to_sleep;
    } else {
      ret = OB_WAIT_ALL_RS_ONLINE_TIMEOUT;
      LOG_WARN(
          "wait all rs in service timeout", "timeout", static_cast<int64_t>(WAIT_RS_IN_SERVICE_TIMEOUT_US), K(ret));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::insert_first_freeze_info()
{
  int ret = OB_SUCCESS;
  ObFreezeInfoProxy freeze_info_proxy;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  storage::ObFrozenStatus frozen_status;
  ObMySQLTransaction trans;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("trans start failed", K(ret));
  } else if (common::PRIMARY_CLUSTER == arg_.cluster_type_) {
    frozen_status.schema_version_ = 1;
    frozen_status.frozen_version_ = arg_.initial_frozen_version_;
    frozen_status.frozen_timestamp_ = 1;
    frozen_status.cluster_version_ = GET_MIN_CLUSTER_VERSION();
    if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, frozen_status))) {
      LOG_WARN("set frozen_status failed", K(frozen_status), K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg_.frozen_status_.count(); ++i) {
      if (arg_.initial_frozen_version_ >= arg_.frozen_status_.at(i).frozen_version_) {
        // if freeze_info is larger than start_freeze_version, no need set freeze info
        if (OB_FAIL(freeze_info_proxy.set_freeze_info(trans, arg_.frozen_status_.at(i)))) {
          LOG_WARN("failed to set frozen status", KR(ret), K(i), K_(arg));
        }
      }
    }
  }
  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("trans end failed", "commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }
  }
  return ret;
}

int ObBootstrap::insert_first_freeze_schema()
{
  int ret = OB_SUCCESS;
  ObFreezeInfoProxy freeze_info_proxy;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  ObSEArray<TenantIdAndSchemaVersion, 1> id_versions;
  TenantIdAndSchemaVersion schema_info;
  schema_info.schema_version_ = 1;
  schema_info.tenant_id_ = 1;
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (ObFreezeInfoManager::ORIGIN_FROZEN_VERSION == arg_.initial_frozen_version_) {
    ObSEArray<TenantIdAndSchemaVersion, 1> id_versions;
    TenantIdAndSchemaVersion schema_info;
    schema_info.schema_version_ = 1;
    schema_info.tenant_id_ = 1;
    if (OB_FAIL(id_versions.push_back(schema_info))) {
      LOG_WARN("fail to push back", KR(ret), K(schema_info));
    } else if (OB_FAIL(
                   freeze_info_proxy.update_frozen_schema_v2(sql_proxy, arg_.initial_frozen_version_, id_versions))) {
      LOG_WARN("fail to update frozen schema version", KR(ret));
    } else {
      LOG_INFO(
          "insert_first_freeze_info finish", K(ret), K(id_versions), "frozen_version", arg_.initial_frozen_version_);
    }
  }
  return ret;
}

int ObBootstrap::check_is_already_bootstrap(bool& is_bootstrap)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  is_bootstrap = true;
  ObMultiVersionSchemaService& schema_service = ddl_service_.get_schema_service();
  ObSchemaGetterGuard guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("get_schema_manager failed", K(ret));
  } else if (OB_FAIL(guard.get_schema_version(OB_SYS_TENANT_ID, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (OB_CORE_SCHEMA_VERSION == schema_version) {
    is_bootstrap = false;
  } else {
    // don't need to set ret
    // LOG_WARN("observer is bootstrap already", "schema_table_count", guard->get_table_count());
    LOG_WARN("observer is already bootstrap");
    // const bool is_verbose = false;
    // guard->print_info(is_verbose);
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_global_stat()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("trans start failed", K(ret));
  } else {
    const int64_t frozen_version = arg_.initial_frozen_version_;
    const int64_t baseline_schema_version = -1;
    const int64_t rootservice_epoch = 0;
    const int64_t split_schema_version = GCONF.__schema_split_mode ? 0 : OB_INVALID_VERSION;
    // FIXME:() it is a sign that sys tenant start to use __all_table_v2/__all_table_history_v2
    // if split_schema_version_v2 < 0, it is invalid
    const int64_t split_schema_version_v2 = 0;
    const int64_t snapshot_gc_ts = 0;
    const int64_t snapshot_gc_timestamp = 0;
    const int64_t next_schema_version = OB_INVALID_VERSION;
    share::schema::ObRefreshSchemaStatus schema_status;
    schema_status.tenant_id_ = OB_SYS_TENANT_ID;
    schema_status.readable_schema_version_ = OB_INVALID_VERSION;
    schema_status.snapshot_timestamp_ = OB_INVALID_TIMESTAMP;
    schema_status.created_schema_version_ = OB_INVALID_VERSION;
    ObGlobalStatProxy global_stat_proxy(trans);
    ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", K(ret));
    } else if (OB_FAIL(global_stat_proxy.set_init_value(OB_CORE_SCHEMA_VERSION,
                   baseline_schema_version,
                   frozen_version,
                   rootservice_epoch,
                   split_schema_version,
                   split_schema_version_v2,
                   snapshot_gc_ts,
                   snapshot_gc_timestamp,
                   next_schema_version))) {
      LOG_WARN("set_init_value failed",
          K(ret),
          "schema_version",
          OB_CORE_SCHEMA_VERSION,
          K(baseline_schema_version),
          K(frozen_version),
          K(rootservice_epoch),
          K(split_schema_version),
          K(split_schema_version_v2),
          K(next_schema_version));
    }

    if (OB_FAIL(ret)) {
    } else if (PRIMARY_CLUSTER == arg_.cluster_type_) {
      if (OB_FAIL(global_stat_proxy.set_schema_snapshot_version(0, 0))) {
        LOG_WARN("fail to set schema snapshot version", KR(ret));
      }
    } else {
      ret = OB_INNER_STAT_ERROR;
      LOG_WARN("get invalid cluster type", KR(ret), K_(arg));
    }
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("trans end failed", "commit", OB_SUCCESS == ret, K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }

    // Initializes a new state of refresh schema
    if (OB_SUCC(ret)) {
      if (OB_FAIL(schema_status_proxy->set_refresh_schema_status(schema_status))) {
        LOG_WARN("fail to init schema status", K(ret));
      } else if (OB_FAIL(init_sequence_id())) {
        LOG_WARN("failed to init_sequence_id", K(ret));
      } else {
        (void)GCTX.set_split_schema_version(split_schema_version);
        (void)GCTX.set_split_schema_version_v2(split_schema_version_v2);
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_sequence_id()
{
  int ret = OB_SUCCESS;
  const int64_t rootservice_epoch = 0;
  ObMultiVersionSchemaService& multi_schema_service = ddl_service_.get_schema_service();
  ObSchemaService* schema_service = multi_schema_service.get_schema_service();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service->init_sequence_id(rootservice_epoch))) {
    LOG_WARN("init sequence id failed", K(ret), K(rootservice_epoch));
  }
  return ret;
}

int ObBootstrap::gen_multiple_zone_deployment_sys_tenant_locality_str(share::schema::ObTenantSchema& tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone list count unexpected", K(ret));
  } else {
    const int64_t BUFF_SIZE = 256;  // 256 is enough for sys tenant
    char locality_str[BUFF_SIZE] = "";
    bool first = true;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(databuff_printf(
              locality_str, BUFF_SIZE, pos, "%sF{1}@%s", first ? "" : ", ", rs_list_.at(i).zone_.ptr()))) {
        LOG_WARN("fail to do databuff printf", K(ret));
      } else {
        first = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_locality(locality_str))) {
        LOG_WARN("fail to set locality", K(ret));
      }
    }
  }
  return ret;
}

int ObBootstrap::gen_sys_tenant_locality_str(share::schema::ObTenantSchema& tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    if (OB_FAIL(gen_multiple_zone_deployment_sys_tenant_locality_str(tenant_schema))) {
      LOG_WARN("fail to gen multiple zone deployment sys tenant locality str", K(ret));
    }
  }
  return ret;
}

int ObBootstrap::create_sys_tenant()
{
  // insert zero system stat value for create system tenant.
  int ret = OB_SUCCESS;
  ObTenantSchema tenant;
  const bool set_primary_zone = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    obrpc::ObCreateTenantArg arg;
    arg.name_case_mode_ = OB_ORIGIN_AND_INSENSITIVE;
    tenant.set_tenant_id(OB_SYS_TENANT_ID);
    tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

    share::schema::ObSchemaGetterGuard dummy_schema_guard;
    ObArray<common::ObZone> zone_list;
    ObArray<share::schema::ObZoneRegion> zone_region_list;
    char locality_str[MAX_LOCALITY_LENGTH + 1];
    int64_t pos = 0;
    ObLocalityDistribution locality_dist;
    common::ObArray<share::ObZoneReplicaAttrSet> zone_replica_num_array;
    if (OB_FAIL(gen_sys_tenant_locality_str(tenant))) {
      LOG_WARN("fail to gen sys tenant locality str", K(ret));
    } else if (OB_FAIL(gen_sys_zone_list(zone_list))) {
      LOG_WARN("fail to gen sys zone list", K(ret));
    } else if (OB_FAIL(build_zone_region_list(zone_region_list))) {
      LOG_WARN("fail to build zone region list", K(ret));
    } else if (OB_FAIL(locality_dist.init())) {
      LOG_WARN("fail to init locality distribution", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(tenant.get_locality_str(), zone_list, &zone_region_list))) {
      LOG_WARN("fail to parse tenant schema locality", K(ret));
    } else if (OB_FAIL(locality_dist.output_normalized_locality(locality_str, MAX_LOCALITY_LENGTH, pos))) {
      LOG_WARN("fail to output normalized locality", K(ret));
    } else if (OB_FAIL(tenant.set_locality(locality_str))) {
      LOG_WARN("fail to set locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_num_array))) {
      LOG_WARN("fail to get zone region replica num array", K(ret));
    } else if (OB_FAIL(tenant.set_zone_replica_attr_array(zone_replica_num_array))) {
      LOG_WARN("fail to set zone replica_num array", K(ret));
    } else if (OB_FAIL(tenant.set_tenant_name(OB_SYS_TENANT_NAME))) {
      LOG_WARN("set_tenant_name failed", "tenant_name", OB_SYS_TENANT_NAME, K(ret));
    } else if (OB_FAIL(tenant.set_comment("system tenant"))) {
      LOG_WARN("set_comment failed", "comment", "system tenant", K(ret));
    } else if (OB_FAIL(set_replica_options(set_primary_zone, tenant))) {
      LOG_WARN("set replica options failed", K(set_primary_zone), K(ret));
    } else if (OB_FAIL(ddl_service_.check_primary_zone_locality_condition(
                   tenant, zone_list, zone_region_list, dummy_schema_guard))) {
      LOG_WARN("fail to check primary zone region condition", K(ret));
    } else if (OB_FAIL(ddl_service_.create_sys_tenant(arg, tenant))) {
      LOG_WARN("create tenant failed", K(ret), K(tenant));
    } else {
    }  // no more to do
  }

  LOG_INFO("create tenant", K(ret), K(tenant));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_system_data(const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_INVALID_ID == server_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_id", K(server_id), K(ret));
  } else if (OB_FAIL(unit_mgr_.load())) {
    LOG_WARN("unit_mgr load failed", K(ret));
  } else if (OB_FAIL(create_sys_unit_config())) {
    LOG_WARN("create_sys_unit_config failed", K(ret));
  } else if (OB_FAIL(create_sys_resource_pool())) {
    LOG_WARN("create sys resource pool failed", K(ret));
  } else if (OB_FAIL(create_sys_tenant())) {
    LOG_WARN("create system tenant failed", K(ret));
  } else if (OB_FAIL(init_server_id(server_id))) {
    LOG_WARN("init server id failed", K(server_id), K(ret));
  } else if (OB_FAIL(init_all_zone_table())) {
    LOG_WARN("failed to init all zone table", K(ret));
  } else if (OB_FAIL(insert_first_freeze_schema())) {
    LOG_WARN("fail to insert first freeze schema", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_gts_service_data()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(unit_mgr_.load())) {
    LOG_WARN("fail to load unit mgr", K(ret));
  } else if (OB_FAIL(create_gts_unit_config())) {
    LOG_WARN("fail to create gts unit config", K(ret));
  } else if (OB_FAIL(create_gts_resource_pool())) {
    LOG_WARN("fail to create sys resource pool", K(ret));
  } else if (OB_FAIL(create_gts_tenant())) {
    LOG_WARN("fail to create gts tenant", K(ret));
  } else if (OB_FAIL(create_original_gts_instance())) {
    LOG_WARN("fail to create gts instance", K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_backup_inner_table()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(ObBackupInfoOperator::set_inner_table_version(
                 ddl_service_.get_sql_proxy(), OB_BACKUP_INNER_TABLE_V3))) {
    LOG_WARN("failed to init backup inner table version", K(ret));
  } else if (OB_FAIL(ObBackupInfoOperator::set_max_piece_id(ddl_service_.get_sql_proxy(), 0))) {
    LOG_WARN("failed to init max piece id", K(ret));
  } else if (OB_FAIL(ObBackupInfoOperator::set_max_piece_create_date(ddl_service_.get_sql_proxy(), 0))) {
    LOG_WARN("failed to init set_max_piece_create_date", K(ret));
  }

  return ret;
}

int ObBootstrap::init_gts_unit_config(share::ObUnitConfig& gts_unit_config)
{
  int ret = OB_SUCCESS;
  ObUnitConfig sys_unit_config;
  if (OB_FAIL(init_sys_unit_config(sys_unit_config))) {
    LOG_WARN("fail to init sys unit config", K(ret));
  } else {
    // use the CPU of sys unit config as gts_unit_config
    gts_unit_config.reset();
    gts_unit_config.unit_config_id_ = OB_GTS_UNIT_CONFIG_ID;
    gts_unit_config.name_ = OB_GTS_UNIT_CONFIG_NAME;
    // gts_unit_config.max_cpu_ = sys_unit_config.max_cpu_;
    // gts_unit_config.min_cpu_ = sys_unit_config.min_cpu_;
    gts_unit_config.max_cpu_ = 0.5;
    gts_unit_config.min_cpu_ = 0.5;
    gts_unit_config.max_memory_ = config_.__min_full_resource_pool_memory;
    gts_unit_config.min_memory_ = gts_unit_config.max_memory_;
    gts_unit_config.max_disk_size_ = sys_unit_config.max_disk_size_;
    gts_unit_config.max_iops_ = sys_unit_config.max_iops_;
    gts_unit_config.min_iops_ = sys_unit_config.min_iops_;
    gts_unit_config.max_session_num_ = INT64_MAX;
  }
  return ret;
}

int ObBootstrap::create_gts_unit_config()
{
  int ret = OB_SUCCESS;
  ObUnitConfig unit_config;
  const bool if_not_exist = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(init_gts_unit_config(unit_config))) {
    LOG_WARN("fail to init default gts unit config", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_unit_config(unit_config, if_not_exist))) {
    LOG_WARN("fail to create unit config", K(ret));
  }
  return ret;
}

int ObBootstrap::gen_gts_resource_pool(share::ObResourcePool& pool)
{
  int ret = OB_SUCCESS;
  pool.resource_pool_id_ = OB_GTS_RESOURCE_POOL_ID;
  pool.name_ = "gts_pool";
  pool.unit_count_ = 1;
  pool.unit_config_id_ = OB_GTS_UNIT_CONFIG_ID;
  pool.tenant_id_ = OB_INVALID_ID;
  if (OB_FAIL(gen_sys_zone_list(pool.zone_list_))) {
    LOG_WARN("fail to gen sys zone list", K(ret));
  }
  return ret;
}

int ObBootstrap::create_gts_resource_pool()
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> gts_units;
  ObArray<ObResourcePoolName> pool_names;
  share::ObResourcePool pool;
  const bool is_bootstrap = true;
  const bool if_not_exist = false;
  const uint64_t tenant_id = OB_GTS_TENANT_ID;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(gen_gts_resource_pool(pool))) {
    LOG_WARN("fail to gen sys zone list", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_resource_pool(pool, OB_GTS_UNIT_CONFIG_NAME, if_not_exist))) {
    LOG_WARN("fail to create resource pool", K(ret), K(pool), "name", OB_GTS_UNIT_CONFIG_NAME);
  } else if (OB_FAIL(gen_gts_units(gts_units))) {
    LOG_WARN("fail to gen gts units", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_gts_units(gts_units))) {
    LOG_WARN("fail to create gts units", K(ret));
  } else if (OB_FAIL(pool_names.push_back(pool.name_))) {
    LOG_WARN("fail to push back", K(ret));
  } else if (OB_FAIL(unit_mgr_.grant_pools(ddl_service_.get_sql_proxy(),
                 share::ObWorker::CompatMode::MYSQL,
                 pool_names,
                 tenant_id,
                 is_bootstrap))) {
    LOG_WARN("fail to grant pools to tenant", K(ret), K(pool_names), K(tenant_id));
  } else {
    const bool grant = true;
    if (OB_FAIL(unit_mgr_.commit_change_pool_owner(grant, pool_names, tenant_id))) {
      LOG_WARN("fail to commit change pool owner", K(ret), K(pool_names), K(tenant_id));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_gts_tenant()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else {
    // GTS tenant is just a virtual tenant
  }
  LOG_INFO("create gts tenant", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_original_gts_instance()
{
  int ret = OB_SUCCESS;
  common::ObRegion primary_region;
  common::ObArray<share::ObUnit> gts_units;
  common::ObArray<common::ObZone> zones_in_primary_region;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(pick_gts_tenant_primary_region(primary_region))) {
    LOG_WARN("fail to pick gts tenant primary region", K(ret));
  } else if (OB_FAIL(gen_gts_units(gts_units))) {
    LOG_WARN("fail to gen gts units", K(ret));
  } else if (OB_FAIL(get_zones_in_primary_region(primary_region, zones_in_primary_region))) {
    LOG_WARN("fail to get zones in primary region", K(ret));
  } else {
    common::ObMemberList member_list;
    common::ObAddr standby_server;
    const common::ObGtsName gts_name = common::OB_ORIGINAL_GTS_NAME;
    const uint64_t gts_id = common::OB_ORIGINAL_GTS_ID;
    int64_t now = common::ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < gts_units.count(); ++i) {
      const share::ObUnit& this_unit = gts_units.at(i);
      const common::ObAddr& this_server = this_unit.server_;
      const common::ObZone& this_zone = this_unit.zone_;
      if (!has_exist_in_array(zones_in_primary_region, this_zone)) {
        // bypass
      } else if (member_list.get_member_number() < OB_GTS_QUORUM) {
        common::ObMember this_member(this_server, now);
        if (OB_FAIL(member_list.add_member(this_member))) {
          LOG_WARN("fail to add member", K(ret), K(this_member));
        }
      } else if (!standby_server.is_valid()) {
        standby_server = this_server;
      } else {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rs_gts_manager_.load())) {
        LOG_WARN("fail to rs gts manager", K(ret));
      } else if (OB_FAIL(rs_gts_manager_.create_gts_instance(
                     gts_id, gts_name, primary_region, member_list, standby_server))) {
        LOG_WARN("fail to create gts instance", K(ret));
      }
    }
  }
  return ret;
}

// TODO: , set sys tenant memory
int ObBootstrap::init_sys_unit_config(share::ObUnitConfig& unit_config)
{
  int ret = OB_SUCCESS;
  ObUnitConfig c;
  share::ObServerStatus server_status;
  c.name_ = OB_SYS_UNIT_CONFIG_NAME;
  c.max_cpu_ = DEFAULT_MAX_SYS_CPU;
  c.max_memory_ = config_.get_max_sys_tenant_memory();
  c.min_memory_ = config_.get_min_sys_tenant_memory();

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(ddl_service_.get_server_manager().get_server_status(
                 ddl_service_.get_server_manager().get_rs_addr(), server_status))) {
    LOG_WARN("get rs status failed", "rs addr", ddl_service_.get_server_manager().get_rs_addr(), K(ret));
  } else {
    c.max_disk_size_ = server_status.resource_info_.disk_total_;
    // TODO :  not used right now, hard coded.
    c.max_iops_ = 10000;
    c.max_session_num_ = INT64_MAX;
    if (OB_FAIL(fill_sys_unit_config(c, unit_config))) {
      LOG_WARN("faill to fill sys unit config", K(ret));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_sys_unit_config()
{
  int ret = OB_SUCCESS;
  ObUnitConfig unit_config;
  const bool if_not_exist = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(init_sys_unit_config(unit_config))) {
    LOG_WARN("init default sys unit config failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_unit_config(unit_config, if_not_exist))) {
    LOG_WARN("create_unit_config failed", K(unit_config), K(if_not_exist), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::gen_sys_resource_pool(share::ObResourcePool& pool)
{
  int ret = OB_SUCCESS;
  pool.resource_pool_id_ = OB_SYS_RESOURCE_POOL_ID;
  pool.name_ = "sys_pool";
  pool.unit_count_ = 1;
  pool.unit_config_id_ = OB_SYS_UNIT_CONFIG_ID;
  pool.tenant_id_ = OB_INVALID_ID;
  if (OB_FAIL(gen_sys_zone_list(pool.zone_list_))) {
    LOG_WARN("fail to gen sys zone list", K(ret));
  }
  return ret;
}

int ObBootstrap::create_sys_resource_pool()
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> sys_units;
  ObArray<ObResourcePoolName> pool_names;
  share::ObResourcePool pool;
  bool is_bootstrap = true;
  const bool if_not_exist = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_resource_pool(pool))) {
    LOG_WARN("gen sys resource pool", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_resource_pool(pool, OB_SYS_UNIT_CONFIG_NAME, if_not_exist))) {
    LOG_WARN("create sys resource pool failed", K(pool), "unit_config", OB_SYS_UNIT_CONFIG_NAME, K(ret));
  } else if (OB_FAIL(gen_sys_units(sys_units))) {
    LOG_WARN("gen_sys_units failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_sys_units(sys_units))) {
    LOG_WARN("create_sys_units failed", K(sys_units), K(ret));
  } else if (OB_FAIL(pool_names.push_back(pool.name_))) {
    LOG_WARN("push_back failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.grant_pools(ddl_service_.get_sql_proxy(),
                 share::ObWorker::CompatMode::MYSQL,
                 pool_names,
                 OB_SYS_TENANT_ID,
                 is_bootstrap))) {
    LOG_WARN(
        "grant_pools_to_tenant failed", K(pool_names), "tenant_id", static_cast<uint64_t>(OB_SYS_TENANT_ID), K(ret));
  } else {
    const bool grant = true;
    if (OB_FAIL(unit_mgr_.commit_change_pool_owner(grant, pool_names, OB_SYS_TENANT_ID))) {
      LOG_WARN("commit_change_pool_owner failed",
          K(grant),
          K(pool_names),
          "tenant_id",
          static_cast<uint64_t>(OB_SYS_TENANT_ID),
          K(ret));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_multiple_zone_deployment_table(common::ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, zone_info)
  {
    int64_t frozen_version = arg_.initial_frozen_version_;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      zone_info.reset();
      zone_info.zone_ = rs_list_[i].zone_;
      zone_info.status_.value_ = ObZoneStatus::ACTIVE;
      zone_info.status_.info_ = ObZoneStatus::get_status_str(ObZoneStatus::ACTIVE);
      zone_info.merge_start_time_.value_ = ::oceanbase::common::ObTimeUtility::current_time();
      zone_info.last_merged_time_.value_ = ::oceanbase::common::ObTimeUtility::current_time();
      zone_info.last_merged_version_.value_ = frozen_version;
      zone_info.all_merged_version_.value_ = frozen_version;
      zone_info.broadcast_version_.value_ = frozen_version;
      // for compatibility with ob1.2, which has no region specified
      if (rs_list_[i].region_.is_empty()) {
        if (OB_FAIL(zone_info.region_.info_.assign(DEFAULT_REGION_NAME))) {
          LOG_WARN("fail assign default region info", K(ret), K(zone_info));
        }
      } else {
        if (OB_FAIL(zone_info.region_.info_.assign(ObString(rs_list_[i].region_.size(), rs_list_[i].region_.ptr())))) {
          LOG_WARN("fail assign region info", K(ret), K(zone_info));
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        zone_info.storage_type_.value_ = ObZoneInfo::STORAGE_TYPE_LOCAL;
        zone_info.storage_type_.info_ = ObZoneInfo::get_storage_type_str(ObZoneInfo::STORAGE_TYPE_LOCAL);
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObZoneTableOperation::insert_zone_info(sql_client, zone_info))) {
          LOG_WARN("insert zone info failed", K(ret), K(zone_info));
        }
      }
    }
  }
  return ret;
}

int ObBootstrap::init_all_zone_table()
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    const int64_t frozen_version = arg_.initial_frozen_version_;
    HEAP_VAR(ObGlobalInfo, global_info)
    {
      global_info.try_frozen_version_.value_ = frozen_version;
      global_info.frozen_version_.value_ = frozen_version;
      global_info.global_broadcast_version_.value_ = frozen_version;
      global_info.proposal_frozen_version_.value_ = frozen_version;
      if (OB_FAIL(ObZoneTableOperation::insert_global_info(trans, global_info))) {
        LOG_WARN("insert global info failed", K(ret));
      } else {
        if (OB_FAIL(init_multiple_zone_deployment_table(trans))) {
          LOG_WARN("fail to init multiple zone deployment table", K(ret));
        }
      }
      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", K(tmp_ret), K(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }

  LOG_INFO("init all zone table", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

// FIXME:it need to write in new table, if table name changes after splitting
int ObBootstrap::init_server_id(const uint64_t server_id)
{
  int ret = OB_SUCCESS;
  ObMaxIdFetcher fetcher(ddl_service_.get_sql_proxy());
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_INVALID_ID == server_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server_id", K(server_id), K(ret));
  } else if (OB_FAIL(fetcher.update_max_id(
                 ddl_service_.get_sql_proxy(), OB_SYS_TENANT_ID, OB_MAX_USED_SERVER_ID_TYPE, server_id))) {
    LOG_WARN("update max used server id failed", K(server_id), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

template <typename SCHEMA>
int ObBootstrap::set_replica_options(const bool set_primary_zone, SCHEMA& schema)
{
  int ret = OB_SUCCESS;
  BOOTSTRAP_CHECK_SUCCESS_V2("before set replica options");
  ObArray<ObZone> zone_list;
  ObArray<ObString> zone_str_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema), K(ret));
  } else if (OB_FAIL(gen_sys_zone_list(zone_list))) {
    LOG_WARN("gen_zone_list failed", K(ret));
  } else if (zone_list.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_list is empty", K(zone_list), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      if (OB_FAIL(zone_str_list.push_back(ObString::make_string(zone_list.at(i).ptr())))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(schema.set_zone_list(zone_str_list))) {
      LOG_WARN("set_zone_list failed", K(zone_str_list), K(ret));
    } else if (zone_str_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone_str_list is empty", K(zone_str_list), K(ret));
    } else {
      if (set_primary_zone) {
        common::ObZone primary_zone;
        if (OB_FAIL(pick_sys_tenant_primary_zone(primary_zone))) {
          LOG_WARN("fail to pick sys tenant primary zone", K(ret));
        } else if (OB_FAIL(schema.set_primary_zone(primary_zone.ptr()))) {
          LOG_WARN("set_primary_zone failed", "zone", primary_zone, K(ret));
        } else if (OB_FAIL(normalize_schema_primary_zone(schema, zone_str_list))) {
          LOG_WARN("fail to normalized schema primary zone", K(ret));
        } else {
        }  // no more to do
      } else {
      }  // do nothing
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

template <typename SCHEMA>
int ObBootstrap::normalize_schema_primary_zone(SCHEMA& schema, const common::ObIArray<common::ObString>& zone_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<share::schema::ObZoneRegion, common::MAX_ZONE_NUM> zone_region_list;
  if (OB_FAIL(build_zone_region_list(zone_region_list))) {
    LOG_WARN("fail to build zone region list", K(ret));
  } else {
    char primary_zone_str[MAX_ZONE_LENGTH];
    int64_t pos = 0;
    ObPrimaryZoneUtil primary_zone_util(schema.get_primary_zone(), &zone_region_list);
    if (OB_FAIL(primary_zone_util.init(zone_list))) {
      LOG_WARN("fail to init primary zone util", K(ret));
    } else if (OB_FAIL(primary_zone_util.check_and_parse_primary_zone())) {
      LOG_WARN("fail to check and parse primary zone", K(ret));
    } else if (OB_FAIL(primary_zone_util.output_normalized_primary_zone(primary_zone_str, MAX_ZONE_LENGTH, pos))) {
      LOG_WARN("fail to output normalized primary zone", K(ret));
    } else if (OB_FAIL(schema.set_primary_zone(primary_zone_str))) {
      LOG_WARN("fail to set primary zone", K(ret));
    } else if (OB_FAIL(schema.set_primary_zone_array(primary_zone_util.get_zone_array()))) {
      LOG_WARN("fail to set primary zone array", K(ret));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObBootstrap::build_zone_region_list(ObIArray<share::schema::ObZoneRegion>& zone_region_list)
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  for (int64_t i = 0; i < rs_list_.count() && OB_SUCC(ret); ++i) {
    const common::ObZone& zone = rs_list_.at(i).zone_;
    const common::ObRegion& region = rs_list_.at(i).region_;
    if (OB_FAIL(zone_region_list.push_back(ObZoneRegion(zone, region)))) {
      LOG_WARN("fail to push back", K(ret), K(zone), K(region));
    } else {
    }  // no more to do
  }
  return ret;
}

int ObBootstrap::set_in_bootstrap()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check inner stat error", K(ret));
  } else {
    ObMultiVersionSchemaService& multi_schema_service = ddl_service_.get_schema_service();
    ObSchemaService* schema_service = multi_schema_service.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else {
      schema_service->set_in_bootstrap(true);
    }
  }
  return ret;
}

int ObBootstrap::check_schema_version(
    ObSchemaGetterGuard& schema_guard, const ObIArray<ObTableSchema>& table_schemas, const bool need_update)
{
  int ret = OB_SUCCESS;
  int64_t refresh_schema_version = 0;
  int64_t gen_schema_version = 0;
  int64_t schema_version = 0;
  const ObTableSchema* table_schema = NULL;
  int64_t baseline_schema_version = OB_INVALID_VERSION;
  int64_t tmp_frozen_version = 0;
  ObMySQLProxy& sql_proxy = ddl_service_.get_sql_proxy();
  ObGlobalStatProxy global_stat_proxy(sql_proxy);
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(global_stat_proxy.get_baseline_schema_version(tmp_frozen_version, baseline_schema_version))) {
    LOG_WARN("fail to get baseline schema version", KR(ret));
  }
  for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
    const ObTableSchema& schema = table_schemas.at(i);
    if (i > 0 && 0 == i % BATCH_INSERT_SCHEMA_CNT) {
      if (OB_FAIL(ObSchemaServiceSQLImpl::gen_bootstrap_schema_version(
              OB_SYS_TENANT_ID, refresh_schema_version, gen_schema_version, schema_version))) {
        LOG_WARN("fail to gen bootstrap schema version", KR(ret));
      } else {
        refresh_schema_version = schema_version;
        gen_schema_version = schema_version;
      }
    }
    if (!is_sys_table(schema.get_table_id())) {
      // nothing todo
    } else if (OB_FAIL(ObSchemaServiceSQLImpl::gen_bootstrap_schema_version(
                   OB_SYS_TENANT_ID, refresh_schema_version, gen_schema_version, schema_version))) {
      LOG_WARN("fail to gen bootstrap schema version", KR(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(schema.get_table_id(), table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid table schema", KR(ret), "table_id", schema.get_table_id());
    } else if (!need_update) {
      if (table_schema->get_schema_version() != schema_version) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table schema",
            KR(ret),
            K(i),
            "table_name",
            schema.get_table_name(),
            K(schema_version),
            "real_schema_version",
            table_schema->get_schema_version());
      }
    } else {
      // need update
      if (table_schema->get_schema_version() == schema_version) {
        // nothing todo
      } else if (table_schema->get_schema_version() > baseline_schema_version) {
        // DDL operation in the upgrade process will cause the schema version to become larger
        // if the schema_version of table is larger than baseline_schema_version,
        // it is no need to update.
        // nothing todo
      } else if (OB_FAIL(ddl_service_.update_table_schema_version(table_schema))) {
        LOG_WARN("fail to update table schema version", KR(ret), "table_id", table_schema->get_table_id());
      } else {
        LOG_INFO("update table schema version success", "table_id", table_schema->get_table_id());
      }
    }
    refresh_schema_version = schema_version;
    gen_schema_version = schema_version;
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
