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

#define USING_LOG_PREFIX RS

#include "ob_all_server_stat.h"

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;

namespace rootserver {
ObAllServerStat::ServerStat::ServerStat()
{
  reset();
}

void ObAllServerStat::ServerStat::reset()
{
  server_load_.reset();
  cpu_assigned_percent_ = 0;
  mem_assigned_percent_ = 0;
  disk_assigned_percent_ = 0;
  unit_num_ = 0;
  migrating_unit_num_ = 0;
  merged_version_ = 0;
  leader_count_ = 0;
  load_ = 0;
  cpu_weight_ = 0;
  memory_weight_ = 0;
  disk_weight_ = 0;
}

int ObAllServerStat::ServerStat::assign(const ObAllServerStat::ServerStat& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_assign(server_load_, other.server_load_))) {
    LOG_WARN("failed to assign server_load_", K(ret));
  }
  cpu_assigned_percent_ = other.cpu_assigned_percent_;
  mem_assigned_percent_ = other.mem_assigned_percent_;
  disk_assigned_percent_ = other.disk_assigned_percent_;
  unit_num_ = other.unit_num_;
  migrating_unit_num_ = other.migrating_unit_num_;
  merged_version_ = other.merged_version_;
  leader_count_ = other.leader_count_;
  load_ = other.load_;
  cpu_weight_ = other.cpu_weight_;
  memory_weight_ = other.memory_weight_;
  disk_weight_ = other.disk_weight_;
  return ret;
}

ObAllServerStat::ObAllServerStat()
    : inited_(false), schema_service_(NULL), unit_mgr_(NULL), server_mgr_(NULL), leader_coordinator_(NULL)
{}

ObAllServerStat::~ObAllServerStat()
{}

int ObAllServerStat::init(ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
    ObServerManager& server_mgr, ObILeaderCoordinator& leader_coordinator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    unit_mgr_ = &unit_mgr;
    server_mgr_ = &server_mgr;
    leader_coordinator_ = &leader_coordinator;
    inited_ = true;
  }

  return ret;
}

int ObAllServerStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init, allocator is null", K(ret));
  } else if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("get schema guard error", K(ret));
  } else if (!start_to_read_) {
    ObArray<ServerStat> server_stats;
    const ObTableSchema* table_schema = NULL;
    const uint64_t table_id = combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SERVER_STAT_TID);
    if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id), K(ret));
    } else if (OB_FAIL(get_server_stats(server_stats))) {
      LOG_WARN("fail to get server load", K(ret));
    } else if (server_stats.count() == 0) {
      LOG_WARN("fail to get server stat");
    } else {
      ObArray<Column> columns;
      FOREACH_CNT_X(server_stat, server_stats, OB_SUCCESS == ret)
      {
        columns.reuse();
        if (OB_FAIL(get_full_row(table_schema, *server_stat, columns))) {
          LOG_WARN("fail to get full row", "table_schema", *table_schema, K(ret));
        } else if (OB_FAIL(project_row(columns, cur_row_))) {
          LOG_WARN("fail to project row", K(columns), K(ret));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("fail to add row", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllServerStat::get_server_stats(ObIArray<ServerStat>& server_stats)
{
  int ret = OB_SUCCESS;
  ObZone zone;
  ObArray<ObUnitManager::ObServerLoad> server_loads;
  ObILeaderCoordinator::ServerLeaderStat leader_stat;
  double resource_weights[RES_MAX];
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_server_loads(zone, server_loads, resource_weights, RES_MAX))) {
    LOG_WARN("fail to get server loads", K(zone), K(ret));
  } else if (OB_FAIL(leader_coordinator_->get_leader_stat(leader_stat))) {
    LOG_WARN("get_leader_stat failed", K(ret));
  } else {
    ServerStat stat;
    for (int64_t i = 0; OB_SUCC(ret) && i < server_loads.count(); ++i) {
      stat.reset();
      if (OB_FAIL(stat.server_load_.assign(server_loads.at(i)))) {
        LOG_WARN("failed to assign stat.server_load_", K(ret));
      } else if (OB_FAIL(stat.server_load_.get_load(resource_weights, RES_MAX, stat.load_))) {
        LOG_WARN("failed to calc server load", K(ret), "server_load", stat.server_load_);
      } else {
        stat.leader_count_ = 0;
        stat.cpu_weight_ = resource_weights[RES_CPU];
        stat.memory_weight_ = resource_weights[RES_MEM];
        stat.disk_weight_ = resource_weights[RES_DISK];

        if (OB_FAIL(server_mgr_->get_merged_version(stat.server_load_.status_.server_, stat.merged_version_))) {
          LOG_WARN("get_merged_version failed", "server", stat.server_load_.status_.server_, K(ret));
        } else if (OB_FAIL(get_leader_count(leader_stat, stat.server_load_.status_.server_, stat.leader_count_))) {
          LOG_WARN("get_leader_count failed", K(leader_stat), "server", stat.server_load_.status_.server_, K(ret));
        } else if (OB_FAIL(server_stats.push_back(stat))) {
          LOG_WARN("fail to push back server stat", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObAllServerStat::get_leader_count(
    const ObILeaderCoordinator::ServerLeaderStat& leader_stat, const common::ObAddr& server, int64_t& leader_count)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid server", K(server), K(ret));
  } else if (leader_stat.count() <= 0) {
    // leader coordinator hasn't count leader ever
    leader_count = INVALID_INT_VALUE;
  } else {
    leader_count = 0;
    bool find = false;
    FOREACH_CNT_X(server_leader_count, leader_stat, !find)
    {
      if (server_leader_count->server_ == server) {
        leader_count = server_leader_count->count_;
        find = true;
      }
    }
  }
  return ret;
}

int ObAllServerStat::calc_server_usage(ServerStat& server_stat)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObUnitManager::calc_sum_load(
                 &server_stat.server_load_.unit_loads_, server_stat.server_load_.sum_load_))) {
    LOG_WARN("calc_sum_load failed", "server_load", server_stat.server_load_, K(ret));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("get_hard_limit failed", K(ret));
  } else {
    double cpu_assigned_percent = 0;
    double mem_assigned_percent = 0;
    double disk_assigned_percent = 0;

    if (std::fabs(server_stat.get_cpu_capacity()) < EPSLISON) {
      cpu_assigned_percent = INVALID_ASSIGNED_PERCENT;
    } else {
      server_stat.server_load_.status_.resource_info_.cpu_ =
          server_stat.server_load_.status_.resource_info_.cpu_;  // just for code layout
      cpu_assigned_percent = 100.0 * static_cast<double>(server_stat.get_cpu_assigned()) /
                             static_cast<double>(server_stat.get_cpu_capacity());
    }
    if (INVALID_TOTAL_RESOURCE == server_stat.get_mem_capacity()) {
      mem_assigned_percent = INVALID_ASSIGNED_PERCENT;
    } else {
      server_stat.server_load_.status_.resource_info_.mem_total_ =
          server_stat.server_load_.status_.resource_info_.mem_total_;  // just for code layout
      mem_assigned_percent = 100.0 * static_cast<double>(server_stat.get_mem_assigned()) /
                             static_cast<double>(server_stat.get_mem_capacity());
    }
    if (INVALID_TOTAL_RESOURCE == server_stat.get_disk_total()) {
      disk_assigned_percent = INVALID_ASSIGNED_PERCENT;
    } else {
      server_stat.server_load_.status_.resource_info_.disk_total_ = static_cast<int64_t>(
          static_cast<double>(server_stat.server_load_.status_.resource_info_.disk_total_) * hard_limit);
      disk_assigned_percent = 100.0 * static_cast<double>(server_stat.get_disk_assigned()) /
                              static_cast<double>(server_stat.get_disk_total());
    }

    server_stat.set_cpu_assigned_percent(static_cast<int64_t>(cpu_assigned_percent));
    server_stat.set_mem_assigned_percent(static_cast<int64_t>(mem_assigned_percent));
    server_stat.set_disk_assigned_percent(static_cast<int64_t>(disk_assigned_percent));
  }
  return ret;
}

int ObAllServerStat::calc_server_unit_num(ServerStat& server_stat)
{
  int ret = OB_SUCCESS;
  int64_t migrating_unit_num = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObArray<ObUnitManager::ObUnitLoad>& unit_loads = server_stat.server_load_.unit_loads_;
    server_stat.set_unit_num(unit_loads.count());

    for (int64_t i = 0; i < unit_loads.count(); ++i) {
      ObUnitManager::ObUnitLoad& unit_load = unit_loads.at(i);
      ObAddr& server = server_stat.server_load_.status_.server_;
      if (unit_load.unit_->migrate_from_server_ == server) {
        ++migrating_unit_num;
      }
    }
    server_stat.set_migrating_unit_num(migrating_unit_num);
  }
  return ret;
}

int ObAllServerStat::get_full_row(const ObTableSchema* table, ServerStat& server_stat, ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  double hard_limit = 0.0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (NULL == table) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is null", K(ret));
  } else if (OB_FAIL(unit_mgr_->get_hard_limit(hard_limit))) {
    LOG_WARN("get hard limit failed", K(ret));
  } else {
    const ObAddr& server = server_stat.server_load_.status_.server_;
    char* ip_buf = NULL;
    char* zone_buf = NULL;
    int n = -1;
    char* build_version = NULL;
    if (OB_FAIL(calc_server_unit_num(server_stat))) {
      LOG_WARN("calc_server_unit_num failed", K(ret));
    } else if (OB_FAIL(calc_server_usage(server_stat))) {
      LOG_WARN("fail to calculate server usage", K(ret));
    } else if (NULL == (ip_buf = static_cast<char*>(allocator_->alloc(MAX_IP_ADDR_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc ip buf", "size", MAX_IP_ADDR_LENGTH, K(ret));
    } else if (!server.ip_to_string(ip_buf, MAX_IP_ADDR_LENGTH)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("server ip is invalid", K(server), K(ret));
    } else if (NULL == (zone_buf = static_cast<char*>(allocator_->alloc(MAX_ZONE_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc zone buf", K(ret));
    } else if (0 > (n = snprintf(zone_buf, MAX_ZONE_LENGTH, "%s", server_stat.get_zone().ptr())) ||
               n >= MAX_ZONE_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("snprintf failed", "buf_len", MAX_ZONE_LENGTH, "src len", server_stat.get_zone().size(), K(ret));
    } else if (NULL == (build_version = static_cast<char*>(allocator_->alloc(OB_SERVER_VERSION_LENGTH)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc build_version", "size", OB_SERVER_VERSION_LENGTH, K(ret));
    } else {
      ADD_COLUMN(set_varchar, table, "svr_ip", ip_buf, columns);
      ADD_COLUMN(set_int, table, "svr_port", server.get_port(), columns);
      ADD_COLUMN(set_varchar, table, "zone", zone_buf, columns);
      if (std::fabs(server_stat.get_cpu_capacity()) < EPSLISON) {
        ADD_NULL_COLUMN(table, "cpu_capacity", columns);
        ADD_NULL_COLUMN(table, "cpu_total", columns);
      } else {
        ADD_COLUMN(set_double, table, "cpu_capacity", server_stat.get_cpu_capacity(), columns);
        ADD_COLUMN(set_double, table, "cpu_total", server_stat.get_cpu_capacity() * hard_limit, columns);
      }
      ADD_COLUMN(set_double, table, "cpu_assigned", server_stat.get_cpu_assigned(), columns);
      ADD_COLUMN(set_double, table, "cpu_max_assigned", server_stat.get_cpu_max_assigned(), columns);
      if (INVALID_ASSIGNED_PERCENT == server_stat.get_cpu_assigned_percent()) {
        ADD_NULL_COLUMN(table, "cpu_assigned_percent", columns);
      } else {
        ADD_COLUMN(set_int, table, "cpu_assigned_percent", server_stat.get_cpu_assigned_percent(), columns);
      }
      if (INVALID_TOTAL_RESOURCE == server_stat.get_mem_capacity()) {
        ADD_NULL_COLUMN(table, "mem_capacity", columns);
        ADD_NULL_COLUMN(table, "mem_total", columns);
      } else {
        ADD_COLUMN(set_int, table, "mem_capacity", server_stat.get_mem_capacity(), columns);
        ADD_COLUMN(set_int,
            table,
            "mem_total",
            static_cast<int64_t>(static_cast<double>(server_stat.get_mem_capacity()) * hard_limit),
            columns);
      }
      ADD_COLUMN(set_int, table, "mem_assigned", server_stat.get_mem_assigned(), columns);
      ADD_COLUMN(set_int, table, "mem_max_assigned", server_stat.get_mem_max_assigned(), columns);
      if (INVALID_ASSIGNED_PERCENT == server_stat.get_mem_assigned_percent()) {
        ADD_NULL_COLUMN(table, "mem_assigned_percent", columns);
      } else {
        ADD_COLUMN(set_int, table, "mem_assigned_percent", server_stat.get_mem_assigned_percent(), columns);
      }
      if (INVALID_TOTAL_RESOURCE == server_stat.get_disk_total()) {
        ADD_NULL_COLUMN(table, "disk_total", columns);
      } else {
        ADD_COLUMN(set_int, table, "disk_total", server_stat.get_disk_total(), columns);
      }
      ADD_COLUMN(set_int, table, "disk_assigned", server_stat.get_disk_assigned(), columns);
      if (INVALID_ASSIGNED_PERCENT == server_stat.get_disk_assigned_percent()) {
        ADD_NULL_COLUMN(table, "disk_assigned_percent", columns);
      } else {
        ADD_COLUMN(set_int, table, "disk_assigned_percent", server_stat.get_disk_assigned_percent(), columns);
      }
      ADD_COLUMN(set_int, table, "unit_num", server_stat.get_unit_num(), columns);
      ADD_COLUMN(set_int, table, "migrating_unit_num", server_stat.get_migrating_unit_num(), columns);
      ADD_COLUMN(set_int, table, "merged_version", server_stat.merged_version_, columns);
      if (INVALID_INT_VALUE == server_stat.leader_count_) {
        ADD_NULL_COLUMN(table, "leader_count", columns);
      } else {
        ADD_COLUMN(set_int, table, "leader_count", server_stat.leader_count_, columns);
      }
      // fields added on V1.4
      ADD_COLUMN(set_double, table, "load", server_stat.load_, columns);
      ADD_COLUMN(set_double, table, "cpu_weight", server_stat.cpu_weight_, columns);
      ADD_COLUMN(set_double, table, "memory_weight", server_stat.memory_weight_, columns);
      ADD_COLUMN(set_double, table, "disk_weight", server_stat.disk_weight_, columns);
      // fields added on V1.4.3
      ADD_COLUMN(set_int, table, "id", server_stat.server_load_.status_.id_, columns);
      ADD_COLUMN(set_int, table, "inner_port", server_stat.server_load_.status_.sql_port_, columns);
      ADD_COLUMN(set_int, table, "register_time", server_stat.server_load_.status_.register_time_, columns);
      ADD_COLUMN(set_int, table, "last_heartbeat_time", server_stat.server_load_.status_.last_hb_time_, columns);
      ADD_COLUMN(
          set_int, table, "block_migrate_in_time", server_stat.server_load_.status_.block_migrate_in_time_, columns);
      ADD_COLUMN(set_int, table, "stop_time", server_stat.server_load_.status_.stop_time_, columns);
      ADD_COLUMN(set_int, table, "start_service_time", server_stat.server_load_.status_.start_service_time_, columns);
      ADD_COLUMN(
          set_int, table, "ssl_key_expired_time", server_stat.server_load_.status_.ssl_key_expired_time_, columns);
      ADD_COLUMN(set_int, table, "force_stop_heartbeat", server_stat.server_load_.status_.force_stop_hb_, columns);
      ADD_COLUMN(set_int, table, "last_offline_time", server_stat.server_load_.status_.last_offline_time_, columns);
      ADD_COLUMN(set_int, table, "with_rootserver", server_stat.server_load_.status_.with_rootserver_, columns);
      ADD_COLUMN(set_int, table, "with_partition", server_stat.server_load_.status_.with_partition_, columns);
      ADD_COLUMN(set_int, table, "mem_in_use", server_stat.server_load_.status_.resource_info_.mem_in_use_, columns);
      ADD_COLUMN(set_int, table, "disk_in_use", server_stat.server_load_.status_.resource_info_.disk_in_use_, columns);
      const char* admin_status_str = NULL;
      (void)ObServerStatus::server_admin_status_str(server_stat.server_load_.status_.admin_status_, admin_status_str);
      ADD_COLUMN(set_varchar, table, "admin_status", admin_status_str, columns);
      const char* hb_status_str = NULL;
      (void)ObServerStatus::heartbeat_status_str(server_stat.server_load_.status_.hb_status_, hb_status_str);
      ADD_COLUMN(set_varchar, table, "heartbeat_status", hb_status_str, columns);
      (void)snprintf(build_version, OB_SERVER_VERSION_LENGTH, "%s", server_stat.server_load_.status_.build_version_);
      ADD_COLUMN(set_varchar, table, "build_version", build_version, columns);
      ADD_COLUMN(set_int, table, "clock_deviation", server_stat.server_load_.status_.last_server_behind_time_, columns);
      ADD_COLUMN(set_int, table, "heartbeat_latency", server_stat.server_load_.status_.last_round_trip_time_, columns);
      bool is_sync = std::abs(server_stat.server_load_.status_.last_server_behind_time_) <= GCONF.rpc_timeout;
      const char* clock_status_str = NULL;
      (void)ObServerStatus::clock_sync_status_str(is_sync, clock_status_str);
      ADD_COLUMN(set_varchar, table, "clock_sync_status", clock_status_str, columns);
    }
    if (OB_FAIL(ret)) {
      if (NULL != ip_buf) {
        allocator_->free(ip_buf);
        ip_buf = NULL;
      }
      if (NULL != zone_buf) {
        allocator_->free(zone_buf);
        zone_buf = NULL;
      }
    }
  }

  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
