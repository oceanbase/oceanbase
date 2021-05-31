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

#include "ob_vtable_location_getter.h"

#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_array_iterator.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_unit_manager.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace rootserver {
ObVTableLocationGetter::ObVTableLocationGetter(ObServerManager& server_mgr, ObUnitManager& unit_mgr)
    : server_mgr_(server_mgr), unit_mgr_(unit_mgr)
{}

ObVTableLocationGetter::~ObVTableLocationGetter()
{}

int ObVTableLocationGetter::get(const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  locations.reuse();
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", KT(table_id), K(ret));
  } else if (!is_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not virtual table", KT(table_id), K(ret));
  } else if (is_only_rs_virtual_table(table_id)) {
    if (OB_FAIL(get_only_rs_vtable_location(table_id, locations))) {
      LOG_WARN("get_only_rs_vtable_location failed", KT(table_id), K(ret));
    }
  } else if (is_global_virtual_table(table_id)) {
    if (OB_FAIL(get_global_vtable_location(table_id, locations))) {
      LOG_WARN("get_global_vtable_location failed", KT(table_id), K(ret));
    }
  } else if (is_tenant_virtual_table(table_id)) {
    if (OB_FAIL(get_tenant_vtable_location(table_id, locations))) {
      LOG_WARN("get_tenant_vtable_location failed", KT(table_id), K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected not to run here", KT(table_id), K(ret));
  }
  return ret;
}

int ObVTableLocationGetter::get_only_rs_vtable_location(
    const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  locations.reuse();
  ObArray<ObAddr> servers;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", KT(table_id), K(ret));
  } else if (!is_only_rs_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not only rs virtual table", KT(table_id), K(ret));
  } else if (OB_FAIL(servers.push_back(server_mgr_.get_rs_addr()))) {
    LOG_WARN("push_back failed", K(ret));
  } else if (OB_FAIL(build_location(table_id, servers, locations))) {
    LOG_WARN("build_location failed", KT(table_id), K(servers), K(ret));
  }
  return ret;
}

int ObVTableLocationGetter::get_global_vtable_location(
    const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  locations.reuse();
  ObZone zone;  // empty zone means all zones
  ObArray<ObAddr> servers;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", KT(table_id), K(ret));
  } else if (!is_global_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not global virtual table", KT(table_id), K(ret));
  } else if (!server_mgr_.has_build()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server manager hasn't built", "server_mgr built", server_mgr_.has_build(), K(ret));
  } else if (OB_FAIL(server_mgr_.get_alive_servers(zone, servers))) {
    LOG_WARN("get_alive_servers failed", K(ret));
  } else if (OB_FAIL(build_location(table_id, servers, locations))) {
    LOG_WARN("build_location failed", KT(table_id), K(servers), K(ret));
  }
  return ret;
}

int ObVTableLocationGetter::get_tenant_vtable_location(
    const uint64_t table_id, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  locations.reuse();
  const uint64_t tenant_id = extract_tenant_id(table_id);
  ObArray<ObAddr> unit_servers;
  ObArray<ObAddr> servers;
  ObArray<uint64_t> pool_ids;
  if (OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_id is invalid", KT(table_id), K(ret));
  } else if (!is_tenant_virtual_table(table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not tenant virtual table", KT(table_id), K(ret));
  } else if (!server_mgr_.has_build() || !unit_mgr_.check_inner_stat()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server manager or unit manager hasn't built",
        "server_mgr built",
        server_mgr_.has_build(),
        "unit_mgr built",
        unit_mgr_.check_inner_stat(),
        K(ret));
  } else if (OB_FAIL(unit_mgr_.get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("get_pool_ids_of_tenant failed", K(tenant_id), K(ret));
  } else {
    ObArray<ObUnitInfo> unit_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      unit_infos.reuse();
      if (OB_FAIL(unit_mgr_.get_unit_infos_of_pool(pool_ids.at(i), unit_infos))) {
        LOG_WARN("get_unit_infos_of_pool failed", "pool_id", pool_ids.at(i), K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < unit_infos.count(); ++j) {
          bool is_alive = false;
          const ObUnit& unit = unit_infos.at(j).unit_;
          if (OB_FAIL(server_mgr_.check_server_alive(unit.server_, is_alive))) {
            LOG_WARN("check_server_alive failed", "server", unit.server_, K(ret));
          } else if (is_alive) {
            if (OB_FAIL(unit_servers.push_back(unit.server_))) {
              LOG_WARN("push_back failed", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            if (unit.migrate_from_server_.is_valid()) {
              if (OB_FAIL(server_mgr_.check_server_alive(unit.migrate_from_server_, is_alive))) {
                LOG_WARN("check_server_alive failed", "server", unit.migrate_from_server_, K(ret));
              } else if (is_alive) {
                if (OB_FAIL(unit_servers.push_back(unit.migrate_from_server_))) {
                  LOG_WARN("push_back failed", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }

  // filter duplicated servers
  if (OB_SUCC(ret)) {
    std::sort(unit_servers.begin(), unit_servers.end());
    ObAddr pre_server;
    for (int64_t i = 0; OB_SUCC(ret) && i < unit_servers.count(); ++i) {
      if (!unit_servers.at(i).is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("server not expected to be invalid", "server", unit_servers.at(i), K(unit_servers), K(ret));
      } else {
        if (pre_server != unit_servers.at(i)) {
          if (OB_FAIL(servers.push_back(unit_servers.at(i)))) {
            LOG_WARN("push_back failed", K(ret));
          }
        }
        pre_server = unit_servers.at(i);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_location(table_id, servers, locations))) {
      LOG_WARN("build_location failed", KT(table_id), K(servers), K(ret));
    }
  }
  return ret;
}

int ObVTableLocationGetter::build_location(
    const uint64_t table_id, const ObArray<ObAddr>& servers, ObSArray<ObPartitionLocation>& locations)
{
  int ret = OB_SUCCESS;
  ObServerManager::ObServerStatusArray statuses;
  if (OB_INVALID_ID == table_id || servers.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KT(table_id), K(servers), K(ret));
  } else if (OB_FAIL(server_mgr_.get_server_statuses(servers, statuses))) {
    LOG_WARN("get_server_statuses failed", K(servers), K(ret));
  } else {
    ObPartitionLocation location;
    ObReplicaLocation replica_location;
    for (int64_t i = 0; OB_SUCC(ret) && i < statuses.count(); ++i) {
      location.reset();
      replica_location.reset();
      location.set_table_id(table_id);
      location.set_partition_id(i);
      location.set_partition_cnt(statuses.count());
      replica_location.role_ = LEADER;
      replica_location.server_ = statuses.at(i).server_;
      replica_location.sql_port_ = statuses.at(i).sql_port_;
      if (OB_FAIL(location.add(replica_location))) {
        LOG_WARN("location add failed", K(replica_location), K(ret));
      } else if (OB_FAIL(locations.push_back(location))) {
        LOG_WARN("push_back failed", K(location), K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase
