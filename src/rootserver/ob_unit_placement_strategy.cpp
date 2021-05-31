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
#include "ob_unit_placement_strategy.h"
#include "lib/charset/ob_mysql_global.h"  // for DBL_MAX
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

double ObUnitPlacementStrategy::ObServerResource::get_assigned(ObResourceType resource_type) const
{
  double ret = -1;
  if (resource_type < RES_MAX && resource_type >= 0) {
    ret = assigned_[resource_type];
  }
  return ret;
}

double ObUnitPlacementStrategy::ObServerResource::get_max_assigned(ObResourceType resource_type) const
{
  double ret = -1;
  if (resource_type < RES_MAX && resource_type >= 0) {
    ret = max_assigned_[resource_type];
  }
  return ret;
}

double ObUnitPlacementStrategy::ObServerResource::get_capacity(ObResourceType resource_type) const
{
  double ret = -1;
  if (resource_type < RES_MAX && resource_type >= 0) {
    ret = capacity_[resource_type];
  }
  return ret;
}

int ObUnitPlacementDPStrategy::choose_server(ObArray<ObUnitPlacementDPStrategy::ObServerResource>& servers,
    const share::ObUnitConfig& unit_config, common::ObAddr& server, const common::ObZone& zone, int64_t& found_idx)
{
  int ret = OB_SUCCESS;
  ObArray<double> all_dot_product;
  double dot_product = 0.0;
  double demands[RES_MAX];    // scaled demands
  double remaining[RES_MAX];  // scaled remaining
  double weight[RES_MAX];
  double max_dot_product = -1.0;
  found_idx = -1;
  if (servers.count() > 0) {
    if (OB_FAIL(ObResourceUtils::calc_server_resource_weight(servers, weight, RES_MAX))) {
      LOG_WARN("failed to calc the resource weight", K(ret));
    }
  }
  ARRAY_FOREACH(servers, i)
  {
    const ObServerResource& server_resource = servers.at(i);
    LOG_DEBUG("consider this server", K(i), K(server_resource), K_(hard_limit));
    if (0 == i) {
      // demand vector
      demands[RES_CPU] = unit_config.min_cpu_ / server_resource.capacity_[RES_CPU];
      demands[RES_MEM] = static_cast<double>(unit_config.min_memory_) / server_resource.capacity_[RES_MEM];
      if (static_cast<double>(unit_config.max_disk_size_) <= 0) {
        demands[RES_DISK] = 0.0;
      } else {
        demands[RES_DISK] = static_cast<double>(unit_config.max_disk_size_) / server_resource.capacity_[RES_DISK];
      }
    }
    if (have_enough_resource(server_resource, unit_config)) {
      // remaining vector
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        if (server_resource.capacity_[j] <= 0) {
          remaining[j] = 0;
        } else {
          remaining[j] = (server_resource.capacity_[j] - server_resource.assigned_[j]) / server_resource.capacity_[j];
        }
        if (remaining[j] < 0) {
          // Disk allocation may exceed its capacity
          remaining[j] = 0.0;
        }
      }
      dot_product = 0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        LOG_DEBUG("calc dot-product", K(j), "w", weight[j], "d", demands[j], "r", remaining[j]);
        dot_product += weight[j] * demands[j] * remaining[j];
      }
      if (dot_product > max_dot_product) {
        found_idx = i;
        max_dot_product = dot_product;
      }
    } else {
      // skip the server
      LOG_DEBUG("server does not have enough resource", K(server_resource), K(unit_config));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(found_idx == -1)) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_USER_ERROR(OB_MACHINE_RESOURCE_NOT_ENOUGH, to_cstring(zone));
      LOG_WARN("no server has enough resource to hold the unit", K(ret), K(unit_config), K(servers));
    } else {
      server = servers[found_idx].addr_;
    }
  }
  return ret;
}

bool ObUnitPlacementDPStrategy::have_enough_resource(
    const ObServerResource& server, const share::ObUnitConfig& unit_config)
{
  bool is_enough = true;
  if (unit_config.min_cpu_ + server.assigned_[RES_CPU] > server.capacity_[RES_CPU] ||
      unit_config.max_cpu_ + server.max_assigned_[RES_CPU] > server.capacity_[RES_CPU] * hard_limit_ ||
      static_cast<double>(unit_config.max_memory_) + server.max_assigned_[RES_MEM] >
          server.capacity_[RES_MEM] * hard_limit_ ||
      static_cast<double>(unit_config.min_memory_) + server.assigned_[RES_MEM] > server.capacity_[RES_MEM]) {
    is_enough = false;
  }
  return is_enough;
}

////////////////////////////////////////////////////////////////
bool ObUnitPlacementBestFitStrategy::have_enough_resource(
    const ObServerResource& server, const share::ObUnitConfig& unit_config)
{
  bool is_enough = true;
  // (unit_config.max_cpu_ + server.assigned_[RES_CPU]) / server.capacity_[RES_CPU] > soft_limit_
  if (unit_config.max_cpu_ > server.capacity_[RES_CPU] * soft_limit_ - server.assigned_[RES_CPU]) {
    is_enough = false;
  }
  return is_enough;
}

// Only consider cpu resource
int ObUnitPlacementBestFitStrategy::choose_server(common::ObArray<ObServerResource>& servers,
    const share::ObUnitConfig& unit_config, common::ObAddr& server, const common::ObZone& zone, int64_t& found_idx)
{
  int ret = OB_SUCCESS;
  double min_resource_diff = DBL_MAX;
  double remaining = 0;
  found_idx = -1;
  server.reset();
  ARRAY_FOREACH(servers, i)
  {
    const ObServerResource& server_resource = servers.at(i);
    if (have_enough_resource(server_resource, unit_config)) {
      remaining = server_resource.capacity_[RES_CPU] - server_resource.assigned_[RES_CPU];
      if (remaining - unit_config.max_cpu_ < min_resource_diff) {
        found_idx = i;
        min_resource_diff = remaining - unit_config.max_cpu_;
      }
    } else {
      // skip the server
      LOG_DEBUG("server does not have enough resource", K(server_resource), K(unit_config));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(found_idx == -1)) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_USER_ERROR(OB_MACHINE_RESOURCE_NOT_ENOUGH, to_cstring(zone));
      LOG_WARN("no server has enough resource to hold the unit", K(unit_config), K(ret));
    } else {
      server = servers[found_idx].addr_;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
bool ObUnitPlacementFFDStrategy::have_enough_resource(
    const ObServerResource& server, const share::ObUnitConfig& unit_config)
{
  bool is_enough = true;
  // (unit_config.max_cpu_ + server.assigned_[RES_CPU]) / server.capacity_[RES_CPU] > hard_limit_
  if (unit_config.max_cpu_ > server.capacity_[RES_CPU] * hard_limit_ - server.assigned_[RES_CPU]) {
    is_enough = false;
  }
  return is_enough;
}

int ObUnitPlacementFFDStrategy::choose_server(common::ObArray<ObServerResource>& servers,
    const share::ObUnitConfig& unit_config, common::ObAddr& server, const common::ObZone& zone, int64_t& found_idx)
{
  int ret = OB_SUCCESS;
  server.reset();
  double min_load = hard_limit_;
  double new_load = 0;
  found_idx = -1;
  ARRAY_FOREACH(servers, i)
  {
    const ObServerResource& server_resource = servers.at(i);
    if (have_enough_resource(server_resource, unit_config)) {
      new_load = (server_resource.assigned_[RES_CPU] + unit_config.max_cpu_) / server_resource.capacity_[RES_CPU];
      if (new_load <= min_load) {
        found_idx = i;
        new_load = min_load;
      }
    } else {
      // skip the server
      LOG_DEBUG("server does not have enough resource", K(server_resource), K(unit_config));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(found_idx == -1)) {
      ret = OB_MACHINE_RESOURCE_NOT_ENOUGH;
      LOG_USER_ERROR(OB_MACHINE_RESOURCE_NOT_ENOUGH, to_cstring(zone));
      LOG_WARN("no server has enough resource to hold the unit", K(unit_config), K(ret));
    } else {
      server = servers[found_idx].addr_;
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int ObUnitPlacementHybridStrategy::choose_server(common::ObArray<ObServerResource>& servers,
    const share::ObUnitConfig& unit_config, common::ObAddr& server, const common::ObZone& zone, int64_t& found_idx)
{
  int ret = OB_SUCCESS;
  found_idx = -1;
  if (OB_SUCCESS != (ret = best_fit_first_.choose_server(servers, unit_config, server, zone, found_idx))) {
    if (OB_MACHINE_RESOURCE_NOT_ENOUGH == ret) {
      ret = least_load_first_.choose_server(servers, unit_config, server, zone, found_idx);
    }
  }
  return ret;
}
