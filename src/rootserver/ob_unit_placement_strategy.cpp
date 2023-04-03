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
#include "ob_root_utils.h"                // resource_type_to_str

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

DEF_TO_STRING(ObUnitPlacementStrategy::ObServerResource)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(addr));
  // CPU
  (void)databuff_printf(buf, buf_len, pos,
      ", cpu_capacity:%.6g, cpu_assigned:%.6g, cpu_assigned_max:%.6g, ",
      capacity_[RES_CPU], assigned_[RES_CPU], max_assigned_[RES_CPU]);

  // MEM
  (void)databuff_printf(buf, buf_len, pos,
      "mem_capacity:\"%.9gGB\", mem_assigned:\"%.9gGB\", ",
      capacity_[RES_MEM]/1024/1024/1024, assigned_[RES_MEM]/1024/1024/1024);

  // LOG_DISK
  (void)databuff_printf(buf, buf_len, pos,
      "log_disk_capacity:\"%.9gGB\", log_disk_assigned:\"%.9gGB\"",
      capacity_[RES_LOG_DISK]/1024/1024/1024, assigned_[RES_LOG_DISK]/1024/1024/1024);
  J_OBJ_END();
  return pos;
}

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

int ObUnitPlacementDPStrategy::choose_server(ObArray<ObUnitPlacementDPStrategy::ObServerResource> &servers,
                                             const share::ObUnitResource &demand_resource,
                                             const char *module,
                                             common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  ObArray<double> all_dot_product;
  double dot_product = 0.0;
  double demands[RES_MAX];  // scaled demands
  double remaining[RES_MAX];  // scaled remaining
  double weight[RES_MAX];
  double max_dot_product = -1.0;
  int64_t found_idx = -1;
  if (servers.count() > 0) {
    if (OB_FAIL(ObResourceUtils::calc_server_resource_weight(servers, weight, RES_MAX))) {
      LOG_WARN("failed to calc the resource weight", K(ret));
    }
  }
  ARRAY_FOREACH(servers, i) {
    const ObServerResource &server_resource = servers.at(i);
    LOG_DEBUG("consider this server", K(i), K(server_resource));
    for (int j = RES_CPU; j < RES_MAX; ++j) {
      const double capacity = server_resource.capacity_[j];
      const double remain = capacity - server_resource.assigned_[j];
      double demand = 0;

      switch (j) {
        case RES_CPU: { demand = demand_resource.min_cpu(); break; }
        case RES_MEM: { demand = static_cast<double>(demand_resource.memory_size()); break; }
        case RES_LOG_DISK: { demand = static_cast<double>(demand_resource.log_disk_size()); break; }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown resource type", KR(ret), K(j));
        }
      }

      if (OB_FAIL(ret)) {
      }
      // NOTE: input servers should have enough resource
      else if (capacity <= 0 || remain <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("server resource not enough, invalid server resource is invalid", K(server_resource),
            "resource_type", resource_type_to_str((ObResourceType)j), K(i), K(j), K(remain));
      } else {
        // compute demands[] vector
        // NOTE: this algorithm assumes that: all servers' capacity are same.
        //       demands[] vector should keep same for all server to calculate dot_product.
        //       so compute demands[] vector by first server resource
        if (0 == i) {
          demands[j] = demand / capacity;
        }

        // compute remaining[] vector
        remaining[j] = remain / capacity;
      }
    }

    if (OB_SUCCESS == ret) {
      dot_product = 0;
      for (int j = RES_CPU; j < RES_MAX; ++j) {
        double dp = weight[j] * demands[j] * remaining[j];
        dot_product += dp;
        _LOG_INFO("[%s] [CHOOSE_SERVER_FOR_UNIT] calc dot-product: server=%s, resource=%s, "
            "demands=%.6g, remain=%.6g, weight=%.6g, dp=%.6g sum=%.6g",
            module,
            to_cstring(server_resource.get_server()),
            resource_type_to_str((ObResourceType)j),
            demands[j], remaining[j], weight[j], dp, dot_product);
      }
      if (dot_product > max_dot_product) {
        found_idx = i;
        max_dot_product = dot_product;
      }
    }
  }

  if (OB_SUCC(ret)) {
    // It must be valid
    if (OB_UNLIKELY(found_idx == -1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("not found valid server for unit, unexpected", KR(ret), K(found_idx),
          K(max_dot_product), K(servers), K(demand_resource));
    } else {
      server = servers[found_idx].addr_;
      LOG_INFO("[CHOOSE_SERVER_FOR_UNIT] choose server succ", K(module), K(server), K(demand_resource), "server_resource", servers[found_idx]);
    }
  }
  return ret;
}
