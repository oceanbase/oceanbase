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
#include "rootserver/ob_unit_manager.h"
#include "share/ob_all_server_tracer.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
ObVTableLocationGetter::ObVTableLocationGetter(ObUnitManager &unit_mgr) : unit_mgr_(unit_mgr)
{
}


ObVTableLocationGetter::~ObVTableLocationGetter()
{
}

// **FIXME (linqiucen.lqc): in the future, we can remove unit_mgr_,
// **                       then this func can be executed locally on observers
int ObVTableLocationGetter::get(const ObVtableLocationType &vtable_type,
                                ObSArray<common::ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  servers.reuse();
  if (OB_UNLIKELY(!vtable_type.is_valid()
      || vtable_type.is_only_local())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input is invalid", KR(ret), K(vtable_type));
  } else if (vtable_type.is_only_rs()) {
    if (OB_FAIL(get_only_rs_vtable_location_(vtable_type, servers))) {
      LOG_WARN("get_only_rs_vtable_location failed", KR(ret), K(vtable_type));
    }
  } else if (vtable_type.is_cluster_distributed()) {
    if (OB_FAIL(get_global_vtable_location_(vtable_type, servers))) {
      LOG_WARN("get_global_vtable_location failed", KR(ret), K(vtable_type));
    }
  } else if (vtable_type.is_tenant_distributed()) {
    if (OB_FAIL(get_tenant_vtable_location_(vtable_type, servers))) {
      LOG_WARN("get_tenant_vtable_location failed", KR(ret), K(vtable_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expected not to run here", KR(ret), K(vtable_type));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(servers.count() <= 0)) {
    ret = OB_LOCATION_NOT_EXIST;
    LOG_WARN("servers are empty", KR(ret), K(vtable_type), K(servers));
  }
  return ret;
}

int ObVTableLocationGetter::get_only_rs_vtable_location_(
    const ObVtableLocationType &vtable_type,
    ObSArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  servers.reuse();
  ObAddr rs_addr;
  if (OB_UNLIKELY(!vtable_type.is_only_rs())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vtable_type is invalid", K(vtable_type), KR(ret));
  } else if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.rs_mgr_ is null", KP(GCTX.rs_mgr_));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("fail to get master root server", KR(ret), KP(GCTX.rs_mgr_));
  } else if (OB_UNLIKELY(!rs_addr.is_valid() || rs_addr != GCTX.self_addr())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("rs_addr is invalid or not equal to self_addr", KR(ret), K(rs_addr), K(GCTX.self_addr()));
  } else if (OB_FAIL(servers.push_back(rs_addr))) {
    LOG_WARN("push_back failed", KR(ret), K(rs_addr));
  }
  return ret;
}

int ObVTableLocationGetter::get_global_vtable_location_(
    const ObVtableLocationType &vtable_type,
    ObSArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  servers.reuse();
  ObZone zone; // empty zone means all zones
  if (OB_UNLIKELY(!(vtable_type.is_cluster_distributed()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vtable_type is invalid", K(vtable_type), KR(ret));
  } else if (OB_FAIL(SVR_TRACER.get_alive_servers(zone, servers))) {
    LOG_WARN("get_alive_servers failed", KR(ret), KP(GCTX.sql_proxy_));
  }
  return ret;
}

int ObVTableLocationGetter::get_tenant_vtable_location_(
    const ObVtableLocationType &vtable_type,
    ObSArray<ObAddr> &servers)
{
  int ret = OB_SUCCESS;
  servers.reuse();
  ObArray<ObAddr> unit_servers;
  ObArray<uint64_t> pool_ids;
  bool unit_mgr_check = unit_mgr_.check_inner_stat();
  if (OB_UNLIKELY(!vtable_type.is_valid()
      || !vtable_type.is_tenant_distributed()
      || is_sys_tenant(vtable_type.get_tenant_id()))) { // sys_tenant should get cluster location
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("vtable_type is invalid", KR(ret), K(vtable_type));
  } else if (OB_UNLIKELY(!unit_mgr_check)) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("unit manager hasn't built", "unit_mgr built", unit_mgr_check, KR(ret));
  } else if (OB_FAIL(unit_mgr_.get_pool_ids_of_tenant(vtable_type.get_tenant_id(), pool_ids))) {
    LOG_WARN("get_pool_ids_of_tenant failed", KR(ret), K(vtable_type));
  } else {
    ObArray<ObUnitInfo> unit_infos;
    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      unit_infos.reuse();
      if (OB_FAIL(unit_mgr_.get_unit_infos_of_pool(pool_ids.at(i), unit_infos))) {
        LOG_WARN("get_unit_infos_of_pool failed", "pool_id", pool_ids.at(i), KR(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < unit_infos.count(); ++j) {
          bool is_alive = false;
          const ObUnit &unit = unit_infos.at(j).unit_;
          if (OB_FAIL(SVR_TRACER.check_server_alive(unit.server_, is_alive))) {
            LOG_WARN("check_server_alive failed", KR(ret), K(unit.server_));
          } else if (is_alive) {
            if (OB_FAIL(unit_servers.push_back(unit.server_))) {
              LOG_WARN("push_back failed", KR(ret));
            }
          }

          if (OB_SUCC(ret)) {
            if (unit.migrate_from_server_.is_valid()) {
              if (OB_FAIL(SVR_TRACER.check_server_alive(unit.migrate_from_server_, is_alive))) {
                LOG_WARN("check_server_alive failed", KR(ret), K(unit.migrate_from_server_));
              } else if (is_alive) {
                if (OB_FAIL(unit_servers.push_back(unit.migrate_from_server_))) {
                  LOG_WARN("push_back failed", KR(ret));
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
        LOG_WARN("server not expected to be invalid",
            "server", unit_servers.at(i), K(unit_servers), KR(ret));
      } else {
        if (pre_server != unit_servers.at(i)) {
          if (OB_FAIL(servers.push_back(unit_servers.at(i)))) {
            LOG_WARN("push_back failed", KR(ret));
          }
        }
        pre_server = unit_servers.at(i);
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
