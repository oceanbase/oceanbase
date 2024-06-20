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

#include "ob_root_minor_freeze.h"

#include "share/ob_srv_rpc_proxy.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_all_server_tracer.h"
#include "lib/container/ob_se_array.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;

namespace rootserver
{
ObRootMinorFreeze::ObRootMinorFreeze()
    :inited_(false),
     stopped_(false),
     rpc_proxy_(NULL),
     unit_manager_(NULL)
{
}

ObRootMinorFreeze::~ObRootMinorFreeze()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(destroy())) {
    LOG_WARN("destroy failed", K(ret));
  }
}

int ObRootMinorFreeze::init(ObSrvRpcProxy &rpc_proxy,
                            ObUnitManager &unit_manager)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    rpc_proxy_ = &rpc_proxy;
    unit_manager_ = &unit_manager;
    stopped_ = false;
    inited_ = true;
  }

  return ret;
}

void ObRootMinorFreeze::start()
{
  ATOMIC_STORE(&stopped_, false);
}

void ObRootMinorFreeze::stop()
{
  ATOMIC_STORE(&stopped_, true);
}

int ObRootMinorFreeze::destroy()
{
  int ret = OB_SUCCESS;
  inited_ = false;
  return ret;
}

inline
int ObRootMinorFreeze::check_cancel() const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (ATOMIC_LOAD(&stopped_)) {
    ret = OB_CANCELED;
    LOG_WARN("rs is stopped", K(ret));
  }
  return ret;
}

inline
bool ObRootMinorFreeze::is_server_alive(const ObAddr &server) const
{
  int ret = OB_SUCCESS;
  bool is_alive = false;

  if (OB_LIKELY(server.is_valid())) {
    if (OB_FAIL(SVR_TRACER.check_server_alive(server, is_alive))) {
      LOG_WARN("fail to check whether server is alive, ", K(server), K(ret));
      is_alive = false;
    }
  }

  return is_alive;
}

int ObRootMinorFreeze::try_minor_freeze(const obrpc::ObRootMinorFreezeArg &arg) const
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootMinorFreeze not init", K(ret));
  } else {
    ParamsContainer params;
    if ((arg.ls_id_.is_valid() && arg.ls_id_.id() > 0) || arg.tablet_id_.is_valid()) {
      if (1 == arg.tenant_ids_.count()) {
        if (OB_FAIL(init_params_by_ls_or_tablet(arg.tenant_ids_.at(0), arg.ls_id_, arg.tablet_id_, params))) {
          LOG_WARN("fail to init param by tablet_id");
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only one tenant is required for tablet_freeze", K(ret), K(arg));
      }
    } else if (arg.tenant_ids_.count() > 0) {
      if (OB_FAIL(init_params_by_tenant(arg.tenant_ids_, arg.zone_, arg.server_list_, params))) {
        LOG_WARN("fail to init param by tenant, ", K(ret), K(arg));
      }
    } else if (arg.server_list_.count() == 0 && arg.zone_.size() > 0) {
      if (OB_FAIL(init_params_by_zone(arg.zone_, params))) {
        LOG_WARN("fail to init param by zone, ", K(ret), K(arg));
      }
    } else {
      if (OB_FAIL(init_params_by_server(arg.server_list_, params))) {
        LOG_WARN("fail to init param by server, ", K(ret), K(arg));
      }
    }

    if (OB_SUCC(ret) && !params.is_empty()) {
      if (OB_FAIL(do_minor_freeze(params))) {
        LOG_WARN("fail to do minor freeze, ", K(ret));
      }
    }
  }

  return ret;
}

int ObRootMinorFreeze::do_minor_freeze(const ParamsContainer &params) const
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t failure_cnt = 0;
  ObMinorFreezeProxy proxy(*rpc_proxy_, &ObSrvRpcProxy::minor_freeze);
  LOG_INFO("do minor freeze", K(params));

  for (int64_t i = 0; OB_SUCC(ret) && i < params.get_params().count(); ++i) {
    const MinorFreezeParam &param = params.get_params().at(i);
    if (OB_FAIL(check_cancel())) {
      LOG_WARN("fail to check cancel", KR(ret));
    } else if (OB_TMP_FAIL(proxy.call(param.server, MINOR_FREEZE_TIMEOUT, param.arg))) {
      LOG_WARN("proxy call failed", KR(tmp_ret), K(param.arg),
               "dest addr", param.server);
      failure_cnt++;
    }
  }

  ObArray<int> return_code_array;
  if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
    LOG_WARN("proxy wait failed", KR(ret), KR(tmp_ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  } else if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
    LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_code_array.count());
  } else {
    for (int i = 0; i < proxy.get_results().count(); ++i) {
      if (OB_TMP_FAIL(static_cast<int>(*proxy.get_results().at(i)))) {
        LOG_WARN("fail to do minor freeze on target server, ", K(tmp_ret),
                 "dest addr:", proxy.get_dests().at(i),
                 "param:", proxy.get_args().at(i));
        failure_cnt++;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (0 != failure_cnt) {
    ret = OB_PARTIAL_FAILED;
    LOG_WARN("minor freeze partial failed", KR(ret), K(failure_cnt));
  }

  return ret;
}

int ObRootMinorFreeze::is_server_belongs_to_zone(const ObAddr &addr,
                                                 const ObZone &zone,
                                                 bool &server_in_zone) const
{
  int ret = OB_SUCCESS;
  ObZone server_zone;

  if (0 == zone.size()) {
    server_in_zone = true;
  } else if (OB_FAIL(SVR_TRACER.get_server_zone(addr, server_zone))) {
    LOG_WARN("fail to get server zone", KR(ret), K(addr));
  } else if (server_zone == zone) {
    server_in_zone = true;
  } else {
    server_in_zone = false;
  }

  return ret;
}

int ObRootMinorFreeze::init_params_by_ls_or_tablet(const uint64_t tenant_id,
                                                   share::ObLSID ls_id,
                                                   const common::ObTabletID &tablet_id,
                                                   ParamsContainer &params) const
{
  int ret = OB_SUCCESS;

  const int64_t expire_renew_time = INT64_MAX;
  share::ObLSLocation location;
  bool is_cache_hit = false;
  if (OB_UNLIKELY(OB_ISNULL(GCTX.location_service_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service ptr is null", KR(ret));
  } else if (tablet_id.is_valid() && !ls_id.is_valid()) {
    // get ls id by tablet_id
    if (tablet_id.is_ls_inner_tablet()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can not minor freeze inner tablet without specifying ls id", K(tenant_id), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(GCTX.location_service_->get(tenant_id, tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
      LOG_WARN("fail to get ls id according to tablet_id", K(ret), K(tenant_id), K(tablet_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ls_id.is_valid()) {
    // get ls location by ls_id
    if (OB_FAIL(GCTX.location_service_->get(
            GCONF.cluster_id, tenant_id, ls_id, expire_renew_time, is_cache_hit, location))) {
      LOG_WARN("get ls location failed", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls_id or tablet_id", KR(ret), K(ls_id), K(tablet_id));
  }

  if (OB_FAIL(ret)) {
  } else {
    const ObIArray<ObLSReplicaLocation> &ls_locations = location.get_replica_locations();
    for (int i = 0; i < ls_locations.count() && OB_SUCC(ret); ++i) {
      const ObAddr &server = ls_locations.at(i).get_server();
      if (is_server_alive(server)) {
        if (OB_FAIL(params.push_back_param(server, tenant_id, ls_id, tablet_id))) {
          LOG_WARN("fail to add tenant & server, ", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        }
      } else {
        int tmp_ret = OB_SERVER_NOT_ACTIVE;
        LOG_WARN("server not alive or invalid", "server", server, K(tmp_ret), K(tenant_id), K(ls_id), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObRootMinorFreeze::init_params_by_tenant(const ObIArray<uint64_t> &tenant_ids,
                                             const ObZone &zone,
                                             const ObIArray<ObAddr> &server_list,
                                             ParamsContainer &params) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 256> target_server_list;

  for (int i = 0; i < tenant_ids.count() && OB_SUCC(ret); ++i) {
    if (server_list.count() > 0) {
      for (int j = 0; j < server_list.count() && OB_SUCC(ret); ++j) {
        if (is_server_alive(server_list.at(j))) {
          if (OB_FAIL(params.push_back_param(server_list.at(j), tenant_ids.at(i)))) {
            LOG_WARN("fail to add tenant & server, ", K(ret));
          }
        } else {
          ret = OB_SERVER_NOT_ACTIVE;
          LOG_WARN("server not alive or invalid", "server", server_list.at(j), K(ret));
        }
      }
    } else {
      // TODO: filter servers according to tenant_id
      if (OB_ISNULL(unit_manager_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unit_manager_ is null", KR(ret), KP(unit_manager_));
      } else if (OB_FAIL(unit_manager_->get_tenant_alive_servers_non_block(tenant_ids.at(i), target_server_list))) {
        LOG_WARN("fail to get tenant server list, ", K(ret));
      } else {
        bool server_in_zone = false;
        for (int j = 0; j < target_server_list.count() && OB_SUCC(ret); ++j) {
          const ObAddr &server = target_server_list.at(j);
          if (OB_FAIL(is_server_belongs_to_zone(server, zone, server_in_zone))) {
            LOG_WARN("fail to check server", K(ret));
          } else if (server_in_zone && OB_FAIL(params.push_back_param(server, tenant_ids.at(i)))) {
            LOG_WARN("fail to add tenant & server", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObRootMinorFreeze::init_params_by_zone(const ObZone &zone,
                                           ParamsContainer &params) const
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> target_server_list;

  if (OB_UNLIKELY(0 == zone.size())) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (OB_FAIL(SVR_TRACER.get_servers_of_zone(zone, target_server_list))) {
      LOG_WARN("fail to get tenant server list, ", KR(ret), K(zone));
    } else if (0 == target_server_list.count()) {
      ret = OB_ZONE_NOT_ACTIVE;
      LOG_WARN("empty zone or invalid", K(zone), K(ret));
    } else {
      for (int i = 0; i < target_server_list.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(params.push_back_param(target_server_list.at(i)))) {
          LOG_WARN("fail to add server", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRootMinorFreeze::init_params_by_server(const ObIArray<ObAddr> &server_list,
                                             ParamsContainer &params) const
{
  int ret = OB_SUCCESS;
  if (server_list.count() > 0) {
    for (int i = 0; i < server_list.count() && OB_SUCC(ret); ++i) {
      if (is_server_alive(server_list.at(i))) {
        if (OB_FAIL(params.push_back_param(server_list.at(i)))) {
          LOG_WARN("fail to add server, ", K(ret));
        }
      } else {
        ret = OB_SERVER_NOT_ACTIVE;
        LOG_WARN("server not alive or invalid", "server", server_list.at(i), K(ret));
      }
    }
  } else {
    ObZone zone; // empty zone, get all server status
    ObSEArray<ObAddr, 256> target_server_list;

    // get all alive server
    if (OB_FAIL(SVR_TRACER.get_alive_servers(zone, target_server_list))) {
      LOG_WARN("fail to get alive servers, ", KR(ret), K(zone));
    } else {
      for (int i = 0; i < target_server_list.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(params.push_back_param(target_server_list.at(i)))) {
          LOG_WARN("fail to add server, ", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObRootMinorFreeze::ParamsContainer::push_back_param(const common::ObAddr &server,
                                                        const uint64_t tenant_id,
                                                        share::ObLSID ls_id,
                                                        const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;

  MinorFreezeParam param;
  param.server = server;
  param.arg.ls_id_ = ls_id;
  param.arg.tablet_id_ = tablet_id;

  if (0 != tenant_id && OB_FAIL(param.arg.tenant_ids_.push_back(tenant_id))) {
    LOG_WARN("fail to push tenant_id, ", K(ret));
  } else if (OB_FAIL(params_.push_back(param))) {
    LOG_WARN("fail to push tenant_id & server, ", K(ret));
  }

  return ret;
}

} // namespace rootserver
} // namespace oceanbase
