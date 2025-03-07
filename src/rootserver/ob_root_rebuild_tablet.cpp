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

#include "ob_root_rebuild_tablet.h"

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
ObRootRebuildTablet::ObRootRebuildTablet()
    :inited_(false),
     stopped_(false),
     rpc_proxy_(NULL),
     unit_manager_(NULL)
{
}

ObRootRebuildTablet::~ObRootRebuildTablet()
{
}

int ObRootRebuildTablet::init(ObSrvRpcProxy &rpc_proxy,
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

bool ObRootRebuildTablet::is_server_alive_(const ObAddr &server) const
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

int ObRootRebuildTablet::get_tenant_server_list_(uint64_t tenant_id,
                                              ObIArray<ObAddr> &target_server_list) const
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 2> pool_ids;
  target_server_list.reset();

  if (OB_FAIL(unit_manager_->get_pool_ids_of_tenant(tenant_id, pool_ids))) {
    LOG_WARN("fail to get pool ids of tenant", K(tenant_id), K(ret));
  } else {
    ObSEArray<share::ObUnitInfo, 4> units;

    for (int64_t i = 0; OB_SUCC(ret) && i < pool_ids.count(); ++i) {
      units.reset();
      if (OB_FAIL(unit_manager_->get_unit_infos_of_pool(pool_ids.at(i), units))) {
        LOG_WARN("fail to get unit infos of pool", K(pool_ids.at(i)), K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < units.count(); ++j) {
          if (OB_LIKELY(units.at(j).is_valid())) {
            const share::ObUnit &unit = units.at(j).unit_;
            if (is_server_alive_(unit.migrate_from_server_)) {
              if (OB_FAIL(target_server_list.push_back(unit.migrate_from_server_))) {
                LOG_WARN("fail to push server, ", K(ret));
              }
            }

            if (OB_SUCC(ret)) {
              if (is_server_alive_(unit.server_)) {
                if (OB_FAIL(target_server_list.push_back(unit.server_))) {
                  LOG_WARN("fail to push server, ", K(ret));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRootRebuildTablet::check_rebuild_src_and_dest_(
    const obrpc::ObRebuildTabletArg &arg,
    const common::ObIArray<common::ObAddr> &target_server_list) const
{
  int ret = OB_SUCCESS;
  bool is_src_exist = false;
  bool is_dest_exist = false;
  ObAddr src;
  ObAddr dest;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootRebuildTablet do not init", K(ret));
  } else if (OB_FAIL(arg.src_.get_location_addr(src))) {
    LOG_WARN("failed to get src location addr", K(ret), K(arg));
  } else if (OB_FAIL(arg.dest_.get_location_addr(dest))) {
    LOG_WARN("failed to get dest location addr", K(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < target_server_list.count(); ++i) {
      const ObAddr &server = target_server_list.at(i);
      if (server == src) {
        is_src_exist = true;
      } else if (server == dest) {
        is_dest_exist = true;
      }

      if (OB_SUCC(ret) && is_src_exist && is_dest_exist) {
        break;
      }
    }

    if (OB_SUCC(ret)) {
      if (!is_src_exist || !is_dest_exist) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("src or dest server is not exist", K(ret), K(arg), K(target_server_list));
      }
    }
  }
  return ret;
}

int ObRootRebuildTablet::try_rebuild_tablet(const obrpc::ObRebuildTabletArg &arg) const
{
  int ret = OB_SUCCESS;
  ObArray<ObAddr> target_server_list;
  ObAddr src_addr;
  ObAddr dest_addr;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootRebuildTablet do not init", K(ret));
  } else if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("try rebuild tablet get invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(arg.dest_.get_location_addr(dest_addr))) {
    LOG_WARN("failed to get dest addr", K(ret), K(arg));
  } else if (OB_FAIL(arg.src_.get_location_addr(src_addr))) {
    LOG_WARN("failed to get src addr", K(ret), K(arg));
  } else if (dest_addr == src_addr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild src should not same with dest", K(ret), K(dest_addr), K(src_addr));
  } else if (OB_FAIL(get_tenant_server_list_(arg.tenant_id_, target_server_list))) {
    LOG_WARN("failed to get tenant server list", K(ret), K(arg));
  } else if (OB_FAIL(check_rebuild_src_and_dest_(arg, target_server_list))) {
    LOG_WARN("failed to check rebuild src and dest", K(ret), K(arg));
  } else if (OB_FAIL(check_rebuild_ls_exist_(arg))) {
    LOG_WARN("failed to check rebuild ls exist", K(ret), K(arg));
  } else if (OB_FAIL(do_rebuild_tablet_(arg))) {
    LOG_WARN("failed to do rebuild tablet", K(ret), K(arg));
  }
  return ret;
}

int ObRootRebuildTablet::check_rebuild_ls_exist_(
    const obrpc::ObRebuildTabletArg &arg) const
{
  int ret = OB_SUCCESS;
  const int64_t expire_renew_time = INT64_MAX;
  share::ObLSLocation location;
  bool is_cache_hit = false;
  ObAddr src;
  ObAddr dest;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootRebuildTablet do not init", K(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(GCTX.location_service_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service ptr is null", KR(ret));
  } else if (OB_FAIL(arg.src_.get_location_addr(src))) {
    LOG_WARN("failed to get src location addr", K(ret), K(arg));
  } else if (OB_FAIL(arg.dest_.get_location_addr(dest))) {
    LOG_WARN("failed to get dest location addr", K(ret), K(arg));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = arg.tablet_id_array_.at(i);
      ObLSID ls_id;
      if (!tablet_id.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be invalid", K(ret), K(tablet_id));
      } else if (tablet_id.is_ls_inner_tablet()) {
        //do nothing
      } else if (OB_FAIL(GCTX.location_service_->get(arg.tenant_id_, tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
        LOG_WARN("fail to get ls id according to tablet_id", K(ret), K(arg), K(tablet_id));
      } else if (ls_id != arg.ls_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls not equal to rebuild tablet dest ls", K(ret), K(arg), K(tablet_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(GCTX.location_service_->get(
              GCONF.cluster_id, arg.tenant_id_, arg.ls_id_, expire_renew_time, is_cache_hit, location))) {
        LOG_WARN("get ls location failed", KR(ret), K(arg));
      } else {
        bool is_src_exist = false;
        bool is_dest_exist = false;
        const ObIArray<ObLSReplicaLocation> &ls_locations = location.get_replica_locations();
        for (int i = 0; i < ls_locations.count() && OB_SUCC(ret); ++i) {
          const ObAddr &server = ls_locations.at(i).get_server();
          if (server == dest) {
            is_dest_exist = true;
          } else if (server == src) {
            is_src_exist = true;
          }
        }

        if (OB_SUCC(ret)) {
          if (!is_dest_exist || !is_src_exist) {
            ret = OB_ENTRY_NOT_EXIST;
            LOG_WARN("rebuild tablet src or dest is not exist", K(ret), K(arg), K(ls_locations));
          }
        }
      }
    }
  }
  return ret;
}

int ObRootRebuildTablet::do_rebuild_tablet_(
    const obrpc::ObRebuildTabletArg &arg) const
{
  int ret = OB_SUCCESS;
  ObAddr dest;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootRebuildTablet do not init", K(ret));
  } else if (OB_FAIL(arg.dest_.get_location_addr(dest))) {
    LOG_WARN("failed to get location addr", K(ret), K(arg));
  } else if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy should not be NULL", K(ret), KP(rpc_proxy_));
  } else if (OB_FAIL(rpc_proxy_->to(dest).by(arg.tenant_id_).ha_rebuild_tablet(arg))) {
    LOG_WARN("failed to do ha rebuild tablet", K(ret), K(arg));
  }
  return ret;
}


} // namespace rootserver
} // namespace oceanbase
