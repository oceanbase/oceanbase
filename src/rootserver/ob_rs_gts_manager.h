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

#ifndef OCEANBASE_ROOTSERVER_OB_RS_GTS_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_RS_GTS_MANAGER_H_

#include "lib/container/ob_iarray.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "common/ob_region.h"
#include "share/ob_gts_table_operator.h"
#include "share/ob_gts_name.h"
#include <random>

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace common {
class ObMySQLProxy;
}

namespace rootserver {
class ObZoneManager;
class ObUnitManager;
class ObServerManager;

class RsGtsInstance {
public:
  RsGtsInstance() : gts_id_(common::OB_INVALID_ID), gts_name_(), region_(), tenant_id_array_()
  {}
  virtual ~RsGtsInstance()
  {}
  void reset();
  int assign(const RsGtsInstance& other);

public:
  uint64_t get_gts_id() const
  {
    return gts_id_;
  }
  void set_gts_id(const uint64_t gts_id)
  {
    gts_id_ = gts_id;
  }
  const common::ObGtsName& get_gts_name() const
  {
    return gts_name_;
  }
  void set_gts_name(const common::ObGtsName& gts_name)
  {
    gts_name_ = gts_name;
  }
  const common::ObRegion& get_region() const
  {
    return region_;
  }
  void set_region(const common::ObRegion& region)
  {
    region_ = region;
  }
  int insert_tenant(const uint64_t tenant_id);
  int try_remove_tenant(const uint64_t tenant_id);
  const common::ObIArray<uint64_t>& get_tenant_id_array() const
  {
    return tenant_id_array_;
  }

  TO_STRING_KV(K_(gts_id), K_(gts_name), K_(region), K_(tenant_id_array));

private:
  uint64_t gts_id_;
  common::ObGtsName gts_name_;
  common::ObRegion region_;
  // record all tenants served by this gts instance
  common::ObArray<uint64_t> tenant_id_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(RsGtsInstance);
};

class ObRsGtsManager {
public:
  ObRsGtsManager();
  virtual ~ObRsGtsManager();

public:
  int init(common::ObMySQLProxy* sql_proxy, rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr,
      rootserver::ObServerManager* server_mgr);
  int load();
  int create_gts_instance(const uint64_t gts_id, const common::ObGtsName& gts_name, const common::ObRegion& region,
      const common::ObMemberList& member_list, const common::ObAddr& standby);
  int alloc_gts_instance_for_tenant(const uint64_t tenant_id, const common::ObRegion& primary_region);
  int erase_tenant_gts(const uint64_t tenant_id);
  int upgrade_cluster_create_ha_gts_util();
  int check_tenant_ha_gts_exist(const uint64_t tenant_id, bool& ha_gts_exist);

private:
  int check_inner_stat();
  int pick_gts_instance_id(const common::ObRegion& primary_region, uint64_t& gts_id);
  int load_gts_instance(const common::ObIArray<common::ObGtsInfo>& gts_info_array);
  int load_tenant_gts_info(const common::ObIArray<common::ObGtsTenantInfo>& tenant_gts_info_array);
  int inner_create_gts_instance(
      const uint64_t gts_id, const common::ObGtsName& gts_name, const common::ObRegion& region);
  int inner_alloc_gts_instance_for_tenant(const uint64_t tenant_id, const uint64_t gts_id);
  int inner_erase_tenant_gts(const uint64_t tenant_id);
  int upgrade_cluster_create_region_ha_gts(const common::ObRegion& region);
  int upgrade_cluster_construct_region_gts_info(const common::ObRegion& region, const common::ObMemberList& member_list,
      const common::ObAddr& standby_server, common::ObGtsInfo& gts_info);
  int fetch_new_ha_gts_id(uint64_t& new_gts_id);

private:
  typedef common::hash::ObHashMap<uint64_t, RsGtsInstance*, common::hash::NoPthreadDefendMode> GtsInstanceMap;
  typedef common::hash::ObHashMap<uint64_t, uint64_t, common::hash::NoPthreadDefendMode> TenantGtsMap;
  typedef common::hash::ObHashMap<common::ObRegion, common::ObSEArray<uint64_t, 7>*,  // gts_id array
      common::hash::NoPthreadDefendMode>
      RegionGtsMap;
  const int64_t MAX_GTS_INSTANCE_CNT = common::OB_DEFAULT_TENANT_COUNT;
  const int64_t MAX_TENANT_CNT = common::OB_DEFAULT_TENANT_COUNT;

private:
  common::SpinRWLock lock_;
  rootserver::ObZoneManager* zone_mgr_;
  rootserver::ObUnitManager* unit_mgr_;
  rootserver::ObServerManager* server_mgr_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObPooledAllocator<RsGtsInstance> gts_instance_allocator_;
  common::ObPooledAllocator<common::ObSEArray<uint64_t, 7> > id_array_allocator_;
  share::ObGtsTableOperator gts_table_operator_;
  GtsInstanceMap gts_instance_map_;  // gts_id -> gts_instance *
  RegionGtsMap region_gts_map_;      // region->gts_id_array *
  TenantGtsMap tenant_gts_map_;      // tenant_id -> gts_id
  bool inited_;
  bool loaded_;
};

}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_RS_GTS_MANAGER_H_
