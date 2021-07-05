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

#ifndef _OB_SERVER_BALANCE_PLAN_H
#define _OB_SERVER_BALANCE_PLAN_H 1
#include "ob_unit_manager.h"
#include "ob_server_manager.h"
#include "ob_server_balancer.h"
#include "rootserver/ob_balance_info.h"
#include "rootserver/ob_unit_stat_manager.h"
#include "share/ob_unit_getter.h"
#include "share/ob_check_stop_provider.h"

namespace oceanbase {
namespace rootserver {
class ObRootBalancer;

struct ServerBalancePlanTask {
  ServerBalancePlanTask() : unit_id_(OB_INVALID_ID), zone_(), resource_pool_id_(OB_INVALID_ID), src_addr_(), dst_addr_()
  {}
  uint64_t unit_id_;
  common::ObZone zone_;
  uint64_t resource_pool_id_;
  common::ObAddr src_addr_;
  common::ObAddr dst_addr_;
  TO_STRING_KV(K(unit_id_), K(zone_), K(resource_pool_id_), K(src_addr_), K(dst_addr_));
};

class ServerManager : public ObServerManager {
public:
  ServerManager() : ObServerManager()
  {}
  virtual ~ServerManager()
  {}

public:
  virtual int get_servers_of_zone(
      const common::ObZone& zone, common::ObArray<common::ObAddr>& server_list) const override;
  virtual int get_server_status(const common::ObAddr& server, share::ObServerStatus& server_status) const override;

public:
  int clone(ObServerManager& server_mgr);
  int reduce_disk_use(const common::ObAddr& server, const int64_t size);
  int add_disk_use(const common::ObAddr& server, const int64_t size);

private:
  int find_server_status(const ObAddr& server, share::ObServerStatus*& status) const;
};

class UnitManager : public ObUnitManager {
public:
  UnitManager(ObServerManager& server_mgr, ObZoneManager& zone_mgr) : ObUnitManager(server_mgr, zone_mgr)
  {}
  virtual ~UnitManager()
  {}

private:
  int migrate_unit(const uint64_t unit_id, const common::ObAddr& dst, const bool is_manual = false) override;
  int end_migrate_unit(const uint64_t unit_id, const EndMigrateOp end_migrate_op = COMMIT) override;
};

class ServerBalancer : public ObServerBalancer {
public:
  ServerBalancer() : ObServerBalancer(), task_iter_idx_(0), task_array_(), unit_iter_idx_(0), unit_distribution_()
  {}
  virtual ~ServerBalancer()
  {}

public:
  int get_next_task(ServerBalancePlanTask& task);
  int get_next_unit_distribution(share::ObUnitInfo& unit);
  int generate_server_balance_plan(common::ObZone& zone);

private:
  struct UnitIdCmp {
    UnitIdCmp() : ret_(common::OB_SUCCESS)
    {}
    bool operator()(share::ObUnitInfo* left, share::ObUnitInfo* right)
    {
      bool bool_ret = false;
      if (OB_UNLIKELY(common::OB_SUCCESS != ret_)) {

      } else if (NULL == left || NULL == right) {
        ret_ = common::OB_ERR_UNEXPECTED;
      } else if (left->unit_.unit_id_ < right->unit_.unit_id_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
      return bool_ret;
    }
    int get_ret()
    {
      return ret_;
    }
    int ret_;
  };

private:
  virtual int do_migrate_unit_task(const common::ObIArray<ObServerBalancer::UnitMigrateStat>& task_array) override;
  virtual int do_migrate_unit_task(const common::ObIArray<ObServerBalancer::UnitMigrateStat*>& task_array) override;
  int get_all_zone_units(const common::ObZone& zone, common::ObIArray<share::ObUnitInfo>& units);
  int check_generate_balance_plan_finished(
      common::ObArray<share::ObUnitInfo>& before, common::ObArray<share::ObUnitInfo>& after, bool& finish);
  int do_generate_server_balance_plan(const common::ObZone& zone);
  int execute_migrate_unit(const share::ObUnit& unit, const ObServerBalancer::UnitMigrateStat& task);

private:
  int64_t task_iter_idx_;
  common::ObArray<ServerBalancePlanTask> task_array_;
  int64_t unit_iter_idx_;
  common::ObArray<share::ObUnitInfo> unit_distribution_;
};

class CheckStopProvider : public share::ObCheckStopProvider {
public:
  CheckStopProvider()
  {}
  virtual ~CheckStopProvider()
  {}
  virtual int check_stop() const
  {
    return common::OB_SUCCESS;
  }
};

class ObServerBalancePlan {
public:
  ObServerBalancePlan(ObUnitManager& unit_mgr, ObILeaderCoordinator& leader_coordinator, ObServerManager& server_mgr,
      ObZoneManager& zone_mgr)
      : unit_mgr_(unit_mgr),
        leader_coordinator_(leader_coordinator),
        server_mgr_(server_mgr),
        zone_mgr_(zone_mgr),
        stop_checker_(),
        unit_stat_getter_(),
        unit_stat_mgr_(),
        tenant_stat_(),
        fake_server_mgr_(),
        fake_unit_mgr_(server_mgr, zone_mgr),
        server_balancer_(),
        is_inited_(false),
        plan_generated_(false)
  {}
  virtual ~ObServerBalancePlan()
  {}

public:
  int init(common::ObMySQLProxy& proxy, common::ObServerConfig& server_config,
      share::schema::ObMultiVersionSchemaService& schema_service, ObRootBalancer& root_balance);
  int generate_server_balance_plan();
  int get_next_task(ServerBalancePlanTask& task);
  int get_next_unit_distribution(share::ObUnitInfo& unit);

private:
  int clone_fake_unit_mgr();

private:
  ObUnitManager& unit_mgr_;
  ObILeaderCoordinator& leader_coordinator_;
  ObServerManager& server_mgr_;
  ObZoneManager& zone_mgr_;
  CheckStopProvider stop_checker_;
  share::ObUnitStatGetter unit_stat_getter_;
  ObUnitStatManager unit_stat_mgr_;
  TenantBalanceStat tenant_stat_;
  ServerManager fake_server_mgr_;
  // In the process of generating ServerBalancePlan,
  // fake_unit_mgr_ will be reloaded from the entity table
  UnitManager fake_unit_mgr_;
  // ServerBalancer, as a derived class of ObServerBalancer,
  // saves the result of unit balance plan
  ServerBalancer server_balancer_;
  bool is_inited_;
  bool plan_generated_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObServerBalancePlan);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_SERVER_BALANCE_PLANH */
