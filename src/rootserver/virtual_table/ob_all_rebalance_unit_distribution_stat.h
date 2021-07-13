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

#ifndef _OB_ALL_REBALANCE_UNIT_DISTRIBUTION_STAT_H
#define _OB_ALL_REBALANCE_UNIT_DISTRIBUTION_STAT_H 1
#include "share/ob_virtual_table_projector.h"
#include "rootserver/ob_server_balance_plan.h"
#include "common/ob_unit_info.h"

namespace oceanbase {
namespace rootserver {
class ObRootBalancer;
class ObAllRebalanceUnitDistributionStat : public common::ObVirtualTableProjector {
public:
  ObAllRebalanceUnitDistributionStat(ObUnitManager& unit_mgr, ObILeaderCoordinator& leader_coordinator,
      ObServerManager& server_mgr, ObZoneManager& zone_mgr);
  virtual ~ObAllRebalanceUnitDistributionStat();

  int init(common::ObMySQLProxy& proxy, common::ObServerConfig& server_config,
      share::schema::ObMultiVersionSchemaService& schema_service, ObRootBalancer& root_balancer);
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  int get_full_row(const share::ObUnitInfo& unit_info, common::ObIArray<Column>& columns);
  int get_table_schema(uint64_t tid);

private:
  // data members
  ObServerBalancePlan server_balance_plan_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  char svr_ip_buf_[common::OB_IP_STR_BUFF];
  share::ObUnitInfo unit_info_;
  bool inited_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_ALL_REBALANCE_MAP_ITEM_STAT_H */
