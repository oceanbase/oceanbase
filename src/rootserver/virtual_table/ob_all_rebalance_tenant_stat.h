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

#ifndef _OB_ALL_REBALANCE_TENANT_STAT_H
#define _OB_ALL_REBALANCE_TENANT_STAT_H 1
#include "share/ob_virtual_table_projector.h"
#include "rootserver/ob_balance_info.h"
#include "lib/container/ob_se_array.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObAllRebalanceUnitStat;
class ObAllRebalanceReplicaStat;

class ObAllRebalanceTenantStat : public common::ObVirtualTableProjector {
public:
  ObAllRebalanceTenantStat();
  virtual ~ObAllRebalanceTenantStat();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
      ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
      share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
      share::ObCheckStopProvider& check_stop_provider);
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  int get_table_schema(uint64_t tid);
  int get_all_tenant();
  int init_tenant_balance_stat(uint64_t tenant_id);
  int get_row();
  int next();
  int get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns);

private:
  // data members
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  TenantBalanceStat tenant_balance_stat_;

  common::ObSEArray<uint64_t, 16> all_tenants_;
  int64_t cur_tenant_idx_;
  int64_t cur_zone_idx_;
  share::schema::ObSchemaGetterGuard schema_guard_;
  const share::schema::ObTableSchema* table_schema_;
  common::ObSEArray<Column, 16> columns_;

  DISALLOW_COPY_AND_ASSIGN(ObAllRebalanceTenantStat);
  friend class ObAllRebalanceUnitStat;
  friend class ObAllRebalanceReplicaStat;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_ALL_REBALANCE_TENANT_STAT_H */
