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

#ifndef _OB_ALL_REBALANCE_UNIT_STAT_H
#define _OB_ALL_REBALANCE_UNIT_STAT_H 1
#include "ob_all_rebalance_tenant_stat.h"
namespace oceanbase {
namespace rootserver {
class ObAllRebalanceUnitStat : public common::ObVirtualTableProjector {
public:
  ObAllRebalanceUnitStat();
  virtual ~ObAllRebalanceUnitStat();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, ObUnitManager& unit_mgr,
      ObServerManager& server_mgr, share::ObPartitionTableOperator& pt_operator,
      share::ObRemotePartitionTableOperator& remote_pt_operator, ObZoneManager& zone_mgr, ObRebalanceTaskMgr& task_mgr,
      share::ObCheckStopProvider& check_stop_provider);
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAllRebalanceUnitStat);
  // function members
  int get_row();
  int next();
  int get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns);

private:
  // data members
  ObAllRebalanceTenantStat impl_;
  int64_t cur_unit_idx_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_ALL_REBALANCE_UNIT_STAT_H */
