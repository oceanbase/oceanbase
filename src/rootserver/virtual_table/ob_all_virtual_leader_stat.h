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

#ifndef _OB_ALL_VIRTUAL_LEADER_STAT_H_
#define _OB_ALL_VIRTUAL_LEADER_STAT_H_ 1
#include "share/ob_virtual_table_projector.h"
#include "rootserver/ob_leader_coordinator.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace rootserver {
class ObAllVirtualLeaderStat : public common::ObVirtualTableProjector {
public:
  ObAllVirtualLeaderStat()
      : schema_guard_(),
        all_tenants_(),
        cur_tenant_idx_(-1),
        columns_(),
        tenant_partition_(),
        partition_group_idx_(-1),
        candidate_leader_info_array_(),
        candidate_leader_info_idx_(-1),
        tenant_unit_(),
        partition_info_container_(),
        leader_coordinator_(NULL),
        schema_service_(NULL),
        table_schema_(NULL),
        inner_allocator_(),
        is_inited_(false)
  {}
  virtual ~ObAllVirtualLeaderStat()
  {}

public:
  int init(
      rootserver::ObLeaderCoordinator& leader_coordinator, share::schema::ObMultiVersionSchemaService& schema_service);
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;

private:
  // get all_virtual_leader_stat's table_schema
  int get_table_schema(uint64_t table_id);
  int get_all_tenant();
  int next();
  int next_candidate_leader();
  bool reach_candidate_leader_array_end();
  int next_partition_group();
  bool reach_tenant_end();
  int next_tenant();
  int get_row();
  int get_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns);
  int build_partition_group_candidate_leader_array();
  int construct_tenant_partition();
  int do_construct_tenant_partition(uint64_t tenant_id);

private:
  static const int64_t MAX_COLUMN_NUM = 32;  // all_virtual_leader_stat column number now will not be greater than 32
private:
  share::schema::ObSchemaGetterGuard schema_guard_;
  common::ObArray<uint64_t> all_tenants_;
  // cur_tenant_idx_ is current idx of all_tenants_;
  int64_t cur_tenant_idx_;
  common::ObSEArray<Column, MAX_COLUMN_NUM> columns_;
  rootserver::ObLeaderCoordinator::TenantPartition tenant_partition_;
  // partition_group_idx_ is current partition group idx of tenant_partitions_
  int64_t partition_group_idx_;
  common::ObArray<ObLeaderCoordinator::CandidateLeaderInfo> candidate_leader_info_array_;
  // idx of candidate_leader_info_array_
  int64_t candidate_leader_info_idx_;
  rootserver::ObLeaderCoordinator::TenantUnit tenant_unit_;
  rootserver::PartitionInfoContainer partition_info_container_;
  rootserver::ObLeaderCoordinator* leader_coordinator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  const share::schema::ObTableSchema* table_schema_;
  common::ObArenaAllocator inner_allocator_;
  bool is_inited_;

private:
  // disallow copy and assign
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLeaderStat);
};

}  // namespace rootserver
}  // namespace oceanbase
#endif  // _OB_ALL_VIRTUAL_LEADER_STAT_H_
