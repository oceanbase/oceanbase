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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_REPLICA_TASK_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_REPLICA_TASK_H_

#include "lib/container/ob_array.h"
#include "share/ob_virtual_table_projector.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_check_stop_provider.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_rebalance_task_mgr.h"
#include "rootserver/ob_balance_info.h"
#include "rootserver/ob_locality_checker.h"
#include "rootserver/ob_rereplication.h"
#include "rootserver/ob_root_utils.h"

namespace oceanbase {
namespace share {
class ObPartitionInfo;
class ObPartitionTableIterator;
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObServerManager;

class ObAllReplicaTask : public common::ObVirtualTableProjector {
public:
  ObAllReplicaTask();
  virtual ~ObAllReplicaTask();

  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObRemotePartitionTableOperator& remote_pt_operator, rootserver::ObServerManager& server_mgr,
      rootserver::ObUnitManager& unit_mgr, rootserver::ObZoneManager& zone_mgr,
      rootserver::ObRebalanceTaskMgr& rask_mgr, share::ObCheckStopProvider& check_stop_provider);

  virtual int inner_open();

  virtual int inner_get_next_row(common::ObNewRow*& row);

protected:
  virtual int get_condition(uint64_t& specific_tenant_id);

private:
  int get_full_row(const share::schema::ObTableSchema* table, ObReplicaTask& task, common::ObIArray<Column>& columns);
  bool inited_;
  TenantBalanceStat tenant_stat_;
  rootserver::ObRereplication rereplication_;
  rootserver::ObLocalityChecker locality_checker_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObArray<ObReplicaTask> results_;
  const share::schema::ObTableSchema* table_schema_;
  ObArenaAllocator arena_allocator_;
  int64_t index_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllReplicaTask);
};

class ObAllReplicaTaskI1 : public ObAllReplicaTask {
public:
  ObAllReplicaTaskI1();
  virtual ~ObAllReplicaTaskI1();

protected:
  virtual int get_condition(uint64_t& specific_tenant_id) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllReplicaTaskI1);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ALL_REPLICA_TASK_H_
