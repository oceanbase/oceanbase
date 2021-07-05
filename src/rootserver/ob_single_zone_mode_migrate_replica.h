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

#ifndef _OB_SINGLE_ZONE_MODE_MIGRATE_REPLICA_H
#define _OB_SINGLE_ZONE_MODE_MIGRATE_REPLICA_H 1

#include "ob_balance_info.h"
#include "ob_root_utils.h"
#include "rootserver/ob_unit_manager.h"
namespace oceanbase {
namespace common {
class ObServerConfig;
}
namespace share {
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
class ObTableSchema;
class ObTenantSchema;
}  // namespace schema
}  // namespace share

namespace rootserver {
namespace balancer {
class HashIndexCollection;
}

class ObSingleZoneModeMigrateReplica {
public:
  ObSingleZoneModeMigrateReplica();
  virtual ~ObSingleZoneModeMigrateReplica()
  {}
  int init(common::ObServerConfig& cfg, share::schema::ObMultiVersionSchemaService& schema_service,
      share::ObPartitionTableOperator& pt_operator, ObRebalanceTaskMgr& task_mgr, ObZoneManager& zone_mgr,
      TenantBalanceStat& tenant_stat, share::ObCheckStopProvider& check_stop_provider);
  int migrate_replica(int64_t& task_cnt);

private:
  int migrate_not_inner_table_replica(const Partition& p, const Partition*& first_migrate_p, Replica& dest_replica,
      bool& small_tenant, common::ObIArray<ObMigrateTaskInfo>& task_info_array, int64_t& task_cnt);
  int get_random_dest(const Partition& partition, const Replica& pr, Replica& dest_replica, Replica& dest);
  int do_migrate_replica(const Partition& partition, const Replica& replica, const Replica& dest_replica,
      const Partition*& first_migrate_p, ObReplicaMember& data_source, bool& small_tenant,
      common::ObIArray<ObMigrateTaskInfo>& task_info_array, int64_t& task_cnt, bool& do_accumulated);
  int try_accumulate_task_info(const bool small_tenant, common::ObIArray<ObMigrateTaskInfo>& task_info_array,
      const ObMigrateTaskInfo& task_info, const Partition* cur_partition, const Partition*& first_migrate_p,
      const Replica& dest_replica_migrate, int64_t& task_cnt, bool& do_accumulated);
  bool check_stop()
  {
    return check_stop_provider_->check_stop();
  }

private:
  bool inited_;
  common::ObServerConfig* config_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  ObRebalanceTaskMgr* task_mgr_;
  ObZoneManager* zone_mgr_;
  TenantBalanceStat* tenant_stat_;
  TenantBalanceStat* origin_tenant_stat_;
  share::ObCheckStopProvider* check_stop_provider_;
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif /* _OB_SINGLE_ZONE_MODE_MIGRATE_REPLICA_H */
