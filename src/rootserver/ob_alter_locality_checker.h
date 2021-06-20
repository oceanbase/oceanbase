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

#ifndef _OCEANBASE_ROOTSERVER_OB_ALTER_LOCALITY_CHECKER_H_
#define _OCEANBASE_ROOTSERVER_OB_ALTER_LOCALITY_CHECKER_H_ 1
#include "share/ob_define.h"
#include "lib/net/ob_addr.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace obrpc {
class ObCommonRpcProxy;
}
namespace share {
class ObPartitionTableOperator;
class ObPartitionInfo;
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
class ITenantStatFinder;
}  // namespace balancer
class ObLeaderCoordinator;
class ObServerManager;
class ObUnitManager;
class ObZoneManager;
class TenantBalanceStat;

struct ObCommitAlterTenantLocalityArg : public obrpc::ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCommitAlterTenantLocalityArg() : tenant_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_;
  }
  TO_STRING_KV(K_(tenant_id));

  uint64_t tenant_id_;
};

struct ObCommitAlterTablegroupLocalityArg : public obrpc::ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCommitAlterTablegroupLocalityArg() : tablegroup_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tablegroup_id_;
  }
  TO_STRING_KV(K_(tablegroup_id));

  uint64_t tablegroup_id_;
};

struct ObCommitAlterTableLocalityArg : public obrpc::ObDDLArg {
  OB_UNIS_VERSION(1);

public:
  ObCommitAlterTableLocalityArg() : table_id_(common::OB_INVALID_ID)
  {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != table_id_;
  }
  TO_STRING_KV(K_(table_id));

  uint64_t table_id_;
};

class ObAlterLocalityChecker {
public:
  explicit ObAlterLocalityChecker(volatile bool& is_stop)
      : is_stop_(is_stop),
        schema_service_(NULL),
        leader_coordinator_(NULL),
        common_rpc_proxy_(NULL),
        pt_operator_(NULL),
        server_mgr_(NULL),
        self_(),
        zone_mgr_(NULL),
        unit_mgr_(NULL),
        has_locality_modification_(true),
        is_inited_(false)
  {}
  virtual ~ObAlterLocalityChecker()
  {}

public:
  int init(share::schema::ObMultiVersionSchemaService* schema_service,
      rootserver::ObLeaderCoordinator* leader_coordinator, obrpc::ObCommonRpcProxy* common_rpc_proxy,
      share::ObPartitionTableOperator* pt_operator, rootserver::ObServerManager* server_mgr, common::ObAddr& addr,
      rootserver::ObZoneManager* zone_mgr, rootserver::ObUnitManager* unit_mgr);

public:
  int check_alter_locality_finished(const uint64_t tenant_id,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat);
  void notify_locality_modification();
  bool has_locality_modification() const;

private:
  int try_compensate_zone_locality(
      const bool compensate_readonly_all_server, common::ObIArray<share::ObZoneReplicaNumSet>& zone_locality);
  int get_readonly_all_server_compensation_mode(
      share::schema::ObSchemaGetterGuard& guard, const uint64_t schema_id, bool& compensate_readonly_all_server);
  int check_alter_locality_finished_by_tenant(const uint64_t tenant_id,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
      bool& is_finished);
  int process_single_table(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const share::schema::ObTenantSchema& tenant_schema,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
      bool& null_locality_table_finished, bool& table_locality_finished);
  int process_single_binding_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
      bool& tablegroup_locality_finished);
  int process_single_non_binding_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
      bool& tablegroup_locality_finished);
  int process_single_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTablegroupSchema& tablegroup_schema,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
      bool& tablegroup_locality_finished);
  int process_single_table_under_new_tablegroup(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, const share::schema::ObTablegroupSchema& tablegroup_schema,
      const balancer::HashIndexCollection& hash_index_collection, rootserver::TenantBalanceStat& tenant_stat,
      bool& table_locality_finished);
  int check_stop();
  int check_partition_entity_locality_distribution(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObPartitionSchema& partition_schema, const share::schema::ZoneLocalityIArray& zone_locality,
      const common::ObIArray<common::ObZone>& zone_list, const balancer::HashIndexCollection& hash_index_collection,
      rootserver::TenantBalanceStat& tenant_stat, bool& locality_match);
  int check_locality_match_replica_distribution(const share::schema::ZoneLocalityIArray& zone_locality,
      const common::ObIArray<common::ObZone>& zone_list, const share::ObPartitionInfo& info,
      bool& table_locality_match);
  int check_partition_quorum_match(
      const share::ObPartitionInfo& partition, const int64_t paxos_num, bool& locality_match);

private:
  static const int64_t GET_SCHEMA_INTERVAL = 10 * 1000;  // 10ms
  static const int64_t GET_SCHEMA_RETRY_LIMIT = 3;

private:
  const volatile bool& is_stop_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  rootserver::ObLeaderCoordinator* leader_coordinator_;
  obrpc::ObCommonRpcProxy* common_rpc_proxy_;
  share::ObPartitionTableOperator* pt_operator_;
  rootserver::ObServerManager* server_mgr_;
  common::ObAddr self_;
  rootserver::ObZoneManager* zone_mgr_;
  rootserver::ObUnitManager* unit_mgr_;
  bool has_locality_modification_;
  bool is_inited_;
};
}  // end of namespace rootserver
}  // end of namespace oceanbase
#endif
