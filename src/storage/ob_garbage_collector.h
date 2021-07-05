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

#ifndef OCEANBASE_STORAGE_OB_GARBAGE_COLLECTOR_H_
#define OCEANBASE_STORAGE_OB_GARBAGE_COLLECTOR_H_

#include "common/ob_partition_key.h"
#include "lib/hash/ob_array_hash_map.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/net/ob_addr.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace obrpc {
class ObSrvRpcProxy;
}
namespace transaction {
class ObTransService;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace common {
class ObMySQLProxy;
}
namespace storage {
class ObIPartitionGroup;
class ObPartitionService;
class ObGarbageCollector : public share::ObThreadPool {
public:
  ObGarbageCollector();
  ~ObGarbageCollector();

public:
  int init(ObPartitionService* partition_service, transaction::ObTransService* trans_service,
      share::schema::ObMultiVersionSchemaService* schema_service, obrpc::ObSrvRpcProxy* rpc_proxy,
      common::ObMySQLProxy* sql_proxy, const common::ObAddr& self_addr);
  int start();
  void stop();
  void wait();
  void destroy();
  void run1();

private:
  class InsertPGFunctor;
  class QueryPGIsValidMemberFunctor;
  class QueryPGFlushedIlogIDFunctor;
  class ExecuteSchemaDropFunctor;

  enum NeedGCReason {
    NO_NEED_TO_GC,
    NOT_IN_LEADER_MEMBER_LIST,
    TENANT_SCHEMA_DROP,
    TENANT_FAIL_TO_CREATE,
    LEADER_PENDDING_SCHEMA_DROP,
    LEADER_PHYSICAL_SCHEMA_DROP,
    FOLLOWER_SCHEMA_DROP,
  };

  struct GCCandidate {
    common::ObPGKey pg_key_;
    NeedGCReason gc_reason_;

    TO_STRING_KV(K(pg_key_), K(gc_reason_));
  };

  struct PGOfflineIlogFlushedInfo {
    int64_t replica_num_;
    int64_t offline_ilog_flushed_replica_num_;
    uint64_t offline_log_id_;
    NeedGCReason gc_reason_;

    TO_STRING_KV(K(replica_num_), K(offline_ilog_flushed_replica_num_), K(offline_log_id_), K(gc_reason_));
  };

  typedef common::ObSEArray<GCCandidate, 16> ObGCCandidateArray;
  typedef common::ObLinearHashMap<common::ObAddr, common::ObPartitionArray> ServerPGMap;
  typedef common::ObArrayHashMap<common::ObPGKey, PGOfflineIlogFlushedInfo> PGOfflineIlogFlushedInfoMap;
  typedef common::hash::ObHashSet<uint64_t> TenantSet;

  static const int64_t GC_INTERVAL = 10 * 1000 * 1000;             // 10 seconds
  static const int64_t GC_SCHEMA_DROP_DELAY = 1800 * 1000 * 1000;  // 30 minutes
  static const int64_t LOG_ARCHIVE_DROP_DELAY = GC_SCHEMA_DROP_DELAY;

private:
  int gc_check_member_list_(ObGCCandidateArray& gc_candidates);
  int construct_server_pg_map_for_member_list_(ServerPGMap& server_pg_map) const;
  int handle_each_pg_for_member_list_(ServerPGMap& server_pg_map, ObGCCandidateArray& gc_candidates);

private:
  int gc_check_schema_(ObGCCandidateArray& gc_candidates, TenantSet& gc_tenant_set);
  int gc_check_schema_(storage::ObIPartitionGroup* pg, share::schema::ObSchemaGetterGuard& schema_guard,
      TenantSet& gc_tenant_set, NeedGCReason& gc_reason);

private:
  static bool is_gc_reason_leader_schema_drop_(const NeedGCReason& gc_reason);
  int execute_gc_except_leader_schema_drop_(const ObGCCandidateArray& gc_candidates);
  int execute_gc_for_leader_schema_drop_(const ObGCCandidateArray& gc_candidates);
  int extract_leader_schema_drop_gc_candidates_(
      const ObGCCandidateArray& gc_candidates, ObGCCandidateArray& leader_schema_drop_gc_candidates);
  int handle_leader_schema_drop_gc_candidates_(const ObGCCandidateArray& gc_candidates);
  int construct_server_pg_map_for_leader_schema_drop_(const ObGCCandidateArray& gc_candidates,
      ServerPGMap& server_pg_map, PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map);
  int handle_each_pg_for_leader_schema_drop_(
      ServerPGMap& server_pg_map, PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map);
  int handle_pg_offline_ilog_flushed_info_map_(PGOfflineIlogFlushedInfoMap& pg_offline_ilog_flushed_info_map);

  int execute_gc_tenant_tmp_file_(TenantSet& gc_tenant_set);
  bool check_gc_condition_() const;

private:
  int gc_check_pg_schema_(
      storage::ObIPartitionGroup* pg, share::schema::ObSchemaGetterGuard& schema_guard, NeedGCReason& gc_reason);
  int handle_schema_drop_pg_(storage::ObIPartitionGroup* pg, const bool is_physical_removed, NeedGCReason& gc_reason);
  int leader_handle_schema_drop_pg_(
      storage::ObIPartitionGroup* pg, const bool is_physical_removed, NeedGCReason& gc_reason);
  int follower_handle_schema_drop_pg_(storage::ObIPartitionGroup* pg, NeedGCReason& gc_reason);
  int gc_check_log_archive_(storage::ObIPartitionGroup* pg, const bool is_physical_removed, NeedGCReason& gc_reason);

private:
  int gc_check_partition_schema_(storage::ObIPartitionGroup* pg, share::schema::ObSchemaGetterGuard& schema_guard,
      const common::ObPartitionKey& partition_key);
  int handle_schema_drop_partition_(storage::ObIPartitionGroup* pg, const common::ObPartitionKey& partition_key);
  int leader_handle_schema_drop_partition_(storage::ObIPartitionGroup* pg, const common::ObPartitionKey& partition_key);
  int follower_handle_schema_drop_partition_(
      storage::ObIPartitionGroup* pg, const common::ObPartitionKey& partition_key);

private:
  int construct_server_pg_map_(
      ServerPGMap& server_pg_map, const common::ObAddr& server, const common::ObPGKey& pg_key) const;

private:
  bool is_inited_;
  ObPartitionService* partition_service_;
  transaction::ObTransService* trans_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* sql_proxy_;
  common::ObAddr self_addr_;
  int64_t seq_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGarbageCollector);
};
}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_GARBAGE_COLLECTOR_H_
