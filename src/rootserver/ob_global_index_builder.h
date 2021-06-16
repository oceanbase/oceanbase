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

#ifndef OCEANBASE_ROOTSERVER_OB_GLOBAL_INDEX_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_GLOBAL_INDEX_BUILDER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "sql/ob_index_sstable_builder.h"
#include "common/ob_partition_key.h"
#include "share/ob_rpc_struct.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/schema/ob_schema_struct.h"
#include "ob_rs_async_rpc_proxy.h"
#include "ob_thread_idling.h"
#include "ob_rs_reentrant_thread.h"

namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
class ObPartitionReplica;
class ObPartitionInfo;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace common {
class ObMySQLProxy;
class ObReplicaMember;
namespace sqlclient {
class ObMySQLResult;
}
}  // namespace common
namespace obrpc {
class ObSrvRpcProxy;
}
namespace rootserver {
class ObServerManager;
class ObRebalanceTaskMgr;
class ObDDLService;
class ObZoneManager;
class ObRebalanceTask;
class ObFreezeInfoManager;

class ObGlobalIndexBuilderIdling : public ObThreadIdling {
public:
  explicit ObGlobalIndexBuilderIdling(volatile bool& stop) : ObThreadIdling(stop)
  {}
  virtual ~ObGlobalIndexBuilderIdling()
  {}

public:
  virtual int64_t get_idle_interval_us()
  {
    return 3600LL * 1000LL * 1000LL;
  }  // max_idle_time_us
};

enum GlobalIndexBuildStatus {
  GIBS_INVALID = 0,
  GIBS_BUILD_SINGLE_REPLICA,
  GIBS_MULTI_REPLICA_COPY,
  GIBS_UNIQUE_INDEX_CALC_CHECKSUM,
  GIBS_UNIQUE_INDEX_CHECK,
  GIBS_INDEX_BUILD_TAKE_EFFECT,
  GIBS_INDEX_BUILD_FAILED,
  GIBS_INDEX_BUILD_FINISH,
  GIBS_MAX,
};

enum UniqueCheckStat {
  UCS_INVALID = 0,
  UCS_NOT_MASTER,
  UCS_SUCCEED,
  UCS_ILLEGAL,
};

struct PartitionUniqueStat {
  PartitionUniqueStat() : pkey_(), unique_check_stat_(UCS_INVALID)
  {}
  void reset()
  {
    pkey_.reset();
    unique_check_stat_ = UCS_INVALID;
  }
  TO_STRING_KV(K_(pkey), K_(unique_check_stat));
  ObPartitionKey pkey_;
  UniqueCheckStat unique_check_stat_;
};

enum ColChecksumStat {
  CCS_INVALID = 0,
  CCS_NOT_MASTER,
  CCS_SUCCEED,
  CCS_FAILED,
};

struct PartitionColChecksumStat {
  PartitionColChecksumStat() : pkey_(), pgkey_(), col_checksum_stat_(CCS_INVALID), snapshot_(-1)
  {}
  void reset()
  {
    pkey_.reset();
    pgkey_.reset();
    col_checksum_stat_ = CCS_INVALID;
    snapshot_ = -1;
  }
  TO_STRING_KV(K_(pkey), K_(pgkey), K_(col_checksum_stat), K_(snapshot));
  ObPartitionKey pkey_;
  ObPartitionKey pgkey_;
  ColChecksumStat col_checksum_stat_;
  int64_t snapshot_;
};

enum CopyMultiReplicaStat {
  CMRS_INVALID = 0,
  CMRS_IDLE,
  CMRS_COPY_TASK_EXIST,
  CMRS_SUCCEED,
};

enum AllReplicaSSTableStat {
  ARSS_INVALID = 0,
  ARSS_ALL_REPLICA_FINISH,
  ARSS_COPY_MULTI_REPLICA_RETRY,
  ARSS_BUILD_SINGLE_REPLICA_RETRY,
};

struct PartitionSSTableBuildStat {
  PartitionSSTableBuildStat() : pkey_(), copy_multi_replica_stat_(CMRS_INVALID)
  {}
  TO_STRING_KV(K(pkey_), K(copy_multi_replica_stat_));
  common::ObPartitionKey pkey_;
  CopyMultiReplicaStat copy_multi_replica_stat_;
};

enum BuildSingleReplicaStat {
  BSRT_INVALID = 0,
  BSRT_INVALID_SNAPSHOT,
  BSRT_PRIMARY_KEY_DUPLICATE,
  BSRT_REPLICA_NOT_READABLE,
  BSRT_SUCCEED,
  BSRT_FAILED,
};

struct ObGlobalIndexTask {
public:
  ObGlobalIndexTask();
  virtual ~ObGlobalIndexTask();

  int get_partition_unique_check_stat(const common::ObPartitionKey& pkey, PartitionUniqueStat*& partition_unique_stat);
  int get_partition_sstable_build_stat(
      const common::ObPartitionKey& pkey, PartitionSSTableBuildStat*& partition_sstable_stat);
  int get_partition_col_checksum_stat(
      const common::ObPartitionKey& pkey, PartitionColChecksumStat*& partition_col_checksum_stat);

  TO_STRING_KV(K(tenant_id_), K(data_table_id_), K(index_table_id_), K(status_), K(snapshot_),
      K(major_sstable_exist_reply_ts_), K(checksum_snapshot_), K(schema_version_), K_(retry_cnt));
  static const int64_t MAX_RETRY_CNT = 3;

  uint64_t tenant_id_;
  uint64_t data_table_id_;
  uint64_t index_table_id_;
  GlobalIndexBuildStatus status_;
  int64_t snapshot_;
  int64_t major_sstable_exist_reply_ts_;
  int64_t checksum_snapshot_;
  int64_t schema_version_;
  // last ts the task was drived
  int64_t last_drive_ts_;
  // used in build single replica
  BuildSingleReplicaStat build_single_replica_stat_;
  // used in copy multi replica
  common::ObArray<PartitionSSTableBuildStat> partition_sstable_stat_array_;
  // used in unique index calc column checksum
  common::ObArray<PartitionColChecksumStat> partition_col_checksum_stat_array_;
  // used in unique stat check
  common::ObArray<PartitionUniqueStat> partition_unique_stat_array_;
  // lock for this struct
  mutable common::SpinRWLock lock_;
  int64_t retry_cnt_;
};

class ObGlobalIndexBuilder : public ObRsReentrantThread, public sql::ObIndexSSTableBuilder::ReplicaPicker {
  typedef common::hash::ObHashMap<uint64_t, ObGlobalIndexTask*>::iterator task_iterator;
  typedef common::hash::ObHashMap<uint64_t, ObGlobalIndexTask*>::const_iterator const_task_iterator;

public:
  ObGlobalIndexBuilder();
  virtual ~ObGlobalIndexBuilder();

public:
  virtual void run3() override;
  virtual int blocking_run() override
  {
    BLOCKING_RUN_IMPLEMENT();
  }
  int init(obrpc::ObSrvRpcProxy* rpc_proxy, common::ObMySQLProxy* mysql_proxy, rootserver::ObServerManager* server_mgr,
      share::ObPartitionTableOperator* pt_operator, rootserver::ObRebalanceTaskMgr* rebalance_task_mgr,
      rootserver::ObDDLService* ddl_service, share::schema::ObMultiVersionSchemaService* schema_service,
      rootserver::ObZoneManager* zone_mgr, rootserver::ObFreezeInfoManager* freeze_info_manager);
  int reload_building_indexes();
  int on_build_single_replica_reply(const uint64_t index_table_id, int64_t snapshot, int ret_code);
  int on_col_checksum_calculation_reply(
      const uint64_t index_table_id, const common::ObPartitionKey& pkey, const int ret_code);
  int on_check_unique_index_reply(const common::ObPartitionKey& pkey, const int ret_code, const bool is_unique);
  int on_copy_multi_replica_reply(const ObRebalanceTask& rebalance_task);
  virtual int pick_data_replica(const common::ObPartitionKey& pkey, const common::ObIArray<common::ObAddr>& previous,
      common::ObAddr& server) override;
  virtual int pick_index_replica(const common::ObPartitionKey& pkey, const common::ObIArray<common::ObAddr>& previous,
      common::ObAddr& server) override;
  int submit_build_global_index_task(const share::schema::ObTableSchema* index_schema);
  void stop() override;

private:
  static const int64_t BUILD_SINGLE_REPLICA_TIMEOUT = 2LL * 3600LL * 1000000LL;        // 2h
  static const int64_t COPY_MULTI_REPLICA_TIMEOUT = 2LL * 3600LL * 1000000LL;          // 2h
  static const int64_t UNIQUE_INDEX_CHECK_TIMEOUT = 2LL * 3600LL * 1000000LL;          // 2h
  static const int64_t UNIQUE_INDEX_CALC_CHECKSUM_TIMEOUT = 2LL * 3600LL * 1000000LL;  // 2h
  static const int64_t TASK_RETRY_TIME_INTERVAL_US = 10LL * 1000000LL;                 // 10s
  static const int64_t COPY_MULTI_REPLICA_RETRY_UPPER_LMT = 3;
  static const int64_t THREAD_CNT = 1;
  static const int64_t TASK_MAP_BUCKET_NUM = 256;
  static const int64_t GET_ASSOCIATED_SNAPSHOT_TIMEOUT = 9000000LL;                    // 9s
  static const int64_t MAX_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US = 120LL * 1000000LL;  // 120s
  static const int64_t MIN_WAIT_CHECK_SCHEMA_VERSION_INTERVAL_US = 20LL * 1000000LL;   // 20s
  static const int64_t TIME_INTERVAL_PER_PART_US = 50 * 1000;                          // 50ms
  static const int64_t WAIT_US = 500 * 1000;                                           // 500ms
  static const bool STATE_SWITCH_ARRAY[GIBS_MAX][GIBS_MAX];
  struct PartitionServer {
    PartitionServer() : pkey_(), server_()
    {}
    void reset()
    {
      pkey_.reset();
      server_.reset();
    }
    TO_STRING_KV(K(pkey_), K(server_));
    int set_server(const common::ObAddr& server)
    {
      int ret = common::OB_SUCCESS;
      if (!server.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(server));
      } else {
        server_ = server;
      }
      return ret;
    }
    int set_partition_key(const common::ObPartitionKey& pkey)
    {
      int ret = common::OB_SUCCESS;
      if (!pkey.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(pkey));
      } else {
        pkey_ = pkey;
      }
      return ret;
    }
    int set(const common::ObAddr& server, common::ObPartitionKey& pkey)
    {
      int ret = common::OB_SUCCESS;
      if (!server.is_valid() || !pkey.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(server), K(pkey));
      } else {
        server_ = server;
        pkey_ = pkey;
      }
      return ret;
    }
    int set(
        const common::ObAddr& server, const uint64_t table_id, const int64_t partition_id, const int64_t partition_cnt)
    {
      int ret = common::OB_SUCCESS;
      if (!server.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid argument", K(ret), K(server));
      } else if (OB_FAIL(pkey_.init(table_id, partition_id, partition_cnt))) {
        RS_LOG(WARN, "fail to init pkey", K(ret), K(table_id), K(partition_id), K(partition_cnt));
      } else {
        server_ = server;
      }
      return ret;
    }
    common::ObPartitionKey pkey_;
    common::ObAddr server_;
  };

private:
  int try_drive(ObGlobalIndexTask* task);
  int get_task_count_in_lock(int64_t& task_cnt);
  int fill_global_index_task_result(common::sqlclient::ObMySQLResult* result, ObGlobalIndexTask* task);
  int check_and_get_index_schema(share::schema::ObSchemaGetterGuard& schema_guard, const uint64_t index_table_id,
      const share::schema::ObTableSchema*& index_schema, bool& index_schema_exist);
  int get_global_index_build_snapshot(ObGlobalIndexTask* task, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema* index_schema, int64_t& snapshot);
  int hold_snapshot(const ObGlobalIndexTask* task, const int64_t snapshot);
  int release_snapshot(const ObGlobalIndexTask* task);
  int init_build_snapshot_ctx(const common::ObIArray<PartitionServer>& partition_leader_array,
      common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array);
  template <typename PROXY>
  int update_build_snapshot_ctx(PROXY& proxy, const common::ObIArray<int>& ret_code_array,
      common::ObIArray<int64_t>& invalid_snapshot_id_array, common::ObIArray<int64_t>& snapshot_array);
  int update_partition_leader_array(common::ObIArray<PartitionServer>& partition_leader_array,
      const common::ObIArray<int>& ret_code_array, const common::ObIArray<int64_t>& invalid_snapshot_id_array);
  template <typename PROXY, typename ARG>
  int do_get_associated_snapshot(PROXY& rpc_proxy, ARG& rpc_arg, ObGlobalIndexTask* task, const int64_t all_part_num,
      common::ObIArray<PartitionServer>& partition_leader_array, int64_t& snapshot);
  int generate_original_table_partition_leader_array(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema* data_schema, common::ObIArray<PartitionServer>& partition_leader_array);
  int update_task_global_index_build_snapshot(ObGlobalIndexTask* task, const int64_t snapshot);
  int pick_build_snapshot(const common::ObIArray<int64_t>& snapshot_array, int64_t& snapshot);
  int switch_state(ObGlobalIndexTask* task, const GlobalIndexBuildStatus next_status);

  int drive_this_unique_index_calc_checksum(const share::schema::ObTableSchema* index_schema,
      const share::schema::ObTableSchema* data_schema, share::schema::ObSchemaGetterGuard& schema_guard,
      ObGlobalIndexTask* task);
  int get_checksum_calculation_snapshot(ObGlobalIndexTask* task, share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema* index_schema, int64_t& checksum_snapshot);
  int build_task_partition_col_checksum_stat(const share::schema::ObTableSchema* index_schema,
      const share::schema::ObTableSchema* data_schema, ObGlobalIndexTask* task);
  int send_col_checksum_calc_rpc(const share::schema::ObTableSchema* index_schema, const int64_t schema_version,
      const int64_t checksum_snapshot, const uint64_t execution_id, const common::ObPartitionKey& pkey,
      const share::ObPartitionReplica* replica);
  int send_checksum_calculation_request(const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task);
  int do_checksum_calculation(ObGlobalIndexTask* task, const share::schema::ObTableSchema* index_schema,
      const share::schema::ObTableSchema* data_schema);
  int launch_new_unique_index_calc_checksum(const share::schema::ObTableSchema* index_schema,
      const share::schema::ObTableSchema* data_schema, share::schema::ObSchemaGetterGuard& schema_guard,
      ObGlobalIndexTask* task);
  int send_check_unique_index_request(const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task);
  int send_check_unique_index_rpc(const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task,
      const common::ObPartitionKey& pkey, const share::ObPartitionReplica* replica);
  int build_task_partition_unique_stat(const share::schema::ObTableSchema* schema, ObGlobalIndexTask* task);
  int drive_this_unique_index_check(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task);
  int launch_new_unique_index_check(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task);
  int drive_this_build_single_replica(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task);
  int launch_new_build_single_replica(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task);
  int check_partition_copy_replica_stat(int64_t& major_sstable_exist_reply_ts, share::ObPartitionInfo& partition_info,
      AllReplicaSSTableStat& all_replica_sstable_stat);
  int build_replica_sstable_copy_task(
      PartitionSSTableBuildStat& part_sstable_build_stat, share::ObPartitionInfo& partition_info);
  int drive_this_copy_multi_replica(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task);
  int launch_new_copy_multi_replica(const share::schema::ObTableSchema* index_schema,
      share::schema::ObSchemaGetterGuard& schema_guard, ObGlobalIndexTask* task);
  int build_task_partition_sstable_stat(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task);
  int generate_task_partition_sstable_array(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task);
  int try_update_index_status_in_schema(const share::schema::ObTableSchema* index_schema, ObGlobalIndexTask* task,
      const share::schema::ObIndexStatus new_status);
  int do_build_single_replica(
      ObGlobalIndexTask* task, const share::schema::ObTableSchema* index_schema, const int64_t snapshot);

private:
  int try_build_single_replica(ObGlobalIndexTask* task);
  int try_copy_multi_replica(ObGlobalIndexTask* task);
  int try_unique_index_calc_checksum(ObGlobalIndexTask* task);
  int try_unique_index_check(ObGlobalIndexTask* task);
  int try_handle_index_build_take_effect(ObGlobalIndexTask* task);
  int try_handle_index_build_failed(ObGlobalIndexTask* task);
  int try_handle_index_build_finish(ObGlobalIndexTask* task);

private:
  int clear_intermediate_result(ObGlobalIndexTask* task);
  int reset_run_condition();
  int check_task_dropped(const ObGlobalIndexTask& task, bool& is_dropped);
  int delete_task_record(const ObGlobalIndexTask& task);

private:
  bool inited_;
  bool loaded_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  common::ObMySQLProxy* mysql_proxy_;
  rootserver::ObServerManager* server_mgr_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  share::ObPartitionTableOperator* pt_operator_;
  rootserver::ObRebalanceTaskMgr* rebalance_task_mgr_;
  rootserver::ObDDLService* ddl_service_;
  rootserver::ObZoneManager* zone_mgr_;
  rootserver::ObFreezeInfoManager* freeze_info_mgr_;
  common::ObPooledAllocator<ObGlobalIndexTask> task_allocator_;
  common::hash::ObHashMap<uint64_t, ObGlobalIndexTask*> task_map_;
  // task map lock, when modify the map per se, for example insert, delete, exclusive lock
  // when modify items of map, inclusive lock, for example update the ObGlobalIndexTask item
  common::SpinRWLock task_map_lock_;
  ObGlobalIndexBuilderIdling idling_;
};
}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_GLOBAL_INDEX_BUILDER_H_
