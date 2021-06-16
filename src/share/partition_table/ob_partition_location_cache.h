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

#ifndef OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_CACHE_H_
#define OCEANBASE_SHARE_PARTITION_TABLE_OB_PARTITION_LOCATION_CACHE_H_

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/queue/ob_dedup_queue.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_inner_config_root_addr.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_root_addr_agent.h"
#include "lib/container/ob_iarray.h"
#include "observer/ob_uniq_task_queue.h"
#include "ob_partition_location.h"

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObPartitionKey;
class ObTimeoutCtx;
}  // end namespace common

namespace obrpc {
class ObCommonRpcProxy;
class ObSrvRpcProxy;
}  // namespace obrpc

namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
class ObRemoteServerProvider;
class ObRsMgr;
class ObPartitionTableOperator;
class ObRemotePartitionTableOperator;
class ObIAliveServerTracer;
class ObILocalityManager;
class ObLocationUpdateTask;

struct ObLocationCacheKey : public common::ObIKVCacheKey {
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t cluster_id_;

  ObLocationCacheKey();
  ObLocationCacheKey(const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id);
  ObLocationCacheKey(const uint64_t table_id, const int64_t partition_id);
  ~ObLocationCacheKey()
  {}
  virtual bool operator==(const ObIKVCacheKey& other) const;
  virtual bool operator!=(const ObIKVCacheKey& other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual uint64_t hash_v2() const;
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const;
  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(cluster_id));
};

struct LocationInfo {
public:
  LocationInfo() : server_(), renew_ts_(0)
  {}
  LocationInfo(const common::ObAddr& server, const int64_t renew_ts) : server_(server), renew_ts_(renew_ts)
  {}
  virtual ~LocationInfo()
  {}

public:
  bool is_valid() const
  {
    return server_.is_valid() && renew_ts_ > 0;
  }
  TO_STRING_KV(K_(server), K_(renew_ts));

public:
  int assign(const LocationInfo& that)
  {
    int ret = common::OB_SUCCESS;
    server_ = that.server_;
    renew_ts_ = that.renew_ts_;
    return ret;
  }
  int assign(const common::ObAddr& server, const int64_t renew_ts)
  {
    int ret = common::OB_SUCCESS;
    server_ = server;
    renew_ts_ = renew_ts;
    return ret;
  }
  void set(const LocationInfo& that)
  {
    server_ = that.server_;
    renew_ts_ = that.renew_ts_;
  }
  void set(const common::ObAddr& server, const int64_t renew_ts)
  {
    server_ = server;
    renew_ts_ = renew_ts;
  }

public:
  common::ObAddr server_;
  int64_t renew_ts_;
};

class ObLocationLeader {
public:
  ObLocationLeader() : key_(), leader_info_()
  {}
  virtual ~ObLocationLeader()
  {}
  const ObLocationCacheKey& get_key() const
  {
    return key_;
  }
  const LocationInfo& get_strong_leader_info() const
  {
    return leader_info_;
  }
  void set_key(const ObLocationCacheKey& key)
  {
    key_ = key;
  }
  void set_strong_leader_info(const LocationInfo& that)
  {
    leader_info_.server_ = that.server_;
    leader_info_.renew_ts_ = that.renew_ts_;
  }
  TO_STRING_KV(K_(key), K_(leader_info));

private:
  ObLocationCacheKey key_;
  LocationInfo leader_info_;
};

struct ObLocationCacheValue : public common::ObIKVCacheValue {
  int64_t size_;
  char* buffer_;
  ObLocationCacheValue();
  ~ObLocationCacheValue()
  {}
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const;
  TO_STRING_KV(K_(size), "buffer", reinterpret_cast<int64_t>(buffer_));
};

struct ObTenantSqlRenewStat {
public:
  uint64_t tenant_id_;
  int64_t start_sql_renew_ts_;
  int64_t sql_renew_count_;
  common::ObSpinLock lock_;
  ObTenantSqlRenewStat()
  {
    reset();
  }
  ~ObTenantSqlRenewStat()
  {}
  int assign(const ObTenantSqlRenewStat& other);
  void reset();
  void init(const uint64_t tenant_id);
  void inc()
  {
    sql_renew_count_++;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_TENANT_ID != tenant_id_;
  }
  TO_STRING_KV(K_(tenant_id), K_(start_sql_renew_ts), K_(sql_renew_count));
};

// TODO: () release memory of ObTenantSqlRenewStat when tenant has been dropped.
typedef common::hash::ObHashMap<uint64_t, ObTenantSqlRenewStat*> TenantSqlRenewInfoMap;
typedef common::ObArray<ObTenantSqlRenewStat> TenantSqlRenewInfoArray;
typedef common::ObIArray<ObTenantSqlRenewStat> TenantSqlRenewInfoIArray;

class ObTenantStatGuard {
public:
  ObTenantStatGuard() = delete;
  ObTenantStatGuard(const uint64_t tenant_id, TenantSqlRenewInfoMap& map,
      common::ObCachedAllocator<ObTenantSqlRenewStat>& object_pool, common::ObSpinLock& lock)
      : tenant_stat_(NULL), tenant_id_(common::OB_INVALID_TENANT_ID)
  {
    (void)set_tenant_stat(tenant_id, map, object_pool, lock);
  }
  ~ObTenantStatGuard()
  {
    if (NULL != tenant_stat_) {
      tenant_stat_->lock_.unlock();
      tenant_stat_ = NULL;
    }
  }

  ObTenantSqlRenewStat* get_tenant_stat()
  {
    return tenant_stat_;
  }
  TO_STRING_KV(KP_(tenant_stat), K_(tenant_id));

private:
  int set_tenant_stat(const uint64_t tenant_id, TenantSqlRenewInfoMap& map,
      common::ObCachedAllocator<ObTenantSqlRenewStat>& object_pool, common::ObSpinLock& lock);

private:
  ObTenantSqlRenewStat* tenant_stat_;
  uint64_t tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantStatGuard);
};

class ObIPartitionLocationCache;
// partition table update task
class ObLocationAsyncUpdateTask : public common::ObDLinkBase<ObLocationAsyncUpdateTask> {
public:
  friend class ObLocationFetcher;
  static const int64_t WAIT_PROCESS_WARN_TIME = 3 * 1000 * 1000;  // 3s
  enum Type { MODE_AUTO = 0, MODE_SQL_ONLY = 1 };
  ObLocationAsyncUpdateTask();
  ObLocationAsyncUpdateTask(ObIPartitionLocationCache& loc_cache, const uint64_t table_id, const int64_t partition_id,
      const int64_t add_timestamp, const int64_t cluster_id, const Type type);

  virtual ~ObLocationAsyncUpdateTask(){};

  bool is_valid() const;
  bool need_process_alone() const;
  void check_task_status() const;

  virtual int64_t hash() const;
  virtual bool operator==(const ObLocationAsyncUpdateTask& other) const;
  bool operator<(const ObLocationAsyncUpdateTask& other) const;
  virtual bool compare_without_version(const ObLocationAsyncUpdateTask& other) const;
  uint64_t get_group_id() const;
  bool is_barrier() const;

  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  inline int64_t get_partition_id() const
  {
    return partition_id_;
  }
  inline uint64_t get_tenant_id() const
  {
    return extract_tenant_id(table_id_);
  }
  inline int64_t get_cluster_id() const
  {
    return cluster_id_;
  }
  inline int64_t get_add_timestamp() const
  {
    return add_timestamp_;
  }
  inline Type get_type() const
  {
    return type_;
  }
  bool need_discard() const;
  TO_STRING_KV(KT_(table_id), K_(partition_id), K_(add_timestamp), K_(cluster_id), K_(type));

private:
  ObIPartitionLocationCache* loc_cache_;
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t add_timestamp_;
  int64_t cluster_id_;
  Type type_;
};

///////////////////////////////////////////
class ObIPartitionLocationCache {
public:
  enum PartitionLocationCacheType {
    PART_LOC_CACHE_TYPE_INVALID = 0,
    PART_LOC_CACHE_TYPE_NORMAL,
    PART_LOC_CACHE_TYPE_SQL,
    PART_LOC_CACHE_TYPE_OPTIMIZER,
  };

public:
  virtual ~ObIPartitionLocationCache()
  {}

  virtual ObIPartitionLocationCache::PartitionLocationCacheType get_type() const = 0;

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true) = 0;

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int force_sql_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit)
  {
    UNUSEDx(table_id, partition_id, location, expire_renew_time, is_cache_hit);
    return common::OB_NOT_SUPPORTED;
  }

  // get partition location of a partition
  // return OB_LOCATION_NOT_EXIST if not record in partition table
  virtual int get(const common::ObPartitionKey& partition, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit) = 0;

  // get all partition locations of a table, virtual table should set partition_num to 1
  // return OB_LOCATION_NOT_EXIST if some partition doesn't have record in partition table
  virtual int get(const uint64_t table_id, common::ObIArray<ObPartitionLocation>& locations,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true) = 0;

  // get leader addr of a partition, return OB_LOCATION_NOT_EXIST if not replica in partition
  // table, return OB_LOCATION_LEADER_NOT_EXIST if leader not exist
  virtual int get_strong_leader(
      const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false) = 0;

  virtual int get_leader_by_election(
      const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false)
  {
    UNUSEDx(partition, leader, force_renew);
    return common::OB_NOT_SUPPORTED;
  }

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) = 0;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const common::ObPartitionKey& partition, ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) = 0;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache,
  // return OB_LOCATION_LEADER_NOT_EXIST if partition's location in cache, but leader not exist
  virtual int nonblock_get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& leader) = 0;

  virtual int nonblock_get_restore_leader(const ObPartitionKey& partition, ObAddr& leader)
  {
    UNUSEDx(partition, leader);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int nonblock_get_leader_by_election(const ObPartitionKey& partition, ObAddr& leader)
  {
    UNUSEDx(partition, leader);
    return common::OB_NOT_SUPPORTED;
  }

  // trigger a location update task and clear location in cache
  virtual int nonblock_renew(const common::ObPartitionKey& partition, const int64_t expire_renew_time,
      const int64_t specific_cluster_id = common::OB_INVALID_ID) = 0;

  // get leader of partition across cluster
  virtual int get_leader_across_cluster(const common::ObPartitionKey& partition, common::ObAddr& leader,
      const int64_t cluster_id, const bool force_renew = false)
  {
    UNUSEDx(partition, leader, cluster_id, force_renew);
    return common::OB_NOT_SUPPORTED;
  }

  // get partition across cluster
  virtual int get_across_cluster(const common::ObPartitionKey& partition, const int64_t expire_renew_time,
      ObPartitionLocation& location, const int64_t specific_cluster_id = common::OB_INVALID_ID)
  {
    UNUSEDx(partition, expire_renew_time, location, specific_cluster_id);
    return common::OB_NOT_SUPPORTED;
  }

  // get leader of partition across cluster which won't trigger renew_location().
  virtual int nonblock_get_strong_leader_without_renew(const common::ObPartitionKey& partition, common::ObAddr& leader,
      const int64_t specific_cluster_id = common::OB_INVALID_ID)
  {
    UNUSEDx(partition, leader, specific_cluster_id);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int nonblock_get_leader_by_election_without_renew(const common::ObPartitionKey& partition,
      common::ObAddr& leader, const int64_t specific_cluster_id = common::OB_INVALID_ID)
  {
    UNUSEDx(partition, leader, specific_cluster_id);
    return common::OB_NOT_SUPPORTED;
  }

  // try to trigger a location update task and clear location in cache,
  // if it is limited by the limiter and not be done, is_limited will be set to true
  virtual int nonblock_renew_with_limiter(
      const common::ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited) = 0;
  // link table.
  virtual int get_link_table_location(const uint64_t table_id, ObPartitionLocation& location) = 0;
  virtual int batch_process_tasks(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks, bool& stopped)
  {
    UNUSEDx(tasks, stopped);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int process_barrier(const ObLocationAsyncUpdateTask& task, bool& stopped)
  {
    UNUSEDx(task, stopped);
    return common::OB_NOT_SUPPORTED;
  }

  // FIXME:() to be removed
  virtual int64_t get_primary_cluster_id() const
  {
    return common::OB_INVALID_ID;
  }
};

struct ObLocationRpcRenewInfo {
public:
  ObLocationRpcRenewInfo();
  ~ObLocationRpcRenewInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(addr), "key_cnt", OB_ISNULL(arg_) ? 0 : arg_->keys_.count(), "idx_cnt",
      OB_ISNULL(idx_array_) ? 0 : idx_array_->count());

public:
  ObAddr addr_;
  obrpc::ObLocationRpcRenewArg* arg_;
  common::ObArray<int64_t>* idx_array_;
};

struct ObReplicaRenewKey {
public:
  ObReplicaRenewKey();
  ObReplicaRenewKey(const uint64_t table_id, const int64_t partition_id, const ObAddr& addr);
  ~ObReplicaRenewKey();

  uint64_t hash() const;
  bool operator==(const ObReplicaRenewKey& other) const;
  TO_STRING_KV(K_(table_id), K_(partition_id), K_(addr));

public:
  uint64_t table_id_;
  int64_t partition_id_;
  ObAddr addr_;
};

class ObILocationFetcher {
public:
  virtual ~ObILocationFetcher()
  {}
  virtual int fetch_location(
      const uint64_t table_id, const int64_t partiton_id, const int64_t cluster_id, ObPartitionLocation& location) = 0;
  // fetch virtual table locations
  virtual int fetch_vtable_location(const uint64_t table_id, common::ObSArray<ObPartitionLocation>& locations) = 0;
  virtual int renew_location_with_rpc_v2(
      common::hash::ObHashMap<ObReplicaRenewKey, const obrpc::ObMemberListAndLeaderArg*>* result_map,
      const ObPartitionLocation& cached_location, ObPartitionLocation& new_location, bool& is_new_location_valid) = 0;
  virtual int batch_renew_sys_table_location_by_rpc(const ObPartitionLocation& core_table_location,
      const common::ObIArray<ObLocationCacheKey>& keys, common::ObIArray<ObLocationLeader>& results) = 0;
  virtual bool treat_sql_as_timeout(int error_code);
  virtual int batch_fetch_location(const common::ObIArray<common::ObPartitionKey>& keys, const int64_t cluster_id,
      common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations) = 0;
  virtual int batch_renew_location_with_rpc(const common::ObIArray<const ObPartitionLocation*>& rpc_locations,
      common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations) = 0;
  static uint64_t get_rpc_tenant_id(const uint64_t table_id);

protected:
  virtual int partition_table_fetch_location(ObPartitionTableOperator* pt, const int64_t cluster_id,
      const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location);
  virtual int partition_table_batch_fetch_location(ObPartitionTableOperator* pt, const int64_t cluster_id,
      const common::ObIArray<common::ObPartitionKey>& keys, common::ObIAllocator& allocator,
      common::ObIArray<ObPartitionLocation*>& new_locations);
  virtual int fill_location(ObPartitionInfo& partition_info, ObPartitionLocation& location);
};

// used by observer
class ObLocationFetcher : public ObILocationFetcher {
public:
  static const int64_t OB_FETCH_LOCATION_TIMEOUT = 1 * 1000 * 1000;           // 1s
  static const int64_t OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT = 500 * 1000;  // 500ms
  ObLocationFetcher();
  virtual ~ObLocationFetcher();
  int init(common::ObServerConfig& config, share::ObPartitionTableOperator& pt,
      share::ObRemotePartitionTableOperator& remote_pt, ObRsMgr& rs_mgr, obrpc::ObCommonRpcProxy& rpc_proxy,
      obrpc::ObSrvRpcProxy& srv_rpc_proxy, ObILocalityManager* locality_manager,
      // ObIRemoteLocatonGetter *remote_location_getter,
      const int64_t cluster_id);
  virtual int fetch_location(const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id,
      ObPartitionLocation& location) override;
  virtual int fetch_vtable_location(const uint64_t table_id, common::ObSArray<ObPartitionLocation>& locations) override;
  virtual int renew_location_with_rpc_v2(
      common::hash::ObHashMap<ObReplicaRenewKey, const obrpc::ObMemberListAndLeaderArg*>* result_map,
      const ObPartitionLocation& cached_location, ObPartitionLocation& new_location,
      bool& is_new_location_valid) override;
  virtual int batch_renew_sys_table_location_by_rpc(const ObPartitionLocation& core_table_location,
      const common::ObIArray<ObLocationCacheKey>& keys, common::ObIArray<ObLocationLeader>& results) override;

  virtual int batch_fetch_location(const common::ObIArray<common::ObPartitionKey>& keys, const int64_t cluster_id,
      common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations) override;
  virtual int batch_renew_location_with_rpc(const common::ObIArray<const ObPartitionLocation*>& rpc_locations,
      common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations) override;

private:
  static int check_member_list(const common::ObIArray<ObReplicaLocation>& cached_member_list,
      const common::ObIArray<common::ObAddr>& server_list, bool& is_same);
  int check_non_paxos_replica(const common::ObIArray<ObReplicaLocation>& cached_member_list,
      const common::ObIArray<ObReplicaMember>& non_paxos_replicas, bool& is_same);
  int deal_with_replica_type_changed(const bool new_mode, const common::ObPartitionKey& pkey,
      const ObReplicaLocation& old_replica_location, const obrpc::ObMemberListAndLeaderArg& member_info,
      ObPartitionLocation& new_location, bool& is_new_location_valid);
  int add_non_paxos_replica(const obrpc::ObMemberListAndLeaderArg& member_info,
      common::ObIArray<common::ObReplicaMember>& non_paxos_replicas);
  int check_leader_and_member_list(const bool new_mode, const ObPartitionKey& pkey, const common::ObAddr& addr,
      const common::ObIArray<ObReplicaLocation>& location_array, const obrpc::ObMemberListAndLeaderArg& member_info,
      ObReplicaLocation& new_leader, bool& is_new_location_valid);

  virtual int init_batch_rpc_renew_struct(const common::ObIArray<const ObPartitionLocation*>& rpc_locations,
      common::ObIAllocator& allocator, common::ObArray<ObLocationRpcRenewInfo>& infos,
      common::ObArray<int>& key_results,
      common::hash::ObHashMap<ObReplicaRenewKey, const obrpc::ObMemberListAndLeaderArg*>& result_map);

private:
  bool inited_;
  common::ObServerConfig* config_;
  share::ObPartitionTableOperator* pt_;
  share::ObRemotePartitionTableOperator* remote_pt_;
  ObRsMgr* rs_mgr_;
  obrpc::ObCommonRpcProxy* rpc_proxy_;
  obrpc::ObSrvRpcProxy* srv_rpc_proxy_;
  ObILocalityManager* locality_manager_;
  int64_t cluster_id_;
};

class ObLocationCacheQueueSet {
public:
  enum Type {
    LOC_QUEUE_SYS_CORE,
    LOC_QUEUE_SYS_RESTART_RELATED,
    LOC_QUEUE_SYS,
    LOC_QUEUE_USER_HA,
    LOC_QUEUE_USER_TENANT_SPACE,
    LOC_QUEUE_USER,
    LOC_QUEUE_MAX
  };

public:
  ObLocationCacheQueueSet();
  virtual ~ObLocationCacheQueueSet();
  int init(common::ObServerConfig& config);
  int add_task(const ObLocationUpdateTask& task);

public:
  static const int32_t ALL_ROOT_THREAD_CNT = 1;
  static const int32_t SYS_THREAD_CNT = 1;
  static const int64_t PLC_TASK_QUEUE_SIZE = 10 * 1000;
  static const int64_t PLC_TASK_MAP_SIZE = 10 * 1000;
  static const int64_t USER_TASK_MAP_SIZE = 1000 * 1000;          // 100W partitions
  static const int64_t MINI_MODE_USER_TASK_MAP_SIZE = 10 * 1000;  // 1W partitions
  static const int64_t USER_TASK_QUEUE_SIZE = USER_TASK_MAP_SIZE;
  static const int64_t MINI_MODE_USER_TASK_QUEUE_SIZE = MINI_MODE_USER_TASK_MAP_SIZE;
  static const char* get_str_by_queue_type(Type type);
  static ObLocationCacheQueueSet::Type get_queue_type(const uint64_t table_id, const int64_t partition_id);

private:
  bool is_inited_;
  // all core table's location don't need a queue because it fetch location through rpc
  common::ObDedupQueue all_root_update_queue_;      // __all_core_table, __all_root_table, __all_tenant_gts, __all_gts
  common::ObDedupQueue rs_restart_queue_;           // rs restart related sys table
  common::ObDedupQueue sys_update_queue_;           // other sys table in sys tenant
  common::ObDedupQueue user_ha_update_queue_;       // __all_dummy, __all_tenant_meta_table
  common::ObDedupQueue tenant_space_update_queue_;  // sys table in tenant space
  int user_update_queue_tg_id_;                     // user table
};

class ObLocationLeaderCache {
public:
  class ObLocationLeaderInfo {
  public:
    ObLocationLeaderInfo() : lock_(), value_(NULL)
    {}
    virtual ~ObLocationLeaderInfo()
    {}
    common::SpinRWLock& get_lock()
    {
      return lock_;
    }
    ObLocationLeader*& get_value()
    {
      return value_;
    }

  private:
    common::SpinRWLock lock_;
    ObLocationLeader* value_;
  };

public:
  ObLocationLeaderCache() : allocator_(), buffer_()
  {}
  virtual ~ObLocationLeaderCache()
  {}
  int get_strong_leader_info(const ObLocationCacheKey& key, LocationInfo& location_info);
  int set_strong_leader_info(const ObLocationCacheKey& key, const LocationInfo& location_info, bool force_update);

private:
  static const int64_t CACHE_NUM = 10000;
  common::ObArenaAllocator allocator_;
  ObLocationLeaderInfo buffer_[CACHE_NUM];
};

typedef observer::ObUniqTaskQueue<ObLocationAsyncUpdateTask, ObIPartitionLocationCache> ObLocationAsyncUpdateQueue;

class ObLocationAsyncUpdateQueueSet {
public:
  ObLocationAsyncUpdateQueueSet(ObIPartitionLocationCache* loc_cache_);
  virtual ~ObLocationAsyncUpdateQueueSet();
  int init(common::ObServerConfig& config);
  int add_task(const ObLocationAsyncUpdateTask& task);
  void stop();
  void wait();

public:
  const static int64_t MINI_MODE_UPDATE_THREAD_CNT = 1;
  static const int64_t PLC_TASK_QUEUE_SIZE = 10 * 1000;
  static const int64_t USER_TASK_QUEUE_SIZE = 200 * 1000;           // 20W partitions
  static const int64_t MINI_MODE_USER_TASK_QUEUE_SIZE = 10 * 1000;  // 1W partitions
private:
  bool is_inited_;
  ObIPartitionLocationCache* loc_cache_;
  // all core table's location don't need a queue because it fetch location through rpc
  ObLocationAsyncUpdateQueue all_root_update_queue_;  // __all_core_table, __all_root_table, __all_tenant_gts, __all_gts
  ObLocationAsyncUpdateQueue rs_restart_queue_;       // rs restart related sys table
  ObLocationAsyncUpdateQueue sys_update_queue_;       // other sys table in sys tenant
  ObLocationAsyncUpdateQueue user_ha_update_queue_;   // __all_dummy, __all_tenant_meta_table
  ObLocationAsyncUpdateQueue tenant_space_update_queue_;  // sys table in tenant space
  ObLocationAsyncUpdateQueue user_update_queue_;          // user table
};

class ObPartitionLocationCache : public ObIPartitionLocationCache {
public:
  friend class ObLocationUpdateTask;

  class LocationSem {
  public:
    LocationSem();
    ~LocationSem();
    void set_max_count(const int64_t max_count);
    int acquire(const int64_t abs_timeout_us);
    int release();

  private:
    int64_t cur_count_;
    int64_t max_count_;
    common::ObThreadCond cond_;
  };

  // limit the concurrency of location cache updates
  class RenewLimiter {
  public:
    const static int64_t RENEW_INTERVAL = 1000000;  // 1s
    const static int64_t PARTITION_HASH_BUCKET_COUNT = 10000;

    RenewLimiter()
    {
      MEMSET(last_renew_timestamps_, 0, sizeof(last_renew_timestamps_));
    }
    virtual ~RenewLimiter()
    {}

    bool try_update_last_renew_timestamp(const common::ObPartitionKey& key, int64_t renew_timestamp);
    inline int64_t get_last_renew_timestamp(const common::ObPartitionKey& key)
    {
      int64_t idx = key.hash() % PARTITION_HASH_BUCKET_COUNT;
      return last_renew_timestamps_[idx];
    }

  private:
    int64_t last_renew_timestamps_[PARTITION_HASH_BUCKET_COUNT];
  };

  explicit ObPartitionLocationCache(ObILocationFetcher& location_fetcher);
  virtual ~ObPartitionLocationCache();

  int init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObServerConfig& config,
      ObIAliveServerTracer& server_tracer, const char* cache_name, const int64_t priority,
      ObILocalityManager* locality_manager,
      // ObIRemoteLocatonGetter *location_getter,
      const int64_t cluster_id, share::ObRemoteServerProvider* remote_server_provider);
  int destroy();

  int reload_config();

  virtual ObIPartitionLocationCache::PartitionLocationCacheType get_type() const override
  {
    return ObIPartitionLocationCache::PART_LOC_CACHE_TYPE_NORMAL;
  }

  void stop();
  void wait();
  // get partition location of a partition
  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true) override;

  // Used by proxy
  virtual int force_sql_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit) override;

  // get partition location of a partition
  virtual int get(const common::ObPartitionKey& partition, ObPartitionLocation& location,
      const int64_t expire_renew_time, bool& is_cache_hit) override;

  // get all partition locations of a table
  virtual int get(const uint64_t table_id, common::ObIArray<ObPartitionLocation>& locations,
      const int64_t expire_renew_time, bool& is_cache_hit, const bool auto_update = true) override;

  // get leader addr of a partition (strong leader)
  virtual int get_strong_leader(
      const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false) override;
  virtual int get_leader_by_election(
      const common::ObPartitionKey& partition, common::ObAddr& leader, const bool force_renew = false) override;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const uint64_t table_id, const int64_t partition_id, ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache
  virtual int nonblock_get(const common::ObPartitionKey& partition, ObPartitionLocation& location,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // return OB_LOCATION_NOT_EXIST if partition's location not in cache,
  // return OB_LOCATION_LEADER_NOT_EXIST if partition's location in cache, but leader not exist
  // (strong leader)
  virtual int nonblock_get_strong_leader(const common::ObPartitionKey& partition, common::ObAddr& leader) override;
  virtual int nonblock_get_restore_leader(const ObPartitionKey& partition, ObAddr& leader) override;
  virtual int nonblock_get_leader_by_election(const ObPartitionKey& partition, ObAddr& leader) override;
  // trigger a location update task and clear location in cache
  virtual int nonblock_renew(const common::ObPartitionKey& partition, const int64_t expire_renew_time,
      const int64_t specific_cluster_id = common::OB_INVALID_ID) override;

  virtual int get_leader_across_cluster(const common::ObPartitionKey& partition, common::ObAddr& leader,
      const int64_t cluster_id, const bool force_renew = false) override;

  // It's used to get location cache from primary cluster in standby cluster.
  virtual int get_across_cluster(const common::ObPartitionKey& partition, const int64_t expire_renew_time,
      ObPartitionLocation& location, const int64_t specific_cluster_id = common::OB_INVALID_ID) override;

  virtual int nonblock_get_strong_leader_without_renew(const common::ObPartitionKey& partition, common::ObAddr& leader,
      const int64_t specific_cluster_id = common::OB_INVALID_ID) override;

  virtual int nonblock_get_leader_by_election_without_renew(const common::ObPartitionKey& partition,
      common::ObAddr& leader, const int64_t specific_cluster_id = common::OB_INVALID_ID) override;

  // try to trigger a location update task and clear location in cache,
  // if it is limited by the limiter and not be done, is_limited will be set to true
  virtual int nonblock_renew_with_limiter(
      const common::ObPartitionKey& partition, const int64_t expire_renew_time, bool& is_limited) override;
  // link table.
  virtual int get_link_table_location(const uint64_t table_id, ObPartitionLocation& location) override;

  /*-----batch async renew location-----*/
  virtual int batch_process_tasks(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks, bool& stopped) override;
  virtual int process_barrier(const ObLocationAsyncUpdateTask& task, bool& stopped) override;

  /*-----batch async renew location end -----*/
  virtual int64_t get_primary_cluster_id() const override;

  static int cache_value2location(const ObLocationCacheValue& cache_value, ObPartitionLocation& location);
  static int location2cache_value(
      const ObPartitionLocation& location, char* buf, const int64_t buf_size, ObLocationCacheValue& cache_value);
  static const int64_t OB_MAX_LOCATION_SERIALIZATION_SIZE = common::OB_MALLOC_BIG_BLOCK_SIZE;

private:
  int remote_get(const common::ObPartitionKey& pkey, ObPartitionLocation& location);

  bool is_duty_time(const ObPartitionLocation& location);
  bool can_do_force_sql_renew(const uint64_t table_id);

  /*-----batch async renew location-----*/
  int pre_check(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks);
  int batch_renew_location(const common::ObIArray<ObLocationAsyncUpdateTask>& tasks);
  int batch_renew_location_by_rpc(const common::ObIArray<ObPartitionLocation*>& locations,
      common::ObIAllocator& allocator, common::ObIArray<ObPartitionLocation*>& new_locations);
  int set_batch_timeout_ctx(const int64_t task_cnt, ObLocationAsyncUpdateTask::Type type, common::ObTimeoutCtx& ctx);
  /*-----batch async renew location end -----*/
private:
  const static int64_t DEFAULT_FETCH_LOCATION_TIMEOUT_US =
      ObLocationFetcher::OB_FETCH_LOCATION_TIMEOUT + ObLocationFetcher::OB_FETCH_MEMBER_LIST_AND_LEADER_TIMEOUT;  // 4s
  static const int64_t OB_SYS_LOCATION_CACHE_BUCKET_NUM = 512;
  typedef common::hash::ObHashMap<ObLocationCacheKey, ObPartitionLocation> NoSwapCache;
  typedef common::hash::ObHashMap<ObLocationCacheKey, LocationInfo> NoSwapLeaderCache;
  typedef common::ObKVCache<ObLocationCacheKey, ObLocationCacheValue> KVCache;

  static int set_timeout_ctx(common::ObTimeoutCtx& ctx);
  int inner_get_from_cache(
      const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id, ObPartitionLocation& result);

  int get_from_cache(const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id,
      const bool is_nonblock, ObPartitionLocation& location, const bool filter_not_readable_replica,
      const bool auto_update);

  // renew location sync
  // if force_renew is false, we will try to get from cache again, because other thread
  // may has already fetched location
  // if ignore_redundant is true, if location's update timestamp till now is less than
  // expire_renew_time means need the location renew time larger than expire_renew_time.
  //    if exist location renew_time is not larger than expire_renew_time, renew is excuted; else renew is ignored
  // if filter_not_readable_replica is true, location has only readable(F/R) replicas; else location has all replicas
  int renew_location(const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id,
      ObPartitionLocation& location, const int64_t expire_renew_time, const bool filter_not_readable_replica,
      const bool force_sql_renew, const bool auto_update);
  // const bool filter_not_readable_replica = true);

  int check_skip_rpc_renew_v2(const ObPartitionLocation& location, bool& skip);

  int batch_renew_sys_table_location_by_rpc(common::ObIArray<ObLocationCacheKey>& keys);

  // update location in cache
  int update_location(const uint64_t table_id, const int64_t partition_id, const int64_t cluster_id,
      const ObPartitionLocation& location);
  // clear location in cache
  int clear_location(
      const uint64_t table_id, const int64_t partiton_id, const int64_t expire_renew_time, const int64_t cluster_id);

  // virtual table related
  int vtable_get(const uint64_t table_id, common::ObIArray<ObPartitionLocation>& locations,
      const int64_t expire_renew_time, bool& is_cache_hit);
  int get_from_vtable_cache(const uint64_t table_id, common::ObSArray<ObPartitionLocation>& locations);
  int renew_vtable_location(const uint64_t table_id, common::ObSArray<ObPartitionLocation>& locations);
  int update_vtable_location(const uint64_t table_id, const common::ObSArray<ObPartitionLocation>& locations);

  int clear_vtable_location(const uint64_t table_id, const int64_t expire_renew_time);

  template <typename LOCATION>
  static int cache_value2location(const ObLocationCacheValue& cache_value, LOCATION& location);
  template <typename LOCATION>
  static int location2cache_value(
      const LOCATION& location, char* buf, const int64_t buf_size, ObLocationCacheValue& cache_value);

  // add location update task to suitable queue
  int add_update_task(const ObLocationAsyncUpdateTask& task);

  bool use_sys_cache(const uint64_t table_id) const;
  bool use_sys_leader_cache(const uint64_t table_id, const int64_t cluster_id) const;

private:
  const int64_t FORCE_SQL_RENEW_WINDOW = 1000000;      // 1s
  const int64_t LC_VALID_THRESHOLD_TS = 10 * 1000000;  // 10s
  bool is_reliable(const int64_t renew_time)
  {
    return renew_time + LC_VALID_THRESHOLD_TS > common::ObTimeUtility::current_time();
  }

private:
  bool is_inited_;
  bool is_stopped_;
  ObILocationFetcher& location_fetcher_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObServerConfig* config_;
  NoSwapCache sys_cache_;  // core or system table location cache, will not be swap out
  NoSwapLeaderCache sys_leader_cache_;
  KVCache user_cache_;  // user table location cache
  ObIAliveServerTracer* server_tracer_;
  LocationSem sem_;
  RenewLimiter renew_limiter_;
  ObILocalityManager* locality_manager_;
  int64_t cluster_id_;  // cluster_id of current cluster
  share::ObRemoteServerProvider* remote_server_provider_;
  common::ObSpinLock lock_;
  TenantSqlRenewInfoMap sql_renew_map_;
  common::ObCachedAllocator<ObTenantSqlRenewStat> sql_renew_pool_;
  ObLocationLeaderCache leader_cache_;  // user leader cache for local cluster
  ObLocationAsyncUpdateQueueSet local_async_queue_set_;
  ObLocationAsyncUpdateQueueSet remote_async_queue_set_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionLocationCache);
};

}  // end namespace share
}  // end namespace oceanbase

#endif
