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

#ifndef OCEANBASE_STORAGE_FREEZE_INFO_SNAPSHOT_MGR_
#define OCEANBASE_STORAGE_FREEZE_INFO_SNAPSHOT_MGR_

#include <stdint.h>

#include "lib/allocator/ob_slice_alloc.h"
#include "lib/hash/ob_hashset.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/task/ob_timer.h"
#include "share/ob_freeze_info_proxy.h"
#include "share/ob_snapshot_table_proxy.h"
#include "share/ob_zone_info.h"

namespace oceanbase {

namespace common {
class ObISQLClient;
}

namespace storage {

struct ObFrozenStatus;

class ObFreezeInfoSnapshotMgr {
public:
  struct FreezeInfoLite {
    int64_t freeze_version;
    int64_t freeze_ts;
    int64_t cluster_version;

    FreezeInfoLite() : freeze_version(-1), freeze_ts(-1), cluster_version(0)
    {}
    FreezeInfoLite(const int64_t version, const int64_t ts, const int64_t cluster_ver)
        : freeze_version(version), freeze_ts(ts), cluster_version(cluster_ver)
    {}
    void reset()
    {
      freeze_version = -1;
      freeze_ts = -1;
      cluster_version = 0;
    }
    TO_STRING_KV(K(freeze_version), K(freeze_ts), K(cluster_version));
  };

  struct FreezeInfo {
    int64_t freeze_version;
    int64_t freeze_ts;
    int64_t schema_version;
    int64_t cluster_version;

    FreezeInfo() : freeze_version(-1), freeze_ts(-1), schema_version(-1), cluster_version(0)
    {}
    FreezeInfo& operator=(const FreezeInfoLite& o)
    {
      freeze_version = o.freeze_version;
      freeze_ts = o.freeze_ts;
      cluster_version = o.cluster_version;
      return *this;
    }
    void reset()
    {
      freeze_version = -1;
      freeze_ts = -1;
      schema_version = -1;
      cluster_version = 0;
    }
    TO_STRING_KV(K(freeze_version), K(freeze_ts), K(schema_version), K(cluster_version));
  };

  struct GCSnapshotInfo {
    int64_t snapshot_ts;
    int64_t schema_version;

    GCSnapshotInfo() : snapshot_ts(-1), schema_version(-1)
    {}
    GCSnapshotInfo(int64_t ts, int64_t version) : snapshot_ts(ts), schema_version(version)
    {}
    TO_STRING_KV(K(snapshot_ts), K(schema_version));
  };

  struct NeighbourFreezeInfoLite {
    FreezeInfoLite next;
    FreezeInfoLite prev;

    void reset()
    {
      next.reset();
      prev.reset();
    }
    TO_STRING_KV(K(next), K(prev));
  };

  struct NeighbourFreezeInfo {
    FreezeInfo next;
    FreezeInfoLite prev;

    NeighbourFreezeInfo& operator=(const NeighbourFreezeInfoLite& o)
    {
      next = o.next;
      prev = o.prev;
      return *this;
    }
    void reset()
    {
      next.reset();
      prev.reset();
    }
    TO_STRING_KV(K(next), K(prev));
  };

  struct SchemaPair {
    uint64_t tenant_id;
    int64_t schema_version;

    SchemaPair() : tenant_id(common::OB_INVALID_ID), schema_version(common::OB_INVALID_VERSION)
    {}
    SchemaPair(uint64_t tenant, int64_t schema) : tenant_id(tenant), schema_version(schema)
    {}

    TO_STRING_KV(K(tenant_id), K(schema_version));
  };

  int init(common::ObISQLClient& sql_proxy, bool is_remote);
  void init_for_test()
  {
    inited_ = true;
  }
  bool is_inited() const
  {
    return inited_;
  }
  int start();
  void wait();
  void stop();

  int64_t get_latest_frozen_timestamp();

  // no schema version is returned if you do not give the tenant id
  int get_freeze_info_by_major_version(const int64_t major_version, FreezeInfoLite& freeze_info);
  int get_freeze_info_by_major_version(const uint64_t table_id, const int64_t major_version, FreezeInfo& freeze_info);
  int get_freeze_info_by_major_version(
      const int64_t major_version, FreezeInfoLite& freeze_info, bool& is_first_major_version);
  int get_freeze_info_behind_major_version(const int64_t major_version, common::ObIArray<FreezeInfoLite>& freeze_infos);

  int get_tenant_freeze_info_by_major_version(
      const uint64_t tenant_id, const int64_t major_version, FreezeInfo& freeze_info);

  int get_freeze_info_by_snapshot_version(const int64_t snapshot_version, FreezeInfoLite& freeze_info);
  int get_freeze_info_by_snapshot_version(
      const uint64_t table_id, const int64_t snapshot_version, FreezeInfo& freeze_info);

  int get_neighbour_major_freeze(const int64_t snapshot_version, NeighbourFreezeInfoLite& info);
  int get_neighbour_major_freeze(const uint64_t table_id, const int64_t snapshot_version, NeighbourFreezeInfo& info);

  int get_min_reserved_snapshot(const common::ObPartitionKey& pkey, const int64_t merged_version,
      const int64_t schema_version, int64_t& snapshot_version, int64_t& backup_snapshot_version);
  int get_reserve_points(const int64_t tenant_id, const share::ObSnapShotType snapshot_type,
      common::ObIArray<share::ObSnapshotInfo>& restore_points, int64_t& snapshot_gc_ts);
  int update_info(const int64_t snapshot_gc_ts, const common::ObIArray<SchemaPair>& gc_schema_version,
      const common::ObIArray<FreezeInfoLite>& info_list, const common::ObIArray<share::ObSnapshotInfo>& snapshots,
      const int64_t backup_snapshot_version, const int64_t delay_delete_snapshot_version,
      const int64_t min_major_version, bool& changed);

  int64_t get_snapshot_gc_ts();
  int get_local_backup_snapshot_version(int64_t& backup_snapshot_version);

  ObFreezeInfoSnapshotMgr(const ObFreezeInfoSnapshotMgr&) = delete;
  ObFreezeInfoSnapshotMgr& operator=(const ObFreezeInfoSnapshotMgr&) = delete;

  ObFreezeInfoSnapshotMgr();
  virtual ~ObFreezeInfoSnapshotMgr();

private:
  typedef common::RWLock::RLockGuard RLockGuard;
  typedef common::RWLock::WLockGuard WLockGuard;

  static const int64_t RELOAD_INTERVAL = 1L * 1000L * 1000L;
  static const int64_t MAX_GC_SNAPSHOT_TS_REFRESH_TS = 10L * 60L * 1000L * 1000L;
  static const int64_t FLUSH_GC_SNAPSHOT_TS_REFRESH_TS = common::MODIFY_GC_SNAPSHOT_INTERVAL + 10L * 1000L * 1000L;

  int get_latest_freeze_version(int64_t& freeze_version);
  int64_t get_next_idx()
  {
    return 1L - cur_idx_;
  }
  void switch_info()
  {
    cur_idx_ = get_next_idx();
  }
  int prepare_new_info_list(const int64_t min_major_version);
  virtual int get_multi_version_duration(const uint64_t tenant_id, int64_t& duration) const;
  int inner_get_neighbour_major_freeze(const int64_t snapshot_version, NeighbourFreezeInfoLite& info);
  int update_next_gc_schema_version(
      const common::ObIArray<SchemaPair>& gc_schema_version, const int64_t snapshot_gc_ts);
  int update_next_info_list(const common::ObIArray<FreezeInfoLite>& info_list);
  int update_next_snapshots(const common::ObIArray<share::ObSnapshotInfo>& snapshots);
  int get_freeze_info_by_major_version_(
      const int64_t major_version, FreezeInfoLite& freeze_info, bool& is_first_major_version);
  int get_tenant_freeze_info_by_major_version_(
      const uint64_t tenant_id, const int64_t major_version, const bool async, FreezeInfo& freeze_info);

  class SchemaCache;
  class SchemaQuerySet {
  public:
    explicit SchemaQuerySet(SchemaCache& schema_cache);
    ~SchemaQuerySet()
    {}
    int init();

    int submit_async_schema_query(const uint64_t tenant_id, const int64_t freeze_version);
    int update_schema_cache();

  private:
    struct SchemaQuery {
      SchemaQuery() : tenant_id_(0), freeze_version_(0)
      {}

      SchemaQuery(const uint64_t tenant_id, const int64_t freeze_version)
          : tenant_id_(tenant_id), freeze_version_(freeze_version)
      {}

      bool operator==(const SchemaQuery& o) const
      {
        return (tenant_id_ == o.tenant_id_) && (freeze_version_ == o.freeze_version_);
      }

      inline uint64_t hash() const
      {
        uint64_t hash_ret = 0;
        hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
        hash_ret = common::murmurhash(&freeze_version_, sizeof(freeze_version_), hash_ret);
        return hash_ret;
      }

      uint64_t tenant_id_;
      int64_t freeze_version_;
    };

    int pop_schema_query(SchemaQuery& query);

    common::hash::ObHashSet<SchemaQuery> schema_querys_;
    SchemaCache& schema_cache_;
    common::RWLock lock_;

    bool inited_;
  };

  class ReloadTask : public common::ObTimerTask {
  public:
    ReloadTask(ObFreezeInfoSnapshotMgr& mgr, SchemaQuerySet& schema_query_set);
    virtual ~ReloadTask()
    {}
    int init(common::ObISQLClient& sql_proxy, bool is_remote);
    virtual void runTimerTask();
    int try_update_info();

  private:
    int get_global_info_compat_below_220(int64_t& snapshot_gc_ts, common::ObIArray<SchemaPair>& gc_schema_version);
    int get_global_info(int64_t& snapshot_gc_ts, common::ObIArray<SchemaPair>& gc_schema_version);
    int get_freeze_info(int64_t& min_major_version, common::ObIArray<FreezeInfoLite>& freeze_info);
    int get_backup_snapshot_version(int64_t& backup_snapshot_version);

    bool inited_;
    bool is_remote_;
    ObFreezeInfoSnapshotMgr& mgr_;
    common::ObISQLClient* sql_proxy_;
    share::ObFreezeInfoProxy freeze_info_proxy_;
    share::ObSnapshotTableProxy snapshot_proxy_;
    int64_t last_change_ts_;
    SchemaQuerySet& schema_query_set_;
  };

  // LRU cache of schema information for different tenant major version
  class SchemaCache {
  public:
    typedef common::ObSpinLockGuard SpinLockGuard;

    explicit SchemaCache(SchemaQuerySet& schema_query_set);
    virtual ~SchemaCache();

    int init(common::ObISQLClient& sql_proxy);
    void reset();

    // return the schema version on success or OB_EAGAIN if major is not ready.
    // the LRU cache will update itself if the cache is not hit.
    int get_freeze_schema_version(
        const uint64_t tenant_id, const int64_t freeze_version, const bool async, int64_t& schema_version);

    int update_schema_version(const uint64_t tenant_id, const int64_t freeze_version);

  private:
    // dlink list
    struct schema_node {
      // key
      uint64_t tenant_id;
      int64_t freeze_version;
      // value
      int64_t schema_version;
      // pointers
      schema_node* prev;
      schema_node* next;

      schema_node()
          : tenant_id(common::OB_INVALID_ID),
            freeze_version(common::OB_INVALID_VERSION),
            schema_version(common::OB_INVALID_VERSION),
            prev(NULL),
            next(NULL)
      {}

      void set(const uint64_t tenant, const int64_t freeze, const int64_t schema)
      {
        tenant_id = tenant;
        freeze_version = freeze;
        schema_version = schema;
      }

      TO_STRING_KV(K(tenant_id), K(freeze_version), K(schema_version));
    };

    void insert(schema_node* p);
    void move_forward(schema_node* p);
    int find(const uint64_t tenant_id, const int64_t freeze_version, int64_t& schema_version);
    bool freeze_info_exist(const int64_t freeze_version);
    bool schema_exist(const uint64_t tenant_id, const int64_t freeze_version);
    bool inner_schema_exist(const uint64_t tenant_id, const int64_t freeze_version);
    schema_node* inner_find(const uint64_t tenant_id, const int64_t freeze_version);

    virtual int fetch_freeze_schema(const uint64_t tenant_id, const int64_t freeze_version, int64_t& schema_version);
    virtual int fetch_freeze_schema(const uint64_t tenant_id, const int64_t freeze_version, int64_t& schema_version,
        common::ObIArray<SchemaPair>& freeze_schema);

  public:  // for ut only
    int update_freeze_schema(const uint64_t tenant_id, const int64_t freeze_version, const int64_t schema_version);

  private:
    int update_freeze_schema(const int64_t freeze_version, common::ObIArray<SchemaPair>& freeze_schema);

    common::ObSpinLock lock_;

    static const int64_t MAX_SCHEMA_ENTRY = 10000L;

    schema_node* head_;
    schema_node* tail_;
    int64_t cnt_;

    bool inited_;
    common::ObISQLClient* sql_proxy_;
    share::ObFreezeInfoProxy freeze_info_proxy_;

    lib::ObMemAttr mem_attr_;
    common::ObSliceAlloc allocator_;

    SchemaQuerySet& schema_query_set_;
  };

  bool inited_;
  int tg_id_;
  ReloadTask reload_task_;
  SchemaQuerySet schema_query_set_;

  int64_t snapshot_gc_ts_;
  common::hash::ObHashMap<uint64_t, GCSnapshotInfo> gc_snapshot_info_[2];

protected:
  SchemaCache schema_cache_;  // query schema version based on tenant id and major freeze version
private:
  common::ObSEArray<FreezeInfoLite, 32> info_list_[2];         // lite one doesnot contain schema_version
  common::ObSEArray<share::ObSnapshotInfo, 32> snapshots_[2];  // snapshots_ matains multi_version_start for index and
                                                               // others
  int64_t backup_snapshot_version_;        // backup snapshot version is used for backup and recovery
  int64_t delay_delete_snapshot_version_;  // reserve the multi-version of __all_ddl_operatuib when backup
  int64_t cur_idx_;

  common::RWLock lock_;
};

class ObFreezeInfoMgrWrapper {
public:
  static ObFreezeInfoSnapshotMgr& get_instance(const uint64_t table_id = common::OB_INVALID_ID);
  static int init(common::ObISQLClient& local_proxy, common::ObISQLClient& remote_proxy);
  static int start();
  static void wait();
  static void stop();

private:
  static ObFreezeInfoSnapshotMgr local_mgr_;
  static ObFreezeInfoSnapshotMgr remote_mgr_;
};
}  // namespace storage
}  // namespace oceanbase

#endif /* OCEANBASE_STORAGE_FREEZE_INFO_SNAPSHOT_MGR_ */
