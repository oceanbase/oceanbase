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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_TABLET_REPLICA_INFO_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_TABLET_REPLICA_INFO_


#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace storage
{
class ObTenantTabletPtrWithInMemObjIterator;
}
namespace observer
{

struct ObTabletReplicaInfo {
  uint64_t tablet_id;
  uint64_t table_id;
  int64_t ls_id;
  int64_t occupy_size;
  int64_t required_size;

  TO_STRING_KV(
    K(tablet_id),
    K(table_id),
    K(ls_id),
    K(occupy_size),
    K(required_size)
  );
};

typedef common::ObArray<ObTabletReplicaInfo> ObTabletReplicaInfoCache;

class ObTabletReplicaInfoCacheMgr;
class ObTabletReplicaInfoCacheIterator
{
public:
  ObTabletReplicaInfoCacheIterator();
  ~ObTabletReplicaInfoCacheIterator();
  int init(bool &cache_available);
  int get_next(ObTabletReplicaInfo &info);
  void reset();
private:
  int64_t row_idx_;
  const ObTabletReplicaInfoCache *cache_snapshot_;
  ObTabletReplicaInfoCacheMgr *cache_mgr_;
};

class ObTabletReplicaInfoCacheMgr
{
  friend class ObTabletReplicaInfoCacheIterator;
/**
 * ObTabletReplicaInfoCacheMgr is a MTL class that manages the cache of tablet replica info for tenant.
 *
 * States: UNAVAILABLE -> BUILDING -> AVAILABLE
 *
 *   begin_build() [ref_cnt > 0]
 *         +--------+
 *         |        |
 *         |        v              begin_build() [ref_cnt == 0]
 *  +--------------------+ ----------------------------------------> +--------------------+
 *  |    UNAVAILABLE     |                                           |      BUILDING      |
 *  +--------------------+ <---------------------------------------- +--------------------+
 *          ^                        finish_build() [if fail]                  |
 *          |                                                                  |
 *          |                                                                  |
 *          |                                                          finish_build() [if succ]
 *          |                                                                  |
 *          |                                                                  v
 *          |                                                        +--------------------+
 *          +--------------------------------------------------------|     AVAILABLE      |
 *                    invalidate() [default 10m interval]            +--------------------+
 *                                                                      |            ^
 *                                                                      |            |
 *                                                                      +------------+
 *                                                               acquire_snapshot() [ref_cnt++]
 *
 * Notes:
 *   1. states:
 *     - UNAVAILABLE: cache is not available, when ref_cnt == 0, one reader may become the builder and build the cache
 *     - BUILDING: cache is building, only one builder is allowed, other readers will use normal path
 *     - AVAILABLE: cache is available, readers can use the cache
 *   2. ref_cnt:
 *     - increase when acquire_snapshot(), decrease when dec_ref()
 *     - only when ref_cnt == 0 and cache is UNAVAILABLE, which means no reader is holding the cache snapshot,
 *       and cache has been invalidated, one reader will be chosen to become the builder and build the cache
 */
private:
  enum ObCacheStatus {
    UNAVAILABLE = 0,
    BUILDING = 1,
    AVAILABLE = 2
  };
public:
  ObTabletReplicaInfoCacheMgr();
  ~ObTabletReplicaInfoCacheMgr() {};
  int init();
  void destroy();
  static int mtl_init(ObTabletReplicaInfoCacheMgr* &cache_mgr);
  int add_cache(const ObTabletReplicaInfo &info);
  void try_invalidate();
  bool begin_build();
  void finish_build(bool is_build_success);
private:
  bool acquire_snapshot(const ObTabletReplicaInfoCache *&cache_snapshot);
  void dec_ref();
  void print_memory_usage();
private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObTabletReplicaInfoCache cache_;
  common::ObSpinLock lock_;
  ObCacheStatus status_;
  int64_t ref_cnt_;
  int64_t last_build_time_;
};

class ObAllVirtualTabletReplicaInfo : public common::ObVirtualTableScannerIterator,
                                      public omt::ObMultiTenantOperator
{
private:
  enum ObReadPath {
    PATH_NORMAL = 0,  // use tenant tablet iterator
    PATH_CACHE = 1,   // use cache
    PATH_BUILD = 2    // use tenant tablet iterator and build cache
  };

public:
  ObAllVirtualTabletReplicaInfo();
  virtual ~ObAllVirtualTabletReplicaInfo();
  int init(common::ObIAllocator *allocator,
           share::schema::ObMultiVersionSchemaService *schema_service,
           common::ObAddr &addr);
  virtual void reset() override;
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
protected:
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual bool is_need_process(uint64_t tenant_id) override {
    if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
      return true;
    }
    return false;
  }
  virtual void release_last_tenant() override;
private:
  int fill_row_(common::ObNewRow *&row);
  int get_next_tablet_replica_info_(ObTabletReplicaInfo &tablet_replica_info);
  int get_next_tablet_replica_info_from_cache_(ObTabletReplicaInfo &tablet_replica_info);
  int get_next_tablet_replica_info_from_iter_(ObTabletReplicaInfo &tablet_replica_info);
  int init_need_fetch_flags_();
  int get_table_related_schemas_(const uint64_t table_id,
                                 const ObSimpleTableSchemaV2 *&table_schema,
                                 const ObSimpleDatabaseSchema *&database_schema,
                                 const ObSimpleTablegroupSchema *&tablegroup_schema);
  int get_ls_role_(const int64_t ls_id, common::ObRole &role);
  int prepare_tablet_to_table_map_();
  int init_curr_tenant_();
private:
  enum {
    SERVER_IP = common::OB_APP_MIN_COLUMN_ID,
    SERVER_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    ROLE,
    ZONE,
    TABLE_ID,
    TABLE_NAME,
    DATABASE_ID,
    DATABASE_NAME,
    TABLE_TYPE,
    TABLEGROUP_ID,
    TABLEGROUP_NAME,
    DATA_TABLE_ID,
    OCCUPY_SIZE,
    REQUIRED_SIZE
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletReplicaInfo);
private:
  bool is_inited_;
  common::ObAddr addr_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  void *iter_buf_;

  // for column pruning
  bool need_fetch_table_schema_;
  bool need_fetch_database_schema_;
  bool need_fetch_tablegroup_schema_;

  /* --------- current tenant specific start --------- */
  bool is_curr_tenant_inited_;
  bool is_build_success_;
  common::hash::ObHashMap<ObTabletID, uint64_t, common::hash::NoPthreadDefendMode> tablet_to_table_map_;
  common::hash::ObHashMap<uint64_t, common::ObRole, common::hash::NoPthreadDefendMode> ls_to_role_map_;
  storage::ObTenantTabletPtrWithInMemObjIterator *tablet_iter_;
  ObReadPath read_path_;
  ObTabletReplicaInfoCacheIterator cache_iter_;
  ObTabletReplicaInfoCacheMgr *cache_mgr_;
  ObSchemaGetterGuard schema_guard_;
  /* --------- current tenant specific end --------- */
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_VIRTUAL_TABLET_REPLICA_INFO */
