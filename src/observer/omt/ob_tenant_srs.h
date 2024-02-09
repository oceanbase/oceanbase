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

#ifndef OCEANBASE_TENANT_SRS_H_
#define OCEANBASE_TENANT_SRS_H_

#include "share/ob_define.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/allocator/page_arena.h"
#include "lib/task/ob_timer.h"
#include "lib/geo/ob_srs_wkt_parser.h"
#include "lib/geo/ob_srs_info.h"

namespace oceanbase
{

namespace common
{

namespace sqlclient
{
class ObMySQLResult;
}
}

namespace omt
{

enum class ObSrsCacheType
{
  SYSTEM_RESERVED = 0,
  USER_DEFINED,
};

class ObSrsCacheSnapShot
{
public:
  static const uint32_t SRS_ITEM_BUCKET_NUM = 6144;
  explicit ObSrsCacheSnapShot(common::ObIAllocator *allocator, ObSrsCacheType srs_type)
    : allocator_(allocator), srs_type_(srs_type), srs_version_(0), ref_count_(0) {}
  virtual ~ObSrsCacheSnapShot() { srs_item_map_.destroy(); }
  int init() { return srs_item_map_.create(SRS_ITEM_BUCKET_NUM, "SrsSnapShot", "SrsSnapShot", MTL_ID()); }
  int add_srs_item(uint64_t srid, const common::ObSrsItem* srs_item) { return srs_item_map_.set_refactored(srid, srs_item); }
  int get_srs_item(uint64_t srid, const common::ObSrsItem *&srs_item);
  void set_srs_version(uint64_t version) { srs_version_ = version; }
  uint64_t get_srs_version() { return srs_version_; }
  void dec_ref_count() { ATOMIC_DEC(&ref_count_); }
  void inc_ref_count() { ATOMIC_INC(&ref_count_); }
  int64_t get_ref_count() { return ATOMIC_LOAD64(&ref_count_); }
  int64_t get_srs_count() { return srs_item_map_.size(); }
  int parse_srs_item(common::sqlclient::ObMySQLResult *result,
                     const common::ObSrsItem *&srs_item, uint64_t &srs_version);
  int add_pg_reserved_srs_item(const common::ObString &pg_wkt, const uint32_t srs_id);

private:
  common::ObIAllocator *allocator_;
  ObSrsCacheType srs_type_;
  uint64_t srs_version_;
  volatile int64_t ref_count_;
  common::hash::ObHashMap<uint64_t, const common::ObSrsItem*> srs_item_map_;

  int extract_bounds_numberic(common::sqlclient::ObMySQLResult *result, const char *field_name, double &value);

  DISALLOW_COPY_AND_ASSIGN(ObSrsCacheSnapShot);
};

class ObSrsCacheGuard
{
public:
  explicit ObSrsCacheGuard() : srs_cache_(nullptr) {}
  virtual ~ObSrsCacheGuard();
  int get_srs_item(uint64_t srs_id, const common::ObSrsItem *&srs_item);
  void set_srs_snapshot(ObSrsCacheSnapShot *srs_cache) { srs_cache_ =  srs_cache; }
  inline bool empty() { return srs_cache_ == nullptr; }
private:
  ObSrsCacheSnapShot *srs_cache_;
};


class ObTenantSrs
{
public:
  class TenantSrsUpdatePeriodicTask : public common::ObTimerTask
  {
  public:
    TenantSrsUpdatePeriodicTask() : tenant_srs_(nullptr) {}
    virtual ~TenantSrsUpdatePeriodicTask() {}
    int init(ObTenantSrs *srs);
    TenantSrsUpdatePeriodicTask(const TenantSrsUpdatePeriodicTask &) = delete;
    TenantSrsUpdatePeriodicTask &operator=(const TenantSrsUpdatePeriodicTask &) = delete;
    void runTimerTask(void) override;
  private:
    static const uint64_t SLEEP_USECONDS = 5000000;
    static const uint64_t BOOTSTRAP_PERIOD = 1000000;
    ObTenantSrs *tenant_srs_;
  };

  class TenantSrsUpdateTask : public common::ObTimerTask
  {
  public:
    TenantSrsUpdateTask() : tenant_srs_(nullptr) {}
    virtual ~TenantSrsUpdateTask() {}
    int init(ObTenantSrs *srs);
    TenantSrsUpdateTask(const TenantSrsUpdateTask &) = delete;
    TenantSrsUpdateTask &operator=(const TenantSrsUpdateTask &) = delete;
    void runTimerTask(void) override;
  private:
    ObTenantSrs *tenant_srs_;
  };

public:
  static const int64_t DEFAULT_PAGE_SIZE = 8192L; // 8kb
  static const uint32_t USER_SRID_MIN = 70000000;
  static const uint32_t USER_SRID_MAX = 2000000000;
  static const uint32_t MAX_WKT_LEN = 4096;
  static const uint32_t RETRY_TIMES = 45;
  static const uint32_t RETRY_INTERVAL_US = 100000;

  explicit ObTenantSrs()
    : alloc_("TenantSrs"), sql_proxy_(nullptr), inited_(false),
      last_sys_snapshot_(nullptr), last_user_snapshot_(nullptr),
      srs_old_snapshots_(&mode_arena_, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR),
      remote_sys_srs_version_(0), remote_user_srs_version_(0),
      local_sys_srs_version_(0), local_user_srs_version_(0), infinite_plane_() {}
  virtual ~ObTenantSrs() {};
  int init();
  inline uint64_t tenant_id() { return MTL_ID(); }
  int get_tenant_srs_guard(ObSrsCacheGuard &srs_guard);
  int get_srs_bounds(uint64_t srid, const ObSrsItem *srs_item, const ObSrsBoundsItem *&bounds_item);
  int get_last_snapshot(ObSrsCacheGuard &srs_guard);
  TenantSrsUpdatePeriodicTask &get_update_srs_task() { return  srs_update_periodic_task_; }
  int try_get_last_snapshot(ObSrsCacheGuard &srs_guard);
  void recycle_old_snapshots();
  void recycle_last_snapshots();
  uint32_t get_snapshots_size();
  int cancle_update_task();
  static int mtl_init(ObTenantSrs* &tenant_srs);
  int start();
  void stop();
  void wait();
  void destroy();

private:
  typedef common::PageArena<ObSrsCacheSnapShot*, common::ModulePageAllocator> ObCGeoModuleArena;
  typedef common::ObVector<ObSrsCacheSnapShot*, ObCGeoModuleArena> ObSrsSnapshotVector;

  int fetch_all_srs(ObSrsCacheSnapShot *&srs_snapshot, bool is_sys_srs = true);
  int parse_srs_item(common::sqlclient::ObMySQLResult *result, const common::ObSrsItem *&srs_item, uint64_t &srs_version);
  int refresh_sys_srs();
  int refresh_usr_srs();
  int refresh_srs(bool is_sys);
  int get_last_sys_snapshot(ObSrsCacheSnapShot *&sys_cache);
  int get_last_user_snapshot(ObSrsCacheSnapShot *&user_cache);
  int generate_pg_reserved_srs(ObSrsCacheSnapShot *&srs_snapshot);

  common::ObFIFOAllocator allocator_;
  common::ObArenaAllocator alloc_;
  common::ObMySQLProxy *sql_proxy_;
  bool inited_;
  common::ModulePageAllocator page_allocator_;
  ObCGeoModuleArena mode_arena_;
  common::TCRWLock lock_;
  // local the newest system reserved srs cache snapshot
  ObSrsCacheSnapShot* last_sys_snapshot_;
  // local the newest user defined srs cache snapshot
  ObSrsCacheSnapShot* last_user_snapshot_;
  // local overdue srs cache snapshot
  ObSrsSnapshotVector srs_old_snapshots_;
  // system reserved srs cache version from other servers
  uint64_t remote_sys_srs_version_;
  // user defined srs cache version from other servers
  uint64_t remote_user_srs_version_;
  // local system reserved srs cache version
  uint64_t local_sys_srs_version_;
  // local user defined srs cache version
  uint64_t local_user_srs_version_;
  TenantSrsUpdatePeriodicTask srs_update_periodic_task_;
  TenantSrsUpdateTask srs_update_task_;
  common::ObSrsBoundsItem infinite_plane_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantSrs);
};

#define OTSRS_MGR (MTL(omt::ObTenantSrs*))

}  // namespace omt
}  // namespace oceanbase

#endif
