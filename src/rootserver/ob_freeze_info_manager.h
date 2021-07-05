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

#ifndef OCEANBASE_ROOTSERVER_OB_FREEZE_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_FREEZE_INFO_MANAGER_H_

#include "lib/lock/ob_spin_rwlock.h"
#include "share/ob_freeze_info_proxy.h"
#include "common/storage/ob_freeze_define.h"
#include "share/ob_snapshot_table_proxy.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase {
namespace share {
class ObRemoteSqlProxy;
}
namespace obrpc {
class ObCommonRpcProxy;
}
namespace common {
class ObAddr;
}
namespace rootserver {
class ObZoneManager;
class ObStandbyClusterSchemaProcessor;
class ObRootService;

// ObFreezeInfoManager used to manage __all_freeze_info in memory.
// provide: set_freeze_info() and get_freeze_info()
// 1. set_freeze_info() used to rs write clog when major freeze
// 2. get_freeze_info() used to get spec version frozen status
class ObFreezeInfoManager {
public:
  ObFreezeInfoManager()
      : is_inited_(false),
        freeze_info_proxy_(),
        snapshot_proxy_(),
        zone_manager_(NULL),
        sql_proxy_(NULL),
        rs_(NULL),
        common_rpc_proxy_(NULL),
        update_lock_(),
        freeze_info_lock_(),
        freeze_info_()
  {
    freeze_info_.reset();
  }
  virtual ~ObFreezeInfoManager()
  {}

private:
  struct FreezeInfo {
  public:
    bool is_loaded_;
    bool fast_loaded_;
    // local cached latest freeze info, schema_version is not included.
    share::ObSimpleFrozenStatus current_frozen_status_;
    // local cached latest frozen schema version.
    common::ObArray<share::TenantIdAndSchemaVersion> frozen_schema_versions_;
    // local cached frozen status which need deferred fill
    // this value has no means on standby.
    common::ObSEArray<storage::ObFrozenStatus, 10> frozen_status_without_schema_version_;
    // local cached latest snapshot gc ts
    int64_t latest_snapshot_gc_ts_;
    // local cached latest snapshot gc ts schema version
    common::ObArray<share::TenantIdAndSchemaVersion> latest_snapshot_gc_schema_versions_;
    // local cached max defined frozen version whose schema version has been decided.
    // this value is meaningful on standby.
    int64_t latest_defined_frozen_version_;
    // local cached latest multi-versions
    // this value is meaningful on standby.
    common::ObArray<share::TenantSnapshot> latest_multi_versions_;
    FreezeInfo()
    {
      reset();
    }
    TO_STRING_KV(K_(is_loaded), K_(fast_loaded), K_(current_frozen_status), K_(frozen_schema_versions),
        K_(frozen_status_without_schema_version), K_(latest_snapshot_gc_ts), K_(latest_snapshot_gc_schema_versions),
        K_(latest_defined_frozen_version), K_(latest_multi_versions));
    void reset();
    void limited_reset();
    bool is_valid() const;
    void set_invalid()
    {
      is_loaded_ = false;
    }
    int set_freeze_info_for_bootstrap(
        const int64_t frozen_version, const int64_t frozen_timestamp, const int64_t cluster_version);
    int set_current_frozen_status(const int64_t frozen_version, const int64_t frozen_timestamp,
        const int64_t cluster_version, const common::ObIArray<share::TenantIdAndSchemaVersion>& tenant_schemas);
    int assign(const FreezeInfo& other);
  };

public:
  int init(
      common::ObMySQLProxy* proxy, ObZoneManager* zone_manager, obrpc::ObCommonRpcProxy* common_rpc, ObRootService& rs);
  void destroy();

public:
  int reload();
  int load_frozen_status();
  void unload();
  int try_reload();

public:
  int set_freeze_info(const int64_t frozen_version, const int64_t tenant_id, const common::ObAddr& server_addr,
      const common::ObAddr& rs_addr);
  int try_update_major_schema_version();
  // recycle merged freeze info periodically.
  // store last month freeze info
  int gc_freeze_info();
  int renew_snapshot_gc_ts();
  int get_max_frozen_status_for_daily_merge(share::ObSimpleFrozenStatus& frozen_status);
  int get_frozen_status_for_create_partition(const uint64_t tenant_id, share::ObSimpleFrozenStatus& frozen_status);
  int get_remote_frozen_status_for_create_partition(
      const uint64_t tenant_id, const int64_t schema_version, share::ObSimpleFrozenStatus& frozen_status);

  int broadcast_frozen_info();
  int get_snapshot_gc_ts_in_memory(int64_t& gc_snapshot_version) const;
  int check_snapshot_gc_ts();
  int get_max_frozen_version(int64_t& max_frozen_version);
  // get schema version by tenant id and frozen version.
  // tenant_id = 0 : get all tenants frozen schema versions.
  int get_freeze_schema_versions(const int64_t tenant_id, const int64_t frozen_version,
      common::ObIArray<share::TenantIdAndSchemaVersion>& schema_versions);

  bool is_leader_cluster();
  int get_cluster_info(share::ObClusterInfo& cluster_info);
  // frozen_version == 0 means to get the frozen information of the current latest version
  // frozen_version > 0 means to get the frozen information of the specified version,
  int get_freeze_info(const int64_t frozen_version, share::ObSimpleFrozenStatus& frozen_status);
  // Check if there are tenants that have not set schema_version in the frozen schema table; fill in in time
  int process_invalid_schema_version();
  // get latest merged frozen status
  int get_latest_merger_frozen_status(share::ObSimpleFrozenStatus& frozen_status);
  int get_latest_snapshot_gc_ts(int64_t& lastest_snapshot_gc_ts) const;

private:
  int check_inner_stat() const;
  int load_multi_version(FreezeInfo& freeze_info);
  bool is_valid_schema(const common::ObIArray<share::TenantIdAndSchemaVersion>& schema_versions);
  static bool is_equal_array(const common::ObIArray<share::TenantIdAndSchemaVersion>& left,
      const common::ObIArray<share::TenantIdAndSchemaVersion>& right);
  int get_schema_from_cache(const int64_t tenant_id, const int64_t frozen_version,
      common::ObIArray<share::TenantIdAndSchemaVersion>& schema_versions);
  bool is_versions_in_order(const common::ObIArray<share::TenantIdAndSchemaVersion>& left_schema,
      const common::ObIArray<share::TenantIdAndSchemaVersion>& right_schema);
  int check_snapshot_info_valid(const int64_t snapshot_gc_ts,
      const common::ObIArray<share::TenantIdAndSchemaVersion>& gc_schema_version, bool& is_valid);
  static int get_snapshot_info(const int64_t tenant_id, const common::ObIArray<share::TenantSnapshot>& new_snapshots,
      const common::ObIArray<share::TenantSnapshot>& old_snapshot, bool& need_update, share::TenantSnapshot& snapshot);
  // void reset();
  int inner_reload(FreezeInfo& freeze_info);
  // handle tenants whose schema is not refreshed
  int fix_tenant_schema_version(const int64_t frozen_version, const int64_t tenant_id);

  int renew_snapshot_gc_ts_223(const int64_t gc_timestamp);
  int renew_snapshot_gc_ts_less_2230();
  void reset_freeze_info();
  void set_freeze_info_invalid();
  bool is_freeze_info_valid() const;
  int set_freeze_info(const FreezeInfo& freeze_info);
  int set_simple_freeze_info(const FreezeInfo& freeze_info);
  int get_freeze_info_shadow(FreezeInfo& freeze_info) const;
  int set_latest_snapshot_gc_ts(const int64_t time);
  int set_latest_multi_versions(const common::ObIArray<share::TenantSnapshot>& snapshot_infos);
  int inner_update_major_schema_version();

public:
  // no merge of frozen_version 1 will happen,
  // we stipulate frozen_status of version 1 as follows
  static const int64_t ORIGIN_FROZEN_VERSION = 1;
  static const common::ObFreezeStatus ORIGIN_FREEZE_STATUS = common::COMMIT_SUCCEED;
  static const int64_t ORIGIN_FROZEN_TIMESTAMP = 1;
  static const int64_t ORIGIN_SCHEMA_VERSION = 1;
  static const int64_t SNAPSHOT_GC_TS_WARN = 30LL * 60LL * 1000LL * 1000LL;
  static const int64_t SNAPSHOT_GC_TS_ERROR = 2LL * 60LL * 60LL * 1000LL * 1000LL;

private:
  bool is_inited_;
  share::ObFreezeInfoProxy freeze_info_proxy_;
  share::ObSnapshotTableProxy snapshot_proxy_;
  ObZoneManager* zone_manager_;
  common::ObMySQLProxy* sql_proxy_;
  ObRootService* rs_;
  obrpc::ObCommonRpcProxy* common_rpc_proxy_;
  mutable common::ObSpinLock update_lock_;
  mutable common::SpinRWLock freeze_info_lock_;
  FreezeInfo freeze_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFreezeInfoManager);
};

}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_FREEZE_INFO_MANAGER_H_
