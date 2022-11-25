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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_INFO_MANAGER_H_

#include "lib/lock/ob_recursive_mutex.h"
#include "share/ob_freeze_info_proxy.h"
#include "common/storage/ob_freeze_define.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
}
namespace common
{
class ObAddr;
class ObMySQLProxy;
}
namespace rootserver
{
class ObZoneMergeManager;

class ObFreezeInfo
{
public:
  // local cached freeze status >= global_broadcast_scn,
  // and stored order by freeze_scn asc
  common::ObSEArray<share::ObSimpleFrozenStatus, 4> frozen_statuses_;
  // local cached latest snapshot gc ts
  int64_t latest_snapshot_gc_ts_;

  ObFreezeInfo()
    : frozen_statuses_(), latest_snapshot_gc_ts_(0)
  {}
  ~ObFreezeInfo() {}

  void set_invalid() { reset(); }

  bool is_valid() const { return !frozen_statuses_.empty() && (latest_snapshot_gc_ts_ >= 0); }
  void reset()
  {
    frozen_statuses_.reset();
    latest_snapshot_gc_ts_ = 0;
  }

  int assign(const ObFreezeInfo &other);
  int get_latest_frozen_scn(int64_t &frozen_scn) const;
  int get_min_freeze_info_greater_than(
      const int64_t frozen_scn, 
      share::ObSimpleFrozenStatus &frozen_status) const;
  int get_frozen_status(
      const int64_t frozen_scn, 
      share::ObSimpleFrozenStatus &frozen_status) const;

  TO_STRING_KV(K_(frozen_statuses), K_(latest_snapshot_gc_ts));
};

// ObFreezeInfoManager(tenant level) used to manage __all_freeze_info in memory.
class ObFreezeInfoManager
{
public:
  ObFreezeInfoManager()
    : is_inited_(false),
      tenant_id_(common::OB_INVALID_ID),
      sql_proxy_(nullptr),
      merge_info_mgr_(nullptr), 
      lock_(), 
      freeze_info_()
  {}
  virtual ~ObFreezeInfoManager() {}

  int init(uint64_t tenant_id,
           common::ObMySQLProxy &proxy,
           ObZoneMergeManager &merge_info_mgr);
  int reload();
  int try_reload();

  int set_freeze_info();

  int get_freeze_info(const int64_t frozen_scn,
                      share::ObSimpleFrozenStatus &frozen_status);

  int renew_snapshot_gc_ts();
  int try_gc_freeze_info();
  int try_update_zone_info(const int64_t expected_epoch);

  int check_snapshot_gc_ts();
  int check_need_broadcast(bool &need_broadcast);
  int broadcast_freeze_info(const int64_t expected_epoch);

  int get_global_last_merged_scn(int64_t &global_last_merged_scn) const;
  int get_global_broadcast_scn(int64_t &global_broadcast_scn) const;
  int get_local_latest_frozen_scn(int64_t &frozen_scn);

  void reset_freeze_info();

private:
  int inner_reload(ObFreezeInfo &freeze_info);

  int generate_frozen_scn(
      const ObFreezeInfo &freeze_info,
      const int64_t snapshot_gc_ts, 
      int64_t &new_frozen_scn);

  int set_local_snapshot_gc_ts(const int64_t time);

  int get_gts(int64_t &ts) const;
  int get_schema_version(const int64_t frozen_scn, int64_t &schema_version) const;

  int get_min_freeze_info(share::ObSimpleFrozenStatus &frozen_status);
  int get_min_freeze_info_to_broadcast(share::ObSimpleFrozenStatus &frozen_status) const;
  int check_inner_stat();

public:
  static const int64_t ORIGIN_FROZEN_SCN = 1;
  static const int64_t ORIGIN_SCHEMA_VERSION = 1;

  static const int64_t SNAPSHOT_GC_TS_WARN = 30LL * 60LL * 1000LL * 1000LL * 1000LL; // ns
  static const int64_t SNAPSHOT_GC_TS_ERROR = 2LL * 60LL * 60LL * 1000LL * 1000LL * 1000LL; // ns

private:
  bool is_inited_;
  int64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  ObZoneMergeManager *merge_info_mgr_;
  mutable common::ObRecursiveMutex lock_;
  ObFreezeInfo freeze_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFreezeInfoManager);
};

} // rootserver
} // oceanbase
#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_FREEZE_INFO_MANAGER_H_
