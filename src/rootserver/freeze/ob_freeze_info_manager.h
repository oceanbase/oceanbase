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
#include "share/scn.h"

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
  // local cached latest snapshot gc scn
  share::SCN latest_snapshot_gc_scn_;

  ObFreezeInfo()
    : frozen_statuses_(), latest_snapshot_gc_scn_(share::SCN::min_scn())
  {}
  ~ObFreezeInfo() {}

  void set_invalid() { reset(); }

  bool is_valid() const { return !frozen_statuses_.empty() && (latest_snapshot_gc_scn_.is_valid()); }
  void reset()
  {
    frozen_statuses_.reset();
    latest_snapshot_gc_scn_.reset();
  }

  int assign(const ObFreezeInfo &other);
  int get_latest_frozen_scn(share::SCN &frozen_scn) const;
  int get_min_freeze_info_greater_than(
      const share::SCN &frozen_scn,
      share::ObSimpleFrozenStatus &frozen_status) const;
  int get_frozen_status(
      const share::SCN &frozen_scn,
      share::ObSimpleFrozenStatus &frozen_status) const;

  TO_STRING_KV(K_(frozen_statuses), K_(latest_snapshot_gc_scn));
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
      lock_(common::ObLatchIds::OB_FREEZE_INFO_MANAGER_LOCK),
      freeze_info_()
  {}
  virtual ~ObFreezeInfoManager() {}

  int init(uint64_t tenant_id,
           common::ObMySQLProxy &proxy,
           ObZoneMergeManager &merge_info_mgr);
  int reload();
  int try_reload();

  int set_freeze_info();

  int get_freeze_info(const share::SCN &frozen_scn,
                      share::ObSimpleFrozenStatus &frozen_status);

  int renew_snapshot_gc_scn();
  int try_gc_freeze_info();
  int try_update_zone_info(const int64_t expected_epoch);

  int check_snapshot_gc_scn();
  int check_need_broadcast(bool &need_broadcast);
  int broadcast_freeze_info(const int64_t expected_epoch);

  int get_global_last_merged_scn(share::SCN &global_last_merged_scn) const;
  int get_global_broadcast_scn(share::SCN &global_broadcast_scn) const;
  int get_local_latest_frozen_scn(share::SCN &frozen_scn);
  int adjust_global_merge_info(const int64_t expected_epoch);

  void reset_freeze_info();
  int get_gts(share::SCN &gts_scn) const;

private:
  int inner_reload(ObFreezeInfo &freeze_info);

  int generate_frozen_scn(
      const ObFreezeInfo &freeze_info,
      const share::SCN &snapshot_gc_scn,
      share::SCN &new_frozen_scn);

  int set_local_snapshot_gc_scn(const share::SCN &new_scn);

  int get_schema_version(const share::SCN &frozen_scn, int64_t &schema_version) const;

  int get_min_freeze_info(share::ObSimpleFrozenStatus &frozen_status);
  int get_min_freeze_info_to_broadcast(share::ObSimpleFrozenStatus &frozen_status) const;
  int check_inner_stat();

public:
  static const int64_t SNAPSHOT_GC_TS_WARN = 30LL * 60LL * 1000LL * 1000LL;
  static const int64_t SNAPSHOT_GC_TS_ERROR = 2LL * 60LL * 60LL * 1000LL * 1000LL;


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
