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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_INFO_MANAGER_H_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_INFO_MANAGER_H_

#include "lib/lock/ob_recursive_mutex.h"
#include "share/ob_freeze_info_manager.h"
#include "rootserver/freeze/ob_zone_merge_manager.h"
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
namespace share
{
class ObFreezeInfoManager;
}
namespace rootserver
{
class ObZoneMergeManager;


// ObMajorMergeInfoManager (tenant level)
class ObMajorMergeInfoManager
{
public:
  ObMajorMergeInfoManager()
    : is_inited_(false),
      tenant_id_(common::OB_INVALID_ID),
      zone_merge_mgr_(),
      freeze_info_mgr_(),
      lock_(common::ObLatchIds::OB_MAJOR_MERGE_INFO_MANAGER_LOCK)
  {}
  virtual ~ObMajorMergeInfoManager() {}
  ObZoneMergeManager &get_zone_merge_mgr() { return zone_merge_mgr_; }
  share::ObFreezeInfoManager &get_freeze_info_mgr() { return freeze_info_mgr_; }
  int init(uint64_t tenant_id,
           common::ObMySQLProxy &sql_proxy);
  int try_reload();
  int reload(const bool reload_zone_merge_info = false);
  void reset_info()
  {
    zone_merge_mgr_.reset_merge_info();
    freeze_info_mgr_.reset_freeze_info();
  };

  int set_freeze_info();
  int get_freeze_info(const share::SCN &frozen_scn,
                      share::ObFreezeInfo &frozen_status);

  int renew_snapshot_gc_scn();
  int try_gc_freeze_info();
  int try_update_zone_info(const int64_t expected_epoch);

  int check_snapshot_gc_scn();
  int check_need_broadcast(bool &need_broadcast);
  int broadcast_freeze_info(const int64_t expected_epoch);
  int get_local_latest_frozen_scn(share::SCN &frozen_scn);
  int adjust_global_merge_info(const int64_t expected_epoch);
  int get_gts(share::SCN &gts_scn) const;

private:
  // used for set freeze info
  int generate_frozen_scn(
      const share::SCN &snapshot_gc_scn,
      share::SCN &new_frozen_scn);
  int get_schema_version(
      const share::SCN &frozen_scn,
      int64_t &schema_version) const;

  int inner_get_min_freeze_info(share::ObFreezeInfo &frozen_status);

public:
  static const int64_t SNAPSHOT_GC_TS_WARN = 30LL * 60LL * 1000LL * 1000LL;
  static const int64_t SNAPSHOT_GC_TS_ERROR = 2LL * 60LL * 60LL * 1000LL * 1000LL;

private:
  bool is_inited_;
  int64_t tenant_id_;
  ObZoneMergeManager zone_merge_mgr_;
  share::ObFreezeInfoManager freeze_info_mgr_;
  mutable common::ObRecursiveMutex lock_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMajorMergeInfoManager);
};

} // rootserver
} // oceanbase
#endif // OCEANBASE_ROOTSERVER_FREEZE_OB_MAJOR_MERGE_INFO_MANAGER_H_
