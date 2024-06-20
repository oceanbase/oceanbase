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

#ifndef OCEANBASE_SHARE_OB_FREEZE_INFO_MANAGER_H_
#define OCEANBASE_SHARE_OB_FREEZE_INFO_MANAGER_H_

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

class ObFreezeInfoList
{
public:
  // local cached freeze status >= global_broadcast_scn,
  // and stored order by freeze_scn asc
  // only kept last 32 freeze infos in __all_freeze_info
  common::ObSEArray<share::ObFreezeInfo, 32> frozen_statuses_;
  // local cached latest snapshot gc scn
  share::SCN latest_snapshot_gc_scn_;

  ObFreezeInfoList()
    : frozen_statuses_(), latest_snapshot_gc_scn_(share::SCN::min_scn())
  {}
  ~ObFreezeInfoList() {}

  void set_invalid() { reset(); }

  bool is_valid() const { return !frozen_statuses_.empty() && (latest_snapshot_gc_scn_.is_valid()); }
  bool empty() const { return frozen_statuses_.empty(); }
  int64_t count() const { return frozen_statuses_.count(); }
  void reset()
  {
    frozen_statuses_.reset();
    latest_snapshot_gc_scn_.reset();
  }

  int assign(const ObFreezeInfoList &other);
  int get_latest_frozen_scn(share::SCN &frozen_scn) const;
  int get_latest_freeze_info(share::ObFreezeInfo &freeze_info) const;
  int get_min_freeze_info_greater_than(
      const share::SCN &frozen_scn,
      share::ObFreezeInfo &freeze_info) const;
  int get_freeze_info(
      const share::SCN &frozen_scn,
      share::ObFreezeInfo &freeze_info,
      int64_t &idx) const;

  TO_STRING_KV(K_(latest_snapshot_gc_scn), K(frozen_statuses_.count()), K_(frozen_statuses));
};


/* ObFreezeInfoManager(tenant level)
 * Only opt freeze info && snapshot_gc_scn
 * The Concurrency should be controlled by the upper layer
 */
class ObFreezeInfoManager
{
public:
  static int fetch_new_freeze_info(
      const int64_t tenant_id,
      const share::SCN &min_frozen_scn,
      common::ObMySQLProxy &sql_proxy,
      common::ObIArray<ObFreezeInfo> &freeze_infos,
      share::SCN &latest_snapshot_gc_scn);

  ObFreezeInfoManager()
    : is_inited_(false),
      tenant_id_(common::OB_INVALID_ID),
      sql_proxy_(nullptr),
      freeze_info_()
  {}
  virtual ~ObFreezeInfoManager() {}
  bool is_valid() const {   return is_inited_ && freeze_info_.is_valid(); }
  int64_t get_freeze_info_count() const { return freeze_info_.frozen_statuses_.count(); }
  share::SCN get_snapshot_gc_scn() const { return freeze_info_.latest_snapshot_gc_scn_; }
  void reset_freeze_info() { freeze_info_.set_invalid(); }
  int init(uint64_t tenant_id, common::ObMySQLProxy &proxy);
  int reload(const share::SCN &min_frozen_scn);
  int update_freeze_info(
      const common::ObIArray<ObFreezeInfo> &freeze_infos,
      const share::SCN &latest_snapshot_gc_scn);
  int add_freeze_info(const share::ObFreezeInfo &frozen_status);
  int update_snapshot_gc_scn(const share::SCN &new_snapshot_gc_scn);

  share::SCN get_latest_frozen_scn();
  int get_freeze_info(const share::SCN &frozen_scn,
                      share::ObFreezeInfo &frozen_status);
  int get_latest_freeze_info(share::ObFreezeInfo &frozen_status);
  int get_freeze_info_by_idx(const int64_t idx, share::ObFreezeInfo &frozen_status);

  int get_freeze_info_by_major_snapshot(
      const int64_t snapshot_version,
      ObIArray<share::ObFreezeInfo> &info_list,
      const bool need_all_behind_info);

  int get_freeze_info_behind_major_snapshot(
      const int64_t snapshot_version,
      share::ObFreezeInfo &frozen_status);

  int get_neighbour_frozen_status(
      const int64_t snapshot_version,
      share::ObFreezeInfo &prev_frozen_status,
      share::ObFreezeInfo &next_frozen_status);

  int get_min_freeze_info_greater_than(
      const share::SCN &frozen_scn,
      share::ObFreezeInfo &frozen_status);

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(freeze_info));
private:
  bool is_inited_;
  int64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  ObFreezeInfoList freeze_info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObFreezeInfoManager);
};

} // share
} // oceanbase
#endif // OCEANBASE_SHARE_OB_FREEZE_INFO_MANAGER_H_
