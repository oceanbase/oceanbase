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

#ifndef OCEANBASE_ROOTSERVER_STANDBY_OB_LS_LOG_MODE_MODIFIER_H_
#define OCEANBASE_ROOTSERVER_STANDBY_OB_LS_LOG_MODE_MODIFIER_H_

#include "share/ob_rpc_struct.h"
#include "logservice/palf/palf_options.h"
#include "logservice/palf/log_define.h"
#include "share/ls/ob_ls_status_operator.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class SCN;
struct ObAllTenantInfo;
}

namespace rootserver
{
using namespace share;

class ObLSLogModeModifier {
public:
  ObLSLogModeModifier(const uint64_t tenant_id, const uint64_t switchover_epoch, const SCN &ref_scn,
    const SCN &sys_ls_sync_scn, const palf::AccessMode target_access_mode, const palf::SyncMode target_sync_mode,
    const share::ObLSStatusInfoArray *status_info_array,
    common::ObMySQLProxy *sql_proxy, obrpc::ObSrvRpcProxy *rpc_proxy)
    : tenant_id_(tenant_id), switchover_epoch_(switchover_epoch), ls_wait_sync_scn_max_(0),
      ref_scn_(ref_scn), sys_ls_sync_scn_(sys_ls_sync_scn), target_access_mode_(target_access_mode),
      target_sync_mode_(target_sync_mode), status_info_array_(status_info_array),
      sql_proxy_(sql_proxy), rpc_proxy_(rpc_proxy) {}
  ~ObLSLogModeModifier() {}
  // if current access mode is flashback, it'll change sync mode, then change access mode to append
  int change_ls_access_mode();

  static int change_ls_sync_mode(const uint64_t tenant_id,
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::SyncMode target_sync_mode,
    const SCN &ref_scn,
    const share::ObSyncStandbyStatusAttr &protection_log,
    ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array);
  static int change_ls_sync_mode_and_check_result(const uint64_t tenant_id,
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::SyncMode target_sync_mode,
    const share::ObSyncStandbyStatusAttr &protection_log,
    const SCN &ref_scn);
  static int change_ls_access_mode(const uint64_t tenant_id,
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::AccessMode target_access_mode,
    const SCN &ref_scn,
    const SCN &sys_ls_sync_scn, // used for switchover to standby, to let user ls sync_scn larger than sys_ls
    ObIArray<obrpc::ObChangeLSAccessModeRes> &change_ls_access_mode_res_array);
  int64_t get_ls_wait_sync_scn_max() const { return ls_wait_sync_scn_max_; }
  static int get_ls_access_mode(const uint64_t tenant_id, const ObIArray<share::ObLSID> &ls_ids,
    ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info);
  int get_ls_access_mode(ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info);
  static int change_ls_sync_mode_until_success(const uint64_t tenant_id,
    const ObIArray<share::ObLSID> &ls_ids, const palf::SyncMode &target_sync_mode, const SCN &ref_scn,
    const share::ObSyncStandbyStatusAttr &protection_log, const int64_t switchover_epoch,
    ObIArray<obrpc::ObChangeLSSyncModeRes> &results, const bool force_check_result = true);
  static int change_ls_access_mode_until_success(const uint64_t tenant_id,
    const ObIArray<share::ObLSID> &ls_ids, const palf::AccessMode &target_access_mode, const SCN &ref_scn,
    const SCN &sys_ls_sync_scn, const int64_t switchover_epoch);
private:
  template<typename TARGET_MODE, typename Arg, typename Res>
  static int change_ls_mode_until_success(const uint64_t tenant_id,
      const ObIArray<share::ObLSID> &ls_ids, const TARGET_MODE &target_mode, const SCN &ref_scn,
      const Arg &arg, const int64_t switchover_epoch, const bool force_check_result,
      ObIArray<Res> &results);
  // should be an alias of change_ls_sync_mode
  static int change_ls_mode(const uint64_t tenant_id,
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::SyncMode target_sync_mode,
    const SCN &ref_scn,
    const share::ObSyncStandbyStatusAttr &protection_log,
    ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array);
  static int change_ls_mode(const uint64_t tenant_id,
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::AccessMode target_access_mode,
    const SCN &ref_scn,
    const SCN &sys_ls_sync_scn,
    ObIArray<obrpc::ObChangeLSAccessModeRes> &change_ls_access_mode_res_array);
  template<typename Arg, typename Res, typename Proxy>
  static int call_and_renew_location(const uint64_t tenant_id,
    const ObIArray<Arg> &args,
    Proxy &proxy,
    ObIArray<Res> &results);
  template<typename Arg, typename Res, typename Proxy>
  static int do_change_mode_(const uint64_t tenant_id,
    const ObIArray<Arg> &args,
    Proxy &proxy,
    ObIArray<Res> &results);
private:
  int check_inner_stat_() const;
  uint64_t tenant_id_;
  uint64_t switchover_epoch_;
  int64_t ls_wait_sync_scn_max_;
  SCN ref_scn_;
  SCN sys_ls_sync_scn_;
  palf::AccessMode target_access_mode_;
  palf::SyncMode target_sync_mode_;
  const share::ObLSStatusInfoArray *status_info_array_;
  common::ObMySQLProxy *sql_proxy_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
};
}
}

#endif // OCEANBASE_ROOTSERVER_STANDBY_OB_LS_LOG_MODE_MODIFIER_H_
