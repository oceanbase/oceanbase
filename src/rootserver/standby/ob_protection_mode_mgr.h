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

#ifndef OCEANBASE_ROOTSERVER_STANDBY_OB_PROTECTION_MODE_MGR_H_
#define OCEANBASE_ROOTSERVER_STANDBY_OB_PROTECTION_MODE_MGR_H_

#include "lib/container/ob_array.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"
#include "share/ob_sync_standby_dest_parser.h"
#include "share/ob_tenant_role.h"
#include "logservice/palf/palf_handle_impl.h"
#include "logservice/palf/palf_options.h"
namespace oceanbase
{
namespace share
{
class ObLogRestoreProxyUtil;
class ObAllTenantInfo;
struct ObLSRecoveryStat;
}
namespace standby
{
#define K_SCN(x) K(x), #x "_display", common::ObTime2Str::ob_timestamp_str(x.convert_to_ts(true))
#define K_SCN_(x) K(x##_), #x "_display", common::ObTime2Str::ob_timestamp_str(x##_.convert_to_ts(true))
class ObMAProtectionLevelAutoSwitchHelper
{
public:
  struct ObLSStandbySyncScnInfo
  {
    ObLSStandbySyncScnInfo() : ls_id_(), standby_sync_scn_(), palf_end_scn_(),
      ls_add_ts_(OB_INVALID_TIMESTAMP) {}
    bool is_valid() const;
    bool is_ls_ts_valid() const;
    int assign(const ObLSStandbySyncScnInfo &other);
    int init(const obrpc::ObGetLSStandbySyncScnRes &res, const int64_t ls_add_ts);
    int init(const share::ObLSRecoveryStat &ls_recovery, const int64_t ls_add_ts);
    int init_empty(const share::ObLSID &ls_id, const int64_t ls_add_ts);
    TO_STRING_KV(K_(ls_id), K_SCN_(standby_sync_scn), K_SCN_(palf_end_scn), KTIME_(ls_add_ts));
    share::ObLSID ls_id_;
    share::SCN standby_sync_scn_;
    share::SCN palf_end_scn_;
    int64_t ls_add_ts_;
  };
public:
  ObMAProtectionLevelAutoSwitchHelper();
  virtual ~ObMAProtectionLevelAutoSwitchHelper() { reset(); }
  bool is_inited() const { return inited_; }
  int init(const uint64_t standby_cluster_id, const uint64_t standby_tenant_id);
  void reset();
  void dump() const;
  bool match_standby_dest(const uint64_t standby_cluster_id, const uint64_t standby_tenant_id) const;
  int refresh_ls_standby_sync_scn();
  bool need_downgrade(const int64_t net_timeout) const;
  bool can_upgrade(const int64_t net_timeout_us, const int64_t health_check_time_us);
  TO_STRING_KV(K_(standby_cluster_id), K_(standby_tenant_id), KTIME_(init_ts),
      KTIME_(last_upgrade_check_fail_ts), K_(inited), K_(ls_standby_sync_scn_info_array));
private:
  int build_init_ls_sync_info_array_(
      const int64_t now,
      ObIArray<ObLSStandbySyncScnInfo> &ls_sync_info_array) const;
  int check_inner_stat_() const;
  bool need_downgrade_by_ls_(const ObLSStandbySyncScnInfo &ls_info, const int64_t now,
      const int64_t net_timeout_us) const;
  bool can_upgrade_by_ls_(const ObLSStandbySyncScnInfo &ls_info, const int64_t now,
      const int64_t net_timeout_us) const;
  int get_new_ls_standby_sync_info_(const share::ObLSID &ls_id, const ObLSStandbySyncScnInfo &old_ls_info,
      const obrpc::ObGetLSStandbySyncScnRes &res, ObLSStandbySyncScnInfo &new_ls_info, const int64_t now) const;
  int get_ls_sync_res_by_ls_id_(const share::ObLSID &ls_id,
      const ObIArray<int> &return_code,
      const ObIArray<obrpc::ObGetLSStandbySyncScnArg> &args,
      const ObIArray<const obrpc::ObGetLSStandbySyncScnRes *> &res_array,
      obrpc::ObGetLSStandbySyncScnRes &res) const;
  int get_sync_info_by_ls_id_(const share::ObLSID &ls_id,
      const ObIArray<ObLSStandbySyncScnInfo> &sync_info_array,
      ObLSStandbySyncScnInfo &sync_info) const;
  int update_sync_info_array_(const ObIArray<int> &return_code,
      const ObIArray<obrpc::ObGetLSStandbySyncScnArg> &args,
      const ObIArray<const obrpc::ObGetLSStandbySyncScnRes *> &res_array,
      const ObIArray<ObLSStatusInfo> &ls_status_array);
private:
  uint64_t standby_cluster_id_;
  uint64_t standby_tenant_id_;
  int64_t init_ts_;
  int64_t last_upgrade_check_fail_ts_;
  bool inited_;
  // lock_ protects helper shared in-memory state. Caller must not hold it across SQL/RPC.
  mutable ObLatch lock_;
  common::ObArray<ObLSStandbySyncScnInfo> ls_standby_sync_scn_info_array_;
};

class ObProtectionModeChangeHelper
{
public:
  ObProtectionModeChangeHelper();
  virtual ~ObProtectionModeChangeHelper();
  int init(const uint64_t user_tenant_id, const share::ObProtectionStat &meta_protection_stat);
  void reset();
  bool is_inited() const { return inited_; }
  int downgrade_protection_mode(int64_t &new_switchover_epoch);
  int upgrade_protection_mode() const;
private:
  int downgrade_protection_mode_set_sync_mode_(
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::SyncMode &target_sync_mode,
    const ObTimeoutCtx &ctx,
    const share::SCN &ref_scn,
    ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array) const;
  int check_all_ls_set_sync_mode_(
    const ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array,
    const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    bool &all_ls_set_sync_mode) const;
  int get_access_info_from_result_(const palf::SyncMode target_sync_mode, const ObIArray<obrpc::ObChangeLSSyncModeRes> &results,
      ObIArray<obrpc::ObLSAccessModeInfo> &access_info) const;
  int check_self_leader_() const;
  int check_status_not_changed_() const;
  int check_inner_stat_() const;
  int get_proposal_id_(int64_t &proposal_id) const;
  int get_ls_ids_(ObISQLClient &client, ObIArray<ObLSID> &ls_ids) const;
  int get_sync_mode_(const ObIArray<ObLSID> &ls_ids,
     ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info) const;
  int set_sync_mode_(const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::SyncMode &sync_mode, const share::SCN &ref_scn,
    ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array) const;
  int set_sync_mode_until_suceess_(const ObIArray<obrpc::ObLSAccessModeInfo> &ls_access_info,
    const palf::SyncMode &sync_mode, const share::SCN &ref_scn, const int64_t timeout,
    ObIArray<obrpc::ObChangeLSSyncModeRes> &change_ls_sync_mode_res_array) const;
  bool check_mode_version_not_change_(const ObIArray<obrpc::ObLSAccessModeInfo> &origin_ls_access_infos,
    const ObIArray<obrpc::ObLSAccessModeInfo> &current_ls_access_infos) const;
  int check_gts_advanced_(const ObTimeoutCtx &ctx) const;
  int splite_sys_ls_from_all_ls_(const ObIArray<obrpc::ObLSAccessModeInfo> &all_ls_access_info,
     ObIArray<obrpc::ObLSAccessModeInfo> &sys_ls_access_info,
     ObIArray<obrpc::ObLSAccessModeInfo> &other_ls_access_info) const;

private:
  uint64_t user_tenant_id_;
  int64_t proposal_id_;
  share::ObProtectionStat meta_protection_stat_;
private:
  // used for upgrade
  bool inited_;
};

class ObSyncStandbyDestCache
{
public:
  ObSyncStandbyDestCache();
  ~ObSyncStandbyDestCache() { reset(); }
  int init();
  void reset();
  int clear_cache();
  int get_sync_standby_dest(bool &is_empty, share::ObSyncStandbyDestStruct &sync_standby_dest);
private:
  static const int64_t CACHE_UPDATE_INTERVAL_US = 10L * 1000L * 1000L;
  int check_inner_stat_() const;
  bool need_refresh_cache_(const int64_t now) const;
private:
  bool inited_;
  int64_t cache_last_update_ts_;
  int64_t sync_standby_dest_ora_rowscn_;
  bool is_empty_;
  mutable ObLatch lock_;
  share::ObSyncStandbyDestStruct sync_standby_dest_;
};

class ObProtectionModeMgr : public rootserver::ObTenantThreadHelper,
                            public logservice::ObICheckpointSubHandler,
                            public logservice::ObIReplaySubHandler
{
public:
  ObProtectionModeMgr();
  ~ObProtectionModeMgr();
  int init();
  void destroy();
  virtual void do_work() override;
  DEFINE_MTL_FUNC(ObProtectionModeMgr)
public:
  // interface for logservice, no use
  virtual share::SCN get_rec_scn() override { return share::SCN::max_scn();}
  virtual int flush(share::SCN &scn) override { return OB_SUCCESS; }
  int replay(const void *buffer, const int64_t nbytes, const palf::LSN &lsn, const share::SCN &scn) override
  {
    UNUSEDx(buffer, nbytes, lsn, scn);
    return OB_SUCCESS;
  }
public:
  static int notify_protection_stat_for_standby_tenant(const uint64_t user_tenant_id,
    const share::ObProtectionStat &meta_protection_stat,
    const share::ObProtectionStat &user_protection_stat);
  int clear_sync_standby_dest_cache();
  // change protection mode/level in tenant_info by get_next_protection_level
  static int advance_meta_tenant_protection_stat(const uint64_t meta_tenant_id,
    const share::ObProtectionStat &meta_protection_stat, ObISQLClient *proxy);
private:
  int load_protection_mode_stat_(share::ObProtectionStat &meta_protection_stat) const;
  static int update_meta_switchover_epoch_by_user_(const uint64_t user_tenant_id,
    const share::ObProtectionStat &meta_protection_stat,
    const share::ObProtectionStat &user_protection_stat,
    share::ObProtectionStat &new_meta_protection_stat);
  int advance_upgrade_protection_mode_(share::ObProtectionStat &meta_protection_stat, bool &status_changed) const;
  int advance_downgrade_protection_mode_(share::ObProtectionStat &meta_protection_stat, bool &status_changed) const;
  void process_upgrade_thread_(share::ObProtectionStat &meta_protection_stat,
      const bool sync_standby_dest_is_empty,
      const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed);
  void process_downgrade_thread_(share::ObProtectionStat &meta_protection_stat,
      const bool sync_standby_dest_is_empty,
      const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed);
  int ma_try_upgrade_from_re_(share::ObProtectionStat &meta_protection_stat,
      const bool sync_standby_dest_is_empty,
      const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed);
  int ma_refresh_standby_sync_scn_(const share::ObProtectionStat &meta_protection_stat,
      const bool sync_standby_dest_is_empty,
      const share::ObSyncStandbyDestStruct &sync_standby_dest_struct);
  int ma_downgrade_level_if_sync_lag_(share::ObProtectionStat &meta_protection_stat,
      const bool sync_standby_dest_is_empty,
      const share::ObSyncStandbyDestStruct &sync_standby_dest_struct, bool &status_changed);
  int trigger_ma_upgrade_(const share::ObProtectionStat &meta_protection_stat) const;
private:
  const int64_t PROTECTION_MODE_MGR_IDLE_INTERVAL = 1000 * 1000L;
private:
  uint64_t user_tenant_id_;
  bool inited_;
  ObMAProtectionLevelAutoSwitchHelper ma_switch_helper_;
  ObSyncStandbyDestCache sync_standby_dest_cache_;
};
#undef K_SCN
#undef K_SCN_
} // namespace standby
} // namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_STANDBY_OB_PROTECTION_MODE_MGR_H_
