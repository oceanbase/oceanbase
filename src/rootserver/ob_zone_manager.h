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

#ifndef OCEANBASE_ROOTSERVER_OB_ZONE_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_ZONE_MANAGER_H_

#include "lib/container/ob_iarray.h"
#include "common/ob_idc.h"
#include "common/ob_zone_type.h"
#include "share/ob_zone_info.h"
#include "share/partition_table/ob_iserver_trace.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include <random>

namespace oceanbase {
namespace obrpc {
class ObAdminZoneArg;
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}  // namespace obrpc
namespace common {
class ObMySQLProxy;
}
namespace share {
class ObIZoneTrace;
}
namespace rootserver {
class ObILeaderCoordinator;
class FakeZoneManager;
class ObLocalityDistribution;
class ObZoneManagerBase : public share::ObIZoneTrace {
public:
  friend class FakeZoneMgr;
  ObZoneManagerBase();
  virtual ~ObZoneManagerBase();
  void reset();

  int init(common::ObMySQLProxy& proxy, ObILeaderCoordinator& leader_coordinator);
  bool is_inited()
  {
    return inited_;
  }
  virtual int check_inner_stat() const;
  virtual int get_zone_count(int64_t& zone_count) const;
  virtual int get_zone(const int64_t idx, share::ObZoneInfo& info) const;           // get zone with idx
  virtual int get_zone(const common::ObZone& zone, share::ObZoneInfo& info) const;  // get with name
  virtual int get_zone(share::ObZoneInfo& info) const;                              // get zone with zone
  virtual int get_zone(const common::ObRegion& region, common::ObIArray<common::ObZone>& zone_list) const;
  virtual int get_active_zone(share::ObZoneInfo& info) const;  // get active zone info with zone
  virtual int get_zone(const share::ObZoneStatus::Status& status, common::ObIArray<common::ObZone>& zone_list) const;
  virtual int get_zone(common::ObIArray<share::ObZoneInfo>& infos) const;
  virtual int get_zone(common::ObIArray<common::ObZone>& zone_list) const;
  virtual int get_region(const common::ObZone& zone, common::ObRegion& region) const;
  virtual int get_all_region(common::ObIArray<common::ObRegion>& region_list) const;
  virtual int get_zone_count(const common::ObRegion& region, int64_t& zone_count) const;
  virtual int get_snapshot(share::ObGlobalInfo& global_info, common::ObIArray<share::ObZoneInfo>& infos) const;
  virtual int check_zone_exist(const common::ObZone& zone, bool& zone_exist) const;
  virtual int check_zone_active(const common::ObZone& zone, bool& zone_active) const;

  virtual int add_zone(const common::ObZone& zone, const common::ObRegion& region, const common::ObIDC& idc,
      const common::ObZoneType& zone_type);
  virtual int delete_zone(const common::ObZone& zone);
  virtual int start_zone(const common::ObZone& zone);
  virtual int stop_zone(const common::ObZone& zone);
  virtual int alter_zone(const obrpc::ObAdminZoneArg& arg);

  virtual int reload();

  virtual bool is_stagger_merge() const;

  virtual int is_merge_error(bool& merge_error) const;
  virtual int set_merge_error(const int64_t merge_error);
  virtual int is_in_merge(bool& merge) const;
  virtual int try_update_global_last_merged_version();
  virtual int generate_next_global_broadcast_version();

  virtual int inc_start_merge_fail_times(const common::ObZone& zone);
  virtual int set_zone_merging(const common::ObZone& zone);
  virtual int clear_zone_merging(const common::ObZone& zone);
  virtual int start_zone_merge(const common::ObZone& zone);
  virtual int finish_zone_merge(
      const common::ObZone& zone, const int64_t merged_version, const int64_t all_merged_version);
  virtual int set_zone_merge_timeout(const common::ObZone& zone);

  virtual int update_privilege_version(const int64_t privilege_version);
  virtual int update_config_version(const int64_t config_version);

  virtual int update_proposal_frozen_version(const int64_t proposal_frozen_version);
  virtual int set_frozen_info(const int64_t frozen_version, const int64_t frozen_time);
  virtual int set_frozen_info(
      common::ObISQLClient& sql_client, const int64_t frozen_version, const int64_t frozen_time);
  virtual int get_frozen_info(int64_t& frozen_version, int64_t& frozen_time) const;
  virtual int get_proposal_frozen_version(int64_t& proposal_frozen_version) const;
  virtual int set_try_frozen_version(const int64_t try_frozen_version);
  virtual int get_try_frozen_version(int64_t& frozen_version, int64_t& try_frozen_version) const;

  virtual int get_cluster(common::ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH>& cluster) const;
  virtual int get_global_broadcast_version(int64_t& global_broadcast_version) const;
  virtual int get_config_version(int64_t& config_version) const;
  virtual int get_time_zone_info_version(int64_t& time_zone_info_version) const;
  virtual int get_lease_info_version(int64_t& lease_info_version) const;
  virtual int get_global_last_merged_version(int64_t& global_last_merged_version) const;
  virtual int64_t get_cluster_create_timestamp() const;

  virtual int check_merge_order(const common::ObString& list_str);
  virtual int check_merge_order(const common::ObIArray<common::ObZone>& merge_list);

  virtual int suspend_merge(const common::ObZone& zone);
  virtual int resume_merge(const common::ObZone& zone);

  virtual int reset_global_merge_status();
  virtual int set_warm_up_start_time(const int64_t time_ts);
  virtual int get_warm_up_start_time(int64_t& time_ts) const;

  virtual int get_snapshot_gc_ts_in_memory(int64_t& gc_timestmap) const;
  virtual int renew_snapshot_gc_ts();
  virtual int get_storage_format_version(int64_t& version) const;
  virtual int set_storage_format_version(const int64_t version);

  DECLARE_TO_STRING;

private:
  int update_zone_status(const common::ObZone& zone, const share::ObZoneStatus::Status& status);
  int inner_try_update_global_last_merged_version();

  // update info item and lease_info_version in one transaction
  int update_value_with_lease(
      const common::ObZone& zone, share::ObZoneInfoItem& item, int64_t value, const char* info = NULL);

  int find_zone(const common::ObZone& zone, int64_t& index) const;
  int suspend_or_resume_merge(const common::ObZone& zone, const bool suspend);

  int update_global_merge_status(share::ObZoneItemTransUpdater& updater);
  int update_global_merge_status(
      const share::ObZoneInfo::MergeStatus zone_status, share::ObZoneItemTransUpdater& updater);
  int construct_zone_region_list(common::ObIArray<share::schema::ObZoneRegion>& zone_region_list,
      const common::ObIArray<common::ObZone>& zone_list);

protected:
  common::SpinRWLock lock_;
  // only used for copying data to/from shadow_
  static int copy_infos(ObZoneManagerBase& dest, const ObZoneManagerBase& src);
  friend class FakeZoneManager;

private:
  bool inited_;
  bool loaded_;
  share::ObZoneInfo zone_infos_[common::MAX_ZONE_NUM];
  int64_t zone_count_;
  int64_t cluster_create_ts_;
  share::ObGlobalInfo global_info_;

  common::ObMySQLProxy* proxy_;
  ObILeaderCoordinator* leader_coordinator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObZoneManagerBase);
};

class ObZoneManager : public ObZoneManagerBase {
public:
  ObZoneManager();
  virtual ~ObZoneManager();

  int init(common::ObMySQLProxy& proxy, ObILeaderCoordinator& leader_coordinator);
  virtual int reload() override;
  virtual int add_zone(const common::ObZone& zone, const common::ObRegion& region, const common::ObIDC& idc,
      const common::ObZoneType& zone_type) override;
  virtual int delete_zone(const common::ObZone& zone) override;
  virtual int start_zone(const common::ObZone& zone) override;
  virtual int stop_zone(const common::ObZone& zone) override;
  virtual int alter_zone(const obrpc::ObAdminZoneArg& arg) override;

  virtual int set_merge_error(const int64_t merge_error) override;
  virtual int try_update_global_last_merged_version() override;
  virtual int generate_next_global_broadcast_version() override;

  virtual int set_zone_merging(const common::ObZone& zone) override;
  virtual int inc_start_merge_fail_times(const common::ObZone& zone) override;
  virtual int clear_zone_merging(const common::ObZone& zone) override;
  virtual int start_zone_merge(const common::ObZone& zone) override;
  virtual int finish_zone_merge(
      const common::ObZone& zone, const int64_t merged_version, const int64_t all_merged_version) override;
  virtual int set_zone_merge_timeout(const common::ObZone& zone) override;

  virtual int update_privilege_version(const int64_t privilege_version) override;
  virtual int update_config_version(const int64_t config_version) override;

  virtual int update_proposal_frozen_version(const int64_t proposal_frozen_version) override;
  virtual int set_frozen_info(const int64_t frozen_version, const int64_t frozen_time) override;
  virtual int set_frozen_info(
      common::ObISQLClient& sql_client, const int64_t frozen_version, const int64_t frozen_time) override;
  virtual int set_try_frozen_version(const int64_t try_frozen_version) override;

  virtual int check_merge_order(const common::ObString& list_str) override;
  virtual int check_merge_order(const common::ObIArray<common::ObZone>& merge_list) override;

  virtual int suspend_merge(const common::ObZone& zone) override;
  virtual int resume_merge(const common::ObZone& zone) override;

  virtual int reset_global_merge_status() override;
  virtual int set_warm_up_start_time(const int64_t time_ts) override;
  std::default_random_engine& get_random_engine()
  {
    return random_;
  }
  virtual int renew_snapshot_gc_ts() override;

  virtual int set_storage_format_version(const int64_t version) override;

public:
  class ObZoneManagerShadowGuard {
  public:
    ObZoneManagerShadowGuard(
        const common::SpinRWLock& lock, ObZoneManagerBase& zone_mgr, ObZoneManagerBase& shadow, int& ret);
    ~ObZoneManagerShadowGuard();

  private:
    common::SpinRWLock& lock_;
    ObZoneManagerBase& zone_mgr_;
    ObZoneManagerBase& shadow_;
    int& ret_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObZoneManagerShadowGuard);
  };
  friend class FakeZoneManager;

private:
  common::SpinRWLock write_lock_;
  ObZoneManagerBase shadow_;
  // this proxy is not inited and
  // used to prevent somebody from trying to directly use the set-interfaces in ObZoneManagerBase
  common::ObMySQLProxy uninit_proxy_;
  std::default_random_engine random_;
};

}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_ZONE_MANAGER_H_
