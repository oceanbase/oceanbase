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
#include "share/ob_iserver_trace.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_cluster_role.h"
#include <random>

namespace oceanbase
{
namespace obrpc
{
class ObAdminZoneArg;
class ObSrvRpcProxy;
class ObCommonRpcProxy;
}
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObIZoneTrace;
}
namespace rootserver
{
class FakeZoneManager;
class ObLocalityDistribution;
class ObZoneManagerBase : public share::ObIZoneTrace
{
public:
  friend class FakeZoneMgr;
  ObZoneManagerBase();
  virtual ~ObZoneManagerBase();
  void reset();

  int init(common::ObMySQLProxy &proxy);
  bool is_inited() { return inited_; }
  virtual int check_inner_stat() const;
  virtual int get_zone_count(int64_t &zone_count) const;
  virtual int get_zone(const int64_t idx, share::ObZoneInfo &info) const; // get zone with idx
  virtual int get_zone(const common::ObZone &zone, share::ObZoneInfo &info) const; // get with name
  virtual int get_zone(share::ObZoneInfo &info) const; // get zone with zone
  virtual int get_zone(const common::ObRegion &region, common::ObIArray<common::ObZone> &zone_list) const;
  virtual int get_active_zone(share::ObZoneInfo &info) const; // get active zone info with zone
  virtual int get_zone(const share::ObZoneStatus::Status &status,
      common::ObIArray<common::ObZone> &zone_list) const;
  virtual int get_zone(common::ObIArray<share::ObZoneInfo> &infos) const;
  virtual int get_zone(common::ObIArray<common::ObZone> &zone_list) const;
  virtual int get_region(const common::ObZone &zone, common::ObRegion &region) const;
  virtual int get_all_region(common::ObIArray<common::ObRegion> &region_list) const;
  virtual int get_zone_count(const common::ObRegion &region, int64_t &zone_count) const;
  virtual int check_zone_exist(const common::ObZone &zone, bool &zone_exist) const;
  virtual int check_zone_active(const common::ObZone &zone, bool &zone_active) const;

  virtual int add_zone(const common::ObZone &zone, const common::ObRegion &region,
                       const common::ObIDC &idc, const common::ObZoneType &zone_type);
  virtual int delete_zone(const common::ObZone &zone);
  virtual int start_zone(const common::ObZone &zone);
  virtual int stop_zone(const common::ObZone &zone);
  virtual int alter_zone(const obrpc::ObAdminZoneArg &arg);

  virtual int reload();

  virtual int update_privilege_version(const int64_t privilege_version);
  virtual int update_config_version(const int64_t config_version);
  virtual int update_recovery_status(const common::ObZone &zone,
                                     const share::ObZoneInfo::RecoveryStatus status);

  virtual int set_cluster_name(const ObString &cluster_name);

  virtual int get_cluster(common::ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH> &cluster) const;
  virtual int get_config_version(int64_t &config_version) const;
  virtual int get_time_zone_info_version(int64_t &time_zone_info_version) const;
  virtual int get_lease_info_version(int64_t &lease_info_version) const;
  virtual common::ObClusterRole get_cluster_role();

  virtual int check_encryption_zone(const common::ObZone &zone, bool &encryption);
  virtual int get_storage_format_version(int64_t &version) const;
  virtual int set_storage_format_version(const int64_t version);

  DECLARE_TO_STRING;

private:
  int update_zone_status(const common::ObZone &zone, const share::ObZoneStatus::Status &status);

  // update info item and lease_info_version in one transaction
  int update_value_with_lease(const common::ObZone &zone, share::ObZoneInfoItem &item,
      int64_t value, const char *info = NULL);

  int find_zone(const common::ObZone &zone, int64_t &index) const;

  int construct_zone_region_list(
      common::ObIArray<share::schema::ObZoneRegion> &zone_region_list,
      const common::ObIArray<common::ObZone> &zone_list);

protected:
  common::SpinRWLock lock_;
  // only used for copying data to/from shadow_
  static int copy_infos(ObZoneManagerBase &dest, const ObZoneManagerBase &src);
  friend class FakeZoneManager;
private:
  bool inited_;
  bool loaded_;
  share::ObZoneInfo zone_infos_[common::MAX_ZONE_NUM];
  int64_t zone_count_;
  share::ObGlobalInfo global_info_;

  common::ObMySQLProxy *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObZoneManagerBase);
};

class ObZoneManager : public ObZoneManagerBase
{
public:
  ObZoneManager();
  virtual ~ObZoneManager();

  int init(common::ObMySQLProxy &proxy);
  virtual int reload();
  virtual int add_zone(const common::ObZone &zone, const common::ObRegion &region,
                       const common::ObIDC &idc, const common::ObZoneType &zone_type);
  virtual int delete_zone(const common::ObZone &zone);
  virtual int start_zone(const common::ObZone &zone);
  virtual int stop_zone(const common::ObZone &zone);
  virtual int alter_zone(const obrpc::ObAdminZoneArg &arg) override;

  virtual int update_privilege_version(const int64_t privilege_version);
  virtual int update_config_version(const int64_t config_version);
  virtual int update_recovery_status(const common::ObZone &zone,
                                     const share::ObZoneInfo::RecoveryStatus status);

  virtual int set_cluster_name(const ObString &cluster_name) override;
  std::default_random_engine &get_random_engine() { return random_; }

  virtual int set_storage_format_version(const int64_t version);
public:
  class ObZoneManagerShadowGuard
  {
  public:
    ObZoneManagerShadowGuard(const common::SpinRWLock &lock,
                             ObZoneManagerBase &zone_mgr,
                             ObZoneManagerBase &shadow,
                             int &ret);
    ~ObZoneManagerShadowGuard();
  private:
    common::SpinRWLock &lock_;
    ObZoneManagerBase &zone_mgr_;
    ObZoneManagerBase &shadow_;
    int &ret_;
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

}// rootserver
}// oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_ZONE_MANAGER_H_
