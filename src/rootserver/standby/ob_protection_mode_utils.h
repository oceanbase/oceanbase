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

#ifndef OCEANBASE_ROOTSERVER_STANDBY_OB_PROTECTION_MODE_UTILS_H
#define OCEANBASE_ROOTSERVER_STANDBY_OB_PROTECTION_MODE_UTILS_H

#include "src/share/ob_tenant_role.h"
#include "src/share/ob_log_restore_proxy.h"
namespace oceanbase {
namespace common
{
class ObMySQLProxy;
}
namespace obrpc
{
class ObLSAccessModeInfo;
}
namespace share {
class ObRestoreSourceServiceAttr;
class ObSyncStandbyStatusAttr;
}
namespace standby
{
class ObProtectionModeUtils
{
public:
  ObProtectionModeUtils() : is_primary_(false), user_tenant_id_(OB_INVALID_TENANT_ID), restore_proxy_util_(), inited_(false) {}
  virtual ~ObProtectionModeUtils() {}
  static int get_tenant_restore_source(const uint64_t user_tenant_id, bool &is_empty,
    share::ObRestoreSourceServiceAttr &restore_source_service_attr);
  static int get_tenant_restore_source(const uint64_t user_tenant_id, ObMySQLTransaction &trans,
    const bool for_update, bool &is_empty,
    share::ObRestoreSourceServiceAttr &restore_source_service_attr);
  static int check_sync_same_with_restore_source(const uint64_t user_tenant_id, bool &equal);
  static int get_tenant_sync_standby_dest(const uint64_t user_tenant_id, ObISQLClient &client,
    const bool for_update, bool &is_empty, share::ObSyncStandbyDestStruct &sync_standby_dest_struct,
    int64_t *ora_rowscn = nullptr);
  static int get_tenant_sync_standby_dest(const uint64_t user_tenant_id, bool &is_empty,
    share::ObSyncStandbyDestStruct &sync_standby_dest_struct, int64_t *ora_rowscn = nullptr);
  static int get_all_ls_largest_end_scn(const uint64_t user_tenant_id, share::SCN &largest_scn);
  static int get_ls_ids_for_protection_mode(const uint64_t user_tenant_id, ObISQLClient &client,
      ObIArray<share::ObLSID> &ls_ids);
  static int check_ls_status_for_upgrade_protection_level(const uint64_t user_tenant_id,
      ObISQLClient &client);
  static int check_tenant_data_version_for_protection_mode(const uint64_t tenant_id, bool &enable);
  static int wait_protection_stat_steady(const uint64_t tenant_id,
      const share::ObProtectionMode &expected_protection_mode,
      common::ObMySQLProxy *sql_proxy);
  static bool check_cluster_version_for_protection_mode();
  static int get_sync_standby_status_attr(const uint64_t user_tenant_id,
      const int64_t switchover_epoch, share::ObSyncStandbyStatusAttr &sync_standby_status_attr);
  static int64_t get_protection_mode_data_version() { return DATA_VERSION_4_4_2_1; }
  static int64_t get_protection_mode_cluster_version() { return CLUSTER_VERSION_4_4_2_1; }
  bool check_user_is_self(const share::ObRestoreSourceServiceAttr &restore_source_service_attr) const;
  int init(const uint64_t user_tenant_id);
  int get_log_restore_source(bool &is_empty, share::ObRestoreSourceServiceAttr &restore_source_service_attr);
  int check_log_restore_source_is_self();
  int wait_standby_tenant_sync(const ObTimeoutCtx &ctx);
  int check_standby_tenant_sync(bool &is_sync);
  int get_tenant_info(share::ObAllTenantInfo &tenant_info);
private:
  static int get_all_ls_end_scn_info_(const uint64_t user_tenant_id, share::SCN &largest_scn,
      share::SCN &smallest_scn);
  int check_inner_stat_() const;
private:
  bool is_primary_;
  uint64_t user_tenant_id_;
  share::ObLogRestoreProxyUtil restore_proxy_util_;
  bool inited_;
};
}
}

#endif // OCEANBASE_ROOTSERVER_STANDBY_OB_PROTECTION_MODE_UTILS_H