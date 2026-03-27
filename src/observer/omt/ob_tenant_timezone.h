/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TENANT_TIMEZONE_H_
#define OCEANBASE_TENANT_TIMEZONE_H_

#include "src/share/ob_time_zone_info_manager.h"

namespace oceanbase {

namespace omt {

class ObTenantTimezoneMgr;

class ObTenantTimezone
{
  friend class ObTenantTimezoneMgr;
public:
  ObTenantTimezone(common::ObMySQLProxy &sql_proxy, uint64_t tenant_id);
  virtual ~ObTenantTimezone();
  ObTenantTimezone(const ObTenantTimezone &)=delete;
  ObTenantTimezone &operator=(const ObTenantTimezone &)=delete;

  int init();
  int update_timezone(int64_t tz_version);

  bool is_inited() { return is_inited_; }
  bool get_update_task_not_exist() { return update_task_not_exist_; }
  int get_ref_count(int64_t &ref_count);
  uint64_t get_tenant_id() const { return tenant_id_; }
  common::ObTZInfoMap *get_tz_map() { return tz_info_mgr_.get_tz_info_map(); }
  common::ObTimeZoneInfoManager &get_tz_mgr() { return tz_info_mgr_; }

  void set_update_task_not_exist() { update_task_not_exist_ = true; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(tenant_id));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObTimeZoneInfoManager tz_info_mgr_;
  bool update_task_not_exist_;
};


} // omt
} // oceanbase

#endif
