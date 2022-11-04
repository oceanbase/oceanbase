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

#ifndef OCEANBASE_TENANT_TIMEZONE_H_
#define OCEANBASE_TENANT_TIMEZONE_H_

#include "lib/timezone/ob_timezone_info.h"

namespace oceanbase {

namespace omt {

class ObTenantTimezoneMgr;

class ObTenantTimezone
{
  friend class ObTenantTimezoneMgr;
public:
  ObTenantTimezone();
  ObTenantTimezone(uint64_t tenant_id);
  virtual ~ObTenantTimezone();
  ObTenantTimezone(const ObTenantTimezone &)=delete;
  ObTenantTimezone &operator=(const ObTenantTimezone &)=delete;

  int init(ObTenantTimezoneMgr *tz_mgr);
  int update_timezone(int64_t tz_version);

  bool is_inited() { return is_inited_; }
  bool get_update_task_not_exist() { return update_task_not_exist_; }
  int get_ref_count(int64_t &ref_count);
  uint64_t get_tenant_id() const { return tenant_id_; }
  common::ObTZInfoMap *get_tz_map() { return tz_info_map_; }
  common::ObTimeZoneInfoManager *get_tz_mgr() { return tz_info_mgr_; }

  void set_update_task_not_exist() { update_task_not_exist_ = true; }
  void set_tz_mgr(common::ObTimeZoneInfoManager *tz_mgr) { tz_info_mgr_ = tz_mgr; }
  void set_tenant_tz_mgr(ObTenantTimezoneMgr *tz_mgr) { tenant_tz_mgr_ = tz_mgr; };
  void destroy();
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(tenant_id));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObTenantTimezoneMgr *tenant_tz_mgr_;
  common::ObTimeZoneInfoManager *tz_info_mgr_;
  common::ObTZInfoMap *tz_info_map_;
  bool update_task_not_exist_;
};


} // omt
} // oceanbase

#endif
