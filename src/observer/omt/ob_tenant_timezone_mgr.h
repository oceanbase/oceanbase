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

#ifndef OCEANBASE_TENANT_TIMEZONE_MGR_H_
#define OCEANBASE_TENANT_TIMEZONE_MGR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"
#include "share/ob_lease_struct.h"
#include "share/rc/ob_context.h"
#include "ob_tenant_timezone.h"
#include "share/ob_time_zone_info_manager.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share
namespace omt {

class ObTenantTimezoneMgr {
private:
  template <class Key, class Value, int num>
  class __ObTimezoneContainer : public common::hash::ObHashMap<Key, Value*> {
  public:
    __ObTimezoneContainer()
    {
      this->create(num, oceanbase::common::ObModIds::OB_HASH_BUCKET_TIME_ZONE_INFO_MAP, "HasNodTzInfM");
    }
    virtual ~__ObTimezoneContainer()
    {}

  private:
    DISALLOW_COPY_AND_ASSIGN(__ObTimezoneContainer);
  };
  // Obtain all_tenant_ids regularly and update the timezone_map in mgr.
  class UpdateAllTenantTask : public common::ObTimerTask {
  public:
    UpdateAllTenantTask(ObTenantTimezoneMgr* tenant_tz_mgr) : tenant_tz_mgr_(tenant_tz_mgr)
    {}
    virtual ~UpdateAllTenantTask()
    {}
    UpdateAllTenantTask(const UpdateAllTenantTask&) = delete;
    UpdateAllTenantTask& operator=(const UpdateAllTenantTask&) = delete;
    void runTimerTask(void) override;
    int update_tenant_map(common::ObIArray<uint64_t>& latest_tenant_ids);

    ObTenantTimezoneMgr* tenant_tz_mgr_;
    const uint64_t SLEEP_USECONDS = 5000000;
  };
  class DeleteTenantTZTask : public common::ObTimerTask {
  public:
    DeleteTenantTZTask(ObTenantTimezoneMgr* tenant_tz_mgr) : tenant_tz_mgr_(tenant_tz_mgr)
    {}
    int init(ObTenantTimezoneMgr* tz_mgr);
    virtual ~DeleteTenantTZTask()
    {}
    void runTimerTask(void) override;

    ObTenantTimezoneMgr* tenant_tz_mgr_;
    const uint64_t SLEEP_USECONDS = 60000000;
  };
  friend UpdateAllTenantTask;
  friend DeleteTenantTZTask;

public:
  using TenantTimezoneMap = __ObTimezoneContainer<uint64_t, ObTenantTimezone, common::OB_MAX_SERVER_TENANT_CNT>;
  typedef int (*tenant_timezone_map_getter)(const uint64_t tenant_id, common::ObTZMapWrap& timezone_wrap);

  virtual ~ObTenantTimezoneMgr();
  ObTenantTimezoneMgr(const ObTenantTimezoneMgr& timezone) = delete;
  ObTenantTimezoneMgr& operator=(const ObTenantTimezoneMgr&) = delete;

  static ObTenantTimezoneMgr& get_instance();
  int init(common::ObMySQLProxy& sql_proxy, const common::ObAddr& server,
      share::schema::ObMultiVersionSchemaService& schema_service);
  // init interface for liboblog only.
  void init(tenant_timezone_map_getter tz_map_getter);
  int add_tenant_timezone(uint64_t tenant_id);
  int del_tenant_timezone(uint64_t tenant_id);

  // observer and liboblog get tenant tz map with the following function.
  int get_tenant_tz(const uint64_t tenant_id, common::ObTZMapWrap& timezone_wrap);
  int refresh_tenant_timezone(const uint64_t tenant_id);
  int get_tenant_timezone(
      const uint64_t tenant_id, common::ObTZMapWrap& timezone_wrap, common::ObTimeZoneInfoManager*& tz_info_mgr);
  int remove_nonexist_tenant();
  int add_new_tenants(const common::ObIArray<uint64_t>& latest_tenant_ids);
  int update_timezone_map();
  int delete_tenant_timezone();
  int cancel(const ObTenantTimezone::TenantTZUpdateTask& task);
  bool is_inited()
  {
    return is_inited_;
  }
  bool get_start_refresh()
  {
    return start_refresh_;
  }
  void set_start_refresh(bool start)
  {
    start_refresh_ = start;
  }
  bool is_usable()
  {
    return usable_;
  }
  void set_usable()
  {
    usable_ = true;
  }
  void destroy();

private:
  int get_tenant_timezone_inner(
      const uint64_t tenant_id, common::ObTZMapWrap& timezone_wrap, common::ObTimeZoneInfoManager*& tz_info_mgr);
  // static function of calling instance().get_tenant_timezone_map(). For init tenant_tz_map_getter_
  static int get_tenant_timezone_static(const uint64_t tenant_id, common::ObTZMapWrap& timezone_wrap);

  static int get_tenant_timezone_default(const uint64_t tenant_id, common::ObTZMapWrap& timezone_wrap);

private:
  ObTenantTimezoneMgr();
  common::ObArenaAllocator allocator_;
  bool is_inited_;
  common::ObAddr self_;
  common::ObMySQLProxy* sql_proxy_;
  // protect timezone_map_
  common::ObLatch rwlock_;
  TenantTimezoneMap timezone_map_;
  UpdateAllTenantTask update_task_;
  DeleteTenantTZTask delete_task_;
  bool start_refresh_;
  bool usable_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  // tenants which have been dropped, waiting for ref_count = 0 and delete them.
  common::ObList<ObTenantTimezone*, common::ObArenaAllocator> drop_tenant_tz_;

public:
  // tenant timezone getter, observer and liboblog init it during start up.
  tenant_timezone_map_getter tenant_tz_map_getter_;
};

}  // namespace omt
}  // namespace oceanbase

#define OTTZ_MGR (::oceanbase::omt::ObTenantTimezoneMgr::get_instance())

#endif
