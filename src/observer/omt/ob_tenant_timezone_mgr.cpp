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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_tenant_timezone_mgr.h"
#include "lib/thread/thread_mgr.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "share/ob_time_zone_info_manager.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/hash/ob_hashset.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace omt {

void ObTenantTimezoneMgr::UpdateAllTenantTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t delay = SLEEP_USECONDS;
  const bool repeat = false;
  if (OB_ISNULL(tenant_tz_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("update all tenant task, tenant tz mgr is null", K(ret));
  } else if (tenant_tz_mgr_->get_start_refresh()) {
    if (OB_FAIL(tenant_tz_mgr_->update_timezone_map())) {
      LOG_WARN("tenant timezone mgr update tenant timezone map failed", K(ret));
    }
  } else {
    delay = 1 * 500 * 1000;
  }

  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, *this, delay, repeat))) {
    LOG_ERROR("schedule timezone update task failed", K(ret));
  }
}

void ObTenantTimezoneMgr::DeleteTenantTZTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_tz_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("delete tenant task, tenant tz mgr is null", K(ret));
  } else if (tenant_tz_mgr_->get_start_refresh()) {
    if (OB_FAIL(tenant_tz_mgr_->remove_nonexist_tenant())) {
      LOG_WARN("remove nonexist tenants failed", K(ret));
    } else if (OB_FAIL(tenant_tz_mgr_->delete_tenant_timezone())) {
      LOG_ERROR("tenant timezone mgr delete tenant timezone failed", K(ret));
    }
  }
  const int64_t delay = SLEEP_USECONDS;
  const bool repeat = false;
  if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, *this, delay, repeat))) {
    LOG_ERROR("schedule timezone delete task failed", K(ret));
  }
}

ObTenantTimezoneMgr::ObTenantTimezoneMgr()
    : allocator_("TenantTZ"),
      is_inited_(false),
      self_(),
      sql_proxy_(nullptr),
      rwlock_(),
      timezone_map_(),
      update_task_(this),
      delete_task_(this),
      start_refresh_(false),
      usable_(false),
      schema_service_(nullptr),
      drop_tenant_tz_(allocator_)
{
  tenant_tz_map_getter_ = ObTenantTimezoneMgr::get_tenant_timezone_default;
}

ObTenantTimezoneMgr::~ObTenantTimezoneMgr()
{
  TG_DESTROY(lib::TGDefIDs::TIMEZONE_MGR);
}

ObTenantTimezoneMgr& ObTenantTimezoneMgr::get_instance()
{
  static ObTenantTimezoneMgr ob_tenant_timezone_mgr;
  return ob_tenant_timezone_mgr;
}

int ObTenantTimezoneMgr::init(
    ObMySQLProxy& sql_proxy, const ObAddr& server, share::schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_START(lib::TGDefIDs::TIMEZONE_MGR))) {
    LOG_WARN("fail to init timer", K(ret));
  } else {
    sql_proxy_ = &sql_proxy;
    self_ = server;
    schema_service_ = &schema_service;
    is_inited_ = true;
  }
  const int64_t delay = 0;
  const bool repeat = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(add_tenant_timezone(OB_SYS_TENANT_ID))) {
    LOG_WARN("add tenant timezone info failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, update_task_, delay, repeat))) {
    LOG_WARN("schedual time zone mgr failed", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, delete_task_, delay, repeat))) {
    LOG_WARN("schedual time zone mgr failed", K(ret));
  } else {
    tenant_tz_map_getter_ = ObTenantTimezoneMgr::get_tenant_timezone_static;
  }
  return ret;
}

void ObTenantTimezoneMgr::init(tenant_timezone_map_getter tz_map_getter)
{
  tenant_tz_map_getter_ = tz_map_getter;
  is_inited_ = true;
}

void ObTenantTimezoneMgr::destroy()
{
  timezone_map_.destroy();
}

int ObTenantTimezoneMgr::add_tenant_timezone(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantTimezone* const* timezone = nullptr;
  ObLatchWGuard wr_guard(rwlock_, ObLatchIds::TIMEZONE_LOCK);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant timezone mgr not inited", K(ret));
  } else if (is_virtual_tenant_id(tenant_id) || OB_NOT_NULL(timezone = timezone_map_.get(tenant_id))) {
  } else {
    ObTenantTimezone* new_timezone = nullptr;
    new_timezone = OB_NEW(ObTenantTimezone, ObModIds::OMT, tenant_id);
    if (OB_ISNULL(new_timezone)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc new tenant timezone failed", K(ret));
    } else {
      const int64_t delay = 0;
      const bool repeat = false;
      if (OB_FAIL(new_timezone->init(this))) {
        LOG_WARN("new tenant timezone init failed", K(ret));
      } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::TIMEZONE_MGR, new_timezone->update_tz_task_, delay, repeat))) {
        LOG_WARN("schedule update time zone task failed", K(delay), K(repeat), K(ret));
      } else if (OB_FAIL(timezone_map_.set_refactored(tenant_id, new_timezone, 1))) {
        LOG_WARN("add new tenant timezone failed", K(ret));
      } else {
        LOG_INFO("add tenant timezone success!", K(tenant_id));
      }
      if (OB_FAIL(ret)) {
        ob_delete(new_timezone);
      }
    }
  }
  return ret;
}

int ObTenantTimezoneMgr::del_tenant_timezone(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObTenantTimezone* timezone = nullptr;
  ObLatchWGuard wr_guard(rwlock_, ObLatchIds::TIMEZONE_LOCK);
  if (is_virtual_tenant_id(tenant_id)) {
  } else if (OB_FAIL(timezone_map_.get_refactored(tenant_id, timezone))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tenant timezone failed", K(tenant_id), K(ret));
    }
  } else if (OB_ISNULL(timezone)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("time zone is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(timezone_map_.erase_refactored(tenant_id))) {
    LOG_WARN("erase tenant timezone failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(drop_tenant_tz_.push_back(timezone))) {
    LOG_WARN("push back timezone failed", K(ret));
  } else {
    LOG_INFO("drop tenant tz push back succeed", K(timezone->get_tz_map()), K(timezone->get_tenant_id()));
  }
  return ret;
}

int ObTenantTimezoneMgr::get_tenant_timezone_inner(
    const uint64_t tenant_id, ObTZMapWrap& timezone_wrap, ObTimeZoneInfoManager*& tz_info_mgr)
{
  int ret = OB_SUCCESS;
  ObTenantTimezone* timezone = nullptr;
  ObLatchRGuard rd_guard(const_cast<ObLatch&>(rwlock_), ObLatchIds::TIMEZONE_LOCK);
  if (OB_FAIL(timezone_map_.get_refactored(tenant_id, timezone))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("timezone map get refactored failed", K(ret));
    }
  } else if (OB_ISNULL(timezone)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant tz is null", K(ret));
  } else {
    timezone_wrap.set_tz_map(timezone->get_tz_map());
    tz_info_mgr = timezone->get_tz_mgr();
  }
  return ret;
}

// Get the tenant_list, if it contains tenant_id, then create the tenant_timezone of the tenant.
int ObTenantTimezoneMgr::refresh_tenant_timezone(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  if (OB_LIKELY(OB_INVALID_ID != tenant_id)) {
    share::schema::ObSchemaGetterGuard sys_schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, sys_schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", K(ret));
    } else if (OB_FAIL(sys_schema_guard.check_tenant_exist(tenant_id, is_exist))) {
      LOG_WARN("get tenant ids failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (is_exist && OB_FAIL(add_tenant_timezone(tenant_id))) {
    LOG_WARN("add tenant timezone failed", K(ret));
  }
  return ret;
}

int ObTenantTimezoneMgr::get_tenant_timezone(
    const uint64_t tenant_id, ObTZMapWrap& timezone_wrap, ObTimeZoneInfoManager*& tz_info_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_tenant_timezone_inner(tenant_id, timezone_wrap, tz_info_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // For newly created tenants, if the tenant_list is not refreshed regularly, refresh it once.
      if (OB_FAIL(refresh_tenant_timezone(tenant_id))) {
        LOG_WARN("update timezone map failed", K(ret));
      } else if (OB_FAIL(get_tenant_timezone_inner(tenant_id, timezone_wrap, tz_info_mgr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // After brushing to the latest tenant_list, it is not found yet, return to the timezone of the system tenant
          if (OB_FAIL(get_tenant_timezone_inner(OB_SYS_TENANT_ID, timezone_wrap, tz_info_mgr))) {
            LOG_ERROR("get tenant time zone failed", K(ret), K(tenant_id));
          }
        } else {
          LOG_WARN("get tenant time zone failed", K(ret), K(tenant_id));
        }
      }
    } else {
      LOG_WARN("failed to get tenant timezone", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObTenantTimezoneMgr::remove_nonexist_tenant()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> remove_tenant_ids;
  {
    TenantTimezoneMap::const_iterator it = timezone_map_.begin();
    share::schema::ObSchemaGetterGuard sys_schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, sys_schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", K(ret));
    }
    for (; OB_SUCC(ret) && it != timezone_map_.end(); it++) {
      bool is_dropped = false;
      if (OB_FAIL(sys_schema_guard.check_if_tenant_has_been_dropped(it->first, is_dropped))) {
        LOG_WARN("check if tenant has been dropped failed", K(ret));
      } else if (is_dropped && OB_FAIL(remove_tenant_ids.push_back(it->first))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < remove_tenant_ids.count(); i++) {
    if (OB_FAIL(del_tenant_timezone(remove_tenant_ids.at(i)))) {
      LOG_WARN("del tenant timezone failed", K(ret));
    }
  }

  return ret;
}

int ObTenantTimezoneMgr::add_new_tenants(const common::ObIArray<uint64_t>& latest_tenant_ids)
{
  int ret = OB_SUCCESS;
  ObTenantTimezone* timezone = nullptr;
  for (int64_t i = 0; i < latest_tenant_ids.count(); i++) {
    uint64_t tenant_id = latest_tenant_ids.at(i);
    if (OB_FAIL(timezone_map_.get_refactored(tenant_id, timezone))) {
      if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(add_tenant_timezone(tenant_id))) {
          LOG_WARN("add tenant timezone failed", K(ret));
        }
      } else {
        LOG_WARN("get tenant timezone failed", K(ret));
      }
    }
  }
  return ret;
}

// get tenant list and update local tenant timezone map periodically
// only add new tenant here
int ObTenantTimezoneMgr::update_timezone_map()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> latest_tenant_ids;
  {
    share::schema::ObSchemaGetterGuard sys_schema_guard;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("not init", K(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, sys_schema_guard))) {
      LOG_WARN("get sys tenant schema guard failed", K(ret));
    } else if (OB_FAIL(sys_schema_guard.get_tenant_ids(latest_tenant_ids))) {
      LOG_WARN("get tenant ids failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (add_new_tenants(latest_tenant_ids)) {
    LOG_WARN("add new tenants failed", K(ret));
  }
  return ret;
}

// delete all tenant timezone in array drop_tenant_tz_ whose ref_count is zero.
int ObTenantTimezoneMgr::delete_tenant_timezone()
{
  int ret = OB_SUCCESS;
  FOREACH_X(it, drop_tenant_tz_, OB_SUCC(ret))
  {
    ObTenantTimezone* tenant_tz = *it;
    int64_t ref_count = 0;
    if (OB_ISNULL(tenant_tz)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant timezone in array is null", K(ret));
    } else if (OB_FAIL(tenant_tz->get_ref_count(ref_count))) {
      LOG_WARN("get ref count failed", K(ret));
    } else if (0 == ref_count) {
      const int try_times = 30;
      const int64_t period = 1000;
      ObTenantTimezone::TenantTZUpdateTask& task = tenant_tz->get_update_tz_task();
      bool locked = false;
      // waiting for running task with lock finished
      for (int64_t j = 0; j < try_times; ++j) {
        if (task.task_lock_.tryLock()) {
          locked = true;
          break;
        }
        usleep(period);
      }
      if (!locked) {
      } else if (OB_FAIL(TG_CANCEL_R(lib::TGDefIDs::TIMEZONE_MGR, task))) {
        LOG_WARN("cancel tenant timezone update task failed", K(ret));
      } else {
        bool is_exist = true;
        for (int64_t j = 0; j < try_times; ++j) {
          if (OB_FAIL(TG_TASK_EXIST(lib::TGDefIDs::TIMEZONE_MGR, task, is_exist))) {
            LOG_WARN("check task exist failed", K(ret));
          } else if (!is_exist) {
            break;
          } else {
            // waiting for running task without lock finished, it should be very soon.
          }
          usleep(period);
        }  // for
        if (!is_exist) {
          if (OB_FAIL(drop_tenant_tz_.erase(it))) {
            LOG_WARN("erase failed", K(ret));
          } else {
            LOG_INFO("ref count is zero, drop tenant timezone", K(tenant_tz->get_tenant_id()));
            task.task_lock_.unlock();
            ob_delete(tenant_tz);
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantTimezoneMgr::get_tenant_timezone_static(const uint64_t tenant_id, ObTZMapWrap& timezone_wrap)
{
  ObTimeZoneInfoManager* tz_info_mgr = NULL;
  return get_instance().get_tenant_timezone(tenant_id, timezone_wrap, tz_info_mgr);
}

int ObTenantTimezoneMgr::get_tenant_tz(const uint64_t tenant_id, ObTZMapWrap& timezone_wrap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tenant_tz_map_getter_)) {
    LOG_ERROR("tenant tz map getter is null", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_tz_map_getter_(tenant_id, timezone_wrap))) {
    LOG_WARN("get tenant tz map failed", K(ret), K(tenant_id));
  }
  return ret;
}

int ObTenantTimezoneMgr::get_tenant_timezone_default(const uint64_t tenant_id, ObTZMapWrap& timezone_wrap)
{
  int ret = OB_SUCCESS;
  static ObTZInfoMap tz_map;
  UNUSED(tenant_id);
  if (OB_UNLIKELY(!tz_map.is_inited()) && OB_FAIL(tz_map.init(ObModIds::OB_HASH_BUCKET_TIME_ZONE_INFO_MAP))) {
    LOG_WARN("init time zone info map failed", K(ret));
  } else {
    timezone_wrap.set_tz_map(&tz_map);
  }
  return ret;
}

}  // namespace omt
}  // namespace oceanbase
