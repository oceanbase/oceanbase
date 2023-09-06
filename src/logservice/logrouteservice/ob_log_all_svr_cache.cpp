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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_all_svr_cache.h"
#include "logservice/common_util/ob_log_time_utils.h"

#include "lib/allocator/ob_mod_define.h"      // ObModIds
#include "lib/utility/ob_macro_utils.h"       // OB_ISNULL, ...
#include "lib/oblog/ob_log_module.h"          // LOG_*
#include "logservice/common_util/ob_log_time_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace logservice
{
ObLogAllSvrCache::ObLogAllSvrCache() :
    is_tenant_mode_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    systable_queryer_(NULL),
    all_server_cache_update_interval_(0),
    all_zone_cache_update_interval_(0),
    assign_region_(""),
    region_lock_(),
    cur_version_(0),
    cur_zone_version_(0),
    zone_need_update_(false),
    zone_last_update_tstamp_(OB_INVALID_TIMESTAMP),
    svr_map_(),
    zone_map_(),
    units_map_()
{
}

ObLogAllSvrCache::~ObLogAllSvrCache()
{
  destroy();
}

bool ObLogAllSvrCache::is_svr_avail(const common::ObAddr &svr)
{
  bool bool_ret = false;
  RegionPriority region_priority = REGION_PRIORITY_UNKNOWN;
  bool_ret = is_svr_avail(svr, region_priority);

  return bool_ret;
}

bool ObLogAllSvrCache::is_svr_avail(
    const common::ObAddr &svr,
    RegionPriority &region_priority)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  region_priority = REGION_PRIORITY_UNKNOWN;

  if (!svr.is_valid()) {
    LOG_WARN("svr is not valid!", K(svr));
    bool_ret = false;
  } else if (! is_tenant_mode_) {
    SvrItem svr_item;
    ZoneItem zone_item;

    if (OB_FAIL(get_svr_item_(svr, svr_item))) {
      LOG_WARN("is_svr_avail: failed to get svr item", KR(ret), K(svr), K(svr_item));
      bool_ret = false;
    } else if (OB_FAIL(get_zone_item_(svr_item.zone_, zone_item))) {
      LOG_ERROR("failed to get zone item", KR(ret), K(svr_item), K(zone_item));
      bool_ret = false;
    } else if (is_svr_serve_(svr_item, zone_item)) {
      bool_ret = true;
      // get region priority of server if server is available
      region_priority = svr_item.region_priority_;
      LOG_DEBUG("is svr avail", K(svr), K(region_priority), K(zone_item));
    } else {
      bool_ret = false;
      region_priority = REGION_PRIORITY_UNKNOWN;
      LOG_DEBUG("svr not avail", K(svr), K(zone_item), K(svr_item));
    }
  } else {
    UnitsRecordItem units_record_item;

    if (OB_FAIL(get_units_record_item_(svr, units_record_item))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        bool_ret = false;
        LOG_WARN("get_units_record_item_ failed", KR(ret), K(svr), K(units_record_item));
      } else {
        bool_ret = true;
      }
    } else if (is_svr_serve_(units_record_item)) {
      bool_ret = true;
      // get the region priority of the server if server is available
      region_priority = units_record_item.region_priority_;
      LOG_TRACE("is svr avail", K(svr), K(region_priority), K(units_record_item));
    } else {}
  }

  return bool_ret;
}

int ObLogAllSvrCache::update_assign_region(const common::ObRegion &prefer_region)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(region_lock_);

  if (OB_FAIL(assign_region_.assign(prefer_region))) {
    LOG_ERROR("assign_region assign fail", KR(ret), K(assign_region_), K(prefer_region));
  }

  return ret;
}

int ObLogAllSvrCache::get_assign_region(common::ObRegion &prefer_region)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(region_lock_);

  if (OB_FAIL(prefer_region.assign(assign_region_))) {
    LOG_ERROR("prefer_region assign fail", KR(ret), K(assign_region_), K(prefer_region));
  }

  return ret;
}

int ObLogAllSvrCache::update_cache_update_interval(const int64_t all_server_cache_update_interval_sec,
    const int64_t all_zone_cache_update_interval_sec)
{
  int ret = OB_SUCCESS;

  const int64_t all_server_cache_update_interval = all_server_cache_update_interval_sec * _SEC_;
  const int64_t all_zone_cache_update_interval = all_zone_cache_update_interval_sec * _SEC_;

  ATOMIC_SET(&all_server_cache_update_interval_, all_server_cache_update_interval);
  ATOMIC_SET(&all_zone_cache_update_interval_, all_zone_cache_update_interval);

  return ret;
}

int ObLogAllSvrCache::get_cache_update_interval(int64_t &all_server_cache_update_interval_sec,
    int64_t &all_zone_cache_update_interval_sec)
{
  int ret = OB_SUCCESS;

  all_server_cache_update_interval_sec = ATOMIC_LOAD(&all_server_cache_update_interval_) / _SEC_;
  all_zone_cache_update_interval_sec = ATOMIC_LOAD(&all_zone_cache_update_interval_) / _SEC_;

  return ret;
}

int ObLogAllSvrCache::get_svr_item_(const common::ObAddr &svr, SvrItem &item)
{
  int ret = OB_SUCCESS;
  int64_t cur_ver = ATOMIC_LOAD(&cur_version_);

  if (OB_FAIL(svr_map_.get(svr, item))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get server item from map fail", KR(ret), K(svr));
    }
  } else if (item.version_ < cur_ver) {
    // treat as invalid record if version of data little than current version
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    // succ
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("[STAT] [ALL_SVR_CACHE] [GET_SVR_ITEM]", K(svr),
        "status", OB_SUCCESS == ret ? print_svr_status_(item.status_) : "NOT_EXIST",
        "svr_ver", item.version_, K(cur_ver),
        "zone", item.zone_,
        "region_priority", item.region_priority_);
  } else {
    LOG_INFO("[STAT] [ALL_SVR_CACHE] [GET_SVR_ITEM]", KR(ret), K(svr),
        "status", OB_SUCCESS == ret ? print_svr_status_(item.status_) : "NOT_EXIST",
        "svr_ver", item.version_, K(cur_ver),
        "zone", item.zone_,
        "region_priority", item.region_priority_);
  }

  return ret;
}

const char *ObLogAllSvrCache::print_svr_status_(StatusType status)
{
  const char *str = "UNKNOWN";
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObServerStatus::display_status_str(status, str))) {
    str = "UNKNOWN";
  }

  return str;
}

int ObLogAllSvrCache::get_region_priority_(const common::ObRegion &region,
    RegionPriority &priority)
{
  int ret = OB_SUCCESS;

  if (is_assign_region_(region)) {
    // specified region
    priority = REGION_PRIORITY_HIGH;
  } else {
    // other region or empty region
    priority = REGION_PRIORITY_LOW;
  }
  LOG_DEBUG("get region priority", K(region), K(assign_region_));

  return ret;
};

bool ObLogAllSvrCache::is_svr_serve_(const SvrItem &svr_item, const ZoneItem &zone_item) const
{
  bool bool_ret = false;
  StatusType status = svr_item.status_;
  bool_ret = ObZoneType::ZONE_TYPE_ENCRYPTION != zone_item.get_zone_type()
             && (ObServerStatus::OB_SERVER_ACTIVE == status
	     || ObServerStatus::OB_SERVER_DELETING == status);
  return bool_ret;
}

bool ObLogAllSvrCache::is_svr_serve_(const UnitsRecordItem &units_record_item) const
{
  bool bool_ret = false;

  bool_ret = ObZoneType::ZONE_TYPE_ENCRYPTION != units_record_item.get_zone_type();

  return bool_ret;
}

bool ObLogAllSvrCache::is_assign_region_(const common::ObRegion &region)
{
  bool bool_ret = false;
  ObByteLockGuard guard(region_lock_);

  // ignore case
  bool_ret = (0 == strncasecmp(assign_region_.ptr(),
                               region.ptr(),
                               assign_region_.size()));

  return bool_ret;
}

int ObLogAllSvrCache::init(
    ObLogSysTableQueryer &systable_queryer,
    const bool is_tenant_mode,
    const uint64_t tenant_id,
    const common::ObRegion &prefer_region,
    const int64_t all_server_cache_update_interval_sec,
    const int64_t all_zone_cache_update_interval_sec)
{
  int ret = OB_SUCCESS;

  if (! is_tenant_mode) {
    if (OB_FAIL(svr_map_.init(ObModIds::OB_LOG_ALL_SERVER_CACHE))) {
      LOG_ERROR("init svr map fail", KR(ret));
    } else if (OB_FAIL(zone_map_.init(ObModIds::OB_LOG_ALL_SERVER_CACHE))) {
      LOG_ERROR("init zone map fail", KR(ret));
    }
  } else {
    if (OB_FAIL(units_map_.init("UnitCache"))) {
      LOG_ERROR("init units map fail", KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(assign_region_.assign(prefer_region))) {
    LOG_ERROR("assign_region assign fail", KR(ret), K(assign_region_), K(prefer_region));
  } else {
    is_tenant_mode_ = is_tenant_mode;
    tenant_id_ = tenant_id;
    systable_queryer_ = &systable_queryer;
    all_server_cache_update_interval_ = all_server_cache_update_interval_sec * _SEC_;
    all_zone_cache_update_interval_ = all_zone_cache_update_interval_sec * _SEC_;
    cur_version_ = 0;
    cur_zone_version_ = 0;
    zone_need_update_ = false;
    zone_last_update_tstamp_ = OB_INVALID_TIMESTAMP;

    LOG_INFO("ObLogAllSvrCache init succ", K(prefer_region), K(is_tenant_mode));
  }

  return ret;
}

void ObLogAllSvrCache::destroy()
{
  LOG_INFO("destroy all svr cache begin");
  systable_queryer_ = NULL;
  all_server_cache_update_interval_ = 0;
  all_zone_cache_update_interval_ = 0;
  cur_version_ = 0;
  cur_zone_version_ = 0;
  zone_need_update_ = false;
  zone_last_update_tstamp_ = OB_INVALID_TIMESTAMP;
  if (! is_tenant_mode_) {
    (void)svr_map_.destroy();
    (void)zone_map_.destroy();
  } else {
    (void)units_map_.destroy();
  }
  is_tenant_mode_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;

  LOG_INFO("destroy all svr cache success");
}

void ObLogAllSvrCache::query_and_update()
{
  int ret = OB_SUCCESS;

  if (! is_tenant_mode_) {
    if (need_update_zone_()) {
      if (OB_FAIL(update_zone_cache_())) {
        LOG_ERROR("update zone cache error", KR(ret));
      } else if (OB_FAIL(purge_stale_zone_records_())) {
        LOG_ERROR("purge stale records fail", KR(ret));
      } else {
        // do nothing
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t all_svr_cache_update_interval = ATOMIC_LOAD(&all_server_cache_update_interval_);

      if (REACH_TIME_INTERVAL_THREAD_LOCAL(all_svr_cache_update_interval)) {
        if (OB_FAIL(update_server_cache_())) {
          LOG_ERROR("update server cache error", KR(ret));
        } else if (OB_FAIL(purge_stale_records_())) {
          LOG_ERROR("purge stale records fail", KR(ret));
        } else {
          // do nothing
        }
      }
    }
  } else {
    // is_tenant_mode_ = true
    const int64_t all_svr_cache_update_interval = ATOMIC_LOAD(&all_server_cache_update_interval_);

    if (REACH_TIME_INTERVAL_THREAD_LOCAL(all_svr_cache_update_interval)) {
      if (OB_FAIL(update_unit_info_cache_())) {
        LOG_WARN("update_unit_info_cache_ failed", KR(ret));
      }
    }
  }
}

bool ObLogAllSvrCache::need_update_zone_()
{
  bool bool_ret = false;

  int64_t all_zone_cache_update_interval = ATOMIC_LOAD(&all_zone_cache_update_interval_);
  int64_t update_delta_time = get_timestamp() - zone_last_update_tstamp_;

  // need update if never update
  if (OB_INVALID_TIMESTAMP == zone_last_update_tstamp_) {
    bool_ret = true;
  }
  // update if set zone_need_update_
  else if (zone_need_update_) {
    bool_ret = true;
  }
  // update by interval
  else if (update_delta_time >= all_zone_cache_update_interval) {
    bool_ret = true;
  }

  return bool_ret;
}

int ObLogAllSvrCache::update_zone_cache_()
{
  int ret = OB_SUCCESS;
  ObAllZoneInfo all_zone_info;
  ObAllZoneTypeInfo all_zone_type_info;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  if (OB_ISNULL(systable_queryer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid systable queryer", KR(ret), K(systable_queryer_));
  } else if (OB_FAIL(systable_queryer_->get_all_zone_info(tenant_id, all_zone_info))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("query all zone info need retry", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE == ret) {
      LOG_WARN("ls is in stop state when query zone info", KR(ret));
    } else {
      LOG_ERROR("query all zone info fail", KR(ret));
    }
  } else if (OB_FAIL(systable_queryer_->get_all_zone_type_info(tenant_id, all_zone_type_info))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("query all zone type need retry", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE == ret) {
      LOG_WARN("ls is in stop state when query zone type info", KR(ret));
    } else {
      LOG_ERROR("query all zone type fail", KR(ret));
    }
  } else {
    int64_t next_version = cur_zone_version_ + 1;
    ObAllZoneInfo::AllZoneRecordArray &all_zone_array = all_zone_info.get_all_zone_array();
    ObAllZoneTypeInfo::AllZoneTypeRecordArray &all_zone_type_array = all_zone_type_info.get_all_zone_type_array();

    ARRAY_FOREACH_N(all_zone_array, idx, count) {
      AllZoneRecord &record = all_zone_array.at(idx);
      common::ObZone &zone = record.zone_;
      common::ObRegion &region = record.region_;
      ZoneStorageType &storage_type = record.storage_type_;

      _LOG_INFO("[STAT] [ALL_ZONE] INDEX=%ld/%ld ZONE=%s REGION=%s STORAGE_TYPE=%s VERSION=%lu",
          idx, count, to_cstring(zone), to_cstring(region),
          ObZoneInfo::get_storage_type_str(storage_type), next_version);
      ZoneItem item;
      item.reset(next_version, region, storage_type);
      LOG_DEBUG("update zone cache item", K(zone), K(item));

      if (OB_FAIL(zone_map_.insert_or_update(zone, item))) {
        LOG_ERROR("zone_map_ insert_or_update fail", KR(ret), K(zone), K(item));
      }
    }

    ARRAY_FOREACH_N(all_zone_type_array, idx, count) {
      ZoneItem item;
      AllZoneTypeRecord &record = all_zone_type_array.at(idx);
      common::ObZone &zone = record.zone_;
      common::ObZoneType &zone_type = record.zone_type_;

      if (OB_FAIL(get_zone_item_(zone, item))) {
        LOG_ERROR("fail to get zone item from cache by zone", KR(ret), K(zone));
      } else {
        item.set_zone_type(zone_type);
        if (OB_FAIL(zone_map_.insert_or_update(zone, item))) {
          LOG_ERROR("zone_map_ insert_or_update set zone_type fail", KR(ret), K(zone), K(item), K(zone_type));
        }
      }
      _LOG_INFO("[STAT] [ALL_ZONE] INDEX=%ld/%ld ZONE=%s ZONE_TYPE=%s VERSION=%lu",
          idx, count, to_cstring(zone), zone_type_to_str(item.get_zone_type()), next_version);
    }

    ATOMIC_INC(&cur_zone_version_);
    _LOG_INFO("[STAT] [ALL_ZONE] COUNT=%ld VERSION=%lu", all_zone_array.count(), cur_zone_version_);
  }

  if (OB_SUCC(ret)) {
    zone_need_update_ = false;
    zone_last_update_tstamp_ = get_timestamp();
  }

  return ret;
}

int ObLogAllSvrCache::update_server_cache_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObAllServerInfo all_server_info;

  if (OB_ISNULL(systable_queryer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid systable queryer", KR(ret), K(systable_queryer_));
  } else if (OB_FAIL(systable_queryer_->get_all_server_info(tenant_id, all_server_info))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("query all server info need retry", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE == ret) {
      LOG_WARN("ls is in stop state when query server info", KR(ret));
    } else {
      LOG_ERROR("query all server info fail", KR(ret));
    }
  } else {
    int64_t next_version = cur_version_ + 1;
    ObAllServerInfo::AllServerRecordArray &all_server_record_array = all_server_info.get_all_server_array();

    ARRAY_FOREACH_N(all_server_record_array, idx, count) {
      AllServerRecord &record = all_server_record_array.at(idx);
      ObAddr &svr = record.server_;
      const char *status_str = NULL;

      int tmp_ret = ObServerStatus::display_status_str(record.status_, status_str);
      if (OB_SUCCESS != tmp_ret) {
        LOG_ERROR("invalid server status, can not cast to string", K(tmp_ret),
            K(record.status_), K(record));
      }

      ZoneItem zone_item;
      RegionPriority region_priority = REGION_PRIORITY_UNKNOWN;

      if (OB_FAIL(get_zone_item_(record.zone_, zone_item))) {
        LOG_ERROR("get_zone_item_ fail", KR(ret), "zone", record.zone_, K(zone_item));
      } else if (OB_FAIL(get_region_priority_(zone_item.region_, region_priority))) {
        LOG_ERROR("get priority based region fail", KR(ret), K(svr),
                  "region", zone_item.region_,
                  "region_priority", print_region_priority(region_priority));
      } else {
        SvrItem item;
        item.reset(record.svr_id_, record.status_, next_version, record.zone_, region_priority);
        LOG_DEBUG("update cache item", K(item));

        if (OB_FAIL(svr_map_.insert_or_update(svr, item))) {
          LOG_ERROR("svr_map_ insert_or_update fail", KR(ret), K(svr), K(item));
        }
      }

      _LOG_INFO("[STAT] [ALL_SERVER_LIST] INDEX=%ld/%ld SERVER_ID=%lu SERVER=%s STATUS=%d(%s) "
          "ZONE=%s REGION=%s(%s) VERSION=%lu",
          idx, count, record.svr_id_, to_cstring(svr), record.status_, status_str,
          to_cstring(record.zone_), to_cstring(zone_item.region_),
          print_region_priority(region_priority), next_version);
    }

    ATOMIC_INC(&cur_version_);
    _LOG_INFO("[STAT] [ALL_SERVER_LIST] COUNT=%ld VERSION=%lu", all_server_record_array.count(), cur_version_);
  }

  return ret;
}

int ObLogAllSvrCache::update_unit_info_cache_()
{
  int ret = OB_SUCCESS;
  ObUnitsRecordInfo units_record_info;

  if (OB_ISNULL(systable_queryer_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid systable queryer", KR(ret), K(systable_queryer_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant_id", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(systable_queryer_->get_all_units_info(tenant_id_, units_record_info))) {
    if (OB_NEED_RETRY == ret) {
      LOG_WARN("query the GV$OB_UNITS failed, need retry", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE == ret) {
      LOG_WARN("ls is in stop state when query units info", KR(ret));
    } else {
      LOG_WARN("query the GV$OB_UNITS failed, will be retried later", KR(ret));
    }
  } else {
    int64_t next_version = cur_version_ + 1;
    ObUnitsRecordInfo::ObUnitsRecordArray &units_record_array = units_record_info.get_units_record_array();

    ARRAY_FOREACH_N(units_record_array, idx, count) {
      ObUnitsRecord &record = units_record_array.at(idx);
      ObAddr &svr = record.server_;
      RegionPriority region_priority = REGION_PRIORITY_UNKNOWN;

      if (OB_FAIL(get_region_priority_(record.region_, region_priority))) {
        LOG_ERROR("get priority based region fail", KR(ret), K(svr),
                  "region", record.region_,
                  "region_priority", print_region_priority(region_priority));
      } else {
        UnitsRecordItem units_record_item;
        units_record_item.reset(next_version, record.zone_, record.zone_type_, region_priority);

        if (OB_FAIL(units_map_.insert_or_update(svr, units_record_item))) {
          LOG_ERROR("units_map_ insert_or_update fail", KR(ret), K(svr), K(units_record_item));
        }

        _LOG_INFO("[STAT] [ALL_SERVER_LIST] INDEX=%ld/%ld SERVER=%s "
            "ZONE=%s REGION=%s(%s) VERSION=%lu",
            idx, count, to_cstring(svr),
            to_cstring(record.zone_), to_cstring(record.region_),
            print_region_priority(region_priority), next_version);
      }
    }

    ATOMIC_INC(&cur_version_);
    _LOG_INFO("[STAT] [ALL_SERVER_LIST] COUNT=%ld VERSION=%lu", units_record_array.count(), cur_version_);
  }

  return ret;
}

int ObLogAllSvrCache::get_units_record_item_(
    const common::ObAddr &svr,
    UnitsRecordItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();

  if (OB_FAIL(units_map_.get(svr, item))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get UnitsRecordItem from map fail", KR(ret), K(svr));
    }
  }

  return ret;
}

int ObLogAllSvrCache::get_zone_item_(const common::ObZone &zone,
    ZoneItem &zone_item)
{
  int ret = OB_SUCCESS;
  zone_item.reset();

  if (OB_FAIL(zone_map_.get(zone, zone_item))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("zone_map_ get zone_item fail", KR(ret), K(zone), K(zone_item));
    } else {
      // update all zone cache if can't get region info in __zone_map
      zone_need_update_ = true;
      LOG_DEBUG("zone_map_ get zone_item not exist, need update", KR(ret), K(zone));
    }
  }

  int64_t cur_zone_ver = ATOMIC_LOAD(&cur_zone_version_);
  if (OB_SUCCESS == ret) {
    if (zone_item.version_ < cur_zone_ver) {
      // treate as invalid record if version little than current version
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      // do nothing
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    zone_item.reset();
  }

  return ret;
}

int ObLogAllSvrCache::purge_stale_records_()
{
  int ret = OB_SUCCESS;
  StaleRecPurger purger(cur_version_);

  if (OB_FAIL(svr_map_.remove_if(purger))) {
    LOG_ERROR("remove if fail", KR(ret), K(cur_version_));
  } else {
    _LOG_INFO("[STAT] [ALL_SERVER_LIST] [PURGE] PURGE_COUNT=%ld CUR_COUNT=%ld VERSION=%lu",
        purger.purge_count_, svr_map_.count(), cur_version_);
  }
  return ret;
}

int ObLogAllSvrCache::purge_stale_zone_records_()
{
  int ret = OB_SUCCESS;
  StaleZoneRecPurger purger(cur_zone_version_);

  if (OB_FAIL(zone_map_.remove_if(purger))) {
    LOG_ERROR("zone_map_ remove if fail", KR(ret), K(cur_zone_version_));
  } else {
    _LOG_INFO("[STAT] [ALL_ZONE] [PURGE] PURGE_COUNT=%ld CUR_COUNT=%ld VERSION=%lu",
        purger.purge_count_, zone_map_.count(), cur_zone_version_);
  }
  return ret;
}

bool ObLogAllSvrCache::StaleRecPurger::operator()(const common::ObAddr &svr,
    const SvrItem &svr_item)
{
  bool need_purge = (svr_item.version_ < cur_ver_);

  if (need_purge) {
		purge_count_++;
    _LOG_INFO("[STAT] [ALL_SERVER_LIST] [PURGE] SERVER=%s VERSION=%lu/%lu",
        to_cstring(svr), svr_item.version_, cur_ver_);
  }
  return need_purge;
}

bool ObLogAllSvrCache::StaleZoneRecPurger::operator()(const common::ObZone &zone,
    const ZoneItem &zone_item)
{
  bool need_purge = (zone_item.version_ < cur_ver_);

  if (need_purge) {
		purge_count_++;
    _LOG_INFO("[STAT] [ALL_ZONE] [PURGE] ZONE=%s VERSION=%lu/%lu",
        zone.ptr(), zone_item.version_, cur_ver_);
  }
  return need_purge;
}

void ObLogAllSvrCache::set_update_interval_(const int64_t time)
{
	ATOMIC_STORE(&all_server_cache_update_interval_, time);
}

}
}
