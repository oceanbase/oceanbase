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

#ifndef OCEANBASE_LOG_ALL_SVR_CACHE_H_
#define OCEANBASE_LOG_ALL_SVR_CACHE_H_

#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "lib/lock/ob_small_spin_lock.h"    // ObByteLock
#include "ob_server_priority.h"             // RegionPriority
#include "ob_log_systable_queryer.h"        // ObLogSysTableQueryer

namespace oceanbase
{
namespace logservice
{
///////////////////// ObLogAllSvrCache //////////////////////
class ObLogAllSvrCache
{
public:
  ObLogAllSvrCache();
  virtual ~ObLogAllSvrCache();

public:
  // check server is available or not
  // server avail means server can serve for RPC(locate start log id, fetch log, ...)
  // used for ObLSWorker to check next svr get from PartSvrList is available or not, input should be actual request svr
  bool is_svr_avail(const common::ObAddr &svr);

  // 1. check server is available or not
  // 2. if svr is available, return region priority of region, otherwise return REGION_PRIORITY_UNKNOWN
  //
  // used for ObLogSvrFinder, check svr from inner table is available for PartSvrList
  //
  // server is available means:
  // 1. server status is ACTIVE or DELETING
  // 2. server not in ENCRYPTION zone
  //
  // @param [in]  svr_with_seq        server addr(with seq info)
  // @param [out] region_priority     region priority
  //
  // @retval true  server is available
  // @retval false server is not available
  bool is_svr_avail(
      const common::ObAddr &svr,
      RegionPriority &region_priority);

  int update_assign_region(const common::ObRegion &prefer_region);
  int get_assign_region(common::ObRegion &prefer_region);

  int update_cache_update_interval(const int64_t all_server_cache_update_interval_sec,
      const int64_t all_zone_cache_update_interval_sec);
  int get_cache_update_interval(int64_t &all_server_cache_update_interval_sec,
      int64_t &all_zone_cache_update_interval_sec);

public:
  int init(ObLogSysTableQueryer &systable_queryer,
      const bool is_tenant_mode,
      const uint64_t tenant_id,
      const common::ObRegion &prefer_region,
      const int64_t all_server_cache_update_interval_sec,
      const int64_t all_zone_cache_update_interval_sec);
  void destroy();
  void query_and_update();

private:
  struct SvrItem;
  int get_svr_item_(const common::ObAddr &svr, SvrItem &item);
  // 1. get region of specified zone from zone_map_
  // 2. refresh cache of __all_zone and retry query if query return ret == OB_ENTRY_NOT_EXIST
  struct ZoneItem;
  int get_zone_item_(const common::ObZone &zone, ZoneItem &zone_item);
  // two priroity of region, named from high to low:
  // 1. region_priority = REGION_PRIORITY_HIGH if current region = specified region(g_assign_zone)
  // 2. other region or empty region(no retion info for lower version of observer)
  //    region_priority = REGION_PRIORITY_LOW
  int get_region_priority_(const common::ObRegion &region, RegionPriority &priority);
  bool is_assign_region_(const common::ObRegion &region);

  struct UnitsRecordItem;
  int get_units_record_item_(const common::ObAddr &svr, UnitsRecordItem &item);

  bool need_update_zone_();
  int update_zone_cache_();
  int update_server_cache_();
  int purge_stale_records_();
  int purge_stale_zone_records_();
  int update_unit_info_cache_();

  // NOTE: server serve in such cases:
  // 1. server status is ACTIVE or DELETING
  // 2. server not in ENCRYPTION zone
  bool is_svr_serve_(const SvrItem &svr_item, const ZoneItem &zone_item) const;

  // NOTE: server serve in such cases:
  // 1. server not in ENCRYPTION zone
  bool is_svr_serve_(const UnitsRecordItem &units_record_item) const;

private:
  typedef share::ObServerStatus::DisplayStatus StatusType;
  typedef share::ObZoneInfo::StorageType ZoneStorageType;

  const char *print_svr_status_(StatusType status);
  struct SvrItem
  {
    uint64_t         svr_id_;
    StatusType       status_;
    uint64_t         version_;
    common::ObZone   zone_;
    RegionPriority   region_priority_;

    void reset(const uint64_t svr_id,
        const StatusType status,
        const uint64_t version,
        const common::ObZone &zone,
        const RegionPriority region_priority)
    {
      svr_id_ = svr_id;
      status_ = status;
      version_ = version;
      zone_ = zone;
      region_priority_ = region_priority;
    }

    TO_STRING_KV(K_(svr_id), K_(status), K_(version), K_(zone),
                 "region_priority", print_region_priority(region_priority_));
  };
  typedef common::ObLinearHashMap<common::ObAddr, SvrItem> SvrMap;

  struct ZoneItem
  {
    // Compatibility: the observer of the lower version does not have the region field,
    // and the region is empty by default, storage_type is LOCAL by default
    common::ObRegion region_;
    common::ObZoneType zone_type_;
    ZoneStorageType  storage_type_;
    uint64_t         version_;

    void reset()
    {
      version_ = -1;
      region_.reset();
      zone_type_ = common::ZONE_TYPE_INVALID;
      storage_type_ = ZoneStorageType::STORAGE_TYPE_LOCAL;
    }

    void reset(const uint64_t version,
        const common::ObRegion &region,
        const ZoneStorageType &zone_storage_type)
    {
      version_ = version;
      region_ = region;
      zone_type_ = common::ZONE_TYPE_INVALID;
      storage_type_ = zone_storage_type;
    }

    void set_zone_type(const common::ObZoneType &zone_type)
    {
      zone_type_ = zone_type;
    }

    const common::ObZoneType& get_zone_type() const { return zone_type_; }

    bool is_valid_storage_type() const { return ZoneStorageType::STORAGE_TYPE_MAX > storage_type_; };
    bool is_local_zone() const { return ZoneStorageType::STORAGE_TYPE_LOCAL == storage_type_; };

    TO_STRING_KV(K_(region), "zone_type", zone_type_to_str(zone_type_), K_(storage_type), K_(version));
  };
  typedef common::ObLinearHashMap<common::ObZone, ZoneItem> ZoneMap;

  struct StaleRecPurger
  {
    uint64_t cur_ver_;
    int64_t purge_count_;

    explicit StaleRecPurger(const int64_t ver) : cur_ver_(ver), purge_count_(0)
    {}

    bool operator()(const common::ObAddr &svr, const SvrItem &svr_item);
  };

  struct StaleZoneRecPurger
  {
    uint64_t cur_ver_;
    int64_t purge_count_;

    explicit StaleZoneRecPurger(const int64_t ver) : cur_ver_(ver), purge_count_(0)
    {}

    bool operator()(const common::ObZone &zone, const ZoneItem &zone_item);
  };

  struct UnitsRecordItem
  {
    uint64_t version_;
    common::ObZone zone_;
    common::ObZoneType zone_type_;
    RegionPriority region_priority_;

    void reset()
    {
      version_ = -1;
      zone_.reset();
      zone_type_ = common::ZONE_TYPE_INVALID;
      region_priority_ = REGION_PRIORITY_UNKNOWN;
    }

    void reset(
        const uint64_t version,
        const common::ObZone &zone,
        const common::ObZoneType &zone_type,
        const RegionPriority region_priority)
    {
      version_ = version;
      zone_ = zone;
      zone_type_ = zone_type;
      region_priority_ = region_priority;
    }
    const common::ObZoneType& get_zone_type() const { return zone_type_; }

    TO_STRING_KV(K_(zone), K_(zone_type), K_(region_priority));
  };
  typedef common::ObLinearHashMap<common::ObAddr, UnitsRecordItem> UnitsMap;

  // set all_server_cache_update_interval for unitest
  void set_update_interval_(const int64_t time);

private:
  bool                  is_tenant_mode_;
  uint64_t              tenant_id_;
  ObLogSysTableQueryer  *systable_queryer_;
  int64_t               all_server_cache_update_interval_;
  int64_t               all_zone_cache_update_interval_;
  common::ObRegion      assign_region_;
  common::ObByteLock    region_lock_;

  uint64_t              cur_version_ CACHE_ALIGNED;
  uint64_t              cur_zone_version_ CACHE_ALIGNED;

  bool                  zone_need_update_;
  int64_t               zone_last_update_tstamp_;

  SvrMap                svr_map_;
  ZoneMap               zone_map_;
  UnitsMap              units_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogAllSvrCache);
};

}
}

#endif
