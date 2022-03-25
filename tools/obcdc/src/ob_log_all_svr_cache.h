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

#ifndef OCEANBASE_LIBOBLOG_OB_LOG_ALL_SVR_CACHE_H__
#define OCEANBASE_LIBOBLOG_OB_LOG_ALL_SVR_CACHE_H__

#include <pthread.h>                        // pthread_*

#include "lib/net/ob_addr.h"                // ObAddr
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "common/ob_zone.h"                 // ObZone
#include "common/ob_zone_type.h"            // ObZoneType
#include "common/ob_region.h"               // ObRegin
#include "share/ob_server_status.h"         // ObServerStatus

#include "ob_log_utils.h"                   // _SEC_
#include "ob_log_server_priority.h"         // RegionPriority

namespace oceanbase
{
namespace liboblog
{

class IObLogAllSvrCache
{
public:
  typedef share::ObServerStatus::DisplayStatus StatusType;

  static const int64_t USLEEP_INTERVAL = 1 * 1000 * 1000;

public:
  virtual ~IObLogAllSvrCache() {}

  // check server is available or not
  // server avail means server can serve for RPC(locate start log id, fetch log, ...)
  virtual bool is_svr_avail(const common::ObAddr &svr) = 0;

  // 1. check server is available or not
  // 2. if svr is available, return region priority of region, otherwise return REGION_PRIORITY_UNKNOWN
  // server is available means:
  // 1. server status is ACTIVE or DELETING
  // 2. server not in ENCRYPTION zone
  //
  // @param [in]  svr             server addr
  // @param [out] region_priority region priority
  //
  // @retval true  server is available
  // @retval false server is not available
  virtual bool is_svr_avail(
      const common::ObAddr &svr,
      RegionPriority &region_priority) = 0;

};

///////////////////// ObLogAllSvrCache //////////////////////

class IObLogErrHandler;
class IObLogSysTableHelper;
class ObLogConfig;
class ObLogAllSvrCache : public IObLogAllSvrCache
{
  // class static variables
public:
  static int64_t g_all_server_cache_update_interval;
  static int64_t g_all_zone_cache_update_interval;
  // specified region(used for svr priority)
  static common::ObRegion g_assign_region;

public:
  ObLogAllSvrCache();
  virtual ~ObLogAllSvrCache();

public:
  virtual bool is_svr_avail(const common::ObAddr &svr);
  virtual bool is_svr_avail(
      const common::ObAddr &svr,
      RegionPriority &region_priority);

  static const char *print_svr_status(StatusType status);

public:
  int init(IObLogSysTableHelper &systable_helper, IObLogErrHandler &err_handler);
  void destroy();
  void run();

public:
  static void configure(const ObLogConfig & config);

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
  bool is_assign_region_(const common::ObRegion &region) const;
  static void *thread_func_(void *arg);
  bool need_update_zone_();
  int update_zone_cache_();
  int update_server_cache_();
  int purge_stale_records_();
  int purge_stale_zone_records_();
  // NOTE: server serve in such cases:
  // 1. server status is ACTIVE or DELETING
  bool is_svr_serve_(const SvrItem &svr_item) const;

private:
  struct SvrItem
  {
    StatusType       status_;
    uint64_t         version_;
    common::ObZone   zone_;
    RegionPriority   region_priority_;

    void reset(const StatusType status,
        const uint64_t version,
        const common::ObZone &zone,
        const RegionPriority region_priority)
    {
      status_ = status;
      version_ = version;
      zone_ = zone;
      region_priority_ = region_priority;
    }

    TO_STRING_KV(K_(status), K_(version), K_(zone),
                 "region_priority", print_region_priority(region_priority_));
  };
  typedef common::ObLinearHashMap<common::ObAddr, SvrItem> SvrMap;

  struct ZoneItem
  {
    // Compatibility: the observer of the lower version does not have the region field,
    // and the region is empty by default
    common::ObRegion region_;
    common::ObZoneType zone_type_;
    uint64_t         version_;

    void reset()
    {
      version_ = -1;
      region_.reset();
      zone_type_ = common::ZONE_TYPE_INVALID;
    }

    void reset(const uint64_t version,
        const common::ObRegion &region)
    {
      version_ = version;
      region_ = region;
      zone_type_ = common::ZONE_TYPE_INVALID;
    }

    void set_zone_type(const common::ObZoneType &zone_type)
    {
      zone_type_ = zone_type;
    }

    const common::ObZoneType& get_zone_type() const { return zone_type_; }

    TO_STRING_KV(K_(region), "zone_type", zone_type_to_str(zone_type_), K_(version));
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

  // set g_all_server_cache_update_interval for unitest
  static void set_update_interval_(const int64_t time);
private:
  pthread_t             tid_;
  IObLogErrHandler      *err_handler_;
  IObLogSysTableHelper  *systable_helper_;

  bool                  stop_flag_ CACHE_ALIGNED;
  uint64_t              cur_version_ CACHE_ALIGNED;
  uint64_t              cur_zone_version_ CACHE_ALIGNED;

  bool                  zone_need_update_;
  int64_t               zone_last_update_tstamp_;
  // For low version observer compatibility, region information exists by default,
  // if update_zone_cache does not query the record, no region information exists
  bool                  is_region_info_valid_ CACHE_ALIGNED;

  SvrMap                svr_map_;
  ZoneMap               zone_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogAllSvrCache);
};

}
}

#endif
