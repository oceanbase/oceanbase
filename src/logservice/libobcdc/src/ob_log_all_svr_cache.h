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
 *
 * cache of tables below in sys tenant
 * oceanbase.__all_server
 * oceanbase.__all_zone
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_ALL_SVR_CACHE_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_ALL_SVR_CACHE_H__

#include <pthread.h>                        // pthread_*

#include "lib/net/ob_addr.h"                // ObAddr
#include "lib/hash/ob_linear_hash_map.h"    // ObLinearHashMap
#include "common/ob_zone.h"                 // ObZone
#include "common/ob_zone_type.h"            // ObZoneType
#include "common/ob_region.h"               // ObRegin
#include "share/ob_zone_info.h"             // ObZoneStorageType
#include "share/ob_server_status.h"         // ObServerStatus

#include "ob_log_utils.h"                   // _SEC_
#include "ob_log_server_priority.h"         // RegionPriority

namespace oceanbase
{
namespace libobcdc
{

class IObLogAllSvrCache
{
public:
  typedef share::ObServerStatus::DisplayStatus StatusType;

  typedef share::ObZoneInfo::StorageType ZoneStorageType;

  static const int64_t USLEEP_INTERVAL = 1 * 1000 * 1000;

public:
  virtual ~IObLogAllSvrCache() {}

  // check server is available or not
  // server avail means server can serve for RPC(locate start log id, fetch log, ...)
  // input shoule be agency addr for OFS single zone mode
  // used for ObLSWorker to check next svr get from PartSvrList is available or not, input should be actual request svr
  virtual bool is_svr_avail(const common::ObAddr &svr) = 0;

  // 1. check server is available or not
  // 2. verify agent_svr is available or not  if try_check_agent is true and cound find agent server,
  //     svr_finder will try find agent for specified principal_svr and verify agent is vaild or not.
  // 3. if svr is available, return region priority of region, otherwise return REGION_PRIORITY_UNKNOWN
  //
  // used for ObLogSvrFinder, check svr from inner table is available for PartSvrList
  //
  // server is available means:
  // 1. server status is ACTIVE or DELETING
  // 2. server not in ENCRYPTION zone
  //
  // @param [in]  svr_with_seq        server addr(with seq info)
  // @param [in]  try_check_agent     need try check agent of svr_with_seq is alive or not
  // @param [out] region_priority     region priority
  //
  // @retval true  server is available
  // @retval false server is not available
  virtual bool is_svr_avail(
      const common::ObAddrWithSeq &svr_with_seq,
      const bool try_check_agent,
      RegionPriority &region_priority) = 0;

  // get agent svr if agent exist
  // @param  [in]    principal_svr   svr_with_seq to check
  // @param  [out]   agent_svr       agent svr for svr_with_seq
  // @retval OB_SUCCESS		     find agent(agent is vaild which is guaranteed while agent_item written into agent_cache)
  // @retval OB_ENTRY_NOT_EXIST      doesn't find agent for specified principal_svr: ob cluster version is less than 3_2_0 or agent not exist
  // @retval other error code	     unexpected error
  virtual int get_agent_svr(const common::ObAddrWithSeq &principal_svr, common::ObAddr &agent_svr) const = 0;

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
  static int64_t g_all_svr_agent_cache_update_interval;

  // specified region(used for svr priority)
  static common::ObRegion g_assign_region;

public:
  ObLogAllSvrCache();
  virtual ~ObLogAllSvrCache();

public:
  virtual bool is_svr_avail(const common::ObAddr &svr);
  virtual bool is_svr_avail(
      const common::ObAddrWithSeq &svr_with_seq,
      const bool try_check_agent,
      RegionPriority &region_priority);
  virtual int get_agent_svr(const common::ObAddrWithSeq &principal_svr, common::ObAddr &agent_svr) const;
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
  struct AgentSvrItem;
  // get agent item for specified principal_svr
  // @param  [in]    principal_svr server to get agent
  // @param  [out]   svr_addr_item agent server item store in agent map for specified principal_svr(invalid if can't find value in cache)
  // @retval OB_SUCCESS succ to get agent for princial_svr, otherwise fail to get valid agent
  // @retval OB_ENTRY_NOT_EXIST can't find agent_item value for principal_svr
  int get_agent_item_(const common::ObAddrWithSeq &principal_svr, AgentSvrItem &svr_addr_item) const;

  static void *thread_func_(void *arg);
  bool need_update_zone_();
  bool need_update_agent_();
  int update_zone_cache_();
  int update_server_cache_();
  int update_agent_cache_();
  int purge_stale_records_();
  int purge_stale_zone_records_();
  int purge_stale_agent_records_();
  // NOTE: server serve in such cases:
  // 1. server status is ACTIVE or DELETING
  // 2. server not in ENCRYPTION zone
  bool is_svr_serve_(const SvrItem &svr_item, const ZoneItem &zone_item) const;

private:
  struct SvrItem
  {
    uint64_t         svr_id_;
    StatusType       status_;
    uint64_t         version_;
    common::ObZone   zone_;
    RegionPriority   region_priority_;
    bool             is_ofs_zone_;

    void reset(const uint64_t svr_id,
        const StatusType status,
        const uint64_t version,
        const common::ObZone &zone,
        const RegionPriority region_priority,
        const bool is_ofs_zone)
    {
      svr_id_ = svr_id;
      status_ = status;
      version_ = version;
      zone_ = zone;
      region_priority_ = region_priority;
      is_ofs_zone_ = is_ofs_zone;
    }

    TO_STRING_KV(K_(svr_id), K_(status), K_(version), K_(zone), K_(is_ofs_zone),
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
    bool is_ofs_zone() const { return false; };

    TO_STRING_KV(K_(region), "zone_type", zone_type_to_str(zone_type_), K_(storage_type), K_(version));
  };
  typedef common::ObLinearHashMap<common::ObZone, ZoneItem> ZoneMap;

  struct AgentSvrItem
  {
    uint64_t         version_;
    common::ObAddr   agent_svr_;

    void reset()
    {
      version_ = -1;
      agent_svr_.reset();
    }

    void reset(const uint64_t version,
               const common::ObAddr &agent_svr)
    {
      version_ = version;
      agent_svr_ = agent_svr;
    }

    TO_STRING_KV(K_(version), K_(agent_svr));
  };

  typedef common::ObLinearHashMap<common::ObAddrWithSeq, AgentSvrItem> AgentMap;

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

  struct StaleAgentRecPurger
  {
    uint64_t cur_ver_;
    int64_t purge_count_;

    explicit StaleAgentRecPurger(const int64_t ver) : cur_ver_(ver), purge_count_(0)
    {}

    bool operator()(const common::ObAddrWithSeq &principal_addr_seq, const AgentSvrItem &agent_item);
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
  uint64_t              cur_agent_version_ CACHE_ALIGNED;

  bool                  zone_need_update_;
  int64_t               zone_last_update_tstamp_;
  // For low version observer compatibility, region information exists by default,
  // if update_zone_cache does not query the record, no region information exists
  bool                  is_region_info_valid_ CACHE_ALIGNED;
  int64_t               agent_last_update_tstamp_;

  SvrMap                svr_map_;
  ZoneMap               zone_map_;
  AgentMap              agent_map_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogAllSvrCache);
};

}
}

#endif
