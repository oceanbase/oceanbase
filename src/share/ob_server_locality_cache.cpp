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

#define USING_LOG_PREFIX SHARE
#include "share/ob_server_locality_cache.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
ObServerLocality::ObServerLocality()
  : inited_(false),
    is_idle_(false),
    is_active_(false),
    addr_(),
    zone_(),
    zone_type_(ObZoneType::ZONE_TYPE_INVALID),
    idc_(),
    region_(),
    start_service_time_(0),
    server_stop_time_(0),
    server_status_(ObServerStatus::OB_DISPLAY_MAX)
{
}

ObServerLocality::~ObServerLocality()
{
}

void ObServerLocality::reset()
{
  inited_ = false;
  is_idle_ = false;
  is_active_ = false;
  addr_.reset();
  zone_.reset();
  zone_type_ = ObZoneType::ZONE_TYPE_INVALID;
  idc_.reset();
  region_.reset();
  start_service_time_ = 0;
  server_stop_time_ = 0;
  server_status_ = ObServerStatus::OB_DISPLAY_MAX;
}

int ObServerLocality::assign(const ObServerLocality &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_.assign(other.zone_))) {
    LOG_WARN("fail to assign zone", K(ret), K(zone_), K(other.zone_));
  } else if (OB_FAIL(idc_.assign(other.idc_))) {
    LOG_WARN("fail to assign idc", K(ret), K(idc_), K(other.idc_));
  } else if (OB_FAIL(region_.assign(other.region_))) {
    LOG_WARN("fail to assign region", K(ret), K(region_), K(other.region_));
  } else {
    addr_ = other.addr_;
    zone_type_ = other.zone_type_;
    inited_ = other.inited_;
    is_idle_ = other.is_idle_;
    is_active_ = other.is_active_;
    zone_type_ = other.zone_type_;
    start_service_time_ = other.start_service_time_;
    server_stop_time_ = other.server_stop_time_;
    server_status_ = other.server_status_;
  }
  return ret;
}


int ObServerLocality::init(const char *svr_ip,
                           const int32_t svr_port,
                           const ObZone &zone,
                           const ObZoneType zone_type,
                           const ObIDC &idc,
                           const ObRegion &region,
                           bool is_active)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", K(ret));
  } else if (OB_UNLIKELY(!addr_.set_ip_addr(svr_ip, svr_port))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("fail to set addr", K(ret), K(svr_ip), K(svr_port));
  } else if (OB_FAIL(zone_.assign(zone))) {
    LOG_WARN("fail to assign zone", K(ret), K(zone_), K(zone));
  } else if (OB_FAIL(idc_.assign(idc))) {
    LOG_WARN("fail to assign idc", K(idc), K(idc_), K(ret));
  } else if (OB_FAIL(region_.assign(region))) {
    LOG_WARN("fail to assign region", K(ret), K(region_), K(region));
  } else {
    zone_type_ = zone_type;
    inited_ = true;
    is_active_ = is_active;
  }
  return ret;
}

int ObServerLocality::set_server_status(const char *svr_status)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObServerStatus::str2display_status(svr_status, server_status_))) {
    LOG_WARN("string to display status failed", K(ret), K(svr_status));
  } else if (server_status_ < 0 || server_status_ >= ObServerStatus::OB_DISPLAY_MAX) {
    ret = OB_INVALID_SERVER_STATUS;
    LOG_WARN("invalid display status", K(svr_status), K(ret));
  }
  return ret;
}

ObServerLocalityCache::ObServerLocalityCache()
  : rwlock_(common::ObLatchIds::SERVER_LOCALITY_CACHE_LOCK),
    server_locality_array_(ObModIds::OB_SERVER_LOCALITY_CACHE, OB_MALLOC_NORMAL_BLOCK_SIZE),
    server_cid_map_(),
    server_region_map_(),
    server_idc_map_(),
    has_readonly_zone_(false),
    is_inited_(false)
{}

ObServerLocalityCache::~ObServerLocalityCache()
{
  destroy();
}

int ObServerLocalityCache::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObServerLocalityCache is already inited", K(ret));
  } else if (OB_FAIL(server_cid_map_.init(ObModIds::OB_SERVER_CID_MAP))) {
    LOG_WARN("server_cid_map_ init failed", K(ret));
  } else if (OB_FAIL(server_region_map_.init(ObModIds::OB_SERVER_REGION_MAP))) {
    LOG_WARN("server_region_map_ init failed", K(ret));
  } else if (OB_FAIL(server_idc_map_.init(ObModIds::OB_SERVER_IDC_MAP))) {
    LOG_WARN("server_idc_map_ init failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObServerLocalityCache::reset()
{
  SpinWLockGuard guard(rwlock_);
  server_locality_array_.reset();
  server_region_map_.reset();
  server_cid_map_.reset();
  server_idc_map_.reset();
  has_readonly_zone_ = false;
}

void ObServerLocalityCache::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    server_cid_map_.destroy();
    server_region_map_.destroy();
    server_idc_map_.destroy();
    LOG_INFO("ObServerLocalityCache destroy finished");
  }
}

int ObServerLocalityCache::get_server_locality_array(
    ObIArray<ObServerLocality> &server_locality_array,
    bool &has_readonly_zone) const
{
  int ret = OB_SUCCESS;
  server_locality_array.reset();
  has_readonly_zone = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    SpinRLockGuard guard(rwlock_);
    if (OB_FAIL(server_locality_array.assign(server_locality_array_))) {
      LOG_WARN("fail to assign server_locality_array", K(ret), K(server_locality_array_));
    } else {
      has_readonly_zone = has_readonly_zone_;
    }
  }
  return ret;
}

// If there are not any servers in a zone, return OB_ENTRY_NOT_EXIST
int ObServerLocalityCache::get_noempty_zone_region(
    const common::ObZone &zone,
    common::ObRegion &region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    SpinRLockGuard guard(rwlock_);
    bool is_found = false;
    for (int64_t i = 0; !is_found && i < server_locality_array_.count(); ++i) {
      const share::ObServerLocality &server_locality = server_locality_array_.at(i);
      if (server_locality.get_zone() == zone) {
        region = server_locality.get_region();
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObServerLocalityCache::get_server_zone(const common::ObAddr &server,
                                           common::ObZone &zone) const
{
  int ret = OB_SUCCESS;
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    SpinRLockGuard guard(rwlock_);
    bool is_found = false;
    for (int64_t i = 0; !is_found && i < server_locality_array_.count(); ++i) {
      const share::ObServerLocality &server_locality = server_locality_array_.at(i);
      if (server_locality.get_addr() == server) {
        zone = server_locality.get_zone();
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_EMPTY_LOCALITY_CACHE)
int ObServerLocalityCache::get_server_zone_type(const common::ObAddr &server,
                                           common::ObZoneType &zone_type) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ERRSIM_EMPTY_LOCALITY_CACHE)) {
    ret = ERRSIM_EMPTY_LOCALITY_CACHE;
  } else if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(server));
  } else {
    SpinRLockGuard guard(rwlock_);
    bool is_found = false;
    for (int64_t i = 0; !is_found && i < server_locality_array_.count(); ++i) {
      const share::ObServerLocality &server_locality = server_locality_array_.at(i);
      if (server_locality.get_addr() == server) {
        zone_type = server_locality.get_zone_type();
        is_found = true;
      }
    }
    if (!is_found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObServerLocalityCache::get_server_region(const common::ObAddr &server,
                                             common::ObRegion &region) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ERRSIM_EMPTY_LOCALITY_CACHE)) {
    ret = ERRSIM_EMPTY_LOCALITY_CACHE;
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    SpinRLockGuard guard(rwlock_);
    bool is_found = false;
    for (int64_t i = 0; !is_found && i < server_locality_array_.count(); ++i) {
      const share::ObServerLocality &server_locality = server_locality_array_.at(i);
      if (server_locality.get_addr() == server) {
        if (!server_locality.get_region().is_empty()) {
          // assign region only when it is not empty
          region = server_locality.get_region();
          is_found = true;
        }
      }
    }
    if (!is_found) {
      // if its locality is not found or its region is empty, we will try
      // get region from region_map.
      ret = get_server_region_from_map_(server, region);
      LOG_TRACE("not found server in server_locality_array_", K(ret), K(server), K(region));
    }
  }
  return ret;
}

int ObServerLocalityCache::get_server_idc(const common::ObAddr &server,
                                          common::ObIDC &idc) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else {
    SpinRLockGuard guard(rwlock_);
    bool is_found = false;
    for (int64_t i = 0; !is_found && i < server_locality_array_.count(); ++i) {
      const share::ObServerLocality &server_locality = server_locality_array_.at(i);
      if (server_locality.get_addr() == server) {
        idc = server_locality.get_idc();
        is_found = true;
      }
    }
    if (!is_found) {
      ret = get_server_idc_from_map_(server, idc);
    }
  }
  return ret;
}

int ObServerLocalityCache::set_server_locality_array(
    const ObIArray<ObServerLocality> &server_locality_array,
    bool has_readonly_zone)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else {
    SpinWLockGuard guard(rwlock_);
    server_locality_array_.reset();
    if (OB_FAIL(server_locality_array_.assign(server_locality_array))) {
      LOG_WARN("fail to assign server_locality_array_", K(ret), K(server_locality_array));
    } else {
      has_readonly_zone_ = has_readonly_zone;
      LOG_INFO("set_server_locality_array success", K(server_locality_array), K(has_readonly_zone));
    }
  }
  return ret;
}

int ObServerLocalityCache::get_server_region_from_map_(const common::ObAddr &server,
                                                       ObRegion &region) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_region_map_.get(server, region))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("server_region_map_.get failed", K(ret), K(server));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObServerLocalityCache::record_server_region(const common::ObAddr &server,
                                                const ObRegion &region)
{
  int ret = OB_SUCCESS;
  ObRegion old_val;
  bool need_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(region));
  } else if (OB_FAIL(server_region_map_.get(server, old_val))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("server_region_map_.get failed", K(ret), K(server));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (old_val == region) {
    need_update = false;
  }

  if (OB_SUCC(ret)) {
    if (!need_update) {
      // no need update
    } else if (OB_FAIL(server_region_map_.insert_or_update(server, region))) {
      LOG_WARN("server_region_map_.get failed", K(ret), K(server), K(region));
    } else {
      LOG_INFO("record server region success", K(server), K(region));
    }
  }
  return ret;
}

int ObServerLocalityCache::get_server_cluster_id(const common::ObAddr &server,
                                                 int64_t &cluster_id) const
{
  int ret = OB_SUCCESS;
  int64_t map_val = INVALID_CLUSTER_ID;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_cid_map_.get(server, map_val))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("server_cid_map_.get failed", K(ret), K(server));
    }
  } else {
    cluster_id = map_val;
  }
  return ret;
}

// Store server's cluster_id
// NOTE: only support store server's information not in self cluster
int ObServerLocalityCache::record_server_cluster_id(const common::ObAddr &server,
                                                    const int64_t &cluster_id)
{
  int ret = OB_SUCCESS;
  int64_t old_val = INVALID_CLUSTER_ID;
  bool need_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(cluster_id));
  } else if (OB_FAIL(server_cid_map_.get(server, old_val))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("server_cid_map_.get failed", K(ret), K(server));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (old_val == cluster_id) {
    need_update = false;
  }

  if (OB_SUCC(ret)) {
    if (!need_update) {
    } else if (OB_FAIL(server_cid_map_.insert_or_update(server, cluster_id))) {
      LOG_WARN("server_cid_map_.get failed", K(ret), K(server), K(cluster_id));
    } else {
      LOG_INFO("record server cluster_id success", K(server), K(cluster_id));
    }
  }
  return ret;
}

int ObServerLocalityCache::get_server_idc_from_map_(const common::ObAddr &server,
                                                    ObIDC &idc) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server));
  } else if (OB_FAIL(server_idc_map_.get(server, idc))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("server_idc_map_.get failed", K(ret), K(server));
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObServerLocalityCache::record_server_idc(const common::ObAddr &server,
                                             const ObIDC &idc)
{
  int ret = OB_SUCCESS;
  ObIDC old_val;
  bool need_update = true;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(idc));
  } else if (OB_FAIL(server_idc_map_.get(server, old_val))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("server_idc_map_.get failed", K(ret), K(server));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (old_val == idc) {
    need_update = false;
  }

  if (OB_SUCC(ret)) {
    if (!need_update) {
      // no need update
    } else if (OB_FAIL(server_idc_map_.insert_or_update(server, idc))) {
      LOG_WARN("server_idc_map_.get failed", K(ret), K(server), K(idc));
    } else {
      LOG_INFO("record server idc success", K(server), K(idc));
    }
  }
  return ret;
}
}/* ns share*/
}/* ns oceanbase */
