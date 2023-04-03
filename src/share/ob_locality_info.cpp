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
#include "share/ob_locality_info.h"

namespace oceanbase
{
using namespace common;
namespace share
{
void ObLocalityZone::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  region_priority_ = UINT64_MAX;
}

int ObLocalityZone::init(const uint64_t tenant_id, const uint64_t region_priority)
{
  tenant_id_ = tenant_id;
  region_priority_ = region_priority;

  return OB_SUCCESS;
}

ObLocalityZone &ObLocalityZone::operator = (const ObLocalityZone &item)
{
  tenant_id_ = item.tenant_id_;
  region_priority_ = item.region_priority_;
  return *this;
}

void ObLocalityRegion::reset()
{
  region_.reset();
  region_priority_ = UINT64_MAX;
  zone_array_.reset();
}

void ObLocalityInfo::reset()
{
  version_ = 0;
  local_region_.reset();
  local_zone_.reset();
  local_idc_.reset();
  local_zone_type_ = ObZoneType::ZONE_TYPE_INVALID;
  local_zone_status_ = ObZoneStatus::UNKNOWN;
  locality_region_array_.reset();
  locality_zone_array_.reset();
}

void ObLocalityInfo::destroy()
{
  locality_region_array_.destroy();
  locality_zone_array_.destroy();
  STORAGE_LOG(INFO, "ObLocalityInfo destroy finished");
}

int ObLocalityInfo::add_locality_zone(const ObLocalityZone &item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(locality_zone_array_.push_back(item))) {
    STORAGE_LOG(WARN, "push to locality info failed", K(ret), K(item));
  } else {
    // do nothing
  }

  return ret;
}

void ObLocalityInfo::set_version(const int64_t version)
{
  version_= version;
}

int64_t ObLocalityInfo::get_version() const
{
  return version_;
}

void ObLocalityInfo::set_local_region(const char *region)
{
  local_region_ = region;
}

void ObLocalityInfo::set_local_zone(const char *zone)
{
  local_zone_ = zone;
}

void ObLocalityInfo::set_local_idc(const char *idc)
{
  local_idc_ = idc;
}

void ObLocalityInfo::set_local_zone_type(const ObZoneType zone_type)
{
  local_zone_type_ = zone_type;
}

const char *ObLocalityInfo::get_local_zone() const
{
  return local_zone_.ptr();
}

const char *ObLocalityInfo::get_local_region() const
{
  return local_region_.ptr();
}

ObZoneType ObLocalityInfo::get_local_zone_type()
{
  return local_zone_type_;
}

const char *ObLocalityInfo::get_local_idc() const
{
  return local_idc_.ptr();
}

int ObLocalityInfo::get_locality_zone(const uint64_t tenant_id, ObLocalityZone &item)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  item.reset();
  for (i = 0;i < locality_zone_array_.count(); i++) {
    if (locality_zone_array_.at(i).get_tenant_id() == tenant_id) {
      item = locality_zone_array_.at(i);
      break;
    }
  }
  if (i == locality_zone_array_.count()) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObLocalityInfo::get_region_priority(const uint64_t tenant_id, uint64_t &region_priority)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  region_priority = UINT64_MAX;
  for (i = 0;i < locality_zone_array_.count(); i++) {
    if (locality_zone_array_.at(i).get_tenant_id() == tenant_id) {
      region_priority = locality_zone_array_.at(i).get_region_priority();
      break;
    }
  }
  if (i == locality_zone_array_.count()) {
    ret = OB_ITER_END;
  }

  return ret;
}

bool ObLocalityInfo::is_valid()
{
  return !local_zone_.is_empty()
         && !local_region_.is_empty()
         && ObZoneType::ZONE_TYPE_INVALID != local_zone_type_;
}

int ObLocalityInfo::copy_to(ObLocalityInfo &locality_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(locality_info.local_region_.assign(local_region_))) {
    LOG_WARN("copy local region fail", K(ret), K_(local_region));
  } else if (OB_FAIL(locality_info.local_zone_.assign(local_zone_))) {
    LOG_WARN("copy local zone fail", K(ret), K_(local_zone));
  } else if (OB_FAIL(locality_info.local_idc_.assign(local_idc_))) {
    LOG_WARN("copy local idc fail", K(ret), K_(local_idc));
  } else if (OB_FAIL(locality_info.locality_zone_array_.assign(locality_zone_array_))) {
    LOG_WARN("copy locality_zone_array fail", K(ret), K_(locality_zone_array));
  } else if (OB_FAIL(locality_info.locality_region_array_.assign(locality_region_array_))) {
    LOG_WARN("copy locality_region_array fail", K(ret), K_(locality_region_array));
  } else {
    locality_info.local_zone_type_ = local_zone_type_;
    locality_info.local_zone_status_ = local_zone_status_;
  }
  return ret;
}
} // end namespace share
} // end namespace oceanbase
