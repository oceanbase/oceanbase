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

#include "ob_server_priority.h"
#include "lib/allocator/ob_mod_define.h"  // ObModIds
#include "lib/oblog/ob_log.h"

#include <cstring>

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::common;

const char *print_replica_priority(ReplicaPriority type)
{
  const char *str = nullptr;
  switch (type) {
    case REPLICA_PRIORITY_UNKNOWN:
      str = "UNKNOWN";
      break;
    case REPLICA_PRIORITY_OTHER:
      str = "OTHER_REPLICA";
      break;
    case REPLICA_PRIORITY_FULL:
      str = "FULL";
      break;
    case REPLICA_PRIORITY_READONLY:
      str = "READ_ONLY";
      break;
    case REPLICA_PRIORITY_LOGONLY:
      str = "LOG_ONLY";
      break;
    default:
      str = "INVALID";
      break;
  }
  return str;
}

int get_replica_priority(const common::ObReplicaType type,
    ReplicaPriority &priority)
{
  int ret = OB_SUCCESS;
  priority = REPLICA_PRIORITY_UNKNOWN;

  if (!ObReplicaTypeCheck::is_replica_type_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    switch(type) {
      case REPLICA_TYPE_FULL: {
        priority = REPLICA_PRIORITY_FULL;
        break;
      }
      case REPLICA_TYPE_READONLY: {
        priority = REPLICA_PRIORITY_READONLY;
        break;
      }
      case REPLICA_TYPE_LOGONLY: {
        priority = REPLICA_PRIORITY_LOGONLY;
        break;
      }
      default: {
        priority = REPLICA_PRIORITY_OTHER;
        break;
      }
    };
  }

  return ret;
}

ObCdcLogFetchPriority::ObCdcLogFetchPriority()
  : configured_(false),
    default_priority_level_(1),
    map_inited_(false),
    zone_tier_map_(),
    assign_region_(""),
    assign_lock_(common::ObLatchIds::OB_LOG_ALL_SVR_CACHE_LOCK)
{
}

ObCdcLogFetchPriority::~ObCdcLogFetchPriority()
{
  destroy();
}

void ObCdcLogFetchPriority::destroy()
{
  if (map_inited_) {
    (void)zone_tier_map_.destroy();
    map_inited_ = false;
  }
  configured_ = false;
  default_priority_level_ = 1;
}

bool ObCdcLogFetchPriority::is_space_(const char c)
{
  return (' ' == c) || ('\t' == c) || ('\n' == c) || ('\r' == c) || ('\'' == c) || ('\"' == c);
}

const char *ObCdcLogFetchPriority::ltrim_(const char *p, const char *end)
{
  while (p < end && is_space_(*p)) {
    ++p;
  }
  return p;
}

const char *ObCdcLogFetchPriority::rtrim_(const char *p, const char *end)
{
  while (end > p && is_space_(end[-1])) {
    --end;
  }
  return end;
}

int ObCdcLogFetchPriority::add_zone_token_(const char *tok_start, const char *tok_end,
    const int64_t tier_idx)
{
  int ret = OB_SUCCESS;
  const char *t0 = ltrim_(tok_start, tok_end);
  const char *t1 = rtrim_(t0, tok_end);
  if (t1 <= t0) {
    // empty token, skip
  } else {
    const int64_t len = t1 - t0;
    if (OB_UNLIKELY(len > MAX_ZONE_LENGTH)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("zone name in zone_priority too long", KR(ret), K(len));
    } else {
      char buf[MAX_ZONE_LENGTH + 1];
      MEMCPY(buf, t0, static_cast<size_t>(len));
      buf[len] = '\0';
      ObZone zone;
      if (OB_FAIL(zone.assign(buf))) {
        LOG_WARN("zone assign fail", KR(ret), K(buf));
      } else {
        int64_t exist = 0;
        const int gret = zone_tier_map_.get(zone, exist);
        if (OB_ENTRY_NOT_EXIST == gret) {
          if (OB_FAIL(zone_tier_map_.insert_or_update(zone, tier_idx))) {
            LOG_WARN("zone_tier_map_ insert fail", KR(ret), K(zone), K(tier_idx));
          }
        }
      }
    }
  }
  return ret;
}

bool ObCdcLogFetchPriority::is_default_zone_(const common::ObZone &zone)
{
  return (0 == strncasecmp(zone.ptr(), "default_zone", zone.size()));
}


int ObCdcLogFetchPriority::init_zone_priority(const char *zone_priority_str)
{
  int ret = OB_SUCCESS;
  destroy();

  if (OB_ISNULL(zone_priority_str) || is_default_zone_(zone_priority_str)) {
    // not configured or default zone
  } else {
    const int64_t raw_len = static_cast<int64_t>(STRLEN(zone_priority_str));
    const char *const raw_end = zone_priority_str + raw_len;
    const char *start = ltrim_(zone_priority_str, raw_end);
    const char *end = rtrim_(start, raw_end);
    if (start >= end) {
      // not configured
    } else if (OB_FAIL(zone_tier_map_.init(ObModIds::OB_LOG_ALL_SERVER_CACHE))) {
      LOG_WARN("zone_tier_map_ init fail", KR(ret));
    } else {
      map_inited_ = true;
      int64_t tier_idx = 1;
      const char *p = start;
      while (OB_SUCC(ret) && p <= end) {
        const char *seg_start = p;
        while (p < end && *p != ';') {
          ++p;
        }
        const char *seg_end = p;
        const char *q = seg_start;
        bool add_zone = false;
        while (OB_SUCC(ret) && q < seg_end) {
          q = ltrim_(q, seg_end);
          if (q >= seg_end) {
            break;
          }
          const char *comma = q;
          while (comma < seg_end && *comma != ',') {
            ++comma;
          }
          if (OB_FAIL(add_zone_token_(q, comma, tier_idx))) {
            LOG_WARN("add_zone_token_ fail", KR(ret));
          } else {
            add_zone = true;
          }
          q = comma;
          if (q < seg_end && *q == ',') {
            ++q;
          }
        }
        if (add_zone) {
          tier_idx = tier_idx + 2;
        }
        if (p >= end) {
          break;
        }
        ++p;
      }
      default_priority_level_ = tier_idx;
      if (OB_SUCC(ret)) {
        configured_ = true;
        LOG_INFO("ObCdcLogFetchPriority zone_priority init", K(default_priority_level_),
            "zone_count", zone_tier_map_.count(), "zone_priority_str", zone_priority_str);
      } else {
        destroy();
      }
    }
  }

  return ret;
}

FetchPriority ObCdcLogFetchPriority::get_zone_priority(const common::ObZone &zone) const
{
  FetchPriority prio = static_cast<FetchPriority>(default_priority_level_);
  if (configured_ && map_inited_) {
    int64_t tier = default_priority_level_;
    if (OB_SUCCESS == zone_tier_map_.get(zone, tier)) {
      prio = static_cast<FetchPriority>(tier);
    }
  }
  return prio;
}

int ObCdcLogFetchPriority::set_assign_region(const common::ObRegion &prefer_region)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(assign_lock_);
  if (OB_FAIL(assign_region_.assign(prefer_region))) {
    LOG_ERROR("assign_region assign fail", KR(ret), K(assign_region_), K(prefer_region));
  }
  return ret;
}

int ObCdcLogFetchPriority::get_assign_region(common::ObRegion &prefer_region)
{
  int ret = OB_SUCCESS;
  ObByteLockGuard guard(assign_lock_);
  if (OB_FAIL(prefer_region.assign(assign_region_))) {
    LOG_ERROR("prefer_region assign fail", KR(ret), K(assign_region_), K(prefer_region));
  }
  return ret;
}

bool ObCdcLogFetchPriority::is_assign_region_(const common::ObRegion &region)
{
  bool bool_ret = false;
  ObByteLockGuard guard(assign_lock_);
  bool_ret = (0 == strncasecmp(assign_region_.ptr(),
                               region.ptr(),
                               assign_region_.size()));
  return bool_ret;
}

int ObCdcLogFetchPriority::get_priority(const common::ObZone &zone, const common::ObRegion &region,
    FetchPriority &priority)
{
  int ret = OB_SUCCESS;
  priority = default_priority_level_;
  if (configured_) {
    // use zone info to get priority1
    priority = get_zone_priority(zone);
  }
    // use region info to get priority2(0/-1) and add priority1 to get final priority
  if (is_assign_region_(region)) {
    priority--;
  }
  LOG_DEBUG("get priority", K(zone), K(region), K(assign_region_), K(priority));
  return ret;
}

} // namespace logservice
} // namespace oceanbase
