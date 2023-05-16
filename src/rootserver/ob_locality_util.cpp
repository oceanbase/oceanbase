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

#define USING_LOG_PREFIX COMMON

#include <algorithm>
#include "ob_locality_util.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_replica_info.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace rootserver
{

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
#define INVALID_LOCALITY() \
        do { \
          ret = OB_INVALID_ARGUMENT; \
          LOG_WARN("invalid locality", K(ret)); \
        } while (0)
// full replica
const char *const ObLocalityDistribution::FULL_REPLICA_STR = "FULL";
const char *const ObLocalityDistribution::F_REPLICA_STR = "F";
// logonly replica
const char *const ObLocalityDistribution::LOGONLY_REPLICA_STR = "LOGONLY";
const char *const ObLocalityDistribution::L_REPLICA_STR = "L";
// readonly replica
const char *const ObLocalityDistribution::READONLY_REPLICA_STR = "READONLY";
const char *const ObLocalityDistribution::R_REPLICA_STR = "R";
// encryption logonly replica
const char *const ObLocalityDistribution::ENCRYPTION_LOGONLY_REPLICA_STR = "ENCRYPTION_LOGONLY";
const char *const ObLocalityDistribution::E_REPLICA_STR = "E";
// some other terminology
const common::ObZone ObLocalityDistribution::EVERY_ZONE("everyzone");
const char *const ObLocalityDistribution::ALL_SERVER_STR = "ALL_SERVER";
const char *const ObLocalityDistribution::MEMSTORE_PERCENT_STR = "MEMSTORE_PERCENT";

bool ObLocalityDistribution::ZoneSetReplicaDist::operator<(
     const ZoneSetReplicaDist &that)
{
  bool bool_ret = false;
  if (zone_set_.count() < that.zone_set_.count()) {
    bool_ret = true;
  } else if (zone_set_.count() > that.zone_set_.count()) {
    bool_ret = false;
  } else {
    for (int64_t i = 0; i < zone_set_.count(); ++i) {
      const common::ObZone &this_zone = zone_set_.at(i);
      const common::ObZone &that_zone = that.zone_set_.at(i);
      if (this_zone == that_zone) {
        // go on next
      } else if (this_zone < that_zone) {
        bool_ret = true;
        break;
      } else {
        bool_ret = false;
        break;
      }
    }
  }
  return bool_ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::assign(
    const ZoneSetReplicaDist &that) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_set_.assign(that.zone_set_))) {
    LOG_WARN("fail to assign", K(ret));
  } else {
    for (ReplicaTypeID i = 0; i < REPLICA_TYPE_MAX; ++i) {
      if (OB_FAIL(all_replica_attr_array_[i].assign(that.all_replica_attr_array_[i]))) {
        LOG_WARN("fail to assign", K(ret));
      }
    }
  }
  return ret;
}

ObLocalityDistribution::ZoneSetReplicaDist::ZoneSetReplicaDist()
  : zone_set_(),
    all_replica_attr_array_()
{
  reset();
  for (ReplicaTypeID i = 0; i < REPLICA_TYPE_MAX; ++i) {
    all_replica_attr_array_[i].set_label("ZoneRepDist");
  }
}

void ObLocalityDistribution::ZoneSetReplicaDist::reset()
{
  zone_set_.reset();
  for (ReplicaTypeID i = 0; i < REPLICA_TYPE_MAX; ++i) {
    all_replica_attr_array_[i].reset();
  }
}

int ObLocalityDistribution::ZoneSetReplicaDist::check_valid_replica_dist(
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
    bool &is_valid) const
{
  int ret = OB_SUCCESS;
  if (zone_set_.count() <= 0) {
    is_valid = false;
  } else if (NULL == zone_region_list) { // invoked in flush schema, must be valid
    is_valid = true;
  } else {
    int64_t full_replica_num = 0;
    int64_t logonly_replica_num = 0;
    int64_t encryption_logonly_replica_num = 0;
    for (int64_t i = 0; i < all_replica_attr_array_[FULL_REPLICA].count(); ++i) {
      full_replica_num += all_replica_attr_array_[FULL_REPLICA].at(i).num_;
    }
    for (int64_t i = 0; i < all_replica_attr_array_[LOGONLY_REPLICA].count(); ++i) {
      logonly_replica_num += all_replica_attr_array_[LOGONLY_REPLICA].at(i).num_;
    }
    for (int64_t i = 0; i < all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA].count(); ++i) {
      encryption_logonly_replica_num += all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA].at(i).num_;
    }
    if (zone_set_.count() == 1) {
      is_valid = ((full_replica_num >= 0 && full_replica_num <= 1)
                   && (logonly_replica_num >= 0 && logonly_replica_num <= 1)
                   && (encryption_logonly_replica_num >= 0 && encryption_logonly_replica_num <= 1)
                   && (full_replica_num + logonly_replica_num + encryption_logonly_replica_num <= 2));
      if (!is_valid) {
      } else {
        for (int64_t i = 0; is_valid && i < all_replica_attr_array_[READONLY_REPLICA].count(); ++i) {
          is_valid = all_replica_attr_array_[READONLY_REPLICA].at(i).num_ >= 0;
        }
      }
    } else {
      is_valid = false;  // do not support mixed-zone locality from 3.2.1 and versions to come
      /*
      // a single zone can deploy one and only one paxos replica for mixed zone deployment
      // and the deployment of readonly replicas on mixed zone is not supported 
      is_valid = ((full_replica_num >= 0)
                   && (logonly_replica_num >= 0)
                   && (encryption_logonly_replica_num >= 0)
                   && (full_replica_num + logonly_replica_num + encryption_logonly_replica_num
                       == zone_set_.count()));
      if (!is_valid) {
      } else {
        for (int64_t i = 0; is_valid && i < all_replica_attr_array_[READONLY_REPLICA].count(); ++i) {
          is_valid = all_replica_attr_array_[READONLY_REPLICA].at(i).num_ <= 0;
        }
      }
      */
    }
    if (!is_valid) {
    } else if (OB_FAIL(check_encryption_zone_cond(zone_region_list, is_valid))) {
      LOG_WARN("fail to check encryption zone cond", KR(ret));
    }
  }
  return ret;
}

bool ObLocalityDistribution::ZoneSetReplicaDist::has_encryption_logonly() const
{
  bool has = false;
  for (int64_t i = 0;
       !has && i < all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA].count();
       ++i) {
    const ReplicaAttr &attr = all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA].at(i);
    has = attr.num_ > 0;
  }
  return has;
}

bool ObLocalityDistribution::ZoneSetReplicaDist::has_non_encryption_logonly() const
{
  bool has = false;
  for (int64_t i = 0;
       !has && i < all_replica_attr_array_[FULL_REPLICA].count();
       ++i) {
    const ReplicaAttr &attr = all_replica_attr_array_[FULL_REPLICA].at(i);
    has = attr.num_ > 0;
  }
  for (int64_t i = 0;
       !has && i < all_replica_attr_array_[LOGONLY_REPLICA].count();
       ++i) {
    const ReplicaAttr &attr = all_replica_attr_array_[LOGONLY_REPLICA].at(i);
    has = attr.num_ > 0;
  }
  for (int64_t i = 0;
       !has && i < all_replica_attr_array_[READONLY_REPLICA].count();
       ++i) {
    const ReplicaAttr &attr = all_replica_attr_array_[READONLY_REPLICA].at(i);
    has = attr.num_ > 0;
  }
  return has;
}

int ObLocalityDistribution::ZoneSetReplicaDist::get_check_zone_type(
    const common::ObZone &zone,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
    ObZoneRegion::CheckZoneType &check_zone_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == zone_region_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone region list is null", KR(ret), KP(zone_region_list));
  } else if (zone_region_list->count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zone region list count invalid", KR(ret));
  } else if (zone == EVERY_ZONE) {
    ObZoneRegion::CheckZoneType sample_type = zone_region_list->at(0).check_zone_type_;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_region_list->count(); ++i) {
      if (sample_type != zone_region_list->at(i).check_zone_type_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "every zone locality");
      }
    }
    if (OB_SUCC(ret)) {
      check_zone_type = sample_type;
    }
  } else {
    bool found = false;
    for (int64_t i = 0; !found && i < zone_region_list->count(); ++i) {
      if (zone != zone_region_list->at(i).zone_) {
        // bypass
      } else {
        check_zone_type = zone_region_list->at(i).check_zone_type_;
        found = true;
      }
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::check_encryption_zone_cond(
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
    bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (nullptr == zone_region_list) {
    is_valid = true;
  } else if (zone_set_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone set unexpected", KR(ret));
  } else if (1 == zone_set_.count()) {
    const common::ObZone &zone = zone_set_.at(0);
    ObZoneRegion::CheckZoneType check_zone_type = ObZoneRegion::CZY_MAX;
    if (OB_FAIL(get_check_zone_type(zone, zone_region_list, check_zone_type))) {
      LOG_WARN("fail to get check zone type", KR(ret));
    } else if (ObZoneRegion::CZY_NO_NEED_TO_CHECK == check_zone_type) {
      // when bootstrap, valid
    } else if (ObZoneRegion::CZY_NO_ENCRYPTION == check_zone_type) {
      is_valid = !has_encryption_logonly();
    } else if (ObZoneRegion::CZY_ENCRYPTION == check_zone_type) {
      is_valid = !has_non_encryption_logonly();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected check zone type", KR(ret), K(check_zone_type));
    }
  } else { // zone_set count >= 2
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < zone_set_.count(); ++i) {
      const common::ObZone &zone = zone_set_.at(i);
      ObZoneRegion::CheckZoneType check_zone_type = ObZoneRegion::CZY_MAX;
      if (OB_FAIL(get_check_zone_type(zone, zone_region_list, check_zone_type))) {
        LOG_WARN("fail to get check zone type", KR(ret));
      } else if (ObZoneRegion::CZY_NO_NEED_TO_CHECK == check_zone_type) {
        // when bootstrap, valid
      } else if (ObZoneRegion::CZY_NO_ENCRYPTION == check_zone_type) {
        // multiple zone, no encryption, good
      } else if (ObZoneRegion::CZY_ENCRYPTION == check_zone_type) {
        is_valid = false;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected check zone type", KR(ret), K(check_zone_type));
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::format_to_locality_str(
    char *buf,
    int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!need_format_to_locality_str())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to format this zone replica distribution", K(ret), K(*this));
  } else {
    bool start_format = false;
    for (ReplicaTypeID i = 0; i < REPLICA_TYPE_MAX && OB_SUCC(ret); ++i) {
      if (!specific_replica_need_format(i)) {
        // no need to format this replica type
      } else if (OB_FAIL(format_specific_replica(
              i, buf, buf_len, pos, start_format))) {
        LOG_WARN("fail to format specific replica", K(ret), "replica_type", i);
      } else {} // format this type ok
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_zone_set(buf, buf_len, pos))) {
      LOG_WARN("fail to format zone", K(ret));
    } else {}
  }
  return ret;
}

bool ObLocalityDistribution::ZoneSetReplicaDist::need_format_to_locality_str() const
{
  bool need_format = false;
  for (ReplicaTypeID i = 0; i < REPLICA_TYPE_MAX && !need_format; ++i) {
    need_format = specific_replica_need_format(i);
  }
  return need_format;
}

int ObLocalityDistribution::ZoneSetReplicaDist::try_set_specific_replica_dist(
    const ReplicaTypeID replica_type,
    const int64_t num,
    const int64_t memstore_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(replica_type < FULL_REPLICA || replica_type >= REPLICA_TYPE_MAX)
      || OB_UNLIKELY(num <= 0)
      || OB_UNLIKELY(memstore_percent < 0 || memstore_percent > MAX_MEMSTORE_PERCENT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica_type), K(num), K(memstore_percent));
  } else {
    ReplicaAttrArray &replica_attr_set = all_replica_attr_array_[replica_type];
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_attr_set.count(); ++i) {
      ReplicaAttr &replica_attr = replica_attr_set.at(i);
      if (replica_attr.memstore_percent_ == memstore_percent) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, redundant replica attribute specified");
      } else {} // go on next
    }
    if (OB_SUCC(ret)) {
      ReplicaAttr new_replica_attr(num, memstore_percent);
      if (OB_FAIL(replica_attr_set.push_back(new_replica_attr))) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, too many replica attributes");
      } else if ((replica_attr_set.count() >= 2) && (READONLY_REPLICA == replica_type)) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "locality, nonpaxos replica cannot specify multiple memstore percentage");
      } else {
        std::sort(replica_attr_set.begin(), replica_attr_set.end());
      }
    } 
  }
  return ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::replace_zone_set(
    const common::ObIArray<common::ObZone> &this_zone_set)
{
  int ret = OB_SUCCESS;
  zone_set_.reset();
  if (OB_FAIL(zone_set_.assign(this_zone_set))) {
    LOG_WARN("fail to assign zone set", K(ret));
  } else {
    std::sort(zone_set_.begin(), zone_set_.end());
  }
  return ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::append_zone(
    const common::ObZone &this_zone)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(zone_set_.push_back(this_zone))) {
    LOG_WARN("fail to push back", K(ret));
  } else {
    std::sort(zone_set_.begin(), zone_set_.end());
  }
  return ret;
}

bool ObLocalityDistribution::ZoneSetReplicaDist::specific_replica_need_format(
     const ReplicaTypeID replica_type) const
{
  bool need_format = false;
  if (OB_UNLIKELY(replica_type < FULL_REPLICA || replica_type >= REPLICA_TYPE_MAX)) {
    need_format = false;
  } else {
    const ReplicaAttrArray &replica_attr_set = all_replica_attr_array_[replica_type];
    for (int64_t i = 0; !need_format && i < replica_attr_set.count(); ++i) {
      if (replica_attr_set.at(i).num_ > 0) {
        need_format = true;
      }
    }
  }
  return need_format;
}

int ObLocalityDistribution::ZoneSetReplicaDist::replica_type_to_str(
    const ReplicaTypeID replica_type,
    const char *&replica_type_str) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(replica_type < FULL_REPLICA || replica_type >= REPLICA_TYPE_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(replica_type));
  } else {
    switch (replica_type) {
    case FULL_REPLICA:
      replica_type_str = "FULL";
      break;
    case LOGONLY_REPLICA:
      replica_type_str = "LOGONLY";
      break;
    case READONLY_REPLICA:
      replica_type_str = "READONLY";
      break;
    case ENCRYPTION_LOGONLY_REPLICA:
      replica_type_str = "ENCRYPTION_LOGONLY";
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected replica type", K(ret), K(replica_type));
    }
  }
  return ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::format_specific_replica(
    const ReplicaTypeID replica_type,
    char *buf,
    int64_t buf_len,
    int64_t &pos,
    bool &start_format) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(replica_type < FULL_REPLICA || replica_type >= REPLICA_TYPE_MAX)
      || OB_UNLIKELY(NULL == buf)
      || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const ReplicaAttrArray &replica_attr_set = all_replica_attr_array_[replica_type];
    for (int64_t i = 0; OB_SUCC(ret) && i < replica_attr_set.count(); ++i) {
      const ReplicaAttr &replica_attr = replica_attr_set.at(i);
      const char *replica_type_str = NULL;
      if (OB_FAIL(replica_type_to_str(replica_type, replica_type_str))) {
        LOG_WARN("fail to format replica type", K(ret), K(replica_type));
      } else if (OB_UNLIKELY(NULL == replica_type_str)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica type str null", K(ret), KP(replica_type_str));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%s",
              (start_format ? "," : ""), replica_type_str))) {
        LOG_WARN("fail to format replica type to data buf", K(ret), K(replica_type));
      } else if (ALL_SERVER_CNT != replica_attr.num_) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{%ld", replica_attr.num_))) {
          LOG_WARN("fail to format replica num to data buf", K(ret), K(replica_type));
        }
      } else {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "{%s", ALL_SERVER_STR))) {
          LOG_WARN("fail to format replica num to data buf", K(ret), K(replica_type));
        }
      }
      // memstore percent
      if (OB_FAIL(ret)) {
        // failed
      } else if (LOGONLY_REPLICA == replica_type
                 || MAX_MEMSTORE_PERCENT == replica_attr.memstore_percent_) {
        // logonly or 100% memstore percent do not specify memstore_percent
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ",%s:%ld",
              MEMSTORE_PERCENT_STR, replica_attr.memstore_percent_))) {
        LOG_WARN("fail to format memstore percent to data buf", K(ret));
      }
      // '}' token
      if (OB_FAIL(ret)) {
        // failed
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "}"))) {
        LOG_WARN("fail to format } to data buf", K(ret));
      } else {
        start_format = true;
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::ZoneSetReplicaDist::format_zone_set(
    char *buf,
    int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len <= 0)
      || OB_UNLIKELY(zone_set_.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf), K(buf_len));
  } else {
    const bool single_zone = (1 == zone_set_.count());
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "@%s", (single_zone ? "" : "[")))) {
      LOG_WARN("fail to format @ token", K(ret));
    } else {
      bool start = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_set_.count(); ++i) {
        const common::ObZone &this_zone = zone_set_.at(i);
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%s", (start ? "," : ""), this_zone.ptr()))) {
          LOG_WARN("fail to format zone to data buf", K(ret));
        } else {
          start = true;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", (single_zone ? "" : "]")))) {
        LOG_WARN("fail to format to  data buff", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(locality_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (nullptr == 
      (locality_str_ = static_cast<char *>(alloc_.alloc(locality_.length() + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", KR(ret), K_(locality));
  } else {
    locality_str_len_ = locality_.length();
    MEMCPY(locality_str_, locality_.ptr(), locality_str_len_);
    locality_str_[locality_str_len_] = '\0';
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_next_zone_set_replica_dist(
    ZoneSetReplicaDist &zone_set_replica_dist)
{
  int ret = OB_SUCCESS;
  int64_t pre_pos = INVALID_CURSOR;
  int64_t at_token_pos = INVALID_CURSOR; // position of the token '@'
  zone_set_replica_dist.reset();
  if (!find_at_token_position(pre_pos, at_token_pos)) {
    ret = check_iter_end(pre_pos);
  } else if (OB_UNLIKELY(INVALID_CURSOR == pre_pos || INVALID_CURSOR == at_token_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got invalid cursor", K(ret), K(pre_pos), K(at_token_pos));
  } else if (OB_FAIL(get_replica_arrangements(pre_pos, at_token_pos, zone_set_replica_dist))) {
    LOG_WARN("fail to get replica type and num range", K(ret));
  } else if (OB_FAIL(get_zone_set_dist(at_token_pos + 1, locality_str_len_, zone_set_replica_dist))) {
    LOG_WARN("fail to get zone", K(ret));
  } else {
    ++output_cnt_; // statistic output count;
  }
  return ret;
}

void ObLocalityDistribution::RawLocalityIter::jump_over_blanks(
    int64_t &cursor,
    int64_t end)
{
  while (cursor < end
         && locality_str_[cursor] == BLANK_TOKEN) {
    ++cursor;
  }
}

void ObLocalityDistribution::RawLocalityIter::inc_cursor(
    int64_t &cursor)
{
  ++cursor;
}

int ObLocalityDistribution::RawLocalityIter::check_iter_end(
    int64_t pre_pos)
{
  int ret = OB_ITER_END;
  if (output_cnt_ <= 0) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, no valid locality info exists");
  } else {
    while (pre_pos < locality_str_len_ && OB_ITER_END == ret) {
      if (BLANK_TOKEN != locality_str_[pre_pos]) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, invalid string");
      } else {
        ++pre_pos;
      }
    }
  }
  return ret;
}

bool ObLocalityDistribution::RawLocalityIter::find_at_token_position(
    int64_t &pre_pos,
    int64_t &token_pos)
{
  bool is_found = false;
  pre_pos = pos_;
  while (pos_ < locality_str_len_ && !is_found) {
    if (AT_TOKEN == locality_str_[pos_]) {
      is_found = true;
      token_pos = pos_;
    }
    ++pos_;
  }
  // if is_found is true, pos_ points to the next char of token '@'
  // else pos_ points to locality_str_len_
  return is_found;
}

int ObLocalityDistribution::RawLocalityIter::get_replica_arrangements(
    int64_t cursor,
    int64_t end,
    ZoneSetReplicaDist &zone_set_replica_dist)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (OB_UNLIKELY(cursor >= end)) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, empty replica info before @ token");
  } else {
    zone_set_replica_dist.reset();
    // convert all letters to upper case
    int64_t tmp_cursor = cursor;
    while (tmp_cursor < end) {
      if (locality_str_[tmp_cursor] >= 'a' && locality_str_[tmp_cursor] <= 'z') {
        char offset = static_cast<char>(locality_str_[tmp_cursor] - 'a');
        locality_str_[tmp_cursor] = static_cast<char>(offset + 'A');
      }
      inc_cursor(tmp_cursor);
    }
    int64_t output_cnt = 0;
    ReplicaTypeID replica_type = REPLICA_TYPE_MAX;
    int64_t memstore_percent = -1;
    int64_t replica_num = INVALID_COUNT;
    while (OB_SUCC(ret)
           && OB_SUCC(get_next_replica_arrangement(
               cursor, end, replica_type, replica_num, memstore_percent))) {
      if (OB_UNLIKELY(FULL_REPLICA != replica_type
                      && READONLY_REPLICA != replica_type)) {
        // TODO: F-replica is supported since 4.0,
        //       R-replica is supported since 4.2,
        //       other types will be supported later
        INVALID_LOCALITY();
        switch (replica_type) {
          case LOGONLY_REPLICA:
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "logonly-replica");
            break;
          case ENCRYPTION_LOGONLY_REPLICA:
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "encryption-logonly-replica");
            break;
          case REPLICA_TYPE_MAX:
            LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, unrecognized replica type");
            break;
        }
      } else if (OB_UNLIKELY(INVALID_COUNT == replica_num)) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num illegal");
      } else if (OB_UNLIKELY(memstore_percent < 0 || memstore_percent > MAX_MEMSTORE_PERCENT)) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, memstore percentage illegal");
      } else if (OB_FAIL(zone_set_replica_dist.try_set_specific_replica_dist(
              replica_type, replica_num, memstore_percent))) {
        LOG_WARN("fail to try set specific replica dist", K(ret));
      } else {
        ++output_cnt;
      }
    }
    if (OB_ITER_END == ret) {
      if (output_cnt <= 0) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, empty replica info before @ token");
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_next_replica_arrangement(
    int64_t &cursor,
    int64_t end,
    ReplicaTypeID &replica_type,
    int64_t &replica_num,
    int64_t &memstore_percent)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_replica_type(cursor, end, replica_type))) {
    LOG_WARN("fail to get replica type", K(ret));
  } else if (OB_FAIL(get_replica_attribute(replica_type, cursor, end, replica_num, memstore_percent))) {
    LOG_WARN("fail to get replica attribute", K(ret));
  } else {} // ok
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_replica_type(
    int64_t &cursor,
    int64_t end,
    ReplicaTypeID &replica_type)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica type empty");
  } else {
    bool type_found = false;
    int64_t remain = end - cursor;
    if (!type_found && remain >= strlen(FULL_REPLICA_STR)) {
      if (0 == strncmp(FULL_REPLICA_STR, &locality_str_[cursor], strlen(FULL_REPLICA_STR))) {
        replica_type = FULL_REPLICA;
        type_found = true;
        cursor += strlen(FULL_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(F_REPLICA_STR)) {
      if (0 == strncmp(F_REPLICA_STR, &locality_str_[cursor], strlen(F_REPLICA_STR))) {
        replica_type = FULL_REPLICA;
        type_found = true;
        cursor += strlen(F_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(LOGONLY_REPLICA_STR)) {
      if (0 == strncmp(LOGONLY_REPLICA_STR, &locality_str_[cursor], strlen(LOGONLY_REPLICA_STR))) {
        replica_type = LOGONLY_REPLICA;
        type_found = true;
        cursor += strlen(LOGONLY_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(L_REPLICA_STR)) {
      if (0 == strncmp(L_REPLICA_STR, &locality_str_[cursor], strlen(L_REPLICA_STR))) {
        replica_type = LOGONLY_REPLICA;
        type_found = true;
        cursor += strlen(L_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(READONLY_REPLICA_STR)) {
      if (0 == strncmp(READONLY_REPLICA_STR, &locality_str_[cursor], strlen(READONLY_REPLICA_STR))) {
        replica_type = READONLY_REPLICA;
        type_found = true;
        cursor += strlen(READONLY_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(R_REPLICA_STR)) {
      if (0 == strncmp(R_REPLICA_STR, &locality_str_[cursor], strlen(R_REPLICA_STR))) {
        replica_type = READONLY_REPLICA;
        type_found = true;
        cursor += strlen(R_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(ENCRYPTION_LOGONLY_REPLICA_STR)) {
      if (0 == strncmp(ENCRYPTION_LOGONLY_REPLICA_STR,
                       &locality_str_[cursor],
                       strlen(ENCRYPTION_LOGONLY_REPLICA_STR))) {
        replica_type = ENCRYPTION_LOGONLY_REPLICA;
        type_found = true;
        cursor += strlen(ENCRYPTION_LOGONLY_REPLICA_STR);
      } else {} // not this type
    }
    if (!type_found && remain >= strlen(E_REPLICA_STR)) {
      if (0 == strncmp(E_REPLICA_STR, &locality_str_[cursor], strlen(E_REPLICA_STR))) {
        replica_type = ENCRYPTION_LOGONLY_REPLICA;
        type_found = true;
        cursor += strlen(E_REPLICA_STR);
      }
    }
    if (OB_FAIL(ret)) {
      // already failed, throw the ret code to the upper layer
    } else if (!type_found) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica type illegal");
    } else {} // good, got replica type
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_replica_attribute_recursively(
    const ReplicaTypeID replica_type,
    bool &replica_num_got,
    bool &memstore_percent_got,
    int64_t &cursor,
    int64_t end,
    int64_t &replica_num,
    int64_t &memstore_percent)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, need }  before @ token");
  } else if (RIGHT_BRACE_TOKEN == locality_str_[cursor]) {
    if (OB_SUCC(ret) && !replica_num_got) {
      // try to be compatible with the previous syntax, if replica_num is not specified in between the token '{}'
      // then it is an invalid locality
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num not set between { } token");
    }
    if (OB_SUCC(ret) && !memstore_percent_got) {
      // try to be compatible with the previous syntax, if memstore_percent is not specified in between the token '{}'
      // then the memstore percent is set to MAX_MEMSTORE_PERCENT;
      memstore_percent = MAX_MEMSTORE_PERCENT;
      memstore_percent_got = true;
    }
    if (OB_FAIL(ret)) {
      // failed,
    } else if (OB_FAIL(check_right_brace_and_afterwards_syntax(cursor, end))) {
      LOG_WARN("fail to check right brace and afterwards syntax", K(ret));
    } else if (replica_num > 1) {
      // each zone should has only one replica
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, each zone should has only one replica");
    }
  } else {
    int64_t remain = end - cursor;
    if (remain >= strlen(MEMSTORE_PERCENT_STR)
        && 0 == strncmp(MEMSTORE_PERCENT_STR, &locality_str_[cursor], strlen(MEMSTORE_PERCENT_STR))) {
      // 'MEMSTORE_PERCENT' keyword match
      cursor += strlen(MEMSTORE_PERCENT_STR);
      jump_over_blanks(cursor, end);
      if (LOGONLY_REPLICA == replica_type) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "locality, cannot specify memstore_percent to L(og) type replica");
      } else if (COLON_TOKEN != locality_str_[cursor]) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "locality, memstore_percent shall be followed by : token");
      } else if (memstore_percent_got) {
        // specified the 'memstore_percent' keyword twice is not allowed
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                       "locality, memstore_percent is set multiple times");
      } else if (OB_FAIL(get_pure_memstore_percent(cursor, end, memstore_percent))) {
        LOG_WARN("fail to get pure memstore percent", K(ret));
      } else if (READONLY_REPLICA == replica_type && MAX_MEMSTORE_PERCENT != memstore_percent) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, readonly replica's memstore_percent must be 100");
      } else {
        memstore_percent_got = true;
      }
    } else {
      // not the 'MEMSTORE_PERCENT' keyword, try to check if this is the keyword 'replica_num'
      if (replica_num_got) {
        // specified the 'replica_num' keyword twice is not allowed
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num is set multiple times");
      } else if (OB_FAIL(get_pure_replica_num(cursor, end, replica_num))) {
        LOG_WARN("fail to get pure replica num", K(ret));
      } else if ((ALL_SERVER_CNT == replica_num)
                  && (FULL_REPLICA == replica_type || LOGONLY_REPLICA == replica_type)) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, cannot specify all server for paxos replica");
      } else {
        replica_num_got = true;
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(get_replica_attribute_recursively(
            replica_type, replica_num_got, memstore_percent_got,
            cursor, end, replica_num, memstore_percent))) {
      LOG_WARN("fail to get replica attribute recursively", K(ret));
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::check_replica_attribute_afterwards_syntax(
    int64_t &cursor,
    int64_t end)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (COMMA_TOKEN == locality_str_[cursor]) {
    inc_cursor(cursor); // pass comma
    jump_over_blanks(cursor, end);
    if (RIGHT_BRACE_TOKEN == locality_str_[cursor]) {
      INVALID_LOCALITY(); // ',' followed directly by '}' is not allowed
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, extra , before } token");
    } else {} // good
  } else if (RIGHT_BRACE_TOKEN == locality_str_[cursor]) {
    // good, reach }
  } else {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica attribute shall be followed with , token");
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_replica_attribute(
    const ReplicaTypeID replica_type,
    int64_t &cursor,
    int64_t end,
    int64_t &replica_num,
    int64_t &memstore_percent)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    // the keywords 'replica_num' and 'memstore_percent' are not specified, no need to increment cursor any more
    replica_num = 1;
    memstore_percent = MAX_MEMSTORE_PERCENT;
  } else if (COMMA_TOKEN == locality_str_[cursor]) {
    // in 4.x, we support only one replica in each zone as locality described
    INVALID_LOCALITY(); // a ',' token before the '@' token is not allowed
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, each zone should have only one replica type");
  } else if (LEFT_BRACE_TOKEN == locality_str_[cursor]) {
    inc_cursor(cursor); // pass left brace token
    jump_over_blanks(cursor, end);
    bool replica_num_got = false;
    bool memstore_percent_got = false;
    if (cursor >= end) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num or memstore percentage illegal after {");
    } else if (OB_FAIL(get_replica_attribute_recursively(
            replica_type, replica_num_got, memstore_percent_got,
            cursor, end, replica_num, memstore_percent))) {
      LOG_WARN("fail to do get replica attribute", K(ret));
    }
  } else {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num or memstore percentage illegal");
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_pure_memstore_percent(
    int64_t &cursor,
    int64_t end,
    int64_t &memstore_percent)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (COLON_TOKEN != locality_str_[cursor]) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, memstore_percent shall be followed by : token");
  } else {
    inc_cursor(cursor); // pass colon
    jump_over_blanks(cursor, end);
    if (PLUS_TOKEN == locality_str_[cursor]) {
      inc_cursor(cursor);
      // pass +, if there is a '+' token, then some specific digits shall be followed
      if (!(locality_str_[cursor] >= '1' && locality_str_[cursor] <= '9')) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, memstore_percent value illegal");
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if ('0' == locality_str_[cursor]) { // memstore_percent with value 0 is legal
      memstore_percent = 0;
      inc_cursor(cursor);
    } else {
      if (OB_FAIL(general_nonzero_quantity_string_to_long(cursor, end, memstore_percent))) {
        LOG_WARN("fail to get general quantity string to long", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (memstore_percent < 0 || memstore_percent > MAX_MEMSTORE_PERCENT) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, memstore_percent value shall between [0, 100]");
    } else if (OB_FAIL(check_replica_attribute_afterwards_syntax(cursor, end))) {
      LOG_WARN("fail to check replica attribute afterwards syntax", K(ret));
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_pure_replica_num(
    int64_t &cursor,
    int64_t end,
    int64_t &num)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, expected } before @ token");
  } else {
    if (PLUS_TOKEN == locality_str_[cursor]) {
      inc_cursor(cursor);
      // pass +, if there is a '+' token, then some specific digits shall be followed
      if (!(locality_str_[cursor] >= '1' && locality_str_[cursor] <= '9')) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num illegal");
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!(locality_str_[cursor] >= '1' && locality_str_[cursor] <= '9')) {
      if (OB_FAIL(special_replica_num_quantity_token_to_long(cursor, end, num))) {
        LOG_WARN("fail to get quantity", K(ret));
      } else {} // ok
    } else {
      if (OB_FAIL(general_nonzero_quantity_string_to_long(cursor, end, num))) {
        LOG_WARN("fail to get quantity", K(ret));
      } else {} // ok
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (OB_FAIL(check_replica_attribute_afterwards_syntax(cursor, end))) {
      LOG_WARN("fail to check replica attribute afterwards syntax", K(ret));
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::special_replica_num_quantity_token_to_long(
    int64_t &cursor,
    int64_t end,
    int64_t &num)
{
  int ret = OB_SUCCESS;
  int64_t remain = end - cursor;
  if (remain >= strlen(ALL_SERVER_STR)) {
    if (0 == strncmp(ALL_SERVER_STR, &locality_str_[cursor], strlen(ALL_SERVER_STR))) {
      num = ALL_SERVER_CNT;
      cursor += strlen(ALL_SERVER_STR);
    } else {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num illegal");
    }
  } else {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, replica num illegal");
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::general_nonzero_quantity_string_to_long(
    int64_t &cursor,
    int64_t end,
    int64_t &num)
{
  int ret = OB_SUCCESS;
  if (!(locality_str_[cursor] >= '1' && locality_str_[cursor] <= '9')) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "loality, num is not legal numeric");
  } else {
    static const int64_t BASE = INT32_MAX / 10;
    static const int64_t OFFSET = INT32_MAX % 10;
    int64_t base = 0;
    int64_t offset = 0;
    while (OB_SUCC(ret)
           && cursor < end
           && (locality_str_[cursor] >= '0' && locality_str_[cursor] <= '9')) {
      offset = locality_str_[cursor] - '0';
      if (base < BASE
          || (base == BASE && offset <= OFFSET)) {
        base = 10 * base + offset;
        inc_cursor(cursor);
      } else {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, num out of range");
      }
    }
    if (OB_SUCC(ret)) {
      num = base;
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::check_right_brace_and_afterwards_syntax(
    int64_t &cursor,
    int64_t end)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, need } before @ token");
  } else if (RIGHT_BRACE_TOKEN == locality_str_[cursor]) {
    inc_cursor(cursor); // pass right brace
    jump_over_blanks(cursor, end);
    if (cursor >= end) {
      // good, reach end
    } else if (COMMA_TOKEN != locality_str_[cursor]) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, expect , after } token");
    } else {
      inc_cursor(cursor); // pass comma
      jump_over_blanks(cursor, end);
      if (cursor >= end) {
        INVALID_LOCALITY(); // a ',' token before the '@' token is not allowed
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, extra , appears before @ token");
      }
    }
  } else {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, need } before @ token");
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_single_zone_name(
    int64_t &cursor,
    const int64_t end,
    ZoneSetReplicaDist &zone_set_replica_dist)
{
  int ret = OB_SUCCESS;
  const int64_t start = cursor;
  while (cursor < end
         && BLANK_TOKEN != locality_str_[cursor]
         && COMMA_TOKEN != locality_str_[cursor]
         && RIGHT_BRACKET_TOKEN != locality_str_[cursor]) {
    inc_cursor(cursor);
  }
  const int64_t zone_str_len = cursor - start;
  if (zone_str_len <= 0) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, invalid zone info after @ token");
  } else {
    ObString zone_string(zone_str_len, &locality_str_[start]);
    ObZone this_zone;
    if (OB_FAIL(this_zone.assign(zone_string))) {
      LOG_WARN("fail to set zone", K(ret), K(zone_string));
    } else if (OB_FAIL(zone_set_replica_dist.append_zone(this_zone))) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, too many zones in single locality configuration");
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_zone_set_with_bracket(
    int64_t &cursor,
    const int64_t end,
    ZoneSetReplicaDist &zone_set_replica_dist)
{
  int ret = OB_SUCCESS;
  if (LEFT_BRACKET_TOKEN != locality_str_[cursor]) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    bool iter_end = false;
    inc_cursor(cursor); // PASS '[' TOKEN
    do {
      // try to search for the next zone name string
      jump_over_blanks(cursor, end);
      if (OB_FAIL(get_single_zone_name(cursor, end, zone_set_replica_dist))) {
        LOG_WARN("fail to get single zone name", K(ret));
      } else {
        jump_over_blanks(cursor, end);
        if (RIGHT_BRACKET_TOKEN == locality_str_[cursor]) {
          iter_end = true;
        } else if (COMMA_TOKEN == locality_str_[cursor]) {
          inc_cursor(cursor); // pass ',' TOKEN
        } else {
          INVALID_LOCALITY();
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, invalid zone set info after @ token");
        }
      }
    } while (OB_SUCC(ret) && !iter_end && cursor < end);

    if (OB_FAIL(ret)) {
      // failed
    } else if (cursor >= end) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, [ ] token not in pair");
    } else if (RIGHT_BRACKET_TOKEN != locality_str_[cursor]) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, [ ] token not in pair");
    } else {
      inc_cursor(cursor); // pass ']' token
    }
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_zone_set_without_bracket(
    int64_t &cursor,
    const int64_t end,
    ZoneSetReplicaDist &zone_set_replica_dist)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_single_zone_name(cursor, end, zone_set_replica_dist))) {
    LOG_WARN("fail to get single zone name", K(ret));
  }
  return ret;
}

int ObLocalityDistribution::RawLocalityIter::get_zone_set_dist(
    int64_t cursor,
    const int64_t end,
    ZoneSetReplicaDist &zone_set_replica_dist)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (cursor >= end) {
    INVALID_LOCALITY();
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, no zone info found after @ token");
  } else {
    if (LEFT_BRACKET_TOKEN == locality_str_[cursor]) {
      // zone set shall be embraced by the '[]' token, for instance '[z1, z2, z3]'
      if (OB_FAIL(get_zone_set_with_bracket(cursor, end, zone_set_replica_dist))) {
        LOG_WARN("fail to get zone set with bracket", K(ret));
      }
    } else {
      // only one zone in zone set without '[]' token embraced
      if (OB_FAIL(get_zone_set_without_bracket(cursor, end, zone_set_replica_dist))) {
        LOG_WARN("fail to get zone set ");
      }
    }
  }
  // help to push pos_ next to comma,
  if (OB_SUCC(ret)) {
    jump_over_blanks(cursor, end);
    if (cursor >= end) { // good, the last sub_locality
      pos_ = cursor;
    } else if (COMMA_TOKEN != locality_str_[cursor]) {
      INVALID_LOCALITY();
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, individual zone locality should be separated by , token");
    } else {
      inc_cursor(cursor); // pass COMMA_TOKEN
      jump_over_blanks(cursor, end);
      if (cursor >= end) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, extra , token at string end");
      } else {
        pos_ = cursor;
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::init()
{
  int ret = OB_SUCCESS;
  is_inited_ = true;
  can_output_normalized_locality_ = false;
  return ret;
}

void ObLocalityDistribution::reset()
{
  zone_set_replica_dist_array_.reset();
  is_inited_ = false;
  can_output_normalized_locality_ = false;
}

int ObLocalityDistribution::parse_multiple_zone(
    const common::ObString &locality,
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list)
{
  int ret = OB_SUCCESS;
  RawLocalityIter replica_dist_iter(locality);
  if (locality.empty()
           || locality == OB_AUTO_LOCALITY_STRATEGY
           || locality == OB_SYS_TENANT_LOCALITY_STRATEGY) {
    if (OB_FAIL(parse_for_empty_locality(zone_list, zone_region_list))) {
      LOG_WARN("fail to parse for empty locality", K(ret));
    }
  } else if (OB_FAIL(replica_dist_iter.init())) {
    LOG_WARN("fail to init loality iter", K(ret));
  } else {
    ZoneSetReplicaDist zone_set_replica_dist;
    while (OB_SUCC(ret)
           && OB_SUCC(replica_dist_iter.get_next_zone_set_replica_dist(zone_set_replica_dist))) {
      bool is_legal = false;
      bool is_replica_dist_valid = false;
      bool does_zone_name_duplicate = false;
      if (OB_FAIL(check_zone_set_legal(
              zone_set_replica_dist.get_zone_set(), zone_list, is_legal))) {
        LOG_WARN("fail to check zone set legal", K(ret));
      } else if (!is_legal) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, zone name illegal");
      } else if (OB_FAIL(zone_set_replica_dist.check_valid_replica_dist(
              zone_region_list, is_replica_dist_valid))) {
        LOG_WARN("fail to check valid replica_dist", K(ret));
      } else if (!is_replica_dist_valid) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "encryption type, replica num or memstore percentage illegal");
      } else if (OB_FAIL(check_zone_name_duplicate(
              zone_set_replica_dist, zone_region_list, does_zone_name_duplicate))) {
        LOG_WARN("fail to check zone region collision", K(ret));
      } else if (does_zone_name_duplicate) {
        INVALID_LOCALITY();
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "locality, zone name duplicates");
      } else if (OB_FAIL(append_zone_set_replica_dist(zone_set_replica_dist, zone_list))) {
          LOG_WARN("fail to append zone set replica dist", K(ret));
      }
    }
    if (OB_LIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLocalityDistribution::parse_locality(
    const common::ObString &locality,
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("start to parse locality", K(locality), K(zone_list));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(parse_multiple_zone(locality, zone_list, zone_region_list))) {
      LOG_WARN("fail to parse multiple zone", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    can_output_normalized_locality_ = true;
  }
  return ret;
}

int ObLocalityDistribution::parse_locality(
    const common::ObString &locality,
    const common::ObIArray<common::ObString> &zone_list_input,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObZone> zone_list;

  if (OB_FAIL(convert_zone_list(zone_list_input, zone_list))) {
    LOG_WARN("fail to convert zone list", K(ret));
  } else if (OB_FAIL(parse_locality(
          locality, zone_list, zone_region_list))) {
    LOG_WARN("fail to parse locality", K(ret));
  } else {} // do nothing
  return ret;
}

int ObLocalityDistribution::check_zone_set_legal(
    const common::ObIArray<common::ObZone> &zone_set,
    const common::ObIArray<common::ObZone> &zone_list,
    bool &is_legal)
{
  int ret = OB_SUCCESS;
  is_legal = true;
  if (OB_UNLIKELY(zone_set.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, zone set empty", K(ret));
  } else if (zone_set.count() == 1) {
    if (EVERY_ZONE == zone_set.at(0)) {
      is_legal = true;
    } else if (has_exist_in_array(zone_list, zone_set.at(0))) {
      is_legal = true;
    } else {
      is_legal = false; // zone name string not included in the zone list
    }
  } else { // zone set count >= 2
    for (int64_t i = 0; is_legal && OB_SUCC(ret) && i < zone_set.count(); ++i) {
      const common::ObZone &this_zone = zone_set.at(i);
      if (EVERY_ZONE == this_zone) {
        is_legal = false; // 'everyzone' shall not appear in zone set
      } else if (has_exist_in_array(zone_list, this_zone)) {
        is_legal = true;
      } else {
        is_legal = false;
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::check_zone_name_duplicate(
    const ZoneSetReplicaDist &zone_set_replica_dist,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
    bool &does_zone_name_duplicate)
{
  int ret = OB_SUCCESS;
  does_zone_name_duplicate = false;
  if (NULL == zone_region_list) { // invoked in flush schema,
    does_zone_name_duplicate = false;
  } else {
    const common::ObIArray<common::ObZone> &test_zone_set = zone_set_replica_dist.get_zone_set();
    if (test_zone_set.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("zone set count zero", K(ret));
    } else if (test_zone_set.count() == 1) {
      if (EVERY_ZONE == test_zone_set.at(0)) { // everyzone
        if (zone_set_replica_dist_array_.count() > 0) {
          does_zone_name_duplicate = true;
        } else {} // good
      } else {
        for (int64_t i = 0;
             !does_zone_name_duplicate && i < zone_set_replica_dist_array_.count();
             ++i) {
          const ZoneSetReplicaDist &this_zone_set_replica_dist = zone_set_replica_dist_array_.at(i);
          const common::ObIArray<common::ObZone> &this_zone_set = this_zone_set_replica_dist.get_zone_set();
          if (has_exist_in_array(this_zone_set, test_zone_set.at(0))) {
            does_zone_name_duplicate = true;
          } else {} // good, go on next
        }
      }
    } else {
      for (int64_t i = 0;
           !does_zone_name_duplicate && i < zone_set_replica_dist_array_.count();
           ++i) {
        const ZoneSetReplicaDist &this_zone_set_replica_dist = zone_set_replica_dist_array_.at(i);
        const common::ObIArray<common::ObZone> &this_zone_set = this_zone_set_replica_dist.get_zone_set();
        for (int64_t j = 0;
             !does_zone_name_duplicate && j < test_zone_set.count(); ++j) {
          if (EVERY_ZONE == test_zone_set.at(j)) {
            does_zone_name_duplicate = true;
          } else if (has_exist_in_array(this_zone_set, test_zone_set.at(j))) {
            does_zone_name_duplicate = true;
          } else {} // good, go one next
        }
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::append_zone_set_replica_dist(
    const ZoneSetReplicaDist &zone_set_replica_dist,
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  if (zone_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone_list));
  } else {
    const common::ObIArray<common::ObZone> &zone_set = zone_set_replica_dist.get_zone_set();
    if (zone_set.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone set count unexpected", K(ret), K(zone_set));
    } else if (zone_set.count() == 1 && EVERY_ZONE == zone_set.at(0)) {
      // expand every zone into all zones in the zone_list
      ZoneSetReplicaDist this_dist = zone_set_replica_dist;
      common::ObArray<common::ObZone> this_zone_set;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        this_zone_set.reset();
        const common::ObZone &this_zone = zone_list.at(i);
        if (OB_FAIL(this_zone_set.push_back(this_zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (this_dist.replace_zone_set(this_zone_set)) {
          LOG_WARN("fail to replace zone set", K(ret));
        } else if (OB_FAIL(zone_set_replica_dist_array_.push_back(this_dist))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    } else {
      if (OB_FAIL(zone_set_replica_dist_array_.push_back(zone_set_replica_dist))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObLocalityDistribution::output_normalized_locality(
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)
      || OB_UNLIKELY(!can_output_normalized_locality_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot output normalized locality", K(ret),
             K(is_inited_), K(can_output_normalized_locality_));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else {
    ObArray<ZoneSetReplicaDist> zone_set_replica_dist_array;
    for (int64_t i = 0; i < zone_set_replica_dist_array_.count() && OB_SUCC(ret); ++i) {
      const ZoneSetReplicaDist this_dist = zone_set_replica_dist_array_.at(i);
      if (this_dist.need_format_to_locality_str()) {
        if (OB_FAIL(zone_set_replica_dist_array.push_back(this_dist))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else {} // this zone do not need to format, go on next
    }
    if (OB_FAIL(ret)) {
      // failed
    } else {
      bool start_format = false;
      std::sort(zone_set_replica_dist_array.begin(), zone_set_replica_dist_array.end());
      for (int64_t i = 0; i < zone_set_replica_dist_array.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", (start_format ? ", " : "")))) {
          LOG_WARN("fail to format margin", K(ret));
        } else if (OB_FAIL(zone_set_replica_dist_array.at(i).format_to_locality_str(
                buf, buf_len, pos))) {
          LOG_WARN("fail to format", K(ret));
        } else {
          start_format = true;
        }
      }
    }
  }
  return ret;
}

/* when the locality string is null, or some special strings like 'auto_locality_strategy', 'sys_locality_strategy'
 * OceanBase generates locality based on the following rules:
 * 1 deploy a full replica on a read/write zone
 * 2 deploy an encrypted replica on a encrypted zone
 */
int ObLocalityDistribution::parse_for_empty_locality(
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list)
{
  int ret = OB_SUCCESS; 
  ZoneSetReplicaDist zone_set_replica_dist;
  for (int64_t i = 0; i < zone_list.count() && OB_SUCC(ret); ++i) {
    const int64_t num = 1; //deploy one replica on this zone
    const int64_t memstore_percent = MAX_MEMSTORE_PERCENT; //set memstore_percent to 100 by default
    zone_set_replica_dist.reset();
    common::ObZone zone;
    ReplicaTypeID replica_type = FULL_REPLICA;
    ObZoneRegion::CheckZoneType check_zone_type = ObZoneRegion::CZY_MAX;
    if (OB_FAIL(zone.assign(zone_list.at(i).ptr()))) {
      LOG_WARN("fail to assign zone", K(ret));
    } else if (nullptr == zone_region_list) {
      // this branch is invoked from schema refresh when inner locality is auto,
      // designate the replica type to full directly
      replica_type = FULL_REPLICA;
    } else if (OB_FAIL(ZoneSetReplicaDist::get_check_zone_type(
              zone, zone_region_list, check_zone_type))) {
      LOG_WARN("fail to get check zone type", KR(ret), K(zone));
    } else if (ObZoneRegion::CZY_NO_NEED_TO_CHECK == check_zone_type) {
      replica_type = FULL_REPLICA;
    } else if (ObZoneRegion::CZY_NO_ENCRYPTION == check_zone_type) {
      replica_type = FULL_REPLICA;
    } else if (ObZoneRegion::CZY_ENCRYPTION == check_zone_type) {
      replica_type = ENCRYPTION_LOGONLY_REPLICA;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected check zone type", KR(ret), K(check_zone_type));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(zone_set_replica_dist.append_zone(zone))) {
        LOG_WARN("fail to append zone", K(ret));
      } else if (OB_FAIL(zone_set_replica_dist.try_set_specific_replica_dist(
              replica_type, num, memstore_percent))) {
        LOG_WARN("fail to set", K(ret));
      } else if (OB_FAIL(zone_set_replica_dist_array_.push_back(zone_set_replica_dist))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // do nothing more
    }
  }
  return ret;
}

int ObLocalityDistribution::convert_zone_list(
    const common::ObIArray<common::ObString> &zone_list_input,
    common::ObIArray<common::ObZone> &zone_list_output)
{
  int ret = OB_SUCCESS;
  zone_list_output.reset();
  int64_t zone_count = zone_list_input.count();
  ObZone tmp_zone;
  for (int64_t i = 0; i < zone_count && OB_SUCC(ret); ++i) {
    tmp_zone.reset();
    if (OB_FAIL(tmp_zone.assign(zone_list_input.at(i)))) {
      LOG_WARN("assign failed", K(ret));
    } else if (OB_FAIL(zone_list_output.push_back(tmp_zone))) {
      LOG_WARN("push zone list failed", K(ret), K(tmp_zone));
    } else {} // no more to do
  }
  return ret;
}

int ObLocalityDistribution::get_zone_replica_num(
    const common::ObZone &zone,
    share::ObReplicaNumSet &replica_num_set)
{
  int ret = OB_SUCCESS;
  if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < zone_set_replica_dist_array_.count(); ++i) {
      const ZoneSetReplicaDist &this_dist = zone_set_replica_dist_array_.at(i);
      if (zone != this_dist.get_zone_set().at(0)) {
        // bypass
      } else{
        replica_num_set.set_replica_num(this_dist.get_full_replica_num(),
                                        this_dist.get_logonly_replica_num(),
                                        this_dist.get_readonly_replica_num(),
                                        this_dist.get_encryption_logonly_replica_num());
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObLocalityDistribution::get_zone_replica_num(
    const common::ObZone &zone,
    share::ObZoneReplicaAttrSet &zone_replica_attr_set)
{
  int ret = OB_SUCCESS;
  if (zone.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(zone));
  } else {
    bool found = false;
    for (int64_t i = 0; !found && OB_SUCC(ret) && i < zone_set_replica_dist_array_.count(); ++i) {
      const ZoneSetReplicaDist &this_dist = zone_set_replica_dist_array_.at(i);
      if (zone != this_dist.get_zone_set().at(0)) {
        // bypass
      } else if (OB_FAIL(zone_replica_attr_set.zone_set_.assign(this_dist.get_zone_set()))) {
        LOG_WARN("fail to assign zone set", K(ret));
      } else {
        zone_replica_attr_set.zone_ = zone;
        if (OB_FAIL(zone_replica_attr_set.replica_attr_set_.set_replica_attr_array(
            this_dist.get_full_replica_attr(),
            this_dist.get_logonly_replica_attr(),
            this_dist.get_readonly_replica_attr(),
            this_dist.get_encryption_logonly_replica_attr()))) {
          LOG_WARN("fail to set replica attr array", KR(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // failed
    } else if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObLocalityDistribution::get_zone_replica_attr_array(
    common::ObIArray<share::ObZoneReplicaAttrSet> &zone_replica_num_array)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObZoneReplicaAttrSet> my_zone_replica_num_array;
  common::ObArray<const share::ObZoneReplicaAttrSet*> sort_array;
  share::ObZoneReplicaAttrSet zone_replica_attr_set;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_set_replica_dist_array_.count(); ++i) {
    const ZoneSetReplicaDist &this_dist = zone_set_replica_dist_array_.at(i);
    if (OB_FAIL(zone_replica_attr_set.zone_.assign(this_dist.get_zone_set().at(0)))) {
      LOG_WARN("fail to assign zone replica num set", K(ret));
    } else if (OB_FAIL(zone_replica_attr_set.zone_set_.assign(this_dist.get_zone_set()))) {
      LOG_WARN("fail to assign zone set", K(ret));
    } else {
      if (OB_FAIL(zone_replica_attr_set.replica_attr_set_.set_replica_attr_array(
          this_dist.get_full_replica_attr(),
          this_dist.get_logonly_replica_attr(),
          this_dist.get_readonly_replica_attr(),
          this_dist.get_encryption_logonly_replica_attr()))) {
        LOG_WARN("fail to set replica attr array", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(my_zone_replica_num_array.push_back(zone_replica_attr_set))) {
      LOG_WARN("fail to push back", K(ret), K(zone_replica_attr_set));
    } else {} // no more to do
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < my_zone_replica_num_array.count(); ++i) {
    if (OB_FAIL(sort_array.push_back(&my_zone_replica_num_array.at(i)))) {
      LOG_WARN("failed to push back", KR(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    std::sort(sort_array.begin(), sort_array.end(), ObZoneReplicaAttrSet::sort_compare_less_than);
    zone_replica_num_array.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < sort_array.count(); ++i) {
      if (OB_FAIL(zone_replica_num_array.push_back(*sort_array.at(i)))) {
        LOG_WARN("failed to push back zone replica set", KR(ret), K(i), K(sort_array));
      }
    }
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
