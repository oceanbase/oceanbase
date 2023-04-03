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

#include "ob_primary_zone_util.h"
#include "lib/container/ob_array_iterator.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_part_mgr_util.h"
#include "rootserver/ob_root_utils.h"
#include "rootserver/ob_zone_manager.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace oceanbase;
using namespace oceanbase::common;
namespace share
{
using namespace schema;

// when invoked by DDL, zone_region_list_ shall not be NULL
int ObPrimaryZoneUtil::init(
    const common::ObIArray<common::ObString> &zone_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(no_need_to_check_primary_zone(primary_zone_))
             || OB_UNLIKELY(zone_list.count() <= 0)
             || OB_UNLIKELY(NULL == zone_region_list_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(primary_zone_),
             "zone list count", zone_list.count(), KP(zone_region_list_));
  } else {
    int64_t len = strlen(primary_zone_.ptr());
    if (common::MAX_ZONE_LENGTH < len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary_zone length overflowed", KR(ret), K(len), "max_zone_length", MAX_ZONE_LENGTH);
    } else {
      MEMCPY(primary_zone_str_, primary_zone_.ptr(), len);
      primary_zone_str_[len] = '\0';
      common::ObZone zone;
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        zone.reset();
        if (OB_FAIL(zone.assign(zone_list.at(i).ptr()))) {
          LOG_WARN("fail to assign zone", K(ret));
        } else if (OB_FAIL(zone_list_.push_back(zone))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

// when invoked by DDL, zone_region_list_ shall not be NULL
int ObPrimaryZoneUtil::init(
    const common::ObIArray<common::ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(no_need_to_check_primary_zone(primary_zone_))
             || OB_UNLIKELY(zone_list.count() <= 0)
             || OB_UNLIKELY(NULL == zone_region_list_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(primary_zone_),
             "zone list count", zone_list.count(), KP(zone_region_list_));
  } else {
    int64_t len = strlen(primary_zone_.ptr());
    if (common::MAX_ZONE_LENGTH < len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary_zone length overflowed", KR(ret), K(len), "max_zone_length", MAX_ZONE_LENGTH);
    } else {
      MEMCPY(primary_zone_str_, primary_zone_.ptr(), len);
      primary_zone_str_[len] = '\0';
      for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
        if (OB_FAIL(zone_list_.push_back(zone_list.at(i)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

// when invoked by schema_service, zone_region_list_ shall be NULL
int ObPrimaryZoneUtil::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(no_need_to_check_primary_zone(primary_zone_))
             || OB_UNLIKELY(NULL != zone_region_list_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(primary_zone_), KP(zone_region_list_));
  } else {
    int64_t len = strlen(primary_zone_.ptr());
    if (common::MAX_ZONE_LENGTH < len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary_zone length overflowed", KR(ret), K(len), "max_zone_length", MAX_ZONE_LENGTH);
    } else {
      MEMCPY(primary_zone_str_, primary_zone_.ptr(), len);
      primary_zone_str_[len] = '\0';
      is_inited_ = true;
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::check_and_parse_primary_zone()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(check_and_parse_finished_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("already check and parse", K(ret), K(check_and_parse_finished_));
  } else if (OB_FAIL(construct_zone_array())) {
    LOG_WARN("fail to construct zone array", K(ret));
  } else if (OB_FAIL(construct_normalized_zone_array())) {
    LOG_WARN("fail to construct normalized zone array", K(ret));
  } else if (OB_FAIL(construct_full_zone_array())) {
    LOG_WARN("fail to construct full zone array", K(ret));
  } else {
    check_and_parse_finished_ = true;
  }
  return ret;
}

int ObPrimaryZoneUtil::construct_zone_array()
{
  int ret = OB_SUCCESS;
  int64_t cursor = 0;
  const int64_t end = strlen(primary_zone_str_);
  share::schema::ObZoneScore zone_score;
  zone_array_.reset();
  while (OB_SUCC(ret)
         && OB_SUCC(get_next_zone_score(cursor, end, zone_score))) {
    bool zone_exist = false;
    if (OB_FAIL(check_zone_exist(zone_score.zone_, zone_exist))) {
      LOG_WARN("fail to check zone exist", K(ret));
    } else if (!zone_exist) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone not exist", K(ret), K(zone_score), K(primary_zone_str_), K(zone_list_));
    } else if (OB_FAIL(zone_array_.push_back(zone_score))) {
      LOG_WARN("fail to push back zone str", K(ret));
    } else {} // ok
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret)) {
    common::ObArray<common::ObString> tmp_zone_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_array_.count(); ++i) {
      if (OB_FAIL(tmp_zone_array.push_back(zone_array_.at(i).zone_))) {
        LOG_WARN("fail to push back", K(ret));
      } else {} // no more
    }
    std::sort(tmp_zone_array.begin(), tmp_zone_array.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_zone_array.count() - 1; ++i) {
      if (tmp_zone_array.at(i) == tmp_zone_array.at(i + 1)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("duplicate zone in primary zone list", K(ret));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "duplicate zone in primary zone list");
      }
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::construct_normalized_zone_array()
{
  int ret = OB_SUCCESS;
  if (NULL == zone_region_list_) {
    if (OB_FAIL(assign_zone_array_to_normalized())) {
      LOG_WARN("fail to assign zone array to normalized", K(ret));
    } else {} // no more to do
  } else {
    if (OB_FAIL(construct_normalized_with_zone_region_list())) {
      LOG_WARN("fail to construct normalized with zon region list", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObPrimaryZoneUtil::assign_zone_array_to_normalized()
{
  // copy zone_array_ to normalized_zone_array_, only zone_ and zone_score_ are filled
  // region and region_score_ are not filled here
  int ret = OB_SUCCESS;
  ZoneRegionScore zone_region_score;
  for (int i = 0; i < zone_array_.count() && OB_SUCC(ret); ++i) {
    const share::schema::ObZoneScore &zone_score = zone_array_.at(i);
    const common::ObZone &this_zone = zone_score.zone_;
    int64_t this_score = zone_score.score_;
    zone_region_score.reset();
    zone_region_score.zone_score_ = this_score;
    zone_region_score.zone_region_.zone_ = this_zone;
    if (OB_FAIL(normalized_zone_array_.push_back(zone_region_score))) {
      LOG_WARN("fail to push back", K(ret));
    } else {} // no more to do
  }
  return ret;
}

int ObPrimaryZoneUtil::construct_normalized_with_zone_region_list()
{
  int ret = OB_SUCCESS;
  if (NULL == zone_region_list_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone region list null", K(ret), KP(zone_region_list_));
  } else if (OB_FAIL(construct_basic_normalized_zone_array())) {
    LOG_WARN("fail to construct basic normalized zone array", K(ret));
  } else if (OB_FAIL(do_construct_normalized_zone_array())) {
    LOG_WARN("fail to do construct normalized_zone_array", K(ret));
  } else {} // do nothing
  return ret;
}

int ObPrimaryZoneUtil::check_zone_exist(
    const common::ObString &zone,
    bool &zone_exist)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(zone.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(zone));
  } else if (NULL == zone_region_list_) {
    // invoked by schema_service
    zone_exist = true;
  } else {
    // invoked by DDL
    zone_exist = false;
    for (int64_t i = 0; i < zone_list_.count() && !zone_exist; ++i) {
      const ObZone &this_zone = zone_list_.at(i);
      zone_exist = (ObString(this_zone.size(), this_zone.ptr()) == zone);
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::output_normalized_primary_zone(
    char *buf,
    int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!check_and_parse_finished_)
             || OB_UNLIKELY(normalized_zone_array_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot output normalized primary zone",
             K(ret), K(check_and_parse_finished_),
             " normalized zone array count", normalized_zone_array_.count());
  } else {
    bool start_format = false;
    int64_t prev_zone_score = normalized_zone_array_.at(0).zone_score_;
    int64_t prev_region_score = normalized_zone_array_.at(0).region_score_;
    const char *separator_token = NULL;
    for (int64_t i = 0; i < normalized_zone_array_.count() && OB_SUCC(ret); ++i) {
      ZoneRegionScore &cur_zone_region_score = normalized_zone_array_.at(i);
      if (INT64_MAX == cur_zone_region_score.region_score_
          && INT64_MAX == cur_zone_region_score.zone_score_) {
        // ignore this zone
      } else {
        const bool same_p = (cur_zone_region_score.zone_score_ == prev_zone_score
                             && cur_zone_region_score.region_score_ == prev_region_score);
        separator_token = (same_p ? "," : ";");
        if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                "%s", (!start_format ? "" : separator_token)))) {
          LOG_WARN("fail to format separator", K(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s",
                static_cast<int32_t>(cur_zone_region_score.zone_region_.zone_.size()),
                cur_zone_region_score.zone_region_.zone_.ptr()))) {
          LOG_WARN("fail to format zone", K(ret));
        } else {
          start_format = true;
          prev_zone_score = cur_zone_region_score.zone_score_;
          prev_region_score = cur_zone_region_score.region_score_;
        }
      }
    } // for
  }
  return ret;
}

void ObPrimaryZoneUtil::jump_over_blanks(
    int64_t &cursor,
    const int64_t end)
{
  while (cursor < end
         && primary_zone_str_[cursor] == BLANK_TOKEN) {
    ++cursor;
  }
}

int ObPrimaryZoneUtil::get_next_zone_score(
    int64_t &cursor,
    const int64_t end,
    share::schema::ObZoneScore &zone_score)
{
  int ret = OB_SUCCESS;
  jump_over_blanks(cursor, end);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cursor >= end) {
    ret = OB_ITER_END;
  } else {
    zone_score.reset();
    int64_t tmp_cursor = cursor;
    while (tmp_cursor < end
           && primary_zone_str_[tmp_cursor] != BLANK_TOKEN
           && primary_zone_str_[tmp_cursor] != COMMA_TOKEN
           && primary_zone_str_[tmp_cursor] != SEMI_COLON_TOKEN) {
      ++tmp_cursor;
    }
    int64_t len = tmp_cursor - cursor;
    if (OB_UNLIKELY(len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone len", K(ret));
    } else {
      zone_score.zone_.assign_ptr(&primary_zone_str_[cursor], static_cast<int32_t>(len));
      // zone score is a member of PrimaryZoneUtil
      zone_score.score_ = zone_score_;
    }
    if (OB_SUCC(ret)) {
      cursor = tmp_cursor;
      jump_over_blanks(cursor, end);
      if (cursor >= end) { // reach end
      } else if (COMMA_TOKEN == primary_zone_str_[cursor]) {
        ++cursor; // no need to update zone_score_, just inc cursor and pass COMMA TOKEN
      } else if (SEMI_COLON_TOKEN == primary_zone_str_[cursor]) {
        ++cursor; // inc cursor and pass SEMI COLON TOKEN
        ++zone_score_; // come across SEMI COLON TOKEN, need to update zone_score_
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid primary zone", K(ret));
      }
    }
  }
  return ret;
}

// delete zones not exist in the zone list, fill zone and region to each ZoneRegion element,
// the zone_score_ and region_score_ of each ZoneRegion are not filled by now
int ObPrimaryZoneUtil::construct_basic_normalized_zone_array()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == zone_region_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone region list null", K(ret), KP(zone_region_list_));
  } else {
    for (int64_t i = 0; i < zone_region_list_->count() && OB_SUCC(ret); ++i) {
      const common::ObZone &zone = zone_region_list_->at(i).zone_;
      const common::ObString zone_str(zone.size(), zone.ptr());
      bool zone_exist = false;
      if (OB_FAIL(check_zone_exist(zone_str, zone_exist))) {
        LOG_WARN("fail to check zone exist", K(ret));
      } else if (zone_exist) {
        if (OB_FAIL(normalized_zone_array_.push_back(
                ZoneRegionScore(zone_region_list_->at(i))))) {
          LOG_WARN("fail to push back", K(ret));
        } else {} // no more to do
      } else {} // do not exist in zone list, do nothing
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::do_construct_normalized_zone_array()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == zone_region_list_)) { // defensive check
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone region list null", K(ret), KP(zone_region_list_));
  } else if (OB_FAIL(prefill_zone_region_score())) {
    LOG_WARN("fail to prefill zone region score", K(ret));
  } else if (OB_FAIL(update_region_score())) {
    LOG_WARN("fail to update region score", K(ret));
  } else {} // no more to do
  return ret;
}

int ObPrimaryZoneUtil::prefill_zone_region_score()
{
  // prefill the zone score in zone_array_ to normalized_zone_array_,
  // set region_score_ to zone_score_ temporarily, region_score_ needs to by modified afterwards
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == zone_region_list_)) { // defensive check
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone region list null", K(ret), KP(zone_region_list_));
  } else {
    for (int64_t i = 0; i < zone_array_.count() && OB_SUCC(ret); ++i) {
      const share::schema::ObZoneScore &zone_score = zone_array_.at(i);
      const common::ObZone &this_zone = zone_score.zone_;
      for (int64_t j = 0; j < normalized_zone_array_.count(); ++j) {
        if (this_zone == normalized_zone_array_.at(j).zone_region_.zone_.ptr()) {
          normalized_zone_array_.at(j).zone_score_ = zone_score.score_;
          normalized_zone_array_.at(j).region_score_ = zone_score.score_;
          break;
        } else {} // go on find
      }
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::update_region_score()
{
  // based on the prefill_zone_region_score,
  // modifiy the region score to one value for all regions with the same name
  // sort by region,region score to make zones with the same region get together
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == zone_region_list_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone region list null", K(ret), KP(zone_region_list_));
  } else {
    RegionScoreCmp region_score_cmp;
    std::sort(normalized_zone_array_.begin(), normalized_zone_array_.end(), region_score_cmp);
    ObRegion last_region;
    int64_t last_region_score = INT64_MAX;
    for (int64_t i = 0; i < normalized_zone_array_.count() && OB_SUCC(ret); ++i) {
      if (last_region != normalized_zone_array_.at(i).zone_region_.region_) {
        last_region = normalized_zone_array_.at(i).zone_region_.region_;
      } else {
        normalized_zone_array_.at(i).region_score_ = last_region_score;
      }
      last_region_score = normalized_zone_array_.at(i).region_score_;
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::construct_full_zone_array()
{
  int ret = OB_SUCCESS;
  // sorted by region_score,zone_score,zone, the zone priority is determined by now
  FinalCmp final_cmp;
  std::sort(normalized_zone_array_.begin(), normalized_zone_array_.end(), final_cmp);
  if (OB_UNLIKELY(normalized_zone_array_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot construct full zone array", K(ret),
             "normalized zone array count", normalized_zone_array_.count());
  } else {
    int64_t zone_p_score = 0;
    int64_t prev_zone_score = normalized_zone_array_.at(0).zone_score_;
    int64_t prev_region_score = normalized_zone_array_.at(0).region_score_;
    for (int64_t i = 0; i < normalized_zone_array_.count() && OB_SUCC(ret); ++i) {
      const ZoneRegionScore &cur_zone_region_score = normalized_zone_array_.at(i);
      const ObZone &zone = cur_zone_region_score.zone_region_.zone_;
      if (INT64_MAX == cur_zone_region_score.region_score_
          && INT64_MAX == cur_zone_region_score.zone_score_) {
        // ignore this zone
      } else {
        ObString zone_str;
        if (cur_zone_region_score.zone_score_ != prev_zone_score
            || cur_zone_region_score.region_score_ != prev_region_score) {
          ++zone_p_score;
        }
        if (OB_FAIL(ob_write_string(allocator_, ObString(zone.size(), zone.ptr()), zone_str))) {
          LOG_WARN("fail to deep copy string", K(ret));
        } else if (OB_FAIL(full_zone_array_.push_back(ObZoneScore(zone_str, zone_p_score)))) {
          LOG_WARN("fail to push back", K(ret));
        } else {
          prev_zone_score = cur_zone_region_score.zone_score_;
          prev_region_score = cur_zone_region_score.region_score_;
        }
      }
    }
  }
  return ret;
}

bool ObPrimaryZoneUtil::RegionScoreCmp::operator()(
    const ZoneRegionScore &left,
    const ZoneRegionScore &right)
{
  bool bool_ret = false;
  if (left.zone_region_.region_ < right.zone_region_.region_) {
    bool_ret = true;
  } else if (left.zone_region_.region_ > right.zone_region_.region_) {
    bool_ret = false;
  } else {
    if (left.region_score_ < right.region_score_) {
      bool_ret = true;
    } else {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObPrimaryZoneUtil::FinalCmp::operator()(
    const ZoneRegionScore &left,
    const ZoneRegionScore &right)
{
  bool bool_ret = false;
  if (left.region_score_ < right.region_score_) {
    bool_ret = true;
  } else if (left.region_score_ > right.region_score_) {
    bool_ret = false;
  } else {
    if (left.zone_score_ < right.zone_score_) {
      bool_ret = true;
    } else if (left.zone_score_ > right.zone_score_) {
      bool_ret = false;
    } else {
      if (left.zone_region_.zone_ < right.zone_region_.zone_) {
        bool_ret = true;
      } else {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
}

bool ObPrimaryZoneUtil::check_primary_zone_equal(
     const share::schema::ObPrimaryZone &left,
     const share::schema::ObPrimaryZone &right)
{
  bool is_equal = true;
  const common::ObIArray<ObZoneScore> &left_primary_zone_array = left.get_primary_zone_array();
  const common::ObIArray<ObZoneScore> &right_primary_zone_array = right.get_primary_zone_array();
  if (left_primary_zone_array.count() != right_primary_zone_array.count()) {
    is_equal = false;
  } else if (0 == left_primary_zone_array.count()) {
    is_equal = left.get_primary_zone() == right.get_primary_zone();
  } else {
    for (int i = 0; is_equal && i < left_primary_zone_array.count(); ++i) {
      is_equal = false;
      for (int j = 0; !is_equal && j < right_primary_zone_array.count(); ++j) {
        if (left_primary_zone_array.at(i).zone_ == right_primary_zone_array.at(j).zone_
            && left_primary_zone_array.at(i).score_ == right_primary_zone_array.at(j).score_) {
          is_equal = true;
        }
      }
    }
  }
  return is_equal;
}

int ObPrimaryZoneUtil::check_primary_zone_equal(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObPartitionSchema &left,
    const share::schema::ObPartitionSchema &right,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  ObArenaAllocator allocator("PrimaryZone");
  ObPrimaryZone left_primary_zone(allocator);
  ObPrimaryZone right_primary_zone(allocator);
  ObArray<share::ObZoneReplicaNumSet> left_zone_locality;
  ObArray<share::ObZoneReplicaNumSet> right_zone_locality;
  // some primary_zone_str in __all_table originated from an old cluster
  // may not be normalized to a standard primary zone string,
  // its normalized form are set into primary_zone_array during a schema flush operation
  if (OB_FAIL(left.get_primary_zone_inherit(schema_guard, left_primary_zone))) {
    LOG_WARN("fail to get left primary_zone_array_inherit", K(ret));
  } else if (OB_FAIL(right.get_primary_zone_inherit(schema_guard, right_primary_zone))) {
    LOG_WARN("fail to get right primary_zone_array_inherit", K(ret));
  } else {
    is_match = check_primary_zone_equal(left_primary_zone, right_primary_zone);
  }
  return ret;
}

int ObPrimaryZoneUtil::get_tenant_primary_zone_array(
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<common::ObZone> &primary_zone_array)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::schema::ObZoneScore, DEFAULT_ZONE_COUNT> zone_score_list;
  ObArenaAllocator allocator("PrimaryZone");
  ObPrimaryZone primary_zone_schema(allocator);
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;

  if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is invalid", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(get_tenant_primary_zone_score(tenant_schema,
          primary_zone_schema, zone_locality, zone_score_list))) {
    LOG_WARN("failed to get tenant primary zone score", KR(ret), K(tenant_schema));
  } else {
    int64_t first_score = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_score_list.count(); ++i) {
      const share::schema::ObZoneScore &zone_score = zone_score_list.at(i);
      if (-1 == first_score) {
        first_score = zone_score.score_;
      }
      if (zone_score.score_ == first_score) {
        ObZone zone(zone_score.zone_);
        if (OB_FAIL(primary_zone_array.push_back(zone))) {
          LOG_WARN("failed to pushback primary zone", KR(ret), K(zone_score), K(zone));
        }
      }
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::get_tenant_primary_zone_score(
      const share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObPrimaryZone &primary_zone_schema,
      common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      common::ObIArray<share::schema::ObZoneScore> &zone_score_list)
{
  int ret = OB_SUCCESS;
  zone_score_list.reset();
  share::schema::ObSchemaGetterGuard guard;
  ObArray<share::schema::ObZoneRegion> zone_region_list;
  common::ObSEArray<common::ObZone, DEFAULT_ZONE_COUNT> zone_list;
  ObZone primary_zone_str;
  if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is invalid", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(tenant_schema.get_primary_zone_inherit(
                 guard, primary_zone_schema))) {
    LOG_WARN("failed to get primary zone", KR(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_list(zone_list))) {
    LOG_WARN("fail to get zone list", K(ret));
  } else if (OB_UNLIKELY(zone_list.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone list count unexpected", K(ret));
  } else if (OB_FAIL(tenant_schema.get_zone_replica_attr_array(
                 zone_locality))) {
    LOG_WARN("fail to get zone replica num array inherit", K(ret));
  } else if (OB_FAIL(generate_integrated_primary_zone_str(
                 primary_zone_schema, zone_list, zone_locality,
                 zone_score_list,
                 primary_zone_str))) {
    LOG_WARN("fail to generate integrated primary zone str", K(ret));
  }
  return ret;
}


int ObPrimaryZoneUtil::get_ls_primary_zone_priority(
    const ObZone &ls_primary_zone,
    const share::schema::ObTenantSchema &tenant_schema,
    ObSqlString &zone_priority)
{
  int ret = OB_SUCCESS;
  zone_priority.reset();

  if (OB_UNLIKELY(ls_primary_zone.is_empty() || !tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("primary zone is empty", KR(ret), K(ls_primary_zone), K(tenant_schema));
  } else if (OB_FAIL(zone_priority.assign(ls_primary_zone.ptr()))) {
    LOG_WARN("failed to assign ls primary zone", KR(ret), K(ls_primary_zone));
  } else {
    const char *separator_token = ";";
    common::ObSEArray<share::schema::ObZoneScore, DEFAULT_ZONE_COUNT> zone_score_array;
    ObArenaAllocator allocator("PrimaryZone");
    ObPrimaryZone primary_zone_schema(allocator);
    ObArray<share::ObZoneReplicaAttrSet> zone_locality;

    if (OB_FAIL(get_tenant_primary_zone_score(tenant_schema,
            primary_zone_schema, zone_locality, zone_score_array))) {
      LOG_WARN("failed to get zone score list", KR(ret), K(tenant_schema));
    } else if (1 == zone_score_array.count()) {
    } else if (OB_FAIL(zone_priority.append_fmt("%s", separator_token))) {
      LOG_WARN("failed to append", KR(ret), K(separator_token));
    } else {
      int64_t prev_zone_score = zone_score_array.at(0).score_;
      bool first = true;
      for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); ++i) {
        const ObZoneScore &cur_zone_score = zone_score_array.at(i);
        const bool same_p = (cur_zone_score.score_ == prev_zone_score);
        separator_token = (same_p ? "," : ";");
        if (cur_zone_score.zone_ == ls_primary_zone.str()) {
          //nothing todo
          //already in zone priority
        } else if (!first && OB_FAIL(zone_priority.append_fmt("%s", separator_token))) {
          LOG_WARN("failed to append", KR(ret), K(separator_token));
        } else if (OB_FAIL(zone_priority.append_fmt("%.*s",
                cur_zone_score.zone_.length(), cur_zone_score.zone_.ptr()))) {
          LOG_WARN("failed to append", KR(ret), K(separator_token), K(cur_zone_score));
        } else {
          first = false;
        }
        prev_zone_score = cur_zone_score.score_;
      }
    }
  }
  return ret;
}


int ObPrimaryZoneUtil::get_tenant_zone_priority(
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObSqlString &zone_priority)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::schema::ObZoneScore, DEFAULT_ZONE_COUNT> zone_score_array;
  ObZone primary_zone_str;
  ObArenaAllocator allocator("PrimaryZone");
  ObPrimaryZone primary_zone_schema(allocator);
  ObArray<share::ObZoneReplicaAttrSet> zone_locality;
  if (OB_UNLIKELY(!tenant_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant schema is invalid", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(get_tenant_primary_zone_score(tenant_schema,
          primary_zone_schema, zone_locality, zone_score_array))) {
    LOG_WARN("failed to get zone score list", KR(ret), K(tenant_schema));
  } else if (OB_FAIL(do_generate_integrated_primary_zone_str(zone_score_array, primary_zone_str))) {
    LOG_WARN("failed to generate_integrated primary zone", KR(ret), K(zone_score_array));
  } else if (OB_FAIL(zone_priority.assign(primary_zone_str.str()))) {
    LOG_WARN("failed to assign zone priority", KR(ret), K(primary_zone_str));
  }
  return ret;

}

int ObPrimaryZoneUtil::convert_random_primary_zone_into_integrated(
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
    common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
    common::ObZone &primary_zone_str)
{
  int ret = OB_SUCCESS;
  primary_zone_str.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &this_set = zone_locality.at(i);
    if (this_set.get_full_replica_num() >= 1) {
      for (int64_t j = 0; OB_SUCC(ret) && j < this_set.get_zone_set().count(); ++j) {
        const common::ObZone &this_zone = this_set.get_zone_set().at(j);
        const char *zone_ptr = this_zone.ptr();
        ObString zone_str(zone_ptr);
        ObZoneScore zone_score(zone_str, 0);
        if (!has_exist_in_array(zone_list, this_zone)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("this zone is not in zone list", K(ret), K(this_zone), K(zone_list));
        } else if (OB_FAIL(zone_score_list.push_back(zone_score))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    } else {} // has no full replica, cannot be leader
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_generate_integrated_primary_zone_str(zone_score_list, primary_zone_str))) {
    LOG_WARN("fail to do generate integrated primary zone str", K(ret));
  }
  return ret;
}

int ObPrimaryZoneUtil::do_generate_integrated_primary_zone_str(
    const common::ObIArray<share::schema::ObZoneScore> &zone_score_array,
    common::ObZone &primary_zone_str)
{
  int ret = OB_SUCCESS;
  char zone_str[MAX_ZONE_LENGTH];
  MEMSET(zone_str, 0, MAX_ZONE_LENGTH);
  if (zone_score_array.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "array num", zone_score_array.count());
  } else {
    int64_t pos = 0;
    bool start_format = false;
    int64_t prev_zone_score = zone_score_array.at(0).score_;
    const char *separator_token = NULL;
    for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); ++i) {
      const ObZoneScore &cur_zone_score = zone_score_array.at(i);
      const bool same_p = (cur_zone_score.score_ == prev_zone_score);
      separator_token = (same_p ? "," : ";");
      if (OB_FAIL(databuff_printf(zone_str, MAX_ZONE_LENGTH, pos,
              "%s", (!start_format ? "" : separator_token)))) {
        LOG_WARN("fail to format separator", K(ret));
      } else if (OB_FAIL(databuff_printf(zone_str, MAX_ZONE_LENGTH, pos, "%.*s",
              static_cast<int32_t>(cur_zone_score.zone_.length()),
              cur_zone_score.zone_.ptr()))) {
        LOG_WARN("fail to format zone", K(ret));
      } else {
        start_format = true;
        prev_zone_score = cur_zone_score.score_;
      }
    } // for
    if (OB_SUCC(ret)) {
      if (OB_FAIL(primary_zone_str.assign(zone_str))) {
        LOG_WARN("fail to assign primary zone", K(ret));
      }
    }
  }
  return ret;
}

int ObPrimaryZoneUtil::convert_normal_primary_zone_into_integrated(
    const common::ObIArray<share::schema::ObZoneScore> &primary_zone_array,
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::ObZoneReplicaAttrSet> &zone_locality,
    common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
    common::ObZone &primary_zone_str)
{
  int ret = OB_SUCCESS;
  primary_zone_str.reset();
  common::ObArray<common::ObZone> full_replica_zone_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < zone_locality.count(); ++i) {
    const share::ObZoneReplicaAttrSet &this_set = zone_locality.at(i);
    if (this_set.get_full_replica_num() >= 1) {
      for (int64_t j = 0; OB_SUCC(ret) && j < this_set.get_zone_set().count(); ++j) {
        const common::ObZone &this_zone = this_set.get_zone_set().at(j);
        if (!has_exist_in_array(zone_list, this_zone)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("this zone is not in zone list", K(ret));
        } else if (OB_FAIL(full_replica_zone_list.push_back(this_zone))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < primary_zone_array.count(); ++i) {
    const share::schema::ObZoneScore &this_score = primary_zone_array.at(i);
    const common::ObZone &this_zone = this_score.zone_.ptr();
    if (has_exist_in_array(full_replica_zone_list, this_zone)) {
      ObZoneScore zone_score = this_score;
      if (OB_FAIL(zone_score_list.push_back(zone_score))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_generate_integrated_primary_zone_str(zone_score_list, primary_zone_str))) {
    LOG_WARN("fail to do generate integrated primary zone str", K(ret));
  }
  return ret;
}

int ObPrimaryZoneUtil::generate_integrated_primary_zone_str(
    const share::schema::ObPrimaryZone &primary_zone,
    const common::ObIArray<common::ObZone> &zone_list,
    const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
    common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
    common::ObZone &primary_zone_str)
{
  int ret = OB_SUCCESS;
  primary_zone_str.reset();
  const ObString &raw_primary_zone = primary_zone.get_primary_zone();
  if (raw_primary_zone.empty() || raw_primary_zone == OB_RANDOM_PRIMARY_ZONE) {
    if (OB_FAIL(convert_random_primary_zone_into_integrated(
            zone_list, zone_locality, zone_score_list, primary_zone_str))) {
      LOG_WARN("fail to convert random primary zone", K(ret));
    }
  } else {
    if (OB_FAIL(convert_normal_primary_zone_into_integrated(
            primary_zone.get_primary_zone_array(), zone_list, zone_locality, zone_score_list, primary_zone_str))) {
      LOG_WARN("fail to convert normal primary zone", K(ret));
    }
  }
  return ret;
}

int ObRawPrimaryZoneUtil::build(
    const ObZone &primary_zone,
    common::ObIArray<ZoneScore> &zone_score_array,
    common::ObIArray<RegionScore> &region_score_array)
{
  int ret = OB_SUCCESS;
  // when upgrade from 1.4.x to 2.0, primary_zone may be empty string, the empty primary zone
  // has the same semantics with a random primary zone, for compatible consideration, empty
  // primary zone here is permitted
  if (!ObPrimaryZoneUtil::no_need_to_check_primary_zone(primary_zone.str())) {
    zone_score_array.reset();
    region_score_array.reset();
    int64_t cursor = 0;
    const int64_t len = primary_zone.size();
    const char *zone_ptr = primary_zone.ptr();
    ZoneScore zone_score;
    while (OB_SUCC(ret)
           && OB_SUCC(get_next_zone_score(zone_ptr, cursor, len, zone_score))) {
      if (OB_FAIL(zone_score_array.push_back(ZoneScore(zone_score)))) {
        LOG_WARN("fail to push back", K(ret), K(zone_score));
      } else {} // ok
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    for (int64_t i = 0; i < zone_score_array.count() && OB_SUCC(ret); ++i) {
      ObRegion region;
      const ZoneScore &zone_score = zone_score_array.at(i);
      HEAP_VAR(share::ObZoneInfo, zone_info) {
        zone_info.zone_ = zone_score.zone_;
        if (OB_FAIL(zone_mgr_.get_zone(zone_info))) {
          LOG_WARN("fail to get zone", K(ret));
        } else if (OB_FAIL(region.assign(zone_info.region_.info_.ptr()))) {
          LOG_WARN("fail to assign region", K(ret));
        } else {
          bool is_region_exist = false;
          for (int64_t j = 0; j < region_score_array.count() && !is_region_exist; ++j) {
            if (region == region_score_array.at(j).region_) {
              is_region_exist = true;
            } else {} // go on
          }
          if (is_region_exist) {
          } else if (OB_FAIL(region_score_array.push_back(
                  RegionScore(region, zone_score.zone_score_)))) {
            LOG_WARN("fail to push back", K(ret), K(region),
                     "score", zone_score.zone_score_);
          } else {} // ok
        }
      }
    }
  }
  return ret;
}

void ObRawPrimaryZoneUtil::jump_over_blanks(
     const char *ptr,
     int64_t &cursor,
     const int64_t end)
{
  while (cursor < end
         && ptr[cursor] == BLANK_TOKEN) {
    ++cursor;
  }
}

int ObRawPrimaryZoneUtil::get_next_zone_score(
    const char *ptr,
    int64_t &cursor,
    const int64_t end,
    ZoneScore &zone_score)
{
  int ret = OB_SUCCESS;
  common::ObZone zone;
  zone_score.reset();
  jump_over_blanks(ptr, cursor, end);
  if (cursor >= end) {
    ret = OB_ITER_END;
  } else {
    int64_t tmp_cursor = cursor;
    while (tmp_cursor < end
           && ptr[tmp_cursor] != BLANK_TOKEN
           && ptr[tmp_cursor] != COMMA_TOKEN
           && ptr[tmp_cursor] != SEMI_COLON_TOKEN) {
      ++tmp_cursor;
    }
    int64_t len = tmp_cursor - cursor;
    bool zone_exist = false;
    if (OB_UNLIKELY(len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid zone len", K(ret), K(len));
    } else if (OB_FAIL(zone.assign(ObString(len, &ptr[cursor])))) {
      LOG_WARN("fail to assign zone", K(ret));
    } else if (OB_FAIL(zone_mgr_.check_zone_exist(zone, zone_exist))) {
      LOG_WARN("fail to check zone exist", K(ret), K(zone));
    } else if (!zone_exist) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone do not exist", K(ret), K(zone));
    } else {
      zone_score.zone_ = zone;
      zone_score.zone_score_= current_score_;
    }
    if (OB_SUCC(ret)) {
      cursor = tmp_cursor;
      jump_over_blanks(ptr, cursor, end);
      if (cursor >= end) {
      } else if (COMMA_TOKEN == ptr[cursor]) {
        ++cursor; // no need to inc current_score
      } else if (SEMI_COLON_TOKEN == ptr[cursor]) {
        ++cursor; // inc cursor and pass SEMI COLON TOKEN
        ++current_score_; // come across SEMI COLON TOKEN, inc current_score_
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid zone", K(ret));
      }
    }
  }
  return ret;
}

int ObRawPrimaryZoneUtil::generate_high_priority_zone_array(
    const common::ObIArray<ZoneScore> &zone_score_array,
    common::ObIArray<common::ObZone> &high_priority_zone_array)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(zone_score_array.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone score array count unexpected", K(ret));
  } else {
    high_priority_zone_array.reset();
    const int64_t sample_score = zone_score_array.at(0).zone_score_;
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_score_array.count(); ++i) {
      const int64_t this_score = zone_score_array.at(i).zone_score_;
      const common::ObZone &this_zone = zone_score_array.at(i).zone_;
      if (sample_score == this_score) {
        if (OB_FAIL(high_priority_zone_array.push_back(this_zone))) {
          LOG_WARN("fail to push back", K(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase

