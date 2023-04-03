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

#ifndef OCEANBASE_SHARE_PRIMARY_ZONE_UTIL_H_
#define OCEANBASE_SHARE_PRIMARY_ZONE_UTIL_H_

#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array.h"
#include "schema/ob_schema_struct.h"

namespace oceanbase
{
namespace rootserver
{
class ObZoneManager;
};
namespace share
{
namespace schema
{
class ObPrimaryZone;
class ObSchemaGetterGuard;
class ObTableSchema;
class ObTenantSchema;
}
/* 1 the nature of primary_zone is extended from OceanBase v1.4, it is extended to contain multiple zones.
 *   zones in primary zone string are designated a priority using a specific primary zone syntax. 
 *   for instance: if we declare a variable ObZone primary_zone("zone1,zone2;zone3");
 *   if means zone1 and zone2 have the highest priority for leader election, the priority of zone3 is lower
 *   than that of zone1 and zone2; in the primary zone syntax, ',' separate zones with the same priority
 *   and ';' separate zones with different priorities. zones before ';' have higher priorities;
 * 2 ObPrimaryZoneUtil provides a method to output a normalized primary zone called output_normalized_primary_zone
 *   the differences between a normalized primary zone and and the input primary zone are as follows:
 *   if the cluster deployment is zone1@HZ, zone2@HZ, zone3@SH, zone4@SH, zone5@SZ;
 *   an input primary zone is "zone1;zone3"; then the normalized primary zone is "zone1;zone2;zone3;zone4",
 *   the principles to normalize the primary zone are as follows:
 *   a no zone2 exists in the input primary zone, however zone2 and zone1 are both in the region HZ,
 *     so zone2 shall have some certain priorities in the normalized primary zone
 *   b the priority of zone2 is lower than that of zone1,
 *     however region HZ has a higher priority than that of region SH, this leads to the consequence that
 *     the priority of zone2 is higher than that of zone3
 *   c the priority calculation method of zone4 is similar with that of zone2
 *     the result of the normalized primary zone is 'zone1;zone2;zone3;zone4'
 * 3 ObPrimaryZoneUtil are invoked in two situations:
 *   a invoked in some ddl operations like create tenant/create database/create table, in this situation
 *     ObPrimaryZoneUtil will normalize the primary zone, and set the normalized primary zone string 
 *     back to schema. Given the convenient use consideration, we format the priority of each zone in primary
 *     zone string in an primary_zone_array_ in the schema, this array is sorted by the zone priority, the one
 *     with a higher priority are at the beginings of the arrary.
 *   b invoked by schema refresh, the primary zone in schema has been normalized at the time they are written
 *     into the schema.
 * 4 the implementation of ObPrimaryZoneUtil is as follows:
 *   a parse all zones in primary zone string using RawPrimaryZoneUtil,
 *     and push these zones pack into zone_array_, for example parse primary zone = "zone1;zone3" and then push
 *     the elements into zone_array_, two elements of zone_array_ are zone_ar(zone1,0), (zone3,1),
 *     each element contains a zone name and a priority score, a smaller priority score means a higher priority.
 *   b push all elements of zone_region_list into normalized_zone_array_, five elements of normalize_zone_array_
 *     are (zone1,max,HZ,max),(zone2,max,HZ,max),(zone3,max,SH,max),(zone4,max,SH,max),(zone5,max,SH,max),
 *     each element in normalize_zone_array_ contains (zone name,zone score,region name,region score). a smaller
 *     priority score menas a higher priority
 *   c replace zone score of each normalized_zone_array_ element with the corresponding zone score of
 *     the zone_array_ elements, the region score of normalized_zone_array_ elements are replaced with
 *     the corresponding zone score of the zone_array_ elements temporarily.
 *     ignore the region score of normalized_zone_array_ whose zone cannot be found in zone_array, after this stop,
 *     normalized_zone_array_ are (zone1,0,HZ,0),(zone2,max,HZ,max),(zone3,1,SH,1),(zone4,max,SH,max),(zone5,max,SH,max).
 *   d sort normalized_zone_array_ with the sequence: region name, region_score.
 *     to aggregate zones with the same regions.
 *   e update the region score for eache zone using the rule: fill all elements region score in the same region
 *     with the first element's region score, after this step, normalized_zone_array_ are
 *     (zone1,0,HZ,0),(zone2,max,HZ,0),(zone3,1,SH,1),(zone4,max,SH,1),(zone5,max,SH,max).
 *   f sort normalized_zone_array_ again with the sequence: region score,zone score, zone name. the consequence are:
 *     (zone1,0,HZ,0),(zone2,max,HZ,0),(zone3,1,SH,1),(zone4,max,SH,1),(zone5,max,SH,max).
 *   g remove the element whose zone score and region score are both max, and then we get the result.
 *     the normalized primary zone is "zone1;zone2;zone3;zone4"
 */
class ObPrimaryZoneUtil
{
public:
  // when invoked by ddl zone_region_list is a valid pointer,
  // when invoked by schema, zone_region_list is NULL
  ObPrimaryZoneUtil(
    const common::ObString &primary_zone,
    const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list = NULL)
    : primary_zone_(primary_zone),
      zone_list_(),
      zone_region_list_(zone_region_list),
      zone_array_(),
      normalized_zone_array_(),
      full_zone_array_(),
      primary_zone_str_(),
      zone_score_(0),
      allocator_(),
      check_and_parse_finished_(false),
      is_inited_(false)

  {}
  ~ObPrimaryZoneUtil() {} // no one shall derive from this
public:
  // when invoked by DDL, zones in primary zone string is not confirmed to be a valid zone,
  // zone_list is an essential input argument to check if all zones in primary zone is valid;
  int init(const common::ObIArray<common::ObString> &zone_list);
  int init(const common::ObIArray<common::ObZone> &zone_list);
  // when invoked by schema service, zones in primary zone string is confirmed to be a valid zone,
  // zone_list is not an essential input argument any more.
  int init();
public:
  // check if the zone exists, if so parse it based on the priority
  int check_and_parse_primary_zone();
  // when primary zone is "zone1,   zone2; zone3", is is normalized to "zone1,zone2;zone3";
  // at the end
  int output_normalized_primary_zone(char *buf, int64_t buf_len, int64_t &pos);
  const common::ObIArray<schema::ObZoneScore> &get_zone_array() {return full_zone_array_;}


private:
  struct ZoneRegionScore
  {
    ZoneRegionScore()
      : zone_region_(),
        zone_score_(INT64_MAX),
        region_score_(INT64_MAX) {}
    ZoneRegionScore(const schema::ObZoneRegion &zone_region)
      : zone_region_(zone_region),
        zone_score_(INT64_MAX),
        region_score_(INT64_MAX) {}
    ~ZoneRegionScore() {} // no one shall derive from this
    int assign(const ZoneRegionScore &that) {
      int ret = common::OB_SUCCESS;
      if (OB_FAIL(zone_region_.assign(that.zone_region_))) {
        SHARE_LOG(WARN, "fail to assign zone and region", K(ret), K(that));
      } else {
        zone_score_ = that.zone_score_;
        region_score_ = that.region_score_;
      }
      return ret;
    }
    void reset() {
      zone_region_.reset();
      zone_score_ = INT64_MAX;
      region_score_ = INT64_MAX;
    }
    TO_STRING_KV(K(zone_region_), K(zone_score_), K(region_score_));

    schema::ObZoneRegion zone_region_;
    int64_t zone_score_;
    int64_t region_score_;
  };
  struct RegionScoreCmp
  {
    bool operator()(const ZoneRegionScore &left, const ZoneRegionScore &that);
  };
  struct FinalCmp
  {
    bool operator()(const ZoneRegionScore &left, const ZoneRegionScore &that);
  };
  typedef common::ObSEArray<ZoneRegionScore,
                            common::MAX_ZONE_NUM,
                            common::ObNullAllocator> ZoneRegionScoreArray;
private:
  int get_next_zone_score(int64_t &cursor, const int64_t end, share::schema::ObZoneScore &zone_score);
  void jump_over_blanks(int64_t &cursor, const int64_t end);
  int check_zone_exist(const common::ObString &zone, bool &zone_exist);
  int construct_zone_array(); // construct zone_array_
  int construct_normalized_zone_array(); //  construct normalized_zone_array_
  // when invoked by flush schema, assign zone_array_ to normalized_zone_array_ directly
  int assign_zone_array_to_normalized();
  // when invoked by ddl, calculate normalized_zone_array_ based on zone_region_list_ and zone_array_
  int construct_normalized_with_zone_region_list();
  // related to the comments 4.b
  int construct_basic_normalized_zone_array();
  // related to the comments 4.c,4.d and 4.e
  int do_construct_normalized_zone_array();
  // related to the comments 4.c and 4.d
  int prefill_zone_region_score();
  // related to the comments 4.e
  int update_region_score();
  // related to the comments 4.f and 4.g
  int construct_full_zone_array(); // construct full_zone_array_
private:
  static const char BLANK_TOKEN = ' ';
  static const char COMMA_TOKEN = ',';
  static const char SEMI_COLON_TOKEN = ';';
  static const int64_t ZONE_COUNT = 5; // usually no more than five
private:
  const common::ObString &primary_zone_; // this is a list indeed
  common::ObSEArray<common::ObZone, ZONE_COUNT> zone_list_;
  const common::ObIArray<schema::ObZoneRegion> *zone_region_list_;
  // zone_array_ contains zones which are included in primary zone string,
  // for instance primary zone = "zone1;zone3"
  // then there two elements in zone_array_, they are zone1 and zone3.
  common::ObArray<share::schema::ObZoneScore> zone_array_;
  // normalized_zone_array_ contains normalized zone based on region infos
  // for instacne primary zone="zone1;zone3"
  // the normalized primary zone="zone1;zone2;zone3;zone4",
  // there are for elements in primary_zone_array_,
  // they are zone1,zone2,zone3 and zone4
  common::ObArray<ZoneRegionScore> normalized_zone_array_;
  // normalized_zone_array_ has a extra region info while full_zone_array_ has not
  common::ObArray<share::schema::ObZoneScore> full_zone_array_;
  // the strings in zone_array_ are pointing into primary_zone_str_
  char primary_zone_str_[common::MAX_ZONE_LENGTH + 1];
  int64_t zone_score_; // this is used to record zone priority during check and parse
  common::ObArenaAllocator allocator_;
  bool check_and_parse_finished_;
  bool is_inited_;

/* static method
 */
public:
  static bool check_primary_zone_equal(const share::schema::ObPrimaryZone &left,
                                       const share::schema::ObPrimaryZone &right);
  static int check_primary_zone_equal(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const share::schema::ObPartitionSchema &left,
      const share::schema::ObPartitionSchema &right,
      bool &is_match);

  static bool is_specific_primary_zone(const common::ObString &primary_zone_str)
  {
    return primary_zone_str != common::ObString(common::OB_RANDOM_PRIMARY_ZONE)
           && !primary_zone_str.empty();
  }
  // no need to check when primary zone is 'random' or 'default'
  static bool no_need_to_check_primary_zone(const common::ObString &primary_zone_str)
  {
    return primary_zone_str == common::ObString(common::OB_RANDOM_PRIMARY_ZONE) ||
           primary_zone_str.empty();
  }

public:
  static int get_tenant_primary_zone_array(
      const share::schema::ObTenantSchema &tenant_schema,
      common::ObIArray<common::ObZone> &primary_zone_array);
  static int get_tenant_zone_priority(
      const share::schema::ObTenantSchema &tenant_schema,
      ObSqlString &zone_priority);
  static int get_ls_primary_zone_priority(const ObZone &ls_primary_zone,
      const share::schema::ObTenantSchema &tenant_schema,
      ObSqlString &zone_priority);
  static int get_tenant_primary_zone_score(
      const share::schema::ObTenantSchema &tenant_schema,
      share::schema::ObPrimaryZone &primary_zone,
      common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      common::ObIArray<share::schema::ObZoneScore> &zone_score_list);
private:
  static int generate_integrated_primary_zone_str(
      const share::schema::ObPrimaryZone &primary_zone,
      const common::ObIArray<common::ObZone> &zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
      common::ObZone &primary_zone_str);
  static int convert_random_primary_zone_into_integrated(
      const common::ObIArray<common::ObZone> &zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
      common::ObZone &primary_zone_str);
  static int convert_normal_primary_zone_into_integrated(
      const common::ObIArray<share::schema::ObZoneScore> &primary_zone_array,
      const common::ObIArray<common::ObZone> &zone_list,
      const common::ObIArray<share::ObZoneReplicaNumSet> &zone_locality,
      common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
      common::ObZone &primary_zone_str);
  static int do_generate_integrated_primary_zone_str(
      const common::ObIArray<share::schema::ObZoneScore> &zone_score_list,
      common::ObZone &primary_zone_str);
};

/* ObPrimaryZoneUtil is a full utility parser of primary zone, One can refer to the comments
 * at the beginning of this file to get its implentation details.
 * ObRawPrimaryZoneUtil is a raw parser of primary zone, we say it raw
 * because it just parse the primary zone from the perspective of raw primary zone string,
 * no region information is used by ObRawPrimaryZoneUtil. So every one uses ObRawPrimaryZoneUtil
 * shall know the implications
 */
class ObRawPrimaryZoneUtil
{
public:
  struct ZoneScore
  {
    ZoneScore() : zone_(), zone_score_(INT64_MAX) { reset(); }
    ZoneScore(const common::ObZone &zone, const int64_t zone_score)
      : zone_(zone), zone_score_(zone_score) {}
    ~ZoneScore() {} // no one shall derive from this
    inline void reset() { zone_.reset(); zone_score_ = INT64_MAX; }
    ZoneScore &operator=(const ZoneScore &that) {
      if (this != &that) {
        zone_ = that.zone_;
        zone_score_ = that.zone_score_;
      }
      return *this;
    }
    bool operator==(const ZoneScore &that) const {
      return zone_ == that.zone_
             && zone_score_ == that.zone_score_;
    }
    TO_STRING_KV(K(zone_), K(zone_score_));
    common::ObZone zone_;
    // a smaller value means a higher priority
    int64_t zone_score_;
  };
  struct RegionScore
  {
    RegionScore() : region_(), region_score_(INT64_MAX) {}
    RegionScore(const common::ObRegion &region, const int64_t region_score)
      : region_(region), region_score_(region_score) {}
    ~RegionScore() {}
    inline void reset() { region_.reset(); region_score_ = INT64_MAX; }
    RegionScore &operator=(const RegionScore &that) {
      if (this != &that) {
        region_ = that.region_;
        region_score_ = that.region_score_;
      }
      return *this;
    }
    bool operator==(const RegionScore &that) const {
      return region_ == that.region_
             && region_score_ == that.region_score_;
    }
    TO_STRING_KV(K(region_), K(region_score_));
    common::ObRegion region_;
    // a smaller value means a higher priority
    int64_t region_score_;
  };
public:
  ObRawPrimaryZoneUtil(const rootserver::ObZoneManager &zone_mgr)
    : zone_mgr_(zone_mgr),
      current_score_(0) {}
  ~ObRawPrimaryZoneUtil() {} // no one shall derive
public:
  int build(
      const common::ObZone &primary,
      common::ObIArray<ZoneScore> &zone_score_array,
      common::ObIArray<RegionScore> &region_score_array);
private:
  int get_next_zone_score(
      const char *ptr,
      int64_t &cursor,
      const int64_t end,
      ZoneScore &zone_score);
  void jump_over_blanks(
      const char *ptr,
      int64_t &cursor,
      const int64_t end);
private:
  static const char BLANK_TOKEN = ' ';
  static const char COMMA_TOKEN = ',';
  static const char SEMI_COLON_TOKEN = ';';
private:
  const rootserver::ObZoneManager &zone_mgr_;
  int64_t current_score_;
// static member func
public:
  static int generate_high_priority_zone_array(
      const common::ObIArray<ZoneScore> &zone_score_array,
      common::ObIArray<common::ObZone> &high_priority_zone_array);
};
} // end namespace share
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_PRIMARY_ZONE_UTIL_H_
