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

#ifndef OCEANBASE_ROOTSERVER_OB_LOCALITY_UTIL_H_
#define OCEANBASE_ROOTSERVER_OB_LOCALITY_UTIL_H_

#include "share/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "share/ob_replica_info.h"
#include "share/schema/ob_schema_struct.h"


namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
namespace schema
{
struct ObZoneRegion;
class ObSchemaGetterGuard;
class ObLocality;
class ObSimpleTableSchemaV2;
class ObTablegroupSchema;
class ObTenantSchema;
class ObDatabaseSchema;
}
}
namespace rootserver
{
/* the replica attributes(include type, number and memstore_percent) parsed from
 * locality string is stored using a hash map. some agreements and restrictions are
 * as follows:
 * 1 this is at most one full replica on a single zone
 * 2 if the input locality string is empty, then a full replica is deployed on a read/write zone
 *   and a encrypted replica is deployed on a encrypted zone
 * 3 ObLocalityDistribution class is used in the following two situations:
 *   a ddl operations like create tenant/tablegroup/table
 *   b flush table/tablegroup/table schema locally
 */
class ObLocalityDistribution
{
public:
  ObLocalityDistribution()
      : zone_set_replica_dist_array_(),
        is_inited_(false),
        can_output_normalized_locality_(false)
  { reset(); }
  virtual ~ObLocalityDistribution() {}
public:
  int init();
  void reset();
  int parse_locality(
      const common::ObString &input_string,
      const common::ObIArray<common::ObString> &zone_list,
      const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list = NULL);
  int parse_locality(
      const common::ObString &input_string,
      const common::ObIArray<common::ObZone> &zone_list,
      const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list = NULL);
  int output_normalized_locality(
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  int get_zone_replica_attr_array(
      common::ObIArray<share::ObZoneReplicaAttrSet> &zone_replica_num_array);
  int get_zone_replica_num(
      const common::ObZone &zone,
      share::ObReplicaNumSet &replica_num_set);
  int get_zone_replica_num(
      const common::ObZone &zone,
      share::ObZoneReplicaAttrSet &zone_replica_num_set);
public:
  static const int64_t ALL_SERVER_CNT = INT64_MAX;
private:
  typedef int32_t ReplicaTypeID;
  static const int32_t FULL_REPLICA = 0;
  static const int32_t LOGONLY_REPLICA = 1;
  static const int32_t READONLY_REPLICA = 2;
  static const int32_t ENCRYPTION_LOGONLY_REPLICA = 3;
  static const int32_t REPLICA_TYPE_MAX = 4;
private:
  static const int64_t MAX_BUCKET_NUM = 2 * common::MAX_ZONE_NUM;
  static const int64_t INVALID_CURSOR = -1;
  static const int64_t INVALID_COUNT = -1;
  // full replica
  static const char *const FULL_REPLICA_STR;
  static const char *const F_REPLICA_STR;
  // logonly replica
  static const char *const LOGONLY_REPLICA_STR;
  static const char *const L_REPLICA_STR;
  // readonly replica
  static const char *const READONLY_REPLICA_STR;
  static const char *const R_REPLICA_STR;
  // encryption logonly replica
  static const char *const ENCRYPTION_LOGONLY_REPLICA_STR;
  static const char *const E_REPLICA_STR;
  // others
  static const common::ObZone EVERY_ZONE;
  static const char *const ALL_SERVER_STR;
  static const char *const MEMSTORE_PERCENT_STR;

  static const int64_t MAX_MEMSTORE_PERCENT = 100;
private:
  // use to save the replica distribution for zone set
  // zone set is accumulated using a zone array zone_set_
  class ZoneSetReplicaDist
  {
  public:
    bool operator<(const ZoneSetReplicaDist &that);
  public:
    ZoneSetReplicaDist(); 
    ~ZoneSetReplicaDist() {} // no one shall derive from this
  public:
    typedef common::ObArray<share::ReplicaAttr> ReplicaAttrArray;
    int assign(const ZoneSetReplicaDist &that);
    void reset();
    
    static int get_check_zone_type(
        const common::ObZone &zone,
        const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
        share::schema::ObZoneRegion::CheckZoneType &check_zone_type); 

    int check_encryption_zone_cond(
        const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
        bool &is_valid) const;

    int check_valid_replica_dist(
        const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
        bool &is_valid) const;
    
    bool has_encryption_logonly() const;

    bool has_non_encryption_logonly() const;

    inline int64_t get_full_replica_num() const {
      int64_t num = (all_replica_attr_array_[FULL_REPLICA].count() > 0
                     ? all_replica_attr_array_[FULL_REPLICA].at(0).num_
                     : 0);
      return num;
    }
    inline int64_t get_logonly_replica_num() const {
      int64_t num = (all_replica_attr_array_[LOGONLY_REPLICA].count() > 0
                     ? all_replica_attr_array_[LOGONLY_REPLICA].at(0).num_
                     : 0);
      return num;
    }
    inline int64_t get_readonly_replica_num() const {
      int64_t num = (all_replica_attr_array_[READONLY_REPLICA].count() > 0
                     ? all_replica_attr_array_[READONLY_REPLICA].at(0).num_
                     : 0);
      return num;
    }
    inline int64_t get_encryption_logonly_replica_num() const {
      int64_t num = (all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA].count() > 0
                     ? all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA].at(0).num_
                     : 0);
      return num;
    }
    inline const ReplicaAttrArray &get_full_replica_attr() const {
      return all_replica_attr_array_[FULL_REPLICA];
    }
    inline const ReplicaAttrArray &get_logonly_replica_attr() const {
      return all_replica_attr_array_[LOGONLY_REPLICA];
    }
    inline const ReplicaAttrArray &get_readonly_replica_attr() const {
      return all_replica_attr_array_[READONLY_REPLICA];
    }
    inline const ReplicaAttrArray &get_encryption_logonly_replica_attr() const {
      return all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA];
    }
    inline const common::ObIArray<common::ObZone> &get_zone_set() const { return zone_set_; }
  public:
    int format_to_locality_str(char *buf, int64_t buf_len, int64_t &pos) const;
    bool need_format_to_locality_str() const;
    int try_set_specific_replica_dist(
        const ReplicaTypeID replica_type,
        const int64_t num,
        const int64_t memstore_percent);
    int append_zone(const common::ObZone &this_zone);
    int replace_zone_set(const common::ObIArray<common::ObZone> &this_zone_set);
    TO_STRING_KV("zone_set", zone_set_,
                 "full_replica_attr", all_replica_attr_array_[FULL_REPLICA],
                 "logonly_replica_attr", all_replica_attr_array_[LOGONLY_REPLICA],
                 "readonly_replica_attr", all_replica_attr_array_[READONLY_REPLICA],
                 "encryption_logonly_replica_attr", all_replica_attr_array_[ENCRYPTION_LOGONLY_REPLICA]);
  private:
    bool specific_replica_need_format(
        const ReplicaTypeID replica_type) const;
    int replica_type_to_str(
        const ReplicaTypeID replica_type,
        const char *&replica_type_str) const;
    int format_specific_replica(
        const ReplicaTypeID replica_type,
        char *buf,
        int64_t buf_len,
        int64_t &pos,
        bool &start_format) const;
    int format_zone_set(char *buf, int64_t buf_len, int64_t &pos) const;
  private:
    // zone_set_ is a array with the maximum elements of seven
    common::ObArray<common::ObZone> zone_set_;
    ReplicaAttrArray all_replica_attr_array_[REPLICA_TYPE_MAX];
  };
  class RawLocalityIter
  {
  // we call "F{2},L@zone1" a sub_locality of the locality "F{2},L@zone1, F{1},L{1}@zone2"
  // and we call 'F{2}' a replica_arrangement of the sub_locality
  public:
    RawLocalityIter(const common::ObString &locality)
        : locality_(locality),
          locality_str_(nullptr),
          locality_str_len_(0),
          pos_(0),
          output_cnt_(0),
          alloc_()
    {}
    ~RawLocalityIter() {} 
  public:
    int init(); 
    int get_next_zone_set_replica_dist(
        ZoneSetReplicaDist &zone_replica_dist);
  private:
    static const char AT_TOKEN = '@';
    static const char BLANK_TOKEN = ' ';
    static const char LEFT_BRACE_TOKEN = '{';
    static const char RIGHT_BRACE_TOKEN = '}';
    static const char COMMA_TOKEN = ',';
    static const char PLUS_TOKEN = '+';
    static const char COLON_TOKEN = ':';
    static const char LEFT_BRACKET_TOKEN = '[';
    static const char RIGHT_BRACKET_TOKEN = ']';
  private:
    void jump_over_blanks(int64_t &cursor, int64_t end);
    void inc_cursor(int64_t &cursor);
    int check_iter_end(int64_t pre_pos);
    bool find_at_token_position(int64_t &pre_pos, int64_t &token_pos);
    int get_replica_arrangements(
        int64_t cursor,
        int64_t end,
        ZoneSetReplicaDist &zone_replica_dist);
    int get_next_replica_arrangement(
        int64_t &cursor,
        int64_t end,
        ReplicaTypeID &replica_type,
        int64_t &replica_num,
        int64_t &memstore_percent);
    int get_replica_type(
        int64_t &cursor,
        int64_t end,
        ReplicaTypeID &replica_type);
    int get_replica_attribute(
        const ReplicaTypeID replica_type,
        int64_t &cursor,
        int64_t end,
        int64_t &replica_num,
        int64_t &memstore_percent);
    int get_replica_attribute_recursively(
        const ReplicaTypeID replica_type,
        bool &replica_num_got,
        bool &memstore_percent_got,
        int64_t &cursor,
        int64_t end,
        int64_t &replica_num,
        int64_t &memstore_percent);
    int check_replica_attribute_afterwards_syntax(
        int64_t &cursor,
        int64_t end);
    int get_pure_memstore_percent(
        int64_t &cursor,
        int64_t end,
        int64_t &memstore_percent);
    int get_pure_replica_num(
        int64_t &cursor,
        int64_t end,
        int64_t &replica_num);
    int special_replica_num_quantity_token_to_long(
        int64_t &cursor,
        int64_t end,
        int64_t &num);
    int general_nonzero_quantity_string_to_long(
        int64_t &cursor,
        int64_t end,
        int64_t &num);
    int check_right_brace_and_afterwards_syntax(
        int64_t &cursor,
        int64_t end);
    int get_zone_set_dist(
        int64_t cursor,
        const int64_t end,
        ZoneSetReplicaDist &zone_set_replica_dist);
    int get_zone_set_with_bracket(
        int64_t &cursor,
        const int64_t end,
        ZoneSetReplicaDist &zone_set_replica_dist);
    int get_zone_set_without_bracket(
        int64_t &cursor,
        const int64_t end,
        ZoneSetReplicaDist &zone_set_replica_dist);
    int get_single_zone_name(
        int64_t &cursor,
        const int64_t end,
        ZoneSetReplicaDist &zone_set_replica_dist);
  private:
    const common::ObString &locality_;
    char *locality_str_;
    int64_t locality_str_len_;
    int64_t pos_;
    int64_t output_cnt_;
    ObArenaAllocator alloc_;
  };
private:
  int parse_multiple_zone(
      const common::ObString &locality,
      const common::ObIArray<common::ObZone> &zone_list,
      const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list = NULL);
  int check_zone_set_legal(
      const common::ObIArray<common::ObZone> &zone_set,
      const common::ObIArray<common::ObZone> &zone_list,
      bool &is_legal);
  int check_zone_name_duplicate(
      const ZoneSetReplicaDist &zone_set_replica_dist,
      const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list,
      bool &does_zone_name_duplicate);
  int append_zone_set_replica_dist(
      const ZoneSetReplicaDist &zone_set_replica_dist,
      const common::ObIArray<common::ObZone> &zone_list);
  int parse_for_empty_locality(
      const common::ObIArray<common::ObZone> &zone_list,
      const common::ObIArray<share::schema::ObZoneRegion> *zone_region_list);
  int convert_zone_list(
      const common::ObIArray<common::ObString> &zone_list_input,
      common::ObIArray<common::ObZone> &zone_list_output);
private:
  common::ObArray<ZoneSetReplicaDist> zone_set_replica_dist_array_;
  bool is_inited_;
  bool can_output_normalized_locality_;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif
