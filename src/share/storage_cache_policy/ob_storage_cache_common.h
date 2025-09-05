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

#ifndef OCEANBASE_STORAGE_CAHCE_COMMON_DEFINE_H_
#define OCEANBASE_STORAGE_CAHCE_COMMON_DEFINE_H_
#include "lib/string/ob_string.h"
#include "lib/ob_date_unit_type.h"
#include "lib/container/ob_iarray.h"
#include "lib/timezone/ob_timezone_info.h"
#include "share/config/ob_server_config.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObPartition;
class ObSubPartition;
}
}
namespace storage
{
const char *const OB_STORAGE_CACHE_POLICY_ALLOCATOR = "ObStorageCachePolicyAllocator";
const char *const OB_DEFAULT_STORAGE_CACHE_POLICY_STR = "{\"GLOBAL\":\"AUTO\"}";
const char *const OB_NONE_STORAGE_CACHE_POLICY_STR = "{\"GLOBAL\":\"NONE\"}";
const char *const OB_DEFAULT_PART_STORAGE_CACHE_POLICY_STR = "NONE";
const int64_t OB_INVALID_TG_ID = 0;
const int32_t OB_INVALID_PARTITION_LEVEL = -1;
// storage_cache_policy format:
// {GLOBAL: HOT_POLICY|AUTO_POLICY, GRANULARITY: PARTITION|ROW} or
// {GRANULARITY: PARTITION|ROW, BOUNDARY_COLUMN: column_name,
// BOUNDARY_COLUMN_UNIT: SECOND|MILLISECOND|MICROSECOND|NANOSECOND,
// HOT_RETENTION_INTERVAL: interval_num, HOT_RETENTION_UNIT: HOUR|DAY|WEEK|MONTH|YEAR}
static constexpr int64_t OB_MAX_STORAGE_CACHE_POLICY_LENGTH = 229;

struct ObStorageCachePolicyStatus
{
  enum PolicyStatus : uint8_t
  {
    HOT = 0,
    AUTO = 1,
    NONE = 2,
    MAX_STATUS
  };
  static int safely_get_str(const PolicyStatus &type, const char *&buf);
  static const char *get_str(const PolicyStatus &type);
  static PolicyStatus get_type(const ObString &type_str);
  static OB_INLINE bool is_valid(const PolicyStatus &type)
  {
    return type >= 0 && type < PolicyStatus::MAX_STATUS;
  }
};

typedef ObStorageCachePolicyStatus::PolicyStatus PolicyStatus;
class ObStorageCachePolicy;
int set_default_storage_cache_policy_for_table(const int64_t tenant_id, share::schema::ObTableSchema &table);
int get_default_storage_cache_policy_for_create_table(const int64_t tenant_id, ObStorageCachePolicy &storage_cache_policy);
bool is_storage_cache_policy_default(const common::ObString &storage_cache_policy_str);
bool is_storage_cache_policy_auto(const common::ObString &storage_cache_policy_str);
bool is_storage_cache_policy_none(const common::ObString &storage_cache_policy_str);
struct ObStorageCacheGlobalPolicy
{
  enum PolicyType : uint8_t
  {
    HOT_POLICY = 0,
    AUTO_POLICY = 1,
    TIME_POLICY = 2,
    NONE_POLICY = 3,
    MAX_POLICY
  };
  static int safely_get_str(const PolicyType &type, const char *&buf);
  static const char *get_str(const PolicyType &type);
  static PolicyType get_type(const ObString &type_str);
  static int policy_type_to_status(const PolicyType &type, PolicyStatus &table_policy_status);
  static OB_INLINE bool is_valid(const PolicyType &type)
  {
    return type >= 0 && type < PolicyType::MAX_POLICY;
  }
};
typedef ObStorageCacheGlobalPolicy::PolicyType ObStorageCachePolicyType;

bool is_part_storage_cache_policy_type_default(const ObStorageCachePolicyType &part_storage_cache_policy_type);

int get_storage_cache_policy_type_from_str(const common::ObString &storage_cache_policy_str,
                                           ObStorageCachePolicyType &policy_type);

int get_storage_cache_policy_type_from_part_str(const common::ObString &storage_cache_policy_part_str,
                                                ObStorageCachePolicyType &policy_type);
bool is_valid_part_storage_cache_policy(const ObStorageCachePolicyType &policy_type);
bool is_hot_or_auto_policy(const ObStorageCachePolicyType &part_policy);

struct ObBoundaryColumnUnit
{
  enum ColumnUnitType : uint8_t
  {
    SECOND = 0,
    MILLISECOND = 1,
    MICROSECOND = 2,
    MAX_UNIT
  };
  static int safely_get_str(const ColumnUnitType &type, const char *&buf);
  static const char *get_str(const ColumnUnitType &type);
  static ColumnUnitType get_type(const ObString &type_str);
  static OB_INLINE bool is_valid(const ColumnUnitType &type)
  {
    return type >= 0 && type < ColumnUnitType::MAX_UNIT;
  }
};

struct ObStorageCacheGranularity
{
  enum GranularityType : uint8_t
  {
    PARTITION = 0,
    ROW = 1,
    MAX_GRANULARITY
  };
  static int safely_get_str(const GranularityType &type, const char *&buf);
  static const char *get_str(const GranularityType &type);
  static GranularityType get_type(const ObString &type_str);
  static OB_INLINE bool is_valid(const GranularityType &type)
  {
    return type >= 0 && type < GranularityType::MAX_GRANULARITY;
  }
};

class ObStorageCachePolicy
{
public:
  ObStorageCachePolicy();
  virtual ~ObStorageCachePolicy() {
    reset();
  };
  void reset();
  int to_json_string(char *buf, const int64_t buf_len, int64_t &pos) const;
  int load_from_string(const common::ObString &str);
  bool is_none_policy() const { return global_policy_ == ObStorageCacheGlobalPolicy::NONE_POLICY; };
  bool is_hot_policy() const { return global_policy_ == ObStorageCacheGlobalPolicy::HOT_POLICY; };
  bool is_auto_policy() const { return global_policy_ == ObStorageCacheGlobalPolicy::AUTO_POLICY; };
  bool is_global_policy() const
  { return global_policy_ == ObStorageCacheGlobalPolicy::HOT_POLICY
        || global_policy_ == ObStorageCacheGlobalPolicy::AUTO_POLICY
        || global_policy_ == ObStorageCacheGlobalPolicy::NONE_POLICY; };
  bool is_time_policy() const;
  int set_global_policy (const ObStorageCacheGlobalPolicy::PolicyType policy);
  int set_granularity (const ObStorageCacheGranularity::GranularityType granularity);
  int set_column_name (const char *column_name, const int64_t len);
  int set_column_unit (const ObBoundaryColumnUnit::ColumnUnitType unit);
  int set_hot_retention_interval (const uint64_t interval);
  int set_hot_retention_unit (const ObDateUnitType unit);
  int set_hot_retention_time (const int64_t time);
  int set_part_level (const int32_t level);
  ObStorageCacheGlobalPolicy::PolicyType get_global_policy() const { return global_policy_; };
  ObStorageCacheGranularity::GranularityType get_granularity() const { return granularity_; };
  const char *get_column_name() const { return column_name_; };
  ObBoundaryColumnUnit::ColumnUnitType get_column_unit() const { return column_unit_; };
  ObDateUnitType get_hot_retention_unit() const { return hot_retention_unit_; };
  uint64_t get_hot_retention_interval() const { return hot_retention_interval_; };
  ObDateUnitType get_date_unit_type(const ObString &unit_str) const;
  int32_t get_part_level() const { return part_level_; };
  int convert_hot_retention_unit(const ObDateUnitType unit, int64_t &hot_rentention_time) const;
  int convert_bound_to_column_unit(const ObString &partition_bound_val, int64_t &bound_time) const;
  int check_boundary_column_with_partition(share::schema::ObPartition &partition) const;
  int check_boundary_column_with_subpartition(share::schema::ObSubPartition &subpartition) const;
  int cal_hot_rentention_time(int64_t &hot_rentention_time) const;
  int check_table_cache_policy() const;
  bool is_valid_date_unit_type() const
  {
    return hot_retention_unit_ >= 0 && hot_retention_unit_ < ObDateUnitType::DATE_UNIT_MAX;
  };
  bool operator ==(const ObStorageCachePolicy &policy) const;
  bool operator !=(const ObStorageCachePolicy &policy) const;
  TO_STRING_KV(K_(global_policy), K_(granularity), KCSTRING_(column_name), K_(column_unit), K_(hot_retention_interval), K_(hot_retention_unit), K_(part_level));

private:
  ObStorageCacheGlobalPolicy::PolicyType global_policy_;
  ObStorageCacheGranularity::GranularityType granularity_;
  char column_name_[OB_MAX_COLUMN_NAME_LENGTH + 1];
  ObBoundaryColumnUnit::ColumnUnitType column_unit_;
  uint64_t hot_retention_interval_;
  ObDateUnitType hot_retention_unit_;
  int32_t part_level_;
};

class ObSCPTraceIdGuard
{
public:
  ObSCPTraceIdGuard() : old_trace_id_(), is_old_trace_saved_(false)
  {
    if (nullptr != common::ObCurTraceId::get_trace_id()) {
      old_trace_id_ = *common::ObCurTraceId::get_trace_id();
      is_old_trace_saved_ = true;
    }

    common::ObCurTraceId::TraceId new_trace_id;
    new_trace_id.init(GCONF.self_addr_);
    common::ObCurTraceId::set(new_trace_id);
  }

  ~ObSCPTraceIdGuard()
  {
    if (is_old_trace_saved_) {
      common::ObCurTraceId::set(old_trace_id_);
    }
  }

private:
  common::ObCurTraceId::TraceId old_trace_id_;
  bool is_old_trace_saved_;
};

}
}
#endif