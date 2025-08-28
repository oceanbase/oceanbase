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
#define USING_LOG_PREFIX STORAGE
#include "lib/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "lib/ob_date_unit_type.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/json/ob_json.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

namespace oceanbase
{
namespace storage
{

bool is_storage_cache_policy_default(const common::ObString &storage_cache_policy_str)
{
  return (storage_cache_policy_str.empty() ||
          0 == storage_cache_policy_str.case_compare(OB_DEFAULT_STORAGE_CACHE_POLICY_STR) ||
          0 == storage_cache_policy_str.case_compare(OB_NONE_STORAGE_CACHE_POLICY_STR));
}

bool is_part_storage_cache_policy_type_default(const ObStorageCachePolicyType &part_storage_cache_policy_type)
{
  return part_storage_cache_policy_type == ObStorageCachePolicyType::MAX_POLICY ||
         part_storage_cache_policy_type == ObStorageCachePolicyType::NONE_POLICY;
}

// get the storage cache policy type from the table storage cache policy string
int get_storage_cache_policy_type_from_str(const common::ObString &storage_cache_policy_str,
                                           ObStorageCachePolicyType &policy_type)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicy policy;
  if (storage_cache_policy_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy", KR(ret), K(storage_cache_policy_str));
  } else if (OB_FAIL(policy.load_from_string(storage_cache_policy_str))) {
    LOG_WARN("fail to load storage cache policy", KR(ret), K(storage_cache_policy_str));
  } else if (policy.is_global_policy()) {
    switch (policy.get_global_policy()) {
      case ObStorageCacheGlobalPolicy::HOT_POLICY: {
        policy_type = ObStorageCachePolicyType::HOT_POLICY;
        break;
      }
      case ObStorageCacheGlobalPolicy::AUTO_POLICY: {
        policy_type = ObStorageCachePolicyType::AUTO_POLICY;
        break;
      }
      // Only local index supports NONE POLICY
      case ObStorageCacheGlobalPolicy::NONE_POLICY: {
        policy_type = ObStorageCachePolicyType::NONE_POLICY;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid storage cache policy", KR(ret), K(storage_cache_policy_str));
      }
    }
  } else if (policy.is_time_policy()) {
    policy_type = ObStorageCachePolicyType::TIME_POLICY;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid storage cache policy", KR(ret), K(storage_cache_policy_str));
  }
  return ret;
}

// get the storage cache policy type from the partition storage cache policy string
int get_storage_cache_policy_type_from_part_str(const common::ObString &storage_cache_policy_part_str,
                                                ObStorageCachePolicyType &policy_type)
{
  int ret = OB_SUCCESS;
  if (storage_cache_policy_part_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy", KR(ret), K(storage_cache_policy_part_str));
  } else if (storage_cache_policy_part_str.case_compare("HOT") == 0) {
    policy_type = ObStorageCachePolicyType::HOT_POLICY;
  } else if (storage_cache_policy_part_str.case_compare("AUTO") == 0) {
    policy_type = ObStorageCachePolicyType::AUTO_POLICY;
  } else if (storage_cache_policy_part_str.case_compare("NONE") == 0) {
    policy_type = ObStorageCachePolicyType::NONE_POLICY;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid storage cache policy", KR(ret), K(storage_cache_policy_part_str));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "storage_cache_policy, storage_cache_policy for partition must be 'HOT', 'AUTO' or 'NONE'");
  }
  return ret;
}

bool is_valid_part_storage_cache_policy(const ObStorageCachePolicyType &part_policy)
{
  return !(storage::ObStorageCachePolicyType::MAX_POLICY == part_policy ||
          storage::ObStorageCachePolicyType::TIME_POLICY == part_policy);
}

bool is_hot_or_auto_policy(const ObStorageCachePolicyType &part_policy)
{
  return storage::ObStorageCachePolicyType::HOT_POLICY == part_policy ||
         storage::ObStorageCachePolicyType::AUTO_POLICY == part_policy;
}

//*************************ObStorageCacheGlobalPolicy*************************/

int ObStorageCacheGlobalPolicy::safely_get_str(const PolicyType &type, const char *&buf)
{
  int ret = OB_SUCCESS;
  if (!is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy type", KR(ret), K(type), K(lbt()));
  } else {
    buf = get_str(type);
  }
  return ret;
}

static const char *storage_cache_policy_type_strs[] = {"HOT", "AUTO", "TIME", "NONE"};

const char *ObStorageCacheGlobalPolicy::get_str(const PolicyType &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= PolicyType::MAX_POLICY) {
    str = "UNKNOWN";
  } else {
    str = storage_cache_policy_type_strs[type];
  }
  return str;
}


ObStorageCacheGlobalPolicy::PolicyType ObStorageCacheGlobalPolicy::get_type(const ObString &type_str)
{
  PolicyType type = PolicyType::MAX_POLICY;

  const int64_t COUNT = ARRAYSIZEOF(storage_cache_policy_type_strs);
  STATIC_ASSERT(static_cast<int64_t>(PolicyType::MAX_POLICY) == COUNT,
                "storage cache policy type count mismatch");
  for (int64_t i = 0; i < COUNT; ++i) {
    if (0 == type_str.case_compare(storage_cache_policy_type_strs[i])) {
      type = static_cast<PolicyType>(i);
      break;
    }
  }
  return type;
}

int ObStorageCacheGlobalPolicy::policy_type_to_status(
    const PolicyType &type, PolicyStatus &table_policy_status)
{
  int ret = OB_SUCCESS;
  table_policy_status = PolicyStatus::MAX_STATUS;
  if (OB_UNLIKELY(!is_valid(type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy type", KR(ret), K(type));
  } else {
    switch (type) {
      case PolicyType::AUTO_POLICY: {
        table_policy_status = PolicyStatus::AUTO;
        break;
      }
      case PolicyType::HOT_POLICY: {
        table_policy_status = PolicyStatus::HOT;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid storage cache policy type", K(ret), K(type), K(get_str(type)));
        break;
      }
    }
  }
  return ret;
}

//*************************ObBoundaryColumnUnit*************************
int ObBoundaryColumnUnit::safely_get_str(const ColumnUnitType &type, const char *&buf)
{
  int ret = OB_SUCCESS;
  if (!is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache boundary column unit type", KR(ret), K(type), K(lbt()));
  } else {
    buf = get_str(type);
  }
  return ret;
}

static const char *boundary_column_unit_strs[] = {"S", "MS", "US"};

const char *ObBoundaryColumnUnit::get_str(const ColumnUnitType &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= ColumnUnitType::MAX_UNIT) {
    str = "UNKNOWN";
  } else {
    str = boundary_column_unit_strs[type];
  }
  return str;
}

ObBoundaryColumnUnit::ColumnUnitType ObBoundaryColumnUnit::get_type(const ObString &type_str)
{
  ColumnUnitType type = ColumnUnitType::MAX_UNIT;

  const int64_t COUNT = ARRAYSIZEOF(boundary_column_unit_strs);
  STATIC_ASSERT(static_cast<int64_t>(ObBoundaryColumnUnit::MAX_UNIT) == COUNT,
                "boundary column unit type count mismatch");
  for (int64_t i = 0; i < COUNT; ++i) {
    if (0 == type_str.case_compare(boundary_column_unit_strs[i])) {
      type = static_cast<ColumnUnitType>(i);
      break;
    }
  }
  return type;
}
//*************************ObStorageCacheGranularity*************************
int ObStorageCacheGranularity::safely_get_str(const GranularityType &type, const char *&buf)
{
  int ret = OB_SUCCESS;
  if (!is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache granularity type", KR(ret), K(type), K(lbt()));
  } else {
    buf = get_str(type);
  }
  return ret;
}

static const char *granularity_type_strs[] = {"PARTITION", "ROW"};

const char *ObStorageCacheGranularity::get_str(const GranularityType &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= GranularityType::MAX_GRANULARITY) {
    str = "UNKNOWN";
  } else {
    str = granularity_type_strs[type];
  }
  return str;
}

ObStorageCacheGranularity::GranularityType ObStorageCacheGranularity::get_type(const ObString &type_str)
{
  GranularityType type = GranularityType::MAX_GRANULARITY;

  const int64_t COUNT = ARRAYSIZEOF(granularity_type_strs);
  STATIC_ASSERT(static_cast<int64_t>(GranularityType::MAX_GRANULARITY) == COUNT,
                "granularity type count mismatch");
  for (int64_t i = 0; i < COUNT; ++i) {
    if (0 == type_str.case_compare(granularity_type_strs[i])) {
      type = static_cast<GranularityType>(i);
      break;
    }
  }
  return type;
}

//*************************ObStorageCachePolicy*************************

ObStorageCachePolicy::ObStorageCachePolicy()
{
  reset();
}

void ObStorageCachePolicy::reset()
{
  global_policy_ = ObStorageCacheGlobalPolicy::MAX_POLICY;
  granularity_ = ObStorageCacheGranularity::PARTITION;
  column_unit_ = ObBoundaryColumnUnit::MAX_UNIT;
  hot_retention_interval_ = 0;
  hot_retention_unit_ = ObDateUnitType::DATE_UNIT_MAX;
  column_name_[0] = '\0';
  part_level_ = share::schema::PARTITION_LEVEL_MAX;
}

bool ObStorageCachePolicy::is_time_policy() const
{
  return strlen(column_name_) > 0
         && hot_retention_interval_ > 0
         && ObDateUnitType::DATE_UNIT_MAX != hot_retention_unit_;
}

// check the validity of the table storage cache policy
int ObStorageCachePolicy::check_table_cache_policy() const
{
  int ret = OB_SUCCESS;
  if (is_global_policy() && is_time_policy()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("global policy and time policy cannot coexist", K(ret));
  } else if (granularity_ == ObStorageCacheGranularity::MAX_GRANULARITY) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy", K(ret), K(global_policy_));
  } else if (granularity_ == ObStorageCacheGranularity::ROW) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row granularity not supported yet", K(ret));
  } else if (!(is_global_policy() || is_time_policy())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy", K(ret), K(*this));
  }
  return ret;
}

int ObStorageCachePolicy::to_json_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  pos = 0;
  J_OBJ_START();
  if (is_global_policy()) {
    databuff_print_kv(buf, buf_len, pos, "\"GLOBAL\"", ObStorageCacheGlobalPolicy::get_str(global_policy_));
    // J_COMMA();
    // databuff_print_kv(buf, buf_len, pos, "\"GRANULARITY\"", ObStorageCacheGranularity::get_str(granularity_));
  } else if (is_time_policy()) {
    databuff_print_kv(buf, buf_len, pos, "\"COLUMN_NAME\"", column_name_);
    J_COMMA();
    // databuff_print_kv(buf, buf_len, pos, "\"GRANULARITY\"", ObStorageCacheGranularity::get_str(granularity_));
    // J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "\"HOT_RETENTION_INTERVAL\"", hot_retention_interval_);
    J_COMMA();
    databuff_print_kv(buf, buf_len, pos, "\"HOT_RETENTION_UNIT\"", ob_date_unit_type_str_upper(hot_retention_unit_));
    if (ObBoundaryColumnUnit::is_valid(column_unit_)) {
      J_COMMA();
      databuff_print_kv(buf, buf_len, pos, "\"COLUMN_UNIT\"", ObBoundaryColumnUnit::get_str(column_unit_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy string", K(ret), KCSTRING(buf), K(buf_len), K(pos));
  }
  J_OBJ_END();
  return ret;
}

int ObStorageCachePolicy::load_from_string(const ObString &str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(OB_STORAGE_CACHE_POLICY_ALLOCATOR);
  json::Value *root = nullptr;
  json::Parser parser;

  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage cache policy string is null", K(ret));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_WARN("parser init failed", K(ret));
  } else if (OB_FAIL(parser.parse(str.ptr(), str.length(), root))) {
    LOG_WARN("parse json failed", K(ret), K(str));
  } else if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no root value", K(ret));
  } else if (json::JT_OBJECT != root->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error json format", K(ret), K(root->get_type()));
  } else {
    DLIST_FOREACH_X(it, root->get_object(), OB_SUCC(ret))
    {
      if (OB_ISNULL(it->name_) || OB_ISNULL(it->value_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error json format", K(ret));
      } else {
        // use set_xxx to check the validity of the policy attribute
        if (it->name_.case_compare("GLOBAL") == 0) {
          set_global_policy(ObStorageCacheGlobalPolicy::get_type(it->value_->get_string()));
        } else if (it->name_.case_compare("GRANULARITY") == 0) {
          set_granularity(ObStorageCacheGranularity::get_type(it->value_->get_string()));
        } else if (it->name_.case_compare("COLUMN_NAME") == 0) {
          set_column_name(it->value_->get_string().ptr(), it->value_->get_string().length());
        } else if (it->name_.case_compare("COLUMN_UNIT") == 0) {
          set_column_unit(ObBoundaryColumnUnit::get_type(it->value_->get_string()));
        } else if (it->name_.case_compare("HOT_RETENTION_INTERVAL") == 0) {
          set_hot_retention_interval(it->value_->get_number());
        } else if (it->name_.case_compare("HOT_RETENTION_UNIT") == 0) {
          ObDateUnitType unit = get_date_unit_type(it->value_->get_string());
          if (ObDateUnitType::DATE_UNIT_MAX == unit) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid hot retention unit", K(ret), K(it->value_->get_string()));
          } else {
            set_hot_retention_unit(unit);
          }
        }
      }
    }
  }
  if (OB_FAIL(check_table_cache_policy())) {
    LOG_WARN("invalid storage cache policy", K(ret));
  }
  return ret;
}

int ObStorageCachePolicy::set_global_policy(const ObStorageCacheGlobalPolicy::PolicyType policy)
{
  int ret = OB_SUCCESS;
  if (ObStorageCacheGlobalPolicy::MAX_POLICY == policy) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(policy));
  } else {
    global_policy_ = policy;
  }
  return ret;
}

int ObStorageCachePolicy::set_column_name(const char *column_name, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_name) || len == 0 || len >= OB_MAX_COLUMN_NAME_LENGTH -1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(column_name), K(len));
  } else {
    MEMCPY(column_name_, column_name, len);
    column_name_[len] = '\0';
  }
  return OB_SUCCESS;
}

int ObStorageCachePolicy::set_column_unit(const ObBoundaryColumnUnit::ColumnUnitType unit)
{
  int ret = OB_SUCCESS;
  if (ObBoundaryColumnUnit::MAX_UNIT == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit));
  } else {
    column_unit_ = unit;
  }
  return ret;
}

int ObStorageCachePolicy::set_granularity(const ObStorageCacheGranularity::GranularityType granularity)
{
  int ret = OB_SUCCESS;
  if (ObStorageCacheGranularity::MAX_GRANULARITY == granularity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(granularity));
  } else if (ObStorageCacheGranularity::ROW == granularity) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row granularity not supported yet", K(ret));
  } else {
    granularity_ = granularity;
  }
  return ret;
}

int ObStorageCachePolicy::set_hot_retention_interval(const uint64_t interval)
{
  int ret = OB_SUCCESS;
  if (interval <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(interval));
  } else {
    hot_retention_interval_ = interval;
  }
  return ret;
}

ObDateUnitType ObStorageCachePolicy::get_date_unit_type(const ObString &unit_str) const
{
  ObDateUnitType unit = ObDateUnitType::DATE_UNIT_MAX;
  for (int i = 0; i < DATE_UNIT_MAX; ++i) {
    if (unit_str.case_compare(ob_date_unit_type_str(static_cast<ObDateUnitType>(i))) == 0) {
      unit = static_cast<ObDateUnitType>(i);
      break;
    }
  }
  return unit;
}

int ObStorageCachePolicy::set_hot_retention_unit(const ObDateUnitType unit)
{
  int ret = OB_SUCCESS;
  if (ObDateUnitType::DATE_UNIT_MAX == unit) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(unit));
  } else if (!(ObDateUnitType::DATE_UNIT_YEAR == unit
               || ObDateUnitType::DATE_UNIT_MONTH == unit
               || ObDateUnitType::DATE_UNIT_WEEK == unit
               || ObDateUnitType::DATE_UNIT_DAY == unit
               || ObDateUnitType::DATE_UNIT_HOUR == unit
               || ObDateUnitType::DATE_UNIT_MINUTE == unit
               || ObDateUnitType::DATE_UNIT_SECOND == unit)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hot retention unit only support year/month/week/day/hour/minute/second", K(ret), K(unit));
  } else {
    hot_retention_unit_ = unit;
  }
  return ret;
}

int ObStorageCachePolicy::set_part_level(const int32_t level)
{
  int ret = OB_SUCCESS;
  if (share::schema::PARTITION_LEVEL_MAX == level) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(level));
  } else {
    part_level_ = level;
  }
  return ret;
}

// calculate hot retention time from hot retention unit to usecs
int ObStorageCachePolicy::convert_hot_retention_unit(const ObDateUnitType unit, int64_t &hot_rentention_time) const
{
  int ret = OB_SUCCESS;
  switch (unit) {
    case ObDateUnitType::DATE_UNIT_YEAR: {
      // A YEAR is considered to be 365 days
      hot_rentention_time = 365 * USECS_PER_DAY;
      break;
    }
    case ObDateUnitType::DATE_UNIT_MONTH: {
      // A MONTH is considered to be 30 days
      hot_rentention_time = 30 * USECS_PER_DAY;
      break;
    }
    case ObDateUnitType::DATE_UNIT_WEEK: {
      hot_rentention_time = 7 * USECS_PER_DAY;
      break;
    }
    case ObDateUnitType::DATE_UNIT_DAY: {
      hot_rentention_time = USECS_PER_DAY;
      break;
    }
    case ObDateUnitType::DATE_UNIT_HOUR: {
      hot_rentention_time = 60 * USECS_PER_MIN;
      break;
    }
    case ObDateUnitType::DATE_UNIT_MINUTE: {
      hot_rentention_time = USECS_PER_MIN;
      break;
    }
    case ObDateUnitType::DATE_UNIT_SECOND: {
      hot_rentention_time = USECS_PER_SEC;
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(unit));
      break;
    }
  }
  return ret;
}

int ObStorageCachePolicy::cal_hot_rentention_time(int64_t &hot_rentention_time) const
{
  int ret = OB_SUCCESS;
  if (is_global_policy()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global policy or none policy does not need to calculate hot retention time", K(ret));
  } else if (ObStorageCacheGranularity::ROW == granularity_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("row granularity not supported yet", K(ret));
  } else if (OB_FAIL(convert_hot_retention_unit(hot_retention_unit_, hot_rentention_time))) {
    LOG_WARN("fail to convert column unit to seconds", K(ret), K(hot_retention_unit_));
  } else {
    hot_rentention_time *= hot_retention_interval_;
  }
  return ret;
}

bool ObStorageCachePolicy::operator ==(const ObStorageCachePolicy &policy) const
{
  bool is_equal = false;
  if (is_global_policy()) {
    is_equal = global_policy_ == policy.get_global_policy();
  } else {
    is_equal = granularity_ == policy.get_granularity()
               && 0 == STRCMP(column_name_, policy.get_column_name())
               && column_unit_ == policy.get_column_unit()
               && hot_retention_interval_ == policy.get_hot_retention_interval()
               && hot_retention_unit_ == policy.get_hot_retention_unit();
  }
  return is_equal;
}

bool ObStorageCachePolicy::operator !=(const ObStorageCachePolicy &policy) const
{
  return !(*this == policy);
}

//*************************ObStorageCachePolicyStatus*************************/
int ObStorageCachePolicyStatus::safely_get_str(const PolicyStatus &type, const char *&buf)
{
  int ret = OB_SUCCESS;
  if (!is_valid(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid storage cache policy status type", KR(ret), K(type), K(lbt()));
  } else {
    buf = get_str(type);
  }
  return ret;
}

static const char *storage_cache_policy_status_type_strs[] = {"HOT", "AUTO", "NONE"};

const char *ObStorageCachePolicyStatus::get_str(const PolicyStatus &type)
{
  const char *str = nullptr;
  if (type < 0 || type >= PolicyStatus::MAX_STATUS) {
    str = "UNKNOWN";
  } else {
    str = storage_cache_policy_status_type_strs[type];
  }
  return str;
}


} // namespace storage
} // namespace oceanbase