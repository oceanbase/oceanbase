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

#ifndef OCEANBASE_SHARE_DYNAMIC_PARTITION_UTIL_H_
#define OCEANBASE_SHARE_DYNAMIC_PARTITION_UTIL_H_

#include "common/object/ob_obj_type.h"
#include "lib/ob_date_unit_type.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{

static const char* const DEFAULT_DYNAMIC_PARTITION_TIME_ZONE_STR = "DEFAULT";
static const char* const DEFAULT_DYNAMIC_PARTITION_BIGINT_PRECISION_STR = "NONE";

class ObDynamicPartitionManager
{
private:
  struct ObDynamicPartitionPolicy
  {
    ObDynamicPartitionPolicy() :
      enable_(true),
      time_unit_(ObDateUnitType::DATE_UNIT_MAX),
      precreate_time_num_(-1),
      precreate_time_unit_(ObDateUnitType::DATE_UNIT_MAX),
      expire_time_num_(-1),
      expire_time_unit_(ObDateUnitType::DATE_UNIT_MAX),
      time_zone_(DEFAULT_DYNAMIC_PARTITION_TIME_ZONE_STR),
      bigint_precision_(DEFAULT_DYNAMIC_PARTITION_BIGINT_PRECISION_STR) {}

    void reset() {
      enable_ = true;
      time_unit_ = ObDateUnitType::DATE_UNIT_MAX;
      precreate_time_num_ = -1;
      precreate_time_unit_ = ObDateUnitType::DATE_UNIT_MAX;
      expire_time_num_ = -1;
      expire_time_unit_ = ObDateUnitType::DATE_UNIT_MAX;
      time_zone_ = DEFAULT_DYNAMIC_PARTITION_TIME_ZONE_STR;
      bigint_precision_ = DEFAULT_DYNAMIC_PARTITION_BIGINT_PRECISION_STR;
    }
    TO_STRING_KV(K_(enable),
                 "time_unit", ob_date_unit_type_str_upper(time_unit_),
                 K_(precreate_time_num),
                 "precreate_time_unit", ob_date_unit_type_str_upper(precreate_time_unit_),
                 K_(expire_time_num),
                 "expire_time_unit", ob_date_unit_type_str_upper(expire_time_unit_),
                 K_(time_zone),
                 K_(bigint_precision));

    bool enable_;
    ObDateUnitType time_unit_;
    int64_t precreate_time_num_;
    ObDateUnitType precreate_time_unit_;
    int64_t expire_time_num_;
    ObDateUnitType expire_time_unit_;
    ObString time_zone_;
    ObString bigint_precision_;
  };

public:
  ObDynamicPartitionManager()
    : inited_(false),
      tenant_id_(OB_INVALID_ID),
      table_schema_(NULL),
      session_(NULL),
      policy_(),
      is_oracle_mode_(false) {}

  int init(const schema::ObTableSchema *table_schema, sql::ObSQLSessionInfo *session);
  int execute(const ObString &specified_precreate_time, const ObIArray<ObString> &specified_time_unit_array, bool &skipped);
private:
  int check_inner_stat_();
  int add_dynamic_partition_(const ObString &specified_precreate_time);
  int drop_dynamic_partition_();
  int write_ddl_(const ObSqlString &ddl);
  int build_add_partition_sql_(const ObString &specified_precreate_time, ObSqlString &sql);
  int build_drop_partition_sql_(ObSqlString &sql);
  int build_precreate_partition_definition_list_(const ObString &specified_precreate_time, ObSqlString &part_def_list);
  int build_expired_partition_name_list_(ObSqlString &part_name_list);
  int build_part_name_(const int64_t timestamp, ObSqlString &str);
  int build_high_bound_val_(const int64_t timestamp, ObSqlString &str);
  int check_partition_name_exists_(const ObString &part_name, bool &exists);
  int get_start_precreate_timestamp_(int64_t &timestamp);
  int get_target_precreate_timestamp_(const ObString &specified_precreate_time, int64_t &timestamp);
  int get_table_time_zone_wrap_(ObTimeZoneInfoWrap &tz_info_wrap);
  int fetch_timestamp_from_part_key_(const schema::ObPartition &part, int64_t &timestamp);
  int get_session_time_zone_str_(char *buf, const int64_t len, int64_t &pos);
  int add_timestamp_(const int64_t num, const ObDateUnitType time_unit, int64_t &timestamp);
  int floor_timestamp_(const ObDateUnitType time_unit, int64_t &timestamp);
public:
  static const int64_t MAX_PRECREATE_PART_NUM = 2048;

public:
  static int str_to_time_unit(const ObString &str, ObDateUnitType &time_unit);
  static int str_to_time(const ObString &str, int64_t &time_num, ObDateUnitType &time_unit);
  static int format_dynamic_partition_policy_str(ObIAllocator &allocator, const ObString &orig_str, ObString &new_str);
  static int fill_default_value(ObIAllocator &allocator, const ObString &orig_str, ObString &new_str);
  static int update_dynamic_partition_policy_str(ObIAllocator &allocator, const ObString &orig_str, const ObString &alter_str, ObString &new_str);
  static int check_tenant_is_valid_for_dynamic_partition(const uint64_t tenant_id, bool &is_valid);
  static int check_is_supported(const schema::ObTableSchema &table_schema);
  static int check_is_valid(const schema::ObTableSchema &table_schema);
  static int get_enable(const schema::ObTableSchema &table_schema, bool &enable);
  static int get_time_unit(const schema::ObTableSchema &table_schema, ObDateUnitType &time_unit);
  static int get_precreate_time(const schema::ObTableSchema &table_schema, int64_t &num, ObDateUnitType &time_unit);
  static int get_expire_time(const schema::ObTableSchema &table_schema, int64_t &num, ObDateUnitType &time_unit);
  static int get_bigint_precision(const schema::ObTableSchema &table_schema, common::ObString &bigint_precision);
  static int get_time_zone(const schema::ObTableSchema &table_schema, common::ObString &time_zone);
  static int print_dynamic_partition_policy(const schema::ObTableSchema &table_schema, char* buf, const int64_t buf_len, int64_t& pos);
private:
  static int64_t get_approximate_hour_num_(ObDateUnitType type);
  static bool is_int_as_timestamp_(ObObjType type);
  static bool is_stored_in_utc_(ObObjType type);
  static bool support_part_key_type_(ObObjType type);
  static bool is_valid_time_unit_(ObObjType type, ObDateUnitType time_unit);
  static int update_dynamic_partition_policy_with_str_(const ObString &str, bool is_alter, ObDynamicPartitionPolicy &policy);
  static int dynamic_partition_policy_to_str_(ObIAllocator &allocator, const ObDynamicPartitionPolicy& policy, ObString &str);
  static int str_to_dynamic_partition_policy_(const ObString &str, ObDynamicPartitionPolicy &policy);
  static int64_t bigint_precision_scale_(const ObString &bigint_precision);
  static int get_time_zone_wrap_(const uint64_t tenant_id, const ObString &time_zone, ObTimeZoneInfoWrap &tz_info_wrap);
  static bool is_time_unit_matching_(const common::ObIArray<common::ObString> &time_unit_strs, const common::ObString &time_unit_str);
  static int64_t get_current_timestamp_();
  static int check_enable_is_valid_(const schema::ObTableSchema &table_schema);
  static int check_time_unit_is_valid_(const schema::ObTableSchema &table_schema);
  static int check_precreate_time_is_valid_(const schema::ObTableSchema &table_schema);
  static int check_expire_time_is_valid_(const schema::ObTableSchema &table_schema);
  static int check_time_zone_is_valid_(const schema::ObTableSchema &table_schema);
  static int check_bigint_precision_is_valid_(const schema::ObTableSchema &table_schema);
  static int print_dynamic_partition_policy_(
    const ObDynamicPartitionPolicy &policy,
    bool is_show_create_table,
    char* buf,
    const int64_t buf_len,
    int64_t& pos);
private:
  bool inited_;
  uint64_t tenant_id_;
  const schema::ObTableSchema *table_schema_;
  sql::ObSQLSessionInfo *session_;
  ObDynamicPartitionPolicy policy_;
  bool is_oracle_mode_;

  DISALLOW_COPY_AND_ASSIGN(ObDynamicPartitionManager);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_DYNAMIC_PARTITION_UTIL_H_
