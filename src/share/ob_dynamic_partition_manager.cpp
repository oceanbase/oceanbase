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

#include "share/ob_dynamic_partition_manager.h"

#include "lib/container/ob_array.h"
#include "lib/utility/ob_fast_convert.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/ob_errno.h"
#include "share/stat/ob_dbms_stats_maintenance_window.h"


namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObDynamicPartitionManager::init(
  const ObTableSchema *table_schema,
  sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dynamic partition manager already inited", KR(ret));
  } else if (OB_ISNULL(table_schema_ = table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is null", KR(ret));
  } else if (OB_ISNULL(session_ = session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_INVALID_ID == (tenant_id_ = table_schema->get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", KR(ret));
  } else if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema->get_dynamic_partition_policy(), policy_))) {
    LOG_WARN("fail to convert str to dynamic partition policy", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode_))) {
    LOG_WARN("fail to check is oracle mode with tenant id", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(check_is_supported(*table_schema))) {
    LOG_WARN("fail to check table schema is supported for dynamic partition", KR(ret), KPC(table_schema));
  } else if (OB_FAIL(check_is_valid(*table_schema))) {
    LOG_WARN("fail to check table schema is valid for dynamic partition", KR(ret), KPC(table_schema));
  } else {
    inited_ = true;
  }

  return ret;
}

int ObDynamicPartitionManager::execute(
  const ObString &specified_precreate_time,
  const ObIArray<ObString> &specified_time_unit_array,
  bool &skipped)
{
  int ret = OB_SUCCESS;
  skipped = false;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    bool enable = policy_.enable_;
    ObDateUnitType time_unit = policy_.time_unit_;
    if (!enable) {
      // dynamic partition is not enabled, skip
      skipped = true;
    } else if (!is_time_unit_matching_(specified_time_unit_array, ob_date_unit_type_str(time_unit))) {
      // time unit is not specified, skip
      skipped = true;
    } else {
      if (OB_FAIL(add_dynamic_partition_(specified_precreate_time))) {
        LOG_WARN("fail to add table dynamic partitions", KR(ret), K(specified_precreate_time));
      } else if (OB_FAIL(drop_dynamic_partition_())) {
        LOG_WARN("fail to drop table dynamic partitions", KR(ret));
      }
    }
  }
  return ret;
}

int ObDynamicPartitionManager::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("dynamic partition manager not inited", KR(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", KR(ret));
  } else if (OB_ISNULL(session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_INVALID_ID == tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant id is invalid", KR(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX schema service is null", KR(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX sql proxy is null", KR(ret));
  }

  return ret;
}

int ObDynamicPartitionManager::add_dynamic_partition_(const ObString &specified_precreate_time)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t precreate_time_num = policy_.precreate_time_num_;
    ObDateUnitType precreate_time_unit = policy_.precreate_time_unit_;
    ObSqlString sql;
    if (precreate_time_num < 0) {
      // precreate is off, skip
    } else if (OB_FAIL(build_add_partition_sql_(specified_precreate_time, sql))) {
      LOG_WARN("fail to build add partition sql", KR(ret), K(specified_precreate_time));
    } else if (sql.empty()) {
      // no need to pre create partitions, skip
    } else if (OB_FAIL(write_ddl_(sql))) {
      LOG_WARN("fail to write ddl", KR(ret), K(sql));
    }
  }

  return ret;
}

int ObDynamicPartitionManager::drop_dynamic_partition_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t expire_time_num = policy_.expire_time_num_;
    ObDateUnitType expire_time_unit = policy_.expire_time_unit_;
    ObSqlString sql;
    if (expire_time_num < 0) {
      // expire is off, skip
    } else if (OB_FAIL(build_drop_partition_sql_(sql))) {
      LOG_WARN("fail to build drop partition sql", KR(ret));
    } else if (sql.empty()) {
      // no need to drop partitions, skip
    } else if (OB_FAIL(write_ddl_(sql))) {
      LOG_WARN("fail to write ddl", KR(ret), K(sql));
    }
  }

  return ret;
}

int ObDynamicPartitionManager::write_ddl_(const ObSqlString &ddl)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t affected_rows = 0;
    ObTimeZoneInfoWrap tz_info_wrap;
    ObSessionParam session_param;
    if (OB_FAIL(get_table_time_zone_wrap_(tz_info_wrap))) {
      LOG_WARN("fail to get tenant time zone wrap", KR(ret));
    } else if (FALSE_IT(session_param.tz_info_wrap_ = &tz_info_wrap)) {
    } else if (OB_FAIL(GCTX.sql_proxy_->write(tenant_id_,
                                              ddl.ptr(),
                                              affected_rows,
                                              is_oracle_mode_ ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE,
                                              &session_param))) {
      LOG_WARN("fail to execute dynamic partition ddl", KR(ret), K_(tenant_id), K(ddl));
    }
  }

  return ret;
}

int ObDynamicPartitionManager::build_add_partition_sql_(
  const ObString &specified_precreate_time,
  ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const uint64_t database_id = table_schema_->get_database_id();
    const ObSimpleDatabaseSchema *database_schema = NULL;
    ObSqlString part_def_list;
    if (OB_FAIL(build_precreate_partition_definition_list_(specified_precreate_time, part_def_list))) {
      LOG_WARN("fail to build precreate partition definition list", KR(ret), K(specified_precreate_time));
    } else if (part_def_list.empty()) {
      // no need to add partition
    }  else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, database_id, database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K(database_id));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database schema is null", KR(ret));
    } else {
      const char *quote = is_oracle_mode_ ? "\"" : "`";
      if (OB_FAIL(sql.append_fmt("ALTER TABLE %s%s%s.%s%s%s ADD %s%.*s%s",
                                 quote, database_schema->get_database_name(), quote,
                                 quote, table_schema_->get_table_name(), quote,
                                 is_oracle_mode_ ? "" : "PARTITION (",
                                 static_cast<int32_t>(part_def_list.length()),
                                 part_def_list.ptr(),
                                 is_oracle_mode_ ? "" : ")"))) {
        LOG_WARN("fail to append sql", KR(ret));
      }
    }
  }
  return ret;
}

int ObDynamicPartitionManager::build_drop_partition_sql_(
  ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  sql.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const uint64_t database_id = table_schema_->get_database_id();
    const ObSimpleDatabaseSchema *database_schema = NULL;
    ObSqlString part_name_list;
    if (OB_FAIL(build_expired_partition_name_list_(part_name_list))) {
      LOG_WARN("fail to build expired partition name list", KR(ret));
    } else if (part_name_list.empty()) {
      // no need to drop expired partition
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
    } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id_, database_id, database_schema))) {
      LOG_WARN("fail to get database schema", KR(ret), K(database_id));
    } else if (OB_ISNULL(database_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database schema is null", KR(ret));
    } else {
      const char *quote = is_oracle_mode_ ? "\"" : "`";
      if (OB_FAIL(sql.append_fmt("ALTER TABLE %s%s%s.%s%s%s DROP PARTITION %.*s",
                                 quote, database_schema->get_database_name(), quote,
                                 quote, table_schema_->get_table_name(), quote,
                                 static_cast<int32_t>(part_name_list.length()),
                                 part_name_list.ptr()))) {
        LOG_WARN("fail to append sql", KR(ret));
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::build_precreate_partition_definition_list_(
  const ObString &specified_precreate_time,
  ObSqlString &part_def_list)
{
  int ret = OB_SUCCESS;
  part_def_list.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObPartition *max_part = NULL;
    int64_t target_precreate_timestamp = 0;
    int64_t low_bound_val_timestamp = 0;
    int64_t high_bound_val_timestamp = 0;
    ObDateUnitType time_unit = policy_.time_unit_;
    if (OB_FAIL(get_target_precreate_timestamp_(specified_precreate_time, target_precreate_timestamp))) {
      LOG_WARN("fail to get target precreate timestamp", KR(ret), K(specified_precreate_time));
    } else if (OB_FAIL(get_start_precreate_timestamp_(low_bound_val_timestamp))) {
      LOG_WARN("fail to get start precreate timestamp", KR(ret));
    } else {
      int64_t part_cnt = 0;
      while (OB_SUCC(ret) && low_bound_val_timestamp <= target_precreate_timestamp) {
        ObSqlString part_name;
        ObSqlString high_bound_val;
        high_bound_val_timestamp = low_bound_val_timestamp;
        if (OB_FAIL(add_timestamp_(1, time_unit, high_bound_val_timestamp))) {
          LOG_WARN("fail to add timestamp", KR(ret), K(time_unit));
        } else if (OB_FAIL(build_part_name_(low_bound_val_timestamp, part_name))) {
          LOG_WARN("fail to convert timestamp to part_name", KR(ret), K(low_bound_val_timestamp));
        } else if (OB_FAIL(build_high_bound_val_(high_bound_val_timestamp, high_bound_val))) {
          LOG_WARN("fail to convert timestamp to obj", KR(ret), K(high_bound_val_timestamp));
        } else {
          part_cnt++;
          if (part_cnt > MAX_PRECREATE_PART_NUM) {
            // too much precreate partition at a time, cut off
            LOG_INFO("too much precreate partitition", KR(ret), K(part_cnt));
            break;
          } else if (OB_FAIL(part_def_list.append_fmt("%sPARTITION %.*s VALUES LESS THAN (%.*s)",
                                                      1 == part_cnt ? "" : ", ",
                                                      static_cast<int32_t>(part_name.length()),
                                                      part_name.ptr(),
                                                      static_cast<int32_t>(high_bound_val.length()),
                                                      high_bound_val.ptr()))) {
            LOG_WARN("fail to append sql", KR(ret));
          }
        }
        low_bound_val_timestamp = high_bound_val_timestamp;
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::build_expired_partition_name_list_(
  ObSqlString &part_name_list)
{
  int ret = OB_SUCCESS;
  part_name_list.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t expire_timestamp = get_current_timestamp_();
    int64_t expire_time_num = policy_.expire_time_num_;
    ObDateUnitType expire_time_unit = policy_.expire_time_unit_;
    if (OB_UNLIKELY(expire_time_num < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("dynamic partition expire is off", KR(ret));
    } else if (OB_FAIL(add_timestamp_(-expire_time_num, expire_time_unit, expire_timestamp))) {
      LOG_WARN("fail to add timestamp", KR(ret), K(-expire_time_num), K(expire_time_unit));
    } else {
      // keep at least one partition
      const char *quote = is_oracle_mode_ ? "\"" : "`";
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_->get_partition_num() - 1; i++) {
        const ObPartition *part = table_schema_->get_part_array()[i];
        int64_t high_bound_val_timestamp = 0;
        if (OB_ISNULL(part)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part is null", KR(ret), K_(table_schema));
        } else if (OB_FAIL(fetch_timestamp_from_part_key_(*part, high_bound_val_timestamp))) {
          LOG_WARN("fail to fetch timestamp from part key", KR(ret), KPC(part));
        } else if (high_bound_val_timestamp >= expire_timestamp) {
          // subsequent partitions are not expired
          break;
        } else if (OB_FAIL(part_name_list.append_fmt("%s%s%.*s%s",
                                                     0 == i ? "" : ", ",
                                                     quote,
                                                     part->get_part_name().length(),
                                                     part->get_part_name().ptr(),
                                                     quote))) {
          LOG_WARN("fail to append sql", KR(ret));
        }
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::build_part_name_(const int64_t timestamp, ObSqlString &str)
{
  int ret = OB_SUCCESS;
  str.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObDateUnitType time_unit = policy_.time_unit_;
    ObTimeZoneInfoWrap time_zone_info_wrap;
    if (OB_FAIL(get_table_time_zone_wrap_(time_zone_info_wrap))) {
      LOG_WARN("fail to get table time zone wrap", KR(ret));
    } else {
      ObString format;
      switch (time_unit) {
        case ObDateUnitType::DATE_UNIT_YEAR:
          format = "%Y";
          break;
        case ObDateUnitType::DATE_UNIT_MONTH:
          format = "%Y%m";
          break;
        case ObDateUnitType::DATE_UNIT_WEEK:
          format = "%x_%v";
          break;
        case ObDateUnitType::DATE_UNIT_DAY:
          format = "%Y%m%d";
          break;
        case ObDateUnitType::DATE_UNIT_HOUR:
          format = "%Y%m%d%H";
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported time unit", KR(ret), K(time_unit));
          break;
      }

      ObTime ob_time;
      char buf[OB_MAX_TIMESTAMP_LENGTH] = {0};
      int64_t pos = 0;
      bool res_null = false;
      ObString locale;
      const char *quote = is_oracle_mode_ ? "\"" : "`";
      if (FAILEDx(ObTimeConverter::datetime_to_ob_time(timestamp, time_zone_info_wrap.get_time_zone_info(), ob_time))) {
        LOG_WARN("fail to convert timestamp to ob time", KR(ret), K(timestamp));
      } else if (OB_FAIL(ObTimeConverter::ob_time_to_str_format(ob_time,
                                                                format,
                                                                buf,
                                                                OB_MAX_TIMESTAMP_LENGTH,
                                                                pos,
                                                                res_null,
                                                                locale))) {
        LOG_WARN("fail to convert ob time to str", KR(ret), K(ob_time), K(format));
      } else if (OB_UNLIKELY(res_null)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to convert ob time to str", KR(ret), K(ob_time), K(format));
      } else if (OB_FAIL(str.append_fmt("%sP%.*s%s", quote, static_cast<int32_t>(pos), buf, quote))) {
        LOG_WARN("fail to assign str", KR(ret));
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::build_high_bound_val_(const int64_t timestamp, ObSqlString &str)
{
  int ret = OB_SUCCESS;
  str.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObObjType obj_type = ObObjType::ObMaxType;
    ObTimeZoneInfoWrap time_zone_info_wrap;
    ObObj high_bound_val_obj;
    if (OB_FAIL(table_schema_->get_part_key_column_type(0, obj_type))) {
      LOG_WARN("fail to get part key column type", KR(ret));
    } else if (OB_FAIL(get_table_time_zone_wrap_(time_zone_info_wrap))) {
      LOG_WARN("fail to get table time zone wrap", KR(ret));
    } else {
      switch (obj_type) {
        case ObObjType::ObIntType:
        case ObObjType::ObNumberType: {
          ObString bigint_precision = policy_.bigint_precision_;
          int64_t scale = bigint_precision_scale_(bigint_precision);
          if (OB_UNLIKELY(0 == scale)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("scale is 0", KR(ret), K(bigint_precision));
          } else {
            high_bound_val_obj.set_int(timestamp / scale);
          }
          break;
        }
        case ObObjType::ObYearType:
        case ObObjType::ObDateType:
        case ObObjType::ObMySQLDateType:
        case ObObjType::ObDateTimeType:
        case ObObjType::ObMySQLDateTimeType:
        case ObObjType::ObTimestampType:
        case ObObjType::ObTimestampNanoType:
        case ObObjType::ObTimestampLTZType: {
          if (ObTimestampLTZType == obj_type) {
            // when column type is ObTimestampLTZType, part key obj type will be ObTimestampTZType
            obj_type = ObTimestampTZType;
          }
          int64_t datetime = 0;
          // ObTimestampType can not cast to oracle time type, so first cast to ObDateTimeType
          if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(timestamp, time_zone_info_wrap.get_time_zone_info(), datetime))) {
            LOG_WARN("fail to convert timestamp to datetime", KR(ret), K(timestamp));
          } else {
            ObObj datetime_obj;
            datetime_obj.set_datetime(datetime);
            ObCastCtx cast_ctx;
            cast_ctx.dtc_params_ = session_->get_dtc_params();
            cast_ctx.dtc_params_.tz_info_ = time_zone_info_wrap.get_time_zone_info();
            if (OB_FAIL(ObObjCaster::to_type(obj_type, cast_ctx, datetime_obj, high_bound_val_obj))) {
              LOG_WARN("fail to cast datetime obj", KR(ret), K(obj_type), K(datetime_obj));
            }
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unsupported column type for dynamic partition", KR(ret), K(obj_type));
        }
      }

      if (OB_SUCC(ret)) {
        ObRowkey high_bound_val_rowkey;
        high_bound_val_rowkey.assign(&high_bound_val_obj, 1);
        const int64_t len = OB_MAX_DEFAULT_VALUE_LENGTH;
        int64_t pos = 0;
        SMART_VAR(char[len], buf) {
        if (FAILEDx(ObPartitionUtils::convert_rowkey_to_sql_literal(is_oracle_mode_,
                                                                    high_bound_val_rowkey,
                                                                    buf,
                                                                    len,
                                                                    pos,
                                                                    false/*print_collation,*/,
                                                                    time_zone_info_wrap.get_time_zone_info()))) {
          LOG_WARN("fail to convert rowkey to sql literal", KR(ret), K_(is_oracle_mode), K(high_bound_val_rowkey));
        } else if (OB_FAIL(str.assign(buf))) {
          LOG_WARN("fail to assign str", KR(ret));
        }
        } // end SMART_VAR
      }
    }
  }

  return ret;
}

// floor max part high bound val as start precreate timestamp
// if create table without partition definition, floor current time as start precreate timestamp
int ObDynamicPartitionManager::get_start_precreate_timestamp_(int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  timestamp = 0;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObPartition *max_part = NULL;
    int64_t part_num = table_schema_->get_partition_num();
    ObDateUnitType time_unit = policy_.time_unit_;
    if (part_num < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table partition num is less or equal than 0", KR(ret), K(part_num));
    } else if (0 == part_num) {
      // create table without partition definition
      timestamp = get_current_timestamp_();
    } else if (OB_ISNULL(max_part = table_schema_->get_part_array()[part_num - 1])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max part is null", KR(ret), K_(table_schema));
    } else if (OB_FAIL(fetch_timestamp_from_part_key_(*max_part, timestamp))) {
      LOG_WARN("fail to fetch timestamp from part key", KR(ret), KPC(max_part));
    }

    if (FAILEDx(floor_timestamp_(time_unit, timestamp))) {
      LOG_WARN("fail to floor timestamp", KR(ret), K(time_unit));
    }
  }

  return ret;
}

// get max precreate time between specified precreate time and table precreate time
int ObDynamicPartitionManager::get_target_precreate_timestamp_(
  const ObString &specified_precreate_time,
  int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  timestamp = 0;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    int64_t cur_timestamp = get_current_timestamp_();
    timestamp = cur_timestamp;
    int64_t precreate_time_num = policy_.precreate_time_num_;
    ObDateUnitType precreate_time_unit = policy_.precreate_time_unit_;
    if (OB_UNLIKELY(precreate_time_num < 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("dynamic partition precreate is off", KR(ret));
    } else if (OB_FAIL(add_timestamp_(precreate_time_num, precreate_time_unit, timestamp))) {
      LOG_WARN("fail to add timestamp", KR(ret), K(precreate_time_num), K(precreate_time_unit));
    } else if (!specified_precreate_time.empty()) {
      int64_t specified_precreate_time_num = -1;
      ObDateUnitType specified_precreate_time_unit = ObDateUnitType::DATE_UNIT_MAX;
      int64_t specified_timestamp = cur_timestamp;
      if (OB_FAIL(str_to_time(specified_precreate_time,
                              specified_precreate_time_num,
                              specified_precreate_time_unit))) {
        LOG_WARN("fail to convert str to time", KR(ret), K(specified_precreate_time));
      } else if (OB_UNLIKELY(specified_precreate_time_num < 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid specified precreate time", KR(ret));
      } else if (OB_FAIL(add_timestamp_(specified_precreate_time_num, specified_precreate_time_unit, specified_timestamp))) {
        LOG_WARN("fail to add timestamp", KR(ret), K(precreate_time_num), K(precreate_time_unit));
      } else {
        timestamp = max(timestamp, specified_timestamp);
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::get_table_time_zone_wrap_(ObTimeZoneInfoWrap &tz_info_wrap)
{
  int ret = OB_SUCCESS;
  tz_info_wrap.reset();

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObString time_zone = policy_.time_zone_;
    int64_t pos = 0;
    const int64_t len = OB_MAX_TIMESTAMP_TZ_LENGTH;
    char buf[len] = {0};
    if (OB_FAIL(get_session_time_zone_str_(buf, len, pos))) {
      LOG_WARN("fail to get session time zone str", KR(ret));
    } else {
      time_zone = 0 == time_zone.case_compare("DEFAULT") ? ObString(pos, buf) : time_zone;
      if (OB_FAIL(get_time_zone_wrap_(tenant_id_, time_zone, tz_info_wrap))) {
        LOG_WARN("failed to get time zone wrap", KR(ret), K_(tenant_id), K(time_zone));
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::fetch_timestamp_from_part_key_(
  const ObPartition &part,
  int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  timestamp = 0;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObRowkey &high_bound_val = part.get_high_bound_val();
    const ObObj *high_bound_val_obj = NULL;
    ObTimeZoneInfoWrap time_zone_info_wrap;
    if (OB_UNLIKELY(1 != high_bound_val.get_obj_cnt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part key has more than one column", KR(ret), K(high_bound_val.get_obj_cnt()), K(part));
    } else if (OB_ISNULL(high_bound_val_obj = high_bound_val.get_obj_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("high bound val obj is null", KR(ret));
    } else if (OB_FAIL(get_table_time_zone_wrap_(time_zone_info_wrap))) {
      LOG_WARN("fail to get tenant time zone wrap", KR(ret));
    } else {
      ObObjType obj_type = high_bound_val_obj->get_type();
      ObCastCtx cast_ctx;
      cast_ctx.dtc_params_ = session_->get_dtc_params();
      cast_ctx.dtc_params_.tz_info_ = time_zone_info_wrap.get_time_zone_info();
      switch (obj_type) {
        case ObObjType::ObDateType:
        case ObObjType::ObMySQLDateType:
        case ObObjType::ObDateTimeType:
        case ObObjType::ObMySQLDateTimeType:
        case ObObjType::ObTimestampType:
        case ObObjType::ObTimestampNanoType:
        case ObObjType::ObTimestampTZType: {
          ObObj datetime_obj;
          // oracle time type can not cast to ObTimestampType, so first cast to ObDateTimeType
          if (OB_FAIL(ObObjCaster::to_type(ObDateTimeType, cast_ctx, *high_bound_val_obj, datetime_obj))) {
            LOG_WARN("fail to cast obj to timestamp type", KR(ret), K(high_bound_val_obj));
          } else if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(datetime_obj.get_datetime(),
                                                                    time_zone_info_wrap.get_time_zone_info(),
                                                                    timestamp))) {
            LOG_WARN("fail to convert datetime to timestamp", KR(ret), K(datetime_obj));
          }
          break;
        }
        case ObObjType::ObYearType: {
          uint8_t year = high_bound_val_obj->get_year();
          int64_t int_val = 0;
          ObTimeConvertCtx cvrt_ctx(time_zone_info_wrap.get_time_zone_info(), true/*is_timestamp*/);
          ObDateSqlMode date_sql_mode;
          // get int as YYYY
          if (OB_FAIL(ObTimeConverter::year_to_int(year, int_val))) {
            LOG_WARN("fail to convert year to int", KR(ret), K(year));
          } else if (FALSE_IT(int_val = (int_val * 100 + 1) * 100 + 1)) {
            // YYYY -> YYYY0101
          } else if (OB_FAIL(ObTimeConverter::int_to_datetime(int_val, 0/*dec_part*/, cvrt_ctx, timestamp, date_sql_mode))) {
            LOG_WARN("fail to convert int to timestamp", KR(ret), K(int_val));
          }
          break;
        }
        case ObObjType::ObIntType:
        case ObObjType::ObNumberType: {
          ObObj int_obj;
          ObString bigint_precision = policy_.bigint_precision_;
          int64_t scale = bigint_precision_scale_(bigint_precision);
          if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, *high_bound_val_obj, int_obj))) {
            LOG_WARN("fail to cast obj to timestamp type", KR(ret), K(high_bound_val_obj));
          } else {
            timestamp = int_obj.get_int();
            if (0 == scale) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("bigint precision not specified", KR(ret), K(bigint_precision));
            } else {
              timestamp = timestamp * scale;
            }
          }
          break;
        }
        default: {
          if (high_bound_val_obj->is_max_value()) {
            timestamp = INT64_MAX;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("unsupported obj type", KR(ret), K(obj_type));
          }
        }
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::get_session_time_zone_str_(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    const ObTimeZoneInfo *tz_info = NULL;
    if (OB_ISNULL(tz_info = session_->get_timezone_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("timezone info is null", KR(ret));
    } else if (OB_FAIL(tz_info->timezone_to_str(buf, len, pos))) {
      LOG_WARN("fail to convert timezone to str", KR(ret));
    }
  }

  return ret;
}

int ObDynamicPartitionManager::add_timestamp_(const int64_t num, const ObDateUnitType time_unit, int64_t &timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObSqlString interval_str;
    ObDateSqlMode date_sql_mode;
    bool is_add = num >= 0;
    ObTimeZoneInfoWrap time_zone_info_wrap;
    int64_t datetime = 0;
    if (0 == num) {
    } else if (OB_FAIL(get_table_time_zone_wrap_(time_zone_info_wrap))) {
      LOG_WARN("fail to get table time zone wrap", KR(ret));
    } else if (OB_FAIL(ObTimeConverter::timestamp_to_datetime(timestamp, time_zone_info_wrap.get_time_zone_info(), datetime))) {
      LOG_WARN("fail to convert timestamp to datetime", KR(ret), K(timestamp), K(time_zone_info_wrap.get_time_zone_info()));
    } else if (OB_FAIL(interval_str.append_fmt("%ld", is_add ? num : -num))) {
      LOG_WARN("fail to append str", KR(ret));
    } else if (OB_FAIL(ObTimeConverter::date_adjust(datetime, interval_str.string(), time_unit, datetime, is_add, date_sql_mode))) {
      LOG_WARN("fail to adjust date", KR(ret), K(timestamp), K(interval_str), K(time_unit));
    } else if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(datetime, time_zone_info_wrap.get_time_zone_info(), timestamp))) {
      LOG_WARN("fail to convert datetime to timestamp", KR(ret), K(datetime), K(time_zone_info_wrap.get_time_zone_info()));
    }
  }

  return ret;
}

int ObDynamicPartitionManager::floor_timestamp_(const ObDateUnitType time_unit, int64_t &timestamp)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else {
    ObTime ob_time;
    ObTimeZoneInfoWrap time_zone_info_wrap;
    const ObTimeZoneInfo *tz_info = NULL;
    int32_t sec_offset = 0;
    if (OB_FAIL(get_table_time_zone_wrap_(time_zone_info_wrap))) {
      LOG_WARN("fail to get table time zone wrap", KR(ret));
    } else if (OB_ISNULL(tz_info = time_zone_info_wrap.get_time_zone_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tz info is null", KR(ret));
    } else if (OB_FAIL(ObTimeConverter::datetime_to_ob_time(timestamp, tz_info, ob_time))) {
      LOG_WARN("fail to convert timestamp to ob time", KR(ret), K(timestamp));
    } else {
      // To floor a datetime:
      //
      // DT_DATE: day count since 1970-01-01, for 1970-01-01, day count is 0
      // DT_HOUR: [0, 23], hour count of today
      // DT_YDAY: [1, 366], day number of this year, for 2025-01-02, day number is 2
      // DT_MDAY: [1, 31], day number of this month, for 2025-01-25, day number is 25
      // DT_WDAY: [1, 7], day number of this week, for Tuesday, day number is 2
      //
      // eg. current time is 2025-02-07 12:34:56, Friday
      //   - to floor with hour   get 2025-02-07 12:00:00         keep DT_DATE, DT_HOUR
      //   - to floor with day    get 2025-02-07 00:00:00         keep DT_DATE
      //   - to floor with week   get 2025-02-03 00:00:00 Monday  DT_WDAY = 5, so DT_DATE = DT_DATE - (DT_WDAY - 1)
      //   - to floor with month  get 2025-02-01 00:00:00         DT_MDAY = 7, so DT_DATE = DT_DATE - (DT_MDAY - 1)
      //   - to floor with year   get 2025-01-01 00:00:00         DT_YDAY = 38, so DT_DATE = DT_DATE - (DT_YDAY - 1)
      //
      // Finally, add time zone offset to get timestamp.

      int32_t sec_offset = tz_info->get_offset();
      int32_t day_offset = 0;
      int32_t day_count = ob_time.parts_[DT_DATE];
      int32_t hour_count = ObDateUnitType::DATE_UNIT_HOUR == time_unit ? ob_time.parts_[DT_HOUR] : 0;
      switch (time_unit) {
        case ObDateUnitType::DATE_UNIT_YEAR:
          day_offset = ob_time.parts_[DT_YDAY] - 1;
          break;
        case ObDateUnitType::DATE_UNIT_MONTH:
          day_offset = (ob_time.parts_[DT_MDAY] - 1);
          break;
        case ObDateUnitType::DATE_UNIT_WEEK:
          day_offset = (ob_time.parts_[DT_WDAY] - 1);
          break;
        case ObDateUnitType::DATE_UNIT_DAY:
        case ObDateUnitType::DATE_UNIT_HOUR:
          day_offset = 0;
          break;
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported type", KR(ret), K(time_unit));
        }
      }

      if (OB_SUCC(ret)) {
        timestamp = (day_count - day_offset) * USECS_PER_DAY + (hour_count * MINS_PER_HOUR * SECS_PER_MIN - sec_offset) * USECS_PER_SEC;
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::check_enable_is_valid_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  bool enable = false;
  if (OB_FAIL(get_enable(table_schema, enable))) {
    LOG_WARN("fail to get dynamic partition enable", KR(ret));
  }

  return ret;
}

// dynamic partition time_unit:
// - option values: hour, day, week, month, year
// - for mysql DATE part key, time_unit should not be hour
// - for mysql YEAR part key, time_unit should only be hour
int ObDynamicPartitionManager::check_time_unit_is_valid_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObDateUnitType time_unit;
  ObObjType col_data_type = ObObjType::ObMaxType;
  if (OB_FAIL(get_time_unit(table_schema, time_unit))) {
    LOG_WARN("fail to get dynamic partition time unit", KR(ret));
  } else if (OB_FAIL(table_schema.get_part_key_column_type(0, col_data_type))) {
    LOG_WARN("fail to get part key column type", KR(ret));
  } else if (!is_valid_time_unit_(col_data_type, time_unit)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid time unit type", KR(ret), K(col_data_type), K(time_unit));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition time_unit");
  }
  return ret;
}

// dynamic partition precreate_time:
// - option values:
//   - -1: never precreate
//   - 0: only precreate current partition
//   - n hour, day, week, month, year
// - precreate partition num should less than MAX_PRECREATE_PART_NUM
// - precreate_partition_num = precreate_time / time_unit
int ObDynamicPartitionManager::check_precreate_time_is_valid_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t precreate_num = -1;
  ObDateUnitType precreate_time_unit = ObDateUnitType::DATE_UNIT_MAX;
  ObDateUnitType time_unit = ObDateUnitType::DATE_UNIT_MAX;
  if (OB_FAIL(get_precreate_time(table_schema, precreate_num, precreate_time_unit))) {
    LOG_WARN("fail to get precreate time", KR(ret), K(table_schema));
  } else if (OB_FAIL(get_time_unit(table_schema, time_unit))) {
    LOG_WARN("fail to get time unit", KR(ret), K(table_schema));
  } else if (precreate_num <= -2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid precreate time", KR(ret));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition precreate_time");
  } else if (-1 != precreate_num && 0 != precreate_num) {
    int64_t precreate_hour_num = get_approximate_hour_num_(precreate_time_unit);
    int64_t time_unit_hour_num = get_approximate_hour_num_(time_unit);
    if (0 == precreate_hour_num || 0 == time_unit_hour_num) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid precreate time", KR(ret), K(precreate_time_unit), K(time_unit));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition precreate_time");
    } else {
      // To avoid precreating too many partitions at a time,
      // (MAX_PRECREATE_PART_NUM * time_unit_hour_num) / (precreate_num * precreate_hour_num) should >= 1
      int64_t ratio = MAX_PRECREATE_PART_NUM * time_unit_hour_num / precreate_num / precreate_hour_num;
      if (ratio < 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid precreate time, too large", KR(ret), K(time_unit), K(precreate_num), K(precreate_time_unit));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition precreate_time");
      }
    }
  } else {
    // -1 == precreate_num || 0 == precreate_num
    if (ObDateUnitType::DATE_UNIT_MAX != precreate_time_unit) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid precreate time", KR(ret), K(precreate_num), K(precreate_time_unit));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition precreate_time");
    }
  }

  return ret;
}

// dynamic partition expire_time:
// - option values:
//   - -1: never expire
//   - 0: only reserve current partition
//   - n hour, day, week, month, year
int ObDynamicPartitionManager::check_expire_time_is_valid_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  int64_t expire_num = -1;
  ObDateUnitType expire_time_unit = ObDateUnitType::DATE_UNIT_MAX;
  if (OB_FAIL(get_expire_time(table_schema, expire_num, expire_time_unit))) {
    LOG_WARN("fail to get expire time", KR(ret), K(table_schema));
  }
  return ret;
}

// dynamic partition time_zone:
// - option values:
//   - default: use tenant time zone
//   - time zone str like +8:00
// - for mysql TIMESTAMP and BIGINT, oracle TIMESTAMP WITH LOCAL TIME ZONE and NUMBER,
//   can not specify time_zone
int ObDynamicPartitionManager::check_time_zone_is_valid_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObObjType col_data_type = ObObjType::ObMaxType;
  ObString time_zone;
  if (OB_FAIL(get_time_zone(table_schema, time_zone))) {
    LOG_WARN("fail to get dynamic partition time zone", KR(ret), K(table_schema));
  } else if (0 == time_zone.case_compare(DEFAULT_DYNAMIC_PARTITION_TIME_ZONE_STR)) {
  } else if (OB_FAIL(table_schema.get_part_key_column_type(0, col_data_type))) {
    LOG_WARN("fail to get part key column type", KR(ret));
  } else if (is_stored_in_utc_(col_data_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not specify time zone", KR(ret), K(col_data_type), K(time_zone));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition time_zone");
  }

  return ret;
}

// dynamic partition bigint_precision:
// - option values: s / ms / us / none
// - only mysql BIGINT and oracle NUMBER can specify bigint_precision, and it must be specified
int ObDynamicPartitionManager::check_bigint_precision_is_valid_(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObObjType col_data_type = ObObjType::ObMaxType;
  ObString bigint_precision;
  if (OB_FAIL(get_bigint_precision(table_schema, bigint_precision))) {
    LOG_WARN("fail to get dynamic partition time zone", KR(ret), K(table_schema));
  } else if (OB_FAIL(table_schema.get_part_key_column_type(0, col_data_type))) {
    LOG_WARN("fail to get part key column type", KR(ret));
  } else {
    if (0 == bigint_precision.case_compare(DEFAULT_DYNAMIC_PARTITION_BIGINT_PRECISION_STR)) {
      if (is_int_as_timestamp_(col_data_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("bigint part key must specify bigint precision", KR(ret), K(col_data_type));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition bigint_precision");
      }
    } else {
      if (!is_int_as_timestamp_(col_data_type)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("non bigint part key can not specify bigint precision", KR(ret), K(col_data_type));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "dynamic partition bigint_precision");
      }
    }
  }
  return ret;
}

int ObDynamicPartitionManager::print_dynamic_partition_policy_(
  const ObDynamicPartitionPolicy &policy,
  bool is_show_create_table,
  char* buf,
  const int64_t buf_len,
  int64_t& pos)
{
  int ret = OB_SUCCESS;
  const char *format = is_show_create_table
                       ? "ENABLE = %s, TIME_UNIT = '%s', PRECREATE_TIME = '%ld%s', EXPIRE_TIME = '%ld%s', TIME_ZONE = '%.*s', BIGINT_PRECISION = '%.*s'"
                       : "ENABLE=%s,TIME_UNIT=%s,PRECREATE_TIME=%ld%s,EXPIRE_TIME=%ld%s,TIME_ZONE=%.*s,BIGINT_PRECISION=%.*s";

  if (OB_FAIL(databuff_printf(buf,
                              buf_len,
                              pos,
                              format,
                              policy.enable_ ? "TRUE": "FALSE",
                              ob_date_unit_type_str_upper(policy.time_unit_),
                              policy.precreate_time_num_,
                              policy.precreate_time_num_ > 0 ? ob_date_unit_type_str_upper(policy.precreate_time_unit_) : "",
                              policy.expire_time_num_,
                              policy.expire_time_num_ > 0 ? ob_date_unit_type_str_upper(policy.expire_time_unit_) : "",
                              policy.time_zone_.length(),
                              policy.time_zone_.ptr(),
                              policy.bigint_precision_.length(),
                              policy.bigint_precision_.ptr()))) {
    LOG_WARN("fail to build dynamic partition policy str", KR(ret), K(policy));
  }
  return ret;
}

int ObDynamicPartitionManager::str_to_time_unit(const ObString &str, ObDateUnitType &time_unit)
{
  int ret = OB_SUCCESS;
  time_unit = ObDateUnitType::DATE_UNIT_MAX;
  if (0 == str.case_compare("HOUR")) {
    time_unit = ObDateUnitType::DATE_UNIT_HOUR;
  } else if (0 == str.case_compare("DAY")) {
    time_unit = ObDateUnitType::DATE_UNIT_DAY;
  } else if (0 == str.case_compare("WEEK")) {
    time_unit = ObDateUnitType::DATE_UNIT_WEEK;
  } else if (0 == str.case_compare("MONTH")) {
    time_unit = ObDateUnitType::DATE_UNIT_MONTH;
  } else if (0 == str.case_compare("YEAR")) {
    time_unit = ObDateUnitType::DATE_UNIT_YEAR;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dynamic partition time unit", KR(ret), K(str));
  }

  return ret;
}

int ObDynamicPartitionManager::str_to_time(
  const ObString &str,
  int64_t &time_num,
  ObDateUnitType &time_unit)
{
  int ret = OB_SUCCESS;
  bool time_num_valid = true;
  bool time_unit_valid = true;
  time_unit = ObDateUnitType::DATE_UNIT_MAX;
  time_num = -1;
  if (str.suffix_match_ci("HOUR")) {
    time_unit = ObDateUnitType::DATE_UNIT_HOUR;
    time_num = ObFastAtoi<int64_t>::atoi(str.ptr(), str.ptr() + str.length() - 4, time_num_valid);
  } else if (str.suffix_match_ci("DAY")) {
    time_unit = ObDateUnitType::DATE_UNIT_DAY;
    time_num = ObFastAtoi<int64_t>::atoi(str.ptr(), str.ptr() + str.length() - 3, time_num_valid);
  } else if (str.suffix_match_ci("WEEK")) {
    time_unit = ObDateUnitType::DATE_UNIT_WEEK;
    time_num = ObFastAtoi<int64_t>::atoi(str.ptr(), str.ptr() + str.length() - 4, time_num_valid);
  } else if (str.suffix_match_ci("MONTH")) {
    time_unit = ObDateUnitType::DATE_UNIT_MONTH;
    time_num = ObFastAtoi<int64_t>::atoi(str.ptr(), str.ptr() + str.length() - 5, time_num_valid);
  } else if (str.suffix_match_ci("YEAR")) {
    time_unit = ObDateUnitType::DATE_UNIT_YEAR;
    time_num = ObFastAtoi<int64_t>::atoi(str.ptr(), str.ptr() + str.length() - 4, time_num_valid);
  } else if (str == "-1") {
    time_num = -1;
    time_unit = ObDateUnitType::DATE_UNIT_MAX;
  } else if (str == "0") {
    time_num = 0;
    time_unit = ObDateUnitType::DATE_UNIT_MAX;
  } else {
    time_unit_valid = false;
  }

  if (!time_num_valid || !time_unit_valid || (ObDateUnitType::DATE_UNIT_MAX != time_unit && time_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dynamic partition policy params", KR(ret), K(str));
  }

  return ret;
}

// upper case, remove space and quotes
int ObDynamicPartitionManager::format_dynamic_partition_policy_str(
  ObIAllocator &allocator,
  const ObString &orig_str,
  ObString &new_str)
{
  int ret = OB_SUCCESS;
  const ObString::obstr_size_t orig_len = orig_str.length();
  char *new_ptr = NULL;
  const char *orig_ptr = orig_str.ptr();
  if (OB_ISNULL(orig_ptr) || OB_UNLIKELY(orig_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dynamic partition policy str is null", KR(ret));
  } else if (NULL == (new_ptr = static_cast<char *>(allocator.alloc(orig_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(orig_len));
  } else {
    int64_t j = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < orig_len; i++) {
      if (!isspace(orig_ptr[i]) && orig_ptr[i] != '\'' && orig_ptr[i] != '\"') {
        new_ptr[j] = orig_ptr[i];
        j++;
      }
    }
    new_str.assign_ptr(new_ptr, j);
    if (FAILEDx(ob_simple_low_to_up(allocator, new_str, new_str))) {
      LOG_WARN("fail to simple low to up", KR(ret));
    }
  }

  return ret;
}

int ObDynamicPartitionManager::fill_default_value(
  ObIAllocator &allocator,
  const ObString &orig_str,
  ObString &new_str)
{
  int ret = OB_SUCCESS;
  ObString empty_str;
  if (OB_FAIL(update_dynamic_partition_policy_str(allocator, empty_str, orig_str, new_str))) {
    LOG_WARN("fail to alter dynamic partition policy str", KR(ret), K(orig_str));
  }

  return ret;
}

int ObDynamicPartitionManager::update_dynamic_partition_policy_str(
  ObIAllocator &allocator,
  const ObString &orig_str,
  const ObString &alter_str,
  ObString &new_str)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  bool is_alter = !orig_str.empty();
  if (OB_FAIL(str_to_dynamic_partition_policy_(orig_str, policy))) {
    LOG_WARN("fail to convert str to dynamic partition policy", KR(ret), K(orig_str));
  } else if (OB_FAIL(update_dynamic_partition_policy_with_str_(alter_str, is_alter, policy))) {
    LOG_WARN("fail to alter dynamic partition policy with str", KR(ret), K(is_alter), K(alter_str));
  } else if (OB_FAIL(dynamic_partition_policy_to_str_(allocator, policy, new_str))) {
    LOG_WARN("fail to convert dynamic partition policy to str", KR(ret), K(policy));
  }
  return ret;
}

int ObDynamicPartitionManager::check_tenant_is_valid_for_dynamic_partition(
  const uint64_t tenant_id,
  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool is_primary = false;
  ObSchemaGetterGuard schema_guard;
  const ObSimpleTenantSchema *tenant_schema = NULL;

  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    is_valid = false;
    LOG_INFO("tenant id is invalid", KR(ret));
  } else if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    is_valid = false;
    LOG_INFO("not user tenant", KR(ret));
  } else if (OB_FAIL(ObShareUtil::mtl_check_if_tenant_role_is_primary(tenant_id, is_primary))) {
    LOG_WARN("fail to check if tenant role is primary", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_primary)) {
    is_valid = false;
    LOG_INFO("not primary tenant", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("fail to get tenant info", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    is_valid = false;
    LOG_INFO("tenant schema is null", KR(ret));
  } else if (OB_UNLIKELY(!tenant_schema->is_normal())) {
    is_valid = false;
    LOG_INFO("tenant status is not normal", KR(ret));
  } else if (OB_UNLIKELY(tenant_schema->is_in_recyclebin())) {
    is_valid = false;
    LOG_INFO("tenant is in recylebin", KR(ret));
  }

  return ret;
}

int ObDynamicPartitionManager::check_is_supported(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  ObObjType col_data_type = ObObjType::ObMaxType;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  ObString part_key_column_name;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("dynamic partition in non-user tenant is not supported", KR(ret), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "dynamic partition in non-user tenant is");
  } else if (!table_schema.is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("specify dynamic partition on non user table is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify dynamic partition on non user table is");
  } else if (ObPartitionLevel::PARTITION_LEVEL_ZERO == table_schema.get_part_level()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("specify dynamic partition on non partitioned table is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify dynamic partition on non partitioned table is");
  } else if (1 != table_schema.get_partition_key_column_num()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("specify dynamic partition on table which part key columns are more than one is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify dynamic partition on table which part key columns are more than one is");
  } else if (!table_schema.is_range_part() || table_schema.is_interval_part()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("specify dynamic partition on table not partitioned by range is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify dynamic partition on table not partitioned by range is");
  } else if (OB_FAIL(table_schema.get_part_key_column_type(0, col_data_type))) {
    LOG_WARN("fail to get part key column type", KR(ret));
  } else if (!support_part_key_type_(col_data_type)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("for dynamic partition, this part key type is not supported", KR(ret), K(col_data_type));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "for dynamic partition, this part key type is");
  } else if (OB_FAIL(table_schema.get_part_key_column_name(0, part_key_column_name))) {
    LOG_WARN("fail to get part key column name", KR(ret));
  } else {
    const ObString &origin_part_func = table_schema.get_part_option().get_part_func_expr_str();
    ObString part_func = origin_part_func;
    // remove ` or " from part_func_expr_str
    if (origin_part_func.length() > 2 &&
        ((origin_part_func[origin_part_func.length() - 1] == '`' && origin_part_func[0] == '`') ||
         (origin_part_func[origin_part_func.length() - 1] == '"' && origin_part_func[0] == '"'))) {
      part_func.assign_ptr(origin_part_func.ptr() + 1, origin_part_func.length() - 2);
    }
    if (0 != part_key_column_name.case_compare(part_func)) {
      // part key with function
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("for dynamic partition, this part func expr is not supported", KR(ret), "part_func_expr", part_func);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "for dynamic partition, this part func expr is");
    }
  }

  return ret;
}

int ObDynamicPartitionManager::check_is_valid(const ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_enable_is_valid_(table_schema))) {
    LOG_WARN("fail to check dynamic partition enable is legal", KR(ret), K(table_schema));
  } else if (OB_FAIL(check_time_unit_is_valid_(table_schema))) {
    LOG_WARN("fail to check dynamic partition time unit is legal", KR(ret), K(table_schema));
  } else if (OB_FAIL(check_precreate_time_is_valid_(table_schema))) {
    LOG_WARN("fail to check dynamic partition precreate time is legal", KR(ret), K(table_schema));
  } else if (OB_FAIL(check_expire_time_is_valid_(table_schema))) {
    LOG_WARN("fail to check dynamic partition expire time is legal", KR(ret), K(table_schema));
  } else if (OB_FAIL(check_time_zone_is_valid_(table_schema))) {
    LOG_WARN("fail to check dynamic partition time zone is legal", KR(ret), K(table_schema));
  } else if (OB_FAIL(check_bigint_precision_is_valid_(table_schema))) {
    LOG_WARN("fail to check dynamic partition bigint precision is legal", KR(ret), K(table_schema));
  }

  return ret;
}

// for show create table
int ObDynamicPartitionManager::print_dynamic_partition_policy(
  const ObTableSchema &table_schema,
  char* buf,
  const int64_t buf_len,
  int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  const ObString &str = table_schema.get_dynamic_partition_policy();
  if (OB_FAIL(str_to_dynamic_partition_policy_(str, policy))) {
    LOG_WARN("fail to convert str to dynamic partition policy", KR(ret), K(str));
  } else if (OB_FAIL(print_dynamic_partition_policy_(policy, true/*is_show_create_table*/, buf, buf_len, pos))) {
    LOG_WARN("fail to print dynamic partition policy", KR(ret), K(policy));
  }
  return ret;
}

int64_t ObDynamicPartitionManager::get_approximate_hour_num_(ObDateUnitType type)
{
  int64_t approximate_hour_num = 0;
  switch (type) {
    case DATE_UNIT_HOUR:
      approximate_hour_num = 1;
      break;
    case DATE_UNIT_DAY:
      approximate_hour_num = 24;
      break;
    case DATE_UNIT_WEEK:
      approximate_hour_num = 168;
      break;
    case DATE_UNIT_MONTH:
      approximate_hour_num = 720;
      break;
    case DATE_UNIT_YEAR:
      approximate_hour_num = 8760;
      break;
    default:
      approximate_hour_num = 0;
      break;
  }
  return approximate_hour_num;
}

bool ObDynamicPartitionManager::is_int_as_timestamp_(ObObjType type)
{
  return ObObjType::ObIntType == type || ObObjType::ObNumberType == type;
}

bool ObDynamicPartitionManager::is_stored_in_utc_(ObObjType type)
{
  return ObObjType::ObTimestampType == type // mysql timestamp
         || ObObjType::ObIntType == type // mysql bigint
         || ObObjType::ObTimestampLTZType == type // oracle timestamp with local time zone
         || ObObjType::ObNumberType == type; // oracle number
}

bool ObDynamicPartitionManager::support_part_key_type_(ObObjType type)
{
  return ObObjType::ObDateType == type // mysql date
         || ObObjType::ObMySQLDateType == type // mysql date
         || ObObjType::ObDateTimeType == type // oracle date
         || ObObjType::ObMySQLDateTimeType == type // mysql datetime
         || ObObjType::ObTimestampType == type // mysql timestamp
         || ObObjType::ObYearType == type // mysql year
         || ObObjType::ObIntType == type // mysql bigint
         || ObObjType::ObTimestampNanoType == type // oracle timestamp
         || ObObjType::ObTimestampLTZType == type // oracle timestamp with local time zone
         || ObObjType::ObNumberType == type; // oracle number
}

bool ObDynamicPartitionManager::is_valid_time_unit_(ObObjType type, ObDateUnitType time_unit)
{
  bool is_valid = true;
  switch (type) {
    case ObObjType::ObDateType:
    case ObObjType::ObMySQLDateType:
      is_valid = time_unit != ObDateUnitType::DATE_UNIT_HOUR;
      break;
    case ObObjType::ObYearType:
      is_valid = time_unit == ObDateUnitType::DATE_UNIT_YEAR;
      break;
    default:
      is_valid = support_part_key_type_(type);
      break;
  }
  return is_valid;
}

int ObDynamicPartitionManager::update_dynamic_partition_policy_with_str_(
  const ObString &str,
  bool is_alter,
  ObDynamicPartitionPolicy &policy)
{
  int ret = OB_SUCCESS;
  ObString tmp_str = str;
  ObArray<ObString> tmp_strs;
  if (tmp_str.empty()) {
  } else if (OB_FAIL(split_on(tmp_str, ',', tmp_strs))) {
    LOG_WARN("fail to split on str", KR(ret), K(tmp_str));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_strs.count(); i++) {
      ObString kv_str = tmp_strs.at(i);
      ObArray<ObString> kv;
      if (OB_FAIL(split_on(kv_str, '=', kv))) {
        LOG_WARN("fail to split on str", KR(ret), K(kv_str));
      } else if (OB_UNLIKELY(kv.count() != 2)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("dynamic partition policy is invalid", KR(ret), K(kv.count()));
      } else {
        ObString k = kv.at(0);
        ObString v = kv.at(1);
        if (0 == k.case_compare("ENABLE")) {
          if (0 == v.case_compare("TRUE")) {
            policy.enable_ = true;
          } else if (0 == v.case_compare("FALSE")) {
            policy.enable_ = false;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid dynamic partition policy params", KR(ret), K(k), K(v));
          }
        } else if (0 == k.case_compare("TIME_UNIT")) {
          if (is_alter) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support to alter dynamic partition time unit", KR(ret));
          } else if (OB_FAIL(str_to_time_unit(v, policy.time_unit_))) {
            LOG_WARN("fail to convert str to time unit", KR(ret), K(v));
          }
        } else if (0 == k.case_compare("PRECREATE_TIME")) {
          if (OB_FAIL(str_to_time(v, policy.precreate_time_num_, policy.precreate_time_unit_))) {
            LOG_WARN("fail to convert str to time interval", KR(ret), K(v));
          }
        } else if (0 == k.case_compare("EXPIRE_TIME")) {
          if (OB_FAIL(str_to_time(v, policy.expire_time_num_, policy.expire_time_unit_))) {
            LOG_WARN("fail to convert str to time interval", KR(ret), K(v));
          }
        } else if (0 == k.case_compare("TIME_ZONE")) {
          int32_t offset = 0;
          int ret_more = OB_SUCCESS;
          if (is_alter) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support to alter dynamic partition time zone", KR(ret));
          } else {
            if (0 != v.case_compare(DEFAULT_DYNAMIC_PARTITION_TIME_ZONE_STR)
                && OB_FAIL(ObTimeConverter::str_to_offset(v, offset, ret_more, false/*is_oracle_mode*/, true/*need_check_valid*/))) {
              LOG_WARN("fail to convert str to offset", KR(ret), K(k), K(v));
            } else {
              policy.time_zone_ = v;
            }
          }
        } else if (0 == k.case_compare("BIGINT_PRECISION")) {
          if (is_alter) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support to alter dynamic partition bigint precision", KR(ret));
          } else {
            if (0 == v.case_compare("S")
                || 0 == v.case_compare("MS")
                || 0 == v.case_compare("US")
                || 0 == v.case_compare(DEFAULT_DYNAMIC_PARTITION_BIGINT_PRECISION_STR)) {
              policy.bigint_precision_ = v;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid dynamic partition policy params", KR(ret), K(k), K(v));
            }
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unknown dynamic partition policy params", KR(ret), K(k));
        }
      }
    }
  }

  return ret;
}

int ObDynamicPartitionManager::dynamic_partition_policy_to_str_(
  ObIAllocator &allocator,
  const ObDynamicPartitionPolicy& policy,
  ObString &str)
{
  int ret = OB_SUCCESS;
  char *new_ptr = NULL;
  const int64_t len = OB_MAX_DYNAMIC_PARTITION_POLICY_LENGTH;
  int64_t pos = 0;
  if (NULL == (new_ptr = static_cast<char *>(allocator.alloc(len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(len));
  } else if (OB_FAIL(print_dynamic_partition_policy_(policy,
                                                     false/*is_show_create_table*/,
                                                     new_ptr,
                                                     len,
                                                     pos))) {
    LOG_WARN("fail to print dynamic partition policy", KR(ret), K(policy));
  } else {
    str.assign_ptr(new_ptr, pos);
  }
  return ret;
}

int ObDynamicPartitionManager::str_to_dynamic_partition_policy_(const ObString &str, ObDynamicPartitionPolicy &policy)
{
  int ret = OB_SUCCESS;
  policy.reset();
  if (OB_FAIL(update_dynamic_partition_policy_with_str_(str, false/*is_alter*/, policy))) {
    LOG_WARN("fail to update dynamic partition polict with str", KR(ret), K(str));
  }
  return ret;
}

int64_t ObDynamicPartitionManager::bigint_precision_scale_(const ObString &bigint_precision)
{
  int64_t scale = 0;
  if (0 == bigint_precision.case_compare("S")) {
    scale = 1000000;
  } else if (0 == bigint_precision.case_compare("MS")) {
    scale = 1000;
  } else if (0 == bigint_precision.case_compare("US")) {
    scale = 1;
  } else {
    scale = 0;
  }
  return scale;
}

int ObDynamicPartitionManager::get_time_zone_wrap_(const uint64_t tenant_id, const ObString &time_zone, ObTimeZoneInfoWrap &tz_info_wrap)
{
  int ret = OB_SUCCESS;
  ObString tenant_time_zone;
  ObTZMapWrap tz_map_wrap;
  ObTimeZoneInfoManager *tz_info_mgr = nullptr;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(OTTZ_MGR.get_tenant_timezone(tenant_id, tz_map_wrap, tz_info_mgr))) {
    LOG_WARN("failed to get tenant timezone", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tz_map_wrap.get_tz_map())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tz map is null", KR(ret));
  } else if (OB_FAIL(tz_info_wrap.init_time_zone(time_zone,
                                                 OB_INVALID_VERSION,
                                                 *(const_cast<ObTZInfoMap *>(tz_map_wrap.get_tz_map()))))) {
    LOG_WARN("failed to init time zone", KR(ret), K(time_zone));
  }
  return ret;
}

bool ObDynamicPartitionManager::is_time_unit_matching_(
  const ObIArray<ObString> &time_unit_strs,
  const ObString &time_unit_str)
{
  // when specified time_unit is null, match all time_unit
  bool match = time_unit_strs.empty();
  for (int64_t i = 0; !match && i < time_unit_strs.count(); i++) {
    match = 0 == time_unit_strs.at(i).case_compare(time_unit_str);
  }
  return match;
}

ERRSIM_POINT_DEF(ERRSIM_DYNAMIC_PARTITION_MOCK_TIME);
int64_t ObDynamicPartitionManager::get_current_timestamp_()
{
  int64_t current_timestamp = ObTimeUtility::current_time();
  if (OB_UNLIKELY(ERRSIM_DYNAMIC_PARTITION_MOCK_TIME)) {
    current_timestamp = -ERRSIM_DYNAMIC_PARTITION_MOCK_TIME.item_.error_code_;
    LOG_INFO("dynamic partition use mock time", K(current_timestamp));
  }
  return current_timestamp;
}

int ObDynamicPartitionManager::get_enable(
  const ObTableSchema &table_schema,
  bool &enable)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  enable = false;
  if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema.get_dynamic_partition_policy(), policy))) {
    LOG_WARN("fail to parse str to dynamic_partition_policy", KR(ret));
  } else {
    enable = policy.enable_;
  }
  return ret;
}

int ObDynamicPartitionManager::get_time_unit(
  const ObTableSchema &table_schema,
  ObDateUnitType &time_unit)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  time_unit = ObDateUnitType::DATE_UNIT_MAX;
  if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema.get_dynamic_partition_policy(), policy))) {
    LOG_WARN("fail to parse str to dynamic_partition_policy", KR(ret));
  } else {
    time_unit = policy.time_unit_;
  }
  return ret;
}

int ObDynamicPartitionManager::get_time_zone(
  const ObTableSchema &table_schema,
  ObString &time_zone)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  time_zone.reset();
  if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema.get_dynamic_partition_policy(), policy))) {
    LOG_WARN("fail to parse str to dynamic_partition_policy", KR(ret));
  } else {
    time_zone = policy.time_zone_;
  }
  return ret;
}

int ObDynamicPartitionManager::get_precreate_time(
  const ObTableSchema &table_schema,
  int64_t &num,
  ObDateUnitType &time_unit)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  num = -1;
  time_unit = ObDateUnitType::DATE_UNIT_MAX;
  if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema.get_dynamic_partition_policy(), policy))) {
    LOG_WARN("fail to parse str to dynamic_partition_policy", KR(ret));
  } else {
    num = policy.precreate_time_num_;
    time_unit = policy.precreate_time_unit_;
  }
  return ret;
}

int ObDynamicPartitionManager::get_expire_time(
  const ObTableSchema &table_schema,
  int64_t &num,
  ObDateUnitType &time_unit)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  num = -1;
  time_unit = ObDateUnitType::DATE_UNIT_MAX;
  if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema.get_dynamic_partition_policy(), policy))) {
    LOG_WARN("fail to parse str to dynamic_partition_policy", KR(ret));
  } else {
    num = policy.expire_time_num_;
    time_unit = policy.expire_time_unit_;
  }
  return ret;
}

int ObDynamicPartitionManager::get_bigint_precision(
  const ObTableSchema &table_schema,
  ObString &bigint_precision)
{
  int ret = OB_SUCCESS;
  ObDynamicPartitionPolicy policy;
  bigint_precision.reset();
  if (OB_FAIL(str_to_dynamic_partition_policy_(table_schema.get_dynamic_partition_policy(), policy))) {
    LOG_WARN("fail to parse str to dynamic_partition_policy", KR(ret));
  } else {
    bigint_precision = policy.bigint_precision_;
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
