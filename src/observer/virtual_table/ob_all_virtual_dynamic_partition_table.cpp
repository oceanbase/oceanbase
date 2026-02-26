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

#include "observer/virtual_table/ob_all_virtual_dynamic_partition_table.h"

#include "share/ob_dynamic_partition_manager.h"

namespace oceanbase
{
namespace observer
{

ObAllVirtualDynamicPartitionTable::ObAllVirtualDynamicPartitionTable()
  : is_inited_(false),
    schema_service_(NULL),
    tenant_ids_(),
    cur_tenant_table_ids_(),
    tenant_idx_(-1),
    table_idx_(-1)
{}

ObAllVirtualDynamicPartitionTable::~ObAllVirtualDynamicPartitionTable()
{
  destroy();
}

int ObAllVirtualDynamicPartitionTable::init(share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", KR(ret));
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret));
  } else {
    is_inited_ = true;
    schema_service_ = schema_service;
    tenant_ids_.reset();
    cur_tenant_table_ids_.reset();
    tenant_idx_ = -1;
    table_idx_ = -1;
  }
  return ret;
}

void ObAllVirtualDynamicPartitionTable::destroy()
{
  is_inited_ = false;
  schema_service_ = NULL;
  tenant_ids_.reset();
  cur_tenant_table_ids_.reset();
  tenant_idx_ = -1;
  table_idx_ = -1;
}

int ObAllVirtualDynamicPartitionTable::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret));
  } else if (tenant_idx_ >= tenant_ids_.count() || tenant_ids_.empty()) {
    ret = OB_ITER_END;
  } else {
    bool found_next_table = false;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    int64_t tenant_schema_version = OB_INVALID_VERSION;
    do {
      bool found_next_tenant = false;
      do {
        if (table_idx_ + 1 >= cur_tenant_table_ids_.count()) {
          if (OB_FAIL(next_tenant_())) {
            if (OB_ITER_END != ret) {
              SERVER_LOG(WARN, "switch to next tenant failed", KR(ret));
            }
          }
        } else {
          found_next_tenant = true;
        }
      } while (OB_SUCC(ret) && !found_next_tenant);

      if (OB_SUCC(ret)) {
        do {
          table_idx_ += 1;
          const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
          const uint64_t table_id = cur_tenant_table_ids_.at(table_idx_);

          if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
            SERVER_LOG(WARN, "fail to get tenant schema guard", KR(ret), K(tenant_id));
          } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
            SERVER_LOG(WARN, "fail to get table schema", KR(ret), K(tenant_id), K(table_id));
          } else if (OB_ISNULL(table_schema) || !table_schema->with_dynamic_partition_policy()) {
            // table may be dropped or altered, skip
            SERVER_LOG(INFO, "table is altered, skip", KR(ret), K(tenant_id), K(table_id));
          } else {
            found_next_table = true;
            if (OB_FAIL(schema_guard.get_schema_version(tenant_id, tenant_schema_version))) {
              SERVER_LOG(WARN, "fail to get schema version", KR(ret), K(tenant_id));
            }
          }
        } while (OB_SUCC(ret) && table_idx_ + 1 < cur_tenant_table_ids_.count() && !found_next_table);
      }
    } while (OB_SUCC(ret) && !found_next_table);

    if (OB_SUCC(ret) && found_next_table) {
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "table schema is null", KR(ret));
      } else if (OB_UNLIKELY(!table_schema->with_dynamic_partition_policy())) {
        SERVER_LOG(WARN, "table schema has no dynamic_partition_policy", KR(ret));
      } else if (OB_FAIL(build_new_row_(tenant_schema_version, *table_schema, row))) {
        SERVER_LOG(WARN, "fail to build new row", KR(ret), K(table_schema));
      }
    }
  }
  return ret;
}

int ObAllVirtualDynamicPartitionTable::inner_open()
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(effective_tenant_id_)) {
    // sys tenant show all user tenants info
    ObArray<uint64_t> tenant_ids;
    if (OB_ISNULL(schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "schema service is null", KR(ret));
    } else if (OB_FAIL(schema_service_->get_tenant_ids(tenant_ids))) {
      SERVER_LOG(WARN, "fail to get tenant ids", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); i++) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (is_user_tenant(tenant_id) && OB_FAIL(tenant_ids_.push_back(tenant_id))) {
          SERVER_LOG(WARN, "fail to push bakc tenant id", KR(ret), K(tenant_id));
        }
      }
    }
  } else {
    // user tenant show self tenant info
    if (OB_FAIL(tenant_ids_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "fail to push back tenant id", KR(ret), K_(effective_tenant_id));
    }
  }

  if (FAILEDx(next_tenant_())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "switch to next tenant failed", KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObAllVirtualDynamicPartitionTable::next_tenant_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service is null", KR(ret));
  } else if (tenant_idx_ + 1 >= tenant_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    tenant_idx_ += 1;
    table_idx_ = -1;
    cur_tenant_table_ids_.reset();

    const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
    ObSchemaGetterGuard schema_guard;
    ObArray<const ObSimpleTableSchemaV2 *> table_schemas;
    if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      SERVER_LOG(WARN, "fail to get tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schemas_in_tenant(tenant_id, table_schemas))) {
      SERVER_LOG(WARN, "fail to get table schemas in tenant", KR(ret), K(tenant_id));
    } else {
      // collect dynamic partition table ids
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); i++) {
        const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
        const ObDatabaseSchema *database_schema = NULL;
        if (OB_ISNULL(table_schema)
            || !table_schema->is_user_table()
            || !table_schema->with_dynamic_partition_policy()
            || table_schema->is_in_recyclebin()
            || !table_schema->is_normal_schema()) {
          // skip
        } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema->get_database_id(), database_schema))) {
          SERVER_LOG(WARN, "fail to get database schema", KR(ret), K(tenant_id), K(table_schema->get_database_id()));
        } else if (OB_ISNULL(database_schema) || database_schema->is_in_recyclebin()) {
          // skip
        } else if (OB_FAIL(cur_tenant_table_ids_.push_back(table_schema->get_table_id()))) {
          SERVER_LOG(WARN, "fail to push back table_id", KR(ret), K(table_schema->get_table_id()));
        }
      }
    }
  }

  return ret;
}

int ObAllVirtualDynamicPartitionTable::build_new_row_(
  const int64_t tenant_schema_version,
  const ObTableSchema &table_schema,
  common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (OB_ISNULL(cells) || OB_ISNULL(allocator_) || OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells or allocator or schema service is null", KR(ret));
  } else if (tenant_idx_ >= tenant_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tenant idx out of bound", KR(ret));
  } else {
    const uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
    const int64_t col_count = output_column_ids_.count();
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch (col_id) {
        case TENANT_ID: {
          cells[cell_idx].set_int(tenant_id);
          break;
        }
        case TENANT_SCHEMA_VERSION: {
          cells[cell_idx].set_int(tenant_schema_version);
          break;
        }
        case DATABASE_NAME: {
          ObSchemaGetterGuard schema_guard;
          const ObDatabaseSchema *database_schema = NULL;
          if (OB_FAIL(schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
            SERVER_LOG(WARN, "fail to get tenant schema guard", KR(ret), K(tenant_id));
          } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, table_schema.get_database_id(), database_schema))) {
            SERVER_LOG(WARN, "fail to get database schema", KR(ret), K(table_schema.get_database_id()));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "database schema is null", KR(ret));
          } else {
            const ObString &tmp_database_name = database_schema->get_database_name_str();
            ObString database_name;
            if (OB_FAIL(ob_write_string(*allocator_, tmp_database_name, database_name))) {
              SERVER_LOG(WARN, "fail to write string", KR(ret), K(tmp_database_name));
            } else {
              cells[cell_idx].set_varchar(database_name);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          }
          break;
        }
        case TABLE_NAME: {
          const ObString &tmp_table_name = table_schema.get_table_name_str();
          ObString table_name;
          if (OB_FAIL(ob_write_string(*allocator_, tmp_table_name, table_name))) {
            SERVER_LOG(WARN, "fail to write string", KR(ret), K(tmp_table_name));
          } else {
            cells[cell_idx].set_varchar(table_name);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case TABLE_ID: {
          const uint64_t table_id = table_schema.get_table_id();
          cells[cell_idx].set_int(table_id);
          break;
        }
        case MAX_HIGH_BOUND_VAL: {
          bool is_oracle_mode = false;
          const ObPartition *max_part = NULL;
          if (OB_UNLIKELY(table_schema.get_partition_num() <= 0)) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "part num is invalid", KR(ret));
          } else if (OB_FAIL(share::ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id, is_oracle_mode))) {
            SERVER_LOG(WARN, "fail to check is oracle mode with tenant id", KR(ret), K(tenant_id));
          } else if (OB_ISNULL(max_part = table_schema.get_part_array()[table_schema.get_partition_num() - 1])) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "max part is null", KR(ret));
          } else {
            // same as __all_part, set tz offset to 0
            ObTimeZoneInfo tz_info;
            tz_info.set_offset(0);
            const ObRowkey &max_high_bound_val = max_part->get_high_bound_val();
            char *buf = NULL;
            int64_t pos = 0;
            const int64_t high_bound_val_length = OB_MAX_B_HIGH_BOUND_VAL_LENGTH;
            if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(high_bound_val_length)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SERVER_LOG(WARN, "allocate memory failed", KR(ret));
            } else if (OB_FAIL(ObPartitionUtils::convert_rowkey_to_sql_literal(is_oracle_mode,
                                                                               max_high_bound_val,
                                                                               buf,
                                                                               high_bound_val_length,
                                                                               pos,
                                                                               false/*print_collation*/,
                                                                               &tz_info))) {
              SERVER_LOG(WARN, "Failed to convert rowkey to sql text", KR(ret), K(max_high_bound_val));
            } else {
              ObString max_high_bound_val_str;
              max_high_bound_val_str.assign_ptr(buf, pos);
              cells[cell_idx].set_varchar(max_high_bound_val_str);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
          }
          break;
        }
        case ENABLE: {
          bool enable = false;
          if (OB_FAIL(share::ObDynamicPartitionManager::get_enable(table_schema, enable))) {
            SERVER_LOG(WARN, "fail to get dynamic partition enable", KR(ret));
          } else {
            cells[cell_idx].set_varchar(enable ? "TRUE" : "FALSE");
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case TIME_UNIT: {
          ObDateUnitType time_unit = ObDateUnitType::DATE_UNIT_MAX;
          if (OB_FAIL(share::ObDynamicPartitionManager::get_time_unit(table_schema, time_unit))) {
            SERVER_LOG(WARN, "fail to get dynamic partition time unit", KR(ret));
          } else {
            cells[cell_idx].set_varchar(ob_date_unit_type_str_upper(time_unit));
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case PRECREATE_TIME: {
          int64_t num = 0;
          ObDateUnitType time_unit = ObDateUnitType::DATE_UNIT_MAX;
          char *buf = NULL;
          const int64_t time_length = 128;
          int64_t pos = 0;
          if (OB_FAIL(share::ObDynamicPartitionManager::get_precreate_time(table_schema, num, time_unit))) {
            SERVER_LOG(WARN, "fail to get dynamic partition precreate time", KR(ret));
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(time_length)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "allocate memory failed", KR(ret));
          } else if (databuff_printf(buf, time_length, pos, "%ld%s", num, num > 0 ? ob_date_unit_type_str_upper(time_unit) : "")) {
            SERVER_LOG(WARN, "fail to do databuff printf", KR(ret));
          } else {
            ObString precreate_time_str;
            precreate_time_str.assign_ptr(buf, pos);
            cells[cell_idx].set_varchar(precreate_time_str);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case EXPIRE_TIME: {
          int64_t num = 0;
          ObDateUnitType time_unit = ObDateUnitType::DATE_UNIT_MAX;
          char *buf = NULL;
          const int64_t time_length = 128;
          int64_t pos = 0;
          if (OB_FAIL(share::ObDynamicPartitionManager::get_expire_time(table_schema, num, time_unit))) {
            SERVER_LOG(WARN, "fail to get dynamic partition expire time", KR(ret));
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(time_length)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            SERVER_LOG(WARN, "allocate memory failed", KR(ret));
          } else if (databuff_printf(buf, time_length, pos, "%ld%s", num, num > 0 ? ob_date_unit_type_str_upper(time_unit) : "")) {
            SERVER_LOG(WARN, "fail to do databuff printf", KR(ret));
          } else {
            ObString expire_time_str;
            expire_time_str.assign_ptr(buf, pos);
            cells[cell_idx].set_varchar(expire_time_str);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case TIME_ZONE: {
          ObString tmp_time_zone;
          ObString time_zone;
          if (OB_FAIL(share::ObDynamicPartitionManager::get_time_zone(table_schema, tmp_time_zone))) {
            SERVER_LOG(WARN, "fail to get dynamic partition time zone", KR(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, tmp_time_zone, time_zone))) {
            SERVER_LOG(WARN, "fail to write string", KR(ret), K(tmp_time_zone));
          } else {
            cells[cell_idx].set_varchar(time_zone);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case BIGINT_PRECISION: {
          ObString tmp_bigint_precision;
          ObString bigint_precision;
          if (OB_FAIL(share::ObDynamicPartitionManager::get_bigint_precision(table_schema, tmp_bigint_precision))) {
            SERVER_LOG(WARN, "fail to get dynamic partition bigint precision", KR(ret));
          } else if (OB_FAIL(ob_write_string(*allocator_, tmp_bigint_precision, bigint_precision))) {
            SERVER_LOG(WARN, "fail to write string", KR(ret), K(tmp_bigint_precision));
          } else {
            cells[cell_idx].set_varchar(bigint_precision);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", KR(ret), K(cell_idx), K(output_column_ids_), K(col_id));
          break;
        }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }

  return ret;
}

}//namespace observer
}//namespace oceanbase
