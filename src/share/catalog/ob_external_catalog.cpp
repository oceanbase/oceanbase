/**
 * Copyright (c) 2023 OceanBase
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
#include "share/catalog/ob_external_catalog.h"

#include "share/schema/ob_column_schema.h"
#include "share/stat/ob_opt_external_table_stat.h"
#include "share/stat/ob_opt_external_column_stat.h"
#include "sql/ob_sql_context.h"
namespace oceanbase
{
namespace share
{

int ObIExternalCatalog::init(const uint64_t tenant_id,
                             const uint64_t catalog_id,
                             const common::ObString &properties,
                             const ObCatalogLocationSchemaProvider *location_schema_provider)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == catalog_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid catalog id", K(ret), K(catalog_id));
  } else {
    tenant_id_ = tenant_id;
    catalog_id_ = catalog_id;
    location_schema_provider_ = location_schema_provider;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(do_init(properties))) {
    LOG_WARN("failed to init properties", K(ret), K(properties));
  }
  return ret;
}

int ObILakeTableMetadata::init(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               const uint64_t database_id,
                               const uint64_t table_id,
                               const ObString &namespace_name,
                               const ObString &table_name,
                               const ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  tenant_id_ = tenant_id;
  catalog_id_ = catalog_id;
  database_id_ = database_id;
  table_id_ = table_id;
  OX(case_mode_ = case_mode);
  OZ(ob_write_string(allocator_, namespace_name, namespace_name_));
  OZ(ob_write_string(allocator_, table_name, table_name_));
  return ret;
}

int ObILakeTableMetadata::assign(const ObILakeTableMetadata &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    tenant_id_ = other.tenant_id_;
    catalog_id_ = other.catalog_id_;
    database_id_ = other.database_id_;
    table_id_ = other.table_id_;
    case_mode_ = other.case_mode_;
    lake_table_metadata_version_ = other.lake_table_metadata_version_;
    OZ(ob_write_string(allocator_, other.namespace_name_, namespace_name_));
    OZ(ob_write_string(allocator_, other.table_name_, table_name_));
  }
  return ret;
}

int ObILakeTableMetadata::build_table_schema(std::optional<int32_t> schema_id,
                                             std::optional<int64_t> snapshot_id,
                                             share::schema::ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_build_table_schema(schema_id, snapshot_id, table_schema))) {
    LOG_WARN("do_build_table_schema failed", K(ret));
  } else {
    // 因为 table_schema 可能是从 ObKVCache 里面拿出来的
    // 此时里面的 database_id 和 table_id 可能是以前 sql 查询临时生成的 id
    // 所以这里我们要对其进行覆写
    table_schema->set_tenant_id(tenant_id_);
    table_schema->set_database_id(database_id_);
    table_schema->set_table_id(table_id_);
    table_schema->set_catalog_id(catalog_id_);
    table_schema->set_table_type(schema::EXTERNAL_TABLE);
    table_schema->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    table_schema->set_charset_type(CHARSET_UTF8MB4);
    // append mocked pk column
    uint64_t COL_IDS[2] = {OB_HIDDEN_FILE_ID_COLUMN_ID, OB_HIDDEN_LINE_NUMBER_COLUMN_ID};
    const char *COL_NAMES[2] = {OB_HIDDEN_FILE_ID_COLUMN_NAME, OB_HIDDEN_LINE_NUMBER_COLUMN_NAME};
    for (int i = 0; OB_SUCC(ret) && i < array_elements(COL_IDS); i++) {
      schema::ObColumnSchemaV2 hidden_pk;
      hidden_pk.reset();
      hidden_pk.set_column_id(COL_IDS[i]);
      hidden_pk.set_data_type(ObIntType);
      hidden_pk.set_nullable(false);
      hidden_pk.set_is_hidden(true);
      hidden_pk.set_charset_type(CHARSET_BINARY);
      hidden_pk.set_collation_type(CS_TYPE_BINARY);
      if (OB_FAIL(hidden_pk.set_column_name(COL_NAMES[i]))) {
        LOG_WARN("failed to set column name", K(ret));
      } else {
        hidden_pk.set_rowkey_position(i + 1);
        if (OB_FAIL(table_schema->add_column(hidden_pk))) {
          LOG_WARN("add column to table_schema failed", K(ret), K(hidden_pk));
        }
      }
    }
    table_schema->set_table_pk_exists_mode(schema::ObTablePrimaryKeyExistsMode::TOM_TABLE_WITHOUT_PK);
  }
  return ret;
}

int ObIExternalCatalog::fetch_table_statistics(
    ObIAllocator &allocator,
    sql::ObSqlSchemaGuard &sql_schema_guard,
    const ObILakeTableMetadata *table_metadata,
    const ObIArray<ObString> &partition_values,
    const ObIArray<ObString> &column_names,
    ObOptExternalTableStat *&external_table_stat,
    ObIArray<ObOptExternalColumnStat *> &external_table_column_stats)
{
  return OB_NOT_SUPPORTED;
}

int ObIExternalCatalog::fetch_partitions(ObIAllocator &allocator,
                                         const ObILakeTableMetadata *table_metadata,
                                         Partitions &partitions)
{
  return OB_NOT_SUPPORTED;
}

int ObIExternalCatalog::get_cache_refresh_interval_sec(int64_t &sec)
{
  return OB_NOT_SUPPORTED;
}

int ObILakeTableMetadata::resolve_time_travel_info(const ObTimeTravelInfo *time_travel_info,
                                                   std::optional<int32_t> &schema_id,
                                                   std::optional<int64_t> &snapshot_id)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(time_travel_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("time_travel_info is null", K(ret));
  } else {
    switch (time_travel_info->type_) {
      case ObTimeTravelInfo::TimeTravelType::NONE: {
        // 默认行为：不支持 time travel 的表格式总是使用当前数据
        snapshot_id = std::nullopt;
        schema_id = std::nullopt;
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported time travel query type", K(ret), K(time_travel_info->type_));
        break;
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase
