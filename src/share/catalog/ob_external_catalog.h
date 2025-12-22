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

#ifndef __SHARE_OB_EXTERNAL_CATALOG_H__
#define __SHARE_OB_EXTERNAL_CATALOG_H__
#include "lib/allocator/ob_allocator.h"
#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_time_travel_info.h"
#include "share/schema/ob_table_schema.h"

#include <optional>
#include <share/catalog/hive/thrift/gen_cpp/ThriftHiveMetastore.h>

namespace oceanbase
{
namespace sql
{
class ObSqlSchemaGuard;
} // namespace sql
namespace share
{

using Partitions = std::vector<Apache::Hadoop::Hive::Partition>;

class ObCatalogLocationSchemaProvider;
class ObOptExternalTableStat;
class ObOptExternalColumnStat;

class ObILakeTableMetadata
{
public:
  explicit ObILakeTableMetadata(ObIAllocator &allocator) : allocator_(allocator) {
  }
  virtual ~ObILakeTableMetadata() = default;
  virtual ObLakeTableFormat get_format_type() const = 0;
  virtual int64_t get_convert_size() const = 0;
  int init(const uint64_t tenant_id,
           const uint64_t catalog_id,
           const uint64_t database_id,
           const uint64_t table_id,
           const ObString &namespace_name,
           const ObString &table_name,
           const ObNameCaseMode case_mode);
  int assign(const ObILakeTableMetadata &other);
  int build_table_schema(std::optional<int32_t> schema_id,
                         std::optional<int64_t> snapshot_id,
                         share::schema::ObTableSchema *&table_schema);
  virtual int resolve_time_travel_info(const ObTimeTravelInfo *time_travel_info,
                                       std::optional<int32_t> &schema_id,
                                       std::optional<int64_t> &snapshot_id);
  TO_STRING_KV(K_(tenant_id),
               K_(catalog_id),
               K_(database_id),
               K_(table_id),
               K_(namespace_name),
               K_(table_name),
               K_(case_mode));

  uint64_t tenant_id_;
  uint64_t catalog_id_;
  uint64_t database_id_;
  uint64_t table_id_;
  ObString namespace_name_;
  ObString table_name_;
  ObNameCaseMode case_mode_;
  int64_t lake_table_metadata_version_ = 0; // used to identify cached lake table metadata version
protected:
  virtual int do_build_table_schema(std::optional<int32_t> schema_id,
                                    std::optional<int64_t> snapshot_id,
                                    share::schema::ObTableSchema *&table_schema) = 0;
  ObIAllocator &allocator_;
};

class ObIExternalCatalog
{
public:
  virtual ~ObIExternalCatalog() = default;
  int init(const uint64_t tenant_id,
           const uint64_t catalog_id,
           const common::ObString &properties,
           const ObCatalogLocationSchemaProvider *location_schema_provider = NULL);
  virtual int list_namespace_names(common::ObIArray<common::ObString> &ns_names) = 0;
  virtual int list_table_names(const common::ObString &db_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tbl_names) = 0;
  // if namespace not found, return OB_ERR_BAD_DATABASE
  virtual int fetch_namespace_schema(const uint64_t database_id,
                                     const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema *&database_schema) = 0;
  // if table not found, return OB_TABLE_NOT_EXIST
  // must use allocator from function's parameter to create ObILakeTableMetadata*
  virtual int fetch_lake_table_metadata(ObIAllocator &allocator,
                                        const uint64_t database_id,
                                        const uint64_t table_id,
                                        const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        ObILakeTableMetadata *&table_metadata) = 0;

  // 如果 partition_values 为空，则代表获取所有分区列的统计信息
  // 如果 column_names 为空，则代表获取所有列的统计信息
  // 如果 partition_values 为空，table_stat 返回的是整个表的统计信息。如果 partition_values 有值，则返回分区合并后的值
  // column_stats 返回的是合并后的列统计信息。
  virtual int fetch_table_statistics(ObIAllocator &allocator,
                                     sql::ObSqlSchemaGuard &sql_schema_guard,
                                     const ObILakeTableMetadata *table_metadata,
                                     const ObIArray<ObString> &partition_values,
                                     const ObIArray<ObString> &column_names,
                                     ObOptExternalTableStat *&external_table_stat,
                                     ObIArray<ObOptExternalColumnStat *> &external_table_column_stats);

  virtual int fetch_partitions(ObIAllocator &allocator,
                               const ObILakeTableMetadata *table_metadata,
                               Partitions &partitions);

  // if table not found, return OB_TABLE_NOT_EXIST
  virtual int fetch_latest_table_schema_version(const common::ObString &ns_name,
                                                const common::ObString &tbl_name,
                                                const ObNameCaseMode case_mode,
                                                int64_t &schema_version) = 0;

  virtual int get_cache_refresh_interval_sec(int64_t &sec);

protected:
  virtual int do_init(const common::ObString &properties) = 0;

protected:
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  const ObCatalogLocationSchemaProvider *location_schema_provider_ = NULL;
};

class ObICatalogMetaGetter
{
public:
  virtual ~ObICatalogMetaGetter() = default;
  virtual int list_namespace_names(const uint64_t tenant_id,
                                   const uint64_t catalog_id,
                                   common::ObIArray<common::ObString> &ns_names) = 0;
  virtual int list_table_names(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               const common::ObString &ns_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tbl_names) = 0;
  virtual int fetch_namespace_schema(const uint64_t tenant_id,
                                     const uint64_t catalog_id,
                                     const uint64_t database_id,
                                     const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema *&database_schema) = 0;
  virtual int fetch_lake_table_metadata(ObIAllocator &allocator,
                                        const uint64_t tenant_id,
                                        const uint64_t catalog_id,
                                        const uint64_t database_id,
                                        const uint64_t table_id,
                                        const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        ObILakeTableMetadata *&table_metadata) = 0;

  virtual int fetch_table_statistics(ObIAllocator &allocator,
                                     sql::ObSqlSchemaGuard &sql_schema_guard,
                                     const ObILakeTableMetadata *table_metadata,
                                     const ObIArray<ObString> &partition_values,
                                     const ObIArray<ObString> &column_names,
                                     ObOptExternalTableStat *&external_table_stat,
                                     ObIArray<ObOptExternalColumnStat *> &external_table_column_stats) = 0;

  virtual int fetch_partitions(ObIAllocator &allocator,
                               const ObILakeTableMetadata *table_metadata,
                               Partitions &partitions) = 0;

  virtual int get_cache_refresh_interval_sec(const ObILakeTableMetadata *table_metadata,
                                             int64_t &sec)
      = 0;
};

class ObIExternalTableMetadataOperations
{
  virtual int current(const ObString &ns_name,
                      const ObString &tbl_name,
                      ObILakeTableMetadata *&metadata) = 0;
  virtual int commit(const ObString &ns_name,
                     const ObString &table_name,
                     const ObILakeTableMetadata *previous,
                     const ObILakeTableMetadata *latest) = 0;
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_EXTERNAL_CATALOG_H__
