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
#include "share/external_table/ob_external_table_file_mgr.h"

namespace oceanbase
{
namespace share
{

struct ObCatalogBasicTableInfo
{
  // unix time, precision is second
  int64_t create_time_s = 0;
  int64_t last_ddl_time_s = 0;
  int64_t last_modification_time_s = 0;
};

// ObIExternalCatalog don't care about tenant_id, database_id, table_id
class ObIExternalCatalog
{
public:
  virtual ~ObIExternalCatalog() = default;
  virtual int init(const common::ObString &properties) = 0;
  virtual int list_namespace_names(common::ObIArray<common::ObString> &ns_names) = 0;
  virtual int list_table_names(const common::ObString &db_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tbl_names) = 0;
  // if namespace not found, return OB_ERR_BAD_DATABASE
  virtual int fetch_namespace_schema(const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema &database_schema) = 0;
  // if table not found, return OB_TABLE_NOT_EXIST
  virtual int fetch_table_schema(const common::ObString &ns_name,
                                 const common::ObString &tbl_name,
                                 const ObNameCaseMode case_mode,
                                 share::schema::ObTableSchema &table_schema) = 0;
  // if table not found, return OB_TABLE_NOT_EXIST
  virtual int fetch_basic_table_info(const common::ObString &ns_name,
                                     const common::ObString &tbl_name,
                                     const ObNameCaseMode case_mode,
                                     ObCatalogBasicTableInfo &table_info) = 0;
};

class ObICatalogMetaGetter
{
public:
  virtual ~ObICatalogMetaGetter() = default;
  virtual int list_namespace_names(const uint64_t tenant_id, const uint64_t catalog_id, common::ObIArray<common::ObString> &ns_names) = 0;
  virtual int list_table_names(const uint64_t tenant_id,
                               const uint64_t catalog_id,
                               const common::ObString &ns_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tbl_names) = 0;
  // database_schema's database_id should assign correct before call this function
  virtual int fetch_namespace_schema(const uint64_t tenant_id,
                                     const uint64_t catalog_id,
                                     const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema &database_schema) = 0;
  // table_schema's table_id/database_id should assign correct before call this function
  virtual int fetch_table_schema(const uint64_t tenant_id,
                                 const uint64_t catalog_id,
                                 const common::ObString &ns_name,
                                 const common::ObString &tbl_name,
                                 const ObNameCaseMode case_mode,
                                 share::schema::ObTableSchema &table_schema) = 0;
};

} // namespace share
} // namespace oceanbase

#endif // __SHARE_OB_EXTERNAL_CATALOG_H__
