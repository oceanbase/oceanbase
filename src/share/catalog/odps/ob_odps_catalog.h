#ifdef OB_BUILD_CPP_ODPS
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
#ifndef _SHARE_OB_ODPS_CATALOG_H
#define _SHARE_OB_ODPS_CATALOG_H

#include "share/catalog/ob_catalog_properties.h"
#include "share/catalog/ob_external_catalog.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/connector/ob_odps_catalog_jni_agent.h"
#include <odps/odps_api.h>

namespace oceanbase
{
namespace share
{

class ObOdpsCatalog final : public ObIExternalCatalog
{
public:
  explicit ObOdpsCatalog(common::ObIAllocator &allocator) : allocator_(allocator) {}
  ~ObOdpsCatalog() = default;
  int list_namespace_names(common::ObIArray<common::ObString> &ns_names) override;
  int list_table_names(const common::ObString &ns_name,
                       const ObNameCaseMode case_mode,
                       common::ObIArray<common::ObString> &tb_names) override;
  int fetch_namespace_schema(const uint64_t database_id,
                             const common::ObString &ns_name,
                             const ObNameCaseMode case_mode,
                             share::schema::ObDatabaseSchema *&database_schema) override;
  int fetch_latest_table_schema_version(const common::ObString &ns_name,
                                        const common::ObString &tbl_name,
                                        const ObNameCaseMode case_mode,
                                        int64_t &schema_version) override;
  int fetch_lake_table_metadata(ObIAllocator &allocator,
                                const uint64_t database_id,
                                const uint64_t table_id,
                                const common::ObString &ns_name,
                                const common::ObString &tbl_name,
                                const ObNameCaseMode case_mode,
                                ObILakeTableMetadata *&table_metadata) override;
  int fetch_table_statistics(ObIAllocator &allocator,
                             const ObILakeTableMetadata *table_metadata,
                             const ObIArray<ObString> &partition_values,
                             const ObIArray<ObString> &column_names,
                             ObOptExternalTableStat *&external_table_stat,
                             ObIArray<ObOptExternalColumnStat *> &external_table_column_stats) override;

private:
  virtual int do_init(const common::ObString &properties) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOdpsCatalog);
  int convert_odps_format_to_str_properties_(const sql::ObODPSGeneralFormat &odps_format, ObString &str);

  common::ObIAllocator &allocator_;
  ObODPSCatalogProperties properties_;
#ifdef OB_BUILD_CPP_ODPS
  apsara::odps::sdk::Configuration conf_;
  apsara::odps::sdk::IODPSPtr odps_;
  apsara::odps::sdk::IODPSTablesPtr tables_;
#endif
#ifdef OB_BUILD_JNI_ODPS
  sql::JNICatalogPtr jni_catalog_ptr_;
#endif
};

} // namespace share
} // namespace oceanbase

#endif // _SHARE_OB_ODPS_CATALOG_H
#endif
