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
#include "sql/engine/connector/ob_odps_catalog_jni_agent.h"

namespace oceanbase
{
namespace share
{

class ObOdpsCatalog final : public ObIExternalCatalog
{
public:
  explicit ObOdpsCatalog(common::ObIAllocator &allocator) : allocator_(allocator) {}
  ~ObOdpsCatalog() = default;
  virtual int init(const common::ObString &properties) override;
  virtual int list_namespace_names(common::ObIArray<common::ObString> &ns_names) override;
  virtual int list_table_names(const common::ObString &ns_name,
                               const ObNameCaseMode case_mode,
                               common::ObIArray<common::ObString> &tb_names) override;
  virtual int fetch_namespace_schema(const common::ObString &ns_name,
                                     const ObNameCaseMode case_mode,
                                     share::schema::ObDatabaseSchema &database_schema) override;
  virtual int fetch_table_schema(const common::ObString &ns_name,
                                 const common::ObString &tb_name,
                                 const ObNameCaseMode case_mode,
                                 share::schema::ObTableSchema &table_schema) override;
  virtual int fetch_basic_table_info(const common::ObString &ns_name,
                                     const common::ObString &tbl_name,
                                     const ObNameCaseMode case_mode,
                                     ObCatalogBasicTableInfo &table_info) override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOdpsCatalog);
  int convert_odps_format_to_str_properties_(const ObODPSGeneralFormat &odps_format, ObString &str);

  common::ObIAllocator &allocator_;
  ObODPSCatalogProperties properties_;
#ifdef OB_BUILD_CPP_ODPS
  apsara::odps::sdk::Configuration conf_;
  apsara::odps::sdk::IODPSPtr odps_;
  apsara::odps::sdk::IODPSTablesPtr tables_;
#endif
#ifdef OB_BUILD_JNI_ODPS
  JNICatalogPtr jni_catalog_ptr_;
#endif
};

} // namespace share
} // namespace oceanbase

#endif // _SHARE_OB_ODPS_CATALOG_H
#endif