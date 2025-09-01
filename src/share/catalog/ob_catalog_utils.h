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

#ifndef OB_CATALOG_UTILS_H
#define OB_CATALOG_UTILS_H

#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace sql
{
class ObBasicSessionInfo;
}
namespace share
{

class ObILakeTableMetadata;

class ObCatalogUtils
{
public:
  // 使用的名字来源于 sql，没有处理过
  static bool is_internal_catalog_name(const common::ObString &name_from_sql, const ObNameCaseMode &case_mode);
  // 使用的名字来源于 CatalogSchema，名字大小写已经转换好了
  static bool is_internal_catalog_name(const common::ObString &name_from_meta);

  template <typename T>
  static typename std::enable_if_t<std::is_base_of_v<ObILakeTableMetadata, T>, int>
  deep_copy_lake_table_metadata(char *buf, const T &old_var, T *&new_var);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCatalogUtils);
};

class ObSwitchCatalogHelper
{
public:
  ObSwitchCatalogHelper()
    : old_catalog_id_(OB_INVALID_ID),
      old_db_id_(OB_INVALID_ID),
      old_database_name_(),
      session_info_(nullptr)
  {}
  int set(uint64_t catalog_id,
          uint64_t db_id,
          const common::ObString& database_name,
          sql::ObBasicSessionInfo* session_info);
  int restore();
  bool is_set() { return OB_INVALID_ID != old_catalog_id_; }
private:
  uint64_t old_catalog_id_;
  uint64_t old_db_id_;
  common::ObSqlString old_database_name_;
  sql::ObBasicSessionInfo* session_info_;
  DISALLOW_COPY_AND_ASSIGN(ObSwitchCatalogHelper);

};

} // namespace share
} // namespace oceanbase

#endif // OB_CATALOG_UTILS_H
