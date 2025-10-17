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
#include "share/catalog/ob_catalog_utils.h"

#include "lib/worker.h"
#include "sql/session/ob_basic_session_info.h"

namespace oceanbase
{
namespace share
{

bool ObCatalogUtils::is_internal_catalog_name(const common::ObString &name_from_sql, const ObNameCaseMode &case_mode)
{
  bool is_internal = false;
  if (lib::is_oracle_mode()) {
    is_internal = (name_from_sql.compare(OB_INTERNAL_CATALOG_NAME_UPPER) == 0);
  } else if (OB_ORIGIN_AND_SENSITIVE == case_mode) {
    is_internal = (name_from_sql.compare(OB_INTERNAL_CATALOG_NAME) == 0);
  } else {
    is_internal = (name_from_sql.case_compare(OB_INTERNAL_CATALOG_NAME) == 0);
  }
  return is_internal;
}

bool ObCatalogUtils::is_internal_catalog_name(const common::ObString &name_from_meta)
{
  return lib::is_oracle_mode() ? (name_from_meta.compare(OB_INTERNAL_CATALOG_NAME_UPPER) == 0)
                               : (name_from_meta.compare(OB_INTERNAL_CATALOG_NAME) == 0);
}

template <typename T>
typename std::enable_if_t<std::is_base_of_v<ObILakeTableMetadata, T>, int>
ObCatalogUtils::deep_copy_lake_table_metadata(char *buf, const T &old_var, T *&new_var)
{
  int ret = OB_SUCCESS;
  new_var = NULL;

  if (NULL == buf) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf));
  } else {
    int64_t size = old_var.get_convert_size() + sizeof(common::ObDataBuffer);
    common::ObDataBuffer *data_buf = new (buf + sizeof(old_var))
        common::ObDataBuffer(buf + sizeof(old_var) + sizeof(common::ObDataBuffer),
                             size - sizeof(old_var) - sizeof(common::ObDataBuffer));
    new_var = new (buf) T(*data_buf);
    if (OB_FAIL(copy_assign(*new_var, old_var))) {
      LOG_WARN("fail to assign lake table metadata", K(ret));
    }
  }
  return ret;
}

int ObSwitchCatalogHelper::set(uint64_t catalog_id,
                               uint64_t db_id,
                               const common::ObString& database_name,
                               sql::ObBasicSessionInfo* session_info) {
  int ret = OB_SUCCESS;
  old_catalog_id_ = catalog_id;
  old_db_id_ = db_id;
  session_info_ = session_info;
  OZ(old_database_name_.assign(database_name));
  return ret;
}

int ObSwitchCatalogHelper::restore() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(session_info_->set_default_catalog_db(old_catalog_id_,
                                                           old_db_id_,
                                                           old_database_name_.string()))) {
    LOG_WARN("failed to restore catalog and db", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase