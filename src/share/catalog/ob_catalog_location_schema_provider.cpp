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

#include "share/catalog/ob_catalog_location_schema_provider.h"

#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{

namespace share
{

ObCatalogLocationSchemaProvider::ObCatalogLocationSchemaProvider(
    schema::ObSchemaGetterGuard &schema_guard)
    : schema_guard_(schema_guard)
{
}

int ObCatalogLocationSchemaProvider::get_access_info_by_path(ObIAllocator &allocator,
                                                             const uint64_t tenant_id,
                                                             const common::ObString &access_path,
                                                             common::ObString &access_info,
                                                             uint64_t &location_id,
                                                             common::ObString &sub_path) const
{
  int ret = OB_SUCCESS;
  access_info.reset();
  location_id = OB_INVALID_ID;
  sub_path.reset();
  const schema::ObLocationSchema *location_schema = NULL;
  sql::ObSQLSessionInfo *session = THIS_WORKER.get_session();
  schema::ObSessionPrivInfo session_priv;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null session", K(ret));
  } else if (OB_FAIL(session->get_session_priv_info(session_priv))) {
    LOG_WARN("get session priv failed", K(ret));
  } else if (OB_FAIL(schema_guard_.get_location_schema_by_prefix_match_with_priv(session_priv,
                                                                                 session->get_enable_role_array(),
                                                                                 tenant_id,
                                                                                 access_path,
                                                                                 location_schema,
                                                                                 false))) {
    LOG_WARN("get location schema failed", K(ret));
  } else if (NULL == location_schema) {
    // do nothing
  } else {
    location_id = location_schema->get_location_id();
    const ObString &location_url = location_schema->get_location_url_str();
    if (access_path.prefix_match(location_url)) {
      int64_t url_len = location_url.length();
      int64_t path_len = access_path.length();
      if (path_len >= url_len) {
        ObString tmp_sub_path(path_len - url_len, access_path.ptr() + url_len);
        if (OB_FAIL(ob_write_string(allocator, tmp_sub_path, sub_path, true))) {
          LOG_WARN("failed to deep copy sub path", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, location_schema->get_location_access_info_str(), access_info, true))) {
          LOG_WARN("failed to deep copy access info", K(ret));
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
