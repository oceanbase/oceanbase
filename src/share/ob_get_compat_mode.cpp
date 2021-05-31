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
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_isql_connection_pool.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_get_compat_mode.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_tenant_config_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share::schema;

ObCompatModeGetter::ObCompatModeGetter() : id_mode_map_(), sql_proxy_(NULL), is_inited_(false)
{}
ObCompatModeGetter::~ObCompatModeGetter()
{
  destroy();
}

ObCompatModeGetter& ObCompatModeGetter::instance()
{
  static ObCompatModeGetter the_compat_mode_getter;
  return the_compat_mode_getter;
}

int ObCompatModeGetter::get_tenant_mode(const uint64_t tenant_id, ObWorker::CompatMode& mode)
{
  return instance().get_tenant_compat_mode(tenant_id, mode);
}

int ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(const uint64_t tenant_id, bool& is_oracle_mode)
{
  int ret = OB_SUCCESS;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::INVALID;

  if (OB_FAIL(instance().get_tenant_compat_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode", K(ret));
  } else if (ObWorker::CompatMode::ORACLE == compat_mode) {
    is_oracle_mode = true;
  } else if (ObWorker::CompatMode::MYSQL == compat_mode) {
    is_oracle_mode = false;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compat_mode should not be INVALID.", K(ret));
  }

  return ret;
}

int ObCompatModeGetter::init(common::ObMySQLProxy* proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(id_mode_map_.create(
                 bucket_num, ObModIds::OB_HASH_BUCKET_TENANT_COMPAT_MODE, ObModIds::OB_HASH_NODE_TENANT_COMPAT_MODE))) {
    LOG_WARN("create hash table failed", K(ret));
  } else {
    sql_proxy_ = proxy;
    is_inited_ = true;
  }
  return ret;
}

void ObCompatModeGetter::destroy()
{
  if (IS_INIT) {
    id_mode_map_.destroy();
  }
}

int ObCompatModeGetter::get_tenant_compat_mode(const uint64_t tenant_id, ObWorker::CompatMode& mode)
{
  int ret = OB_SUCCESS;
  ObCompatibilityMode tmp_mode = ObCompatibilityMode::MYSQL_MODE;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", K(ret), K(tenant_id), K(lbt()));
  } else if (OB_SYS_TENANT_ID == tenant_id) {
    mode = ObWorker::CompatMode::MYSQL;
  } else if (OB_GTS_TENANT_ID == tenant_id) {
    mode = ObWorker::CompatMode::MYSQL;
  } else if (OB_HASH_NOT_EXIST == (ret = id_mode_map_.get_refactored(tenant_id, mode))) {
    bool is_exist = false;
    int overwrite = 1;
    // First take from the local persistent tenant information
    if (OB_FAIL(storage::ObTenantConfigMgr::get_instance().get_compat_mode(tenant_id, mode))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("failed to get compat mode", K(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      is_exist = true;
      ret = id_mode_map_.set_refactored(tenant_id, mode, overwrite);
    }
    // cant get it locally, go to the schema to get it
    if (OB_SUCC(ret) && !is_exist) {
      if (OB_ISNULL(sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sql_proxy is null", K(ret), K(lbt()));
      } else if (OB_FAIL(ObSchemaServiceSQLImpl::fetch_tenant_compat_mode(*sql_proxy_, tenant_id, tmp_mode))) {
        LOG_WARN("failed to fetch tenant compat mode", K(ret), K(tenant_id), K(lbt()));
      } else {
        if (tmp_mode == ObCompatibilityMode::MYSQL_MODE) {
          mode = ObWorker::CompatMode::MYSQL;
          ret = id_mode_map_.set_refactored(tenant_id, mode, overwrite);
        } else if (tmp_mode == ObCompatibilityMode::ORACLE_MODE) {
          mode = ObWorker::CompatMode::ORACLE;
          ret = id_mode_map_.set_refactored(tenant_id, mode, overwrite);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected compat mode", K(tmp_mode), K(ret), K(lbt()));
        }
      }
    }
  }
  return ret;
}

// only for unittest
int ObCompatModeGetter::set_tenant_compat_mode(const uint64_t tenant_id, ObWorker::CompatMode& mode)
{
  int ret = OB_SUCCESS;
  int overwrite = 1;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id is invalid", K(ret), K(tenant_id));
  } else if (ObWorker::CompatMode::MYSQL != mode && ObWorker::CompatMode::ORACLE != mode) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant compat mode", K(ret), K(tenant_id), K(mode));
  } else if (OB_FAIL(id_mode_map_.set_refactored(tenant_id, mode, overwrite))) {
    LOG_WARN("fail to set tenant compat mode", K(ret), K(tenant_id), K(mode));
  }
  return ret;
}

int ObCompatModeGetter::reset_compat_getter_map()
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compat mode geter do not init, can not reset map", K(ret));
  } else if (OB_FAIL(id_mode_map_.reuse())) {
    LOG_WARN("fail to reuse id mode map", K(ret));
  }
  return ret;
}
