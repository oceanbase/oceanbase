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

#include "share/ob_max_id_fetcher.h"

#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "common/ob_zone.h"
#include "common/object/ob_object.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_schema_status_proxy.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
namespace share {
using namespace share::schema;

const char* ObMaxIdFetcher::max_id_name_info_[OB_MAX_ID_TYPE][2] = {{"ob_max_used_table_id", "max used table id"},
    {"ob_max_used_tenant_id", "max used tenant id"},
    {"ob_max_used_database_id", "max used database id"},
    {"ob_max_used_tablegroup_id", "max used tablegroup id"},
    {"ob_max_used_user_id", "max used user id"},
    {"ob_max_used_unit_config_id", "max used unit config id"},
    {"ob_max_used_unit_id", "max used unit id"},
    {"ob_max_used_resource_pool_id", "max used resource pool id"},
    {"ob_max_used_sequence_id", "max used sequence id"},
    {"ob_max_used_server_id", "max used server id"},
    {"ob_max_used_outline_id", "max used outline id"},
    {"ob_max_used_constraint_id", "max used constraint id"},
    {"ob_max_used_synonym_id", "max used synonym id"},
    {"ob_max_used_plan_baseline_id", "max used plan_baseline id"},
    {"ob_max_sql_execution_id", "max global sql execution id"},
    {"ob_max_used_udf_id", "max used udf id"},
    {"ob_max_used_profile_id", "max used profile id"},
    {"ob_max_used_ha_gts_id", "max used ha gts id"},
    {"ob_max_used_dblink_id", "max used dblink id"}};

ObMaxIdFetcher::ObMaxIdFetcher(ObMySQLProxy& proxy) : proxy_(proxy)
{}

ObMaxIdFetcher::~ObMaxIdFetcher()
{}

int ObMaxIdFetcher::fetch_new_max_id(
    const uint64_t tenant_id, const ObMaxIdType max_id_type, uint64_t& id, const uint64_t initial /* = UINT64_MAX */)
{
  int ret = OB_SUCCESS;
  uint64_t fetch_id = 0;
  bool need_update = false;
  ObMySQLTransaction trans;
  if (OB_INVALID_ID == tenant_id || !valid_max_id_type(max_id_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(max_id_type));
  } else if (OB_FAIL(trans.start(&proxy_))) {
    LOG_WARN("fail to to start transaction", K(ret));
  } else if (OB_FAIL(fetch_max_id(trans, tenant_id, max_id_type, fetch_id))) {
    if (OB_ENTRY_NOT_EXIST == ret && UINT64_MAX != initial) {
      if (OB_FAIL(insert_initial_value(trans, tenant_id, max_id_type, initial))) {
        LOG_WARN("init initial value failed", K(ret));
      } else if (OB_FAIL(fetch_max_id(trans, tenant_id, max_id_type, fetch_id))) {
        LOG_WARN("failed to get max id", K(ret), K(tenant_id), K(max_id_type));
      }
    } else {
      LOG_WARN("failed to get max id", K(ret), K(tenant_id), K(max_id_type));
    }
  }
  if (OB_SUCC(ret)) {
    fetch_id++;
    if (max_id_type >= OB_MAX_USED_TABLE_ID_TYPE && max_id_type < OB_MAX_ID_TYPE) {
      // After 2.0, the lower 40 bits of tablegroup_id are 1
      if (OB_MAX_USED_TABLEGROUP_ID_TYPE == max_id_type && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2000) {
        fetch_id = fetch_id | OB_TABLEGROUP_MASK;
      }
      // update max_id when:
      //  - id is invalid
      //  - id is valid and id>=fetch_id
      if (OB_INVALID_ID == id) {
        id = fetch_id;
        need_update = true;
      } else if (id >= fetch_id) {
        need_update = true;
      }
      if (OB_MAX_USED_TABLE_ID_TYPE == max_id_type && is_new_tablegroup_id(id)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_ERROR("table id reach max", K(ret), K(id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid schema type", K(ret), K(max_id_type));
    }

    if (tenant_id != extract_tenant_id(id)) {
      switch (max_id_type) {
        case OB_MAX_USED_TABLE_ID_TYPE:
        case OB_MAX_USED_DATABASE_ID_TYPE:
        case OB_MAX_USED_TABLEGROUP_ID_TYPE:
        case OB_MAX_USED_USER_ID_TYPE:
        case OB_MAX_USED_OUTLINE_ID_TYPE:
        case OB_MAX_USED_SYNONYM_ID_TYPE:
        case OB_MAX_USED_SEQUENCE_ID_TYPE:
        case OB_MAX_USED_PLAN_BASELINE_ID_TYPE:
        case OB_MAX_USED_UDF_ID_TYPE: {
          ret = OB_SIZE_OVERFLOW;
          LOG_ERROR("id reach max", K(ret), K(id), K(max_id_type));
          break;
        }
        default: {
          // do nothing
        }
      }
    }

    if (OB_FAIL(ret)) {
      // skip
    } else if (need_update) {
      if (OB_FAIL(update_max_id(trans, tenant_id, max_id_type, id))) {
        LOG_WARN("failed to update max id", K(ret), K(tenant_id), K(max_id_type), K(id));
      }
    }
  }

  if (trans.is_started()) {
    const bool is_commit = (OB_SUCC(ret));
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(is_commit))) {
      LOG_WARN("failed to end trans", K(is_commit), K(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }
  }
  return ret;
}

int ObMaxIdFetcher::update_max_id(
    ObISQLClient& sql_client, const uint64_t tenant_id, ObMaxIdType max_id_type, const uint64_t max_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObZone zone;
  int64_t affected_rows = 0L;
  const char* id_name = NULL;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_INVALID_ID == tenant_id || !valid_max_id_type(max_id_type) || OB_INVALID_ID == max_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(max_id_type), K(max_id));
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't write sys table now", K(ret), K(tenant_id));
  } else if (OB_ISNULL(id_name = get_max_id_name(max_id_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL name", K(ret));
  } else if (OB_FAIL(sql.append_fmt("UPDATE %s SET VALUE = '%lu', gmt_modified = now(6) "
                                    "WHERE ZONE = '%s' AND NAME = '%s' AND TENANT_ID = %lu",
                 OB_ALL_SYS_STAT_TNAME,
                 ObSchemaUtils::get_extract_schema_id(exec_tenant_id, max_id),
                 zone.ptr(),
                 id_name,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("sql_string append format string failed", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("sql client write fail", K(sql), K(affected_rows), K(ret));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("unexpected affected row", K(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObMaxIdFetcher::fetch_max_id(
    ObISQLClient& sql_client, const uint64_t tenant_id, ObMaxIdType max_id_type, uint64_t& max_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObZone zone;
  const char* id_name = NULL;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_INVALID_ID == tenant_id || !valid_max_id_type(max_id_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(max_id_type));
  } else if (OB_ISNULL(id_name = get_max_id_name(max_id_type))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL name", K(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT VALUE FROM %s WHERE ZONE = '%s' AND NAME = '%s' AND TENANT_ID = %lu "
                                    "FOR UPDATE",
                 OB_ALL_SYS_STAT_TNAME,
                 zone.ptr(),
                 id_name,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))) {
    LOG_WARN("sql append format string failed", K(ret));
  } else {
    ObSQLClientRetryWeak sql_client_retry_weak(&sql_client, exec_tenant_id, OB_ALL_SYS_STAT_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult* result = NULL;
      ObString id_str;
      if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (OB_SUCCESS == (ret = result->next())) {
        if (OB_FAIL(result->get_varchar(static_cast<int64_t>(0), id_str))) {
          LOG_WARN("fail to get id as int value.", K(ret));
          result->print_info();
        } else if (OB_FAIL(str_to_uint(id_str, max_id))) {
          LOG_WARN("str_to_uint failed", K(id_str), K(ret));
          // The fetch_id of ordinary tenants may be 0, which requires special treatment
        } else {
          if (OB_SYS_TENANT_ID != exec_tenant_id && static_cast<int64_t>(max_id) >= 0) {
            max_id = combine_id(exec_tenant_id, max_id);
          }
          if (OB_ITER_END != (ret = result->next())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is more than one row", K(ret));
          } else {
            ret = OB_SUCCESS;
          }
        }
      } else {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("fail to get id", "name", id_name, K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMaxIdFetcher::insert_initial_value(
    common::ObISQLClient& sql_client, uint64_t tenant_id, ObMaxIdType max_id_type, const uint64_t initial_value)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObZone zone;
  ObObj obj;
  obj.set_int(static_cast<int64_t>(initial_value));
  int64_t affected_rows = 0;
  const char* name = get_max_id_name(max_id_type);
  const char* info = get_max_id_info(max_id_type);
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t value = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, initial_value);
  if (OB_INVALID_ID == tenant_id || !valid_max_id_type(max_id_type) || UINT64_MAX == initial_value) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(max_id_type), K(initial_value));
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("can't write sys table now", K(ret), K(tenant_id));
  } else if (OB_ISNULL(name) || OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL name or info", K(ret), KP(name), KP(info));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
                                    "(tenant_id, zone, name, data_type, value, info) VALUES "
                                    "(%lu, '%s', '%s', '%d', '%ld', '%s') ON DUPLICATE KEY UPDATE value = value",
                 OB_ALL_SYS_STAT_TNAME,
                 ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                 zone.ptr(),
                 name,
                 obj.get_type(),
                 static_cast<int64_t>(value),
                 info))) {
    LOG_WARN("sql string assign failed", K(ret));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(ret));
  }
  return ret;
}

const char* ObMaxIdFetcher::get_max_id_name(const ObMaxIdType max_id_type)
{
  const char* name = NULL;
  if (max_id_type < 0 || max_id_type >= ARRAYSIZEOF(max_id_name_info_)) {
    LOG_WARN("invalid argument", K(max_id_type), "array size", ARRAYSIZEOF(max_id_name_info_));
  } else {
    name = max_id_name_info_[max_id_type][0];
  }
  return name;
}

const char* ObMaxIdFetcher::get_max_id_info(const ObMaxIdType max_id_type)
{
  const char* info = NULL;
  if (max_id_type < 0 || max_id_type >= ARRAYSIZEOF(max_id_name_info_)) {
    LOG_WARN("invalid argument", K(max_id_type), "array size", ARRAYSIZEOF(max_id_name_info_));
  } else {
    info = max_id_name_info_[max_id_type][1];
  }
  return info;
}

int ObMaxIdFetcher::str_to_uint(const ObString& str, uint64_t& value)
{
  int ret = OB_SUCCESS;
  int64_t int_value = 0;
  char buf[OB_MAX_CONFIG_VALUE_LEN];
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str));
  } else {
    int n = snprintf(buf, OB_MAX_CONFIG_VALUE_LEN, "%.*s", str.length(), str.ptr());
    if (n < 0 || n >= OB_MAX_CONFIG_VALUE_LEN) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("id_buf is not long enough", K(ret), K(n), LITERAL_K(OB_MAX_CONFIG_VALUE_LEN));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t base = 10;
    int_value = strtol(buf, NULL, base);
    if (LONG_MAX == int_value || LONG_MIN == int_value) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("convert str to int failed", K(buf), K(ret));
    } else {
      value = static_cast<uint64_t>(int_value);
    }
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
