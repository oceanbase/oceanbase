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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_schema_getter_guard.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "lib/net/ob_net_util.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/ob_schema_status_proxy.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "share/catalog/ob_catalog_utils.h"
namespace oceanbase
{
using namespace common;
using namespace observer;

namespace share
{
namespace schema
{
int ObSchemaGetterGuard::get_sensitive_rule_schema_count(const uint64_t tenant_id, int64_t &count)
{
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sensitive_rule_mgr_.get_schema_count(count))) {
    LOG_WARN("get schema count failed", KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sensitive_rule_schema_by_name(const uint64_t tenant_id,
                                                           const common::ObString &name,
                                                           const ObSensitiveRuleSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(name), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), K(tenant_id));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  } else if (OB_FAIL(mgr->sensitive_rule_mgr_.get_schema_by_name(tenant_id, mode, name, schema))) {
    LOG_WARN("get schema failed", K(name), KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sensitive_rule_schema_by_id(const uint64_t tenant_id,
                                                         const uint64_t sensitive_rule_id,
                                                         const ObSensitiveRuleSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id) || !is_valid_id(sensitive_rule_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(sensitive_rule_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sensitive_rule_mgr_.get_schema_by_id(sensitive_rule_id, schema))) {
    LOG_WARN("get schema failed", K(sensitive_rule_id), KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sensitive_rule_schema_by_column(const uint64_t tenant_id,
                                                             const uint64_t table_id,
                                                             const uint64_t column_id,
                                                             const ObSensitiveRuleSchema *&schema)
{
  int ret = OB_SUCCESS;
  schema = nullptr;
  const ObSchemaMgr *mgr = NULL;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !is_valid_id(table_id) || !is_valid_id(column_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(table_id), K(column_id), KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->sensitive_rule_mgr_.get_schema_by_column(tenant_id, table_id, column_id, schema))) {
    LOG_WARN("get sensitive rule schema by column failed", K(table_id), K(column_id), KR(ret));
  }
  return ret;
}

int ObSchemaGetterGuard::get_sensitive_rule_schemas_by_table(const ObTableSchema &table_schema,
                                                             ObIArray<const ObSensitiveRuleSchema *> &schemas)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = table_schema.get_tenant_id();
  const ObSchemaMgr *mgr = NULL;
  ObSEArray<uint64_t, 8> column_ids;
  if (!check_inner_stat()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner stat error", KR(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id),KR(ret));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id), K_(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(table_schema.get_column_ids(column_ids))) {
    LOG_WARN("get column ids failed", KR(ret), K(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      const ObSensitiveRuleSchema *schema = NULL;
      if (OB_FAIL(get_sensitive_rule_schema_by_column(tenant_id, table_schema.get_table_id(), column_ids.at(i), schema))) {
        LOG_WARN("get sensitive rule schema by column failed", KR(ret), K(table_schema.get_table_id()), K(column_ids.at(i)));
      } else if (NULL == schema) {
      } else if (OB_FAIL(add_var_to_array_no_dup(schemas, schema))) {
        LOG_WARN("push back schema failed", KR(ret), K(schema));
      }
    }
  }
  return ret;
}

}
}
}