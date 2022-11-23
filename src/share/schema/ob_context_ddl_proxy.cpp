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
#include "share/schema/ob_context_ddl_proxy.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "rootserver/ob_ddl_operator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObContextDDLProxy::ObContextDDLProxy(ObMultiVersionSchemaService &schema_service)
    : schema_service_(schema_service)
{
}

ObContextDDLProxy::~ObContextDDLProxy()
{
}

int ObContextDDLProxy::create_context(
    ObContextSchema &ctx_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const bool or_replace,
    const bool obj_exist,
    const share::schema::ObContextSchema *old_schema,
    bool &need_clean,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  if (or_replace) {
    if (OB_FAIL(create_or_replace_context(ctx_schema, trans, schema_guard, obj_exist,
                                          old_schema, need_clean, ddl_stmt_str))) {
      LOG_WARN("failed to create or replace context", K(ret), K(ctx_schema), K(obj_exist));
    }
  } else if (obj_exist) {
    ret = OB_ERR_EXIST_OBJECT;
    LOG_WARN("Name is already used by an existing object", K(ret), K(ctx_schema));
  } else if (OB_FAIL(inner_create_context(ctx_schema, trans, schema_guard,
                                          ddl_stmt_str))) {
    LOG_WARN("fail inner create context", K(ctx_schema), K(ret));
  }
  return ret;
}

int ObContextDDLProxy::inner_create_context(
    ObContextSchema &ctx_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ctx_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t new_context_id = OB_INVALID_ID;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service->fetch_new_context_id(tenant_id, new_context_id))) {
    LOG_WARN("failed to fetch new_context_id", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ctx_schema.set_schema_version(new_schema_version);
    ctx_schema.set_context_id(new_context_id);
    if (OB_FAIL(schema_service->get_context_sql_service().insert_context(
                ctx_schema, &trans, ddl_stmt_str))) {
      LOG_WARN("insert context info failed", K(ctx_schema.get_namespace()), K(ret));
    }
  }
  return ret;
}

int ObContextDDLProxy::drop_context(
    share::schema::ObContextSchema &ctx_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const share::schema::ObContextSchema *old_schema,
    bool &need_clean,
    const common::ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = ctx_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t context_id = OB_INVALID_ID;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_ISNULL(old_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get old schema", K(ret));
  } else if (OB_INVALID_ID != old_schema->get_context_id()) {
    ctx_schema.set_context_id(old_schema->get_context_id());
    ctx_schema.set_context_type(old_schema->get_context_type());
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get context id", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_service->get_context_sql_service().drop_context(
              ctx_schema, new_schema_version, &trans, need_clean, ddl_stmt_str))) {
    LOG_WARN("drop context info failed", K(ctx_schema.get_namespace()), K(ret));
  }
  return ret;
}

int ObContextDDLProxy::create_or_replace_context(
    ObContextSchema &ctx_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const bool obj_exist,
    const ObContextSchema *old_schema,
    bool &need_clean,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  bool is_replace = false;
  uint64_t new_context_id = OB_INVALID_ID;
  need_clean = false;
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (obj_exist) {
    ObContextSchema tmp_schema;
    if (OB_ISNULL(old_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get context id", K(ret));
    } else if (OB_FAIL(tmp_schema.assign(*old_schema))) {
      LOG_WARN("failed to assign old schema", K(ret));
    } else if (old_schema->get_context_type() != ctx_schema.get_context_type()) {
      if (OB_FAIL(drop_context(tmp_schema, trans, schema_guard, old_schema,
                               need_clean, nullptr))) {
        LOG_WARN("failed to drop old context", K(ret), K(*old_schema));
      } else if (OB_FAIL(inner_create_context(ctx_schema, trans, schema_guard, ddl_stmt_str))) {
        LOG_WARN("failed to create new context", K(ret), K(ctx_schema));
      }
    } else if (FALSE_IT(ctx_schema.set_context_id(old_schema->get_context_id()))) {
    } else if (OB_FAIL(inner_alter_context(ctx_schema, trans, schema_guard, ddl_stmt_str))) {
      LOG_WARN("failed to inner alter context", K(ctx_schema), K(ret));
    }
  } else if (OB_FAIL(inner_create_context(ctx_schema, trans, schema_guard,
                                          ddl_stmt_str))) {
    LOG_WARN("fail inner create context", K(ctx_schema), K(ret));
  }
  return ret;
}

int ObContextDDLProxy::inner_alter_context(
    ObContextSchema &ctx_schema,
    common::ObMySQLTransaction &trans,
    share::schema::ObSchemaGetterGuard &schema_guard,
    const ObString *ddl_stmt_str)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = ctx_schema.get_tenant_id();
  int64_t new_schema_version = OB_INVALID_VERSION;
  ObSchemaService *schema_service = schema_service_.get_schema_service();
  uint64_t new_context_id = OB_INVALID_ID;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema_service must not null", K(ret));
  } else if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id, new_schema_version))) {
    LOG_WARN("fail to gen new schema_version", K(ret), K(tenant_id));
  } else {
    ctx_schema.set_schema_version(new_schema_version);
    if (OB_FAIL(schema_service->get_context_sql_service().alter_context(
                ctx_schema, &trans, ddl_stmt_str))) {
      LOG_WARN("alter context failed", K(ctx_schema.get_namespace()), K(ret));
    }
  }
  return ret;
}