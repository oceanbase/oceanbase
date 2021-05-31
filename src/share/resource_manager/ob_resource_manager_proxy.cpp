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
#include "ob_resource_manager_proxy.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_errno.h"
#include "share/schema/ob_schema_utils.h"
#include "share/resource_manager/ob_resource_plan_manager.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "common/ob_timeout_ctx.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

// Automatically do trans start and commit
ObResourceManagerProxy::TransGuard::TransGuard(ObMySQLTransaction& trans, int& ret) : trans_(trans), ret_(ret)
{
  ret_ = trans_.start(GCTX.sql_proxy_, true);
  if (OB_SUCCESS != ret_) {
    LOG_WARN("fail start trans", K_(ret));
  }
}

bool ObResourceManagerProxy::TransGuard::ready()
{
  return OB_SUCCESS == ret_;
}

ObResourceManagerProxy::TransGuard::~TransGuard()
{
  if (trans_.is_started()) {
    bool is_commit = (OB_SUCCESS == ret_);
    int trans_ret = trans_.end(is_commit);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("fail commit/rollback trans", K_(ret), K(is_commit), K(trans_ret));
    }
    if (OB_SUCCESS == ret_) {
      ret_ = trans_ret;
    }
  }
}

ObResourceManagerProxy::ObResourceManagerProxy()
{}

ObResourceManagerProxy::~ObResourceManagerProxy()
{}

int ObResourceManagerProxy::create_plan(uint64_t tenant_id, const ObString& plan, const ObObj& comments)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_PLAN_TNAME;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, plan.ptr(), plan.length(), "plan");
      if (OB_SUCC(ret) && comments.is_varchar()) {
        const ObString& c = comments.get_string();
        SQL_COL_APPEND_STR_VALUE(sql, values, c.ptr(), c.length(), "comments");
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)", static_cast<int32_t>(values.length()), values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
          trans.reset_last_error();
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_ERR_RES_OBJ_ALREADY_EXIST;
            LOG_USER_ERROR(OB_ERR_RES_OBJ_ALREADY_EXIST, "resource plan", plan.length(), plan.ptr());
            LOG_WARN("Concurrent call create plan or plan already exist", K(ret), K(tname), K(plan), K(comments));
          } else {
            LOG_WARN("fail to execute sql", K(sql), K(ret));
          }
        } else {
          if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value. expect only 1 row affected", K(affected_rows), K(sql), K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && try_init_default_consumer_groups(trans, tenant_id)) {
      LOG_WARN("fail init default consumer groups", K(tenant_id), K(ret));
    }
  }
  return ret;
}

// Compatible with upgrade, upgrade from the old system, without SYS_GROUP and OTHER_GROUPS
int ObResourceManagerProxy::try_init_default_consumer_groups(ObMySQLTransaction& trans, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObResourceManagerProxy proxy;
  ObObj comments;
  bool consumer_group_exist = true;
  for (int i = 0; i < ObPlanDirective::INTERNAL_GROUP_NAME_COUNT && OB_SUCC(ret); ++i) {
    const ObString& group_name = ObPlanDirective::INTERNAL_GROUP_NAME[i];
    if (OB_FAIL(check_if_consumer_group_exist(trans, tenant_id, group_name, consumer_group_exist))) {
      LOG_WARN("fail check consumer group exist", K(tenant_id), K(group_name), K(ret));
    } else if (!consumer_group_exist &&
               OB_FAIL(proxy.create_consumer_group(trans, tenant_id, group_name, comments, i))) {
      LOG_WARN("fail create default consumer group", K(group_name), K(i), K(tenant_id), K(ret));
    }
  }
  LOG_INFO("init default tenant cgroups OK", K(ret), K(tenant_id));
  return ret;
}

int ObResourceManagerProxy::delete_plan(uint64_t tenant_id, const common::ObString& plan)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    // When deleting plan, delete directive in cascade
    const char* tname_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    const char* tname_plan = OB_ALL_RES_MGR_PLAN_TNAME;
    if (OB_FAIL(sql.assign_fmt("DELETE /* REMOVE_RES_PLAN */ FROM %s "
                               "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
            tname_plan,
            ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
            plan.length(),
            plan.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_RES_PLAN_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_RES_PLAN_NOT_EXIST, plan.length(), plan.ptr());
    } else if (OB_FAIL(sql.assign_fmt("DELETE /* REMOVE_RES_PLAN */ FROM %s "
                                      "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
                   tname_directive,
                   ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                   plan.length(),
                   plan.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    }
  }
  return ret;
}

// @private
int ObResourceManagerProxy::allocate_consumer_group_id(ObMySQLTransaction& trans, uint64_t tenant_id, int64_t& group_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_CONSUMER_GROUP_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
    if (OB_FAIL(sql.assign_fmt(
            "SELECT /* ALLOC_MAX_GROUP_ID */ COALESCE(MAX(CONSUMER_GROUP_ID) + 1, 1) AS NEXT_GROUP_ID FROM %s "
            "WHERE TENANT_ID = %ld",
            tname,
            ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
      LOG_WARN("fail format sql", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
    } else if (OB_SUCCESS != (ret = result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
      }
    } else {
      // Get the next available group id and assign it to the new consumer group
      EXTRACT_INT_FIELD_MYSQL(*result, "NEXT_GROUP_ID", group_id, int64_t);
    }
  }
  return ret;
}

int ObResourceManagerProxy::create_consumer_group(
    uint64_t tenant_id, const ObString& consumer_group, const ObObj& comments)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    ret = create_consumer_group(trans, tenant_id, consumer_group, comments);
  }
  return ret;
}

int ObResourceManagerProxy::create_consumer_group(ObMySQLTransaction& trans, uint64_t tenant_id,
    const ObString& consumer_group, const ObObj& comments, int64_t consumer_group_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char* tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
  // If id >=0, use the externally passed id
  if (consumer_group_id < 0 && OB_FAIL(allocate_consumer_group_id(trans, tenant_id, consumer_group_id))) {
    LOG_WARN("fail alloc group id", K(tenant_id), K(ret));
  } else if (OB_FAIL(sql.assign_fmt("INSERT /* CREATE_CONSUMER_GROUP */ INTO %s (", tname))) {
    STORAGE_LOG(WARN, "append table name failed, ", K(ret));
  } else {
    ObSqlString values;
    SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
    SQL_COL_APPEND_VALUE(sql, values, consumer_group_id, "consumer_group_id", "%lu");
    SQL_COL_APPEND_STR_VALUE(sql, values, consumer_group.ptr(), consumer_group.length(), "consumer_group");
    if (OB_SUCC(ret) && comments.is_varchar()) {
      const ObString& c = comments.get_string();
      SQL_COL_APPEND_STR_VALUE(sql, values, c.ptr(), c.length(), "comments");
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)", static_cast<int32_t>(values.length()), values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
        trans.reset_last_error();
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_ERR_RES_OBJ_ALREADY_EXIST;
          LOG_USER_ERROR(
              OB_ERR_RES_OBJ_ALREADY_EXIST, "resource consumer group", consumer_group.length(), consumer_group.ptr());
          LOG_WARN("Concurrent call create consumer_group or consumer_group already exist",
              K(ret),
              K(tname),
              K(consumer_group),
              K(comments));
        } else {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        }
      } else {
        if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value. expect only 1 row affected", K(affected_rows), K(sql), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::delete_consumer_group(uint64_t tenant_id, const common::ObString& consumer_group)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    const char* tname_consumer_group = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
    if (ObPlanDirective::is_internal_group_name(consumer_group)) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "delete internal consumer group");
    } else if (OB_FAIL(sql.assign_fmt("DELETE /* REMOVE_RES_CONSUMER_GROUP */ FROM %s "
                                      "WHERE TENANT_ID = %ld AND CONSUMER_GROUP = '%.*s'",
                   tname_consumer_group,
                   ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                   consumer_group.length(),
                   consumer_group.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_CONSUMER_GROUP_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_CONSUMER_GROUP_NOT_EXIST, consumer_group.length(), consumer_group.ptr());
    }
  }
  return ret;
}

int ObResourceManagerProxy::create_plan_directive(uint64_t tenant_id, const common::ObString& plan,
    const common::ObString& group, const common::ObObj& comment, const common::ObObj& mgmt_p1,
    const common::ObObj& utilization_limit)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    ret = create_plan_directive(trans, tenant_id, plan, group, comment, mgmt_p1, utilization_limit);
  }
  return ret;
}

int ObResourceManagerProxy::create_plan_directive(common::ObMySQLTransaction& trans, uint64_t tenant_id,
    const ObString& plan, const ObString& group, const ObObj& comments, const ObObj& mgmt_p1,
    const ObObj& utilization_limit)
{
  int ret = OB_SUCCESS;
  bool consumer_group_exist = true;
  if (OB_FAIL(check_if_consumer_group_exist(trans, tenant_id, group, consumer_group_exist))) {
    LOG_WARN("fail check consumer group exist", K(tenant_id), K(group), K(ret));
  } else if (!consumer_group_exist) {
    ret = OB_ERR_CONSUMER_GROUP_NOT_EXIST;
    LOG_USER_ERROR(OB_ERR_CONSUMER_GROUP_NOT_EXIST, group.length(), group.ptr());
  } else {
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      int64_t v = 0;
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, plan.ptr(), plan.length(), "plan");
      SQL_COL_APPEND_STR_VALUE(sql, values, group.ptr(), group.length(), "group_or_subplan");
      if (OB_SUCC(ret) && comments.is_varchar()) {
        const ObString& c = comments.get_string();
        SQL_COL_APPEND_STR_VALUE(sql, values, c.ptr(), c.length(), "comments");
      }
      if (OB_SUCC(ret) && OB_SUCC(get_percentage("MGMT_P1", mgmt_p1, v))) {
        SQL_COL_APPEND_VALUE(sql, values, v, "MGMT_P1", "%ld");
      }
      if (OB_SUCC(ret) && OB_SUCC(get_percentage("UTILIZATION_LIMIT", utilization_limit, v))) {
        SQL_COL_APPEND_VALUE(sql, values, v, "UTILIZATION_LIMIT", "%ld");
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)", static_cast<int32_t>(values.length()), values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
          trans.reset_last_error();
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST;
            LOG_USER_ERROR(OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST, plan.length(), plan.ptr(), group.length(), group.ptr());
            LOG_WARN("Concurrent call create plan or plan already exist", K(ret), K(tname), K(plan), K(comments));
          } else {
            LOG_WARN("fail to execute sql", K(sql), K(ret));
          }
        } else {
          if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value. expect only 1 row affected", K(affected_rows), K(sql), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::check_if_consumer_group_exist(
    ObMySQLTransaction& trans, uint64_t tenant_id, const ObString& group, bool& exist)
{
  int ret = OB_SUCCESS;
  exist = true;
  ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_CONSUMER_GROUP_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
    if (OB_FAIL(sql.assign_fmt("SELECT /* CHECK_IF_RES_CONSUMER_GROUP_EXIST */ * FROM %s "
                               "WHERE TENANT_ID = %ld AND CONSUMER_GROUP = '%.*s'",
            tname,
            ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
            group.length(),
            group.ptr()))) {
      LOG_WARN("fail format sql", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
    } else if (OB_SUCCESS != (ret = result->next())) {
      if (OB_ITER_END == ret) {
        exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
      }
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObResourceManagerProxy::check_if_plan_directive_exist(
    ObMySQLTransaction& trans, uint64_t tenant_id, const ObString& plan, const ObString& group, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_DIRECTIVE_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    if (OB_FAIL(sql.assign_fmt("SELECT /* CHECK_IF_RES_PLAN_DIRECTIVE_EXIST */ * FROM %s "
                               "WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
            tname,
            ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
            plan.length(),
            plan.ptr(),
            group.length(),
            group.ptr()))) {
      LOG_WARN("fail format sql", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
    } else if (OB_SUCCESS != (ret = result->next())) {
      if (OB_ITER_END == ret) {
        exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
      }
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObResourceManagerProxy::check_if_plan_exist(
    ObMySQLTransaction& trans, uint64_t tenant_id, const ObString& plan, bool& exist)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_PLAN_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_PLAN_TNAME;
    if (OB_FAIL(sql.assign_fmt("SELECT /* CHECK_IF_RES_PLAN_EXIST */ * FROM %s "
                               "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
            tname,
            ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
            plan.length(),
            plan.ptr()))) {
      LOG_WARN("fail format sql", K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
    } else if (OB_SUCCESS != (ret = result->next())) {
      if (OB_ITER_END == ret) {
        exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
      }
    } else {
      exist = true;
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_percentage(const char* name, const ObObj& obj, int64_t& v)
{
  int ret = OB_SUCCESS;
  if (obj.is_int()) {
    v = obj.get_int();
  } else if (obj.is_number()) {
    if (OB_FAIL(obj.get_number().extract_valid_int64_with_trunc(v))) {
      LOG_WARN("fail get value from number", K(obj), K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect a valid number value", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (0 > v || 100 < v) {
      ret = OB_ERR_PERCENTAGE_OUT_OF_RANGE;
      LOG_USER_ERROR(OB_ERR_PERCENTAGE_OUT_OF_RANGE, v, name);
    }
  }
  return ret;
}

int ObResourceManagerProxy::update_plan_directive(uint64_t tenant_id, const ObString& plan, const ObString& group,
    const ObObj& comments, const ObObj& mgmt_p1, const ObObj& utilization_limit)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    const char* comma = "";
    bool exist = false;
    if (OB_FAIL(check_if_plan_directive_exist(trans, tenant_id, plan, group, exist))) {
      LOG_WARN("fail check if plan exist", K(tenant_id), K(plan), K(group), K(ret));
    } else if (!exist) {
      ret = OB_ERR_PLAN_DIRECTIVE_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_PLAN_DIRECTIVE_NOT_EXIST, plan.length(), plan.ptr(), group.length(), group.ptr());
    } else if (comments.is_null() && mgmt_p1.is_null() && utilization_limit.is_null()) {
      // No valid parameters are specified, do nothing, compatible with oracle
      ret = OB_SUCCESS;
    } else if (OB_FAIL(sql.assign_fmt("UPDATE /* UPDATE_PLAN_DIRECTIVE */ %s SET ", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      int64_t v = 0;
      if (OB_SUCC(ret) && comments.is_varchar()) {
        const ObString& c = comments.get_varchar();
        ret = sql.append_fmt("COMMENT='%.*s'", static_cast<int32_t>(c.length()), c.ptr());
        comma = ",";
      }
      if (OB_SUCC(ret) && OB_SUCC(get_percentage("NEW_MGMT_P1", mgmt_p1, v))) {
        ret = sql.append_fmt("%s MGMT_P1=%ld", comma, v);
        comma = ",";
      }
      if (OB_SUCC(ret) && OB_SUCC(get_percentage("NEW_UTILIZATION_LIMIT", utilization_limit, v))) {
        ret = sql.append_fmt("%s UTILIZATION_LIMIT=%ld", comma, v);
        comma = ",";
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail append value", K(ret));
      } else if (OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
                     ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                     plan.length(),
                     plan.ptr(),
                     group.length(),
                     group.ptr()))) {
        LOG_WARN("fail append value", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
        trans.reset_last_error();
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (affected_rows > 1) {
        // Note: The affected_rows of the update statement may be 0 or 1 row
        // if the value entered in update is exactly the same as the old value
        // it is 0 rows, which means nothing has been changed
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected row value not expected", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::delete_plan_directive(uint64_t tenant_id, const ObString& plan, const ObString& group)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    const char* tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    bool exist = false;
    if (OB_FAIL(check_if_plan_directive_exist(trans, tenant_id, plan, group, exist))) {
      LOG_WARN("fail check if plan exist", K(tenant_id), K(plan), K(group), K(ret));
    } else if (!exist) {
      ret = OB_ERR_PLAN_DIRECTIVE_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_PLAN_DIRECTIVE_NOT_EXIST, plan.length(), plan.ptr(), group.length(), group.ptr());
    } else if (OB_FAIL(sql.assign_fmt("DELETE /* REMOVE_PLAN_DIRECTIVE */ FROM %s "
                                      "WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
                   tname,
                   ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                   plan.length(),
                   plan.ptr(),
                   group.length(),
                   group.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (affected_rows != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected row value not expected", K(affected_rows), K(ret));
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_all_plan_directives(
    uint64_t tenant_id, const ObString& plan, ObIArray<ObPlanDirective>& directives)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    bool plan_exist = false;
    if (OB_FAIL(check_if_plan_exist(trans, tenant_id, plan, plan_exist))) {
      LOG_WARN("fail check plan exist", K(tenant_id), K(plan), K(ret));
    } else if (!plan_exist) {
      // skip
    } else {
      ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_DIRECTIVE_TID);
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        common::sqlclient::ObMySQLResult* result = NULL;
        ObSqlString sql;
        const char* tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
        if (OB_FAIL(sql.assign_fmt("SELECT /* GET_ALL_PLAN_DIRECTIVE_SQL */ "
                                   "group_or_subplan group_name, mgmt_p1, utilization_limit FROM %s "
                                   "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
                tname,
                ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                plan.length(),
                plan.ptr()))) {
          LOG_WARN("fail format sql", K(ret));
        } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
        } else {
          while (OB_SUCC(result->next())) {
            ObPlanDirective directive;
            directive.set_tenant_id(tenant_id);
            EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, group_name, directive);
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, mgmt_p1, directive, int64_t);
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, utilization_limit, directive, int64_t);
            if (OB_SUCC(ret) && OB_FAIL(directives.push_back(directive))) {
              LOG_WARN("fail push back directives", K(directive), K(ret));
            }
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

/** attribute: entity type
 * value: The entity may be user name or function name, which is determined by attribute
 * consumer_group: value corresponds to the consumer group to which the entity belongs
 */
int ObResourceManagerProxy::replace_mapping_rule(
    uint64_t tenant_id, const ObString& attribute, const ObString& value, const ObString& consumer_group)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    if (!consumer_group.empty()) {
      bool consumer_group_exist = true;
      if (OB_FAIL(check_if_consumer_group_exist(trans, tenant_id, consumer_group, consumer_group_exist))) {
        LOG_WARN("fail check if consumer group exist", K(tenant_id), K(consumer_group), K(ret));
      } else if (!consumer_group_exist) {
        ret = OB_ERR_CONSUMER_GROUP_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_INVALID_PLAN_DIRECTIVE_NAME, consumer_group.length(), consumer_group.ptr());
      }
    }
    if (OB_SUCC(ret)) {
      ObSqlString sql;
      const char* tname = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s (", tname))) {
        STORAGE_LOG(WARN, "append table name failed, ", K(ret));
      } else {
        ObSqlString values;
        SQL_COL_APPEND_VALUE(
            sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
        SQL_COL_APPEND_STR_VALUE(sql, values, attribute.ptr(), attribute.length(), "attribute");
        SQL_COL_APPEND_STR_VALUE(sql, values, value.ptr(), value.length(), "value");
        SQL_COL_APPEND_STR_VALUE(sql, values, consumer_group.ptr(), consumer_group.length(), "consumer_group");
        if (OB_SUCC(ret)) {
          int64_t affected_rows = 0;
          if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)", static_cast<int32_t>(values.length()), values.ptr()))) {
            LOG_WARN("append sql failed, ", K(ret));
          } else if (OB_FAIL(trans.write(tenant_id, sql.ptr(), affected_rows))) {
            trans.reset_last_error();
            LOG_WARN("fail to execute sql", K(sql), K(ret));
          } else {
            if (is_single_row(affected_rows) || is_double_row(affected_rows)) {
              // insert or replace
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected value. expect 1 or 2 row affected", K(affected_rows), K(sql), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_all_resource_mapping_rules(
    uint64_t tenant_id, common::ObIArray<ObResourceMappingRule>& rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      ObSqlString sql;
      const char* tname = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      if (OB_FAIL(sql.assign_fmt("SELECT /* GET_ALL_RES_MAPPING_RULE */ "
                                 "attribute attr, `value`, consumer_group `group` FROM %s "
                                 "WHERE TENANT_ID = %ld",
              tname,
              ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail format sql", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObResourceMappingRule rule;
          rule.set_tenant_id(tenant_id);
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, attr, rule);
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, value, rule);
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, group, rule);
          if (OB_SUCC(ret) && OB_FAIL(rules.push_back(rule))) {
            LOG_WARN("fail push back rules", K(rule), K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_all_resource_mapping_rules_by_user(
    uint64_t tenant_id, const common::ObString& plan, common::ObIArray<ObResourceUserMappingRule>& rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(&trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      common::sqlclient::ObMySQLResult* result = NULL;
      ObSqlString sql;
      const char* t_a_res_name = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      const char* t_b_user_name = OB_ALL_USER_TNAME;
      const char* t_c_consumer_group_name = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
      const char* t_d_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
      uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      // When the resource plan is empty, or the corresponding directive does not exist
      // the default group OTHER_GROUPS is used
      if (OB_FAIL(sql.assign_fmt(
              "SELECT /* GET_ALL_RES_MAPPING_RULE_BY_USER */ "
              "user_id, "
              "(case when d.group_or_subplan is NULL then 'OTHER_GROUPS' else c.consumer_group  end) as group_name,"
              "(case when d.group_or_subplan is null then 0 else c.consumer_group_id end) as group_id  "
              "FROM %s a left join %s d ON a.consumer_group = d.group_or_subplan AND d.plan = '%.*s' AND d.tenant_id = "
              "%ld,"
              "%s b,%s c "
              "WHERE a.`value` = b.user_name "
              "AND a.TENANT_ID = %ld AND b.tenant_id = %ld AND c.tenant_id = %ld "
              "AND a.TENANT_ID = b.tenant_id AND a.tenant_id = c.tenant_id "
              "AND a.attribute = 'ORACLE_USER' AND c.consumer_group = a.consumer_group",
              t_a_res_name,
              t_d_directive,
              plan.length(),
              plan.ptr(),
              sql_tenant_id,
              t_b_user_name,
              t_c_consumer_group_name,
              sql_tenant_id,
              sql_tenant_id,
              sql_tenant_id))) {
        LOG_WARN("fail format sql", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(t_a_res_name), K(tenant_id), K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObResourceUserMappingRule rule;
          rule.set_tenant_id(tenant_id);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, user_id, rule, uint64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, group_id, rule, uint64_t);
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, group_name, rule);
          if (OB_SUCC(ret) && OB_FAIL(rules.push_back(rule))) {
            LOG_WARN("fail push back rules", K(rule), K(ret));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail get next row", K(ret), K(t_a_res_name), K(tenant_id));
        }
      }
    }
  }
  return ret;
}
