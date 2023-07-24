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
#include "share/resource_manager/ob_cgroup_ctrl.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/resource_manager/ob_resource_mapping_rule_manager.h"
#include "common/ob_timeout_ctx.h"
#include "observer/ob_sql_client_decorator.h"
#include "observer/ob_server_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/utility/ob_fast_convert.h"

using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

// 一个小的 Helper Guard，自动做 trans start 和 commit，避免面条代码，增加可维护性。
ObResourceManagerProxy::TransGuard::TransGuard(
    ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    int &ret)
  : trans_(trans), ret_(ret)
{
  ret_ = trans_.start(GCTX.sql_proxy_, tenant_id, true);
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
      LOG_WARN_RET(trans_ret, "fail commit/rollback trans", K_(ret), K(is_commit), K(trans_ret));
    }
    if (OB_SUCCESS == ret_) {
      ret_ = trans_ret;
    }
  }
}

// ObResourceManagerProxy 实现，用于操作 resource manager 内部表
ObResourceManagerProxy::ObResourceManagerProxy()
{
}

ObResourceManagerProxy::~ObResourceManagerProxy()
{
}

int ObResourceManagerProxy::create_plan(
    uint64_t tenant_id,
    const ObString &plan,
    const ObObj &comments)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_PLAN_TNAME;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, plan.ptr(), plan.length(), "plan");
      if (OB_SUCC(ret) && comments.is_varchar()) {
        const ObString &c = comments.get_string();
        SQL_COL_APPEND_STR_VALUE(sql, values, c.ptr(), c.length(), "comments");
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id,
                                       sql.ptr(),
                                       affected_rows))) {
          trans.reset_last_error();
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_ERR_RES_OBJ_ALREADY_EXIST;
            LOG_USER_ERROR(OB_ERR_RES_OBJ_ALREADY_EXIST, "resource plan", plan.length(), plan.ptr());
            LOG_WARN("Concurrent call create plan or plan already exist",
                     K(ret), K(tname), K(plan), K(comments));
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

int ObResourceManagerProxy::delete_plan(
    uint64_t tenant_id,
    const common::ObString &plan)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    // 删除 plan 时要级联删除 directive
    const char *tname_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    const char *tname_plan = OB_ALL_RES_MGR_PLAN_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "DELETE /* REMOVE_RES_PLAN */ FROM %s "
                "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
                tname_plan, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                plan.length(), plan.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(
                tenant_id,
                sql.ptr(),
                affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_RES_PLAN_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_RES_PLAN_NOT_EXIST, plan.length(), plan.ptr());
    } else if (OB_FAIL(sql.assign_fmt(
                "DELETE /* REMOVE_RES_PLAN */ FROM %s "
                "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
                tname_directive, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                plan.length(), plan.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(
                tenant_id,
                sql.ptr(),
                affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ObResMgrVarcharValue cur_plan;
    ObResourcePlanManager &plan_mgr = G_RES_MGR.get_plan_mgr();
    if (OB_FAIL(plan_mgr.get_cur_plan(tenant_id, cur_plan))) {
      LOG_WARN("get cur plan failed", K(ret), K(tenant_id), K(cur_plan));
    } else if (cur_plan.get_value() != plan) {
      //删除非当前使用plan，do nothing
    } else {
      //删除当前使用的plan，把当前所有IO资源置空
      if (OB_FAIL(GCTX.cgroup_ctrl_->reset_all_group_iops(
                        tenant_id,
                        1))) {
        LOG_WARN("reset cur plan group directive failed",K(plan), K(ret));
      } else if (OB_FAIL(reset_all_mapping_rules())) {
        LOG_WARN("reset hashmap failed when delete using plan");
      } else {
        LOG_INFO("reset cur plan group directive success",K(plan), K(ret));
      }
    }
  }
  return ret;
}

// @private
int ObResourceManagerProxy::allocate_consumer_group_id(
    ObMySQLTransaction &trans,
    uint64_t tenant_id,
    int64_t &group_id)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(
      &trans, tenant_id, OB_ALL_RES_MGR_CONSUMER_GROUP_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "SELECT /* ALLOC_MAX_GROUP_ID */ COALESCE(MAX(CONSUMER_GROUP_ID) + 1, 10000) AS NEXT_GROUP_ID FROM %s "
                "WHERE TENANT_ID = %ld",
                tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
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
      // 获取到下一个可用的 group id 分给新建的 consumer group
      EXTRACT_INT_FIELD_MYSQL(*result, "NEXT_GROUP_ID", group_id, int64_t);
    }
  }
  return ret;
}


int ObResourceManagerProxy::create_consumer_group(
    uint64_t tenant_id,
    const ObString &consumer_group,
    const ObObj &comments)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ret = create_consumer_group(trans, tenant_id, consumer_group, comments);
  }
  return ret;
}

int ObResourceManagerProxy::create_consumer_group(
    ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const ObString &consumer_group,
    const ObObj &comments,
    int64_t consumer_group_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
  if (consumer_group_id < 0 && /* 如果 id >=0 则使用外面传入的 id */
      OB_FAIL(allocate_consumer_group_id(trans, tenant_id, consumer_group_id))) {
    LOG_WARN("fail alloc group id", K(tenant_id), K(ret));
  } else if (OB_FAIL(sql.assign_fmt("INSERT /* CREATE_CONSUMER_GROUP */ INTO %s (", tname))) {
    STORAGE_LOG(WARN, "append table name failed, ", K(ret));
  } else {
    ObSqlString values;
    SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
    SQL_COL_APPEND_VALUE(sql, values, consumer_group_id, "consumer_group_id", "%lu");
    SQL_COL_APPEND_STR_VALUE(sql, values, consumer_group.ptr(), consumer_group.length(), "consumer_group");
    if (OB_SUCC(ret) && comments.is_varchar()) {
      const ObString &c = comments.get_string();
      SQL_COL_APPEND_STR_VALUE(sql, values, c.ptr(), c.length(), "comments");
    }
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                 static_cast<int32_t>(values.length()),
                                 values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id,
                                     sql.ptr(),
                                     affected_rows))) {
        trans.reset_last_error();
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_ERR_RES_OBJ_ALREADY_EXIST;
          LOG_USER_ERROR(OB_ERR_RES_OBJ_ALREADY_EXIST, "resource consumer group",
                         consumer_group.length(), consumer_group.ptr());
          LOG_WARN("Concurrent call create consumer_group or consumer_group already exist",
                   K(ret), K(tname), K(consumer_group), K(comments));
        } else {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        }
      } else {
        if (!is_single_row(affected_rows)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value. expect only 1 row affected",
                   K(affected_rows), K(sql), K(ret));
        }
      }
    }
  }
  return ret;
}


int ObResourceManagerProxy::delete_consumer_group(
    uint64_t tenant_id,
    const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    // 删除 group 时要级联删除 directive
    const char *tname_consumer_group = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
    const char *tname_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "DELETE /* REMOVE_RES_CONSUMER_GROUP */ FROM %s "
                "WHERE TENANT_ID = %ld AND CONSUMER_GROUP = '%.*s'",
                tname_consumer_group, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                consumer_group.length(), consumer_group.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(
                tenant_id,
                sql.ptr(),
                affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_CONSUMER_GROUP_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_CONSUMER_GROUP_NOT_EXIST, consumer_group.length(), consumer_group.ptr());
    } else if (OB_FAIL(sql.assign_fmt(
                "DELETE /* REMOVE_RES_CONSUMER_GROUP */ FROM %s "
                "WHERE TENANT_ID = %ld AND GROUP_OR_SUBPLAN = '%.*s'",
                tname_directive, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                consumer_group.length(), consumer_group.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(
                tenant_id,
                sql.ptr(),
                affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // 在这里inner sql之后就stop io_control的原因是，无法从内部表读到被删除group的信息
    if (OB_FAIL(GCTX.cgroup_ctrl_->delete_group_iops(tenant_id, 1, consumer_group))) {
      LOG_WARN("fail to stop cur iops isolation", K(ret), K(tenant_id), K(consumer_group));
    }
  }
  return ret;
}

int ObResourceManagerProxy::create_plan_directive(
    uint64_t tenant_id,
    const common::ObString &plan,
    const common::ObString &group,
    const common::ObObj &comment,
    const common::ObObj &mgmt_p1,
    const common::ObObj &utilization_limit,
    const common::ObObj &min_iops,
    const common::ObObj &max_iops,
    const common::ObObj &weight_iops)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ret = create_plan_directive(trans, tenant_id, plan, group, comment, mgmt_p1, utilization_limit, min_iops, max_iops, weight_iops);
  }
  return ret;
}

int ObResourceManagerProxy::create_plan_directive(
    common::ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const common::ObString &plan,
    const common::ObString &group,
    const common::ObObj &comments,
    const common::ObObj &mgmt_p1,
    const common::ObObj &utilization_limit,
    const common::ObObj &min_iops,
    const common::ObObj &max_iops,
    const common::ObObj &weight_iops)
{
  int ret = OB_SUCCESS;
  bool consumer_group_exist = true;
  bool plan_exist = true;
  if (OB_FAIL(check_if_plan_exist(trans, tenant_id, plan, plan_exist))) {
    LOG_WARN("fail check consumer group exist", K(tenant_id), K(group), K(ret));
  } else if (OB_FAIL(check_if_consumer_group_exist(trans, tenant_id, group, consumer_group_exist))) {
    LOG_WARN("fail check consumer group exist", K(tenant_id), K(group), K(ret));
  } else if (!plan_exist) {
    ret = OB_ERR_RES_PLAN_NOT_EXIST;
    LOG_USER_ERROR(OB_ERR_RES_PLAN_NOT_EXIST, plan.length(), plan.ptr());
  } else if (!consumer_group_exist) {
    ret = OB_ERR_CONSUMER_GROUP_NOT_EXIST;
    LOG_USER_ERROR(OB_ERR_CONSUMER_GROUP_NOT_EXIST, group.length(), group.ptr());
  } else {
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      int64_t v = 0;
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, plan.ptr(), plan.length(), "plan");
      SQL_COL_APPEND_STR_VALUE(sql, values, group.ptr(), group.length(), "group_or_subplan");
      if (OB_SUCC(ret) && comments.is_varchar()) {
        const ObString &c = comments.get_string();
        SQL_COL_APPEND_STR_VALUE(sql, values, c.ptr(), c.length(), "comments");
      }
      if (OB_SUCC(ret) && OB_SUCC(get_percentage("MGMT_P1", mgmt_p1, v))) {
        SQL_COL_APPEND_VALUE(sql, values, v, "MGMT_P1", "%ld");
      }
      if (OB_SUCC(ret) && OB_SUCC(get_percentage("UTILIZATION_LIMIT", utilization_limit, v))) {
        SQL_COL_APPEND_VALUE(sql, values, v, "UTILIZATION_LIMIT", "%ld");
      }
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_1_0_0 && (
                 (OB_SUCC(get_percentage("MIN_IOPS", min_iops, v) && v != 0)) ||
                 (OB_SUCC(get_percentage("MAX_IOPS", max_iops, v) && v != 100)) ||
                 (OB_SUCC(get_percentage("WEIGHT_IOPS", weight_iops, v) && v != 0)))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("iops setting is not suppported when tenant's data version is below 4.1.0.0", K(ret));
      } else if (tenant_data_version >= DATA_VERSION_4_1_0_0) {
        int64_t iops_minimum = 0;
        int64_t iops_maximum = 100;
        if (OB_SUCC(ret) && OB_SUCC(get_percentage("MIN_IOPS", min_iops, v))) {
          iops_minimum = v;
          SQL_COL_APPEND_VALUE(sql, values, v, "MIN_IOPS", "%ld");
        }
        if (OB_SUCC(ret) && OB_SUCC(get_percentage("MAX_IOPS", max_iops, v))) {
          iops_maximum = v;
          bool is_valid = false;
          if (OB_FAIL(check_iops_validity(tenant_id, plan, group, iops_minimum, iops_maximum, is_valid))) {
            LOG_WARN("check iops setting failed", K(tenant_id), K(plan), K(iops_minimum), K(iops_maximum));
          } else if (OB_UNLIKELY(!is_valid)) {
            ret = OB_INVALID_CONFIG;
            LOG_WARN("invalid iops config", K(ret), K(tenant_id), K(iops_minimum), K(iops_maximum));
          } else {
            SQL_COL_APPEND_VALUE(sql, values, v, "MAX_IOPS", "%ld");
          }
        }
        if (OB_SUCC(ret) && OB_SUCC(get_percentage("WEIGHT_IOPS", weight_iops, v))) {
          SQL_COL_APPEND_VALUE(sql, values, v, "WEIGHT_IOPS", "%ld");
        }
      }
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                   static_cast<int32_t>(values.length()),
                                   values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id,
                                       sql.ptr(),
                                       affected_rows))) {
          trans.reset_last_error();
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            ret = OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST;
            LOG_USER_ERROR(OB_ERR_PLAN_DIRECTIVE_ALREADY_EXIST,
                           plan.length(), plan.ptr(), group.length(), group.ptr());
            LOG_WARN("Concurrent call create plan or plan already exist",
                     K(ret), K(tname), K(plan), K(comments));
          } else {
            LOG_WARN("fail to execute sql", K(sql), K(ret));
          }
        } else {
          if (!is_single_row(affected_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value. expect only 1 row affected",
                     K(affected_rows), K(sql), K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::check_if_consumer_group_exist(
    ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const ObString &group,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = true;
  ObSQLClientRetryWeak sql_client_retry_weak(
      &trans, tenant_id, OB_ALL_RES_MGR_CONSUMER_GROUP_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "SELECT /* CHECK_IF_RES_CONSUMER_GROUP_EXIST */ * FROM %s "
                "WHERE TENANT_ID = %ld AND CONSUMER_GROUP = '%.*s'",
                tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                group.length(), group.ptr()))) {
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
    ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const ObString &plan,
    const ObString &group,
    bool &exist)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(
      &trans, tenant_id, OB_ALL_RES_MGR_DIRECTIVE_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "SELECT /* CHECK_IF_RES_PLAN_DIRECTIVE_EXIST */ * FROM %s "
                "WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
                tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                plan.length(), plan.ptr(), group.length(), group.ptr()))) {
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
    uint64_t tenant_id,
    const ObString &plan,
    bool &exist)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    bool plan_exist = false;
    if (OB_FAIL(check_if_plan_exist(trans, tenant_id, plan, exist))) {
      LOG_WARN("fail check if plan exists", K(ret), K(tenant_id), K(plan));
    }
  }
  return ret;
}

int ObResourceManagerProxy::check_if_plan_exist(
    ObMySQLTransaction &trans,
    uint64_t tenant_id,
    const ObString &plan,
    bool &exist)
{
  int ret = OB_SUCCESS;
  ObSQLClientRetryWeak sql_client_retry_weak(
      &trans, tenant_id, OB_ALL_RES_MGR_PLAN_TID);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_PLAN_TNAME;
    if (OB_FAIL(sql.assign_fmt(
                "SELECT /* CHECK_IF_RES_PLAN_EXIST */ * FROM %s "
                "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
                tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                plan.length(), plan.ptr()))) {
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

int ObResourceManagerProxy::check_if_user_exist(
    uint64_t tenant_id,
    const ObString &user_name,
    bool &exist)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObSEArray<const ObUserInfo *, 4>users_info;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_name, users_info))) {
    LOG_WARN("get users info failed", K(ret), K(user_name));
  } else if (users_info.empty()) {
    exist = false;
  }
  return ret;
}

int ObResourceManagerProxy::check_if_function_exist(const ObString &function_name, bool &exist)
{
  int ret = OB_SUCCESS;
  if (0 == function_name.compare("COMPACTION_HIGH") ||
      0 == function_name.compare("HA_HIGH") ||
      0 == function_name.compare("COMPACTION_MID") ||
      0 == function_name.compare("HA_MID") ||
      0 == function_name.compare("COMPACTION_LOW") ||
      0 == function_name.compare("HA_LOW") ||
      0 == function_name.compare("DDL_HIGH") ||
      0 == function_name.compare("DDL")) {
    exist = true;
  } else {
    exist = false;
    LOG_WARN("invalid function name", K(function_name));
  }
  return ret;
}

int ObResourceManagerProxy::check_if_column_exist(
    uint64_t tenant_id,
    const ObString &db_name,
    const ObString &table_name,
    const ObString &column_name)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObColumnSchemaV2 *col_schema = NULL;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service is null");
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, db_name, table_name, false, table_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(db_name), K(table_name));
  } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(column_name))) {
    ret = OB_ERR_COLUMN_NOT_FOUND;
    LOG_WARN("column not exist", K(ret), K(tenant_id), K(db_name), K(table_name), K(column_name));
  }
  return ret;
}

int ObResourceManagerProxy::formalize_column_mapping_value(
    const ObString &db_name,
    const ObString &table_name,
    const ObString &column_name,
    const ObString &literal_value,
    const ObString &user_name,
    bool is_oracle_mode,
    ObIAllocator &allocator,
    ObString &formalized_value)
{
  int ret = OB_SUCCESS;
  const int64_t wrap_len = 24;
  const int64_t buf_len = db_name.length() + table_name.length() + column_name.length()
                          + literal_value.length() + user_name.length() + wrap_len;
  char *buf = NULL;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocator memory failed", K(ret), K(buf_len), K(literal_value.length()));
  } else if (is_oracle_mode &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, "\"%.*s\".\"%.*s\".\"%.*s\" = \\'%.*s\\'",
                                    db_name.length(), db_name.ptr(),
                                    table_name.length(), table_name.ptr(),
                                    column_name.length(), column_name.ptr(),
                                    literal_value.length(), literal_value.ptr()))) {
    LOG_WARN("print name and literal value failed", K(ret), K(buf_len));
  } else if (!is_oracle_mode &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, "`%.*s`.`%.*s`.`%.*s` = \\'%.*s\\'",
                                    db_name.length(), db_name.ptr(),
                                    table_name.length(), table_name.ptr(),
                                    column_name.length(), column_name.ptr(),
                                    literal_value.length(), literal_value.ptr()))) {
    LOG_WARN("print name and literal value failed", K(ret), K(buf_len));
  } else if (user_name.length() > 0 && is_oracle_mode &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, " for \"%.*s\"",
                                    user_name.length(), user_name.ptr()))) {
    LOG_WARN("print user name failed", K(ret), K(buf_len));
  } else if (user_name.length() > 0 && !is_oracle_mode &&
             OB_FAIL(databuff_printf(buf, buf_len, pos, " for `%.*s`",
                                    user_name.length(), user_name.ptr()))) {
    LOG_WARN("print user name failed", K(ret), K(buf_len));
  } else {
    formalized_value.assign(buf, pos);
    LOG_TRACE("formalize column mapping rule", K(formalized_value));
  }
  return ret;
}

int ObResourceManagerProxy::check_if_column_and_user_exist(
    common::ObMySQLTransaction &trans,
    uint64_t tenant_id,
    common::ObString &value,
    const sql::ObSQLSessionInfo &session,
    ObIAllocator &allocator,
    bool &exist,
    ObString &formalized_value)
{
  int ret = OB_SUCCESS;
  ObString db_name;
  ObString table_name;
  ObString column_name;
  ObString literal_value;
  ObString user_name;
  exist = true;
  ObNameCaseMode case_mode = OB_ORIGIN_AND_SENSITIVE;
  if (lib::is_mysql_mode() && OB_FAIL(session.get_name_case_mode(case_mode))) {
    LOG_WARN("get name case mode failed", K(ret));
  } else if (OB_FAIL(parse_column_mapping_rule(value, &session, db_name, table_name, column_name,
                                        literal_value, user_name, case_mode))) {
    LOG_WARN("parse column mapping rule", K(ret));
  } else if (!user_name.empty()
             && OB_FAIL(check_if_user_exist(tenant_id, user_name, exist))) {
    LOG_WARN("check if user exist failed", K(ret));
  } else if (!exist) {
    ret = OB_ERR_USER_NOT_EXIST;
    LOG_WARN("user not exist", K(ret), K(tenant_id), K(user_name));
  } else if (OB_FAIL(check_if_column_exist(tenant_id, db_name, table_name,
                                           column_name))) {
    LOG_WARN("check if column exist failed", K(ret));
  } else if (OB_FAIL(formalize_column_mapping_value(db_name, table_name, column_name, literal_value,
                                               user_name, session.is_oracle_compatible(), allocator,
                                               formalized_value))) {
    LOG_WARN("formalize column mapping rule failed", K(ret));
  } else {
    LOG_TRACE("user and column both exist", K(user_name), K(db_name), K(table_name),
              K(column_name), K(session.get_user_name()));
  }
  return ret;
}

int ObResourceManagerProxy::get_percentage(const char *name, const ObObj &obj, int64_t &v)
{
  int ret = OB_SUCCESS;
  if (obj.is_integer_type()) {
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

int ObResourceManagerProxy::check_iops_validity(
    const uint64_t tenant_id,
    const common::ObString &plan_name,
    const common::ObString &group,
    const int64_t iops_minimum,
    const int64_t iops_maximum,
    bool &valid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (iops_maximum < iops_minimum) {
    // precheck
    valid = false;
  } else {
    ObSEArray<ObPlanDirective, 8> directives;
    if (OB_FAIL(get_all_plan_directives(tenant_id, plan_name, directives))) {
      LOG_WARN("fail get plan directive", K(tenant_id), K(plan_name), K(ret));
    } else {
      uint64_t total_min = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < directives.count(); ++i) {
        ObPlanDirective &cur_directive = directives.at(i);
        if (OB_UNLIKELY(!is_user_group(cur_directive.group_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected group id", K(cur_directive));
        } else if (OB_UNLIKELY(!cur_directive.is_valid())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid group io config", K(cur_directive));
        } else if ((0 == group.compare(cur_directive.group_name_.get_value()))) {
          //skip cur group
        } else {
          total_min += cur_directive.min_iops_;
        }
      }
      if(OB_SUCC(ret)) {
        total_min += iops_minimum;
        if (total_min > 100) {
          valid = false;
          LOG_WARN("invalid group io config", K(total_min), K(iops_minimum), K(iops_maximum), K(plan_name));
        } else {
          valid = true;
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_user_mapping_info(
    const uint64_t tenant_id,
    const common::ObString &user,
    ObResourceUserMappingRule &rule)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else {
    ObMySQLTransaction trans;
    TransGuard trans_guard(trans, tenant_id, ret);
    if (trans_guard.ready()) {
      ObSQLClientRetryWeak sql_client_retry_weak(
          &trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        ObSqlString sql;
        const char *t_a_res_name = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
        const char *t_b_user_name = OB_ALL_USER_TNAME;
        uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
        if (OB_FAIL(sql.assign_fmt(
                    "SELECT /* GET_USER_ID_FROM_MAPPING_RULE */ "
                    "user_id FROM %s a, %s b "
                    "WHERE a.`value` = b.user_name "
                    "AND a.TENANT_ID = %ld AND b.tenant_id = %ld "
                    "AND a.attribute = 'USER' AND a.`value` = '%.*s'",
                    t_a_res_name, t_b_user_name,
                    sql_tenant_id, sql_tenant_id, user.length(), user.ptr()))) {
          LOG_WARN("fail format sql", K(ret));
        } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail query sql", K(t_a_res_name), K(tenant_id), K(ret));
        } else {
          int64_t affected_rows = 0;
          while (OB_SUCC(result->next())) {
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, user_id, rule, uint64_t);
            ++affected_rows;
          }
          if (OB_ITER_END == ret) {
            if (OB_UNLIKELY(affected_rows > 1)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected value. expect only 0 or 1 row affected", K(ret), K(affected_rows), K(user));
            } else {
              ret = OB_SUCCESS;
            }
          } else {
            LOG_WARN("fail get next row", K(ret), K(user), K(tenant_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::update_plan_directive(
    uint64_t tenant_id,
    const ObString &plan,
    const ObString &group,
    const ObObj &comments,
    const ObObj &mgmt_p1,
    const ObObj &utilization_limit,
    const ObObj &min_iops,
    const ObObj &max_iops,
    const ObObj &weight_iops)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    const char *comma = "";
    bool exist = false;
    if (OB_FAIL(check_if_plan_directive_exist(trans, tenant_id, plan, group, exist))) {
      LOG_WARN("fail check if plan exist", K(tenant_id), K(plan), K(group), K(ret));
    } else if (!exist) {
      ret = OB_ERR_PLAN_DIRECTIVE_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_PLAN_DIRECTIVE_NOT_EXIST,
                     plan.length(), plan.ptr(), group.length(), group.ptr());
    } else if (comments.is_null() && mgmt_p1.is_null() && utilization_limit.is_null() &&
               min_iops.is_null() && max_iops.is_null() && weight_iops.is_null()) {
      // 没有指定任何有效参数，什么都不做，也不报错。兼容 Oracle 行为。
      ret = OB_SUCCESS;
    } else if (OB_FAIL(sql.assign_fmt("UPDATE /* UPDATE_PLAN_DIRECTIVE */ %s SET ", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      int64_t v = 0;
      if (OB_SUCC(ret) && comments.is_varchar()) {
        const ObString &c = comments.get_varchar();
        ret = sql.append_fmt("COMMENTS='%.*s'", static_cast<int32_t>(c.length()), c.ptr());
        comma = ",";
      }
      if (OB_SUCC(ret) &&
          !mgmt_p1.is_null() &&
          OB_SUCC(get_percentage("NEW_MGMT_P1", mgmt_p1, v))) {
        ret = sql.append_fmt("%s MGMT_P1=%ld", comma, v);
        comma = ",";
      }
      if (OB_SUCC(ret) &&
          !utilization_limit.is_null() &&
          OB_SUCC(get_percentage("NEW_UTILIZATION_LIMIT", utilization_limit, v))) {
        ret = sql.append_fmt("%s UTILIZATION_LIMIT=%ld", comma, v);
        comma = ",";
      }
      uint64_t tenant_data_version = 0;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
        LOG_WARN("get tenant data version failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_1_0_0 && (
                 (OB_SUCC(get_percentage("NEW_MIN_IOPS", min_iops, v) && v != 0)) ||
                 (OB_SUCC(get_percentage("NEW_MAX_IOPS", max_iops, v) && v != 100)) ||
                 (OB_SUCC(get_percentage("NEW_WEIGHT_IOPS", weight_iops, v) && v != 0)))) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("iops setting is not suppported when tenant's data version is below 4.1.0.0", K(ret));
      } else if (tenant_data_version >= DATA_VERSION_4_1_0_0) {
        if (min_iops.is_null() && max_iops.is_null()) {
          // no need to check
        } else {
          // check if iops_config is valid
          int64_t new_iops_minimum = 0;
          int64_t new_iops_maximum = 100;
          if (OB_SUCC(ret)) {
            ObPlanDirective directive;
            if (OB_FAIL(get_iops_config(tenant_id, plan, group, directive))) {
              LOG_WARN("get iops config from table failed", K(ret), K(plan), K(group), K(directive));
            } else {
              new_iops_minimum = directive.min_iops_;
              new_iops_maximum = directive.max_iops_;
            }
          }
          if (OB_SUCC(ret) &&
              !min_iops.is_null() &&
              OB_SUCC(get_percentage("NEW_MIN_IOPS", min_iops, v))) {
            new_iops_minimum = v;
            ret = sql.append_fmt("%s MIN_IOPS=%ld", comma, v);
            comma = ",";
          }
          if (OB_SUCC(ret) &&
              !max_iops.is_null() &&
              OB_SUCC(get_percentage("NEW_MAX_IOPS", max_iops, v))) {
            new_iops_maximum = v;
          }
          if (OB_SUCC(ret)) {
            bool is_valid = false;
            if (OB_FAIL(check_iops_validity(tenant_id, plan, group, new_iops_minimum, new_iops_maximum, is_valid))) {
              LOG_WARN("check iops setting failed", K(tenant_id), K(plan), K(new_iops_minimum), K(new_iops_maximum));
            } else if (OB_UNLIKELY(!is_valid)) {
              ret = OB_INVALID_CONFIG;
              LOG_WARN("invalid iops config", K(ret), K(tenant_id), K(new_iops_minimum), K(new_iops_maximum));
            } else {
              ret = sql.append_fmt("%s MAX_IOPS=%ld", comma, v);
              comma = ",";
            }
          }
        }
        if (OB_SUCC(ret) &&
            !weight_iops.is_null() &&
            OB_SUCC(get_percentage("NEW_WEIGHT_IOPS", weight_iops, v))) {
          ret = sql.append_fmt("%s WEIGHT_IOPS=%ld", comma, v);
          comma = ",";
        }
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail append value", K(ret));
      } else if (OB_FAIL(sql.append_fmt(
                  " WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
                  ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                  plan.length(), plan.ptr(), group.length(), group.ptr()))) {
        LOG_WARN("fail append value", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id,
                                     sql.ptr(),
                                     affected_rows))) {
        trans.reset_last_error();
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (affected_rows > 1) {
        // 注意：update 语句的 affected_rows 可能为 0 行，也可能为 1 行
        // 如果update 进去的值和旧值完全一样，则是0行，表示什么都没改
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected row value not expected", K(affected_rows), K(ret));
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::delete_plan_directive(
    uint64_t tenant_id,
    const ObString &plan,
    const ObString &group)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    int64_t affected_rows = 0;
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
    bool exist = false;
    if (OB_FAIL(check_if_plan_directive_exist(trans, tenant_id, plan, group, exist))) {
      LOG_WARN("fail check if plan exist", K(tenant_id), K(plan), K(group), K(ret));
    } else if (!exist) {
      ret = OB_ERR_PLAN_DIRECTIVE_NOT_EXIST;
      LOG_USER_ERROR(OB_ERR_PLAN_DIRECTIVE_NOT_EXIST,
                     plan.length(), plan.ptr(), group.length(), group.ptr());
    } else if (OB_FAIL(sql.assign_fmt(
                "DELETE /* REMOVE_PLAN_DIRECTIVE */ FROM %s "
                "WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
                tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                plan.length(), plan.ptr(), group.length(), group.ptr()))) {
      LOG_WARN("fail append value", K(ret));
    } else if (OB_FAIL(trans.write(tenant_id,
                                   sql.ptr(),
                                   affected_rows))) {
      trans.reset_last_error();
      LOG_WARN("fail to execute sql", K(sql), K(ret));
    } else if (affected_rows != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected row value not expected", K(affected_rows), K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // 在这里inner sql之后就stop的原因是， 无法从内部表读到被删除group的信息
    if (OB_FAIL(GCTX.cgroup_ctrl_->reset_group_iops(
                     tenant_id,
                     1,
                     group))) {
      LOG_WARN("reset deleted group directive failed", K(ret), K(group));
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_all_plan_directives(
    uint64_t tenant_id,
    const ObString &plan,
    ObIArray<ObPlanDirective> &directives)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    bool plan_exist = false;
    if (OB_FAIL(check_if_plan_exist(trans, tenant_id, plan, plan_exist))) {
      LOG_WARN("fail check plan exist", K(tenant_id), K(plan), K(ret));
    } else if (!plan_exist) {
      // skip
    } else {
      ObSQLClientRetryWeak sql_client_retry_weak(
          &trans, tenant_id, OB_ALL_RES_MGR_DIRECTIVE_TID);
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        ObSqlString sql;
        const char *tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
        if (OB_FAIL(sql.assign_fmt(
                    "SELECT /* GET_ALL_PLAN_DIRECTIVE_SQL */ "
                    "* FROM %s "
                    "WHERE TENANT_ID = %ld AND PLAN = '%.*s'",
                    tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                    plan.length(), plan.ptr()))) {
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
            EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, group_or_subplan, directive);
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, mgmt_p1, directive, int64_t);
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, utilization_limit, directive, int64_t);
            bool skip_null_error = false;
            bool skip_column_error = true;
            EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "min_iops", directive.min_iops_, int64_t, skip_null_error, skip_column_error, 0);
            EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "max_iops", directive.max_iops_, int64_t, skip_null_error, skip_column_error, 100);
            EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*result, "weight_iops", directive.weight_iops_, int64_t, skip_null_error, skip_column_error, 0);
            if (OB_SUCC(ret)) {
              ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
              //如果读失败了，可能是group info还没有放到map里，因此不执行该directive直到下一次刷新
              if (OB_FAIL(rule_mgr.get_group_id_by_name(tenant_id, directive.group_name_, directive.group_id_))) {
                LOG_WARN("fail get group id",K(ret), K(directive.group_id_), K(directive.group_name_));
              } else if (OB_FAIL(directives.push_back(directive))) {
                LOG_WARN("fail push back directives", K(directive), K(ret));
              }
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

/* attribute: 实体类型
 * value: 实体可能为 user name，可能为 function name，是 attribute 决定的
 * consumer_group: value 对应实体所属 consumer group
 */
int ObResourceManagerProxy::replace_mapping_rule(
    uint64_t tenant_id,
    const ObString &attribute,
    const ObString &value,
    const ObString &consumer_group,
    const sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    if (!consumer_group.empty()) {
      bool consumer_group_exist = true;
      if (OB_FAIL(check_if_consumer_group_exist(trans,
                                                tenant_id,
                                                consumer_group,
                                                consumer_group_exist))) {
        LOG_WARN("fail check if consumer group exist", K(tenant_id), K(consumer_group), K(ret));
      } else if (!consumer_group_exist) {
        ret = OB_ERR_CONSUMER_GROUP_NOT_EXIST;
        LOG_USER_ERROR(OB_ERR_INVALID_PLAN_DIRECTIVE_NAME,
                       consumer_group.length(), consumer_group.ptr());
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == attribute.case_compare("user")) {
        if (OB_FAIL(replace_user_mapping_rule(trans, tenant_id, attribute, value, consumer_group))) {
          LOG_WARN("replace user mapping rule failed", K(ret));
        }
      } else if (0 == attribute.case_compare("function")) {
        if (OB_FAIL(replace_function_mapping_rule(trans, tenant_id, attribute, value, consumer_group))) {
          LOG_WARN("replace user mapping rule failed", K(ret));
        }
      } else if (0 == attribute.case_compare("column")) {
        if (OB_FAIL(replace_column_mapping_rule(trans, tenant_id, attribute, value, consumer_group, session))) {
          LOG_WARN("replace column mapping rule failed", K(ret));
        } else if (OB_FAIL(update_resource_mapping_version(trans, tenant_id))) {
          LOG_WARN("update resource mapping rule version failed", K(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid attribute", K(ret), K(attribute));
      }
    }
  }
  return ret;
}
int ObResourceManagerProxy::replace_user_mapping_rule(ObMySQLTransaction &trans, uint64_t tenant_id,
                                                      const ObString &attribute, const ObString &value,
                                                      const ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  bool user_exist = true;
  uint64_t user_id = 0;
  if (OB_SUCC(ret)) {
    // if user not exists, do nothing, do not throw error
    //
    if (OB_FAIL(check_if_user_exist(tenant_id,
                                    value,
                                    user_exist))) {
      LOG_WARN("fail check if user exist", K(tenant_id), K(value), K(ret));
    }
  }
  if (OB_SUCC(ret) && user_exist && consumer_group.empty()) {
    // get user_id for reset map
    ObResourceUserMappingRule rule;
    if (OB_FAIL(get_user_mapping_info(tenant_id, value, rule))) {
      LOG_WARN("get user info failed", K(ret), K(tenant_id), K(value));
    } else {
      user_id = rule.user_id_;
    }
  }
  if (OB_SUCC(ret) && user_exist) {
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
    if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, attribute.ptr(), attribute.length(), "attribute");
      SQL_COL_APPEND_STR_VALUE(sql, values, value.ptr(), value.length(), "value");
      SQL_COL_APPEND_STR_VALUE(sql, values, consumer_group.ptr(), consumer_group.length(), "consumer_group");
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                  static_cast<int32_t>(values.length()),
                                  values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id,
                                      sql.ptr(),
                                      affected_rows))) {
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
      if (OB_SUCC(ret) && user_exist && consumer_group.empty()) {
        // reset map
        if (consumer_group.empty()) {
          ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
          if (OB_FAIL(rule_mgr.reset_group_id_by_user(tenant_id, user_id))) {
            LOG_WARN("fail reset user_group map", K(ret), K(tenant_id), K(user_id), K(value));
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::replace_function_mapping_rule(ObMySQLTransaction &trans, uint64_t tenant_id,
                                                          const ObString &attribute, const ObString &value,
                                                          const ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  bool function_exist = false;
  if (OB_SUCC(ret)) {
    // Same as user rule, the mapping is unsuccessful but no error is thrown
    if (OB_FAIL(check_if_function_exist(value, function_exist))) {
      LOG_WARN("fail check if function exist", K(tenant_id), K(value), K(ret));
    } else if (OB_UNLIKELY(!function_exist)) {
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid function name, please check");
    }
  }
  if (OB_SUCC(ret) && function_exist) {
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
    if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, attribute.ptr(), attribute.length(), "attribute");
      SQL_COL_APPEND_STR_VALUE(sql, values, value.ptr(), value.length(), "value");
      SQL_COL_APPEND_STR_VALUE(sql, values, consumer_group.ptr(), consumer_group.length(), "consumer_group");
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                  static_cast<int32_t>(values.length()),
                                  values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id,
                                      sql.ptr(),
                                      affected_rows))) {
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
      if (OB_SUCC(ret) && function_exist && consumer_group.empty()) {
        // reset map
        if (consumer_group.empty()) {
          ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
          ObResMgrVarcharValue func;
          func.set_value(value);
          if (OB_FAIL(rule_mgr.reset_group_id_by_function(tenant_id, func))) {
            LOG_WARN("fail reset user_group map", K(ret), K(tenant_id), K(func), K(value));
          }
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::replace_column_mapping_rule(ObMySQLTransaction &trans, uint64_t tenant_id,
                                                        const ObString &attribute, const ObString &value,
                                                        const ObString &consumer_group,
                                                        const sql::ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  bool both_exist = true;
  ObArenaAllocator allocator;
  ObString formalized_value;
  ObString tmp_value;
  if (OB_FAIL(ob_write_string(allocator, value, tmp_value))) {
    LOG_WARN("copy value failed", K(ret), K(value.length()));
  } else if (OB_FAIL(check_if_column_and_user_exist(trans,
                                              tenant_id,
                                              tmp_value,
                                              session,
                                              allocator,
                                              both_exist,
                                              formalized_value))) {
    LOG_WARN("fail check if user exist", K(tenant_id), K(tmp_value), K(ret));
  } else if (!both_exist) {
    LOG_WARN("user or column may not exist", K(both_exist));
  } else {
    ObSqlString sql;
    const char *tname = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
    if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s (", tname))) {
      STORAGE_LOG(WARN, "append table name failed, ", K(ret));
    } else {
      ObSqlString values;
      SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
      SQL_COL_APPEND_STR_VALUE(sql, values, attribute.ptr(), attribute.length(), "attribute");
      SQL_COL_APPEND_STR_VALUE(sql, values, formalized_value.ptr(), formalized_value.length(), "value");
      SQL_COL_APPEND_STR_VALUE(sql, values, consumer_group.ptr(), consumer_group.length(), "consumer_group");
      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                    static_cast<int32_t>(values.length()),
                                    values.ptr()))) {
          LOG_WARN("append sql failed, ", K(ret));
        } else if (OB_FAIL(trans.write(tenant_id,
                                        sql.ptr(),
                                        affected_rows))) {
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
  return ret;
}

int ObResourceManagerProxy::update_resource_mapping_version(ObMySQLTransaction &trans,
                                                            uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *tname = OB_ALL_SYS_STAT_TNAME;
  if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s (", tname))) {
    STORAGE_LOG(WARN, "append table name failed, ", K(ret));
  } else {
    ObSqlString values;
    SQL_COL_APPEND_VALUE(sql, values, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), "tenant_id", "%lu");
    SQL_COL_APPEND_CSTR_VALUE(sql, values, "", "zone");
    SQL_COL_APPEND_CSTR_VALUE(sql, values, "ob_current_resource_mapping_version", "name");
    SQL_COL_APPEND_VALUE(sql, values, 5, "data_type", "%d");
    // need use microsecond in case insert multiple rules in one second concurrently.
    // It means mapping rule is updated but version keeps the same.
    SQL_COL_APPEND_VALUE(sql, values, "cast(unix_timestamp(now(6)) * 1000000 as signed)", "value", "%s");
    SQL_COL_APPEND_CSTR_VALUE(sql, values, "version of resource mapping rule", "info");
    if (OB_SUCC(ret)) {
      int64_t affected_rows = 0;
      if (OB_FAIL(sql.append_fmt(") VALUES (%.*s)",
                                  static_cast<int32_t>(values.length()),
                                  values.ptr()))) {
        LOG_WARN("append sql failed, ", K(ret));
      } else if (OB_FAIL(trans.write(tenant_id,
                                      sql.ptr(),
                                      affected_rows))) {
        trans.reset_last_error();
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else {
        if (is_single_row(affected_rows) || is_double_row(affected_rows)) {
          // insert or replace
          LOG_TRACE("update resource mapping version successfully", K(sql.string()));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected value. expect 1 or 2 row affected", K(affected_rows), K(sql), K(ret));
        }
      }
    }
  }
  LOG_INFO("update resource mapping version", K(ret), K(tenant_id));
  return ret;
}

int ObResourceManagerProxy::get_all_resource_mapping_rules(
    uint64_t tenant_id,
    common::ObIArray<ObResourceMappingRule> &rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const char *tname = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT /* GET_ALL_RES_MAPPING_RULE */ "
                  "attribute attr, `value`, consumer_group `group` FROM %s "
                  "WHERE TENANT_ID = %ld",
                  tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
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

int ObResourceManagerProxy::get_all_group_info(
    uint64_t tenant_id,
    const common::ObString &plan,
    common::ObIArray<ObResourceUserMappingRule> &rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_CONSUMER_GROUP_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const char *tname = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT /* GET_ALL_GROUP_INFOS */ "
                  "consumer_group as group_name,"
                  "consumer_group_id as group_id "
                  "FROM %s WHERE TENANT_ID = %ld",
                  tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail format sql", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObResourceUserMappingRule rule;
          rule.set_tenant_id(tenant_id);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, group_id, rule, uint64_t);
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, group_name, rule);
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

// 构建当前租户下全部 group_id -> group_name 的映射表
int ObResourceManagerProxy::get_all_resource_mapping_rules_for_plan(
    uint64_t tenant_id,
    const common::ObString &plan,
    common::ObIArray<ObResourceIdNameMappingRule> &rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const char *t_a_res_name = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      const char *t_c_consumer_group_name = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
      const char *t_d_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
      uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      // 当 resource plan 为空时，或者对应的 directive 不存在时，使用默认组 OTHER_GROUPS
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT /* GET_ALL_RES_MAPPING_RULE_BY_PLAN_NAME */ "
                  "(case when d.group_or_subplan is NULL then 'OTHER_GROUPS' else c.consumer_group  end) as group_name,"
                  "(case when d.group_or_subplan is null then 0 else c.consumer_group_id end) as group_id  "
                  "FROM %s a left join %s d ON a.consumer_group = d.group_or_subplan AND d.plan = '%.*s' AND d.tenant_id = %ld,"
                  "%s c "
                  "WHERE a.TENANT_ID = %ld AND c.tenant_id = %ld "
                  "AND a.tenant_id = c.tenant_id "
                  "AND c.consumer_group = a.consumer_group",
                  t_a_res_name, t_d_directive, plan.length(), plan.ptr(), sql_tenant_id,
                  t_c_consumer_group_name, sql_tenant_id, sql_tenant_id))) {
        LOG_WARN("fail format sql", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(t_a_res_name), K(tenant_id), K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObResourceIdNameMappingRule rule;
          rule.set_tenant_id(tenant_id);
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

// 构建当前租户下全部后台 Function 到 group_id 的映射表
int ObResourceManagerProxy::get_all_resource_mapping_rules_by_function(
    uint64_t tenant_id,
    const common::ObString &plan,
    common::ObIArray<ObResourceMappingRule> &rules)
// 构建当前租户下全部 group_id -> group_name 的映射表
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const char *t_a_res_name = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      const char *t_c_consumer_group_name = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
      const char *t_d_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
      uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      // 当 resource plan 为空时，或者对应的 directive 不存在时，使用默认组 OTHER_GROUPS
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT /* GET_ALL_RES_MAPPING_RULE_BY_FUNCTION */ "
                  "a.`value` as `value`,"
                  "(case when d.group_or_subplan is NULL then 'OTHER_GROUPS' else c.consumer_group end) as `group`, "
                  "(case when d.group_or_subplan is NULL then 0 else c.consumer_group_id end) as group_id  "
                  "FROM %s a left join %s d ON a.consumer_group = d.group_or_subplan AND d.plan = '%.*s' AND d.tenant_id = %ld,"
                  "%s c "
                  "WHERE a.TENANT_ID = %ld AND c.tenant_id = %ld AND a.tenant_id = c.tenant_id "
                  "AND a.attribute = 'FUNCTION' AND c.consumer_group = a.consumer_group",
                  t_a_res_name, t_d_directive, plan.length(), plan.ptr(), sql_tenant_id,
                  t_c_consumer_group_name,
                  sql_tenant_id, sql_tenant_id))) {
        LOG_WARN("fail format sql", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(t_a_res_name), K(tenant_id), K(ret));
      } else {
        while (OB_SUCC(result->next())) {
          ObResourceMappingRule rule;
          rule.set_tenant_id(tenant_id);
          rule.set_attr(ObString::make_string(""));
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, value, rule);
          EXTRACT_VARCHAR_FIELD_TO_CLASS_MYSQL(*result, group, rule);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, group_id, rule, uint64_t);
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

// 构建当前租户下全部 user_id -> group_id 的映射表
int ObResourceManagerProxy::get_all_resource_mapping_rules_by_user(
    uint64_t tenant_id,
    const common::ObString &plan,
    common::ObIArray<ObResourceUserMappingRule> &rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const char *t_a_res_name = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
      const char *t_b_user_name = OB_ALL_USER_TNAME;
      const char *t_c_consumer_group_name = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
      const char *t_d_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
      uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      // 当 resource plan 为空时，或者对应的 directive 不存在时，使用默认组 OTHER_GROUPS
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT /* GET_ALL_RES_MAPPING_RULE_BY_USER */ "
                  "user_id, "
                  "(case when d.group_or_subplan is NULL then 'OTHER_GROUPS' else c.consumer_group  end) as group_name,"
                  "(case when d.group_or_subplan is null then 0 else c.consumer_group_id end) as group_id  "
                  "FROM %s a left join %s d ON a.consumer_group = d.group_or_subplan AND d.plan = '%.*s' AND d.tenant_id = %ld,"
                  "%s b,%s c "
                  "WHERE a.`value` = b.user_name "
                  "AND a.TENANT_ID = %ld AND b.tenant_id = %ld AND c.tenant_id = %ld "
                  "AND a.TENANT_ID = b.tenant_id AND a.tenant_id = c.tenant_id "
                  "AND a.attribute = 'USER' AND c.consumer_group = a.consumer_group",
                  t_a_res_name, t_d_directive, plan.length(), plan.ptr(), sql_tenant_id,
                  t_b_user_name, t_c_consumer_group_name,
                  sql_tenant_id, sql_tenant_id, sql_tenant_id))) {
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

int ObResourceManagerProxy::get_resource_mapping_version(uint64_t tenant_id, int64_t &current_version)
{
  int ret = OB_SUCCESS;
  current_version = 0;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_SYS_STAT_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;

      uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
      if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id,
            "SELECT value from oceanbase.__all_sys_stat where name = 'ob_current_resource_mapping_version'"))) {
        LOG_WARN("fail to execute sql", K(ret));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", K(result), K(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          // return version = 0 if no version in __all_sys_stat which means no mapping rule defined.
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("ObMySQLResult next failed", K(ret));
        }
      } else {
        ObString version_str;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", version_str);
        bool is_valid = false;
        current_version = ObFastAtoi<int64_t>::atoi(version_str.ptr(),
            version_str.ptr() + version_str.length(), is_valid);
        if (!is_valid) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid key version", K(ret), K(version_str));
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::get_next_element(const ObString &value, int64_t &pos, char wrapper,
                                            ObString end_chars, ObString &element, bool &is_wrapped)
{
  int ret = OB_SUCCESS;
  bool allow_wrapper = '`' == wrapper || '"' == wrapper || '\'' == wrapper;
  element.reset();
  is_wrapped = false;
  while (pos < value.length() && value[pos] == ' ') {
    pos++;
  }
  if (OB_LIKELY(pos < value.length())) {
    if (allow_wrapper && value[pos] == wrapper) {
      is_wrapped = true;
      end_chars = ObString(1, &wrapper);
      pos++;
    }
    int64_t start_pos = pos;
    bool end_found = false;
    while (pos < value.length() && !end_found) {
      for (int64_t i = 0; i < end_chars.length() && !end_found; i++) {
        end_found = value[pos] == end_chars[i];
      }
      if (end_found) {
        element.assign_ptr(value.ptr() + start_pos, pos - start_pos);
        if (is_wrapped) {
          pos++;
        }
      } else {
        pos++;
      }
    }
    if (!end_found) {
      if (OB_UNLIKELY(is_wrapped)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("only left wrapper character found", K(ret), K(value));
      } else {
        element.assign_ptr(value.ptr() + start_pos, value.length() - start_pos);
      }
    }
  }
  return ret;
}

void ObResourceManagerProxy::upper_db_table_name(const bool need_modify_case,
                                                 const bool is_oracle_mode,
                                                 const ObNameCaseMode case_mode,
                                                 const bool is_wrapped,
                                                 ObString& name)
{
  if (!need_modify_case) {
    // do nothing.
  } else if (is_oracle_mode) {
    if (!is_wrapped) {
      char *ptr = str_toupper(name.ptr(), name.length());
    }
  } else {
    if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      char *ptr = str_tolower(name.ptr(), name.length());
    }
  }
}

// session = NULL.  load data from inner table into cache. Don't need modify case of schema name.
// session != NULL. Call procedure and need modify case of schema name according to case mode.
int ObResourceManagerProxy::parse_column_mapping_rule(ObString &value,
                                                      const sql::ObSQLSessionInfo *session,
                                                      ObString &db_name,
                                                      ObString &table_name,  ObString &column_name,
                                                      ObString &literal_value, ObString &user_name,
                                                      const ObNameCaseMode case_mode)
{
  int ret = OB_SUCCESS;
  db_name.reset();
  char wrap_char = '"';
  int64_t pos = 0;
  bool need_modify_case = false;
  if (NULL != session) {
    need_modify_case = true;
    if (!session->is_oracle_compatible()) {
      wrap_char = '`';
    }
    // currently used database.
    db_name = session->get_database_name();
  } else if (OB_UNLIKELY(value.empty() || (value[0] != '`' && value[0] != '"'))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("formalized value stored in inner table should start with wrapper char", K(ret), K(value));
  } else {
    wrap_char = value[0];
  }
  const bool is_oracle_mode = '"' == wrap_char;
  // parse databasename.tablename.columnname
  if (OB_SUCC(ret)) {
    // name1 = database_name, name2 = table_name
    // OR name1 = table_name, name2 = column_name
    ObString name1;
    ObString name2;
    bool is_wrapped1 = false;
    bool is_wrapped2 = false;
    if (OB_FAIL(get_next_element(value, pos, wrap_char, ObString("."), name1, is_wrapped1))) {
      LOG_WARN("get next element failed", K(ret));
    } else if (FALSE_IT(pos++)) {
    } else if (OB_FAIL(get_next_element(value, pos, wrap_char, ObString(". ="), name2, is_wrapped2))) {
      LOG_WARN("get next element failed", K(ret));
    } else if (pos >= value.length()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("literal value expected", K(ret), K(value));
    } else if ('.' == value[pos]) {
      db_name = name1;
      table_name = name2;
      pos++;
      upper_db_table_name(need_modify_case, is_oracle_mode, case_mode, is_wrapped1, db_name);
      upper_db_table_name(need_modify_case, is_oracle_mode, case_mode, is_wrapped2, table_name);
      if (OB_FAIL(get_next_element(value, pos, wrap_char, ObString(" ="), column_name, is_wrapped2))) {
        LOG_WARN("get next element failed", K(ret));
      } else {
        upper_db_table_name(need_modify_case, is_oracle_mode, OB_ORIGIN_AND_INSENSITIVE, is_wrapped2, column_name);
      }
    } else {
      table_name = name1;
      column_name = name2;
      upper_db_table_name(need_modify_case, is_oracle_mode, case_mode, is_wrapped1, table_name);
      // column name is case insensitive in mysql mode.
      upper_db_table_name(need_modify_case, is_oracle_mode, OB_ORIGIN_AND_INSENSITIVE, is_wrapped2, column_name);
    }
  }
  // parse literal
  if (OB_SUCC(ret)) {
    ObString literal;
    if (OB_UNLIKELY(db_name.empty() || table_name.empty() || column_name.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema name is empty", K(ret), K(db_name), K(table_name), K(column_name));
    } else {
      while (pos < value.length() && value[pos] == ' ') {
        pos++;
      }
      bool is_wrapped = false;
      char literal_wrapper = '\'';
      if (OB_UNLIKELY(pos >= value.length())) {
        LOG_WARN("literal value expected", K(ret), K(value), K(db_name), K(table_name),
                  K(column_name), K(pos));
      } else if (OB_UNLIKELY('=' != value[pos])) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expect equal before literal value", K(ret), K(value), K(pos));
      } else if (FALSE_IT(pos++)) {
      } else if (OB_FAIL(get_next_element(value, pos, literal_wrapper, ObString(" "), literal_value, is_wrapped))) {
        LOG_WARN("get literal value failed", K(ret));
      } else if (OB_UNLIKELY(literal_value.empty())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("literal value is empty", K(ret), K(value), K(pos));
      }
    }
  }
  // parse user name
  if (OB_SUCC(ret)) {
    while (pos < value.length() && value[pos] == ' ') {
      pos++;
    }
    if (pos < value.length()) {
      ObString for_str;
      bool is_wrapped = false;
      if (OB_FAIL(get_next_element(value, pos, ' ', ObString(" "), for_str, is_wrapped))) {
        LOG_WARN("get FOR str failed", K(ret));
      } else if (OB_UNLIKELY(0 != for_str.case_compare("for"))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expect \"FOR\" before user name", K(ret), K(value));
      } else if (OB_FAIL(get_next_element(value, pos, wrap_char, ObString(" "), user_name, is_wrapped))) {
        LOG_WARN("get literal value failed", K(ret));
      } else {
        // user name is case sensitive in mysql mode.
        upper_db_table_name(need_modify_case, is_oracle_mode, OB_ORIGIN_AND_SENSITIVE, is_wrapped, user_name);
      }
    }
  }
  LOG_TRACE("parse column mapping rule end", K(ret), K(value), K(pos), K(db_name), K(table_name),
            K(column_name), K(literal_value), K(user_name));
  return ret;
}

// 构建当前租户下全部 table_id+column_id -> rule_id 的映射表
int ObResourceManagerProxy::get_all_resource_mapping_rules_by_column(
    uint64_t tenant_id,
    const common::ObString &plan,
    ObIAllocator &allocator,
    common::ObIArray<ObResourceColumnMappingRule> &rules)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_MAPPING_RULE_TID);
    ObSchemaGetterGuard schema_guard;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null");
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        ObSqlString sql;
        const char *t_a_res_name = OB_ALL_RES_MGR_MAPPING_RULE_TNAME;
        const char *t_c_consumer_group_name = OB_ALL_RES_MGR_CONSUMER_GROUP_TNAME;
        const char *t_d_directive = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
        uint64_t sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
        // If consumer_group is NULL or not exist, consumer_group_id = -1, that means mapping rule is deleted.
        // Else if not specify cpu usage of consumer_group in current plan, consumer_group_id = 0, that means use OTHER_GROUP.
        if (OB_FAIL(sql.assign_fmt(
                    "SELECT /* GET_ALL_RES_MAPPING_RULE_BY_COLUMN */ "
                    "a.value, "
                    "(case when c.consumer_group_id is null then -1 when d.group_or_subplan is null then 0 else c.consumer_group_id end) as group_id  "
                    "FROM %s a left join %s d ON a.consumer_group = d.group_or_subplan AND d.plan = '%.*s' AND d.tenant_id = %ld "
                    "left join %s c ON c.consumer_group = a.consumer_group and c.tenant_id = %ld "
                    "WHERE a.TENANT_ID = %ld AND a.attribute = 'COLUMN'",
                    t_a_res_name, t_d_directive, plan.length(), plan.ptr(), sql_tenant_id,
                    t_c_consumer_group_name,
                    sql_tenant_id, sql_tenant_id))) {
          LOG_WARN("fail format sql", K(ret));
        } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", K(sql), K(ret));
        } else if (NULL == (result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail query sql", K(t_a_res_name), K(tenant_id), K(ret));
        } else {
          while (OB_SUCC(result->next())) {
            ObResourceColumnMappingRule rule;
            rule.set_tenant_id(tenant_id);
            ObString value;
            EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, group_id, rule, uint64_t);
            EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", value);
            if (OB_SUCC(ret)) {
              ObString db_name;
              ObString table_name;
              ObString column_name;
              ObString literal_value;
              ObString user_name;
              uint64_t database_id = OB_INVALID_ID;
              ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
              if (OB_FAIL(parse_column_mapping_rule(value, NULL, db_name, table_name, column_name,
                          literal_value, user_name, case_mode))) {
                LOG_WARN("parse column mapping rule failed", K(ret), K(value));
              } else if (OB_FAIL(schema_guard.get_database_id(tenant_id, db_name, database_id))) {
                LOG_WARN("get database_id", K(ret));
              } else if (OB_INVALID_ID == database_id) {
                // do nothing
              } else if (OB_FAIL(schema_guard.get_tenant_name_case_mode(tenant_id, case_mode))) {
                LOG_WARN("get tenant name case mode failed", K(ret));
              } else if (OB_FAIL(rule.write_string_values(tenant_id, table_name, column_name,
                                                          literal_value, user_name, allocator))) {
                LOG_WARN("write string values failed", K(ret));
              } else {
                rule.set_database_id(database_id);
                // get and set case mode in oracle mode is ok. When TableNameHashWrapper compare
                // other with ObCompareNameWithTenantID, case mode will be set according to compat mode.
                rule.set_case_mode(case_mode);
                if (OB_FAIL(rules.push_back(rule))) {
                  LOG_WARN("fail push back rules", K(rule), K(ret));
                  rule.reset(allocator);
                }
              }
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
  }
  return ret;
}

int ObResourceManagerProxy::get_iops_config(
                            const uint64_t tenant_id,
                            const common::ObString &plan,
                            const common::ObString &group,
                            ObPlanDirective &directive)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  TransGuard trans_guard(trans, tenant_id, ret);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_TENANT_ID;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (trans_guard.ready()) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &trans, tenant_id, OB_ALL_RES_MGR_DIRECTIVE_TID);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      ObSqlString sql;
      const char *tname = OB_ALL_RES_MGR_DIRECTIVE_TNAME;
      if (OB_FAIL(sql.assign_fmt(
                  "SELECT /* GET IOPS CONFIG */ min_iops, max_iops FROM %s "
                  "WHERE TENANT_ID = %ld AND PLAN = '%.*s' AND GROUP_OR_SUBPLAN = '%.*s'",
                  tname, ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                  plan.length(), plan.ptr(), group.length(), group.ptr()))) {
        LOG_WARN("fail format sql", K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(sql), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail query sql", K(tname), K(tenant_id), K(ret));
      } else {
        int64_t affected_rows = 0;
        while (OB_SUCC(result->next())) {
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, min_iops, directive, int64_t);
          EXTRACT_INT_FIELD_TO_CLASS_MYSQL(*result, max_iops, directive, int64_t);
          ++affected_rows;
        }
        if (OB_ITER_END == ret) {
          if (OB_UNLIKELY(!is_single_row(affected_rows))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected value. expect only 1 row affected", K(ret), K(affected_rows), K(group), K(plan));
          } else {
            ret = OB_SUCCESS;
          }
        } else {
          LOG_WARN("fail get next row", K(ret), K(tname), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObResourceManagerProxy::reset_all_mapping_rules()
{
  int ret = G_RES_MGR.get_mapping_rule_mgr().reset_mapping_rules();
  return ret;
}