/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_alter_mview_utils.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/resolver/ddl/ob_mview_resolver_helper.h"
#include "storage/mview/ob_mview_sched_job_utils.h"

namespace oceanbase
{
namespace sql
{
int ObAlterMviewUtils::resolve_mv_options(const ParseNode &node,
                                          ObSQLSessionInfo *session_info,
                                          ObAlterTableStmt *alter_table_stmt,
                                          const share::schema::ObTableSchema *table_schema,
                                          ObSchemaGetterGuard *schema_guard,
                                          common::ObIAllocator *allocator,
                                          ObResolverParams &resolver_params)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 0;
  uint64_t data_version = 0;

  if (OB_ISNULL(session_info) || OB_ISNULL(alter_table_stmt) || OB_ISNULL(table_schema) ||
      OB_ISNULL(schema_guard) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(session_info), K(alter_table_stmt),
             K(table_schema), K(schema_guard), K(allocator));
  } else if (!table_schema->is_materialized_view()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("altering non-materialized-view objects is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering non-materialized-view objects is");
  } else if (OB_UNLIKELY(T_MV_OPTIONS != node.type_ || 1 != node.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mv options node", KR(ret), K(node.type_), K(node.num_child_));
  } else if (FALSE_IT(tenant_id = session_info->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_5_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.5.1, altering mv options is not supported",
             K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant data version is less than 4.3.5.1, altering mv options is");
  } else {
    ObAlterMViewArg &alter_mview_arg = alter_table_stmt->get_alter_table_arg().alter_mview_arg_;
    ObString exec_env;
    char buf[OB_MAX_PROC_ENV_LENGTH];
    int64_t pos = 0;
    OZ(ObExecEnv::gen_exec_env(*session_info, buf, OB_MAX_PROC_ENV_LENGTH, pos));
    OX(exec_env.assign(buf, pos));
    OZ(ob_write_string(*allocator, exec_env, exec_env));
    OX(alter_mview_arg.set_exec_env(exec_env));
    if (OB_FAIL(ret)) {
    } else if (1 == node.int32_values_[0]) {
      // ON QUERY COMPUTATION
      ret = OB_NOT_SUPPORTED;
      LOG_WARN(
          "altering the 'on query computation' attribute of materialized view is not supported yet",
          KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "altering the 'on query computation' attribute of materialized view is");
      // alter_mview_arg.is_alter_on_query_computation_ = true;
      // alter_mview_arg.enable_on_query_computation_ = node.int32_values_[1];
    } else if (2 == node.int32_values_[0]) {
      // QUERY REWRITE
      // alter_mview_arg.is_alter_query_rewrite_ = true;
      // alter_mview_arg.enable_query_rewrite_ = node.int32_values_[1];
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("altering the 'query rewrite' attribute of materialized view is not supported yet",
               KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "altering the 'query rewrite' attribute of materialized view is");
    } else if (3 == node.int32_values_[0]) {
      // PARALLEL
      ParseNode *parallel_node = node.children_[0];
      if (OB_ISNULL(parallel_node) || OB_ISNULL(parallel_node->children_[0]) ||
          OB_UNLIKELY(T_PARALLEL != parallel_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parallel node", K(node.children_[0]), K(ret));
      } else if (OB_UNLIKELY(parallel_node->children_[0]->value_ < 0)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid table dop", K(parallel_node->children_[0]->value_), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "value for PARALLEL or DEGREE must be equal to or greater than 0!");
      } else {
        const int64_t table_dop = parallel_node->children_[0]->value_;
        alter_mview_arg.set_table_dop(table_dop);
      }
    } else if (0 == node.int32_values_[0]) {
      // REFRESH INFO
      ParseNode *refresh_info_node = node.children_[0];
      if (OB_ISNULL(refresh_info_node) || OB_ISNULL(table_schema) ||
          OB_UNLIKELY(T_MV_REFRESH_INFO != refresh_info_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected refresh info node", KR(ret), K(refresh_info_node), K(table_schema));
      } else if (1 == refresh_info_node->int32_values_[0]) {
        // NEVER REFRESH BRANCH
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("altering the refresh method of materialized view is not supported yet", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering the refresh method of materialized view is");
      } else if (0 == refresh_info_node->int32_values_[0]) {
        if (OB_UNLIKELY(5 != refresh_info_node->num_child_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected refresh info node", K(ret), K(refresh_info_node->type_), K(refresh_info_node->num_child_));
        } else {
          const int32_t refresh_method = refresh_info_node->int32_values_[1];
          const ParseNode *refresh_parallel_node = refresh_info_node->children_[0];
          const ParseNode *nested_refresh_node = refresh_info_node->children_[1];
          const ParseNode *refresh_on_node = refresh_info_node->children_[2];
          const ParseNode *refresh_interval_node = refresh_info_node->children_[3];
          const ParseNode *compat_version_node = refresh_info_node->children_[4];
          if (-1 != refresh_method) {
            // REFRESH METHOD BRANCH
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("altering the refresh method of materialized view is not supported yet", KR(ret));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering the refresh method of materialized view is");
          } else if (NULL != refresh_parallel_node) {
            // REFRESH PARALLEL BRANCH
            int64_t refresh_dop = 0;
            if (OB_FAIL(ObMViewResolverHelper::resolve_refresh_parallel_node(refresh_parallel_node,
                                                                             refresh_dop))) {
              LOG_WARN("failed to resolve refresh parallel node", KR(ret));
            } else {
              alter_mview_arg.set_refresh_dop(refresh_dop);
            }
          } else if (NULL != nested_refresh_node) {
            // NESTED REFRESH MODE BRANCH
            share::schema::ObMVNestedRefreshMode nested_refresh_mode;
            if (OB_UNLIKELY(data_version < MOCK_DATA_VERSION_4_3_5_3
                            || (data_version >= DATA_VERSION_4_4_0_0 && data_version < MOCK_DATA_VERSION_4_4_2_0)
                            || (data_version >= DATA_VERSION_4_5_0_0 && data_version < DATA_VERSION_4_5_1_0))) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("tenant data version is less than 4.3.5.3 or between 4.4.0.0 and 4.4.2.0 or between 4.5.0.0 and 4.5.1.0, alter mv nested refresh mode is not supported", K(ret), K(data_version));
            } else if (OB_FAIL(ObMViewResolverHelper::resolve_nested_refresh_node(nested_refresh_node,
                                                                                  nested_refresh_mode))) {
              LOG_WARN("failed to resolve nested refresh node", KR(ret));
            } else {
              alter_mview_arg.set_alter_nested_refresh_mode(nested_refresh_mode);
            }
          } else if (NULL != refresh_on_node) {
            // REFRESH ON BRANCH
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected refresh on node", KR(ret), K(refresh_on_node));
          } else if (NULL != refresh_interval_node) {
            // REFRESH INTERVAL BRANCH
            int64_t start_time = OB_INVALID_TIMESTAMP;
            ObString next_time_expr;
            int64_t period_sec = 0;
            if (OB_FAIL(ObMViewResolverHelper::resolve_refresh_interval_node(refresh_interval_node,
                                                                             session_info,
                                                                             allocator,
                                                                             resolver_params,
                                                                             start_time,
                                                                             next_time_expr,
                                                                             period_sec))) {
              LOG_WARN("failed to resolve interval node", KR(ret));
            } else if ((!next_time_expr.empty())
                       && OB_FAIL(check_complete_refresh_min_interval(tenant_id,
                                                                      table_schema->get_table_id(),
                                                                      period_sec))) {
              LOG_WARN("failed to check complete refresh min interval", KR(ret), K(period_sec));
            } else {
              alter_mview_arg.set_start_time(start_time);
              if (!next_time_expr.empty()) {
                alter_mview_arg.set_next_time_expr(next_time_expr);
              }
            }
          } else if (NULL != compat_version_node) {
            // COMPAT VERSION BRANCH
            if (OB_UNLIKELY(!((data_version >= MOCK_DATA_VERSION_4_4_2_2 && data_version < DATA_VERSION_4_5_0_0)
                              || (data_version >= DATA_VERSION_4_6_1_0)))) {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("tenant data version is less than 4.4.2.2, alter mv compat version is not supported", K(ret), K(data_version));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter mv compat version is");
            } else {
              uint64_t compat_version = 0;
              if (OB_FAIL(ObMViewResolverHelper::resolve_compat_version_node(
                      compat_version_node, tenant_id, compat_version))) {
                LOG_WARN("failed to resolve compat version node", KR(ret));
              } else {
                alter_mview_arg.set_compat_version(compat_version);
              }
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected refresh info node", KR(ret), K(refresh_info_node->int32_values_[0]), K(refresh_info_node->int32_values_[1]));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected refresh info node", KR(ret), K(refresh_info_node->int32_values_[0]));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mv options node", KR(ret), K(node.int32_values_[0]));
    }
    if (OB_SUCC(ret)) {
      if (ObMVRefreshMethod::FAST == alter_mview_arg.get_refresh_method() ||
          alter_mview_arg.get_enable_on_query_computation()) {
        FastRefreshableNotes note;
        bool can_fast_refresh = false;
        if (OB_FAIL(ObMVProvider::check_mv_refreshable(tenant_id,
                                                       table_schema->get_table_id(),
                                                       session_info,
                                                       schema_guard,
                                                       can_fast_refresh,
                                                       note))) {
          LOG_WARN("fail to check refresh type", KR(ret));
        } else if (!can_fast_refresh) {
          if (alter_mview_arg.get_enable_on_query_computation()) {
            ret = OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE;
            LOG_WARN("mv can not enable on query computation", KR(ret));
            LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE, table_schema->get_table_name(),
                           note.error_.ptr());
          } else {
            ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
            LOG_WARN("mv can not fast refresh", KR(ret));
            LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH, table_schema->get_table_name(),
                           note.error_.ptr());
          }
        }
      }
    }
    LOG_INFO("[ALTER MVIEW] resolve mv options", KR(ret), K(alter_mview_arg));
  }

  return ret;
}

int ObAlterMviewUtils::resolve_mlog_options(const ParseNode &node,
                                            ObSQLSessionInfo *session_info,
                                            ObAlterTableStmt *alter_table_stmt,
                                            common::ObIAllocator *allocator,
                                            ObResolverParams &resolver_params)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  if (OB_ISNULL(session_info) || OB_ISNULL(alter_table_stmt) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(session_info), K(alter_table_stmt), K(allocator));
  } else if (OB_UNLIKELY(T_ALTER_MLOG_OPTIONS != node.type_ || 1 != node.num_child_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mlog options node", K(node.type_), K(node.num_child_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.5.1, altering mlog options is not supported",
             K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant data version is less than 4.3.5.1, altering mlog options is");
  } else {
    ObAlterMLogArg &alter_mlog_arg = alter_table_stmt->get_alter_table_arg().alter_mlog_arg_;
    ObString exec_env;
    char buf[OB_MAX_PROC_ENV_LENGTH];
    int64_t pos = 0;
    OZ(ObExecEnv::gen_exec_env(*session_info, buf, OB_MAX_PROC_ENV_LENGTH, pos));
    OX(exec_env.assign(buf, pos));
    OZ(ob_write_string(*allocator, exec_env, exec_env));
    OX(alter_mlog_arg.set_exec_env(exec_env));
    if (OB_FAIL(ret)) {
    } else if (node.value_ == 1) {
      // ALTER MATERIALIZED VIEW LOG ON t1 PARALLEL 4;
      ParseNode *parallel_node = node.children_[0];
      if (OB_ISNULL(parallel_node) || OB_ISNULL(parallel_node->children_[0]) ||
          OB_UNLIKELY(T_PARALLEL != parallel_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parallel node", K(node.children_[0]), K(ret));
      } else if (OB_UNLIKELY(parallel_node->children_[0]->value_ < 1)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("invalid table dop", K(parallel_node->children_[0]->value_), K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "value for PARALLEL or DEGREE must be greater than 0!");
      } else {
        const int64_t table_dop = parallel_node->children_[0]->value_;
        alter_mlog_arg.set_table_dop(table_dop);
      }
    } else if (node.value_ == 2) {
      // ALTER MATERIALIZED VIEW LOG ON t1 PURGE NEXT sysdate() + INTERVAL 10 SECOND;
      ParseNode *interval_node = node.children_[0];
      if (OB_ISNULL(interval_node)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", KR(ret));
      } else {
        int64_t start_time = OB_INVALID_TIMESTAMP;
        ObString next_time_expr;
        int64_t period_sec = 0;
        if (OB_FAIL(ObMViewResolverHelper::resolve_refresh_interval_node(interval_node,
                                                                         session_info,
                                                                         allocator,
                                                                         resolver_params,
                                                                         start_time,
                                                                         next_time_expr,
                                                                         period_sec))) {
          LOG_WARN("failed to resolve interval node", KR(ret));
        } else {
          alter_mlog_arg.set_start_time(start_time);
          if (!next_time_expr.empty()) {
            alter_mlog_arg.set_next_time_expr(next_time_expr);
          }
        }
      }
    } else if (node.value_ == 3) {
      ParseNode *thres_node = node.children_[0];
      if (OB_ISNULL(thres_node) || OB_ISNULL(thres_node->children_[0]) ||
          OB_UNLIKELY(T_LOB_INROW_THRESHOLD != thres_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parallel node", K(node.children_[0]), K(ret));
      } else {
        const int64_t threshold = thres_node->children_[0]->value_;
        if (threshold < OB_MIN_LOB_INROW_THRESHOLD || threshold > OB_MAX_LOB_INROW_THRESHOLD) {
          ret = OB_INVALID_ARGUMENT;
          SQL_RESV_LOG(ERROR, "invalid inrow threshold", K(ret), K(threshold));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "lob inrow threshold, should be [0, 786432]");
        } else {
          alter_mlog_arg.set_lob_threshold(threshold);
        }
      }
    }
    LOG_INFO("[ALTER MLOG] resolve mlog options", KR(ret), K(alter_mlog_arg));
  }

  return ret;
}

int ObAlterMviewUtils::check_complete_refresh_min_interval(const uint64_t tenant_id,
                                                           const uint64_t mview_id,
                                                           const int64_t period_sec)
{
  int ret = OB_SUCCESS;
  share::schema::ObMViewInfo mview_info;
  int64_t mview_complete_refresh_min_interval = 600;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (tenant_config.is_valid()) {
    mview_complete_refresh_min_interval = tenant_config->_mv_complete_refresh_min_interval / 1000000L;
  }
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(share::schema::ObMViewInfo::fetch_mview_info(*GCTX.sql_proxy_, tenant_id,
                                                                  mview_id, mview_info))) {
    LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id), K(mview_id));
  } else if (ObMVRefreshMethod::COMPLETE == mview_info.get_refresh_method()
             && period_sec < mview_complete_refresh_min_interval) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("the interval of complete refresh is less than the minimum interval", KR(ret),
             K(period_sec), K(mview_complete_refresh_min_interval));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "the interval of complete refresh is less than the minimum interval");
  }
  return ret;
}

int ObAlterMviewUtils::check_column_option_for_mv_base_table(const ObTableSchema &table_schema,
                                                             const ObItemType type)
{
  int ret = OB_SUCCESS;
  if (table_schema.required_by_mv_refresh()) {
    if (T_COLUMN_ADD == type || T_COLUMN_MODIFY == type || T_COLUMN_ALTER == type) {
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "modify column to table required by materialized view is");
      LOG_WARN("modify column to table required by materialized view is not supported",
               KR(ret), K(table_schema.get_table_name()));
    }
    SQL_RESV_LOG(INFO, "resolve the master of mlog for modify column", K(ret), K(type), K(table_schema));
  }
  return ret;
}

int ObAlterMviewUtils::check_action_node_for_mv_base_table(const ObTableSchema &table_schema,
                                                           const ObItemType type)
{
  int ret = OB_SUCCESS;
  if (table_schema.required_by_mv_refresh()) {
    if (T_TABLE_OPTION_LIST == type
        || T_ALTER_COLUMN_OPTION == type
        || T_ALTER_INDEX_OPTION_ORACLE == type
        || T_ALTER_CHECK_CONSTRAINT_OPTION == type
        || T_DROP_CONSTRAINT == type
        || T_ALTER_FOREIGN_KEY_OPTION == type
        || T_ALTER_INDEX_OPTION == type
        || T_ALTER_MLOG_OPTIONS == type
        || T_MV_OPTIONS == type
        || T_ALTER_REFRESH_EXTERNAL_TABLE == type
        || T_ALTER_PARTITION_OPTION == type) {
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "this alter table to table required by materialized view is");
      LOG_WARN("this alter table to table required by materialized view is not supported", KR(ret),
               K(table_schema.get_table_name()));
    }
    SQL_RESV_LOG(INFO, "resolve the master of mlog for alter table", K(ret), K(type), K(table_schema));
  }
  return ret;
}

int ObAlterMviewUtils::check_partition_option_for_mv_base_table(const ObTableSchema &table_schema,
                                                                const ObItemType type)
{
  int ret = OB_SUCCESS;
  if (table_schema.required_by_mv_fast_refresh()) {
    if (false) {
      // todo: T_ALTER_PARTITION_ADD, it makes refresh failed because
      // the mlog query begin version is less than tablet create commit version
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "this alter table to table required by materialized view is");
      LOG_WARN("this alter table to table required by materialized view is not supported", KR(ret),
               K(table_schema.get_table_name()));
    }
    SQL_RESV_LOG(INFO, "resolve the master of mlog for alter table", K(ret), K(type), K(table_schema));
  }
  return ret;
}

int ObAlterMviewUtils::check_database_referenced_by_mv_from_other_database(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const uint64_t database_id)
{
  int ret = OB_SUCCESS;
  bool referenced_by_other_database_mv = false;
  if (OB_FAIL(ObMVDepUtils::check_database_referenced_by_mv_from_other_database(
                 sql_client, tenant_id, database_id, referenced_by_other_database_mv))) {
    LOG_WARN("failed to check materialized view reference", KR(ret), K(tenant_id), K(database_id));
  } else if (OB_UNLIKELY(referenced_by_other_database_mv)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop database with table required by mview from other database is not supported",
             KR(ret), K(tenant_id), K(database_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "drop database with table required by materialized view in other database is");
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
