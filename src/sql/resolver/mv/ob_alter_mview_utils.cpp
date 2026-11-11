/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_alter_mview_utils.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "sql/resolver/ddl/ob_mview_resolver_helper.h"
#include "storage/mview/ob_mview_sched_job_utils.h"

namespace oceanbase
{
namespace sql
{
int ObAlterMviewUtils::resolve_mv_options(const ParseNode &node,
                                          ObAlterTableStmt *alter_table_stmt,
                                          const share::schema::ObTableSchema *table_schema,
                                          ObResolverParams &resolver_params)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = resolver_params.session_info_;
  common::ObIAllocator *allocator = resolver_params.allocator_;
  ObAlterMViewArg *alter_mview_arg = NULL;
  uint64_t data_version = 0;
  ObString exec_env;
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  if (OB_ISNULL(session_info) || OB_ISNULL(allocator)
      || OB_ISNULL(alter_table_stmt) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(session_info), K(allocator), K(alter_table_stmt),
             K(table_schema));
  } else if (OB_UNLIKELY(!table_schema->is_materialized_view())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("altering non-materialized-view objects is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering non-materialized-view objects is");
  } else if (OB_UNLIKELY(T_MV_OPTION_LIST != node.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mview options node type", KR(ret), K(node.type_));
  } else if (OB_FAIL(ObMViewResolverHelper::check_duplicate_mv_options(&node))) {
    LOG_WARN("failed to check mview options", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info->get_effective_tenant_id(), data_version))) {
    LOG_WARN("get tenant data version failed", KR(ret));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_5_1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version is less than 4.3.5.1, altering mv options is not supported",
             KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant data version is less than 4.3.5.1, altering mv options is");
  } else if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to generate exec env", KR(ret));
  } else if (OB_FAIL(ob_write_string(*allocator, ObString(pos, buf), exec_env))) {
    LOG_WARN("failed to copy exec env", KR(ret));
  } else {
    alter_mview_arg = &alter_table_stmt->get_alter_table_arg().alter_mview_arg_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < node.num_child_; ++i) {
    const ParseNode &option_node = *node.children_[i];
    if (T_PARALLEL == option_node.type_) {
      int64_t table_dop = 0;
      if (OB_FAIL(ObMViewResolverHelper::resolve_refresh_parallel_node(&option_node, table_dop))) {
        LOG_WARN("failed to resolve table parallel node", KR(ret));
      } else {
        alter_mview_arg->set_table_dop(table_dop);
      }
    } else if (T_MV_REFRESH_INFO == option_node.type_) {
      if (OB_FAIL(resolve_mv_refresh_info(option_node, table_schema->get_table_id(),
                                          resolver_params, *alter_mview_arg))) {
        LOG_WARN("failed to resolve mview refresh info", KR(ret));
      }
    } else if (T_MV_BUILD_OPT == option_node.type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("altering the build mode of materialized view is not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering the build mode of materialized view is");
    } else if (T_MV_OPTIONS == option_node.type_) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("altering materialized view query options is not supported",
               KR(ret), K(option_node.int32_values_[0]));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     1 == option_node.int32_values_[0]
                       ? "altering the 'on query computation' attribute of materialized view is"
                       : "altering the 'query rewrite' attribute of materialized view is");
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mview option type", KR(ret), K(option_node.type_));
    }
  }
  if (OB_SUCC(ret)) {
    alter_mview_arg->set_exec_env(exec_env);
  }
  LOG_INFO("[ALTER MVIEW] resolve mv options", KR(ret), KPC(alter_mview_arg));
  return ret;
}

int ObAlterMviewUtils::resolve_mv_refresh_info(const ParseNode &node,
                                               const uint64_t mview_id,
                                               ObResolverParams &resolver_params,
                                               ObAlterMViewArg &alter_mview_arg)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = resolver_params.session_info_;
  ObMVRefreshInfo refresh_info;
  if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret));
  } else if (OB_FAIL(ObMViewResolverHelper::resolve_mv_refresh_info(&node, resolver_params,
                                                                     refresh_info))) {
    LOG_WARN("failed to resolve mview refresh info", KR(ret));
  } else if (OB_UNLIKELY(ObMVRefreshMethod::MAX != refresh_info.refresh_method_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("altering the refresh method of materialized view is not supported yet", KR(ret),
             K(refresh_info.refresh_method_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering the refresh method of materialized view is");
  } else if (!refresh_info.next_time_expr_.empty()
             && OB_FAIL(check_complete_refresh_min_interval(
                    session_info->get_effective_tenant_id(), mview_id,
                    refresh_info.refresh_interval_ / 1000000L))) {
    LOG_WARN("failed to check complete refresh min interval", KR(ret),
             K(refresh_info.refresh_interval_));
  } else {
    if (refresh_info.sched_suspended_) {
      alter_mview_arg.set_sched_suspended();
    }
    if (ObMVRefreshMode::MAX != refresh_info.refresh_mode_) {
      alter_mview_arg.set_refresh_mode(refresh_info.refresh_mode_);
    }
    if (-1 != refresh_info.refresh_dop_) {
      alter_mview_arg.set_refresh_dop(refresh_info.refresh_dop_);
    }
    if (ObMVNestedRefreshMode::MAX != refresh_info.nested_refresh_mode_) {
      alter_mview_arg.set_alter_nested_refresh_mode(refresh_info.nested_refresh_mode_);
    }
    if (OB_INVALID_VERSION != refresh_info.compat_version_) {
      alter_mview_arg.set_compat_version(refresh_info.compat_version_);
    }
    if (!refresh_info.start_time_.is_null()) {
      alter_mview_arg.set_start_time(refresh_info.start_time_.get_timestamp());
    }
    if (!refresh_info.next_time_expr_.empty()) {
      alter_mview_arg.set_next_time_expr(refresh_info.next_time_expr_);
      alter_mview_arg.set_refresh_interval(refresh_info.refresh_interval_);
    }
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
      if (OB_ISNULL(interval_node)
          || OB_UNLIKELY(T_MV_REFRESH_INTERVAL != interval_node->type_ || 2 != interval_node->num_child_)
          || OB_ISNULL(interval_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected refresh interval node", KR(ret), K(interval_node));
      } else {
        int64_t start_time = OB_INVALID_TIMESTAMP;
        ObString next_time_expr;
        int64_t period_sec = 0;
        if (OB_FAIL(ObMViewResolverHelper::resolve_refresh_interval_node(
                        interval_node->children_[0], interval_node->children_[1],
                        resolver_params, start_time, next_time_expr, period_sec))) {
          LOG_WARN("failed to resolve refresh interval exprs", KR(ret));
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
    bool is_allowed = false;
    if (T_COLUMN_ADD == type || T_COLUMN_MODIFY == type || T_COLUMN_ALTER == type) {
      is_allowed = true;
    } else if (T_COLUMN_CHANGE == type
               || T_COLUMN_RENAME == type
               || T_COLUMN_DROP == type) {
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(table_schema.get_tenant_id(), data_version))) {
        LOG_WARN("failed to get tenant data version", K(ret), K(table_schema));
      } else {
        is_allowed = data_version >= DATA_VERSION_5_0_2_0;
      }
    }
    if (OB_SUCC(ret) && !is_allowed) {
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
        || T_MV_OPTION_LIST == type
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
