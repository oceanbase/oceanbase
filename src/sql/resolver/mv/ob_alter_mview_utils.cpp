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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/mv/ob_alter_mview_utils.h"
#include "sql/resolver/mv/ob_mv_provider.h"
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
  } else if (data_version < DATA_VERSION_4_3_5_1) {
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
      ret = OB_NOT_SUPPORTED;
      LOG_WARN(
          "altering the 'on query computation' attribute of materialized view is not supported yet",
          KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "altering the 'on query computation' attribute of materialized view is");
      // alter_mview_arg.is_alter_on_query_computation_ = true;
      // alter_mview_arg.enable_on_query_computation_ = node.int32_values_[1];
    } else if (2 == node.int32_values_[0]) {
      // alter_mview_arg.is_alter_query_rewrite_ = true;
      // alter_mview_arg.enable_query_rewrite_ = node.int32_values_[1];
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("altering the 'query rewrite' attribute of materialized view is not supported yet",
               KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "altering the 'query rewrite' attribute of materialized view is");
    } else if (3 == node.int32_values_[0]) {
      ParseNode *parallel_node = node.children_[0];
      if (OB_ISNULL(parallel_node) || OB_ISNULL(parallel_node->children_[0]) ||
          OB_UNLIKELY(T_PARALLEL != parallel_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid parallel node", K(node.children_[0]), K(ret));
      } else {
        const int64_t refresh_dop = parallel_node->children_[0]->value_;
        alter_mview_arg.set_refresh_dop(refresh_dop);
      }
    } else {
      ParseNode *refresh_info_node = node.children_[0];
      if (OB_ISNULL(refresh_info_node) || OB_ISNULL(table_schema) ||
          OB_UNLIKELY(T_MV_REFRESH_INFO != refresh_info_node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected refresh info node", KR(ret), K(refresh_info_node), K(table_schema));
      } else if (refresh_info_node->int32_values_[0] == 0) {
        // int32_t refresh_method = refresh_info_node->int32_values_[1];
        // alter_mview_arg.is_alter_refresh_method_ = true;
        // switch (refresh_method) {
        // case 0:
        //   alter_mview_arg.refresh_method_ = ObMVRefreshMethod::FAST;
        //   break;
        // case 1:
        //   alter_mview_arg.refresh_method_ = ObMVRefreshMethod::COMPLETE;
        //   break;
        // case 2:
        //   alter_mview_arg.refresh_method_ = ObMVRefreshMethod::FORCE;
        //   break;
        // default:
        //   ret = OB_ERR_UNEXPECTED;
        //   LOG_WARN("unexpected refresh method", KR(ret), K(refresh_method));
        // }
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("altering the refresh method of materialized view is not supported yet", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering the refresh method of materialized view is");
      } else if (refresh_info_node->int32_values_[0] == 1) {
        // NEVER REFRESH BRANCH
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("altering the refresh method of materialized view is not supported yet", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "altering the refresh method of materialized view is");
      } else if (refresh_info_node->int32_values_[0] == 2) {
        ParseNode *interval_node = refresh_info_node->children_[1];
        if (OB_ISNULL(interval_node)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", KR(ret));
        } else if (OB_FAIL(resolve_interval_node(*interval_node, session_info, allocator,
                                                 resolver_params, alter_mview_arg))) {
          LOG_WARN("failed to resolve interval node", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (ObMVRefreshMethod::FAST == alter_mview_arg.get_refresh_method() ||
          alter_mview_arg.get_enable_on_query_computation()) {
        ObMVProvider mv_provider(tenant_id, table_schema->get_table_id());
        FastRefreshableNotes note;
        bool can_fast_refresh = false;
        share::SCN start;
        share::SCN end;
        start.set_min();
        end.set_min();
        if (OB_FAIL(mv_provider.init_mv_provider(start, end, schema_guard, session_info, note))) {
          LOG_WARN("fail to init mv provider", KR(ret), K(tenant_id));
        } else if (OB_FAIL(mv_provider.check_mv_refreshable(can_fast_refresh))) {
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
      } else if (OB_FAIL(resolve_interval_node(*interval_node, session_info, allocator,
                                               resolver_params, alter_mlog_arg))) {
        LOG_WARN("failed to resolve interval node", KR(ret));
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

template<typename T>
int ObAlterMviewUtils::resolve_interval_node(const ParseNode &node,
                                             ObSQLSessionInfo *session_info,
                                             common::ObIAllocator *allocator,
                                             ObResolverParams &resolver_params,
                                             T &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session_info) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(session_info), K(allocator));
  } else if (T_MV_REFRESH_INTERVAL != node.type_ || 2 != node.num_child_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node type", KR(ret), K(node.type_), K(node.num_child_));
  } else {
    ParseNode *start_date = node.children_[0];
    ParseNode *next_date = node.children_[1];
    int64_t current_time =
        ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
    int64_t start_time = OB_INVALID_TIMESTAMP;
    if (OB_NOT_NULL(start_date)) {
      if (T_MV_REFRESH_START_EXPR != start_date->type_ || OB_ISNULL(start_date->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected node type", KR(ret), K(start_date->type_),
                 K(start_date->children_[0]));
      } else if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(
                     resolver_params, *session_info, *(start_date->children_[0]), *allocator,
                     start_time))) {
        LOG_WARN("failed to resolve date expr to timestamp", KR(ret));
      } else if (start_time < current_time) {
        ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
        LOG_WARN("the parameter start date must evaluate to a time in the future", KR(ret),
                 K(current_time), K(start_time));
        LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "start date");
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(next_date)) {
      int64_t next_time = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(
              resolver_params, *session_info, *next_date, *allocator, next_time))) {
        LOG_WARN("failed to resolve date expr to timestamp", KR(ret));
      } else if (next_time < current_time) {
        ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
        LOG_WARN("the parameter next date must evaluate to a time in the future", KR(ret),
                 K(current_time), K(next_time));
        LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "next date");
      } else if (OB_INVALID_TIMESTAMP == start_time) {
        start_time = next_time;
      }

      if (OB_SUCC(ret)) {
        ObString next_date_str(next_date->str_len_, next_date->str_value_);
        if (OB_FAIL(ob_write_string(*allocator, next_date_str, next_date_str))) {
          LOG_WARN("fail to write string", KR(ret));
        } else {
          arg.set_next_time_expr(next_date_str);
        }
      }
    }
    if (OB_SUCC(ret)) {
      arg.set_start_time(start_time);
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
