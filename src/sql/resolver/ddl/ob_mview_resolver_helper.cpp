/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX  SQL_RESV

#include "sql/resolver/ddl/ob_mview_resolver_helper.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/ddl/ob_create_table_resolver_base.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "sql/resolver/mv/ob_mv_dep_utils.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "share/ob_license_utils.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "storage/mview/ob_mview_refresh.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/printer/ob_raw_expr_printer.h"

namespace oceanbase
{
namespace sql
{

int ObMViewResolverHelper::resolve_mview_table_organization(const ParseNode *table_option_node,
                                                            const ParseNode *mv_primary_key_node,
                                                            ObTableOrganizationMode &organization_mode,
                                                            ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  organization_mode = TOM_INDEX_ORGANIZED;
  if (NULL == table_option_node) {
    // do nothing
  } else if (OB_ISNULL(resolver.session_info_)
             || OB_UNLIKELY(T_TABLE_OPTION_LIST != table_option_node->type_
                            || (0 < table_option_node->num_child_ && NULL == table_option_node->children_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table option or session info", KR(ret), K(resolver.session_info_),
             K(table_option_node->type_), K(table_option_node->num_child_), K(table_option_node->children_));
  } else {
    const ParseNode *option_node = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_option_node->num_child_; ++i) {
      if (OB_ISNULL(option_node = table_option_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null table option node", KR(ret), K(i), K(table_option_node->num_child_));
      } else if (T_ORGANIZATION != option_node->type_) {
        // do nothing
      } else if (resolver.session_info_->is_oracle_compatible()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("oracle mode should not specify organization type", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "specify organization type in oracle mode");
      } else if (OB_UNLIKELY(1 != option_node->num_child_)
                 || OB_ISNULL(option_node->children_)
                 || OB_ISNULL(option_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected organization option node", KR(ret), K(i), K(option_node->num_child_),
                 K(option_node->children_), K(option_node));
      } else if (OB_UNLIKELY(T_ORGANIZATION_INDEX != option_node->children_[0]->type_
                             && T_ORGANIZATION_HEAP != option_node->children_[0]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected organization type", KR(ret), K(i), K(option_node->children_[0]->type_));
      } else if (T_ORGANIZATION_INDEX == option_node->children_[0]->type_) {
        organization_mode = TOM_INDEX_ORGANIZED;
      } else if (T_ORGANIZATION_HEAP == option_node->children_[0]->type_) {
        organization_mode = TOM_HEAP_ORGANIZED;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(TOM_HEAP_ORGANIZED == organization_mode && NULL != mv_primary_key_node)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("primary key on heap organized materialized view is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "primary key on heap organized materialized view is");
  }
  return ret;
}


int ObMViewResolverHelper::resolve_materialized_view_container_table(ParseNode *partition_node,
                                                                     ParseNode *mv_primary_key_node,
                                                                     const ObTableOrganizationMode organization_mode,
                                                                     const uint64_t tenant_data_version,
                                                                     ObTableSchema &container_table_schema,
                                                                     ObSEArray<ObConstraint,4>& csts,
                                                                     ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = NULL;
  container_table_schema.set_table_type(ObTableType::USER_TABLE);
  container_table_schema.get_view_schema().reset();
  container_table_schema.set_max_dependency_version(OB_INVALID_VERSION);
  if (OB_ISNULL(resolver.schema_checker_) || OB_ISNULL(schema_guard = resolver.schema_checker_->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema checker or schema guard is null", KR(ret), K(resolver.schema_checker_), K(schema_guard));
  } else if (OB_FAIL(resolver.resolve_partition_option(partition_node, container_table_schema, true))) {
    LOG_WARN("fail to resolve_partition_option", KR(ret));
  } else if (OB_FAIL(resolver.set_table_option_to_schema(container_table_schema))) {
    SQL_RESV_LOG(WARN, "set table option to schema failed", KR(ret));
  } else if (OB_FALSE_IT(container_table_schema.set_table_organization_mode(organization_mode))) {
  } else if (NULL != mv_primary_key_node
             && OB_FAIL(resolve_primary_key_node(*mv_primary_key_node, container_table_schema, resolver))) {
    LOG_WARN("failed to resolve primary key node", K(ret));
  } else if (0 < container_table_schema.get_rowkey_column_num()) {  // create mv with primary key
    container_table_schema.set_table_pk_mode(ObTablePKMode::TPKM_OLD_NO_PK);
    container_table_schema.set_table_pk_exists_mode(ObTablePrimaryKeyExistsMode::TOM_TABLE_WITH_PK);
    if (lib::is_oracle_mode() && OB_FAIL(resolver.resolve_pk_constraint_node(*mv_primary_key_node, ObString::make_empty_string(), csts))) {
      LOG_WARN("failed to add pk constraint for oracle mode", KR(ret));
    }
  } else if (OB_FAIL(resolver.add_hidden_tablet_seq_col(container_table_schema))) {
    LOG_WARN("fail to add hidden pk", KR(ret));
  } else {  // create mv without primary key
    container_table_schema.set_table_pk_mode(TPKM_TABLET_SEQ_PK);
    container_table_schema.set_table_pk_exists_mode(ObTablePrimaryKeyExistsMode::TOM_TABLE_WITHOUT_PK);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(container_table_schema.check_primary_key_cover_partition_column(*schema_guard))) {
    SQL_RESV_LOG(WARN, "fail to check primary key cover partition column", KR(ret));
  } else {
    container_table_schema.set_collation_type(resolver.collation_type_);
    container_table_schema.set_charset_type(resolver.charset_type_);
    container_table_schema.set_mv_container_table(IS_MV_CONTAINER_TABLE);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObResolverUtils::check_heap_organized_mview_container_schema(
                 container_table_schema, tenant_data_version))) {
    LOG_WARN("fail to check materialized view container schema", KR(ret), K(container_table_schema));
  }
  return ret;
}

int ObMViewResolverHelper::resolve_primary_key_node(ParseNode &pk_node,
                                                    ObTableSchema &table_schema,
                                                    ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  ParseNode *cur_node = NULL;
  if (OB_UNLIKELY(2 > pk_node.num_child_) || OB_ISNULL(cur_node = pk_node.children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(pk_node.num_child_), K(cur_node));
  } else if (OB_UNLIKELY(T_COLUMN_LIST != cur_node->type_ || cur_node->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected node", K(ret), K(get_type_name(cur_node->type_)), K(cur_node->num_child_));
  } else {
    ParseNode *key_node = NULL;
    int64_t pk_data_length = 0;
    ObColumnSchemaV2 *col = NULL;
    for (int32_t i = 0; OB_SUCC(ret) && i < cur_node->num_child_; ++i) {
      if (OB_ISNULL(key_node = cur_node->children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i));
      } else if (OB_FAIL(resolver.add_primary_key_part(ObString(key_node->str_len_, key_node->str_value_),
                                                                         table_schema, i,
                                                                         pk_data_length, col))) {
        LOG_WARN("failed to add primary key part", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret) || lib::is_oracle_mode()) {
    } else if (OB_UNLIKELY(3 != pk_node.num_child_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params", K(ret), K(pk_node.num_child_));
    } else {
      if (NULL != (cur_node = pk_node.children_[1])) {
        table_schema.set_index_using_type(T_USING_HASH == cur_node->type_ ? share::schema::USING_HASH
                                                                          : share::schema::USING_BTREE);
      }
      if (NULL != (cur_node = pk_node.children_[2])) {
        if (OB_FAIL(table_schema.set_pk_comment(ObString(cur_node->str_len_, cur_node->str_value_)))) {
          LOG_WARN("fail to set primary key comment", K(ret), K(ObString(cur_node->str_len_, cur_node->str_value_)));
        }
      }
    }
  }
  return ret;
}

int ObMViewResolverHelper::check_duplicate_mv_options(const ParseNode *options_node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(options_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null mview options node", KR(ret));
  } else if (OB_UNLIKELY((T_MV_OPTION_LIST != options_node->type_ && T_MV_REFRESH_INFO != options_node->type_)
                         || (T_MV_REFRESH_INFO == options_node->type_ && 0 != options_node->value_)
                         || 0 > options_node->num_child_
                         || (0 < options_node->num_child_ && NULL == options_node->children_)
                         || (T_MV_OPTION_LIST == options_node->type_ && 0 == options_node->num_child_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mview options node", KR(ret), K(options_node->type_),
             K(options_node->value_), K(options_node->num_child_), K(options_node->children_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < options_node->num_child_; ++i) {
    const ParseNode *option_node = options_node->children_[i];
    if (OB_ISNULL(option_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null mview option", KR(ret), K(i));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
      const ParseNode *seen_option = options_node->children_[j];
      if (OB_UNLIKELY(seen_option->type_ == option_node->type_
                      && (T_MV_OPTIONS != option_node->type_
                          || seen_option->int32_values_[0] == option_node->int32_values_[0]))) {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("duplicate or conflicting materialized view option", KR(ret), K(option_node->type_),
                 K(option_node->int32_values_[0]));
      }
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_create_mview_options(const ParseNode *options_node,
                                                       const ObSelectStmt *stmt,
                                                       ObCreateViewResolver &resolver,
                                                       obrpc::ObMVRefreshInfo &refresh_info,
                                                       int64_t &mv_flags)
{
  int ret = OB_SUCCESS;
  int64_t option_count = 0;
  mv_flags = 0;
  if (OB_ISNULL(resolver.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret));
  } else if (OB_FAIL(init_create_mview_options(resolver.session_info_->get_effective_tenant_id(),
                                               refresh_info))) {
    LOG_WARN("failed to init create mview options", KR(ret));
  } else if (NULL == options_node) {
  } else if (OB_UNLIKELY(T_MV_OPTION_LIST != options_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mview options node type", KR(ret), K(options_node->type_));
  } else if (OB_FAIL(check_duplicate_mv_options(options_node))) {
    LOG_WARN("failed to check mview options", KR(ret));
  } else {
    option_count = options_node->num_child_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < option_count; ++i) {
    const ParseNode *option_node = options_node->children_[i];
    if (T_MV_BUILD_OPT == option_node->type_) {
      if (OB_FAIL(resolve_mv_build_mode(option_node, refresh_info.build_mode_))) {
        LOG_WARN("failed to resolve mview build mode", KR(ret));
      }
    } else if (T_MV_REFRESH_INFO == option_node->type_) {
      if (OB_FAIL(resolve_mv_refresh_info(option_node, resolver.params_, refresh_info))) {
        LOG_WARN("failed to resolve mview refresh info", KR(ret));
      } else if (OB_UNLIKELY(refresh_info.sched_suspended_)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("create materialized view with suspend is not supported", KR(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view with suspend is");
#ifdef OB_BUILD_MV_CLOSE_MODULES
      } else if (OB_NOT_NULL(stmt) && !refresh_info.start_time_.is_null()
                 && OB_FAIL(ObMVDepUtils::check_dep_mview_refresh_interval_order(
                        *stmt, resolver.session_info_->get_effective_tenant_id(),
                        refresh_info.refresh_interval_))) {
        LOG_WARN("failed to check mview refresh interval order", KR(ret),
                 K(refresh_info.refresh_interval_));
#endif
      }
    } else if (T_MV_OPTIONS == option_node->type_) {
      if (OB_UNLIKELY(1 > option_node->int32_values_[0] || 2 < option_node->int32_values_[0]
                      || 0 > option_node->int32_values_[1] || 1 < option_node->int32_values_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid materialized view option", KR(ret),
                 K(option_node->int32_values_[0]), K(option_node->int32_values_[1]));
      } else {
        mv_flags |= 1 == option_node->int32_values_[1] ? option_node->int32_values_[0] : 0;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected create mview option type", KR(ret), K(option_node->type_));
    }
  }
  return ret;
}

int ObMViewResolverHelper::init_create_mview_options(const uint64_t tenant_id,
                                                     obrpc::ObMVRefreshInfo &refresh_info)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  refresh_info.build_mode_ = ObMViewBuildMode::IMMEDIATE;
  refresh_info.refresh_method_ = ObMVRefreshMethod::FORCE;
  refresh_info.refresh_mode_ = ObMVRefreshMode::DEMAND;
  refresh_info.refresh_dop_ = 0;
  refresh_info.nested_refresh_mode_ = ObMVNestedRefreshMode::INDIVIDUAL;
  refresh_info.start_time_.reset();
  refresh_info.next_time_expr_.reset();
  refresh_info.refresh_interval_ = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!((data_version >= MOCK_DATA_VERSION_4_4_2_2
                            && data_version < DATA_VERSION_4_5_0_0)
                           || data_version >= DATA_VERSION_4_6_1_0))) {
    refresh_info.compat_version_ = 0;
  } else {
    refresh_info.compat_version_ = GET_MIN_CLUSTER_VERSION();
  }
  return ret;
}

int ObMViewResolverHelper::resolve_mv_options(const ObSelectStmt *stmt,
                                              ParseNode *options_node,
                                              obrpc::ObMVRefreshInfo &refresh_info,
                                              ObTableSchema &table_schema,
                                              ObTableSchema &container_table_schema,
                                              ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos,
                                              ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  refresh_info.reset();
  int64_t mv_flags = 0;
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  bool cnt_proctime_table = false;
  const int64_t on_query_computation_flag = 1;
  const int64_t query_rewrite_flag = 1 << 1;
  if (OB_ISNULL(stmt) || OB_ISNULL(resolver.session_info_) || OB_ISNULL(resolver.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(stmt), K(resolver.session_info_), K(resolver.allocator_));
  } else if (OB_FAIL(ObExecEnv::gen_exec_env(*resolver.session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("failed to generate exec env", KR(ret));
  } else if (OB_FAIL(ob_write_string(*resolver.allocator_, ObString(pos, buf),
                                    refresh_info.exec_env_))) {
    LOG_WARN("failed to write exec env", KR(ret));
  } else if (OB_FAIL(resolve_create_mview_options(options_node, stmt, resolver, refresh_info, mv_flags))) {
    LOG_WARN("failed to resolve create mview options", KR(ret));
  } else if (0 != (mv_flags & on_query_computation_flag)
             && OB_FALSE_IT(table_schema.set_mv_on_query_computation(ObMVOnQueryComputationFlag::IS_MV_ON_QUERY_COMPUTATION))) {
  } else if ((table_schema.mv_on_query_computation() || ObMVRefreshMethod::FAST == refresh_info.refresh_method_)
             && OB_FAIL(resolve_mv_fast_refresh(stmt, refresh_info, table_schema, container_table_schema,
                                         required_columns_infos, resolver))) {
    LOG_WARN("failed to resolve mview fast refresh", KR(ret));
  } else if (OB_FAIL(stmt->is_contains_mv_proctime_table(cnt_proctime_table))) {
    LOG_WARN("failed to check contains mv proctime table", KR(ret));
  } else {
    if (mv_flags & query_rewrite_flag) {
      table_schema.set_mv_enable_query_rewrite(ObMVEnableQueryRewriteFlag::IS_MV_ENABLE_QUERY_REWRITE);
    }
    if (cnt_proctime_table) {
      table_schema.set_mv_cnt_proctime_table(MV_CNT_PROCTIME_TABLE);
      container_table_schema.set_mv_cnt_proctime_table(MV_CNT_PROCTIME_TABLE);
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_mv_fast_refresh(
    const ObSelectStmt *stmt,
    obrpc::ObMVRefreshInfo &refresh_info,
    ObTableSchema &table_schema,
    ObTableSchema &container_table_schema,
    ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos,
    ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  ObMVCheckerResult checker_result;
  ObDMLStmt *copied_stmt = NULL;
  ObDMLStmt *check_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(resolver.allocator_) || OB_ISNULL(resolver.schema_checker_)
      || OB_ISNULL(resolver.params_.stmt_factory_) || OB_ISNULL(resolver.params_.expr_factory_)
      || OB_ISNULL(resolver.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(stmt), K(resolver.allocator_), K(resolver.schema_checker_),
             K(resolver.params_.stmt_factory_), K(resolver.params_.expr_factory_), K(resolver.session_info_));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(resolver.session_info_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get min data version", KR(ret));
  } else {
    const ObMVCheckerParams checker_params{*resolver.allocator_,
                                          *resolver.schema_checker_,
                                          *resolver.params_.stmt_factory_,
                                          *resolver.params_.expr_factory_,
                                          *resolver.session_info_,
                                          table_schema,
                                          container_table_schema,
                                          refresh_info.compat_version_,
                                          table_schema.mv_on_query_computation(),
                                          true /* for_create_mview */};
    if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*resolver.params_.stmt_factory_,
                                               *resolver.params_.expr_factory_,
                                               stmt, copied_stmt))) {
      LOG_WARN("failed to deep copy stmt", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*resolver.params_.stmt_factory_,
                                                      *resolver.params_.expr_factory_,
                                                      stmt,
                                                      check_stmt))) {
      LOG_WARN("failed to deep copy stmt for check", K(ret));
    } else if (OB_FAIL(ObMVChecker::prepare_mv_stmt_and_check_refresh_type(checker_params,
                                                                        check_stmt,
                                                                        checker_result))) {
      LOG_WARN("fail to check mv type", KR(ret));
    } else if (OB_UNLIKELY(checker_result.get_dependent_columns().count() > 0
                           && ((data_version < MOCK_DATA_VERSION_4_3_5_5)
                               || (data_version >= DATA_VERSION_4_4_0_0 && data_version < MOCK_DATA_VERSION_4_4_2_0)
                               || (data_version >= DATA_VERSION_4_5_0_0 && data_version < DATA_VERSION_4_5_1_0)))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN(
        "data version below 4.3.5.5 or between 4.4.0.0 and 4.4.2.0 or between 4.5.0.0 and 4.5.1.0, not support fast "
        "refresh auto add dependent columns",
        K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED,
                     "data version below 4.3.5.5 or between 4.4.0.0 and 4.4.2.0 or between 4.5.0.0 and 4.5.1.0, not "
                     "support fast refresh auto add dependent columns");
    } else if (OB_UNLIKELY(!IS_VALID_FAST_REFRESH_TYPE(checker_result.get_refresh_type()))) {
      // When creating an MV, which can not be fast refreshed, with both fast refresh
      // and on query computation, we should return CAN_NOT_ON_QUERY_COMPUTE
      if (table_schema.mv_on_query_computation()) {
        ret = OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE;
        LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE,
                       table_schema.get_table_name(),
                       checker_result.get_note().error_.ptr());
      } else {
        ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
        LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH,
                       table_schema.get_table_name(),
                       checker_result.get_note().error_.ptr());
      }
      LOG_WARN("fast refresh is not supported for this mv", KR(ret), K(checker_result.get_refresh_type()));
    } else if (OB_FAIL(checker_result.get_mlog_required_columns_info().convert_to_required_columns_infos(
                   required_columns_infos))) {
      LOG_WARN("failed to convert to required columns infos", K(ret));
    } else if (OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV == checker_result.get_refresh_type()) {
      table_schema.set_mv_major_refresh(IS_MV_MAJOR_REFRESH);
      container_table_schema.set_mv_major_refresh(IS_MV_MAJOR_REFRESH);
      refresh_info.refresh_mode_ = ObMVRefreshMode::MAJOR_COMPACTION;
      LOG_INFO("[MAJ_REF_MV] match major refresh mv", K(table_schema.get_table_name()));
    }
  }
  if (OB_SUCC(ret)) {
    if ((checker_result.get_dependent_columns().count() > 0)
        && OB_FAIL(ObMVProvider::expand_mv_stmt_with_dependent_columns(static_cast<ObSelectStmt *>(copied_stmt),
                                                                    checker_result.get_dependent_columns(),
                                                                    *resolver.allocator_,
                                                                    resolver.session_info_,
                                                                    *resolver.params_.expr_factory_))) {
      LOG_WARN("failed to expand mv stmt with dependent columns", K(ret));
    } else if (table_schema.mv_on_query_computation()
               && OB_FAIL(check_on_query_computation_supported(static_cast<ObSelectStmt *>(copied_stmt)))) {
      LOG_WARN("fail to check on query computation mv column type", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    int dep_col_count = checker_result.get_dependent_columns().count();
    int expend_stmt_select_items_count = static_cast<ObSelectStmt*>(copied_stmt)->get_select_item_size();
    if (dep_col_count == 0) {
      // do nothing
    } else {
      // for exp: user defined columns (a, b, c) + hidden column (d),
      // expend_stmt_select_items_count = 4, dep_col_count = 1
      // select_item_index = 3, dep_col_idx = 0, hidden_column_index = 4
      int select_item_index = (expend_stmt_select_items_count - dep_col_count);
      int dep_col_idx = 0;
      int hidden_column_index = container_table_schema.get_max_used_column_id() + 1; // columns idx for container table
      if (select_item_index > 0) {
        for (; dep_col_idx < dep_col_count && OB_SUCC(ret);
             select_item_index++, dep_col_idx++, hidden_column_index++) {
          const SelectItem &select_item = static_cast<ObSelectStmt*>(copied_stmt)->get_select_item(select_item_index);
          char comment_buf[MAX_COLUMN_COMMENT_CHAR_LENGTH];
          ObString comment_string;
          if (OB_FAIL(generate_hidden_column_comment(checker_result.get_dependent_columns().at(dep_col_idx),
                                                     copied_stmt->is_set_stmt(),
                                                     resolver.schema_checker_->get_schema_guard(),
                                                     resolver.session_info_,
                                                     comment_buf,
                                                     comment_string))) {
            LOG_WARN("failed to generate hidden column comment", KR(ret));
          } else if (OB_FAIL(add_hidden_cols_for_mv(container_table_schema,
                                                  hidden_column_index,
                                                  select_item,
                                                  comment_string,
                                                  resolver))) {
            LOG_WARN("failed to add hidden cols for mv", KR(ret));
          }
        }
        ObString sql;
        ObSchemaGetterGuard *schema_guard = resolver.schema_checker_->get_schema_guard();
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObMVProvider::align_mv_stmt_output_column_names(
                       static_cast<ObSelectStmt *>(copied_stmt), container_table_schema))) {
          LOG_WARN("failed to align mv output column names", KR(ret));
        } else if (OB_FAIL(ObSQLUtils::reconstruct_sql(*resolver.allocator_,
                           copied_stmt, sql, schema_guard, resolver.session_info_->create_obj_print_params()))) {
          LOG_WARN("failed to reconstruct sql", KR(ret));
        } else if (OB_FAIL(table_schema.set_expand_view_definition_for_mv(sql))) {
          LOG_WARN("failed to set expend view definition for mv", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_mv_build_mode(const ParseNode *build_mode_node,
                                                  ObMViewBuildMode &build_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(build_mode_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null mv build mode node", KR(ret));
  } else if (OB_UNLIKELY(T_MV_BUILD_OPT != build_mode_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mv build mode node", KR(ret), K(build_mode_node->type_));
  } else if (OB_UNLIKELY(0 != build_mode_node->value_ && 1 != build_mode_node->value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mv build mode", KR(ret), K(build_mode_node->value_));
  } else {
    build_mode = 0 == build_mode_node->value_ ? ObMViewBuildMode::IMMEDIATE
                                              : ObMViewBuildMode::DEFERRED;
  }
  return ret;
}

int ObMViewResolverHelper::check_on_query_computation_supported(const ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObTransformerImpl::StmtFunc func;
  if (OB_FAIL(ObTransformerImpl::check_stmt_functions(stmt, func))) {
    LOG_WARN("failed to check stmt functions", K(ret));
  } else if (OB_UNLIKELY(func.contain_enum_set_values_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "on query computation mview use enum type");
    LOG_WARN("not support on query computation mview use enum type", KR(ret), K(func.contain_enum_set_values_));
  }
  return ret;
}


int ObMViewResolverHelper::resolve_mv_refresh_info(const ParseNode *refresh_info_node,
                                                  ObResolverParams &resolver_params,
                                                  obrpc::ObMVRefreshInfo &refresh_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(refresh_info_node) || OB_ISNULL(resolver_params.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(refresh_info_node), K(resolver_params.session_info_));
  } else if (OB_UNLIKELY(T_MV_REFRESH_INFO != refresh_info_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mview refresh info node type", KR(ret), K(refresh_info_node->type_));
  } else if (OB_UNLIKELY((1 == refresh_info_node->value_ || 2 == refresh_info_node->value_)
                         && 0 != refresh_info_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected children for mview refresh state", KR(ret), K(refresh_info_node->value_),
             K(refresh_info_node->num_child_));
  } else if (1 == refresh_info_node->value_) {
    refresh_info.refresh_mode_ = ObMVRefreshMode::NEVER;
  } else if (2 == refresh_info_node->value_) {
    if (OB_FAIL(resolve_mv_sched_suspended(resolver_params.session_info_->get_effective_tenant_id(),
                                           refresh_info.sched_suspended_))) {
      LOG_WARN("failed to resolve mview scheduler suspension", KR(ret));
    }
  } else if (OB_FAIL(check_duplicate_mv_options(refresh_info_node))) {
    LOG_WARN("failed to check mview refresh options", KR(ret));
  } else {
    const uint64_t tenant_id = resolver_params.session_info_->get_effective_tenant_id();
    const int64_t option_count = refresh_info_node->num_child_;
    const ParseNode *start_node = NULL;
    const ParseNode *next_node = NULL;
    const ParseNode *compat_version_node = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < option_count; ++i) {
      const ParseNode *option_node = refresh_info_node->children_[i];
      switch (option_node->type_) {
        case T_MV_REFRESH_METHOD:
          if (OB_FAIL(resolve_refresh_method(option_node->value_, refresh_info.refresh_method_))) {
            LOG_WARN("failed to resolve refresh method", KR(ret));
          }
          break;
        case T_PARALLEL:
          if (OB_FAIL(resolve_refresh_parallel_node(option_node, refresh_info.refresh_dop_))) {
            LOG_WARN("failed to resolve refresh parallel", KR(ret));
          }
          break;
        case T_MV_NESTED_REFRESH_CLAUSE:
          if (OB_FAIL(resolve_nested_refresh_node(option_node, tenant_id, refresh_info.nested_refresh_mode_))) {
            LOG_WARN("failed to resolve nested refresh mode", KR(ret));
          }
          break;
        case T_MV_REFRESH_ON_CLAUSE:
          if (OB_FAIL(resolve_refresh_on_node(option_node, refresh_info.refresh_mode_))) {
            LOG_WARN("failed to resolve refresh on mode", KR(ret));
          }
          break;
        case T_MV_REFRESH_START_EXPR:
          start_node = option_node;
          break;
        case T_MV_REFRESH_INTERVAL:
          next_node = option_node;
          break;
        case T_VERSION:
          compat_version_node = option_node;
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected mview refresh option type", KR(ret), K(option_node->type_));
          break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(fill_mv_refresh_interval(start_node, next_node, tenant_id,
                                                resolver_params, refresh_info))) {
      LOG_WARN("failed to fill mview refresh interval", KR(ret));
    } else if (NULL != compat_version_node
               && OB_FAIL(resolve_compat_version(compat_version_node, tenant_id, refresh_info.compat_version_))) {
      LOG_WARN("failed to resolve compat version", KR(ret));
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_mv_sched_suspended(const uint64_t tenant_id,
                                                      bool &sched_suspended)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(DATA_VERSION_5_0_2_0 > data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version does not support suspending mview refresh",
             KR(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "tenant's data version is below 5.0.2.0, suspend mview refresh is");
  } else {
    sched_suspended = true;
  }
  return ret;
}

int ObMViewResolverHelper::fill_mv_refresh_interval(const ParseNode *start_node,
                                                    const ParseNode *next_node,
                                                    const uint64_t tenant_id,
                                                    ObResolverParams &resolver_params,
                                                    obrpc::ObMVRefreshInfo &refresh_info)
{
  int ret = OB_SUCCESS;
  int64_t start_time = OB_INVALID_TIMESTAMP;
  ObString next_time_expr;
  int64_t period_sec = 0;
  if (OB_NOT_NULL(next_node)
      && (OB_UNLIKELY(NULL != next_node->children_[0]) || OB_ISNULL(next_node->children_[1]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NEXT option", KR(ret), K(next_node->children_[0]), K(next_node->children_[1]));
  } else if (NULL == start_node && NULL == next_node) {
    refresh_info.start_time_.reset();
    refresh_info.next_time_expr_.reset();
    refresh_info.refresh_interval_ = 0;
  } else if (OB_FAIL(resolve_refresh_interval_node(start_node,
                                                    NULL == next_node ? NULL : next_node->children_[1],
                                                    resolver_params,
                                                    start_time,
                                                    next_time_expr,
                                                    period_sec))) {
    LOG_WARN("failed to resolve interval node", KR(ret));
  } else if (OB_INVALID_TIMESTAMP == start_time && next_time_expr.empty()) {
    refresh_info.start_time_.reset();
    refresh_info.next_time_expr_.reset();
    refresh_info.refresh_interval_ = 0;
  } else if (OB_FAIL(check_mv_refresh_interval(tenant_id, refresh_info, period_sec))) {
    LOG_WARN("failed to check mview refresh interval", KR(ret));
  } else if (OB_FALSE_IT(refresh_info.start_time_.set_timestamp(start_time))) {
  } else if (OB_FALSE_IT(refresh_info.next_time_expr_ = next_time_expr)) {
  } else {
    refresh_info.refresh_interval_ = period_sec * 1000000L;
  }
  return ret;
}

int ObMViewResolverHelper::check_mv_refresh_interval(const uint64_t tenant_id,
                                                     const obrpc::ObMVRefreshInfo &refresh_info,
                                                     const int64_t period_sec)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObMVRefreshMode::COMMIT == refresh_info.refresh_mode_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ON COMMIT attribute followed by start with/next clause is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ON COMMIT attribute followed by start with/next clause is");
  } else if (OB_UNLIKELY(ObMVRefreshMode::STATEMENT == refresh_info.refresh_mode_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ON STATEMENT attribute followed by start with/next clause is not supported", KR(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ON STATEMENT attribute followed by start with/next clause is");
  } else if (ObMVRefreshMethod::COMPLETE == refresh_info.refresh_method_) {
    int64_t mview_complete_refresh_min_interval = 600;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      mview_complete_refresh_min_interval = tenant_config->_mv_complete_refresh_min_interval / 1000000L;
    }
    if (OB_UNLIKELY(period_sec < mview_complete_refresh_min_interval)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("the interval of complete refresh is less than the minimum interval", KR(ret),
               K(period_sec), K(mview_complete_refresh_min_interval));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "the interval of complete refresh is less than the minimum interval");
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_refresh_method(const int64_t refresh_method_value,
                                                  ObMVRefreshMethod &refresh_method)
{
  int ret = OB_SUCCESS;
  switch (refresh_method_value) {
    case 0:
      refresh_method = ObMVRefreshMethod::FAST;
      break;
    case 1:
      refresh_method = ObMVRefreshMethod::COMPLETE;
      break;
    case 2:
      refresh_method = ObMVRefreshMethod::FORCE;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid refresh method value", KR(ret), K(refresh_method_value));
      break;
  }
  return ret;
}

int ObMViewResolverHelper::resolve_refresh_parallel_node(const ParseNode *refresh_parallel_node,
                                                         int64_t &refresh_dop)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(refresh_parallel_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null refresh parallel node", K(ret));
  } else if (OB_UNLIKELY(T_PARALLEL != refresh_parallel_node->type_)
             || OB_UNLIKELY(1 != refresh_parallel_node->num_child_)
             || OB_ISNULL(refresh_parallel_node->children_)
             || OB_ISNULL(refresh_parallel_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected refresh parallel node", K(ret), K(refresh_parallel_node->type_), K(refresh_parallel_node->children_));
  } else if (OB_UNLIKELY(refresh_parallel_node->children_[0]->value_ < 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid refresh dop", KR(ret), K(refresh_parallel_node->children_[0]->value_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "value of REFRESH PARALLEL less than 0 is");
  } else {
    refresh_dop = refresh_parallel_node->children_[0]->value_;
  }
  return ret;
}

int ObMViewResolverHelper::resolve_nested_refresh_node(const ParseNode *nested_refresh_node,
                                                       const uint64_t tenant_id,
                                                       ObMVNestedRefreshMode &nested_refresh_mode)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_ISNULL(nested_refresh_node)
      || OB_UNLIKELY(T_MV_NESTED_REFRESH_CLAUSE != nested_refresh_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null nested refresh node", K(ret), K(nested_refresh_node));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(data_version < MOCK_DATA_VERSION_4_3_5_3
                         || (data_version >= DATA_VERSION_4_4_0_0
                             && data_version < MOCK_DATA_VERSION_4_4_2_0)
                         || (data_version >= DATA_VERSION_4_5_0_0
                             && data_version < DATA_VERSION_4_5_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("data version does not support nested refresh type", KR(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   "data version below 4.3.5.3 or between 4.4.0.0 and 4.4.2.0 or between 4.5.0.0 and "
                   "4.5.1.0, set nested refresh type is");
  } else {
    switch (nested_refresh_node->value_) {
      case 0:
        nested_refresh_mode = ObMVNestedRefreshMode::INDIVIDUAL;
        break;
      case 1:
        nested_refresh_mode = ObMVNestedRefreshMode::INCONSISTENT;
        break;
      case 2:
        nested_refresh_mode = ObMVNestedRefreshMode::CONSISTENT;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid nested refresh mode", KR(ret), K(nested_refresh_node->value_));
        break;
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_refresh_on_node(const ParseNode *refresh_on_node,
                                                   ObMVRefreshMode &refresh_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(refresh_on_node)
      || OB_UNLIKELY(T_MV_REFRESH_ON_CLAUSE != refresh_on_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null refresh on node", K(ret));
  } else {
    switch (refresh_on_node->value_) {
      case 0:
        refresh_mode = ObMVRefreshMode::DEMAND;
        break;
      case 1:
        refresh_mode = ObMVRefreshMode::COMMIT;
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview refresh on commit is");
        break;
      case 2:
        refresh_mode = ObMVRefreshMode::STATEMENT;
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview refresh on statement is");
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid refresh mode", K(ret), K(refresh_on_node->value_));
        break;
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_refresh_interval_node(const ParseNode *start_date,
                                                          const ParseNode *next_date,
                                                          ObResolverParams &resolver_params,
                                                          int64_t &start_time,
                                                          ObString &next_time_expr,
                                                          int64_t &period_sec)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = resolver_params.session_info_;
  common::ObIAllocator *allocator = resolver_params.allocator_;
  start_time = OB_INVALID_TIMESTAMP;
  next_time_expr.reset();
  period_sec = 0;
  if (OB_ISNULL(session_info) || OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KR(ret), K(session_info), K(allocator));
  } else {
    int64_t current_time = ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
    if (NULL != start_date) {
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
    if (OB_SUCC(ret) && NULL != next_date) {
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
        if (OB_FAIL(ob_write_string(*allocator, next_date_str, next_time_expr))) {
          LOG_WARN("fail to write string", KR(ret));
        } else {
          int64_t next_ts = 0;
          const uint64_t tenant_id = session_info->get_effective_tenant_id();
          if (OB_FAIL(ObMViewSchedJobUtils::calc_date_expr_from_str(
                  *session_info, *allocator, tenant_id, next_time_expr, next_ts))) {
            LOG_WARN("failed to calc date expr from str", KR(ret), K(next_time_expr));
          } else {
            period_sec = (next_ts - current_time) / 1000000L;
          }
        }
      }
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_compat_version(const ParseNode *compat_version_node,
                                                  const uint64_t tenant_id,
                                                  uint64_t &compat_version)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  const ParseNode *version_node = NULL;
  if (OB_ISNULL(compat_version_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null compat version node", KR(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!((data_version >= MOCK_DATA_VERSION_4_4_2_2
                            && data_version < DATA_VERSION_4_5_0_0)
                           || data_version >= DATA_VERSION_4_6_1_0))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("tenant data version does not support specifying mview compat version",
             KR(ret), K(tenant_id), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "specifying mview compat version at this data version is");
  } else if (T_VERSION == compat_version_node->type_
             && (OB_ISNULL(compat_version_node->children_) || OB_ISNULL(compat_version_node->children_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null compat version", KR(ret));
  } else {
    version_node = T_VERSION == compat_version_node->type_
                   ? compat_version_node->children_[0]
                   : compat_version_node;
    ObString version_str(version_node->str_len_, version_node->str_value_);
    uint64_t version_val = 0;
    if (OB_FAIL(ObClusterVersion::get_version(version_str, version_val))) {
      LOG_WARN("failed to parse compat version", KR(ret), K(version_str));
    } else if (OB_FAIL(ObCompatControl::check_compat_version(tenant_id, version_val))) {
      LOG_WARN("failed to check compat version", KR(ret), K(tenant_id), K(version_val));
    } else {
      compat_version = version_val;
    }
    if (OB_INVALID_ARGUMENT == ret) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "compat version");
    }
  }
  return ret;
}

int ObMViewResolverHelper::load_mview_dep_session_vars(ObSQLSessionInfo &session_info,
                                                       ObSelectStmt *stmt,
                                                       ObLocalSessionVar &dep_vars)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(session_info.get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get min data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_3_0) {
    //  when use data version before DATA_VERSION_4_3_3_0, do not extract local var
  } else if (OB_FAIL(dep_vars.reserve_max_local_vars_capacity())) {
    LOG_WARN("fail to reserve max local vars capacity", K(ret));
  } else if (OB_FAIL(get_dep_session_vars_from_stmt(session_info, stmt, dep_vars))) {
    LOG_WARN("fail to get dep session vars from stmt", K(ret));
  } else {
    LOG_TRACE("finish load mview dep session vars", K(session_info.get_sql_mode()), K(dep_vars));
  }
  return ret;
}

int ObMViewResolverHelper::get_dep_session_vars_from_stmt(ObSQLSessionInfo &session_info,
                                                         ObSelectStmt *stmt,
                                                         ObLocalSessionVar &dep_vars)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt *, 4> childs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(childs))) {
    LOG_WARN("get sel exprs failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < childs.count(); i++) {
      if (OB_FAIL(SMART_CALL(get_dep_session_vars_from_stmt(session_info, childs.at(i), dep_vars)))) {
        LOG_WARN("fail to get dep session vars from stmt", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 32> exprs;
    if (OB_FAIL(stmt->get_relation_exprs(exprs))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
        if (OB_ISNULL(exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(exprs.at(i)->get_expr_dep_session_vars_recursively(&session_info, dep_vars))) {
          LOG_WARN("fail to get expr dep session vars recursively", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMViewResolverHelper::resolve_materialized_view(const ParseNode &parse_tree,
                                                     ObCreateTableStmt *stmt,
                                                     ParseNode *mv_primary_key_node,
                                                     ObTableSchema &table_schema,
                                                     ObSelectStmt *select_stmt,
                                                     ObCreateTableArg &create_arg,
                                                     ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(select_stmt));
  } else {
    ObMVAdditionalInfo *mv_ainfo = nullptr;
    ObSEArray<ObConstraint, 4> &csts = stmt->get_create_table_arg().constraint_list_;
    uint64_t tenant_data_version = 0;
    ObTableOrganizationMode organization_mode = TOM_INDEX_ORGANIZED;
    if (OB_FAIL(GET_MIN_DATA_VERSION(resolver.session_info_->get_effective_tenant_id(),
                                     tenant_data_version))) {
      LOG_WARN("get tenant data version failed", KR(ret));
    } else if (OB_FAIL(ObResolverUtils::check_schema_valid_for_mview(table_schema))) {
      LOG_WARN("failed to check schema valid for mview", KR(ret), K(table_schema));
    } else if (OB_FAIL(resolver.resolve_table_options(
                   parse_tree.children_[ObCreateViewResolver::TABLE_OPTION_NODE], false))) {
      LOG_WARN("fail to resolve table options", KR(ret));
    } else if (OB_FAIL(resolve_mview_table_organization(
                   parse_tree.children_[ObCreateViewResolver::TABLE_OPTION_NODE],
                   mv_primary_key_node,
                   organization_mode,
                   resolver))) {
      LOG_WARN("fail to resolve materialized view table organization", KR(ret));
    } else if (OB_ISNULL(mv_ainfo = create_arg.mv_ainfo_.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObMVAdditionalInfo from array error", K(ret));
    } else if (OB_FAIL(mv_ainfo->container_table_schema_.assign(table_schema))) {
      LOG_WARN("fail to assign table schema", KR(ret));
    } else if (OB_FAIL(resolve_materialized_view_container_table(
                   parse_tree.children_[ObCreateViewResolver::PARTITION_NODE],
                   mv_primary_key_node,
                   organization_mode,
                   tenant_data_version,
                   mv_ainfo->container_table_schema_, csts, resolver))) {
      LOG_WARN("fail do resolve for materialized view", K(ret));
    } else if (OB_FAIL(load_mview_dep_session_vars(*resolver.session_info_, select_stmt,
                                                   table_schema.get_local_session_var()))) {
      LOG_WARN("fail to load mview dep session variables", K(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[ObCreateViewResolver::COLUMN_GROUP_NODE])
               && OB_FALSE_IT(mv_ainfo->container_table_schema_.set_column_store(true))) {
      // mark is_column_store_supported_ in advance for the major refresh check in ObMVChecker
      // set_column_store will also be done in resolve_column_group_helper
    } else if (OB_FAIL(resolve_mv_options(select_stmt,
                                          parse_tree.children_[ObCreateViewResolver::MVIEW_NODE],
                                          mv_ainfo->mv_refresh_info_,
                                          table_schema,
                                          mv_ainfo->container_table_schema_,
                                          mv_ainfo->required_columns_infos_,
                                          resolver))) {
      LOG_WARN("fail to resolve mv options", K(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[ObCreateViewResolver::COLUMN_GROUP_NODE]) &&
               OB_FAIL(resolver.resolve_column_group_helper(
                   parse_tree.children_[ObCreateViewResolver::COLUMN_GROUP_NODE],
                   mv_ainfo->container_table_schema_))) {
      LOG_WARN("fail to resolve column group", KR(ret));
    } else if (OB_UNLIKELY((tenant_data_version < MOCK_DATA_VERSION_4_3_5_4
                            || (tenant_data_version >= DATA_VERSION_4_4_0_0 && tenant_data_version < MOCK_DATA_VERSION_4_4_2_0)
                            || (tenant_data_version >= DATA_VERSION_4_5_0_0 && tenant_data_version < DATA_VERSION_4_5_1_0))
                           && table_schema.is_mv_cnt_proctime_table())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version is less than 4.3.5.4 or between 4.4.0.0 and 4.4.2.0 or between 4.5.0.0 and 4.5.1.0, create materialized view with table AS OF PROCTIME() is");
    }
  }
  return ret;
}

int ObMViewResolverHelper::generate_hidden_column_comment(const std::pair<ObRawExpr *, int64_t> &dep_col,
                                                          const bool is_set_stmt,
                                                          ObSchemaGetterGuard *schema_guard,
                                                          ObSQLSessionInfo *session_info,
                                                          char *buf,
                                                          ObString &comment_string)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObRawExprPrinter expr_printer;
  if (OB_ISNULL(buf) || OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(buf), K(schema_guard), K(session_info));
  } else {
    static constexpr char comment_prefix[] = "refresh dependent: ";
    static constexpr int64_t prefix_len = sizeof(comment_prefix) - 1;
    MEMCPY(buf, comment_prefix, prefix_len);
    pos = prefix_len;
  }
  if (OB_FAIL(ret)) {
  } else if (is_set_stmt && OB_INVALID_INDEX == dep_col.second) {
    // it is a union all marker column
    static constexpr char marker_comment[] = "union all branch marker";
    static constexpr int64_t marker_len = sizeof(marker_comment) - 1;
    if (OB_LIKELY(pos + marker_len <= MAX_COLUMN_COMMENT_CHAR_LENGTH)) {
      MEMCPY(buf + pos, marker_comment, marker_len);
      pos += marker_len;
    }
  } else if (OB_ISNULL(dep_col.first)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FALSE_IT(expr_printer.init(buf,
                                           MAX_COLUMN_COMMENT_CHAR_LENGTH,
                                           &pos,
                                           schema_guard,
                                           TZ_INFO(session_info)))) {
  } else if (OB_FAIL(expr_printer.do_print(const_cast<ObRawExpr *>(dep_col.first), T_FIELD_LIST_SCOPE))) {
    if (OB_LIKELY(OB_SIZE_OVERFLOW == ret && pos > 0)) {
      // comment may be truncated, just ignore it
      LOG_WARN("select item expr is too long, comment may be truncated", K(ret), K(pos));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to print select item expr", K(ret));
    }
  } else if (is_set_stmt
             && OB_FAIL(common::databuff_printf(buf,
                                                MAX_COLUMN_COMMENT_CHAR_LENGTH,
                                                pos,
                                                " in the %ld-th set child query",
                                                dep_col.second + 1))) {
    if (OB_LIKELY(OB_SIZE_OVERFLOW == ret && pos > 0)) {
      // comment may be truncated, just ignore it
      LOG_WARN("set stmt branch index is too long, comment may be truncated", K(ret), K(pos));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to append set stmt branch index", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    comment_string.assign(buf, pos);
  }
  return ret;
}

int ObMViewResolverHelper::add_hidden_cols_for_mv(ObTableSchema &table_schema,
                                                  const uint64_t column_id,
                                                  const SelectItem &select_item,
                                                  const ObString &comment_string,
                                                  ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *expr = select_item.expr_;
  if (OB_ISNULL(expr) || OB_ISNULL(resolver.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(expr), KP(resolver.schema_checker_));
  } else {
    ObColumnSchemaV2 hidden_column;
    hidden_column.reset();
    hidden_column.set_column_id(column_id);
    hidden_column.set_is_hidden(true);
    // Set column name
    if (OB_FAIL(hidden_column.set_column_name(expr->get_alias_column_name()))) {
      LOG_WARN("failed to set column name", KR(ret));
    } else if (OB_FAIL(resolver.fill_column_meta_infos(*expr,
                                                       table_schema.get_charset_type(),
                                                       table_schema.get_table_id(),
                                                       *resolver.session_info_,
                                                       hidden_column,
                                                       true))) {
      LOG_WARN("failed to fill column meta infos for mv", K(ret), K(hidden_column));
    } else if (OB_FAIL(hidden_column.set_comment(comment_string))) {
      LOG_WARN("failed to set hidden column comment", K(ret), K(comment_string));
    } else if (OB_FAIL(table_schema.add_column(hidden_column))) {
      LOG_WARN("add column to table_schema failed", K(ret), K(hidden_column));
    } else {
      // for debug
      LOG_TRACE("add hidden column for materialized view", KPC(expr), K(hidden_column), K(table_schema));
    }
  }
  return ret;
}

}
}
