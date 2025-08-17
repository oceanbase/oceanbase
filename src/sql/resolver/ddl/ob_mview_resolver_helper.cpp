#define USING_LOG_PREFIX  SQL_RESV

#include "sql/resolver/ddl/ob_mview_resolver_helper.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/ddl/ob_create_table_resolver_base.h"
#include "sql/resolver/mv/ob_mv_checker.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "share/ob_license_utils.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "storage/mview/ob_mview_refresh.h"
#include "sql/resolver/mv/ob_mv_provider.h"

namespace oceanbase
{
namespace sql
{


int ObMViewResolverHelper::resolve_materialized_view_container_table(ParseNode *partition_node,
                                                                     ParseNode *mv_primary_key_node,
                                                                     ObTableSchema &container_table_schema,
                                                                     ObSEArray<ObConstraint,4>& csts,
                                                                     ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  container_table_schema.set_table_type(ObTableType::USER_TABLE);
  container_table_schema.get_view_schema().reset();
  container_table_schema.set_max_dependency_version(OB_INVALID_VERSION);

  if (OB_FAIL(resolver.resolve_partition_option(partition_node, container_table_schema, true))) {
    LOG_WARN("fail to resolve_partition_option", KR(ret));
  } else if (OB_FAIL(resolver.set_table_option_to_schema(container_table_schema))) {
    SQL_RESV_LOG(WARN, "set table option to schema failed", KR(ret));
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
  } else if (OB_FAIL(container_table_schema.check_primary_key_cover_partition_column())) {
    SQL_RESV_LOG(WARN, "fail to check primary key cover partition column", KR(ret));
  } else {
    container_table_schema.set_collation_type(resolver.collation_type_);
    container_table_schema.set_charset_type(resolver.charset_type_);
    container_table_schema.set_mv_container_table(IS_MV_CONTAINER_TABLE);
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

int ObMViewResolverHelper::resolve_mv_options(const ObSelectStmt *stmt,
                                              ParseNode *options_node,
                                              obrpc::ObMVRefreshInfo &refresh_info,
                                              ObTableSchema &table_schema,
                                              ObTableSchema &container_table_schema,
                                              ObIArray<obrpc::ObMVRequiredColumnsInfo> &required_columns_infos,
                                              ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  refresh_info.refresh_method_ = ObMVRefreshMethod::FORCE; //default method is force
  refresh_info.refresh_mode_ = ObMVRefreshMode::DEMAND; //default mode is demand
  if (NULL == options_node) {
    /* do nothing */
  } else if (OB_UNLIKELY(T_MV_OPTIONS != options_node->type_ || 1 != options_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(options_node));
  } else if (OB_FAIL(resolve_mv_refresh_info(options_node->children_[0], refresh_info, resolver))) {
    LOG_WARN("fail to resolve mv refresh info", KR(ret));
  } else {
    const int64_t on_query_computation_flag = 1;
    const int64_t query_rewrite_flag = 1 << 1;
    if (options_node->value_ & on_query_computation_flag) {
      table_schema.set_mv_on_query_computation(ObMVOnQueryComputationFlag::IS_MV_ON_QUERY_COMPUTATION);
    }
    if (options_node->value_ & query_rewrite_flag) {
      table_schema.set_mv_enable_query_rewrite(ObMVEnableQueryRewriteFlag::IS_MV_ENABLE_QUERY_REWRITE);
    }
  }

  if (OB_SUCC(ret)) {
    if ((table_schema.mv_on_query_computation() ||
                ObMVRefreshMethod::FAST == refresh_info.refresh_method_)) {
      ObMVRefreshableType refresh_type = OB_MV_REFRESH_INVALID;
      FastRefreshableNotes fast_refreshable_note;
      if (OB_FAIL(ObMVChecker::check_mv_fast_refresh_type(
              stmt, resolver.allocator_, resolver.schema_checker_, resolver.params_.stmt_factory_,
              resolver.params_.expr_factory_, resolver.session_info_, container_table_schema,
              table_schema.mv_on_query_computation(), refresh_type, fast_refreshable_note, required_columns_infos))) {
        LOG_WARN("fail to check mv type", KR(ret));
      } else if (OB_UNLIKELY(!IS_VALID_FAST_REFRESH_TYPE(refresh_type))) {
        // When creating an MV, which can not be fast refreshed, with both fast refresh
        // and on query computation, we should return CAN_NOT_ON_QUERY_COMPUTE
        if (table_schema.mv_on_query_computation()) {
          ret = OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE;
          LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE, table_schema.get_table_name(),
                          fast_refreshable_note.error_.ptr());
        } else {
          ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
          LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH, table_schema.get_table_name(),
                          fast_refreshable_note.error_.ptr());
        }
        LOG_WARN("fast refresh is not supported for this mv", KR(ret), K(refresh_type));
      } else if (OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV == refresh_type) {
        table_schema.set_mv_major_refresh(IS_MV_MAJOR_REFRESH);
        container_table_schema.set_mv_major_refresh(IS_MV_MAJOR_REFRESH);
        refresh_info.refresh_mode_ = ObMVRefreshMode::MAJOR_COMPACTION;
        LOG_INFO("[MAJ_REF_MV] match major refresh mv", K(table_schema.get_table_name()));
      }
      if (OB_SUCC(ret) && table_schema.mv_on_query_computation() &&
          OB_FAIL(check_on_query_computation_supported(stmt))) {
        LOG_WARN("fail to check on query computation mv column type", KR(ret));
      }
    }
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

int ObMViewResolverHelper::resolve_mv_refresh_info(ParseNode *refresh_info_node,
                                                   obrpc::ObMVRefreshInfo &refresh_info,
                                                   ObCreateViewResolver &resolver)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (resolver.allocator_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", KR(ret));
  }
  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  OZ (ObExecEnv::gen_exec_env(*resolver.session_info_, buf, OB_MAX_PROC_ENV_LENGTH, pos));
  OX (refresh_info.exec_env_.assign(buf, pos));
  OZ (ob_write_string(*resolver.allocator_, refresh_info.exec_env_, refresh_info.exec_env_));
  OZ (OB_FAIL(GET_MIN_DATA_VERSION(resolver.session_info_->get_effective_tenant_id(), data_version)));
  if (OB_SUCC(ret) && refresh_info_node != nullptr) {
    if (refresh_info_node->int32_values_[0] == 1) { //never refresh
      refresh_info.refresh_method_ = ObMVRefreshMethod::NEVER;
      refresh_info.refresh_mode_ = ObMVRefreshMode::NEVER;
    } else if ((3 == refresh_info_node->num_child_)
               && OB_NOT_NULL(refresh_info_node->children_)) {
      int32_t refresh_method = refresh_info_node->int32_values_[1];
      ParseNode *refresh_on_clause = refresh_info_node->children_[1];
      ParseNode *refresh_interval_node = refresh_info_node->children_[2];
      ParseNode *nested_refresh_node = refresh_info_node->children_[0];

      switch (refresh_method) {
        case 0:
          refresh_info.refresh_method_ = ObMVRefreshMethod::FAST;
          break;
        case 1:
          refresh_info.refresh_method_ = ObMVRefreshMethod::COMPLETE;
          break;
        case 2:
          refresh_info.refresh_method_ = ObMVRefreshMethod::FORCE;
          break;
      }

      if (refresh_on_clause != nullptr) {
        ParseNode *refresh_mode_node = refresh_on_clause->children_[0];
        if (refresh_mode_node != nullptr) {
          switch (refresh_mode_node->value_) {
            case 0:
              refresh_info.refresh_mode_ = ObMVRefreshMode::DEMAND;
              break;
            case 1:
              refresh_info.refresh_mode_ = ObMVRefreshMode::COMMIT;
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview refresh on commit");
              break;
            case 2:
              refresh_info.refresh_mode_ = ObMVRefreshMode::STATEMENT;
              ret = OB_NOT_SUPPORTED;
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview refresh on statement");
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid refresh mode", K(refresh_mode_node->value_));
              break;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(nested_refresh_node)) {
        ParseNode *nested_refresh_mode_node = nested_refresh_node->children_[0];
        if (data_version < DATA_VERSION_4_3_5_3 ) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("data version below 4.3.5.3, not support nested refresh type", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "data version below 4.3.5.3, set nested refresh type");
        } else if (OB_ISNULL(nested_refresh_mode_node) ||
                   OB_UNLIKELY(T_MV_NESTED_REFRESH_CLAUSE != nested_refresh_node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid nested refresh node", K(ret), K(nested_refresh_node->type_),
                   KP(nested_refresh_mode_node));
        } else if (data_version >= DATA_VERSION_4_3_5_3) {
          switch (nested_refresh_mode_node->value_) {
            case 0:
              refresh_info.nested_refresh_mode_ = ObMVNestedRefreshMode::INDIVIDUAL;
              break;
            case 1:
              refresh_info.nested_refresh_mode_ = ObMVNestedRefreshMode::INCONSISTENT;
              break;
            case 2:
              // ret = OB_NOT_SUPPORTED;
              // LOG_WARN("sync refresh not supported now", K(ret));
              // LOG_USER_ERROR(OB_NOT_SUPPORTED, "nested sync refresh");
              refresh_info.nested_refresh_mode_ = ObMVNestedRefreshMode::CONSISTENT;
              break;
            default:
              break;
          }
        }
        LOG_INFO("nested refresh mode", K(nested_refresh_mode_node->value_));
      } else if (OB_ISNULL(nested_refresh_node)) {
        refresh_info.nested_refresh_mode_ = ObMVNestedRefreshMode::INDIVIDUAL;
      }

      if (OB_SUCC(ret) && refresh_interval_node != nullptr
          && 2 == refresh_interval_node->num_child_
          && (OB_NOT_NULL(refresh_interval_node->children_[0])
              || OB_NOT_NULL(refresh_interval_node->children_[1]))) {
        if (refresh_info.refresh_mode_ == ObMVRefreshMode::COMMIT) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ON COMMIT attribute followed by start with/next clause is not supported", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ON COMMIT attribute followed by start with/next clause is");
        } else if (refresh_info.refresh_mode_ == ObMVRefreshMode::STATEMENT) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ON STATEMENT attribute followed by start with/next clause is not supported", KR(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ON STATEMENT attribute followed by start with/next clause is");
        } else {
          ParseNode *start_date = refresh_interval_node->children_[0];
          ParseNode *next_date = refresh_interval_node->children_[1];
          int64_t current_time = ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
          int64_t start_time = OB_INVALID_TIMESTAMP;

          if (OB_NOT_NULL(start_date)
              && (T_MV_REFRESH_START_EXPR == start_date->type_)
              && (1 == start_date->num_child_)
              && (OB_NOT_NULL(start_date->children_))
              && (OB_NOT_NULL(start_date->children_[0]))) {
            if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(resolver.params_,
                *resolver.session_info_, *(start_date->children_[0]), *resolver.allocator_, start_time))) {
              LOG_WARN("failed to resolve date expr to timestamp", KR(ret));
            } else if (start_time < current_time) {
              ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
              LOG_WARN("the parameter start date must evaluate to a time in the future",
                  KR(ret), K(current_time), K(start_time));
              LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "start date");
            }
          }

          if (OB_SUCC(ret) && OB_NOT_NULL(next_date)) {
            int64_t next_time = OB_INVALID_TIMESTAMP;
            if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(resolver.params_,
                *resolver.session_info_, *next_date, *resolver.allocator_, next_time))) {
              LOG_WARN("fail to resolve date expr to timestamp", KR(ret));
            } else if (next_time < current_time) {
              ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
              LOG_WARN("the parameter next date must evaluate to a time in the future",
                  KR(ret), K(current_time), K(next_time));
              LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "next date");
            } else if (OB_INVALID_TIMESTAMP == start_time) {
              start_time = next_time;
            }

            if (OB_SUCC(ret)) {
              ObString next_date_str(next_date->str_len_, next_date->str_value_);
              if (OB_FAIL(ob_write_string(*resolver.allocator_, next_date_str, refresh_info.next_time_expr_))) {
                LOG_WARN("fail to write string", KR(ret));
              }
            }
          }

          if (OB_SUCC(ret)) {
            refresh_info.start_time_.set_timestamp(start_time);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    refresh_info.refresh_dop_ = resolver.mv_refresh_dop_;
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
    omt::ObTenantConfigGuard tenant_config(
        TENANT_CONF(resolver.session_info_->get_effective_tenant_id()));
    uint64_t tenant_data_version = 0;

    if (OB_FAIL(GET_MIN_DATA_VERSION(resolver.session_info_->get_effective_tenant_id(),
                                     tenant_data_version))) {
      LOG_WARN("get tenant data version failed", KR(ret));
    } else if (OB_FAIL(ObResolverUtils::check_schema_valid_for_mview(table_schema))) {
      LOG_WARN("failed to check schema valid for mview", KR(ret), K(table_schema));
    } else if (OB_FAIL(resolver.resolve_table_options(
                   parse_tree.children_[ObCreateViewResolver::TABLE_OPTION_NODE], false))) {
      LOG_WARN("fail to resolve table options", KR(ret));
    } else if (OB_ISNULL(mv_ainfo = create_arg.mv_ainfo_.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObMVAdditionalInfo from array error", K(ret));
    } else if (OB_FAIL(mv_ainfo->container_table_schema_.assign(table_schema))) {
      LOG_WARN("fail to assign table schema", KR(ret));
    } else if (OB_FAIL(resolve_materialized_view_container_table(
                   parse_tree.children_[ObCreateViewResolver::PARTITION_NODE],
                   mv_primary_key_node,
                   mv_ainfo->container_table_schema_, csts, resolver))) {
      LOG_WARN("fail do resolve for materialized view", K(ret));
    } else if (OB_FAIL(load_mview_dep_session_vars(*resolver.session_info_, select_stmt,
                                                   table_schema.get_local_session_var()))) {
      LOG_WARN("fail to load mview dep session variables", K(ret));
    } else if (!tenant_config.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant config is invalid", KR(ret));
    } else if (OB_NOT_NULL(parse_tree.children_[ObCreateViewResolver::COLUMN_GROUP_NODE]) &&
               OB_FAIL(resolver.resolve_column_group_helper(
                   parse_tree.children_[ObCreateViewResolver::COLUMN_GROUP_NODE],
                   mv_ainfo->container_table_schema_))) {
      LOG_WARN("fail to resolve column group", KR(ret));
    } else if (OB_FAIL(resolve_mv_options(
                   select_stmt, parse_tree.children_[ObCreateViewResolver::MVIEW_NODE],
                   mv_ainfo->mv_refresh_info_, table_schema, mv_ainfo->container_table_schema_,
                   mv_ainfo->required_columns_infos_, resolver))) {
      LOG_WARN("fail to resolve mv options", K(ret));
    }

    if (OB_SUCC(ret)) {
      int64_t refresh_parallelism = 0;
      if (OB_FAIL(resolver.resolve_hints(parse_tree.children_[ObCreateViewResolver::HINT_NODE],
                                         *stmt, mv_ainfo->container_table_schema_))) {
        LOG_WARN("resolve hints failed", K(ret));
      } else if (tenant_data_version < DATA_VERSION_4_3_5_1) {
        mv_ainfo->mv_refresh_info_.parallel_ = stmt->get_parallelism();
      } else if (OB_FAIL(storage::ObMViewRefresher::calc_mv_refresh_parallelism(
                     mv_ainfo->mv_refresh_info_.refresh_dop_, resolver.session_info_,
                     refresh_parallelism))) {
        LOG_WARN("fail to calculate refresh parallelism", KR(ret), "explicit_parallelism",
                 mv_ainfo->mv_refresh_info_.refresh_dop_);
      } else {
        mv_ainfo->mv_refresh_info_.parallel_ = refresh_parallelism;
      }
    }
  }

  return ret;
}

}
}