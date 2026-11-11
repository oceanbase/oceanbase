/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/mv/ob_simple_mav_printer.h"
#include "sql/resolver/mv/ob_simple_mjv_printer.h"
#include "sql/resolver/mv/ob_simple_join_mav_printer.h"
#include "sql/resolver/mv/ob_major_refresh_mjv_printer.h"
#include "sql/resolver/mv/ob_outer_join_mjv_printer.h"
#include "sql/resolver/mv/ob_outer_join_mav_printer.h"
#include "sql/resolver/mv/ob_union_all_mv_printer.h"
#include "sql/resolver/mv/ob_mv_compat_control.h"
#ifdef OB_BUILD_MV_CLOSE_MODULES
#include "sql/resolver/mv/ob_mv_refresh_helper.h"
#endif
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "share/schema/ob_mview_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMVProvider::~ObMVProvider()
{
  if (NULL != stmt_factory_) {
    stmt_factory_->~ObStmtFactory();
    stmt_factory_ = NULL;
  }
  if (NULL != expr_factory_) {
    expr_factory_->~ObRawExprFactory();
    expr_factory_ = NULL;
  }
  if (NULL != mem_ctx_) {
    DESTROY_CONTEXT(mem_ctx_);
    mem_ctx_ = NULL;
  }
}

int ObMVProvider::align_mv_stmt_output_column_names(ObSelectStmt *stmt,
                                                    const ObTableSchema &container_table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret));
  } else {
    const int64_t select_item_count = stmt->get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_item_count; ++i) {
      SelectItem &select_item = stmt->get_select_item(i);
      const ObColumnSchemaV2 *column_schema =
          container_table_schema.get_column_schema(OB_APP_MIN_COLUMN_ID + i);
      if (OB_ISNULL(select_item.expr_) || OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null select expression or column schema", K(ret), K(i),
                 KP(select_item.expr_), KP(column_schema));
      } else {
        if (lib::is_oracle_mode()) {
          // Oracle mode does not support explicit column lists on generated tables.
          // Outer MV queries need to reference these columns, so use container column
          // names instead of SELECT aliases.
          select_item.select_alias_name_.reset();
        } else if (select_item.select_alias_name_.empty()) {
          select_item.select_alias_name_ = select_item.alias_name_;
        }
        select_item.alias_name_ = column_schema->get_column_name_str();
        select_item.is_real_alias_ = true;
      }
    }
    if (OB_SUCC(ret) && stmt->is_set_stmt()) {
      ObIArray<ObSelectStmt *> &set_queries = stmt->get_set_query();
      for (int64_t i = 0; OB_SUCC(ret) && i < set_queries.count(); ++i) {
        ObSelectStmt *set_query = set_queries.at(i);
        if (OB_ISNULL(set_query) ||
            OB_UNLIKELY(select_item_count != set_query->get_select_item_size())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected set query", K(ret), K(i), K(select_item_count), KPC(set_query));
        } else if (OB_FAIL(SMART_CALL(align_mv_stmt_output_column_names(set_query, container_table_schema)))) {
          LOG_WARN("failed to align set query output column names", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

// 1. resolve mv definition and get stmt
// 2. check refresh type by stmt
int ObMVProvider::init_mv_provider(ObSQLSessionInfo *session_info,
                                   ObSchemaGetterGuard *schema_guard,
                                   const RtMvCheckType rt_expand_type,
                                   const uint64_t compat_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mv provider is initted twice", K(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(session_info));
  } else {
    const uint64_t tenant_id = session_info->get_effective_tenant_id();
    lib::ContextParam param;
    param.set_mem_attr(tenant_id, "MVProvider", ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL)
         .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    void *stmt_factory_buf = NULL;
    void *expr_factory_buf = NULL;
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_ctx_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mem entity is null", K(ret));
    } else if (OB_ISNULL(stmt_factory_buf = mem_ctx_->get_allocator().alloc(sizeof(ObStmtFactory)))
               || OB_ISNULL(expr_factory_buf = mem_ctx_->get_allocator().alloc(sizeof(ObRawExprFactory)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret), K(stmt_factory_buf), K(expr_factory_buf), K(sizeof(ObStmtFactory)), K(sizeof(ObRawExprFactory)));
    } else {
      stmt_factory_ = new (stmt_factory_buf) ObStmtFactory(mem_ctx_->get_allocator());
      expr_factory_ = new (expr_factory_buf) ObRawExprFactory(mem_ctx_->get_allocator());
      WITH_CONTEXT(mem_ctx_) {
        ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
        ObSelectStmt *view_stmt = NULL;
        ObDMLStmt *trans_stmt = NULL;
        ObQueryCtx *query_ctx = NULL;
        ObSchemaChecker schema_checker;
        SMART_VARS_2((ObExecContext, exec_ctx, alloc), (ObPhysicalPlanCtx, phy_plan_ctx, alloc)) {
          LinkExecCtxGuard link_guard(*session_info, exec_ctx);
          // add empty sql ctx
          ObSqlCtx empty_ctx;
          empty_ctx.schema_guard_ = schema_guard;
          exec_ctx.set_sql_ctx(&empty_ctx);
          exec_ctx.set_my_session(session_info);
          exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
          bool is_vars_matched = false;
          if (OB_ISNULL(query_ctx = stmt_factory_->get_query_ctx())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret), K(query_ctx));
          } else if (OB_FALSE_IT(query_ctx->sql_schema_guard_.set_schema_guard(schema_guard))) {
          } else if (OB_FALSE_IT(expr_factory_->set_query_ctx(query_ctx))) {
          } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_, (session_info->get_session_type() != ObSQLSessionInfo::INNER_SESSION
                                                                                ? session_info->get_sessid_for_table() : OB_INVALID_ID)))) {
            LOG_WARN("init schema checker failed", K(ret));
          } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(mview_id_, mv_schema_))
                    || OB_ISNULL(mv_schema_) || OB_UNLIKELY(!mv_schema_->is_materialized_view())) {
            COVER_SUCC(OB_ERR_UNEXPECTED);
            LOG_WARN("unexpected mv schema", K(ret), KPC(mv_schema_));
          } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(mv_schema_->get_data_table_id(), mv_container_schema_))
                    || OB_ISNULL(mv_container_schema_)) {
            COVER_SUCC(OB_ERR_UNEXPECTED);
            LOG_WARN("fail to get mv container schema", KR(ret), K(mv_schema_->get_data_table_id()), K(mv_container_schema_));
          } else if (OB_FAIL(check_mview_dep_session_vars(*mv_schema_, *session_info, true, is_vars_matched))) {
            LOG_WARN("failed to check mview dep session vars", K(ret));
          } else if (OB_FAIL(generate_mv_stmt(alloc,
                                              *stmt_factory_,
                                              *expr_factory_,
                                              schema_checker,
                                              *session_info,
                                              *mv_schema_,
                                              view_stmt))) {
            LOG_WARN("failed to gen mv stmt", K(ret));
          } else if (OB_FAIL(align_mv_stmt_output_column_names(view_stmt, *mv_container_schema_))) {
            LOG_WARN("failed to align mv output column names", K(ret));
          } else if (OB_FALSE_IT(trans_stmt = view_stmt)) {
          } else {
            const ObMVCheckerParams checker_params{alloc,
                                                   schema_checker,
                                                   *stmt_factory_,
                                                   *expr_factory_,
                                                   *session_info,
                                                   *mv_schema_,
                                                   *mv_container_schema_,
                                                   compat_version,
                                                   rt_expand_type > NO_NEED_RT_EXPAND && mv_schema_->mv_on_query_computation(),
                                                   false /* for_create_mview */};
            if (OB_UNLIKELY(NEED_RT_EXPAND == rt_expand_type && !mv_schema_->mv_on_query_computation())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("need to check rt expand, but the mview is not defined with on query computation", KR(ret));
            } else if (OB_FAIL(ObMVChecker::prepare_mv_stmt_and_check_refresh_type(checker_params,
                                                                                   trans_stmt,
                                                                                   checker_result_))) {
              LOG_WARN("failed to check mv fast refresh type", K(ret));
            } else if (OB_ISNULL(mv_def_stmt_ = static_cast<ObSelectStmt *>(trans_stmt))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null stmt", K(ret));
            } else if (!IS_VALID_FAST_REFRESH_TYPE(checker_result_.get_refresh_type())) {
              LOG_TRACE("mv not support fast refresh",
                        K(checker_result_.get_refresh_type()),
                        K(mv_schema_->get_table_name()));
            }
          }
          exec_ctx.set_physical_plan_ctx(NULL);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    compat_version_ = compat_version;
    inited_ = true;
  }
  return ret;
}

int ObMVProvider::collect_tables_need_mlog(const ObSelectStmt* stmt,
                                           ObIArray<uint64_t> &tables_need_mlog)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret), KPC(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
    const TableItem* table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (table->is_mv_proctime_table_) {
      if (NULL != table->ref_query_
          && OB_FAIL(ObOptimizerUtil::remove_item(child_stmts, table->ref_query_))) {
        LOG_WARN("failed to remove proctime view stmt", K(ret));
      }
    } else if (OB_FAIL(add_var_to_array_no_dup(tables_need_mlog, table->ref_id_))) {
      LOG_WARN("failed to push back table ref id", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(collect_tables_need_mlog(child_stmts.at(i), tables_need_mlog)))) {
      LOG_WARN("failed to check collect tables need mlog for child stmt", K(ret), K(i), KPC(stmt));
    }
  }
  return ret;
}

int ObMVProvider::print_mv_operators(ObSQLSessionInfo *session_info,
                                     const ObMVPrinterRefreshInfo *refresh_info,
                                     ObIAllocator &str_alloc,
                                     ObIArray<ObString> &operators)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObSchemaChecker schema_checker;
  operators.reuse();
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("mv provider is not inited", K(ret));
  } else if (OB_ISNULL(mv_def_stmt_) || OB_ISNULL(mv_schema_) || OB_ISNULL(mv_container_schema_)
             || OB_ISNULL(mem_ctx_) || OB_ISNULL(stmt_factory_) || OB_ISNULL(expr_factory_)
             || OB_ISNULL(session_info) || OB_ISNULL(query_ctx = mv_def_stmt_->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_def_stmt_), K(mv_schema_), K(mv_container_schema_),
             K(stmt_factory_), K(expr_factory_), K(session_info), K(query_ctx));
  } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_,
                          (session_info->get_session_type() != ObSQLSessionInfo::INNER_SESSION
                          ? session_info->get_sessid_for_table() : OB_INVALID_ID)))) {
    LOG_WARN("failed to init schema checker", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObIAllocator &alloc = mem_ctx_->get_arena_allocator();
    ObTransformerCtx trans_ctx;
    ObMVPrinterCtx mv_printer_ctx(alloc,
                                  *session_info,
                                  *stmt_factory_,
                                  *expr_factory_,
                                  compat_version_,
                                  trans_ctx,
                                  refresh_info);
    trans_ctx.allocator_ = &alloc;
    trans_ctx.schema_checker_ = &schema_checker;
    trans_ctx.session_info_ = session_info;
    trans_ctx.exec_ctx_ = session_info->get_cur_exec_ctx();
    trans_ctx.stmt_factory_ = stmt_factory_;
    trans_ctx.expr_factory_ = expr_factory_;
    trans_ctx.sql_schema_guard_ = &query_ctx->sql_schema_guard_;
    mv_printer_ctx.rt_mv_last_refresh_ts_ = rt_mv_last_refresh_ts_;
#ifdef OB_BUILD_MV_CLOSE_MODULES
    bool enable_fast_refresh_with_cur_time = false;
    bool enable_simplify_aggr_dep = false;
    const ObOptParamHint &opt_params = query_ctx->get_global_hint().opt_params_;
    if (OB_MV_FAST_REFRESH_GENERAL == checker_result_.get_refresh_type()) {
      if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ENABLE_FAST_REFRESH_WITH_CUR_TIME,
                                                enable_fast_refresh_with_cur_time))) {
        LOG_WARN("failed to get current time fast refresh option", K(ret));
      } else if (OB_FAIL(opt_params.get_bool_opt_param(ObOptParamHint::ENABLE_FAST_REFRESH_SIMPLIFY_AGGR_DEP,
                                                       enable_simplify_aggr_dep))) {
        LOG_WARN("failed to get simplify aggregate dependency option", K(ret));
      }
    }
#endif
    if (OB_SUCC(ret)) {
      query_ctx->get_query_hint_for_update().reset();
      query_ctx->stmt_count_ = 0;
      switch (checker_result_.get_refresh_type()) {
        case OB_MV_FAST_REFRESH_SIMPLE_MAV: {
          ObSimpleMAVPrinter printer(mv_printer_ctx,
                                     *mv_schema_,
                                     *mv_container_schema_,
                                     *mv_def_stmt_,
                                     checker_result_.get_mlog_tables());
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print simple mav operator stmts", K(ret));
          }
          break;
        }
        case OB_MV_FAST_REFRESH_SIMPLE_MJV: {
          ObSimpleMJVPrinter printer(mv_printer_ctx,
                                     *mv_schema_,
                                     *mv_container_schema_,
                                     *mv_def_stmt_,
                                     checker_result_.get_mlog_tables());
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print simple mjv operator stmts", K(ret));
          }
          break;
        }
        case OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV: {
          ObSimpleJoinMAVPrinter printer(mv_printer_ctx,
                                         *mv_schema_,
                                         *mv_container_schema_,
                                         *mv_def_stmt_,
                                         checker_result_.get_mlog_tables());
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print simple join mav operator stmts", K(ret));
          }
          break;
        }
        case OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV: {
          ObMajorRefreshMJVPrinter printer(mv_printer_ctx, *mv_schema_, *mv_container_schema_, *mv_def_stmt_);
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print major refresh mjv operator stmts", K(ret));
          }
          break;
        }
        case OB_MV_FAST_REFRESH_OUTER_JOIN_MJV: {
          // Used when RELATION_BASED_FAST_REFRESH is disabled.
          ObOuterJoinMJVPrinter printer(mv_printer_ctx,
                                        *mv_schema_,
                                        *mv_container_schema_,
                                        *mv_def_stmt_,
                                        checker_result_.get_mlog_tables());
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print simple outer join mjv operator stmts", K(ret));
          }
          break;
        }
        case OB_MV_FAST_REFRESH_UNION_ALL: {
          // Used when RELATION_BASED_FAST_REFRESH is disabled.
          ObUnionAllMVPrinter printer(mv_printer_ctx,
                                      *mv_schema_,
                                      *mv_container_schema_,
                                      *mv_def_stmt_,
                                      checker_result_.get_union_all_marker_idx(),
                                      checker_result_.get_child_refresh_types(),
                                      checker_result_.get_mlog_tables());
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print union all operator stmts", K(ret));
          }
          break;
        }
        case OB_MV_FAST_REFRESH_OUTER_JOIN_MAV: {
          ObOuterJoinMAVPrinter printer(mv_printer_ctx,
                                        *mv_schema_,
                                        *mv_container_schema_,
                                        *mv_def_stmt_,
                                        checker_result_.get_mlog_tables());
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print outer join mav operator stmts", K(ret));
          }
          break;
        }
#ifdef OB_BUILD_MV_CLOSE_MODULES
        case OB_MV_FAST_REFRESH_GENERAL: {
          mv_printer_ctx.update_as_insert_delete_ = false;
          ObMVRefreshHelper printer(mv_printer_ctx,
                                    *mv_schema_,
                                    *mv_container_schema_,
                                    *mv_def_stmt_,
                                    checker_result_,
                                    false /* for_create_mview */,
                                    mv_printer_ctx.for_rt_expand() && mv_schema_->mv_on_query_computation(),
                                    enable_fast_refresh_with_cur_time,
                                    enable_simplify_aggr_dep);
          if (OB_FAIL(printer.print_mv_operators(str_alloc, operators))) {
            LOG_WARN("failed to print relation-based mv operator stmts", K(ret));
          }
          break;
        }
#endif
        default:  {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected refresh type", K(ret), K(checker_result_.get_refresh_type()));
          break;
        }
      }
    }
  }
  return ret;
}

int ObMVProvider::check_mv_refreshable(const uint64_t tenant_id,
                                       const uint64_t mview_id,
                                       ObSQLSessionInfo *session_info,
                                       ObSchemaGetterGuard *schema_guard,
                                       uint64_t compat_version,
                                       bool &can_fast_refresh,
                                       FastRefreshableNotes &note)
{
  int ret = OB_SUCCESS;
  share::schema::ObMViewInfo mview_info;
  can_fast_refresh = false;
  ObMVProvider mv_provider(mview_id);
  if (OB_INVALID_VERSION == compat_version) {
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_FAIL(share::schema::ObMViewInfo::fetch_mview_info(*GCTX.sql_proxy_,
                                                                    tenant_id,
                                                                    mview_id,
                                                                    mview_info))) {
      LOG_WARN("failed to fetch mview info", K(ret), K(tenant_id), K(mview_id));
    } else {
      compat_version = mview_info.get_compat_version();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mv_provider.init_mv_provider(session_info,
                                                  schema_guard,
                                                  DEPENDS_ON_SCHEMA,
                                                  compat_version))) {
    LOG_WARN("fail to init mv provider", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_REFRESH_INVALID
                         == mv_provider.checker_result_.get_refresh_type())) {
    // column type for mv is changed after it is created
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not refresh mv", K(ret), K(mv_provider.checker_result_.get_refresh_type()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh mv after column types change is");
  } else if (OB_FAIL(note.error_.assign(mv_provider.checker_result_.get_note().error_))) {
    LOG_WARN("fail to assign ObSqlString", K(ret));
  } else {
    can_fast_refresh = IS_VALID_FAST_REFRESH_TYPE(mv_provider.checker_result_.get_refresh_type());
  }
  return ret;
}

int ObMVProvider::get_mlog_mv_refresh_infos(ObSQLSessionInfo *session_info,
                                            ObSchemaGetterGuard *schema_guard,
                                            const ObMVRefreshMethod refresh_method,
                                            const bool initial_refresh_finished,
                                            const uint64_t compat_version,
                                            ObIArray<ObDependencyInfo> &dep_infos,
                                            ObIArray<uint64_t> &tables_need_mlog,
                                            bool &can_fast_refresh)
{
  int ret = OB_SUCCESS;
  can_fast_refresh = false;
  ObQueryCtx *query_ctx = NULL;
  uint64_t data_version = 0;
  dep_infos.reuse();
  tables_need_mlog.reuse();
  if (OB_FAIL(init_mv_provider(session_info,
                               schema_guard,
                               NO_NEED_RT_EXPAND,
                               compat_version))) {
    LOG_WARN("failed to init mv provider", K(ret));
  } else if (OB_ISNULL(mv_def_stmt_) || OB_ISNULL(query_ctx = mv_def_stmt_->get_query_ctx())
             || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(mv_def_stmt_), K(query_ctx), K(session_info));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (OB_FAIL(ObDependencyInfo::collect_dep_infos_for_view(query_ctx->reference_obj_tables_,
                                                                  (data_version >= MOCK_DATA_VERSION_4_4_2_0 && data_version < DATA_VERSION_4_5_0_0)
                                                                  || (data_version >= DATA_VERSION_4_5_1_0) /* need_nested_dep */,
                                                                  dep_infos))) {
    LOG_WARN("failed to collect dep infos", K(ret));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_REFRESH_INVALID == checker_result_.get_refresh_type())) {
    // column type for mv is changed after it is created
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not refresh mv", K(ret), K(checker_result_.get_refresh_type()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh mv after column types change is");
  } else if (!IS_VALID_FAST_REFRESH_TYPE(checker_result_.get_refresh_type())) {
    can_fast_refresh = false;
  } else if (ObMVRefreshMethod::FAST != refresh_method && ObMVRefreshMethod::FORCE != refresh_method) {
    LOG_INFO("skip fast refresh check as refresh method is not fast or force", K(refresh_method));
    can_fast_refresh = false;
    if (OB_ISNULL(get_error_str().ptr())) {
      checker_result_.get_note().error_.assign_fmt("materialized view is defined with complete refresh");
    }
  } else if (!initial_refresh_finished) {
    LOG_INFO("mview not refreshed yet, disable fast refresh", K_(mview_id), K(refresh_method));
    can_fast_refresh = false;
    if (OB_ISNULL(get_error_str().ptr())) {
      checker_result_.get_note().error_.assign_fmt("initial refresh of the materialized view has not been completed yet");
    }
  } else {
    can_fast_refresh = true;
  }

  if (OB_SUCC(ret) && can_fast_refresh && OB_FAIL(collect_tables_need_mlog(mv_def_stmt_, tables_need_mlog))) {
    LOG_WARN("failed to collect tables need mlog", K(ret));
  }
  return ret;
}

int ObMVProvider::get_major_refresh_operators(ObSQLSessionInfo *session_info,
                                              ObSchemaGetterGuard *schema_guard,
                                              const ObMVPrinterRefreshInfo &refresh_info,
                                              ObIArray<ObString> &operators)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_mv_provider(session_info, schema_guard, NO_NEED_RT_EXPAND, 0))) {
    LOG_WARN("failed to init mv provider", K(ret));
  } else if (OB_ISNULL(mv_def_stmt_) || OB_ISNULL(mv_schema_) || OB_ISNULL(mv_container_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_def_stmt_), K(mv_schema_), K(mv_container_schema_));
  } else if (OB_UNLIKELY(!mv_container_schema_->mv_major_refresh()
                         || OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV != checker_result_.get_refresh_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected major refresh checker result",
             K(ret),
             "mv_major_refresh",
             mv_container_schema_->mv_major_refresh(),
             "refresh_type",
             checker_result_.get_refresh_type());
  } else if (OB_FAIL(print_mv_operators(session_info,
                                        &refresh_info,
                                        CURRENT_CONTEXT->get_arena_allocator(),
                                        operators))) {
    LOG_WARN("failed to print mv operators", K(ret));
  } else if (OB_UNLIKELY(operators.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  }
  return ret;
}

int ObMVProvider::get_rt_mview_info(const share::schema::ObMViewInfo &mview_info,
                                    ObIArray<ObRtMViewInfo> &rt_mview_infos,
                                    ObRtMViewInfo &rt_mview_info) const
{
  int ret = OB_SUCCESS;
  share::SCN refresh_scn;
  int64_t last_refresh_ts_us = 0;
  if (OB_UNLIKELY(mview_id_ != mview_info.get_mview_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mview id", K(ret), K_(mview_id), K(mview_info));
  } else if (ObRtMViewInfo::find(rt_mview_infos, mview_id_, rt_mview_info)) {
    // do nothing
  } else {
    // Keep the reference SCN consistent with execution, which uses DATA_SYNC_SCN
    // for a synchronized non-major MV.
    const bool use_last_refresh_scn = !mview_info.get_is_synced()
                                      || share::schema::ObMVRefreshMode::MAJOR_COMPACTION
                                         == mview_info.get_refresh_mode();
    const uint64_t effective_refresh_scn = use_last_refresh_scn ? mview_info.get_last_refresh_scn()
                                                                : mview_info.get_data_sync_scn();
    const bool has_valid_refresh_scn = 0 != effective_refresh_scn
                                       && share::OB_INVALID_SCN_VAL != effective_refresh_scn;
    if (!has_valid_refresh_scn) {
      // do nothing
    } else if (OB_FAIL(refresh_scn.convert_for_sql(effective_refresh_scn))) {
      LOG_WARN("failed to convert refresh scn", K(ret), K(effective_refresh_scn));
    } else {
      last_refresh_ts_us = refresh_scn.convert_to_ts();
    }
    if (OB_SUCC(ret)) {
      rt_mview_info = ObRtMViewInfo(mview_id_, last_refresh_ts_us);
      if (OB_FAIL(rt_mview_infos.push_back(rt_mview_info))) {
        LOG_WARN("failed to add rt mview info", K(ret), K(rt_mview_info));
      }
    }
  }
  return ret;
}

// expand_view will used to generate plan, need use alloc to deep copy the query str
int ObMVProvider::get_real_time_mv_expand_view(const uint64_t tenant_id,
                                               const uint64_t mview_id,
                                               ObSQLSessionInfo *session_info,
                                               ObSchemaGetterGuard *schema_guard,
                                               ObIAllocator &alloc,
                                               ObString &expand_view,
                                               bool &is_major_refresh_mview,
                                               ObIArray<ObRtMViewInfo> &rt_mview_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 1> operators;
  ObMVProvider mv_provider(mview_id);
  share::schema::ObMViewInfo mview_info;
  ObRtMViewInfo rt_mview_info;
  expand_view.reset();
  is_major_refresh_mview = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(share::schema::ObMViewInfo::fetch_mview_info(*GCTX.sql_proxy_, tenant_id, mview_id, mview_info))) {
    LOG_WARN("failed to fetch mview info", K(ret), K(mview_id));
  } else if (OB_FAIL(mv_provider.init_mv_provider(session_info,
                                                  schema_guard,
                                                  NEED_RT_EXPAND,
                                                  mview_info.get_compat_version()))) {
    LOG_WARN("failed to init mv provider", K(ret));
  } else if (OB_UNLIKELY(!IS_VALID_FAST_REFRESH_TYPE(mv_provider.checker_result_.get_refresh_type()))) {
    ret = OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE;
    LOG_WARN("mview can not on query computation", K(ret), K(mview_id));
  } else if (OB_FAIL(mv_provider.get_rt_mview_info(mview_info, rt_mview_infos, rt_mview_info))) {
    LOG_WARN("failed to get rt mview info", K(ret), K(mview_id));
  } else if (OB_FALSE_IT(mv_provider.rt_mv_last_refresh_ts_ = rt_mview_info.last_refresh_ts_us_)) {
  } else if (OB_FAIL(mv_provider.print_mv_operators(session_info,
                                                    NULL,  // refresh_info
                                                    alloc,
                                                    operators))) {
    LOG_WARN("failed to print mv operators", K(ret));
  } else if (OB_UNLIKELY(1 != operators.count() || operators.at(0).empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else {
    expand_view = operators.at(0);
    is_major_refresh_mview = OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV == mv_provider.checker_result_.get_refresh_type();
    LOG_TRACE("finish generate rt mv expand view", K(mview_id), K(is_major_refresh_mview), K(expand_view));
  }
  return ret;
}

int ObMVProvider::generate_mv_stmt(ObIAllocator &alloc,
                                   ObStmtFactory &stmt_factory,
                                   ObRawExprFactory &expr_factory,
                                   ObSchemaChecker &schema_checker,
                                   ObSQLSessionInfo &session_info,
                                   const ObTableSchema &mv_schema,
                                   ObSelectStmt *&view_stmt)
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  const ObString raw_view_def = mv_schema.get_view_schema().get_expand_view_definition_for_mv_str().empty() ?
                                mv_schema.get_view_schema().get_view_definition_str() :
                                mv_schema.get_view_schema().get_expand_view_definition_for_mv_str();
  ObString view_definition;
  ParseResult parse_result;
  ParseNode *node = NULL;
  ObParser parser(alloc, session_info.get_sql_mode(), session_info.get_charsets4parser());
  ObResolverParams resolver_ctx;
  resolver_ctx.allocator_ = &alloc;
  resolver_ctx.schema_checker_ = &schema_checker;
  resolver_ctx.session_info_ = &session_info;
  resolver_ctx.expr_factory_ = &expr_factory;
  resolver_ctx.stmt_factory_ = &stmt_factory;
  resolver_ctx.sql_proxy_ = GCTX.sql_proxy_;
  resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
  resolver_ctx.is_mview_definition_sql_ = true;
  ObSelectStmt *sel_stmt = NULL;
  ObSelectResolver select_resolver(resolver_ctx);
  if (OB_FAIL(ObSQLUtils::copy_and_convert_string_charset(alloc,
                                                          raw_view_def,
                                                          view_definition,
                                                          CS_TYPE_UTF8MB4_GENERAL_CI,
                                                          session_info.get_local_collation_connection()))) {
    LOG_WARN("fail to copy and convert string charset", K(ret));
  } else if (OB_FAIL(parser.parse(view_definition, parse_result))) {
    LOG_WARN("parse view definition failed", K(view_definition), K(ret));
  } else if (OB_ISNULL(node = parse_result.result_tree_->children_[0]) ||
             OB_UNLIKELY(T_SELECT != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mv select node", K(ret), K(node), K(node->type_));
  } else if (OB_FALSE_IT(resolver_ctx.query_ctx_->set_questionmark_count(
                                   static_cast<int64_t>(parse_result.question_mark_ctx_.count_)))) {
  } else if (OB_FAIL(select_resolver.resolve(*node))) {
    LOG_WARN("resolve view definition failed", K(ret), K(view_definition));
  } else if (OB_ISNULL(sel_stmt = static_cast<ObSelectStmt *>(select_resolver.get_basic_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mv stmt", K(ret), K(sel_stmt));
  } else if (OB_FAIL(stmt_factory.get_query_ctx()->query_hint_.init_query_hint(&alloc,
                                                                               &session_info,
                                                                               resolver_ctx.global_hint_,
                                                                               sel_stmt))) {
    LOG_WARN("failed to init query hint", K(ret), K(sel_stmt));
  } else {
    view_stmt = sel_stmt;
    LOG_DEBUG("generate mv stmt", KPC(view_stmt));
  }

  return ret;
}

int ObMVProvider::check_mview_dep_session_vars(const ObTableSchema &mv_schema,
                                               const ObSQLSessionInfo &session,
                                               const bool gen_error,
                                               bool &is_vars_matched)
{
  int ret = OB_SUCCESS;
  is_vars_matched = false;
  ObSEArray<const ObSessionSysVar*, 8> local_diff_vars;
  ObSEArray<ObObj, 8> cur_var_vals;
  if (OB_UNLIKELY(!mv_schema.is_materialized_view())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table schema", K(ret), K(mv_schema));
  } else if (OB_FAIL(mv_schema.get_local_session_var().get_different_vars_from_session(&session,
                                                                                       local_diff_vars,
                                                                                       cur_var_vals))) {
    LOG_WARN("failed to check vars same with session ", K(ret), K(mv_schema.get_local_session_var()));
  } else if (local_diff_vars.empty()) {
    is_vars_matched = true;
  } else if (OB_UNLIKELY(local_diff_vars.count() != cur_var_vals.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected array size", K(ret), K(local_diff_vars.count()), K(cur_var_vals.count()));
  } else {
    is_vars_matched = false;
    ObArenaAllocator alloc;
    ObString var_name;
    ObString local_var_val;
    ObString cur_var_val;
    const ObSessionSysVar *sys_var = NULL;
    const ObString &mview_name = mv_schema.get_table_name();
    OPT_TRACE_BEGIN_SECTION;
    OPT_TRACE_TITLE("some session variables differ from values used when the mview was created: ", mview_name);
    if (gen_error) {
      LOG_WARN("some session variables differ from values used when the mview was created. ", K(mview_name));
    } else {
      LOG_TRACE("some session variables differ from values used when the mview was created. ", K(mview_name));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < local_diff_vars.count(); ++i) {
      if (OB_ISNULL(sys_var = local_diff_vars.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(sys_var));
      } else if (OB_FAIL(ObSysVarFactory::get_sys_var_name_by_id(sys_var->type_, var_name))) {
        LOG_WARN("get sysvar name failed", K(ret));
      } else if (OB_FAIL(ObSessionSysVar::get_sys_var_val_str(sys_var->type_, sys_var->val_, alloc, local_var_val))) {
        LOG_WARN("failed to get sys var str", K(ret));
      } else if (OB_FAIL(ObSessionSysVar::get_sys_var_val_str(sys_var->type_, cur_var_vals.at(i), alloc, cur_var_val))) {
        LOG_WARN("failed to get sys var str", K(ret));
      } else {
        OPT_TRACE(i, ".", var_name, ",  old value:", local_var_val, ",  current value:", cur_var_val);
        if (gen_error) {
          LOG_WARN("session variable changed", K(i), K(var_name), K(local_var_val), K(cur_var_val));
        } else {
          LOG_TRACE("session variable changed", K(i), K(var_name), K(local_var_val), K(cur_var_val));
        }
      }
    }

    OPT_TRACE_END_SECTION;

    if (!gen_error) {
      ret = OB_SUCCESS;
    } else {
      // show user error use the last different sys variables
      ret = OB_ERR_SESSION_VAR_CHANGED;
      LOG_USER_ERROR(OB_ERR_SESSION_VAR_CHANGED,
                        var_name.length(), var_name.ptr(),
                        mv_schema.get_table_name_str().length(), mv_schema.get_table_name_str().ptr(),
                        local_var_val.length(), local_var_val.ptr());
    }
  }
  return ret;
}

//  str_alloc used to allocate mview_str
int ObMVProvider::get_complete_refresh_mview_str(const ObTableSchema &mv_schema,
                                                 ObSchemaGetterGuard &schema_guard,
                                                 const share::SCN *mv_refresh_scn,
                                                 const share::SCN *table_refresh_scn,
                                                 ObIAllocator &str_alloc,
                                                 ObString &mview_str)
{
  int ret = OB_SUCCESS;
  mview_str.reset();
  const uint64_t tenant_id = mv_schema.get_tenant_id();
  bool is_oracle_mode = false;
  ObSQLSessionInfo mock_session(tenant_id);
  const ObTenantSchema *tenant_info = nullptr;
  static const uint32_t MOCK_MV_REFRESH_SESSID = 100;
  if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(tenant_id,
                                                                     mv_schema.get_table_id(),
                                                                     is_oracle_mode))) {
    LOG_WARN("check oracle mode failed", K(ret), K(tenant_id), K(mv_schema.get_table_id()));
  } else if (OB_FAIL(mock_session.init(MOCK_MV_REFRESH_SESSID, 0, NULL, NULL, 0, tenant_id, 0))) {
    LOG_WARN("fail to init mock session for complete refresh mview str", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("Failed to get tenant info", K(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null tenant schema", K(ret), K(tenant_id));
  } else if (OB_FAIL(mock_session.load_default_sys_variable(false, false))) {
    LOG_WARN("Failed to load default sys variable", K(ret), K(tenant_id));
  } else if (OB_FAIL(mock_session.load_default_configs_in_pc())) {
    LOG_WARN("Failed to load default configs in pc", K(ret), K(tenant_id));
  } else if (OB_FAIL(mock_session.init_tenant(tenant_info->get_tenant_name(), tenant_id))) {
    LOG_WARN("Failed to init tenant in session", K(ret), K(tenant_id));
  } else if (OB_FAIL(mv_schema.get_local_session_var().apply_different_vars_to_session(mock_session, NULL, NULL))) {
    LOG_WARN("fail to apply solidified session vars for complete refresh mview str", K(ret));
  } else {
    mock_session.set_inner_session();
    mock_session.set_compatibility_mode(is_oracle_mode ? ObCompatibilityMode::ORACLE_MODE : ObCompatibilityMode::MYSQL_MODE);
    mock_session.set_query_start_time(ObTimeUtility::current_time());
    lib::ContextParam param;
    param.set_mem_attr(mock_session.get_effective_tenant_id(), "MVProvider", ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL)
         .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    CREATE_WITH_TEMP_CONTEXT(param) {
      ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
      ObSelectStmt *view_stmt = NULL;
      ObStmtFactory stmt_factory(alloc);
      ObRawExprFactory expr_factory(alloc);
      ObSchemaChecker schema_checker;
      ObQueryCtx *query_ctx = NULL;
      SMART_VARS_2((ObExecContext, exec_ctx, alloc), (ObPhysicalPlanCtx, phy_plan_ctx, alloc)) {
        LinkExecCtxGuard link_guard(mock_session, exec_ctx);
        // add empty sql ctx so that resolve_external_symbol can fetch the package guard
        // from the exec context when resolving schema-qualified UDFs in the reparsed view def
        ObSqlCtx empty_ctx;
        empty_ctx.schema_guard_ = &schema_guard;
        exec_ctx.set_sql_ctx(&empty_ctx);
        exec_ctx.set_my_session(&mock_session);
        exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
        if (OB_ISNULL(query_ctx = stmt_factory.get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(query_ctx));
        } else if (OB_FALSE_IT(query_ctx->sql_schema_guard_.set_schema_guard(&schema_guard))) {
        } else if (OB_FALSE_IT(expr_factory.set_query_ctx(query_ctx))) {
        } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_, OB_INVALID_ID))) {
          LOG_WARN("init schema checker failed", K(ret));
        } else if (OB_FAIL(generate_mv_stmt(alloc,
                                            stmt_factory,
                                            expr_factory,
                                            schema_checker,
                                            mock_session,
                                            mv_schema,
                                            view_stmt))) {
          LOG_WARN("failed to gen mv stmt", K(ret));
        } else if (OB_FAIL(ObMVPrinter::print_complete_refresh_mview_operator(expr_factory,
                                                                              mv_refresh_scn,
                                                                              table_refresh_scn,
                                                                              *view_stmt,
                                                                              str_alloc,
                                                                              mview_str))) {
          LOG_WARN("failed to print complete refresh mview operator", K(ret));
        } else if (OB_UNLIKELY(mview_str.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty mview select sql", K(ret), K(tenant_id),
                   "mview_id", mv_schema.get_table_id());
        }
        exec_ctx.set_physical_plan_ctx(NULL);
      }
    }
  }
  return ret;
}

// ATTENTION:
// Only call expand_mv_stmt_with_dependent_columns ONCE when creating materialized view!
// When refreshing or checking refreshable, use the expand_view_definition_for_mv_ stored in the schema
int ObMVProvider::expand_mv_stmt_with_dependent_columns(ObSelectStmt *view_stmt,
                                                        const ObIArray<std::pair<ObRawExpr*, int64_t>> &dependent_columns,
                                                        ObIAllocator &alloc,
                                                        ObSQLSessionInfo *session_info,
                                                        ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  int64_t alias_name_idx = 0;
  if (OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(view_stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dependent_columns.count(); ++i) {
    const std::pair<ObRawExpr*, int64_t> &dep_column = dependent_columns.at(i);
    ObString alias_name;
    if (OB_ISNULL(dep_column.first)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(dep_column.first));
    } else if (OB_FAIL(gen_dep_column_alias_name(*view_stmt, alias_name_idx, alloc, alias_name))) {
      LOG_WARN("failed to gen dep column alias name", K(ret));
    } else if (OB_FAIL(create_select_item_for_mv_stmt(view_stmt,
                                                      dep_column.first,
                                                      dep_column.second,
                                                      alias_name,
                                                      expr_factory))) {
      LOG_WARN("failed to create select item for mv stmt", K(ret));
    } else if (view_stmt->is_set_stmt()
               && OB_FAIL(ObOptimizerUtil::gen_set_target_list(&alloc,
                                                               session_info,
                                                               &expr_factory,
                                                               view_stmt))) {
      LOG_WARN("failed to generate select list for set", K(ret));
    }
  }
  return ret;
}

int ObMVProvider::gen_dep_column_alias_name(const ObSelectStmt &stmt,
                                            int64_t &idx,
                                            ObIAllocator &alloc,
                                            ObString &alias_name)
{
  int ret = OB_SUCCESS;
  const uint64_t OB_MAX_SUBQUERY_NAME_LENGTH = 64;
  char buf[OB_MAX_SUBQUERY_NAME_LENGTH];
  int64_t buf_len = OB_MAX_SUBQUERY_NAME_LENGTH;
  int64_t pos = 0;
  bool need_gen_next = true;
  while (OB_SUCC(ret) && need_gen_next) {
    alias_name.reset();
    pos = 0;
    need_gen_next = false;
    if (OB_FAIL(BUF_PRINTF("__MV_DEP_COL_%ld$$", idx++))) {
      LOG_WARN("failed to buf print for alias name", K(ret));
    } else if (OB_FAIL(ob_write_string(alloc, ObString(pos, buf), alias_name))) {
      LOG_WARN("failed to write string", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_select_item_size(); ++i) {
      const SelectItem &select_item = stmt.get_select_item(i);
      if (ObCharset::case_compat_mode_equal(alias_name, select_item.alias_name_)) {
        need_gen_next = true;
        break;
      }
    }
  }
  return ret;
}

int ObMVProvider::create_select_item_for_mv_stmt(ObSelectStmt *stmt,
                                                 ObRawExpr *select_expr,
                                                 const int64_t sub_stmt_idx,
                                                 ObString &alias_name,
                                                 ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(select_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(select_expr), K(sub_stmt_idx));
  } else if (stmt->is_set_stmt()) {
    const ObIArray<ObSelectStmt*> &set_queries = stmt->get_set_query();
    if (OB_INVALID_ID == sub_stmt_idx) {
      // build the union all marker column
      ObObj obj;
      number::ObNumber num;
      const ObObjType &marker_type = select_expr->get_result_type().get_type();
      if (OB_UNLIKELY(!select_expr->is_const_raw_expr()
                      || !ob_is_numeric_type(marker_type))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected union all marker expr, expect const numeric", K(ret), KPC(select_expr));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < set_queries.count(); ++i) {
        ObRawExpr *marker_expr = NULL;
        switch (marker_type) {
          case ObNumberType:
            if (OB_FAIL(num.from(i, expr_factory.get_allocator()))) {
              LOG_WARN("failed to build union all marker number from branch idx", K(ret), K(i));
            } else {
              obj.set_number(marker_type, num);
            }
            break;
          case ObIntType:
            obj.set_int(marker_type, i);
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected marker type", K(ret), K(marker_type));
            break;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObRawExprCopier::copy_expr(expr_factory, select_expr, marker_expr))) {
          LOG_WARN("failed to deep copy marker expr", K(ret));
        } else if (OB_ISNULL(marker_expr) || OB_UNLIKELY(!marker_expr->is_const_raw_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected copied marker expr", K(ret), KPC(select_expr), K(i), KPC(marker_expr));
        } else if (OB_FALSE_IT(static_cast<ObConstRawExpr*>(marker_expr)->set_value(obj))) {
        } else if (OB_FAIL(create_select_item_for_mv_stmt(set_queries.at(i),
                                                          marker_expr,
                                                          OB_INVALID_ID,
                                                          alias_name,
                                                          expr_factory))) {
          LOG_WARN("failed to create select item for mv stmt", K(ret));
        }
      }
    } else {
      // build the select item for each set branch
      ObRawExpr *null_expr = NULL;
      if (OB_UNLIKELY(sub_stmt_idx < 0 || sub_stmt_idx >= set_queries.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid sub stmt idx", K(ret), K(sub_stmt_idx), K(set_queries.count()));
      } else if (OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory, null_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < set_queries.count(); ++i) {
        if (OB_FAIL(create_select_item_for_mv_stmt(set_queries.at(i),
                                                   i == sub_stmt_idx ? select_expr : null_expr,
                                                   OB_INVALID_ID,
                                                   alias_name,
                                                   expr_factory))) {
          LOG_WARN("failed to create select item for mv stmt", K(ret), K(i), K(sub_stmt_idx), KPC(select_expr), KPC(null_expr));
        }
      }
    }
  } else {
    // build the dependent column for current stmt
    ObSEArray<ObRawExpr*, 8> column_exprs;
    SelectItem select_item;
    select_item.expr_ = select_expr;
    select_item.expr_name_ = select_expr->get_expr_name();
    select_item.alias_name_ = alias_name;
    select_item.is_real_alias_ = true;
    if (OB_FAIL(stmt->add_select_item(select_item))) {
      LOG_WARN("failed to add select item", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(select_expr, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
      ObRawExpr *expr = column_exprs.at(i);
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(expr) || OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected column expr", K(ret), K(i), KPC(column_exprs.at(i)));
      } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
      } else if (NULL != stmt->get_column_item(col_expr->get_table_id(), col_expr->get_column_id())) {
        // do nothing, column item already exists
      } else {
        ColumnItem column_item;
        column_item.expr_ = col_expr;
        column_item.table_id_ = col_expr->get_table_id();
        column_item.column_id_ = col_expr->get_column_id();
        column_item.column_name_ = col_expr->get_column_name();
        if (OB_FAIL(stmt->add_column_item(column_item))) {
          LOG_WARN("add column item to stmt failed", K(ret), K(column_item));
        }
      }
    }
    ObSEArray<ObAliasRefRawExpr *, 4> group_alias_exprs;
    if (OB_SUCC(ret) && OB_FAIL(ObTransformUtils::extract_alias_expr(stmt->get_group_exprs(), group_alias_exprs))) {
      LOG_WARN("failed to extract group by aliases", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < group_alias_exprs.count(); ++i) {
      ObAliasRefRawExpr *alias_expr = group_alias_exprs.at(i);
      if (OB_ISNULL(alias_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group by alias is unexpected null", K(ret));
      } else if (!alias_expr->is_select_alias_ref()) {
        // do nothing
      } else if (OB_ISNULL(alias_expr->get_ref_select_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select alias reference is unexpected null", K(ret));
      } else if (alias_expr->get_ref_select_expr()->same_as(*select_expr)) {
        // Checking and expansion use separate statement copies.
        alias_expr->set_select_alias_ref(alias_name);
      }
    }
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
