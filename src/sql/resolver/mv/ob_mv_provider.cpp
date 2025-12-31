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

#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/resolver/ddl/ob_create_view_resolver.h"
#include "sql/resolver/mv/ob_simple_mav_printer.h"
#include "sql/resolver/mv/ob_simple_mjv_printer.h"
#include "sql/resolver/mv/ob_simple_join_mav_printer.h"
#include "sql/resolver/mv/ob_major_refresh_mjv_printer.h"
#include "sql/resolver/mv/ob_outer_join_mjv_printer.h"
#include "sql/resolver/mv/ob_outer_join_mav_printer.h"
#include "sql/resolver/mv/ob_union_all_mv_printer.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

// 1. resolve mv definition and get stmt
// 2. check refresh type by stmt
// 3. print refresh dmls
int ObMVProvider::init_mv_provider(ObSQLSessionInfo *session_info,
                                   ObSchemaGetterGuard *schema_guard,
                                   const bool check_refreshable_only,
                                   ObMVPrinterRefreshInfo *refresh_info,
                                   ObTableReferencedColumnsInfo *table_referenced_columns_info)
{
  int ret = OB_SUCCESS;
  dependency_infos_.reuse();
  tables_need_mlog_.reuse();
  operators_.reuse();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mv provider is inited twice", K(ret));
  } else if (OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(session_info));
  } else if (check_refreshable_only && OB_NOT_NULL(refresh_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("refresh info should be null when check refreshable only", K(ret), K(refresh_info));
  } else {
    lib::ContextParam param;
    const uint64_t tenant_id = session_info->get_effective_tenant_id();
    ObSEArray<std::pair<ObRawExpr*, int64_t>, 8> fast_refresh_dependent_columns;
    param.set_mem_attr(tenant_id, "MVProvider", ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL)
         .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    CREATE_WITH_TEMP_CONTEXT(param) {
      ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
      ObSelectStmt *view_stmt = NULL;
      ObDMLStmt *trans_stmt = NULL;
      ObStmtFactory stmt_factory(alloc);
      ObRawExprFactory expr_factory(alloc);
      ObSchemaChecker schema_checker;
      const ObTableSchema *mv_schema = NULL;
      const ObTableSchema *mv_container_schema = NULL;
      ObQueryCtx *query_ctx = NULL;
      int64_t max_version = OB_INVALID_VERSION;
      ObSEArray<ObString, 4> operators;
      ObSEArray<ObDependencyInfo, 4> dependency_infos;
      ObSEArray<uint64_t, 4> tables_need_mlog;
      bool is_rt_expand = false;
      SMART_VARS_2((ObExecContext, exec_ctx, alloc), (ObPhysicalPlanCtx, phy_plan_ctx, alloc)) {
        LinkExecCtxGuard link_guard(*session_info, exec_ctx);
        // add empty sql ctx
        ObSqlCtx empty_ctx;
        empty_ctx.schema_guard_ = schema_guard;
        exec_ctx.set_sql_ctx(&empty_ctx);
        exec_ctx.set_my_session(session_info);
        exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
        ObMVPrinterCtx mv_printer_ctx(alloc,
                                      *session_info,
                                      stmt_factory,
                                      expr_factory,
                                      refresh_info);
        bool is_vars_matched = false;
        if (OB_ISNULL(query_ctx = stmt_factory.get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(query_ctx));
        } else if (OB_FALSE_IT(query_ctx->sql_schema_guard_.set_schema_guard(schema_guard))) {
        } else if (OB_FALSE_IT(expr_factory.set_query_ctx(query_ctx))) {
        } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_, (session_info->get_session_type() != ObSQLSessionInfo::INNER_SESSION
                                                                              ? session_info->get_sessid_for_table() : OB_INVALID_ID)))) {
          LOG_WARN("init schema checker failed", K(ret));
        } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(mview_id_, mv_schema))
                   || OB_ISNULL(mv_schema) || OB_UNLIKELY(!mv_schema->is_materialized_view())) {
          COVER_SUCC(OB_ERR_UNEXPECTED);
          LOG_WARN("unexpected mv schema", K(ret), KPC(mv_schema));
        } else if (OB_FAIL(query_ctx->sql_schema_guard_.get_table_schema(mv_schema->get_data_table_id(), mv_container_schema))
                   || OB_ISNULL(mv_container_schema)) {
          COVER_SUCC(OB_ERR_UNEXPECTED);
          LOG_WARN("fail to get mv container schema", KR(ret), K(mv_schema->get_data_table_id()), K(mv_container_schema));
        } else if (OB_FAIL(check_mview_dep_session_vars(*mv_schema, *session_info, true, is_vars_matched))) {
          LOG_WARN("failed to check mview dep session vars", K(ret));
        } else if (OB_FAIL(generate_mv_stmt(alloc,
                                            stmt_factory,
                                            expr_factory,
                                            schema_checker,
                                            *session_info,
                                            *mv_schema,
                                            view_stmt))) {
          LOG_WARN("failed to gen mv stmt", K(ret));
        } else if (OB_FALSE_IT(trans_stmt = view_stmt)) {
        } else if (OB_FAIL(transform_mv_def_stmt(trans_stmt,
                                                 &alloc,
                                                 &schema_checker,
                                                 session_info,
                                                 &expr_factory,
                                                 &stmt_factory))) {
          LOG_WARN("failed to transform mv stmt", K(ret));
        } else if (OB_ISNULL(view_stmt = static_cast<ObSelectStmt *>(trans_stmt))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null stmt", K(ret));
        } else if (OB_FAIL(ObDependencyInfo::collect_dep_infos(query_ctx->reference_obj_tables_,
                                                               dependency_infos,
                                                               ObObjectType::VIEW,
                                                               OB_INVALID_ID,
                                                               max_version))) {
          LOG_WARN("failed to collect dep infos", K(ret));
        } else if (OB_FAIL(dependency_infos_.assign(dependency_infos))) {
          LOG_WARN("failed to assign fixed array", K(ret));
        } else if (OB_FAIL(collect_tables_need_mlog(view_stmt, tables_need_mlog))) {
          LOG_WARN("failed to collect tables need mlog", K(ret));
        } else if (OB_FAIL(tables_need_mlog_.assign(tables_need_mlog))) {
          LOG_WARN("failed to assign tables need mlog", K(ret));
        } else if (OB_FAIL(check_mv_column_type(mv_container_schema, view_stmt, *session_info))) {
          if (OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH == ret) {
            inited_ = true;
            refreshable_type_ = OB_MV_REFRESH_INVALID;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to check mv column type", K(ret));
          }
        } else if (OB_FAIL(check_is_rt_expand(check_refreshable_only, *mv_schema, mv_printer_ctx, is_rt_expand))) {
          LOG_WARN("failed to check is rt expand", K(ret));
        } else {
          ObMVChecker checker(*view_stmt,
                              expr_factory,
                              session_info,
                              *mv_container_schema,
                              is_rt_expand,
                              fast_refreshable_note_,
                              fast_refresh_dependent_columns,
                              table_referenced_columns_info);
          if (OB_FAIL(ObMVChecker::pre_process_view_stmt(&expr_factory, session_info, checker.get_gen_cols(), *view_stmt))) {
            LOG_WARN("failed to pre process view stmt", K(ret));
          } else if (OB_FAIL(checker.check_mv_refresh_type())) {
            LOG_WARN("failed to check mv refresh type", K(ret));
          } else if (!IS_VALID_FAST_REFRESH_TYPE(refreshable_type_ = checker.get_refersh_type())) {
            LOG_TRACE("mv not support fast refresh", K_(refreshable_type), K(mv_schema->get_table_name()));
          } else if (!fast_refresh_dependent_columns.empty()) {
            refreshable_type_ = OB_MV_COMPLETE_REFRESH;
            fast_refreshable_note_.error_.assign_fmt("missing fast refresh dependent columns in the SELECT list");
            LOG_TRACE("mv not support fast refresh, need additional dependent columns", K(mv_schema->get_table_name()), K(fast_refresh_dependent_columns));
          } else if (check_refreshable_only) {
            inited_ = true;
          } else if (OB_FALSE_IT(query_ctx->get_query_hint_for_update().reset())) {
          } else if (OB_FAIL(print_mv_operators(mv_printer_ctx,
                                                checker,
                                                *mv_schema,
                                                *mv_container_schema,
                                                *view_stmt,
                                                operators))) {
            LOG_WARN("failed to print mv operators", K(ret));
          } else if (OB_FAIL(operators_.assign(operators))) {
            LOG_WARN("failed to assign fixed array", K(ret));
          } else {
            inited_ = true;
          }
        }
        exec_ctx.set_physical_plan_ctx(NULL);
      }
    }
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
      // do nothing
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

int ObMVProvider::print_mv_operators(ObMVPrinterCtx &mv_printer_ctx,
                                     ObMVChecker &checker,
                                     const ObTableSchema &mv_schema,
                                     const ObTableSchema &mv_container_schema,
                                     const ObSelectStmt &mv_def_stmt,
                                     ObIArray<ObString> &operators)
{
  int ret = OB_SUCCESS;
  operators.reuse();
  switch (refreshable_type_) {
    case OB_MV_FAST_REFRESH_SIMPLE_MAV: {
      ObSimpleMAVPrinter printer(mv_printer_ctx,
                                 mv_schema,
                                 mv_container_schema,
                                 mv_def_stmt,
                                 checker.get_mlog_tables(),
                                 checker.get_expand_aggrs());
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print simple mav operator stmts", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_SIMPLE_MJV: {
      ObSimpleMJVPrinter printer(mv_printer_ctx, mv_schema, mv_container_schema, mv_def_stmt, checker.get_mlog_tables());
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print simple mjv operator stmts", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_SIMPLE_JOIN_MAV: {
      ObSimpleJoinMAVPrinter printer(mv_printer_ctx,
                                     mv_schema,
                                     mv_container_schema,
                                     mv_def_stmt,
                                     checker.get_mlog_tables(),
                                     checker.get_expand_aggrs());
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print simple join mav operator stmts", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV: {
      ObMajorRefreshMJVPrinter printer(mv_printer_ctx, mv_schema, mv_container_schema, mv_def_stmt);
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print major refresh mjv operator stmts", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_OUTER_JOIN_MJV: {
      ObOuterJoinMJVPrinter printer(mv_printer_ctx, mv_schema, mv_container_schema, mv_def_stmt, checker.get_mlog_tables());
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print simple outer join mjv operator stmts", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_UNION_ALL: {
      ObUnionAllMVPrinter printer(mv_printer_ctx, mv_schema, mv_container_schema,
                                  mv_def_stmt,
                                  checker.get_union_all_marker_idx(),
                                  checker.get_child_refresh_types(),
                                  checker.get_mlog_tables(),
                                  checker.get_expand_aggrs());
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print union all operator stmts", K(ret));
      }
      break;
    }
    case OB_MV_FAST_REFRESH_OUTER_JOIN_MAV: {
      ObOuterJoinMAVPrinter printer(mv_printer_ctx,
                                    mv_schema,
                                    mv_container_schema,
                                    mv_def_stmt,
                                    checker.get_mlog_tables(),
                                    checker.get_expand_aggrs());
      if (OB_FAIL(printer.print_mv_operators(inner_alloc_, operators))) {
        LOG_WARN("failed to print outer join mav operator stmts", K(ret));
      }
      break;
    }
    default:  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected refresh type", K(ret), K(refreshable_type_));
      break;
    }
  }
  return ret;
}

int ObMVProvider::check_mv_refreshable(const uint64_t tenant_id,
                                       const uint64_t mview_id,
                                       ObSQLSessionInfo *session_info,
                                       ObSchemaGetterGuard *schema_guard,
                                       bool &can_fast_refresh,
                                       FastRefreshableNotes &note)
{
  int ret = OB_SUCCESS;
  can_fast_refresh = false;
  ObMVProvider mv_provider(tenant_id, mview_id);
  if (OB_FAIL(mv_provider.init_mv_provider(session_info, schema_guard, true /*check_refreshable_only*/, NULL /*refresh_info*/))) {
    LOG_WARN("fail to init mv provider", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_REFRESH_INVALID == mv_provider.refreshable_type_)) {
    // column type for mv is changed after it is created
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not refresh mv", K(ret), K(mv_provider.refreshable_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh mv after column types change is");
  } else if (OB_FAIL(note.error_.assign(mv_provider.fast_refreshable_note_.error_))) {
    LOG_WARN("fail to assign ObSqlString", K(ret));
  } else if (ObMVRefreshableType::OB_MV_COMPLETE_REFRESH == mv_provider.refreshable_type_) {
    can_fast_refresh = false;
  } else {
    can_fast_refresh = true;
  }
  return ret;
}

int ObMVProvider::get_mlog_mv_refresh_infos(ObSQLSessionInfo *session_info,
                                            ObSchemaGetterGuard *schema_guard,
                                            const share::SCN &last_refresh_scn,
                                            const share::SCN &refresh_scn,
                                            const share::SCN *mv_last_refresh_scn,
                                            const share::SCN *mv_refresh_scn,
                                            ObIArray<ObDependencyInfo> &dep_infos,
                                            ObIArray<uint64_t> &tables_need_mlog,
                                            bool &can_fast_refresh,
                                            const ObIArray<ObString> *&operators)
{
  int ret = OB_SUCCESS;
  dep_infos.reuse();
  operators = NULL;
  ObMVPrinterRefreshInfo refresh_info(last_refresh_scn, refresh_scn);
  refresh_info.mv_last_refresh_scn_ = mv_last_refresh_scn;
  refresh_info.mv_refresh_scn_ = mv_refresh_scn;
  if (OB_FAIL(init_mv_provider(session_info, schema_guard, false /*check_refreshable_only*/, &refresh_info))) {
    LOG_WARN("failed to init mv provider", K(ret));
  } else if (OB_FAIL(dep_infos.assign(dependency_infos_))) {
    LOG_WARN("failed to assign dependency_infos", K(ret));
  } else if (OB_FAIL(tables_need_mlog.assign(tables_need_mlog_))) {
    LOG_WARN("failed to assign tables need mlog", K(ret));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_REFRESH_INVALID == refreshable_type_)) {
    // column type for mv is changed after it is created
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not refresh mv", K(ret), K(refreshable_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh mv after column types change is");
  } else if (ObMVRefreshableType::OB_MV_COMPLETE_REFRESH == refreshable_type_) {
    can_fast_refresh = false;
  } else if (OB_UNLIKELY(operators_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else {
    can_fast_refresh = true;
    operators = &operators_;
  }
  return ret;
}

int ObMVProvider::get_major_refresh_operators(ObSQLSessionInfo *session_info,
                                              ObSchemaGetterGuard *schema_guard,
                                              const share::SCN &last_refresh_scn,
                                              const share::SCN &refresh_scn,
                                              const int64_t part_idx,
                                              const int64_t sub_part_idx,
                                              const ObNewRange &range,
                                              const ObIArray<ObString> *&operators)
{
  int ret = OB_SUCCESS;
  ObMVPrinterRefreshInfo refresh_info(last_refresh_scn, refresh_scn, part_idx, sub_part_idx, &range);
  if (OB_FAIL(init_mv_provider(session_info, schema_guard, false /*check_refreshable_only*/, &refresh_info))) {
    LOG_WARN("failed to init mv provider", K(ret));
  } else if (OB_UNLIKELY(operators_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else {
    operators = &operators_;
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
                                               bool &is_major_refresh_mview)
{
  int ret = OB_SUCCESS;
  expand_view.reset();
  is_major_refresh_mview = false;
  ObMVProvider mv_provider(tenant_id, mview_id);
  if (OB_FAIL(mv_provider.init_mv_provider(session_info, schema_guard, false /*check_refreshable_only*/, NULL /*refresh_info*/))) {
    LOG_WARN("failed to init mv provider", K(ret));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_COMPLETE_REFRESH >= mv_provider.refreshable_type_)) {
    ret = OB_ERR_MVIEW_CAN_NOT_ON_QUERY_COMPUTE;
    LOG_WARN("mview can not on query computation", K(ret), K(mview_id));
  } else if (OB_UNLIKELY(1 != mv_provider.operators_.count() || mv_provider.operators_.at(0).empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, mv_provider.operators_.at(0), expand_view))) {
    LOG_WARN("failed to write string", K(ret));
  } else {
    is_major_refresh_mview = OB_MV_FAST_REFRESH_MAJOR_REFRESH_MJV == mv_provider.refreshable_type_;
    LOG_TRACE("finish generate rt mv expand view", K(mview_id), K(is_major_refresh_mview), K(expand_view));
  }
  return ret;
}

// check mv can refresh.
// if the result type from mv_schema and view_stmt is different, no refresh method is allowed
// get new column info same as ObCreateViewResolver::add_column_infos
int ObMVProvider::check_mv_column_type(const ObTableSchema *mv_schema,
                                       const ObSelectStmt *view_stmt,
                                       ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mv_schema) || OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(mv_schema), K(view_stmt));
  } else {
    const ObIArray<SelectItem> &select_items = view_stmt->get_select_items();
    const ObColumnSchemaV2 *org_column = NULL;
    ObColumnSchemaV2 cur_column;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      cur_column.reset();
      if (OB_ISNULL(org_column = mv_schema->get_column_schema(i + OB_APP_MIN_COLUMN_ID))
          || OB_ISNULL(select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(org_column), K(select_items.at(i)));
      } else if (select_items.at(i).expr_->is_const_expr()) {
        /* do nothing */
      } else if (OB_FAIL(ObCreateViewResolver::fill_column_meta_infos(*select_items.at(i).expr_,
                                                                      mv_schema->get_charset_type(),
                                                                      mv_schema->get_table_id(),
                                                                      session,
                                                                      cur_column))) {
        LOG_WARN("failed to fill column meta infos", K(ret), K(cur_column));
      } else if (OB_FAIL(check_mv_column_type(*org_column, cur_column))) {
        LOG_WARN("mv column changed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObMVProvider::check_mv_column_type(const ObColumnSchemaV2 &org_column,
                                       const ObColumnSchemaV2 &cur_column)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObString> &orig_strs = org_column.get_extended_type_info();
  const ObIArray<ObString> &cur_strs = cur_column.get_extended_type_info();
  bool is_valid_col = // org_column.get_meta_type() == cur_column.get_meta_type()
                     org_column.get_sub_data_type() == cur_column.get_sub_data_type()
                     // && org_column.get_charset_type() == cur_column.get_charset_type() todo: org_column charset_type is invalid now
                     && org_column.is_zero_fill() == cur_column.is_zero_fill()
                     && orig_strs.count() == cur_strs.count();
  if (is_valid_col && OB_FAIL(check_column_type_and_accuracy(org_column, cur_column, is_valid_col))) {
    LOG_WARN("failed to check column type and accuracy", K(ret));
  }
  for (int64_t i = 0; is_valid_col && OB_SUCC(ret) && i < orig_strs.count(); ++i) {
   if (orig_strs.at(i) == cur_strs.at(i)) {
     /* do nothing */
   } else {
     is_valid_col = false;
     LOG_WARN("enum or set column in mv changed", K(orig_strs), K(cur_strs));
   }
  }

  if (OB_SUCC(ret) && !is_valid_col) {
   ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
   LOG_WARN("mv column changed", K(ret), K(org_column), K(cur_column));
  }
  return ret;
}

int ObMVProvider::check_column_type_and_accuracy(const ObColumnSchemaV2 &org_column,
                                                 const ObColumnSchemaV2 &cur_column,
                                                 bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (org_column.get_meta_type().is_number()
      && cur_column.get_meta_type().is_decimal_int()
      && lib::is_oracle_mode()) {
    /* in oracle mode, const number is resolved as decimal int except ddl stmt */
    is_match = true;
  } else if (org_column.get_meta_type().get_type() != cur_column.get_meta_type().get_type()) {
    is_match = false;
  } else if (ob_is_string_type(org_column.get_meta_type().get_type())) {
    is_match = org_column.get_accuracy().get_length() >= cur_column.get_accuracy().get_length();
  } else if (ob_is_numeric_type(org_column.get_meta_type().get_type())) {
    // only check scale for number
    // check scale and length for decimal int
    // not need to check precision here
    is_match = true;
    const ObAccuracy &org = org_column.get_accuracy();
    const ObAccuracy &cur = cur_column.get_accuracy();
    is_match &= (-1 == org.get_scale() || org.get_scale() >= cur.get_scale());
    is_match &= (cur_column.get_meta_type().is_number() || -1 == org.get_length() || org.get_length() >= cur.get_length());
  } else {
    // for columns neither string nor numeric, only check the type
    is_match = true;
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
    LOG_WARN("resolve view definition failed", K(ret));
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
                                                 ObSQLSessionInfo &session_info,
                                                 ObSchemaGetterGuard &schema_guard,
                                                 const share::SCN *mv_refresh_scn,
                                                 const share::SCN *table_refresh_scn,
                                                 ObIAllocator &str_alloc,
                                                 ObString &mview_str)
{
  int ret = OB_SUCCESS;
  mview_str.reset();
  lib::ContextParam param;
  param.set_mem_attr(session_info.get_effective_tenant_id(), "MVProvider", ObCtxIds::DEFAULT_CTX_ID)
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
      LinkExecCtxGuard link_guard(session_info, exec_ctx);
      exec_ctx.set_my_session(&session_info);
      exec_ctx.set_physical_plan_ctx(&phy_plan_ctx);
      if (OB_ISNULL(query_ctx = stmt_factory.get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(query_ctx));
      } else if (OB_FALSE_IT(query_ctx->sql_schema_guard_.set_schema_guard(&schema_guard))) {
      } else if (OB_FALSE_IT(expr_factory.set_query_ctx(query_ctx))) {
      } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_, (session_info.get_session_type() != ObSQLSessionInfo::INNER_SESSION
                                                                            ? session_info.get_sessid_for_table() : OB_INVALID_ID)))) {
        LOG_WARN("init schema checker failed", K(ret));
      } else if (OB_FAIL(generate_mv_stmt(alloc,
                                          stmt_factory,
                                          expr_factory,
                                          schema_checker,
                                          session_info,
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
      }
      exec_ctx.set_physical_plan_ctx(NULL);
    }
  }
  return ret;
}

int ObMVProvider::transform_mv_def_stmt(ObDMLStmt *&mv_def_stmt,
                                        ObIAllocator *allocator,
                                        ObSchemaChecker *schema_checker,
                                        ObSQLSessionInfo *session_info,
                                        ObRawExprFactory *expr_factory,
                                        ObStmtFactory *stmt_factory)
{
  int ret = OB_SUCCESS;
  uint64_t rule_set = 0;
  bool trans_happened = false;
  if (OB_ISNULL(mv_def_stmt) || OB_ISNULL(allocator) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_def_stmt), K(allocator), K(session_info));
  } else if (OB_FAIL(get_trans_rule_set(mv_def_stmt, rule_set))) {
    LOG_WARN("failed to get trans rule set", K(ret), KPC(mv_def_stmt));
  } else if (0 == rule_set) {
    // do nothing
  } else {
    ObTransformerCtx trans_ctx;
    ObTransformerImpl transformer(&trans_ctx);
    trans_ctx.allocator_ = allocator;
    trans_ctx.schema_checker_ = schema_checker;
    trans_ctx.session_info_ = session_info;
    trans_ctx.exec_ctx_ = session_info->get_cur_exec_ctx();
    trans_ctx.expr_factory_ = expr_factory;
    trans_ctx.stmt_factory_ = stmt_factory;
    if (OB_FAIL(ObTransformUtils::right_join_to_left(mv_def_stmt))) {
      LOG_WARN("failed to transform right join to left", K(ret));
    } else if (OB_FAIL(transformer.transform_rule_set(mv_def_stmt, rule_set, 1, trans_happened))) {
      LOG_WARN("failed to transform mv def stmt", K(ret));
    }
  }
  return ret;
}

int ObMVProvider::get_trans_rule_set(const ObDMLStmt *mv_def_stmt,
                                     uint64_t &rule_set)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  ObSEArray<const JoinedTable*, 8> joined_table;
  if (OB_ISNULL(mv_def_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_def_stmt));
  } else if (OB_FAIL(mv_def_stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret), KPC(mv_def_stmt));
  } else if (OB_FAIL(append(joined_table, mv_def_stmt->get_joined_tables()))) {
    LOG_WARN("failed to append joined tables", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(get_trans_rule_set(child_stmts.at(i), rule_set)))) {
      LOG_WARN("failed to get trans rule set", K(ret), KPC(child_stmts.at(i)));
    }
  }
  // check whether need ELIMINATE_OJ
  while (OB_SUCC(ret) && (0 == (rule_set & 1L << ELIMINATE_OJ)) && !joined_table.empty()) {
    const JoinedTable* table = NULL;
    if (OB_FAIL(joined_table.pop_back(table))) {
      LOG_WARN("failed to pop back joined table", K(ret));
    } else if (OB_ISNULL(table) || OB_ISNULL(table->left_table_) || OB_ISNULL(table->right_table_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), KPC(table));
    } else if (!table->is_inner_join()) {
      rule_set |= 1L << ELIMINATE_OJ;
    } else if (table->left_table_->is_joined_table()
               && OB_FAIL(joined_table.push_back(static_cast<const JoinedTable*>(table->left_table_)))) {
      LOG_WARN("failed to append left joined tables", K(ret));
    } else if (table->right_table_->is_joined_table()
               && OB_FAIL(joined_table.push_back(static_cast<const JoinedTable*>(table->right_table_)))) {
      LOG_WARN("failed to append right joined tables", K(ret));
    }
  }
  return ret;
}

int ObMVProvider::get_columns_referenced_by_mv(const uint64_t tenant_id,
                                               const uint64_t mview_id,
                                               const uint64_t table_id,
                                               ObSQLSessionInfo *session_info,
                                               ObSchemaGetterGuard *schema_guard,
                                               common::hash::ObHashSet<uint64_t> &table_referenced_columns)
{
  int ret = OB_SUCCESS;
  ObTableReferencedColumnsInfo table_referenced_columns_info;
  if (OB_FAIL(table_referenced_columns_info.init())) {
    LOG_WARN("failed to init table referenced columns info", KR(ret));
  } else if (OB_FAIL(init_mv_provider(session_info, schema_guard, true /*check_refreshable_only*/, NULL /*refresh_info*/, &table_referenced_columns_info))) {
    LOG_WARN("failed to init mv provider", KR(ret));
  } else if (OB_FAIL(table_referenced_columns_info.append_to_table_referenced_columns(table_id, table_referenced_columns))) {
    LOG_WARN("failed to append to table referenced columns", KR(ret));
  }

  return ret;
}

int ObMVProvider::check_is_rt_expand(const bool check_refreshable_only,
                                     const ObTableSchema &mv_schema,
                                     const ObMVPrinterCtx &mv_printer_ctx,
                                     bool &is_rt_expand)
{
  int ret = OB_SUCCESS;
  is_rt_expand = false;

  // the result of this function `is_rt_expand` is passed to mv checker to decide the refresh type of a mview.
  // if the mv provider is only used to check fast refreshable, then we should pass the on query computation definition of the mview, just like the resolver does.
  // otherwise, `is_rt_expand` is decided by the actual usage of the mv provider:
  //   if the mv provider is used to print the rt expand sqls, then `is_rt_expand` is true
  //   otherwise, `is_rt_expand` is false.

  if (!check_refreshable_only) {
    if (mv_printer_ctx.for_rt_expand() && !mv_schema.mv_on_query_computation()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("need to print rt expand sqls, but the mview is not defined with on query computation", KR(ret));
    } else {
      is_rt_expand = mv_printer_ctx.for_rt_expand();
    }
  } else {
    is_rt_expand = mv_schema.mv_on_query_computation();
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
  if (OB_ISNULL(stmt) || OB_UNLIKELY(OB_INVALID_ID != sub_stmt_idx && NULL == select_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(sub_stmt_idx), K(select_expr));
  } else if (OB_INVALID_ID != sub_stmt_idx) {
    const ObIArray<ObSelectStmt*> &set_queries = stmt->get_set_query();
    if (OB_UNLIKELY(sub_stmt_idx < 0 || sub_stmt_idx >= set_queries.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sub stmt idx", K(ret), K(sub_stmt_idx), K(set_queries.count()));
    } else if (OB_FAIL(create_select_item_for_mv_stmt(set_queries.at(sub_stmt_idx),
                                                      select_expr,
                                                      OB_INVALID_ID,
                                                      alias_name,
                                                      expr_factory))) {
      LOG_WARN("failed to create select item for mv stmt", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < set_queries.count(); ++i) {
      if (i == sub_stmt_idx) {
        // do nothing
      } else if (OB_FAIL(create_select_item_for_mv_stmt(set_queries.at(i),
                                                        NULL,
                                                        OB_INVALID_ID,
                                                        alias_name,
                                                        expr_factory))) {
        LOG_WARN("failed to create select item for mv stmt", K(ret));
      }
    }
  } else if (NULL == select_expr
             && OB_FAIL(ObRawExprUtils::build_null_expr(expr_factory, select_expr))) {
    LOG_WARN("failed to build null expr", K(ret));
  } else {
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
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
