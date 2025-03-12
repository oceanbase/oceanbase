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

namespace oceanbase
{
using namespace common;
namespace sql
{

// 1. resolve mv definition and get stmt
// 2. check refresh type by stmt
// 3. print refresh dmls
int ObMVProvider::init_mv_provider(const share::SCN &last_refresh_scn,
                                   const share::SCN &refresh_scn,
                                   ObSchemaGetterGuard *schema_guard,
                                   ObSQLSessionInfo *session_info,
                                   int64_t part_idx,
                                   int64_t sub_part_idx,
                                   ObNewRange &range)
{
  int ret = OB_SUCCESS;
  MajorRefreshInfo major_refresh_info(part_idx, sub_part_idx, range);
  major_refresh_info_ = major_refresh_info.is_valid_info() ? &major_refresh_info : NULL;
  FastRefreshableNotes note;
  if (OB_FAIL(init_mv_provider(last_refresh_scn, refresh_scn, schema_guard, session_info, note))) {
    LOG_WARN("Failed to init mv provider", K(ret), K(major_refresh_info));
  }
  major_refresh_info_ = NULL;
  return ret;
}

// 1. resolve mv definition and get stmt
// 2. check refresh type by stmt
// 3. print refresh dmls
int ObMVProvider::init_mv_provider(const share::SCN &last_refresh_scn,
                                   const share::SCN &refresh_scn,
                                   ObSchemaGetterGuard *schema_guard,
                                   ObSQLSessionInfo *session_info,
                                   FastRefreshableNotes &fast_refreshable_note)
{
  int ret = OB_SUCCESS;
  dependency_infos_.reuse();
  operators_.reuse();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("mv provider is inited twice", K(ret));
  } else if (OB_ISNULL(schema_guard) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(schema_guard), K(session_info));
  } else {
    lib::ContextParam param;
    param.set_mem_attr(session_info->get_effective_tenant_id(), "MVProvider", ObCtxIds::DEFAULT_CTX_ID)
         .set_properties(lib::USE_TL_PAGE_OPTIONAL)
         .set_page_size(OB_MALLOC_NORMAL_BLOCK_SIZE);
    CREATE_WITH_TEMP_CONTEXT(param) {
      ObIAllocator &alloc = CURRENT_CONTEXT->get_arena_allocator();
      const ObSelectStmt *view_stmt = NULL;
      ObStmtFactory stmt_factory(alloc);
      ObRawExprFactory expr_factory(alloc);
      ObSchemaChecker schema_checker;
      const ObTableSchema *mv_schema = NULL;
      const ObTableSchema *mv_container_schema = NULL;
      ObQueryCtx *query_ctx = NULL;
      int64_t max_version = OB_INVALID_VERSION;
      ObSEArray<ObString, 4> operators;
      ObSEArray<ObDependencyInfo, 4> dependency_infos;
      if (OB_ISNULL(query_ctx = stmt_factory.get_query_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(query_ctx));
      } else if (OB_FALSE_IT(query_ctx->sql_schema_guard_.set_schema_guard(schema_guard))) {
      } else if (OB_FAIL(schema_checker.init(query_ctx->sql_schema_guard_, (session_info->get_session_type() != ObSQLSessionInfo::INNER_SESSION
                                                                            ? session_info->get_sessid_for_table() : OB_INVALID_ID)))) {
        LOG_WARN("init schema checker failed", K(ret));
      } else if (OB_FAIL(generate_mv_stmt(alloc,
                                          stmt_factory,
                                          expr_factory,
                                          schema_checker,
                                          *session_info,
                                          mview_id_,
                                          mv_schema,
                                          mv_container_schema,
                                          view_stmt))) {
        LOG_WARN("failed to gen mv stmt", K(ret));
      } else if (OB_FAIL(ObDependencyInfo::collect_dep_infos(query_ctx->reference_obj_tables_,
                                                             dependency_infos,
                                                             ObObjectType::VIEW,
                                                             OB_INVALID_ID,
                                                             max_version))) {
        LOG_WARN("failed to collect dep infos", K(ret));
      } else if (OB_FAIL(dependency_infos_.assign(dependency_infos))) {
        LOG_WARN("failed to assign fixed array", K(ret));
      } else if (OB_FAIL(check_mv_column_type(mv_schema, view_stmt, *session_info))) {
        if (OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH == ret) {
          inited_ = true;
          refreshable_type_ = OB_MV_REFRESH_INVALID;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to check mv column type", K(ret));
        }
      } else if (OB_FALSE_IT(query_ctx->get_query_hint_for_update().reset())) { // reset hint from mview definition
      } else if (OB_FAIL(ObMVPrinter::print_mv_operators(*mv_schema, *mv_container_schema,
                                                         *view_stmt, for_rt_expand_,
                                                         last_refresh_scn, refresh_scn, major_refresh_info_,
                                                         alloc, inner_alloc_,
                                                         schema_guard,
                                                         stmt_factory,
                                                         expr_factory,
                                                         session_info,
                                                         operators,
                                                         refreshable_type_,
                                                         fast_refreshable_note))) {
        LOG_WARN("failed to print mv operators", K(ret));
      } else if (OB_FAIL(operators_.assign(operators))) {
        LOG_WARN("failed to assign fixed array", K(ret));
      } else {
        inited_ = true;
      }
    }
  }
  return ret;
}

int ObMVProvider::check_mv_refreshable(bool &can_fast_refresh) const
{
  int ret = OB_SUCCESS;
  can_fast_refresh = false;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObMVProvider is not init", K(ret));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_REFRESH_INVALID == refreshable_type_)) {
    // column type for mv is changed after it is created
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not refresh mv", K(ret), K(refreshable_type_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "refresh mv after column types change is");
  } else if (ObMVRefreshableType::OB_MV_COMPLETE_REFRESH == refreshable_type_) {
    can_fast_refresh = false;
  } else {
    can_fast_refresh = true;
  }
  return ret;
}

int ObMVProvider::get_fast_refresh_operators(const ObIArray<ObString> *&operators) const
{
  int ret = OB_SUCCESS;
  operators = NULL;
  if (OB_UNLIKELY(!inited_ || for_rt_expand_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call for get_operators", K(ret), K(inited_), K(for_rt_expand_));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_COMPLETE_REFRESH >= refreshable_type_)) {
    ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
    LOG_WARN("mview can not fast refresh", K(ret), K(mview_id_));
  } else if (OB_UNLIKELY(operators_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else {
    operators = &operators_;
  }
  return ret;
}

// expand_view will used to generate plan, need use alloc to deep copy the query str
int ObMVProvider::get_real_time_mv_expand_view(ObIAllocator &alloc, ObString &expand_view) const
{
  int ret = OB_SUCCESS;
  expand_view.reset();
  if (OB_UNLIKELY(!inited_ || !for_rt_expand_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call for get_operators", K(ret), K(inited_), K(for_rt_expand_));
  } else if (OB_UNLIKELY(ObMVRefreshableType::OB_MV_COMPLETE_REFRESH >= refreshable_type_)) {
    ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
    LOG_WARN("mview can not on query computation", K(ret), K(mview_id_));
  } else if (OB_UNLIKELY(1 != operators_.count() || operators_.at(0).empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else if (OB_FAIL(ob_write_string(alloc, operators_.at(0), expand_view))) {
    LOG_WARN("failed to write string", K(ret));
  } else {
    LOG_TRACE("finish generate rt mv expand view", K(mview_id_), K(expand_view));
  }
  return ret;
}

int ObMVProvider::get_mv_dependency_infos(ObIArray<ObDependencyInfo> &dep_infos) const
{
  int ret = OB_SUCCESS;
  dep_infos.reuse();
  if (OB_UNLIKELY(!inited_ || dependency_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call for get_operators", K(ret), K(inited_), K(dependency_infos_.empty()));
  } else if (OB_FAIL(dep_infos.assign(dependency_infos_))) {
    LOG_WARN("failed to assign dependency_infos", K(ret));
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
                                   const uint64_t mv_id,
                                   const ObTableSchema *&mv_schema,
                                   const ObTableSchema *&mv_container_schema,
                                   const ObSelectStmt *&view_stmt)
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  mv_schema = NULL;
  mv_container_schema = NULL;
  uint64_t mv_container_id = 0;
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
  ObSelectStmt *sel_stmt = NULL;
  ObSelectResolver select_resolver(resolver_ctx);
  bool is_vars_matched = false;
  if (OB_ISNULL(resolver_ctx.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(resolver_ctx.query_ctx_));
  } else if (OB_FAIL(resolver_ctx.query_ctx_->sql_schema_guard_.get_table_schema(mv_id, mv_schema))) {
    LOG_WARN("fail to get mv schema", K(ret), K(mv_id));
  } else if (OB_ISNULL(mv_schema) || OB_UNLIKELY(!mv_schema->is_materialized_view())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mv schema", K(ret), KPC(mv_schema));
    mv_schema = NULL;
  } else if (FALSE_IT(mv_container_id = mv_schema->get_data_table_id())) {

  } else if (OB_FAIL(resolver_ctx.query_ctx_->sql_schema_guard_.get_table_schema(mv_container_id,
                                                                                 mv_container_schema))) {
    LOG_WARN("fail to get mv container schema", KR(ret), K(mv_container_id));
  } else if (OB_ISNULL(mv_container_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("unexpected null", KR(ret), K(mv_container_schema));
  } else if (OB_FAIL(check_mview_dep_session_vars(*mv_schema, session_info, true, is_vars_matched))) {
    LOG_WARN("failed to check mview dep session vars", K(ret));
  } else if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(alloc,
                                                                session_info.get_local_collation_connection(),
                                                                mv_schema->get_view_schema(),
                                                                view_definition))) {
    LOG_WARN("fail to generate view definition for resolve", K(ret));
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
  } else if (OB_FAIL(sel_stmt->formalize_stmt_expr_reference(&expr_factory, &session_info, true))) {
    LOG_WARN("failed to formalize stmt reference", K(ret));
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

}//end of namespace sql
}//end of namespace oceanbase
