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
#include "common/ob_smart_call.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/mv/ob_mv_printer.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"
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
                                   ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  dependency_infos_.reuse();
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
      ObQueryCtx *query_ctx = NULL;
      int64_t max_version = OB_INVALID_VERSION;
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
                                          view_stmt))) {
        LOG_WARN("failed to gen mv stmt", K(ret));
      } else if (OB_FAIL(ObDependencyInfo::collect_dep_infos(query_ctx->reference_obj_tables_,
                                                             dependency_infos_,
                                                             ObObjectType::VIEW,
                                                             OB_INVALID_ID,
                                                             max_version))) {
        LOG_WARN("failed to collect dep infos", K(ret));
      } else if (OB_FAIL(check_mv_column_type(mv_schema, view_stmt))) {
        if (OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH == ret) {
          inited_ = true;
          refreshable_type_ = OB_MV_REFRESH_INVALID;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to check mv column type", K(ret));
        }
      } else if (OB_FAIL(ObMVPrinter::print_mv_operators(*mv_schema, *view_stmt, for_rt_expand_,
                                                         last_refresh_scn, refresh_scn,
                                                         alloc, inner_alloc_,
                                                         schema_guard,
                                                         stmt_factory,
                                                         expr_factory,
                                                         session_info,
                                                         operators_,
                                                         refreshable_type_))) {
        LOG_WARN("failed to print mv operators", K(ret));
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

int ObMVProvider::get_operators(const ObIArray<ObString> *&operators) const
{
  int ret = OB_SUCCESS;
  operators = NULL;
  if (OB_UNLIKELY(!inited_ || ObMVRefreshableType::OB_MV_COMPLETE_REFRESH >= refreshable_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected call for get_operators", K(ret), K(inited_), K(refreshable_type_));
  } else if (OB_UNLIKELY(operators_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty operators", K(ret));
  } else {
    operators = &operators_;
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
                                       const ObSelectStmt *view_stmt)
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
      } else if (OB_FAIL(ObCreateViewResolver::fill_column_meta_infos(*select_items.at(i).expr_,
                                                                      mv_schema->get_charset_type(),
                                                                      mv_schema->get_table_id(),
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
                     org_column.get_meta_type().get_type() == cur_column.get_meta_type().get_type()
                     && org_column.get_sub_data_type() == cur_column.get_sub_data_type()
                     // && org_column.get_charset_type() == cur_column.get_charset_type() todo: org_column charset_type is invalid now
                     && org_column.get_accuracy() == cur_column.get_accuracy()
                     && org_column.is_zero_fill() == cur_column.is_zero_fill()
                     && orig_strs.count() == cur_strs.count();
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

int ObMVProvider::generate_mv_stmt(ObIAllocator &alloc,
                                   ObStmtFactory &stmt_factory,
                                   ObRawExprFactory &expr_factory,
                                   ObSchemaChecker &schema_checker,
                                   ObSQLSessionInfo &session_info,
                                   const uint64_t mv_id,
                                   const ObTableSchema *&mv_schema,
                                   const ObSelectStmt *&view_stmt)
{
  int ret = OB_SUCCESS;
  view_stmt = NULL;
  mv_schema = NULL;
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
  ObSelectResolver select_resolver(resolver_ctx);
  if (OB_ISNULL(resolver_ctx.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(resolver_ctx.query_ctx_));
  } else if (OB_FAIL(resolver_ctx.query_ctx_->sql_schema_guard_.get_table_schema(mv_id, mv_schema))) {
    LOG_WARN("fail to get mv schema", K(ret), K(mv_id));
  } else if (OB_ISNULL(mv_schema) || OB_UNLIKELY(!mv_schema->is_materialized_view())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mv schema", K(ret), KPC(mv_schema));
    mv_schema = NULL;
  } else if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(alloc,
                                                                session_info.get_local_collation_connection(),
                                                                mv_schema->get_view_schema(),
                                                                view_definition))) {
    LOG_WARN("fail to generate view definition for resolve", K(ret));
  } else if (OB_FAIL(parser.parse(view_definition, parse_result))) {
    LOG_WARN("parse view definition failed", K(view_definition), K(ret));
  } else if (OB_ISNULL(node = parse_result.result_tree_->children_[0]) || OB_UNLIKELY(T_SELECT != node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mv select node", K(ret), K(node), K(node->type_));
  } else if (OB_FALSE_IT(resolver_ctx.query_ctx_->question_marks_count_ = static_cast<int64_t>(parse_result.question_mark_ctx_.count_))) {
  } else if (OB_FAIL(select_resolver.resolve(*node))) {
    LOG_WARN("resolve view definition failed", K(ret));
  } else if (OB_ISNULL(view_stmt = static_cast<ObSelectStmt*>(select_resolver.get_basic_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mv stmt", K(ret), K(view_stmt));
  } else {
    LOG_DEBUG("generate mv stmt", KPC(view_stmt));
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
