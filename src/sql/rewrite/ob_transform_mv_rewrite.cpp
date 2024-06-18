/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_mv_rewrite.h"
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_wrap_enum_set.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{

int ObTransformMVRewrite::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             ObDMLStmt *&stmt,
                                             bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  const ObDMLStmt *root_stmt = parent_stmts.empty() ? stmt : parent_stmts.at(0).stmt_;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(prepare_mv_info(root_stmt, stmt))) {
    LOG_WARN("failed to prepate mv info", K(ret));
  } else if (OB_FAIL(do_transform(stmt, trans_happened))) {
    LOG_WARN("failed to do transform", K(ret));
  }
  return ret;
}

ObTransformMVRewrite::~ObTransformMVRewrite() {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_)) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_infos_.count(); ++i) {
      if (OB_ISNULL(mv_infos_.at(i).view_stmt_)) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, mv_infos_.at(i).view_stmt_))) {
        LOG_WARN("failed to free stmt", K(ret));
      } else {
        mv_infos_.at(i).view_stmt_ = NULL;
      }
      if (OB_FAIL(ret) || OB_ISNULL(mv_infos_.at(i).select_mv_stmt_)) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, mv_infos_.at(i).select_mv_stmt_))) {
        LOG_WARN("failed to free stmt", K(ret));
      } else {
        mv_infos_.at(i).select_mv_stmt_ = NULL;
      }
    }
  }
}

int ObTransformMVRewrite::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                         const int64_t current_level,
                                         const ObDMLStmt &stmt,
                                         bool &need_trans)
{
  int ret = OB_SUCCESS;
  int64_t query_rewrite_enabled = QueryRewriteEnabledType::REWRITE_ENABLED_FALSE;
  bool has_mv = false;
  bool hint_rewrite = false;
  bool hint_no_rewrite = false;
  bool is_stmt_valid = false;
  uint64_t data_version;
  need_trans = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexprcted null", K(ret), K(ctx_), K(stmt.get_query_ctx()));
  } else if (OB_FAIL(ObTransformRule::need_transform(parent_stmts,
                                                     current_level,
                                                     stmt,
                                                     need_trans))) {
    LOG_WARN("failed to check need transformation", K(ret));
  } else if (!need_trans) {
    // do nothing
  } else if (ctx_->iteration_level_ > 0) {
    need_trans = false;
    OPT_TRACE("only do mv rewrite in the first iteration");
  } else if (stmt.is_select_stmt() && static_cast<const ObSelectStmt*>(&stmt)->is_expanded_mview()) {
    need_trans = false;
  } else if (ctx_->session_info_->get_ddl_info().is_refreshing_mview()) {
    need_trans = false;
    OPT_TRACE("not a user SQL, skip mv rewrite");
  } else if (stmt.get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_1) {
    need_trans = false;
    OPT_TRACE("optimizer features enable version is lower than 4.3.1, skip mv rewrite");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx_->session_info_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret), K(ctx_->session_info_->get_effective_tenant_id()));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_1_0)) {
    // data version lower than 4.3.1 does not have the inner table used to get mv list
    need_trans = false;
    OPT_TRACE("min data version is lower than 4.3.1, skip mv rewrite");
  } else if (OB_FAIL(check_hint_valid(stmt, hint_rewrite, hint_no_rewrite))) {
    LOG_WARN("failed to check mv rewrite hint", K(ret));
  } else if (hint_rewrite) {
    need_trans = true;
    OPT_TRACE("hint force mv rewrite");
  } else if (hint_no_rewrite) {
    need_trans = false;
    OPT_TRACE("hint reject mv rewrite");
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_enabled(query_rewrite_enabled))) {
    LOG_WARN("failed to get query rewrite enabled", K(ret));
  } else if (QueryRewriteEnabledType::REWRITE_ENABLED_FALSE == query_rewrite_enabled) {
    need_trans = false;
    OPT_TRACE("system variable reject mv rewrite");
  } else if (OB_FAIL(check_table_has_mv(stmt, has_mv))) {
    LOG_WARN("failed to check table has mv", K(ret));
  } else if (!has_mv) {
    need_trans = false;
    OPT_TRACE("table does not have mv, no need to rewrite");
  } else if (OB_FAIL(check_basic_validity(stmt, is_stmt_valid))) {
    LOG_WARN("failed to check basic validity", K(ret));
  } else if (!is_stmt_valid) {
    need_trans = false;
    OPT_TRACE("stmt can not do mv rewrite");
  } else {
    need_trans = true;
  }
  return ret;
}

int ObTransformMVRewrite::check_hint_valid(const ObDMLStmt &stmt,
                                           bool &force_rewrite,
                                           bool &force_no_rewrite)
{
  int ret = OB_SUCCESS;
  force_rewrite = false;
  force_no_rewrite = false;
  const ObQueryHint *query_hint = NULL;
  const ObMVRewriteHint *myhint = static_cast<const ObMVRewriteHint*>(get_hint(stmt.get_stmt_hint()));
  if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("query hint is null", K(ret), K(stmt));
  } else {
    force_rewrite = NULL != myhint && myhint->is_enable_hint();
    force_no_rewrite = !force_rewrite && query_hint->has_outline_data();
    // force_no_rewrite does not need to handle (NULL != myhint && myhint->is_disable_hint()),
    // because it has been handled in ObTransformRule::check_hint_status
  }
  return ret;
}

int ObTransformMVRewrite::check_table_has_mv(const ObDMLStmt &stmt,
                                             bool &has_mv)
{
  int ret = OB_SUCCESS;
  has_mv = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_mv && i < stmt.get_table_size(); ++i) {
    const TableItem *table = NULL;
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(table = stmt.get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i), K(stmt.get_table_size()));
    } else if (!table->is_basic_table()) {
      // do nothing
    } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(),
                                                               table->ref_id_,
                                                               table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(i), K(table->ref_id_));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret), K(table->ref_id_));
    } else if (ObTableReferencedByMVFlag::IS_REFERENCED_BY_MV ==
               ObTableMode::get_table_referenced_by_mv_flag(table_schema->get_table_mode())) {
      has_mv = true;
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_basic_validity(const ObDMLStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> seq_exprs;
  is_valid = true;
  if (!stmt.is_select_stmt()) {
    OPT_TRACE("stmt can not do mv rewrite, not a select stmt");
    is_valid = false;
  } else if (stmt.is_set_stmt()) {
    OPT_TRACE("stmt can not do mv rewrite, is a set stmt");
    is_valid = false;
  } else if (stmt.is_hierarchical_query()) {
    OPT_TRACE("stmt can not do mv rewrite, is a hierarchical query");
    is_valid = false;
  } else if (stmt.is_contains_assignment()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains assignment");
    is_valid = false;
  } else if (stmt.has_for_update()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains for update");
    is_valid = false;
  } else if (stmt.has_ora_rowscn()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains ora rowscn");
    is_valid = false;
  } else if (stmt.is_unpivot_select()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains unpivot");
    is_valid = false;
  } else if (!stmt.get_pseudo_column_like_exprs().empty()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains pseudo column");
    is_valid = false;
  } else if (static_cast<const ObSelectStmt*>(&stmt)->has_rollup()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains roll up");
    is_valid = false;
  } else if (static_cast<const ObSelectStmt*>(&stmt)->has_select_into()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains select into");
    is_valid = false;
  } else if (OB_FAIL(stmt.get_sequence_exprs(seq_exprs))) {
    LOG_WARN("failed to get sequence exprs", K(ret));
  } else if (!seq_exprs.empty()) {
    OPT_TRACE("stmt can not do mv rewrite, is a contains sequence");
    is_valid = false;
  }
  return ret;
}

int ObTransformMVRewrite::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  const MvInfo *mv_info = NULL;
  ObMVRewriteHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(mv_info = static_cast<const MvInfo*>(trans_params))
      || OB_ISNULL(mv_info->mv_schema_) || OB_ISNULL(mv_info->db_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(trans_params));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_MV_REWRITE, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else if (OB_FAIL(hint->get_mv_list().push_back(
      ObTableInHint(NULL, mv_info->db_schema_->get_database_name(), mv_info->mv_schema_->get_table_name())))) {
    LOG_WARN("failed to push back hint table", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
  }
  return ret;
}

int ObTransformMVRewrite::prepare_mv_info(const ObDMLStmt *root_stmt,
                                          const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> mv_list;
  if (is_mv_info_generated_) {
    // do nothing
  } else if (OB_FAIL(get_mv_list(root_stmt, mv_list))) {
    LOG_WARN("failed to get mv list", K(ret));
  } else if (OB_FAIL(generate_mv_info(stmt, mv_list))) {
    LOG_WARN("failed to generate mv info", K(ret));
  } else {
    is_mv_info_generated_ = true;
    OPT_TRACE("use", mv_infos_.count(), "materialized view(s) to perform rewrite");
  }
  return ret;
}

int ObTransformMVRewrite::get_mv_list(const ObDMLStmt *root_stmt,
                                      ObIArray<uint64_t> &mv_list)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = NULL;
  ObSqlString sql;
  sqlclient::ObMySQLResult *mysql_result = NULL;
  ObSqlString table_ids;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(get_base_table_id_string(root_stmt, table_ids))) {
    LOG_WARN("failed to get base table id string", K(ret));
  } else if (OB_ISNULL(sql_proxy = ctx_->exec_ctx_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT DISTINCT MVIEW_ID FROM `%s`.`%s` WHERE TENANT_ID = 0 AND P_OBJ IN (%.*s)",
                                    OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                                    static_cast<int>(table_ids.length()), table_ids.ptr()))) {
    LOG_WARN("failed to assign sql", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_proxy->read(res, ctx_->session_info_->get_effective_tenant_id(), sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get result", K(ret));
      } else {
        while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
          int64_t mv_id = OB_INVALID_ID;
          if (OB_FAIL(mysql_result->get_int(0L, mv_id))) {
            LOG_WARN("failed to get mv id", K(ret));
          } else if (OB_FAIL(mv_list.push_back(mv_id))) {
            LOG_WARN("failed to push back mv id", K(ret), K(mv_id));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          OPT_TRACE("get", mv_list.count(), "materialized view(s)");
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to get inner sql result", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::get_base_table_id_string(const ObDMLStmt *stmt,
                                                   ObSqlString &table_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_id_list;
  ObSEArray<uint64_t, 4> visited_id;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(get_all_base_table_id(stmt, table_id_list))) {
    LOG_WARN("failed to get table ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_id_list.count(); ++i) {
    if (is_contain(visited_id, table_id_list.at(i))) {
      // do nothing
    } else if (OB_FAIL(visited_id.push_back(table_id_list.at(i)))) {
      LOG_WARN("failed to push back table ref id", K(ret));
    } else if (i > 0 && OB_FAIL(table_ids.append(","))) {
      LOG_WARN("failed to append comma", K(ret));
    } else if (OB_FAIL(table_ids.append(to_cstring(table_id_list.at(i))))) {
      LOG_WARN("failed to append table id", K(ret));
    }
  }
  return ret;
}

int ObTransformMVRewrite::get_all_base_table_id(const ObDMLStmt *stmt,
                                                ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
    const TableItem *table = NULL;
    if (OB_ISNULL(table = stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(i));
    } else if (!table->is_basic_table()) {
      // do nothing
    } else if (OB_FAIL(table_ids.push_back(table->ref_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(get_all_base_table_id(child_stmts.at(i), table_ids)))) {
      LOG_WARN("failed to get all base table id", K(ret));
    }
  }
  return ret;
}

int ObTransformMVRewrite::generate_mv_info(const ObDMLStmt *stmt,
                                           ObIArray<uint64_t> &mv_list)
{
  int ret = OB_SUCCESS;
  int64_t query_rewrite_integrity = QueryRewriteIntegrityType::REWRITE_INTEGRITY_ENFORCED;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)
      || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(stmt));
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_integrity(query_rewrite_integrity))) {
    LOG_WARN("failed to get query rewrite integrity", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_list.count(); ++i) {
    const ObTableSchema *mv_schema = NULL;
    const ObDatabaseSchema *db_schema = NULL;
    bool is_valid = true;
    if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(),
                                                        mv_list.at(i),
                                                        mv_schema))) {
      LOG_WARN("failed to get mv schema", K(ret), K(mv_list.at(i)));
    } else if (OB_ISNULL(mv_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv schema is null", K(ret), K(mv_list.at(i)));
    } else if (OB_FAIL(ctx_->schema_checker_->get_database_schema(ctx_->session_info_->get_effective_tenant_id(),
                                                                  mv_schema->get_database_id(),
                                                                  db_schema))) {
      LOG_WARN("failed to get data base schema", K(ret), K(mv_schema->get_database_id()));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("database schema is NULL", K(ret), K(mv_schema->get_database_id()));
    } else if (OB_FAIL(quick_rewrite_check(mv_schema,
                                           query_rewrite_integrity == QueryRewriteIntegrityType::REWRITE_INTEGRITY_STALE_TOLERATED,
                                           is_valid))) {
      LOG_WARN("failed to do quick check", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else {
      uint64_t data_table_id = mv_schema->get_data_table_id();
      const ObTableSchema *data_table_schema = NULL;
      if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(),
                                                          data_table_id,
                                                          data_table_schema))) {
        LOG_WARN("failed to get data table schema", K(ret), K(data_table_id));
      } else if (OB_ISNULL(data_table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data table schema is null", K(ret), K(mv_list.at(i)));
      } else if (OB_FAIL(mv_infos_.push_back(MvInfo(mv_list.at(i),
                                                    data_table_id,
                                                    mv_schema,
                                                    data_table_schema,
                                                    db_schema,
                                                    NULL)))) {
        LOG_WARN("failed to push back mv info", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::quick_rewrite_check(const ObTableSchema *mv_schema,
                                              bool allow_stale,
                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(mv_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv schema is null", K(ret));
  } else if (!mv_schema->mv_enable_query_rewrite()) {
    is_valid = false;
  } else if (!allow_stale && !mv_schema->mv_on_query_computation()) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return false;
}

int ObTransformMVRewrite::check_mv_stmt_basic_validity(const MvInfo &mv_info,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = NULL;
  is_valid = false;
  if (OB_ISNULL(stmt = mv_info.view_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(mv_info));
  } else if (!stmt->is_spj()) {
    is_valid = false;
    OPT_TRACE("mv can not be used for rewrite");
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::generate_mv_stmt(MvInfo &mv_info)
{
  int ret = OB_SUCCESS;
  void* qctx_ptr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->stmt_factory_)
      || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(mv_info.mv_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv_info is null", K(ret), K(ctx_), KPC(mv_info.mv_schema_));
  } else {
    SMART_VARS_2((ObTransformerCtx, trans_ctx), (ObResolverParams, resolver_ctx)) {
      ObParser parser(*ctx_->allocator_, ctx_->session_info_->get_sql_mode(), ctx_->session_info_->get_charsets4parser());
      ParseResult parse_result;
      ParseNode *node = NULL;
      ObString view_definition;
      ObDMLStmt *view_stmt = NULL;
      uint64_t dummy_value = 0;
      ObIAllocator &alloc = *ctx_->allocator_;
      resolver_ctx.allocator_ = ctx_->allocator_;
      resolver_ctx.schema_checker_ = ctx_->schema_checker_;
      resolver_ctx.session_info_ = ctx_->session_info_;
      resolver_ctx.expr_factory_ = ctx_->expr_factory_;
      resolver_ctx.stmt_factory_ = ctx_->stmt_factory_;
      resolver_ctx.sql_proxy_ = GCTX.sql_proxy_;
      resolver_ctx.query_ctx_ = &mv_temp_query_ctx_;
      // resolver_ctx.is_for_rt_mv_ = true;
      trans_ctx = *ctx_;
      trans_ctx.reset();
      ObSelectResolver select_resolver(resolver_ctx);
      ObTransformPreProcess transform_pre_process(&trans_ctx);
      transform_pre_process.set_transformer_type(PRE_PROCESS);
      STOP_OPT_TRACE;
      if (OB_ISNULL(resolver_ctx.query_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ctx is null", K(ret));
      } else if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(*ctx_->allocator_,
                                                                          ctx_->session_info_->get_local_collation_connection(),
                                                                          mv_info.mv_schema_->get_view_schema(),
                                                                          view_definition))) {
        LOG_WARN("fail to generate view definition for resolve", K(ret));
      } else if (OB_FAIL(parser.parse(view_definition, parse_result))) {
        LOG_WARN("parse view definition failed", K(view_definition), K(ret));
      } else if (OB_ISNULL(node = parse_result.result_tree_->children_[0]) || OB_UNLIKELY(T_SELECT != node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mv select node", K(ret), K(node), K(node->type_));
      } else if (OB_FALSE_IT(resolver_ctx.query_ctx_->question_marks_count_
                             = static_cast<int64_t>(parse_result.question_mark_ctx_.count_))) {
      } else if (OB_FAIL(select_resolver.resolve(*node))) {
        LOG_WARN("resolve view definition failed", K(ret));
      } else if (OB_ISNULL(view_stmt = static_cast<ObSelectStmt*>(select_resolver.get_basic_stmt()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mv stmt", K(ret), K(view_stmt));
      } else if (OB_FAIL(resolver_ctx.query_ctx_->query_hint_.init_query_hint(resolver_ctx.allocator_,
                                                                              resolver_ctx.session_info_,
                                                                              view_stmt))) {
        LOG_WARN("failed to init query hint.", K(ret));
      } else if (OB_FAIL(resolver_ctx.query_ctx_->query_hint_.check_and_set_params_from_hint(resolver_ctx,
                                                                                            *view_stmt))) {
        LOG_WARN("failed to check and set params from hint", K(ret));
      } else if (OB_FAIL(transform_pre_process.transform(view_stmt, dummy_value))) {
        LOG_WARN("failed to do transform pre process", K(ret), KPC(view_stmt));
      } else if (OB_ISNULL(mv_info.view_stmt_ = static_cast<ObSelectStmt*>(view_stmt))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mv stmt", K(ret), K(view_stmt));
      } else {
        LOG_DEBUG("generate mv stmt", KPC(mv_info.view_stmt_));
      }

      // resolve "select 1 from mv" for mv with multi partitions
      bool has_multi_part = false;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(check_mv_has_multi_part(mv_info, has_multi_part))) {
        LOG_WARN("failed to check mv has multi part", K(ret));
      } else if (has_multi_part) {
        ObParser mv_sql_parser(*ctx_->allocator_, ctx_->session_info_->get_sql_mode(), ctx_->session_info_->get_charsets4parser());
        ObSelectResolver mv_sql_select_resolver(resolver_ctx);
        ObSqlString mv_sql; // select 1 from mv;
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(mv_sql.assign_fmt(lib::is_oracle_mode() ?
                            "SELECT /*+no_mv_rewrite*/ 1 FROM \"%.*s\".\"%.*s\"" :
                            "SELECT /*+no_mv_rewrite*/ 1 FROM `%.*s`.`%.*s`",
                            mv_info.db_schema_->get_database_name_str().length(), mv_info.db_schema_->get_database_name_str().ptr(),
                            mv_info.mv_schema_->get_table_name_str().length(), mv_info.mv_schema_->get_table_name_str().ptr()))) {
          LOG_WARN("failed to assign sql", K(ret));
        } else if (OB_FAIL(mv_sql_parser.parse(ObString::make_string(mv_sql.ptr()), parse_result))) {
          LOG_WARN("parse view definition failed", K(mv_sql), K(ret));
        } else if (OB_ISNULL(node = parse_result.result_tree_->children_[0]) || OB_UNLIKELY(T_SELECT != node->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid mv select node", K(ret), K(node), K(node->type_));
        } else if (OB_FALSE_IT(resolver_ctx.query_ctx_->question_marks_count_
                              = static_cast<int64_t>(parse_result.question_mark_ctx_.count_))) {
        } else if (OB_FAIL(mv_sql_select_resolver.resolve(*node))) {
          LOG_WARN("resolve view definition failed", K(ret));
        } else if (OB_ISNULL(mv_info.select_mv_stmt_ = static_cast<ObSelectStmt*>(mv_sql_select_resolver.get_basic_stmt()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid mv stmt", K(ret), K(mv_info.select_mv_stmt_));
        }
      } else {
        mv_info.select_mv_stmt_ = NULL;
      }

      RESUME_OPT_TRACE;
      OPT_TRACE(mv_info.mv_schema_->get_table_name(), ":", mv_info.view_stmt_);
    }
  }
  return ret;
}

int ObTransformMVRewrite::check_mv_has_multi_part(const MvInfo &mv_info,
                                                  bool &has_multi_part)
{
  int ret = OB_SUCCESS;
  has_multi_part = false;
  ObSEArray<ObAuxTableMetaInfo, 8> simple_index_infos;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)
      || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(mv_info.data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(mv_info));
  } else if (mv_info.data_table_schema_->get_part_level() != PARTITION_LEVEL_ZERO) {
    has_multi_part = true;
  } else if (OB_FAIL(mv_info.data_table_schema_->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("failed to get simple index infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_multi_part && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_schema = NULL;
    if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(), simple_index_infos.at(i).table_id_, index_schema))) {
      LOG_WARN("get index schema from schema checker failed", K(ret), K(simple_index_infos.at(i).table_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index table not exists", K(simple_index_infos.at(i).table_id_));
    } else if (index_schema->is_final_invalid_index() || !index_schema->is_global_index_table()) {
      //do nothing
    } else if (index_schema->get_part_level() != PARTITION_LEVEL_ZERO) {
      has_multi_part = true;
    }
  }
  return ret;
}

int ObTransformMVRewrite::do_transform(ObDMLStmt *&stmt,
                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  const int64_t max_mv_stmt_gen = 10;
  trans_happened = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(stmt));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else {
    ObSelectStmt *ori_stmt = static_cast<ObSelectStmt*>(stmt);
    ObSelectStmt *new_stmt = NULL;
    const ObCollationType cs_type = stmt->get_query_ctx()->get_query_hint().cs_type_;
    const ObMVRewriteHint *myhint = static_cast<const ObMVRewriteHint*>(get_hint(stmt->get_stmt_hint()));
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_infos_.count(); ++i) {
      bool is_happened = false;
      bool is_match_hint = true;
      bool is_mv_valid = false;
      MvInfo &mv_info = mv_infos_.at(i);
      if (NULL != myhint && OB_FAIL(myhint->check_mv_match_hint(cs_type,
                                                                mv_info.mv_schema_,
                                                                mv_info.db_schema_,
                                                                is_match_hint))) {
        LOG_WARN("failed to check mv match hint", K(ret));
      } else if (!is_match_hint) {
        // do nothing
      } else if (NULL == mv_info.view_stmt_ && ++mv_stmt_gen_count_ > max_mv_stmt_gen) {
        // do nothing
      } else if (NULL == mv_info.view_stmt_ && OB_FAIL(generate_mv_stmt(mv_info))) {
        LOG_WARN("failed to generate mv stmt", K(ret), K(mv_info));
      } else if (OB_FAIL(check_mv_stmt_basic_validity(mv_info, is_mv_valid))) {
        LOG_WARN("failed to check mv basic validity", K(ret), K(mv_info));
      } else if (!is_mv_valid) {
        // do nothing
      } else if (OB_FAIL(try_transform_with_one_mv(ori_stmt, mv_info, new_stmt, is_happened))) {
        LOG_WARN("failed to try one mv", K(ret));
      } else if (is_happened) {
        ori_stmt = new_stmt;
        trans_happened = true;
        break;
      }
    }
    if (OB_SUCC(ret) && trans_happened) {
      stmt = ori_stmt;
    }
  }
  return ret;
}

int ObTransformMVRewrite::try_transform_with_one_mv(ObSelectStmt *origin_stmt,
                                                    MvInfo &mv_info,
                                                    ObSelectStmt *&new_stmt,
                                                    bool &transform_happened)
{
  int ret = OB_SUCCESS;
  ObStmtMapInfo map_info;
  QueryRelation relation;
  ObTryTransHelper try_trans_helper;
  bool is_valid = false;
  transform_happened = false;
  new_stmt = NULL;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.mv_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info.mv_schema_));
  } else if (OB_FAIL(try_trans_helper.fill_helper(origin_stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(ObStmtComparer::check_stmt_containment(origin_stmt,
                                                            mv_info.view_stmt_,
                                                            map_info,
                                                            relation,
                                                            false,
                                                            false,
                                                            false))) {
    LOG_WARN("failed to check stmt containment", K(ret));
  } else if ((QueryRelation::QUERY_EQUAL != relation
             && QueryRelation::QUERY_LEFT_SUBSET != relation)
             || map_info.left_can_be_replaced_ == false) {
    OPT_TRACE(mv_info.mv_schema_->get_table_name(), ": stmt is not matched");
  } else if (OB_FAIL(do_transform_use_one_mv(origin_stmt,
                                             mv_info,
                                             map_info,
                                             new_stmt,
                                             is_valid))) {
    LOG_WARN("failed to do transform use one mv", K(ret));
  } else if (!is_valid) {
    OPT_TRACE(mv_info.mv_schema_->get_table_name(), ": stmt is not matched");
  } else if (OB_FAIL(check_rewrite_expected(origin_stmt,
                                            new_stmt,
                                            mv_info,
                                            is_valid))) {
    LOG_WARN("failed to check rewrite expected", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(add_param_constraint(map_info))) {
    LOG_WARN("failed to add param constraint", K(ret));
  } else if (OB_FAIL(add_transform_hint(*new_stmt, &mv_info))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    transform_happened = true;
    LOG_DEBUG("succeed to transform with one mv", KPC(new_stmt));
  }
  if (OB_SUCC(ret) && !transform_happened
     && OB_FAIL(try_trans_helper.recover(origin_stmt->get_query_ctx()))) {
    LOG_WARN("failed to recover params", K(ret));
  }
  return ret;
}

int ObTransformMVRewrite::do_transform_use_one_mv(ObSelectStmt *origin_stmt,
                                                  const MvInfo &mv_info,
                                                  ObStmtMapInfo &map_info,
                                                  ObSelectStmt *&new_stmt,
                                                  bool &is_valid_transform)
{
  int ret = OB_SUCCESS;
  is_valid_transform = false;
  new_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.mv_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(origin_stmt), K(mv_info.mv_schema_));
  } else {
    bool is_valid = true;
    GenerateStmtHelper helper(*ctx_->expr_factory_);
    if (OB_FAIL(ctx_->stmt_factory_->create_stmt(helper.new_stmt_))) {
      LOG_WARN("failed to create stmt", K(ret));
    } else if (OB_ISNULL(helper.new_stmt_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret));
    } else if (OB_FALSE_IT(helper.new_stmt_->set_query_ctx(origin_stmt->get_query_ctx()))) {
    } else if (OB_FAIL(helper.new_stmt_->get_stmt_hint().assign(origin_stmt->get_stmt_hint()))) {
      LOG_WARN("failed to assign stmt hint", K(ret));
    } else if (OB_FAIL(helper.new_stmt_->adjust_statement_id(ctx_->allocator_,
                                                             ctx_->src_qb_name_,
                                                             ctx_->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    } else if (OB_FALSE_IT(helper.map_info_ = &map_info)) {
    } else if (OB_FAIL(create_mv_table_item(mv_info, helper))) {
      LOG_WARN("failed to create mv table item", K(ret), K(mv_info));
    } else if (OB_ISNULL(helper.mv_item_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mv table item is null", K(ret));
    } else if (OB_FAIL(create_mv_column_item(mv_info, helper))) {
      LOG_WARN("failed to create mv column item", K(ret));
    } else if (OB_FAIL(fill_from_item(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_condition_exprs(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add condition exprs", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_groupby_exprs(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add group by exprs", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_select_item(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add select item", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_having_exprs(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add having exprs", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_distinct(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add distinct", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_orderby_exprs(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add order by exprs", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(fill_limit(origin_stmt, mv_info, helper, is_valid))) {
      LOG_WARN("failed to add limit exprs", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(expand_rt_mv_table(helper))) {
      LOG_WARN("failed to expand real time mv", K(ret));
    } else if (OB_FAIL(helper.new_stmt_->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt info", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (is_valid) {
      new_stmt = helper.new_stmt_;
      is_valid_transform = true;
      OPT_TRACE("generate rewrite stmt use", mv_info.mv_schema_->get_table_name(), ":", new_stmt);
    } else if (OB_FAIL(ObTransformUtils::free_stmt(*ctx_->stmt_factory_, helper.new_stmt_))) {
      LOG_WARN("failed to free stmt", K(ret));
    } else {
      helper.new_stmt_ = NULL;
    }
  }
  return ret;
}

int ObTransformMVRewrite::create_mv_table_item(const MvInfo &mv_info,
                                               GenerateStmtHelper &helper)
{
  int ret = OB_SUCCESS;
  TableItem *mv_item = NULL;
  ObString qb_name;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->schema_checker_) ||
      OB_ISNULL(ctx_->session_info_) || OB_ISNULL(helper.new_stmt_) || OB_ISNULL(helper.new_stmt_->get_query_ctx()) ||
      OB_ISNULL(mv_info.mv_schema_) || OB_ISNULL(mv_info.data_table_schema_) || OB_ISNULL(mv_info.db_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(helper.new_stmt_), K(mv_info));
  } else if (OB_FAIL(helper.new_stmt_->get_qb_name(qb_name))) {
  } else if (OB_UNLIKELY(NULL == (mv_item = helper.new_stmt_->create_table_item(*ctx_->allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create table item failed", K(ret));
  } else {
    mv_item->type_ = TableItem::ALIAS_TABLE;
    // mv_item->synonym_name_ = ;
    // mv_item->synonym_db_name_ = ;
    mv_item->database_name_ = mv_info.db_schema_->get_database_name_str();
    mv_item->ref_id_ = mv_info.mv_schema_->get_data_table_id();
    mv_item->mview_id_ = mv_info.mv_schema_->get_table_id();
    mv_item->table_type_ = mv_info.mv_schema_->get_table_type();
    mv_item->table_id_ = helper.new_stmt_->get_query_ctx()->available_tb_id_--;
    mv_item->is_index_table_ = mv_info.mv_schema_->is_index_table();
    mv_item->table_name_ = mv_info.data_table_schema_->get_table_name_str();
    mv_item->alias_name_ = mv_info.mv_schema_->get_table_name_str();
    mv_item->qb_name_ = qb_name;

    ObSchemaObjVersion table_version;
    table_version.object_id_ = mv_info.mv_schema_->get_table_id();
    table_version.object_type_ = mv_info.mv_schema_->is_view_table() ? DEPENDENCY_VIEW : DEPENDENCY_TABLE;
    table_version.version_ = mv_info.mv_schema_->get_schema_version();
    table_version.is_db_explicit_ = true;
    uint64_t dep_db_id = mv_info.mv_schema_->get_database_id();

    ObSchemaObjVersion data_table_version;
    data_table_version.object_id_ = mv_info.data_table_schema_->get_table_id();
    data_table_version.object_type_ = DEPENDENCY_TABLE;
    data_table_version.version_ = mv_info.data_table_schema_->get_schema_version();
    data_table_version.is_db_explicit_ = true;
    uint64_t data_dep_db_id = mv_info.data_table_schema_->get_database_id();

    if (OB_FAIL(helper.new_stmt_->add_global_dependency_table(table_version))) {
      LOG_WARN("add global dependency table failed", K(ret));
    } else if (OB_FAIL(helper.new_stmt_->add_ref_obj_version(mv_info.mv_schema_->get_table_id(),
                                                             dep_db_id,
                                                             ObObjectType::VIEW,
                                                             table_version,
                                                             *ctx_->allocator_))) {
      LOG_WARN("failed to add ref obj version", K(ret));
    } else if (OB_FAIL(helper.new_stmt_->add_global_dependency_table(data_table_version))) {
      LOG_WARN("add global dependency table failed", K(ret));
    } else if (OB_FAIL(helper.new_stmt_->add_ref_obj_version(mv_info.mv_schema_->get_table_id(),
                                                             data_dep_db_id,
                                                             ObObjectType::VIEW,
                                                             data_table_version,
                                                             *ctx_->allocator_))) {
      LOG_WARN("failed to add ref obj version", K(ret));
    } else if (OB_FAIL(helper.new_stmt_->add_table_item(ctx_->session_info_, mv_item))) {
      LOG_WARN("push back table item failed", K(ret), KPC(mv_item));
    } else if (OB_FAIL(helper.new_stmt_->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuid table hash", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    helper.mv_item_ = mv_item;
  }
  return ret;
}

int ObTransformMVRewrite::create_mv_column_item(const MvInfo &mv_info,
                                                GenerateStmtHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->sql_schema_guard_)
      || OB_ISNULL(ctx_->sql_schema_guard_->get_schema_guard()) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(helper.new_stmt_) || OB_ISNULL(helper.mv_item_)
      || OB_ISNULL(mv_info.view_stmt_) || OB_ISNULL(mv_info.data_table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(helper.new_stmt_), K(helper.mv_item_), K(mv_info));
  }
  // create mv column item
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_info.data_table_schema_->get_column_count(); ++i) {
    const ObColumnSchemaV2 *col_schema = NULL;
    ObColumnRefRawExpr *col_expr = NULL;
    bool is_uni = false;
    bool is_mul = false;
    ColumnItem column_item;
    if (OB_ISNULL(col_schema = mv_info.data_table_schema_->get_column_schema_by_idx(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*ctx_->expr_factory_, *col_schema, col_expr))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_FAIL(mv_info.data_table_schema_->is_unique_key_column(*ctx_->sql_schema_guard_->get_schema_guard(),
                                                                         col_schema->get_column_id(),
                                                                         is_uni))) {
      LOG_WARN("fail to check is unique key column", K(ret), KPC(mv_info.data_table_schema_), K(col_schema->get_column_id()));
    } else if (OB_FAIL(mv_info.data_table_schema_->is_multiple_key_column(*ctx_->sql_schema_guard_->get_schema_guard(),
                                                                           col_schema->get_column_id(),
                                                                           is_mul))) {
      LOG_WARN("fail to check is multiple key column", K(ret), KPC(mv_info.data_table_schema_), K(col_schema->get_column_id()));
    } else {
      if (!ob_enable_lob_locator_v2()) {
        // Notice: clob will not convert to ObLobType if locator v2 enabled
        if (is_oracle_mode() && ObLongTextType == col_expr->get_data_type()
            && ! is_virtual_table(col_schema->get_table_id())) {
          col_expr->set_data_type(ObLobType);
        }
      }
      if (helper.mv_item_->alias_name_.empty()) {
        col_expr->set_synonym_db_name(helper.mv_item_->synonym_db_name_);
        col_expr->set_synonym_name(helper.mv_item_->synonym_name_);
      }
      col_expr->set_column_attr(helper.mv_item_->get_table_name(), col_schema->get_column_name_str());
      col_expr->set_from_alias_table(!helper.mv_item_->alias_name_.empty());
      col_expr->set_database_name(helper.mv_item_->database_name_);
      //column maybe from alias table, so must reset ref id by table id from table_item
      col_expr->set_ref_id(helper.mv_item_->table_id_, col_schema->get_column_id());
      col_expr->set_unique_key_column(is_uni);
      col_expr->set_mul_key_column(is_mul);
      if (!helper.mv_item_->alias_name_.empty()) {
        col_expr->set_table_alias_name();
      }
      col_expr->set_lob_column(is_lob_storage(col_schema->get_data_type()));
      if (ctx_->session_info_->get_ddl_info().is_ddl()) {
        column_item.set_default_value(col_schema->get_orig_default_value());
      } else {
        column_item.set_default_value(col_schema->get_cur_default_value());
      }
      column_item.expr_ = col_expr;
      column_item.table_id_ = col_expr->get_table_id();
      column_item.column_id_ = col_expr->get_column_id();
      column_item.column_name_ = col_expr->get_column_name();
      column_item.base_tid_ = helper.mv_item_->ref_id_;
      column_item.base_cid_ = column_item.column_id_;
      column_item.is_geo_ = col_schema->is_geometry();
      LOG_DEBUG("succ to fill column_item", K(column_item), KPC(col_schema));
      if (OB_FAIL(helper.new_stmt_->add_column_item(column_item))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else if (OB_FAIL(col_expr->pull_relation_id())) {
        LOG_WARN("failed to pullup relation ids", K(ret));
      }
    }
  }
  // add column expr to copier
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_info.view_stmt_->get_select_item_size(); ++i) {
    ObColumnRefRawExpr *col_expr = NULL;
    if (OB_ISNULL(col_expr = helper.new_stmt_->get_column_expr_by_id(helper.mv_item_->table_id_, OB_APP_MIN_COLUMN_ID + i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column id", K(ret), K(helper.mv_item_->table_id_), K(OB_APP_MIN_COLUMN_ID + i));
    } else if (OB_FAIL(helper.col_copier_.add_replaced_expr(mv_info.view_stmt_->get_select_item(i).expr_, col_expr))) {
      LOG_WARN("failed to add replaced expr", K(ret), KPC(mv_info.view_stmt_->get_select_item(i).expr_), KPC(col_expr));
    }
  }
  // fill part expr
  if (OB_SUCC(ret) && NULL != mv_info.select_mv_stmt_
      && mv_info.select_mv_stmt_->get_part_exprs().count() > 0) {
    ObRawExprCopier part_expr_copier(*ctx_->expr_factory_); // FROM select_mv_stmt_ TO new_stmt_
    ObSEArray<ObColumnRefRawExpr*, 4> mv_col_exprs; // column expr in select_mv_stmt_
    // make part_expr_copier which maps select_mv_stmt_ columns to new_stmt_ columns
    if (OB_FAIL(mv_info.select_mv_stmt_->get_column_exprs(mv_col_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_col_exprs.count(); ++i) {
      ObColumnRefRawExpr *col_expr = NULL; // column expr in new_stmt_
      if (OB_ISNULL(mv_col_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret), K(i));
      } else if (OB_ISNULL(col_expr = helper.new_stmt_->get_column_expr_by_id(helper.mv_item_->table_id_, mv_col_exprs.at(i)->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column id", K(ret), K(helper.mv_item_->table_id_), K(i));
      } else if (OB_FAIL(part_expr_copier.add_replaced_expr(mv_col_exprs.at(i), col_expr))) {
        LOG_WARN("failed to add replaced expr", K(ret), KPC(mv_col_exprs.at(i)), KPC(col_expr));
      }
    }
    // do fill part expr for new_stmt_
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_info.select_mv_stmt_->get_part_exprs().count(); ++i) {
      ObDMLStmt::PartExprItem &part_item = mv_info.select_mv_stmt_->get_part_exprs().at(i);
      ObRawExpr *part_expr = NULL;
      ObRawExpr *subpart_expr = NULL;
      if (OB_FAIL(part_expr_copier.copy_on_replace(part_item.part_expr_, part_expr))) {
        LOG_WARN("failed to copy part expr", K(ret), K(part_item));
      } else if (NULL != part_item.subpart_expr_
                 && OB_FAIL(part_expr_copier.copy_on_replace(part_item.subpart_expr_, subpart_expr))) {
        LOG_WARN("failed to copy subpart expr", K(ret), K(part_item));
      } else if (OB_FAIL(helper.new_stmt_->set_part_expr(helper.mv_item_->table_id_,
                                                         part_item.index_tid_,
                                                         part_expr,
                                                         subpart_expr))) {
        LOG_WARN("set part expr to new stmt failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::fill_from_item(ObSelectStmt *origin_stmt,
                                         const MvInfo &mv_info,
                                         GenerateStmtHelper &helper,
                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  TableItem *mv_item = NULL;
  is_valid = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(helper.new_stmt_) || OB_ISNULL(helper.mv_item_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(helper.new_stmt_), K(helper.mv_item_));
  } else if (OB_FAIL(helper.new_stmt_->add_from_item(helper.mv_item_->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret), KPC(helper.new_stmt_), KPC(helper.mv_item_));
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::fill_select_item(ObSelectStmt *origin_stmt,
                                           const MvInfo &mv_info,
                                           GenerateStmtHelper &helper,
                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> ori_select_exprs;
  ObSEArray<ObRawExpr*, 4> mv_select_exprs;
  ObSEArray<ObRawExpr*, 4> new_select_exprs;
  bool is_sub_valid = false;
  is_valid = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(origin_stmt)
      || OB_ISNULL(mv_info.view_stmt_) || OB_ISNULL(helper.new_stmt_)
      || OB_ISNULL(helper.map_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(origin_stmt), K(helper.new_stmt_), K(helper.map_info_));
  } else if (OB_FAIL(origin_stmt->get_select_exprs(ori_select_exprs))) {
    LOG_WARN("failed to get origin select exprs", K(ret));
  } else if (OB_FAIL(mv_info.view_stmt_->get_select_exprs(mv_select_exprs))) {
    LOG_WARN("failed to get mv select exprs", K(ret));
  } else if (OB_FAIL(ObStmtComparer::compute_new_expr(ori_select_exprs,
                                                      origin_stmt,
                                                      mv_select_exprs,
                                                      mv_info.view_stmt_,
                                                      *helper.map_info_,
                                                      helper.compute_expr_copier_,
                                                      new_select_exprs,
                                                      is_sub_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_sub_valid) {
    LOG_TRACE("select exprs can not be computed", K(new_select_exprs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_select_exprs.count(); ++i) {
      ObRawExpr *select_expr = NULL;
      if (OB_FAIL(helper.col_copier_.copy_on_replace(new_select_exprs.at(i), select_expr))) {
        LOG_WARN("failed to copy expr", K(ret), KPC(new_select_exprs.at(i)));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                              select_expr,
                                                              helper.new_stmt_))) {
        LOG_WARN("failed to create select item", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_aggr_winfun_expr(helper.new_stmt_, select_expr))) {
        LOG_WARN("failed to add aggr winfun expr", K(ret), KPC(select_expr));
      }
    }
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::fill_condition_exprs(ObSelectStmt *origin_stmt,
                                               const MvInfo &mv_info,
                                               GenerateStmtHelper &helper,
                                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> origin_unmatched_conds;
  ObSEArray<int64_t, 4> mv_unmatched_conds;
  ObSEArray<ObRawExpr*, 4> mv_select_exprs;
  ObSEArray<ObRawExpr*, 4> cond_exprs;
  ObSEArray<ObRawExpr*, 4> new_cond_exprs;
  bool is_sub_valid = false;
  is_valid = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.view_stmt_)
      || OB_ISNULL(helper.map_info_) || OB_ISNULL(helper.new_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info), K(helper.map_info_));
  } else if (OB_FAIL(ObStmtComparer::compute_unmatched_item(helper.map_info_->cond_map_,
                                                            origin_stmt->get_condition_size(),
                                                            mv_info.view_stmt_->get_condition_size(),
                                                            origin_unmatched_conds,
                                                            mv_unmatched_conds))) {
    LOG_WARN("failed to compute unmatched item", K(ret));
  } else if (OB_FAIL(cond_exprs.prepare_allocate(origin_unmatched_conds.count()))) {
    LOG_WARN("failed to pre-allocate table map", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_unmatched_conds.count(); ++i) {
    cond_exprs.at(i) = origin_stmt->get_condition_expr(origin_unmatched_conds.at(i));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mv_info.view_stmt_->get_select_exprs(mv_select_exprs))) {
    LOG_WARN("failed to get mv select exprs", K(ret));
  } else if (OB_FAIL(ObStmtComparer::compute_new_expr(cond_exprs,
                                                      origin_stmt,
                                                      mv_select_exprs,
                                                      mv_info.view_stmt_,
                                                      *helper.map_info_,
                                                      helper.compute_expr_copier_,
                                                      new_cond_exprs,
                                                      is_sub_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_sub_valid) {
    LOG_TRACE("condition exprs can not be computed", K(new_cond_exprs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_cond_exprs.count(); ++i) {
      ObRawExpr *cond_expr = NULL;
      if (OB_FAIL(helper.col_copier_.copy_on_replace(new_cond_exprs.at(i), cond_expr))) {
        LOG_WARN("failed to copy expr", K(ret), KPC(new_cond_exprs.at(i)));
      } else if (OB_FAIL(helper.new_stmt_->add_condition_expr(cond_expr))) {
        LOG_WARN("failed to add condition expr", K(ret), KPC(cond_expr));
      }
    }
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::fill_having_exprs(ObSelectStmt *origin_stmt,
                                            const MvInfo &mv_info,
                                            GenerateStmtHelper &helper,
                                            bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> origin_unmatched_conds;
  ObSEArray<int64_t, 4> mv_unmatched_conds;
  ObSEArray<ObRawExpr*, 4> mv_select_exprs;
  ObSEArray<ObRawExpr*, 4> new_having_exprs;
  bool is_sub_valid = false;
  is_valid = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.view_stmt_)
      || OB_ISNULL(helper.map_info_) || OB_ISNULL(helper.new_stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info), K(helper.map_info_));
  } else if (OB_FAIL(mv_info.view_stmt_->get_select_exprs(mv_select_exprs))) {
    LOG_WARN("failed to get mv select exprs", K(ret));
  } else if (OB_FAIL(ObStmtComparer::compute_new_expr(origin_stmt->get_having_exprs(),
                                                      origin_stmt,
                                                      mv_select_exprs,
                                                      mv_info.view_stmt_,
                                                      *helper.map_info_,
                                                      helper.compute_expr_copier_,
                                                      new_having_exprs,
                                                      is_sub_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_sub_valid) {
    LOG_TRACE("having exprs can not be computed", K(new_having_exprs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_having_exprs.count(); ++i) {
      ObRawExpr *having_expr = NULL;
      if (OB_FAIL(helper.col_copier_.copy_on_replace(new_having_exprs.at(i), having_expr))) {
        LOG_WARN("failed to copy expr", K(ret), KPC(new_having_exprs.at(i)));
      } else if (OB_FAIL(helper.new_stmt_->add_having_expr(having_expr))) {
        LOG_WARN("failed to add having expr", K(ret), KPC(having_expr));
      } else if (OB_FAIL(ObTransformUtils::add_aggr_winfun_expr(helper.new_stmt_, having_expr))) {
        LOG_WARN("failed to add aggr winfun expr", K(ret), KPC(having_expr));
      }
    }
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::fill_groupby_exprs(ObSelectStmt *origin_stmt,
                                             const MvInfo &mv_info,
                                             GenerateStmtHelper &helper,
                                             bool &is_valid)
{
  // TODO only mv has no group by
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> mv_select_exprs;
  ObSEArray<ObRawExpr*, 4> new_groupby_exprs;
  ObSEArray<ObRawExpr*, 4> new_rollup_exprs;
  bool is_sub_valid = false;
  is_valid = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(origin_stmt)
      || OB_ISNULL(mv_info.view_stmt_) || OB_ISNULL(helper.new_stmt_)
      || OB_ISNULL(helper.map_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(origin_stmt), K(mv_info), K(helper.new_stmt_), K(helper.map_info_));
  } else if (OB_FAIL(mv_info.view_stmt_->get_select_exprs(mv_select_exprs))) {
    LOG_WARN("failed to get mv select exprs", K(ret));
  } else if (OB_FAIL(ObStmtComparer::compute_new_expr(origin_stmt->get_group_exprs(),
                                                      origin_stmt,
                                                      mv_select_exprs,
                                                      mv_info.view_stmt_,
                                                      *helper.map_info_,
                                                      helper.compute_expr_copier_,
                                                      new_groupby_exprs,
                                                      is_sub_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_sub_valid) {
    LOG_TRACE("group by exprs can not be computed", K(new_groupby_exprs));
  } else if (OB_FAIL(ObStmtComparer::compute_new_expr(origin_stmt->get_rollup_exprs(),
                                                      origin_stmt,
                                                      mv_select_exprs,
                                                      mv_info.view_stmt_,
                                                      *helper.map_info_,
                                                      helper.compute_expr_copier_,
                                                      new_rollup_exprs,
                                                      is_sub_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_sub_valid) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_groupby_exprs.count(); ++i) {
      ObRawExpr *groupby_expr = NULL;
      if (OB_FAIL(helper.col_copier_.copy_on_replace(new_groupby_exprs.at(i), groupby_expr))) {
        LOG_WARN("failed to copy expr", K(ret), KPC(new_groupby_exprs.at(i)));
      } else if (OB_FAIL(helper.new_stmt_->get_group_exprs().push_back(groupby_expr))) {
        LOG_WARN("failed to push back group by expr", K(ret), KPC(groupby_expr));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_rollup_exprs.count(); ++i) {
      ObRawExpr *rollup_expr = NULL;
      if (OB_FAIL(helper.col_copier_.copy_on_replace(new_rollup_exprs.at(i), rollup_expr))) {
        LOG_WARN("failed to copy expr", K(ret), KPC(new_rollup_exprs.at(i)));
      } else if (OB_FAIL(helper.new_stmt_->get_rollup_exprs().push_back(rollup_expr))) {
        LOG_WARN("failed to push back roll up expr", K(ret), KPC(rollup_expr));
      }
    }
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::fill_distinct(ObSelectStmt *origin_stmt,
                                        const MvInfo &mv_info,
                                        GenerateStmtHelper &helper,
                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.view_stmt_)
      || OB_ISNULL(helper.new_stmt_) || OB_ISNULL(helper.map_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info), K(helper.new_stmt_), K(helper.map_info_));
  } else if (!origin_stmt->is_distinct()) { // here we assume that mv will not contain DISTINCT
    is_valid = true;
  } else if (origin_stmt->is_distinct()) {
    helper.new_stmt_->assign_distinct();
    is_valid = true;
  } else {
    is_valid = false;
  }
  return ret;
}

int ObTransformMVRewrite::fill_orderby_exprs(ObSelectStmt *origin_stmt,
                                             const MvInfo &mv_info,
                                             GenerateStmtHelper &helper,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> mv_select_exprs;
  ObSEArray<ObRawExpr*, 4> ori_orderby_exprs;
  ObSEArray<ObRawExpr*, 4> new_orderby_exprs;
  bool is_sub_valid = false;
  is_valid = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.view_stmt_)
      || OB_ISNULL(helper.new_stmt_) || OB_ISNULL(helper.map_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info), K(helper.new_stmt_), K(helper.map_info_));
  } else if (OB_FAIL(origin_stmt->get_order_exprs(ori_orderby_exprs))) {
    LOG_WARN("failed to get order by exprs", K(ret));
  } else if (OB_FAIL(mv_info.view_stmt_->get_select_exprs(mv_select_exprs))) {
    LOG_WARN("failed to get mv select exprs", K(ret));
  } else if (OB_FAIL(ObStmtComparer::compute_new_expr(ori_orderby_exprs,
                                                      origin_stmt,
                                                      mv_select_exprs,
                                                      mv_info.view_stmt_,
                                                      *helper.map_info_,
                                                      helper.compute_expr_copier_,
                                                      new_orderby_exprs,
                                                      is_sub_valid))) {
    LOG_WARN("failed to compute select expr", K(ret));
  } else if (!is_sub_valid) {
    LOG_TRACE("order by exprs can not be computed", K(new_orderby_exprs));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_orderby_exprs.count(); ++i) {
      ObRawExpr *orderby_expr = NULL;
      OrderItem order_item;
      if (OB_FAIL(helper.col_copier_.copy_on_replace(new_orderby_exprs.at(i), orderby_expr))) {
        LOG_WARN("failed to copy expr", K(ret), KPC(new_orderby_exprs.at(i)));
      } else if (OB_FALSE_IT(order_item.expr_ = orderby_expr)) {
      } else if (OB_FALSE_IT(order_item.order_type_ = origin_stmt->get_order_item(i).order_type_)) {
      } else if (OB_FAIL(helper.new_stmt_->add_order_item(order_item))) {
        LOG_WARN("fail to add order item", K(ret), K(order_item));
      } else if (OB_FAIL(ObTransformUtils::add_aggr_winfun_expr(helper.new_stmt_, orderby_expr))) {
        LOG_WARN("failed to add aggr winfun expr", K(ret), KPC(orderby_expr));
      }
    }
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::fill_limit(ObSelectStmt *origin_stmt,
                                     const MvInfo &mv_info,
                                     GenerateStmtHelper &helper,
                                     bool &is_valid)
{
  // TODO we assumed that the materialized view has no LIMIT
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(mv_info.view_stmt_)
      || OB_ISNULL(helper.new_stmt_) || OB_ISNULL(helper.map_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(origin_stmt), K(mv_info), K(helper.new_stmt_), K(helper.map_info_));
  } else if (!origin_stmt->has_limit()) {
    is_valid = true;
  } else {
    helper.new_stmt_->get_limit_expr() = origin_stmt->get_limit_expr();
    helper.new_stmt_->get_offset_expr() = origin_stmt->get_offset_expr();
    helper.new_stmt_->get_limit_percent_expr() = origin_stmt->get_limit_percent_expr();
    helper.new_stmt_->set_has_fetch(origin_stmt->has_fetch());
    helper.new_stmt_->set_fetch_with_ties(origin_stmt->is_fetch_with_ties());
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewrite::expand_rt_mv_table(GenerateStmtHelper &helper)
{
  int ret = OB_SUCCESS;
  ObString src_qb_name;
  int64_t query_rewrite_integrity = QueryRewriteIntegrityType::REWRITE_INTEGRITY_ENFORCED;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(helper.mv_item_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(helper.mv_item_));
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_integrity(query_rewrite_integrity))) {
    LOG_WARN("failed to get query rewrite integrity", K(ret));
  } else if (QueryRewriteIntegrityType::REWRITE_INTEGRITY_ENFORCED != query_rewrite_integrity) {
    // do nothing
  } else if (OB_FALSE_IT(src_qb_name = ctx_->src_qb_name_)) {
  } else if (OB_FALSE_IT(helper.mv_item_->need_expand_rt_mv_ = true)) {
  } else if (OB_FAIL(ObTransformUtils::expand_mview_table(ctx_, helper.new_stmt_, helper.mv_item_))) {
    LOG_WARN("failed to expand mv table", K(ret));
  } else {
    ctx_->src_qb_name_ = src_qb_name;
  }
  return ret;
}

int ObTransformMVRewrite::check_rewrite_expected(const ObSelectStmt *origin_stmt,
                                                 const ObSelectStmt *new_stmt,
                                                 const MvInfo &mv_info,
                                                 bool &is_expected)
{
  int ret = OB_SUCCESS;
  bool hint_rewrite = false;
  bool hint_no_rewrite = false;
  int64_t query_rewrite_enabled = QueryRewriteEnabledType::REWRITE_ENABLED_TRUE;
  bool is_match_index = false;
  is_expected = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(origin_stmt) || OB_ISNULL(new_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(origin_stmt), K(new_stmt));
  } else if (OB_FAIL(check_hint_valid(*origin_stmt, hint_rewrite, hint_no_rewrite))) {
    LOG_WARN("failed to check mv rewrite hint", K(ret));
  } else if (hint_rewrite) {
    is_expected = true;
    OPT_TRACE("hint force mv rewrite, skip cost check");
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_enabled(query_rewrite_enabled))) {
    LOG_WARN("failed to get query rewrite enabled", K(ret));
  } else if (QueryRewriteEnabledType::REWRITE_ENABLED_FORCE == query_rewrite_enabled) {
    is_expected = true;
    OPT_TRACE("system variable force mv rewrite, skip cost check");
  } else if (OB_FAIL(check_condition_match_index(new_stmt, is_match_index))) {
    LOG_WARN("failed to check condition match index", K(ret));
  } else if (!is_match_index) {
    is_expected = false;
    OPT_TRACE("condition does not match index, can not rewrite");
  } else {
    is_expected = true;
  }
  // TODO for the first version, we do not use the cost check
  return ret;
}

int ObTransformMVRewrite::check_condition_match_index(const ObSelectStmt *new_stmt,
                                                      bool &is_match_index)
{
  int ret = OB_SUCCESS;
  is_match_index = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(new_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(new_stmt));
  } else if (0 == new_stmt->get_condition_size()) {
    is_match_index = true;
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_match_index && i < new_stmt->get_condition_size(); ++i) {
    ObSEArray<ObRawExpr*, 8> column_exprs;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(new_stmt->get_condition_expr(i), column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && !is_match_index && j < column_exprs.count(); ++j) {
      ObRawExpr *e = column_exprs.at(j);
      ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(e) || OB_UNLIKELY(!e->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(e))) {
      } else if (OB_FAIL(ObTransformUtils::check_column_match_index(new_stmt,
                                                                    new_stmt,
                                                                    ctx_->sql_schema_guard_,
                                                                    col_expr,
                                                                    is_match_index))) {
        LOG_WARN("failed to check column expr is match index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMVRewrite::add_param_constraint(const ObStmtMapInfo &map_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(append_array_no_dup(ctx_->expr_constraints_, map_info.expr_cons_map_))) {
    LOG_WARN("failed to add param constraint", K(ret));
  } else if (OB_FAIL(append_array_no_dup(ctx_->plan_const_param_constraints_, map_info.const_param_map_))) {
    LOG_WARN("failed to add param constraint", K(ret));
  } else if (OB_FAIL(append_array_no_dup(ctx_->equal_param_constraints_, map_info.equal_param_map_))) {
    LOG_WARN("failed to add param constraint", K(ret));
  }
  return ret;
}


} //namespace sql
} //namespace oceanbase