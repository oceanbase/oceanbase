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
#include "sql/rewrite/ob_transform_mv_rewrite_prepare.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/rewrite/ob_transform_pre_process.h"

namespace oceanbase
{
namespace sql
{

int ObTransformMVRewritePrepare::prepare_mv_rewrite_info(const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  bool need_prepare;
  if (OB_FAIL(need_do_prepare(stmt, need_prepare))) {
    LOG_WARN("failed to check need do prepare", K(ret));
  } else if (!need_prepare) {
    // do nothing
  } else if (OB_FAIL(prepare_mv_info(stmt))) {
    LOG_WARN("failed to prepare mv info", K(ret));
  }
  return ret;
}

int ObTransformMVRewritePrepare::need_do_prepare(const ObDMLStmt *stmt,
                                                 bool &need_prepare)
{
  int ret = OB_SUCCESS;
  bool has_mv = false;
  uint64_t data_version;
  need_prepare = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexprcted null", K(ret), K(ctx_), K(stmt));
  } else if (ctx_->session_info_->get_ddl_info().is_refreshing_mview()) {
    need_prepare = false;
    OPT_TRACE("not a user SQL, skip mv rewrite");
  } else if (stmt->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_1) {
    need_prepare = false;
    OPT_TRACE("optimizer features enable version is lower than 4.3.1, skip mv rewrite");
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(ctx_->session_info_->get_effective_tenant_id(), data_version))) {
    LOG_WARN("failed to get data version", K(ret), K(ctx_->session_info_->get_effective_tenant_id()));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_3_1_0)) {
    // data version lower than 4.3.1 does not have the inner table used to get mv list
    need_prepare = false;
    OPT_TRACE("min data version is lower than 4.3.1, skip mv rewrite");
  } else if (OB_FAIL(check_table_has_mv(stmt, has_mv))) {
    LOG_WARN("failed to check table has mv", K(ret));
  } else if (!has_mv) {
    need_prepare = false;
    OPT_TRACE("table does not have mv, no need to rewrite");
  } else if (OB_FAIL(check_sys_var_and_hint(stmt, need_prepare))) {
    LOG_WARN("failed to check sys var and hint", K(ret));
  } else if (!need_prepare) {
    OPT_TRACE("system variable reject mv rewrite, no need to rewrite");
  } else {
    need_prepare = true;
  }
  return ret;
}

int ObTransformMVRewritePrepare::check_table_has_mv(const ObDMLStmt *stmt,
                                                    bool &has_mv)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  has_mv = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)
      || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(stmt));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret), KPC(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !has_mv && i < stmt->get_table_size(); ++i) {
    const TableItem *table = NULL;
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(table = stmt->get_table_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret), K(i), K(stmt->get_table_size()));
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
  for (int64_t i = 0; OB_SUCC(ret) && !has_mv && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(check_table_has_mv(child_stmts.at(i), has_mv)))) {
      LOG_WARN("failed to check child stmt has mv", K(ret), K(i));
    }
  }
  return ret;
}

int ObTransformMVRewritePrepare::check_sys_var_and_hint(const ObDMLStmt *stmt,
                                                        bool &need_prepare)
{
  int ret = OB_SUCCESS;
  int64_t query_rewrite_enabled = QueryRewriteEnabledType::REWRITE_ENABLED_FALSE;
  need_prepare = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->session_info_->get_query_rewrite_enabled(query_rewrite_enabled))) {
    LOG_WARN("failed to get query rewrite enabled", K(ret));
  } else if (QueryRewriteEnabledType::REWRITE_ENABLED_FALSE != query_rewrite_enabled) {
    need_prepare = true;
  } else if (OB_FAIL(recursive_check_hint(stmt, need_prepare))) {
    LOG_WARN("failed to check hint enabled", K(ret));
  }
  return ret;
}

int ObTransformMVRewritePrepare::recursive_check_hint(const ObDMLStmt *stmt,
                                                      bool &need_prepare)
{
  int ret = OB_SUCCESS;
  need_prepare = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else {
    const ObMVRewriteHint *myhint = static_cast<const ObMVRewriteHint*>(stmt->get_stmt_hint().get_normal_hint(T_MV_REWRITE));
    need_prepare = NULL != myhint && myhint->is_enable_hint(); // has mv_rewrite hint
    ObSEArray<ObSelectStmt*, 4> child_stmts;
    if (!need_prepare && OB_FAIL(stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !need_prepare && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_check_hint(child_stmts.at(i), need_prepare)))) {
        LOG_WARN("failed to recursive check hint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMVRewritePrepare::prepare_mv_info(const ObDMLStmt *root_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> mv_list;
  ObSEArray<uint64_t, 4> intersect_tbl_num;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(get_mv_list(root_stmt, mv_list, intersect_tbl_num))) {
    LOG_WARN("failed to get mv list", K(ret));
  } else if (OB_FAIL(generate_mv_info(mv_list, intersect_tbl_num))) {
    LOG_WARN("failed to generate mv info", K(ret));
  } else if (OB_FAIL(sort_mv_infos())) {
    LOG_WARN("failed to sort mv infos", K(ret));
  } else {
    OPT_TRACE("prepare", ctx_->mv_infos_.count(), "materialized view(s) to perform rewrite");
  }
  return ret;
}

int ObTransformMVRewritePrepare::get_mv_list(const ObDMLStmt *root_stmt,
                                             ObIArray<uint64_t> &mv_list,
                                             ObIArray<uint64_t> &intersect_tbl_num)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = NULL;
  ObSqlString sql;
  sqlclient::ObMySQLResult *mysql_result = NULL;
  ObSqlString table_ids;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(sql_proxy = ctx_->exec_ctx_->get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_), K(sql_proxy));
  } else if (OB_FAIL(get_base_table_id_string(root_stmt, table_ids))) {
    LOG_WARN("failed to get base table id string", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("SELECT MVIEW_ID ID, COUNT(P_OBJ) CNT FROM `%s`.`%s` WHERE TENANT_ID = 0 AND P_OBJ IN (%.*s) GROUP BY MVIEW_ID"
                                     " HAVING NOT EXISTS(SELECT 1 FROM `%s`.`%s` WHERE TENANT_ID = 0 AND MVIEW_ID = ID AND P_OBJ NOT IN (%.*s))", // remove mv which contains table not in query
                                    OB_SYS_DATABASE_NAME, OB_ALL_MVIEW_DEP_TNAME,
                                    static_cast<int>(table_ids.length()), table_ids.ptr(),
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
          int64_t tbl_num = 0;
          if (OB_FAIL(mysql_result->get_int(0L, mv_id))) {
            LOG_WARN("failed to get mv id", K(ret));
          } else if (OB_FAIL(mysql_result->get_int(1L, tbl_num))) {
            LOG_WARN("failed to get intersect table num", K(ret));
          } else if (OB_FAIL(mv_list.push_back(mv_id))) {
            LOG_WARN("failed to push back mv id", K(ret), K(mv_id));
          } else if (OB_FAIL(intersect_tbl_num.push_back(tbl_num))) {
            LOG_WARN("failed to push back intersect table num", K(ret), K(tbl_num));
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          OPT_TRACE("find", mv_list.count(), "materialized view(s)");
        } else if (OB_FAIL(ret)) {
          LOG_WARN("failed to get inner sql result", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformMVRewritePrepare::get_base_table_id_string(const ObDMLStmt *stmt,
                                                          ObSqlString &table_ids)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> table_id_list;
  hash::ObHashSet<uint64_t> visited_id;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(get_all_base_table_id(stmt, table_id_list))) {
    LOG_WARN("failed to get table ids", K(ret));
  } else if (OB_FAIL(visited_id.create(table_id_list.count(), "MvRewrite", "HashNode"))) {
    LOG_WARN("failed to init visited id hash set", K(ret));
  }
  ObCStringHelper helper;
  const char* table_id_str = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_id_list.count(); ++i) {
    int tmp_ret = visited_id.exist_refactored(table_id_list.at(i));
    if (OB_HASH_EXIST == tmp_ret) {
      // do nothing
    } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != tmp_ret)) {
      ret = tmp_ret;
      LOG_WARN("failed to check visited id", K(ret), K(table_id_list.at(i)));
    } else if (OB_FAIL(visited_id.set_refactored(table_id_list.at(i)))) {
      LOG_WARN("failed to push back table ref id", K(ret));
    } else if (i > 0 && OB_FAIL(table_ids.append(","))) {
      LOG_WARN("failed to append comma", K(ret));
    } else if (OB_FAIL(helper.convert(table_id_list.at(i), table_id_str))) {
      LOG_WARN("failed to convert", K(ret));
    } else if (OB_FAIL(table_ids.append(table_id_str))) {
      LOG_WARN("failed to append table id", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(visited_id.destroy())) {
    LOG_WARN("failed to destroy visited id hash set", K(ret));
  }
  return ret;
}

int ObTransformMVRewritePrepare::get_all_base_table_id(const ObDMLStmt *stmt,
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

int ObTransformMVRewritePrepare::generate_mv_info(ObIArray<uint64_t> &mv_list,
                                                  ObIArray<uint64_t> &intersect_tbl_num)
{
  int ret = OB_SUCCESS;
  int64_t query_rewrite_integrity = QueryRewriteIntegrityType::REWRITE_INTEGRITY_ENFORCED;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
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
    } else if (OB_FAIL(quick_rewrite_check(*ctx_->session_info_,
                                           *mv_schema,
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
      } else if (OB_FAIL(ctx_->mv_infos_.push_back(MvInfo(mv_list.at(i),
                                                          data_table_id,
                                                          mv_schema,
                                                          data_table_schema,
                                                          db_schema,
                                                          NULL,
                                                          NULL,
                                                          intersect_tbl_num.at(i))))) {
        LOG_WARN("failed to push back mv info", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformMVRewritePrepare::sort_mv_infos()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else {
    lib::ob_sort(ctx_->mv_infos_.begin(), ctx_->mv_infos_.end());
  }
  return ret;
}

int ObTransformMVRewritePrepare::quick_rewrite_check(const ObSQLSessionInfo &session_info,
                                                     const ObTableSchema &mv_schema,
                                                     bool allow_stale,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_FAIL(ObMVProvider::check_mview_dep_session_vars(mv_schema, session_info, false, is_valid))) {
    LOG_WARN("failed to check mview dep session vars", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (!mv_schema.mv_enable_query_rewrite()) {
    is_valid = false;
  } else if (!allow_stale && (!mv_schema.mv_on_query_computation()
                              || mv_schema.is_mv_cnt_proctime_table())) {
    is_valid = false;
  } else {
    is_valid = true;
  }
  return ret;
}

int ObTransformMVRewritePrepare::generate_mv_stmt(MvInfo &mv_info,
                                                  ObTransformerCtx *ctx,
                                                  ObQueryCtx *temp_query_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_)
      || OB_ISNULL(ctx->session_info_) || OB_ISNULL(mv_info.mv_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mv_info is null", K(ret), K(ctx), KPC(mv_info.mv_schema_));
  } else {
    ObString view_definition;
    ObSqlString mv_sql; // select * from mv;
    if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(*ctx->allocator_,
                                                                 ctx->session_info_->get_local_collation_connection(),
                                                                 mv_info.mv_schema_->get_view_schema(),
                                                                 view_definition))) {
      LOG_WARN("fail to generate view definition for resolve", K(ret));
    } else if (OB_FAIL(resolve_temp_stmt(view_definition, ctx, temp_query_ctx, mv_info.view_stmt_))) {
      LOG_WARN("failed to resolve mv define stmt", K(ret), K(view_definition));
    } else if (OB_FAIL(mv_sql.assign_fmt(lib::is_oracle_mode() ?
                        "SELECT * FROM \"%.*s\".\"%.*s\"" :
                        "SELECT * FROM `%.*s`.`%.*s`",
                        mv_info.db_schema_->get_database_name_str().length(), mv_info.db_schema_->get_database_name_str().ptr(),
                        mv_info.mv_schema_->get_table_name_str().length(), mv_info.mv_schema_->get_table_name_str().ptr()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(resolve_temp_stmt(ObString::make_string(mv_sql.ptr()), ctx, temp_query_ctx, mv_info.select_mv_stmt_))) {
      LOG_WARN("failed to resolve mv define stmt", K(ret), K(mv_sql));
    } else {
      LOG_DEBUG("generate mv stmt", KPC(mv_info.view_stmt_), KPC(mv_info.select_mv_stmt_));
    }
    OPT_TRACE("generate stmt for", mv_info.mv_schema_->get_table_name(), ":", mv_info.view_stmt_);
  }
  return ret;
}

int ObTransformMVRewritePrepare::resolve_temp_stmt(const ObString &sql_string,
                                                   ObTransformerCtx *ctx,
                                                   ObQueryCtx *query_ctx,
                                                   ObSelectStmt *&output_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(query_ctx) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx), K(query_ctx));
  } else {
    SMART_VARS_2((ObTransformerCtx, trans_ctx), (ObResolverParams, resolver_ctx)) {
      ObParser parser(*ctx->allocator_, ctx->session_info_->get_sql_mode(), ctx->session_info_->get_charsets4parser());
      ParseResult parse_result;
      ParseNode *node = NULL;
      ObDMLStmt *dml_stmt = NULL;
      uint64_t dummy_value = 0;
      ObIAllocator &alloc = *ctx->allocator_;
      ObQueryCtx *old_query_ctx = ctx->expr_factory_->get_query_ctx();
      resolver_ctx.allocator_ = ctx->allocator_;
      resolver_ctx.schema_checker_ = ctx->schema_checker_;
      resolver_ctx.session_info_ = ctx->session_info_;
      resolver_ctx.expr_factory_ = ctx->expr_factory_;
      resolver_ctx.stmt_factory_ = ctx->stmt_factory_;
      resolver_ctx.sql_proxy_ = GCTX.sql_proxy_;
      resolver_ctx.query_ctx_ = query_ctx;
      resolver_ctx.is_mview_definition_sql_ = true;
      resolver_ctx.is_for_rt_mv_ = true;
      resolver_ctx.expr_factory_->set_query_ctx(resolver_ctx.query_ctx_);
      trans_ctx = *ctx;
      trans_ctx.reset();
      ObSelectResolver select_resolver(resolver_ctx);
      ObTransformPreProcess transform_pre_process(&trans_ctx);
      transform_pre_process.set_transformer_type(PRE_PROCESS);
      STOP_OPT_TRACE;
      if (OB_FAIL(parser.parse(sql_string, parse_result))) {
        LOG_WARN("parse view definition failed", K(sql_string), K(ret));
      } else if (OB_ISNULL(node = parse_result.result_tree_->children_[0]) || OB_UNLIKELY(T_SELECT != node->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mv select node", K(ret), K(node));
      } else if (OB_FALSE_IT(resolver_ctx.query_ctx_->question_marks_count_
                             = static_cast<int64_t>(parse_result.question_mark_ctx_.count_))) {
      } else if (OB_FAIL(select_resolver.resolve(*node))) {
        LOG_WARN("resolve view definition failed", K(ret));
      } else if (OB_ISNULL(dml_stmt = static_cast<ObDMLStmt*>(select_resolver.get_basic_stmt()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mv stmt", K(ret), K(dml_stmt));
      } else if (OB_FAIL(resolver_ctx.query_ctx_->query_hint_.init_query_hint(resolver_ctx.allocator_,
                                                                              resolver_ctx.session_info_,
                                                                              resolver_ctx.global_hint_,
                                                                              dml_stmt))) {
        LOG_WARN("failed to init query hint.", K(ret));
      } else if (OB_FAIL(resolver_ctx.query_ctx_->query_hint_.set_params_from_hint(resolver_ctx))) {
        LOG_WARN("failed to check and set params from hint", K(ret));
      } else if (OB_FAIL(transform_pre_process.transform(dml_stmt, dummy_value))) {
        LOG_WARN("failed to do transform pre process", K(ret), KPC(dml_stmt));
      } else if (OB_ISNULL(output_stmt = static_cast<ObSelectStmt*>(dml_stmt))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid mv stmt", K(ret), K(dml_stmt));
      }
      resolver_ctx.expr_factory_->set_query_ctx(old_query_ctx);
      RESUME_OPT_TRACE;
    }
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
