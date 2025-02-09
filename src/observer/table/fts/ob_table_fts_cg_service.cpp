/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "observer/table/ob_table_cg_service.h"
#include "share/datum/ob_datum_util.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/das/ob_das_ir_define.h"
#include "ob_table_fts_cg_service.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{

int ObTableFtsExprCgService::fill_doc_id_expr_param(ObTableCtx &ctx, ObRawExpr *&doc_id_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *calc_tablet_id_expr = nullptr;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  if (OB_ISNULL(table_schema) && OB_ISNULL(doc_id_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(table_schema), KP(doc_id_expr));
  } else if (OB_UNLIKELY(T_FUN_SYS_DOC_ID != doc_id_expr->get_expr_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not doc id expr", K(ret), "expr type", doc_id_expr->get_expr_type());
  } else if (OB_FAIL(ObTableExprCgService::generate_calc_tablet_id_expr(ctx, *table_schema, calc_tablet_id_expr))) {
    LOG_WARN("fail to generate calc tablet id expr", K(ret));
  } else if (OB_ISNULL(calc_tablet_id_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc_tablet_id expr is NULL", K(ret));
  } else {
    ObSQLSessionInfo &sess_info = ctx.get_session_info();
    ObSysFunRawExpr *expr = static_cast<ObSysFunRawExpr *>(doc_id_expr);
    if (OB_FAIL(expr->add_param_expr(calc_tablet_id_expr))) {
      LOG_WARN("fail to add param expr", K(ret), KP(calc_tablet_id_expr));
    } else if (OB_FAIL(expr->formalize(&sess_info))) {
      LOG_WARN("fail to formalize", K(ret), K(sess_info));
    }
  }

  return ret;
}

bool ObTableFtsExprCgService::need_calc_doc_id(ObTableCtx &ctx)
{
  ObTableOperationType::Type op_type = ctx.get_opertion_type();
  return ObTableOperationType::Type::INSERT == op_type ||
         ObTableOperationType::Type::INSERT_OR_UPDATE == op_type ||
         ObTableOperationType::Type::REPLACE == op_type ||
         ctx.is_inc_append_insert();
}

int ObTableFtsExprCgService::generate_text_retrieval_dep_exprs(ObTableCtx &ctx, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  ObTextRetrievalInfo *tr_info = nullptr;
  ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();
  const ObTableSchema *table_schema = ctx.get_table_schema();
  const ObTableSchema *inv_index_schema = nullptr;
  ObRawExprFactory &expr_factory = ctx.get_expr_factory();
  ObSQLSessionInfo &session_info = ctx.get_session_info();
  ObColumnRefRawExpr *token_column = nullptr;
  ObColumnRefRawExpr *token_cnt_column = nullptr;
  ObColumnRefRawExpr *doc_length_column = nullptr;
  ObColumnRefRawExpr *doc_id_column = nullptr;
  ObAggFunRawExpr *related_doc_cnt = nullptr;
  ObAggFunRawExpr *total_doc_cnt = nullptr;
  ObAggFunRawExpr *doc_token_cnt = nullptr;
  ObOpRawExpr *relevance_expr = nullptr;
  ObMatchFunRawExpr *match_expr = nullptr;
  ObRawExpr *pushdown_match_filter = nullptr;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(table_schema) || OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is NULL", K(ret), KP(schema_guard), KP(table_schema), KP(fts_ctx));
  } else if (OB_FAIL(ctx.prepare_text_retrieval_scan())) {
    LOG_WARN("fail to prepare text retrieval scan", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("text_retrieval_info is NULL", K(ret));
  } else if (OB_FAIL(generate_match_against_exprs(ctx, match_expr, pushdown_match_filter))) {
    LOG_WARN("fail to generate match against expr", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ctx.get_tenant_id(), tr_info->inv_idx_tid_, inv_index_schema))) {
    LOG_WARN("failed to get inv_index_schema schema", K(ret));
  } else {
    // may have fulltext index more than one, so that we should get the related column by inv_index_schema
    ObRawExprCopier copier(expr_factory);
    for (int64_t i = 0; OB_SUCC(ret) && i < inv_index_schema->get_column_count(); ++i) {
      const ObColumnSchemaV2 *col_schema = inv_index_schema->get_column_schema_by_idx(i);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else {
        const ObColumnSchemaV2 *col_schema_in_data_table = table_schema->get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(col_schema_in_data_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, column schema is nullptr in data table", K(ret), KPC(col_schema), KPC(table_schema));
        } else if (col_schema_in_data_table->is_doc_id_column()) {
          if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, doc_id_column))) {
            LOG_WARN("failed to build doc id column expr", K(ret));
          }
        } else if (col_schema_in_data_table->is_word_count_column()) {
          if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, token_cnt_column))) {
            LOG_WARN("failed to build doc id column expr", K(ret));
          } else if (OB_NOT_NULL(token_cnt_column)) {
            token_cnt_column->set_ref_id(ctx.get_ref_table_id(), col_schema->get_column_id());
            token_cnt_column->set_column_attr(ctx.get_table_name(), col_schema->get_column_name_str());
            token_cnt_column->set_database_name(ctx.get_database_name());
          }
        } else if (col_schema_in_data_table->is_word_segment_column()) {
          if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, token_column))) {
            LOG_WARN("failed to build doc id column expr", K(ret));
          } else if (OB_NOT_NULL(token_column)) {
            token_column->set_ref_id(ctx.get_ref_table_id(), col_schema->get_column_id());
            token_column->set_column_attr(ctx.get_table_name(), col_schema->get_column_name_str());
            token_column->set_database_name(ctx.get_database_name());
          }
        } else if (col_schema_in_data_table->is_doc_length_column()) {
          if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, doc_length_column))) {
            LOG_WARN("failed to build doc id column expr", K(ret));
          } else if (OB_NOT_NULL(doc_length_column)) {
            doc_length_column->set_ref_id(ctx.get_ref_table_id(), col_schema->get_column_id());
            doc_length_column->set_column_attr(ctx.get_table_name(), col_schema->get_column_name_str());
            doc_length_column->set_database_name(ctx.get_database_name());
          }
        }
      }
    } // end for

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(token_cnt_column) || OB_ISNULL(token_column) || OB_ISNULL(doc_id_column) ||
               OB_ISNULL(doc_length_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null fulltext generated column", K(ret),
          KP(token_cnt_column), KP(token_column), KP(doc_id_column));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, related_doc_cnt))) {
      LOG_WARN("failed to create related doc cnt agg expr", K(ret));
    } else if (OB_FAIL(related_doc_cnt->add_real_param_expr(token_cnt_column))) {
      LOG_WARN("failed to set agg param", K(ret));
    } else if (OB_FAIL(related_doc_cnt->formalize(&session_info))) {
      LOG_WARN("failed to formalize related doc cnt expr", K(ret));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, total_doc_cnt))) {
      LOG_WARN("failed to create related doc cnt agg expr", K(ret));
    } else if (OB_FAIL(total_doc_cnt->add_real_param_expr(doc_id_column))) {
      LOG_WARN("failed to set agg param", K(ret));
    } else if (OB_FAIL(total_doc_cnt->formalize(&session_info))) {
      LOG_WARN("failed to formalize total doc cnt expr", K(ret));
    } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SUM, doc_token_cnt))) {
      LOG_WARN("failed to create document token count sum agg expr", K(ret));
    } else if (OB_FAIL(doc_token_cnt->add_real_param_expr(token_cnt_column))) {
      LOG_WARN("failed to set agg param", K(ret));
    } else if (OB_FAIL(doc_token_cnt->formalize(&session_info))) {
      LOG_WARN("failed to formalize document token count expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_bm25_expr(expr_factory, related_doc_cnt,
                                                      token_cnt_column, total_doc_cnt,
                                                      doc_token_cnt, relevance_expr,
                                                      &session_info))) {
      LOG_WARN("failed to build bm25 expr", K(ret));
    } else if (OB_FAIL(relevance_expr->formalize(&session_info))) {
      LOG_WARN("failed to formalize bm25 expr", K(ret));
    // Copy column ref expr referenced by aggregation in different index table scan
    // to avoid share expression
    } else if (OB_FAIL(copier.copy(related_doc_cnt->get_param_expr(0)))) {
      LOG_WARN("fail to copy related doc cnt", K(ret));
    } else if (OB_FAIL(copier.copy(total_doc_cnt->get_param_expr(0)))) {
      LOG_WARN("fail to copy total doc cnt", K(ret));
    } else if (OB_FAIL(copier.copy(doc_token_cnt->get_param_expr(0)))) {
      LOG_WARN("fail to copy total doc cnt", K(ret));
    } else {
      tr_info->match_expr_ = match_expr;
      tr_info->pushdown_match_filter_ = pushdown_match_filter;
      tr_info->token_column_ = token_column;
      tr_info->token_cnt_column_ = token_cnt_column;
      tr_info->doc_id_column_ = doc_id_column;
      tr_info->doc_length_column_ = doc_length_column;
      tr_info->related_doc_cnt_ = related_doc_cnt;
      tr_info->doc_token_cnt_ = doc_token_cnt;
      tr_info->total_doc_cnt_ = total_doc_cnt;
      tr_info->relevance_expr_ = relevance_expr;
    }
  }
  return ret;
}

int ObTableFtsExprCgService::generate_topn_related_params(ObTableCtx &ctx, ObTextRetrievalInfo &tr_info)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory &expr_factory = ctx.get_expr_factory();
  ObConstRawExpr *limit_expr = nullptr;
  ObConstRawExpr *offset_expr = nullptr;
  if (OB_FAIL(expr_factory.create_raw_expr(T_INT, limit_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_INT, offset_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(limit_expr) || OB_ISNULL(offset_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret), KP(limit_expr), KP(offset_expr));
  } else {
    ObObj obj;
    obj.set_int(ObIntType, -1);
    limit_expr->set_value(obj);
    obj.set_int(ObIntType, 0);
    offset_expr->set_value(obj);
    if (OB_FAIL(limit_expr->formalize(&ctx.get_session_info()))) {
      LOG_WARN("fail to formalize limit_expr", K(ret));
    } else if (OB_FAIL(offset_expr->formalize(&ctx.get_session_info()))) {
      LOG_WARN("fail to formalize limit_expr", K(ret));
    } else {
      tr_info.topk_limit_expr_ = limit_expr;
      tr_info.topk_offset_expr_ = offset_expr;
      tr_info.sort_key_.expr_ = tr_info.match_expr_;
      tr_info.sort_key_.order_type_ = default_desc_direction();
      tr_info.with_ties_ = false;
    }
  }
  return ret;
}

int ObTableFtsExprCgService::generate_match_against_exprs(ObTableCtx &ctx,
                                                       ObMatchFunRawExpr *&match_expr,
                                                       ObRawExpr *&pushdown_match_filter)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  ObMatchFunRawExpr *match_against = nullptr;
  ObConstRawExpr *search_keywords = nullptr;
  ObRawExprFactory &expr_factory = ctx.get_expr_factory();
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_MATCH_AGAINST, match_against))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(match_against)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_VARCHAR, search_keywords))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_FAIL(search_keywords->extract_info())) {
    LOG_WARN("failed to extract info", K(ret));
  } else if (!search_keywords->is_static_const_expr()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-const search query");
    LOG_WARN("search query is not const expr", K(ret));
  } else if (OB_ISNULL(search_keywords)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    match_against->set_search_key(search_keywords);
    match_against->set_mode_flag(ObMatchAgainstMode::NATURAL_LANGUAGE_MODE);
  }
  // add column ref expr to match against
  if (OB_SUCC(ret)) {
    ObSEArray<uint64_t, 4> indexed_column_ids;
    ObTextRetrievalInfo *tr_info = fts_ctx->get_text_retrieval_info();
    const ObTableSchema *table_schema = ctx.get_table_schema();
    uint64_t doc_id_col_id = OB_INVALID_ID;
    uint64_t ft_col_id = OB_INVALID_ID;
    const ObColumnSchemaV2 *word_seg_col_schema = nullptr;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is NULL", K(ret));
    } else if (OB_ISNULL(tr_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ObTextRetrievalInfo is NULL", K(ret));
    } else if (OB_FAIL(table_schema->get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
      LOG_WARN("fail to get fulltext column ids", K(ret));
    } else if (OB_ISNULL(word_seg_col_schema = table_schema->get_column_schema(ft_col_id))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("word_seg_col_schema is NULL", K(ret));
    } else if (OB_FAIL(word_seg_col_schema->get_cascaded_column_ids(indexed_column_ids))) {
      LOG_WARN("failed to get cascaded column ids", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < indexed_column_ids.count(); i++) {
        const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(indexed_column_ids.at(i));
        ObColumnRefRawExpr *column_ref_expr = nullptr;
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is NULL", K(ret), K(indexed_column_ids.at(i)), K(i));
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, column_ref_expr))) {
          LOG_WARN("failed to build doc id column expr", K(ret));
        } else if (OB_FAIL(match_against->get_match_columns().push_back(column_ref_expr))) {
          LOG_WARN("fail to push back column ref expr", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(match_against->extract_info())) {
      LOG_WARN("failed to extract info", K(ret));
    } else if (OB_FAIL(match_against->formalize(&ctx.get_session_info()))) {
      LOG_WARN("fail to formalize match against", K(ret));
    } else {
      match_expr = match_against;
    }
  }
  // construct pushdown match filter:
  //  T_OP_BOOL
  //     |
  // T_FUN_MATCH_AGAINST
  if (OB_SUCC(ret)) {
    ObRawExpr *bool_expr = nullptr;
    if (OB_FAIL(ObRawExprUtils::try_create_bool_expr(match_against, bool_expr, expr_factory))) {
      LOG_WARN("try create bool expr failed", K(ret));
    } else if (OB_FAIL(bool_expr->formalize(&ctx.get_session_info()))) {
      LOG_WARN("fail to formalize match against", K(ret));
    } else {
      pushdown_match_filter = bool_expr;
    }
  }
  return ret;
}

int ObTableFtsExprCgService::add_all_text_retrieval_scan_dep_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();
  ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  ObTextRetrievalInfo *tr_info = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->match_expr_))) {
    LOG_WARN("fail to push back match_expr", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->pushdown_match_filter_))) {
    LOG_WARN("fail to push back pushdown_match_filter", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->token_column_))) {
    LOG_WARN("fail to push back token_column", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->token_cnt_column_))) {
    LOG_WARN("fail to push back token_cnt_column", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->doc_id_column_))) {
    LOG_WARN("fail to push back doc_id_column", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->doc_length_column_))) {
    LOG_WARN("fail to push back doc_length_column", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->related_doc_cnt_))) {
    LOG_WARN("fail to push back related_doc_cnt", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->doc_token_cnt_))) {
    LOG_WARN("fail to push back doc_token_cnt", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->total_doc_cnt_))) {
    LOG_WARN("fail to push back total_doc_cnt", K(ret));
  } else if (OB_FAIL(all_exprs.push_back(tr_info->relevance_expr_))) {
    LOG_WARN("fail to push back relevance_expr", K(ret));
  }
  return ret;
}

int ObTableFtsDmlCgService::generate_rowkey_doc_das_ctdef(ObTableCtx &ctx, ObDASScanCtDef &rowkey_doc_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> rowkey_doc_column_exprs;
  ObSEArray<ObRawExpr*, 4> rowkey_doc_column_ids;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTableSchema *rowkey_doc_schema = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(rowkey_doc_schema = fts_ctx->get_rowkey_doc_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey doc schema is NULL", K(ret));
  } else if (OB_FAIL(ObTableFtsTscCgService::extract_rowkey_doc_exprs(ctx, rowkey_doc_column_exprs))) {
    LOG_WARN("fail to extrace rowkey doc_id exprs", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(rowkey_doc_column_exprs, rowkey_doc_scan_ctdef.pd_expr_spec_.access_exprs_))) {
    LOG_WARN("fail to generate rowkey doc rt exprs", K(ret));
  } else if (OB_FAIL(rowkey_doc_scan_ctdef.access_column_ids_.init(rowkey_doc_column_exprs.count()))) {
    LOG_WARN("fail to init access column ids", K(ret), K(rowkey_doc_column_exprs.count()));
  } else  {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_doc_column_exprs.count(); i++) {
      ObRawExpr *raw_expr = rowkey_doc_column_exprs.at(i);
      if (OB_ISNULL(raw_expr) || !raw_expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), KPC(raw_expr), K(i));
      } else {
        ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
        if (OB_FAIL(rowkey_doc_scan_ctdef.access_column_ids_.push_back(col_ref_expr->get_column_id()))) {
          LOG_WARN("fail to push back column id", K(ret));
        }
      }
    } // end for

    if (OB_SUCC(ret)) {
      rowkey_doc_scan_ctdef.ref_table_id_ = rowkey_doc_schema->get_table_id();
      if (OB_FAIL(ObTableTscCgService::generate_table_param(ctx, rowkey_doc_scan_ctdef))) {
        LOG_WARN("fail to generate rowkey doc table param", K(ret));
      }
    }
  }

  return ret;
}

int ObTableFtsDmlCgService::generate_rowkey_doc_ctdef(ObTableCtx &ctx,
                                                   ObIAllocator &allocator,
                                                   ObDASAttachSpec &attach_spec,
                                                   ObDASScanCtDef *&rowkey_doc_scan_ctdef)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTableSchema *rowkey_doc_schema = nullptr;
  ObDASScanCtDef *scan_ctdef = nullptr;
  ObDASTableLocMeta *loc_meta = nullptr;
  uint64_t rowkey_doc_tid = OB_INVALID_ID;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(rowkey_doc_schema = fts_ctx->get_rowkey_doc_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey doc schema", K(ret), K(rowkey_doc_tid));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, allocator, scan_ctdef))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else if (OB_ISNULL(scan_ctdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey doc ctdef is NULL", K(ret));
  } else if (OB_FAIL(generate_rowkey_doc_das_ctdef(ctx, *scan_ctdef))) {
    LOG_WARN("fail to init rowkey doc ctdef", K(ret));
  } else if (OB_ISNULL(loc_meta = OB_NEWx(ObDASTableLocMeta, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate rowkey doc scan location meta failed", K(ret));
  } else if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(ctx, *rowkey_doc_schema, *loc_meta, nullptr))) {
    LOG_WARN("fail to generate table loc meta", K(ret));
  } else if (OB_FAIL(attach_spec.attach_loc_metas_.push_back(loc_meta))) {
    LOG_WARN("store scan loc meta failed", K(ret));
  } else {
    rowkey_doc_scan_ctdef = scan_ctdef;
  }
  return ret;
}

int ObTableFtsDmlCgService::generate_scan_with_doc_id_ctdef_if_need(ObTableCtx &ctx,
                                                                 ObIAllocator &allocator,
                                                                 ObDASScanCtDef &scan_ctdef,
                                                                 ObDASAttachSpec &attach_spec)
{
  int ret = OB_SUCCESS;

  bool need_doc_id_merge_iter = ctx.has_fts_index();
  ObArray<ObExpr*> result_outputs;
  ObDASDocIdMergeCtDef *doc_id_merge_ctdef = nullptr;
  ObDASScanCtDef *rowkey_doc_scan_ctdef = nullptr;
  if (!need_doc_id_merge_iter) {
    // just skip, nothing to do
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_DOC_ID_MERGE, allocator, doc_id_merge_ctdef))) {
    LOG_WARN("fail to allocate to doc id merge ctdef", K(ret));
  } else if (OB_ISNULL(doc_id_merge_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate doc id merge ctdef child array memory", K(ret));
  } else if (OB_FAIL(generate_rowkey_doc_ctdef(ctx, allocator, attach_spec, rowkey_doc_scan_ctdef))) {
    LOG_WARN("fail to generate rowkey doc ctdef", K(ret));
  } else if (OB_FAIL(result_outputs.assign(scan_ctdef.result_output_))) {
    LOG_WARN("construct aux lookup ctdef failed", K(ret));
  } else if (OB_UNLIKELY(result_outputs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, result outputs is nullptr", K(ret));
  } else {
    doc_id_merge_ctdef->children_cnt_ = 2;
    doc_id_merge_ctdef->children_[0] = &scan_ctdef;
    doc_id_merge_ctdef->children_[1] = rowkey_doc_scan_ctdef;
    if (OB_FAIL(doc_id_merge_ctdef->result_output_.assign(result_outputs))) {
      LOG_WARN("fail to assign result output", K(ret));
    } else {
      attach_spec.attach_ctdef_ = static_cast<ObDASBaseCtDef *>(doc_id_merge_ctdef);
    }
  }
  return ret;
}

int ObTableFtsTscCgService::extract_text_ir_access_columns(const ObTableCtx &ctx,
                                                           ObDASScanCtDef &scan_ctdef,
                                                           ObIArray<ObRawExpr *> &access_exprs) {
  int ret = OB_SUCCESS;
  uint64_t scan_table_id = scan_ctdef.ref_table_id_;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTextRetrievalInfo *tr_info = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else if (scan_table_id == fts_ctx->get_doc_rowkey_tid()) {
    if (OB_FAIL(extract_doc_rowkey_exprs(ctx, access_exprs))) {
      LOG_WARN("fail to extract doc rowkey exprs", K(ret));
    }
  } else {
    switch (scan_ctdef.ir_scan_type_) {
      case ObTSCIRScanType::OB_IR_INV_IDX_SCAN:
        if (OB_ISNULL(tr_info->token_cnt_column_) || OB_ISNULL(tr_info->doc_id_column_) || OB_ISNULL(tr_info->doc_length_column_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inv index expr is NULL", KP(tr_info->token_cnt_column_), KP(tr_info->doc_id_column_),
                    KP(tr_info->doc_length_column_), K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(tr_info->token_cnt_column_)))) {
          LOG_WARN("failed to push token cnt column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(tr_info->doc_id_column_)))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, static_cast<ObRawExpr*>(tr_info->doc_length_column_)))) {
          LOG_WARN("failed to add document length column to access exprs", K(ret));
        }
        break;
      case ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG:
        if (OB_ISNULL(tr_info->total_doc_cnt_) || OB_ISNULL(tr_info->total_doc_cnt_->get_param_expr((0)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("doc id index agg expr is NULL", K(ret), KP(tr_info->total_doc_cnt_));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, tr_info->total_doc_cnt_->get_param_expr((0))))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        }
        break;
      case ObTSCIRScanType::OB_IR_INV_IDX_AGG:
        if (OB_ISNULL(tr_info->related_doc_cnt_) || OB_ISNULL(tr_info->related_doc_cnt_->get_param_expr((0)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("inv index agg expr is NULL", K(ret), KP(tr_info->related_doc_cnt_));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, tr_info->related_doc_cnt_->get_param_expr(0)))) {
          LOG_WARN("failed to push token cnt column to access exprs", K(ret));
        }
        break;
      case ObTSCIRScanType::OB_IR_FWD_IDX_AGG:
        if (OB_ISNULL(tr_info->doc_token_cnt_) || OB_ISNULL(tr_info->doc_token_cnt_->get_param_expr((0)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fwd index agg expr is NULL", K(ret), KP(tr_info->doc_token_cnt_));
        } else if (OB_FAIL(add_var_to_array_no_dup(access_exprs, tr_info->doc_token_cnt_->get_param_expr(0)))) {
          LOG_WARN("failed to push token cnt column to access exprs", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected text ir scan type", K(ret), K(scan_ctdef));
    }
  }
  return ret;
}

int ObTableFtsTscCgService::extract_text_ir_das_output_column_ids(const ObTableCtx &ctx,
                                                               ObDASScanCtDef &scan_ctdef,
                                                               ObIArray<uint64_t> &tsc_out_cols)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTextRetrievalInfo *tr_info = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else if (scan_ctdef.ref_table_id_ == fts_ctx->get_doc_rowkey_tid()) {
    if (OB_FAIL(tsc_out_cols.assign(scan_ctdef.access_column_ids_))) {
      LOG_WARN("fail to assgin tsc_out_cols", K(ret), K(scan_ctdef.access_column_ids_));
    }
  } else if (ObTSCIRScanType::OB_IR_INV_IDX_SCAN == scan_ctdef.ir_scan_type_) {
    if (OB_ISNULL(tr_info->token_cnt_column_) || OB_ISNULL(tr_info->doc_id_column_) || OB_ISNULL(tr_info->doc_id_column_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tr_info exprs is NULL", K(ret), KP(tr_info->token_cnt_column_),
                KP(tr_info->doc_id_column_), KP(tr_info->doc_id_column_));
    } else if (OB_FAIL(tsc_out_cols.push_back(
        static_cast<ObColumnRefRawExpr *>(tr_info->token_cnt_column_)->get_column_id()))) {
      LOG_WARN("failed to push output token cnt col id", K(ret));
    } else if (OB_FAIL(tsc_out_cols.push_back(
        static_cast<ObColumnRefRawExpr *>(tr_info->doc_id_column_)->get_column_id()))) {
      LOG_WARN("failed to push output doc id col id", K(ret));
    } else if (OB_FAIL(tsc_out_cols.push_back(
        static_cast<ObColumnRefRawExpr *>(tr_info->doc_length_column_)->get_column_id()))) {
      LOG_WARN("failed to push output doc length col id", K(ret));
    }
  }
  return ret;
}

int ObTableFtsTscCgService::get_fts_schema(const ObTableCtx &ctx, uint64_t table_id, const ObTableSchema *&index_schema)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTextRetrievalInfo *tr_info = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else {
    if (table_id == tr_info->doc_id_idx_tid_) {
      index_schema = fts_ctx->get_doc_rowkey_schema();
    } else if (table_id == tr_info->inv_idx_tid_) {
      index_schema = ctx.get_index_schema();
    } else if (table_id == tr_info->fwd_idx_tid_) {
      ObSchemaGetterGuard *schema_guard = const_cast<ObSchemaGetterGuard*>(ctx.get_schema_guard());
      if (OB_ISNULL(schema_guard)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tr_info is NULL", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(ctx.get_tenant_id(), table_id, index_schema))) {
        LOG_WARN("fail to get rowkey doc table schema", K(ret), K(table_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table id", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObTableFtsTscCgService::generate_text_ir_pushdown_expr_ctdef(const ObTableCtx &ctx, ObDASScanCtDef &scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const uint64_t scan_table_id = scan_ctdef.ref_table_id_;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTextRetrievalInfo *tr_info = nullptr;
  if (!scan_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown()) {
    // this das scan do not need aggregate pushdown
  } else if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else {
    ObSEArray<ObAggFunRawExpr *, 2> agg_expr_arr;
    switch (scan_ctdef.ir_scan_type_) {
      case ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG:
        if (OB_FAIL(add_var_to_array_no_dup(agg_expr_arr, tr_info->total_doc_cnt_))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        }
        break;
      case ObTSCIRScanType::OB_IR_INV_IDX_AGG:
        if (OB_FAIL(add_var_to_array_no_dup(agg_expr_arr, tr_info->related_doc_cnt_))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        }
        break;
      case ObTSCIRScanType::OB_IR_FWD_IDX_AGG:
        if (OB_FAIL(add_var_to_array_no_dup(agg_expr_arr, tr_info->doc_token_cnt_))) {
          LOG_WARN("failed to push document id column to access exprs", K(ret));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected text ir scan type with aggregate", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scan_ctdef.aggregate_column_ids_.init(agg_expr_arr.count()))) {
      LOG_WARN("failed to init aggregate column ids", K(ret), K(agg_expr_arr.count()));
    } else if (OB_FAIL(scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_.reserve(agg_expr_arr.count()))) {
      LOG_WARN("failed to reserve memory for aggregate output expr array", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < agg_expr_arr.count(); ++i) {
      ObAggFunRawExpr *agg_expr = agg_expr_arr.at(i);
      ObExpr *expr = nullptr;
      ObRawExpr *param_expr = nullptr;
      ObColumnRefRawExpr *param_col_expr = nullptr;
      if (OB_ISNULL(agg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected agg expr", K(ret), KPC(agg_expr));
      } else if (OB_FAIL(cg.generate_rt_expr(*agg_expr, expr))) {
        LOG_WARN("failed to generate runtime expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate runtime expr", K(ret), KPC(agg_expr));
      } else if (OB_FAIL(scan_ctdef.pd_expr_spec_.pd_storage_aggregate_output_.push_back(expr))) {
        LOG_WARN("failed to append expr to aggregate output", K(ret));
      } else if (OB_UNLIKELY(agg_expr->get_real_param_exprs().empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected count all agg expr in text retrieval scan", K(ret));
      } else if (OB_ISNULL(param_expr = agg_expr->get_param_expr(0))
          || OB_UNLIKELY(!param_expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected agg param expr type", K(ret), KPC(param_expr));
      } else if (OB_ISNULL(param_col_expr = static_cast<ObColumnRefRawExpr *>(param_expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null param column expr", K(ret));
      } else if (OB_UNLIKELY(param_col_expr->get_table_id() != ctx.get_ref_table_id() && !param_col_expr->is_doc_id_column())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexoected column to aggregate", K(ret), KPC(param_col_expr), K(ctx.get_ref_table_id() ));
      } else if (OB_FAIL(scan_ctdef.aggregate_column_ids_.push_back(param_col_expr->get_column_id()))) {
        LOG_WARN("failed to append aggregate column ids", K(ret));
      }
    }
  }
  return ret;
}

int ObTableFtsTscCgService::generate_text_ir_ctdef(const ObTableCtx &ctx,
                                                ObIAllocator &allocator,
                                                ObTableApiScanCtDef &tsc_ctdef,
                                                ObDASBaseCtDef *&root_ctdef)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTextRetrievalInfo *tr_info = nullptr;
  ObMatchFunRawExpr *match_against = nullptr;
  ObDASIRScanCtDef *ir_scan_ctdef = nullptr;
  ObDASSortCtDef *sort_ctdef = nullptr;
  const bool use_approx_pre_agg = false;
  ObExpr *index_back_doc_id_column = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else if (OB_ISNULL(match_against = tr_info->match_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("match against expr is NULL", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tr_info->inv_idx_tid_
      || (tr_info->need_calc_relevance_ && OB_INVALID_ID == tr_info->fwd_idx_tid_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fulltext index table id", K(ret), KPC(match_against));
  } else if (OB_UNLIKELY(ObTSCIRScanType::OB_IR_INV_IDX_SCAN != tsc_ctdef.scan_ctdef_.ir_scan_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ir scan type for inverted index scan", K(ret), K(tsc_ctdef.scan_ctdef_));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_SCAN, allocator, ir_scan_ctdef))) {
    LOG_WARN("allocate ir scan ctdef failed", K(ret));
  } else if (tr_info->need_calc_relevance_) {
    ObDASScanCtDef *inv_idx_scan_ctdef = &tsc_ctdef.scan_ctdef_;
    ObDASScanCtDef *inv_idx_agg_ctdef = nullptr;
    ObDASScanCtDef *doc_id_idx_agg_ctdef = nullptr;
    ObDASScanCtDef *fwd_idx_agg_ctdef = nullptr;
    if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, allocator, inv_idx_agg_ctdef))) {
      LOG_WARN("allocate inv idx agg ctdef failed", K(ret));
    } else {
      inv_idx_agg_ctdef->ref_table_id_ = tr_info->inv_idx_tid_;
      inv_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
      inv_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_AGG;
      if (OB_FAIL(ObTableTscCgService::generate_das_tsc_ctdef(ctx, allocator, *inv_idx_agg_ctdef))) {
        LOG_WARN("failed to generate das scan ctdef", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, allocator, doc_id_idx_agg_ctdef))) {
        LOG_WARN("allocate doc id idx agg ctdef failed", K(ret));
      } else {
        doc_id_idx_agg_ctdef->ref_table_id_ = tr_info->doc_id_idx_tid_;
        doc_id_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
        doc_id_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG;
        if (OB_FAIL(ObTableTscCgService::generate_das_tsc_ctdef(ctx, allocator, *doc_id_idx_agg_ctdef))) {
          LOG_WARN("failed to generate das scan ctdef", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, allocator, fwd_idx_agg_ctdef))) {
        LOG_WARN("allocate fwd idx agg ctdef failed", K(ret));
      } else {
        fwd_idx_agg_ctdef->ref_table_id_ = tr_info->fwd_idx_tid_;
        fwd_idx_agg_ctdef->pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
        fwd_idx_agg_ctdef->ir_scan_type_ = ObTSCIRScanType::OB_IR_FWD_IDX_AGG;
        if (OB_FAIL(ObTableTscCgService::generate_das_tsc_ctdef(ctx, allocator, *fwd_idx_agg_ctdef))) {
          LOG_WARN("generate das scan ctdef failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t ir_scan_children_cnt = 4;
      if (OB_ISNULL(ir_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, ir_scan_children_cnt))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate ir scan ctdef children failed", K(ret));
      } else {
        ir_scan_ctdef->children_cnt_ = ir_scan_children_cnt;
        ir_scan_ctdef->children_[0] = inv_idx_scan_ctdef;
        ir_scan_ctdef->children_[1] = inv_idx_agg_ctdef;
        ir_scan_ctdef->children_[2] = doc_id_idx_agg_ctdef;
        ir_scan_ctdef->children_[3] = fwd_idx_agg_ctdef;
        ir_scan_ctdef->has_inv_agg_ = true;
        ir_scan_ctdef->has_doc_id_agg_ = true;
        ir_scan_ctdef->has_fwd_agg_ = true;
      }
    }
  } else {
    if (OB_ISNULL(ir_scan_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate ir scan ctdef children failed", K(ret));
    } else {
      ir_scan_ctdef->children_cnt_ = 1;
      ir_scan_ctdef->children_[0] = &tsc_ctdef.scan_ctdef_;
    }
  }

  if (OB_SUCC(ret)) {
    root_ctdef = ir_scan_ctdef;
    if (OB_FAIL(generate_text_ir_spec_exprs(ctx, *ir_scan_ctdef))) {
      LOG_WARN("failed to generate text ir spec exprs", K(ret), KPC(match_against));
    } else {
      tsc_ctdef.search_text_ = ir_scan_ctdef->search_text_;
      // No estimated info or approx agg not allowed, do total document count on execution;
      ir_scan_ctdef->estimated_total_doc_cnt_ = 0;
      index_back_doc_id_column = ir_scan_ctdef->inv_scan_doc_id_col_;
    }
  }

 if (OB_SUCC(ret) && ctx.is_index_back()) {
    ObDASIRAuxLookupCtDef *aux_lookup_ctdef = nullptr;
    ObDASBaseCtDef *ir_output_ctdef = static_cast<ObDASBaseCtDef *>(ir_scan_ctdef);
    if (OB_FAIL(generate_doc_id_lookup_ctdef(ctx, allocator, tsc_ctdef, ir_output_ctdef,
                  index_back_doc_id_column, aux_lookup_ctdef))) {
      LOG_WARN("generate doc id lookup ctdef failed", K(ret));
    } else if (OB_FAIL(append_fts_relavence_project_col(aux_lookup_ctdef, ir_scan_ctdef))) {
      LOG_WARN("failed to append fts relavence project col", K(ret));
    } else {
      root_ctdef = aux_lookup_ctdef;
    }
  }

  return ret;
}

int ObTableFtsTscCgService::generate_das_sort_ctdef(const ObTableCtx &ctx,
                                                   ObIAllocator &allocator,
                                                   ObTextRetrievalInfo &tr_info,
                                                   ObDASBaseCtDef *child_ctdef,
                                                   ObDASSortCtDef *&sort_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t SORT_KEY_COUNT = 1;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  if (OB_ISNULL(child_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sort arg", K(ret), KPC(child_ctdef));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_SORT, allocator, sort_ctdef))) {
    LOG_WARN("alloc sort ctdef failed ", K(ret));
  } else if (OB_FAIL(sort_ctdef->sort_collations_.init(SORT_KEY_COUNT))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else if (OB_FAIL(sort_ctdef->sort_cmp_funcs_.init(SORT_KEY_COUNT))) {
    LOG_WARN("failed to init sort cmp funcs", K(ret));
  } else if (OB_FAIL(sort_ctdef->sort_exprs_.init(SORT_KEY_COUNT))) {
    LOG_WARN("failed to init sort exprs", K(ret));
  } else if (OB_ISNULL(sort_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 1))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate ir scan ctdef children failed", K(ret));
  } else if (nullptr != tr_info.topk_limit_expr_ &&
        OB_FAIL(cg.generate_rt_expr(*tr_info.topk_limit_expr_, sort_ctdef->limit_expr_))) {
    LOG_WARN("cg rt expr for top-k limit expr failed", K(ret));
  } else if (nullptr != tr_info.topk_offset_expr_ &&
        OB_FAIL(cg.generate_rt_expr(*tr_info.topk_offset_expr_, sort_ctdef->offset_expr_))) {
    LOG_WARN("cg rt expr for top-k offset expr failed", K(ret));
  } else {
    sort_ctdef->children_cnt_ = 1;
    sort_ctdef->children_[0] = child_ctdef;
    sort_ctdef->fetch_with_ties_ = tr_info.with_ties_;
  }

  ObSEArray<ObExpr *, 4> result_output;
  int64_t field_idx = 0;
  if (OB_SUCC(ret)) {
    const OrderItem &order_item = tr_info.sort_key_;
    ObExpr *expr = nullptr;
    if (OB_FAIL(cg.generate_rt_expr(*order_item.expr_, expr))) {
      LOG_WARN("failed to generate rt expr", K(ret));
    } else {
      ObSortFieldCollation field_collation(field_idx,
          expr->datum_meta_.cs_type_,
          order_item.is_ascending(),
          (order_item.is_null_first() ^ order_item.is_ascending()) ? NULL_LAST : NULL_FIRST);
      ObSortCmpFunc cmp_func;
      cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
          expr->datum_meta_.type_,
          expr->datum_meta_.type_,
          field_collation.null_pos_,
          field_collation.cs_type_,
          expr->datum_meta_.scale_,
          lib::is_oracle_mode(),
          expr->obj_meta_.has_lob_header(),
          expr->datum_meta_.precision_,
          expr->datum_meta_.precision_);
      if (OB_ISNULL(cmp_func.cmp_func_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
      } else if (OB_FAIL(sort_ctdef->sort_cmp_funcs_.push_back(cmp_func))) {
        LOG_WARN("failed to append sort function", K(ret));
      } else if (OB_FAIL(sort_ctdef->sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back field collation", K(ret));
      } else if (OB_FAIL(sort_ctdef->sort_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_array_no_dup(result_output, sort_ctdef->sort_exprs_))) {
    LOG_WARN("failed to append sort exprs to result output", K(ret));
  } else if (ObDASTaskFactory::is_attached(child_ctdef->op_type_)
      && OB_FAIL(append_array_no_dup(result_output, static_cast<ObDASAttachCtDef *>(child_ctdef)->result_output_))) {
    LOG_WARN("failed to append child result output", K(ret));
  } else if (child_ctdef->op_type_ == DAS_OP_TABLE_SCAN
      && OB_FAIL(append_array_no_dup(result_output, static_cast<ObDASScanCtDef *>(child_ctdef)->result_output_))) {
    LOG_WARN("failed to append child result output", K(ret));
  } else if (OB_FAIL(sort_ctdef->result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret));
  }
  LOG_TRACE("generate sort ctdef finished", K(tr_info.sort_key_), K(sort_ctdef->sort_exprs_), K(result_output), K(ret));
  return ret;
}

int ObTableFtsTscCgService::generate_text_ir_spec_exprs(const ObTableCtx &ctx, ObDASIRScanCtDef &text_ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr *, 4> result_output;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTextRetrievalInfo *tr_info = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(tr_info = fts_ctx->get_text_retrieval_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tr_info is NULL", K(ret));
  } else if (OB_ISNULL(tr_info->match_expr_) || OB_ISNULL(tr_info->relevance_expr_) ||
      OB_ISNULL(tr_info->doc_id_column_) || OB_ISNULL(tr_info->doc_length_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(tr_info->match_expr_), KP(tr_info->relevance_expr_),
                KP(tr_info->doc_id_column_), KP(tr_info->doc_length_column_));
  } else if (OB_FAIL(cg.generate_rt_expr(*tr_info->match_expr_->get_search_key(), text_ir_scan_ctdef.search_text_))) {
    LOG_WARN("cg rt expr for search text failed", K(ret));
  } else if (OB_ISNULL(tr_info->pushdown_match_filter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null match filter", K(ret));
  } else if (OB_FAIL(cg.generate_rt_expr(*tr_info->pushdown_match_filter_, text_ir_scan_ctdef.match_filter_))) {
    LOG_WARN("cg rt expr for match filter failed", K(ret));
  } else {
    const UIntFixedArray &inv_scan_col_id = text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->access_column_ids_;
    const ObColumnRefRawExpr *doc_id_column = static_cast<ObColumnRefRawExpr *>(tr_info->doc_id_column_);
    const ObColumnRefRawExpr *doc_length_column = static_cast<ObColumnRefRawExpr *>(tr_info->doc_length_column_);

    int64_t doc_id_col_idx = -1;
    int64_t doc_length_col_idx = -1;
    for (int64_t i = 0; i < inv_scan_col_id.count(); ++i) {
      if (inv_scan_col_id.at(i) == doc_id_column->get_column_id()) {
        doc_id_col_idx = i;
      } else if (inv_scan_col_id.at(i) == doc_length_column->get_column_id()) {
        doc_length_col_idx = i;
      }
    }
    if (OB_UNLIKELY(-1 == doc_id_col_idx || -1 == doc_length_col_idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected doc id not found in inverted index scan access columns",
          K(ret), K(text_ir_scan_ctdef), K(doc_id_col_idx), K(doc_length_col_idx));
    } else {
      text_ir_scan_ctdef.inv_scan_doc_id_col_ =
          text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_.at(doc_id_col_idx);
      text_ir_scan_ctdef.inv_scan_doc_length_col_ =
          text_ir_scan_ctdef.get_inv_idx_scan_ctdef()->pd_expr_spec_.access_exprs_.at(doc_length_col_idx);
      if (OB_FAIL(result_output.push_back(text_ir_scan_ctdef.inv_scan_doc_id_col_))) {
        LOG_WARN("failed to append output exprs", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (tr_info->need_calc_relevance_) {
    if (OB_ISNULL(tr_info->relevance_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null relevance expr", K(ret));
    } else if (OB_FAIL(cg.generate_rt_expr(*tr_info->relevance_expr_, text_ir_scan_ctdef.relevance_expr_))) {
      LOG_WARN("cg rt expr for relevance expr failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if ((tr_info->need_calc_relevance_ || nullptr != tr_info->pushdown_match_filter_)) {
    if (OB_FAIL(cg.generate_rt_expr(*tr_info->match_expr_,
                                            text_ir_scan_ctdef.relevance_proj_col_))) {
      LOG_WARN("cg rt expr for relevance score proejction failed", K(ret));
    } else if (OB_ISNULL(text_ir_scan_ctdef.relevance_proj_col_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected relevance pseudo score colum not found", K(ret));
    } else if (OB_FAIL(result_output.push_back(text_ir_scan_ctdef.relevance_proj_col_))) {
      LOG_WARN("failed to append relevance expr", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(text_ir_scan_ctdef.result_output_.assign(result_output))) {
    LOG_WARN("failed to assign result output", K(ret), K(result_output));
  }
  return ret;
}

int ObTableFtsTscCgService::generate_doc_id_lookup_ctdef(const ObTableCtx &ctx,
                                                        ObIAllocator &allocator,
                                                        ObTableApiScanCtDef &tsc_ctdef,
                                                        ObDASBaseCtDef *ir_scan_ctdef,
                                                        ObExpr *doc_id_expr,
                                                        ObDASIRAuxLookupCtDef *&aux_lookup_ctdef)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTableSchema *index_schema = nullptr;
  ObDASScanCtDef *scan_ctdef = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(index_schema = fts_ctx->get_doc_rowkey_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is NULL", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, allocator, scan_ctdef))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_IR_AUX_LOOKUP, allocator, aux_lookup_ctdef))) {
    LOG_WARN("alloc aux lookup ctdef failed", K(ret));
  } else if (OB_ISNULL(aux_lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    ObArray<ObExpr*> result_outputs;
    scan_ctdef->ref_table_id_ = fts_ctx->get_doc_rowkey_tid();
    aux_lookup_ctdef->children_cnt_ = 2;
    ObDASTableLocMeta *scan_loc_meta = OB_NEWx(ObDASTableLocMeta, &allocator, allocator);
    if (OB_ISNULL(scan_loc_meta)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate scan location meta failed", K(ret));
    } else if (OB_FAIL(ObTableTscCgService::generate_das_tsc_ctdef(ctx, allocator, *scan_ctdef))) {
      LOG_WARN("generate das lookup scan ctdef failed", K(ret));
    } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
      LOG_WARN("construct aux lookup ctdef failed", K(ret));
    } else if (OB_ISNULL(doc_id_expr) || OB_UNLIKELY(!scan_ctdef->rowkey_exprs_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected doc id expr status", K(ret), KPC(doc_id_expr), KPC(scan_ctdef));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.reserve(1))) {
      LOG_WARN("failed to reserve doc id lookup rowkey exprs", K(ret));
    } else if (OB_FAIL(scan_ctdef->rowkey_exprs_.push_back(doc_id_expr))) {
      LOG_WARN("failed to append doc id rowkey expr", K(ret));
    } else if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(ctx, *index_schema, *scan_loc_meta, nullptr))) {
      LOG_WARN("generate table loc meta failed", K(ret));
    } else if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(scan_loc_meta))) {
      LOG_WARN("store scan loc meta failed", K(ret));
    } else {
      aux_lookup_ctdef->children_[0] = ir_scan_ctdef;
      aux_lookup_ctdef->children_[1] = scan_ctdef;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(aux_lookup_ctdef->result_output_.assign(result_outputs))) {
        LOG_WARN("assign result output failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTableFtsTscCgService::append_fts_relavence_project_col(ObDASIRAuxLookupCtDef *aux_lookup_ctdef,
                                                             ObDASIRScanCtDef *ir_scan_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ir_scan_ctdef)) {
    if (ir_scan_ctdef->relevance_proj_col_ != nullptr) {
      ObArray<ObExpr*> result_outputs;
      if (OB_FAIL(result_outputs.push_back(ir_scan_ctdef->relevance_proj_col_))) {
        LOG_WARN("store relevance projector column expr failed", K(ret));
      } else if (OB_FAIL(append(result_outputs, aux_lookup_ctdef->result_output_))) {
        LOG_WARN("append tmp array failed", K(ret));
      } else {
        aux_lookup_ctdef->result_output_.destroy();
        if (OB_FAIL(aux_lookup_ctdef->result_output_.init(result_outputs.count()))) {
          LOG_WARN("reserve slot failed", K(ret));
        } else if (OB_FAIL(aux_lookup_ctdef->result_output_.assign(result_outputs))) {
          LOG_WARN("store relevance projector column expr failed", K(ret));
        } else {
          aux_lookup_ctdef->relevance_proj_col_ = ir_scan_ctdef->relevance_proj_col_;
        }
      }
    }
  }
  return ret;
}

int ObTableFtsTscCgService::generate_das_scan_ctdef_with_doc_id(ObIAllocator &alloc,
                                                               const ObTableCtx &ctx,
                                                               ObTableApiScanCtDef &tsc_ctdef,
                                                               ObDASScanCtDef *scan_ctdef,
                                                               ObDASDocIdMergeCtDef *&doc_id_merge_ctdef)
{
  int ret = OB_SUCCESS;
  ObArray<ObExpr*> result_outputs;
  ObDASScanCtDef *rowkey_doc_scan_ctdef = nullptr;
  if (OB_ISNULL(scan_ctdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(scan_ctdef));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_DOC_ID_MERGE, alloc, doc_id_merge_ctdef))) {
    LOG_WARN("fail to allocate to doc id merge ctdef", K(ret));
  } else if (OB_ISNULL(doc_id_merge_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &alloc, 2))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate doc id merge ctdef child array memory", K(ret));
  } else if (OB_FAIL(generate_rowkey_doc_ctdef(alloc, ctx, tsc_ctdef, rowkey_doc_scan_ctdef))) {
    LOG_WARN("fail to generate rowkey doc ctdef", K(ret));
  } else if (OB_FAIL(result_outputs.assign(scan_ctdef->result_output_))) {
    LOG_WARN("construct aux lookup ctdef failed", K(ret));
  } else if (OB_UNLIKELY(result_outputs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, result outputs is nullptr", K(ret));
  } else {
    doc_id_merge_ctdef->children_cnt_ = 2;
    doc_id_merge_ctdef->children_[0] = scan_ctdef;
    doc_id_merge_ctdef->children_[1] = rowkey_doc_scan_ctdef;
    if (OB_FAIL(doc_id_merge_ctdef->result_output_.assign(result_outputs))) {
      LOG_WARN("fail to assign result output", K(ret));
    }
  }
  return ret;
}

int ObTableFtsTscCgService::generate_rowkey_doc_ctdef(ObIAllocator &alloc,
                                                     const ObTableCtx &ctx,
                                                     ObTableApiScanCtDef &tsc_ctdef,
                                                     ObDASScanCtDef *&rowkey_doc_scan_ctdef)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTableSchema *rowkey_doc_schema = nullptr;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(rowkey_doc_schema = fts_ctx->get_rowkey_doc_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey doc schema", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN, alloc, rowkey_doc_scan_ctdef))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else {
    rowkey_doc_scan_ctdef->ref_table_id_ = fts_ctx->get_rowkey_doc_tid();
    ObDASTableLocMeta *scan_loc_meta = OB_NEWx(ObDASTableLocMeta, &alloc, alloc);
    bool is_cs_replica_query = false;
    if (OB_ISNULL(scan_loc_meta)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate scan location meta failed", K(ret));
    } else if (OB_FAIL(ctx.check_is_cs_replica_query(is_cs_replica_query))) {
      LOG_WARN("fail to check is cs_replica_query", K(ret));
    } else if (OB_FAIL(ObTableTscCgService::generate_das_tsc_ctdef(ctx, alloc, *rowkey_doc_scan_ctdef, is_cs_replica_query))) {
      LOG_WARN("generate rowkey doc das scan ctdef failed", K(ret));
    } else if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(ctx, *rowkey_doc_schema, *scan_loc_meta, nullptr))) {
      LOG_WARN("fail to generate table loc meta", K(ret));
    } else if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(scan_loc_meta))) {
      LOG_WARN("fail to push back loc meta", K(ret));
    }
  }
  return ret;
}

int ObTableFtsTscCgService::extract_rowkey_doc_exprs(const ObTableCtx &ctx,
                                                     ObIArray<ObRawExpr *> &rowkey_doc_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTableSchema *rowkey_doc_schema = nullptr;
  ObSEArray<uint64_t, 4> rowkey_doc_cids;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(rowkey_doc_schema = fts_ctx->get_rowkey_doc_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey doc schema", K(ret));
  } else if (!rowkey_doc_schema->is_rowkey_doc_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is not rowkey doc table", K(ret));
  } else if (OB_FAIL(rowkey_doc_schema->get_column_ids(rowkey_doc_cids))) {
    LOG_WARN("fail to get rowkey column ids in rowkey doc", K(ret), KPC(rowkey_doc_schema));
  } else {
    uint64_t doc_id_col_id = OB_INVALID_ID;
    uint64_t ft_col_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_doc_cids.count(); i++) {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(ctx.get_column_item_by_column_id(rowkey_doc_cids.at(i), item))) {
        LOG_WARN("fail to get column item", K(ret), K(rowkey_doc_cids), K(i));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret));
      } else if (OB_FAIL(rowkey_doc_exprs.push_back(item->raw_expr_))) {
        LOG_WARN("fail to push back index expr", K(ret), K(i));
      }
    } // end for
  }
  return ret;
}

int ObTableFtsTscCgService::extract_doc_rowkey_exprs(const ObTableCtx &ctx,
                                                     ObIArray<ObRawExpr *> &doc_rowkey_exprs)
{
  int ret = OB_SUCCESS;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  const ObTableSchema *doc_rowkey_schema = nullptr;
  ObSEArray<uint64_t, 4> rowkey_doc_cids;
  if (OB_ISNULL(fts_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fts_ctx is NULL", K(ret));
  } else if (OB_ISNULL(doc_rowkey_schema = fts_ctx->get_doc_rowkey_schema())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rowkey doc schema", K(ret));
  } else if (!doc_rowkey_schema->is_doc_id_rowkey()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is not rowkey doc table", K(ret), K(doc_rowkey_schema->get_index_type()));
  } else if (OB_FAIL(doc_rowkey_schema->get_column_ids(rowkey_doc_cids))) {
    LOG_WARN("fail to get rowkey column ids in rowkey doc", K(ret), KP(doc_rowkey_schema));
  } else {
    uint64_t doc_id_col_id = OB_INVALID_ID;
    uint64_t ft_col_id = OB_INVALID_ID;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_doc_cids.count(); i++) {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(ctx.get_column_item_by_column_id(rowkey_doc_cids.at(i), item))) {
        LOG_WARN("fail to get column item", K(ret), K(rowkey_doc_cids), K(i));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret));
      } else if (OB_FAIL(doc_rowkey_exprs.push_back(item->raw_expr_))) {
        LOG_WARN("fail to push back index expr", K(ret), K(i));
      }
    } // end for
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase
