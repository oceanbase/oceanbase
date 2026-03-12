/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/hybrid_search/ob_fulltext_search_node.h"

namespace oceanbase
{
namespace sql
{

ObTextRetrievalIndexInfo::ObTextRetrievalIndexInfo()
  : token_column_(nullptr),
    token_cnt_column_(nullptr),
    domain_id_column_(nullptr),
    doc_length_column_(nullptr),
    pos_list_column_(nullptr),
    related_doc_cnt_(nullptr),
    total_doc_cnt_(nullptr),
    doc_token_cnt_(nullptr),
    avg_doc_token_cnt_(nullptr),
    relevance_expr_(nullptr),
    generated_(false)
{}

int ObTextRetrievalIndexInfo::generate_exprs(
    const uint64_t inv_idx_tid,
    const bool doc_id_as_domain_id,
    const TableItem &table_item,
    ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory &expr_factory = opt_ctx.get_expr_factory();
  ObSQLSessionInfo *session_info = opt_ctx.get_session_info();
  ObSqlSchemaGuard *schema_guard = opt_ctx.get_sql_schema_guard();
  const uint64_t table_id = table_item.table_id_;
  const ObTableSchema *table_schema = nullptr;
  const ObTableSchema *inv_idx_schema = nullptr;
  if (generated_) {
    // already generated
    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid text retrieval index info", K(ret), KPC(this));
    }
  } else if (OB_ISNULL(session_info) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret), KP(session_info), KP(schema_guard));
  } else if (OB_FAIL(generate_column_exprs(
      inv_idx_tid, doc_id_as_domain_id, table_item, expr_factory, *session_info, *schema_guard))) {
    LOG_WARN("failed to generate column exprs", K(ret));
  } else if (OB_FAIL(generate_calc_exprs(expr_factory, *session_info))) {
    LOG_WARN("failed to generate calc exprs", K(ret));
  } else {
    generated_ = true;
  }
  return ret;
}

int ObTextRetrievalIndexInfo::generate_column_exprs(
    const uint64_t inv_idx_tid,
    const bool doc_id_as_domain_id,
    const TableItem &table_item,
    ObRawExprFactory &expr_factory,
    ObSQLSessionInfo &session_info,
    ObSqlSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  const ObTableSchema *table_schema = nullptr;
  const ObTableSchema *inv_idx_schema = nullptr;
  if (OB_UNLIKELY(OB_INVALID_ID == inv_idx_tid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv idx tid", K(ret), K(inv_idx_tid));
  } else if (OB_FAIL(schema_guard.get_table_schema(inv_idx_tid, inv_idx_schema))) {
    LOG_WARN("failed to get inv idx schema", K(ret));
  } else if (OB_ISNULL(inv_idx_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected inv idx schema", K(ret), KP(inv_idx_schema), K(inv_idx_tid));
  } else if (FALSE_IT(table_id = inv_idx_schema->get_data_table_id())) {
  } else if (OB_FAIL(schema_guard.get_table_schema(table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table schema", K(ret), KP(table_schema), K(table_id));
  }

  uint64_t token_cnt_col_id = OB_INVALID_ID;
  uint64_t token_col_id = OB_INVALID_ID;
  uint64_t doc_length_col_id = OB_INVALID_ID;
  uint64_t pos_list_col_id = OB_INVALID_ID;
  ObColumnRefRawExpr *docid_or_rowkey_column = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < inv_idx_schema->get_column_count(); ++i) {
    const ObColumnSchemaV2 *col_schema = inv_idx_schema->get_column_schema_by_idx(i);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column schema ptr", K(ret));
    } else {
      const ObColumnSchemaV2 *col_schema_in_data_table = table_schema->get_column_schema(col_schema->get_column_id());
      if (OB_ISNULL(col_schema_in_data_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr in data table",
            K(ret), KPC(col_schema), KPC(table_schema));
      } else if (col_schema_in_data_table->is_word_count_column()) {
        token_cnt_col_id = col_schema->get_column_id();
      } else if (col_schema_in_data_table->is_word_segment_column()) {
        token_col_id = col_schema->get_column_id();
      } else if (col_schema_in_data_table->is_doc_length_column()) {
        doc_length_col_id = col_schema->get_column_id();
      } else if (col_schema_in_data_table->is_pos_list_column()) {
        pos_list_col_id = col_schema->get_column_id();
      } else if (col_schema_in_data_table->is_doc_id_column() && OB_ISNULL(docid_or_rowkey_column)) {
        // NOTE :
        // create doc id expr
        // Since currently, doc id column on main table schema is a special "virtual generated" column,
        // which can not be calculated by its expr record on schema
        // So we use its column ref expr on index table for index back / projection instead
        if (!doc_id_as_domain_id) {
          // do nothing
        } else if (OB_FAIL(ObRawExprUtils::build_column_expr(
            expr_factory, *col_schema, &session_info, docid_or_rowkey_column))) {
          LOG_WARN("failed to build doc id column expr", K(ret));
        } else {
          docid_or_rowkey_column->set_ref_id(table_item.table_id_, col_schema->get_column_id());
          docid_or_rowkey_column->set_column_attr(table_schema->get_table_name(), col_schema->get_column_name_str());
          docid_or_rowkey_column->set_database_name(table_item.database_name_);
        }
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count(); ++i) {
    const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(i);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null column schema ptr", K(ret));
    } else if (col_schema->get_column_id() == token_cnt_col_id) {
      if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, &session_info, token_cnt_column_))) {
        LOG_WARN("failed to build doc id column expr", K(ret));
      } else if (OB_ISNULL(token_cnt_column_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null token cnt column expr", K(ret), KPC_(token_cnt_column));
      } else {
        token_cnt_column_->set_ref_id(table_item.table_id_, col_schema->get_column_id());
        token_cnt_column_->set_column_attr(table_schema->get_table_name(), col_schema->get_column_name_str());
        token_cnt_column_->set_database_name(table_item.database_name_);
      }
    } else if (col_schema->get_column_id() == token_col_id) {
      if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, &session_info, token_column_))) {
        LOG_WARN("failed to build doc id column expr", K(ret));
      } else if (OB_ISNULL(token_column_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null token column expr", K(ret), KPC_(token_column));
      } else {
        token_column_->set_ref_id(table_item.table_id_, col_schema->get_column_id());
        token_column_->set_column_attr(table_schema->get_table_name(), col_schema->get_column_name_str());
        token_column_->set_database_name(table_item.database_name_);
      }
    } else if (col_schema->get_column_id() == doc_length_col_id) {
      if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, &session_info, doc_length_column_))) {
        LOG_WARN("failed to build doc id column expr", K(ret));
      } else if (OB_ISNULL(doc_length_column_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null doc length column expr", K(ret), KPC_(doc_length_column));
      } else {
        doc_length_column_->set_ref_id(table_item.table_id_, col_schema->get_column_id());
        doc_length_column_->set_column_attr(table_schema->get_table_name(), col_schema->get_column_name_str());
        doc_length_column_->set_database_name(table_item.database_name_);
      }
    } else if (col_schema->get_column_id() == pos_list_col_id) {
      if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, &session_info, pos_list_column_))) {
        LOG_WARN("failed to build pos list column expr", K(ret));
      } else if (OB_ISNULL(pos_list_column_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pos list column expr", K(ret), KPC_(doc_length_column));
      } else {
        pos_list_column_->set_ref_id(table_item.table_id_, col_schema->get_column_id());
        pos_list_column_->set_column_attr(table_schema->get_table_name(), col_schema->get_column_name_str());
        pos_list_column_->set_database_name(table_item.database_name_);
      }
    } else if (col_schema->is_rowkey_column() && !doc_id_as_domain_id) {
      if (1 == col_schema->get_rowkey_position()) {
        if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *col_schema, &session_info, docid_or_rowkey_column))) {
          LOG_WARN("failed to build doc id column expr", K(ret));
        } else if (OB_ISNULL(docid_or_rowkey_column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null doc id or rowkey column expr", K(ret), KPC(docid_or_rowkey_column));
        } else {
          docid_or_rowkey_column->set_ref_id(table_item.table_id_, col_schema->get_column_id());
          docid_or_rowkey_column->set_column_attr(table_schema->get_table_name(), col_schema->get_column_name_str());
          docid_or_rowkey_column->set_database_name(table_item.database_name_);
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    domain_id_column_ = docid_or_rowkey_column;
  }
  return ret;
}

int ObTextRetrievalIndexInfo::generate_calc_exprs(
    ObRawExprFactory &expr_factory,
    ObSQLSessionInfo &session_info)
{
  int ret = OB_SUCCESS;
  ObOpPseudoColumnRawExpr *avg_doc_token_cnt_expr = nullptr;
  ObOpRawExpr *relevance_expr = nullptr;
  ObRawExprResType avg_doc_token_cnt_res_type;
  avg_doc_token_cnt_res_type.set_type(ObDoubleType);
  avg_doc_token_cnt_res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObDoubleType]);
  ObRawExprCopier copier(expr_factory);
  if (OB_ISNULL(token_column_)
      || OB_ISNULL(token_cnt_column_)
      || OB_ISNULL(domain_id_column_)
      || OB_ISNULL(doc_length_column_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), KP_(token_column), KP_(token_cnt_column),
        KP_(domain_id_column), KP_(doc_length_column));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, related_doc_cnt_))) {
    LOG_WARN("failed to create related doc cnt expr", K(ret));
  } else if (OB_FAIL(related_doc_cnt_->add_real_param_expr(token_cnt_column_))) {
    LOG_WARN("failed to set agg param", K(ret));
  } else if (OB_FAIL(related_doc_cnt_->formalize(&session_info))) {
    LOG_WARN("failed to formalize related doc cnt expr", K(ret));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_COUNT, total_doc_cnt_))) {
    LOG_WARN("failed to create total doc cnt expr", K(ret));
  } else if (OB_FAIL(total_doc_cnt_->add_real_param_expr(domain_id_column_))) {
    LOG_WARN("failed to set agg param", K(ret));
  } else if (OB_FAIL(total_doc_cnt_->formalize(&session_info))) {
    LOG_WARN("failed to formalize total doc cnt expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(
      expr_factory,
      T_PSEUDO_COLUMN,
      "avg_doc_token_cnt_expr",
      avg_doc_token_cnt_res_type,
      avg_doc_token_cnt_expr))) {
    LOG_WARN("failed to build avg doc token count pseudo column expr", K(ret));
  } else if (OB_FAIL(avg_doc_token_cnt_expr->formalize(&session_info))) {
    LOG_WARN("failed to formalize avg doc token count expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_bm25_expr(
      expr_factory,
      related_doc_cnt_,
      token_cnt_column_,
      total_doc_cnt_,
      doc_length_column_,
      doc_length_column_,
      avg_doc_token_cnt_expr,
      relevance_expr,
      true,
      &session_info))) {
    LOG_WARN("failed to build bm25 expr", K(ret));
  } else if (OB_FAIL(relevance_expr->formalize(&session_info))) {
    LOG_WARN("failed to formalize bm25 expr", K(ret));
  // Copy column ref expr referenced by aggregation in different index table scan
  // to avoid share expression
  } else if (OB_FAIL(copier.copy(related_doc_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to copy related doc cnt expr", K(ret));
  } else if (OB_FAIL(copier.copy(total_doc_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to copy total doc cnt expr", K(ret));
  } else {
    avg_doc_token_cnt_ = avg_doc_token_cnt_expr;
    relevance_expr_ = relevance_expr;
  }
  return ret;
}

int ObTextRetrievalIndexInfo::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid text retrieval index info", K(ret), KPC(this));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(token_column_)))) {
    LOG_WARN("failed to push back token column expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(token_cnt_column_)))) {
    LOG_WARN("failed to push back token count column expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(domain_id_column_)))) {
    LOG_WARN("failed to push back domain id column expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(doc_length_column_)))) {
    LOG_WARN("failed to push back doc length column expr", K(ret));
  } else if (nullptr != pos_list_column_
      && OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(pos_list_column_)))) {
    LOG_WARN("failed to push back pos list column expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(related_doc_cnt_)))) {
    LOG_WARN("failed to push back related doc cnt expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(total_doc_cnt_)))) {
    LOG_WARN("failed to push back total doc cnt expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(avg_doc_token_cnt_)))) {
    LOG_WARN("failed to push back avg doc token cnt expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, static_cast<ObRawExpr*>(relevance_expr_)))) {
    LOG_WARN("failed to push back relevance expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, total_doc_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to push back total doc cnt expr", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(query_exprs, related_doc_cnt_->get_param_expr(0)))) {
    LOG_WARN("failed to push back related doc cnt expr", K(ret));
  }
  return ret;
}

bool ObTextRetrievalIndexInfo::is_valid() const
{
  return nullptr != token_column_
      && nullptr != token_cnt_column_
      && nullptr != doc_length_column_
      && nullptr != related_doc_cnt_
      && nullptr != total_doc_cnt_
      && nullptr != avg_doc_token_cnt_
      && nullptr != relevance_expr_
      && generated_;
}

ObFullTextQueryNode::ObFullTextQueryNode(common::ObIAllocator &allocator, const QueryType query_type)
  : ObHybridSearchNodeBase(allocator),
    query_type_(query_type),
    doc_id_idx_tid_(OB_INVALID_ID),
    query_text_(nullptr),
    score_project_expr_(nullptr)
{
  node_type_ = INDEX_MERGE_HYBRID_FTS_QUERY;
}

int ObFullTextQueryNode::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  ObRawExprResType relevance_res_type;
  relevance_res_type.set_type(ObDoubleType);
  relevance_res_type.set_accuracy(ObAccuracy::MAX_ACCURACY[ObDoubleType]);
  ObOpPseudoColumnRawExpr *score_project_expr = nullptr;
  if (OB_FAIL(ObHybridSearchNodeBase::generate_access_exprs(table_item, opt_ctx))) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_op_pseudo_column_expr(
      opt_ctx.get_expr_factory(),
      T_PSEUDO_RELEVANCE_SCORE,
      "relevance_expr",
      relevance_res_type,
      score_project_expr))) {
    LOG_WARN("failed to build relevance projection column expr", K(ret));
  } else if (OB_FAIL(score_project_expr->formalize(opt_ctx.get_session_info()))) {
    LOG_WARN("failed to formalize relevance projection column expr", K(ret));
  } else {
    score_project_expr_ = score_project_expr;
  }
  return ret;
}

int ObFullTextQueryNode::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(query_exprs.push_back(query_text_))) {
    LOG_WARN("failed to push back query text expr", K(ret), KPC_(query_text));
  } else if (OB_FAIL(query_exprs.push_back(boost_))) {
    LOG_WARN("failed to push back boost expr", K(ret), K_(boost));
  } else if (OB_FAIL(score_exprs.push_back(score_project_expr_))) {
    LOG_WARN("failed to push back score project expr", K(ret), KPC_(score_project_expr));
  }
  return ret;
}

int ObFullTextQueryNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == doc_id_idx_tid_) {
    // skip doc id idx tid
  } else if (OB_FAIL(index_ids.push_back(doc_id_idx_tid_))) {
    LOG_WARN("failed to push back index id", K(ret));
  }
  return ret;
}

ObMatchQueryNode::ObMatchQueryNode(common::ObIAllocator &allocator)
  : ObFullTextQueryNode(allocator, OB_FULLTEXT_QUERY_TYPE_MATCH),
    field_(nullptr),
    inv_idx_tid_(OB_INVALID_ID),
    index_info_(),
    minimum_should_match_(nullptr),
    operator_(nullptr)
{}

int ObMatchQueryNode::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  const bool doc_id_as_domain_id = OB_INVALID_ID != doc_id_idx_tid_;
  if (OB_FAIL(ObFullTextQueryNode::generate_access_exprs(table_item, opt_ctx))) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(index_info_.generate_exprs(inv_idx_tid_, doc_id_as_domain_id, table_item, opt_ctx))) {
    LOG_WARN("failed to generate index info exprs", K(ret));
  }
  return ret;
}

int ObMatchQueryNode::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::collect_exprs(query_exprs, score_exprs))) {
    LOG_WARN("failed to collect exprs", K(ret));
  } else if (OB_FAIL(index_info_.collect_exprs(query_exprs, score_exprs))) {
    LOG_WARN("failed to collect index info exprs", K(ret));
  } else if (OB_FAIL(query_exprs.push_back(minimum_should_match_))) {
    LOG_WARN("failed to push back minimum should match expr", K(ret), KPC_(minimum_should_match));
  } else if (OB_FAIL(query_exprs.push_back(operator_))) {
    LOG_WARN("failed to push back operator expr", K(ret), KPC_(operator));
  }
  return ret;
}

int ObMatchQueryNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::get_all_index_ids(index_ids))) {
    LOG_WARN("failed to get all index ids", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == inv_idx_tid_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid inv idx tid", K(ret), KPC(this));
  } else if (OB_FAIL(index_ids.push_back(inv_idx_tid_))) {
    LOG_WARN("failed to push back index id", K(ret));
  }
  return ret;
}

int ObMatchQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("match node: "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("index name=%.*s", index_name_.length(), index_name_.ptr()))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  return ret;
}

ObMultiMatchQueryNode::ObMultiMatchQueryNode(common::ObIAllocator &allocator)
  : ObFullTextQueryNode(allocator, OB_FULLTEXT_QUERY_TYPE_MULTI_MATCH),
    fields_(),
    field_boosts_(),
    inv_idx_tids_(),
    index_infos_(),
    minimum_should_match_(nullptr),
    operator_(nullptr),
    field_type_(nullptr)
{}

int ObMultiMatchQueryNode::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::generate_access_exprs(table_item, opt_ctx))) {
    LOG_WARN("failed to generate access exprs", K(ret));
  }
  const bool doc_id_as_domain_id = OB_INVALID_ID != doc_id_idx_tid_;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_infos_.count(); ++i) {
    if (OB_FAIL(index_infos_.at(i).generate_exprs(inv_idx_tids_.at(i), doc_id_as_domain_id, table_item, opt_ctx))) {
      LOG_WARN("failed to generate index info exprs", K(ret));
    }
  }
  return ret;
}

int ObMultiMatchQueryNode::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::collect_exprs(query_exprs, score_exprs))) {
    LOG_WARN("failed to collect exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_infos_.count(); ++i) {
    if (OB_FAIL(index_infos_.at(i).collect_exprs(query_exprs, score_exprs))) {
      LOG_WARN("failed to collect index info exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_boosts_.count(); ++i) {
    if (OB_FAIL(query_exprs.push_back(field_boosts_.at(i)))) {
      LOG_WARN("failed to push back field boost expr", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query_exprs.push_back(minimum_should_match_))) {
    LOG_WARN("failed to push back minimum should match expr", K(ret), KPC_(minimum_should_match));
  } else if (OB_FAIL(query_exprs.push_back(operator_))) {
    LOG_WARN("failed to push back operator expr", K(ret), KPC_(operator));
  } else if (OB_FAIL(query_exprs.push_back(field_type_))) {
    LOG_WARN("failed to push back field type expr", K(ret), KPC_(field_type));
  }
  return ret;
}

int ObMultiMatchQueryNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::get_all_index_ids(index_ids))) {
    LOG_WARN("failed to get all index ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inv_idx_tids_.count(); ++i) {
    if (OB_UNLIKELY(OB_INVALID_ID == inv_idx_tids_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid inv idx tid", K(ret), K(i), KPC(this));
    } else if (OB_FAIL(index_ids.push_back(inv_idx_tids_.at(i)))) {
      LOG_WARN("failed to append index id", K(ret));
    }
  }
  return ret;
}

int ObMultiMatchQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("multi match node: "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("index name=("))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  for (int i = 0; i < index_names_.count() && OB_SUCC(ret); i++) {
    if (i > 0 && OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%.*s", index_names_.at(i).length(), index_names_.at(i).ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(BUF_PRINTF(")"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  return ret;
}

ObMatchPhraseQueryNode::ObMatchPhraseQueryNode(common::ObIAllocator &allocator)
  : ObFullTextQueryNode(allocator, OB_FULLTEXT_QUERY_TYPE_MATCH_PHRASE),
    field_(nullptr),
    inv_idx_tid_(OB_INVALID_ID),
    index_info_(),
    slop_(nullptr)
{}

int ObMatchPhraseQueryNode::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  const bool doc_id_as_domain_id = OB_INVALID_ID != doc_id_idx_tid_;
  if (OB_FAIL(ObFullTextQueryNode::generate_access_exprs(table_item, opt_ctx))) {
    LOG_WARN("failed to generate access exprs", K(ret));
  } else if (OB_FAIL(index_info_.generate_exprs(inv_idx_tid_, doc_id_as_domain_id, table_item, opt_ctx))) {
    LOG_WARN("failed to generate index info exprs", K(ret));
  } else if (OB_ISNULL(index_info_.pos_list_column_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "match_phrase query on this full-text index");
    LOG_WARN("missing pos list column in index", K(ret));
  }
  return ret;
}

int ObMatchPhraseQueryNode::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::collect_exprs(query_exprs, score_exprs))) {
    LOG_WARN("failed to collect exprs", K(ret));
  } else if (OB_FAIL(index_info_.collect_exprs(query_exprs, score_exprs))) {
    LOG_WARN("failed to collect index info exprs", K(ret));
  } else if (OB_FAIL(query_exprs.push_back(field_))) {
    LOG_WARN("failed to push back field expr", K(ret), KPC_(field));
  } else if (OB_FAIL(query_exprs.push_back(slop_))) {
    LOG_WARN("failed to push back slop expr", K(ret), KPC_(slop));
  }
  return ret;
}

int ObMatchPhraseQueryNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::get_all_index_ids(index_ids))) {
    LOG_WARN("failed to get all index ids", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == inv_idx_tid_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid inv idx tid", K(ret), KPC(this));
  } else if (OB_FAIL(index_ids.push_back(inv_idx_tid_))) {
    LOG_WARN("failed to push back index id", K(ret));
  }
  return ret;
}

int ObMatchPhraseQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("match phrase node: "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("index name=%.*s", index_name_.length(), index_name_.ptr()))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  return ret;
}

ObQueryStringQueryNode::ObQueryStringQueryNode(common::ObIAllocator &allocator)
  : ObFullTextQueryNode(allocator, OB_FULLTEXT_QUERY_TYPE_QUERY_STRING),
    fields_(),
    field_boosts_(),
    inv_idx_tids_(),
    index_infos_(),
    index_names_(),
    minimum_should_match_(nullptr),
    default_operator_(nullptr),
    field_type_(nullptr)
{}

int ObQueryStringQueryNode::generate_access_exprs(const TableItem &table_item, ObOptimizerContext &opt_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::generate_access_exprs(table_item, opt_ctx))) {
    LOG_WARN("failed to generate access exprs", K(ret));
  }
  const bool doc_id_as_domain_id = OB_INVALID_ID != doc_id_idx_tid_;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_infos_.count(); ++i) {
    if (OB_FAIL(index_infos_.at(i).generate_exprs(inv_idx_tids_.at(i), doc_id_as_domain_id, table_item, opt_ctx))) {
      LOG_WARN("failed to generate index info exprs", K(ret));
    }
  }
  return ret;
}

int ObQueryStringQueryNode::collect_exprs(ObIArray<ObRawExpr*> &query_exprs, ObIArray<ObRawExpr*> &score_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::collect_exprs(query_exprs, score_exprs))) {
    LOG_WARN("failed to collect exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_infos_.count(); ++i) {
    if (OB_FAIL(index_infos_.at(i).collect_exprs(query_exprs, score_exprs))) {
      LOG_WARN("failed to collect index info exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < field_boosts_.count(); ++i) {
    if (OB_FAIL(query_exprs.push_back(field_boosts_.at(i)))) {
      LOG_WARN("failed to push back field boost expr", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(query_exprs.push_back(minimum_should_match_))) {
    LOG_WARN("failed to push back minimum should match expr", K(ret), KPC_(minimum_should_match));
  } else if (OB_FAIL(query_exprs.push_back(default_operator_))) {
    LOG_WARN("failed to push back operator expr", K(ret), KPC_(default_operator));
  } else if (OB_FAIL(query_exprs.push_back(field_type_))) {
    LOG_WARN("failed to push back field type expr", K(ret), KPC_(field_type));
  }
  return ret;
}

int ObQueryStringQueryNode::get_all_index_ids(ObIArray<uint64_t> &index_ids) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObFullTextQueryNode::get_all_index_ids(index_ids))) {
    LOG_WARN("failed to get all index ids", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < inv_idx_tids_.count(); ++i) {
    if (OB_UNLIKELY(OB_INVALID_ID == inv_idx_tids_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid inv idx tid", K(ret), K(i), KPC(this));
    } else if (OB_FAIL(index_ids.push_back(inv_idx_tids_.at(i)))) {
      LOG_WARN("failed to append index id", K(ret));
    }
  }
  return ret;
}

int ObQueryStringQueryNode::explain_info(char *buf, int64_t buf_len, int64_t &pos, ExplainType type, int blank_space_count)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(print_blank_space(buf, buf_len, pos, blank_space_count))) {
    LOG_WARN("failed to print blank space", K(ret), K(blank_space_count));
  } else if (OB_FAIL(BUF_PRINTF("query string node: "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("index name=("))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  for (int i = 0; i < index_names_.count() && OB_SUCC(ret); i++) {
    if (i > 0 && OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%.*s", index_names_.at(i).length(), index_names_.at(i).ptr()))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(BUF_PRINTF(")"))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
