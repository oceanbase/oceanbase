/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "objit/common/ob_item_type.h"
#include "share/hybrid_search/ob_request_base.h"
#include <cstddef>
#include <cstdint>
#include <sys/types.h>
#define USING_LOG_PREFIX SERVER
#include "ob_query_parse.h"


namespace oceanbase
{
namespace share
{

int ObESQueryParser::parse(const common::ObString &req_str, ObQueryReqFromJson *&query_res)
{
  int ret = OB_SUCCESS;
  ObJsonNode *j_node = NULL;
  const char *syntaxerr = NULL;
  uint64_t err_offset = 0;
  uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG | ObJsonParser::JSN_UNIQUE_FLAG;
  if (OB_FAIL(ObJsonParser::parse_json_text(&alloc_, req_str.ptr(), req_str.length(), syntaxerr, &err_offset, j_node, parse_flag))) {
    LOG_WARN("failed to parse array text", K(ret), K(req_str), KCSTRING(syntaxerr), K(err_offset));
  } else {
    uint64_t count = j_node->element_count();
    ObQueryReqFromJson *query = NULL;
    ObQueryReqFromJson *knn = NULL;
    ObReqConstExpr *from_expr = NULL;
    ObReqConstExpr *size_expr = NULL;
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObString key;
      ObIJsonBase *req_node = NULL;
      if (OB_FAIL(j_node->get_key(i, key))) {
        LOG_WARN("fail to get key", K(ret), K(i));
      } else if (OB_FAIL(j_node->get_object_value(i, req_node))) {
        LOG_WARN("fail to get value", K(ret), K(i));
      } else if (key.case_compare("query") == 0) {
        if (OB_FAIL(parse_query(*req_node, query))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("knn") == 0) {
        if (OB_FAIL(parse_knn(*req_node, knn))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("_source") == 0) {
        if (OB_FAIL(parse_source(*req_node))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("from") == 0) {
        if (OB_FAIL(parse_const(*req_node, from_expr))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("size") == 0) {
        if (OB_FAIL(parse_const(*req_node, size_expr))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid query param", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret)) {
      out_cols_ = source_cols_.empty() ? &user_cols_ : &source_cols_;
      if (query != NULL && knn != NULL) {
        if (OB_FAIL(construct_hybrid_query(query, knn, query_res))) {
          LOG_WARN("fail to construct hybrid query", K(ret));
        }
      } else {
        query_res = (query == NULL ? knn : query);
        if (OB_ISNULL(query_res) && OB_FAIL(construct_all_query(query_res))) {
          LOG_WARN("fail to construct all query", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (size_expr == NULL && from_expr != NULL) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported sytnax in query, 'size' must be set when 'from' is specified", K(ret));
      } else {
        query_res->set_offset(from_expr);
        if (query_res->get_limit() == NULL) {
          query_res->set_limit(size_expr);
        } else if (size_expr != NULL && OB_FAIL(choose_limit(query_res, size_expr))) {
          LOG_WARN("fail to choose limit expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && !out_cols_->empty()) {
        if (OB_FAIL(set_output_columns(*query_res))) {
          LOG_WARN("fail to set output columns", K(ret));
        } else if (need_json_wrap_ && OB_FAIL(wrap_json_result(query_res))) {
          LOG_WARN("fail to wrap json result", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::choose_limit(ObQueryReqFromJson *query_res, ObReqConstExpr *size_expr)
{
  int ret = OB_SUCCESS;
  uint64_t limit_val = 0;
  uint64_t size_val = 0;
  if (!query_res->get_limit()->expr_name.is_numeric() ||
      !size_expr->expr_name.is_numeric()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd value type", K(ret), K(query_res->get_limit()->expr_name), K(size_expr->expr_name));
  } else if (OB_FAIL(convert_const_numeric(query_res->get_limit()->expr_name, limit_val))) {
    LOG_WARN("failed to get const value", K(ret), K(query_res->get_limit()->expr_name), K(size_expr->expr_name));
  } else if (OB_FAIL(convert_const_numeric(size_expr->expr_name, size_val))) {
    LOG_WARN("failed to get const value", K(ret), K(query_res->get_limit()->expr_name), K(size_expr->expr_name));
  } else if (size_val < limit_val) {
    query_res->set_limit(size_expr);
  }
  return ret;
}

int ObESQueryParser::convert_const_numeric(const ObString &cont_val, uint64_t &val)
{
  int ret = OB_SUCCESS;
  int err = 0;
  val = ObCharset::strntoull(cont_val.ptr(), cont_val.length(), 10, &err);
  if (err == 0) {
    if (val > UINT_MAX32) {
      ret = OB_ERR_INVALID_PARAM_ENCOUNTERED;
      LOG_WARN("input value out of range", K(ret), K(val));
    }
  } else {
    ret = OB_ERR_INVALID_PARAM_ENCOUNTERED;
    LOG_WARN("input value out of range", K(ret));
  }
  return ret;
}

int ObESQueryParser::convert_signed_const_numeric(const ObString &cont_val, int64_t &val)
{
  int ret = OB_SUCCESS;
  int err = 0;
  val = ObCharset::strntoll(cont_val.ptr(), cont_val.length(), 10, &err);
  if (err != 0) {
    ret = OB_ERR_INVALID_PARAM_ENCOUNTERED;
    LOG_WARN("input value must be a integer", K(ret));
  }
  return ret;
}

int ObESQueryParser::wrap_json_result(ObQueryReqFromJson *&query_res)
{
  int ret = OB_SUCCESS;

  ObReqExpr *j_obj_expr = NULL;
  ObReqExpr *j_arrayagg_expr = NULL;
  if (OB_ISNULL(j_obj_expr = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create json obj expr", K(ret));
  } else if (OB_ISNULL(j_arrayagg_expr = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create json array agg expr", K(ret));
  } else if (FALSE_IT(j_obj_expr->expr_name = "json_object")) {
  } else if (FALSE_IT(j_arrayagg_expr->expr_name = "json_arrayagg")) {
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < out_cols_->count(); i++) {
      ObReqConstExpr *col_name = NULL;
      if (OB_ISNULL(col_name = OB_NEWx(ObReqConstExpr, &alloc_, ObVarcharType))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create match expr", K(ret));
      } else if (FALSE_IT(col_name->expr_name = out_cols_->at(i))) {
      } else if (OB_FAIL(j_obj_expr->params.push_back(col_name))) {
        LOG_WARN("fail to append name", K(ret));
      } else {
        bool found = false;
        for (uint64_t j = 0; OB_SUCC(ret) && !found && j < query_res->select_items_.count(); j++) {
          ObReqExpr *sel_expr = query_res->select_items_.at(j);
          if ((!sel_expr->alias_name.empty() && out_cols_->at(i).case_compare(sel_expr->alias_name) == 0) ||
              (!sel_expr->expr_name.empty() && out_cols_->at(i).case_compare(sel_expr->expr_name) == 0)) {
            found = true;
          }
          if (found && OB_FAIL(j_obj_expr->params.push_back(sel_expr))) {
            LOG_WARN("fail to append select item", K(ret));
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to find output expr", K(ret), K(col_name->expr_name));
        }
      }
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < query_res->score_items_.count(); i++) {
      ObReqExpr *score = query_res->score_items_.at(i);
      ObReqConstExpr *col_name = NULL;
      ObReqColumnExpr *col = NULL;
      if (score->alias_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get score item alias", K(ret));
      } else if (OB_ISNULL(col_name = OB_NEWx(ObReqConstExpr, &alloc_, ObVarcharType))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create match expr", K(ret));
      } else if (FALSE_IT(col_name->expr_name = score->alias_name)) {
      } else if (OB_FAIL(j_obj_expr->params.push_back(col_name))) {
        LOG_WARN("fail to append name", K(ret));
      } else if (OB_ISNULL(col = OB_NEWx(ObReqColumnExpr, &alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create column expr", K(ret));
      } else if (FALSE_IT(col->expr_name = score->alias_name)) {
      } else if (OB_FAIL(j_obj_expr->params.push_back(col))) {
        LOG_WARN("fail to append name", K(ret));
      }
    }
    ObString sub_query_name;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(j_arrayagg_expr->params.push_back(j_obj_expr))) {
      LOG_WARN("fail to append json array agg param", K(ret));
    } else if (OB_FAIL(wrap_sub_query(sub_query_name, query_res))) {
      LOG_WARN("fail to push back sub query", K(ret));
    } else if (OB_FAIL(query_res->select_items_.push_back(j_arrayagg_expr))) {
      LOG_WARN("fail to append json array agg param", K(ret));
    } else {
      query_res->output_all_columns_ = false;
      ObString res_name("hits");
      j_arrayagg_expr->set_alias(res_name);
    }
  }
  return ret;
}

int ObESQueryParser::set_output_columns(ObQueryReqFromJson &query_res)
{
  int ret = OB_SUCCESS;
  query_res.output_all_columns_ = false;
  for (uint64_t i = 0; OB_SUCC(ret) && i < out_cols_->count(); i++) {
    ObReqColumnExpr *col = NULL;
    if (OB_ISNULL(col = OB_NEWx(ObReqColumnExpr, &alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create column expr", K(ret));
    } else if (FALSE_IT(col->expr_name = out_cols_->at(i))) {
    } else if (OB_FAIL(query_res.select_items_.push_back(col))) {
      LOG_WARN("failed to append output columns", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::parse_source(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObIJsonBase *val_node = NULL;
    if (OB_FAIL(req_node.get_array_element(i, val_node))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (val_node->json_type() != ObJsonNodeType::J_STRING) {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("unexpectd json type", K(ret), K(val_node->json_type()));
    } else {
      ObString field_name(val_node->get_data_length(), val_node->get_data());
      if (OB_FAIL(source_cols_.push_back(field_name))) {
        LOG_WARN("failed to append output columns", K(ret), K(field_name));
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_join_condition(const ObString &l_table, const ObString &r_table,
                                              const ObString &l_expr_name, const ObString &r_expr_name,
                                              ObItemType condition, ObReqOpExpr *&join_condition)
{
  int ret = OB_SUCCESS;
  ObReqColumnExpr *l_expr = nullptr;
  ObReqColumnExpr *r_expr = nullptr;
  if (OB_ISNULL(join_condition = OB_NEWx(ObReqOpExpr, &alloc_, condition))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_ISNULL(l_expr = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_ISNULL(r_expr = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  } else {
    l_expr->table_name = l_table;
    l_expr->expr_name = l_expr_name;
    r_expr->table_name = r_table;
    r_expr->expr_name = r_expr_name;
    if (OB_FAIL(join_condition->init(l_expr, r_expr, condition))) {
      LOG_WARN("fail to init op expr", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::construct_hybrid_query(ObQueryReqFromJson *fts, ObQueryReqFromJson *knn, ObQueryReqFromJson *&hybrid)
{
  int ret = OB_SUCCESS;
  ObString rowkey_hint("opt_param('hidden_column_visible', 'true')");
  ObReqColumnExpr *fts_rowkey = NULL;
  ObReqColumnExpr *knn_rowkey = NULL;
  ObReqColumnExpr *fts_score = NULL;
  ObReqColumnExpr *knn_score = NULL;
  ObReqJoinedTable *join_table = NULL;
  ObReqTable *fts_table = NULL;
  ObReqTable *knn_table = NULL;
  ObString fts_alias = "_fts";
  ObString knn_alias = "_vs";
  ObString score_alias = "_score";
  ObString rowkey = "__pk_increment";
  ObReqOpExpr *join_condition = NULL;
  ObReqOpExpr *score_res = NULL;
  OrderInfo *order_info = NULL;
  ObQueryReqFromJson *base_table_fts_req = NULL;
  ObQueryReqFromJson *base_table_knn_req = NULL;
  if (OB_FAIL(get_base_table_query(fts, base_table_fts_req))) {
    LOG_WARN("fail to get base table query", K(ret));
  } else if (OB_FAIL(get_base_table_query(knn, base_table_knn_req))) {
    LOG_WARN("fail to get base table query", K(ret));
  } else if (OB_ISNULL(hybrid = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(fts_rowkey = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(knn_rowkey = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(fts_score = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(knn_score = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(join_table = OB_NEWx(ObReqJoinedTable, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (FALSE_IT(fts_rowkey->expr_name = rowkey)) {
  } else if (FALSE_IT(knn_rowkey->expr_name = rowkey)) {
  } else if (FALSE_IT(fts_score->expr_name = "_keyword_score")) {
  } else if (FALSE_IT(knn_score->expr_name = "_semantic_score")) {
  } else if (FALSE_IT(fts_score->table_name = "_fts")) {
  } else if (FALSE_IT(knn_score->table_name = "_vs")) {
  } else if (OB_FAIL(base_table_fts_req->add_req_hint(rowkey_hint))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(base_table_knn_req->add_req_hint(rowkey_hint))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(base_table_fts_req->select_items_.push_back(fts_rowkey))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(base_table_knn_req->select_items_.push_back(knn_rowkey))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(construct_sub_query_table(fts_alias, fts, fts_table))) {
    LOG_WARN("fail to create sub query table", K(ret));
  } else if (OB_FAIL(construct_sub_query_table(knn_alias, knn, knn_table))) {
    LOG_WARN("fail to create sub query table", K(ret));
  } else if (OB_FAIL(construct_join_condition(fts_alias, knn_alias, rowkey, rowkey, T_OP_EQ, join_condition))) {
    LOG_WARN("fail to create op expr", K(ret));
  } else if (FALSE_IT(join_table->init(fts_table, knn_table, join_condition, ObReqJoinType::RIGHT_OUTER_JOIN))) {
  } else if (OB_FAIL(hybrid->from_items_.push_back(join_table))) {
    LOG_WARN("fail to append from item", K(ret));
  } else if (OB_FAIL(contruct_score_sum_expr(fts_score, knn_score, score_alias, score_res))) {
    LOG_WARN("fail to construct score sum expr", K(ret));
  } else if (OB_FAIL(hybrid->score_items_.push_back(score_res))) {
    LOG_WARN("fail to append select item", K(ret));
  } else if (OB_FAIL(construct_order_by_item(score_res, false, order_info))) {
    LOG_WARN("fail to construct order by item", K(ret));
  } else if (OB_FAIL(hybrid->order_items_.push_back(order_info))) {
    LOG_WARN("fail to push query order item", K(ret));
  } else {
    fts->output_all_columns_ = false;
    fts->score_alias_ = "_keyword_score";
    knn->score_alias_ = "_semantic_score";
    if (!fts->order_items_.empty()) {
      fts->order_items_.at(0)->order_item->set_alias(fts->score_alias_);
    }
    if (!out_cols_->empty()) {
      knn->output_all_columns_ = false;
      for (uint64_t i = 0; i < out_cols_->count() && OB_SUCC(ret); i++) {
        if (!is_inner_column(out_cols_->at(i))) {
          ObReqColumnExpr *out_col = NULL;
          if (OB_ISNULL(out_col = OB_NEWx(ObReqColumnExpr, &alloc_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create query request", K(ret));
          } else if (FALSE_IT(out_col->expr_name = out_cols_->at(i))) {
          } else if (OB_FAIL(knn->select_items_.push_back(out_col))) {
            LOG_WARN("fail to create query request", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (base_table_fts_req != fts && OB_FAIL(fts->select_items_.push_back(fts_rowkey))) {
      LOG_WARN("fail to create query request", K(ret));
    } else if (base_table_knn_req != knn && !knn->output_all_columns_ && OB_FAIL(knn->select_items_.push_back(knn_rowkey))) {
      LOG_WARN("fail to create query request", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::parse_basic_table(const ObString &table_name, ObQueryReqFromJson *query_req)
{
  int ret = OB_SUCCESS;
  ObReqTable *table = NULL;
  if (OB_ISNULL(table = OB_NEWx(ObReqTable, &alloc_, ReqTableType::BASE_TABLE, table_name, database_name_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(query_req->from_items_.push_back(table))) {
    LOG_WARN("fail to create query request", K(ret));
  }
  return ret;
}

int ObESQueryParser::parse_query(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if ((count = req_node.element_count()) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query clause must not be empty", K(ret));
  } else if (OB_ISNULL(query_req = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  }
  ObReqExpr *and_expr = NULL;
  bool has_search_clause = false;
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = NULL;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("bool") == 0) {
      if (has_search_clause) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("query clause must only contain one query term", K(ret));
      } else if (OB_FAIL(parse_bool_query(*sub_node, query_req, and_expr))) {
        LOG_WARN("fail to parse bool query", K(ret), K(i));
      } else {
        has_search_clause = true;
      }
    } else if (key.case_compare("rank_feature") == 0) {
      ObReqExpr *rank_feat = NULL;
      ObReqColumnExpr *column_expr = NULL;
      UNUSED(column_expr);
      if (has_search_clause) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("query clause must only contain one query term", K(ret));
      } else if (OB_FAIL(parse_rank_feature(*sub_node, rank_feat, column_expr))) {
        LOG_WARN("fail to parse must not clauses", K(ret), K(i));
      } else if (OB_FAIL(query_req->add_score_item(alloc_, rank_feat))) {
        LOG_WARN("fail to append rank feature expr", K(ret), K(i));
      } else {
        has_search_clause = true;
      }
    } else {
       // single clause
      ObReqExpr *expr = nullptr;
      ObReqExpr *where_condition = nullptr;
      if (has_search_clause) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("query clause must only contain one query term", K(ret));
      } else if (OB_FAIL(parse_single_term(req_node, *query_req, expr, where_condition))) {
        LOG_WARN("unexpectd json type", K(ret), K(i));
      } else if (OB_FAIL(query_req->add_score_item(alloc_, expr))) {
        LOG_WARN("failed add term to score items", K(ret), K(i));
      } else {
        has_search_clause = true;
        ObReqExpr *condition_expr = where_condition ? where_condition : expr;
        if (OB_FAIL(query_req->condition_items_.push_back(condition_expr))) {
          LOG_WARN("failed add term to query request", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(and_expr)) {
    if (OB_FAIL(query_req->condition_items_.push_back(and_expr))) {
      LOG_WARN("failed add term to query request", K(ret));
    }
  }
  OrderInfo *order_info = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_basic_table(table_name_, query_req))) {
    LOG_WARN("fail to parse basic table", K(ret));
  } else if (!query_req->score_items_.empty()) {
    if (OB_FAIL(construct_order_by_item(query_req->score_items_.at(0), false, order_info))) {
      LOG_WARN("fail to construct order by item", K(ret));
    } else if (OB_FAIL(query_req->order_items_.push_back(order_info))) {
      LOG_WARN("fail to push query order item", K(ret));
    }
  } else if (query_req->score_items_.empty()) {
    ObReqExpr *zero_score = NULL;
    if (OB_ISNULL(zero_score = OB_NEWx(ObReqConstExpr, &alloc_, ObDoubleType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create query request", K(ret));
    } else if (OB_FAIL(query_req->add_score_item(alloc_, zero_score))) {
      LOG_WARN("failed add term to score items", K(ret));
    } else {
      zero_score->expr_name = "0.0";
    }
  }
  return ret;
}

int ObESQueryParser::parse_bool_query(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&and_expr, bool need_cal_score /*= true*/)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    count = req_node.element_count();
  }
  ObReqExpr *and_inner_expr = NULL;
  and_expr = NULL;
  // Affects the default value of minimum_should_match.
  // IF exists must or filter, the default value of minimum_should_match will be 0.
  BoolQueryMinShouldMatchInfo bq_min_should_match_info;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(construct_minimum_should_match_info(req_node, bq_min_should_match_info))) {
      LOG_WARN("fail to check has minimum should match", K(ret));
    } else if (bq_min_should_match_info.has_minimum_should_match_) {
      count = count - 1;
    }
    if (OB_SUCC(ret) && count > 1 && OB_ISNULL(and_inner_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND, false))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create query request", K(ret));
    }
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = NULL;
    ObReqExpr *expr = NULL;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("must") == 0) {
      if (OB_FAIL(parse_must_clauses(*sub_node, query_req, expr, need_cal_score))) {
        LOG_WARN("fail to parse must clauses", K(ret), K(i));
      }
    } else if (key.case_compare("should") == 0) {
      if (OB_FAIL(parse_should_clauses(*sub_node, query_req, expr, bq_min_should_match_info, need_cal_score))) {
        LOG_WARN("fail to parse should clauses", K(ret), K(i));
      }
    } else if (key.case_compare("filter") == 0) {
      if (OB_FAIL(parse_filter_clauses(*sub_node, query_req, expr))) {
        LOG_WARN("fail to parse filter clauses", K(ret), K(i));
      }
    } else if (key.case_compare("must_not") == 0) {
      if (OB_FAIL(parse_must_not_clauses(*sub_node, query_req, expr))) {
        LOG_WARN("fail to parse must not clauses", K(ret), K(i));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sytnax in query", K(ret), K(key));
    }
    if (OB_FAIL(ret)) {
    } else if (and_inner_expr != NULL && expr != NULL) {
      if (OB_FAIL(and_inner_expr->params.push_back(expr))) {
        LOG_WARN("failed add term to and expr", K(ret), K(i));
      }
    } else if (expr != NULL) {
      and_expr = expr;
    }
  }
  if (OB_SUCC(ret) && and_inner_expr != NULL) {
    and_expr = and_inner_expr;
  }
  return ret;
}

int ObESQueryParser::parse_must_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&and_expr, bool need_cal_score /*= true*/)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = NULL;
  ObReqExpr *and_inner_expr = NULL;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    count = req_node.element_count();
  }
  if (OB_FAIL(ret)) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must clause must only has one key", K(ret));
  } else if (count > 1) {
    if (OB_ISNULL(and_inner_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND, false))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create query request", K(ret));
    } else {
      and_inner_expr->expr_name = "and";
    }
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObReqExpr *single_expr = nullptr;
    ObReqExpr *where_condition = nullptr;

    ObString key;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clause_val->get_key(0, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (key.case_compare("bool") == 0) {
      ObIJsonBase *sub_node = NULL;
      if (OB_FAIL(clause_val->get_object_value(0, sub_node))) {
        LOG_WARN("unexpectd json type", K(ret), K(i));
      } else if (OB_FAIL(parse_bool_query(*sub_node, query_req, single_expr, need_cal_score))) {
        LOG_WARN("fail to parse bool query", K(ret), K(i));
      } else if (OB_ISNULL(single_expr)) {
        if (OB_ISNULL(single_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create query request", K(ret));
        } else {
          single_expr->expr_name = "1";
        }
      }
    } else if (key.case_compare("rank_feature") == 0) {
      if (!need_cal_score) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support rank_feature sytnax in must_not or filter", K(ret), K(key));
      } else {
        ObReqExpr *rank_feat = NULL;
        ObIJsonBase *sub_node = NULL;
        ObReqColumnExpr *column_expr = NULL;
        ObReqOpExpr *is_not_expr = NULL;
        ObReqConstExpr *const_expr = NULL;
        if (OB_FAIL(clause_val->get_object_value(0, sub_node))) {
          LOG_WARN("unexpectd json type", K(ret), K(i));
        } else if (OB_FAIL(parse_rank_feature(*sub_node, rank_feat, column_expr))) {
          LOG_WARN("fail to parse must not clauses", K(ret), K(i));
        } else if (OB_FAIL(query_req->add_score_item(alloc_, rank_feat))) {
          LOG_WARN("fail to append rank feature expr", K(ret), K(i));
        } else if (OB_ISNULL(const_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObNullType))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create query request", K(ret));
        } else if (OB_FAIL(construct_op_expr(column_expr, const_expr, T_OP_IS_NOT, is_not_expr))) {
          LOG_WARN("fail to construct is not expr", K(ret), K(i));
        } else {
          const_expr->expr_name = "NULL";
          single_expr = is_not_expr;
        }
      }
    } else if (OB_FAIL(parse_single_term(*clause_val, *query_req, single_expr, where_condition))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_NOT_NULL(single_expr) && need_cal_score && OB_FAIL(query_req->add_score_item(alloc_, single_expr))) {
      LOG_WARN("failed add term to score items", K(ret), K(i));
    }
    if (OB_SUCC(ret)) {
      ObReqExpr *condition_expr = where_condition ? where_condition : single_expr;
      if (OB_NOT_NULL(and_inner_expr)) {
        if (OB_NOT_NULL(condition_expr) && OB_FAIL(and_inner_expr->params.push_back(condition_expr))) {
          LOG_WARN("failed add term to and expr", K(ret), K(i));
        }
      } else {
        and_expr = condition_expr;
      }
    }
  }
  if (OB_SUCC(ret) && and_inner_expr != NULL) {
    and_expr = and_inner_expr;
  }
  return ret;
}

int ObESQueryParser::parse_must_not_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&and_expr)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = nullptr;
  ObReqExpr *and_not_inner_expr = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    count = req_node.element_count();
  }
  if (OB_FAIL(ret)) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must not clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must not clause must only has one key", K(ret));
  } else if (count > 1) {
    if (OB_ISNULL(and_not_inner_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND, false))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create query request", K(ret));
    }
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObReqExpr *single_expr = nullptr;
    ObReqExpr *where_condition = nullptr;
    ObReqExpr *not_expr = nullptr;
    ObString key;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clause_val->get_key(0, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (key.case_compare("bool") == 0) {
      ObIJsonBase *sub_node = NULL;
      if (OB_FAIL(clause_val->get_object_value(0, sub_node))) {
        LOG_WARN("unexpectd json type", K(ret), K(i));
      } else if (OB_FAIL(parse_bool_query(*sub_node, query_req, single_expr, false))) {
        LOG_WARN("fail to parse bool query", K(ret), K(i));
      } else if (OB_ISNULL(single_expr)) {
        if (OB_ISNULL(single_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create query request", K(ret));
        } else {
          single_expr->expr_name = "1";
        }
      }
    } else if (key.case_compare("rank_feature") == 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support rank_feature sytnax in must_not", K(ret), K(key));
    } else if (OB_FAIL(parse_single_term(*clause_val, *query_req, single_expr, where_condition))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(not_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_NOT))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create query request", K(ret));
    } else if (OB_NOT_NULL(single_expr) && OB_FAIL(not_expr->params.push_back(single_expr))) {
      LOG_WARN("failed add term to and expr", K(ret), K(i));
    }

    ObReqExpr *condition_expr = where_condition ? where_condition : not_expr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(and_not_inner_expr)) {
      and_expr = condition_expr;
    } else if (OB_NOT_NULL(condition_expr) && OB_FAIL(and_not_inner_expr->params.push_back(condition_expr))) {
      LOG_WARN("failed add term to and expr", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && and_not_inner_expr != NULL) {
    and_expr = and_not_inner_expr;
  }
  return ret;
}

int ObESQueryParser::parse_should_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&and_expr, BoolQueryMinShouldMatchInfo &bq_min_should_match_info, bool need_cal_score /*= true*/)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = NULL;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    count = req_node.element_count();
  }
  if (OB_FAIL(ret)) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should clause must only has one key", K(ret));
  }
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> should_exprs;
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObReqExpr *single_expr = NULL;
    ObReqExpr *where_condition = NULL;
    ObString key;
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to parse should clause", K(ret), K(i));
    } else if (OB_FAIL(clause_val->get_key(0, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (key.case_compare("bool") == 0) {
      ObIJsonBase *sub_node = NULL;
      if (OB_FAIL(clause_val->get_object_value(0, sub_node))) {
        LOG_WARN("unexpectd json type", K(ret), K(i));
      } else {
        if (OB_FAIL(parse_bool_query(*sub_node, query_req, single_expr, need_cal_score))) {
          LOG_WARN("fail to parse bool query", K(ret), K(i));
        } else if (OB_ISNULL(single_expr)) {
          if (OB_ISNULL(single_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create query request", K(ret));
          } else {
            single_expr->expr_name = "1";
          }
        }
      }
    } else if (key.case_compare("rank_feature") == 0) {
      if (!need_cal_score) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not rank_feature sytnax in must_not or filter", K(ret), K(key));
      } else {
        ObReqExpr *rank_feat = NULL;
        ObIJsonBase *sub_node = NULL;
        ObReqColumnExpr *column_expr = NULL;
        UNUSED(column_expr);
        if (OB_FAIL(clause_val->get_object_value(0, sub_node))) {
          LOG_WARN("unexpectd json type", K(ret), K(i));
        } else if (OB_FAIL(parse_rank_feature(*sub_node, rank_feat, column_expr))) {
          LOG_WARN("fail to parse must not clauses", K(ret), K(i));
        } else if (OB_FAIL(query_req->add_score_item(alloc_, rank_feat))) {
          LOG_WARN("fail to append rank feature expr", K(ret), K(i));
        }
      }
    } else if (OB_FAIL(parse_single_term(*clause_val, *query_req, single_expr, where_condition))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_NOT_NULL(single_expr) && need_cal_score && OB_FAIL(query_req->add_score_item(alloc_, single_expr))) {
      LOG_WARN("fail to add unit expr to score items", K(ret), K(i));
    }
    if (OB_SUCC(ret) && key.case_compare("rank_feature") != 0) {
      ObReqExpr *unit_expr = (where_condition != NULL) ? where_condition : single_expr;
      if (OB_NOT_NULL(unit_expr) && OB_FAIL(should_exprs.push_back(unit_expr))) {
        LOG_WARN("fail to add unit expr to should exprs", K(ret), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!bq_min_should_match_info.has_where_condition_) {
    and_expr = NULL;
  } else {
    ObReqExpr *gt_expr = NULL;
    if (OB_FAIL(ret)) {
    } else if (!should_exprs.empty() && OB_FAIL(build_should_groups(bq_min_should_match_info.msm_count_, bq_min_should_match_info.minimum_should_match_, should_exprs, gt_expr))) {
      LOG_WARN("fail to build should groups", K(ret));
    } else {
      and_expr = gt_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_filter_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&and_expr)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = NULL;
  ObReqExpr *and_inner_expr = NULL;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    count = req_node.element_count();
  }
  if (OB_FAIL(ret)) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filter clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filter clause must only has one key", K(ret));
  } else if (count > 1) {
    if (OB_ISNULL(and_inner_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND, false))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create query request", K(ret));
    }
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObReqExpr *single_expr = nullptr;
    ObReqExpr *where_condition = nullptr;
    ObString key;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(clause_val->get_key(0, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (key.case_compare("bool") == 0) {
      ObIJsonBase *sub_node = NULL;
      if (OB_FAIL(clause_val->get_object_value(0, sub_node))) {
        LOG_WARN("unexpectd json type", K(ret), K(i));
      } else if (OB_FAIL(parse_bool_query(*sub_node, query_req, single_expr, false))) {
        LOG_WARN("fail to parse bool query", K(ret), K(i));
      } else if (OB_ISNULL(single_expr)) {
        if (OB_ISNULL(single_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create query request", K(ret));
        } else {
          single_expr->expr_name = "1";
        }
      }
    } else if (key.case_compare("rank_feature") == 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support rank_feature sytnax in filter", K(ret), K(key));
    } else if (OB_FAIL(parse_single_term(*clause_val, *query_req, single_expr, where_condition))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObReqExpr *condition_expr = where_condition ? where_condition : single_expr;
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(and_inner_expr)) {
      and_expr = condition_expr;
    } else if (OB_NOT_NULL(condition_expr) && OB_FAIL(and_inner_expr->params.push_back(condition_expr))) {
      LOG_WARN("failed add term to and expr", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && and_inner_expr != NULL) {
    and_expr = and_inner_expr;
  }
  return ret;
}

int ObESQueryParser::parse_single_term(ObIJsonBase &req_node, ObQueryReqFromJson &query_req, ObReqExpr *&expr, ObReqExpr *&where_condition)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT ||
      req_node.element_count() != 1) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()), K(req_node.element_count()));
  }
  ObString key;
  ObIJsonBase *sub_node = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_key(0, key))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (key.case_compare("range") == 0) {
    if (OB_FAIL(parse_range_condition(*sub_node, expr))) {
      LOG_WARN("fail to parse range condition", K(ret));
    }
  } else if (key.case_compare("match") == 0 &&
             OB_FAIL(parse_match_expr(*sub_node, query_req, expr))) {
    LOG_WARN("fail to parse match expr", K(ret));
  } else if (key.case_compare("term") == 0 &&
             OB_FAIL(parse_term_expr(*sub_node, expr))) {
    LOG_WARN("fail to parse term expr", K(ret));
  } else if (key.case_compare("query_string") == 0 &&
             OB_FAIL(parse_query_string_expr(*sub_node, expr, where_condition))) {
    LOG_WARN("fail to parse query string expr", K(ret));
  }
  // no match any case
  if (OB_SUCC(ret) && OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not supported sytnax in query", K(ret), K(key));
  }
  return ret;
}

int ObESQueryParser::construct_op_expr(ObReqExpr *l_param, ObReqExpr *r_param, ObItemType type, ObReqOpExpr *&cmp_expr, bool need_parentheses /*= true*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cmp_expr = OB_NEWx(ObReqOpExpr, &alloc_, type, need_parentheses))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_FAIL(cmp_expr->init(l_param, r_param, type))) {
    LOG_WARN("fail to init op expr", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_weighted_expr(ObReqExpr *base_expr, double weight, ObReqExpr *&weighted_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(base_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("base_expr is null", K(ret));
  } else if (weight == 1.0 || weight == -1.0) {
    weighted_expr = base_expr;
  } else {
    ObReqConstExpr *weight_const = nullptr;
    if (OB_ISNULL(weight_const = OB_NEWx(ObReqConstExpr, &alloc_, ObDoubleType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create weight const expr", K(ret));
    } else {
      char weight_str_tmp[32];
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(weight_str_tmp, sizeof(weight_str_tmp), pos, "%g", weight))) {
        LOG_WARN("fail to format weight string", K(ret));
      } else {
        char *weight_str = static_cast<char *>(alloc_.alloc(pos));
        if (OB_ISNULL(weight_str)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for weight string", K(ret));
        } else {
          MEMCPY(weight_str, weight_str_tmp, pos);
          weight_const->expr_name.assign_ptr(weight_str, pos);
          ObReqOpExpr *temp_expr = nullptr;
          if (OB_FAIL(construct_op_expr(base_expr, weight_const, T_OP_MUL, temp_expr, false))) {
            LOG_WARN("fail to construct weight multiplication", K(ret));
          } else {
            weighted_expr = temp_expr;
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_best_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                       const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                       ObReqExpr *&result_expr, ObReqExpr *&where_condition,
                                       QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_greatest_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < keyword_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < field_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = field_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObReqExpr *weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, weighted_expr))) {
            LOG_WARN("fail to construct field expr", K(ret));
          } else {
            field_weighted_exprs.push_back(weighted_expr);
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *greatest_expr = nullptr;
        if (field_weighted_exprs.count() == 1) {
          greatest_expr = field_weighted_exprs.at(0);
        } else {
          if (OB_ISNULL(greatest_expr = OB_NEWx(ObReqExpr, &alloc_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create greatest expr", K(ret));
          } else {
            greatest_expr->expr_name = "GREATEST";
            for (int64_t k = 0; OB_SUCC(ret) && k < field_weighted_exprs.count(); k++) {
              if (OB_FAIL(greatest_expr->params.push_back(field_weighted_exprs.at(k)))) {
                LOG_WARN("fail to add field match expr to greatest", K(ret), K(k));
              }
            }
          }
        }

        ObReqExpr *keyword_weighted_expr = nullptr;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(construct_weighted_expr(greatest_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword expr", K(ret));
          } else {
            keyword_greatest_exprs.push_back(keyword_weighted_expr);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (keyword_greatest_exprs.count() == 1) {
        result_expr = keyword_greatest_exprs.at(0);
      } else {
        ObReqOpExpr *add_expr = nullptr;
        if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create add expr for keyword scores", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < keyword_greatest_exprs.count(); i++) {
            if (OB_FAIL(add_expr->params.push_back(keyword_greatest_exprs.at(i)))) {
              LOG_WARN("fail to add keyword score to add expr", K(ret), K(i));
            }
          }
          if (OB_SUCC(ret)) {
            result_expr = add_expr;
          }
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(construct_query_string_condition(field_exprs, keyword_exprs, SCORE_TYPE_BEST_FIELDS, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to construct where condition for best_fields", K(ret));
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_cross_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                        const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                        ObReqExpr *&result_expr, ObReqExpr *&where_condition,
                                        QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_greatest_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < keyword_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < field_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = field_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          match_expr->score_type_ = SCORE_TYPE_CROSS_FIELDS;
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObReqExpr *weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, weighted_expr))) {
            LOG_WARN("fail to construct field expr", K(ret));
          } else {
            field_weighted_exprs.push_back(weighted_expr);
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *greatest_expr = nullptr;
        if (field_weighted_exprs.count() == 1) {
          greatest_expr = field_weighted_exprs.at(0);
        } else {
          if (OB_ISNULL(greatest_expr = OB_NEWx(ObReqExpr, &alloc_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create greatest expr", K(ret));
          } else {
            greatest_expr->expr_name = "GREATEST";
            for (int64_t k = 0; OB_SUCC(ret) && k < field_weighted_exprs.count(); k++) {
              if (OB_FAIL(greatest_expr->params.push_back(field_weighted_exprs.at(k)))) {
                LOG_WARN("fail to add field match expr to greatest", K(ret), K(k));
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          ObReqExpr *keyword_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(greatest_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword expr", K(ret));
          } else {
            keyword_greatest_exprs.push_back(keyword_weighted_expr);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (keyword_greatest_exprs.count() == 1) {
        result_expr = keyword_greatest_exprs.at(0);
      } else {
        ObReqOpExpr *add_expr = nullptr;
        if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create add expr for keyword scores", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < keyword_greatest_exprs.count(); i++) {
            if (OB_FAIL(add_expr->params.push_back(keyword_greatest_exprs.at(i)))) {
              LOG_WARN("fail to add keyword score to add expr", K(ret), K(i));
            }
          }
          if (OB_SUCC(ret)) {
            result_expr = add_expr;
          }
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(construct_query_string_condition(field_exprs, keyword_exprs, SCORE_TYPE_CROSS_FIELDS, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to construct where condition for best_fields", K(ret));
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_most_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                       const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                       ObReqExpr *&result_expr, ObReqExpr *&where_condition,
                                       QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;

  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_sum_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < keyword_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < field_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = field_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          match_expr->score_type_ = SCORE_TYPE_MOST_FIELDS;
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObReqExpr *weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, weighted_expr))) {
            LOG_WARN("fail to construct field expr", K(ret));
          } else {
            field_weighted_exprs.push_back(weighted_expr);
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *sum_expr = nullptr;
        if (field_weighted_exprs.count() == 1) {
          sum_expr = field_weighted_exprs.at(0);
        } else {
          ObReqOpExpr *add_expr = nullptr;
          if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create add expr for field sum", K(ret));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < field_weighted_exprs.count(); j++) {
              if (OB_FAIL(add_expr->params.push_back(field_weighted_exprs.at(j)))) {
                LOG_WARN("fail to add field weighted expr to sum", K(ret), K(j));
              }
            }
            if (OB_SUCC(ret)) {
              sum_expr = add_expr;
            }
          }
        }

        ObReqExpr *keyword_weighted_expr = nullptr;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(construct_weighted_expr(sum_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword expr", K(ret));
          } else {
            keyword_sum_exprs.push_back(keyword_weighted_expr);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (keyword_sum_exprs.count() == 1) {
        result_expr = keyword_sum_exprs.at(0);
      } else {
        ObReqOpExpr *add_expr = nullptr;
        if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create add expr for keyword sum", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < keyword_sum_exprs.count(); i++) {
            if (OB_FAIL(add_expr->params.push_back(keyword_sum_exprs.at(i)))) {
              LOG_WARN("fail to add keyword sum to add expr", K(ret), K(i));
            }
          }
          if (OB_SUCC(ret)) {
            result_expr = add_expr;
          }
        }
      }

      // deal with query_string with minimum_should_match
      if (OB_SUCC(ret) && OB_FAIL(construct_query_string_condition(field_exprs, keyword_exprs, SCORE_TYPE_MOST_FIELDS, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to construct where condition for most_fields", K(ret));
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_phrase(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                  const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                  ObReqExpr *&result_expr, ObReqExpr *&where_condition,
                                  QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    bool has_keyword_weight = false;
    for (int64_t i = 0; i < keyword_exprs.count(); i++) {
      ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
      if (keyword_expr->weight_ != -1.0) {
        has_keyword_weight = true;
        break;
      }
    }

    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_greatest_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < keyword_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_match_exprs;
      ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
      ObReqScoreType score_type = SCORE_TYPE_BEST_FIELDS;
      if (keyword_expr->weight_ == -1.0) {
        score_type = SCORE_TYPE_PHRASE;
      } else {
        score_type = SCORE_TYPE_BEST_FIELDS;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < field_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = field_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          match_expr->score_type_ = score_type;
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          ObReqExpr *field_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, field_weighted_expr))) {
            LOG_WARN("fail to construct field weighted expr", K(ret));
          } else if (OB_FAIL(field_match_exprs.push_back(field_weighted_expr))) {
            LOG_WARN("fail to add field weighted expr", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *greatest_expr = nullptr;
        if (field_match_exprs.count() == 1) {
          greatest_expr = field_match_exprs.at(0);
        } else {
          if (OB_ISNULL(greatest_expr = OB_NEWx(ObReqExpr, &alloc_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create greatest expr", K(ret));
          } else {
            greatest_expr->expr_name = "GREATEST";
            for (int64_t j = 0; OB_SUCC(ret) && j < field_match_exprs.count(); j++) {
              if (OB_FAIL(greatest_expr->params.push_back(field_match_exprs.at(j)))) {
                LOG_WARN("fail to add field match expr to greatest", K(ret), K(j));
              }
            }
          }
        }
        ObReqExpr *keyword_weighted_expr = nullptr;
        if (OB_SUCC(ret)) {
          if (OB_FAIL(construct_weighted_expr(greatest_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword weighted expr", K(ret));
          } else if (OB_FAIL(keyword_greatest_exprs.push_back(keyword_weighted_expr))) {
            LOG_WARN("fail to add keyword weighted expr", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (keyword_greatest_exprs.count() == 1) {
        result_expr = keyword_greatest_exprs.at(0);
      } else {
        ObReqOpExpr *add_expr = nullptr;
        if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create add expr for keyword scores", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < keyword_greatest_exprs.count(); i++) {
            if (OB_FAIL(add_expr->params.push_back(keyword_greatest_exprs.at(i)))) {
              LOG_WARN("fail to add keyword score to add expr", K(ret), K(i));
            }
          }
          if (OB_SUCC(ret)) {
            result_expr = add_expr;
          }
        }
      }

      if (OB_SUCC(ret) && OB_FAIL(construct_query_string_condition(field_exprs, keyword_exprs, SCORE_TYPE_PHRASE, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to construct where condition for phrase", K(ret));
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_range_condition(ObIJsonBase &req_node, ObReqExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT ||
       req_node.element_count() != 1) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()), K(req_node.element_count()));
  }
  ObString col_name;
  ObIJsonBase *sub_node = NULL;
  ObReqColumnExpr *col_para = NULL;
  ObReqOpExpr *first_expr = NULL;
  ObReqOpExpr *and_expr = NULL;
  int condition_num = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_key(0, col_name))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (OB_ISNULL(col_para = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create column expr", K(ret));
  } else {
    col_para->expr_name = col_name;
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < sub_node->element_count(); i++) {
    ObString key;
    ObIJsonBase *var_node = NULL;
    ObReqConstExpr *var = NULL;
    ObReqOpExpr *cmp_expr = NULL;
    ObItemType type = T_INVALID;
    if (OB_FAIL(sub_node->get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(sub_node->get_object_value(i, var_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (OB_FAIL(parse_const(*var_node, var))) {
      LOG_WARN("fail to parse const value", K(ret), K(i));
    } else if (key.case_compare("gt") == 0) {
      type = T_OP_GT;
      condition_num++;
    } else if (key.case_compare("gte") == 0) {
      type = T_OP_GE;
      condition_num++;
    } else if (key.case_compare("lt") == 0) {
      type = T_OP_LT;
      condition_num++;
    } else if (key.case_compare("lte") == 0) {
      type = T_OP_LE;
      condition_num++;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sytnax in query", K(ret), K(key));
    }
    if (OB_FAIL(ret)) {
    } else if (type != T_INVALID && OB_FAIL(construct_op_expr(col_para, var, type, cmp_expr, false))) {
      LOG_WARN("fail to construct cmp expr", K(ret));
    } else if (first_expr == NULL) {
      first_expr = cmp_expr;
      expr = cmp_expr;
    }
    if (OB_SUCC(ret) && condition_num > 1) {
      if (and_expr == NULL) {
        if (OB_FAIL(construct_op_expr(first_expr, cmp_expr, T_OP_AND, and_expr, false))) {
          LOG_WARN("fail to construct cmp expr", K(ret));
        } else {
          expr = and_expr;
        }
      } else if (OB_FAIL(and_expr->params.push_back(cmp_expr))) {
        LOG_WARN("fail to append cmp expr", K(ret));
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_rank_feature(ObIJsonBase &req_node, ObReqExpr *&rank_feat, ObReqColumnExpr *&column_expr)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()), K(req_node.element_count()));
  }
  ObRankFeatDef rank_feat_def;
  bool has_field = false;
  for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = NULL;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("field") == 0) {
      if (OB_FAIL(parse_field(*sub_node, rank_feat_def.number_field))) {
        LOG_WARN("fail to parse vec field", K(ret), K(i));
      } else {
        column_expr = rank_feat_def.number_field;
        has_field = true;
      }
    } else if (key.case_compare("saturation") == 0) {
      ObString empty_str;
      if (OB_FAIL(parse_rank_feat_param(*sub_node, "pivot", empty_str, rank_feat_def.pivot,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::SATURATION;
      }
    } else if (key.case_compare("sigmoid") == 0) {
      ObString ex_str = "exponent";
      if (OB_FAIL(parse_rank_feat_param(*sub_node, "pivot", ex_str, rank_feat_def.pivot,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::SIGMOID;
      }
    } else if (key.case_compare("linear") == 0) {
      ObString empty_str;
      if (OB_FAIL(parse_rank_feat_param(*sub_node, empty_str, empty_str, rank_feat_def.pivot,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::LINEAR;
      }
    } else if (key.case_compare("log") == 0) {
      ObString empty_str;
      if (OB_FAIL(parse_rank_feat_param(*sub_node, "scaling_factor", empty_str, rank_feat_def.scaling_factor,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::LOG;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpectd rank feature param", K(ret), K(key));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_field) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rank feature must has field", K(ret));
  } else if (OB_FAIL(construct_rank_feat_expr(rank_feat_def, rank_feat))) {
    LOG_WARN("fail to construct rank feature expr", K(ret));
  }

  return ret;
}

int ObESQueryParser::parse_rank_feat_param(ObIJsonBase &req_node, const ObString &para1, const ObString &para2,
                                           ObReqConstExpr *&const_para1, ObReqConstExpr *&const_para2, bool &positive)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *val1 = NULL;
  ObIJsonBase *val2 = NULL;
  ObString positive_str = "positive_score_impact";
  ObIJsonBase *pos_val = NULL;
  positive = true;
  if (!para1.empty() && OB_FAIL(req_node.get_object_value(para1, val1))) {
    LOG_WARN("fail to get pivot value", K(ret));
  } else if (!para2.empty() && OB_FAIL(req_node.get_object_value(para2, val2))) {
    LOG_WARN("fail to get pivot value", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(positive_str, pos_val))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get positive_score_impact value", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (val1 != NULL && OB_FAIL(parse_const(*val1, const_para1))) {
    LOG_WARN("fail to parse const value", K(ret));
  } else if (val2 != NULL && OB_FAIL(parse_const(*val2, const_para2))) {
    LOG_WARN("fail to parse const value", K(ret));
  } else if (pos_val != NULL) {
    positive = pos_val->get_boolean();
  }
  return ret;
}

int ObESQueryParser::construct_rank_feat_expr(const ObRankFeatDef &rank_feat_def, ObReqExpr *&rank_feat_expr)
{
  int ret = OB_SUCCESS;
  switch (rank_feat_def.type) {
    case ObRankFeatureType::SATURATION : {
      ObReqOpExpr *div_expr = NULL;
      ObReqOpExpr *add_expr = NULL;
      if (OB_ISNULL(div_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_DIV))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(rank_feat_def.number_field) || OB_ISNULL(rank_feat_def.pivot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(add_expr->params.push_back(rank_feat_def.number_field))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(add_expr->params.push_back(rank_feat_def.pivot))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (rank_feat_def.positive_impact && OB_FAIL(div_expr->params.push_back(rank_feat_def.number_field))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (!rank_feat_def.positive_impact && OB_FAIL(div_expr->params.push_back(rank_feat_def.pivot))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(div_expr->params.push_back(add_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else {
        rank_feat_expr = div_expr;
      }
      break;
    }
    case ObRankFeatureType::LINEAR : {
      if (rank_feat_def.positive_impact) {
        rank_feat_expr = rank_feat_def.number_field;
      } else {
        ObReqOpExpr *div_expr = NULL;
        ObReqConstExpr *one_expr = NULL;
        if (OB_ISNULL(div_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_DIV))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create op expr", K(ret));
        } else if (OB_ISNULL(one_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create constant expr", K(ret));
        } else if (OB_FAIL(div_expr->params.push_back(one_expr))) {
          LOG_WARN("failed to append param", K(ret));
        } else if (OB_FAIL(div_expr->params.push_back(rank_feat_def.number_field))) {
          LOG_WARN("failed to append param", K(ret));
        } else {
          one_expr->expr_name = "1";
          rank_feat_expr = div_expr;
        }
      }
      break;
    }
    case ObRankFeatureType::LOG : {
      // only positive impact
      ObReqExpr *ln_expr = NULL;
      ObReqOpExpr *add_expr = NULL;
      if (OB_ISNULL(ln_expr = OB_NEWx(ObReqExpr, &alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(rank_feat_def.number_field) || OB_ISNULL(rank_feat_def.scaling_factor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(add_expr->params.push_back(rank_feat_def.number_field))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(add_expr->params.push_back(rank_feat_def.scaling_factor))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(ln_expr->params.push_back(add_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else {
        ln_expr->expr_name = N_LN;
        rank_feat_expr = ln_expr;
      }
      break;
    }
    case ObRankFeatureType::SIGMOID : {
      ObReqExpr *field_pow_expr = NULL;
      ObReqExpr *piv_pow_expr = NULL;
      ObReqOpExpr *add_expr = NULL;
      ObReqOpExpr *div_expr = NULL;
      if (OB_ISNULL(field_pow_expr = OB_NEWx(ObReqExpr, &alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(piv_pow_expr = OB_NEWx(ObReqExpr, &alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(div_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_DIV))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(rank_feat_def.number_field) || OB_ISNULL(rank_feat_def.pivot) ||
                 OB_ISNULL(rank_feat_def.exponent)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(field_pow_expr->params.push_back(rank_feat_def.number_field))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(field_pow_expr->params.push_back(rank_feat_def.exponent))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(piv_pow_expr->params.push_back(rank_feat_def.pivot))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(piv_pow_expr->params.push_back(rank_feat_def.exponent))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(add_expr->params.push_back(field_pow_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(add_expr->params.push_back(piv_pow_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (rank_feat_def.positive_impact && OB_FAIL(div_expr->params.push_back(field_pow_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (!rank_feat_def.positive_impact && OB_FAIL(div_expr->params.push_back(piv_pow_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else if (OB_FAIL(div_expr->params.push_back(add_expr))) {
        LOG_WARN("failed to append param", K(ret));
      } else {
        field_pow_expr->expr_name = N_POW;
        piv_pow_expr->expr_name = N_POW;
        rank_feat_expr = div_expr;
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpect rank feature expr type", K(ret), K(rank_feat_def.type));
    }
  }
  return ret;
}

int ObESQueryParser::parse_match_expr(ObIJsonBase &req_node, ObQueryReqFromJson &query_req, ObReqExpr *&match_expr)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  ObString idx_name;
  ObIJsonBase* val_node = nullptr;
  if (OB_FAIL(req_node.get_object_value(0, col_name, val_node))) {
    LOG_WARN("fail to get value.", K(ret));
  } else if (val_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(val_node->json_type()));
  } else {
    ObString query_text(val_node->get_data_length(), val_node->get_data());
    ObReqColumnExpr *col_para = NULL;
    ObReqConstExpr *text_para = NULL;
    if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create match expr", K(ret));
    } else if (OB_ISNULL(col_para = OB_NEWx(ObReqColumnExpr, &alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN(" fail to create match expr", K(ret));
    } else if (OB_ISNULL(text_para = OB_NEWx(ObReqConstExpr, &alloc_, ObVarcharType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create match expr", K(ret));
    } else if (FALSE_IT(match_expr->expr_name = "match")) {
    } else if (FALSE_IT(col_para->expr_name = col_name)) {
    } else if (FALSE_IT(text_para->expr_name = query_text)) {
    } else if (OB_FAIL(match_expr->params.push_back(col_para))) {
      LOG_WARN("fail to push match expr param", K(ret));
    } else if (OB_FAIL(match_expr->params.push_back(text_para))) {
      LOG_WARN("fail to push match expr param", K(ret));
    } else if (OB_FAIL(get_match_idx_name(col_name, idx_name))) {
      LOG_WARN("fail to get match index name", K(ret));
    } else if (!idx_name.empty()) {
      if (query_req.match_idxs_.count() == 0) {
        // add table name first, for generate union merge hint
        if (OB_FAIL(query_req.match_idxs_.push_back(table_name_))) {
          LOG_WARN("fail to append table name", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(query_req.match_idxs_.push_back(idx_name))) {
        LOG_WARN("fail to append match index name", K(ret));
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_term_expr(ObIJsonBase &req_node, ObReqExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  ObIJsonBase *value_node = NULL;
  ObReqOpExpr *eq_expr = NULL;
  if (OB_FAIL(req_node.get_object_value(0, col_name, value_node))) {
    LOG_WARN("fail to get value.", K(ret));
  } else {
    ObReqColumnExpr *col_para = NULL;
    ObReqConstExpr *value_para = NULL;
    if (OB_FAIL(parse_const(*value_node, value_para))) {
      LOG_WARN("fail to parse const value", K(ret));
    } else if (OB_ISNULL(col_para = OB_NEWx(ObReqColumnExpr, &alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create term expr", K(ret));
    } else if (FALSE_IT(col_para->expr_name = col_name)) {
    } else if (OB_FAIL(construct_op_expr(col_para, value_para, T_OP_EQ, eq_expr))) {
      LOG_WARN("fail to construct eq expr", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    expr = eq_expr;
  }
  return ret;
}

int ObESQueryParser::parse_query_string_type(ObIJsonBase &req_node, ObReqScoreType &score_type)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *type_node = nullptr;
  ObString type_key("type");
  if (OB_FAIL(req_node.get_object_value(type_key, type_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      score_type = SCORE_TYPE_BEST_FIELDS;
      ret = OB_SUCCESS;
      LOG_DEBUG("type field not found in query_string, use default value", K(score_type));
    } else {
      LOG_WARN("fail to get type field", K(ret));
    }
  } else if (OB_ISNULL(type_node) || type_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("type field is null", K(ret));
  } else if (type_node->json_type() == ObJsonNodeType::J_STRING) {
    ObString type_str(type_node->get_data_length(), type_node->get_data());
    if (type_str.case_compare("best_fields") == 0) {
      score_type = SCORE_TYPE_BEST_FIELDS;
    } else if (type_str.case_compare("cross_fields") == 0) {
      score_type = SCORE_TYPE_CROSS_FIELDS;
    } else if (type_str.case_compare("most_fields") == 0) {
      score_type = SCORE_TYPE_MOST_FIELDS;
    } else if (type_str.case_compare("phrase") == 0) {
      score_type = SCORE_TYPE_PHRASE;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported query_string type", K(type_str));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("type field should be string", K(ret), K(type_node->json_type()));
  }
  return ret;
}

int ObESQueryParser::parse_query_string_fields(ObIJsonBase &req_node, common::ObIArray<ObReqColumnExpr *> &field_exprs)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *fields_node = nullptr;
  ObString fields_key("fields");
  if (OB_FAIL(req_node.get_object_value(fields_key, fields_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      LOG_WARN("fields field is required in query_string", K(ret));
    } else {
      LOG_WARN("fail to get fields field", K(ret));
    }
  } else if (OB_ISNULL(fields_node) || fields_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("fields field is null", K(ret));
  } else if (fields_node->json_type() == ObJsonNodeType::J_ARRAY) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < fields_node->element_count(); i++) {
      ObIJsonBase *field_node = nullptr;
      ObReqColumnExpr *field = nullptr;
      if (OB_FAIL(fields_node->get_array_element(i, field_node))) {
        LOG_WARN("fail to get field element", K(ret), K(i));
      } else if (OB_FAIL(parse_field(*field_node, field))) {
        LOG_WARN("fail to parse clause", K(ret), K(i));
      } else if (OB_FAIL(field_exprs.push_back(field))) {
        LOG_WARN("fail to add field to field_exprs", K(ret), K(i));
      }
    }
  } else if (fields_node->json_type() == ObJsonNodeType::J_STRING) {
    ObReqColumnExpr *field = nullptr;
    if (OB_FAIL(parse_field(*fields_node, field))) {
      LOG_WARN("fail to parse clause", K(ret));
    } else if (OB_FAIL(field_exprs.push_back(field))) {
      LOG_WARN("fail to add single field to field_exprs", K(ret));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("fields should be string or array", K(ret), K(fields_node->json_type()));
  }
  return ret;
}

int ObESQueryParser::parse_query_string_default_operator(ObIJsonBase &req_node, ObItemType &opr)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *operator_node = nullptr;
  ObString operator_key("default_operator");
  if (OB_FAIL(req_node.get_object_value(operator_key, operator_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      opr = T_OP_OR;
      ret = OB_SUCCESS;
      LOG_DEBUG("default_operator field not found in query_string, use default value OR", K(opr));
    } else {
      LOG_WARN("fail to get default_operator field", K(ret));
    }
  } else if (OB_ISNULL(operator_node) || operator_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("default_operator field is null", K(ret));
  } else if (operator_node->json_type() == ObJsonNodeType::J_STRING) {
    ObString operator_str(operator_node->get_data_length(), operator_node->get_data());
    if (operator_str.case_compare("AND") == 0) {
      opr = T_OP_AND;
    } else if (operator_str.case_compare("OR") == 0) {
      opr = T_OP_OR;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported default_operator value", K(operator_str));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("default_operator field should be string", K(ret), K(operator_node->json_type()));
  }
  return ret;
}

int ObESQueryParser::parse_query_string_query(ObIJsonBase &req_node, common::ObIArray<ObReqConstExpr *> &keyword_exprs, ObReqScoreType score_type)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *query_node = nullptr;
  ObString query_key("query");
  if (OB_FAIL(req_node.get_object_value(query_key, query_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      LOG_WARN("query field is required in query_string", K(ret));
    } else {
      LOG_WARN("fail to get query field", K(ret));
    }
  } else if (OB_ISNULL(query_node) || query_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("query field is null", K(ret));
  } else if (query_node->json_type() == ObJsonNodeType::J_STRING) {
    ObString query_text_str(query_node->get_data_length(), query_node->get_data());
    if (OB_FAIL(parse_keyword(query_text_str, keyword_exprs, score_type))) {
      LOG_WARN("fail to parse query clause", K(ret));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("query should be string", K(ret), K(query_node->json_type()));
  }
  return ret;
}

int ObESQueryParser::parse_query_string_boost(ObIJsonBase &req_node, ObReqExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObReqConstExpr *boost_expr = nullptr;
  ObIJsonBase *boost_node = nullptr;
  ObString boost_key("boost");
  if (OB_FAIL(req_node.get_object_value(boost_key, boost_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
      LOG_DEBUG("boost field not found in query_string, use default value");
    } else {
      LOG_WARN("fail to get boost field", K(ret));
    }
  } else if (OB_ISNULL(boost_node) || boost_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("boost field is null", K(ret));
  } else if (!boost_node->is_json_number(boost_node->json_type()) &&
             boost_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("boost field should be number or string", K(ret), K(boost_node->json_type()));
  } else if (OB_FAIL(parse_const(*boost_node, boost_expr))) {
    LOG_WARN("fail to parse boost value", K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(boost_expr)) {
    double boost_value = 1.0;
    ObString boost_str = boost_expr->expr_name;
    char *end_ptr = nullptr;
    boost_value = strtod(boost_str.ptr(), &end_ptr);
    if (boost_value < 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid boost value, must be non-negative", K(boost_value));
    } else if (boost_value != 1.0) {
      ObReqOpExpr *boost_mul_expr = nullptr;
      if (OB_FAIL(construct_op_expr(expr, boost_expr, T_OP_MUL, boost_mul_expr))) {
        LOG_WARN("fail to construct boost multiplication expr", K(ret));
      } else {
        expr = boost_mul_expr;
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_query_string_expr(ObIJsonBase &req_node, ObReqExpr *&query_string_expr, ObReqExpr *&where_condition)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    uint64_t count = req_node.element_count();
    if (count == 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("query_string should have at least one element", K(ret));
    } else {
      ObReqScoreType score_type;
      ObItemType default_operator;
      common::ObSEArray<ObReqColumnExpr *, 4, common::ModulePageAllocator, true> field_exprs;
      common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> keyword_exprs;
      QueryStringMinShouldMatchInfo qs_min_should_match_info;
      uint64_t parsed_keys = 0;

      if (OB_FAIL(parse_query_string_type(req_node, score_type))) {
        LOG_WARN("fail to parse query_string type", K(ret));
      } else if (++parsed_keys &&
                 OB_FAIL(parse_query_string_fields(req_node, field_exprs))) {
        LOG_WARN("fail to parse query_string fields", K(ret));
      } else if (++parsed_keys &&
                 OB_FAIL(parse_query_string_query(req_node, keyword_exprs, score_type))) {
        LOG_WARN("fail to parse query_string query", K(ret));
      } else if (++parsed_keys &&
                 OB_FAIL(parse_query_string_default_operator(req_node, default_operator))) {
        LOG_WARN("fail to parse query_string default operator", K(ret));
      } else if (++parsed_keys && default_operator != T_OP_AND &&
                 OB_FAIL(parse_minimum_should_match_with_query_string(req_node, keyword_exprs.count(), qs_min_should_match_info))) {
        LOG_WARN("fail to parse minimum_should_match", K(ret));
      } else if (++parsed_keys &&
                 OB_FAIL(parse_query_string_by_type(field_exprs, keyword_exprs, score_type, default_operator, query_string_expr, where_condition, qs_min_should_match_info))) {
        LOG_WARN("fail to parse query_string by type", K(ret));
      } else if (++parsed_keys &&
                 OB_FAIL(parse_query_string_boost(req_node, query_string_expr))) {
        LOG_WARN("fail to parse query_string boost", K(ret));
      } else {
        parsed_keys++;
      }

      if (OB_SUCC(ret) && parsed_keys != count) {
        for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
          ObString key;
          if (OB_FAIL(req_node.get_key(i, key))) {
            LOG_WARN("fail to get key", K(ret), K(i));
          } else if (key.case_compare("type") != 0 &&
                     key.case_compare("fields") != 0 &&
                     key.case_compare("query") != 0 &&
                     key.case_compare("default_operator") != 0 &&
                     key.case_compare("minimum_should_match") != 0 &&
                     key.case_compare("boost") != 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("unsupported key in query_string", K(ret), K(key));
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_field(ObIJsonBase &val_node, ObReqColumnExpr *&field)
{
  int ret = OB_SUCCESS;
  if (val_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(val_node.json_type()));
  } else {
    ObString field_name(val_node.get_data_length(), val_node.get_data());
    if (OB_ISNULL(field = OB_NEWx(ObReqColumnExpr, &alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create field(s) expr", K(ret));
    } else {
      const char *caret_ptr = field_name.find('^');
      if (caret_ptr != nullptr && caret_ptr > field_name.ptr()) {
        int64_t field_len = caret_ptr - field_name.ptr();
        ObString pure_field_name(field_len, field_name.ptr());
        field->expr_name = pure_field_name;
        const char *weight_start = caret_ptr + 1;
        int64_t weight_len = field_name.length() - field_len - 1;
        if (weight_len > 0) {
          // Make a null-terminated copy to safely use strtod
          char *tmp = static_cast<char *>(alloc_.alloc(static_cast<int32_t>(weight_len + 1)));
          if (OB_ISNULL(tmp)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory for field weight", K(ret), K(weight_len));
          } else {
            MEMCPY(tmp, weight_start, weight_len);
            tmp[weight_len] = '\0';
            char *end_ptr = nullptr;
            double weight = strtod(tmp, &end_ptr);
            // Skip trailing whitespace
            const char *end_limit = tmp + weight_len;
            while (end_ptr != nullptr && end_ptr < end_limit && (*end_ptr == ' ' || *end_ptr == '\t' || *end_ptr == '\r' || *end_ptr == '\n')) {
              end_ptr++;
            }
            int error = (end_ptr == end_limit) ? 0 : 1;
            if (OB_UNLIKELY(error != 0 || weight < 0)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid field weight", K(weight));
            } else {
              field->weight_ = weight;
            }
          }
        }
      } else {
        field->expr_name = field_name;
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_keyword(const ObString &query_text, common::ObIArray<ObReqConstExpr *> &keywords, ObReqScoreType score_type)
{
  int ret = OB_SUCCESS;
  if (query_text.empty()) {
    return ret;
  }
  common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> raw_keywords;
  const char *start = query_text.ptr();
  const char *end = start + query_text.length();
  const char *current = start;
  while (current < end && OB_SUCC(ret)) {
    while (current < end && *current == ' ') {
      current++;
    }
    if (current >= end) {
      break;
    }
    const char *keyword_start = current;
    while (current < end && *current != ' ' && *current != '^') {
      current++;
    }
    if (current > keyword_start) {
      int64_t keyword_len = current - keyword_start;
      ObString keyword_str(keyword_len, keyword_start);
      ObReqConstExpr *keyword = nullptr;
      if (OB_ISNULL(keyword = OB_NEWx(ObReqConstExpr, &alloc_, ObVarcharType))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create keyword expr", K(ret));
      } else {
        keyword->expr_name = keyword_str;
        if (current < end && *current == '^') {
          current++;
          const char *weight_start = current;
          while (current < end && *current != ' ') {
            current++;
          }
          if (current > weight_start) {
            char *end_ptr = nullptr;
            char temp_weight[32];
            int64_t weight_len = current - weight_start;
            if (weight_len >= sizeof(temp_weight)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("weight string too long", K(weight_len));
            } else {
              MEMCPY(temp_weight, weight_start, weight_len);
              temp_weight[weight_len] = '\0';
              ObString weight_str(weight_len, temp_weight);
              double weight = strtod(weight_str.ptr(), &end_ptr);
              int error = end_ptr == weight_str.ptr() + weight_str.length() ? 0 : 1;
              if (OB_UNLIKELY(error != 0 || weight < 0)) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("invalid keyword weight", K(weight));
              } else {
                keyword->weight_ = weight;
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(raw_keywords.push_back(keyword))) {
          LOG_WARN("fail to add raw keyword", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (score_type != SCORE_TYPE_CROSS_FIELDS) {
    common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> current_phrase_keywords;
    for (int64_t i = 0; OB_SUCC(ret) && i <= raw_keywords.count(); i++) {
      bool is_weighted = false;
      ObReqConstExpr *current_keyword = nullptr;
      if (i < raw_keywords.count()) {
        current_keyword = raw_keywords.at(i);
        is_weighted = (current_keyword->weight_ != -1.0);
      }
      if ((is_weighted || i == raw_keywords.count()) && current_phrase_keywords.count() > 0) {
        if (current_phrase_keywords.count() == 1) {
          if (OB_FAIL(keywords.push_back(current_phrase_keywords.at(0)))) {
            LOG_WARN("fail to add single unweighted keyword", K(ret));
          }
        } else {
          ObReqConstExpr *combined_keyword = nullptr;
          if (OB_ISNULL(combined_keyword = OB_NEWx(ObReqConstExpr, &alloc_, ObVarcharType))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create combined keyword expr", K(ret));
          } else {
            int64_t total_len = 0;
            for (int64_t j = 0; j < current_phrase_keywords.count(); j++) {
              total_len += current_phrase_keywords.at(j)->expr_name.length();
            }
            total_len += current_phrase_keywords.count() - 1;
            char *combined_str = static_cast<char *>(alloc_.alloc(total_len));
            if (OB_ISNULL(combined_str)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory for combined keyword", K(ret));
            } else {
              int64_t pos = 0;
              for (int64_t j = 0; j < current_phrase_keywords.count(); j++) {
                if (j > 0) {
                  combined_str[pos++] = ' ';
                }
                MEMCPY(combined_str + pos, current_phrase_keywords.at(j)->expr_name.ptr(), current_phrase_keywords.at(j)->expr_name.length());
                pos += current_phrase_keywords.at(j)->expr_name.length();
              }
              combined_keyword->expr_name.assign_ptr(combined_str, total_len);
              if (OB_FAIL(keywords.push_back(combined_keyword))) {
                LOG_WARN("fail to add combined phrase keyword", K(ret));
              }
            }
          }
        }
        current_phrase_keywords.reset();
      }
      if (OB_SUCC(ret) && i < raw_keywords.count() && is_weighted) {
        if (OB_FAIL(keywords.push_back(current_keyword))) {
          LOG_WARN("fail to add weighted keyword", K(ret));
        }
      } else if (OB_SUCC(ret) && i < raw_keywords.count() && !is_weighted) {
        if (OB_FAIL(current_phrase_keywords.push_back(current_keyword))) {
          LOG_WARN("fail to add keyword to phrase segment", K(ret));
          }
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < raw_keywords.count(); i++) {
        if (OB_FAIL(keywords.push_back(raw_keywords.at(i)))) {
          LOG_WARN("fail to add keyword", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_const(ObIJsonBase &val_node, ObReqConstExpr *&var)
{
  int ret = OB_SUCCESS;
    ObObjType var_type = val_node.is_json_number(val_node.json_type()) ? ObNumberType : ObVarcharType;
    if (OB_ISNULL(var = OB_NEWx(ObReqConstExpr, &alloc_, var_type))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create const expr", K(ret));
    } else {
      ObJsonBuffer j_buffer(&alloc_);
      if (OB_FAIL(val_node.print(j_buffer, false))) {
        LOG_WARN("fail to create const expr", K(ret));
      } else {
        j_buffer.get_result_string(var->expr_name);
    }
  }
  return ret;
}

int ObESQueryParser::construct_order_by_item(ObReqExpr *order_expr, bool ascent, OrderInfo *&order_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(order_info = OB_NEWx(OrderInfo, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create order info", K(ret));
  } else {
    order_info->order_item = order_expr;
    order_info->ascent = ascent;
  }
  return ret;
}

int ObESQueryParser::construct_required_params(const char *params_name[], uint32_t name_len, RequiredParamsSet &required_params)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(required_params.create(32))) {
    LOG_WARN("failed to create params name set", K(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < name_len; ++idx) {
      ObString para_name(strlen(params_name[idx]), params_name[idx]);
      if (OB_FAIL(required_params.set_refactored(para_name))) {
        LOG_WARN("failed to set_refactored object_ids", K(ret));
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  RequiredParamsSet required_params;
  const char *params_name[] = {"field", "k", "query_vector"};
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_ISNULL(query_req = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(construct_required_params(params_name, 3, required_params))) {
    LOG_WARN("fail to create required params set", K(ret));
  }
  ObReqColumnExpr *vec_field = NULL;
  ObReqConstExpr *query_vec = NULL;
  ObReqConstExpr *K = NULL;
  ObReqConstExpr *boost = NULL;
  ObReqConstExpr *similar = NULL;
  ObReqExpr *dist_vec = NULL;
  OrderInfo *order_info = NULL;
  ObReqExpr *filter_expr = NULL;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> score_array;
  for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = NULL;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("field") == 0) {
      if (OB_FAIL(parse_field(*sub_node, vec_field))) {
        LOG_WARN("fail to parse vec field", K(ret), K(i));
      } else if (OB_FAIL(required_params.erase_refactored("field"))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("k") == 0) {
      if (OB_FAIL(parse_const(*sub_node, K))) {
        LOG_WARN("fail to parse k constant", K(ret), K(i));
      } else if (OB_FAIL(required_params.erase_refactored("k"))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("query_vector") == 0) {
      if (OB_FAIL(parse_const(*sub_node, query_vec))) {
        LOG_WARN("fail to parse query vector", K(ret), K(i));
      } else if (OB_FAIL(required_params.erase_refactored("query_vector"))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(parse_const(*sub_node, boost))) {
        LOG_WARN("fail to parse boost clauses", K(ret), K(i));
      }
    } else if (key.case_compare("similarity") == 0 ) {
      if (OB_FAIL(parse_const(*sub_node, similar))) {
        LOG_WARN("fail to parse similarity clauses", K(ret), K(i));
      }
    } else if (key.case_compare("rank_feature") == 0) {
      ObReqExpr *rank_feat = NULL;
      ObReqColumnExpr *column_expr = NULL;
      UNUSED(column_expr);
      if (OB_FAIL(parse_rank_feature(*sub_node, rank_feat, column_expr))) {
        LOG_WARN("fail to parse must not clauses", K(ret), K(i));
      } else if (OB_FAIL(query_req->add_score_item(alloc_, rank_feat))) {
        LOG_WARN("fail to append rank feature expr", K(ret), K(i));
      }
    } else if (key.case_compare("num_candidates") == 0) {
      // do nothing, ignore
    } else if (key.case_compare("filter") == 0) {
      if (OB_FAIL(parse_filter_clauses(*sub_node, query_req, filter_expr))) {
        LOG_WARN("fail to parse filter clauses", K(ret), K(i));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sytnax in query", K(ret), K(key));
    }
  }
  // construct normalize expr
  ObReqExpr *normalize_expr = NULL;
  ObReqOpExpr *div_expr = NULL;
  ObReqOpExpr *add_expr = NULL;
  ObReqConstExpr *norm_const = NULL;
  ObReqConstExpr *round_const = NULL;
  ObReqOpExpr *boost_expr = NULL;
  ObReqExpr *order_by_4ip = NULL;
  ObVectorIndexDistAlgorithm alg_type = ObVectorIndexDistAlgorithm::VIDA_L2;
  if (OB_FAIL(ret)) {
  } else if (!required_params.empty()) {
    ret = OB_ERR_PARSER_SYNTAX;
    ObString param_name = required_params.begin()->first;
    LOG_WARN("query required params is missed", K(ret), K(param_name));
  } else if (OB_ISNULL(dist_vec = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create distance expr", K(ret));
  } else if (OB_ISNULL(normalize_expr = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_ISNULL(div_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_DIV))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_ISNULL(add_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_ISNULL(norm_const = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_ISNULL(round_const = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (filter_expr != NULL && OB_FAIL(query_req->condition_items_.push_back(filter_expr))) {
    LOG_WARN("fail to push query item", K(ret));
  } else if (OB_FAIL(parse_basic_table(table_name_, query_req))) {
    LOG_WARN("fail to parse basic table", K(ret));
  } else if (OB_FAIL(get_distance_algor_type(*vec_field, alg_type))) {
    LOG_WARN("fail to get distance algo type", K(ret));
  } else if (alg_type == ObVectorIndexDistAlgorithm::VIDA_IP) {
    if (OB_FAIL(construct_ip_expr(vec_field, query_vec, div_expr, add_expr, order_by_4ip))) {
      LOG_WARN("fail to construct ip expr", K(ret));
    } else if (OB_FAIL(construct_order_by_item(order_by_4ip, true, order_info))) {
      LOG_WARN("fail to construct order by item", K(ret));
    } else if (OB_FAIL(query_req->select_items_.push_back(add_expr))) {
      LOG_WARN("fail to push query item", K(ret));
    } else {
      add_expr->set_alias(ObString(strlen("_distance"), "_distance"));
    }
  } else if (alg_type != ObVectorIndexDistAlgorithm::VIDA_IP) {
    if (OB_FAIL(set_distance_score_expr(alg_type, norm_const, dist_vec, add_expr, div_expr))) {
      LOG_WARN("fail to set distance expr", K(ret));
    } else if (OB_FAIL(dist_vec->params.push_back(vec_field))) {
      LOG_WARN("fail to push vector distance expr param", K(ret));
    } else if (OB_FAIL(dist_vec->params.push_back(query_vec))) {
      LOG_WARN("fail to push vector distance expr param", K(ret));
    } else if (OB_FAIL(construct_order_by_item(dist_vec, true, order_info))) {
      LOG_WARN("fail to construct order by item", K(ret));
    } else if (OB_FAIL(query_req->select_items_.push_back(dist_vec))) {
      LOG_WARN("fail to push query item", K(ret));
    } else {
      dist_vec->set_alias(ObString(strlen("_distance"), "_distance"));
    }
  }

  if (OB_SUCC(ret)) {
    query_req->set_vec_approx();
    query_req->set_limit(K);
    normalize_expr->expr_name = "round";
    round_const->expr_name = "8";
    if (OB_FAIL(normalize_expr->params.push_back(div_expr))) {
      LOG_WARN("fail to push back expr param", K(ret));
    } else if (OB_FAIL(normalize_expr->params.push_back(round_const))) {
      LOG_WARN("fail to push back expr param", K(ret));
    } else if (OB_FAIL(query_req->order_items_.push_back(order_info))) {
      LOG_WARN("fail to push query order item", K(ret));
    } else if (boost != NULL) {
      if (OB_ISNULL(boost_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_MUL))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_FAIL(boost_expr->init(normalize_expr, boost, T_OP_MUL))) {
        LOG_WARN("fail to push vector distance expr param", K(ret));
      } else if (OB_FAIL(query_req->add_score_item(alloc_, boost_expr))) {
        LOG_WARN("failed add term to score items", K(ret));
      }
    } else {
      if (OB_FAIL(query_req->add_score_item(alloc_, normalize_expr))) {
        LOG_WARN("fail to push query item", K(ret));
      }
    }
    if (OB_SUCC(ret) && similar != NULL) {
      if (alg_type == ObVectorIndexDistAlgorithm::VIDA_IP) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported sytnax in query", K(ret));
      } else if (OB_FAIL(construct_query_with_similarity(dist_vec, similar, query_req))) {
        LOG_WARN("fail to construct query with similarity", K(ret));
      }
    }
  }

  return ret;
}

int ObESQueryParser::construct_ip_expr(ObReqColumnExpr *vec_field, ObReqConstExpr *query_vec, ObReqOpExpr *div_expr/* score */,
                                       ObReqOpExpr *minus_expr/* distance */, ObReqExpr *&order_by_vec)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *multi_expr = NULL;
  ObReqExpr *vec_norm1 = NULL;
  ObReqExpr *vec_norm2 = NULL;
  ObReqExpr *abs_expr = NULL;
  ObReqConstExpr *one_const = NULL;
  if (OB_ISNULL(order_by_vec = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create distance expr", K(ret));
  } else if (OB_ISNULL(vec_norm1 = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create distance expr", K(ret));
  } else if (OB_ISNULL(vec_norm2 = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create distance expr", K(ret));
  } else if (OB_ISNULL(multi_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_MUL))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_ISNULL(abs_expr = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_ISNULL(one_const = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_FAIL(order_by_vec->params.push_back(vec_field))) {
    LOG_WARN("fail to push back expr param", K(ret));
  } else if (OB_FAIL(order_by_vec->params.push_back(query_vec))) {
    LOG_WARN("fail to push back expr param", K(ret));
  } else if (OB_FAIL(abs_expr->params.push_back(order_by_vec))) {
    LOG_WARN("fail to push back expr param", K(ret));
  } else if (OB_FAIL(vec_norm1->params.push_back(vec_field))) {
    LOG_WARN("fail to push back expr param", K(ret));
  } else if (OB_FAIL(vec_norm2->params.push_back(query_vec))) {
    LOG_WARN("fail to push back expr param", K(ret));
  } else if (OB_FAIL(multi_expr->init(vec_norm1, vec_norm2, T_OP_MUL))) {
    LOG_WARN("fail to init add expr", K(ret));
  } else if (OB_FAIL(div_expr->init(abs_expr, multi_expr, T_OP_DIV))) {
    LOG_WARN("fail to init add expr", K(ret));
  } else if (OB_FAIL(minus_expr->init(one_const, div_expr, T_OP_MINUS))) {
    LOG_WARN("fail to init add expr", K(ret));
  } else {
    order_by_vec->expr_name = N_VECTOR_NEGATIVE_INNER_PRODUCT;
    vec_norm1->expr_name = N_VECTOR_NORM;
    vec_norm2->expr_name = N_VECTOR_NORM;
    abs_expr->expr_name = N_ABS;
    one_const->expr_name = "1";
  }

  return ret;
}

int ObESQueryParser::get_distance_algor_type(const ObReqColumnExpr &vec_field, ObVectorIndexDistAlgorithm &alg_type)
{
  int ret = OB_SUCCESS;
  ObColumnIndexInfo *index_info = nullptr;
  if (!index_name_map_.created()) {
    // do nothing
  } else if (OB_FAIL(index_name_map_.get_refactored(vec_field.expr_name, index_info))) {
    LOG_WARN("fail to get vector index info", K(ret), K(vec_field.expr_name));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(index_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpectd null ptr", K(ret), K(vec_field.expr_name));
  } else {
    alg_type = index_info->dist_algorithm_;
  }
  return ret;
}

int ObESQueryParser::get_match_idx_name(const ObString &match_field, ObString &idx_name)
{
  int ret = OB_SUCCESS;
  ObColumnIndexInfo *index_info = nullptr;
  if (!index_name_map_.created()) {
    // do nothing
  } else if (OB_FAIL(index_name_map_.get_refactored(match_field, index_info))) {
    LOG_WARN("fail to get vector index info", K(ret), K(match_field));
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(index_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpectd null ptr", K(ret), K(match_field));
  } else {
    idx_name = index_info->index_name_;
  }
  return ret;
}

int ObESQueryParser::set_distance_score_expr(const ObVectorIndexDistAlgorithm alg_type, ObReqConstExpr *norm_const, ObReqExpr *dist_vec,
                                             ObReqOpExpr *add_expr, ObReqOpExpr *&score_expr)
{
  int ret = OB_SUCCESS;
  switch (alg_type) {
    case ObVectorIndexDistAlgorithm::VIDA_L2 : {
      // l2_distance : score = 1 / (1 + l2_distance)
      dist_vec->expr_name = N_VECTOR_L2_DISTANCE;
      norm_const->expr_name = "1";
      if (OB_FAIL(add_expr->init(norm_const, dist_vec, T_OP_ADD))) {
        LOG_WARN("fail to init add expr", K(ret));
      } else if (OB_FAIL(score_expr->init(norm_const, add_expr, T_OP_DIV))) {
        LOG_WARN("fail to init div expr", K(ret));
      }
      break;
    }
    case ObVectorIndexDistAlgorithm::VIDA_COS : {
      dist_vec->expr_name = N_VECTOR_COS_DISTANCE;
      norm_const->expr_name = "2";
      ObReqOpExpr *minus_expr = NULL;
      ObReqConstExpr *const_minus = NULL;
      if (OB_ISNULL(minus_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_MINUS))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_ISNULL(const_minus = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_FAIL(score_expr->init(dist_vec, norm_const, T_OP_DIV))) {
        LOG_WARN("fail to init div expr", K(ret));
      } else if (OB_FAIL(minus_expr->init(const_minus, score_expr, T_OP_MINUS))) {
        LOG_WARN("fail to init div expr", K(ret));
      } else {
        // cos_distance : score = 1 - (cos_distance / 2)
        const_minus->expr_name = "1";
        score_expr = minus_expr;
      }
      break;
    }
    default : {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpectd dist algorithm type", K(ret), K(alg_type));
    }
  }
  return ret;
}

int ObESQueryParser::contruct_score_sum_expr(ObReqExpr *fts_score, ObReqExpr *vs_score, ObString &score_alias, ObReqOpExpr *&score)
{
  int ret = OB_SUCCESS;
  ObReqExpr *if_null_fts = NULL;
  ObReqExpr *if_null_vs = NULL;
  ObReqConstExpr *zero_const = NULL;
  if (OB_ISNULL(if_null_fts = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create if null expr", K(ret));
  } else if (OB_ISNULL(if_null_vs = OB_NEWx(ObReqExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create if null expr", K(ret));
  } else if (OB_ISNULL(zero_const = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create constant expr", K(ret));
  } else if (OB_FAIL(if_null_fts->params.push_back(fts_score))) {
    LOG_WARN("fail to append param", K(ret));
  } else if (OB_FAIL(if_null_fts->params.push_back(zero_const))) {
    LOG_WARN("fail to append param", K(ret));
  } else if (OB_FAIL(if_null_vs->params.push_back(vs_score))) {
    LOG_WARN("fail to append param", K(ret));
  } else if (OB_FAIL(if_null_vs->params.push_back(zero_const))) {
    LOG_WARN("fail to append param", K(ret));
  } else if (OB_FAIL(construct_op_expr(if_null_fts, if_null_vs, T_OP_ADD, score))) {
    LOG_WARN("fail to construct op expr", K(ret));
  } else {
    if_null_fts->expr_name = N_IFNULL;
    if_null_vs->expr_name = N_IFNULL;
    zero_const->expr_name = "0";
    score->set_alias(score_alias);
  }
  return ret;
}

int ObESQueryParser::construct_sub_query_table(ObString &sub_query_name, ObQueryReqFromJson *query_req, ObReqTable *&sub_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_query = OB_NEWx(ObReqTable, &alloc_, ReqTableType::SUB_QUERY, sub_query_name, database_name_, query_req))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else {
    sub_query->alias_name_ = sub_query_name;
  }
  return ret;
}

int ObESQueryParser::wrap_sub_query(ObString &sub_query_name, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObQueryReqFromJson *wrap_query = NULL;
  ObReqTable *sub_query = NULL;
  if (OB_ISNULL(wrap_query = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(construct_sub_query_table(sub_query_name, query_req, sub_query))) {
    LOG_WARN("fail to create sub query table", K(ret));
  } else if (OB_FAIL(wrap_query->from_items_.push_back(sub_query))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else {
    query_req = wrap_query;
  }

  return ret;
}

int ObESQueryParser::construct_query_with_similarity(ObReqExpr *dist, ObReqConstExpr *similar, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *le = NULL;
  ObReqColumnExpr *col = NULL;
  ObString sub_query_name("_vs0");
  ObReqColumnExpr *score_col = NULL;
  if (OB_ISNULL(le = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_LE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(col = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(wrap_sub_query(sub_query_name, query_req))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else if (OB_FAIL(query_req->condition_items_.push_back(le))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else if (OB_FAIL(le->init(col, similar, T_OP_LE))) {
    LOG_WARN("fail to init expr", K(ret));
  } else if (OB_ISNULL(score_col = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else {
    col->table_name = sub_query_name;
    col->expr_name = (dist->alias_name.empty() ? dist->expr_name : dist->alias_name);
    score_col->table_name = sub_query_name;
    score_col->expr_name = "_score";
    if (OB_FAIL(query_req->add_score_item(alloc_, score_col))) {
      LOG_WARN("fail to add score item", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::construct_minimum_should_match_info(ObIJsonBase &req_node, BoolQueryMinShouldMatchInfo &bq_min_should_match_info)
{
  int ret = OB_SUCCESS;
  bool has_must = false;
  bool has_filter = false;
  int64_t should_count = 0;
  bq_min_should_match_info.msm_count_ = 0;
  bq_min_should_match_info.minimum_should_match_ = NULL;
  bq_min_should_match_info.has_where_condition_ = true;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
      ObString key;
      ObIJsonBase *sub_node = NULL;
      if (OB_FAIL(req_node.get_key(i, key))) {
        LOG_WARN("fail to get key", K(ret), K(i));
      } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
        LOG_WARN("fail to get value", K(ret), K(i));
      } else if (key.case_compare("minimum_should_match") == 0) {
        bq_min_should_match_info.has_minimum_should_match_ = true;
      } else if (key.case_compare("must") == 0) {
        has_must = true;
      } else if (key.case_compare("filter") == 0) {
        has_filter = true;
      } else if (key.case_compare("should") == 0) {
        if (sub_node->json_type() == ObJsonNodeType::J_ARRAY) {
          should_count = sub_node->element_count();
        } else if (sub_node->json_type() == ObJsonNodeType::J_OBJECT) {
          should_count = 1;
        } else {
          ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
          LOG_WARN("should should be array or object", K(ret), K(sub_node->json_type()));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_minimum_should_match_with_bool_query(req_node, should_count, bq_min_should_match_info))) {
    LOG_WARN("fail to parse minimum should match", K(ret));
  } else if (OB_ISNULL(bq_min_should_match_info.minimum_should_match_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minimum_should_match is null", K(ret));
  } else if (bq_min_should_match_info.msm_count_ == 0) {
    if (has_must || has_filter) {
      bq_min_should_match_info.has_where_condition_ = false;
      bq_min_should_match_info.msm_count_ = 0;
      bq_min_should_match_info.minimum_should_match_->expr_name.assign_ptr("0", 1);
    } else {
      bq_min_should_match_info.msm_count_ = 1;
      bq_min_should_match_info.minimum_should_match_->expr_name.assign_ptr("1", 1);
    }
  }
  return ret;
}

int ObESQueryParser::parse_minimum_should_match_with_percentage(const common::ObString &percent_str, const int64_t term_count, uint64_t &msm_count)
{
  int ret = OB_SUCCESS;
  ObString num_part;
  if (percent_str.length() < 2 || percent_str.ptr()[percent_str.length() - 1] != '%') {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("percent_str is not a valid percentage string", K(ret), K(percent_str));
  } else {
    num_part.assign_ptr(percent_str.ptr(), static_cast<int32_t>(percent_str.length() - 1));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_minimum_should_match_with_integer(num_part, true, term_count, msm_count))) {
    LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(num_part));
  }
  return ret;
}

int ObESQueryParser::parse_minimum_should_match_with_integer(const common::ObString &val_str, const bool is_percentage_value, const int64_t term_count, uint64_t &msm_count)
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  if (val_str.length() == 0 || !val_str.is_numeric()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("string value is empty or not numeric", K(ret), K(val_str));
  } else if (OB_FAIL(convert_signed_const_numeric(val_str, val))) {
    LOG_WARN("failed to convert string value to number", K(ret), K(val_str));
  } else if (is_percentage_value) {
    val = (term_count * val) / 100;
  }

  if (OB_FAIL(ret)) {
  } else if (val < 0) {
    int64_t new_val = term_count + val;
    if (new_val < 0) {
      msm_count = 0;
    } else {
      msm_count = static_cast<uint64_t>(new_val);
    }
  } else {
    msm_count = static_cast<uint64_t>(val);
  }

  return ret;
}

int ObESQueryParser::parse_minimum_should_match_with_query_string(ObIJsonBase &req_node, const int64_t key_count, QueryStringMinShouldMatchInfo &qs_min_should_match_info)
{
  int ret = OB_SUCCESS;
  uint64_t min_should_match_value = 0;
  ObString minimum_should_match_str("minimum_should_match");
  ObIJsonBase *minimum_should_match_node = NULL;
  if (key_count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key word count of query_string is 0", K(ret), K(key_count));
  }

  if (OB_FAIL(ret)) {
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("minimum_should_match should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(req_node.get_object_value(minimum_should_match_str, minimum_should_match_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      qs_min_should_match_info.minimum_should_match_ = NULL;
      ret = OB_SUCCESS;
      LOG_DEBUG("minimum_should_match is not found", K(ret));
    } else {
      LOG_WARN("fail to get minimum should match node", K(ret));
    }
  } else if (OB_FAIL(parse_const(*minimum_should_match_node, qs_min_should_match_info.minimum_should_match_))) {
    LOG_WARN("fail to parse minimum should match expr", K(ret));
  } else if (OB_NOT_NULL(qs_min_should_match_info.minimum_should_match_)) {
    const ObString &msm_str = qs_min_should_match_info.minimum_should_match_->expr_name.trim();
    if (msm_str.length() > 0 && msm_str.ptr()[msm_str.length() - 1] == '%') {
      if (OB_FAIL(parse_minimum_should_match_with_percentage(msm_str, key_count, min_should_match_value))) {
        LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(msm_str));
      }
    } else if (msm_str.is_numeric()) {
      if (OB_FAIL(parse_minimum_should_match_with_integer(msm_str, false, key_count, min_should_match_value))) {
        LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(msm_str));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("minimum_should_match is not numeric or percentage", K(ret), K(msm_str));
    }
  }

  if (OB_SUCC(ret)) {
    if (min_should_match_value == 0) {
      qs_min_should_match_info.msm_count_ = 1;
      if (qs_min_should_match_info.minimum_should_match_ != NULL) {
        qs_min_should_match_info.minimum_should_match_->expr_name.assign_ptr("1", 1);
      } else if (OB_ISNULL(qs_min_should_match_info.minimum_should_match_ = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for minimum should match expr", K(ret));
      } else {
        qs_min_should_match_info.minimum_should_match_->expr_name.assign_ptr("1", 1);
      }
    } else if (min_should_match_value > 0) {
      qs_min_should_match_info.msm_count_ = min_should_match_value;
      char buf[32];
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos, "%ld", min_should_match_value))) {
        LOG_WARN("fail to format minimum_should_match value", K(ret), K(min_should_match_value));
      } else {
        char *numstr = static_cast<char *>(alloc_.alloc(pos));
        if (OB_ISNULL(numstr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for msm string", K(ret), K(pos));
        } else {
          MEMCPY(numstr, buf, pos);
          qs_min_should_match_info.minimum_should_match_->expr_name.assign_ptr(numstr, static_cast<int32_t>(pos));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(qs_min_should_match_info.minimum_should_match_)) {
    qs_min_should_match_info.minimum_should_match_->var_type_ = ObIntType;
  }
  return ret;
}

int ObESQueryParser::parse_minimum_should_match_with_bool_query(ObIJsonBase &req_node, const int64_t should_count, BoolQueryMinShouldMatchInfo &bq_min_should_match_info)
{
  int ret = OB_SUCCESS;
  uint64_t min_should_match_value = 0;
  ObString minimum_should_match_str("minimum_should_match");
  ObIJsonBase *minimum_should_match_node = NULL;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("minimum_should_match should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(req_node.get_object_value(minimum_should_match_str, minimum_should_match_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      bq_min_should_match_info.minimum_should_match_ = NULL;
      bq_min_should_match_info.has_minimum_should_match_ = false;
      ret = OB_SUCCESS;
      LOG_DEBUG("minimum_should_match is not found", K(ret));
    } else {
      LOG_WARN("fail to get minimum should match node", K(ret));
    }
  } else if (OB_FAIL(parse_const(*minimum_should_match_node, bq_min_should_match_info.minimum_should_match_))) {
    LOG_WARN("fail to parse minimum should match expr", K(ret));
  } else if (OB_NOT_NULL(bq_min_should_match_info.minimum_should_match_)) {
    const ObString &msm_str = bq_min_should_match_info.minimum_should_match_->expr_name.trim();
    if (msm_str.length() > 0 && msm_str.ptr()[msm_str.length() - 1] == '%') {
      if (OB_FAIL(parse_minimum_should_match_with_percentage(msm_str, should_count, min_should_match_value))) {
        LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(msm_str));
      }
    } else if (msm_str.is_numeric()) {
      if (OB_FAIL(parse_minimum_should_match_with_integer(msm_str, false, should_count, min_should_match_value))) {
        LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(msm_str));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("minimum_should_match is not numeric or percentage", K(ret), K(msm_str));
    }
  }

  if (OB_SUCC(ret)) {
    if (min_should_match_value == 0) {
      bq_min_should_match_info.msm_count_ = 0;
      if (bq_min_should_match_info.minimum_should_match_ != NULL) {
        bq_min_should_match_info.minimum_should_match_->expr_name.assign_ptr("0", 1);
      } else if (OB_ISNULL(bq_min_should_match_info.minimum_should_match_ = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for minimum should match expr", K(ret));
      } else {
        bq_min_should_match_info.minimum_should_match_->expr_name.assign_ptr("0", 1);
      }
    } else if (min_should_match_value > 0) {
      bq_min_should_match_info.msm_count_ = min_should_match_value;
      char buf[32];
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(buf, sizeof(buf), pos, "%ld", min_should_match_value))) {
        LOG_WARN("fail to format minimum_should_match value", K(ret), K(min_should_match_value));
      } else {
        char *numstr = static_cast<char *>(alloc_.alloc(pos));
        if (OB_ISNULL(numstr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory for msm string", K(ret), K(pos));
        } else {
          MEMCPY(numstr, buf, pos);
          bq_min_should_match_info.minimum_should_match_->expr_name.assign_ptr(numstr, static_cast<int32_t>(pos));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(bq_min_should_match_info.minimum_should_match_)) {
    bq_min_should_match_info.minimum_should_match_->var_type_ = ObIntType;
  }
  return ret;
}

int ObESQueryParser::construct_query_with_minnum_should_match(ObReqExpr *score, ObReqConstExpr *minnum_should_match, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *ge_expr = NULL;
  char* buf = NULL;
  ObString sub_query_name;
  ObReqColumnExpr *score_col = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(alloc_.alloc(OB_MAX_INDEX_PER_TABLE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    snprintf(buf, OB_MAX_INDEX_PER_TABLE, "_fts%d", sub_query_count_);
    sub_query_name.assign_ptr(buf, static_cast<int32_t>(strlen(buf)));
    sub_query_count_++;
  if (OB_ISNULL(ge_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_GE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(wrap_sub_query(sub_query_name, query_req))) {
    LOG_WARN("fail to wrap sub query", K(ret));
  } else if (OB_FAIL(query_req->condition_items_.push_back(ge_expr))) {
    LOG_WARN("fail to add const expr to query request", K(ret));
  } else if (OB_FAIL(ge_expr->init(score, minnum_should_match, T_OP_GE))) {
    LOG_WARN("fail to init op expr", K(ret));
  } else if (OB_ISNULL(score_col = OB_NEWx(ObReqColumnExpr, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else {
    score_col->expr_name = "_score";
    score_col->table_name = sub_query_name;
    query_req->add_score_item(alloc_, score_col);
  }
  }
  return ret;
}

int ObESQueryParser::construct_bool_score_expr(ObReqExpr *&bool_score_item, common::ObIArray<ObReqExpr *> &score_array)
{
  int ret = OB_SUCCESS;
  int count = score_array.count();
  ObReqExpr *expr = NULL;

  if (count == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("score array is empty", K(ret));
  } else if (count == 1) {
    expr = score_array.at(0);
  } else {
    if (OB_ISNULL(expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create expr", K(ret));
    } else {
      for (uint32_t i = 0; OB_SUCC(ret) && i < count; i++) {
        if (OB_FAIL(expr->params.push_back(score_array.at(i)))) {
          LOG_WARN("fail to append param", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        score_array.reset();
      }
    }
  }
  if (OB_SUCC(ret)) {
    bool_score_item = expr;
  }
  return ret;
}

int ObESQueryParser::construct_condition_best_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                     const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                     common::ObIArray<ObReqExpr *> &conditions,
                                                     QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                                     ObReqScoreType score_type,
                                                     const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (opr == T_OP_OR && qs_min_should_match_info.msm_count_ > 1 &&
            (SCORE_TYPE_CROSS_FIELDS == score_type || keyword_exprs.count() > 1)) {
    if (OB_FAIL(construct_should_group_expr_for_query_string(field_exprs, keyword_exprs, score_type, qs_min_should_match_info))) {
      LOG_WARN("fail to construct should group expr for query string", K(ret));
    }
  } else {
    bool need_parentheses = (opr == T_OP_AND && field_exprs.count() > 1) ? true : false;
    for (int64_t i = 0; OB_SUCC(ret) && i < field_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_conditions;
      ObReqColumnExpr *field_expr = field_exprs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < keyword_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqConstExpr *keyword_expr = keyword_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          match_expr->score_type_ = score_type;
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          } else {
            keyword_conditions.push_back(match_expr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (keyword_conditions.count() == 1) {
          conditions.push_back(keyword_conditions.at(0));
        } else {
          ObReqOpExpr *keyword_expr = nullptr;
          if (OB_ISNULL(keyword_expr = OB_NEWx(ObReqOpExpr, &alloc_, opr, need_parentheses))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create conditions expr", K(ret), K(opr));
          } else {
            keyword_expr->expr_name = (opr == T_OP_AND) ? "AND" : "OR";
            for (int64_t k = 0; OB_SUCC(ret) && k < keyword_conditions.count(); k++) {
              if (OB_FAIL(keyword_expr->params.push_back(keyword_conditions.at(k)))) {
                LOG_WARN("fail to add condition to expr", K(ret), K(k), K(opr));
              }
            }
            if (OB_SUCC(ret)) {
              conditions.push_back(keyword_expr);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_condition_cross_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                      const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                      common::ObIArray<ObReqExpr *> &conditions,
                                                      QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                                      ObReqScoreType score_type,
                                                      const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (opr == T_OP_OR && OB_FAIL(construct_condition_best_fields(field_exprs, keyword_exprs, conditions, qs_min_should_match_info, score_type, opr))) {
      LOG_WARN("fail to construct condition for cross_fields + OR", K(ret));
  } else if (opr == T_OP_AND) {
    bool need_parentheses = keyword_exprs.count() > 1 ? true : false;
    for (int64_t i = 0; OB_SUCC(ret) && i < keyword_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_conditions;
      ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < field_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = field_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          match_expr->score_type_ = score_type;
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          } else {
            field_conditions.push_back(match_expr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (field_conditions.count() == 1) {
          conditions.push_back(field_conditions.at(0));
        } else {
          ObReqOpExpr *field_expr = nullptr;
          if (OB_ISNULL(field_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_OR, need_parentheses))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create field condition expr", K(ret));
          } else {
            field_expr->expr_name = "OR";
            for (int64_t k = 0; OB_SUCC(ret) && k < field_conditions.count(); k++) {
              if (OB_FAIL(field_expr->params.push_back(field_conditions.at(k)))) {
                LOG_WARN("fail to add field condition to expr", K(ret), K(k));
              }
            }
            if (OB_SUCC(ret)) {
              conditions.push_back(field_expr);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_condition_most_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                     const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                     common::ObIArray<ObReqExpr *> &conditions,
                                                     QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                                     ObReqScoreType score_type,
                                                     const ObItemType opr /*= T_OP_OR*/)
{
  return construct_condition_best_fields(field_exprs, keyword_exprs, conditions, qs_min_should_match_info, score_type, opr);
}

int ObESQueryParser::construct_condition_phrase(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                common::ObIArray<ObReqExpr *> &conditions,
                                                QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                                ObReqScoreType score_type,
                                                const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (opr == T_OP_OR && qs_min_should_match_info.msm_count_ > 1 && keyword_exprs.count() > 1) {
    if (OB_FAIL(construct_should_group_expr_for_query_string(field_exprs, keyword_exprs, score_type, qs_min_should_match_info))) {
      LOG_WARN("fail to construct should group expr for query string", K(ret));
    }
  } else {
    bool need_parentheses = (opr == T_OP_AND && field_exprs.count() > 1) ? true : false;
    for (int64_t i = 0; OB_SUCC(ret) && i < field_exprs.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_conditions;
      ObReqColumnExpr *field_expr = field_exprs.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < keyword_exprs.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqConstExpr *keyword_expr = keyword_exprs.at(j);
        if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          match_expr->expr_name = "match";
          if (keyword_expr->weight_ == -1.0) {
            match_expr->score_type_ = SCORE_TYPE_PHRASE;
          }
          if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          } else {
            keyword_conditions.push_back(match_expr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (keyword_conditions.count() == 1) {
          conditions.push_back(keyword_conditions.at(0));
        } else {
          ObReqOpExpr *condition_expr = nullptr;
          if (OB_ISNULL(condition_expr = OB_NEWx(ObReqOpExpr, &alloc_, opr, need_parentheses))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create conditions expr", K(ret));
          } else {
            condition_expr->expr_name = (opr == T_OP_AND) ? "AND" : "OR";
            for (int64_t k = 0; OB_SUCC(ret) && k < keyword_conditions.count(); k++) {
              if (OB_FAIL(condition_expr->params.push_back(keyword_conditions.at(k)))) {
                LOG_WARN("fail to add condition to expr", K(ret), K(k));
              }
            }
            if (OB_SUCC(ret)) {
              conditions.push_back(condition_expr);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_query_string_condition(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                      const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                      ObReqScoreType score_type,
                                                      ObReqExpr *&where_condition,
                                                      QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                                      const ObItemType opr /*= T_OP_OR*/)
{
  int ret = OB_SUCCESS;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (opr != T_OP_AND && opr != T_OP_OR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operator between conditions must be AND or OR", K(ret), K(opr));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> conditions;
    switch (score_type) {
      case SCORE_TYPE_BEST_FIELDS: {
        if (OB_FAIL(construct_condition_best_fields(field_exprs, keyword_exprs, conditions, qs_min_should_match_info, score_type, opr))) {
          LOG_WARN("fail to construct condition for best_fields", K(ret));
        }
        break;
      }
      case SCORE_TYPE_MOST_FIELDS: {
        if (OB_FAIL(construct_condition_most_fields(field_exprs, keyword_exprs, conditions, qs_min_should_match_info, score_type, opr))) {
          LOG_WARN("fail to construct condition for most_fields", K(ret));
        }
        break;
      }
      case SCORE_TYPE_CROSS_FIELDS: {
        if (OB_FAIL(construct_condition_cross_fields(field_exprs, keyword_exprs, conditions, qs_min_should_match_info, score_type, opr))) {
          LOG_WARN("fail to construct condition for cross_fields", K(ret));
        }
        break;
      }
      case SCORE_TYPE_PHRASE: {
        if (OB_FAIL(construct_condition_phrase(field_exprs, keyword_exprs, conditions, qs_min_should_match_info, score_type, opr))) {
          LOG_WARN("fail to construct condition for phrase", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported score type", K(ret), K(score_type));
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(qs_min_should_match_info.ge_expr_)) {
      where_condition = qs_min_should_match_info.ge_expr_;
    } else {
      if (conditions.count() == 1) {
        where_condition = conditions.at(0);
      } else {
        ObReqOpExpr *final_condition = nullptr;
        ObItemType condition_operator = (score_type == SCORE_TYPE_CROSS_FIELDS && opr == T_OP_AND) ? T_OP_AND : T_OP_OR;
        if (OB_ISNULL(final_condition = OB_NEWx(ObReqOpExpr, &alloc_, condition_operator, false))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to create where condition expr", K(ret), K(condition_operator));
        } else {
          final_condition->expr_name = (condition_operator == T_OP_AND) ? "AND" : "OR";
          for (int64_t i = 0; OB_SUCC(ret) && i < conditions.count(); i++) {
            if (OB_FAIL(final_condition->params.push_back(conditions.at(i)))) {
              LOG_WARN("fail to add condition to expr", K(ret), K(i), K(condition_operator));
            }
          }
          if (OB_SUCC(ret)) {
            where_condition = final_condition;
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_should_group_expr_for_query_string(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                                  const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                                  const ObReqScoreType score_type,
                                                                  QueryStringMinShouldMatchInfo &qs_min_should_match_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> or_group_exprs;
  if (field_exprs.count() == 0 || keyword_exprs.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (qs_min_should_match_info.msm_count_ > keyword_exprs.count()) {
    // to improve performance, avoid creating unnecessary conditions in the WHERE clause.
    if (OB_ISNULL(qs_min_should_match_info.ge_expr_ = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create expr", K(ret));
    } else {
      qs_min_should_match_info.ge_expr_->expr_name = "0";
    }
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < keyword_exprs.count(); i++) {
      ObReqExpr *expr = nullptr;
      if (OB_ISNULL(expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_OR, true))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create expr", K(ret));
      } else {
        for (uint64_t j = 0; OB_SUCC(ret) && j < field_exprs.count(); j++) {
          ObReqMatchExpr *match_expr = nullptr;
          if (OB_ISNULL(match_expr = OB_NEWx(ObReqMatchExpr, &alloc_))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to create match expr", K(ret));
          }
          ObReqColumnExpr *field_expr = field_exprs.at(j);
          ObReqConstExpr *keyword_expr = keyword_exprs.at(i);
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(match_expr->params.push_back(field_expr))) {
            LOG_WARN("fail to add field to match expr", K(ret));
          } else if (OB_FAIL(match_expr->params.push_back(keyword_expr))) {
            LOG_WARN("fail to add keyword to match expr", K(ret));
          } else if (OB_FAIL(expr->params.push_back(match_expr))) {
            LOG_WARN("fail to add match expr to expr", K(ret));
          } else if (score_type == SCORE_TYPE_PHRASE) {
            match_expr->score_type_ = SCORE_TYPE_PHRASE;
          } else {
            match_expr->score_type_ = SCORE_TYPE_BEST_FIELDS;
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(or_group_exprs.push_back(expr))) {
          LOG_WARN("fail to push or group expr", K(ret));
        }
      }
    }
    ObReqExpr *ge_expr = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_should_groups(qs_min_should_match_info.msm_count_, qs_min_should_match_info.minimum_should_match_, or_group_exprs, ge_expr))) {
      LOG_WARN("fail to build should groups", K(ret));
    } else {
      qs_min_should_match_info.ge_expr_ = ge_expr;
    }
  }
  return ret;
}

int ObESQueryParser::get_base_table_query(ObQueryReqFromJson *query_req, ObQueryReqFromJson *&base_table_req)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(query_req));
  } else if (OB_NOT_NULL(base_table_req)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_req->from_items_.count(); i++) {
      ObReqTable *table = query_req->from_items_.at(i);
      if (table->table_type_ == ReqTableType::BASE_TABLE) {
        base_table_req = query_req;
        break;
      } else if (table->table_type_ == ReqTableType::SUB_QUERY) {
        ObQueryReqFromJson *query = NULL;
        if (OB_ISNULL(query = dynamic_cast<ObQueryReqFromJson *>(table->ref_query_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(table->ref_query_));
        } else if (OB_FAIL(get_base_table_query(query, base_table_req))) {
          LOG_WARN("fail to get base table query", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_query_string_by_type(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                const ObReqScoreType score_type,
                                                const ObItemType opr,
                                                ObReqExpr *&result_expr,
                                                ObReqExpr *&where_condition,
                                                QueryStringMinShouldMatchInfo &qs_min_should_match_info)
{
  int ret = OB_SUCCESS;
  switch (score_type) {
    case SCORE_TYPE_BEST_FIELDS: {
      if (OB_FAIL(parse_best_fields(field_exprs, keyword_exprs, result_expr, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to parse best_fields", K(ret));
      }
      break;
    }
    case SCORE_TYPE_CROSS_FIELDS: {
      if (OB_FAIL(parse_cross_fields(field_exprs, keyword_exprs, result_expr, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to parse cross_fields", K(ret));
      }
      break;
    }
    case SCORE_TYPE_MOST_FIELDS: {
      if (OB_FAIL(parse_most_fields(field_exprs, keyword_exprs, result_expr, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to parse most_fields", K(ret));
      }
      break;
    }
    case SCORE_TYPE_PHRASE: {
      if (OB_FAIL(parse_phrase(field_exprs, keyword_exprs, result_expr, where_condition, qs_min_should_match_info, opr))) {
        LOG_WARN("fail to parse phrase", K(ret));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported score type", K(ret), K(score_type));
    }
  }
  return ret;
}

int ObESQueryParser::build_should_groups(uint64_t start, uint64_t k, const common::ObIArray<ObReqExpr *> &items, common::ObIArray<ObReqExpr *> &expr_array, ObReqExpr *&or_expr)
{
  int ret = OB_SUCCESS;
  if (k == 0 || items.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (k > items.count()) {
    if (OB_ISNULL(or_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      // 0 means is false condition
      or_expr->expr_name = "0";
    }
  } else if (k == items.count()) {
    if (OB_ISNULL(or_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
        if (OB_FAIL(or_expr->params.push_back(items.at(i)))) {
          LOG_WARN("fail to push or param", K(ret), K(i));
        }
      }
    }
  } else {
    if (expr_array.count() == k) {
      ObReqOpExpr *and_expr = NULL;
      if (OB_ISNULL(and_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create and expr", K(ret));
      } else if (OB_ISNULL(or_expr) && OB_ISNULL(or_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_OR))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create or expr", K(ret));
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < expr_array.count(); i++) {
          if (OB_FAIL(and_expr->params.push_back(expr_array.at(i)))) {
            LOG_WARN("fail to push and param", K(ret), K(i));
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(or_expr->params.push_back(and_expr))) {
          LOG_WARN("fail to push or param", K(ret));
        }
      }
    } else {
      for (uint64_t i = start; OB_SUCC(ret) && i < items.count(); i++) {
        if (OB_FAIL(expr_array.push_back(items.at(i)))) {
          LOG_WARN("fail to push and param", K(ret), K(i));
        } else if (OB_FAIL(build_should_groups(i + 1, k, items, expr_array, or_expr))) {
          LOG_WARN("fail to build deeper groups", K(ret));
        } else {
          expr_array.pop_back();
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::build_should_groups(uint64_t msm_count, ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&ge_expr)
{
  int ret = OB_SUCCESS;
  ge_expr = NULL;
  if (msm_count == 0 || items.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(msm_count), K(items.count()));
  } else if (msm_count > items.count()) {
    if (OB_ISNULL(ge_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      // 0 means is false condition
      ge_expr->expr_name = "0";
    }
  } else if (msm_count == items.count()) {
    if (msm_count == 1) {
      ge_expr = items.at(0);
    } else if (OB_ISNULL(ge_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_AND))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      for (uint64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
        if (OB_FAIL(ge_expr->params.push_back(items.at(i)))) {
          LOG_WARN("fail to push ge param", K(ret), K(i));
        }
      }
    }
  } else {
    ObReqOpExpr *sum_expr = NULL;
    ObReqConstExpr *zero_const = NULL;
    if (OB_ISNULL(sum_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_ADD))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create sum expr", K(ret));
    } else if (OB_ISNULL(zero_const = OB_NEWx(ObReqConstExpr, &alloc_, ObIntType))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create zero const", K(ret));
    } else if (FALSE_IT(zero_const->expr_name = "0")) {
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
      ObReqExpr *group = items.at(i);
      ObReqOpExpr *gt_zero = NULL;
      if (OB_ISNULL(gt_zero = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_GT))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create gt expr", K(ret));
      } else if (OB_FAIL(gt_zero->init(group, zero_const, T_OP_GT))) {
        LOG_WARN("fail to init gt expr", K(ret));
      } else if (OB_FAIL(sum_expr->params.push_back(gt_zero))) {
        LOG_WARN("fail to push gt expr into sum", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObReqOpExpr *cmp_expr = NULL;
      if (OB_ISNULL(cmp_expr = OB_NEWx(ObReqOpExpr, &alloc_, T_OP_GE))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create ge expr", K(ret));
      } else if (OB_FAIL(cmp_expr->init(sum_expr, msm_expr, T_OP_GE))) {
        LOG_WARN("fail to init ge expr", K(ret));
      } else {
        ge_expr = cmp_expr;
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_all_query(ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObReqExpr *score_expr = NULL;
  if (OB_ISNULL(query_req = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(parse_basic_table(table_name_, query_req))) {
    LOG_WARN("fail to parse basic table", K(ret));
  } else if (OB_ISNULL(score_expr = OB_NEWx(ObReqConstExpr, &alloc_, ObDoubleType))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create score expr", K(ret));
  } else {
    score_expr->expr_name = "1";
    query_req->add_score_item(alloc_, score_expr);
  }
  return ret;
}
}  // namespace share
}  // namespace oceanbase