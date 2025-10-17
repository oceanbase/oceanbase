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

#define USING_LOG_PREFIX SERVER
#include "ob_query_parse.h"

namespace oceanbase
{
namespace share
{


const ObString ObESQueryParser::SCORE_NAME("_score");
const ObString ObESQueryParser::FTS_SCORE_NAME("_keyword_score");
const ObString ObESQueryParser::VS_SCORE_NAME("_semantic_score");
const ObString ObESQueryParser::SIMILARITY_SCORE_NAME("_similarity_score");
const ObString ObESQueryParser::FTS_RANK_NAME("_keyword_rank");
const ObString ObESQueryParser::VS_RANK_NAME("_semantic_rank");
const ObString ObESQueryParser::ROWKEY_NAME("__pk_increment");
const ObString ObESQueryParser::RANK_CONST_DEFAULT("60");
const ObString ObESQueryParser::SIZE_DEFAULT("10");
const ObString ObESQueryParser::FTS_ALIAS("_fts");
const ObString ObESQueryParser::VS_ALIAS("_vs");

int ObESQueryParser::parse(const common::ObString &req_str, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObJsonNode *j_node = NULL;
  const char *syntaxerr = NULL;
  ObString fusion_key = "rank";
  ObIJsonBase *fusion_node = NULL;
  uint64_t err_offset = 0;
  uint32_t parse_flag = ObJsonParser::JSN_RELAXED_FLAG | ObJsonParser::JSN_UNIQUE_FLAG;
  if (OB_FAIL(ObJsonParser::parse_json_text(&alloc_, req_str.ptr(), req_str.length(), syntaxerr, &err_offset, j_node, parse_flag))) {
    LOG_WARN("failed to parse array text", K(ret), K(req_str), KCSTRING(syntaxerr), K(err_offset));
  } else if (OB_FAIL(init_default_params(*j_node))) {
    LOG_WARN("fail to init default params", K(ret));
  }
  if (OB_SUCC(ret)) {
    uint64_t count = j_node->element_count();
    ObQueryReqFromJson *query = NULL;
    ObQueryReqFromJson *knn = NULL;
    ObReqConstExpr *from_expr = NULL;
    ObIJsonBase *es_mode_node = NULL;
    ObString es_mode_key("es_mode");
    if (OB_FAIL(j_node->get_object_value(es_mode_key, es_mode_node))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
        LOG_DEBUG("es_mode field not found, use default value", K(enable_es_mode_));
      } else {
        LOG_WARN("fail to get type field", K(ret));
      }
    } else if (es_mode_node->json_type() == ObJsonNodeType::J_BOOLEAN) {
      enable_es_mode_ = es_mode_node->get_boolean();
    } else if (es_mode_node->json_type() == ObJsonNodeType::J_STRING) {
      ObString es_mode_str = ObString(es_mode_node->get_data_length(), es_mode_node->get_data()).trim();
      if (es_mode_str.case_compare("true") == 0) {
        enable_es_mode_ = true;
      } else if (es_mode_str.case_compare("false") == 0) {
        enable_es_mode_ = false;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("es_mode field must be boolean type or string 'true' or 'false'", K(ret));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
      LOG_WARN("es_mode field must be boolean type or string 'true' or 'false'", K(ret), K(es_mode_node->json_type()));
    }
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
        if (OB_FAIL(parse_multi_knn(*req_node, knn))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("_source") == 0) {
        if (OB_FAIL(parse_source(*req_node))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("from") == 0) {
        if (OB_FAIL(parse_const(*req_node, from_expr, true))) {
          LOG_WARN("fail to get value", K(ret), K(i));
        }
      } else if (key.case_compare("size") == 0) {
        // do nothing, parsed in init_default_params
      } else if (key.case_compare(es_mode_key) == 0) {
        // do nothing and continue
      } else if (key.case_compare(fusion_key) == 0) {
        // do nothing
      } else {
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("invalid query param", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret)) {
      bool is_hybrid = query != NULL && knn != NULL;
      out_cols_ = source_cols_.empty() ? &user_cols_ : &source_cols_;
      if (is_hybrid) {
        if (OB_FAIL(set_fts_limit_expr(query, default_size_, from_expr))) {
          LOG_WARN("fail to set limit expr to fts query", K(ret));
        } else if (OB_FAIL(construct_hybrid_query(query, knn, query_req))) {
          LOG_WARN("fail to construct hybrid query", K(ret));
        }
      } else {
        query_req = (query == NULL ? knn : query);
        if (OB_ISNULL(query_req) && OB_FAIL(construct_all_query(query_req))) {
          LOG_WARN("fail to construct all query", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (default_size_ == NULL && from_expr != NULL) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported sytnax in query, 'size' must be set when 'from' is specified", K(ret));
      } else {
        query_req->set_offset(from_expr);
        if (query_req->get_limit() == NULL) {
          query_req->set_limit(default_size_);
        } else if (default_size_ != NULL && OB_FAIL(choose_limit(query_req, default_size_))) {
          LOG_WARN("fail to choose limit expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && !out_cols_->empty()) {
        if (OB_FAIL(set_output_columns(*query_req, is_hybrid))) {
          LOG_WARN("fail to set output columns", K(ret));
        } else if (need_json_wrap_ && OB_FAIL(wrap_json_result(query_req))) {
          LOG_WARN("fail to wrap json result", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::choose_limit(ObQueryReqFromJson *query_req, ObReqConstExpr *size_expr)
{
  int ret = OB_SUCCESS;
  int64_t limit_val = 0;
  int64_t size_val = 0;
  if (!query_req->get_limit()->expr_name.is_numeric() ||
      !size_expr->expr_name.is_numeric()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd value type", K(ret), K(query_req->get_limit()->expr_name), K(size_expr->expr_name));
  } else if (OB_FAIL(convert_const_numeric(query_req->get_limit()->expr_name, limit_val))) {
    LOG_WARN("failed to get const value", K(ret), K(query_req->get_limit()->expr_name), K(size_expr->expr_name));
  } else if (OB_FAIL(convert_const_numeric(size_expr->expr_name, size_val))) {
    LOG_WARN("failed to get const value", K(ret), K(query_req->get_limit()->expr_name), K(size_expr->expr_name));
  } else if (size_val < limit_val) {
    query_req->set_limit(size_expr);
  }
  return ret;
}

int ObESQueryParser::parse_multi_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  uint64_t knn_count = req_node.json_type() == ObJsonNodeType::J_OBJECT ? 1 : req_node.element_count();
  ObQueryReqFromJson *knn_req = NULL;
  common::ObSEArray<ObQueryReqFromJson*, 4, common::ModulePageAllocator, true> knn_queries;
  for (uint64_t i = 0; OB_SUCC(ret) && i < knn_count; i++) {
    ObIJsonBase *val_node = NULL;
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      val_node = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, val_node))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_knn(*val_node, knn_req))) {
      LOG_WARN("fail to parse knn", K(ret), K(i));
    } else if (OB_FAIL(knn_queries.push_back(knn_req))) {
      LOG_WARN("fail to append query", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (knn_queries.count() > 1) {
      if (OB_FAIL(knn_fusion(knn_queries, query_req))) {
        LOG_WARN("fail to do knn fusion", K(ret));
      }
    } else {
      query_req = knn_req;
    }
  }
  return ret;
}

int ObESQueryParser::init_default_params(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObString fusion_key = "rank";
  ObString size_key = "size";
  ObIJsonBase *fusion_node = NULL;
  ObIJsonBase *size_node = NULL;
  if (OB_FAIL(req_node.get_object_value(fusion_key, fusion_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get rank node", K(ret));
    }
  } else if (fusion_node != NULL && OB_FAIL(parse_rank(*fusion_node))) {
    LOG_WARN("fail to parse rank node", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_object_value(size_key, size_node))) {
    if (OB_SEARCH_NOT_FOUND == ret) {
      // set default size
      ret = OB_SUCCESS;
      if (OB_FAIL(ObReqConstExpr::construct_const_expr(default_size_, alloc_, SIZE_DEFAULT, ObIntType))) {
        LOG_WARN("fail to create const expr", K(ret));
      }
    } else {
      LOG_WARN("fail to get rank node", K(ret));
    }
  } else if (size_node != NULL && OB_FAIL(parse_const(*size_node, default_size_, true))) {
    LOG_WARN("fail to parse rank node", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (fusion_config_.method == ObFusionMethod::RRF) {
      int64_t window_size = 0;
      if (fusion_config_.size == NULL) {
        // use size as default value
        fusion_config_.size = default_size_;
      }
      if (OB_FAIL(convert_const_numeric(fusion_config_.size->expr_name, window_size))) {
        LOG_WARN("fail to convert size expr", K(ret));
      } else if (size_node != NULL) {
        // size isn't default value
        int64_t size_val = 0;
        if (OB_FAIL(convert_const_numeric(default_size_->expr_name, size_val))) {
          LOG_WARN("fail to convert size expr", K(ret));
        } else if (OB_FAIL(convert_const_numeric(fusion_config_.size->expr_name, window_size))) {
          LOG_WARN("fail to convert size expr", K(ret));
        } else if (size_val > window_size) {
          ret = OB_WARN_OPTION_BELOW_LIMIT;
          LOG_USER_WARN(OB_WARN_OPTION_BELOW_LIMIT, "rank_window_size", "size");
        } else if (window_size < 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid window size value", K(ret), K(window_size));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (window_size < 1) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid window size value", K(ret), K(window_size));
      } else if (fusion_config_.rank_const == NULL && // use default rank const
                 OB_FAIL(ObReqConstExpr::construct_const_expr(fusion_config_.rank_const, alloc_, RANK_CONST_DEFAULT, ObIntType))) {
        LOG_WARN("fail to create const expr", K(ret));
      } else {
        // verify validilty
        int64_t rank_const = 0;
        if (OB_FAIL(convert_const_numeric(fusion_config_.rank_const->expr_name, rank_const))) {
          LOG_WARN("fail to convert size expr", K(ret));
        } else if (rank_const < 1) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid rank const value", K(ret), K(rank_const));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_rank(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObString key;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (req_node.element_count() != 1) {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("unexpected param count", K(ret));
  } else if (OB_FAIL(req_node.get_key(0, key))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (key.case_compare("rrf") == 0) {
    ObIJsonBase *sub_node = NULL;
    if (OB_FAIL(req_node.get_object_value(0, sub_node))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (OB_FAIL(parse_rrf(*sub_node))) {
      LOG_WARN("fail to parse rrf", K(ret));
    } else {
      fusion_config_.method = ObFusionMethod::RRF;
    }
  } else {
    ret = OB_ERR_PARSER_SYNTAX;
    LOG_WARN("invalid query param", K(ret), K(key));
  }

  return ret;
}

int ObESQueryParser::parse_rrf(ObIJsonBase &req_node)
{
  int ret = OB_SUCCESS;
  ObString key;
  uint64_t count = req_node.element_count();
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = NULL;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("rank_constant") == 0) {
      if (OB_FAIL(parse_const(*sub_node, fusion_config_.rank_const, true))) {
        LOG_WARN("fail to parse rank constant value", K(ret), K(i));
      }
    } else if (key.case_compare("rank_window_size") == 0) {
      if (OB_FAIL(parse_const(*sub_node, fusion_config_.size, true))) {
        LOG_WARN("fail to parse rank constant value", K(ret), K(i));
      }
    } else {
      ret = OB_ERR_PARSER_SYNTAX;
      LOG_WARN("not supported sytnax in query", K(ret), K(key));
    }
  }
  return ret;
}

int ObESQueryParser::knn_fusion(const ObIArray<ObQueryReqFromJson*> &knn_queries, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObMultiSetTable *multi_set_table = NULL;
  ObQueryReqFromJson *res = NULL;
  ObReqColumnExpr *rowkey_expr = NULL;
  ObString rowkey = ROWKEY_NAME;
  ObString rowkey_hint("opt_param('hidden_column_visible', 'true')");
  if (OB_ISNULL(multi_set_table = OB_NEWx(ObMultiSetTable, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_ISNULL(res = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(rowkey_expr, alloc_, rowkey))) {
    LOG_WARN("fail to create json obj expr", K(ret));
  }
  for (uint64_t i = 0; i < knn_queries.count() && OB_SUCC(ret); i++) {
    ObString empty_str;
    ObReqTable *sub_query = NULL;
    ObQueryReqFromJson *base_table_req = NULL;
    if (OB_FAIL(get_base_table_query(knn_queries.at(i), base_table_req))) {
      LOG_WARN("fail to get base table query", K(ret));
    } else if (OB_FAIL(base_table_req->add_req_hint(rowkey_hint))) {
      LOG_WARN("fail to create query request", K(ret));
    } else if (OB_FAIL(base_table_req->select_items_.push_back(rowkey_expr))) {
      LOG_WARN("fail to create query request", K(ret));
    } else if (OB_FAIL(construct_sub_query_table(empty_str, knn_queries.at(i), sub_query))) {
      LOG_WARN("fail to create sub query table", K(ret));
    } else if (OB_FAIL(multi_set_table->sub_queries_.push_back(sub_query))) {
      LOG_WARN("fail to append sub query", K(ret));
    }
  }

  ObReqExpr *sum_expr = NULL;
  ObReqColumnExpr *score_col = NULL;
  OrderInfo *order_info = NULL;
  ObString sum_name = "sum";
  ObString score_name = SCORE_NAME;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(score_col, alloc_, score_name))) {
    LOG_WARN("fail to construnct column expr", K(ret));
  } else if (OB_FAIL(ObReqExpr::construct_expr(sum_expr, alloc_, sum_name, score_col, score_name))) {
    LOG_WARN("fail to construnct sum expr", K(ret));
  } else if (OB_FAIL(res->from_items_.push_back(multi_set_table))) {
    LOG_WARN("fail to append from item", K(ret));
  } else if (OB_FAIL(res->score_items_.push_back(sum_expr))) {
    LOG_WARN("fail to append select item", K(ret));
  } else if (OB_FAIL(res->group_items_.push_back(rowkey_expr))) {
    LOG_WARN("fail to push query order item", K(ret));
  } else if (OB_FAIL(construct_order_by_item(sum_expr, false, order_info))) {
    LOG_WARN("fail to construct order by item", K(ret));
  } else if (OB_FAIL(res->order_items_.push_back(order_info))) {
    LOG_WARN("fail to push query order item", K(ret));
  } else {
    multi_set_table->joined_type_ = ObReqJoinType::UNION_ALL;
    multi_set_table->table_type_ = MULTI_SET;
    query_req = res;
  }

  return ret;
}

int ObESQueryParser::convert_const_numeric(const ObString &cont_val, int64_t &val)
{
  int ret = OB_SUCCESS;
  int err = 0;
  val = ObCharset::strntoll(cont_val.ptr(), cont_val.length(), 10, &err);
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
  if (err == 0) {
    if (val > INT_MAX32 || val < INT_MIN32) {
      ret = OB_ERR_INVALID_PARAM_ENCOUNTERED;
      LOG_WARN("input value out of 32-bit range", K(ret), K(val));
    }
  } else {
    ret = OB_ERR_INVALID_PARAM_ENCOUNTERED;
    LOG_WARN("input value must be a integer", K(ret));
  }
  return ret;
}

int ObESQueryParser::wrap_json_result(ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;

  ObReqExpr *j_obj_expr = NULL;
  ObReqExpr *j_arrayagg_expr = NULL;
  if (OB_FAIL(ObReqExpr::construct_expr(j_obj_expr, alloc_, "json_object"))) {
    LOG_WARN("fail to create json obj expr", K(ret));
  } else if (OB_FAIL(ObReqExpr::construct_expr(j_arrayagg_expr, alloc_, "json_arrayagg", "hits"))) {
    LOG_WARN("fail to create json array agg expr", K(ret));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < out_cols_->count(); i++) {
      ObReqConstExpr *col_name = NULL;
      ObReqColumnExpr *col_expr = NULL;
      if (OB_FAIL(ObReqConstExpr::construct_const_expr(col_name, alloc_, out_cols_->at(i), ObVarcharType))) {
        LOG_WARN("fail to create const expr", K(ret));
      } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, out_cols_->at(i)))) {
        LOG_WARN("fail to create column expr", K(ret));
      } else if (OB_FAIL(j_obj_expr->params.push_back(col_name))) {
        LOG_WARN("fail to append name", K(ret));
      } else {
        bool found = false;
        for (uint64_t j = 0; OB_SUCC(ret) && !found && j < query_req->select_items_.count(); j++) {
          ObReqExpr *sel_expr = query_req->select_items_.at(j);
          if ((!sel_expr->alias_name.empty() && out_cols_->at(i).case_compare(sel_expr->alias_name) == 0) ||
              (!sel_expr->expr_name.empty() && out_cols_->at(i).case_compare(sel_expr->expr_name) == 0)) {
            found = true;
          }
          if (found && OB_FAIL(j_obj_expr->params.push_back(col_expr))) {
            LOG_WARN("fail to append select item", K(ret));
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to find output expr", K(ret), K(col_name->expr_name));
        }
      }
    }
    for (uint64_t i = 0; OB_SUCC(ret) && i < query_req->score_items_.count(); i++) {
      ObReqExpr *score = query_req->score_items_.at(i);
      ObReqConstExpr *col_name = NULL;
      ObReqColumnExpr *col = NULL;
      if (score->alias_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get score item alias", K(ret));
      } else if (OB_FAIL(ObReqConstExpr::construct_const_expr(col_name, alloc_, score->alias_name, ObVarcharType))) {
        LOG_WARN("fail to create const expr", K(ret));
      } else if (OB_FAIL(j_obj_expr->params.push_back(col_name))) {
        LOG_WARN("fail to append name", K(ret));
      } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col, alloc_, score->alias_name))) {
        LOG_WARN("fail to create column expr", K(ret));
      } else if (OB_FAIL(j_obj_expr->params.push_back(col))) {
        LOG_WARN("fail to append name", K(ret));
      }
    }
    ObString sub_query_name;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(j_arrayagg_expr->params.push_back(j_obj_expr))) {
      LOG_WARN("fail to append json array agg param", K(ret));
    } else if (OB_FAIL(wrap_sub_query(sub_query_name, query_req))) {
      LOG_WARN("fail to push back sub query", K(ret));
    } else if (OB_FAIL(query_req->select_items_.push_back(j_arrayagg_expr))) {
      LOG_WARN("fail to append json array agg param", K(ret));
    } else {
      query_req->output_all_columns_ = false;
    }
  }
  return ret;
}

int ObESQueryParser::set_output_columns(ObQueryReqFromJson &query_res, bool is_hybrid, bool include_inner_column/* true */)
{
  int ret = OB_SUCCESS;
  query_res.output_all_columns_ = false;
  if (!query_res.is_score_item_exist()) {
    ObReqColumnExpr *score_col = NULL;
    if (OB_FAIL(ObReqColumnExpr::construct_column_expr(score_col, alloc_, SCORE_NAME))) {
      LOG_WARN("fail to create column expr", K(ret));
    } else if (OB_FAIL(query_res.add_score_item(alloc_, score_col))) {
      LOG_WARN("fail to add score item", K(ret));
    }
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < out_cols_->count(); i++) {
    if (is_hybrid && !is_inner_column(out_cols_->at(i))) {
      ObReqColumnExpr *fts_col = NULL;
      ObReqColumnExpr *vs_col = NULL;
      ObReqExpr *if_null = NULL;
      if (OB_FAIL(ObReqColumnExpr::construct_column_expr(fts_col, alloc_, out_cols_->at(i), FTS_ALIAS))) {
        LOG_WARN("fail to create column expr", K(ret));
      } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(vs_col, alloc_, out_cols_->at(i), VS_ALIAS))) {
        LOG_WARN("fail to create column expr", K(ret));
      } else if (OB_FAIL(ObReqExpr::construct_expr(if_null, alloc_, N_IFNULL, out_cols_->at(i)))) {
        LOG_WARN("fail to create score expr for query", K(ret));
      } else if (OB_FAIL(if_null->params.push_back(fts_col))) {
        LOG_WARN("fail to append param", K(ret));
      } else if (OB_FAIL(if_null->params.push_back(vs_col))) {
        LOG_WARN("fail to append param", K(ret));
      } else if (OB_FAIL(query_res.select_items_.push_back(if_null))) {
        LOG_WARN("failed to append output columns", K(ret));
      }
    } else if (!is_inner_column(out_cols_->at(i)) || include_inner_column) {
      ObReqColumnExpr *col = NULL;
      if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col, alloc_, out_cols_->at(i)))) {
        LOG_WARN("fail to create column expr", K(ret));
      } else if (OB_FAIL(query_res.select_items_.push_back(col))) {
        LOG_WARN("failed to append output columns", K(ret));
      }
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
  if (OB_FAIL(ObReqColumnExpr::construct_column_expr(l_expr, alloc_, l_expr_name, l_table))) {
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(r_expr, alloc_, r_expr_name, r_table))) {
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(join_condition, alloc_, condition, l_expr, r_expr))) {
    LOG_WARN("fail to create join condition", K(ret));
  }
  return ret;
}

int ObESQueryParser::set_default_score(ObQueryReqFromJson *query_req, double default_score)
{
  int ret = OB_SUCCESS;
  ObReqConstExpr *score_expr = nullptr;
  // negative score is invalid
  OB_ASSERT(default_score >= 0);
  if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(score_expr, alloc_, default_score, ObIntType))) {
    LOG_WARN("fail to create score expr", K(ret));
  } else if (OB_FAIL(query_req->add_score_item(alloc_, score_expr))) {
    LOG_WARN("fail to add score item", K(ret));
  }
  return ret;
}

int ObESQueryParser::set_order_by_column(ObQueryReqFromJson *query_req, const ObString &column_name, bool ascent)
{
  int ret = OB_SUCCESS;
  ObReqColumnExpr *column_expr = nullptr;
  OrderInfo *order_info = nullptr;
  if (OB_FAIL(ObReqColumnExpr::construct_column_expr(column_expr, alloc_, column_name))) {
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_FAIL(construct_order_by_item(column_expr, ascent, order_info))) {
    LOG_WARN("fail to construct order by item", K(ret));
  } else if (OB_FAIL(query_req->order_items_.push_back(order_info))) {
    LOG_WARN("fail to push order item", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_hybrid_query(ObQueryReqFromJson *fts, ObQueryReqFromJson *knn, ObQueryReqFromJson *&hybrid)
{
  int ret = OB_SUCCESS;
  ObString rowkey_hint("opt_param('hidden_column_visible', 'true')");
  ObReqColumnExpr *fts_rowkey = NULL;
  ObReqColumnExpr *knn_rowkey = NULL;
  ObReqExpr *fts_score = NULL;
  ObReqExpr *knn_score = NULL;
  ObReqColumnExpr *fts_col = NULL;
  ObReqColumnExpr *knn_col = NULL;
  ObReqJoinedTable *join_table = NULL;
  ObReqTable *fts_table = NULL;
  ObReqTable *knn_table = NULL;
  ObString fts_alias = "_fts";
  ObString knn_alias = "_vs";
  ObString score_alias = SCORE_NAME;
  ObString rowkey = ROWKEY_NAME;
  ObReqOpExpr *join_condition = NULL;
  ObReqOpExpr *score_res = NULL;
  OrderInfo *order_info = NULL;
  ObQueryReqFromJson *base_table_fts_req = NULL;
  ObQueryReqFromJson *base_table_knn_req = NULL;
  ReqTableType knn_table_type = UNKNOWN_TABLE;
  if (OB_FAIL(get_base_table_query(fts, base_table_fts_req))) {
    LOG_WARN("fail to get base table query", K(ret));
  } else if (OB_FAIL(get_base_table_query(knn, base_table_knn_req, &knn_table_type))) {
    LOG_WARN("fail to get base table query", K(ret));
  } else if (OB_ISNULL(hybrid = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(fts_rowkey, alloc_, rowkey))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(knn_rowkey, alloc_, rowkey))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(fts_col, alloc_, FTS_SCORE_NAME))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(knn_col, alloc_, VS_SCORE_NAME))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (FALSE_IT(fts_score = fts_col)) {
  } else if (FALSE_IT(knn_score = knn_col)) {
  } else if (OB_ISNULL(join_table = OB_NEWx(ObReqJoinedTable, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(base_table_fts_req->add_req_hint(rowkey_hint))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(knn_table_type != MULTI_SET && base_table_knn_req->add_req_hint(rowkey_hint))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(base_table_fts_req->select_items_.push_back(fts_rowkey))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(knn_table_type != MULTI_SET && base_table_knn_req->select_items_.push_back(knn_rowkey))) {
    LOG_WARN("fail to create query request", K(ret));
  } else {
    fts->output_all_columns_ = false;
    fts->score_alias_ = FTS_SCORE_NAME;
    knn->score_alias_ = VS_SCORE_NAME;
    if (!fts->order_items_.empty()) {
      if (query_not_need_order(fts)) {
        fts->order_items_.reset();
      } else {
        fts->order_items_.at(0)->order_item->set_alias(fts->score_alias_);
      }
    }
    if (!knn->order_items_.empty() && query_not_need_order(knn)) {
      knn->order_items_.reset();
    }
    if (!out_cols_->empty()) {
      if (OB_FAIL(set_output_columns(*knn, false, false))) {
        LOG_WARN("fail to set output columns", K(ret));
      } else if (OB_FAIL(set_output_columns(*fts, false, false))) {
        LOG_WARN("fail to set output columns", K(ret));
      }
    } else {
      // only for unitest
      if (!knn->is_score_item_exist() && OB_FAIL(add_score_col("", *knn))) {
        LOG_WARN("fail to add score col", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (base_table_fts_req != fts && OB_FAIL(fts->select_items_.push_back(fts_rowkey))) {
      LOG_WARN("fail to create query request", K(ret));
    } else if (base_table_knn_req != knn && !knn->output_all_columns_ && OB_FAIL(knn->select_items_.push_back(knn_rowkey))) {
      LOG_WARN("fail to create query request", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (fusion_config_.method == ObFusionMethod::RRF) {
    ObString empty_str = "";
    ObString fts_rank_alias = FTS_RANK_NAME;
    ObString vs_rank_alias = VS_RANK_NAME;
    if (OB_FAIL(construct_rank_query(empty_str, fts_score, fts_rank_alias, fts))) {
      LOG_WARN("fail to construct keyword rank query", K(ret));
    } else if (OB_FAIL(construct_rank_query(empty_str, knn_score, vs_rank_alias, knn))) {
      LOG_WARN("fail to construct keyword rank query", K(ret));
    } else if (OB_FAIL(construct_rank_score(fts_alias, fts_rank_alias, fts_score))) {
      LOG_WARN("fail to construct keyword rank score expr", K(ret));
    } else if (OB_FAIL(construct_rank_score(knn_alias, vs_rank_alias, knn_score))) {
      LOG_WARN("fail to construct knn rank score expr", K(ret));
    }
  } else if (FALSE_IT(static_cast<ObReqColumnExpr *>(fts_score)->table_name = fts_alias)) {
  } else if (FALSE_IT(static_cast<ObReqColumnExpr *>(knn_score)->table_name = knn_alias)) {
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(construct_sub_query_table(fts_alias, fts, fts_table))) {
    LOG_WARN("fail to create sub query table", K(ret));
  } else if (OB_FAIL(construct_sub_query_table(knn_alias, knn, knn_table))) {
    LOG_WARN("fail to create sub query table", K(ret));
  } else if (OB_FAIL(construct_join_condition(fts_alias, knn_alias, rowkey, rowkey, T_OP_EQ, join_condition))) {
    LOG_WARN("fail to create op expr", K(ret));
  } else if (FALSE_IT(join_table->init(fts_table, knn_table, join_condition, ObReqJoinType::FULL_OUTER_JOIN))) {
  } else if (OB_FAIL(hybrid->from_items_.push_back(join_table))) {
    LOG_WARN("fail to append from item", K(ret));
  } else if (OB_FAIL(construct_score_sum_expr(fts_score, knn_score, score_alias, score_res))) {
    LOG_WARN("fail to construct score sum expr", K(ret));
  } else if (OB_FAIL(hybrid->score_items_.push_back(score_res))) {
    LOG_WARN("fail to append select item", K(ret));
  } else if (OB_FAIL(construct_order_by_item(score_res, false, order_info))) {
    LOG_WARN("fail to construct order by item", K(ret));
  } else if (OB_FAIL(hybrid->order_items_.push_back(order_info))) {
    LOG_WARN("fail to push query order item", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_rank_query(ObString &sub_query_name, ObReqExpr *order_expr, ObString &rank_alias, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObReqWindowFunExpr *rank_expr = NULL;
  OrderInfo *order_info = NULL;
  ObString expr_name = "RANK";
  if (OB_FAIL(wrap_sub_query(sub_query_name, query_req))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else if (OB_FAIL(construct_order_by_item(order_expr, false, order_info))) {
    LOG_WARN("fail to construct order by item", K(ret));
  } else if (OB_FAIL(ObReqWindowFunExpr::construct_window_fun_expr(alloc_, order_info, expr_name, rank_alias, rank_expr))) {
    LOG_WARN("fail to construct window fun expr", K(ret));
  } else if (OB_FAIL(query_req->select_items_.push_back(rank_expr))) {
    LOG_WARN("fail to append rank expr", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_rank_score(const ObString &table_name, const ObString &rank_alias, ObReqExpr *&rank_score)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *div_expr = NULL;
  ObReqOpExpr *add_expr = NULL;
  ObReqConstExpr *const_expr = NULL;
  ObReqColumnExpr *ref_expr = NULL;

  if (OB_FAIL(ObReqColumnExpr::construct_column_expr(ref_expr, alloc_, rank_alias, table_name))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(const_expr, alloc_, 1.0, ObIntType))) {
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc_, T_OP_ADD, ref_expr, fusion_config_.rank_const))) {
    LOG_WARN("fail to create op expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(div_expr, alloc_, T_OP_DIV, const_expr, add_expr))) {
    LOG_WARN("fail to create op expr", K(ret));
  } else {
    rank_score = div_expr;
  }

  return ret;
}

int ObESQueryParser::parse_basic_table(const ObString &table_name, ObQueryReqFromJson *query_req)
{
  int ret = OB_SUCCESS;
  ObReqTable *table = NULL;
  if (OB_ISNULL(table = OB_NEWx(ObReqTable, &alloc_, BASE_TABLE, table_name, database_name_))) {
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
  if (OB_ISNULL(query_req = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(check_is_basic_query(req_node, 0))) {
    LOG_WARN("fail to check is basic query", K(ret));
  }

  ObEsQueryInfo query_info(query_req, QUERY_ITEM_QUERY, is_basic_query_);
  if (OB_SUCC(ret) && OB_FAIL(parse_single_term(req_node, query_info))) {
    LOG_WARN("unexpectd json type", K(ret));
  }

  ObReqExpr *score_expr = nullptr;
  ObReqExpr *condition_expr = nullptr;
  ObReqExpr *basic_query_condition_expr = nullptr;
  if (OB_FAIL(ret)) {
  } else if (enable_es_mode_ && query_info.support_es_mode()) {
    if (OB_FAIL(construct_es_expr(query_info))) {
      LOG_WARN("fail to construct match expr es", K(ret));
    } else {
      ObReqExpr *esql_score_expr = nullptr;
      if (OB_FAIL(ObReqExpr::construct_expr(esql_score_expr, alloc_, "score()"))) {
        LOG_WARN("fail to create score expr for query", K(ret));
      } else {
        score_expr = esql_score_expr;
      }
      condition_expr = query_info.esql_condition_expr_;
    }
  } else {
    score_expr = query_info.score_expr_;
    condition_expr = query_info.condition_expr_;
    if (is_basic_query_ && check_need_construct_msm_expr(query_info)) {
      if (OB_FAIL(construct_basic_query_select_items_with_query_string(query_info, query_info.basic_query_score_items_))) {
        LOG_WARN("fail to construct basic query select items with query string", K(ret));
      } else {
        basic_query_condition_expr = query_info.basic_query_condition_expr_;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(basic_query_condition_expr) && OB_FAIL(outer_filter_items_.push_back(basic_query_condition_expr))) {
    LOG_WARN("failed add term to outer filter items", K(ret));
  } else if (OB_NOT_NULL(condition_expr) && OB_FAIL(query_req->condition_items_.push_back(condition_expr))) {
      LOG_WARN("failed add term to query request", K(ret));
  } else if (OB_NOT_NULL(score_expr) && OB_FAIL(query_req->add_score_item(alloc_, score_expr))) {
      LOG_WARN("failed add term to score items", K(ret));
  } else if (OB_FAIL(parse_basic_table(table_name_, query_req))) {
    LOG_WARN("fail to parse basic table", K(ret));
  } else if (need_construct_sub_query() && OB_FAIL(construct_sub_query_with_minimum_should_match(query_req))) {
    LOG_WARN("fail to construct query with minimum should match", K(ret));
  } else if (query_req->score_items_.empty()) {
    if (OB_FAIL(set_default_score(query_req, 0.0))) {
      LOG_WARN("fail to set default score", K(ret));
    } else {
      query_info.score_is_const_ = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (query_info.score_is_const_) {
    if (OB_FAIL(set_order_by_column(query_req, ROWKEY_NAME, true))) {
      LOG_WARN("fail to set order by rowkey", K(ret));
    }
  } else {
    OrderInfo *order_info = nullptr;
    if (OB_FAIL(construct_order_by_item(query_req->score_items_.at(0), false, order_info))) {
      LOG_WARN("fail to construct order by item", K(ret));
    } else if (OB_FAIL(query_req->order_items_.push_back(order_info))) {
      LOG_WARN("fail to push query order item", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::construct_es_expr_fields(ObReqColumnExpr *raw_field, ObReqExpr *&field)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(raw_field)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("raw_field is null", K(ret));
  } else {
    char *buf = static_cast<char *>(alloc_.alloc(OB_MAX_SQL_LENGTH));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for field param buffer", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, "%.*s", raw_field->expr_name.length(), raw_field->expr_name.ptr()))) {
      LOG_WARN("fail to write field name", K(ret));
    } else if (OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, "^%.15g",
                                       (raw_field->weight_ == -1.0) ? 1.0 : raw_field->weight_))) {
      LOG_WARN("fail to write field weight", K(ret));
    } else {
      ObString field_param_str(pos, buf);
      if (OB_FAIL(ObReqExpr::construct_expr(field, alloc_, field_param_str))) {
        LOG_WARN("fail to create field param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_es_expr_params(const ObEsQueryInfo &query_info, ObReqConstExpr *&options)
{
  int ret = OB_SUCCESS;
  char *buf = static_cast<char *>(alloc_.alloc(OB_MAX_SQL_LENGTH));
  int64_t pos = 0;
  const char *score_type_str = (query_info.score_type_ == SCORE_TYPE_BEST_FIELDS) ? "best_fields" : "most_fields";
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for match params buffer", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, "operator=or"))) {
    LOG_WARN("fail to write default_operator", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, ";boost=%.15g", query_info.boost_))) {
    LOG_WARN("fail to write boost", K(ret));
  } else if (OB_NOT_NULL(query_info.qs_msm_info_.msm_expr_) &&
             OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, ";minimum_should_match=%.*s", query_info.qs_msm_info_.msm_expr_->expr_name.length(), query_info.qs_msm_info_.msm_expr_->expr_name.ptr()))) {
    LOG_WARN("fail to write minimum_should_match", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, ";type=%s", score_type_str))) {
    LOG_WARN("fail to write score_type", K(ret));
  } else {
    ObString options_str(pos, buf);
    if (OB_FAIL(ObReqConstExpr::construct_const_expr(options, alloc_, options_str, ObVarcharType))) {
      LOG_WARN("fail to create match params expr", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::construct_es_expr(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (!query_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid match params", K(ret));
  } else {
    // construct fields
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> params;
    for (int i = 0; OB_SUCC(ret) && i < query_info.field_exprs_.count(); i++) {
      ObReqExpr *field = nullptr;
      if (OB_FAIL(construct_es_expr_fields(query_info.field_exprs_.at(i), field))) {
        LOG_WARN("fail to create field param expr", K(ret));
      } else if (OB_FAIL(params.push_back(field))) {
        LOG_WARN("fail to add field param expr to params", K(ret));
      }
    }

    // construct keywords
    if (OB_SUCC(ret)) {
      ObReqConstExpr *keywords = nullptr;
      char *buf = static_cast<char *>(alloc_.alloc(OB_MAX_SQL_LENGTH));
      int64_t pos = 0;
      ObString keywords_str;
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for keyword param buffer", K(ret));
      } else if (OB_FAIL(databuff_printf(buf, OB_MAX_SQL_LENGTH, pos, "%.*s", query_info.query_keywords_.length(), query_info.query_keywords_.ptr()))) {
        LOG_WARN("fail to write keyword param", K(ret));
      } else if (OB_FALSE_IT(keywords_str = ObString(pos, buf))) {
      } else if (OB_FAIL(ObReqConstExpr::construct_const_expr(keywords, alloc_, keywords_str, ObVarcharType))) {
        LOG_WARN("fail to create keyword param expr", K(ret));
      } else if (OB_FAIL(params.push_back(keywords))) {
        LOG_WARN("fail to add keyword param expr to params", K(ret));
      }
    }

    // construct options
    if (OB_SUCC(ret)) {
      ObReqConstExpr *options = nullptr;
      if (OB_FAIL(construct_es_expr_params(query_info, options))) {
        LOG_WARN("fail to create es params expr", K(ret));
      } else if (OB_FAIL(params.push_back(options))) {
        LOG_WARN("fail to add match params expr to params", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObReqExpr *es_match_expr = nullptr;
      if (OB_FAIL(ObReqExpr::construct_expr(es_match_expr, alloc_, "MATCH", params))) {
        LOG_WARN("fail to create es_match_expr", K(ret));
      } else {
        query_info.esql_condition_expr_ = es_match_expr;
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_bool(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObReqConstExpr *boost_expr = nullptr;
  ObQueryReqFromJson *query_req = query_info.query_req_;
  bool need_cal_score = query_info.need_cal_score_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> score_items;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> condition_items;
  BoolQueryMinShouldMatchInfo bq_msm_info;
  query_info.query_item_ = QUERY_ITEM_BOOL;
  query_info.is_basic_query_ = is_basic_query_;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  // Affects the default value of minimum_should_match.
  // IF exists must or filter, the default value of minimum_should_match will be 0.
  } else if (OB_FAIL(construct_minimum_should_match_info(req_node, bq_msm_info))) {
    LOG_WARN("fail to check has minimum should match", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *sub_node = nullptr;
    ObReqExpr *condition_item = nullptr;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("must") == 0) {
      if (OB_FAIL(parse_must_clauses(*sub_node, query_req, condition_item, score_items, need_cal_score))) {
        LOG_WARN("fail to parse must clauses", K(ret), K(i));
      }
    } else if (key.case_compare("should") == 0) {
      if (OB_FAIL(parse_should_clauses(*sub_node, query_req, condition_item, score_items, bq_msm_info, need_cal_score))) {
        LOG_WARN("fail to parse should clauses", K(ret), K(i));
      }
    } else if (key.case_compare("filter") == 0) {
      if (OB_FAIL(parse_filter_clauses(*sub_node, query_req, condition_item))) {
        LOG_WARN("fail to parse filter clauses", K(ret), K(i));
      } else {
        query_info.has_filter_ = true;
      }
    } else if (key.case_compare("must_not") == 0) {
      if (OB_FAIL(parse_must_not_clauses(*sub_node, query_req, condition_item))) {
        LOG_WARN("fail to parse must not clauses", K(ret), K(i));
      } else {
        query_info.has_must_not_ = true;
      }
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(parse_const(*sub_node, boost_expr, true))) {
        LOG_WARN("fail to parse boost value", K(ret), K(i));
      } else if (boost_expr->get_numeric_value() < 0.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("boost value must be greater than 0", K(ret));
      } else {
        continue;
      }
    } else if (key.case_compare("minimum_should_match") == 0) {
      if (OB_NOT_NULL(bq_msm_info.or_expr_)) {
        condition_item = bq_msm_info.or_expr_;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sytnax in query", K(ret), K(key));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(condition_item) && OB_FAIL(condition_items.push_back(condition_item))) {
      LOG_WARN("failed add term to bool expr array", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret) && query_info.need_cal_score_ && score_items.empty() && !query_info.has_must_not_ && !query_info.has_filter_) {
    ObReqConstExpr *score_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(score_expr, alloc_, 1.0, ObIntType))) {
      LOG_WARN("fail to create score expr", K(ret));
    } else if (OB_FAIL(score_items.push_back(score_expr))) {
      LOG_WARN("fail to add score expr to score items", K(ret));
    } else if (query_info.parent_query_item_ == QUERY_ITEM_QUERY) {
      query_info.score_is_const_ = true;
    }
  }
  if (OB_SUCC(ret) && condition_items.empty() &&
      (query_info.parent_query_item_ == QUERY_ITEM_MUST_NOT ||
       query_info.parent_query_item_ == QUERY_ITEM_SHOULD)) {
    ObReqConstExpr *one_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(one_expr, alloc_, 1.0, ObIntType))) {
      LOG_WARN("fail to create one expr", K(ret));
    } else if (OB_FAIL(condition_items.push_back(one_expr))) {
      LOG_WARN("fail to add one expr to condition items", K(ret));
    }
  }
  if (OB_SUCC(ret) && !score_items.empty()) {
    ObReqOpExpr *tmp_score_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_score_expr, alloc_, T_OP_ADD, score_items))) {
      LOG_WARN("fail to construct score expr", K(ret));
    } else if (OB_ISNULL(boost_expr)) {
      query_info.score_expr_ = tmp_score_expr;
    } else {
      ObReqOpExpr *boost_mul_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_mul_expr, alloc_, T_OP_MUL, tmp_score_expr, boost_expr))) {
        LOG_WARN("fail to construct boost expr", K(ret));
      } else {
        query_info.score_expr_ = boost_mul_expr;
      }
    }
  }
  if (OB_SUCC(ret) && !condition_items.empty()) {
    ObReqOpExpr *tmp_condition_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_condition_expr, alloc_, T_OP_AND, condition_items))) {
      LOG_WARN("fail to construct bool expr", K(ret));
    } else {
      query_info.condition_expr_ = tmp_condition_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_must_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr, ObIArray<ObReqExpr *> &score_items, bool need_cal_score /*= true*/)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = NULL;
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> condition_items;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must clause must only has one key", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObEsQueryInfo sub_query_info(query_req, QUERY_ITEM_MUST, is_basic_query_, need_cal_score);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_single_term(*clause_val, sub_query_info))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_NOT_NULL(sub_query_info.score_expr_) && OB_FAIL(score_items.push_back(sub_query_info.score_expr_))) {
      LOG_WARN("failed add term to score items", K(ret), K(i));
    } else if (OB_NOT_NULL(sub_query_info.condition_expr_) && OB_FAIL(condition_items.push_back(sub_query_info.condition_expr_))) {
      LOG_WARN("failed add term to condition items", K(ret), K(i));
    } else if (is_basic_query_ && sub_query_info.qs_msm_info_.msm_val_ > 1) {
      if (OB_FAIL(construct_basic_query_select_items_with_query_string(sub_query_info, sub_query_info.basic_query_score_items_))) {
        LOG_WARN("fail to construct basic query select items with query string", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObReqOpExpr *tmp_condition_expr = nullptr;
    if (is_basic_query_ && tmp_outer_filter_items_.count() > 0) {
      if (OB_FAIL(construct_basic_query_filter_condition_with_and_expr())) {
        LOG_WARN("fail to construct basic query filter condition with and expr", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_condition_expr, alloc_, T_OP_AND, condition_items))) {
      LOG_WARN("fail to construct bool expr", K(ret));
    } else {
      condition_expr = tmp_condition_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_must_not_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = nullptr;
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> condition_items;
  ObReqConstExpr *one_expr = nullptr;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must not clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must not clause must only has one key", K(ret));
  }

  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    // must not clause is not supported in basic query
    ObEsQueryInfo sub_query_info(query_req, QUERY_ITEM_MUST_NOT, false, false);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_single_term(*clause_val, sub_query_info))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_NOT_NULL(sub_query_info.condition_expr_)) {
      if (OB_FAIL(condition_items.push_back(sub_query_info.condition_expr_))) {
        LOG_WARN("failed add term to condition items", K(ret), K(i));
      }
    } else if (OB_ISNULL(one_expr)) {
      if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(one_expr, alloc_, 1.0, ObIntType))) {
        LOG_WARN("fail to create one expr", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObReqOpExpr *or_expr = nullptr;
    // if one_expr is not null, clear condition_items and add one_expr to condition_items as the only one
    if (OB_NOT_NULL(one_expr)) {
      condition_items.reset();
      if (OB_FAIL(condition_items.push_back(one_expr))) {
        LOG_WARN("fail to add one expr to condition items", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObReqOpExpr::construct_op_expr(or_expr, alloc_, T_OP_OR, condition_items))) {
      LOG_WARN("fail to construct or expr", K(ret));
    } else {
      ObReqOpExpr *not_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_unary_op_expr(not_expr, alloc_, T_OP_NOT, or_expr))) {
        LOG_WARN("fail to create not expr", K(ret));
      } else {
        condition_expr = not_expr;
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_should_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr, ObIArray<ObReqExpr *> &score_items, BoolQueryMinShouldMatchInfo &bq_msm_info, bool need_cal_score /*= true*/)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = NULL;
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> condition_items;

  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("should clause must only has one key", K(ret));
  }

  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_FAIL(clause_val->get_key(0, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    }
    bool is_basic_query = is_basic_query_ && bq_msm_info.has_where_condition_;
    ObEsQueryInfo sub_query_info(query_req, QUERY_ITEM_SHOULD, is_basic_query, need_cal_score);
    sub_query_info.bq_msm_info_ = bq_msm_info;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_single_term(*clause_val, sub_query_info))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_NOT_NULL(sub_query_info.score_expr_) && OB_FAIL(score_items.push_back(sub_query_info.score_expr_))) {
      LOG_WARN("fail to add score expr to score items", K(ret), K(i));
    } else if (OB_NOT_NULL(sub_query_info.condition_expr_) && OB_FAIL(condition_items.push_back(sub_query_info.condition_expr_))) {
      LOG_WARN("fail to add condition expr to should exprs", K(ret), K(i));
    } else if (sub_query_info.is_basic_query_) {
      if (sub_query_info.qs_msm_info_.msm_val_ > 1) {
        if (OB_FAIL(construct_basic_query_select_items_with_query_string(sub_query_info, sub_query_info.basic_query_score_items_))) {
          LOG_WARN("fail to construct basic query select items with query string", K(ret));
        }
      } else if (sub_query_info.bq_msm_info_.msm_val_ > 1) {
        if (key.case_compare("query_string") == 0) {
          if (OB_FAIL(construct_alias_column_expr_to_select_items_with_query_string(*sub_query_info.query_req_, sub_query_info))) {
            LOG_WARN("fail to construct basic query select items with query string", K(ret));
          }
        } else {
          if (OB_FAIL(construct_alias_column_expr_to_select_items(*sub_query_info.query_req_, sub_query_info))) {
            LOG_WARN("fail to construct alias column expr to select items", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && bq_msm_info.has_where_condition_) {
    ObReqExpr *should_condition = nullptr;
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> expr_array;

    if (!condition_items.empty()) {
      if (is_basic_query_) {
        if (OB_FAIL(construct_basic_query_filter_condition_with_or_expr(bq_msm_info.msm_val_, bq_msm_info.msm_expr_, condition_items, should_condition))) {
          LOG_WARN("fail to construct basic query filter condition with or expr", K(ret));
        }
      } else {
        if (OB_FAIL(build_should_condition_combine(0, bq_msm_info.msm_val_, condition_items, expr_array, should_condition))) {
          LOG_WARN("fail to build should groups", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      condition_expr = should_condition;
    }
  }
  return ret;
}

int ObESQueryParser::parse_filter_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *clause_val = NULL;
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> condition_items;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT &&
      req_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filter clause must not be empty", K(ret));
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT && count > 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filter clause must only has one key", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
      clause_val = &req_node;
    } else if (OB_FAIL(req_node.get_array_element(i, clause_val))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    }
    ObEsQueryInfo sub_query_info(query_req, QUERY_ITEM_FILTER, is_basic_query_, false);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(parse_single_term(*clause_val, sub_query_info))) {
      LOG_WARN("unexpectd json type", K(ret), K(i));
    } else if (OB_NOT_NULL(sub_query_info.condition_expr_) && OB_FAIL(condition_items.push_back(sub_query_info.condition_expr_))) {
      LOG_WARN("failed add term to condition items", K(ret), K(i));
    } else if (is_basic_query_ && sub_query_info.qs_msm_info_.msm_val_ > 1) {
      if (OB_FAIL(construct_basic_query_select_items_with_query_string(sub_query_info, sub_query_info.basic_query_score_items_))) {
        LOG_WARN("fail to construct basic query select items with query string", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObReqOpExpr *tmp_condition_expr = nullptr;
    if (is_basic_query_ && tmp_outer_filter_items_.count() > 0) {
      if (OB_FAIL(construct_basic_query_filter_condition_with_and_expr())) {
        LOG_WARN("fail to construct basic query filter condition with and expr", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_condition_expr, alloc_, T_OP_AND, condition_items))) {
      LOG_WARN("fail to construct bool expr", K(ret));
    } else {
      condition_expr = tmp_condition_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_single_term(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("single term must only contain one term", K(ret));
  }
  ObString key;
  ObIJsonBase *sub_node = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(req_node.get_key(0, key))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (key.case_compare("bool") == 0) {
    if (OB_FAIL(parse_bool(*sub_node, query_info))) {
      LOG_WARN("fail to parse bool expr", K(ret));
    }
  } else if (key.case_compare("range") == 0) {
    if (OB_FAIL(parse_range(*sub_node, query_info))) {
      LOG_WARN("fail to parse range expr", K(ret));
    }
  } else if (key.case_compare("match") == 0) {
    if (OB_FAIL(parse_match(*sub_node, query_info))) {
      LOG_WARN("fail to parse match expr", K(ret));
    }
  } else if (key.case_compare("term") == 0) {
    if (OB_FAIL(parse_term(*sub_node, query_info))) {
      LOG_WARN("fail to parse term expr", K(ret));
    }
  } else if (key.case_compare("query_string") == 0) {
    if (OB_FAIL(parse_query_string(*sub_node, query_info))) {
      LOG_WARN("fail to parse query string expr", K(ret));
    }
  } else if (key.case_compare("multi_match") == 0) {
    if (OB_FAIL(parse_multi_match(*sub_node, query_info))) {
      LOG_WARN("fail to parse multi match expr", K(ret));
    }
  } else if (key.case_compare("rank_feature") == 0) {
    if (OB_FAIL(parse_rank_feature(*sub_node, query_info))) {
      LOG_WARN("fail to parse rank feature expr", K(ret));
    }
  } else if (key.case_compare("terms") == 0) {
    if (OB_FAIL(parse_terms(*sub_node, query_info))) {
      LOG_WARN("fail to parse terms expr", K(ret));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported sytnax in query", K(ret), K(key));
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
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(weight_const, alloc_, weight, ObDoubleType))) {
      LOG_WARN("fail to create weight const expr", K(ret));
    } else {
      ObReqOpExpr *tmp_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(tmp_expr, alloc_, T_OP_MUL, base_expr, weight_const))) {
        LOG_WARN("fail to construct weight multiplication", K(ret));
      } else {
        weighted_expr = tmp_expr;
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_best_fields(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_greatest_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.keyword_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.field_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = query_info.field_exprs_.at(j);
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, SCORE_TYPE_BEST_FIELDS))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          ObReqExpr *weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, weighted_expr))) {
            LOG_WARN("fail to construct field expr", K(ret));
          } else if (OB_FAIL(field_weighted_exprs.push_back(weighted_expr))) {
            LOG_WARN("fail to add field weighted expr", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *greatest_expr = nullptr;
        if (field_weighted_exprs.count() == 1) {
          greatest_expr = field_weighted_exprs.at(0);
        } else if (OB_FAIL(ObReqExpr::construct_expr(greatest_expr, alloc_, "GREATEST", field_weighted_exprs))) {
          LOG_WARN("fail to create greatest expr", K(ret));
        }
        if (OB_SUCC(ret)) {
          ObReqExpr *keyword_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(greatest_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword weighted expr", K(ret));
          } else if (OB_FAIL(keyword_greatest_exprs.push_back(keyword_weighted_expr))) {
            LOG_WARN("fail to add keyword weighted expr", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObReqOpExpr *add_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, keyword_greatest_exprs))) {
        LOG_WARN("fail to create add expr for keyword scores", K(ret));
      } else if (OB_FALSE_IT(query_info.score_expr_ = add_expr)) {
      } else if (OB_FAIL(construct_query_string_condition(query_info))) {
        LOG_WARN("fail to construct where condition for best_fields", K(ret));
      } else if (query_info.is_basic_query_ && check_need_construct_msm_expr(query_info)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < keyword_greatest_exprs.count(); i++) {
          ObReqExpr *keyword_greatest_expr = keyword_greatest_exprs.at(i);
          if (OB_FAIL(query_info.basic_query_score_items_.push_back(keyword_greatest_expr))) {
            LOG_WARN("fail to add keyword greatest expr to basic query score items", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_cross_fields(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_greatest_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.keyword_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.field_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = query_info.field_exprs_.at(j);
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, SCORE_TYPE_CROSS_FIELDS))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          ObReqExpr *weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, weighted_expr))) {
            LOG_WARN("fail to construct field expr", K(ret));
          } else if (OB_FAIL(field_weighted_exprs.push_back(weighted_expr))) {
            LOG_WARN("fail to add field weighted expr", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *greatest_expr = nullptr;
        if (field_weighted_exprs.count() == 1) {
          greatest_expr = field_weighted_exprs.at(0);
        } else if (OB_FAIL(ObReqExpr::construct_expr(greatest_expr, alloc_, "GREATEST", field_weighted_exprs))) {
          LOG_WARN("fail to create greatest expr", K(ret));
        }
        if (OB_SUCC(ret)) {
          ObReqExpr *keyword_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(greatest_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword weighted expr", K(ret));
          } else if (OB_FAIL(keyword_greatest_exprs.push_back(keyword_weighted_expr))) {
            LOG_WARN("fail to add keyword weighted expr", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObReqOpExpr *add_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, keyword_greatest_exprs))) {
        LOG_WARN("fail to create add expr for keyword scores", K(ret));
      } else if (OB_FALSE_IT(query_info.score_expr_ = add_expr)) {
      } else if (OB_FAIL(construct_query_string_condition(query_info))) {
        LOG_WARN("fail to construct where condition for best_fields", K(ret));
      } else if (query_info.is_basic_query_ && check_need_construct_msm_expr(query_info)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < keyword_greatest_exprs.count(); i++) {
          ObReqExpr *keyword_greatest_expr = keyword_greatest_exprs.at(i);
          if (OB_FAIL(query_info.basic_query_score_items_.push_back(keyword_greatest_expr))) {
            LOG_WARN("fail to add keyword greatest expr to basic query score items", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_most_fields(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_sum_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.keyword_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.field_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = query_info.field_exprs_.at(j);
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, SCORE_TYPE_MOST_FIELDS))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          ObReqExpr *weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, weighted_expr))) {
            LOG_WARN("fail to construct field expr", K(ret));
          } else if (OB_FAIL(field_weighted_exprs.push_back(weighted_expr))) {
            LOG_WARN("fail to add field weighted expr", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqOpExpr *sum_expr = nullptr;
        if (OB_FAIL(ObReqOpExpr::construct_op_expr(sum_expr, alloc_, T_OP_ADD, field_weighted_exprs))) {
          LOG_WARN("fail to create add expr for field sum", K(ret));
        } else {
          ObReqExpr *keyword_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(sum_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword expr", K(ret));
          } else if (OB_FAIL(keyword_sum_exprs.push_back(keyword_weighted_expr))) {
            LOG_WARN("fail to add keyword weighted expr", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObReqOpExpr *add_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, keyword_sum_exprs))) {
        LOG_WARN("fail to create add expr for keyword sum", K(ret));
      } else if (OB_FALSE_IT(query_info.score_expr_ = add_expr)) {
      } else if (OB_FAIL(construct_query_string_condition(query_info))) {
        LOG_WARN("fail to construct where condition for most_fields", K(ret));
      } else if (query_info.is_basic_query_ && check_need_construct_msm_expr(query_info)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < keyword_sum_exprs.count(); i++) {
          ObReqExpr *keyword_sum_expr = keyword_sum_exprs.at(i);
          if (OB_FAIL(query_info.basic_query_score_items_.push_back(keyword_sum_expr))) {
            LOG_WARN("fail to add keyword sum expr to basic query score items", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_phrase(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_greatest_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.keyword_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_weighted_exprs;
      ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(i);
      ObEsScoreType score_type = (keyword_expr->weight_ == -1.0) ? SCORE_TYPE_PHRASE : SCORE_TYPE_BEST_FIELDS;
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.field_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = query_info.field_exprs_.at(j);
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, score_type))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else {
          ObReqExpr *field_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(match_expr, field_expr->weight_, field_weighted_expr))) {
            LOG_WARN("fail to construct field weighted expr", K(ret));
          } else if (OB_FAIL(field_weighted_exprs.push_back(field_weighted_expr))) {
            LOG_WARN("fail to add field weighted expr", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        ObReqExpr *greatest_expr = nullptr;
        if (field_weighted_exprs.count() == 1) {
          greatest_expr = field_weighted_exprs.at(0);
        } else if (OB_FAIL(ObReqExpr::construct_expr(greatest_expr, alloc_, "GREATEST", field_weighted_exprs))) {
          LOG_WARN("fail to create greatest expr", K(ret));
        }
        if (OB_SUCC(ret)) {
          ObReqExpr *keyword_weighted_expr = nullptr;
          if (OB_FAIL(construct_weighted_expr(greatest_expr, keyword_expr->weight_, keyword_weighted_expr))) {
            LOG_WARN("fail to construct keyword weighted expr", K(ret));
          } else if (OB_FAIL(keyword_greatest_exprs.push_back(keyword_weighted_expr))) {
            LOG_WARN("fail to add keyword weighted expr", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObReqOpExpr *add_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, keyword_greatest_exprs))) {
        LOG_WARN("fail to create add expr for keyword scores", K(ret));
      } else if (OB_FALSE_IT(query_info.score_expr_ = add_expr)) {
      } else if (OB_FAIL(construct_query_string_condition(query_info))) {
        LOG_WARN("fail to construct where condition for phrase", K(ret));
      } else if (query_info.is_basic_query_ && check_need_construct_msm_expr(query_info)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < keyword_greatest_exprs.count(); i++) {
          ObReqExpr *keyword_greatest_expr = keyword_greatest_exprs.at(i);
          if (OB_FAIL(query_info.basic_query_score_items_.push_back(keyword_greatest_expr))) {
            LOG_WARN("fail to add keyword greatest expr to basic query score items", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_range(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  ObIJsonBase *sub_node = nullptr;
  ObReqColumnExpr *col_para = nullptr;
  ObReqConstExpr *boost_expr = nullptr;
  uint64_t count = 0;
  int condition_num = 0;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> condition_exprs;
  query_info.query_item_ = QUERY_ITEM_RANGE;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("range must only contain one term", K(ret));
  } else if (OB_FAIL(req_node.get_key(0, col_name))) {
    LOG_WARN("fail to get key", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, sub_node))) {
    LOG_WARN("fail to get value", K(ret));
  } else if (FALSE_IT(count = sub_node->element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpectd range condition", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_para, alloc_, col_name))) {
    LOG_WARN("fail to create column expr", K(ret));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObString key;
    ObIJsonBase *var_node = NULL;
    ObReqConstExpr *var = NULL;
    ObReqOpExpr *cmp_expr = NULL;
    ObItemType type = T_INVALID;
    if (OB_FAIL(sub_node->get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(sub_node->get_object_value(i, var_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (OB_FAIL(parse_const(*var_node, var, true))) {
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
    } else if (key.case_compare("boost") == 0) {
      if (var->get_numeric_value() < 0.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("boost value must be greater than 0", K(ret));
        break;
      } else {
        boost_expr = var;
        continue;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported sytnax in query", K(ret), K(key));
    }

    if (OB_FAIL(ret)) {
    } else if (type != T_INVALID && OB_FAIL(ObReqOpExpr::construct_binary_op_expr(cmp_expr, alloc_, type, col_para, var))) {
      LOG_WARN("fail to construct cmp expr", K(ret));
    } else if (OB_FAIL(condition_exprs.push_back(cmp_expr))) {
      LOG_WARN("fail to add condition to array", K(ret));
    }
  }

  if (OB_SUCC(ret) && condition_exprs.count() > 0) {
    ObReqOpExpr *and_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(and_expr, alloc_, T_OP_AND, condition_exprs))) {
      LOG_WARN("fail to construct and expr", K(ret));
    } else if (OB_FALSE_IT(query_info.condition_expr_ = and_expr)) {
    } else if (OB_ISNULL(boost_expr)) {
      query_info.score_expr_ = and_expr;
    } else {
      ObReqOpExpr *boost_mul_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_mul_expr, alloc_, T_OP_MUL, and_expr, boost_expr))) {
        LOG_WARN("fail to construct boost mul expr", K(ret));
      } else {
        query_info.score_expr_ = boost_mul_expr;
      }
    }
  }

  return ret;
}

int ObESQueryParser::parse_rank_feature(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  query_info.query_item_ = QUERY_ITEM_RANK_FEATURE;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()), K(req_node.element_count()));
  }
  ObRankFeatDef rank_feat_def;
  bool has_field = false;
  uint64_t algorithm_count = 0;
  for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
    ObString key;
    ObIJsonBase *sub_node = NULL;
    if (OB_FAIL(req_node.get_key(i, key))) {
      LOG_WARN("fail to get key", K(ret), K(i));
    } else if (OB_FAIL(req_node.get_object_value(i, sub_node))) {
      LOG_WARN("fail to get value", K(ret), K(i));
    } else if (key.case_compare("field") == 0) {
      if (OB_FAIL(check_rank_feat_param(sub_node, algorithm_count, has_field, key))) {
        LOG_WARN("fail to check rank feature param", K(ret), K(i));
      } else if (OB_FAIL(parse_field(*sub_node, rank_feat_def.number_field))) {
        LOG_WARN("fail to parse vec field", K(ret), K(i));
      } else {
        ObReqConstExpr *const_expr = NULL;
        ObReqOpExpr *is_not_expr = NULL;
        if (OB_FAIL(ObReqConstExpr::construct_const_expr(const_expr, alloc_, "NULL", ObNullType))) {
          LOG_WARN("fail to create query request", K(ret));
        } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(is_not_expr, alloc_, T_OP_IS_NOT, rank_feat_def.number_field, const_expr))) {
          LOG_WARN("fail to construct is not expr", K(ret));
        } else {
          query_info.condition_expr_ = is_not_expr;
        }
      }
    } else if (key.case_compare("saturation") == 0) {
      ObString empty_str;
      if (OB_FAIL(check_rank_feat_param(sub_node, algorithm_count, has_field, key))) {
        LOG_WARN("fail to check rank feature param", K(ret), K(i));
      } else if (OB_FAIL(parse_rank_feat_param(*sub_node, "pivot", empty_str, rank_feat_def.pivot,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::SATURATION;
      }
    } else if (key.case_compare("sigmoid") == 0) {
      ObString ex_str = "exponent";
      if (OB_FAIL(check_rank_feat_param(sub_node, algorithm_count, has_field, key))) {
        LOG_WARN("fail to check rank feature param", K(ret), K(i));
      } else if (OB_FAIL(parse_rank_feat_param(*sub_node, "pivot", ex_str, rank_feat_def.pivot,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::SIGMOID;
      }
    } else if (key.case_compare("linear") == 0) {
      ObString empty_str;
      if (OB_FAIL(check_rank_feat_param(sub_node, algorithm_count, has_field, key))) {
        LOG_WARN("fail to check rank feature param", K(ret), K(i));
      } else if (OB_FAIL(parse_rank_feat_param(*sub_node, empty_str, empty_str, rank_feat_def.pivot,
                                        rank_feat_def.exponent, rank_feat_def.positive_impact))) {
        LOG_WARN("fail to parse rank feature param", K(ret), K(i));
      } else {
        rank_feat_def.type = ObRankFeatureType::LINEAR;
      }
    } else if (key.case_compare("log") == 0) {
      ObString empty_str;
      if (OB_FAIL(check_rank_feat_param(sub_node, algorithm_count, has_field, key))) {
        LOG_WARN("fail to check rank feature param", K(ret), K(i));
      } else if (OB_FAIL(parse_rank_feat_param(*sub_node, "scaling_factor", empty_str, rank_feat_def.scaling_factor,
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
  } else if (!has_field || algorithm_count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rank feature must has field and one algorithm", K(ret));
  } else if (OB_FAIL(construct_rank_feat_expr(rank_feat_def, query_info.score_expr_))) {
    LOG_WARN("fail to construct rank feature expr", K(ret));
  }

  return ret;
}

int ObESQueryParser::check_rank_feat_param(ObIJsonBase *sub_node, uint64_t &algorithm_count, bool &has_field, const ObString &key)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObIJsonBase *pos_val = NULL;
  if (key.case_compare("field") == 0) {
    has_field = true;
  } else {
    if (FALSE_IT(count = sub_node->element_count())) {
    } else if (OB_FAIL(sub_node->get_object_value("positive_score_impact", pos_val))) {
      if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get positive_score_impact value", K(ret));
      }
    } else {
      --count;
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(algorithm_count++)) {
    } else if (algorithm_count > 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpectd rank feature param, only one algorithm is supported", K(ret), K(key));
    } else if (key.case_compare("saturation") == 0 &&
               count != 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpectd rank feature param, saturation must has one param", K(ret), K(key));
    } else if (key.case_compare("sigmoid") == 0 &&
               count != 2) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpectd rank feature param, sigmoid must has two params", K(ret), K(key));
    } else if (key.case_compare("linear") == 0 &&
               count != 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpectd rank feature param, linear must has no param", K(ret), K(key));
    } else if (key.case_compare("log") == 0 &&
               count != 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpectd rank feature param, log must has one param", K(ret), K(key));
    }
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
  } else if (val1 != NULL && OB_FAIL(parse_const(*val1, const_para1, true))) {
    LOG_WARN("fail to parse const value", K(ret));
  } else if (val2 != NULL && OB_FAIL(parse_const(*val2, const_para2, true))) {
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
      if (OB_ISNULL(rank_feat_def.number_field) || OB_ISNULL(rank_feat_def.pivot)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc_, T_OP_ADD, rank_feat_def.number_field, rank_feat_def.pivot))) {
        LOG_WARN("fail to create add expr", K(ret));
      } else if (rank_feat_def.positive_impact && OB_FAIL(ObReqOpExpr::construct_binary_op_expr(div_expr, alloc_, T_OP_DIV, rank_feat_def.number_field, add_expr))) {
        LOG_WARN("fail to create div expr", K(ret));
      } else if (!rank_feat_def.positive_impact && OB_FAIL(ObReqOpExpr::construct_binary_op_expr(div_expr, alloc_, T_OP_DIV, rank_feat_def.pivot, add_expr))) {
        LOG_WARN("fail to create div expr", K(ret));
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
        if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(one_expr, alloc_, 1.0, ObIntType))) {
          LOG_WARN("fail to create const expr", K(ret));
        } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(div_expr, alloc_, T_OP_DIV, one_expr, rank_feat_def.number_field))) {
          LOG_WARN("fail to create div expr", K(ret));
        } else {
          rank_feat_expr = div_expr;
        }
      }
      break;
    }
    case ObRankFeatureType::LOG : {
      // only positive impact
      ObReqExpr *ln_expr = NULL;
      ObReqOpExpr *add_expr = NULL;
      if (OB_ISNULL(rank_feat_def.number_field) || OB_ISNULL(rank_feat_def.scaling_factor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc_, T_OP_ADD, rank_feat_def.number_field, rank_feat_def.scaling_factor))) {
        LOG_WARN("fail to create add expr", K(ret));
      } else if (OB_FAIL(ObReqExpr::construct_expr(ln_expr, alloc_, N_LN, add_expr))) {
        LOG_WARN("fail to create ln expr", K(ret));
      } else {
        rank_feat_expr = ln_expr;
      }
      break;
    }
    case ObRankFeatureType::SIGMOID : {
      ObReqExpr *field_pow_expr = NULL;
      ObReqExpr *piv_pow_expr = NULL;
      ObReqOpExpr *add_expr = NULL;
      ObReqOpExpr *div_expr = NULL;
      if (OB_ISNULL(rank_feat_def.number_field) || OB_ISNULL(rank_feat_def.pivot) || OB_ISNULL(rank_feat_def.exponent)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd null ptr", K(ret));
      } else if (OB_FAIL(ObReqExpr::construct_expr(field_pow_expr, alloc_, N_POW, rank_feat_def.number_field, rank_feat_def.exponent))) {
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_FAIL(ObReqExpr::construct_expr(piv_pow_expr, alloc_, N_POW, rank_feat_def.pivot, rank_feat_def.exponent))) {
        LOG_WARN("fail to create op expr", K(ret));
      } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc_, T_OP_ADD, field_pow_expr, piv_pow_expr))) {
        LOG_WARN("fail to create add expr", K(ret));
      } else if (rank_feat_def.positive_impact && OB_FAIL(ObReqOpExpr::construct_binary_op_expr(div_expr, alloc_, T_OP_DIV, field_pow_expr, add_expr))) {
        LOG_WARN("fail to create div expr", K(ret));
      } else if (!rank_feat_def.positive_impact && OB_FAIL(ObReqOpExpr::construct_binary_op_expr(div_expr, alloc_, T_OP_DIV, piv_pow_expr, add_expr))) {
        LOG_WARN("fail to create div expr", K(ret));
      } else {
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

int ObESQueryParser::parse_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  ObString query_text;
  ObString idx_name;
  ObIJsonBase *col_para = nullptr;
  ObReqColumnExpr *col_expr = nullptr;
  ObReqConstExpr *boost_expr = nullptr;
  ObReqMatchExpr *match_expr = nullptr;
  ObReqExpr *score_expr = nullptr;
  query_info.query_item_ = QUERY_ITEM_MATCH;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("match expr should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("match expr should have exactly one element", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, col_para))) {
    LOG_WARN("fail to get value.", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, col_name))) {
    LOG_WARN("fail to create match expr", K(ret));
  } else if (col_para->json_type() == ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("match field should have exactly one element", K(ret));
  } else if (col_para->json_type() != ObJsonNodeType::J_OBJECT) {
    query_text = ObString(col_para->get_data_length(), col_para->get_data()).trim();
    ObReqConstExpr *query_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_expr(query_expr, alloc_, query_text, ObVarcharType))) {
      LOG_WARN("fail to create const expr", K(ret));
    } else if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, col_expr, query_expr))) {
      LOG_WARN("fail to construct match expr", K(ret));
    } else {
      score_expr = match_expr;
    }
  } else if (col_para->json_type() == ObJsonNodeType::J_OBJECT) {
    bool found_query = false;
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_para->element_count(); i++) {
      ObIJsonBase *value_node = NULL;
      ObString key;
      if (OB_FAIL(col_para->get_object_value(i, key, value_node))) {
        LOG_WARN("fail to get value.", K(ret));
      } else if (key.case_compare("query") == 0) {
        query_text = ObString(value_node->get_data_length(), value_node->get_data()).trim();
        ObReqConstExpr *query_expr = nullptr;
        if (OB_FAIL(ObReqConstExpr::construct_const_expr(query_expr, alloc_, query_text, ObVarcharType))) {
          LOG_WARN("fail to create const expr", K(ret));
        } else if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, col_expr, query_expr))) {
          LOG_WARN("fail to construct match expr", K(ret));
        } else {
          found_query = true;
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(parse_const(*value_node, boost_expr, true))) {
          LOG_WARN("fail to parse const value", K(ret));
        } else if (boost_expr->get_numeric_value() < 0.0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("boost value must be greater than 0", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("It's not supported to use this key in match expr", K(ret), K(key));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!found_query) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("match expr should have query", K(ret));
    } else if (OB_ISNULL(boost_expr)) {
      score_expr = match_expr;
    } else {
      ObReqOpExpr *boost_mul_expr = nullptr;
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_mul_expr, alloc_, T_OP_MUL, match_expr, boost_expr))) {
        LOG_WARN("fail to construct boost multiplication expr", K(ret));
      } else {
        score_expr = boost_mul_expr;
        query_info.boost_ = boost_expr->get_numeric_value();
      }
    }
  }

  // index hint
  ObQueryReqFromJson *query_req = query_info.query_req_;
  if (OB_SUCC(ret) && OB_FAIL(get_match_idx_name(col_name, idx_name))) {
    LOG_WARN("fail to get match index name", K(ret));
  } else if (!idx_name.empty()) {
    if (query_req->match_idxs_.count() == 0) {
      // add table name first, for generate union merge hint
      if (OB_FAIL(query_req->match_idxs_.push_back(table_name_))) {
        LOG_WARN("fail to append table name", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(query_req->match_idxs_.push_back(idx_name))) {
      LOG_WARN("fail to append match index name", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    query_info.score_expr_ = score_expr;
    query_info.condition_expr_ = match_expr;
    if (OB_FAIL(query_info.field_exprs_.push_back(col_expr))) {
      LOG_WARN("fail to add field to query_info.field_exprs_", K(ret));
    } else {
      query_info.query_keywords_ = query_text;
    }
  }

  return ret;
}

int ObESQueryParser::parse_term(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObString col_name;
  ObIJsonBase *col_para = NULL;
  ObReqOpExpr *eq_expr = NULL;
  ObReqColumnExpr *col_expr = NULL;
  ObReqConstExpr *value_expr = NULL;
  ObReqConstExpr *boost_expr = NULL;
  query_info.query_item_ = QUERY_ITEM_TERM;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("term expr should be object", K(ret));
  } else if (req_node.element_count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("term expr should have exactly one element", K(ret));
  } else if (OB_FAIL(req_node.get_object_value(0, col_name, col_para))) {
    LOG_WARN("fail to get value.", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, col_name))) {
    LOG_WARN("fail to create term expr", K(ret));
  } else if (col_para->json_type() == ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("term field should have exactly one element", K(ret));
  } else if (col_para->json_type() != ObJsonNodeType::J_OBJECT) {
    if (OB_FAIL(parse_const(*col_para, value_expr))) {
      LOG_WARN("fail to parse const value", K(ret));
    } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(eq_expr, alloc_, T_OP_EQ, col_expr, value_expr))) {
      LOG_WARN("fail to construct eq expr", K(ret));
    } else {
      query_info.score_expr_ = eq_expr;
      query_info.condition_expr_ = eq_expr;
    }
  } else /*if (col_para->json_type() == ObJsonNodeType::J_OBJECT)*/ {
    for (uint64_t i = 0; OB_SUCC(ret) && i < col_para->element_count(); i++) {
      ObIJsonBase *value_node = NULL;
      ObString key;
      if (OB_FAIL(col_para->get_object_value(i, key, value_node))) {
        LOG_WARN("fail to get value.", K(ret));
      } else if (key.case_compare("value") == 0) {
        if (OB_FAIL(parse_const(*value_node, value_expr))) {
          LOG_WARN("fail to parse const value", K(ret));
        } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(eq_expr, alloc_, T_OP_EQ, col_expr, value_expr))) {
          LOG_WARN("fail to construct eq expr", K(ret));
        }
      } else if (key.case_compare("boost") == 0) {
        if (OB_FAIL(parse_const(*value_node, boost_expr, true))) {
          LOG_WARN("fail to parse const value", K(ret));
        } else if (boost_expr->get_numeric_value() < 0.0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("boost value must be greater than 0", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported key in term expr", K(ret), K(key));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(eq_expr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("term expr should have value", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(query_info.condition_expr_ = eq_expr)) {
  } else if (OB_ISNULL(boost_expr)) {
    query_info.score_expr_ = eq_expr;
  } else {
    ObReqOpExpr *boost_mul_expr = NULL;
    if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_mul_expr, alloc_, T_OP_MUL, eq_expr, boost_expr))) {
      LOG_WARN("fail to construct boost multiplication expr", K(ret));
    } else {
      query_info.score_expr_ = boost_mul_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_terms(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  ObReqColumnExpr *col_expr = NULL;
  ObReqConstExpr *value_expr = NULL;
  ObReqConstExpr *boost_expr = NULL;
  ObReqOpExpr *in_expr = NULL;
  common::ObSEArray<ObReqConstExpr*, 4, common::ModulePageAllocator, true> value_exprs;
  bool has_field = false;
  query_info.query_item_ = QUERY_ITEM_TERMS;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("terms expr should be object", K(ret));
  } else if (FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0 || count > 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("terms expr only supports field and boost", K(ret));
  }

  for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObIJsonBase *value_node = NULL;
    ObString key;
    if (OB_FAIL(req_node.get_object_value(i, key, value_node))) {
      LOG_WARN("fail to get value.", K(ret));
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(parse_const(*value_node, boost_expr, true))) {
        LOG_WARN("fail to parse const value", K(ret));
      } else if (boost_expr->get_numeric_value() < 0.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("boost value must be greater than 0", K(ret));
      }
    } else {
      if (has_field) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("terms expr only supports one field", K(ret));
      } else if (value_node->json_type() == ObJsonNodeType::J_OBJECT) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("It's not supported to use object as field value", K(ret), K(key));
      } else if (value_node->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
        LOG_WARN("unexpectd value type, should be array", K(ret), K(value_node->json_type()));
      } else if (key.empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("field should not be empty string", K(ret));
      } else if (FALSE_IT(has_field = true)) {
      } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, key))) {
        LOG_WARN("fail to create term expr", K(ret));
      } else if (OB_FAIL(parse_keyword_array(*value_node, value_exprs))) {
        LOG_WARN("fail to parse keyword array", K(ret));
      } else if (OB_FAIL(ObReqOpExpr::construct_in_expr(alloc_, col_expr, value_exprs, in_expr))) {
        LOG_WARN("fail to construct in expr", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!has_field) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("terms expr should have field", K(ret));
  } else if (FALSE_IT(query_info.condition_expr_ = in_expr)) {
  } else if (FALSE_IT(query_info.score_expr_ = in_expr)) {
  } else if (OB_NOT_NULL(boost_expr)) {
    ObReqOpExpr *boost_mul_expr = NULL;
    if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_mul_expr, alloc_, T_OP_MUL, in_expr, boost_expr))) {
      LOG_WARN("fail to construct boost multiplication expr", K(ret));
    } else {
      query_info.score_expr_ = boost_mul_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_query_string_type(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *type_node = nullptr;
  ObString type_key("type");
  if (OB_FAIL(req_node.get_object_value(type_key, type_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      LOG_DEBUG("type field not found in query_string, use default value", K(query_info.score_type_));
    } else {
      LOG_WARN("fail to get type field", K(ret));
    }
  } else if (OB_ISNULL(type_node) || type_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("type field is null", K(ret));
  } else if (type_node->json_type() == ObJsonNodeType::J_STRING) {
    ObString type_str(type_node->get_data_length(), type_node->get_data());
    if (type_str.case_compare("best_fields") == 0) {
      query_info.score_type_ = SCORE_TYPE_BEST_FIELDS;
    } else if (type_str.case_compare("cross_fields") == 0) {
      query_info.score_type_ = SCORE_TYPE_CROSS_FIELDS;
    } else if (type_str.case_compare("most_fields") == 0) {
      query_info.score_type_ = SCORE_TYPE_MOST_FIELDS;
    } else if (type_str.case_compare("phrase") == 0) {
      query_info.score_type_ = SCORE_TYPE_PHRASE;
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

int ObESQueryParser::parse_query_string_fields(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *fields_node = nullptr;
  ObString fields_key("fields");
  if (OB_FAIL(req_node.get_object_value(fields_key, fields_node))) {
    LOG_WARN("fail to get fields field", K(ret));
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
      } else if (OB_FAIL(query_info.field_exprs_.push_back(field))) {
        LOG_WARN("fail to add field to query_info.field_exprs_", K(ret), K(i));
      }
    }
  } else if (fields_node->json_type() == ObJsonNodeType::J_STRING) {
    ObReqColumnExpr *field = nullptr;
    if (OB_FAIL(parse_field(*fields_node, field))) {
      LOG_WARN("fail to parse clause", K(ret));
    } else if (OB_FAIL(query_info.field_exprs_.push_back(field))) {
      LOG_WARN("fail to add single field to query_info.field_exprs_", K(ret));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("fields should be string or array", K(ret), K(fields_node->json_type()));
  }
  return ret;
}

int ObESQueryParser::parse_query_string_operator(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *operator_node = nullptr;
  ObString operator_key(query_info.query_item_ == QUERY_ITEM_MULTI_MATCH ? "operator" : "default_operator");
  if (OB_FAIL(req_node.get_object_value(operator_key, operator_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      LOG_DEBUG("default_operator field not found in query_string, use default value OR", K(query_info.operator_));
    } else {
      LOG_WARN("fail to get default_operator field", K(ret));
    }
  } else if (OB_ISNULL(operator_node) || operator_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("default_operator field is null", K(ret));
  } else if (operator_node->json_type() == ObJsonNodeType::J_STRING) {
    ObString operator_str(operator_node->get_data_length(), operator_node->get_data());
    if (operator_str.case_compare("AND") == 0) {
      query_info.operator_ = T_OP_AND;
    } else if (operator_str.case_compare("OR") == 0) {
      query_info.operator_ = T_OP_OR;
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

int ObESQueryParser::parse_query_string_query(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObIJsonBase *query_node = nullptr;
  ObString query_key("query");
  ObString query_text;
  if (OB_FAIL(req_node.get_object_value(query_key, query_node))) {
    LOG_WARN("fail to get query field", K(ret));
  } else if (OB_ISNULL(query_node) || query_node->json_type() == ObJsonNodeType::J_NULL) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("query field is null", K(ret));
  } else if (query_node->json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("query should be string", K(ret), K(query_node->json_type()));
  } else if (OB_FALSE_IT(query_text.assign_ptr(query_node->get_data(), query_node->get_data_length()))) {
  } else if (query_text.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query should not be empty", K(ret));
  } else if (OB_FAIL(parse_keyword(query_text, query_info))) {
    LOG_WARN("fail to parse query clause", K(ret));
  }
  return ret;
}

int ObESQueryParser::parse_query_string_boost(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObReqConstExpr *boost_expr = nullptr;
  ObIJsonBase *boost_node = nullptr;
  ObString boost_key("boost");
  if (OB_FAIL(req_node.get_object_value(boost_key, boost_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
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
  } else if (OB_FAIL(parse_const(*boost_node, boost_expr, true))) {
    LOG_WARN("fail to parse boost value", K(ret));
  } else if (OB_NOT_NULL(boost_expr) && boost_expr->get_numeric_value() < 0.0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("boost value must be greater than 0", K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(boost_expr) && boost_expr->get_numeric_value() != 1.0) {
    query_info.boost_ = boost_expr->get_numeric_value();
    ObReqOpExpr *boost_mul_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_mul_expr, alloc_, T_OP_MUL, query_info.score_expr_, boost_expr))) {
      LOG_WARN("fail to create boost multiplication expr", K(ret));
    } else {
      query_info.score_expr_ = boost_mul_expr;
    }
  }
  return ret;
}

int ObESQueryParser::parse_multi_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  query_info.query_item_ = QUERY_ITEM_MULTI_MATCH;
  return parse_query_string(req_node, query_info);
}

int ObESQueryParser::parse_query_string(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  uint32_t count = 0;
  if (query_info.query_item_ == QUERY_ITEM_UNKNOWN) {
    query_info.query_item_ = QUERY_ITEM_QUERY_STRING;
  }
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(req_node.json_type()));
  } else if (OB_FALSE_IT(count = req_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("query_string should have at least one element", K(ret));
  } else {
    uint32_t parsed_keys = 0;
    if (OB_SUCC(parse_query_string_type(req_node, query_info))) {
      parsed_keys++;
    } else if (ret == OB_SEARCH_NOT_FOUND) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to parse query_string type", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(parse_query_string_fields(req_node, query_info))) {
        parsed_keys++;
      } else {
        LOG_WARN("fail to parse query_string fields", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(parse_query_string_query(req_node, query_info))) {
        parsed_keys++;
      } else {
        LOG_WARN("fail to parse query_string query", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(parse_query_string_operator(req_node, query_info))) {
        parsed_keys++;
      } else if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to parse query_string operator", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(parse_minimum_should_match(req_node, query_info))) {
        parsed_keys++;
      } else if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to parse minimum_should_match", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_FAIL(parse_query_string_by_type(query_info))) {
      LOG_WARN("fail to parse query_string by type", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(parse_query_string_boost(req_node, query_info))) {
        parsed_keys++;
      } else if (ret == OB_SEARCH_NOT_FOUND) {
        ret = OB_SUCCESS;
      } else {
      LOG_WARN("fail to parse query_string boost", K(ret));
      }
    }

    if (OB_SUCC(ret) && parsed_keys != count) {
      for (uint32_t i = 0; OB_SUCC(ret) && i < count; i++) {
        ObString key;
        if (OB_FAIL(req_node.get_key(i, key))) {
          LOG_WARN("fail to get key", K(ret), K(i));
        } else if (key.case_compare("type") != 0 &&
                   key.case_compare("fields") != 0 &&
                   key.case_compare("query") != 0 &&
                   key.case_compare(query_info.query_item_ == QUERY_ITEM_MULTI_MATCH ? "operator" : "default_operator") != 0 &&
                   key.case_compare("minimum_should_match") != 0 &&
                   key.case_compare("boost") != 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("unsupported key in query_string", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_field(ObIJsonBase &val_node, ObReqColumnExpr *&field)
{
  int ret = OB_SUCCESS;
  ObString field_str;
  if (val_node.json_type() != ObJsonNodeType::J_STRING) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(val_node.json_type()));
  } else if (OB_FALSE_IT(field_str = ObString(val_node.get_data_length(), val_node.get_data()))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("field name is null", K(ret));
  } else {
    char *pure_field_str = static_cast<char *>(alloc_.alloc(field_str.length() + 1));
    int64_t str_len = 0;
    if (OB_ISNULL(pure_field_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to create field(s) expr", K(ret));
    } else {
      for (int64_t i = 0; i < field_str.length(); i++) {
        if (field_str.ptr()[i] != ' ') {
          pure_field_str[str_len++] = field_str.ptr()[i];
        }
      }
      pure_field_str[str_len] = '\0';
    }
    if (OB_SUCC(ret)) {
      ObString expr_name;
      double weight = -1.0;
      const char *caret_ptr = strchr(pure_field_str, '^');
      if (caret_ptr != nullptr && caret_ptr > pure_field_str) {
        int64_t field_len = caret_ptr - pure_field_str;
        expr_name = ObString(field_len, pure_field_str);
        const char *weight_start = caret_ptr + 1;
        if (*weight_start != '\0') {
          char *end_ptr = nullptr;
          weight = strtod(weight_start, &end_ptr);
          if (end_ptr <= weight_start || weight < 0) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid field weight", K(weight));
          }
        }
      } else {
        expr_name = ObString(str_len, pure_field_str);
      }
      if (OB_SUCC(ret) && OB_FAIL(ObReqColumnExpr::construct_column_expr(field, alloc_, expr_name, weight))) {
        LOG_WARN("fail to create field(s) expr", K(ret));
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_keyword(const ObString &query_text, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (query_info.query_item_ == QUERY_ITEM_QUERY_STRING) {
    ret = parse_keyword_query_string(query_text, query_info);
  } else if (query_info.query_item_ == QUERY_ITEM_MULTI_MATCH) {
    ret = parse_keyword_multi_match(query_text, query_info);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unsupported item type", K(ret), K(query_info.query_item_));
  }
  return ret;
}

int ObESQueryParser::parse_keyword_multi_match(const ObString &query_text, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  const char *start = nullptr;
  const char *end = nullptr;
  const char *current = nullptr;
  char *query_str = static_cast<char *>(alloc_.alloc(query_text.length() + 1));
  if (OB_ISNULL(query_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for query copy", K(ret));
  } else {
    MEMCPY(query_str, query_text.ptr(), query_text.length());
    query_str[query_text.length()] = '\0';
    start = query_str;
    end = start + query_text.length();
    current = start;
  }

  common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> raw_keywords;
  while (OB_SUCC(ret) && current < end) {
    while (current < end && (isspace(*current) || ispunct(*current))) {
      current++;
    }
    if (current >= end) {
      break;
    }
    const char *keyword_start = current;
    while (current < end && !isspace(*current) && !ispunct(*current)) {
      current++;
    }
    if (current > keyword_start) {
      int64_t keyword_len = current - keyword_start;
      ObString keyword_str(keyword_len, keyword_start);
      ObReqConstExpr *keyword = nullptr;
      if (OB_FAIL(ObReqConstExpr::construct_const_expr(keyword, alloc_, keyword_str, ObVarcharType))) {
        LOG_WARN("fail to create keyword expr", K(ret));
      } else if (OB_FAIL(raw_keywords.push_back(keyword))) {
        LOG_WARN("fail to add raw keyword", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < raw_keywords.count(); i++) {
    if (OB_FAIL(query_info.keyword_exprs_.push_back(raw_keywords.at(i)))) {
      LOG_WARN("fail to add keyword", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    query_info.query_keywords_ = query_text;
  }
  return ret;
}

int ObESQueryParser::parse_keyword_query_string(const ObString &query_text, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  const char *start = nullptr;
  const char *end = nullptr;
  const char *current = nullptr;
  char *query_str = static_cast<char *>(alloc_.alloc(query_text.length() + 1));
  if (OB_ISNULL(query_str)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for query copy", K(ret));
  } else {
    MEMCPY(query_str, query_text.ptr(), query_text.length());
    query_str[query_text.length()] = '\0';
    start = query_str;
    end = start + query_text.length();
    current = start;
  }

  common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> raw_keywords;
  while (OB_SUCC(ret) && current < end) {
    while (current < end && *current == ' ') {
      current++;
    }
    if (current >= end) {
      break;
    }
    if (*current == '^') {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid keyword in query", K(ret));
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
      if (OB_FAIL(ObReqConstExpr::construct_const_expr(keyword, alloc_, keyword_str, ObVarcharType))) {
        LOG_WARN("fail to create keyword expr", K(ret));
      } else {
        while (current < end && *current == ' ') {
          current++;
        }
        if (current < end && *current == '^') {
          current++;
          if (current >= end || !isdigit(*current)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("weight must follow ^ immediately", K(current < end ? *current : 'E'));
          } else {
            const char *weight_start = current;
            char *end_ptr = nullptr;
            double weight = strtod(current, &end_ptr);
            if (end_ptr > current && weight >= 0) {
              keyword->weight_ = weight;
              current = end_ptr;
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid keyword weight", K(weight));
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
    query_info.is_one_keyword_ = (raw_keywords.count() == 1);
  }

  if (OB_FAIL(ret)) {
  } else if (query_info.score_type_ != SCORE_TYPE_CROSS_FIELDS) {
    common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> current_phrase_keywords;
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_keywords.count(); i++) {
      ObReqConstExpr *current_keyword = raw_keywords.at(i);
      if (current_keyword->weight_ != -1.0) {
        if (OB_FAIL(process_phrase_keywords(current_phrase_keywords, query_info))) {
          LOG_WARN("fail to process phrase keywords", K(ret));
        } else if (OB_FAIL(query_info.keyword_exprs_.push_back(current_keyword))) {
          LOG_WARN("fail to add weighted keyword", K(ret));
        }
      } else if (OB_FAIL(current_phrase_keywords.push_back(current_keyword))) {
        LOG_WARN("fail to add keyword to phrase segment", K(ret));
      } else if (i == raw_keywords.count() - 1 && OB_FAIL(process_phrase_keywords(current_phrase_keywords, query_info))) {
        LOG_WARN("fail to process phrase keywords", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < raw_keywords.count(); i++) {
      if (OB_FAIL(query_info.keyword_exprs_.push_back(raw_keywords.at(i)))) {
        LOG_WARN("fail to add keyword", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    query_info.query_keywords_ = query_text;
  }
  return ret;
}

int ObESQueryParser::parse_keyword_array(ObIJsonBase &val_node, common::ObIArray<ObReqConstExpr *> &value_items)
{
  int ret = OB_SUCCESS;
  uint64_t count = 0;
  if (val_node.json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(val_node.json_type()));
  } else if (FALSE_IT(count = val_node.element_count())) {
  } else if (count == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keyword array should have at least one element", K(ret));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObIJsonBase *value_node = NULL;
      ObReqConstExpr *value_expr = NULL;
      if (OB_FAIL(val_node.get_array_element(i, value_node))) {
        LOG_WARN("fail to get value.", K(ret));
      } else if (OB_FAIL(parse_const(*value_node, value_expr))) {
        LOG_WARN("fail to parse const value", K(ret));
      } else if (OB_FAIL(value_items.push_back(value_expr))) {
        LOG_WARN("fail to add value to value_items", K(ret));
      }
    }
  }
  return ret;
}

//TODO: remove cover_value_to_str
int ObESQueryParser::parse_const(ObIJsonBase &val_node, ObReqConstExpr *&var, const bool accept_numeric_string/*= false*/, const bool cover_value_to_str/*= false*/)
{
  int ret = OB_SUCCESS;
  ObJsonBuffer j_buffer(&alloc_);
  if (!cover_value_to_str &&
    (val_node.json_type() == ObJsonNodeType::J_ARRAY || val_node.json_type() == ObJsonNodeType::J_OBJECT)) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("unexpectd json type", K(ret), K(val_node.json_type()));
  } else if (OB_FAIL(val_node.print(j_buffer, false))) {
    LOG_WARN("fail to create const expr", K(ret));
  } else {
    ObString expr_name;
    j_buffer.get_result_string(expr_name);
    bool is_numeric_value = val_node.is_json_number(val_node.json_type());
    if (accept_numeric_string || is_numeric_value) {
      if (accept_numeric_string) {
        expr_name = expr_name.trim();
      }
      int64_t str_len = expr_name.length();
      char *temp_str = static_cast<char *>(alloc_.alloc(str_len + 1));
      if (OB_ISNULL(temp_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for temp string", K(ret));
      } else {
        MEMCPY(temp_str, expr_name.ptr(), str_len);
        temp_str[str_len] = '\0';
        char *end_ptr = nullptr;
        double num_value = strtod(temp_str, &end_ptr);
        if (end_ptr == temp_str || end_ptr != temp_str + str_len) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid numeric string", K(expr_name));
        } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(var, alloc_, num_value, ObNumberType))) {
          LOG_WARN("fail to create numeric const expr", K(ret));
        }
      }
    } else if (OB_FAIL(ObReqConstExpr::construct_const_expr(var, alloc_, expr_name, ObVarcharType))) {
      LOG_WARN("fail to create const expr", K(ret));
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
      if (OB_FAIL(parse_const(*sub_node, K, true))) {
        LOG_WARN("fail to parse k constant", K(ret), K(i));
      } else if (OB_FAIL(required_params.erase_refactored("k"))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("query_vector") == 0) {
      if (OB_FAIL(parse_const(*sub_node, query_vec, false, true))) {
        LOG_WARN("fail to parse query vector", K(ret), K(i));
      } else if (OB_FAIL(required_params.erase_refactored("query_vector"))) {
        LOG_WARN("fail to erase set", K(ret));
      }
    } else if (key.case_compare("boost") == 0) {
      if (OB_FAIL(parse_const(*sub_node, boost, true))) {
        LOG_WARN("fail to parse boost clauses", K(ret), K(i));
      } else if (boost->get_numeric_value() < 0.0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("boost value must be greater than 0", K(ret));
      }
    } else if (key.case_compare("similarity") == 0 ) {
      if (OB_FAIL(parse_const(*sub_node, similar, true))) {
        LOG_WARN("fail to parse similarity clauses", K(ret), K(i));
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
  ObReqCaseWhenExpr *case_when_expr = NULL;
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
  } else if (filter_expr != NULL && OB_FAIL(query_req->condition_items_.push_back(filter_expr))) {
    LOG_WARN("fail to push query item", K(ret));
  } else if (OB_FAIL(parse_basic_table(table_name_, query_req))) {
    LOG_WARN("fail to parse basic table", K(ret));
  } else if (OB_FAIL(get_distance_algor_type(*vec_field, alg_type))) {
    LOG_WARN("fail to get distance algo type", K(ret));
  } else if (alg_type == ObVectorIndexDistAlgorithm::VIDA_IP) {
    if (OB_FAIL(construct_ip_expr(vec_field, query_vec, case_when_expr, add_expr, order_by_4ip))) {
      LOG_WARN("fail to construct ip expr", K(ret));
    } else if (OB_FAIL(construct_order_by_item(order_by_4ip, true, order_info))) {
      LOG_WARN("fail to construct order by item", K(ret));
    } else if (OB_FAIL(query_req->select_items_.push_back(add_expr))) {
      LOG_WARN("fail to push query item", K(ret));
    }
  } else {
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
    }
  }

  if (OB_SUCC(ret)) {
    query_req->set_vec_approx();
    query_req->set_limit(K);
    ObReqExpr *score = (alg_type == ObVectorIndexDistAlgorithm::VIDA_IP) ?
      static_cast<ObReqExpr *>(case_when_expr) : static_cast<ObReqExpr *>(div_expr);
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(round_const, alloc_, 8.0, ObIntType))) {
      LOG_WARN("fail to create const expr", K(ret));
    } else if (OB_FAIL(ObReqExpr::construct_expr(normalize_expr, alloc_, "round", score, round_const))) {
      LOG_WARN("fail to create round expr", K(ret));
    } else if (OB_FAIL(query_req->order_items_.push_back(order_info))) {
      LOG_WARN("fail to push query order item", K(ret));
    } else if (boost != NULL) {
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(boost_expr, alloc_, T_OP_MUL, normalize_expr, boost))) {
        LOG_WARN("fail to create boost expr", K(ret));
      } else if (OB_FAIL(query_req->add_score_item(alloc_, boost_expr))) {
        LOG_WARN("failed add term to score items", K(ret));
      }
    } else {
      if (OB_FAIL(query_req->add_score_item(alloc_, normalize_expr))) {
        LOG_WARN("fail to push query item", K(ret));
      }
    }
    if (OB_SUCC(ret) && similar != NULL) {
      ObReqExpr *dist = (alg_type == ObVectorIndexDistAlgorithm::VIDA_IP) ? add_expr : dist_vec;
      if (OB_FAIL(construct_query_with_similarity(alg_type, dist, similar, query_req))) {
        LOG_WARN("fail to construct query with similarity", K(ret));
      }
    }
  }

  return ret;
}

// distance : 0 - negative_inner_product(vec_field, query_vec)
// score : case distance < 0 then 1 / (1 - 1 * distance)  else 1 + distance end
// equals
// score : case when negative_inner_product(vec_field, query_vec) > 0 then 1 / (1 + negative_inner_product) else 1 - negative_inner_product end
int ObESQueryParser::construct_ip_expr(ObReqColumnExpr *vec_field, ObReqConstExpr *query_vec, ObReqCaseWhenExpr *&case_when/* score */,
                                       ObReqOpExpr *&minus_expr/* distance */, ObReqExpr *&order_by_vec)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *negative_expr = NULL;
  ObReqOpExpr *negative_score_expr = NULL;
  ObReqConstExpr *one_const = NULL;
  ObReqConstExpr *zero_const = NULL;
  ObReqOpExpr *cmp_expr = NULL;
  ObReqOpExpr *add_expr = NULL;
  if (OB_FAIL(ObReqExpr::construct_expr(order_by_vec, alloc_, N_VECTOR_NEGATIVE_INNER_PRODUCT, vec_field, query_vec))) {
    LOG_WARN("fail to create distance expr", K(ret));
  } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(one_const, alloc_, 1.0, ObIntType))) {
    LOG_WARN("fail to create one const expr", K(ret));
  } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(zero_const, alloc_, 0.0, ObIntType))) {
    LOG_WARN("fail to create zero const expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc_, T_OP_ADD, one_const, order_by_vec))) {
    LOG_WARN("fail to create add expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(minus_expr, alloc_, T_OP_MINUS, zero_const, order_by_vec, "_distance"))) {
    LOG_WARN("fail to create minus expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(cmp_expr, alloc_, T_OP_GT, order_by_vec, zero_const))) {
    LOG_WARN("fail to create cmp expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(negative_score_expr, alloc_, T_OP_DIV, one_const, add_expr))) {
    LOG_WARN("fail to create negative score expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(negative_expr, alloc_, T_OP_MINUS, one_const, order_by_vec))) {
    LOG_WARN("fail to create negative expr", K(ret));
  } else if (OB_FAIL(ObReqCaseWhenExpr::construct_case_when_expr(case_when, alloc_, cmp_expr, negative_score_expr, negative_expr))) {
    LOG_WARN("fail to create case when expr", K(ret));
  }
  return ret;
}

int ObESQueryParser::set_fts_limit_expr(ObQueryReqFromJson *query, const ObReqConstExpr *size_expr, const ObReqConstExpr *from_expr)
{
  // add limit for fts query
  int ret = OB_SUCCESS;
  const int64_t FTS_LIMIT_FACTOR = 20;
  int64_t size_val = 0;
  int64_t from_val = 0;
  char *buf = NULL;
  if (fusion_config_.size != NULL) {
    if (OB_FAIL(convert_const_numeric(fusion_config_.size->expr_name, size_val))) {
      LOG_WARN("fail to convert size expr", K(ret));
    }
  } else if (OB_ISNULL(size_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(convert_const_numeric(size_expr->expr_name, size_val))) {
    LOG_WARN("fail to convert size expr", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (from_expr != NULL && OB_FAIL(convert_const_numeric(from_expr->expr_name, from_val))) {
    LOG_WARN("fail to convert from expr", K(ret));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(alloc_.alloc(ObFastFormatInt::MAX_DIGITS10_STR_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    int64_t limit_val = (size_val + from_val) * FTS_LIMIT_FACTOR;
    ObReqConstExpr *fts_limit_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(fts_limit_expr, alloc_, limit_val, ObIntType))) {
      LOG_WARN("fail to create fts limit expr", K(ret));
    } else {
      query->set_limit(fts_limit_expr);
    }
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

int ObESQueryParser::set_distance_score_expr(const ObVectorIndexDistAlgorithm alg_type, ObReqConstExpr *&norm_const, ObReqExpr *&dist_vec,
                                             ObReqOpExpr *&add_expr, ObReqOpExpr *&score_expr)
{
  int ret = OB_SUCCESS;
  switch (alg_type) {
    case ObVectorIndexDistAlgorithm::VIDA_L2 : {
      // l2_distance : score = 1 / (1 + l2_distance)
      if (OB_FAIL(ObReqExpr::construct_expr(dist_vec, alloc_, N_VECTOR_L2_DISTANCE, "_distance"))) {
        LOG_WARN("fail to create distance expr", K(ret));
      } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(norm_const, alloc_, 1.0, ObIntType))) {
        LOG_WARN("fail to create const expr", K(ret));
      } else {
        if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(add_expr, alloc_, T_OP_ADD, norm_const, dist_vec))) {
          LOG_WARN("fail to create add expr", K(ret));
        } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(score_expr, alloc_, T_OP_DIV, norm_const, add_expr))) {
          LOG_WARN("fail to create div expr", K(ret));
        }
      }
      break;
    }
    case ObVectorIndexDistAlgorithm::VIDA_COS : {
      if (OB_FAIL(ObReqExpr::construct_expr(dist_vec, alloc_, N_VECTOR_COS_DISTANCE, "_distance"))) {
        LOG_WARN("fail to create distance expr", K(ret));
      } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(norm_const, alloc_, 2.0, ObIntType))) {
        LOG_WARN("fail to create const expr", K(ret));
      } else {
        ObReqOpExpr *minus_expr = NULL;
        ObReqConstExpr *const_minus = NULL;
        if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(const_minus, alloc_, 1.0, ObIntType))) {
          LOG_WARN("fail to create op expr", K(ret));
        } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(score_expr, alloc_, T_OP_DIV, dist_vec, norm_const))) {
          LOG_WARN("fail to create div expr", K(ret));
        } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(minus_expr, alloc_, T_OP_MINUS, const_minus, score_expr))) {
          LOG_WARN("fail to create minus expr", K(ret));
        } else {
          // cos_distance : score = 1 - (cos_distance / 2)
          score_expr = minus_expr;
        }
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

int ObESQueryParser::construct_score_sum_expr(ObReqExpr *fts_score, ObReqExpr *vs_score, ObString &score_alias, ObReqOpExpr *&score)
{
  int ret = OB_SUCCESS;
  ObReqExpr *if_null_fts = NULL;
  ObReqExpr *if_null_vs = NULL;
  ObReqConstExpr *zero_const = NULL;
  if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(zero_const, alloc_, 0.0, ObIntType))) {
    LOG_WARN("fail to create constant expr", K(ret));
  } else if (OB_FAIL(ObReqExpr::construct_expr(if_null_fts, alloc_, N_IFNULL, fts_score, zero_const))) {
    LOG_WARN("fail to create if null expr", K(ret));
  } else if (OB_FAIL(ObReqExpr::construct_expr(if_null_vs, alloc_, N_IFNULL, vs_score, zero_const))) {
    LOG_WARN("fail to create if null expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(score, alloc_, T_OP_ADD, if_null_fts, if_null_vs, score_alias))) {
    LOG_WARN("fail to construct op expr", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_sub_query_table(ObString &sub_query_name, ObQueryReqFromJson *query_req, ObReqTable *&sub_query)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_query = OB_NEWx(ObReqTable, &alloc_, SUB_QUERY, sub_query_name, database_name_, query_req))) {
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

int ObESQueryParser::construct_query_with_similarity(ObVectorIndexDistAlgorithm algor, ObReqExpr *dist, ObReqConstExpr *similar, ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *cmp_expr = NULL;
  ObReqColumnExpr *col = NULL;
  ObString sub_query_name("_vs0");
  ObItemType op_type = (algor == ObVectorIndexDistAlgorithm::VIDA_IP) ? T_OP_GE : T_OP_LE;
  ObString col_name = dist->alias_name.empty() ? dist->expr_name : dist->alias_name;
  if (OB_FAIL(wrap_sub_query(sub_query_name, query_req))) {
    LOG_WARN("fail to wrap sub query", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col, alloc_, col_name, sub_query_name))) {
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(cmp_expr, alloc_, op_type, col, similar))) {
    LOG_WARN("fail to create le expr", K(ret));
  } else if (OB_FAIL(query_req->condition_items_.push_back(cmp_expr))) {
    LOG_WARN("fail to push back sub query", K(ret));
  } else {
    col->table_name = sub_query_name;
    col->expr_name = dist->alias_name.empty() ? dist->expr_name : dist->alias_name;
  }
  return ret;
}

int ObESQueryParser::construct_sub_query_with_minimum_should_match(ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *and_expr = nullptr;
  ObString sub_query_name("_fts0");
  ObReqColumnExpr *col_expr = nullptr;
  if (OB_FAIL(ObReqOpExpr::construct_op_expr(and_expr, alloc_, T_OP_AND, outer_filter_items_))) {
    LOG_WARN("fail to construct and expr", K(ret));
  } else if (OB_FAIL(wrap_sub_query(sub_query_name, query_req))) {
    LOG_WARN("fail to wrap sub query", K(ret));
  } else if (OB_FAIL(query_req->condition_items_.push_back(and_expr))) {
    LOG_WARN("fail to push back and expr", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, SCORE_NAME, sub_query_name))) {
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_FAIL(query_req->score_items_.push_back(col_expr))) {
    LOG_WARN("fail to push back column expr", K(ret));
  } else {
    col_expr->alias_name = SCORE_NAME;
  }
  return ret;
}

int ObESQueryParser::construct_minimum_should_match_info(ObIJsonBase &req_node, BoolQueryMinShouldMatchInfo &bq_msm_info)
{
  int ret = OB_SUCCESS;
  bool has_must = false;
  bool has_filter = false;
  int64_t term_cnt = 0;
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
        bq_msm_info.has_msm_ = true;
      } else if (key.case_compare("must") == 0) {
        has_must = true;
      } else if (key.case_compare("filter") == 0) {
        has_filter = true;
      } else if (key.case_compare("should") == 0) {
        if (sub_node->json_type() == ObJsonNodeType::J_ARRAY) {
          term_cnt = sub_node->element_count();
        } else if (sub_node->json_type() == ObJsonNodeType::J_OBJECT) {
          term_cnt = 1;
        } else {
          ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
          LOG_WARN("should should be array or object", K(ret), K(sub_node->json_type()));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(parse_minimum_should_match_with_bool_query(req_node, term_cnt, bq_msm_info))) {
    LOG_WARN("fail to parse minimum should match", K(ret));
  } else if (OB_ISNULL(bq_msm_info.msm_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("minimum_should_match is null", K(ret));
  } else if (bq_msm_info.has_msm_ && bq_msm_info.msm_val_ > 0 && term_cnt == 0) {
    ObReqConstExpr *const_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(const_expr, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to create query request", K(ret));
    } else {
      bq_msm_info.or_expr_ = const_expr;
    }
  } else if (bq_msm_info.msm_val_ == 0) {
    if (has_must || has_filter) {
      bq_msm_info.has_where_condition_ = false;
      if (OB_FAIL(bq_msm_info.msm_expr_->set_numeric(alloc_, 0.0, ObIntType))) {
        LOG_WARN("fail to set numeric properties for minimum_should_match", K(ret));
      }
    } else {
      bq_msm_info.msm_val_ = 1;
      if (OB_FAIL(bq_msm_info.msm_expr_->set_numeric(alloc_, 1.0, ObIntType))) {
        LOG_WARN("fail to set numeric properties for minimum_should_match", K(ret));
      }
    }
  }
  return ret;
}

int ObESQueryParser::parse_minimum_should_match_by_value(const common::ObString &val_str, const int64_t term_cnt, uint64_t &msm_val)
{
  int ret = OB_SUCCESS;
  int64_t val = 0;
  ObString num_part = val_str.trim();
  uint32_t num_part_len = num_part.length();
  bool is_percentage = false;

  if (num_part.length() > 0 && num_part.ptr()[num_part.length() - 1] == '%') {
    num_part.assign_ptr(num_part.ptr(), static_cast<int32_t>(num_part.length() - 1));
    is_percentage = true;
  }

  if (!num_part.is_numeric()) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("string value is empty or not numeric", K(ret), K(num_part));
  } else if (OB_FAIL(convert_signed_const_numeric(num_part, val))) {
    LOG_WARN("failed to convert string value to number", K(ret), K(num_part));
  } else if (is_percentage) {
    val = (term_cnt * val) / 100;
  }

  if (OB_FAIL(ret)) {
  } else if (val < 0) {
    int64_t new_val = term_cnt + val;
    if (new_val < 0) {
      msm_val = 0;
    } else {
      msm_val = static_cast<uint64_t>(new_val);
    }
  } else {
    msm_val = static_cast<uint64_t>(val);
  }

  return ret;
}

int ObESQueryParser::parse_minimum_should_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  uint64_t msm_val = 0;
  uint64_t kwd_cnt = query_info.keyword_exprs_.count();
  ObString msm_str("minimum_should_match");
  ObIJsonBase *msm_node = NULL;
  if (kwd_cnt == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("key word count of query_string is 0", K(ret), K(kwd_cnt));
  } else if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("minimum_should_match should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(req_node.get_object_value(msm_str, msm_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      LOG_DEBUG("minimum_should_match is not found", K(ret));
    } else {
      LOG_WARN("fail to get minimum should match node", K(ret));
    }
  } else if (query_info.operator_ == T_OP_AND) {
    LOG_INFO("minimum_should_match is not supported for AND operator, ignore it", K(ret));
  } else if (OB_FAIL(parse_const(*msm_node, query_info.qs_msm_info_.msm_expr_))) {
    LOG_WARN("fail to parse minimum should match expr", K(ret));
  } else if (OB_FAIL(parse_minimum_should_match_by_value(query_info.qs_msm_info_.msm_expr_->expr_name, kwd_cnt, msm_val))) {
    LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(query_info.qs_msm_info_.msm_expr_->expr_name));
  }

  if (OB_FAIL(ret)) {
  } else if (msm_val == 0) {
    query_info.qs_msm_info_.msm_val_ = 1;
    if (query_info.qs_msm_info_.msm_expr_ != NULL) {
      if (OB_FAIL(query_info.qs_msm_info_.msm_expr_->set_numeric(alloc_, 1.0, ObIntType))) {
        LOG_WARN("fail to set numeric properties for minimum_should_match", K(ret));
      }
    } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(query_info.qs_msm_info_.msm_expr_, alloc_, 1.0, ObIntType))) {
      LOG_WARN("fail to allocate memory for minimum should match expr", K(ret));
    }
  } else if (msm_val > 0) {
    query_info.qs_msm_info_.msm_val_ = msm_val;
    if (OB_FAIL(query_info.qs_msm_info_.msm_expr_->set_numeric(alloc_, msm_val, ObIntType))) {
      LOG_WARN("fail to set numeric properties for minimum_should_match", K(ret), K(msm_val));
    }
  }
  return ret;
}

int ObESQueryParser::parse_minimum_should_match_with_bool_query(ObIJsonBase &req_node, const int64_t term_cnt, BoolQueryMinShouldMatchInfo &bq_msm_info)
{
  int ret = OB_SUCCESS;
  uint64_t msm_val = 0;
  ObString msm_str("minimum_should_match");
  ObIJsonBase *msm_node = NULL;
  if (req_node.json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_INVALID_TYPE_FOR_ARGUMENT;
    LOG_WARN("minimum_should_match should be object", K(ret), K(req_node.json_type()));
  } else if (OB_FAIL(req_node.get_object_value(msm_str, msm_node))) {
    if (ret == OB_SEARCH_NOT_FOUND) {
      bq_msm_info.msm_expr_ = NULL;
      bq_msm_info.has_msm_ = false;
      ret = OB_SUCCESS;
      LOG_DEBUG("minimum_should_match is not found", K(ret));
    } else {
      LOG_WARN("fail to get minimum should match node", K(ret));
    }
  } else if (OB_FAIL(parse_const(*msm_node, bq_msm_info.msm_expr_))) {
    LOG_WARN("fail to parse minimum should match expr", K(ret));
  } else if (OB_FAIL(parse_minimum_should_match_by_value(bq_msm_info.msm_expr_->expr_name, term_cnt, msm_val))) {
    LOG_WARN("failed to convert minimum_should_match to number", K(ret), K(bq_msm_info.msm_expr_->expr_name));
  }

  if (OB_FAIL(ret)) {
  } else if (msm_val == 0) {
    bq_msm_info.msm_val_ = 0;
    if (bq_msm_info.msm_expr_ != NULL) {
      if (OB_FAIL(bq_msm_info.msm_expr_->set_numeric(alloc_, 0.0, ObIntType))) {
        LOG_WARN("fail to set numeric properties for minimum_should_match", K(ret));
      }
    } else if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(bq_msm_info.msm_expr_, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to allocate memory for minimum should match expr", K(ret));
    }
  } else if (msm_val > 0) {
    bq_msm_info.msm_val_ = msm_val;
    if (OB_FAIL(bq_msm_info.msm_expr_->set_numeric(alloc_, msm_val, ObIntType))) {
      LOG_WARN("fail to set numeric properties for minimum_should_match", K(ret), K(msm_val));
    }
  }
  return ret;
}

int ObESQueryParser::add_score_col(const ObString &table_name, ObQueryReqFromJson &query_req)
{
  int ret = OB_SUCCESS;
  ObReqColumnExpr *score_col = NULL;
  if (OB_FAIL(ObReqColumnExpr::construct_column_expr(score_col, alloc_, SCORE_NAME, table_name))) {
    LOG_WARN("fail to create column expr", K(ret));
  } else if (OB_FAIL(query_req.add_score_item(alloc_, score_col))) {
    LOG_WARN("fail to add score item", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_condition_best_fields(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (!query_info.is_basic_query_ && check_need_construct_msm_expr(query_info)) {
    if (OB_FAIL(construct_should_group_expr_for_query_string(query_info))) {
      LOG_WARN("fail to construct should group expr for query string", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.field_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_conditions;
      ObReqColumnExpr *field_expr = query_info.field_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.keyword_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(j);
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, query_info.score_type_))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else if (OB_FAIL(keyword_conditions.push_back(match_expr))) {
          LOG_WARN("fail to add keyword condition", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObReqOpExpr *keyword_expr = nullptr;
        if (OB_FAIL(ObReqOpExpr::construct_op_expr(keyword_expr, alloc_, query_info.operator_, keyword_conditions))) {
          LOG_WARN("fail to create conditions expr", K(ret), K(query_info.operator_));
        } else if (OB_FAIL(conditions.push_back(keyword_expr))) {
          LOG_WARN("fail to add keyword condition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_condition_cross_fields(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  if (query_info.operator_ == T_OP_OR && OB_FAIL(construct_condition_best_fields(query_info, conditions))) {
      LOG_WARN("fail to construct condition for cross_fields + OR", K(ret));
  } else if (query_info.operator_ == T_OP_AND) {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.keyword_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> field_conditions;
      ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.field_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = query_info.field_exprs_.at(j);
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, query_info.score_type_))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else if (OB_FAIL(field_conditions.push_back(match_expr))) {
          LOG_WARN("fail to add field condition", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObReqOpExpr *field_expr = nullptr;
        if (OB_FAIL(ObReqOpExpr::construct_op_expr(field_expr, alloc_, T_OP_OR, field_conditions))) {
          LOG_WARN("fail to create field condition expr", K(ret));
        } else if (OB_FAIL(conditions.push_back(field_expr))) {
          LOG_WARN("fail to add field condition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_condition_most_fields(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions)
{
  return construct_condition_best_fields(query_info, conditions);
}

int ObESQueryParser::construct_condition_phrase(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (!query_info.is_basic_query_ && check_need_construct_msm_expr(query_info)) {
    if (OB_FAIL(construct_should_group_expr_for_query_string(query_info))) {
      LOG_WARN("fail to construct should group expr for query string", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < query_info.field_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> keyword_conditions;
      ObReqColumnExpr *field_expr = query_info.field_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < query_info.keyword_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(j);
        ObEsScoreType score_type = (keyword_expr->weight_ == -1.0) ? SCORE_TYPE_PHRASE : SCORE_TYPE_BEST_FIELDS;
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, score_type))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else if (OB_FAIL(keyword_conditions.push_back(match_expr))) {
          LOG_WARN("fail to add keyword condition", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObReqOpExpr *condition_expr = nullptr;
        if (OB_FAIL(ObReqOpExpr::construct_op_expr(condition_expr, alloc_, query_info.operator_, keyword_conditions))) {
          LOG_WARN("fail to create conditions expr", K(ret));
        } else if (OB_FAIL(conditions.push_back(condition_expr))) {
          LOG_WARN("fail to add condition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_query_string_condition(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (query_info.operator_ != T_OP_AND && query_info.operator_ != T_OP_OR) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("operator between conditions must be AND or OR", K(ret), K(query_info.operator_));
  } else {
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> conditions;
    switch (query_info.score_type_) {
      case SCORE_TYPE_BEST_FIELDS: {
        if (OB_FAIL(construct_condition_best_fields(query_info, conditions))) {
          LOG_WARN("fail to construct condition for best_fields", K(ret));
        }
        break;
      }
      case SCORE_TYPE_MOST_FIELDS: {
        if (OB_FAIL(construct_condition_most_fields(query_info, conditions))) {
          LOG_WARN("fail to construct condition for most_fields", K(ret));
        }
        break;
      }
      case SCORE_TYPE_CROSS_FIELDS: {
        if (OB_FAIL(construct_condition_cross_fields(query_info, conditions))) {
          LOG_WARN("fail to construct condition for cross_fields", K(ret));
        }
        break;
      }
      case SCORE_TYPE_PHRASE: {
        if (OB_FAIL(construct_condition_phrase(query_info, conditions))) {
          LOG_WARN("fail to construct condition for phrase", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported score type", K(ret), K(query_info.score_type_));
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_NOT_NULL(query_info.qs_msm_info_.or_expr_)) {
      query_info.condition_expr_ = query_info.qs_msm_info_.or_expr_;
    } else {
      ObReqOpExpr *final_condition = nullptr;
      ObItemType condition_operator = (query_info.score_type_ == SCORE_TYPE_CROSS_FIELDS && query_info.operator_ == T_OP_AND) ? T_OP_AND : T_OP_OR;
      if (OB_FAIL(ObReqOpExpr::construct_op_expr(final_condition, alloc_, condition_operator, conditions))) {
        LOG_WARN("fail to create where condition expr", K(ret), K(condition_operator));
      } else {
        query_info.condition_expr_ = final_condition;
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_should_group_expr_for_query_string(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> or_group_exprs;
  if (query_info.field_exprs_.count() == 0 || query_info.keyword_exprs_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("field_exprs or keyword_exprs is empty", K(ret));
  } else if (query_info.qs_msm_info_.msm_val_ > query_info.keyword_exprs_.count()) {
    // to improve performance, avoid creating unnecessary conditions in the WHERE clause.
    ObReqConstExpr *tmp_ge_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(tmp_ge_expr, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to create expr", K(ret));
    } else {
      query_info.qs_msm_info_.or_expr_ = tmp_ge_expr;
    }
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < query_info.keyword_exprs_.count(); i++) {
      common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> match_exprs;
      ObReqConstExpr *keyword_expr = query_info.keyword_exprs_.at(i);
      for (uint64_t j = 0; OB_SUCC(ret) && j < query_info.field_exprs_.count(); j++) {
        ObReqMatchExpr *match_expr = nullptr;
        ObReqColumnExpr *field_expr = query_info.field_exprs_.at(j);
        ObEsScoreType score_type = (query_info.score_type_ == SCORE_TYPE_PHRASE) ? SCORE_TYPE_PHRASE : SCORE_TYPE_BEST_FIELDS;
        if (OB_FAIL(ObReqMatchExpr::construct_match_expr(match_expr, alloc_, field_expr, keyword_expr, score_type))) {
          LOG_WARN("fail to create match expr", K(ret));
        } else if (OB_FAIL(match_exprs.push_back(match_expr))) {
          LOG_WARN("fail to add match expr to array", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObReqOpExpr *expr = nullptr;
        if (OB_FAIL(ObReqOpExpr::construct_op_expr(expr, alloc_, T_OP_OR, match_exprs))) {
          LOG_WARN("fail to create expr", K(ret));
        } else if (OB_FAIL(or_group_exprs.push_back(expr))) {
          LOG_WARN("fail to push or group expr", K(ret));
        }
      }
    }
    ObReqExpr *should_condition = nullptr;
    common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> expr_array;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(build_should_condition_combine(0, query_info.qs_msm_info_.msm_val_, or_group_exprs, expr_array, should_condition))) {
      LOG_WARN("fail to build should groups", K(ret));
    } else {
      query_info.qs_msm_info_.or_expr_ = should_condition;
    }
  }
  return ret;
}

int ObESQueryParser::get_base_table_query(ObQueryReqFromJson *query_req, ObQueryReqFromJson *&base_table_req, ReqTableType *table_type/*=nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_req)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(query_req));
  } else if (OB_NOT_NULL(base_table_req)) {
  } else {
    // for sub_query and multi_set, suppose all the sub queries have the same base table
    for (int64_t i = 0; OB_SUCC(ret) && i < query_req->from_items_.count(); i++) {
      ObReqTable *table = query_req->from_items_.at(i);
      if (OB_NOT_NULL(table_type) && *table_type == UNKNOWN_TABLE) {
        OB_ASSERT(table->table_type_ != UNKNOWN_TABLE);
        *table_type = table->table_type_;
      }
      if (table->table_type_ == BASE_TABLE) {
        base_table_req = query_req;
        break;
      } else if (table->table_type_ == SUB_QUERY) {
        ObQueryReqFromJson *query = NULL;
        if (OB_ISNULL(query = dynamic_cast<ObQueryReqFromJson *>(table->ref_query_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(table->ref_query_));
        } else if (OB_FAIL(get_base_table_query(query, base_table_req, table_type))) {
          LOG_WARN("fail to get base table query", K(ret));
        } else if (OB_NOT_NULL(base_table_req)) {
          break;
        }
      } else if (table->table_type_ == MULTI_SET) {
        ObMultiSetTable *multi_set = nullptr;
        ObQueryReqFromJson *query = nullptr;
        if (OB_ISNULL(multi_set = dynamic_cast<ObMultiSetTable *>(table)) || multi_set->sub_queries_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(table));
        } else if (OB_ISNULL(query = dynamic_cast<ObQueryReqFromJson *>(multi_set->sub_queries_.at(0)->ref_query_))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(multi_set->sub_queries_.at(0)));
        } else if (OB_FAIL(get_base_table_query(query, base_table_req, table_type))) {
          LOG_WARN("fail to get base table query", K(ret));
        } else if (OB_NOT_NULL(base_table_req)) {
          break;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(table->table_type_));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(base_table_req)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("base table not found", K(ret));
    }
  }
  return ret;
}

int ObESQueryParser::parse_query_string_by_type(ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  switch (query_info.score_type_) {
    case SCORE_TYPE_BEST_FIELDS: {
      if (OB_FAIL(parse_best_fields(query_info))) {
        LOG_WARN("fail to parse best_fields", K(ret));
      }
      break;
    }
    case SCORE_TYPE_CROSS_FIELDS: {
      if (OB_FAIL(parse_cross_fields(query_info))) {
        LOG_WARN("fail to parse cross_fields", K(ret));
      }
      break;
    }
    case SCORE_TYPE_MOST_FIELDS: {
      if (OB_FAIL(parse_most_fields(query_info))) {
        LOG_WARN("fail to parse most_fields", K(ret));
      }
      break;
    }
    case SCORE_TYPE_PHRASE: {
      if (OB_FAIL(parse_phrase(query_info))) {
        LOG_WARN("fail to parse phrase", K(ret));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unsupported score type", K(ret), K(query_info.score_type_));
    }
  }
  return ret;
}

// build a combination expression as a should condition
int ObESQueryParser::build_should_condition_combine(uint64_t start, uint64_t msm_val, const common::ObIArray<ObReqExpr *> &items, common::ObIArray<ObReqExpr *> &expr_array, ObReqExpr *&should_condition)
{
  int ret = OB_SUCCESS;
  if (msm_val == 0 || items.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (msm_val > items.count()) {
    ObReqConstExpr *tmp_or_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(tmp_or_expr, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to create zero expr", K(ret));
    } else {
      should_condition = tmp_or_expr;
    }
  } else if (msm_val == items.count()) {
    ObReqOpExpr *and_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(and_expr, alloc_, T_OP_AND, items))) {
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      should_condition = and_expr;
    }
  } else if (msm_val == expr_array.count()) {
    ObReqOpExpr *and_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(and_expr, alloc_, T_OP_AND, expr_array))) {
      LOG_WARN("fail to create and expr", K(ret));
    } else if (OB_ISNULL(should_condition)) {
      common::ObSEArray<ObReqExpr*, 1, common::ModulePageAllocator, true> params;
      ObReqOpExpr *tmp_or_expr = nullptr;
      if (OB_FAIL(params.push_back(and_expr))) {
        LOG_WARN("fail to push and_expr to params", K(ret));
      } else if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_or_expr, alloc_, T_OP_OR, params))) {
        LOG_WARN("fail to create or expr", K(ret));
      } else {
        should_condition = tmp_or_expr;
      }
    } else if (OB_FAIL(should_condition->params.push_back(and_expr))) {
      LOG_WARN("fail to push or param", K(ret));
    }
  } else {
    for (uint64_t i = start; OB_SUCC(ret) && i < items.count(); i++) {
      if (OB_FAIL(expr_array.push_back(items.at(i)))) {
        LOG_WARN("fail to push and param", K(ret), K(i));
      } else if (OB_FAIL(build_should_condition_combine(i + 1, msm_val, items, expr_array, should_condition))) {
        LOG_WARN("fail to build deeper groups", K(ret));
      } else {
        expr_array.pop_back();
      }
    }
  }
  return ret;
}

// build a comparison expression as a should condition
int ObESQueryParser::build_should_condition_compare(uint64_t msm_val, ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&should_condition)
{
  int ret = OB_SUCCESS;
  if (msm_val == 0 || items.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(msm_val), K(items.count()));
  } else if (msm_val > items.count()) {
    ObReqConstExpr *tmp_ge_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(tmp_ge_expr, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      should_condition = tmp_ge_expr;
    }
  } else if (msm_val == items.count()) {
    ObReqOpExpr *and_expr = nullptr;
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(and_expr, alloc_, T_OP_AND, items))) {
      LOG_WARN("fail to create and expr", K(ret));
    } else {
      should_condition = and_expr;
    }
  } else {
    ObReqOpExpr *sum_expr = NULL;
    ObReqOpExpr *cmp_expr = NULL;
    ObReqConstExpr *zero_const = NULL;
    common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> params;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(zero_const, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to create zero const", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
      ObReqExpr *group = items.at(i);
      ObReqOpExpr *gt_zero = NULL;
      if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(gt_zero, alloc_, T_OP_GT, group, zero_const))) {
        LOG_WARN("fail to create gt expr", K(ret));
      } else if (OB_FAIL(params.push_back(gt_zero))) {
        LOG_WARN("fail to push gt expr into sum", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObReqOpExpr::construct_op_expr(sum_expr, alloc_, T_OP_ADD, params))) {
      LOG_WARN("fail to create sum expr", K(ret));
    } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(cmp_expr, alloc_, T_OP_GE, sum_expr, msm_expr))) {
      LOG_WARN("fail to create ge expr", K(ret));
    } else {
      should_condition = cmp_expr;
    }
  }
  return ret;
}

int ObESQueryParser::construct_all_query(ObQueryReqFromJson *&query_req)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(query_req = OB_NEWx(ObQueryReqFromJson, &alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to create query request", K(ret));
  } else if (OB_FAIL(parse_basic_table(table_name_, query_req))) {
    LOG_WARN("fail to parse basic table", K(ret));
  } else if (OB_FAIL(set_default_score(query_req, 1.0))) {
    LOG_WARN("fail to set default score", K(ret));
  } else if (OB_FAIL(set_order_by_column(query_req, ROWKEY_NAME, true))) {
    LOG_WARN("fail to set order by rowkey", K(ret));
  }
  return ret;
}

int ObESQueryParser::process_phrase_keywords(common::ObIArray<ObReqConstExpr *> &phrase_keywords, ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  if (!phrase_keywords.empty()) {
    if (phrase_keywords.count() == 1) {
      if (OB_FAIL(query_info.keyword_exprs_.push_back(phrase_keywords.at(0)))) {
        LOG_WARN("fail to add single unweighted keyword", K(ret));
      }
    } else {
      int64_t total_len = phrase_keywords.count() - 1;
      for (int64_t j = 0; j < phrase_keywords.count(); j++) {
        total_len += phrase_keywords.at(j)->expr_name.length();
      }
      char *combined_str = static_cast<char *>(alloc_.alloc(total_len));
      if (OB_ISNULL(combined_str)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for combined keyword", K(ret));
      } else {
        int64_t pos = 0;
        for (int64_t j = 0; j < phrase_keywords.count(); j++) {
          if (j > 0) {
            combined_str[pos++] = ' ';
          }
          MEMCPY(combined_str + pos, phrase_keywords.at(j)->expr_name.ptr(), phrase_keywords.at(j)->expr_name.length());
          pos += phrase_keywords.at(j)->expr_name.length();
        }
        ObString combined_keyword_str(total_len, combined_str);
        ObReqConstExpr *combined_keyword = nullptr;
        if (OB_FAIL(ObReqConstExpr::construct_const_expr(combined_keyword, alloc_, combined_keyword_str, ObVarcharType))) {
          LOG_WARN("fail to create combined keyword expr", K(ret));
        } else if (OB_FAIL(query_info.keyword_exprs_.push_back(combined_keyword))) {
          LOG_WARN("fail to add combined phrase keyword", K(ret));
        }
      }
    }
    phrase_keywords.reset();
  }
  return ret;
}

int ObESQueryParser::check_is_basic_query(ObIJsonBase &req_node, int depth)
{
  int ret = OB_SUCCESS;
  ObString key;
  ObIJsonBase *sub_node = nullptr;

  if (depth <= 1) {
    is_basic_query_ = true;
  }
  if (depth > 1) {
    is_basic_query_ = false;
  } else if (req_node.json_type() == ObJsonNodeType::J_ARRAY) {
    uint64_t count = req_node.element_count();
    for (uint64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      ObIJsonBase *elem = NULL;
      if (OB_FAIL(req_node.get_array_element(i, elem))) {
        LOG_WARN("fail to get array element", K(ret), K(i));
      } else if (elem != NULL && elem->json_type() == ObJsonNodeType::J_OBJECT) {
        ObString elem_key;
        ObIJsonBase *elem_sub_node = NULL;
        if (elem->element_count() == 0) {
          // skip empty object
        } else if (OB_FAIL(elem->get_key(0, elem_key))) {
          LOG_WARN("fail to get key name", K(ret));
        } else if (OB_FAIL(elem->get_object_value(0, elem_sub_node))) {
          LOG_WARN("fail to get sub node", K(ret));
        } else if (check_is_bool_query(elem_key)) {
          if (elem_key == "bool") {
            if (OB_FAIL(check_is_basic_query(*elem_sub_node, depth + 1))) {
              LOG_WARN("fail to check is basic query", K(ret));
            }
          } else {
            if (OB_FAIL(check_is_basic_query(*elem_sub_node, depth))) {
              LOG_WARN("fail to check is basic query", K(ret));
            }
          }
        }
      } else {
        // non-object elements do not affect basic query check
      }
    }
  } else if (req_node.json_type() == ObJsonNodeType::J_OBJECT) {
    for (uint64_t i = 0; OB_SUCC(ret) && i < req_node.element_count(); i++) {
      ObString k;
      ObIJsonBase *sn = nullptr;
      if (OB_FAIL(req_node.get_key(i, k))) {
        LOG_WARN("fail to get key name", K(ret));
      } else if (OB_FAIL(req_node.get_object_value(i, sn))) {
        LOG_WARN("fail to get sub node", K(ret));
      } else if (check_is_bool_query(k)) {
        if (k == "bool") {
          if (OB_FAIL(check_is_basic_query(*sn, depth + 1))) {
            LOG_WARN("fail to check is basic query", K(ret));
          }
        } else {
          if (OB_FAIL(check_is_basic_query(*sn, depth))) {
            LOG_WARN("fail to check is basic query", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_basic_query_filter_condition_with_and_expr()
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *and_expr = NULL;
  if (OB_FAIL(ObReqOpExpr::construct_op_expr(and_expr, alloc_, T_OP_AND, tmp_outer_filter_items_))) {
    LOG_WARN("fail to construct and expr", K(ret));
  } else if (OB_FAIL(outer_filter_items_.push_back(and_expr))) {
    LOG_WARN("fail to push and expr to outer filter items", K(ret));
  }
  tmp_outer_filter_items_.reset();
  return ret;
}

int ObESQueryParser::construct_basic_query_filter_condition_with_or_expr(uint64_t msm_val, ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&or_expr)
{
  int ret = OB_SUCCESS;
  ObReqOpExpr *tmp_or_expr = NULL;
  ObReqOpExpr *ge_expr = NULL;
  ObReqOpExpr *add_expr = NULL;
  // construct or expr, it is used to filter inner query to get the initial result
  // format: or_expr = items[0] OR items[1] OR ... OR items[n-1]
  if (msm_val == 0 || items.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(msm_val), K(items.count()));
  } else if (msm_val > items.count()) {
    ObReqConstExpr *tmp_or_expr = nullptr;
    if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(tmp_or_expr, alloc_, 0.0, ObIntType))) {
      LOG_WARN("fail to create or expr", K(ret));
    } else {
      or_expr = tmp_or_expr;
    }
  } else if (msm_val == items.count()) {
    // format: or_expr = items[0] AND items[1] AND ... AND items[n-1]
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_or_expr, alloc_, T_OP_AND, items))) {
      LOG_WARN("fail to construct or expr", K(ret));
    } else {
      or_expr = tmp_or_expr;
    }
  } else {
    // format: or_expr = items[0] OR items[1] OR ... OR items[n-1]
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(tmp_or_expr, alloc_, T_OP_OR, items))) {
      LOG_WARN("fail to construct or expr", K(ret));
    } else {
      or_expr = tmp_or_expr;
    }
  }

  // construct ge expr, it is used to filter outer query
  // format: (item[0] > 0) + (item[1] > 0) + ... + (item[n-1] > 0) >= msm_val
  if (OB_FAIL(ret)) {
  } else if (tmp_outer_filter_items_.count() > 0) {
    if (tmp_outer_filter_items_.count() == 1) {
      if (OB_FAIL(outer_filter_items_.push_back(tmp_outer_filter_items_.at(0)))) {
        LOG_WARN("fail to push single item to outer filter items", K(ret));
      }
    } else {
      ObReqConstExpr *zero_score_expr = nullptr;
      if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(zero_score_expr, alloc_, 0, ObIntType))) {
        LOG_WARN("fail to create zero score expr", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_outer_filter_items_.count(); i++) {
        ObReqOpExpr *gt_expr = nullptr;
        ObReqExpr *item = tmp_outer_filter_items_.at(i);
        if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(gt_expr, alloc_, T_OP_GT, item, zero_score_expr))) {
          LOG_WARN("fail to create gt expr", K(ret));
        } else {
          tmp_outer_filter_items_.at(i) = gt_expr;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, tmp_outer_filter_items_))) {
        LOG_WARN("fail to construct add expr", K(ret));
      } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(ge_expr, alloc_, T_OP_GE, add_expr, msm_expr))) {
        LOG_WARN("fail to create ge expr", K(ret));
      } else if (OB_FAIL(outer_filter_items_.push_back(ge_expr))) {
        LOG_WARN("fail to push ge expr to outer filter items", K(ret));
      } else {
        tmp_outer_filter_items_.reset();
      }
    }
  }
  return ret;
}

int ObESQueryParser::construct_basic_query_select_items_with_query_string(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &score_items)
{
  int ret = OB_SUCCESS;
  ObReqConstExpr *zero_score_expr = nullptr;
  ObReqOpExpr *ge_expr = nullptr;
  ObReqOpExpr *add_expr = nullptr;
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> params;
  ObQueryReqFromJson *query_req = query_info.query_req_;

  if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(zero_score_expr, alloc_, 0, ObIntType))) {
    LOG_WARN("fail to create zero score expr", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < score_items.count(); i++) {
    ObReqExpr *score_item = score_items.at(i);
    ObString score_str;
    ObReqColumnExpr *col_expr = nullptr;
    ObReqOpExpr *gt_expr = nullptr;
    char* buf = NULL;
    int64_t pos = 0;
    if(OB_ISNULL(buf = static_cast<char *>(alloc_.alloc(OB_MAX_INDEX_PER_TABLE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if(databuff_printf(buf, OB_MAX_INDEX_PER_TABLE, pos, "_word_score_%d", item_seq_) < 0) {
      LOG_WARN("fail to write column name", K(ret));
    } else if (OB_FALSE_IT(item_seq_++)) {
    } else if (OB_FALSE_IT(score_str.assign_ptr(buf, pos))) {
      LOG_WARN("fail to assign column name", K(ret));
    } else if (OB_FAIL(query_req->select_items_.push_back(score_item))) {
      LOG_WARN("fail to push score item to select items", K(ret));
    } else if (OB_FALSE_IT(score_item->set_alias(score_str))) {
    } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, score_str))) {
      LOG_WARN("fail to construct column expr", K(ret));
    } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(gt_expr, alloc_, T_OP_GT, col_expr, zero_score_expr))) {
      LOG_WARN("fail to create gt expr", K(ret));
    } else if (OB_FAIL(params.push_back(gt_expr))) {
      LOG_WARN("fail to push gt expr to params", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, params))) {
      LOG_WARN("fail to create add expr", K(ret));
    } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(ge_expr, alloc_, T_OP_GE, add_expr, query_info.qs_msm_info_.msm_expr_))) {
      LOG_WARN("fail to create ge expr", K(ret));
    } else if (OB_FAIL(tmp_outer_filter_items_.push_back(ge_expr))) {
      LOG_WARN("fail to push ge expr to outer filter items", K(ret));
    } else {
      query_info.basic_query_condition_expr_ = ge_expr;
    }
  }

  return ret;
}

int ObESQueryParser::construct_alias_column_expr_to_select_items(ObQueryReqFromJson &query_req, const ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObReqColumnExpr *col_expr = nullptr;
  ObString wi_score_str;
  char* buf = NULL;
  int64_t pos = 0;
  if(OB_ISNULL(buf = static_cast<char *>(alloc_.alloc(OB_MAX_INDEX_PER_TABLE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if(databuff_printf(buf, OB_MAX_INDEX_PER_TABLE, pos, "_word_score_%d", item_seq_) < 0) {
    LOG_WARN("fail to write column name", K(ret));
  } else if (OB_FALSE_IT(item_seq_++)) {
  } else if (OB_FALSE_IT(wi_score_str.assign_ptr(buf, pos))) {
    LOG_WARN("fail to assign column name", K(ret));
  } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, wi_score_str))) {
    LOG_WARN("fail to construct column expr", K(ret));
  } else if (OB_FAIL(tmp_outer_filter_items_.push_back(col_expr))) {
    LOG_WARN("fail to push column expr to outer filter items", K(ret));
  } else if (OB_FALSE_IT(query_info.condition_expr_->set_alias(wi_score_str))) {
  } else if (OB_FAIL(query_req.select_items_.push_back(query_info.condition_expr_))) {
    LOG_WARN("fail to push column expr to select items", K(ret));
  }
  return ret;
}

int ObESQueryParser::construct_alias_column_expr_to_select_items_with_query_string(ObQueryReqFromJson &query_req, const ObEsQueryInfo &query_info)
{
  int ret = OB_SUCCESS;
  ObReqConstExpr *zero_score_expr = nullptr;
  ObReqOpExpr *outer_gt_expr = nullptr;
  ObReqOpExpr *add_expr = nullptr;
  common::ObSEArray<ObReqExpr*, 4, common::ModulePageAllocator, true> params;
  if (OB_FAIL(ObReqConstExpr::construct_const_numeric_expr(zero_score_expr, alloc_, 0, ObIntType))) {
    LOG_WARN("fail to create zero score expr", K(ret));
  } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(outer_gt_expr, alloc_, T_OP_GT, zero_score_expr, zero_score_expr))) {
    LOG_WARN("fail to create ge expr", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < query_info.basic_query_score_items_.count(); i++) {
    ObReqExpr *score_item = query_info.basic_query_score_items_.at(i);
    ObString score_str;
    ObReqColumnExpr *col_expr = nullptr;
    ObReqOpExpr *gt_expr = nullptr;
    char* buf = NULL;
    int64_t pos = 0;
    if(OB_ISNULL(buf = static_cast<char *>(alloc_.alloc(OB_MAX_INDEX_PER_TABLE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", K(ret));
    } else if(databuff_printf(buf, OB_MAX_INDEX_PER_TABLE, pos, "_word_score_%d", item_seq_) < 0) {
      LOG_WARN("fail to write column name", K(ret));
    } else if (OB_FALSE_IT(item_seq_++)) {
    } else if (OB_FALSE_IT(score_str.assign_ptr(buf, pos))) {
      LOG_WARN("fail to assign column name", K(ret));
    } else if (OB_FAIL(query_req.select_items_.push_back(score_item))) {
      LOG_WARN("fail to push score item to select items", K(ret));
    } else if (OB_FALSE_IT(score_item->set_alias(score_str))) {
    } else if (OB_FAIL(ObReqColumnExpr::construct_column_expr(col_expr, alloc_, score_str))) {
      LOG_WARN("fail to construct column expr", K(ret));
    } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(gt_expr, alloc_, T_OP_GT, col_expr, zero_score_expr))) {
      LOG_WARN("fail to create gt expr", K(ret));
    } else if (OB_FAIL(params.push_back(gt_expr))) {
      LOG_WARN("fail to push gt expr to params", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObReqOpExpr::construct_op_expr(add_expr, alloc_, T_OP_ADD, params))) {
      LOG_WARN("fail to create add expr", K(ret));
    } else if (OB_FAIL(ObReqOpExpr::construct_binary_op_expr(outer_gt_expr, alloc_, T_OP_GT, add_expr, zero_score_expr))) {
      LOG_WARN("fail to create gt expr", K(ret));
    } else if (OB_FAIL(tmp_outer_filter_items_.push_back(outer_gt_expr))) {
      LOG_WARN("fail to push gt expr to outer filter items", K(ret));
    }
  }

  return ret;
}

bool ObESQueryParser::check_need_construct_msm_expr(ObEsQueryInfo &query_info)
{
  bool need_construct_msm_expr = true;

  if (query_info.bq_msm_info_.msm_val_ > 1) {
    need_construct_msm_expr = true;
  } else if (query_info.qs_msm_info_.msm_val_ <= 1) {
    need_construct_msm_expr = false;
  } else if (query_info.operator_ != T_OP_OR) {
    need_construct_msm_expr = false;
  } else if (query_info.score_type_ == SCORE_TYPE_CROSS_FIELDS) {
    if (query_info.keyword_exprs_.count() <= 1) {
      need_construct_msm_expr = false;
    }
  } else if (query_info.score_type_ == SCORE_TYPE_PHRASE) {
    if (query_info.keyword_exprs_.count() <= 1) {
      need_construct_msm_expr = false;
    }
  } else {
    if (query_info.keyword_exprs_.count() == 1) {
      if (query_info.is_one_keyword_) {
        need_construct_msm_expr = false;
      } else if (query_info.field_exprs_.count() > 1) {
        need_construct_msm_expr = false;
      }
    }
  }
  return need_construct_msm_expr;
}
}  // namespace share
}  // namespace oceanbase