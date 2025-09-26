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

#ifndef OCEANBASE_SHARE_OB_QUERY_PARSE_H_
#define OCEANBASE_SHARE_OB_QUERY_PARSE_H_

#include "ob_query_request.h"
#include "lib/json_type/ob_json_parse.h"
#include "share/hybrid_search/ob_request_base.h"
#include "share/schema/ob_schema_struct.h"
#include "share/vector_index/ob_vector_index_util.h"


namespace oceanbase
{
namespace share
{

enum ObRankFeatureType
{
  SATURATION = 0,
  SIGMOID,
  LINEAR,
  LOG
};

class ObRankFeatDef {
public :
  ObRankFeatDef() : type(SATURATION), number_field(nullptr), pivot(nullptr),
                    exponent(nullptr), positive_impact(true) {}
  virtual ~ObRankFeatDef() {}
  ObRankFeatureType type;
  ObReqColumnExpr *number_field;
  ObReqConstExpr *pivot;
  ObReqConstExpr *exponent;
  ObReqConstExpr *scaling_factor;
  bool positive_impact;
};

struct BoolQueryMinShouldMatchInfo {
  BoolQueryMinShouldMatchInfo() : has_minimum_should_match_(false), has_where_condition_(true), ge_expr_(nullptr) {}
  virtual ~BoolQueryMinShouldMatchInfo() {}
  bool has_minimum_should_match_;
  bool has_where_condition_;
  ObReqConstExpr *minimum_should_match_;
  uint64_t msm_count_;
  ObReqExpr *ge_expr_;
  TO_STRING_KV(K(has_minimum_should_match_), K(has_where_condition_), K(minimum_should_match_), K(msm_count_), K(ge_expr_));
};

struct QueryStringMinShouldMatchInfo {
  QueryStringMinShouldMatchInfo() : minimum_should_match_(nullptr), msm_count_(0), ge_expr_(nullptr) {}
  virtual ~QueryStringMinShouldMatchInfo() {}
  ObReqConstExpr *minimum_should_match_;
  uint64_t msm_count_;
  ObReqExpr *ge_expr_;
  TO_STRING_KV(K(minimum_should_match_), K(msm_count_), K(ge_expr_));
};

class ObColumnIndexInfo
{
public :
  ObColumnIndexInfo() : index_name_(), index_type_(schema::ObIndexType::INDEX_TYPE_IS_NOT), dist_algorithm_(ObVectorIndexDistAlgorithm::VIDA_L2) {}
  virtual ~ObColumnIndexInfo() {}
  common::ObString index_name_;
  schema::ObIndexType index_type_;
  ObVectorIndexDistAlgorithm dist_algorithm_;
};

typedef common::hash::ObHashMap<common::ObString, ObColumnIndexInfo*> ColumnIndexNameMap;
typedef common::hash::ObHashSet<common::ObString> RequiredParamsSet;
class ObESQueryParser
{
public :
  ObESQueryParser(ObIAllocator &alloc, common::ObString *table_name) : alloc_(alloc), source_cols_(),
    need_json_wrap_(false), table_name_(*table_name), user_cols_(), out_cols_(nullptr) {}
  ObESQueryParser(ObIAllocator &alloc, bool need_json_wrap,
                  const common::ObString *table_name,
                  const common::ObString *database_name = nullptr)
    : alloc_(alloc), source_cols_(), need_json_wrap_(need_json_wrap), table_name_(*table_name),
      database_name_(*database_name), user_cols_(), out_cols_(nullptr) {}
  virtual ~ObESQueryParser() {}
  int parse(const common::ObString &req_str, ObQueryReqFromJson *&query_req);
  inline ColumnIndexNameMap &get_index_name_map() { return index_name_map_; }
  inline ObIArray<ObString> &get_user_column_names() { return user_cols_; }
private :
  int parse_query(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int parse_bool_query(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&where_condition, ObReqExpr *&score_expr, bool need_cal_score = true);
  int parse_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int parse_source(ObIJsonBase &req_node);
  int parse_must_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&where_condition, common::ObIArray<ObReqExpr *> &score_items, bool need_cal_score = true);
  int parse_must_not_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&where_condition);
  int parse_should_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&where_condition, common::ObIArray<ObReqExpr *> &score_items,
                           BoolQueryMinShouldMatchInfo &bq_min_should_match_info, bool need_cal_score = true);
  int parse_filter_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&where_condition);
  int parse_single_term(ObIJsonBase &req_node, ObQueryReqFromJson &query_req, ObReqExpr *&score_expr, ObReqExpr *&where_condition);
  int parse_match_expr(ObIJsonBase &req_node, ObQueryReqFromJson &query_req, ObReqExpr *&score_expr, ObReqExpr *&where_condition);
  int parse_term_expr(ObIJsonBase &req_node, ObReqExpr *&score_expr, ObReqExpr *&where_condition);
  int parse_query_string_expr(ObIJsonBase &req_node, ObReqExpr *&query_string_expr, ObReqExpr *&where_condition);
  int parse_query_string_boost(ObIJsonBase &req_node, ObReqExpr *&expr);
  int parse_query_string_fields(ObIJsonBase &req_node, common::ObIArray<ObReqColumnExpr *> &field_exprs);
  int parse_query_string_default_operator(ObIJsonBase &req_node, ObItemType &opr);
  int parse_query_string_query(ObIJsonBase &req_node, common::ObIArray<ObReqConstExpr *> &keyword_exprs, ObReqScoreType score_type);
  int parse_query_string_type(ObIJsonBase &req_node, ObReqScoreType &score_type);
  int parse_query_string_by_type(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                 const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                 const ObReqScoreType score_type, const ObItemType opr,
                                 ObReqExpr *&result_expr, ObReqExpr *&where_condition,
                                 QueryStringMinShouldMatchInfo &qs_min_should_match_info);
  int parse_minimum_should_match_with_percentage(const common::ObString &percent_str, const int64_t term_count, uint64_t &msm_count);
  int parse_minimum_should_match_with_integer(const common::ObString &val, const bool is_percentage_value, const int64_t term_count, uint64_t &msm_count);
  int parse_rank_feature(ObIJsonBase &req_node, ObReqExpr *&rank_feat, ObReqExpr *&where_condition);
  int parse_rank_feat_param(ObIJsonBase &req_node, const ObString &para1, const ObString &para2,
                            ObReqConstExpr *&const_para1, ObReqConstExpr *&const_para2, bool &positive);
  int parse_basic_table(const ObString &table_name, ObQueryReqFromJson *query_req);
  int parse_field(ObIJsonBase &val_node, ObReqColumnExpr *&field);
  int parse_keyword(const ObString &query_text, common::ObIArray<ObReqConstExpr *> &keywords, ObReqScoreType score_type);
  int parse_minimum_should_match_with_query_string(ObIJsonBase &req_node, const int64_t key_counts, QueryStringMinShouldMatchInfo &qs_min_should_match_info);
  int parse_minimum_should_match_with_bool_query(ObIJsonBase &req_node, const int64_t should_counts, BoolQueryMinShouldMatchInfo &bq_min_should_match_info);
  int parse_best_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                        const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                        ObReqExpr *&result_expr, ObReqExpr *&where_condition, QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr = T_OP_OR);
  int parse_cross_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                         const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                         ObReqExpr *&result_expr, ObReqExpr *&where_condition, QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr = T_OP_OR);
  int parse_most_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                        const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                        ObReqExpr *&result_expr, ObReqExpr *&where_condition, QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr = T_OP_OR);
  int parse_phrase(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                   const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                   ObReqExpr *&result_expr, ObReqExpr *&where_condition, QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr = T_OP_OR);
  int construct_query_string_condition(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                       const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                       ObReqScoreType score_type, ObReqExpr *&where_condition,
                                       QueryStringMinShouldMatchInfo &qs_min_should_match_info, const ObItemType opr = T_OP_OR);
  int construct_condition_best_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                      const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                      common::ObIArray<ObReqExpr *> &conditions,
                                      QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                      ObReqScoreType score_type, const ObItemType opr = T_OP_OR);
  int construct_condition_most_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                      const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                      common::ObIArray<ObReqExpr *> &conditions,
                                      QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                      ObReqScoreType score_type, const ObItemType opr = T_OP_OR);
  int construct_condition_cross_fields(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                       const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                       common::ObIArray<ObReqExpr *> &conditions,
                                       QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                       ObReqScoreType score_type, const ObItemType opr = T_OP_OR);
  int construct_condition_phrase(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                 const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                 common::ObIArray<ObReqExpr *> &conditions,
                                 QueryStringMinShouldMatchInfo &qs_min_should_match_info,
                                 ObReqScoreType score_type, const ObItemType opr = T_OP_OR);
  int construct_match_expr(ObIJsonBase &val_node, ObQueryReqFromJson &query_req, ObReqColumnExpr *col_expr, ObReqMatchExpr *&match_expr);
  int construct_boost_expr(ObReqExpr *expr, ObReqConstExpr *boost_value, ObReqOpExpr *&boost_mul_expr);
  int construct_all_query(ObQueryReqFromJson *&query_req);
  int parse_const(ObIJsonBase &val_node, ObReqConstExpr *&var, bool is_numeric = false, bool cover_value_to_str = false);
  int wrap_sub_query(ObString &sub_query_name, ObQueryReqFromJson *&query_req);
  int wrap_json_result(ObQueryReqFromJson *&query_res);
  int construct_query_with_similarity(ObVectorIndexDistAlgorithm algor, ObReqExpr *dist, ObReqConstExpr *similar, ObQueryReqFromJson *&query_req);
  int construct_query_with_minnum_should_match(ObReqExpr *score, ObReqConstExpr *minnum_should_match, ObQueryReqFromJson *&query_req);
  int construct_op_expr(ObReqExpr *l_param, ObReqExpr *r_param, ObItemType type, ObReqOpExpr *&cmp_expr, bool need_parentheses = true);
  int construct_op_expr(common::ObIArray<ObReqExpr *> &expr_items, const ObItemType op, ObReqExpr *&expr, bool need_parentheses = true);
  int parse_range_condition(ObIJsonBase &req_node, ObReqExpr *&expr, ObReqExpr *&where_condition);
  int construct_sub_query_table(ObString &sub_query_name, ObQueryReqFromJson *query_req, ObReqTable *&sub_query);
  int construct_hybrid_query(ObQueryReqFromJson *fts, ObQueryReqFromJson *knn, ObQueryReqFromJson *&hybrid);
  int contruct_score_sum_expr(ObReqExpr *fts_score, ObReqExpr *vs_score, common::ObString &score_alias, ObReqOpExpr *&score);
  int construct_rank_feat_expr(const ObRankFeatDef &rank_feat_def, ObReqExpr *&rank_feat_expr);
  int construct_order_by_item(ObReqExpr *order_expr, bool ascent, OrderInfo *&order_info);
  int construct_join_condition(const ObString &l_table, const ObString &r_table,
                               const ObString &l_expr_name, const ObString &r_expr_name,
                               ObItemType condition, ObReqOpExpr *&join_condition);
  int construct_weighted_expr(ObReqExpr *base_expr, double weight, ObReqExpr *&weighted_expr);
  int construct_ip_expr(ObReqColumnExpr *vec_field, ObReqConstExpr *query_vec, ObReqCaseWhenExpr *&case_when/* score */,
                        ObReqOpExpr *minus_expr/* distance */, ObReqExpr *&order_by_vec);
  int set_fts_limit_expr(ObQueryReqFromJson *query, const ObReqConstExpr *size_expr, const ObReqConstExpr *from_expr);
  int get_distance_algor_type(const ObReqColumnExpr &vec_field, ObVectorIndexDistAlgorithm &alg_type);
  int get_match_idx_name(const ObString &match_field, ObString &idx_name);
  int set_distance_score_expr(const ObVectorIndexDistAlgorithm alg_type, ObReqConstExpr *norm_const, ObReqExpr *dist_vec,
                              ObReqOpExpr *add_expr, ObReqOpExpr *&score_expr);
  int set_output_columns(ObQueryReqFromJson &query_res);
  int convert_const_numeric(const ObString &cont_val, uint64_t &val);
  int convert_signed_const_numeric(const ObString &cont_val, int64_t &val);
  int choose_limit(ObQueryReqFromJson *query_res, ObReqConstExpr *size_expr);
  inline bool is_inner_column(const ObString &col_name) { return col_name == "_keyword_score" ||
                                                                 col_name == "_semantic_score" ||
                                                                 col_name == "__pk_increment"; }
  int construct_minimum_should_match_info(ObIJsonBase &req_node, BoolQueryMinShouldMatchInfo &min_should_match_info);
  int construct_required_params(const char *params_name[], uint32_t name_len, RequiredParamsSet &required_params);
  int get_base_table_query(ObQueryReqFromJson *query_req, ObQueryReqFromJson *&base_table_req);
  int build_should_groups(uint64_t start, uint64_t k, const common::ObIArray<ObReqExpr *> &items, common::ObIArray<ObReqExpr *> &expr_array, ObReqExpr *&or_expr);
  int build_should_groups(uint64_t msm_count, ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&gt_expr);
  int construct_should_group_expr_for_query_string(const common::ObIArray<ObReqColumnExpr *> &field_exprs,
                                                   const common::ObIArray<ObReqConstExpr *> &keyword_exprs,
                                                   const ObReqScoreType score_type,
                                                   QueryStringMinShouldMatchInfo &qs_min_should_match_info);
  int check_rank_feat_param(ObIJsonBase *sub_node, uint64_t &algorithm_count, bool &has_field, const ObString &key);
  ObIAllocator &alloc_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> source_cols_;
  bool need_json_wrap_;
  common::ObString table_name_;
  common::ObString database_name_;
  ColumnIndexNameMap index_name_map_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> user_cols_;
  ObIArray<ObString> *out_cols_;
  uint32_t sub_query_count_ = 0;
};


}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_QUERY_PARSE_H_