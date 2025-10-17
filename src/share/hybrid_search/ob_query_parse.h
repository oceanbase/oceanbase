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

#include "lib/json_type/ob_json_parse.h"
#include "ob_query_request.h"
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

enum ObEsQueryItem : int8_t {
  QUERY_ITEM_UNKNOWN = -1,
  QUERY_ITEM_QUERY = 0,
  QUERY_ITEM_MUST,
  QUERY_ITEM_MUST_NOT,
  QUERY_ITEM_SHOULD,
  QUERY_ITEM_FILTER,
  QUERY_ITEM_BOOL,
  QUERY_ITEM_RANGE,
  QUERY_ITEM_MATCH,
  QUERY_ITEM_QUERY_STRING,
  QUERY_ITEM_MULTI_MATCH,
  QUERY_ITEM_TERM,
  QUERY_ITEM_RANK_FEATURE,
  QUERY_ITEM_TERMS,
  QUERY_ITEM_COUNT
};

enum ObFusionMethod
{
  WEIGHT_SUM = 0,
  RRF
};

class ObRankFusion {
public :
  ObRankFusion() :
    method(ObFusionMethod::WEIGHT_SUM), rank_const(nullptr), size(nullptr) {}
  virtual ~ObRankFusion() {}
  ObFusionMethod method;
  ObReqConstExpr *rank_const;
  ObReqConstExpr *size;
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
  BoolQueryMinShouldMatchInfo() : has_msm_(false), has_where_condition_(true), msm_expr_(nullptr), msm_val_(0), or_expr_(nullptr) {}
  virtual ~BoolQueryMinShouldMatchInfo() {}
  bool has_msm_;
  bool has_where_condition_;
  ObReqConstExpr *msm_expr_;
  uint64_t msm_val_;
  ObReqExpr *or_expr_;
  TO_STRING_KV(K(has_msm_), K(has_where_condition_), K(msm_expr_), K(msm_val_), K(or_expr_));
};

struct QueryStringMinShouldMatchInfo {
  QueryStringMinShouldMatchInfo() : msm_expr_(nullptr), msm_val_(0), or_expr_(nullptr) {}
  virtual ~QueryStringMinShouldMatchInfo() {}
  ObReqConstExpr *msm_expr_;
  uint64_t msm_val_;
  ObReqExpr *or_expr_;
  TO_STRING_KV(K(msm_expr_), K(msm_val_), K(or_expr_));
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
class ObEsQueryInfo {
public:
  ObEsQueryInfo(ObQueryReqFromJson *query_req, ObEsQueryItem parent_query_item, bool is_basic_query, bool need_cal_score = true)
    : query_req_(query_req),
      need_cal_score_(need_cal_score),
      parent_query_item_(parent_query_item),
      query_item_(QUERY_ITEM_UNKNOWN),
      score_type_(SCORE_TYPE_BEST_FIELDS),
      operator_(T_OP_OR),
      boost_(1.0),
      score_expr_(nullptr),
      condition_expr_(nullptr),
      basic_query_condition_expr_(nullptr),
      esql_condition_expr_(nullptr),
      query_keywords_(ObString()),
      has_must_not_(false),
      has_filter_(false),
      score_is_const_(false),
      is_basic_query_(is_basic_query),
      score_items_(),
      condition_items_(),
      field_exprs_(),
      keyword_exprs_(),
      apply_es_mode_(false) {}

  ObQueryReqFromJson *query_req_;
  bool need_cal_score_;
  ObEsQueryItem parent_query_item_;
  ObEsQueryItem query_item_;
  ObEsScoreType score_type_;
  ObItemType operator_;
  double boost_;
  ObReqExpr *score_expr_;
  ObReqExpr *condition_expr_;
  ObReqExpr *basic_query_condition_expr_;
  ObReqExpr *esql_condition_expr_;
  QueryStringMinShouldMatchInfo qs_msm_info_;
  BoolQueryMinShouldMatchInfo bq_msm_info_;
  ObString query_keywords_;
  bool has_must_not_;
  bool has_filter_;
  bool score_is_const_;
  bool is_basic_query_;
  bool is_one_keyword_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> score_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> condition_items_;
  common::ObSEArray<ObReqColumnExpr *, 4, common::ModulePageAllocator, true> field_exprs_;
  common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> keyword_exprs_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> basic_query_score_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> token_exprs_;

  inline bool is_valid() const {
    if (OB_ISNULL(query_req_) || OB_ISNULL(score_expr_) || OB_ISNULL(condition_expr_)) {
      return false;
    }
    switch (query_item_) {
      case QUERY_ITEM_MATCH:
      case QUERY_ITEM_MULTI_MATCH:
      case QUERY_ITEM_QUERY_STRING:
        return !field_exprs_.empty() && !query_keywords_.empty() &&
               (OB_ISNULL(qs_msm_info_.msm_expr_) || qs_msm_info_.msm_val_ > 0) && boost_ > 0;
      case QUERY_ITEM_TERM:
      case QUERY_ITEM_RANGE:
        return (OB_ISNULL(qs_msm_info_.msm_expr_) || qs_msm_info_.msm_val_ > 0) && boost_ > 0;
      default:
        return false;
    }
  }
  inline bool support_es_mode() {
    set_es_mode_((query_item_ == QUERY_ITEM_MATCH ||
                  query_item_ == QUERY_ITEM_MULTI_MATCH||
                  query_item_ == QUERY_ITEM_QUERY_STRING) &&
                 operator_ == T_OP_OR &&
                 boost_ != 0.0 &&
                 (score_type_ == SCORE_TYPE_BEST_FIELDS || score_type_ == SCORE_TYPE_MOST_FIELDS));
    return apply_es_mode_;
  }
  inline bool is_es_mode() const {
    return apply_es_mode_;
  }
private:
  bool apply_es_mode_;
  inline void set_es_mode_(bool value) {
    apply_es_mode_ = value;
  }
};

class ObESQueryParser
{
public :
  ObESQueryParser(ObIAllocator &alloc, common::ObString *table_name) : alloc_(alloc), source_cols_(),
    need_json_wrap_(false), table_name_(*table_name), user_cols_(), item_seq_(0), out_cols_(nullptr), enable_es_mode_(false),
    fusion_config_(), default_size_(nullptr), is_basic_query_(true) {}
  ObESQueryParser(ObIAllocator &alloc, bool need_json_wrap,
                  const common::ObString *table_name,
                  const common::ObString *database_name = nullptr,
                  bool enable_es_mode = false,
                  bool is_basic_query = true)
    : alloc_(alloc), source_cols_(), need_json_wrap_(need_json_wrap), table_name_(*table_name), database_name_(*database_name),
      user_cols_(), item_seq_(0), out_cols_(nullptr), enable_es_mode_(enable_es_mode), fusion_config_(), default_size_(nullptr) {}
  virtual ~ObESQueryParser() {}
  int parse(const common::ObString &req_str, ObQueryReqFromJson *&query_req);
  inline ColumnIndexNameMap &get_index_name_map() { return index_name_map_; }
  inline ObIArray<ObString> &get_user_column_names() { return user_cols_; }
  inline bool need_construct_sub_query() {return outer_filter_items_.count() > 0;}
private :
  int parse_query(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int parse_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int parse_source(ObIJsonBase &req_node);
  int parse_bool(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_must_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr, common::ObIArray<ObReqExpr *> &score_items, bool need_cal_score = true);
  int parse_must_not_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr);
  int parse_should_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr, common::ObIArray<ObReqExpr *> &score_items,
                           BoolQueryMinShouldMatchInfo &bq_msm_info, bool need_cal_score = true);
  int parse_filter_clauses(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req, ObReqExpr *&condition_expr);
  int parse_single_term(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_range(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_term(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_terms(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_multi_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string_boost(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string_fields(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string_operator(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string_query(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string_type(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_query_string_by_type(ObEsQueryInfo &query_info);
  int parse_rank_feature(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_rank_feat_param(ObIJsonBase &req_node, const ObString &para1, const ObString &para2,
                            ObReqConstExpr *&const_para1, ObReqConstExpr *&const_para2, bool &positive);
  int parse_basic_table(const ObString &table_name, ObQueryReqFromJson *query_req);
  int parse_field(ObIJsonBase &val_node, ObReqColumnExpr *&field);
  int parse_const(ObIJsonBase &val_node, ObReqConstExpr *&var, const bool accept_numeric_string = false, const bool cover_value_to_str = false);
  int parse_keyword(const ObString &query_text, ObEsQueryInfo &query_info);
  int parse_keyword_array(ObIJsonBase &val_node, common::ObIArray<ObReqConstExpr *> &value_items);
  int parse_keyword_query_string(const ObString &query_text, ObEsQueryInfo &query_info);
  int parse_keyword_multi_match(const ObString &query_text, ObEsQueryInfo &query_info);
  int process_phrase_keywords(common::ObIArray<ObReqConstExpr *> &phrase_keywords, ObEsQueryInfo &query_info);
  int parse_minimum_should_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_minimum_should_match_by_value(const common::ObString &val, const int64_t term_cnt, uint64_t &msm_count);
  int parse_minimum_should_match_with_bool_query(ObIJsonBase &req_node, const int64_t term_cnt, BoolQueryMinShouldMatchInfo &bq_msm_info);
  int parse_best_fields(ObEsQueryInfo &query_info);
  int parse_cross_fields(ObEsQueryInfo &query_info);
  int parse_most_fields(ObEsQueryInfo &query_info);
  int parse_phrase(ObEsQueryInfo &query_info);
  int construct_es_expr(ObEsQueryInfo &query_info);
  int construct_es_expr_params(const ObEsQueryInfo &query_info, ObReqConstExpr *&options);
  int construct_es_expr_fields(ObReqColumnExpr *raw_field, ObReqExpr *&field);
  int construct_condition_best_fields(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions);
  int construct_condition_most_fields(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions);
  int construct_condition_cross_fields(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions);
  int construct_condition_phrase(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &conditions);
  int construct_query_string_condition(ObEsQueryInfo &query_info);
  int construct_in_expr(ObReqColumnExpr *col_expr, common::ObIArray<ObReqConstExpr *> &value_exprs, ObReqOpExpr *&in_expr);
  int wrap_sub_query(ObString &sub_query_name, ObQueryReqFromJson *&query_req);
  int wrap_json_result(ObQueryReqFromJson *&query_req);
  int construct_query_with_similarity(ObVectorIndexDistAlgorithm algor, ObReqExpr *dist, ObReqConstExpr *similar, ObQueryReqFromJson *&query_req);
  int construct_all_query(ObQueryReqFromJson *&query_req);
  int construct_sub_query_table(ObString &sub_query_name, ObQueryReqFromJson *query_req, ObReqTable *&sub_query);
  int construct_hybrid_query(ObQueryReqFromJson *fts, ObQueryReqFromJson *knn, ObQueryReqFromJson *&hybrid);
  int construct_score_sum_expr(ObReqExpr *fts_score, ObReqExpr *vs_score, ObString &score_alias, ObReqOpExpr *&score);
  int construct_rank_feat_expr(const ObRankFeatDef &rank_feat_def, ObReqExpr *&rank_feat_expr);
  int construct_order_by_item(ObReqExpr *order_expr, bool ascent, OrderInfo *&order_info);
  int construct_join_condition(const ObString &l_table, const ObString &r_table,
                               const ObString &l_expr_name, const ObString &r_expr_name,
                               ObItemType condition, ObReqOpExpr *&join_condition);
  int construct_weighted_expr(ObReqExpr *base_expr, double weight, ObReqExpr *&weighted_expr);
  int construct_ip_expr(ObReqColumnExpr *vec_field, ObReqConstExpr *query_vec, ObReqCaseWhenExpr *&case_when/* score */,
                        ObReqOpExpr *&minus_expr/* distance */, ObReqExpr *&order_by_vec);
  int set_default_score(ObQueryReqFromJson *query_req, double default_score);
  int set_order_by_column(ObQueryReqFromJson *query_req, const ObString &column_name, bool ascent = true);
  int set_fts_limit_expr(ObQueryReqFromJson *query, const ObReqConstExpr *size_expr, const ObReqConstExpr *from_expr);
  inline bool query_not_need_order(ObQueryReqFromJson *query_req) { return OB_ISNULL(query_req->limit_item_) && query_req->group_items_.empty(); }
  int get_distance_algor_type(const ObReqColumnExpr &vec_field, ObVectorIndexDistAlgorithm &alg_type);
  int get_match_idx_name(const ObString &match_field, ObString &idx_name);
  int set_distance_score_expr(const ObVectorIndexDistAlgorithm alg_type, ObReqConstExpr *&norm_const, ObReqExpr *&dist_vec,
                              ObReqOpExpr *&add_expr, ObReqOpExpr *&score_expr);
  int set_output_columns(ObQueryReqFromJson &query_res, bool is_hybrid, bool include_inner_column = true);
  int convert_const_numeric(const ObString &cont_val, int64_t &val);
  int convert_signed_const_numeric(const ObString &cont_val, int64_t &val);
  int choose_limit(ObQueryReqFromJson *query_req, ObReqConstExpr *size_expr);
  inline bool is_inner_column(const ObString &col_name) { return col_name == "_keyword_score" ||
                                                                 col_name == "_semantic_score" ||
                                                                 col_name == "__pk_increment" ||
                                                                 col_name == "_keyword_rank" ||
                                                                 col_name == "_semantic_rank"; }
  int construct_minimum_should_match_info(ObIJsonBase &req_node, BoolQueryMinShouldMatchInfo &bq_msm_info);
  int construct_required_params(const char *params_name[], uint32_t name_len, RequiredParamsSet &required_params);
  int get_base_table_query(ObQueryReqFromJson *query_req, ObQueryReqFromJson *&base_table_req, ReqTableType *table_type = nullptr);
  int build_should_condition_combine(uint64_t start, uint64_t k, const common::ObIArray<ObReqExpr *> &items, common::ObIArray<ObReqExpr *> &expr_array, ObReqExpr *&should_condition);
  int build_should_condition_compare(uint64_t msm_val, ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&should_condition);
  int construct_should_group_expr_for_query_string(ObEsQueryInfo &query_info);
  int check_rank_feat_param(ObIJsonBase *sub_node, uint64_t &algorithm_count, bool &has_field, const ObString &key);
  int parse_multi_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int knn_fusion(const ObIArray<ObQueryReqFromJson*> &knn_queries, ObQueryReqFromJson *&query_req);
  int add_score_col(const ObString &table_name, ObQueryReqFromJson &query_req);
  int parse_rank(ObIJsonBase &req_node);
  int parse_rrf(ObIJsonBase &req_node);
  int construct_rank_score(const ObString &table_name, const ObString &rank_alias, ObReqExpr *&rank_score);
  int construct_rank_query(ObString &sub_query_name, ObReqExpr *order_expr, ObString &rank_alias, ObQueryReqFromJson *&query_req);
  int init_default_params(ObIJsonBase &req_node);
  bool check_need_construct_msm_expr(ObEsQueryInfo &query_info);

  /// use in basic query
  int construct_basic_query_filter_condition_with_and_expr();
  int construct_basic_query_filter_condition_with_or_expr(uint64_t msm_val, ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&or_expr);
  int construct_basic_query_select_items_with_query_string(ObEsQueryInfo &query_info, common::ObIArray<ObReqExpr *> &score_items);
  int construct_alias_column_expr_to_select_items(ObQueryReqFromJson &query_req, const ObEsQueryInfo &query_info);
  int construct_alias_column_expr_to_select_items_with_query_string(ObQueryReqFromJson &query_req, const ObEsQueryInfo &query_info);
  int construct_sub_query_with_minimum_should_match(ObQueryReqFromJson *&query_req);

  ObIAllocator &alloc_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> source_cols_;
  bool need_json_wrap_;
  common::ObString table_name_;
  common::ObString database_name_;
  ColumnIndexNameMap index_name_map_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> user_cols_;

  // for construct basic query filter condition in outer query
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> tmp_outer_filter_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> outer_filter_items_;
  uint32_t item_seq_;

  ObIArray<ObString> *out_cols_;
  // if enable es mode
  bool enable_es_mode_ = false;
  ObRankFusion fusion_config_;
  ObReqConstExpr *default_size_;
  bool is_basic_query_;
  static const ObString SCORE_NAME;
  static const ObString FTS_SCORE_NAME;
  static const ObString VS_SCORE_NAME;
  static const ObString SIMILARITY_SCORE_NAME;
  static const ObString FTS_RANK_NAME;
  static const ObString VS_RANK_NAME;
  static const ObString ROWKEY_NAME;
  static const ObString RANK_CONST_DEFAULT;
  static const ObString SIZE_DEFAULT;
  static const ObString FTS_ALIAS;
  static const ObString VS_ALIAS;

  int check_is_basic_query(ObIJsonBase &req_node, int depth);
  inline bool check_is_bool_query(ObString &key) {
    return key == "must" || key == "must_not" ||
           key == "should" || key == "filter" ||
           key == "bool";
  }
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_QUERY_PARSE_H_