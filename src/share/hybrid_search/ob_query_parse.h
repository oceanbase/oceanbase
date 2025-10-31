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
  QUERY_ITEM_KNN, // FARM COMPAT WHITELIST
  QUERY_ITEM_HYBRID,
};

enum ObFusionMethod
{
  WEIGHT_SUM = 0,
  RRF
};

enum ObMsmApplyType : int8_t {
  MSM_NOT_APPLY = 0,
  MSM_APPLY_NOT_SUB,
  MSM_APPLY_WITH_SUB,
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

struct MinimumShouldMatchInfo {
  MinimumShouldMatchInfo() : term_cnt_(0), msm_expr_(nullptr), condition_expr_(nullptr), apply_type_(MSM_NOT_APPLY) {}
  virtual ~MinimumShouldMatchInfo() {}
  inline uint64_t get_msm_val() const {
    return OB_NOT_NULL(msm_expr_) ? static_cast<uint64_t>(msm_expr_->get_numeric_value()) : 0;
  }
  uint64_t term_cnt_;
  ObReqConstExpr *msm_expr_;
  ObReqExpr *condition_expr_;
  ObMsmApplyType apply_type_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> msm_items_;
  TO_STRING_KV(K(term_cnt_), K(msm_expr_), K(condition_expr_), K(apply_type_));
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
  ObEsQueryInfo() = delete;
  static int init_query_info(ObEsQueryInfo *&query_info,
                             ObIAllocator &alloc,
                             ObQueryReqFromJson *query_req,
                             ObEsQueryInfo *parent_query_info,
                             ObEsQueryItem outer_query_item,
                             bool need_cal_score = false);

  virtual ~ObEsQueryInfo() {}

  ObQueryReqFromJson *query_req_;
  bool need_cal_score_;
  uint64_t total_depth_;
  ObEsQueryInfo *parent_query_info_;
  ObEsQueryItem outer_query_item_;
  ObEsQueryItem query_item_;
  ObEsScoreType score_type_;
  ObItemType opr_;
  MinimumShouldMatchInfo msm_info_;
  ObReqConstExpr *boost_expr_;
  ObReqExpr *score_expr_;
  ObReqExpr *condition_expr_;
  ObReqExpr *score_alias_expr_;
  ObReqExpr *esql_condition_expr_;
  ObReqConstExpr *esql_options_expr_;
  ObString query_text_;
  // count of items in must, must_not, should, filter,
  // inited to -1 meaning the key does not exist
  int64_t must_cnt_;
  int64_t must_not_cnt_;
  int64_t should_cnt_;
  int64_t filter_cnt_;
  bool score_is_const_;
  uint64_t tkn_cnt_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> score_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> condition_items_;
  common::ObSEArray<ObReqExpr *, 4, common::ModulePageAllocator, true> score_alias_items_;
  common::ObSEArray<ObReqColumnExpr *, 4, common::ModulePageAllocator, true> field_exprs_;
  common::ObSEArray<ObReqConstExpr *, 4, common::ModulePageAllocator, true> keyword_exprs_;
  common::ObSEArray<ObEsQueryInfo *, 4, common::ModulePageAllocator, true> sub_query_infos_;
  common::ObSEArray<common::ObSEArray<ObReqMatchExpr *, 4, common::ModulePageAllocator, true>, 4, common::ModulePageAllocator, true> match_exprs_matrix_;

  void set_msm_apply_type();
  inline bool combine_keywords() const {
    return opr_ == T_OP_OR && (score_type_ != SCORE_TYPE_PHRASE || keyword_exprs_.count() == tkn_cnt_);
  }
  inline bool is_valid() const {
    if (OB_ISNULL(query_req_) || OB_ISNULL(score_expr_) || OB_ISNULL(condition_expr_)) {
      return false;
    }
    switch (query_item_) {
      case QUERY_ITEM_MATCH:
      case QUERY_ITEM_MULTI_MATCH:
      case QUERY_ITEM_QUERY_STRING:
        return !field_exprs_.empty() && !keyword_exprs_.empty() &&
               (OB_ISNULL(msm_info_.msm_expr_) || msm_info_.msm_expr_->get_numeric_value() > 0) &&
               (OB_ISNULL(boost_expr_) || boost_expr_->get_numeric_value() > 0);
      case QUERY_ITEM_TERM:
      case QUERY_ITEM_RANGE:
      default:
        return true;
    }
  }
  inline bool is_es_mode() const {
    return apply_es_mode_;
  }
  bool need_construct_sub_query_with_minimum_should_match() const;
  bool support_es_mode();
  uint64_t get_upward_depth() const;
  uint64_t get_total_depth() const;
  inline ObEsQueryInfo *get_top_query_info() const {
    ObEsQueryInfo *top_query_info = const_cast<ObEsQueryInfo *>(this);
    while (top_query_info->parent_query_info_ != nullptr) {
      top_query_info = top_query_info->parent_query_info_;
    }
    return top_query_info;
  }

  TO_STRING_KV(K(query_req_), K(need_cal_score_), K(total_depth_), K(parent_query_info_),
               K(outer_query_item_), K(query_item_), K(score_type_), K(opr_),
               K(boost_expr_),K(score_expr_), K(condition_expr_), K(score_alias_expr_),
               K(esql_condition_expr_), K(esql_options_expr_), K(msm_info_), K(query_text_),
               K(must_cnt_), K(must_not_cnt_), K(should_cnt_), K(filter_cnt_),
               K(score_is_const_), K(tkn_cnt_), K(apply_es_mode_));

private:
  ObEsQueryInfo(ObQueryReqFromJson *query_req, ObEsQueryInfo *parent_query_info, ObEsQueryItem outer_query_item, bool need_cal_score)
    : query_req_(query_req),
      need_cal_score_(need_cal_score),
      total_depth_(1),
      parent_query_info_(parent_query_info),
      outer_query_item_(outer_query_item),
      query_item_(QUERY_ITEM_UNKNOWN),
      score_type_(SCORE_TYPE_BEST_FIELDS),
      opr_(T_OP_OR),
      msm_info_(),
      boost_expr_(nullptr),
      score_expr_(nullptr),
      condition_expr_(nullptr),
      score_alias_expr_(nullptr),
      esql_condition_expr_(nullptr),
      esql_options_expr_(nullptr),
      query_text_(ObString()),
      must_cnt_(-1),
      must_not_cnt_(-1),
      should_cnt_(-1),
      filter_cnt_(-1),
      score_is_const_(false),
      tkn_cnt_(0),
      score_items_(),
      condition_items_(),
      score_alias_items_(),
      field_exprs_(),
      keyword_exprs_(),
      sub_query_infos_(),
      match_exprs_matrix_(),
      apply_es_mode_(false) {}
  bool apply_es_mode_;
  inline void set_es_mode_(bool value) {
    apply_es_mode_ = value;
  }
};

class ObESQueryParser
{
public :
  ObESQueryParser(ObIAllocator &alloc, common::ObString *table_name) :
    alloc_(alloc), source_cols_(), need_json_wrap_(false), table_name_(*table_name), database_name_(), index_name_map_(),
    user_cols_(), out_cols_(nullptr), enable_es_mode_(false), fusion_config_(), default_size_(nullptr) {}
  ObESQueryParser(ObIAllocator &alloc, bool need_json_wrap,
                  const common::ObString *table_name,
                  const common::ObString *database_name = nullptr,
                  bool enable_es_mode = false) :
    alloc_(alloc), source_cols_(), need_json_wrap_(need_json_wrap), table_name_(*table_name), database_name_(*database_name), index_name_map_(),
    user_cols_(), out_cols_(nullptr), enable_es_mode_(enable_es_mode), fusion_config_(), default_size_(nullptr) {}
  virtual ~ObESQueryParser() {}
  int parse(const common::ObString &req_str, ObQueryReqFromJson *&query_req);
  inline ColumnIndexNameMap &get_index_name_map() { return index_name_map_; }
  inline ObIArray<ObString> &get_user_column_names() { return user_cols_; }
private :
  int parse_query(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int parse_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int parse_source(ObIJsonBase &req_node);
  int parse_bool(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_must_clauses(ObIJsonBase &req_node, ObEsQueryInfo &query_info, ObReqExpr *&condition_expr, common::ObIArray<ObReqExpr *> &score_items);
  int parse_must_not_clauses(ObIJsonBase &req_node, ObEsQueryInfo &query_info, ObReqExpr *&condition_expr);
  int parse_should_clauses(ObIJsonBase &req_node, ObEsQueryInfo &query_info, ObReqExpr *&condition_expr, common::ObIArray<ObReqExpr *> &score_items);
  int parse_filter_clauses(ObIJsonBase &req_node, ObEsQueryInfo &query_info, ObReqExpr *&condition_expr);
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
  int parse_boost(ObIJsonBase &req_node, ObReqConstExpr *&boost_expr);
  int parse_const(ObIJsonBase &val_node, ObReqConstExpr *&var, const bool accept_numeric_string = false, const bool cover_value_to_str = false);
  int parse_keyword(const ObString &query_text, ObEsQueryInfo &query_info);
  int parse_keyword_array(ObIJsonBase &val_node, common::ObIArray<ObReqConstExpr *> &value_items);
  int parse_keyword_query_string(ObEsQueryInfo &query_info, const char *&current, const char *end, common::ObIArray<ObReqConstExpr *> &raw_keywords);
  int parse_keyword_multi_match(ObEsQueryInfo &query_info, const char *&current, const char *end, common::ObIArray<ObReqConstExpr *> &raw_keywords);
  int process_phrase_keywords(common::ObIArray<ObReqConstExpr *> &phrase_keywords, ObEsQueryInfo &query_info);
  int parse_minimum_should_match(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int parse_minimum_should_match_by_value(const common::ObString &val, const int64_t term_cnt, uint64_t &msm_count);
  int parse_best_fields(ObEsQueryInfo &query_info);
  int parse_cross_fields(ObEsQueryInfo &query_info);
  int parse_most_fields(ObEsQueryInfo &query_info);
  int parse_phrase(ObEsQueryInfo &query_info);
  int concat_const_exprs(const common::ObIArray<ObReqConstExpr *> &array, const ObString &connect_str, ObReqConstExpr *&result);
  int construct_expr_with_boost(ObReqExpr *expr, ObReqConstExpr *boost_expr, ObReqExpr *&result);
  int construct_es_expr(ObEsQueryInfo &query_info);
  int construct_es_expr_options(ObEsQueryInfo &query_info);
  int construct_es_expr_field(ObReqColumnExpr *raw_field, ObReqExpr *&field);
  int construct_condition_best_fields(ObEsQueryInfo &query_info);
  int construct_condition_most_fields(ObEsQueryInfo &query_info);
  int construct_condition_cross_fields(ObEsQueryInfo &query_info);
  int construct_condition_phrase(ObEsQueryInfo &query_info);
  int construct_match_exprs_matrix(ObEsQueryInfo &query_info);
  int construct_query_string_score(ObEsQueryInfo &query_info);
  int construct_query_string_condition(ObEsQueryInfo &query_info);
  int construct_in_expr(ObReqColumnExpr *col_expr, common::ObIArray<ObReqConstExpr *> &value_exprs, ObReqOpExpr *&in_expr);
  int wrap_sub_query(const ObString &sub_query_name, ObQueryReqFromJson *&query_req);
  int wrap_json_result(ObQueryReqFromJson *&query_req);
  int construct_query_with_similarity(ObVectorIndexDistAlgorithm algor, ObReqExpr *dist, ObReqConstExpr *similar, ObQueryReqFromJson *&query_req);
  int construct_all_query(ObQueryReqFromJson *&query_req);
  int construct_sub_query_table(const ObString &sub_query_name, ObQueryReqFromJson *query_req, ObReqTable *&sub_query);
  int construct_hybrid_query(ObQueryReqFromJson *fts, ObQueryReqFromJson *knn, ObQueryReqFromJson *&hybrid);
  int construct_score_sum_expr(ObReqExpr *fts_score, ObReqExpr *vs_score, const ObString &score_alias, ObReqOpExpr *&score);
  int construct_rank_feat_expr(const ObRankFeatDef &rank_feat_def, ObReqExpr *&rank_feat_expr);
  int construct_order_by_item(ObReqExpr *order_expr, bool ascent, OrderInfo *&order_info);
  int construct_join_condition(const ObString &l_table, const ObString &r_table,
                               const ObString &l_expr_name, const ObString &r_expr_name,
                               ObItemType condition, ObReqOpExpr *&join_condition);
  int construct_weighted_expr(ObReqExpr *base_expr, double weight, ObReqExpr *&weighted_expr);
  int construct_ip_expr(ObReqColumnExpr *vec_field, ObReqConstExpr *query_vec, ObReqCaseWhenExpr *&case_when/* score */,
                        ObReqOpExpr *&minus_expr/* distance */, ObReqExpr *&order_by_vec);
  int set_default_score(ObQueryReqFromJson *query_req, double default_score);
  int set_order_by_column(ObQueryReqFromJson *query_req, const ObString &column_name, const ObString &table_name, bool ascent = true);
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
  int construct_minimum_should_match_info(ObIJsonBase &req_node, ObEsQueryInfo &query_info);
  int construct_required_params(const char *params_name[], uint32_t name_len, RequiredParamsSet &required_params);
  int get_base_table_query(ObQueryReqFromJson *query_req, ObQueryReqFromJson *&base_table_req, ReqTableType *table_type = nullptr);
  int build_should_condition_combine(uint64_t start, uint64_t k, const common::ObIArray<ObReqExpr *> &items, common::ObIArray<ObReqExpr *> *work_array, ObReqExpr *&should_condition);
  int build_should_condition_compare(ObReqConstExpr *msm_expr, const common::ObIArray<ObReqExpr *> &items, ObReqExpr *&should_condition);
  int handle_msm_for_sub_score(ObEsQueryInfo &query_info, ObEsQueryInfo &inner_query_info, ObReqExpr *score_expr);
  int handle_msm_for_sub_condition(ObEsQueryInfo &query_info);
  int construct_should_group_expr(ObEsQueryInfo &query_info);
  int check_rank_feat_param(ObIJsonBase *sub_node, uint64_t &algorithm_count, bool &has_field, const ObString &key);
  int parse_multi_knn(ObIJsonBase &req_node, ObQueryReqFromJson *&query_req);
  int knn_fusion(const ObIArray<ObQueryReqFromJson*> &knn_queries, ObQueryReqFromJson *&query_req);
  int add_score_col(const ObString &table_name, ObQueryReqFromJson &query_req);
  int add_pk_to_sort(ObQueryReqFromJson *query, const ObEsQueryItem query_item);
  int parse_rank(ObIJsonBase &req_node);
  int parse_rrf(ObIJsonBase &req_node);
  int construct_rank_score(const ObString &table_name, const ObString &rank_alias, ObReqExpr *&rank_score);
  int construct_rank_query(ObString &sub_query_name, ObReqExpr *order_expr, ObString &rank_alias, ObQueryReqFromJson *&query_req);
  int init_default_params(ObIJsonBase &req_node);
  int construct_sub_query_with_minimum_should_match(ObQueryReqFromJson *&query_req, ObEsQueryInfo &query_info, const ObString &sub_query_name);
  int get_query_depth(ObIJsonBase &req_node, uint64_t &depth);
  inline bool check_is_bool_key(ObString &key) {
    return key.case_compare("must") == 0 || key.case_compare("must_not") == 0 || key.case_compare("should") == 0 || key.case_compare("filter") == 0 || key.case_compare("bool") == 0;
  }
  ObIAllocator &alloc_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> source_cols_;
  bool need_json_wrap_;
  common::ObString table_name_;
  common::ObString database_name_;
  ColumnIndexNameMap index_name_map_;
  common::ObSEArray<common::ObString, 4, common::ModulePageAllocator, true> user_cols_;
  ObIArray<ObString> *out_cols_;
  // if enable es mode
  bool enable_es_mode_ = false;
  ObRankFusion fusion_config_;
  ObReqConstExpr *default_size_;
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
  static const ObString MSM_KEY;
  static const ObString FTS_SUB_SCORE_PREFIX;
  static const ObString HIDDEN_COLUMN_VISIBLE_HINT;
};

}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_QUERY_PARSE_H_