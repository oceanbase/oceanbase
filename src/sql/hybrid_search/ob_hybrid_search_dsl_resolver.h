/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_RESOLVER_H_
#define OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_RESOLVER_H_

#include "share/hybrid_search/ob_query_parse.h"

// hybrid_search supports scalar scoring on array/json/scalar query
#define HYBRID_SEARCH_SUPPORT_SCALAR_SCORING(cluster_version) ((cluster_version) >= CLUSTER_VERSION_4_6_0_1)

namespace oceanbase
{
namespace sql
{
class ObDSLFullTextQuery;
struct ObDSLFullTextMultiFieldQueryParam;
class ObSelectStmt;
class ObWinFunRawExpr;
class ObAggFunRawExpr;
struct TableItem;

// ObDSLQuery is an abstract class and would never be instantiated directly
class ObDSLQuery
{
public:
  virtual ~ObDSLQuery() {}

  ObEsQueryItem query_type_;
  ObEsQueryItem outer_query_type_;
  ObDSLQuery *parent_query_;
  ObConstRawExpr *boost_;
  bool need_cal_score_;
  bool is_top_level_score_;
  TO_STRING_KV(K(query_type_), K(outer_query_type_), K(parent_query_), K(boost_),
               K(need_cal_score_), K(is_top_level_score_));

  inline bool check_need_cal_score() const
  {
    if (!HYBRID_SEARCH_SUPPORT_SCALAR_SCORING(GET_MIN_CLUSTER_VERSION()) &&
        (IS_QUERY_ITEM_ARRAY(query_type_) ||
         IS_QUERY_ITEM_JSON(query_type_) ||
         IS_QUERY_ITEM_SCALAR(query_type_))) {
      return false;
    } else if (outer_query_type_ == QUERY_ITEM_FILTER || outer_query_type_ == QUERY_ITEM_MUST_NOT ||
               (outer_query_type_ == QUERY_ITEM_UNKNOWN && query_type_ != QUERY_ITEM_KNN)) {// it is a sql but not a dsl
      return false;
    } else if (parent_query_ != nullptr && !parent_query_->need_cal_score_) {
      return false;
    }
    return true;
  }
  inline static bool check_need_cal_score_in_bool(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
  {
    if (HYBRID_SEARCH_SUPPORT_SCALAR_SCORING(GET_MIN_CLUSTER_VERSION())) {
      // this function is only to check if the query need scoring to report OB_NOT_SUPPORTED error
      // so if scalar scoring is supported, return false to skip the check and error reporting
      return false;
    } else if (outer_query_type != QUERY_ITEM_MUST && outer_query_type != QUERY_ITEM_SHOULD) {
      return false;
    } else if (parent_query != nullptr && !parent_query->need_cal_score_) {
      return false;
    }
    return true;
  }
  inline void assign_common_attr(const ObDSLQuery *src)
  {
    need_cal_score_ = src->need_cal_score_;
    is_top_level_score_ = src->is_top_level_score_;
  }

protected:
  ObDSLQuery(ObEsQueryItem query_type, ObEsQueryItem outer_query_type, ObDSLQuery *parent_query = nullptr)
    : query_type_(query_type),
      outer_query_type_(outer_query_type),
      parent_query_(parent_query),
      boost_(nullptr),
      need_cal_score_(false),
      is_top_level_score_(false)
  {
    need_cal_score_ = check_need_cal_score();
  }
};

class ObDSLBoolQuery : public ObDSLQuery
{
public:
  ObDSLBoolQuery() = delete;
  static int create(ObIAllocator &alloc, ObDSLBoolQuery *&bool_query,
                    ObEsQueryItem outer_query_type, ObDSLQuery *parent_query);
  virtual ~ObDSLBoolQuery() {}

  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> must_;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> should_;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter_;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> must_not_;
  // for must/should/filter/must_not, -1: not exists, 0: exists but empty, >0: exists and has content
  int64_t must_cnt_;
  int64_t should_cnt_;
  int64_t filter_cnt_;
  int64_t must_not_cnt_;
  int32_t msm_;
  ObRawExpr *origin_expr_; // for search index
  INHERIT_TO_STRING_KV("ObDSLQuery", ObDSLQuery,
                       K(must_), K(should_), K(filter_), K(must_not_),
                       K(must_cnt_), K(should_cnt_), K(filter_cnt_), K(must_not_cnt_),
                       K(msm_), K(origin_expr_));

private:
  ObDSLBoolQuery(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLQuery(QUERY_ITEM_BOOL, outer_query_type, parent_query),
      must_(), should_(), filter_(), must_not_(),
      must_cnt_(-1), should_cnt_(-1), filter_cnt_(-1), must_not_cnt_(-1),
      msm_(1), origin_expr_(nullptr) {}
};

class ObDSLKnnQuery : public ObDSLQuery
{
public:
  ObDSLKnnQuery() = delete;
  static int create(ObIAllocator &alloc, ObDSLKnnQuery *&knn_query,
                    ObEsQueryItem outer_query_type);
  virtual ~ObDSLKnnQuery() {}

  struct SearchOption {
    SearchOption()
      : param_(), filter_mode_(INVALID_KNN_FILTER_MODE), is_set_num_candidates_(false), num_candidates_(0) {}
    ~SearchOption() {}
    ObVectorIndexQueryParam param_;
    ObKnnFilterMode filter_mode_;  // ["pre", "pre-knn", "pre-brute", "post", "post-index-merge"]
    bool is_set_num_candidates_;
    int32_t num_candidates_;
    TO_STRING_KV(K(param_), K(filter_mode_), K(is_set_num_candidates_), K(num_candidates_));
  };
  ObVectorIndexDistAlgorithm dist_algo_;
  ObColumnRefRawExpr *field_;
  ObConstRawExpr *k_;
  ObConstRawExpr *query_vector_;
  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> filter_;
  ObRawExpr *distance_;
  SearchOption *search_option_;
  INHERIT_TO_STRING_KV("ObDSLQuery", ObDSLQuery,
                       K(dist_algo_), K(field_), K(k_), K(query_vector_),
                       K(filter_), K(distance_), K(search_option_));

private:
  ObDSLKnnQuery(ObEsQueryItem outer_query_type)
    : ObDSLQuery(QUERY_ITEM_KNN, outer_query_type),
      dist_algo_(ObVectorIndexDistAlgorithm::VIDA_L2),
      field_(nullptr),
      k_(nullptr),
      query_vector_(nullptr),
      filter_(),
      distance_(nullptr),
      search_option_(nullptr) {}
};

class ObDSLScalarQuery : public ObDSLQuery
{
public:
  ObDSLScalarQuery() = delete;
  static int create(ObIAllocator &alloc, ObDSLScalarQuery *&scalar_query,
                    ObEsQueryItem query_type, ObEsQueryItem outer_query_type, ObDSLQuery *parent_query);
  virtual ~ObDSLScalarQuery() {}

  bool is_field_json_extract() const { return OB_NOT_NULL(field_) && field_->get_expr_type() == T_FUN_SYS_JSON_EXTRACT; }

  // field_ can be either ObColumnRefRawExpr (for table column) or
  // ObSysFunRawExpr with T_FUN_SYS_JSON_EXTRACT (for JSON path like doc_json.id)
  ObRawExpr *field_;
  ObRawExpr *scalar_expr_;
  INHERIT_TO_STRING_KV("ObDSLQuery", ObDSLQuery,
                       K(field_), K(scalar_expr_));

private:
  ObDSLScalarQuery(ObEsQueryItem query_type, ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
    : ObDSLQuery(query_type, outer_query_type, parent_query),
      field_(nullptr),
      scalar_expr_(nullptr) {}
};

struct ObDSLRankInfo {
  ObDSLRankInfo()
    : method_(ObFusionMethod::WEIGHT_SUM), window_size_(nullptr), rank_const_(nullptr) {}
  ~ObDSLRankInfo() {}
  ObFusionMethod method_;
  ObRawExpr *window_size_;
  ObRawExpr *rank_const_;
  //TODO: ai_rerank_info_
  TO_STRING_KV(K(method_), K(window_size_), K(rank_const_));
};

enum class ObDSLResultMode
{
  SEARCH_HITS = 0,
  COUNT_AGG,
  BUCKET_AGG
};

struct ObDSLSortItem
{
  enum class MissingMode
  {
    NONE = 0,
    FIRST,
    LAST,
    LITERAL
  };

  ObDSLSortItem()
    : field_expr_(nullptr), is_asc_(true), missing_mode_(MissingMode::NONE),
      missing_literal_(nullptr), is_score_sort_(false) {}
  ~ObDSLSortItem() {}

  ObRawExpr *field_expr_;         // `_score` sort uses the final score expr
  bool is_asc_;
  MissingMode missing_mode_;      // NONE/FIRST/LAST/LITERAL for DSL missing handling
  ObConstRawExpr *missing_literal_; // used when missing_mode_ == LITERAL
  bool is_score_sort_;
  TO_STRING_KV(K(field_expr_), K(is_asc_), K(missing_mode_), K(missing_literal_), K(is_score_sort_));
};

struct ObDSLCollapseInfo {
  ObDSLCollapseInfo()
    : field_(nullptr), win_func_expr_(nullptr), qualify_expr_(nullptr) {}
  ObColumnRefRawExpr *field_;
  ObWinFunRawExpr *win_func_expr_;
  ObRawExpr *qualify_expr_;
  TO_STRING_KV(K(field_), KPC(win_func_expr_), KPC(qualify_expr_));
};

struct ObDSLAggTermsItem {
  enum AggType { TERMS, CARDINALITY };
  enum OrderByType { BY_COUNT, BY_KEY };
  ObDSLAggTermsItem()
    : agg_type_(TERMS), agg_name_(), field_(nullptr), size_(10),
      min_doc_count_(1), order_by_(BY_COUNT), order_asc_(false),
      count_expr_(nullptr), name_conflicts_with_user_column_(false) {}
  AggType              agg_type_;
  ObString             agg_name_;
  ObColumnRefRawExpr  *field_;
  int64_t              size_;
  int64_t              min_doc_count_;
  OrderByType          order_by_;
  bool                 order_asc_;
  ObAggFunRawExpr     *count_expr_;
  bool                 name_conflicts_with_user_column_;
  TO_STRING_KV(K(agg_type_), K(agg_name_), KPC(field_), K(size_), K(min_doc_count_),
               K(order_by_), K(order_asc_), KPC(count_expr_), K(name_conflicts_with_user_column_));
};

struct ObDSLQueryInfo
{
  ObDSLQueryInfo()
    : queries_(), from_(nullptr), size_(nullptr), min_score_(nullptr), one_const_expr_(nullptr),
      query_top_level_boost_(nullptr), sort_items_(), raw_dsl_param_str_(),
      is_top_k_query_(true), query_dop_(1), result_mode_(ObDSLResultMode::SEARCH_HITS), has_dsl_sort_(false),
      has_dsl_aggs_(false), has_dsl_rank_(false), has_dsl_collapse_(false), track_score_(true) {}
  static int check_column_in_dsl(ObIArray<TableItem*> &table_items, ObColumnRefRawExpr *col_expr, bool &in_dsl);
  int deep_copy(const ObDSLQueryInfo& src, ObIRawExprCopier &expr_copier, ObIAllocator* allocator);
  static int deep_copy_query(const ObDSLQuery *src, ObDSLQuery *&dst,
                             ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent_query = nullptr);
  static int deep_copy_query_bool(const ObDSLBoolQuery *src, ObDSLBoolQuery *&dst,
                                  ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent);
  static int deep_copy_query_knn(const ObDSLKnnQuery *src, ObDSLKnnQuery *&dst,
                                 ObIRawExprCopier &expr_copier, ObIAllocator* allocator);
  static int deep_copy_query_fulltext(const ObDSLFullTextQuery *src, ObDSLFullTextQuery *&dst,
                                      ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent);
  static int deep_copy_query_scalar(const ObDSLScalarQuery *src, ObDSLScalarQuery *&dst,
                                    ObIRawExprCopier &expr_copier, ObIAllocator* allocator, ObDSLQuery *parent);
  int deep_copy_sort_items(const ObDSLQueryInfo &src, ObIRawExprCopier &expr_copier);
  int init_default_params(ObRawExprFactory &expr_factory, bool is_top_k_query = true);

  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> queries_;
  ObRawExpr *from_;
  ObRawExpr *size_;
  ObRawExpr *min_score_;
  ObRawExpr *one_const_expr_;
  ObRawExpr *query_top_level_boost_;
  ObDSLRankInfo rank_info_;
  ObSEArray<ObColumnRefRawExpr*, 4, ModulePageAllocator, true> rowkey_cols_;
  ObSEArray<ObColumnRefRawExpr*, 4, ModulePageAllocator, true> dsl_cols;
  ObSEArray<ObOpPseudoColumnRawExpr*, 4, ModulePageAllocator, true> score_cols_;
  ObSEArray<ObRawExpr*, 4, ModulePageAllocator, true> dsl_exprs_;
  ObSEArray<ObDSLSortItem, 4, ModulePageAllocator, true> sort_items_;
  ObString raw_dsl_param_str_;
  bool is_top_k_query_;
  int64_t query_dop_;
  ObDSLResultMode result_mode_;
  bool has_dsl_sort_;
  bool has_dsl_aggs_;
  bool has_dsl_rank_;
  bool has_dsl_collapse_;
  bool track_score_;
  ObDSLCollapseInfo collapse_info_;
  ObSEArray<ObDSLAggTermsItem, 2, ModulePageAllocator, true> agg_items_;
  TO_STRING_KV(K(queries_), K(from_), K(size_), K(min_score_), K(query_top_level_boost_),
               K(rank_info_), K(rowkey_cols_), K(dsl_cols), K(score_cols_), K(sort_items_),
               K(dsl_exprs_), K(raw_dsl_param_str_), K(is_top_k_query_), K(query_dop_),
               K(result_mode_), K(has_dsl_sort_), K(has_dsl_aggs_), K(has_dsl_rank_), K(track_score_),
               K(has_dsl_collapse_), K(collapse_info_), K(agg_items_));
};

class ObDSLResolver
{
public :
  explicit ObDSLResolver(ObResolverParams &params, const ObTableSchema *table_schema, ObDMLStmt *stmt,
                         TableItem &table_item)
    : allocator_(params.allocator_),
    schema_checker_(params.schema_checker_),
    session_info_(params.session_info_),
    params_(params),
    table_schema_(table_schema),
    stmt_(stmt),
    table_item_(table_item),
    dsl_query_info_(table_item.dsl_query_),
    col_schema_map_(),
    col_idx_map_() {}
  virtual ~ObDSLResolver()
  {
    if (col_schema_map_.created()) {
      col_schema_map_.destroy();
    }
    if (col_idx_map_.created()) {
      col_idx_map_.destroy();
    }
  }

  int resolve(const ParseNode &parse_tree);

  // Unified entry point for resolving hybrid_search pseudo columns (_score, aggs buckets, etc.).
  // Returns OB_ERR_BAD_FIELD_ERROR when the column is not a recognized pseudo column.
  static int resolve_hybrid_search_pseudo_column_ref_expr(
      TableItem &table_item,
      const ObQualifiedName &q_name,
      ObDMLStmt &stmt,
      ObRawExprFactory &expr_factory,
      ObSQLSessionInfo *session_info,
      ObRawExpr *&real_ref_expr);

  static int resolve_hybrid_search_score_column_ref_expr(
      const TableItem &table_item,
      const ObQualifiedName &q_name,
      ObDMLStmt &stmt,
      ObRawExpr *&real_ref_expr);

  // Returns OB_ERR_BAD_FIELD_ERROR when the column name is not an aggs bucket name.
  static int resolve_hybrid_search_aggs_bucket_column_ref_expr(
      TableItem &table_item,
      const ObQualifiedName &q_name,
      ObDMLStmt &stmt,
      ObRawExprFactory &expr_factory,
      ObSQLSessionInfo *session_info,
      ObRawExpr *&real_ref_expr);

private:
  static int find_aggs_bucket_item(
      const TableItem &table_item,
      const ObQualifiedName &q_name,
      const ObDMLStmt &stmt,
      ObDSLAggTermsItem *&bucket_item);
  static int resolve_terms_bucket_expr(
      ObDSLAggTermsItem &bucket_item,
      ObRawExpr *&real_ref_expr);
  static int resolve_cardinality_bucket_expr(
      ObDSLAggTermsItem &bucket_item,
      ObDMLStmt &stmt,
      ObRawExprFactory &expr_factory,
      ObSQLSessionInfo *session_info,
      ObRawExpr *&real_ref_expr);
  static int check_hybrid_search_clause_compat(ObSelectStmt *select_stmt);
  static int check_hybrid_search_cardinality_agg(ObSelectStmt *select_stmt);
  static bool is_hybrid_search_count_star_stmt(const ObSelectStmt &select_stmt,
                                                const ObAggFunRawExpr *&aggr_expr);
  static int ignore_count_star_dsl_rewrites(ObSelectStmt &select_stmt,
                                              ObDSLQueryInfo &dsl_query_info);

public:

  // Add `_score` pseudo column to select list for `hybrid_search(table ...)` in `SELECT *` expansion.
  // This function checks if `_score` already exists in target_list, and if not, adds it.
  static int add_hybrid_search_score_to_select_list(
      const TableItem &table_item,
      ObDMLStmt &stmt,
      common::ObIArray<SelectItem> &target_list);

  static int check_hybrid_search_stmt(ObSelectStmt *select_stmt);
  static int register_collapse_exprs(ObSelectStmt *select_stmt);
  static int refresh_result_modes(ObSelectStmt *select_stmt);

  // FROM is resolved before SELECT in MySQL mode, so aggregate items are missing during dsl resolve().
  // Call after select list is resolved to set COUNT_AGG / track_score for scalar count(*).
  static int refresh_result_mode_after_select_resolved(const ObSelectStmt *select_stmt,
                                                       ObDSLQueryInfo *dsl_query_info);
  // for a column expr in scalar filter, whether it could be merged can be easily checked,
  // but for other exprs, we need to check if it is in the whitelist
  inline static bool in_merge_node_whitelist(const ObRawExpr *expr)
  {
    bool in = false;
    if (OB_NOT_NULL(expr) && !expr->is_column_ref_expr()) {
      ObItemType expr_type = expr->get_expr_type();
      switch (expr_type) {
        case T_FUN_SYS_JSON_VALUE:
        case T_FUN_SYS_JSON_EXTRACT:
          in = true;
          break;
        default:
          in = false;
          break;
      }
    }
    return in;
  }
  inline static bool is_scalar_json_type(ObJsonNodeType json_type)
  {
    return json_type == ObJsonNodeType::J_STRING ||
           json_type == ObJsonNodeType::J_BOOLEAN ||
           ObIJsonBase::is_json_number_type(json_type);
  }

  static const int64_t FROM_DEFAULT = 0;
  static const int64_t SIZE_DEFAULT = 10;
  static const int64_t SIZE_VALUE_MIN = 0;
  static const int64_t SIZE_VALUE_MAX = 10000;
  static const int64_t KNN_K_VALUE_MAX = 16384;
  static const int64_t RANK_CONST_DEFAULT = 60;
  static constexpr double MIN_SCORE_DEFAULT = 0.0;
  static const ObString FTS_SCORE_NAME;
  static const ObString VS_SCORE_PREFIX;

private :
  int add_dsl_expr(ObRawExpr *expr);
  int add_dsl_expr_recursive(ObDSLQuery *query);
  int build_array_intersects_expr(ObColumnRefRawExpr *col_expr, const ObIArray<ObRawExpr*> &value_exprs, ObRawExpr *&expr);
  int build_field_expr_with_path(ObColumnRefRawExpr *col_expr, const ObString &path_str, const ObEsQueryItem query_type, ObRawExpr *&field_expr);
  int build_json_contains_scalar_expr(ObRawExpr *target_expr, ObIJsonBase &json_node, ObRawExpr *&expr);
  int build_json_extract_expr(ObRawExpr *col_expr, const ObString &json_path, ObRawExpr *&expr);
  int build_json_overlaps_array_expr(ObRawExpr *target_expr, const ObIArray<ObRawExpr*> &value_exprs, ObRawExpr *&expr);
  int check_fields_collation_types(const ObIArray<ObColumnRefRawExpr*> &fields, bool &compatible);
  int check_fields_parsers(const ObIArray<ObColumnRefRawExpr*> &fields, bool &compatible);
  int collect_exprs();
  int construct_dist_expr(ObColumnRefRawExpr *field_expr, ObRawExpr *vector_expr, ObVectorIndexDistAlgorithm dist_algo, ObRawExpr *&distance_expr);
  int construct_required_params(const char *param_names[], uint32_t name_count, RequiredParamsSet &required_params);
  int construct_rowkey_columns();
  int construct_score_columns();
  int construct_string_expr(const ObString &str_value, ObRawExpr *&expr, ObCollationType collation_type = CS_TYPE_INVALID);
  int convert_wildcard_pattern_to_like(const ObString &src, ObString &dst);
  int append_wildcard_pattern_char(char *buf, const int64_t max_len, int64_t &pos, const char ch);
  int append_wildcard_like_escaped_char(char *buf, const int64_t max_len, int64_t &pos, const char ch);
  int resolve_query_string_expr(const ObString &str_value, const ObCollationType target_coll, ObRawExpr *&expr);
  int formalize_exprs();
  int get_col_idx_info(const ObString &col_name, ObColumnIndexInfo *&idx_info);
  int get_dist_algo_type(ObColumnRefRawExpr *field_expr, ObVectorIndexDistAlgorithm &algo_type);
  int get_field_expr_and_path(const ObString &field_name, ObColumnRefRawExpr *&col_expr, ObString &path_str);
  int get_fulltext_index_schema(const ObString &col_name, const ObTableSchema *&index_schema);
  int get_json_string_from_node(const ParseNode *node, ObString &json_str);
  int get_user_column_expr(ObString &col_name, ObColumnRefRawExpr *&col_expr);
  int init_bool_info(ObIJsonBase &req_node, int32_t &msm, ObConstRawExpr *&boost_expr);
  int init_col_idx_map();
  int init_col_schema_map();
  int init_resolver();
  int is_array_column(ObColumnRefRawExpr *col_expr, bool &is_array_col);
  int print_json_node(ObIJsonBase &node, ObJsonBuffer &j_buf);
  int check_aggs_bucket_name_conflict(const ObString &agg_name, bool &is_conflict);
  int resolve_array_contains(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_array_contains_all(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_array_expr(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type, ObEsQueryItem query_type);
  int resolve_array_overlaps(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_bool(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_bool_clause(ObIJsonBase &req_node, ObIArray<ObDSLQuery*> &queries, int64_t &count, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_boost(ObIJsonBase &req_node, ObConstRawExpr *&boost_expr, ObEsQueryItem query_type, ObEsQueryItem outer_query_type);
  int resolve_const(ObIJsonBase &req_node, ObRawExpr *&expr, ObJsonNodeType target_type, ObEsQueryItem query_type = QUERY_ITEM_UNKNOWN);
  int resolve_default_params(ObIJsonBase &req_node);
  int resolve_field(ObIJsonBase &req_node, ObColumnRefRawExpr *&col_expr, ObConstRawExpr *&boost_expr);
  int resolve_from(ObIJsonBase &req_node);
  int resolve_json_contains(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_json_expr(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type, ObEsQueryItem query_type);
  int resolve_json_member_of(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_json_overlaps(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_knn(ObIJsonBase &req_node, ObDSLQuery *&query);
  int resolve_match(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_match_phrase(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_min_score(ObIJsonBase &req_node);
  int resolve_minimum_should_match(ObIJsonBase &req_node, int32_t &msm);
  int resolve_multi_knn(ObIJsonBase &req_node);
  int resolve_multi_match(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_multi_fields_query_param(
      ObIJsonBase &req_node,
      const bool is_multi_match,
      ObDSLFullTextMultiFieldQueryParam &fields_param,
      RequiredParamsSet &required_params,
      hash::ObHashSet<int32_t> &resolved_field_idx_set);
  int resolve_query(ObIJsonBase &req_node);
  int resolve_query_search_options(ObIJsonBase &req_node);
  int resolve_query_string(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_query_string_fields(ObIJsonBase &req_node, ObIArray<ObColumnRefRawExpr*> &fields, ObIArray<ObConstRawExpr*> &field_boosts, bool &compatible);
  int resolve_query_string_operator(ObIJsonBase &req_node, ObMatchOperator &opr);
  int resolve_query_string_query(ObIJsonBase &req_node, ObConstRawExpr *&query_expr, ObCollationType collation_type);
  int resolve_query_string_type(ObIJsonBase &req_node, ObMatchFieldsType &type);
  int resolve_range(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_rank(ObIJsonBase &req_node);
  int resolve_rrf(ObIJsonBase &req_node);
  int resolve_search_options(ObIJsonBase &req_node, ObDSLKnnQuery::SearchOption *&search_option);
  int resolve_single_term(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int dispatch_query_type(const ObString &key, ObIJsonBase &sub_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_size(ObIJsonBase &req_node);
  int resolve_sort_item(ObIJsonBase &sort_item, ObSelectStmt &select_stmt);
  int resolve_sort_string_item(const ObString &field_name, ObSelectStmt &select_stmt);
  int resolve_sort_object_item(ObIJsonBase &sort_item, ObSelectStmt &select_stmt);
  int resolve_sort_options(ObIJsonBase *field_opts, ObDSLSortItem &dsl_sort_item);
  int resolve_sort_field(ObString field_name, bool validate_sort_type, ObDSLSortItem &dsl_sort_item);
  int resolve_sort_missing_literal(ObIJsonBase &missing_node, ObDSLSortItem &dsl_sort_item);
  int build_sort_missing_string_literal(ObIJsonBase &missing_node, ObDSLSortItem &dsl_sort_item);
  int build_sort_missing_temporal_literal(ObIJsonBase &missing_node, ObDSLSortItem &dsl_sort_item);
  int build_sort_expr(const ObDSLSortItem &dsl_sort_item, ObRawExpr *&sort_expr);
  int add_sort_order_item(ObSelectStmt &select_stmt, const ObDSLSortItem &dsl_sort_item);
  int resolve_slop(ObIJsonBase &req_node, int32_t &slop);
  int resolve_term(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_terms(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_wildcard(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_weighted_sum(ObIJsonBase &req_node);
  int resolve_sort(ObIJsonBase &req_node);
  int init_result_mode_and_track_score();
  static void set_track_score(ObDSLQueryInfo *dsl_query_info);
  int setup_top_level_score(ObDSLQuery *query);
  int set_default_rank_window_size();
  int trim_strtod(const ObString &num_str, double &num_val);
  int try_push_nested_boost_to_leaf_query(ObDSLQuery *query, const double cumulative_boost);

  // collapse
  int resolve_collapse(ObIJsonBase &req_node);
  int inject_collapse_stmt_rewrites();

  // aggs
  int resolve_aggs(ObIJsonBase &aggs_node);
  int resolve_agg_terms(const ObString &agg_name, ObIJsonBase &terms_node);
  int resolve_agg_cardinality(const ObString &agg_name, ObIJsonBase &cardinality_node);
  int inject_agg_stmt_rewrites();

  inline ObConstRawExpr *setup_boost(ObConstRawExpr *boost_expr)
  { return (boost_expr != nullptr) ? boost_expr : static_cast<ObConstRawExpr*>(dsl_query_info_->one_const_expr_); }
  inline void setup_column_expr_attr(ObColumnRefRawExpr *col_expr)
  {
    col_expr->set_ref_id(table_item_.table_id_, col_expr->get_column_id());
    col_expr->set_column_attr(table_schema_->get_table_name_str(), col_expr->get_column_name());
    col_expr->set_lob_column(col_expr->get_result_type().is_lob_storage());
    col_expr->set_database_name(table_item_.database_name_);
  }
  static int set_const_long_text_prefix_len(ObRawExpr *src_expr, ObIArray<ObRawExpr*> &longtext_exprs, ObIArray<int32_t> &origin_lens);
  static bool is_groupable_type(ObObjType type);
  static int set_stmt_limit_offset(ObDSLQueryInfo *dsl_query_info, ObSelectStmt &select_stmt);

  ObIAllocator *allocator_;
  ObSchemaChecker *schema_checker_;
  ObSQLSessionInfo *session_info_;
  ObResolverParams &params_;
  const ObTableSchema *table_schema_;
  ObDMLStmt *stmt_;
  TableItem &table_item_;
  ObDSLQueryInfo *&dsl_query_info_;     // output of resolver
  ColumnSchemaMap col_schema_map_;
  ColumnIndexNameMap col_idx_map_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_RESOLVER_H_ */
