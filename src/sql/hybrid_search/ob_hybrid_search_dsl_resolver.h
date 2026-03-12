/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_RESOLVER_H_
#define OCEANBASE_SQL_HYBRID_SEARCH_OB_HYBRID_SEARCH_DSL_RESOLVER_H_

#include "share/hybrid_search/ob_query_parse.h"

namespace oceanbase
{
namespace sql
{
class ObDSLFullTextQuery;
struct ObDSLFullTextMultiFieldQueryParam;

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
    if (IS_QUERY_ITEM_SCALAR(query_type_) || IS_QUERY_ITEM_JSON(query_type_) || IS_QUERY_ITEM_ARRAY(query_type_)) {
      return false;
    } else if (outer_query_type_ == QUERY_ITEM_FILTER || outer_query_type_ == QUERY_ITEM_MUST_NOT) {
      return false;
    } else if (parent_query_ != nullptr && !parent_query_->need_cal_score_) {
      return false;
    }
    return true;
  }
  inline static bool check_need_cal_score_in_bool(ObEsQueryItem outer_query_type, ObDSLQuery *parent_query)
  {
    if (outer_query_type != QUERY_ITEM_MUST && outer_query_type != QUERY_ITEM_SHOULD) {
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
      : param_(), filter_mode_(INVALID_KNN_FILTER_MODE) {}
    ~SearchOption() {}
    ObVectorIndexQueryParam param_;
    ObKnnFilterMode filter_mode_;  // ["pre", "pre-knn", "pre-brute", "post", "post-index-merge"]
    TO_STRING_KV(K(param_), K(filter_mode_));
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

struct ObDSLQueryInfo
{
  ObDSLQueryInfo()
    : queries_(), from_(nullptr), size_(nullptr), min_score_(nullptr), raw_dsl_param_str_(), is_top_k_query_(true) {}
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
  int init_default_params(ObRawExprFactory &expr_factory, bool is_top_k_query = true);

  ObSEArray<ObDSLQuery*, 4, ModulePageAllocator, true> queries_;
  ObRawExpr *from_;
  ObRawExpr *size_;
  ObRawExpr *min_score_;
  ObRawExpr *one_const_expr_;
  ObDSLRankInfo rank_info_;
  ObSEArray<ObColumnRefRawExpr*, 4, ModulePageAllocator, true> rowkey_cols_;
  ObSEArray<ObColumnRefRawExpr*, 4, ModulePageAllocator, true> dsl_cols;
  ObSEArray<ObOpPseudoColumnRawExpr*, 4, ModulePageAllocator, true> score_cols_;
  ObSEArray<ObRawExpr*, 4, ModulePageAllocator, true> dsl_exprs_;
  ObString raw_dsl_param_str_;
  bool is_top_k_query_;
  TO_STRING_KV(K(queries_), K(from_), K(size_), K(min_score_),
               K(rank_info_), K(rowkey_cols_), K(dsl_cols), K(score_cols_),
               K(dsl_exprs_), K(raw_dsl_param_str_), K(is_top_k_query_));
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

  // Resolve `_score` pseudo column for `hybrid_search(table ...)`.
  // Return handled=true when this function recognizes and resolves the column, and sets real_ref_expr.
  // Return handled=false when the column should be resolved by normal column resolution paths.
  static int resolve_hybrid_search_score_column_ref_expr(
      const TableItem &table_item,
      const ObQualifiedName &q_name,
      ObDMLStmt &stmt,
      ObRawExpr *&real_ref_expr);

  // Add `_score` pseudo column to select list for `hybrid_search(table ...)` in `SELECT *` expansion.
  // This function checks if `_score` already exists in target_list, and if not, adds it.
  static int add_hybrid_search_score_to_select_list(
      const TableItem &table_item,
      ObDMLStmt &stmt,
      common::ObIArray<SelectItem> &target_list);
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
  int check_fields_collation_types(const ObIArray<ObColumnRefRawExpr*> &fields, bool &compatible);
  int check_fields_parsers(const ObIArray<ObColumnRefRawExpr*> &fields, bool &compatible);
  int collect_exprs();
  int construct_dist_expr(ObColumnRefRawExpr *field_expr, ObRawExpr *vector_expr, ObVectorIndexDistAlgorithm dist_algo, ObRawExpr *&distance_expr);
  int construct_required_params(const char *param_names[], uint32_t name_count, RequiredParamsSet &required_params);
  int construct_rowkey_columns();
  int construct_score_columns();
  int construct_string_expr(const ObString &str_value, ObRawExpr *&expr, ObCollationType collation_type = CS_TYPE_INVALID);
  int resolve_query_string_expr(const ObString &str_value, const ObCollationType target_coll, ObRawExpr *&expr);
  int formalize_exprs();
  int get_dist_algo_type(ObColumnRefRawExpr *field_expr, ObVectorIndexDistAlgorithm &algo_type);
  int get_field_expr_and_path(const ObString &field_name, ObColumnRefRawExpr *&col_expr, ObString &path_str);
  int get_fulltext_index_schema(const ObString &field_name, const ObTableSchema *&index_schema);
  int get_json_string_from_node(const ParseNode *node, ObString &json_str);
  int get_user_column_expr(const ObString &col_name, ObColumnRefRawExpr *&col_expr);
  int init_bool_info(ObIJsonBase &req_node, int32_t &msm, ObConstRawExpr *&boost_expr);
  int init_col_idx_map();
  int init_col_schema_map();
  int init_resolver();
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
  int resolve_size(ObIJsonBase &req_node);
  int resolve_slop(ObIJsonBase &req_node, int32_t &slop);
  int resolve_term(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_terms(ObIJsonBase &req_node, ObDSLQuery *&query, ObDSLQuery *parent_query, ObEsQueryItem outer_query_type);
  int resolve_weighted_sum(ObIJsonBase &req_node);
  int setup_top_level_score(ObDSLQuery *query);
  int set_default_rank_window_size();
  int trim_strtod(const ObString &num_str, double &num_val);
  int try_push_nested_boost_to_leaf_query(ObDSLQuery *query, const double cumulative_boost);
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
