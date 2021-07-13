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

#ifndef OCEANBASE_SQL_SELECTSTMT_H_
#define OCEANBASE_SQL_SELECTSTMT_H_

#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_vector.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/ob_sql_temp_table.h"

namespace oceanbase {
namespace sql {
enum SelectTypeAffectFoundRows { AFFECT_FOUND_ROWS, NOT_AFFECT_FOUND_ROWS };

struct SelectItem {
  SelectItem()
      : expr_(NULL),
        is_real_alias_(false),
        alias_name_(),
        paramed_alias_name_(),
        expr_name_(),
        default_value_(),
        default_value_expr_(NULL),
        questions_pos_(),
        params_idx_(),
        neg_param_idx_(),
        esc_str_flag_(false),
        need_check_dup_name_(false),
        implicit_filled_(false),
        is_unpivot_mocked_column_(false),
        is_implicit_added_(false),
        is_hidden_rowid_(false)
  {}
  void reset()
  {
    expr_ = NULL;
    default_value_expr_ = NULL;
    is_real_alias_ = false;
    alias_name_.reset();
    paramed_alias_name_.reset();
    expr_name_.reset();
    default_value_.reset();
    questions_pos_.reset();
    params_idx_.reset();
    neg_param_idx_.reset();
    esc_str_flag_ = false;
    need_check_dup_name_ = false;
    implicit_filled_ = false;
    is_unpivot_mocked_column_ = false;
    is_implicit_added_ = false;
    is_hidden_rowid_ = false;
  }

  void reset_param_const_infos()
  {
    questions_pos_.reset();
    params_idx_.reset();
    neg_param_idx_.reset();
    esc_str_flag_ = false;
    need_check_dup_name_ = false;
  }
  int deep_copy(ObRawExprFactory& expr_factory, const SelectItem& other, const uint64_t copy_types);
  TO_STRING_KV(N_EXPR, expr_, N_IS_ALIAS, is_real_alias_, N_ALIAS_NAME, alias_name_, N_EXPR_NAME, expr_name_, N_DEFAULT,
      default_value_, K_(paramed_alias_name), K_(questions_pos), K_(params_idx), K_(esc_str_flag),
      K_(need_check_dup_name), K_(implicit_filled), K_(is_unpivot_mocked_column), K_(is_hidden_rowid));

  ObRawExpr* expr_;
  bool is_real_alias_;
  common::ObString alias_name_;
  common::ObString paramed_alias_name_;
  common::ObString expr_name_;
  common::ObObj default_value_;
  ObRawExpr* default_value_expr_;

  common::ObSEArray<int64_t, OB_DEFAULT_SE_ARRAY_COUNT> questions_pos_;
  common::ObSEArray<int64_t, OB_DEFAULT_SE_ARRAY_COUNT> params_idx_;
  common::ObBitSet<> neg_param_idx_;
  bool esc_str_flag_;
  bool need_check_dup_name_;
  // select item is implicit filled in updatable view, to pass base table's column to top view.
  bool implicit_filled_;
  bool is_unpivot_mocked_column_;  // used for unpivot
  bool is_implicit_added_;         // used for temporary table at insert resolver

  bool is_hidden_rowid_;
};

struct ObSelectIntoItem {
  ObSelectIntoItem()
      : into_type_(),
        outfile_name_(),
        filed_str_(),
        line_str_(),
        user_vars_(),
        pl_vars_(),
        closed_cht_(DEFAULT_FIELD_ENCLOSED_CHAR),
        is_optional_(DEFAULT_OPTIONAL_ENCLOSED)
  {
    filed_str_.set_varchar(DEFAULT_FIELD_TERM_STR);
    filed_str_.set_collation_type(ObCharset::get_system_collation());
    line_str_.set_varchar(DEFAULT_LINE_TERM_STR);
    line_str_.set_collation_type(ObCharset::get_system_collation());
  }
  int assign(const ObSelectIntoItem& other)
  {
    into_type_ = other.into_type_;
    outfile_name_ = other.outfile_name_;
    filed_str_ = other.filed_str_;
    line_str_ = other.line_str_;
    closed_cht_ = other.closed_cht_;
    is_optional_ = other.is_optional_;
    return user_vars_.assign(other.user_vars_);
  }
  TO_STRING_KV(K_(into_type), K_(outfile_name), K_(filed_str), K_(line_str), K_(closed_cht), K_(is_optional));
  ObItemType into_type_;
  common::ObObj outfile_name_;
  common::ObObj filed_str_;                            // filed terminated str
  common::ObObj line_str_;                             // line teminated str
  common::ObSEArray<common::ObString, 16> user_vars_;  // user variables
  common::ObSEArray<sql::ObRawExpr*, 16> pl_vars_;     // pl variables
  char closed_cht_;                                    // all fileds, "123","ab"
  bool is_optional_;                                   //  for string, closed character, such as "aa"

  static const char* const DEFAULT_FIELD_TERM_STR;
  static const char* const DEFAULT_LINE_TERM_STR;
  static const char DEFAULT_FIELD_ENCLOSED_CHAR;
  static const bool DEFAULT_OPTIONAL_ENCLOSED;
};

struct ObGroupbyExpr {
  ObGroupbyExpr() : groupby_exprs_()
  {}
  int assign(const ObGroupbyExpr& other)
  {
    return groupby_exprs_.assign(other.groupby_exprs_);
  }

  TO_STRING_KV("grouping sets groupby expr", groupby_exprs_);
  common::ObSEArray<sql::ObRawExpr*, 8, common::ModulePageAllocator, true> groupby_exprs_;
};

struct ObMultiRollupItem {
  ObMultiRollupItem() : rollup_list_exprs_()
  {}
  int assign(const ObMultiRollupItem& other)
  {
    return rollup_list_exprs_.assign(other.rollup_list_exprs_);
  }
  int deep_copy(ObRawExprFactory& expr_factory, const ObMultiRollupItem& other, const uint64_t copy_types);
  TO_STRING_KV("rollup list exprs", rollup_list_exprs_);
  common::ObSEArray<ObGroupbyExpr, 8, common::ModulePageAllocator, true> rollup_list_exprs_;
};

struct ObGroupingSetsItem {
  ObGroupingSetsItem() : grouping_sets_exprs_(), multi_rollup_items_()
  {}
  int assign(const ObGroupingSetsItem& other);
  int deep_copy(ObRawExprFactory& expr_factory, const ObGroupingSetsItem& other, const uint64_t copy_types);
  TO_STRING_KV("grouping sets exprs", grouping_sets_exprs_, K_(multi_rollup_items));
  common::ObSEArray<ObGroupbyExpr, 8, common::ModulePageAllocator, true> grouping_sets_exprs_;
  common::ObSEArray<ObMultiRollupItem, 8, common::ModulePageAllocator, true> multi_rollup_items_;
};

}  // namespace sql

namespace common {
template <>
struct ob_vector_traits<oceanbase::sql::SelectItem> {
  typedef oceanbase::sql::SelectItem* pointee_type;
  typedef oceanbase::sql::SelectItem value_type;
  typedef const oceanbase::sql::SelectItem const_value_type;
  typedef value_type* iterator;
  typedef const value_type* const_iterator;
  typedef int32_t difference_type;
};

template <>
struct ob_vector_traits<oceanbase::sql::FromItem> {
  typedef oceanbase::sql::FromItem* pointee_type;
  typedef oceanbase::sql::FromItem value_type;
  typedef const oceanbase::sql::FromItem const_value_type;
  typedef value_type* iterator;
  typedef const value_type* const_iterator;
  typedef int32_t difference_type;
};
}  // namespace common

namespace sql {
class ObSelectStmt : public ObDMLStmt {
public:
  enum SetOperator {
    NONE = 0,
    UNION,
    INTERSECT,
    EXCEPT,
    RECURSIVE,
    SET_OP_NUM,
  };

  static const char* set_operator_str(SetOperator op)
  {
    static const char* set_operator_name[SET_OP_NUM + 1] = {
        "none",
        "union",
        "intersect",
        "except",
        "recursive",
        "unknown",
    };
    static const char* set_operator_name_oracle[SET_OP_NUM + 1] = {
        "none",
        "union",
        "intersect",
        "minus",
        "unknown",
        "unknown",
    };
    return lib::is_oracle_mode() ? set_operator_name_oracle[op] : set_operator_name[op];
  }

  class ObShowStmtCtx {
  public:
    ObShowStmtCtx()
        : is_from_show_stmt_(false),
          global_scope_(false),
          tenant_id_(common::OB_INVALID_ID),
          show_database_id_(common::OB_INVALID_ID),
          show_table_id_(common::OB_INVALID_ID),
          grants_user_id_(common::OB_INVALID_ID),
          show_seed_(false)
    {}
    virtual ~ObShowStmtCtx()
    {}

    void assign(const ObShowStmtCtx& other)
    {
      is_from_show_stmt_ = other.is_from_show_stmt_;
      global_scope_ = other.global_scope_;
      tenant_id_ = other.tenant_id_;
      show_database_id_ = other.show_database_id_;
      show_table_id_ = other.show_table_id_;
      grants_user_id_ = other.grants_user_id_;
      show_seed_ = other.show_seed_;
    }

    bool is_from_show_stmt_;  // is from show stmt
    bool global_scope_;
    uint64_t tenant_id_;
    uint64_t show_database_id_;
    uint64_t show_table_id_;   // ex: show columns from t1, and show_table_id_ is the table id of t1
    uint64_t grants_user_id_;  // for show grants
    bool show_seed_;           // for show seed parameter

    TO_STRING_KV(K_(is_from_show_stmt), K_(global_scope), K_(tenant_id), K_(show_database_id), K_(show_table_id),
        K_(grants_user_id), K_(show_seed));
  };

  ObSelectStmt();
  virtual ~ObSelectStmt();
  int assign(const ObSelectStmt& other);
  virtual int replace_inner_stmt_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& exprs) override;
  int update_stmt_table_id(const ObSelectStmt& other);
  int64_t get_select_item_size() const
  {
    return select_items_.count();
  }
  int64_t get_group_expr_size() const
  {
    return group_exprs_.count();
  }
  int64_t get_rollup_expr_size() const
  {
    return rollup_exprs_.count();
  }
  int64_t get_grouping_sets_items_size() const
  {
    return grouping_sets_items_.count();
  }
  int64_t get_multi_rollup_items_size() const
  {
    return multi_rollup_items_.count();
  }
  int64_t get_rollup_dir_size() const
  {
    return rollup_directions_.count();
  }
  int64_t get_aggr_item_size() const
  {
    return agg_items_.count();
  }
  int64_t get_having_expr_size() const
  {
    return having_exprs_.count();
  }
  void set_recursive_union(bool is_recursive_union)
  {
    is_recursive_cte_ = is_recursive_union;
  }
  void set_breadth_strategy(bool is_breadth_search)
  {
    is_breadth_search_ = is_breadth_search;
  }
  void assign_distinct()
  {
    is_distinct_ = true;
  }
  void assign_all()
  {
    is_distinct_ = false;
  }
  void assign_set_op(SetOperator op)
  {
    set_op_ = op;
  }
  void assign_set_distinct()
  {
    is_set_distinct_ = true;
  }
  void assign_set_all()
  {
    is_set_distinct_ = false;
  }
  void set_parent_set_distinct(const bool is_parent_set_distinct)
  {
    is_parent_set_distinct_ = is_parent_set_distinct;
  }
  void assign_rollup()
  {
    has_rollup_ = true;
  }
  void reassign_rollup()
  {
    has_rollup_ = false;
  }
  void assign_grouping()
  {
    has_grouping_ = true;
  }
  void reassign_grouping()
  {
    has_grouping_ = false;
  }
  void assign_grouping_sets()
  {
    has_grouping_sets_ = true;
  }
  void reassign_grouping_sets()
  {
    has_grouping_sets_ = false;
  }
  void set_is_from_show_stmt(bool is_from_show_stmt)
  {
    show_stmt_ctx_.is_from_show_stmt_ = is_from_show_stmt;
  }
  void set_global_scope(bool global_scope)
  {
    show_stmt_ctx_.global_scope_ = global_scope;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    show_stmt_ctx_.tenant_id_ = tenant_id;
  }
  void set_show_seed(bool show_seed)
  {
    show_stmt_ctx_.show_seed_ = show_seed;
  }
  void set_show_database_id(uint64_t show_database_id)
  {
    show_stmt_ctx_.show_database_id_ = show_database_id;
  }
  void set_show_table_id(uint64_t show_table_id)
  {
    show_stmt_ctx_.show_table_id_ = show_table_id;
  }
  void set_show_grants_user_id(uint64_t user_id)
  {
    show_stmt_ctx_.grants_user_id_ = user_id;
  }
  void set_select_into(ObSelectIntoItem* into_item)
  {
    into_item_ = into_item;
  }
  uint64_t get_show_grants_user_id()
  {
    return show_stmt_ctx_.grants_user_id_;
  }
  int check_alias_name(ObStmtResolver& ctx, const common::ObString& alias) const;
  int check_using_column(ObStmtResolver& ctx, const common::ObString& column_name) const;
  bool get_global_scope() const
  {
    return show_stmt_ctx_.global_scope_;
  }
  uint64_t get_tenant_id() const
  {
    return show_stmt_ctx_.tenant_id_;
  }
  bool get_show_seed() const
  {
    return show_stmt_ctx_.show_seed_;
  }
  uint64_t get_show_database_id() const
  {
    return show_stmt_ctx_.show_database_id_;
  }
  uint64_t get_show_table_id() const
  {
    return show_stmt_ctx_.show_table_id_;
  }
  bool is_distinct() const
  {
    return is_distinct_;
  }
  bool is_recursive_union() const
  {
    return is_recursive_cte_;
  }
  bool is_breadth_search() const
  {
    return is_breadth_search_;
  }
  bool is_parent_set_distinct() const
  {
    return is_parent_set_distinct_;
  }
  bool is_set_distinct() const
  {
    return is_set_distinct_;
  }
  bool is_from_show_stmt() const
  {
    return show_stmt_ctx_.is_from_show_stmt_;
  }
  // view
  void set_is_view_stmt(bool is_view_stmt, uint64_t view_ref_id)
  {
    is_view_stmt_ = is_view_stmt;
    view_ref_id_ = view_ref_id;
  }
  bool is_view_stmt() const
  {
    return is_view_stmt_;
  }
  uint64_t get_view_ref_id() const
  {
    return view_ref_id_;
  }
  bool has_select_into() const
  {
    return into_item_ != NULL;
  }
  bool is_select_into_outfile() const
  {
    return has_select_into() && into_item_->into_type_ == T_INTO_OUTFILE;
  }
  bool has_materalized_view() const
  {
    return common::OB_INVALID_ID != base_table_id_ && common::OB_INVALID_ID != depend_table_id_;
  }
  // check if the stmt is a Select-Project-Join(SPJ) query
  // stmt can't be unnested if contain assignment user variable expr
  bool is_spj() const;

  inline bool has_set_op() const
  {
    return NONE != get_set_op();
  }
  ObRawExpr* get_expr(uint64_t expr_id);
  /* use table name instead of alias name */
  void normalize_index_hint();
  inline bool is_single_table_stmt() const
  {
    return (1 == get_table_size() && 1 == get_from_item_size());
  }
  inline bool has_group_by() const
  {
    return (get_group_expr_size() > 0 || get_rollup_expr_size() > 0 || get_aggr_item_size() > 0 ||
               get_grouping_sets_items_size() > 0) ||
           get_multi_rollup_items_size() > 0;
  }
  inline bool is_scala_group_by() const
  {
    return get_group_expr_size() == 0 && get_rollup_expr_size() == 0 && get_grouping_sets_items_size() == 0 &&
           get_multi_rollup_items_size() == 0 && get_aggr_item_size() > 0;
  }

  inline bool has_hierarchical_query() const
  {
    return is_hierarchical_query_;
  }
  inline bool has_recusive_cte() const
  {
    return is_recursive_cte_;
  }
  // return single row
  inline bool is_single_set_query() const
  {
    return (get_aggr_item_size() > 0 && group_exprs_.empty() && rollup_exprs_.empty() && grouping_sets_items_.empty() &&
            multi_rollup_items_.empty());
  }
  inline bool has_rollup() const
  {
    return has_rollup_;
  };
  inline bool has_grouping() const
  {
    return has_grouping_;
  };
  inline bool has_grouping_sets() const
  {
    return has_grouping_sets_;
  }
  inline bool has_order_by() const
  {
    return (get_order_item_size() > 0);
  }
  inline bool has_distinct() const
  {
    return is_distinct();
  }
  inline bool has_having() const
  {
    return (get_having_expr_size() > 0);
  }
  inline bool has_rollup_dir() const
  {
    return (get_rollup_dir_size() > 0);
  }
  SetOperator get_set_op() const
  {
    return set_op_;
  }
  void set_from_pivot(const bool value)
  {
    is_from_pivot_ = value;
  }
  bool is_from_pivot() const
  {
    return is_from_pivot_;
  }
  void set_hidden_rowid(const bool value)
  {
    has_hidden_rowid_ = value;
  }
  bool has_hidden_rowid() const
  {
    return has_hidden_rowid_;
  }
  bool is_temp_table() const
  {
    return NULL != temp_table_info_;
  }
  void set_temp_table_info(ObSqlTempTableInfo* temp_table_info)
  {
    temp_table_info_ = temp_table_info;
  }
  ObSqlTempTableInfo* get_temp_table_info()
  {
    return temp_table_info_;
  }
  bool need_temp_table_trans() const
  {
    return need_temp_table_trans_;
  }
  void set_need_temp_table_trans(bool need_temp_table_trans)
  {
    need_temp_table_trans_ = need_temp_table_trans;
  }
  bool is_last_access() const
  {
    return is_last_access_;
  }
  void set_is_last_access(bool is_last_access)
  {
    is_last_access_ = is_last_access;
  }

  virtual int has_special_expr(const ObExprInfoFlag, bool& has) const override;
  virtual int clear_sharable_expr_reference() override;
  virtual int remove_useless_sharable_expr() override;

  const common::ObIArray<OrderItem>& get_search_by_items() const
  {
    return search_by_items_;
  }
  const common::ObIArray<ColumnItem>& get_cycle_items() const
  {
    return cycle_by_items_;
  }

  const SelectItem& get_select_item(int64_t index) const
  {
    return select_items_[index];
  }
  SelectItem& get_select_item(int64_t index)
  {
    return select_items_[index];
  }
  common::ObIArray<SelectItem>& get_select_items()
  {
    return select_items_;
  }
  const common::ObIArray<SelectItem>& get_select_items() const
  {
    return select_items_;
  }
  int get_select_exprs(ObIArray<ObRawExpr*>& select_exprs, const bool is_for_outout = false);
  int get_select_exprs(ObIArray<ObRawExpr*>& select_exprs, const bool is_for_outout = false) const;
  int inner_get_share_exprs(ObIArray<ObRawExpr*>& candi_share_exprs) const;
  const common::ObIArray<ObAggFunRawExpr*>& get_aggr_items() const
  {
    return agg_items_;
  }
  common::ObIArray<ObAggFunRawExpr*>& get_aggr_items()
  {
    return agg_items_;
  }
  const ObAggFunRawExpr* get_aggr_item(int64_t index) const
  {
    return agg_items_[index];
  }
  ObAggFunRawExpr* get_aggr_item(int64_t index)
  {
    return agg_items_[index];
  }
  common::ObIArray<ObRawExpr*>& get_having_exprs()
  {
    return having_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_having_exprs() const
  {
    return having_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_start_with_exprs()
  {
    return start_with_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_start_with_exprs() const
  {
    return start_with_exprs_;
  }
  void reset_start_with_exprs()
  {
    start_with_exprs_.reset();
  }
  common::ObIArray<ObRawExpr*>& get_connect_by_exprs()
  {
    return connect_by_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_connect_by_exprs() const
  {
    return connect_by_exprs_;
  }
  int get_connect_by_root_exprs(common::ObIArray<ObRawExpr*>& connect_by_root_exprs) const;
  int get_sys_connect_by_path_exprs(common::ObIArray<ObRawExpr*>& sys_connect_by_path_exprs) const;
  int recursive_get_expr(ObRawExpr* expr, common::ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag target_flag,
      ObExprInfoFlag search_flag) const;
  common::ObIArray<ObRawExpr*>& get_cte_exprs()
  {
    return cte_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_cte_exprs() const
  {
    return cte_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_connect_by_prior_exprs()
  {
    return connect_by_prior_exprs_;
  }
  int add_connect_by_prior_expr(ObRawExpr* expr)
  {
    return connect_by_prior_exprs_.push_back(expr);
  }
  void clear_connect_by_exprs()
  {
    connect_by_exprs_.reset();
  }
  void set_nocycle(bool is_nocycle)
  {
    is_nocycle_ = is_nocycle;
  }
  bool is_nocycle() const
  {
    return is_nocycle_;
  }
  const common::ObIArray<ObRawExpr*>& get_group_exprs() const
  {
    return group_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_rollup_exprs() const
  {
    return rollup_exprs_;
  }
  const common::ObIArray<ObGroupingSetsItem>& get_grouping_sets_items() const
  {
    return grouping_sets_items_;
  }
  common::ObIArray<ObGroupingSetsItem>& get_grouping_sets_items()
  {
    return grouping_sets_items_;
  }
  const common::ObIArray<ObMultiRollupItem>& get_multi_rollup_items() const
  {
    return multi_rollup_items_;
  }
  common::ObIArray<ObMultiRollupItem>& get_multi_rollup_items()
  {
    return multi_rollup_items_;
  }
  bool is_expr_in_groupings_sets_item(const ObRawExpr* expr) const;
  bool is_expr_in_multi_rollup_items(const ObRawExpr* expr) const;
  const common::ObIArray<ObOrderDirection>& get_rollup_dirs() const
  {
    return rollup_directions_;
  }
  common::ObIArray<ObOrderDirection>& get_rollup_dirs()
  {
    return rollup_directions_;
  }
  ObSelectIntoItem* get_select_into() const
  {
    return into_item_;
  }
  int adjust_view_parent_namespace_stmt(ObDMLStmt* new_parent);
  common::ObIArray<ObRawExpr*>& get_group_exprs()
  {
    return const_cast<common::ObIArray<ObRawExpr*>&>((static_cast<const ObSelectStmt&>(*this)).get_group_exprs());
  }
  common::ObIArray<ObRawExpr*>& get_rollup_exprs()
  {
    return const_cast<common::ObIArray<ObRawExpr*>&>((static_cast<const ObSelectStmt&>(*this)).get_rollup_exprs());
  }
  int add_group_expr(ObRawExpr* expr)
  {
    return group_exprs_.push_back(expr);
  }
  int add_rollup_expr(ObRawExpr* expr)
  {
    return rollup_exprs_.push_back(expr);
  }
  int add_grouping_sets_item(ObGroupingSetsItem& grouping_sets_item)
  {
    return grouping_sets_items_.push_back(grouping_sets_item);
  }
  int add_rollup_item(ObMultiRollupItem& rollup_item)
  {
    return multi_rollup_items_.push_back(rollup_item);
  }
  int add_rollup_dir(ObOrderDirection dir)
  {
    return rollup_directions_.push_back(dir);
  }
  int add_agg_item(ObAggFunRawExpr& agg_expr)
  {
    agg_expr.set_explicited_reference();
    agg_expr.set_expr_level(current_level_);
    return agg_items_.push_back(&agg_expr);
  }
  int add_having_expr(ObRawExpr* expr)
  {
    return having_exprs_.push_back(expr);
  }
  bool has_for_update() const;
  ObIArray<ObColumnRefRawExpr*>& get_for_update_columns()
  {
    return for_update_columns_;
  }
  const ObIArray<ObColumnRefRawExpr*>& get_for_update_columns() const
  {
    return for_update_columns_;
  }
  virtual bool is_affect_found_rows() const
  {
    bool ret = false;
    if (select_type_ == AFFECT_FOUND_ROWS) {
      ret = true;
    } else {
      ret = false;
    }
    return ret;
  }
  virtual bool has_link_table() const
  {
    bool bret = ObDMLStmt::has_link_table();
    for (int64_t i = 0; !bret && i < set_query_.count(); i++) {
      if (OB_NOT_NULL(set_query_.at(i))) {
        bret = set_query_.at(i)->has_link_table();
      }
    }
    return bret;
  }
  void set_select_type(SelectTypeAffectFoundRows type)
  {
    select_type_ = type;
  }
  int check_having_ident(
      ObStmtResolver& ctx, common::ObString& column_name, TableItem* table_item, ObColumnRefRawExpr& ret_expr) const;
  int add_select_item(SelectItem& item);
  void clear_select_item()
  {
    select_items_.reset();
  }
  void clear_aggr_item()
  {
    agg_items_.reset();
  }
  int reset_select_item(const common::ObIArray<SelectItem>& sorted_select_items);
  //@hualong unused code
  // static  const char *get_set_op_type_str(SetOperator set_op);
  DECLARE_VIRTUAL_TO_STRING;
  int do_to_string(char* buf, const int64_t buf_len, int64_t& pos) const;
  /**
   * compare with another select stmt
   * all members must be equal
   * @param[in] stmt                another select stmt
   * @return                        true equal, false not equal
   */
  bool equals(const ObSelectStmt& stmt);
  int check_and_get_same_aggr_item(ObRawExpr* expr, ObAggFunRawExpr*& same_aggr);
  ObWinFunRawExpr* get_same_win_func_item(const ObRawExpr* expr);
  bool is_mix_of_group_func_and_fileds() const;
  // replace expression from %from to %to, only replace expr pointer of stmt,
  // will not recursive replace of expr
  virtual int replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to);
  void set_star_select()
  {
    is_select_star_ = true;
  }
  void set_star_select(const bool is_select_star)
  {
    is_select_star_ = is_select_star;
  }
  bool is_star_select() const
  {
    return is_select_star_;
  }
  void set_match_topk(bool is_match)
  {
    is_match_topk_ = is_match;
  }
  bool is_match_topk() const
  {
    return is_match_topk_;
  }
  virtual bool is_set_stmt() const
  {
    return NONE != set_op_;
  }
  int get_child_stmt_size(int64_t& child_size) const;
  int get_child_stmts(common::ObIArray<ObSelectStmt*>& child_stmts) const;
  int set_child_stmt(const int64_t child_num, ObSelectStmt* child_stmt);
  int get_from_subquery_stmts(common::ObIArray<ObSelectStmt*>& child_stmts) const;
  const common::ObIArray<ObWinFunRawExpr*>& get_window_func_exprs() const
  {
    return win_func_exprs_;
  };
  common::ObIArray<ObWinFunRawExpr*>& get_window_func_exprs()
  {
    return win_func_exprs_;
  };
  bool has_window_function() const
  {
    return win_func_exprs_.count() != 0;
  }
  int add_window_func_expr(ObWinFunRawExpr* expr);
  const ObWinFunRawExpr* get_window_func_expr(int64_t i) const
  {
    return win_func_exprs_.at(i);
  }
  ObWinFunRawExpr* get_window_func_expr(int64_t i)
  {
    return win_func_exprs_.at(i);
  }
  int64_t get_window_func_count() const
  {
    return win_func_exprs_.count();
  }
  int remove_window_func_expr(ObWinFunRawExpr* expr);
  // for materialized view
  uint64_t get_base_table_id()
  {
    return base_table_id_;
  }
  uint64_t get_depend_table_id()
  {
    return depend_table_id_;
  }
  void set_base_table_id(uint64_t table_id)
  {
    base_table_id_ = table_id;
  }
  void set_depend_table_id(uint64_t table_id)
  {
    depend_table_id_ = table_id;
  }
  void set_having_has_self_column()
  {
    having_has_self_column_ = true;
  }
  bool has_having_self_column() const
  {
    return having_has_self_column_;
  }
  void set_children_swapped()
  {
    children_swapped_ = true;
  }
  bool get_children_swapped() const
  {
    return children_swapped_;
  }
  const ObString* get_select_alias(const char* col_name, uint64_t table_id, uint64_t col_id);

  // for cte table
  int add_search_item(const OrderItem& order_item)
  {
    return search_by_items_.push_back(order_item);
  }
  int add_cycle_item(const ColumnItem& col_item)
  {
    return cycle_by_items_.push_back(col_item);
  }
  void set_generated_cte_count()
  {
    generated_cte_count_ = CTE_table_items_.count();
  }
  int64_t get_generated_cte_count() const
  {
    return generated_cte_count_;
  }

  int add_sample_info(const SampleInfo& sample_info)
  {
    return sample_infos_.push_back(sample_info);
  }
  common::ObIArray<SampleInfo>& get_sample_infos()
  {
    return sample_infos_;
  }
  const common::ObIArray<SampleInfo>& get_sample_infos() const
  {
    return sample_infos_;
  }
  const SampleInfo* get_sample_info_by_table_id(uint64_t table_id) const;
  SampleInfo* get_sample_info_by_table_id(uint64_t table_id);
  // check if a table is using sample scan
  bool is_sample_scan(uint64_t table_id) const
  {
    return get_sample_info_by_table_id(table_id) != nullptr;
  }
  virtual bool check_table_be_modified(uint64_t ref_table_id) const;

  // check aggregation has distinct or group concat e.g.:
  //  count(distinct c1)
  //  group_concat(c1 order by c2))
  bool has_distinct_or_concat_agg() const;
  int pullup_stmt_level();
  int set_sharable_exprs_level(int32_t level);
  virtual int get_equal_set_conditions(
      ObIArray<ObRawExpr*>& conditions, const bool is_strict, const int check_scope) override;
  int create_select_list_for_set_stmt(ObRawExprFactory& expr_factory);

  int deep_copy_stmt_struct(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other) override;

  bool contain_nested_aggr() const;

  int get_set_stmt_size(int64_t& size) const;
  common::ObIArray<ObSelectStmt*>& get_set_query()
  {
    return set_query_;
  }
  const common::ObIArray<ObSelectStmt*>& get_set_query() const
  {
    return set_query_;
  }
  int add_set_query(ObSelectStmt* stmt)
  {
    return set_query_.push_back(stmt);
  }
  int set_set_query(const int64_t index, ObSelectStmt* stmt);
  inline ObSelectStmt* get_set_query(const int64_t index) const
  {
    return OB_LIKELY(index >= 0 && index < set_query_.count()) ? set_query_.at(index) : NULL;
  }
  inline ObSelectStmt* get_set_query(const int64_t index)
  {
    return const_cast<ObSelectStmt*>(static_cast<const ObSelectStmt&>(*this).get_set_query(index));
  }
  const ObSelectStmt* get_real_stmt() const;
  ObSelectStmt* get_real_stmt()
  {
    return const_cast<ObSelectStmt*>(static_cast<const ObSelectStmt&>(*this).get_real_stmt());
  }

private:
  int replace_multi_rollup_items_expr(const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs,
      ObIArray<ObMultiRollupItem>& multi_rollup_items);
  int get_relation_exprs_from_multi_rollup_items(
      RelExprCheckerBase& expr_checker, ObIArray<ObMultiRollupItem>& multi_rollup_items);
  int replace_expr_in_multi_rollup_items(
      ObRawExpr* from, ObRawExpr* to, ObIArray<ObMultiRollupItem>& multi_rollup_items);
  int has_special_expr_in_multi_rollup_items(
      const ObIArray<ObMultiRollupItem>& multi_rollup_items, const ObExprInfoFlag flag, bool& has) const;

protected:
  virtual int inner_get_relation_exprs(RelExprCheckerBase& expr_checker);
  virtual int inner_get_relation_exprs_for_wrapper(RelExprChecker& expr_checker)
  {
    return inner_get_relation_exprs(expr_checker);
  }

private:
  SetOperator set_op_;
  /* these var is only used for recursive union */
  bool is_recursive_cte_;
  bool is_breadth_search_;
  int64_t generated_cte_count_;
  /* These fields are only used by normal select */
  bool is_distinct_;
  bool has_rollup_;
  bool has_grouping_;
  bool has_grouping_sets_;
  bool is_nocycle_;
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> search_by_items_;
  common::ObSEArray<ColumnItem, 8, common::ModulePageAllocator, true> cycle_by_items_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> cte_exprs_;
  common::ObSEArray<SelectItem, 16, common::ModulePageAllocator, true> select_items_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> group_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> rollup_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> having_exprs_;
  common::ObSEArray<ObAggFunRawExpr*, 8, common::ModulePageAllocator, true> agg_items_;
  common::ObSEArray<ObWinFunRawExpr*, 8, common::ModulePageAllocator, true> win_func_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> start_with_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> connect_by_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> connect_by_prior_exprs_;
  // select a,b,sum(d) from t group by a desc, b asc with rollup.
  common::ObSEArray<ObOrderDirection, 8, common::ModulePageAllocator, true> rollup_directions_;
  common::ObSEArray<ObGroupingSetsItem, 8, common::ModulePageAllocator, true> grouping_sets_items_;
  common::ObSEArray<ObMultiRollupItem, 8, common::ModulePageAllocator, true> multi_rollup_items_;

  // sample scan infos
  common::ObSEArray<SampleInfo, 4, common::ModulePageAllocator, true> sample_infos_;

  // for oracle mode only, for stmt print only
  common::ObSEArray<ObColumnRefRawExpr*, 4, common::ModulePageAllocator, true> for_update_columns_;

  /* These fields are only used by set select */
  bool is_parent_set_distinct_;  // indicate parent is set distinct
  bool is_set_distinct_;
  /* for set stmt child stmt*/
  common::ObSEArray<ObSelectStmt*, 2, common::ModulePageAllocator, true> set_query_;

  /* for show statment*/
  ObShowStmtCtx show_stmt_ctx_;
  // view
  bool is_view_stmt_;  // for view privilege check
  uint64_t view_ref_id_;
  SelectTypeAffectFoundRows select_type_;
  bool is_select_star_;
  ObSelectIntoItem* into_item_;  // select .. into outfile/dumpfile/var_name
  bool is_match_topk_;
  // materialized view, only two table permit mow,
  // for materialized view
  uint64_t base_table_id_;
  uint64_t depend_table_id_;
  // denote having exists ref columns that belongs to current stmt
  bool having_has_self_column_;
  // A set operator B -> B set operator A, children_swapped_ will be
  // set to true.
  bool children_swapped_;
  bool is_from_pivot_;
  bool has_hidden_rowid_;
  ObSqlTempTableInfo* temp_table_info_;
  bool need_temp_table_trans_;
  // to be removed
  bool is_last_access_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_SELECTSTMT_H_
