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

#ifndef OCEANBASE_SQL_STMT_H_
#define OCEANBASE_SQL_STMT_H_
#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_hint.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/parser/parse_node.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/dml/ob_raw_expr_sets.h"

namespace oceanbase {
namespace sql {
class ObStmtResolver;
class ObDMLStmt;

struct TransposeItem {
  enum UnpivotNullsType {
    UNT_NOT_SET = 0,
    UNT_EXCLUDE,
    UNT_INCLUDE,
  };

  struct AggrPair {
    ObRawExpr* expr_;
    ObString alias_name_;
    AggrPair() : expr_(NULL), alias_name_()
    {}
    ~AggrPair()
    {}
    TO_STRING_KV(KPC_(expr), K_(alias_name));
  };

  struct ForPair {
    ObString column_name_;
    ForPair() : column_name_()
    {}
    ~ForPair()
    {}

    TO_STRING_KV(K_(column_name));
  };

  struct InPair {
    common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> exprs_;
    ObString pivot_expr_alias_;
    common::ObSEArray<ObString, 1, common::ModulePageAllocator, true> column_names_;
    InPair() : pivot_expr_alias_()
    {}
    ~InPair()
    {}
    int assign(const InPair& other);
    TO_STRING_KV(K_(exprs), K_(pivot_expr_alias), K_(column_names));
  };

  TransposeItem() : old_column_count_(0), is_unpivot_(false), is_incude_null_(false)
  {}
  void reset()
  {
    old_column_count_ = 0;
    is_unpivot_ = false;
    is_incude_null_ = false;
    aggr_pairs_.reset();
    for_columns_.reset();
    in_pairs_.reset();
    unpivot_columns_.reset();
  }
  inline bool is_pivot() const
  {
    return !is_unpivot_;
  }
  inline bool is_unpivot() const
  {
    return is_unpivot_;
  }
  inline bool is_include_null() const
  {
    return is_incude_null_;
  }
  inline bool is_exclude_null() const
  {
    return !is_incude_null_;
  }
  inline bool need_use_unpivot_op() const
  {
    return is_unpivot_ && in_pairs_.count() > 1;
  }
  void set_pivot()
  {
    is_unpivot_ = false;
  }
  void set_unpivot()
  {
    is_unpivot_ = true;
  }
  void set_include_nulls(const bool is_include_nulls)
  {
    is_incude_null_ = is_include_nulls;
  }
  int assign(const TransposeItem& other);
  int deep_copy(const TransposeItem& other, ObRawExprFactory& expr_factory);
  TO_STRING_KV(K_(old_column_count), K_(is_unpivot), K_(is_incude_null), K_(aggr_pairs), K_(for_columns), K_(in_pairs),
      K_(unpivot_columns), K_(alias_name));

  int64_t old_column_count_;
  bool is_unpivot_;
  bool is_incude_null_;

  common::ObSEArray<AggrPair, 16, common::ModulePageAllocator, true> aggr_pairs_;
  common::ObSEArray<ObString, 16, common::ModulePageAllocator, true> for_columns_;
  common::ObSEArray<InPair, 16, common::ModulePageAllocator, true> in_pairs_;
  common::ObSEArray<ObString, 16, common::ModulePageAllocator, true> unpivot_columns_;

  ObString alias_name_;
};

struct ObUnpivotInfo {
public:
  OB_UNIS_VERSION(1);

public:
  ObUnpivotInfo() : is_include_null_(false), old_column_count_(0), for_column_count_(0), unpivot_column_count_(0)
  {}
  ObUnpivotInfo(const bool is_include_null, const int64_t old_column_count, const int64_t for_column_count,
      const int64_t unpivot_column_count)
      : is_include_null_(is_include_null),
        old_column_count_(old_column_count),
        for_column_count_(for_column_count),
        unpivot_column_count_(unpivot_column_count)
  {}

  void reset()
  {
    new (this) ObUnpivotInfo();
  }
  OB_INLINE bool has_unpivot() const
  {
    return old_column_count_ >= 0 && unpivot_column_count_ > 0 && for_column_count_ > 0;
  };
  OB_INLINE int64_t get_new_column_count() const
  {
    return unpivot_column_count_ + for_column_count_;
  }
  OB_INLINE int64_t get_output_column_count() const
  {
    return old_column_count_ + get_new_column_count();
  }
  TO_STRING_KV(K_(is_include_null), K_(old_column_count), K_(unpivot_column_count), K_(for_column_count));

  bool is_include_null_;
  int64_t old_column_count_;
  int64_t for_column_count_;
  int64_t unpivot_column_count_;
};

struct TableItem {
  TableItem()
  {
    table_id_ = common::OB_INVALID_ID;
    is_index_table_ = false;
    ref_id_ = common::OB_INVALID_ID;
    ref_query_ = NULL;
    is_system_table_ = true;
    is_recursive_union_fake_table_ = false;
    cte_type_ = CTEType::NOT_CTE;
    type_ = BASE_TABLE;
    is_view_table_ = false;
    is_materialized_view_ = false;
    for_update_ = false;
    for_update_wait_us_ = -1;
    mock_id_ = common::OB_INVALID_ID;
    node_ = NULL;
    view_base_item_ = NULL;
    function_table_expr_ = nullptr;
  }
  virtual TO_STRING_KV(N_TID, table_id_, N_TABLE_NAME, table_name_, N_ALIAS_NAME, alias_name_, N_SYNONYM_NAME,
      synonym_name_, "synonym_db_name", synonym_db_name_, N_TABLE_TYPE, static_cast<int32_t>(type_),
      //"recursive union fake table", is_recursive_union_fake_table_,
      N_REF_ID, ref_id_, N_DATABASE_NAME, database_name_, N_FOR_UPDATE, for_update_, N_WAIT, for_update_wait_us_,
      N_MOCK_ID, mock_id_, "view_base_item", (NULL == view_base_item_ ? OB_INVALID_ID : view_base_item_->table_id_));

  enum TableType {
    BASE_TABLE,
    ALIAS_TABLE,
    GENERATED_TABLE,
    JOINED_TABLE,
    CTE_TABLE,
    FUNCTION_TABLE,
    UNPIVOT_TABLE,
    TEMP_TABLE,
  };

  /**
   * with cte(a,b,c) as (select 1,2,3 from dual union all select a+1,b+1,c+1 from cte where a < 10) select * from cte;
   *                                                                               ^                               ^
   *                                                                           FAKE_CTE RECURSIVE_CTE with cte(a,b) as
   * (select 1,2 from dual) select * from cte,        t2    where cte.a = t2.c1; ^           ^ NORMAL_CTE   NOT_CTE
   *
   */
  enum CTEType { NOT_CTE, NORMAL_CTE, RECURSIVE_CTE, FAKE_CTE };

  enum FlashBackQueryType { NOT_USING, USING_TIMESTAMP, USING_SCN };

  int is_same(const TableItem& other, ObStmtResolver& ctx, bool& is_same) const;
  // this table resolved from schema, is base table or alias from base table
  bool is_basic_table() const
  {
    return BASE_TABLE == type_ || ALIAS_TABLE == type_;
  }
  bool is_generated_table() const
  {
    return GENERATED_TABLE == type_;
  }
  bool is_temp_table() const
  {
    return TEMP_TABLE == type_;
  }
  bool is_fake_cte_table() const
  {
    return CTE_TABLE == type_;
  }
  bool is_joined_table() const
  {
    return JOINED_TABLE == type_;
  }
  bool is_function_table() const
  {
    return FUNCTION_TABLE == type_;
  }
  bool is_link_table() const
  {
    return common::is_link_table_id(ref_id_);
  }
  bool is_oracle_all_or_user_sys_view() const
  {
    return (is_ora_sys_view_table(table_id_) &&
            (0 == strncmp(table_name_.ptr(), "USER_", 5) || 0 == strncmp(table_name_.ptr(), "ALL_", 4)));
  }
  bool is_oracle_dba_sys_view() const
  {
    return (is_ora_sys_view_table(table_id_) && (0 == strncmp(table_name_.ptr(), "DBA_", 4)));
  }
  bool is_oracle_all_or_user_sys_view_for_alias() const
  {
    return ((database_name_ == OB_ORA_SYS_SCHEMA_NAME) &&
            (0 == strncmp(table_name_.ptr(), "USER_", 5) || 0 == strncmp(table_name_.ptr(), "ALL_", 4)));
  }
  int deep_copy(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const TableItem& other);
  const common::ObString& get_table_name() const
  {
    return alias_name_.empty() ? table_name_ : alias_name_;
  }
  const common::ObString& get_object_name() const
  {
    return alias_name_.empty() ? (synonym_name_.empty() ? get_table_name() : synonym_name_) : alias_name_;
  }
  const TableItem& get_base_table_item() const
  {
    return is_generated_table() && view_base_item_ != NULL ? view_base_item_->get_base_table_item() : *this;
  }
  // if real table id, it is valid for all threads,
  // else if generated id, it is unique just during the thread session
  uint64_t table_id_;
  common::ObString table_name_;
  common::ObString alias_name_;
  common::ObString synonym_name_;
  common::ObString synonym_db_name_;
  TableType type_;
  // type == BASE_TABLE? ref_id_ is the real Id of the schema
  // type == ALIAS_TABLE? ref_id_ is the real Id of the schema, while table_id_ new generated
  // type == GENERATED_TABLE? ref_id_ is the reference of the sub-query.
  // type == UNPIVOT_TABLE? ref_id_ is the reference of the sub-query,
  // which like   SELECT  "normal_column",
  //                      'in_pair_literal1' AS "for_column1", 'in_pair_literal2' AS "for_column2",
  //                      "in_pair_column1" AS "unpivot_column1", "in_pair_column2" AS "unpivot_column2"
  //              FROM table
  //              WHERE  "in_pair_column1" IS NOT NULL OR "in_pair_column2" IS NOT NULL
  uint64_t ref_id_;
  ObSelectStmt* ref_query_;
  bool is_system_table_;
  bool is_index_table_;                 // just for index table resolver
  bool is_view_table_;                  // for VIEW privilege check
  bool is_recursive_union_fake_table_;  // mark whether this table is a tmp fake table for resolve the recursive cte
                                        // table
  CTEType cte_type_;
  bool is_materialized_view_;
  common::ObString database_name_;
  /* FOR UPDATE clause */
  bool for_update_;
  int64_t for_update_wait_us_;  // 0 means nowait, -1 means infinite
  uint64_t mock_id_;
  const ParseNode* node_;
  // base table item for updatable view
  const TableItem* view_base_item_;  // seems to be useful only in the resolve phase
  ObRawExpr* function_table_expr_;
  // dblink
  common::ObString dblink_name_;
  common::ObString link_database_name_;
};

struct ColumnItem {
  ColumnItem()
      : column_id_(common::OB_INVALID_ID),
        table_id_(common::OB_INVALID_ID),
        column_name_(),
        expr_(NULL),
        default_value_(),
        auto_filled_timestamp_(false),
        default_value_expr_(NULL),
        base_tid_(common::OB_INVALID_ID),
        base_cid_(common::OB_INVALID_ID)
  {}
  bool is_invalid() const
  {
    return NULL == expr_;
  }
  const ObColumnRefRawExpr* get_expr() const
  {
    return expr_;
  }
  ObColumnRefRawExpr* get_expr()
  {
    return expr_;
  }
  void set_default_value(const common::ObObj& val)
  {
    default_value_ = val;
  }
  void set_default_value_expr(ObRawExpr* expr)
  {
    default_value_expr_ = expr;
  }
  bool is_auto_increment() const
  {
    return expr_ != NULL && expr_->is_auto_increment();
  }
  bool is_not_null() const
  {
    return expr_ != NULL && expr_->is_not_null();
  }
  int deep_copy(ObRawExprFactory& expr_factory, const ColumnItem& other);
  void reset()
  {
    column_id_ = common::OB_INVALID_ID;
    table_id_ = common::OB_INVALID_ID;
    column_name_.reset();
    default_value_.reset();
    expr_ = NULL;
    auto_filled_timestamp_ = false;
    default_value_expr_ = NULL;
    base_tid_ = common::OB_INVALID_ID;
    base_cid_ = common::OB_INVALID_ID;
  }
  void set_ref_id(uint64_t table_id, uint64_t column_id)
  {
    table_id_ = table_id;
    column_id_ = column_id;
    (NULL != expr_) ? expr_->set_ref_id(table_id, column_id) : (void)0;
  }
  const ObExprResType* get_column_type() const
  {
    const ObExprResType* column_type = NULL;
    if (expr_ != NULL) {
      column_type = &(expr_->get_result_type());
    }
    return column_type;
  }
  inline uint64_t hash(uint64_t seed) const;
  TO_STRING_KV(N_CID, column_id_, N_TID, table_id_, N_COLUMN, column_name_, K_(auto_filled_timestamp), N_DEFAULT_VALUE,
      default_value_, K_(base_tid), K_(base_cid), N_EXPR, expr_);

  uint64_t column_id_;
  uint64_t table_id_;
  common::ObString column_name_;
  ObColumnRefRawExpr* expr_;
  // the following two are used by DML operations, such as insert or update
  common::ObObj default_value_;
  bool auto_filled_timestamp_;
  ObRawExpr* default_value_expr_;
  // base table id and column id
  uint64_t base_tid_;
  uint64_t base_cid_;
};

inline uint64_t ColumnItem::hash(uint64_t seed) const
{
  seed = common::do_hash(column_id_, seed);
  seed = common::do_hash(column_name_, seed);
  seed = common::do_hash(table_id_, seed);
  seed = common::do_hash(*expr_, seed);

  return seed;
}

class ObDMLStmt;
struct FromItem {
  FromItem() : table_id_(common::OB_INVALID_ID), link_table_id_(common::OB_INVALID_ID), is_joined_(false)
  {}
  uint64_t table_id_;
  uint64_t link_table_id_;
  // false: it is the real table id
  // true: it is the joined table id
  bool is_joined_;

  bool operator==(const FromItem& r) const
  {
    return table_id_ == r.table_id_ && is_joined_ == r.is_joined_;
  }

  int deep_copy(const FromItem& other);
  TO_STRING_KV(N_TID, table_id_, N_IS_JOIN, is_joined_);
};

struct JoinedTable : public TableItem {
  JoinedTable()
      : joined_type_(UNKNOWN_JOIN),
        left_table_(NULL),
        right_table_(NULL),
        single_table_ids_(common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        join_conditions_(common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        using_columns_(common::OB_MALLOC_NORMAL_BLOCK_SIZE),
        coalesce_expr_(common::OB_MALLOC_NORMAL_BLOCK_SIZE)
  {}

  int deep_copy(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const JoinedTable& other, const uint64_t copy_types);
  bool same_as(const JoinedTable& other) const;
  bool is_join_by_using() const
  {
    return 0 < using_columns_.count();
  }
  bool is_inner_join() const
  {
    return INNER_JOIN == joined_type_;
  }
  bool is_left_join() const
  {
    return LEFT_OUTER_JOIN == joined_type_;
  }
  bool is_right_join() const
  {
    return RIGHT_OUTER_JOIN == joined_type_;
  }
  bool is_full_join() const
  {
    return FULL_OUTER_JOIN == joined_type_;
  }
  common::ObIArray<ObRawExpr*>& get_join_conditions()
  {
    return join_conditions_;
  }
  const common::ObIArray<ObRawExpr*>& get_join_conditions() const
  {
    return join_conditions_;
  }
  TO_STRING_KV(N_TID, table_id_, N_TABLE_TYPE, static_cast<int32_t>(type_), N_JOIN_TYPE, ob_join_type_str(joined_type_),
      N_LEFT_TABLE, left_table_, N_RIGHT_TABLE, right_table_, "join_condition", join_conditions_);

  ObJoinType joined_type_;
  TableItem* left_table_;
  TableItem* right_table_;
  common::ObSEArray<uint64_t, 16, common::ModulePageAllocator, true> single_table_ids_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> join_conditions_;
  common::ObSEArray<common::ObString, 8, common::ModulePageAllocator, true> using_columns_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> coalesce_expr_;
};

class SemiInfo {
public:
  SemiInfo(ObJoinType join_type = LEFT_SEMI_JOIN)
      : join_type_(join_type),
        semi_id_(common::OB_INVALID_ID),
        right_table_id_(common::OB_INVALID_ID),
        left_table_ids_(),
        semi_conditions_()
  {}
  virtual ~SemiInfo(){};
  int deep_copy(ObRawExprFactory& expr_factory, const SemiInfo& other, const uint64_t copy_types);
  inline bool is_semi_join() const
  {
    return LEFT_SEMI_JOIN == join_type_ || RIGHT_SEMI_JOIN == join_type_;
  }
  inline bool is_anti_join() const
  {
    return LEFT_ANTI_JOIN == join_type_ || RIGHT_ANTI_JOIN == join_type_;
  }
  TO_STRING_KV(K_(join_type), K_(semi_id), K_(left_table_ids), K_(right_table_id), K_(semi_conditions));

  ObJoinType join_type_;
  uint64_t semi_id_;
  // generate table create from subquery
  uint64_t right_table_id_;
  // table ids which involved right table in parent query
  common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> left_table_ids_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> semi_conditions_;
};

/// assignment: column_expr = expr
struct ObAssignment {
  ObColumnRefRawExpr* column_expr_;
  ObRawExpr* expr_;
  bool is_duplicated_;  // for judging whether or not the column is updated repeatedly
  bool is_implicit_;    // not in update set clause, but add for inner implement,
                        // see ObDMLResolver::resolve_additional_assignments().
  uint64_t base_table_id_;
  uint64_t base_column_id_;
  ObAssignment()
  {
    column_expr_ = NULL;
    expr_ = NULL;
    is_duplicated_ = false;
    is_implicit_ = false;
    base_table_id_ = OB_INVALID_ID;
    base_column_id_ = OB_INVALID_ID;
  }

  int deep_copy(ObRawExprFactory& expr_factory, const ObAssignment& other);
  int assign(const ObAssignment& other);
  uint64_t hash(uint64_t seed) const
  {
    if (NULL != column_expr_) {
      seed = do_hash(*column_expr_, seed);
    }
    if (NULL != expr_) {
      seed = do_hash(*expr_, seed);
    }
    seed = do_hash(is_duplicated_, seed);

    return seed;
  }

  TO_STRING_KV(N_COLUMN, column_expr_, N_EXPR, expr_, K_(base_table_id), K_(base_column_id));
};

struct ObMaterializedViewContext {
  uint64_t base_table_id_;
  uint64_t depend_table_id_;
  common::ObArray<ObColumnRefRawExpr*> select_cols_;
  common::ObArray<ObColumnRefRawExpr*> base_select_cols_;
  common::ObArray<ObColumnRefRawExpr*> depend_select_cols_;
  common::ObArray<ObColumnRefRawExpr*> base_join_cols_;
  common::ObArray<ObColumnRefRawExpr*> depend_join_cols_;
  common::ObArray<ObColumnRefRawExpr*> order_cols_;
  TO_STRING_KV(K_(base_table_id), K_(depend_table_id), K_(select_cols), K_(base_select_cols), K_(depend_select_cols),
      K_(base_join_cols), K_(depend_join_cols), K_(order_cols));
};

typedef common::ObSEArray<ObAssignment, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> ObAssignments;
/// all assignments of one table
struct ObTableAssignment {
  ObTableAssignment() : table_id_(OB_INVALID_ID), assignments_(), is_update_part_key_(false)
  {}

  int deep_copy(ObRawExprFactory& expr_factory, const ObTableAssignment& other);
  int assign(const ObTableAssignment& other);
  static int expand_expr(ObAssignments& assigns, ObRawExpr*& expr);
  static int replace_assigment_expr(ObAssignments& assigns, ObRawExpr*& expr);
  uint64_t hash(uint64_t seed) const
  {
    int64_t M = assignments_.count();
    seed = do_hash(table_id_, seed);
    for (int64_t j = 0; j < M; ++j) {
      seed = do_hash(assignments_.at(j), seed);
    }
    seed = do_hash(is_update_part_key_, seed);

    return seed;
  }

  uint64_t table_id_;
  ObAssignments assignments_;
  bool is_update_part_key_;
  TO_STRING_KV(K_(table_id), N_ASSIGN, assignments_, K_(is_update_part_key));
};
/// multi-table assignments
typedef common::ObSEArray<ObTableAssignment, 3, common::ModulePageAllocator, true> ObTablesAssignments;

enum EQUAL_SET_SCOPE { SCOPE_WHERE = 1 << 0, SCOPE_HAVING = 1 << 1, SCOPE_MAX = 1 << 2, SCOPE_ALL = SCOPE_MAX - 1 };

/// In fact, ObStmt is ObDMLStmt.
class ObDMLStmt : public ObStmt {
public:
  struct PartExprArray {
    PartExprArray() : table_id_(common::OB_INVALID_ID), index_tid_(common::OB_INVALID_ID)
    {}
    uint64_t table_id_;
    uint64_t index_tid_;
    ObArray<ObRawExpr*> part_expr_array_;
    ObArray<ObRawExpr*> subpart_expr_array_;
    void reset()
    {
      table_id_ = common::OB_INVALID_ID;
      index_tid_ = common::OB_INVALID_ID;
      part_expr_array_.reset();
      subpart_expr_array_.reset();
    }
    int deep_copy(ObRawExprFactory& expr_factory, const PartExprArray& other, const uint64_t copy_types);
    TO_STRING_KV(K_(table_id), K_(index_tid));
  };

  struct PartExprItem {
    PartExprItem()
        : table_id_(common::OB_INVALID_ID), index_tid_(common::OB_INVALID_ID), part_expr_(NULL), subpart_expr_(NULL)
    {}
    uint64_t table_id_;
    uint64_t index_tid_;
    ObRawExpr* part_expr_;
    ObRawExpr* subpart_expr_;
    void reset()
    {
      table_id_ = common::OB_INVALID_ID;
      index_tid_ = common::OB_INVALID_ID;
      part_expr_ = NULL;
      subpart_expr_ = NULL;
    }
    int deep_copy(ObRawExprFactory& expr_factory, const PartExprItem& other, const uint64_t copy_types);
    TO_STRING_KV(K_(table_id), K_(index_tid), KPC_(part_expr), KPC_(subpart_expr));
  };
  typedef common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> ObViewTableIds;

public:
  explicit ObDMLStmt(stmt::StmtType type);
  virtual ~ObDMLStmt();
  int assign(const ObDMLStmt& other);
  int deep_copy(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other);
  virtual int deep_copy_stmt_struct(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other);
  int deep_copy_share_exprs(ObRawExprFactory& expr_factory, const ObDMLStmt& other_stmt,
      ObIArray<ObRawExpr*>& old_share_exprs, ObIArray<ObRawExpr*>& new_share_exprs);

  int get_child_table_id_recurseive(common::ObIArray<share::schema::ObObjectStruct>& object_ids,
      const int64_t object_limit_count = common::OB_MAX_TABLE_NUM_PER_STMT) const;
  int get_child_table_id_count_recurseive(
      int64_t& object_ids_cnt, const int64_t object_limit_count = common::OB_MAX_TABLE_NUM_PER_STMT) const;

  virtual bool check_table_be_modified(uint64_t ref_table_id) const
  {
    UNUSED(ref_table_id);
    return false;
  }

  virtual int replace_inner_stmt_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs);

  int update_stmt_table_id(const ObDMLStmt& other);
  int adjust_statement_id();
  int adjust_subquery_stmt_parent(const ObDMLStmt* old_parent, ObDMLStmt* new_parent);

  int64_t get_from_item_size() const
  {
    return from_items_.count();
  }
  void clear_from_items()
  {
    from_items_.reset();
  }
  int add_from_item(uint64_t tid, bool is_joined = false)
  {
    int ret = common::OB_SUCCESS;
    if (common::OB_INVALID_ID != tid) {
      FromItem item;
      item.table_id_ = tid;
      item.is_joined_ = is_joined;
      ret = from_items_.push_back(item);
    } else {
      ret = common::OB_ERR_ILLEGAL_ID;
    }
    return ret;
  }
  int remove_from_item(uint64_t tid);

  TableItem* create_table_item(common::ObIAllocator& allocator);
  int add_view_table_id(uint64_t view_table_id);
  int add_view_table_ids(const ObIArray<uint64_t>& view_ids);
  int merge_from_items(const ObDMLStmt& stmt);
  const FromItem& get_from_item(int64_t index) const
  {
    return from_items_[index];
  }
  FromItem& get_from_item(int64_t index)
  {
    return from_items_[index];
  }
  int64_t get_from_item_idx(uint64_t table_id) const;
  common::ObIArray<FromItem>& get_from_items()
  {
    return from_items_;
  }
  const common::ObIArray<FromItem>& get_from_items() const
  {
    return from_items_;
  }
  common::ObIArray<PartExprItem>& get_part_exprs()
  {
    return part_expr_items_;
  }
  const common::ObIArray<PartExprItem>& get_part_exprs() const
  {
    return part_expr_items_;
  }
  common::ObIArray<PartExprArray>& get_related_part_expr_arrays()
  {
    return related_part_expr_arrays_;
  }
  const common::ObIArray<PartExprArray>& get_related_part_expr_arrays() const
  {
    return related_part_expr_arrays_;
  }

  int reset_from_item(const common::ObIArray<FromItem>& from_items);
  void clear_from_item()
  {
    from_items_.reset();
  }
  int reset_table_item(const common::ObIArray<TableItem*>& table_items);
  void clear_column_items();

  int remove_part_expr_items(uint64_t table_id);
  int remove_part_expr_items(ObIArray<uint64_t>& table_ids);
  int get_part_expr_items(uint64_t table_id, ObIArray<PartExprItem>& part_items);
  int get_part_expr_items(ObIArray<uint64_t>& table_ids, ObIArray<PartExprItem>& part_items);
  int set_part_expr_items(ObIArray<PartExprItem>& part_items);
  int set_part_expr_item(PartExprItem& part_item);
  ObRawExpr* get_part_expr(uint64_t table_id, uint64_t index_tid) const;
  ObRawExpr* get_subpart_expr(const uint64_t table_id, uint64_t index_tid) const;

  ObRawExpr* get_related_part_expr(uint64_t table_id, uint64_t index_tid, int32_t idx) const;
  ObRawExpr* get_related_subpart_expr(const uint64_t table_id, uint64_t index_tid, int32_t idx) const;

  int set_part_expr(uint64_t table_id, uint64_t index_tid, ObRawExpr* part_expr, ObRawExpr* subpart_expr);
  inline ObStmtHint& get_stmt_hint()
  {
    return stmt_hint_;
  }
  inline const ObStmtHint& get_stmt_hint() const
  {
    return stmt_hint_;
  }
  void set_subquery_flag(bool has_subquery);
  virtual bool has_subquery() const;
  inline bool has_order_by() const
  {
    return (get_order_item_size() > 0);
  }
  int add_joined_table(JoinedTable* joined_table)
  {
    return joined_tables_.push_back(joined_table);
  }
  const JoinedTable* get_joined_table(uint64_t table_id) const;
  JoinedTable* get_joined_table(uint64_t table_id);
  const common::ObIArray<JoinedTable*>& get_joined_tables() const
  {
    return joined_tables_;
  }
  common::ObIArray<JoinedTable*>& get_joined_tables()
  {
    return joined_tables_;
  }
  inline int64_t get_order_item_size() const
  {
    return order_items_.count();
  }
  inline bool is_single_table_stmt() const
  {
    return (1 == get_table_size());
  }
  int add_semi_info(SemiInfo* info)
  {
    return semi_infos_.push_back(info);
  }
  inline common::ObIArray<SemiInfo*>& get_semi_infos()
  {
    return semi_infos_;
  }
  inline const common::ObIArray<SemiInfo*>& get_semi_infos() const
  {
    return semi_infos_;
  }
  inline int64_t get_semi_info_size() const
  {
    return semi_infos_.count();
  }
  inline void set_limit_offset(ObRawExpr* limit, ObRawExpr* offset)
  {
    limit_count_expr_ = limit;
    limit_offset_expr_ = offset;
  }
  inline bool has_limit() const
  {
    return (limit_count_expr_ != NULL || limit_offset_expr_ != NULL || limit_percent_expr_ != NULL);
  }
  ObRawExpr* get_limit_expr() const
  {
    return limit_count_expr_;
  }
  ObRawExpr* get_offset_expr() const
  {
    return limit_offset_expr_;
  }
  ObRawExpr*& get_limit_expr()
  {
    return limit_count_expr_;
  }
  ObRawExpr*& get_offset_expr()
  {
    return limit_offset_expr_;
  }
  ObRawExpr* get_limit_percent_expr() const
  {
    return limit_percent_expr_;
  }
  ObRawExpr*& get_limit_percent_expr()
  {
    return limit_percent_expr_;
  }
  void set_limit_percent_expr(ObRawExpr* percent_expr)
  {
    limit_percent_expr_ = percent_expr;
  }
  void set_fetch_with_ties(bool is_with_ties)
  {
    is_fetch_with_ties_ = is_with_ties;
  }
  bool is_fetch_with_ties() const
  {
    return is_fetch_with_ties_;
  }
  void set_has_fetch(bool has_fetch)
  {
    has_fetch_ = has_fetch;
  }
  bool has_fetch() const
  {
    return has_fetch_;
  }
  void set_fetch_info(ObRawExpr* offset_expr, ObRawExpr* count_expr, ObRawExpr* percent_expr)
  {
    set_limit_offset(count_expr, offset_expr);
    limit_percent_expr_ = percent_expr;
  }
  int add_order_item(OrderItem& order_item)
  {
    return order_items_.push_back(order_item);
  }
  inline const OrderItem& get_order_item(int64_t index) const
  {
    return order_items_[index];
  }
  inline OrderItem& get_order_item(int64_t index)
  {
    return order_items_[index];
  }
  inline const common::ObIArray<OrderItem>& get_order_items() const
  {
    return order_items_;
  }
  inline common::ObIArray<OrderItem>& get_order_items()
  {
    return order_items_;
  }
  int get_order_exprs(common::ObIArray<ObRawExpr*>& order_exprs);
  inline const ObViewTableIds& get_view_table_id_store() const
  {
    return view_table_id_store_;
  }
  int pull_all_expr_relation_id_and_levels();
  int formalize_stmt(ObSQLSessionInfo* session_info);
  int formalize_stmt_expr_reference();
  int set_sharable_expr_reference(ObRawExpr& expr);
  virtual int remove_useless_sharable_expr();
  virtual int clear_sharable_expr_reference();
  virtual int get_from_subquery_stmts(common::ObIArray<ObSelectStmt*>& child_stmts) const;
  int is_referred_by_partitioning_expr(const ObRawExpr* expr, bool& is_referred);
  int64_t get_table_size() const
  {
    return table_items_.count();
  }
  int64_t get_CTE_table_size() const
  {
    return CTE_table_items_.count();
  }
  int64_t get_column_size() const
  {
    return column_items_.count();
  }
  inline int64_t get_condition_size() const
  {
    return condition_exprs_.count();
  }
  void reset_table_items()
  {
    table_items_.reset();
  }
  void reset_CTE_table_items()
  {
    CTE_table_items_.reset();
  }
  const ColumnItem* get_column_item(int64_t index) const
  {
    const ColumnItem* column_item = NULL;
    if (0 <= index && index < column_items_.count()) {
      column_item = &column_items_.at(index);
    }
    return column_item;
  }
  ColumnItem* get_column_item(int64_t index)
  {
    ColumnItem* column_item = NULL;
    if (0 <= index && index < column_items_.count()) {
      column_item = &column_items_.at(index);
    }
    return column_item;
  }
  inline common::ObIArray<ColumnItem>& get_column_items()
  {
    return column_items_;
  }
  inline const common::ObIArray<ColumnItem>& get_column_items() const
  {
    return column_items_;
  }
  int get_column_items(uint64_t table_id, ObIArray<ColumnItem>& column_items) const;
  int get_column_items(ObIArray<uint64_t>& table_ids, ObIArray<ColumnItem>& column_items) const;
  int get_column_exprs(ObIArray<ObColumnRefRawExpr*>& column_exprs) const;
  int get_column_exprs(ObIArray<ObRawExpr*>& column_exprs) const;
  int get_column_exprs(ObIArray<TableItem*>& table_items, ObIArray<ObRawExpr*>& column_exprs) const;
  int get_view_output(
      const TableItem& table, ObIArray<ObRawExpr*>& select_list, ObIArray<ObRawExpr*>& column_list) const;

  int64_t get_user_var_size() const
  {
    return user_var_exprs_.count();
  }
  inline ObUserVarIdentRawExpr* get_user_var(int64_t index)
  {
    return user_var_exprs_.at(index);
  }
  inline const ObUserVarIdentRawExpr* get_user_var(int64_t index) const
  {
    return user_var_exprs_.at(index);
  }
  inline common::ObIArray<ObUserVarIdentRawExpr*>& get_user_vars()
  {
    return user_var_exprs_;
  }
  inline const common::ObIArray<ObUserVarIdentRawExpr*>& get_user_vars() const
  {
    return user_var_exprs_;
  }

  int get_joined_item_idx(const TableItem* child_table, int64_t& idx) const;
  int get_table_item(const ObSQLSessionInfo* session_info, const common::ObString& database_name,
      const common::ObString& table_name, const TableItem*& table_item) const;
  int get_all_table_item_by_tname(const ObSQLSessionInfo* session_info, const common::ObString& db_name,
      const common::ObString& table_name, common::ObIArray<const TableItem*>& table_items) const;
  const TableItem* get_table_item_by_id(uint64_t table_id) const;
  TableItem* get_table_item_by_id(uint64_t table_id);
  int get_table_item_by_id(ObIArray<uint64_t>& table_ids, ObIArray<TableItem*>& tables);
  const TableItem* get_table_item(int64_t index) const
  {
    return table_items_.at(index);
  }
  const TableItem* get_CTE_table_item(int64_t index) const
  {
    return CTE_table_items_.at(index);
  }
  TableItem* get_table_item(int64_t index)
  {
    return table_items_.at(index);
  }
  int remove_table_item(const TableItem* ti);
  int remove_table_item(const ObIArray<TableItem*>& table_items);
  TableItem* get_CTE_table_item(int64_t index)
  {
    return CTE_table_items_.at(index);
  }
  TableItem* get_table_item(const FromItem item);
  const TableItem* get_table_item(const FromItem item) const;
  int get_table_item_idx(const TableItem* child_table, int64_t& idx) const;
  int add_table_item(const ObSQLSessionInfo* session_info, TableItem* table_item);
  int add_table_item(const ObSQLSessionInfo* session_info, ObIArray<TableItem*>& table_items);
  int add_table_item(const ObSQLSessionInfo* session_info, TableItem* table_item, bool& have_same_table_name);
  int add_mock_table_item(TableItem* table_item);
  int add_cte_table_item(TableItem* table_item, bool& dup_name);
  int check_CTE_name_exist(const ObString& var_name, bool& dup_name, TableItem*& table_item);
  int check_CTE_name_exist(const ObString& var_name, bool& dup_name);
  int generate_view_name(ObIAllocator& allocator, ObString& view_name, bool is_temp = false);
  int append_id_to_view_name(char* buf, int64_t buf_len, int64_t& pos, bool is_temp = false);
  int32_t get_table_bit_index(uint64_t table_id) const;
  int set_table_bit_index(uint64_t table_id);
  ColumnItem* get_column_item(uint64_t table_id, const common::ObString& col_name);
  int add_column_item(ColumnItem& column_item);
  int add_column_item(ObIArray<ColumnItem>& column_items);
  int remove_column_item(uint64_t table_id, uint64_t column_id);
  int remove_column_item(uint64_t table_id);
  int remove_column_item(const ObRawExpr* column_expr);
  int remove_column_item(const ObIArray<ObRawExpr*>& column_exprs);
  const ObRawExpr* get_condition_expr(int64_t index) const
  {
    return condition_exprs_.at(index);
  }
  ObRawExpr* get_condition_expr(int64_t index)
  {
    return condition_exprs_.at(index);
  }
  common::ObIArray<ObRawExpr*>& get_condition_exprs()
  {
    return condition_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_condition_exprs() const
  {
    return condition_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_deduced_exprs()
  {
    return deduced_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_deduced_exprs() const
  {
    return deduced_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_pseudo_column_like_exprs()
  {
    return pseudo_column_like_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_pseudo_column_like_exprs() const
  {
    return pseudo_column_like_exprs_;
  }
  common::ObRowDesc& get_table_hash()
  {
    return tables_hash_;
  }
  int rebuild_tables_hash();
  int update_rel_ids(ObRelIds& rel_ids, const ObIArray<int64_t>& bit_index_map);
  int update_column_item_rel_id();
  common::ObIArray<TableItem*>& get_table_items()
  {
    return table_items_;
  }
  const common::ObIArray<TableItem*>& get_table_items() const
  {
    return table_items_;
  }
  const common::ObIArray<TableItem*>& get_CTE_table_items() const
  {
    return CTE_table_items_;
  }
  const common::ObIArray<uint64_t>& get_nextval_sequence_ids() const
  {
    return nextval_sequence_ids_;
  }
  common::ObIArray<uint64_t>& get_nextval_sequence_ids()
  {
    return nextval_sequence_ids_;
  }
  const common::ObIArray<uint64_t>& get_currval_sequence_ids() const
  {
    return currval_sequence_ids_;
  }
  common::ObIArray<uint64_t>& get_currval_sequence_ids()
  {
    return currval_sequence_ids_;
  }
  int add_nextval_sequence_id(uint64_t id)
  {
    return nextval_sequence_ids_.push_back(id);
  }
  int add_currval_sequence_id(uint64_t id)
  {
    return currval_sequence_ids_.push_back(id);
  }
  bool has_sequence() const
  {
    return nextval_sequence_ids_.count() > 0 || currval_sequence_ids_.count() > 0;
  }
  void clear_sequence()
  {
    nextval_sequence_ids_.reset();
    currval_sequence_ids_.reset();
  }
  bool has_part_key_sequence() const
  {
    return has_part_key_sequence_;
  }
  void set_has_part_key_sequence(const bool v)
  {
    has_part_key_sequence_ = v;
  }
  int add_condition_expr(ObRawExpr* expr)
  {
    return condition_exprs_.push_back(expr);
  }
  int add_condition_exprs(const common::ObIArray<ObRawExpr*>& exprs)
  {
    return append(condition_exprs_, exprs);
  }
  int add_deduced_expr(ObRawExpr* expr)
  {
    return deduced_exprs_.push_back(expr);
  }
  int add_deduced_exprs(const common::ObIArray<ObRawExpr*>& exprs)
  {
    return append(deduced_exprs_, exprs);
  }
  // move from ObStmt
  // user var
  bool is_contains_assignment() const
  {
    return is_contains_assignment_;
  }
  void set_contains_assignment(bool v)
  {
    is_contains_assignment_ |= v;
  }
  void set_calc_found_rows(const bool found_rows)
  {
    is_calc_found_rows_ = found_rows;
  }
  bool is_calc_found_rows()
  {
    return is_calc_found_rows_;
  }
  void set_has_top_limit(const bool is_top_limit)
  {
    has_top_limit_ = is_top_limit;
  }
  bool has_top_limit() const
  {
    return has_top_limit_;
  }

  virtual bool is_affect_found_rows() const
  {
    return false;
  }
  // if with explict autoinc column, "insert into values" CAN NOT do multi part insert,
  // to ensure intra-partition autoinc ascending
  inline bool with_explicit_autoinc_column() const
  {
    bool with = false;
    for (int64_t i = 0; i < autoinc_params_.count(); ++i) {
      if (common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID != autoinc_params_.at(i).autoinc_col_id_) {
        with = true;
        break;
      }
    }
    return with;
  }
  inline common::ObIArray<share::AutoincParam>& get_autoinc_params()
  {
    return autoinc_params_;
  }
  inline void set_affected_last_insert_id(bool affected_last_insert_id)
  {
    affected_last_insert_id_ = affected_last_insert_id;
  }
  inline bool get_affected_last_insert_id() const
  {
    return affected_last_insert_id_;
  }
  int add_autoinc_param(share::AutoincParam& autoinc_param)
  {
    return autoinc_params_.push_back(autoinc_param);
  }
  inline void set_parent_namespace_stmt(ObDMLStmt* parent_namespace_stmt)
  {
    parent_namespace_stmt_ = parent_namespace_stmt;
  }
  inline ObDMLStmt* get_parent_namespace_stmt() const
  {
    return parent_namespace_stmt_;
  }
  inline void set_current_level(int32_t current_level)
  {
    current_level_ = current_level;
  }
  inline int32_t get_current_level() const
  {
    return current_level_;
  }
  int add_subquery_ref(ObQueryRefRawExpr* query_ref);
  virtual int get_child_stmt_size(int64_t& child_size) const;
  int64_t get_subquery_expr_size() const
  {
    return subquery_exprs_.count();
  }
  virtual int get_child_stmts(common::ObIArray<ObSelectStmt*>& child_stmts) const;
  virtual int set_child_stmt(const int64_t child_num, ObSelectStmt* child_stmt);
  common::ObIArray<ObQueryRefRawExpr*>& get_subquery_exprs()
  {
    return subquery_exprs_;
  }
  const common::ObIArray<ObQueryRefRawExpr*>& get_subquery_exprs() const
  {
    return subquery_exprs_;
  }
  bool is_root_stmt() const
  {
    return NULL == parent_namespace_stmt_;
  }
  virtual bool is_set_stmt() const
  {
    return false;
  }
  virtual bool has_link_table() const;

  int get_relation_exprs(common::ObIArray<ObRawExpr*>& relation_exprs, int32_t ignore_scope = 0) const;
  int get_relation_exprs(common::ObIArray<ObRawExprPointer>& relation_expr_ptrs, int32_t ignore_scope = 0);
  // this func is used for enum_set_wrapper to get exprs which need to be handled
  int get_relation_exprs_for_enum_set_wrapper(common::ObIArray<ObRawExpr*>& rel_array);
  virtual int replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to);
  const TableItem* get_table_item_in_all_namespace(uint64_t table_id) const;
  ColumnItem* get_column_item_by_id(uint64_t table_id, uint64_t column_id);
  ObColumnRefRawExpr* get_column_expr_by_id(uint64_t table_id, uint64_t column_id);
  const ColumnItem* get_column_item_by_id(uint64_t table_id, uint64_t column_id) const;
  const ObColumnRefRawExpr* get_column_expr_by_id(uint64_t table_id, uint64_t column_id) const;
  int get_column_exprs(uint64_t table_id, ObIArray<ObColumnRefRawExpr*>& table_cols) const;
  int get_column_exprs(uint64_t table_id, ObIArray<ObRawExpr*>& table_cols) const;
  int convert_table_ids_to_bitset(common::ObIArray<ObTablesIndex>& table_ids, common::ObIArray<ObRelIds>& table_idxs);
  int convert_table_ids_to_bitset(
      common::ObIArray<ObPQDistributeHint>& hints_ids, common::ObIArray<ObPQDistributeIndex>& hint_idxs);
  int convert_table_ids_to_bitset(common::ObIArray<ObPQMapHint>& table_ids, ObRelIds& table_idxs);
  /////////functions for sql hint/////////////
  // Allow database_name is empty.Find the table item match the name. If duplicated,
  //*table_item is NULL. Used for hint. If you want to change this func, connect
  int find_table_item(const ObSQLSessionInfo& session_info, const common::ObString& database_name,
      const common::ObString& table_name, TableItem*& table_item);
  // Use find_table_item to get table id. If no match or duplicated, table id is OB_INVALID_ID.
  // Used for hint.
  int get_table_id(const ObSQLSessionInfo& session_info, const common::ObString& stmt_name,
      const common::ObString& database_name, const common::ObString& table_name, uint64_t& table_id);

  int mark_share_exprs() const;

  virtual int inner_get_share_exprs(ObIArray<ObRawExpr*>& share_exprs) const;

  int has_ref_assign_user_var(bool& has_ref_user_var) const;

  int recursive_check_has_ref_assign_user_var(bool& has_ref_user_var) const;

  int get_temp_table_ids(ObIArray<uint64_t>& temp_table_ids);

  int check_pseudo_column_exist(ObItemType type, ObPseudoColumnRawExpr*& expr);

  /////////end of functions for sql hint//////////
  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_), N_TABLE, table_items_, N_TABLE, CTE_table_items_, N_PARTITION_EXPR,
      part_expr_items_, N_COLUMN, column_items_, N_COLUMN, nextval_sequence_ids_, N_COLUMN, currval_sequence_ids_,
      N_WHERE, condition_exprs_, N_ORDER_BY, order_items_, N_LIMIT, limit_count_expr_, N_OFFSET, limit_offset_expr_,
      N_QUERY_HINT, stmt_hint_, N_SUBQUERY_EXPRS, subquery_exprs_, N_USER_VARS, user_var_exprs_);

  void set_is_table_flag();
  int has_is_table(bool& has_is_table) const;
  int has_inner_table(bool& has_inner_table) const;

  ///////////functions for sql hint/////////////
  int check_and_convert_hint(const ObSQLSessionInfo& session_info);

  inline bool is_eliminated() const
  {
    return eliminated_;
  }
  void set_eliminated(const bool eliminated)
  {
    eliminated_ = eliminated;
  }
  int get_join_condition_expr(JoinedTable& join_table, RelExprCheckerBase& expr_checker) const;
  int check_if_contain_inner_table(bool& is_contain_inner_table) const;
  int check_if_contain_select_for_update(bool& is_contain_select_for_update) const;
  int check_if_table_exists(uint64_t table_id, bool& is_existed) const;
  int has_for_update(bool& has) const;
  int get_rownum_expr(ObRawExpr*& expr) const;
  int has_rownum(bool& has_rownum) const;
  int get_sequence_expr(ObRawExpr*& expr,
      const common::ObString seq_name,    // sequence object name
      const common::ObString seq_action,  // NEXTVAL or CURRVAL
      const uint64_t seq_id) const;
  int has_rand(bool& has_rand) const
  {
    return has_special_expr(CNT_RAND_FUNC, has_rand);
  }
  virtual int has_special_expr(const ObExprInfoFlag, bool& has) const;
  int pull_up_object_ids();
  int push_down_query_hint();
  static int copy_query_hint(ObDMLStmt* from, ObDMLStmt* to);
  void set_hierarchical_query(bool is_hierarchical_query)
  {
    is_hierarchical_query_ = is_hierarchical_query;
  }
  void set_hierarchical_query()
  {
    is_hierarchical_query_ = true;
  }
  bool is_hierarchical_query() const
  {
    return is_hierarchical_query_;
  }
  int contain_hierarchical_query(bool& contain_hie_query) const;
  void set_order_siblings(bool is_order_siblings)
  {
    is_order_siblings_ = is_order_siblings;
  }
  void set_order_siblings()
  {
    is_order_siblings_ = true;
  }
  bool is_order_siblings() const
  {
    return is_order_siblings_;
  }
  void set_has_prior(bool has_prior)
  {
    has_prior_ = has_prior;
  }
  void set_has_prior()
  {
    has_prior_ = true;
  }
  bool has_prior()
  {
    return has_prior_;
  }
  const TransposeItem* get_transpose_item() const
  {
    return transpose_item_;
  }
  void set_transpose_item(const TransposeItem* transpose_item)
  {
    transpose_item_ = transpose_item;
  }
  const ObUnpivotInfo get_unpivot_info() const
  {
    return (transpose_item_ != NULL ? ObUnpivotInfo(transpose_item_->is_include_null(),
                                          transpose_item_->old_column_count_,
                                          transpose_item_->for_columns_.count(),
                                          transpose_item_->unpivot_columns_.count())
                                    : ObUnpivotInfo());
  }
  bool is_unpivot_select() const
  {
    const ObUnpivotInfo& unpivot_info = get_unpivot_info();
    return unpivot_info.has_unpivot();
  }
  int disable_px_hint();
  int remove_subquery_expr(const ObRawExpr* expr);
  // rebuild query ref exprs
  int adjust_subquery_list();
  int get_stmt_equal_sets(EqualSets& equal_sets, ObIAllocator& allocator, const bool is_strict, const int check_scope);
  virtual int get_equal_set_conditions(ObIArray<ObRawExpr*>& conditions, const bool is_strict, const int check_scope);
  int extract_equal_condition_from_joined_table(
      const TableItem* table, ObIArray<ObRawExpr*>& equal_set_conditions, const bool is_strict);

  int add_value_to_check_constraint_exprs(ObRawExpr* expr)
  {
    return check_constraint_exprs_.push_back(expr);
  }
  const common::ObIArray<ObRawExpr*>& get_check_constraint_exprs() const
  {
    return check_constraint_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_check_constraint_exprs()
  {
    return check_constraint_exprs_;
  }
  int64_t get_check_constraint_exprs_size() const
  {
    return check_constraint_exprs_.count();
  }

  int check_rowid_column_exists(const uint64_t table_id, bool& is_contain_rowid);

  virtual bool is_returning() const
  {
    return false;
  }
  int reset_statement_id(const ObDMLStmt& other);

protected:
  int check_and_convert_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint);
  int check_and_convert_index_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint);
  int check_and_convert_leading_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint);
  int check_and_convert_join_method_hint(const ObSQLSessionInfo& session_info,
      const common::ObIArray<ObTableInHint>& hint_tables,
      const common::ObIArray<std::pair<uint8_t, uint8_t>>& join_order_pairs,
      common::ObIArray<ObTablesIndex>& table_ids);
  int check_and_convert_pq_dist_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint);
  int check_and_convert_pq_map_hint(const ObSQLSessionInfo& session_info, ObStmtHint& hint);
  //////////end of functions for sql hint/////////////

protected:
  int replace_expr_in_joined_table(JoinedTable& joined_table, ObRawExpr* from, ObRawExpr* to);
  int create_table_item(TableItem*& table_item);

  virtual int inner_get_relation_exprs(RelExprCheckerBase& expr_checker);
  virtual int inner_get_relation_exprs_for_wrapper(RelExprChecker& expr_checker)
  {
    return inner_get_relation_exprs(expr_checker);
  }

protected:
  int construct_join_tables(const ObDMLStmt& other);
  int construct_join_table(const ObDMLStmt& other, const JoinedTable& other_joined_table, JoinedTable& joined_table);
  int extract_column_expr(
      const common::ObIArray<ColumnItem>& column_items, common::ObIArray<ObColumnRefRawExpr*>& column_exprs) const;
  int update_table_item_id_for_joined_table(
      const ObDMLStmt& other_stmt, const JoinedTable& other, JoinedTable& current);
  int update_table_item_id(
      const ObDMLStmt& other, const TableItem& old_item, const bool has_bit_index, TableItem& new_item);

  int replace_expr_for_joined_table(const common::ObIArray<ObRawExpr*>& other_exprs,
      const common::ObIArray<ObRawExpr*>& new_exprs, JoinedTable& joined_tables);

protected:
  /**
   * @note
   * Per MySQL 5.7, the following clauses are common in 'select', 'delete' and 'update' statement:
   *     - table_references(joined_tables)
   *     - where
   *     - order by
   *     - limit
   * which then should be define in ObStmt.
   */
  // order by
  common::ObSEArray<OrderItem, 8, common::ModulePageAllocator, true> order_items_;
  // limit
  /* -1 means no limit */
  ObRawExpr* limit_count_expr_;
  ObRawExpr* limit_offset_expr_;
  ObRawExpr* limit_percent_expr_;
  bool has_fetch_;
  bool is_fetch_with_ties_;
  // table_references
  common::ObSEArray<FromItem, 8, common::ModulePageAllocator, true> from_items_;
  common::ObSEArray<PartExprItem, 8, common::ModulePageAllocator, true> part_expr_items_;
  common::ObSEArray<PartExprArray, 8, common::ModulePageAllocator, true> related_part_expr_arrays_;
  common::ObSEArray<JoinedTable*, 8, common::ModulePageAllocator, true> joined_tables_;
  ObStmtHint stmt_hint_;
  // semi info for semi join
  common::ObSEArray<SemiInfo*, 4, common::ModulePageAllocator, true> semi_infos_;
  // dependent sub-queres in expression
  bool has_subquery_;
  // used for plan cache check view version
  ObViewTableIds view_table_id_store_;

  // move from ObStmt
  common::ObSEArray<share::AutoincParam, 2, common::ModulePageAllocator, true> autoinc_params_;  // auto-increment
                                                                                                 // related
  // member for found_rows
  bool is_calc_found_rows_;
  bool has_top_limit_;
  // if the stmt  contains user variable assignment
  // such as @a:=123
  // we may need to serialize the map to remote server
  bool is_contains_assignment_;
  bool affected_last_insert_id_;
  // insert into values (s1.nextval, ...) s1.nextval
  bool has_part_key_sequence_;
  common::ObSEArray<uint64_t, 2> nextval_sequence_ids_;
  common::ObSEArray<uint64_t, 2> currval_sequence_ids_;
  common::ObSEArray<TableItem*, 4, common::ModulePageAllocator, true> table_items_;
  common::ObSEArray<TableItem*, 4, common::ModulePageAllocator, true> CTE_table_items_;
  common::ObSEArray<ColumnItem, 16, common::ModulePageAllocator, true> column_items_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> condition_exprs_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> deduced_exprs_;  // for deducing generated exprs
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> pseudo_column_like_exprs_;
  // it is only used to record the table_id--bit_index map
  // although it is a little weird, but it is high-performance than ObHashMap
  common::ObRowDesc tables_hash_;
  ObDMLStmt* parent_namespace_stmt_;
  int32_t current_level_;
  common::ObSEArray<ObQueryRefRawExpr*, 4, common::ModulePageAllocator, true> subquery_exprs_;
  bool is_hierarchical_query_;
  bool is_order_siblings_;
  bool has_prior_;
  const TransposeItem* transpose_item_;

  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> check_constraint_exprs_;
  common::ObSEArray<ObUserVarIdentRawExpr*, 4, common::ModulePageAllocator, true> user_var_exprs_;

private:
  bool has_is_table_;
  bool eliminated_;
  bool has_temp_table_;
  bool has_temp_table_insert_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_STMT_H_
