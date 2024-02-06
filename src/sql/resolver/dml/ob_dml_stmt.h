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
#include "sql/resolver/ob_stmt.h"
#include "sql/parser/parse_node.h"
#include "sql/ob_sql_define.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/dml/ob_raw_expr_sets.h"
#include "sql/resolver/expr/ob_raw_expr_copier.h"
#include "sql/resolver/dml/ob_stmt_expr_visitor.h"

namespace oceanbase
{
namespace sql
{
class ObStmtResolver;
class ObDMLStmt;
class ObStmtExprVisitor;
class ObStmtExprGetter;

struct TransposeItem
{
  enum UnpivotNullsType
  {
    UNT_NOT_SET = 0,
    UNT_EXCLUDE,
    UNT_INCLUDE,
  };

  struct AggrPair
  {
    ObRawExpr *expr_;
    ObString alias_name_;
    AggrPair() : expr_(NULL), alias_name_() {}
    ~AggrPair() {}
    TO_STRING_KV(KPC_(expr), K_(alias_name));
  };

  struct ForPair
  {
    ObString column_name_;
    ForPair() : column_name_() {}
    ~ForPair() {}

    TO_STRING_KV(K_(column_name));
  };


  struct InPair
  {
    common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> exprs_;
    ObString pivot_expr_alias_;
    common::ObSEArray<ObString, 1, common::ModulePageAllocator, true> column_names_;
    InPair() : pivot_expr_alias_() {}
    ~InPair() {}
    int assign(const InPair &other);
    TO_STRING_KV(K_(exprs), K_(pivot_expr_alias), K_(column_names));
  };

  TransposeItem()
      : old_column_count_(0), is_unpivot_(false), is_incude_null_(false)
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
  inline bool is_pivot() const { return !is_unpivot_; }
  inline bool is_unpivot() const { return is_unpivot_; }
  inline bool is_include_null() const { return is_incude_null_; }
  inline bool is_exclude_null() const { return !is_incude_null_; }
  inline bool need_use_unpivot_op() const
  {
    return is_unpivot_ && in_pairs_.count() > 1;
  }
  void set_pivot() { is_unpivot_ = false; }
  void set_unpivot() { is_unpivot_ = true; }
  void set_include_nulls(const bool is_include_nulls)
  {
    is_incude_null_ = is_include_nulls;
  }
  int assign(const TransposeItem &other);
  int deep_copy(ObIRawExprCopier &expr_copier,
                const TransposeItem &other);
  TO_STRING_KV(K_(old_column_count), K_(is_unpivot), K_(is_incude_null), K_(aggr_pairs),
               K_(for_columns), K_(in_pairs), K_(unpivot_columns), K_(alias_name));

  int64_t old_column_count_;
  bool is_unpivot_;
  bool is_incude_null_;

  common::ObSEArray<AggrPair, 16, common::ModulePageAllocator, true> aggr_pairs_;
  common::ObSEArray<ObString, 16, common::ModulePageAllocator, true> for_columns_;
  common::ObSEArray<InPair, 16, common::ModulePageAllocator, true> in_pairs_;
  common::ObSEArray<ObString, 16, common::ModulePageAllocator, true> unpivot_columns_;

  ObString alias_name_;
};

struct ObUnpivotInfo
{
public:
  OB_UNIS_VERSION(1);
public:
  ObUnpivotInfo()
    : is_include_null_(false),
      old_column_count_(0),
      for_column_count_(0),
      unpivot_column_count_(0)
  {}
  ObUnpivotInfo(const bool is_include_null, const int64_t old_column_count,
                const int64_t for_column_count, const int64_t unpivot_column_count)
    : is_include_null_(is_include_null), old_column_count_(old_column_count),
      for_column_count_(for_column_count), unpivot_column_count_(unpivot_column_count)
  {}

  void reset() { new (this) ObUnpivotInfo(); }
  OB_INLINE bool has_unpivot() const
  { return old_column_count_>= 0 && unpivot_column_count_ > 0 && for_column_count_ > 0; };
  OB_INLINE int64_t get_new_column_count() const
  { return unpivot_column_count_ + for_column_count_; }
  OB_INLINE int64_t get_output_column_count() const
  { return old_column_count_ + get_new_column_count(); }
  TO_STRING_KV(K_(is_include_null), K_(old_column_count), K_(unpivot_column_count),
               K_(for_column_count));

  bool is_include_null_;
  int64_t old_column_count_;
  int64_t for_column_count_;
  int64_t unpivot_column_count_;
};

struct ObJsonTableDef;

struct TableItem
{
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
    for_update_ = false;
    for_update_wait_us_ = -1;
    skip_locked_ = false;
    mock_id_ = common::OB_INVALID_ID;
    node_ = NULL;
    view_base_item_ = NULL;
    flashback_query_expr_ = nullptr;
    flashback_query_type_ = FlashBackQueryType::NOT_USING;
    function_table_expr_ = nullptr;
    is_reverse_link_ = false;
    dblink_id_ = OB_INVALID_ID;
    ddl_schema_version_ = 0;
    ddl_table_id_ = common::OB_INVALID_ID;
    json_table_def_ = nullptr;
    table_type_ = MAX_TABLE_TYPE;
  }

  virtual TO_STRING_KV(N_TID, table_id_,
               N_TABLE_NAME, table_name_,
               N_ALIAS_NAME, alias_name_,
               N_SYNONYM_NAME, synonym_name_,
               "synonym_db_name", synonym_db_name_,
               N_QB_NAME, qb_name_,
               N_TABLE_TYPE, static_cast<int32_t>(type_),
               //"recursive union fake table", is_recursive_union_fake_table_,
               N_REF_ID, ref_id_,
               N_DATABASE_NAME, database_name_,
               N_FOR_UPDATE, for_update_,
               N_WAIT, for_update_wait_us_,
               K_(skip_locked),
               N_MOCK_ID, mock_id_,
               "view_base_item",
               (NULL == view_base_item_ ? OB_INVALID_ID : view_base_item_->table_id_),
               K_(dblink_id), K_(dblink_name), K_(link_database_name), K_(is_reverse_link),
               K_(ddl_schema_version), K_(ddl_table_id),
               K_(is_view_table), K_(part_ids), K_(part_names), K_(cte_type),
               KPC_(function_table_expr),
               K_(flashback_query_type), KPC_(flashback_query_expr), K_(table_type),
               K(table_values_));

  enum TableType
  {
    BASE_TABLE,
    ALIAS_TABLE,
    GENERATED_TABLE,
    JOINED_TABLE,
    CTE_TABLE,
    FUNCTION_TABLE,
    UNPIVOT_TABLE,
    TEMP_TABLE,
    LINK_TABLE,
    JSON_TABLE,
    EXTERNAL_TABLE,
    VALUES_TABLE
  };

  /**
   * with cte(a,b,c) as (select 1,2,3 from dual union all select a+1,b+1,c+1 from cte where a < 10) select * from cte;
   *                                                                               ^                               ^
   *                                                                           FAKE_CTE                      RECURSIVE_CTE
   * with cte(a,b) as (select 1,2 from dual) select * from cte,        t2    where cte.a = t2.c1;
   *                                                        ^           ^
   *                                                    NORMAL_CTE   NOT_CTE
   *
   */
  enum CTEType
  {
    NOT_CTE,
    NORMAL_CTE,
    RECURSIVE_CTE,
    FAKE_CTE
  };

  enum FlashBackQueryType
  {
    NOT_USING,
    USING_TIMESTAMP,
    USING_SCN
  };
  //this table resolved from schema, is base table or alias from base table
  bool is_basic_table() const { return BASE_TABLE == type_ || ALIAS_TABLE == type_; }
  bool is_generated_table() const { return GENERATED_TABLE == type_; }
  bool is_temp_table() const { return TEMP_TABLE == type_; }
  bool is_fake_cte_table() const { return CTE_TABLE == type_; }
  bool is_joined_table() const { return JOINED_TABLE == type_; }
  bool is_function_table() const { return FUNCTION_TABLE == type_; }
  bool is_link_table() const { return OB_INVALID_ID != dblink_id_; } // why not use type_, cause type_ will be changed in dblink transform rule, but dblink id don't change
  bool is_link_type() const { return LINK_TABLE == type_; } // after dblink transformer, LINK_TABLE will be BASE_TABLE, BASE_TABLE will be LINK_TABLE
  bool is_json_table() const { return JSON_TABLE == type_; }
  bool is_values_table() const { return VALUES_TABLE == type_; }//used to mark values statement: values row(1,2), row(3,4);
  bool is_synonym() const { return !synonym_name_.empty(); }
  bool is_oracle_all_or_user_sys_view() const
  {
    return (is_ora_sys_view_table(ref_id_) && (table_name_.prefix_match("USER_") || table_name_.prefix_match("ALL_")));
  }
  bool is_oracle_dba_sys_view() const
  {
    return (is_ora_sys_view_table(ref_id_) && table_name_.prefix_match("DBA_"));
  }
  bool is_oracle_all_or_user_sys_view_for_alias() const
  {
    return ((database_name_ == OB_ORA_SYS_SCHEMA_NAME) && (table_name_.prefix_match("USER_") || table_name_.prefix_match("ALL_")));
  }
  bool access_all_part() const { return part_ids_.empty(); }
  int deep_copy(ObIRawExprCopier &expr_copier,
                const TableItem &other,
                ObIAllocator* allocator = nullptr);
  const common::ObString &get_table_name() const { return alias_name_.empty() ? table_name_ : alias_name_; }
  const common::ObString &get_object_name() const
  {
    return alias_name_.empty() ? (synonym_name_.empty() ? get_table_name() : synonym_name_) : alias_name_;
  }
  const TableItem &get_base_table_item() const
  {
    return (is_generated_table() || is_temp_table()) && view_base_item_ != NULL
        ? view_base_item_->get_base_table_item() : *this;
  }

  ObJsonTableDef* get_json_table_def() { return json_table_def_; }
  int deep_copy_json_table_def(const ObJsonTableDef& jt_def, ObIRawExprCopier &expr_copier, ObIAllocator* allocator);

  virtual bool has_for_update() const { return for_update_; }
  // if real table id, it is valid for all threads,
  // else if generated id, it is unique just during the thread session
  uint64_t    table_id_;
  common::ObString    table_name_;
  common::ObString    alias_name_;
  common::ObString    synonym_name_;
  common::ObString    synonym_db_name_;
  common::ObString    qb_name_; // used for hint
  TableType   type_;
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
  ObSelectStmt *ref_query_;
  bool is_system_table_;
  bool is_index_table_; //just for index table resolver
  bool is_view_table_; //for VIEW privilege check
  bool is_recursive_union_fake_table_; //mark whether this table is a tmp fake table for resolve the recursive cte table
  share::schema::ObTableType table_type_;
  CTEType cte_type_;
  common::ObString database_name_;
  /* FOR UPDATE clause */
  bool for_update_;
  int64_t for_update_wait_us_;//0 means nowait, -1 means infinite
  bool skip_locked_;
  //在hierarchical query, 记录由当前table_item mock出来的table_item的table id
  uint64_t mock_id_;
  const ParseNode* node_;
  // base table item for updatable view
  const TableItem *view_base_item_; // seems to be useful only in the resolve phase
  ObRawExpr *flashback_query_expr_;
  FlashBackQueryType flashback_query_type_;
  ObRawExpr *function_table_expr_;
  // dblink
  bool is_reverse_link_;
  int64_t dblink_id_;
  common::ObString dblink_name_;
  common::ObString link_database_name_;
  int64_t ddl_schema_version_;
  int64_t ddl_table_id_;
  // table partition
  common::ObSEArray<ObObjectID, 1, common::ModulePageAllocator, true> part_ids_;
  common::ObSEArray<ObString, 1, common::ModulePageAllocator, true> part_names_;
  // json table
  ObJsonTableDef* json_table_def_;
  // values table
  common::ObArray<ObRawExpr*, common::ModulePageAllocator, true> table_values_;
};

struct ColumnItem
{
  ColumnItem()
      :column_id_(common::OB_INVALID_ID),
       table_id_(common::OB_INVALID_ID),
       column_name_(),
       expr_(NULL),
       default_value_(),
       auto_filled_timestamp_(false),
       default_value_expr_(NULL),
       default_empty_expr_(NULL),
       base_tid_(common::OB_INVALID_ID),
       base_cid_(common::OB_INVALID_ID),
       col_idx_(common::OB_INVALID_ID),
       is_geo_(false)
  {}
  bool is_invalid() const { return NULL == expr_; }
  const ObColumnRefRawExpr *get_expr() const { return expr_; }
  ObColumnRefRawExpr *get_expr() { return expr_; }
  void set_default_value(const common::ObObj &val)
  {
    default_value_ = val;
  }
  void set_default_value_expr(ObRawExpr *expr)
  {
    default_value_expr_ = expr;
  }
  bool is_auto_increment() const { return expr_ != NULL && expr_->is_auto_increment(); }
  bool is_not_null_for_write() const { return expr_ != NULL && expr_->is_not_null_for_write(); }
  bool is_not_null_for_read() const { return expr_ != NULL && expr_->is_not_null_for_read(); }
  int deep_copy(ObIRawExprCopier &expr_copier,
                const ColumnItem &other);
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
    col_idx_ = common::OB_INVALID_ID;
    default_empty_expr_ = NULL;
    default_value_expr_ = NULL;
    is_geo_ = false;
  }
  void set_ref_id(uint64_t table_id, uint64_t column_id)
  {
    table_id_ = table_id; column_id_ = column_id;
    (NULL != expr_) ? expr_->set_ref_id(table_id, column_id) : (void) 0;
  }
  const ObExprResType *get_column_type() const
  {
    const ObExprResType *column_type = NULL;
    if (expr_ != NULL) {
      column_type = &(expr_->get_result_type());
    }
    return column_type;
  }
  inline uint64_t hash(uint64_t seed) const;
  TO_STRING_KV(N_CID, column_id_,
               N_TID, table_id_,
               N_COLUMN, column_name_,
               K_(auto_filled_timestamp),
               N_DEFAULT_VALUE, default_value_,
               K_(base_tid),
               K_(base_cid),
               N_EXPR, expr_);

  uint64_t column_id_;
  uint64_t table_id_;
  common::ObString column_name_;
  ObColumnRefRawExpr *expr_;
  // the following two are used by DML operations, such as insert or update
  common::ObObj default_value_;
  bool auto_filled_timestamp_;
  // json table reuse default_value_expr_ for on error default
  ObRawExpr *default_value_expr_;
  // add for json table for on emtpy default expr
  ObRawExpr *default_empty_expr_;
  // base table id and column id
  uint64_t base_tid_;
  uint64_t base_cid_;
  uint64_t col_idx_;
  bool is_geo_;
};

inline uint64_t ColumnItem::hash(uint64_t seed) const
{
  seed = common::do_hash(column_id_, seed);
  seed = common::do_hash(column_name_, seed);
  seed = common::do_hash(table_id_, seed);
  seed = common::do_hash(*expr_, seed);
  seed = common::do_hash(is_geo_, seed);

  return seed;
}

typedef struct ObJtColBaseInfo
{
  ObJtColBaseInfo();
  ObJtColBaseInfo(const ObJtColBaseInfo& info);

  int32_t col_type_;
  int32_t truncate_;
  int32_t format_json_;
  int32_t wrapper_;
  int32_t allow_scalar_;
  int64_t output_column_idx_;
  int64_t empty_expr_id_;
  int64_t error_expr_id_;
  ObString col_name_;
  ObString path_;
  int32_t on_empty_;
  int32_t on_error_;
  int32_t on_mismatch_;
  int32_t on_mismatch_type_;
  int64_t res_type_;
  ObDataType data_type_;
  int32_t parent_id_;
  int32_t id_;
  union {
    int32_t value_;
    struct {
      int32_t is_name_quoted_ : 1;
      int32_t reserved_ : 31;
    };
  };

  int deep_copy(const ObJtColBaseInfo& src, ObIAllocator* allocator);
  int assign(const ObJtColBaseInfo& src);

  TO_STRING_KV(K_(col_type), K_(format_json), K_(wrapper), K_(allow_scalar),
   K_(output_column_idx), K_(col_name), K_(path), K_(parent_id), K_(id));
} ObJtColBaseInfo;

typedef struct ObJsonTableDef {
  ObJsonTableDef()
    : all_cols_(),
      doc_expr_(nullptr) {}

  int deep_copy(const ObJsonTableDef& src, ObIRawExprCopier &expr_copier, ObIAllocator* allocator);
  int assign(const ObJsonTableDef& src);
  common::ObSEArray<ObJtColBaseInfo*, 4, common::ModulePageAllocator, true> all_cols_;
  ObRawExpr *doc_expr_;
} ObJsonTableDef;

struct FromItem
{
  FromItem()
    : table_id_(common::OB_INVALID_ID),
      link_table_id_(common::OB_INVALID_ID),
      is_joined_(false)
  {}
  uint64_t   table_id_;
  uint64_t   link_table_id_;
  // false: it is the real table id
  // true: it is the joined table id
  bool      is_joined_;

  bool operator ==(const FromItem &r) const
  { return table_id_ == r.table_id_ && is_joined_ == r.is_joined_; }

  int deep_copy(const FromItem &other);
  TO_STRING_KV(N_TID, table_id_,
               N_IS_JOIN, is_joined_);
};

struct JoinedTable : public TableItem
{
  JoinedTable() :
    joined_type_(UNKNOWN_JOIN),
    left_table_(NULL),
    right_table_(NULL),
    single_table_ids_(common::OB_MALLOC_NORMAL_BLOCK_SIZE),
    join_conditions_(common::OB_MALLOC_NORMAL_BLOCK_SIZE)
  {
  }

  int deep_copy(ObIAllocator &allocator,
                ObIRawExprCopier &expr_copier,
                const JoinedTable &other);
  bool same_as(const JoinedTable &other) const;
  bool is_inner_join() const { return INNER_JOIN == joined_type_; }
  bool is_left_join() const { return LEFT_OUTER_JOIN == joined_type_; }
  bool is_right_join() const { return RIGHT_OUTER_JOIN == joined_type_; }
  bool is_full_join() const { return FULL_OUTER_JOIN == joined_type_; }
  virtual bool has_for_update() const
  {
    return (left_table_ != NULL &&  left_table_->has_for_update())
            || (right_table_ != NULL &&  right_table_->has_for_update());
  }
  common::ObIArray<ObRawExpr*> &get_join_conditions() { return join_conditions_; }
  const common::ObIArray<ObRawExpr*> &get_join_conditions() const { return join_conditions_; }
  TO_STRING_KV(N_TID, table_id_,
               N_TABLE_TYPE, static_cast<int32_t>(type_),
               N_JOIN_TYPE, ob_join_type_str(joined_type_),
               N_LEFT_TABLE, left_table_,
               N_RIGHT_TABLE, right_table_,
               "join_condition", join_conditions_);

  ObJoinType joined_type_;
  TableItem *left_table_;
  TableItem *right_table_;
  common::ObSEArray<uint64_t, 16, common::ModulePageAllocator, true> single_table_ids_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> join_conditions_;
};

class SemiInfo
{
public:
  SemiInfo(ObJoinType join_type = LEFT_SEMI_JOIN) :
    join_type_(join_type),
    semi_id_(common::OB_INVALID_ID),
    right_table_id_(common::OB_INVALID_ID),
    left_table_ids_(),
    semi_conditions_() {}
  virtual ~SemiInfo() {};
  int deep_copy(ObIRawExprCopier &copier,
                const SemiInfo &other);
  inline bool is_semi_join() const { return LEFT_SEMI_JOIN == join_type_ || RIGHT_SEMI_JOIN == join_type_; }
  inline bool is_anti_join() const { return LEFT_ANTI_JOIN == join_type_ || RIGHT_ANTI_JOIN == join_type_; }
  TO_STRING_KV(K_(join_type),
               K_(semi_id),
               K_(left_table_ids),
               K_(right_table_id),
               K_(semi_conditions));

  ObJoinType join_type_;
  uint64_t semi_id_;
  //generate table create from subquery
  uint64_t right_table_id_;
  //table ids which involved right table in parent query
  common::ObSEArray<uint64_t, 8, common::ModulePageAllocator, true> left_table_ids_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> semi_conditions_;
};

/// In fact, ObStmt is ObDMLStmt.
class ObDMLStmt : public ObStmt
{
public:
  struct PartExprItem
  {
    PartExprItem()
        : table_id_(common::OB_INVALID_ID),
          index_tid_(common::OB_INVALID_ID),
          part_expr_(NULL),
          subpart_expr_(NULL)
    {
    }
    uint64_t table_id_;
    uint64_t index_tid_;
    ObRawExpr *part_expr_;
    ObRawExpr *subpart_expr_;
    void reset() {
      table_id_ = common::OB_INVALID_ID;
      index_tid_ = common::OB_INVALID_ID;
      part_expr_ = NULL;
      subpart_expr_ = NULL;
    }
    int deep_copy(ObIRawExprCopier &expr_copier,
                  const PartExprItem &other);
    TO_STRING_KV(K_(table_id),
                 K_(index_tid),
                 KPC_(part_expr),
                 KPC_(subpart_expr));
  };

  enum CheckConstraintFlag
  {
    IS_ENABLE_CHECK       = 1,
    IS_VALIDATE_CHECK     = 1 << 1,
    IS_RELY_CHECK         = 1 << 2
  };
  struct CheckConstraintItem
  {
    CheckConstraintItem()
        : table_id_(common::OB_INVALID_ID),
          ref_table_id_(common::OB_INVALID_ID),
          check_constraint_exprs_(),
          check_flags_()
    {
    }
    void reset() {
      table_id_ = common::OB_INVALID_ID;
      ref_table_id_ = common::OB_INVALID_ID;
      check_constraint_exprs_.reset();
      check_flags_.reset();
    }
    int deep_copy(ObIRawExprCopier &expr_copier,
                  const CheckConstraintItem &other);
    int assign(const CheckConstraintItem &other);
    TO_STRING_KV(K_(table_id),
                 K_(ref_table_id),
                 K_(check_constraint_exprs),
                 K_(check_flags));
    uint64_t table_id_;
    uint64_t ref_table_id_;
    ObSEArray<ObRawExpr*, 4, common::ModulePageAllocator, true> check_constraint_exprs_;
    ObSEArray<int64_t, 4,common::ModulePageAllocator, true> check_flags_;
  };

  typedef common::hash::ObHashMap<uint64_t, int64_t, common::hash::NoPthreadDefendMode,
                                  common::hash::hash_func<uint64_t>,
                                  common::hash::equal_to<uint64_t>,
                                  TableHashAllocator,
                                  common::hash::NormalPointer,
                                  common::ObWrapperAllocator> ObDMLStmtTableHash;
public:

  explicit ObDMLStmt(stmt::StmtType type);
  virtual ~ObDMLStmt();
  int assign(const ObDMLStmt &other);
  int deep_copy(ObStmtFactory &stmt_factory,
                ObRawExprFactory &expr_factory,
                const ObDMLStmt &other);
  int deep_copy(ObStmtFactory &stmt_factory,
                ObRawExprCopier &expr_copier,
                const ObDMLStmt &other);
  virtual int deep_copy_stmt_struct(ObIAllocator &allocator,
                                    ObRawExprCopier &expr_factory,
                                    const ObDMLStmt &other);

  int get_child_table_id_recurseive(common::ObIArray<share::schema::ObObjectStruct> &object_ids,
      const int64_t object_limit_count = common::OB_MAX_TABLE_NUM_PER_STMT) const;
  int get_child_table_id_count_recurseive(int64_t &object_ids_cnt,
      const int64_t object_limit_count = common::OB_MAX_TABLE_NUM_PER_STMT) const;

  virtual int check_table_be_modified(uint64_t ref_table_id, bool& is_modified) const
  {
    UNUSED(ref_table_id);
    is_modified = false;
    return OB_SUCCESS;
  }

  virtual int init_stmt(TableHashAllocator &table_hash_alloc, ObWrapperAllocator &wrapper_alloc) override;

  bool is_hierarchical_query() const;

  int replace_relation_exprs(const common::ObIArray<ObRawExpr *> &other_exprs,
                             const common::ObIArray<ObRawExpr *> &new_exprs);

  int copy_and_replace_stmt_expr(ObRawExprCopier &copier);

  virtual int iterate_stmt_expr(ObStmtExprVisitor &vistor);

  int iterate_joined_table_expr(JoinedTable *joined_table,
                                ObStmtExprVisitor &visitor) const;

  int update_stmt_table_id(const ObDMLStmt &other);
  int set_table_item_qb_name();
  int adjust_qb_name(ObIAllocator *allocator,
                     const ObString &src_qb_name,
                     const ObIArray<uint32_t> &src_hash_val,
                     int64_t *sub_num = NULL);
  int adjust_statement_id(ObIAllocator *allocator,
                          const ObString &src_qb_name,
                          const ObIArray<uint32_t> &src_hash_val,
                          int64_t *sub_num = NULL);
  int recursive_adjust_statement_id(ObIAllocator *allocator,
                                    const ObIArray<uint32_t> &src_hash_val,
                                    int64_t sub_num);
  int get_stmt_by_stmt_id(int64_t stmt_id, ObDMLStmt *&stmt);

  int64_t get_from_item_size() const { return from_items_.count(); }
  void clear_from_items() { from_items_.reset(); }
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
  int remove_from_item(ObIArray<TableItem*> &tables);
  int remove_from_item(uint64_t tid, bool *remove_happened = NULL);
  int remove_joined_table_item(const ObIArray<JoinedTable*> &tables);
  int remove_joined_table_item(const JoinedTable *joined_table);

  TableItem *create_table_item(common::ObIAllocator &allocator);
  int merge_from_items(const ObDMLStmt &stmt);
  const FromItem &get_from_item(int64_t index) const { return from_items_[index]; }
  FromItem &get_from_item(int64_t index) { return from_items_[index]; }
  int64_t get_from_item_idx(uint64_t table_id) const;
  common::ObIArray<FromItem> &get_from_items() { return from_items_; }
  const common::ObIArray<FromItem> &get_from_items() const { return from_items_; }
  common::ObIArray<PartExprItem> &get_part_exprs() { return part_expr_items_; }
  const common::ObIArray<PartExprItem> &get_part_exprs() const{ return part_expr_items_; }

  int get_table_function_exprs(ObIArray<ObRawExpr *> &table_func_exprs) const;
  int get_json_table_exprs(ObIArray<ObRawExpr *> &json_table_exprs) const;

  int reset_from_item(const common::ObIArray<FromItem> &from_items);
  void clear_from_item() { from_items_.reset(); }
  int reset_table_item(const common::ObIArray<TableItem*> &table_items);
  void clear_column_items();

  int remove_part_expr_items(uint64_t table_id);
  int remove_part_expr_items(ObIArray<uint64_t> &table_ids);
  int get_part_expr_items(uint64_t table_id, ObIArray<PartExprItem> &part_items);
  int get_part_expr_items(ObIArray<uint64_t> &table_ids, ObIArray<PartExprItem> &part_items);
  int set_part_expr_items(ObIArray<PartExprItem> &part_items);
  int set_part_expr_item(PartExprItem &part_item);
  ObRawExpr *get_part_expr(uint64_t table_id, uint64_t index_tid) const;
  ObRawExpr *get_subpart_expr(const uint64_t table_id, uint64_t index_tid) const;

  int set_part_expr(uint64_t table_id, uint64_t index_tid, ObRawExpr *part_expr, ObRawExpr *subpart_expr);
  inline ObStmtHint &get_stmt_hint() { return stmt_hint_; }
  inline const ObStmtHint &get_stmt_hint() const { return stmt_hint_; }
  int check_hint_table_matched_table_item(ObCollationType cs_type,
                                          const ObTableInHint &hint_table,
                                          bool &matched) const;
  virtual bool has_subquery() const;
  inline bool has_order_by() const { return (get_order_item_size() > 0); }
  int add_joined_table(JoinedTable *joined_table) { return joined_tables_.push_back(joined_table); }
  JoinedTable *get_joined_table(uint64_t table_id) const;
  const common::ObIArray<JoinedTable*>& get_joined_tables() const { return joined_tables_; }
  common::ObIArray<JoinedTable*> &get_joined_tables() { return joined_tables_; }
  inline int64_t get_order_item_size() const { return order_items_.count(); }
  inline bool is_single_table_stmt() const { return (1 == get_table_size()); }
  int add_semi_info(SemiInfo* info) { return semi_infos_.push_back(info); }
  int remove_semi_info(SemiInfo* info);
  inline common::ObIArray<SemiInfo*> &get_semi_infos() { return semi_infos_; }
  inline const common::ObIArray<SemiInfo*> &get_semi_infos() const { return semi_infos_; }
  inline int64_t get_semi_info_size() const { return semi_infos_.count(); }
  inline void set_limit_offset(ObRawExpr* limit, ObRawExpr* offset)
  {
    limit_count_expr_ = limit;
    limit_offset_expr_ = offset;
  }
  inline bool has_limit() const { return (limit_count_expr_ != NULL || limit_offset_expr_ != NULL || limit_percent_expr_ != NULL); }
  ObRawExpr* get_limit_expr() const { return limit_count_expr_; }
  ObRawExpr*  get_offset_expr() const { return limit_offset_expr_; }
  ObRawExpr* &get_limit_expr() { return limit_count_expr_; }
  ObRawExpr*  &get_offset_expr() { return limit_offset_expr_; }
  ObRawExpr* get_limit_percent_expr() const { return limit_percent_expr_; }
  ObRawExpr* &get_limit_percent_expr() { return limit_percent_expr_; }
  void set_limit_percent_expr(ObRawExpr *percent_expr) { limit_percent_expr_ = percent_expr; }
  void set_fetch_with_ties(bool is_with_ties) { is_fetch_with_ties_ = is_with_ties; }
  bool is_fetch_with_ties() const { return is_fetch_with_ties_; }
  void set_has_fetch(bool has_fetch) { has_fetch_ =  has_fetch; }
  bool has_fetch() const { return has_fetch_; }
  void set_fetch_info(ObRawExpr *offset_expr, ObRawExpr *count_expr, ObRawExpr *percent_expr) {
    set_limit_offset(count_expr, offset_expr);
    limit_percent_expr_ = percent_expr;
  }
  int add_order_item(OrderItem& order_item) { return order_items_.push_back(order_item); }
  inline const OrderItem &get_order_item(int64_t index) const { return order_items_[index]; }
  inline OrderItem &get_order_item(int64_t index) { return order_items_[index]; }
  inline const common::ObIArray<OrderItem> &get_order_items() const { return order_items_; }
  inline common::ObIArray<OrderItem> &get_order_items() { return order_items_; }
  int get_order_exprs(common::ObIArray<ObRawExpr*> &order_exprs) const;
  //提取该stmt中所有表达式的relation id
  int pull_all_expr_relation_id();
  int formalize_stmt(ObSQLSessionInfo *session_info);
  int formalize_relation_exprs(ObSQLSessionInfo *session_info);
  int formalize_stmt_expr_reference();
  int formalize_child_stmt_expr_reference();
  int set_sharable_expr_reference(ObRawExpr &expr, ExplicitedRefType ref_type);
  int check_pseudo_column_valid();
  int get_ora_rowscn_column(const uint64_t table_id, ObPseudoColumnRawExpr *&ora_rowscn);
  virtual int remove_useless_sharable_expr();
  virtual int clear_sharable_expr_reference();
  virtual int get_from_subquery_stmts(common::ObIArray<ObSelectStmt*> &child_stmts) const;
  virtual int get_subquery_stmts(common::ObIArray<ObSelectStmt*> &child_stmts) const;
  int generated_column_depend_column_is_referred(ObRawExpr *expr, bool &has_no_dep);
  int is_referred_by_partitioning_expr(const ObRawExpr *expr,
                                       bool &is_referred);
  int64_t get_table_size() const { return table_items_.count(); }
  int64_t get_CTE_table_size() const;
  int64_t get_column_size() const { return column_items_.count(); }
  int64_t get_column_size(const uint64_t table_id) const {
    int64_t size = 0;
    for (int64_t i = 0; i < column_items_.count(); i++) {
      if (table_id == column_items_.at(i).table_id_) {
        ++size;
      }
    }
    return size;
  }
  inline int64_t get_condition_size() const { return condition_exprs_.count(); }
  void reset_table_items() { table_items_.reset(); }
  const ColumnItem *get_column_item(int64_t index) const
  {
    const ColumnItem *column_item = NULL;
    if (0 <= index && index < column_items_.count()) {
      column_item = &column_items_.at(index);
    }
    return column_item;
  }
  ColumnItem *get_column_item(int64_t index)
  {
    ColumnItem *column_item = NULL;
    if (0 <= index && index < column_items_.count()) {
      column_item = &column_items_.at(index);
    }
    return column_item;
  }
  inline common::ObIArray<ColumnItem> &get_column_items() { return column_items_; }
  inline const common::ObIArray<ColumnItem> &get_column_items() const { return column_items_; }
  int get_column_ids(uint64_t table_id, ObSqlBitSet<> &column_ids)const;
  int get_column_items(uint64_t table_id, ObIArray<ColumnItem> &column_items) const;
  int get_column_items(ObIArray<uint64_t> &table_ids, ObIArray<ColumnItem> &column_items) const;
  int get_column_exprs(ObIArray<ObColumnRefRawExpr*> &column_exprs) const;
  int get_column_exprs(ObIArray<ObRawExpr *> &column_exprs) const;
  int get_column_exprs(ObIArray<TableItem *> &table_items,
                       ObIArray<ObRawExpr *> &column_exprs) const;
  int get_view_output(const TableItem &table,
                      ObIArray<ObRawExpr *> &select_list,
                      ObIArray<ObRawExpr *> &column_list) const;
  int get_ddl_view_output(const TableItem &table,
      ObIArray<ObRawExpr *> &column_list) const;

  int64_t get_user_var_size() const { return user_var_exprs_.count(); }
  inline ObUserVarIdentRawExpr *get_user_var(int64_t index) { return user_var_exprs_.at(index); }
  inline const ObUserVarIdentRawExpr *get_user_var(int64_t index) const
  { return user_var_exprs_.at(index); }
  inline common::ObIArray<ObUserVarIdentRawExpr *> &get_user_vars() { return user_var_exprs_; }
  inline const common::ObIArray<ObUserVarIdentRawExpr *> &get_user_vars() const
  { return user_var_exprs_; }

  int get_joined_item_idx(const TableItem *child_table, int64_t &idx) const;
  int get_table_item(const ObSQLSessionInfo *session_info, const common::ObString &database_name,
                     const common::ObString &table_name, const TableItem *&table_item) const;
  int get_all_table_item_by_tname(const ObSQLSessionInfo *session_info,
                                  const common::ObString &db_name,
                                  const common::ObString &table_name,
                                  common::ObIArray<const TableItem*> &table_items) const;
  bool is_semi_left_table(const uint64_t table_id);
  SemiInfo *get_semi_info_by_id(const uint64_t semi_id);
  int get_general_table_by_id(uint64_t table_id, TableItem *&table);
  int get_general_table_by_id(TableItem *cur_table,
                              const uint64_t table_id,
                              TableItem *&table);
  TableItem *get_table_item_by_id(uint64_t table_id) const;
  int get_table_item_by_id(ObIArray<uint64_t> &table_ids, ObIArray<TableItem*>&tables);
  const TableItem *get_table_item(int64_t index) const { return table_items_.at(index); }
  TableItem *get_table_item(int64_t index) { return table_items_.at(index); }
  int remove_table_item(const TableItem *ti);
  int remove_table_item(const ObIArray<TableItem *> &table_items);
  int remove_table_info(const TableItem *table);
  int remove_table_info(const ObIArray<TableItem *> &table_items);
  TableItem *get_table_item(const FromItem item);
  const TableItem *get_table_item(const FromItem item) const;
  int get_table_item_idx(const TableItem *child_table, int64_t &idx) const;
  int get_table_item_idx(const uint64_t table_id, int64_t &idx) const;

  int relids_to_table_items(const ObRelIds &table_set, ObIArray<TableItem*> &tables) const;
  int relids_to_table_items(const ObSqlBitSet<> &table_set, ObIArray<TableItem*> &tables) const;
  int relids_to_table_ids(const ObSqlBitSet<> &table_set, ObIArray<uint64_t> &table_ids) const;
  int get_table_rel_ids(const TableItem &target, ObSqlBitSet<> &table_set) const;
  int get_table_rel_ids(const ObIArray<uint64_t> &table_ids, ObSqlBitSet<> &table_set) const;
  int get_table_rel_ids(const uint64_t table_id, ObSqlBitSet<> &table_set) const;
  int get_table_rel_ids(const ObIArray<TableItem*> &tables, ObSqlBitSet<> &table_set) const;
  int get_from_tables(ObRelIds &table_set) const;
  int get_from_tables(ObSqlBitSet<> &table_set) const;
  int get_from_tables(common::ObIArray<TableItem*>& from_tables) const;

  int add_table_item(const ObSQLSessionInfo *session_info, TableItem *table_item);
  int add_table_item(const ObSQLSessionInfo *session_info, TableItem *table_item, bool &have_same_table_name);

  int generate_view_name(ObIAllocator &allocator, ObString &view_name, bool is_temp = false);
  int generate_anonymous_view_name(ObIAllocator &allocator, ObString &view_name);
  int generate_func_table_name(ObIAllocator &allocator, ObString &table_name);
  int generate_json_table_name(ObIAllocator &allocator, ObString &table_name);
  int generate_values_table_name(ObIAllocator &allocator, ObString &table_name);
  int append_id_to_view_name(char *buf,
                             int64_t buf_len,
                             int64_t &pos,
                             bool is_temp = false,
                             bool is_anonymous = false);
  int32_t get_table_bit_index(uint64_t table_id) const;
  int set_table_bit_index(uint64_t table_id);
  int assign_tables_hash(const ObDMLStmtTableHash &tables_hash);
  ColumnItem *get_column_item(uint64_t table_id, const common::ObString &col_name);
  ColumnItem *get_column_item(uint64_t table_id, uint64_t column_id);
  int add_column_item(ColumnItem &column_item);
  int add_column_item(ObIArray<ColumnItem> &column_items);
  int remove_column_item(uint64_t table_id, uint64_t column_id);
  int remove_column_item(uint64_t table_id);
  int remove_column_item(ObIArray<uint64_t> &table_ids);
  int remove_column_item(const ObRawExpr *column_expr);
  int remove_column_item(const ObIArray<ObRawExpr *> &column_exprs);
  const ObRawExpr *get_condition_expr(int64_t index) const { return condition_exprs_.at(index); }
  ObRawExpr *get_condition_expr(int64_t index) { return condition_exprs_.at(index); }
  common::ObIArray<ObRawExpr*> &get_condition_exprs() { return condition_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_condition_exprs() const { return condition_exprs_; }
  common::ObIArray<ObRawExpr *> &get_pseudo_column_like_exprs()
  { return pseudo_column_like_exprs_; }
  const common::ObIArray<ObRawExpr *> &get_pseudo_column_like_exprs() const
  { return pseudo_column_like_exprs_; }
  int get_table_pseudo_column_like_exprs(uint64_t table_id, ObIArray<ObRawExpr *> &pseudo_columns);
  int get_table_pseudo_column_like_exprs(ObIArray<uint64_t> &table_id, ObIArray<ObRawExpr *> &pseudo_columns);
  int rebuild_tables_hash();
  int update_rel_ids(ObRelIds &rel_ids, const ObIArray<int64_t> &bit_index_map);
  int update_column_item_rel_id();
  common::ObIArray<TableItem*> &get_table_items() { return table_items_; }
  const common::ObIArray<TableItem*> &get_table_items() const { return table_items_; }
  int get_CTE_table_items(ObIArray<TableItem *> &cte_table_items) const;
  int get_all_CTE_table_items_recursive(ObIArray<TableItem *> &cte_table_items) const;
  const common::ObIArray<uint64_t> &get_nextval_sequence_ids() const { return nextval_sequence_ids_; }
  common::ObIArray<uint64_t> &get_nextval_sequence_ids() { return nextval_sequence_ids_; }
  const common::ObIArray<uint64_t> &get_currval_sequence_ids() const { return currval_sequence_ids_; }
  common::ObIArray<uint64_t> &get_currval_sequence_ids() { return currval_sequence_ids_; }
  int add_nextval_sequence_id(uint64_t id) { return nextval_sequence_ids_.push_back(id); }
  int add_currval_sequence_id(uint64_t id) { return currval_sequence_ids_.push_back(id); }
  bool has_sequence() const { return nextval_sequence_ids_.count() > 0 || currval_sequence_ids_.count() > 0; }
  void clear_sequence()
  {
    nextval_sequence_ids_.reset();
    currval_sequence_ids_.reset();
  }
  bool has_part_key_sequence() const { return has_part_key_sequence_; }
  void set_has_part_key_sequence(const bool v) { has_part_key_sequence_ = v; }
  int add_condition_expr(ObRawExpr *expr) { return condition_exprs_.push_back(expr); }
  int add_condition_exprs(const common::ObIArray<ObRawExpr*> &exprs) { return append(condition_exprs_, exprs); }
  //move from ObStmt
  //user var
  bool is_contains_assignment() const {return is_contains_assignment_;}
  void set_contains_assignment(bool v) {is_contains_assignment_ |= v;}
  void set_calc_found_rows(const bool found_rows) { is_calc_found_rows_ = found_rows; }
  bool is_calc_found_rows() const { return is_calc_found_rows_; }
  void set_has_top_limit(const bool is_top_limit) { has_top_limit_ = is_top_limit; }
  bool has_top_limit()const { return has_top_limit_; }

  virtual bool is_affect_found_rows() const { return false; }
  // if with explict autoinc column, "insert into values" CAN NOT do multi part insert,
  // to ensure intra-partition autoinc ascending
  inline bool with_explicit_autoinc_column() const
  {
    // Since 4.0, there should be only on autoinc column
    return autoinc_params_.count() > 0;
  }
  inline common::ObIArray<share::AutoincParam> &get_autoinc_params() { return autoinc_params_; }
  inline const common::ObIArray<share::AutoincParam> &get_autoinc_params() const { return autoinc_params_; }
  inline void set_affected_last_insert_id(bool affected_last_insert_id) { affected_last_insert_id_ = affected_last_insert_id; }
  inline bool get_affected_last_insert_id() const { return affected_last_insert_id_; }
  inline bool get_affected_last_insert_id() { return affected_last_insert_id_; }
  int add_autoinc_param(share::AutoincParam &autoinc_param) { return autoinc_params_.push_back(autoinc_param); }
  inline void set_dblink_id(int64_t id) { dblink_id_ = id; }
  inline int64_t get_dblink_id() const { return dblink_id_; }
  inline bool is_dblink_stmt() const { return OB_INVALID_ID != dblink_id_; }
  inline void set_reverse_link() { is_reverse_link_ = true; }
  inline bool is_reverse_link() const { return is_reverse_link_; }
  int add_subquery_ref(ObQueryRefRawExpr *query_ref);
  virtual int get_child_stmt_size(int64_t &child_size) const;
  int64_t get_subquery_expr_size() const { return subquery_exprs_.count(); }
  virtual int get_child_stmts(common::ObIArray<ObSelectStmt*> &child_stmts) const;
  virtual int set_child_stmt(const int64_t child_num, ObSelectStmt* child_stmt);
  common::ObIArray<ObQueryRefRawExpr*> &get_subquery_exprs() { return subquery_exprs_; }
  const common::ObIArray<ObQueryRefRawExpr*> &get_subquery_exprs() const { return subquery_exprs_; }
  bool is_set_stmt() const;
  virtual bool has_link_table() const;
  //该函数用于获取该stmt中所有跟语义相关的表达式的根节点
  //用get_relation_exprs()接口代替以前的get_all_expr()接口，
  //该方法的作用是获取该stmt中所有由查询显示指定的expr tree的root节点，
  //例如：select c1+1 from t1 where c1>1 and c2<1;这个查询中，
  //这个表达式中relation expr的节点是select item中c1+1的‘+’表达式，
  //条件表达式中的c1>1的'>'以及c2<1中的'<'，一些stmt会自己生成一些中间表达式，
  //中间表达式并不记录到relation expr中，
  //例如select语句中如果有等值条件会生成equal set，
  //equal set中的表达式不加入到relation exprs中，
  //relation expr中的表达式会进行去重处理，去重的数据结构使用的是hash set，
  //让relation expr trees的flag以及deduce type以及relation id的分析都变得性能更优，
  //避免做一些重复的分析，消耗的内存和时间更少

  int get_relation_exprs(common::ObIArray<ObRawExpr *> &relation_exprs,
                         ObStmtExprGetter &visitor) const;
  //如果希望修改指针的指向，需要使用下面的接口；ignore_scope表示不需要拿的relation
  int get_relation_exprs(common::ObIArray<ObRawExprPointer> &relation_expr_ptrs,
                         ObStmtExprGetter &visitor);
  int get_relation_exprs(common::ObIArray<ObRawExpr *> &relation_exprs) const;
  int get_relation_exprs(common::ObIArray<ObRawExprPointer> &relation_expr_ptrs);

  //this func is used for enum_set_wrapper to get exprs which need to be handled
  int get_relation_exprs_for_enum_set_wrapper(common::ObIArray<ObRawExpr*> &rel_array);
  ColumnItem *get_column_item_by_id(uint64_t table_id, uint64_t column_id) const;
  const ColumnItem *get_column_item_by_base_id(uint64_t table_id, uint64_t base_column_id) const;
  ObColumnRefRawExpr *get_column_expr_by_id(uint64_t table_id, uint64_t column_id) const;
  int get_column_exprs(uint64_t table_id, ObIArray<ObColumnRefRawExpr *> &table_cols) const;
  int get_column_exprs(uint64_t table_id, ObIArray<ObRawExpr*> &table_cols) const;

  int find_var_assign_in_query_ctx(bool &is_found) const;
  int check_user_vars_has_var_assign(bool &has_var_assign) const;
  int has_ref_assign_user_var(bool &has_ref_user_var, bool need_check_child = true) const;
  int check_has_var_assign_rec(bool &has_ref_user_var, bool need_check_child) const;
  int check_var_assign(bool &has_var_assign, bool &is_var_assign_only_in_root) const;
  int check_has_var_assign_rec(bool &has_var_assign, bool &is_var_assign_only_in_root, bool is_root) const;

  int get_temp_table_ids(ObIArray<uint64_t> &temp_table_ids);

  common::ObIArray<CheckConstraintItem> &get_check_constraint_items() {
    return check_constraint_items_; }
  const common::ObIArray<CheckConstraintItem> &get_check_constraint_items() const {
    return check_constraint_items_; }
  int set_check_constraint_item(CheckConstraintItem &check_constraint_item);
  int remove_check_constraint_item(const uint64_t table_id);
  int get_check_constraint_items(const uint64_t table_id,
                                 CheckConstraintItem &check_constraint_item);

  int get_qb_name(ObString &qb_name) const;

  TO_STRING_KV(N_STMT_TYPE, ((int)stmt_type_),
               N_TABLE, table_items_,
               N_PARTITION_EXPR, part_expr_items_,
               N_COLUMN, column_items_,
               N_COLUMN, nextval_sequence_ids_,
               N_COLUMN, currval_sequence_ids_,
               N_WHERE, condition_exprs_,
               N_ORDER_BY, order_items_,
               N_LIMIT, limit_count_expr_,
               N_OFFSET, limit_offset_expr_,
               N_STMT_HINT, stmt_hint_,
               N_SUBQUERY_EXPRS, subquery_exprs_,
               N_USER_VARS, user_var_exprs_,
               K_(dblink_id),
               K_(is_reverse_link));

  int check_if_contain_inner_table(bool &is_contain_inner_table) const;
  int check_if_contain_select_for_update(bool &is_contain_select_for_update) const;
  int check_if_table_exists(uint64_t table_id, bool &is_existed) const;
  bool has_for_update() const;
  int get_rownum_expr(ObRawExpr *&expr) const;
  int has_rownum(bool &has_rownum) const;
  bool has_ora_rowscn() const;
  int get_sequence_expr(ObRawExpr *&expr,
                        const common::ObString seq_name, // sequence object name
                        const common::ObString seq_action, // NEXTVAL or CURRVAL
                        const uint64_t seq_id) const;
  int get_sequence_exprs(common::ObIArray<ObRawExpr *> &exprs) const;
  int has_rand(bool &has_rand) const { return has_special_expr(CNT_RAND_FUNC, has_rand); }
  virtual int has_special_expr(const ObExprInfoFlag, bool &has) const;
  const TransposeItem *get_transpose_item() const { return transpose_item_; }
  void set_transpose_item(const TransposeItem *transpose_item) { transpose_item_ = transpose_item; }
  const ObUnpivotInfo get_unpivot_info() const
  {
    return (transpose_item_ != NULL
            ? ObUnpivotInfo(transpose_item_->is_include_null(),
                            transpose_item_->old_column_count_,
                            transpose_item_->for_columns_.count(),
                            transpose_item_->unpivot_columns_.count())
            : ObUnpivotInfo());
  }
  bool is_unpivot_select() const
  {
    const ObUnpivotInfo &unpivot_info = get_unpivot_info();
    return unpivot_info.has_unpivot();
  }
  int remove_subquery_expr(const ObRawExpr *expr);
  // rebuild query ref exprs
  int adjust_subquery_list();
  int get_stmt_equal_sets(EqualSets &equal_sets,
                          ObIAllocator &allocator,
                          const bool is_strict,
                          const bool check_having = false) const;
  virtual int get_equal_set_conditions(ObIArray<ObRawExpr *> &conditions,
                                       const bool is_strict,
                                       const bool check_having = false) const;
  int get_where_scope_conditions(ObIArray<ObRawExpr *> &conditions,
                                 bool outer_semi_only = false) const;
  static int extract_equal_condition_from_joined_table(const TableItem *table,
                                                       ObIArray<ObRawExpr *> &equal_set_conditions,
                                                       const bool is_strict);
  virtual bool is_returning() const { return false; }
  virtual bool has_instead_of_trigger() const { return false; }
  int has_lob_column(int64_t table_id, bool &has_lob)const;
  int has_virtual_generated_column(int64_t table_id, bool &has_virtual_col) const;

  struct TempTableInfo {
    TempTableInfo()
    :table_items_(),
    upper_stmts_(),
    temp_table_query_(NULL)
    {}

    virtual ~TempTableInfo(){}

    TO_STRING_KV(
      K_(table_items)
    );

    ObSEArray<TableItem*, 8> table_items_;
    ObSEArray<ObDMLStmt*, 8> upper_stmts_;
    ObSelectStmt *temp_table_query_;
  };
  int collect_temp_table_infos(ObIArray<TempTableInfo> &temp_table_infos);
  int get_stmt_rowid_exprs(ObIArray<ObRawExpr *> &rowid_exprs);
  int check_and_get_same_rowid_expr(const ObRawExpr *expr, ObRawExpr *&same_rowid_expr);
  int add_cte_definition(TableItem * table_item) { return cte_definitions_.push_back(table_item); }
  int64_t get_cte_definition_size() const { return cte_definitions_.count(); }
  common::ObIArray<TableItem *>& get_cte_definitions() { return cte_definitions_; }
  const common::ObIArray<TableItem *>& get_cte_definitions() const { return cte_definitions_; }

  int check_has_subquery_in_function_table(bool &has_subquery_in_function_table) const;

  int disable_writing_external_table(bool basic_stmt_is_dml = false);
  int formalize_query_ref_exprs();

  int formalize_query_ref_exec_params(ObStmtExecParamFormatter &formatter,
                                      bool need_replace);

  int check_has_cursor_expression(bool &has_cursor_expr) const;
  bool is_values_table_query() const;

  int do_formalize_query_ref_exprs_pre();

  int do_formalize_query_ref_exprs_post();

protected:
  int create_table_item(TableItem *&table_item);
  //获取到stmt中所有查询相关的表达式(由查询语句中指定的属性生成的表达式)的root expr

protected:
  int deep_copy_join_tables(ObIAllocator &allocator,
                            ObIRawExprCopier &expr_copier,
                            const ObDMLStmt &other);
  int construct_join_table(const ObDMLStmt &other,
                           const JoinedTable &other_joined_table,
                           JoinedTable &joined_table);
  int update_table_item_id_for_joined_table(const ObDMLStmt &other_stmt,
                                            const JoinedTable &other,
                                            JoinedTable &current);
  int update_table_item_id(const ObDMLStmt &other,
                           const TableItem &old_item,
                           const bool has_bit_index,
                           TableItem &new_item);

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
  ObRawExpr*  limit_count_expr_;
  ObRawExpr*  limit_offset_expr_;
  ObRawExpr*  limit_percent_expr_;
  bool has_fetch_;
  bool is_fetch_with_ties_;
  // table_references
  common::ObSEArray<FromItem, 8, common::ModulePageAllocator, true> from_items_;
  common::ObSEArray<PartExprItem, 8, common::ModulePageAllocator, true> part_expr_items_;
  common::ObSEArray<JoinedTable*, 8, common::ModulePageAllocator, true> joined_tables_;
  ObStmtHint stmt_hint_;
  // semi info for semi join
  common::ObSEArray<SemiInfo*, 4, common::ModulePageAllocator, true> semi_infos_;

  //move from ObStmt
  common::ObSEArray<share::AutoincParam, 2, common::ModulePageAllocator, true> autoinc_params_;  // auto-increment related
  //member for found_rows
  bool is_calc_found_rows_;
  bool has_top_limit_; // no longer used, should be removed
  //if the stmt  contains user variable assignment
  //such as @a:=123
  //we may need to serialize the map to remote server
  bool is_contains_assignment_;
  bool affected_last_insert_id_;
  // insert into values (s1.nextval, ...) s1.nextval 对应位置正好是一个分区列
  // 就设置这个标记为 true，提示生成 multi-dml 计划
  bool has_part_key_sequence_;
  // sequence 对象个数，用于 ObSequence 计算 nextval，已去重
  common::ObSEArray<uint64_t, 2> nextval_sequence_ids_;
  // sequence 对象个数，用于记录 currval 的sequence id，已去重
  common::ObSEArray<uint64_t, 2> currval_sequence_ids_;
  // `table_items` 在resolve_from_clause的时候生成, 顺序是从SQL语句左到右push_back的.
  common::ObSEArray<TableItem *, 4, common::ModulePageAllocator, true> table_items_;
  common::ObSEArray<ColumnItem, 16, common::ModulePageAllocator, true> column_items_;
  common::ObSEArray<ObRawExpr *, 16, common::ModulePageAllocator, true> condition_exprs_;
  // 存放共享的类伪列表达式, 我们认为除了一般的伪列表达式ObPseudoColumnRawExpr, rownum和sequence也属于伪列
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> pseudo_column_like_exprs_;
  ObDMLStmtTableHash tables_hash_;
  common::ObSEArray<ObQueryRefRawExpr*, 4, common::ModulePageAllocator, true> subquery_exprs_;
  const TransposeItem *transpose_item_;

  common::ObSEArray<ObUserVarIdentRawExpr *, 4, common::ModulePageAllocator, true> user_var_exprs_;
  common::ObSEArray<CheckConstraintItem, 8, common::ModulePageAllocator, true> check_constraint_items_;
  /*
    keep all cte table defined in current level. Only used for print stmt.
    Needn't maintain it after resolver.
  */
  common::ObSEArray<TableItem*, 2, common::ModulePageAllocator, true> cte_definitions_;
  /*
   * If the current needs to be executed at the remote end, dblink_id indicates the remote definition
   */
  int64_t dblink_id_;
  bool is_reverse_link_;
};

template <typename T>
int deep_copy_stmt_object(ObIAllocator &allocator,
                          ObIRawExprCopier &expr_copier,
                          const T *obj,
                          T *&new_obj)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  new_obj = NULL;
  if (OB_LIKELY(NULL != obj)) {
    if (OB_ISNULL(ptr = allocator.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(WARN, "failed to allocate memory", K(ret), K(lbt()));
    } else {
      new_obj = new (ptr) T();
      if (OB_FAIL(new_obj->deep_copy(expr_copier, *obj))) {
        SQL_RESV_LOG(WARN, "failed to deep copy obj", K(ret));
      }
    }
  }
  return ret;
}


template <typename T>
int deep_copy_stmt_objects(ObIAllocator &allocator,
                           ObIRawExprCopier &expr_copier,
                           const ObIArray<T *> &objs,
                           ObIArray<T *> &new_objs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < objs.count(); ++i) {
    const T *obj = objs.at(i);
    void *ptr = NULL;
    T *new_obj = NULL;
    if (OB_LIKELY(NULL != obj)) {
      if (OB_ISNULL(ptr = allocator.alloc(sizeof(T)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(WARN, "failed to allocate memory", K(ret), K(lbt()));
      } else {
        new_obj = new (ptr) T();
        if (OB_FAIL(new_obj->deep_copy(expr_copier, *obj))) {
          SQL_RESV_LOG(WARN, "failed to deep copy obj", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(new_objs.push_back(new_obj))) {
      SQL_RESV_LOG(WARN, "failed to push back new object", K(ret));
    }
  }
  return ret;
}

template <typename T>
int deep_copy_stmt_objects(ObIRawExprCopier &expr_copier,
                           const ObIArray<T> &objs,
                           ObIArray<T> &new_objs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < objs.count(); ++i) {
    const T &obj = objs.at(i);
    T new_obj;
    if (OB_FAIL(new_obj.deep_copy(expr_copier, obj))) {
      SQL_RESV_LOG(WARN, "failed to deep copy object", K(ret));
    } else if (OB_FAIL(new_objs.push_back(new_obj))) {
      SQL_RESV_LOG(WARN, "failed to push back new obj", K(ret));
    }
  }
  return ret;
}

}
}

#endif //OCEANBASE_SQL_STMT_H_
