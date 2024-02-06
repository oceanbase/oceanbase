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

#ifndef OCEANBASE_SQL_DEL_UPD_STMT_H_
#define OCEANBASE_SQL_DEL_UPD_STMT_H_
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "lib/string/ob_string.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{

/// assignment: column_expr = expr
struct ObAssignment
{
  ObColumnRefRawExpr *column_expr_;
  ObRawExpr *expr_;
  bool is_duplicated_; //for judging whether or not the column is updated repeatedly
  bool is_implicit_; // not in update set clause, but add for inner implement,
  bool is_predicate_column_; //mark whether update the predicate column
                     // see ObDMLResolver::resolve_additional_assignments().
  ObAssignment()
  {
    column_expr_ = NULL;
    expr_ = NULL;
    is_duplicated_ = false;
    is_implicit_ = false;
    is_predicate_column_ = false;
  }

  int deep_copy(ObIRawExprCopier &expr_factory,
                const ObAssignment &other);
  int assign(const ObAssignment &other);
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

  int hash(uint64_t &hash_val, uint64_t seed) const
  {
    hash_val = hash(seed);
    return OB_SUCCESS;
  }

  TO_STRING_KV(N_COLUMN, column_expr_,
               N_EXPR, expr_,
               K_(is_predicate_column));
};
typedef common::ObSEArray<ObAssignment, 16, common::ModulePageAllocator, true> ObAssignments;

enum ObDmlTableType
{
  INVALID_TABLE = 0,
  DELETE_TABLE,
  UPDATE_TABLE,
  INSERT_TABLE,
  MERGE_TABLE,
  INSERT_ALL_TABLE,
};

class ObDmlTableInfo
{
public:
ObDmlTableInfo(ObDmlTableType table_type)
  : table_id_(OB_INVALID_ID),
    loc_table_id_(OB_INVALID_ID),
    ref_table_id_(OB_INVALID_ID),
    table_name_(),
    table_type_(table_type),
    column_exprs_(),
    check_constraint_exprs_(),
    view_check_exprs_(),
    part_ids_(),
    is_link_table_(false),
    need_filter_null_(false)
  {}
  virtual ~ObDmlTableInfo() {}

  virtual int assign(const ObDmlTableInfo &other);

  virtual int deep_copy(ObIRawExprCopier &expr_copier,
                        const ObDmlTableInfo &other);

  virtual int iterate_stmt_expr(ObStmtExprVisitor &visitor);

  inline bool is_update_table() { return ObDmlTableType::UPDATE_TABLE == table_type_; }
  inline bool is_delete_table() { return ObDmlTableType::DELETE_TABLE == table_type_; }
  inline bool is_merge_table() { return ObDmlTableType::MERGE_TABLE == table_type_; }
  inline bool is_insert_table() const { return ObDmlTableType::INSERT_TABLE == table_type_; }
  inline bool is_insert_all_table() { return ObDmlTableType::INSERT_ALL_TABLE == table_type_; }

  TO_STRING_KV(K_(table_id),
               K_(loc_table_id),
               K_(ref_table_id),
               K_(table_name),
               K_(table_type),
               K_(column_exprs),
               K_(check_constraint_exprs),
               K_(view_check_exprs),
               K_(need_filter_null));
  //
  // e.g.:
  //   create view V as select * from T1 as T;
  //   update V set ...;
  //
  //   table_id_: table_id_ of V table item
  //   loc_table_id_: table_id_ of T table item
  //   ref_table_id: ref_id_ of T table item
  //
  //
  uint64_t table_id_; // table id for the view table item
  uint64_t loc_table_id_; // table id for the updated table
  uint64_t ref_table_id_; // refer table id for the updated table
  common::ObString table_name_;
  ObDmlTableType table_type_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> column_exprs_;
  // check exprs for each dml table
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> check_constraint_exprs_;
  // check exprs for updatable view
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> view_check_exprs_;
  //partition used for base table
  common::ObSEArray<ObObjectID, 1, common::ModulePageAllocator, true> part_ids_;
  bool is_link_table_;
  bool need_filter_null_;
};

class ObDeleteTableInfo: public ObDmlTableInfo
{
public:
  ObDeleteTableInfo() :
      ObDmlTableInfo(ObDmlTableType::DELETE_TABLE)
  {
  }
  virtual ~ObDeleteTableInfo()
  {
  }
};

class ObUpdateTableInfo: public ObDmlTableInfo
{
public:
  ObUpdateTableInfo() :
      ObDmlTableInfo(ObDmlTableType::UPDATE_TABLE),
      assignments_()
  {
  }
  virtual ~ObUpdateTableInfo()
  {
  }

  int assign(const ObUpdateTableInfo &other);

  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObUpdateTableInfo &other);

  int iterate_stmt_expr(ObStmtExprVisitor &visitor) override;

  TO_STRING_KV(K_(table_id),
               K_(loc_table_id),
               K_(ref_table_id),
               K_(table_name),
               K_(table_type),
               K_(column_exprs),
               K_(check_constraint_exprs),
               K_(view_check_exprs),
               K_(need_filter_null),
               K_(assignments));
  ObAssignments assignments_;
};

class ObInsertTableInfo: public ObDmlTableInfo
{
public:
  ObInsertTableInfo() :
      ObDmlTableInfo(ObDmlTableType::INSERT_TABLE),
      is_replace_(false),
      values_desc_(),
      values_vector_(),
      column_conv_exprs_(),
      assignments_()
  {
  }
  ObInsertTableInfo(ObDmlTableType dml_type) :
      ObDmlTableInfo(dml_type),
      is_replace_(false),
      values_desc_(),
      values_vector_(),
      column_conv_exprs_(),
      part_generated_col_dep_cols_(),
      assignments_()
  {
  }
  virtual ~ObInsertTableInfo()
  {
  }
  int assign(const ObInsertTableInfo &other);

  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObInsertTableInfo &other);

  int iterate_stmt_expr(ObStmtExprVisitor &visitor) override;

  TO_STRING_KV(K_(table_id),
               K_(loc_table_id),
               K_(ref_table_id),
               K_(table_name),
               K_(table_type),
               K_(column_exprs),
               K_(check_constraint_exprs),
               K_(view_check_exprs),
               K_(is_replace),
               K_(values_desc),
               K_(values_vector),
               K_(column_conv_exprs),
               K_(part_generated_col_dep_cols),
               K_(assignments));
  bool is_replace_;  // replace semantic for mysql
  // 下面两个变量组合在一起描述了 INSERT 的 VALUES 结构
  // 以 INSERT INTO T1 (i, j, k) VALUES (1,2,3),(4,5,6) 为例：
  //  - values_desc_ 的大小为 3，里面保存了 i, j, k 三列的 column reference expr
  //  - value_vectors_ 的大小为 6，保存的内容为 1,2,3,4,5,6 这几个表达式
  common::ObSEArray<ObColumnRefRawExpr*, 16, common::ModulePageAllocator, true> values_desc_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> values_vector_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> column_conv_exprs_;
  // if generated col is partition key in heap table, we need to store all dep cols,
  // eg:
  //  create table t1(c0 int, c1 int, c2 int as (c0 + c1)) partition by hash(c2);
  //  insert into t1(c0) values(1);
  // part_generated_col_dep_cols_ store c1.
  common::ObSEArray<ObColumnRefRawExpr*, 16, common::ModulePageAllocator, true> part_generated_col_dep_cols_;
  ObAssignments assignments_;
};

class ObMergeTableInfo: public ObInsertTableInfo
{
public:
  ObMergeTableInfo() :
      ObInsertTableInfo(ObDmlTableType::MERGE_TABLE),
      source_table_id_(OB_INVALID_ID),
      target_table_id_(OB_INVALID_ID),
      match_condition_exprs_(),
      insert_condition_exprs_(),
      update_condition_exprs_(),
      delete_condition_exprs_()
  {
  }
  virtual ~ObMergeTableInfo()
  {
  }
  int assign(const ObMergeTableInfo &other);

  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObMergeTableInfo &other);

  int iterate_stmt_expr(ObStmtExprVisitor &visitor) override;

  TO_STRING_KV(K_(table_id),
               K_(loc_table_id),
               K_(ref_table_id),
               K_(table_name),
               K_(table_type),
               K_(column_exprs),
               K_(check_constraint_exprs),
               K_(view_check_exprs),
               K_(is_replace),
               K_(values_desc),
               K_(values_vector),
               K_(column_conv_exprs),
               K_(assignments));
  uint64_t source_table_id_;
  uint64_t target_table_id_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> match_condition_exprs_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> insert_condition_exprs_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> update_condition_exprs_;
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> delete_condition_exprs_;
};

class ObInsertAllTableInfo: public ObInsertTableInfo
{
public:
  ObInsertAllTableInfo() :
      ObInsertTableInfo(ObDmlTableType::INSERT_ALL_TABLE),
      when_cond_idx_(OB_INVALID_ID),
      when_cond_exprs_()
  {
  }
  virtual ~ObInsertAllTableInfo()
  {
  }
  int assign(const ObInsertAllTableInfo &other);

  int deep_copy(ObIRawExprCopier &expr_copier,
                const ObInsertAllTableInfo &other);

  int iterate_stmt_expr(ObStmtExprVisitor &visitor) override;

  TO_STRING_KV(K_(table_id),
               K_(loc_table_id),
               K_(ref_table_id),
               K_(table_name),
               K_(table_type),
               K_(column_exprs),
               K_(check_constraint_exprs),
               K_(view_check_exprs),
               K_(is_replace),
               K_(values_desc),
               K_(values_vector),
               K_(column_conv_exprs),
               K_(assignments),
               K_(when_cond_idx),
               K_(when_cond_exprs));

  int64_t when_cond_idx_;//属于第几个条件对应的插入表
  common::ObSEArray<ObRawExpr*, 16, common::ModulePageAllocator, true> when_cond_exprs_;
};

struct ObUniqueConstraintInfo
{
  ObUniqueConstraintInfo()
      : table_id_(common::OB_INVALID_ID),
        index_tid_(common::OB_INVALID_ID),
        constraint_name_(),
        constraint_columns_()
  {
  }
  TO_STRING_KV(K_(table_id),
               K_(index_tid),
               K_(constraint_name),
               K_(constraint_columns));
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    index_tid_ = common::OB_INVALID_ID;
    constraint_name_.reset();
    constraint_columns_.reset();
  }
  int assign(const ObUniqueConstraintInfo &other);
  int deep_copy(const ObUniqueConstraintInfo &other,
                ObIRawExprCopier &expr_copier);
  uint64_t table_id_;
  uint64_t index_tid_;
  common::ObString constraint_name_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> constraint_columns_;
};
struct ObErrLogInfo
{
  ObErrLogInfo()
    : is_error_log_(false),
      table_id_(OB_INVALID_ID),
      table_name_(),
      database_name_(),
      reject_limit_(0),
      error_log_exprs_()
  {
  }
  int assign(const ObErrLogInfo &other);
  int deep_copy(const ObErrLogInfo &other,
                ObRawExprCopier &expr_copier);
  TO_STRING_KV(K_(is_error_log),
               K_(table_id),
               K_(table_name),
               K_(database_name),
               K_(reject_limit),
               K_(error_log_exprs));
  bool is_error_log_;
  uint64_t table_id_;
  ObString table_name_;
  ObString database_name_;
  int64_t reject_limit_;
  common::ObSEArray<ObColumnRefRawExpr*, 4, common::ModulePageAllocator, true> error_log_exprs_;
};

class ObDelUpdStmt : public ObDMLStmt
{
public:
  explicit ObDelUpdStmt(stmt::StmtType type)
      : ObDMLStmt(type),
        returning_exprs_(),
        returning_strs_(),
        returning_agg_items_(),
        ignore_(false),
        has_global_index_(false),
        error_log_info_(),
        has_instead_of_trigger_(false),
        ab_stmt_id_expr_(nullptr),
        dml_source_from_join_(false)
  { }
  virtual ~ObDelUpdStmt() { }
  int deep_copy_stmt_struct(ObIAllocator &allocator,
                            ObRawExprCopier &expr_factory,
                            const ObDMLStmt &other) override;
  int assign(const ObDelUpdStmt &other);
  virtual void set_ignore(bool ignore) { ignore_ = ignore; }
  virtual bool is_ignore() const { return ignore_; }
  bool is_returning() const override { return !returning_exprs_.empty(); }
  int add_value_to_returning_exprs(ObRawExpr *expr) { return returning_exprs_.push_back(expr); }
  const common::ObIArray<ObRawExpr*> &get_returning_exprs() const { return returning_exprs_; }
  common::ObIArray<ObRawExpr*> &get_returning_exprs() { return returning_exprs_; }
  common::ObIArray<ObRawExpr*> &get_returning_into_exprs() { return returning_into_exprs_; }
  const common::ObIArray<ObRawExpr*> &get_returning_into_exprs() const { return returning_into_exprs_; }
  int add_value_to_returning_strs(ObString str) { return returning_strs_.push_back(str); }
  const common::ObIArray<ObString> &get_returning_strs() const { return returning_strs_; }
  int add_returning_agg_item(ObAggFunRawExpr &agg_expr)
  {
    agg_expr.set_explicited_reference();
    return returning_agg_items_.push_back(&agg_expr);
  }
  int64_t get_returning_aggr_item_size() const { return returning_agg_items_.size(); }
  const common::ObIArray<ObAggFunRawExpr*> &get_returning_aggr_items() const
  { return returning_agg_items_; }
  common::ObIArray<ObAggFunRawExpr*> &get_returning_aggr_items()
  { return returning_agg_items_; }
  bool has_global_index() const { return has_global_index_; }
  void set_has_global_index(bool has_global_index) { has_global_index_ |= has_global_index; }
  bool is_dml_table_from_join() const;
  virtual int64_t get_instead_of_trigger_column_count() const;
  int update_base_tid_cid();
  virtual int iterate_stmt_expr(ObStmtExprVisitor &visitor) override;

  void set_is_error_logging(bool is_error_logging) { error_log_info_.is_error_log_ = is_error_logging; }
  bool is_error_logging() const { return error_log_info_.is_error_log_; }
  void set_err_log_table_name(ObString err_log_table_name) { error_log_info_.table_name_ = err_log_table_name; }
  const ObString get_err_log_table_name() const { return error_log_info_.table_name_; }
  void set_err_log_database_name(ObString err_log_database_name) { error_log_info_.database_name_ = err_log_database_name; }
  const ObString get_err_log_database_name() const { return error_log_info_.database_name_; }
  void set_err_log_table_id(uint64_t err_log_table_id) { error_log_info_.table_id_ = err_log_table_id; }
  uint64_t get_err_log_table_id() { return error_log_info_.table_id_; }
  void set_err_log_reject_limit(int64_t reject_limit) { error_log_info_.reject_limit_ = reject_limit; }
  int64_t get_err_log_reject_limit() const { return error_log_info_.reject_limit_; }
  const ObErrLogInfo &get_error_log_info() const { return error_log_info_; }
  ObErrLogInfo &get_error_log_info() { return error_log_info_; }
  bool has_instead_of_trigger() const override { return has_instead_of_trigger_; }
  inline void set_has_instead_of_trigger(bool v) { has_instead_of_trigger_ = v; }
  void set_ab_stmt_id_expr(ObRawExpr *ab_stmt_id) { ab_stmt_id_expr_ = ab_stmt_id; }
  ObRawExpr *get_ab_stmt_id_expr() const { return ab_stmt_id_expr_; }
  virtual uint64_t get_trigger_events() const = 0;
  common::ObIArray<ObRawExpr *> &get_sharding_conditions() { return sharding_conditions_; }
  const common::ObIArray<ObRawExpr *> &get_sharding_conditions() const { return sharding_conditions_; }
  int check_part_key_is_updated(const common::ObIArray<ObAssignment> &assigns,
                                bool &is_updated) const;
  virtual int get_assignments_exprs(ObIArray<ObRawExpr*> &exprs) const;
  virtual int get_dml_table_infos(ObIArray<ObDmlTableInfo*>& dml_table_info) = 0;
  virtual int get_dml_table_infos(ObIArray<const ObDmlTableInfo*>& dml_table_info) const = 0;
  virtual int get_view_check_exprs(ObIArray<ObRawExpr*>& view_check_exprs) const;
  virtual int get_value_exprs(ObIArray<ObRawExpr *> &value_exprs) const;
  virtual int remove_table_item_dml_info(const TableItem* table);
  int has_dml_table_info(const uint64_t table_id, bool &has) const;
  int check_dml_need_filter_null();
  int extract_need_filter_null_table(const JoinedTable *cur_table, ObIArray<uint64_t> &table_ids);
  void set_dml_source_from_join(bool from_join) { dml_source_from_join_ = from_join; }
  inline bool dml_source_from_join() const { return dml_source_from_join_; }
  int check_dml_source_from_join();
protected:
  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> returning_exprs_;
  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> returning_into_exprs_;
  common::ObSEArray<ObString, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> returning_strs_;
  common::ObArray<ObAggFunRawExpr*, common::ModulePageAllocator, true> returning_agg_items_;
  bool ignore_;
  bool has_global_index_;
  ObErrLogInfo error_log_info_;
  bool has_instead_of_trigger_; // for instead of trigger, the trigger need to fired
  // for insert and merge stmt
  common::ObSEArray<ObRawExpr *, 16, common::ModulePageAllocator, true> sharding_conditions_;
  ObRawExpr *ab_stmt_id_expr_; //for array binding batch execution to mark the stmt id
  bool dml_source_from_join_;
};
}
}

#endif // OCEANBASE_SQL_DEL_UPD_STMT_H_
