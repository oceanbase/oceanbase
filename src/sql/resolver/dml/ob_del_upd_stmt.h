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

namespace oceanbase {
namespace sql {

struct IndexDMLInfo {
public:
  IndexDMLInfo()
  {
    reset();
  }
  inline void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    loc_table_id_ = common::OB_INVALID_ID;
    index_tid_ = common::OB_INVALID_ID;
    index_name_.reset();
    part_cnt_ = common::OB_INVALID_ID;
    all_part_num_ = 0;
    rowkey_cnt_ = 0;
    column_exprs_.reset();
    column_convert_exprs_.reset();
    primary_key_ids_.reset();
    part_key_ids_.reset();
    assignments_.reset();
    need_filter_null_ = false;
    distinct_algo_ = T_DISTINCT_NONE;
    calc_part_id_exprs_.reset();
  }
  int64_t to_explain_string(char* buf, int64_t buf_len, ExplainType type) const;
  int init_assignment_info(const ObAssignments& assignments);
  int add_spk_assignment_info(ObRawExprFactory& expr_factory);

  int deep_copy(ObRawExprFactory& expr_factory, const IndexDMLInfo& other);

  uint64_t hash(uint64_t seed) const
  {
    seed = do_hash(table_id_, seed);
    seed = do_hash(loc_table_id_, seed);
    seed = do_hash(index_tid_, seed);
    seed = do_hash(rowkey_cnt_, seed);
    seed = do_hash(part_cnt_, seed);
    seed = do_hash(all_part_num_, seed);
    for (int64_t i = 0; i < column_exprs_.count(); ++i) {
      if (NULL != column_exprs_.at(i)) {
        seed = do_hash(*column_exprs_.at(i), seed);
      }
    }
    for (int64_t i = 0; i < column_convert_exprs_.count(); ++i) {
      if (NULL != column_convert_exprs_.at(i)) {
        seed = do_hash(*column_convert_exprs_.at(i), seed);
      }
    }
    for (int64_t i = 0; i < assignments_.count(); ++i) {
      seed = do_hash(assignments_.at(i), seed);
    }
    seed = do_hash(need_filter_null_, seed);
    seed = do_hash(distinct_algo_, seed);
    for (int64_t i = 0; i < primary_key_ids_.count(); ++i) {
      seed = do_hash(primary_key_ids_.at(i), seed);
    }
    for (int64_t i = 0; i < part_key_ids_.count(); ++i) {
      seed = do_hash(part_key_ids_.at(i), seed);
    }
    return seed;
    return seed;
  }

private:
  int init_column_convert_expr(const ObAssignments& assignments);

public:
  //
  // table_id_: the table_id_ of table TableItem.
  // loc_table_id_: the table_id_ used for table location lookup.
  // index_tid_: table_id in schema
  //
  // e.g.:
  //   create view V as select * from T1 as T;
  //   update V set ...;
  //
  //   table_id_: table_id_ of V table item
  //   loc_table_id_: table_id_ of T table item
  //   index_tid_: ref_id_ of T table item
  //
  // table_id_ may be updated in view merge transform.
  //
  uint64_t table_id_;
  uint64_t loc_table_id_;  // location table id
  uint64_t index_tid_;
  common::ObString index_name_;
  int64_t rowkey_cnt_;
  int64_t part_cnt_;
  int64_t all_part_num_;
  common::ObSEArray<ObColumnRefRawExpr*, 8, common::ModulePageAllocator, true> column_exprs_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> column_convert_exprs_;
  ObAssignments assignments_;
  bool need_filter_null_;
  DistinctType distinct_algo_;
  common::ObSEArray<ObRawExpr*, 8, common::ModulePageAllocator, true> calc_part_id_exprs_;

  common::ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> primary_key_ids_;
  common::ObSEArray<uint64_t, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> part_key_ids_;

  TO_STRING_KV(K_(table_id), K_(index_tid), K_(loc_table_id), K_(index_name), K_(rowkey_cnt), K_(primary_key_ids),
      K_(part_cnt), K_(part_key_ids), K_(all_part_num), K_(column_exprs), K_(column_convert_exprs), K_(assignments),
      K_(need_filter_null), K_(distinct_algo), K_(calc_part_id_exprs));
};

/**
 *  This used in delete/update where the modification could take place on multiple tables
 *  and for each one we need to provide a series of columns as required in the access layer
 */
class TableColumns {
public:
  TableColumns() : table_name_()
  {}
  virtual ~TableColumns()
  {}

  int deep_copy(ObRawExprFactory& expr_factory, const TableColumns& other);

  TO_STRING_KV(K_(table_name), K_(index_dml_infos));

  common::ObString table_name_;
  common::ObSEArray<IndexDMLInfo, 4, common::ModulePageAllocator, true> index_dml_infos_;
  int64_t to_explain_string(char* buf, int64_t buf_len, ExplainType type) const;
  uint64_t hash(uint64_t seed) const;
};

class ObDelUpdStmt : public ObDMLStmt {
public:
  explicit ObDelUpdStmt(stmt::StmtType type)
      : ObDMLStmt(type),
        all_table_columns_(),
        returning_exprs_(),
        returning_strs_(),
        returning_agg_items_(),
        ignore_(false),
        is_returning_(false),
        has_global_index_(false),
        dml_source_from_join_(false)
  {}
  virtual ~ObDelUpdStmt()
  {}
  int deep_copy_stmt_struct(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& other) override;
  int assign(const ObDelUpdStmt& other);
  inline common::ObIArray<TableColumns>& get_all_table_columns()
  {
    return const_cast<common::ObIArray<TableColumns>&>(static_cast<const ObDelUpdStmt*>(this)->get_all_table_columns());
  }
  const common::ObIArray<TableColumns>* get_slice_from_all_table_columns(
      common::ObIAllocator& allocator, int64_t table_idx, int64_t index_idx) const;
  inline const common::ObIArray<TableColumns>& get_all_table_columns() const
  {
    return all_table_columns_;
  }
  virtual void set_ignore(bool ignore)
  {
    ignore_ = ignore;
  }
  virtual bool is_ignore() const
  {
    return ignore_;
  }

  IndexDMLInfo* get_or_add_table_columns(uint64_t table_id);
  IndexDMLInfo* get_table_dml_info(uint64_t table_id);
  int check_dml_need_filter_null();
  void set_returning(bool is_returning)
  {
    is_returning_ = is_returning;
  }
  bool is_returning() const override
  {
    return is_returning_;
  }
  int add_value_to_returning_exprs(ObRawExpr* expr)
  {
    return returning_exprs_.push_back(expr);
  }
  const common::ObIArray<ObRawExpr*>& get_returning_exprs() const
  {
    return returning_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_returning_exprs()
  {
    return returning_exprs_;
  }
  common::ObIArray<ObRawExpr*>& get_returning_into_exprs()
  {
    return returning_into_exprs_;
  }
  const common::ObIArray<ObRawExpr*>& get_returning_into_exprs() const
  {
    return returning_into_exprs_;
  }
  int add_value_to_returning_strs(ObString str)
  {
    return returning_strs_.push_back(str);
  }
  const common::ObIArray<ObString>& get_returning_strs() const
  {
    return returning_strs_;
  }
  int add_returning_agg_item(ObAggFunRawExpr& agg_expr)
  {
    agg_expr.set_explicited_reference();
    agg_expr.set_expr_level(current_level_);
    return returning_agg_items_.push_back(&agg_expr);
  }
  int64_t get_returning_aggr_item_size() const
  {
    return returning_agg_items_.size();
  }
  const common::ObIArray<ObAggFunRawExpr*>& get_returning_aggr_items() const
  {
    return returning_agg_items_;
  }
  common::ObIArray<ObAggFunRawExpr*>& get_returning_aggr_items()
  {
    return returning_agg_items_;
  }
  int add_multi_table_dml_info(const IndexDMLInfo& index_dml_info);
  int find_index_column(uint64_t table_id, uint64_t index_id, uint64_t column_id, ObColumnRefRawExpr*& col_expr);
  bool has_global_index() const
  {
    return has_global_index_;
  }
  void set_has_global_index(bool has_global_index)
  {
    has_global_index_ |= has_global_index;
  }
  virtual bool check_table_be_modified(uint64_t ref_table_id) const;
  void set_dml_source_from_join(bool from_join)
  {
    dml_source_from_join_ = from_join;
  }
  inline bool dml_source_from_join() const
  {
    return dml_source_from_join_;
  }

  virtual int update_base_tid_cid();
  virtual int inner_get_share_exprs(ObIArray<ObRawExpr*>& candi_share_exprs) const;
  virtual int replace_inner_stmt_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs) override;
  uint64_t get_insert_table_id(uint64_t table_offset = 0) const;
  uint64_t get_insert_base_tid(uint64_t table_offset = 0) const;
  uint64_t get_ref_table_id() const;

protected:
  int inner_get_relation_exprs(RelExprCheckerBase& expr_checker);
  virtual int inner_get_relation_exprs_for_wrapper(RelExprChecker& expr_checker)
  {
    return ObDelUpdStmt::inner_get_relation_exprs(expr_checker);
  }
  int recursively_check_filter_null(const JoinedTable* cur_table);
  int replace_table_assign_exprs(const common::ObIArray<ObRawExpr*>& other_exprs,
      const common::ObIArray<ObRawExpr*>& new_exprs, ObAssignments& assignments);

protected:
  common::ObArray<TableColumns, common::ModulePageAllocator, true> all_table_columns_;
  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> returning_exprs_;
  common::ObSEArray<ObRawExpr*, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> returning_into_exprs_;
  common::ObSEArray<ObString, common::OB_PREALLOCATED_NUM, common::ModulePageAllocator, true> returning_strs_;
  common::ObArray<ObAggFunRawExpr*, common::ModulePageAllocator, true> returning_agg_items_;
  bool ignore_;
  bool is_returning_;
  bool has_global_index_;
  bool dml_source_from_join_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_DEL_UPD_STMT_H_
