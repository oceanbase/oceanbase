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

#ifndef _OB_TABLE_MERGE_H
#define _OB_TABLE_MERGE_H
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "storage/ob_dml_param.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObDMLBaseParam;
}  // namespace storage
namespace sql {
////////////////////////////////////// ObTableMergeInput //////////////////////////////////////
class ObTableMergeInput : public ObTableModifyInput {
  friend class ObTableMerge;

public:
  ObTableMergeInput() : ObTableModifyInput()
  {}
  virtual ~ObTableMergeInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_MERGE;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableMergeInput);
};

////////////////////////////////////// ObTableMerge //////////////////////////////////////
class ObTableMerge : public ObTableModify {
  OB_UNIS_VERSION(1);

public:
  class ObTableMergeCtx : public ObTableModifyCtx {
  public:
    explicit ObTableMergeCtx(ObExecContext& ctx);
    ~ObTableMergeCtx();
    int init(ObExecContext& ctx, bool has_insert_clause, bool has_update_clause, int64_t insert_row_column_count,
        int64_t target_rowkey_column_count, int64_t update_row_columns_count);
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      insert_row_store_.reset();
      part_infos_.reset();
      dml_param_.~ObDMLBaseParam();
      affected_rows_ = 0;
    }

    void reset()
    {
      part_infos_.reset();
      affected_rows_ = 0;
    }

    common::ObNewRow& get_update_row()
    {
      return update_row_;
    }

  public:
    // fileds for update clause
    common::ObNewRow update_row_;     // row buffer for update clause, hold both old row and new row
    common::ObNewRow target_rowkey_;  // rowkey for the updated row
    common::ObNewRow old_row_;        // row before update, without projector
    common::ObNewRow new_row_;        // row after update, without projector

    // fileds for insert clause
    common::ObNewRow insert_row_;  // inserted row, without projector
    // for merge into, insert clause is expected to be executed after the update clause
    // i.e. the insert clause insert row r1, and the update clause delete r1
    //      should be executed successfully
    // but, 'insert r1 and then delete r1' may result in duplicate key error
    // the insert_row_store_ is used to hold duplicate rows,
    // and will re-insert rows into memtable after the update clasue is totally finished
    common::ObRowStore insert_row_store_;

    // result fileds
    int64_t affected_rows_;
    storage::ObDMLBaseParam dml_param_;
    common::ObSEArray<DMLPartInfo, 1> part_infos_;
  };

public:
  explicit ObTableMerge(common::ObIAllocator& alloc);
  virtual ~ObTableMerge()
  {}

  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int calc_condition(common::ObExprCtx& expr_ctx, const common::ObNewRow& row,
      const common::ObDList<ObSqlExpression>& cond_exprs, bool& is_true_cond) const;
  int calc_condition(common::ObExprCtx& expr_ctx, const common::ObNewRow& left_row, const common::ObNewRow& right_row,
      const common::ObDList<ObSqlExpression>& cond_exprs, bool& is_true_cond) const;
  int calc_delete_condition(ObExecContext& ctx, const common::ObNewRow& input_row, bool& is_true_cond) const;

  int check_is_match(ObExprCtx& expr_ctx, const ObNewRow& input_row, bool& is_match) const;
  int process_insert(storage::ObPartitionService* partition_service, const transaction::ObTransDesc& trans_desc,
      const storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey, ObExecContext& ctx,
      common::ObExprCtx& expr_ctx, const common::ObNewRow* input_row_desc) const;
  int calc_insert_row(ObExecContext& ctx, common::ObExprCtx& expr_ctx, const common::ObNewRow*& insert_row) const;
  int insert_all_rows(ObExecContext& ctx, storage::ObPartitionService* partition_service,
      const transaction::ObTransDesc& trans_desc, const storage::ObDMLBaseParam& dml_param,
      const common::ObPartitionKey& pkey) const;

  int add_match_condition(ObSqlExpression* expr);
  int add_delete_condition(ObSqlExpression* expr);
  int add_update_condition(ObSqlExpression* expr);
  int add_insert_condition(ObSqlExpression* expr);
  int set_rowkey_desc(const common::ObIArray<int64_t>& rowkey_desc);
  int init_scan_column_id_array(int64_t count)
  {
    return init_array_size<>(scan_column_ids_, count);
  }

  inline int add_scan_column_id(const uint64_t column_id)
  {
    return add_id_to_array(scan_column_ids_, column_id);
  }
  inline const common::ObIArray<uint64_t>* get_scan_column_ids() const
  {
    return &scan_column_ids_;
  };

  inline int add_updated_column_ids(uint64_t column_id)
  {
    return add_id_to_array(updated_column_ids_, column_id);
  }
  inline const common::ObIArray<uint64_t>* get_updated_column_ids() const
  {
    return &updated_column_ids_;
  }

  inline int add_delete_column_id(uint64_t column_id)
  {
    return add_id_to_array(delete_column_ids_, column_id);
  }
  inline const common::ObIArray<uint64_t>* get_delete_column_ids() const
  {
    return &delete_column_ids_;
  }
  inline int init_delete_columns_array(int64_t count)
  {
    return init_array_size<>(delete_column_ids_, count);
  }

  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count)
  {
    UNUSED(allocator);
    int ret = common::OB_SUCCESS;
    if (OB_FAIL(updated_column_infos_.prepare_allocate(count))) {
      SQL_ENG_LOG(WARN, "prepare allocate update column infos failed", K(ret), K(count));
    } else if (OB_FAIL(updated_column_ids_.prepare_allocate(count))) {
      SQL_ENG_LOG(WARN, "prepare allocate updated column ids failed", K(ret), K(count));
    }
    return ret;
  }
  int init_assignment_expr_count(int64_t count)
  {
    return init_array_size<>(assignment_infos_, count);
  }
  int add_assignment_expr(ObColumnExpression* expr);

  int set_old_projector(int32_t* projector, int64_t projector_size);
  inline const int32_t* get_old_projector() const
  {
    return old_projector_;
  }
  inline int64_t get_old_projector_size() const
  {
    return old_projector_size_;
  }
  void set_updated_projector(int32_t* projector, int64_t projector_size)
  {
    new_projector_ = projector;
    new_projector_size_ = projector_size;
  }
  void set_delete_projector(int32_t* projector, int64_t projector_size)
  {
    delete_projector_ = projector;
    delete_projector_size_ = projector_size;
  }
  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp);
  void set_has_insert_clause(bool has)
  {
    has_insert_clause_ = has;
  }
  void set_has_update_clause(bool has)
  {
    has_update_clause_ = has;
  }
  inline void set_column_projector(int32_t* projector, int64_t projector_size)
  {
    update_origin_projector_ = projector;
    update_origin_projector_size_ = projector_size;
  }

  int copy_row(const ObNewRow& old_row, ObNewRow& new_row, int32_t* projector = NULL, int64_t projector_size = 0) const;
  virtual int rescan(ObExecContext& ctx) const;

protected:
  int generate_origin_row(ObExecContext& ctx, const common::ObNewRow* input_row, bool& conflict) const;
  int calc_update_row(ObExecContext& ctx, const common::ObNewRow* input_row) const;

private:
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int do_table_merge(ObExecContext& ctx) const;
  int process_update(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey,
      ObExecContext& ctx, common::ObExprCtx& expr_ctx, storage::ObDMLBaseParam& dml_param,
      const common::ObNewRow* input_row) const;
  int update_row(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey, bool need_delete,
      ObExecContext& ctx, storage::ObDMLBaseParam& dml_param) const;
  int lock_row(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey, ObExecContext& ctx,
      storage::ObDMLBaseParam& dml_param) const;
  int check_updated_row(const common::ObNewRow& row1, const common::ObNewRow& row2, bool& is_changed) const;
  int deserialize_projector(
      const char* buf, const int64_t data_len, int64_t& pos, int32_t*& projector, int64_t& projector_size);

private:
  template <class T>
  inline int add_id_to_array(T& array, uint64_t id);
  DISALLOW_COPY_AND_ASSIGN(ObTableMerge);

protected:
  bool has_insert_clause_;
  bool has_update_clause_;

  common::ObDList<ObSqlExpression> match_conds_;  // not necessary
  common::ObDList<ObSqlExpression> delete_conds_;
  common::ObDList<ObSqlExpression> update_conds_;
  common::ObDList<ObSqlExpression> insert_conds_;
  common::ObFixedArray<int64_t, common::ObIAllocator> rowkey_desc_;

  common::ObFixedArray<uint64_t, common::ObIAllocator> scan_column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> update_related_column_ids_;  // unused
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> delete_column_ids_;  // unused

  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  common::ObFixedArray<ObColumnExpression*, common::ObIAllocator> assignment_infos_;

  int32_t* old_projector_;      // nonsense
  int64_t old_projector_size_;  // nonsense
  int32_t* new_projector_;
  int64_t new_projector_size_;
  int32_t* delete_projector_;      // unused
  int64_t delete_projector_size_;  // unused
  int32_t* update_origin_projector_;
  int64_t update_origin_projector_size_;
};

template <class T>
int ObTableMerge::add_id_to_array(T& array, uint64_t id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!common::is_valid_id(id))) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(id));
  } else if (OB_FAIL(array.push_back(id))) {
    SQL_ENG_LOG(WARN, "fail to push back column id", K(ret));
  }
  return ret;
}
/////////////////////////////////// functor //////////////////////////////////////
class ExprFunction {
public:
  ExprFunction(ObTableMerge& table_merge) : table_merge_(table_merge)
  {}
  ~ExprFunction()
  {}
  virtual int operator()(ObSqlExpression* expr) = 0;
  ObTableMerge& get_phy_op()
  {
    return table_merge_;
  }

protected:
  ObTableMerge& table_merge_;
};

class MatchCondition : public ExprFunction {
public:
  MatchCondition(ObTableMerge& table_merge) : ExprFunction(table_merge)
  {}
  ~MatchCondition()
  {}
  virtual int operator()(ObSqlExpression* expr)
  {
    return table_merge_.add_match_condition(expr);
  }
};

class DeleteCondition : public ExprFunction {
public:
  DeleteCondition(ObTableMerge& table_merge) : ExprFunction(table_merge)
  {}
  ~DeleteCondition()
  {}
  int operator()(ObSqlExpression* expr)
  {
    return table_merge_.add_delete_condition(expr);
  }
};

class UpdateCondition : public ExprFunction {
public:
  UpdateCondition(ObTableMerge& table_merge) : ExprFunction(table_merge)
  {}
  ~UpdateCondition()
  {}
  int operator()(ObSqlExpression* expr)
  {
    return table_merge_.add_update_condition(expr);
  }
};

class InsertCondition : public ExprFunction {
public:
  InsertCondition(ObTableMerge& table_merge) : ExprFunction(table_merge)
  {}
  ~InsertCondition()
  {}
  int operator()(ObSqlExpression* expr)
  {
    return table_merge_.add_insert_condition(expr);
  }
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_TABLE_MERGE_H */
