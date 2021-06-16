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

#ifndef _OB_TABLE_INSERT_UP_H
#define _OB_TABLE_INSERT_UP_H
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify.h"
#include "storage/ob_dml_param.h"
namespace oceanbase {
namespace sql {
class ObTableInsertUpInput : public ObTableModifyInput {
  friend class ObTableInsertUp;

public:
  ObTableInsertUpInput() : ObTableModifyInput()
  {}
  virtual ~ObTableInsertUpInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_INSERT_ON_DUP;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertUpInput);
};

class ObTableInsertUp : public ObTableModify {
  OB_UNIS_VERSION(1);

public:
  class ObTableInsertUpCtx : public ObTableModifyCtx {
  public:
    explicit ObTableInsertUpCtx(ObExecContext& ctx);
    ~ObTableInsertUpCtx();
    common::ObNewRow& get_update_row();
    int init(ObExecContext& ctx, int64_t update_row_projector_size, int64_t primary_key_count,
        int64_t update_row_columns_count, int64_t insert_row_column_count);
    void reset_update_row();
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      dml_param_.~ObDMLBaseParam();
      part_infos_.reset();
    }
    void reset()
    {
      get_count_ = 0;
      found_rows_ = 0;
      cur_gi_task_iter_end_ = false;
      part_infos_.reset();
    }
    void inc_found_rows()
    {
      ++found_rows_;
    }

  public:
    common::ObNewRow rowkey_row_;
    int64_t found_rows_;
    int64_t get_count_;
    common::ObNewRow update_row_;
    int64_t update_row_projector_size_;
    int32_t* update_row_projector_;
    common::ObNewRow insert_row_;
    common::ObSEArray<DMLPartInfo, 1> part_infos_;
    storage::ObDMLBaseParam dml_param_;
    // GI
    //  INSERT UPDATE
    //  Under such a plan, GI calls get_next_row() need to return iter_end instead of row
    bool cur_gi_task_iter_end_;
  };

public:
  explicit ObTableInsertUp(common::ObIAllocator& alloc);
  virtual ~ObTableInsertUp();

  virtual void reset();
  virtual void reuse();
  virtual int64_t to_string_kv(char* buf, int64_t buf_len) const;
  virtual int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int add_filter(ObSqlExpression* expr);
  inline int add_update_related_column_id(uint64_t column_id);
  inline const common::ObIArray<uint64_t>* get_update_related_column_ids() const;
  inline int add_scan_column_id(const uint64_t column_id);
  inline const common::ObIArray<uint64_t>* get_scan_column_ids() const;
  inline const common::ObIArray<uint64_t>* get_updated_column_ids() const;
  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp);
  inline int set_column_projector_index(int64_t assign_index, int64_t projector_index);
  inline int set_old_projector(int32_t* projector, int64_t projector_size);
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
  int add_assignment_expr(ObColumnExpression* expr);
  int init_scan_column_id_array(int64_t count)
  {
    return init_array_size<>(scan_column_ids_, count);
  }
  int init_update_related_column_array(int64_t count)
  {
    return init_array_size<>(update_related_column_ids_, count);
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
  virtual int init_assignment_expr_count(int64_t count)
  {
    return init_array_size<>(assignment_infos_, count);
  }

protected:
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  virtual int rescan(ObExecContext& ctx) const;
  int do_table_insert_up(ObExecContext& ctx) const;
  int init_autoinc_param(ObExecContext& ctx, const common::ObPartitionKey& pkey) const;
  int calc_insert_row(ObExecContext& ctx, common::ObExprCtx& expr_ctx, const common::ObNewRow*& insert_row) const;
  int process_on_duplicate_update(ObExecContext& ctx, common::ObNewRowIterator* duplicated_rows,
      storage::ObDMLBaseParam& dml_param, int64_t& n_duplicated_rows, int64_t& affected_rows,
      int64_t& update_count) const;
  int build_scan_param(ObExecContext& ctx, const share::ObPartitionReplicaLocation& part_replica,
      const common::ObNewRow* dup_row, storage::ObTableScanParam& scan_param) const;

  int calc_update_rows(ObExecContext& ctx, const common::ObNewRow& insert_row, const common::ObNewRow& duplicate_row,
      bool& is_row_changed) const;

  int calc_new_row(ObExecContext& ctx, const common::ObNewRow& scan_row, const common::ObNewRow& insert_row,
      bool& is_row_changed) const;
  int update_auto_increment(const ObExecContext& ctx, const common::ObExprCtx& expr_ctx, const common::ObObj* val,
      int64_t assignemnt_index, bool& is_auto_col_changed) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;

  template <class T>
  inline int add_id_to_array(T& array, uint64_t id);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertUp);
  // function members
protected:
  int32_t* old_projector_;
  int64_t old_projector_size_;
  int32_t* new_projector_;
  int64_t new_projector_size_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> scan_column_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> update_related_column_ids_;
  // for assignment column
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  common::ObFixedArray<ObColumnExpression*, common::ObIAllocator> assignment_infos_;
};

template <class T>
int ObTableInsertUp::add_id_to_array(T& array, uint64_t id)
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

int ObTableInsertUp::add_update_related_column_id(uint64_t column_id)
{
  return add_id_to_array(update_related_column_ids_, column_id);
}

const common::ObIArray<uint64_t>* ObTableInsertUp::get_update_related_column_ids() const
{
  return &update_related_column_ids_;
}

int ObTableInsertUp::add_scan_column_id(const uint64_t column_id)
{
  return add_id_to_array(scan_column_ids_, column_id);
}

const common::ObIArray<uint64_t>* ObTableInsertUp::get_scan_column_ids() const
{
  return &scan_column_ids_;
}

const common::ObIArray<uint64_t>* ObTableInsertUp::get_updated_column_ids() const
{
  return &updated_column_ids_;
}

int ObTableInsertUp::set_column_projector_index(int64_t assign_index, int64_t projector_index)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!common::is_valid_idx(assign_index)) || OB_UNLIKELY(!common::is_valid_idx(projector_index))) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(assign_index), K(projector_index));
  } else if (OB_UNLIKELY(assign_index < 0) || OB_UNLIKELY(assign_index) >= updated_column_infos_.count()) {
    ret = common::OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "invalid array index", K(assign_index), K(updated_column_infos_.count()));
  } else {
    updated_column_infos_.at(assign_index).projector_index_ = projector_index;
  }
  return ret;
}

int ObTableInsertUp::set_old_projector(int32_t* projector, int64_t projector_size)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(projector) || OB_UNLIKELY(projector_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(projector), K(projector_size));
  } else {
    old_projector_ = projector;
    old_projector_size_ = projector_size;
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase

#endif /* _OB_TABLE_INSERT_UP_H */
