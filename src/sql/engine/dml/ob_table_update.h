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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_UPDATE_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_UPDATE_H_
#include "sql/engine/dml/ob_table_modify.h"
#include "storage/ob_dml_param.h"
namespace oceanbase {
namespace sql {
class ObTableUpdateInput : public ObTableModifyInput {
  friend class ObTableUpdate;

public:
  ObTableUpdateInput() : ObTableModifyInput()
  {}
  virtual ~ObTableUpdateInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_UPDATE;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableUpdateInput);
};

class ObTableUpdate : public ObTableModify {
public:
  class ObTableUpdateCtx : public ObTableModifyCtx {
    friend class ObTableUpdate;

  public:
    explicit ObTableUpdateCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx),
          full_row_(),
          new_row_(),
          old_row_(),
          has_got_old_row_(false),
          found_rows_(0),
          changed_rows_(0),
          affected_rows_(0),
          dml_param_(),
          part_key_(),
          part_row_cnt_(0),
          part_infos_(),
          cur_part_idx_(0)
    {}
    ~ObTableUpdateCtx()
    {}
    void inc_affected_rows()
    {
      affected_rows_++;
    }
    void inc_found_rows()
    {
      found_rows_++;
    }
    void inc_changed_rows()
    {
      changed_rows_++;
    }
    int64_t get_affected_rows()
    {
      return affected_rows_;
    }
    int64_t get_found_rows()
    {
      return found_rows_;
    }
    int64_t get_changed_rows()
    {
      return changed_rows_;
    }

    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      dml_param_.~ObDMLBaseParam();
      part_infos_.reset();
    }
    friend class ObTableUpdate;

  protected:
    common::ObNewRow full_row_;
    common::ObNewRow new_row_;
    common::ObNewRow old_row_;
    common::ObNewRow lock_row_;
    bool has_got_old_row_;
    int64_t found_rows_;
    int64_t changed_rows_;
    int64_t affected_rows_;
    storage::ObDMLBaseParam dml_param_;
    common::ObPartitionKey part_key_;
    int64_t part_row_cnt_;
    common::ObSEArray<DMLPartInfo, 4> part_infos_;
    int64_t cur_part_idx_;
    int32_t* new_row_projector_;
    int64_t new_row_projector_size_;
  };

  OB_UNIS_VERSION(1);

public:
  explicit ObTableUpdate(common::ObIAllocator& alloc);
  virtual ~ObTableUpdate();

  void reset();
  void reuse();
  virtual int rescan(ObExecContext& ctx) const override;
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int switch_iterator(ObExecContext& ctx) const override;
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
  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t projector_index, bool auto_filled_timestamp);
  void set_updated_projector(int32_t* projector, int64_t projector_size)
  {
    set_projector(projector, projector_size);
  }
  inline void set_is_global_index(bool is_global_index)
  {
    is_global_index_ = is_global_index;
  }
  void project_old_and_new_row(ObTableUpdateCtx& ctx, const common::ObNewRow& full_row) const;
  bool check_row_whether_changed(const common::ObNewRow& new_row) const;
  const common::ObIArrayWrap<ColumnContent>& get_assign_columns() const
  {
    return updated_column_infos_;
  }

  const common::ObIArray<uint64_t>* get_updated_column_ids() const
  {
    return &updated_column_ids_;
  }
  int add_new_spk_expr(ObColumnExpression* expr)
  {
    return ObSqlExpressionUtil::add_expr_to_list(new_spk_exprs_, expr);
  }

protected:
  /**
   * @brief called by my_get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  int init_op_ctx(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  inline int update_rows(ObExecContext& ctx, int64_t& affected_rows) const;
  int build_lock_row(ObTableUpdateCtx& update_ctx, const common::ObNewRow& old_row) const;
  int do_table_update(ObExecContext& ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableUpdate);

protected:
  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  bool is_global_index_;
  common::ObDList<ObSqlExpression> new_spk_exprs_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_UPDATE_H_ */
