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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERT_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERT_H_
#include "sql/engine/dml/ob_table_modify.h"
namespace oceanbase {
namespace share {
class ObPartitionReplicaLocation;
}
namespace sql {
class ObPhyTableLocation;

class ObTableInsertInput : public ObTableModifyInput {
  friend class ObTableUpdate;

public:
  ObTableInsertInput() : ObTableModifyInput()
  {}
  virtual ~ObTableInsertInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_INSERT;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertInput);
};

class ObTableInsert : public ObTableModify {
public:
  class ObTableInsertCtx : public ObTableModifyCtx {
    friend class ObTableInsert;

  public:
    explicit ObTableInsertCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx),
          curr_row_num_(0),
          estimate_rows_(0),
          first_bulk_(true),
          part_row_cnt_(0),
          dml_param_(),
          part_infos_(),
          new_row_exprs_(NULL),
          new_row_projector_(NULL),
          new_row_projector_size_(0)
    {}
    ~ObTableInsertCtx()
    {}
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      dml_param_.~ObDMLBaseParam();
      part_infos_.reset();
    }

  public:
    int64_t curr_row_num_;
    int64_t estimate_rows_;
    bool first_bulk_;
    int64_t part_row_cnt_;
    storage::ObDMLBaseParam dml_param_;
    common::ObSEArray<DMLPartInfo, 1> part_infos_;
    const ObDList<ObSqlExpression>* new_row_exprs_;
    int32_t* new_row_projector_;
    int64_t new_row_projector_size_;
  };

public:
  explicit ObTableInsert(common::ObIAllocator& alloc);
  ~ObTableInsert();

  int add_filter(ObSqlExpression* expr);
  virtual int switch_iterator(ObExecContext& ctx) const
  {
    UNUSED(ctx);
    return common::OB_ITER_END;
  }

protected:
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
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
   * @note not need to iterator a row to parent operator, so forbid this function
   */
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;

  int get_next_rows(ObExecContext& ctx, const common::ObNewRow*& row, int64_t& row_count) const;

  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  int do_column_convert(ObExprCtx& expr_ctx, const ObDList<ObSqlExpression>& new_row_exprs, ObNewRow& insert_row) const;
  int process_row(ObExecContext& ctx, ObTableInsertCtx* insert_ctx, const common::ObNewRow*& insert_row) const;
  int deep_copy_row(ObTableInsertCtx* insert_ctx, const common::ObNewRow*& insert_row) const;
  int is_valid(ObTableInsertCtx*& insert_ctx, ObExecContext& ctx) const;
  int insert_rows(ObExecContext& ctx, int64_t& affected_rows) const;
  int rescan(ObExecContext& ctx) const;
  int copy_insert_row(
      ObTableInsertCtx& insert_ctx, const ObNewRow*& input_row, ObNewRow& insert_row, bool need_copy) const;

private:
  int do_table_insert(ObExecContext& ctx) const;
  int copy_insert_rows(ObTableInsertCtx& insert_ctx, const ObNewRow*& input_row) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsert);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERT_H_ */
