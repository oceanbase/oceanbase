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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_OP_

#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase {
namespace sql {
class ObTableInsertSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableInsertSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type) : ObTableModifySpec(alloc, type)
  {}

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertSpec);
};

class ObTableInsertOp : public ObTableModifyOp {
public:
  ObTableInsertOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObTableModifyOp(ctx, spec, input), curr_row_num_(0), part_row_cnt_(0), is_end_(false)
  {}
  virtual ~ObTableInsertOp(){};

  virtual int switch_iterator(ObExecContext& ctx);
  virtual void destroy() override
  {
    dml_param_.~ObDMLBaseParam();
    part_infos_.reset();
    ObTableModifyOp::destroy();
  }

protected:
  virtual int inner_open() override;
  virtual int inner_get_next_row() override;
  virtual int rescan() override;
  virtual int inner_close() override;
  virtual int prepare_next_storage_row(const ObExprPtrIArray*& output) override;
  int process_row();
  OB_INLINE int insert_rows(
      storage::ObDMLBaseParam& dml_param, const common::ObIArray<DMLPartInfo>& part_infos, int64_t& affected_rows);
  int do_table_insert();

public:
  int64_t curr_row_num_;
  int64_t part_row_cnt_;
  storage::ObDMLBaseParam dml_param_;
  common::ObSEArray<DMLPartInfo, 4> part_infos_;
  // insert op will call inner_get_next_row() in inner_open(), and swallow error codes OB_ITER_END
  // we set this flag  = true to avoid call child_->get_next_row() after OB_ITER_END
  bool is_end_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertOp);
};

class ObTableInsertOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableInsertOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertOpInput);
};

class ObSeInsertRowIterator : public ObTableModifyOp::DMLRowIterator {
public:
  ObSeInsertRowIterator(ObExecContext& ctx, ObTableInsertOp& op)
      : DMLRowIterator(ctx, op),
        batch_rows_(NULL),
        first_bulk_(true),
        estimate_rows_(0),
        row_count_(0),
        index_(0),
        row_copy_mem_(NULL)
  {}
  ~ObSeInsertRowIterator()
  {
    destroy_row_copy_mem();
  }

  int get_next_rows(common::ObNewRow*& row, int64_t& row_count) override;
  virtual void reset() override;

  // create or reset %row_copy_mem_
  int setup_row_copy_mem();
  void destroy_row_copy_mem()
  {
    if (NULL != row_copy_mem_) {
      DESTROY_CONTEXT(row_copy_mem_);
      row_copy_mem_ = NULL;
    }
  }

  // for batch insert
private:
  static const int64_t BULK_COUNT = 500;  // get 500 rows when get_next_rows is called
  int create_cur_rows(int64_t total_cnt, int64_t col_cnt, common::ObNewRow*& row, int64_t& row_cnt);
  int alloc_rows_cells(const int64_t col_cnt, const int64_t row_cnt, common::ObNewRow* rows);
  int copy_cur_rows(const ObNewRow& src_row);

private:
  ObNewRow* batch_rows_;
  bool first_bulk_;
  int64_t estimate_rows_;
  int64_t row_count_;
  int64_t index_;
  lib::MemoryContext* row_copy_mem_;
};

}  // end namespace sql
}  // end namespace oceanbase
#endif
