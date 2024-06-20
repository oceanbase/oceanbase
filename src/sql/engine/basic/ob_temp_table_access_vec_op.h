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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_VEC_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/basic/ob_temp_column_store.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObTempTableAccessVecOp;
class ObTempTableAccessVecOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
  friend class ObTempTableAccessVecOp;
public:
  ObTempTableAccessVecOpInput(ObExecContext &ctx, const ObOpSpec &spec);
  virtual ~ObTempTableAccessVecOpInput();
  virtual void reset() override;
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void set_deserialize_allocator(common::ObIAllocator *allocator);
  virtual int init(ObTaskInfo &task_info);
  int check_finish(bool &is_end, int64_t &interm_res_ids);
protected:
  common::ObIAllocator *deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTempTableAccessVecOpInput);
public:
  uint64_t unfinished_count_ptr_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
};

class ObTempTableAccessVecOpSpec : public ObOpSpec
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObTempTableAccessVecOpSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      output_indexs_(alloc),
      temp_table_id_(0),
      is_distributed_(false),
      access_exprs_(alloc) {}
  virtual ~ObTempTableAccessVecOpSpec() {}
  void set_temp_table_id(uint64_t temp_table_id) { temp_table_id_ = temp_table_id; }
  uint64_t get_table_id() const { return temp_table_id_; }
  void set_distributed(bool is_distributed) { is_distributed_ = is_distributed; }
  bool is_distributed() const { return is_distributed_; }
  int add_output_index(int64_t index) { return output_indexs_.push_back(index); }
  int init_output_index(int64_t count) { return output_indexs_.init(count); }
  int add_access_expr(ObExpr *access_expr) { return access_exprs_.push_back(access_expr); }
  int init_access_exprs(int64_t count) { return access_exprs_.init(count); }

  DECLARE_VIRTUAL_TO_STRING;
public:
  common::ObFixedArray<int64_t, common::ObIAllocator> output_indexs_;
  uint64_t temp_table_id_;
  bool is_distributed_;
  // Operator access exprs expressions
  ExprFixedArray access_exprs_;
};

class ObTempTableAccessVecOp : public ObOperator
{
public:
  ObTempTableAccessVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      col_store_it_(),
      interm_result_ids_(),
      cur_idx_(0),
      can_rescan_(false),
      is_started_(false),
      result_info_guard_(),
      output_exprs_(exec_ctx.get_allocator()) {}
  ~ObTempTableAccessVecOp() { destroy(); }

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
  int locate_next_interm_result(bool &is_end);
  int locate_interm_result(int64_t result_id);
  int get_local_interm_result_id(int64_t &result_id);

private:
  ObTempColumnStore::Iterator col_store_it_;
  //这里的result id是当前算子可用的任务（对于rescan而言）或者是已经完成或正在完成的任务
  //TempTableAccess的rescan不会重新从任务池中抢占任务，而是选择重新执行之前抢占到的任务
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
  uint64_t cur_idx_;
  bool can_rescan_;
  //如果是local result，只能读一次
  bool is_started_;
  dtl::ObDTLIntermResultInfoGuard result_info_guard_;
  ExprFixedArray output_exprs_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_VEC_OP_H_ */
