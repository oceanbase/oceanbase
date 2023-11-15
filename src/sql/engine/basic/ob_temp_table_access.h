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

// Copyright 2014 Alibaba Inc. All Rights Reserved.
// Author:
//     ruoshi <>

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_H_

#include "sql/engine/ob_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObTempTableAccessInput : public ObIPhyOperatorInput
{
  friend class ObTempTableAccess;
  OB_UNIS_VERSION_V(1);
public:
  ObTempTableAccessInput();
  virtual ~ObTempTableAccessInput();
  virtual void reset() override;
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void set_deserialize_allocator(common::ObIAllocator *allocator);
  virtual int init(ObExecContext &ctx, ObTaskInfo &task_info, const ObPhyOperator &op);
  int check_finish(bool &is_end, int64_t &index);
  int assign_ids(common::ObIArray<uint64_t> &interm_res_ids);
protected:
  common::ObIAllocator *deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTempTableAccessInput);
public:
  uint64_t unfinished_count_ptr_;
  //这里的result id是相当于任务池，需要抢占式拿任务
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
};

class ObTempTableAccess : public ObNoChildrenPhyOperator
{
  OB_UNIS_VERSION_V(1);
protected:
  class ObTempTableAccessCtx: public ObPhyOperatorCtx
  {
    friend class ObTempTableAccess;
    static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL =  1 << 13;
  public:
    explicit ObTempTableAccessCtx(ObExecContext &ctx)
        : ObPhyOperatorCtx(ctx),
          tta_input_(NULL),
          interm_result_ids_(),
          cur_idx_(0),
          can_rescan_(false),
          is_started_(false),
          is_distributed_(false),
          temp_table_id_(0),
          iterated_rows_(0),
          result_info_guard_() {}
    virtual ~ObTempTableAccessCtx() {}
    virtual void destroy() { ObPhyOperatorCtx::destroy_base(); row_store_it_.reset(); result_info_guard_.reset(); interm_result_ids_.reset();}
    int get_next_row(ObExecContext &exec_ctx, common::ObNewRow *&row);
    int rescan(const ObTempTableAccess *access_op, ObExecContext &ctx);
  private:
    int locate_next_interm_result(ObExecContext &ctx, bool &is_end);

    int get_local_interm_result_id(ObExecContext &ctx, int64_t &result_id);

    int locate_interm_result(int64_t result_id);

  private:
    ObChunkRowStore::Iterator row_store_it_;
    ObTempTableAccessInput *tta_input_;
    //这里的result id是当前算子可用的任务（对于rescan而言）或者是已经完成或正在完成的任务
    //TempTableAccess的rescan不会重新从任务池中抢占任务，而是选择重新执行之前抢占到的任务
    common::ObSEArray<uint64_t, 8> interm_result_ids_;
    uint64_t cur_idx_;
    bool can_rescan_;
    //如果是local result，只能读一次
    bool is_started_;
    bool is_distributed_;
    uint64_t temp_table_id_;
    int64_t iterated_rows_;
    dtl::ObDTLIntermResultInfoGuard result_info_guard_;
  };
public:
  ObTempTableAccess(common::ObIAllocator &alloc)
    : ObNoChildrenPhyOperator(alloc),
      output_indexs_(alloc),
      temp_table_id_(0),
      is_distributed_(false){}
  virtual ~ObTempTableAccess() {}
  void reset();
  void reuse();
  int rescan(ObExecContext &exec_ctx) const;

  DECLARE_VIRTUAL_TO_STRING;
  virtual int inner_open(ObExecContext &exec_ctx) const;
  virtual int inner_close(ObExecContext &exec_ctx) const;
  virtual int init_op_ctx(ObExecContext &ctx) const;
  virtual int inner_get_next_row(ObExecContext &exec_ctx, const common::ObNewRow *&row) const;
  int create_operator_input(ObExecContext &ctx) const;
  void set_temp_table_id(uint64_t temp_table_id) { temp_table_id_ = temp_table_id; }
  uint64_t get_table_id() const { return temp_table_id_; }
  void set_distributed(bool is_distributed) { is_distributed_ = is_distributed; }
  bool is_distributed() const { return is_distributed_; }
  int add_output_index(int64_t index) { return output_indexs_.push_back(index); }
  int init_output_index(int64_t count) { return init_array_size<>(output_indexs_, count); }

  DISALLOW_COPY_AND_ASSIGN(ObTempTableAccess);
private:
  common::ObFixedArray<int64_t, common::ObIAllocator> output_indexs_;
  uint64_t temp_table_id_;
  bool is_distributed_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_H_ */
