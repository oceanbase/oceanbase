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

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObTempTableAccessInput : public ObIPhyOperatorInput {
  friend class ObTempTableAccess;
  OB_UNIS_VERSION_V(1);

public:
  ObTempTableAccessInput();
  virtual ~ObTempTableAccessInput();
  virtual void reset() override;
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator);
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  int check_finish(bool& is_end, int64_t& index);
  int check_closed_finish(bool& is_end);
  int assign_ids(common::ObIArray<uint64_t>& interm_res_ids);

protected:
  common::ObIAllocator* deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTempTableAccessInput);

public:
  uint64_t closed_count_;
  uint64_t unfinished_count_ptr_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
};

class ObTempTableAccess : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObTempTableAccessCtx : public ObPhyOperatorCtx {
    friend class ObTempTableAccess;

  public:
    explicit ObTempTableAccessCtx(ObExecContext& ctx)
        : ObPhyOperatorCtx(ctx), row_store_(NULL), tta_input_(NULL), is_started_(false)
    {}
    virtual ~ObTempTableAccessCtx()
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
    int locate_interm_result(dtl::ObDTLIntermResultKey& dtl_int_key);

  private:
    ObChunkRowStore* row_store_;
    ObChunkRowStore::Iterator row_store_it_;
    ObTempTableAccessInput* tta_input_;
    bool is_started_;
  };

public:
  ObTempTableAccess(common::ObIAllocator& alloc)
      : ObNoChildrenPhyOperator(alloc),
        output_indexs_(alloc),
        temp_table_id_(0),
        is_distributed_(false),
        need_release_(false)
  {}
  virtual ~ObTempTableAccess()
  {}
  void reset();
  void reuse();
  int rescan(ObExecContext& exec_ctx) const;

  DECLARE_VIRTUAL_TO_STRING;
  virtual int inner_open(ObExecContext& exec_ctx) const;
  virtual int inner_close(ObExecContext& exec_ctx) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int create_operator_input(ObExecContext& ctx) const;
  int locate_next_interm_result(ObExecContext& ctx, bool& is_end) const;
  int destory_interm_results(ObExecContext& exec_ctx) const;
  int construct_row_from_interm_row(ObExecContext& ctx, ObNewRow* input_row, const ObNewRow*& row) const;
  void set_temp_table_id(uint64_t temp_table_id)
  {
    temp_table_id_ = temp_table_id;
  }
  uint64_t get_table_id() const
  {
    return temp_table_id_;
  }
  void set_distributed(bool is_distributed)
  {
    is_distributed_ = is_distributed;
  }
  bool is_distributed() const
  {
    return is_distributed_;
  }
  void set_need_release(bool need_release)
  {
    need_release_ = need_release;
  }
  bool is_need_release() const
  {
    return need_release_;
  }
  int add_output_index(int64_t index)
  {
    return output_indexs_.push_back(index);
  }
  int init_output_index(int64_t count)
  {
    return init_array_size<>(output_indexs_, count);
  }

  DISALLOW_COPY_AND_ASSIGN(ObTempTableAccess);

private:
  common::ObFixedArray<int64_t, common::ObIAllocator> output_indexs_;
  uint64_t temp_table_id_;
  bool is_distributed_;
  bool need_release_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_H_ */
