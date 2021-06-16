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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_INSERT_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_INSERT_H_

#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"

namespace oceanbase {
namespace sql {
class ObExecContext;
class ObTempTableInsertInput : public ObIPhyOperatorInput {
  friend class ObTempTableInsert;
  OB_UNIS_VERSION_V(1);

public:
  ObTempTableInsertInput();
  virtual ~ObTempTableInsertInput();
  virtual void reset() override;
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator);
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  int assign_ids(common::ObIArray<uint64_t>& interm_res_ids);

protected:
  common::ObIAllocator* deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTempTableInsertInput);

public:
  uint64_t unfinished_count_ptr_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
};

class ObTempTableInsert : public ObSingleChildPhyOperator {
  OB_UNIS_VERSION_V(1);

protected:
  class ObTempTableInsertCtx : public ObPhyOperatorCtx {
    friend class ObTempTableInsert;

  public:
    explicit ObTempTableInsertCtx(ObExecContext& ctx) : ObPhyOperatorCtx(ctx), interm_result_id_(0)
    {}
    virtual ~ObTempTableInsertCtx()
    {}
    virtual void destroy()
    {
      ObPhyOperatorCtx::destroy_base();
    }
    void set_iterm_result_id(uint64_t id)
    {
      interm_result_id_ = id;
    }

  public:
    uint64_t interm_result_id_;
  };

public:
  ObTempTableInsert(common::ObIAllocator& alloc)
      : ObSingleChildPhyOperator(alloc), temp_table_id_(0), is_distributed_(false)
  {}
  virtual ~ObTempTableInsert()
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
  int prepare_interm_result(ObExecContext& ctx) const;
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

  DISALLOW_COPY_AND_ASSIGN(ObTempTableInsert);

private:
  uint64_t temp_table_id_;
  bool is_distributed_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_INSERT_H_ */
