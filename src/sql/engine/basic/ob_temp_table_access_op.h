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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_OP_H_

#include "sql/engine/ob_operator.h"
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
class ObTempTableAccessOp;
class ObTempTableAccessOpInput : public ObOpInput {
  OB_UNIS_VERSION_V(1);
  friend class ObTempTableAccessOp;

public:
  ObTempTableAccessOpInput(ObExecContext& ctx, const ObOpSpec& spec);
  virtual ~ObTempTableAccessOpInput();
  virtual void reset() override;
  virtual ObPhyOperatorType get_phy_op_type() const;
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator);
  virtual int init(ObTaskInfo& task_info);
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  int check_finish(bool& is_end, int64_t& interm_res_ids);
  int check_closed_finish(bool& is_end);

protected:
  common::ObIAllocator* deserialize_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObTempTableAccessOpInput);

public:
  uint64_t closed_count_;
  uint64_t unfinished_count_ptr_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
};

class ObTempTableAccessOpSpec : public ObOpSpec {
public:
  OB_UNIS_VERSION_V(1);

public:
  ObTempTableAccessOpSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type),
        output_indexs_(alloc),
        temp_table_id_(0),
        is_distributed_(false),
        need_release_(false),
        access_exprs_(alloc)
  {}
  virtual ~ObTempTableAccessOpSpec()
  {}
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
    return output_indexs_.init(count);
  }
  int add_access_expr(ObExpr* access_expr)
  {
    return access_exprs_.push_back(access_expr);
  }
  int init_access_exprs(int64_t count)
  {
    return access_exprs_.init(count);
  }

  DECLARE_VIRTUAL_TO_STRING;

public:
  common::ObFixedArray<int64_t, common::ObIAllocator> output_indexs_;
  uint64_t temp_table_id_;
  bool is_distributed_;
  bool need_release_;
  // Operator access exprs expressions
  ExprFixedArray access_exprs_;
};

class ObTempTableAccessOp : public ObOperator {
public:
  ObTempTableAccessOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObOperator(exec_ctx, spec, input), datum_store_(NULL), datum_store_it_(), is_started_(false)
  {}
  ~ObTempTableAccessOp()
  {}

  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_close() override;
  virtual void destroy() override;
  int prepare_scan_param();
  int locate_next_interm_result(bool& is_end);
  int locate_interm_result(dtl::ObDTLIntermResultKey& dtl_int_key);
  int destory_interm_results();

private:
  ObChunkDatumStore* datum_store_;
  ObChunkDatumStore::Iterator datum_store_it_;
  bool is_started_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_ACCESS_OP_H_ */
