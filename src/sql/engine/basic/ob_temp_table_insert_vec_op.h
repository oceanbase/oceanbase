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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_INSERT_VEC_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_INSERT_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_ra_row_store.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "sql/dtl/ob_dtl_interm_result_manager.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_px_sqc_proxy.h"
#include "sql/engine/ob_operator_reg.h"

namespace oceanbase
{
namespace sql
{
class ObExecContext;
class ObTempTableInsertVecOp;
class ObPxTask;

class ObTempTableInsertVecOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTempTableInsertVecOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      qc_id_(common::OB_INVALID_ID),
      dfo_id_(common::OB_INVALID_ID),
      sqc_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObTempTableInsertVecOpInput() {}
  virtual void reset() override {}
  virtual int init(ObTaskInfo &task_info) override
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }
  int64_t qc_id_;
  int64_t dfo_id_;
  int64_t sqc_id_;
};

class ObTempTableInsertVecOpSpec : public ObOpSpec
{
public:
  OB_UNIS_VERSION_V(1);
public:
  ObTempTableInsertVecOpSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      temp_table_id_(0),
      is_distributed_(false) {}
  virtual ~ObTempTableInsertVecOpSpec() {}
  void set_temp_table_id(uint64_t temp_table_id) { temp_table_id_ = temp_table_id; }
  uint64_t get_table_id() const { return temp_table_id_; }
  void set_distributed(bool is_distributed) { is_distributed_ = is_distributed; }
  bool is_distributed() const { return is_distributed_; }

  DECLARE_VIRTUAL_TO_STRING;
public:
  uint64_t temp_table_id_;
  bool is_distributed_;
};

class ObTempTableInsertVecOp : public ObOperator
{
public:
  ObTempTableInsertVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      mem_context_(nullptr),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info_),
      all_interm_res_info_(),
      interm_result_ids_(),
      task_(NULL),
      init_temp_table_(true) {}
  ~ObTempTableInsertVecOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
  int do_get_next_vector(const int64_t max_row_cnt);
  int add_rows_to_temp_table(dtl::ObDTLIntermResultInfo *&interm_res_info,
                             const ObBatchRows *brs);
  inline int create_interm_result_info(dtl::ObDTLIntermResultInfo *&interm_res_info);
  inline int insert_interm_result_info();
  inline int clear_all_interm_res_info();
  inline int prepare_interm_result_id_for_local(uint64_t &interm_result_id);
  inline int prepare_interm_result_id_for_distribute(uint64_t &interm_result_id);
  ObIArray<uint64_t> &get_interm_result_ids() { return interm_result_ids_; }
  void set_px_task(ObPxTask *task) {task_ = task;}
private:
  int process_dump(dtl::ObDTLIntermResultInfo &interm_res_info);
  bool need_dump()
  { return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound(); }
  void destroy_mem_context()
  {
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }

private:
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  common::ObSEArray<dtl::ObDTLIntermResultInfo *, 8> all_interm_res_info_;
  common::ObSEArray<uint64_t, 8> interm_result_ids_;
  ObPxTask *task_;
  bool init_temp_table_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_TEMP_TABLE_INSERT_VEC_OP_H_ */
