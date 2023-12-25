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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_VEC_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase
{
namespace sql
{

class ObMaterialVecSpec: public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObMaterialVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type)
  {}
};

class ObMaterialVecOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
  friend ObMaterialVecOp;
public:
  ObMaterialVecOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec), bypass_(false) {};
  virtual ~ObMaterialVecOpInput() = default;
  virtual int init(ObTaskInfo &task_info) override { UNUSED(task_info); return common::OB_SUCCESS; }
  virtual void reset() override { bypass_ = false; }
  void set_bypass(bool bypass) { bypass_ = bypass; }
  int64_t is_bypass() const { return bypass_; }
protected:
  bool bypass_; // if true, do bypass
};

class ObMaterialVecOp : public ObOperator
{
public:
  ObMaterialVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    is_first_(false),
    store_it_(),
    mem_context_(nullptr),
    profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_)
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;

  int get_material_row_count(int64_t &count) const
  {
    count = store_.get_row_cnt();
    return common::OB_SUCCESS;
  }
  // reset material iterator, used for NLJ/NL connectby
  int rewind();
private:
  int get_all_batch_from_child();
  void reset();
  void destroy_mem_context()
  {
    if (nullptr != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
  }
  inline bool need_dump() const
  { return sql_mem_processor_.get_data_size() > sql_mem_processor_.get_mem_bound(); }
  int process_dump();

private:
  bool is_first_;
  ObTempColumnStore store_;
  ObTempColumnStore::Iterator store_it_;
  lib::MemoryContext mem_context_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_VEC_OP_H_ */
