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

#ifndef OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/basic/ob_material_op_impl.h"

namespace oceanbase
{
namespace sql
{

class ObMaterialSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObMaterialSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type)
  {}
};

class ObMaterialOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
  friend ObMaterialOp;
public:
  ObMaterialOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec), bypass_(false) {};
  virtual ~ObMaterialOpInput() = default;
  virtual int init(ObTaskInfo &task_info) override { UNUSED(task_info); return common::OB_SUCCESS; }
  virtual void reset() override { bypass_ = false; }
  void set_bypass(bool bypass) { bypass_ = bypass; }
  int64_t is_bypass() const { return bypass_; }
protected:
  bool bypass_; // if true, do bypass
};

class ObMaterialOp : public ObOperator
{
public:
  ObMaterialOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    is_first_(false),
    material_impl_(op_monitor_info_, profile_)
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
  int init_material_impl(int64_t tenant_id, int64_t row_count);

  int get_material_row_count(int64_t &count) const
  {
    count = material_impl_.get_material_row_count();
    return common::OB_SUCCESS;
  }
  // reset material iterator, used for NLJ/NL connectby
  int rewind();
private:
  int get_all_row_from_child(ObSQLSessionInfo &session);
  int get_all_batch_from_child(ObSQLSessionInfo &session);

private:
  friend class ObValues;
  ObSqlWorkAreaProfile profile_;
  bool is_first_;
  ObMaterialOpImpl material_impl_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_BASIC_OB_MATERIAL_OP_H_ */
