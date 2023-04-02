/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_INSERT_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_INSERT_EXECUTOR_H
#include "ob_table_modify_executor.h"
#include "ob_table_context.h"

namespace oceanbase
{
namespace table
{

class ObTableApiInsertSpec : public ObTableApiModifySpec
{
public:
  ObTableApiInsertSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        ins_ctdef_(alloc)
  {
  }
public:
  OB_INLINE const ObTableInsCtDef& get_ctdef() const { return ins_ctdef_; }
  OB_INLINE ObTableInsCtDef& get_ctdef() { return ins_ctdef_; }
private:
  ObTableInsCtDef ins_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiInsertSpec);
};

class ObTableApiInsertExecutor : public ObTableApiModifyExecutor
{
public:
  ObTableApiInsertExecutor(ObTableCtx &ctx, const ObTableApiInsertSpec &spec)
      : ObTableApiModifyExecutor(ctx),
        ins_spec_(spec),
        cur_idx_(0)
  {
  }

public:
  virtual int open() override;
  virtual int get_next_row() override;
  virtual int close() override;
private:
  int process_single_operation(const ObTableEntity *entity);
  int get_next_row_from_child();
  int ins_rows_post_proc();
  int insert_row_to_das();

private:
  const ObTableApiInsertSpec &ins_spec_;
  ObTableInsRtDef ins_rtdef_;
  int64_t cur_idx_;
};

}  // namespace table
}  // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_INSERT_EXECUTOR_H */