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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_UPDATE_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_UPDATE_EXECUTOR_H
#include "ob_table_modify_executor.h"
#include "ob_table_context.h"

namespace oceanbase
{
namespace table
{
class ObTableApiUpdateSpec : public ObTableApiModifySpec
{
public:
  ObTableApiUpdateSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        udp_ctdef_(alloc)
  {
  }
public:
  OB_INLINE const ObTableUpdCtDef& get_ctdef() const { return udp_ctdef_; }
  OB_INLINE ObTableUpdCtDef& get_ctdef() { return udp_ctdef_; }
private:
  ObTableUpdCtDef udp_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiUpdateSpec);
};

class ObTableApiUpdateExecutor : public ObTableApiModifyExecutor
{
public:
  ObTableApiUpdateExecutor(ObTableCtx &ctx, const ObTableApiUpdateSpec &spec)
      : ObTableApiModifyExecutor(ctx),
        upd_spec_(spec),
        cur_idx_(0)
  {
  }
public:
  virtual int open() override;
  virtual int get_next_row() override;
  virtual int close() override;
private:
  int get_next_row_from_child();
  int update_row_to_das();
  int upd_rows_post_proc();
  int process_single_operation(const ObTableEntity *entity);
private:
  const ObTableApiUpdateSpec &upd_spec_;
  ObTableUpdRtDef upd_rtdef_;
  int64_t cur_idx_;
};

}  // namespace table
}  // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_UPDATE_EXECUTOR_H */