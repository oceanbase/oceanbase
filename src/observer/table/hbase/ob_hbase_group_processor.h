/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_HBASE_GROUP_PROCESSOR_H
#define _OB_HBASE_GROUP_PROCESSOR_H

#include "ob_hbase_group_struct.h"
#include "observer/table/group/ob_i_table_struct.h"
#include "observer/table/ob_table_multi_batch_common.h"

namespace oceanbase
{
namespace table
{
class ObHbaseOpProcessor : public ObITableOpProcessor
{
public:
  ObHbaseOpProcessor()
    : ObITableOpProcessor(),
      allocator_("HbasePror", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      default_entity_factory_("HbaseEntFac", MTL_ID()),
      multi_batch_ctx_(nullptr)
  {}
  ObHbaseOpProcessor(ObTableGroupType op_type,
                     ObTableGroupCtx *group_ctx,
                     ObIArray<ObITableOp *> *ops,
                     ObTableCreateCbFunctor *functor)
      : ObITableOpProcessor(op_type, group_ctx, ops, functor),
        allocator_("HbasePror", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
  {}
  virtual ~ObHbaseOpProcessor() {}
  virtual int init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops) override;
  virtual int process() override;
private:
  int fill_batch_op(const ObTableTabletOp &tablet_op, ObTableBatchOperation &batch_op);
  int init_multi_batch_ctx();
  int init_table_ctx(const ObTableSingleOp &single_op, ObTableCtx &tb_ctx);
  int init_result(const int64_t total_tablet_count, ObTableMultiBatchResult &result);
  int64_t get_tablet_count() const
  {
    int cnt = 0;

    if (OB_NOT_NULL(ops_)) {
      for (int64_t i = 0; i < ops_->count(); i++) {
        const ObHbaseOp *op = reinterpret_cast<ObHbaseOp*>(ops_->at(i));
        cnt += op->ls_req_.ls_op_.count();
      }
    }

    return cnt;
  }
private:
  ObArenaAllocator allocator_;
  table::ObTableEntityFactory<table::ObTableSingleOpEntity> default_entity_factory_;
  ObTableMultiBatchCtx *multi_batch_ctx_;
};

}  // namespace table
}  // namespace oceanbase

#endif /* _OB_HBASE_GROUP_PROCESSOR_H */
