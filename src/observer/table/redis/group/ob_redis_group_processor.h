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

#pragma once
#include "observer/table/group/ob_i_table_struct.h"
#include "observer/table/redis/cmd/ob_redis_cmd.h"
#include "observer/table/redis/ob_redis_context.h"
namespace oceanbase
{
namespace table
{
class ObRedisGroupOpProcessor : public ObITableOpProcessor
{
public:
  ObRedisGroupOpProcessor()
    : ObITableOpProcessor(),
      allocator_(nullptr),
      processor_entity_factory_("RedisProrEntFac", MTL_ID())
  {}

  ObRedisGroupOpProcessor(
      ObTableGroupType op_type,
      ObTableGroupCtx *group_ctx,
      ObIArray<ObITableOp *> *ops,
      ObTableCreateCbFunctor *functor)
      : ObITableOpProcessor(op_type, group_ctx, ops, functor),
        allocator_(nullptr),
        processor_entity_factory_("RedisProrEntFac", MTL_ID())
  {}
  virtual ~ObRedisGroupOpProcessor() {}
  virtual int init(ObTableGroupCtx &group_ctx, ObIArray<ObITableOp*> *ops) override;
  virtual int process() override;
  int is_valid();

private:
  int init_batch_ctx(ObRedisBatchCtx &batch_ctx);
  int set_group_need_dist_das(ObRedisBatchCtx &batch_ctx);
  int end_trans( ObRedisBatchCtx &redis_ctx, bool need_snapshot, bool is_rollback);
private:
  common::ObIAllocator *allocator_;
  table::ObTableEntityFactory<table::ObTableEntity> processor_entity_factory_;
};

}  // namespace table
}  // namespace oceanbase
