/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "observer/table_load/plan/ob_table_load_data_channel.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace storage;

/**
 * ObTableLoadTableOpCtx
 */

ObTableLoadTableOpCtx::ObTableLoadTableOpCtx()
  : store_table_ctx_(nullptr),
    table_store_(),
    insert_table_ctx_(nullptr),
    dml_row_handler_(nullptr),
    merge_mode_(ObDirectLoadMergeMode::INVALID_MERGE_MODE),
    allocator_("TLD_TableOpCtx")
{
  allocator_.set_tenant_id(MTL_ID());
}

/**
 * ObTableLoadTableOp
 */

ObTableLoadTableOp::ObTableLoadTableOp(ObTableLoadPlan *plan)
  : ObTableLoadTableBaseOp(plan),
    table_type_(ObTableLoadTableType::INVALID_TABLE_TYPE),
    input_type_(ObTableLoadInputType::INVALID_INPUT_TYPE)
{
  op_ctx_ = &inner_op_ctx_;
  dependencies_.set_block_allocator(ModulePageAllocator(plan_->get_allocator()));
  dependees_.set_block_allocator(ModulePageAllocator(plan_->get_allocator()));
  input_channels_.set_block_allocator(ModulePageAllocator(plan_->get_allocator()));
  output_channels_.set_block_allocator(ModulePageAllocator(plan_->get_allocator()));
}

int ObTableLoadTableOp::add_dependency(ObTableLoadTableOp *dependency,
                                       const ObTableLoadDependencyType::Type dependency_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == dependency)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(dependency));
  } else {
    switch (dependency_type) {
      // 业务依赖, 只添加dependency关系
      case ObTableLoadDependencyType::BUSINESS_DEPENDENCY: {
        if (OB_FAIL(dependencies_.push_back(dependency))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(dependency->dependees_.push_back(this))) {
          LOG_WARN("fail to push back", KR(ret));
        }
        break;
      }
      // 数据依赖, 构建table_channel并添加dependency关系
      case ObTableLoadDependencyType::DATA_DEPENDENCY: {
        ObTableLoadTableChannel *table_channel = nullptr;
        if (OB_UNLIKELY(ObTableLoadInputType::CHANNEL_INPUT != input_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected input type", KR(ret), K(input_type_));
        } else if (OB_FAIL(plan_->alloc_channel(dependency, this, table_channel))) {
          LOG_WARN("fail to alloc channel", KR(ret));
        } else if (OB_FAIL(input_channels_.push_back(table_channel))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(dependency->output_channels_.push_back(table_channel))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(dependencies_.push_back(dependency))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(dependency->dependees_.push_back(this))) {
          LOG_WARN("fail to push back", KR(ret));
        }
        break;
      }
      // 嵌套数据依赖, 只构建table_channel
      case ObTableLoadDependencyType::NESTED_DATA_DEPENDENCY: {
        ObTableLoadTableChannel *table_channel = nullptr;
        if (OB_UNLIKELY(ObTableLoadInputType::CHANNEL_INPUT != input_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected input type", KR(ret), K(input_type_));
        } else if (OB_FAIL(plan_->alloc_channel(dependency, this, table_channel))) {
          LOG_WARN("fail to alloc channel", KR(ret));
        } else if (OB_FAIL(input_channels_.push_back(table_channel))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(dependency->output_channels_.push_back(table_channel))) {
          LOG_WARN("fail to push back", KR(ret));
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected dependency type", KR(ret), K(dependency_type));
        break;
    }
  }
  return ret;
}

// open/close

} // namespace observer
} // namespace oceanbase
