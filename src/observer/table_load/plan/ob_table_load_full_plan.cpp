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

#include "observer/table_load/plan/ob_table_load_full_plan.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_data_table_row_handler.h"
#include "observer/table_load/plan/ob_table_load_table_op_define.h"

namespace oceanbase
{
namespace observer
{
using namespace storage;

static const uint64_t OB_TABLE_LOAD_FULL_BASIC_OPEN_FLAG =
  ObTableLoadTableOpOpenFlag::NEED_ONLINE_OPT_STAT_GATHER |
  ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB;

// 定义TableOp
// op_type, op_name, input_type, input_data_type, merge_mode, insert_sstable_type, dml_row_handler,
// open_flag
OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(FULL_DATA_TABLE_OP, FullDataTableNormalOp, WRITE_INPUT,
                                   ADAPTIVE_FULL_ROW, MERGE_WITH_ORIGIN_DATA, MAJOR,
                                   ObTableLoadDataTableInsertRowHandler,
                                   OB_TABLE_LOAD_FULL_BASIC_OPEN_FLAG |
                                     ObTableLoadTableOpOpenFlag::NEED_RESERVED_PARALLEL);
OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(FULL_DATA_TABLE_OP, FullDataTableOverwriteOp, WRITE_INPUT,
                                   ADAPTIVE_FULL_ROW, NORMAL, MAJOR,
                                   ObTableLoadDataTableInsertRowHandler,
                                   OB_TABLE_LOAD_FULL_BASIC_OPEN_FLAG);

/**
 * ObTableLoadFullPlan
 */

int ObTableLoadFullPlan::generate()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_generated())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected generate twice", KR(ret));
  } else {
    ObTableLoadTableOp *data_table_op = nullptr;
    // overwrite
    if (ObDirectLoadInsertMode::is_overwrite_mode(store_ctx_->ctx_->param_.insert_mode_)) {
      ObTableLoadFullDataTableOverwriteOp *data_table_overwrite_op = nullptr;
      if (OB_FAIL(ObTableLoadFullDataTableOverwriteOp::build(
            this, store_ctx_->data_store_table_ctx_, get_write_type(), data_table_overwrite_op))) {
        LOG_WARN("fail to build full data table op", KR(ret));
      } else {
        data_table_op = data_table_overwrite_op;
      }
    }
    // normal
    else {
      ObTableLoadFullDataTableNormalOp *data_table_normal_op = nullptr;
      if (OB_FAIL(ObTableLoadFullDataTableNormalOp::build(
            this, store_ctx_->data_store_table_ctx_, get_write_type(), data_table_normal_op))) {
        LOG_WARN("fail to build full data table op", KR(ret));
      } else {
        data_table_op = data_table_normal_op;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_generate(data_table_op))) {
        LOG_WARN("fail to finish generate", KR(ret), KPC(data_table_op));
      }
    }
  }
  return ret;
}

int ObTableLoadFullPlan::create_plan(ObTableLoadStoreCtx *store_ctx, ObIAllocator &allocator,
                                     ObTableLoadPlan *&plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan = OB_NEWx(ObTableLoadFullPlan, &allocator, store_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadFullPlan", KR(ret));
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
