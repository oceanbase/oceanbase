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

#pragma once

#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_data_channel.h"
#include "observer/table_load/plan/ob_table_load_plan.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "storage/direct_load/ob_direct_load_dml_row_handler.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableOpOpenFlag
{
public:
  typedef uint64_t Type;

  static const uint64_t NEED_TRANS_PARAM = 1 << 0;
  static const uint64_t NEED_RESERVED_PARALLEL = 1 << 1;
  static const uint64_t NEED_ONLINE_OPT_STAT_GATHER = 1 << 2;
  static const uint64_t NEED_INSERT_LOB = 1 << 3;

  static bool need_trans_param(const Type flag) { return flag & NEED_TRANS_PARAM; }
  static bool need_reserved_parallel(const Type flag) { return flag & NEED_RESERVED_PARALLEL; }
  static bool need_online_opt_stat_gather(const Type flag)
  {
    return flag & NEED_ONLINE_OPT_STAT_GATHER;
  }
  static bool need_insert_lob(const Type flag) { return flag & NEED_INSERT_LOB; }
};

template <ObTableLoadTableType::Type table_type, ObTableLoadInputDataType::Type input_data_type,
          storage::ObDirectLoadMergeMode::Type merge_mode,
          storage::ObDirectLoadInsertSSTableType::Type insert_sstable_type,
          typename DMLRowHandlerType, ObTableLoadTableOpOpenFlag::Type open_flag>
class ObTableLoadTableOpDriver : public ObTableLoadTableOp
{
protected:
  ObTableLoadTableOpDriver(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx)
    : ObTableLoadTableOp(plan)
  {
    inner_op_ctx_.store_table_ctx_ = store_table_ctx;
  }

public:
  virtual ~ObTableLoadTableOpDriver() { reset(); }

  int prepare() override
  {
    int ret = OB_SUCCESS;
    if (inner_op_ctx_.table_store_.is_inited()) {
      // 如果一个TableOp有输入数据通道, 则可能被prepare多次
    } else if (OB_FAIL(inner_op_ctx_.table_store_.init())) {
      SERVER_LOG(WARN, "fail to init table store", KR(ret));
    } else {
      // 这里只设置table_data_desc
      // table类型由第一个推数据的对象设置, 后续推数据的对象都要校验table类型
      ObDirectLoadTableDataDesc table_data_desc;
      if (OB_FAIL(inner_op_ctx_.store_table_ctx_->get_table_data_desc(input_data_type,
                                                                      table_data_desc))) {
        SERVER_LOG(WARN, "fail to get table data desc", KR(ret));
      } else {
        inner_op_ctx_.table_store_.set_table_data_desc(table_data_desc);
      }
    }
    return ret;
  }

  int open() override
  {
    int ret = OB_SUCCESS;
    ObTableLoadStoreCtx *store_ctx = plan_->get_store_ctx();
    if (OB_FAIL(prepare())) {
      SERVER_LOG(WARN, "fail to prepare", KR(ret));
    }
    // 1. insert_table_ctx_
    if (OB_SUCC(ret)) {
      ObTableLoadStoreInsertTableParam param;
      param.insert_sstable_type_ = insert_sstable_type;
      param.need_reserved_parallel_ = ObTableLoadTableOpOpenFlag::need_reserved_parallel(open_flag);
      param.need_online_opt_stat_gather_ =
        ObTableLoadTableOpOpenFlag::need_online_opt_stat_gather(open_flag);
      param.need_insert_lob_ = ObTableLoadTableOpOpenFlag::need_insert_lob(open_flag);
      if (ObTableLoadTableOpOpenFlag::need_trans_param(open_flag) &&
          OB_FAIL(store_ctx->init_trans_param(param.trans_param_))) {
        SERVER_LOG(WARN, "fail to init trans param", KR(ret));
      } else if (OB_FAIL(inner_op_ctx_.store_table_ctx_->open_insert_table_ctx(
                   param, inner_op_ctx_.allocator_, inner_op_ctx_.insert_table_ctx_))) {
        SERVER_LOG(WARN, "fail to open insert table ctx", KR(ret));
      }
    }
    // 2. dml_row_handler_
    if (OB_SUCC(ret)) {
      DMLRowHandlerType *dml_row_handler = nullptr;
      if (OB_ISNULL(inner_op_ctx_.dml_row_handler_ = dml_row_handler =
                      OB_NEWx(DMLRowHandlerType, &inner_op_ctx_.allocator_, this))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "fail to new dml row handler", KR(ret));
      } else if (OB_FAIL(dml_row_handler->init())) {
        SERVER_LOG(WARN, "fail to init dml row handler", KR(ret));
      }
    }
    // 3. merge_mode_
    inner_op_ctx_.merge_mode_ = merge_mode;

    // 打开所有输出数据通道
    for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_.count(); ++i) {
      ObTableLoadTableChannel *table_channel = output_channels_.at(i);
      if (OB_FAIL(table_channel->open())) {
        SERVER_LOG(WARN, "fail to open table channel", KR(ret), KPC(table_channel));
      }
    }
    return ret;
  }

  int close() override
  {
    int ret = OB_SUCCESS;
    // 关闭所有输出数据通道
    for (int64_t i = 0; OB_SUCC(ret) && i < output_channels_.count(); ++i) {
      ObTableLoadTableChannel *table_channel = output_channels_.at(i);
      if (OB_FAIL(table_channel->close())) {
        SERVER_LOG(WARN, "fail to close table channel", KR(ret), KPC(table_channel));
      }
    }

    // 收集统计信息
    if (OB_SUCC(ret) && ObTableLoadTableOpOpenFlag::need_online_opt_stat_gather(open_flag)) {
      if (OB_FAIL(inner_op_ctx_.insert_table_ctx_->collect_sql_stats(
            plan_->get_store_ctx()->dml_stats_, plan_->get_store_ctx()->sql_stats_))) {
        SERVER_LOG(WARN, "fail to collect sql stats", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(inner_op_ctx_.insert_table_ctx_->close())) {
        SERVER_LOG(WARN, "fail to close insert table ctx", KR(ret));
      } else {
        reset();
      }
    }
    return ret;
  }

private:
  void reset()
  {
    if (nullptr != inner_op_ctx_.insert_table_ctx_) {
      inner_op_ctx_.insert_table_ctx_->~ObDirectLoadInsertTableContext();
      inner_op_ctx_.allocator_.free(inner_op_ctx_.insert_table_ctx_);
      inner_op_ctx_.insert_table_ctx_ = nullptr;
    }
    inner_op_ctx_.table_store_.reset();
    if (nullptr != inner_op_ctx_.dml_row_handler_) {
      inner_op_ctx_.dml_row_handler_->~ObDirectLoadDMLRowHandler();
      inner_op_ctx_.allocator_.free(inner_op_ctx_.dml_row_handler_);
      inner_op_ctx_.dml_row_handler_ = nullptr;
    }
    inner_op_ctx_.merge_mode_ = storage::ObDirectLoadMergeMode::INVALID_MERGE_MODE;
    inner_op_ctx_.allocator_.reset();
  }
};

} // namespace observer
} // namespace oceanbase
