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

#include "lib/allocator/page_arena.h"
#include "observer/table_load/plan/ob_table_load_op.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObDirectLoadDMLRowHandler;
} // namespace storage
namespace observer
{
class ObTableLoadStoreTableCtx;
class ObTableLoadTableChannel;

struct ObTableLoadTableOpCtx
{
public:
  ObTableLoadTableOpCtx();

  TO_STRING_KV(KP_(store_table_ctx), K_(table_store), KP_(insert_table_ctx), KP_(dml_row_handler),
               "merge_mode", storage::ObDirectLoadMergeMode::get_type_string(merge_mode_));

public:
  ObTableLoadStoreTableCtx *store_table_ctx_;
  storage::ObDirectLoadTableStore table_store_;
  storage::ObDirectLoadInsertTableContext *insert_table_ctx_;
  storage::ObDirectLoadDMLRowHandler *dml_row_handler_;
  storage::ObDirectLoadMergeMode::Type merge_mode_;
  ObArenaAllocator allocator_; // TableOp运行时的内存分配器
};

class ObTableLoadTableBaseOp : public ObTableLoadOpFactory<ObTableLoadTableBaseOp>
{
  using SuperClass = ObTableLoadOpFactory<ObTableLoadTableBaseOp>;

protected:
  // 在plan下创建, 只有TableOp会直接在plan下创建, 如DataTableOp, LobTableOp, IndexTableOp
  ObTableLoadTableBaseOp(ObTableLoadPlan *plan) : SuperClass(plan), op_ctx_(nullptr) {}
  // 在TableBaseOp下创建孩子TableBaseOp, 如MergeDataOp, MemSortOp, CompactDataOp, InsertSSTableOp
  ObTableLoadTableBaseOp(ObTableLoadTableBaseOp *parent)
    : SuperClass(parent), op_ctx_(parent->op_ctx_)
  {
  }

public:
  INHERIT_TO_STRING_KV("ObTableLoadOp", ObTableLoadOp, KP_(op_ctx));

public:
  ObTableLoadTableOpCtx *op_ctx_;
};

class ObTableLoadTableOp : public ObTableLoadTableBaseOp
{
protected:
  ObTableLoadTableOp(ObTableLoadPlan *plan);

public:
  virtual ~ObTableLoadTableOp() = default;
  // prepare是初始化table_store, 让上游TableOp能推数据进来
  // 对于WRITE_INPUT, 则由write_op调用
  // 对于CHANNEL_INPUT, 则由data_channel调用
  virtual int prepare() = 0;
  virtual int open() = 0;
  virtual int close() = 0;

  int add_dependency(ObTableLoadTableOp *dependency,
                     const ObTableLoadDependencyType::Type dependency_type);

  ObTableLoadTableType::Type get_table_type() const { return table_type_; }
  ObTableLoadInputType::Type get_input_type() const { return input_type_; }
  const ObIArray<ObTableLoadTableOp *> &get_dependencies() const { return dependencies_; }
  const ObIArray<ObTableLoadTableOp *> &get_dependees() const { return dependees_; }
  const ObIArray<ObTableLoadTableChannel *> &get_input_channels() const { return input_channels_; }
  const ObIArray<ObTableLoadTableChannel *> &get_output_channels() const
  {
    return output_channels_;
  }

  INHERIT_TO_STRING_KV("ObTableLoadTableBaseOp", ObTableLoadTableBaseOp, "table_type",
                       ObTableLoadTableType::get_type_string(table_type_), "input_type",
                       ObTableLoadInputType::get_type_string(input_type_));

protected:
  ObTableLoadTableType::Type table_type_;
  ObTableLoadInputType::Type input_type_;
  // TableOp之间的依赖关系
  ObArray<ObTableLoadTableOp *> dependencies_; // 上游TableOp
  ObArray<ObTableLoadTableOp *> dependees_; // 下游TableOp
  // 数据通道
  ObArray<ObTableLoadTableChannel *> input_channels_; // 输入通道
  ObArray<ObTableLoadTableChannel *> output_channels_; // 输出通道
  // 每个TableOp都有独立的上下文
  ObTableLoadTableOpCtx inner_op_ctx_;
};

// open/close

class ObTableLoadTableOpOpenOp final : public ObTableLoadTableBaseOp
{
public:
  ObTableLoadTableOpOpenOp(ObTableLoadTableBaseOp *parent, ObTableLoadTableOp *table_op)
    : ObTableLoadTableBaseOp(parent), table_op_(table_op)
  {
    op_type_ = ObTableLoadOpType::TABLE_OP_OPEN_OP;
  }

public:
  ObTableLoadTableOp *const table_op_;
};

class ObTableLoadTableOpCloseOp final : public ObTableLoadTableBaseOp
{
public:
  ObTableLoadTableOpCloseOp(ObTableLoadTableBaseOp *parent, ObTableLoadTableOp *table_op)
    : ObTableLoadTableBaseOp(parent), table_op_(table_op)
  {
    op_type_ = ObTableLoadOpType::TABLE_OP_CLOSE_OP;
  }

public:
  ObTableLoadTableOp *const table_op_;
};

} // namespace observer
} // namespace oceanbase
