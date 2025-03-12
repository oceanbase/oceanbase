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

#include "observer/table_load/ob_table_load_merge_op.h"
#include "observer/table_load/ob_table_load_merge_phase_op.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObDirectLoadTableStore;
class ObDirectLoadDMLRowHandler;
} // namespace storage
namespace observer
{
class ObTableLoadStoreTableCtx;

class ObTableLoadMergeTableCtx
{
public:
  ObTableLoadMergeTableCtx();
  TO_STRING_KV(KP_(store_table_ctx), KP_(insert_table_ctx), KP_(table_store), KP_(dml_row_handler),
               "merge_mode", ObDirectLoadMergeMode::get_type_string(merge_mode_),
               K_(use_batch_mode), K_(need_calc_range), K_(need_close_insert_tablet_ctx),
               K_(is_del_lob));

public:
  ObTableLoadStoreTableCtx *store_table_ctx_;
  storage::ObDirectLoadInsertTableContext *insert_table_ctx_;
  storage::ObDirectLoadTableStore *table_store_;
  storage::ObDirectLoadDMLRowHandler *dml_row_handler_;
  ObDirectLoadMergeMode::Type merge_mode_;
  bool use_batch_mode_; // 全量 + 列存表
  // 合并任务结束要做的事情
  bool need_calc_range_; // rescan的前置
  bool need_close_insert_tablet_ctx_; // 本次合并是否需要关闭insert_table_ctx
  // 特殊合并任务标记
  bool is_del_lob_;
};

class ObTableLoadMergeTableBaseOp : public ObTableLoadMergePhaseBaseOp
{
public:
  ObTableLoadMergeTableBaseOp(ObTableLoadMergePhaseBaseOp *parent);
  ObTableLoadMergeTableBaseOp(ObTableLoadMergeTableBaseOp *parent);
  virtual ~ObTableLoadMergeTableBaseOp() = default;

public:
  ObTableLoadMergeTableCtx *merge_table_ctx_;
};

class ObTableLoadMergeTableOp : public ObTableLoadMergeTableBaseOp
{
public:
  ObTableLoadMergeTableOp(ObTableLoadMergeTableBaseOp *parent);
  ObTableLoadMergeTableOp(ObTableLoadMergePhaseBaseOp *parent);
  virtual ~ObTableLoadMergeTableOp();

protected:
  virtual int inner_init() = 0;
  virtual int inner_close() = 0;
  int switch_next_op(bool is_parent_called) override;
  int acquire_child_op(ObTableLoadMergeOpType::Type child_op_type, ObIAllocator &allocator,
                       ObTableLoadMergeOp *&child) override;

protected:
  enum Status
  {
    NONE = 0,
    MERGE_DATA,
    RESCAN,
    DEL_LOB,
    COMPLETED
  };

protected:
  ObTableLoadMergeTableCtx inner_ctx_;
  Status status_;
  bool need_rescan_;
  bool need_del_lob_;
};

class ObTableLoadMergeIndexTableOp final : public ObTableLoadMergeTableOp
{
public:
  ObTableLoadMergeIndexTableOp(ObTableLoadMergeTableBaseOp *parent,
                               ObTableLoadStoreTableCtx *store_table_ctx);
  virtual ~ObTableLoadMergeIndexTableOp() = default;

protected:
  int inner_init() override;
  int inner_close() override;

protected:
  ObTableLoadStoreTableCtx *store_table_ctx_;
};

class ObTableLoadMergeIndexesTableOp final : public ObTableLoadMergeTableOp
{
public:
  ObTableLoadMergeIndexesTableOp(ObTableLoadMergePhaseBaseOp *parent);
  virtual ~ObTableLoadMergeIndexesTableOp() = default;

protected:
  int inner_init() override;
  int inner_close() override;
  int switch_next_op(bool is_parent_called) override;
  int acquire_child_op(ObTableLoadMergeOpType::Type child_op_type, ObIAllocator &allocator,
                       ObTableLoadMergeOp *&child) override;

private:
  int64_t pos_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
