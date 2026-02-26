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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_merge_data_op.h"
#include "observer/table_load/ob_table_load_merge_compact_table_op.h"
#include "observer/table_load/ob_table_load_merge_insert_sstable_op.h"
#include "observer/table_load/ob_table_load_merge_mem_sort_op.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace storage;

/**
 * ObTableLoadMergeDataOp
 */

ObTableLoadMergeDataOp::ObTableLoadMergeDataOp(ObTableLoadMergeTableBaseOp *parent)
  : ObTableLoadMergeTableBaseOp(parent), status_(Status::NONE)
{
}

ObTableLoadMergeDataOp::~ObTableLoadMergeDataOp() {}

int ObTableLoadMergeDataOp::switch_next_op(bool is_parent_called)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(merge_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge table ctx is null", KR(ret));
  } else {
    ObTableLoadMergeOpType::Type child_op_type = ObTableLoadMergeOpType::INVALID_OP_TYPE;
    switch (status_) {
      case Status::NONE:
        if (merge_table_ctx_->table_store_->empty()) {
          status_ = Status::INSERT_SSTABLE;
          child_op_type = ObTableLoadMergeOpType::INSERT_SSTABLE;
        } else if (merge_table_ctx_->table_store_->is_external_table()) {
          status_ = Status::MEM_SORT;
          child_op_type = ObTableLoadMergeOpType::MEM_SORT;
        } else {
          status_ = Status::COMPACT_TABLE;
          child_op_type = ObTableLoadMergeOpType::COMPACT_TABLE;
        }
        break;
      case Status::MEM_SORT:
        if (merge_table_ctx_->table_store_->is_multiple_sstable()) {
          status_ = Status::COMPACT_TABLE;
          child_op_type = ObTableLoadMergeOpType::COMPACT_TABLE;
        } else if (merge_table_ctx_->table_store_->is_multiple_heap_table()) {
          status_ = Status::INSERT_SSTABLE;
          child_op_type = ObTableLoadMergeOpType::INSERT_SSTABLE;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table store", KR(ret), KPC(merge_table_ctx_->table_store_));
        }
        break;
      case Status::COMPACT_TABLE:
        status_ = Status::INSERT_SSTABLE;
        child_op_type = ObTableLoadMergeOpType::INSERT_SSTABLE;
        break;
      case Status::INSERT_SSTABLE:
        status_ = Status::COMPLETED;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status", KR(ret), K(status_));
        break;
    }
    if (OB_SUCC(ret)) {
      if (Status::COMPLETED == status_) {
        if (OB_FAIL(switch_parent_op())) {
          LOG_WARN("fail to switch parent op", KR(ret));
        }
      } else if (OB_FAIL(switch_child_op(child_op_type))) {
        LOG_WARN("fail to switch child op", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadMergeDataOp::acquire_child_op(ObTableLoadMergeOpType::Type child_op_type,
                                             ObIAllocator &allocator, ObTableLoadMergeOp *&child)
{
  int ret = OB_SUCCESS;
  child = nullptr;
  switch (child_op_type) {
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::MEM_SORT,
                                         ObTableLoadMergeMemSortOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::COMPACT_TABLE,
                                         ObTableLoadMergeCompactTableOp);
    OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(ObTableLoadMergeOpType::INSERT_SSTABLE,
                                         ObTableLoadMergeInsertSSTableOp);
    OB_TABLE_LOAD_MERGE_UNEXPECTED_CHILD_OP_TYPE(child_op_type);
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
