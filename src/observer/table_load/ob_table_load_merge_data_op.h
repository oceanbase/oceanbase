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

#include "observer/table_load/ob_table_load_merge_table_op.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadMergeDataOp final : public ObTableLoadMergeTableBaseOp
{
public:
  ObTableLoadMergeDataOp(ObTableLoadMergeTableBaseOp *parent);
  virtual ~ObTableLoadMergeDataOp();

protected:
  int switch_next_op(bool is_parent_called) override;
  int acquire_child_op(ObTableLoadMergeOpType::Type child_op_type, ObIAllocator &allocator,
                       ObTableLoadMergeOp *&child) override;

private:
  enum Status
  {
    NONE = 0,
    MEM_SORT,
    COMPACT_TABLE,
    INSERT_SSTABLE,
    COMPLETED
  };
  Status status_;
};

} // namespace observer
} // namespace oceanbase
