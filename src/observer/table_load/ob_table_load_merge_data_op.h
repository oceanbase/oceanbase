/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
