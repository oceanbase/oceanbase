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
class ObTableLoadMemCompactor;
class ObTableLoadMultipleHeapTableCompactor;

class ObTableLoadMergeMemSortOp final : public ObTableLoadMergeTableBaseOp
{
  friend class ObTableLoadMemCompactor;
  friend class ObTableLoadMultipleHeapTableCompactor;

public:
  ObTableLoadMergeMemSortOp(ObTableLoadMergeTableBaseOp *parent);
  virtual ~ObTableLoadMergeMemSortOp();
  int on_success() override;
  void stop() override;

protected:
  int switch_next_op(bool is_parent_called) override;

private:
  ObTableLoadMemCompactor *mem_compactor_;
  ObTableLoadMultipleHeapTableCompactor *multiple_heap_table_compactor_;
};

} // namespace observer
} // namespace oceanbase
