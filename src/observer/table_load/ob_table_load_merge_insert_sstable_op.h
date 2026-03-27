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
class ObTableLoadParallelMerger;

class ObTableLoadMergeInsertSSTableOp final : public ObTableLoadMergeTableBaseOp
{
public:
  ObTableLoadMergeInsertSSTableOp(ObTableLoadMergeTableBaseOp *parent);
  virtual ~ObTableLoadMergeInsertSSTableOp();
  int on_success() override;
  void stop() override;

protected:
  int switch_next_op(bool is_parent_called) override;

private:
  ObTableLoadParallelMerger *parallel_merger_;
};

} // namespace observer
} // namespace oceanbase
