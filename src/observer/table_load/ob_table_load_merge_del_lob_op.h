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
class ObTableLoadMergeDelLobOp final : public ObTableLoadMergeTableOp
{
public:
  ObTableLoadMergeDelLobOp(ObTableLoadMergeTableBaseOp *parent);
  virtual ~ObTableLoadMergeDelLobOp() = default;

protected:
  int inner_init() override;
  int inner_close() override;
};

} // namespace observer
} // namespace oceanbase
