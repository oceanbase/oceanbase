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
class ObTableLoadMergeDataTableOp final : public ObTableLoadMergeTableOp
{
public:
  ObTableLoadMergeDataTableOp(ObTableLoadMergePhaseBaseOp *parent);
  virtual ~ObTableLoadMergeDataTableOp() = default;

protected:
  int inner_init() override;
  int inner_close() override;
};

class ObTableLoadMergeDeletePhaseDataTableOp final : public ObTableLoadMergeTableOp
{
public:
  ObTableLoadMergeDeletePhaseDataTableOp(ObTableLoadMergePhaseBaseOp *parent);
  virtual ~ObTableLoadMergeDeletePhaseDataTableOp() = default;

protected:
  int inner_init() override;
  int inner_close() override;
};

class ObTableLoadMergeAckPhaseDataTableOp final : public ObTableLoadMergeTableOp
{
public:
  ObTableLoadMergeAckPhaseDataTableOp(ObTableLoadMergePhaseBaseOp *parent);
  virtual ~ObTableLoadMergeAckPhaseDataTableOp() = default;

protected:
  int inner_init() override;
  int inner_close() override;
};

} // namespace observer
} // namespace oceanbase