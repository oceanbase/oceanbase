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