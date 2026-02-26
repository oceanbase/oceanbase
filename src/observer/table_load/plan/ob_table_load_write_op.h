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

#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadDagDirectWriteChannel;
class ObTableLoadDagStoreWriteChannel;
class ObTableLoadDagPreSortWriteChannel;

class ObTableLoadWriteOp : public ObTableLoadTableBaseOp
{
protected:
  ObTableLoadWriteOp(ObTableLoadTableBaseOp *parent) : ObTableLoadTableBaseOp(parent) {}

public:
  static int build(ObTableLoadTableOp *table_op, const ObTableLoadWriteType::Type write_type,
                   ObTableLoadWriteOp *&write_op);
};

// direct_write
class ObTableLoadDirectWriteOp final : public ObTableLoadWriteOp
{
public:
  ObTableLoadDirectWriteOp(ObTableLoadTableBaseOp *parent);
  virtual ~ObTableLoadDirectWriteOp();

public:
  ObTableLoadDagDirectWriteChannel *write_channel_;
};

// store_write
class ObTableLoadStoreWriteOp final : public ObTableLoadWriteOp
{
public:
  ObTableLoadStoreWriteOp(ObTableLoadTableBaseOp *parent);
  virtual ~ObTableLoadStoreWriteOp();

public:
  ObTableLoadDagStoreWriteChannel *write_channel_;
};

// pre_sort_write
class ObTableLoadPreSortWriteOp final : public ObTableLoadWriteOp
{
public:
  ObTableLoadPreSortWriteOp(ObTableLoadTableBaseOp *parent);
  virtual ~ObTableLoadPreSortWriteOp();

public:
  ObTableLoadDagPreSortWriteChannel *write_channel_;
};

} // namespace observer
} // namespace oceanbase
