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
#include "observer/table_load/ob_table_load_struct.h"
#include "storage/direct_load/ob_direct_load_trans_param.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadMergePhaseCtx
{
public:
  ObTableLoadMergePhaseCtx();
  TO_STRING_KV(K_(phase), K_(trans_param));

public:
  ObTableLoadMergerPhaseType phase_;
  storage::ObDirectLoadTransParam trans_param_;
};

class ObTableLoadMergePhaseBaseOp : public ObTableLoadMergeOp
{
public:
  ObTableLoadMergePhaseBaseOp(ObTableLoadMergeOp *parent);
  ObTableLoadMergePhaseBaseOp(ObTableLoadMergePhaseBaseOp *parent);
  virtual ~ObTableLoadMergePhaseBaseOp() = default;

public:
  ObTableLoadMergePhaseCtx *merge_phase_ctx_;
};

class ObTableLoadMergePhaseOp : public ObTableLoadMergePhaseBaseOp
{
public:
  ObTableLoadMergePhaseOp(ObTableLoadMergeOp *parent);
  virtual ~ObTableLoadMergePhaseOp() = default;

protected:
  virtual int inner_init() = 0;
  virtual int inner_close() = 0;
  int acquire_child_op(ObTableLoadMergeOpType::Type child_op_type, ObIAllocator &allocator,
                       ObTableLoadMergeOp *&child) override;

protected:
  enum Status
  {
    NONE = 0,
    DATA_MERGE,
    INDEX_MERGE,
    CONFLICT_MERGE,
    COMPLETED
  };

protected:
  ObTableLoadMergePhaseCtx inner_phase_ctx_;
  Status status_;
};

class ObTableLoadMergeInsertPhaseOp : public ObTableLoadMergePhaseOp
{
public:
  ObTableLoadMergeInsertPhaseOp(ObTableLoadMergeOp *parent);
  virtual ~ObTableLoadMergeInsertPhaseOp();

protected:
  int inner_init() override;
  int inner_close() override;
  int switch_next_op(bool is_parent_called) override;
};

class ObTableLoadMergeDeletePhaseOp : public ObTableLoadMergePhaseOp
{
public:
  ObTableLoadMergeDeletePhaseOp(ObTableLoadMergeOp *parent);
  virtual ~ObTableLoadMergeDeletePhaseOp();

protected:
  int inner_init() override;
  int inner_close() override;
  int switch_next_op(bool is_parent_called) override;
};

class ObTableLoadMergeAckPhaseOp : public ObTableLoadMergePhaseOp
{
public:
  ObTableLoadMergeAckPhaseOp(ObTableLoadMergeOp *parent);
  virtual ~ObTableLoadMergeAckPhaseOp();

protected:
  int inner_init() override;
  int inner_close() override;
  int switch_next_op(bool is_parent_called) override;
};

} // namespace observer
} // namespace oceanbase