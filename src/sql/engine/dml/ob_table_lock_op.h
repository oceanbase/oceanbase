/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_TABLE_LOCK_OP_H
#define OB_TABLE_LOCK_OP_H
#include "sql/engine/dml/ob_table_modify_op.h"

namespace oceanbase {
namespace sql {

class ObTableLockOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableLockOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  virtual int init(ObTaskInfo& task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
};

class ObTableLockSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableLockSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);
  ~ObTableLockSpec();

  void set_wait_time(int64_t for_update_wait_us)
  {
    for_update_wait_us_ = for_update_wait_us;
  }
  void set_skip_locked(bool skip)
  {
    skip_locked_ = skip;
  }
  bool is_skip_locked() const
  {
    return skip_locked_;
  }

  bool is_nowait() const
  {
    return for_update_wait_us_ == 0;
  }

  INHERIT_TO_STRING_KV("table_modify_spec", ObTableModifySpec, K_(for_update_wait_us), K_(skip_locked));

public:
  // projector for build the lock row
  int64_t for_update_wait_us_;
  bool skip_locked_;  // UNUSED for the current
};

class ObTableLockOp : public ObTableModifyOp {
public:
  friend class ObTableModifyOp;

  ObTableLockOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);

  int inner_open() override;
  int rescan() override;
  int inner_get_next_row() override;
  int inner_close() override;
  void destroy() override
  {
    return ObTableModifyOp::destroy();
  }

protected:
  int do_table_lock();
  int lock_single_part();
  int lock_multi_part();
  int prepare_lock_row();

protected:
  common::ObNewRow lock_row_;
  storage::ObDMLBaseParam dml_param_;
  common::ObPartitionKey part_key_;
  common::ObSEArray<DMLPartInfo, 4> part_infos_;
  int64_t for_update_wait_timeout_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OB_TABLE_LOCK_OP_H
