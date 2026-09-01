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

namespace oceanbase
{
namespace sql
{

class ObTableLockOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableLockOpInput(ObExecContext &ctx, const ObOpSpec &spec)
      : ObTableModifyOpInput(ctx, spec)
  {
  }
  virtual int init(ObTaskInfo &task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
};

class ObTableLockSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:

  typedef common::ObArrayWrap<ObLockCtDef*> LockCtDefArray;
  typedef common::ObArrayWrap<LockCtDefArray> LockCtDef2DArray;

  ObTableLockSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObTableLockSpec();

  void set_wait_time(int64_t for_update_wait_us)
  {
    for_update_wait_us_ = for_update_wait_us;
  }
  void set_is_skip_locked(bool skip)
  {
    skip_locked_ = skip;
  }
  bool is_skip_locked() const { return skip_locked_; }

  bool is_nowait() const { return for_update_wait_us_ == 0; }

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(lock_ctdefs_.count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(lock_ctdefs_.count()));
    } else if (OB_UNLIKELY(lock_ctdefs_.at(0).count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(lock_ctdefs_.at(0).count()));
    } else {
      dml_ctdef = lock_ctdefs_.at(0).at(0);
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("table_modify_spec", ObTableModifySpec,
                       K_(for_update_wait_us), K_(skip_locked));
public:
  // projector for build the lock row
  int64_t for_update_wait_us_;
  bool skip_locked_;

  LockCtDef2DArray lock_ctdefs_;

  // used for multi table join lock
  // select * from t1 left join t2 on ... for update
  bool is_multi_table_skip_locked_;
private:
  common::ObIAllocator &alloc_;
};

class ObTableLockOp : public ObTableModifyOp
{
public:
  friend class ObTableModifyOp;

  typedef common::ObArrayWrap<ObLockRtDef> LockRtDefArray;
  typedef common::ObArrayWrap<LockRtDefArray> LockRtDef2DArray;

  ObTableLockOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  int inner_open() override;
  int inner_open_with_das();
  int inner_rescan() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(const int64_t max_row_cnt) override;
  int inner_close() override;
  virtual void destroy() override
  {
    ObTableModifyOp::destroy();
    lock_rtdefs_.release_array();
  }
  int init_lock_rtdef();

protected:
  OB_INLINE int get_next_batch_from_child(const int64_t max_row_cnt,
                                          const ObBatchRows *&child_brs);
  int lock_row_to_das();
  int lock_batch_to_das(const ObBatchRows *child_brs);
  int calc_tablet_loc(const ObLockCtDef &lock_ctdef,
                      ObLockRtDef &lock_rtdef,
                      ObDASTabletLoc *&tablet_loc);
  int lock_one_row_post_proc();
  virtual int write_rows_post_proc(int last_errno) override;
  int submit_row_by_strategy();

protected:
  transaction::ObTxSEQ savepoint_no_;
  LockRtDef2DArray lock_rtdefs_;
  bool need_return_row_;
};

} // end of sql
} // end of oceanbase

#endif // OB_TABLE_LOCK_OP_H
