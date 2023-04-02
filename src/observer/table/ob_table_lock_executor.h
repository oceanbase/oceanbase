/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_LOCK_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_LOCK_EXECUTOR_H
#include "ob_table_modify_executor.h"
#include "ob_table_scan_executor.h"
#include "ob_table_context.h"
namespace oceanbase
{
namespace table
{

class ObTableApiLockSpec : public ObTableApiModifySpec
{
public:
  ObTableApiLockSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        lock_ctdef_(alloc)
  {
  }
public:
  OB_INLINE const ObTableLockCtDef& get_ctdef() const { return lock_ctdef_; }
  OB_INLINE ObTableLockCtDef& get_ctdef() { return lock_ctdef_; }
private:
  ObTableLockCtDef lock_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiLockSpec);
};

class ObTableApiLockExecutor : public ObTableApiModifyExecutor
{
public:
  ObTableApiLockExecutor(ObTableCtx &ctx, const ObTableApiLockSpec &lock_spec)
      : ObTableApiModifyExecutor(ctx),
        lock_spec_(lock_spec),
        lock_rtdef_(),
        cur_idx_(0)
  {
  }
  ~ObTableApiLockExecutor()
  {
    if (OB_NOT_NULL(child_)) {
      ObTableApiScanExecutor *scan_executor = static_cast<ObTableApiScanExecutor *>(child_);
      scan_executor->~ObTableApiScanExecutor();
    }
  }
public:
  virtual int open();
  virtual int get_next_row();
  virtual int close();
private:
  int generate_lock_rtdef();
  int get_next_row_from_child();
  int lock_row_to_das();
  int lock_rows_post_proc();
private:
  const ObTableApiLockSpec &lock_spec_;
  ObTableLockRtDef lock_rtdef_;
  int64_t cur_idx_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_LOCK_EXECUTOR_H */