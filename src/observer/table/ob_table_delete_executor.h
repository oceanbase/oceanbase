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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_DELETE_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_DELETE_EXECUTOR_H
#include "ob_table_modify_executor.h"
#include "ob_table_scan_executor.h"
#include "ob_table_context.h"

namespace oceanbase
{
namespace table
{

class ObTableApiDelSpec : public ObTableApiModifySpec
{
public:
  ObTableApiDelSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        del_ctdef_(alloc)
  {
  }
public:
  OB_INLINE const ObTableDelCtDef& get_ctdef() const { return del_ctdef_; }
  OB_INLINE ObTableDelCtDef& get_ctdef() { return del_ctdef_; }
private:
  ObTableDelCtDef del_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiDelSpec);
};

class ObTableApiDeleteExecutor : public ObTableApiModifyExecutor
{
public:
  ObTableApiDeleteExecutor(ObTableCtx &ctx, const ObTableApiDelSpec &spec)
      : ObTableApiModifyExecutor(ctx),
        entity_(nullptr),
        is_skip_scan_(false),
        del_spec_(spec),
        del_rtdef_(),
        cur_idx_(0)
  {
  }
  ~ObTableApiDeleteExecutor()
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
  OB_INLINE void set_entity(const ObITableEntity *entity) { entity_ = entity; }
  OB_INLINE void set_skip_scan(const bool &is_skip_scan) { is_skip_scan_ = is_skip_scan; }
  OB_INLINE int is_skip_scan() { return is_skip_scan_; }
private:
  int get_next_row_from_child();
  int del_rows_post_proc();
  int process_single_operation(const ObTableEntity *entity);
  int delete_row_skip_scan();
private:
  // for refresh expr frame
  const ObITableEntity *entity_;
  bool is_skip_scan_;
  const ObTableApiDelSpec &del_spec_;
  ObTableDelRtDef del_rtdef_;
  int64_t cur_idx_;
};

}  // namespace table
}  // namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_DELETE_EXECUTOR_H */