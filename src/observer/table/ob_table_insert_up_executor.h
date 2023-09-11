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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_INSERT_UP_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_INSERT_UP_EXECUTOR_H
#include "ob_table_modify_executor.h"
#include "ob_table_context.h"

namespace oceanbase
{
namespace table
{
class ObTableApiInsertUpSpec : public ObTableApiModifySpec
{
public:
  ObTableApiInsertUpSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        insert_up_ctdef_(alloc),
        conflict_checker_ctdef_(alloc)
  {
  }
public:
  OB_INLINE const ObTableInsUpdCtDef& get_ctdef() const { return insert_up_ctdef_; }
  OB_INLINE ObTableInsUpdCtDef& get_ctdef() { return insert_up_ctdef_; }
  OB_INLINE const sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() const { return conflict_checker_ctdef_; }
  OB_INLINE sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() { return conflict_checker_ctdef_; }
private:
  ObTableInsUpdCtDef insert_up_ctdef_;
  sql::ObConflictCheckerCtdef conflict_checker_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiInsertUpSpec);
};

class ObTableApiInsertUpExecutor : public ObTableApiModifyExecutor
{
public:
  ObTableApiInsertUpExecutor(ObTableCtx &ctx, const ObTableApiInsertUpSpec &spec)
      : ObTableApiModifyExecutor(ctx),
        allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        insert_up_spec_(spec),
        insert_up_rtdef_(),
        insert_row_(nullptr),
        insert_rows_(0),
        upd_changed_rows_(0),
        upd_rtctx_(eval_ctx_, exec_ctx_, get_fake_modify_op()),
        conflict_checker_(allocator_, eval_ctx_, spec.get_conflict_checker_ctdef()),
        is_duplicated_(false),
        cur_idx_(0),
        is_row_changed_(false)
  {
  }
  virtual ~ObTableApiInsertUpExecutor()
  {
    destroy();
  }
public:
  virtual int open();
  virtual int get_next_row();
  virtual int close();
  virtual void destroy()
  {
    // destroy
    conflict_checker_.destroy();
    upd_rtctx_.cleanup();
    ObTableApiModifyExecutor::destroy();
  }
public:
  OB_INLINE bool is_insert_duplicated()
  {
    return is_duplicated_;
  }
private:
  OB_INLINE bool is_duplicated()
  {
    is_duplicated_ = insert_up_rtdef_.ins_rtdef_.das_rtdef_.is_duplicated_;
    return is_duplicated_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *>& get_primary_table_new_row()
  {
    return insert_up_spec_.get_ctdef().ins_ctdef_.new_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_upd_new_row()
  {
    return insert_up_spec_.get_ctdef().upd_ctdef_.new_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_upd_old_row()
  {
    return insert_up_spec_.get_ctdef().upd_ctdef_.old_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_insert_row()
  {
    return insert_up_spec_.get_ctdef().ins_ctdef_.new_row_;
  }
  int generate_insert_up_rtdef(const ObTableInsUpdCtDef &ctdef,
                               ObTableInsUpdRtDef &rtdef);
  int refresh_exprs_frame(const ObTableEntity *entity);
  int get_next_row_from_child();
  int try_insert_row();
  int try_update_row();
  int do_insert_up_cache();
  int cache_insert_row();
  int prepare_final_insert_up_task();
  int do_update(const ObRowkey &constraint_rowkey,
                const sql::ObConflictValue &constraint_value);
  int do_insert(const ObRowkey &constraint_rowkey,
                const sql::ObConflictValue &constraint_value);
private:
  common::ObArenaAllocator allocator_;
  const ObTableApiInsertUpSpec &insert_up_spec_;
  ObTableInsUpdRtDef insert_up_rtdef_;
  ObChunkDatumStore::StoredRow *insert_row_;
  int64_t insert_rows_;
  int64_t upd_changed_rows_;
  sql::ObDMLRtCtx upd_rtctx_;
  sql::ObConflictChecker conflict_checker_;
  bool is_duplicated_;
  int64_t cur_idx_;
  bool is_row_changed_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_INSERT_UP_EXECUTOR_H */