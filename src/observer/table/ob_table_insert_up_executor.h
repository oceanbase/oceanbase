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
  typedef common::ObArrayWrap<ObTableInsUpdCtDef*> ObTableInsUpdCtDefArray;
  ObTableApiInsertUpSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        insert_up_ctdefs_(),
        conflict_checker_ctdef_(alloc)
  {
  }
  int init_ctdefs_array(int64_t size);
  virtual ~ObTableApiInsertUpSpec();
public:
  OB_INLINE const ObTableInsUpdCtDefArray& get_ctdefs() const { return insert_up_ctdefs_; }
  OB_INLINE ObTableInsUpdCtDefArray& get_ctdefs() { return insert_up_ctdefs_; }
  OB_INLINE const sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() const { return conflict_checker_ctdef_; }
  OB_INLINE sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() { return conflict_checker_ctdef_; }
private:
  ObTableInsUpdCtDefArray insert_up_ctdefs_;
  sql::ObConflictCheckerCtdef conflict_checker_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiInsertUpSpec);
};

class ObTableApiInsertUpExecutor : public ObTableApiModifyExecutor
{
public:
  typedef common::ObArrayWrap<ObTableInsUpdRtDef> ObTableInsUpdRtDefArray;
  ObTableApiInsertUpExecutor(ObTableCtx &ctx, const ObTableApiInsertUpSpec &spec)
      : ObTableApiModifyExecutor(ctx),
        insert_up_spec_(spec),
        insert_up_rtdefs_(),
        insert_row_(nullptr),
        old_row_(nullptr),
        insert_rows_(0),
        upd_changed_rows_(0),
        upd_rtctx_(eval_ctx_, exec_ctx_, get_fake_modify_op()),
        conflict_checker_(allocator_, eval_ctx_, spec.get_conflict_checker_ctdef()),
        is_duplicated_(false),
        cur_idx_(0),
        is_row_changed_(false),
        use_put_(false)
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
    insert_up_rtdefs_.release_array();
    conflict_checker_.destroy();
    upd_rtctx_.cleanup();
    ObTableApiModifyExecutor::destroy();
  }
public:
  OB_INLINE bool is_insert_duplicated()
  {
    return is_duplicated_;
  }
  OB_INLINE bool is_use_put()
  {
    return use_put_;
  }

  // WARIING: This interface is used to return the old row to determine whether it has expired when there
  // is a conflict between the data written in the redis ttl table.
  // When using it, you need to pay attention to the life cycle of the old row.
  // All in all: the row is assigned by the allocator of tb_ctx, which is consistent with its life cycle.
  int get_old_row(const ObNewRow *&row);

private:
  bool is_duplicated();
  OB_INLINE const common::ObIArray<sql::ObExpr *>& get_primary_table_new_row()
  {
    return insert_up_spec_.get_ctdefs().at(0)->ins_ctdef_.new_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_upd_new_row()
  {
    return insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_.new_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_upd_old_row()
  {
    return insert_up_spec_.get_ctdefs().at(0)->upd_ctdef_.old_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_insert_row()
  {
    return insert_up_spec_.get_ctdefs().at(0)->ins_ctdef_.new_row_;
  }
  int generate_insert_up_rtdefs();
  int refresh_exprs_frame(const ObTableEntity *entity);
  int get_next_row_from_child();
  int try_insert_row();
  int insert_row_to_das();
  void set_need_fetch_conflict();
  int try_update_row();
  int do_insert_up_cache();
  int cache_insert_row();
  int prepare_final_insert_up_task();
  int do_update(const sql::ObConflictValue &constraint_value);
  int do_insert(const sql::ObConflictValue &constraint_value);
  int delete_upd_old_row_to_das();
  int insert_upd_new_row_to_das();
  int reset_das_env();

private:
  const ObTableApiInsertUpSpec &insert_up_spec_;
  ObTableInsUpdRtDefArray insert_up_rtdefs_;
  ObChunkDatumStore::StoredRow *insert_row_;
  const ObChunkDatumStore::StoredRow *old_row_;
  int64_t insert_rows_;
  int64_t upd_changed_rows_;
  sql::ObDMLRtCtx upd_rtctx_;
  sql::ObConflictChecker conflict_checker_;
  bool is_duplicated_;
  int64_t cur_idx_;
  bool is_row_changed_;
  bool use_put_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_INSERT_UP_EXECUTOR_H */
