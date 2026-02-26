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

#ifndef OCEANBASE_OBSERVER_TABLE_REPLACE_EXECUTOR_H
#define OCEANBASE_OBSERVER_TABLE_REPLACE_EXECUTOR_H
#include "ob_table_modify_executor.h"
#include "ob_table_context.h"

namespace oceanbase
{
namespace table
{

class ObTableApiReplaceSpec : public ObTableApiModifySpec
{
public:
  typedef common::ObArrayWrap<ObTableReplaceCtDef*> ObTableReplaceCtDefArray;
  ObTableApiReplaceSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        replace_ctdefs_(),
        conflict_checker_ctdef_(alloc)
  {
  }
  int init_ctdefs_array(int64_t size);
  virtual ~ObTableApiReplaceSpec();
public:
  OB_INLINE const ObTableReplaceCtDefArray& get_ctdefs() const { return replace_ctdefs_; }
  OB_INLINE ObTableReplaceCtDefArray& get_ctdefs() { return replace_ctdefs_; }
  OB_INLINE const sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() const { return conflict_checker_ctdef_; }
  OB_INLINE sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() { return conflict_checker_ctdef_; }
private:
  ObTableReplaceCtDefArray replace_ctdefs_;
  sql::ObConflictCheckerCtdef conflict_checker_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiReplaceSpec);
};

class ObTableApiReplaceExecutor : public ObTableApiModifyExecutor
{
public:
  typedef common::ObArrayWrap<ObTableReplaceRtDef> ObTableReplaceRtDefArray;
  ObTableApiReplaceExecutor(ObTableCtx &ctx, const ObTableApiReplaceSpec &replace_spec)
      : ObTableApiModifyExecutor(ctx),
        replace_spec_(replace_spec),
        insert_row_(NULL),
        insert_rows_(0),
        delete_rows_(0),
        conflict_checker_(allocator_, eval_ctx_, replace_spec_.get_conflict_checker_ctdef()),
        cur_idx_(0)
  {
  }
  virtual ~ObTableApiReplaceExecutor()
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
    replace_rtdefs_.release_array();
    conflict_checker_.destroy();
    ObTableApiModifyExecutor::destroy();
  }
private:
  const static int64_t DEFAULT_REPLACE_BATCH_ROW_COUNT = 1000L;
  OB_INLINE const common::ObIArray<sql::ObExpr *>& get_primary_table_new_row()
  {
    return replace_spec_.get_ctdefs().at(0)->ins_ctdef_.new_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *>& get_primary_table_old_row()
  {
    return replace_spec_.get_ctdefs().at(0)->del_ctdef_.old_row_;
  }
  bool is_duplicated();
  int generate_replace_rtdefs();
  int refresh_exprs_frame(const ObTableEntity *entity);
  void set_need_fetch_conflict();
  int load_replace_rows(bool &is_iter_end);
  int get_next_row_from_child();
  int insert_row_to_das();
  int delete_row_to_das();
  int post_das_task();
  int check_values(bool &is_equal,
                   const ObChunkDatumStore::StoredRow *replace_row,
                   const ObChunkDatumStore::StoredRow *delete_row);
  int prepare_final_replace_task();
  int cache_insert_row();
  int do_delete(ObConflictRowMap *primary_map);
  int do_insert();
  int reuse();
  int reset_das_env();
private:
  const ObTableApiReplaceSpec &replace_spec_;
  ObTableReplaceRtDefArray replace_rtdefs_;
  ObChunkDatumStore::StoredRow *insert_row_;
  int64_t insert_rows_;
  int64_t delete_rows_;
  sql::ObConflictChecker conflict_checker_;
  int64_t cur_idx_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_REPLACE_EXECUTOR_H */