/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_TTL_EXECUTOR_H
#define OCEANBASE_OBSERVER_OB_TABLE_TTL_EXECUTOR_H
#include "src/observer/table/ob_table_modify_executor.h"
#include "src/observer/table/ob_table_context.h"

namespace oceanbase
{
namespace table
{
class ObTableApiTTLSpec : public ObTableApiModifySpec
{
public:
  ObTableApiTTLSpec(common::ObIAllocator &alloc, const ObTableExecutorType type)
      : ObTableApiModifySpec(alloc, type),
        ttl_ctdef_(alloc),
	conflict_checker_ctdef_(alloc)
  {
  }
public:
  OB_INLINE const ObTableTTLCtDef& get_ctdef() const { return ttl_ctdef_; }
  OB_INLINE ObTableTTLCtDef& get_ctdef() { return ttl_ctdef_; }
  OB_INLINE const sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() const { return conflict_checker_ctdef_; }
  OB_INLINE sql::ObConflictCheckerCtdef& get_conflict_checker_ctdef() { return conflict_checker_ctdef_; }
private:
  ObTableTTLCtDef ttl_ctdef_;
	sql::ObConflictCheckerCtdef conflict_checker_ctdef_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableApiTTLSpec);
};

class ObTableApiTTLExecutor : public ObTableApiModifyExecutor
{
public:
  ObTableApiTTLExecutor(ObTableCtx &ctx, const ObTableApiTTLSpec &ttl_spec)
      : ObTableApiModifyExecutor(ctx),
        allocator_(ObModIds::TABLE_PROC, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
        ttl_spec_(ttl_spec),
        upd_rtctx_(eval_ctx_, exec_ctx_, get_fake_modify_op()),
        conflict_checker_(allocator_, eval_ctx_, ttl_spec_.get_conflict_checker_ctdef()),
        cur_idx_(0),
        is_row_changed_(false),
        is_duplicated_(false),
        is_expired_(false),
        insert_rows_(0)
  {
  }
  virtual ~ObTableApiTTLExecutor()
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
  OB_INLINE bool is_expired()
  {
    return is_expired_;
  }
private:
  OB_INLINE bool is_duplicated()
  {
    is_duplicated_ =  ttl_rtdef_.ins_rtdef_.das_rtdef_.is_duplicated_;
    return is_duplicated_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_insert_row()
  {
    return ttl_spec_.get_ctdef().ins_ctdef_.new_row_;
  }
  OB_INLINE const common::ObIArray<sql::ObExpr *> &get_primary_table_upd_old_row()
  {
    return ttl_spec_.get_ctdef().upd_ctdef_.old_row_;
  }
  int check_expired(bool &is_expired);
  int generate_ttl_rtdef(const ObTableTTLCtDef &ctdef, ObTableTTLRtDef &rtdef);
  int get_next_row_from_child();
  int refresh_exprs_frame(const table::ObTableEntity *entity);
  int do_insert();
  int do_delete();
  int try_insert_row();
  int process_expire();
  int update_row_to_conflict_checker();
  int update_row_to_das();
  int do_update();
private:
  common::ObArenaAllocator allocator_;
  const ObTableApiTTLSpec &ttl_spec_;
  ObTableTTLRtDef ttl_rtdef_;
  sql::ObDMLRtCtx upd_rtctx_;
  sql::ObConflictChecker conflict_checker_;
  int64_t cur_idx_;
  bool is_row_changed_;
  bool is_duplicated_;
  bool is_expired_;
  int64_t insert_rows_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_TTL_EXECUTOR_H */