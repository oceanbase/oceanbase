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

#ifndef OCEANBASE_SQL_OB_TABLE_REPLACE_OP_
#define OCEANBASE_SQL_OB_TABLE_REPLACE_OP_

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_conflict_checker.h"

namespace oceanbase
{
namespace sql
{

class ObTableReplaceSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
  typedef common::ObArrayWrap<ObReplaceCtDef*> ReplaceCtDefArray;
public:
  ObTableReplaceSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      replace_ctdefs_(),
      only_one_unique_key_(false),
      conflict_checker_ctdef_(alloc),
      has_global_unique_index_(false),
      all_saved_exprs_(alloc),
      doc_id_col_id_(OB_INVALID_ID),
      hidden_ck_col_id_(OB_INVALID_ID),
      alloc_(alloc)
  {
  }

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(replace_ctdefs_.count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(replace_ctdefs_.count()));
    } else {
      dml_ctdef = replace_ctdefs_.at(0)->ins_ctdef_;
    }
    return ret;
  }

  ReplaceCtDefArray replace_ctdefs_;
  bool only_one_unique_key_;
  ObConflictCheckerCtdef conflict_checker_ctdef_;
  bool has_global_unique_index_;
  // insert_row(new_row) + dependency child_output
  ExprFixedArray all_saved_exprs_;
  uint64_t doc_id_col_id_;
  uint64_t hidden_ck_col_id_;
protected:
  common::ObIAllocator &alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableReplaceSpec);
};

class ObTableReplaceOp : public ObTableModifyOp
{
public:
  ObTableReplaceOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObTableModifyOp(ctx, spec, input),
      insert_rows_(0),
      delete_rows_(0),
      conflict_checker_(ctx.get_allocator(),
                        eval_ctx_,
                        MY_SPEC.conflict_checker_ctdef_),
      replace_row_store_("ReplaceRow"),
      replace_rtctx_(eval_ctx_, ctx, *this),
      gts_state_(WITHOUT_GTS_OPT_STATE)
  {}
  virtual ~ObTableReplaceOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;

  // 物化所有要被replace into的行到replace_row_store_
  int load_all_replace_row(bool &is_iter_end);
  int get_next_row_from_child();

  // 执行所有尝试插入的 das task， fetch冲突行的主表主键
  int fetch_conflict_rowkey(int64_t replace_row_cnt);
  int get_next_conflict_rowkey(DASTaskIter &task_iter);

  virtual void destroy()
  {
    replace_rtctx_.cleanup();
    conflict_checker_.destroy();
    replace_rtdefs_.release_array();
    replace_row_store_.~ObChunkDatumStore();
    ObTableModifyOp::destroy();
  }

protected:

  int do_replace_into();

  int rollback_savepoint(const transaction::ObTxSEQ &savepoint_no);

  // 检查是否有duplicated key 错误发生
  bool check_is_duplicated();

  // 遍历replace_row_store_中的所有行，并且更新冲突map，
  int replace_conflict_row_cache();

  // 遍历主表的hash map，确定最后提交delete + insert对应的das task
  // 提交顺序上必须先提交delete
  int prepare_final_replace_task();

  int do_delete(ObConflictRowMap *primary_map);

  int do_insert(ObConflictRowMap *primary_map);

  // 提交所有的 try_insert 的 das task;
  int post_all_try_insert_das_task(ObDMLRtCtx &dml_rtctx);

  // 提交所有的 insert 和delete das task;
  int post_all_dml_das_task(ObDMLRtCtx &dml_rtctx);

  // batch的执行插入 process_row and then write to das,
  int insert_row_to_das(bool &is_skipped);

  int final_insert_row_to_das();

  // batch的执行插入 process_row and then write to das,
  int delete_row_to_das(bool need_do_trigger);

  int calc_insert_tablet_loc(const ObInsCtDef &ins_ctdef,
                             ObInsRtDef &ins_rtdef,
                             ObDASTabletLoc *&tablet_loc);
  int calc_delete_tablet_loc(const ObDelCtDef &del_ctdef,
                             ObDelRtDef &del_rtdef,
                             ObDASTabletLoc *&tablet_loc);

  int inner_open_with_das();

  int init_replace_rtdef();

  int check_values(bool &is_equal,
                   const ObChunkDatumStore::StoredRow *replace_row,
                   const ObChunkDatumStore::StoredRow *delete_row);
  virtual int check_need_exec_single_row() override;
  virtual ObDasParallelType check_das_parallel_type() override;
private:
  int check_replace_ctdefs_valid() const;

  const ObIArray<ObExpr *> &get_all_saved_exprs();

  const ObIArray<ObExpr *> &get_primary_table_new_row();

  const ObIArray<ObExpr *> &get_primary_table_old_row();

  int reset_das_env();

  void add_need_conflict_result_flag();

  int reuse();

protected:
  const static int64_t DEFAULT_REPLACE_BATCH_ROW_COUNT = 1000L;
  const static int64_t OB_DEFAULT_REPLACE_MEMORY_LIMIT = 2 * 1024 * 1024L;

  int64_t insert_rows_;
  int64_t delete_rows_;
  ObConflictChecker conflict_checker_;
  common::ObArrayWrap<ObReplaceRtDef> replace_rtdefs_;
  ObChunkDatumStore replace_row_store_; //所有的replace的行的集合
  ObDMLRtCtx replace_rtctx_;
  ObDmlGTSOptState gts_state_;
};

class ObTableReplaceOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableReplaceOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableReplaceOpInput);
};

} // end namespace sql
} // end namespace oceanbase

#endif
