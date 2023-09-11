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

#ifndef OCEANBASE_DML_OB_TABLE_INSERT_UP_OP_H_
#define OCEANBASE_DML_OB_TABLE_INSERT_UP_OP_H_

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_conflict_checker.h"

namespace oceanbase
{
namespace sql
{
class ObTableInsertUpOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableInsertUpOpInput(ObExecContext &ctx, const ObOpSpec &spec)
      : ObTableModifyOpInput(ctx, spec)
  {
  }
  virtual int init(ObTaskInfo &task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
};

class ObTableInsertUpSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
  typedef common::ObArrayWrap<ObInsertUpCtDef*> InsUpdCtDefArray;
public:
  ObTableInsertUpSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      insert_up_ctdefs_(),
      conflict_checker_ctdef_(alloc),
      all_saved_exprs_(alloc),
      alloc_(alloc)
  {
  }

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(insert_up_ctdefs_.count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(insert_up_ctdefs_.count()));
    } else {
      dml_ctdef = insert_up_ctdefs_.at(0)->ins_ctdef_;
    }
    return ret;
  }

  InsUpdCtDefArray insert_up_ctdefs_;
  ObConflictCheckerCtdef conflict_checker_ctdef_;
  // insert_row + child_->output_
  ExprFixedArray all_saved_exprs_;
protected:
  common::ObIAllocator &alloc_;
};

class ObTableInsertUpOp : public ObTableModifyOp
{
public:
  ObTableInsertUpOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObTableModifyOp(ctx, spec, input),
      insert_rows_(0),
      upd_changed_rows_(0),
      found_rows_(0),
      upd_rtctx_(eval_ctx_, ctx, *this),
      conflict_checker_(ctx.get_allocator(),
                        eval_ctx_,
                        MY_SPEC.conflict_checker_ctdef_),
      insert_up_row_store_("InsertUpRow"),
      is_ignore_(false)

  {
  }

  virtual ~ObTableInsertUpOp() {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;

  int calc_auto_increment(const ObUpdCtDef &upd_ctdef);

  int update_auto_increment(const ObExpr &expr,
                            const uint64_t cid);

  // 执行所有尝试插入的 das task， fetch冲突行的主表主键
  int fetch_conflict_rowkey();
  int get_next_conflict_rowkey(DASTaskIter &task_iter);

  virtual void destroy()
  {
    // destroy
    conflict_checker_.destroy();
    insert_up_rtdefs_.release_array();
    insert_up_row_store_.~ObChunkDatumStore();
    upd_rtctx_.cleanup();
    ObTableModifyOp::destroy();
  }

protected:

  // 物化所有要被replace into的行到replace_row_store_
  int load_batch_insert_up_rows(bool &is_iter_end, int64_t &insert_rows);

  int get_next_row_from_child();

  int do_insert_up();

  // 检查是否有duplicated key 错误发生
  bool check_is_duplicated();

  // 遍历replace_row_store_中的所有行，并且更新冲突map，
  int do_insert_up_cache();

  int set_heap_table_new_pk(const ObUpdCtDef &upd_ctdef,
                            ObUpdRtDef &upd_rtdef);

  // 遍历主表的hash map，确定最后提交delete + update + insert das_tasks
  // 提交顺序上必须先提交delete
  int prepare_final_insert_up_task();

  int lock_one_row_to_das(const ObUpdCtDef &upd_ctdef,
                          ObUpdRtDef &upd_rtdef,
                          const ObDASTabletLoc *tablet_loc);

  int insert_row_to_das();

  int delete_upd_old_row_to_das();

  int insert_upd_new_row_to_das();

  int delete_one_upd_old_row_das(const ObUpdCtDef &upd_ctdef,
                                 ObUpdRtDef &upd_rtdef,
                                 const ObDASTabletLoc *tablet_loc);

  int insert_one_upd_new_row_das(const ObUpdCtDef &upd_ctdef,
                                 ObUpdRtDef &upd_rtdef,
                                 const ObDASTabletLoc *tablet_loc);

  int insert_row_to_das(const ObInsCtDef &ins_ctdef,
                        ObInsRtDef &ins_rtdef,
                        const ObDASTabletLoc *tablet_loc);

  int insert_row_to_das(const ObInsCtDef &ins_ctdef,
                                         ObInsRtDef &ins_rtdef,
                                         const ObDASTabletLoc *tablet_loc,
                                         ObDMLModifyRowNode &modify_row);

  int calc_update_tablet_loc(const ObUpdCtDef &upd_ctdef,
                             ObUpdRtDef &upd_rtdef,
                             ObDASTabletLoc *&old_tablet_loc,
                             ObDASTabletLoc *&new_tablet_loc);

  int calc_update_multi_tablet_id(const ObUpdCtDef &upd_ctdef,
                                  ObExpr &part_id_expr,
                                  common::ObTabletID &tablet_id);

  // 其实这里就是计算update 的old_row 的pkey
  int calc_upd_old_row_tablet_loc(const ObUpdCtDef &upd_ctdef,
                                  ObUpdRtDef &upd_rtdef,
                                  ObDASTabletLoc *&tablet_loc);

  int calc_upd_new_row_tablet_loc(const ObUpdCtDef &upd_ctdef,
                                  ObUpdRtDef &upd_rtdef,
                                  ObDASTabletLoc *&tablet_loc);

  // TODO @kaizhan.dkz 这个函数后续被删除
  int calc_insert_tablet_loc(const ObInsCtDef &ins_ctdef,
                             ObInsRtDef &ins_rtdef,
                             ObDASTabletLoc *&tablet_loc);

  int do_update(const ObConflictValue &constraint_value);

  int do_lock(const ObConflictValue &constraint_value);

  int do_insert(const ObConflictValue &constraint_value);

  int do_update_with_ignore();

  // 提交当前所有的 das task;
  int post_all_dml_das_task(ObDMLRtCtx &das_ctx, bool del_task_ahead);

  // batch的执行插入 process_row and then write to das,
  int try_insert_row();

  // batch的执行插入 process_row and then write to das,
  int update_row_to_das(bool need_do_trigger);

  int inner_open_with_das();

  int init_insert_up_rtdef();

  int deal_hint_part_selection(ObObjectID partition_id);
  virtual int check_need_exec_single_row() override;

private:
  int check_insert_up_ctdefs_valid() const;

  const ObIArray<ObExpr *> &get_primary_table_insert_row();

  const ObIArray<ObExpr *> &get_primary_table_columns();

  const ObIArray<ObExpr *> &get_primary_table_upd_new_row();

  const ObIArray<ObExpr *> &get_primary_table_upd_old_row();

  int reset_das_env();

  void add_need_conflict_result_flag();

  int reuse();

protected:
  const static int64_t OB_DEFAULT_INSERT_UP_MEMORY_LIMIT = 2 * 1024 * 1024L;  // 2M in default
  int64_t insert_rows_;
  int64_t upd_changed_rows_;
  int64_t found_rows_;
  ObDMLRtCtx upd_rtctx_;
  ObConflictChecker conflict_checker_;
  common::ObArrayWrap<ObInsertUpRtDef> insert_up_rtdefs_;
  ObChunkDatumStore insert_up_row_store_; //所有的insert_up的行的集合
  bool is_ignore_; // 暂时记录一下是否是ignore的insert_up SQL语句
};
} // end namespace sql
} // end namespace oceanbase
#endif // OCEANBASE_DML_OB_TABLE_INSERT_UP_OP_H_
