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

#ifndef DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_
#define DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_
#include "sql/engine/dml/ob_dml_ctx_define.h"
#include "sql/das/ob_das_context.h"
#include "ob_table_modify_op.h"
namespace oceanbase
{
namespace sql
{
class ObTableModifyOp;
class ObErrLogService;
class ObDMLService
{
public:
  static int check_row_null(const ObExprPtrIArray &row,
                            ObEvalCtx &eval_ctx,
                            int64_t row_num,
                            const ColContentIArray &column_infos,
                            const ObDASDMLBaseCtDef &das_ctdef,
                            bool is_single_value,
                            ObTableModifyOp &dml_op);
  static int check_column_type(const ExprFixedArray &dml_row,
                               int64_t row_num,
                               const common::ObIArray<ColumnContent> &column_infos,
                               ObTableModifyOp &dml_op);
  static int check_rowkey_is_null(const ObExprPtrIArray &row,
                                  int64_t rowkey_cnt,
                                  ObEvalCtx &eval_ctx,
                                  bool &is_null);
  static int check_rowkey_whether_distinct(const ObExprPtrIArray &row,
                                           DistinctType distinct_algo,
                                           ObEvalCtx &eval_ctx,
                                           ObExecContext &root_ctx,
                                           ObRowkey &table_rowkey,
                                           SeRowkeyDistCtx *rowkey_dist_ctx,
                                           bool &is_dist);

  static int create_rowkey_check_hashset(int64_t estimate_row,
                                           ObExecContext *root_ctx,
                                           SeRowkeyDistCtx *&rowkey_dist_ctx);

  static int check_lob_column_changed(ObEvalCtx &eval_ctx,
              const ObExpr& old_expr, ObDatum& old_datum,
              const ObExpr& new_expr, ObDatum& new_datum,
              int64_t& result);
  static int check_row_whether_changed(const ObUpdCtDef &upd_ctdef, ObUpdRtDef &upd_rtdef, ObEvalCtx &eval_ctx);
  static int filter_row_for_check_cst(const ExprFixedArray &cst_exprs,
                                      ObEvalCtx &eval_ctx,
                                      bool &filtered);

  static int filter_row_for_view_check(const ExprFixedArray &cst_exprs,
                                      ObEvalCtx &eval_ctx,
                                      bool &filtered);
  static int init_heap_table_pk_for_ins(const ObInsCtDef &ins_ctdef,
                                        ObEvalCtx &eval_ctx);
  static int process_insert_row(const ObInsCtDef &ins_ctdef,
                                ObInsRtDef &ins_rtdef,
                                ObTableModifyOp &dml_op,
                                bool &is_check_cst_violated_ignored);
  static int process_before_stmt_trigger(const ObDMLBaseCtDef &dml_ctdef,
                                         ObDMLBaseRtDef &dml_rtdef,
                                         ObDMLRtCtx &dml_rtctx,
                                         const ObDmlEventType &dml_event);
  static int process_after_stmt_trigger(const ObDMLBaseCtDef &dml_ctdef,
                                        ObDMLBaseRtDef &dml_rtdef,
                                        ObDMLRtCtx &dml_rtctx,
                                        const ObDmlEventType &dml_event);
  static int process_delete_row(const ObDelCtDef &del_ctdef,
                                ObDelRtDef &del_rtdef,
                                bool &is_skipped,
                                ObTableModifyOp &dml_op);
  static int process_lock_row(const ObLockCtDef &lock_ctdef,
                                ObLockRtDef &lock_rtdef,
                                bool &is_skipped,
                                ObTableModifyOp &dml_op);
  static int process_update_row(const ObUpdCtDef &upd_ctdef,
                                ObUpdRtDef &upd_rtdef,
                                bool &is_skipped,
                                ObTableModifyOp &dml_op);
  static int insert_row(const ObInsCtDef &ins_ctdef,
                        ObInsRtDef &ins_rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        ObChunkDatumStore::StoredRow *&stored_row);
  static int insert_row(const ObDASInsCtDef &ins_ctdef,
                        ObDASInsRtDef &ins_rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &das_rtctx,
                        const ExprFixedArray &new_row,
                        ObChunkDatumStore::StoredRow *&stored_row);
  static int delete_row(const ObDelCtDef &del_ctdef,
                        ObDelRtDef &del_rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        ObChunkDatumStore::StoredRow *&stored_row);
  static int update_row(const ObDASUpdCtDef &ctdef,
                        ObDASUpdRtDef &rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        const ExprFixedArray &full_row);
  static int update_row(const ObUpdCtDef &upd_ctdef,
                        ObUpdRtDef &upd_rtdef,
                        const ObDASTabletLoc *old_tablet_loc,
                        const ObDASTabletLoc *new_tablet_loc,
                        ObDMLRtCtx &dml_rtctx,
                        ObChunkDatumStore::StoredRow *&old_row,
                        ObChunkDatumStore::StoredRow *&new_row,
                        ObChunkDatumStore::StoredRow *&full_row);

  static int delete_row(const ObDASDelCtDef &ctdef,
                        ObDASDelRtDef &rtdef,
                        const ObDASTabletLoc *tablet_loc,
                        ObDMLRtCtx &das_rtctx,
                        const ExprFixedArray &old_row,
                        ObChunkDatumStore::StoredRow *&stored_row);

  static int lock_row(const ObDASLockCtDef &dlock_ctdef,
                      ObDASLockRtDef &dlock_rtdef,
                      const ObDASTabletLoc *tablet_loc,
                      ObDMLRtCtx &das_rtctx,
                      const ExprFixedArray &old_row);

  static int lock_row(const ObLockCtDef &lock_ctdef,
                      ObLockRtDef &lock_rtdef,
                      const ObDASTabletLoc *tablet_loc,
                      ObDMLRtCtx &dml_rtctx);

  static int init_dml_param(const ObDASDMLBaseCtDef &base_ctdef,
                            ObDASDMLBaseRtDef &base_rtdef,
                            transaction::ObTxReadSnapshot &snapshot,
                            common::ObIAllocator &das_alloc,
                            storage::ObDMLBaseParam &dml_param);
  static int init_das_dml_rtdef(ObDMLRtCtx &dml_rtctx,
                                const ObDASDMLBaseCtDef &das_ctdef,
                                ObDASDMLBaseRtDef &das_rtdef,
                                const ObDASTableLocMeta *loc_meta);
  static int init_related_das_rtdef(ObDMLRtCtx &dml_rtctx,
                                    const DASDMLCtDefArray &das_ctdefs,
                                    DASDMLRtDefArray &das_rtdefs);
  static int init_ins_rtdef(ObDMLRtCtx &dml_rtctx,
                            ObInsRtDef &ins_rtdef,
                            const ObInsCtDef &ins_ctdef,
                            ObIArray<ObExpr*> &clear_exprs,
                            ObIArray<ObForeignKeyChecker*> &fk_checkers);
  static int init_trigger_for_insert(ObDMLRtCtx &dml_rtctx,
                                     const ObInsCtDef &ins_ctdef,
                                     ObInsRtDef &ins_rtdef,
                                     ObIArray<ObExpr*> &clear_exprs);
  static int init_del_rtdef(ObDMLRtCtx &dml_rtctx, ObDelRtDef &del_rtdef, const ObDelCtDef &del_ctdef);
  static int init_trigger_for_delete(ObDMLRtCtx &dml_rtctx, const ObDelCtDef &del_ctdef, ObDelRtDef &del_rtdef);
  static int init_upd_rtdef(ObDMLRtCtx &dml_rtctx,
                            ObUpdRtDef &upd_rtdef,
                            const ObUpdCtDef &upd_ctdef,
                            ObIArray<ObExpr*> &clear_exprs,
                            ObIArray<ObForeignKeyChecker*> &fk_checkers);
  static int init_das_ins_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                           const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef);
  static int init_das_del_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                           const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef);
  static int init_das_lock_rtdef_for_update(ObDMLRtCtx &dml_rtctx,
                                           const ObUpdCtDef &upd_ctdef,
                                           ObUpdRtDef &upd_rtdef);
  static int init_trigger_for_update(ObDMLRtCtx &dml_rtctx,
                                      const ObUpdCtDef &upd_ctdef,
                                      ObUpdRtDef &upd_rtdef,
                                      ObTableModifyOp &dml_op,
                                      ObIArray<ObExpr*> &clear_exprs);
  static int init_lock_rtdef(ObDMLRtCtx &dml_rtctx,
                             const ObLockCtDef &lock_ctdef,
                             ObLockRtDef &lock_rtdef,
                             int64_t wait_ts);
  static int convert_exprs_to_stored_row(common::ObIAllocator &allocator,
                                         ObEvalCtx &eval_ctx,
                                         const ObExprPtrIArray &exprs,
                                         ObChunkDatumStore::StoredRow *&new_row);
  static int catch_violate_error(int err_ret,
                                 transaction::ObTxSEQ savepoint_no,
                                 ObDMLRtCtx &dml_rtctx,
                                 ObErrLogRtDef &err_log_rt_def,
                                 ObErrLogCtDef &error_logging_ctdef,
                                 ObErrLogService &err_log_service,
                                 ObDASOpType type);

  static int write_one_row_post_proc(ObDMLRtCtx &dml_rtctx);

  static int set_update_hidden_pk(ObEvalCtx &eval_ctx,
                                  const ObUpdCtDef &upd_ctdef,
                                  const common::ObTabletID &tablet_id);

  static int copy_heap_table_hidden_pk(ObEvalCtx &eval_ctx,
                                       const ObUpdCtDef &upd_ctdef);

  static int get_heap_table_hidden_pk(uint64_t tenant_id,
                                      const common::ObTabletID &tablet_id,
                                      uint64_t &pk);

  static int set_heap_table_hidden_pk(const ObInsCtDef &ins_ctdef,
                                      const common::ObTabletID &tablet_id,
                                      ObEvalCtx &eval_ctx);
  static int create_anonymous_savepoint(transaction::ObTxDesc &tx_desc, transaction::ObTxSEQ &savepoint);
  static int rollback_local_savepoint(transaction::ObTxDesc &tx_desc,
                                      const transaction::ObTxSEQ savepoint,
                                      int64_t expire_ts);
  static int check_local_index_affected_rows(int64_t table_affected_rows,
                                             int64_t index_affected_rows,
                                             const ObDASDMLBaseCtDef &ctdef,
                                             ObDASDMLBaseRtDef &rtdef,
                                             const ObDASDMLBaseCtDef &related_ctdef,
                                             ObDASDMLBaseRtDef &related_rtdef);
  static int check_dml_tablet_validity(ObDMLRtCtx &dml_rtctx,
                                       const ObDASTabletLoc &tablet_loc,
                                       const ExprFixedArray &row,
                                       const ObDMLBaseCtDef &dml_ctdef,
                                       ObDMLBaseRtDef &dml_rtdef);
  static bool is_nested_dup_table(const uint64_t table_id,DASDelCtxList& del_ctx_list);
  static int get_nested_dup_table_ctx(const uint64_t table_id,
                                      DASDelCtxList& del_ctx_list,
                                      SeRowkeyDistCtx *&rowkey_dist_ctx);
  static int handle_after_processing_single_row(ObDMLModifyRowsList *dml_modify_rows);
  static int handle_after_processing_multi_row(ObDMLModifyRowsList *dml_modify_rows, ObTableModifyOp *op);
  // static int handle_after_processing_batch(const ObTableModifyOp *op,
  //                                           ObDMLModifyRowsList *dml_modify_rows);
  static int handle_after_row_processing(ObTableModifyOp *op, ObDMLModifyRowsList *dml_modify_rows);
  static int init_ob_rowkey( ObIAllocator &allocator, const int64_t rowkey_cnt, ObRowkey &table_rowkey);
  static int add_trans_info_datum(ObExpr *trans_info_expr,
                                  ObEvalCtx &eval_ctx,
                                  ObChunkDatumStore::StoredRow *stored_row);
  static int init_fk_checker_array(ObDMLRtCtx &dml_rtctx,
                                   const ObDMLBaseCtDef &dml_ctdef,
                                   FkCheckerArray &fk_checker_array);
  static int log_user_error_inner(int ret,
                                  int64_t row_num,
                                  common::ObString &column_name,
                                  ObExecContext &ctx);

private:
  template <int N>
  static int write_row_to_das_op(const ObDASDMLBaseCtDef &ctdef,
                                 ObDASDMLBaseRtDef &rtdef,
                                 const ObDASTabletLoc *tablet_loc,
                                 ObDMLRtCtx &dml_rtctx,
                                 const ExprFixedArray &row,
                                 ObExpr *trans_info_expr, // debug info for 4377 defensive check
                                 ObChunkDatumStore::StoredRow *&stored_row);
  template <typename T>
  static const ObDASTableLocMeta *get_table_loc_meta(const T *multi_ctdef);
  static int check_nested_sql_legality(ObExecContext &ctx, common::ObTableID ref_table_id);
  static int add_related_index_info(const ObDASTabletLoc &tablet_loc,
                                    const DASDMLCtDefArray &related_ctdefs,
                                    DASDMLRtDefArray &related_rtdefs,
                                    ObIDASTaskOp &das_op);
  static int convert_exprs_to_row(const ExprFixedArray &exprs,
                                  ObEvalCtx &eval_ctx,
                                  ObDMLBaseRtDef &dml_rtdef,
                                  common::ObIAllocator &allocator);
  static int build_batch_fk_check_tasks(const ObDMLBaseCtDef &dml_ctdef,
                                        ObDMLBaseRtDef &dml_rtdef);
};

template <int N, typename DMLIterator>
class ObDASIndexDMLAdaptor
{
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASCtDef CtDefType;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASRtDef RtDefType;
  typedef typename das_reg::ObDASOpTypeTraits<N>::DASOp OpType;
public:
  ObDASIndexDMLAdaptor()
    : tx_desc_(nullptr),
      snapshot_(nullptr),
      ctdef_(nullptr),
      rtdef_(nullptr),
      related_ctdefs_(nullptr),
      related_rtdefs_(nullptr),
      tablet_id_(),
      ls_id_(),
      related_tablet_ids_(nullptr),
      das_allocator_(nullptr),
      dml_param_()
  { }
  int write_tablet(DMLIterator &iter, int64_t &affected_rows);
  int write_rows(const share::ObLSID &ls_id,
                 const common::ObTabletID &tablet_id,
                 const CtDefType &ctdef,
                 RtDefType &rtdef,
                 DMLIterator &iter,
                 int64_t &affected_rows);
  int write_tablet_with_ignore(DMLIterator &iter, int64_t &affected_rows);
public:
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  const CtDefType *ctdef_;
  RtDefType *rtdef_;
  const DASCtDefFixedArray *related_ctdefs_;
  DASRtDefFixedArray *related_rtdefs_;
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  ObTabletIDFixedArray *related_tablet_ids_;
  common::ObIAllocator *das_allocator_;
private:
  storage::ObDMLBaseParam dml_param_;
};

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::write_tablet(DMLIterator &iter, int64_t &affected_rows)
{
  int ret = common::OB_SUCCESS;
  affected_rows = 0;
  SQL_DAS_LOG(TRACE, "begin to write the main tablet",
              K(ls_id_), K(tablet_id_), K(ctdef_->table_id_), K(ctdef_->index_tid_));
  if (ctdef_->is_ignore_) {
    if (OB_FAIL(write_tablet_with_ignore(iter, affected_rows))) {
      LOG_WARN("write tablet with ignore failed", K(ret));
    }
  } else if (OB_FAIL(ObDMLService::init_dml_param(*ctdef_, *rtdef_, *snapshot_, *das_allocator_, dml_param_))) {
    SQL_DAS_LOG(WARN, "init dml param failed", K(ret), K(ctdef_->table_id_), K(ctdef_->index_tid_));
  } else if (OB_FAIL(write_rows(ls_id_, tablet_id_, *ctdef_, *rtdef_, iter, affected_rows))) {
    SQL_DAS_LOG(WARN, "write rows failed", K(ret),
                K(ls_id_), K(tablet_id_), K(ctdef_->table_id_), K(ctdef_->index_tid_));
  } else if (related_ctdefs_ != nullptr && !related_ctdefs_->empty()) {
    //write local index
    for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_->count(); ++i) {
      const CtDefType *related_ctdef = static_cast<const CtDefType*>(related_ctdefs_->at(i));
      RtDefType *related_rtdef = static_cast<RtDefType*>(related_rtdefs_->at(i));
      ObTabletID related_tablet_id = related_tablet_ids_->at(i);
      int64_t index_affected_rows = 0;
      SQL_DAS_LOG(TRACE, "rewind iterator and write local index tablet",
                  K(ls_id_), K(related_tablet_id), K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
      if (OB_FAIL(iter.rewind(related_ctdef))) {
        SQL_DAS_LOG(WARN, "rewind iterator failed", K(ret));
      } else if (OB_FAIL(ObDMLService::init_dml_param(*related_ctdef, *related_rtdef, *snapshot_, *das_allocator_, dml_param_))) {
        SQL_DAS_LOG(WARN, "init index dml param failed", K(ret),
                    K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
      } else if (OB_FAIL(write_rows(ls_id_,
                                    related_tablet_id,
                                    *related_ctdef,
                                    *related_rtdef,
                                    iter,
                                    index_affected_rows))) {
        SQL_DAS_LOG(WARN, "write local index rows failed", K(ret),
                 K(related_tablet_id), K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
      } else if (OB_FAIL(ObDMLService::check_local_index_affected_rows(affected_rows,
                                                                       index_affected_rows,
                                                                       *ctdef_,
                                                                       *rtdef_,
                                                                       *related_ctdef,
                                                                       *related_rtdef))) {
        SQL_DAS_LOG(WARN, "check local index affected rows failed", K(ret));
      }
    }
  }
  return ret;
}

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::write_tablet_with_ignore(DMLIterator &iter,
                                                                   int64_t &affected_rows)
{
  int ret = common::OB_SUCCESS;
  affected_rows = 0;
  const ObDASWriteBuffer::DmlRow *dml_row = nullptr;
  ObDASWriteBuffer::Iterator write_iter;
  if (OB_FAIL(iter.get_write_buffer().begin(write_iter))) {
    LOG_WARN("begin write iterator failed", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(write_iter.get_next_row(dml_row))) {
    transaction::ObTxSEQ savepoint_no;
    int64_t table_affected_rows = 0;
    ObDASWriteBuffer single_row_buffer;
    ObDASWriteBuffer::DmlShadowRow dsr;
    bool added = false;
    //这里后续要删掉，临时处理
    ObChunkDatumStore::StoredRow *store_row = nullptr;
    dsr.store_row_ = const_cast<ObDASWriteBuffer::DmlRow*>(dml_row);
    if (OB_FAIL(ObDMLService::create_anonymous_savepoint(*tx_desc_, savepoint_no))) {
      SQL_DAS_LOG(WARN, "create anonymous savepoint failed", K(ret));
    } else if (OB_FAIL(single_row_buffer.init(*das_allocator_, ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE, MTL_ID()))) {
      SQL_DAS_LOG(WARN, "init single row buffer failed", K(ret));
    } else if (OB_FAIL(single_row_buffer.try_add_row(dsr, das::OB_DAS_MAX_PACKET_SIZE, added, &store_row))) {
      SQL_DAS_LOG(WARN, "try add row to single row buffer failed", K(ret));
    } else if (!added) {
      ret = OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "add row to single row buffer failed", K(ret));
    } else {
      SQL_DAS_LOG(TRACE, "write table dml row with ignore", KPC(dml_row), K(ls_id_), K(tablet_id_),
                  K(ctdef_->table_id_), K(ctdef_->index_tid_));
      DMLIterator single_row_iter(ctdef_, single_row_buffer, *das_allocator_);
      if (OB_FAIL(ObDMLService::init_dml_param(*ctdef_, *rtdef_, *snapshot_, *das_allocator_, dml_param_))) {
        SQL_DAS_LOG(WARN, "init dml param failed", K(ret), KPC_(ctdef), KPC_(rtdef));
      } else if (OB_FAIL(write_rows(ls_id_,
                                    tablet_id_,
                                    *ctdef_,
                                    *rtdef_,
                                    single_row_iter,
                                    table_affected_rows))) {
        SQL_DAS_LOG(WARN, "write rows failed", K(ret), K(ls_id_), K(tablet_id_), KPC(ctdef_), KPC(rtdef_));
      } else if (related_ctdefs_ != nullptr && !related_ctdefs_->empty()) {
        //write local index
        for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_->count(); ++i) {
          const CtDefType *related_ctdef = static_cast<const CtDefType*>(related_ctdefs_->at(i));
          RtDefType *related_rtdef = static_cast<RtDefType*>(related_rtdefs_->at(i));
          ObTabletID related_tablet_id = related_tablet_ids_->at(i);
          int64_t index_affected_rows = 0;
          SQL_DAS_LOG(TRACE, "rewind and write index dml row with ignore", KPC(dml_row),
                      K(ls_id_), K(related_tablet_id),
                      K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
          if (OB_FAIL(single_row_iter.rewind(related_ctdef))) {
            SQL_DAS_LOG(WARN, "rewind iterator failed", K(ret));
          } else if (OB_FAIL(ObDMLService::init_dml_param(*related_ctdef,
                                                          *related_rtdef,
                                                          *snapshot_,
                                                          *das_allocator_,
                                                          dml_param_))) {
            SQL_DAS_LOG(WARN, "init index dml param failed", K(ret),
                        KPC(related_ctdef), KPC(related_rtdef));
          } else if (OB_FAIL(write_rows(ls_id_,
                                        related_tablet_id,
                                        *related_ctdef,
                                        *related_rtdef,
                                        single_row_iter,
                                        index_affected_rows))) {
            SQL_DAS_LOG(WARN, "write local index rows failed", K(ret),
                     K(related_tablet_id), K(related_ctdef->table_id_), K(related_ctdef->index_tid_));
          } else if (OB_FAIL(ObDMLService::check_local_index_affected_rows(table_affected_rows,
                                                                           index_affected_rows,
                                                                           *ctdef_,
                                                                           *rtdef_,
                                                                           *related_ctdef,
                                                                           *related_rtdef))) {
             SQL_DAS_LOG(WARN, "check local index affected rows failed", K(ret));
          }
        }
      }
    }
    if (common::OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      //rollback to savepoint
      if (OB_FAIL(ObDMLService::rollback_local_savepoint(*tx_desc_,
                                                         savepoint_no,
                                                         dml_param_.timeout_))) {
        SQL_DAS_LOG(WARN, "rollbakc to local savepoint failed", K(ret));
      }
    } else if (OB_SUCC(ret)) {
      ++affected_rows;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

template <int N, typename DMLIterator>
int ObDASIndexDMLAdaptor<N, DMLIterator>::write_rows(const share::ObLSID &ls_id,
                                                     const common::ObTabletID &tablet_id,
                                                     const CtDefType &ctdef,
                                                     RtDefType &rtdef,
                                                     DMLIterator &iter,
                                                     int64_t &affected_rows)
{
  UNUSEDx(ls_id, tablet_id, ctdef, rtdef, iter, affected_rows);
  return common::OB_NOT_IMPLEMENT;
}
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_DML_OB_DML_SERVICE_H_ */
