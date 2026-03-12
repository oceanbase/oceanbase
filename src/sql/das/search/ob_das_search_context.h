/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */


/*
 * Overview
 * - Runtime Context for DAS hybrid search, no need to serialize/deserialize.
 *
 * Key Responsibilities
 * - Share query-level resources: allocator, ls id, transaction/snapshot, attached das task.
 * - Store rowid/score/output expression metadata used by search operators.
 * - Provide utilities: get related tablet id and initialize table-scan parameters, etc.
 * - Manage search operators: creation, cleanup and id assignment.
*/

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_CONTEXT_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_CONTEXT_H_

#include "sql/das/ob_das_scan_op.h"
#include "sql/das/search/ob_das_search_define.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "storage/retrieval/ob_inv_idx_param_cache.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace sql
{

class ObIDASSearchOp;
class RowIDStore;
struct ObDASScalarScanCtDef;
struct ObDASScalarScanRtDef;

class ObDASSearchCtx
{

friend class ObIDASSearchOp;
friend class ObDASSearchDriverIterParam;
friend class ObDASSearchDriverIter;
friend class RowIDStore;
friend class UInt64RowIDStore;
friend class CompactRowIDStore;

public:
  explicit ObDASSearchCtx(common::ObIAllocator &allocator, ObDASScanOp &op)
    : allocator_(allocator),
      ls_id_(op.get_ls_id()),
      tx_desc_(op.get_trans_desc()),
      snapshot_(op.get_snapshot()),
      root_das_task_(op),
      eval_ctx_(nullptr),
      rowid_meta_(),
      rowid_exprs_(nullptr),
      output_(nullptr),
      op_count_(0),
      mock_skip_(nullptr),
      bm25_param_cache_(),
      use_dynamic_pruning_(true),
      rowid_type_(DAS_ROWID_TYPE_UINT64),
      table_row_count_()
  { }

  ~ObDASSearchCtx() { reset(); }

  TO_STRING_KV(K(ls_id_), K(tx_desc_), K(snapshot_), KPC(eval_ctx_), KPC(rowid_exprs_), K(op_count_));

public:
  int init(
    const sql::ExprFixedArray &rowid_exprs,
    const sql::ExprFixedArray &output,
    bool use_dynamic_pruning);

  /*
   * Get the related tablet id from the attached das task using scan ctdef.
   * Called by search op when open/rescan.
  */
  int get_related_tablet_id(const ObDASScalarScanCtDef *scalar_ctdef, common::ObTabletID &tablet_id);

  int init_scan_param(common::ObTabletID &tablet_id,
                      const ObDASScalarScanCtDef *ctdef,
                      ObDASScalarScanRtDef *rtdef,
                      ObTableScanParam &scan_param);

  OB_INLINE bool is_valid() const
  { return nullptr != eval_ctx_ && nullptr != rowid_exprs_ && nullptr != output_ && nullptr != mock_skip_ && table_row_count_.is_valid(); }
  OB_INLINE int64_t max_batch_size() const { return eval_ctx_->max_batch_size_; }
  OB_INLINE storage::ObInvIdxParamCache &get_bm25_param_cache() { return bm25_param_cache_; }
  OB_INLINE RowMeta &get_rowid_meta() { return rowid_meta_; }
  OB_INLINE const common::ObIArray<ObExpr *> &get_rowid_exprs() const { return *rowid_exprs_; }
  OB_INLINE common::ObIAllocator &get_allocator() { return allocator_; }
  void reset();

  OB_INLINE ObDASRowIDType get_rowid_type() const { return rowid_type_; }
  OB_INLINE const ObDASSearchCost &get_row_count() const { return table_row_count_; }
  int lower_bound_in_frame(const ObDASRowID &target, const ObIArray<ObExpr *> *rowid_exprs,
                           const int64_t count, int64_t &idx);
  int estimate_row_count(const ObDASScalarScanCtDef *ctdef, const ObDASScalarScanRtDef *rtdef, int64_t &row_count);

  /*
   * RowID utility methods - type-agnostic operations on ObDASRowID.
   * These methods internally dispatch to the correct implementation based on rowid_type_.
   */
  int compare_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2, int &cmp) const
  { return all_rowid_funcs_[rowid_type_].compare_(rowid1, rowid2, rowid_meta_, *rowid_exprs_, cmp); }
  int deep_copy_rowid(const ObDASRowID &src, ObDASRowID &dst, common::ObIAllocator &alloc) const
  { return all_rowid_funcs_[rowid_type_].deep_copy_(src, dst, alloc); }
  int create_rowid_store(int64_t capacity, RowIDStore *&store)
  { return all_rowid_funcs_[rowid_type_].create_store_(*this, capacity, store, allocator_); }
  int rowid_to_expr(const ObDASRowID &rowid, int64_t batch_idx)
  { return all_rowid_funcs_[rowid_type_].to_expr_(rowid, *rowid_exprs_, rowid_meta_, *eval_ctx_, batch_idx); }
  int write_datum_to_rowid(const ObDatum &datum, ObDASRowID &rowid, common::ObIAllocator &alloc) const
  { return all_rowid_funcs_[rowid_type_].write_datum_(datum, rowid_meta_, *rowid_exprs_, rowid, alloc); }
  int get_datum_from_rowid(const ObDASRowID &rowid, ObDatum &datum, int64_t col_idx = 0) const
  { return all_rowid_funcs_[rowid_type_].get_datum_(rowid, rowid_meta_, datum, col_idx); }

public:
  template<class Op, class OpParam>
  int create_op(const OpParam &op_param, Op *&op)
  {
    int ret = OB_SUCCESS;
    Op *result = nullptr;
    if (OB_ISNULL(result = OB_NEWx(Op, &allocator_, *this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate search op", K(ret));
    } else if (OB_FAIL(result->init(op_param))) {
      LOG_WARN("failed to init search op", K(ret));
    } else if (OB_FAIL(op_store_.push_back(result))) {
      result->close();
      LOG_WARN("failed to push back op", K(ret));
    } else {
      op = result;
      op->set_op_id(op_count_++);
    }
    return ret;
  }

private:
  struct RowIDFuncs
  {
    int (*compare_)(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                    const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                    int &cmp);
    int (*deep_copy_)(const ObDASRowID &src, ObDASRowID &dst,
                      common::ObIAllocator &alloc);
    int (*create_store_)(ObDASSearchCtx &ctx, int64_t capacity, RowIDStore *&store,
                         common::ObIAllocator &alloc);
    int (*to_expr_)(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                    const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx);
    int (*write_datum_)(const ObDatum &datum, const RowMeta &meta,
                        const ExprFixedArray &rowid_exprs, ObDASRowID &rowid,
                        common::ObIAllocator &alloc);
    int (*get_datum_)(const ObDASRowID &rowid, const RowMeta &meta, ObDatum &datum,
                      int64_t col_idx);
  };

private:
  common::ObIAllocator &allocator_;
  share::ObLSID ls_id_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObDASScanOp &root_das_task_;
  ObEvalCtx *eval_ctx_;
  RowMeta rowid_meta_;
  const sql::ExprFixedArray *rowid_exprs_;
  const sql::ExprFixedArray *output_;
  int64_t op_count_;
  ObSEArray<ObIDASSearchOp *, 16> op_store_;
  sql::ObBitVector *mock_skip_;
  storage::ObInvIdxParamCache bm25_param_cache_;
  bool use_dynamic_pruning_;
  ObDASRowIDType rowid_type_;
  ObDASSearchCost table_row_count_;

private:
  static int compare_uint64_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                  const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                  int &cmp);
  static int compare_compact_rowid(const ObDASRowID &rowid1, const ObDASRowID &rowid2,
                                   const RowMeta &rowid_meta, const ExprFixedArray &rowid_exprs,
                                   int &cmp);

  static int deep_copy_uint64_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                    common::ObIAllocator &alloc);
  static int deep_copy_compact_rowid(const ObDASRowID &src, ObDASRowID &dst,
                                     common::ObIAllocator &alloc);

  template<typename StoreType>
  static int create_rowid_store(ObDASSearchCtx &ctx, int64_t capacity, RowIDStore *&store,
                                common::ObIAllocator &alloc);

  static int uint64_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                  const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx);
  static int compact_rowid_to_expr(const ObDASRowID &rowid, const ExprFixedArray &rowid_exprs,
                                   const RowMeta &rowid_meta, ObEvalCtx &eval_ctx, int64_t batch_idx);

  static int write_datum_to_uint64_rowid(const ObDatum &datum, const RowMeta &meta,
                                         const ExprFixedArray &rowid_exprs, ObDASRowID &rowid,
                                         common::ObIAllocator &alloc);
  static int write_datum_to_compact_rowid(const ObDatum &datum, const RowMeta &meta,
                                          const ExprFixedArray &rowid_exprs, ObDASRowID &rowid,
                                          common::ObIAllocator &alloc);

  static int get_datum_from_uint64_rowid(const ObDASRowID &rowid, const RowMeta &meta, ObDatum &datum,
                                         int64_t col_idx);
  static int get_datum_from_compact_rowid(const ObDASRowID &rowid, const RowMeta &meta, ObDatum &datum,
                                          int64_t col_idx);

  static const RowIDFuncs all_rowid_funcs_[DAS_ROWID_TYPE_MAX];
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_SEARCH_CONTEXT_H_
