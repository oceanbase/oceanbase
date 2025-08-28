/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_DAS_TEXT_RETRIEVAL_ITER_H_
#define OB_DAS_TEXT_RETRIEVAL_ITER_H_

#include "ob_das_iter.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_ls_id.h"
#include "src/storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace transaction
{
class ObTxDesc;
class ObTxReadSnapshot;
}
namespace sql
{
class ObDASScanRtDef;
class ObDASScanCtDef;
class ObDASScanIter;
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;

struct ObDASTextRetrievalIterParam : public ObDASIterParam
{
public:
  ObDASTextRetrievalIterParam()
    : ObDASIterParam(DAS_ITER_TEXT_RETRIEVAL),
      ir_ctdef_(nullptr),
      ir_rtdef_(nullptr),
      inv_idx_scan_iter_(nullptr),
      inv_idx_agg_iter_(nullptr),
      fwd_idx_iter_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr),
      need_inv_idx_agg_reset_(true)
  {}

  virtual bool is_valid() const override
  {
    return nullptr != ir_ctdef_ && nullptr != ir_rtdef_ && nullptr != inv_idx_scan_iter_;
  }

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObDASIter *inv_idx_scan_iter_;
  ObDASIter *inv_idx_agg_iter_;
  ObDASIter *fwd_idx_iter_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  bool need_inv_idx_agg_reset_;
};

// single token
class ObDASTextRetrievalIter : public ObDASIter
{
public:
  ObDASTextRetrievalIter();
  virtual ~ObDASTextRetrievalIter() {}
  virtual int do_table_scan() override;
  virtual int rescan() override;

  int set_query_token(const ObString &query_token);
  int set_query_token_and_rangekey(const ObString &query_token, const common::ObIArray<ObDocId> &doc_id, const int64_t &batch_size);
  void set_ls_tablet_ids(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_tablet_id,
      const ObTabletID &fwd_tablet_id)
  {
    ls_id_ = ls_id;
    inv_idx_tablet_id_ = inv_tablet_id;
    fwd_idx_tablet_id_ = fwd_tablet_id;
  }
  INHERIT_TO_STRING_KV("ObDASIter", ObDASIter, K_(calc_exprs), K_(need_fwd_idx_agg),
      K_(need_inv_idx_agg), K_(inv_idx_agg_evaluated), K_(not_first_fwd_agg), K_(is_inited));
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
protected:
  int init_inv_idx_scan_param();
  int init_fwd_idx_scan_param();
  static int init_base_idx_scan_param(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const sql::ObDASScanCtDef *ctdef,
      ObDASScanRtDef *rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      storage::ObTableScanParam &scan_param);
  int get_next_doc_token_cnt(const bool use_fwd_idx_agg);
  int do_doc_cnt_agg();
  int do_token_cnt_agg(const ObDocId &doc_id, int64_t &token_count);
  int get_inv_idx_scan_doc_id(ObDocId &doc_id);
  int get_next_row_inner();
  int fill_token_cnt_with_doc_len();
  int batch_fill_token_cnt_with_doc_len(const int64_t &count);
  int fill_token_doc_cnt();
  int project_relevance_expr();
  int batch_project_relevance_expr(const int64_t &count);
  int reuse_fwd_idx_iter();
  int gen_default_inv_idx_scan_range(const ObString &query_token, ObNewRange &scan_range);
  int gen_inv_idx_scan_range(const ObString &query_token, const ObDocId &doc_id, ObNewRange &scan_range);

  int gen_fwd_idx_scan_range(const ObDocId &doc_id, ObNewRange &scan_range);
  inline bool need_calc_relevance() { return true; } // TODO: reduce tsc ops if no need to calc relevance
  int init_calc_exprs();
  void clear_row_wise_evaluated_flag();
  void clear_batch_wise_evaluated_flag(const int64_t &count);
  // TODO: delete this after enable standard vectorized execution
  inline int get_next_single_row(const bool is_vectorized, ObNewRowIterator *iter)
  {
    int ret = OB_SUCCESS;
    if (is_vectorized) {
      int64_t scan_row_cnt = 0;
      ret =  iter->get_next_rows(scan_row_cnt, 1);
    } else {
      ret =  iter->get_next_row();
    }
    return ret;
  }

  int add_rowkey_range_key(const ObNewRange &range);
  int add_agg_rang_key(const ObNewRange &range);
  int check_inv_idx_scan_and_agg_param();

  // tools method
  // In ivector2.0, need use the size which is created by precision to alloc the memeory.
  int set_decimal_int_by_precision(ObDatum &result_datum, const uint64_t decint, const ObPrecision precision) const
  {
    int ret = OB_SUCCESS;
    if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected precision, precision is too short", K(ret), K(precision));
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
      const int128_t result = decint;
      result_datum.set_decimal_int(result);
    } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
      const int256_t result = decint;
      result_datum.set_decimal_int(result);
    } else {
      const int512_t result = decint;
      result_datum.set_decimal_int(result);
    }
    return ret;
  }
 
protected:
  static const int64_t FWD_IDX_ROWKEY_COL_CNT = 2;
  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;
protected:
  lib::MemoryContext mem_context_;
  ObArenaAllocator allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  share::ObLSID ls_id_;
  common::ObTabletID inv_idx_tablet_id_;
  common::ObTabletID fwd_idx_tablet_id_;
  storage::ObTableScanParam inv_idx_scan_param_;
  storage::ObTableScanParam inv_idx_agg_param_;
  storage::ObTableScanParam fwd_idx_scan_param_;
  common::ObSEArray<sql::ObExpr *, 2> calc_exprs_;
  ObDASScanIter *inverted_idx_scan_iter_;
  ObDASScanIter *inverted_idx_agg_iter_;
  ObDASScanIter *forward_idx_iter_;
  ObObj *fwd_range_objs_;
  sql::ObExpr *doc_token_cnt_expr_;
  sql::ObBitVector *skip_;
  int64_t token_doc_cnt_;
  int64_t max_batch_size_;
  bool need_fwd_idx_agg_;
  bool need_inv_idx_agg_;
  bool inv_idx_agg_evaluated_;
  bool need_inv_idx_agg_reset_;
  bool not_first_fwd_agg_;
  bool is_inited_;
};

class ObDASTRCacheIter : public ObDASTextRetrievalIter
{
public:
  ObDASTRCacheIter();
  virtual ~ObDASTRCacheIter() {}
  int get_cur_row(double &relevance, ObDocId &doc_id) const;
  INHERIT_TO_STRING_KV("ObDASTextRetrievalIter", ObDASTextRetrievalIter, K_(cur_idx), K_(count),
      K_(relevance), K_(doc_id));
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
private:
  int get_next_batch_inner();
  int save_relevances_and_docids();
  int save_docids();
private:
  int64_t cur_idx_;
  int64_t count_;
  ObFixedArray<double, ObIAllocator> relevance_;
  ObFixedArray<ObDocId, ObIAllocator> doc_id_;
};


} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_TEXT_RETRIEVAL_ITER_H_
