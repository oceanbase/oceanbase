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

#ifndef OB_TEXT_RETRIEVAL_ITERATOR_H_
#define OB_TEXT_RETRIEVAL_ITERATOR_H_

#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_dml_param.h"


namespace oceanbase
{
namespace sql
{
struct ObDASScanCtDef;
struct ObDASScanRtDef;
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;
}
namespace storage
{

struct ObTokenRetrievalParam
{
public:
  ObTokenRetrievalParam()
    : app_avg_tablet_doc_token_cnt_(0),
      ls_id_(),
      inv_idx_tablet_id_(),
      fwd_idx_tablet_id_(),
      doc_id_idx_tablet_id_(),
      ir_ctdef_(nullptr),
      ir_rtdef_(nullptr)
    {}
  ~ObTokenRetrievalParam() {}

  bool need_relevance() const;
  const share::ObLSID &get_ls_id() const;
  const sql::ObDASIRScanCtDef *get_ir_ctdef() const;
  sql::ObDASIRScanRtDef *get_ir_rtdef();
  const sql::ObDASScanCtDef *get_inv_idx_scan_ctdef() const;
  const sql::ObDASScanCtDef *get_inv_idx_agg_ctdef() const;
  const sql::ObDASScanCtDef *get_fwd_idx_agg_ctdef() const;
  const sql::ObDASScanCtDef *get_doc_id_idx_agg_ctdef() const;
  const common::ObTabletID &get_inv_idx_tablet_id() const;
  const common::ObTabletID &get_fwd_idx_tablet_id() const;
  const common::ObTabletID &get_doc_id_idx_tablet_id() const;
  inline void set_param(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_idx_tablet_id,
      const ObTabletID &fwd_idx_tablet_id,
      const ObTabletID &doc_id_idx_tablet_id,
      const sql::ObDASIRScanCtDef *ir_ctdef,
      sql::ObDASIRScanRtDef *ir_rtdef,
      const int64_t approx_avg_token_cnt = 0)
  {
    ls_id_ = ls_id;
    inv_idx_tablet_id_ = inv_idx_tablet_id;
    fwd_idx_tablet_id_ = fwd_idx_tablet_id;
    doc_id_idx_tablet_id_ = doc_id_idx_tablet_id;
    ir_ctdef_ = ir_ctdef;
    ir_rtdef_ = ir_rtdef;
  }

  TO_STRING_KV(K_(app_avg_tablet_doc_token_cnt),
      K_(ls_id),
      K_(inv_idx_tablet_id),
      K_(fwd_idx_tablet_id),
      K_(doc_id_idx_tablet_id));
private:
  int64_t app_avg_tablet_doc_token_cnt_; // TODO: use app avg tablet doc token cnt to calc bm25 idf
  share::ObLSID ls_id_;
  common::ObTabletID inv_idx_tablet_id_;
  common::ObTabletID fwd_idx_tablet_id_;
  common::ObTabletID doc_id_idx_tablet_id_;
  const sql::ObDASIRScanCtDef *ir_ctdef_;
  sql::ObDASIRScanRtDef *ir_rtdef_;
};

// Single token retrieval iter
class ObTextRetrievalIterator : public common::ObNewRowIterator
{
public:
  ObTextRetrievalIterator();
  virtual ~ObTextRetrievalIterator();

  int init(
      ObTokenRetrievalParam &retrieval_param,
      const ObString &query_token,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot);

  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;

  int get_curr_iter_row(const sql::ExprFixedArray *&curr_row, sql::ObEvalCtx *&curr_eval_ctx);

  int get_curr_doc_id();
  int forward_to_doc(const common::ObDocId &doc_id); // TODO: impl this primitive for skipping scan in conjunctive processing

  TO_STRING_KV(KPC_(retrieval_param), K_(is_inited));
private:
  int init_inv_idx_scan_param(const ObString &query_token);
  int init_fwd_idx_scan_param();
  static int init_base_idx_scan_param(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const sql::ObDASScanCtDef *ctdef,
      sql::ObDASScanRtDef *rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      ObTableScanParam &scan_param);
  int get_next_doc_token_cnt(const bool use_fwd_idx_agg);
  int do_doc_cnt_agg();
  int do_token_cnt_agg(const ObDocId &doc_id, int64_t &token_count);
  int get_inv_idx_scan_doc_id(ObDocId &doc_id);
  int fill_token_cnt_with_doc_len();
  int fill_token_doc_cnt();
  int project_relevance_expr();
  int reuse_fwd_idx_iter();
  int gen_inv_idx_scan_range(const ObString &query_token, ObNewRange &scan_range);
  int gen_fwd_idx_scan_range(const ObDocId &doc_id, ObNewRange &scan_range);
  inline bool need_calc_relevance() { return true; } // TODO: reduce tsc ops if no need to calc relevance
  int init_calc_exprs();
  void clear_row_wise_evaluated_flag();

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
private:
  static const int64_t FWD_IDX_ROWKEY_COL_CNT = 2;
  static const int64_t INV_IDX_ROWKEY_COL_CNT = 2;
private:
  lib::MemoryContext mem_context_;
  ObTokenRetrievalParam *retrieval_param_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObTableScanParam inv_idx_scan_param_;
  ObTableScanParam inv_idx_agg_param_;
  ObTableScanParam fwd_idx_scan_param_;
  common::ObSEArray<sql::ObExpr *, 2> calc_exprs_;
  common::ObNewRowIterator *inverted_idx_iter_;
  common::ObNewRowIterator *forward_idx_iter_;
  ObObj *fwd_range_objs_;
  sql::ObExpr *doc_token_cnt_expr_;
  int64_t token_doc_cnt_;
  bool need_fwd_idx_agg_;
  bool need_inv_idx_agg_;
  bool inv_idx_agg_evaluated_;
  bool is_inited_;
};

}
}


#endif //OB_TEXT_RETRIEVAL_ITERATOR_H_
