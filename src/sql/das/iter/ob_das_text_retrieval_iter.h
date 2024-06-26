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

namespace oceanbase
{
namespace sql
{
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
      snapshot_(nullptr)
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
private:
  int init_inv_idx_scan_param();
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
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  share::ObLSID ls_id_;
  common::ObTabletID inv_idx_tablet_id_;
  common::ObTabletID fwd_idx_tablet_id_;
  ObTableScanParam inv_idx_scan_param_;
  ObTableScanParam inv_idx_agg_param_;
  ObTableScanParam fwd_idx_scan_param_;
  common::ObSEArray<sql::ObExpr *, 2> calc_exprs_;
  ObDASScanIter *inverted_idx_scan_iter_;
  ObDASScanIter *inverted_idx_agg_iter_;
  ObDASScanIter *forward_idx_iter_;
  ObObj *fwd_range_objs_;
  sql::ObExpr *doc_token_cnt_expr_;
  int64_t token_doc_cnt_;
  bool need_fwd_idx_agg_;
  bool need_inv_idx_agg_;
  bool inv_idx_agg_evaluated_;
  bool not_first_fwd_agg_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_TEXT_RETRIEVAL_ITER_H_
