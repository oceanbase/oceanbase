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

#ifndef OBDEV_SRC_SQL_DAS_OB_TEXT_RETRIEVAL_OP_H_
#define OBDEV_SRC_SQL_DAS_OB_TEXT_RETRIEVAL_OP_H_

#include "lib/container/ob_loser_tree.h"
#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_attach_define.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "storage/fts/ob_text_retrieval_iterator.h"

namespace oceanbase
{
namespace sql
{
static const int64_t OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT = 256;

struct ObDASIRScanCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRScanCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_SCAN),
      search_text_(nullptr),
      inv_scan_doc_id_col_(nullptr),
      inv_scan_doc_length_col_(nullptr),
      match_filter_(nullptr),
      relevance_expr_(nullptr),
      relevance_proj_col_(nullptr),
      estimated_total_doc_cnt_(0),
      flags_(0)
  {
  }
  bool need_calc_relevance() const { return nullptr != relevance_expr_; }
  bool need_proj_relevance_score() const { return nullptr != relevance_proj_col_; }
  const ObDASScanCtDef *get_inv_idx_scan_ctdef() const
  {
    const ObDASScanCtDef *idx_scan_ctdef = nullptr;
    if (children_cnt_ > 0 && children_ != nullptr) {
      idx_scan_ctdef = static_cast<const ObDASScanCtDef*>(children_[get_inv_scan_idx()]);
    }
    return idx_scan_ctdef;
  }
  const ObDASScanCtDef *get_inv_idx_agg_ctdef() const
  {
    const ObDASScanCtDef *idx_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_inv_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_INV_IDX_AGG) {
        idx_agg_ctdef = child;
      }
    }
    return idx_agg_ctdef;
  }
  const ObDASScanCtDef *get_doc_id_idx_agg_ctdef() const
  {
    const ObDASScanCtDef *doc_id_idx_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_doc_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_DOC_ID_IDX_AGG) {
        doc_id_idx_agg_ctdef = child;
      }
    }
    return doc_id_idx_agg_ctdef;
  }
  const ObDASScanCtDef *get_fwd_idx_agg_ctdef() const
  {
    const ObDASScanCtDef *fwd_idx_agg_ctdef = nullptr;
    const int64_t ctdef_idx = get_fwd_agg_idx();
    if (children_cnt_ > ctdef_idx && ctdef_idx > 0 && children_ != nullptr) {
      const ObDASScanCtDef *child = static_cast<const ObDASScanCtDef *>(children_[ctdef_idx]);
      if (child->ir_scan_type_ == ObTSCIRScanType::OB_IR_FWD_IDX_AGG) {
        fwd_idx_agg_ctdef = child;
      }
    }
    return fwd_idx_agg_ctdef;
  }
  int64_t get_inv_scan_idx() const { return 0; }
  int64_t get_inv_agg_idx() const { return has_inv_agg_ ? 1 : -1; }
  int64_t get_doc_agg_idx() const { return has_doc_id_agg_ ? (1 + has_inv_agg_) : -1; }
  int64_t get_fwd_agg_idx() const { return has_fwd_agg_ ? (1 + has_inv_agg_ + has_doc_id_agg_) : -1; }
  bool need_do_total_doc_cnt() const { return 0 == estimated_total_doc_cnt_; }

  INHERIT_TO_STRING_KV("ObDASBaseCtDef", ObDASBaseCtDef,
                       K_(flags),
                       KPC_(search_text),
                       KPC_(inv_scan_doc_id_col),
                       KPC_(inv_scan_doc_length_col),
                       KPC_(match_filter),
                       KPC_(relevance_expr),
                       KPC_(relevance_proj_col),
                       K_(estimated_total_doc_cnt));

  ObExpr *search_text_;
  ObExpr *inv_scan_doc_id_col_;
  ObExpr *inv_scan_doc_length_col_;
  ObExpr *match_filter_;
  ObExpr *relevance_expr_;
  ObExpr *relevance_proj_col_;
  int64_t estimated_total_doc_cnt_;
  union
  {
    uint8_t flags_;
    struct
    {
      uint8_t has_inv_agg_:1;
      uint8_t has_doc_id_agg_:1;
      uint8_t has_fwd_agg_:1;
      uint8_t reserved_:5;
    };
  };
};

struct ObDASIRScanRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRScanRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_SCAN) {}

  virtual ~ObDASIRScanRtDef() {}

  ObDASScanRtDef *get_inv_idx_scan_rtdef()
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_inv_scan_idx();
    ObDASScanRtDef *idx_scan_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && children_ != nullptr) {
      idx_scan_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return idx_scan_rtdef;
  }
  ObDASScanRtDef *get_inv_idx_agg_rtdef()
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_inv_agg_idx();
    ObDASScanRtDef *idx_agg_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      idx_agg_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return idx_agg_rtdef;
  }
  ObDASScanRtDef *get_doc_id_idx_agg_rtdef()
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_doc_agg_idx();
    ObDASScanRtDef *doc_id_idx_agg_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      doc_id_idx_agg_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return doc_id_idx_agg_rtdef;
  }
  ObDASScanRtDef *get_fwd_idx_agg_rtdef() const
  {
    const ObDASIRScanCtDef *ctdef = static_cast<const ObDASIRScanCtDef *>(ctdef_);
    const int64_t rtdef_idx = ctdef->get_fwd_agg_idx();
    ObDASScanRtDef *fwd_idx_agg_rtdef = nullptr;
    if (children_cnt_ > rtdef_idx && rtdef_idx > 0 && children_ != nullptr) {
      fwd_idx_agg_rtdef = static_cast<ObDASScanRtDef*>(children_[rtdef_idx]);
    }
    return fwd_idx_agg_rtdef;
  }
};

struct ObDASIRAuxLookupCtDef : ObDASAttachCtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRAuxLookupCtDef(common::ObIAllocator &alloc)
    : ObDASAttachCtDef(alloc, DAS_OP_IR_AUX_LOOKUP),
      relevance_proj_col_(nullptr)
  { }

  const ObDASBaseCtDef *get_doc_id_scan_ctdef() const
  {
    OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
    return children_[0];
  }
  const ObDASScanCtDef *get_lookup_scan_ctdef() const
  {
    OB_ASSERT(children_cnt_ == 2 && children_ != nullptr);
    return static_cast<const ObDASScanCtDef*>(children_[1]);
  }

  ObExpr *relevance_proj_col_;
};

struct ObDASIRAuxLookupRtDef : ObDASAttachRtDef
{
  OB_UNIS_VERSION(1);
public:
  ObDASIRAuxLookupRtDef()
    : ObDASAttachRtDef(DAS_OP_IR_AUX_LOOKUP)
  {}

  virtual ~ObDASIRAuxLookupRtDef() {}

  ObDASBaseRtDef *get_doc_id_scan_rtdef()
  {
    OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
    return children_[0];
  }
  ObDASScanRtDef *get_lookup_scan_rtdef()
  {
    OB_ASSERT(children_cnt_ == 2 && children_ != nullptr);
    return static_cast<ObDASScanRtDef*>(children_[1]);
  }
};

struct ObIRIterLoserTreeItem
{
  ObIRIterLoserTreeItem();
  ~ObIRIterLoserTreeItem() = default;

  TO_STRING_KV(K_(iter_idx), K_(relevance), K_(doc_id), K(doc_id_.get_string()));

  double relevance_;
  ObDocId doc_id_;
  int64_t iter_idx_;
};

struct ObIRIterLoserTreeCmp
{
  ObIRIterLoserTreeCmp();
  virtual ~ObIRIterLoserTreeCmp();

  int init();
  int cmp(const ObIRIterLoserTreeItem &l, const ObIRIterLoserTreeItem &r, int64_t &cmp_ret);
private:
  common::ObDatumCmpFuncType cmp_func_;
  bool is_inited_;
};

typedef common::ObLoserTree<ObIRIterLoserTreeItem, ObIRIterLoserTreeCmp, OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT> ObIRIterLoserTree;

class ObTextRetrievalMerge : public common::ObNewRowIterator
{
public:
  enum TokenRelationType
  {
    DISJUNCTIVE = 0,
    // CONJUNCTIVE = 1,
    // BOOLEAN = 2,
    MAX_RELATION_TYPE
  };
  enum RetrievalProcType
  {
    DAAT = 0,
    // TAAT = 1,
    // VAAT = 2,
    MAX_PROC_TYPE
  };
public:
  ObTextRetrievalMerge();
  virtual ~ObTextRetrievalMerge();

  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_idx_tablet_id,
      const ObTabletID &fwd_idx_tablet_id,
      const ObTabletID &doc_id_idx_tablet_id,
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      ObIAllocator &allocator);
  int rescan(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_idx_tablet_id,
      const ObTabletID &fwd_idx_tablet_id,
      const ObTabletID &doc_id_idx_tablet_id,
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      ObIAllocator &allocator);

  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override { ObNewRow *r = nullptr; return get_next_row(r); }
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;
private:
  int init_iter_params(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_idx_tablet_id,
      const ObTabletID &fwd_idx_tablet_id,
      const ObTabletID &doc_id_idx_tablet_id,
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef);
  int init_iters(
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      const ObIArray<ObString> &query_tokens);
  int init_query_tokens(const ObDASIRScanCtDef *ir_ctdef, ObDASIRScanRtDef *ir_rtdef);
  void release_iters();
  int pull_next_batch_rows();
  int fill_loser_tree_item(
      storage::ObTextRetrievalIterator &iter,
      const int64_t iter_idx,
      ObIRIterLoserTreeItem &item);
  int next_disjunctive_document();
  int init_total_doc_cnt_param(transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot *snapshot);
  int do_total_doc_cnt();
  int project_result(const ObIRIterLoserTreeItem &item, const double relevance);
  void clear_evaluated_infos();
private:
  static const int64_t OB_DEFAULT_QUERY_TOKEN_ITER_CNT = 4;
  typedef ObSEArray<storage::ObTextRetrievalIterator *, OB_DEFAULT_QUERY_TOKEN_ITER_CNT> ObTokenRetrievalIterArray;
  TokenRelationType relation_type_;
  RetrievalProcType processing_type_;
  ObIAllocator *allocator_;
  ObTokenRetrievalParam retrieval_param_;
  ObArray<ObString> query_tokens_;
  ObTokenRetrievalIterArray token_iters_;
  ObIRIterLoserTreeCmp loser_tree_cmp_;
  ObIRIterLoserTree *iter_row_heap_;
  ObFixedArray<int64_t, ObIAllocator> next_batch_iter_idxes_;
  int64_t next_batch_cnt_;
  common::ObNewRowIterator *whole_doc_cnt_iter_;
  ObTableScanParam whole_doc_agg_param_;
  bool doc_cnt_calculated_;
  bool is_inited_;
};


class ObTextRetrievalOp : public common::ObNewRowIterator
{
public:
  ObTextRetrievalOp();
  virtual ~ObTextRetrievalOp();

  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_idx_tablet_id,
      const ObTabletID &fwd_idx_tablet_id,
      const ObTabletID &doc_id_idx_tablet_id,
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      const ObDASSortCtDef *sort_ctdef,
      ObDASSortRtDef *sort_rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot);
  int rescan(
      const share::ObLSID &ls_id,
      const ObTabletID &inv_idx_tablet_id,
      const ObTabletID &fwd_idx_tablet_id,
      const ObTabletID &doc_id_idx_tablet_id,
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      const ObDASSortCtDef *sort_ctdef,
      ObDASSortRtDef *sort_rtdef,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot);

  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override { ObNewRow *r = nullptr; return get_next_row(r); }
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual void reset() override;
private:
  int inner_get_next_row_for_output();
  int init_sort(
      const ObDASIRScanCtDef *ir_ctdef,
      const ObDASSortCtDef *sort_ctdef,
      ObDASSortRtDef *sort_rtdef);
  int init_limit(
      const ObDASIRScanCtDef *ir_ctdef,
      ObDASIRScanRtDef *ir_rtdef,
      const ObDASSortCtDef *sort_ctdef,
      ObDASSortRtDef *sort_rtdef);
  int do_sort();
private:
  lib::MemoryContext mem_context_;
  ObTextRetrievalMerge token_merge_;
  common::ObLimitParam limit_param_;
  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
  ObSortOpImpl *sort_impl_;
  ObSEArray<ObExpr *, 2> sort_row_;
  bool sort_finished_;
  bool is_inited_;
};


} // namespace sql
} // namespace oceanbase

#endif
