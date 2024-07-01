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

#ifndef OB_DAS_TEXT_RETRIEVAL_MERGE_ITER_H_
#define OB_DAS_TEXT_RETRIEVAL_MERGE_ITER_H_

#include "ob_das_iter.h"
#include "lib/container/ob_loser_tree.h"

namespace oceanbase
{
namespace sql
{
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;
class ObDASTextRetrievalIterator;

static const int64_t OB_MAX_TEXT_RETRIEVAL_TOKEN_CNT = 256;


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



struct ObDASTextRetrievalMergeIterParam : public ObDASIterParam
{
public:
  ObDASTextRetrievalMergeIterParam()
    : ObDASIterParam(DAS_ITER_TEXT_RETRIEVAL_MERGE),
      ir_ctdef_(nullptr),
      ir_rtdef_(nullptr),
      doc_cnt_iter_(nullptr),
      tx_desc_(nullptr),
      snapshot_(nullptr)
  {}

  virtual bool is_valid() const override
  {
    return nullptr != ir_ctdef_ && nullptr != ir_rtdef_;
  }

  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  ObDASIter *doc_cnt_iter_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
};

class ObDASTextRetrievalMergeIter : public ObDASIter
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
  ObDASTextRetrievalMergeIter();
  virtual ~ObDASTextRetrievalMergeIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  void set_doc_id_idx_tablet_id(const ObTabletID &tablet_id) { doc_id_idx_tablet_id_ = tablet_id; }
  void set_ls_id(const ObLSID &ls_id) { ls_id_ = ls_id; }
  storage::ObTableScanParam &get_doc_agg_param() { return whole_doc_agg_param_; }
  int set_related_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids);
  int set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters);
  const ObIArray<ObString> &get_query_tokens() { return query_tokens_; }
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int init_iters(
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      const ObIArray<ObString> &query_tokens);
  int init_query_tokens(const ObDASIRScanCtDef *ir_ctdef, ObDASIRScanRtDef *ir_rtdef);
  void release_iters();
  int pull_next_batch_rows();
  int fill_loser_tree_item(
      ObDASTextRetrievalIter &iter,
      const int64_t iter_idx,
      ObIRIterLoserTreeItem &item);
  int next_disjunctive_document();
  int init_total_doc_cnt_param(transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot *snapshot);
  int do_total_doc_cnt();
  int project_result(const ObIRIterLoserTreeItem &item, const double relevance);
  void clear_evaluated_infos();
private:
  static const int64_t OB_DEFAULT_QUERY_TOKEN_ITER_CNT = 4;
  typedef ObSEArray<ObDASTextRetrievalIter *, OB_DEFAULT_QUERY_TOKEN_ITER_CNT> ObDASTokenRetrievalIterArray;
  lib::MemoryContext mem_context_;
  TokenRelationType relation_type_;
  RetrievalProcType processing_type_;
  ObIAllocator *allocator_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  share::ObLSID ls_id_;
  common::ObTabletID doc_id_idx_tablet_id_;
  ObArray<ObString> query_tokens_;
  ObDASTokenRetrievalIterArray token_iters_;
  ObIRIterLoserTreeCmp loser_tree_cmp_;
  ObIRIterLoserTree *iter_row_heap_;
  ObFixedArray<int64_t, ObIAllocator> next_batch_iter_idxes_;
  int64_t next_batch_cnt_;
  ObDASScanIter *whole_doc_cnt_iter_;
  ObTableScanParam whole_doc_agg_param_;
  common::ObLimitParam limit_param_;
  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
  bool doc_cnt_calculated_;
  bool is_inited_;
};

} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_TEXT_RETRIEVAL_MERGE_ITER_H_
