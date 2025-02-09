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
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{
struct ObDASIRScanCtDef;
struct ObDASIRScanRtDef;
class ObDASTextRetrievalIterator;
class ObFtsEvalNode;
class ObDASTextRetrievalIter;

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
      snapshot_(nullptr),
      query_tokens_(),
      force_return_docid_(false)
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
  ObArray<ObString> query_tokens_;
  bool force_return_docid_;
};

class ObDASTextRetrievalMergeIter : public ObDASIter
{
public:
  enum TokenRelationType
  {
    DISJUNCTIVE = 0,
    CONJUNCTIVE = 1,
    BOOLEAN = 2,
    MAX_RELATION_TYPE
  };
  enum RetrievalProcType
  {
    DAAT = 0,
    TAAT = 1,
    // VAAT = 2,
    MAX_PROC_TYPE
  };
public:
  ObDASTextRetrievalMergeIter();
  virtual ~ObDASTextRetrievalMergeIter() {}

  virtual int do_table_scan() override;
  virtual int rescan() override;
  void set_doc_id_idx_tablet_id(const ObTabletID &tablet_id) { doc_id_idx_tablet_id_ = tablet_id; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  storage::ObTableScanParam &get_doc_agg_param() { return whole_doc_agg_param_; }
  int set_related_tablet_ids(const share::ObLSID &ls_id, const ObDASFTSTabletID &related_tablet_ids);
  virtual int set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters);
  const ObIArray<ObString> &get_query_tokens() { return query_tokens_; }
  bool is_taat_mode() { return RetrievalProcType::TAAT == processing_type_; }
  static int build_query_tokens(const ObDASIRScanCtDef *ir_ctdef, ObDASIRScanRtDef *ir_rtdef, common::ObIAllocator &alloc, ObArray<ObString> &query_tokens, ObFtsEvalNode *&root_node);
  virtual int set_rangkey_and_selector(const common::ObIArray<std::pair<ObDocId, int>> &virtual_rangkeys);
  void set_boolean_compute_node(ObFtsEvalNode *root_node) { root_node_ = root_node;}
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
protected:
  virtual int check_and_prepare();
  int project_result(const ObDocId &docid, const double relevance);
  int project_relevance(const ObDocId &docid, const double relevance);
  int project_docid();
  void clear_evaluated_infos();
  int init_iters(
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot *snapshot,
      const ObIArray<ObString> &query_tokens);
  int init_query_tokens(const ObArray<ObString> &query_tokens);
  void release_iters();
  int init_total_doc_cnt_param(transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot *snapshot);
  int do_total_doc_cnt();
protected:
  static const int64_t OB_DEFAULT_QUERY_TOKEN_ITER_CNT = 4;
  typedef ObSEArray<ObDASTextRetrievalIter *, OB_DEFAULT_QUERY_TOKEN_ITER_CNT> ObDASTokenRetrievalIterArray;
  lib::MemoryContext mem_context_;
  ObArenaAllocator allocator_;
  TokenRelationType relation_type_;
  RetrievalProcType processing_type_;
  const ObDASIRScanCtDef *ir_ctdef_;
  ObDASIRScanRtDef *ir_rtdef_;
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  share::ObLSID ls_id_;
  common::ObTabletID doc_id_idx_tablet_id_;
  ObArray<ObString> query_tokens_;
  ObDASTokenRetrievalIterArray token_iters_;
  ObFixedArray<ObDocId, ObIAllocator> cache_doc_ids_;
  ObFixedArray<int64_t, ObIAllocator> hints_; // the postion of the cur idx cache doc in output exprs
  ObFixedArray<double, ObIAllocator> cache_relevances_; // cache the relevances_ in func lookup
  ObFixedArray<int64_t, ObIAllocator> reverse_hints_; // the postion of the cur output doc in cache doc
  ObFixedArray<double, ObIAllocator> boolean_relevances_; // cache the relevances_ in boolean mode
  ObFtsEvalNode *root_node_;
  int64_t rangekey_size_;
  int64_t next_written_idx_;
  ObDASScanIter *whole_doc_cnt_iter_;
  ObTableScanParam whole_doc_agg_param_;
  common::ObLimitParam limit_param_;
  int64_t input_row_cnt_;
  int64_t output_row_cnt_;
  bool force_return_docid_; // for function lookup
  bool doc_cnt_calculated_;
  bool doc_cnt_iter_acquired_;
  bool is_inited_;
};

class ObDASTRTaatIter : public ObDASTextRetrievalMergeIter
{
public:
  ObDASTRTaatIter();
  virtual ~ObDASTRTaatIter() {}
  virtual int rescan() override;
  virtual int set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters) override;
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int check_and_prepare() override;
protected:
  int get_next_batch_rows(int64_t &count, int64_t capacity);
  virtual int fill_output_exprs(int64_t &count, int64_t safe_capacity);
  int load_next_hashmap();
  int inner_load_next_hashmap();
  int fill_total_doc_cnt();
  int init_stores_by_partition();
  int fill_chunk_store_by_tr_iter();
  int reset_query_token(const ObString &query_token);
protected:
  static const int64_t OB_MAX_HASHMAP_COUNT = 20;
  static const int64_t OB_HASHMAP_DEFAULT_SIZE = 1000;
  hash::ObHashMap<ObDocId, double> **hash_maps_;
  sql::ObChunkDatumStore **datum_stores_;
  sql::ObChunkDatumStore::Iterator **datum_store_iters_;
  int64_t hash_map_size_;
  hash::ObHashMap<ObDocId, double>::iterator *cur_map_iter_;
  int64_t total_doc_cnt_;
  int64_t next_clear_map_idx_;
  int64_t cur_map_idx_;
  ObDocId cache_first_docid_;
  bool is_chunk_store_inited_;
  bool is_hashmap_inited_;
};

class ObDASTRTaatLookupIter : public ObDASTRTaatIter
{
public:
  ObDASTRTaatLookupIter();
  virtual ~ObDASTRTaatLookupIter() {}
  virtual int rescan() override;
protected:
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int fill_output_exprs(int64_t &count, int64_t safe_capacity) override;
};

class ObDASTRDaatIter : public ObDASTextRetrievalMergeIter
{
public:
  ObDASTRDaatIter();
  virtual ~ObDASTRDaatIter() {}
  virtual int rescan() override;
  virtual int do_table_scan() override;
  virtual int set_merge_iters(const ObIArray<ObDASIter *> &retrieval_iters) override;
protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int pull_next_batch_rows();
  virtual int pull_next_batch_rows_with_batch_mode();
  virtual int fill_loser_tree_item(
              ObDASTextRetrievalIter &iter,
              const int64_t iter_idx,
              ObIRIterLoserTreeItem &item);
  int next_disjunctive_document(const bool is_batch, bool &doc_valid);
protected:
  ObIRIterLoserTreeCmp loser_tree_cmp_;
  ObIRIterLoserTree *iter_row_heap_;
  ObFixedArray<int64_t, ObIAllocator> next_batch_iter_idxes_;
  int64_t next_batch_cnt_;
};

class ObDASTRDaatLookupIter : public ObDASTRDaatIter
{
public:
  ObDASTRDaatLookupIter();
  virtual ~ObDASTRDaatLookupIter() {}
  virtual int rescan() override;
protected:
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;
  int next_disjunctive_document(const int capacity);
};
} // namespace sql
} // namespace oceanbase

#endif // OB_DAS_TEXT_RETRIEVAL_MERGE_ITER_H_
