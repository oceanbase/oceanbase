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

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_FTS_AND_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_FTS_AND_ITER_H_

#include "sql/das/iter/ob_das_index_merge_and_iter.h"
#include "share/vector_type/ob_vector_common_util.h"
#include "sql/das/iter/sparse_retrieval/ob_das_tr_merge_iter.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

class ObDASTRMergeIter;

struct ObFTSResultItem {
  double relevance_;
  ObDASIndexMergeIter::MergeResultBuffer* row_buffer_;

  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
    } else {
      pos += snprintf(buf + pos, buf_len - pos, "relevance=%f", relevance_);
    }
    return pos;
  }
};

struct ObFTSResultRelevanceCmp
{
public:
  bool operator() (const ObFTSResultItem &a, const ObFTSResultItem &b)
  {
    return a.relevance_ > b.relevance_;
  }
  int get_error_code() { return common::OB_SUCCESS; }
};

struct ObFTSResultItemRowkeyCmp
{
public:
  ObFTSResultItemRowkeyCmp(int &ret, const common::ObIArray<ObExpr*> *rowkey_exprs, const bool is_reverse_)
    : ret_(ret), rowkey_exprs_(rowkey_exprs), is_reverse_(is_reverse_)
  {}

  int ret_;
  const common::ObIArray<ObExpr*> *rowkey_exprs_;
  const bool is_reverse_;

  bool operator() (const ObFTSResultItem &a, const ObFTSResultItem &b)
  {
    int ret = OB_SUCCESS;
    int cmp_ret = 0;

    ObDASIndexMergeIter::MergeResultBuffer *row_buffer = a.row_buffer_;
    ObDASIndexMergeIter::MergeResultBuffer *cmp_buffer = b.row_buffer_;
    const ObChunkDatumStore::StoredRow *sr = NULL;
    const ObChunkDatumStore::StoredRow *cmp_sr = NULL;

    if (OB_ISNULL(row_buffer) || OB_ISNULL(cmp_buffer)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row_buffer or cmp_buffer is nullptr", K(ret));
    } else if (OB_FAIL(row_buffer->result_store_iter_.get_cur_row(sr))) {
      LOG_WARN("failed to get next row from row buffer", K(ret));
    } else if (OB_FAIL(cmp_buffer->result_store_iter_.get_cur_row(cmp_sr))) {
      LOG_WARN("failed to get next row from cmp buffer", K(ret));
    } else {
      const ObDatum *cur_datums = sr->cells();
      const ObDatum *cmp_datums = cmp_sr->cells();
      if (OB_ISNULL(cur_datums) || OB_ISNULL(cmp_datums)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), K(cur_datums), K(cmp_datums));
      } else if (OB_UNLIKELY(cur_datums->is_null() || cmp_datums->is_null())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null datum", K(ret), KPC(cur_datums), KPC(cmp_datums));
      } else {
        ObObj cur_obj;
        ObObj cmp_obj;
        for (int64_t i = 0; (cmp_ret == 0) && OB_SUCC(ret) && i < rowkey_exprs_->count(); i++) {
          const ObExpr *expr = rowkey_exprs_->at(i);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(ret));
          } else if (OB_FAIL(cur_datums[i].to_obj(cur_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("failed to convert left datum to obj", K(i), KPC(expr), K(ret));
          } else if (OB_FAIL(cmp_datums[i].to_obj(cmp_obj, expr->obj_meta_, expr->obj_datum_map_))) {
            LOG_WARN("failed to convert right datum to obj", K(i), KPC(expr), K(ret));
          } else if (OB_FAIL(cur_obj.check_collation_free_and_compare(cmp_obj, cmp_ret))) {
            LOG_WARN("failed to compare cur obj with output obj", K(ret));
          } else if (OB_FALSE_IT(cmp_ret = OB_UNLIKELY(is_reverse_) ? -cmp_ret : cmp_ret)) {
          }
        }
      }
    }

    ret_ = ret;
    return cmp_ret < 0;
  }
};


struct ObDASIndexMergeFTSAndIterParam : public ObDASIndexMergeIterParam
{
  public:
  ObDASIndexMergeFTSAndIterParam()
    : ObDASIndexMergeIterParam()
  {
    limit_ = 0;
    offset_ = 0;
  }

  int64_t limit_;
  int64_t offset_;
  common::ObSEArray<ObExpr*, 16> relevance_exprs_;
  ObDASTRMergeIter *pushdown_topk_iter_;
  ObDASIter *pushdown_topk_iter_tree_;
  int64_t first_fts_idx_;
};

class ObDASIndexMergeFTSAndIter : public ObDASIndexMergeAndIter
{

public:
  ObDASIndexMergeFTSAndIter()
    : ObDASIndexMergeAndIter(),
      limit_(0),
      offset_(0),
      ready_to_output_(false),
      cur_result_item_idx_(0),
      result_item_size_(0),
      result_items_(NULL),
      first_round_min_relevance_(0),
      pushdown_topk_iter_first_scan_(true),
      fts_index_idxs_(),
      normal_index_idxs_(),
      relevance_cmp_(),
      relevance_exprs_(),
      fts_index_output_exprs_(),
      normal_index_output_exprs_()
  {}

  virtual ~ObDASIndexMergeFTSAndIter() {}

  int64_t get_first_fts_idx() const { return first_fts_idx_; }
  ObDASIter *get_pushdown_topk_iter_tree() const { return pushdown_topk_iter_tree_; }

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:

  typedef ObSPIVFixedSizeHeap<ObFTSResultItem, ObFTSResultRelevanceCmp> ObMinRelevanceHeap;

  int get_relevance(int64_t child_idx, double &relevance);
  int compare(ObDASIndexMergeIter::MergeResultBuffer* row_buffer, IndexMergeRowStore &cmp_store, int &cmp_ret) const;
  int fill_other_child_stores(int64_t capacity);
  int fill_one_child_stores(int64_t capacity, int64_t child_idx, ObDASIter *child_iter);
  int prepare_outout(bool is_vectorized, int64_t capacity);

  int execute_first_round_scan(ObMinRelevanceHeap &first_round_results) { return OB_NOT_IMPLEMENT; };
  int execute_first_round_scan_vectorized(int64_t capacity, ObMinRelevanceHeap &first_round_results);
  int set_topk_relevance_threshold();
  int execute_second_round_scan(bool is_vectorized, int64_t capacity, ObMinRelevanceHeap &first_round_results);
  int get_topn_fts_result(ObMinRelevanceHeap &heap) { return OB_NOT_IMPLEMENT; };
  int get_topn_fts_result_vectorized(int64_t capacity, ObMinRelevanceHeap &heap);
  int sort_fts_result_by_rowkey(ObMinRelevanceHeap &heap, ObFTSResultItem* &fts_result_items);
  int filter_fts_result_by_other_index(ObMinRelevanceHeap &first_round_results, ObFTSResultItem* &sorted_fts_result_items, int64_t actual_top_n) { return OB_NOT_IMPLEMENT; };
  int filter_fts_result_by_other_index_vectorized(int64_t capacity, ObMinRelevanceHeap &first_round_results, ObFTSResultItem* &sorted_fts_result_items, int64_t actual_top_n);
  int sort_first_round_result_by_relevance(ObMinRelevanceHeap &heap, ObFTSResultItem* &result_items);
  int64_t limit_;
  int64_t offset_;
  bool ready_to_output_;

  int64_t cur_result_item_idx_;
  int64_t result_item_size_;
  ObFTSResultItem* result_items_;

  double first_round_min_relevance_;
  ObDASTRMergeIter *pushdown_topk_iter_;
  ObDASIter *pushdown_topk_iter_tree_;
  int64_t first_fts_idx_;
  bool pushdown_topk_iter_first_scan_;

  common::ObSEArray<uint64_t, 16> fts_index_idxs_;
  common::ObSEArray<uint64_t, 16> normal_index_idxs_;

  ObFTSResultRelevanceCmp relevance_cmp_;
  common::ObSEArray<ObExpr*, 16> relevance_exprs_;

  common::ObSEArray<ObExpr*, 16> fts_index_output_exprs_;
  common::ObSEArray<ObExpr*, 16> normal_index_output_exprs_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_INDEX_MERGE_FTS_AND_ITER_H_ */

