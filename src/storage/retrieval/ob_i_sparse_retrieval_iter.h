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

#ifndef OB_I_SPARSE_RETRIEVAL_MERGE_ITER_H_
#define OB_I_SPARSE_RETRIEVAL_MERGE_ITER_H_

#include "share/ob_define.h"
#include "sql/das/ob_das_ir_define.h"

namespace oceanbase
{

namespace common
{
  struct ObLimitParam;
  struct ObDatum;
}
namespace sql
{
  class ObEvalCtx;
  class ObExpr;
  class ObDocIDExt;
}
namespace storage
{
class ObMaxScoreTuple;

class ObISparseRetrievalDimIter
{
public:
  ObISparseRetrievalDimIter() {}
  virtual ~ObISparseRetrievalDimIter() {}

  // basic iterator interface
  virtual int get_next_row() = 0;
  virtual int get_next_batch(const int64_t capacity, int64_t &count) = 0;
  // iterface for dynamic pruning
  virtual int advance_to(const ObDatum &id_datum)
  {
    UNUSED(id_datum);
    return OB_NOT_IMPLEMENT;
  }
  TO_STRING_EMPTY();
private:
  DISALLOW_COPY_AND_ASSIGN(ObISparseRetrievalDimIter);
};

class ObISRDaaTDimIter : public ObISparseRetrievalDimIter
{
public:
  ObISRDaaTDimIter() : ObISparseRetrievalDimIter() {}
  virtual ~ObISRDaaTDimIter() {}
  // DaaT dimension iterator does not need explicit vectorized batch interface, but can process
  //  and buffer batched row in inner implementation.
  virtual int get_next_batch(const int64_t capacity, int64_t &count) override
  {
    return OB_NOT_IMPLEMENT;
  }
  // interface for daat processing
  virtual int get_curr_score(double &score) const = 0;
  virtual int get_curr_id(const ObDatum *&datum) const = 0;
  // interface for plain dynamic pruning algorithms such as WAND and MaxScore
  virtual int get_dim_max_score(double &score)
  {
    return OB_NOT_SUPPORTED;
  }
  TO_STRING_EMPTY();
private:
  DISALLOW_COPY_AND_ASSIGN(ObISRDaaTDimIter);
};

class ObISRDimBlockMaxIter : public ObISRDaaTDimIter
{
public :
  ObISRDimBlockMaxIter() : ObISRDaaTDimIter() {}
  virtual ~ObISRDimBlockMaxIter() {}

  // interfaces for Block-Max dynamic pruning such as BMW and BMM
  virtual int advance_shallow(const ObDatum &id_datum, const bool inclusive) = 0;
  virtual int get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple) = 0;
  virtual bool in_shallow_status() const = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObISRDimBlockMaxIter);
};

struct ObSparseRetrievalMergeParam
{
  ObSparseRetrievalMergeParam()
    : dim_weights_(nullptr),
      limit_param_(nullptr),
      eval_ctx_(nullptr),
      id_proj_expr_(nullptr),
      relevance_expr_(nullptr),
      relevance_proj_expr_(nullptr),
      filter_expr_(nullptr),
      topk_limit_(0),
      field_boost_(1.0),
      max_batch_size_(1)
  {}
  ~ObSparseRetrievalMergeParam() {}
  bool need_project_relevance() const { return relevance_proj_expr_ != nullptr; }
  bool need_filter() const { return filter_expr_ != nullptr; }
  bool need_pushdown_topk() const { return topk_limit_ > 0; }
  TO_STRING_KV(KPC_(dim_weights), KPC(limit_param_), KP_(eval_ctx),
      KP_(id_proj_expr), KP_(relevance_expr), KP_(relevance_proj_expr), KP_(filter_expr),
      K_(topk_limit), K_(max_batch_size));
  const ObIArray<double> *dim_weights_; // score weight for each dimension
  const common::ObLimitParam *limit_param_;
  sql::ObEvalCtx *eval_ctx_;
  sql::ObExpr *id_proj_expr_;
  sql::ObExpr *relevance_expr_;
  sql::ObExpr *relevance_proj_expr_;
  sql::ObExpr *filter_expr_; // filter expr on score
  int64_t topk_limit_;
  double field_boost_;
  int64_t max_batch_size_;
};

class ObISparseRetrievalMergeIter
{
public:
  ObISparseRetrievalMergeIter() : input_row_cnt_(0), output_row_cnt_(0), is_inited_(false) {}
  virtual ~ObISparseRetrievalMergeIter() {}

  virtual void reuse() = 0;
  virtual void reset() = 0;
  virtual int get_next_row() = 0;
  virtual int get_next_rows(const int64_t capacity, int64_t &count) = 0;
  virtual int get_query_max_score(double &score) {
    return OB_NOT_SUPPORTED;
  }
  VIRTUAL_TO_STRING_KV(K_(input_row_cnt), K_(output_row_cnt));
public:
  inline static void set_datum_int(ObDatum &datum, const sql::ObDocIdExt &id)
  {
    datum.set_int(id.get_datum().get_int());
  }
  inline static void set_datum_shallow(ObDatum &datum, const sql::ObDocIdExt &id)
  {
    datum.set_datum(id.get_datum());
  }
protected:
  int64_t input_row_cnt_; // iterated row count
  int64_t output_row_cnt_; // projected row count
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObISparseRetrievalMergeIter);
};

} // namespace storage
} // namespace oceanbase

#endif