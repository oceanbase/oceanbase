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

#ifndef OB_SPARSE_UTILS_H_
#define OB_SPARSE_UTILS_H_

#include "ob_i_sparse_retrieval_iter.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/iter/ob_das_text_retrieval_eval_node.h"

namespace oceanbase
{
namespace storage
{

struct ObSRDaaTRelevanceCollector
{
  ObSRDaaTRelevanceCollector() {}
  virtual ~ObSRDaaTRelevanceCollector() {}

  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual int collect_one_dim(const int64_t dim_idx, const double relevance) = 0;
  virtual int get_result(double &relevance, bool &is_valid) = 0;
};

struct ObSRDaaTInnerProductRelevanceCollector : ObSRDaaTRelevanceCollector
{
  ObSRDaaTInnerProductRelevanceCollector() : ObSRDaaTRelevanceCollector(),
      total_relevance_(0),
      matched_cnt_(0),
      should_match_(0) {}
  virtual ~ObSRDaaTInnerProductRelevanceCollector() {};

  int init(int64_t should_match = 0);
  virtual void reset() override;
  virtual void reuse() override;
  virtual int collect_one_dim(const int64_t dim_idx, const double) override;
  virtual int get_result(double &relevance, bool &is_valid) override;
private:
  double total_relevance_;
  int64_t matched_cnt_;
  int64_t should_match_;
};

struct ObSRDaaTBooleanRelevanceCollector : ObSRDaaTRelevanceCollector
{
  ObSRDaaTBooleanRelevanceCollector() : ObSRDaaTRelevanceCollector(),
      allocator_(nullptr), dim_cnt_(0), boolean_compute_node_(nullptr), boolean_relevances_() {}
  virtual ~ObSRDaaTBooleanRelevanceCollector() {};

  int init(ObIAllocator *allocator, const int64_t dim_cnt, ObFtsEvalNode *node);
  virtual void reset() override;
  virtual void reuse() override;
  virtual int collect_one_dim(const int64_t dim_idx, const double) override;
  virtual int get_result(double &relevance, bool &is_valid) override;
private:
  ObIAllocator *allocator_;
  int64_t dim_cnt_;
  ObFtsEvalNode *boolean_compute_node_;
  ObFixedArray<double, ObIAllocator> boolean_relevances_;
};

} // namespace storage
} // namespace oceanbase

#endif