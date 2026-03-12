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
  ObSRDaaTRelevanceCollector() : is_inited_(false) {}
  virtual ~ObSRDaaTRelevanceCollector() {}

  virtual void reset() = 0;
  virtual void reuse() = 0;
  virtual int collect_one_dim(const int64_t dim_idx, const double relevance) = 0;
  virtual int get_result(double &relevance, bool &is_valid) = 0;
  virtual int get_partial_result(double &relevance)
  {
    bool is_valid = false;
    return get_result(relevance, is_valid);
  }
  virtual int set_norm(const double max_score) { return OB_NOT_IMPLEMENT; }
protected:
  bool is_inited_;
};

struct ObSRDaaTInnerProductRelevanceCollector : ObSRDaaTRelevanceCollector
{
  ObSRDaaTInnerProductRelevanceCollector() : ObSRDaaTRelevanceCollector(),
      total_relevance_(0) {}
  virtual ~ObSRDaaTInnerProductRelevanceCollector() {}

  int init();
  virtual void reset() override;
  virtual void reuse() override;
  virtual int collect_one_dim(const int64_t dim_idx, const double relevance) override;
  virtual int get_result(double &relevance, bool &is_valid) override;
private:
  double total_relevance_;
};

struct ObSRDaaTBooleanRelevanceCollector : ObSRDaaTRelevanceCollector
{
  ObSRDaaTBooleanRelevanceCollector() : ObSRDaaTRelevanceCollector(),
      boolean_compute_node_(nullptr), boolean_relevances_() {}
  virtual ~ObSRDaaTBooleanRelevanceCollector() {}

  int init(ObIAllocator *allocator, const int64_t dim_cnt, ObFtsEvalNode *node);
  virtual void reset() override;
  virtual void reuse() override;
  virtual int collect_one_dim(const int64_t dim_idx, const double relevance) override;
  virtual int get_result(double &relevance, bool &is_valid) override;
private:
  ObFtsEvalNode *boolean_compute_node_;
  ObFixedArray<double, ObIAllocator> boolean_relevances_;
};

struct ObMultiMatchRelevanceCollector : ObSRDaaTRelevanceCollector
{
  ObMultiMatchRelevanceCollector() : ObSRDaaTRelevanceCollector(),
      field_boosts_(), token_weights_(), match_boost_(1.0), norm_(1.0),
      should_match_(1), type_(MATCH_BEST_FIELDS), per_field_matches_(), per_field_relevances_() {}
  virtual ~ObMultiMatchRelevanceCollector() {}

  int init(ObIAllocator *allocator,
           const ObIArray<double> &field_boosts,
           const ObIArray<double> &token_weights,
           const double match_boost,
           const int64_t should_match,
           const ObMatchFieldsType match_fields_type);
  virtual void reset() override;
  virtual void reuse() override;
  virtual int collect_one_dim(const int64_t dim_idx, const double relevance) override;
  virtual int get_result(double &relevance, bool &is_valid) override;
  virtual int get_partial_result(double &relevance) override;
  virtual int set_norm(const double max_score) override;
private:
  ObFixedArray<double, ObIAllocator> field_boosts_;
  ObFixedArray<double, ObIAllocator> token_weights_;
  double match_boost_;
  double norm_;
  int64_t should_match_;
  ObMatchFieldsType type_;
  ObFixedArray<int64_t, ObIAllocator> per_field_matches_;
  ObFixedArray<double, ObIAllocator> per_field_relevances_;
};


} // namespace storage
} // namespace oceanbase

#endif