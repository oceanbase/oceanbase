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

#define USING_LOG_PREFIX STORAGE

#include "ob_sparse_utils.h"

namespace oceanbase
{
namespace storage
{

int ObSRDaaTInnerProductRelevanceCollector::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else {
    total_relevance_ = 0.0;
    is_inited_ = true;
  }
  return ret;
}

void ObSRDaaTInnerProductRelevanceCollector::reset()
{
  total_relevance_ = 0.0;
  is_inited_ = false;
}

void ObSRDaaTInnerProductRelevanceCollector::reuse()
{
  total_relevance_ = 0.0;
}

int ObSRDaaTInnerProductRelevanceCollector::collect_one_dim(const int64_t dim_idx, const double relevance)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    total_relevance_ += relevance;
  }
  return ret;
}

int ObSRDaaTInnerProductRelevanceCollector::get_result(double &relevance, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    relevance = total_relevance_;
    is_valid = true;
  }
  return ret;
}

int ObSRDaaTBooleanRelevanceCollector::init(ObIAllocator *allocator, const int64_t dim_cnt, ObFtsEvalNode *node)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (FALSE_IT(boolean_compute_node_ = node)) {
  } else if (FALSE_IT(boolean_relevances_.set_allocator(allocator))) {
  } else if (OB_FAIL(boolean_relevances_.init(dim_cnt))) {
    LOG_WARN("failed to init boolean relevances array", K(ret));
  } else if (OB_FAIL(boolean_relevances_.prepare_allocate(dim_cnt))) {
    LOG_WARN("failed to prepare allocate boolean relevacnes array", K(ret));
  } else {
    for (int64_t i = 0; i < boolean_relevances_.count(); ++i) {
      boolean_relevances_[i] = 0.0;
    }
    is_inited_ = true;
  }
  return ret;
}

void ObSRDaaTBooleanRelevanceCollector::reset()
{
  boolean_relevances_.reset();
  if (OB_NOT_NULL(boolean_compute_node_)) {
    boolean_compute_node_->release();
    boolean_compute_node_ = nullptr;
  }
  is_inited_ = false;
}

void ObSRDaaTBooleanRelevanceCollector::reuse()
{
  for (int64_t i = 0; i < boolean_relevances_.count(); ++i) {
    boolean_relevances_[i] = 0.0;
  }
}

int ObSRDaaTBooleanRelevanceCollector::collect_one_dim(const int64_t dim_idx, const double relevance)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(dim_idx < 0 || dim_idx >= boolean_relevances_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dim idx", K(dim_idx), K(boolean_relevances_.count()));
  } else {
    boolean_relevances_[dim_idx] = relevance;
  }
  return ret;
}

int ObSRDaaTBooleanRelevanceCollector::get_result(double &relevance, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_ISNULL(boolean_compute_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null boolean compute node", K(ret));
  } else if (OB_FAIL(ObFtsEvalNode::fts_boolean_eval(boolean_compute_node_, boolean_relevances_, relevance))) {
    LOG_WARN("failed to evaluate boolean relevance");
  } else {
    is_valid = relevance > 0;
  }
  return ret;
}

int ObMultiMatchRelevanceCollector::init(
    ObIAllocator *allocator,
    const ObIArray<double> &field_boosts,
    const ObIArray<double> &token_weights,
    const double match_boost,
    const int64_t should_match,
    const ObMatchFieldsType match_fields_type)
{
  int ret = OB_SUCCESS;
  const int64_t field_count = field_boosts.count();
  field_boosts_.set_allocator(allocator);
  token_weights_.set_allocator(allocator);
  per_field_matches_.set_allocator(allocator);
  per_field_relevances_.set_allocator(allocator);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("double initialization", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_UNLIKELY(ObMatchFieldsType::MATCH_MOST_FIELDS != match_fields_type
      && ObMatchFieldsType::MATCH_BEST_FIELDS != match_fields_type)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected match fields type", K(ret), K(match_fields_type));
  } else if (OB_FAIL(field_boosts_.assign(field_boosts))) {
    LOG_WARN("failed to assign field weights", K(ret));
  } else if (OB_FAIL(token_weights_.assign(token_weights))) {
    LOG_WARN("failed to assign token weights", K(ret));
  } else if (OB_FAIL(per_field_matches_.init(field_count))) {
    LOG_WARN("failed to init per field matches", K(ret));
  } else if (OB_FAIL(per_field_matches_.prepare_allocate(field_count))) {
    LOG_WARN("failed to prepare allocate per field matches");
  } else if (OB_FAIL(per_field_relevances_.init(field_count))) {
    LOG_WARN("failed to init per field relevances", K(ret));
  } else if (OB_FAIL(per_field_relevances_.prepare_allocate(field_count))) {
    LOG_WARN("failed to prepare allocate per field relevances", K(ret));
  } else {
    for (int64_t i = 0; i < field_count; ++i) {
      per_field_relevances_[i] = 0.0;
    }
    for (int64_t i = 0; i < field_count; ++i) {
      per_field_matches_[i] = 0;
    }
    match_boost_ = match_boost;
    should_match_ = should_match;
    type_ = match_fields_type;
    is_inited_ = true;
  }
  return ret;
}

void ObMultiMatchRelevanceCollector::reset()
{
  field_boosts_.reset();
  token_weights_.reset();
  per_field_matches_.reset();
  per_field_relevances_.reset();
  is_inited_ = false;
}

void ObMultiMatchRelevanceCollector::reuse()
{
  for (int64_t i = 0; i < per_field_matches_.count(); ++i) {
    per_field_matches_[i] = 0;
  }
  for (int64_t i = 0; i < per_field_relevances_.count(); ++i) {
    per_field_relevances_[i] = 0.0;
  }
}

int ObMultiMatchRelevanceCollector::collect_one_dim(const int64_t dim_idx, const double relevance)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else if (OB_UNLIKELY(dim_idx < 0 || dim_idx >= field_boosts_.count() * token_weights_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid dim idx", K(dim_idx), K(field_boosts_.count()), K(token_weights_.count()));
  } else {
    int64_t field_idx = dim_idx / token_weights_.count();
    int64_t token_idx = dim_idx % token_weights_.count();
    per_field_relevances_[field_idx] += relevance * token_weights_[token_idx] * field_boosts_[field_idx];
    ++per_field_matches_[field_idx];
  }
  return ret;
}

int ObMultiMatchRelevanceCollector::get_result(double &relevance, bool &is_valid)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    is_valid = false;
    relevance = 0.0;
    for (int64_t i = 0; i < per_field_relevances_.count(); ++i) {
      if (per_field_matches_[i] >= should_match_) {
        is_valid = true;
        if (ObMatchFieldsType::MATCH_BEST_FIELDS == type_) {
          relevance = MAX(relevance, per_field_relevances_[i]);
        } else if (ObMatchFieldsType::MATCH_MOST_FIELDS == type_) {
          relevance += per_field_relevances_[i];
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected match fields type", K(ret), K_(type));
        }
      }
    }
    relevance = relevance * match_boost_ / norm_;
  }
  return ret;
}

int ObMultiMatchRelevanceCollector::get_partial_result(double &relevance)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret));
  } else {
    relevance = 0.0;
    for (int64_t i = 0; i < per_field_relevances_.count(); ++i) {
      if (ObMatchFieldsType::MATCH_BEST_FIELDS == type_) {
        relevance = MAX(relevance, per_field_relevances_[i]);
      } else if (ObMatchFieldsType::MATCH_MOST_FIELDS == type_) {
        relevance += per_field_relevances_[i];
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected match fields type", K(ret), K_(type));
      }
    }
    relevance = relevance * match_boost_ / norm_;
  }
  return ret;
}

int ObMultiMatchRelevanceCollector::set_norm(const double max_score)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max_score <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("max score must be positive", K(ret), K(max_score));
  } else {
    norm_ = max_score / match_boost_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase