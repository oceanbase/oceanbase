/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_PARAM_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_PARAM_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace share
{

enum ObVecIdxQueryStrategy
{
  LATENCY_FIRST = 0,
  RECALL_FIRST = 1,
  STRATEGY_MAX
};

struct ObVectorIndexQueryParam
{
public:
  OB_UNIS_VERSION(1);

public:
  ObVectorIndexQueryParam():
    flags_(0),
    ef_search_(0),
    refine_k_(0),
    ob_sparse_drop_ratio_search_(0),
    similarity_threshold_(0),
    ivf_nprobes_(0),
    strategy_(ObVecIdxQueryStrategy::RECALL_FIRST),
    bruteforce_fallback_threshold_(0),
    post_filter_max_scan_rows_(0),
    pre_filter_threshold_(0)
  {}
  virtual ~ObVectorIndexQueryParam() {}
  int assign(const ObVectorIndexQueryParam &other);
  bool is_valid() const { return flags_ > 0; }

  union { // FARM COMPAT WHITELIST
    uint64_t flags_;
    struct {
      uint64_t is_set_ef_search_            : 1;
      uint64_t is_set_refine_k_             : 1;
      uint64_t is_set_drop_ratio_search_    : 1;
      uint64_t is_set_similarity_threshold_ : 1;
      uint64_t is_set_ivf_nprobes_          : 1;
      uint64_t is_set_strategy_             : 1;
      uint64_t is_set_bruteforce_fallback_threshold_ : 1;
      uint64_t is_set_post_filter_max_scan_rows_ : 1;
      uint64_t is_set_pre_filter_threshold_ : 1;
      uint64_t reserved_                    : 55;
    };
  };
  int32_t ef_search_;
  float refine_k_;
  float ob_sparse_drop_ratio_search_;
  float similarity_threshold_;
  int32_t ivf_nprobes_;
  ObVecIdxQueryStrategy strategy_; // from sql query parameter
  int32_t bruteforce_fallback_threshold_;
  int64_t post_filter_max_scan_rows_;
  float pre_filter_threshold_;

  TO_STRING_KV(K_(is_set_ef_search), K_(ef_search),
      K_(is_set_refine_k), K_(refine_k), K_(is_set_drop_ratio_search), K_(ob_sparse_drop_ratio_search), K_(is_set_similarity_threshold), K_(similarity_threshold), K_(is_set_ivf_nprobes), K_(ivf_nprobes), K_(is_set_strategy), K_(strategy), K_(is_set_bruteforce_fallback_threshold), K_(bruteforce_fallback_threshold), K_(is_set_post_filter_max_scan_rows), K_(post_filter_max_scan_rows), K_(is_set_pre_filter_threshold), K_(pre_filter_threshold), K_(reserved));

};

}  // namespace share
}  // namespace oceanbase

#endif
