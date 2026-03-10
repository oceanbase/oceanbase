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
#ifndef OCEANBASE_SHARE_VECTOR_INDEX_PARAM_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_PARAM_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace share
{

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
    ivf_nprobes_(0)
  {}
  virtual ~ObVectorIndexQueryParam() {}
  int assign(const ObVectorIndexQueryParam &other);
  bool is_valid() const { return flags_ > 0; }

  union {
    uint64_t flags_;
    struct {
      uint64_t is_set_ef_search_            : 1;
      uint64_t is_set_refine_k_             : 1;
      uint64_t is_set_drop_ratio_search_    : 1;
      uint64_t is_set_similarity_threshold_ : 1;
      uint64_t is_set_ivf_nprobes_          : 1;
      uint64_t reserved_                    : 59;
    };
  };
  int32_t ef_search_;
  float refine_k_;
  float ob_sparse_drop_ratio_search_;
  float similarity_threshold_;
  int32_t ivf_nprobes_;

  TO_STRING_KV(K_(is_set_ef_search), K_(ef_search),
      K_(is_set_refine_k), K_(refine_k), K_(ob_sparse_drop_ratio_search), K_(is_set_similarity_threshold), K_(similarity_threshold), K_(is_set_ivf_nprobes), K_(ivf_nprobes), K_(reserved));

};

}  // namespace share
}  // namespace oceanbase

#endif
