/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_param.h"

namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObVectorIndexQueryParam, flags_, ef_search_, refine_k_, ob_sparse_drop_ratio_search_, similarity_threshold_, ivf_nprobes_, strategy_);

int ObVectorIndexQueryParam::assign(const ObVectorIndexQueryParam &other)
{
  int ret = OB_SUCCESS;
  flags_ = other.flags_;
  ef_search_ = other.ef_search_;
  refine_k_ = other.refine_k_;
  ob_sparse_drop_ratio_search_ = other.ob_sparse_drop_ratio_search_;
  similarity_threshold_ = other.similarity_threshold_;
  ivf_nprobes_ = other.ivf_nprobes_;
  strategy_ = other.strategy_;
  return ret;
}

}  // namespace share
}  // namespace oceanbase
