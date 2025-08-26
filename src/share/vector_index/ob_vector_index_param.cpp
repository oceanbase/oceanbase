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

#define USING_LOG_PREFIX SHARE

#include "ob_vector_index_param.h"

namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObVectorIndexQueryParam, flags_, ef_search_, refine_k_, ob_sparse_drop_ratio_search_);

int ObVectorIndexQueryParam::assign(const ObVectorIndexQueryParam &other)
{
  int ret = OB_SUCCESS;
  flags_ = other.flags_;
  ef_search_ = other.ef_search_;
  refine_k_ = other.refine_k_;
  ob_sparse_drop_ratio_search_ = other.ob_sparse_drop_ratio_search_;
  return ret;
}

}  // namespace share
}  // namespace oceanbase