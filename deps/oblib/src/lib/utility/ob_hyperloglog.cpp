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

#define USING_LOG_PREFIX LIB

#include "lib/utility/ob_hyperloglog.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace common
{
OB_DEF_SERIALIZE(ObHyperLogLogCalculator)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, n_bit_, n_count_);
  for (int64_t i = 0; i < n_bucket_ && OB_SUCC(ret); ++i) {
    LST_DO_CODE(OB_UNIS_ENCODE, buckets_[i]);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObHyperLogLogCalculator)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, n_bit_, n_count_);
  if (n_bit_ != 0) {
    if (OB_ISNULL(alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "fail to get deserialize allocator of hyperloglog", K(ret));
    } else if (OB_FAIL(init(alloc_, n_bit_))) {
      COMMON_LOG(WARN, "fail to init hyperloglog", K(ret));
    } else {
      for (int64_t i = 0; i < n_bucket_ && OB_SUCC(ret); ++i) {
        LST_DO_CODE(OB_UNIS_DECODE, buckets_[i]);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObHyperLogLogCalculator)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, n_bit_, n_count_);
  len += n_bucket_;
  return len;
}

} // end namespace common
} // end namespace oceanbase
