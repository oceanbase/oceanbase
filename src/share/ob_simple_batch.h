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

#ifndef OCEANBASE_SHARE_OB_COMMON_BATCH_H_
#define OCEANBASE_SHARE_OB_COMMON_BATCH_H_
#include "common/ob_range.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
namespace common
{
typedef ObNewRange SQLScanRange;
typedef ObSEArray<ObNewRange, 10, ModulePageAllocator, true> SQLScanRangeArray;

struct ObSimpleBatch
{
  //TODO shengle is extracted together with ObBatchType in ObBatch, and I will not change it this time.
  //To adjust the storage layer code, change it separately later
  enum ObBatchType
  {
    T_NONE,
    T_GET,
    T_MULTI_GET,
    T_SCAN,
    T_MULTI_SCAN,
  };
  ObBatchType type_;
  union
  {
    const SQLScanRange *range_;
    const SQLScanRangeArray *ranges_;
  };

  ObSimpleBatch() : type_(T_NONE), range_(NULL) { }
  virtual ~ObSimpleBatch() {};

  void destroy();

  bool is_valid() const
  {
    return (T_NONE != type_
            && T_GET != type_
            && T_MULTI_GET != type_
            && (NULL != range_ || NULL != ranges_));
  }
  int64_t to_string(char *buffer, const int64_t length) const
  {
    int64_t pos = 0;
    if (T_NONE == type_) {
      common::databuff_printf(buffer, length, pos, "NONE:");
    } else if (T_SCAN == type_) {
      common::databuff_printf(buffer, length, pos, "SCAN:");
      pos += range_->to_string(buffer + pos, length - pos);
    } else if (T_MULTI_SCAN == type_) {
      common::databuff_printf(buffer, length, pos, "MULTI SCAN:");
      pos += ranges_->to_string(buffer + pos, length - pos);
    } else {
      common::databuff_printf(buffer, length, pos, "invalid type:%d", type_);
    }
    return pos;
  }

  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int64_t get_serialize_size(void) const;
};

} // end of namespace common
} // end of namespace oceanbase

#endif /*OCEANBASE_SHARE_OB_COMMON_BATCH_H_*/
