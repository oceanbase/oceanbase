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

#include "share/ob_simple_batch.h"
namespace oceanbase
{
namespace common
{

void ObSimpleBatch::destroy()
{
  if (T_SCAN == type_ && range_ != NULL) {
    range_->~SQLScanRange();
    range_ = NULL;
  } else if (T_MULTI_SCAN == type_ && ranges_ != NULL) {
    ranges_->~SQLScanRangeArray();
    ranges_ = NULL;
  }
}

int64_t ObSimpleBatch::get_serialize_size(void) const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  if (T_NONE == type_) {
    /*do nothing*/
  } else if (OB_ISNULL(range_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "NULL data on calc size", K(range_));
  } else if (T_SCAN == type_) {
    len += range_->get_serialize_size();
  } else if (T_MULTI_SCAN == type_) {
    len += ranges_->get_serialize_size();
  }

  return len;
}

int ObSimpleBatch::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (T_NONE == type_) {
    /* send nothing */
  } else if (OB_ISNULL(range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL data", K(ret));
  } else if (T_SCAN == type_) {
    if (OB_FAIL(range_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize range", K(ret));
    }
  } else if (T_MULTI_SCAN == type_) {
    if (OB_FAIL(ranges_->serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize ranges", K(ret));
    }
  }

  return ret;
}

#define ALLOC_VAR(type, x) \
    do { \
      if (OB_ISNULL(x = (type*)allocator.alloc(sizeof(type)))) {  \
        ret = OB_ERR_UNEXPECTED;  \
        LOG_WARN("NULL value", K(ret), K(x));  \
      } else {  \
        (x) = new((x)) type;  \
      } \
    } while(0)

#define GET_DESERIALIZED_VAR(type, x)  \
    do {  \
      ALLOC_VAR(type, x); \
      if (OB_SUCC(ret) && OB_FAIL((x)->deserialize(allocator, buf, data_len, pos))) { \
        LOG_WARN("fail to serialize var", K(ret));  \
      } \
    } while(0)

int ObSimpleBatch::deserialize(common::ObIAllocator &allocator,
                         const char *buf,
                         const int64_t data_len,
                         int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode(buf, data_len, pos, type_))) {
    LOG_WARN("fail to deserialize batch type", K(ret));
  } else if (T_NONE == type_) {
    /* send nothing */
  } else if (T_SCAN == type_) {
    SQLScanRange *range = NULL;
    GET_DESERIALIZED_VAR(SQLScanRange, range);
    if (OB_SUCC(ret)) {
      range_ = range;
    }
  } else if (T_MULTI_SCAN == type_) {
    SQLScanRangeArray *ranges = NULL;
    int64_t M = 0;
    ALLOC_VAR(SQLScanRangeArray, ranges);
    if (OB_SUCC(ret) && OB_FAIL(serialization::decode_vi64(buf,
                                                           data_len,
                                                           pos,
                                                           ((int64_t *)(&M))))) {
      LOG_WARN("failed to deserialize count", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < M; i++) {
      SQLScanRange range;
      if (OB_FAIL(range.deserialize(allocator, buf, data_len, pos))) {
        LOG_WARN("failed to deserialize range", K(ret));
      } else if (OB_FAIL(ranges->push_back(range))) {
        LOG_WARN("failed to push back range to batch", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ranges_ = ranges;
    }
  }

  return ret;
}
#undef ALLOC_VAR
#undef GET_DESERIALIZED_VAR

} // end namespace share
} // end namespace oceanbase
