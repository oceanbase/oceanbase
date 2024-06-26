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

#define USING_LOG_PREFIX SERVER

#include "ob_redis_meta.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/string/ob_string_buffer.h"

namespace oceanbase
{
namespace table
{
OB_SERIALIZE_MEMBER(ObRedisMeta, ttl_, reserved_);
OB_SERIALIZE_MEMBER(ObRedisListMeta, count_, left_idx_, right_idx_, ttl_, reserved_);

int ObRedisMeta::decode(const ObString &encoded_content)
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("should not call ObRedisMeta::encode", K(ret));
  return ret;
}

int ObRedisMeta::encode(ObIAllocator &allocator, ObString &encoded_content) const
{
  int ret = OB_ERR_UNEXPECTED;
  LOG_WARN("should not call ObRedisMeta::encode", K(ret));

  return ret;
}

int ObRedisListMeta::decode(const ObString &encoded_content)
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  // debug mode
  ObString meta_value = encoded_content;
  // meta_value: count:left_idx:right_idx:ttl
  ObString left_idx_str;
  ObString right_idx_str;
  ObString count_str = meta_value.split_on(META_SPLIT_FLAG);
  if (count_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else if (OB_FALSE_IT(left_idx_str = meta_value.split_on(META_SPLIT_FLAG))) {
  } else if (left_idx_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else if (OB_FALSE_IT(right_idx_str = meta_value)) {
  } else if (right_idx_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid meta value", K(ret), K(meta_value));
  } else {
    bool is_valid;
    count_ =
        ObFastAtoi<int64_t>::atoi(count_str.ptr(), count_str.ptr() + count_str.length(), is_valid);
    if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid meta value", K(ret), K(meta_value));
    } else if (OB_FALSE_IT(
                   left_idx_ = ObFastAtoi<int64_t>::atoi(
                       left_idx_str.ptr(), left_idx_str.ptr() + left_idx_str.length(), is_valid))) {
    } else if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid meta value", K(ret), K(meta_value));
    } else if (OB_FALSE_IT(right_idx_ = ObFastAtoi<int64_t>::atoi(right_idx_str.ptr(),
                                                                  right_idx_str.ptr()
                                                                      + right_idx_str.length(),
                                                                  is_valid))) {
    } else if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid meta value", K(ret), K(meta_value));
    }
  }

#else
  // release mode
  int64_t pos = 0;
  if (OB_FAIL(deserialize(encoded_content.ptr(), encoded_content.length(), pos))) {
    LOG_WARN("fail to deserialize meta value", K(ret), K(encoded_content));
  }
#endif
  return ret;
}

int ObRedisListMeta::encode(ObIAllocator &allocator, ObString &encoded_content) const
{
  int ret = OB_SUCCESS;
#ifndef NDEBUG
  // debug mode
  ObFastFormatInt count_i(count_);
  ObString count_str(count_i.length(), count_i.ptr());

  ObFastFormatInt left_idx_i(left_idx_);
  ObString left_idx_str(left_idx_i.length(), left_idx_i.ptr());

  ObFastFormatInt right_idx_i(right_idx_);
  ObString right_idx_str(right_idx_i.length(), right_idx_i.ptr());

  ObFastFormatInt ttl_i(ttl_);
  ObString ttl_str(ttl_i.length(), ttl_i.ptr());

  const char flag = META_SPLIT_FLAG;

  ObStringBuffer buffer(&allocator);
  int split_flag_count = ObRedisListMeta::ELE_COUNT - 1;
  if (OB_FAIL(buffer.reserve(split_flag_count + count_i.length() + left_idx_i.length()
                             + right_idx_i.length() + ttl_str.length()))) {
    LOG_WARN("fail to reserve memory for string buffer", K(ret), KP(this));
  } else if (OB_FAIL(buffer.append(count_str))) {
    LOG_WARN("fail to append count_str", K(ret), K(count_str));
  } else if (OB_FAIL(buffer.append(&flag, 1))) {
    LOG_WARN("fail to append flag", K(ret), K(flag));
  } else if (OB_FAIL(buffer.append(left_idx_str))) {
    LOG_WARN("fail to append left_idx_str", K(ret), K(left_idx_str));
  } else if (OB_FAIL(buffer.append(&flag, 1))) {
    LOG_WARN("fail to append flag", K(ret), K(flag));
  } else if (OB_FAIL(buffer.append(right_idx_str))) {
    LOG_WARN("fail to append right_idx_str", K(ret), K(right_idx_str));
  } else if (OB_FAIL(buffer.get_result_string(encoded_content))) {
    LOG_WARN("fail to get result string", K(ret), KP(this));
  }
#else
  // release mode
  const int64_t size = get_serialize_size();
  if (size > 0) {
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(size));
    } else if (OB_FAIL(serialize(buf, size, pos))) {
      LOG_WARN("fail to serialize meta value", K(ret), KP(this));
    } else {
      encoded_content.assign_ptr(buf, size);
    }
  }
#endif

  return ret;
}

}  // namespace table
}  // namespace oceanbase