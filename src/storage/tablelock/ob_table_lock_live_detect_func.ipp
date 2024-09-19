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

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_IPP
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_IPP
#include "lib/function/ob_function.h"
#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_H_IPP
#  define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_H_IPP
#  include "ob_table_lock_live_detect_func.h"
#endif
// #define USING_LOG_PREFIX TABLELOCK

namespace oceanbase
{
namespace transaction
{

namespace tablelock
{
template <typename... Args>
// TODO: modify the interface of calling function
int ObTableLockDetectFunc<Args...>::call_function_by_param(const char *buf, bool &is_alive, Args &...args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deserialize_detect_func_param(buf, args...))) {
    TABLELOCK_LOG(WARN, "deserialize_detect_func_param failed", K(ret), K(func_no_));
  } else if (OB_FAIL(func_(args...))) {
    TABLELOCK_LOG(WARN, "execute detect function failed", K(ret), K(func_no_));
  }

  return ret;
}

template <typename... Args>
int ObTableLockDetectFunc<Args...>::call_function_directly(Args &...args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(func_(args...))) {
    TABLELOCK_LOG(WARN, "execute detect function failed", K(ret), K(func_no_));
  }

  return ret;
}

template <typename... Args>
int ObTableLockDetectFunc<Args...>::serialize_detect_func_param(char *buf, const Args &...args)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = MAX_LOCK_DETECT_PARAM_LENGTH + 1;
  int64_t pos = 0;

  if (OB_FAIL(serialize_func_param_(buf, buf_len, pos, args...))) {
    TABLELOCK_LOG(WARN, "serialize params for the detect function failed", K(ret), K(pos));
  } else {
    // save buf to inner_table
  }
  return ret;
}

template <typename... Args>
int ObTableLockDetectFunc<Args...>::deserialize_detect_func_param(const char *buf, Args &...args)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = MAX_LOCK_DETECT_PARAM_LENGTH + 1;
  int64_t pos = 0;

  if (OB_FAIL(deserialize_func_param_(buf, buf_len, pos, args...))) {
    TABLELOCK_LOG(WARN, "deserialize params for the detect function failed", K(ret), K(pos));
  }
  return ret;
}

template <typename... Args>
template <typename T, typename... Others>
int ObTableLockDetectFunc<Args...>::serialize_func_param_(
  char *buf, int64_t buf_len, int64_t &pos, const T &arg, const Others &...args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode(buf, buf_len, pos, arg))) {
    TABLELOCK_LOG(WARN, "encode failed", K(ret), K(pos));
  } else if (OB_FAIL(serialize_func_param_(buf, buf_len, pos, args...))) {
    TABLELOCK_LOG(WARN, "serialize params for detect functions failed", K(ret), K(pos));
  }
  return ret;
}

template <typename... Args>
template <typename T, typename... Others>
int ObTableLockDetectFunc<Args...>::deserialize_func_param_(
  const char *buf, int64_t buf_len, int64_t &pos, T &arg, Others &...args)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode(buf, buf_len, pos, arg))) {
    TABLELOCK_LOG(WARN, "decode failed", K(ret), K(pos));
  } else if (OB_FAIL(deserialize_func_param_(buf, buf_len, pos, args...))) {
    TABLELOCK_LOG(WARN, "deserialize params for detect functions failed", K(ret), K(pos));
  }
  return ret;
}
}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase
#endif
