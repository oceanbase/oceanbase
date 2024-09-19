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
#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_H
#define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_H
#include <cstdint>
#include "deps/oblib/src/lib/ob_errno.h"
namespace oceanbase
{
namespace common
{
class ObStringHolder;
template <typename T>
class ObFunction;
}
namespace transaction
{
namespace tablelock
{
enum ObTableLockDetectType : uint8_t
{
  INVALID_DETECT_TYPE = 0,
  DETECT_SESSION_ALIVE = 1
};
template <typename... Args>
class ObTableLockDetectFunc
{
public:
  ObTableLockDetectFunc() : func_no_(INVALID_DETECT_TYPE), func_(nullptr){};
  ObTableLockDetectFunc(ObTableLockDetectType func_no, common::ObFunction<int(Args...)> func) :
    func_no_(func_no), func_(func){};

public:
  int call_function_by_param(const char *buf, bool &is_alive, Args &...args);
  int call_function_directly(Args &...args);
  int serialize_detect_func_param(char *buf, const Args &...args);
  int deserialize_detect_func_param(const char *buf, Args &...args);

private:
  template <typename T, typename... Others>
  int serialize_func_param_(char *buf, int64_t buf_len, int64_t &pos, const T &arg, const Others &...args);
  int serialize_func_param_(char *buf, int64_t buf_len, int64_t &pos) { return OB_SUCCESS; }
  template <typename T, typename... Others>
  int deserialize_func_param_(const char *buf, int64_t buf_len, int64_t &pos, T &arg, Others &...args);
  int deserialize_func_param_(const char *buf, int64_t buf_len, int64_t &pos) { return OB_SUCCESS; }

public:
  ObTableLockDetectType func_no_;
  common::ObFunction<int(Args...)> func_;
};

}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase

#ifndef OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_H_IPP
#  define OCEANBASE_STORAGE_TABLELOCK_OB_TABLE_LOCK_LIVE_DETECT_FUNC_H_IPP
#  include "ob_table_lock_live_detect_func.ipp"
#endif

#endif
