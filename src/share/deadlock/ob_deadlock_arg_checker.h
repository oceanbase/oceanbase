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

#ifndef OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_ARG_CHECKER_H
#define OCEANBASE_SHARE_DEADLOCK_OB_DEADLOCK_ARG_CHECKER_H

#include "lib/container/ob_array.h"
#include "lib/oblog/ob_log_module.h"
#include "ob_deadlock_parameters.h"
#include "lib/utility/utility.h"
#include <stdint.h>

namespace oceanbase
{
namespace share
{
namespace detector
{

using namespace common;

template <class T>
inline bool check_args(const T &arg)
{
  return arg.is_valid();
}

template <class T>
inline bool check_args(T *arg)
{
  return nullptr != arg;
}

template <class T>
inline bool check_args(const common::ObIArray<T> &arg)
{
  return !arg.empty();
}

template <>
inline bool check_args<int64_t>(const int64_t &arg)
{
  return INVALID_VALUE != arg;
}

template <>
inline bool check_args<uint64_t>(const uint64_t &arg)
{
  return INVALID_VALUE != arg;
}

template <class HEAD, class ...Args>
inline int check_args(const HEAD &head, const Args &...rest)
{
  int ret = OB_SUCCESS;
  if (!check_args(head)) {
    ret = OB_INVALID_ARGUMENT;
    DETECT_LOG(WARN, "arg invalid");
  } else {
    ret = check_args(rest...);
  }
  return ret;
}

#define CHECK_ARGS(args...) \
do {\
  int temp_ret = check_args(args);\
  if (temp_ret == OB_INVALID_ARGUMENT) {\
    return temp_ret;\
  }\
} while(0)

#define CHECK_INIT()\
do {\
if (is_inited_ != true) {\
  DETECT_LOG_RET(WARN, OB_NOT_INIT, "not init yet", K_(is_inited));\
  return OB_NOT_INIT;\
}} while(0)

#define CHECK_START()\
do {\
if (is_running_ != true) {\
  DETECT_LOG_RET(WARN, OB_NOT_RUNNING, "not running", K_(is_running));\
  return OB_NOT_RUNNING;\
}} while(0)

#define CHECK_ENABLED()\
do {\
if (!is_deadlock_enabled()) {\
  DETECT_LOG_RET(WARN, OB_NOT_RUNNING, "deadlock not enabled", K(is_deadlock_enabled()));\
  return OB_NOT_RUNNING;\
}} while(0)

#define CHECK_INIT_AND_START() CHECK_INIT();CHECK_START()

}
}
}
#endif
