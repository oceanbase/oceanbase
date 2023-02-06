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

#ifndef LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_ARGS_CHECKER_H
#define LOGSERVICE_PALF_ELECTION_UTILS_OB_ELECTION_ARGS_CHECKER_H

#include <utility>

namespace oceanbase
{
namespace palf
{
namespace election
{

template <typename T>
inline bool check_arg(T &&arg)
{
  return arg.is_valid();
}

template <typename T>
inline bool check_arg(T *arg)
{
  return arg != nullptr;
}

inline bool check_args() { return true; }

template <typename T, typename ...Args>
inline bool check_args(T &&arg, Args &&...rest)
{
  bool ret = false;
  if (check_arg(std::forward<T>(arg))) {
    ret = check_args(std::forward<Args>(rest)...);
  } else {
    ret = false;
  }
  return ret;
}

#define CHECK_ELECTION_ARGS(...) \
do {\
  if (OB_UNLIKELY(!check_args(__VA_ARGS__))) {\
    ret = common::OB_INVALID_ARGUMENT;\
    ELECT_LOG(ERROR, "invalid argument");\
    return ret;\
  }\
} while(0)

#define CHECK_ELECTION_INIT() \
do {\
  if (OB_UNLIKELY(!is_inited_)) {\
    ELECT_LOG_RET(WARN, common::OB_NOT_INIT, "not init yet", K(*this), K(lbt()));\
    return common::OB_NOT_INIT;\
  }\
} while(0)

#define CHECK_ELECTION_INIT_AND_START() \
do { \
  CHECK_ELECTION_INIT(); \
  if (OB_UNLIKELY(!is_running_)) { \
    ELECT_LOG_RET(WARN, common::OB_NOT_RUNNING, "not running", K(*this), K(lbt()));\
    return common::OB_NOT_RUNNING;\
  }\
} while(0)

}
}
}

#endif
