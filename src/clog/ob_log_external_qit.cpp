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

#include "ob_log_external_qit.h"

namespace oceanbase {
using namespace common;
namespace extlog {

static const int64_t handle_times_[ObExtRpcQit::RPC_TYPE_MAX] = {
    ObExtRpcQit::HANDLE_TIME_INVALD,
    ObExtRpcQit::HANDLE_TIME_TS,
    ObExtRpcQit::HANDLE_TIME_ID,
};

int ObExtRpcQit::init(const ExtRpcType& type, const int64_t deadline)
{
  int ret = OB_SUCCESS;
  // a small engouh timestamp, used to avoid misuse time_interval and timestamp
  const int64_t BASE_DEADLINE = 1000000000000000;  // 2001-09-09
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(!is_valid_rpc_type(type)) || OB_UNLIKELY(deadline <= BASE_DEADLINE)) {
    ret = OB_INVALID_ARGUMENT;
    EXTLOG_LOG(WARN, "Qit init error", K(ret), K(type), K(deadline));
  } else {
    type_ = type;
    deadline_ = deadline;
    is_inited_ = true;
    EXTLOG_LOG(TRACE, "qit init success", K(type), K(deadline));
  }
  return ret;
}

int64_t ObExtRpcQit::get_handle_time() const
{
  int64_t interval = 0;
  if (is_valid_rpc_type(type_)) {
    interval = handle_times_[type_];
  } else {
    EXTLOG_LOG(ERROR, "invalid ext rpc type", K(type_));
  }
  return interval;
}

int ObExtRpcQit::enough_time(const int64_t reserved_interval, bool& is_enough) const
{
  int ret = OB_SUCCESS;
  int64_t now = cur_ts();
  EXTLOG_LOG(TRACE, "enough_time", K(now), K(deadline_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (now >= deadline_) {
    is_enough = false;
    EXTLOG_LOG(WARN, "wait in rpc queue too long", K(deadline_), K(now));
  } else if (deadline_ - now < reserved_interval) {
    is_enough = false;
    EXTLOG_LOG(WARN, "not enough time to handle req", K(deadline_), K(now), K(reserved_interval));
  } else {
    is_enough = true;
  }
  return ret;
}

int ObExtRpcQit::should_hurry_quit(bool& is_hurry_quit) const
{
  int ret = OB_SUCCESS;
  bool is_enough = false;
  ret = enough_time(HURRY_QUIT_RESERVED, is_enough);
  is_hurry_quit = !is_enough;
  return ret;
}

}  // namespace extlog
}  // namespace oceanbase
