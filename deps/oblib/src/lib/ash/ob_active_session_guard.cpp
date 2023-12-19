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

#include "lib/ash/ob_active_session_guard.h"

using namespace oceanbase::common;

ActiveSessionStat ObActiveSessionGuard::dummy_stat_;
thread_local ActiveSessionStat ObActiveSessionGuard::thread_local_stat_;

ActiveSessionStat *&ObActiveSessionGuard::get_stat_ptr()
{
  // before ObActiveSessionGuard constructed,
  // ensure it can get the dummy value if anyone call get_stat
  RLOCAL_INIT(ActiveSessionStat *, stat, &thread_local_stat_);
  return stat;
}

ActiveSessionStat &ObActiveSessionGuard::get_stat()
{
  return *get_stat_ptr();
}

void ObActiveSessionGuard::setup_default_ash()
{
  setup_thread_local_ash();
}

void ObActiveSessionGuard::setup_ash(ActiveSessionStat &stat)
{
  get_stat_ptr() = &stat;
}

void ObActiveSessionGuard::setup_thread_local_ash()
{
  get_stat_ptr() = &thread_local_stat_;
}

ObBackgroundSessionIdGenerator &ObBackgroundSessionIdGenerator::get_instance() {
  static ObBackgroundSessionIdGenerator the_one;
  return the_one;
}
// |<---------------------------------64bit---------------------------->|
// 0b                        32b                                      64b
// +---------------------------+----------------------------------------+
// |         Local Seq         |                Zero                    |
// +---------------------------+----------------------------------------+
//
//Local Seq: 一个server可用连接数，目前单台server最多有INT32_MAX个连接;
//Zero     : 置零，保留字段，用于和sql_session做区分
uint64_t ObBackgroundSessionIdGenerator::get_next_sess_id() {
  uint64_t sessid = 0;
  const uint64_t local_seq = static_cast<uint32_t>(ATOMIC_AAF(&local_seq_, 1));
  sessid |= (local_seq << 32);
  LOG_INFO("succ to generate background session id", K(sessid));

  return sessid;
}