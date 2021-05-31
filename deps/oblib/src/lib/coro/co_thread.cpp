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
#include "co_thread.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace lib {

static constexpr int pthread_stack_size = 2l << 20;
CoThread::CoThread() : Thread(pthread_stack_size)
{}

int CoThread::start()
{
  return Thread::start([this] { CoSched::start(); });
}

}  // namespace lib
}  // namespace oceanbase
