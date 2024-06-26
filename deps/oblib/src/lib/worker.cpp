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
#include "worker.h"
#include <stdlib.h>
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "lib/allocator/ob_malloc.h"
#include "common/ob_clock_generator.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

#ifdef ERRSIM
  OB_SERIALIZE_MEMBER(ObRuntimeContext, compat_mode_, module_type_);
#else
  OB_SERIALIZE_MEMBER(ObRuntimeContext, compat_mode_);
#endif


namespace oceanbase {
namespace lib {

void * __attribute__((weak)) alloc_worker()
{
  static TLOCAL(Worker, worker);
  return (&worker);
}

int __attribute__((weak)) common_yield()
{
  // do nothing;
  return OB_SUCCESS;
}

}
}

__thread Worker *Worker::self_;

Worker::Worker()
    : allocator_(nullptr),
      st_current_priority_(0),
      session_(nullptr),
      cur_request_(nullptr),
      worker_level_(INT32_MAX),
      curr_request_level_(0),
      group_id_(0),
      rpc_stat_srv_(nullptr),
      timeout_ts_(INT64_MAX),
      ntp_offset_(0),
      rpc_tenant_id_(0),
      disable_wait_(false)
{
  worker_node_.get_data() = this;
}

Worker::~Worker()
{
  if (self_ == this) {
    // We only remove this worker not other worker since the reason
    // described in SET stage.
    self_ = nullptr;
  }
}

Worker::Status Worker::check_wait()
{
  Worker::Status ret_status = WS_NOWAIT;
  int ret = common_yield();
  if (OB_SUCCESS != ret && OB_CANCELED != ret) {
    ret_status = WS_INVALID;
  }
  return ret_status;
}

const uint64_t OBCG_DEFAULT_GROUP_ID = 0;
const uint64_t USER_RESOURCE_GROUP_START_ID = 10000;
const uint64_t OB_INVALID_GROUP_ID = UINT64_MAX;
OB_INLINE bool is_user_group(const uint64_t group_id)
{
  return group_id >= USER_RESOURCE_GROUP_START_ID && group_id != OB_INVALID_GROUP_ID;
}

OB_INLINE bool is_valid_resource_group(const uint64_t group_id)
{
  // other group or user group
  return group_id == OBCG_DEFAULT_GROUP_ID || is_user_group(group_id);
}

void Worker::set_group_id(int32_t group_id)
{
  const int64_t USER_RESOURCE_GROUP_START_ID = 10000;
  if (OBCG_DEFAULT_GROUP_ID == group_id_ || (is_user_group(group_id_) && is_valid_resource_group(group_id))) {
    group_id_ = group_id;
  } else {
    LOG_ERROR_RET(OB_INNER_STAT_ERROR, "group_id is unexpected", K(group_id_), K(group_id));
  }
}

bool Worker::sched_wait()
{
  return true;
}

bool Worker::sched_run(int64_t waittime)
{
  UNUSED(waittime);
  check_status();
  return true;
}


int64_t Worker::get_timeout_remain() const
{
  return timeout_ts_ - ObTimeUtility::current_time();
}

bool Worker::is_timeout() const
{
  return common::ObClockGenerator::getClock() >= timeout_ts_;
}
