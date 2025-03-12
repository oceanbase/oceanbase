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
#include "rpc/obrpc/ob_rpc_proxy.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;

OB_DEF_SERIALIZE(ObExtraRpcHeader)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, obrpc::ObRpcProxy::myaddr_);
  return ret;
}
OB_DEF_DESERIALIZE(ObExtraRpcHeader)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, src_addr_);
  return ret;
}
OB_DEF_SERIALIZE_SIZE(ObExtraRpcHeader)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, obrpc::ObRpcProxy::myaddr_);
  return len;
}

#ifdef ERRSIM
  OB_SERIALIZE_MEMBER(ObRuntimeContext, compat_mode_, module_type_, log_reduction_mode_, extra_rpc_header_);
#else
  OB_SERIALIZE_MEMBER(ObRuntimeContext, compat_mode_, log_reduction_mode_, extra_rpc_header_);
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

int __attribute__((weak)) SET_GROUP_ID(uint64_t group_id, bool is_background)
{
  int ret = OB_SUCCESS;
  UNUSED(is_background);
  THIS_WORKER.set_group_id_(group_id);
  return ret;
}

int __attribute__((weak)) CONVERT_FUNCTION_TYPE_TO_GROUP_ID(const uint8_t function_type, uint64_t &group_id)
{
  int ret = OB_SUCCESS;
  UNUSED(function_type);
  group_id = GET_GROUP_ID();
  return ret;
}

}  // namespace lib
}  // namespace oceanbase
__thread Worker *Worker::self_;

Worker::Worker()
    : group_(nullptr),
      allocator_(nullptr),
      st_current_priority_(0),
      session_(nullptr),
      cur_request_(nullptr),
      worker_level_(INT32_MAX),
      curr_request_level_(0),
      is_th_worker_(false),
      group_id_(0),
      func_type_(0),
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
