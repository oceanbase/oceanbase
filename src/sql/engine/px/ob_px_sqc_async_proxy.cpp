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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/px/ob_px_sqc_async_proxy.h"

namespace oceanbase {
using namespace common;
namespace sql {
/* ObSqcAsyncCB */
int ObSqcAsyncCB::process()
{
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  is_processed_ = true;
  ret = cond_.broadcast();
  return ret;
}

void ObSqcAsyncCB::on_invalid()
{
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  is_invalid_ = true;
  ret = cond_.broadcast();
  LOG_WARN("ObSqcAsyncCB invalid, check object serialization impl or oom", K(trace_id_), K(ret));
}

void ObSqcAsyncCB::on_timeout()
{
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  is_timeout_ = true;
  ret = cond_.broadcast();
  LOG_WARN("ObSqcAsyncCB timeout, check timeout value, peer cpu load, network "
           "packet drop rate",
      K(trace_id_),
      K(ret));
}

rpc::frame::ObReqTransport::AsyncCB* ObSqcAsyncCB::clone(const rpc::frame::SPAlloc& alloc) const
{
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB*>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB* const>(this));
}

/* ObPxSqcAsyncProxy */
int ObPxSqcAsyncProxy::launch_all_rpc_request()
{
  int ret = OB_SUCCESS;
  // prepare allocate the results_ array
  if (OB_FAIL(results_.prepare_allocate(sqcs_.count()))) {
    LOG_WARN("fail to prepare allocate result array");
  }

  ARRAY_FOREACH_X(sqcs_, idx, count, OB_SUCC(ret))
  {
    ret = launch_one_rpc_request(idx, NULL);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to launch all sqc rpc request", K(ret));
    fail_process();
  }
  return ret;
}

int ObPxSqcAsyncProxy::launch_one_rpc_request(int64_t idx, ObSqcAsyncCB* cb)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::TraceId* trace_id = NULL;
  ObPxSqcMeta& sqc = *sqcs_.at(idx);
  const ObAddr& addr = sqc.get_exec_addr();
  ObPxRpcInitSqcArgs args;
  int64_t timeout_us = phy_plan_ctx_->get_timeout_timestamp() - ObTimeUtility::current_time();
  if (phy_plan_->is_new_engine()) {
    if (OB_FAIL(sqc.split_values(exec_ctx_))) {
      LOG_WARN("fail to split values", K(ret));
    } else {
      args.set_serialize_param(exec_ctx_, const_cast<ObOpSpec&>(*dfo_.get_root_op_spec()), *phy_plan_);
    }
  } else {
    args.set_serialize_param(exec_ctx_, const_cast<ObPhyOperator&>(*dfo_.get_root_op()), *phy_plan_);
  }
  if (OB_FAIL(ret)) {
  } else if (timeout_us < 0) {
    ret = OB_TIMEOUT;
  } else if (OB_FAIL(args.sqc_.assign(sqc))) {
    LOG_WARN("fail assign sqc", K(ret));
  } else if (OB_ISNULL(trace_id = ObCurTraceId::get_trace_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get trace id");
  } else {
    // allocate SqcAsync callback
    if (cb == NULL) {
      void* mem = NULL;
      if (NULL == (mem = allocator_.alloc(sizeof(ObSqcAsyncCB)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc memory failed", "size", sizeof(ObSqcAsyncCB), K(ret));
      } else {
        cb = new (mem) ObSqcAsyncCB(cond_, *trace_id);
        if (OB_FAIL(callbacks_.push_back(cb))) {
          // free the callback
          LOG_WARN("callback obarray push back failed.");
          cb->~ObSqcAsyncCB();
          allocator_.free(cb);
          cb = NULL;
        }
      }
    }
    if (cb != NULL) {
      if (OB_SUCC(ret)) {
        if (OB_FAIL(proxy_.to(addr)
                        .by(THIS_WORKER.get_rpc_tenant() ?: session_->get_effective_tenant_id())
                        .as(OB_SYS_TENANT_ID)
                        .timeout(timeout_us)
                        .async_init_sqc(args, cb))) {
          LOG_WARN("fail to call asynchronous sqc rpc", K(sqc), K(timeout_us), K(ret));
          // error_index_ = idx;
        } else {
          LOG_DEBUG("send the sqc request successfully.", K(idx), K(sqc), K(args), K(cb));
        }
      }
      if (OB_FAIL(ret) && cb != NULL) {
        int temp_ret = callbacks_.remove(idx);
        if (temp_ret != OB_SUCCESS) {
          // set callback invalid, which processed by fail_process()
          cb->set_invalid(true);
          LOG_WARN("callback obarray remove element failed", K(ret));
        } else {
          cb->~ObSqcAsyncCB();
          allocator_.free(cb);
          cb = NULL;
        }
      }
    }
  }
  return ret;
}

int ObPxSqcAsyncProxy::wait_all()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("wail all async sqc rpc to end", K(dfo_));
  // break while:
  // 1. get enough callback result
  // 2. timeout , ret = OB_TIMEOUT
  // 3. retry rpc fail
  while (return_cb_count_ < sqcs_.count() && OB_SUCC(ret)) {

    ObThreadCondGuard guard(cond_);
    // wait for timeout or until notified.
    cond_.wait_us(500);

    if ((phy_plan_ctx_->get_timeout_timestamp() - ObTimeUtility::current_time()) < 0) {
      ret = OB_TIMEOUT;
    }

    ARRAY_FOREACH_X(callbacks_, idx, count, OB_SUCC(ret))
    {
      ObSqcAsyncCB& callback = *callbacks_.at(idx);
      if (!callback.is_visited() && callback.is_timeout()) {
        // callback timeout, no need retry
        return_cb_count_++;
        if (phy_plan_ctx_->get_timeout_timestamp() - ObTimeUtility::current_time() > 0) {
          error_index_ = idx;
          ret = OB_RPC_CONNECT_ERROR;
        } else {
          ret = OB_TIMEOUT;
        }
        callback.set_visited(true);
      } else if (!callback.is_visited() && callback.is_invalid()) {
        // rpc decode fail, on_invalid() will be called, need no retry
        return_cb_count_++;
        ret = OB_RPC_PACKET_INVALID;
        callback.set_visited(true);
      } else if (!callback.is_visited() && callback.is_processed()) {
        return_cb_count_++;
        callback.set_visited(true);
        if (OB_SUCC(callback.get_ret_code().rcode_)) {
          const ObPxRpcInitSqcResponse& cb_result = callback.get_result();
          if (cb_result.rc_ == OB_ERR_INSUFFICIENT_PX_WORKER) {
            // can not acquire enough px worker, no need SQC retry, stmt retry is needed
            LOG_INFO("can't get enough worker resource, and not retry", K(cb_result.rc_), K(*sqcs_.at(idx)));
          }
          if (OB_FAIL(cb_result.rc_)) {
            if (is_data_not_readable_err(ret)) {
              error_index_ = idx;
            }
          } else {
            results_.at(idx) = &cb_result;
          }
          sqcs_.at(idx)->set_need_report(true);
        } else {
          // RPC framework error, need no retry
          ret = callback.get_ret_code().rcode_;
          LOG_WARN("call rpc failed", K(ret), K(callback.get_ret_code()));
        }
      }

      if (callback.need_retry() && OB_SUCC(ret)) {
        // need retry the task.
        // reset: visit, eturn_cb_count_
        callback.set_visited(false);
        return_cb_count_--;
        if (check_for_retry(callback)) {
          callback.reset();
          if (OB_FAIL(launch_one_rpc_request(idx, &callback))) {
            LOG_WARN("retrying to send sqc rpc failed");
          }
        }
      }
    }
  }

  if (return_cb_count_ < callbacks_.count()) {
    // hash unfinished callback, need wait all callback finish
    fail_process();
  }
  return ret;
}

void ObPxSqcAsyncProxy::destroy()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("async sqc proxy deconstruct, the callbacklist is ", K(callbacks_));
  ARRAY_FOREACH(callbacks_, idx)
  {
    ObSqcAsyncCB* callback = callbacks_.at(idx);
    LOG_DEBUG("async sqc proxy deconstruct, the callback status is ", K(idx), K(*callback));
    callback->~ObSqcAsyncCB();
  }
  allocator_.reuse();
  callbacks_.reuse();
  results_.reuse();
}

bool ObPxSqcAsyncProxy::check_for_retry(ObSqcAsyncCB& callback)
{
  bool retry = false;
  int64_t timeout_us = phy_plan_ctx_->get_timeout_timestamp() - ObTimeUtility::current_time();
  int64_t send_duration = ObTimeUtility::current_time() - callback.get_send_ts();
  // avoid retry too mutch
  if (timeout_us >= 100 * 1000L && send_duration >= 10 * 1000L) {
    retry = true;
  }
  return retry;
}

void ObPxSqcAsyncProxy::fail_process()
{
  LOG_WARN("async sqc fails, process the callbacks that have not yet got results",
      K(return_cb_count_),
      K(callbacks_.count()));
  while (return_cb_count_ < callbacks_.count()) {
    ObThreadCondGuard guard(cond_);
    ARRAY_FOREACH_X(callbacks_, idx, count, true)
    {
      ObSqcAsyncCB& callback = *callbacks_.at(idx);
      if (!callback.is_visited()) {
        if (callback.is_processed() || callback.is_timeout() || callback.is_invalid()) {
          return_cb_count_++;
          LOG_DEBUG("async sql fails, wait all callbacks", K(return_cb_count_), K(callbacks_.count()));
          callback.set_visited(true);
        }
        // same condition with ObPxSqcAsyncProxy::wait_all().
        if (callback.get_ret_code().rcode_ == OB_SUCCESS && callback.is_processed()) {
          sqcs_.at(idx)->set_need_report(true);
        }
      }
    }
    cond_.wait_us(500);
  }
  LOG_WARN("async sqc fails, all callbacks have been processed");
}

}  // namespace sql
}  // namespace oceanbase
