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
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
namespace sql {
/* ObSqcAsyncCB */
int ObSqcAsyncCB::process() {
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  is_processed_ = true;
  ret = cond_.broadcast();
  return ret;
}

void ObSqcAsyncCB::on_invalid() {
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  is_invalid_ = true;
  ret = cond_.broadcast();
  LOG_WARN("ObSqcAsyncCB invalid, check object serialization impl or oom",
           K(trace_id_), K(ret));
}

void ObSqcAsyncCB::on_timeout() {
  ObThreadCondGuard guard(cond_);
  int ret = OB_SUCCESS;
  is_timeout_ = true;
  ret = cond_.broadcast();
  LOG_WARN("ObSqcAsyncCB timeout, check timeout value, peer cpu load, network "
           "packet drop rate",
           K(trace_id_), K(ret));
}

rpc::frame::ObReqTransport::AsyncCB *
ObSqcAsyncCB::clone(const rpc::frame::SPAlloc &alloc) const {
  UNUSED(alloc);
  return const_cast<rpc::frame::ObReqTransport::AsyncCB *>(
      static_cast<const rpc::frame::ObReqTransport::AsyncCB *const>(this));
}

/* ObPxSqcAsyncProxy */
int ObPxSqcAsyncProxy::launch_all_rpc_request() {
  int ret = OB_SUCCESS;
  // prepare allocate the results_ array
  if (OB_FAIL(results_.prepare_allocate(sqcs_.count()))) {
    LOG_WARN("fail to prepare allocate result array");
  }

  if (OB_SUCC(ret)) {
    int64_t cluster_id = GCONF.cluster_id;
    SMART_VAR(ObPxRpcInitSqcArgs, args) {
      if (sqcs_.count() > 1) {
        args.enable_serialize_cache();
      }
      ARRAY_FOREACH_X(sqcs_, idx, count, OB_SUCC(ret)) {
        if (OB_UNLIKELY(ObPxCheckAlive::is_in_blacklist(sqcs_.at(idx)->get_exec_addr(),
                        session_->get_process_query_time()))) {
          ret = OB_RPC_CONNECT_ERROR;
          LOG_WARN("peer no in communication, maybe crashed", K(ret),
                  KPC(sqcs_.at(idx)), K(cluster_id), K(session_->get_process_query_time()));
        } else {
          ret = launch_one_rpc_request(args, idx, NULL);
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to launch all sqc rpc request", K(ret));
    fail_process();
  }
  return ret;
}

int ObPxSqcAsyncProxy::launch_one_rpc_request(ObPxRpcInitSqcArgs &args, int64_t idx, ObSqcAsyncCB *cb) {
  int ret = OB_SUCCESS;
  ObCurTraceId::TraceId *trace_id = NULL;
  ObPxSqcMeta &sqc = *sqcs_.at(idx);
  const ObAddr &addr = sqc.get_exec_addr();
  int64_t timeout_us =
      phy_plan_ctx_->get_timeout_timestamp() - ObTimeUtility::current_time();
  args.set_serialize_param(exec_ctx_, const_cast<ObOpSpec &>(*dfo_.get_root_op_spec()), *phy_plan_);
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
      void *mem = NULL;
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
                        .by(THIS_WORKER.get_rpc_tenant()?: session_->get_effective_tenant_id())
                        .timeout(timeout_us)
                        .async_init_sqc(args, cb))) {
          LOG_WARN("fail to call asynchronous sqc rpc", K(sqc), K(timeout_us),
                   K(ret));
          // error_index_ = idx;
        } else {
          LOG_DEBUG("send the sqc request successfully.", K(idx), K(sqc),
                    K(args), K(cb));
        }
      }
      // ret为TIME_OUT，或者在重新发送异步rpc的时候失败，都需要把对应的callback回收掉
      // 如果remove对应的callback失败，就不能对callback进行析构
      if (OB_FAIL(ret) && cb != NULL) {
        // 使用temp_ret的原因是需要保留原始 ret 错误码
        int temp_ret = callbacks_.remove(idx);
        if (temp_ret != OB_SUCCESS) {
          // 这里需要将callback标记为无效，等待`fail_process`处理
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

int ObPxSqcAsyncProxy::wait_all() {
  int ret = OB_SUCCESS;
  LOG_TRACE("wail all async sqc rpc to end", K(dfo_));
  // 退出while的条件：3个条件任意满足即退出while循环
  // 1. 在有效时间内获得足够多并且正确的callback结果
  // 2. 超时，ret = OB_TIMEOUT
  // 3. retry一个rpc失败
  while (return_cb_count_ < sqcs_.count() && OB_SUCC(ret)) {

    ObThreadCondGuard guard(cond_);
    // wait for timeout or until notified.
    cond_.wait_us(500);

    if ((phy_plan_ctx_->get_timeout_timestamp() -
         ObTimeUtility::current_time()) < 0) {
      // 超过查询计划的timeout，满足退出条件2
      ret = OB_TIMEOUT;
    }

    ARRAY_FOREACH_X(callbacks_, idx, count, OB_SUCC(ret)) {
      ObSqcAsyncCB &callback = *callbacks_.at(idx);
      if (!callback.is_visited() && callback.is_timeout()) {
        // callback超时，不需要重试
        // 可能只是RPC超时, 但不是QUERY超时, 实现上需要区分
        // 这种情况需要标记为RPC CONNECT ERROR进行重试
        return_cb_count_++;
        if (phy_plan_ctx_->get_timeout_timestamp() -
          ObTimeUtility::current_time() > 0) {
          error_index_ = idx;
          ret = OB_RPC_CONNECT_ERROR;
        } else {
          ret = OB_TIMEOUT;
        }
        callback.set_visited(true);
      } else if (!callback.is_visited() && callback.is_invalid()) {
        // rpc解析pack失败，callback调用on_invalid方法，不需要重试
        return_cb_count_++;
        ret = OB_RPC_PACKET_INVALID;
        callback.set_visited(true);
      } else if (!callback.is_visited() && callback.is_processed()) {
        return_cb_count_++;
        callback.set_visited(true);
        if (OB_SUCC(callback.get_ret_code().rcode_)) {
          const ObPxRpcInitSqcResponse &cb_result = callback.get_result();
          if (cb_result.rc_ == OB_ERR_INSUFFICIENT_PX_WORKER) {
            // 没有获得足够的px worker，不需要再做内部SQC的重试，防止死锁
            // SQC如果没有获得足够的worker，外层直接进行query级别的重试
            //
            LOG_INFO("can't get enough worker resource, and not retry",
                K(cb_result.rc_), K(*sqcs_.at(idx)));
          }
          if (OB_FAIL(cb_result.rc_)) {
            // 错误可能包含 is_data_not_readable_err或者其他类型的错误
            if (is_data_not_readable_err(ret)) {
              error_index_ = idx;
            }
          } else {
            // 获得正确的返回结果
            results_.at(idx) = &cb_result;
          }
        } else {
          // RPC框架错误，直接返回对应的错误码，当前SQC不需要再进行重试
          ret = callback.get_ret_code().rcode_;
          LOG_WARN("call rpc failed", K(ret), K(callback.get_ret_code()));
        }
      }
    }
  }

  // wait_all的结果：
  // 1. sqc对应的所有callback都返回正确的结果，return_cb_count_=sqcs_.count()，直接返回OB_SUCCESS;
  // 2. 由于超时或者重试sqc rpc失败，这种情况下需要等待所有callback响应结束后，才能返回ret。
  if (return_cb_count_ < callbacks_.count()) {
    // 还有未处理完的callback，需要等待所有的callback响应结束才能够退出`wait_all`方法
    fail_process();
  }
  return ret;
}

void ObPxSqcAsyncProxy::destroy() {
  int ret = OB_SUCCESS;
  LOG_DEBUG("async sqc proxy deconstruct, the callbacklist is ", K(callbacks_));
  ARRAY_FOREACH(callbacks_, idx) {
    ObSqcAsyncCB *callback = callbacks_.at(idx);
    LOG_DEBUG("async sqc proxy deconstruct, the callback status is ", K(idx), K(*callback));
    callback->~ObSqcAsyncCB();
  }
  allocator_.reuse();
  callbacks_.reuse();
  results_.reuse();
}

void ObPxSqcAsyncProxy::fail_process() {
  LOG_WARN_RET(OB_SUCCESS,
      "async sqc fails, process the callbacks that have not yet got results",
      K(return_cb_count_), K(callbacks_.count()));
  while (return_cb_count_ < callbacks_.count()) {
    ObThreadCondGuard guard(cond_);
    ARRAY_FOREACH_X(callbacks_, idx, count, true) {
      ObSqcAsyncCB &callback = *callbacks_.at(idx);
      if (!callback.is_visited()) {
        if (callback.is_processed() || callback.is_timeout() || callback.is_invalid()) {
          return_cb_count_++;
          LOG_DEBUG("async sql fails, wait all callbacks", K(return_cb_count_),
              K(callbacks_.count()));
          callback.set_visited(true);
        }
      }
    }
    cond_.wait_us(500);
  }
  LOG_WARN_RET(OB_SUCCESS, "async sqc fails, all callbacks have been processed");
}

} // namespace sql
} // namespace oceanbase
