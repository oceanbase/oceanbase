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
#include "ob_px_worker.h"
#include "lib/time/ob_time_utility.h"
#include "sql/engine/px/ob_px_worker_stat.h"
#include "sql/engine/px/ob_px_interruption.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/px/ob_px_admission.h"
#include "observer/omt/ob_tenant.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_context.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;
using namespace oceanbase::lib;
using namespace oceanbase::share;


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

ObPxRpcWorker::ObPxRpcWorker(const observer::ObGlobalContext &gctx,
                             obrpc::ObPxRpcProxy &rpc_proxy,
                             common::ObIAllocator &alloc)
  : gctx_(gctx),
    rpc_proxy_(rpc_proxy),
    alloc_(alloc)
{
}

ObPxRpcWorker::~ObPxRpcWorker()
{
}

int ObPxRpcWorker::run(ObPxRpcInitTaskArgs &arg)
{
  int ret = OB_SUCCESS;
  // 50ms 以内必须分配出 task 线程，若排队时间超过 50ms，则失败回退到 1 个线程
  int64_t timeout_us = 50 * 1000;
  ret = rpc_proxy_
      .to(arg.task_.get_exec_addr())
      .by(THIS_WORKER.get_rpc_tenant())
      .timeout(timeout_us)
      .init_task(arg, resp_);
  return ret;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

ObPxCoroWorker::ObPxCoroWorker(const observer::ObGlobalContext &gctx,
                               common::ObIAllocator &alloc)
  : gctx_(gctx),
    alloc_(alloc),
    exec_ctx_(alloc, gctx_.session_mgr_),
    phy_plan_(),
    task_arg_(),
    task_proc_(gctx, task_arg_),
    task_co_id_(0)
{
}

int ObPxCoroWorker::run(ObPxRpcInitTaskArgs &arg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(deep_copy_assign(arg, task_arg_))) {
    LOG_WARN("fail deep copy assign arg", K(arg), K(ret));
  } else {
  }
  return ret;
}

int ObPxCoroWorker::exit()
{
  int ret = OB_SUCCESS;
  ret = OB_NOT_INIT;
  return ret;
}

int ObPxCoroWorker::deep_copy_assign(const ObPxRpcInitTaskArgs &src,
                                     ObPxRpcInitTaskArgs &dest)
{
  int ret = OB_SUCCESS;
  dest.set_deserialize_param(exec_ctx_, phy_plan_, &alloc_);
  // 深拷贝 arg 中所有元素，入session、op tree 等
  // 暂时通过序列化+反序列化完成
  int64_t ser_pos = 0;
  int64_t des_pos = 0;
  void *ser_ptr = NULL;
  int64_t ser_arg_len = src.get_serialize_size();

  if (OB_ISNULL(ser_ptr = alloc_.alloc(ser_arg_len))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail alloc memory", K(ser_arg_len), KP(ser_ptr), K(ret));
  } else if (OB_FAIL(src.serialize(static_cast<char *>(ser_ptr), ser_arg_len, ser_pos))) {
    LOG_WARN("fail serialize init task arg", KP(ser_ptr), K(ser_arg_len), K(ser_pos), K(ret));
  } else if (OB_FAIL(dest.deserialize(static_cast<const char *>(ser_ptr), ser_pos, des_pos))) {
    LOG_WARN("fail des task arg", KP(ser_ptr), K(ser_pos), K(des_pos), K(ret));
  } else if (ser_pos != des_pos) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("data_len and pos mismatch", K(ser_arg_len), K(ser_pos), K(des_pos), K(ret));
  } else {
    // PLACE_HOLDER: if want to shared trans_desc
    // dest.exec_ctx_->get_my_session()->set_effective_trans_desc(src.exec_ctx_->get_my_session()->get_effective_trans_desc());
  }
  return ret;
}



//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
class SQCHandlerGuard
{
public:
  SQCHandlerGuard(ObPxSqcHandler *h) : sqc_handler_(h)
  {
    if (OB_LIKELY(sqc_handler_)) {
      sqc_handler_->get_notifier().worker_start(GETTID());
    }
  }
  ~SQCHandlerGuard()
  {
    if (OB_LIKELY(sqc_handler_)) {
      sqc_handler_->worker_end_hook();
      int report_ret = OB_SUCCESS;
      ObPxSqcHandler::release_handler(sqc_handler_, report_ret);
      sqc_handler_ = nullptr;
    }
  }
private:
  ObPxSqcHandler *sqc_handler_;
};

void PxWorkerFunctor::operator ()(bool need_exec)
{
  int ret = OB_SUCCESS;
  ObCurTraceId::set(env_arg_.get_trace_id());
  /**
   * 中断必须覆盖到release handler，因为它的流程含有sqc向qc发送消息，
   * 需要check中断。而中断本身是线程局部，不依赖于租户空间才对。
   */
  ObPxInterruptGuard px_int_guard(task_arg_.task_.get_interrupt_id().px_interrupt_id_);
  ObPxSqcHandler *sqc_handler = task_arg_.get_sqc_handler();
  SQCHandlerGuard sqc_handler_guard(sqc_handler);
  lib::MemoryContext mem_context = nullptr;
  //ensure PX worker skip updating timeout_ts_ by ntp offset
  THIS_WORKER.set_ntp_offset(0);
  if (!need_exec) {
    LOG_INFO("px pool already stopped, do not execute the task.");
  } else if (OB_FAIL(px_int_guard.get_interrupt_reg_ret())) {
    LOG_WARN("px worker failed to SET_INTERRUPTABLE");
  } else if (OB_NOT_NULL(sqc_handler) && OB_LIKELY(!sqc_handler->has_interrupted())) {
    THIS_WORKER.set_worker_level(sqc_handler->get_rpc_level());
    THIS_WORKER.set_curr_request_level(sqc_handler->get_rpc_level());
    LOG_TRACE("init flt ctx", K(sqc_handler->get_flt_ctx()));
    if (sqc_handler->get_flt_ctx().trace_id_.is_inited()) {
      OBTRACE->init(sqc_handler->get_flt_ctx());
    }
    FLTSpanGuard(px_task);

    FLT_SET_TAG(task_id, task_arg_.task_.get_task_id(),
                dfo_id, task_arg_.task_.get_dfo_id(),
                sqc_id, task_arg_.task_.get_sqc_id(),
                qc_id, task_arg_.task_.get_qc_id(),
                group_id, THIS_WORKER.get_group_id());
    // Do not set thread local log level while log level upgrading (OB_LOGGER.is_info_as_wdiag)
    if (OB_LOGGER.is_info_as_wdiag()) {
      ObThreadLogLevelUtils::clear();
    } else {
      if (OB_LOG_LEVEL_NONE != env_arg_.get_log_level()) {
        ObThreadLogLevelUtils::init(env_arg_.get_log_level());
      }
    }
    THIS_WORKER.set_group_id(env_arg_.get_group_id());
    // When deserialize expr, sql mode will affect basic function of expr.
    CompatModeGuard mode_guard(env_arg_.is_oracle_mode() ? Worker::CompatMode::ORACLE : Worker::CompatMode::MYSQL);
    MTL_SWITCH(sqc_handler->get_tenant_id()) {
      CREATE_WITH_TEMP_ENTITY(RESOURCE_OWNER, sqc_handler->get_tenant_id()) {
        if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(mem_context,
            lib::ContextParam().set_mem_attr(MTL_ID(), ObModIds::OB_SQL_PX)))) {
          LOG_WARN("create memory entity failed", K(ret));
        } else {
          WITH_CONTEXT(mem_context) {
            lib::ContextTLOptGuard guard(true);
            // 在worker线程中进行args的deep copy，分担sqc的线程的负担。
            ObPxRpcInitTaskArgs runtime_arg;
            if (OB_FAIL(runtime_arg.init_deserialize_param(mem_context, *env_arg_.get_gctx()))) {
              LOG_WARN("fail to init args", K(ret));
            } else if (OB_FAIL(runtime_arg.deep_copy_assign(task_arg_, mem_context->get_arena_allocator()))) {
              (void) ObInterruptUtil::interrupt_qc(task_arg_.task_, ret, task_arg_.exec_ctx_);
              LOG_WARN("fail deep copy assign arg", K(task_arg_), K(ret));
            } else {
              // 绑定sqc_handler，方便算子任何地方都可以拿sqc_handle
              runtime_arg.sqc_handler_ = sqc_handler;
            }

            // 执行
            ObPxTaskProcess worker(*env_arg_.get_gctx(), runtime_arg);
            worker.set_is_oracle_mode(env_arg_.is_oracle_mode());
            if (OB_SUCC(ret)) {
              worker.run();
            }
            runtime_arg.destroy();
          }
        }
      }
      if (nullptr != mem_context) {
        DESTROY_CONTEXT(mem_context);
        mem_context = NULL;
      }
      auto *pm = common::ObPageManager::thread_local_instance();
      if (OB_LIKELY(nullptr != pm)) {
        if (pm->get_used() != 0) {
          LOG_ERROR("page manager's used should be 0, unexpected!!!", KP(pm));
        }
      }
    }
    ObThreadLogLevelUtils::clear();
  } else if (OB_ISNULL(sqc_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected null sqc handler", K(sqc_handler));
  } else {
    LOG_WARN("already interrupted");
  }

  if (OB_ISNULL(sqc_handler)) {
    // do nothing
  } else if (sqc_handler->get_flt_ctx().trace_id_.is_inited()) {
    OBTRACE->reset();
  }
  PxWorkerFinishFunctor on_func_finish;
  on_func_finish();
  ObCurTraceId::reset();
}

void PxWorkerFinishFunctor::operator ()()
{
  // 每个 worker 结束后，都释放一个槽位
  ObPxSubAdmission::release(1);
  ObActiveSessionGuard::setup_default_ash();
}


ObPxThreadWorker::ObPxThreadWorker(const observer::ObGlobalContext &gctx)
  : gctx_(gctx),
    task_co_id_(0)
{
}

ObPxThreadWorker::~ObPxThreadWorker()
{
}

// 在 group 对应的 px_pool 中执行
int ObPxThreadWorker::run(ObPxRpcInitTaskArgs &task_arg)
{
  int ret = OB_SUCCESS;
  int64_t group_id = THIS_WORKER.get_group_id();
  omt::ObPxPools* px_pools = MTL(omt::ObPxPools*);
  if (OB_ISNULL(px_pools)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail get px pools", K(ret));
  } else {
    omt::ObPxPool *pool = nullptr;
    if (OB_FAIL(px_pools->get_or_create(group_id, pool))) {
      LOG_WARN("fail get px pool", K(group_id), K(ret));
    } else if (OB_FAIL(run_at(task_arg, *pool))) {
      LOG_WARN("fail sched worker thread", K(ret));
    }
  }
  return ret;
}

int ObPxThreadWorker::run_at(ObPxRpcInitTaskArgs &task_arg, omt::ObPxPool &px_pool)
{
  int ret = OB_SUCCESS;
  int retry_times = 0;
  ObPxWorkerEnvArgs env_args;

  env_args.set_enqueue_timestamp(ObTimeUtility::current_time());
  env_args.set_trace_id(*ObCurTraceId::get_trace_id());
  env_args.set_is_oracle_mode(lib::is_oracle_mode());
  env_args.set_gctx(&gctx_);
  env_args.set_group_id(THIS_WORKER.get_group_id());
  if (OB_LOG_LEVEL_NONE != common::ObThreadLogLevelUtils::get_level()) {
    env_args.set_log_level(common::ObThreadLogLevelUtils::get_level());
  }

  PxWorkerFunctor func(env_args, task_arg);
  /*
   * 将 task 提交到 px pool
   * 如果 px pool 内线程不足，则会扩容 pool，直至能容纳下这个 task
   */
  if (OB_SUCC(ret)) {
    do {
      if (OB_FAIL(px_pool.submit(func))) {
        if (retry_times++ % 10 == 0) {
          LOG_WARN("fail submit task, will allocate thread and do inplace retry",
                   K(retry_times), K(ret));
        }
        if (OB_SIZE_OVERFLOW == ret) {
          // 线程不够，动态增加线程并重试
          int tmp_ret = px_pool.inc_thread_count(1);
          if (OB_SUCCESS != tmp_ret) {
            LOG_WARN("fail increase thread count. abort!", K(tmp_ret), K(ret));
            ret = tmp_ret;
            break;
          }
        }
        ob_usleep(5000);
      }
    } while (OB_SIZE_OVERFLOW == ret);
  }
  if (OB_FAIL(ret)) {
    LOG_ERROR("Failed to submit px func to thread pool",
              K(retry_times), "px_pool_size", px_pool.get_pool_size(),  K(ret));
  }
  LOG_DEBUG("submit px worker to poll", K(env_args.is_oracle_mode()), K(ret));
  return ret;
}

int ObPxThreadWorker::exit()
{
  // SQC will wait all PxWorker finish.
  // Just return success.
  return OB_SUCCESS;
}

int ObPxLocalWorker::run(ObPxRpcInitTaskArgs &task_arg)
{
  int ret = OB_SUCCESS;
  ObPxSqcHandler *h = task_arg.get_sqc_handler();

  if (OB_ISNULL(h)) {
  } else if (h->get_flt_ctx().trace_id_.is_inited()) {
    OBTRACE->init(h->get_flt_ctx());
  }

  {
    FLTSpanGuard(px_task);
    ObPxTaskProcess task_proc(gctx_, task_arg);
    ret = task_proc.process();
  }

  if (OB_ISNULL(h)) {
  } else if (h->get_flt_ctx().trace_id_.is_inited()) {
    OBTRACE->reset();
  }
  return ret;
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

ObPxRpcWorker * ObPxRpcWorkerFactory::create_worker()
{
  ObPxRpcWorker *worker = NULL;
  void *ptr = alloc_.alloc(sizeof(ObPxRpcWorker));
  if (OB_NOT_NULL(ptr)) {
    worker = new(ptr)ObPxRpcWorker(gctx_,
                                   rpc_proxy_,
                                   alloc_);
    if (OB_SUCCESS != workers_.push_back(worker)) {
      worker->~ObPxRpcWorker();
      worker = NULL;
    }
  }
  return worker;
}

void ObPxRpcWorkerFactory::destroy()
{
  for (int64_t i = 0; i < workers_.count(); ++i) {
    workers_.at(i)->~ObPxRpcWorker();
  }
  workers_.reset();
}

ObPxRpcWorkerFactory::~ObPxRpcWorkerFactory()
{
  destroy();
}


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////

ObPxThreadWorker * ObPxThreadWorkerFactory::create_worker()
{
  ObPxThreadWorker *worker = NULL;
  int ret = OB_SUCCESS;
  void *ptr = alloc_.alloc(sizeof(ObPxThreadWorker));
  if (OB_NOT_NULL(ptr)) {
    worker = new(ptr)ObPxThreadWorker(gctx_);
    if (OB_FAIL(workers_.push_back(worker))) {
      LOG_WARN("array push back failed", K(ret));
    }
    if (OB_SUCCESS != ret) {
      worker->~ObPxThreadWorker();
      worker = NULL;
    }
  }
  return worker;
}

int ObPxThreadWorkerFactory::join()
{
  int ret = OB_SUCCESS;
  int eret = OB_SUCCESS;
  for (int64_t i = 0; i < workers_.count(); ++i) {
    if (OB_SUCCESS != (eret = workers_.at(i)->exit())) {
      ret = eret; // try join as many workers as possible, return last error
      LOG_ERROR("fail join px thread workers", K(ret));
    }
  }
  return ret;
}

void ObPxThreadWorkerFactory::destroy()
{
  for (int64_t i = 0; i < workers_.count(); ++i) {
    workers_.at(i)->~ObPxThreadWorker();
  }
  workers_.reset();
}

ObPxThreadWorkerFactory::~ObPxThreadWorkerFactory()
{
  destroy();
}

//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////


ObPxCoroWorker * ObPxCoroWorkerFactory::create_worker()
{
  ObPxCoroWorker *worker = NULL;
  void *ptr = alloc_.alloc(sizeof(ObPxCoroWorker));
  if (OB_NOT_NULL(ptr)) {
    worker = new(ptr)ObPxCoroWorker(gctx_,
                                   alloc_);
    if (OB_SUCCESS != workers_.push_back(worker)) {
      worker->~ObPxCoroWorker();
      worker = NULL;
    }
  }
  return worker;
}

int ObPxCoroWorkerFactory::join()
{
  int ret = OB_SUCCESS;
  int eret = OB_SUCCESS;
  for (int64_t i = 0; i < workers_.count(); ++i) {
    if (OB_SUCCESS != (eret = workers_.at(i)->exit())) {
      ret = eret; // try join as many workers as possible, return last error
      LOG_ERROR("fail join coroutine", K(ret));
    }
  }
  return ret;
}

void ObPxCoroWorkerFactory::destroy()
{
  for (int64_t i = 0; i < workers_.count(); ++i) {
    workers_.at(i)->~ObPxCoroWorker();
  }
}

ObPxCoroWorkerFactory::~ObPxCoroWorkerFactory()
{
  destroy();
}


//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////


ObPxWorkerRunnable *ObPxLocalWorkerFactory::create_worker()
{
  return &worker_;
}

void ObPxLocalWorkerFactory::destroy()
{
}

ObPxLocalWorkerFactory::~ObPxLocalWorkerFactory()
{
  destroy();
}



//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////
int ObPxWorker::check_status()
{
  int ret = OB_SUCCESS;
  if (nullptr != session_) {
    session_->is_terminate(ret);
  }

  if (OB_SUCC(ret)) {
    if (is_timeout()) {
      ret = OB_TIMEOUT;
    } else if (IS_INTERRUPTED()) {
      ObInterruptCode &ic = GET_INTERRUPT_CODE();
      ret = ic.code_;
      LOG_WARN("px execution was interrupted", K(ic), K(ret));
    }
  }
  return ret;
}
