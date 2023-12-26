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

#ifndef __OB_SQL_ENGINE_PX_WORKER_RUNNABLE_H__
#define __OB_SQL_ENGINE_PX_WORKER_RUNNABLE_H__

#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/px/ob_dfo.h"
#include "sql/engine/px/ob_px_rpc_proxy.h"
#include "sql/engine/px/ob_px_task_process.h"
#include "sql/engine/ob_des_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/lock/ob_scond.h"

namespace oceanbase
{
namespace omt { class ObPxPool; }
namespace sql
{
class ObPxWorkerRunnable
{
public:
  virtual int run(ObPxRpcInitTaskArgs &arg) = 0;
};

// 使用 RPC 工作线程作为 Px Worker 的执行容器
class ObPxRpcWorker: public ObPxWorkerRunnable
{
public:
  ObPxRpcWorker(const observer::ObGlobalContext &gctx,
                obrpc::ObPxRpcProxy &rpc_proxy,
                common::ObIAllocator &alloc);
  virtual ~ObPxRpcWorker();
  int run(ObPxRpcInitTaskArgs &arg);
  uint64_t get_task_co_id() { return resp_.task_co_id_; }
  TO_STRING_KV(K_(resp));
private:
  DISABLE_WARNING_GCC_PUSH
  DISABLE_WARNING_GCC_ATTRIBUTES
  const observer::ObGlobalContext &gctx_ __maybe_unused;
  obrpc::ObPxRpcProxy &rpc_proxy_;
  common::ObIAllocator &alloc_ __maybe_unused;
  DISABLE_WARNING_GCC_POP
  ObPxRpcInitTaskResponse resp_;
};

// 使用协程作为 Px Worker 的执行容器
class ObPxCoroWorker : public ObPxWorkerRunnable
{
public:
  ObPxCoroWorker(const observer::ObGlobalContext &gctx,
                 common::ObIAllocator &alloc);
  virtual ~ObPxCoroWorker() = default;
  int run(ObPxRpcInitTaskArgs &arg);
  int exit();
  uint64_t get_task_co_id() { return task_co_id_; }
  TO_STRING_KV(K_(task_co_id));
private:
  int deep_copy_assign(const ObPxRpcInitTaskArgs &src,
                       ObPxRpcInitTaskArgs &dest);
  /* variables */
  const observer::ObGlobalContext &gctx_;
  common::ObIAllocator &alloc_;
  sql::ObDesExecContext exec_ctx_;
  sql::ObPhysicalPlan phy_plan_;
  ObPxRpcInitTaskArgs task_arg_;
  ObPxTaskProcess task_proc_;
  uint64_t task_co_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPxCoroWorker);
};

// Px Worker 的执行容器
class ObPxThreadWorker : public ObPxWorkerRunnable
{
public:
  ObPxThreadWorker(const observer::ObGlobalContext &gctx);
  virtual ~ObPxThreadWorker();

  virtual int run(ObPxRpcInitTaskArgs &arg) override;
  int exit();
  uint64_t get_task_co_id() { return task_co_id_; }

  TO_STRING_KV(K_(task_co_id));
private:
  int run_at(ObPxRpcInitTaskArgs &task_arg, omt::ObPxPool &px_pool);
private:
  /* variables */
  const observer::ObGlobalContext &gctx_;
  uint64_t task_co_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPxThreadWorker);
};

class ObPxLocalWorker : public ObPxWorkerRunnable
{
public:
  ObPxLocalWorker(const observer::ObGlobalContext &gctx) : gctx_(gctx) {}
  virtual ~ObPxLocalWorker() = default;
  virtual int run(ObPxRpcInitTaskArgs &arg) override;
private:
  const observer::ObGlobalContext &gctx_;
};

// Worker 工厂，封装 Worker 的分配，便于管理资源
class ObPxRpcWorkerFactory
{
public:
  ObPxRpcWorkerFactory(const observer::ObGlobalContext &gctx,
                          obrpc::ObPxRpcProxy &rpc_proxy,
                          common::ObIAllocator &alloc)
      : gctx_(gctx),
        rpc_proxy_(rpc_proxy),
        alloc_(alloc)
  {}
  virtual ~ObPxRpcWorkerFactory();
  ObPxRpcWorker *create_worker();
  int join() { return common::OB_SUCCESS; }
private:
  void destroy();
private:
  const observer::ObGlobalContext &gctx_;
  obrpc::ObPxRpcProxy &rpc_proxy_;
  common::ObIAllocator &alloc_;
  common::ObSEArray<ObPxRpcWorker *, 64> workers_;
};

class ObPxThreadWorkerFactory
{
public:
  ObPxThreadWorkerFactory(const observer::ObGlobalContext &gctx,
                       common::ObIAllocator &alloc)
      : gctx_(gctx), alloc_(alloc)
  {}
  virtual ~ObPxThreadWorkerFactory();
  ObPxThreadWorker *create_worker();
  int join();
private:
  void destroy();
private:
  const observer::ObGlobalContext &gctx_;
  common::ObIAllocator &alloc_;
  common::ObSEArray<ObPxThreadWorker *, 64> workers_;
};

class ObPxCoroWorkerFactory
{
public:
  ObPxCoroWorkerFactory(const observer::ObGlobalContext &gctx,
                       common::ObIAllocator &alloc)
      : gctx_(gctx), alloc_(alloc)
  {}
  virtual ~ObPxCoroWorkerFactory();
  ObPxCoroWorker *create_worker();
  int join();
private:
  void destroy();
private:
  const observer::ObGlobalContext &gctx_;
  common::ObIAllocator &alloc_;
  common::ObSEArray<ObPxCoroWorker *, 64> workers_;
};

class ObPxLocalWorkerFactory
{
public:
  ObPxLocalWorkerFactory(const observer::ObGlobalContext &gctx,
                       common::ObIAllocator &alloc)
      : gctx_(gctx), alloc_(alloc), worker_(gctx)
  {}
  virtual ~ObPxLocalWorkerFactory();
  ObPxWorkerRunnable *create_worker();
private:
  void destroy();
private:
  DISABLE_WARNING_GCC_PUSH
  DISABLE_WARNING_GCC_ATTRIBUTES
  const observer::ObGlobalContext &gctx_ __maybe_unused;
  common::ObIAllocator &alloc_ __maybe_unused;
  DISABLE_WARNING_GCC_POP
  ObPxLocalWorker worker_;
};


class PxWorkerFunctor {
public:
  explicit PxWorkerFunctor(ObPxWorkerEnvArgs &env_arg, ObPxRpcInitTaskArgs &task_arg) {
    env_arg_ = env_arg;
    task_arg_ = task_arg;
  }
  ~PxWorkerFunctor() = default;

  // px thread will invoke this function.
  void operator ()(bool need_exec);

  PxWorkerFunctor &operator = (const PxWorkerFunctor &other) {
    if (&other != this) {
      env_arg_ = other.env_arg_;
      task_arg_ = other.task_arg_;
    }
    return *this;
  }
  ObPxWorkerEnvArgs env_arg_;
  ObPxRpcInitTaskArgs task_arg_;
};

class PxWorkerFinishFunctor {
public:
  explicit PxWorkerFinishFunctor() {
  }
  ~PxWorkerFinishFunctor() = default;

  // px thread will invoke this function.
  void operator ()();

  PxWorkerFinishFunctor &operator = (const PxWorkerFinishFunctor &other) {
    UNUSED(other);
    return *this;
  }
};

class ObPxWorker : public lib::Worker
{
public:
  virtual int check_status() override;
};

}
}
#endif /* __OB_SQL_ENGINE_PX_WORKER_RUNNABLE_H__ */
//// end of header file
