/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/dag/ob_table_load_dag.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{
using namespace share;
using namespace storage;
using namespace common;

struct ObTableLoadDagInitParam final : public ObIDagInitParam
{
public:
  bool is_valid() const override { return true; }
};

class ObTableLoadDag::StartDagProcessor : public ObITableLoadTaskProcessor
{
public:
  StartDagProcessor(ObTableLoadTask &task, ObTableLoadStoreCtx *store_ctx, ObTableLoadDag *dag)
    : ObITableLoadTaskProcessor(task), store_ctx_(store_ctx), dag_(dag)
  {
    store_ctx_->ctx_->inc_ref_count();
  }
  virtual ~StartDagProcessor() { ObTableLoadService::put_ctx(store_ctx_->ctx_); }
  int process() override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(dag_->process())) {
      LOG_WARN("fail to process", KR(ret));
    } else {
      ret = dag_->get_dag_ret();
    }
    return ret;
  }

private:
  ObTableLoadStoreCtx *store_ctx_;
  ObTableLoadDag *dag_;
};

class ObTableLoadDag::StartDagCallback : public ObITableLoadTaskCallback
{
public:
  StartDagCallback(ObTableLoadStoreCtx *store_ctx) : store_ctx_(store_ctx)
  {
    store_ctx_->ctx_->inc_ref_count();
  }
  virtual ~StartDagCallback() { ObTableLoadService::put_ctx(store_ctx_->ctx_); }
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ret_code)) {
      store_ctx_->set_status_error(ret);
    }
    store_ctx_->ctx_->free_task(task);
  }

private:
  ObTableLoadStoreCtx *store_ctx_;
};

ObTableLoadDag::ObTableLoadDag() : store_ctx_(nullptr), is_inited_(false) {}

ObTableLoadDag::~ObTableLoadDag() { reset(); }

int ObTableLoadDag::init(ObTableLoadStoreCtx *store_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadDag init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(store_ctx));
  } else {
    ObTableLoadDagInitParam init_param;
    const ObDagId *cur_dag_id = ObCurTraceId::get_trace_id();
    if (OB_FAIL(ObIndependentDag::init(&init_param, cur_dag_id))) {
      LOG_WARN("fail to init dag", K(ret));
    } else {
      store_ctx_ = store_ctx;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadDag::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDag not init", KR(ret), KP(this));
  } else {
    for (int64_t thread_idx = 0; OB_SUCC(ret) && thread_idx < store_ctx_->thread_cnt_;
         ++thread_idx) {
      ObTableLoadTask *task = nullptr;
      // 1. 分配task
      if (OB_FAIL(store_ctx_->ctx_->alloc_task(task))) {
        LOG_WARN("fail to alloc task", KR(ret));
      }
      // 2. 设置processor
      else if (OB_FAIL(task->set_processor<StartDagProcessor>(store_ctx_, this))) {
        LOG_WARN("fail to set start dag task processor", KR(ret));
      }
      // 3. 设置callback
      else if (OB_FAIL(task->set_callback<StartDagCallback>(store_ctx_))) {
        LOG_WARN("fail to set start dag task callback", KR(ret));
      }
      // 4. 把task放入调度器
      else if (OB_FAIL(store_ctx_->dag_task_scheduler_->add_task(thread_idx, task))) {
        LOG_WARN("fail to add task", KR(ret), K(thread_idx), KPC(task));
      }
      if (OB_FAIL(ret)) {
        if (nullptr != task) {
          store_ctx_->ctx_->free_task(task);
          task = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDag::check_status()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ret_code_)) {
    LOG_WARN("dag has error", KR(ret));
  }
  return ret;
}

void ObTableLoadDag::clear_task()
{
  ObIndependentDag::clear_task_list();
}

} // namespace observer
} // namespace oceanbase
