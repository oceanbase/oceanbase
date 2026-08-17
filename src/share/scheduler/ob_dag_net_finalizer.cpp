/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "share/scheduler/ob_dag_net_finalizer.h"
#include "storage/compaction/ob_compaction_diagnose.h"

namespace oceanbase
{
using namespace common;
using namespace lib;

namespace share
{

void ObIDagNet::ObDagNetFinalizerInfo::reset()
{
  const ObDagNetFinalizerState current_state = state_;
  if (OB_UNLIKELY(DAG_NET_FINALIZER_NONE != current_state
      && DAG_NET_FINALIZER_DONE != current_state)) {
    COMMON_LOG_RET(ERROR, OB_STATE_NOT_MATCH,
        "refuse to reset active dag net finalizer info",
        K(current_state));
    ob_abort();
  }
  priority_ = ObDagPrio::DAG_PRIO_MAX;
  state_ = DAG_NET_FINALIZER_NONE;
  ret_ = OB_SUCCESS;
}

int64_t ObIDagNet::ObDagNetFinalizerInfo::to_string(
    char *buf,
    const int64_t buf_len) const
{
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    const ObDagPrio::ObDagPrioEnum priority = ATOMIC_LOAD_RLX(&priority_);
    const ObDagNetFinalizerState state = ATOMIC_LOAD_ACQ(&state_);
    J_OBJ_START();
    J_KV(K(priority), K(state));
    if (DAG_NET_FINALIZER_DONE == state) {
      const int ret = ret_;
      J_COMMA();
      J_KV(K(ret));
    }
    J_OBJ_END();
  }
  return pos;
}

ObDagPrio::ObDagPrioEnum ObIDagNet::get_dag_net_finalizer_priority() const
{
  return ATOMIC_LOAD_RLX(&dag_net_finalizer_info_.priority_);
}

int ObIDagNet::set_dag_net_finalizer_priority(
    const ObDagPrio::ObDagPrioEnum input_priority)
{
  int ret = OB_SUCCESS;
  const ObDagPrio::ObDagPrioEnum current_priority = get_dag_net_finalizer_priority();
  const ObDagNetFinalizerState current_state = get_dag_net_finalizer_state();
  if (OB_UNLIKELY(!ObDagPrio::is_ha_prio(input_priority))) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid dag net finalizer priority",
        K(ret), K(input_priority));
  } else if (input_priority == current_priority) {
    // Multiple failure paths may enable the same conditional finalizer.
  } else if (OB_UNLIKELY(ObDagPrio::DAG_PRIO_MAX != current_priority
      || DAG_NET_FINALIZER_NONE != current_state)) {
    ret = OB_STATE_NOT_MATCH;
    COMMON_LOG(WARN, "unexpected dag net finalizer info",
        K(ret), K(input_priority), K(current_priority), K(current_state));
  } else {
    // Priority writers are serialized; relaxed only protects diagnostic reads.
    ATOMIC_STORE_RLX(&dag_net_finalizer_info_.priority_, input_priority);
  }
  return ret;
}

void ObIDagNet::enable_dag_net_finalizer_on_failure(
    const int failure_ret,
    const ObDagPrio::ObDagPrioEnum priority)
{
  if (OB_UNLIKELY(OB_SUCCESS != failure_ret)) {
    const int ret = set_dag_net_finalizer_priority(priority);
    if (OB_SUCCESS != ret) {
      COMMON_LOG(WARN, "failed to enable dag net finalizer on failure",
          K(ret), K(failure_ret), K(priority));
    }
  }
}

ObIDagNet::ObDagNetFinalizerState ObIDagNet::get_dag_net_finalizer_state() const
{
  return ATOMIC_LOAD_ACQ(&dag_net_finalizer_info_.state_);
}

int ObIDagNet::get_dag_net_finalizer_ret() const
{
  return dag_net_finalizer_info_.ret_;
}

void ObIDagNet::set_dag_net_finalizer_submitted()
{
  // Only the DagScheduler thread submits carriers, but add_dag() may expose
  // one to try_switch() before it returns.  DONE is therefore an expected old
  // state here and must never be overwritten by SUBMITTED.
  const ObDagNetFinalizerState current_state =
      ATOMIC_VCAS_AR(&dag_net_finalizer_info_.state_,
          DAG_NET_FINALIZER_NONE, DAG_NET_FINALIZER_SUBMITTED);
  if (OB_UNLIKELY(DAG_NET_FINALIZER_NONE != current_state
      && DAG_NET_FINALIZER_DONE != current_state)) {
    COMMON_LOG_RET(ERROR, OB_STATE_NOT_MATCH,
        "unexpected state after submitting dag net finalizer",
        K(current_state));
    ob_abort();
  }
}

bool ObIDagNet::finish_dag_net_finalizer(const int ret)
{
  bool finished = false;
  const ObDagNetFinalizerState current_state = get_dag_net_finalizer_state();
  if (DAG_NET_FINALIZER_NONE == current_state
      || DAG_NET_FINALIZER_SUBMITTED == current_state) {
    dag_net_finalizer_info_.ret_ = ret;
    // Publish ret_ with DONE.  The scheduler may reclaim the DagNet as soon as
    // it observes DONE, so this store must be this carrier's final access to it.
    ATOMIC_STORE_REL(&dag_net_finalizer_info_.state_, DAG_NET_FINALIZER_DONE);
    finished = true;
  }
  if (!finished) {
    COMMON_LOG_RET(ERROR, OB_STATE_NOT_MATCH,
        "unexpected dag net finalizer state transition",
        "expected_state", "NONE or SUBMITTED",
        K(current_state), "target_state", DAG_NET_FINALIZER_DONE,
        "finalizer_ret", ret);
  }
  return finished;
}

ObDagNetFinalizerTask::ObDagNetFinalizerTask()
  : ObITask(TASK_TYPE_DAG_NET_FINALIZER)
{
}

int ObDagNetFinalizerTask::init()
{
  return OB_SUCCESS;
}

int ObDagNetFinalizerTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag *dag = get_dag();
  if (OB_ISNULL(dag)
      || ObDagType::DAG_TYPE_DAG_NET_FINALIZER != dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid dag net finalizer task", K(ret), KP(dag));
  } else {
    ret = static_cast<ObDagNetFinalizerDag *>(dag)->process_dag_net_finalizer_();
  }
  return ret;
}

ObDagNetFinalizerDag::ObDagNetFinalizerDag()
  : ObIDag(ObDagType::DAG_TYPE_DAG_NET_FINALIZER),
    owner_dag_net_(nullptr),
    dag_net_type_(ObDagNetType::DAG_NET_TYPE_MAX),
    compat_mode_(lib::Worker::CompatMode::INVALID)
{
}

int ObDagNetFinalizerDag::init(ObIDagNet &dag_net)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(owner_dag_net_)) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "dag net finalizer dag is initialized", K(ret), K(dag_net));
  } else {
    owner_dag_net_ = &dag_net;
    dag_net_type_ = dag_net.get_type();
    compat_mode_ = THIS_WORKER.get_compatibility_mode();
    if (lib::Worker::CompatMode::INVALID == compat_mode_) {
      compat_mode_ = lib::Worker::CompatMode::MYSQL;
    }
  }
  return ret;
}

int ObDagNetFinalizerDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObDagNetFinalizerTask *task = nullptr;
  if (OB_ISNULL(owner_dag_net_)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag net finalizer dag is not initialized", K(ret));
  } else if (OB_FAIL(create_task(nullptr/*parent*/, task))) {
    COMMON_LOG(WARN, "failed to create dag net finalizer task", K(ret), KPC(owner_dag_net_));
  }
  return ret;
}

int ObDagNetFinalizerDag::process_dag_net_finalizer_()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = owner_dag_net_;
  if (OB_ISNULL(dag_net)) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "dag net finalizer dag is not initialized", K(ret));
  } else if (OB_FAIL(dag_net->clear_dag_net_ctx())) {
    COMMON_LOG(WARN, "dag net finalizer attempt failed", K(ret), KPC(dag_net));
  } else {
    COMMON_LOG(TRACE, "dag net finalizer attempt finished", KPC(dag_net));
  }
  return ret;
}

int ObDagNetFinalizerDag::report_result()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = owner_dag_net_;
  // Detach first so a repeated report cannot touch a reclaimed DagNet.
  owner_dag_net_ = nullptr;
  if (OB_NOT_NULL(dag_net)) {
    const int finalizer_ret = get_dag_ret();
    COMMON_LOG(TRACE, "report dag net finalizer carrier", K(finalizer_ret), KPC(dag_net));
    // Publishing DONE may let the scheduler reclaim dag_net immediately.  Do
    // not dereference it after this call.
    if (!dag_net->finish_dag_net_finalizer(finalizer_ret)) {
      COMMON_LOG_RET(ERROR, OB_STATE_NOT_MATCH,
          "failed to finish dag net finalizer", K(finalizer_ret));
    }
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    if (OB_ISNULL(scheduler)) {
      COMMON_LOG_RET(ERROR, OB_ERR_UNEXPECTED,
          "tenant dag scheduler is null after finalizer carrier finished");
    } else {
      scheduler->notify_when_dag_net_finish();
    }
  }
  return ret;
}

int ObDagNetFinalizerDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  typedef compaction::ObDiagnoseInfoParam<1, 0> FinalizerInfoParam;
  FinalizerInfoParam sample;
  void *buf = nullptr;
  FinalizerInfoParam *info_param = nullptr;
  out_param = nullptr;
  if (OB_ISNULL(buf = allocator.alloc(sample.get_deep_copy_size()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate finalizer info param", K(ret));
  } else {
    info_param = new (buf) FinalizerInfoParam();
    info_param->type_.dag_type_ = get_type();
    info_param->struct_type_ = compaction::ObInfoParamStructType::DAG_WARNING_INFO_PARAM;
    info_param->param_int_[0] = static_cast<int64_t>(dag_net_type_);
    out_param = info_param;
  }
  return ret;
}

int ObDagNetFinalizerDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "dag_net_finalizer"))) {
    COMMON_LOG(WARN, "failed to fill dag net finalizer key", K(ret));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
