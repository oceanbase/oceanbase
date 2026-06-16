/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_transfer_parallel_build_tablet_info.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "ob_rebuild_service.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
/******************ObTransferParallelBuildTabletDag*********************/
ObTransferParallelBuildTabletDag::ObTransferParallelBuildTabletDag()
  : ObIDag(ObDagType::DAG_TYPE_TRANSFER_BUILD_TABLET_INFO),
    is_inited_(false),
    ls_id_(),
    ls_handle_(),
    ctx_(nullptr),
    timeout_ctx_(nullptr)
{
}

ObTransferParallelBuildTabletDag::~ObTransferParallelBuildTabletDag()
{
}

bool ObTransferParallelBuildTabletDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTransferParallelBuildTabletDag &other_dag = static_cast<const ObTransferParallelBuildTabletDag&>(other);
    if (ls_id_ != other_dag.ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

uint64_t ObTransferParallelBuildTabletDag::hash() const
{
  uint64_t hash_value = 0;
  hash_value = common::murmurhash(
      &ls_id_, sizeof(ls_id_), hash_value);
  return hash_value;
}

int ObTransferParallelBuildTabletDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag do not init", K(ret));
  } else {
    int64_t pos = 0;
    ret = databuff_print_multi_objs(buf, buf_len, pos, "ObTransferParallelBuildTabletDag: ls_id = ", ls_id_);;
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to fill comment", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObTransferParallelBuildTabletDag::init(
    const share::ObLSID &ls_id,
    ObTransferBuildTabletInfoCtx *ctx,
    ObTimeoutCtx *timeout_ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer parallel build tablet init twice", K(ret));
  } else if (!ls_id.is_valid() || OB_ISNULL(ctx) || OB_ISNULL(timeout_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer parallel build tablet dag get invalid argument", K(ret), K(ls_id), KP(ctx), KP(timeout_ctx));
  } else if (OB_FAIL(set_dag_id(ctx->get_task_id()))) {
    LOG_WARN("failed to set dag id", K(ret), K(ls_id), KPC(ctx));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ls_id, ls_handle_))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else {
    ls_id_ = ls_id;
    ctx_ = ctx;
    timeout_ctx_ = timeout_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTransferParallelBuildTabletDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTransferParallelBuildTabletTask *task = NULL;
  share::ObTransferTabletInfo tablet_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer parallel build tablet dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(tablet_info, ctx_, timeout_ctx_))) {
    LOG_WARN("failed to init tablet rebuild major task", K(ret), KPC(this));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTransferParallelBuildTabletDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();

  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE] = {'\0'};
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer parallele build tablet dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
      static_cast<int64_t>(tenant_id), ls_id_.id(),
      "dag_id", ObCurTraceId::get_trace_id_str(trace_id_buf, sizeof(trace_id_buf))))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObTransferParallelBuildTabletDag::get_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  ls = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer parallel build tablet dag is not init", K(ret));
  } else {
    ls = ls_handle_.get_ls();
  }
  return ret;
}

int64_t ObTransferParallelBuildTabletDag::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls remove member dag do not init", K(ret));
  } else if (FALSE_IT(pos = ObIDag::to_string(buf, buf_len))) {
  } else if (pos >= buf_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("dag to string buffer length is over limit", K(ret), K(pos), K(buf_len));
  }
  return pos;
}

/******************ObTransferParallelBuildTabletTask*********************/
ObTransferParallelBuildTabletTask::ObTransferParallelBuildTabletTask()
  : ObITask(TASK_TYPE_TRANSFER_BUILD_TABLET_INFO),
    is_inited_(false),
    first_tablet_info_(),
    ctx_(nullptr),
    ls_(nullptr),
    timeout_ctx_(nullptr)
{
}

ObTransferParallelBuildTabletTask::~ObTransferParallelBuildTabletTask()
{
  if (is_inited_) {
    ctx_->dec_child_task_num();
    ls_->get_transfer_handler()->wakeup_thread_cond();
  }
}

int ObTransferParallelBuildTabletTask::init(
    const share::ObTransferTabletInfo &first_tablet_info,
    ObTransferBuildTabletInfoCtx *ctx,
    ObTimeoutCtx *timeout_ctx)
{
  int ret = OB_SUCCESS;
  ObTransferParallelBuildTabletDag *dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer parallel build tablet task init twice", K(ret));
  } else if (OB_ISNULL(ctx) || OB_ISNULL(timeout_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer parallel build tablet task init get invalid argument", K(ret), KP(ctx), KP(timeout_ctx));
  } else if (OB_ISNULL(dag = static_cast<ObTransferParallelBuildTabletDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer parallel build tablet dag should not be NULL", K(ret), KP(dag));
  } else if (OB_FAIL(dag->get_ls(ls_))) {
    LOG_WARN("failed to get ls", K(ret), KPC(dag), K(first_tablet_info));
  } else {
    first_tablet_info_ = first_tablet_info;
    ctx_ = ctx;
    timeout_ctx_ = timeout_ctx;
    ctx_->inc_child_task_num();
    is_inited_ = true;
  }
  return ret;
}

int ObTransferParallelBuildTabletTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do transfer parallel build tablet task", K(first_tablet_info_));
  DEBUG_SYNC(BEFORE_PARALLEL_BUILD_TABLET_INFO_TABLET);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer parallel build tablet task do not init", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(timeout_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("timeout ctx should not be null", K(ret));
  } else if (timeout_ctx_->is_timeouted()) {
    ret = OB_TIMEOUT;
    LOG_WARN("transfer parallel build tablet task already timeout", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(do_build_tablet_infos_())) {
    LOG_WARN("failed to do build tablet infos", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->set_result(ret);
  }
  return ret;
}

int ObTransferParallelBuildTabletTask::do_build_tablet_infos_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer parallel build tablet task do not init", K(ret), KPC(ctx_));
  } else {
    if (first_tablet_info_.is_valid()) {
      if (OB_FAIL(ObTransferBuildTabletInfoHelper::build_tablet_info(ls_, first_tablet_info_, *ctx_))) {
        LOG_WARN("failed to build tablet info", K(ret), K(first_tablet_info_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransferBuildTabletInfoHelper::loop_to_build_tablet_infos(ls_, *timeout_ctx_, *ctx_))) {
      LOG_WARN("failed to build tablet infos", K(ret));
    }
  }
  return ret;
}

int ObTransferParallelBuildTabletTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  ObTransferParallelBuildTabletTask *tmp_next_task = nullptr;
  bool is_iter_end = false;
  int64_t index = 0;
  share::ObTransferTabletInfo tablet_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("parallel create tablet task do not init", K(ret));
  } else if (OB_FAIL(ctx_->get_next_tablet_info(tablet_info))) {
    if (OB_ITER_END == ret) {
      //do nothing
      } else {
        LOG_WARN("failed to get next copy tablet info index", K(ret), KPC(ctx_));
      }
  } else if (OB_FAIL(dag_->alloc_task(tmp_next_task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(tmp_next_task->init(tablet_info, ctx_, timeout_ctx_))) {
    LOG_WARN("failed to init next task", K(ret), K(tablet_info), K(index));
  } else {
    next_task = tmp_next_task;
  }
  return ret;
}

}
}
