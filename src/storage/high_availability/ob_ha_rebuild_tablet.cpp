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

#define USING_LOG_PREFIX STORAGE
#include "ob_ha_rebuild_tablet.h"
#include "observer/ob_server.h"
#include "ob_physical_copy_task.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "logservice/ob_log_service.h"
#include "lib/hash/ob_hashset.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "ob_storage_ha_src_provider.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "ob_storage_ha_utils.h"
#include "storage/tablet/ob_tablet.h"
#include "share/ls/ob_ls_table_operator.h"
#include "ob_rebuild_service.h"
#include "share/ob_cluster_version.h"
#include "ob_storage_ha_utils.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

/******************ObMigrationCtx*********************/
ObRebuildTabletCtx::ObRebuildTabletCtx()
  : ObIHADagNetCtx(),
    tenant_id_(OB_INVALID_ID),
    arg_(),
    start_ts_(0),
    finish_ts_(0),
    task_id_(),
    src_(),
    tablet_group_ctx_(),
    src_ls_rebuild_seq_(-1)
{
}

ObRebuildTabletCtx::~ObRebuildTabletCtx()
{
}

bool ObRebuildTabletCtx::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && tenant_id_ != 0 && tenant_id_ != OB_INVALID_ID;
}

void ObRebuildTabletCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  arg_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  task_id_.reset();
  src_.reset();
  tablet_group_ctx_.reuse();
  src_ls_rebuild_seq_ = -1;
  ObIHADagNetCtx::reset();
}

int ObRebuildTabletCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else {
  }
  return ret;
}

void ObRebuildTabletCtx::reuse()
{
  src_.reset();
  tablet_group_ctx_.reuse();
  src_ls_rebuild_seq_ = -1;
  ObIHADagNetCtx::reuse();
}

/******************ObRebuildTabletCopyCtx*********************/
ObRebuildTabletCopyCtx::ObRebuildTabletCopyCtx()
  : tablet_id_(),
    tablet_handle_(),
    sstable_info_(),
    copy_header_(),
    lock_(common::ObLatchIds::MIGRATE_LOCK),
    status_(ObCopyTabletStatus::MAX_STATUS)
{
}

ObRebuildTabletCopyCtx::~ObRebuildTabletCopyCtx()
{
}

bool ObRebuildTabletCopyCtx::is_valid() const
{
  return tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && tablet_handle_.is_valid())
          || ObCopyTabletStatus::TABLET_NOT_EXIST == status_);
}

void ObRebuildTabletCopyCtx::reset()
{
  tablet_id_.reset();
  tablet_handle_.reset();
  sstable_info_.reset();
  copy_header_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
}

int ObRebuildTabletCopyCtx::set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  if (!ObCopyTabletStatus::is_valid(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet src status is invalid", K(ret), K(status));
  } else {
    common::SpinWLockGuard guard(lock_);
    status_ = status;
  }
  return ret;
}

int ObRebuildTabletCopyCtx::get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const
{
  int ret = OB_SUCCESS;
  status = ObCopyTabletStatus::MAX_STATUS;
  common::SpinRLockGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy tablet ctx is invalid", K(ret), KPC(this));
  } else {
    status = status_;
  }
  return ret;
}

void ObRebuildTabletCopyCtx::reuse()
{
  tablet_handle_.reset();
  sstable_info_.reset();
  copy_header_.reset();
}

int ObRebuildTabletCopyCtx::get_copy_tablet_record_extra_info(ObCopyTabletRecordExtraInfo *&extra_info)
{
  int ret = OB_SUCCESS;
  extra_info = nullptr;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy tablet ctx is invalid", K(ret), KPC(this));
  } else {
    extra_info = &extra_info_;
  }
  return ret;
}

/******************ObRebuildTabletDagNet*********************/
ObRebuildTabletDagNetInitParam::ObRebuildTabletDagNetInitParam()
  : arg_(),
    task_id_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    sql_proxy_(nullptr)
{
}

bool ObRebuildTabletDagNetInitParam::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && OB_NOT_NULL(bandwidth_throttle_)
      && OB_NOT_NULL(svr_rpc_proxy_)
      && OB_NOT_NULL(storage_rpc_)
      && OB_NOT_NULL(sql_proxy_);
}


ObRebuildTabletDagNet::ObRebuildTabletDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET),
      is_inited_(false),
      ctx_(nullptr),
      bandwidth_throttle_(nullptr),
      svr_rpc_proxy_(nullptr),
      storage_rpc_(nullptr),
      sql_proxy_(nullptr)

{
}

ObRebuildTabletDagNet::~ObRebuildTabletDagNet()
{
  LOG_INFO("rebuild tablet dag net free", KPC(ctx_));
  free_rebuild_tablet_ctx_();
}

int ObRebuildTabletDagNet::alloc_rebuild_tablet_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rebuild tablet ctx init twice", K(ret), KPC(ctx_));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObRebuildTabletCtx), "RTCtx"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ctx_ = new (buf) ObRebuildTabletCtx())) {
  }
  return ret;
}

void ObRebuildTabletDagNet::free_rebuild_tablet_ctx_()
{
  if (OB_ISNULL(ctx_)) {
    //do nothing
  } else {
    ctx_->~ObRebuildTabletCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

int ObRebuildTabletDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObRebuildTabletDagNetInitParam* init_param = static_cast<const ObRebuildTabletDagNetInitParam*>(param);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rebuild tablet dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(alloc_rebuild_tablet_ctx_())) {
    LOG_WARN("failed to alloc rebuild tablet ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else {
    ObStorageHASrcInfo src;
    src.cluster_id_ = GCONF.cluster_id;
    src.src_addr_ = init_param->arg_.src_.get_server();

    ctx_->tenant_id_ = MTL_ID();
    ctx_->arg_ = init_param->arg_;
    ctx_->task_id_ = init_param->task_id_;
    ctx_->start_ts_ = ObTimeUtil::current_time();
    ctx_->src_ = src;
    bandwidth_throttle_ = init_param->bandwidth_throttle_;
    svr_rpc_proxy_ = init_param->svr_rpc_proxy_;
    storage_rpc_ = init_param->storage_rpc_;
    sql_proxy_ = init_param->sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

int ObRebuildTabletDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_rebuild_tablet_())) {
    LOG_WARN("failed to start running for rebuild tablet", K(ret));
  }

  return ret;
}

int ObRebuildTabletDagNet::start_running_for_rebuild_tablet_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialRebuildTabletDag *initial_rebuild_tablet_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_->start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_rebuild_tablet_dag))) {
    LOG_WARN("failed to alloc rebuild tablet dag ", K(ret));
  } else if (OB_FAIL(initial_rebuild_tablet_dag->init(this))) {
    LOG_WARN("failed to init rebuild tablet dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_rebuild_tablet_dag))) {
    LOG_WARN("failed to add rebuild tablet initial dag into dag net", K(ret));
  } else if (OB_FAIL(initial_rebuild_tablet_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_rebuild_tablet_dag))) {
    LOG_WARN("failed to add initial rebuild tablet dag", K(ret), K(*initial_rebuild_tablet_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_rebuild_tablet_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_rebuild_tablet_dag) && OB_NOT_NULL(scheduler)) {
    initial_rebuild_tablet_dag->reset_children();
    if (OB_SUCCESS != (tmp_ret = erase_dag_from_dag_net(*initial_rebuild_tablet_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_rebuild_tablet_dag));
    }
    scheduler->free_dag(*initial_rebuild_tablet_dag);
  }
  return ret;
}

bool ObRebuildTabletDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObRebuildTabletDagNet &other_rebuild_tablet_dag = static_cast<const ObRebuildTabletDagNet &>(other);
    if (OB_ISNULL(other_rebuild_tablet_dag.ctx_) || OB_ISNULL(ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "rebuild tablet ctx is NULL", KPC(ctx_), KPC(other_rebuild_tablet_dag.ctx_));
    } else if (ctx_->arg_.ls_id_ != other_rebuild_tablet_dag.ctx_->arg_.ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObRebuildTabletDagNet::hash() const
{
  int64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "rebuild tablet ctx is NULL", KPC(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_->arg_.ls_id_, sizeof(ctx_->arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObRebuildTabletDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  UNUSED(buf);
  UNUSED(buf_len);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet dag net do not init ", K(ret));
  } else if (OB_FAIL(ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    LOG_WARN("failed to trace task id to string", K(ret), K(*ctx_));
  } else {
  }
  return ret;
}

int ObRebuildTabletDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet dag net do not init", K(ret));
  } else {
  }
  return ret;
}

int ObRebuildTabletDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  int32_t result = OB_SUCCESS;
  ObLSHandle ls_handle;
  LOG_INFO("start clear dag net ctx", KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet dag net do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx_), KP(ls));
  } else {
    if (OB_FAIL(ctx_->get_result(result))) {
      LOG_WARN("failed to get migration ctx result", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ls->get_ls_migration_handler()->switch_next_stage(result))) {
      LOG_WARN("failed to report result", K(ret), KPC(ctx_));
    }

    ctx_->finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_->finish_ts_ - ctx_->start_ts_;
    FLOG_INFO("finish rebuild tablet dag net", "ls id", ctx_->arg_.ls_id_, "type", ctx_->arg_.type_, K(cost_ts), K(result));
  }
  return ret;
}

int ObRebuildTabletDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/******************ObRebuildTabletDag*********************/
ObRebuildTabletDag::ObRebuildTabletDag(
    const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObRebuildTabletDag::~ObRebuildTabletDag()
{
}

int ObRebuildTabletDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  UNUSED(out_param);
  UNUSED(allocator);
  if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ctx));
  } else {
  }
  return ret;
}

/******************ObInitialRebuildTabletDag*********************/
ObInitialRebuildTabletDag::ObInitialRebuildTabletDag()
  : ObRebuildTabletDag(ObDagType::DAG_TYPE_INITIAL_REBUILD_TABLET),
    is_inited_(false)
{
}

ObInitialRebuildTabletDag::~ObInitialRebuildTabletDag()
{
}

bool ObInitialRebuildTabletDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  ObRebuildTabletCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObRebuildTabletDag &other_dag = static_cast<const ObRebuildTabletDag&>(other);
    ctx = get_rebuild_tablet_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_rebuild_tablet_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "rebuild tablet ctx should not be NULL", KP(ctx), KP(other_dag.get_rebuild_tablet_ctx()));
    } else if (ctx->arg_.ls_id_ != other_dag.get_rebuild_tablet_ctx()->arg_.ls_id_) {
      is_same = false;
    } else {
      is_same = true;
    }
  }
  return is_same;
}

int64_t ObInitialRebuildTabletDag::hash() const
{
  int64_t hash_value = 0;
  ObRebuildTabletCtx * ctx = get_rebuild_tablet_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "rebuild tablet ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObInitialRebuildTabletDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  UNUSED(buf);
  UNUSED(buf_len);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial rebuild tablet dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial rebuild tablet ctx should not be NULL", K(ret), KP(ctx));
  } else {
  }
  return ret;
}

int ObInitialRebuildTabletDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial rebuild tablet dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInitialRebuildTabletDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialRebuildTabletTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial rebuild tablet dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial rebuild tablet task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObInitialRebuildTabletTask*********************/
ObInitialRebuildTabletTask::ObInitialRebuildTabletTask()
  : ObITask(TASK_TYPE_INITIAL_REBUILD_TABLET_TASK),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    dag_net_(nullptr)
{
}

ObInitialRebuildTabletTask::~ObInitialRebuildTabletTask()
{
}

int ObInitialRebuildTabletTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial rebuild tablet task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else {
    ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx();
    bandwidth_throttle_ = rebuild_tablet_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = rebuild_tablet_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = rebuild_tablet_dag_net->get_storage_rpc();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init initial rebuild tablet task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObInitialRebuildTabletTask::process()
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_REBUILD_TABLET_INITAL_TASK);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial rebuild tablet task do not init", K(ret));
  } else if (OB_FAIL(check_tablet_status_())) {
    LOG_WARN("failed to check tablet status", K(ret));
  } else if (OB_FAIL(build_tablet_group_ctx_())) {
    LOG_WARN("failed to build tablet group ctx", K(ret));
  } else if (OB_FAIL(generate_rebuild_tablet_dags_())) {
    LOG_WARN("failed to generate rebuild tablet dags", K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

int ObInitialRebuildTabletTask::generate_rebuild_tablet_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartRebuildTabletDag *start_rebuild_tablet_dag = nullptr;
  ObFinishRebuildTabletDag *finish_rebuild_tablet_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialRebuildTabletDag *initial_rebuild_tablet_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial rebuild tablet task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (ObDagType::DAG_TYPE_INITIAL_REBUILD_TABLET != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", K(ret), KPC(this));
  } else if (OB_ISNULL(initial_rebuild_tablet_dag = static_cast<ObInitialRebuildTabletDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial rebuild tablet dag is null", K(ret), KP(initial_rebuild_tablet_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(start_rebuild_tablet_dag))) {
    LOG_WARN("failed to alloc start rebuild tablet dag ", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(finish_rebuild_tablet_dag))) {
    LOG_WARN("failed to alloc finish rebuild tablet dag", K(ret));
  } else if (OB_FAIL(start_rebuild_tablet_dag->init(dag_net_, finish_rebuild_tablet_dag))) {
    LOG_WARN("failed to init start rebuild tablet dag", K(ret));
  } else if (OB_FAIL(finish_rebuild_tablet_dag->init(dag_net_))) {
    LOG_WARN("failed to init finish rebuild tablet dag", K(ret));
  } else if (OB_FAIL(initial_rebuild_tablet_dag->add_child(*start_rebuild_tablet_dag))) {
    LOG_WARN("failed to add start rebuild tablet dag", K(ret), KPC(start_rebuild_tablet_dag));
  } else if (OB_FAIL(start_rebuild_tablet_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(start_rebuild_tablet_dag->add_child(*finish_rebuild_tablet_dag))) {
    LOG_WARN("failed to add finish rebuild tablet dag as child", K(ret));
  } else if (OB_FAIL(finish_rebuild_tablet_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(finish_rebuild_tablet_dag))) {
    LOG_WARN("failed to add finish rebuild tablet dag", K(ret), K(*finish_rebuild_tablet_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(start_rebuild_tablet_dag))) {
    LOG_WARN("failed to add dag", K(ret), K(*start_rebuild_tablet_dag));
    if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(finish_rebuild_tablet_dag, start_rebuild_tablet_dag))) {
      LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(initial_rebuild_tablet_dag));
    } else {
      finish_rebuild_tablet_dag = nullptr;
    }

    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("succeed to schedule start rebuild tablet dag", K(*start_rebuild_tablet_dag));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_rebuild_tablet_dag)) {
      scheduler->free_dag(*finish_rebuild_tablet_dag);
      finish_rebuild_tablet_dag = nullptr;
    }
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_rebuild_tablet_dag)) {
      scheduler->free_dag(*start_rebuild_tablet_dag);
      start_rebuild_tablet_dag = nullptr;
    }
    const bool need_retry = true;
    if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, need_retry,
        this->get_dag()->get_type()))) {
      LOG_WARN("failed to set rebuild tablet result", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

int ObInitialRebuildTabletTask::build_tablet_group_ctx_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial rebuild tablet task do not init", K(ret));
  } else {
    ctx_->tablet_group_ctx_.reuse();
    if (OB_FAIL(ctx_->tablet_group_ctx_.init(tablet_id_array_))) {
      LOG_WARN("failed to init tablet group ctx", K(ret), KPC(ctx_), K(tablet_id_array_));
    }
  }
  return ret;
}

int ObInitialRebuildTabletTask::check_tablet_status_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  tablet_id_array_.reset();
  ObLogicTabletID logic_tablet_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial rebuild tablet task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->arg_.tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->arg_.tablet_id_array_.at(i);
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      logic_tablet_id.reset();
      if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id), K(tablet_handle));
      } else if (!tablet->get_tablet_meta().ha_status_.is_none() || tablet->get_tablet_meta().transfer_info_.has_transfer_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet data is incomplete or still has transfer table, cannot do tablet rebuild", K(ret), K(tablet_id), KPC(tablet));
      } else if (OB_FAIL(logic_tablet_id.init(tablet_id, tablet->get_tablet_meta().transfer_info_.transfer_seq_))) {
        LOG_WARN("failed to init logic tablet id", K(ret), K(tablet_id), KPC(tablet));
      } else if (OB_FAIL(tablet_id_array_.push_back(logic_tablet_id))) {
        LOG_WARN("failed to push logic id into array", K(ret), K(logic_tablet_id));
      }
    }
  }
  return ret;
}

/******************ObStartMigrationDag*********************/
ObStartRebuildTabletDag::ObStartRebuildTabletDag()
  : ObRebuildTabletDag(ObDagType::DAG_TYPE_START_REBUILD_TABLET),
    is_inited_(false),
    finish_dag_(nullptr)
{
}

ObStartRebuildTabletDag::~ObStartRebuildTabletDag()
{
}

bool ObStartRebuildTabletDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObRebuildTabletDag &other_dag = static_cast<const ObRebuildTabletDag&>(other);
    const ObRebuildTabletCtx *ctx = get_rebuild_tablet_ctx();
    const ObRebuildTabletCtx *other_ctx = other_dag.get_rebuild_tablet_ctx();
    if (NULL != ctx && NULL != other_ctx) {
      if (ctx->arg_.ls_id_ != other_ctx->arg_.ls_id_) {
        is_same = false;
      }
    } else {
      LOG_INFO("rebuild ctx is null", KP(ctx), KP(other_ctx));
    }
  }
  return is_same;
}

int64_t ObStartRebuildTabletDag::hash() const
{
  int64_t hash_value = 0;
  ObRebuildTabletCtx *ctx = get_rebuild_tablet_ctx();

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObStartRebuildTabletDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  UNUSED(buf);
  UNUSED(buf_len);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start rebuild tablet dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ctx));
  } else {
  }
  return ret;
}

int ObStartRebuildTabletDag::init(
    ObIDagNet *dag_net,
    ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start rebuild tablet dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net), KP(finish_dag));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    finish_dag_ = finish_dag;
    is_inited_ = true;
  }
  return ret;
}

int ObStartRebuildTabletDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartRebuildTabletTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start rebuild tablet dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(finish_dag_))) {
    LOG_WARN("failed to init start rebuild tablet task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartRebuildTabletTask*********************/
ObStartRebuildTabletTask::ObStartRebuildTabletTask()
  : ObITask(TASK_TYPE_START_REBUILD_TABLET_TASK),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    finish_dag_(nullptr)
{
}

ObStartRebuildTabletTask::~ObStartRebuildTabletTask()
{
}

int ObStartRebuildTabletTask::init(
    share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start rebuild tablet task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net), KP(finish_dag));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else {
    ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx();
    bandwidth_throttle_ = rebuild_tablet_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = rebuild_tablet_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = rebuild_tablet_dag_net->get_storage_rpc();
    finish_dag_ = finish_dag;
    is_inited_ = true;
    LOG_INFO("succeed init start rebuild tablet task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObStartRebuildTabletTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_copy_data = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start rebuild tablet task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(get_src_ls_rebuild_seq_())) {
    LOG_WARN("failed to get src ls rebuild seq", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_rebuild_tablets_dag_())) {
    LOG_WARN("failed to generate rebuild tablets dag", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObStartRebuildTabletTask::generate_rebuild_tablets_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObStartRebuildTabletDag *start_rebuild_tablet_dag = nullptr;
  ObTabletRebuildMajorDag *tablet_rebuild_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start rebuild tablet task do not init", K(ret));
  } else if (OB_ISNULL(start_rebuild_tablet_dag = static_cast<ObStartRebuildTabletDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start rebuild tablet dag should not be NULL", K(ret), KP(start_rebuild_tablet_dag));
  } else if (OB_ISNULL(dag_net = start_rebuild_tablet_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    ObIDag *parent = this->get_dag();
    ObLogicTabletID logic_tablet_id;
    if (OB_FAIL(ctx_->tablet_group_ctx_.get_next_tablet_id(logic_tablet_id))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next tablet id", K(ret), KPC(ctx_));
      }
    } else if (OB_FAIL(scheduler->alloc_dag(tablet_rebuild_dag))) {
      LOG_WARN("failed to alloc tablet rebuild dag ", K(ret));
    } else if (OB_FAIL(tablet_rebuild_dag->init(logic_tablet_id.tablet_id_, dag_net, finish_dag_))) {
      LOG_WARN("failed to init tablet rebuild dag", K(ret), K(*ctx_));
    } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_rebuild_dag))) {
      LOG_WARN("failed to add dag into dag net", K(ret), K(*ctx_));
    } else if (OB_FAIL(parent->add_child_without_inheritance(*tablet_rebuild_dag))) {
      LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
    } else if (OB_FAIL(tablet_rebuild_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), K(*ctx_));
    } else if (OB_FAIL(tablet_rebuild_dag->add_child_without_inheritance(*finish_dag_))) {
      LOG_WARN("failed to add finish dag as child", K(ret), K(*ctx_));
    } else if (OB_FAIL(scheduler->add_dag(tablet_rebuild_dag))) {
      LOG_WARN("failed to add tablet rebuild dag", K(ret), K(*tablet_rebuild_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("succeed to schedule tablet rebuild dag", K(*tablet_rebuild_dag), K(logic_tablet_id));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(tablet_rebuild_dag)) {
        scheduler->free_dag(*tablet_rebuild_dag);
        tablet_rebuild_dag = nullptr;
      }
    }
  }
  return ret;
}

int ObStartRebuildTabletTask::get_src_ls_rebuild_seq_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  obrpc::ObFetchLSMetaInfoResp ls_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start rebuild tablet task do not init", K(ret));
  } else if (OB_FAIL(storage_rpc_->post_ls_meta_info_request(
      tenant_id, ctx_->src_, ctx_->arg_.ls_id_, ls_info))) {
    LOG_WARN("fail to post fetch ls meta info request", K(ret), "src_info", ctx_->src_, "arg", ctx_->arg_);
  } else {
    ctx_->src_ls_rebuild_seq_ = ls_info.ls_meta_package_.ls_meta_.get_rebuild_seq();
  }
  return ret;
}

/******************ObTabletRebuildMajorDag*********************/
ObTabletRebuildMajorDag::ObTabletRebuildMajorDag()
  : ObRebuildTabletDag(ObDagType::DAG_TYPE_TABLET_REBUILD),
    is_inited_(false),
    ls_handle_(),
    copy_tablet_ctx_(),
    finish_dag_(nullptr)
{
}

ObTabletRebuildMajorDag::~ObTabletRebuildMajorDag()
{
}

bool ObTabletRebuildMajorDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObRebuildTabletDag &other_dag = static_cast<const ObRebuildTabletDag&>(other);
    const ObRebuildTabletCtx *other_dag_ctx = other_dag.get_rebuild_tablet_ctx();
    const ObRebuildTabletCtx *ctx = get_rebuild_tablet_ctx();
    if (NULL != ctx && NULL != other_dag_ctx) {
      if (ctx->arg_.ls_id_ != other_dag_ctx->arg_.ls_id_) {
        is_same = false;
      } else {
        const ObTabletRebuildMajorDag &tablet_rebuild_dag = static_cast<const ObTabletRebuildMajorDag&>(other);
        if (tablet_rebuild_dag.copy_tablet_ctx_.tablet_id_ != copy_tablet_ctx_.tablet_id_) {
          is_same = false;
        }
      }
    } else {
      LOG_INFO("rebuild ctx is null", KP(ctx), KP(other_dag_ctx));
    }
  }
  return is_same;
}

int64_t ObTabletRebuildMajorDag::hash() const
{
  int64_t hash_value = 0;
  ObRebuildTabletCtx *ctx = get_rebuild_tablet_ctx();

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    hash_value = common::murmurhash(
        &copy_tablet_ctx_.tablet_id_, sizeof(copy_tablet_ctx_.tablet_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObTabletRebuildMajorDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  UNUSED(buf);
  UNUSED(buf_len);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild ctx should not be NULL", K(ret), KP(ctx));
  } else {
  }
  return ret;
}

int ObTabletRebuildMajorDag::init(
    const common::ObTabletID &tablet_id,
    share::ObIDagNet *dag_net,
    share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;
  ObLS *ls = nullptr;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  ObRebuildTabletCtx *ctx = nullptr;
  ObTabletHandle tablet_handle;
  DEBUG_SYNC(BEFORE_REBUILD_TABLET_INIT_DAG);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet rebuild major dag init twice", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet rebuild major dag init get invalid argument", K(ret), K(tablet_id), KP(dag_net), KP(finish_dag));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else if (FALSE_IT(ctx = rebuild_tablet_dag_net->get_rebuild_tablet_ctx())) {
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx->arg_.ls_id_, ls_handle_))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(tablet_id));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      status = ObCopyTabletStatus::TABLET_NOT_EXIST;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    }
  } else {
    status = ObCopyTabletStatus::TABLET_EXIST;
    copy_tablet_ctx_.tablet_handle_ = tablet_handle;
    compat_mode_ = tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(copy_tablet_ctx_.tablet_id_ = tablet_id)) {
  } else if (OB_FAIL(copy_tablet_ctx_.set_copy_tablet_status(status))) {
    LOG_WARN("failed to set copy tablet status", K(ret), K(status), K(tablet_id));
  } else if (FALSE_IT(ha_dag_net_ctx_ = ctx)) {
  } else {
    finish_dag_ = finish_dag;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletRebuildMajorDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletRebuildMajorTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(copy_tablet_ctx_))) {
    LOG_WARN("failed to init tablet rebuild major task", K(ret), KPC(ha_dag_net_ctx_), K(copy_tablet_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletRebuildMajorDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  UNUSED(out_param);
  UNUSED(allocator);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet rebuild dag ctx should not be NULL", K(ret), KP(ctx));
  } else {
  }
  return ret;
}

int ObTabletRebuildMajorDag::get_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  ls = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag is not init", K(ret));
  } else {
    ls = ls_handle_.get_ls();
  }
  return ret;
}

int ObTabletRebuildMajorDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletRebuildMajorDag *tablet_rebuild_dag = nullptr;
  ObLogicTabletID logic_tablet_id;
  ObDagId dag_id;
  const int64_t start_ts = ObTimeUtil::current_time();
  ObRebuildTabletCtx *ctx = nullptr;
  ObLS *ls = nullptr;
  bool need_set_failed_result = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ctx));
  } else if (ctx->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ctx->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ctx));
      ret = tmp_ret;
    }
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx));
  } else {
    while (OB_SUCC(ret)) {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ctx->tablet_group_ctx_.get_next_tablet_id(logic_tablet_id))) {
        if (OB_ITER_END == ret) {
          need_set_failed_result = false;
        } else {
          LOG_WARN("failed to get next tablet id", K(ret), KPC(this));
        }
      } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rebuild tablet major dag net should not be NULL", K(ret), KP(dag_net));
      } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
      } else if (OB_FAIL(ls->ha_get_tablet(logic_tablet_id.tablet_id_, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(logic_tablet_id));
        }
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_rebuild_dag))) {
        LOG_WARN("failed to alloc tablet rebuild dag", K(ret));
      } else {
        if (OB_FAIL(tablet_rebuild_dag->init(logic_tablet_id.tablet_id_, dag_net, finish_dag_))) {
          LOG_WARN("failed to init tablet rebuild major dag", K(ret), K(logic_tablet_id));
        } else if (FALSE_IT(dag_id.init(MYADDR))) {
        } else if (OB_FAIL(tablet_rebuild_dag->set_dag_id(dag_id))) {
          LOG_WARN("failed to set dag id", K(ret), K(logic_tablet_id));
        } else {
          LOG_INFO("succeed generate next dag", K(logic_tablet_id));
          dag = tablet_rebuild_dag;
          tablet_rebuild_dag = nullptr;
          break;
        }
      }
    }
  }

  if (OB_NOT_NULL(tablet_rebuild_dag)) {
    scheduler->free_dag(*tablet_rebuild_dag);
    tablet_rebuild_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }

  LOG_INFO("generate_next_dag", K(ret), K(logic_tablet_id), "cost", ObTimeUtil::current_time() - start_ts,
      "dag_id", dag_id);
  return ret;
}

int ObTabletRebuildMajorDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ctx));
  } else if (ctx->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ctx->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ctx));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner set status for retry failed", K(ret), KPC(ctx));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), KP(ctx));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to get retry count", K(ret));
  } else {
    LOG_INFO("start retry", KPC(this));
    result_mgr_.reuse();
    copy_tablet_ctx_.reuse();
    SERVER_EVENT_ADD("storage_ha", "tablet_rebuild_retry",
        "tenant_id", ctx->tenant_id_,
        "ls_id", ctx->arg_.ls_id_.id(),
        "tablet_id", copy_tablet_ctx_.tablet_id_,
        "result", result, "retry_count", retry_count);
    if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(copy_tablet_ctx_));
    } else if (OB_FAIL(ls->ha_get_tablet(copy_tablet_ctx_.tablet_id_, copy_tablet_ctx_.tablet_handle_))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        const ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_NOT_EXIST;
        if (OB_FAIL(copy_tablet_ctx_.set_copy_tablet_status(status))) {
          LOG_WARN("failed to set copy tablet status", K(ret), K(status));
        } else {
          FLOG_INFO("tablet in dest is deleted, set copy status not exist", K(copy_tablet_ctx_));
        }
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(copy_tablet_ctx_));
      }
    }

    //ATTENTION : DAG in retry status should create first task
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

/******************ObTabletRebuildMajorTask*********************/
ObTabletRebuildMajorTask::ObTabletRebuildMajorTask()
  : ObITask(TASK_TYPE_TABLET_REBUILD_TASK),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    sql_proxy_(nullptr),
    copy_tablet_ctx_(nullptr),
    copy_table_key_array_(),
    copy_sstable_info_mgr_()
{
}

ObTabletRebuildMajorTask::~ObTabletRebuildMajorTask()
{
}

int ObTabletRebuildMajorTask::init(ObRebuildTabletCopyCtx &copy_tablet_ctx)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet rebuild major task init twice", K(ret));
  } else if (!copy_tablet_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet rebuild major task init get invalid argument", K(ret), K(copy_tablet_ctx));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else {
    ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx();
    bandwidth_throttle_ = rebuild_tablet_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = rebuild_tablet_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = rebuild_tablet_dag_net->get_storage_rpc();
    sql_proxy_ = rebuild_tablet_dag_net->get_sql_proxy();
    copy_tablet_ctx_ = &copy_tablet_ctx;
    is_inited_ = true;
    LOG_INFO("succeed init tablet rebuild major task", "ls id", ctx_->arg_.ls_id_, "tablet_id", copy_tablet_ctx.tablet_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObTabletRebuildMajorTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start do tablet rebuild major task", KPC(copy_tablet_ctx_));
  const int64_t start_ts = ObTimeUtility::current_time();
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major task do not init", K(ret), KPC(copy_tablet_ctx_));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(copy_tablet_ctx_->get_copy_tablet_status(status))) {
    LOG_WARN("failed to get copy tablet status", K(ret), KPC(copy_tablet_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("copy tablet is not exist, skip copy it", KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(build_copy_table_key_info_())) {
    LOG_WARN("failed to build copy table key info", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(build_copy_sstable_info_mgr_())) {
    LOG_WARN("failed to build copy sstable info mgr", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(generate_tablet_rebuild_tasks_())) {
    LOG_WARN("failed to generate tablet rebuild tasks", K(ret), KPC(copy_tablet_ctx_));
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_ts;
  if (OB_SUCCESS != (tmp_ret = record_server_event_(cost_us, ret))) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(cost_us), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObTabletRebuildMajorTask::generate_tablet_rebuild_tasks_()
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  ObTabletCopyFinishTask *tablet_copy_finish_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_tablet_copy_finish_task_(tablet_copy_finish_task))) {
    LOG_WARN("failed to generate tablet copy finish task", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_major_copy_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate migrate major tasks", K(ret), K(*ctx_));
  } else if (OB_FAIL(parent_task->add_child(*tablet_copy_finish_task))) {
    LOG_WARN("failed to add tablet copy finish task as child", K(ret), KPC(ctx_));
  } else if (OB_FAIL(dag_->add_task(*tablet_copy_finish_task))) {
    LOG_WARN("failed to add tablet copy finish task", K(ret), KPC(ctx_));
  } else {
    LOG_INFO("generate tablet rebuild major tasks", KPC(copy_tablet_ctx_), K(copy_table_key_array_));
  }
  return ret;
}

int ObTabletRebuildMajorTask::generate_physical_copy_task_(
    const ObStorageHASrcInfo &src_info,
    const ObITable::TableKey &copy_table_key,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    ObITask *parent_task,
    ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObPhysicalCopyTask *copy_task = NULL;
  ObSSTableCopyFinishTask *finish_task = NULL;
  const int64_t task_idx = 0;
  ObLS *ls = nullptr;
  ObPhysicalCopyTaskInitParam init_param;
  ObTabletRebuildMajorDag *tablet_rebuild_dag = nullptr;
  bool is_tablet_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major task do not init", K(ret));
  } else if (!src_info.is_valid() || !copy_table_key.is_valid() || OB_ISNULL(tablet_copy_finish_task)
      || OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate physical copy task get invalid argument", K(ret),
        K(src_info), KP(parent_task), KP(child_task));
  } else if (FALSE_IT(tablet_rebuild_dag = static_cast<ObTabletRebuildMajorDag *>(dag_))) {
  } else if (OB_FAIL(tablet_rebuild_dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(copy_sstable_info_mgr_.check_src_tablet_exist(is_tablet_exist))) {
    LOG_WARN("failed to check src tablet exist", K(ret), K(copy_table_key));
  } else if (!is_tablet_exist) {
    if (OB_FAIL(tablet_copy_finish_task->set_tablet_status(ObCopyTabletStatus::TABLET_NOT_EXIST))) {
      LOG_WARN("failed to set tablet status", K(ret), K(copy_table_key), KPC(copy_tablet_ctx_));
    } else if (OB_FAIL(parent_task->add_child(*child_task))) {
      LOG_WARN("failed to add chiild task", K(ret), KPC(copy_tablet_ctx_), K(copy_table_key));
    }
  } else {
    init_param.tenant_id_ = ctx_->tenant_id_;
    init_param.ls_id_ = ctx_->arg_.ls_id_;
    init_param.tablet_id_ = copy_tablet_ctx_->tablet_id_;
    init_param.src_info_ = src_info;
    init_param.tablet_copy_finish_task_ = tablet_copy_finish_task;
    init_param.ls_ = ls;
    init_param.need_check_seq_ = true;
    init_param.ls_rebuild_seq_ = ctx_->src_ls_rebuild_seq_;
    init_param.sstable_param_ = &copy_tablet_ctx_->sstable_info_.param_;
    init_param.extra_info_ = &copy_tablet_ctx_->extra_info_;

    if (OB_FAIL(copy_sstable_info_mgr_.get_copy_sstable_maro_range_info(copy_table_key, init_param.sstable_macro_range_info_))) {
      LOG_WARN("failed to get copy sstable macro range info", K(ret), K(copy_table_key));
    } else if (!init_param.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical copy task init param not valid", K(ret), K(init_param), KPC(ctx_));
    } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
      LOG_WARN("failed to alloc finish task", K(ret));
    } else if (OB_FAIL(finish_task->init(init_param))) {
      LOG_WARN("failed to init finish task", K(ret), K(copy_table_key), K(*ctx_));
    } else if (OB_FAIL(finish_task->add_child(*child_task))) {
      LOG_WARN("failed to add child", K(ret));
    } else if (init_param.sstable_macro_range_info_.copy_macro_range_array_.count() > 0) {
      // parent->copy->finish->child
      if (OB_FAIL(dag_->alloc_task(copy_task))) {
        LOG_WARN("failed to alloc copy task", K(ret));
      } else if (OB_FAIL(copy_task->init(finish_task->get_copy_ctx(), finish_task))) {
        LOG_WARN("failed to init copy task", K(ret));
      } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
        LOG_WARN("failed to add child copy task", K(ret));
      } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish task", K(ret));
      } else if (OB_FAIL(dag_->add_task(*copy_task))) {
        LOG_WARN("failed to add copy task to dag", K(ret));
      }
    } else {
      if (OB_FAIL(parent_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish_task for parent", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dag_->add_task(*finish_task))) {
        LOG_WARN("failed to add finish task to dag", K(ret));
      } else {
        FLOG_INFO("succeed to generate physical copy task",
            K(copy_table_key), K(src_info), KPC(copy_task), KPC(finish_task));
      }
    }
  }
  return ret;
}

int ObTabletRebuildMajorTask::build_copy_table_key_info_()
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTableStoreIterator iter;
  int64_t local_max_major_snapshot = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *last_major_sstable = nullptr;
  ObRebuildTabletSSTableInfoObReader reader;
  obrpc::ObRebuildTabletSSTableInfoArg rpc_arg;
  uint64_t server_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major task do not init", K(ret));
  } else if (OB_ISNULL(tablet = copy_tablet_ctx_->tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret), KPC(copy_tablet_ctx_));
  } else if (FALSE_IT(last_major_sstable = static_cast<ObSSTable *>(
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/)))) {
  } else if (OB_ISNULL(last_major_sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet do not has major sstable, unexpected", K(ret), KP(last_major_sstable));
  } else if (FALSE_IT(local_max_major_snapshot = last_major_sstable->get_snapshot_version())) {
  } else if (OB_FAIL(ObStorageHAUtils::get_server_version(server_version))) {
    LOG_WARN("failed to get server version", K(ret), KPC(copy_tablet_ctx_));
  } else {
    rpc_arg.tenant_id_ = MTL_ID();
    rpc_arg.ls_id_ = ctx_->arg_.ls_id_;
    rpc_arg.tablet_id_ = copy_tablet_ctx_->tablet_id_;
    rpc_arg.dest_major_sstable_snapshot_ = local_max_major_snapshot;
    rpc_arg.version_ = server_version;
    //TODO(muwei.ym) COSSTable should using iterator
    if (OB_FAIL(reader.init(ctx_->src_, rpc_arg, *svr_rpc_proxy_, *bandwidth_throttle_))) {
      LOG_WARN("failed to init rebuild tablet sstable info reader", K(ret));
    } else if (OB_FAIL(reader.get_next_tablet_sstable_header(copy_tablet_ctx_->copy_header_))) {
      LOG_WARN("failed to get next tablet sstable hader", K(ret), KPC(copy_tablet_ctx_));
    } else if (0 == copy_tablet_ctx_->copy_header_.sstable_count_ && ObCopyTabletStatus::TABLET_EXIST == copy_tablet_ctx_->copy_header_.status_) {
      ret = OB_MAJOR_SSTABLE_NOT_EXIST;
      LOG_WARN("rebuild tablet src do not has needed major sstable", K(ret), K(rpc_arg), KPC(ctx_), KPC(copy_tablet_ctx_));
    } else if (copy_tablet_ctx_->copy_header_.sstable_count_ > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rebuild tablet src sstable count unexpected", K(ret), KPC(copy_tablet_ctx_));
    } else if (1 == copy_tablet_ctx_->copy_header_.sstable_count_) {
      if (OB_FAIL(reader.get_next_sstable_info(copy_tablet_ctx_->sstable_info_))) {
        LOG_WARN("failed to get next sstable info", K(ret), KPC(copy_tablet_ctx_));
      } else if (OB_FAIL(copy_table_key_array_.push_back(copy_tablet_ctx_->sstable_info_.table_key_))) {
        LOG_WARN("failed to get next sstable info", K(ret), KPC(copy_tablet_ctx_));
      }
    } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == copy_tablet_ctx_->copy_header_.status_) {
      if (OB_FAIL(copy_tablet_ctx_->set_copy_tablet_status(copy_tablet_ctx_->copy_header_.status_))) {
        LOG_WARN("failed to set copy tablet status", K(ret), K(copy_tablet_ctx_));
      }
    }
  }
  return ret;
}

int ObTabletRebuildMajorTask::build_copy_sstable_info_mgr_()
{
  int ret = OB_SUCCESS;
  ObStorageHACopySSTableParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major task do not init", K(ret));
  } else if (OB_FAIL(param.copy_table_key_array_.assign(copy_table_key_array_))) {
    LOG_WARN("failed to assign copy table key info array", K(ret), K(copy_table_key_array_));
  } else {
    param.tenant_id_ = ctx_->tenant_id_;
    param.ls_id_ = ctx_->arg_.ls_id_;
    param.tablet_id_ = copy_tablet_ctx_->tablet_id_;
    param.is_leader_restore_ = false;
    param.need_check_seq_ = true;
    param.meta_index_store_ = nullptr;
    param.second_meta_index_store_ = nullptr;
    param.restore_base_info_ = nullptr;
    param.src_info_ = ctx_->src_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;
    param.bandwidth_throttle_ = bandwidth_throttle_;
    param.src_ls_rebuild_seq_ = ctx_->src_ls_rebuild_seq_;

    if (OB_FAIL(copy_sstable_info_mgr_.init(param))) {
      LOG_WARN("failed to init copy sstable info mgr", K(ret), K(param), KPC(ctx_), KPC(copy_tablet_ctx_));
    }
  }
  return ret;
}

int ObTabletRebuildMajorTask::generate_major_copy_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_REBUILD_TABLET_COPY_SSTABLE_TASK);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate copy task get invalid argument", K(ret), KP(parent_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < copy_table_key_array_.count(); ++i) {
      const ObITable::TableKey &copy_table_key = copy_table_key_array_.at(i);
      ObFakeTask *wait_finish_task = nullptr;
      bool need_copy = true;

      if (!copy_table_key.is_valid() || !copy_table_key.is_major_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy table key info is invalid", K(ret), K(copy_table_key));
      } else {
        if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(generate_physical_copy_task_(ctx_->src_,
            copy_table_key, tablet_copy_finish_task, parent_task, wait_finish_task))) {
          LOG_WARN("failed to generate_physical copy task", K(ret), K(*ctx_), K(copy_table_key));
        } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task", K(ret));
        } else {
          parent_task = wait_finish_task;
          LOG_INFO("succeed to generate_sstable_copy_task", "src", ctx_->src_, K(copy_table_key));
        }
      }
    }
  }
  return ret;
}

int ObTabletRebuildMajorTask::generate_tablet_copy_finish_task_(
    ObTabletCopyFinishTask *&tablet_copy_finish_task)
{
  int ret = OB_SUCCESS;
  tablet_copy_finish_task = nullptr;
  ObLS *ls = nullptr;
  ObTabletRebuildMajorDag *tablet_rebuild_dag = nullptr;
  observer::ObIMetaReport *reporter = GCTX.ob_service_;
  const ObTabletRestoreAction::ACTION restore_action = ObTabletRestoreAction::RESTORE_NONE;
  const ObMigrationTabletParam *src_tablet_meta = nullptr;
  ObTabletCopyFinishTaskParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet rebuild major task do not init", K(ret));
  } else if (FALSE_IT(tablet_rebuild_dag = static_cast<ObTabletRebuildMajorDag *>(dag_))) {
  } else if (OB_FAIL(dag_->alloc_task(tablet_copy_finish_task))) {
    LOG_WARN("failed to alloc tablet copy finish task", K(ret), KPC(ctx_));
  } else if (OB_FAIL(tablet_rebuild_dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else {
    param.ls_ = ls;
    param.tablet_id_ = copy_tablet_ctx_->tablet_id_;
    param.copy_tablet_ctx_ = copy_tablet_ctx_;
    param.reporter_ = reporter;
    param.restore_action_ = restore_action;
    param.src_tablet_meta_ = &copy_tablet_ctx_->copy_header_.tablet_meta_;
    param.is_only_replace_major_ = true;
    if (OB_FAIL(tablet_copy_finish_task->init(param))) {
      LOG_WARN("failed to init tablet copy finish task", K(ret), KPC(ctx_), KPC(copy_tablet_ctx_));
    } else {
      LOG_INFO("generate tablet copy finish task", "ls_id", ls->get_ls_id().id(), "tablet_id", copy_tablet_ctx_->tablet_id_);
    }
  }
  return ret;
}

int ObTabletRebuildMajorTask::record_server_event_(const int64_t cost_us, const int64_t result)
{
  int ret = OB_SUCCESS;
  const ObMigrationTabletParam *src_tablet_meta = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletRebuildMajorDag *tablet_rebuild_dag = nullptr;
  const int64_t buf_len = MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
  char comment[buf_len] = {0};

  if (OB_ISNULL(ctx_) || OB_ISNULL(copy_tablet_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx), KPC_(copy_tablet_ctx));
  } else if (FALSE_IT(tablet_rebuild_dag = static_cast<ObTabletRebuildMajorDag *>(dag_))) {
  } else if (OB_FAIL(tablet_rebuild_dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->ha_get_tablet(copy_tablet_ctx_->tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet));
  } else if (OB_FAIL(databuff_printf(comment, buf_len,
      "ret:%ld, cost_ts:%ld", result, cost_us))) {
   LOG_WARN("failed to fill comment", K(ret), KPC(copy_tablet_ctx_));
  } else {
    const char *tablet_status = ObTabletStatus::get_str(user_data.tablet_status_);
    SERVER_EVENT_ADD("storage_ha", "tablet_rebuild_major_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "tablet_id", copy_tablet_ctx_->tablet_id_.id(),
        "tablet_status", tablet_status, comment);
  }
  return ret;
}

/******************ObFinishRebuildTabletDag*********************/
ObFinishRebuildTabletDag::ObFinishRebuildTabletDag()
  : ObRebuildTabletDag(ObDagType::DAG_TYPE_FINISH_REBUILD_TABLET),
    is_inited_(false)
{
}

ObFinishRebuildTabletDag::~ObFinishRebuildTabletDag()
{
}

bool ObFinishRebuildTabletDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObRebuildTabletDag &other_dag = static_cast<const ObRebuildTabletDag&>(other);
    ObRebuildTabletCtx *ctx = get_rebuild_tablet_ctx();
    if (NULL != ctx && NULL != other_dag.get_rebuild_tablet_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_rebuild_tablet_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObFinishRebuildTabletDag::hash() const
{
  int64_t hash_value = 0;
  ObRebuildTabletCtx *ctx = get_rebuild_tablet_ctx();

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObFinishRebuildTabletDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObRebuildTabletCtx *ctx = nullptr;
  UNUSED(buf);
  UNUSED(buf_len);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish rebuild tablet dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_rebuild_tablet_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else {
  }
  return ret;
}

int ObFinishRebuildTabletDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish rebuild tablet dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rebuild tablet ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFinishRebuildTabletDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishRebuildTabletTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish rebuild tablet dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init log stream migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishRebuildTabletTask*********************/
ObFinishRebuildTabletTask::ObFinishRebuildTabletTask()
  : ObITask(TASK_TYPE_FINISH_REBUILD_TABLET_TASK),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObFinishRebuildTabletTask::~ObFinishRebuildTabletTask()
{
}

int ObFinishRebuildTabletTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObRebuildTabletDagNet *rebuild_tablet_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish rebuild tablet task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_REBUILD_TABLET != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(rebuild_tablet_dag_net = static_cast<ObRebuildTabletDagNet*>(dag_net))) {
  } else {
    ctx_ = rebuild_tablet_dag_net->get_rebuild_tablet_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init finish rebuild tablet task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObFinishRebuildTabletTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("start do finish rebuild tablet task");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish rebuild tablet task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    bool allow_retry = false;
    if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
      LOG_ERROR("failed to check allow retry", K(ret), K(*ctx_));
    } else if (allow_retry) {
      if (OB_FAIL(generate_rebuild_tablet_init_dag_())) {
        LOG_WARN("failed to generate rebuild tablet init dag", K(ret), KPC(ctx_));
      }
    }
  } else {
  }
  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObFinishRebuildTabletTask::generate_rebuild_tablet_init_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialRebuildTabletDag *initial_rebuild_tablet_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObFinishRebuildTabletDag *finish_rebuild_tablet_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet rebuild task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(finish_rebuild_tablet_dag = static_cast<ObFinishRebuildTabletDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish rebuild tablet dag should not be NULL", K(ret), KP(finish_rebuild_tablet_dag));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(initial_rebuild_tablet_dag))) {
      LOG_WARN("failed to alloc initial rebuild tablet dag ", K(ret));
    } else if (OB_FAIL(initial_rebuild_tablet_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial rebuild tablet dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_rebuild_tablet_dag))) {
      LOG_WARN("failed to add initial rebuild tablet dag", K(ret), KPC(initial_rebuild_tablet_dag));
    } else if (OB_FAIL(initial_rebuild_tablet_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_rebuild_tablet_dag))) {
      LOG_WARN("failed to add initial rebuild tablet dag", K(ret), K(*initial_rebuild_tablet_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create initial rebuild tablet dag", K(ret), K(*ctx_));
      ctx_->reuse();
      initial_rebuild_tablet_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_rebuild_tablet_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_rebuild_tablet_dag);
      initial_rebuild_tablet_dag = nullptr;
    }
  }
  return ret;
}

int ObFinishRebuildTabletTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "finish_rebuild_tablet_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "is_failed", ctx_->is_failed(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}



} //storage
} //ocenabase
