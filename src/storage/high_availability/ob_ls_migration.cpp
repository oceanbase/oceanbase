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
#include "ob_ls_migration.h"
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

namespace oceanbase
{
using namespace share;
namespace storage
{

ERRSIM_POINT_DEF(EN_BUILD_SYS_TABLETS_DAG_FAILED);
ERRSIM_POINT_DEF(EN_UPDATE_LS_MIGRATION_STATUS_FAILED);
ERRSIM_POINT_DEF(EN_JOIN_LEARNER_LIST_FAILED);

/******************ObMigrationCtx*********************/
ObMigrationCtx::ObMigrationCtx()
  : ObIHADagNetCtx(),
    tenant_id_(OB_INVALID_ID),
    arg_(),
    local_clog_checkpoint_scn_(SCN::min_scn()),
    local_rebuild_seq_(-1),
    start_ts_(0),
    finish_ts_(0),
    task_id_(),
    minor_src_(),
    major_src_(),
    src_ls_meta_package_(),
    sys_tablet_id_array_(),
    data_tablet_id_array_(),
    ha_table_info_mgr_(),
    check_tablet_info_cost_time_(0)
{
  local_clog_checkpoint_scn_.set_min();
}

ObMigrationCtx::~ObMigrationCtx()
{
}

bool ObMigrationCtx::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && tenant_id_ != 0 && tenant_id_ != OB_INVALID_ID;
}

void ObMigrationCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  arg_.reset();
  local_clog_checkpoint_scn_.set_min();
  local_rebuild_seq_ = -1;
  start_ts_ = 0;
  finish_ts_ = 0;
  task_id_.reset();
  minor_src_.reset();
  major_src_.reset();
  src_ls_meta_package_.reset();
  ha_table_info_mgr_.reuse();
  tablet_group_mgr_.reuse();
  ObIHADagNetCtx::reset();
  check_tablet_info_cost_time_ = 0;
  tablet_simple_info_map_.reuse();
}

int ObMigrationCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("log stream migration ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int n = snprintf(buf, buf_len,
        "ls migration : task_id = %s, tenant_id = %s, ls_id = %s, op_type = %s, src = %s, dest = %s",
        to_cstring(task_id_),
        to_cstring(tenant_id_),
        to_cstring(arg_.ls_id_),
        ObMigrationOpType::get_str(arg_.type_),
        to_cstring(arg_.data_src_.get_server()),
        to_cstring(arg_.dst_.get_server()));
    if (n < 0 || n >= buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) { // 10s
        STORAGE_LOG(WARN, "buf not enough, comment is truncated", K(ret), K(n), K(buf));
      }
    }
  }
  return ret;
}

void ObMigrationCtx::reuse()
{
  local_clog_checkpoint_scn_.set_min();
  local_rebuild_seq_ = -1;
  minor_src_.reset();
  major_src_.reset();
  src_ls_meta_package_.reset();
  sys_tablet_id_array_.reset();
  data_tablet_id_array_.reset();
  ha_table_info_mgr_.reuse();
  tablet_group_mgr_.reuse();
  ObIHADagNetCtx::reuse();
  check_tablet_info_cost_time_ = 0;
  tablet_simple_info_map_.reuse();
}

/******************ObCopyTabletCtx*********************/
ObCopyTabletCtx::ObCopyTabletCtx()
  : tablet_id_(),
    tablet_handle_(),
    lock_(common::ObLatchIds::MIGRATE_LOCK),
    status_(ObCopyTabletStatus::MAX_STATUS)
{
}

ObCopyTabletCtx::~ObCopyTabletCtx()
{
}

bool ObCopyTabletCtx::is_valid() const
{
  return tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && tablet_handle_.is_valid())
          || ObCopyTabletStatus::TABLET_NOT_EXIST == status_);
}

void ObCopyTabletCtx::reset()
{
  tablet_id_.reset();
  tablet_handle_.reset();
  status_ = ObCopyTabletStatus::MAX_STATUS;
}

int ObCopyTabletCtx::set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status)
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

int ObCopyTabletCtx::get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const
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

/******************ObMigrationDagNet*********************/
ObMigrationDagNetInitParam::ObMigrationDagNetInitParam()
  : arg_(),
    task_id_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    sql_proxy_(nullptr)
{
}

bool ObMigrationDagNetInitParam::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && OB_NOT_NULL(bandwidth_throttle_)
      && OB_NOT_NULL(svr_rpc_proxy_)
      && OB_NOT_NULL(storage_rpc_)
      && OB_NOT_NULL(sql_proxy_);
}


ObMigrationDagNet::ObMigrationDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_MIGARTION),
      is_inited_(false),
      ctx_(nullptr),
      bandwidth_throttle_(nullptr),
      svr_rpc_proxy_(nullptr),
      storage_rpc_(nullptr),
      sql_proxy_(nullptr)

{
}

ObMigrationDagNet::~ObMigrationDagNet()
{
  LOG_INFO("ls migration dag net free", KPC(ctx_));
  free_migration_ctx_();
}

int ObMigrationDagNet::alloc_migration_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("log stream migration ctx init twice", K(ret), KPC(ctx_));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObMigrationCtx), "MigrationCtx"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ctx_ = new (buf) ObMigrationCtx())) {
  }
  return ret;
}

void ObMigrationDagNet::free_migration_ctx_()
{
  if (OB_ISNULL(ctx_)) {
    //do nothing
  } else {
    ctx_->~ObMigrationCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

int ObMigrationDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObMigrationDagNetInitParam* init_param = static_cast<const ObMigrationDagNetInitParam*>(param);
  const int64_t MAX_BUCKET_NUM = 8192;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migration dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(alloc_migration_ctx_())) {
    LOG_WARN("failed to alloc migration ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.init())) {
    LOG_WARN("failed to init ha table key mgr", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->tablet_group_mgr_.init())) {
    LOG_WARN("failed to init tablet group mgr", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->tablet_simple_info_map_.create(MAX_BUCKET_NUM, "SHATaskBucket", "SHATaskNode", MTL_ID()))) {
    LOG_WARN("failed to create tablet simple info map", K(ret));
  } else {
    ctx_->tenant_id_ = MTL_ID();
    ctx_->arg_ = init_param->arg_;
    ctx_->task_id_ = init_param->task_id_;
    bandwidth_throttle_ = init_param->bandwidth_throttle_;
    svr_rpc_proxy_ = init_param->svr_rpc_proxy_;
    storage_rpc_ = init_param->storage_rpc_;
    sql_proxy_ = init_param->sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

int ObMigrationDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_migration_())) {
    LOG_WARN("failed to start running for migration", K(ret));
  }

  return ret;
}

int ObMigrationDagNet::start_running_for_migration_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialMigrationDag *initial_migration_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_->start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_migration_dag))) {
    LOG_WARN("failed to alloc migration dag ", K(ret));
  } else if (OB_FAIL(initial_migration_dag->init(this))) {
    LOG_WARN("failed to init migration dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_migration_dag))) {
    LOG_WARN("failed to add migration init dag into dag net", K(ret));
  } else if (OB_FAIL(initial_migration_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_migration_dag))) {
    LOG_WARN("failed to add migration finish dag", K(ret), K(*initial_migration_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_migration_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_migration_dag) && OB_NOT_NULL(scheduler)) {
    initial_migration_dag->reset_children();
    if (OB_SUCCESS != (tmp_ret = erase_dag_from_dag_net(*initial_migration_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_migration_dag));
    }
    scheduler->free_dag(*initial_migration_dag);
  }

  return ret;
}

bool ObMigrationDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDagNet &other_migration_dag = static_cast<const ObMigrationDagNet &>(other);
    if (OB_ISNULL(other_migration_dag.ctx_) || OB_ISNULL(ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "migration ctx is NULL", KPC(ctx_), KPC(other_migration_dag.ctx_));
    } else if (ctx_->arg_.ls_id_ != other_migration_dag.ctx_->arg_.ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObMigrationDagNet::hash() const
{
  int64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "migration ctx is NULL", KPC(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_->arg_.ls_id_, sizeof(ctx_->arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObMigrationDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("log stream migration dag net do not init ", K(ret));
  } else if (OB_FAIL(ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    LOG_WARN("failed to trace task id to string", K(ret), K(*ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "ObLSMigrationDagNet: tenant_id=%s, ls_id=%s, migration_type=%d, trace_id=%s",
          to_cstring(ctx_->tenant_id_), to_cstring(ctx_->arg_.ls_id_), ctx_->arg_.type_, task_id_str))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

int ObMigrationDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObLSMigrationDagNet: ls_id = %s, migration_type = %s",
      to_cstring(ctx_->arg_.ls_id_), ObMigrationOpType::get_str(ctx_->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

int ObMigrationDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  int32_t result = OB_SUCCESS;
  ObLSHandle ls_handle;
  LOG_INFO("start clear dag net ctx", KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration dag net do not init", K(ret));
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
    FLOG_INFO("finish migration dag net", "ls id", ctx_->arg_.ls_id_, "type", ctx_->arg_.type_, K(cost_ts), K(result));
  }
  return ret;
}

int ObMigrationDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls migration dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/******************ObMigrationDag*********************/
ObMigrationDag::ObMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObMigrationDag::~ObMigrationDag()
{
}

int ObMigrationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration dag migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->arg_.ls_id_.id(),
                                static_cast<int64_t>(ctx->arg_.type_),
                                "dag_net_task_id", to_cstring(ctx->task_id_),
                                "src", to_cstring(ctx->arg_.src_.get_server()),
                                "dest", to_cstring(ctx->arg_.dst_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

/******************ObInitialMigrationDag*********************/
ObInitialMigrationDag::ObInitialMigrationDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_INITIAL_MIGRATION),
    is_inited_(false)
{
}

ObInitialMigrationDag::~ObInitialMigrationDag()
{
}

bool ObInitialMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  ObMigrationCtx *ctx = nullptr;

  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ctx = get_migration_ctx();
    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_migration_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "migration ctx should not be NULL", KP(ctx), KP(other_dag.get_migration_ctx()));
    } else if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObInitialMigrationDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx * ctx = get_migration_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "migration ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObInitialMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inital migration dag migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObInitialMigrationDag: ls_id = %s, migration_type = %s",
       to_cstring(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObInitialMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInitialMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial migration dag dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObInitialMigrationTask*********************/
ObInitialMigrationTask::ObInitialMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    dag_net_(nullptr)
{
}

ObInitialMigrationTask::~ObInitialMigrationTask()
{
}

int ObInitialMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_migration_ctx();
    bandwidth_throttle_ = migration_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = migration_dag_net->get_storage_rpc();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init initial migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObInitialMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("storage_ha", "before_prepare_migration_task");
  DEBUG_SYNC(BEFORE_PREPARE_MIGRATION_TASK);
#endif

#ifdef ERRSIM
  const int64_t errsim_migration_ls_id = GCONF.errsim_migration_ls_id;
  if (!is_meta_tenant(ctx_->tenant_id_) && errsim_migration_ls_id == ctx_->arg_.ls_id_.id()) {
    SERVER_EVENT_SYNC_ADD("storage_ha", "errsim_before_initial_migration",
                          "tenant_id", ctx_->tenant_id_,
                          "ls_id", errsim_migration_ls_id);
    DEBUG_SYNC(BEFORE_INITIAL_MIGRATION_TASK);
  }
#endif
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial migration task do not init", K(ret));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_INITIAL_MIGRATION_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_INITIAL_MIGRATION_TASK_FAILED", K(ret));
      }
    }
#endif
    if (FAILEDx(generate_migration_dags_())) {
      LOG_WARN("failed to generate migration dags", K(ret), K(*ctx_));
    }
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

int ObInitialMigrationTask::generate_migration_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartMigrationDag *start_migration_dag = nullptr;
  ObMigrationFinishDag *migration_finish_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialMigrationDag *initial_migration_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration init task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_migration_dag = static_cast<ObInitialMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial migration dag is null", K(ret), KP(initial_migration_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(start_migration_dag))) {
    LOG_WARN("failed to alloc start migration dag ", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(migration_finish_dag))) {
    LOG_WARN("failed to alloc migration finish dag", K(ret));
  } else if (OB_FAIL(start_migration_dag->init(dag_net_))) {
    LOG_WARN("failed to init start migration dag", K(ret));
  } else if (OB_FAIL(migration_finish_dag->init(dag_net_))) {
    LOG_WARN("failed to init migration finish dag", K(ret));
  } else if (OB_FAIL(this->get_dag()->add_child(*start_migration_dag))) {
    LOG_WARN("failed to add start migration dag", K(ret), KPC(start_migration_dag));
  } else if (OB_FAIL(start_migration_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(start_migration_dag->add_child(*migration_finish_dag))) {
    LOG_WARN("failed to add migration finish dag as child", K(ret));
  } else if (OB_FAIL(migration_finish_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(migration_finish_dag))) {
    LOG_WARN("failed to add migration finish dag", K(ret), K(*migration_finish_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(start_migration_dag))) {
    LOG_WARN("failed to add dag", K(ret), K(*start_migration_dag));
    if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(migration_finish_dag, start_migration_dag))) {
      LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(initial_migration_dag));
    } else {
      migration_finish_dag = nullptr;
    }

    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("succeed to schedule migration dag", K(*start_migration_dag));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(migration_finish_dag)) {
      scheduler->free_dag(*migration_finish_dag, start_migration_dag);
      migration_finish_dag = nullptr;
    }
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_migration_dag)) {
      scheduler->free_dag(*start_migration_dag, initial_migration_dag);
      start_migration_dag = nullptr;
    }
    const bool need_retry = true;
    if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, need_retry,
        this->get_dag()->get_type()))) {
      LOG_WARN("failed to set migration result", K(ret), K(tmp_ret), K(*ctx_));
    }
  }
  return ret;
}

int ObInitialMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "initial_migration_task",
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

/******************ObStartMigrationDag*********************/
ObStartMigrationDag::ObStartMigrationDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_START_MIGRATION),
    is_inited_(false)
{
}

ObStartMigrationDag::~ObStartMigrationDag()
{
}

bool ObStartMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObStartMigrationDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = get_migration_ctx();

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObStartMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObStartMigrationDag: ls_id = %s, migration_type = %s",
       to_cstring(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObStartMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStartMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartMigrationTask*********************/
ObStartMigrationTask::ObStartMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr)
{
}

ObStartMigrationTask::~ObStartMigrationTask()
{
}

int ObStartMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_migration_ctx();
    bandwidth_throttle_ = migration_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = migration_dag_net->get_storage_rpc();
    ctx_->reuse();
    is_inited_ = true;
    LOG_INFO("succeed init start migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObStartMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_copy_data = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(try_remove_member_list_())) {
    LOG_WARN("failed to try remove member list", K(ret));
  } else if (OB_FAIL(deal_with_local_ls_())) {
    LOG_WARN("failed to deal with local ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(check_ls_need_copy_data_(need_copy_data))) {
    LOG_WARN("failed to check ls need copy data", K(ret), KPC(ctx_));
  } else if (!need_copy_data) {
    //do nothing
  } else if (OB_FAIL(report_ls_meta_table_())) {
    LOG_WARN("failed to report ls meta table", K(ret), KPC(ctx_));
  } else if (OB_FAIL(choose_src_())) {
    LOG_WARN("failed to choose src", K(ret), KPC(ctx_));
  } else if (OB_FAIL(build_ls_())) {
    LOG_WARN("failed to build ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(fill_restore_arg_if_needed_())) {
    LOG_WARN("failed to fill restore arg", K(ret), KPC(ctx_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_START_MIGRATION_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_START_MIGRATION_TASK_FAILED", K(ret));
      }
    }
#endif
    if (FAILEDx(generate_tablets_migration_dag_())) {
      LOG_WARN("failed to generate sys tablets dag", K(ret), K(*ctx_));
    }
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObStartMigrationTask::try_remove_member_list_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const int64_t change_member_list_timeout_us = GCONF.sys_bkgd_migration_change_member_list_timeout;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (ObMigrationOpType::CHANGE_LS_OP != ctx_->arg_.type_) {
    LOG_INFO("migration op is not change log stream op, no need remove", K(*ctx_));
  } else {
    const ObReplicaType src_type = ctx_->arg_.src_.get_replica_type();
    const ObReplicaType dest_type = ctx_->arg_.dst_.get_replica_type();
    const ObAddr &self_addr = MYADDR;

    if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
      LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
    } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", KR(ret), K(*ctx_), KP(ls));
    } else if (self_addr != ctx_->arg_.src_.get_server()
        || self_addr != ctx_->arg_.dst_.get_server()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("replica type do not match", K(ret), K(self_addr), K(src_type), K(dest_type));
    } else if (src_type == dest_type) {
      ret = OB_ALREADY_DONE;
      LOG_WARN("src type and dest type is same, no need change", K(ret), K(src_type), K(dest_type));
    } else if (!ObReplicaTypeCheck::change_replica_op_allow(src_type, dest_type)) {
      ret = OB_OP_NOT_ALLOW;
      STORAGE_LOG(WARN, "change replica op not allow", K(src_type), K(dest_type), K(ret));
    } else if (!(ObReplicaTypeCheck::is_paxos_replica(src_type)
               && !ObReplicaTypeCheck::is_paxos_replica(dest_type))) {
      LOG_INFO("no need remove member list");
    } else if (OB_FAIL(ls->remove_member(ctx_->arg_.dst_, ctx_->arg_.paxos_replica_number_, change_member_list_timeout_us))) {
      LOG_WARN("failed to remove member", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObStartMigrationTask::deal_with_local_ls_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObRole role;
  int64_t proposal_id = 0;
  ObLSMeta local_ls_meta;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("log stream should not be NULL", K(ret), K(*ctx_));
  } else if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("failed to get role", K(ret), "arg", ctx_->arg_);
  } else if (is_strong_leader(role)) {
    if (ObMigrationOpType::REBUILD_LS_OP == ctx_->arg_.type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("leader can not as rebuild dst", K(ret), K(role), "myaddr", MYADDR, "arg", ctx_->arg_);
    } else if (ObMigrationOpType::ADD_LS_OP == ctx_->arg_.type_
        || ObMigrationOpType::MIGRATE_LS_OP == ctx_->arg_.type_
        || ObMigrationOpType::CHANGE_LS_OP == ctx_->arg_.type_) {
      ret = OB_ERR_SYS;
      LOG_WARN("leader cannot as add, migrate, change dst",
          K(ret), K(role), "myaddr", MYADDR, "arg", ctx_->arg_);
    }
  } else if (OB_FAIL(ls->offline())) {
    LOG_WARN("failed to disable log", K(ret), KPC(ctx_));
  } else if (ObMigrationOpType::REBUILD_LS_OP == ctx_->arg_.type_) {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_UPDATE_LS_MIGRATION_STATUS_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_UPDATE_LS_MIGRATION_STATUS_FAILED", K(ret));
      }
    }
#endif

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls->set_ls_rebuild())) {
      LOG_WARN("failed to set ls rebuild", K(ret), KPC(ctx_));
    }
  } else {
    ObRebuildService *rebuild_service = nullptr;
    if (OB_ISNULL(rebuild_service = MTL(ObRebuildService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rebuild service should not be NULL", K(ret), KP(rebuild_service));
    } else if (OB_FAIL(rebuild_service->remove_rebuild_ls(ctx_->arg_.ls_id_))) {
      LOG_WARN("failed to remove rebuild ls", K(ret), KPC(ctx_));
    }
  }

  if (OB_FAIL(ret) || OB_ISNULL(ls)) {
    //do nothing
  } else {
    if (OB_FAIL(ls->get_ls_meta(local_ls_meta))) {
      LOG_WARN("failed to get ls meta", K(ret), "arg", ctx_->arg_);
    } else {
      ctx_->local_clog_checkpoint_scn_ = local_ls_meta.get_clog_checkpoint_scn();
      ctx_->local_rebuild_seq_ = local_ls_meta.get_rebuild_seq();
    }
  }
  return ret;
}

int ObStartMigrationTask::report_ls_meta_table_()
{
  int ret = OB_SUCCESS;
  ObMigrationStatus migration_status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ObMigrationStatusHelper::trans_migration_op(
      ctx_->arg_.type_, migration_status))) {
    LOG_WARN("failed to trans migration op", K(ret), K(ctx_));
  } else {
    const uint64_t tenant_id = ctx_->tenant_id_;
    const share::ObLSID &ls_id = ctx_->arg_.ls_id_;
    if (OB_FAIL(ObStorageHAUtils::report_ls_meta_table(tenant_id, ls_id, migration_status))) {
      LOG_WARN("failed to report ls meta table", K(ret), K(tenant_id), K(ls_id));
    }
  }
  DEBUG_SYNC(AFTER_MIGRATION_REPORT_LS_META_TABLE);
  return ret;
}

int ObStartMigrationTask::choose_src_()
{
  int ret = OB_SUCCESS;
  storage::ObStorageHASrcProvider src_provider;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    const uint64_t tenant_id = ctx_->tenant_id_;
    const share::ObLSID &ls_id = ctx_->arg_.ls_id_;
    ObStorageHASrcInfo src_info;
    obrpc::ObCopyLSInfo ls_info;
    SCN local_clog_checkpoint_scn = SCN::min_scn();
    if (OB_FAIL(get_local_ls_checkpoint_scn_(local_clog_checkpoint_scn))) {
      LOG_WARN("failed to get local ls checkpoint ts", K(ret));
    } else if (OB_FAIL(src_provider.init(tenant_id, ctx_->arg_.type_, storage_rpc_))) {
      LOG_WARN("failed to init src provider", K(ret), K(tenant_id), "type", ctx_->arg_.type_);
    } else if (OB_FAIL(src_provider.choose_ob_src(ls_id, local_clog_checkpoint_scn, src_info))) {
      LOG_WARN("failed to choose ob src", K(ret), K(tenant_id), K(ls_id), K(local_clog_checkpoint_scn));
    } else if (OB_FAIL(fetch_ls_info_(tenant_id, ls_id, src_info.src_addr_, ls_info))) {
      LOG_WARN("failed to fetch ls info", K(ret), K(tenant_id), K(ls_id), K(src_info));
    } else if (OB_FAIL(ObStorageHAUtils::check_server_version(ls_info.version_))) {
      LOG_WARN("failed to check server version", K(ret), K(ls_id), K(ls_info));
    } else {
      ctx_->minor_src_ = src_info;
      ctx_->major_src_ = src_info;
      ctx_->src_ls_meta_package_ = ls_info.ls_meta_package_;
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_info.tablet_id_array_.count(); ++i) {
        const ObTabletID &tablet_id = ls_info.tablet_id_array_.at(i);
        if (tablet_id.is_ls_inner_tablet()) {
          if (OB_FAIL(ctx_->sys_tablet_id_array_.push_back(tablet_id))) {
            LOG_WARN("failed to push tablet id into array", K(ret));
          }
        } else {
          if (OB_FAIL(ctx_->data_tablet_id_array_.push_back(tablet_id))) {
            LOG_WARN("failed to push tablet id into array", K(ret));
          }
        }
      }
#if ERRSIM
      const ObString &errsim_server = GCONF.errsim_migration_src_server_addr.str();
      if (!errsim_server.empty() && !ctx_->arg_.ls_id_.is_sys_ls() && ctx_->arg_.type_ != ObMigrationOpType::REBUILD_LS_OP) {
        SERVER_EVENT_ADD("storage_ha", "after_choose_src",
                         "tenant_id", ctx_->tenant_id_,
                         "ls_id", ctx_->arg_.ls_id_.id(),
                         "local_rebuild_seq", ctx_->local_rebuild_seq_,
                         "transfer_scn", ctx_->src_ls_meta_package_.ls_meta_.get_transfer_scn());
        DEBUG_SYNC(ALTER_LS_CHOOSE_SRC);
      }
#endif

      FLOG_INFO("succeed choose src",  K(src_info),
          K(ls_info), K(ctx_->sys_tablet_id_array_), K(ctx_->data_tablet_id_array_));
    }
  }
  return ret;
}

int ObStartMigrationTask::fetch_ls_info_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObAddr &member_addr, obrpc::ObCopyLSInfo &ls_info)
{
  int ret = OB_SUCCESS;
  ls_info.reset();
  ObStorageHASrcInfo src_info;
  src_info.src_addr_ = member_addr;
  src_info.cluster_id_ = GCONF.cluster_id;
  if (OB_ISNULL(storage_rpc_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc should not be null", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !member_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id), K(member_addr));
  } else if (OB_FAIL(storage_rpc_->post_ls_info_request(tenant_id, src_info, ls_id, ls_info))) {
    LOG_WARN("failed to post ls info request", K(ret), K(tenant_id), K(src_info), K(ls_id));
  }
  return ret;
}

int ObStartMigrationTask::get_local_ls_checkpoint_scn_(SCN &local_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  local_checkpoint_scn.set_min();
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSMeta local_ls_meta;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_SYS;
    LOG_WARN("log stream should not be NULL", K(ret), K(*ctx_));
  } else if (OB_FAIL(ls->get_ls_meta(local_ls_meta))) {
    LOG_WARN("failed to get ls meta", K(ret), "arg", ctx_->arg_);
  } else {
    local_checkpoint_scn = local_ls_meta.get_clog_checkpoint_scn();
  }
  return ret;
}

int ObStartMigrationTask::update_ls_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  const bool update_restore_status = true;
  palf::LSN end_lsn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx_));
  } else {
    ObLSLockGuard lock_ls(ls);
    const bool is_rebuild = (ctx_->arg_.type_ == ObMigrationOpType::REBUILD_LS_OP);
    if (OB_FAIL(ls->update_ls_meta(update_restore_status,
                                   ctx_->src_ls_meta_package_.ls_meta_))) {
      LOG_WARN("failed to update ls meta", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ls->set_dup_table_ls_meta(ctx_->src_ls_meta_package_.dup_ls_meta_,
                                                 true /*need_flush_slog*/))) {
      LOG_WARN("failed to set dup table ls meta", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ls->get_end_lsn(end_lsn))) {
      LOG_WARN("failed to get end lsn", K(ret), KPC(ctx_));
    } else if (end_lsn >= ctx_->src_ls_meta_package_.palf_meta_.curr_lsn_) {
      LOG_INFO("end lsn is bigger than src curr lsn, skip advance base info", "end_lsn",
          end_lsn, "src palf meta", ctx_->src_ls_meta_package_.palf_meta_,
          "ls_id", ctx_->arg_.ls_id_);
    } else if (OB_FAIL(ls->advance_base_info(ctx_->src_ls_meta_package_.palf_meta_, is_rebuild))) {
      LOG_WARN("failed to advance base lsn for migration", K(ret), KPC(ctx_));
    } else {
      ctx_->local_clog_checkpoint_scn_ = ctx_->src_ls_meta_package_.ls_meta_.get_clog_checkpoint_scn();
    }

    if (OB_SUCC(ret)) {
      ctx_->local_rebuild_seq_ = ctx_->src_ls_meta_package_.ls_meta_.get_rebuild_seq();
      LOG_INFO("update rebuild seq", "old_ls_rebuld_seq", ctx_->local_rebuild_seq_,
          "new_ls_rebuild_seq", ctx_->src_ls_meta_package_.ls_meta_.get_rebuild_seq(), K(lbt()));
    }
  }
  return ret;
}

int ObStartMigrationTask::generate_tablets_migration_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSysTabletsMigrationDag *sys_tablets_migration_dag = nullptr;
  ObDataTabletsMigrationDag *data_tablets_migration_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObStartMigrationDag *start_migration_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_ISNULL(start_migration_dag = static_cast<ObStartMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be NULL", K(ret), KP(start_migration_dag));
  } else if (OB_ISNULL(dag_net = start_migration_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(sys_tablets_migration_dag))) {
      LOG_WARN("failed to alloc sys tablets migration dag ", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag(data_tablets_migration_dag))) {
      LOG_WARN("failed to alloc data tablets migration dag ", K(ret));
    } else if (OB_FAIL(sys_tablets_migration_dag->init(dag_net))) {
      LOG_WARN("failed to init sys tablets migration dag", K(ret), K(*ctx_));
    } else if (OB_FAIL(data_tablets_migration_dag->init(dag_net))) {
      LOG_WARN("failed to init data tablets migration dag", K(ret), K(*ctx_));
    } else if (OB_FAIL(this->get_dag()->add_child(*sys_tablets_migration_dag))) {
      LOG_WARN("failed to add sys tablets migration dag as chilid", K(ret), K(*ctx_));
    } else if (OB_FAIL(sys_tablets_migration_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(sys_tablets_migration_dag->add_child(*data_tablets_migration_dag))) {
      LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
    } else if (OB_FAIL(data_tablets_migration_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = EN_BUILD_SYS_TABLETS_DAG_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_BUILD_SYS_TABLETS_DAG_FAILED", K(ret));
      }
    }
#endif

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scheduler->add_dag(sys_tablets_migration_dag))) {
      LOG_WARN("failed to add sys tablets migration dag", K(ret), K(*sys_tablets_migration_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(scheduler->add_dag(data_tablets_migration_dag))) {
      LOG_WARN("failed to add data tablets migration dag", K(ret), K(*data_tablets_migration_dag));
      if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(sys_tablets_migration_dag, start_migration_dag))) {
        LOG_WARN("failed to cancel ha dag", K(ret), KPC(start_migration_dag));
      } else {
        sys_tablets_migration_dag = nullptr;
      }

      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("succeed to schedule sys tablets migration dag and data tablets migration dag",
          K(*sys_tablets_migration_dag), K(*data_tablets_migration_dag));
    }

  #ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_MIGRATION_GENERATE_SYS_TABLETS_DAG_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_MIGRATION_GENERATE_SYS_TABLETS_DAG_FAILED", K(ret));

        if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(data_tablets_migration_dag, sys_tablets_migration_dag))) {
          LOG_WARN("failed to cancel ha dag", K(ret), KPC(sys_tablets_migration_dag));
        } else {
          data_tablets_migration_dag = nullptr;
        }

        if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(sys_tablets_migration_dag, start_migration_dag))) {
          LOG_WARN("failed to cancel ha dag", K(ret), KPC(start_migration_dag));
        } else {
          sys_tablets_migration_dag = nullptr;
        }
      }
    }
  #endif

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(data_tablets_migration_dag)) {
        scheduler->free_dag(*data_tablets_migration_dag, sys_tablets_migration_dag);
        data_tablets_migration_dag = nullptr;
      }

      if (OB_NOT_NULL(sys_tablets_migration_dag)) {
        scheduler->free_dag(*sys_tablets_migration_dag, start_migration_dag);
        sys_tablets_migration_dag = nullptr;
      }
    }
  }
  return ret;
}

int ObStartMigrationTask::get_tablet_id_array_(
    common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  tablet_id_array.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->sys_tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->sys_tablet_id_array_.at(i);
      if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
        LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->data_tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->data_tablet_id_array_.at(i);
      if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
        LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObStartMigrationTask::check_ls_need_copy_data_(bool &need_copy)
{
  int ret = OB_SUCCESS;
  need_copy = true;
  ObLSHandle ls_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (ObMigrationOpType::CHANGE_LS_OP == ctx_->arg_.type_ &&
      ObReplicaTypeCheck::is_readable_replica(ctx_->arg_.src_.get_replica_type())) {
    //no need generate copy task, only change member
    need_copy = false;
    LOG_INFO("no need change replica no need copy task", "src_type", ctx_->arg_.src_.get_replica_type(),
        " dest_type", ctx_->arg_.dst_.get_replica_type());
  }
  return ret;
}

int ObStartMigrationTask::check_before_ls_migrate_(const ObLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  ObLSRestoreStatus ls_restore_status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(ls_meta.get_restore_status(ls_restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ctx_));
  } else if (ls_restore_status.is_restore_failed()) {
    ret = OB_LS_RESTORE_FAILED;
    LOG_WARN("ls restore failed, cannot migrate", K(ret), KPC(ctx_), K(ls_restore_status));
  } else if (!ls_restore_status.can_migrate()) {
    ret = OB_SRC_DO_NOT_ALLOWED_MIGRATE;
    LOG_WARN("src ls is in restore status, cannot migrate, wait later", K(ret), K(ls_restore_status));
  }
  return ret;
}

int ObStartMigrationTask::build_ls_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else {
    if (OB_FAIL(inner_build_ls_())) {
      LOG_WARN("failed to do inner build ls", K(ret));
    }

    if (OB_NOT_SUPPORTED == ret) {
      //build ls with old rpc, overwrite ret
      if (OB_FAIL(inner_build_ls_with_old_rpc_())) {
        LOG_WARN("failed to do inner build ls with old rpc", K(ret));
      }
    }
  }
  return ret;
}

int ObStartMigrationTask::inner_build_ls_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObCopyLSViewInfoObReader *ob_reader = nullptr;
  obrpc::ObCopyLSViewArg arg;
  arg.tenant_id_ = ctx_->tenant_id_;
  arg.ls_id_ = ctx_->arg_.ls_id_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (FALSE_IT(buf = ob_malloc(sizeof(ObCopyLSViewInfoObReader), "CopyLSViewRead"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (FALSE_IT(ob_reader = new (buf) ObCopyLSViewInfoObReader())) {
  } else if (OB_FAIL(ob_reader->init(ctx_->minor_src_, arg, *svr_rpc_proxy_, *bandwidth_throttle_))) {
    LOG_WARN("failed to init tablet ob reader", K(ret), KPC(ctx_), K(arg));
  } else if (OB_FAIL(ob_reader->get_ls_meta(ctx_->src_ls_meta_package_))) {
    LOG_WARN("fail to read ls meta infos", K(ret));
  } else if (OB_FAIL(check_before_ls_migrate_(ctx_->src_ls_meta_package_.ls_meta_))) {
    LOG_WARN("failed to check before ls migrate", K(ret), KPC(ctx_));
  } else if (OB_FAIL(update_ls_())) {
    LOG_WARN("failed to update local ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(create_all_tablets_(ob_reader))) {
    LOG_WARN("failed to create all tablets", K(ret), KPC(ctx_));
  }

  if (OB_NOT_NULL(ob_reader)) {
    ob_reader->~ObCopyLSViewInfoObReader();
    ob_free(ob_reader);
    ob_reader = nullptr;
  }
  return ret;
}

int ObStartMigrationTask::create_all_tablets_(
    ObCopyLSViewInfoObReader *ob_reader)
{
  int ret = OB_SUCCESS;
  ObStorageHATabletsBuilder ha_tablets_builder;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObArray<ObTabletID> tablet_id_array;
  bool need_check_tablet_limit = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_ISNULL(ob_reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create all tablets get ivnalid argument", K(ret));
  } else if (FALSE_IT(need_check_tablet_limit = ctx_->arg_.type_ != ObMigrationOpType::REBUILD_LS_OP)) {
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(ObLSMigrationUtils::init_ha_tablets_builder(
      ctx_->tenant_id_, tablet_id_array, ctx_->minor_src_, ctx_->local_rebuild_seq_, ctx_->arg_.type_,
      ls, &ctx_->ha_table_info_mgr_, ha_tablets_builder))) {
    LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ha_tablets_builder.create_all_tablets(need_check_tablet_limit, ob_reader,
      ctx_->sys_tablet_id_array_, ctx_->data_tablet_id_array_,
      ctx_->tablet_simple_info_map_))) {
    LOG_WARN("failed to create all tablets", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartMigrationTask::fill_restore_arg_if_needed_()
{
  // As the source log stream status can be ignored during transfer when log scn
  // is before restore consistent scn. So, we should ensure consistent scn is
  // valid when replaying transfer log during migration.
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSRestoreStatus restore_status;
  if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(ls->get_restore_status(restore_status))) {
    LOG_WARN("failed to get restore status", K(ret), KPC(ls), KPC(ctx_));
  } else if (!restore_status.is_in_restore()) {
    // do nothing
  } else if (OB_FAIL(ls->get_ls_restore_handler()->fill_restore_arg())) {
    LOG_WARN("failed to fill restore arg", K(ret), KPC(ls), KPC(ctx_));
  } else {
    LOG_INFO("succeed fill restore arg during migration", "ls_id", ctx_->arg_.ls_id_, K(restore_status));
  }

  return ret;
}

int ObStartMigrationTask::inner_build_ls_with_old_rpc_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(update_ls_())) {
    LOG_WARN("failed to update local ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(create_all_tablets_with_4_1_rpc_())) {
    LOG_WARN("failed to create all tablets with 4_1 rpc", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartMigrationTask::create_all_tablets_with_4_1_rpc_()
{
  int ret = OB_SUCCESS;
  ObStorageHATabletsBuilder ha_tablets_builder;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObArray<ObTabletID> tablet_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(append(tablet_id_array, ctx_->sys_tablet_id_array_))) {
    LOG_WARN("failed to append sys tablet id array", K(ret), KPC(ctx_));
  } else if (OB_FAIL(append(tablet_id_array, ctx_->data_tablet_id_array_))) {
    LOG_WARN("failed to append data tablet id array", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(ObLSMigrationUtils::init_ha_tablets_builder(
      ctx_->tenant_id_, tablet_id_array, ctx_->minor_src_, ctx_->local_rebuild_seq_, ctx_->arg_.type_,
      ls, &ctx_->ha_table_info_mgr_, ha_tablets_builder))) {
    LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ha_tablets_builder.create_all_tablets_with_4_1_rpc(
      ctx_->tablet_simple_info_map_))) {
    LOG_WARN("failed to create all tablets", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "start_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "data_tablet_count", ctx_->data_tablet_id_array_.count(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}

/******************ObSysTabletsMigrationDag*********************/
ObSysTabletsMigrationDag::ObSysTabletsMigrationDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_SYS_TABLETS_MIGRATION),
    is_inited_(false)
{
}

ObSysTabletsMigrationDag::~ObSysTabletsMigrationDag()
{
}

bool ObSysTabletsMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObSysTabletsMigrationDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = get_migration_ctx();

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObSysTabletsMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObSysTabletsMigrationDag: ls_id = %s, migration_type = %s",
       to_cstring(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObSysTabletsMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sys tablets migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSysTabletsMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSysTabletsMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init sys tablets migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObSysTabletsMigrationTask*********************/
ObSysTabletsMigrationTask::ObSysTabletsMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ls_handle_(),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    ha_tablets_builder_()

{
}

ObSysTabletsMigrationTask::~ObSysTabletsMigrationTask()
{
}

int ObSysTabletsMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sys tablets migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_migration_ctx();
    bandwidth_throttle_ = migration_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = migration_dag_net->get_storage_rpc();

    if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle_))) {
      LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ObLSMigrationUtils::init_ha_tablets_builder(
        ctx_->tenant_id_, ctx_->sys_tablet_id_array_, ctx_->minor_src_, ctx_->local_rebuild_seq_, ctx_->arg_.type_,
        ls_handle_.get_ls(), &ctx_->ha_table_info_mgr_, ha_tablets_builder_))) {
      LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed init sys tablets migration task", "ls id", ctx_->arg_.ls_id_,
          "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_,
          "sys_tablet_list", ctx_->sys_tablet_id_array_);
    }
  }
  return ret;
}

int ObSysTabletsMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(build_tablets_sstable_info_())) {
    LOG_WARN("failed to build tablets sstable info", K(ret), K(*ctx_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_SYS_TABLETS_MIGRATION_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_SYS_TABLETS_MIGRATION_TASK_FAILED", K(ret));
      }
    }
#endif
    if (FAILEDx(generate_sys_tablet_migartion_dag_())) {
      LOG_WARN("failed to generate sys tablet migration dag", K(ret), K(*ctx_));
    }
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObSysTabletsMigrationTask::build_tablets_sstable_info_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets migration task do not init", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ha_tablets_builder_.build_tablets_sstable_info())) {
    LOG_WARN("failed to build tablets sstable info", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObSysTabletsMigrationTask::generate_sys_tablet_migartion_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObIDag *> tablet_migration_dag_array;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObSysTabletsMigrationDag *sys_tablets_migration_dag = nullptr;
  ObLS *ls = nullptr;
  ObIDag *parent = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets migration task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_ISNULL(sys_tablets_migration_dag = static_cast<ObSysTabletsMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tablets migration dag should not be NULL", K(ret), KP(sys_tablets_migration_dag));
  } else if (OB_ISNULL(dag_net = sys_tablets_migration_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FALSE_IT(parent = this->get_dag())) {
  } else if (OB_FAIL(tablet_migration_dag_array.push_back(parent))) {
    LOG_WARN("failed to push sys_tablets_migration_dag into array", K(ret), K(*ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->sys_tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->sys_tablet_id_array_.at(i);
      ObTabletMigrationDag *tablet_migration_dag = nullptr;
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_migration_dag))) {
        LOG_WARN("failed to alloc tablet migration dag ", K(ret));
      } else if (OB_FAIL(tablet_migration_dag_array.push_back(tablet_migration_dag))) {
        LOG_WARN("failed to push tablet migration dag into array", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_migration_dag->init(tablet_id, tablet_handle, dag_net))) {
        LOG_WARN("failed to init tablet migration dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(parent->add_child(*tablet_migration_dag))) {
        LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_migration_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), K(*ctx_));
      } else if (OB_FAIL(scheduler->add_dag(tablet_migration_dag))) {
        LOG_WARN("failed to add tablet migration dag", K(ret), K(*tablet_migration_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule tablet migration dag", KPC(tablet_migration_dag));
        parent = tablet_migration_dag;
        tablet_migration_dag = nullptr;
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(tablet_migration_dag)) {
        // tablet_migration_dag_array is not empty.
        ObIDag *last = tablet_migration_dag_array.at(tablet_migration_dag_array.count() - 1);
        if (last == tablet_migration_dag) {
          tablet_migration_dag_array.pop_back();
          last = tablet_migration_dag_array.at(tablet_migration_dag_array.count() - 1);
        }

        scheduler->free_dag(*tablet_migration_dag, last);
        tablet_migration_dag = nullptr;
      }
    }

    // Cancel all dags from back to front, except the first dag which is 'sys_tablets_migration_dag'.
    if (OB_FAIL(ret)) {
      // The i-th dag is the parent dag of (i+1)-th dag.
      for (int64_t child_idx = tablet_migration_dag_array.count() - 1; child_idx > 0; child_idx--) {
        if (OB_TMP_FAIL(scheduler->cancel_dag(
              tablet_migration_dag_array.at(child_idx),
              tablet_migration_dag_array.at(child_idx - 1)))) {
          LOG_WARN("failed to cancel inner tablet migration dag", K(tmp_ret), K(child_idx));
        }
      }
      tablet_migration_dag_array.reset();
    }
  }
  return ret;
}

int ObSysTabletsMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "sys_tablets_migration_task",
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

/******************ObTabletMigrationDag*********************/
ObTabletMigrationDag::ObTabletMigrationDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_TABLET_MIGRATION),
    is_inited_(false),
    ls_handle_(),
    copy_tablet_ctx_(),
    tablet_group_ctx_(nullptr)
{
}

ObTabletMigrationDag::~ObTabletMigrationDag()
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = get_migration_ctx();
  if (OB_NOT_NULL(ctx)) {
    if (OB_FAIL(ctx->ha_table_info_mgr_.remove_tablet_table_info(copy_tablet_ctx_.tablet_id_))) {
      LOG_WARN("failed to remove tablet table info", K(ret), KPC(ctx), K(copy_tablet_ctx_));
    }
  }
}

bool ObTabletMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      } else {
        const ObTabletMigrationDag &tablet_migration_dag = static_cast<const ObTabletMigrationDag&>(other);
        if (tablet_migration_dag.copy_tablet_ctx_.tablet_id_ != copy_tablet_ctx_.tablet_id_) {
          is_same = false;
        }
      }
    }
  }
  return is_same;
}

int64_t ObTabletMigrationDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = get_migration_ctx();

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

int ObTabletMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletMigrationDag: log_stream_id = %s, tablet_id = %s, migration_type = %s",
       to_cstring(ctx->arg_.ls_id_), to_cstring(copy_tablet_ctx_.tablet_id_), ObMigrationOpType::get_str(ctx->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx), K(copy_tablet_ctx_));

  }
  return ret;
}

int ObTabletMigrationDag::init(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    ObIDagNet *dag_net,
    ObHATabletGroupCtx *tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet *migration_dag_net = nullptr;
  ObLS *ls = nullptr;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_EXIST;
  ObMigrationCtx *ctx = nullptr;
  bool is_exist = true;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet migration dag init twice", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet migration dag init get invalid argument", K(ret), K(tablet_id), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ctx = migration_dag_net->get_migration_ctx())) {
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx->arg_.ls_id_, ls_handle_))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), K(tablet_id));
  } else if (OB_FAIL(ctx->ha_table_info_mgr_.check_copy_tablet_exist(tablet_id, is_exist))) {
    LOG_WARN("failed to check copy tablet exist", K(ret), K(tablet_id));
  } else if (FALSE_IT(status = is_exist ? ObCopyTabletStatus::TABLET_EXIST : ObCopyTabletStatus::TABLET_NOT_EXIST)) {
  } else if (FALSE_IT(copy_tablet_ctx_.tablet_id_ = tablet_id)) {
  } else if (FALSE_IT(copy_tablet_ctx_.tablet_handle_ = tablet_handle)) {
  } else if (OB_FAIL(copy_tablet_ctx_.set_copy_tablet_status(status))) {
    LOG_WARN("failed to set copy tablet status", K(ret), K(status), K(tablet_id));
  } else if (FALSE_IT(ha_dag_net_ctx_ = ctx)) {
  } else {
    compat_mode_ = copy_tablet_ctx_.tablet_handle_.get_obj()->get_tablet_meta().compat_mode_;
    tablet_group_ctx_ = tablet_group_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(copy_tablet_ctx_))) {
    LOG_WARN("failed to init sys tablets migration task", K(ret), KPC(ha_dag_net_ctx_), K(copy_tablet_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletMigrationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("Tablet migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Tablet migration dag migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->arg_.ls_id_.id(),
                                static_cast<int64_t>(copy_tablet_ctx_.tablet_id_.id()),
                                static_cast<int64_t>(ctx->arg_.type_),
                                "dag_net_task_id", to_cstring(ctx->task_id_),
                                "src", to_cstring(ctx->arg_.src_.get_server()),
                                "dest", to_cstring(ctx->arg_.dst_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObTabletMigrationDag::get_ls(ObLS *&ls)
{
  int ret = OB_SUCCESS;
  ls = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration dag is not init", K(ret));
  } else {
    ls = ls_handle_.get_ls();
  }
  return ret;
}

int ObTabletMigrationDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  bool need_set_failed_result = true;
  ObTabletID tablet_id;
  ObDagId dag_id;
  const int64_t start_ts = ObTimeUtil::current_time();
  ObMigrationCtx *ctx = nullptr;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_ISNULL(tablet_group_ctx_)) {
    ret = OB_ITER_END;
    need_set_failed_result = false;
    LOG_INFO("tablet migration do not has next dag", KPC(this));
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
      if (OB_FAIL(tablet_group_ctx_->get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          //do nothing
          need_set_failed_result = false;
        } else {
          LOG_WARN("failed to get next tablet id", K(ret), KPC(this));
        }
      } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls migration dag net should not be NULL", K(ret), KP(dag_net));
      } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
      } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        }
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_migration_dag))) {
        LOG_WARN("failed to alloc tablet migration dag", K(ret));
      } else {
        if (OB_FAIL(tablet_migration_dag->init(tablet_id, tablet_handle, dag_net, tablet_group_ctx_))) {
          LOG_WARN("failed to init tablet migration migration dag", K(ret), K(tablet_id));
        } else if (FALSE_IT(dag_id.init(MYADDR))) {
        } else if (OB_FAIL(tablet_migration_dag->set_dag_id(dag_id))) {
          LOG_WARN("failed to set dag id", K(ret), K(tablet_id));
        } else {
          LOG_INFO("succeed generate next dag", K(tablet_id));
          dag = tablet_migration_dag;
          tablet_migration_dag = nullptr;
          break;
        }
      }
    }
  }

  if (OB_NOT_NULL(tablet_migration_dag)) {
    scheduler->free_dag(*tablet_migration_dag);
    tablet_migration_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }

  LOG_INFO("generate_next_dag", K(tablet_id), "cost", ObTimeUtil::current_time() - start_ts,
      "dag_id", dag_id);

  return ret;
}

int ObTabletMigrationDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
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
    SERVER_EVENT_ADD("storage_ha", "tablet_migration_retry",
        "tenant_id", ctx->tenant_id_,
        "ls_id", ctx->arg_.ls_id_.id(),
        "tablet_id", copy_tablet_ctx_.tablet_id_,
        "result", result, "retry_count", retry_count);
    if (OB_FAIL(ctx->ha_table_info_mgr_.remove_tablet_table_info(copy_tablet_ctx_.tablet_id_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to remove tablet info", K(ret), KPC(ctx), K(copy_tablet_ctx_));
      }
    }

    if (OB_SUCC(ret)) {
      copy_tablet_ctx_.tablet_handle_.reset();
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
    }

#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_TABLET_MIGRATION_DAG_INNER_RETRY) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_TABLET_MIGRATION_DAG_INNER_RETRY", K(ret));
      }
    }
#endif

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

/******************ObTabletMigrationTask*********************/
ObTabletMigrationTask::ObTabletMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
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

ObTabletMigrationTask::~ObTabletMigrationTask()
{
}

int ObTabletMigrationTask::init(ObCopyTabletCtx &copy_tablet_ctx)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet migration task init twice", K(ret));
  } else if (!copy_tablet_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet migration task init get invalid argument", K(ret), K(copy_tablet_ctx));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION !=  dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_migration_ctx();
    bandwidth_throttle_ = migration_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = migration_dag_net->get_storage_rpc();
    sql_proxy_ = migration_dag_net->get_sql_proxy();
    copy_tablet_ctx_ = &copy_tablet_ctx;
    is_inited_ = true;
    LOG_INFO("succeed init tablet migration task", "ls id", ctx_->arg_.ls_id_, "tablet_id", copy_tablet_ctx.tablet_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObTabletMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start do tablet migration task", KPC(copy_tablet_ctx_));
  const int64_t start_ts = ObTimeUtility::current_time();
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;

  if (OB_NOT_NULL(copy_tablet_ctx_)) {
    if (copy_tablet_ctx_->tablet_id_.is_inner_tablet() || copy_tablet_ctx_->tablet_id_.is_ls_inner_tablet()) {
    } else {
      DEBUG_SYNC(BEFORE_MIGRATION_TABLET_COPY_SSTABLE);
    }
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret), KPC(copy_tablet_ctx_));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(check_tablet_replica_validity_(copy_tablet_ctx_->tablet_id_))) {
    LOG_WARN("failed to check tablet replica validity", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(try_update_tablet_())) {
    LOG_WARN("failed to try update tablet", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(OB_FAIL(copy_tablet_ctx_->get_copy_tablet_status(status)))) {
    LOG_WARN("failed to get copy tablet status", K(ret), KPC(copy_tablet_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("copy tablet is not exist, skip copy it", KPC(copy_tablet_ctx_));
    if (OB_FAIL(update_ha_expected_status_(status))) {
      LOG_WARN("failed to update ha expected status", K(ret), KPC(copy_tablet_ctx_));
    }
  } else if (OB_FAIL(build_copy_table_key_info_())) {
    LOG_WARN("failed to build copy table key info", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(build_copy_sstable_info_mgr_())) {
    LOG_WARN("failed to build copy sstable info mgr", K(ret), KPC(copy_tablet_ctx_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_TABLET_MIGRATION_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_TABLET_MIGRATION_TASK_FAILED", K(ret));
      }
    }
#endif
    if (FAILEDx(generate_migration_tasks_())) {
      LOG_WARN("failed to generate migration tasks", K(ret), K(*copy_tablet_ctx_));
    }
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

// task running dag map
   // copy minor sstables task ->
   // copy major sstables task ->
   // copy ddl sstables task   ->
   // tablet copy finish task  ->

// task function introduce
   // copy minor sstables task is copy needed minor sstable from remote
   // copy major sstables task is copy needed major sstable from remote
   // copy ddl sstables task is copy needed ddl tables from remote
   // tablet copy finish task is using copyed sstables to create new table store
int ObTabletMigrationTask::generate_migration_tasks_()
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  ObTabletCopyFinishTask *tablet_copy_finish_task = nullptr;
  ObTabletFinishMigrationTask *tablet_finish_migration_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->arg_.dst_.get_replica_type())) {
    ret = OB_ERR_SYS;
    LOG_WARN("no need to generate minor task", K(ret), K(ctx_->arg_));
  } else if (OB_FAIL(generate_tablet_copy_finish_task_(tablet_copy_finish_task))) {
    LOG_WARN("failed to generate tablet copy finish task", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_minor_copy_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate migrate minor tasks", K(ret), K(*ctx_));
  } else if (OB_FAIL(generate_major_copy_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate migrate major tasks", K(ret), K(*ctx_));
  } else if (OB_FAIL(generate_ddl_copy_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate ddl copy tasks", K(ret), K(*ctx_));
  } else if (OB_FAIL(generate_tablet_finish_migration_task_(tablet_finish_migration_task))) {
    LOG_WARN("failed to generate tablet finish migration task", K(ret), K(ctx_->arg_));
  } else if (OB_FAIL(tablet_copy_finish_task->add_child(*tablet_finish_migration_task))) {
    LOG_WARN("failed to add child finsih task for parent", K(ret), KPC(tablet_finish_migration_task));
  } else if (OB_FAIL(parent_task->add_child(*tablet_copy_finish_task))) {
    LOG_WARN("failed to add tablet copy finish task as child", K(ret), KPC(ctx_));
  } else if (OB_FAIL(dag_->add_task(*tablet_copy_finish_task))) {
    LOG_WARN("failed to add tablet copy finish task", K(ret), KPC(ctx_));
  } else if (OB_FAIL(dag_->add_task(*tablet_finish_migration_task))) {
    LOG_WARN("failed to add tablet finish migration task", K(ret));
  } else {
    LOG_INFO("generate sstable migration tasks", KPC(copy_tablet_ctx_), K(copy_table_key_array_));
  }
  return ret;
}

int ObTabletMigrationTask::generate_tablet_finish_migration_task_(
    ObTabletFinishMigrationTask *&tablet_finish_migration_task)
{
  int ret = OB_SUCCESS;
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  ObLS *ls = nullptr;
  if (OB_NOT_NULL(tablet_finish_migration_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet finish migration task must not be null", K(ret), KPC(tablet_finish_migration_task));
  } else if (FALSE_IT(tablet_migration_dag = static_cast<ObTabletMigrationDag *>(dag_))) {
  } else if (OB_FAIL(dag_->alloc_task(tablet_finish_migration_task))) {
    LOG_WARN("failed to alloc tablet finish task", K(ret), KPC(ctx_));
  } else if (OB_FAIL(tablet_migration_dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(tablet_finish_migration_task->init(*copy_tablet_ctx_, *ls))) {
    LOG_WARN("failed to init tablet copy finish task", K(ret), KPC(ctx_), KPC(copy_tablet_ctx_));
  } else {
    LOG_INFO("generate tablet migration finish task", "ls_id", ls->get_ls_id().id(), "tablet_id", copy_tablet_ctx_->tablet_id_);
  }
  return ret;
}

int ObTabletMigrationTask::generate_minor_copy_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (OB_FAIL(generate_copy_tasks_(ObITable::is_minor_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate copy minor tasks", K(ret), KPC(copy_tablet_ctx_));
  }
  return ret;
}

int ObTabletMigrationTask::generate_major_copy_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (OB_FAIL(generate_copy_tasks_(ObITable::is_major_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate copy major tasks", K(ret), KPC(copy_tablet_ctx_));
  }
  return ret;
}

int ObTabletMigrationTask::generate_ddl_copy_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (OB_FAIL(generate_copy_tasks_(ObITable::is_ddl_dump_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate copy ddl tasks", K(ret), KPC(copy_tablet_ctx_));
  }
  return ret;
}

int ObTabletMigrationTask::generate_physical_copy_task_(
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
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  bool is_tablet_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (!src_info.is_valid() || !copy_table_key.is_valid() || OB_ISNULL(tablet_copy_finish_task)
      || OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate physical copy task get invalid argument", K(ret),
        K(src_info), KP(parent_task), KP(child_task));
  } else if (FALSE_IT(tablet_migration_dag = static_cast<ObTabletMigrationDag *>(dag_))) {
  } else if (OB_FAIL(tablet_migration_dag->get_ls(ls))) {
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
    if (FALSE_IT(init_param.tenant_id_ = ctx_->tenant_id_)) {
    } else if (FALSE_IT(init_param.ls_id_ = ctx_->arg_.ls_id_)) {
    } else if (FALSE_IT(init_param.tablet_id_ = copy_tablet_ctx_->tablet_id_)) {
    } else if (FALSE_IT(init_param.src_info_ = src_info)) {
    } else if (FALSE_IT(init_param.tablet_copy_finish_task_ = tablet_copy_finish_task)) {
    } else if (FALSE_IT(init_param.ls_ = ls)) {
    } else if (FALSE_IT(init_param.need_check_seq_ = true)) {
    } else if (FALSE_IT(init_param.ls_rebuild_seq_ = ctx_->local_rebuild_seq_)) {
    } else if (OB_FAIL(ctx_->ha_table_info_mgr_.get_table_info(copy_tablet_ctx_->tablet_id_, copy_table_key, init_param.sstable_param_))) {
      LOG_WARN("failed to get table info", K(ret), KPC(copy_tablet_ctx_), K(copy_table_key));
    } else if (OB_FAIL(copy_sstable_info_mgr_.get_copy_sstable_maro_range_info(copy_table_key, init_param.sstable_macro_range_info_))) {
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

int ObTabletMigrationTask::build_copy_table_key_info_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.get_table_keys(copy_tablet_ctx_->tablet_id_, copy_table_key_array_))) {
    LOG_WARN("failed to get copy table keys", K(ret), KPC(copy_tablet_ctx_));
  }
  return ret;
}

int ObTabletMigrationTask::build_copy_sstable_info_mgr_()
{
  int ret = OB_SUCCESS;
  ObStorageHACopySSTableParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_FAIL(param.copy_table_key_array_.assign(copy_table_key_array_))) {
    LOG_WARN("failed to assign copy table key info array", K(ret), K(copy_table_key_array_));
  } else {
    param.tenant_id_ = ctx_->tenant_id_;
    param.ls_id_ = ctx_->arg_.ls_id_;
    param.tablet_id_ = copy_tablet_ctx_->tablet_id_;
    param.is_leader_restore_ = false;
    param.local_rebuild_seq_ = ctx_->local_rebuild_seq_;
    param.meta_index_store_ = nullptr;
    param.second_meta_index_store_ = nullptr;
    param.need_check_seq_ = true;
    param.restore_base_info_ = nullptr;
    param.src_info_ = ctx_->minor_src_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;
    param.bandwidth_throttle_ = bandwidth_throttle_;

    if (OB_FAIL(copy_sstable_info_mgr_.init(param))) {
      LOG_WARN("failed to init copy sstable info mgr", K(ret), K(param), KPC(ctx_), KPC(copy_tablet_ctx_));
    }
  }
  return ret;
}

int ObTabletMigrationTask::generate_copy_tasks_(
    IsRightTypeSSTableFunc is_right_type_sstable,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate copy task get invalid argument", K(ret), KP(parent_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < copy_table_key_array_.count(); ++i) {
      const ObITable::TableKey &copy_table_key = copy_table_key_array_.at(i);
      ObFakeTask *wait_finish_task = nullptr;
      bool need_copy = true;

      if (!copy_table_key.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy table key info is invalid", K(ret), K(copy_table_key));
      } else if (!is_right_type_sstable(copy_table_key.table_type_)) {
        //do nothing
      } else {
        if (OB_FAIL(check_need_copy_sstable_(copy_table_key, need_copy))) {
          LOG_WARN("failed to check need copy sstable", K(ret), K(copy_table_key));
        } else if (!need_copy) {
          LOG_INFO("local contains the sstable, no need copy", K(copy_table_key));
        } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(generate_physical_copy_task_(ctx_->minor_src_,
            copy_table_key, tablet_copy_finish_task, parent_task, wait_finish_task))) {
          LOG_WARN("failed to generate_physical copy task", K(ret), K(*ctx_), K(copy_table_key));
        } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task", K(ret));
        } else {
          parent_task = wait_finish_task;
          LOG_INFO("succeed to generate_sstable_copy_task", "minor src", ctx_->minor_src_, K(copy_table_key));
        }
      }
    }
  }
  return ret;
}

int ObTabletMigrationTask::generate_tablet_copy_finish_task_(
    ObTabletCopyFinishTask *&tablet_copy_finish_task)
{
  int ret = OB_SUCCESS;
  tablet_copy_finish_task = nullptr;
  ObLS *ls = nullptr;
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  observer::ObIMetaReport *reporter = GCTX.ob_service_;
  const ObTabletRestoreAction::ACTION restore_action = ObTabletRestoreAction::RESTORE_NONE;
  const ObMigrationTabletParam *src_tablet_meta = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (FALSE_IT(tablet_migration_dag = static_cast<ObTabletMigrationDag *>(dag_))) {
  } else if (OB_FAIL(dag_->alloc_task(tablet_copy_finish_task))) {
    LOG_WARN("failed to alloc tablet copy finish task", K(ret), KPC(ctx_));
  } else if (OB_FAIL(tablet_migration_dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.get_tablet_meta(copy_tablet_ctx_->tablet_id_, src_tablet_meta))) {
    LOG_WARN("failed to get src tablet meta", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(tablet_copy_finish_task->init(
      copy_tablet_ctx_->tablet_id_, ls, reporter, restore_action, src_tablet_meta, copy_tablet_ctx_))) {
    LOG_WARN("failed to init tablet copy finish task", K(ret), KPC(ctx_), KPC(copy_tablet_ctx_));
  } else {
    LOG_INFO("generate tablet copy finish task", "ls_id", ls->get_ls_id().id(), "tablet_id", copy_tablet_ctx_->tablet_id_);
  }
  return ret;
}

int ObTabletMigrationTask::record_server_event_(const int64_t cost_us, const int64_t result)
{
  int ret = OB_SUCCESS;
  const ObMigrationTabletParam *src_tablet_meta = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  if (OB_ISNULL(ctx_) || OB_ISNULL(copy_tablet_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx), KPC_(copy_tablet_ctx));
  } else if (FALSE_IT(tablet_migration_dag = static_cast<ObTabletMigrationDag *>(dag_))) {
  } else if (OB_FAIL(tablet_migration_dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->ha_get_tablet(copy_tablet_ctx_->tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tx data", K(ret), KPC(tablet));
  } else {
    const char *tablet_status = ObTabletStatus::get_str(user_data.tablet_status_);
    SERVER_EVENT_ADD("storage_ha", "tablet_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "tablet_id", copy_tablet_ctx_->tablet_id_.id(),
        "tablet_status", tablet_status);
  }
  return ret;
}

int ObTabletMigrationTask::try_update_tablet_()
{
  int ret = OB_SUCCESS;
  ObTabletMigrationDag *dag = nullptr;
  int32_t retry_count = 0;
  ObStorageHATabletsBuilder ha_tablets_builder;
  ObSEArray<ObTabletID, 1> tablet_id_array;
  ObLS *ls = nullptr;
  bool is_exist = false;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObTabletMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet migration dag should not be NULL", K(ret), KP(dag));
  } else if (OB_FAIL(copy_tablet_ctx_->get_copy_tablet_status(status))) {
    LOG_WARN("failed to get copy tablet status", K(ret), KPC(dag), KPC(copy_tablet_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    //do nothing
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.check_tablet_table_info_exist(copy_tablet_ctx_->tablet_id_, is_exist))) {
    LOG_WARN("failed to check tablet table info exist", K(ret), KPC(copy_tablet_ctx_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(tablet_id_array.push_back(copy_tablet_ctx_->tablet_id_))) {
    LOG_WARN("failed to push tablet id into array", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(copy_tablet_ctx_));
  } else if (OB_FAIL(ObLSMigrationUtils::init_ha_tablets_builder(
      ctx_->tenant_id_, tablet_id_array, ctx_->minor_src_, ctx_->local_rebuild_seq_, ctx_->arg_.type_,
      ls, &ctx_->ha_table_info_mgr_, ha_tablets_builder))) {
    LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
  } else {
    //Here inner tablet copy data before clog replay, and now just create a new tablet to replace it.
    //Data tablet copy data during clog replay, so the data tablet can only be updated.
    if (copy_tablet_ctx_->tablet_id_.is_ls_inner_tablet()) {
      if (OB_FAIL(ha_tablets_builder.create_or_update_tablets())) {
        LOG_WARN("failed to create or update tablets", K(ret), KPC(ctx_));
      }
    } else {
      if (OB_FAIL(ha_tablets_builder.update_local_tablets())) {
        LOG_WARN("failed to create or update tablets", K(ret), KPC(ctx_), KPC(copy_tablet_ctx_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ha_tablets_builder.build_tablets_sstable_info())) {
      LOG_WARN("failed to build tablets sstable info", K(ret), KPC(ctx_), KPC(copy_tablet_ctx_));
    } else if (OB_FAIL(ctx_->ha_table_info_mgr_.check_tablet_table_info_exist(copy_tablet_ctx_->tablet_id_, is_exist))) {
      LOG_WARN("failed to check tablet table info exist", K(ret), KPC(copy_tablet_ctx_));
    } else if (!is_exist) {
      status = ObCopyTabletStatus::TABLET_NOT_EXIST;
      if (OB_FAIL(copy_tablet_ctx_->set_copy_tablet_status(status))) {
        LOG_WARN("failed to set copy tablet status", K(ret), KPC(dag));
      }
    }
  }
  return ret;
}

int ObTabletMigrationTask::update_ha_expected_status_(
    const ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  ObTabletMigrationDag *dag = nullptr;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (status != ObCopyTabletStatus::TABLET_NOT_EXIST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update ha meta status get invalid argument", K(ret), K(status));
  } else if (OB_ISNULL(dag = static_cast<ObTabletMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet migration dag should not be NULL", K(ret), KP(dag));
  } else if (OB_FAIL(dag->get_ls(ls))) {
    LOG_WARN("failed to get ls", K(ret), KPC(copy_tablet_ctx_));
  } else {
    const ObTabletExpectedStatus::STATUS expected_status = ObTabletExpectedStatus::DELETED;
    if (OB_FAIL(ls->get_tablet_svr()->update_tablet_ha_expected_status(copy_tablet_ctx_->tablet_id_, expected_status))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("migration tablet maybe deleted, skip it", K(ret), KPC(copy_tablet_ctx_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to update tablet ha expected status", K(ret), K(expected_status), KPC(copy_tablet_ctx_));
      }
    }
  }
  return ret;
}

int ObTabletMigrationTask::check_need_copy_sstable_(
    const ObITable::TableKey &table_key,
    bool &need_copy)
{
  int ret = OB_SUCCESS;
  need_copy = true;
  const blocksstable::ObMigrationSSTableParam *copy_table_info = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need copy sstable get invlaid argument", K(ret), K(table_key));
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.get_table_info(copy_tablet_ctx_->tablet_id_, table_key, copy_table_info))) {
    LOG_WARN("failed to get table info", K(ret), KPC(copy_tablet_ctx_), K(table_key));
  } else if (OB_FAIL(ObStorageHATaskUtils::check_need_copy_sstable(*copy_table_info, copy_tablet_ctx_->tablet_handle_, need_copy))) {
    LOG_WARN("failed to check need copy sstable", K(ret), KPC(copy_tablet_ctx_), K(table_key));
  }
  return ret;
}

int ObTabletMigrationTask::check_tablet_replica_validity_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (OB_ISNULL(sql_proxy_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy should not be null", K(ret));
  } else {
    const uint64_t tenant_id = ctx_->tenant_id_;
    const share::ObLSID &ls_id = ctx_->arg_.ls_id_;
    const common::ObAddr &src_addr = ctx_->major_src_.src_addr_;
    if (OB_FAIL(ObStorageHAUtils::check_tablet_replica_validity(tenant_id, ls_id, src_addr, tablet_id, *sql_proxy_))) {
      LOG_WARN("failed to check tablet replica validity", K(ret), K(tenant_id), K(ls_id), K(src_addr), K(tablet_id));
    } else {
      ctx_->check_tablet_info_cost_time_ += ObTimeUtility::current_time() - start_ts;
      LOG_DEBUG("check tablet replica validity", KPC(ctx_), K(tablet_id));
    }
  }
  return ret;
}

/******************ObTabletFinishMigrationTask*********************/
ObTabletFinishMigrationTask::ObTabletFinishMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    copy_tablet_ctx_(nullptr),
    ls_(nullptr)
{
}

ObTabletFinishMigrationTask::~ObTabletFinishMigrationTask()
{
}

int ObTabletFinishMigrationTask::init(ObCopyTabletCtx &ctx, ObLS &ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet migration finish task init twice", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctx));
  } else {
    ha_dag_net_ctx_ = static_cast<ObStorageHADag *>(this->get_dag())->get_ha_dag_net_ctx();
    copy_tablet_ctx_ = &ctx;
    ls_ = &ls;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletFinishMigrationTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do tablet finish migration task", KPC(copy_tablet_ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish migration task do not init", K(ret), KPC(copy_tablet_ctx_));
  } else {
    bool is_failed = false;
    if (ha_dag_net_ctx_->is_failed()) {
      is_failed = true;
    } else if (OB_FAIL(update_data_and_expected_status_())) {
      LOG_WARN("failed to update data and expected status", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(copy_tablet_ctx_));
    }
  }
  return ret;
}

int ObTabletFinishMigrationTask::update_data_and_expected_status_()
{
  int ret = OB_SUCCESS;
  ObCopyTabletStatus::STATUS status;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(copy_tablet_ctx_->get_copy_tablet_status(status))) {
    LOG_WARN("failed to get copy tablet status", KPC(copy_tablet_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    const ObTabletExpectedStatus::STATUS expected_status = ObTabletExpectedStatus::DELETED;
    if (OB_FAIL(ls_->get_tablet_svr()->update_tablet_ha_expected_status(copy_tablet_ctx_->tablet_id_, expected_status))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("migration tablet maybe deleted, skip it", K(ret), KPC(copy_tablet_ctx_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to update tablet ha expected status", K(ret), K(expected_status), KPC(copy_tablet_ctx_));
      }
    } else {
      LOG_INFO("update tablet expected status", KPC(copy_tablet_ctx_), K(expected_status));
      SERVER_EVENT_ADD("storage_ha", "tablet_finish_migration_task",
          "tenant_id", MTL_ID(),
          "ls_id", ls_->get_ls_id().id(),
          "tablet_id", copy_tablet_ctx_->tablet_id_,
          "expected_status", expected_status);
      }
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_UPDATE_TABLET_HA_STATUS_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_UPDATE_TABLET_HA_STATUS_FAILED", K(ret));
      }
    }
#endif
    const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
    if (OB_FAIL(ls_->update_tablet_ha_data_status(copy_tablet_ctx_->tablet_id_, data_status))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("migration tablet maybe deleted, skip it", K(ret), KPC(copy_tablet_ctx_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[HA]failed to update tablet ha data status", K(ret), KPC(copy_tablet_ctx_), K(data_status));
      }
    } else {
      LOG_INFO("update tablet ha data status", KPC(copy_tablet_ctx_), K(data_status));
      SERVER_EVENT_ADD("storage_ha", "tablet_finish_migration_task",
          "tenant_id", MTL_ID(),
          "ls_id", ls_->get_ls_id().id(),
          "tablet_id", copy_tablet_ctx_->tablet_id_,
          "data_status", data_status);
    }
  }
  return ret;
}

/******************ObDataTabletsMigrationDag*********************/
ObDataTabletsMigrationDag::ObDataTabletsMigrationDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_DATA_TABLETS_MIGRATION),
    is_inited_(false)
{
}

ObDataTabletsMigrationDag::~ObDataTabletsMigrationDag()
{
}

bool ObDataTabletsMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObDataTabletsMigrationDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = nullptr;

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObDataTabletsMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObDataTabletsMigrationDag: log_stream_id = %s, migration_type = %s",
       to_cstring(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObDataTabletsMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data tablets migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDataTabletsMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObDataTabletsMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init sys tablets migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObDataTabletsMigrationTask*********************/
ObDataTabletsMigrationTask::ObDataTabletsMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ls_handle_(),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    finish_dag_(nullptr),
    ha_tablets_builder_()
{
}

ObDataTabletsMigrationTask::~ObDataTabletsMigrationTask()
{
}

int ObDataTabletsMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data tablets migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    const common::ObIArray<ObINodeWithChild*> &child_node_array = this->get_dag()->get_child_nodes();
    if (child_node_array.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data tablets migration dag get unexpected child node", K(ret), K(child_node_array));
    } else {
      ObMigrationDag *child_dag = static_cast<ObMigrationDag*>(child_node_array.at(0));
      if (ObDagType::DAG_TYPE_MIGRATION_FINISH != child_dag->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("migrtion dag type is unexpected", K(ret), K(*child_dag));
      } else {
        ctx_ = migration_dag_net->get_migration_ctx();
        bandwidth_throttle_ = migration_dag_net->get_bandwidth_throttle();
        svr_rpc_proxy_ = migration_dag_net->get_storage_rpc_proxy();
        storage_rpc_ = migration_dag_net->get_storage_rpc();
        finish_dag_ = static_cast<ObIDag*>(child_dag);

        if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle_))) {
          LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
        } else if (OB_FAIL(ObLSMigrationUtils::init_ha_tablets_builder(
            ctx_->tenant_id_, ctx_->data_tablet_id_array_, ctx_->minor_src_, ctx_->local_rebuild_seq_,
            ctx_->arg_.type_, ls_handle_.get_ls(), &ctx_->ha_table_info_mgr_, ha_tablets_builder_))) {
          LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
        } else {
          is_inited_ = true;
          LOG_INFO("succeed init data tablets migration task", "ls id", ctx_->arg_.ls_id_,
              "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
        }
      }
    }
  }
  return ret;
}

int ObDataTabletsMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start do data tablets migration task", K(ret), KPC(ctx_));
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("storage_ha", "before_data_tablets_migration_task",
                        "tenant_id", ctx_->tenant_id_,
                        "ls_id", ctx_->arg_.ls_id_.id());
#endif
  DEBUG_SYNC(BEFORE_DATA_TABLETS_MIGRATION_TASK);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(try_remove_unneeded_tablets_())) {
    LOG_WARN("failed to try remove unneeded tablets", K(ret), KPC(ctx_));
  } else if (OB_FAIL(join_learner_list_())) {
    LOG_WARN("failed to add to learner list", K(ret));
  } else if (OB_FAIL(ls_online_())) {
    LOG_WARN("failed to start replay log", K(ret), K(*ctx_));
  } else if (OB_FAIL(build_tablet_group_info_())) {
    LOG_WARN("failed to build tablet group info", K(ret), KPC(ctx_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      int32_t retry_count = 0;
      if (OB_SUCCESS != (tmp_ret = ctx_->get_retry_count(retry_count))) {
        LOG_WARN("failed to get retry count", K(tmp_ret), K(retry_count));
      } else if (retry_count > 0) {
        ret = OB_SUCCESS;
      } else {
        ret = OB_E(EventTable::EN_DATA_TABLETS_MIGRATION_TASK_FAILED) OB_SUCCESS;
        if (OB_FAIL(ret)) {
          STORAGE_LOG(ERROR, "fake EN_DATA_TABLETS_MIGRATION_TASK_FAILED", K(ret));
        }
      }
    }
#endif

    if (FAILEDx(generate_tablet_group_dag_())) {
      LOG_WARN("failed to generate tablet group dag", K(ret), KPC(ctx_));
    }
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    bool allow_retry = true;
    if (OB_SUCCESS != (tmp_ret = try_offline_ls_())) {
      LOG_WARN("failed to try offline ls", K(tmp_ret));
    } else if (FALSE_IT(allow_retry = OB_SUCCESS == tmp_ret)) {
    }

    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag(), allow_retry))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(*ctx_));
    }
  }

  DEBUG_SYNC(AFTER_DATA_TABLETS_MIGRATION);
  return ret;
}

int ObDataTabletsMigrationTask::join_learner_list_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const int64_t timeout = GCONF.sys_bkgd_migration_change_member_list_timeout;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration task do not init", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_2_0_0) {
    // do nothing
  } else if (ObMigrationOpType::ADD_LS_OP != ctx_->arg_.type_
      && ObMigrationOpType::MIGRATE_LS_OP != ctx_->arg_.type_) {
    // only join learner list when migration and copy
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else {
    ctx_->arg_.dst_.set_migrating();
    const ObMember &dst_member = ctx_->arg_.dst_;
    if (OB_FAIL(ls->add_learner(dst_member, timeout))) {
      LOG_WARN("failed to add learner", K(ret), K(dst_member));
    } else {
      LOG_INFO("add to learner list succ", KPC(ctx_));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_JOIN_LEARNER_LIST_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_JOIN_LEARNER_LIST_FAILED", K(ret));
      SERVER_EVENT_SYNC_ADD("storage_ha", "join_learner_list_failed",
          "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(),
          "src", ctx_->arg_.src_.get_server(),
          "dst", ctx_->arg_.dst_.get_server());
    }
  }
#endif
  SERVER_EVENT_SYNC_ADD("storage_ha", "after_join_learner_list",
          "tenant_id", ctx_->tenant_id_,
          "ls_id", ctx_->arg_.ls_id_.id(),
          "src", ctx_->arg_.src_.get_server(),
          "dst", ctx_->arg_.dst_.get_server());
  DEBUG_SYNC(AFTER_JOIN_LEARNER_LIST);
  return ret;
}

int ObDataTabletsMigrationTask::ls_online_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("storage_ha", "before_ls_online");
  FLOG_INFO("errsim point before ls online");
  DEBUG_SYNC(BEFORE_MIGRATION_ENABLE_LOG);
#endif
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->online())) {
    LOG_WARN("failed to online ls", K(ret), KPC(ctx_));
  } else {
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_MIGRATION_ENABLE_LOG_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_MIGRATION_ENABLE_LOG_FAILED", K(ret));
    }
  }
#endif
    FLOG_INFO("succeed online ls", K(ret), KPC(ctx_));
  }
#ifdef ERRSIM
  SERVER_EVENT_SYNC_ADD("storage_ha", "after_ls_online");
  FLOG_INFO("errsim point after ls online");
  DEBUG_SYNC(AFTER_MIGRATION_ENABLE_LOG);
#endif


#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_MIGRATION_ONLINE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_MIGRATION_ONLINE_FAILED", K(ret));
    }
  }
#endif

  return ret;
}

int ObDataTabletsMigrationTask::build_tablet_group_info_()
{
  int ret = OB_SUCCESS;
  ObCopyTabletSimpleInfo tablet_simple_info;
  ObArray<ObTabletID> tablet_group_id_array;
  ObArray<ObTabletID> tablet_id_array;
  hash::ObHashSet<ObTabletID> remove_tablet_set;

  DEBUG_SYNC(BEFORE_BUILD_TABLET_GROUP_INFO);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration task do not init", K(ret));
  } else {
    ctx_->tablet_group_mgr_.reuse();
    const hash::ObHashMap<common::ObTabletID, ObCopyTabletSimpleInfo> &tablet_simple_info_map =
        ctx_->tablet_simple_info_map_;

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->data_tablet_id_array_.count(); ++i) {
      tablet_simple_info.reset();
      const ObTabletID &tablet_id = ctx_->data_tablet_id_array_.at(i);
      tablet_group_id_array.reset();

      if (OB_FAIL(tablet_simple_info_map.get_refactored(tablet_id, tablet_simple_info))) {
        if (OB_HASH_NOT_EXIST == ret) {
          FLOG_INFO("tablet do not exist in src ls, skip it", K(tablet_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet simple info", K(ret), K(tablet_id));
        }
      } else if (!tablet_simple_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet simple info is not valid", K(ret), K(tablet_simple_info));
      } else if (tablet_simple_info.data_size_ >= MAX_TABLET_GROUP_SIZE
          && ObCopyTabletStatus::TABLET_EXIST == tablet_simple_info.status_) {
        if (OB_FAIL(tablet_group_id_array.push_back(tablet_id))) {
          LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
        } else if (OB_FAIL(ctx_->tablet_group_mgr_.build_tablet_group_ctx(tablet_group_id_array))) {
          LOG_WARN("failed to build tablet group ctx", K(ret), KPC(ctx_));
        } else {
          LOG_INFO("succeed build tablet group ctx", K(tablet_group_id_array));
        }
      } else if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
        LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
      }
    }

    if (OB_SUCC(ret)) {
      if (tablet_id_array.empty()) {
        //do nothing
      } else if (OB_FAIL(remove_tablet_set.create(tablet_id_array.count()))) {
        LOG_WARN("failed to create remove tablet set", K(ret), K(tablet_id_array));
      } else {
        LOG_INFO("need tablet group list", K(tablet_id_array));
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
          tablet_simple_info.reset();
          const ObTabletID &tablet_id = tablet_id_array.at(i);
          tablet_group_id_array.reset();
          int64_t total_size = 0;
          int hash_ret = OB_SUCCESS;

          if (OB_FAIL(tablet_simple_info_map.get_refactored(tablet_id, tablet_simple_info))) {
            LOG_WARN("failed to get tablet simple info", K(ret), K(tablet_id));
          } else if (FALSE_IT(hash_ret = remove_tablet_set.exist_refactored(tablet_id))) {
          } else if (OB_HASH_EXIST == hash_ret) {
            //do nothing
          } else if (hash_ret != OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
            LOG_WARN("failed to check remove tablet exist", K(ret), K(tablet_id));
          } else if (OB_FAIL(tablet_group_id_array.push_back(tablet_id))) {
            LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
          } else {
            total_size = tablet_simple_info.data_size_;
            int64_t max_tablet_count = 0;
            for (int64_t j = i + 1; OB_SUCC(ret) && j < tablet_id_array.count() && max_tablet_count < MAX_TABLET_COUNT; ++j) {
              const ObTabletID &tmp_tablet_id = tablet_id_array.at(j);
              ObCopyTabletSimpleInfo tmp_tablet_simple_info;

              if (FALSE_IT(hash_ret = remove_tablet_set.exist_refactored(tmp_tablet_id))) {
              } else if (OB_HASH_EXIST == hash_ret) {
                //do nothing
              } else if (hash_ret != OB_HASH_NOT_EXIST) {
                ret = OB_SUCCESS == hash_ret ? OB_ERR_UNEXPECTED : hash_ret;
                LOG_WARN("failed to check remove tablet exist", K(ret), K(tablet_id));
              } else if (OB_FAIL(tablet_simple_info_map.get_refactored(tmp_tablet_id, tmp_tablet_simple_info))) {
                LOG_WARN("failed to get tablet simple info", K(ret), K(tmp_tablet_id));
              } else if (total_size + tmp_tablet_simple_info.data_size_ <= MAX_TABLET_GROUP_SIZE) {
                if (OB_FAIL(tablet_group_id_array.push_back(tmp_tablet_id))) {
                  LOG_WARN("failed to set tablet id into array", K(ret), K(tmp_tablet_id));
                } else if (OB_FAIL(remove_tablet_set.set_refactored(tmp_tablet_id))) {
                  LOG_WARN("failed to set tablet into set", K(ret), K(tmp_tablet_id));
                } else {
                  total_size += tmp_tablet_simple_info.data_size_;
                  max_tablet_count++;
                }
              }
            }

            if (OB_SUCC(ret)) {
              if (OB_FAIL(ctx_->tablet_group_mgr_.build_tablet_group_ctx(tablet_group_id_array))) {
                LOG_WARN("failed to build tablet group ctx", K(ret), K(tablet_group_id_array), KPC(ctx_));
              } else {
                LOG_INFO("succeed build tablet group ctx", K(tablet_group_id_array), "count", tablet_group_id_array.count());
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDataTabletsMigrationTask::generate_tablet_group_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletGroupMigrationDag *tablet_group_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObDataTabletsMigrationDag *data_tablets_migration_dag = nullptr;
  ObHATabletGroupCtx *tablet_group_ctx = nullptr;
  ObArray<ObTabletID> tablet_id_array;

  DEBUG_SYNC(BEFORE_TABLET_GROUP_MIGRATION_GENERATE_NEXT_DAG);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets migration task do not init", K(ret));
  } else if (OB_FAIL(ctx_->tablet_group_mgr_.get_next_tablet_group_ctx(tablet_group_ctx))) {
    if (OB_ITER_END == ret) {
     ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next tablet group ctx", K(ret), KPC(ctx_));
    }
  } else if (OB_FAIL(tablet_group_ctx->get_all_tablet_ids(tablet_id_array))) {
    LOG_WARN("failed to get all tablet ids", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(data_tablets_migration_dag = static_cast<ObDataTabletsMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data tablets migration dag should not be NULL", K(ret), KP(data_tablets_migration_dag));
  } else if (OB_ISNULL(dag_net = data_tablets_migration_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), K(*this));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(tablet_group_dag))) {
      LOG_WARN("failed to alloc tablet group migration dag ", K(ret));
    } else if (OB_FAIL(tablet_group_dag->init(tablet_id_array, dag_net, finish_dag_, tablet_group_ctx))) {
      LOG_WARN("failed to init tablet group dag", K(ret), K(tablet_id_array));
    } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_group_dag))) {
      LOG_WARN("failed to add dag into dag net", K(ret), KPC(tablet_group_dag));
    } else if (OB_FAIL(this->get_dag()->add_child_without_inheritance(*tablet_group_dag))) {
      LOG_WARN("failed to add tablet group dag as child", K(ret), K(*tablet_group_dag));
    } else if (OB_FAIL(tablet_group_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), K(*ctx_));
    } else if (OB_FAIL(tablet_group_dag->add_child_without_inheritance(*finish_dag_))) {
      LOG_WARN("failed to add finish dag as child", K(ret), K(*tablet_group_dag), K(*finish_dag_));
    } else if (OB_FAIL(scheduler->add_dag(tablet_group_dag))) {
      LOG_WARN("failed to add sys tablets migration dag", K(ret), K(*tablet_group_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(tablet_group_dag)) {
        scheduler->free_dag(*tablet_group_dag, data_tablets_migration_dag);
        tablet_group_dag = nullptr;
      }
    }
  }
  return ret;
}

int ObDataTabletsMigrationTask::try_remove_unneeded_tablets_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  hash::ObHashSet<ObTabletID> tablet_id_set;
  int64_t bucket_num = 0;
  ObArray<ObTabletID> tablet_id_array;
  const int64_t MAX_BUCKET_NUM = 1024;
  const bool need_initial_state = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx_));
  } else if (FALSE_IT(bucket_num = std::max(MAX_BUCKET_NUM,
      ctx_->sys_tablet_id_array_.count() + ctx_->data_tablet_id_array_.count()))) {
  } else if (OB_FAIL(tablet_id_set.create(bucket_num))) {
    LOG_WARN("failed to create tablet id set", K(ret), KPC(ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->sys_tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->sys_tablet_id_array_.at(i);
      if (OB_FAIL(tablet_id_set.set_refactored(tablet_id))) {
        LOG_WARN("failed to set tablet into set", K(ret), K(tablet_id), KPC(ctx_));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->data_tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->data_tablet_id_array_.at(i);
      if (OB_FAIL(tablet_id_set.set_refactored(tablet_id))) {
        LOG_WARN("failed to set tablet into set", K(ret), K(tablet_id), KPC(ctx_));
      }
    }

    if (OB_SUCC(ret)) {
      ObHALSTabletIDIterator iter(ls->get_ls_id(), need_initial_state);
      ObTabletID tablet_id;
      if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(iter))) {
        LOG_WARN("failed to build tablet iter", K(ret), KPC(ctx_));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter.get_next_tablet_id(tablet_id))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get tablet id", K(ret));
            }
          } else {
            int32_t hash_ret = tablet_id_set.exist_refactored(tablet_id);
            if (OB_HASH_EXIST == hash_ret) {
              //do nothing
            } else if (OB_HASH_NOT_EXIST == hash_ret) {
              if (OB_FAIL(tablet_id_array.push_back(tablet_id))) {
                LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
              }
            } else {
              ret = hash_ret == OB_SUCCESS ? OB_ERR_UNEXPECTED : hash_ret;
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (tablet_id_array.empty()) {
          //do nothing
        } else if (OB_FAIL(ls->remove_tablets(tablet_id_array))) {
          LOG_WARN("failed to remove tablets", K(ret), KPC(ls), K(tablet_id_array));
        } else {
          FLOG_INFO("succeed remove tablets", K(tablet_id_array));
        }
      }
    }
  }
  return ret;
}

int ObDataTabletsMigrationTask::try_offline_ls_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->offline())) {
    LOG_WARN("failed to offline ls", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObDataTabletsMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "data_tablets_migration_task",
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

/******************ObTabletGroupMigrationDag*********************/
ObTabletGroupMigrationDag::ObTabletGroupMigrationDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_TABLET_GROUP_MIGRATION),
    is_inited_(false),
    tablet_id_array_(),
    finish_dag_(nullptr),
    tablet_group_ctx_(nullptr)
{
}

ObTabletGroupMigrationDag::~ObTabletGroupMigrationDag()
{
}

bool ObTabletGroupMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      } else {
        const ObTabletGroupMigrationDag &tablet_group_dag = static_cast<const ObTabletGroupMigrationDag&>(other);
        if (tablet_id_array_.count() != tablet_group_dag.tablet_id_array_.count()) {
          is_same = false;
        } else {
          for (int64_t i = 0; is_same && i < tablet_id_array_.count(); ++i) {
            if (tablet_id_array_.at(i) != tablet_group_dag.tablet_id_array_.at(i)) {
              is_same = false;
            }
          }
        }
      }
    }
  }
  return is_same;
}

int64_t ObTabletGroupMigrationDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = nullptr;
  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
    for (int64_t i = 0; i < tablet_id_array_.count(); ++i) {
      hash_value = common::murmurhash(
          &tablet_id_array_.at(i), sizeof(tablet_id_array_.at(i)), hash_value);
    }
  }
  return hash_value;
}

int ObTabletGroupMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletGroupMigrationDag: log_stream_id = %s, migration_type = %s, first_tablet_id = %s",
       to_cstring(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_),
       to_cstring(tablet_id_array_.at(0))))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObTabletGroupMigrationDag::init(
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    share::ObIDagNet *dag_net,
    share::ObIDag *finish_dag,
    ObHATabletGroupCtx *tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group migration dag init twice", K(ret));
  } else if (tablet_id_array.empty() || OB_ISNULL(dag_net) || OB_ISNULL(finish_dag) || OB_ISNULL(tablet_group_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet group migration init get invalid argument", K(ret), KP(dag_net), KP(finish_dag), KP(tablet_group_ctx));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assgin tablet id array", K(ret), K(tablet_id_array));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    finish_dag_ = finish_dag;
    tablet_group_ctx_ = tablet_group_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletGroupMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletGroupMigrationTask *task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(tablet_id_array_, finish_dag_, tablet_group_ctx_))) {
    LOG_WARN("failed to tablet group migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletGroupMigrationDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupMigrationDag *tablet_group_migration_dag = nullptr;
  bool need_set_failed_result = true;
  ObMigrationCtx *ctx = nullptr;
  ObHATabletGroupCtx *tablet_group_ctx = nullptr;
  ObArray<ObTabletID> tablet_id_array;
  ObDagId dag_id;
  const int64_t start_ts = ObTimeUtil::current_time();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (ctx->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ctx->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ctx));
      ret = tmp_ret;
    }
  } else if (OB_FAIL(ctx->tablet_group_mgr_.get_next_tablet_group_ctx(tablet_group_ctx))) {
    if (OB_ITER_END == ret) {
      //do nothing
      need_set_failed_result = false;
    } else {
      LOG_WARN("failed to get group ctx", K(ret), KPC(ctx));
    }
  } else if (FALSE_IT(dag_id.init(MYADDR))) {
  } else if (OB_FAIL(tablet_group_ctx->get_all_tablet_ids(tablet_id_array))) {
    LOG_WARN("failed to get all tablet ids", K(ret), KPC(ctx));
  } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls migration dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_group_migration_dag))) {
    LOG_WARN("failed to alloc tablet backfill tx migration dag ", K(ret));
  } else if (OB_FAIL(tablet_group_migration_dag->init(tablet_id_array, dag_net, finish_dag_, tablet_group_ctx))) {
    LOG_WARN("failed to init tablet migration dag", K(ret), KPC(ctx));
  } else if (OB_FAIL(tablet_group_migration_dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(ctx));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_group_migration_dag));
    dag = tablet_group_migration_dag;
    tablet_group_migration_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_group_migration_dag)) {
    scheduler->free_dag(*tablet_group_migration_dag);
    tablet_group_migration_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("generate_next_tablet_group dag", "cost", ObTimeUtil::current_time() - start_ts,
        "dag_id", dag_id, "dag_net_id", ctx->task_id_);
  }

  return ret;
}


int ObTabletGroupMigrationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group migration dag migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(ctx->tenant_id_), ctx->arg_.ls_id_.id(),
                                static_cast<int64_t>(tablet_id_array_.at(0).id()),
                                static_cast<int64_t>(ctx->arg_.type_),
                                "dag_net_task_id", to_cstring(ctx->task_id_),
                                "src", to_cstring(ctx->arg_.src_.get_server()),
                                "dest", to_cstring(ctx->arg_.dst_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

/******************ObTabletGroupMigrationTask*********************/
ObTabletGroupMigrationTask::ObTabletGroupMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ls_handle_(),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    tablet_id_array_(),
    finish_dag_(nullptr),
    ha_tablets_builder_(),
    tablet_group_ctx_(nullptr)
{
}

ObTabletGroupMigrationTask::~ObTabletGroupMigrationTask()
{
}

int ObTabletGroupMigrationTask::init(
    const ObIArray<ObTabletID> &tablet_id_array,
    share::ObIDag *finish_dag,
    ObHATabletGroupCtx *tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group migration task init twice", K(ret));
  } else if (tablet_id_array.empty() || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet group migration task init get invlaid argument", K(ret), K(tablet_id_array), KP(finish_dag));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_migration_ctx();
    bandwidth_throttle_ = migration_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = migration_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = migration_dag_net->get_storage_rpc();
    finish_dag_ = finish_dag;
    tablet_group_ctx_ = tablet_group_ctx;

    if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle_))) {
      LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ObLSMigrationUtils::init_ha_tablets_builder(
        ctx_->tenant_id_, tablet_id_array, ctx_->minor_src_, ctx_->local_rebuild_seq_, ctx_->arg_.type_,
        ls_handle_.get_ls(), &ctx_->ha_table_info_mgr_, ha_tablets_builder_))) {
      LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed init tablet group migration task", "ls id", ctx_->arg_.ls_id_,
          "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_, K(tablet_id_array));
    }
  }
  return ret;
}

int ObTabletGroupMigrationTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start do tablet group migration task", K(ret), K(tablet_id_array_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(try_update_local_tablets_())) {
    LOG_WARN("failed to try update local tablets", K(ret), KPC(ctx_));
  } else if (OB_FAIL(build_tablets_sstable_info_())) {
    LOG_WARN("failed to build tablets sstable info", K(ret));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(EventTable::EN_TABLET_GROUP_MIGRATION_TASK_FAILED) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_TABLET_GROUP_MIGRATION_TASK_FAILED", K(ret));
      }
    }
#endif
    if (FAILEDx(generate_tablet_migration_dag_())) {
      LOG_WARN("failed to generate tablet migration dag", K(ret));
    }
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObTabletGroupMigrationTask::generate_tablet_migration_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupMigrationDag *tablet_group_migration_dag = nullptr;
  ObTabletMigrationDag *tablet_migration_dag = nullptr;
  ObLS *ls = nullptr;

  DEBUG_SYNC(BEFORE_TABLET_MIGRATION_GENERATE_NEXT_DAG);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration task do not init", K(ret));
  } else if (OB_ISNULL(tablet_group_migration_dag = static_cast<ObTabletGroupMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group migration dag should not be NULL", K(ret), KP(tablet_group_migration_dag));
  } else if (OB_ISNULL(dag_net = tablet_group_migration_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else {
    ObIDag *parent = this->get_dag();
    ObTabletID tablet_id;
    //generate next_day can execute successful generation only if the first dag is successfully generated
    while (OB_SUCC(ret)) {
      ObTabletHandle tablet_handle;
      if (OB_FAIL(tablet_group_ctx_->get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet id", K(ret), KPC(ctx_));
        }
      } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        }
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_migration_dag))) {
        LOG_WARN("failed to alloc tablet migration dag ", K(ret));
      } else if (OB_FAIL(tablet_migration_dag->init(tablet_id, tablet_handle, dag_net, tablet_group_ctx_))) {
        LOG_WARN("failed to init tablet migration migration dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_migration_dag))) {
        LOG_WARN("failed to add dag into dag net", K(ret), K(*ctx_));
      } else if (OB_FAIL(parent->add_child_without_inheritance(*tablet_migration_dag))) {
        LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_migration_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_migration_dag->add_child_without_inheritance(*finish_dag_))) {
        LOG_WARN("failed to add finish dag as child", K(ret), K(*ctx_));
      } else if (OB_FAIL(scheduler->add_dag(tablet_migration_dag))) {
        LOG_WARN("failed to add tablet migration dag", K(ret), K(*tablet_migration_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule tablet migration dag", K(*tablet_migration_dag), K(tablet_id));
        break;
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(tablet_migration_dag)) {
        scheduler->free_dag(*tablet_migration_dag, tablet_group_migration_dag);
        tablet_migration_dag = nullptr;
      }
    }
  }
  return ret;
}


int ObTabletGroupMigrationTask::build_tablets_sstable_info_()
{
  int ret = OB_SUCCESS;
  bool has_inner_table = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration task do not init", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ObMigrationUtils::check_tablets_has_inner_table(tablet_id_array_, has_inner_table))) {
    LOG_WARN("failed to check tablets has inner table", K(ret), KPC(ctx_));
  } else {
    if (!has_inner_table) {
      DEBUG_SYNC(BEFORE_MIGRATION_BUILD_TABLET_SSTABLE_INFO);
    }

    if (OB_FAIL(ha_tablets_builder_.build_tablets_sstable_info())) {
      LOG_WARN("failed to build tablets sstable info", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObTabletGroupMigrationTask::try_update_local_tablets_()
{
  int ret = OB_SUCCESS;
  bool is_in_retry = false;
  ObTabletGroupMigrationDag *dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration task do not init", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(dag = static_cast<ObTabletGroupMigrationDag *> (this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group migration dag should not be NULL", K(ret), KPC(ctx_), KP(dag));
  } else if (OB_FAIL(dag->check_is_in_retry(is_in_retry))) {
    LOG_WARN("failed to check is in retry", K(ret), KPC(ctx_), KP(dag));
  } else if (!is_in_retry) {
    //do nothing
  } else if (OB_FAIL(try_remove_tablets_info_())) {
    LOG_WARN("failed to try remove tablets info", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ha_tablets_builder_.update_local_tablets())) {
    LOG_WARN("failed to build tablets sstable info", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObTabletGroupMigrationTask::try_remove_tablets_info_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group migration task do not init", K(ret), KPC(ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = tablet_id_array_.at(i);
      if (OB_FAIL(ctx_->ha_table_info_mgr_.remove_tablet_table_info(tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to remove tablet info", K(ret), K(tablet_id), KPC(ctx_));
        }
      }
    }
  }
  return ret;
}

int ObTabletGroupMigrationTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "tablet_group_migration_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "tablet_count", tablet_id_array_.count(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
  }
  return ret;
}

/******************ObMigrationFinishDag*********************/
ObMigrationFinishDag::ObMigrationFinishDag()
  : ObMigrationDag(ObDagType::DAG_TYPE_MIGRATION_FINISH),
    is_inited_(false)
{
}

ObMigrationFinishDag::~ObMigrationFinishDag()
{
}

bool ObMigrationFinishDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObMigrationDag &other_dag = static_cast<const ObMigrationDag&>(other);
    ObMigrationCtx *ctx = get_migration_ctx();
    if (NULL != ctx && NULL != other_dag.get_migration_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_migration_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObMigrationFinishDag::hash() const
{
  int64_t hash_value = 0;
  ObMigrationCtx *ctx = get_migration_ctx();

  if (NULL != ctx) {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObMigrationFinishDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObMigrationCtx *ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_migration_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObMigrationFinishDag: ls_id = %s, migration_type = %s",
       to_cstring(ctx->arg_.ls_id_), ObMigrationOpType::get_str(ctx->arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObMigrationFinishDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migration finish dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = migration_dag_net->get_migration_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObMigrationFinishDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObMigrationFinishTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish dag do not init", K(ret));
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

/******************ObMigrationFinishTask*********************/
ObMigrationFinishTask::ObMigrationFinishTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObMigrationFinishTask::~ObMigrationFinishTask()
{
}

int ObMigrationFinishTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObMigrationDagNet* migration_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("migration finish task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(migration_dag_net = static_cast<ObMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = migration_dag_net->get_migration_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init migration finish task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObMigrationFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("start do migration finish task");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    bool allow_retry = false;
    if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
      LOG_ERROR("failed to check allow retry", K(ret), K(*ctx_));
    } else if (allow_retry) {
      if (OB_FAIL(generate_migration_init_dag_())) {
        LOG_WARN("failed to generate migration init dag", K(ret), KPC(ctx_));
      }
    }
  } else {
  }
  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret), K(ret));
  }
  return ret;
}

int ObMigrationFinishTask::generate_migration_init_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialMigrationDag *initial_migration_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObMigrationFinishDag *migration_finish_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migration finish task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(migration_finish_dag = static_cast<ObMigrationFinishDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration finish dag should not be NULL", K(ret), KP(migration_finish_dag));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(initial_migration_dag))) {
      LOG_WARN("failed to alloc initial migration dag ", K(ret));
    } else if (OB_FAIL(initial_migration_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial migration dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_migration_dag))) {
      LOG_WARN("failed to add initial migration dag", K(ret), KPC(initial_migration_dag));
    } else if (OB_FAIL(initial_migration_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_migration_dag))) {
      LOG_WARN("failed to add migration finish dag", K(ret), K(*initial_migration_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create initial migration dag", K(ret), K(*ctx_));
      initial_migration_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_migration_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_migration_dag, migration_finish_dag);
      initial_migration_dag = nullptr;
    }
  }
  return ret;
}

int ObMigrationFinishTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret), KPC_(ctx));
  } else {
    SERVER_EVENT_ADD("storage_ha", "migration_finish_task",
        "tenant_id", ctx_->tenant_id_,
        "ls_id", ctx_->arg_.ls_id_.id(),
        "src", ctx_->arg_.src_.get_server(),
        "dst", ctx_->arg_.dst_.get_server(),
        "task_id", ctx_->task_id_,
        "is_failed", ctx_->is_failed(),
        ObMigrationOpType::get_str(ctx_->arg_.type_));
    const int64_t total_cost_time = ctx_->check_tablet_info_cost_time_;
    const int64_t total_tablet_count = ctx_->data_tablet_id_array_.count();
    int64_t avg_cost_time = 0;
    if (0 != total_tablet_count) {
      avg_cost_time = total_cost_time / total_tablet_count;
    }
    SERVER_EVENT_ADD("storage_ha", "check_tablet_info",
                     "tenant_id", ctx_->tenant_id_,
                     "ls_id", ctx_->arg_.ls_id_.id(),
                     "total_cost_time_us", total_cost_time,
                     "total_tablet_count", total_tablet_count,
                     "avg_cost_time_us", avg_cost_time);
  }
  return ret;
}

/******************ObLSMigrationUtils*********************/
int ObLSMigrationUtils::init_ha_tablets_builder(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    const ObStorageHASrcInfo src_info,
    const int64_t local_rebuild_seq,
    const ObMigrationOpType::TYPE &type,
    ObLS *ls,
    ObStorageHATableInfoMgr *ha_table_info_mgr,
    ObStorageHATabletsBuilder &ha_tablets_builder)
{
  int ret = OB_SUCCESS;
  ObStorageHATabletsBuilderParam param;
  ObLSService *ls_service = nullptr;

  if (OB_INVALID_ID == tenant_id || !src_info.is_valid() || local_rebuild_seq < 0 || !ObMigrationOpType::is_valid(type)
      || OB_ISNULL(ls) || OB_ISNULL(ha_table_info_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ha tablets builder get unexpected error", K(ret), KP(tenant_id), K(tablet_id_array),
        K(src_info), K(local_rebuild_seq), KP(ls));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(param.tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    param.bandwidth_throttle_ = GCTX.bandwidth_throttle_;
    param.is_leader_restore_ = false;
    param.local_rebuild_seq_ = local_rebuild_seq;
    param.need_check_seq_ = true;
    param.ls_ = ls;
    param.meta_index_store_ = nullptr;
    param.need_check_seq_ = true;
    param.restore_base_info_ = nullptr;
    param.restore_action_ = ObTabletRestoreAction::RESTORE_NONE;
    param.src_info_ = src_info;
    param.storage_rpc_ = ls_service->get_storage_rpc();
    param.svr_rpc_proxy_ = ls_service->get_storage_rpc_proxy();
    param.tenant_id_ = tenant_id;
    param.ha_table_info_mgr_ = ha_table_info_mgr;
    param.need_keep_old_tablet_ = ObMigrationOpType::need_keep_old_tablet(type);

    if (OB_FAIL(ha_tablets_builder.init(param))) {
      LOG_WARN("failed to init ha tablets builder", K(ret), K(param));
    }
  }
  return ret;
}

}
}

