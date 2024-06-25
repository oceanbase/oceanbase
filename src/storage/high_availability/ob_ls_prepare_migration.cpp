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
#include "ob_ls_prepare_migration.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "logservice/ob_log_service.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "ob_transfer_service.h"
#include "storage/tablet/ob_tablet.h"
#include "ob_rebuild_service.h"
#include "ob_storage_ha_utils.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

/******************ObLSMigrationPrepareCtx*********************/
ObLSPrepareMigrationCtx::ObLSPrepareMigrationCtx()
  : ObIHADagNetCtx(),
    tenant_id_(OB_INVALID_ID),
    arg_(),
    task_id_(),
    start_ts_(0),
    finish_ts_(0),
    log_sync_scn_(SCN::min_scn())
{
}

ObLSPrepareMigrationCtx::~ObLSPrepareMigrationCtx()
{
}

bool ObLSPrepareMigrationCtx::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && tenant_id_ != 0 && tenant_id_ != OB_INVALID_ID && log_sync_scn_.is_valid();
}

void ObLSPrepareMigrationCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  arg_.reset();
  task_id_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  log_sync_scn_.set_min();
  ObIHADagNetCtx::reset();
}

int ObLSPrepareMigrationCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ls prepare migration : task_id = %s, "
      "tenant_id = %s, ls_id = %s, src = %s, dest = %s",
      to_cstring(task_id_), to_cstring(tenant_id_), to_cstring(arg_.ls_id_), to_cstring(arg_.src_.get_server()),
      to_cstring(arg_.dst_.get_server())))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

void ObLSPrepareMigrationCtx::reuse()
{
  ObIHADagNetCtx::reuse();
  log_sync_scn_.set_min();
}

/******************ObLSPrepareMigrationDagNet*********************/
ObLSPrepareMigrationParam::ObLSPrepareMigrationParam()
  : arg_(),
    task_id_()
{
}

bool ObLSPrepareMigrationParam::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid();
}


ObLSPrepareMigrationDagNet::ObLSPrepareMigrationDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_PREPARE_MIGARTION),
      is_inited_(false),
      ctx_()

{
}

ObLSPrepareMigrationDagNet::~ObLSPrepareMigrationDagNet()
{
}

int ObLSPrepareMigrationDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLSPrepareMigrationParam* init_param = static_cast<const ObLSPrepareMigrationParam*>(param);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls prepare migration dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else {
    ctx_.tenant_id_ = MTL_ID();
    ctx_.arg_ = init_param->arg_;
    ctx_.task_id_ = init_param->task_id_;;
    is_inited_ = true;
  }
  return ret;
}

bool ObLSPrepareMigrationDagNet::is_valid() const
{
  return ctx_.is_valid();
}

int ObLSPrepareMigrationDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_migration_())) {
    LOG_WARN("failed to start running for migration", K(ret));
  }

  return ret;
}

int ObLSPrepareMigrationDagNet::start_running_for_migration_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialPrepareMigrationDag *initial_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_.start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_.arg_.type_, prio))) {
    LOG_WARN("failed to get dag priority", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, initial_dag))) {
    LOG_WARN("failed to alloc initial dag ", K(ret));
  } else if (OB_FAIL(initial_dag->init(this))) {
    LOG_WARN("failed to init initial dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_dag))) {
    LOG_WARN("failed to add initial dag into dag net", K(ret));
  } else if (OB_FAIL(initial_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_dag))) {
    LOG_WARN("failed to add initial dag", K(ret), K(*initial_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_dag) && OB_NOT_NULL(scheduler)) {
    if (OB_SUCCESS != (tmp_ret = erase_dag_from_dag_net(*initial_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_dag));
    }
    scheduler->free_dag(*initial_dag); // contain reset_children
  }

  return ret;
}

bool ObLSPrepareMigrationDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSPrepareMigrationDagNet &other_dag_net = static_cast<const ObLSPrepareMigrationDagNet &>(other);
    if (!is_valid() || !other_dag_net.is_valid()) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ERROR, "ls prepare migration dag net is invalid", K(*this), K(other));
    } else if (ctx_.arg_.ls_id_ != other_dag_net.get_ls_id()) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObLSPrepareMigrationDagNet::hash() const
{
  int64_t hash_value = 0;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    tmp_ret = OB_NOT_INIT;
    LOG_ERROR_RET(tmp_ret, "migration ctx is NULL", K(tmp_ret), K(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_.arg_.ls_id_, sizeof(ctx_.arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObLSPrepareMigrationDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration dag net do not init ", K(ret));
  } else if (OB_UNLIKELY(0 > ctx_.task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), K(ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "ObLSMigrationPrepareDagNet: tenant_id=%s, ls_id=%s, migration_type=%d, trace_id=%s",
          to_cstring(ctx_.tenant_id_), to_cstring(ctx_.arg_.ls_id_), ctx_.arg_.type_, task_id_str))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

int ObLSPrepareMigrationDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObLSMigrationDagNet: ls_id = %s, migration_type = %s",
      to_cstring(ctx_.arg_.ls_id_), ObMigrationOpType::get_str(ctx_.arg_.type_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

int ObLSPrepareMigrationDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  int32_t result = OB_SUCCESS;
  ObLSMigrationHandler *ls_migration_handler = nullptr;
  ObLSHandle ls_handle;
  LOG_INFO("start clear dag net ctx", K(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), K(ctx_));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_.arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ls should not be NULL", K(ret), K(ctx_));
  } else {
    if (OB_ISNULL(ls_migration_handler = ls->get_ls_migration_handler())) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls migration handler should not be NULL", K(tmp_ret), K(ctx_));
    } else if (OB_SUCCESS != (tmp_ret = ls_migration_handler->switch_next_stage(result))) {
      LOG_WARN("failed to report result", K(ret), K(tmp_ret), K(ctx_));
    }

    ctx_.finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_.finish_ts_ - ctx_.start_ts_;
    FLOG_INFO("finish ls prepare migration dag net", "ls id", ctx_.arg_.ls_id_, "type", ctx_.arg_.type_, K(cost_ts));
  }
  return ret;
}

int ObLSPrepareMigrationDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls prepare migration dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_.set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/******************ObPrepareMigrationDag*********************/
ObPrepareMigrationDag::ObPrepareMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObPrepareMigrationDag::~ObPrepareMigrationDag()
{
}

bool ObPrepareMigrationDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (OB_ISNULL(ha_dag_net_ctx_) || OB_ISNULL(ha_dag.get_ha_dag_net_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "prepare migration ctx should not be NULL", KP(ha_dag_net_ctx_), KP(ha_dag.get_ha_dag_net_ctx()));
    } else if (ha_dag_net_ctx_->get_dag_net_ctx_type() != ha_dag.get_ha_dag_net_ctx()->get_dag_net_ctx_type()) {
      is_same = false;
    } else {
      ObLSPrepareMigrationCtx *self_ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_);
      ObLSPrepareMigrationCtx *other_ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag.get_ha_dag_net_ctx());
      if (self_ctx->arg_.ls_id_ != other_ctx->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObPrepareMigrationDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("prepare migration ctx should not be NULL", KP(ha_dag_net_ctx_));
  } else if (ObIHADagNetCtx::LS_PREPARE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else {
    ObLSPrepareMigrationCtx *self_ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_);
    hash_value = common::murmurhash(
        &self_ctx->arg_.ls_id_, sizeof(self_ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObPrepareMigrationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObLSPrepareMigrationCtx *ctx = nullptr;

  if (FALSE_IT(ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_))) {
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

int ObPrepareMigrationDag::prepare_ctx(share::ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSPrepareMigrationDagNet *prepare_dag_net = nullptr;
  ObLSPrepareMigrationCtx *self_ctx = nullptr;

  if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_PREPARE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(prepare_dag_net = static_cast<ObLSPrepareMigrationDagNet*>(dag_net))) {
  } else if (FALSE_IT(self_ctx = prepare_dag_net->get_ctx())) {
  } else if (OB_ISNULL(self_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls prepare migration dag net ctx should not be NULL", K(ret), KP(self_ctx));
  } else {
    ha_dag_net_ctx_ = self_ctx;
  }
  return ret;
}

/******************ObInitialPrepareMigrationDag*********************/
ObInitialPrepareMigrationDag::ObInitialPrepareMigrationDag()
  : ObPrepareMigrationDag(ObDagType::DAG_TYPE_INITIAL_PREPARE_MIGRATION),
    is_inited_(false)
{
}

ObInitialPrepareMigrationDag::~ObInitialPrepareMigrationDag()
{
}

int ObInitialPrepareMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSPrepareMigrationCtx *self_ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_PREPARE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
         "ObInitialPrepareMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
         to_cstring(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
         ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObInitialPrepareMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial prepare migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init initial prepare migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInitialPrepareMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialPrepareMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial prepare migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObInitialMigrationTask*********************/
ObInitialPrepareMigrationTask::ObInitialPrepareMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObInitialPrepareMigrationTask::~ObInitialPrepareMigrationTask()
{
}

int ObInitialPrepareMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSPrepareMigrationDagNet *prepare_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial prepare migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_PREPARE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(prepare_dag_net = static_cast<ObLSPrepareMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = prepare_dag_net->get_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init initial prepare migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObInitialPrepareMigrationTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration task do not init", K(ret));
  } else if (OB_FAIL(generate_migration_dags_())) {
    LOG_WARN("failed to generate migration dags", K(ret), K(*ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObInitialPrepareMigrationTask::generate_migration_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartPrepareMigrationDag *start_prepare_dag = nullptr;
  ObFinishPrepareMigrationDag *finish_prepare_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialPrepareMigrationDag *initial_prepare_migration_dag = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial prepare migration task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_prepare_migration_dag = static_cast<ObInitialPrepareMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial prepare migration dag should not be NULL", K(ret), KP(initial_prepare_migration_dag));
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_->arg_.type_, prio))) {
    LOG_WARN("failed to get dag priority", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, start_prepare_dag))) {
      LOG_WARN("failed to alloc start prepare migration dag ", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, finish_prepare_dag))) {
      LOG_WARN("failed to alloc finish prepare migration dag", K(ret));
    } else if (OB_FAIL(start_prepare_dag->init(dag_net_))) {
      LOG_WARN("failed to init start prepare migration dag", K(ret));
    } else if (OB_FAIL(finish_prepare_dag->init(dag_net_))) {
      LOG_WARN("failed to init finish prepare migration dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*start_prepare_dag))) {
      LOG_WARN("failed to add start prepare dag", K(ret), KPC(start_prepare_dag));
    } else if (OB_FAIL(start_prepare_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(start_prepare_dag->add_child(*finish_prepare_dag))) {
      LOG_WARN("failed to add migration finish dag as child", K(ret));
    } else if (OB_FAIL(finish_prepare_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(finish_prepare_dag))) {
      LOG_WARN("failed to add finish prepare migration dag", K(ret), K(*finish_prepare_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(scheduler->add_dag(start_prepare_dag))) {
      LOG_WARN("failed to add dag", K(ret), K(*start_prepare_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
      if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(finish_prepare_dag))) {
        LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(initial_prepare_migration_dag));
      } else {
        finish_prepare_dag = nullptr;
      }
    } else {
      LOG_INFO("succeed to schedule start prepare migration dag", K(*start_prepare_dag));
      start_prepare_dag = nullptr;
      finish_prepare_dag = nullptr;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_prepare_dag)) {
        scheduler->free_dag(*finish_prepare_dag);
        finish_prepare_dag = nullptr;
      }

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_prepare_dag)) {
        scheduler->free_dag(*start_prepare_dag);
        start_prepare_dag = nullptr;
      }

      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, true /*allow_retry*/, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set ls prepare migration result", K(ret), K(tmp_ret), K(*ctx_));
      }
    }
  }
  return ret;
}

/******************ObStartPrepareMigrationDag*********************/
ObStartPrepareMigrationDag::ObStartPrepareMigrationDag()
  : ObPrepareMigrationDag(ObDagType::DAG_TYPE_START_PREPARE_MIGRATION),
    is_inited_(false)
{
}

ObStartPrepareMigrationDag::~ObStartPrepareMigrationDag()
{
}

int ObStartPrepareMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSPrepareMigrationCtx *self_ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start prepare migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_PREPARE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObStartPrepareMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
       to_cstring(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObStartPrepareMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start prepare migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init start prepare migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStartPrepareMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartPrepareMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start prepare migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init start prepare migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("success to create start prepare migration first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartPrepareMigrationTask*********************/
ObStartPrepareMigrationTask::ObStartPrepareMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr)
{
}

ObStartPrepareMigrationTask::~ObStartPrepareMigrationTask()
{
}

int ObStartPrepareMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSPrepareMigrationDagNet *prepare_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_PREPARE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(prepare_dag_net = static_cast<ObLSPrepareMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = prepare_dag_net->get_ctx();
    is_inited_ = true;
    LOG_INFO("succeed init start prepare migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObStartPrepareMigrationTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start prepare migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(deal_with_local_ls_())) {
    LOG_WARN("failed to deal with local ls", K(ret), K(*ctx_));
  } else if (OB_FAIL(wait_transfer_tablets_ready_())) {
    LOG_WARN("failed to wait transfer tablets ready", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObStartPrepareMigrationTask::deal_with_local_ls_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  bool is_leader = false;
  ObLSSavedInfo saved_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start prepare migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("log stream should not be NULL", K(ret), K(*ctx_));
  } else if (OB_FAIL(ObStorageHAUtils::check_ls_is_leader(
        ctx_->tenant_id_, ctx_->arg_.ls_id_, is_leader))) {
    LOG_WARN("failed to check ls leader", K(ret), KPC(ctx_));
  } else if (is_leader) {
    if (ObMigrationOpType::REBUILD_LS_OP == ctx_->arg_.type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("leader can not as rebuild dst", K(ret), K(is_leader), "myaddr", MYADDR, "arg", ctx_->arg_);
    } else if (ObMigrationOpType::ADD_LS_OP == ctx_->arg_.type_
        || ObMigrationOpType::MIGRATE_LS_OP == ctx_->arg_.type_
        || ObMigrationOpType::CHANGE_LS_OP == ctx_->arg_.type_) {
      ret = OB_ERR_SYS;
      LOG_WARN("leader cannot as add, migrate, change dst",
          K(ret), K(is_leader), "myaddr", MYADDR, "arg", ctx_->arg_);
    }
  }

  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->disable_sync())) {
    LOG_WARN("failed to disable log sync", K(ret), KPC(ls));
  } else if (OB_FAIL(ls->get_saved_info(saved_info))) {
    LOG_WARN("failed to get saved info", K(ret), KPC(ls));
  } else if (!saved_info.is_empty()) {
    ctx_->log_sync_scn_ = saved_info.clog_checkpoint_scn_;
  } else if (OB_FAIL(ls->get_end_scn(ctx_->log_sync_scn_))) {
    LOG_WARN("failed to get end ts ns", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartPrepareMigrationTask::build_tablet_backfill_info_(common::ObArray<ObTabletBackfillInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillInfo tablet_info;
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->tablet_id_array_.count(); ++i) {
    ObTabletID tablet_id = ctx_->tablet_id_array_.at(i);
    if (OB_FAIL(tablet_info.init(tablet_id, true/*is_committed*/))) {
      LOG_WARN("failed to init tablet info", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
      LOG_WARN("failed to push tablet info into array", K(ret), K(tablet_info));
    }
  }
  return ret;
}

int ObStartPrepareMigrationTask::wait_transfer_tablets_ready_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSTabletIterator tablet_iterator(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ObTabletCreateDeleteMdsUserData user_data;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start prepare migration task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(ls->build_tablet_iter(tablet_iterator))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), KPC(ctx_));
  } else {
    while (OB_SUCC(ret)) {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      user_data.reset();
      if (OB_FAIL(tablet_iterator.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet", K(ret), KPC(ctx_));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle));
      } else if (!tablet->get_tablet_meta().ha_status_.is_data_status_complete()) {
        //do nothing
      } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
        //do nothing
      } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet, user_data))) {
        LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
        }
      } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
          && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_) {
        //do nothing
      } else if (OB_FAIL(wait_transfer_out_tablet_ready_(ls, tablet))) {
        LOG_WARN("failed to wait transfer out tablet ready", K(ret), KPC(tablet));
      }
    }
  }
  return ret;
}

int ObStartPrepareMigrationTask::wait_transfer_out_tablet_ready_(
    ObLS *ls,
    ObTablet *tablet)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  bool is_cancel = false;
  ObLSHandle dest_ls_handle;
  ObLS *dest_ls = nullptr;
  ObTabletHandle dest_tablet_handle;
  ObTablet *dest_tablet = nullptr;
  ObTabletCreateDeleteMdsUserData dest_user_data;
  ObTransferService *transfer_service = nullptr;
  const int64_t MAX_WAIT_TRANSFER_IN_TABLET_READY = 30 *60 * 1000 * 1000L; //30min
  const int64_t MAX_SLEEP_INTERVAL_MS = 100* 1000; //100ms
  SCN current_replay_scn;
  ObMigrationStatus status = ObMigrationStatus::OB_MIGRATION_STATUS_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start prepare migration task do not init", K(ret));
  } else if (OB_ISNULL(transfer_service = (MTL(ObTransferService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service should not be NULL", K(ret), KP(transfer_service));
  } else if (OB_ISNULL(tablet) || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("wait transfer out tablet ready get invalid argument", K(ret), KP(tablet), KP(ls));
  } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, tablet, user_data))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
  } else if (ObTabletStatus::TRANSFER_OUT != user_data.tablet_status_
      && ObTabletStatus::TRANSFER_OUT_DELETED != user_data.tablet_status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet status is unexpected", K(ret), K(user_data), KPC(tablet));
  } else {
    const int64_t wait_transfer_tablet_ready_ts = ObTimeUtility::current_time();
    while (OB_SUCC(ret)) {
      if (ctx_->is_failed()) {
        ret = OB_CANCELED;
        STORAGE_LOG(WARN, "ls migration task is failed, cancel wait ls check point ts push", K(ret));
      } else if (ls->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, stop migration dag net", K(ret), K(ctx_));
      } else if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(get_dag()->get_dag_id(), is_cancel))) {
        STORAGE_LOG(ERROR, "failed to check is task canceled", K(ret), K(*this));
      } else if (is_cancel) {
        ret = OB_CANCELED;
        STORAGE_LOG(WARN, "task is cancelled", K(ret), K(*this));
      } else if (OB_FAIL(ObStorageHADagUtils::get_ls(user_data.transfer_ls_id_, dest_ls_handle))) {
        if (OB_LS_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get ls", K(ret), K(user_data));
        }
      } else if (OB_ISNULL(dest_ls = dest_ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls should not be NULL", K(ret), KP(dest_ls), K(user_data));
      } else if (OB_FAIL(dest_ls->get_migration_status(status))) {
        LOG_WARN("failed to get dest ls migration status", K(ret), K(user_data), KPC(dest_ls));
      } else if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != status
            && ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_WAIT != status
            && ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_WAIT != status
            && ObMigrationStatus::OB_MIGRATION_STATUS_ADD_WAIT != status
            && ObMigrationStatus::OB_MIGRATION_STATUS_HOLD != status) {
        if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL == status
            || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL == status
            || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_FAIL == status) {
          LOG_INFO("dest ls is in migration failed status, no need wait", K(ret), K(status), KPC(dest_ls));
          break;
        }
      } else if (OB_FAIL(dest_ls->get_max_decided_scn(current_replay_scn))) {
        LOG_WARN("failed to get current replay log scn", K(ret), KPC(ctx_));
      } else if (current_replay_scn < user_data.transfer_scn_) {
        ObRebuildService *rebuild_service = nullptr;
        bool need_rebuild = false;
        if (OB_ISNULL(rebuild_service = MTL(ObRebuildService *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("rebuild service should not be NULL", K(ret), KP(rebuild_service));
        } else if (OB_FAIL(rebuild_service->check_ls_need_rebuild(dest_ls->get_ls_id(), need_rebuild))) {
          LOG_WARN("failed to to check dest ls need rebuild", K(ret), KPC(dest_ls));
        } else if (need_rebuild) {
          LOG_INFO("dest ls need rebuild, no need wait", K(ret), KPC(dest_ls));
          break;
        }
      } else if (OB_FAIL(dest_ls->get_tablet(tablet->get_tablet_meta().tablet_id_, dest_tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), KPC(tablet));
        }
      } else if (OB_ISNULL(dest_tablet = dest_tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(dest_tablet));
      } else if ((ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD_WAIT == status
            || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_WAIT == status
            || ObMigrationStatus::OB_MIGRATION_STATUS_ADD_WAIT == status)
          && dest_tablet->get_tablet_meta().has_transfer_table()) {
      } else if (OB_FAIL(ObTXTransferUtils::get_tablet_status(false/*get_commit*/, dest_tablet, dest_user_data))) {
        LOG_WARN("failed to get tablet status", K(ret), KPC(dest_tablet));
      } else if (ObTabletStatus::TRANSFER_IN != dest_user_data.tablet_status_
          || dest_tablet->get_tablet_meta().transfer_info_.transfer_seq_ != tablet->get_tablet_meta().transfer_info_.transfer_seq_ + 1
          || !dest_tablet->get_tablet_meta().has_transfer_table()) {
        break;
      } else {
        transfer_service->wakeup();
      }

      if (OB_SUCC(ret)) {
        const int64_t current_ts = ObTimeUtility::current_time();
        if (current_ts - wait_transfer_tablet_ready_ts >= MAX_WAIT_TRANSFER_IN_TABLET_READY) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait transfer in tablet ready time out",
              "dest_ls_id", user_data.transfer_ls_id_, "dest user data", dest_user_data);
        } else {
          LOG_INFO("wait transfer in tablet ready", "dest user data", dest_user_data,
              KPC(dest_tablet));
          ob_usleep(MAX_SLEEP_INTERVAL_MS);
        }
      }
    }
  }
  return ret;
}

/******************ObFinishPrepareMigrationDag*********************/
ObFinishPrepareMigrationDag::ObFinishPrepareMigrationDag()
  : ObPrepareMigrationDag(ObDagType::DAG_TYPE_FINISH_PREPARE_MIGRATION),
    is_inited_(false)
{
}

ObFinishPrepareMigrationDag::~ObFinishPrepareMigrationDag()
{
}

int ObFinishPrepareMigrationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSPrepareMigrationCtx *self_ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish prepare migration dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_PREPARE_MIGRATION != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  } else if (FALSE_IT(self_ctx = static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObFinishPrepareMigrationDag: ls_id = %s, migration_type = %s, dag_prio = %s",
       to_cstring(self_ctx->arg_.ls_id_), ObMigrationOpType::get_str(self_ctx->arg_.type_),
       ObIDag::get_dag_prio_str(this->get_priority())))) {
    LOG_WARN("failed to fill comment", K(ret), K(*self_ctx));
  }
  return ret;
}

int ObFinishPrepareMigrationDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish prepare migration dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init finish prepare migration dag get invalid argument", K(ret), KP(dag_net));
  } else if (OB_FAIL(prepare_ctx(dag_net))) {
    LOG_WARN("failed to prepare ctx", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFinishPrepareMigrationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishPrepareMigrationTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish prepare migration dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish prepare migration task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishPrepareMigrationTask*********************/
ObFinishPrepareMigrationTask::ObFinishPrepareMigrationTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObFinishPrepareMigrationTask::~ObFinishPrepareMigrationTask()
{
}

int ObFinishPrepareMigrationTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSPrepareMigrationDagNet *prepare_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish prepare migration task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_PREPARE_MIGARTION != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(prepare_dag_net = static_cast<ObLSPrepareMigrationDagNet*>(dag_net))) {
  } else {
    ctx_ = prepare_dag_net->get_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init start prepare migration task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObFinishPrepareMigrationTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start do finish prepare migration task", KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish prepare migration task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    bool allow_retry = false;
    if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
      LOG_ERROR("failed to check need retry", K(ret), K(*ctx_));
    } else if (allow_retry) {
      ctx_->reuse();
      if (OB_FAIL(generate_prepare_initial_dag_())) {
        LOG_WARN("failed to generate prepare initial dag", K(ret), KPC(ctx_));
      }
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObFinishPrepareMigrationTask::generate_prepare_initial_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialPrepareMigrationDag *initial_prepare_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObFinishPrepareMigrationDag *finish_prepare_migration_dag = nullptr;
  ObDagPrio::ObDagPrioEnum prio = ObDagPrio::DAG_PRIO_MAX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish prepare migration task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(finish_prepare_migration_dag = static_cast<ObFinishPrepareMigrationDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish prepare migration dag should not be NULL", K(ret), KP(finish_prepare_migration_dag));
  } else if (OB_FAIL(ObMigrationUtils::get_dag_priority(ctx_->arg_.type_, prio))) {
    LOG_WARN("failed to get dag priority", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, initial_prepare_dag))) {
      LOG_WARN("failed to alloc initial prepare migration dag ", K(ret));
    } else if (OB_FAIL(initial_prepare_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial migration dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_prepare_dag))) {
      LOG_WARN("failed to add initial prepare dag", K(ret), KPC(initial_prepare_dag));
    } else if (OB_FAIL(initial_prepare_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_prepare_dag))) {
      LOG_WARN("failed to add initial prepare migration dag", K(ret), K(*initial_prepare_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create initial prepare migration dag", K(ret), K(*ctx_));
      initial_prepare_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_prepare_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_prepare_dag);
      initial_prepare_dag = nullptr;
    }
  }
  return ret;
}
