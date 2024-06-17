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
#include "ob_ls_restore.h"
#include "observer/ob_server.h"
#include "ob_physical_copy_task.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_reader.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_mds_helper.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

/******************ObLSRestoreCtx*********************/
ObLSRestoreCtx::ObLSRestoreCtx()
  : ObIHADagNetCtx(),
    arg_(),
    start_ts_(0),
    finish_ts_(0),
    task_id_(),
    src_(),
    src_ls_meta_package_(),
    sys_tablet_id_array_(),
    data_tablet_id_array_(),
    ha_table_info_mgr_(),
    tablet_group_mgr_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1)
{
}

ObLSRestoreCtx::~ObLSRestoreCtx()
{
}

bool ObLSRestoreCtx::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
        && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);;
}

void ObLSRestoreCtx::reset()
{
  arg_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  task_id_.reset();
  src_.reset();
  src_ls_meta_package_.reset();
  ha_table_info_mgr_.reuse();
  tablet_group_mgr_.reuse();
  ObIHADagNetCtx::reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}


int ObLSRestoreCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ls restore : task_id = %s, ls_id = %s, src = %s, dest = %s",
      to_cstring(task_id_), to_cstring(arg_.ls_id_), to_cstring(arg_.src_.get_server()),
      to_cstring(arg_.dst_.get_server())))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

void ObLSRestoreCtx::reuse()
{
  ObIHADagNetCtx::reuse();
  src_.reset();
  src_ls_meta_package_.reset();
  sys_tablet_id_array_.reset();
  data_tablet_id_array_.reset();
  ha_table_info_mgr_.reuse();
  tablet_group_mgr_.reuse();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

/******************ObLSRestoreDagNet*********************/
ObLSRestoreDagNetInitParam::ObLSRestoreDagNetInitParam()
  : arg_(),
    task_id_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr)
{
}

bool ObLSRestoreDagNetInitParam::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && OB_NOT_NULL(bandwidth_throttle_)
      && OB_NOT_NULL(svr_rpc_proxy_)
      && OB_NOT_NULL(storage_rpc_);
}


ObLSRestoreDagNet::ObLSRestoreDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_RESTORE),
      is_inited_(false),
      ctx_(nullptr),
      meta_index_store_(),
      second_meta_index_store_(),
      kv_cache_(nullptr),
      bandwidth_throttle_(nullptr),
      svr_rpc_proxy_(nullptr),
      storage_rpc_(nullptr)

{
}

ObLSRestoreDagNet::~ObLSRestoreDagNet()
{
  free_ls_restore_ctx_();
}

int ObLSRestoreDagNet::alloc_ls_restore_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls restore ctx init twice", K(ret), KPC(ctx_));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObLSRestoreCtx), "LSRestoreCtx"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ctx_ = new (buf) ObLSRestoreCtx())) {
  }
  return ret;
}

void ObLSRestoreDagNet::free_ls_restore_ctx_()
{
  if (OB_ISNULL(ctx_)) {
    //do nothing
  } else {
    ctx_->~ObLSRestoreCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

int ObLSRestoreDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLSRestoreDagNetInitParam *init_param = static_cast<const ObLSRestoreDagNetInitParam*>(param);
  const int64_t priority = 1;
  char buf[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls restore dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid() || !OB_BACKUP_INDEX_CACHE.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (init_param->arg_.is_leader_) {
    const backup::ObBackupRestoreMode mode = backup::ObBackupRestoreMode::RESTORE_MODE;
    const backup::ObBackupIndexLevel index_level = backup::ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
    storage::ObExternBackupSetInfoDesc backup_set_file_desc;
    backup::ObBackupIndexStoreParam index_store_param;
    storage::ObBackupDataStore store;
    int64_t retry_id = 0;
    if (OB_FAIL(store.init(init_param->arg_.restore_base_info_.backup_dest_))) {
      LOG_WARN("fail to init mgr", K(ret));
    } else if (OB_FAIL(store.read_backup_set_info(backup_set_file_desc))) {
      LOG_WARN("fail to read backup set info", K(ret));
    } else {
      share::ObBackupDataType data_type;
      data_type.set_sys_data_backup();
      index_store_param.index_level_ = index_level;
      index_store_param.tenant_id_ = MTL_ID();
      index_store_param.backup_set_id_ = backup_set_file_desc.backup_set_file_.backup_set_id_;
      index_store_param.ls_id_ = init_param->arg_.ls_id_;
      index_store_param.is_tenant_level_ = false;
      index_store_param.backup_data_type_ = data_type;
      index_store_param.turn_id_ = init_param->arg_.ls_id_.is_sys_ls() ?
                                   1/*sys ls only has one turn*/ : backup_set_file_desc.backup_set_file_.meta_turn_id_;

      ObBackupPath backup_path;
      if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(
          init_param->arg_.restore_base_info_.backup_dest_, init_param->arg_.ls_id_, backup_path))) {
        LOG_WARN("failed to get ls backup dir path", K(ret), KPC(init_param));
      } else if (OB_FAIL(store.get_max_sys_ls_retry_id(backup_path, init_param->arg_.ls_id_, index_store_param.turn_id_, retry_id))) {
        LOG_WARN("failed to get max sys retry id", K(ret), K(backup_path), KPC(init_param));
      } else {
        index_store_param.retry_id_ = retry_id;
        LOG_INFO("get max sys ls retry id", "arg", init_param->arg_, K(retry_id));
      }
    }

    share::ObBackupDest dest;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_index_store_.init(mode, index_store_param,
        init_param->arg_.restore_base_info_.backup_dest_,
        backup_set_file_desc.backup_set_file_, false/*is_sec_meta*/, true/*init sys tablet index store*/, OB_BACKUP_INDEX_CACHE))) {
      LOG_WARN("failed to init meta index store", K(ret), KPC(init_param));
    } else if (OB_FAIL(second_meta_index_store_.init(mode, index_store_param,
        init_param->arg_.restore_base_info_.backup_dest_,
        backup_set_file_desc.backup_set_file_, true/*is_sec_meta*/, true/*init sys tablet index store*/, OB_BACKUP_INDEX_CACHE))) {
      LOG_WARN("failed to init macro index store", K(ret), KPC(init_param));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(alloc_ls_restore_ctx_())) {
    LOG_WARN("failed to alloc ls restore ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->arg_.assign(init_param->arg_))) {
    LOG_WARN("failed to assign restore ctx arg", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.init())) {
    LOG_WARN("failed to init ha table key mgr", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->tablet_group_mgr_.init())) {
    LOG_WARN("failed to init tablet group mgr", K(ret), KPC(init_param));
  } else {
    ctx_->task_id_ = init_param->task_id_;
    kv_cache_ = &OB_BACKUP_INDEX_CACHE;
    bandwidth_throttle_ = init_param->bandwidth_throttle_;
    svr_rpc_proxy_ = init_param->svr_rpc_proxy_;
    storage_rpc_ = init_param->storage_rpc_;
    is_inited_ = true;
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_LS_INIT_PARAM_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("init ls restore dag param failed", K(ret));
    }
  }
#endif
  return ret;
}

int ObLSRestoreDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_ls_restore_())) {
    LOG_WARN("failed to start running for ls restore", K(ret));
  }

  return ret;
}

int ObLSRestoreDagNet::start_running_for_ls_restore_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialLSRestoreDag *initial_ls_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_->start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_ls_restore_dag))) {
    LOG_WARN("failed to alloc initial ls restore dag ", K(ret));
  } else if (OB_FAIL(initial_ls_restore_dag->init(this))) {
    LOG_WARN("failed to initial ls restore dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_ls_restore_dag))) {
    LOG_WARN("failed to ad initial ls restore dag into dag net", K(ret));
  } else if (OB_FAIL(initial_ls_restore_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_ls_restore_dag))) {
    LOG_WARN("failed to add initial ls restore dag", K(ret), K(*initial_ls_restore_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_ls_restore_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_ls_restore_dag) && OB_NOT_NULL(scheduler)) {
    if (OB_SUCCESS != (tmp_ret = erase_dag_from_dag_net(*initial_ls_restore_dag))) {
      LOG_WARN("failed to erase dag from dag net", K(tmp_ret), KPC(initial_ls_restore_dag));
    }
    scheduler->free_dag(*initial_ls_restore_dag); // contain reset_children
    initial_ls_restore_dag = nullptr;
  }

  return ret;
}

bool ObLSRestoreDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSRestoreDagNet &other_ls_restore_dag = static_cast<const ObLSRestoreDagNet &>(other);
    if (OB_ISNULL(other_ls_restore_dag.ctx_) || OB_ISNULL(ctx_)) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ls restore ctx is NULL", KPC(ctx_), KPC(other_ls_restore_dag.ctx_));
    } else if (ctx_->arg_.ls_id_ != other_ls_restore_dag.ctx_->arg_.ls_id_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObLSRestoreDagNet::hash() const
{
  int64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ls restore ctx is NULL", KPC(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_->arg_.ls_id_, sizeof(ctx_->arg_.ls_id_), hash_value);
  }
  return hash_value;
}

int ObLSRestoreDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init ", K(ret));
  } else if (OB_UNLIKELY(0 > ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), K(*ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "ObLSRestoreDagNet: ls_id=%s, trace_id=%s",
          to_cstring(ctx_->arg_.ls_id_), task_id_str))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

int ObLSRestoreDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObLSRestoreDagNet: ls_id = %s", to_cstring(ctx_->arg_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

int ObLSRestoreDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start clear dag net ctx", KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init", K(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = report_result_())) {
      LOG_WARN("failed to report result", K(tmp_ret), K(ret), KPC(ctx_));
    }
    ctx_->finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_->finish_ts_ - ctx_->start_ts_;
    FLOG_INFO("finish ls restore dag net", "ls id", ctx_->arg_.ls_id_, K(cost_ts));
  }
  return ret;
}

int ObLSRestoreDagNet::report_result_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  share::ObTaskId failed_task_id;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObArray<ObTabletID> succeed_tablet_array;
  ObArray<ObTabletID> failed_tablet_array;


  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get ls restore ctx result", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_first_failed_task_id(failed_task_id))) {
    LOG_WARN("failed to get ls restore failed task id", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->get_ls_restore_handler()->handle_execute_over(
      OB_SUCCESS == result ? ctx_->task_id_ : failed_task_id, succeed_tablet_array, failed_tablet_array, ctx_->arg_.ls_id_, result))) {
    LOG_WARN("failed to handle execute over ls restore result", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObLSRestoreDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/******************ObLSRestoreDag*********************/
ObLSRestoreDag::ObLSRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObLSRestoreDag::~ObLSRestoreDag()
{
}

bool ObLSRestoreDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSRestoreDag &other_dag = static_cast<const ObLSRestoreDag&>(other);
    ObLSRestoreCtx *ctx = get_ctx();

    if (OB_ISNULL(ctx) || OB_ISNULL(other_dag.get_ctx())) {
      is_same = false;
      LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ls restore ctx should not be NULL", KP(ctx), KP(other_dag.get_ctx()));
    } else if (NULL != ctx && NULL != other_dag.get_ctx()) {
      if (ctx->arg_.ls_id_ != other_dag.get_ctx()->arg_.ls_id_) {
        is_same = false;
      }
    }
  }
  return is_same;
}

int64_t ObLSRestoreDag::hash() const
{
  int64_t hash_value = 0;
  ObLSRestoreCtx *ctx = get_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "ls restore ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObLSRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ctx->arg_.ls_id_.id(),
                                  static_cast<int64_t>(ctx->arg_.is_leader_),
                                  "dag_net_task_id", to_cstring(ctx->task_id_),
                                  "src", to_cstring(ctx->arg_.src_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret), KP(ctx));
  }
  return ret;
}

/******************ObInitialLSRestoreDag*********************/
ObInitialLSRestoreDag::ObInitialLSRestoreDag()
  : ObLSRestoreDag(ObDagType::DAG_TYPE_INITIAL_LS_RESTORE),
    is_inited_(false)
{
}

ObInitialLSRestoreDag::~ObInitialLSRestoreDag()
{
}

int ObInitialLSRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObInitialLSRestoreDag: ls_id = %s", to_cstring(ctx->arg_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObInitialLSRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = ls_restore_dag_net->get_ls_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInitialLSRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialLSRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial ls restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObLSRestoreInitTask*********************/
ObInitialLSRestoreTask::ObInitialLSRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    dag_net_(nullptr)
{
}

ObInitialLSRestoreTask::~ObInitialLSRestoreTask()
{
}

int ObInitialLSRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls restore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else {
    ctx_ = ls_restore_dag_net->get_ls_restore_ctx();
    bandwidth_throttle_ = ls_restore_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = ls_restore_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = ls_restore_dag_net->get_storage_rpc();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init initial ls restore task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObInitialLSRestoreTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore task do not init", K(ret));
  } else {
    DEBUG_SYNC(BEFORE_LS_RESTORE_SYS_TABLETS);
    if (OB_FAIL(generate_ls_restore_dags_())) {
      LOG_WARN("failed to generate ls restore dags", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObInitialLSRestoreTask::generate_ls_restore_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartLSRestoreDag *start_ls_restore_dag = nullptr;
  ObFinishLSRestoreDag *finish_ls_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialLSRestoreDag *initial_ls_restore_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls restore init task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_ls_restore_dag = static_cast<ObInitialLSRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial ls restore dag should not be NULL", K(ret), KP(initial_ls_restore_dag));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(start_ls_restore_dag))) {
      LOG_WARN("failed to alloc start ls restore dag ", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag(finish_ls_restore_dag))) {
      LOG_WARN("failed to alloc finish ls restore dag", K(ret));
    } else if (OB_FAIL(start_ls_restore_dag->init(dag_net_))) {
      LOG_WARN("failed to init start ls restore dag", K(ret));
    } else if (OB_FAIL(finish_ls_restore_dag->init(dag_net_))) {
      LOG_WARN("failed to init finish ls restore dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*start_ls_restore_dag))) {
      LOG_WARN("failed to add start ls restore dag as child", K(ret), KPC(start_ls_restore_dag));
    } else if (OB_FAIL(start_ls_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(start_ls_restore_dag->add_child(*finish_ls_restore_dag))) {
      LOG_WARN("failed to add finish ls retore dag as child", K(ret));
    } else if (OB_FAIL(finish_ls_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(finish_ls_restore_dag))) {
      LOG_WARN("failed to add ls restore finish dag", K(ret), K(*finish_ls_restore_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(scheduler->add_dag(start_ls_restore_dag))) {
      LOG_WARN("failed to add dag", K(ret), K(*start_ls_restore_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }

      if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(finish_ls_restore_dag))) {
        LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(start_ls_restore_dag));
      } else {
        finish_ls_restore_dag = nullptr;
      }
    } else {
      LOG_INFO("succeed to schedule ls restore start dag", K(*start_ls_restore_dag));
      start_ls_restore_dag = nullptr;
      finish_ls_restore_dag = nullptr;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_ls_restore_dag)) {
        scheduler->free_dag(*finish_ls_restore_dag);
        finish_ls_restore_dag = nullptr;
      }

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_ls_restore_dag)) {
        scheduler->free_dag(*start_ls_restore_dag);
        start_ls_restore_dag = nullptr;
      }
      const bool need_retry = true;
      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, need_retry, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set ls restore result", K(ret), K(tmp_ret), K(*ctx_));
      }
    }
  }
  return ret;
}

/******************ObStartLSRestoreDag*********************/
ObStartLSRestoreDag::ObStartLSRestoreDag()
  : ObLSRestoreDag(ObDagType::DAG_TYPE_START_LS_RESTORE),
    is_inited_(false)
{
}

ObStartLSRestoreDag::~ObStartLSRestoreDag()
{
}

int ObStartLSRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), K(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObStartLSRestoreDag: ls_id = %s", to_cstring(ctx->arg_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObStartLSRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start ls restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = ls_restore_dag_net->get_ls_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObStartLSRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartLSRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init ls restore start task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartLSRestoreTask*********************/
ObStartLSRestoreTask::ObStartLSRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr)
{
}

ObStartLSRestoreTask::~ObStartLSRestoreTask()
{
}

int ObStartLSRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start ls restore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else {
    ctx_ = ls_restore_dag_net->get_ls_restore_ctx();
    bandwidth_throttle_ = ls_restore_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = ls_restore_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = ls_restore_dag_net->get_storage_rpc();
    is_inited_ = true;
    LOG_INFO("succeed init start ls restore task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObStartLSRestoreTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls restore task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(deal_with_local_ls_())) {
    LOG_WARN("failed to deal with local ls", K(ret), KPC_(ctx));
  } else if (OB_FAIL(update_ls_meta_and_create_all_tablets_())) {
    LOG_WARN("failed to update ls meta and create all tablets", K(ret), KPC_(ctx));
  } else if (OB_FAIL(generate_tablets_restore_dag_())) {
    LOG_WARN("failed to generate tablets retore dag", K(ret), K(*ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC_(ctx));
    }
  }

  return ret;
}

int ObStartLSRestoreTask::deal_with_local_ls_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObRole role;
  int64_t leader_epoch = 0;
  ObLSMeta local_ls_meta;
  ObLSRestoreStatus restore_status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls restore task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ls should not be NULL", K(ret), K(*ctx_));
  } else {
    if (OB_FAIL(ls->get_restore_status(restore_status))) {
      LOG_WARN("failed to get restore status", K(ret), KPC(ctx_));
    } else if (ObLSRestoreStatus::RESTORE_SYS_TABLETS != restore_status) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls restore status is unexpected", K(ret), K(restore_status));
    }
  }
  return ret;
}

int ObStartLSRestoreTask::alloc_copy_ls_view_reader_(ObICopyLSViewInfoReader *&reader)
{
  int ret = OB_SUCCESS;
  reader = nullptr;
  void *buf = nullptr;

  if (ctx_->arg_.is_leader_) {
    ObCopyLSViewInfoRestoreReader *restore_reader = nullptr;
    ObIDagNet *dag_net = nullptr;
    ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

    if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
    } else if (OB_ISNULL(dag_net)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
    } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
    } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
    } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObCopyLSViewInfoRestoreReader), "CpLSViewRestore"))) {
    } else if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (FALSE_IT(restore_reader = new (buf) ObCopyLSViewInfoRestoreReader())) {
    } else if (OB_FAIL(restore_reader->init(ctx_->arg_.ls_id_, ctx_->arg_.restore_base_info_, ls_restore_dag_net->get_meta_index_store()))) {
      LOG_WARN("failed to init tablet restore reader", K(ret), KPC(ctx_));
    } else {
      reader = restore_reader;
      restore_reader = nullptr;
    }

    if (OB_NOT_NULL(restore_reader)) {
      restore_reader->~ObCopyLSViewInfoRestoreReader();
      mtl_free(restore_reader);
      restore_reader = nullptr;
    }
  } else {
    ObCopyLSViewInfoObReader *ob_reader = nullptr;

    ObStorageHASrcInfo src_info;
    src_info.src_addr_ = ctx_->arg_.src_.get_server();
    src_info.cluster_id_ = GCONF.cluster_id;

    obrpc::ObCopyLSViewArg arg;
    arg.tenant_id_ = ctx_->arg_.tenant_id_;
    arg.ls_id_ = ctx_->arg_.ls_id_;
    if (FALSE_IT(buf = mtl_malloc(sizeof(ObCopyLSViewInfoObReader), "CpLSViewRead"))) {
    } else if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else if (FALSE_IT(ob_reader = new (buf) ObCopyLSViewInfoObReader())) {
    } else if (OB_FAIL(ob_reader->init(src_info, arg, *svr_rpc_proxy_, *bandwidth_throttle_))) {
      LOG_WARN("failed to init tablet restore reader", K(ret), K(src_info), K(arg));
    } else {
      reader = ob_reader;
      ob_reader = nullptr;
      ctx_->src_ = src_info;
    }

    if (OB_NOT_NULL(ob_reader)) {
      ob_reader->~ObCopyLSViewInfoObReader();
      mtl_free(ob_reader);
      ob_reader = nullptr;
    }
  }
  return ret;
}

void ObStartLSRestoreTask::free_copy_ls_view_reader_(ObICopyLSViewInfoReader *&reader)
{
  if (OB_NOT_NULL(reader)) {
    reader->~ObICopyLSViewInfoReader();
    ob_free(reader);
    reader = nullptr;
  }
}

int ObStartLSRestoreTask::create_tablet_(
    const ObMigrationTabletParam &tablet_meta,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTablesHandleArray remote_table;
  ObBatchUpdateTableStoreParam param;

  if (!tablet_meta.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create tablet get invalid argument", K(ret), K(tablet_meta), KP(ls));
  } else if (OB_FAIL(ObTabletCreateMdsHelper::check_create_new_tablets(1LL, ObTabletCreateThrottlingLevel::SOFT))) {
    LOG_WARN("failed to check create new tablet", K(ret), K(tablet_meta));
  } else if (OB_FAIL(ls->rebuild_create_tablet(tablet_meta, false /*keep old*/))) {
    LOG_WARN("failed to create tablet", K(ret), K(tablet_meta));
  } else {
    LOG_INFO("succeed to create tablet and table store", KPC(ls), K(tablet_meta), K(remote_table));
  }

  return ret;
}

void ObStartLSRestoreTask::set_tablet_to_restore(ObMigrationTabletParam &tablet_meta)
{
  tablet_meta.ha_status_.set_restore_status(ObTabletRestoreStatus::PENDING);
}

int ObStartLSRestoreTask::update_ls_meta_and_create_all_tablets_()
{
  int ret = OB_SUCCESS;
  ObICopyLSViewInfoReader *reader = nullptr;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ctx_->arg_.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get ls handle", K(ret), "ls_id", ctx_->arg_.ls_id_);
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_handle));
  } else if (OB_FAIL(alloc_copy_ls_view_reader_(reader))) {
    LOG_WARN("failed to alloc copy ls view reader", K(ret));
  } else {
    ObMigrationStatus migration_status;
    ObLSRestoreStatus restore_status;
    obrpc::ObCopyTabletInfo tablet_info;
    HEAP_VAR(ObLSMetaPackage, ls_meta_package) {
      if (OB_FAIL(reader->get_ls_meta(ls_meta_package))) {
        LOG_WARN("fail to read ls meta infos", K(ret));
      } else if (OB_FAIL(ls_meta_package.ls_meta_.get_migration_status(migration_status))) {
        LOG_WARN("failed to get migration status", K(ret), K(ls_meta_package));
      } else if (OB_FAIL(ls_meta_package.ls_meta_.get_restore_status(restore_status))) {
        LOG_WARN("failed to get restore status", K(ret), K(ls_meta_package));
      } else if (ctx_->arg_.is_leader_
                 && (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status
                    || ObLSRestoreStatus::NONE != restore_status)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader ls restore get unexpected migration status or restore status", K(ret), K(migration_status),
            K(restore_status), K(ls_meta_package));
      } else if (!ctx_->arg_.is_leader_
                 && !restore_status.is_wait_restore_sys_tablets()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("follower ls restore get unexpected restore status", K(ret), K(restore_status), K(ls_meta_package));
      } else if (OB_FAIL(ls->update_ls_meta(false/*don't update restore status*/,
                                            ls_meta_package.ls_meta_))) {
        LOG_WARN("fail to update ls meta", K(ret), KPC(ls), K(ls_meta_package));
      } else {
        LOG_INFO("update ls meta succeed", KPC(ls), K(ls_meta_package));
        ctx_->src_ls_meta_package_ = ls_meta_package;
        ctx_->need_check_seq_ = ctx_->arg_.is_leader_ ? false : true;
        ctx_->ls_rebuild_seq_ = ctx_->arg_.is_leader_ ? -1 : ls_meta_package.ls_meta_.get_rebuild_seq();
        ctx_->sys_tablet_id_array_.reset();
        int64_t tablet_cnt = 0;
        // create all tablets on the log stream
        while (OB_SUCC(ret)) {
          tablet_info.reset();
          if (OB_FAIL(reader->get_next_tablet_info(tablet_info))) {
            if (OB_ITER_END == ret) {
              LOG_INFO("update ls meta and create all tablets succeed", KPC_(ctx), K(tablet_cnt));
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next tablet meta", K(ret));
            }
          } else if (tablet_info.param_.tablet_id_.is_ls_inner_tablet()
                     && OB_FAIL(ctx_->sys_tablet_id_array_.push_back(tablet_info.param_.tablet_id_))) {
            LOG_WARN("failed to push sys tablet id into array", K(ret), "array count", ctx_->sys_tablet_id_array_.count());
          } else if (!tablet_info.param_.is_empty_shell() && OB_FALSE_IT(set_tablet_to_restore(tablet_info.param_))) {
          } else if (OB_FAIL(reset_multi_version_start_(tablet_info.param_))) {
            LOG_WARN("failed to reset multi version start", K(ret), K(tablet_info));
          } else if (OB_FAIL(create_tablet_(tablet_info.param_, ls))) {
            LOG_WARN("failed to create tablet", K(ret));
          } else {
            ++tablet_cnt;
          }
        }
      }
    }

    free_copy_ls_view_reader_(reader);
  }
  return ret;
}

int ObStartLSRestoreTask::generate_tablet_id_array_(
    const ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls restore task do not init", K(ret));
  } else if (tablet_id_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate tablet id array get invalid argument", K(ret), K(tablet_id_array));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
      const ObTabletID &tablet_id = tablet_id_array.at(i);
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
  }
  return ret;
}

int ObStartLSRestoreTask::update_ls_meta_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    LOG_WARN("start ls restore task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore ls should not be NULL", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->update_ls_meta(false/*don't update restore status*/,
                                        ctx_->src_ls_meta_package_.ls_meta_))) {
    LOG_WARN("fail to update ls meta", K(ret), KPC(ls), KPC(ctx_));
  } else if (OB_FAIL(ls->set_dup_table_ls_meta(ctx_->src_ls_meta_package_.dup_ls_meta_,
                                               true /*need_flush_slog*/))) {
    LOG_WARN("fail to set dup table ls meta", K(ret), KPC(ctx_));
  } else {
    LOG_INFO("update ls meta succeed", KPC(ls), KPC(ctx_));
  }
  return ret;
}

int ObStartLSRestoreTask::generate_tablets_restore_dag_()
{
  //TODO(muwei.y) It is same with other generate dag, can it be using same function in 4.3
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSysTabletsRestoreDag *sys_tablets_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObStartLSRestoreDag *start_ls_restore_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start ls restore task do not init", K(ret));
  } else if (OB_ISNULL(start_ls_restore_dag = static_cast<ObStartLSRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start ls restore dag should not be NULL", K(ret), KP(start_ls_restore_dag));
  } else if (OB_ISNULL(dag_net = this->get_dag()->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(sys_tablets_restore_dag))) {
      LOG_WARN("failed to alloc sys tablets restore dag ", K(ret));
    } else if (OB_FAIL(sys_tablets_restore_dag->init(dag_net))) {
      LOG_WARN("failed to init sys tablets restore dag", K(ret), K(*ctx_));
    } else if (OB_FAIL(this->get_dag()->add_child(*sys_tablets_restore_dag))) {
      LOG_WARN("failed to add sys tablets restore dag as chilid", K(ret), K(*ctx_));
    } else if (OB_FAIL(sys_tablets_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(sys_tablets_restore_dag))) {
      LOG_WARN("failed to add sys tablets restore dag", K(ret), K(*sys_tablets_restore_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("succeed to schedule sys tablets restore dag", K(*sys_tablets_restore_dag));
      sys_tablets_restore_dag = nullptr;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(sys_tablets_restore_dag)) {
        scheduler->free_dag(*sys_tablets_restore_dag);
        sys_tablets_restore_dag = nullptr;
      }
    }
  }
  return ret;
}

int ObStartLSRestoreTask::reset_multi_version_start_(ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is not valid", K(ret), K(param));
  } else {
    // We reset the multi version start of the tablet migration parameter that is backup in backup consistent SCN stage.
    // This stage does not have any SSTables and only serves as a placeholder.
    // So the multi version start can be reset to 0 and then pushed up in the restore minor stage and restore major stage.
    param.multi_version_start_ = 0;
  }
  return ret;
}

/******************ObSysTabletsRestoreDag*********************/
ObSysTabletsRestoreDag::ObSysTabletsRestoreDag()
  : ObLSRestoreDag(ObDagType::DAG_TYPE_SYS_TABLETS_RESTORE),
    is_inited_(false)
{
}

ObSysTabletsRestoreDag::~ObSysTabletsRestoreDag()
{
}

int ObSysTabletsRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("log stream restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be null", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObSysTabletsRestoreDag: ls_id = %s", to_cstring(ctx->arg_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObSysTabletsRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sys tablets restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = ls_restore_dag_net->get_ls_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSysTabletsRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSysTabletsRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init sys tablets restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObSysTabletsRestoreTask*********************/
ObSysTabletsRestoreTask::ObSysTabletsRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ls_handle_(),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    ha_tablets_builder_()

{
}

ObSysTabletsRestoreTask::~ObSysTabletsRestoreTask()
{
}

int ObSysTabletsRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("sys tablets restore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else {
    ctx_ = ls_restore_dag_net->get_ls_restore_ctx();
    bandwidth_throttle_ = ls_restore_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = ls_restore_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = ls_restore_dag_net->get_storage_rpc();
    meta_index_store_ = ls_restore_dag_net->get_meta_index_store();
    second_meta_index_store_ = ls_restore_dag_net->get_second_meta_index_store();
    const ObTabletRestoreAction::ACTION &restore_action = ObTabletRestoreAction::ACTION::RESTORE_ALL;
    if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle_))) {
      LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ObTabletGroupRestoreUtils::init_ha_tablets_builder(
        ctx_->arg_.tenant_id_, ctx_->sys_tablet_id_array_, ctx_->arg_.is_leader_,
        ctx_->need_check_seq_, ctx_->ls_rebuild_seq_, ctx_->src_,
        ls_handle_.get_ls(), &ctx_->arg_.restore_base_info_, restore_action,
        meta_index_store_, &ctx_->ha_table_info_mgr_,
        ha_tablets_builder_))) {
      LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed init sys tablets restore task", "ls id", ctx_->arg_.ls_id_,
          "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
    }
  }
  return ret;
}

int ObSysTabletsRestoreTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets restore task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(create_or_update_tablets_())) {
    LOG_WARN("failed to create or update tablets", K(ret), K(*ctx_));
  } else if (OB_FAIL(build_tablets_sstable_info_())) {
    LOG_WARN("failed to build tablets sstable info", K(ret), K(*ctx_));
  } else if (OB_FAIL(generate_sys_tablet_restore_dag_())) {
    LOG_WARN("failed to generate sys tablet restore dag", K(ret), K(*ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }
  return ret;
}

//TODO(zeyong) check need to create or update anyway
int ObSysTabletsRestoreTask::create_or_update_tablets_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets restore task do not init", K(ret));
  } else if (OB_FAIL(ha_tablets_builder_.create_or_update_tablets())) {
    LOG_WARN("failed to create or update tablets", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObSysTabletsRestoreTask::build_tablets_sstable_info_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets restore task do not init", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ha_tablets_builder_.build_tablets_sstable_info())) {
    LOG_WARN("failed to build tablets sstable info", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObSysTabletsRestoreTask::generate_sys_tablet_restore_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObArray<ObIDag *> tablet_restore_dag_array;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  const ObTabletRestoreAction::ACTION action = ObTabletRestoreAction::RESTORE_ALL;
  ObSysTabletsRestoreDag *sys_tablets_restore_dag = nullptr;
  ObIDag *parent = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("sys tablets restore task do not init", K(ret));
  } else if (OB_ISNULL(sys_tablets_restore_dag = static_cast<ObSysTabletsRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tablets restore dag should not be NULL", K(ret), KP(sys_tablets_restore_dag));
  } else if (OB_ISNULL(dag_net = sys_tablets_restore_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FALSE_IT(parent = this->get_dag())) {
  } else if (OB_FAIL(tablet_restore_dag_array.push_back(parent))) {
    LOG_WARN("failed to push sys_tablets_restore_dag into array", K(ret), K(*ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->sys_tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->sys_tablet_id_array_.at(i);
      ObTabletRestoreDag *tablet_restore_dag = nullptr;
      ObInitTabletRestoreParam param;
      param.tenant_id_ = ctx_->arg_.tenant_id_;
      param.ls_id_ = ctx_->arg_.ls_id_;
      param.tablet_id_ = tablet_id;
      param.ha_dag_net_ctx_ = ctx_;
      param.is_leader_ = ctx_->arg_.is_leader_;
      param.action_ = action;
      param.restore_base_info_ = &ctx_->arg_.restore_base_info_;
      param.ha_table_info_mgr_ = &ctx_->ha_table_info_mgr_;
      param.meta_index_store_ = meta_index_store_;
      param.second_meta_index_store_ = second_meta_index_store_;

      if (!param.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("init tablet restore param is invalid", K(ret), K(param), KPC(ctx_));
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_restore_dag))) {
        LOG_WARN("failed to alloc tablet restore dag", K(ret));
      } else if (OB_FAIL(tablet_restore_dag_array.push_back(tablet_restore_dag))) {
        LOG_WARN("failed to push tablet restore dag into array", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_restore_dag->init(param))) {
        LOG_WARN("failed to init tablet restore dag", K(ret), K(*ctx_), K(param));
      } else if (OB_FAIL(parent->add_child(*tablet_restore_dag))) {
        LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_restore_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), K(*ctx_));
      } else if (OB_FAIL(scheduler->add_dag(tablet_restore_dag))) {
        LOG_WARN("failed to add tablet restore dag", K(ret), K(*tablet_restore_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule tablet restore dag", KPC(tablet_restore_dag));
        parent = tablet_restore_dag;
        tablet_restore_dag = nullptr;
      }

      if (OB_FAIL(ret) && OB_NOT_NULL(tablet_restore_dag)) {
        // tablet_restore_dag_array is not empty.
        ObIDag *last = tablet_restore_dag_array.at(tablet_restore_dag_array.count() - 1);
        if (last == tablet_restore_dag) {
          tablet_restore_dag_array.pop_back();
        }

        scheduler->free_dag(*tablet_restore_dag);
        tablet_restore_dag = nullptr;
      }
    }

    // Cancel all dags from back to front, except the first dag which is 'sys_tablets_restore_dag'.
    if (OB_FAIL(ret)) {
      // The i-th dag is the parent dag of (i+1)-th dag.
      for (int64_t child_idx = tablet_restore_dag_array.count() - 1; child_idx > 0; child_idx--) {
        if (OB_TMP_FAIL(scheduler->cancel_dag(tablet_restore_dag_array.at(child_idx)))) {
          LOG_WARN("failed to cancel inner tablet restore dag", K(tmp_ret), K(child_idx));
        }
      }
      tablet_restore_dag_array.reset();
    }
  }
  return ret;
}

/******************ObDataTabletsMetaRestoreDag*********************/
ObDataTabletsMetaRestoreDag::ObDataTabletsMetaRestoreDag()
  : ObLSRestoreDag(ObDagType::DAG_TYPE_DATA_TABLETS_META_RESTORE),
    is_inited_(false)
{
}

ObDataTabletsMetaRestoreDag::~ObDataTabletsMetaRestoreDag()
{
}

int ObDataTabletsMetaRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObDataTabletsMetaRestoreDag: ls_id = %s", to_cstring(ctx->arg_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObDataTabletsMetaRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data tablets meta restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = ls_restore_dag_net->get_ls_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDataTabletsMetaRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObDataTabletsMetaRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init data tablets meta restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObDataTabletsMetaRestoreTask*********************/
ObDataTabletsMetaRestoreTask::ObDataTabletsMetaRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    finish_dag_(nullptr)
{
}

ObDataTabletsMetaRestoreTask::~ObDataTabletsMetaRestoreTask()
{
}

int ObDataTabletsMetaRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data tablets meta restore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else {
    const common::ObIArray<ObINodeWithChild*> &child_node_array = this->get_dag()->get_child_nodes();
    if (child_node_array.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data tablets meta restore dag get unexpected child node", K(ret), K(child_node_array));
    } else {
      ObLSRestoreDag *child_dag = static_cast<ObLSRestoreDag*>(child_node_array.at(0));
      if (ObDagType::DAG_TYPE_FINISH_LS_RESTORE != child_dag->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls restore dag type is unexpected", K(ret), K(*child_dag));
      } else {
        ctx_ = ls_restore_dag_net->get_ls_restore_ctx();
        bandwidth_throttle_ = ls_restore_dag_net->get_bandwidth_throttle();
        svr_rpc_proxy_ = ls_restore_dag_net->get_storage_rpc_proxy();
        storage_rpc_ = ls_restore_dag_net->get_storage_rpc();
        finish_dag_ = static_cast<ObIDag*>(child_dag);
        is_inited_ = true;

        LOG_INFO("succeed init data tablets restore task", "ls id", ctx_->arg_.ls_id_,
            "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
      }
    }
  }
  return ret;
}

int ObDataTabletsMetaRestoreTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do data tablets meta restore task", K(ret), KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(build_tablet_group_info_())) {
    LOG_WARN("failed to build tablet group info", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_tablet_group_dag_())) {
    LOG_WARN("failed to generate tablet group dag", K(ret), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(*ctx_));
    }
  }

  return ret;
}

int ObDataTabletsMetaRestoreTask::build_tablet_group_info_()
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_group_id_array;
  const int64_t MAX_TABLET_GROUP_NUM = 128;
  int64_t index = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore task do not init", K(ret));
  } else {
    ctx_->tablet_group_mgr_.reuse();
    while (OB_SUCC(ret) && index < ctx_->data_tablet_id_array_.count()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_TABLET_GROUP_NUM
          && index < ctx_->data_tablet_id_array_.count(); ++i, index++) {
        const ObTabletID &tablet_id = ctx_->data_tablet_id_array_.at(index);
        if (OB_FAIL(tablet_group_id_array.push_back(tablet_id))) {
          LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ctx_->tablet_group_mgr_.build_tablet_group_ctx(tablet_group_id_array))) {
          LOG_WARN("failed to build tablet group ctx", K(ret), K(tablet_group_id_array));
        } else {
          tablet_group_id_array.reset();
          LOG_INFO("succeed generate tablet group id array", K(tablet_group_id_array));
        }
      }
    }
  }
  return ret;
}

int ObDataTabletsMetaRestoreTask::generate_tablet_group_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletGroupMetaRestoreDag *tablet_group_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObDataTabletsMetaRestoreDag *data_tablets_meta_restore_dag = nullptr;
  ObHATabletGroupCtx *tablet_group_ctx = nullptr;
  ObArray<ObTabletID> tablet_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore task do not init", K(ret));
  } else if (OB_FAIL(ctx_->tablet_group_mgr_.get_next_tablet_group_ctx(tablet_group_ctx))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next tablet group ctx", K(ret), KPC(ctx_));
    }
  } else if (OB_FAIL(tablet_group_ctx->get_all_tablet_ids(tablet_id_array))) {
    LOG_WARN("failed to get all tablet ids", K(ret), KPC(ctx_));
  } else if (tablet_id_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate tablet group dag get invalid argument", K(ret), K(tablet_id_array));
  } else if (OB_ISNULL(data_tablets_meta_restore_dag = static_cast<ObDataTabletsMetaRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data tablets meta restore dag should not be NULL", K(ret), KP(data_tablets_meta_restore_dag));
  } else if (OB_ISNULL(dag_net = data_tablets_meta_restore_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), K(*this));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(tablet_group_dag))) {
      LOG_WARN("failed to alloc tablet group meta restore dag ", K(ret));
    } else if (OB_FAIL(tablet_group_dag->init(tablet_id_array, dag_net, finish_dag_))) {
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
      LOG_WARN("failed to add tablet group meta dag", K(ret), K(*tablet_group_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(tablet_group_dag)) {
        scheduler->free_dag(*tablet_group_dag);
        tablet_group_dag = nullptr;
      }
    }
  }

  return ret;
}

/******************ObTabletGroupMetaRestoreDag*********************/
ObTabletGroupMetaRestoreDag::ObTabletGroupMetaRestoreDag()
  : ObLSRestoreDag(ObDagType::DAG_TYPE_TABLET_GROUP_META_RESTORE),
    is_inited_(false),
    tablet_id_array_(),
    finish_dag_(nullptr)
{
}

ObTabletGroupMetaRestoreDag::~ObTabletGroupMetaRestoreDag()
{
}

bool ObTabletGroupMetaRestoreDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else {
    is_same = false;
  }
  return is_same;
}

int64_t ObTabletGroupMetaRestoreDag::hash() const
{
  int64_t hash_value = 0;
  ObLSRestoreCtx *ctx = get_ctx();

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

int ObTabletGroupMetaRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group meta restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be null", K(ret), KP(ctx));
  } else if (tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id array should not be empty", K(ret), KPC(ctx), K(tablet_id_array_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletGroupMetaRestoreDag: ls_id = %s, first_tablet_id = %s", to_cstring(ctx->arg_.ls_id_),
       to_cstring(tablet_id_array_.at(0))))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObTabletGroupMetaRestoreDag::init(
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    share::ObIDagNet *dag_net,
    share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group meta restore dag init twice", K(ret));
  } else if (tablet_id_array.empty() || OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet group meta restore init get invalid argument", K(ret), KP(dag_net), KP(finish_dag));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = ls_restore_dag_net->get_ls_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    finish_dag_ = finish_dag;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletGroupMetaRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletGroupMetaRestoreTask *task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group meta restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(tablet_id_array_, finish_dag_))) {
    LOG_WARN("failed to tablet group meta restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletGroupMetaRestoreDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupMetaRestoreDag *tablet_group_meta_restore_dag = nullptr;
  bool need_set_failed_result = true;
  ObLSRestoreCtx *ctx = nullptr;
  ObHATabletGroupCtx *tablet_group_ctx = nullptr;
  ObArray<ObTabletID> tablet_id_array;
  ObDagId dag_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group meta restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ctx));
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
    LOG_WARN("ls prepare migration dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_group_meta_restore_dag))) {
    LOG_WARN("failed to alloc tablet group meta restore dag ", K(ret));
  } else if (OB_FAIL(tablet_group_meta_restore_dag->init(tablet_id_array, dag_net, finish_dag_))) {
    LOG_WARN("failed to init tablet migration dag", K(ret), KPC(ctx));
  } else if (OB_FAIL(tablet_group_meta_restore_dag->set_dag_id(dag_id))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(ctx));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_group_meta_restore_dag));
    dag = tablet_group_meta_restore_dag;
    tablet_group_meta_restore_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_group_meta_restore_dag)) {
    scheduler->free_dag(*tablet_group_meta_restore_dag);
    tablet_group_meta_restore_dag = nullptr;
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }
  return ret;
}

int ObTabletGroupMetaRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group meta restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ctx->arg_.ls_id_.id(),
                                  static_cast<int64_t>(tablet_id_array_.at(0).id()),
                                  static_cast<int64_t>(ctx->arg_.is_leader_),
                                  "dag_net_task_id", to_cstring(ctx->task_id_),
                                  "src", to_cstring(ctx->arg_.src_.get_server())))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}
/******************ObTabletGroupMetaRestoreTask*********************/
ObTabletGroupMetaRestoreTask::ObTabletGroupMetaRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    tablet_id_array_(),
    finish_dag_(nullptr)
{
}

ObTabletGroupMetaRestoreTask::~ObTabletGroupMetaRestoreTask()
{
}

int ObTabletGroupMetaRestoreTask::init(
    const ObIArray<ObTabletID> &tablet_id_array,
    share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group meta restore task init twice", K(ret));
  } else if (tablet_id_array.empty() || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet group meta restore task init get invalid argument", K(ret), K(tablet_id_array), KP(finish_dag));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else {
    ctx_ = ls_restore_dag_net->get_ls_restore_ctx();
    finish_dag_ = finish_dag;
    is_inited_ = true;
    LOG_INFO("succeed init tablet group restore task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_, K(tablet_id_array));
  }
  return ret;
}

int ObTabletGroupMetaRestoreTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do tablet group meta restore task", K(ret), K(tablet_id_array_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group meta restore task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(create_or_update_tablets_())) {
    LOG_WARN("failed to create tablets sstable", K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObTabletGroupMetaRestoreTask::create_or_update_tablets_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore task do not init", K(ret));
  } else if (tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("tablet id array should not be Empty", KPC(ctx_), K(tablet_id_array_));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ctx_->arg_.ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), KP(ls));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = tablet_id_array_.at(i);
      if (OB_FAIL(create_or_update_tablet_(tablet_id, ls))) {
        LOG_WARN("failed to create or update tablet", K(ret), K(tablet_id), KPC(ctx_));
      }
    }
  }
  return ret;
}

int ObTabletGroupMetaRestoreTask::create_or_update_tablet_(
    const common::ObTabletID &tablet_id,
    storage::ObLS *ls)
{
  int ret = OB_SUCCESS;
  const bool is_transfer = false;
  const ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::PENDING;
  const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("data tablets meta restore task do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create or update tablet get invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(share::ObCompatModeGetter::get_tablet_compat_mode(ctx_->arg_.tenant_id_, tablet_id, compat_mode))) {
    LOG_WARN("failed to get tenant mode", KR(ret),"tenant_id", ctx_->arg_.tenant_id_, K(tablet_id));
  } else {
    lib::CompatModeGuard g(compat_mode);
    ObMigrationTabletParam param;
    param.ls_id_ = ctx_->arg_.ls_id_;
    param.tablet_id_ = tablet_id;
    param.data_tablet_id_ = tablet_id;
    param.create_scn_ = ObTabletMeta::INIT_CREATE_SCN;
    param.clog_checkpoint_scn_.reset();
    // Compat mode of sys tables is MYSQL no matter what if the tenant is ORACLE mode.
    param.compat_mode_ = compat_mode;
    param.multi_version_start_ = 0;
    param.snapshot_version_ = 0;

    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = ObTabletStatus::NORMAL;

    const int64_t length = user_data.get_serialize_size();
    char *buffer = static_cast<char*>(param.allocator_.alloc(length));
    int64_t pos = 0;
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), K(length));
    } else if (OB_FAIL(user_data.serialize(buffer, length, pos))) {
      LOG_WARN("failed to serialize", K(ret));
    } else {
      mds::MdsDumpNode &node = param.mds_data_.tablet_status_committed_kv_.v_;
      node.allocator_ = &param.allocator_;
      node.user_data_.assign(buffer, length);
    }

    if (OB_FAIL(ret)) {
      if (nullptr != buffer) {
        param.allocator_.free(buffer);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.transfer_info_.init())) {
      LOG_WARN("failed to init transfer info", K(ret), K(param));
    } else if (OB_FAIL(param.ha_status_.set_restore_status(restore_status))) {
      LOG_WARN("failed to set restore status", K(ret), K(restore_status));
    } else if (OB_FAIL(param.ha_status_.set_data_status(data_status))) {
      LOG_WARN("failed to set data status", K(ret), K(data_status));
    } else if (OB_FAIL(ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(
        param.allocator_,
        param.storage_schema_,
        param.medium_info_list_,
        param.mds_data_))) {
      LOG_WARN("failed to construct placeholder storage schema");
    } else if (!param.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("create tablet param is invalid", K(ret), K(param));
    } else if (OB_FAIL(ls->create_or_update_migration_tablet(param, is_transfer))) {
      LOG_WARN("failed to create or update tablet", K(ret), K(param));
    }
  }

  return ret;
}

/******************ObFinishLSRestoreDag*********************/
ObFinishLSRestoreDag::ObFinishLSRestoreDag()
  : ObLSRestoreDag(ObDagType::DAG_TYPE_FINISH_LS_RESTORE),
    is_inited_(false)
{
}

ObFinishLSRestoreDag::~ObFinishLSRestoreDag()
{
}

int ObFinishLSRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObLSRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish ls restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObFinishLSRestoreDag: ls_id = %s", to_cstring(ctx->arg_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObFinishLSRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish ls restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = ls_restore_dag_net->get_ls_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFinishLSRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishLSRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish ls restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init log stream restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishLSRestoreTask*********************/
ObFinishLSRestoreTask::ObFinishLSRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObFinishLSRestoreTask::~ObFinishLSRestoreTask()
{
}

int ObFinishLSRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObLSRestoreDagNet *ls_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish ls rstore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(ls_restore_dag_net = static_cast<ObLSRestoreDagNet*>(dag_net))) {
  } else {
    ctx_ = ls_restore_dag_net->get_ls_restore_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init tablet group restore task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_);
  }
  return ret;
}

int ObFinishLSRestoreTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("start do finish ls restore task");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish ls restore task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    bool allow_retry = false;
    if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
      LOG_ERROR("failed to check allow retry", K(ret), K(*ctx_));
    } else if (allow_retry) {
      ctx_->reuse();
      if (OB_FAIL(generate_initial_ls_restore_dag_())) {
        LOG_WARN("failed to generate initial ls restore dag", K(ret), KPC(ctx_));
      }
    }
  }
  return ret;
}

int ObFinishLSRestoreTask::generate_initial_ls_restore_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialLSRestoreDag *initial_ls_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObFinishLSRestoreDag *finish_ls_restore_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish ls restore task do not init", K(ret));
  } else if (OB_ISNULL(finish_ls_restore_dag = static_cast<ObFinishLSRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fiish ls restore dag should not be NULL", K(ret), KP(finish_ls_restore_dag));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(initial_ls_restore_dag))) {
      LOG_WARN("failed to alloc ls restore dag", K(ret));
    } else if (OB_FAIL(initial_ls_restore_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial ls restore dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_ls_restore_dag))) {
      LOG_WARN("failed to add initial ls restore dag as child", K(ret), KPC(initial_ls_restore_dag));
    } else if (OB_FAIL(initial_ls_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_ls_restore_dag))) {
      LOG_WARN("failed to add initial ls restore dag", K(ret), K(*initial_ls_restore_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create initial ls restore dag", K(ret), K(*ctx_));
      initial_ls_restore_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_ls_restore_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_ls_restore_dag);
      initial_ls_restore_dag = nullptr;
    }
  }

  return ret;
}


}
}

