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
#include "ob_tablet_group_restore.h"
#include "observer/ob_server.h"
#include "ob_physical_copy_task.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_ls_restore.h"
#include "storage/backup/ob_backup_data_store.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

static int update_deleted_and_undefine_tablet(ObLS &ls, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObTabletExpectedStatus::STATUS expected_status = ObTabletExpectedStatus::DELETED;
  const ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::UNDEFINED;
  if (OB_FAIL(ls.get_tablet_svr()->update_tablet_ha_expected_status(tablet_id, expected_status))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("restore tablet maybe deleted, skip update expected status to DELETED", K(ret), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to update expected status to DELETED", K(ret), K(expected_status), K(tablet_id));
    }
  } else if (OB_FAIL(ls.update_tablet_restore_status(tablet_id, restore_status, true/* need reset transfer flag */))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_INFO("restore tablet maybe deleted, skip update restore status to UNDEFINED", K(ret), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to update restore status to UNDEFINED", K(ret), K(restore_status), K(tablet_id));
    }
  } else {
    LOG_INFO("remote tablet is not exist, update expected status to DELETED, and restore status to UNDEFINED", K(tablet_id));
  }
  return ret;
}

/******************ObTabletGroupRestoreCtx*********************/
ObTabletGroupRestoreCtx::ObTabletGroupRestoreCtx()
  : ObIHADagNetCtx(),
    arg_(),
    start_ts_(0),
    finish_ts_(0),
    task_id_(),
    src_(),
    ha_table_info_mgr_(),
    tablet_id_array_(),
    tablet_group_ctx_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1)
{
}

ObTabletGroupRestoreCtx::~ObTabletGroupRestoreCtx()
{
}

bool ObTabletGroupRestoreCtx::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
        && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);
}

void ObTabletGroupRestoreCtx::reset()
{
  arg_.reset();
  start_ts_ = 0;
  finish_ts_ = 0;
  task_id_.reset();
  src_.reset();
  ha_table_info_mgr_.reuse();
  tablet_id_array_.reset();
  tablet_group_ctx_.reuse();
  ObIHADagNetCtx::reset();
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

int ObTabletGroupRestoreCtx::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (!is_valid()) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore ctx do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (arg_.tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id array should not be empty", K(ret), K(arg_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "tablet group restore : task_id = %s, ls_id = %s, "
      "first_tablet_id = %s, src = %s, dest = %s", to_cstring(task_id_), to_cstring(arg_.ls_id_),
      to_cstring(arg_.tablet_id_array_.at(0)), to_cstring(arg_.src_.get_server()), to_cstring(arg_.dst_.get_server())))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

void ObTabletGroupRestoreCtx::reuse()
{
  ObIHADagNetCtx::reuse();
  ha_table_info_mgr_.reuse();
  src_.reset();
  tablet_id_array_.reset();
  tablet_group_ctx_.reuse();
}

/******************ObTabletRestoreCtx*********************/
ObTabletRestoreCtx::ObTabletRestoreCtx()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    tablet_handle_(),
    restore_base_info_(nullptr),
    is_leader_(false),
    action_(ObTabletRestoreAction::MAX),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    replica_type_(ObReplicaType::REPLICA_TYPE_MAX),
    ha_table_info_mgr_(nullptr),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    lock_(common::ObLatchIds::RESTORE_LOCK),
    status_(ObCopyTabletStatus::MAX_STATUS)
{
}

ObTabletRestoreCtx::~ObTabletRestoreCtx()
{
}

bool ObTabletRestoreCtx::is_valid() const
{
  return tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_id_.is_valid()
      && ObCopyTabletStatus::is_valid(status_)
      && ((ObCopyTabletStatus::TABLET_EXIST == status_ && tablet_handle_.is_valid())
          || ObCopyTabletStatus::TABLET_NOT_EXIST == status_)
      && OB_NOT_NULL(restore_base_info_)
      && ObTabletRestoreAction::is_valid(action_)
      && (!is_leader_ || (OB_NOT_NULL(meta_index_store_) && OB_NOT_NULL(second_meta_index_store_)))
      && ObReplicaTypeCheck::is_replica_type_valid(replica_type_)
      && OB_NOT_NULL(ha_table_info_mgr_)
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);
}

void ObTabletRestoreCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  tablet_handle_.reset();
  restore_base_info_ = nullptr;
  is_leader_ = false;
  action_ = ObTabletRestoreAction::MAX;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  replica_type_ = ObReplicaType::REPLICA_TYPE_MAX;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  status_ = ObCopyTabletStatus::MAX_STATUS;
  ha_table_info_mgr_ = nullptr;
}

int ObTabletRestoreCtx::set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status)
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

int ObTabletRestoreCtx::get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const
{
  int ret = OB_SUCCESS;
  status = ObCopyTabletStatus::MAX_STATUS;
  common::SpinRLockGuard guard(lock_);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet restore ctx is invalid", K(ret), KPC(this));
  } else {
    status = status_;
  }
  return ret;
}

/******************ObTabletGroupRestoreDagNet*********************/
ObTGRDagNetInitParam::ObTGRDagNetInitParam()
  : arg_(),
    task_id_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr)
{
}

bool ObTGRDagNetInitParam::is_valid() const
{
  return arg_.is_valid() && !task_id_.is_invalid()
      && OB_NOT_NULL(bandwidth_throttle_)
      && OB_NOT_NULL(svr_rpc_proxy_)
      && OB_NOT_NULL(storage_rpc_);
}


ObTabletGroupRestoreDagNet::ObTabletGroupRestoreDagNet()
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

ObTabletGroupRestoreDagNet::~ObTabletGroupRestoreDagNet()
{
  free_restore_ctx_();
}

int ObTabletGroupRestoreDagNet::alloc_restore_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;

  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group restore ctx init twice", K(ret), KPC(ctx_));
  } else if (FALSE_IT(buf = mtl_malloc(sizeof(ObTabletGroupRestoreCtx), "TGRestoreCtx"))) {
  } else if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), KP(buf));
  } else if (FALSE_IT(ctx_ = new (buf) ObTabletGroupRestoreCtx())) {
  }
  return ret;
}

void ObTabletGroupRestoreDagNet::free_restore_ctx_()
{
  if (OB_ISNULL(ctx_)) {
    //do nothing
  } else {
    ctx_->~ObTabletGroupRestoreCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

int ObTabletGroupRestoreDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTGRDagNetInitParam* init_param = static_cast<const ObTGRDagNetInitParam*>(param);
  const int64_t priority = 1;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet group restore dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid() || !OB_BACKUP_INDEX_CACHE.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (init_param->arg_.is_leader_) {
    const backup::ObBackupRestoreMode mode = backup::ObBackupRestoreMode::RESTORE_MODE;
    const backup::ObBackupIndexLevel index_level = backup::ObBackupIndexLevel::BACKUP_INDEX_LEVEL_LOG_STREAM;
    storage::ObExternBackupSetInfoDesc backup_set_file_desc;
    backup::ObBackupIndexStoreParam index_store_param;
    storage::ObBackupDataStore store;
    if (OB_FAIL(store.init(init_param->arg_.restore_base_info_.backup_dest_))) {
      LOG_WARN("fail to init mgr", K(ret));
    } else if (OB_FAIL(store.read_backup_set_info(backup_set_file_desc))) {
      LOG_WARN("fail to read backup set info", K(ret));
    } else {
      share::ObBackupDataType data_type;
      data_type.set_major_data_backup();
      index_store_param.index_level_ = index_level;
      index_store_param.tenant_id_ = MTL_ID();
      index_store_param.backup_set_id_ = backup_set_file_desc.backup_set_file_.backup_set_id_;
      index_store_param.ls_id_ = init_param->arg_.ls_id_;
      index_store_param.is_tenant_level_ = true;
      index_store_param.backup_data_type_ = data_type;
      index_store_param.turn_id_ = backup_set_file_desc.backup_set_file_.data_turn_id_;
      index_store_param.retry_id_ = 0; // unused retry id.
    }

    share::ObBackupDest dest;
    // if the ls is new created, there are no ls sys tablet index backup.
    // because no sys tablet index backup exist, it will failed to init sys tablet index store.
    // on the other hand, ls which has sys tablet backup is restore sys tablet finish in restore sys status.
    // so it's no need to init sys tablet index store here.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_index_store_.init(mode, index_store_param,
        init_param->arg_.restore_base_info_.backup_dest_,
        backup_set_file_desc.backup_set_file_, false/*is_sec_meta*/, false/*not init sys tablet index store*/, OB_BACKUP_INDEX_CACHE))) {
      LOG_WARN("failed to init meta index store", K(ret), KPC(init_param));
    } else if (OB_FAIL(second_meta_index_store_.init(mode, index_store_param,
        init_param->arg_.restore_base_info_.backup_dest_,
        backup_set_file_desc.backup_set_file_, true/*is_sec_meta*/, false/*not init sys tablet index store*/, OB_BACKUP_INDEX_CACHE))) {
      LOG_WARN("failed to init macro index store", K(ret), KPC(init_param));
    }
  }


  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(alloc_restore_ctx_())) {
    LOG_WARN("failed to alloc tablet group restore ctx", K(ret));
  } else if (OB_FAIL(this->set_dag_id(init_param->task_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->arg_.assign(init_param->arg_))) {
    LOG_WARN("failed to assign tablet group restore arg", K(ret), KPC(init_param));
  } else if (OB_FAIL(ctx_->ha_table_info_mgr_.init())) {
    LOG_WARN("failed to init ha table key mgr", K(ret), KPC(init_param));
  }  else {
    ctx_->task_id_ = init_param->task_id_;
    kv_cache_ = &OB_BACKUP_INDEX_CACHE;
    bandwidth_throttle_ = init_param->bandwidth_throttle_;
    svr_rpc_proxy_ = init_param->svr_rpc_proxy_;
    storage_rpc_ = init_param->storage_rpc_;
    is_inited_ = true;
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_TABLET_INIT_PARAM_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("init tablet restore dag param failed", K(ret));
    }
  } 
#endif

  return ret;
}

int ObTabletGroupRestoreDagNet::start_running()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag net do not init", K(ret));
  } else if (OB_FAIL(start_running_for_restore_())) {
    LOG_WARN("failed to start running for restore", K(ret));
  }

  return ret;
}

int ObTabletGroupRestoreDagNet::start_running_for_restore_()
{
  int ret = OB_SUCCESS;
  ObInitialTabletGroupRestoreDag *initial_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group dag net do not init", K(ret));
  } else if (FALSE_IT(ctx_->start_ts_ = ObTimeUtil::current_time())) {
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(initial_restore_dag))) {
    LOG_WARN("failed to alloc inital restore dag ", K(ret));
  } else if (OB_FAIL(initial_restore_dag->init(this))) {
    LOG_WARN("failed to init initial restore dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*initial_restore_dag))) {
    LOG_WARN("failed to add initial restore dag into dag net", K(ret));
  } else if (OB_FAIL(initial_restore_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(initial_restore_dag))) {
    LOG_WARN("failed to add initial restore dag", K(ret), K(*initial_restore_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    initial_restore_dag = nullptr;
  }

  if (OB_NOT_NULL(initial_restore_dag) && OB_NOT_NULL(scheduler)) {
    scheduler->free_dag(*initial_restore_dag);
  }

  return ret;
}

bool ObTabletGroupRestoreDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    is_same = true;
  } else {
    is_same = false;
    //Here do not need check restore tablet is is same. Because :
    //1.Restore is ha service is a local thread which schedule it.It check duplicate tablet id
    //2.Scheduler is set limit for tablet group tablet number, so it allows duplicate tablet restore.
  }
  return is_same;
}

int64_t ObTabletGroupRestoreDagNet::hash() const
{
  int64_t hash_value = 0;
  if (OB_ISNULL(ctx_)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "restore ctx is NULL", KPC(ctx_));
  } else {
    hash_value = common::murmurhash(&ctx_->arg_.ls_id_, sizeof(ctx_->arg_.ls_id_), hash_value);
    for (int64_t i = 0; i < ctx_->arg_.tablet_id_array_.count(); ++i) {
      hash_value = common::murmurhash(&ctx_->arg_.tablet_id_array_.at(i),
          sizeof(ctx_->arg_.tablet_id_array_.at(i)), hash_value);
    }
  }
  return hash_value;
}

int ObTabletGroupRestoreDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag net do not init ", K(ret));
  } else if (ctx_->arg_.tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore tablet id array should not be empty", K(ret), KPC(ctx_));
  } else if (OB_UNLIKELY(0 > ctx_->task_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), K(*ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "ObTabletGroupRestoreDagNet: ls_id=%s,first_tablet_id=%s, trace_id=%s",
          to_cstring(ctx_->arg_.ls_id_), to_cstring(ctx_->arg_.tablet_id_array_.at(0)), task_id_str))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

int ObTabletGroupRestoreDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag net do not init", K(ret));
  } else if (ctx_->arg_.tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore tablet id array should not be empty", K(ret), KPC(ctx_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "ObTabletGroupRestoreDagNet: ls_id = %s, first_tablet_id = %s",
      to_cstring(ctx_->arg_.ls_id_), to_cstring(ctx_->arg_.tablet_id_array_.at(0))))) {
    LOG_WARN("failed to fill comment", K(ret), K(*ctx_));
  }
  return ret;
}

int ObTabletGroupRestoreDagNet::clear_dag_net_ctx()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start clear dag net ctx", KPC(ctx_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag net do not init", K(ret));
  } else {
    if (OB_TMP_FAIL(report_result_())) {
      LOG_WARN("failed to report result", K(tmp_ret), K(ret), KPC(ctx_));
    }
    ctx_->finish_ts_ = ObTimeUtil::current_time();
    const int64_t cost_ts = ctx_->finish_ts_ - ctx_->start_ts_;
    FLOG_INFO("finish tablet group restore dag net", "ls id", ctx_->arg_.ls_id_, "first_tablet_id",
        ctx_->arg_.tablet_id_array_.at(0), K(cost_ts));
  }
  return ret;
}

int ObTabletGroupRestoreDagNet::report_result_()
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  share::ObTaskId failed_task_id;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObArray<ObTabletID> succeed_tablet_array;
  ObArray<ObTabletID> failed_tablet_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag net do not init", K(ret));
  } else if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ctx_->arg_.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_result(result))) {
    LOG_WARN("failed to get tablet group restore ctx result", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ctx_->get_first_failed_task_id(failed_task_id))) {
    LOG_WARN("failed to get tablet group restore failed task id", K(ret), KPC(ctx_));
  } else if (OB_FAIL(ls->get_ls_restore_handler()->handle_execute_over(
      OB_SUCCESS == result ? ctx_->task_id_ : failed_task_id, succeed_tablet_array, failed_tablet_array, ctx_->arg_.ls_id_, result))) {
    LOG_WARN("failed to handle execute over tablet group restotre", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObTabletGroupRestoreDagNet::deal_with_cancel()
{
  int ret = OB_SUCCESS;
  const int32_t result = OB_CANCELED;
  const bool need_retry = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag net do not init", K(ret));
  } else if (OB_FAIL(ctx_->set_result(result, need_retry))) {
    LOG_WARN("failed to set result", K(ret), KPC(this));
  }
  return ret;
}

/******************ObTabletGroupRestoreDag*********************/
ObTabletGroupRestoreDag::ObTabletGroupRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObStorageHADag(dag_type)
{
}

ObTabletGroupRestoreDag::~ObTabletGroupRestoreDag()
{
}

bool ObTabletGroupRestoreDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    is_same = false;
    //Here do not need check restore tablet is is same. Because :
    //1.Restore is ha service is a local thread which schedule it.It check duplicate tablet id
    //2.Scheduler is set limit for tablet group tablet number, so it allows duplicate tablet restore.
  }
  return is_same;
}

int64_t ObTabletGroupRestoreDag::hash() const
{
  int64_t hash_value = 0;
  ObTabletGroupRestoreCtx *ctx = get_ctx();

  if (OB_ISNULL(ctx)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "tablet group restore ctx should not be NULL", KP(ctx));
  } else {
    hash_value = common::murmurhash(
        &ctx->arg_.ls_id_, sizeof(ctx->arg_.ls_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
    for (int64_t i = 0; i < ctx->arg_.tablet_id_array_.count(); ++i) {
      hash_value = common::murmurhash(&ctx->arg_.tablet_id_array_.at(i),
          sizeof(ctx->arg_.tablet_id_array_.at(i)), hash_value);
    }
  }
  return hash_value;
}

int ObTabletGroupRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreCtx *ctx = nullptr;
  if (OB_ISNULL(ctx = get_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore ctx should not be NULL", K(ret), KP(ctx));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                ctx->arg_.ls_id_.id(),
                                static_cast<int64_t>(ctx->arg_.tablet_id_array_.at(0).id()),
                                static_cast<int64_t>(ctx->arg_.is_leader_),
                                "dag_net_task_id", to_cstring(ctx->task_id_),
                                "src", to_cstring(ctx->arg_.src_.get_server())))){
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

/******************ObInitialTabletGroupRestoreDag*********************/
ObInitialTabletGroupRestoreDag::ObInitialTabletGroupRestoreDag()
  : ObTabletGroupRestoreDag(ObDagType::DAG_TYPE_INITIAL_TABLET_GROUP_RESTORE),
    is_inited_(false)
{
}

ObInitialTabletGroupRestoreDag::~ObInitialTabletGroupRestoreDag()
{
}

int ObInitialTabletGroupRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet group restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx()) || ctx->arg_.tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore ctx should not be NULL or tablet id array should not empty", K(ret), KPC(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObInitialTabletGroupRestoreDag: ls_id = %s, first_tablet_id = %s",
       to_cstring(ctx->arg_.ls_id_), to_cstring(ctx->arg_.tablet_id_array_.at(0))))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObInitialTabletGroupRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreDagNet *tablet_group_restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial tablet group restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(tablet_group_restore_dag_net = static_cast<ObTabletGroupRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = tablet_group_restore_dag_net->get_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObInitialTabletGroupRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObInitialTabletGroupRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init initial tablet group restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObInitialTabletGroupRestoreTask*********************/
ObInitialTabletGroupRestoreTask::ObInitialTabletGroupRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    dag_net_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    ls_handle_(),
    ha_tablets_builder_()
{
}

ObInitialTabletGroupRestoreTask::~ObInitialTabletGroupRestoreTask()
{
}

int ObInitialTabletGroupRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupRestoreDagNet *restore_dag_net = nullptr;
  ObLSService *ls_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initial tablet group restore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(restore_dag_net = static_cast<ObTabletGroupRestoreDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ls_service =  (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else {
    ctx_ = restore_dag_net->get_restore_ctx();
    bandwidth_throttle_ = restore_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = restore_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = restore_dag_net->get_storage_rpc();
    dag_net_ = dag_net;
    meta_index_store_ = restore_dag_net->get_meta_index_store();
    second_meta_index_store_ = restore_dag_net->get_second_meta_index_store();

    if (OB_FAIL(ls_service->get_ls(ctx_->arg_.ls_id_, ls_handle_, ObLSGetMod::HA_MOD))) {
      LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed init initial tablet group restore task", "ls id", ctx_->arg_.ls_id_,
          "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_, "tablet_id_array", ctx_->arg_.tablet_id_array_);
    }
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (OB_FAIL(check_local_ls_restore_status_())) {
    LOG_WARN("failed to check local ls restore status", K(ret), KPC(ctx_));
  } else if (OB_FAIL(check_local_tablets_restore_status_())) {
    LOG_WARN("failed to check tablets restore status", K(ret), KPC(ctx_));
  } else if (ctx_->tablet_id_array_.empty()) {
    LOG_INFO("no tablet need restore, skip it", KPC(ctx_));
  } else if (OB_FAIL(choose_src_())) {
    LOG_WARN("failed to choose src", K(ret), KPC(ctx_));
  } else if (OB_FAIL(init_ha_tablets_builder_())) {
    LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
  } else if (OB_FAIL(renew_tablets_meta_())) {
    LOG_WARN("failed to create or update tablets", K(ret), KPC(ctx_));
  } else if (ObTabletRestoreAction::is_restore_tablet_meta(ctx_->arg_.action_)) {
    LOG_INFO("only restore tablet meta, skip generate tablet restore dags", KPC(ctx_));
  } else if (OB_FAIL(build_tablet_group_ctx_())) {
    LOG_WARN("failed to build tablet group ctx", K(ret), KPC(ctx_));
  } else if (OB_FAIL(generate_tablet_restore_dags_())) {
    LOG_WARN("failed to generate tablet restore dags", K(ret), K(*ctx_));
  }

  if (ctx_->arg_.ls_id_.is_sys_ls() && ctx_->arg_.is_leader_) {
    DEBUG_SYNC(WHILE_LEADER_RESTORE_GROUP_TABLET);
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObInitialTabletGroupRestoreTask::check_local_ls_restore_status_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSRestoreStatus ls_restore_status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else {
    if (OB_FAIL(ls->get_restore_status(ls_restore_status))) {
      LOG_WARN("failed to get restore status", K(ret), KPC(ctx_));
    } else if (!ls_restore_status.is_restore_tablets_meta()
               && !ls_restore_status.is_quick_restore()
               && !ls_restore_status.is_restore_major_data()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls restore status is unexpected", K(ret), K(ls_restore_status), KPC(ctx_));
    }
  }

  return ret;
}

int ObInitialTabletGroupRestoreTask::check_local_tablets_restore_status_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTablet *tablet = nullptr;
  ObTabletRestoreStatus::STATUS action_restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  ObTabletRestoreStatus::STATUS tablet_restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  const ObMDSGetTabletMode timeout_us = ObMDSGetTabletMode::READ_WITHOUT_CHECK;
  ObInitialTabletGroupRestoreDag *dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_ISNULL(dag = static_cast<ObInitialTabletGroupRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start tablet group restore dag should not be NULL", K(ret), KP(dag));
  } else if (OB_FAIL(ObTabletRestoreAction::trans_restore_action_to_restore_status(
      ctx_->arg_.action_, action_restore_status))) {
    LOG_WARN("failed to trans restore action to restore status", K(ret), KPC(ctx_));
  } else {
    ctx_->tablet_id_array_.reset();
    bool can_change = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->arg_.tablet_id_array_.count(); ++i) {
      const ObTabletID &tablet_id = ctx_->arg_.tablet_id_array_.at(i);
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))
          && OB_TABLET_NOT_EXIST != ret) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id), KPC(ctx_), K(timeout_us));
      } else if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("tablet not exist, skip restore", K(tablet_id));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), KPC(ctx_));
      } else if (OB_FAIL(tablet->get_tablet_meta().ha_status_.get_restore_status(tablet_restore_status))) {
        LOG_WARN("failed to get restore status", K(ret), KPC(tablet));
      } else if (!ObTabletRestoreStatus::is_valid(tablet_restore_status)) {
        ret = OB_ERR_UNEXPECTED;
         LOG_WARN("tablet restore status is invalid", K(ret), K(tablet_id), K(action_restore_status),
             K(tablet_restore_status), K(ctx_->arg_));
      } else if (ObTabletRestoreStatus::is_undefined(tablet_restore_status)) {
        LOG_INFO("tablet restore status is undefined, skip restore it", K(ret));
      } else if (OB_FAIL(ObTabletRestoreStatus::check_can_change_status(tablet_restore_status,
          action_restore_status, can_change))) {
        LOG_WARN("failed to check can change status", K(ret), K(tablet_restore_status),
            K(action_restore_status), KPC(ctx_));
      } else if (!can_change) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet restore status is unexpected", K(ret), K(tablet_id), K(action_restore_status),
            K(tablet_restore_status), K(ctx_->arg_));
      } else if (action_restore_status == tablet_restore_status) {
        LOG_INFO("tablet restore status is equal to restore action, skip restore it", K(tablet_id),
            K(action_restore_status), K(tablet_restore_status), K(ctx_->arg_));
      } else if (OB_FAIL(ctx_->tablet_id_array_.push_back(tablet_id))) {
        LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_id), KPC(ctx_));
      }
    }
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::choose_src_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (ctx_->arg_.is_leader_) {
    if (OB_FAIL(choose_leader_src_())) {
      LOG_WARN("failed to choose leader src", K(ret), KPC(ctx_));
    }
  } else {
    if (OB_FAIL(choose_follower_src_())) {
      LOG_WARN("failed to choose follower src", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::choose_follower_src_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  obrpc::ObFetchLSMetaInfoResp ls_info;
  ObLSRestoreStatus ls_restore_status;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (ctx_->arg_.is_leader_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("choose follower src get invalid argumnet", K(ret), KPC(ctx_));
  } else {
    ObStorageHASrcInfo src_info;
    src_info.src_addr_ = ctx_->arg_.src_.get_server();
    src_info.cluster_id_ = GCONF.cluster_id;
    if (OB_FAIL(storage_rpc_->post_ls_meta_info_request(
        tenant_id, src_info, ctx_->arg_.ls_id_, ls_info))) {
      LOG_WARN("fail to post fetch ls meta info request", K(ret), K(src_info), "arg", ctx_->arg_);
    } else if (OB_FAIL(ls_info.ls_meta_package_.ls_meta_.get_restore_status(ls_restore_status))) {
      LOG_WARN("failed to get restore status", K(ret), K(ls_info));
    } else {
      ctx_->src_ = src_info;
      ctx_->need_check_seq_ = true;
      ctx_->ls_rebuild_seq_ = ls_info.ls_meta_package_.ls_meta_.get_rebuild_seq();
    }
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::choose_leader_src_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  obrpc::ObCopyLSInfo ls_info;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (!ctx_->arg_.is_leader_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("choose leader src get invalid argumnet", K(ret), KPC(ctx_));
  } else {
    ctx_->need_check_seq_ = false;
    ctx_->ls_rebuild_seq_ = -1;
    //TODO(muwei.ym) using restore reader get ls info in 4.3
    //TOOD(muwei.ym) use more ls info to check src in 4.3
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::generate_tablet_restore_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStartTabletGroupRestoreDag *start_restore_dag = nullptr;
  ObFinishTabletGroupRestoreDag *finish_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObInitialTabletGroupRestoreDag *initial_tablets_group_restore_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("inital tablet group restore init task do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(initial_tablets_group_restore_dag = static_cast<ObInitialTabletGroupRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("initial tablets group restore dag should not be NULL", K(ret), KP(initial_tablets_group_restore_dag));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(start_restore_dag))) {
      LOG_WARN("failed to alloc start restore dag ", K(ret));
    } else if (OB_FAIL(scheduler->alloc_dag(finish_restore_dag))) {
      LOG_WARN("failed to alloc finish restore dag", K(ret));
    } else if (OB_FAIL(start_restore_dag->init(dag_net_, finish_restore_dag))) {
      LOG_WARN("failed to init start restore dag", K(ret));
    } else if (OB_FAIL(finish_restore_dag->init(dag_net_))) {
      LOG_WARN("failed to init finish restore dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*start_restore_dag))) {
      LOG_WARN("failed to add start restore dag as child", K(ret), KPC(start_restore_dag));
    } else if (OB_FAIL(start_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(start_restore_dag->add_child(*finish_restore_dag))) {
      LOG_WARN("failed to add finish restore dag as child", K(ret));
    } else if (OB_FAIL(finish_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(finish_restore_dag))) {
      LOG_WARN("failed to add finish restore dag", K(ret), K(*finish_restore_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else if (OB_FAIL(scheduler->add_dag(start_restore_dag))) {
      LOG_WARN("failed to add dag", K(ret), K(*start_restore_dag));
      if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(finish_restore_dag))) {
        LOG_WARN("failed to cancel ha dag", K(tmp_ret), KPC(start_restore_dag));
      } else {
        finish_restore_dag = nullptr;
      }
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("succeed to schedule start restore dag", K(*start_restore_dag));
    }

    if (OB_FAIL(ret)) {

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(finish_restore_dag)) {
        scheduler->free_dag(*finish_restore_dag);
        finish_restore_dag = nullptr;
      }

      if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(start_restore_dag)) {
        scheduler->free_dag(*start_restore_dag);
        start_restore_dag = nullptr;
      }

      const bool need_retry = true;
      if (OB_SUCCESS != (tmp_ret = ctx_->set_result(ret, need_retry, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set restore result", K(ret), K(tmp_ret), K(*ctx_));
      }
    }
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::renew_tablets_meta_()
{
  // Tablets are first created using tablets' meta which are backed up
  // at phase BACKUP_META. These tablets' meta are relatively older than
  // them backed up at phase BACKUP_DATA_MINOR. So we renew it.
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (!ObTabletRestoreAction::is_restore_tablet_meta(ctx_->arg_.action_)
             && !ObTabletRestoreAction::is_restore_all(ctx_->arg_.action_)) {
    //do nothing
  } else if (OB_FAIL(ha_tablets_builder_.update_pending_tablets_with_remote())) {
    LOG_WARN("failed to update_pending_tablets_with_remote", K(ret), KPC_(ctx));
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::init_ha_tablets_builder_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else if (OB_FAIL(ObTabletGroupRestoreUtils::init_ha_tablets_builder(
      ctx_->arg_.tenant_id_, ctx_->tablet_id_array_, ctx_->arg_.is_leader_,
      ctx_->need_check_seq_, ctx_->ls_rebuild_seq_, ctx_->src_,
      ls_handle_.get_ls(), &ctx_->arg_.restore_base_info_, ctx_->arg_.action_, meta_index_store_,
      &ctx_->ha_table_info_mgr_, ha_tablets_builder_))) {
   LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
 }
  return ret;
}

int ObInitialTabletGroupRestoreTask::build_tablet_group_ctx_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("initial tablet group restore task do not init", K(ret));
  } else {
    ctx_->tablet_group_ctx_.reuse();
    if (OB_FAIL(ctx_->tablet_group_ctx_.init(ctx_->tablet_id_array_))) {
      LOG_WARN("failed to init tablet group ctx", K(ret), KPC(ctx_));
    }
  }
  return ret;
}

int ObInitialTabletGroupRestoreTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (ctx_->arg_.tablet_id_array_.empty()) {
    // do nothing
  } else {
    SERVER_EVENT_ADD("storage_ha", "initial_tablet_group_restore",
      "tenant_id", ctx_->arg_.tenant_id_,
      "ls_id", ctx_->arg_.ls_id_.id(),
      "is_leader", ctx_->arg_.is_leader_,
      "first_tablet_id", ctx_->arg_.tablet_id_array_.at(0));
  }
  return ret;
}

/******************ObStartTabletGroupRestoreDag*********************/
ObStartTabletGroupRestoreDag::ObStartTabletGroupRestoreDag()
  : ObTabletGroupRestoreDag(ObDagType::DAG_TYPE_START_TABLET_GROUP_RESTORE),
    is_inited_(false),
    finish_dag_(nullptr)
{
}

ObStartTabletGroupRestoreDag::~ObStartTabletGroupRestoreDag()
{
}

int ObStartTabletGroupRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx()) || ctx->arg_.tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore ctx should not be NULL or tablet id array should not empty", K(ret), KP(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObStartTabletGroupRestoreDag: ls_id = %s, first_tablet_id = %s",
       to_cstring(ctx->arg_.ls_id_), to_cstring(ctx->arg_.tablet_id_array_.at(0))))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObStartTabletGroupRestoreDag::init(
    share::ObIDagNet *dag_net,
    share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreDagNet *restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start tablet group restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net) || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start tablet group restore dag init get invalid argument", K(ret), KP(dag_net), KP(finish_dag));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(restore_dag_net = static_cast<ObTabletGroupRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = restore_dag_net->get_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    finish_dag_ = finish_dag;
    is_inited_ = true;
  }
  return ret;
}

int ObStartTabletGroupRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObStartTabletGroupRestoreTask *task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(finish_dag_))) {
    LOG_WARN("failed to init start tablet group restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObStartTabletGroupRestoreTask*********************/
ObStartTabletGroupRestoreTask::ObStartTabletGroupRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    finish_dag_(nullptr),
    ls_handle_(),
    ha_tablets_builder_()
{
}

ObStartTabletGroupRestoreTask::~ObStartTabletGroupRestoreTask()
{
}

int ObStartTabletGroupRestoreTask::init(
    share::ObIDag *finish_dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupRestoreDagNet* restore_dag_net = nullptr;
  ObLSService *ls_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("start tablet group restore task init twice", K(ret));
  } else if (OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("start tablet group restore task init get invlaid argument", K(ret), KP(finish_dag));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(restore_dag_net = static_cast<ObTabletGroupRestoreDagNet*>(dag_net))) {
  } else if (OB_ISNULL(ls_service = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else {
    ctx_ = restore_dag_net->get_restore_ctx();
    bandwidth_throttle_ = restore_dag_net->get_bandwidth_throttle();
    svr_rpc_proxy_ = restore_dag_net->get_storage_rpc_proxy();
    storage_rpc_ = restore_dag_net->get_storage_rpc();
    meta_index_store_ = restore_dag_net->get_meta_index_store();
    second_meta_index_store_ = restore_dag_net->get_second_meta_index_store();
    finish_dag_ = finish_dag;
    if (OB_FAIL(ls_service->get_ls(ctx_->arg_.ls_id_, ls_handle_, ObLSGetMod::HA_MOD))) {
      LOG_WARN("failed to get ls", K(ret), KPC(ctx_));
    } else if (OB_FAIL(ObTabletGroupRestoreUtils::init_ha_tablets_builder(
        ctx_->arg_.tenant_id_, ctx_->tablet_id_array_, ctx_->arg_.is_leader_,
        ctx_->need_check_seq_, ctx_->ls_rebuild_seq_, ctx_->src_,
        ls_handle_.get_ls(), &ctx_->arg_.restore_base_info_, ctx_->arg_.action_, meta_index_store_,
        &ctx_->ha_table_info_mgr_, ha_tablets_builder_))) {
      LOG_WARN("failed to init ha tablets builder", K(ret), KPC(ctx_));
    } else {
      ctx_->ha_table_info_mgr_.reuse();
      is_inited_ = true;
      LOG_INFO("succeed init start tablet group restore task", "ls id", ctx_->arg_.ls_id_,
          "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_, "tablet_id_array", ctx_->tablet_id_array_);
    }
  }
  return ret;
}

int ObStartTabletGroupRestoreTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start do start tablet group restore task", K(ret), KPC(ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group retore task do not init", K(ret));
  } else if (ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(create_tablets_sstable_())) {
    LOG_WARN("failed to create tablets sstable", K(ret));
  } else if (OB_FAIL(generate_tablet_restore_dag_())) {
    LOG_WARN("failed to generate tablet restore dag", K(ret));
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(ctx_));
    }
  }

  return ret;
}

int ObStartTabletGroupRestoreTask::generate_tablet_restore_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObStartTabletGroupRestoreDag *start_tablet_group_restore_dag = nullptr;
  ObTabletID tablet_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group restore task do not init", K(ret));
  } else if (OB_ISNULL(start_tablet_group_restore_dag = static_cast<ObStartTabletGroupRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start tablet group restore dag should not be NULL", K(ret), KP(start_tablet_group_restore_dag));
  } else if (OB_ISNULL(dag_net = start_tablet_group_restore_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_->tablet_group_ctx_.get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("no tablets need restore", KPC(ctx_));
        } else {
          LOG_WARN("failed to get next tablet id", K(ret), KPC(ctx_));
        }
        break;
      }

      ObIDag *parent = this->get_dag();
      ObTabletRestoreDag *tablet_restore_dag = nullptr;
      ObInitTabletRestoreParam param;
      param.tenant_id_ = ctx_->arg_.tenant_id_;
      param.ls_id_ = ctx_->arg_.ls_id_;
      param.tablet_id_ = tablet_id;
      param.ha_dag_net_ctx_ = ctx_;
      param.is_leader_ = ctx_->arg_.is_leader_;
      param.action_ = ctx_->arg_.action_;
      param.ha_table_info_mgr_ = &ctx_->ha_table_info_mgr_;
      param.restore_base_info_ = &ctx_->arg_.restore_base_info_;
      param.meta_index_store_ = meta_index_store_;
      param.second_meta_index_store_ = second_meta_index_store_;
      param.tablet_group_ctx_ = &ctx_->tablet_group_ctx_;
      param.need_check_seq_ = ctx_->need_check_seq_;
      param.ls_rebuild_seq_ = ctx_->ls_rebuild_seq_;

      if (!param.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("init tablet restore param not valid", K(ret), K(param), KPC(ctx_));
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_restore_dag))) {
        LOG_WARN("failed to alloc tablet restore dag ", K(ret));
      } else if (OB_FAIL(tablet_restore_dag->init(param))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          //overwrite ret
          LOG_INFO("tablet is deleted, skip restore", K(tablet_id), K(param));
          scheduler->free_dag(*tablet_restore_dag);
          tablet_restore_dag = nullptr;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to init tablet restore dag", K(ret), K(*ctx_));
        }
      } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*tablet_restore_dag))) {
        LOG_WARN("failed to add dag into dag net", K(ret), K(*ctx_));
      } else if (OB_FAIL(parent->add_child_without_inheritance(*tablet_restore_dag))) {
        LOG_WARN("failed to add child dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_restore_dag->create_first_task())) {
        LOG_WARN("failed to create first task", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_restore_dag->add_child_without_inheritance(*finish_dag_))) {
        LOG_WARN("failed to add finish dag as child", K(ret), K(*ctx_));
      } else if (OB_FAIL(scheduler->add_dag(tablet_restore_dag))) {
        LOG_WARN("failed to add tablet restore dag", K(ret), K(*tablet_restore_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      } else {
        LOG_INFO("succeed to schedule tablet restore dag", K(*tablet_restore_dag));
        tablet_restore_dag = nullptr;
        break;
      }

      if (OB_FAIL(ret)) {
        if (OB_NOT_NULL(tablet_restore_dag)) {
          scheduler->free_dag(*tablet_restore_dag);
          tablet_restore_dag = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObStartTabletGroupRestoreTask::create_tablets_sstable_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("start tablet group restore task do not init", K(ret), KPC(ctx_));
  } else if (!ObTabletRestoreAction::is_restore_minor(ctx_->arg_.action_)
      && !ObTabletRestoreAction::is_restore_major(ctx_->arg_.action_)
      && !ObTabletRestoreAction::is_restore_all(ctx_->arg_.action_)) {
    //do nothing
  } else if (OB_FAIL(ha_tablets_builder_.build_tablets_sstable_info())) {
    LOG_WARN("failed to build tablets sstable info", K(ret), KPC(ctx_));
  }
  return ret;
}

int ObStartTabletGroupRestoreTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (ctx_->arg_.tablet_id_array_.empty()) {
    // do nothing
  } else {
    SERVER_EVENT_ADD("storage_ha", "start_tablet_group_restore",
      "tenant_id", ctx_->arg_.tenant_id_,
      "ls_id", ctx_->arg_.ls_id_.id(),
      "is_leader", ctx_->arg_.is_leader_,
      "first_tablet_id", ctx_->arg_.tablet_id_array_.at(0));
  }
  return ret;
}

/******************ObFinishTabletGroupRestoreDag*********************/
ObFinishTabletGroupRestoreDag::ObFinishTabletGroupRestoreDag()
  : ObTabletGroupRestoreDag(ObDagType::DAG_TYPE_FINISH_TABLET_GROUP_RESTORE),
    is_inited_(false)
{
}

ObFinishTabletGroupRestoreDag::~ObFinishTabletGroupRestoreDag()
{
}

int ObFinishTabletGroupRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreCtx *ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group restore dag do not init", K(ret));
  } else if (OB_ISNULL(ctx = get_ctx()) || ctx->arg_.tablet_id_array_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet group restore ctx should not be NULL or tablet restore id array should not be empty", K(ret), KPC(ctx));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObFinishTabletGroupRestoreDag: ls_id = %s, first_tablet_id = %s",
       to_cstring(ctx->arg_.ls_id_), to_cstring(ctx->arg_.tablet_id_array_.at(0))))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(ctx));
  }
  return ret;
}

int ObFinishTabletGroupRestoreDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObTabletGroupRestoreDagNet *restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish tablet group restore dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(restore_dag_net = static_cast<ObTabletGroupRestoreDagNet*>(dag_net))) {
  } else if (FALSE_IT(ha_dag_net_ctx_ = restore_dag_net->get_restore_ctx())) {
  } else if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore ctx should not be NULL", K(ret), KP(ha_dag_net_ctx_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObFinishTabletGroupRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishTabletGroupRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish tablet group restore task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishTabletGroupRestoreTask*********************/
ObFinishTabletGroupRestoreTask::ObFinishTabletGroupRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ctx_(nullptr),
    dag_net_(nullptr)
{
}

ObFinishTabletGroupRestoreTask::~ObFinishTabletGroupRestoreTask()
{
}

int ObFinishTabletGroupRestoreTask::init()
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  ObTabletGroupRestoreDagNet *restore_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish tablet group restore task init twice", K(ret));
  } else if (FALSE_IT(dag_net = this->get_dag()->get_dag_net())) {
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_RESTORE != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else if (FALSE_IT(restore_dag_net = static_cast<ObTabletGroupRestoreDagNet*>(dag_net))) {
  } else {
    ctx_ = restore_dag_net->get_restore_ctx();
    dag_net_ = dag_net;
    is_inited_ = true;
    LOG_INFO("succeed init finish tablet group restore task", "ls id", ctx_->arg_.ls_id_,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", ctx_->task_id_, "tablet_id_array", ctx_->arg_.tablet_id_array_);

  }
  return ret;
}

int ObFinishTabletGroupRestoreTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  FLOG_INFO("start do finish tablet group restore task");

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group restore task do not init", K(ret));
  } else {

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_FINISH_TABLET_GROUP_RESTORE_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_FINISH_TABLET_GROUP_RESTORE_FAILED", K(ret));
      int32_t retry_count = 0;
      const int32_t error_code = ret;
      if (ctx_->is_failed()) {
        //do nothing
      } else if (OB_FAIL(ctx_->get_retry_count(retry_count))) {
        LOG_WARN("failed to get retry count", K(ret), KPC(ctx_));
      } else if (0 != retry_count) {
        //do nothing
      } else if (OB_FAIL(ctx_->set_result(error_code, true/*allow retry*/, this->get_dag()->get_type()))) {
        LOG_WARN("failed to set result", K(ret), K(error_code), KPC(ctx_));
      }
    }
  }
#endif

    if (ctx_->is_failed()) {
      bool allow_retry = false;
      if (OB_FAIL(ctx_->check_allow_retry(allow_retry))) {
        LOG_ERROR("failed to check need retry", K(ret), K(*ctx_));
      } else if (allow_retry) {
        ctx_->reuse();
        if (OB_FAIL(generate_restore_init_dag_())) {
          LOG_WARN("failed to generate restore init dag", K(ret), KPC(ctx_));
        }
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
        LOG_WARN("failed to record server event", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObFinishTabletGroupRestoreTask::generate_restore_init_dag_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObInitialTabletGroupRestoreDag *initial_restore_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObFinishTabletGroupRestoreDag *finish_tablet_group_restore_dag = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet group task do not init", K(ret));
  } else if (OB_ISNULL(finish_tablet_group_restore_dag = static_cast<ObFinishTabletGroupRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish tablet group restore dag should not be NULL", K(ret), KP(finish_tablet_group_restore_dag));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(initial_restore_dag))) {
      LOG_WARN("failed to alloc initial restore dag ", K(ret));
    } else if (OB_FAIL(initial_restore_dag->init(dag_net_))) {
      LOG_WARN("failed to init initial restore dag", K(ret));
    } else if (OB_FAIL(this->get_dag()->add_child(*initial_restore_dag))) {
      LOG_WARN("failed to add initial restore dag as chiild", K(ret), KPC(initial_restore_dag));
    } else if (OB_FAIL(initial_restore_dag->create_first_task())) {
      LOG_WARN("failed to create first task", K(ret));
    } else if (OB_FAIL(scheduler->add_dag(initial_restore_dag))) {
      LOG_WARN("failed to add initial restore dag", K(ret), K(*initial_restore_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
    } else {
      LOG_INFO("start create tablet group initial restore dag", K(ret), K(*ctx_));
      initial_restore_dag = nullptr;
    }

    if (OB_NOT_NULL(initial_restore_dag) && OB_NOT_NULL(scheduler)) {
      scheduler->free_dag(*initial_restore_dag);
      initial_restore_dag = nullptr;
    }
  }
  return ret;
}

int ObFinishTabletGroupRestoreTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else if (ctx_->arg_.tablet_id_array_.empty()) {
    // do nothing
  } else {
    SERVER_EVENT_ADD("storage_ha", "finish_tablet_group_restore",
      "tenant_id", ctx_->arg_.tenant_id_,
      "ls_id", ctx_->arg_.ls_id_.id(),
      "is_leader", ctx_->arg_.is_leader_,
      "first_tablet_id", ctx_->arg_.tablet_id_array_.at(0));
  }
  return ret;
}

/******************ObInitTabletRestoreParam*********************/
ObInitTabletRestoreParam::ObInitTabletRestoreParam()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    ha_dag_net_ctx_(nullptr),
    is_leader_(false),
    action_(ObTabletRestoreAction::MAX),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    ha_table_info_mgr_(nullptr),
    tablet_group_ctx_(nullptr),
    need_check_seq_(false),
    ls_rebuild_seq_(-1)
{
}

ObInitTabletRestoreParam::~ObInitTabletRestoreParam()
{
}

void ObInitTabletRestoreParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  ha_dag_net_ctx_ = nullptr;
  is_leader_ = false;
  action_ = ObTabletRestoreAction::MAX;
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  ha_table_info_mgr_ = nullptr;
  tablet_group_ctx_ = nullptr;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
}

bool ObInitTabletRestoreParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && tablet_id_.is_valid()
      && OB_NOT_NULL(ha_dag_net_ctx_)
      && ObTabletRestoreAction::is_valid(action_)
      && OB_NOT_NULL(ha_table_info_mgr_)
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);
  if (bool_ret) {
    if (is_leader_) {
      bool_ret = OB_NOT_NULL(restore_base_info_)
        && OB_NOT_NULL(meta_index_store_)
        && OB_NOT_NULL(second_meta_index_store_);
    }
  }
  return bool_ret;
}

/******************ObTabletRestoreDag*********************/
ObTabletRestoreDag::ObTabletRestoreDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_TABLET_RESTORE),
    is_inited_(false),
    tablet_restore_ctx_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    ls_handle_(),
    tablet_group_ctx_(nullptr)
{
}

ObTabletRestoreDag::~ObTabletRestoreDag()
{
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(tablet_restore_ctx_.ha_table_info_mgr_)) {
    if (OB_SUCCESS != (tmp_ret = tablet_restore_ctx_.ha_table_info_mgr_->remove_tablet_table_info(
        tablet_restore_ctx_.tablet_id_))) {
      LOG_WARN_RET(tmp_ret, "failed to remove tablet table info", K(tmp_ret), K(tablet_restore_ctx_));
    }
  }
}

bool ObTabletRestoreDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTabletRestoreDag &other_dag = static_cast<const ObTabletRestoreDag&>(other);
    if (tablet_restore_ctx_.ls_id_ != other_dag.tablet_restore_ctx_.ls_id_
        || tablet_restore_ctx_.tablet_id_ != other_dag.tablet_restore_ctx_.tablet_id_) {
      is_same = false;
    }

  }
  return is_same;
}

int64_t ObTabletRestoreDag::hash() const
{
  int64_t hash_value = 0;
  const ObDagType::ObDagTypeEnum type = get_type();
  hash_value = common::murmurhash(
      &tablet_restore_ctx_.ls_id_, sizeof(tablet_restore_ctx_.ls_id_), hash_value);
  hash_value = common::murmurhash(
      &type, sizeof(type), hash_value);
  hash_value = common::murmurhash(
      &tablet_restore_ctx_.tablet_id_, sizeof(tablet_restore_ctx_.tablet_id_), hash_value);
  return hash_value;
}

int ObTabletRestoreDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore dag do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletRestoreDag: ls_id = %s, tablet_id = %s, restore_action = %d",
       to_cstring(tablet_restore_ctx_.ls_id_), to_cstring(tablet_restore_ctx_.tablet_id_),
       tablet_restore_ctx_.action_))) {
    LOG_WARN("failed to fill comment", K(ret), K(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreDag::get_dag_net_task_id_(share::ObTaskId &task_id) const
{
  int ret = OB_SUCCESS;
  task_id.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore dag do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_RESTORE == ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ObLSRestoreCtx *ctx = static_cast<ObLSRestoreCtx *>(ha_dag_net_ctx_);
    task_id = ctx->task_id_;
  } else if (ObIHADagNetCtx::TABLET_GROUP_RESTORE == ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ObTabletGroupRestoreCtx *ctx = static_cast<ObTabletGroupRestoreCtx *>(ha_dag_net_ctx_);
    task_id = ctx->task_id_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  }
  return ret;
}

int ObTabletRestoreDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  share::ObTaskId task_id;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore dag do not init", K(ret));
  } else if (OB_FAIL(get_dag_net_task_id_(task_id))) {
    LOG_WARN("failed to get dag net task id", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                tablet_restore_ctx_.ls_id_.id(),
                                static_cast<int64_t>(tablet_restore_ctx_.tablet_id_.id()),
                                static_cast<int64_t>(tablet_restore_ctx_.is_leader_),
                                "dag_net_task_id", to_cstring(task_id)))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObTabletRestoreDag::init(
    const ObInitTabletRestoreParam &param)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  bool is_exist = false;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet restore dag init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet restore dag init get invalid argument", K(ret), K(param));
  } else if (OB_ISNULL(ls_service = (MTL(ObLSService *)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(param.ls_id_, ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(param));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(param));
  } else if (OB_FAIL(ls->ha_get_tablet(param.tablet_id_, tablet_restore_ctx_.tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), K(param));
  } else if (OB_FAIL(param.ha_table_info_mgr_->check_copy_tablet_exist(param.tablet_id_, is_exist))) {
    LOG_WARN("failed to check copy tablet exist", K(ret), K(param));
  } else if (FALSE_IT(status = is_exist ? ObCopyTabletStatus::TABLET_EXIST : ObCopyTabletStatus::TABLET_NOT_EXIST)) {
  } else if (OB_FAIL(tablet_restore_ctx_.set_copy_tablet_status(status))) {
    LOG_WARN("failed to set copy tablet status", K(ret), K(status), K(param));
  } else {
    tablet_restore_ctx_.tenant_id_ = param.tenant_id_;
    tablet_restore_ctx_.ls_id_ = param.ls_id_;
    tablet_restore_ctx_.tablet_id_ = param.tablet_id_;
    tablet_restore_ctx_.restore_base_info_ = param.restore_base_info_;
    tablet_restore_ctx_.action_ = param.action_;
    tablet_restore_ctx_.is_leader_ = param.is_leader_;
    tablet_restore_ctx_.meta_index_store_ = param.meta_index_store_;
    tablet_restore_ctx_.second_meta_index_store_ = param.second_meta_index_store_;
    tablet_restore_ctx_.replica_type_ = REPLICA_TYPE_FULL;
    tablet_restore_ctx_.ha_table_info_mgr_ = param.ha_table_info_mgr_;
    tablet_restore_ctx_.need_check_seq_ = param.need_check_seq_;
    tablet_restore_ctx_.ls_rebuild_seq_ = param.ls_rebuild_seq_;
    ha_dag_net_ctx_ = param.ha_dag_net_ctx_;
    bandwidth_throttle_ = GCTX.bandwidth_throttle_;
    svr_rpc_proxy_ = ls_service->get_storage_rpc_proxy();
    storage_rpc_ = ls_service->get_storage_rpc();
    compat_mode_ = tablet_restore_ctx_.tablet_handle_.get_obj()->get_tablet_meta().compat_mode_;
    tablet_group_ctx_ = param.tablet_group_ctx_;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletRestoreDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletRestoreTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(tablet_restore_ctx_))) {
    LOG_WARN("failed to init sys tablets restore task", K(ret), K(tablet_restore_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletRestoreDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore dag do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ha_dag_net_ctx_));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner reset status for retry failed", K(ret), KPC(ha_dag_net_ctx_));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret));
  } else {
    LOG_INFO("start retry", KPC(this));
    result_mgr_.reuse();
    if (!tablet_restore_ctx_.is_leader_) {
      if (OB_FAIL(tablet_restore_ctx_.ha_table_info_mgr_->remove_tablet_table_info(tablet_restore_ctx_.tablet_id_))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to remove tablet info", K(ret), K(tablet_restore_ctx_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_SUCC(ret)) {
        tablet_restore_ctx_.tablet_handle_.reset();
        if (OB_ISNULL(ls = ls_handle_.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls should not be NULL", K(ret), K(tablet_restore_ctx_));
        } else if (OB_FAIL(ls->ha_get_tablet(tablet_restore_ctx_.tablet_id_, tablet_restore_ctx_.tablet_handle_))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            const ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::TABLET_NOT_EXIST;
            if (OB_FAIL(tablet_restore_ctx_.set_copy_tablet_status(status))) {
              LOG_WARN("failed to set copy tablet status", K(ret), K(status));
            } else {
              FLOG_INFO("tablet in dest is deleted, set copy status not exist", K(tablet_restore_ctx_));
            }
          } else {
            LOG_WARN("failed to get tablet", K(ret), K(tablet_restore_ctx_));
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObTabletRestoreDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObTabletRestoreDag *tablet_restore_dag = nullptr;
  bool need_set_failed_result = true;
  ObTabletID tablet_id;
  ObInitTabletRestoreParam param;
  ObDagId dag_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore dag do not init", K(ret));
  } else if (OB_ISNULL(tablet_group_ctx_)) {
    ret = OB_ITER_END;
    need_set_failed_result = false;
    LOG_INFO("tablet restore dag not has next dag", KPC(this));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ha_dag_net_ctx_));
      ret = tmp_ret;
    }
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_group_ctx_->get_next_tablet_id(tablet_id))) {
        if (OB_ITER_END == ret) {
          //do nothing
          need_set_failed_result = false;
        } else {
          LOG_WARN("failed to get next tablet id", K(ret), KPC(this));
        }
      } else {
        param.tenant_id_ = tablet_restore_ctx_.tenant_id_;
        param.ls_id_ = tablet_restore_ctx_.ls_id_;
        param.tablet_id_ = tablet_id;
        param.ha_dag_net_ctx_ = ha_dag_net_ctx_;
        param.is_leader_ = tablet_restore_ctx_.is_leader_;
        param.action_ = tablet_restore_ctx_.action_;
        param.ha_table_info_mgr_ = tablet_restore_ctx_.ha_table_info_mgr_;
        param.restore_base_info_ = tablet_restore_ctx_.restore_base_info_;
        param.meta_index_store_ = tablet_restore_ctx_.meta_index_store_;
        param.second_meta_index_store_ = tablet_restore_ctx_.second_meta_index_store_;
        param.tablet_group_ctx_ = tablet_group_ctx_;

        if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
        } else if (OB_FAIL(scheduler->alloc_dag(tablet_restore_dag))) {
          LOG_WARN("failed to alloc tablet restore dag", K(ret));
        } else if (OB_FAIL(tablet_restore_dag->init(param))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            //overwrite ret
            LOG_INFO("tablet is deleted, skip restore", K(tablet_id), K(param));
            scheduler->free_dag(*tablet_restore_dag);
            tablet_restore_dag = nullptr;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to init tablet restore dag", K(ret), K(param));
          }
        } else if (FALSE_IT(dag_id.init(MYADDR))) {
        } else if (OB_FAIL(tablet_restore_dag->set_dag_id(dag_id))) {
          LOG_WARN("failed to set dag id", K(ret), K(param));
        } else {
          LOG_INFO("succeed generate next dag", KPC(tablet_restore_dag));
          dag = tablet_restore_dag;
          tablet_restore_dag = nullptr;
          break;
        }
      }
    }
  }

  if (OB_NOT_NULL(tablet_restore_dag)) {
    scheduler->free_dag(*tablet_restore_dag);
    tablet_restore_dag = nullptr;
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


/******************ObTabletRestoreTask*********************/
ObTabletRestoreTask::ObTabletRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    tablet_restore_ctx_(nullptr),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    storage_rpc_(nullptr),
    ls_(nullptr),
    src_info_(),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    copy_table_key_array_(),
    copy_sstable_info_mgr_()

{
}

ObTabletRestoreTask::~ObTabletRestoreTask()
{
}

int ObTabletRestoreTask::init(ObTabletRestoreCtx &tablet_restore_ctx)
{
  int ret = OB_SUCCESS;
  ObTabletRestoreDag *tablet_restore_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet restore task init twice", K(ret));
  } else if (!tablet_restore_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet restore task init get invalid argument", K(ret), K(tablet_restore_ctx));
  } else if (FALSE_IT(tablet_restore_dag = static_cast<ObTabletRestoreDag *>(this->get_dag()))) {
  } else {
    ha_dag_net_ctx_ = tablet_restore_dag->get_ha_dag_net_ctx();
    bandwidth_throttle_ = tablet_restore_dag->get_bandwidth_throttle();
    svr_rpc_proxy_ = tablet_restore_dag->get_storage_rpc_proxy();
    storage_rpc_ = tablet_restore_dag->get_storage_rpc();
    tablet_restore_ctx_ = &tablet_restore_ctx;
    ls_ = tablet_restore_dag->get_ls();
    is_inited_ = true;
  }
  return ret;
}

int ObTabletRestoreTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_exist = false;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  LOG_INFO("start do tablet restore task", KPC(tablet_restore_ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret), KPC(tablet_restore_ctx_));
  } else if (ha_dag_net_ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(get_src_info_())) {
    LOG_WARN("failed to get src info", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(try_update_tablet_())) {
    LOG_WARN("failed to try update tablet", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(tablet_restore_ctx_->get_copy_tablet_status(status))) {
    LOG_WARN("failed to get copy tablet status", K(ret), KPC(tablet_restore_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("copy tablet is not exist, skip copy it", KPC(tablet_restore_ctx_));
    if (OB_FAIL(update_ha_status_(status))) {
      LOG_WARN("failed to update ha expected status", K(ret), KPC(tablet_restore_ctx_));
    }
  } else if (OB_FAIL(build_copy_table_key_info_())) {
    LOG_WARN("failed to build copy table key info", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(build_copy_sstable_info_mgr_())) {
    LOG_WARN("failed to build copy sstable info mgr", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_restore_tasks_())) {
    LOG_WARN("failed to generate restore tasks", K(ret), KPC(tablet_restore_ctx_));
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret) && tablet_restore_ctx_->is_leader_
      && ObTabletRestoreAction::is_restore_minor(tablet_restore_ctx_->action_)
      && tablet_restore_ctx_->ls_id_.is_user_ls()) {
    SERVER_EVENT_SYNC_ADD("storage_ha", "follower_restore_major_errsim", "tablet_id", tablet_restore_ctx_->tablet_id_.id());
    DEBUG_SYNC(AFTER_RESTORE_TABLET_TASK);
    ret = OB_E(EventTable::EN_RESTORE_TABLET_TASK_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to do restore tablet task.", K(ret));
    }
  }
#endif

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(tablet_restore_ctx_));
    }
  }
  return ret;
}

int ObTabletRestoreTask::generate_restore_tasks_()
{
  int ret = OB_SUCCESS;
  ObITask *parent_task = this;
  ObTabletCopyFinishTask *tablet_copy_finish_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(tablet_restore_ctx_->replica_type_)) {
    LOG_INFO("no need to generate restore task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(check_src_sstable_exist_())) {
    LOG_WARN("failed to check src sstable exist", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_tablet_copy_finish_task_(tablet_copy_finish_task))) {
    LOG_WARN("failed to generate tablet copy finish task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_mds_restore_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate restore mds tasks", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_minor_restore_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate restore minor tasks", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_major_restore_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate restore major tasks", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_ddl_restore_tasks_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate ddl copy tasks", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_finish_restore_task_(tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate finish restore task", K(ret), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::generate_minor_restore_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_remote_sstable_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (OB_ISNULL(tablet = tablet_restore_ctx_->tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (!ObTabletRestoreAction::is_restore_minor(tablet_restore_ctx_->action_)
      && !ObTabletRestoreAction::is_restore_all(tablet_restore_ctx_->action_)) {
    LOG_INFO("tablet not restore minor or restore all, skip minor restore tasks",
        KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_restore_task_(ObITable::is_minor_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate minor restore task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::generate_major_restore_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (!ObTabletRestoreAction::is_restore_major(tablet_restore_ctx_->action_)
      && !ObTabletRestoreAction::is_restore_all(tablet_restore_ctx_->action_)) {
    LOG_INFO("tablet is not restore major or restore all, skip major restore tasks", K(ret),
        KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_restore_task_(ObITable::is_major_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate major restore task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  }
  return ret;
}

//TODO(muwei.ym) reconsider ddl sstable in 4.3
int ObTabletRestoreTask::generate_ddl_restore_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore ddl task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (!ObTabletRestoreAction::is_restore_minor(tablet_restore_ctx_->action_)) {
    LOG_INFO("tablet not restore minor, skip ddl restore tasks",
        K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_restore_task_(ObITable::is_ddl_dump_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate ddl restore task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::generate_physical_restore_task_(
    const ObITable::TableKey &copy_table_key,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    ObITask *parent_task,
    ObITask *child_task)
{
  int ret = OB_SUCCESS;
  ObPhysicalCopyTask *copy_task = NULL;
  ObSSTableCopyFinishTask *finish_task = NULL;
  const int64_t task_idx = 0;
  ObPhysicalCopyTaskInitParam init_param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (!copy_table_key.is_valid() || OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate physical copy task get invalid argument", K(ret), K(copy_table_key), KP(tablet_copy_finish_task),
        KP(parent_task), KP(child_task));
  } else if (FALSE_IT(init_param.tenant_id_ = tablet_restore_ctx_->tenant_id_)) {
  } else if (FALSE_IT(init_param.ls_id_ = tablet_restore_ctx_->ls_id_)) {
  } else if (FALSE_IT(init_param.tablet_id_ = tablet_restore_ctx_->tablet_id_)) {
  } else if (FALSE_IT(init_param.src_info_ = src_info_)) {
  } else if (FALSE_IT(init_param.tablet_copy_finish_task_ = tablet_copy_finish_task)) {
  } else if (FALSE_IT(init_param.ls_ = ls_)) {
  } else if (FALSE_IT(init_param.is_leader_restore_ = tablet_restore_ctx_->is_leader_)) {
  } else if (FALSE_IT(init_param.restore_base_info_ = tablet_restore_ctx_->restore_base_info_)) {
  } else if (FALSE_IT(init_param.meta_index_store_ = tablet_restore_ctx_->meta_index_store_)) {
  } else if (FALSE_IT(init_param.second_meta_index_store_ = tablet_restore_ctx_->second_meta_index_store_)) {
  } else if (FALSE_IT(init_param.need_sort_macro_meta_ = !copy_table_key.is_normal_cg_sstable())) {
  } else if (FALSE_IT(init_param.need_check_seq_ = tablet_restore_ctx_->need_check_seq_)) {
  } else if (FALSE_IT(init_param.ls_rebuild_seq_ = tablet_restore_ctx_->ls_rebuild_seq_)) {
  } else if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->get_table_info(tablet_restore_ctx_->tablet_id_,
      copy_table_key, init_param.sstable_param_))) {
    LOG_WARN("failed to get table info", K(ret), KPC(tablet_restore_ctx_), K(copy_table_key));
  } else if (OB_FAIL(copy_sstable_info_mgr_.get_copy_sstable_maro_range_info(copy_table_key, init_param.sstable_macro_range_info_))) {
    LOG_WARN("failed to get copy sstable macro range info", K(ret), K(copy_table_key));
  } else if (!init_param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical copy task init param not valid", K(ret), K(init_param), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(finish_task->init(init_param))) {
    LOG_WARN("failed to init finish task", K(ret), K(init_param), K(copy_table_key), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  } else if (init_param.sstable_macro_range_info_.copy_macro_range_array_.count() > 0) {
    // parent->copy->finish->child
    if (OB_FAIL(dag_->alloc_task(copy_task))) {
      LOG_WARN("failed to alloc copy task", K(ret));
    } else if (OB_FAIL(copy_task->init(finish_task->get_copy_ctx(), finish_task, 0))) {
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
          K(copy_table_key), K(src_info_), KPC(copy_task), KPC(finish_task));
    }
  }
  return ret;
}

int ObTabletRestoreTask::generate_finish_restore_task_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    ObITask *parent_task)
{
  int ret = OB_SUCCESS;
  ObTabletFinishRestoreTask *tablet_finish_restore_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate finish restore task get invalid argument", K(ret),
        KP(tablet_copy_finish_task), KP(parent_task));
  } else if (OB_FAIL(dag_->alloc_task(tablet_finish_restore_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(tablet_finish_restore_task->init(*tablet_restore_ctx_))) {
    LOG_WARN("failed to init finish task", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(tablet_copy_finish_task->add_child(*tablet_finish_restore_task))) {
    LOG_WARN("failed to add child finish_task for parent", K(ret), KPC(tablet_finish_restore_task));
  } else if (OB_FAIL(parent_task->add_child(*tablet_copy_finish_task))) {
    LOG_WARN("failed to add tablet copy finish task as child", K(ret), KPC(tablet_finish_restore_task));
  } else if (OB_FAIL(dag_->add_task(*tablet_copy_finish_task))) {
    LOG_WARN("failed to add tablet copy finish task", K(ret), KPC(tablet_copy_finish_task));
  } else if (OB_FAIL(dag_->add_task(*tablet_finish_restore_task))) {
    LOG_WARN("failed to add tablet finish restore task to dag", K(ret));
  } else {
    FLOG_INFO("succeed to generate tablet finish restore task", KPC(tablet_finish_restore_task));
  }
  return ret;
}

int ObTabletRestoreTask::get_src_info_()
{
  int ret = OB_SUCCESS;
  src_info_.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (ObIHADagNetCtx::LS_RESTORE == ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ObLSRestoreCtx *ctx = static_cast<ObLSRestoreCtx *>(ha_dag_net_ctx_);
    src_info_ = ctx->src_;
    need_check_seq_ = ctx->need_check_seq_;
    ls_rebuild_seq_ = ctx->ls_rebuild_seq_;
  } else if (ObIHADagNetCtx::TABLET_GROUP_RESTORE == ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ObTabletGroupRestoreCtx *ctx = static_cast<ObTabletGroupRestoreCtx *>(ha_dag_net_ctx_);
    src_info_ = ctx->src_;
    need_check_seq_ = ctx->need_check_seq_;
    ls_rebuild_seq_ = ctx->ls_rebuild_seq_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag net ctx type is unexpected", K(ret), KPC(ha_dag_net_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::generate_restore_task_(
    IsRightTypeSSTableFunc is_right_type_sstable,
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate restore task get invalid argument",
        K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < copy_table_key_array_.count(); ++i) {
      const ObITable::TableKey &copy_table_key = copy_table_key_array_.at(i);
      ObFakeTask *wait_finish_task = nullptr;
      bool need_copy = true;
      const bool is_right_type = is_right_type_sstable(copy_table_key.table_type_);

      if (!copy_table_key.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy table key info is invalid", K(ret), K(copy_table_key));
      } else if (!is_right_type) {
        //do nothing
      } else {
        if (OB_FAIL(check_need_copy_sstable_(copy_table_key, need_copy))) {
          LOG_WARN("failed to check need copy sstable", K(ret), K(copy_table_key));
        } else if (!need_copy) {
          LOG_INFO("local contains the sstable, no need copy", K(copy_table_key));
        } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
          LOG_WARN("failed to alloc wait finish task", K(ret));
        } else if (OB_FAIL(generate_physical_restore_task_(copy_table_key,
            tablet_copy_finish_task, parent_task, wait_finish_task))) {
          LOG_WARN("failed to generate physical restore task", K(ret), K(copy_table_key));
        } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
          LOG_WARN("failed to add wait finish task", K(ret));
        } else {
          parent_task = wait_finish_task;
          LOG_INFO("succeed to generate sstable restore task", "is_leader",
              tablet_restore_ctx_->is_leader_, "src", src_info_, K(copy_table_key),
              "restore_action", tablet_restore_ctx_->action_, K(is_right_type),
              K(copy_table_key_array_), K(i));
        }
      }
    }
  }
  return ret;
}

int ObTabletRestoreTask::build_copy_table_key_info_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->get_table_keys(
      tablet_restore_ctx_->tablet_id_, copy_table_key_array_))) {
    LOG_WARN("failed to get copy table keys", K(ret), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::build_copy_sstable_info_mgr_()
{
  int ret = OB_SUCCESS;
  ObStorageHACopySSTableParam param;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_FAIL(param.copy_table_key_array_.assign(copy_table_key_array_))) {
    LOG_WARN("failed to assign copy table key info array", K(ret), K(copy_table_key_array_));
  } else {
    param.tenant_id_ = tablet_restore_ctx_->tenant_id_;
    param.ls_id_ = tablet_restore_ctx_->ls_id_;
    param.tablet_id_ = tablet_restore_ctx_->tablet_id_;
    param.is_leader_restore_ = tablet_restore_ctx_->is_leader_;
    param.local_rebuild_seq_ = ls_rebuild_seq_;
    param.need_check_seq_ = need_check_seq_;
    param.meta_index_store_ = tablet_restore_ctx_->meta_index_store_;
    param.second_meta_index_store_ = tablet_restore_ctx_->second_meta_index_store_;
    param.restore_base_info_ = tablet_restore_ctx_->restore_base_info_;
    param.src_info_ = src_info_;
    param.storage_rpc_ = storage_rpc_;
    param.svr_rpc_proxy_ = svr_rpc_proxy_;
    param.bandwidth_throttle_ = bandwidth_throttle_;

    if (OB_FAIL(copy_sstable_info_mgr_.init(param))) {
      LOG_WARN("failed to init copy sstable info mgr", K(ret), K(param), KPC(tablet_restore_ctx_));
    }
  }
  return ret;
}

int ObTabletRestoreTask::generate_tablet_copy_finish_task_(
    ObTabletCopyFinishTask *&tablet_copy_finish_task)
{
  int ret = OB_SUCCESS;
  tablet_copy_finish_task = nullptr;
  observer::ObIMetaReport *reporter = GCTX.ob_service_;
  const ObMigrationTabletParam *src_tablet_meta = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(tablet_copy_finish_task))) {
    LOG_WARN("failed to alloc tablet copy finish task", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->get_tablet_meta(
      tablet_restore_ctx_->tablet_id_, src_tablet_meta))) {
    LOG_WARN("failed to get src tablet meta", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(tablet_copy_finish_task->init(tablet_restore_ctx_->tablet_id_, ls_, reporter,
      tablet_restore_ctx_->action_, src_tablet_meta, tablet_restore_ctx_))) {
    LOG_WARN("failed to init tablet copy finish task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::try_update_tablet_()
{
  int ret = OB_SUCCESS;
  ObTabletRestoreDag *dag = nullptr;
  int32_t retry_count = 0;
  ObStorageHATabletsBuilder ha_tablets_builder;
  ObSEArray<ObTabletID, 1> tablet_id_array;
  ObLS *ls = nullptr;
  bool is_exist = false;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObTabletRestoreDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet migration dag should not be NULL", K(ret), KP(dag));
  } else if (OB_FAIL(tablet_restore_ctx_->get_copy_tablet_status(status))) {
    LOG_WARN("failed to get copy tablet status", K(ret), KPC(dag), KPC(tablet_restore_ctx_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    //do nothing
  } else if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->check_tablet_table_info_exist(
      tablet_restore_ctx_->tablet_id_, is_exist))) {
    LOG_WARN("failed to check tablet table info exist", K(ret), KPC(tablet_restore_ctx_));
  } else if (is_exist) {
    //do nothing
  } else if (OB_FAIL(tablet_id_array.push_back(tablet_restore_ctx_->tablet_id_))) {
    LOG_WARN("failed to push tablet id into array", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_ISNULL(ls = dag->get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(ObTabletGroupRestoreUtils::init_ha_tablets_builder(
      tablet_restore_ctx_->tenant_id_, tablet_id_array, tablet_restore_ctx_->is_leader_,
      need_check_seq_, ls_rebuild_seq_, src_info_,
      ls, tablet_restore_ctx_->restore_base_info_, tablet_restore_ctx_->action_,
      tablet_restore_ctx_->second_meta_index_store_,
      tablet_restore_ctx_->ha_table_info_mgr_, ha_tablets_builder))) {
    LOG_WARN("failed to init ha tablets builder", K(ret), KPC(tablet_restore_ctx_));
  } else {
    if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->remove_tablet_table_info(tablet_restore_ctx_->tablet_id_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to remove tablet info", K(ret), KPC(tablet_restore_ctx_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (tablet_restore_ctx_->tablet_id_.is_ls_inner_tablet() && OB_FAIL(ha_tablets_builder.create_or_update_tablets())) {
      LOG_WARN("failed to create or update inner tablet", K(ret), KPC(tablet_restore_ctx_));
    } else if (OB_FAIL(ha_tablets_builder.build_tablets_sstable_info())) {
      LOG_WARN("failed to build tablets sstable info", K(ret), KPC(tablet_restore_ctx_));
    } else if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->check_tablet_table_info_exist(
        tablet_restore_ctx_->tablet_id_, is_exist))) {
      LOG_WARN("failed to check tablet table info exist", K(ret), KPC(tablet_restore_ctx_));
    } else if (!is_exist) {
      status = ObCopyTabletStatus::TABLET_NOT_EXIST;
      if (OB_FAIL(tablet_restore_ctx_->set_copy_tablet_status(status))) {
        LOG_WARN("failed to set copy tablet status", K(ret), KPC(dag));
      }
    }
  }

  return ret;
}

int ObTabletRestoreTask::update_ha_status_(
    const ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (status != ObCopyTabletStatus::TABLET_NOT_EXIST) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("update ha meta status get invalid argument", K(ret), K(status));
  } else if (OB_FAIL(update_deleted_and_undefine_tablet(*ls_, tablet_restore_ctx_->tablet_id_))) {
    LOG_WARN("failed to update deleted and undefine tablet", K(ret), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::check_need_copy_sstable_(
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
  } else if (OB_FAIL(tablet_restore_ctx_->ha_table_info_mgr_->get_table_info(
      tablet_restore_ctx_->tablet_id_, table_key, copy_table_info))) {
    LOG_WARN("failed to get table info", K(ret), KPC(tablet_restore_ctx_), K(table_key));
  } else if (OB_FAIL(ObStorageHATaskUtils::check_need_copy_sstable(*copy_table_info, tablet_restore_ctx_->tablet_handle_, need_copy))) {
    LOG_WARN("failed to check need copy sstable", K(ret), KPC(tablet_restore_ctx_), K(table_key));
    if (OB_INVALID_DATA == ret) {
      LOG_ERROR("restore invalid data", K(ret), K(table_key), KPC(tablet_restore_ctx_));
      abort(); // TODO@wenqu: remove this line
    }
  }
  return ret;
}

int ObTabletRestoreTask::check_src_sstable_exist_()
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_major_sstable_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet migration task do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_restore_ctx_->tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KPC(tablet_restore_ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < copy_table_key_array_.count(); ++i) {
      const ObITable::TableKey &table_key = copy_table_key_array_.at(i);
      if (table_key.is_major_sstable()) {
        is_major_sstable_exist = true;
      } else if (table_key.is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("copy table key should not contain remote logical minor sstable, unexpected",
            K(ret), K(copy_table_key_array_));
      }
    }

    if (OB_SUCC(ret)) {
      if (ObTabletRestoreAction::is_restore_all(tablet_restore_ctx_->action_)) {
        if (!is_major_sstable_exist && tablet->get_tablet_meta().table_store_flag_.with_major_sstable()) {
          ret = OB_SSTABLE_NOT_EXIST;
          LOG_WARN("src restore sstable do not exist", K(ret), K(copy_table_key_array_), KPC(tablet_restore_ctx_));
        }
      } else if (ObTabletRestoreAction::is_restore_major(tablet_restore_ctx_->action_)) {
        if (!is_major_sstable_exist && tablet->get_tablet_meta().table_store_flag_.with_major_sstable()) {
          ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
          if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
            LOG_WARN("fail to fetch table store", K(ret));
          } else if (table_store_wrapper.get_member()->get_major_sstables().empty()) {
            ret = OB_SSTABLE_NOT_EXIST;
            LOG_WARN("src restore sstable do not exist", K(ret), K(copy_table_key_array_), KPC(tablet_restore_ctx_));
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletRestoreTask::generate_mds_restore_tasks_(
    ObTabletCopyFinishTask *tablet_copy_finish_task,
    share::ObITask *&parent_task)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_remote_sstable_exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet restore task do not init", K(ret));
  } else if (OB_ISNULL(tablet_copy_finish_task) || OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate minor task get invalid argument", K(ret), KP(tablet_copy_finish_task), KP(parent_task));
  } else if (OB_ISNULL(tablet = tablet_restore_ctx_->tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (!ObTabletRestoreAction::is_restore_minor(tablet_restore_ctx_->action_)
      && !ObTabletRestoreAction::is_restore_all(tablet_restore_ctx_->action_)) {
    LOG_INFO("tablet not restore minor or restore all, skip minor restore tasks",
        KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(generate_restore_task_(ObITable::is_mds_sstable, tablet_copy_finish_task, parent_task))) {
    LOG_WARN("failed to generate mds restore task", K(ret), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
  }
  return ret;
}

int ObTabletRestoreTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_restore_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "tablet_restore_task",
      "tenant_id", tablet_restore_ctx_->tenant_id_,
      "ls_id", tablet_restore_ctx_->ls_id_.id(),
      "tablet_id", tablet_restore_ctx_->tablet_id_.id(),
      "action", ObTabletRestoreAction::get_action_str(tablet_restore_ctx_->action_),
      "is_leader", tablet_restore_ctx_->is_leader_);
  }
  return ret;
}

/******************ObTabletFinishRestoreTask*********************/
ObTabletFinishRestoreTask::ObTabletFinishRestoreTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    tablet_restore_ctx_(nullptr),
    ls_(nullptr)
{
}

ObTabletFinishRestoreTask::~ObTabletFinishRestoreTask()
{
}

int ObTabletFinishRestoreTask::init(ObTabletRestoreCtx &tablet_restore_ctx)
{
  int ret = OB_SUCCESS;
  ObTabletRestoreDag *tablet_restore_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet restore task init twice", K(ret));
  } else if (!tablet_restore_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet restore task init get invalid argument", K(ret), K(tablet_restore_ctx));
  } else if (FALSE_IT(tablet_restore_dag = static_cast<ObTabletRestoreDag *>(this->get_dag()))) {
  } else {
    ha_dag_net_ctx_ = tablet_restore_dag->get_ha_dag_net_ctx();
    tablet_restore_ctx_ = &tablet_restore_ctx;
    ls_ = tablet_restore_dag->get_ls();
    is_inited_ = true;
  }
  return ret;
}

int ObTabletFinishRestoreTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LOG_INFO("start do tablet finish restore task", KPC(tablet_restore_ctx_));
  ObCopyTabletStatus::STATUS status;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish restore task do not init", K(ret), KPC(tablet_restore_ctx_));
  } else if (ha_dag_net_ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(tablet_restore_ctx_->get_copy_tablet_status(status))) {
    LOG_WARN("failed to get copy tablet status", K(ret));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    if (OB_FAIL(update_deleted_and_undefine_tablet(*ls_, tablet_restore_ctx_->tablet_id_))) {
      LOG_WARN("failed to update deleted and undefine tablet", K(ret), KPC(tablet_restore_ctx_));
    }
  } else if (OB_FAIL(update_restore_status_())) {
    LOG_WARN("failed to update restore status", K(ret), KPC(tablet_restore_ctx_));
  }

  if (OB_SUCCESS != (tmp_ret = record_server_event_())) {
    LOG_WARN("failed to record server event", K(tmp_ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, this->get_dag()))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), KPC(tablet_restore_ctx_));
    }
  }
  return ret;
}

int ObTabletFinishRestoreTask::update_restore_status_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish restore task do not init", K(ret));
  } else if (!ObTabletRestoreAction::is_restore_all(tablet_restore_ctx_->action_)
      && !ObTabletRestoreAction::is_restore_major(tablet_restore_ctx_->action_)) {
    //do nothing
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_restore_ctx_->tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), KPC(tablet_restore_ctx_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), KPC(tablet_restore_ctx_));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &minor_sstables = table_store_wrapper.get_member()->get_minor_sstables();
    const ObSSTableArray &major_sstables = table_store_wrapper.get_member()->get_major_sstables();
    for (int64_t i = 0; OB_SUCC(ret) && i < minor_sstables.count(); ++i) {
      const ObITable *table = minor_sstables[i];
      if (OB_ISNULL(table) || table->is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or tablet still has remote logical minor sstable, unexpected",
            K(ret), KP(table), KPC(tablet));
      }
    }

    if (OB_SUCC(ret)
        && tablet->get_tablet_meta().table_store_flag_.with_major_sstable()
        && major_sstables.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should has major sstable, unexpected", K(ret), K(tablet), K(major_sstables));
    }
  }
  if (OB_SUCC(ret)) {
    ObTabletRestoreStatus::STATUS tablet_restore_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
    if (OB_FAIL(ObTabletRestoreAction::trans_restore_action_to_restore_status(
        tablet_restore_ctx_->action_, tablet_restore_status))) {
      LOG_WARN("failed to trans restore action to restore status", K(ret), KPC(tablet_restore_ctx_));
    } else if (OB_FAIL(ls_->update_tablet_restore_status(tablet_restore_ctx_->tablet_id_,
                                                         tablet_restore_status,
                                                         false /* donot reset has transfer table flag */))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("restore tablet maybe deleted, skip it", K(ret), KPC(tablet_restore_ctx_));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to update tablet restore status", K(ret), KPC(tablet_restore_ctx_), K(tablet_restore_status));
      }
    } else {
      FLOG_INFO("succeed to update restore", K(tablet_restore_ctx_->tablet_id_),
          K(tablet_restore_ctx_->action_), K(tablet_restore_status));
    }
  }
  return ret;
}

int ObTabletFinishRestoreTask::record_server_event_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet_restore_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx should not be null", K(ret));
  } else {
    SERVER_EVENT_ADD("storage_ha", "tablet_finish_restore_task",
      "tenant_id", tablet_restore_ctx_->tenant_id_,
      "ls_id", tablet_restore_ctx_->ls_id_.id(),
      "tablet_id", tablet_restore_ctx_->tablet_id_.id(),
      "action", ObTabletRestoreAction::get_action_str(tablet_restore_ctx_->action_),
      "is_leader", tablet_restore_ctx_->is_leader_);
  }
  return ret;
}

/******************ObTabletGroupRestoreUtils*********************/
int ObTabletGroupRestoreUtils::init_ha_tablets_builder(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    const bool is_leader_restore,
    const bool need_check_seq,
    const int64_t ls_rebuild_seq,
    const ObStorageHASrcInfo src_info,
    ObLS *ls,
    const ObRestoreBaseInfo *restore_base_info,
    const ObTabletRestoreAction::ACTION &restore_action,
    backup::ObBackupMetaIndexStoreWrapper *meta_index_store,
    ObStorageHATableInfoMgr *ha_table_info_mgr,
    ObStorageHATabletsBuilder &ha_tablets_builder)
{
  int ret = OB_SUCCESS;
  ObStorageHATabletsBuilderParam param;
  ObLSService *ls_service = nullptr;

  if (OB_INVALID_ID == tenant_id || tablet_id_array.empty() || OB_ISNULL(ls)
      || (!is_leader_restore && !src_info.is_valid())
      || (is_leader_restore && (OB_ISNULL(restore_base_info) || OB_ISNULL(meta_index_store)))
      || !ObTabletRestoreAction::is_valid(restore_action)
      || OB_ISNULL(ha_table_info_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ha tablets builder get unexpected error", K(ret), K(tenant_id),
        K(tablet_id_array), KP(ls), K(src_info), K(is_leader_restore), KP(restore_base_info),
        KP(meta_index_store), KP(ha_table_info_mgr));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(param.tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    param.bandwidth_throttle_ = GCTX.bandwidth_throttle_;
    param.is_leader_restore_ = is_leader_restore;
    param.local_rebuild_seq_ = ls_rebuild_seq;
    param.need_check_seq_ = need_check_seq;
    param.ls_ = ls;
    param.meta_index_store_ = meta_index_store;
    param.restore_base_info_ = restore_base_info;
    param.restore_action_ = restore_action;
    param.src_info_ = src_info;
    param.storage_rpc_ = ls_service->get_storage_rpc();
    param.svr_rpc_proxy_ = ls_service->get_storage_rpc_proxy();
    param.tenant_id_ = tenant_id;
    param.ha_table_info_mgr_ = ha_table_info_mgr;
    param.need_keep_old_tablet_ = false;

    if (OB_FAIL(ha_tablets_builder.init(param))) {
      LOG_WARN("failed to init ha tablets builder", K(ret), K(param));
    }
  }
  return ret;
}


}
}

