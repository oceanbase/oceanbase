/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_backup_archive_piece_copier.h"
#include "share/ob_get_compat_mode.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/location_cache/ob_location_service.h"
#include "share/config/ob_server_config.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace share;
namespace backup {


/* ObBackupArchivePieceCopyParam */
ObBackupArchivePieceCopyParam::ObBackupArchivePieceCopyParam()
  : trace_id_(),
    job_id_(0),
    tenant_id_(0),
    dest_id_(0),
    round_id_(0),
    piece_id_(0),
    backup_path_()
{
}

bool ObBackupArchivePieceCopyParam::is_valid() const
{
  return !trace_id_.is_invalid()
      && job_id_ > 0
      && tenant_id_ > 0
      && dest_id_ > 0
      && round_id_ > 0
      && piece_id_ > 0
      && !backup_path_.is_empty();
}

int ObBackupArchivePieceCopyParam::assign(const ObBackupArchivePieceCopyParam &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret), K(other));
  } else if (OB_FAIL(backup_path_.assign(other.backup_path_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to assign backup path", K(ret), K(other));
  } else {
    trace_id_ = other.trace_id_;
    job_id_ = other.job_id_;
    tenant_id_ = other.tenant_id_;
    dest_id_ = other.dest_id_;
    round_id_ = other.round_id_;
    piece_id_ = other.piece_id_;
  }
  return ret;
}

int ObBackupArchivePieceCopyParam::set(const obrpc::ObNotifyBackupArchiveArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(backup_path_.assign(arg.backup_path_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to assign backup path", K(ret), K(arg));
  } else {
    trace_id_ = arg.trace_id_;
    job_id_ = arg.job_id_;
    tenant_id_ = arg.tenant_id_;
    dest_id_ = arg.archive_dest_id_;
    round_id_ = arg.round_id_;
    piece_id_ = arg.piece_id_;
  }
  return ret;
}


/* ObBackupArchivePieceCopyDagNet */
ObBackupArchivePieceCopyDagNet::ObBackupArchivePieceCopyDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_BACKUP_ARCHIVE),
    is_inited_(false),
    param_()
{
}

int ObBackupArchivePieceCopyDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObBackupArchivePieceCopyParam *init_param = static_cast<const ObBackupArchivePieceCopyParam *>(param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag net init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(this->set_dag_id(init_param->trace_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to set dag id", K(ret), KPC(init_param));
  } else if (OB_FAIL(param_.assign(*init_param))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to assign param", K(ret), KPC(init_param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchivePieceCopyDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObBackupArchivePieceCopyDag *copy_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag net not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]failed to get tenant dag scheduler", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(copy_dag))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to alloc piece copy dag", K(ret));
  } else if (OB_FAIL(copy_dag->init_by_param(&param_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init piece copy dag", K(ret), K_(param));
  } else if (OB_FAIL(add_dag_into_dag_net(*copy_dag))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to add piece copy dag into dag net", K(ret), K_(param));
  } else if (OB_FAIL(copy_dag->create_first_task())) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to create first task", K(ret), K_(param));
  } else if (OB_FAIL(scheduler->add_dag(copy_dag))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to add piece copy dag", K(ret), KPC(copy_dag));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]success to start piece copy dag net", KPC(copy_dag));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(copy_dag)) {
    scheduler->free_dag(*copy_dag);
    copy_dag = nullptr;
  }
  return ret;
}

bool ObBackupArchivePieceCopyDagNet::operator==(const ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupArchivePieceCopyDagNet &other_dag_net = static_cast<const ObBackupArchivePieceCopyDagNet &>(other);
    bret = param_.tenant_id_ == other_dag_net.param_.tenant_id_
        && param_.job_id_ == other_dag_net.param_.job_id_
        && param_.dest_id_ == other_dag_net.param_.dest_id_
        && param_.round_id_ == other_dag_net.param_.round_id_
        && param_.piece_id_ == other_dag_net.param_.piece_id_
        && param_.trace_id_ == other_dag_net.param_.trace_id_;
  }
  return bret;
}

uint64_t ObBackupArchivePieceCopyDagNet::hash() const
{
  uint64_t hash_value = 0;
  const int64_t type = get_type();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_.tenant_id_, sizeof(param_.tenant_id_), hash_value);
  hash_value = common::murmurhash(&param_.job_id_, sizeof(param_.job_id_), hash_value);
  hash_value = common::murmurhash(&param_.dest_id_, sizeof(param_.dest_id_), hash_value);
  hash_value = common::murmurhash(&param_.round_id_, sizeof(param_.round_id_), hash_value);
  hash_value = common::murmurhash(&param_.piece_id_, sizeof(param_.piece_id_), hash_value);
  hash_value = common::murmurhash(&param_.trace_id_, sizeof(param_.trace_id_), hash_value);
  return hash_value;
}

int ObBackupArchivePieceCopyDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char trace_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag net not init", K(ret));
  } else if (OB_UNLIKELY(0 > param_.trace_id_.to_string(trace_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("[BACKUP_ARCHIVE]failed to get trace id string", K(ret), K_(param));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "tenant_id=%lu, job_id=%ld, dest_id=%ld, round_id=%ld, piece_id=%ld, trace_id=%s",
          param_.tenant_id_, param_.job_id_, param_.dest_id_, param_.round_id_, param_.piece_id_,
          trace_id_str))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to fill comment", K(ret), K_(param));
  }
  return ret;
}

int ObBackupArchivePieceCopyDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag net not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "tenant_id=%lu, job_id=%ld, dest_id=%ld, round_id=%ld, piece_id=%ld",
          param_.tenant_id_, param_.job_id_, param_.dest_id_, param_.round_id_, param_.piece_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to fill dag net key", K(ret), K_(param));
  }
  return ret;
}


/* ObBackupArchivePieceCopyDag */
ObBackupArchivePieceCopyDag::ObBackupArchivePieceCopyDag()
  : ObIDag(ObDagType::DAG_TYPE_BACKUP_ARCHIVE_PIECE_COPY),
    is_inited_(false),
    param_(),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    ctx_(param_)
{
}

ObBackupArchivePieceCopyDag::~ObBackupArchivePieceCopyDag()
{
}

int ObBackupArchivePieceCopyDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObBackupArchivePieceCopyParam *init_param = static_cast<const ObBackupArchivePieceCopyParam *>(param);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == init_param || !init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid init param", K(ret), KPC(init_param));
  } else if (OB_FAIL(param_.assign(*init_param))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to assign param", K(ret), KPC(init_param));
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(param_.tenant_id_, compat_mode_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get compat mode", K(ret), K_(param));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchivePieceCopyDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupArchivePieceCopyPrepareTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag not init", K(ret));
  } else if (OB_FAIL(create_task(nullptr/*parent*/, task, ctx_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to create prepare task", K(ret), K_(param));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]success to add piece copy prepare task", K_(param));
  }
  return ret;
}

int ObBackupArchivePieceCopyDag::generate_copy_tasks(ObITask *prepare_task)
{
  int ret = OB_SUCCESS;
  ObBackupArchivePieceCopyTask *copy_task = nullptr;
  ObBackupArchivePieceCopyFinishTask *finish_task = nullptr;
  const int64_t ls_cnt = ctx_.ls_ids_.count();
  if (OB_ISNULL(prepare_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret), KP(prepare_task));
  } else if (0 == ls_cnt) {
    if (OB_FAIL(create_task(prepare_task/*parent*/, finish_task, ctx_))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to create finish task", K(ret), K_(param));
    }
  } else if (OB_FAIL(create_task(prepare_task/*parent*/, copy_task, 0/*idx*/, ctx_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to create copy task", K(ret), K_(param));
  } else if (OB_ISNULL(copy_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]copy task is unexpected null", K(ret), K_(param));
  } else if (OB_FAIL(create_task(copy_task/*parent*/, finish_task, ctx_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to create finish task", K(ret), K_(param));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]success to generate copy tasks", K_(param), K(ls_cnt));
  }
  return ret;
}

bool ObBackupArchivePieceCopyDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObBackupArchivePieceCopyDag &other_dag = static_cast<const ObBackupArchivePieceCopyDag &>(other);
    bret = param_.tenant_id_ == other_dag.param_.tenant_id_
        && param_.job_id_ == other_dag.param_.job_id_
        && param_.dest_id_ == other_dag.param_.dest_id_
        && param_.round_id_ == other_dag.param_.round_id_
        && param_.piece_id_ == other_dag.param_.piece_id_
        && param_.trace_id_ == other_dag.param_.trace_id_;
  }
  return bret;
}

uint64_t ObBackupArchivePieceCopyDag::hash() const
{
  uint64_t hash_value = 0;
  const int64_t type = get_type();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value = common::murmurhash(&param_.tenant_id_, sizeof(param_.tenant_id_), hash_value);
  hash_value = common::murmurhash(&param_.job_id_, sizeof(param_.job_id_), hash_value);
  hash_value = common::murmurhash(&param_.dest_id_, sizeof(param_.dest_id_), hash_value);
  hash_value = common::murmurhash(&param_.round_id_, sizeof(param_.round_id_), hash_value);
  hash_value = common::murmurhash(&param_.piece_id_, sizeof(param_.piece_id_), hash_value);
  hash_value = common::murmurhash(&param_.trace_id_, sizeof(param_.trace_id_), hash_value);
  return hash_value;
}

int ObBackupArchivePieceCopyDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy dag not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                 static_cast<int64_t>(param_.tenant_id_), param_.dest_id_, param_.round_id_, param_.piece_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to add dag warning info param", K(ret));
  }
  return ret;
}

int ObBackupArchivePieceCopyDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos,
          "tenant_id=%lu, dest_id=%ld, round_id=%ld, piece_id=%ld",
          param_.tenant_id_, param_.dest_id_, param_.round_id_, param_.piece_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to fill dag key", K(ret), K_(param));
  }
  return ret;
}


/* ObBackupArchivePieceCopyPrepareTask */
int ObBackupArchivePieceCopyPrepareTask::init(ObBackupArchivePieceCopyCtx &copy_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]prepare task init twice", K(ret));
  } else {
    ctx_ = &copy_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchivePieceCopyPrepareTask::process()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]prepare task not init", K(ret));
  } else if (OB_FAIL(ctx_->init())) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init ctx", K(ret));
  } else if (OB_FAIL(ctx_->copy_round_files())) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy round files", K(ret));
  } else if (OB_FAIL(ctx_->copy_piece_placeholder(false/*copy_end_file*/))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy piece start placeholder", K(ret));
  } else if (OB_FAIL(ctx_->copy_dir_recursively(ctx_->src_piece_dir_, ctx_->dst_piece_dir_, ctx_->allocator_,
                                                     ctx_->buf_, ObBackupArchivePieceCopyCtx::MAX_PART_SIZE, true/*skip ls subdir*/))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy piece dir", K(ret), KPC(ctx_));
  } else if (OB_UNLIKELY(nullptr == dag_ || ObDagType::DAG_TYPE_BACKUP_ARCHIVE_PIECE_COPY != dag_->get_type())) {
    ret = OB_ERR_SYS;
    LOG_WARN("[BACKUP_ARCHIVE]dag type not match", K(ret), KPC_(dag));
  } else if (OB_FAIL(static_cast<ObBackupArchivePieceCopyDag *>(dag_)->generate_copy_tasks(this/*prepare_task*/))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to generate copy tasks", K(ret));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(ctx_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_ARCHIVE]copy ctx should not be null", K(tmp_ret), K(ret));
    } else if (OB_TMP_FAIL(ObBackupArchiveHandler::report_piece_copy_result(ctx_->param_, ret))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to report piece copy result", K(tmp_ret), K(ret));
    }
    if (OB_CANCELED == ret) {
      LOG_INFO("[BACKUP_ARCHIVE]job has been canceled", K(ret), KPC(ctx_));
    }
  }
  return ret;
}


/* ObBackupArchivePieceCopyTask */
int ObBackupArchivePieceCopyTask::init(const int64_t idx, ObBackupArchivePieceCopyCtx &copy_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy task init twice", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= copy_ctx.ls_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret), K(idx), K(copy_ctx));
  } else {
    ctx_ = &copy_ctx;
    idx_ = idx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchivePieceCopyTask::process()
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObBackupPath src_ls_dir;
  ObBackupPath dst_ls_dir;
  ObArenaAllocator allocator("BackupArcFile");
  char *buf = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy task not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]copy ctx should not be null", K(ret));
  } else if (OB_UNLIKELY(idx_ >= ctx_->ls_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]idx out of range", K(ret), K_(idx), "ls_cnt", ctx_->ls_ids_.count());
  } else if (FALSE_IT(ls_id = ctx_->ls_ids_.at(idx_))) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(ObBackupArchivePieceCopyCtx::MAX_PART_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[BACKUP_ARCHIVE]failed to alloc copy buffer", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_ls_dir_path(ctx_->src_dest_, ctx_->param_.dest_id_, ctx_->param_.round_id_, ctx_->param_.piece_id_, ls_id, src_ls_dir))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get src ls dir path", K(ret), KPC(ctx_), K(ls_id));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_ls_dir_path(ctx_->dst_dest_, ctx_->param_.dest_id_, ctx_->param_.round_id_, ctx_->param_.piece_id_, ls_id, dst_ls_dir))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get dst ls dir path", K(ret), KPC(ctx_), K(ls_id));
  } else if (OB_FAIL(ctx_->copy_dir_recursively(src_ls_dir, dst_ls_dir, allocator, buf, ObBackupArchivePieceCopyCtx::MAX_PART_SIZE, false/*skip ls subdir*/))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy ls dir", K(ret), K(src_ls_dir), K(dst_ls_dir));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]success to copy ls dir", K_(idx), K(ls_id), KPC(ctx_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_ISNULL(ctx_)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[BACKUP_ARCHIVE]copy ctx should not be null", K(tmp_ret), K(ret));
    } else if (OB_TMP_FAIL(ObBackupArchiveHandler::report_piece_copy_result(ctx_->param_, ret))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to report piece copy result", K(tmp_ret), K(ret));
    }
  }
  return ret;
}

int ObBackupArchivePieceCopyTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]piece copy task not init", K(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]copy ctx should not be null", K(ret));
  } else if (idx_ + 1 >= ctx_->ls_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    ObBackupArchivePieceCopyTask *copy_task = nullptr;
    if (OB_ISNULL(dag_) || ObDagType::DAG_TYPE_BACKUP_ARCHIVE_PIECE_COPY != dag_->get_type()) {
      ret = OB_ERR_SYS;
      LOG_WARN("[BACKUP_ARCHIVE]dag type not match", K(ret), KPC_(dag));
    } else if (OB_FAIL(static_cast<ObBackupArchivePieceCopyDag *>(dag_)->alloc_task(copy_task))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to alloc copy task", K(ret));
    } else if (OB_FAIL(copy_task->init(idx_ + 1, *ctx_))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to init copy task", K(ret), K_(idx));
    } else {
      next_task = copy_task;
    }
  }
  return ret;
}


/* ObBackupArchivePieceCopyFinishTask */
int ObBackupArchivePieceCopyFinishTask::init(ObBackupArchivePieceCopyCtx &copy_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]finish task init twice", K(ret));
  } else {
    ctx_ = &copy_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchivePieceCopyFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper helper;
  const ObBackupArchivePieceCopyParam *param = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]finish task not init", K(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]copy ctx and sql proxy should not be null", K(ret), KP_(ctx), KP(GCTX.sql_proxy_));
  } else if (FALSE_IT(param = &ctx_->param_)) {
  } else if (OB_FAIL(ctx_->copy_piece_placeholder(true/*copy_end_file*/))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy piece end placeholder", K(ret), KPC(ctx_));
  } else if (OB_FAIL(helper.init(param->tenant_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init archive persist helper", K(ret), KPC(param));
  } else if (OB_FAIL(helper.mark_piece_backup_file_status(*GCTX.sql_proxy_, ctx_->param_.dest_id_, ctx_->param_.round_id_, ctx_->param_.piece_id_,
                                                          ObBackupFileStatus::BACKUP_FILE_INCOMPLETE, ObBackupFileStatus::BACKUP_FILE_AVAILABLE))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to mark piece backup file status", K(ret), KPC(param));
  } else {
    FLOG_INFO("[BACKUP_ARCHIVE]mark piece backup file status available", KPC(param));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    tmp_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]copy ctx should not be null", K(tmp_ret), K(ret));
  } else if (OB_TMP_FAIL(ObBackupArchiveHandler::report_piece_copy_result(ctx_->param_, ret))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to report piece copy result", K(tmp_ret), K(ret));
  }
  return ret;
}


/* ObBackupArchivePieceCopyCtx */
int ObBackupArchivePieceCopyCtx::init()
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *proxy = GCTX.sql_proxy_;
  ObBackupJobAttr job_attr;
  ObDirPrefixLSIDFilter prefix_lsid_op(ls_ids_);
  char ls_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };
  int64_t ls_prefix_len = 0;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[BACKUP_ARCHIVE]ctx init twice", K(ret));
  } else if (OB_UNLIKELY(nullptr == proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE] get unexpected null sql proxy", K(ret));
  } else if (OB_FAIL(ObBackupJobOperator::get_job(*proxy, false /*need_lock*/, param_.tenant_id_, param_.job_id_, false /*is_initiator*/, job_attr))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_CANCELED;
    } else {
      LOG_WARN("[BACKUP_ARCHIVE]failed to get job", K(ret), K_(param));
    }
  } else if (job_attr.status_.is_backup_finish()) {
    ret = OB_CANCELED;
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(MAX_PART_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[BACKUP_ARCHIVE]failed to alloc copy buffer", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*proxy, param_.tenant_id_, param_.dest_id_, src_dest_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get source archive dest", K(ret), K(param_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*proxy, param_.tenant_id_, param_.backup_path_, dst_dest_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get backup archive dest", K(ret), K(param_));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(src_dest_, param_.dest_id_, param_.round_id_, param_.piece_id_, src_piece_dir_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get src piece dir path", K(ret), K_(param));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(dst_dest_, param_.dest_id_, param_.round_id_, param_.piece_id_, dst_piece_dir_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get dst piece dir path", K(ret), K_(param));
  } else if (OB_FAIL(databuff_printf(ls_prefix, sizeof(ls_prefix), ls_prefix_len, "%s_", OB_STR_LS))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to build ls dir prefix", K(ret));
  } else if (OB_FAIL(prefix_lsid_op.init(ls_prefix, static_cast<int32_t>(ls_prefix_len)))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to init ls dir prefix filter", K(ret), K(ls_prefix));
  } else if (OB_FAIL(ObBackupIoAdapter::list_directories(src_piece_dir_.get_obstr(), src_dest_.get_storage_info(), prefix_lsid_op))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to list ls dirs", K(ret), K(src_piece_dir_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupArchivePieceCopyCtx::copy_round_files() const
{
  int ret = OB_SUCCESS;
  ObBackupPath src_path;
  ObBackupPath dst_path;
  bool is_exist = false;

  // use src archive dest id for backup dest path to make the backuped files can be used directly for restore
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[BACKUP_ARCHIVE]ctx not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_start_file_path(src_dest_, param_.dest_id_, param_.round_id_, src_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get src round start path", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_is_exist(src_path.get_obstr(), src_dest_.get_storage_info(), is_exist))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check round file exist", K(ret), K(src_path));
  } else if (OB_UNLIKELY(!is_exist)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]round start file not found on source", K(ret), K(src_path));
  } else if (OB_FAIL(ObArchivePathUtil::get_round_start_file_path(dst_dest_, param_.dest_id_, param_.round_id_, dst_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get dst round start path", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_is_exist(dst_path.get_obstr(), dst_dest_.get_storage_info(), is_exist))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check round file exist", K(ret), K(dst_path));
  } else if (!is_exist && OB_FAIL(copy_single_file_(src_path, dst_path, buf_, MAX_PART_SIZE))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy round file", K(ret), K(src_path), K(dst_path));
  }

  src_path.reset();
  dst_path.reset();
  if (FAILEDx(ObArchivePathUtil::get_round_end_file_path(src_dest_, param_.dest_id_, param_.round_id_, src_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get src round end path", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_is_exist(src_path.get_obstr(), src_dest_.get_storage_info(), is_exist))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check round file exist", K(ret), K(src_path));
  } else if (!is_exist) {
    // round is still running, no end round file to copy
  } else if (OB_FAIL(ObArchivePathUtil::get_round_end_file_path(dst_dest_, param_.dest_id_, param_.round_id_, dst_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get dst round end path", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_is_exist(dst_path.get_obstr(), dst_dest_.get_storage_info(), is_exist))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to check round file exist", K(ret), K(dst_path));
  } else if (!is_exist && OB_FAIL(copy_single_file_(src_path, dst_path, buf_, MAX_PART_SIZE))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to copy round file", K(ret), K(src_path), K(dst_path));
  }
  return ret;
}


int ObBackupArchivePieceCopyCtx::copy_piece_placeholder(const bool copy_end_file)
{
  int ret = OB_SUCCESS;
  const char *FILE_TOKEN = copy_end_file ? "end_" : "start_";
  int64_t copied_cnt = 0;

  ObArray<ObString> file_names;
  ObFileListArrayOp list_op(file_names, allocator_);
  ObBackupPath src_pieces_path;
  ObBackupPath dst_pieces_path;
  char prefix[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
  int64_t prefix_len = 0;

  if (OB_FAIL(ObArchivePathUtil::get_pieces_dir_path(src_dest_, src_pieces_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get src pieces dir path", K(ret), K_(param));
  } else if (OB_FAIL(ObArchivePathUtil::get_pieces_dir_path(dst_dest_, dst_pieces_path))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get dst pieces dir path", K(ret), K_(param));
  } else if (OB_FAIL(databuff_printf(prefix, sizeof(prefix), prefix_len,
      "piece_d%ldr%ldp%ld_%s", param_.dest_id_, param_.round_id_, param_.piece_id_, FILE_TOKEN))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to build placeholder prefix", K(ret), K_(param));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_list_files(src_pieces_path.get_obstr(), src_dest_.get_storage_info(), list_op))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to list pieces dir", K(ret), K(src_pieces_path));
  }

  for (int64_t idx = 0; OB_SUCC(ret) && idx < file_names.count(); ++idx) {
    const ObString &name = file_names.at(idx);
    if (name.length() > prefix_len && 0 == MEMCMP(name.ptr(), prefix, prefix_len)) {
      if (OB_FAIL(src_pieces_path.join(name, ObBackupFileSuffix::NONE))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to join src placeholder name", K(ret), K(name));
      } else if (OB_FAIL(dst_pieces_path.join(name, ObBackupFileSuffix::NONE))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to join dst placeholder name", K(ret), K(name));
      } else if (OB_FAIL(copy_single_file_(src_pieces_path, dst_pieces_path, buf_, MAX_PART_SIZE))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to copy placeholder", K(ret), K(src_pieces_path), K(dst_pieces_path));
      } else {
        ++copied_cnt;
      }
      break;
    }
  }

  // the end placeholder is the completion mark: its absence means the piece is not frozen yet
  if (OB_SUCC(ret) && copy_end_file && 0 == copied_cnt) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("[BACKUP_ARCHIVE]piece end placeholder not found on source, piece is not frozen/complete", K(ret), K_(param));
  }
  return ret;
}


/*
 * LOG_ARCHIVE_DEST/piece_DESTIDrROUNDIDpPIECEID/
 * ├── file1
 * ├── file2
 * ├── subdir1
 * │   ├── file3
 * │   └── file4
 * └── subdir2
 *     ├── file5
 *     └── file6
 */
int ObBackupArchivePieceCopyCtx::copy_dir_recursively(
    const ObBackupPath &src_dir,
    const ObBackupPath &dst_dir,
    ObIAllocator &allocator,
    char *buf,
    const int64_t buf_len,
    const bool skip_ls_subdir) const
{
  int ret = OB_SUCCESS;
  ObArray<ObString> file_names;
  ObArray<ObString> dir_names;
  ObFileListArrayOp file_op(file_names, allocator);
  ObFileListArrayOp dir_op(dir_names, allocator);
  char ls_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };
  int64_t ls_prefix_len = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid copy buffer", K(ret), KP(buf), K(buf_len), K(src_dir));
  } else if (skip_ls_subdir && OB_FAIL(databuff_printf(ls_prefix, sizeof(ls_prefix), ls_prefix_len, "%s_", OB_STR_LS))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to build ls dir prefix", K(ret));
  // list all files and subdirectories under log_archive_dest/piece_DESTIDrROUNDIDpPIECEID/
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_list_files(src_dir.get_obstr(), src_dest_.get_storage_info(), file_op))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to list files", K(ret), K(src_dir));
  } else if (OB_FAIL(ObBackupIoAdapter::list_directories(src_dir.get_obstr(), src_dest_.get_storage_info(), dir_op))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to list directories", K(ret), K(src_dir));
  }

  // copy files first
  for (int64_t i = 0; OB_SUCC(ret) && i < file_names.count(); ++i) {
    const ObString &name = file_names.at(i);
    ObBackupPath src_path;
    ObBackupPath dst_path;
    if (OB_FAIL(src_path.init(src_dir.get_obstr()))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to init src file path", K(ret), K(src_dir));
    } else if (OB_FAIL(src_path.join(name, ObBackupFileSuffix::NONE))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to join src file name", K(ret), K(name));
    } else if (OB_FAIL(dst_path.init(dst_dir.get_obstr()))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to init dst file path", K(ret), K(dst_dir));
    } else if (OB_FAIL(dst_path.join(name, ObBackupFileSuffix::NONE))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to join dst file name", K(ret), K(name));
    } else if (OB_FAIL(copy_single_file_(src_path, dst_path, buf, buf_len))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to copy file", K(ret), K(src_path), K(dst_path));
    }
  }

  // open subdirectories and copy the files recursively
  for (int64_t i = 0; OB_SUCC(ret) && i < dir_names.count(); ++i) {
    const ObString &name = dir_names.at(i);
    bool hash_copied = false;
    ObBackupPath src_sub;
    ObBackupPath dst_sub;

    // skip ls subdirs at the piece dir level, they are handled by the parallel copy tasks
    if (skip_ls_subdir && name.length() >= ls_prefix_len && 0 == MEMCMP(name.ptr(), ls_prefix, ls_prefix_len)) {
      continue;
    }
    // adaptively_list_files may surface a marker-based "file" that list_directories also
    // returns as a sub-directory; skip such names here to avoid copying them twice.
    for (int64_t j = 0; !hash_copied && j < file_names.count(); ++j) {
      if (name == file_names.at(j)) {
        hash_copied = true;
      }
    }

    if (hash_copied) {
      // do nothing
    } else if (OB_FAIL(src_sub.init(src_dir.get_obstr()))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to init src subdir path", K(ret), K(src_dir));
    } else if (OB_FAIL(src_sub.join(name, ObBackupFileSuffix::NONE))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to join src subdir name", K(ret), K(name));
    } else if (OB_FAIL(dst_sub.init(dst_dir.get_obstr()))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to init dst subdir path", K(ret), K(dst_dir));
    } else if (OB_FAIL(dst_sub.join(name, ObBackupFileSuffix::NONE))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to join dst subdir name", K(ret), K(name));
    } else if (OB_FAIL(copy_dir_recursively(src_sub, dst_sub, allocator, buf, buf_len, false/*ls dirs only exist at the piece dir level*/))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to copy subdir", K(ret), K(src_sub), K(dst_sub));
    }
  }
  return ret;
}

int ObBackupArchivePieceCopyCtx::copy_single_file_(
    const ObBackupPath &src_path,
    const ObBackupPath &dst_path,
    char *buf,
    const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = nullptr;
  ObIOFd fd;
  int64_t src_len = 0;
  const ObStorageIdMod mod = ObStorageIdMod::get_default_backup_id_mod();

  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid copy buffer", K(ret), KP(buf), K(buf_len), K(src_path));
  } else if (OB_FAIL(ObBackupIoAdapter::adaptively_get_file_length(src_path.get_obstr(), src_dest_.get_storage_info(), src_len))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get src file length", K(ret), K(src_path));
  } else if (OB_FAIL(ObBackupIoAdapter::mk_parent_dir(dst_path.get_obstr(), dst_dest_.get_storage_info()))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to make parent dir", K(ret), K(dst_path));
  } else if (OB_FAIL(ObBackupIoAdapter::open_with_access_type(device_handle, fd, dst_dest_.get_storage_info(), dst_path.get_obstr(), OB_STORAGE_ACCESS_MULTIPART_WRITER, mod))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to open dst file", K(ret), K(dst_path));
  } else {
    int64_t dst_len = 0;
    while (OB_SUCC(ret) && dst_len < src_len) {
      const int64_t transfer_len = std::min(src_len - dst_len, buf_len);
      int64_t read_len = 0;
      int64_t write_size = 0;
      if (OB_FAIL(ObBackupIoAdapter::adaptively_read_part_file(src_path.get_obstr(), src_dest_.get_storage_info(),
              buf, transfer_len, dst_len, read_len, mod))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to read part file", K(ret), K(src_path), K(dst_len), K(transfer_len));
      } else if (read_len != transfer_len) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[BACKUP_ARCHIVE]read len mismatch", K(ret), K(read_len), K(transfer_len), K(src_path));
      } else if (OB_FAIL(device_handle->pwrite(fd, dst_len, transfer_len, buf, write_size))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to pwrite dst file", K(ret), K(dst_path), K(dst_len));
      } else {
        dst_len += transfer_len;
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(device_handle->abort(fd))) {
        LOG_WARN("[BACKUP_ARCHIVE]failed to abort multipart upload", K(tmp_ret), K(ret), K(dst_path));
      }
    } else if (OB_FAIL(device_handle->complete(fd))) {
      LOG_WARN("[BACKUP_ARCHIVE]failed to complete multipart upload", K(ret), K(dst_path));
    }
    if (OB_TMP_FAIL(ObBackupIoAdapter::close_device_and_fd(device_handle, fd))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("[BACKUP_ARCHIVE]failed to close device and fd", K(tmp_ret), K(ret), K(dst_path));
    }
  }
  return ret;
}


/* ObBackupArchiveHandler */
int ObBackupArchiveHandler::schedule_piece_copy_dag(const obrpc::ObNotifyBackupArchiveArg &arg)
{
  int ret = OB_SUCCESS;
  ObBackupArchivePieceCopyParam param;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObTenantDagScheduler *dag_scheduler = nullptr;

  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(param.set(arg))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to convert arg to param", K(ret), K(arg));
  } else if (OB_FAIL(guard.switch_to(arg.tenant_id_))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to switch to tenant", K(ret), K(arg));
  } else if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]dag scheduler must not be null", K(ret), K(arg));
  } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObBackupArchivePieceCopyDagNet>(&param))) {
    if (OB_TASK_EXIST == ret) {
      LOG_INFO("[BACKUP_ARCHIVE]piece copy dag net already exists", K(ret), K(arg));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[BACKUP_ARCHIVE]failed to create and add piece copy dag net", K(ret), K(arg));
      (void) report_piece_copy_result(param, ret);
    }
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]success to schedule piece copy dag", K(arg));
  }
  return ret;
}

int ObBackupArchiveHandler::report_piece_copy_result(const ObBackupArchivePieceCopyParam &param, const int result)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader_addr;
  obrpc::ObBackupTaskRes res;
  res.job_id_ = param.job_id_;
  res.round_id_ = param.round_id_;
  res.piece_id_ = param.piece_id_;
  res.tenant_id_ = param.tenant_id_;
  res.src_server_ = GCTX.self_addr();
  res.result_ = result;
  res.trace_id_ = param.trace_id_;
  res.dag_id_ = param.trace_id_;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(param.tenant_id_);
  if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[BACKUP_ARCHIVE]location service or rpc proxy is null", K(ret));
  } else if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[BACKUP_ARCHIVE]invalid backup task res", K(ret), K(res));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
      cluster_id, meta_tenant_id, ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to get rs leader address", K(ret), K(meta_tenant_id));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).report_backup_archive_over(res))) {
    LOG_WARN("[BACKUP_ARCHIVE]failed to report backup archive over", K(ret), K(res), K(leader_addr));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]report backup archive over", K(res), K(leader_addr));
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase
