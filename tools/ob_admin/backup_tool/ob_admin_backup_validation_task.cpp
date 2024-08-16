/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */
#include "ob_admin_backup_validation_task.h"
#include "logservice/restoreservice/ob_remote_log_iterator.h"
#include "logservice/restoreservice/ob_remote_log_source.h"
#include "logservice/restoreservice/ob_remote_log_source_allocator.h"
#include "ob_admin_backup_validation_executor.h"
#include "ob_admin_backup_validation_util.h"
#include "share/backup/ob_archive_checkpoint_mgr.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#define CLEAR_LINE "\033[1K\033[0G"
namespace oceanbase
{
namespace tools
{
////////////////ObAdminLogArchiveValidationDagNet Start////////////////
ObAdminLogArchiveValidationDagNet::~ObAdminLogArchiveValidationDagNet() {}
bool ObAdminLogArchiveValidationDagNet::operator==(const ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminLogArchiveValidationDagNet &other_dag
        = static_cast<const ObAdminLogArchiveValidationDagNet &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminLogArchiveValidationDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->log_archive_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len,
                                  "[LOG_ARCHIVE_VALIDATION_DAG_NET]: log_archive_dest=%s",
                                  ctx_->log_archive_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill comment", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len,
                                  "[LOG_ARCHIVE_VALIDATION_DAG_NET]: backup_piece_path="))) {
        STORAGE_LOG(WARN, "failed to fill comment", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_piece_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill comment", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int ObAdminLogArchiveValidationDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->log_archive_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, "log_archive_dest=%s",
                                  ctx_->log_archive_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill dag net key", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, "backup_piece_path="))) {
        STORAGE_LOG(WARN, "failed to fill dag net key", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_piece_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill dag net key", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int64_t ObAdminLogArchiveValidationDagNet::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}
int ObAdminLogArchiveValidationDagNet::init(ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminLogArchiveValidationDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObAdminLogArchiveValidationDagInitParam *dag_init_param
      = static_cast<const ObAdminLogArchiveValidationDagInitParam *>(param);
  if (OB_ISNULL(dag_init_param)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "dag_init_param is null", K(ret));
  } else if (OB_ISNULL(dag_init_param->ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else if (OB_FAIL(init(dag_init_param->ctx_))) {
    STORAGE_LOG(WARN, "failed to init", K(ret));
  }
  return ret;
}
bool ObAdminLogArchiveValidationDagNet::is_valid() const { return true; }
int ObAdminLogArchiveValidationDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObAdminPrepareLogArchiveValidationDag *prepare_log_archive_validation_dag = nullptr;
  ObAdminBackupPieceValidationDag *backup_piece_validation_dag = nullptr;
  ObAdminFinishLogArchiveValidationDag *finish_log_archive_validation_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  // TODO: jiangshichao.jsc handle state cancelation when encounter error
  // create dag and connections
  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "scheduler is null", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(prepare_log_archive_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to create dag", K(ret));
  } else if (OB_FAIL(prepare_log_archive_validation_dag->init(0 /*dag_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "Fail to init dag", K(ret));
  } else if (OB_FAIL(prepare_log_archive_validation_dag->create_first_task())) {
    STORAGE_LOG(WARN, "Fail to create first task", K(ret));
  } else if (OB_FAIL(
                 add_dag_into_dag_net(*prepare_log_archive_validation_dag))) { // add first dag
                                                                               // into this dag_net
    STORAGE_LOG(WARN, "Fail to add dag into dag_net", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(backup_piece_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to create dag", K(ret));
  } else if (OB_FAIL(backup_piece_validation_dag->init(0 /*dag_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "Fail to init dag", K(ret));
  } else if (OB_FAIL(backup_piece_validation_dag->create_first_task())) {
    STORAGE_LOG(WARN, "Fail to create first task", K(ret));
  } else if (OB_FAIL(prepare_log_archive_validation_dag->add_child(*backup_piece_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_log_archive_validation_dag),
                KPC(backup_piece_validation_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(finish_log_archive_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to create dag", K(ret));
  } else if (OB_FAIL(finish_log_archive_validation_dag->init(0 /*dag_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "Fail to init dag", K(ret));
  } else if (OB_FAIL(finish_log_archive_validation_dag->create_first_task())) {
    STORAGE_LOG(WARN, "Fail to create first task", K(ret));
  } else if (OB_FAIL(backup_piece_validation_dag->add_child(*finish_log_archive_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add child", K(ret), KPC(backup_piece_validation_dag),
                KPC(finish_log_archive_validation_dag));
  } else if (OB_FAIL(scheduler->add_dag(prepare_log_archive_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(backup_piece_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(finish_log_archive_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
  } else {
    // add all dags into dag_scheduler
    STORAGE_LOG(INFO, "success to schedule all dags into dag_scheduler", K(ret));
  }
  return ret;
}
////////////////ObAdminLogArchiveValidationDagNet End////////////////

////////////////ObAdminPrepareLogArchiveValidationDag Start////////////////
ObAdminPrepareLogArchiveValidationDag::~ObAdminPrepareLogArchiveValidationDag() {}
bool ObAdminPrepareLogArchiveValidationDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminPrepareLogArchiveValidationDag &other_dag
        = static_cast<const ObAdminPrepareLogArchiveValidationDag &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminPrepareLogArchiveValidationDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             static_cast<int64_t>(OB_SERVER_TENANT_ID)))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}
int ObAdminPrepareLogArchiveValidationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->log_archive_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, "log_archive_dest=%s",
                                  ctx_->log_archive_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, "backup_piece_path="))) {
        STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_piece_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int64_t ObAdminPrepareLogArchiveValidationDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}
int ObAdminPrepareLogArchiveValidationDag::init(int64_t id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    id_ = id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminPrepareLogArchiveValidationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAdminPrepareLogArchiveValidationTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(0 /*task_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "failed to init task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "failed to add task", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create first task", K(ret));
  }
  return ret;
}
////////////////ObAdminPrepareLogArchiveValidationDag End////////////////

////////////////ObAdminPrepareLogArchiveValidationTask Start////////////////
ObAdminPrepareLogArchiveValidationTask::~ObAdminPrepareLogArchiveValidationTask() {}
int ObAdminPrepareLogArchiveValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminPrepareLogArchiveValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else {
    switch (ctx_->validation_type_) {
    case ObAdminBackupValidationType::DATABASE_VALIDATION: {
      if (OB_ISNULL(ctx_->log_archive_dest_) || OB_ISNULL(ctx_->data_backup_dest_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "log_archive_dest or data_backup_dest is null", K(ret));
      } else if (OB_FAIL(check_format_file_())) {
        STORAGE_LOG(WARN, "failed to check backup format file", K(ret));
      } else if (OB_FAIL(collect_backup_piece_())) {
        STORAGE_LOG(WARN, "failed to collect backup piece", K(ret));
      } else {
        STORAGE_LOG(INFO, "succeed to collect backup piece", K(ret));
      }
      break;
    }
    case ObAdminBackupValidationType::BACKUPPIECE_VALIDATION: {
      if (OB_ISNULL(ctx_->log_archive_dest_) && 0 == ctx_->backup_piece_path_array_.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup piece path is empty", K(ret));
      } else if (OB_NOT_NULL(ctx_->log_archive_dest_)) {
        if (OB_FAIL(check_format_file_())) {
          STORAGE_LOG(WARN, "failed to check backup format file", K(ret));
        } else if (OB_FAIL(collect_backup_piece_())) {
          STORAGE_LOG(WARN, "failed to collect backup piece", K(ret));
        } else {
          STORAGE_LOG(INFO, "succeed to collect backup piece", K(ret));
        }
      } else if (ctx_->backup_piece_path_array_.count() > 0) {
        if (OB_FAIL(retrieve_backup_piece_())) {
          STORAGE_LOG(WARN, "failed to retrieve backup piece", K(ret));
        } else {
          STORAGE_LOG(INFO, "succeed to retrieve backup piece", K(ret));
        }
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected validation type", K(ret), K(ctx_->validation_type_));
      break;
    }
    }
  }

  post_process_(ret);
  return ret;
}
int ObAdminPrepareLogArchiveValidationTask::check_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupStore backup_store;
  ObBackupFormatDesc backup_format_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_->log_archive_dest_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "log_archive_dest is null", K(ret));
  } else if (OB_FAIL(backup_store.init(*ctx_->log_archive_dest_))) {
    STORAGE_LOG(WARN, "failed to init backup data store", K(ret));
  } else if (OB_FAIL(backup_store.read_format_file(backup_format_desc))) {
    STORAGE_LOG(WARN, "failed to read backup format file", K(ret));
  } else if (ObBackupFileType::BACKUP_FORMAT_FILE != backup_format_desc.get_data_type()) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "backup format file is not backup format file", K(ret));
  }
  return ret;
}
int ObAdminPrepareLogArchiveValidationTask::collect_backup_piece_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObBackupPath backup_path;
  ObArray<share::ObPieceKey> backup_piece_array;
  share::ObArchiveStore backup_piece_store;
  ObBackupDest backup_piece_dest;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_->log_archive_dest_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "log_archive_dest is null", K(ret));
  } else if (OB_FAIL(backup_piece_store.init(*ctx_->log_archive_dest_))) {
    STORAGE_LOG(WARN, "failed to init backup piece store", K(ret));
  } else if (OB_FAIL(backup_piece_store.get_all_piece_keys(backup_piece_array))) {
    STORAGE_LOG(WARN, "failed to get backup piece array", K(ret));
  } else if (backup_piece_array.empty()) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "no backup piece found", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to get backup piece names", K(ret), K(backup_piece_array));
    ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
    if (ctx_->backup_piece_key_array_.count()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < ctx_->backup_piece_key_array_.count(); ++j) {
        const share::ObPieceKey &backup_piece_key = ctx_->backup_piece_key_array_.at(j);
        bool filtered = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_array.count(); ++i) {
          if (backup_piece_key == backup_piece_array.at(i)) {
            backup_piece_dest.reset();
            if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(
                    *ctx_->log_archive_dest_, backup_piece_key.dest_id_, backup_piece_key.round_id_,
                    backup_piece_key.piece_id_, backup_path))) {
              STORAGE_LOG(WARN, "failed to get backup piece dir path", K(ret));
            } else if (OB_FAIL(backup_piece_dest.set(
                           backup_path.get_ptr(), ctx_->log_archive_dest_->get_storage_info()))) {
              STORAGE_LOG(WARN, "failed to set backup piece dest", K(ret));
            } else if (OB_FAIL(ctx_->add_backup_piece(backup_piece_key))) {
              STORAGE_LOG(WARN, "failed to add backup piece key", K(ret), K(backup_piece_key));
            } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
              STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
            } else if (OB_ISNULL(backup_piece_attr)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "backup piece attr is null", K(ret));
            } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(share::ObArchiveStore)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "failed to alloc backup piece store", K(ret));
            } else if (FALSE_IT(backup_piece_attr->backup_piece_store_
                                = new (alc_ptr) share::ObArchiveStore())) {
            } else if (OB_FAIL(backup_piece_attr->backup_piece_store_->init(backup_piece_dest))) {
              STORAGE_LOG(WARN, "failed to init backup piece store", K(ret), K(backup_piece_key));
            } else {
              filtered = true;
              STORAGE_LOG(INFO, "succeed to init backup piece store", K(ret), K(backup_piece_key));
            }
          }
        }
        if (!filtered) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "designated backup piece found", K(ret), K(backup_piece_key));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_piece_array.count(); ++i) {
        const share::ObPieceKey &backup_piece_key = backup_piece_array.at(i);
        backup_piece_dest.reset();
        if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(
                *ctx_->log_archive_dest_, backup_piece_key.dest_id_, backup_piece_key.round_id_,
                backup_piece_key.piece_id_, backup_path))) {
          STORAGE_LOG(WARN, "failed to get backup piece dir path", K(ret));
        } else if (OB_FAIL(backup_piece_dest.set(backup_path.get_ptr(),
                                                 ctx_->log_archive_dest_->get_storage_info()))) {
          STORAGE_LOG(WARN, "failed to set backup piece dest", K(ret));
        } else if (OB_FAIL(ctx_->add_backup_piece(backup_piece_key))) {
          STORAGE_LOG(WARN, "failed to add backup piece key", K(ret), K(backup_piece_key));
        } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
          STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
        } else if (OB_ISNULL(backup_piece_attr)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "backup piece attr is null", K(ret));
        } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(share::ObArchiveStore)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc backup piece store", K(ret));
        } else if (FALSE_IT(backup_piece_attr->backup_piece_store_
                            = new (alc_ptr) share::ObArchiveStore())) {
        } else if (OB_FAIL(backup_piece_attr->backup_piece_store_->init(backup_piece_dest))) {
          STORAGE_LOG(WARN, "failed to init backup piece store", K(ret), K(backup_piece_key));
        } else {
          STORAGE_LOG(INFO, "succeed to init backup piece store", K(ret), K(backup_piece_key));
        }
      }
    }
  }
  return ret;
}
int ObAdminPrepareLogArchiveValidationTask::retrieve_backup_piece_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObArchiveStore backup_piece_store;
  ObSinglePieceDesc backup_piece_info;
  share::ObPieceKey backup_piece_key;
  ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->backup_piece_path_array_.count(); ++i) {
      backup_piece_store.reset();
      const share::ObBackupDest *backup_piece_dest = nullptr;
      if (OB_ISNULL(backup_piece_dest = ctx_->backup_piece_path_array_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup piece dest is null", K(ret));
      } else if (OB_FAIL(backup_piece_store.init(*backup_piece_dest))) {
        STORAGE_LOG(WARN, "failed to init backup piece store", K(ret));
      } else if (OB_FAIL(backup_piece_store.read_single_piece(backup_piece_info))) {
        STORAGE_LOG(WARN, "failed to read backup piece info", K(ret));
      } else if (FALSE_IT(backup_piece_key.dest_id_ = backup_piece_info.piece_.key_.dest_id_)
                 || FALSE_IT(backup_piece_key.round_id_ = backup_piece_info.piece_.key_.round_id_)
                 || FALSE_IT(backup_piece_key.piece_id_
                             = backup_piece_info.piece_.key_.piece_id_)) {
      } else if (OB_FAIL(ctx_->add_backup_piece(backup_piece_key))) {
        STORAGE_LOG(WARN, "failed to add backup piece key", K(ret), K(backup_piece_key));
      } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
        STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
      } else if (OB_ISNULL(backup_piece_attr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup piece attr is null", K(ret));
      } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(share::ObArchiveStore)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc backup piece store", K(ret));
      } else if (FALSE_IT(backup_piece_attr->backup_piece_store_
                          = new (alc_ptr) share::ObArchiveStore())) {
      } else if (OB_FAIL(backup_piece_attr->backup_piece_store_->init(*backup_piece_dest))) {
        STORAGE_LOG(WARN, "failed to init backup piece store", K(ret));
      } else {
        STORAGE_LOG(INFO, "succeed to init backup piece store", K(ret), K(backup_piece_key));
      }
    }
  }
  return ret;
}
void ObAdminPrepareLogArchiveValidationTask::post_process_(int ret)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), K(tmp_ret));
  } else if (ctx_->processing_backup_piece_key_array_.empty()) {
    printf(CLEAR_LINE);
    printf("Cannot found any valid OB backup piece, maybe none selected\n");
    ctx_->go_abort("Error Path", "Not a valid OB backup path");
  } else {
    if (OB_SUCC(ret)) {
      printf(CLEAR_LINE);
      printf("Succeed found OB backup piece: ");
      FOREACH(iter, ctx_->processing_backup_piece_key_array_)
      {
        printf("d%ldr%ldp%ld ", iter->dest_id_, iter->round_id_, iter->piece_id_);
      }
      printf("\n");
    } else {
      printf(CLEAR_LINE);
      printf("Log Archive path seems not a valid OB backup path, please check "
             "carefully\n");
      ctx_->go_abort("Error Path", "Not a valid OB backup path");
    }
  }
  fflush(stdout);
}
////////////////ObAdminPrepareDataBackupValidationTask End////////////////

////////////////ObAdminBackupPieceValidationDag Start////////////////
ObAdminBackupPieceValidationDag::~ObAdminBackupPieceValidationDag()
{
  storage_handler_.stop();
  storage_handler_.wait();
  storage_handler_.destroy();
  large_buffer_pool_.destroy();
}
bool ObAdminBackupPieceValidationDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminBackupPieceValidationDag &other_dag
        = static_cast<const ObAdminBackupPieceValidationDag &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminBackupPieceValidationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                                     ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  share::ObPieceKey backup_piece_key;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(id_, backup_piece_key))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(id_));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             static_cast<int64_t>(OB_SERVER_TENANT_ID),
                                             backup_piece_key.dest_id_, backup_piece_key.round_id_,
                                             backup_piece_key.piece_id_))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}
int ObAdminBackupPieceValidationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    share::ObPieceKey backup_piece_key;
    if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(id_, backup_piece_key))) {
      STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(id_));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, "piece_id: d%ldr%ldp%ld",
                                       backup_piece_key.dest_id_, backup_piece_key.round_id_,
                                       backup_piece_key.piece_id_))) {
      STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
    }
  }
  return ret;
}
int64_t ObAdminBackupPieceValidationDag::hash() const
{
  uint64_t hash_value = 0;
  int64_t ptr = reinterpret_cast<int64_t>(this);
  hash_value = common::murmurhash(&id_, sizeof(id_), hash_value);
  hash_value = common::murmurhash(&ptr, sizeof(ptr), hash_value);
  return hash_value;
}
int ObAdminBackupPieceValidationDag::init(int64_t id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  const int64_t GB = 1024 * 1024 * 1024L;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else if (OB_FAIL(storage_handler_.init())) {
    STORAGE_LOG(WARN, "failed to init storage_handler");
  } else if (OB_FAIL(storage_handler_.start(0))) {
    STORAGE_LOG(WARN, "failed to start storage_handler");
  } else if (OB_FAIL(large_buffer_pool_.init("DagLargePool", 1 * GB))) {
    STORAGE_LOG(WARN, "failed to init large_buffer_pool");
  } else {
    is_inited_ = true;
    id_ = id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminBackupPieceValidationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAdminBackupPieceMetaValidationTask *piece_meta_task = nullptr;
  ObAdminBackupPieceLogIterationTask *log_iter_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(alloc_task(piece_meta_task))) {
    STORAGE_LOG(WARN, "failed to alloc piece_meta_task", K(ret));
  } else if (OB_FAIL(alloc_task(log_iter_task))) {
    STORAGE_LOG(WARN, "failed to alloc log_iter_task", K(ret));
  } else if (OB_FAIL(piece_meta_task->init(id_, ctx_))) {
    STORAGE_LOG(WARN, "failed to init piece_meta_task", K(ret));
  } else if (OB_FAIL(log_iter_task->init(0 /*task_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "failed to init log_iter_task", K(ret));
  } else if (OB_FAIL(piece_meta_task->add_child(*log_iter_task))) {
    STORAGE_LOG(WARN, "failed to add child", K(ret));
  } else if (OB_FAIL(add_task(*piece_meta_task))) {
    STORAGE_LOG(WARN, "failed to add piece_meta_task", K(ret));
  } else if (OB_FAIL(add_task(*log_iter_task))) {
    STORAGE_LOG(WARN, "failed to add log_iter_task", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create first task", K(ret));
  }
  return ret;
}
int ObAdminBackupPieceValidationDag::generate_next_dag(ObIDag *&next_dag)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObAdminBackupPieceValidationDag *sibling_dag = nullptr;
  int64_t next_id = id_ + 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (next_id >= ctx_->processing_backup_piece_key_array_.count()) {
    ret = OB_ITER_END;
    next_dag = nullptr;
    STORAGE_LOG(INFO, "no more backup set", K(ret), K(next_id),
                K(ctx_->processing_backup_piece_key_array_.count()));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(sibling_dag))) {
    COMMON_LOG(WARN, "failed to alloc sibling_dag", K(ret));
  } else if (OB_FAIL(sibling_dag->init(next_id, ctx_))) {
    COMMON_LOG(WARN, "failed to init tablet migration dag", K(ret));
  } else {
    next_dag = sibling_dag;
    sibling_dag = nullptr;
  }

  if (OB_NOT_NULL(sibling_dag)) {
    scheduler->free_dag(*sibling_dag);
  }
  return ret;
}
////////////////ObAdminBackupPieceValidationDag End////////////////

////////////////ObAdminBackupPieceMetaValidationTask Start////////////////
ObAdminBackupPieceMetaValidationTask::~ObAdminBackupPieceMetaValidationTask() {}
int ObAdminBackupPieceMetaValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminBackupPieceMetaValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (ctx_->processing_backup_piece_key_array_.empty()) {
    STORAGE_LOG(INFO, "backup piece key is all empty", K(ret), K(task_id_));
  } else if (OB_FAIL(check_backup_piece_info_())) {
    STORAGE_LOG(WARN, "failed to check backup piece info", K(ret));
  } else if (OB_FAIL(collect_and_check_piece_ls_info_())) {
    STORAGE_LOG(WARN, "failed to collect and check piece ls info", K(ret));
  } else if (OB_FAIL(collect_and_check_piece_ls_onefile_length_())) {
    STORAGE_LOG(WARN, "failed to collect and check piece ls onefile length", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to process ObAdminBackupPieceMetaValidationTask", K(ret),
                K(task_id_));
  }
  post_process_(ret);
  return ret;
}
int ObAdminBackupPieceMetaValidationTask::check_backup_piece_info_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObPieceKey backup_piece_key;
  share::ObBackupPath full_path;
  ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
  share::ObArchiveStore *backup_piece_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  bool is_single_piece_file_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(task_id_, backup_piece_key))) {
    STORAGE_LOG(WARN, "failed to get backup piece key", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
    STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
  } else if (OB_ISNULL(backup_piece_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece attr is null", K(ret));
  } else if (OB_ISNULL(backup_piece_store = backup_piece_attr->backup_piece_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_piece_store->get_backup_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (OB_FAIL(share::ObArchivePathUtil::get_single_piece_file_info_path(
                 backup_piece_store->get_backup_dest(), full_path))) {
    STORAGE_LOG(WARN, "failed to get single piece file info path", K(ret),
                K(backup_piece_store->get_backup_dest()));
  } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(share::ObSinglePieceDesc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc backup piece info desc", K(ret));
  } else if (FALSE_IT(backup_piece_attr->backup_piece_info_desc_
                      = new (alc_ptr) share::ObSinglePieceDesc())) {
  } else if (OB_FAIL(ObAdminBackupValidationUtil::check_file_exist(
                 full_path.get_ptr(), backup_storage_info, is_single_piece_file_exist))) {
    STORAGE_LOG(WARN, "failed to check file exist", K(ret), K(full_path));
  }
  if (is_single_piece_file_exist) {
    // forzen piece
    if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
            full_path, *backup_storage_info, *backup_piece_attr->backup_piece_info_desc_, ctx_))) {
      STORAGE_LOG(WARN, "failed to read single piece file info", K(ret), K(full_path));
    } else if (!backup_piece_attr->backup_piece_info_desc_->piece_.is_valid()
               || !backup_piece_attr->backup_piece_info_desc_->piece_.is_frozen()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "not a forzon piece", K(ret),
                  K(*backup_piece_attr->backup_piece_info_desc_));
    } else {
      STORAGE_LOG(INFO, "succeed to read single piece file info", K(ret), K(full_path),
                  K(*backup_piece_attr->backup_piece_info_desc_));
    }
  } else {
    STORAGE_LOG(INFO, "single piece file info is not exist, not a forzon piece", K(ret),
                K(full_path));
    full_path.reset();
    ObPieceCheckpointDesc checkpoint_desc;
    ObTenantArchivePieceInfosDesc extend_desc;
    share::ObArchiveCheckpointMgr mgr;
    uint64_t max_checkpoint_scn = 0;
    bool is_piece_checkpoint_file_exist = false;
    if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_file_path(
            backup_piece_store->get_backup_dest(), 0, full_path))) {
      STORAGE_LOG(WARN, "failed to get piece checkpoint file info path", K(ret),
                  K(backup_piece_store->get_backup_dest()));
    } else if (OB_FAIL(ObAdminBackupValidationUtil::check_file_exist(
                   full_path.get_ptr(), backup_storage_info, is_piece_checkpoint_file_exist))) {
      STORAGE_LOG(WARN, "failed to check file exist", K(ret), K(full_path));
    } else if (!is_piece_checkpoint_file_exist) {
      STORAGE_LOG(INFO, "empty piece checkpoint file", K(ret), K(full_path));
    } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                   full_path, *backup_storage_info, checkpoint_desc, ctx_))) {
      STORAGE_LOG(WARN, "failed to read single piece file info", K(ret), K(full_path));
    } else if (FALSE_IT(full_path.reset())) {
    } else if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_dir_path(
                   backup_piece_store->get_backup_dest(), full_path))) {
      STORAGE_LOG(WARN, "failed to get piece checkpoint file info path", K(ret),
                  K(backup_piece_store->get_backup_dest()));
    } else if (OB_FAIL(mgr.init(full_path, OB_STR_CHECKPOINT_FILE_NAME, ObBackupFileSuffix::ARCHIVE,
                                backup_storage_info))) {
      STORAGE_LOG(WARN, "failed to init archive checkpoint mgr", K(ret), K(full_path));
    } else if (OB_FAIL(mgr.read(max_checkpoint_scn))) {
      STORAGE_LOG(WARN, "failed to read archive checkpoint mgr", K(ret), K(full_path));
    } else if (0 == max_checkpoint_scn) {
      // do nothing, archive is not started yet
    } else if (OB_FAIL(checkpoint_desc.checkpoint_scn_.convert_for_inner_table_field(
                   max_checkpoint_scn))) {
      STORAGE_LOG(WARN, "failed to convert checkpoint scn", K(ret), K(max_checkpoint_scn));
    } else if (OB_FAIL(
                   checkpoint_desc.max_scn_.convert_for_inner_table_field(max_checkpoint_scn))) {
      STORAGE_LOG(WARN, "failed to convert checkpoint scn", K(ret), K(max_checkpoint_scn));
    } else if (FALSE_IT(full_path.reset())) {
    } else if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(
                   backup_piece_store->get_backup_dest(), full_path))) {
    } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                   full_path, *backup_storage_info, extend_desc, ctx_))) {
      STORAGE_LOG(WARN, "failed to read check piece extend info file info", K(ret), K(full_path));
    } else {
      // merge static piece attr with dynamic attr.
      backup_piece_attr->backup_piece_info_desc_->piece_.key_.tenant_id_ = extend_desc.tenant_id_;
      backup_piece_attr->backup_piece_info_desc_->piece_.key_.dest_id_ = extend_desc.dest_id_;
      backup_piece_attr->backup_piece_info_desc_->piece_.key_.round_id_ = extend_desc.round_id_;
      backup_piece_attr->backup_piece_info_desc_->piece_.key_.piece_id_ = extend_desc.piece_id_;
      backup_piece_attr->backup_piece_info_desc_->piece_.incarnation_ = extend_desc.incarnation_;
      backup_piece_attr->backup_piece_info_desc_->piece_.dest_no_ = extend_desc.dest_no_;
      backup_piece_attr->backup_piece_info_desc_->piece_.start_scn_ = extend_desc.start_scn_;
      backup_piece_attr->backup_piece_info_desc_->piece_.checkpoint_scn_
          = checkpoint_desc.checkpoint_scn_;
      backup_piece_attr->backup_piece_info_desc_->piece_.max_scn_ = checkpoint_desc.max_scn_;
      backup_piece_attr->backup_piece_info_desc_->piece_.end_scn_ = extend_desc.end_scn_;
      backup_piece_attr->backup_piece_info_desc_->piece_.compatible_ = extend_desc.compatible_;
      backup_piece_attr->backup_piece_info_desc_->piece_.status_.set_active();
      backup_piece_attr->backup_piece_info_desc_->piece_.file_status_
          = ObBackupFileStatus::STATUS::BACKUP_FILE_AVAILABLE;
      backup_piece_attr->backup_piece_info_desc_->piece_.path_ = extend_desc.path_;
      STORAGE_LOG(INFO, "succeed to merge static piece attr with dynamic attr", K(ret),
                  K(full_path), K(*backup_piece_attr->backup_piece_info_desc_));
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "piece info is not consistent with other meta info");
  }
  return ret;
}
int ObAdminBackupPieceMetaValidationTask::collect_and_check_piece_ls_info_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObPieceKey backup_piece_key;
  share::ObBackupPath full_path;
  ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
  share::ObArchiveStore *backup_piece_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  share::ObPieceInfoDesc backup_piece_ls_list_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(task_id_, backup_piece_key))) {
    STORAGE_LOG(WARN, "failed to get backup piece key", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
    STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
  } else if (OB_ISNULL(backup_piece_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece attr is null", K(ret));
  } else if (OB_ISNULL(backup_piece_attr->backup_piece_info_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece info desc is null", K(ret));
  } else if (backup_piece_attr->backup_piece_info_desc_->piece_.is_active()) {
    if (OB_FAIL(inner_collect_active_piece_ls_info_(backup_piece_key, *backup_piece_attr))) {
      STORAGE_LOG(WARN, "failed to collect active piece ls info", K(ret), K(backup_piece_key));
    }
  } else if (backup_piece_attr->backup_piece_info_desc_->piece_.start_scn_
             == backup_piece_attr->backup_piece_info_desc_->piece_.max_scn_) {
    // empty piece, just skip
    STORAGE_LOG(INFO, "empty piece, just skip", K(ret), K(backup_piece_key));
  } else if (OB_ISNULL(backup_piece_store = backup_piece_attr->backup_piece_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_piece_store->get_backup_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (OB_FAIL(share::ObArchivePathUtil::get_piece_info_file_path(
                 backup_piece_store->get_backup_dest(), full_path))) {
    STORAGE_LOG(WARN, "failed to get piece info file info path", K(ret),
                K(backup_piece_store->get_backup_dest()));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                 full_path, *backup_storage_info, backup_piece_ls_list_desc, ctx_))) {
    STORAGE_LOG(WARN, "failed to read single piece file info", K(ret), K(full_path));
  } else {
    STORAGE_LOG(INFO, "succeed to collect piece ls info", K(ret), K(full_path));
    ObAdminLSAttr *ls_attr = nullptr;
    ObSingleLSInfoDesc *single_ls_info_desc = nullptr;
    FOREACH_X(ls_info_desc, backup_piece_ls_list_desc.filelist_, OB_SUCC(ret))
    {
      const share::ObLSID ls_id = ls_info_desc->ls_id_;
      bool is_schema_dir_empty = false;
      // cross check with inner file
      if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(share::ObSingleLSInfoDesc)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc backup piece info desc", K(ret));
      } else if (FALSE_IT(single_ls_info_desc = new (alc_ptr) share::ObSingleLSInfoDesc())) {
      } else if (FALSE_IT(full_path.reset())) {
      } else if (OB_FAIL(share::ObArchivePathUtil::get_ls_file_info_path(
                     backup_piece_store->get_backup_dest(), ls_id, full_path))) {
        STORAGE_LOG(WARN, "failed to get ls file info path", K(ret),
                    K(backup_piece_store->get_backup_dest()), K(ls_id));
      } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                     full_path, *backup_storage_info, *single_ls_info_desc, ctx_))) {
        STORAGE_LOG(WARN, "failed to read single piece file info", K(ret), K(full_path));
      } else if (single_ls_info_desc->dest_id_ != ls_info_desc->dest_id_
                 || single_ls_info_desc->round_id_ != ls_info_desc->round_id_
                 || single_ls_info_desc->piece_id_ != ls_info_desc->piece_id_
                 || single_ls_info_desc->ls_id_ != ls_info_desc->ls_id_
                 || single_ls_info_desc->start_scn_ != ls_info_desc->start_scn_
                 || single_ls_info_desc->checkpoint_scn_ != ls_info_desc->checkpoint_scn_
                 || single_ls_info_desc->min_lsn_ != ls_info_desc->min_lsn_
                 || single_ls_info_desc->max_lsn_ != ls_info_desc->max_lsn_
                 || single_ls_info_desc->filelist_.count() != ls_info_desc->filelist_.count()
                 || single_ls_info_desc->deleted_ != ls_info_desc->deleted_
                 || single_ls_info_desc->dest_id_ != backup_piece_key.dest_id_
                 || single_ls_info_desc->round_id_ != backup_piece_key.round_id_
                 || single_ls_info_desc->piece_id_ != backup_piece_key.piece_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "single ls info desc is not equal", K(ret), K(backup_piece_key),
                    K(ls_id));
      } else if (OB_FAIL(ctx_->add_ls(backup_piece_key, ls_id))) {
        STORAGE_LOG(WARN, "failed to add ls", K(ret), K(backup_piece_key), K(ls_id));
      } else if (OB_FAIL(ctx_->get_ls_attr(backup_piece_key, ls_id, ls_attr))) {
        STORAGE_LOG(WARN, "failed to get ls", K(ret), K(backup_piece_key), K(ls_id));
      } else {
        ls_attr->single_ls_info_desc_ = single_ls_info_desc;
        single_ls_info_desc = nullptr;
      }
      if (OB_NOT_NULL(single_ls_info_desc)) {
        single_ls_info_desc->~ObSingleLSInfoDesc();
        single_ls_info_desc = nullptr;
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "piece info is not consistent with inner ls info");
  }
  return ret;
}
int ObAdminBackupPieceMetaValidationTask::inner_collect_active_piece_ls_info_(
    const share::ObPieceKey &backup_piece_key, const ObAdminBackupPieceAttr &backup_piece_attr)
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObBackupPath full_path;
  common::ObArray<share::ObLSID> ls_ids;
  share::ObArchiveStore *backup_piece_store = nullptr;
  share::ObSinglePieceDesc *single_piece_desc = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(backup_piece_store = backup_piece_attr.backup_piece_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece store is null", K(ret));
  } else if (OB_ISNULL(single_piece_desc = backup_piece_attr.backup_piece_info_desc_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece info desc is null", K(ret));
  } else if (!single_piece_desc->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece info desc is not valid", K(ret));
  } else if (!single_piece_desc->piece_.is_active()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece info desc is not active", K(ret));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::get_ls_ids_from_active_piece(
                 backup_piece_store->get_backup_dest(), ls_ids))) {
    STORAGE_LOG(WARN, "failed to get ls ids from active piece", K(ret));
  } else {
    FOREACH_X(ls_id_iter, ls_ids, OB_SUCC(ret))
    {
      const share::ObLSID ls_id = *ls_id_iter;
      ObAdminLSAttr *ls_attr = nullptr;
      if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(share::ObSingleLSInfoDesc)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc backup piece info desc", K(ret));
      } else if (OB_FAIL(ctx_->add_ls(backup_piece_key, ls_id))) {
        STORAGE_LOG(WARN, "failed to add ls", K(ret), K(backup_piece_key), K(ls_id));
      } else if (OB_FAIL(ctx_->get_ls_attr(backup_piece_key, ls_id, ls_attr))) {
        STORAGE_LOG(WARN, "failed to get ls", K(ret), K(backup_piece_key), K(ls_id));
      } else {
        ls_attr->single_ls_info_desc_ = new (alc_ptr) share::ObSingleLSInfoDesc();
        ls_attr->single_ls_info_desc_->dest_id_ = single_piece_desc->piece_.key_.dest_id_;
        ls_attr->single_ls_info_desc_->round_id_ = single_piece_desc->piece_.key_.round_id_;
        ls_attr->single_ls_info_desc_->piece_id_ = single_piece_desc->piece_.key_.piece_id_;
        ls_attr->single_ls_info_desc_->ls_id_ = ls_id;
        ls_attr->single_ls_info_desc_->start_scn_ = single_piece_desc->piece_.start_scn_;
        ls_attr->single_ls_info_desc_->checkpoint_scn_ = single_piece_desc->piece_.checkpoint_scn_;
        ls_attr->single_ls_info_desc_->min_lsn_ = palf::LOG_INVALID_LSN_VAL;
        ls_attr->single_ls_info_desc_->max_lsn_ = palf::LOG_MAX_LSN_VAL;
        if (OB_FAIL(ObAdminBackupValidationUtil::get_piece_ls_start_lsn(
                backup_piece_store->get_backup_dest(), ls_id,
                ls_attr->single_ls_info_desc_->min_lsn_, ctx_))) {
          STORAGE_LOG(WARN, "failed to get piece ls start lsn", K(ret),
                      K(backup_piece_store->get_backup_dest()), K(ls_id));
        } else {
          STORAGE_LOG(INFO, "succeed to get piece ls start lsn", K(ret),
                      K(backup_piece_store->get_backup_dest()), K(ls_id),
                      K(ls_attr->single_ls_info_desc_->min_lsn_));
        }
      }
    }
  }
  return ret;
}
int ObAdminBackupPieceMetaValidationTask::collect_and_check_piece_ls_onefile_length_()
{
  int ret = OB_SUCCESS;
  share::ObPieceKey backup_piece_key;
  share::ObBackupPath full_path;
  ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
  share::ObArchiveStore *backup_piece_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  share::ObPieceInfoDesc backup_piece_ls_list_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(task_id_, backup_piece_key))) {
    STORAGE_LOG(WARN, "failed to get backup piece key", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
    STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
  } else if (OB_ISNULL(backup_piece_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece attr is null", K(ret));
  } else if (OB_ISNULL(backup_piece_store = backup_piece_attr->backup_piece_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_piece_store->get_backup_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (backup_piece_attr->backup_piece_info_desc_->piece_.is_active()) {
    STORAGE_LOG(INFO, "ls info of active piece is acquired from outside");
  } else {
    FOREACH_X(ls_map_iter, backup_piece_attr->ls_map_, OB_SUCC(ret))
    {
      if (OB_ISNULL(ls_map_iter->second) || OB_ISNULL(ls_map_iter->second->single_ls_info_desc_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "single ls info desc is null", K(ret));
      }
      FOREACH_X(one_file, ls_map_iter->second->single_ls_info_desc_->filelist_, OB_SUCC(ret))
      {
        int64_t file_length = 0;
        full_path.reset();
        if (OB_FAIL(ObArchivePathUtil::get_ls_archive_file_path(
                backup_piece_store->get_backup_dest(), ls_map_iter->first, one_file->file_id_,
                full_path))) {
          STORAGE_LOG(WARN, "failed to get ls archive file path", K(ret),
                      K(backup_piece_store->get_backup_dest()), K(ls_map_iter->first),
                      K(one_file->file_id_));
        } else if (OB_FAIL(ObAdminBackupValidationUtil::get_file_length(
                       full_path.get_ptr(), backup_storage_info, file_length))) {
          STORAGE_LOG(WARN, "failed to get file length", K(ret), K(full_path));
        } else if (file_length != one_file->size_bytes_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "file length is not equal", K(ret), K(file_length),
                      K(one_file->size_bytes_));
        } else {
          STORAGE_LOG(DEBUG, "succeed to get file length", K(ret), K(full_path), K(file_length),
                      K(one_file->size_bytes_));
        }
        if (OB_FAIL(ret)) {
          ctx_->go_abort(full_path.get_ptr(), "log piece file is not complete, maybe lost");
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "piece ls info does not consistent with log file");
  }
  return ret;
}
void ObAdminBackupPieceMetaValidationTask::post_process_(int ret)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), K(tmp_ret));
  } else if (ctx_->processing_backup_piece_key_array_.empty()) {
    STORAGE_LOG(INFO, "backup piece key is all empty", K(ret), K(task_id_));
  } else {
    share::ObPieceKey backup_piece_key;
    ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ctx_->processing_backup_piece_key_array_.at(task_id_, backup_piece_key))) {
      STORAGE_LOG(WARN, "failed to get backup piece key", K(tmp_ret), K(task_id_));
    } else if (OB_TMP_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
      STORAGE_LOG(WARN, "failed to get backup piece attr", K(tmp_ret), K(backup_piece_key));
    } else if (OB_ISNULL(backup_piece_attr)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup piece attr is null", K(tmp_ret));
    } else if (OB_SUCC(ret)) {
      printf(CLEAR_LINE);
      printf("%s Backup piece d%ldr%ldp%ld passed meta info validation\n",
             backup_piece_attr->backup_piece_info_desc_->piece_.is_frozen() ? "Frozen" : "Active",
             ctx_->processing_backup_piece_key_array_.at(task_id_).dest_id_,
             ctx_->processing_backup_piece_key_array_.at(task_id_).round_id_,
             ctx_->processing_backup_piece_key_array_.at(task_id_).piece_id_);
    } else {
      printf(CLEAR_LINE);
      printf("%s Backup piece d%ldr%ldp%ld has corrupted\n",
             backup_piece_attr->backup_piece_info_desc_->piece_.is_frozen() ? "Frozen" : "Active",
             ctx_->processing_backup_piece_key_array_.at(task_id_).dest_id_,
             ctx_->processing_backup_piece_key_array_.at(task_id_).round_id_,
             ctx_->processing_backup_piece_key_array_.at(task_id_).piece_id_);
    }
  }
  fflush(stdout);
}
////////////////ObAdminBackupPieceMetaValidationTask End////////////////

////////////////ObAdminBackupPieceLogIterationTask Start////////////////
ObAdminBackupPieceLogIterationTask::~ObAdminBackupPieceLogIterationTask() {}
int ObAdminBackupPieceLogIterationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminBackupPieceLogIterationTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  int64_t next_task_id = task_id_ + 1;
  ObAdminBackupPieceValidationDag *piece_validation_dag = nullptr;
  ObAdminBackupPieceLogIterationTask *sibling_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(piece_validation_dag
                       = static_cast<ObAdminBackupPieceValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get piece validation dag", K(ret));
  } else if (0 == task_id_) {
    share::ObPieceKey backup_piece_key;
    ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
    if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(piece_validation_dag->get_id(),
                                                            backup_piece_key))) {
      STORAGE_LOG(WARN, "failed to get backup piece key", K(ret),
                  K(piece_validation_dag->get_id()));
    } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
      STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
    } else if (OB_FAIL(backup_piece_attr->split_lsn_range(
                   piece_validation_dag->processing_lsn_range_array_))) {
      STORAGE_LOG(WARN, "failed to split lsn range", K(ret), K(backup_piece_key));
    } else if (OB_FAIL(backup_piece_attr->stat_.add_scheduled_lsn_range_count_(
                   piece_validation_dag->processing_lsn_range_array_.count()))
               || OB_FAIL(ctx_->global_stat_.add_scheduled_lsn_range_count_(
                   piece_validation_dag->processing_lsn_range_array_.count()))) {
      STORAGE_LOG(WARN, "failed to add scheduled lsn range count", K(ret), K(backup_piece_key));
    } else {
      STORAGE_LOG(INFO, "succeed to split lsn range", K(ret), K(backup_piece_key));
    }
  }
  if (OB_SUCC(ret)) {
    if (next_task_id >= piece_validation_dag->processing_lsn_range_array_.count()) {
      ret = OB_ITER_END;
      next_task = nullptr;
      STORAGE_LOG(INFO, "no more sibling task", K(ret), K(next_task_id));
    } else if (OB_FAIL(piece_validation_dag->alloc_task(sibling_task))) {
      STORAGE_LOG(WARN, "failed to alloc sibling task", K(ret));
    } else if (OB_FAIL(sibling_task->init(next_task_id, ctx_))) {
      STORAGE_LOG(WARN, "failed to init sibling task", K(ret));
    } else {
      next_task = sibling_task;
      sibling_task = nullptr;
    }
  }

  return ret;
}
int ObAdminBackupPieceLogIterationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (OB_FAIL(iterate_log_())) {
    STORAGE_LOG(WARN, "failed to iterate log", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "succeed to process ObAdminBackupPieceLogIterationTask", K(ret),
                K(task_id_));
  }
  return ret;
}
int ObAdminBackupPieceLogIterationTask::iterate_log_()
{
  int ret = OB_SUCCESS;
  share::ObPieceKey backup_piece_key;
  ObAdminBackupPieceValidationDag *piece_validation_dag = nullptr;
  share::ObBackupPath full_path;
  share::ObLSID ls_id;
  ObAdminBackupPieceAttr *backup_piece_attr = nullptr;
  share::ObArchiveStore *backup_piece_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  std::pair<share::ObLSID, std::pair<palf::LSN, palf::LSN>> lsn_range;
  ObAdminLSAttr *ls_attr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(piece_validation_dag
                       = static_cast<ObAdminBackupPieceValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get piece validation dag", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_piece_key_array_.at(piece_validation_dag->get_id(),
                                                                 backup_piece_key))) {
    STORAGE_LOG(WARN, "failed to get backup piece key", K(ret), K(piece_validation_dag->get_id()));
  } else if (OB_FAIL(ctx_->get_backup_piece_attr(backup_piece_key, backup_piece_attr))) {
    STORAGE_LOG(WARN, "failed to get backup piece attr", K(ret), K(backup_piece_key));
  } else if (OB_ISNULL(backup_piece_store = backup_piece_attr->backup_piece_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup piece store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_piece_store->get_backup_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (task_id_ >= piece_validation_dag->processing_lsn_range_array_.count()) {
    // no lsn range to iterate
    // maybe an active or empty piece
    if (0 == backup_piece_attr->stat_.scheduled_lsn_range_count_) {
      ret = OB_SUCCESS;
      ctx_->global_stat_.add_succeed_piece_count_(1);
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "scheduled lsn range count is not zero", K(ret), K(backup_piece_key));
    }
  } else if (OB_FAIL(piece_validation_dag->processing_lsn_range_array_.at(task_id_, lsn_range))) {
    STORAGE_LOG(WARN, "failed to get backup piece key", K(ret), K(task_id_));
  } else if (FALSE_IT(ls_id = lsn_range.first)) {
  } else if (OB_FAIL(ctx_->get_ls_attr(backup_piece_key, ls_id, ls_attr))) {
    STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(backup_piece_key), K(ls_id));
  } else {
    char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH];
    MEMSET(storage_info_str, 0, sizeof(storage_info_str));
    logservice::DirArray dir_array;
    logservice::DirInfo dir_info;
    dir_info.first = backup_piece_store->get_backup_dest().get_root_path();
    backup_storage_info->get_storage_info_str(storage_info_str, sizeof(storage_info_str));
    dir_info.second = storage_info_str;
    dir_array.push_back(dir_info);

    share::SCN archive_scn;
    share::SCN restore_scn;
    palf::LSN tmp_lsn;
    logservice::ObRemoteRawPathParent raw_path_parent(ls_id);
    if (backup_piece_attr->backup_piece_info_desc_->piece_.is_active()) {
      STORAGE_LOG(INFO, "iterate active piece", K(ret), K(backup_piece_key), K(*storage_info_str));
      raw_path_parent.set(dir_array, ls_attr->single_ls_info_desc_->checkpoint_scn_);
      logservice::ObLogRawPathPieceContext *log_raw_path_piece_context = nullptr;
      if (FALSE_IT(raw_path_parent.get(log_raw_path_piece_context, restore_scn))) {
      } else if (OB_FAIL(log_raw_path_piece_context->cal_lsn_to_file_id(lsn_range.second.first))) {
        STORAGE_LOG(WARN, "failed to cal lsn to file id", K(ret));
      } else if (OB_FAIL(
                     log_raw_path_piece_context->locate_precise_piece(lsn_range.second.first))) {
        STORAGE_LOG(WARN, "failed to locate precise piece", K(ret));
      } else if (OB_FAIL(log_raw_path_piece_context->get_max_archive_log(tmp_lsn, archive_scn))) {
        STORAGE_LOG(WARN, "failed to get max archive log", K(ret));
      } else if (archive_scn < restore_scn) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "archive scn is less than restore scn", K(ret), K(backup_piece_key),
                    K(archive_scn), K(restore_scn));
      }
    } else if (backup_piece_attr->backup_piece_info_desc_->piece_.is_frozen()) {
      raw_path_parent.set(dir_array, ls_attr->single_ls_info_desc_->checkpoint_scn_);
      restore_scn = ls_attr->single_ls_info_desc_->checkpoint_scn_;
      class GetSourceFunctor
      {
      public:
        GetSourceFunctor(logservice::ObRemoteRawPathParent &raw_path_parent)
            : raw_path_parent_(raw_path_parent)
        {
        }
        int operator()(const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard)
        {
          int ret = OB_SUCCESS;
          logservice::ObRemoteRawPathParent *raw_path_parent
              = static_cast<logservice::ObRemoteRawPathParent *>(
                  logservice::ObResSrcAlloctor::alloc(share::ObLogRestoreSourceType::RAWPATH, id));
          if (OB_ISNULL(raw_path_parent)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            CLOG_LOG(WARN, "failed to allocate raw_path_parent", K(id));
          } else if (OB_FAIL(raw_path_parent_.deep_copy_to(*raw_path_parent))) {
            CLOG_LOG(WARN, "failed to deep copy to raw_path_parent");
          } else if (OB_FAIL(guard.set_source(raw_path_parent))) {
            CLOG_LOG(WARN, "failed to set location source");
          }

          if (OB_FAIL(ret) && OB_NOT_NULL(raw_path_parent)) {
            logservice::ObResSrcAlloctor::free(raw_path_parent);
            raw_path_parent = nullptr;
          }
          return ret;
        }

      private:
        logservice::ObRemoteRawPathParent &raw_path_parent_;
      };
      GetSourceFunctor get_source_func(raw_path_parent);
      logservice::ObRemoteLogGroupEntryIterator remote_iter(get_source_func);
      logservice::LogGroupEntry tmp_entry;
      const char *tmp_buf = NULL;
      int64_t tmp_buf_len = 0;

      if (OB_FAIL(remote_iter.init(
              backup_piece_attr->backup_piece_info_desc_->piece_.key_.tenant_id_, ls_id,
              share::SCN(), palf::LSN(lsn_range.second.first), palf::LSN(lsn_range.second.second),
              &piece_validation_dag->large_buffer_pool_, &piece_validation_dag->storage_handler_,
              64L * 1024 * 1024 /*64M*/))) {
        STORAGE_LOG(WARN, "failed to init remote_iter");
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(remote_iter.next(tmp_entry, tmp_lsn, tmp_buf, tmp_buf_len))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            STORAGE_LOG(DEBUG, "succeed to iterate log", K(ret));
            break;
          } else {
            STORAGE_LOG(WARN, "failed to iterate remote_log", K(ret), K(ls_id),
                        K(palf::LSN(lsn_range.second.first)),
                        K(palf::LSN(lsn_range.second.second)));
          }
        } else if (FALSE_IT(archive_scn = tmp_entry.get_scn())) {
        } else if (OB_FAIL(ctx_->limit_and_sleep(tmp_entry.get_group_entry_size()))) {
          STORAGE_LOG(WARN, "failed to limit and sleep", K(ret), K(ls_id));
        }
      }
      remote_iter.reset();
      if (OB_SUCC(ret)) {
        if (lsn_range.second.second == palf::LSN(ls_attr->single_ls_info_desc_->max_lsn_)) {
          // this is the last piece part
          if (archive_scn < restore_scn) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "archive scn is less than restore scn", K(ret), K(backup_piece_key),
                        K(archive_scn), K(restore_scn));
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup piece is not active or frozen", K(ret), K(backup_piece_key));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(backup_piece_attr->stat_.add_succeed_lsn_range_count_(1))
          || OB_FAIL(ctx_->global_stat_.add_succeed_lsn_range_count_(1))) {
        STORAGE_LOG(WARN, "failed to add succeed lsn range count", K(ret), K(ls_id));
      } else if (backup_piece_attr->stat_.scheduled_lsn_range_count_
                 == backup_piece_attr->stat_.succeed_lsn_range_count_) {
        if (OB_FAIL(ctx_->global_stat_.add_succeed_piece_count_(1))) {
          STORAGE_LOG(WARN, "failed to add succeed piece count", K(ret), K(ls_id));
        }
      }
    } else {
      char buf[1024];
      if (OB_FAIL(databuff_printf(buf, sizeof(buf) - 1,
                                  "Backup Piece in dest_id: %ld, round_id: %ld, piece_id: %ld, "
                                  "ls_id: %ld",
                                  backup_piece_attr->backup_piece_info_desc_->piece_.key_.dest_id_,
                                  backup_piece_attr->backup_piece_info_desc_->piece_.key_.round_id_,
                                  backup_piece_attr->backup_piece_info_desc_->piece_.key_.piece_id_,
                                  ls_id.id()))) {
        STORAGE_LOG(WARN, "failed to print buf", K(ret));
      } else if (OB_FAIL(ctx_->go_abort(buf, "log piece is corrupted or not consistent"))) {
        STORAGE_LOG(WARN, "failed to go abort", K(ret));
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("iterate log failed", "log piece is corrupted or not consistent");
  }
  return ret;
}
////////////////ObAdminBackupPieceLogIterationTask End////////////////

////////////////ObAdminFinishLogArchiveValidationDag Start////////////////
ObAdminFinishLogArchiveValidationDag::~ObAdminFinishLogArchiveValidationDag() {}
bool ObAdminFinishLogArchiveValidationDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminFinishLogArchiveValidationDag &other_dag
        = static_cast<const ObAdminFinishLogArchiveValidationDag &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminFinishLogArchiveValidationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                                          ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             static_cast<int64_t>(OB_SERVER_TENANT_ID)))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}
int ObAdminFinishLogArchiveValidationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->log_archive_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, "log_archive_dest=%s",
                                  ctx_->log_archive_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, "backup_piece_path="))) {
        STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_piece_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int64_t ObAdminFinishLogArchiveValidationDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}
int ObAdminFinishLogArchiveValidationDag::init(int64_t id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    id_ = id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminFinishLogArchiveValidationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAdminFinishLogArchiveValidationTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(0 /*task_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "failed to init task", K(ret));
  } /*add task child here*/ else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "failed to add task", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create first task", K(ret));
  }
  return ret;
}
////////////////ObAdminFinishLogArchiveValidationDag End////////////////

////////////////ObAdminFinishLogArchiveValidationTask Start////////////////
ObAdminFinishLogArchiveValidationTask::~ObAdminFinishLogArchiveValidationTask() {}
int ObAdminFinishLogArchiveValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminFinishLogArchiveValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (OB_FAIL(cross_check_scn_continuity_())) {
    STORAGE_LOG(WARN, "cross check scn continuity", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to process ObAdminFinishLogArchiveValidationTask", K(ret),
                K(task_id_));
  }
  return ret;
}
int ObAdminFinishLogArchiveValidationTask::cross_check_scn_continuity_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::DATABASE_VALIDATION) {
    // no need to cross check
    ret = OB_SUCCESS;
  } else {
    // get all scn range, check if could cover
    common::ObArray<std::pair<share::SCN, share::SCN>> backup_set_required_scn_range;
    common::ObArray<std::pair<share::SCN, share::SCN>> backup_piece_have_scn_range;
    if (OB_FAIL(inner_get_backup_set_scn_range_(backup_set_required_scn_range))) {
      STORAGE_LOG(WARN, "failed to get backup set scn range", K(ret));
    } else if (OB_FAIL(inner_get_backup_piece_scn_range_(backup_piece_have_scn_range))) {
      STORAGE_LOG(WARN, "failed to get backup piece scn range", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_required_scn_range.count(); ++i) {
      share::SCN left_scn = backup_set_required_scn_range[i].first;
      share::SCN right_scn = backup_set_required_scn_range[i].second;
      int64_t left_idx
          = std::upper_bound(backup_piece_have_scn_range.begin(), backup_piece_have_scn_range.end(),
                             std::make_pair(left_scn, share::SCN()))
            - backup_piece_have_scn_range.begin();
      if (left_idx >= backup_piece_have_scn_range.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "left_idx is out of range", K(ret));
      } else {
        share::SCN piece_start = backup_piece_have_scn_range[left_idx].first;
        if (piece_start < left_scn) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "piece_start is smaller than left_scn", K(ret));
        }
        share::SCN piece_end = backup_piece_have_scn_range[left_idx].second;
        for (int64_t j = left_idx + 1; OB_SUCC(ret) && j < backup_piece_have_scn_range.count();
             ++j) {
          if (piece_end >= backup_piece_have_scn_range[j].first) {
            piece_end = backup_piece_have_scn_range[j].second;
          } else {
            // discontinous
            break;
          }
          if (piece_end >= right_scn) {
            break;
          }
        }
        if (piece_end < right_scn) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "piece_end is smaller than right_scn", K(ret));
        }
        if (OB_FAIL(ret)) {
          STORAGE_LOG(WARN, "cross check scn continuity failed", K(ret), K(left_scn), K(right_scn),
                      K(piece_start), K(piece_end));
        }
      }
      if (OB_FAIL(ret)) {
        ctx_->go_abort("cross check scn continuity failed",
                       "given piece log cannot cover all backup set");
      }
    }
  }
  return ret;
}
int ObAdminFinishLogArchiveValidationTask::inner_get_backup_set_scn_range_(
    common::ObArray<std::pair<share::SCN, share::SCN>> &backup_set_required_scn_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::DATABASE_VALIDATION) {
    // no need to cross check
    ret = OB_SUCCESS;
  } else {
    FOREACH_X(backup_set_map_iter, ctx_->backup_set_map_, OB_SUCC(ret))
    {
      if (OB_ISNULL(backup_set_map_iter->second)
          || OB_ISNULL(backup_set_map_iter->second->backup_set_info_desc_)
          || !backup_set_map_iter->second->backup_set_info_desc_->backup_set_file_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup_set_attr is null", K(ret));
      } else if (OB_FAIL(backup_set_required_scn_range.push_back(
                     std::make_pair(backup_set_map_iter->second->backup_set_info_desc_
                                        ->backup_set_file_.start_replay_scn_,
                                    backup_set_map_iter->second->backup_set_info_desc_
                                        ->backup_set_file_.min_restore_scn_)))) {
        STORAGE_LOG(WARN, "failed to push back", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "backup_set_required_scn_range", K(backup_set_required_scn_range));
      std::sort(backup_set_required_scn_range.begin(), backup_set_required_scn_range.end());
    }
  }
  return ret;
}
int ObAdminFinishLogArchiveValidationTask::inner_get_backup_piece_scn_range_(
    common::ObArray<std::pair<share::SCN, share::SCN>> &backup_piece_have_scn_range)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::DATABASE_VALIDATION) {
    // no need to cross check
    ret = OB_SUCCESS;
  } else {
    FOREACH_X(backup_piece_map_iter, ctx_->backup_piece_map_, OB_SUCC(ret))
    {
      if (OB_ISNULL(backup_piece_map_iter->second)
          || OB_ISNULL(backup_piece_map_iter->second->backup_piece_info_desc_)
          || !backup_piece_map_iter->second->backup_piece_info_desc_->piece_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup_piece_attr is null", K(ret));
      } else {
        if (backup_piece_map_iter->second->backup_piece_info_desc_->piece_.is_frozen()) {
          if (OB_FAIL(backup_piece_have_scn_range.push_back(std::make_pair(
                  backup_piece_map_iter->second->backup_piece_info_desc_->piece_.start_scn_,
                  backup_piece_map_iter->second->backup_piece_info_desc_->piece_.end_scn_)))) {
            STORAGE_LOG(WARN, "failed to push back", K(ret));
          }
        } else if (backup_piece_map_iter->second->backup_piece_info_desc_->piece_.is_active()) {
          if (OB_FAIL(backup_piece_have_scn_range.push_back(std::make_pair(
                  backup_piece_map_iter->second->backup_piece_info_desc_->piece_.start_scn_,
                  backup_piece_map_iter->second->backup_piece_info_desc_->piece_
                      .checkpoint_scn_)))) {
            STORAGE_LOG(WARN, "failed to push back", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected piece status", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "backup_piece_have_scn_range", K(backup_piece_have_scn_range));
      std::sort(backup_piece_have_scn_range.begin(), backup_piece_have_scn_range.end());
    }
  }

  return ret;
}
////////////////ObAdminFinishLogArchiveValidationTask End////////////////

////////////////ObAdminDataBackupValidationDagNet Start////////////////
ObAdminDataBackupValidationDagNet::~ObAdminDataBackupValidationDagNet() {}
bool ObAdminDataBackupValidationDagNet::operator==(const ObIDagNet &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminDataBackupValidationDagNet &other_dag
        = static_cast<const ObAdminDataBackupValidationDagNet &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminDataBackupValidationDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->data_backup_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len,
                                  "[DATA_BACKUP_VALIDATION_DAG_NET]: data_path_dest=%s",
                                  ctx_->data_backup_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill comment", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len,
                                  "[DATA_BACKUP_VALIDATION_DAG_NET]: backup_set_path="))) {
        STORAGE_LOG(WARN, "failed to fill comment", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_set_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill comment", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int ObAdminDataBackupValidationDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->data_backup_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, "data_path_dest=%s",
                                  ctx_->data_backup_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill dag net key", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, "backup_set_path="))) {
        STORAGE_LOG(WARN, "failed to fill dag net key", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_set_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill dag net key", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int64_t ObAdminDataBackupValidationDagNet::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}
int ObAdminDataBackupValidationDagNet::init(ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminDataBackupValidationDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObAdminDataBackupValidationDagInitParam *dag_init_param
      = static_cast<const ObAdminDataBackupValidationDagInitParam *>(param);
  if (OB_ISNULL(dag_init_param)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "dag_init_param is null", K(ret));
  } else if (OB_ISNULL(dag_init_param->ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else if (OB_FAIL(init(dag_init_param->ctx_))) {
    STORAGE_LOG(WARN, "failed to init", K(ret));
  }
  return ret;
}
bool ObAdminDataBackupValidationDagNet::is_valid() const { return true; }
int ObAdminDataBackupValidationDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObAdminPrepareDataBackupValidationDag *prepare_data_backup_validation_dag = nullptr;
  ObAdminBackupSetMetaValidationDag *backup_set_meta_validation_dag = nullptr;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObAdminFinishDataBackupValidationDag *finish_data_backup_validation_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  // TODO: jiangshichao.jsc handle state cancelation when encounter error
  // create dag and connections
  if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "scheduler is null", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(prepare_data_backup_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to create dag", K(ret));
  } else if (OB_FAIL(prepare_data_backup_validation_dag->init(0 /*dag_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "Fail to init dag", K(ret));
  } else if (OB_FAIL(prepare_data_backup_validation_dag->create_first_task())) {
    STORAGE_LOG(WARN, "Fail to create first task", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*prepare_data_backup_validation_dag))) { // add first dag
    STORAGE_LOG(WARN, "Fail to add dag into dag_net", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(backup_set_meta_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to create dag", K(ret));
  } else if (OB_FAIL(backup_set_meta_validation_dag->init(0 /*dag_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "Fail to init dag", K(ret));
  } else if (OB_FAIL(backup_set_meta_validation_dag->create_first_task())) {
    STORAGE_LOG(WARN, "Fail to create first task", K(ret));
  } else if (OB_FAIL(
                 prepare_data_backup_validation_dag->add_child(*backup_set_meta_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add child", K(ret), KPC(prepare_data_backup_validation_dag),
                KPC(backup_set_meta_validation_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to create dag", K(ret));
  } else if (OB_FAIL(tablet_validation_dag->init(0 /*dag_id, first mean*/, ctx_,
                                                 true /*generate_sibling_dag*/))) {
    STORAGE_LOG(WARN, "Fail to init dag", K(ret));
  } else if (OB_FAIL(tablet_validation_dag->create_first_task())) {
    STORAGE_LOG(WARN, "Fail to create first task", K(ret));
  } else if (OB_FAIL(backup_set_meta_validation_dag->add_child(*tablet_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add child", K(ret), KPC(backup_set_meta_validation_dag),
                KPC(tablet_validation_dag));
  } else if (OB_FAIL(scheduler->add_dag(prepare_data_backup_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(backup_set_meta_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
    // rollback prepare_data_backup_validation_dag
  } else if (OB_FAIL(scheduler->add_dag(tablet_validation_dag))) {
    STORAGE_LOG(WARN, "Fail to add dag into dag_scheduler", K(ret));
  } else {
    // add all dags into dag_scheduler
    STORAGE_LOG(INFO, "success to schedule all dags into dag_scheduler", K(ret));
  }
  return ret;
}
////////////////ObAdminDataBackupValidationDagNet End////////////////

////////////////ObAdminPrepareDataBackupValidationDag Start////////////////
ObAdminPrepareDataBackupValidationDag::~ObAdminPrepareDataBackupValidationDag() {}
bool ObAdminPrepareDataBackupValidationDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminPrepareDataBackupValidationDag &other_dag
        = static_cast<const ObAdminPrepareDataBackupValidationDag &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminPrepareDataBackupValidationDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             static_cast<int64_t>(OB_SERVER_TENANT_ID)))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}
int ObAdminPrepareDataBackupValidationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    if (OB_NOT_NULL(ctx_->data_backup_dest_)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, "data_path_dest=%s",
                                  ctx_->data_backup_dest_->get_root_path().ptr()))) {
        STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(buf, buf_len, "backup_set_path="))) {
        STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
      } else {
        FOREACH_X(path_iter, ctx_->backup_set_path_array_, OB_SUCC(ret))
        {
          if (OB_NOT_NULL(path_iter)) {
            if (OB_FAIL(databuff_printf(buf, buf_len, "%s%s ", buf,
                                        (*path_iter)->get_root_path().ptr()))) {
              STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "path_iter is null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int64_t ObAdminPrepareDataBackupValidationDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}
int ObAdminPrepareDataBackupValidationDag::init(int64_t id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    id_ = id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminPrepareDataBackupValidationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAdminPrepareDataBackupValidationTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(0 /*task_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "failed to init task", K(ret));
  } /*add task child here*/ else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "failed to add task", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create first task", K(ret));
  }
  return ret;
}
////////////////ObAdminPrepareDataBackupValidationDag End////////////////

////////////////ObAdminPrepareDataBackupValidationTask Start////////////////
ObAdminPrepareDataBackupValidationTask::~ObAdminPrepareDataBackupValidationTask() {}
int ObAdminPrepareDataBackupValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminPrepareDataBackupValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  }
  switch (ctx_->validation_type_) {
  case ObAdminBackupValidationType::DATABASE_VALIDATION: {
    if (OB_ISNULL(ctx_->log_archive_dest_) || OB_ISNULL(ctx_->data_backup_dest_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "log_archive_dest or data_backup_dest is null", K(ret));
    } else if (OB_FAIL(check_format_file_())) {
      STORAGE_LOG(WARN, "failed to check backup format file", K(ret));
    } else if (OB_FAIL(collect_backup_set_())) {
      STORAGE_LOG(WARN, "failed to collect backup set", K(ret));
    } else {
      STORAGE_LOG(INFO, "succeed to collect backup set", K(ret));
    }
    break;
  }
  case ObAdminBackupValidationType::BACKUPSET_VALIDATION: {
    if (OB_ISNULL(ctx_->data_backup_dest_) && 0 == ctx_->backup_set_path_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backup set is empty", K(ret));
    } else if (OB_NOT_NULL(ctx_->data_backup_dest_)) {
      if (OB_FAIL(check_format_file_())) {
        STORAGE_LOG(WARN, "failed to check backup format file", K(ret));
      } else if (OB_FAIL(collect_backup_set_())) {
        STORAGE_LOG(WARN, "failed to collect backup set", K(ret));
      } else {
        STORAGE_LOG(INFO, "succeed to collect backup set", K(ret));
      }
    } else if (ctx_->backup_set_path_array_.count() > 0) {
      if (OB_FAIL(retrieve_backup_set_())) {
        STORAGE_LOG(WARN, "failed to retrieve backup set", K(ret));
      } else {
        STORAGE_LOG(INFO, "succeed to retrieve backup set", K(ret));
      }
    }
    break;
  }
  default: {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected validation type", K(ret), K(ctx_->validation_type_));
    break;
  }
  }
  post_process_(ret);
  return ret;
}
int ObAdminPrepareDataBackupValidationTask::check_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupStore backup_store;
  ObBackupFormatDesc backup_format_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_->data_backup_dest_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "data_backup_dest is null", K(ret));
  } else if (OB_FAIL(backup_store.init(*ctx_->data_backup_dest_))) {
    STORAGE_LOG(WARN, "failed to init backup data store", K(ret));
  } else if (OB_FAIL(backup_store.read_format_file(backup_format_desc))) {
    STORAGE_LOG(WARN, "failed to read backup format file", K(ret));
  } else if (ObBackupFileType::BACKUP_FORMAT_FILE != backup_format_desc.get_data_type()) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(WARN, "backup format file is not backup format file", K(ret));
  }
  return ret;
}
int ObAdminPrepareDataBackupValidationTask::collect_backup_set_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObBackupPath backup_path;
  common::ObBackupIoAdapter util;
  storage::ObBackupSetFilter op;
  ObSArray<share::ObBackupSetDesc> backup_set_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_->data_backup_dest_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "data_backup_dest is null", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_sets_dir_path(*ctx_->data_backup_dest_,
                                                                       backup_path))) {
    STORAGE_LOG(WARN, "fail to get backup place holder dir path", K(ret), K(backup_path));
  } else if (OB_FAIL(util.adaptively_list_files(backup_path.get_ptr(),
                                                ctx_->data_backup_dest_->get_storage_info(), op))) {
    STORAGE_LOG(WARN, "fail to list files", K(ret));
  } else if (OB_FAIL(op.get_backup_set_array(backup_set_array))) {
    STORAGE_LOG(WARN, "fail to get backup set names", K(ret));
  } else if (backup_set_array.empty()) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "no backup set found", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to get backup set names", K(ret), K(backup_set_array));
    ObAdminBackupSetAttr *backup_set_attr = nullptr;
    if (ctx_->backup_set_id_array_.count()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < ctx_->backup_set_id_array_.count(); ++j) {
        int64_t backup_set_id = ctx_->backup_set_id_array_.at(j);
        bool filtered = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_array.count(); ++i) {
          const share::ObBackupSetDesc &backup_set_desc = backup_set_array.at(i);
          if (backup_set_id == backup_set_desc.backup_set_id_) {
            if (OB_FAIL(ctx_->add_backup_set(backup_set_id))) {
              STORAGE_LOG(WARN, "failed to add backup set id", K(ret), K(backup_set_id));
            } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
              STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
            } else if (OB_ISNULL(backup_set_attr)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "backup set attr is null", K(ret));
            } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(ObAdminBackupSetAttr)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "failed to alloc backup set attr", K(ret));
            } else if (FALSE_IT(backup_set_attr->backup_set_store_
                                = new (alc_ptr) storage::ObBackupDataStore())) {
            } else if (OB_FAIL(backup_set_attr->backup_set_store_->init(*ctx_->data_backup_dest_,
                                                                        backup_set_desc))) {
              STORAGE_LOG(WARN, "failed to init backup set store", K(ret));
            } else {
              filtered = true;
              STORAGE_LOG(INFO, "succeed to init backup set store", K(ret), K(backup_set_id));
            }
          }
        }
        if (!filtered) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "designated backup set found", K(ret), K(backup_set_id));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_array.count(); ++i) {
        const share::ObBackupSetDesc &backup_set_desc = backup_set_array.at(i);
        int64_t backup_set_id = backup_set_desc.backup_set_id_;

        if (OB_FAIL(ctx_->add_backup_set(backup_set_id))) {
          STORAGE_LOG(WARN, "failed to add backup set id", K(ret), K(backup_set_id));
        } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
          STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
        } else if (OB_ISNULL(backup_set_attr)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "backup set attr is null", K(ret));
        } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(ObAdminBackupSetAttr)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc backup set attr", K(ret));
        } else if (FALSE_IT(backup_set_attr->backup_set_store_
                            = new (alc_ptr) storage::ObBackupDataStore())) {
        } else if (OB_FAIL(backup_set_attr->backup_set_store_->init(*ctx_->data_backup_dest_,
                                                                    backup_set_desc))) {
          STORAGE_LOG(WARN, "failed to init backup set store", K(ret));
        } else {
          STORAGE_LOG(INFO, "succeed to init backup set store", K(ret), K(backup_set_id));
        }
      }
    }
  }
  return ret;
}
int ObAdminPrepareDataBackupValidationTask::retrieve_backup_set_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  storage::ObBackupDataStore backup_set_store;
  ObExternBackupSetInfoDesc backup_set_info;
  int64_t backup_set_id = 0;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->backup_set_path_array_.count(); ++i) {
      backup_set_store.reset();
      const share::ObBackupDest *backup_set_dest = nullptr;
      if (OB_ISNULL(backup_set_dest = ctx_->backup_set_path_array_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set dest is null", K(ret));
      } else if (OB_FAIL(backup_set_store.init(*backup_set_dest))) {
        STORAGE_LOG(WARN, "failed to init backup set store", K(ret));
      } else if (OB_FAIL(backup_set_store.read_backup_set_info(backup_set_info))) {
        STORAGE_LOG(WARN, "failed to read backup set info", K(ret));
      } else if (FALSE_IT(backup_set_id = backup_set_info.backup_set_file_.backup_set_id_)) {
      } else if (OB_FAIL(ctx_->add_backup_set(backup_set_id))) {
        STORAGE_LOG(WARN, "failed to add backup set id", K(ret), K(backup_set_id));
      } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
        STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
      } else if (OB_ISNULL(backup_set_attr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set attr is null", K(ret));
      } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(ObAdminBackupSetAttr)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc backup set attr", K(ret));
      } else if (FALSE_IT(backup_set_attr->backup_set_store_
                          = new (alc_ptr) storage::ObBackupDataStore())) {
      } else if (OB_FAIL(backup_set_attr->backup_set_store_->init(*backup_set_dest))) {
        STORAGE_LOG(WARN, "failed to init backup set store", K(ret));
      } else {
        STORAGE_LOG(INFO, "succeed to init backup set store", K(ret), K(backup_set_id));
      }
    }
  }
  return ret;
}
void ObAdminPrepareDataBackupValidationTask::post_process_(int ret)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), K(tmp_ret));
  } else if (ctx_->processing_backup_set_id_array_.empty()) {
    printf(CLEAR_LINE);
    printf("Cannot found any valid OB backup set, maybe none selected\n");
    ctx_->go_abort("Error Path", "Not a valid OB backup path");
  } else {
    if (OB_SUCC(ret)) {
      printf(CLEAR_LINE);
      printf("Succeed found OB backup set: ");
      FOREACH(iter, ctx_->processing_backup_set_id_array_) { printf("%ld ", *iter); }
      printf("\n");
    } else {
      printf(CLEAR_LINE);
      printf("Data backup path seems not a valid OB backup path, please check "
             "carefully\n");
      ctx_->go_abort("Error Path", "Not a valid OB backup path");
    }
  }
  fflush(stdout);
}
////////////////ObAdminPrepareDataBackupValidationTask End////////////////

////////////////ObAdminBackupSetMetaValidationDag Start////////////////
ObAdminBackupSetMetaValidationDag::~ObAdminBackupSetMetaValidationDag() {}
bool ObAdminBackupSetMetaValidationDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminBackupSetMetaValidationDag &other_dag
        = static_cast<const ObAdminBackupSetMetaValidationDag &>(other);
    bret = id_ == other_dag.id_;
  }
  return bret;
}
int ObAdminBackupSetMetaValidationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                                       ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(id_));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             static_cast<int64_t>(OB_SERVER_TENANT_ID),
                                             backup_set_id))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}
int ObAdminBackupSetMetaValidationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t backup_set_id = -1;
    if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(id_, backup_set_id))) {
      STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(id_));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, "backup_set_id: %ld", backup_set_id))) {
      STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
    }
  }
  return ret;
}
int64_t ObAdminBackupSetMetaValidationDag::hash() const
{
  uint64_t hash_value = 0;
  int64_t ptr = reinterpret_cast<int64_t>(this);
  hash_value = common::murmurhash(&id_, sizeof(id_), hash_value);
  hash_value = common::murmurhash(&ptr, sizeof(ptr), hash_value);
  return hash_value;
}
int ObAdminBackupSetMetaValidationDag::init(int64_t id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    id_ = id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminBackupSetMetaValidationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAdminBackupSetMetaValidationTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    STORAGE_LOG(WARN, "failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(id_, ctx_))) {
    STORAGE_LOG(WARN, "failed to init task", K(ret));
  } /*add task child here*/ else if (OB_FAIL(add_task(*task))) {
    STORAGE_LOG(WARN, "failed to add task", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create first task", K(ret));
  }
  return ret;
}
int ObAdminBackupSetMetaValidationDag::generate_next_dag(ObIDag *&next_dag)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObAdminBackupSetMetaValidationDag *sibling_dag = nullptr;
  int64_t next_id = id_ + 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (next_id >= ctx_->processing_backup_set_id_array_.count()) {
    ret = OB_ITER_END;
    next_dag = nullptr;
    STORAGE_LOG(INFO, "no more backup set", K(ret), K(next_id),
                K(ctx_->processing_backup_set_id_array_.count()));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(sibling_dag))) {
    COMMON_LOG(WARN, "failed to alloc sibling_dag", K(ret));
  } else if (OB_FAIL(sibling_dag->init(next_id, ctx_))) {
    COMMON_LOG(WARN, "failed to init tablet migration dag", K(ret));
  } else {
    next_dag = sibling_dag;
    sibling_dag = nullptr;
  }

  if (OB_NOT_NULL(sibling_dag)) {
    scheduler->free_dag(*sibling_dag);
  }
  return ret;
}
////////////////ObAdminBackupSetMetaValidationDag End////////////////

////////////////ObAdminBackupSetMetaValidationTask Start////////////////
ObAdminBackupSetMetaValidationTask::~ObAdminBackupSetMetaValidationTask() {}
int ObAdminBackupSetMetaValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
    // constant self-init
    backup_sys_data_type_.set_sys_data_backup();
    backup_minor_data_type_.set_minor_data_backup();
    backup_major_data_type_.set_major_data_backup();
    inner_tablet_id_array_.push_back(common::LS_TX_CTX_TABLET);
    inner_tablet_id_array_.push_back(common::LS_TX_DATA_TABLET);
    inner_tablet_id_array_.push_back(common::LS_LOCK_TABLET);
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (ctx_->processing_backup_set_id_array_.empty()) {
    STORAGE_LOG(INFO, "backup set id is all empty", K(ret), K(task_id_));
  } else if (OB_FAIL(check_backup_set_info_())) {
    STORAGE_LOG(WARN, "failed to check placeholder", K(ret));
  } else if (OB_FAIL(check_locality_file_())) {
    STORAGE_LOG(WARN, "failed to check locality file", K(ret));
  } else if (OB_FAIL(collect_tenant_ls_meta_info_())) {
    STORAGE_LOG(WARN, "failed to collect tenant ls meta info", K(ret));
  } else if (OB_FAIL(collect_inner_tablet_meta_index_())) {
    STORAGE_LOG(WARN, "failed to collect inner tablet meta index", K(ret));
  } else if (OB_FAIL(check_inner_tablet_meta_index_())) {
    STORAGE_LOG(WARN, "failed to check inner tablet meta index", K(ret));
  } else if (OB_FAIL(collect_consistent_scn_tablet_id_())) {
    STORAGE_LOG(WARN, "failed to collect consistent tablet id", K(ret));
  } else if (OB_FAIL(collect_minor_tablet_meta_index_())) {
    STORAGE_LOG(WARN, "failed to collect minor tablet meta index", K(ret));
  } else if (OB_FAIL(collect_major_tablet_meta_index_())) {
    STORAGE_LOG(WARN, "failed to collect major tablet meta index", K(ret));
  } else if (OB_FAIL(check_tenant_tablet_meta_index())) {
    STORAGE_LOG(WARN, "failed to check tenant tablet meta index", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to process ObAdminBackupSetMetaValidationTask", K(ret), K(task_id_));
  }
  post_process_(ret);
  return ret;
}
int ObAdminBackupSetMetaValidationTask::check_backup_set_info_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  storage::ObExternBackupSetInfoDesc *backup_set_info_desc = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_set_info_path(
                 backup_set_store->get_backup_set_dest(), full_path))) {
    STORAGE_LOG(WARN, "failed to get backup set info path", K(ret), K(full_path));
  } else if (OB_ISNULL(alc_ptr
                       = ctx_->allocator_.alloc(sizeof(storage::ObExternBackupSetInfoDesc)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc backup set info desc", K(ret));
  } else if (FALSE_IT(backup_set_info_desc = new (alc_ptr) storage::ObExternBackupSetInfoDesc())) {
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                 full_path, *backup_storage_info, *backup_set_info_desc, ctx_))) {
    STORAGE_LOG(WARN, "failed to read backup info file", K(ret), K(full_path));
  } else if (OB_VSN_MAJOR(backup_set_info_desc->backup_set_file_.tenant_compatible_)
                 != OB_VSN_MAJOR(DATA_CURRENT_VERSION)
             || OB_VSN_MINOR(backup_set_info_desc->backup_set_file_.tenant_compatible_)
                    != OB_VSN_MINOR(DATA_CURRENT_VERSION)
             || OB_VSN_MAJOR_PATCH(backup_set_info_desc->backup_set_file_.tenant_compatible_)
                    != OB_VSN_MAJOR_PATCH(DATA_CURRENT_VERSION)) {
    // major, minor, major patch should be the same
    ret = OB_ERR_UNEXPECTED;
    ctx_->go_abort(full_path.get_ptr(), "Not Support OB backup version");
    STORAGE_LOG(WARN, "tenant compatible is not current version", K(ret),
                K(backup_set_info_desc->backup_set_file_.tenant_compatible_));
  } else {
    backup_set_attr->backup_set_info_desc_ = backup_set_info_desc;
    backup_set_info_desc = nullptr;
    STORAGE_LOG(INFO, "succeed to check backup set info", K(ret), K(full_path));
  }
  if (OB_NOT_NULL(backup_set_info_desc)) {
    backup_set_info_desc->~ObExternBackupSetInfoDesc();
    backup_set_info_desc = nullptr;
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "backup set info file not correct");
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::check_locality_file_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  storage::ObExternTenantLocalityInfoDesc locality_info_desc;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_locality_info_path(
                 backup_set_store->get_backup_set_dest(), full_path))) {
    STORAGE_LOG(WARN, "failed to get locality info path", K(ret), K(full_path));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                 full_path, *backup_storage_info, locality_info_desc, ctx_))) {
    STORAGE_LOG(WARN, "failed to read backup info file", K(ret), K(full_path));
  } else {
    STORAGE_LOG(INFO, "succeed to check locality file", K(ret), K(full_path));
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "locality file not correct");
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::collect_tenant_ls_meta_info_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  storage::ObBackupLSMetaInfosDesc ls_meta_infos_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_ls_meta_infos_path(
                 backup_set_store->get_backup_set_dest(), full_path))) {
    STORAGE_LOG(WARN, "failed to get locality info path", K(ret), K(full_path));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                 full_path, *backup_storage_info, ls_meta_infos_desc, ctx_))) {
    STORAGE_LOG(WARN, "failed to read backup info file", K(ret), K(full_path));
  } else {
    STORAGE_LOG(INFO, "succeed to collect tenant ls meta info", K(ret), K(full_path));
    FOREACH_X(ls_meta_package, ls_meta_infos_desc.ls_meta_packages_, OB_SUCC(ret))
    {
      const ObLSMeta &ls_meta = ls_meta_package->ls_meta_;
      ObAdminLSAttr *ls_attr = nullptr;
      if (OB_FAIL(ctx_->add_ls(backup_set_id, ls_meta.ls_id_))) {
        STORAGE_LOG(WARN, "failed to add ls", K(ret), K(ls_meta.ls_id_));
      } else if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_meta.ls_id_, ls_attr))) {
        STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_meta.ls_id_));
      } else {
        ls_attr->ls_meta_package_ = *ls_meta_package;
      }
    }
  }

  return ret;
}
int ObAdminBackupSetMetaValidationTask::collect_inner_tablet_meta_index_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else {
    ObArray<backup::ObBackupMetaIndex> meta_index_list;
    ObArray<backup::ObBackupMetaIndex> sec_meta_index_list;
    ObArray<backup::ObBackupMetaIndexIndex> index_index_list; // unused
    FOREACH_X(ls_map_iter, backup_set_attr->ls_map_, OB_SUCC(ret))
    {
      full_path.reset();
      meta_index_list.reuse();
      sec_meta_index_list.reuse();
      index_index_list.reuse();
      // handle each ls, not incluing post-created ls
      const ObLSMeta &ls_meta = ls_map_iter->second->ls_meta_package_.ls_meta_;
      ObAdminLSAttr *ls_attr = nullptr;
      int64_t sys_retry_id = 0;
      if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(backup_set_store->get_backup_set_dest(),
                                                           ls_meta.ls_id_, full_path))) {
        STORAGE_LOG(WARN, "failed to get ls backup dir path", K(ret), K(full_path));
      } else if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_meta.ls_id_, ls_attr))) {
        STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(backup_set_id), K(ls_meta.ls_id_));
      } else if (FALSE_IT(ls_attr->ls_type_ = ObAdminLSAttr::NORMAL)) {
      } else if (OB_FAIL(backup_set_store->get_max_sys_ls_retry_id(full_path, ls_meta.ls_id_,
                                                                   sys_turn_id, sys_retry_id))) {
        STORAGE_LOG(WARN, "failed to get max sys ls retry id", K(ret), K(full_path),
                    K(ls_meta.ls_id_));
      } else if (OB_FAIL(ObBackupPathUtil::get_ls_meta_index_backup_path(
                     backup_set_store->get_backup_set_dest(), ls_meta.ls_id_, backup_sys_data_type_,
                     sys_turn_id, sys_retry_id, false /*is_sec_meta*/, full_path))) {
        STORAGE_LOG(WARN, "failed to get ls meta index backup path", K(ret), K(full_path));
      } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_index_file(
                     full_path, *backup_storage_info, meta_index_list, index_index_list, ctx_))) {
        STORAGE_LOG(WARN, "failed to read backup meta index file", K(ret), K(full_path));
      } else if (FALSE_IT(full_path.reset())) {
      } else if (ObBackupPathUtil::get_ls_meta_index_backup_path(
                     backup_set_store->get_backup_set_dest(), ls_meta.ls_id_, backup_sys_data_type_,
                     sys_turn_id, sys_retry_id, true /*is_sec_meta*/, full_path)) {
        STORAGE_LOG(WARN, "failed to get ls sec meta index backup path", K(ret), K(full_path));
      } else if (FALSE_IT(index_index_list.reuse())) {
      } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_index_file(
                     full_path, *backup_storage_info, sec_meta_index_list, index_index_list,
                     ctx_))) {
        STORAGE_LOG(WARN, "failed to read backup sec meta index file", K(ret), K(full_path));
      } else {
        FOREACH_X(id_array_iter, inner_tablet_id_array_, OB_SUCC(ret))
        {
          const common::ObTabletID &inner_tablet_id = *id_array_iter;
          if (OB_FAIL(ctx_->add_tablet(backup_set_id, ls_meta.ls_id_, backup_sys_data_type_,
                                       inner_tablet_id))) {
            STORAGE_LOG(WARN, "failed to add tablet", K(ret), K(ls_meta.ls_id_),
                        K(inner_tablet_id));
          } else {
            STORAGE_LOG(INFO, "succeed to add tablet", K(ret), K(ls_meta.ls_id_),
                        K(inner_tablet_id));
          }
        }
        FOREACH_X(meta_index, meta_index_list, OB_SUCC(ret))
        {
          ObAdminTabletAttr *tablet_attr = nullptr;
          if (!meta_index->meta_key_.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(meta_index->meta_key_));
          } else if (backup::ObBackupMetaType::BACKUP_SSTABLE_META
                         != meta_index->meta_key_.meta_type_
                     && backup::ObBackupMetaType::BACKUP_TABLET_META
                            != meta_index->meta_key_.meta_type_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected meta type", K(ret), K(meta_index->meta_key_));
          } else if (OB_FAIL(
                         ctx_->get_tablet_attr(backup_set_id, ls_meta.ls_id_, backup_sys_data_type_,
                                               meta_index->meta_key_.tablet_id_, tablet_attr))) {
            STORAGE_LOG(WARN, "failed to get tablet attr", K(ret), K(ls_meta.ls_id_),
                        K(meta_index->meta_key_));
          } else if (OB_FAIL(inner_assign_meta_index_to_tablet_attr_(
                         backup_set_id, ls_meta.ls_id_, *meta_index, backup_sys_data_type_))) {
            STORAGE_LOG(WARN, "failed to assign meta index to tablet attr", K(ret),
                        K(ls_meta.ls_id_), K(meta_index->meta_key_));
          }
        }
        FOREACH_X(sec_meta_index, sec_meta_index_list, OB_SUCC(ret))
        {
          if (!sec_meta_index->meta_key_.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(sec_meta_index->meta_key_));
          } else if (backup::ObBackupMetaType::BACKUP_MACRO_BLOCK_ID_MAPPING_META
                     != sec_meta_index->meta_key_.meta_type_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "unexpected meta type", K(ret), K(sec_meta_index->meta_key_));
          } else if (OB_FAIL(inner_assign_meta_index_to_tablet_attr_(
                         backup_set_id, ls_meta.ls_id_, *sec_meta_index, backup_sys_data_type_))) {
            STORAGE_LOG(WARN, "failed to assign meta index to tablet attr", K(ret),
                        K(ls_meta.ls_id_), K(sec_meta_index->meta_key_));
          }
        }
      }
      if (OB_SUCC(ret)) {
        STORAGE_LOG(INFO, "succeed to collect inner tablet meta index", K(ret), K(backup_set_id),
                    K(ls_meta.ls_id_));
      } else {
        full_path.reset();
        if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(
                backup_set_store->get_backup_set_dest(), ls_meta.ls_id_, full_path))) {
          STORAGE_LOG(WARN, "failed to get ls dir path", K(ret), K(full_path));
        } else {
          ctx_->go_abort(full_path.get_ptr(), "ls sys data seems corrputed");
        }
      }
    }
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::inner_assign_meta_index_to_tablet_attr_(
    int64_t backup_set_id, const share::ObLSID &ls_id, const backup::ObBackupMetaIndex &meta_index,
    const share::ObBackupDataType &data_type)
{
  int ret = OB_SUCCESS;
  ObAdminTabletAttr *tablet_attr = nullptr;
  void *alc_ptr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(ls_id));
  } else if (!meta_index.meta_key_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(meta_index.meta_key_));
  } else if (!data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(data_type));
  } else if (OB_FAIL(ctx_->get_tablet_attr(backup_set_id, ls_id /*expected ls*/, data_type,
                                           meta_index.meta_key_.tablet_id_, tablet_attr))) {
    STORAGE_LOG(WARN, "failed to get tablet attr", K(ret), K(ls_id), K(meta_index.meta_key_));
  } else if (meta_index.turn_id_ == 1 && tablet_attr->ls_id_ != meta_index.ls_id_) {
    // change turn
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected ls id", K(ret), K(ls_id), K(meta_index.ls_id_));
  } else {
    switch (meta_index.meta_key_.meta_type_) {
    case backup::ObBackupMetaType::BACKUP_SSTABLE_META: {
      if (OB_NOT_NULL(tablet_attr->sstable_meta_index_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet meta index duplicat", K(ret), K(ls_id), K(meta_index.meta_key_));
      } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(backup::ObBackupMetaIndex)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        tablet_attr->ls_id_ = ls_id;
        tablet_attr->sstable_meta_index_ = new (alc_ptr) backup::ObBackupMetaIndex(meta_index);
      }
      break;
    }
    case backup::ObBackupMetaType::BACKUP_TABLET_META: {
      if (OB_NOT_NULL(tablet_attr->tablet_meta_index_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet meta index duplicat", K(ret), K(ls_id), K(meta_index.meta_key_));
      } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(backup::ObBackupMetaIndex)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        tablet_attr->ls_id_ = ls_id;
        tablet_attr->tablet_meta_index_ = new (alc_ptr) backup::ObBackupMetaIndex(meta_index);
      }
      break;
    }
    case backup::ObBackupMetaType::BACKUP_MACRO_BLOCK_ID_MAPPING_META: {
      if (OB_NOT_NULL(tablet_attr->macro_block_id_mappings_meta_index_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet meta index duplicat", K(ret), K(ls_id), K(meta_index.meta_key_));
      } else if (OB_ISNULL(alc_ptr = ctx_->allocator_.alloc(sizeof(backup::ObBackupMetaIndex)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        tablet_attr->ls_id_ = ls_id;
        tablet_attr->macro_block_id_mappings_meta_index_
            = new (alc_ptr) backup::ObBackupMetaIndex(meta_index);
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected meta type", K(ret), K(meta_index.meta_key_));
    }
    }
  }

  return ret;
}
int ObAdminBackupSetMetaValidationTask::check_inner_tablet_meta_index_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup data store is null", K(ret));
  } else {
    FOREACH_X(ls_map_iter, backup_set_attr->ls_map_, OB_SUCC(ret))
    {
      share::ObLSID ls_id = ls_map_iter->first;
      FOREACH_X(id_array_iter, inner_tablet_id_array_, OB_SUCC(ret))
      {
        const common::ObTabletID &inner_tablet_id = *id_array_iter;
        // should have all the attrs
        ObAdminTabletAttr *tablet_attr = nullptr;
        if (OB_FAIL(ctx_->get_tablet_attr(backup_set_id, ls_id, backup_sys_data_type_,
                                          inner_tablet_id, tablet_attr))) {
          STORAGE_LOG(WARN, "failed to get tablet attr", K(ret), K(ls_id), K(inner_tablet_id));
        } else if (OB_ISNULL(tablet_attr)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet attr is null", K(ret), K(ls_id), K(inner_tablet_id));
        } else if (OB_ISNULL(tablet_attr->sstable_meta_index_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet meta index is null", K(ret), K(ls_id), K(inner_tablet_id));
        } else if (OB_ISNULL(tablet_attr->tablet_meta_index_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet meta index is null", K(ret), K(ls_id), K(inner_tablet_id));
        } else if (OB_ISNULL(tablet_attr->macro_block_id_mappings_meta_index_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet meta index is null", K(ret), K(ls_id), K(inner_tablet_id));
        }
      }
      if (OB_SUCC(ret)) {
        STORAGE_LOG(INFO, "succeed to check inner tablet meta index", K(ret), K(backup_set_id),
                    K(ls_id));
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("ls sys data", "sys data seems corrputed");
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::collect_consistent_scn_tablet_id_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  int64_t minor_turn_id = 1; // found turn 1 in minor
  storage::ObBackupDataTabletToLSDesc minor_tablet_to_ls_desc;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup data store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_data_tablet_ls_info_path(
                 backup_set_store->get_backup_set_dest(), backup_minor_data_type_, minor_turn_id,
                 full_path))) {
    STORAGE_LOG(WARN, "failed to get minor tablet ls info path", K(ret), K(backup_minor_data_type_),
                K(minor_turn_id));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_info_file(
                 full_path, *backup_storage_info, minor_tablet_to_ls_desc, ctx_))) {
    STORAGE_LOG(WARN, "failed to read backup info file", K(ret), K(full_path));
  } else {
    FOREACH_X(desc_iter, minor_tablet_to_ls_desc.tablet_to_ls_, OB_SUCC(ret))
    {
      const share::ObLSID &ls_id = desc_iter->ls_id_;
      ObAdminLSAttr *ls_attr = nullptr;
      if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // encountered with post constructed logstream
          STORAGE_LOG(WARN, "post constructed logstream", K(ret), K(ls_id));
          ret = OB_SUCCESS;
          if (OB_FAIL(ctx_->add_ls(backup_set_id, ls_id))) {
            STORAGE_LOG(WARN, "failed to add ls", K(ret), K(ls_id));
          } else if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
            STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
          } else if (FALSE_IT(ls_attr->ls_type_ = ObAdminLSAttr::POST_CONSTRUCTED)) {
          } else {
            STORAGE_LOG(WARN, "post constructed logstream fixed", K(ret), K(ls_id));
          }
        } else {
          STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
        }
      }

      if OB_SUCC (ret) {
        bool ls_complecity = false;
        FOREACH_X(id_array_iter, desc_iter->tablet_id_list_, OB_SUCC(ret))
        {
          const common::ObTabletID &tablet_id = *id_array_iter;
          if (tablet_id.is_ls_inner_tablet()) {
            // record inner but not backup for post constructed
            // just skip
            ls_complecity = true;
          } else if (OB_FAIL(ctx_->add_tablet(backup_set_id, ls_id, backup_minor_data_type_,
                                              tablet_id))) {
            STORAGE_LOG(WARN, "failed to add tablet", K(ret), K(ls_id), K(tablet_id));
          } else if (OB_FAIL(ctx_->add_tablet(backup_set_id, ls_id, backup_major_data_type_,
                                              tablet_id))) {
            STORAGE_LOG(WARN, "failed to add tablet", K(ret), K(ls_id), K(tablet_id));
          }
        }
        if (OB_SUCC(ret)) {
          if (!ls_complecity) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "ls complecity is false", K(ret), K(ls_id));
          } else {
            STORAGE_LOG(INFO, "succeed to collect tenant consistent scn tablet id", K(ret),
                        K(backup_set_id), K(ls_id));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(backup_set_attr->minor_tablet_id_.begin(), backup_set_attr->minor_tablet_id_.end());
      std::sort(backup_set_attr->major_tablet_id_.begin(), backup_set_attr->major_tablet_id_.end());
      STORAGE_LOG(INFO, "succeed to collect consistent tablet id", K(ret), K(backup_set_id));
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "consistent scn data info seems corrputed");
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::collect_minor_tablet_meta_index_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  int64_t minor_turn_id = 0;
  int64_t minor_retry_id = -1;
  ObArray<backup::ObBackupMetaIndex> meta_index_list;
  ObArray<backup::ObBackupMetaIndex> sec_meta_index_list;
  ObArray<backup::ObBackupMetaIndexIndex> index_index_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup data store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (FALSE_IT(minor_turn_id
                      = backup_set_attr->backup_set_info_desc_->backup_set_file_.minor_turn_id_)
             || 0 == minor_turn_id) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "minor_turn_id is 0", K(ret));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::get_tenant_meta_index_retry_id(
                 backup_set_store->get_backup_set_dest(), backup_minor_data_type_,
                 backup::ObBackupFileType::BACKUP_META_INDEX_FILE, minor_turn_id,
                 minor_retry_id))) {
    STORAGE_LOG(WARN, "failed to get tenant minor meta index retry id", K(ret),
                K(backup_minor_data_type_), K(minor_turn_id), K(minor_retry_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_meta_index_backup_path(
                 backup_set_store->get_backup_set_dest(), backup_minor_data_type_, minor_turn_id,
                 minor_retry_id, false /*is_sec_meta*/, full_path))) {
    STORAGE_LOG(WARN, "failed to get minor meta index backup path", K(ret), K(full_path));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_index_file(
                 full_path, *backup_storage_info, meta_index_list, index_index_list, ctx_))) {
    STORAGE_LOG(WARN, "failed to read minor meta index file", K(ret), K(full_path));
  } else if (FALSE_IT(full_path.reset())) {
  } else if (FALSE_IT(index_index_list.reuse())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_meta_index_backup_path(
                 backup_set_store->get_backup_set_dest(), backup_minor_data_type_, minor_turn_id,
                 minor_retry_id, true /*is_sec_meta*/, full_path))) {
    STORAGE_LOG(WARN, "failed to get minor sec meta index backup path", K(ret), K(full_path));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_index_file(
                 full_path, *backup_storage_info, sec_meta_index_list, index_index_list, ctx_))) {
    STORAGE_LOG(WARN, "failed to read minor sec meta index file", K(ret), K(full_path));
  } else if (OB_FAIL(inner_check_tablet_meta_index_(meta_index_list, sec_meta_index_list))) {
    STORAGE_LOG(WARN, "failed to check between meta and sec meta index", K(ret));
  } else {
    FOREACH_X(sec_meta_index, sec_meta_index_list, OB_SUCC(ret))
    {
      if (OB_ISNULL(sec_meta_index)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta index is null", K(ret));
      } else if (!sec_meta_index->meta_key_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(sec_meta_index->meta_key_));
      } else if (backup::ObBackupMetaType::BACKUP_MACRO_BLOCK_ID_MAPPING_META
                 != sec_meta_index->meta_key_.meta_type_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(sec_meta_index->meta_key_));
      } else if (sec_meta_index->meta_key_.tablet_id_.is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ls inner tablet", K(ret), K(sec_meta_index->meta_key_));
      } else if (sec_meta_index->backup_set_id_ != backup_set_id) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set id not match", K(ret), K(sec_meta_index->backup_set_id_),
                    K(backup_set_id));
      } else {
        ObLSID ls_id = sec_meta_index->ls_id_;
        ObAdminLSAttr *ls_attr = nullptr;
        if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // encountered with post constructed logstream
            STORAGE_LOG(WARN, "post constructed logstream", K(ret), K(ls_id));
            ret = OB_SUCCESS;
            if (OB_FAIL(ctx_->add_ls(backup_set_id, ls_id))) {
              STORAGE_LOG(WARN, "failed to add ls", K(ret), K(ls_id));
            } else if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
              STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
            } else if (FALSE_IT(ls_attr->ls_type_ = ObAdminLSAttr::POST_CONSTRUCTED)) {
            } else {
              STORAGE_LOG(WARN, "post constructed logstream fixed", K(ret), K(ls_id));
            }
          } else {
            STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
          }
        }
        if (OB_FAIL(ret)) {
          STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
        } else if (OB_FAIL(inner_assign_meta_index_to_tablet_attr_(
                       backup_set_id, ls_id, *sec_meta_index, backup_minor_data_type_))) {
          STORAGE_LOG(WARN, "failed to assign meta index to tablet attr", K(ret), K(ls_id),
                      K(sec_meta_index->meta_key_));
        }
      }
    }

    FOREACH_X(meta_index, meta_index_list, OB_SUCC(ret))
    {
      if (OB_ISNULL(meta_index)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta index is null", K(ret));
      } else if (!meta_index->meta_key_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(meta_index->meta_key_));
      } else if (backup::ObBackupMetaType::BACKUP_SSTABLE_META != meta_index->meta_key_.meta_type_
                 && backup::ObBackupMetaType::BACKUP_TABLET_META
                        != meta_index->meta_key_.meta_type_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(meta_index->meta_key_));
      } else if (meta_index->meta_key_.tablet_id_.is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ls inner tablet", K(ret), K(meta_index->meta_key_));
      } else if (meta_index->backup_set_id_ != backup_set_id) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set id not match", K(ret), K(meta_index->backup_set_id_),
                    K(backup_set_id));
      } else {
        ObLSID ls_id = meta_index->ls_id_;
        ObAdminLSAttr *ls_attr = nullptr;
        if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
          // should not have more ls here
          STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
        } else if (OB_FAIL(inner_assign_meta_index_to_tablet_attr_(
                       backup_set_id, ls_id, *meta_index, backup_minor_data_type_))) {
          STORAGE_LOG(WARN, "failed to assign meta index to tablet attr", K(ret), K(ls_id),
                      K(meta_index->meta_key_));
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "minor data info seems corrputed");
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::collect_major_tablet_meta_index_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  int64_t backup_set_id = -1;
  share::ObBackupPath full_path;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  storage::ObBackupDataStore *backup_set_store = nullptr;
  share::ObBackupStorageInfo *backup_storage_info = nullptr;
  int64_t major_turn_id = 0;
  int64_t major_retry_id = -1;
  ObArray<backup::ObBackupMetaIndex> meta_index_list;
  ObArray<backup::ObBackupMetaIndex> sec_meta_index_list;
  ObArray<backup::ObBackupMetaIndexIndex> index_index_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret));
  } else if (OB_ISNULL(backup_set_store = backup_set_attr->backup_set_store_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup data store is null", K(ret));
  } else if (OB_ISNULL(backup_storage_info
                       = backup_set_store->get_backup_set_dest().get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup storage info is null", K(ret));
  } else if (FALSE_IT(major_turn_id
                      = backup_set_attr->backup_set_info_desc_->backup_set_file_.major_turn_id_)
             || 0 == major_turn_id) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "major_turn_id is 0", K(ret));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::get_tenant_meta_index_retry_id(
                 backup_set_store->get_backup_set_dest(), backup_major_data_type_,
                 backup::ObBackupFileType::BACKUP_META_INDEX_FILE, major_turn_id,
                 major_retry_id))) {
    STORAGE_LOG(WARN, "failed to get tenant major meta index retry id", K(ret),
                K(backup_major_data_type_), K(major_turn_id), K(major_retry_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_meta_index_backup_path(
                 backup_set_store->get_backup_set_dest(), backup_major_data_type_, major_turn_id,
                 major_retry_id, false /*is_sec_meta*/, full_path))) {
    STORAGE_LOG(WARN, "failed to get major meta index backup path", K(ret), K(full_path));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_index_file(
                 full_path, *backup_storage_info, meta_index_list, index_index_list, ctx_))) {
    STORAGE_LOG(WARN, "failed to read major meta index file", K(ret), K(full_path));
  } else if (FALSE_IT(full_path.reset())) {
  } else if (FALSE_IT(index_index_list.reuse())) {
  } else if (OB_FAIL(ObBackupPathUtil::get_tenant_meta_index_backup_path(
                 backup_set_store->get_backup_set_dest(), backup_major_data_type_, major_turn_id,
                 major_retry_id, true /*is_sec_meta*/, full_path))) {
    STORAGE_LOG(WARN, "failed to get major sec meta index backup path", K(ret), K(full_path));
  } else if (OB_FAIL(ObAdminBackupValidationUtil::read_backup_index_file(
                 full_path, *backup_storage_info, sec_meta_index_list, index_index_list, ctx_))) {
    STORAGE_LOG(WARN, "failed to read major sec meta index file", K(ret), K(full_path));
  } else if (OB_FAIL(inner_check_tablet_meta_index_(meta_index_list, sec_meta_index_list))) {
    STORAGE_LOG(WARN, "failed to check between meta and sec meta index", K(ret));
  } else {
    FOREACH_X(sec_meta_index, sec_meta_index_list, OB_SUCC(ret))
    {
      if (OB_ISNULL(sec_meta_index)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta index is null", K(ret));
      } else if (!sec_meta_index->meta_key_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(sec_meta_index->meta_key_));
      } else if (backup::ObBackupMetaType::BACKUP_MACRO_BLOCK_ID_MAPPING_META
                 != sec_meta_index->meta_key_.meta_type_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(sec_meta_index->meta_key_));
      } else if (sec_meta_index->meta_key_.tablet_id_.is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ls inner tablet", K(ret), K(sec_meta_index->meta_key_));
      } else if (sec_meta_index->backup_set_id_ != backup_set_id) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set id not match", K(ret), K(sec_meta_index->backup_set_id_),
                    K(backup_set_id));
      } else {
        ObLSID ls_id = sec_meta_index->ls_id_;
        ObAdminLSAttr *ls_attr = nullptr;
        if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            // encountered with post constructed logstream
            STORAGE_LOG(WARN, "post constructed logstream", K(ret), K(ls_id));
            ret = OB_SUCCESS;
            if (OB_FAIL(ctx_->add_ls(backup_set_id, ls_id))) {
              STORAGE_LOG(WARN, "failed to add ls", K(ret), K(ls_id));
            } else if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
              STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
            } else if (FALSE_IT(ls_attr->ls_type_ = ObAdminLSAttr::POST_CONSTRUCTED)) {
            } else {
              STORAGE_LOG(WARN, "post constructed logstream fixed", K(ret), K(ls_id));
            }
          } else {
            STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
          }
        }
        if (OB_FAIL(ret)) {
          STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
        } else if (OB_FAIL(inner_assign_meta_index_to_tablet_attr_(
                       backup_set_id, ls_id, *sec_meta_index, backup_major_data_type_))) {
          STORAGE_LOG(WARN, "failed to assign meta index to tablet attr", K(ret), K(ls_id),
                      K(sec_meta_index->meta_key_));
        }
      }
    }

    FOREACH_X(meta_index, meta_index_list, OB_SUCC(ret))
    {
      if (OB_ISNULL(meta_index)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "meta index is null", K(ret));
      } else if (!meta_index->meta_key_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(meta_index->meta_key_));
      } else if (backup::ObBackupMetaType::BACKUP_SSTABLE_META != meta_index->meta_key_.meta_type_
                 && backup::ObBackupMetaType::BACKUP_TABLET_META
                        != meta_index->meta_key_.meta_type_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(meta_index->meta_key_));
      } else if (meta_index->meta_key_.tablet_id_.is_ls_inner_tablet()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected ls inner tablet", K(ret), K(meta_index->meta_key_));
      } else if (meta_index->backup_set_id_ != backup_set_id) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set id not match", K(ret), K(meta_index->backup_set_id_),
                    K(backup_set_id));
      } else {
        ObLSID ls_id = meta_index->ls_id_;
        ObAdminLSAttr *ls_attr = nullptr;
        if (OB_FAIL(ctx_->get_ls_attr(backup_set_id, ls_id, ls_attr))) {
          // should not have more ls here
          STORAGE_LOG(WARN, "failed to get ls attr", K(ret), K(ls_id));
        } else if (OB_FAIL(inner_assign_meta_index_to_tablet_attr_(
                       backup_set_id, ls_id, *meta_index, backup_major_data_type_))) {
          STORAGE_LOG(WARN, "failed to assign meta index to tablet attr", K(ret), K(ls_id),
                      K(meta_index->meta_key_));
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort(full_path.get_ptr(), "major data info seems corrputed");
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::inner_check_tablet_meta_index_(
    const ObArray<backup::ObBackupMetaIndex> &meta_index_list,
    const ObArray<backup::ObBackupMetaIndex> &sec_meta_index_list)
{
  // inner check between meta index
  int ret = OB_SUCCESS;
  ObArray<const backup::ObBackupMetaIndex *> sstable_meta_index_list;
  ObArray<const backup::ObBackupMetaIndex *> tablet_meta_index_list;
  ObArray<const backup::ObBackupMetaIndex *> macro_block_id_mappings_meta_index_list;
  for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_list.count(); ++i) {
    const backup::ObBackupMetaIndex *meta_index = &meta_index_list.at(i);
    if (!meta_index->meta_key_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(*meta_index));
    } else if (backup::ObBackupMetaType::BACKUP_SSTABLE_META == meta_index->meta_key_.meta_type_) {
      if (OB_FAIL(sstable_meta_index_list.push_back(meta_index))) {
        STORAGE_LOG(WARN, "failed to push back sstable meta index", K(ret), K(*meta_index));
      }
    } else if (backup::ObBackupMetaType::BACKUP_TABLET_META == meta_index->meta_key_.meta_type_) {
      if (OB_FAIL(tablet_meta_index_list.push_back(meta_index))) {
        STORAGE_LOG(WARN, "failed to push back tablet meta index", K(ret), K(*meta_index));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected meta type", K(ret), K(*meta_index));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sec_meta_index_list.count(); ++i) {
    const backup::ObBackupMetaIndex *meta_index = &sec_meta_index_list.at(i);
    if (!meta_index->meta_key_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to check meta key", K(ret), K(*meta_index));
    } else if (backup::ObBackupMetaType::BACKUP_MACRO_BLOCK_ID_MAPPING_META
               == meta_index->meta_key_.meta_type_) {
      if (OB_FAIL(macro_block_id_mappings_meta_index_list.push_back(meta_index))) {
        STORAGE_LOG(WARN,
                    "failed to push back macro block id mapping "
                    "meta index",
                    K(ret), K(*meta_index));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected meta type", K(ret), K(*meta_index));
    }
  }

  if (tablet_meta_index_list.count() != macro_block_id_mappings_meta_index_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN,
                "tablet meta index list size not equal macro block id mapping "
                "meta index list size",
                K(ret), K(tablet_meta_index_list.count()),
                K(macro_block_id_mappings_meta_index_list.count()));
  } else if (sstable_meta_index_list.count() > tablet_meta_index_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "sstable meta index list size not equal tablet meta index list size", K(ret),
                K(sstable_meta_index_list.count()), K(tablet_meta_index_list.count()));
  } else {
    uint64_t i = 0; // sstable_meta_index_list idx
    uint64_t j = 0; // tablet_meta_index_list
    uint64_t k = 0; // macro_block_id_mappings_meta_index_list
    while (OB_SUCC(ret) && j < tablet_meta_index_list.count()
           && k < macro_block_id_mappings_meta_index_list.count()) {
      if (tablet_meta_index_list.at(j)->meta_key_.tablet_id_
          != macro_block_id_mappings_meta_index_list.at(k)->meta_key_.tablet_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet id not match", K(ret),
                    K(tablet_meta_index_list.at(j)->meta_key_.tablet_id_),
                    K(macro_block_id_mappings_meta_index_list.at(k)->meta_key_.tablet_id_));
      } else if (tablet_meta_index_list.at(j)->backup_set_id_
                 != macro_block_id_mappings_meta_index_list.at(k)->backup_set_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set id not match", K(ret),
                    K(tablet_meta_index_list.at(j)->backup_set_id_),
                    K(macro_block_id_mappings_meta_index_list.at(k)->backup_set_id_));
      } else if (tablet_meta_index_list.at(j)->ls_id_
                 != macro_block_id_mappings_meta_index_list.at(k)->ls_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "ls id not match", K(ret), K(tablet_meta_index_list.at(j)->ls_id_),
                    K(macro_block_id_mappings_meta_index_list.at(k)->ls_id_));
      } else if (tablet_meta_index_list.at(j)->turn_id_
                 != macro_block_id_mappings_meta_index_list.at(k)->turn_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "turn id not match", K(ret), K(tablet_meta_index_list.at(j)->turn_id_),
                    K(macro_block_id_mappings_meta_index_list.at(k)->turn_id_));
      } else if (tablet_meta_index_list.at(j)->retry_id_
                 != macro_block_id_mappings_meta_index_list.at(k)->retry_id_) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "retry id not match", K(ret), K(tablet_meta_index_list.at(j)->retry_id_),
                    K(macro_block_id_mappings_meta_index_list.at(k)->retry_id_));
      } else {
        // till here tablet_meta and macro_block_id_mapping_meta
        // is match advance i
        while (i + 1 < sstable_meta_index_list.count()
               && sstable_meta_index_list.at(i + 1)->meta_key_.tablet_id_
                      <= tablet_meta_index_list.at(j)->meta_key_.tablet_id_) {
          i++;
        }
        if (i < sstable_meta_index_list.count()
            && sstable_meta_index_list.at(i)->meta_key_.tablet_id_
                   == tablet_meta_index_list.at(j)->meta_key_.tablet_id_) {
          // this tablet have sstable
          if (sstable_meta_index_list.at(i)->backup_set_id_
              != macro_block_id_mappings_meta_index_list.at(k)->backup_set_id_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "backup set id not match", K(ret),
                        K(sstable_meta_index_list.at(i)->backup_set_id_),
                        K(macro_block_id_mappings_meta_index_list.at(k)->backup_set_id_));
          } else if (sstable_meta_index_list.at(i)->ls_id_
                     != macro_block_id_mappings_meta_index_list.at(k)->ls_id_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "ls id not match", K(ret), K(sstable_meta_index_list.at(i)->ls_id_),
                        K(macro_block_id_mappings_meta_index_list.at(k)->ls_id_));
          } else if (sstable_meta_index_list.at(i)->turn_id_
                     != macro_block_id_mappings_meta_index_list.at(k)->turn_id_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "turn id not match", K(ret),
                        K(sstable_meta_index_list.at(i)->turn_id_),
                        K(macro_block_id_mappings_meta_index_list.at(k)->turn_id_));
          } else if (sstable_meta_index_list.at(i)->retry_id_
                     != macro_block_id_mappings_meta_index_list.at(k)->retry_id_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "retry id not match", K(ret),
                        K(sstable_meta_index_list.at(i)->retry_id_),
                        K(macro_block_id_mappings_meta_index_list.at(k)->retry_id_));
          } else {
            i++;
          }
        }
        if (OB_SUCC(ret)) {
          j++;
          k++;
        }
      }
    }
    if (OB_FAIL(ret) || i != sstable_meta_index_list.count() || j != tablet_meta_index_list.count()
        || k != macro_block_id_mappings_meta_index_list.count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "process not finish", K(ret), K(i), K(j), K(k));
    } else {
      STORAGE_LOG(INFO, "check tablet meta index finish", K(ret), K(i), K(j), K(k));
    }
  }
  return ret;
}
int ObAdminBackupSetMetaValidationTask::check_tenant_tablet_meta_index()
{
  // cross check between tenant minor/major meta index from initial to end
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  common::ObArray<std::pair<share::ObLSID, common::ObTabletID>> missing_tablet;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set store", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup data store is null", K(ret));
  } else if (backup_set_attr->minor_tablet_id_.count()
             != backup_set_attr->minor_tablet_map_.size()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "minor tablet id size not equal minor tablet map size", K(ret),
                K(backup_set_attr->minor_tablet_id_.count()),
                K(backup_set_attr->minor_tablet_map_.size()));
  } else if (backup_set_attr->major_tablet_id_.count()
             != backup_set_attr->major_tablet_map_.size()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "major tablet id size not equal major tablet map size", K(ret),
                K(backup_set_attr->major_tablet_id_.count()),
                K(backup_set_attr->major_tablet_map_.size()));
  } else if (backup_set_attr->minor_tablet_id_.count()
             != backup_set_attr->major_tablet_id_.count()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "minor tablet id size not equal major tablet id size", K(ret),
                K(backup_set_attr->minor_tablet_id_.count()),
                K(backup_set_attr->major_tablet_id_.count()));
  } else {
    // cross check tablet, found any may be missing
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_set_attr->minor_tablet_id_.count()
                        && i < backup_set_attr->major_tablet_id_.count();
         i++) {
      ObAdminTabletAttr *minor_tablet_attr = nullptr;
      ObAdminTabletAttr *major_tablet_attr = nullptr;
      if (backup_set_attr->minor_tablet_id_.at(i) != backup_set_attr->major_tablet_id_.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "minor tablet id not equal major tablet id", K(ret),
                    K(backup_set_attr->minor_tablet_id_.at(i)),
                    K(backup_set_attr->major_tablet_id_.at(i)));
      } else if (OB_FAIL(backup_set_attr->minor_tablet_map_.get_refactored(
                     backup_set_attr->minor_tablet_id_.at(i), minor_tablet_attr))) {
        STORAGE_LOG(WARN, "failed to get minor tablet store", K(ret),
                    K(backup_set_attr->minor_tablet_id_.at(i)));
      } else if (OB_FAIL(backup_set_attr->major_tablet_map_.get_refactored(
                     backup_set_attr->major_tablet_id_.at(i), major_tablet_attr))) {
        STORAGE_LOG(WARN, "failed to get major tablet store", K(ret),
                    K(backup_set_attr->major_tablet_id_.at(i)));
      } else if ((OB_ISNULL(minor_tablet_attr->tablet_meta_index_)
                  && OB_ISNULL(major_tablet_attr->tablet_meta_index_))
                 || (OB_NOT_NULL(minor_tablet_attr->tablet_meta_index_)
                     && OB_ISNULL(major_tablet_attr->tablet_meta_index_))) {
        // maybe deleted or lost
        if (OB_FAIL(missing_tablet.push_back(
                {minor_tablet_attr->ls_id_, backup_set_attr->minor_tablet_id_.at(i)}))) {
          STORAGE_LOG(WARN, "failed to push back tablet id", K(ret));
        }
      } else if (OB_ISNULL(minor_tablet_attr->tablet_meta_index_)
                 && OB_NOT_NULL(major_tablet_attr->tablet_meta_index_)) {
        // bug
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "minor tablet meta should exist but got lost", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && missing_tablet.count() > 0) {
    // query __all_backup_skipped_tablet_history
    STORAGE_LOG(WARN, "missing tablet", K(ret), K(missing_tablet));
    common::ObSqlString sql;
    ObMySQLProxy::MySQLResult res;
    sqlclient::ObMySQLResult *result = nullptr;
    if (OB_ISNULL(ctx_->sql_proxy_)) {
      // ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "sql proxy is nullptr, skip query check", K(ret));
    } else {
      FOREACH_X(missing_tablet_iter, missing_tablet, OB_SUCC(ret))
      {
        sql.reset();
        res.reset();
        result = nullptr;
        if (OB_FAIL(sql.assign_fmt(
                "SELECT * FROM %s WHERE tenant_id = %ld AND backup_set_id = %ld AND ls_id = %ld "
                "AND "
                "tablet_id = %ld",
                OB_ALL_BACKUP_SKIPPED_TABLET_HISTORY_TNAME,
                backup_set_attr->backup_set_info_desc_->backup_set_file_.tenant_id_,
                backup_set_attr->backup_set_info_desc_->backup_set_file_.backup_set_id_,
                missing_tablet_iter->first.id(), missing_tablet_iter->second.id()))) {
          STORAGE_LOG(WARN, "failed to assign sql", K(ret));
        } else if (OB_FAIL(ctx_->sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
          STORAGE_LOG(WARN, "failed to read sql", K(ret));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "result is null", K(ret));
        } else {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              STORAGE_LOG(WARN, "failed to next", K(ret));
            } else {
              // no such a deletion tablet
              STORAGE_LOG(WARN, "missing tablet is not normal deleted", K(ret),
                          K(missing_tablet_iter->second));
              int tmp_ret = OB_SUCCESS;
              char buf[1024];
              if (OB_TMP_FAIL(databuff_printf(
                      buf, sizeof(buf) - 1, "Tablet %ld in Backup Set %ld is missing",
                      missing_tablet_iter->second.id(),
                      backup_set_attr->backup_set_info_desc_->backup_set_file_.backup_set_id_))) {
                STORAGE_LOG(WARN, "failed to print buf", K(tmp_ret));
              } else if (OB_TMP_FAIL(ctx_->go_abort(buf, "missing tablet is not normal deleted"))) {
                STORAGE_LOG(WARN, "failed to go abort", K(tmp_ret));
              }
            }
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("tenant minor/major data info", "ls minor/major data info failed cross check");
  }
  return ret;
}
void ObAdminBackupSetMetaValidationTask::post_process_(int ret)
{
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    tmp_ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), K(tmp_ret));
  } else if (ctx_->processing_backup_set_id_array_.empty()) {
    STORAGE_LOG(INFO, "backup set id is all empty", K(ret), K(task_id_));
  } else {
    if (OB_SUCC(ret)) {
      printf(CLEAR_LINE);
      printf("Backup set %ld passed meta info validation\n",
             ctx_->processing_backup_set_id_array_.at(task_id_));
    } else {
      printf(CLEAR_LINE);
      printf("Backup set %ld has corrupted meta info\n",
             ctx_->processing_backup_set_id_array_.at(task_id_));
    }
  }
  fflush(stdout);
}
////////////////ObAdminBackupSetMetaValidationTask End////////////////

////////////////ObAdminBackupTabletValidationDag Start////////////////
ObAdminBackupTabletValidationDag::~ObAdminBackupTabletValidationDag() {}
bool ObAdminBackupTabletValidationDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObAdminBackupTabletValidationDag &other_dag
        = static_cast<const ObAdminBackupTabletValidationDag &>(other);
    if (id_ == other_dag.id_ && time_identifier_ == other_dag.time_identifier_) {
      bret = true;
    }
  }
  return bret;
}
int ObAdminBackupTabletValidationDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                                      ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(id_));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             static_cast<int64_t>(OB_SERVER_TENANT_ID),
                                             backup_set_id, time_identifier_))) {
    STORAGE_LOG(WARN, "failed to fill info param", K(ret));
  }
  return ret;
}
int ObAdminBackupTabletValidationDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "get invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    int64_t backup_set_id = -1;
    if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(id_, backup_set_id))) {
      STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(id_));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, "backup_set_id: %ld time_identifier: %ld",
                                       backup_set_id, time_identifier_))) {
      STORAGE_LOG(WARN, "failed to fill dag key", K(ret));
    }
  }
  return ret;
}
int64_t ObAdminBackupTabletValidationDag::hash() const
{
  uint64_t hash_value = 0;
  int64_t ptr = reinterpret_cast<int64_t>(this);
  hash_value = common::murmurhash(&id_, sizeof(id_), hash_value);
  hash_value = common::murmurhash(&lock_, sizeof(lock_), hash_value);
  hash_value = common::murmurhash(&time_identifier_, sizeof(time_identifier_), hash_value);
  hash_value = common::murmurhash(&ptr, sizeof(ptr), hash_value);
  return hash_value;
}
int ObAdminBackupTabletValidationDag::init(int64_t id, ObAdminBackupValidationCtx *ctx,
                                           bool generate_sibling_dag)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    id_ = id;
    ctx_ = ctx;
    generate_sibling_dag_ = generate_sibling_dag;
  }
  return ret;
}
int ObAdminBackupTabletValidationDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObAdminPrepareTabletValidationTask *prepare_task = nullptr;
  ObAdminTabletMetaValidationTask *sstable_task = nullptr;
  ObAdminMacroBlockDataValidationTask *macro_block_task = nullptr;
  ObAdminFinishTabletValidationTask *finish_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "failed to alloc prepare task", K(ret));
  } else if (OB_FAIL(alloc_task(sstable_task))) {
    STORAGE_LOG(WARN, "failed to alloc sstable task", K(ret));
  } else if (OB_FAIL(alloc_task(macro_block_task))) {
    STORAGE_LOG(WARN, "failed to alloc macro block task", K(ret));
  } else if (OB_FAIL(alloc_task(finish_task))) {
    STORAGE_LOG(WARN, "failed to alloc finish task", K(ret));
  } else if (OB_FAIL(prepare_task->init(id_, ctx_))) {
    STORAGE_LOG(WARN, "failed to init prepare task", K(ret));
  } else if (OB_FAIL(sstable_task->init(0 /*task_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "failed to init sstable task", K(ret));
  } else if (OB_FAIL(macro_block_task->init(0 /*task_id, first mean*/, ctx_))) {
    STORAGE_LOG(WARN, "failed to init macro block task", K(ret));
  } else if (OB_FAIL(finish_task->init(id_, ctx_))) {
    STORAGE_LOG(WARN, "failed to init finish task", K(ret));
  } else if (OB_FAIL(macro_block_task->add_child(*finish_task))) {
    STORAGE_LOG(WARN, "failed to construct dependency chain", K(ret));
  } else if (OB_FAIL(sstable_task->add_child(*macro_block_task))) {
    STORAGE_LOG(WARN, "failed to construct dependency chain", K(ret));
  } else if (OB_FAIL(prepare_task->add_child(*sstable_task))) {
    STORAGE_LOG(WARN, "failed to construct dependency chain", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "failed to add prepare task", K(ret));
  } else if (OB_FAIL(add_task(*sstable_task))) {
    STORAGE_LOG(WARN, "failed to add sstable task", K(ret));
  } else if (OB_FAIL(add_task(*macro_block_task))) {
    STORAGE_LOG(WARN, "failed to add macro block task", K(ret));
  } else if (OB_FAIL(add_task(*finish_task))) {
    STORAGE_LOG(WARN, "failed to add finish task", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create first task", K(ret));
  }

  return ret;
}
int ObAdminBackupTabletValidationDag::generate_next_dag(ObIDag *&next_dag)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = nullptr;
  ObAdminBackupTabletValidationDag *sibling_dag = nullptr;
  int64_t next_id = id_ + 1;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (!generate_sibling_dag_) {
    ret = OB_ITER_END;
    next_dag = nullptr;
    STORAGE_LOG(INFO, "no need to generate sibling dag", K(ret));
  } else if (next_id >= ctx_->processing_backup_set_id_array_.count()) {
    ret = OB_ITER_END;
    next_dag = nullptr;
    STORAGE_LOG(INFO, "no more backup set", K(ret), K(next_id),
                K(ctx_->processing_backup_set_id_array_.count()));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(sibling_dag))) {
    COMMON_LOG(WARN, "failed to alloc sibling_dag", K(ret));
  } else if (OB_FAIL(sibling_dag->init(next_id, ctx_))) {
    COMMON_LOG(WARN, "failed to init sibling_dag", K(ret));
  } else {
    next_dag = sibling_dag;
    sibling_dag = nullptr;
  }

  if (OB_NOT_NULL(sibling_dag)) {
    scheduler->free_dag(*sibling_dag);
  }
  return ret;
}
int ObAdminBackupTabletValidationDag::add_macro_block(
    const backup::ObBackupMacroBlockIDPair &macro_block_id_pair,
    const share::ObBackupDataType &data_type)
{
  ObSpinLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (0 == processing_macro_block_array_.count()
      || processing_macro_block_array_.at(processing_macro_block_array_.count() - 1).count()
             == ObAdminBackupValidationExecutor::MAX_MACRO_BLOCK_BATCH_COUNT) {
    if (OB_FAIL(processing_macro_block_array_.push_back(
            ObArray<std::pair<backup::ObBackupMacroBlockIDPair, share::ObBackupDataType>>()))) {
      STORAGE_LOG(WARN, "failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(processing_macro_block_array_.at(processing_macro_block_array_.count() - 1)
                    .push_back({macro_block_id_pair, data_type}))) {
      STORAGE_LOG(WARN, "failed to push back", K(ret));
    }
  }
  return ret;
}
////////////////ObAdminBackupTabletValidationDag End////////////////

////////////////ObAdminPrepareTabletValidationTask Start////////////////
ObAdminPrepareTabletValidationTask::~ObAdminPrepareTabletValidationTask() {}
int ObAdminPrepareTabletValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminPrepareTabletValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (ctx_->processing_backup_set_id_array_.empty()) {
    STORAGE_LOG(INFO, "backup set id is all empty", K(ret), K(task_id_));
  } else if (OB_FAIL(collect_tablet_group_())) {
    STORAGE_LOG(WARN, "failed to collect tablet group", K(ret));
  } else {
    while (OB_FAIL(schedule_next_dag_())) {
      STORAGE_LOG(WARN, "failed to schedule next dag, but continue to try", K(ret));
    }
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO, "succeed to process ObAdminPrepareTabletValidationTask", K(ret),
                  K(task_id_));
    }
  }

  return ret;
}
int ObAdminPrepareTabletValidationTask::collect_tablet_group_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret), K(backup_set_id));
  } else if (OB_FAIL(backup_set_attr->fetch_next_tablet_group(
                 tablet_validation_dag->processing_tablet_group_,
                 tablet_validation_dag->scheduled_tablet_cnt_))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "no more tablet group", K(ret), K(backup_set_id));
    } else {
      STORAGE_LOG(WARN, "failed to fetch next tablet group", K(ret), K(backup_set_id));
    }
  }
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "succeed to collect tablet group", K(ret), K(backup_set_id),
                K(tablet_validation_dag->processing_tablet_group_.count()));
  }
  return ret;
}
int ObAdminPrepareTabletValidationTask::schedule_next_dag_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  ObAdminDataBackupValidationDagNet *dag_net = nullptr;
  ObAdminBackupTabletValidationDag *currect_dag = nullptr;
  ObAdminBackupTabletValidationDag *cloned_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_ISNULL(currect_dag = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (OB_ISNULL(dag_net = static_cast<ObAdminDataBackupValidationDagNet *>(
                           currect_dag->get_dag_net()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get dag net", K(ret));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret), K(backup_set_id));
  } else if (backup_set_attr->is_all_tablet_done()) {
    STORAGE_LOG(INFO, "no more tablet group", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(cloned_dag))) {
    STORAGE_LOG(WARN, "failed to alloc cloned_dag", K(ret));
  } else if (OB_FAIL(cloned_dag->init(task_id_, ctx_, false))) {
    STORAGE_LOG(WARN, "failed to init cloned_dag", K(ret));
  } else if (OB_FAIL(cloned_dag->create_first_task())) {
    STORAGE_LOG(WARN, "failed to create first task", K(ret));
  } else if (OB_FAIL(cloned_dag->deep_copy_children(currect_dag->get_child_nodes()))) {
    STORAGE_LOG(WARN, "failed to deep copy child", K(ret), K(cloned_dag));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*cloned_dag))) {
    STORAGE_LOG(WARN, "failed to add cloned_dag into dag net", K(ret), K(cloned_dag));
  } else if (OB_FAIL(scheduler->add_dag(cloned_dag))) {
    STORAGE_LOG(WARN, "failed to add cloned_dag into dag scheduler", K(ret), K(cloned_dag));
  } else {
    STORAGE_LOG(INFO, "succeed to schedule next tablet validation dag", K(ret), K(cloned_dag),
                KPC(dag_net));
  }
  if (!OB_SUCC(ret) && OB_NOT_NULL(scheduler) && OB_NOT_NULL(cloned_dag)) {
    STORAGE_LOG(WARN, "failed to schedule next tablet validation dag", K(ret), K(cloned_dag));
    scheduler->cancel_dag(cloned_dag);
    scheduler->free_dag(*cloned_dag);
    cloned_dag = nullptr;
  }

  return ret;
}
////////////////ObAdminPrepareTabletValidationTask End////////////////

////////////////ObAdminTabletMetaValidationTask Start////////////////
ObAdminTabletMetaValidationTask::~ObAdminTabletMetaValidationTask()
{
  for (int64_t i = 0; i < tablet_meta_.count(); i++) {
    if (OB_NOT_NULL(tablet_meta_.at(i))) {
      tablet_meta_.at(i)->~ObBackupTabletMeta();
      tablet_meta_.at(i) = nullptr;
    }
  }
  for (int64_t i = 0; i < id_mappings_meta_.count(); i++) {
    if (OB_NOT_NULL(id_mappings_meta_.at(i))) {
      id_mappings_meta_.at(i)->~ObBackupMacroBlockIDMappingsMeta();
      id_mappings_meta_.at(i) = nullptr;
    }
  }
}
int ObAdminTabletMetaValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminTabletMetaValidationTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  int64_t next_task_id = task_id_ + 1;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObAdminTabletMetaValidationTask *sibling_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (next_task_id >= tablet_validation_dag->processing_tablet_group_.count()) {
    ret = OB_ITER_END;
    next_task = nullptr;
    STORAGE_LOG(INFO, "no more sibling task", K(ret), K(next_task_id));
  } else if (OB_FAIL(tablet_validation_dag->alloc_task(sibling_task))) {
    STORAGE_LOG(WARN, "failed to alloc sibling task", K(ret));
  } else if (OB_FAIL(sibling_task->init(next_task_id, ctx_))) {
    STORAGE_LOG(WARN, "failed to init sibling task", K(ret));
  } else {
    next_task = sibling_task;
    sibling_task = nullptr;
  }
  return ret;
}
int ObAdminTabletMetaValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (OB_FAIL(collect_tablet_meta_info_())) {
    STORAGE_LOG(WARN, "failed to collect tablet meta info", K(ret));
  } else if (OB_FAIL(collect_sstable_meta_info_())) {
    STORAGE_LOG(WARN, "failed to collect sstable meta info", K(ret));
  } else if (OB_FAIL(check_sstable_meta_info_and_prepare_macro_block_())) {
    STORAGE_LOG(WARN, "failed to check sstable meta info and prepare macro block", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "succeed to process ObAdminTabletMetaValidationTask", K(ret), K(task_id_));
  }
  return ret;
}
int ObAdminTabletMetaValidationTask::collect_tablet_meta_info_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  ObArray<ObAdminTabletAttr *> current_group;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (OB_FAIL(tablet_validation_dag->processing_tablet_group_.at(task_id_, current_group))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet tablet group", K(ret));
  } else if (OB_FAIL(tablet_meta_.reserve(current_group.count()))) {
    STORAGE_LOG(WARN, "failed to reserve tablet meta", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < current_group.count(); i++) {
      const ObAdminTabletAttr *tablet_attr = nullptr;
      int64_t backup_set_id = -1;
      if (OB_ISNULL(tablet_attr = current_group.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet attr is null", K(ret));
      } else if (FALSE_IT(backup_set_id = tablet_attr->tablet_meta_index_->backup_set_id_)) {
      } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
        STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
      } else if (OB_ISNULL(backup_set_attr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set attr is null", K(ret), K(backup_set_id));
      } else if (OB_ISNULL(alc_ptr = local_allocator_.alloc(sizeof(backup::ObBackupTabletMeta)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc backup::ObBackupTabletMeta", K(ret));
      } else if (FALSE_IT(tablet_meta_.at(i) = new (alc_ptr) backup::ObBackupTabletMeta())) {
      } else if (OB_FAIL(ObAdminBackupValidationUtil::read_tablet_meta(
                     backup_set_attr->backup_set_store_->get_backup_set_dest(),
                     *tablet_attr->tablet_meta_index_, tablet_attr->data_type_, *tablet_meta_.at(i),
                     ctx_))) {
        STORAGE_LOG(WARN, "failed to read tablet meta", K(ret), K(backup_set_id),
                    K(tablet_attr->tablet_meta_index_), K(tablet_attr->data_type_));
      } else if (!tablet_meta_.at(i)->tablet_meta_.ha_status_.is_none()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet meta is not complete", K(ret), K(backup_set_id),
                    K(*tablet_meta_.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      if (current_group.count() == 2) {
        // major/minor same tablet
        if (tablet_meta_.at(0)->tablet_id_ != tablet_meta_.at(1)->tablet_id_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet id is not same", K(ret), K(tablet_meta_.at(0)->tablet_id_),
                      K(tablet_meta_.at(1)->tablet_id_));
        } else if (tablet_meta_.at(0)->tablet_meta_.clog_checkpoint_scn_
                   > tablet_meta_.at(1)->tablet_meta_.clog_checkpoint_scn_) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "minor tablet clog checkpoint scn is larger than major tablet", K(ret),
                      K(tablet_meta_.at(0)->tablet_meta_.clog_checkpoint_scn_),
                      K(tablet_meta_.at(1)->tablet_meta_.clog_checkpoint_scn_));
        } else {
          const int64_t major_snapshot_version
              = tablet_meta_.at(1)->tablet_meta_.report_status_.merge_snapshot_version_;
          const int64_t minor_snapshot_version
              = tablet_meta_.at(0)->tablet_meta_.report_status_.merge_snapshot_version_;
          if ((minor_snapshot_version <= 0
               && tablet_meta_.at(0)->tablet_meta_.table_store_flag_.with_major_sstable())
              || (major_snapshot_version <= 0
                  && tablet_meta_.at(1)->tablet_meta_.table_store_flag_.with_major_sstable())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "snapshot version is invalid", K(ret), K(minor_snapshot_version),
                        K(major_snapshot_version));
          } else if (major_snapshot_version < minor_snapshot_version) {
            ret = OB_BACKUP_MAJOR_NOT_COVER_MINOR;
            STORAGE_LOG(WARN, "major snapshot version is smaller than minor snapshot version",
                        K(ret), K(major_snapshot_version), K(minor_snapshot_version));
          }
        }
      } else if (current_group.count() == 3) {
        // sys tablet in same ls
        const share::ObLSID ls_id = tablet_meta_.at(0)->tablet_meta_.ls_id_;
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_meta_.count(); i++) {
          if (!tablet_meta_.at(i)->tablet_id_.is_ls_inner_tablet()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "tablet meta is not sys tablet", K(ret), K(tablet_meta_.at(i)));
          } else if (tablet_meta_.at(i)->tablet_meta_.ls_id_ != ls_id) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "not corresponding ls", K(ret), K(tablet_meta_.at(i)));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet group count is not 2 or 3", K(ret), K(current_group));
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("Tablet meta info", "tablet meta info is not complete");
  }
  return ret;
}
int ObAdminTabletMetaValidationTask::collect_sstable_meta_info_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObArray<ObAdminTabletAttr *> current_group;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else {
    if (OB_FAIL(tablet_validation_dag->processing_tablet_group_.at(task_id_, current_group))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to get tablet tablet group", K(ret));
    } else if (OB_FAIL(id_mappings_meta_.reserve(current_group.count()))) {
      STORAGE_LOG(WARN, "failed to reserve id_mappings_meta", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < current_group.count(); i++) {
        ObAdminTabletAttr *tablet_attr = nullptr;
        int64_t backup_set_id = -1;
        ObAdminBackupSetAttr *backup_set_attr = nullptr;
        common::ObArray<backup::ObBackupSSTableMeta> inner_sstable_meta;
        if (OB_ISNULL(tablet_attr = current_group.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet attr is null", K(ret));
        } else if (FALSE_IT(backup_set_id = tablet_attr->tablet_meta_index_->backup_set_id_)) {
        } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
          STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
        } else if (OB_ISNULL(backup_set_attr)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "backup set attr is null", K(ret), K(backup_set_id));
        } else if (OB_ISNULL(backup_set_attr->backup_set_store_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "backup set store is null", K(ret), K(backup_set_id));
        } else if (OB_ISNULL(tablet_attr->macro_block_id_mappings_meta_index_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "tablet meta index is null", K(ret));
        } else if (OB_ISNULL(alc_ptr = local_allocator_.alloc(
                                 sizeof(backup::ObBackupMacroBlockIDMappingsMeta)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc backup::ObBackupMacroBlockIDMappingsMeta", K(ret));
        } else if (FALSE_IT(id_mappings_meta_.at(i)
                            = new (alc_ptr) backup::ObBackupMacroBlockIDMappingsMeta())) {
        } else if (OB_FAIL(ObAdminBackupValidationUtil::read_macro_block_id_mappings_meta(
                       backup_set_attr->backup_set_store_->get_backup_set_dest(),
                       *tablet_attr->macro_block_id_mappings_meta_index_, tablet_attr->data_type_,
                       *id_mappings_meta_.at(i), ctx_))) {
          STORAGE_LOG(WARN, "failed to read id mappings meta", K(ret), K(backup_set_id),
                      K(tablet_attr->tablet_meta_index_), K(tablet_attr->data_type_));
        } else if ((OB_ISNULL(tablet_attr->sstable_meta_index_)
                    && id_mappings_meta_.at(i)->sstable_count_ > 0)
                   || (OB_NOT_NULL(tablet_attr->sstable_meta_index_)
                       && 0 == id_mappings_meta_.at(i)->sstable_count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "empty tablet does not match non-empty tablet", K(ret));
        } else if (OB_NOT_NULL(tablet_attr->sstable_meta_index_)
                   && OB_FAIL(ObAdminBackupValidationUtil::read_sstable_metas(
                       backup_set_attr->backup_set_store_->get_backup_set_dest(),
                       *tablet_attr->sstable_meta_index_, tablet_attr->data_type_,
                       inner_sstable_meta, ctx_))) {
          STORAGE_LOG(WARN, "failed to read sstable meta", K(ret), K(backup_set_id),
                      K(tablet_attr->tablet_meta_index_), K(tablet_attr->data_type_));
        } else if (OB_FAIL(sstable_metas_.push_back(inner_sstable_meta))) {
          STORAGE_LOG(WARN, "failed to push back sstable meta", K(ret), K(backup_set_id),
                      K(tablet_attr->tablet_meta_index_), K(tablet_attr->data_type_));
        } else {
          STORAGE_LOG(INFO, "succeed to read sstable meta info", K(ret), K(backup_set_id));
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("SSTable meta and Macro Block ID mappings meta info",
                   "meta info is not complete");
  }
  return ret;
}
int ObAdminTabletMetaValidationTask::check_sstable_meta_info_and_prepare_macro_block_()
{
  int ret = OB_SUCCESS;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObArray<ObAdminTabletAttr *> current_group;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (OB_FAIL(tablet_validation_dag->processing_tablet_group_.at(task_id_, current_group))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet tablet group", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < current_group.count(); i++) {
      ObAdminTabletAttr *tablet_attr = nullptr;
      ObAdminBackupSetAttr *backup_set_attr = nullptr;
      if (OB_ISNULL(tablet_attr = current_group.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "tablet attr is null", K(ret));
      } else if (OB_FAIL(ctx_->get_backup_set_attr(tablet_attr->tablet_meta_index_->backup_set_id_,
                                                   backup_set_attr))) {
        STORAGE_LOG(WARN, "failed to get backup set attr", K(ret),
                    K(tablet_attr->tablet_meta_index_->backup_set_id_));
      } else if (OB_ISNULL(backup_set_attr)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "backup set attr is null", K(ret),
                    K(tablet_attr->tablet_meta_index_->backup_set_id_));
      } else if (id_mappings_meta_.at(i)->sstable_count_ != sstable_metas_.at(i).count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "sstable meta count not match", K(ret),
                    K(id_mappings_meta_.at(i)->sstable_count_), K(sstable_metas_.count()));
      } else if (0 == id_mappings_meta_.at(i)->sstable_count_) {
        STORAGE_LOG(DEBUG, "empty tablet", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < id_mappings_meta_.at(i)->sstable_count_; ++j) {
          backup::ObBackupMacroBlockIDMapping &id_mapping
              = id_mappings_meta_.at(i)->id_map_list_[j];
          backup::ObBackupSSTableMeta &sstable_meta = sstable_metas_.at(i).at(j);
          if (id_mapping.table_key_ != sstable_meta.sstable_meta_.table_key_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "sstable meta table key not match", K(ret), K(id_mapping.table_key_),
                        K(sstable_meta.sstable_meta_.table_key_));
          } else if (id_mapping.id_pair_list_.count() != sstable_meta.logic_id_list_.count()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "sstable meta logic id count not match", K(ret),
                        K(id_mapping.id_pair_list_.count()),
                        K(sstable_meta.logic_id_list_.count()));
          } else if (sstable_meta.tablet_id_ != tablet_meta_.at(i)->tablet_id_) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "sstable meta tablet id not match", K(ret),
                        K(sstable_meta.tablet_id_), K(tablet_meta_.at(i)->tablet_id_));
          } else {
            std::sort(id_mapping.id_pair_list_.begin(), id_mapping.id_pair_list_.end());
            std::sort(sstable_meta.logic_id_list_.begin(), sstable_meta.logic_id_list_.end());
            for (int64_t k = 0; OB_SUCC(ret) && k < id_mapping.id_pair_list_.count(); ++k) {
              const backup::ObBackupMacroBlockIDPair &macro_block_id_pair
                  = id_mapping.id_pair_list_.at(k);
              const blocksstable::ObLogicMacroBlockId &logic_macro_block_id
                  = sstable_meta.logic_id_list_.at(k);
              if (!logic_macro_block_id.is_valid()) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "logic macro block id is not valid", K(ret));
              } else if (!macro_block_id_pair.logic_id_.is_valid()) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "macro block id pair is not valid", K(ret));
              } else if (macro_block_id_pair.logic_id_ != logic_macro_block_id) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "sstable meta logic id not match", K(ret),
                            K(macro_block_id_pair.logic_id_), K(logic_macro_block_id));
              } else if (!macro_block_id_pair.physical_id_.is_valid()) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "macro block id pair is not valid", K(ret));
              } else if (OB_FAIL(tablet_validation_dag->add_macro_block(macro_block_id_pair,
                                                                        tablet_attr->data_type_))) {
                STORAGE_LOG(WARN, "failed to push back macro block id pair", K(ret),
                            K(macro_block_id_pair));
              }
            }
            if (OB_SUCC(ret)
                && (OB_FAIL(tablet_validation_dag->stat_.add_scheduled_macro_block_count_(
                        id_mapping.id_pair_list_.count()))
                    || OB_FAIL(backup_set_attr->stat_.add_scheduled_macro_block_count_(
                        id_mapping.id_pair_list_.count()))
                    || OB_FAIL(ctx_->global_stat_.add_scheduled_macro_block_count_(
                        id_mapping.id_pair_list_.count())))) {
              STORAGE_LOG(WARN, "failed to add scheduled macro block count", K(ret),
                          K(id_mapping.id_pair_list_.count()));
            }
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("Macro Block meta info", "Macro Block meta info is not complete");
  }
  return ret;
}
////////////////ObAdminTabletMetaValidationTask End////////////////

////////////////ObAdminMacroBlockDataValidationTask Start////////////////
ObAdminMacroBlockDataValidationTask::~ObAdminMacroBlockDataValidationTask() {}
int ObAdminMacroBlockDataValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminMacroBlockDataValidationTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  int64_t next_task_id = task_id_ + 1;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObAdminMacroBlockDataValidationTask *sibling_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (next_task_id >= tablet_validation_dag->processing_macro_block_array_.count()) {
    ret = OB_ITER_END;
    next_task = nullptr;
    STORAGE_LOG(INFO, "no more sibling task", K(ret), K(next_task_id));
  } else if (OB_FAIL(tablet_validation_dag->alloc_task(sibling_task))) {
    STORAGE_LOG(WARN, "failed to alloc sibling task", K(ret));
  } else if (OB_FAIL(sibling_task->init(next_task_id, ctx_))) {
    STORAGE_LOG(WARN, "failed to init sibling task", K(ret));
  } else {
    next_task = sibling_task;
    sibling_task = nullptr;
  }
  return ret;
}
int ObAdminMacroBlockDataValidationTask::process()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (OB_FAIL(check_macro_block_data_())) {
    STORAGE_LOG(WARN, "failed to check macro block data", K(ret));
  } else {
    STORAGE_LOG(DEBUG, "succeed to process ObAdminMacroBlockDataValidationTask", K(ret),
                K(task_id_));
  }
  return ret;
}
int ObAdminMacroBlockDataValidationTask::check_macro_block_data_()
{
  int ret = OB_SUCCESS;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  blocksstable::ObBufferReader data_buffer;
  char *buf = nullptr;
  common::ObArenaAllocator tmp_allocator("ObAdmBakVal");
  blocksstable::ObSSTableMacroBlockChecker checker;
  backup::ObBackupMacroBlockIndex macro_index;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (OB_ISNULL(buf
                       = static_cast<char *>(tmp_allocator.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc buf", K(ret));
  } else if (FALSE_IT(data_buffer.assign(buf, OB_DEFAULT_MACRO_BLOCK_SIZE))) {
    STORAGE_LOG(WARN, "failed to assign data buffer", K(ret));
  } else if (task_id_ >= tablet_validation_dag->processing_macro_block_array_.count()) {
    STORAGE_LOG(INFO, "tablet group is all empty tablet, no sstable", K(ret), K(task_id_));
  } else {
    ObArray<std::pair<backup::ObBackupMacroBlockIDPair, share::ObBackupDataType>>
        &macro_block_id_pairs = tablet_validation_dag->processing_macro_block_array_.at(task_id_);
    int64_t start_time = common::ObTimeUtility::current_time();
    FOREACH_X(macro_block_id_pair_iter, macro_block_id_pairs, OB_SUCC(ret))
    {
      macro_index.reset();
      data_buffer.set_pos(0);
      backup_set_attr = nullptr;
      if (OB_ISNULL(macro_block_id_pair_iter)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "macro block id pair is null", K(ret));
      } else if (OB_FAIL(macro_block_id_pair_iter->first.physical_id_.get_backup_macro_block_index(
                     macro_block_id_pair_iter->first.logic_id_, macro_index))) {
        STORAGE_LOG(WARN, "failed to get macro index", K(ret));
      } else if (OB_FAIL(ctx_->get_backup_set_attr(macro_index.backup_set_id_, backup_set_attr))) {
        STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(macro_index.backup_set_id_));
        int tmp_ret = OB_SUCCESS;
        char msg[1024];
        if (OB_TMP_FAIL(databuff_printf(msg, sizeof(msg) - 1, "Missing Backup Set %ld",
                                        macro_index.backup_set_id_))) {
          STORAGE_LOG(WARN, "failed to print backup set id", K(tmp_ret));
        } else if (OB_TMP_FAIL(ctx_->go_abort(msg, "Dependent Backup Set is Not Given"))) {
          STORAGE_LOG(WARN, "failed to go abort", K(tmp_ret));
        }
      } else if (OB_FAIL(ObAdminBackupValidationUtil::read_macro_block_data(
                     backup_set_attr->backup_set_store_->get_backup_set_dest(), macro_index,
                     macro_block_id_pair_iter->second, data_buffer, ctx_))) {
        STORAGE_LOG(WARN, "failed to read macro block data", K(ret), K(macro_index));
      } else if (!data_buffer.is_valid()) {
        ret = OB_CHECKSUM_ERROR;
        STORAGE_LOG(WARN, "data buffer is not valid", K(ret));
      } else if (OB_FAIL(checker.check(data_buffer.data(), data_buffer.length(),
                                       ctx_->mb_check_level_))) {
        STORAGE_LOG(WARN, "failed to check macro block data", K(ret));
      }
    }
    int64_t end_time = common::ObTimeUtility::current_time();
    if (OB_SUCC(ret)
        && (OB_FAIL(tablet_validation_dag->stat_.add_succeed_macro_block_count_(
                macro_block_id_pairs.count()))
            || OB_FAIL(
                backup_set_attr->stat_.add_succeed_macro_block_count_(macro_block_id_pairs.count()))
            || OB_FAIL(
                ctx_->global_stat_.add_succeed_macro_block_count_(macro_block_id_pairs.count())))) {
      STORAGE_LOG(WARN, "failed to add succeed macro block count", K(ret),
                  K(macro_block_id_pairs.count()));
    } else {
      STORAGE_LOG(INFO, "succeed to check macro block data", K(ret), K(task_id_),
                  K(macro_block_id_pairs.count()), K(end_time - start_time));
    }
  }
  if (OB_NOT_NULL(ctx_) && OB_FAIL(ret)) {
    ctx_->go_abort("Macro Block data", "Macro Block data maybe corrupted");
  }
  return ret;
}
////////////////ObAdminMacroBlockDataValidationTask End////////////////

////////////////ObAdminFinishTabletValidationTask Start////////////////
ObAdminFinishTabletValidationTask::~ObAdminFinishTabletValidationTask() {}
int ObAdminFinishTabletValidationTask::init(int64_t task_id, ObAdminBackupValidationCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "ctx is null", K(ret));
  } else {
    is_inited_ = true;
    task_id_ = task_id;
    ctx_ = ctx;
  }
  return ret;
}
int ObAdminFinishTabletValidationTask::process()
{
  int ret = OB_SUCCESS;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(ctx_) || ctx_->aborted_) {
    ret = OB_CANCELED;
    STORAGE_LOG(WARN, "aborted", K(ret));
  } else if (ctx_->processing_backup_set_id_array_.empty()) {
    STORAGE_LOG(INFO, "backup set id is all empty", K(ret), K(task_id_));
  } else if (OB_FAIL(check_and_update_stat_())) {
    STORAGE_LOG(WARN, "failed to check and update stat", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to process ObAdminFinishTabletValidationTask", K(ret), K(task_id_));
  }
  return ret;
}
int ObAdminFinishTabletValidationTask::check_and_update_stat_()
{
  int ret = OB_SUCCESS;
  int64_t backup_set_id = -1;
  ObAdminBackupTabletValidationDag *tablet_validation_dag = nullptr;
  ObAdminBackupSetAttr *backup_set_attr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(ctx_->processing_backup_set_id_array_.at(task_id_, backup_set_id))) {
    STORAGE_LOG(WARN, "failed to get backup set id", K(ret), K(task_id_));
  } else if (OB_ISNULL(tablet_validation_dag
                       = static_cast<ObAdminBackupTabletValidationDag *>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get tablet validation dag", K(ret));
  } else if (OB_FAIL(ctx_->get_backup_set_attr(backup_set_id, backup_set_attr))) {
    STORAGE_LOG(WARN, "failed to get backup set attr", K(ret), K(backup_set_id));
  } else if (OB_ISNULL(backup_set_attr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup set attr is null", K(ret), K(backup_set_id));
  } else if (tablet_validation_dag->stat_.succeed_macro_block_count_
             != tablet_validation_dag->stat_.scheduled_macro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "succeed macro block count not match scheduled macro block count", K(ret),
                K(tablet_validation_dag->stat_.succeed_macro_block_count_),
                K(tablet_validation_dag->stat_.scheduled_macro_block_count_));
    char buf[1024];
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(databuff_printf(buf, sizeof(buf) - 1, "Backupset %ld did not finish validation",
                                    backup_set_id))) {
      STORAGE_LOG(WARN, "failed to print backup set id", K(tmp_ret));
    } else if (OB_TMP_FAIL(ctx_->go_abort(buf, "Succeed macro block count not match "
                                               "scheduled macro block count"))) {
      STORAGE_LOG(WARN, "failed to go abort", K(tmp_ret));
    }
  } else if (OB_FAIL(backup_set_attr->stat_.add_succeed_tablet_count_(
                 tablet_validation_dag->scheduled_tablet_cnt_))
             || OB_FAIL(ctx_->global_stat_.add_succeed_tablet_count_(
                 tablet_validation_dag->scheduled_tablet_cnt_))) {
    STORAGE_LOG(WARN, "failed to add succeed tablet count", K(ret),
                K(tablet_validation_dag->scheduled_tablet_cnt_));
  }
  return ret;
}
////////////////ObAdminFinishTabletValidationTask End////////////////
}; // namespace tools
}; // namespace oceanbase