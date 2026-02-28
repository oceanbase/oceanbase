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

#include "storage/backup/ob_backup_validate_tasks.h"
#include "storage/backup/ob_backup_validate_base.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/backup/ob_backup_sstable_sec_meta_iterator.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_validate_struct.h"
#include "share/backup/ob_backup_struct.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/backup/ob_backup_meta_cache.h"
#include "storage/high_availability/ob_storage_restore_struct.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_archive_path.h"
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#include "logservice/restoreservice/ob_remote_log_source.h"
#include "logservice/restoreservice/ob_remote_log_source_allocator.h"
#include "logservice/restoreservice/ob_remote_log_iterator.h"
#include "logservice/archiveservice/large_buffer_pool.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/backup/ob_backup_validate_dag_scheduler.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "storage/backup/ob_backup_block_file_reader_writer.h"
#include "storage/restore/ob_tenant_restore_info_mgr.h"

using namespace oceanbase::backup;
namespace oceanbase
{
namespace storage
{
template<typename DagType>
static int alloc_and_add_dag_into_dag_net(
    DagType *&dag,
    ObTenantDagScheduler *dag_scheduler,
    ObIDag *parent_dag)
{
  int ret = OB_SUCCESS;
  ObIDagNet *dag_net = nullptr;
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret), KP(dag_scheduler));
  } else if (OB_ISNULL(dag_net = parent_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net is null", KR(ret), KP(parent_dag));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP_VALIDATE != dag_net->get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(dag_net), KPC(parent_dag));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc dag", KR(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag must not be nullptr", KR(ret));
  } else if (OB_FAIL(dag->init(dag_net))) {
    LOG_WARN("failed to init dag", KR(ret));
  } else if (OB_FAIL(dag->create_first_task())) {
    LOG_WARN("failed to create first task for dag", KR(ret));
  } else if (OB_FAIL(parent_dag->add_child(*dag))) {
    LOG_WARN("failed to add dag as child of parent dag", KR(ret), KP(dag), KP(parent_dag));
  }

  return ret;
}

template<typename ExecuteDagType, typename FinishDagType>
static int add_dags_to_scheduler(
    ObTenantDagScheduler *dag_scheduler,
    ExecuteDagType *&execute_dag,
    FinishDagType *&finish_dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag scheduler is null", KR(ret));
  } else if (OB_ISNULL(execute_dag) || OB_ISNULL(finish_dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dag pointers are null", KR(ret), KP(execute_dag), KP(finish_dag));
  } else {
    if (OB_FAIL(dag_scheduler->add_dag(execute_dag))) {
      LOG_WARN("failed to add execute dag to scheduler", KR(ret));
    } else if (OB_FAIL(dag_scheduler->add_dag(finish_dag))) {
      LOG_WARN("failed to add finish dag to scheduler", KR(ret));
      if (OB_TMP_FAIL(dag_scheduler->cancel_dag(execute_dag))) {
        LOG_WARN("failed to cancel execute dag", KR(tmp_ret), KR(ret));
      } else {
        execute_dag = nullptr;
      }
    } else {
      LOG_INFO("successfully add dags to scheduler", KPC(execute_dag), KPC(finish_dag));
    }
  }
  return ret;
}

/*
**************************ObBackupValidateBasicTask**************************
*/
ObBackupValidateBasicTask::ObBackupValidateBasicTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_BASIC),
    is_inited_(false),
    param_(),
    task_id_(0),
    ctx_(nullptr),
    report_ctx_(),
    storage_info_()
{
  MEMSET(error_msg_, 0, sizeof(error_msg_));
}

ObBackupValidateBasicTask::~ObBackupValidateBasicTask()
{
}

int ObBackupValidateBasicTask::init(
    const ObBackupValidateDagNetInitParam &param,
    const int64_t task_id,
    const backup::ObBackupReportCtx &report_ctx,
    const share::ObBackupStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateBasicTask already inited", KR(ret));
  } else if (!param.is_valid() || task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param or task id", KR(ret), K(param), K(task_id));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", KR(ret), K(param));
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    LOG_WARN("failed to assign storage info", KR(ret), K(storage_info));
  } else {
    share::ObIDag *dag = nullptr;
    share::ObIDagNet *dag_net = nullptr;
    if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (OB_ISNULL(ctx_ = static_cast<ObBackupValidateBasicDag*>(dag)->get_task_context())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is null", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    task_id_ = task_id;
    report_ctx_ = report_ctx;
    is_inited_ = true;
    LOG_INFO("ObBackupValidateBasicTask init success", K(param), K(task_id));
  }
  return ret;
}

int ObBackupValidateBasicTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateBasicTask not init", KR(ret));
  } else if (OB_SUCCESS != ctx_->get_result()) {
    //other task has failed, skip validate
  } else {
    ObBackupPathString dir_path;
    if (OB_FAIL(ctx_->get_dir_path(task_id_, dir_path))) {
      LOG_WARN("failed to get dir path", KR(ret), K_(task_id), K_(*ctx));
    } else if (OB_FAIL(do_basic_validate_(dir_path))) {
      LOG_WARN("failed to do basic validate", KR(ret));
    } else {
      LOG_INFO("ObBackupValidateBasicTask process success", K(task_id_), K(dir_path));
    }
    if (OB_SUCC(ret)) {
      bool update_checkpoint = false;
      int64_t validate_checkpoint = -1;
      int64_t read_bytes = 0;
      int64_t delta_read_bytes = 0;
      int64_t total_read_bytes = 0;
      common::ObISQLClient *sql_proxy = GCTX.sql_proxy_;
      if (OB_FAIL(ctx_->remove_running_task_id(task_id_, read_bytes, update_checkpoint,
                                                validate_checkpoint, delta_read_bytes, total_read_bytes))) {
        LOG_WARN("failed to remove running group", KR(ret), K_(task_id), K(read_bytes));
      } else if (update_checkpoint && OB_FAIL(ObBackupValidateLSTaskOperator::update_stats(*sql_proxy, param_.task_id_,
                                                                              param_.tenant_id_, param_.ls_id_,
                                                                              total_read_bytes, validate_checkpoint))) {
        LOG_WARN("failed to update stats", K(ret), K_(param), K(validate_checkpoint), K(total_read_bytes));
      }
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_) && OB_TMP_FAIL(ctx_->set_validate_result(ret, error_msg_, param_, GCTX.self_addr(),
                                                        dag_id, report_ctx_))) {
      LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K_(param), K_(error_msg), K(dag_id));
    }
  }
  return ret;
}

int ObBackupValidateBasicTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  int64_t next_task_id = -1;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null", KR(ret), KP_(ctx));
  } else if (OB_FAIL(ctx_->get_next_task_id(next_task_id))) {
    LOG_WARN("failed to get next task id", KR(ret));
  } else if (next_task_id == -1) {
    ret = OB_ITER_END;
    next_task = nullptr;
  } else {
    next_task = nullptr;
    ObIDag *dag = nullptr;
    ObBackupValidateBasicDag *basic_dag = nullptr;
    ObBackupValidateBasicTask *basic_task = nullptr;
    if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate basic", KR(ret), K(dag->get_type()));
    } else if (FALSE_IT(basic_dag = static_cast<ObBackupValidateBasicDag *>(dag))) {
    } else if (OB_FAIL(basic_dag->alloc_task(basic_task))) {
      LOG_WARN("failed to alloc basic task", KR(ret));
    } else if (OB_ISNULL(basic_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to alloc basic task", KR(ret));
    } else if (OB_FAIL(basic_task->init(param_, next_task_id, report_ctx_, storage_info_))) {
      LOG_WARN("failed to init basic task", KR(ret), K(next_task_id));
    } else {
      next_task = basic_task;
      basic_task = nullptr;
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    char error_msg[OB_MAX_VALIDATE_LOG_INFO_LENGTH] = {0};
    if (OB_TMP_FAIL(databuff_printf(error_msg, OB_MAX_VALIDATE_LOG_INFO_LENGTH,
                                        "failed to generate next task, task id: %ld", next_task_id))) {
      LOG_WARN("failed to print error message", KR(ret), KR(tmp_ret));
    }
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, error_msg, param_, GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(error_msg), K(dag_id), K_(param));
      }
    }
    LOG_ERROR("failed to generate next task", KR(ret), KR(tmp_ret), K_(param), K(next_task_id));
  }
  return ret;
}

int ObBackupValidateBasicTask::do_basic_validate_(const ObBackupPathString &dir_path)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null", KR(ret), KP_(ctx));
  } else {
    ObSArray<ObBackupFileInfo> file_list;
    common::ObBackupIoAdapter io_adapter;
    int tmp_ret = OB_SUCCESS;
    const ObBackupBlockFileDataType data_type = ObBackupBlockFileDataType::FILE_PATH_INFO;
    ObBackupBlockFileItemReader reader;
    ObStorageIdMod storage_id_mod;
    storage_id_mod.storage_id_ = param_.dest_id_;
    storage_id_mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    ObBackupPath backup_path;
    ObBackupFileSuffix suffix = param_.task_type_.is_archivelog() ? ObBackupFileSuffix::ARCHIVE
                                                                  : ObBackupFileSuffix::BACKUP;
    ObBackupFileInfo file_info;
    int64_t actual_size = 0;
    share::ObBackupPathString absolute_path;
    if (OB_FAIL(backup_path.init(dir_path.ptr()))) {
      LOG_WARN("failed to init backup path", KR(ret), K(dir_path));
    } else if (OB_FAIL(reader.init(&storage_info_, data_type, backup_path, suffix, storage_id_mod))) {
      LOG_WARN("failed to init reader", KR(ret), K(backup_path), K_(storage_info), K(storage_id_mod), K(suffix));
    }
    while (OB_SUCC(ret)) {
      file_info.reset();
      actual_size = 0;
      if (OB_FAIL(reader.get_next_item(file_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        }
      } else if (!file_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file info is not valid", KR(ret), K(file_info));
      } else if (file_info.is_file()) {
        absolute_path.reset();
        if (OB_FAIL(ObBackupFileListReaderUtil::compose_absolute_path(backup_path, file_info.path_, absolute_path))) {
          LOG_WARN("failed to compose absolute path", KR(ret), K(backup_path), K(file_info.path_));
        } else if (OB_FAIL(io_adapter.adaptively_get_file_length(absolute_path.str(), &storage_info_, actual_size))) {
          LOG_WARN("failed to get file length", KR(ret), K(file_info));
          if (OB_OBJECT_NOT_EXIST == ret) {
            if (OB_TMP_FAIL(databuff_printf(error_msg_, OB_MAX_VALIDATE_LOG_INFO_LENGTH,
                                                "error when get file %s", file_info.path_.ptr()))) {
              LOG_WARN("failed to format error message", KR(tmp_ret), KR(ret), K(file_info));
            }
          }
        } else if (actual_size != file_info.file_size_) {
          ret = OB_FILE_LENGTH_INVALID;
          if (OB_TMP_FAIL(databuff_printf(error_msg_, OB_MAX_VALIDATE_LOG_INFO_LENGTH,
            "file: %s, file size not match", file_info.path_.ptr()))) {
            LOG_WARN("failed to format error message", KR(tmp_ret), KR(ret), K(file_info));
          }
          LOG_WARN("file size mismatch", KR(ret), K(file_info));
        }
      }
    }
  }
  return ret;
}

/*
-------------------------ObBackupValidatePrepareTask-------------------------------
*/
ObBackupValidatePrepareTask::ObBackupValidatePrepareTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_PREPARE),
    is_inited_(false),
    param_(),
    ctx_(nullptr),
    report_ctx_(),
    storage_info_()
{
}

ObBackupValidatePrepareTask::~ObBackupValidatePrepareTask()
{
}

int ObBackupValidatePrepareTask::init(
    const ObBackupValidateDagNetInitParam &param,
    const backup::ObBackupReportCtx &report_ctx,
    const share::ObBackupStorageInfo &storage_info,
    ObBackupValidateTaskContext *ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidatePrepareTask already inited", KR(ret));
  } else if (!param.is_valid() || !report_ctx.is_valid() || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument to init prepare task", KR(ret), K(param), K(report_ctx), KP(ctx));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", KR(ret), K(param));
  } else if (FALSE_IT(report_ctx_ = report_ctx)) {
  } else if (FALSE_IT(ctx_ = ctx)) {
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    LOG_WARN("failed to assign storage info", KR(ret), K(storage_info));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObBackupValidatePrepareTask::process()
{
  int ret = OB_SUCCESS;
  bool has_task = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidatePrepareTask not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null", KR(ret), KP_(ctx));
  } else if (param_.validate_level_.is_basic()) {
    if (OB_FAIL(prepare_basic_validate_())) {
      LOG_WARN("failed to prepare basic validate", KR(ret));
    } else if (ctx_->has_validate_task()) {
      if (OB_FAIL(generate_basic_validate_dag_())) {
        LOG_WARN("failed to generate basic validate dag", KR(ret), KPC_(ctx));
      } else {
        has_task = true;
      }
    }
  } else if (param_.validate_level_.is_physical() && param_.task_type_.is_backupset()) {
    if (OB_FAIL(prepare_backupset_physical_validate_())) {
      LOG_WARN("failed to prepare physical validate", KR(ret));
    } else if (ctx_->has_validate_task()) {
      if (OB_FAIL(generate_backupset_physical_validate_dag_())) {
        LOG_WARN("failed to generate backupset physical validate dag", KR(ret), KPC_(ctx));
      } else {
        has_task = true;
      }
    }
  } else if (param_.validate_level_.is_physical() && param_.task_type_.is_archivelog()) {
    if (OB_FAIL(prepare_archive_piece_physical_validate_())) {
      LOG_WARN("failed to prepare archive piece physical validate", KR(ret));
    } else if (ctx_->has_validate_task()) {
      if (OB_FAIL(generate_archive_piece_physical_validate_dag_())) {
        LOG_WARN("failed to generate archive piece physical validate dag", KR(ret), KPC_(ctx));
      } else {
        has_task = true;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported validate level", KR(ret), K_(param));
  }
  if (OB_FAIL(ret)) {
  } else if (has_task) {
    if(OB_FAIL(ObBackupValidateLSTaskOperator::init_stats(*report_ctx_.sql_proxy_, param_.task_id_,
                                                            param_.tenant_id_, param_.ls_id_, ctx_->get_task_count()))) {
      LOG_WARN("failed to init stats", KR(ret), K_(param), KPC_(ctx));
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to prepare validate task", param_,
                                                    GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
  } else if (!has_task) {
    if (OB_FAIL(report_validate_succ_())) {
      LOG_WARN("failed to report validate succ", KR(ret));
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::report_validate_succ_()
{
  int ret = OB_SUCCESS;
  obrpc::ObBackupTaskRes res;
  res.job_id_ = param_.job_id_;
  res.task_id_ = param_.task_id_;
  res.tenant_id_ = param_.tenant_id_;
  res.ls_id_ = param_.ls_id_;
  res.result_ = OB_SUCCESS;
  res.src_server_ = GCTX.self_addr();
  res.trace_id_ = param_.trace_id_;
  ObIDag *dag = nullptr;
  if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (FALSE_IT(res.dag_id_ = dag->get_dag_id())) {
  } else if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res));
  } else if (OB_FAIL(ObBackupValidateObUtils::report_validate_over(res, report_ctx_))) {
    LOG_WARN("failed to report validate over", KR(ret), K(res), K(report_ctx_));
  }
  return ret;
}

int ObBackupValidatePrepareTask::generate_basic_validate_dag_()
{
  int ret = OB_SUCCESS;
  ObBackupValidateBasicDag *basic_dag = nullptr;
  ObBackupValidateFinishDag *finish_dag = nullptr;
  ObTenantDagScheduler *dag_scheduler = nullptr;
  ObIDag *dag = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret), KP(dag_scheduler));
  } else if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (OB_FAIL(alloc_and_add_dag_into_dag_net(basic_dag, dag_scheduler, dag))) {
    LOG_WARN("failed to alloc and add basic dag into dag net", KR(ret), KPC(dag));
  } else if (OB_FAIL(alloc_and_add_dag_into_dag_net(finish_dag, dag_scheduler, basic_dag))) {
    LOG_WARN("failed to alloc and add finish dag into dag net", KR(ret), KPC(basic_dag));
  } else if (OB_FAIL(add_dags_to_scheduler(dag_scheduler, basic_dag, finish_dag))) {
    LOG_WARN("failed to add dags to scheduler", KR(ret), KP(dag_scheduler), KP(basic_dag), KP(finish_dag));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler)) {
    if (OB_NOT_NULL(finish_dag)) {
      dag_scheduler->free_dag(*finish_dag);
      finish_dag = nullptr;
    }
    if (OB_NOT_NULL(basic_dag)) {
      dag_scheduler->free_dag(*basic_dag);
      basic_dag = nullptr;
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::generate_backupset_physical_validate_dag_()
{
  int ret = OB_SUCCESS;
  ObBackupValidateBackupSetPhysicalDag *backupset_dag = nullptr;
  ObBackupValidateFinishDag *finish_dag = nullptr;
  ObTenantDagScheduler *dag_scheduler = nullptr;
  ObIDag *dag = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret), KP(dag_scheduler));
  } else if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (OB_FAIL(alloc_and_add_dag_into_dag_net(backupset_dag, dag_scheduler, dag))) {
    LOG_WARN("failed to alloc and add backupset dag into dag net", KR(ret), KPC(dag));
  } else if (OB_FAIL(alloc_and_add_dag_into_dag_net(finish_dag, dag_scheduler, backupset_dag))) {
    LOG_WARN("failed to alloc and add finish dag into dag net", KR(ret), KPC(backupset_dag));
  } else if (OB_FAIL(add_dags_to_scheduler(dag_scheduler, backupset_dag, finish_dag))) {
    LOG_WARN("failed to add dags to scheduler", KR(ret), KP(dag_scheduler), KP(backupset_dag), KP(finish_dag));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler)) {
    if (OB_NOT_NULL(finish_dag)) {
      dag_scheduler->free_dag(*finish_dag);
      finish_dag = nullptr;
    }
    if (OB_NOT_NULL(backupset_dag)) {
      dag_scheduler->free_dag(*backupset_dag);
      backupset_dag = nullptr;
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::generate_archive_piece_physical_validate_dag_()
{
  int ret = OB_SUCCESS;
  ObBackupValidateArchivePiecePhysicalDag *archivepiece_dag = nullptr;
  ObBackupValidateFinishDag *finish_dag = nullptr;
  ObTenantDagScheduler *dag_scheduler = nullptr;
  ObIDag *dag = nullptr;
  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret), KP(dag_scheduler));
  } else if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (OB_FAIL(alloc_and_add_dag_into_dag_net(archivepiece_dag, dag_scheduler, dag))) {
    LOG_WARN("failed to alloc and add archive piece dag into dag net", KR(ret), KPC(dag));
  } else if (OB_FAIL(alloc_and_add_dag_into_dag_net(finish_dag, dag_scheduler, archivepiece_dag))) {
    LOG_WARN("failed to alloc and add finish dag into dag net", KR(ret), KPC(archivepiece_dag));
  } else if (OB_FAIL(add_dags_to_scheduler(dag_scheduler, archivepiece_dag, finish_dag))) {
    LOG_WARN("failed to add dags to scheduler", KR(ret), KP(dag_scheduler), KP(archivepiece_dag), KP(finish_dag));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag_scheduler)) {
    if (OB_NOT_NULL(finish_dag)) {
      dag_scheduler->free_dag(*finish_dag);
      finish_dag = nullptr;
    }
    if (OB_NOT_NULL(archivepiece_dag)) {
      dag_scheduler->free_dag(*archivepiece_dag);
      archivepiece_dag = nullptr;
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::prepare_archive_piece_physical_validate_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null", KR(ret), KP_(ctx));
  } else if (OB_ISNULL(report_ctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(report_ctx_.sql_proxy_));
  } else {
    share::ObPieceKey piece_key;
    share::ObBackupDest piece_dest;
    share::ObSinglePieceDesc single_piece_desc;
    share::ObArchiveStore archive_store;
    bool is_empty_piece = false;
    share::ObSingleLSInfoDesc ls_info;
    palf::LSN min_lsn;
    palf::LSN max_lsn;
    if (OB_FAIL(piece_dest.set(param_.validate_path_.ptr(), &storage_info_))) {
      LOG_WARN("failed to set archive dest", KR(ret), K(param_));
    } else if (OB_FAIL(archive_store.init(piece_dest))) {
        LOG_WARN("failed to init archive store", KR(ret), K(piece_dest));
    } else if (OB_FAIL(archive_store.get_single_piece_info(is_empty_piece, single_piece_desc))) {
      LOG_WARN("failed to get single piece info", KR(ret));
    } else if (is_empty_piece) {
      ret = OB_OBJECT_NOT_EXIST;
      LOG_WARN("archive piece not exist", KR(ret), K(param_));
    } else if (OB_FAIL(collect_and_check_piece_ls_info_(archive_store, piece_dest, single_piece_desc, ls_info))) {
      LOG_WARN("failed to collect and check piece ls info", KR(ret), K(param_));
    } else if (FALSE_IT(min_lsn = palf::LSN(ls_info.min_lsn_))) {
    } else if (FALSE_IT(max_lsn = palf::LSN(ls_info.max_lsn_))) {
    } else {
      if (OB_FAIL(ctx_->set_ls_info(ls_info))) {
        LOG_WARN("failed to set ls info", KR(ret), K(ls_info));
      } else if (min_lsn < max_lsn) {
        ObBackupArchivePieceLSNRange range;
        int64_t group_id = 0;
        palf::LSN end_lsn = min_lsn;
        while (OB_SUCC(ret) && end_lsn < max_lsn) {
          range.reset();
          range.start_lsn_ = end_lsn;
          end_lsn = end_lsn + palf::PALF_BLOCK_SIZE;
          if (end_lsn > max_lsn) {
            end_lsn = max_lsn;
          }
          range.group_id_ = group_id;
          range.end_lsn_ = end_lsn;
          if (OB_FAIL(ctx_->add_archive_piece_lsn_range(range))) {
            LOG_WARN("failed to push back lsn range", KR(ret), K(range));
          } else {
            group_id++;
          }
        }
        if (FAILEDx(set_task_id_())) {
          LOG_WARN("failed to set task id", KR(ret), K_(param), KPC_(ctx));
        }
      } else {
        LOG_INFO("empty lsn range for ls, skip", K(param_));
      }
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::collect_and_check_piece_ls_info_(
    const share::ObArchiveStore &archive_store,
    const share::ObBackupDest &piece_root_dest,
    const share::ObSinglePieceDesc &single_piece_desc,
    share::ObSingleLSInfoDesc &ls_info)
{
  int ret = OB_SUCCESS;

  if (!single_piece_desc.is_valid() || ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(single_piece_desc), K(ls_info));
  } else {
    if (single_piece_desc.piece_.is_active()) {
      if (OB_FAIL(get_active_piece_ls_info_(piece_root_dest, single_piece_desc, ls_info))) {
        LOG_WARN("failed to get active piece ls info", KR(ret), K(param_.ls_id_), K(piece_root_dest));
      }
    } else {
      if (OB_FAIL(archive_store.read_single_ls_info(param_.ls_id_, ls_info))) {
        LOG_WARN("failed to read single ls info", KR(ret), K(*this));
      } else if (!ls_info.is_valid() || ls_info.ls_id_ != param_.ls_id_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("single ls info not match piece key/ls", KR(ret), K(ls_info), K(param_.ls_id_));
      }
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::get_active_piece_ls_info_(
    const share::ObBackupDest &piece_root_dest,
    const share::ObSinglePieceDesc &single_piece_desc,
    share::ObSingleLSInfoDesc &ls_info)
{
  int ret = OB_SUCCESS;
  if (!single_piece_desc.is_valid() || ls_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(single_piece_desc), K(ls_info));
  } else {
    palf::LSN min_lsn;
    palf::LSN max_lsn;
    char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
    if (OB_ISNULL(piece_root_dest.get_storage_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("piece_root_dest storage info is null", KR(ret), K(piece_root_dest));
    } else if (OB_FAIL(piece_root_dest.get_storage_info()->get_storage_info_str(storage_info_str,
                                                                                sizeof(storage_info_str)))) {
      LOG_WARN("failed to get storage info str", KR(ret));
    } else {
      logservice::DirArray dirs;
      std::pair<share::ObBackupPathString, share::ObBackupPathString> dir_info;
      if (OB_FAIL(dir_info.first.assign(piece_root_dest.get_root_path()))) {
        LOG_WARN("failed to assign root path", KR(ret), K(piece_root_dest));
      } else if (OB_FAIL(dir_info.second.assign(storage_info_str))) {
        LOG_WARN("failed to assign storage info str", KR(ret));
      } else if (OB_FAIL(dirs.push_back(dir_info))) {
        LOG_WARN("failed to push back dir pair", KR(ret));
      } else {
        logservice::ObLogRawPathPieceContext raw_ctx;
        if (OB_FAIL(raw_ctx.init(param_.ls_id_, dirs))) {
          LOG_WARN("raw path ctx init failed", KR(ret), K(param_.ls_id_));
        } else if (dirs.count() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("empty dirs for raw path", KR(ret));
        } else if (OB_FAIL(raw_ctx.get_active_piece_ls_lsn_range(min_lsn, max_lsn))) {
          LOG_WARN("get active piece ls lsn range failed", KR(ret), K(param_.ls_id_), K(piece_root_dest));
        } else {
          ls_info.min_lsn_ = min_lsn.val_;
          ls_info.max_lsn_ = max_lsn.val_;
          ls_info.dest_id_ = param_.dest_id_;
          ls_info.round_id_ = single_piece_desc.piece_.key_.round_id_;
          ls_info.piece_id_ = single_piece_desc.piece_.key_.piece_id_;
          ls_info.ls_id_ = param_.ls_id_;
          ls_info.start_scn_ = single_piece_desc.piece_.start_scn_;
          ls_info.checkpoint_scn_ = single_piece_desc.piece_.checkpoint_scn_;
          ls_info.deleted_ = false;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !ls_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls info is not valid", KR(ret), K(ls_info));
  }
  return ret;
}

int ObBackupValidatePrepareTask::set_task_id_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), KP_(ctx));
  } else {
    common::ObMySQLProxy *sql_proxy = report_ctx_.sql_proxy_;
    int64_t validated_object_count = 0;
    int64_t validated_bytes = 0;
    if (OB_FAIL(ObBackupValidateLSTaskOperator::get_validated_stats(*sql_proxy, param_.task_id_,
          param_.tenant_id_, param_.ls_id_, validated_object_count, validated_bytes))) {
      LOG_WARN("failed to get validated stats", KR(ret), K_(param), KPC_(ctx));
    } else if (OB_FAIL(ctx_->set_next_task_id_and_read_bytes(validated_object_count, validated_bytes))) {
      LOG_WARN("failed to set next task id", KR(ret), K_(param), KPC_(ctx));
    }
  }
  return ret;
}
int ObBackupValidatePrepareTask::prepare_basic_validate_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || !param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null or param is invalid", KR(ret), KP_(ctx), K_(param));
  } else {
    common::ObArray<ObBackupPath> path_list;
    char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
    ObBackupPath info_path;
    ObBackupDest set_dest;
    ObBackupPath ls_dir_path;
    const share::ObLSID ls_id = param_.ls_id_;
    ObArchiveStore store;
    if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
      LOG_WARN("failed to get root path", KR(ret), K_(param));
    } else if (OB_FAIL(set_dest.set(raw_path, &storage_info_))) {
      LOG_WARN("failed to set storage path", KR(ret), K(param_), K_(storage_info));
    } else if (OB_FAIL(ObBackupPathUtil::get_ls_backup_dir_path(set_dest, ls_id, ls_dir_path))) {
      LOG_WARN("failed to get ls backup dir path", KR(ret), K(set_dest), K_(param));
    // every log stream need to check self log stream dir
    } else if (OB_FAIL(path_list.push_back(ls_dir_path))) {
      LOG_WARN("failed to push back ls backup dir path to path list", KR(ret), K(ls_dir_path));
    // For sys log stream, also need to check meta files
    } else if (param_.ls_id_.is_sys_ls()) {
      if (param_.task_type_.is_backupset()) {
        if (OB_FAIL(ObBackupPathUtil::get_ls_info_dir_path(set_dest, info_path))) {
          LOG_WARN("failed to get ls info dir path", KR(ret), K(set_dest));
        } else if (OB_FAIL(path_list.push_back(info_path))) {
          LOG_WARN("failed to push back ls info dir path to queue", KR(ret), K(info_path));
        }
      } else if (param_.task_type_.is_archivelog()) {
        ObArchiveStore store;
        bool is_exist = false;
        ObBackupPath checkpoint_path;
        const ObBackupFileSuffix suffix(ObBackupFileSuffix::ARCHIVE);
        if (OB_FAIL(store.init(set_dest))) {
          LOG_WARN("failed to init archive store", KR(ret), K(set_dest));
        } else if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_dir_path(set_dest, checkpoint_path))) {
          LOG_WARN("failed to get piece checkpoint dir path", KR(ret), K(set_dest));
        } else if (OB_FAIL(store.is_file_list_file_exist(checkpoint_path, suffix, is_exist))) {
          LOG_WARN("failed to check file list file exist", KR(ret), K(checkpoint_path));
        } else if (is_exist && OB_FAIL(path_list.push_back(checkpoint_path))) {
          LOG_WARN("failed to push back piece checkpoint dir path to queue", KR(ret), K(checkpoint_path));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid task type", KR(ret), K_(param));
        }
      }
    }
    if (FAILEDx(get_and_add_dir_list_(path_list))) {
      LOG_WARN("failed to get and add dir list", KR(ret), K(path_list));
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::get_and_add_dir_list_(const common::ObArray<ObBackupPath> &path_list)
{
  int ret = OB_SUCCESS;
  if (path_list.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("path list is empty", KR(ret), K(path_list));
  } else {
    common::ObSArray<share::ObBackupPathString> dir_queue;
    ObBackupFileSuffix suffix;
    common::ObStorageIdMod storage_id_mod;
    storage_id_mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    storage_id_mod.storage_id_ = param_.dest_id_;
    char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
    if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
      LOG_WARN("failed to get root path", KR(ret), K(param_));
    } else if (ObBackupValidateType::ValidateType::BACKUPSET == param_.task_type_.type_) {
      suffix = ObBackupFileSuffix::BACKUP;
    } else {
      suffix = ObBackupFileSuffix::ARCHIVE;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < path_list.count(); ++i) {
      const ObBackupPath &path = path_list.at(i);
      if (OB_FAIL(ObBackupFileListReaderUtil::get_all_dir_list(&storage_info_, path, suffix,
                                                                        storage_id_mod, dir_queue))) {
        LOG_WARN("failed to get all dir list", KR(ret), K(path), K(storage_id_mod), K(suffix), K_(storage_info));
      }
    }
    if (OB_SUCC(ret) && param_.ls_id_.is_sys_ls()) {
      if (OB_FAIL(dir_queue.push_back(raw_path))) {
        LOG_WARN("failed to push back base path to queue", KR(ret), K(raw_path));
      }
    }
    if (FAILEDx(ctx_->set_dir_queue(dir_queue))) {
      LOG_WARN("failed to set dir queue", KR(ret), K(dir_queue));
    } else if (OB_FAIL(set_task_id_())) {
      LOG_WARN("failed to set task id", KR(ret), K_(param), KPC_(ctx));
    } else {
      LOG_INFO("ObBackupValidatePrepareTask process success", K_(param));
    }
  }
  return ret;
}

int ObBackupValidatePrepareTask::prepare_backupset_physical_validate_()
{
  int ret = OB_SUCCESS;
  common::ObSArray<common::ObTabletID> all_tablets;

  if (OB_FAIL(get_tablet_list_(all_tablets))) {
    LOG_WARN("failed to get tablet list from tenant index", KR(ret));
  } else if (all_tablets.empty()) {
    //ls without tablet meta, skip
  } else if (FALSE_IT(ob_sort(all_tablets.begin(), all_tablets.end()))) {
  } else if (OB_FAIL(ctx_->set_tablet_array(all_tablets))) {
    LOG_WARN("failed to group tablets for validation", KR(ret));
  } else if (OB_FAIL(set_task_id_())) {
    LOG_WARN("failed to set task id", KR(ret), K_(param), KPC_(ctx));
  } else {
    LOG_INFO("ObBackupValidatePrepareTask physical validate process success", K_(param), K(all_tablets));
  }
  return ret;
}

int ObBackupValidatePrepareTask::get_tablet_list_(common::ObIArray<common::ObTabletID> &all_tablets)
{
  int ret = OB_SUCCESS;
  all_tablets.reset();
  share::ObBackupDest backup_set_dest;
  ObBackupDataStore store;
  int64_t first_turn_id = 1;
  ObBackupDataType data_type;
  data_type.reset();
  char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
  if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
    LOG_WARN("failed to get root path", KR(ret), K(param_));
  } else if (OB_FAIL(backup_set_dest.set(raw_path, &storage_info_))) {
    LOG_WARN("failed to set storage path", KR(ret), K(param_), K_(storage_info));
  } else if (OB_FAIL(store.init(backup_set_dest))) {
    LOG_WARN("failed to init backup data store", KR(ret), K(backup_set_dest));
  } else {
    data_type.set_user_data_backup();
    if (OB_FAIL(store.read_tablet_list(data_type, first_turn_id, param_.ls_id_, all_tablets))) {
      LOG_WARN("failed to read tablets", KR(ret), K_(param), K(backup_set_dest), K(data_type), K(first_turn_id));
    }
  }
  return ret;
}

/*
-------------------------ObBackupValidateFinishTask-------------------------------
*/
ObBackupValidateFinishTask::ObBackupValidateFinishTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_FINISH),
    is_inited_(false),
    param_(),
    ctx_(nullptr),
    report_ctx_()
{
}

ObBackupValidateFinishTask::~ObBackupValidateFinishTask()
{
}

int ObBackupValidateFinishTask::init(
    const ObBackupValidateDagNetInitParam &param,
    const backup::ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateFinishTask already inited", KR(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", KR(ret), K(param));
  } else {
    share::ObIDag *dag = nullptr;
    if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (OB_ISNULL(ctx_ = static_cast<ObBackupValidateFinishDag*>(dag)->get_task_context())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is null", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    report_ctx_ = report_ctx;
    is_inited_ = true;
    LOG_INFO("ObBackupValidateFinishTask init success", K(param));
  }
  return ret;
}

int ObBackupValidateFinishTask::process()
{
  int ret = OB_SUCCESS;
  int64_t result = OB_SUCCESS;
  ObIDag *dag = nullptr;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateFinishTask not init", KR(ret));
  } else if (OB_SUCCESS != ctx_->get_result()) {
  } else {
    if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (OB_FAIL(report_validate_succ_())) {
      LOG_WARN("failed to report validate over", KR(ret), K(dag->get_dag_id()));
    }
  }

  if (OB_FAIL(ret)) {
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "", param_, GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
  }

  return ret;
}

int ObBackupValidateFinishTask::report_validate_succ_()
{
  int ret = OB_SUCCESS;
  obrpc::ObBackupTaskRes res;
  res.job_id_ = param_.job_id_;
  res.task_id_ = param_.task_id_;
  res.tenant_id_ = param_.tenant_id_;
  res.ls_id_ = param_.ls_id_;
  res.result_ = OB_SUCCESS;
  res.src_server_ = GCTX.self_addr();
  res.trace_id_ = param_.trace_id_;
  ObIDag *dag = nullptr;
  if (OB_ISNULL(dag = this->get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else if (FALSE_IT(res.dag_id_ = dag->get_dag_id())) {
  } else if (!res.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(res));
  } else if (OB_FAIL(ObBackupValidateObUtils::report_validate_over(res, report_ctx_))) {
    LOG_WARN("failed to report validate over", KR(ret), K(res), K(report_ctx_));
  }
  return ret;
}

/*
**************************ObBackupValidateBackupSetPhysicalTask**************************
*/
ObBackupValidateBackupSetPhysicalTask::ObBackupValidateBackupSetPhysicalTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_BACKUPSET_PHYSICAL),
    is_inited_(false),
    param_(),
    report_ctx_(),
    storage_info_(),
    ctx_(nullptr),
    task_id_(-1),
    tablet_id_(),
    validated_meta_files_(0),
    validated_macro_blocks_(0),
    total_meta_files_(0),
    total_macro_blocks_(0),
    backup_set_info_(),
    backup_dest_(),
    read_bytes_(0),
    error_msg_(),
    meta_index_store_(nullptr)
{
}

ObBackupValidateBackupSetPhysicalTask::~ObBackupValidateBackupSetPhysicalTask()
{
}

int ObBackupValidateBackupSetPhysicalTask::init(
    ObBackupValidateBaseDag &base_dag,
    const int64_t task_id,
    ObExternBackupSetInfoDesc &backup_set_info,
    backup::ObBackupMetaIndexStoreWrapper *meta_index_store)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateBackupSetPhysicalTask init twice", KR(ret));
  } else if (!base_dag.is_inited() || OB_ISNULL(meta_index_store) || !backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(meta_index_store), K(backup_set_info));
  } else {
    const ObBackupValidateDagNetInitParam &param = base_dag.get_param();
    ObBackupValidateTaskContext *ctx = base_dag.get_task_context();
    const share::ObBackupStorageInfo &storage_info = base_dag.get_storage_info();
    const backup::ObBackupReportCtx &report_ctx = base_dag.get_report_ctx();
    if (OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is null", KR(ret));
    } else if (OB_FAIL(ctx->get_tablet_id(task_id, tablet_id_))) {
      LOG_WARN("failed to get tablet group", KR(ret), K(task_id));
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("failed to assign param", KR(ret), K(param));
    } else if (OB_FAIL(storage_info_.assign(storage_info))) {
      LOG_WARN("failed to assign storage info", KR(ret), K(storage_info));
    } else {
      ctx_ = ctx;
      report_ctx_ = report_ctx;
      backup_set_info_ = backup_set_info;
      task_id_ = task_id;
      meta_index_store_ = meta_index_store;
      char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
      ObBackupDataStore store;
      if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
        LOG_WARN("failed to get root path", KR(ret), K(param_.validate_path_));
      } else if (OB_FAIL(backup_dest_.set(raw_path, &storage_info_))) {
        LOG_WARN("failed to set backup dest", KR(ret), K(raw_path), K(storage_info_));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateBackupSetPhysicalTask not init", KR(ret));
  } else if (OB_SUCCESS != ctx_->get_result()) {
    //other task has failed, skip validate
  } else {
    ObBackupTabletMeta tablet_meta;
    bool is_deleted_tablet = false;
    int64_t validate_checkpoint = OB_INVALID_ID;
    int64_t total_read_bytes = 0;
    ObBackupDataType backup_data_type;
    ObBackupValidateTabletFinishTask *tablet_finish_task = nullptr;
    if (OB_FAIL(get_backup_data_type_(backup_data_type))) {
      LOG_WARN("failed to get backup data type", KR(ret));
    } else if (OB_FAIL(get_tablet_meta_(tablet_id_, backup_data_type, is_deleted_tablet, tablet_meta))) {
      LOG_WARN("failed to get tablet metas", KR(ret), K_(tablet_id), K(backup_data_type));
    } else if (FALSE_IT(read_bytes_ = tablet_meta.get_serialize_size())) {
    } else if (OB_FAIL(generate_tablet_finish_task_(task_id_, tablet_finish_task))) {
      LOG_WARN("failed to generate tablet finish task", KR(ret), K(task_id_));
    } else if (is_deleted_tablet) {
        // deleted tablet, do not need to validate
    } else if (OB_FAIL(validate_sstables_(tablet_meta, backup_data_type, tablet_finish_task))) {
      LOG_WARN("failed to validate tablet", KR(ret), K(tablet_meta), K(backup_data_type));
    } else if (OB_FAIL(tablet_finish_task->add_read_bytes(read_bytes_))) {
      LOG_WARN("failed to add read bytes", KR(ret), K(read_bytes_));
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    char error_msg[OB_MAX_VALIDATE_LOG_INFO_LENGTH] = {0};
    if (OB_TMP_FAIL(databuff_printf(error_msg, OB_MAX_VALIDATE_LOG_INFO_LENGTH,
                                        "failed to validate tablet id: %ld", tablet_id_.id()))) {
      LOG_WARN("failed to print error message", KR(ret), KR(tmp_ret), K(tablet_id_));
    }
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, error_msg, param_, GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(error_msg), K(dag_id), K_(param));
      }
    }
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::generate_tablet_finish_task_(
    const int64_t task_id,
    ObBackupValidateTabletFinishTask *&tablet_finish_task)
{
  int ret = OB_SUCCESS;
  if (task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_id));
  } else {
    ObBackupValidateBaseDag *base_dag = nullptr;
    ObIDag *dag = nullptr;
    if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (FALSE_IT(base_dag = static_cast<ObBackupValidateBaseDag *>(dag))) {
    } else if (OB_FAIL(base_dag->alloc_task(tablet_finish_task))) {
      LOG_WARN("failed to alloc tablet finish task", KR(ret));
    } else if (OB_ISNULL(tablet_finish_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet finish task is null", KR(ret), KP(tablet_finish_task));
    } else if (OB_FAIL(tablet_finish_task->init(*base_dag, task_id_))) {
      LOG_WARN("failed to init tablet finish task", KR(ret));
    } else if (OB_FAIL(this->add_child(*tablet_finish_task))) {
      LOG_WARN("failed to add tablet finish task as child of this task", KR(ret));
    } else if (OB_FAIL(base_dag->add_task(*tablet_finish_task))) {
      LOG_WARN("failed to add tablet finish task to dag", KR(ret));
    }
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::get_backup_data_type_(ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  if (tablet_id_.is_ls_inner_tablet()) {
    backup_data_type.set_sys_data_backup();
  } else {
    backup_data_type.set_user_data_backup();
  }
  return ret;
}

// get tablet meta from extern tablet meta reader according to tablet id
int ObBackupValidateBackupSetPhysicalTask::get_tablet_meta_(
    const common::ObTabletID &tablet_id,
    const ObBackupDataType &backup_data_type,
    bool &is_deleted_tablet,
    ObBackupTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(meta_index_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta index store is null", KR(ret));
  } else {
    is_deleted_tablet = false;
    backup::ObBackupMetaIndex tablet_meta_index;
    backup::ObBackupMetaType meta_type = backup::ObBackupMetaType::BACKUP_TABLET_META;
    share::ObBackupPath full_path;
    ObStorageIdMod mod;
    mod.storage_id_ = param_.dest_id_;
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    if (OB_FAIL(meta_index_store_->get_backup_meta_index(backup_data_type, tablet_id, meta_type, tablet_meta_index))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        is_deleted_tablet = true;
      } else {
        LOG_WARN("failed to get backup meta index", KR(ret), K(tablet_id), K(meta_type));
      }
    } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_dest_, tablet_meta_index.ls_id_,
                                              backup_data_type, tablet_meta_index.turn_id_, tablet_meta_index.retry_id_,
                                              tablet_meta_index.file_id_, full_path))) {
      LOG_WARN("failed to get macro block backup path", KR(ret), K(tablet_meta_index),
                  K_(backup_dest), K(backup_data_type));
    } else if (OB_FAIL(ObLSBackupRestoreUtil::read_tablet_meta(full_path.get_obstr(), backup_dest_.get_storage_info(),
                                                    mod, tablet_meta_index, tablet_meta))) {
      LOG_WARN("failed to read sstable metas", KR(ret), K(full_path), K(tablet_meta_index), K_(backup_dest));
    }
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateBackupSetPhysicalTask not init", KR(ret));
  } else if (OB_SUCCESS != ctx_->get_result()) {
    ret = OB_ITER_END;
    next_task = nullptr;
  } else {
    int64_t task_id = -1;
    ObIDag *dag = nullptr;
    ObBackupValidateBaseDag *base_dag = nullptr;
    ObBackupValidateBackupSetPhysicalTask *physical_task = nullptr;
    if (OB_FAIL(ctx_->get_next_task_id(task_id))) {
      LOG_WARN("failed to get next task id", KR(ret));
    } else if (-1 == task_id) {
      ret = OB_ITER_END;
      next_task = nullptr;
    } else if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (FALSE_IT(base_dag = static_cast<ObBackupValidateBaseDag*>(dag))) {
    } else if (OB_FAIL(base_dag->alloc_task(physical_task))) {
      LOG_WARN("failed to get task", KR(ret), K(task_id));
    } else if (OB_ISNULL(physical_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("physical task is null", KR(ret), KP(physical_task));
    } else if (OB_FAIL(physical_task->init(*base_dag, task_id, backup_set_info_, meta_index_store_))) {
      LOG_WARN("failed to init task", KR(ret), K(base_dag), K(task_id),
                  KP_(meta_index_store));
    } else {
      next_task = physical_task;
      physical_task = nullptr;
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret ){
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to generate next tablet validate task", param_,
                                                    GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
    LOG_ERROR("failed to generate next task", KR(ret), KR(tmp_ret), K_(task_id), K_(param));
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::validate_sstables_(
    const ObBackupTabletMeta &tablet_meta,
    const ObBackupDataType &backup_data_type,
    ObBackupValidateTabletFinishTask *tablet_finish_task)
{
  int ret = OB_SUCCESS;
  ObArray<backup::ObBackupSSTableMeta> sstable_meta_array;
  common::ObTabletID tablet_id;
  if (!backup_dest_.is_valid() || !tablet_meta.is_valid() || OB_ISNULL(tablet_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(backup_dest_), K(tablet_meta), KP(tablet_finish_task));
  } else if (FALSE_IT(tablet_id = tablet_meta.tablet_id_)) {
  } else if (OB_FAIL(get_sstable_meta_(tablet_id, backup_data_type, sstable_meta_array))) {
    LOG_WARN("failed to get sstable meta", KR(ret), K(tablet_id), K(backup_data_type));
  } else if (sstable_meta_array.empty()) {
    //all sstables of this tablet are empty, skip validate
  } else if (OB_FAIL(tablet_finish_task->set_sstable_meta_array(sstable_meta_array))) {
    LOG_WARN("failed to set sstable meta array", KR(ret), K(sstable_meta_array));
  } else if (OB_FAIL(generate_sstable_validate_task_(tablet_meta.tablet_meta_, tablet_finish_task))) {
    LOG_WARN("failed to generate sstable validate task", KR(ret), K(sstable_meta_array), K(tablet_meta), KP(tablet_finish_task));
  } else if (OB_FAIL(validate_filled_tx_scn_(sstable_meta_array))) {
    LOG_WARN("failed to validate filled tx scn", KR(ret), K(tablet_id), K(sstable_meta_array));
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::validate_filled_tx_scn_(
    const ObIArray<backup::ObBackupSSTableMeta> &sstable_meta_array)
{
  int ret = OB_SUCCESS;
  if (sstable_meta_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable meta array is empty", KR(ret), K(sstable_meta_array));
  } else {
    share::SCN backup_tx_table_filled_tx_scn;
    ObSArray<storage::ObSSTableTxScnMeta> tx_scn_meta_array;
    ObSSTableTxScnMeta tx_scn_meta;
    if (OB_FAIL(get_backup_tx_data_table_filled_tx_scn_(backup_tx_table_filled_tx_scn))) {
      LOG_WARN("failed to get backup tx data table filled tx scn", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_meta_array.count(); ++i) {
      const backup::ObBackupSSTableMeta &sstable_meta = sstable_meta_array.at(i);
      const storage::ObITable::TableKey &table_key = sstable_meta.sstable_meta_.table_key_;
      if (ObITable::TableType::MINOR_SSTABLE == table_key.table_type_) {
        tx_scn_meta.reset();
        const bool contain_uncommitted_row = sstable_meta.sstable_meta_.basic_meta_.contain_uncommitted_row_;
        const share::SCN filled_tx_scn = sstable_meta.sstable_meta_.basic_meta_.filled_tx_scn_;
        const share::SCN end_scn = table_key.get_end_scn();
        if (OB_FAIL(tx_scn_meta.set(contain_uncommitted_row, filled_tx_scn, end_scn))) {
          LOG_WARN("failed to set tx scn meta", KR(ret), K(contain_uncommitted_row), K(filled_tx_scn), K(end_scn));
        } else if (OB_FAIL(tx_scn_meta_array.push_back(tx_scn_meta))) {
          LOG_WARN("failed to push tx scn meta into array", KR(ret), K(tx_scn_meta));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tx_scn_meta_array.empty()) {
      //don't has minor sstable, skip
    } else if (OB_FAIL(ObTablet::check_tx_data_with_minor_tx_scn_meta_array(tx_scn_meta_array,
                                                                                backup_tx_table_filled_tx_scn))) {
      LOG_WARN("failed to check tx data with minor tx scn meta array", KR(ret), K(tx_scn_meta_array),
                K(backup_tx_table_filled_tx_scn));
    }
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::get_backup_tx_data_table_filled_tx_scn_(share::SCN &filled_tx_scn)
{
  int ret = OB_SUCCESS;
  share::ObBackupDataType sys_backup_data_type;
  sys_backup_data_type.set_sys_data_backup();
  ObRestoreMetaIndexStore *meta_index_store = nullptr;
  bool is_backup_set_support_quick_restore = false;
  const share::ObBackupSetFileDesc::Compatible &compatible = backup_set_info_.backup_set_file_.backup_compatible_;
  if (share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(compatible)) {
    is_backup_set_support_quick_restore = true;
  }
  if (OB_FAIL(meta_index_store_->get_backup_meta_index_store(sys_backup_data_type, meta_index_store))) {
    LOG_WARN("failed to get meta index store", KR(ret), K(sys_backup_data_type), K(backup_set_info_), K(backup_dest_));
  } else if (OB_FAIL(backup::ObBackupUtils::get_backup_tx_data_table_filled_tx_scn(*meta_index_store,
                        is_backup_set_support_quick_restore, backup_dest_, param_.dest_id_, filled_tx_scn))) {
    LOG_WARN("failed to get backup tx data table filled tx scn", KR(ret), K_(param), K_(backup_dest),
                K(is_backup_set_support_quick_restore));
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::get_sstable_meta_(
    const common::ObTabletID &tablet_id,
    const ObBackupDataType &backup_data_type,
    ObIArray<backup::ObBackupSSTableMeta> &sstable_meta_array)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id));
  } else {
    ObBackupMetaIndex meta_index;
    ObBackupPath backup_path;
    ObArray<ObBackupSSTableMeta> meta_array;
    ObStorageIdMod mod;
    mod.storage_id_ = param_.dest_id_;
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    const backup::ObBackupMetaType meta_type = backup::ObBackupMetaType::BACKUP_SSTABLE_META;
    ObSArray<backup::ObBackupSSTableMeta> tmp_sstable_meta_array;
    if (OB_FAIL(meta_index_store_->get_backup_meta_index(backup_data_type, tablet_id, meta_type, meta_index))) {
      LOG_WARN("failed to get backup meta index", KR(ret), K(tablet_id), K(meta_type));
    } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_dest_, meta_index.ls_id_,
                                                        backup_data_type, meta_index.turn_id_, meta_index.retry_id_,
                                                        meta_index.file_id_, backup_path))) {
      LOG_WARN("failed to get macro block backup path", KR(ret), K(meta_index), K_(backup_dest), K(backup_data_type));
    } else if (OB_FAIL(ObLSBackupRestoreUtil::read_sstable_metas(backup_path.get_obstr(), backup_dest_.get_storage_info(),
                                                     mod, meta_index, &OB_BACKUP_META_CACHE, tmp_sstable_meta_array))) {
      LOG_WARN("failed to read sstable metas", KR(ret), K(backup_path), K(meta_index), K_(backup_dest));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_sstable_meta_array.count(); ++i) {
      const backup::ObBackupSSTableMeta &sstable_meta = tmp_sstable_meta_array.at(i);
      read_bytes_ += sstable_meta.get_serialize_size();
      if (sstable_meta.sstable_meta_.is_empty_sstable()) {
        continue;
      } else if (OB_FAIL(sstable_meta_array.push_back(sstable_meta))) {
        LOG_WARN("failed to push back sstable meta", KR(ret), K(sstable_meta));
      }
    }
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalTask::generate_sstable_validate_task_(
  const storage::ObMigrationTabletParam &tablet_param,
  ObBackupValidateTabletFinishTask *tablet_finish_task)
{
  int ret = OB_SUCCESS;
  if (!tablet_param.is_valid() || OB_ISNULL(tablet_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable meta array is empty or tablet param is invalid", KR(ret), K(tablet_param), KP(tablet_finish_task));
  } else {
    ObBackupValidateBaseDag *base_dag = nullptr;
    ObIDag *dag = nullptr;
    bool check_child_task_status = false;
    if (OB_ISNULL(dag = get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (FALSE_IT(base_dag = static_cast<ObBackupValidateBaseDag *>(dag))) {
    } else {
      ObBackupValidateSSTableTask *sstable_task = nullptr;
      const int64_t first_sstable_index = 0;
      if (OB_FAIL(base_dag->alloc_task(sstable_task))) {
        LOG_WARN("failed to alloc sstable task", KR(ret));
      } else if (OB_ISNULL(sstable_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable task is null", KR(ret), KP(sstable_task));
      } else if (OB_FAIL(sstable_task->init(*base_dag, first_sstable_index, backup_set_info_,
                                      tablet_param, meta_index_store_, tablet_finish_task))) {
        LOG_WARN("failed to init sstable task", KR(ret), K(first_sstable_index), K(tablet_param),
                    KP(meta_index_store_), KP(tablet_finish_task));
      } else if (OB_FAIL(sstable_task->add_child(*tablet_finish_task, check_child_task_status))) {
        LOG_WARN("failed to add tablet finish task as child of sstable task", KR(ret));
      } else if (OB_FAIL(base_dag->add_task(*sstable_task))) {
        LOG_WARN("failed to add sstable task to dag", KR(ret));
      }
    }
  }
  return ret;
}

//////////////////////////// ObBackupValidateTabletFinishTask ///////////////////////////////

ObBackupValidateTabletFinishTask::ObBackupValidateTabletFinishTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_BACKUPSET_PHYSICAL),
    is_inited_(false),
    ctx_(nullptr),
    report_ctx_(),
    param_(),
    tablet_id_(),
    task_id_(-1),
    read_bytes_(0),
    sstable_meta_array_()
{
}

ObBackupValidateTabletFinishTask::~ObBackupValidateTabletFinishTask()
{
}

int ObBackupValidateTabletFinishTask::init(
    ObBackupValidateBaseDag &base_dag,
    const int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet finish task init twice", KR(ret));
  } else if (task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(task_id));
  } else {
    ObBackupValidateTaskContext *ctx = base_dag.get_task_context();
    const backup::ObBackupReportCtx &report_ctx = base_dag.get_report_ctx();
    const ObBackupValidateDagNetInitParam &param = base_dag.get_param();
    common::ObTabletID tablet_id;
    if (OB_ISNULL(ctx) || !report_ctx.is_valid() || !param.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is null", KR(ret), KP(ctx), K(report_ctx), K(param));
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("failed to assign param", KR(ret), K(param));
    } else if (OB_FAIL(ctx->get_tablet_id(task_id, tablet_id))) {
      LOG_WARN("failed to get tablet id", KR(ret), K(param), K(task_id));
    } else {
      tablet_id_ = tablet_id;
      ctx_ = ctx;
      report_ctx_ = report_ctx;
      task_id_ = task_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupValidateTabletFinishTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish task not init", KR(ret));
  } else if (OB_SUCCESS != ctx_->get_result()) {
  } else {
    bool update_checkpoint = false;
    int64_t validate_checkpoint = 0;
    int64_t delta_read_bytes = 0;
    int64_t total_read_bytes = 0;
    common::ObMySQLTransaction trans;
    if (OB_FAIL(ctx_->remove_running_task_id(task_id_, read_bytes_, update_checkpoint,
                                                    validate_checkpoint, delta_read_bytes, total_read_bytes))) {
      LOG_WARN("failed to remove running tablet task id", KR(ret), K_(task_id));
    } else if (update_checkpoint) {
      if (OB_FAIL(trans.start(report_ctx_.sql_proxy_, gen_meta_tenant_id(param_.tenant_id_)))) {
        LOG_WARN("failed to start transaction", KR(ret));
      } else if (OB_FAIL(ObBackupValidateLSTaskOperator::update_stats(trans, param_.task_id_,
                                                                    param_.tenant_id_, param_.ls_id_,
                                                                    total_read_bytes, validate_checkpoint))) {
        LOG_WARN("failed to update stats", K(ret), K_(param), K(validate_checkpoint), K(total_read_bytes));
      } else if (0 != delta_read_bytes && OB_FAIL(ObBackupValidateTaskOperator::update_validated_bytes(
                     trans, param_.task_id_, param_.tenant_id_, delta_read_bytes))) {
        LOG_WARN("failed to add validated bytes", KR(ret), K(delta_read_bytes), K(param_));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    char error_msg[OB_MAX_VALIDATE_LOG_INFO_LENGTH] = {0};
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_TMP_FAIL(databuff_printf(error_msg, OB_MAX_VALIDATE_LOG_INFO_LENGTH,
                                        "failed to finish validate tablet task id: %ld", tablet_id_.id()))) {
      LOG_WARN("failed to print error message", KR(ret), KR(tmp_ret), K(tablet_id_));
    }
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, error_msg, param_, GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(error_msg), K(dag_id), K_(param));
      }
    }
    LOG_ERROR("failed to finished validate tablet task", KR(ret), KR(tmp_ret), K_(param), K(tablet_id_));
  } else {
    LOG_INFO("successfully finish tablet validate task", K_(task_id), K_(param), K(tablet_id_));
  }
  return ret;
}

int ObBackupValidateTabletFinishTask::set_sstable_meta_array(
    const common::ObIArray<backup::ObBackupSSTableMeta> &sstable_meta_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish task not init", KR(ret));
  } else if (sstable_meta_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable meta array is empty", KR(ret), K(sstable_meta_array));
  } else if (OB_FAIL(sstable_meta_array_.assign(sstable_meta_array))) {
    LOG_WARN("failed to assign sstable meta array", KR(ret), K(sstable_meta_array));
  }
  return ret;
}

int ObBackupValidateTabletFinishTask::get_sstable_meta(const int64_t index, backup::ObBackupSSTableMeta &sstable_meta)
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= sstable_meta_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(index));
  } else if (OB_FAIL(sstable_meta.assign(sstable_meta_array_.at(index)))) {
    LOG_WARN("failed to assign sstable meta", KR(ret), K(index), K(sstable_meta));
  }
  return ret;
}

int ObBackupValidateTabletFinishTask::add_read_bytes(const int64_t read_bytes)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish task not init", KR(ret));
  } else if (read_bytes < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_bytes));
  } else {
    ATOMIC_AAF(&read_bytes_, read_bytes);
  }
  return ret;
}

ObBackupValidateSSTableTask::ObBackupValidateSSTableTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_BACKUPSET_PHYSICAL),
    is_inited_(false),
    param_(),
    report_ctx_(),
    ctx_(nullptr),
    backup_dest_(),
    backup_set_info_(),
    meta_index_store_(nullptr),
    sstable_meta_(),
    sstable_index_(-1),
    tablet_param_(),
    tablet_finish_task_(nullptr)
{
}

ObBackupValidateSSTableTask::~ObBackupValidateSSTableTask()
{
}

int ObBackupValidateSSTableTask::init(
    ObBackupValidateBaseDag &base_dag,
    const int64_t sstable_index,
    ObExternBackupSetInfoDesc &backup_set_info,
    const storage::ObMigrationTabletParam &tablet_param,
    backup::ObBackupMetaIndexStoreWrapper *meta_index_store,
    ObBackupValidateTabletFinishTask *tablet_finish_task)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateSSTableTask init twice", KR(ret));
  } else if (OB_ISNULL(tablet_finish_task) || OB_ISNULL(meta_index_store)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null ptr", KR(ret), KP(tablet_finish_task), KP(meta_index_store));
  } else if (OB_FAIL(tablet_finish_task->get_sstable_meta(sstable_index, sstable_meta_))) {
    LOG_WARN("failed to get sstable meta", KR(ret), K(sstable_index));
  } else if (!tablet_param.is_valid() || !backup_set_info.is_valid() || !sstable_meta_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sstable_meta_), K(tablet_param), K(backup_set_info));
  } else if (!base_dag.is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("base dag is not inited", KR(ret));
  } else {
    const ObBackupValidateDagNetInitParam &param = base_dag.get_param();
    ObBackupValidateTaskContext *ctx = base_dag.get_task_context();
    const ObBackupDest &backup_dest = base_dag.get_backup_dest();
    const backup::ObBackupReportCtx &report_ctx = base_dag.get_report_ctx();
    ObArenaAllocator *dag_allocator = base_dag.get_allocator();
    if (OB_ISNULL(ctx) || OB_ISNULL(dag_allocator) || !backup_dest.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("argument is invalid", KR(ret), KP(ctx), KP(dag_allocator), K(backup_dest));
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("failed to assign param", KR(ret), K(param));
    } else if (OB_FAIL(tablet_param_.assign(tablet_param))) {
      LOG_WARN("failed to assign tablet param", KR(ret), K(tablet_param));
    } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to assign backup dest", KR(ret), K(backup_dest));
    } else {
      ctx_ = ctx;
      report_ctx_ = report_ctx;
      sstable_index_ = sstable_index;
      backup_set_info_ = backup_set_info;
      meta_index_store_ = meta_index_store;
      tablet_finish_task_ = tablet_finish_task;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupValidateSSTableTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateSSTableTask not init", KR(ret));
  } else if (OB_ISNULL(tablet_finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet finish task is null", KR(ret), KP_(tablet_finish_task));
  } else {
    int64_t next_sstable_indx = sstable_index_ + 1;
    if (next_sstable_indx >= tablet_finish_task_->get_sstable_meta_count()) {
      ret = OB_ITER_END;
      next_task = nullptr;
    } else {
      ObIDag *dag = nullptr;
      ObBackupValidateSSTableTask *sstable_task = nullptr;
      ObBackupValidateBaseDag *base_dag = nullptr;
      if (OB_ISNULL(dag = this->get_dag())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag is null", KR(ret));
      } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
      } else if (FALSE_IT(base_dag = static_cast<ObBackupValidateBaseDag *>(dag))) {
      } else if (OB_FAIL(base_dag->alloc_task(sstable_task))) {
        LOG_WARN("failed to alloc sstable task", KR(ret));
      } else if (OB_ISNULL(sstable_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable task is null", KR(ret), KP(sstable_task));
      } else if (OB_FAIL(sstable_task->init(*base_dag, next_sstable_indx, backup_set_info_,
                                                tablet_param_, meta_index_store_, tablet_finish_task_))) {
        LOG_WARN("failed to init next task", KR(ret), K(next_sstable_indx), K_(tablet_param),
                    KP(meta_index_store_), KP(tablet_finish_task_));
      } else {
        next_task = sstable_task;
        sstable_task = nullptr;
      }
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to generate next tablet validate task", param_,
                                                    GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
    LOG_ERROR("failed to generate next task", KR(ret), KR(tmp_ret), K_(sstable_index), K_(param));
  }
  return ret;
}

int ObBackupValidateSSTableTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateSSTableTask not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), KP_(ctx));
  } else if (OB_SUCCESS != ctx_->get_result() || sstable_meta_.sstable_meta_.is_empty_sstable()) {
    // other task has failed or sstable is empty, skip validate
  } else if (OB_FAIL(generate_validate_macro_block_task_())) {
    LOG_WARN("failed to generate validate macro block task", KR(ret));
  } else if (OB_FAIL(validate_sstable_checksum_())) {
    LOG_WARN("failed to validate sstable", KR(ret));
  } else {
    LOG_INFO("successfully validate sstable checksum", KR(ret), K_(sstable_meta), K_(tablet_param));
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    char error_msg[OB_MAX_VALIDATE_LOG_INFO_LENGTH] = {0};
    uint64_t tablet_id = tablet_param_.tablet_id_.id();
    if (OB_TMP_FAIL(databuff_printf(error_msg, OB_MAX_VALIDATE_LOG_INFO_LENGTH,
                                        "failed to validate tablet sstable, tablet: %ld", tablet_id))) {
      LOG_WARN("failed to print error message", KR(ret), KR(tmp_ret));
    }
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, error_msg, param_, GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(error_msg), K(dag_id), K_(param));
      }
    }
  }
  return ret;
}

int ObBackupValidateSSTableTask::generate_validate_macro_block_task_()
{
  int ret = OB_SUCCESS;

  if (!sstable_meta_.is_valid() || !tablet_param_.is_valid() || !backup_dest_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable meta is invalid", KR(ret), K_(sstable_meta), K_(tablet_param), K_(backup_dest));
  } else if (OB_ISNULL(meta_index_store_) || OB_ISNULL(tablet_finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null ptr", KR(ret), KP_(meta_index_store), KP_(tablet_finish_task));
  } else {
    common::ObArray<blocksstable::ObDataMacroBlockMeta> macro_metas;
    ObBackupValidateMacroBlockTask *macro_task = nullptr;
    ObBackupValidateMacroBlockFinishTask *macro_task_finish_task = nullptr;
    ObIDag *dag = nullptr;
    ObBackupValidateBaseDag *base_dag = nullptr;
    uint64_t tenant_id = MTL_ID();
    if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (FALSE_IT(base_dag = static_cast<ObBackupValidateBaseDag*>(dag))) {
    } else if (OB_FAIL(base_dag->alloc_task(macro_task_finish_task))) {
      LOG_WARN("failed to alloc macro task finish task", KR(ret));
    } else if (OB_ISNULL(macro_task_finish_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro task finish task is null", KR(ret), KP(macro_task_finish_task));
    } else if (OB_FAIL(macro_task_finish_task->init(*base_dag, sstable_meta_, tablet_param_, backup_set_info_,
                                                        *meta_index_store_, tablet_finish_task_))) {
      LOG_WARN("failed to init macro task finish task", KR(ret), K(sstable_meta_), K(tablet_param_), K(backup_set_info_),
                  K_(meta_index_store), K_(backup_dest), K_(param), KP(tablet_finish_task_));
    } else {
      ObArray<backup::ObBackupMacroBlockIndex> macro_index_array;
      bool check_child_task_status = false;
      if (OB_FAIL(base_dag->alloc_task(macro_task))) {
        LOG_WARN("failed to alloc macro task", KR(ret));
      } else if (OB_ISNULL(macro_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro task is null", KR(ret), KP(macro_task));
      } else if (OB_FAIL(macro_task_finish_task->get_next_bacth_macro_index_array(macro_index_array))) {
        LOG_WARN("failed to get next batch macro index array", KR(ret));
      } else if (OB_FAIL(macro_task->init(*base_dag, sstable_meta_, backup_set_info_,
                                              macro_index_array, macro_task_finish_task, tablet_finish_task_))) {
        LOG_WARN("failed to init macro task", KR(ret), K(sstable_meta_),
                    K(backup_set_info_), K(macro_index_array), KP(macro_task_finish_task));
      } else if (OB_FAIL(macro_task_finish_task->add_child(*tablet_finish_task_, check_child_task_status))) {
        LOG_WARN("failed to add tablet finish task", KR(ret), KP(tablet_finish_task_));
      } else if (OB_FAIL(macro_task->add_child(*macro_task_finish_task))) {
        LOG_WARN("failed to add macro task finish task", KR(ret), KP(macro_task_finish_task));
      } else if (OB_FAIL(base_dag->add_task(*macro_task))) {
        LOG_WARN("failed to add macro task to dag", KR(ret));
      } else if (OB_FAIL(base_dag->add_task(*macro_task_finish_task))) {
        LOG_WARN("failed to add macro task finish task to dag", KR(ret));
      }
    }
  }
  return ret;
}

int ObBackupValidateSSTableTask::validate_sstable_checksum_()
{
  int ret = OB_SUCCESS;
  if (!sstable_meta_.is_valid() || !tablet_param_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable meta or tablet param is invalid", KR(ret), K_(sstable_meta), K_(tablet_param));
  } else {
    blocksstable::ObDataMacroBlockMeta macro_meta;
    uint64_t actual_data_checksum = 0;
    common::ObArenaAllocator allocator;
    backup::ObBackupSSTableSecMetaIterator sec_meta_iterator;
    const uint64_t expected_data_checksum = static_cast<uint64_t>(sstable_meta_.sstable_meta_.basic_meta_.data_checksum_);
    const storage::ObITableReadInfo *rowkey_read_info = nullptr;
    ObStorageIdMod mod(param_.dest_id_, ObStorageUsedMod::STORAGE_USED_BACKUP);
    if (OB_FAIL(ObBackupValidateObUtils::get_rowkey_read_info(sstable_meta_.sstable_meta_.table_key_, tablet_param_,
                                                                  allocator, rowkey_read_info))) {
      LOG_WARN("failed to get rowkey read info", KR(ret), K_(sstable_meta), K_(tablet_param));
    } else if (OB_ISNULL(rowkey_read_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey read info is null", KR(ret), K(sstable_meta_.sstable_meta_.table_key_), K(tablet_param_));
    } else if (OB_FAIL(ObBackupValidateObUtils::get_sec_meta_iterator(sstable_meta_, *rowkey_read_info, backup_set_info_,
                                                              *meta_index_store_, backup_dest_, mod, sec_meta_iterator))) {
      LOG_WARN("failed to get sec meta iterator", KR(ret), K_(sstable_meta),
                    K_(tablet_param), K_(backup_set_info), K_(meta_index_store), K_(backup_dest));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sec_meta_iterator.get_next(macro_meta))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next macro meta", KR(ret));
        }
      } else {
        actual_data_checksum = ob_crc64_sse42(actual_data_checksum, &macro_meta.val_.data_checksum_,
                                                sizeof(actual_data_checksum));
      }
    }
    if (OB_SUCC(ret)) {
      if (actual_data_checksum != expected_data_checksum) {
        ret = OB_CHECKSUM_ERROR;
        LOG_WARN("data checksum mismatch", KR(ret), K_(sstable_meta), K(actual_data_checksum), K_(sstable_meta));
      }
    }
  }
  return ret;
}

ObBackupValidateMacroBlockTask::ObBackupValidateMacroBlockTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_BACKUPSET_PHYSICAL),
    is_inited_(false),
    ctx_(nullptr),
    backup_dest_(),
    report_ctx_(),
    param_(),
    sstable_meta_(),
    backup_set_info_(),
    macro_index_array_(),
    macro_block_finish_task_(nullptr),
    tablet_finish_task_(nullptr)
{
}

ObBackupValidateMacroBlockTask::~ObBackupValidateMacroBlockTask()
{
}

int ObBackupValidateMacroBlockTask::init(
    ObBackupValidateBaseDag &base_dag,
    const backup::ObBackupSSTableMeta &sstable_meta,
    const ObExternBackupSetInfoDesc &backup_set_info,
    const ObIArray<backup::ObBackupMacroBlockIndex> &macro_index_array,
    ObBackupValidateMacroBlockFinishTask *macro_block_finish_task,
    ObBackupValidateTabletFinishTask *tablet_finish_task)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateMacroBlockTask init twice", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(macro_block_finish_task) || macro_index_array.empty() || OB_ISNULL(tablet_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro block finish task is null or array is empty", KR(ret),
                KP(macro_block_finish_task), K(macro_index_array), KP(tablet_finish_task));
  } else if (!sstable_meta.is_valid() || !backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable meta is invalid", KR(ret), K(sstable_meta), K(backup_set_info));
  } else {
    tablet_finish_task_ = tablet_finish_task;
    const ObBackupValidateDagNetInitParam &param = base_dag.get_param();
    ObBackupValidateTaskContext *ctx = base_dag.get_task_context();
    const ObBackupDest &backup_dest = base_dag.get_backup_dest();
    const backup::ObBackupReportCtx &report_ctx = base_dag.get_report_ctx();
    if (OB_ISNULL(ctx) || !report_ctx.is_valid() || !backup_dest.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is null", KR(ret), KP(ctx), K(report_ctx), K(backup_dest));
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("failed to assign param", KR(ret), K(param));
    } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
      LOG_WARN("failed to assign backup dest", KR(ret), K(backup_dest));
    } else if (OB_FAIL(sstable_meta_.assign(sstable_meta))) {
      LOG_WARN("failed to assign sstable meta", KR(ret), K(sstable_meta));
    } else if (OB_FAIL(macro_index_array_.assign(macro_index_array))) {
      LOG_WARN("failed to assign macro index array", KR(ret), K(macro_index_array));
    } else {
      macro_block_finish_task_ = macro_block_finish_task;
      ctx_ = ctx;
      report_ctx_ = report_ctx;
      backup_set_info_ = backup_set_info;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupValidateMacroBlockTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateMacroBlockTask not init", KR(ret));
  } else if (OB_ISNULL(ctx_) || macro_index_array_.empty() || !sstable_meta_.is_valid() || !backup_dest_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter unexpected", KR(ret), KP_(ctx), K(macro_index_array_), K_(sstable_meta), K_(backup_dest));
  } else if (OB_SUCCESS != ctx_->get_result()) {
    //other task has failed, skip validate
  } else {
    blocksstable::ObBufferReader data_buffer;
    blocksstable::ObBufferReader read_buffer;
    common::ObArenaAllocator allocator;
    share::ObBackupDataType data_type;
    const ObITable::TableKey &table_key = sstable_meta_.sstable_meta_.table_key_;
    const int64_t backup_set_id = backup_set_info_.backup_set_file_.backup_set_id_;
    if (OB_FAIL(alloc_macro_block_data_buffer_(allocator, read_buffer, data_buffer))) {
      LOG_WARN("failed to alloc macro block data buffer", KR(ret));
    } else if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, data_type))) {
      LOG_WARN("fail to get backup data type", K(ret), K(table_key));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_index_array_.count(); ++i) {
      data_buffer.set_pos(0);
      read_buffer.set_pos(0);
      const backup::ObBackupMacroBlockIndex &macro_index = macro_index_array_.at(i);
      if (OB_FAIL(read_macro_block_(macro_index, data_type, data_buffer, read_buffer))) {
        LOG_WARN("failed to read macro block", KR(ret), K(macro_index));
      }
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to validate macro block", param_, GCTX.self_addr(),
                                                    dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
  }
  return ret;
}

int ObBackupValidateMacroBlockTask::read_macro_block_(
    const backup::ObBackupMacroBlockIndex &macro_index,
    const share::ObBackupDataType &data_type,
    blocksstable::ObBufferReader &data_buffer,
    blocksstable::ObBufferReader &read_buffer)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || !macro_index.is_valid() || !backup_dest_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parameter unexpected", KR(ret), KP_(ctx), K(macro_index), K_(backup_dest));
  } else {
    share::ObBackupPath backup_path;
    share::ObBackupDest backup_set_dest;
    ObBackupMetaIndex index;
    const int64_t align_size = DIO_READ_ALIGN_SIZE;
    const share::ObBackupStorageInfo *storage_info = backup_dest_.get_storage_info();
    ObStorageIdMod mod;
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    mod.storage_id_ = param_.dest_id_;
    ObBackupDeviceMacroBlockId backup_macro_id;
    index.ls_id_ = macro_index.ls_id_;
    index.turn_id_ = macro_index.turn_id_;
    index.retry_id_ = macro_index.retry_id_;
    index.file_id_ = macro_index.file_id_;
    ObTenantBackupDestInfoMgr *mgr = MTL(ObTenantBackupDestInfoMgr *);
    if (OB_ISNULL(mgr) || OB_ISNULL(storage_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant restore info mgr or storage info is null", KR(ret), KP(mgr), KP(storage_info));
    } else if (OB_FAIL(mgr->get_backup_dest(macro_index.backup_set_id_, backup_set_dest))) {
      LOG_WARN("failed to get backup dest", KR(ret), K(macro_index));
    } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest, index.ls_id_,
                                            data_type, index.turn_id_, index.retry_id_, index.file_id_, backup_path))) {
      LOG_WARN("failed to get macro block backup path", KR(ret), K(index), K(backup_set_dest), K(data_type));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_data_with_retry(backup_path.get_obstr(),
                                                              storage_info, mod, macro_index,
                                                              align_size, read_buffer, data_buffer))) {
        LOG_WARN("failed to read macro block data", K(ret), K(macro_index), KP(storage_info));
    } else if (OB_FAIL(tablet_finish_task_->add_read_bytes(macro_index.length_))) {
      LOG_WARN("failed to add read bytes", KR(ret), K(macro_index));
    }
  }
  return ret;
}

int ObBackupValidateMacroBlockTask::alloc_macro_block_data_buffer_(
  common::ObArenaAllocator &allocator,
  blocksstable::ObBufferReader &read_buffer,
  blocksstable::ObBufferReader &data_buffer)
{
  int ret = OB_SUCCESS;
  // TODO(yangyi.yyy): change to a general value later
  const int64_t READ_BUFFER_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE * 2;
  char *buf = NULL;
  char *read_buf = NULL;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator.alloc(READ_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read_buf", K(ret));
  } else {
    data_buffer.assign(buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
    read_buffer.assign(read_buf, READ_BUFFER_SIZE);
  }

  if (OB_FAIL(ret)) {
    allocator.reset();
  }

  return ret;
}

int ObBackupValidateMacroBlockTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateMacroBlockTask not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ is null", KR(ret), KP_(ctx));
  } else if (OB_SUCCESS != ctx_->get_result()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(macro_block_finish_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block finish task is null", KR(ret), KP_(macro_block_finish_task));
  } else {
    ObArray<backup::ObBackupMacroBlockIndex> next_index_array;
    if (OB_FAIL(macro_block_finish_task_->get_next_bacth_macro_index_array(next_index_array))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch macro index array", KR(ret));
      }
    } else {
      ObBackupValidateMacroBlockTask *macro_task = nullptr;
      ObBackupValidateBaseDag *base_dag = nullptr;
      ObIDag *dag = nullptr;
      if (OB_ISNULL(dag = this->get_dag())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag is null", KR(ret));
      } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
      } else if (FALSE_IT(base_dag = static_cast<ObBackupValidateBaseDag*>(dag))) {
      } else if (OB_FAIL(base_dag->alloc_task(macro_task))) {
        LOG_WARN("failed to alloc macro task", KR(ret));
      } else if (OB_ISNULL(macro_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro task is null", KR(ret), KP(macro_task));
      } else if (OB_FAIL(macro_task->init(*base_dag, sstable_meta_, backup_set_info_, next_index_array,
                                              macro_block_finish_task_, tablet_finish_task_))) {
        LOG_WARN("failed to init macro task", KR(ret), K(sstable_meta_),
                    K(backup_set_info_), KPC(base_dag), K(next_index_array), KP(macro_block_finish_task_));
      } else {
        next_task = macro_task;
        macro_task = nullptr;
      }
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to generate next macro block task", param_,
                                                    GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
    LOG_ERROR("failed to generate next task", KR(ret), KR(tmp_ret), K_(param));
  }
  return ret;
}

// ObBackupValidateMacroBlockFinishTask is used to manage sec_meta_iterator_ status and lifetime.
ObBackupValidateMacroBlockFinishTask::ObBackupValidateMacroBlockFinishTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_BACKUPSET_PHYSICAL),
    is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    ctx_(nullptr),
    next_macro_index(0),
    allocator_(),
    param_(),
    sstable_meta_(),
    sec_meta_iterator_(),
    tablet_finish_task_(nullptr)
{
}

int ObBackupValidateMacroBlockFinishTask::init(
    ObBackupValidateBaseDag &base_dag,
    const backup::ObBackupSSTableMeta &sstable_meta,
    const storage::ObMigrationTabletParam &tablet_param,
    const ObExternBackupSetInfoDesc &backup_set_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    ObBackupValidateTabletFinishTask *tablet_finish_task)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateMacroBlockFinishTask init twice", KR(ret));
  } else if (!sstable_meta.is_valid() || !tablet_param.is_valid()
                || !backup_set_info.is_valid() || OB_ISNULL(tablet_finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(sstable_meta), K(tablet_param), K(backup_set_info), KP(tablet_finish_task));
  } else if (OB_FAIL(sstable_meta_.assign(sstable_meta))) {
    LOG_WARN("failed to assign sstable meta", KR(ret), K(sstable_meta));
  } else {
    tablet_finish_task_ = tablet_finish_task;
    const ObBackupValidateDagNetInitParam &param = base_dag.get_param();
    const ObBackupReportCtx &report_ctx = base_dag.get_report_ctx();
    const ObBackupDest &backup_dest = base_dag.get_backup_dest();
    allocator_.set_attr(common::ObMemAttr(MTL_ID(), ObModIds::BACKUP));
    ObStorageIdMod mod;
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_BACKUP;
    mod.storage_id_ = param.dest_id_;
    const storage::ObITableReadInfo *rowkey_read_info = nullptr;
    const common::ObTabletID &tablet_id = sstable_meta.tablet_id_;
    const ObITable::TableKey &table_key = sstable_meta.sstable_meta_.table_key_;
    ObBackupValidateTaskContext *ctx = base_dag.get_task_context();
    if (OB_ISNULL(ctx) || !report_ctx.is_valid() || !param.is_valid() || !backup_dest.is_valid() || OB_ISNULL(ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("report ctx or param or backup dest is invalid", KR(ret), K(report_ctx), K(param), K(backup_dest), KP(ctx));
    } else if (OB_FAIL(param_.assign(param))) {
      LOG_WARN("failed to assign param", KR(ret), K(param));
    } else if (OB_FAIL(ObBackupValidateObUtils::get_rowkey_read_info(
            sstable_meta.sstable_meta_.table_key_, tablet_param, allocator_, rowkey_read_info))) {
      LOG_WARN("failed to get rowkey read info", KR(ret));
    } else if (OB_ISNULL(rowkey_read_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey read info is null", KR(ret));
    } else if (OB_FAIL(ObBackupValidateObUtils::get_sec_meta_iterator(sstable_meta, *rowkey_read_info, backup_set_info,
                                                            meta_index_store, backup_dest, mod, sec_meta_iterator_))) {
      LOG_WARN("failed to get sec meta iterator", KR(ret), K(sstable_meta), K(tablet_param), K(backup_set_info),
                  K(meta_index_store), K(backup_dest), K(mod));
    } else {
      ctx_ = ctx;
      report_ctx_ = report_ctx;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupValidateMacroBlockFinishTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateMacroBlockFinishTask not init", KR(ret));
  } else {
    LOG_INFO("Finish validate sstable macro blocks", KR(ret), K_(sstable_meta));
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "", param_, GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
  }
  return ret;
}

int ObBackupValidateMacroBlockFinishTask::get_next_bacth_macro_index_array(
    ObIArray<backup::ObBackupMacroBlockIndex> &next_macro_index_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateMacroBlockFinishTask not init", KR(ret));
  } else {
    ObSpinLockGuard guard(lock_);
    next_macro_index_array.reset();
    blocksstable::ObDataMacroBlockMeta macro_meta;
    backup::ObBackupDeviceMacroBlockId macro_id;
    backup::ObBackupMacroBlockIndex macro_index;
    while(OB_SUCC(ret)) {
      macro_meta.reset();
      macro_id.reset();
      macro_index.reset();
      if (OB_MAX_MACRO_BLOCK_NUMBER_PER_TASK == next_macro_index_array.count()) {
        break;
      } else if (OB_FAIL(sec_meta_iterator_.get_next(macro_meta))) {
        if (OB_ITER_END == ret && !next_macro_index_array.empty()) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next macro meta", KR(ret));
        }
      } else if (OB_FAIL(macro_id.set(macro_meta.get_macro_id()))) {
        LOG_WARN("failed to set macro id", KR(ret), K(macro_meta));
      } else if (OB_FAIL(macro_id.get_backup_macro_block_index(macro_meta.get_logic_id(), macro_index))) {
        LOG_WARN("failed to get backup macro block index", KR(ret), K(macro_meta), K(macro_id));
      } else if (OB_FAIL(next_macro_index_array.push_back(macro_index))) {
        LOG_WARN("failed to push back macro index", KR(ret), K(macro_index));
      }
    }
  }
  return ret;
}

/*
**************************ObBackupValidateArchivePiecePhysicalTask**************************
*/
ObBackupValidateArchivePiecePhysicalTask::GetSourceFunctor::GetSourceFunctor(
  logservice::ObRemoteRawPathParent &raw_path_parent)
  : raw_path_parent_(raw_path_parent)
{
}

ObBackupValidateArchivePiecePhysicalTask::GetSourceFunctor::~GetSourceFunctor() {}

int ObBackupValidateArchivePiecePhysicalTask::GetSourceFunctor::operator()(
  const share::ObLSID &id, logservice::ObRemoteSourceGuard &guard)
{
  int ret = OB_SUCCESS;
  logservice::ObRemoteRawPathParent *raw_path_parent
      = static_cast<logservice::ObRemoteRawPathParent *>(
          logservice::ObResSrcAlloctor::alloc(share::ObLogRestoreSourceType::RAWPATH, id));
  if (OB_ISNULL(raw_path_parent)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate raw_path_parent", K(id));
  } else if (OB_FAIL(raw_path_parent_.deep_copy_to(*raw_path_parent))) {
    LOG_WARN("failed to deep copy to raw_path_parent");
  } else if (OB_FAIL(guard.set_source(raw_path_parent))) {
    LOG_WARN("failed to set location source");
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(raw_path_parent)) {
    logservice::ObResSrcAlloctor::free(raw_path_parent);
    raw_path_parent = nullptr;
  }
  return ret;
}

ObBackupValidateArchivePiecePhysicalTask::ObBackupValidateArchivePiecePhysicalTask()
  : ObITask(ObITaskType::TASK_TYPE_BACKUP_VALIDATE_ARCHIVE_PIECE_PHYSICAL),
    is_inited_(false),
    is_last_task_(false),
    param_(),
    report_ctx_(),
    storage_info_(),
    ctx_(nullptr),
    lsn_range_(nullptr),
    validated_log_bytes_(0)
{
}

ObBackupValidateArchivePiecePhysicalTask::~ObBackupValidateArchivePiecePhysicalTask()
{
}

int ObBackupValidateArchivePiecePhysicalTask::init(
    const ObBackupValidateDagNetInitParam &param,
    const backup::ObBackupReportCtx &report_ctx,
    const share::ObBackupStorageInfo &storage_info,
    const int64_t lsn_group_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateArchivePiecePhysicalTask init twice", KR(ret));
  } else if (!param.is_valid() || !report_ctx.is_valid() || !storage_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(param), K(report_ctx), K(storage_info));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", KR(ret), K(param));
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    LOG_WARN("failed to assign storage info", KR(ret));
  } else if (FALSE_IT(report_ctx_ = report_ctx)) {
  } else {
    share::ObIDag *dag = nullptr;
    share::ObIDagNet *dag_net = nullptr;
    if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (OB_ISNULL(ctx_ = static_cast<ObBackupValidateArchivePiecePhysicalDag*>(dag)->get_task_context())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ctx is null", KR(ret));
    } else if (OB_FAIL(ctx_->get_archive_piece_lsn_range(lsn_group_id, lsn_range_, is_last_task_))) {
      LOG_WARN("failed to get archive piece lsn range", KR(ret), K(lsn_group_id), KPC(ctx_));
    } else if (OB_ISNULL(lsn_range_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lsn range is null", KR(ret), K(lsn_group_id));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupValidateArchivePiecePhysicalTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateArchivePiecePhysicalTask not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), KP_(ctx));
  } else if (OB_SUCCESS != ctx_->get_result()) {
    //other task has failed, skip validate
  } else if (OB_FAIL(do_validate_ls_range_())) {
    LOG_WARN("failed to validate ls range", KR(ret), K_(param), KPC_(lsn_range));
  }
  if (OB_SUCC(ret)) {
    int64_t current_group_id = lsn_range_->group_id_;
    bool update_checkpoint = false;
    int64_t validate_checkpoint = OB_INVALID_ID;
    int64_t delta_read_bytes = 0;
    int64_t total_read_bytes = 0;
    if (OB_FAIL(ctx_->remove_running_task_id(current_group_id, validated_log_bytes_, update_checkpoint,
                                              validate_checkpoint, delta_read_bytes, total_read_bytes))) {
      LOG_WARN("failed to remove running group", KR(ret), K(current_group_id), K(validated_log_bytes_));
    } else if (update_checkpoint) {
      common::ObMySQLTransaction trans;
      if (OB_FAIL(trans.start(report_ctx_.sql_proxy_, gen_meta_tenant_id(param_.tenant_id_)))) {
        LOG_WARN("failed to start transaction", KR(ret));
      } else if (OB_FAIL(ObBackupValidateLSTaskOperator::update_stats(trans, param_.task_id_,
                                                                        param_.tenant_id_, param_.ls_id_,
                                                                        total_read_bytes, validate_checkpoint))) {
        LOG_WARN("failed to update stats", K(ret), K_(param), K(validate_checkpoint), K(total_read_bytes));
      } else if (0 != delta_read_bytes && OB_FAIL(ObBackupValidateTaskOperator::update_validated_bytes(
                     trans, param_.task_id_, param_.tenant_id_, delta_read_bytes))) {
        LOG_WARN("failed to add validated bytes", KR(ret), K(param_.task_id_), K(param_.tenant_id_), K(delta_read_bytes));
      }
      if (trans.is_started()) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
          LOG_WARN("failed to end transaction", KR(ret), KR(tmp_ret));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to validate piece log entry", param_,
                                                    GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
  }
  return ret;
}

int ObBackupValidateArchivePiecePhysicalTask::do_validate_ls_range_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(lsn_range_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or lsn_range is null", KR(ret), KP_(ctx), KP_(lsn_range));
  } else {
    // build DirArray from validate path and storage info
    logservice::DirArray dirs;
    std::pair<share::ObBackupPathString, share::ObBackupPathString> dir_pair;
    char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {0};
    share::ObBackupPathString raw_path;
    share::SCN archive_scn;
    logservice::ObRemoteRawPathParent raw_parent(param_.ls_id_);
    if (OB_FAIL(storage_info_.get_storage_info_str(storage_info_str, sizeof(storage_info_str)))) {
      LOG_WARN("failed to get storage info str", KR(ret));
    } else if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path.ptr(), raw_path.capacity()))) {
      LOG_WARN("failed to get root path", KR(ret), K_(param));
    } else if (OB_FAIL(dir_pair.first.assign(raw_path))) {
      LOG_WARN("failed to assign root path", KR(ret), K(raw_path));
    } else if (OB_FAIL(dir_pair.second.assign(storage_info_str))) {
      LOG_WARN("failed to assign storage info str", KR(ret));
    } else if (OB_FAIL(dirs.push_back(dir_pair))) {
      LOG_WARN("failed to push back dir pair", KR(ret));
    } else if (OB_ISNULL(ctx_->get_ls_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls info is null", KR(ret));
    } else if (OB_FAIL(raw_parent.set(dirs, ctx_->get_ls_info()->checkpoint_scn_))) {
      LOG_WARN("failed to set raw parent", KR(ret));
    } else {
      GetSourceFunctor get_source_func(raw_parent);
      logservice::ObRemoteIGroupEntryIterator iter(get_source_func);
      archive::LargeBufferPool buffer_pool;
      logservice::ObLogExternalStorageHandler handler;
      ipalf::IGroupEntry entry;
      palf::LSN lsn;
      const char *buf = nullptr;
      int64_t buf_len = 0;
      const int64_t BUF_SIZE = palf::MAX_LOG_BUFFER_SIZE + palf::MAX_LOG_HEADER_SIZE;
      const bool enable_logservice = GCONF.enable_logservice;
      if (OB_FAIL(buffer_pool.init("TempReadArcPool", BUF_SIZE))) {
        LOG_WARN("failed to init buffer pool", KR(ret));
      } else if (OB_FAIL(handler.init())) {
        LOG_WARN("failed to init storage handler", KR(ret));
      } else if (OB_FAIL(handler.start(0/*default concurrency*/))) {
        LOG_WARN("failed to start storage handler", KR(ret));
      } else if (OB_FAIL(iter.init(MTL_ID(), param_.ls_id_, share::SCN::min_scn(), lsn_range_->start_lsn_,
                                      lsn_range_->end_lsn_, &buffer_pool, &handler,
                                      palf::MAX_LOG_BUFFER_SIZE, enable_logservice))) {
        LOG_WARN("failed to init remote log iter", KR(ret), KPC(lsn_range_));
      } else {
        const share::SCN checkpoint_scn = ctx_->get_ls_info()->checkpoint_scn_;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter.next(entry, lsn, buf, buf_len))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("iterate next failed", KR(ret));
            }
          } else {
            validated_log_bytes_ += buf_len;
          }
        }
        if (OB_FAIL(ret) || !is_last_task_) {
        } else if (FALSE_IT(archive_scn = entry.get_scn())) {
        } else if (archive_scn < checkpoint_scn) {
          ret = OB_LS_ARCHIVE_MAX_SCN_LESS_THAN_CHECKPOINT;
          LOG_WARN("max archive log scn is less than checkpoint scn", KR(ret), K(archive_scn), K(checkpoint_scn));
        }
      }
      iter.reset();
      handler.stop();
      handler.wait();
      handler.destroy();
      buffer_pool.destroy();
    }
  }
  return ret;
}

int ObBackupValidateArchivePiecePhysicalTask::generate_next_task(share::ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateArchivePiecePhysicalTask not init", KR(ret));
  } else {
    int64_t task_id = -1;
    ObIDag *dag = nullptr;
    ObBackupValidateArchivePiecePhysicalDag *physical_dag = nullptr;
    ObBackupValidateArchivePiecePhysicalTask *task = nullptr;
    if (OB_FAIL(ctx_->get_next_task_id(task_id))) {
      LOG_WARN("failed to get next task id", KR(ret));
    } else if (-1 == task_id) {
      ret = OB_ITER_END;
      next_task = nullptr;
    } else if (OB_ISNULL(dag = this->get_dag())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag is null", KR(ret));
    } else if (ObDagType::DAG_TYPE_BACKUP_VALIDATE != dag->get_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dag type is not backup validate dag", KR(ret), K(dag->get_type()));
    } else if (FALSE_IT(physical_dag = static_cast<ObBackupValidateArchivePiecePhysicalDag*>(dag))) {
    } else if (OB_FAIL(physical_dag->alloc_task(task))) {
      LOG_WARN("failed to alloc next task", KR(ret), K(task_id));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("task is null", KR(ret), KP(task));
    } else if (OB_FAIL(task->init(param_, report_ctx_, storage_info_, task_id))) {
      LOG_WARN("failed to init next task", KR(ret), K(task_id));
    } else {
      next_task = task;
      task = nullptr;
    }
  }
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    int tmp_ret = OB_SUCCESS;
    const share::ObTaskId dag_id = this->get_dag()->get_dag_id();
    if (OB_NOT_NULL(ctx_)) {
      if (OB_TMP_FAIL(ctx_->set_validate_result(ret, "failed to generate next piece log entry task", param_,
                                                    GCTX.self_addr(), dag_id, report_ctx_))) {
        LOG_WARN("failed to set validate result", KR(ret), KR(tmp_ret), K(dag_id), K_(param));
      }
    }
    LOG_ERROR("failed to generate next task", KR(ret), KR(tmp_ret), KPC_(lsn_range), K_(param));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
