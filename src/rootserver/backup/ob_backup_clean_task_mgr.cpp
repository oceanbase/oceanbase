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

#define USING_LOG_PREFIX RS
#include "ob_backup_clean_task_mgr.h"
#include "ob_backup_clean_ls_task_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/backup/ob_backup_data_store.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_clean_struct.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_clean_util.h"
#include "lib/utility/utility.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/ob_debug_sync_point.h"
#include "lib/utility/ob_tracepoint.h"

using namespace oceanbase;
using namespace rootserver;
using namespace share;


//****************************ObBackupCleanTaskMgr**********************
ObBackupCleanTaskMgr::ObBackupCleanTaskMgr()
 : is_inited_(false),
   tenant_id_(OB_INVALID_TENANT_ID),
   backup_set_info_(),
   backup_piece_info_(),
   task_attr_(),
   backup_dest_(),
   job_attr_(nullptr),
   sql_proxy_(nullptr),
   rpc_proxy_(nullptr),
   task_scheduler_(nullptr),
   backup_service_(nullptr)
{
}

int ObBackupCleanTaskMgr::init(
    const uint64_t tenant_id,
    const int64_t task_id,
    ObBackupCleanJobAttr &job_attr,
    common::ObISQLClient &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupCleanService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
    } else if (OB_FAIL(ObBackupCleanTaskOperator::get_backup_clean_task(sql_proxy, task_id, job_attr.tenant_id_, false/* need_lock */, task_attr_))) {
    LOG_WARN("failed to get backup task", K(ret), "job_id", job_attr.job_id_, "tenant_id", job_attr.tenant_id_);
  } else if (!task_attr_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_attr_));
  } else if (task_attr_.is_delete_backup_set_task()) {
    if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(sql_proxy, false/* need_lock */, task_attr_.backup_set_id_,
        task_attr_.incarnation_id_, task_attr_.tenant_id_, task_attr_.dest_id_, backup_set_info_))) {
      LOG_WARN("failed to get backup set file", K(ret), K(task_attr_));
    } else if (!backup_set_info_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(task_attr_));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(sql_proxy, job_attr.tenant_id_, backup_set_info_.backup_path_, backup_dest_))) {
      LOG_WARN("failed to get backup dest", K(ret), K(backup_set_info_));
    } 
  } else if (task_attr_.is_delete_backup_piece_task()) {
    ObArchivePersistHelper archive_table_op;
    if (OB_FAIL(archive_table_op.init(job_attr.tenant_id_))) {
      LOG_WARN("failed to init archive piece attr", K(ret));
    } else if (OB_FAIL(archive_table_op.get_piece(sql_proxy, task_attr_.dest_id_, task_attr_.round_id_,
        task_attr_.backup_piece_id_, false/* need_lock */, backup_piece_info_))) {
      LOG_WARN("failed to get backup piece", K(ret), K(task_attr_));  
    } else if(!backup_piece_info_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(task_attr_));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(sql_proxy, job_attr.tenant_id_, backup_piece_info_.path_, backup_dest_))) {
      LOG_WARN("failed to get backup dest", K(ret), K(backup_piece_info_));
    }
  }
  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    job_attr_ = &job_attr;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    task_scheduler_ = &task_scheduler;
    backup_service_ = &backup_service;
    is_inited_ = true;
  }
  // TODO(wenjinyu.wjy): integrate sql_proxy and lease_service 4.3
  return ret;
}

int ObBackupCleanTaskMgr::advance_task_status_(
    common::ObISQLClient &sql_proxy,
    const ObBackupCleanStatus &next_status, 
    const int result,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanTaskOperator::advance_task_status(sql_proxy, task_attr_, next_status, result, end_ts))) {
    LOG_WARN("failed to advance set status", K(ret), K(task_attr_), K(next_status));
  } 
  return ret;
}

int ObBackupCleanTaskMgr::mark_backup_set_files_deleting_()
{
  int ret = OB_SUCCESS;
  ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::STATUS::BACKUP_FILE_DELETING;
  if (!backup_set_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set info is valid", K(ret));
  } else if (backup_set_info_.file_status_ == file_status) {
    // do nothing
  } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(backup_set_info_.file_status_, file_status))) {
    LOG_WARN("failed to check can change status", K(ret));
  } else if (FALSE_IT(backup_set_info_.file_status_ = file_status)) {
  } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file_status(*sql_proxy_, backup_set_info_))) {
    LOG_WARN("failed to update backup set file_status", K(ret), K(file_status));
  }
  return ret;
}

int ObBackupCleanTaskMgr::mark_backup_set_files_deleted_()
{
  int ret = OB_SUCCESS;
  ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::STATUS::BACKUP_FILE_DELETED;
  if (!backup_set_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set info is valid", K(ret));
  } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(backup_set_info_.file_status_, file_status))) {
    LOG_WARN("failed to check can change status", K(ret));
  } else if (FALSE_IT(backup_set_info_.file_status_ = file_status)) {
  } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file_status(*sql_proxy_, backup_set_info_))) {
    LOG_WARN("failed to update backup set file", K(ret));
  } 
  return ret;
}

int ObBackupCleanTaskMgr::mark_backup_piece_files_deleting_()
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper archive_table_op;
  ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::STATUS::BACKUP_FILE_DELETING;
  if (!backup_piece_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup piece info is valid", K(ret));
  } else if (backup_piece_info_.file_status_ == file_status) {
    // do nothing
  } else if (OB_FAIL(archive_table_op.init(task_attr_.tenant_id_))) {
    LOG_WARN("failed to init archive piece attr", K(ret));
  } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(backup_piece_info_.file_status_, file_status))) {
    LOG_WARN("failed to check can change status", K(ret));
  } else if (OB_FAIL(archive_table_op.mark_new_piece_file_status(*sql_proxy_, backup_piece_info_.key_.dest_id_,
    backup_piece_info_.key_.round_id_, backup_piece_info_.key_.piece_id_, file_status))) {
    LOG_WARN("failed to update backup piece file status", K(ret));
  }
  return ret;
}

int ObBackupCleanTaskMgr::mark_backup_piece_files_deleted_()
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper archive_table_op;
  ObBackupFileStatus::STATUS file_status = ObBackupFileStatus::STATUS::BACKUP_FILE_DELETED;
  if (!backup_piece_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup piece info is valid", K(ret));
  } else if (OB_FAIL(archive_table_op.init(task_attr_.tenant_id_))) {
    LOG_WARN("failed to init archive piece attr", K(ret)); 
  } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(backup_piece_info_.file_status_, file_status))) {
    LOG_WARN("failed to check can change status", K(ret));
  } else if (FALSE_IT(backup_piece_info_.file_status_ = file_status)) {
  } else if (OB_FAIL(archive_table_op.mark_new_piece_file_status(*sql_proxy_, backup_piece_info_.key_.dest_id_,
    backup_piece_info_.key_.round_id_, backup_piece_info_.key_.piece_id_, file_status))) {
    LOG_WARN("failed to update backup piece file status", K(ret));
  }
  return ret;
}

int ObBackupCleanTaskMgr::mark_backup_files_deleted_() 
{
  int ret = OB_SUCCESS;
  if (task_attr_.is_delete_backup_set_task()) {
    if (OB_FAIL(mark_backup_set_files_deleted_())) {
      LOG_WARN("failed to mark backup set files deleted", K(ret));
    }
  } else if (task_attr_.is_delete_backup_piece_task()) {
    if (OB_FAIL(mark_backup_piece_files_deleted_())) {
      LOG_WARN("failed to mark backup piece files deleted", K(ret));
    }
  }
  return ret; 
}

int ObBackupCleanTaskMgr::mark_backup_files_deleting_()
{
  int ret = OB_SUCCESS;
  if (task_attr_.is_delete_backup_set_task()) {
    if (OB_FAIL(mark_backup_set_files_deleting_())) {
      LOG_WARN("failed to mark backup set files deleting", K(ret), K_(backup_set_info));
    }
  } else if (task_attr_.is_delete_backup_piece_task()) {
    if (OB_FAIL(mark_backup_piece_files_deleting_())) {
      LOG_WARN("failed to mark backup piece files deleting", K(ret), K_(backup_piece_info));
    }
  }
  return ret; 
}
// file:///backup/backup_sets/backup_set_xx_xxx_xxxx_end_xxx.obbak
int ObBackupCleanTaskMgr::try_delete_extern_set_end_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath end_path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (!backup_set_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set info is valid", K(ret)); 
  } else if (OB_SUCCESS == backup_set_info_.result_) {
    if (OB_FAIL(ObBackupPathUtil::get_backup_set_placeholder_end_success_path(
        backup_dest_, desc, backup_set_info_.min_restore_scn_, end_path))) {
      LOG_WARN("failed to get backup set end placeholder path", K(ret));
    }  
  } else {
    if (OB_FAIL(ObBackupPathUtil::get_backup_set_placeholder_end_failed_path(
        backup_dest_, desc, backup_set_info_.min_restore_scn_, end_path))) {
      LOG_WARN("failed to get backup set end placeholder path", K(ret));
    }  
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_file(end_path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_));
  }
  return ret;
}

int ObBackupCleanTaskMgr::try_delete_extern_piece_end_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath end_path;
  if (!backup_piece_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup piece info is invalid", K(ret), K_(backup_piece_info)); 
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_end_file_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, backup_piece_info_.checkpoint_scn_, end_path))) {
    LOG_WARN("failed to get backup set end placeholder path", K(ret), K_(backup_piece_info));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_file(end_path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K_(task_attr), K(end_path));
  }
  return ret;
}

int ObBackupCleanTaskMgr::try_delete_extern_end_file_()
{
  int ret = OB_SUCCESS;
  if (task_attr_.is_delete_backup_set_task()) {
    if (OB_FAIL(try_delete_extern_set_end_file_())) {
      LOG_WARN("failed to try delete extern set end file", K(ret));
    }
  } else if (task_attr_.is_delete_backup_piece_task()) {
    if (OB_FAIL(try_delete_extern_piece_end_file_())) {
      LOG_WARN("failed to try delete extern piece end file", K(ret));
    }
  }
  return ret; 
}

bool ObBackupCleanTaskMgr::is_can_retry_(const int error)
{
  return job_attr_->can_retry_ && job_attr_->retry_count_ < OB_MAX_RETRY_TIMES && ObBackupUtils::is_need_retry_error(error);
}

int ObBackupCleanTaskMgr::prepare_backup_clean_task_()
{
  int ret = OB_SUCCESS;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_DELETE_MARK_DELETING) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) { 
  } else if (OB_FAIL(mark_backup_files_deleting_())) {
    LOG_WARN("failed to mark backup file deleting", K(ret));
  } else if (OB_FAIL(try_delete_extern_end_file_())) {
    LOG_WARN("failed to try delete extern end file", K(ret));
  } else if (OB_FAIL(persist_ls_tasks_())) {
    LOG_WARN("failed to persist log stream task", K(ret), K(task_attr_));
  }
  return ret;
}

int ObBackupCleanTaskMgr::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_CLEAN]schedule backup clean set task", K(task_attr_));
  
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant backup data scheduler not init", K(ret));
  } else {
    ObBackupCleanStatus::Status status = task_attr_.status_.status_;
    switch (status) {
      case ObBackupCleanStatus::Status::INIT: {
        if (OB_FAIL(prepare_backup_clean_task_())) {
          LOG_WARN("failed to persist backup clean ls task", K(ret), K(task_attr_));
        }
        break;
      } 
      case ObBackupCleanStatus::Status::DOING: {
        DEBUG_SYNC(BACKUP_DELETE_TASK_STATUS_DOING);
        if (OB_FAIL(backup_clean_ls_tasks_())) {
          LOG_WARN("failed to backup clean ls task", K(ret), K(task_attr_));
        }
        break;
      }
      case ObBackupCleanStatus::Status::COMPLETED: 
      case ObBackupCleanStatus::Status::FAILED: 
      case ObBackupCleanStatus::Status::CANCELED: {
        if (OB_FAIL(do_cleanup())) {
          LOG_WARN("failed to move history", K(ret), K(task_attr_));
        }
        break;
      }
      case ObBackupCleanStatus::Status::CANCELING: {
        if (OB_FAIL(do_cancel_())) {
          LOG_WARN("failed to cancel backup", K(ret), K(task_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("unknown backup clean status", K(ret), K(task_attr_));
      }
    }
    if (OB_FAIL(ret) && !is_can_retry_(ret)) {
      if (ObBackupCleanStatus::Status::INIT == task_attr_.status_.status_
          || ObBackupCleanStatus::Status::DOING == task_attr_.status_.status_) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = deal_failed_task(ret))) {
          LOG_WARN("failed to advance task status", K(tmp_ret), K(ret), K(task_attr_.status_));
        }
        ret = (OB_SUCCESS != tmp_ret) ? tmp_ret : ret; 
      }
    }
  }
  
  return ret;
}

int ObBackupCleanTaskMgr::persist_ls_tasks_()
{ 
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_ids;
  ObBackupCleanStatus next_status;
  ObMySQLTransaction trans;
  next_status.status_ = ObBackupCleanStatus::Status::DOING;
  if (OB_FAIL(get_ls_ids_(ls_ids))) {
    LOG_WARN("failed to get ls ids", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (0 != ls_ids.count()) {
      ObBackupCleanLSTaskAttr new_ls_attr;
      for (int i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
        const ObLSID &ls_id = ls_ids.at(i);
        new_ls_attr.reset(); 
        if (OB_FAIL(generate_ls_task_(ls_id, new_ls_attr))) {
          LOG_WARN("failed to generate log stream tasks", K(ret), K(ls_ids));
        } else if (OB_FAIL(backup_service_->check_leader())) {
          LOG_WARN("failed to check leader", K(ret));
        } else if (OB_FAIL(ObBackupCleanLSTaskOperator::insert_ls_task(trans, new_ls_attr))) {
          LOG_WARN("failed to insert backup log stream task", K(ret), K(new_ls_attr));
        } 
      }
    }
    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(task_attr_.total_ls_count_ = ls_ids.count())) {
    } else if (OB_FAIL(advance_task_status_(trans, next_status, task_attr_.result_, task_attr_.end_ts_))) {
      LOG_WARN("failed to advance status to DOING", K(ret), K(ls_ids));
    } else if (OB_FAIL(ObBackupCleanTaskOperator::update_ls_count(trans, task_attr_, true/*is_total*/))) {
      LOG_WARN("failed to update ls task count", K(ret), K(task_attr_.total_ls_count_), K(ls_ids)); 
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back status", K(ret), K(tmp_ret));
      } 
    }
  }
  return ret;
}

int ObBackupCleanTaskMgr::get_set_ls_ids_from_traverse_(ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS; 
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = task_attr_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(
      backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant data backup set dir path", K(ret), K_(backup_set_info));  
  } else if (OB_FAIL(get_ls_ids_from_traverse_(path, ls_ids))) {
    LOG_WARN("failed to get set ls ids from traverse", K(ret), K(path)); 
  } else {
    LOG_INFO("[BACKUP_CLEAN]success get set ls ids from traverse", K(path), K(ls_ids));  
  }
  return ret;
}

int ObBackupCleanTaskMgr::get_set_ls_ids_(ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  share::ObBackupSetDesc desc;
  ObBackupDataLSAttrDesc ls_attr;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(store.init(backup_dest_, desc))) {
    LOG_WARN("faield to init backup data extern mgr", K(ret));
  } else if (OB_FAIL(store.read_ls_attr_info(ls_attr))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      if (OB_FAIL(get_set_ls_ids_from_traverse_(ls_ids))) {
        LOG_WARN("failed to get set ls ids from traverse", K(ret));
      }
    } else {
      LOG_WARN("failed to read log stream info", K(ret));
    }
  } else if (!ls_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls info", K(ret), K(ls_attr));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ls_attr.ls_attr_array_.count(); ++i) {
      const ObLSID &ls_id = ls_attr.ls_attr_array_.at(i).get_ls_id();
      if (!ls_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls info", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
        LOG_WARN("failed to push back ls_id", K(ret), K(ls_id));
      } 
    }
  }

  return ret;
}

int ObBackupCleanTaskMgr::get_piece_ls_ids_from_traverse_(ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS; 
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, path))) {
    LOG_WARN("failed to get piece dir path", K(ret), K_(backup_piece_info));  
  } else if (OB_FAIL(get_ls_ids_from_traverse_(path, ls_ids))) {
    LOG_WARN("failed to get ls ids from traverse", K(ret), K(path)); 
  }
  return ret;
}

int ObBackupCleanTaskMgr::get_piece_ls_ids_(ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObArchiveStore store;
  ObPieceInfoDesc desc;
  if (OB_FAIL(store.init(backup_dest_))) {
    LOG_WARN("failed to init store", K(ret), K_(task_attr));
  } else if (OB_FAIL(store.read_piece_info(backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      if (OB_FAIL(get_piece_ls_ids_from_traverse_(ls_ids))) {
        LOG_WARN("failed to get piece ls ids from traverse", K(ret));
      }
    } else {
      LOG_WARN("failed to read piece info", K(ret), K_(task_attr), K(backup_piece_info_)); 
    }
  } else if (!desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls info", K(ret), K(desc));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < desc.filelist_.count(); ++i) {
      const ObLSID &ls_id = desc.filelist_.at(i).ls_id_;
      if (!ls_id.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls info", K(ret), K(ls_id));
      } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
        LOG_WARN("failed to push back ls_id", K(ret), K(ls_id));
      } 
    }
  }

  return ret;
}

int ObBackupCleanTaskMgr::get_ls_ids_(ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  if (task_attr_.is_delete_backup_set_task()) {
    if (OB_FAIL(get_set_ls_ids_(ls_ids))) {
      LOG_WARN("failed to get set ls ids", K(ret), K_(task_attr), K_(backup_set_info));
    }
  } else if (task_attr_.is_delete_backup_piece_task()) {
    if (OB_FAIL(get_piece_ls_ids_(ls_ids))) {
      LOG_WARN("failed to piece ls ids", K(ret), K_(task_attr), K_(backup_piece_info));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clean task type is invalid", K(ret), K_(task_attr)); 
  }
  return ret;
}

int ObBackupCleanTaskMgr::parse_int_(const char *str, int64_t &val)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  val = 0;
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(str));
  } else {
    char *p_end = NULL;
    if ('\0' == str[0]) {
      is_valid = false;
    } else if (OB_FAIL(ob_strtoll(str, p_end, val))) {
      LOG_WARN("failed to get value from string", K(ret), K(str));
    } else if ('\0' == *p_end) {
      is_valid = true;
    } else {
      is_valid = false;
      LOG_WARN("set int error", K(str), K(is_valid));
    }

    if (!is_valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid int str", K(ret), K(str));
    }
  }

  return ret;
}

int ObBackupCleanTaskMgr::parse_ls_id_(const char *dir_name, int64_t &id_val)
{
  int ret = OB_SUCCESS;
  id_val = 0;
  const char *LOGSTREAM = "logstream";
  char *token = NULL;
  char *saved_ptr = NULL;
  char tmp_name[OB_MAX_URI_LENGTH] = { 0 };
  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(dir_name));
  } else if (OB_FAIL(databuff_printf(tmp_name, sizeof(tmp_name), "%s", dir_name))) {
    LOG_WARN("failed to set dir name", K(ret), KP(dir_name));
  } else {
    token = tmp_name;
    for (char *str = token; OB_SUCC(ret); str = NULL) {
      token = ::strtok_r(str, "_", &saved_ptr);
      if (NULL == token) {
        break;
      } else if (0 == strncmp(LOGSTREAM, token, strlen(LOGSTREAM))) {
        continue;
      } else if (OB_FAIL(parse_int_(token, id_val))){
        LOG_WARN("invalid number", K(token), K(id_val)); 
      }
    }
  }

  return ret;
}

int ObBackupCleanTaskMgr::get_ls_ids_from_traverse_(const ObBackupPath &path, ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  char logstream_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };
  if (OB_FAIL(databuff_printf(logstream_prefix, sizeof(logstream_prefix), "%s", "logstream_"))) {
    LOG_WARN("failed to set log stream prefix", K(ret));  
  } else if (OB_FAIL(prefix_op.init(logstream_prefix, strlen(logstream_prefix)))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(logstream_prefix));
  // TODO(wenjinyu.wjy) iterate dir sequentially 4.3
  } else if (OB_FAIL(util.list_directories(path.get_obstr(), backup_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list files", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObLSID ls_id; 
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      int64_t id_val = 0;
      tmp_entry = d_entrys.at(i);
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(parse_ls_id_(tmp_entry.name_, id_val))) {
        LOG_WARN("failed to parse ls id", K(ret));
      } else if (FALSE_IT(ls_id = id_val)) {
      } else if (OB_FAIL(ls_ids.push_back(ls_id))) {
        LOG_WARN("failed to push back ls_ids", K(ret));
      }
    }
  }
  return ret;
}


int ObBackupCleanTaskMgr::generate_ls_task_(const share::ObLSID &ls_id, share::ObBackupCleanLSTaskAttr &new_ls_attr)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls id is invalid", K(ret), K(ls_id));
  } else {
    new_ls_attr.task_id_ = task_attr_.task_id_;
    new_ls_attr.tenant_id_ = task_attr_.tenant_id_;
    new_ls_attr.ls_id_ = ls_id;
    new_ls_attr.job_id_ = task_attr_.job_id_;
    new_ls_attr.backup_set_id_ = task_attr_.backup_set_id_;
    new_ls_attr.backup_piece_id_ = task_attr_.backup_piece_id_;
    new_ls_attr.round_id_ = backup_piece_info_.key_.round_id_ != 0 ? backup_piece_info_.key_.round_id_ : 0; 
    new_ls_attr.task_type_ = task_attr_.task_type_;
    new_ls_attr.status_.status_ = ObBackupTaskStatus::Status::INIT;
    const int64_t current_time = ObTimeUtility::current_time();
    new_ls_attr.start_ts_ = current_time;
    new_ls_attr.end_ts_ = 0; 
    new_ls_attr.result_ = OB_SUCCESS;
  }
  return ret;
}
// file:///obbackup/backup_set_1_full/infos/locality_info.obbak
int ObBackupCleanTaskMgr::delete_tenant_locality_info_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_locality_info_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant locality path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/diagnose_info.obbak
int ObBackupCleanTaskMgr::delete_tenant_diagnose_info_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_diagnose_info_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant locality path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/meta_info/
int ObBackupCleanTaskMgr::delete_meta_info_dir_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_tenant_meta_info_dir_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant locality path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_dir_files(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret; 
}

// file:///obbackup/backup_set_1_full/infos/major_data_info_turn_X/
int ObBackupCleanTaskMgr::delete_major_data_info_dir_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  share::ObBackupDataType backup_data_type;
  backup_data_type.set_major_data_backup();
  if (OB_FAIL(ObBackupPathUtil::get_ls_info_data_info_dir_path(backup_dest_, desc, backup_data_type,
      backup_set_info_.major_turn_id_, path))) {
    LOG_WARN("failed to get ls info data info", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_dir_files(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/infos/minor_data_info_turn_X/
int ObBackupCleanTaskMgr::delete_minor_data_info_dir_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  share::ObBackupDataType backup_data_type;
  backup_data_type.set_minor_data_backup();
  if (OB_FAIL(ObBackupPathUtil::get_ls_info_data_info_dir_path(backup_dest_, desc,
      backup_data_type, backup_set_info_.minor_turn_id_, path))) {
    LOG_WARN("failed to get ls info data info path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_dir_files(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/single_backup_set_info.obbak
int ObBackupCleanTaskMgr::delete_single_backup_set_info_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_info_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant locality path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

int ObBackupCleanTaskMgr::delete_data_info_turn_files_(const ObBackupPath &infos_path)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  const char info_turn_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = "data_info_turn_";
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  if (OB_FAIL(prefix_op.init(info_turn_prefix, strlen(info_turn_prefix)))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(info_turn_prefix));
  // TODO(wenjinyu.wjy) iterate dir sequentially 4.3
  } else if (OB_FAIL(util.list_directories(infos_path.get_obstr(), backup_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list directories", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObBackupPath info_path; 
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      ObIODirentEntry tmp_entry = d_entrys.at(i);
      info_path.reset();
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(info_path.init(infos_path.get_ptr()))) {
        LOG_WARN("failed to init major path", K(ret), K(info_path));
      } else if (OB_FAIL(info_path.join(tmp_entry.name_, ObBackupFileSuffix::NONE))) {
        LOG_WARN("failed to join major path", K(ret));
      } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(info_path, backup_dest_.get_storage_info()))) {
        LOG_WARN("failed to delete backup log stream dir files", K(ret), K(info_path));
      } 
    }
  }
  return ret;
}

int ObBackupCleanTaskMgr::delete_backup_dir_(const share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete info turn files", K(ret), K(path));
  }
  return ret;
}

// file:///obbackup/backup_set_1_full/
int ObBackupCleanTaskMgr::delete_backup_set_dir_()
{
  int ret = OB_SUCCESS;
  ObBackupPath set_path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_dest_, desc, set_path))) {
    LOG_WARN("failed to get tenant data backup set dir", K(ret), K_(backup_dest), K_(task_attr));
  } else if (OB_FAIL(delete_backup_dir_(set_path))) {
    LOG_WARN("failed to delete backup dir", K(ret), K(set_path));
  }
  return ret;
}

// file:///backup/backup_sets/backup_set_xx_xxx_start
int ObBackupCleanTaskMgr::delete_backup_set_start_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath start_path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_placeholder_start_path(backup_dest_, desc, start_path))) {
    LOG_WARN("failed to get backup set end placeholder path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(start_path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(start_path));
  } 
  return ret;
}

// file:///obbackup/backup_set_1_full/backup_set_1_full_xxxx_xxxxx
int ObBackupCleanTaskMgr::delete_backup_set_inner_placeholder_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  char placeholder_prefix[OB_MAX_BACKUP_CHECK_FILE_NAME_LENGTH] = { 0 };
  ObBackupIoAdapter util;
  ObBackupPrefixDeleteFileOp prefix_delete_op;
  if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant data backup set dir", K(ret), K_(backup_dest));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_set_inner_placeholder_prefix(desc, placeholder_prefix, sizeof(placeholder_prefix)))) {
    LOG_WARN("failed to get backup set inner placeholder prefix", K(ret));
  } else if (OB_FAIL(prefix_delete_op.init(placeholder_prefix, strlen(placeholder_prefix), path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to init prefix delete", K(ret), K(placeholder_prefix));
  } else if (OB_FAIL(util.list_files(path.get_obstr(), backup_dest_.get_storage_info(), prefix_delete_op))) {
    LOG_WARN("failed to list files", K(ret), K(path), K(placeholder_prefix));
  }
  return ret;
}

// oss://backup/backup_set_xx_xxx_xxxxx/infos/tenant_backup_set_infos
int ObBackupCleanTaskMgr::delete_tenant_backup_set_infos_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_; 
  if (OB_FAIL(ObBackupPathUtil::get_tenant_backup_set_infos_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get backup set end placeholder path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  }
  return ret;
}

int ObBackupCleanTaskMgr::delete_backup_set_meta_info_files_()
{
  int ret = OB_SUCCESS;
  ObBackupPath infos_path;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_info_.backup_set_id_;
  desc.backup_type_ = backup_set_info_.backup_type_;
  if (!backup_set_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup set info is valid", K(ret)); 
  } else if (OB_FAIL(delete_meta_info_dir_())) { // first deleted meta file on infos dir
    LOG_WARN("failed to delete ls attr info file", K(ret));
  } else if (OB_FAIL(delete_major_data_info_dir_())) {
    LOG_WARN("failed to delete major data info file", K(ret));
  } else if (OB_FAIL(delete_minor_data_info_dir_())) {
    LOG_WARN("failed to delete minor data info file", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_ls_info_dir_path(backup_dest_, desc, infos_path))) {
    LOG_WARN("failed to get backup log stream info path", K(ret));
  } else if (OB_FAIL(delete_data_info_turn_files_(infos_path))) {
    LOG_WARN("failed to delete tenant info turn ", K(ret));
  } else if (OB_FAIL(delete_tenant_locality_info_())) {
    LOG_WARN("failed to delete tenant locality info", K(ret)); 
  } else if (OB_FAIL(delete_tenant_diagnose_info_())) {
    LOG_WARN("failed to delete tenant diagnose info", K(ret));
  } else if (OB_FAIL(delete_backup_dir_(infos_path))) {
    LOG_WARN("failed to delete backup infos dir", K(ret));
  } else if (OB_FAIL(delete_single_backup_set_info_())) {
    LOG_WARN("failed to delete backup set file", K(ret));
  } else if (OB_FAIL(delete_tenant_backup_set_infos_())) {
    LOG_WARN("failed to delete tenant backup set infos", K(ret)); 
  } else if (OB_FAIL(delete_backup_set_inner_placeholder_())) {
    LOG_WARN("failed to delete backup set inner placeholder", K(ret)); 
  } else if (OB_FAIL(delete_backup_set_dir_())) {
    LOG_WARN("failed to delete backup set dir", K(ret)); 
  } else if (OB_FAIL(delete_backup_set_start_file_())) {
    LOG_WARN("failed to delete backup set start file", K(ret)); 
  }
  return ret;
}

// oss://archive/pieces/piece_[dest_id]_[round_id]_[piece_id]_[create_timestamp]_start
int ObBackupCleanTaskMgr::delete_backup_piece_start_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_piece_start_file_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, backup_piece_info_.start_scn_, path))) {
    LOG_WARN("failed to get piece start file path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

// oss://archive/[dest_id]_[round_id]_[piece_id]/single_piece_info.obarc
int ObBackupCleanTaskMgr::delete_single_piece_info_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_single_piece_file_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, path))) {
    LOG_WARN("failed to get single piece file path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

// oss://archive/piece_d[dest_id]r[round_id]p[piece_id]/tenant_archive_piece_infos.obarc
int ObBackupCleanTaskMgr::delete_tenant_archive_piece_infos()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_tenant_archive_piece_infos_file_path(backup_dest_, backup_piece_info_.key_.dest_id_,
      backup_piece_info_.key_.round_id_, backup_piece_info_.key_.piece_id_, path))) {
    LOG_WARN("failed to get tenant archive piece infos path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  }
  return ret;
}

// oss://archive/[dest_id]_[round_id]_[piece_id]/file_info.obarc
int ObBackupCleanTaskMgr::delete_piece_info_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_piece_info_file_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, path))) {
    LOG_WARN("failed to get piece info file path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

// oss://archive/[dest_id]_[round_id]_[piece_id]/piece_[dest_id]_[round_id]_[piece_id]_[start_ts]_[end_ts]
int ObBackupCleanTaskMgr::delete_piece_inner_placeholder_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_piece_inner_placeholder_file_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, backup_piece_info_.start_scn_, backup_piece_info_.checkpoint_scn_, path))) {
    LOG_WARN("failed to get piece inner placeholder file path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_file(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  } 
  return ret;
}

// oss://archive/[dest_id]_[round_id]_[piece_id]/checkpoint
int ObBackupCleanTaskMgr::delete_piece_checkpoint_file_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (OB_FAIL(ObArchivePathUtil::get_piece_checkpoint_dir_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, path))) {
    LOG_WARN("failed to get piece checkpoint dir path", K(ret));
  } else if (OB_FAIL(share::ObBackupCleanUtil::delete_backup_dir_files(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup file", K(ret), K(task_attr_), K(path));
  }
  return ret;
}

int ObBackupCleanTaskMgr::delete_backup_piece_meta_info_files_()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (!backup_piece_info_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup piece info is valid", K(ret));
  } else if (OB_FAIL(delete_single_piece_info_())) {
    LOG_WARN("failed to delete single piece info file", K(ret), K(backup_piece_info_));
  } else if (OB_FAIL(delete_piece_info_file_())) {
    LOG_WARN("failed to delete info file", K(ret), K(backup_piece_info_));
  } else if (OB_FAIL(delete_tenant_archive_piece_infos())) {
    LOG_WARN("failed to delete info file", K(ret), K(backup_piece_info_));
  } else if (OB_FAIL(delete_piece_inner_placeholder_file_())) {
    LOG_WARN("failed to delete piece inner placeholder file", K(ret), K(backup_piece_info_));
  } else if (OB_FAIL(delete_piece_checkpoint_file_())) {
    LOG_WARN("failed to delete checkpoint file", K(ret), K(backup_piece_info_));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(
      backup_dest_, backup_piece_info_.key_.dest_id_, backup_piece_info_.key_.round_id_,
      backup_piece_info_.key_.piece_id_, path))) {
    LOG_WARN("failed to get tenant backup piece dir path", K(ret));  
  } else if (OB_FAIL(delete_backup_dir_(path))) {
    LOG_WARN("failed to delete backup infos dir", K(ret));
  } else if (OB_FAIL(delete_backup_piece_start_file_())) {
    LOG_WARN("failed to delete backup set start file", K(ret)); 
  }
  return ret;
}

int ObBackupCleanTaskMgr::delete_backup_meta_info_files_()
{
  int ret = OB_SUCCESS;
  if (task_attr_.is_delete_backup_set_task()) {
    if (OB_FAIL(delete_backup_set_meta_info_files_())) {
      LOG_WARN("failed to delete set meta info files", K(ret));
    }
  } else if (task_attr_.is_delete_backup_piece_task()) {
    if (OB_FAIL(delete_backup_piece_meta_info_files_())) {
      LOG_WARN("failed to delete piece meta info files", K(ret));
    }
  } 
  return ret;
}

int ObBackupCleanTaskMgr::backup_clean_ls_tasks_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupCleanLSTaskAttr> ls_tasks;
  int64_t finish_cnt = 0;
  bool need_do_ls_task = true;
  int tmp_ret = OB_SUCCESS;
  ObBackupCleanStatus next_status;
  if (OB_FAIL(ObBackupCleanLSTaskOperator::get_ls_tasks_from_task_id(*sql_proxy_, task_attr_.task_id_, task_attr_.tenant_id_, false/*update*/, ls_tasks))) {
    LOG_WARN("failed to get log stream tasks", K(ret));
  } else if (ls_tasks.count() != task_attr_.total_ls_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls task count error", K(ret), K(ls_tasks.count()), K(task_attr_.total_ls_count_)); 
  } else if (ls_tasks.empty() && task_attr_.total_ls_count_ == 0) {
    // do nothing, allow no log stream tasks
  } else if (OB_FAIL(do_backup_clean_ls_tasks_(ls_tasks, finish_cnt))) {
    LOG_WARN("failed to do backup ls task", K(ret), K_(task_attr));
  }
  if (OB_FAIL(ret)) {
  } else if (ls_tasks.count() == finish_cnt) {
    if (OB_FAIL(delete_backup_meta_info_files_())) {
      LOG_WARN("failed to delete meta info files ", K(ret)); 
    } else if (OB_FAIL(mark_backup_files_deleted_())) {
      LOG_WARN("failed to mark backup files deleted", K(ret)); 
    }

    if (OB_FAIL(ret)) {
    } else {
      task_attr_.end_ts_ = ObTimeUtil::current_time();
      task_attr_.status_.status_ = ObBackupCleanStatus::Status::COMPLETED;
      if (OB_FAIL(advance_task_status_(*sql_proxy_, task_attr_.status_, OB_SUCCESS, task_attr_.end_ts_))) {
        LOG_WARN("failed to advance task status", K(ret), K_(task_attr));
      }
    }
  }
  task_attr_.finish_ls_count_ = finish_cnt;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObBackupCleanTaskOperator::update_ls_count(*sql_proxy_, task_attr_, false/*is_total*/))) {
    LOG_WARN("failed to update finish ls count", K(ret), K_(task_attr));
  }

  return ret;
}

int ObBackupCleanTaskMgr::do_backup_clean_ls_tasks_(
    ObArray<ObBackupCleanLSTaskAttr> &ls_tasks, 
    int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  finish_cnt = 0;
  int retry_count = 0;
  if (ls_tasks.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_tasks));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
      ObBackupCleanLSTaskAttr &ls_attr = ls_tasks.at(i);
      ObBackupCleanLSTaskMgr ls_task_mgr;
#ifdef ERRSIM
      const int64_t OB_ERRSIM_BACKUP_MAX_RETRY_TIMES = 4;
      if (ls_attr.retry_id_ >= OB_ERRSIM_BACKUP_MAX_RETRY_TIMES && OB_SUCCESS != ls_attr.result_) {
#else
      if (ls_attr.retry_id_ >= OB_BACKUP_MAX_RETRY_TIMES && OB_SUCCESS != ls_attr.result_) {
#endif
        job_attr_->can_retry_ = false;
        ret = ls_attr.result_;
        LOG_WARN("retry times exceeds the limit", K(ret), K(ls_attr.retry_id_), K(ls_attr.ls_id_), K(task_attr_), K(job_attr_->can_retry_));
      } else if (OB_FAIL(ls_task_mgr.init(task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *backup_service_))) {
        LOG_WARN("failed to init task advancer", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("failed to process log stream task", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanTaskMgr::deal_failed_task(const int error)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupCleanLSTaskAttr> ls_tasks;
  int result = OB_SUCCESS;
  ObMySQLTransaction trans;
  int64_t success_ls_count = 0;
  const bool for_update = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupCleanTaskMgr not init", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(ObBackupCleanLSTaskOperator::get_ls_tasks_from_task_id(trans,
      task_attr_.task_id_, task_attr_.tenant_id_, for_update, ls_tasks))) {
    LOG_WARN("failed to get log stream tasks", K(ret));
  } else {
    if (OB_FAIL(do_failed_ls_task_(trans, ls_tasks, success_ls_count, result))) {
      LOG_WARN("failed to do failed backup task", K(ret), K(ls_tasks));
    }
    result = (OB_SUCCESS == result) ? error : result;
    if (OB_SUCC(ret)) {
      ObBackupCleanStatus next_status;
      if (OB_SUCCESS == result) {
        next_status.status_ = ObBackupCleanStatus::Status::CANCELED;
        result = OB_CANCELED;
      } else {
        next_status.status_ = ObBackupCleanStatus::Status::FAILED;
      }
      int64_t end_ts = ObTimeUtil::current_time();
      task_attr_.finish_ls_count_ = success_ls_count;
      if (OB_FAIL(advance_task_status_(trans, next_status, result, end_ts))) {
        LOG_WARN("failed to advance task status to finish", K(ret), K_(task_attr));
      } else if (OB_FAIL(ObBackupCleanTaskOperator::update_ls_count(trans, task_attr_, false/*is_total*/))) {
        LOG_WARN("failed to update finish ls count", K(ret), K_(task_attr));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back status", K(ret), K(tmp_ret));
      } 
    }
  }

  return ret;
}

int ObBackupCleanTaskMgr::do_failed_ls_task_(
    common::ObISQLClient &sql_proxy,
    ObIArray<ObBackupCleanLSTaskAttr> &ls_tasks,
    int64_t &success_ls_count,
    int &result) 
{
  int ret = OB_SUCCESS;
  int64_t finish_cnt = 0;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::FINISH;
  result = OB_SUCCESS;
  if (ls_tasks.empty()) {
    LOG_INFO("ls tasks is empty", K(ls_tasks)); 
    // do nothing
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
      ObBackupCleanLSTaskAttr &ls_attr = ls_tasks.at(i);
      ObBackupCleanLSTaskMgr ls_task_mgr;
      if (ObBackupTaskStatus::Status::FINISH == ls_attr.status_.status_) {
        if (OB_SUCCESS == ls_attr.result_) {
          success_ls_count++;
        } else if (OB_SUCCESS == result) {
          result = ls_attr.result_; 
        }
      } else if (OB_FAIL(ls_task_mgr.init(task_attr_, ls_attr, *task_scheduler_, sql_proxy, *backup_service_))) {
        LOG_WARN("failed to init task mgr", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.cancel(finish_cnt))) {
        LOG_WARN("failed to cancel task", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupCleanTaskMgr::do_cleanup()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupCleanTaskMgr not init", K(ret));
  } else if (OB_FAIL(move_task_to_history_())) {
    LOG_WARN("failed to move task to his", K(ret), K_(task_attr));
  }
  return ret;
}

int ObBackupCleanTaskMgr::move_task_to_history_() 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupCleanLSTaskOperator::move_ls_to_his(*sql_proxy_, task_attr_.tenant_id_, task_attr_.task_id_))) {
    LOG_WARN("failed to move ls task to history", K(ret));
  } else if (OB_FAIL(ObBackupCleanTaskOperator::move_task_to_history(*sql_proxy_, task_attr_.tenant_id_, task_attr_.task_id_))) {
    LOG_WARN("failed to move task to history", K(ret)); 
  }
  return ret;
}

int ObBackupCleanTaskMgr::do_cancel_()
{
  int ret = OB_SUCCESS;
  int64_t finish_cnt = 0;
  int64_t success_ls_count = 0;
  ObArray<ObBackupCleanLSTaskAttr> ls_task;
  ObMySQLTransaction trans;
  const bool for_update = true;
  if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(ObBackupCleanLSTaskOperator::get_ls_tasks_from_task_id(trans, task_attr_.task_id_, job_attr_->tenant_id_, for_update, ls_task))) {
      LOG_WARN("failed to get log stream tasks", K(ret));
    } else if (ls_task.empty()) {
      // do nothing
    } else {
      for (int i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
        ObBackupCleanLSTaskMgr task_mgr;
        ObBackupCleanLSTaskAttr &ls_attr = ls_task.at(i);
        if (ObBackupTaskStatus::Status::FINISH == ls_attr.status_.status_) {
          ++finish_cnt;
          if (OB_SUCCESS == ls_attr.result_) {
            success_ls_count++;
          }
        } else if (OB_FAIL(task_mgr.init(task_attr_, ls_attr, *task_scheduler_, trans, *backup_service_))) {
          LOG_WARN("failed to init task mgr", K(ret), K(ls_attr));
        } else if (OB_FAIL(task_mgr.cancel(finish_cnt))) {
          LOG_WARN("failed to cancel task", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && ls_task.count() == finish_cnt) {
      ObBackupCleanStatus next_status;
      next_status.status_ = ObBackupCleanStatus::Status::CANCELED;
      int64_t end_ts = ObTimeUtil::current_time();
      task_attr_.finish_ls_count_ = success_ls_count;
      if (OB_FAIL(advance_task_status_(trans, next_status, OB_CANCELED, end_ts))) {
        LOG_WARN("failed to advance set task status to CANCELED", K(ret));
      } else if (OB_FAIL(ObBackupCleanTaskOperator::update_ls_count(trans, task_attr_, false/*is_total*/))) {
        LOG_WARN("failed to update finish ls count", K(ret), K_(task_attr));
      } else {
        FLOG_INFO("[BACKUP_CLEAN]advance set status CANCELED success", K(next_status));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans.end(true))) {
        LOG_WARN("failed to commit trans", K(ret));
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to roll back status", K(ret), K(tmp_ret));
      } 
    }
  }
  
  return ret;
}
