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

#include "ob_backup_data_scheduler.h"
#include "ob_backup_data_ls_task_mgr.h"
#include "ob_backup_data_set_task_mgr.h"
#include "storage/tx/ob_ts_mgr.h"
#include "rootserver/ob_root_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "rootserver/ob_server_manager.h"
#include "observer/ob_sql_client_decorator.h"
#include "storage/ls/ob_ls.h"
#include "share/ls/ob_ls_operator.h"
#include "rootserver/backup/ob_backup_service.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/backup/ob_backup_data_store.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/backup/ob_backup_operator.h"

using namespace oceanbase;
using namespace omt;
using namespace common::hash;
using namespace share;
using namespace rootserver;
using namespace backup;

ObBackupSetTaskMgr::ObBackupSetTaskMgr()
 : is_inited_(false),
   meta_tenant_id_(0),
   next_status_(),
   set_task_attr_(),
   job_attr_(nullptr),
   sql_proxy_(nullptr),
   rpc_proxy_(nullptr),
   lease_service_(nullptr),
   task_scheduler_(nullptr),
   schema_service_(nullptr),
   backup_service_(nullptr),
   store_(),
   trans_()
{
}

int ObBackupSetTaskMgr::init(
    const uint64_t tenant_id, 
    ObBackupJobAttr &job_attr,
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObBackupTaskScheduler &task_scheduler,
    ObBackupLeaseService  &lease_service,
    ObMultiVersionSchemaService &schema_service,
    ObBackupService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[DATA_BACKUP]init twice", K(ret));
  } else if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(job_attr));
  } else if (OB_FAIL(ObBackupTaskOperator::get_backup_task(sql_proxy, job_attr.job_id_, job_attr.tenant_id_, 
      set_task_attr_))) {
    LOG_WARN("[DATA_BACKUP]failed to get backup task", K(ret), "job_id", job_attr.job_id_, 
        "tenant_id", job_attr.tenant_id_);
  } else {
    ObBackupDest backup_dest;
    share::ObBackupSetDesc desc;
    desc.backup_set_id_ = job_attr.backup_set_id_;
    desc.backup_type_ = job_attr.backup_type_;
    if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(sql_proxy, job_attr.tenant_id_, 
        job_attr.backup_path_, backup_dest))) {
      LOG_WARN("fail to get backup dest", K(ret), K(job_attr));
    } else if (OB_FAIL(store_.init(backup_dest, desc))) {
      LOG_WARN("fail to init backup data store", K(ret));
    } 
  }
  
  if (OB_SUCC(ret)) {
    meta_tenant_id_ = gen_meta_tenant_id(tenant_id);
    job_attr_ = &job_attr;
    sql_proxy_ = &sql_proxy;
    rpc_proxy_ = &rpc_proxy;
    lease_service_ = &lease_service;
    task_scheduler_ = &task_scheduler;
    schema_service_ = &schema_service;
    backup_service_ = &backup_service;
    next_status_ = set_task_attr_.status_;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupSetTaskMgr::advance_status_(
    ObMySQLTransaction &trans,
    const ObBackupStatus &next_status, 
    const int result,
    const SCN &scn,
    const int64_t end_ts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lease_service_->check_lease())) {
    LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
  } else if (OB_FAIL(ObBackupTaskOperator::advance_task_status(trans, set_task_attr_, next_status, result, scn, end_ts))) {
    LOG_WARN("[DATA_BACKUP]failed to advance set status", K(ret), K(set_task_attr_), K(next_status));
  } 
  return ret;
}


int ObBackupSetTaskMgr::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[DATA_BACKUP]schedule backup set task", K(set_task_attr_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DATA_BACKUP]tenant backup set task mgr not init", K(ret));
  } else {
    ObBackupStatus::Status status = set_task_attr_.status_.status_;
    switch (status) {
      case ObBackupStatus::Status::INIT: {
        if (OB_FAIL(persist_sys_ls_task_())) {
          LOG_WARN("[DATA_BACKUP]failed to persist log stream task", K(ret), K(set_task_attr_));
        }
        break;
      } 
      case ObBackupStatus::Status::BACKUP_SYS_META: {
        if (OB_FAIL(backup_sys_meta_())) {
          LOG_WARN("fail to backup sys meta", K(ret), K(set_task_attr_));
        }
        break;
      }
      case ObBackupStatus::Status::BACKUP_USER_META: {
        if (OB_FAIL(backup_user_meta_())) {
          LOG_WARN("[DATA_BACKUP]failed to backup meta", K(ret), K(set_task_attr_));
        }
        break;
      }
      case ObBackupStatus::Status::BACKUP_DATA_SYS:
      case ObBackupStatus::Status::BACKUP_DATA_MINOR:
      case ObBackupStatus::Status::BACKUP_DATA_MAJOR: {
        if (OB_FAIL(backup_data_())) {
          LOG_WARN("[DATA_BACKUP]failed to backup data", K(ret), K(set_task_attr_));
        }
        break;
      }
      case ObBackupStatus::Status::BACKUP_LOG: {
        if (OB_FAIL(backup_completing_log_())) {
          LOG_WARN("[DATA_BACKUP]failed to backup completing log", K(ret), K(set_task_attr_));
        }
        break;
      }
      case ObBackupStatus::Status::COMPLETED: 
      case ObBackupStatus::Status::FAILED: 
      case ObBackupStatus::Status::CANCELED: {
        break;
      }
      case ObBackupStatus::Status::CANCELING: {
        if (OB_FAIL(do_cancel_())) {
          LOG_WARN("[DATA_BACKUP]failed to cancel backup", K(ret), K(set_task_attr_));
        }
        break;
      }
      default: {
        ret = OB_ERR_SYS;
        LOG_ERROR("[DATA_BACKUP]unknown backup status", K(ret), K(set_task_attr_));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::persist_sys_ls_task_()
{ 
  int ret = OB_SUCCESS;
  next_status_.status_ = ObBackupStatus::Status::BACKUP_SYS_META;

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_PERSIST_LS_FAILED) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_backup_set_placeholder(true/*start*/))) {
    LOG_WARN("fail to write backup set start placeholder", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
  } else {
    if (OB_FAIL(do_persist_sys_ls_task_())) {
      LOG_WARN("fail to do persist ls tasks", K(ret));
    } else if (OB_FAIL(advance_status_(trans_, next_status_))) {
      LOG_WARN("fail to advance status to DOING", K(ret), K(set_task_attr_));
    } 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans_.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      } else {
        ROOTSERVICE_EVENT_ADD("backup_data", "persist sys ls task succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
        LOG_INFO("[BACKUP_DATA]succeed persit sys ls task", K(ret), K(next_status_), K(set_task_attr_));
        backup_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
        LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_persist_sys_ls_task_()
{
  int ret = OB_SUCCESS;
  ObArray<ObLSID> ls_ids;
  share::ObBackupDataTaskType type(share::ObBackupDataTaskType::Type::BACKUP_META);
  if (OB_FAIL(ls_ids.push_back(ObLSID(ObLSID::SYS_LS_ID)))) {
    LOG_WARN("fail to push sys ls id", K(ret));
  } else if (OB_FAIL(generate_ls_tasks_(ls_ids, type))) {
    LOG_WARN("fail to generate ls task", K(ret), K(ls_ids));
  } else {
    LOG_INFO("succeed to persist sys ls task", K(ret), K(ls_ids), "job_id", job_attr_->job_id_);
  }
  return ret;
}

int ObBackupSetTaskMgr::persist_ls_attr_info_(ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObBackupDataLSAttrDesc ls_attr_desc;
  ObLSAttrOperator ls_attr_operator(set_task_attr_.tenant_id_, sql_proxy_);
  bool ls_attr_info_exist = false;
  if (OB_FAIL(store_.read_ls_attr_info(set_task_attr_.meta_turn_id_, ls_attr_desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      if (OB_FAIL(ls_attr_operator.load_all_ls_and_snapshot(ls_attr_desc.backup_scn_, ls_attr_desc.ls_attr_array_))) {
        LOG_WARN("fail to get all ls by order", K(ret));
      }
    }
  } else {
    ls_attr_info_exist = true;
  }

  if (OB_FAIL(ret)) {
  } else if (!ls_attr_desc.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls desc", K(ret), K(ls_attr_desc));
  } else {
    ARRAY_FOREACH_X(ls_attr_desc.ls_attr_array_, i, cnt, OB_SUCC(ret)) {
      const ObLSAttr &attr = ls_attr_desc.ls_attr_array_.at(i);
      if (!attr.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls attr", K(ret), K(attr));
      } else if (attr.get_ls_id().is_sys_ls()) {
      } else if (OB_FAIL(ls_ids.push_back(attr.get_ls_id()))) {
        LOG_WARN("fail to push back ls id", K(ret), K(attr));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!ls_attr_info_exist && OB_FAIL(store_.write_ls_attr(set_task_attr_.meta_turn_id_, ls_attr_desc))) {
    LOG_WARN("fail to write ls attrs", K(ret), K(ls_attr_desc));
  } else if (OB_FAIL(ObBackupTaskOperator::update_user_ls_start_scn(*sql_proxy_, 
      set_task_attr_.task_id_, set_task_attr_.tenant_id_, ls_attr_desc.backup_scn_))) {
    LOG_WARN("fail to update backup set start scn", K(ret), "tenant_id", set_task_attr_.tenant_id_,
        "task_id", set_task_attr_.task_id_);
  } else {
    FLOG_INFO("[BACKUP_DATA]succeed persist ls attrs and ls tasks", K(ls_attr_desc));
  }
  return ret;
}

int ObBackupSetTaskMgr::generate_ls_tasks_(
    const ObIArray<ObLSID> &ls_ids,
    const ObBackupDataTaskType &type)
{
  int ret = OB_SUCCESS;
  ObBackupLSTaskAttr new_ls_attr;
  int64_t backup_date = -1;
  if (!type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(type));
  } else if (OB_FAIL(ObBackupUtils::convert_timestamp_to_date(job_attr_->start_ts_, backup_date))) {
    LOG_WARN("[DATA_BACKUP]failed to get backup date", K(ret), "start_ts", job_attr_->start_ts_);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
      new_ls_attr.task_id_ = set_task_attr_.task_id_;
      new_ls_attr.tenant_id_ = job_attr_->tenant_id_;
      new_ls_attr.ls_id_ = ls_ids.at(i);
      new_ls_attr.job_id_ = job_attr_->job_id_;
      new_ls_attr.backup_set_id_ = job_attr_->backup_set_id_;
      new_ls_attr.backup_type_.type_ = job_attr_->backup_type_.type_;
      new_ls_attr.task_type_.type_ = type.type_;
      new_ls_attr.status_.status_ = ObBackupTaskStatus::Status::INIT;
      new_ls_attr.start_ts_ = ObTimeUtility::current_time();
      new_ls_attr.end_ts_ = 0;
      new_ls_attr.backup_date_ = backup_date;
      new_ls_attr.start_turn_id_ = type.is_backup_meta() ? set_task_attr_.meta_turn_id_ : set_task_attr_.data_turn_id_;
      new_ls_attr.turn_id_ = type.is_backup_meta() ? set_task_attr_.meta_turn_id_ : set_task_attr_.data_turn_id_;
      new_ls_attr.retry_id_ = 0;
      new_ls_attr.result_ = OB_SUCCESS;

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(lease_service_->check_lease())) {
        LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret), K(set_task_attr_));
      } else if (OB_FAIL(ObBackupLSTaskOperator::insert_ls_task(trans_, new_ls_attr))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[DATA_BACKUP]failed to insert backup log stream task", K(ret), K(new_ls_attr));
        }
      } 
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::backup_sys_meta_()
{
  // backup sys ls meta and persist ls attr info
  int ret = OB_SUCCESS;
  ObArray<ObBackupLSTaskAttr> ls_task;
  int64_t finish_cnt = 0;
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, false/*update*/, ls_task))) {
    LOG_WARN("fail to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no lostream task", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(do_backup_meta_(ls_task, finish_cnt))) {
    LOG_WARN("fail to do backuo meta", K(ret), K(ls_task));
  } else if (ls_task.count() == finish_cnt) {
    next_status_.status_ = ObBackupStatus::Status::BACKUP_USER_META;
    ObArray<share::ObLSID> ls_ids;
    if (OB_FAIL(disable_transfer_())) {
      LOG_WARN("fail to disable transfer", K(ret));
    } else if (OB_FAIL(persist_ls_attr_info_(ls_ids))) {
      LOG_WARN("fail to do persist ls task", K(ret));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      share::ObBackupDataTaskType type(share::ObBackupDataTaskType::Type::BACKUP_META);
      if (OB_FAIL(generate_ls_tasks_(ls_ids, type))) {
        LOG_WARN("failed to generate log stream tasks", K(ret), KPC(job_attr_), K(ls_ids));
      } else if (OB_FAIL(advance_status_(trans_, next_status_))) {
        LOG_WARN("fail to advance status", K(ret), K(next_status_));
      } 

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("backup_data", "backup sys meta succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          LOG_INFO("succeed to backup sys ls meta", K(ret), KPC(job_attr_));
          backup_service_->wakeup();
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
          LOG_WARN("fail to rollback", KR(ret), K(tmp_ret));
        }
      }
    }  
  }
  return ret;
}

int ObBackupSetTaskMgr::backup_user_meta_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupLSTaskAttr> ls_task;
  int64_t finish_cnt = 0;
  DEBUG_SYNC(BEFORE_BACKUP_UESR_META);
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, false/*update*/, ls_task))) {
    LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]no lostream task", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(do_backup_meta_(ls_task, finish_cnt))) {
    LOG_WARN("[DATA_BACKUP]failed to do backuo meta", K(ret), K(ls_task));
  } else if (ls_task.count() == finish_cnt) {
    ROOTSERVICE_EVENT_ADD("backup_data", "before_backup_data");
    DEBUG_SYNC(BEFORE_BACKUP_DATA);
    next_status_.status_ = ObBackupStatus::Status::BACKUP_DATA_SYS;
    if (OB_FAIL(merge_tablet_to_ls_info_(ls_task))) {
      LOG_WARN("[DATA_BACKUP]failed to merge tabelt to ls info", K(ret), K(ls_task));
    } else if (OB_FAIL(merge_ls_meta_infos_(ls_task))) {
      LOG_WARN("fail to merge ls meta infos", K(ret), K(ls_task));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      if (OB_FAIL(update_task_type_(ls_task))) {
        LOG_WARN("[DATA_BACKUP]fail to update task type to backup data", K(ret));
      } else if (OB_FAIL(advance_status_(trans_, next_status_, OB_SUCCESS))) {
        LOG_WARN("[DATA_BACKUP]failed to advance status", K(ret), K(next_status_));
      } 

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("failed to commit trans", KR(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("backup_data", "backup user ls meta succeed", "tenant_id", 
              job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          LOG_INFO("succeed to backup user ls meta", K(ret), KPC(job_attr_));
          backup_service_->wakeup();
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
          LOG_WARN("failed to rollback", KR(ret), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::change_meta_turn_()
{
  int ret = OB_SUCCESS;
  int64_t next_meta_turn_id = ++set_task_attr_.meta_turn_id_;
  ObArray<share::ObLSID> ls_ids;
  if (next_meta_turn_id > OB_MAX_RETRY_TIMES) {
    ret = OB_TIMEOUT;
    job_attr_->can_retry_ = false;
    LOG_WARN("backup meta timeout, backup can't continue", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(task_scheduler_->cancel_tasks(BackupJobType::BACKUP_DATA_JOB, job_attr_->job_id_, 
      job_attr_->tenant_id_))) {
    LOG_WARN("fail to cancel task from backup task scheduelr", K(ret));
  } else if (OB_FAIL(disable_transfer_())) {
    LOG_WARN("fail to disable transfer", K(ret));
  } else if (OB_FAIL(persist_ls_attr_info_(ls_ids))) {
    LOG_WARN("fail to do persist ls task", K(ret));
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
  } else {
    share::ObBackupDataTaskType type(share::ObBackupDataTaskType::Type::BACKUP_META);
    if (OB_FAIL(ObBackupLSTaskOperator::delete_ls_task_without_sys(trans_, set_task_attr_.tenant_id_, 
        set_task_attr_.task_id_))) {
      LOG_WARN("fail to delete ls task", K(ret), "tenant_id", set_task_attr_.tenant_id_, "job_id", 
          set_task_attr_.job_id_);
    } else if (OB_FAIL(generate_ls_tasks_(ls_ids, type))) {
      LOG_WARN("failed to generate log stream tasks", K(ret), KPC(job_attr_), K(ls_ids));
    } else if (OB_FAIL(ObBackupTaskOperator::update_meta_turn_id(trans_, set_task_attr_.task_id_, 
        set_task_attr_.tenant_id_, next_meta_turn_id))) {
      LOG_WARN("fail to update meta turn id", K(ret), K(set_task_attr_));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans_.end(true))) {
        LOG_WARN("fail to commit trans", KR(ret));
      } else {
        LOG_INFO("backup user meta change turn.", K(ret), KPC(job_attr_));
        backup_service_->wakeup();
      }
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
        LOG_WARN("fail to rollback", KR(ret), K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::merge_ls_meta_infos_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupSetDesc desc;
  desc.backup_set_id_ = job_attr_->backup_set_id_;
  desc.backup_type_ = job_attr_->backup_type_;
  ObBackupLSMetaInfosDesc ls_meta_infos;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, job_attr_->tenant_id_, 
    job_attr_->backup_path_, backup_dest))) {
    LOG_WARN("fail to get backup dest", K(ret), KPC(job_attr_));
  } else {
    ARRAY_FOREACH_X(ls_tasks, i, cnt, OB_SUCC(ret)) {
      const ObBackupLSTaskAttr &ls_task_attr = ls_tasks.at(i);
      backup::ObExternLSMetaMgr ls_meta_mgr;
      ObBackupLSMetaInfo ls_meta;
      if (OB_FAIL(ls_meta_mgr.init(backup_dest, desc, ls_task_attr.ls_id_, ls_task_attr.turn_id_, 
          ls_task_attr.retry_id_))) {
        LOG_WARN("fail to init ls meta mgr", K(ret), K(ls_task_attr));
      } else if (OB_FAIL(ls_meta_mgr.read_ls_meta_info(ls_meta))) {
        LOG_WARN("fail to read ls meta info", K(ret));
      } else if (!ls_meta.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls meta", K(ret));
      } else if (OB_FAIL(ls_meta_infos.ls_meta_packages_.push_back(ls_meta.ls_meta_package_))) {
        LOG_WARN("fail to push backup ls meta package", K(ret), K(ls_meta));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(store_.write_ls_meta_infos(ls_meta_infos))) {
        LOG_WARN("fail to write ls meta infos", K(ret), K(ls_meta_infos));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::merge_tablet_to_ls_info_(const ObIArray<ObBackupLSTaskAttr> &ls_tasks)
{
  // merge ls level tablet list to tenant level tablet to ls info.
  int ret = OB_SUCCESS;
  ObHashMap<ObLSID, ObArray<ObTabletID>> tablet_to_ls;
  ObHashMap<ObLSID, const ObBackupLSTaskAttr *> ls_map;
  share::ObBackupDataTabletToLSDesc tablet_to_ls_desc;
  const int64_t OB_BACKUP_MAX_LS_BUCKET = 1024;
  SCN max_backup_scn;
  if (ls_tasks.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_tasks));
  } else if (OB_FAIL(tablet_to_ls.create(OB_BACKUP_MAX_LS_BUCKET, "tabletToLS"))) {
    LOG_WARN("[DATA_BACKUP]fail to create tablet to ls map", K(ret));
  } else if (OB_FAIL(ls_map.create(OB_BACKUP_MAX_LS_BUCKET, "lSTaskToLS"))) {
    LOG_WARN("[DATA_BACKUP]fail to create ls set", K(ret));
  } else if (OB_FAIL(construct_ls_task_map_(ls_tasks, ls_map))) {
    LOG_WARN("[DATA_BACKUP]fail to construct ls set", K(ret), K(ls_tasks));
  } else if (OB_FAIL(ObBackupTabletToLSOperator::get_ls_and_tablet(
      *sql_proxy_, set_task_attr_.tenant_id_, tablet_to_ls))) {
    LOG_WARN("[DATA_BACKUP]fail to get ls and tablet", K(ret), "tenant_id", set_task_attr_.tenant_id_);
  } else {
    // In order to ensure observer backup all the tablet, we need to check if latest tablet list is match the 
    // extern tablet lists.
    // if not match, return an err and backup failed.
    ObHashMap<ObLSID, const ObBackupLSTaskAttr *>::iterator iter = ls_map.begin();
    while (OB_SUCC(ret) && iter != ls_map.end()) {
      const ObLSID &ls_id = iter->first;
      const int64_t retry_id = iter->second->retry_id_;
      share::ObBackupDataTabletToLSInfo tablet_to_ls_info;
      SCN backup_scn = SCN::min_scn();
      ObArray<ObTabletID> latest_tablet_id;
      if (OB_FAIL(get_extern_tablet_info_(ls_id, retry_id, tablet_to_ls_info, backup_scn))) {
        LOG_WARN("fail to get extern tablet info", K(ret), K(ls_id), K(retry_id));
      } else if (OB_FAIL(tablet_to_ls.get_refactored(ls_id, latest_tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // ls may be deleted, or has no tablet.
          ret = OB_SUCCESS;
          LOG_INFO("ls may be deleted, or has no tablet, skip it", K(ls_id)); 
        } else {
          LOG_WARN("fail to get refactored", K(ret), K(ls_id));
        }
      } else if (OB_FAIL(check_tablets_match_(ls_id, latest_tablet_id, tablet_to_ls_info.tablet_id_list_, backup_scn))) {
        LOG_WARN("fail to check latest tablet match extern ls level tablet to ls info ", 
          K(ret), K(ls_id), K(latest_tablet_id), K(tablet_to_ls_info));
      } 
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tablet_to_ls_desc.tablet_to_ls_.push_back(tablet_to_ls_info))) {
        LOG_WARN("[DATA_BACKUP]fail to push back tablet_to_ls_info", K(ret));
      } else {
        max_backup_scn = std::max(max_backup_scn, backup_scn);
        tablet_to_ls_desc.backup_scn_ = max_backup_scn;
        ++iter;
      }
    }
  }

  if (OB_FAIL(ret)) { // when backup meta succeed, write the first turn tablet_to_ls_info file used by backup data.
  } else if (OB_FAIL(store_.write_tablet_to_ls_info(tablet_to_ls_desc, 1/*first turn*/))) { 
    LOG_WARN("fail to write first turn tablet to ls info", K(ret), K(tablet_to_ls_desc));
  } else {
    LOG_INFO("[BACKUP_DATA]succeed to write first turn tablet to ls info", K(tablet_to_ls_desc), K(set_task_attr_));
  }
  return ret;
}

int ObBackupSetTaskMgr::get_extern_tablet_info_(
    const share::ObLSID &ls_id, const int64_t &retry_cnt, 
    share::ObBackupDataTabletToLSInfo &tablet_to_ls_info, 
    SCN &backup_scn)
{
  int ret = OB_SUCCESS;
  share::ObBackupSetDesc backup_set_desc;
  ObArray<common::ObTabletID> tablet_ids;
  share::ObBackupDataTabletToLSDesc desc;
  int64_t turn_id = ls_id.is_sys_ls() ? 1/*sys ls only has one meta turn*/ : set_task_attr_.meta_turn_id_;
  if (OB_FAIL(store_.read_tablet_to_ls_info(ls_id, turn_id, retry_cnt, desc))) {
    LOG_WARN("fail to read read_tablet_to_ls_info", K(ret), K(ls_id), K(turn_id), K(retry_cnt));
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < desc.tablet_to_ls_.count(); ++i) {
      const share::ObBackupDataTabletToLSInfo &info = desc.tablet_to_ls_.at(i);
      if (info.ls_id_ == ls_id) {
        tablet_to_ls_info.ls_id_ = ls_id;
        if (OB_FAIL(tablet_to_ls_info.tablet_id_list_.assign(info.tablet_id_list_))) {
          LOG_WARN("fail to assign tablet ids", K(ret), K(info));
        } else {
          backup_scn = desc.backup_scn_;
        }
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("do not found entry", K(ls_id));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::check_tablets_match_(
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &cur_tablet_ids, 
    const ObIArray<ObTabletID> &user_tablet_ids, 
    const SCN &backup_scn)
{
  int ret = OB_SUCCESS;
  ObHashSet<ObTabletID> tablet_set;
  int64_t OB_BACKUP_MAX_LS_BUCKET = 1024;
  ObBackupCheckTabletArg arg;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid ls id", K(ret), K(ls_id));
  } else if (OB_FAIL(tablet_set.create(OB_MAX_TABLE_ID_LIST_SIZE))) {
    LOG_WARN("[DATA_BACKUP]fail to create ls set", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < user_tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = user_tablet_ids.at(i);
      if (OB_FAIL(tablet_set.set_refactored(user_tablet_ids.at(i)))) {
        LOG_WARN("[DATA_BACKUP]fail to insert tablet into tablet set", K(ret), K(tablet_id));
      }
    }
    ObArray<ObTabletID> inc_tablets;
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_tablet_ids.count(); ++i) {
      const ObTabletID &tablet_id = cur_tablet_ids.at(i);
      if (OB_FAIL(tablet_set.exist_refactored(tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(inc_tablets.push_back(tablet_id))) {
            LOG_WARN("[DATA_BACKUP]fail to push back tablet id into inc tablets", K(ret), K(tablet_id));
          }
        } else if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[DATA_BACKUP]fail to check tablet exist", K(ret), K(tablet_id));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (inc_tablets.empty()) { 
    } else if (OB_FAIL(do_check_inc_tablets_(ls_id, inc_tablets, backup_scn))) {
      LOG_WARN("[DATA_BACKUP]fail to do check backup all tablets", K(ret), K(ls_id), K(inc_tablets));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_check_inc_tablets_(
    const ObLSID &ls_id, 
    const ObIArray<ObTabletID> &inc_tablets, 
    const SCN &backup_scn)
{
  int ret = OB_SUCCESS;
  ObAddr dst_server;
  if (!ls_id.is_valid() || inc_tablets.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_id), K(inc_tablets));
  } else if (OB_FAIL(get_dst_server_(ls_id, dst_server))) {
    LOG_WARN("[DATA_BACKUP]fail to get dst server", K(ret));
  } else {
    ObBackupCheckTabletArg arg;
    arg.tenant_id_ = set_task_attr_.tenant_id_;
    arg.ls_id_ = ls_id;
    arg.backup_scn_ = backup_scn;
    if (OB_FAIL(append(arg.tablet_ids_, inc_tablets))) {
      LOG_WARN("[DATA_BACKUP]append inc tablets", K(ret), K(inc_tablets));
    } else if (OB_FAIL(rpc_proxy_->to(dst_server).check_not_backup_tablet_create_scn(arg))) {
      LOG_WARN("[DATA_BACKUP]fail to send backup check tablet rpc", K(ret), K(arg), K(dst_server));
    } else {
      FLOG_INFO("[DATA_BACKUP]succeed send backup check tablet rpc", K(arg), K(dst_server));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::get_dst_server_(const ObLSID &ls_id, ObAddr &dst)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = set_task_attr_.tenant_id_;
  share::ObLSTableOperator *lst_operator = GCTX.lst_operator_;
  int64_t cluster_id = GCONF.cluster_id;
  ObLSInfo ls_info;
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(lst_operator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]lst_operator ptr is null", K(ret));
  } else {
    // When change leader, the new leader may not be reported to __all_ls_meta_table timely, and we could get no leader.
    // And ownerless election may cost more than 30s for choosing leader.
    // So, we add retry to tolerate this scene, and set the abs timeout to 30s in the future.
    const int64_t abs_timeout = ObTimeUtility::current_time() + 30 * 1000 * 1000;
    do {
      if (OB_FAIL(lst_operator->get(cluster_id, tenant_id, ls_id, share::ObLSTable::DEFAULT_MODE, ls_info))) {
        LOG_WARN("[DATA_BACKUP]failed to get log stream info", K(ret), K(cluster_id), K(tenant_id), K(ls_id));
      } else {
        const ObLSInfo::ReplicaArray &replica_array = ls_info.get_replicas();
        for (int64_t i = 0; OB_SUCC(ret) && i < replica_array.count(); ++i) {
          const ObLSReplica &replica = replica_array.at(i);
          if (replica.is_in_service() && replica.is_strong_leader() && replica.is_valid()) {
            dst = replica.get_server();
            break;
          }
        }
      }
      if (!dst.is_valid()) {
        // wait 100 ms for next retry.
        usleep(100 * 1000);
        if(OB_FAIL(lease_service_->check_lease())) {
          LOG_WARN("failed to check lease", K(ret));
        }
      }
    } while (OB_SUCC(ret) && !dst.is_valid() && ObTimeUtility::current_time() < abs_timeout);
  }

  if (OB_FAIL(ret)) {
  } else if (!dst.is_valid()) {
    ret = OB_LEADER_NOT_EXIST;
    LOG_WARN("[DATA_BACKUP]no leader be found", K(ret), K(ls_id), K(set_task_attr_));
  }
  return ret;
}

int ObBackupSetTaskMgr::construct_ls_task_map_(const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, 
    hash::ObHashMap<share::ObLSID, const ObBackupLSTaskAttr *> &ls_map)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
    const ObBackupLSTaskAttr &ls_task_attr = ls_tasks.at(i);
    if (OB_FAIL(ls_map.set_refactored(ls_task_attr.ls_id_, &ls_task_attr))) {
      LOG_WARN("[DATA_BACKUP]fail to insert ls id into set", K(ret));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::disable_transfer_()
{
  int ret = OB_SUCCESS;
  FLOG_WARN("disable transfer success");
  return ret;
}

int ObBackupSetTaskMgr::enable_transfer_()
{
  int ret = OB_SUCCESS;
  FLOG_WARN("enable transfer success");
  return ret;
}

int ObBackupSetTaskMgr::do_backup_meta_(ObArray<ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  finish_cnt = 0;
  if (ls_task.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
      ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
      ObBackupDataLSTaskMgr ls_task_mgr;
      if (OB_FAIL(ls_task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, 
          *lease_service_, *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task advancer", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to process ls backup meta task", K(ret), K(ls_attr), K(set_task_attr_));
      } 
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::get_next_status_(const share::ObBackupStatus &cur_status, share::ObBackupStatus &next_status)
{
  int ret = OB_SUCCESS;
  if (!cur_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cur_status));
  } else {
    switch (cur_status.status_)
    {
    case ObBackupStatus::Status::INIT: {
      next_status = ObBackupStatus::Status::BACKUP_SYS_META;
      break;
    }
    case ObBackupStatus::Status::BACKUP_SYS_META: {
      next_status = ObBackupStatus::Status::BACKUP_USER_META;
      break;
    }
    case ObBackupStatus::Status::BACKUP_USER_META: {
      next_status = ObBackupStatus::Status::BACKUP_DATA_SYS;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_SYS: {
      next_status = ObBackupStatus::Status::BACKUP_DATA_MINOR;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_MINOR: {
      next_status = ObBackupStatus::Status::BACKUP_DATA_MAJOR;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_MAJOR: {
      next_status = ObBackupStatus::Status::BACKUP_LOG;
      break;
    }
    case ObBackupStatus::Status::BACKUP_LOG: {
      next_status = ObBackupStatus::Status::COMPLETED;
      break;
    }
    default:
      break;
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::backup_data_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupLSTaskAttr> ls_task;
  int64_t finish_cnt = 0;
  ObBackupLSTaskAttr *build_index_attr = nullptr;
  if (OB_FAIL(enable_transfer_())) {
    LOG_WARN("[DATA_BACKUP]failed to enbale transfer", K(ret));
  } else if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, false/*update*/, ls_task))) {
    LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]no lostream task", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(do_backup_data_(ls_task, finish_cnt, build_index_attr))) {
    LOG_WARN("[DATA_BACKUP]failed to do backup ls task", K(ret), K(ls_task));
  } else if (ls_task.count() == finish_cnt) {
    SCN end_scn = SCN::min_scn();
    bool need_change_turn = false;
    bool finish_build_index = false;
    ObSArray<share::ObBackupDataTabletToLSInfo> tablets_to_ls;
    ObSArray<ObLSID> new_ls_array; 
    if (OB_FAIL(get_next_status_(set_task_attr_.status_, next_status_))) {
      LOG_WARN("fail to get next status", K(set_task_attr_.status_), K(next_status_));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      if (OB_FAIL(ObBackupDataScheduler::get_scn(*sql_proxy_, job_attr_->tenant_id_, end_scn))) {
        LOG_WARN("[DATA_BACKUP]failed to get end ts", K(ret), "tenant_id", job_attr_->tenant_id_);
      } else if (OB_FAIL(build_index_(build_index_attr, set_task_attr_.data_turn_id_, set_task_attr_.task_id_, finish_build_index))) {
        LOG_WARN("[DATA_BACKUP]failed to wait build index", K(ret), K(set_task_attr_), KPC(build_index_attr));
      } else if (!finish_build_index) {
      } else if (OB_FAIL(check_change_task_turn_(ls_task, need_change_turn, tablets_to_ls, new_ls_array))) {
        LOG_WARN("[DATA_BACKUP]failed to check change task turn", K(ret), K(set_task_attr_));
      } else if (need_change_turn) {
        if (OB_FAIL(change_task_turn_(ls_task, tablets_to_ls, new_ls_array))) {
          LOG_WARN("[DATA_BACKUP]failed to change task turn", K(ret), K(set_task_attr_));
        } else {
          backup_service_->wakeup();
        }
      } else {
        if (ObBackupStatus::Status::BACKUP_DATA_MAJOR == set_task_attr_.status_.status_ && !job_attr_->plus_archivelog_) {
        } else if (OB_FAIL(update_task_type_(ls_task))) {
          LOG_WARN("[DATA_BACKUP]failed to update task type to PLUS_ARCHIVE_LOG", K(ret), K(ls_task));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(advance_status_(trans_, next_status_, OB_SUCCESS, end_scn))) {
          LOG_WARN("[DATA_BACKUP]failed to update set task status to COMPLETEING", K(ret), K(set_task_attr_));
        } else {
          LOG_INFO("backup data succeed, advance status to backup compelement log", "tenant_id", job_attr_->tenant_id_,
            "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          ROOTSERVICE_EVENT_ADD("backup_data", "backup data succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          backup_service_->wakeup();
        }
      } 

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } 
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
          LOG_WARN("fail to rollback", KR(ret), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_backup_data_(
    ObArray<ObBackupLSTaskAttr> &ls_task, 
    int64_t &finish_cnt, 
    ObBackupLSTaskAttr *& build_index_attr)
{
  int ret = OB_SUCCESS;
  finish_cnt = 0;
  if (ls_task.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_task));
  } else {
    set_task_attr_.stats_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
      ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
      if (ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX == ls_attr.task_type_.type_) {
        build_index_attr = &ls_attr;
      }
      ObBackupDataLSTaskMgr ls_task_mgr;
      if (OB_FAIL(ls_task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *lease_service_, *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task advancer", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to process log stream task", K(ret), K(ls_attr), K(set_task_attr_));
      } else {
        set_task_attr_.stats_.cum_with(ls_attr.stats_);
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(ObBackupTaskOperator::update_stats(*sql_proxy_, set_task_attr_.task_id_, set_task_attr_.tenant_id_, set_task_attr_.stats_))) {
      LOG_WARN("[DATA_BACKUP]failed update statistic infomation", K(ret), K(set_task_attr_));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::check_change_task_turn_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks,
    bool &need_change_turn, 
    ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls,
    ObIArray<ObLSID> &new_ls_array)
{
  int ret = OB_SUCCESS;
  need_change_turn = true;
  if (OB_FAIL(get_change_turn_tablets_(ls_tasks, tablets_to_ls, new_ls_array))) {
    LOG_WARN("[DATA_BACKUP]failed to get change turn tablets", K(ret), K(ls_tasks));
  } else if (tablets_to_ls.empty() && new_ls_array.empty()) {
    need_change_turn = false;
  } 
  return ret;
}

int ObBackupSetTaskMgr::change_task_turn_(
    ObIArray<ObBackupLSTaskAttr> &ls_task, 
    ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls,
    ObIArray<ObLSID> &new_ls_array)
{
  int ret = OB_SUCCESS;
  ObSArray<ObBackupLSTaskAttr *> need_change_turn_ls_tasks;
  if (ls_task.empty() || (tablets_to_ls.empty() && new_ls_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_task), K(tablets_to_ls), K(new_ls_array));
  } else {
    set_task_attr_.data_turn_id_++;
    if (OB_FAIL(write_tablet_to_ls_infos_(tablets_to_ls, set_task_attr_.data_turn_id_))) {
      LOG_WARN("[DATA_BACKUP]failed to write ls and tablets info when change turn", K(ret), K(tablets_to_ls), K(set_task_attr_));
    } else if (OB_FAIL(get_change_turn_ls_(ls_task, tablets_to_ls, need_change_turn_ls_tasks))) {
      LOG_WARN("[DATA_BACKUP]failed to get change turn id and ls when change turn", K(ret), K(ls_task), K(tablets_to_ls));
    } else if (OB_FAIL(update_inner_task_(new_ls_array, need_change_turn_ls_tasks))) {
      LOG_WARN("[DATA_BACKUP]failed to update inner task when change turn", K(ret), K(need_change_turn_ls_tasks));
    } 
  }
  return ret;
}

int ObBackupSetTaskMgr::update_inner_task_(
    const ObIArray<ObLSID> &new_ls_ids,
    const ObIArray<ObBackupLSTaskAttr *> &need_change_turn_ls_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < need_change_turn_ls_tasks.count(); ++i) {
    ObBackupLSTaskAttr *ls_attr = need_change_turn_ls_tasks.at(i);
    if (nullptr == ls_attr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[DATA_BACKUP]null ls ptr", K(ret));
  // TODO use another error code to determine change turn in 4.1
    } else if (OB_FAIL(ObBackupDataLSTaskMgr::redo_ls_task(
        *lease_service_, trans_, *ls_attr, ls_attr->start_turn_id_, set_task_attr_.data_turn_id_, 0/*retry_id*/))) {
      LOG_WARN("[DATA_BACKUP]failed to update ls task result to success", K(ret), KPC(ls_attr));
    }
  } 
  ObBackupDataTaskType type;
  if (ObBackupStatus::Status::BACKUP_DATA_SYS == set_task_attr_.status_.status_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys tablet must not transfer", K(ret), K(set_task_attr_));
  } else if (ObBackupStatus::Status::BACKUP_DATA_MINOR == set_task_attr_.status_.status_) {
    type.type_ = ObBackupDataTaskType::Type::BACKUP_DATA_MINOR;
  } else if (ObBackupStatus::Status::BACKUP_DATA_MAJOR == set_task_attr_.status_.status_) {
    type.type_ = ObBackupDataTaskType::Type::BACKUP_DATA_MAJOR;
  }

  if (OB_FAIL(ret)) {
  } else if (!new_ls_ids.empty() && OB_FAIL(generate_ls_tasks_(new_ls_ids, type))) {
    LOG_WARN("[DATA_BACKUP]failed to generate new task", K(ret), K(new_ls_ids));
  } else if ((!new_ls_ids.empty() || !need_change_turn_ls_tasks.empty())
      && OB_FAIL(ObBackupTaskOperator::update_max_turn_id(
      trans_, set_task_attr_.task_id_, set_task_attr_.tenant_id_, set_task_attr_.data_turn_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to update max turn id", K(ret), "task_id", set_task_attr_.task_id_, 
      "tenant_id", set_task_attr_.tenant_id_, "max_turn_id", set_task_attr_.data_turn_id_);
  }
  return ret;
}

int ObBackupSetTaskMgr::write_tablet_to_ls_infos_(
    const ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls, 
    const int64_t turn_id)
{
  int ret = OB_SUCCESS;
  share::ObBackupDataTabletToLSDesc tablet_to_ls_desc;
  if (tablets_to_ls.empty()) { // no tablet, no need to write extern tablets info
  } else if (OB_FAIL(append(tablet_to_ls_desc.tablet_to_ls_, tablets_to_ls))) {
    LOG_WARN("[DATA_BACKUP]failed to append tablets_to_ls", K(ret));
  } else if (OB_FAIL(store_.write_tablet_to_ls_info(tablet_to_ls_desc, turn_id))) {
    LOG_WARN("[DATA_BACKUP]failed to write tablet to log stream", K(ret), K(tablet_to_ls_desc), K(turn_id));
  }
  return ret;
}

int ObBackupSetTaskMgr::build_index_(
    ObBackupLSTaskAttr *build_index_attr, 
    const int64_t turn_id, 
    const int64_t task_id,
    bool &finish_build_index)
{
  int ret = OB_SUCCESS;
  finish_build_index = false;
  if (OB_FAIL(lease_service_->check_lease())) {
      LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
  } else if (nullptr == build_index_attr) {
    ObBackupLSTaskAttr build_index_task;
    build_index_task.task_id_ = task_id;
    build_index_task.tenant_id_ = job_attr_->tenant_id_;
    build_index_task.ls_id_ = ObLSID(0);
    build_index_task.job_id_ = job_attr_->job_id_;
    build_index_task.backup_set_id_ = job_attr_->backup_set_id_;
    build_index_task.backup_type_.type_ = job_attr_->backup_type_.type_;
    build_index_task.task_type_.type_ = ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX;
    build_index_task.status_.status_ = ObBackupTaskStatus::Status::INIT;
    build_index_task.start_ts_ = ObTimeUtility::current_time();
    build_index_task.start_turn_id_ = turn_id;
    build_index_task.turn_id_ = turn_id;
    build_index_task.retry_id_ = 0;
    build_index_task.result_ = OB_SUCCESS;
    if (OB_FAIL(ObBackupUtils::convert_timestamp_to_date(build_index_task.start_ts_, build_index_task.backup_date_))) { 
      LOG_WARN("[DATA_BACKUP]failed to get date", K(ret), K(build_index_task));
    } else if (OB_FAIL(ObBackupLSTaskOperator::insert_build_index_task(trans_, build_index_task))) {
      LOG_WARN("[DATA_BACKUP]failed to insert build index task", K(ret), K(build_index_task));
    } else {
      backup_service_->wakeup();
    }
  } else if (OB_SUCCESS == build_index_attr->result_ && ObBackupTaskStatus::Status::FINISH == build_index_attr->status_.status_) {
    if (OB_FAIL(ObBackupLSTaskOperator::delete_build_index_task(trans_, *build_index_attr))) {
      LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
    } else {
      finish_build_index = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]unexpected err", K(ret), KPC(build_index_attr));
  }
  return ret;
}

int ObBackupSetTaskMgr::get_change_turn_ls_(
    ObIArray<ObBackupLSTaskAttr> &ls_task,
    const ObIArray<share::ObBackupDataTabletToLSInfo> &tablets_to_ls,
    ObIArray<ObBackupLSTaskAttr *> &need_change_turn_ls_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < tablets_to_ls.count(); ++j) {
      ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
      const share::ObBackupDataTabletToLSInfo& tablets = tablets_to_ls.at(j);
      if (ls_attr.ls_id_ == tablets.ls_id_) {
        if (OB_FAIL(need_change_turn_ls_tasks.push_back(&ls_attr))) {
          LOG_WARN("[DATA_BACKUP]failed to push ls task", K(ret));
        } 
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::get_change_turn_tablets_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks,
    ObIArray<share::ObBackupDataTabletToLSInfo> &tablet_to_ls,
    ObIArray<ObLSID> &new_ls_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSkipTabletAttr> tablet_attrs;
  ObBackupSkippedType skipped_type(ObBackupSkippedType::TRANSFER);
  if (OB_FAIL(ObBackupSkippedTabletOperator::get_skip_tablet(trans_, true/*lock*/, set_task_attr_.tenant_id_, set_task_attr_.task_id_, skipped_type, tablet_attrs))) {
    LOG_WARN("[DATA_BACKUP]failed to get skip tablet", K(ret), "teannt_id", set_task_attr_.tenant_id_, "task_id", set_task_attr_.task_id_);
  } else if (tablet_attrs.empty()) {
    LOG_INFO("[DATA_BACKUP]no change turn tablets found", K(ret));
  } else if (OB_FAIL(do_get_change_turn_tablets_(ls_tasks, tablet_attrs, tablet_to_ls, new_ls_ids))) {
    LOG_WARN("[DATA_BACKUP]failed to do get change turn tables", K(ret), K(tablet_attrs), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupSkippedTabletOperator::move_skip_tablet_to_his(trans_, set_task_attr_.tenant_id_, set_task_attr_.task_id_, skipped_type))) {
    LOG_WARN("[DATA_BACKUP]failed to move skip tablet to history", K(ret), K(tablet_attrs));
  }
  return ret;
}

int ObBackupSetTaskMgr::do_get_change_turn_tablets_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks,
    const ObIArray<ObBackupSkipTabletAttr> &all_tablets,
    ObIArray<share::ObBackupDataTabletToLSInfo> &tablet_to_ls,
    ObIArray<ObLSID> &new_ls_ids) 
{
  int ret = OB_SUCCESS;
  // if tablet can't be found in __all_tablet_to_ls, it need't to change turn
  // if tablet transfer to a new create ls, need insert a new ls task
  ObHashMap<ObLSID, ObArray<ObTabletID>> tablet_to_ls_map;
  ObHashSet<ObLSID> ls_id_set;
  ObHashSet<ObLSID> new_ls_id_set;
  ObLSID ls_id;
  const int64_t OB_BACKUP_MAX_LS_BUCKET = 1024;
  if (OB_FAIL(tablet_to_ls_map.create(OB_BACKUP_MAX_LS_BUCKET, "tabletToLS"))) {
    LOG_WARN("[DATA_BACKUP]failed to create map", K(ret));
  } else if (OB_FAIL(ls_id_set.create(OB_BACKUP_MAX_LS_BUCKET))) {
    LOG_WARN("[DATA_BACKUP]failed to create set", K(ret));
  } else if (OB_FAIL(new_ls_id_set.create(OB_BACKUP_MAX_LS_BUCKET))) {
    LOG_WARN("[DATA_BACKUP]failed to create set", K(ret));
  } else if (OB_FAIL(construct_cur_ls_set_(ls_tasks, ls_id_set))) {
    LOG_WARN("[DATA_BACKUP]failed to get last turn ls ids", K(ret), K(set_task_attr_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_tablets.count(); ++i) {
      const ObBackupSkipTabletAttr &skip_tablet = all_tablets.at(i);
      if (OB_FAIL(ObBackupTabletToLSOperator::get_ls_of_tablet(*sql_proxy_, job_attr_->tenant_id_, skip_tablet.tablet_id_, ls_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("[DATA_BACKUP]deleted tablet", K(skip_tablet));
        } else {
          LOG_WARN("[DATA_BACKUP]failed to get tablet", K(ret), K(skip_tablet));
        }
      } else if (ls_id != skip_tablet.ls_id_) { // transfer or split
        ObArray<ObTabletID> tmp_tablet_ids;
        if (OB_FAIL(tablet_to_ls_map.get_refactored(ls_id, tmp_tablet_ids))) {
          if (OB_ENTRY_NOT_EXIST) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("[DATA_BACKUP]failed to get refactored", K(ret), K(ls_id));
          }
        } 
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(tmp_tablet_ids.push_back(skip_tablet.tablet_id_))) {
          LOG_WARN("[DATA_BACKUP]failed to push tablet id", K(ret), K(skip_tablet));
        } else if (OB_FAIL(tablet_to_ls_map.set_refactored(ls_id, tmp_tablet_ids, 1/*cover exist object*/))) {
          LOG_WARN("[DATA_BACKUP]failed to set_refactored", K(ret),  K(ls_id));
        } else if (OB_FAIL(ls_id_set.exist_refactored(ls_id))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else if (OB_HASH_NOT_EXIST == ret) {
            if (OB_FAIL(new_ls_id_set.set_refactored(ls_id))) {
              LOG_WARN("[DATA_BACKUP]failed to push ls id", K(ret));
            }
          } else {
            LOG_WARN("[DATA_BACKUP]failed to check exist", K(ret), K(ls_id));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObHashMap<ObLSID, ObArray<ObTabletID>>::hashtable::const_iterator map_iter = tablet_to_ls_map.begin();
      for (; OB_SUCC(ret) && map_iter != tablet_to_ls_map.end(); ++map_iter) {
        share::ObBackupDataTabletToLSInfo ls_info;
        ls_info.ls_id_ = map_iter->first;
        if (map_iter->second.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[DATA_BACKUP]failed to get tablet to ls", K(ret), K(ls_info.ls_id_));
        } else if (OB_FAIL(append(ls_info.tablet_id_list_, map_iter->second))) {
          LOG_WARN("[DATA_BACKUP]failed to append tablet to ls array", K(ret));
        } else if (OB_FAIL(tablet_to_ls.push_back(ls_info))) {
          LOG_WARN("[DATA_BACKUP]failed to push backup ls info", K(ret));
        } 
      }
      ObHashSet<ObLSID>::const_iterator set_iter = new_ls_id_set.begin();
      for (; OB_SUCC(ret) && set_iter != new_ls_id_set.end(); ++set_iter) {
        const ObLSID &ls_id = set_iter->first;
        if (OB_FAIL(new_ls_ids.push_back(ls_id))) {
          LOG_WARN("[DATA_BACKUP]failed to push back ls id", K(ret));
        } 
      }
    } 

    if (OB_SUCC(ret)) {
      FLOG_INFO("[DATA_BACKUP]get new turn tablet_to_ls", K(tablet_to_ls));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::persist_deleted_tablets_info_(const common::ObIArray<ObBackupSkipTabletAttr> &skipped_tablets)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUCKET = 1024;
  ObHashMap<ObLSID, ObArray<ObTabletID> *> deleted_tablet_to_ls_map;
  if (OB_FAIL(deleted_tablet_to_ls_map.create(MAX_BUCKET, ObModIds::BACKUP))) {
    LOG_WARN("[DATA_BACKUP]failed to create map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < skipped_tablets.count(); ++i) {
      const ObBackupSkipTabletAttr &skip_tablet = skipped_tablets.at(i);
      const share::ObLSID &ls_id = skip_tablet.ls_id_;
      ObArray<ObTabletID> *tablet_ids_ptr = NULL;
      if (OB_FAIL(deleted_tablet_to_ls_map.get_refactored(ls_id, tablet_ids_ptr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_ISNULL(tablet_ids_ptr = OB_NEW(ObArray<ObTabletID>, ObModIds::BACKUP))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_ERROR("[DATA_BACKUP]failed to construct tablet id array", KR(ret));
          } else if (OB_FAIL(deleted_tablet_to_ls_map.set_refactored(ls_id, tablet_ids_ptr, 1/*overwrite*/))) {
            LOG_WARN("[DATA_BACKUP]failed to set refactored", K(ret), K(ls_id), KP(tablet_ids_ptr));
          }
          if (OB_FAIL(ret)) {
            OB_DELETE(ObArray<ObTabletID>, ObModIds::BACKUP, tablet_ids_ptr);
          }
        } else {
          LOG_WARN("[DATA_BACKUP]failed to get refactored", K(ret), K(skip_tablet));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tablet_ids_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[DATA_BACKUP]tmp tablet id array is null", K(ret));
      } else if (OB_FAIL(tablet_ids_ptr->push_back(skip_tablet.tablet_id_))) {
        LOG_WARN("[DATA_BACKUP]failed to push back", K(ret), K(skip_tablet));
      }
    }
  }
  if (OB_SUCC(ret)) {
    ObBackupDeletedTabletToLSDesc deleted_tablet_to_ls;
    ObHashMap<ObLSID, ObArray<ObTabletID> *>::hashtable::const_iterator map_iter = deleted_tablet_to_ls_map.begin();
    for (; OB_SUCC(ret) && map_iter != deleted_tablet_to_ls_map.end(); ++map_iter) {
      share::ObBackupDataTabletToLSInfo ls_info;
      ls_info.ls_id_ = map_iter->first;
      if (OB_ISNULL(map_iter->second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[DATA_BACKUP]map iter should not be null", K(ret), K(ls_info));
      } else if (map_iter->second->empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[DATA_BACKUP]failed to get tablet to ls", K(ret), K(ls_info));
      } else if (OB_FAIL(append(ls_info.tablet_id_list_, *map_iter->second))) {
        LOG_WARN("[DATA_BACKUP]failed to append tablet to ls array", K(ret));
      } else if (OB_FAIL(deleted_tablet_to_ls.deleted_tablet_to_ls_.push_back(ls_info))) {
        LOG_WARN("[DATA_BACKUP]failed to push backup ls info", K(ret));
      }
    }
    if (FAILEDx(store_.write_deleted_tablet_info(deleted_tablet_to_ls))) {
      LOG_WARN("[DATA_BACKUP]failed to write deleted tablet info", K(ret));
    } else {
      LOG_INFO("write deleted tablet info", K(deleted_tablet_to_ls));
    }
  }
  if (deleted_tablet_to_ls_map.created()) {
    ObHashMap<ObLSID, ObArray<ObTabletID> *>::hashtable::iterator del_map_iter = deleted_tablet_to_ls_map.begin();
    for (; del_map_iter != deleted_tablet_to_ls_map.end(); ++del_map_iter) {
      if (OB_NOT_NULL(del_map_iter->second)) {
        OB_DELETE(ObArray<ObTabletID>, ObModIds::BACKUP, del_map_iter->second);
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::construct_cur_ls_set_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks, 
    ObHashSet<ObLSID> &ls_id_set)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
    const ObLSID &ls_id = ls_tasks.at(i).ls_id_;
    if (!ls_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DATA_BACKUP]invalid ls", K(ret), K(ls_id), K(set_task_attr_));
    } else if (OB_FAIL(ls_id_set.set_refactored(ls_id))) {
      LOG_WARN("[DATA_BACKUP]failed to insert ls_id into ls id set", K(ret), K(ls_id));
    } 
  }
  return ret;
}

int ObBackupSetTaskMgr::update_task_type_(const ObIArray<ObBackupLSTaskAttr> &ls_tasks) 
{
  int ret = OB_SUCCESS;
  ObBackupDataTaskType type;
  
  switch(set_task_attr_.status_.status_) {
    case ObBackupStatus::Status::BACKUP_USER_META: {
      type.type_ = ObBackupDataTaskType::Type::BACKUP_DATA_SYS;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_SYS: {
      type.type_ = ObBackupDataTaskType::Type::BACKUP_DATA_MINOR;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_MINOR: {
      type.type_ = ObBackupDataTaskType::Type::BACKUP_DATA_MAJOR;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_MAJOR: {
      type.type_ = ObBackupDataTaskType::Type::BACKUP_PLUS_ARCHIVE_LOG;
      break;
    }
    default:
      break;
  }
  // because of reusing the same ls task record at different stages of backup, 
  // when changing task type, ls task need to reset some info.
  // 1. update ls task's task_type, turn_id, retry_id, status, and result.
  // 2. if type is BACKUP_DATA_SYS, BACKUP_DATA_MINOR, BACKUP_DATA_MAJOR, inserting new ls task info.
  // turn id is reset to the set_task_attr.data_turn_id, and retry_id is set to 0.
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
    const ObBackupLSTaskAttr &ls_task = ls_tasks.at(i);
    if (ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX == ls_task.task_type_.type_) {
    } else if (OB_FAIL(lease_service_->check_lease())) {
      LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
    } else if (OB_FAIL(ObBackupLSTaskOperator::update_task_type(trans_, ls_task, type))) {
      LOG_WARN("[DATA_BACKUP]failed to update task type", K(ret), K(ls_task));
    } else if (type.is_backup_data()) {
      share::ObBackupDataType backup_data_type;
      if (OB_FAIL(type.get_backup_data_type(backup_data_type))) {
        LOG_WARN("failed to get backup data type", K(ret), K(type));
      } else if (OB_FAIL(ObLSBackupOperator::insert_ls_backup_task_info(ls_task.tenant_id_,
          ls_task.task_id_, set_task_attr_.data_turn_id_, 0/*retry_id*/, ls_task.ls_id_, ls_task.backup_set_id_, backup_data_type, trans_))) {
        LOG_WARN("failed to insert ls backup task info", K(ret), K(ls_task), K(backup_data_type));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::deal_failed_set_task(ObMySQLTransaction &trans)
{
  // update all task status to finish
  // advance set task status to FAILED
  int ret = OB_SUCCESS;
  ObArray<ObBackupLSTaskAttr> ls_task;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DATA_BACKUP]not init", K(ret));
  } else {
    if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(
        trans, job_attr_->job_id_, job_attr_->tenant_id_, true/*update*/, ls_task))) {
      LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, 
          "tenant_id", job_attr_->tenant_id_);
    } else if (ls_task.empty()) {
    } else if (OB_FAIL(do_failed_ls_task_(trans, ls_task))) {
      LOG_WARN("[DATA_BACKUP]failed to do failed backup task", K(ret), K(ls_task));
    } 
    
    if (OB_FAIL(ret)) {
    } else {
      next_status_.status_ = ObBackupStatus::Status::FAILED;
      set_task_attr_.end_ts_ = job_attr_->end_ts_;
      if (OB_FAIL(enable_transfer_())) {
        LOG_WARN("[DATA_BACKUP]failed to enable transfer", K(ret));
      } else if (OB_FAIL(set_backup_set_files_failed_(trans))) {
        LOG_WARN("fail to set backup set files failed", K(ret));
      } else if (OB_FAIL(advance_status_(trans, next_status_, job_attr_->result_, set_task_attr_.end_scn_, 
          job_attr_->end_ts_))) {
        LOG_WARN("[DATA_BACKUP]failed to advance set task status to FAILED", K(ret), K(set_task_attr_), 
        "failed job result", job_attr_->result_);
      } 
    }
  }
  

  return ret;
}

bool ObBackupSetTaskMgr::is_force_cancel() const
{
  return ObBackupStatus::CANCELING == set_task_attr_.status_.status_;
}

int ObBackupSetTaskMgr::do_failed_ls_task_(ObMySQLTransaction &trans, const ObIArray<ObBackupLSTaskAttr> &ls_task) 
{
  int ret = OB_SUCCESS;
  ObBackupTaskStatus next_status;
  next_status.status_ = ObBackupTaskStatus::Status::FINISH;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
    const ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
    if (OB_FAIL(ObBackupDataLSTaskMgr::advance_status(
        *lease_service_, trans, ls_attr, next_status, ls_attr.result_, job_attr_->end_ts_))) {
      LOG_WARN("[DATA_BACKUP]failed to advance ls task status", K(ret), K(ls_attr), K(next_status));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::backup_completing_log_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupLSTaskAttr> ls_task;
  ObTenantArchiveRoundAttr round_attr;
  int64_t finish_cnt = 0;
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, true/*update*/, ls_task))) {
    LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]no lostream task", K(ret), K(ls_task));
  } else if (job_attr_->plus_archivelog_) {
    if (OB_FAIL(ObTenantArchiveMgr::get_tenant_current_round(job_attr_->tenant_id_, job_attr_->incarnation_id_, round_attr))) {
      LOG_WARN("[DATA_BACKUP]failed to get tenant current round", K(ret));
    } else if (!round_attr.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[DATA_BACKUP]invalid round attr", K(ret));
    } else if (round_attr.checkpoint_scn_ < set_task_attr_.end_scn_) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_WARN("[DATA_BACKUP]log archive checkpoint is less than task's end scn, try later", "checkpoint_scn", round_attr.checkpoint_scn_, 
          "end_scn", set_task_attr_.end_scn_);
      }
    } else if (OB_FAIL(do_backup_completing_log_(ls_task, finish_cnt))) {
      LOG_WARN("[DATA_BACKUP]failed to do backup ls task", K(ret), K(set_task_attr_), K(ls_task));
    } 
  } else {
    finish_cnt = ls_task.count();
  }
  
  if (OB_SUCC(ret) && ls_task.count() == finish_cnt) {
    next_status_.status_ = ObBackupStatus::Status::COMPLETED;
    set_task_attr_.end_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(write_extern_infos())) {
      LOG_WARN("fail to write_extern_infos", K(ret), K(set_task_attr_));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      if (OB_FAIL(advance_status_(trans_, next_status_, OB_SUCCESS, set_task_attr_.end_scn_, set_task_attr_.end_ts_))) {
        LOG_WARN("fail to advance status to COMPLETED", K(ret), K(set_task_attr_));
      } 
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("backup_data", "backup completing log succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          LOG_INFO("backup completing log succeed", "tenant_id", job_attr_->tenant_id_, "job_id", job_attr_->job_id_,
              "task_id", set_task_attr_.task_id_);
          backup_service_->wakeup();
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
          LOG_WARN("fail to rollback", KR(ret), K(tmp_ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_backup_completing_log_(ObArray<ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt)
{
  int ret = OB_SUCCESS;
  finish_cnt = 0;
  if (ls_task.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_task));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
      ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
      ObBackupDataLSTaskMgr ls_task_mgr;
      if (OB_FAIL(ls_task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *lease_service_, 
          *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task advancer", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to process log stream task", K(ret), K(ls_attr));
      } 
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_clean_up(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObBackupSkippedType skipped_type(ObBackupSkippedType::DELETED);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DATA_BACKUP]not init", K(ret));
  } else if (OB_FAIL(lease_service_->check_lease())) {
    LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
  } else if (OB_FAIL(ObBackupLSTaskInfoOperator::move_ls_task_info_to_his(trans, set_task_attr_.task_id_, 
      set_task_attr_.tenant_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move task to history", K(ret), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupLSTaskOperator::move_ls_to_his(trans, set_task_attr_.tenant_id_, set_task_attr_.job_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move ls to history", K(ret), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupTaskOperator::move_task_to_his(trans, set_task_attr_.tenant_id_, set_task_attr_.job_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move task to history", K(ret), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupSkippedTabletOperator::move_skip_tablet_to_his(
      trans, set_task_attr_.tenant_id_, set_task_attr_.task_id_, skipped_type))) {
    LOG_WARN("[DATA_BACKUP]failed to move skip tablet to history", K(ret), K(set_task_attr_));
  }
  return ret;
}

int ObBackupSetTaskMgr::do_cancel_()
{
  // step1: get all ls tasks
  // step2: do different operations according to the different status of task
  // step3: if all task status are FINISH and results are OB_CANCELED , push job status to FINISH and result to OB_CANCELED;

  int ret = OB_SUCCESS;
  int64_t finish_cnt = 0;
  ObArray<ObBackupLSTaskAttr> ls_task;
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, true/*for update*/, ls_task))) {
    LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
      ObBackupDataLSTaskMgr task_mgr;
      ObBackupLSTaskAttr ls_attr = ls_task.at(i);
      if (OB_FAIL(task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *lease_service_, *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task mgr", K(ret), K(ls_attr));
      } else if (OB_FAIL(task_mgr.cancel(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to cancel task", K(ret), K(ls_attr), K(set_task_attr_));
      }
    }
  }
  if (OB_SUCC(ret) && ls_task.count() == finish_cnt) {
    next_status_.status_ = ObBackupStatus::Status::CANCELED;
    set_task_attr_.end_ts_ = ObTimeUtility::current_time();
    if (!is_force_cancel() && OB_FAIL(write_extern_infos())) {
      LOG_WARN("[DATA_BACKUP]failed to write extern infos", K(ret), K(set_task_attr_));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      if (OB_FAIL(set_backup_set_files_failed_(trans_))) {
        LOG_WARN("failed to set backup set files failed", K(ret));
      } else if (OB_FAIL(advance_status_(trans_, next_status_, OB_CANCELED, set_task_attr_.end_scn_, set_task_attr_.end_ts_))) {
        LOG_WARN("[DATA_BACKUP]failed to advance set task status to CANCELED", K(ret), K(set_task_attr_));
      } else {
        FLOG_INFO("[DATA_BACKUP]advance set status success", K(next_status_));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } else {
          ROOTSERVICE_EVENT_ADD("backup_data", "cancel backup succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          backup_service_->wakeup();
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans_.end(false))) {
          LOG_WARN("fail to rollback", KR(ret), K(tmp_ret));
        }
      }
    }
  }
  return OB_SUCCESS;
}


int ObBackupSetTaskMgr::write_extern_infos()
{ 
  int ret = OB_SUCCESS;
  HEAP_VARS_2((ObExternTenantLocalityInfoDesc, locality_info),
      (ObExternBackupSetInfoDesc, backup_set_info)) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(write_extern_locality_info_(locality_info))) {
      LOG_WARN("[DATA_BACKUP]failed to write extern tenant locality info", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_backup_set_info_(set_task_attr_, backup_set_info))) { 
      LOG_WARN("[DATA_BACKUP]failed to write backup set info", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_tenant_backup_set_infos_())) {
      LOG_WARN("[DATA_BACKUP]failed to write tenant backup set infos", K(ret));
    } else if (OB_FAIL(write_extern_diagnose_info_(locality_info, backup_set_info))) { // 
      LOG_WARN("[DATA_BACKUP]failed to write extern tenant diagnose info", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_backup_set_placeholder(false/*finish job*/))) {
      LOG_WARN("[DATA_BACKUP]failed to write backup set finish placeholder", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_deleted_tablet_infos_())) {
      LOG_WARN("[DATA_BACKUP]failed to write deleted tablet infos", K(ret), KPC(job_attr_));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::write_backup_set_info_(
    const ObBackupSetTaskAttr &set_task_attr, 
    ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = 0;
  uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  ObBackupSetFileDesc &backup_set_file = backup_set_info.backup_set_file_;
  ObBackupDest backup_dest;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, job_attr_->tenant_id_, set_task_attr.backup_path_, backup_dest))) {
    LOG_WARN("[DATA_BACKUP]fail to get backup dest", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, job_attr_->tenant_id_, backup_dest, dest_id))) {
    LOG_WARN("[DATA_BACKUP]failed to get dest id", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(*sql_proxy_, false/*for update*/, job_attr_->backup_set_id_, job_attr_->incarnation_id_, 
      job_attr_->tenant_id_, dest_id, backup_set_file))) {
    LOG_WARN("[DATA_BACKUP]failed to get backup set", K(ret), KPC(job_attr_));
  } else if (cluster_version != backup_set_file.tenant_compatible_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("[DATA_BACKUP]when cluster_version change, backup can't continue", K(ret), K(cluster_version), K(backup_set_file.tenant_compatible_));
  } else {
    backup_set_file.backup_set_id_ = job_attr_->backup_set_id_;
    backup_set_file.incarnation_ = job_attr_->incarnation_id_;
    backup_set_file.tenant_id_ = job_attr_->tenant_id_;
    backup_set_file.dest_id_ = dest_id;
    backup_set_file.stats_.assign(set_task_attr.stats_);
    backup_set_file.end_time_ = set_task_attr.end_ts_;
    backup_set_file.status_ = OB_SUCCESS == job_attr_->result_? 
        ObBackupSetFileDesc::BackupSetStatus::SUCCESS : ObBackupSetFileDesc::BackupSetStatus::FAILED;
    backup_set_file.result_ = job_attr_->result_;
    backup_set_file.file_status_ = OB_SUCCESS == job_attr_->result_ ? 
        ObBackupFileStatus::BACKUP_FILE_AVAILABLE : ObBackupFileStatus::BACKUP_FILE_BROKEN;
    backup_set_file.data_turn_id_ = set_task_attr.data_turn_id_;
    backup_set_file.meta_turn_id_ = set_task_attr.meta_turn_id_;
    backup_set_file.min_restore_scn_ = set_task_attr.end_scn_;
    if (OB_FAIL(calculate_start_replay_scn_(backup_set_file.start_replay_scn_))) {
      LOG_WARN("fail to calculate start replay scn", K(ret));
    } else if (OB_FAIL(store_.write_backup_set_info(backup_set_info))) {
      LOG_WARN("[DATA_BACKUP]failed to write backup set start place holder", K(ret), K(backup_set_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lease_service_->check_lease())) {
    LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file(*sql_proxy_, backup_set_info.backup_set_file_))) {
    LOG_WARN("[DATA_BACKUP]failed to update backup set", K(ret), K(backup_set_info));
  } 
  return ret;
}

int ObBackupSetTaskMgr::write_deleted_tablet_infos_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupSkipTabletAttr> tablet_attrs;
  ObBackupSkippedType skipped_type(ObBackupSkippedType::DELETED);
  if (OB_FAIL(ObBackupSkippedTabletOperator::get_skip_tablet(*sql_proxy_, false/*lock*/,
      set_task_attr_.tenant_id_, set_task_attr_.task_id_, skipped_type, tablet_attrs))) {
    LOG_WARN("[DATA_BACKUP]failed to get skip tablet", K(ret), "teannt_id", set_task_attr_.tenant_id_, "task_id", set_task_attr_.task_id_);
  } else if (tablet_attrs.empty()) {
    LOG_INFO("[DATA_BACKUP]no change turn tablets found", K(ret));
  } else if (OB_FAIL(persist_deleted_tablets_info_(tablet_attrs))) {
    LOG_WARN("[DATA_BACKUP]failed to do persist deleted tablets info", K(ret), K(tablet_attrs), K(set_task_attr_));
  }
  return ret;
}

int ObBackupSetTaskMgr::calculate_start_replay_scn_(SCN &start_replay_scn)
{
  int ret = OB_SUCCESS;
  ObBackupLSMetaInfosDesc ls_meta_infos;
  ObTenantArchiveRoundAttr round_attr;
  if (OB_FAIL(ObTenantArchiveMgr::get_tenant_current_round(job_attr_->tenant_id_, job_attr_->incarnation_id_, round_attr))) {
    LOG_WARN("failed to get tenant current round", K(ret), KPC(job_attr_));
  } else if (!round_attr.state_.is_doing()) {
    ret = OB_LOG_ARCHIVE_NOT_RUNNING;
    LOG_WARN("backup is not supported when log archive is not doing", K(ret), K(round_attr));
  } else if (round_attr.start_scn_ > set_task_attr_.start_scn_) {
    ret = OB_LOG_ARCHIVE_INTERRUPTED;
    LOG_WARN("backup is not supported when archive is interrupted", K(ret), K(round_attr), K(set_task_attr_.start_scn_));
  } else if (OB_FAIL(store_.read_ls_meta_infos(ls_meta_infos))) {
    LOG_WARN("fail to read ls meta infos", K(ret));
  } else {
    // To ensure that restore can be successfully initiated,
    // we need to avoid clearing too many logs and the start_replay_scn less than the start_scn of the first piece.
    // so we choose the minimum palf_base_info.prev_log_info_.scn firstly, to ensure keep enough logs.
    // Then we choose the max(minimum palf_base_info.prev_log_info_.scn, round_attr.start_scn) as the start_replay_scn,
    // to ensure the start_replay_scn is greater than the start scn of first piece
    SCN tmp_start_replay_scn = set_task_attr_.start_scn_;
    ARRAY_FOREACH_X(ls_meta_infos.ls_meta_packages_, i, cnt, OB_SUCC(ret)) {
      const palf::PalfBaseInfo &palf_base_info = ls_meta_infos.ls_meta_packages_.at(i).palf_meta_;
      tmp_start_replay_scn = SCN::min(tmp_start_replay_scn, palf_base_info.prev_log_info_.scn_);
    }
    if (OB_SUCC(ret)) {
      start_replay_scn = SCN::max(tmp_start_replay_scn, round_attr.start_scn_);
      LOG_INFO("calculate start replay scn finish", K(start_replay_scn), K(ls_meta_infos), K(round_attr));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::write_extern_locality_info_(ObExternTenantLocalityInfoDesc &locality_info)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_info = NULL;
  if (OB_FAIL(schema_service_->get_tenant_schema_guard(job_attr_->tenant_id_, schema_guard))) {
    LOG_WARN("[DATA_BACKUP]failed to get_tenant_schema_guard", KR(ret), "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(schema_guard.get_tenant_info(job_attr_->tenant_id_, tenant_info))) {
    LOG_WARN("[DATA_BACKUP]failed to get tenant info", K(ret), "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(locality_info.tenant_name_.assign(tenant_info->get_tenant_name()))) {
    LOG_WARN("[DATA_BACKUP]failed to assign tenant name", K(ret), K(tenant_info));
  } else if (OB_FAIL(locality_info.locality_.assign(tenant_info->get_locality()))) {
    LOG_WARN("[DATA_BACKUP]failed to assign locality info", K(ret), K(tenant_info));
  } else if (OB_FAIL(locality_info.primary_zone_.assign(tenant_info->get_primary_zone()))) {
    LOG_WARN("[DATA_BACKUP]failed to assign primary zone", K(ret), K(tenant_info));
  } else if (OB_FAIL(locality_info.cluster_name_.assign(GCONF.cluster))) {
    LOG_WARN("fail to assign cluster name", K(ret));
  } else {
    locality_info.tenant_id_ = job_attr_->tenant_id_;
    locality_info.backup_set_id_ = job_attr_->backup_set_id_;
    locality_info.cluster_id_ = GCONF.cluster_id;
    locality_info.compat_mode_ = static_cast<lib::Worker::CompatMode>(tenant_info->get_compatibility_mode());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(store_.write_tenant_locality_info(locality_info))) {
    LOG_WARN("[DATA_BACKUP]failed to write backup set start place holder", K(ret), K(locality_info));
  } 
  return ret;
}

int ObBackupSetTaskMgr::write_extern_diagnose_info_(
    const ObExternTenantLocalityInfoDesc &locality_info, 
    const ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  if (!locality_info.is_valid() || !backup_set_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(locality_info), K(backup_set_info));
  } else {
    HEAP_VAR(ObExternTenantDiagnoseInfoDesc, diagnose_info) {
      diagnose_info.tenant_id_ = job_attr_->tenant_id_;
      diagnose_info.tenant_locality_info_ = locality_info;
      diagnose_info.backup_set_file_ = backup_set_info.backup_set_file_;
      if (OB_FAIL(store_.write_tenant_diagnose_info(diagnose_info))) {
        LOG_WARN("[DATA_BACKUP]failed to write teannt diagnose info", K(ret), K(diagnose_info));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::write_backup_set_placeholder(const bool is_start) 
{
  int ret = OB_SUCCESS;
  SCN start_replay_scn = job_attr_->plus_archivelog_ ? set_task_attr_.end_scn_ : set_task_attr_.start_scn_;
  SCN min_restore_scn = set_task_attr_.end_scn_;
  bool is_inner = is_start ? false : true;
  bool is_success = OB_SUCCESS == job_attr_->result_ ? true : false;
  if (is_start) {
    if (OB_FAIL(store_.write_backup_set_placeholder(is_inner, is_start, is_success, start_replay_scn, min_restore_scn))) {
      LOG_WARN("[DATA_BACKUP]failed to write backup set start place holder", K(ret));
    } 
  } else if (OB_FAIL(store_.write_backup_set_placeholder(is_inner, is_start, is_success, start_replay_scn, min_restore_scn))) {
    LOG_WARN("[DATA_BACKUP]failed to write backup set inner place holder", K(ret));
  } else if (OB_FALSE_IT(is_inner = false)) {
  } else if (OB_FAIL(store_.write_backup_set_placeholder(is_inner, is_start, is_success, start_replay_scn, min_restore_scn))) {
    LOG_WARN("[DATA_BACKUP]failed to write backup set finish place holder", K(ret));
  }
  return ret;
}

int ObBackupSetTaskMgr::write_tenant_backup_set_infos_()
{
  int ret = OB_SUCCESS;
  share::ObTenantBackupSetInfosDesc tenant_backup_set_infos;
  if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_files(*sql_proxy_, job_attr_->tenant_id_, tenant_backup_set_infos))) {
    LOG_WARN("[DATA_BACKUP]failed to get backup set", K(ret), KPC(job_attr_));
  } else if (!tenant_backup_set_infos.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant backup set infos", K(ret), K(tenant_backup_set_infos));
  } else if (OB_FAIL(store_.write_tenant_backup_set_infos(tenant_backup_set_infos))) {
    LOG_WARN("failed to write tenant backup set infos", K(ret));
  }
  return ret;
}

bool ObBackupSetTaskMgr::can_write_extern_infos(const int err) const
{
  return !is_force_cancel() && err != OB_BACKUP_DEVICE_OUT_OF_SPACE && err != OB_IO_ERROR;
}

int ObBackupSetTaskMgr::set_backup_set_files_failed_(ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = 0;
  ObBackupSetFileDesc backup_set_file;
  ObBackupDest backup_dest;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(trans, job_attr_->tenant_id_, set_task_attr_.backup_path_, backup_dest))) {
    LOG_WARN("fail to get backup dest", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(trans, job_attr_->tenant_id_, backup_dest, dest_id))) {
    LOG_WARN("failed to get dest id", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(trans, true/*for update*/, job_attr_->backup_set_id_, job_attr_->incarnation_id_, 
      job_attr_->tenant_id_, dest_id, backup_set_file))) {
    LOG_WARN("failed to get backup set", K(ret), KPC(job_attr_));
  } else {
    backup_set_file.backup_set_id_ = job_attr_->backup_set_id_;
    backup_set_file.incarnation_ = job_attr_->incarnation_id_;
    backup_set_file.tenant_id_ = job_attr_->tenant_id_;
    backup_set_file.dest_id_ = dest_id;
    backup_set_file.stats_.assign(set_task_attr_.stats_);
    backup_set_file.end_time_ = set_task_attr_.end_ts_;
    backup_set_file.status_ = ObBackupSetFileDesc::BackupSetStatus::FAILED;
    backup_set_file.result_ = job_attr_->result_;
    backup_set_file.file_status_ = ObBackupFileStatus::BACKUP_FILE_INCOMPLETE;
    backup_set_file.data_turn_id_ = set_task_attr_.data_turn_id_;
    backup_set_file.meta_turn_id_ = set_task_attr_.meta_turn_id_;
    backup_set_file.min_restore_scn_ = set_task_attr_.end_scn_;
    if (OB_FAIL(lease_service_->check_lease())) {
      LOG_WARN("failed to check lease", K(ret));
    } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file(trans, backup_set_file))) {
      LOG_WARN("failed to update backup set", K(ret), K(backup_set_file));
    } 
  }
  return ret;
}
