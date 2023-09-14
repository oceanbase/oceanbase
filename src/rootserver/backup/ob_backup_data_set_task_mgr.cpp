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
#include "ob_backup_task_scheduler.h"
#include "storage/tx/ob_ts_mgr.h"
#include "rootserver/ob_root_utils.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "observer/ob_sql_client_decorator.h"
#include "storage/ls/ob_ls.h"
#include "share/ls/ob_ls_operator.h"
#include "rootserver/backup/ob_backup_service.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_extern_info_mgr.h"
#include "share/ls/ob_ls_table_operator.h"
#include "storage/backup/ob_backup_data_store.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/backup/ob_backup_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/ob_tenant_info_proxy.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/backup/ob_backup_server_mgr.h"

using namespace oceanbase;
using namespace omt;
using namespace common::hash;
using namespace share;
using namespace rootserver;
using namespace backup;

ObBackupSetTaskMgr::ObBackupSetTaskMgr()
 : is_inited_(false),
   meta_tenant_id_(0),
   set_task_attr_(),
   job_attr_(nullptr),
   sql_proxy_(nullptr),
   rpc_proxy_(nullptr),
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
    ObMultiVersionSchemaService &schema_service,
    ObBackupDataService &backup_service)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[DATA_BACKUP]init twice", K(ret));
  } else if (!job_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(job_attr));
  } else if (OB_FAIL(ObBackupTaskOperator::get_backup_task(sql_proxy, job_attr.job_id_, job_attr.tenant_id_, false/*for update*/,
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
    task_scheduler_ = &task_scheduler;
    schema_service_ = &schema_service;
    backup_service_ = &backup_service;
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
  if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("[DATA_BACKUP]failed to check leader", K(ret));
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
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_PERSIST_LS_FAILED) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(write_backup_set_placeholder_(true/*start*/))) {
    LOG_WARN("fail to write backup set start placeholder", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
  } else {
    ObBackupStatus next_status = ObBackupStatus::BACKUP_SYS_META;
    if (OB_FAIL(do_persist_sys_ls_task_())) {
      LOG_WARN("fail to do persist ls tasks", K(ret));
    } else if (OB_FAIL(advance_status_(trans_, next_status))) {
      LOG_WARN("fail to advance status to backup sys meta", K(ret), K(next_status));
    } 
    if (OB_SUCC(ret)) {
      if (OB_FAIL(trans_.end(true))) {
        LOG_WARN("failed to commit trans", KR(ret));
      } else {
        set_task_attr_.status_ = next_status;
        ROOTSERVICE_EVENT_ADD("backup_data", "persist sys ls task succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
        LOG_INFO("[BACKUP_DATA]succeed persit sys ls task", K(ret), K(set_task_attr_));
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

int ObBackupSetTaskMgr::persist_ls_attr_info_(const share::ObBackupLSTaskAttr &sys_ls_task, ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataLSAttrDesc ls_attr_desc;
  ObLSAttrOperator ls_attr_operator(set_task_attr_.tenant_id_, sql_proxy_);
  bool ls_attr_info_exist = false;
  if (OB_FAIL(store_.read_ls_attr_info(set_task_attr_.meta_turn_id_, ls_attr_desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      if (OB_FAIL(sync_wait_backup_user_ls_scn_(sys_ls_task, ls_attr_desc.backup_scn_))) {
        LOG_WARN("failed to calc backup user ls scn", K(ret));
      } else if (OB_FAIL(ls_attr_operator.load_all_ls_and_snapshot(ls_attr_desc.backup_scn_, ls_attr_desc.ls_attr_array_))) {
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

int ObBackupSetTaskMgr::sync_wait_backup_user_ls_scn_(const share::ObBackupLSTaskAttr &sys_ls_task, share::SCN &scn)
{
  // user ls scn must newer than backup sys ls clog checkpoint scn.
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupSetDesc desc;
  desc.backup_set_id_ = job_attr_->backup_set_id_;
  desc.backup_type_ = job_attr_->backup_type_;
  backup::ObExternLSMetaMgr ls_meta_mgr;
  ObBackupLSMetaInfo ls_meta;
  int64_t sys_ls_turn_id = 1;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, job_attr_->tenant_id_,
    job_attr_->backup_path_, backup_dest))) {
    LOG_WARN("fail to get backup dest", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ls_meta_mgr.init(backup_dest, desc, sys_ls_task.ls_id_, sys_ls_task.turn_id_, sys_ls_task.retry_id_))) {
    LOG_WARN("failed to init ls meta mgr", K(ret), K(backup_dest), K(desc), K(sys_ls_task));
  } else if (OB_FAIL(ls_meta_mgr.read_ls_meta_info(ls_meta))) {
    LOG_WARN("failed to read ls meta info", K(ret));
  } else {
    share::SCN tmp_scn(SCN::min_scn());
    int64_t abs_timeout = ObTimeUtility::current_time() + 10 * 60 * 1000 * 1000;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ObBackupDataScheduler::get_backup_scn(*sql_proxy_, set_task_attr_.tenant_id_, true, tmp_scn))) {
        LOG_WARN("failed to get backup scn", K(ret));
      } else if (tmp_scn >= ls_meta.ls_meta_package_.ls_meta_.get_clog_checkpoint_scn()) {
        scn = tmp_scn;
        LOG_INFO("succeed get backup user ls scn", K(scn), K(ls_meta));
        break;
      } else if (ObTimeUtility::current_time() > abs_timeout) {
        ret = OB_TIMEOUT;
        break;
      } else if (OB_FAIL(backup_service_->check_leader())) {
        LOG_WARN("failed to check leader", K(ret));
      }
    }
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
      new_ls_attr.start_turn_id_ = 1;
      new_ls_attr.retry_id_ = 0;
      new_ls_attr.result_ = OB_SUCCESS;
      new_ls_attr.max_tablet_checkpoint_scn_.set_min();

      share::ObBackupDataType backup_data_type;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(calc_task_turn_(new_ls_attr.task_type_, new_ls_attr.turn_id_))) {
        LOG_WARN("failed to calc ls task turn id", K(ret));
      } else if (OB_FAIL(backup_service_->check_leader())) {
        LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret), K(set_task_attr_));
      } else if (OB_FAIL(ObBackupLSTaskOperator::insert_ls_task(trans_, new_ls_attr))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[DATA_BACKUP]failed to insert backup log stream task", K(ret), K(new_ls_attr));
        }
      } else if (!type.is_backup_data()) {
      } else if (OB_FAIL(type.get_backup_data_type(backup_data_type))) {
        LOG_WARN("failed to get backup data type", K(ret), K(type));
      } else if (OB_FAIL(ObLSBackupOperator::insert_ls_backup_task_info(new_ls_attr.tenant_id_,
          new_ls_attr.task_id_, new_ls_attr.turn_id_, new_ls_attr.retry_id_, new_ls_attr.ls_id_,
          new_ls_attr.backup_set_id_, backup_data_type, trans_))) {
        LOG_WARN("failed to insert ls backup task info", K(ret), K(new_ls_attr), K(backup_data_type));
      } 
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::calc_task_turn_(const ObBackupDataTaskType &type, int64_t &turn_id)
{
  int ret = OB_SUCCESS;
  turn_id = 0;
  switch(set_task_attr_.status_) {
    case ObBackupStatus::INIT:
    case ObBackupStatus::BACKUP_SYS_META: {
      if (type.is_backup_meta()) {
        turn_id = 1;
      }
      break;
    }
    case ObBackupStatus::BACKUP_USER_META: {
      if (type.is_backup_meta()) {
        turn_id = set_task_attr_.meta_turn_id_;
      } else if (type.is_backup_minor()) {
        turn_id = set_task_attr_.minor_turn_id_;
      }
      break;
    }
    case ObBackupStatus::BACKUP_DATA_MINOR: {
      if (type.is_backup_minor() || type.is_backup_index()) {
        turn_id = set_task_attr_.minor_turn_id_;
      }
      break;
    }
    case ObBackupStatus::BACKUP_DATA_MAJOR: {
      if (type.is_backup_major() || type.is_backup_index()) {
        turn_id = set_task_attr_.major_turn_id_;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate ls task in unmatched status", K(set_task_attr_));
      break;
    }
  }

  if (OB_SUCC(ret) && 0 == turn_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc task turn meets unexpected set status and ls task type", K(ret), K(set_task_attr_), K(type));
  }
  return ret;
}

int ObBackupSetTaskMgr::backup_sys_meta_()
{
  int ret = OB_SUCCESS;
  ObArray<ObBackupLSTaskAttr> ls_task;
  int64_t finish_cnt = 0;
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, false/*update*/, ls_task))) {
    LOG_WARN("fail to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no logstream task", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(do_backup_meta_(ls_task, finish_cnt))) {
    LOG_WARN("fail to do backup meta", K(ret), K(ls_task));
  } else if (ls_task.count() == finish_cnt) {
    ObArray<share::ObLSID> ls_ids;
    if (OB_FAIL(persist_ls_attr_info_(ls_task.at(0), ls_ids))) {
       LOG_WARN("fail to do persist ls task", K(ret));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      ObBackupStatus next_status = ObBackupStatus::BACKUP_USER_META;
      share::ObBackupDataTaskType type(share::ObBackupDataTaskType::Type::BACKUP_META);
      if (OB_FAIL(generate_ls_tasks_(ls_ids, type))) {
        LOG_WARN("failed to generate ls tasks", K(ret), K(ls_ids), K(type));
      } else if (OB_FAIL(advance_status_(trans_, next_status))) {
        LOG_WARN("fail to advance status to backup advance checkpoint", K(ret), K(next_status));
      } 

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } else {
          set_task_attr_.status_ = next_status;
          ROOTSERVICE_EVENT_ADD("backup_data", "backup sys ls meta succeed", "tenant_id",
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
    LOG_WARN("[DATA_BACKUP]no logstream task", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(do_backup_meta_(ls_task, finish_cnt))) {
    LOG_WARN("[DATA_BACKUP]failed to do backup meta", K(ret), K(ls_task));
  } else if (OB_FAIL(do_backup_root_key_())) {
    LOG_WARN("[DATA_BACKUP]failed to do backup root key", K(ret));
  } else if (ls_task.count() == finish_cnt) {
    ROOTSERVICE_EVENT_ADD("backup_data", "before_backup_data");
    share::SCN consistent_scn;
    bool need_change_meta_turn = false;
    if (OB_FAIL(check_need_change_meta_turn_(ls_task, need_change_meta_turn))) {
      LOG_WARN("failed to check need change meta turn", K(ret), K(ls_task));
    } else if (need_change_meta_turn) {
      const ObBackupLSTaskAttr &sys_ls_task = ls_task.at(0);
      if (OB_FAIL(change_meta_turn_(sys_ls_task))) {
        LOG_WARN("failed to change meta turn", K(ret));
      }
    } else if (OB_FAIL(calc_consistent_scn_(ls_task, consistent_scn))) {
      LOG_WARN("failed to calc consistent scn", K(ret), K(ls_task));
    } else if (OB_FAIL(merge_ls_meta_infos_(ls_task))) {
      LOG_WARN("fail to merge ls meta infos", K(ret), K(ls_task));
    } else if (OB_FAIL(merge_tablet_to_ls_info_(consistent_scn, ls_task))) {
      LOG_WARN("[DATA_BACKUP]failed to merge tablet to ls info", K(ret), K(ls_task));
    } else if (OB_FALSE_IT(DEBUG_SYNC(BEFORE_BACKUP_DATA))) {
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      ObBackupStatus next_status = ObBackupStatus::BACKUP_DATA_MINOR;
      if (OB_FAIL(convert_task_type_(ls_task))) {
        LOG_WARN("[DATA_BACKUP]fail to update task type to backup data", K(ret));
      } else if (OB_FAIL(advance_status_(trans_, next_status))) {
        LOG_WARN("[DATA_BACKUP]failed to advance status to BACKUP_DATA_MINOR", K(ret), K(next_status));
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

int ObBackupSetTaskMgr::calc_consistent_scn_(ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, share::SCN &consistent_scn)
{
  int ret = OB_SUCCESS;
  consistent_scn.set_min();
  // let consistent_scn be the biggest max_tablet_checkpoint_scn_ of all the ls and the cur gts.
  if (OB_FAIL(ObBackupDataScheduler::get_backup_scn(*sql_proxy_, job_attr_->tenant_id_, true, consistent_scn))) {
    LOG_WARN("failed to get backup scn", K(ret), "tenant_id", job_attr_->tenant_id_);
  }
  ARRAY_FOREACH(ls_tasks, i) {
    const ObBackupLSTaskAttr &task = ls_tasks.at(i);
    consistent_scn = MAX(consistent_scn, task.max_tablet_checkpoint_scn_);
  }
  return ret;
}

int ObBackupSetTaskMgr::check_need_change_meta_turn_(ObIArray<share::ObBackupLSTaskAttr> &ls_tasks, bool &need_change_turn)
{
  // if deleting ls when backup user meta, the deleted ls may not has ls meta backup.
  // if ls included in ls_attr has no ls meta, restore scheduler will create ls failed.
  // so when deleting ls happended, we need to change meta turn and backup ls_attr and ls_meta in new turn
  int ret = OB_SUCCESS;
  need_change_turn = false;
  ARRAY_FOREACH_X(ls_tasks, i, cnt, OB_SUCC(ret) && !need_change_turn) {
    const ObBackupLSTaskAttr &ls_task = ls_tasks.at(i);
    if (!ls_task.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ls task", K(ret), K(ls_task));
    } else {
      need_change_turn = OB_LS_NOT_EXIST == ls_task.result_;
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::change_meta_turn_(const share::ObBackupLSTaskAttr &sys_ls_task)
{
  int ret = OB_SUCCESS;
  const int64_t next_meta_turn_id = ++set_task_attr_.meta_turn_id_;
  ObArray<share::ObLSID> ls_ids;
  if (OB_FAIL(persist_ls_attr_info_(sys_ls_task, ls_ids))) {
    LOG_WARN("failed to do persist ls task", K(ret));
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("failed to start trans", K(ret), K(meta_tenant_id_));
  } else {
    share::ObBackupDataTaskType type(share::ObBackupDataTaskType::BACKUP_META);
    if (OB_FAIL(ObBackupLSTaskOperator::delete_ls_task_without_sys(trans_, set_task_attr_.tenant_id_, 
        set_task_attr_.task_id_))) {
      LOG_WARN("fail to delete ls task", K(ret), "tenant_id", set_task_attr_.tenant_id_, "job_id", 
          set_task_attr_.job_id_);
    } else if (OB_FAIL(generate_ls_tasks_(ls_ids, type))) {
      LOG_WARN("failed to generate log stream tasks", K(ret), KPC(job_attr_), K(ls_ids));
    } else if (OB_FAIL(ObBackupTaskOperator::update_turn_id(trans_, set_task_attr_.status_, set_task_attr_.task_id_,
        set_task_attr_.tenant_id_, next_meta_turn_id))) {
      LOG_WARN("failed to update meta turn id", K(ret), K(set_task_attr_));
    }

    if (trans_.is_started()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans_.end(OB_SUCC(ret)))) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("change meta turn", K(ret), K(next_meta_turn_id), KPC(job_attr_));
        backup_service_->wakeup();
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::get_backup_user_meta_task_(ObIArray<share::ObBackupLSTaskAttr> &ls_task)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObBackupLSTaskAttr> tmp_ls_task;
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, false/*update*/, tmp_ls_task))) {
    LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else {
    ARRAY_FOREACH(tmp_ls_task, i) {
      const ObBackupLSTaskAttr &task = tmp_ls_task.at(i);
      if (task.task_type_.is_backup_meta()) {
        if (OB_FAIL(ls_task.push_back(task))) {
          LOG_WARN("failed to push back", K(ret), K(task));
        }
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::merge_ls_meta_infos_(
    const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupSetDesc desc;
  desc.backup_set_id_ = job_attr_->backup_set_id_;
  desc.backup_type_ = job_attr_->backup_type_;
  storage::ObBackupLSMetaInfosDesc ls_meta_infos;
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
        LOG_WARN("invalid ls meta", K(ret), K(ls_meta));
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

int ObBackupSetTaskMgr::merge_tablet_to_ls_info_(const share::SCN &consistent_scn, const ObIArray<ObBackupLSTaskAttr> &ls_tasks)
{
  int ret = OB_SUCCESS;
  ObHashMap<ObLSID, ObArray<ObTabletID>> latest_ls_tablet_map;
  ObHashMap<ObLSID, const ObBackupLSTaskAttr *> backup_ls_map; // the ls task persisted in __all_backup_ls_task
  ObArray<share::ObLSID> ls_ids;
  const int64_t OB_BACKUP_MAX_LS_BUCKET = 1024;
  SCN max_backup_scn;
  if (ls_tasks.empty() || !consistent_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_tasks), K(consistent_scn));
  } else if (OB_FAIL(latest_ls_tablet_map.create(OB_BACKUP_MAX_LS_BUCKET, "tabletToLS"))) {
    LOG_WARN("[DATA_BACKUP]fail to create tablet to ls map", K(ret));
  } else if (OB_FAIL(backup_ls_map.create(OB_BACKUP_MAX_LS_BUCKET, "lSTaskToLS"))) {
    LOG_WARN("[DATA_BACKUP]fail to create ls set", K(ret));
  } else {
    ObArray<ObTabletID> empty_tablet_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tasks.count(); ++i) {
      const ObBackupLSTaskAttr &ls_task_attr = ls_tasks.at(i);
      if (OB_FAIL(backup_ls_map.set_refactored(ls_task_attr.ls_id_, &ls_task_attr))) {
        LOG_WARN("[DATA_BACKUP]fail to insert ls id into set", K(ret));
      } else if (OB_FAIL(latest_ls_tablet_map.set_refactored(ls_task_attr.ls_id_, empty_tablet_ids))) {
        LOG_WARN("failed to set refactored", K(ret), K(ls_task_attr.ls_id_));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_tablet_list_by_snapshot(consistent_scn, latest_ls_tablet_map))) {
    LOG_WARN("failed to get tablet list by snapshot", K(ret));
  } else {
    // In order to ensure observer backup all the tablet, we need to check if latest tablet list is match the 
    // extern tablet lists. if not match, return an err and backup failed.
    ObHashMap<ObLSID, const ObBackupLSTaskAttr *>::iterator iter = backup_ls_map.begin();
    while (OB_SUCC(ret) && iter != backup_ls_map.end()) {
      const ObLSID &ls_id = iter->first;
      ObArray<ObTabletID> user_tablet_ids;
      SCN backup_scn = SCN::min_scn();
      ObArray<ObTabletID> latest_tablet_id;
      if (OB_FAIL(get_extern_tablet_info_(ls_id, user_tablet_ids, backup_scn))) {
        LOG_WARN("fail to get extern tablet info", K(ret), K(ls_id));
      } else if (OB_FAIL(latest_ls_tablet_map.get_refactored(ls_id, latest_tablet_id))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // ls may be deleted, or has no tablet.
          ret = OB_SUCCESS;
          LOG_INFO("ls may be deleted, or has no tablet, skip it", K(ls_id)); 
        } else {
          LOG_WARN("fail to get refactored", K(ret), K(ls_id));
        }
      } else if (OB_FAIL(latest_ls_tablet_map.erase_refactored(ls_id))) {
        LOG_WARN("fail to erase ls", K(ret), K(ls_id));
      }

      if (OB_SUCC(ret)) {
        iter ++;
      }
    }
    for (ObHashMap<ObLSID, ObArray<ObTabletID>>::iterator iter = latest_ls_tablet_map.begin();
        OB_SUCC(ret) && iter != latest_ls_tablet_map.end(); ++iter) {
      if (OB_FAIL(ls_ids.push_back(iter->first))) {
        LOG_WARN("failed to push backup");
      }
    }
  }
  share::ObBackupDataTaskType type(share::ObBackupDataTaskType::Type::BACKUP_DATA_MINOR);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
  } else if (OB_FAIL(generate_ls_tasks_(ls_ids, type))) {
    LOG_WARN("failed to generate ls tasks", K(ret), K(ls_ids), K(type));
  } else {
    ROOTSERVICE_EVENT_ADD("backup_data", "after_backup_consistent_scn");
  }
  if (trans_.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans_.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::get_tablet_list_by_snapshot(
    const share::SCN &consistent_scn, common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>> &latest_ls_tablet_map)
{
  int ret = OB_SUCCESS;
  ObBackupDataTabletToLSDesc tablet_to_ls_info;
  int64_t first_turn_id = 1;
  share::ObBackupDataType type;
  type.set_minor_data_backup();
  DEBUG_SYNC(BEFORE_BACKUP_CONSISTENT_SCN);
  if (OB_FAIL(store_.read_tablet_to_ls_info(first_turn_id, type, tablet_to_ls_info))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      share::SCN snapshot(SCN::min_scn());
      int64_t abs_timeout = ObTimeUtility::current_time() + 10 * 60 * 1000 * 1000;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(ObBackupDataScheduler::get_backup_scn(*sql_proxy_, set_task_attr_.tenant_id_, true, snapshot))) {
          LOG_WARN("failed to get backup scn", K(ret));
        } else if (snapshot >= consistent_scn) {
          snapshot = consistent_scn;
          break;
        } else if (ObTimeUtility::current_time() > abs_timeout) {
          ret = OB_TIMEOUT;
          break;
        } else if (OB_FAIL(backup_service_->check_leader())) {
          LOG_WARN("failed to check leader", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(fill_map_with_sys_tablets_(latest_ls_tablet_map))) {
        LOG_WARN("failed to fill map with sys ls tablets", K(ret));
      } else if (OB_FAIL(ObBackupTabletToLSOperator::get_ls_and_tablet(
          *sql_proxy_, set_task_attr_.tenant_id_, snapshot, latest_ls_tablet_map))) {
        LOG_WARN("fail to get ls and tablet", K(ret), "tenant_id", set_task_attr_.tenant_id_);
      } else {
        tablet_to_ls_info.backup_scn_ = snapshot;
        common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>>::const_iterator iter = latest_ls_tablet_map.begin();
        for(; OB_SUCC(ret) && iter != latest_ls_tablet_map.end(); ++iter) {
          ObBackupDataTabletToLSInfo info;
          info.ls_id_ = iter->first;
          ObLS::ObLSInnerTabletIDIter tablet_iter;
          common::ObTabletID tablet_id;
          if (!info.ls_id_.is_sys_ls()) {
            while (OB_SUCC(ret)) {
              if (OB_FAIL(tablet_iter.get_next(tablet_id))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                }
              } else if (OB_FAIL(info.tablet_id_list_.push_back(tablet_id))) {
                LOG_WARN("failed to push back tablet id", K(tablet_id));
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(append(info.tablet_id_list_, iter->second))) {
            LOG_WARN("failed to append tablet id list", K(ret));
          } else if (OB_FAIL(tablet_to_ls_info.tablet_to_ls_.push_back(info))) {
            LOG_WARN("failed to push backup info", K(info));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(store_.write_tablet_to_ls_info(tablet_to_ls_info, first_turn_id, type))) {
          LOG_WARN("failed to write tablet to ls info", K(ret), K(tablet_to_ls_info), K(first_turn_id), K(type));
        }
      }
    } else {
      LOG_WARN("failed to read tablet to ls info", K(ret));
    }
  } else {
    ObArray<ObTabletID> tablet_id_array;
    ARRAY_FOREACH(tablet_to_ls_info.tablet_to_ls_, i) {
      const ObBackupDataTabletToLSInfo &info = tablet_to_ls_info.tablet_to_ls_.at(i);
      tablet_id_array.reset();
      ARRAY_FOREACH(info.tablet_id_list_, j) {
        if (OB_FAIL(tablet_id_array.push_back(info.tablet_id_list_.at(j)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(latest_ls_tablet_map.set_refactored(info.ls_id_, tablet_id_array, 1/*over write*/))) {
        LOG_WARN("failed to set refactored", K(ret), K(info));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FALSE_IT(type.set_major_data_backup())) {
  } else if (OB_FAIL(store_.write_tablet_to_ls_info(tablet_to_ls_info, first_turn_id, type))) {
    LOG_WARN("failed to write tablet to ls info", K(ret), K(tablet_to_ls_info), K(first_turn_id), K(type));
  } else if (OB_FAIL(update_tablet_id_backup_scn_(tablet_to_ls_info.backup_scn_))) {
    LOG_WARN("failed to update tablet id backup scn", K(ret), K(tablet_to_ls_info.backup_scn_));
  }
  return ret;
}

int ObBackupSetTaskMgr::fill_map_with_sys_tablets_(
    common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>> &latest_ls_tablet_map)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;
  share::SCN backup_scn;
  ObLSID sys_ls_id(ObLSID::SYS_LS_ID);
  if (OB_FAIL(get_extern_tablet_info_(sys_ls_id, tablet_ids, backup_scn))) {
    LOG_WARN("failed to get extern sys ls tablet info", K(ret));
  } else if (OB_FALSE_IT(std::sort(tablet_ids.begin(), tablet_ids.end()))) {
  } else if (OB_FAIL(latest_ls_tablet_map.set_refactored(sys_ls_id, tablet_ids, 1))) {
    LOG_WARN("failed to set refactored", K(ret), K(sys_ls_id), K(tablet_ids));
  } else {
    LOG_INFO("succeed fill sys ls tablets", K(sys_ls_id), K(tablet_ids));
  }
  return ret;
}

int ObBackupSetTaskMgr::update_tablet_id_backup_scn_(const share::SCN &backup_scn)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = 0;
  ObBackupSetFileDesc backup_set_file;
  ObBackupDest backup_dest;
  ObMySQLTransaction trans;
  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, job_attr_->tenant_id_, set_task_attr_.backup_path_, backup_dest))) {
    LOG_WARN("fail to get backup dest", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, job_attr_->tenant_id_, backup_dest, dest_id))) {
    LOG_WARN("failed to get dest id", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(trans.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("failed to start trans", K(ret));
  } else {
    if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(trans, true/*for update*/, job_attr_->backup_set_id_,
        job_attr_->incarnation_id_, job_attr_->tenant_id_, dest_id, backup_set_file))) {
      LOG_WARN("failed to get backup set", K(ret), KPC(job_attr_));
    } else if (OB_FALSE_IT(backup_set_file.consistent_scn_ = backup_scn)) {
    } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file(trans, backup_set_file))) {
      LOG_WARN("failed to update backup set file", K(ret));
    } else if (OB_FAIL(trans.end(true))) {
      LOG_WARN("failed to commit", K(ret));
    }

    if (OB_FAIL(ret) && trans.is_active()) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        LOG_WARN("failed to rollback", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::get_extern_tablet_info_(
    const share::ObLSID &ls_id, ObIArray<ObTabletID> &user_tablet_ids, SCN &backup_scn)
{
  int ret = OB_SUCCESS;
  storage::ObLSMetaPackage ls_meta_package;
  if (OB_FAIL(store_.read_base_tablet_list(ls_id, user_tablet_ids))) {
    LOG_WARN("failed to read base tablet lsit", K(ret), K(ls_id));
  } else if (OB_FAIL(store_.read_ls_meta_infos(ls_id, ls_meta_package))) {
    LOG_WARN("failed to read ls meta infos", K(ret));
  } else if (!ls_meta_package.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls meta", K(ret));
  } else {
    backup_scn = ls_meta_package.ls_meta_.get_clog_checkpoint_scn();
  }
  return ret;
}

int ObBackupSetTaskMgr::do_backup_meta_(ObIArray<ObBackupLSTaskAttr> &ls_task, int64_t &finish_cnt)
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
      if (OB_FAIL(ls_task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task advancer", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to process ls backup meta task", K(ret), K(ls_attr), K(set_task_attr_));
      } 
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_backup_root_key_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(store_.write_root_key_info(job_attr_->tenant_id_))) {
    LOG_WARN("failed to write root key info");
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
    switch (cur_status)
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
      next_status = ObBackupStatus::Status::BACKUP_DATA_MINOR;
      break;
    }
    case ObBackupStatus::Status::BACKUP_DATA_MINOR: {
      DEBUG_SYNC(BEFORE_BACKUP_MAJOR);
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
  if (OB_FAIL(ObBackupLSTaskOperator::get_ls_tasks(*sql_proxy_, job_attr_->job_id_, job_attr_->tenant_id_, false/*update*/, ls_task))) {
    LOG_WARN("[DATA_BACKUP]failed to get log stream tasks", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (ls_task.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]no logstream task", K(ret), "job_id", job_attr_->job_id_, "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(do_backup_data_(ls_task, finish_cnt, build_index_attr))) {
    LOG_WARN("[DATA_BACKUP]failed to do backup ls task", K(ret), K(ls_task));
  } else if (ls_task.count() == finish_cnt) {
    bool need_change_turn = false;
    bool finish_build_index = false;
    ObSArray<storage::ObBackupDataTabletToLSInfo> tablets_to_ls;
    if (OB_FAIL(build_index_(build_index_attr, finish_build_index))) {
      LOG_WARN("[DATA_BACKUP]failed to wait build index", K(ret), KPC(build_index_attr));
    } else if (!finish_build_index) {
    } else if (OB_FAIL(check_need_change_turn_(ls_task, need_change_turn, tablets_to_ls))) {
      LOG_WARN("[DATA_BACKUP]failed to check change task turn", K(ret), K(set_task_attr_));
    } else if (need_change_turn) {
      if (OB_FAIL(change_turn_(ls_task, *build_index_attr, tablets_to_ls))) {
        LOG_WARN("failed to change turn", K(ret), K(set_task_attr_), K(ls_task), K(tablets_to_ls));
      }
    } else if (OB_FAIL(backup_data_finish_(ls_task, *build_index_attr))) {
      LOG_WARN("failed to backup data finish", K(ret), K(ls_task));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::backup_data_finish_(
    const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks,
    const ObBackupLSTaskAttr &build_index_attr)
{
  int ret = OB_SUCCESS;
  share::ObBackupStatus next_status;
  SCN end_scn = SCN::min_scn();
  if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(ObBackupLSTaskOperator::delete_build_index_task(trans_, build_index_attr))) {
    LOG_WARN("[DATA_BACKUP]failed to delete build index task", K(ret));
  } else if (OB_FAIL(get_next_status_(set_task_attr_.status_, next_status))) {
    LOG_WARN("fail to get next status", K(set_task_attr_.status_), K(next_status));
  } else if (ObBackupStatus::Status::BACKUP_DATA_MAJOR == set_task_attr_.status_.status_ && !job_attr_->plus_archivelog_) {
  } else if (OB_FAIL(convert_task_type_(ls_tasks))) {
    LOG_WARN("[DATA_BACKUP]failed to update task type to PLUS_ARCHIVE_LOG", K(ret), K(ls_tasks));
  }

  if (FAILEDx(ObBackupDataScheduler::get_backup_scn(*sql_proxy_, job_attr_->tenant_id_, false/*end scn*/, end_scn))) {
    LOG_WARN("[DATA_BACKUP]failed to get end ts", K(ret), "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(advance_status_(trans_, next_status, OB_SUCCESS, end_scn))) {
    LOG_WARN("[DATA_BACKUP]failed to update set task status to COMPLETEING", K(ret), K(set_task_attr_));
  }
  if (trans_.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans_.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
    }

    if (OB_SUCC(ret)) {
      set_task_attr_.status_ = next_status;
      LOG_INFO("backup data succeed, advance status to backup compelement log", "tenant_id", job_attr_->tenant_id_,
        "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
      ROOTSERVICE_EVENT_ADD("backup_data", "backup data succeed", "tenant_id",
        job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
      backup_service_->wakeup();
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::change_turn_(
    const ObIArray<share::ObBackupLSTaskAttr> &ls_tasks,
    const ObBackupLSTaskAttr &build_index_attr,
    ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls)
{
  int ret = OB_SUCCESS;
  ObBackupStatus::BACKUP_DATA_MINOR == set_task_attr_.status_ ? set_task_attr_.minor_turn_id_++ : set_task_attr_.major_turn_id_++;
  if (OB_FAIL(write_or_update_tablet_to_ls_(tablets_to_ls))) {
  LOG_WARN("[DATA_BACKUP]failed to write ls and tablets info when change turn", K(ret), K(tablets_to_ls));
  } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(change_task_turn_(ls_tasks, tablets_to_ls))) {
    LOG_WARN("[DATA_BACKUP]failed to change task turn", K(ret), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupLSTaskOperator::delete_build_index_task(trans_, build_index_attr))) {
    LOG_WARN("[DATA_BACKUP]failed to delete build index task", K(ret));
  } else if (OB_FAIL(ObBackupSkippedTabletOperator::move_skip_tablet_to_his(
      trans_, set_task_attr_.tenant_id_, set_task_attr_.task_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move skip tablet to history", K(ret));
  }

  if (trans_.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans_.end(OB_SUCC(ret)))) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
    }

    if (OB_SUCC(ret)) {
      backup_service_->wakeup();
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
      if (OB_FAIL(ls_task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *backup_service_))) {
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

int ObBackupSetTaskMgr::check_need_change_turn_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks,
    bool &need_change_turn, 
    ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls)
{
  int ret = OB_SUCCESS;
  need_change_turn = true;
#ifdef ERRSIM
  ROOTSERVICE_EVENT_ADD("backup_errsim", "before_check_change_task_turn",
                        "tenant_id", set_task_attr_.tenant_id_,
                        "task_id", set_task_attr_.task_id_);
#endif
  DEBUG_SYNC(BEFORE_CHANGE_BACKUP_TURN);
  if (set_task_attr_.status_.is_backup_sys()) {
    need_change_turn = false;
  } else if (OB_FAIL(get_change_turn_tablets_(ls_tasks, tablets_to_ls))) {
    LOG_WARN("[DATA_BACKUP]failed to get change turn tablets", K(ret), K(ls_tasks));
  } else if (tablets_to_ls.empty()) {
    need_change_turn = false;
  }
  return ret;
}

int ObBackupSetTaskMgr::change_task_turn_(
    const ObIArray<ObBackupLSTaskAttr> &ls_task,
    const ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObBackupLSTaskAttr *> need_change_turn_ls_tasks;
  ObSArray<ObLSID> new_ls_array;
  if (ls_task.empty() || (tablets_to_ls.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid argument", K(ret), K(ls_task), K(tablets_to_ls), K(new_ls_array));
  } else if (OB_FAIL(filter_new_ls_from_tablet_info_(ls_task, tablets_to_ls, new_ls_array))) {
    LOG_WARN("failed to filter new ls from tablet info", K(ret));
  } else if (OB_FAIL(get_change_turn_ls_(ls_task, tablets_to_ls, need_change_turn_ls_tasks))) {
    LOG_WARN("[DATA_BACKUP]failed to get change turn id and ls when change turn", K(ret), K(ls_task), K(tablets_to_ls));
  } else if (OB_FAIL(update_inner_task_(new_ls_array, need_change_turn_ls_tasks))) {
    LOG_WARN("[DATA_BACKUP]failed to update inner task when change turn", K(ret), K(need_change_turn_ls_tasks));
  }
  ROOTSERVICE_EVENT_ADD("backup_data", "after_change_backup_turn_id",
                        "tenant_id", job_attr_->tenant_id_,
                        "backup_status", set_task_attr_.status_,
                        "new_turn_id", ObBackupStatus::BACKUP_DATA_MINOR == set_task_attr_.status_ ?
                                            set_task_attr_.minor_turn_id_ : set_task_attr_.major_turn_id_);
  DEBUG_SYNC(AFTER_CHANGE_BACKUP_TURN_ID);
  return ret;
}

int ObBackupSetTaskMgr::filter_new_ls_from_tablet_info_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks,
    const ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls,
    ObIArray<ObLSID> &new_ls_array)
{
  int ret = OB_SUCCESS;
  ObHashSet<ObLSID> ls_id_set;
  const int64_t OB_BACKUP_MAX_LS_BUCKET = 32;
  if (OB_FAIL(ls_id_set.create(OB_BACKUP_MAX_LS_BUCKET))) {
    LOG_WARN("[DATA_BACKUP]failed to create set", K(ret));
  } else if (OB_FAIL(construct_cur_ls_set_(ls_tasks, ls_id_set))) {
    LOG_WARN("[DATA_BACKUP]failed to get last turn ls ids", K(ret), K(set_task_attr_));
  }

  ARRAY_FOREACH(tablets_to_ls, i) {
    const ObLSID ls_id =tablets_to_ls.at(i).ls_id_;
    if (OB_FAIL(ls_id_set.exist_refactored(ls_id))) {
      if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else if (OB_HASH_NOT_EXIST == ret) {
        if (OB_FAIL(new_ls_array.push_back(ls_id))) {
          LOG_WARN("failed to push back ls id", K(ret), K(ls_id));
        }
      } else {
        LOG_WARN("failed to exist refactored", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::update_inner_task_(
    const ObIArray<ObLSID> &new_ls_ids,
    const ObIArray<const ObBackupLSTaskAttr *> &need_change_turn_ls_tasks)
{
  int ret = OB_SUCCESS;
  int64_t turn_id = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < need_change_turn_ls_tasks.count(); ++i) {
    const ObBackupLSTaskAttr *ls_attr = need_change_turn_ls_tasks.at(i);
    if (nullptr == ls_attr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[DATA_BACKUP]null ls ptr", K(ret));
    // else if (OB_LS_NOT_EXIST == ls_attr->result_) {
    // LOG_WARN("[DATA_BACKUP]ls has been delete, need not to redo backup", K(ret), "ls_id", ls_attr->ls_id_);
    // } // TODO(yangyi.yyy): use another error code to determine change turn in 4.1
    } else if (OB_FAIL(calc_task_turn_(ls_attr->task_type_, turn_id))) {
      LOG_WARN("failed to calc task turn id", K(ret));
    } else if (OB_FAIL(ObBackupDataLSTaskMgr::redo_ls_task(
        *backup_service_, trans_, *ls_attr, ls_attr->start_turn_id_, turn_id, 0/*retry_id*/))) {
      LOG_WARN("[DATA_BACKUP]failed to update ls task result to success", K(ret), KPC(ls_attr), K(turn_id));
    }
  } 
  ObBackupDataTaskType type;
  if (OB_FAIL(ret)) {
  } else if (ObBackupStatus::Status::BACKUP_DATA_SYS == set_task_attr_.status_.status_) {
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
  } else if ((!new_ls_ids.empty() || !need_change_turn_ls_tasks.empty())) {
    // change backup task turn.
    turn_id = ObBackupStatus::BACKUP_DATA_MINOR == set_task_attr_.status_ ? set_task_attr_.minor_turn_id_ : set_task_attr_.major_turn_id_;
    if (OB_FAIL(ObBackupTaskOperator::update_turn_id(
                trans_,
                set_task_attr_.status_,
                set_task_attr_.task_id_,
                set_task_attr_.tenant_id_,
                turn_id))) {
      LOG_WARN("[DATA_BACKUP]failed to update backup task turn id", K(ret), K_(set_task_attr), K(turn_id));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::write_or_update_tablet_to_ls_(ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataTabletToLSDesc tablet_to_ls_desc;
  int64_t turn_id = 0;
  ObBackupDataType data_type;
  ObBackupDataTaskType task_type;
  if (tablets_to_ls.empty()) { // no tablet, no need to write extern tablets info
  } else {
    switch(set_task_attr_.status_) {
      case ObBackupStatus::BACKUP_DATA_MINOR: {
        data_type.set_minor_data_backup();
        task_type.set_backup_minor();
        break;
      }
      case ObBackupStatus::BACKUP_DATA_MAJOR: {
        data_type.set_major_data_backup();
        task_type.set_backup_major();
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected backup status while change turn", K(ret));
      }
    }
    if (FAILEDx(calc_task_turn_(task_type, turn_id))) {
      LOG_WARN("failed to calc task turn id", K(ret));
    }
  }
  // tablet_to_ls may be different because of transfer,
  // and tablet_to_ls_info can't be overwrote becuase of WORM,
  // so write the target turn tablet to ls info only once.
  if (FAILEDx(store_.read_tablet_to_ls_info(turn_id, data_type, tablet_to_ls_desc))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      if (OB_FAIL(tablet_to_ls_desc.tablet_to_ls_.assign(tablets_to_ls))) {
        LOG_WARN("failed to assign tablet to ls", K(tablets_to_ls));
      } else if (OB_FAIL(store_.write_tablet_to_ls_info(tablet_to_ls_desc, turn_id, data_type))) {
        LOG_WARN("[DATA_BACKUP]failed to write tablet to log stream", K(ret), K(tablet_to_ls_desc), K(turn_id));
      }
    } else {
      LOG_WARN("failed to read tablet to ls info", K(ret), K(turn_id), K(data_type));
    }
  } else if (!tablet_to_ls_desc.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet to ls desc", K(ret), K(tablet_to_ls_desc));
  } else if (OB_FALSE_IT(tablets_to_ls.reset())) {
  } else if (OB_FAIL(tablets_to_ls.assign(tablet_to_ls_desc.tablet_to_ls_))) {
    LOG_WARN("failed to assign", K(tablet_to_ls_desc));
  }
  return ret;
}

int ObBackupSetTaskMgr::build_index_(ObBackupLSTaskAttr *build_index_attr, bool &finish_build_index)
{
  int ret = OB_SUCCESS;
  finish_build_index = false;
  if (OB_FAIL(backup_service_->check_leader())) {
      LOG_WARN("[DATA_BACKUP]failed to check leader", K(ret));
  } else if (nullptr == build_index_attr) {
    ObBackupLSTaskAttr build_index_task;
    build_index_task.task_id_ = set_task_attr_.task_id_;
    build_index_task.tenant_id_ = job_attr_->tenant_id_;
    build_index_task.ls_id_ = ObLSID(0);
    build_index_task.job_id_ = job_attr_->job_id_;
    build_index_task.backup_set_id_ = job_attr_->backup_set_id_;
    build_index_task.backup_type_.type_ = job_attr_->backup_type_.type_;
    build_index_task.task_type_.type_ = ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX;
    build_index_task.status_.status_ = ObBackupTaskStatus::Status::INIT;
    build_index_task.start_ts_ = ObTimeUtility::current_time();
    build_index_task.start_turn_id_ = 1;
    build_index_task.retry_id_ = 0;
    build_index_task.result_ = OB_SUCCESS;
    build_index_task.max_tablet_checkpoint_scn_.set_min();
    if (OB_FAIL(calc_task_turn_(build_index_task.task_type_, build_index_task.turn_id_))) {
      LOG_WARN("failed to calc ls task turn", K(ret));
    } else if (OB_FAIL(ObBackupUtils::convert_timestamp_to_date(build_index_task.start_ts_, build_index_task.backup_date_))) {
      LOG_WARN("[DATA_BACKUP]failed to get date", K(ret), K(build_index_task));
    } else if (OB_FAIL(ObBackupLSTaskOperator::insert_build_index_task(*sql_proxy_, build_index_task))) {
      LOG_WARN("[DATA_BACKUP]failed to insert build index task", K(ret), K(build_index_task));
    } else {
      backup_service_->wakeup();
    }
  } else if (OB_SUCCESS == build_index_attr->result_ && ObBackupTaskStatus::Status::FINISH == build_index_attr->status_.status_) {
    finish_build_index = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[DATA_BACKUP]unexpected err", K(ret), KPC(build_index_attr));
  }
  return ret;
}

int ObBackupSetTaskMgr::get_change_turn_ls_(
    const ObIArray<ObBackupLSTaskAttr> &ls_task,
    const ObIArray<storage::ObBackupDataTabletToLSInfo> &tablets_to_ls,
    ObIArray<const ObBackupLSTaskAttr *> &need_change_turn_ls_tasks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < tablets_to_ls.count(); ++j) {
      const ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
      const storage::ObBackupDataTabletToLSInfo& tablets = tablets_to_ls.at(j);
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
    ObIArray<storage::ObBackupDataTabletToLSInfo> &tablet_to_ls)
{
  int ret = OB_SUCCESS;
  ObHashSet<ObBackupSkipTabletAttr> skipped_tablets;
  ObBackupSkippedType skipped_type(ObBackupSkippedType::TRANSFER);
  const int OB_MAX_SKIPPED_TABLET_NUM = 1000;
  if (OB_FAIL(skipped_tablets.create(OB_MAX_SKIPPED_TABLET_NUM))) {
    LOG_WARN("failed to create skipped tablets set", K(ret));
  } else if (OB_FAIL(get_tablets_of_deleted_ls_(ls_tasks, skipped_tablets))) {
    LOG_WARN("failed to get tablets of deleted ls", K(ret), K(ls_tasks));
  } else if (OB_FAIL(ObBackupSkippedTabletOperator::get_skip_tablet(*sql_proxy_, false/*no lock*/, set_task_attr_.tenant_id_, set_task_attr_.task_id_, skipped_type, skipped_tablets))) {
    LOG_WARN("[DATA_BACKUP]failed to get skip tablet", K(ret), "teannt_id", set_task_attr_.tenant_id_, "task_id", set_task_attr_.task_id_);
  } else if (skipped_tablets.empty()) {
    LOG_INFO("[DATA_BACKUP]no change turn tablets found", K(ret));
  } else if (OB_FAIL(do_get_change_turn_tablets_(ls_tasks, skipped_tablets, tablet_to_ls))) {
    LOG_WARN("[DATA_BACKUP]failed to do get change turn tables", K(ret), K(set_task_attr_));
  }
  return ret;
}

int ObBackupSetTaskMgr::get_tablets_of_deleted_ls_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks, common::hash::ObHashSet<ObBackupSkipTabletAttr> &skipped_tablets)
{
  int ret = OB_SUCCESS;
  ObBackupDataType type;
  type.type_ = set_task_attr_.status_.is_backup_minor() ? ObBackupDataType::BACKUP_MINOR : ObBackupDataType::BACKUP_MAJOR;
  const int64_t set_task_turn_id = set_task_attr_.status_.is_backup_minor() ? set_task_attr_.minor_turn_id_ : set_task_attr_.major_turn_id_;
  ARRAY_FOREACH(ls_tasks, i) {
    ObArray<common::ObTabletID> tablet_ids;
    const ObBackupLSTaskAttr &ls_task = ls_tasks.at(i);
    if (ls_task.result_ != OB_LS_NOT_EXIST || ls_task.turn_id_ != set_task_turn_id) {
      continue;
    } else if (OB_FAIL(store_.read_tablet_list(type, ls_task.turn_id_, ls_task.ls_id_, tablet_ids))) {
      LOG_WARN("failed to read tablet list", K(ret));
    }

    ARRAY_FOREACH(tablet_ids, i) {
      if (tablet_ids.at(i).is_inner_tablet()) {
        continue;
      }
      ObBackupSkipTabletAttr skipped_tablet;
      skipped_tablet.tablet_id_ = tablet_ids.at(i);
      skipped_tablet.skipped_type_ = ObBackupSkippedType(ObBackupSkippedType::TRANSFER);
      if (OB_FAIL(skipped_tablets.set_refactored(skipped_tablet))) {
        LOG_WARN("failed to push back skipped tablet", K(ret), K(skipped_tablet));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_get_change_turn_tablets_(
    const ObIArray<ObBackupLSTaskAttr> &ls_tasks,
    const common::hash::ObHashSet<ObBackupSkipTabletAttr> &skipped_tablets,
    ObIArray<storage::ObBackupDataTabletToLSInfo> &tablet_to_ls)
{
  int ret = OB_SUCCESS;
  // if tablet can't be found in __all_tablet_to_ls, it need't to change turn
  // if tablet transfer to a new create ls, need insert a new ls task
  ObHashMap<ObLSID, ObArray<common::ObTabletID>> tablet_to_ls_map;
  ObLSID ls_id;
  int64_t transfer_seq = 0;
  const int64_t OB_BACKUP_MAX_LS_BUCKET = 1024;
  if (OB_FAIL(tablet_to_ls_map.create(OB_BACKUP_MAX_LS_BUCKET, "tabletToLS"))) {
    LOG_WARN("[DATA_BACKUP]failed to create map", K(ret));
  } else {
    for (auto iter = skipped_tablets.begin(); OB_SUCC(ret) && iter != skipped_tablets.end(); ++iter) {
      const ObBackupSkipTabletAttr &skip_tablet = iter->first;
      if (OB_FAIL(ObBackupTabletToLSOperator::get_ls_of_tablet(*sql_proxy_, job_attr_->tenant_id_, skip_tablet.tablet_id_, ls_id, transfer_seq))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_WARN("[DATA_BACKUP]deleted tablet", K(skip_tablet));
        } else {
          LOG_WARN("[DATA_BACKUP]failed to get tablet", K(ret), K(skip_tablet));
        }
      } else {
        ObArray<common::ObTabletID> *tablet_ids = nullptr;
        if (OB_ISNULL(tablet_ids = tablet_to_ls_map.get(ls_id))) {
          ObArray<ObTabletID> cur_tablet_ids;
          if (OB_FAIL(cur_tablet_ids.push_back(skip_tablet.tablet_id_))) {
            LOG_WARN("[DATA_BACKUP]failed to push tablet id", K(ret), K(skip_tablet.tablet_id_));
          } else if (OB_FAIL(tablet_to_ls_map.set_refactored(ls_id, cur_tablet_ids, 1/*cover exist object*/))) {
            LOG_WARN("[DATA_BACKUP]failed to set_refactored", K(ret),  K(ls_id));
          }
        } else if (OB_FAIL(tablet_ids->push_back(skip_tablet.tablet_id_))) {
          LOG_WARN("fail to append tablet ids", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObHashMap<ObLSID, ObArray<common::ObTabletID>>::hashtable::const_iterator map_iter = tablet_to_ls_map.begin();
      for (; OB_SUCC(ret) && map_iter != tablet_to_ls_map.end(); ++map_iter) {
        storage::ObBackupDataTabletToLSInfo ls_info;
        ls_info.ls_id_ = map_iter->first;
        if (map_iter->second.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[DATA_BACKUP]failed to get tablet to ls", K(ret), K(ls_info.ls_id_));
        } else if (OB_FAIL(append(ls_info.tablet_id_list_, map_iter->second))) {
          LOG_WARN("[DATA_BACKUP]failed to append tablet to ls array", K(ret));
        } else if (OB_FALSE_IT(std::sort(ls_info.tablet_id_list_.begin(), ls_info.tablet_id_list_.end()))) {
        } else if (OB_FAIL(tablet_to_ls.push_back(ls_info))) {
          LOG_WARN("[DATA_BACKUP]failed to push backup ls info", K(ret));
        } 
      }
    } 

    if (OB_SUCC(ret)) {
      FLOG_INFO("[DATA_BACKUP]get new turn tablet_to_ls", K(tablet_to_ls));
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

int ObBackupSetTaskMgr::convert_task_type_(const ObIArray<ObBackupLSTaskAttr> &ls_tasks)
{
  int ret = OB_SUCCESS;
  ObBackupDataTaskType type;
  switch(set_task_attr_.status_.status_) {
    case ObBackupStatus::Status::BACKUP_USER_META: {
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
    ObBackupLSTaskAttr new_ls_task;
    if (ObBackupDataTaskType::Type::BACKUP_BUILD_INDEX == ls_task.task_type_.type_) {
    } else if (OB_FAIL(new_ls_task.assign(ls_task))) {
      LOG_WARN("failed to assign new ls task", K(ret));
    } else {
      new_ls_task.task_type_ = type;
      new_ls_task.status_ = ObBackupTaskStatus::INIT;
      new_ls_task.result_ = OB_SUCCESS;
      new_ls_task.dst_.reset();
      new_ls_task.task_trace_id_.reset();
      new_ls_task.retry_id_ = 0;
      new_ls_task.turn_id_ = 1;
      if (OB_FAIL(backup_service_->check_leader())) {
        LOG_WARN("failed to check leader", K(ret));
      } else if (OB_FAIL(ObBackupLSTaskOperator::report_ls_task(trans_, new_ls_task))) {
        LOG_WARN("failed to report ls task", K(ret));
      } else if (type.is_backup_data()) {
        share::ObBackupDataType backup_data_type;
        if (OB_FAIL(type.get_backup_data_type(backup_data_type))) {
          LOG_WARN("failed to get backup data type", K(ret), K(type));
        } else if (OB_FAIL(ObLSBackupOperator::insert_ls_backup_task_info(new_ls_task.tenant_id_,
            new_ls_task.task_id_, new_ls_task.turn_id_, new_ls_task.retry_id_, new_ls_task.ls_id_,
            new_ls_task.backup_set_id_, backup_data_type, trans_))) {
          LOG_WARN("failed to insert ls backup task info", K(ret), K(new_ls_task), K(backup_data_type));
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("succeed update ls task type", K(ls_task), K(new_ls_task));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("update ls task finish", K(type), K(set_task_attr_.task_id_), K(set_task_attr_.tenant_id_));
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
      set_task_attr_.status_ = ObBackupStatus::Status::FAILED;
      set_task_attr_.end_ts_ = job_attr_->end_ts_;
      if (OB_FAIL(set_backup_set_files_failed_(trans))) {
        LOG_WARN("fail to set backup set files failed", K(ret));
      } else if (OB_FAIL(advance_status_(trans, set_task_attr_.status_, job_attr_->result_, set_task_attr_.end_scn_,
          job_attr_->end_ts_))) {
        LOG_WARN("[DATA_BACKUP]failed to advance set task status to FAILED", K(ret), K(set_task_attr_), 
        "failed job result", job_attr_->result_);
      } 
    }
  }
  

  return ret;
}

int ObBackupSetTaskMgr::do_failed_ls_task_(ObMySQLTransaction &trans, const ObIArray<ObBackupLSTaskAttr> &ls_task) 
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_task.count(); ++i) {
    const ObBackupLSTaskAttr &ls_attr = ls_task.at(i);
    ObBackupLSTaskAttr new_ls_attr;
    if (OB_FAIL(new_ls_attr.assign(ls_attr))) {
      LOG_WARN("failed to assign ls attr", K(ret));
    } else {
      new_ls_attr.status_ = ObBackupTaskStatus::FINISH;
      new_ls_attr.end_ts_ = job_attr_->end_ts_;
      if (OB_FAIL(ObBackupLSTaskOperator::report_ls_task(trans, new_ls_attr))) {
        LOG_WARN("failed to report ls task", K(ret));
      }
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
    LOG_WARN("[DATA_BACKUP]no logstream task", K(ret), K(ls_task));
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
    ObBackupStatus next_status = ObBackupStatus::COMPLETED;
    set_task_attr_.end_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(write_extern_infos_())) {
      LOG_WARN("fail to write_extern_infos", K(ret), K(set_task_attr_));
    } else if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      if (OB_FAIL(advance_status_(trans_, next_status, OB_SUCCESS, set_task_attr_.end_scn_, set_task_attr_.end_ts_))) {
        LOG_WARN("fail to advance status to COMPLETED", K(ret), K(set_task_attr_));
      } 
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } else {
          set_task_attr_.status_ = next_status;
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
      if (OB_FAIL(ls_task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task advancer", K(ret), K(ls_attr));
      } else if (OB_FAIL(ls_task_mgr.process(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to process log stream task", K(ret), K(ls_attr));
      } 
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::do_clean_up()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DATA_BACKUP]not init", K(ret));
  } else if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("[DATA_BACKUP]failed to check lease", K(ret));
  } else if (OB_FAIL(ObBackupSkippedTabletOperator::batch_move_skip_tablet(*sql_proxy_, set_task_attr_.tenant_id_, set_task_attr_.task_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move skip tablet", K(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(set_task_attr_.tenant_id_)))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (OB_FAIL(ObBackupLSTaskInfoOperator::move_ls_task_info_to_his(trans, set_task_attr_.task_id_, 
      set_task_attr_.tenant_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move task to history", K(ret), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupLSTaskOperator::move_ls_to_his(trans, set_task_attr_.tenant_id_, set_task_attr_.job_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move ls to history", K(ret), K(set_task_attr_));
  } else if (OB_FAIL(ObBackupTaskOperator::move_task_to_his(trans, set_task_attr_.tenant_id_, set_task_attr_.job_id_))) {
    LOG_WARN("[DATA_BACKUP]failed to move task to history", K(ret), K(set_task_attr_));
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to end trans", K(ret), K(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
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
      if (OB_FAIL(task_mgr.init(*job_attr_, set_task_attr_, ls_attr, *task_scheduler_, *sql_proxy_, *backup_service_))) {
        LOG_WARN("[DATA_BACKUP]failed to init task mgr", K(ret), K(ls_attr));
      } else if (OB_FAIL(task_mgr.cancel(finish_cnt))) {
        LOG_WARN("[DATA_BACKUP]failed to cancel task", K(ret), K(ls_attr), K(set_task_attr_));
      }
    }
  }
  if (OB_SUCC(ret) && ls_task.count() == finish_cnt) {
    ObBackupStatus next_status = ObBackupStatus::Status::CANCELED;
    set_task_attr_.end_ts_ = ObTimeUtility::current_time();
    if (OB_FAIL(trans_.start(sql_proxy_, meta_tenant_id_))) {
      LOG_WARN("fail to start trans", K(ret), K(meta_tenant_id_));
    } else {
      if (OB_FAIL(set_backup_set_files_failed_(trans_))) {
        LOG_WARN("failed to set backup set files failed", K(ret));
      } else if (OB_FAIL(advance_status_(
          trans_, next_status, OB_CANCELED, set_task_attr_.end_scn_, set_task_attr_.end_ts_))) {
        LOG_WARN("[DATA_BACKUP]failed to advance set task status to CANCELED", K(ret), K(set_task_attr_));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans_.end(true))) {
          LOG_WARN("fail to commit trans", KR(ret));
        } else {
          set_task_attr_.status_ = next_status;
          ROOTSERVICE_EVENT_ADD("backup_data", "cancel backup succeed", "tenant_id", 
            job_attr_->tenant_id_, "job_id", job_attr_->job_id_, "task_id", set_task_attr_.task_id_);
          FLOG_INFO("[DATA_BACKUP]advance status to CANCELED", K(set_task_attr_));
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


int ObBackupSetTaskMgr::write_extern_infos_()
{ 
  int ret = OB_SUCCESS;
  HEAP_VARS_2((ObExternTenantLocalityInfoDesc, locality_info),
      (ObExternBackupSetInfoDesc, backup_set_info)) {
    if (OB_FAIL(ret)) {
    } else if (job_attr_->plus_archivelog_ && OB_FAIL(write_log_format_file_())) {
      LOG_WARN("failed to write log format file", K(ret));
    } else if (OB_FAIL(write_extern_locality_info_(locality_info))) {
      LOG_WARN("[DATA_BACKUP]failed to write extern tenant locality info", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_backup_set_info_(set_task_attr_, backup_set_info))) { 
      LOG_WARN("[DATA_BACKUP]failed to write backup set info", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_tenant_backup_set_infos_())) {
      LOG_WARN("[DATA_BACKUP]failed to write tenant backup set infos", K(ret));
    } else if (OB_FAIL(write_extern_diagnose_info_(locality_info, backup_set_info))) { // 
      LOG_WARN("[DATA_BACKUP]failed to write extern tenant diagnose info", K(ret), KPC(job_attr_));
    } else if (OB_FAIL(write_backup_set_placeholder_(false/*finish job*/))) {
      LOG_WARN("[DATA_BACKUP]failed to write backup set finish placeholder", K(ret), KPC(job_attr_));
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::write_log_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupFormatDesc format_desc;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_info = NULL;
  share::ObBackupStore store;
  ObBackupDest backup_dest;
  ObBackupDest new_backup_dest;
  share::ObBackupSetDesc desc;
  desc.backup_set_id_ = job_attr_->backup_set_id_;
  desc.backup_type_ = job_attr_->backup_type_;
  int64_t dest_id = 0;
  bool is_exist = false;

  if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, job_attr_->tenant_id_, job_attr_->backup_path_,
                                                           backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupPathUtil::construct_backup_complement_log_dest(backup_dest, desc, new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(backup_dest));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, job_attr_->tenant_id_, backup_dest, dest_id))) {
    LOG_WARN("failed to get dest id", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(job_attr_->tenant_id_, schema_guard))) {
    LOG_WARN("failed to get_tenant_schema_guard", KR(ret), "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(schema_guard.get_tenant_info(job_attr_->tenant_id_, tenant_info))) {
    LOG_WARN("failed to get tenant info", K(ret), "tenant_id", job_attr_->tenant_id_);
  } else if (OB_FAIL(format_desc.cluster_name_.assign(GCONF.cluster.str()))) {
    LOG_WARN("failed to assign cluster name", K(ret));
  } else if (OB_FAIL(format_desc.tenant_name_.assign(tenant_info->get_tenant_name()))) {
    LOG_WARN("failed to assign tenant name", K(ret));
  } else if (OB_FAIL(new_backup_dest.get_backup_path_str(format_desc.path_.ptr(), format_desc.path_.capacity()))) {
    LOG_WARN("failed to get backup path", K(ret), K(new_backup_dest));
  } else {
    format_desc.tenant_id_ = job_attr_->tenant_id_;
    format_desc.incarnation_ = OB_START_INCARNATION;
    format_desc.dest_id_ = dest_id;
    format_desc.dest_type_ = ObBackupDestType::DEST_TYPE_ARCHIVE_LOG;
    format_desc.cluster_id_ = GCONF.cluster_id;
  }

  if (FAILEDx(store.init(new_backup_dest))) {
    LOG_WARN("failed to set archive dest", K(ret), K(backup_dest));
  } else if (OB_FAIL(store.is_format_file_exist(is_exist))) {
    LOG_WARN("failed to check is format file exist", K(ret));
  } else if (is_exist) {// do not recreate the format file
  } else if (OB_FAIL(store.write_format_file(format_desc))) {
    LOG_WARN("failed to write format file", K(ret), K(format_desc));
  } else {
    LOG_INFO("write log format file succeed", K(format_desc));
  }
  return ret;
}

int ObBackupSetTaskMgr::write_backup_set_info_(
    const ObBackupSetTaskAttr &set_task_attr, 
    ObExternBackupSetInfoDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  int64_t dest_id = 0;
  ObBackupSetFileDesc &backup_set_file = backup_set_info.backup_set_file_;
  ObBackupDest backup_dest;

  uint64_t cur_data_version = 0;
  uint64_t cur_cluster_version = 0;
  if (OB_FAIL(ObShareUtil::fetch_current_data_version(*sql_proxy_, job_attr_->tenant_id_, cur_data_version))) {
    LOG_WARN("failed to get data version", K(ret));
  } else if (OB_FAIL(ObShareUtil::fetch_current_cluster_version(*sql_proxy_, cur_cluster_version))) {
    LOG_WARN("failed to get cluster version", K(ret));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, job_attr_->tenant_id_, set_task_attr.backup_path_, backup_dest))) {
    LOG_WARN("[DATA_BACKUP]fail to get backup dest", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(*sql_proxy_, job_attr_->tenant_id_, backup_dest, dest_id))) {
    LOG_WARN("[DATA_BACKUP]failed to get dest id", K(ret), KPC(job_attr_));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(*sql_proxy_, false/*for update*/, job_attr_->backup_set_id_, job_attr_->incarnation_id_, 
      job_attr_->tenant_id_, dest_id, backup_set_file))) {
    LOG_WARN("[DATA_BACKUP]failed to get backup set", K(ret), KPC(job_attr_));
  } else if (cur_data_version != backup_set_file.tenant_compatible_ || cur_cluster_version != backup_set_file.cluster_version_) {
    ret = OB_VERSION_NOT_MATCH;

    LOG_WARN("cluster version or tenant data version are not match", K(ret),
        K(cur_data_version),
        "backup_data_version", backup_set_file.tenant_compatible_,
        K(cur_cluster_version),
        "backup_cluster_version", backup_set_file.cluster_version_);
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
    backup_set_file.minor_turn_id_ = set_task_attr.minor_turn_id_;
    backup_set_file.major_turn_id_ = set_task_attr.major_turn_id_;
    backup_set_file.min_restore_scn_ = set_task_attr.end_scn_;
    if (OB_FAIL(calculate_start_replay_scn_(backup_set_file.start_replay_scn_))) {
      LOG_WARN("fail to calculate start replay scn", K(ret));
    } else if (OB_FAIL(store_.write_backup_set_info(backup_set_info))) {
      LOG_WARN("[DATA_BACKUP]failed to write backup set start place holder", K(ret), K(backup_set_info));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(backup_service_->check_leader())) {
    LOG_WARN("[DATA_BACKUP]failed to check leader", K(ret));
  } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file(*sql_proxy_, backup_set_info.backup_set_file_))) {
    LOG_WARN("[DATA_BACKUP]failed to update backup set", K(ret), K(backup_set_info));
  } 
  return ret;
}

int ObBackupSetTaskMgr::calculate_start_replay_scn_(SCN &start_replay_scn)
{
  int ret = OB_SUCCESS;
  storage::ObBackupLSMetaInfosDesc ls_meta_infos;
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
  } else if (OB_FAIL(backup::ObBackupUtils::calc_start_replay_scn(set_task_attr_, ls_meta_infos, round_attr, start_replay_scn))) {
    LOG_WARN("failed to calc start replay scn", K(ret), K_(set_task_attr), K(ls_meta_infos), K(round_attr));
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
  } else if (OB_FAIL(ObBackupUtils::get_tenant_sys_time_zone_wrap(set_task_attr_.tenant_id_,
                                                                  locality_info.sys_time_zone_,
                                                                  locality_info.sys_time_zone_wrap_))) {
    LOG_WARN("failed to get tenant sys time zone wrap", K(ret));
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
      diagnose_info.backup_set_file_ = backup_set_info.backup_set_file_;
      if (OB_FAIL(diagnose_info.tenant_locality_info_.assign(locality_info))) {
        LOG_WARN("failed to assign", K(ret), K(locality_info));
      } else if (OB_FAIL(store_.write_tenant_diagnose_info(diagnose_info))) {
        LOG_WARN("[DATA_BACKUP]failed to write teannt diagnose info", K(ret), K(diagnose_info));
      }
    }
  }
  return ret;
}

int ObBackupSetTaskMgr::write_backup_set_placeholder_(const bool is_start)
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
  storage::ObTenantBackupSetInfosDesc tenant_backup_set_infos;
  if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_files(*sql_proxy_, job_attr_->tenant_id_, tenant_backup_set_infos.backup_set_infos_))) {
    LOG_WARN("[DATA_BACKUP]failed to get backup set", K(ret), KPC(job_attr_));
  } else if (!tenant_backup_set_infos.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant backup set infos", K(ret), K(tenant_backup_set_infos));
  } else if (OB_FAIL(store_.write_tenant_backup_set_infos(tenant_backup_set_infos))) {
    LOG_WARN("failed to write tenant backup set infos", K(ret));
  }
  return ret;
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
    if (OB_FAIL(backup_service_->check_leader())) {
      LOG_WARN("failed to check leader", K(ret));
    } else if (OB_FAIL(ObBackupSetFileOperator::update_backup_set_file(trans, backup_set_file))) {
      LOG_WARN("failed to update backup set", K(ret), K(backup_set_file));
    } 
  }
  return ret;
}
