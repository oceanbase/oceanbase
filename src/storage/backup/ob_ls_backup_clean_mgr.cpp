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
#include "ob_ls_backup_clean_mgr.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_backup_clean_operator.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/backup/ob_backup_clean_util.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/location_cache/ob_location_service.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
/*****************ObLSBackupCleanScheduler*******************/
int ObLSBackupCleanScheduler::schedule_backup_clean_dag(const obrpc::ObLSBackupCleanArg &args)
{
  int ret = OB_SUCCESS;
  ObLSBackupCleanDagNetInitParam param;
  MTL_SWITCH(args.tenant_id_) {
    ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
    if (OB_ISNULL(scheduler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null MTL scheduler", K(ret), K(scheduler));
    } else if (OB_FAIL(param.set(args))) {
      LOG_WARN("failed to set ls backup clean net init param",K(ret), K(args));
    } else if (OB_FAIL(scheduler->create_and_add_dag_net<ObLSBackupCleanDagNet>(&param))) {
      if (OB_TASK_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("[BACKUP_CLEAN]alreadly have log stream backup dag net in DagScheduler", K(ret));
      } else {
        LOG_WARN("failed to create log stream backup dag net", K(ret));
      }
    } else {
      LOG_INFO("[BACKUP_CLEAN]success to create log stream backup dag net", K(ret), K(param));
    }
  }

  return ret;
}

/******************ObLSBackupCleanDagNet*********************/
ObLSBackupCleanDagNetInitParam::ObLSBackupCleanDagNetInitParam()
  : trace_id_(),
    job_id_(0),
    tenant_id_(0),
    incarnation_(0),
    task_id_(0),
    ls_id_(),
    task_type_(),
    id_(0),
    dest_id_(0),
    round_id_(0)
{
}

bool ObLSBackupCleanDagNetInitParam::is_valid() const
{
  return ls_id_.is_valid()
      && !trace_id_.is_invalid()
      && (tenant_id_ != OB_INVALID_TENANT_ID)
      && (incarnation_ > 0) 
      && (task_id_ > 0) 
      && ObBackupCleanTaskType::is_valid(task_type_)
      && id_ > 0 
      && dest_id_ >= 0
      && round_id_ >= 0
      && job_id_ >0;
}

int ObLSBackupCleanDagNetInitParam::set(const obrpc::ObLSBackupCleanArg &args)
{
  int ret = OB_SUCCESS;
  trace_id_ = args.trace_id_;
  job_id_ = args.job_id_;
  tenant_id_ = args.tenant_id_;
  incarnation_ = args.incarnation_;
  task_id_ = args.task_id_;
  ls_id_ = args.ls_id_;
  task_type_ = args.task_type_;
  id_ = args.id_;
  dest_id_ = args.dest_id_;
  round_id_ = args.round_id_;
  return ret;
}

bool ObLSBackupCleanDagNetInitParam::operator == (const ObLSBackupCleanDagNetInitParam &other) const
{
  return trace_id_ == other.trace_id_
      && job_id_ == other.job_id_
      && tenant_id_ == other.tenant_id_
      && incarnation_ == other.incarnation_
      && task_id_ == other.task_id_
      && ls_id_.id() == other.ls_id_.id()
      && task_type_ == other.task_type_
      && id_ == other.id_
      && dest_id_ == other.dest_id_
      && round_id_ == other.round_id_; 
}

bool ObLSBackupCleanDagNetInitParam::operator != (const ObLSBackupCleanDagNetInitParam &other) const
{
  return !(*this == other); 
}

int64_t ObLSBackupCleanDagNetInitParam::hash() const
{ 
  int64_t hash_value = 0; 
  hash_value += trace_id_.hash();
  hash_value = common::murmurhash(&job_id_, sizeof(job_id_), hash_value);
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = common::murmurhash(&incarnation_, sizeof(incarnation_), hash_value);
  hash_value = common::murmurhash(&task_id_, sizeof(task_id_), hash_value);
  hash_value += ls_id_.hash();
  hash_value = common::murmurhash(&task_type_, sizeof(task_type_), hash_value);
  hash_value = common::murmurhash(&id_, sizeof(id_), hash_value);
  hash_value = common::murmurhash(&dest_id_, sizeof(dest_id_), hash_value);
  hash_value = common::murmurhash(&round_id_, sizeof(round_id_), hash_value);
  return hash_value; 
}

//****************ObLSBackupCleanDagNet*********************
ObLSBackupCleanDagNet::ObLSBackupCleanDagNet()
    : ObIDagNet(ObDagNetType::DAG_NET_TYPE_BACKUP_CLEAN),
      is_inited_(false),
      param_(),
      sql_proxy_(nullptr)
{
}

ObLSBackupCleanDagNet::~ObLSBackupCleanDagNet()
{
}

int ObLSBackupCleanDagNet::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObLSBackupCleanDagNetInitParam *init_param = static_cast<const ObLSBackupCleanDagNetInitParam*>(param);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup clean dag net is init twice", K(ret));
  } else if (OB_ISNULL(param) || !param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null or invalid", K(ret), KPC(init_param));
  } else if (OB_FAIL(this->set_dag_id(init_param->trace_id_))) {
    LOG_WARN("failed to set dag id", K(ret), KPC(init_param));
  } else if (FALSE_IT(param_ = *init_param)) {
  } else {
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupCleanDagNet::start_running()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSBackupCleanDag *clean_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  FLOG_INFO("[BACKUP_CLEAN]start running ls backup clean dagnet");
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean dag net do not init", K(ret));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(clean_dag))) {
    LOG_WARN("failed to alloc backup clean dag ", K(ret));
  } else if (OB_FAIL(clean_dag->init(this))) {
    LOG_WARN("failed to init backup clean dag", K(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*clean_dag))) {
    LOG_WARN("failed to add backup clean dag into dag net", K(ret));
  } else if (OB_FAIL(clean_dag->create_first_task())) {
    LOG_WARN("failed to create first task", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(clean_dag))) {
    LOG_WARN("failed to add dag", K(ret), K(*clean_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("Fail to add task", K(ret));
      ret = OB_EAGAIN;
    }
  } else {
    LOG_INFO("[BACKUP_CLEAN]succeed to schedule backup clean dag", K(*clean_dag));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(clean_dag)) {
      scheduler->free_dag(*clean_dag);
    }
  }
  return ret;
}

bool ObLSBackupCleanDagNet::operator == (const ObIDagNet &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (this->get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSBackupCleanDagNet &other_clean_dag = static_cast<const ObLSBackupCleanDagNet &>(other);
    if (OB_ISNULL(sql_proxy_)) {
      is_same = false;
    } else if (param_ != other_clean_dag.param_) {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObLSBackupCleanDagNet::hash() const
{
  int64_t hash_value = 0;
  const int64_t type = ObDagNetType::DAG_NET_TYPE_BACKUP_CLEAN;
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  hash_value += param_.hash();
  return hash_value;
}

int ObLSBackupCleanDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("log stream backup clean dag net do not init ", K(ret));
  } else if (OB_UNLIKELY(0 > param_.trace_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", K(ret), K(param_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "tenant_id=%lu, task_id=%ld, ls_id=%ld, task_type=%s, id=%ld, trace_id=%s",
          param_.tenant_id_,
          param_.task_id_,
          param_.ls_id_.id(),
          share::ObBackupCleanTaskType::get_str(param_.task_type_),
          param_.id_,
          task_id_str))) {
    LOG_WARN("failed to fill comment", K(ret), K(param_));
  }
  return ret;
}

int ObLSBackupCleanDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean dag net do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
          "tenant_id=%lu, task_id=%ld, ls_id=%ld, task_type=%s, id=%ld",
          param_.tenant_id_,
          param_.task_id_,
          param_.ls_id_.id(),
          share::ObBackupCleanTaskType::get_str(param_.task_type_),
          param_.id_))) {
    LOG_WARN("failed to fill comment", K(ret), K(param_));
  }
  return ret;
}

//******************ObLSBackupCleanDag*********************
ObLSBackupCleanDag::ObLSBackupCleanDag()
  : ObIDag(ObDagType::DAG_TYPE_BACKUP_CLEAN),
    is_inited_(false),
    param_(),
    result_(),
    sql_proxy_(nullptr)
{
}

ObLSBackupCleanDag::~ObLSBackupCleanDag()
{
}

int ObLSBackupCleanDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls backup clean dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                static_cast<int64_t>(param_.tenant_id_),
                                static_cast<int64_t>(param_.task_id_),
                                param_.ls_id_.id(),
                                static_cast<int64_t>(param_.id_),
                                "task_type", share::ObBackupCleanTaskType::get_str(param_.task_type_)))) {
    LOG_WARN("failed to add dag warning info param", K(ret));
  }
  return ret;
}

bool ObLSBackupCleanDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObLSBackupCleanDag &other_clean_dag = static_cast<const ObLSBackupCleanDag &>(other);
    if (OB_ISNULL(sql_proxy_)) {
      is_same = false;
    } else if (param_ == other_clean_dag.param_
        && result_ == other_clean_dag.result_) {
    } else {
      is_same = false;
    }
  }
  return is_same;
}

int64_t ObLSBackupCleanDag::hash() const
{
  return param_.hash();
}

int ObLSBackupCleanDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%s", to_cstring(param_.ls_id_)))) {
    LOG_WARN("failed to fill dag_key", K(ret), K_(param));
  }
  return ret;
}

int ObLSBackupCleanDag::init(ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObLSBackupCleanDagNet *clean_dag_net = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup clean dag init twice", K(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net should not be NULL", K(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP_CLEAN != dag_net->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag net type is unexpected", K(ret), KPC(dag_net));
  } else {
    clean_dag_net = static_cast<ObLSBackupCleanDagNet*>(dag_net);
    param_ = clean_dag_net->get_param();
    sql_proxy_ = GCTX.sql_proxy_;
    result_ = OB_SUCCESS;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupCleanDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObLSBackupCleanTask *task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", K(ret));
  } else if (OB_FAIL(task->init(param_, *sql_proxy_))) {
    LOG_WARN("failed to init log stream backup clean task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success finish create first task", K(*task));
  }
  return ret;
}

int ObLSBackupCleanDag::report_result()
{
  // do nothing
  return OB_SUCCESS;
}

//******************ObLSBackupCleanTask*********************
ObLSBackupCleanTask::ObLSBackupCleanTask()
  : ObITask(TASK_TYPE_BACKUP_CLEAN),
    is_inited_(false),
    sql_proxy_(NULL),
    backup_set_desc_(),
    backup_piece_info_(),
    backup_dest_(),
    task_id_(0),
    job_id_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    incarnation_(0),
    backup_set_id_(0),
    backup_piece_id_(0),
    round_id_(0),
    dest_id_(0),
    ls_id_(),
    task_type_(ObBackupCleanTaskType::TYPE::MAX) 
{
}

ObLSBackupCleanTask::~ObLSBackupCleanTask()
{
}

int ObLSBackupCleanTask::init(const ObLSBackupCleanDagNetInitParam &param, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup clean task init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param));
  } else if (FALSE_IT(sql_proxy_ = &sql_proxy)) {
  } else if (ObBackupCleanTaskType::BACKUP_SET == param.task_type_) {
    if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(
        *sql_proxy_, false/*need_lock*/, param.id_, param.incarnation_, param.tenant_id_, param.dest_id_, backup_set_desc_))) {
      LOG_WARN("failed to get backup set file", K(ret), K(param));
    } else if (!backup_set_desc_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup set info is invalid", K(ret), K_(backup_set_desc)); 
    } else if (FALSE_IT(backup_set_id_ = param.id_)) {
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, param.tenant_id_, backup_set_desc_.backup_path_, backup_dest_))) {
      LOG_WARN("failed to set backup dest", K(ret), K_(backup_set_desc)); 
    }
  } else if (ObBackupCleanTaskType::BACKUP_PIECE == param.task_type_) {
    ObArchivePersistHelper archive_table_op;
    if (OB_FAIL(archive_table_op.init(param.tenant_id_))) {
      LOG_WARN("failed to init archive piece attr", K(ret));
    } else if (OB_FAIL(archive_table_op.get_piece(*sql_proxy_, param.dest_id_, param.round_id_,
        param.id_, false/* need_lock */, backup_piece_info_))) {
      LOG_WARN("failed to get backup piece", K(ret));
    } else if (!backup_piece_info_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("backup piece info is invalid", K(ret), K_(backup_piece_info));
    } else if (FALSE_IT(backup_piece_id_ = param.id_)) {
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(*sql_proxy_, param.tenant_id_, backup_piece_info_.path_, backup_dest_))) {
      LOG_WARN("failed to set backup dest", K(ret), K_(backup_piece_info));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    task_id_ = param.task_id_;
    job_id_ = param.job_id_;
    tenant_id_ = param.tenant_id_;
    incarnation_ = param.incarnation_;
    round_id_ = param.round_id_;
    dest_id_ = param.dest_id_;
    ls_id_ = param.ls_id_;
    task_type_ = param.task_type_; 
    trace_id_ = param.trace_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObLSBackupCleanTask::process()
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[BACKUP_CLEAN]start process backup clean ls task", K(task_id_), K(ls_id_)); 
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup clean task do not init", K(ret));
  } else if (OB_FAIL(start_handle_ls_task_())) {
    LOG_WARN("failed to handle ls task", K(ret));
  }

  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = post_rpc_result_(ret))) {
    LOG_WARN("failed to post rpc result", K(tmp_ret));
  }

  return ret;
}

int ObLSBackupCleanTask::post_rpc_result_(const int64_t result)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader_addr;
  obrpc::ObBackupTaskRes clean_ls_res;
  clean_ls_res.job_id_ = job_id_; 
  clean_ls_res.task_id_ = task_id_;
  clean_ls_res.tenant_id_ = tenant_id_;
  clean_ls_res.src_server_ = GCTX.self_addr();
  clean_ls_res.ls_id_ = ls_id_;
  clean_ls_res.result_ = result;
  clean_ls_res.trace_id_ = trace_id_;
  clean_ls_res.dag_id_ = get_dag()->get_dag_id();
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  if (OB_ISNULL(GCTX.srv_rpc_proxy_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
  } else if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(
        cluster_id, meta_tenant_id, ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("failed to get leader address", K(ret));
  } else if (OB_FAIL(GCTX.srv_rpc_proxy_->to(leader_addr).report_backup_clean_over(clean_ls_res))) {
    LOG_WARN("failed to post backup ls data res", K(ret), K(clean_ls_res));
  } else {
    LOG_INFO("[BACKUP_CLEAN] success finish task post rpc result", K(clean_ls_res));
  }

  return ret;
}

int ObLSBackupCleanTask::start_handle_ls_task_()
{
  int ret = OB_SUCCESS;
  bool can = false;
  DEBUG_SYNC(BACKUP_DELETE_LS_TASK_STATUS_DOING);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS clean task do not init", K(ret));
  } else if (OB_FAIL(check_can_do_task_(can))) {
    LOG_WARN("failed to check can do task", K(ret));
  } else if (!can) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("task can't handle", K(ret), K(*this));
  } else if (OB_FAIL(do_ls_task())) {
    LOG_WARN("failed to do ls task", K(ret));
  }

  return ret;
}

int ObLSBackupCleanTask::check_can_do_task_(bool &can)
{
  int ret = OB_SUCCESS;
  can = true;
  ObBackupFileStatus file_status;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS clean task do not init", K(ret));
  } else if (ObBackupCleanTaskType::BACKUP_SET == task_type_) {
    if (!backup_set_desc_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup set info is invalid", K(ret), K(backup_set_desc_));
    } else if (ObBackupFileStatus::BACKUP_FILE_DELETING != backup_set_desc_.file_status_) {
      can = false;
    }
  } else if (ObBackupCleanTaskType::BACKUP_PIECE == task_type_) {
    if (!backup_piece_info_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup piece info is invalid", K(ret), K(backup_piece_info_));
    } else if (ObBackupFileStatus::BACKUP_FILE_DELETING != backup_piece_info_.file_status_) {
      can = false;
    }
  }

  return ret;
}

int ObLSBackupCleanTask::do_ls_task()
{
  int ret = OB_SUCCESS;
  ObBackupPath path;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS clean task do not init", K(ret));
  } else if (ObBackupCleanTaskType::BACKUP_SET == task_type_) {
    if (OB_FAIL(get_set_ls_path_(path))) {
      LOG_WARN("failed to get set ls path", K(ret));
    } else if (OB_FAIL(delete_backup_set_ls_files_(path))) {
      LOG_WARN("failed to delete backup set ls", K(ret));
    }
  } else if (ObBackupCleanTaskType::BACKUP_PIECE == task_type_) {
    if (OB_FAIL(get_piece_ls_path(path))) {
      LOG_WARN("failed to get piece ls path", K(ret));
    } else if (OB_FAIL(delete_backup_piece_ls_files_(path))) {
      LOG_WARN("failed to delete backup piece ls", K(ret), K(path));
    }
  }
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_DELETE_HANDLE_LS_TASK) OB_SUCCESS;
#endif
  FLOG_INFO("[BACKUP_CLEAN]finish do ls task", K(ret), K(path), K(*this));
  return ret;
}

int ObLSBackupCleanTask::delete_backup_piece_ls_files_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_piece_log_files_(path))) {
    LOG_WARN("failed to delete log files", K(ret), K(path));
  } else if (OB_FAIL(delete_piece_ls_meta_files_(path))) {
    LOG_WARN("failed to delete piece ls meta files", K(ret), K(path));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup log stream dir files", K(ret), K(path));
  }
  return ret;
}

int ObLSBackupCleanTask::delete_complement_log_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupPath complement_path;
  if (OB_FAIL(complement_path.init(path.get_ptr()))) {
    LOG_WARN("failed to init complement log path", K(ret), K(path));
  } else if (OB_FAIL(complement_path.join("complement_log", ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join complement log", K(ret), K(complement_path));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(complement_path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup log stream dir files", K(ret), K(complement_path));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success delete complement log", K(complement_path)); 
  } 
  return ret;
}

int ObLSBackupCleanTask::delete_sys_data_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  const char sys_data_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = "sys_data_turn_";
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  if (OB_FAIL(prefix_op.init(sys_data_prefix, strlen(sys_data_prefix)))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(sys_data_prefix));
  } else if (OB_FAIL(util.list_directories(path.get_obstr(), backup_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list files", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObBackupPath sys_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      ObIODirentEntry tmp_entry = d_entrys.at(i);
      sys_path.reset();
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(sys_path.init(path.get_ptr()))) {
        LOG_WARN("failed to init major path", K(ret), K(path));
      } else if (OB_FAIL(sys_path.join(tmp_entry.name_, ObBackupFileSuffix::NONE))) {
        LOG_WARN("failed to join major path", K(ret));
      } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(sys_path, backup_dest_.get_storage_info()))) {
        LOG_WARN("failed to delete backup log stream dir files", K(ret), K(path));
      } else {
        LOG_INFO("[BACKUP_CLEAN]success delete sys data turn", K(sys_path)); 
      } 
    }
  }
  return ret;
}

int ObLSBackupCleanTask::delete_major_data_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  const char major_data_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = "major_data_turn_";
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  if (OB_FAIL(prefix_op.init(major_data_prefix, strlen(major_data_prefix)))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(major_data_prefix));
  } else if (OB_FAIL(util.list_directories(path.get_obstr(), backup_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list files", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObBackupPath major_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      ObIODirentEntry tmp_entry = d_entrys.at(i);
      major_path.reset();
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(major_path.init(path.get_ptr()))) {
        LOG_WARN("failed to init major path", K(ret), K(path));
      } else if (OB_FAIL(major_path.join(tmp_entry.name_, ObBackupFileSuffix::NONE))) {
        LOG_WARN("failed to join major path", K(ret));
      } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(major_path, backup_dest_.get_storage_info()))) {
        LOG_WARN("failed to delete backup log stream dir files", K(ret), K(path));
      } else {
        LOG_INFO("[BACKUP_CLEAN]success delete major data turn", K(major_path)); 
      } 
    }
  }
  return ret;
}

int ObLSBackupCleanTask::delete_minor_data_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  const char minor_data_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = "minor_data_turn_";
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  if (OB_FAIL(prefix_op.init(minor_data_prefix, strlen(minor_data_prefix)))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(minor_data_prefix));
  } else if (OB_FAIL(util.list_directories(path.get_obstr(), backup_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list files", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObBackupPath minor_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      ObIODirentEntry tmp_entry = d_entrys.at(i);
      minor_path.reset();
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(minor_path.init(path.get_ptr()))) {
        LOG_WARN("failed to init minor path", K(ret), K(path));
      } else if (OB_FAIL(minor_path.join(tmp_entry.name_, ObBackupFileSuffix::NONE))) {
        LOG_WARN("failed to join minor path", K(ret));
      } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(minor_path, backup_dest_.get_storage_info()))) {
        LOG_WARN("failed to delete backup log stream dir files", K(ret), K(path));
      } else {
        LOG_INFO("[BACKUP_CLEAN]success delete minor data turn", K(minor_path)); 
      } 
    }
  }
  return ret;
}

int ObLSBackupCleanTask::delete_meta_info_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  ObArray<ObIODirentEntry> d_entrys;
  const char meta_info_prefix[OB_BACKUP_DIR_PREFIX_LENGTH] = "meta_info_turn_";
  ObDirPrefixEntryNameFilter prefix_op(d_entrys);
  if (OB_FAIL(prefix_op.init(meta_info_prefix, strlen(meta_info_prefix)))) {
    LOG_WARN("failed to init dir prefix", K(ret), K(meta_info_prefix));
  } else if (OB_FAIL(util.list_directories(path.get_obstr(), backup_dest_.get_storage_info(), prefix_op))) {
    LOG_WARN("failed to list files", K(ret));
  } else {
    ObIODirentEntry tmp_entry;
    ObBackupPath meta_path;
    for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
      ObIODirentEntry tmp_entry = d_entrys.at(i);
      meta_path.reset();
      if (OB_ISNULL(tmp_entry.name_)) {
        ret = OB_ERR_UNEXPECTED; 
        LOG_WARN("file name is null", K(ret));
      } else if (OB_FAIL(meta_path.init(path.get_ptr()))) {
        LOG_WARN("failed to init meta path", K(ret), K(path));
      } else if (OB_FAIL(meta_path.join(tmp_entry.name_, ObBackupFileSuffix::NONE))) {
        LOG_WARN("failed to join meta path", K(ret));
      } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(meta_path, backup_dest_.get_storage_info()))) {
        LOG_WARN("failed to delete backup log stream dir files", K(ret), K(path));
      } else {
        LOG_INFO("[BACKUP_CLEAN]success delete meta info turn", K(meta_path)); 
      } 
    }
  }
  return ret;
}

int ObLSBackupCleanTask::delete_log_stream_dir_(const share::ObBackupPath &ls_path)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(ObModIds::BACKUP);
  ObArray<common::ObString> file_names;
  ObBackupIoAdapter util;
  ObFileListArrayOp file_name_op(file_names, allocator);
  if (OB_FAIL(util.list_files(ls_path.get_ptr(), backup_dest_.get_storage_info(), file_name_op))) {
    LOG_WARN("failed to list files", K(ret), K(ls_path));
  } else if (0 != file_names.count()) {
    // do nothing
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir(ls_path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup dir", K(ret));
  }
  return ret;
}

int ObLSBackupCleanTask::delete_backup_set_ls_files_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS clean task do not init", K(ret));
  } else if (OB_FAIL(delete_complement_log_(path))) {
    LOG_WARN("failed to delete complement log", K(ret));
  } else if (OB_FAIL(delete_sys_data_(path))) {
    LOG_WARN("failed to delete complement log", K(ret));
  } else if (OB_FAIL(delete_major_data_(path))) {
    LOG_WARN("failed to delete major data", K(ret));
  } else if (OB_FAIL(delete_minor_data_(path))) {
    LOG_WARN("failed to delete minor data", K(ret));
  } else if (OB_FAIL(delete_meta_info_(path))) {
    LOG_WARN("failed to delete meta info", K(ret)); 
  } else if (OB_FAIL(delete_log_stream_dir_(path))) {
    if (OB_DIR_NOT_EXIST == ret) {
      LOG_INFO("dir is not exist", K(ret), K(path), K(*this));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to delete backup log stream dir", K(ret), K(path));
    }
  } else {
    LOG_INFO("[BACKUP_CLEAN]success finish delete backup set files", K(path)); 
  }
  return ret;
}

int ObLSBackupCleanTask::get_set_ls_path_(ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupSetDesc desc;
  desc.backup_set_id_ = backup_set_desc_.backup_set_id_;
  desc.backup_type_ = backup_set_desc_.backup_type_;
  char log_sream_str[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS clean task do not init", K(ret));
  } else if (OB_FAIL(ObBackupPathUtil::get_backup_set_dir_path(backup_dest_, desc, path))) {
    LOG_WARN("failed to get tenant data backup set dir path", K(ret));
  } else if (OB_FAIL(databuff_printf(log_sream_str, sizeof(log_sream_str), "logstream_%ld", ls_id_.id()))) {
    LOG_WARN("failed to get logsream_str", K(ret));
  } else if (OB_FAIL(path.join(log_sream_str, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join log stream dir", K(ret));
  } 

  return ret;
}

int ObLSBackupCleanTask::delete_piece_log_files_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupPath log_path;
  if (OB_FAIL(log_path.init(path.get_ptr()))) {
    LOG_WARN("failed to init piece log path", K(ret), K(path));
  } else if (OB_FAIL(log_path.join("log", ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join log", K(ret), K(log_path));
  } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(log_path, backup_dest_.get_storage_info()))) {
    LOG_WARN("failed to delete backup log stream dir files", K(ret), K(path));
  } else {
    LOG_INFO("[BACKUP_CLEAN]success delete log files", K(log_path));
  }
  return ret;
}

int ObLSBackupCleanTask::delete_piece_ls_meta_files_(const ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  ObBackupPath meta_path;
  ObArchiveLSMetaType meta_type;
  do {
    meta_path.reset();
    if (OB_FAIL(meta_type.get_next_type())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next type", K(ret), K(meta_type));
      }
    } else if (!meta_type.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta type is invalid", K(ret), K(meta_type));
    } else if (OB_FAIL(meta_path.init(path.get_ptr()))) {
      LOG_WARN("failed to init complement log path", K(ret), K(path));
    } else if (OB_FAIL(meta_path.join(meta_type.get_type_str(), ObBackupFileSuffix::NONE))) {
      LOG_WARN("failed to join meta type", K(ret), K(path));
    } else if (OB_FAIL(ObBackupCleanUtil::delete_backup_dir_files(meta_path, backup_dest_.get_storage_info()))) {
      LOG_WARN("failed to delete backup log stream dir files", K(ret), K(meta_path));
    } else {
      LOG_INFO("[BACKUP_CLEAN]success delete meta files", K(meta_path));
    }
  } while (OB_SUCC(ret));
  return ret;
}

int ObLSBackupCleanTask::get_piece_ls_path(ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  char log_sream_str[OB_BACKUP_DIR_PREFIX_LENGTH] = { 0 };
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LS clean task do not init", K(ret));
  } else if (OB_FAIL(ObArchivePathUtil::get_piece_dir_path(
      backup_dest_, dest_id_, backup_piece_info_.key_.round_id_, backup_piece_id_, path))) {
    LOG_WARN("failed to get tenant clog piece dir path", K(ret));
  } else if (OB_FAIL(databuff_printf(log_sream_str, sizeof(log_sream_str), "logstream_%ld", ls_id_.id()))) {
    LOG_WARN("failed to get log_sream_str", K(ret));
  } else if (OB_FAIL(path.join(log_sream_str, ObBackupFileSuffix::NONE))) {
    LOG_WARN("failed to join log stream dir", K(ret));
  }
  return ret;
}

}
}
