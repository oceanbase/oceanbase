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

#include "storage/backup/ob_backup_validate_dag_scheduler.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server_struct.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "share/backup/ob_backup_connectivity.h"
#include "storage/restore/ob_tenant_restore_info_mgr.h"
#include "share/backup/ob_backup_validate_table_operator.h"
#include "src/storage/restore/ob_tenant_restore_info_mgr.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
/*
**************************ObBackupValidateBaseDag**************************
*/
ObBackupValidateBaseDag::ObBackupValidateBaseDag()
  : ObIDag(ObDagType::DAG_TYPE_BACKUP_VALIDATE),
    is_inited_(false),
    param_(),
    storage_info_(),
    backup_dest_(),
    report_ctx_(),
    ctx_(nullptr),
    allocator_()
{
}

ObBackupValidateBaseDag::~ObBackupValidateBaseDag()
{
}

bool ObBackupValidateBaseDag::operator ==(const share::ObIDag &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else if (get_type() != other.get_type()) {
    is_equal = false;
  } else {
    const ObBackupValidateBaseDag &other_dag = static_cast<const ObBackupValidateBaseDag &>(other);
    if (get_dag_type() != other_dag.get_dag_type()) {
      is_equal = false;
    } else if (param_ == other_dag.param_) {
      is_equal = true;
    } else {
      is_equal = false;
    }
  }
  return is_equal;
}

uint64_t ObBackupValidateBaseDag::hash() const
{
  uint64_t hash_value = 0;
  const int64_t type = static_cast<int64_t>(get_dag_type());
  hash_value = param_.hash();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  return hash_value;
}

int ObBackupValidateBaseDag::init(share::ObIDagNet *dag_net)
{
  int ret = OB_SUCCESS;
  ObBackupValidateDagNet *validate_dag_net = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateBaseDag init twice", KR(ret));
  } else if (OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dag_net));
  } else if (ObDagNetType::DAG_NET_TYPE_BACKUP_VALIDATE != dag_net->get_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(dag_net));
  } else {
    validate_dag_net = static_cast<ObBackupValidateDagNet *>(dag_net);
    char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
    if (OB_FAIL(param_.assign(validate_dag_net->get_param()))) {
      LOG_WARN("failed to assign param", KR(ret), KPC(dag_net));
    } else if (OB_FAIL(storage_info_.assign(validate_dag_net->get_storage_info()))) {
      LOG_WARN("failed to assign storage info", KR(ret), KPC(dag_net));
    } else if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
      LOG_WARN("failed to get root path", KR(ret), K(param_.validate_path_));
    } else if (OB_FAIL(backup_dest_.set(raw_path, &storage_info_))) {
      LOG_WARN("failed to set backup dest", KR(ret), K(raw_path), K(storage_info_));
    } else {
      report_ctx_ = validate_dag_net->get_report_ctx();
      ctx_ = &validate_dag_net->get_task_context();
      is_inited_ = true;
      allocator_.set_attr(ObMemAttr(param_.tenant_id_, ObModIds::BACKUP));
    }
  }
  return ret;
}

int ObBackupValidateBaseDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateBaseDag do not init", KR(ret), K_(is_inited), K_(param));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), static_cast<int64_t>(param_.tenant_id_),
                                              static_cast<int64_t>(param_.task_id_),
                                              static_cast<int64_t>(param_.validate_id_), param_.ls_id_.id(),
                                              static_cast<int64_t>(get_dag_type())))) {
    LOG_WARN("failed to fill info param", KR(ret), K_(param));
  }
  return ret;
}

int ObBackupValidateBaseDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ls_id="))) {
    LOG_WARN("failed to databuff printf ls_id", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, param_.ls_id_.id()))) {
    LOG_WARN("failed to databuff printf ls_id", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "dag_type="))) {
    LOG_WARN("failed to databuff printf dag_type", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, static_cast<int64_t>(get_dag_type())))) {
    LOG_WARN("failed to databuff printf dag_type", KR(ret));
  }
  return ret;
}

/*
**************************ObBackupValidateScheduler**************************
*/
int ObLSBackupValidateScheduler::schedule_backup_validate_dag(const obrpc::ObBackupValidateLSArg &args)
{
  int ret = OB_SUCCESS;
  ObBackupValidateDagNetInitParam param;
  MTL_SWITCH(args.tenant_id_) {
    ObTenantDagScheduler *dag_scheduler = nullptr;
    ObBackupValidateDagNet *dag_net = nullptr;
    if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret));
    } else if (OB_FAIL(param.set(args))) {
      LOG_WARN("failed to set param", KR(ret), K(args));
    } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObBackupValidateDagNet>(&param))) {
      LOG_WARN("failed to create and add dag net", KR(ret), K(param));
    } else {
      LOG_INFO("success schedule backup validate dag", K(args), K(param));
    }
  }
  return ret;
}

/*
**************************ObBackupValidateDagNet**************************
*/
ObBackupValidateDagNet::ObBackupValidateDagNet()
  : ObIDagNet(ObDagNetType::DAG_NET_TYPE_BACKUP_VALIDATE),
    is_inited_(false),
    param_(),
    report_ctx_(),
    task_context_()
{
}

ObBackupValidateDagNet::~ObBackupValidateDagNet()
{
}

int ObBackupValidateDagNet::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObBackupValidateDagNet init twice", KR(ret));
  } else {
    const ObBackupValidateDagNetInitParam *validate_param = nullptr;
    share::ObBackupDest backup_dest;
    if (OB_ISNULL(param)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(param));
    } else if (OB_ISNULL(GCTX.sql_proxy_) || OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.srv_rpc_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, sql proxy must not be nullptr", KR(ret));
    } else if (FALSE_IT(validate_param = static_cast<const ObBackupValidateDagNetInitParam *>(param))) {
    } else if (!validate_param->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid validate param", KR(ret), KPC(validate_param));
    } else if (OB_FAIL(this->set_dag_id(validate_param->trace_id_))) {
      LOG_WARN("failed to set dag id", KR(ret), KPC(validate_param));
    } else if (OB_FAIL(param_.assign(*validate_param))) {
      LOG_WARN("failed to assign param", KR(ret), KPC(validate_param));
    } else if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*GCTX.sql_proxy_, param_.tenant_id_,
                                                                                      param_.dest_id_, backup_dest))) {
      LOG_WARN("failed to get backup dest", KR(ret), K(param));
    } else if (OB_FAIL(storage_info_.assign(*backup_dest.get_storage_info()))) {
      LOG_WARN("failed to assign storage info", KR(ret), K(backup_dest));
    } else if (OB_FAIL(task_context_.init())) {
      LOG_WARN("failed to init task context", KR(ret));
    } else {
      // Initialize report_ctx_
      report_ctx_.location_service_ = GCTX.location_service_;
      report_ctx_.sql_proxy_ = GCTX.sql_proxy_;
      report_ctx_.rpc_proxy_ = GCTX.srv_rpc_proxy_;
      if (!report_ctx_.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("report ctx is not valid", KR(ret));
      } else {
        is_inited_ = true;
        LOG_INFO("success init backup validate dag net", KPC(validate_param));
      }
    }
  }
  return ret;
}

int ObBackupValidateDagNet::start_running()
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *dag_scheduler = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateDagNet not init", KR(ret));
  } else if (!param_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), K_(param));
  } else if (OB_FAIL(schedule_prepare_validate_())) {
    LOG_WARN("failed to schedule prepare validate chain", KR(ret));
  }
  if (OB_FAIL(ret)) {
    obrpc::ObBackupTaskRes res;
    int tmp_ret = OB_SUCCESS;
    const common::ObAddr src_server = GCTX.self_addr();
    res.job_id_ = param_.job_id_;
    res.task_id_ = param_.task_id_;
    res.tenant_id_ = param_.tenant_id_;
    res.ls_id_ = param_.ls_id_;
    res.result_ = ret;
    res.src_server_ = src_server;
    res.trace_id_ = param_.trace_id_;
    res.dag_id_ = get_dag_id();
    if (!res.is_valid()) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to init backup task res", KR(ret), KR(tmp_ret), K(src_server), K(param_));
    } else if (OB_TMP_FAIL(ObBackupValidateObUtils::report_validate_over(res, report_ctx_))) {
      LOG_WARN("failed to report validate over", KR(ret), KR(tmp_ret), K(res), K(report_ctx_));
    }
  }
  return ret;
}

bool ObBackupValidateDagNet::operator == (const share::ObIDagNet &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else if (this->get_type() != other.get_type()) {
    is_equal = false;
  } else {
    const ObBackupValidateDagNet &other_dag_net = static_cast<const ObBackupValidateDagNet &>(other);
    if (!report_ctx_.is_valid()) {
      is_equal = false;
    } else if (param_ == other_dag_net.param_) {
      is_equal = true;
    } else {
      is_equal = false;
    }
  }
  return is_equal;
}

uint64_t ObBackupValidateDagNet::hash() const
{
  uint64_t hash_value = 0;
  const int64_t type = ObDagNetType::DAG_NET_TYPE_BACKUP_VALIDATE;
  hash_value = param_.hash();
  hash_value = common::murmurhash(&type, sizeof(type), hash_value);
  return hash_value;
}

int ObBackupValidateDagNet::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  const int64_t MAX_TRACE_ID_LENGTH = 64;
  char task_id_str[MAX_TRACE_ID_LENGTH] = { 0 };
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(0 > param_.trace_id_.to_string(task_id_str, MAX_TRACE_ID_LENGTH))) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("failed to get trace id string", KR(ret), K_(param));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "tenant_id=%lu, job_id=%ld, task_id=%ld, ls_id=%ld, task_type=%s, validate_level=%s, validate_id=%ld, trace_id=%s",
      param_.tenant_id_, param_.job_id_, param_.task_id_, param_.ls_id_.id(),
      param_.task_type_.get_str(),
      param_.validate_level_.get_str(),
      param_.validate_id_,
      task_id_str))) {
    LOG_WARN("failed to fill comment", KR(ret), K_(param));
  }
  return ret;
}

int ObBackupValidateDagNet::fill_dag_net_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
      "tenant_id=%lu, job_id=%ld, task_id=%ld, ls_id=%ld, task_type=%s, validate_level=%s, validate_id=%ld",
      param_.tenant_id_, param_.job_id_, param_.task_id_, param_.ls_id_.id(),
      param_.task_type_.get_str(),
      param_.validate_level_.get_str(),
      param_.validate_id_))) {
    LOG_WARN("failed to fill comment", KR(ret), K_(param));
  }
  return ret;
}

int ObBackupValidateDagNet::schedule_prepare_validate_()
{
  int ret = OB_SUCCESS;
  ObBackupValidatePrepareDag *prepare_dag = nullptr;
  ObTenantDagScheduler *dag_scheduler = nullptr;

  if (OB_ISNULL(dag_scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag scheduler must not be nullptr", KR(ret), KP(dag_scheduler));
  } else if (OB_FAIL(dag_scheduler->alloc_dag(prepare_dag, true/*is_ha_dag*/))) {
    LOG_WARN("failed to alloc dag", KR(ret));
  } else if (OB_ISNULL(prepare_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, dag must not be nullptr", KR(ret));
  } else if (OB_FAIL(prepare_dag->init(this))) {
    LOG_WARN("failed to init dag", KR(ret));
  } else if (OB_FAIL(prepare_dag->create_first_task())) {
    LOG_WARN("failed to create first task for dag", KR(ret));
  } else if (OB_FAIL(add_dag_into_dag_net(*prepare_dag))) {
    LOG_WARN("failed to add dag into dag net", KR(ret), KPC(prepare_dag));
  } else if (OB_FAIL(dag_scheduler->add_dag(prepare_dag))) {
    LOG_WARN("failed to add dag to scheduler", KR(ret), KPC(prepare_dag));
  } else {
    LOG_INFO("success schedule basic validate chain", K_(param));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(prepare_dag)) {
    dag_scheduler->free_dag(*prepare_dag);
    prepare_dag = nullptr;
  }
  return ret;
}
/*
**************************ObBackupValidatePrepareDag**************************
*/
ObBackupValidatePrepareDag::ObBackupValidatePrepareDag()
  : ObBackupValidateBaseDag()
{
}

ObBackupValidatePrepareDag::~ObBackupValidatePrepareDag()
{
}

int ObBackupValidatePrepareDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupValidatePrepareTask *task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidatePrepareDag not init", KR(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc prepare task", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, task must not be nullptr", KR(ret));
  } else if (OB_FAIL(task->init(param_, report_ctx_, storage_info_, ctx_))) {
    LOG_WARN("failed to init prepare task", KR(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add prepare task", KR(ret));
  } else {
    LOG_INFO("success create prepare task", KPC(task));
  }
  return ret;
}

/*
**************************ObBackupValidateBasicDag**************************
*/
ObBackupValidateBasicDag::ObBackupValidateBasicDag()
  : ObBackupValidateBaseDag()
{
}

ObBackupValidateBasicDag::~ObBackupValidateBasicDag()
{
}

int ObBackupValidateBasicDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupValidateBasicTask *task = nullptr;
  int64_t file_group_id = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateBasicDag not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), KP_(ctx));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc basic task", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, task must not be nullptr", KR(ret));
  } else if (OB_FAIL(ctx_->get_next_task_id(file_group_id))) {
    LOG_WARN("failed to get next task id", KR(ret));
  } else {
    if (OB_FAIL(task->init(param_, file_group_id, report_ctx_, storage_info_))) {
      LOG_WARN("failed to init basic task", KR(ret), K(file_group_id));
    } else if (OB_FAIL(add_task(*task))) {
      LOG_WARN("failed to add basic task", KR(ret));
    } else {
      LOG_INFO("success create basic task", KPC(task));
    }
  }
  return ret;
}

/*
**************************ObBackupValidateBackupSetPhysicalDag**************************
*/
ObBackupValidateBackupSetPhysicalDag::ObBackupValidateBackupSetPhysicalDag()
  : ObBackupValidateBaseDag(),
    meta_index_store_(),
    succeed_set_dest_info_(false)
{
}

ObBackupValidateBackupSetPhysicalDag::~ObBackupValidateBackupSetPhysicalDag()
{
  if (succeed_set_dest_info_) {
    ObTenantBackupDestInfoMgr *mgr = MTL(ObTenantBackupDestInfoMgr *);
    if (OB_NOT_NULL(mgr)) {
      mgr->reset();
      succeed_set_dest_info_ = false;
    }
  }
}

int ObBackupValidateBackupSetPhysicalDag::create_first_task()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateBackupSetPhysicalDag not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), KP_(ctx));
  } else {
    ObBackupValidateBackupSetPhysicalTask *task = nullptr;
    share::ObBackupDest backup_set_dest;
    ObBackupDataStore store;
    char raw_path[OB_MAX_BACKUP_PATH_LENGTH] = {0};
    ObExternBackupSetInfoDesc backup_set_info;
    ObBackupValidateBaseDag *base_dag = static_cast<ObBackupValidateBaseDag *>(this);
    int64_t task_id = -1;
    if (OB_FAIL(ctx_->get_next_task_id(task_id))) {
      LOG_WARN("failed to get next task id", KR(ret));
    } else if (OB_FAIL(ObBackupUtils::get_raw_path(param_.validate_path_.ptr(), raw_path, OB_MAX_BACKUP_PATH_LENGTH))) {
      LOG_WARN("failed to get root path", KR(ret), K(param_.validate_path_));
    } else if (OB_FAIL(backup_set_dest.set(raw_path, &storage_info_))) {
      LOG_WARN("failed to set storage path", KR(ret), K(param_.validate_path_), K_(storage_info));
    } else if (OB_FAIL(store.init(backup_set_dest))) {
      LOG_WARN("failed to init backup data store", KR(ret), K(backup_set_dest));
    } else if (OB_FAIL(store.read_backup_set_info(backup_set_info))) {
      LOG_WARN("failed to read backup set info", KR(ret), K(backup_set_dest));
    } else {
      ObTenantBackupDestInfoMgr *mgr = MTL(ObTenantBackupDestInfoMgr *);
      common::ObArray<share::ObBackupSetBriefInfo> backup_set_list;
      ObBackupSetDesc backup_set_desc;
      backup_set_desc.backup_set_id_ = backup_set_info.backup_set_file_.backup_set_id_;
      backup_set_desc.backup_type_ = backup_set_info.backup_set_file_.backup_type_;
      backup_set_desc.min_restore_scn_ = backup_set_info.backup_set_file_.min_restore_scn_;
      backup_set_desc.total_bytes_ = backup_set_info.backup_set_file_.stats_.output_bytes_;
      if (OB_ISNULL(mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant restore info mgr is null", KR(ret));
      } else if (OB_FAIL(get_dependency_backup_sets_(backup_set_dest, backup_set_desc, backup_set_list))) {
        LOG_WARN("failed to get dependency backup sets", KR(ret), K(backup_set_desc));
      } else if (OB_FAIL(mgr->set_backup_set_info(backup_set_list, param_.dest_id_,
                                                      ObTenantBackupDestInfoMgr::InfoType::VALIDATE))) {
        LOG_WARN("failed to set validate mode", KR(ret));
      } else {
        succeed_set_dest_info_ = true;
      }
    }
    if (FAILEDx(ObBackupValidateObUtils::init_meta_index_store(backup_set_info, param_, false/*is_sec_meta*/,
                                                                   store, meta_index_store_))) {
      LOG_WARN("failed to init meta index store", KR(ret), K(backup_set_info), K_(param));
    } else if (OB_FAIL(alloc_task(task))) {
      LOG_WARN("failed to alloc backup set physical task", KR(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, task must not be nullptr", KR(ret));
    } else if (OB_FAIL(task->init(*base_dag, task_id, backup_set_info, &meta_index_store_))) {
      LOG_WARN("failed to init backup set physical task", KR(ret), K(backup_set_info), K_(param), K(task_id));
    } else if (OB_FAIL(add_task(*task))) {
      LOG_WARN("failed to add backup set physical task", KR(ret));
    } else {
      LOG_INFO("success create backup set physical task", KPC(task));
    }
  }
  return ret;
}

int ObBackupValidateBackupSetPhysicalDag::get_dependency_backup_sets_(
    const share::ObBackupDest &current_set_dest,
    const share::ObBackupSetDesc &current_set_desc,
    common::ObIArray<share::ObBackupSetBriefInfo> &backup_set_list)
{
  int ret = OB_SUCCESS;
  backup_set_list.reset();
  share::ObBackupValidateJobAttr job_attr;
  share::ObBackupDest backup_dest;
  bool is_set_dest = false;

  if (!current_set_dest.is_valid() || !current_set_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(current_set_dest), K(current_set_desc));
  } else if (OB_ISNULL(report_ctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObBackupValidateJobOperator::get_job(*report_ctx_.sql_proxy_, false/*need_lock*/,
                                              param_.tenant_id_, param_.job_id_, false/*is_initiator*/, job_attr))) {
    LOG_WARN("fail to get validate job", K(ret), K_(param));
  } else {
    is_set_dest = (job_attr.path_type_.type_ == share::ObBackupValidatePathType::BACKUP_SET_DEST);
  }

  if (OB_SUCC(ret)) {
    if (current_set_desc.backup_type_.is_full_backup() || is_set_dest) {
      share::ObBackupSetBriefInfo brief_info;
      if (OB_FAIL(brief_info.backup_set_desc_.assign(current_set_desc))) {
        LOG_WARN("fail to assign backup set desc", K(ret));
      } else if (OB_FAIL(current_set_dest.get_backup_dest_str(
          brief_info.backup_set_path_.ptr(), brief_info.backup_set_path_.capacity()))) {
        LOG_WARN("fail to get backup set path", K(ret));
      } else if (OB_FAIL(backup_set_list.push_back(brief_info))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else {
      ObBackupDataStore store;
      common::ObArray<share::ObBackupSetBriefInfo> all_backup_set_list;
      share::SCN restore_start_scn;
      ObString passwd_array;
      const share::SCN &restore_scn = current_set_desc.min_restore_scn_;
      bool found = false;
      if (OB_FAIL(share::ObBackupStorageInfoOperator::get_backup_dest_by_dest_id(*report_ctx_.sql_proxy_,
                                                                param_.tenant_id_, param_.dest_id_, backup_dest))) {
        LOG_WARN("fail to get backup dest by dest id", K(ret), K_(param));
      } else if (OB_FAIL(store.init(backup_dest))) {
        LOG_WARN("fail to init backup data store", K(ret), K(backup_dest));
      } else if (OB_FAIL(store.get_backup_set_array(false/*check_passwd*/, passwd_array, restore_scn,
                                                        restore_start_scn, all_backup_set_list))) {
        LOG_WARN("fail to get backup set array", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < all_backup_set_list.count(); ++i) {
        const share::ObBackupSetBriefInfo &info = all_backup_set_list.at(i);
        if (info.backup_set_desc_.backup_set_id_ > current_set_desc.backup_set_id_) {
          break;
        }
        if (OB_FAIL(backup_set_list.push_back(info))) {
          LOG_WARN("fail to push back", K(ret));
        } else if (info.backup_set_desc_.backup_set_id_ == current_set_desc.backup_set_id_) {
          found = true;
          break;
        }
      }
      if (OB_SUCC(ret) && !found) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("current backup set not found in backup dest", K(ret), K(current_set_desc));
      }
    }
  }
  return ret;
}

/*
**************************ObBackupValidateArchivePiecePhysicalDag**************************
*/
ObBackupValidateArchivePiecePhysicalDag::ObBackupValidateArchivePiecePhysicalDag()
  : ObBackupValidateBaseDag()
{
}

ObBackupValidateArchivePiecePhysicalDag::~ObBackupValidateArchivePiecePhysicalDag()
{
}

int ObBackupValidateArchivePiecePhysicalDag::create_first_task()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateArchivePiecePhysicalDag not init", KR(ret));
  } else if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", KR(ret), KP_(ctx));
  } else {
    ObBackupValidateArchivePiecePhysicalTask *task = nullptr;
    int64_t lsn_group_id = -1;
    if (OB_FAIL(ctx_->get_next_task_id(lsn_group_id))) {
      LOG_WARN("failed to get next task id", KR(ret));
    } else if (OB_FAIL(alloc_task(task))) {
      LOG_WARN("failed to alloc archive piece physical task", KR(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, task must not be nullptr", KR(ret));
    } else if (OB_FAIL(task->init(param_, report_ctx_, storage_info_, lsn_group_id))) {
      LOG_WARN("failed to init archive piece physical task", KR(ret));
    } else if (OB_FAIL(add_task(*task))) {
      LOG_WARN("failed to add archive piece physical task", KR(ret));
    } else {
      LOG_INFO("success create archive piece physical task", KPC(task));
    }
  }
  return ret;
}

/*
**************************ObBackupValidateFinishDag**************************
*/
ObBackupValidateFinishDag::ObBackupValidateFinishDag()
  : ObBackupValidateBaseDag()
{
}

ObBackupValidateFinishDag::~ObBackupValidateFinishDag()
{
}

int ObBackupValidateFinishDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObBackupValidateFinishTask *task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObBackupValidateFinishDag not init", KR(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc finish task", KR(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, task must not be nullptr", KR(ret));
  } else if (OB_FAIL(task->init(param_, report_ctx_))) {
    LOG_WARN("failed to init finish task", KR(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add finish task", KR(ret));
  } else {
    LOG_INFO("success create finish task", KPC(task));
  }

  return ret;
}

}//storage
}//oceanbase