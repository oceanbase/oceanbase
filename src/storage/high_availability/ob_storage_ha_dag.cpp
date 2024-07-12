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
#include "ob_storage_ha_dag.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

/******************ObStorageHAResultMgr*********************/
ObStorageHAResultMgr::ObStorageHAResultMgr()
  : lock_(),
    result_(OB_SUCCESS),
    retry_count_(0),
    allow_retry_(true),
    failed_task_id_list_()
{
}

ObStorageHAResultMgr::~ObStorageHAResultMgr()
{
}

int ObStorageHAResultMgr::set_result(
    const int32_t result,
    const bool allow_retry,
    const enum ObDagType::ObDagTypeEnum type)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  const uint64_t tenant_id = MTL_ID();
  if (OB_SUCCESS == result_ && OB_SUCCESS != result) {
    result_ = result;
    allow_retry_ = allow_retry;
    if (NULL != ObCurTraceId::get_trace_id() && OB_FAIL(failed_task_id_list_.push_back(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("failed to push trace id into array", K(ret));
    } else {
      SERVER_EVENT_ADD("storage_ha", "set_first_result",
        "tenant_id", tenant_id,
        "result", result,
        "allow_retry", allow_retry,
        "retry_count", retry_count_,
        "failed_task_id", to_cstring(failed_task_id_list_),
        "dag_type", OB_DAG_TYPES[type].dag_type_str_);
      FLOG_INFO("set first result", K(result), K(allow_retry), K(retry_count_), K(failed_task_id_list_));
    }
  }
  return ret;
}

int ObStorageHAResultMgr::get_result(int32_t &result)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  result = result_;
  return ret;
}

bool ObStorageHAResultMgr::is_failed() const
{
  common::SpinRLockGuard guard(lock_);
  return OB_SUCCESS != result_;
}

int ObStorageHAResultMgr::check_allow_retry(bool &allow_retry)
{
  int ret = OB_SUCCESS;
  allow_retry = false;
  common::SpinRLockGuard guard(lock_);
  if (!allow_retry_) {
    allow_retry = false;
  } else {
    allow_retry = ObMigrationUtils::is_need_retry_error(result_);
    if (allow_retry && retry_count_ < MAX_RETRY_CNT) {
      //do nohitng
    } else {
      allow_retry = false;
    }
  }
  return ret;
}

int ObStorageHAResultMgr::get_first_failed_task_id(share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  if (OB_SUCCESS != result_) {
    ARRAY_FOREACH(failed_task_id_list_, i) {
      task_id.set(failed_task_id_list_.at(i));
      break;
    }

    if (task_id.is_invalid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get first failed task id", K(ret));
    }
  }
  return ret;
}

void ObStorageHAResultMgr::reuse()
{
  common::SpinWLockGuard guard(lock_);
  retry_count_++;
  result_ = OB_SUCCESS;
  allow_retry_ = true;
}

void ObStorageHAResultMgr::reset()
{
  common::SpinWLockGuard guard(lock_);
  result_ = OB_SUCCESS;
  retry_count_ = 0;
  allow_retry_ = true;
}

int ObStorageHAResultMgr::get_retry_count(int32_t &retry_count)
{
  int ret = OB_SUCCESS;
  common::SpinRLockGuard guard(lock_);
  retry_count = retry_count_;
  return ret;
}

/******************ObIHADagNetCtx*********************/
ObIHADagNetCtx::ObIHADagNetCtx()
  : result_mgr_()
{
}

ObIHADagNetCtx::~ObIHADagNetCtx()
{
}

int ObIHADagNetCtx::set_result(
    const int32_t result,
    const bool need_retry,
    const enum share::ObDagType::ObDagTypeEnum type)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ha dag net ctx is not init", K(ret), K(*this));
  } else if (OB_FAIL(result_mgr_.set_result(result, need_retry, type))) {
    LOG_WARN("failed to set result", K(ret), K(result), K(*this));
  }
  return ret;
}

bool ObIHADagNetCtx::is_failed() const
{
  return result_mgr_.is_failed();
}

int ObIHADagNetCtx::check_allow_retry(bool &allow_retry)
{
  int ret = OB_SUCCESS;
  allow_retry = false;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ha dag net ctx is invalid", K(ret), K(*this));
  } else if (OB_FAIL(result_mgr_.check_allow_retry(allow_retry))) {
    LOG_WARN("failed to check need retry", K(ret), K(*this));
  }
  return ret;
}

int ObIHADagNetCtx::get_result(int32_t &result)
{
  int ret = OB_SUCCESS;
  result = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ha dag net ctx is invalid", K(ret), K(*this));
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  }
  return ret;
}

int ObIHADagNetCtx::get_first_failed_task_id(share::ObTaskId &task_id)
{
  int ret = OB_SUCCESS;
  task_id.reset();
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ha dag net ctx is invalid", K(ret), K(*this));
  } else if (OB_FAIL(result_mgr_.get_first_failed_task_id(task_id))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  }
  return ret;
}

void ObIHADagNetCtx::reuse()
{
  result_mgr_.reuse();
}

void ObIHADagNetCtx::reset()
{
  result_mgr_.reset();
}

int ObIHADagNetCtx::check_is_in_retry(bool &is_in_retry)
{
  int ret = OB_SUCCESS;
  is_in_retry = false;
  int32_t retry_count = 0;

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ha dag net ctx is invalid", K(ret), K(*this));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  } else {
    is_in_retry = retry_count > 0;
  }
  return ret;
}

int ObIHADagNetCtx::get_retry_count(int32_t &retry_count)
{
  int ret = OB_SUCCESS;
  retry_count = 0;

  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ha dag net ctx is invalid", K(ret), K(*this));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to get result", K(ret), K(*this));
  }
  return ret;
}

/******************ObStorageHADag*********************/
ObStorageHADag::ObStorageHADag(
    const share::ObDagType::ObDagTypeEnum &dag_type)
  : ObIDag(dag_type),
    ha_dag_net_ctx_(),
    result_mgr_(),
    compat_mode_(lib::Worker::CompatMode::MYSQL)
{
}

ObStorageHADag::~ObStorageHADag()
{
}

int ObStorageHADag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get ha dag net ctx result", K(tmp_ret), KPC(ha_dag_net_ctx_));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner set status for retry failed", K(ret), KPC(ha_dag_net_ctx_));
    }
  } else {
    LOG_INFO("start retry", KPC(this));
    result_mgr_.reuse();
    if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

bool ObStorageHADag::check_can_retry()
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = result_mgr_.check_allow_retry(bool_ret))) {
    bool_ret = false;
    LOG_ERROR("failed to check need retry", K(ret), K(*this));
  }
  return bool_ret;
}

int ObStorageHADag::set_result(
    const int32_t result,
    const bool allow_retry,
    const enum share::ObDagType::ObDagTypeEnum type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (OB_SUCCESS == result) {
    //do nothing
  } else if (OB_FAIL(result_mgr_.set_result(result, allow_retry, type))) {
    LOG_WARN("failed to set result", K(ret), K(result), KPC(ha_dag_net_ctx_));
  }
  return ret;
}

int ObStorageHADag::report_result()
{
  int ret = OB_SUCCESS;
  int32_t dag_ret = OB_SUCCESS;
  int32_t tmp_result = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (OB_FAIL(result_mgr_.get_result(tmp_result))) {
    LOG_WARN("failed to get result", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_SUCCESS != tmp_result) {
    result = tmp_result;
  } else if (FALSE_IT(dag_ret = this->get_dag_ret())) {
  } else if (OB_SUCCESS != dag_ret) {
    result = dag_ret;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_SUCCESS == result) {
    //do nothing
  } else if (OB_FAIL(ha_dag_net_ctx_->set_result(result, true /*allow_retry*/, get_type()))) {
    LOG_WARN("failed to set ha dag net ctx result", K(ret), KPC(ha_dag_net_ctx_));
  }
  return ret;
}

int ObStorageHADag::check_is_in_retry(bool &is_in_retry)
{
  int ret = OB_SUCCESS;
  is_in_retry = false;
  int32_t retry_count = 0;

  if (OB_ISNULL(ha_dag_net_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("storage ha dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to check need retry", K(ret), K(*this));
  } else {
    is_in_retry = retry_count > 0;
  }
  return ret;
}

/******************ObStorageHADagUtils*********************/
int ObStorageHADagUtils::deal_with_fo(
    const int err,
    share::ObIDag *dag,
    const bool allow_retry)
{
  int ret = OB_SUCCESS;
  ObStorageHADag *ha_dag = nullptr;

  if (OB_SUCCESS == err || OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("deal with fo get invalid argument", K(ret), K(err), KP(dag));
  } else if (0 != STRCMP(OB_DAG_TYPES[dag->get_type()].dag_module_str_, "MIGRATE")
      && 0 != STRCMP(OB_DAG_TYPES[dag->get_type()].dag_module_str_, "RESTORE")
      && 0 != STRCMP(OB_DAG_TYPES[dag->get_type()].dag_module_str_, "BACKFILL_TX")
      && 0 != STRCMP(OB_DAG_TYPES[dag->get_type()].dag_module_str_, "TRANSFER")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag type is unexpected", K(ret), KPC(dag));
  } else if (OB_ISNULL(ha_dag = static_cast<ObStorageHADag *>(dag))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ha dag should not be NULL", K(ret), KPC(ha_dag));
  } else if (OB_FAIL(ha_dag->set_result(err, allow_retry, dag->get_type()))) {
    LOG_WARN("failed to set result", K(ret), K(err));
  }
  return ret;
}

int ObStorageHADagUtils::get_ls(const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get ls get invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(ls_id), KP(ls));
  }
  return ret;
}

int ObStorageHADagUtils::check_self_is_valid_member(
    const share::ObLSID &ls_id,
    bool &is_valid_member)
{
  int ret = OB_SUCCESS;
  is_valid_member = true;
  const uint64_t tenant_id = MTL_ID();
  share::ObLocationService *location_service = nullptr;
  const bool force_renew = true;
  ObLSService *ls_service = nullptr;
  storage::ObStorageRpc *storage_rpc = nullptr;
  obrpc::ObFetchLSMemberAndLearnerListInfo member_info;
  storage::ObStorageHASrcInfo src_info;
  src_info.cluster_id_ = GCONF.cluster_id;
  const ObAddr &self_addr = GCONF.self_addr_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check self in member list get invalid argument", K(ret), K(ls_id));
  } else if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service should not be NULL", K(ret), KP(location_service));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->get_log_handler()->get_election_leader(src_info.src_addr_))) {
    LOG_WARN("failed to get election leader", K(ret), K(tenant_id), K(ls_id));
    if (OB_LEADER_NOT_EXIST == ret) {
      //overwrite ret
      if (OB_FAIL(location_service->get_leader(src_info.cluster_id_, tenant_id, ls_id, force_renew, src_info.src_addr_))) {
        LOG_WARN("failed to get ls leader server", K(ret), K(tenant_id), K(ls_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc should not be NULL", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(storage_rpc->fetch_ls_member_and_learner_list(tenant_id, ls_id, src_info, member_info))) {
    LOG_WARN("failed to check ls is valid member", K(ret), K(tenant_id), K(ls_id));
  } else if (member_info.member_list_.contains(self_addr)) {
    is_valid_member = true;
  } else if (!member_info.learner_list_.contains(self_addr)) {
    is_valid_member = false;
  } else {
    ObMember member;
    if (OB_FAIL(member_info.learner_list_.get_learner_by_addr(self_addr, member))) {
      LOG_WARN("failed to get_learner_by_addr", K(ret));
    } else if (member.is_migrating()) {
      is_valid_member = false;
      LOG_INFO("self is not valid member", K(ret), K(member), K(member_info), K(self_addr), K(ls_id));
    } else {
      is_valid_member = true;
    }
  }
  return ret;
}

/******************ObHATabletGroupCtx*********************/
ObHATabletGroupCtx::ObHATabletGroupCtx()
  : is_inited_(false),
    lock_(),
    tablet_id_array_(),
    index_(0)
{
}

ObHATabletGroupCtx::~ObHATabletGroupCtx()
{
}

int ObHATabletGroupCtx::init(const common::ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ha tablet group ctx already init", K(ret));
  } else if (tablet_id_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ha tablet group ctx get invalid argument", K(ret), K(tablet_id_array));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    index_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObHATabletGroupCtx::get_next_tablet_id(ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ha tablet group ctx do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (index_ > tablet_id_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet group index is bigger than tablet id array count", K(ret), K(index_), K(tablet_id_array_));
    } else if (index_ == tablet_id_array_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet_id = tablet_id_array_.at(index_);
      index_++;
    }
  }
  return ret;
}

int ObHATabletGroupCtx::get_all_tablet_ids(ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  tablet_id_array.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ha tablet group ctx do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(tablet_id_array.assign(tablet_id_array_))) {
      LOG_WARN("failed to get tablet id array", K(ret), K(tablet_id_array_));
    }
  }
  return ret;
}

void ObHATabletGroupCtx::reuse()
{
  common::SpinWLockGuard guard(lock_);
  tablet_id_array_.reuse();
  index_ = 0;
  is_inited_ = false;
}

/******************ObHATabletGroupCtx*********************/
ObHATabletGroupMgr::ObHATabletGroupMgr()
  : is_inited_(false),
    lock_(),
    allocator_("HATGMgr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    tablet_group_ctx_array_(),
    index_(0)
{
}

ObHATabletGroupMgr::~ObHATabletGroupMgr()
{
  if (!is_inited_) {
  } else {
    reuse();
    is_inited_ = false;
  }
}

int ObHATabletGroupMgr::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ha tablet group mgr init twice", K(ret));
  } else {
    index_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObHATabletGroupMgr::get_next_tablet_group_ctx(
    ObHATabletGroupCtx *&tablet_group_ctx)
{
  int ret = OB_SUCCESS;
  tablet_group_ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ha tablet group mgr do not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (index_ == tablet_group_ctx_array_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet_group_ctx = tablet_group_ctx_array_.at(index_);
      index_++;
    }
  }
  return ret;
}

int ObHATabletGroupMgr::build_tablet_group_ctx(
    const ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObHATabletGroupCtx *tablet_group_ctx = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ha tablet group mgr do not init", K(ret));
  } else if (tablet_id_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build tablet group ctx get invalid argument", K(ret), K(tablet_id_array));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObHATabletGroupCtx)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret), KP(buf));
    } else if (FALSE_IT(tablet_group_ctx = new (buf) ObHATabletGroupCtx())) {
    } else if (OB_FAIL(tablet_group_ctx->init(tablet_id_array))) {
      LOG_WARN("failed to init tablet group ctx", K(ret), K(tablet_id_array));
    } else if (OB_FAIL(tablet_group_ctx_array_.push_back(tablet_group_ctx))) {
      LOG_WARN("failed to push tablet group ctx into array", K(ret));
    } else {
      tablet_group_ctx = nullptr;
    }

    if (OB_NOT_NULL(tablet_group_ctx)) {
      tablet_group_ctx->~ObHATabletGroupCtx();
    }
  }
  return ret;
}

void ObHATabletGroupMgr::reuse()
{
  common::SpinWLockGuard guard(lock_);
  for (int64_t i = 0; i < tablet_group_ctx_array_.count(); ++i) {
    ObHATabletGroupCtx *tablet_group_ctx = tablet_group_ctx_array_.at(i);
    if (OB_NOT_NULL(tablet_group_ctx)) {
      tablet_group_ctx->~ObHATabletGroupCtx();
    }
  }
  tablet_group_ctx_array_.reset();
  allocator_.reset();
  index_ = 0;
}

/******************ObStorageHATaskUtils*********************/
int ObStorageHATaskUtils::check_need_copy_sstable(
    const ObMigrationSSTableParam &param,
    ObTabletHandle &tablet_handle,
    bool &need_copy)
{
  int ret = OB_SUCCESS;
  need_copy = true;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need copy sstable get invalid argument", K(ret), K(param));
  } else if (param.table_key_.is_major_sstable()) {
    if (OB_FAIL(check_major_sstable_need_copy_(param, tablet_handle, need_copy))) {
      LOG_WARN("failed to check major sstable need copy", K(ret), K(param), K(tablet_handle));
    }
  } else if (param.table_key_.is_minor_sstable()) {
    if (OB_FAIL(check_minor_sstable_need_copy_(param, tablet_handle, need_copy))) {
      LOG_WARN("failed to check minor sstable need copy", K(ret), K(param), K(tablet_handle));
    }
  } else if (param.table_key_.is_ddl_dump_sstable()) {
    if (OB_FAIL(check_ddl_sstable_need_copy_(param, tablet_handle, need_copy))) {
      LOG_WARN("failed to check ddl sstable need copy", K(ret), K(param), K(tablet_handle));
    }
  } else if (param.table_key_.is_mds_sstable()) {
    need_copy = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy sstable table type is unexpected", K(ret), K(param));
  }
  return ret;
}

int ObStorageHATaskUtils::check_major_sstable_need_copy_(
    const ObMigrationSSTableParam &param,
    ObTabletHandle &tablet_handle,
    bool &need_copy)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObSSTableWrapper sstable_wrapper;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableMetaHandle sst_meta_hdl;

  if (!param.table_key_.is_major_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check major sstable need copy get invalid argument", K(ret), K(param));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(param), K(tablet_handle));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &major_sstable_array = table_store_wrapper.get_member()->get_major_sstables();
    if (major_sstable_array.empty()) {
      need_copy = true;
    } else if (OB_FAIL(major_sstable_array.get_table(param.table_key_, sstable_wrapper))) {
      LOG_WARN("failed to get table", K(ret), K(param), K(major_sstable_array));
    } else if (nullptr == sstable_wrapper.get_sstable()) {
      need_copy = true;
    } else if (OB_FAIL(sstable_wrapper.get_sstable()->get_meta(sst_meta_hdl))) {
      LOG_WARN("failed to get sstable meta handle", K(ret));
    } else if (OB_FAIL(ObSSTableMetaChecker::check_sstable_meta(param, sst_meta_hdl.get_sstable_meta()))) {
      LOG_WARN("failed to check sstable meta", K(ret), K(param), K(sstable_wrapper));
    } else {
      need_copy = false;
    }
  }
  return ret;
}

int ObStorageHATaskUtils::check_minor_sstable_need_copy_(
    const ObMigrationSSTableParam &param,
    ObTabletHandle &tablet_handle,
    bool &need_copy)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  const ObSSTable *sstable = nullptr;
  ObTableStoreIterator minor_table_iter;

  if (!param.table_key_.is_minor_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check minor sstable need copy get invalid argument", K(ret), K(param));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(param), K(tablet_handle));
  } else if (OB_FAIL(tablet->get_all_minor_sstables(minor_table_iter))) {
    LOG_WARN("failed to get tables handle array", K(ret), K(param));
  } else if (0 == minor_table_iter.count()) {
    need_copy = true;
  } else {
    bool found = false;

    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      if (OB_FAIL(minor_table_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to iterate minor tables", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("minor sstable should not be NULL", K(ret), KP(table), K(minor_table_iter));
      } else if (table->get_key() == param.table_key_) {
        found = true;
        need_copy = true;
        //TODO(muwei.ym) Fix it in 4.3
        //Need copy should be false and reuse local minor sstable.
      }
    }
    if (OB_SUCC(ret) && !found) {
      need_copy = true;
    }
  }
  return ret;
}

int ObStorageHATaskUtils::check_ddl_sstable_need_copy_(
    const ObMigrationSSTableParam &param,
    ObTabletHandle &tablet_handle,
    bool &need_copy)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObSSTableWrapper sstable_wrapper;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTableMetaHandle sst_meta_hdl;

  if (!param.table_key_.is_ddl_dump_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check ddl sstable need copy get invalid argument", K(ret), K(param));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(param), K(tablet_handle));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &ddl_sstable_array = table_store_wrapper.get_member()->get_ddl_sstables();
    const ObSSTableArray &major_sstable_array = table_store_wrapper.get_member()->get_major_sstables();

    if (!major_sstable_array.empty()) {
      need_copy = false;
    } else if (ddl_sstable_array.empty()) {
      need_copy = true;
    } else if (OB_FAIL(ddl_sstable_array.get_table(param.table_key_, sstable_wrapper))) {
      LOG_WARN("failed to get table", K(ret), K(param), K(ddl_sstable_array));
    } else if (nullptr == sstable_wrapper.get_sstable()) {
      const SCN start_scn = ddl_sstable_array.get_boundary_table(false)->get_start_scn();
      const SCN end_scn = ddl_sstable_array.get_boundary_table(true)->get_end_scn();
      if (param.table_key_.scn_range_.start_scn_ >= start_scn
          && param.table_key_.scn_range_.end_scn_ <= end_scn) {
        need_copy = false;
      } else {
        need_copy = true;
      }
    } else if (OB_FAIL(sstable_wrapper.get_sstable()->get_meta(sst_meta_hdl))) {
      LOG_WARN("failed to get sstable meta handle", K(ret));
    } else if (OB_FAIL(ObSSTableMetaChecker::check_sstable_meta(param, sst_meta_hdl.get_sstable_meta()))) {
      LOG_WARN("failed to check sstable meta", K(ret), K(param), K(sstable_wrapper));
    } else {
      need_copy = false;
    }
  }
  return ret;
}


}
}

