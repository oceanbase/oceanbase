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

#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/slog_ckpt/ob_server_checkpoint_slog_handler.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_defs.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_task.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_service.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_mgr.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_meta_table.h"
namespace oceanbase
{
namespace storage
{

bool ObTenantSnapshotCreateParam::is_valid() const
{
  int ret = OB_SUCCESS;

  bool bret = false;
  if (!tenant_snapshot_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant_snapshot_id_ is invalid", KR(ret), KPC(this));
  } else if (creating_ls_id_arr_.empty())  {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the creating_ls_id_arr_ is empty", KR(ret), KPC(this));
  } else if (!trace_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the trace_id is not valid", KR(ret), KPC(this));
  } else if (OB_ISNULL(tenant_snapshot_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_mgr_ is nullptr", KR(ret), KPC(this));
  } else {
    bret = true;
  }

  return bret;
}

int ObTenantSnapshotCreateDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTenantSnapshotCreateParam *create_param = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantSnapshotCreateParam cannot init twice", KR(ret), KPC(this), K(param));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObTenantSnapshotCreateParam input param is null", KR(ret), KPC(this), K(param));
  } else if (FALSE_IT(create_param = static_cast<const ObTenantSnapshotCreateParam *>(param))) {
  } else if (!create_param->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObTenantSnapshotCreateParam input param is not valid",
        KR(ret), KPC(this), KPC(create_param));
  } else if (OB_FAIL(this->set_dag_id(create_param->trace_id_))) {
    LOG_WARN("fail to set dag id", KR(ret), KPC(create_param));
  } else {
    tenant_snapshot_id_ = create_param->tenant_snapshot_id_;
    creating_ls_id_arr_ = create_param->creating_ls_id_arr_;
    tenant_snapshot_mgr_ = create_param->tenant_snapshot_mgr_;
    is_inited_ = true;
  }

  return ret;
}

int ObTenantSnapshotCreateDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotCreateTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotCreateDag has not been inited", KR(ret), KPC(this));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("fail to alloc ObTenantSnapshotCreateTask", KR(ret), KPC(this));
  } else if (OB_FAIL(task->init(tenant_snapshot_id_,
                                &creating_ls_id_arr_,
                                tenant_snapshot_mgr_))) {
    LOG_WARN("fail to init ObTenantSnapshotCreateTask", KR(ret), KPC(this), KPC(task));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add task", KR(ret), KPC(this), KPC(task));
  } else {
    LOG_INFO("success to add ObTenantSnapshotCreateTask", KPC(this), KPC(task));
  }
  return ret;
}

bool ObTenantSnapshotCreateDag::operator==(const ObIDag &other) const
{
  bool bret = true;
  const ObTenantSnapshotCreateDag &other_dag = static_cast<const ObTenantSnapshotCreateDag &>(other);
  if (this != &other) {
    if (get_type() != other.get_type() ||
        tenant_snapshot_id_ != other_dag.tenant_snapshot_id_) {
      bret = false;
    }
  }
  return bret;
}

int ObTenantSnapshotCreateDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                                               ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotCreateDag has not been inited", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             tenant_snapshot_id_.id()))) {
    LOG_WARN("fail to fill info param", KR(ret), KPC(this));
  }
  return ret;
}

int ObTenantSnapshotCreateDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotCreateDag has not been inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fill_dag_key parameters are invalid", KR(ret), KPC(this), KP(buf), K(buf_len));
  } else if (!tenant_snapshot_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_snapshot_id_ is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_snapshot_id=%s",
                                     to_cstring(tenant_snapshot_id_)))) {
    LOG_WARN("fail to fill dag_key", KR(ret), KPC(this));
  }
  return ret;
}

int64_t ObTenantSnapshotCreateDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

int ObTenantSnapshotCreateTask::init(const ObTenantSnapshotID& tenant_snapshot_id,
                                     const ObArray<ObLSID>* creating_ls_id_arr,
                                     ObTenantSnapshotMgr* tenant_snapshot_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", KR(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must not null", KR(ret), K(tenant_snapshot_id));
  } else if (share::ObDagType::DAG_TYPE_TENANT_SNAPSHOT_CREATE != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", KR(ret), KPC(dag_), K(tenant_snapshot_id));
  } else if (!tenant_snapshot_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_id is invalid", KR(ret), K(tenant_snapshot_id));
  } else if (OB_ISNULL(creating_ls_id_arr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("creating_ls_id_arr is unexpected null", KR(ret), K(tenant_snapshot_id));
  } else if (creating_ls_id_arr->empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("creating_ls_id_arr is empty array", KR(ret), K(tenant_snapshot_id));
  } else if (OB_ISNULL(tenant_snapshot_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_mgr is unexpected null", KR(ret), K(tenant_snapshot_id));
  } else {
    tenant_snapshot_id_ = tenant_snapshot_id;
    creating_ls_id_arr_ = creating_ls_id_arr;
    tenant_snapshot_mgr_ = tenant_snapshot_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantSnapshotCreateTask::process()
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObTenantSnapshot* tenant_snapshot = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotCreateTask not inited", KR(ret));
  } else if (OB_ISNULL(creating_ls_id_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("creating_ls_id_arr_ is unexpected nullptr", KR(ret), KPC(this));
  } else if (OB_FAIL(tenant_snapshot_mgr_->get_tenant_snapshot(tenant_snapshot_id_, tenant_snapshot))) {
    LOG_WARN("fail to get tenant_snapshot", KR(ret), K(tenant_snapshot_id_));
  } else if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_snapshot is unexpected nullptr", KR(ret), K(tenant_snapshot_id_));
  } else {
    if (OB_FAIL(tenant_snapshot->execute_create_tenant_snapshot_dag(*creating_ls_id_arr_))) {
      LOG_WARN("fail to create tenant_snapshot", KR(ret), KPC(tenant_snapshot));
    }
    tenant_snapshot->finish_create_tenant_snapshot_dag();
    tenant_snapshot_mgr_->revert_tenant_snapshot(tenant_snapshot);
  }

  LOG_INFO("ObTenantSnapshotCreateTask process finished", KR(ret), K(tenant_snapshot_id_), KPC(this));
  return ret;
}

//****** ObTenantSnapshotGCParam
bool ObTenantSnapshotGCParam::is_valid() const
{
  int ret = OB_SUCCESS;
  bool bret = false;

  if (OB_UNLIKELY(!tenant_snapshot_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the tenant_snapshot_id_ is invalid", KR(ret), KPC(this));
  } else if (!gc_tenant_snapshot_ && gc_ls_id_arr_.empty()) {  // gc_tenant_snapshot_ == false means gc ls snap
    ret = OB_INVALID_ARGUMENT;                                    // therefore gc_ls_id_arr_ could not be empty
    LOG_WARN("gc_ls_id_arr_ is empty", KR(ret), KPC(this));
  } else if (!trace_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the trace_id is not valid", KR(ret), KPC(this));
  } else if (OB_ISNULL(tenant_snapshot_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_mgr_ is nullptr", KR(ret), KPC(this));
  } else {
    bret = true;
  }

  return bret;
}

//****** ObTenantSnapshotGCDag
int ObTenantSnapshotGCDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObTenantSnapshotGCParam *gc_param = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantSnapshotGCDag cannot init twice", KR(ret), KPC(this), K(param));
  } else if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObTenantSnapshotGCDag input param is null", KR(ret), KPC(this));
  } else if (FALSE_IT(gc_param = static_cast<const ObTenantSnapshotGCParam *>(param))) {
  } else if (OB_UNLIKELY(!gc_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ObTenantSnapshotGCDag input param is not valid",
        KR(ret), KPC(this), KPC(gc_param));
  } else if (OB_FAIL(this->set_dag_id(gc_param->trace_id_))) {
    LOG_WARN("fail to set dag id", KR(ret), KPC(gc_param));
  } else {
    tenant_snapshot_id_ = gc_param->tenant_snapshot_id_;
    gc_ls_id_arr_ = gc_param->gc_ls_id_arr_;
    gc_tenant_snapshot_ = gc_param->gc_tenant_snapshot_;
    tenant_snapshot_mgr_ = gc_param->tenant_snapshot_mgr_;
    is_inited_ = true;
  }

  return ret;
}

int ObTenantSnapshotGCDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTenantSnapshotGCTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("fail to create ObTenantSnapshotGCDag", KR(ret));
  } else if (OB_FAIL(task->init(tenant_snapshot_id_,
                     &gc_ls_id_arr_,
                     gc_tenant_snapshot_,
                     tenant_snapshot_mgr_))) {
    LOG_WARN("fail to init ObTenantSnapshotGCTask", KR(ret));
  } else if(OB_FAIL(add_task(*task))) {
    LOG_WARN("fail to add task", KR(ret), KPC(this));
  } else {
    LOG_INFO("success to add ObTenantSnapshotGCTask", KPC(this));
  }
  return ret;
}

bool ObTenantSnapshotGCDag::operator==(const ObIDag &other) const
{
  bool bret = true;
  const ObTenantSnapshotGCDag &other_dag = static_cast<const ObTenantSnapshotGCDag &>(other);
  if (this != &other) {
    if (get_type() != other.get_type() ||
        tenant_snapshot_id_ != other_dag.tenant_snapshot_id_) {
      bret = false;
    }
  }
  return bret;
}

int ObTenantSnapshotGCDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param,
                              ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotGCDag has not been inited", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                             tenant_snapshot_id_.id()))) {
    LOG_WARN("failed to fill info param", KR(ret), KPC(this));
  }
  return ret;
}

int ObTenantSnapshotGCDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotGCDag has not been inited", KR(ret), KPC(this));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fill_dag_key parameters are invalid", KR(ret), KPC(this), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(!tenant_snapshot_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_snapshot_id_ is invalid", KR(ret), KPC(this));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_snapshot_id=%s",
                                     to_cstring(tenant_snapshot_id_)))) {
    LOG_WARN("failed to fill dag_key", KR(ret), KPC(this));
  }
  return ret;
}

int64_t ObTenantSnapshotGCDag::hash() const
{
  int64_t ptr = reinterpret_cast<int64_t>(this);
  return common::murmurhash(&ptr, sizeof(ptr), 0);
}

//****** ObTenantSnapshotGCTask
int ObTenantSnapshotGCTask::init(const ObTenantSnapshotID tenant_snapshot_id,
                                 const ObArray<ObLSID> *gc_ls_id_arr,
                                 bool gc_tenant_snapshot,
                                 ObTenantSnapshotMgr* tenant_snapshot_mgr)
{
  int ret = OB_SUCCESS;
  if(is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantSnapshotGCTask can not init twice", KR(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("dag must be not null", KR(ret), K(tenant_snapshot_id));
  } else if (share::ObDagType::DAG_TYPE_TENANT_SNAPSHOT_GC != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", KR(ret), KPC(dag_));
  } else if (OB_ISNULL(tenant_snapshot_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_snapshot_mgr is null", KR(ret), K(tenant_snapshot_id));
  } else {
    tenant_snapshot_id_ = tenant_snapshot_id;
    gc_ls_id_arr_ = gc_ls_id_arr;
    gc_tenant_snapshot_ = gc_tenant_snapshot;
    tenant_snapshot_mgr_ = tenant_snapshot_mgr;
    is_inited_ = true;
  }
  return ret;
}

int ObTenantSnapshotGCTask::process()
{
  int ret = OB_SUCCESS;
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObTenantSnapshot* tenant_snapshot = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantSnapshotGCTask not inited", KR(ret));
  } else if (OB_ISNULL(gc_ls_id_arr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc_ls_id_arr_ is null", KR(ret));
  } else if (OB_UNLIKELY(OB_ISNULL(tenant_snapshot_mgr_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_snapshot_mgr_ is null", KR(ret), K(tenant_snapshot_id_));
  } else if (OB_FAIL(tenant_snapshot_mgr_->get_tenant_snapshot(tenant_snapshot_id_, tenant_snapshot))) {
    LOG_WARN("fail to get tenant_snapshot", KR(ret), K(tenant_snapshot_id_));
  } else if (OB_ISNULL(tenant_snapshot)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant_snapshot has been deleted", KR(ret), K(tenant_snapshot_id_));
  } else {
    if (OB_FAIL(tenant_snapshot->execute_gc_tenant_snapshot_dag(gc_tenant_snapshot_, *gc_ls_id_arr_))) {
      LOG_WARN("fail to execute gc tenant snapshot dag", KR(ret));
    } else {
      if (gc_tenant_snapshot_) {
        if (OB_FAIL(tenant_snapshot_mgr_->del_tenant_snapshot(tenant_snapshot->get_tenant_snapshot_id()))) {
          LOG_WARN("fail to delete tenant snapshot in tenant_snapshot_mgr_",
              KR(ret), KPC(tenant_snapshot_mgr_), KPC(tenant_snapshot));
        }
      }
    }
    // once task finished, we clean has_unfinished_gc_dag_ flag
    tenant_snapshot->finish_gc_tenant_snapshot_dag();
    tenant_snapshot_mgr_->revert_tenant_snapshot(tenant_snapshot);
  }

  LOG_INFO("ObTenantSnapshotGCTask finished",
      KR(ret), K(gc_tenant_snapshot_), K(tenant_snapshot_id_), KPC(gc_ls_id_arr_));
  return ret;
}

}
}
