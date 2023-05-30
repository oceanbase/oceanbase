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
#include "ob_tablet_backfill_tx.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_storage_struct.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
/******************ObBackfillTXCtx*********************/
ObBackfillTXCtx::ObBackfillTXCtx()
  : task_id_(),
    ls_id_(),
    log_sync_scn_(SCN::min_scn()),
    lock_(),
    tablet_id_index_(0),
    tablet_id_array_()
{
}

ObBackfillTXCtx::~ObBackfillTXCtx()
{
}

void ObBackfillTXCtx::reset()
{
  task_id_.reset();
  ls_id_.reset();
  tablet_id_index_ = 0;
  tablet_id_array_.reset();
}

bool ObBackfillTXCtx::is_valid() const
{
  common::SpinRLockGuard guard(lock_);
  return inner_is_valid_();
}

bool ObBackfillTXCtx::inner_is_valid_() const
{
  return !task_id_.is_invalid() && ls_id_.is_valid()
      && tablet_id_index_ >= 0 && !tablet_id_array_.empty()
      && tablet_id_index_ <= tablet_id_array_.count();
}

int ObBackfillTXCtx::get_tablet_id(ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id.reset();
  common::SpinWLockGuard guard(lock_);

  if (!inner_is_valid_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backfill tx ctx is invalid", K(ret), K(*this));
  } else {
    if (tablet_id_index_ > tablet_id_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet id index should not bigger than tablet id array count",
          K(ret), K(tablet_id_index_), K(tablet_id_array_));
    } else if (tablet_id_index_ == tablet_id_array_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet_id = tablet_id_array_.at(tablet_id_index_);
      tablet_id_index_++;
    }
  }
  return ret;
}

int ObBackfillTXCtx::build_backfill_tx_ctx(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    const SCN log_sync_scn,
    const common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (!tablet_id_array_.empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backfill tx ctx init twice", K(ret), KPC(this));
  } else if (task_id.is_invalid() || !ls_id.is_valid() || !log_sync_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build backfill tx ctx get invalid argument", K(ret), K(task_id), K(ls_id),
        K(log_sync_scn), K(tablet_id_array));
  } else if (!tablet_id_array.empty() && OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    task_id_ = task_id;
    ls_id_ = ls_id;
    log_sync_scn_ = log_sync_scn;
    tablet_id_index_ = 0;
  }
  return ret;
}

bool ObBackfillTXCtx::is_empty() const
{
  common::SpinRLockGuard guard(lock_);
  return tablet_id_array_.empty();
}

int ObBackfillTXCtx::get_tablet_id_array(
    common::ObIArray<common::ObTabletID> &tablet_id_array) const
{
  int ret = OB_SUCCESS;
  tablet_id_array.reset();
  common::SpinRLockGuard guard(lock_);

  if (!inner_is_valid_()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tx ctx is not init", K(ret));
  } else {
    if (OB_FAIL(tablet_id_array.assign(tablet_id_array_))) {
      LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array_));
    }
  }
  return ret;
}

int ObBackfillTXCtx::check_is_same(
    const ObBackfillTXCtx &backfill_tx_ctx,
    bool &is_same) const
{
  int ret = OB_SUCCESS;
  is_same = true;
  ObArray<ObTabletID> tablet_id_array;
  common::SpinRLockGuard guard(lock_);

  if (!inner_is_valid_()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tx ctx is not init", K(ret), K(*this));
  } else if (!backfill_tx_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check is same get invalid argument", K(ret), K(backfill_tx_ctx));
  } else if (ls_id_ != backfill_tx_ctx.ls_id_) {
    is_same = false;
  } else if(OB_FAIL(backfill_tx_ctx.get_tablet_id_array(tablet_id_array))) {
    LOG_WARN("failed to get tablet id array", K(ret), K(backfill_tx_ctx));
  } else {

    if (tablet_id_array.count() != tablet_id_array_.count()) {
      is_same = false;
    } else {
      for (int64_t i = 0; i < tablet_id_array_.count() && is_same; ++i) {
        if (tablet_id_array_.at(i) != tablet_id_array.at(i)) {
          is_same = false;
        }
      }
    }
  }
  return ret;
}

int64_t ObBackfillTXCtx::hash() const
{
  int64_t hash_value = 0;
  common::SpinRLockGuard guard(lock_);
  hash_value = common::murmurhash(
      &ls_id_, sizeof(ls_id_), hash_value);
  for (int64_t i = 0; i < tablet_id_array_.count(); ++i) {
    hash_value = common::murmurhash(
        &tablet_id_array_.at(i), sizeof(tablet_id_array_.at(i)), hash_value);
  }
  return hash_value;
}

/******************ObTabletBackfillTXDag*********************/
ObTabletBackfillTXDag::ObTabletBackfillTXDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_BACKFILL_TX, ObStorageHADagType::TABLET_BACKFILL_TX_DAG),
    is_inited_(false),
    dag_net_id_(),
    ls_id_(),
    tablet_id_(),
    backfill_tx_ctx_(nullptr),
    tablet_handle_()
{
}

ObTabletBackfillTXDag::~ObTabletBackfillTXDag()
{
}

int ObTabletBackfillTXDag::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "tablet backfill tx : dag_net_id = %s, "
      "ls_id = %s, tablet_id = %s",
      to_cstring(dag_net_id_), to_cstring(ls_id_), to_cstring(tablet_id_)))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

int ObTabletBackfillTXDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObTabletBackfillTXDag: ls_id = %s, tablet_id = %s",
       to_cstring(ls_id_), to_cstring(tablet_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), KPC(backfill_tx_ctx_), KPC(ha_dag_net_ctx_));
  }
  return ret;
}

bool ObTabletBackfillTXDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (ha_dag.get_sub_type() != sub_type_) {
      is_same = false;
    } else {
      const ObTabletBackfillTXDag &tablet_backfill_tx_dag = static_cast<const ObTabletBackfillTXDag&>(other);
      if (tablet_backfill_tx_dag.ls_id_ != ls_id_ || tablet_backfill_tx_dag.tablet_id_ != tablet_id_) {
        is_same = false;
      } else {
        is_same = true;
      }
    }
  }
  return is_same;
}

int64_t ObTabletBackfillTXDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else {
    hash_value = common::murmurhash(
        &ls_id_, sizeof(ls_id_), hash_value);
    hash_value = common::murmurhash(
        &tablet_id_, sizeof(tablet_id_), hash_value);
    hash_value = common::murmurhash(
        &sub_type_, sizeof(sub_type_), hash_value);
  }
  return hash_value;
}

int ObTabletBackfillTXDag::init(
    const share::ObTaskId &dag_net_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObBackfillTXCtx *backfill_tx_ctx)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill tx dag init twice", K(ret));
  } else if (dag_net_id.is_invalid() || !ls_id.is_valid() || !tablet_id.is_valid()
      || OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(backfill_tx_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill tx dag get invalid argument", K(ret), K(dag_net_id), K(ls_id), K(tablet_id),
        KP(ha_dag_net_ctx), KP(backfill_tx_ctx));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle_, ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else {
    dag_net_id_ = dag_net_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    backfill_tx_ctx_ = backfill_tx_ctx;
    compat_mode_ = tablet_handle_.get_obj()->get_tablet_meta().compat_mode_;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletBackfillTXDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init(dag_net_id_, ls_id_, tablet_id_))) {
    LOG_WARN("failed to init tablet backfill tx task", K(ret), KPC(ha_dag_net_ctx_), KPC(backfill_tx_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

int ObTabletBackfillTXDag::generate_next_dag(share::ObIDag *&dag)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  common::ObTabletID next_tablet_id;
  ObIDagNet *dag_net = nullptr;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  bool need_set_failed_result = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(this));
      ret = tmp_ret;
    }
  } else if (OB_FAIL(backfill_tx_ctx_->get_tablet_id(next_tablet_id))) {
    if (OB_ITER_END == ret) {
      //do nothing
      need_set_failed_result = false;
    } else {
      LOG_WARN("failed to get tablet id", K(ret), KPC(backfill_tx_ctx_));
    }
  } else if (OB_ISNULL(dag_net = this->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls prepare migration dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag(tablet_backfill_tx_dag))) {
    LOG_WARN("failed to alloc tablet backfill tx migration dag ", K(ret));
  } else if (OB_FAIL(tablet_backfill_tx_dag->init(dag_net_id_, ls_id_, next_tablet_id, ha_dag_net_ctx_, backfill_tx_ctx_))) {
    LOG_WARN("failed to init tablet migration dag", K(ret), KPC(ha_dag_net_ctx_), KPC(backfill_tx_ctx_));
  } else {
    LOG_INFO("succeed generate next dag", KPC(tablet_backfill_tx_dag));
    dag = tablet_backfill_tx_dag;
    tablet_backfill_tx_dag = nullptr;
  }

  if (OB_NOT_NULL(tablet_backfill_tx_dag)) {
    scheduler->free_dag(*tablet_backfill_tx_dag);
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    const bool need_retry = false;
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
    }
  }
  return ret;
}

int ObTabletBackfillTXDag::get_tablet_handle(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else {
    tablet_handle = tablet_handle_;
  }
  return ret;
}

/******************ObTabletBackfillTXTask*********************/
ObTabletBackfillTXTask::ObTabletBackfillTXTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    backfill_tx_ctx_(nullptr),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_id_()

{
}

ObTabletBackfillTXTask::~ObTabletBackfillTXTask()
{
}

int ObTabletBackfillTXTask::init(
    const share::ObTaskId &dag_net_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill tx task init twice", K(ret));
  } else if (dag_net_id.is_invalid() || !ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill tx get invalid argument", K(ret), K(dag_net_id), K(ls_id), K(tablet_id));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
    LOG_INFO("succeed init st migration task", "ls id", ls_id, "tablet_id", tablet_id,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", dag_net_id);

  }
  return ret;
}

int ObTabletBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to do tablet backfill tx task", KPC(ha_dag_net_ctx_), K(tablet_id_), K(ls_id_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(generate_backfill_tx_task_())) {
    LOG_WARN("failed to generate backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::generate_backfill_tx_task_()
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObFinishTabletBackfillTXTask *finish_backfill_tx_task = nullptr;
  ObArray<ObITable *> minor_tables;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->alloc_task(finish_backfill_tx_task))) {
    LOG_WARN("failed to finish backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(finish_backfill_tx_task->init(ls_id_, tablet_id_))) {
    LOG_WARN("failed to init finish backfill tx task", K(ret));
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->get_table_store().get_minor_sstables().get_all_tables(minor_tables))) {
    LOG_WARN("failed to get minor tables", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(generate_table_backfill_tx_task_(finish_backfill_tx_task, minor_tables))) {
    LOG_WARN("failed to generate minor sstables backfill tx task", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(dag_->add_task(*finish_backfill_tx_task))) {
    LOG_WARN("failed to add copy task to dag", K(ret));
  }
  return ret;
}

int ObTabletBackfillTXTask::generate_table_backfill_tx_task_(
    ObFinishTabletBackfillTXTask *finish_backfill_tx_task,
    common::ObIArray<ObITable *> &table_array)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle tablet_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(finish_backfill_tx_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate table backfill tx task get invalid argument",
        K(ret), KP(finish_backfill_tx_task), K(ls_id_), K(tablet_id_));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
      ObITable *table = table_array.at(i);
      ObSSTable *sstable = nullptr;
      ObTabletTableBackfillTXTask *table_backfill_tx_task = nullptr;
      ObTableHandleV2 table_handle(table, t3m, ObITable::TableType::MAJOR_SSTABLE);

      if (OB_ISNULL(table) || !table->is_minor_sstable() || table->is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
      } else if (!sstable->get_meta().get_basic_meta().contain_uncommitted_row_
          || sstable->get_meta().get_basic_meta().filled_tx_scn_ >= backfill_tx_ctx_->log_sync_scn_) {
        FLOG_INFO("sstable do not contain uncommitted row, no need backfill tx", KPC(sstable),
            "log sync scn", backfill_tx_ctx_->log_sync_scn_);
      } else if (OB_FAIL(tablet_backfill_tx_dag->alloc_task(table_backfill_tx_task))) {
        LOG_WARN("failed to alloc table backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
      } else if (OB_FAIL(table_backfill_tx_task->init(ls_id_, tablet_id_, tablet_handle, table_handle))) {
        LOG_WARN("failed to init table backfill tx task", K(ret), K(ls_id_), K(tablet_id_));
      } else if (OB_FAIL(this->add_child(*table_backfill_tx_task))) {
        LOG_WARN("failed to add table backfill tx task as child", K(ret), K(ls_id_), K(tablet_id_), KPC(table));
      } else if (OB_FAIL(table_backfill_tx_task->add_child(*finish_backfill_tx_task))) {
        LOG_WARN("failed to add finish backfill tx task as child", K(ret), K(ls_id_), K(tablet_id_), KPC(table));
      } else if (OB_FAIL(dag_->add_task(*table_backfill_tx_task))) {
        LOG_WARN("failed to add table backfill tx task", K(ret), K(ls_id_), K(tablet_id_));
      }
      if (OB_SUCC(ret)) {
        LOG_INFO("generate table backup fill", KPC(sstable), K(i), "log_sync_scn", backfill_tx_ctx_->log_sync_scn_);
      }
    }
  }
  return ret;
}

/******************ObTabletTableBackfillTXTask*********************/
ObTabletTableBackfillTXTask::ObTabletTableBackfillTXTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    backfill_tx_ctx_(nullptr),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_id_(),
    tablet_handle_(),
    table_handle_(),
    param_(),
    allocator_("TableBackfillTX"),
    tablet_merge_ctx_(param_, allocator_),
    merger_(nullptr)
{
}

ObTabletTableBackfillTXTask::~ObTabletTableBackfillTXTask()
{
  if (OB_NOT_NULL(merger_)) {
    merger_->~ObPartitionMerger();
    merger_ = nullptr;
  }
}

int ObTabletTableBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet table back fill tx task do not init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid() || !tablet_handle.is_valid() || !table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table backfill task get invalid argument", K(ret), K(ls_id),
        K(tablet_id), K(tablet_handle), K(table_handle));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    tablet_handle_ = tablet_handle;
    table_handle_ = table_handle;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletTableBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do tablet table backfill tx task", K(ls_id_), K(tablet_id_), K(table_handle_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (OB_FAIL(prepare_merge_ctx_())) {
    LOG_WARN("failed to prepare merge ctx", K(ret), KPC(this));
  } else if (OB_FAIL(tablet_merge_ctx_.prepare_index_tree())) {
    LOG_WARN("failed to prepare index tree", K(ret), KPC(this));
  } else if (OB_FAIL(do_backfill_tx_())) {
    LOG_WARN("failed to do backfill tx", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
    }
  }
  return ret;
}

int ObTabletTableBackfillTXTask::prepare_merge_ctx_()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, tablet_merge_ctx_.ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else {
    // init tablet merge dag param
    tablet_merge_ctx_.param_.ls_id_ = ls_id_;
    tablet_merge_ctx_.param_.merge_type_ = ObMergeType::BACKFILL_TX_MERGE;
    tablet_merge_ctx_.param_.report_ = nullptr;
    tablet_merge_ctx_.param_.tablet_id_ = tablet_id_;
    // init version range and sstable
    tablet_merge_ctx_.tablet_handle_ = tablet_handle_;
    tablet_merge_ctx_.sstable_version_range_.multi_version_start_ = tablet_handle_.get_obj()->get_multi_version_start();
    tablet_merge_ctx_.sstable_version_range_.snapshot_version_ = tablet_handle_.get_obj()->get_snapshot_version();
    tablet_merge_ctx_.scn_range_ = table_handle_.get_table()->get_key().scn_range_;
    tablet_merge_ctx_.merge_scn_ = backfill_tx_ctx_->log_sync_scn_;
    tablet_merge_ctx_.create_snapshot_version_ = 0;
    tablet_merge_ctx_.schedule_major_ = false;

    if (OB_FAIL(tablet_merge_ctx_.tables_handle_.add_table(table_handle_))) {
      LOG_WARN("failed to add table into tables handle", K(ret), K(table_handle_));
    } else if (OB_FAIL(tablet_merge_ctx_.get_storage_schema_to_merge(tablet_merge_ctx_.tables_handle_,
        true/*get_schema_on_memtable*/))) {
      LOG_ERROR("Fail to get storage schema", K(ret), K(tablet_merge_ctx_));
    } else {
      //get_basic_info_from_result result
      tablet_merge_ctx_.schema_ctx_.base_schema_version_ = tablet_merge_ctx_.schema_ctx_.schema_version_;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tablet_merge_ctx_.cal_minor_merge_param())) {
      LOG_WARN("fail to cal minor merge param", K(ret), K(tablet_merge_ctx_));
    } else if (OB_FAIL(tablet_merge_ctx_.init_merge_info())) {
      LOG_WARN("fail to init merge info", K(ret), K(tablet_merge_ctx_));
    }
  }
  return ret;
}

int ObTabletTableBackfillTXTask::do_backfill_tx_()
{
  int ret = OB_SUCCESS;
  const int64_t idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (OB_FAIL(prepare_partition_merge_())) {
    LOG_WARN("failed to prepare partition merge", K(ret));
  } else if (OB_FAIL(merger_->merge_partition(tablet_merge_ctx_, idx))) {
    LOG_WARN("failed to merge partition", K(ret), K(tablet_merge_ctx_));
  } else if (OB_FAIL(update_merge_sstable_())) {
    LOG_WARN("failed to update merge sstable", K(ret), K(tablet_merge_ctx_));
  } else {
    FLOG_INFO("merge backfill tx task finish", "task", *this);
  }

  if (OB_NOT_NULL(merger_)) {
    merger_->reset();
  }
  return ret;
}

int ObTabletTableBackfillTXTask::prepare_partition_merge_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(compaction::ObPartitionMinorMerger)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory for minor merger", K(ret));
  } else {
    merger_ = new (buf) compaction::ObPartitionMinorMerger();
  }
  return ret;
}

int ObTabletTableBackfillTXTask::update_merge_sstable_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls = tablet_merge_ctx_.ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id_));
  } else if (OB_FAIL(tablet_merge_ctx_.merge_info_.create_sstable(tablet_merge_ctx_))) {
    LOG_WARN("fail to create sstable", K(ret), K(tablet_merge_ctx_));
  } else {
    const int64_t rebuild_seq = ls->get_rebuild_seq();
    ObUpdateTableStoreParam param(tablet_merge_ctx_.merged_table_handle_,
                                  tablet_merge_ctx_.sstable_version_range_.snapshot_version_,
                                  tablet_merge_ctx_.sstable_version_range_.multi_version_start_,
                                  tablet_merge_ctx_.schema_ctx_.storage_schema_,
                                  rebuild_seq,
                                  is_major_merge_type(tablet_merge_ctx_.param_.merge_type_));
    ObTabletHandle new_tablet_handle;
    if (OB_FAIL(ls->update_tablet_table_store(
        tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(param));
    }
  }
  return ret;
}

/******************ObFinishTabletBackfillTXTask*********************/
ObFinishTabletBackfillTXTask::ObFinishTabletBackfillTXTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_id_()
{
}

ObFinishTabletBackfillTXTask::~ObFinishTabletBackfillTXTask()
{
}

int ObFinishTabletBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish tablet backfill tx task init twice", K(ret));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill tx get invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObFinishTabletBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish tablet backfill tx task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    //do nothing
  } else {
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_id_));
    }
  }
  return ret;
}

/******************ObFinishBackfillTXDag*********************/
ObFinishBackfillTXDag::ObFinishBackfillTXDag()
  : ObStorageHADag(
      ObDagType::DAG_TYPE_BACKFILL_TX, ObStorageHADagType::FINISH_BACKFILL_TX_DAG),
    is_inited_(false),
    backfill_tx_ctx_()
{
}

ObFinishBackfillTXDag::~ObFinishBackfillTXDag()
{
}

int ObFinishBackfillTXDag::fill_comment(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish backfill tx dag do not init", K(ret));
  } else if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "finish backfill tx : dag_net_id = %s, ls_id = %s",
      to_cstring(backfill_tx_ctx_.task_id_), to_cstring(backfill_tx_ctx_.ls_id_)))) {
    LOG_WARN("failed to set comment", K(ret), K(buf), K(pos), K(buf_len));
  }
  return ret;
}

int ObFinishBackfillTXDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
       "ObFinishBackfillTXDag: ls_id = %s ", to_cstring(backfill_tx_ctx_.ls_id_)))) {
    LOG_WARN("failed to fill comment", K(ret), K(backfill_tx_ctx_));
  }
  return ret;
}

bool ObFinishBackfillTXDag::operator == (const ObIDag &other) const
{
  int ret = OB_SUCCESS;
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObStorageHADag &ha_dag = static_cast<const ObStorageHADag&>(other);
    if (ha_dag.get_sub_type() != sub_type_) {
      is_same = false;
    } else {
      const ObFinishBackfillTXDag &finish_backfill_tx_dag = static_cast<const ObFinishBackfillTXDag&>(other);
      if (OB_FAIL(backfill_tx_ctx_.check_is_same(finish_backfill_tx_dag.backfill_tx_ctx_, is_same))) {
        LOG_WARN("failed to check is same", K(ret), K(*this));
      }
    }
  }
  return is_same;
}

int64_t ObFinishBackfillTXDag::hash() const
{
  int ret = OB_SUCCESS;
  int64_t hash_value = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else {
    hash_value += backfill_tx_ctx_.hash();
    hash_value = common::murmurhash(
        &sub_type_, sizeof(sub_type_), hash_value);
  }
  return hash_value;
}

int ObFinishBackfillTXDag::init(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    const SCN log_sync_scn,
    ObIHADagNetCtx *ha_dag_net_ctx)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish backfill tx dag init twice", K(ret));
  } else if (task_id.is_invalid() || !ls_id.is_valid() || !log_sync_scn.is_valid() || OB_ISNULL(ha_dag_net_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init finish backfill tx dag get invalid argument", K(ret), K(task_id), K(ls_id), K(log_sync_scn) ,KP(ha_dag_net_ctx));
  } else if (OB_FAIL(prepare_backfill_tx_ctx_(task_id, ls_id, log_sync_scn))) {
    LOG_WARN("failed to prepare backfill tx ctx", K(ret), K(task_id), K(ls_id));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObFinishBackfillTXDag::prepare_backfill_tx_ctx_(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    const SCN log_sync_scn)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSTabletIterator tablet_iter;
  ObArray<common::ObTabletID> tablet_id_array;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    while (OB_SUCC(ret)) {
      tablet_handle.reset();
      tablet = nullptr;
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", K(ret), K(ls_id));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
      } else if (tablet->get_tablet_meta().tablet_id_.is_ls_inner_tablet()) {
        //do nothing
      } else if (OB_FAIL(tablet_id_array.push_back(tablet->get_tablet_meta().tablet_id_))) {
        LOG_WARN("failed to push tablet id into array", K(ret), KPC(tablet), K(ls_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(backfill_tx_ctx_.build_backfill_tx_ctx(task_id, ls_id, log_sync_scn, tablet_id_array))) {
      LOG_WARN("failed to build backfill tx ctx", K(ret), K(tablet_id_array));
    }
  }
  return ret;
}

int ObFinishBackfillTXDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObFinishBackfillTXTask *task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("Fail to alloc task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("failed to init finish backfill tx task", K(ret), KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("Fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

/******************ObFinishBackfillTXTask*********************/
ObFinishBackfillTXTask::ObFinishBackfillTXTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    ha_dag_net_ctx_(nullptr)

{
}

ObFinishBackfillTXTask::~ObFinishBackfillTXTask()
{
}

int ObFinishBackfillTXTask::init()
{
  int ret = OB_SUCCESS;
  ObFinishBackfillTXDag *finish_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish backfill tx migration task init twice", K(ret));
  } else if (FALSE_IT(finish_backfill_tx_dag = static_cast<ObFinishBackfillTXDag *>(this->get_dag()))) {
  } else if (OB_ISNULL(finish_backfill_tx_dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish backfill tx dag should not be NULL", K(ret), KP(finish_backfill_tx_dag));
  } else {
    ha_dag_net_ctx_ = finish_backfill_tx_dag->get_ha_dag_net_ctx();
    is_inited_ = true;
  }
  return ret;
}

int ObFinishBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do finish backfill tx task", KPC(ha_dag_net_ctx_));

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("finish backfill tx migration task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    //do nothing
  } else {
    //TODO(muwei.ym) FIX IT later ObFinishBackfillTXTask::process
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObFinishBackfillTXDag *finish_backfill_tx_dag = static_cast<ObFinishBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(finish_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("finish backfill tx dag should not be NULL", K(tmp_ret), KP(finish_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = finish_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_));
    }
  }
  return ret;
}


}
}

