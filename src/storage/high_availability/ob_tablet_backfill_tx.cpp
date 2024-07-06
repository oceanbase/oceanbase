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
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ob_storage_struct.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

ERRSIM_POINT_DEF(EN_TRANSFER_BACKFILL_DATA_ERROR);
/******************ObBackfillTXCtx*********************/
ObBackfillTXCtx::ObBackfillTXCtx()
  : task_id_(),
    ls_id_(),
    backfill_scn_(SCN::min_scn()),
    lock_(),
    tablet_info_index_(0),
    tablet_info_array_()
{
}

ObBackfillTXCtx::~ObBackfillTXCtx()
{
}

void ObBackfillTXCtx::reset()
{
  task_id_.reset();
  ls_id_.reset();
  tablet_info_index_ = 0;
  tablet_info_array_.reset();
}

bool ObBackfillTXCtx::is_valid() const
{
  common::SpinRLockGuard guard(lock_);
  return inner_is_valid_();
}

bool ObBackfillTXCtx::inner_is_valid_() const
{
  return !task_id_.is_invalid() && ls_id_.is_valid()
      && tablet_info_index_ >= 0 && !tablet_info_array_.empty()
      && tablet_info_index_ <= tablet_info_array_.count();
}

int ObBackfillTXCtx::get_tablet_info(ObTabletBackfillInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();
  common::SpinWLockGuard guard(lock_);

  if (!inner_is_valid_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backfill tx ctx is invalid", K(ret), K(*this));
  } else {
    if (tablet_info_index_ > tablet_info_array_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet info index should not bigger than tablet info array count",
          K(ret), K(tablet_info_index_), K(tablet_info_array_));
    } else if (tablet_info_index_ == tablet_info_array_.count()) {
      ret = OB_ITER_END;
    } else {
      tablet_info = tablet_info_array_.at(tablet_info_index_);
      tablet_info_index_++;
    }
  }
  return ret;
}

int ObBackfillTXCtx::build_backfill_tx_ctx(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    const SCN backfill_scn,
    const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (!tablet_info_array_.empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backfill tx ctx init twice", K(ret), KPC(this));
  } else if (task_id.is_invalid() || !ls_id.is_valid() || !backfill_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build backfill tx ctx get invalid argument", K(ret), K(task_id), K(ls_id),
        K(backfill_scn), K(tablet_info_array));
  } else if (!tablet_info_array.empty() && OB_FAIL(tablet_info_array_.assign(tablet_info_array))) {
    LOG_WARN("failed to assign tablet info array", K(ret), K(tablet_info_array));
  } else {
    task_id_ = task_id;
    ls_id_ = ls_id;
    backfill_scn_ = backfill_scn;
    tablet_info_index_ = 0;
  }
  return ret;
}

bool ObBackfillTXCtx::is_empty() const
{
  common::SpinRLockGuard guard(lock_);
  return tablet_info_array_.empty();
}

int ObBackfillTXCtx::get_tablet_info_array(
    common::ObIArray<ObTabletBackfillInfo> &tablet_info_array) const
{
  int ret = OB_SUCCESS;
  tablet_info_array.reset();
  common::SpinRLockGuard guard(lock_);

  if (!inner_is_valid_()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tx ctx is not init", K(ret));
  } else {
    if (OB_FAIL(tablet_info_array.assign(tablet_info_array_))) {
      LOG_WARN("failed to assign tablet info array", K(ret), K(tablet_info_array_));
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
  ObArray<ObTabletBackfillInfo> tablet_info_array;
  common::SpinRLockGuard guard(lock_);

  if (!inner_is_valid_()) {
    ret = OB_NOT_INIT;
    LOG_WARN("backfill tx ctx is not init", K(ret), K(*this));
  } else if (!backfill_tx_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check is same get invalid argument", K(ret), K(backfill_tx_ctx));
  } else if (ls_id_ != backfill_tx_ctx.ls_id_) {
    is_same = false;
  } else if(OB_FAIL(backfill_tx_ctx.get_tablet_info_array(tablet_info_array))) {
    LOG_WARN("failed to get tablet info array", K(ret), K(backfill_tx_ctx));
  } else {

    if (tablet_info_array.count() != tablet_info_array_.count()) {
      is_same = false;
    } else {
      for (int64_t i = 0; i < tablet_info_array_.count() && is_same; ++i) {
        if (!(tablet_info_array_.at(i) == tablet_info_array.at(i))) {
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
  for (int64_t i = 0; i < tablet_info_array_.count(); ++i) {
    hash_value = common::murmurhash(
        &tablet_info_array_.at(i), sizeof(tablet_info_array_.at(i)), hash_value);
  }
  return hash_value;
}

/******************ObTabletBackfillTXDag*********************/
ObTabletBackfillTXDag::ObTabletBackfillTXDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_TABLET_BACKFILL_TX),
    is_inited_(false),
    dag_net_id_(),
    ls_id_(),
    tablet_info_(),
    backfill_tx_ctx_(nullptr),
    tablet_handle_(),
    tablets_table_mgr_(nullptr)
{
}

ObTabletBackfillTXDag::~ObTabletBackfillTXDag()
{
}

int ObTabletBackfillTXDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  ls_id_.id(), static_cast<int64_t>(tablet_info_.tablet_id_.id()),
                                  "dag_net_id", to_cstring(dag_net_id_)))){
    LOG_WARN("failed to fill info param", K(ret));
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
       to_cstring(ls_id_), to_cstring(tablet_info_.tablet_id_)))) {
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
    const ObTabletBackfillTXDag &tablet_backfill_tx_dag = static_cast<const ObTabletBackfillTXDag&>(other);
    if (tablet_backfill_tx_dag.ls_id_ != ls_id_ || !(tablet_backfill_tx_dag.tablet_info_ == tablet_info_)) {
      is_same = false;
    } else {
      is_same = true;
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
        &tablet_info_.tablet_id_, sizeof(tablet_info_.tablet_id_), hash_value);
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObTabletBackfillTXDag::init(
    const share::ObTaskId &dag_net_id,
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObBackfillTXCtx *backfill_tx_ctx,
    ObBackfillTabletsTableMgr *tablets_table_mgr)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill tx dag init twice", K(ret));
  } else if (dag_net_id.is_invalid() || !ls_id.is_valid() || !tablet_info.is_valid()
      || OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(backfill_tx_ctx) || OB_ISNULL(tablets_table_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill tx dag get invalid argument", K(ret), K(dag_net_id), K(ls_id), K(tablet_info),
        KP(ha_dag_net_ctx), KP(backfill_tx_ctx), KP(tablets_table_mgr));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ls_id, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info.tablet_id_, tablet_handle))) {
    //Here get tablet handle just get compat mode
    //tablet_handle_ will init by ObTabletBackfillTXTask
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info));
  } else {
    dag_net_id_ = dag_net_id;
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    backfill_tx_ctx_ = backfill_tx_ctx;
    tablets_table_mgr_ = tablets_table_mgr;
    compat_mode_ = tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
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
  } else if (OB_FAIL(task->init(dag_net_id_, ls_id_, tablet_info_))) {
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
  ObTabletBackfillInfo next_tablet_info;
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
  } else if (OB_FAIL(backfill_tx_ctx_->get_tablet_info(next_tablet_info))) {
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
  } else if (OB_FAIL(tablet_backfill_tx_dag->init(dag_net_id_, ls_id_, next_tablet_info, ha_dag_net_ctx_,
      backfill_tx_ctx_, tablets_table_mgr_))) {
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
    if (need_set_failed_result && OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->set_result(ret, need_retry, get_type()))) {
     LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
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
  } else if (!tablet_handle_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle should not be invalid, cannot get tablet handle", K(ret), K(tablet_handle_), K(tablet_info_));
  } else {
    tablet_handle = tablet_handle_;
  }
  return ret;
}

int ObTabletBackfillTXDag::inner_reset_status_for_retry()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackfillTXCtx *ctx = nullptr;
  int32_t result = OB_SUCCESS;
  int32_t retry_count = 0;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (ha_dag_net_ctx_->is_failed()) {
    if (OB_SUCCESS != (tmp_ret = ha_dag_net_ctx_->get_result(ret))) {
      LOG_WARN("failed to get result", K(tmp_ret), KPC(ha_dag_net_ctx_));
      ret = tmp_ret;
    } else {
      LOG_INFO("set inner set status for retry failed", K(ret), KPC(ha_dag_net_ctx_));
    }
  } else if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("failed to get result", K(ret), KP(ctx));
  } else if (OB_FAIL(result_mgr_.get_retry_count(retry_count))) {
    LOG_WARN("failed to get retry count", K(ret));
  } else {
    LOG_INFO("start retry", KPC(this));
    result_mgr_.reuse();
    tablet_handle_.reset();
    if (OB_FAIL(tablets_table_mgr_->remove_tablet_table_mgr(tablet_info_.tablet_id_))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to remove tablet table mgr", K(ret), K(tablet_info_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
      LOG_WARN("failed to get ls", K(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id_));
    } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle_))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
    } else if (OB_FAIL(create_first_task())) {
      LOG_WARN("failed to create first task", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObTabletBackfillTXDag::init_tablet_handle()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret), KP(ha_dag_net_ctx_));
  } else if (tablet_handle_.is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet handle is already init", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
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
    tablet_info_(),
    tablets_table_mgr_(nullptr)

{
}

ObTabletBackfillTXTask::~ObTabletBackfillTXTask()
{
}

int ObTabletBackfillTXTask::init(
    const share::ObTaskId &dag_net_id,
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill tx task init twice", K(ret));
  } else if (dag_net_id.is_invalid() || !ls_id.is_valid() || !tablet_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill tx get invalid argument", K(ret), K(dag_net_id), K(ls_id), K(tablet_info));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    tablets_table_mgr_ = tablet_backfill_tx_dag->get_backfill_tablets_table_mgr();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    is_inited_ = true;
    LOG_INFO("succeed init st migration task", "ls id", ls_id, "tablet_info", tablet_info,
        "dag_id", *ObCurTraceId::get_trace_id(), "dag_net_id", dag_net_id);

  }
  return ret;
}

int ObTabletBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to do tablet backfill tx task", KPC(ha_dag_net_ctx_), K(tablet_info_), K(ls_id_));
  const int64_t start_ts = ObTimeUtility::current_time();
  share::ObStorageHACostItemName diagnose_result_msg = share::ObStorageHACostItemName::TRANSFER_BACKFILL_START;
  process_transfer_perf_diagnose_(start_ts, start_ts, false/*is_report*/,
      share::ObStorageHACostItemName::TRANSFER_BACKFILL_START, ret);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    //do nothing
  } else if (OB_FAIL(wait_memtable_frozen_())) {
    LOG_WARN("failed to wait memtable frozen", K(ret));
  } else if (OB_FAIL(init_tablet_handle_())) {
    LOG_WARN("failed to init tablet handle", K(ret));
  } else if (OB_FAIL(init_tablet_table_mgr_())) {
    LOG_WARN("failed to init tablet table mgr", K(ret), K(tablet_info_), K(ls_id_));
  } else if (OB_FAIL(generate_backfill_tx_task_())) {
    LOG_WARN("failed to generate backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    share::ObLSID dest_ls_id;
    share::SCN log_sync_scn;
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(tmp_ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
    }
    if (OB_TMP_FAIL(get_diagnose_support_info_(dest_ls_id, log_sync_scn))) {
      LOG_WARN("failed to get diagnose support info", K(tmp_ret));
    } else {
      ObTransferUtils::add_transfer_error_diagnose_in_backfill(dest_ls_id, log_sync_scn, ret, tablet_info_.tablet_id_, diagnose_result_msg);
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
  //ObFinishTabletBackfillTXTask *finish_backfill_tx_task = nullptr;
  ObTransferReplaceTableTask *transfer_replace_task = nullptr;
  ObArray<ObTableHandleV2> table_array;
  ObTablesHandleArray sstable_handles;
  ObITask *child_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->alloc_task(transfer_replace_task))) {
    LOG_WARN("failed to alloc transfer replace task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(transfer_replace_task->init(tablet_info_))) {
    LOG_WARN("failed to init finish backfill tx task", K(ret));
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(get_all_sstable_handles_(tablet, sstable_handles))) {
    LOG_WARN("failed to get all sstable handles", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_all_backfill_tx_tables_(sstable_handles, tablet, table_array))) {
    LOG_WARN("get all backfill tx tabels", K(ret), KPC(tablet));
  } else if (OB_FAIL(generate_mds_table_backfill_task_(transfer_replace_task, child_task))) {
    LOG_WARN("failed to generate mds table backfill task", K(ret), KPC(tablet));
  } else if (OB_FAIL(generate_table_backfill_tx_task_(transfer_replace_task, table_array, child_task))) {
    LOG_WARN("failed to generate minor sstables backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(dag_->add_task(*transfer_replace_task))) {
    LOG_WARN("failed to add transfer replace task to dag", K(ret));
  }
  return ret;
}

int ObTabletBackfillTXTask::get_all_backfill_tx_tables_(
    const ObTablesHandleArray &sstable_handles,
    ObTablet *tablet,
    common::ObIArray<ObTableHandleV2> &table_array)
{
  int ret = OB_SUCCESS;
  table_array.reset();
  ObArray<ObTableHandleV2> memtables;
  ObArray<ObTableHandleV2> non_backfill_sstables;
  ObArray<ObTableHandleV2> backfill_sstables;
  const int64_t emergency_sstable_count = ObTabletTableStore::EMERGENCY_SSTABLE_CNT;
  SCN max_end_major_scn(SCN::min_scn());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get all backfll tx tables get invalid argument", K(ret), KP(tablet));
  } else if (OB_FAIL(split_sstable_array_by_backfill_(sstable_handles, non_backfill_sstables, backfill_sstables, max_end_major_scn))) {
    LOG_WARN("failed to split sstable array by backfil", K(ret), KPC(tablet));
  } else if (OB_FAIL(add_ready_sstable_into_table_mgr_(max_end_major_scn, tablet, non_backfill_sstables))) {
    LOG_WARN("failed to add ready sstable into table mgr", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_backfill_tx_memtables_(tablet, memtables))) {
    LOG_WARN("failed to get backfill tx memtables", K(ret), KPC(tablet));
  } else if (OB_FAIL(append(table_array, memtables))) {
    LOG_WARN("failed to append memtables", K(ret), KPC(tablet), K(memtables));
  } else if (OB_FAIL(append(table_array, backfill_sstables))) {
    LOG_WARN("failed to append minor sstables", K(ret), KPC(tablet), K(backfill_sstables));
  } else {
    // The backfill of sstable needs to start with a larger start_scn
    if (table_array.count() > emergency_sstable_count) {
        ret = OB_TOO_MANY_SSTABLE;
        LOG_WARN("transfer src tablet has too many sstable, cannot backfill, need retry", K(ret),
            "table_count", table_array.count(), "emergency sstable count", emergency_sstable_count);
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::get_backfill_tx_memtables_(
    ObTablet *tablet,
    common::ObIArray<ObTableHandleV2> &table_array)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObArray<ObTableHandleV2> memtables;
  const int64_t OB_CHECK_MEMTABLE_INTERVAL = 200 * 1000; // 200ms
  const int64_t OB_WAIT_MEMTABLE_READY_TIMEOUT = 30 * 60 * 1000 * 1000L; // 30 min
  table_array.reset();
  const bool need_active = true;
  share::SCN memtable_end_scn;
  share::SCN tablet_clog_checkpoint_scn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backfll tx memtables get invalid argument", K(ret), KP(tablet));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_), KPC(tablet));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(ls_id_));
  } else if (OB_FAIL(tablet->get_memtables(memtables, need_active))) {
    LOG_WARN("failed to get_memtable_mgr for get all memtable", K(ret), KPC(tablet));
  } else if (FALSE_IT(tablet_clog_checkpoint_scn = tablet->get_clog_checkpoint_scn())) {
  } else if (memtables.empty()) {
    FLOG_INFO("transfer src tablet memtable is empty", KPC(tablet));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      ObITable *table = memtables.at(i).get_table();
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
      if (OB_ISNULL(table) || !table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KP(table));
      } else if (!memtable->is_frozen_memtable()) {
        if (tablet_info_.is_committed_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable should be frozen memtable, active memtable is unexpected", K(ret), KPC(memtable), KPC_(backfill_tx_ctx));
        } else {
          ret = OB_EAGAIN;
          LOG_WARN("tablet still has active memtable, need retry", K(ret), KPC(memtable), KPC_(backfill_tx_ctx));
        }
      } else if (table->is_direct_load_memtable()) {
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_WARN("find a direct load memtable", KR(ret), K(tablet_info_.tablet_id_), KPC(table));
      } else if (table->get_start_scn() >= backfill_tx_ctx_->backfill_scn_
          && (memtable->not_empty() || !table->get_scn_range().is_empty())) {
        if (tablet_info_.is_committed_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable start log ts is bigger than log sync scn but not empty", K(ret), KPC(memtable), KPC_(backfill_tx_ctx));
        } else {
          ret = OB_EAGAIN;
          LOG_WARN("memtable start log ts is bigger than log sync scn but not empty, need retry", K(ret), KPC(memtable), KPC_(backfill_tx_ctx));
        }
      } else {
        memtable_end_scn = memtable->get_end_scn();
      }

      if (OB_FAIL(ret)) {
      } else if (memtable_end_scn <= tablet_clog_checkpoint_scn) {
        FLOG_INFO("memtable end scn is smaller than tablet clog checkpoint scn, sstable contains memtable whole range",
            K(memtable_end_scn), K(tablet_clog_checkpoint_scn), KPC(memtable));
      } else if (OB_FAIL(table_array.push_back(memtables.at(i)))) {
        LOG_WARN("failed to push table into array", K(ret), KPC(table));
      }
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::generate_table_backfill_tx_task_(
    share::ObITask *replace_task,
    common::ObIArray<ObTableHandleV2> &table_array,
    share::ObITask *child)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTabletHandle tablet_handle;
  share::ObITask *pre_task = child;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(replace_task) || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate table backfill tx task get invalid argument",
        K(ret), KP(replace_task), K(ls_id_), K(tablet_info_), KP(child));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_array.count(); ++i) {
      ObITable *table = table_array.at(i).get_table();
      ObTabletTableBackfillTXTask *table_backfill_tx_task = nullptr;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KPC(table));
      } else if ((!table->is_data_memtable() && !table->is_minor_sstable())
          || table->is_remote_logical_minor_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KPC(table));
      } else if (OB_FAIL(tablet_backfill_tx_dag->alloc_task(table_backfill_tx_task))) {
        LOG_WARN("failed to alloc table backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
      } else if (OB_FAIL(table_backfill_tx_task->init(ls_id_, tablet_info_, tablet_handle, table_array.at(i), replace_task))) {
        LOG_WARN("failed to init table backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
      } else if (OB_FAIL(pre_task->add_child(*table_backfill_tx_task))) {
        LOG_WARN("failed to add table backfill tx task as child", K(ret), K(ls_id_), K(tablet_info_), KPC(table), KPC(pre_task));
      } else if (OB_FAIL(table_backfill_tx_task->add_child(*replace_task))) {
        LOG_WARN("failed to add replace task as child", K(ret), K(ls_id_), K(tablet_info_), KPC(table));
      } else if (OB_FAIL(dag_->add_task(*table_backfill_tx_task))) {
        LOG_WARN("failed to add table backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
      } else {
        //all table will run concurrency
        LOG_INFO("generate table backfill TX", KPC(table), K(i), KPC(table_backfill_tx_task));
      }
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::get_all_sstable_handles_(
    const ObTablet *tablet,
    ObTablesHandleArray &sstable_handles)
{
  int ret = OB_SUCCESS;
  sstable_handles.reset();
  ObTableHandleV2 table_handle;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  ObTableStoreIterator sstable_iter;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet should not be nullptr", K(ret));
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_undefined()
      || tablet->get_tablet_meta().ha_status_.is_restore_status_pending()
      || tablet->get_tablet_meta().ha_status_.is_restore_status_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet data is incomplete, replacement should not be performed", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), KP(tablet));
  } else if (OB_FAIL(check_major_sstable_(tablet, wrapper))) {
    LOG_WARN("failed check major sstable", K(ret), KP(tablet));
  } else if (OB_FAIL(wrapper.get_member()->get_all_sstable(sstable_iter, false /*unpack_co_table*/))) {
    LOG_WARN("get all sstable fail", K(ret));
  } else if (OB_FAIL(add_sstable_into_handles_(sstable_iter, sstable_handles))) {
    LOG_WARN("failed to add data sstable into handles", K(ret));
  }
  return ret;
}

int ObTabletBackfillTXTask::check_major_sstable_(
    const ObTablet *tablet,
    const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator ddl_iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet) || !table_store_wrapper.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check major sstable get invalid argument", K(ret), KP(tablet), K(table_store_wrapper));
  } else if (!table_store_wrapper.get_member()->get_major_sstables().empty()) {
    // do nothing
  } else if (OB_FAIL(tablet->get_ddl_sstables(ddl_iter))) {
    LOG_WARN("failed to get ddl sstable", K(ret));
  } else if (ddl_iter.is_valid()) {
    ret = OB_EAGAIN;
    LOG_WARN("wait for ddl sstable to merge to generate major sstable", K(ret), K(ddl_iter));
  } else if (tablet->get_tablet_meta().ha_status_.is_restore_status_full()) {
    ret = OB_INVALID_TABLE_STORE;
    LOG_ERROR("neither major sstable nor ddl sstable exists", K(ret), K(ddl_iter));
  }

  return ret;
}

int ObTabletBackfillTXTask::init_tablet_table_mgr_()
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int64_t transfer_seq = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_info_));
  } else if (FALSE_IT(transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (OB_FAIL(tablets_table_mgr_->init_tablet_table_mgr(tablet_info_.tablet_id_, transfer_seq))) {
    LOG_WARN("failed to init tablet table mgr", K(ret), K(tablet_info_));
  }
  return ret;
}

int ObTabletBackfillTXTask::split_sstable_array_by_backfill_(
    const ObTablesHandleArray &sstable_handles,
    common::ObIArray<ObTableHandleV2> &non_backfill_sstable,
    common::ObIArray<ObTableHandleV2> &backfill_sstable,
    share::SCN &max_end_major_scn)
{
  int ret = OB_SUCCESS;
  non_backfill_sstable.reset();
  backfill_sstable.reset();
  max_end_major_scn.set_min();
  DEBUG_SYNC(STOP_TRANSFER_LS_LOGICAL_TABLE_REPLACED);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_handles.get_count(); ++i) {
      ObTableHandleV2 table_handle;
      ObITable *table = nullptr;
      if (OB_FAIL(sstable_handles.get_table(i, table_handle))) {
        LOG_WARN("failed to get table", K(ret), K(tablet_info_), K(ls_id_));
      } else if (OB_ISNULL(table = table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be NULL", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
      } else if (table->is_major_sstable()) {
        //major sstable will do not add to table mgr, the major sstable will get from tablet
        max_end_major_scn = SCN::max(table->get_end_scn(), max_end_major_scn);
      } else if (table->is_minor_sstable() || table->is_mds_sstable()) {
        ObSSTable *sstable = nullptr;
        ObSSTableMetaHandle sst_meta_hdl;
        if (FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
        } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
          LOG_WARN("failed to get sstable meta handle", K(ret));
        } else if (!sstable->contain_uncommitted_row()
              || (!sst_meta_hdl.get_sstable_meta().get_filled_tx_scn().is_max()
                && sst_meta_hdl.get_sstable_meta().get_filled_tx_scn() >= backfill_tx_ctx_->backfill_scn_)) {
          FLOG_INFO("sstable do not contain uncommitted row, no need backfill tx", KPC(sstable),
              "backfill scn", backfill_tx_ctx_->backfill_scn_);
          if (OB_FAIL(non_backfill_sstable.push_back(table_handle))) {
            LOG_WARN("failed to push table handle into array", K(ret), K(table_handle));
          }
        } else if (OB_FAIL(backfill_sstable.push_back(table_handle))) {
          LOG_WARN("failed to push table handle into array", K(ret), K(tablet_info_), K(ls_id_), K(table_handle));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is not sstable, unexpected", K(ret), KPC(table), K(tablet_info_), K(ls_id_));
      }
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::add_ready_sstable_into_table_mgr_(
    const share::SCN &max_major_end_scn,
    ObTablet *tablet,
    common::ObIArray<ObTableHandleV2> &non_backfill_sstable)
{
  int ret = OB_SUCCESS;
  int64_t transfer_seq = 0;
  SCN transfer_start_scn;
  ObTabletCreateDeleteMdsUserData user_data;
  bool is_committed = false;
  ObLSService* ls_srv = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  int64_t rebuild_seq = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet) || !max_major_end_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add ready sstable into table mgr get invalid argument", K(ret), KP(tablet), K(max_major_end_scn));
  } else if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls srv should not be NULL", K(ret), KP(ls_srv));
  } else if (OB_FAIL(ls_srv->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is NULL", KR(ret), K(ls_id_));
  } else if (FALSE_IT(rebuild_seq = ls->get_ls_meta().get_rebuild_seq())) {
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
    LOG_WARN("failed to get latest tablet status", K(ret), KP(tablet));
  } else if (FALSE_IT(transfer_start_scn = user_data.transfer_scn_)) {
  } else if (FALSE_IT(transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < non_backfill_sstable.count(); ++i) {
      ObTableHandleV2 &table_handle = non_backfill_sstable.at(i);
      if (OB_FAIL(tablets_table_mgr_->add_sstable(
          tablet_info_.tablet_id_, rebuild_seq, transfer_start_scn, transfer_seq, table_handle))) {
        LOG_WARN("failed to add sstable", K(ret), K(table_handle), K(tablet_info_), K(ls_id_));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(tablets_table_mgr_->set_max_major_end_scn(tablet_info_.tablet_id_, max_major_end_scn))) {
        LOG_WARN("failed to push max major end scn into table mgr", K(ret), K(tablet_info_), K(ls_id_));
      }
    }

    if (OB_SUCC(ret)) {
      //double check src tablet transfer scn
      if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
        LOG_WARN("failed to get latest tablet status", K(ret), KP(tablet));
      } else if (user_data.transfer_scn_ != backfill_tx_ctx_->backfill_scn_) {
        ret = OB_EAGAIN;
        LOG_WARN("transfer src tablet transfer scn is not equal to backfill scn, may transfer out trans rollback",
            K(ret), K(user_data), "backfill scn", backfill_tx_ctx_->backfill_scn_);
      }
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::get_diagnose_support_info_(share::ObLSID &dest_ls_id, share::SCN &backfill_scn) const
{
  int ret = OB_SUCCESS;
  dest_ls_id.reset();
  backfill_scn.reset();
  if (ObIHADagNetCtx::TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), "ctx type", ha_dag_net_ctx_->get_dag_net_ctx_type());
  } else {
    dest_ls_id = static_cast<const ObTransferBackfillTXCtx *>(ha_dag_net_ctx_)->get_ls_id();
    backfill_scn = backfill_tx_ctx_->backfill_scn_;
  }
  return ret;
}

void ObTabletBackfillTXTask::process_transfer_perf_diagnose_(
    const int64_t timestamp,
    const int64_t start_ts,
    const bool is_report,
    const ObStorageHACostItemName name,
    const int result) const
{
  int ret = OB_SUCCESS;
  share::ObLSID dest_ls_id;
  share::SCN log_sync_scn;
  if (OB_FAIL(get_diagnose_support_info_(dest_ls_id, log_sync_scn))) {
    LOG_WARN("failed to get diagnose support info", K(ret));
  } else {
    ObStorageHAPerfDiagParams params;
    ObTransferUtils::process_backfill_perf_diag_info(dest_ls_id, tablet_info_.tablet_id_,
        ObStorageHACostItemType::FLUENT_TIMESTAMP_TYPE, name, params);
    ObTransferUtils::add_transfer_perf_diagnose_in_backfill(params, log_sync_scn, result, timestamp, start_ts, is_report);
  }
}

int ObTabletBackfillTXTask::add_sstable_into_handles_(
    ObTableStoreIterator &sstable_iter,
    ObTablesHandleArray &sstable_handles)
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_iter.get_next(table_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get mds table", K(ret));
      }
    } else if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, table is nullptr", K(ret), K(table_handle));
    } else if (table_handle.get_table()->is_mds_sstable()) {
      //do nothing
    } else if (OB_FAIL(sstable_handles.add_table(table_handle))) {
      LOG_WARN("fail to add sstable handle", K(ret), K(table_handle));
    }
  }
  return ret;
}

int ObTabletBackfillTXTask::generate_mds_table_backfill_task_(
    share::ObITask *finish_task,
    share::ObITask *&child)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  ObTabletHandle tablet_handle;
  ObTabletMdsTableBackfillTXTask *mds_table_backfill_tx_task = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(finish_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("generate table backfill tx task get invalid argument",
        K(ret), KP(finish_task), K(ls_id_), K(tablet_info_));
  } else if (FALSE_IT(tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag*>(this->get_dag()))) {
  } else if (OB_FAIL(tablet_backfill_tx_dag->get_tablet_handle(tablet_handle))) {
    LOG_WARN("failed to get tablet handler", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(tablet_backfill_tx_dag->alloc_task(mds_table_backfill_tx_task))) {
    LOG_WARN("failed to alloc table backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(mds_table_backfill_tx_task->init(ls_id_, tablet_info_, tablet_handle))) {
    LOG_WARN("failed to init mds table backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(this->add_child(*mds_table_backfill_tx_task))) {
    LOG_WARN("failed to add table backfill tx task as child", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(mds_table_backfill_tx_task->add_child(*finish_task))) {
    LOG_WARN("failed to add finish backfill tx task as child", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(dag_->add_task(*mds_table_backfill_tx_task))) {
    LOG_WARN("failed to add table backfill tx task", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    child = mds_table_backfill_tx_task;
    LOG_INFO("generate mds table backfill task", KPC(mds_table_backfill_tx_task));
  }
  return ret;
}

int ObTabletBackfillTXTask::wait_memtable_frozen_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObArray<ObTableHandleV2> memtables;
  const int64_t OB_CHECK_MEMTABLE_INTERVAL = 200 * 1000; // 200ms
  const int64_t OB_WAIT_MEMTABLE_READY_TIMEOUT = 30 * 60 * 1000 * 1000L; // 30 min
  const int64_t start_ts = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHADagUtils::get_ls(ls_id_, ls_handle))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_), KPC(tablet));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id_));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_));
  } else {
    const int64_t wait_memtable_start_ts = ObTimeUtility::current_time();
    const int64_t current_ts = 0;
    while (OB_SUCC(ret)) {
      memtables.reset();
      bool is_memtable_ready = true;
      if (OB_FAIL(tablet->get_all_memtables(memtables))) {
        LOG_WARN("failed to get all memtables", K(ret), KPC(tablet));
      } else if (memtables.empty()) {
        FLOG_INFO("transfer src tablet memtable is empty", KPC(tablet));
        break;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
          ObITable *table = memtables.at(i).get_table();
          memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(table);
          if (OB_ISNULL(table) || !table->is_memtable()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table should not be NULL or table type is unexpected", K(ret), KP(table));
          } else if (table->is_direct_load_memtable()) {
            ret = OB_TRANSFER_SYS_ERROR;
            LOG_WARN("find a direct load memtable", KR(ret), K(tablet_info_.tablet_id_), KPC(table));
          } else if (table->get_start_scn() >= backfill_tx_ctx_->backfill_scn_
              && memtable->not_empty()
              && !memtable->get_rec_scn().is_max()) {
            if (tablet_info_.is_committed_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("memtable start log ts is bigger than log sync scn but not empty", K(ret), KPC(memtable), KPC_(backfill_tx_ctx));
            } else {
              ret = OB_EAGAIN;
              LOG_WARN("memtable start log ts is bigger than log sync scn but not empty, need retry", K(ret), KPC(memtable), KPC_(backfill_tx_ctx));
            }
          } else if (!table->is_frozen_memtable()) {
            is_memtable_ready = false;
            const bool is_sync = false;
            if (OB_FAIL(ls->tablet_freeze(tablet_info_.tablet_id_, is_sync))) {
              if (OB_EAGAIN == ret) {
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to force tablet freeze", K(ret), K(tablet_info_), KPC(table));
              }
            } else {
              break;
            }
          } else if (!memtable->is_can_flush()) {
            is_memtable_ready = false;
          }

          if (OB_SUCC(ret)) {
            if (is_memtable_ready) {
              break;
            } else {
              const int64_t current_ts = ObTimeUtility::current_time();
              if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
                LOG_INFO("tablet not ready, retry next loop", "tablet_id", tablet_info_,
                    "wait_tablet_start_ts", wait_memtable_start_ts,
                    "current_ts", current_ts);
              }

              if (current_ts - wait_memtable_start_ts < OB_WAIT_MEMTABLE_READY_TIMEOUT) {
              } else {
                ret = OB_TIMEOUT;
                STORAGE_LOG(WARN, "failed to check tablet memtable ready, timeout, stop backfill",
                    K(ret), KPC(tablet), K(current_ts),
                    K(wait_memtable_start_ts));
              }

              if (OB_SUCC(ret)) {
                ob_usleep(OB_CHECK_MEMTABLE_INTERVAL);
              }
            }
          }
        }
      }
    }
  }

  LOG_INFO("wait memtable frozen finish", K(ret), K(ls_id_), K(tablet_info_),
      "cost_ts: ", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObTabletBackfillTXTask::init_tablet_handle_()
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx task do not init", K(ret));
  } else if (ObDagType::DAG_TYPE_TABLET_BACKFILL_TX != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init tablet mds table backfill dag type is unexpected", K(ret), KPC(this));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    if (OB_FAIL(tablet_backfill_tx_dag->init_tablet_handle())) {
      LOG_WARN("failed to init tablet handle", K(ret), KPC(this), K(ls_id_), K(tablet_info_));
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
    tablet_info_(),
    tablet_handle_(),
    table_handle_(),
    tablets_table_mgr_(nullptr),
    child_(nullptr)
{
}

ObTabletTableBackfillTXTask::~ObTabletTableBackfillTXTask()
{
}

int ObTabletTableBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObTabletHandle &tablet_handle,
    ObTableHandleV2 &table_handle,
    share::ObITask *child)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet table back fill tx task do not init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || !tablet_handle.is_valid() || !table_handle.is_valid() || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table backfill task get invalid argument", K(ret), K(ls_id),
        K(tablet_info), K(tablet_handle), K(table_handle));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    tablet_handle_ = tablet_handle;
    table_handle_ = table_handle;
    tablets_table_mgr_ = tablet_backfill_tx_dag->get_backfill_tablets_table_mgr();
    child_ = child;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletTableBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do tablet table backfill tx task", K(ls_id_), K(tablet_info_), K(table_handle_));
  bool need_merge = true;
  const int64_t start_ts = ObTimeUtility::current_time();
  share::ObStorageHACostItemName diagnose_result_msg = share::ObStorageHACostItemName::MAX_NAME;
  process_transfer_perf_diagnose_(start_ts, start_ts, false/*is_report*/,
      ObStorageHACostItemType::FLUENT_TIMESTAMP_TYPE, ObStorageHACostItemName::TRANSFER_BACKFILLED_TABLE_BEGIN, ret);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    LOG_INFO("ctx already failed", KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(check_need_merge_(need_merge))) {
    LOG_WARN("failed to check need merge", K(ret));
  } else if (!need_merge) {
    LOG_INFO("tablet table no need merge", K(ret), K_(tablet_handle));
  } else if (OB_FAIL(generate_merge_task_())) {
    LOG_WARN("failed to generate merge task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_TRANSFER_DIAGNOSE_BACKFILL_FAILED ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "fake EN_TRANSFER_DIAGNOSE_BACKFILL_FAILED", K(ret));
    }
  }
#endif
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    share::ObLSID dest_ls_id;
    share::SCN log_sync_scn;
    if (OB_TMP_FAIL(get_diagnose_support_info_(dest_ls_id, log_sync_scn))) {
      LOG_WARN("failed to get diagnose support info", K(tmp_ret));
    } else {
      ObTransferUtils::add_transfer_error_diagnose_in_backfill(dest_ls_id, log_sync_scn, ret, tablet_info_.tablet_id_, diagnose_result_msg);
    }
  }

  if (OB_SUCC(ret)) {
    const int64_t end_ts = ObTimeUtility::current_time();
    process_transfer_perf_diagnose_(end_ts, start_ts, false/*is_report*/,
        ObStorageHACostItemType::FLUENT_TIMESTAMP_TYPE, ObStorageHACostItemName::TX_BACKFILL, ret);
  }
  return ret;
}

int ObTabletTableBackfillTXTask::check_need_merge_(bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = true;
  if (!table_handle_.get_table()->is_memtable()) {
    // do nothing
  } else if (!table_handle_.get_table()->get_key().scn_range_.is_empty()) {
    // do nothing
  } else {
    need_merge = false;
  }
  return ret;
}

int ObTabletTableBackfillTXTask::generate_merge_task_()
{
  int ret = OB_SUCCESS;
  ObTabletMergeTask *merge_task = nullptr;
  ObTabletTableFinishBackfillTXTask *finish_backfill_task = nullptr;
  const int64_t index = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(finish_backfill_task))) {
    LOG_WARN("failed to alloc tablet finish backfill tx task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(finish_backfill_task->init(ls_id_, tablet_info_, tablet_handle_, table_handle_, child_))) {
    LOG_WARN("failed to init table finish backfill tx task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(finish_backfill_task->add_child(*child_))) {
    LOG_WARN("failed to add child task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(dag_->alloc_task(merge_task))) {
    LOG_WARN("failed to alloc table merge task", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(merge_task->init(index, finish_backfill_task->get_tablet_merge_ctx()))) {
    LOG_WARN("failed to init table merge task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(merge_task->add_child(*finish_backfill_task))) {
    LOG_WARN("failed to add child task", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(this->add_child(*merge_task))) {
    LOG_WARN("failed to add child task", K(ret), K(ls_id_), K(tablet_info_), KPC(this));
  } else if (OB_FAIL(dag_->add_task(*finish_backfill_task))) {
    LOG_WARN("failed to add table finish backfill tx task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(dag_->add_task(*merge_task))) {
    LOG_WARN("failed to add table merge task", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  }
  return ret;
}

int ObTabletTableBackfillTXTask::get_diagnose_support_info_(share::ObLSID &dest_ls_id, share::SCN &log_sync_scn) const
{
  int ret = OB_SUCCESS;
  dest_ls_id.reset();
  log_sync_scn.reset();
  if (ObIHADagNetCtx::TRANSFER_BACKFILL_TX != ha_dag_net_ctx_->get_dag_net_ctx_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ret), "ctx type", ha_dag_net_ctx_->get_dag_net_ctx_type());
  } else {
    dest_ls_id = static_cast<const ObTransferBackfillTXCtx *>(ha_dag_net_ctx_)->get_ls_id();
    log_sync_scn = backfill_tx_ctx_->backfill_scn_;
  }
  return ret;
}

void ObTabletTableBackfillTXTask::process_transfer_perf_diagnose_(
    const int64_t timestamp,
    const int64_t start_ts,
    const bool is_report,
    const ObStorageHACostItemType type,
    const ObStorageHACostItemName name,
    const int result) const
{
  int ret = OB_SUCCESS;
  share::ObLSID dest_ls_id;
  share::SCN log_sync_scn;
  if (OB_FAIL(get_diagnose_support_info_(dest_ls_id, log_sync_scn))) {
    LOG_WARN("failed to get diagnose support info", K(ret));
  } else {
    ObStorageHAPerfDiagParams params;
    ObTransferUtils::process_backfill_perf_diag_info(dest_ls_id, tablet_info_.tablet_id_,
        type, name, params);
    ObTransferUtils::add_transfer_perf_diagnose_in_backfill(params, log_sync_scn, result, timestamp, start_ts, is_report);
  }
}

/******************ObTabletTableFinishBackfillTXTask*********************/
ObTabletTableFinishBackfillTXTask::ObTabletTableFinishBackfillTXTask()
  : ObITask(TASK_TYPE_TABLE_FINISH_BACKFILL),
    is_inited_(false),
    backfill_tx_ctx_(nullptr),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    tablet_handle_(),
    table_handle_(),
    param_(),
    allocator_("TableBackfillTX"),
    tablet_merge_ctx_(param_, allocator_),
    tablets_table_mgr_(nullptr),
    child_(nullptr)
{
}

ObTabletTableFinishBackfillTXTask::~ObTabletTableFinishBackfillTXTask()
{
}

int ObTabletTableFinishBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObTabletHandle &tablet_handle,
    ObTableHandleV2 &table_handle,
    share::ObITask *child)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet table finish backfill tx task do not init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || !tablet_handle.is_valid() || !table_handle.is_valid() || OB_ISNULL(child)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet table finish backfill task get invalid argument", K(ret), K(ls_id),
        K(tablet_info), K(tablet_handle), K(table_handle));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    tablet_handle_ = tablet_handle;
    table_handle_ = table_handle;
    tablets_table_mgr_ = tablet_backfill_tx_dag->get_backfill_tablets_table_mgr();
    child_ = child;

    if (OB_FAIL(prepare_merge_ctx_())) {
      LOG_WARN("failed to prepare merge ctx", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTabletTableFinishBackfillTXTask::prepare_merge_ctx_()
{
  int ret = OB_SUCCESS;
  param_.merge_version_ = ObVersion::MIN_VERSION; //only major or meta major need this, mini or minor do not use it
  param_.ls_id_ = ls_id_;
  param_.tablet_id_ = tablet_info_.tablet_id_;
  param_.skip_get_tablet_ = true;
  param_.merge_type_ = table_handle_.get_table()->is_memtable() ? ObMergeType::MINI_MERGE : ObMergeType::MINOR_MERGE;
  bool unused_finish_flag = false;
  int64_t local_rebuild_seq = 0;
  if (OB_FAIL(tablets_table_mgr_->get_local_rebuild_seq(local_rebuild_seq))) {
    LOG_WARN("failed to get local rebuild seq", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(tablet_merge_ctx_.init(backfill_tx_ctx_->backfill_scn_, local_rebuild_seq, tablet_handle_, table_handle_))) {
    LOG_WARN("failed to init tablet merge ctx", K(ret), K(ls_id_), K(tablet_info_), K(table_handle_));
  } else if (OB_FAIL(tablet_merge_ctx_.build_ctx(unused_finish_flag))) {
    LOG_WARN("failed to build ctx", K(ret), K(ls_id_), K(tablet_info_));
  }
  return ret;
}

int ObTabletTableFinishBackfillTXTask::update_merge_sstable_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  bool skip_to_create_empty_cg = false; // placeholder
  compaction::ObStaticMergeParam &static_param = tablet_merge_ctx_.static_param_;
  int64_t transfer_seq = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int64_t rebuild_seq = 0;
  ObTabletCreateDeleteMdsUserData user_data;
  bool is_committed = false;
  SCN transfer_start_scn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table finish backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls = static_param.ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id_));
  } else if (FALSE_IT(rebuild_seq = ls->get_ls_meta().get_rebuild_seq())) {
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(tablet_handle));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
    LOG_WARN("failed to get latest tablet status", K(ret), KP(tablet));
  } else if (FALSE_IT(transfer_start_scn = user_data.transfer_scn_)) {
  } else if (FALSE_IT(transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (!transfer_start_scn.is_valid()) {
    if (tablet_info_.is_committed_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer transaction is committed but transfer start scn is invalid, unexpected", K(ret), K(tablet_info_), K(user_data));
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("transfer start scn is invalid, may transfer transaction rollback, need retry", K(ret), K(tablet_info_), K(user_data));
    }
  } else if (OB_FAIL(tablet_merge_ctx_.merge_info_.create_sstable(
      tablet_merge_ctx_,
      tablet_merge_ctx_.merged_table_handle_,
      skip_to_create_empty_cg))) {
    LOG_WARN("fail to create sstable", K(ret), K(tablet_merge_ctx_));
  } else if (OB_FAIL(tablets_table_mgr_->add_sstable(
      tablet_info_.tablet_id_, rebuild_seq, transfer_start_scn, transfer_seq, tablet_merge_ctx_.merged_table_handle_))) {
    LOG_WARN("failed to add sstable", K(ret), K(ls_id_), K(tablet_info_), K(tablet_merge_ctx_));
  }
  return ret;
}

int ObTabletTableFinishBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start do tablet table finish backfill tx task", K(ls_id_), K(tablet_info_), K(table_handle_));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet table backfill tx task do not init", K(ret));
  } else if (ha_dag_net_ctx_->is_failed()) {
    LOG_INFO("ctx already failed", KPC(ha_dag_net_ctx_));
  } else if (OB_FAIL(update_merge_sstable_())) {
    LOG_WARN("failed to update merge sstable", K(ret), KPC(this));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    if (OB_ISNULL(tablet_backfill_tx_dag)) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
    } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
      LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
    }
  }
  return ret;
}

/******************ObFinishBackfillTXDag*********************/
ObFinishBackfillTXDag::ObFinishBackfillTXDag()
  : ObStorageHADag(ObDagType::DAG_TYPE_FINISH_BACKFILL_TX),
    is_inited_(false),
    backfill_tx_ctx_(),
    tablets_table_mgr_(nullptr)
{
}

ObFinishBackfillTXDag::~ObFinishBackfillTXDag()
{
}

int ObFinishBackfillTXDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill tx dag do not init", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                  backfill_tx_ctx_.ls_id_.id(),
                                  "dag_net_id", to_cstring(backfill_tx_ctx_.task_id_)))) {
    LOG_WARN("failed to fill info param", K(ret));
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
    const ObFinishBackfillTXDag &finish_backfill_tx_dag = static_cast<const ObFinishBackfillTXDag&>(other);
    if (OB_FAIL(backfill_tx_ctx_.check_is_same(finish_backfill_tx_dag.backfill_tx_ctx_, is_same))) {
      LOG_WARN("failed to check is same", K(ret), K(*this));
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
    ObDagType::ObDagTypeEnum dag_type = get_type();
    hash_value = common::murmurhash(
        &dag_type, sizeof(dag_type), hash_value);
  }
  return hash_value;
}

int ObFinishBackfillTXDag::init(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    const SCN &log_sync_scn,
    common::ObArray<ObTabletBackfillInfo> &tablet_info_array,
    ObIHADagNetCtx *ha_dag_net_ctx,
    ObBackfillTabletsTableMgr *tablets_table_mgr)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("finish backfill tx dag init twice", K(ret));
  } else if (task_id.is_invalid() || !ls_id.is_valid() || !log_sync_scn.is_valid()
      || OB_ISNULL(ha_dag_net_ctx) || OB_ISNULL(tablets_table_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init finish backfill tx dag get invalid argument", K(ret), K(task_id), K(ls_id), K(log_sync_scn) ,KP(ha_dag_net_ctx));
  } else if (OB_FAIL(backfill_tx_ctx_.build_backfill_tx_ctx(task_id, ls_id, log_sync_scn, tablet_info_array))) {
    LOG_WARN("failed to build backfill tx ctx", K(ret), K(tablet_info_array));
  } else {
    ha_dag_net_ctx_ = ha_dag_net_ctx;
    tablets_table_mgr_ = tablets_table_mgr;
    is_inited_ = true;
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
    LOG_INFO("ctx already failed", KPC(ha_dag_net_ctx_));
  } else {
    //TODO(muwei.ym) FIX IT later ObFinishBackfillTXTask::process in 4.3
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

/******************ObTabletMdsTableBackfillTXTask*********************/
ObTabletMdsTableBackfillTXTask::ObTabletMdsTableBackfillTXTask()
  : ObITask(TASK_TYPE_MIGRATE_PREPARE),
    is_inited_(false),
    backfill_tx_ctx_(nullptr),
    ha_dag_net_ctx_(nullptr),
    ls_id_(),
    tablet_info_(),
    tablet_handle_(),
    allocator_("MdsBackfillTX"),
    merger_arena_("TblBkfilMerger", OB_MALLOC_NORMAL_BLOCK_SIZE),
    tablets_table_mgr_(nullptr)
{
}

ObTabletMdsTableBackfillTXTask::~ObTabletMdsTableBackfillTXTask()
{
}

int ObTabletMdsTableBackfillTXTask::init(
    const share::ObLSID &ls_id,
    const ObTabletBackfillInfo &tablet_info,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletBackfillTXDag *tablet_backfill_tx_dag = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet mds table back fill tx task do not init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_info.is_valid() || !tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet mds table backfill task get invalid argument", K(ret), K(ls_id),
        K(tablet_info), K(tablet_handle));
  } else if (ObDagType::DAG_TYPE_TABLET_BACKFILL_TX != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init tablet mds table backfill dag type is unexpected", K(ret), KPC(this));
  } else {
    tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
    ha_dag_net_ctx_ = tablet_backfill_tx_dag->get_ha_dag_net_ctx();
    backfill_tx_ctx_ = tablet_backfill_tx_dag->get_backfill_tx_ctx();
    ls_id_ = ls_id;
    tablet_info_ = tablet_info;
    tablet_handle_ = tablet_handle;
    tablets_table_mgr_ = tablet_backfill_tx_dag->get_backfill_tablets_table_mgr();
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::process()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 mds_sstable;
  common::ObArray<ObTableHandleV2> mds_sstable_array;
  ObTableHandleV2 final_mds_sstable;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_FAIL(do_backfill_mds_table_(mds_sstable))) {
    LOG_WARN("failed to do backfill mds table", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_FAIL(prepare_backfill_mds_sstables_(mds_sstable, mds_sstable_array))) {
    LOG_WARN("failed to prepare backfill mds sstables", K(ret), K(ls_id_), K(tablet_info_));
  } else if (mds_sstable_array.empty()) {
    //allow mds sstable array empty, because tablet status will be filtered by transfer
  } else if (OB_FAIL(do_backfill_mds_sstables_(mds_sstable_array, final_mds_sstable))) {
    LOG_WARN("failed to do backfill mds sstables", K(ret), K(ls_id_), K(tablet_info_));
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (ObDagType::DAG_TYPE_TABLET_BACKFILL_TX != this->get_dag()->get_type()) {
      tmp_ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet mds table backfill dag type is unexpected", K(tmp_ret), KPC(this));
    } else {
      ObTabletBackfillTXDag *tablet_backfill_tx_dag = static_cast<ObTabletBackfillTXDag *>(this->get_dag());
      if (OB_ISNULL(tablet_backfill_tx_dag)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablet backfill tx dag should not be NULL", K(tmp_ret), KP(tablet_backfill_tx_dag));
      } else if (OB_SUCCESS != (tmp_ret = tablet_backfill_tx_dag->set_result(ret))) {
        LOG_WARN("failed to set result", K(ret), KPC(ha_dag_net_ctx_), K(ls_id_), K(tablet_info_));
      }
    }
  }
  return ret;
}

//mds table means the mds data in memory
int ObTabletMdsTableBackfillTXTask::do_backfill_mds_table_(
    ObTableHandleV2 &mds_sstable)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(tablet_handle_));
  } else if (tablet->get_mds_checkpoint_scn() >= backfill_tx_ctx_->backfill_scn_) {
    LOG_INFO("mds checkpoint is bigger than backfill scn, skip mds table backfill", KPC(tablet), KPC(backfill_tx_ctx_));
  } else {
    SMART_VARS_2((compaction::ObTabletMergeDagParam, param), (compaction::ObTabletMergeCtx, tablet_merge_ctx, param, allocator_)) {
      if (OB_FAIL(prepare_mds_table_merge_ctx_(tablet_merge_ctx))) {
        LOG_WARN("failed to prepare mds table merge ctx", K(ret));
      } else if (OB_FAIL(build_mds_table_to_sstable_(tablet_merge_ctx, mds_sstable))) {
        LOG_WARN("failed to build mds table to sstable", K(ret), K(tablet_merge_ctx), K(tablet_info_), K(ls_id_));
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::prepare_mds_table_merge_ctx_(
    compaction::ObTabletMergeCtx &tablet_merge_ctx)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  compaction::ObTabletMergeDagParam param;
  compaction::ObStaticMergeParam &static_param = tablet_merge_ctx.static_param_;
  ObTablet *tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, static_param.ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(tablet_handle_));
  } else {
    // init tablet merge dag param
    static_param.dag_param_.ls_id_ = ls_id_;
    static_param.dag_param_.merge_type_ = compaction::ObMergeType::MDS_MINI_MERGE;
    static_param.report_ = nullptr;
    static_param.dag_param_.tablet_id_ = tablet_info_.tablet_id_;
    // init version range and sstable
    tablet_merge_ctx.tablet_handle_ = tablet_handle_;
    static_param.scn_range_.start_scn_ = tablet->get_mds_checkpoint_scn();
    static_param.scn_range_.end_scn_ = backfill_tx_ctx_->backfill_scn_;
    static_param.version_range_.snapshot_version_ = backfill_tx_ctx_->backfill_scn_.get_val_for_tx();
    static_param.version_range_.multi_version_start_ = tablet_handle_.get_obj()->get_multi_version_start();
    static_param.merge_scn_ = backfill_tx_ctx_->backfill_scn_;
    static_param.create_snapshot_version_ = 0;
    static_param.need_parallel_minor_merge_ = false;

    if (OB_FAIL(tablet_merge_ctx.init_tablet_merge_info(false/*need_check*/))) {
      LOG_WARN("failed to init tablet merge info", K(ret), K(ls_id_), K(tablet_info_), K(tablet_merge_ctx));
    }
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::build_mds_table_to_sstable_(
    compaction::ObTabletMergeCtx &tablet_merge_ctx,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  table_handle.reset();
  const int64_t mds_construct_sequence = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else {
    ObCrossLSMdsMiniMergeOperator op(backfill_tx_ctx_->backfill_scn_);
    SMART_VAR(ObMdsTableMiniMerger, mds_mini_merger) {
      if (OB_FAIL(mds_mini_merger.init(tablet_merge_ctx, op))) {
        LOG_WARN("fail to init mds mini merger", K(ret), K(tablet_merge_ctx), K(ls_id_), K(tablet_info_));
      } else if (OB_FAIL(tablet_merge_ctx.get_tablet()->scan_mds_table_with_op(mds_construct_sequence, op))) {
        LOG_WARN("fail to scan mds table with op", K(ret), K(tablet_merge_ctx), K(ls_id_), K(tablet_info_));
      } else if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(allocator_, table_handle))) {
        LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
      }
    }
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::prepare_backfill_mds_sstables_(
    const ObTableHandleV2 &mds_sstable,
    common::ObIArray<ObTableHandleV2> &mds_sstable_array)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> wrapper;
  const ObTabletTableStore *table_store = nullptr;
  ObTableStoreIterator table_store_iter;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(tablet_handle_));
  } else if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
    LOG_WARN("fetch table store fail", K(ret), KP(tablet));
  } else if (OB_ISNULL(table_store = wrapper.get_member())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet table store should not be NULL", K(ret), K(ls_id_), K(tablet_info_), KPC(tablet));
  } else if (OB_FAIL(table_store->get_mds_sstables(table_store_iter))) {
    LOG_WARN("failed to get mds sstables", K(ret), K(ls_id_), K(tablet_info_), KPC(tablet));
  } else {
    ObTableHandleV2 table_handle;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_store_iter.get_next(table_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get mds table", K(ret));
        }
      } else if (OB_UNLIKELY(!table_handle.is_valid() || !table_handle.get_table()->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), K(table_handle));
      } else if (OB_FAIL(mds_sstable_array.push_back(table_handle))) {
        LOG_WARN("failed to push mds sstable into array", K(ret), KPC(tablet));
      }
    }
    if (OB_SUCC(ret)) {
      if (mds_sstable.is_valid() && OB_FAIL(mds_sstable_array.push_back(mds_sstable))) {
        LOG_WARN("failed to push mds sstable into array", K(ret), K(mds_sstable));
      }
    }
  }
  return ret;
}

//mds sstable means the mds data in sstable
int ObTabletMdsTableBackfillTXTask::do_backfill_mds_sstables_(
    const common::ObIArray<ObTableHandleV2> &mds_sstable_array,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds sstable backfill tx task do not init", K(ret));
  } else {
    SMART_VARS_2((compaction::ObTabletMergeDagParam, param), (compaction::ObTabletCrossLSMdsMinorMergeCtx, tablet_merge_ctx, param, allocator_)) {
      if (OB_FAIL(prepare_mds_sstable_merge_ctx_(mds_sstable_array, tablet_merge_ctx))) {
        LOG_WARN("failed to prepare mds sstable merge ctx", K(ret), K(ls_id_), K(tablet_info_));
      } else if (OB_FAIL(build_mds_sstable_(tablet_merge_ctx, table_handle))) {
        LOG_WARN("failed to build mds sstable", K(ret), K(ls_id_), K(tablet_info_), K(tablet_merge_ctx));
      }
    }
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::prepare_mds_sstable_merge_ctx_(
    const common::ObIArray<ObTableHandleV2> &mds_sstable_array,
    ObTabletCrossLSMdsMinorMergeCtx &tablet_merge_ctx)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  compaction::ObStaticMergeParam &static_param = tablet_merge_ctx.static_param_;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (mds_sstable_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare mds sstable merge ctx get invalid argument", K(ret), K(mds_sstable_array));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, static_param.ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id_));
  } else {
    // init tablet merge dag param
    static_param.dag_param_.ls_id_ = ls_id_;
    static_param.dag_param_.merge_type_ = compaction::ObMergeType::MDS_MINOR_MERGE;
    static_param.report_ = nullptr;
    static_param.dag_param_.tablet_id_ = tablet_info_.tablet_id_;
    // init version range and sstable
    tablet_merge_ctx.tablet_handle_ = tablet_handle_;
    static_param.scn_range_.start_scn_ = mds_sstable_array.at(0).get_table()->get_start_scn();
    static_param.scn_range_.end_scn_ = backfill_tx_ctx_->backfill_scn_;
    static_param.version_range_.snapshot_version_ = backfill_tx_ctx_->backfill_scn_.get_val_for_tx();
    static_param.version_range_.multi_version_start_ = tablet_handle_.get_obj()->get_multi_version_start();
    static_param.merge_scn_ = backfill_tx_ctx_->backfill_scn_;
    static_param.create_snapshot_version_ = 0;
    static_param.need_parallel_minor_merge_ = false;
    if (OB_FAIL(tablet_merge_ctx.prepare_merge_tables(mds_sstable_array))) {
      LOG_WARN("failed to prepare merge tables", K(ret), K(mds_sstable_array));
    } else if (OB_FAIL(tablet_merge_ctx.prepare_schema())) {
      LOG_WARN("failed to prepare schema", K(ret), K(tablet_merge_ctx));
    } else if (OB_FAIL(tablet_merge_ctx.build_ctx_after_init())) {
      LOG_WARN("fail to build ctx after init", K(ret), K(tablet_merge_ctx));
    } else if (1 != tablet_merge_ctx.parallel_merge_ctx_.get_concurrent_cnt()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parallel merge concurrent cnt should be 1", K(ret), K(tablet_merge_ctx));
    }
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::build_mds_sstable_(
    compaction::ObTabletCrossLSMdsMinorMergeCtx &tablet_merge_ctx,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  const int64_t mds_construct_sequence = 0;
  ObTablet *tablet = nullptr;
  table_handle.reset();
  void *buf = nullptr;
  compaction::ObPartitionMinorMerger *merger = nullptr;
  const int64_t idx = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle_), K(tablet_info_));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(compaction::ObPartitionMinorMerger)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory for minor merger", K(ret));
  } else {
    merger = new (buf) compaction::ObPartitionMinorMerger(merger_arena_, tablet_merge_ctx.static_param_);
    if (OB_FAIL(merger->merge_partition(tablet_merge_ctx, idx))) {
      LOG_WARN("failed to merge partition", K(ret), K(tablet_merge_ctx));
    } else if (OB_FAIL(update_merge_sstable_(tablet_merge_ctx))) {
      LOG_WARN("failed to update merge sstable", K(ret), K(tablet_merge_ctx), K(ls_id_), K(tablet_info_));
    }

    if (OB_NOT_NULL(merger)) {
      merger->~ObPartitionMinorMerger();
      merger = nullptr;
    }
  }
  return ret;
}

int ObTabletMdsTableBackfillTXTask::update_merge_sstable_(
    compaction::ObTabletCrossLSMdsMinorMergeCtx &tablet_merge_ctx)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  bool skip_to_create_empty_cg = false; // placeholder
  compaction::ObStaticMergeParam &static_param = tablet_merge_ctx.static_param_;
  int64_t transfer_seq = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int64_t rebuild_seq = 0;
  ObTabletCreateDeleteMdsUserData user_data;
  bool is_committed = false;
  SCN transfer_start_scn;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet mds sstable table backfill tx task do not init", K(ret));
  } else if (OB_ISNULL(ls = static_param.ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(ls_id_));
  } else if (FALSE_IT(rebuild_seq = ls->get_ls_meta().get_rebuild_seq())) {
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_info_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(ls_id_), K(tablet_info_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id_), K(tablet_info_), K(tablet_handle));
  } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
    LOG_WARN("failed to get latest tablet status", K(ret), KP(tablet));
  } else if (FALSE_IT(transfer_start_scn = user_data.transfer_scn_)) {
  } else if (FALSE_IT(transfer_seq = tablet->get_tablet_meta().transfer_info_.transfer_seq_)) {
  } else if (!transfer_start_scn.is_valid()) {
    if (tablet_info_.is_committed_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("transfer transaction is committed but transfer start scn is invalid, unexpected", K(ret), K(tablet_info_), K(user_data));
    } else {
      ret = OB_EAGAIN;
      LOG_WARN("transfer start scn is invalid, may transfer transaction rollback, need retry", K(ret), K(tablet_info_), K(user_data));
    }
  } else if (OB_FAIL(tablet_merge_ctx.merge_info_.create_sstable(
      tablet_merge_ctx,
      tablet_merge_ctx.merged_table_handle_,
      skip_to_create_empty_cg))) {
    LOG_WARN("fail to create sstable", K(ret), K(tablet_merge_ctx));
  } else if (OB_FAIL(tablets_table_mgr_->add_sstable(
      tablet_info_.tablet_id_, rebuild_seq, transfer_start_scn, transfer_seq, tablet_merge_ctx.merged_table_handle_))) {
    LOG_WARN("failed to add sstable", K(ret), K(ls_id_), K(tablet_info_), K(tablet_merge_ctx));
  }
  return ret;
}

/******************ObTabletBackfillMergeCtx*********************/
ObTabletBackfillMergeCtx::ObTabletBackfillMergeCtx(
    ObTabletMergeDagParam &param,
    common::ObArenaAllocator &allocator)
  : ObTabletMergeCtx(param, allocator),
    is_inited_(false),
    backfill_table_handle_(),
    backfill_scn_(),
    ls_rebuild_seq_(0)

{
}

ObTabletBackfillMergeCtx::~ObTabletBackfillMergeCtx()
{
}

int ObTabletBackfillMergeCtx::init(
    const SCN &backfill_scn,
    const int64_t ls_rebuild_seq,
    ObTabletHandle &tablet_handle,
    storage::ObTableHandleV2 &backfill_table_handle)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet backfill merge ctx already init", K(ret));
  } else if (!backfill_scn.is_valid() || !tablet_handle.is_valid() || !backfill_table_handle.is_valid() || ls_rebuild_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet backfill merge ctx get invalid argument", K(ret), K(backfill_scn), K(tablet_handle), K(backfill_table_handle), K(ls_rebuild_seq));
  } else {
    backfill_scn_ = backfill_scn;
    tablet_handle_ = tablet_handle;
    backfill_table_handle_ = backfill_table_handle;
    ls_rebuild_seq_ = ls_rebuild_seq;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletBackfillMergeCtx::get_ls_and_tablet()
{
  //tablet_handle_ is already set in init function.
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill merge ctx is not init", K(ret));
  } else {
    ObLSHandle &ls_handle = static_param_.ls_handle_;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(static_param_.get_ls_id(), ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get log stream", K(ret), K(static_param_.get_ls_id()));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls should not be NULL", K(ret), K(static_param_), K(ls_handle));
    } else if (ls->is_offline()) {
      ret = OB_CANCELED;
      LOG_INFO("ls offline, skip merge", K(ret), "param", get_dag_param());
    } else if (ls_rebuild_seq_ != ls->get_rebuild_seq()) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("tablet backfill merge ctx get different rebuild seq", K(static_param_), K(ls_rebuild_seq_), KPC(ls));
    } else {
      static_param_.ls_rebuild_seq_ = ls_rebuild_seq_;
    }
  }
  return ret;
}

int ObTabletBackfillMergeCtx::get_merge_tables(ObGetMergeTablesResult &get_merge_table_result)
{
  int ret = OB_SUCCESS;
  get_merge_table_result.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill merge ctx do not init", K(ret));
  } else if (OB_FAIL(get_merge_table_result.handle_.add_table(backfill_table_handle_))) {
    LOG_WARN("failed to add table into merge table result", K(ret), K(backfill_table_handle_));
  } else {
    if (backfill_table_handle_.get_table()->is_memtable()) {
      memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(backfill_table_handle_.get_table());
      if (!memtable->is_frozen_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("memtable should be frozen memtable", K(ret), KPC(memtable));
      }
    }

    if (OB_SUCC(ret)) {
      get_merge_table_result.version_range_.base_version_ = 0;
      get_merge_table_result.version_range_.multi_version_start_ = get_tablet()->get_multi_version_start();
      get_merge_table_result.version_range_.snapshot_version_ = get_tablet()->get_snapshot_version();
      // Here using a smaller snapshot version make snapshot version semantics is right
      // The freeze_snapshot_version represents that
      // all tx of the previous version has been committed.
      get_merge_table_result.scn_range_ = backfill_table_handle_.get_table()->get_key().scn_range_;
      get_merge_table_result.merge_version_ = ObVersionRange::MIN_VERSION;
      get_merge_table_result.is_backfill_ = true;
      get_merge_table_result.backfill_scn_ = backfill_scn_;
      get_merge_table_result.error_location_= nullptr;
      get_merge_table_result.schedule_major_ = false;
      get_merge_table_result.update_tablet_directly_ = false; //only for mini, local sstable version range already contains mini will update tablet directly
      get_merge_table_result.is_simplified_ = false;  //only for minor, is_simplified_ will not hold sstable handle, only using table key
      //snapshot_info is only for calculate multi version start, backfill will not change mulit version start
      get_merge_table_result.snapshot_info_.snapshot_type_ = ObStorageSnapshotInfo::SNAPSHOT_MULTI_VERSION_START_ON_TABLET;
      get_merge_table_result.snapshot_info_.snapshot_ = get_tablet()->get_multi_version_start();
    }
  }
  return ret;
}

int ObTabletBackfillMergeCtx::prepare_schema()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet backfill merge ctx do not init", K(ret));
  } else if (OB_FAIL(get_storage_schema())) {
    LOG_WARN("failed to get storage schema from tablet", KR(ret));
  } else if (is_mini_merge(static_param_.dag_param_.merge_type_) && OB_FAIL(update_storage_schema_by_memtable(
      *static_param_.schema_, static_param_.tables_handle_))) {
    LOG_WARN("failed to update storage schema by memtable", KR(ret));
  }
  return ret;
}


}
}

