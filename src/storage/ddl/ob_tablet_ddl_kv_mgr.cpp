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

#include "ob_tablet_ddl_kv_mgr.h"
#include "share/ob_force_print_log.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/blocksstable/ob_sstable_sec_meta_iterator.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::storage;

ObTabletDDLKvMgr::ObTabletDDLKvMgr()
  : is_inited_(false), success_start_log_ts_(0), ls_id_(), tablet_id_(), table_key_(), cluster_version_(0),
    start_log_ts_(0), max_freeze_log_ts_(0),
    table_id_(0), execution_id_(0), head_(0), tail_(0), lock_(), ref_cnt_(0)
{
  MEMSET(ddl_kvs_, 0, MAX_DDL_KV_CNT_IN_STORAGE * sizeof(ddl_kvs_[0]));
}

ObTabletDDLKvMgr::~ObTabletDDLKvMgr()
{
  destroy();
}

void ObTabletDDLKvMgr::destroy()
{
  if (is_started()) {
    LOG_INFO("start destroy ddl kv manager", K(ls_id_), K(tablet_id_), K(start_log_ts_), K(head_), K(tail_), K(lbt()));
  }
  TCWLockGuard guard(lock_);
  ATOMIC_STORE(&ref_cnt_, 0);
  for (int64_t pos = head_; pos < tail_; ++pos) {
    const int64_t idx = get_idx(pos);
    free_ddl_kv(idx);
  }
  head_ = 0;
  tail_ = 0;
  MEMSET(ddl_kvs_, 0, sizeof(ddl_kvs_));
  ls_id_.reset();
  tablet_id_.reset();
  table_key_.reset();
  cluster_version_ = 0;
  start_log_ts_ = 0;
  max_freeze_log_ts_ = 0;
  table_id_ = 0;
  execution_id_ = 0;
  success_start_log_ts_ = 0;
  is_inited_ = false;
}

int ObTabletDDLKvMgr::init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletDDLKvMgr is already inited", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

// ddl start from log
//    cleanup ddl sstable
// ddl start from checkpoint
//    keep ddl sstable table

int ObTabletDDLKvMgr::ddl_start(const ObITable::TableKey &table_key,
                                const int64_t start_log_ts,
                                const int64_t cluster_version,
                                const int64_t execution_id,
                                const int64_t checkpoint_log_ts)
{
  int ret = OB_SUCCESS;
  bool is_brand_new = false;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!table_key.is_valid() || start_log_ts <= 0 || execution_id < 0 || cluster_version < 0
        || (checkpoint_log_ts > 0 && checkpoint_log_ts < start_log_ts))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(start_log_ts), K(execution_id), K(cluster_version), K(checkpoint_log_ts));
  } else if (table_key.get_tablet_id() != tablet_id_) {
    ret = OB_ERR_SYS;
    LOG_WARN("tablet id not same", K(ret), K(table_key), K(tablet_id_));
  } else if (0 != start_log_ts_) {
    if (execution_id >= execution_id_ && start_log_ts >= start_log_ts_) {
      LOG_INFO("execution id changed, need cleanup", K(ls_id_), K(tablet_id_), K(execution_id_), K(execution_id), K(start_log_ts_), K(start_log_ts));
      cleanup_unlock();
      is_brand_new = true;
    } else {
      if (checkpoint_log_ts <= 0) {
        // only return error code when not start from checkpoint.
        ret = OB_TASK_EXPIRED;
      }
      LOG_INFO("ddl start ignored", K(ls_id_), K(tablet_id_), K(execution_id_), K(execution_id), K(start_log_ts_), K(start_log_ts));
    }
  } else {
    is_brand_new = true;
  }
  if (OB_SUCC(ret) && is_brand_new) {
    table_key_ = table_key;
    cluster_version_ = cluster_version;
    execution_id_ = execution_id;
    start_log_ts_ = start_log_ts;
    max_freeze_log_ts_ = max(start_log_ts, checkpoint_log_ts);
  }
  if (OB_SUCC(ret) && checkpoint_log_ts <= 0) {
    // remove ddl sstable if exists and flush ddl start log ts and snapshot version into tablet meta
    if (OB_FAIL(update_tablet(start_log_ts_, table_key_.get_snapshot_version(), start_log_ts_))) {
      LOG_WARN("clean up ddl sstable failed", K(ret), K(ls_id_), K(tablet_id_));
    }
  }
  FLOG_INFO("start ddl kv mgr finished", K(ret), K(is_brand_new), K(start_log_ts), K(execution_id), K(checkpoint_log_ts), K(*this));
  return ret;
}

int ObTabletDDLKvMgr::ddl_prepare(const int64_t start_log_ts,
                                  const int64_t prepare_log_ts,
                                  const uint64_t table_id,
                                  const int64_t ddl_task_id)
{
  int ret = OB_SUCCESS;
  ObDDLKVHandle kv_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else if (start_log_ts < start_log_ts_) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("skip ddl prepare log", K(start_log_ts), K(*this));
  } else if (OB_FAIL(freeze_ddl_kv(prepare_log_ts))) {
    LOG_WARN("freeze ddl kv failed", K(ret), K(prepare_log_ts));
  } else {
    table_id_ = table_id;
    ddl_task_id_ = ddl_task_id;

    ObDDLTableMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.rec_log_ts_ = prepare_log_ts;
    param.is_commit_ = true;
    param.start_log_ts_ = start_log_ts;
    param.table_id_ = table_id;
    param.execution_id_ = execution_id_;
    param.ddl_task_id_ = ddl_task_id_;
    const int64_t start_ts = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret) && is_started()) {
      if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
        } else {
          ret = OB_SUCCESS;
          ob_usleep(10L * 1000L);
          if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
            LOG_INFO("retry schedule ddl commit task",
                K(start_log_ts), K(prepare_log_ts), K(table_id), K(ddl_task_id), K(*this),
                "wait_elpased_s", (ObTimeUtility::fast_current_time() - start_ts) / 1000000L);
          }
        }
      } else {
        LOG_INFO("schedule ddl commit task success", K(start_log_ts), K(prepare_log_ts), K(table_id), K(ddl_task_id), K(*this));
        break;
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::ddl_commit(const int64_t start_log_ts, const int64_t prepare_log_ts, const bool is_replay)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (is_commit_success()) {
    FLOG_INFO("ddl commit already succeed", K(start_log_ts), K(prepare_log_ts), K(*this));
  } else if (start_log_ts < start_log_ts_) {
    ret = OB_TASK_EXPIRED;
    LOG_INFO("skip ddl commit log", K(start_log_ts), K(prepare_log_ts), K(*this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else {
    ObDDLTableMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.rec_log_ts_ = prepare_log_ts;
    param.is_commit_ = true;
    param.start_log_ts_ = start_log_ts;
    param.table_id_ = table_id_;
    param.execution_id_ = execution_id_;
    param.ddl_task_id_ = ddl_task_id_;
    // retry submit dag in case of the previous dag failed
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW == ret || OB_EAGAIN == ret) {
        ret = OB_EAGAIN;
      } else {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    } else {
      ret = OB_EAGAIN; // until major sstable is ready
    }
  }
  if (OB_FAIL(ret) && is_replay)  {
    if (OB_TABLET_NOT_EXIST == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS; // think as succcess for replay
    } else {
      if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
        LOG_INFO("replay ddl commit", K(ret), K(start_log_ts), K(prepare_log_ts), K(*this));
      }
      ret = OB_EAGAIN; // retry by replay service
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::wait_ddl_commit(const int64_t start_log_ts, const int64_t prepare_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(start_log_ts <= 0 || prepare_log_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_log_ts), K(prepare_log_ts));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else if (start_log_ts > start_log_ts_) {
    ret = OB_ERR_SYS;
    LOG_WARN("start log ts not match", K(ret), K(start_log_ts), K(start_log_ts_), K(ls_id_), K(tablet_id_));
  } else {
    const int64_t wait_start_ts = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ddl_commit(start_log_ts, prepare_log_ts, false/*is_replay*/))) {
        if (OB_EAGAIN == ret) {
          ob_usleep(10L * 1000L);
          ret = OB_SUCCESS; // retry
        } else {
          LOG_WARN("commit ddl log failed", K(ret), K(start_log_ts), K(prepare_log_ts), K(ls_id_), K(tablet_id_));
        }
      } else {
        break;
      }
      if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) {
        LOG_INFO("wait build ddl sstable", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_), K(prepare_log_ts), K(max_freeze_log_ts_),
            "wait_elpased_s", (ObTimeUtility::fast_current_time() - wait_start_ts) / 1000000L);
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::set_commit_success(const int64_t start_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(start_log_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_log_ts));
  } else {
    TCWLockGuard guard(lock_);
    if (start_log_ts < start_log_ts_) {
      ret = OB_TASK_EXPIRED;
      LOG_WARN("ddl task expired", K(ret), K(start_log_ts), K(*this));
    } else if (OB_UNLIKELY(start_log_ts > start_log_ts_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("sucess start log ts too large", K(ret), K(start_log_ts), K(*this));
    } else {
      success_start_log_ts_ = start_log_ts;
    }
  }
  return ret;
}

bool ObTabletDDLKvMgr::is_commit_success() const
{
  TCRLockGuard guard(lock_);
  return success_start_log_ts_ > 0 && success_start_log_ts_ == start_log_ts_;
}

int ObTabletDDLKvMgr::cleanup()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    TCWLockGuard guard(lock_);
    cleanup_unlock();
  }
  return ret;
}

void ObTabletDDLKvMgr::cleanup_unlock()
{
  LOG_INFO("cleanup ddl kv mgr", K(*this));
  for (int64_t pos = head_; pos < tail_; ++pos) {
    const int64_t idx = get_idx(pos);
    free_ddl_kv(idx);
  }
  head_ = 0;
  tail_ = 0;
  MEMSET(ddl_kvs_, 0, sizeof(ddl_kvs_));
  table_key_.reset();
  cluster_version_ = 0;
  start_log_ts_ = 0;
  max_freeze_log_ts_ = 0;
  table_id_ = 0;
  execution_id_ = 0;
  success_start_log_ts_ = 0;
}

bool ObTabletDDLKvMgr::is_execution_id_older(const int64_t execution_id)
{
  TCRLockGuard guard(lock_);
  return execution_id < execution_id_;
}

int ObTabletDDLKvMgr::online()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    const ObTabletMeta &tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
    ObITable::TableKey table_key;
    table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    table_key.tablet_id_ = tablet_meta.tablet_id_;
    table_key.version_range_.base_version_ = 0;
    table_key.version_range_.snapshot_version_ = tablet_meta.ddl_snapshot_version_;
    const int64_t start_log_ts = tablet_meta.ddl_start_log_ts_;
    if (OB_FAIL(ddl_start(table_key,
                          start_log_ts,
                          tablet_meta.ddl_cluster_version_,
                          tablet_meta.ddl_execution_id_,
                          tablet_meta.ddl_checkpoint_ts_))) {
      if (OB_TASK_EXPIRED == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("start ddl kv manager failed", K(ret), K(tablet_meta));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::register_to_tablet(const int64_t ddl_start_log_ts, ObDDLKvMgrHandle &kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(ddl_start_log_ts <= 0 || kv_mgr_handle.get_obj() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_start_log_ts), KP(kv_mgr_handle.get_obj()), KP(this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    TCWLockGuard guard(lock_);
    if (ddl_start_log_ts < start_log_ts_) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl task expired", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_), K(ddl_start_log_ts));
    } else if (ddl_start_log_ts > start_log_ts_) {
      if (0 == start_log_ts_) {
        // maybe ls offline
        ret = OB_EAGAIN;
      } else {
        ret = OB_ERR_SYS;
      }
      LOG_WARN("ddl kv mgr register before start", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_), K(ddl_start_log_ts));
    } else {
      if (OB_FAIL(tablet_handle.get_obj()->set_ddl_kv_mgr(kv_mgr_handle))) {
        LOG_WARN("set ddl kv mgr into tablet failed", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::unregister_from_tablet(const int64_t ddl_start_log_ts, ObDDLKvMgrHandle &kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(ddl_start_log_ts <= 0 || kv_mgr_handle.get_obj() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_start_log_ts), KP(kv_mgr_handle.get_obj()), KP(this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_,
                                                    tablet_handle,
                                                    ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    TCWLockGuard guard(lock_);
    if (ddl_start_log_ts < start_log_ts_) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl task expired", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_), K(ddl_start_log_ts));
    } else if (ddl_start_log_ts > start_log_ts_) {
      if (0 == start_log_ts_) {
        // maybe ls offline
        ret = OB_EAGAIN;
      } else {
        ret = OB_ERR_SYS;
      }
      LOG_WARN("ddl kv mgr register before start", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_), K(ddl_start_log_ts));
    } else {
      if (OB_FAIL(tablet_handle.get_obj()->remove_ddl_kv_mgr(kv_mgr_handle))) {
        LOG_WARN("set ddl kv mgr into tablet failed", K(ret), K(ls_id_), K(tablet_id_), K(start_log_ts_));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::update_tablet(const int64_t start_log_ts, const int64_t snapshot_version, const int64_t ddl_checkpoint_ts)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(start_log_ts <= 0 || snapshot_version <= 0 || ddl_checkpoint_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_log_ts), K(snapshot_version), K(ddl_checkpoint_ts));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id_), K(tablet_id_));
  } else {
    ObTableHandleV2 table_handle; // empty
    const int64_t rebuild_seq = ls_handle.get_ls()->get_rebuild_seq();
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(tablet_handle.get_obj()->get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION, // multi_version_start
                                  &tablet_handle.get_obj()->get_storage_schema(),
                                  rebuild_seq);
    param.keep_old_ddl_sstable_ = false;
    param.ddl_start_log_ts_ = start_log_ts;
    param.ddl_snapshot_version_ = snapshot_version;
    param.ddl_checkpoint_ts_ = ddl_checkpoint_ts;
    param.ddl_execution_id_ = execution_id_;
    param.ddl_cluster_version_ = cluster_version_;
    if (OB_FAIL(ls_handle.get_ls()->update_tablet_table_store(tablet_id_, param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", K(ret), K(ls_id_), K(tablet_id_), K(param));
    } else {
      LOG_INFO("update tablet success", K(ls_id_), K(tablet_id_), K(param), K(start_log_ts), K(snapshot_version), K(ddl_checkpoint_ts));
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_param(ObTabletDDLParam &ddl_param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (!is_started()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl not started", K(ret));
  } else {
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id_;
    ddl_param.table_key_ = table_key_;
    ddl_param.start_log_ts_ = start_log_ts_;
    ddl_param.snapshot_version_ = table_key_.get_snapshot_version();
    ddl_param.cluster_version_ = cluster_version_;
  }

  return ret;
}

int ObTabletDDLKvMgr::get_freezed_ddl_kv(const int64_t freeze_log_ts, ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    bool found = false;
    TCRLockGuard guard(lock_);
    for (int64_t i = head_; OB_SUCC(ret) && !found && i < tail_; ++i) {
      const int64_t idx = get_idx(i);
      ObDDLKV *cur_kv = ddl_kvs_[idx];
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(cur_kv), K(i), K(head_), K(tail_));
      } else if (freeze_log_ts == cur_kv->get_freeze_log_ts()) {
        found = true;
        if (OB_FAIL(kv_handle.set_ddl_kv(cur_kv))) {
          LOG_WARN("hold ddl kv failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("freezed ddl kv with given freeze log ts is not found", K(ret), K(freeze_log_ts));
    }
  }
  return ret;
}

int64_t ObTabletDDLKvMgr::get_count() const
{
  return tail_ - head_;
}

int64_t ObTabletDDLKvMgr::get_idx(const int64_t pos) const
{
  return pos & (MAX_DDL_KV_CNT_IN_STORAGE - 1);
}

int ObTabletDDLKvMgr::get_active_ddl_kv_impl(ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKV *kv = nullptr;
  if (get_count() == 0) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    kv = ddl_kvs_[get_idx(tail_ - 1)];
    if (nullptr == kv) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, kv must not be nullptr", K(ret));
    } else if (kv->is_freezed()) {
      kv = nullptr;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(kv_handle.set_ddl_kv(kv))) {
      LOG_WARN("fail to set ddl kv", K(ret));
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_or_create_ddl_kv(const int64_t log_ts, ObDDLKVHandle &kv_handle)
{
  int ret = OB_SUCCESS;
  kv_handle.reset();
  ObDDLKV *kv = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (log_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_ts));
  } else {
    TCRLockGuard guard(lock_);
    try_get_ddl_kv_unlock(log_ts, kv);
    if (nullptr != kv) {
      // increase or decrease the reference count must be under the lock
      if (OB_FAIL(kv_handle.set_ddl_kv(kv))) {
        LOG_WARN("fail to set ddl kv", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && nullptr == kv) {
    TCWLockGuard guard(lock_);
    try_get_ddl_kv_unlock(log_ts, kv);
    if (nullptr != kv) {
      // do nothing
    } else if (OB_FAIL(alloc_ddl_kv(kv))) {
      LOG_WARN("create ddl kv failed", K(ret));
    }
    if (OB_SUCC(ret) && nullptr != kv) {
      // increase or decrease the reference count must be under the lock
      if (OB_FAIL(kv_handle.set_ddl_kv(kv))) {
        LOG_WARN("fail to set ddl kv", K(ret));
      }
    }
  }
  return ret;
}

void ObTabletDDLKvMgr::try_get_ddl_kv_unlock(const int64_t log_ts, ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  if (get_count() > 0) {
    for (int64_t i = tail_ - 1; OB_SUCC(ret) && i >= head_ && nullptr == kv; ++i) {
      ObDDLKV *tmp_kv = ddl_kvs_[get_idx(i)];
      if (OB_ISNULL(tmp_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(tmp_kv), K(i), K(head_), K(tail_));
      } else if (log_ts <= tmp_kv->get_freeze_log_ts()) {
        kv = tmp_kv;
        break;
      }
    }
  }
}

int ObTabletDDLKvMgr::freeze_ddl_kv(const int64_t freeze_log_ts)
{
  int ret = OB_SUCCESS;
  ObDDLKVHandle kv_handle;
  ObDDLKV *kv = nullptr;
  TCWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (0 == get_count()) {
    // do nothing
  } else if (OB_FAIL(get_active_ddl_kv_impl(kv_handle))) {
    LOG_WARN("fail to get active ddl kv", K(ret));
  } else if (OB_FAIL(kv_handle.get_ddl_kv(kv))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get ddl kv", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  if (OB_SUCC(ret) && nullptr == kv && freeze_log_ts > max_freeze_log_ts_) {
    // freeze_log_ts > 0 only occured when ddl prepare
    // assure there is an alive ddl kv, for waiting pre-logs
    if (OB_FAIL(alloc_ddl_kv(kv))) {
      LOG_WARN("create ddl kv failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && nullptr != kv) {
    if (OB_FAIL(kv->freeze(freeze_log_ts))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("fail to freeze active ddl kv", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      max_freeze_log_ts_ = MAX(max_freeze_log_ts_, kv->get_freeze_log_ts());
      LOG_INFO("freeze ddl kv", "kv", *kv);
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::release_ddl_kvs(const int64_t end_log_ts)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_RELEASE_DDL_KV);
  TCWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t i = head_; OB_SUCC(ret) && i < tail_; ++i) {
      const int64_t idx = get_idx(head_);
      ObDDLKV *kv = ddl_kvs_[idx];
      LOG_INFO("try release ddl kv", K(end_log_ts), KPC(kv));
#ifdef ERRSIM
          if (OB_SUCC(ret)) {
            ret = E(EventTable::EN_DDL_RELEASE_DDL_KV_FAIL) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              LOG_WARN("errsim release ddl kv failed", KR(ret));
            }
          }
#endif
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(kv), K(i), K(head_), K(tail_));
      } else if (kv->is_closed() && kv->get_freeze_log_ts() <= end_log_ts) {
        const int64_t freeze_log_ts = kv->get_freeze_log_ts();
        free_ddl_kv(idx);
        ++head_;
        LOG_INFO("succeed to release ddl kv", K(ls_id_), K(tablet_id_), K(freeze_log_ts));
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kv_min_log_ts(int64_t &min_log_ts)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  min_log_ts = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t i = head_; OB_SUCC(ret) && i < tail_; ++i) {
      const int64_t idx = get_idx(head_);
      ObDDLKV *kv = ddl_kvs_[idx];
      if (OB_ISNULL(kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(kv), K(i), K(head_), K(tail_));
      } else {
        min_log_ts = MIN(min_log_ts, kv->get_min_log_ts());
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::get_ddl_kvs(const bool frozen_only, ObDDLKVsHandle &ddl_kvs_handle)
{
  int ret = OB_SUCCESS;
  ddl_kvs_handle.reset();
  TCRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    for (int64_t pos = head_; OB_SUCC(ret) && pos < tail_; ++pos) {
      const int64_t idx = get_idx(pos);
      ObDDLKV *cur_kv = ddl_kvs_[idx];
      if (OB_ISNULL(cur_kv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ddl kv is null", K(ret), K(ls_id_), K(tablet_id_), KP(cur_kv), K(pos), K(head_), K(tail_));
      } else if (!frozen_only || cur_kv->is_freezed()) {
        if (OB_FAIL(ddl_kvs_handle.add_ddl_kv(cur_kv))) {
          LOG_WARN("fail to push back ddl kv", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTabletDDLKvMgr::check_has_effective_ddl_kv(bool &has_ddl_kv)
{
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else {
    has_ddl_kv = 0 != get_count();
  }
  return ret;
}

int ObTabletDDLKvMgr::alloc_ddl_kv(ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv manager not init", K(ret));
  } else if (OB_UNLIKELY(!is_started())) {
    ret = OB_ERR_SYS;
    LOG_WARN("ddl kv manager not started", K(ret));
  } else if (get_count() == MAX_DDL_KV_CNT_IN_STORAGE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, too much ddl kv count", K(ret));
  } else if (OB_ISNULL(kv = op_alloc(ObDDLKV))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_FAIL(kv->init(ls_id_,
                              tablet_id_,
                              start_log_ts_,
                              table_key_.get_snapshot_version(),
                              max_freeze_log_ts_,
                              cluster_version_))) {
    LOG_WARN("fail to init ddl kv", K(ret), K(ls_id_), K(tablet_id_),
        K(start_log_ts_), K(table_key_), K(max_freeze_log_ts_), K(cluster_version_));
  } else {
    const int64_t idx = get_idx(tail_);
    tail_++;
    ddl_kvs_[idx] = kv;
    kv->inc_ref();
    FLOG_INFO("succeed to add ddl kv", K(ls_id_), K(tablet_id_), K(head_), K(tail_), "ddl_kv_cnt", get_count(), KP(kv));
  }
  if (OB_FAIL(ret) && nullptr != kv) {
    op_free(kv);
    kv = nullptr;
  }
  return ret;
}

void ObTabletDDLKvMgr::free_ddl_kv(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletDDLKvMgr is not inited", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= MAX_DDL_KV_CNT_IN_STORAGE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx));
  } else {
    ObDDLKV *kv = ddl_kvs_[idx];
    ddl_kvs_[idx] = nullptr;
    if (nullptr != kv) {
      if (0 == kv->dec_ref()) {
        op_free(kv);
        FLOG_INFO("free ddl kv", K(ls_id_), K(tablet_id_), KP(kv));
      }
    }
  }
}

