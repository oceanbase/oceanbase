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
#include "storage/ls/ob_ls_ddl_log_handler.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/ls/ob_ls.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "logservice/ob_log_base_header.h"
#include "share/scn.h"
#include "observer/ob_server_event_history_table_operator.h"

namespace oceanbase
{

using namespace blocksstable;
using namespace share;

namespace storage
{

ObActiveDDLKVMgr::ObActiveDDLKVMgr()
  : lock_(), active_ddl_tablets_()
{
}

int ObActiveDDLKVMgr::add_tablet(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id));
  } else {
    ObSpinLockGuard guard(lock_);
    if (!has_exist_in_array(active_ddl_tablets_, tablet_id)) {
      if (OB_FAIL(active_ddl_tablets_.push_back(tablet_id))) {
        LOG_WARN("push back tablet id failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    FLOG_INFO("add tablet to active ddl kv mgr", K(tablet_id));
  }
  return ret;
}

int ObActiveDDLKVMgr::del_tablets(const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tablet_ids.count() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_ids.count()));
  } else if (tablet_ids.count() == 0) {
    // do nothing
  } else {
    ObSpinLockGuard guard(lock_);
    ObArray<ObTabletID> tmp_active_tablet_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < active_ddl_tablets_.count(); ++i) {
      if (!has_exist_in_array(tablet_ids, active_ddl_tablets_.at(i))) {
        if (OB_FAIL(tmp_active_tablet_ids.push_back(active_ddl_tablets_.at(i)))) {
          LOG_WARN("push back active tablet id failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(active_ddl_tablets_.assign(tmp_active_tablet_ids))) {
        LOG_WARN("assign active ddl tablet ids failed", K(ret));
      } else {
        FLOG_INFO("del tablets from active ddl kv mgr", K_(active_ddl_tablets), K(tablet_ids));
      }
    }
  }
  return ret;
}

int ObActiveDDLKVMgr::get_tablets(common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(lock_);
  if (OB_FAIL(tablet_ids.assign(active_ddl_tablets_))) {
    LOG_WARN("assign tablet ids failed", K(ret));
  }
  return ret;
}

int ObActiveDDLKVIterator::init(ObLS *ls, ObActiveDDLKVMgr &mgr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObActiveDDLKVIterator has been inited twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ls));
  } else if (OB_FAIL(mgr.get_tablets(active_ddl_tablets_))) {
    LOG_WARN("get tablets failed", K(ret));
  } else {
    ls_ = ls;
    mgr_ = &mgr;
    idx_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObActiveDDLKVIterator::get_next_ddl_kv_mgr(ObDDLKvMgrHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObActiveDDLKVIterator has not been inited", K(ret));
  } else {
    ObTabletHandle tablet_handle;
    while (OB_SUCC(ret) && !handle.is_valid()) {
      if (idx_ >= active_ddl_tablets_.count()) {
        ret = OB_ITER_END;
      } else {
        ObTabletID &tablet_id = active_ddl_tablets_.at(idx_);
        if (OB_FAIL(ls_->get_tablet(tablet_id, tablet_handle,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            if (OB_FAIL(to_del_tablets_.push_back(tablet_id))) {
              LOG_WARN("push back to delete tablet id failed", K(ret));
            }
          } else {
            LOG_WARN("failed to get tablet", K(ret), K(ls_->get_ls_id()), K(tablet_id));
          }
        } else if (tablet_handle.get_obj()->get_tablet_meta().ddl_commit_scn_.is_valid_and_not_min() &&
          tablet_handle.get_obj()->get_tablet_meta().ddl_checkpoint_scn_ >= tablet_handle.get_obj()->get_tablet_meta().ddl_commit_scn_) {
          if (OB_FAIL(to_del_tablets_.push_back(tablet_id))) {
            LOG_WARN("push back to deleted tablet failed", K(ret));
          }
        } else if (tablet_handle.get_obj()->get_major_table_count() > 0
            || tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable()) {
          if (OB_FAIL(to_del_tablets_.push_back(tablet_id))) {
            LOG_WARN("push back to deleted tablet failed", K(ret));
          }
        } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(handle))) {
          LOG_WARN("get ddl kv mgr failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ++idx_;
      }
    }
  }
  if (OB_ITER_END == ret) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(mgr_->del_tablets(to_del_tablets_))) {
      LOG_WARN("del tablets failed", K(tmp_ret));
    }
  }
  return ret;
}

int ObLSDDLLogHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSDDLLogHandler init twice", K(ret));
  } else if (nullptr == ls) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(ddl_log_replayer_.init(ls))) {
    LOG_WARN("fail to init ddl log replayer", K(ret));
  } else {
    TCWLockGuard guard(online_lock_);
    is_online_ = false;
    ls_ = ls;
    last_rec_scn_ = ls_->get_clog_checkpoint_scn();
    is_inited_ = true;
  }
  return ret;
}

void ObLSDDLLogHandler::reset()
{
  TCWLockGuard guard(online_lock_);
  is_online_ = false;
  is_inited_ = false;
  ls_ = nullptr;
  last_rec_scn_.reset();
  ddl_log_replayer_.reset();
}

int ObLSDDLLogHandler::offline()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl log handler not init", K(ret));
  } else {
    TCWLockGuard guard(online_lock_);
    is_online_ = false;
  }

  add_ddl_event(ret, "ddl log hanlder offline");
  FLOG_INFO("ddl log hanlder offline", K(ret), "ls_meta", ls_->get_ls_meta(), "ddl_event_info", ObDDLEventInfo());
  return OB_SUCCESS;
}

int ObLSDDLLogHandler::online()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl log handler not init", K(ret));
  } else {
    ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
    if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
      LOG_WARN("failed to build ls tablet iter", K(ret), K(ls_));
    } else {
      while (OB_SUCC(ret)) {
        ObTabletHandle tablet_handle;
        ObDDLKvMgrHandle ddl_kv_mgr_handle;
        if (OB_FAIL(tablet_iter.get_next_ddl_kv_mgr(ddl_kv_mgr_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next ddl kv mgr", K(ret));
          }
        } else if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet handle", K(ret), K(ddl_kv_mgr_handle));
        } else if (OB_FAIL(ls_->get_tablet(ddl_kv_mgr_handle.get_obj()->get_tablet_id(), tablet_handle,
            ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          LOG_WARN("get tablet handle failed", K(ret), KPC(ddl_kv_mgr_handle.get_obj()));
        } else if (OB_FAIL(tablet_handle.get_obj()->start_direct_load_task_if_need())) {
          LOG_WARN("start ddl if need failed", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    TCWLockGuard guard(online_lock_);
    is_online_ = true;
  }
  add_ddl_event(ret, "ddl log hanlder online");
  FLOG_INFO("ddl log hanlder online", K(ret), "ls_meta", ls_->get_ls_meta(), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

int ObLSDDLLogHandler::replay(const void *buffer,
                              const int64_t buf_size,
                              const palf::LSN &lsn,
                              const SCN &log_scn)
{
  int ret = OB_SUCCESS;
  logservice::ObLogBaseHeader base_header;
  ObDDLClogHeader ddl_header;
  int64_t tmp_pos = 0;
  const char *log_buf = static_cast<const char *>(buffer);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLSDDLLogHandler not inited", K(ret));
  } else if (OB_FAIL(base_header.deserialize(log_buf, buf_size, tmp_pos))) {
    LOG_WARN("log base header deserialize error", K(ret));
  } else if (OB_FAIL(ddl_header.deserialize(log_buf, buf_size, tmp_pos))) {
    LOG_WARN("fail to deserialize ddl header", K(ret));
  } else {
    TCRLockGuard guard(online_lock_);
    if (!is_online_) {
      LOG_ERROR("ddl log handler is offline, skip replay", "ls_meta", ls_->get_ls_meta(), K(base_header), K(ddl_header));
    } else {
      switch (ddl_header.get_ddl_clog_type()) {
      case ObDDLClogType::DDL_REDO_LOG: {
        ret = replay_ddl_redo_log_(log_buf, buf_size, tmp_pos, log_scn);
        break;
      }
      case ObDDLClogType::OLD_DDL_COMMIT_LOG: {
        break; // ignore the old ddl commit log
      }
      case ObDDLClogType::DDL_COMMIT_LOG: {
        ret = replay_ddl_commit_log_(log_buf, buf_size, tmp_pos, log_scn);
        break;
      }
      case ObDDLClogType::DDL_TABLET_SCHEMA_VERSION_CHANGE_LOG: {
        ret = replay_ddl_tablet_schema_version_change_log_(log_buf, buf_size, tmp_pos, log_scn);
        break;
      }
      case ObDDLClogType::DDL_START_LOG: {
        ret = replay_ddl_start_log_(log_buf, buf_size, tmp_pos, log_scn);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("Unknown ddl log type", K(ddl_header.get_ddl_clog_type()), K(ret));
      }
      }
      if (OB_FAIL(ret)) {
        if (OB_TABLET_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("tablet not exist when replaying ddl log", "type", ddl_header.get_ddl_clog_type());
        } else if (OB_EAGAIN == ret) {
          // retry replay again
        } else {
          LOG_WARN("failed to replay ddl log", K(ret), "type", ddl_header.get_ddl_clog_type());
          ret = OB_EAGAIN;
        }
      }
    }
  }
  return ret;
}

void ObLSDDLLogHandler::switch_to_follower_forcedly()
{
  // TODO
}

int ObLSDDLLogHandler::switch_to_leader()
{
  int ret = OB_SUCCESS;

  //TODO

  return ret;
}

int ObLSDDLLogHandler::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;

  //TODO

  return ret;
}

int ObLSDDLLogHandler::resume_leader()
{
  int ret = OB_SUCCESS;

  //TODO

  return ret;
}

int ObLSDDLLogHandler::flush(SCN &rec_scn)
{
  int ret = OB_SUCCESS;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls_));
  } else {
    TCRLockGuard guard(online_lock_);
    if (!is_online_) {
      LOG_INFO("ddl log handler is offline, no need to flush", K(ret), "ls_meta", ls_->get_ls_meta());
    } else if (OB_ISNULL(tenant_direct_load_mgr)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys", K(ret), K(MTL_ID()));
    } else {
      while (OB_SUCC(ret)) {
        bool has_ddl_kv = false;
        ObDDLKvMgrHandle ddl_kv_mgr_handle;
        ObTabletDirectLoadMgrHandle direct_load_mgr_hdl;
        bool is_major_sstable_exist = false;
        if (OB_FAIL(tablet_iter.get_next_ddl_kv_mgr(ddl_kv_mgr_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get ddl kv mgr", K(ret), K(ddl_kv_mgr_handle));
          }
        } else if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ddl kv mgr handle", K(ret), K(ddl_kv_mgr_handle));
        } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->check_has_effective_ddl_kv(has_ddl_kv))) {
          LOG_WARN("failed to check ddl kv", K(ret));
        } else if (!has_ddl_kv) {
        } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
                ls_->get_ls_id(),
                ddl_kv_mgr_handle.get_obj()->get_tablet_id(),
                true/* is_full_direct_load */,
                direct_load_mgr_hdl,
                is_major_sstable_exist))) {
          if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
            LOG_WARN("major sstable already exist, ddl kv may leak", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
          } else {
            LOG_WARN("get tablet direct load mgr failed", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id(), K(is_major_sstable_exist));
          }
        } else {
          DEBUG_SYNC(BEFORE_DDL_CHECKPOINT);
          ObDDLTableMergeDagParam param;
          param.ls_id_               = ls_->get_ls_id();
          param.tablet_id_           = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
          param.start_scn_           = direct_load_mgr_hdl.get_full_obj()->get_start_scn();
          param.rec_scn_             = rec_scn;
          param.direct_load_type_    = direct_load_mgr_hdl.get_full_obj()->get_direct_load_type();
          param.is_commit_           = false;
          param.data_format_version_ = direct_load_mgr_hdl.get_full_obj()->get_data_format_version();
          param.snapshot_version_    = direct_load_mgr_hdl.get_full_obj()->get_table_key().get_snapshot_version();
          LOG_INFO("schedule ddl merge dag", K(param));
          if (OB_FAIL(ObTabletDDLUtil::freeze_ddl_kv(param))) {
            LOG_WARN("try to freeze ddl kv failed", K(ret), K(param));
          } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
            LOG_WARN("try schedule ddl merge dag failed when ddl kv is full ", K(ret), K(param));
          }
        }
      }
      (void)tenant_direct_load_mgr->gc_tablet_direct_load();
    }
  }
  return OB_SUCCESS;
}

SCN ObLSDDLLogHandler::get_rec_scn()
{
  int ret = OB_SUCCESS;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  SCN rec_scn = SCN::max_scn();
  bool has_ddl_kv = false;
  ObActiveDDLKVIterator active_ddl_kv_mgr_iter;
  ObTabletID barrier_tablet_id;
  if (OB_FAIL(active_ddl_kv_mgr_iter.init(ls_, active_ddl_kv_mgr_))) {
    LOG_WARN("initialize active ddl kv mgr iterator failed", K(ret));
  } else {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    while (OB_SUCC(ret)) {
      SCN last_scn = rec_scn;
      if (OB_FAIL(active_ddl_kv_mgr_iter.get_next_ddl_kv_mgr(ddl_kv_mgr_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next ddl kv mgr failed", K(ret));
        }
      } else if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ddl kv mgr handle", K(ret), K(ddl_kv_mgr_handle));
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_rec_scn(rec_scn))) {
        LOG_WARN("get rec scn failed", K(ret));
      } else if (rec_scn < last_scn) {
        barrier_tablet_id = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
      }
    }
  }
  if (OB_FAIL(ret)) {
    rec_scn = SCN::max(last_rec_scn_, ls_->get_clog_checkpoint_scn());
  } else if (!rec_scn.is_max()) {
    last_rec_scn_ = SCN::max(last_rec_scn_, rec_scn);
  }

  // gc tablet direct load periodically
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  if (OB_NOT_NULL(tenant_direct_load_mgr)) {
    (void)tenant_direct_load_mgr->gc_tablet_direct_load();
  }

  LOG_INFO("[CHECKPOINT] ObLSDDLLogHandler::get_rec_scn", K(ret),
      "ls_id", OB_ISNULL(ls_) ? ObLSID() : ls_->get_ls_id(),
      K(barrier_tablet_id), K(rec_scn), K_(last_rec_scn));
  return rec_scn;
}

int ObLSDDLLogHandler::replay_ddl_redo_log_(const char *log_buf,
                                            const int64_t buf_size,
                                            int64_t pos,
                                            const SCN &log_scn)
{
  int ret = OB_SUCCESS;
  ObDDLRedoLog log;
  if (OB_FAIL(log.deserialize(log_buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ddl redo log", K(ret));
  } else if (OB_FAIL(ddl_log_replayer_.replay_redo(log, log_scn))) {
    if (OB_TABLET_NOT_EXIST != ret && OB_EAGAIN != ret) {
      LOG_WARN("fail to replay ddl redo log", K(ret), K(log));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObLSDDLLogHandler::replay_ddl_commit_log_(const char *log_buf,
                                               const int64_t buf_size,
                                               int64_t pos,
                                               const SCN &log_scn)
{
  int ret = OB_SUCCESS;
  ObDDLCommitLog log;
  if (OB_FAIL(log.deserialize(log_buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ddl commit log", K(ret));
  } else if (OB_FAIL(ddl_log_replayer_.replay_commit(log, log_scn))) {
    if (OB_TABLET_NOT_EXIST != ret && OB_EAGAIN != ret) {
      LOG_WARN("fail to replay ddl commit log", K(ret), K(log));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObLSDDLLogHandler::replay_ddl_tablet_schema_version_change_log_(const char *log_buf,
                                                                    const int64_t buf_size,
                                                                    int64_t pos,
                                                                    const SCN &log_scn)
{
  int ret = OB_SUCCESS;
  ObTabletSchemaVersionChangeLog log;
  ObSchemaChangeReplayExecutor replay_executor;

  if (OB_FAIL(log.deserialize(log_buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize source barrier log", K(ret));
  } else if (OB_FAIL(replay_executor.init(log, log_scn))) {
    LOG_WARN("failed to init tablet schema version change log replay executor", K(ret));
  } else if (OB_FAIL(replay_executor.execute(log_scn, ls_->get_ls_id(), log.get_tablet_id()))) {
    if (OB_NO_NEED_UPDATE == ret) {
      LOG_WARN("no need replay tablet schema version change log", K(ret), K(log), K(log_scn));
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("failed to replay", K(ret), K(log), K(log_scn));
    }
  }

  return ret;
}

int ObLSDDLLogHandler::replay_ddl_start_log_(const char *log_buf,
                                             const int64_t buf_size,
                                             int64_t pos,
                                             const SCN &log_scn)
{
  int ret = OB_SUCCESS;
  ObDDLStartLog log;
  if (OB_FAIL(log.deserialize(log_buf, buf_size, pos))) {
    LOG_WARN("fail to deserialize ddl redo log", K(ret));
  } else if (OB_FAIL(ddl_log_replayer_.replay_start(log, log_scn))) {
    if (OB_TABLET_NOT_EXIST != ret && OB_EAGAIN != ret) {
      LOG_WARN("fail to replay ddl redo log", K(ret), K(log));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

void ObLSDDLLogHandler::add_ddl_event(const int ret, const ObString &ddl_event_stmt)
{
  SERVER_EVENT_ADD("ddl", ddl_event_stmt.ptr(),
    "tenant_id", MTL_ID(),
    "ret", ret,
    "trace_id", *ObCurTraceId::get_trace_id(),
    "last_rec_scn", last_rec_scn_);
}

int ObLSDDLLogHandler::add_tablet(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(active_ddl_kv_mgr_.add_tablet(tablet_id))) {
    LOG_WARN("add tablet failed", K(ret));
  }
  return ret;
}

int ObLSDDLLogHandler::del_tablets(const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(active_ddl_kv_mgr_.del_tablets(tablet_ids))) {
    LOG_WARN("del tablets failed", K(ret));
  }
  return ret;
}

int ObLSDDLLogHandler::get_tablets(common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(active_ddl_kv_mgr_.get_tablets(tablet_ids))) {
    LOG_WARN("get tablets failed", K(ret));
  }
  return ret;
}

}
}
