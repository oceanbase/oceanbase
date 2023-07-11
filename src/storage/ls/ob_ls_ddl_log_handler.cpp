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
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "logservice/ob_log_base_header.h"
#include "share/scn.h"

namespace oceanbase
{

using namespace blocksstable;
using namespace share;

namespace storage
{

int ObLSDDLLogHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLSDDLLogHandler init twice", K(ret));
  } else if (nullptr == ls) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (ddl_log_replayer_.init(ls)) {
    LOG_WARN("fail to init ddl log replayer", K(ret));
  } else {
    TCWLockGuard guard(online_lock_);
    is_online_ = true;
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
  FLOG_INFO("ddl log hanlder offline", K(ret), "ls_meta", ls_->get_ls_meta());
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
        ObDDLKvMgrHandle ddl_kv_mgr_handle;
        if (OB_FAIL(tablet_iter.get_next_ddl_kv_mgr(ddl_kv_mgr_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get ddl kv mgr", K(ret), K(ddl_kv_mgr_handle));
          }
        } else if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet handle", K(ret), K(ddl_kv_mgr_handle));
        } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->online())) {
          LOG_WARN("ddl kv mgr cleanup failed", K(ret), "ls_meta", ls_->get_ls_meta(), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    TCWLockGuard guard(online_lock_);
    is_online_ = true;
  }
  FLOG_INFO("ddl log hanlder online", K(ret), "ls_meta", ls_->get_ls_meta());
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
  if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls_));
  } else {
    TCRLockGuard guard(online_lock_);
    if (!is_online_) {
      LOG_INFO("ddl log handler is offline, no need to flush", K(ret), "ls_meta", ls_->get_ls_meta());
    } else {
      bool has_ddl_kv = false;
      while (OB_SUCC(ret)) {
        ObDDLKvMgrHandle ddl_kv_mgr_handle;
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
        } else if (has_ddl_kv) {
          DEBUG_SYNC(BEFORE_DDL_CHECKPOINT);
          const SCN start_scn = ddl_kv_mgr_handle.get_obj()->get_start_scn();
          const ObTabletID &tablet_id = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
          ObTabletHandle tablet_handle;
          if (OB_FAIL(ls_->get_tablet(tablet_id, tablet_handle))) {
            LOG_WARN("failed to get tablet", K(ret), K(ls_->get_ls_id()), K(tablet_id));
          } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->schedule_ddl_dump_task(*tablet_handle.get_obj(), start_scn, rec_scn))) {
            if (OB_EAGAIN != ret && OB_SIZE_OVERFLOW != ret) {
              LOG_WARN("failed to schedule ddl kv merge dag", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
      }
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
  if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", K(ret), K(ls_));
  } else {
    while (OB_SUCC(ret)) {
      ObDDLKvMgrHandle ddl_kv_mgr_handle;
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
      } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_rec_scn(rec_scn))) {
        LOG_WARN("failed to get rec scn", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
    rec_scn = SCN::max(last_rec_scn_, ls_->get_clog_checkpoint_scn());
  } else if (!rec_scn.is_max()) {
    last_rec_scn_ = SCN::max(last_rec_scn_, rec_scn);
  }
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

}
}
