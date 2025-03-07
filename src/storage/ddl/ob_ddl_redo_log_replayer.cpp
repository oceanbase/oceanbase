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

#include "ob_ddl_redo_log_replayer.h"
#include "storage/ddl/ob_ddl_replay_executor.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;

ObDDLRedoLogReplayer::ObDDLRedoLogReplayer()
  : is_inited_(false), ls_(nullptr), allocator_()
{
}

ObDDLRedoLogReplayer::~ObDDLRedoLogReplayer()
{
  destroy();
}

int ObDDLRedoLogReplayer::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(ls->get_tenant_id(), "RedoLogBuckLock");
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLRedoLogReplayer has been inited twice", K(ret));
  } else if (OB_FAIL(allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(DEFAULT_HASH_BUCKET_COUNT, ObLatchIds::DEFAULT_BUCKET_LOCK, attr))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLRedoLogReplayer::replay_start(const ObDDLStartLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObDDLStartReplayExecutor replay_executor;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("failed to init ddl start log replay executor", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.get_table_key().tablet_id_))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("failed to replay", K(ret), K(log), K(scn));
    }
  }

  return ret;
}

int ObDDLRedoLogReplayer::replay_redo(const ObDDLRedoLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObDDLRedoReplayExecutor replay_executor;

  DEBUG_SYNC(BEFORE_REPLAY_DDL_MACRO_BLOCK);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("failed to init ddl redo log replay executor", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.get_redo_info().table_key_.tablet_id_))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("failed to replay", K(ret), K(log), K(scn));
    }
  }

  return ret;
}

int ObDDLRedoLogReplayer::replay_commit(const ObDDLCommitLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObDDLCommitReplayExecutor replay_executor;

  DEBUG_SYNC(BEFORE_REPLAY_DDL_PREPRARE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("init replay executor failed", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.get_table_key().tablet_id_))) {
    LOG_WARN("execute replay execute failed", K(ret));
  }
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLRedoLogReplayer::replay_finish(const ObDDLFinishLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObDDLFinishReplayExecutor replay_executor;

  DEBUG_SYNC(BEFORE_REPLAY_DDL_PREPRARE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("init replay executor failed", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.get_table_key().get_tablet_id()))) {
    LOG_WARN("execute replay execute failed", K(ret));
  }
  return ret;
}
#endif

int ObDDLRedoLogReplayer::replay_split_start(const ObTabletSplitStartLog &log, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObSplitStartReplayExecutor replay_executor;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("failed to init ddl commit log replay executor", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.basic_info_.source_tablet_id_))) {
    if (OB_NO_NEED_UPDATE == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("failed to replay split start log", K(ret), K(scn), K(log), K(ls_->get_ls_id()));
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::replay_inc_start(const ObDDLIncStartLog &log, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else {
    ObTabletID tablet_id = log.get_log_basic().get_tablet_id();
    ObTabletID lob_meta_tablet_id = log.get_log_basic().get_lob_meta_tablet_id();
    ObDDLIncStartReplayExecutor replay_executor;
    ObDDLIncStartReplayExecutor lob_meta_replay_executor;
    if (OB_FAIL(replay_executor.init(ls_, tablet_id, scn))) {
      LOG_WARN("failed to init ddl inc start log replay executor", K(ret));
    } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
      if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
        FLOG_INFO("no need to replay ddl inc start log", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_EAGAIN != ret) {
        LOG_WARN("failed to replay", K(ret), K(log), K(scn));
        ret = OB_EAGAIN;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (lob_meta_tablet_id.is_valid()) {
      if (OB_FAIL(lob_meta_replay_executor.init(ls_, lob_meta_tablet_id, scn))) {
        LOG_WARN("failed to init ddl inc start log lob meta replay executor", K(ret));
      } else if (OB_FAIL(lob_meta_replay_executor.execute(scn, ls_->get_ls_id(), lob_meta_tablet_id))) {
        if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
          FLOG_INFO("no need to replay ddl inc start log", K(ret));
          ret = OB_SUCCESS;
        } else if (OB_EAGAIN != ret) {
          LOG_WARN("failed to replay", K(ret), K(log), K(scn));
          ret = OB_EAGAIN;
        }
      }
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::replay_split_finish(const ObTabletSplitFinishLog &log, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObSplitFinishReplayExecutor replay_executor;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("failed to init ddl commit log replay executor", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.basic_info_.source_tablet_id_))) {
    if (OB_NO_NEED_UPDATE == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("failed to replay split finish log", K(ret), K(scn), K(log), K(ls_->get_ls_id()));
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::replay_tablet_freeze(const ObTabletFreezeLog &log, const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  ObTabletFreezeReplayExecutor replay_executor;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_FAIL(replay_executor.init(ls_, log, scn))) {
    LOG_WARN("failed to init tablet freeze log replay executor", K(ret));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), log.tablet_id_))) {
    if (OB_NO_NEED_UPDATE == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("failed to replay tablet freeze log", K(ret), K(scn), K(log), K(ls_->get_ls_id()));
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::replay_inc_commit(const ObDDLIncCommitLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else {
    ObTabletID tablet_id = log.get_log_basic().get_tablet_id();
    ObTabletID lob_meta_tablet_id = log.get_log_basic().get_lob_meta_tablet_id();
    ObDDLIncCommitReplayExecutor replay_executor;
    ObDDLIncCommitReplayExecutor lob_meta_replay_executor;
    if (OB_FAIL(replay_executor.init(ls_, tablet_id, scn))) {
      LOG_WARN("failed to init ddl inc commit log replay executor", K(ret));
    } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
      if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
        FLOG_INFO("no need to replay ddl inc commit log", K(ret));
        ret = OB_SUCCESS;
      } else if (OB_EAGAIN != ret) {
        LOG_WARN("failed to replay", K(ret), K(log), K(scn));
        ret = OB_EAGAIN;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (lob_meta_tablet_id.is_valid()) {
      if (OB_FAIL(lob_meta_replay_executor.init(ls_, lob_meta_tablet_id, scn))) {
        LOG_WARN("failed to init ddl inc commit log lob meta replay executor", K(ret));
      } else if (OB_FAIL(lob_meta_replay_executor.execute(scn, ls_->get_ls_id(), lob_meta_tablet_id))) {
        if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
          FLOG_INFO("no need to replay ddl inc commit log", K(ret));
          ret = OB_SUCCESS;
        } else if (OB_EAGAIN != ret) {
          LOG_WARN("failed to replay", K(ret), K(log), K(scn));
          ret = OB_EAGAIN;
        }
      }
    }
  }
  return ret;
}

void ObDDLRedoLogReplayer::destroy()
{
  is_inited_ = false;
  ls_ = nullptr;
  allocator_.reset();
}
