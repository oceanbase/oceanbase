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
using namespace oceanbase::transaction;

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
    if (is_incremental_major_direct_load(log.get_log_basic().get_direct_load_type())) {
      if (OB_FAIL(do_replay_inc_major_start(tablet_id, scn, log.has_cs_replica(), false/*is_lob*/, log.get_storage_schema()))) {
        LOG_WARN("failed to do replay inc major start", KR(ret), K(tablet_id), K(scn), K(log));
      } else if (lob_meta_tablet_id.is_valid()
          && OB_FAIL(do_replay_inc_major_start(lob_meta_tablet_id, scn,
              false/*lob meta only has row store*/, true/*is_lob*/, nullptr/*storage_schema*/))) {
        LOG_WARN("failed to do replay inc major start for lob", KR(ret), K(lob_meta_tablet_id), K(scn));
      }
      FLOG_INFO("replay inc major start log", K(ret), K(log));
    } else {
      if (OB_FAIL(do_replay_inc_minor_start(tablet_id, scn))) {
        LOG_WARN("failed to do replay inc minor start", KR(ret), K(tablet_id), K(scn));
      } else if (lob_meta_tablet_id.is_valid()
          && OB_FAIL(do_replay_inc_minor_start(lob_meta_tablet_id, scn))) {
        LOG_WARN("failed to do replay inc minor start for lob", KR(ret), K(lob_meta_tablet_id), K(scn));
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

int ObDDLRedoLogReplayer::replay_inc_commit(
    const ObDDLIncCommitLog &log,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else {
    const ObDDLIncLogBasic &log_basic = log.get_log_basic();
    ObTabletID tablet_id = log_basic.get_tablet_id();
    ObTabletID lob_meta_tablet_id = log_basic.get_lob_meta_tablet_id();
    const ObTransID &trans_id = log_basic.get_trans_id();
    const ObTxSEQ &seq_no = log_basic.get_seq_no();
    const int64_t snapshot_version = log_basic.get_snapshot_version();
    const uint64_t data_format_version = log_basic.get_data_format_version();
    const bool is_rollback = log.is_rollback();
    if (is_incremental_major_direct_load(log_basic.get_direct_load_type())) {
      if (OB_FAIL(do_replay_inc_major_commit(tablet_id,
                                             scn,
                                             trans_id,
                                             seq_no,
                                             snapshot_version,
                                             data_format_version,
                                             is_rollback))) {
        LOG_WARN("failed to do replay inc major commit", KR(ret), K(tablet_id), K(scn),
                 K(log_basic), K(is_rollback));
      } else if (lob_meta_tablet_id.is_valid() &&
                 OB_FAIL(do_replay_inc_major_commit(lob_meta_tablet_id,
                                                    scn,
                                                    trans_id,
                                                    seq_no,
                                                    snapshot_version,
                                                    data_format_version,
                                                    is_rollback))) {
        LOG_WARN("failed to do replay inc major commit for lob", KR(ret), K(lob_meta_tablet_id),
                 K(scn), K(log_basic), K(is_rollback));
      }
    } else {
      if (OB_FAIL(do_replay_inc_minor_commit(tablet_id, scn))) {
        LOG_WARN("failed to do replay inc minor commit", KR(ret), K(tablet_id), K(scn));
      } else if (lob_meta_tablet_id.is_valid()
          && OB_FAIL(do_replay_inc_minor_commit(lob_meta_tablet_id, scn))) {
        LOG_WARN("failed to do replay inc minor commit for lob", KR(ret), K(lob_meta_tablet_id), K(scn));
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

int ObDDLRedoLogReplayer::do_replay_inc_minor_start(
    const ObTabletID &tablet_id,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObDDLIncMinorStartReplayExecutor replay_executor;
  if (OB_FAIL(replay_executor.init(ls_, tablet_id, scn))) {
    STORAGE_LOG(WARN, "failed to init ddl inc minor start log replay executor",
        KR(ret), K(tablet_id), K(scn));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
      FLOG_INFO("no need to replay ddl inc minor start log", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to replay", KR(ret), K(tablet_id), K(scn));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::do_replay_inc_major_start(
    const ObTabletID &tablet_id,
    const SCN &scn,
    const bool has_cs_replica,
    const bool is_lob,
    const ObStorageSchema *storage_schema)
{
  int ret = OB_SUCCESS;
  ObDDLIncMajorStartReplayExecutor replay_executor;
  if (OB_FAIL(replay_executor.init(ls_, tablet_id, scn, has_cs_replica, is_lob, storage_schema))) {
    STORAGE_LOG(WARN, "failed to init ddl inc major start log replay executor",
        KR(ret), K(tablet_id), K(scn), K(has_cs_replica), K(is_lob), KPC(storage_schema));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
      FLOG_INFO("no need to replay ddl inc major start log", KR(ret), K(tablet_id), K(scn));
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to replay", KR(ret), K(tablet_id), K(scn));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::do_replay_inc_minor_commit(
    const ObTabletID &tablet_id,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObDDLIncMinorCommitReplayExecutor replay_executor;
  if (OB_FAIL(replay_executor.init(ls_, tablet_id, scn))) {
    STORAGE_LOG(WARN, "failed to init ddl inc minor commit log replay executor", KR(ret), K(tablet_id), K(scn));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
      FLOG_INFO("no need to replay ddl inc minor commit log", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to replay ddl inc minor commit log", KR(ret), K(tablet_id), K(scn));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

int ObDDLRedoLogReplayer::do_replay_inc_major_commit(
    const ObTabletID &tablet_id,
    const SCN &scn,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    const bool is_rollback)
{
  int ret = OB_SUCCESS;
  ObDDLIncMajorCommitReplayExecutor replay_executor;
  if (OB_FAIL(replay_executor.init(ls_,
                                   tablet_id,
                                   scn,
                                   trans_id,
                                   seq_no,
                                   snapshot_version,
                                   data_format_version,
                                   is_rollback))) {
    STORAGE_LOG(WARN, "failed to init ddl inc major commit log replay executor", KR(ret),
                K(tablet_id), K(scn), K(trans_id), K(seq_no), K(snapshot_version),
                K(data_format_version), K(is_rollback));
  } else if (OB_FAIL(replay_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
      FLOG_INFO("no need to replay ddl inc major commit log", KR(ret));
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to replay ddl inc major commit log", KR(ret), K(tablet_id), K(scn));
      ret = OB_EAGAIN;
    }
  }
  return ret;
}