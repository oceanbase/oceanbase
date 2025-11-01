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

#include "storage/ddl/ob_ddl_inc_clog_callback.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_schedule.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"

namespace oceanbase
{
namespace storage
{

using namespace blocksstable;
using namespace share;
using namespace common;
using namespace transaction;

ObDDLIncStartClogCb::ObDDLIncStartClogCb()
  : is_inited_(false), ls_id_(), log_basic_()
{
}

int ObDDLIncStartClogCb::init(const ObLSID &ls_id, const ObDDLIncLogBasic &log_basic)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !log_basic.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(log_basic));
  } else {
    ls_id_ = ls_id;
    log_basic_ = log_basic;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLIncStartClogCb::on_success()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("ObDDLIncStartClogCb::on_success", 1 * 1000 * 1000); // 1s
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  SCN scn = __get_scn();
  FLOG_INFO("write ddl inc start log success", K(ls_id_), K(scn), K(log_basic_));
  try_release();
  return OB_SUCCESS;
}

int ObDDLIncStartClogCb::on_failure()
{
  int ret = OB_SUCCESS;
  status_.set_state(STATE_FAILED);
  try_release();

  return OB_SUCCESS;
}

void ObDDLIncStartClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    ObDDLIncStartClogCb *cb = this;
    ob_delete(cb);
  }
}

ObDDLIncRedoClogCb::ObDDLIncRedoClogCb()
  : is_inited_(false), ls_id_(), redo_info_(), macro_block_id_(),
    data_buffer_lock_(), is_data_buffer_freed_(false),
    direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), with_cs_replica_(false)
{
}

ObDDLIncRedoClogCb::~ObDDLIncRedoClogCb()
{
  int ret = OB_SUCCESS;
  if (macro_block_id_.is_valid() && OB_FAIL(OB_STORAGE_OBJECT_MGR.dec_ref(macro_block_id_))) {
    LOG_ERROR("dec ref failed", K(ret), K(macro_block_id_), K(common::lbt()));
  }
  macro_block_id_.reset();
}

int ObDDLIncRedoClogCb::init(const share::ObLSID &ls_id,
                             const storage::ObDDLMacroBlockRedoInfo &redo_info,
                             const blocksstable::MacroBlockId &macro_block_id,
                             ObTabletHandle &tablet_handle,
                             ObDirectLoadType direct_load_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !redo_info.is_valid() || !macro_block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(redo_info), K(macro_block_id));
  } else if (OB_UNLIKELY(!is_incremental_direct_load(direct_load_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("only support incremental minor/major direct load type", KR(ret), K(direct_load_type));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.inc_ref(macro_block_id))) {
    LOG_WARN("inc reference count failed", K(ret), K(macro_block_id));
  } else if (OB_FAIL(tablet_handle_.assign(tablet_handle))) {
    LOG_WARN("failed to assign tablet_handle", K(ret), K(tablet_handle));
  } else {
    redo_info_ = redo_info;
    ls_id_ = ls_id;
    macro_block_id_ = macro_block_id;
    direct_load_type_ = direct_load_type;
    with_cs_replica_ = redo_info.with_cs_replica_;
  }
  return ret;
}

void ObDDLIncRedoClogCb::try_release()
{
  {
    ObSpinLockGuard data_buffer_guard(data_buffer_lock_);
    is_data_buffer_freed_ = true;
  }
  if (status_.try_set_release_flag()) {
  } else {
    ObDDLIncRedoClogCb *cb = this;
    ob_delete(cb);
  }
}

int ObDDLIncRedoClogCb::on_success()
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("ObDDLIncRedoClogCb::on_success", 1 * 1000 * 1000); // 1s
  ObDDLMacroBlock macro_block;
  {
    ObSpinLockGuard data_buffer_guard(data_buffer_lock_);
    const bool force_set_macro_meta = is_incremental_minor_direct_load(direct_load_type_);
    if (is_data_buffer_freed_) {
      LOG_INFO("data buffer is freed, do not need to callback");
    } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_block_id_))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_block_id_));
    } else if (OB_FAIL(macro_block.set_data_macro_meta(macro_block_id_,
                                                       redo_info_.data_buffer_.ptr(),
                                                       redo_info_.data_buffer_.length(),
                                                       redo_info_.block_type_,
                                                       force_set_macro_meta))) {
      LOG_WARN("fail to set data macro meta", K(ret), K(macro_block_id_),
                                                      KP(redo_info_.data_buffer_.ptr()),
                                                      K(redo_info_.data_buffer_.length()),
                                                      K(redo_info_.block_type_));

    } else {
      macro_block.block_type_ = redo_info_.block_type_;
      macro_block.logic_id_ = redo_info_.logic_id_;
      macro_block.scn_ = __get_scn();
      macro_block.ddl_start_scn_ = redo_info_.start_scn_;
      macro_block.table_key_ = redo_info_.table_key_;
      macro_block.end_row_id_ = redo_info_.end_row_id_;
      macro_block.trans_id_ = redo_info_.trans_id_;
      macro_block.seq_no_ = redo_info_.seq_no_;
      const int64_t snapshot_version = redo_info_.table_key_.get_snapshot_version();
      const uint64_t data_format_version = redo_info_.data_format_version_;

      if (with_cs_replica_ && macro_block.table_key_.is_column_store_sstable()) {
        LOG_INFO("[CS-Replica] skip replay cs replica inc redo clog in leader", K(ret), K_(with_cs_replica), K(macro_block));
      } else if (OB_FAIL(set_macro_block(macro_block, snapshot_version, data_format_version))) {
        LOG_WARN("fail to set macro block", K(ret));
      } else {
        LOG_INFO("set macro block success", K_(with_cs_replica), K(macro_block)); // tmp log, change to debug log later
      }
    }
  }
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();

  return OB_SUCCESS; // force return success
}

int ObDDLIncRedoClogCb::on_failure()
{
  status_.set_state(STATE_FAILED);
  try_release();
  return OB_SUCCESS;
}

int ObDDLIncRedoClogCb::set_macro_block(
    const ObDDLMacroBlock &macro_block,
    const int64_t snapshot_version,
    const uint64_t data_format_version)
{
  int ret = OB_SUCCESS;
  if (is_incremental_minor_direct_load(direct_load_type_)) {
    if (OB_FAIL(tablet_handle_.get_obj()->set_macro_block(macro_block, snapshot_version, data_format_version))) {
      LOG_WARN("failed to set macro block", KR(ret));
    }
  } else {
    ObTabletDirectLoadMgrHandle mock_mgr_handle;
    if (OB_FAIL(ObDDLKVPendingGuard::set_macro_block(
        tablet_handle_.get_obj(), macro_block, snapshot_version, data_format_version, mock_mgr_handle, direct_load_type_))) {
      LOG_WARN("failed to set macro block", KR(ret), KPC(tablet_handle_.get_obj()), K(macro_block),
          K(snapshot_version), K(data_format_version));
    }
  }
  return ret;
}

/**
 * ObDDLIncCommitClogCb
 */

ObDDLIncCommitClogCb::ObDDLIncCommitClogCb()
  : ls_id_(), log_basic_(), is_rollback_(false), is_inited_(false)
{
}

int ObDDLIncCommitClogCb::init(const ObLSID &ls_id, const ObDDLIncCommitLog &log)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(log));
  } else {
    ls_id_ = ls_id;
    log_basic_ = log.get_log_basic();
    is_rollback_ = log.is_rollback();

    ObTimeGuard timeguard("ObDDLIncCommitClogCb::prepare", 1 * 1000 * 1000); // 1s
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(ls_id_));
    } else if (OB_FAIL(prepare_(ls_handle, log_basic_.get_tablet_id()))) {
      LOG_WARN("fail to prepare", KR(ret), K(log_basic_));
    } else if (log_basic_.get_lob_meta_tablet_id().is_valid() &&
               OB_FAIL(prepare_(ls_handle, log_basic_.get_lob_meta_tablet_id()))) {
      LOG_WARN("fail to prepare", KR(ret), K(log_basic_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObDDLIncCommitClogCb::on_success()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIncCommitClogCb not init", KR(ret), KP(this));
  } else {
    ObTimeGuard timeguard("ObDDLIncCommitClogCb::on_success", 1 * 1000 * 1000); // 1s
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(ls_id_));
    } else if (OB_FAIL(on_success_(ls_handle, log_basic_.get_tablet_id()))) {
      LOG_WARN("fail to on success", KR(ret), K(log_basic_));
    } else if (log_basic_.get_lob_meta_tablet_id().is_valid() &&
               OB_FAIL(on_success_(ls_handle, log_basic_.get_lob_meta_tablet_id()))) {
      LOG_WARN("fail to on success", KR(ret), K(log_basic_));
    }
  }

  SCN scn = __get_scn();
  FLOG_INFO("write ddl inc commit log end", KR(ret), K(ls_id_), K(log_basic_), K(is_rollback_), K(scn));
  if (OB_FAIL(ret)) {
    LOG_ERROR("write ddl inc commit log failed", KR(ret), K(ls_id_), K(log_basic_), K(is_rollback_), K(scn));
    // 无法处理callback失败的情况, 修改错误码防止走到重试逻辑
    ret = OB_ERR_UNEXPECTED;
  }
  status_.set_ret_code(ret);
  status_.set_state(STATE_SUCCESS);
  try_release();

  return OB_SUCCESS;
}

int ObDDLIncCommitClogCb::on_failure()
{
  int ret = OB_SUCCESS;
  status_.set_state(STATE_FAILED);
  try_release();

  return OB_SUCCESS;
}

void ObDDLIncCommitClogCb::try_release()
{
  if (status_.try_set_release_flag()) {
  } else {
    ObDDLIncCommitClogCb *cb = this;
    ob_delete(cb);
  }
}

int ObDDLIncCommitClogCb::prepare_(const ObLSHandle &ls_handle, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_handle), K(tablet_id));
  } else if (is_incremental_minor_direct_load(log_basic_.get_direct_load_type())) {
    // 普通增量
    // do nothing
  } else {
    // 增量基线
    if (is_rollback_) {
      // do nothing
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (GCTX.is_shared_storage_mode()) {
      // do nothing
#endif
    } else {
      ObLS *ls = ls_handle.get_ls();
      ObTabletHandle tablet_handle;
      const int64_t start_time = ObTimeUtility::current_time();
      while (OB_SUCC(ret)) {
        if (OB_FAIL(THIS_WORKER.check_status())) {
          LOG_WARN("check status failed", KR(ret));
        } else if (OB_UNLIKELY(ls->is_offline() || ls->is_stopped())) {
          ret = OB_LS_NOT_EXIST;
          LOG_WARN("ls status error", KR(ret), K(ls_id_), KPC(ls));
        } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle))) {
          LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
        } else if (OB_FAIL(ObIncDDLMergeTaskUtils::prepare_freeze_inc_major_ddl_kv(tablet_handle))) {
          if (OB_UNLIKELY(OB_EAGAIN != ret)) {
            LOG_WARN("fail to prepare freeze inc major ddl kv", KR(ret), K(tablet_handle));
          } else {
            ret = OB_SUCCESS;
            if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
              const int64_t cost_time = ObTimeUtility::current_time() - start_time;
              LOG_INFO("wait for prepare freeze inc major ddl kv too long time", K(ls_id_), K(tablet_id), K(cost_time));
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(ls, tablet_handle))) {
                LOG_WARN("fail to schedule tablet ddl inc major merge", KR(tmp_ret), K(ls_id_), K(tablet_id));
              }
            }
            ob_usleep(1000);
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

int ObDDLIncCommitClogCb::on_success_(const ObLSHandle &ls_handle, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(ls_handle), K(tablet_id));
  } else if (is_incremental_minor_direct_load(log_basic_.get_direct_load_type())) {
    // 普通增量
    const bool is_sync = false;
    (void)ls_handle.get_ls()->tablet_freeze(tablet_id,
                                            is_sync,
                                            0, /*timeout, useless for async one*/
                                            false, /*need_rewrite_meta*/
                                            ObFreezeSourceFlag::DIRECT_INC_END);
  } else {
    // 增量基线
    const SCN &commit_scn = __get_scn();
    ObTabletHandle tablet_handle;
    if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle))) {
      LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
    } else if (OB_FAIL(ObIncDDLMergeTaskUtils::freeze_inc_major_ddl_kv(tablet_handle,
                                                                       is_rollback_ ? SCN::min_scn() : commit_scn,
                                                                       log_basic_.get_trans_id(),
                                                                       log_basic_.get_seq_no(),
                                                                       log_basic_.get_snapshot_version(),
                                                                       log_basic_.get_data_format_version(),
                                                                       false /*is_replay*/))) {
      LOG_WARN("fail to freeze inc major ddl kv", KR(ret), K(ls_id_), K(tablet_id), K(commit_scn), K(log_basic_));
    } else if (is_rollback_) {
      // do nothing
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (GCTX.is_shared_storage_mode()) {
      // do nothing
#endif
    } else if (OB_FAIL(ObIncDDLMergeTaskUtils::update_tablet_table_store(ls_handle.get_ls(), tablet_handle))) {
      LOG_WARN("fail to update tablet table store", KR(ret), K(ls_id_), K(tablet_id));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
