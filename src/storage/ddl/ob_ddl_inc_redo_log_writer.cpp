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

#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::archive;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;

ObDDLIncLogHandle::ObDDLIncLogHandle()
  : cb_(nullptr), scn_(SCN::min_scn())
{
}

ObDDLIncLogHandle::~ObDDLIncLogHandle()
{
  reset();
}

void ObDDLIncLogHandle::reset()
{
  if (nullptr != cb_) {
    cb_->try_release();
    cb_ = nullptr;
  }
}

int ObDDLIncLogHandle::wait(const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cb_)) {
  } else {
    bool finish = false;
    const int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !finish) {
      if (OB_FAIL(THIS_WORKER.check_status())) {
        LOG_WARN("check status failed", K(ret));
      } else if (cb_->is_success()) {
        finish = true;
      } else if (cb_->is_failed()) {
        ret = OB_NOT_MASTER;
      }
      if (OB_SUCC(ret) && !finish) {
        const int64_t current_time = ObTimeUtility::current_time();
        if (current_time - start_time > timeout) {
          ret = OB_TIMEOUT;
          LOG_WARN("write ddl redo log timeout", K(ret), K(current_time), K(start_time));
        } else {
          if (REACH_TIME_INTERVAL(10L * 1000L * 1000L)) { //10s
            LOG_INFO("wait ddl redo log callback", K(ret), K(finish), K(current_time), K(start_time));
          }
          ob_usleep(CHECK_DDL_INC_LOG_FINISH_INTERVAL);
        }
      }
    }
  }

  return ret;
}

ObDDLIncRedoLogWriter::ObDDLIncRedoLogWriter()
  : is_inited_(false), remote_write_(false),
    ls_id_(), tablet_id_(), ddl_inc_log_handle_(), leader_addr_(), leader_ls_id_(), buffer_(nullptr)
{
}

ObDDLIncRedoLogWriter::~ObDDLIncRedoLogWriter()
{
  ddl_inc_log_handle_.reset();
  if (nullptr != buffer_) {
    ob_free(buffer_);
    buffer_ = nullptr;
  }
}

int ObDDLIncRedoLogWriter::init(const ObLSID &ls_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

void ObDDLIncRedoLogWriter::reset()
{
  is_inited_ = false;
  remote_write_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  ddl_inc_log_handle_.reset();
  leader_addr_.reset();
  leader_ls_id_.reset();
}

bool ObDDLIncRedoLogWriter::need_retry(int ret_code, bool allow_remote_write)
{
  return OB_TX_NOLOGCB == ret_code || (allow_remote_write && OB_NOT_MASTER == ret_code);
}

int ObDDLIncRedoLogWriter::write_inc_start_log(
    const ObTabletID &lob_meta_tablet_id,
    transaction::ObTxDesc *tx_desc,
    SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObDDLIncLogBasic log_basic;
  ObDDLIncStartLog log;
  start_scn.set_min();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(tx_desc == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tx_desc));
  } else if (OB_FAIL(log_basic.init(tablet_id_, lob_meta_tablet_id))) {
    LOG_WARN("fail to init log_basic", K(ret), K(tablet_id_), K(lob_meta_tablet_id));
  } else if (OB_FAIL(log.init(log_basic))) {
    LOG_WARN("fail to init DDLIncStartLog", K(ret), K(log_basic));
  } else if (OB_FAIL(local_write_inc_start_log(log, tx_desc, start_scn))) {
    LOG_WARN("local write inc start log fail", K(ret));
  } else {
    LOG_INFO("local write inc start log success", K(tablet_id_), K(lob_meta_tablet_id));
  }

  return ret;
}

int ObDDLIncRedoLogWriter::write_inc_redo_log(
    const ObDDLMacroBlockRedoInfo &redo_info,
    const blocksstable::MacroBlockId &macro_block_id,
    const int64_t task_id,
    ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  const int64_t BUF_SIZE = 2 * 1024 * 1024 + 16 * 1024;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!redo_info.is_valid() || !macro_block_id.is_valid() || task_id == 0 || OB_ISNULL(tx_desc))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info), K(macro_block_id), K(task_id), KP(tx_desc));
  } else if (buffer_ == nullptr && OB_ISNULL(buffer_ = static_cast<char *>(ob_malloc(BUF_SIZE, ObMemAttr(MTL_ID(), "DDL_REDO_LOG"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(BUF_SIZE));
  } else if (OB_FAIL(local_write_inc_redo_log(redo_info, macro_block_id, task_id, tx_desc))) {
    LOG_WARN("local write inc redo log fail", K(ret), K(redo_info));
  }

  return ret;
}

int ObDDLIncRedoLogWriter::write_inc_commit_log(
    const bool allow_remote_write,
    const ObTabletID &lob_meta_tablet_id,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(tx_desc == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tx_desc));
  } else if (!remote_write_) {
    ObDDLIncLogBasic log_basic;
    ObDDLIncCommitLog log;
    if (OB_FAIL(log_basic.init(tablet_id_, lob_meta_tablet_id))) {
      LOG_WARN("fail to init log_basic", K(ret), K(tablet_id_), K(lob_meta_tablet_id));
    } else if (OB_FAIL(log.init(log_basic))) {
      LOG_WARN("fail to init DDLIncCommitLog", K(ret), K(log_basic));
    } else if (OB_FAIL(local_write_inc_commit_log(log, tx_desc))) {
      if (ObDDLUtil::need_remote_write(ret) && allow_remote_write) {
        if (OB_FAIL(switch_to_remote_write())) {
          LOG_WARN("fail to switch to remote write", K(ret), K(tablet_id_));
        }
      } else {
        LOG_WARN("local write inc commit log fail", K(ret), K(tablet_id_));
      }
    } else {
      LOG_INFO("local write inc commit log success", K(tablet_id_), K(lob_meta_tablet_id));
    }
  }
  if (OB_SUCC(ret) && remote_write_) {
    if (OB_FAIL(retry_remote_write_inc_commit_log(lob_meta_tablet_id, tx_desc))) {
      LOG_WARN("remote write inc commit log fail", K(ret), K(tablet_id_));
    } else {
      LOG_INFO("remote write inc commit log success", K(tablet_id_), K(lob_meta_tablet_id), K(leader_addr_));
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::wait_inc_redo_log_finish()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (remote_write_) {
    // remote write no need to wait local handle
  } else if (OB_UNLIKELY(!ddl_inc_log_handle_.is_valid())) {
    // no redo log has been written yet
  } else {
    common::ObTimeGuard timeguard("write_inc_redo_log wait", 5 * 1000 * 1000); // 5s
    if (OB_FAIL(ddl_inc_log_handle_.wait())) {
      LOG_WARN("fail to wait io finish", K(ret));
    } else if (OB_FAIL(ddl_inc_log_handle_.cb_->get_ret_code())) {
      LOG_WARN("ddl redo callback executed failed", K(ret));
    }
  }
  ddl_inc_log_handle_.reset();

  return ret;
}

int ObDDLIncRedoLogWriter::write_inc_start_log_with_retry(
    const ObTabletID &lob_meta_tablet_id,
    transaction::ObTxDesc *tx_desc,
    share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  const int64_t timeout_us = ObDDLIncRedoLogWriter::DEFAULT_RETRY_TIMEOUT_US;
  int64_t retry_count = 0;
  do {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(write_inc_start_log(lob_meta_tablet_id, tx_desc, start_scn))) {
      LOG_WARN("write inc ddl start log failed", K(ret));
    }
    if (ObDDLIncRedoLogWriter::need_retry(ret, false/*allow_remote_write*/)) {
      usleep(1000L * 1000L); // 1s
      ++retry_count;
      LOG_WARN("retry write ddl inc start log", K(ret), K(ls_id_), K(tablet_id_), K(retry_count));
      ret = OB_SUCCESS;
    } else {
      break;
    }
  } while (ObTimeUtility::fast_current_time() - start_ts < timeout_us);

  return ret;
}

int ObDDLIncRedoLogWriter::write_inc_redo_log_with_retry(
    const storage::ObDDLMacroBlockRedoInfo &redo_info,
    const blocksstable::MacroBlockId &macro_block_id,
    const int64_t task_id,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  int64_t retry_count = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(write_inc_redo_log(redo_info, macro_block_id, task_id, tx_desc))) {
      LOG_WARN("write inc ddl redo log failed", K(ret));
    }
    if (ObDDLIncRedoLogWriter::need_retry(ret, false/*allow_remote_write*/)) {
      usleep(1000L * 1000L); // 1s
      ++retry_count;
      LOG_WARN("retry write ddl inc redo log", K(ret), K(ls_id_), K(tablet_id_), K(retry_count));
      ret = OB_SUCCESS;
    } else {
      break;
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::write_inc_commit_log_with_retry(
    const bool allow_remote_write,
    const ObTabletID &lob_meta_tablet_id,
    ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  const int64_t timeout_us = ObDDLIncRedoLogWriter::DEFAULT_RETRY_TIMEOUT_US;
  int64_t retry_count = 0;
  do {
    if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(write_inc_commit_log(allow_remote_write, lob_meta_tablet_id, tx_desc))) {
      LOG_WARN("write inc ddl commit log failed", K(ret));
    }
    if (ObDDLIncRedoLogWriter::need_retry(ret, allow_remote_write)) {
      usleep(1000L * 1000L); // 1s
      ++retry_count;
      LOG_WARN("retry write ddl commit log", K(ret), K(ls_id_), K(tablet_id_), K(retry_count));
    } else {
      break;
    }
  } while (ObTimeUtility::fast_current_time() - start_ts < timeout_us);

  return ret;
}

int ObDDLIncRedoLogWriter::get_write_store_ctx_guard(
    ObTxDesc *tx_desc,
    ObStoreCtxGuard &ctx_guard,
    storage::ObLS *&ls)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = ctx_guard.get_store_ctx();
  if (OB_NOT_NULL(ls)) {
    ls->reset();
    ls = nullptr;
  }
  if (OB_FAIL(ctx_guard.init(ls_id_))) {
    LOG_WARN("ctx_guard init fail", K(ret), K(ls_id_));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ctx_guard.get_ls_handle(), ObLSGetMod::DAS_MOD))) {
    LOG_WARN("get ls failed", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls = ctx_guard.get_ls_handle().get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret), K(ls_id_));
  } else {
    ctx.ls_ = ls;
    ctx.timeout_ = DEFAULT_RETRY_TIMEOUT_US;
    ObDMLBaseParam dml_param;
    dml_param.snapshot_.init_none_read();
    dml_param.spec_seq_no_ = ObTxSEQ(1, 1);
    if (OB_FAIL(ls->get_write_store_ctx(*tx_desc,
                                        dml_param.snapshot_,
                                        dml_param.write_flag_,
                                        ctx_guard.get_store_ctx(),
                                        dml_param.spec_seq_no_))) {
      LOG_WARN("can not get write store ctx", K(ret), K(ls_id_), K(*tx_desc));
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::switch_to_remote_write()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  share::ObLocationService *location_service = nullptr;
  bool is_cache_hit = false;
  if (OB_ISNULL(location_service = GCTX.location_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("location service is null", K(ret), KP(location_service));
  } else if (OB_FAIL(location_service->get(tenant_id,
                                           tablet_id_,
                                           INT64_MAX/*expire_renew_time*/,
                                           is_cache_hit,
                                           leader_ls_id_))) {
    LOG_WARN("fail to get log stream id", K(ret), K_(tablet_id));
  } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id,
                                                  tenant_id,
                                                  leader_ls_id_,
                                                  true, /*force_renew*/
                                                  leader_addr_))) {
      LOG_WARN("get leader failed", K(ret), K(leader_ls_id_));
  } else {
    remote_write_ = true;
    LOG_INFO("switch to remote write", K(ret), K_(tablet_id), K_(leader_ls_id), K_(leader_addr));
  }
  return ret;
}

int ObDDLIncRedoLogWriter::local_write_inc_start_log(
    ObDDLIncStartLog &log,
    ObTxDesc *tx_desc,
    SCN &start_scn)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObLS *ls = nullptr;
  ObTabletID lob_meta_tablet_id = log.get_log_basic().get_lob_meta_tablet_id();
  ObDDLIncStartClogCb *cb = nullptr;
  ObPartTransCtx *trans_ctx = nullptr;
  ObDDLIncLogHandle handle;
  const bool is_sync = true;
  const int64_t abs_timeout_ts = ObClockGenerator::getClock() + DEFAULT_RETRY_TIMEOUT_US;

  ObDDLRedoLockGuard guard(tablet_id_.hash());
  if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(log));
  } else if (OB_FAIL(get_write_store_ctx_guard(tx_desc, ctx_guard, ls))) {
    LOG_WARN("fail to get_write_store_ctx_guard", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls) || !tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(ls), K(tablet_id_));
  } else if (OB_FAIL(ls->tablet_freeze(tablet_id_, is_sync, abs_timeout_ts))) {
    LOG_WARN("sync tablet freeze failed", K(ret), K(tablet_id_));
  } else if (lob_meta_tablet_id.is_valid() && OB_FAIL(ls->tablet_freeze(lob_meta_tablet_id, is_sync, abs_timeout_ts))) {
    LOG_WARN("sync tablet freeze failed", K(ret), K(lob_meta_tablet_id));
  } else if (OB_ISNULL(cb = OB_NEW(ObDDLIncStartClogCb, ObMemAttr(MTL_ID(), "DDL_IRLW")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(cb->init(log.get_log_basic()))) {
    LOG_WARN("failed to init cb", K(ret));
  } else if (OB_ISNULL(trans_ctx = ctx_guard.get_store_ctx().mvcc_acc_ctx_.tx_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trans_ctx should not be null", K(ret));
  } else if (OB_FAIL(trans_ctx->submit_direct_load_inc_start_log(log, cb, start_scn))) {
    LOG_WARN("fail to submit ddl inc start log", K(ret), K(log));
  } else {
    common::ObTimeGuard timeguard("write_inc_start_log wait", 5 * 1000 * 1000); // 5s
    if (OB_SUCC(ret)) {
      handle.cb_ = cb;
      cb = nullptr;
      if (OB_FAIL(handle.wait())) {
        LOG_WARN("wait inc start log finish failed", K(ret), K(tablet_id_));
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      ob_delete(cb);
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::local_write_inc_redo_log(
    const ObDDLMacroBlockRedoInfo &redo_info,
    const blocksstable::MacroBlockId &macro_block_id,
    const int64_t task_id,
    ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  ObDDLRedoLog log;
  ObStoreCtxGuard ctx_guard;
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObDDLIncRedoClogCb *cb = nullptr;
  ObDDLMacroBlockRedoInfo tmp_redo_info;
  int64_t pos = 0;
  int64_t buffer_size = 0;

  if (OB_UNLIKELY(!redo_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(redo_info));
  } else if (OB_FAIL(log.init(redo_info))) {
    LOG_WARN("fail to init DDLRedoLog", K(ret), K(redo_info));
  } else if (OB_FAIL(get_write_store_ctx_guard(tx_desc, ctx_guard, ls))) {
    LOG_WARN("fail to get_write_store_ctx_guard", K(ret), K(ls_id_));
  } else if (OB_FAIL(ls->get_tablet(redo_info.table_key_.tablet_id_, tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet_handle failed", K(ret), K(redo_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else {
    ObDDLIncNeedStopWriteChecker checker(*tablet);
    int tmp_ret = OB_SUCCESS;
    int64_t real_sleep_us = 0;
    buffer_size = log.get_serialize_size();
    if (OB_TMP_FAIL(ObDDLCtrlSpeedHandle::get_instance().limit_and_sleep(MTL_ID(), ls_id_, buffer_size, task_id, checker, real_sleep_us))) {
      LOG_WARN("fail to limit and sleep", K(tmp_ret), K(MTL_ID()), K(task_id), K(ls_id_), K(buffer_size), K(real_sleep_us));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cb = OB_NEW(ObDDLIncRedoClogCb, ObMemAttr(MTL_ID(), "DDL_IRLW")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (FALSE_IT(buffer_size = redo_info.get_serialize_size())) {
  } else if (OB_FAIL(redo_info.serialize(buffer_, buffer_size, pos))) {
    LOG_WARN("fail to seriaize ddl redo log", K(ret));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(tmp_redo_info.deserialize(buffer_, buffer_size, pos))) {
    LOG_WARN("fail to deserialize ddl redo log", K(ret));
  } else if (OB_FAIL(cb->init(ls_id_, tmp_redo_info, macro_block_id, tablet_handle))) {
    LOG_WARN("init ddl clog callback failed", K(ret));
  } else {
    ObPartTransCtx *trans_ctx = nullptr;
    ObRandom rand;
    int64_t replay_hint = rand.get();
    SCN scn;
    if (OB_ISNULL(trans_ctx = ctx_guard.get_store_ctx().mvcc_acc_ctx_.tx_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("trans_ctx should not be null", K(ret));
    } else if (OB_FAIL(trans_ctx->submit_direct_load_inc_redo_log(log, cb, replay_hint, scn))) {
      LOG_WARN("fail to submit ddl inc redo log", K(ret), K(log));
    } else {
      ddl_inc_log_handle_.cb_ = cb;
      cb = nullptr;
      ddl_inc_log_handle_.scn_ = scn;
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      ob_delete(cb);
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::local_write_inc_commit_log(
    ObDDLIncCommitLog &log,
    ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  ObStoreCtxGuard ctx_guard;
  ObLS *ls = nullptr;
  ObTabletID lob_meta_tablet_id = log.get_log_basic().get_lob_meta_tablet_id();
  ObDDLIncCommitClogCb *cb = nullptr;
  ObPartTransCtx *trans_ctx = nullptr;
  SCN base_scn = SCN::min_scn();
  SCN scn = SCN::min_scn();
  bool is_external_consistent = false;
  ObDDLIncLogHandle handle;

  if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(log));
  } else if (OB_FAIL(get_write_store_ctx_guard(tx_desc, ctx_guard, ls))) {
    LOG_WARN("fail to get_write_store_ctx_guard", K(ret), K(ls_id_));
  } else if (OB_ISNULL(cb = OB_NEW(ObDDLIncCommitClogCb, ObMemAttr(MTL_ID(), "DDL_IRLW")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_FAIL(cb->init(ls_id_, log.get_log_basic()))) {
    LOG_WARN("failed to init cb", K(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(MTL_ID(), ObDDLIncLogHandle::DDL_INC_LOG_TIMEOUT, base_scn, is_external_consistent))) {
    LOG_WARN("fail to get gts sync", K(ret), K(log));
  } else if (OB_ISNULL(trans_ctx = ctx_guard.get_store_ctx().mvcc_acc_ctx_.tx_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trans_ctx should not be null", K(ret));
  } else if (OB_FAIL(trans_ctx->submit_direct_load_inc_commit_log(log, cb, scn))) {
    LOG_WARN("fail to submit ddl inc commit log", K(ret), K(log));
  } else {
    bool need_retry = true;
    while (need_retry) {
      if (OB_FAIL(OB_TS_MGR.wait_gts_elapse(MTL_ID(), scn))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("fail to wait gts elapse", K(ret), K(log));
        } else {
          ob_usleep(1000);
        }
      } else {
        need_retry = false;
      }
    }
    {
      common::ObTimeGuard timeguard("write_inc_commit_log wait", 5 * 1000 * 1000); // 5s
      if (OB_SUCC(ret)) {
        handle.cb_ = cb;
        cb = nullptr;
        if (OB_FAIL(handle.wait())) {
          LOG_WARN("wait inc commit log finish failed", K(ret), K(tablet_id_));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != cb) {
      ob_delete(cb);
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::retry_remote_write_inc_commit_log(
    const ObTabletID lob_meta_tablet_id,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  int retry_cnt = 0;
  const int64_t MAX_REMOTE_WRITE_RETRY_CNT = 800;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(switch_to_remote_write())) {
      LOG_WARN("flush ls leader location failed", K(ret));
    } else if (OB_FAIL(remote_write_inc_commit_log(lob_meta_tablet_id, tx_desc))) {
      if (OB_NOT_MASTER == ret && retry_cnt++ < MAX_REMOTE_WRITE_RETRY_CNT) {
        ob_usleep(10 * 1000); // 10 ms.
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("remote write ddl inc commit log failed", K(ret), K_(leader_ls_id), K_(leader_addr));
      }
    } else {
      break;
    }
  }

  return ret;
}

int ObDDLIncRedoLogWriter::remote_write_inc_commit_log(
    const ObTabletID lob_meta_tablet_id,
    transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  ObSrvRpcProxy *srv_rpc_proxy = GCTX.srv_rpc_proxy_;

  if (OB_ISNULL(srv_rpc_proxy)) {
    ret = OB_ERR_SYS;
    LOG_WARN("srv rpc proxy or location service is null", K(ret), KP(srv_rpc_proxy));
  } else {
    obrpc::ObRpcRemoteWriteDDLIncCommitLogArg arg;
    obrpc::ObRpcRemoteWriteDDLIncCommitLogRes res;
    if (OB_FAIL(arg.init(MTL_ID(), leader_ls_id_, tablet_id_, lob_meta_tablet_id, tx_desc))) {
      LOG_WARN("fail to init ObRpcRemoteWriteDDLIncCommitLogArg", K(ret));
    } else if (OB_FAIL(srv_rpc_proxy->to(leader_addr_).by(MTL_ID()).remote_write_ddl_inc_commit_log(arg, res))) {
      LOG_WARN("remote write inc commit log failed", K(ret), K_(leader_ls_id), K_(leader_addr));
    } else if (OB_FAIL(MTL(ObTransService *)->add_tx_exec_result(*arg.tx_desc_, res.tx_result_))) {
      LOG_WARN("fail to get_tx_exec_result", K(ret), K(*arg.tx_desc_));
    }
  }

  return ret;
}

ObDDLIncRedoLogWriterCallback::ObDDLIncRedoLogWriterCallback()
  : is_inited_(false),
    redo_info_(),
    macro_block_id_(),
    block_type_(ObDDLMacroBlockType::DDL_MB_INVALID_TYPE),
    table_key_(),
    task_id_(0),
    data_format_version_(0),
    direct_load_type_(DIRECT_LOAD_INVALID),
    tx_desc_(nullptr),
    trans_id_()
{
}

ObDDLIncRedoLogWriterCallback::~ObDDLIncRedoLogWriterCallback()
{
  (void)wait();
}

int ObDDLIncRedoLogWriterCallback::init(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObDDLMacroBlockType block_type,
    const ObITable::TableKey &table_key,
    const int64_t task_id,
    const share::SCN &start_scn,
    const uint64_t data_format_version,
    const ObDirectLoadType direct_load_type,
    ObTxDesc *tx_desc,
    const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || block_type == DDL_MB_INVALID_TYPE ||
                         !table_key.is_valid() || task_id == 0 || data_format_version < 0 ||
                         !is_valid_direct_load(direct_load_type) || OB_ISNULL(tx_desc) || !trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(block_type), K(table_key), K(task_id), K(data_format_version),
        K(direct_load_type), KP(tx_desc), K(trans_id));
  } else if (OB_FAIL(ddl_inc_writer_.init(ls_id, tablet_id))) {
    LOG_WARN("fail to init ddl_inc_writer_", K(ret), K(ls_id), K(tablet_id));
  } else {
    block_type_ = block_type;
    table_key_ = table_key;
    task_id_ = task_id;
    start_scn_ = start_scn;
    data_format_version_ = data_format_version;
    direct_load_type_ = direct_load_type;
    tx_desc_ = tx_desc;
    trans_id_ = trans_id;
    is_inited_ = true;
  }

  return ret;
}

void ObDDLIncRedoLogWriterCallback::reset()
{
  is_inited_ = false;
  redo_info_.reset();
  macro_block_id_.reset();
  ddl_inc_writer_.reset();
  block_type_ = ObDDLMacroBlockType::DDL_MB_INVALID_TYPE;
  table_key_.reset();
  task_id_ = 0;
  start_scn_.reset();
  data_format_version_ = 0;
  direct_load_type_ = DIRECT_LOAD_INVALID;
  tx_desc_ = nullptr;
  trans_id_.reset();
}

int ObDDLIncRedoLogWriterCallback::write(
    ObMacroBlockHandle &macro_handle,
    const ObLogicMacroBlockId &logic_id,
    char *buf,
    const int64_t buf_len,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIncRedoLogWriterCallback is not inited", K(ret));
  } else if (OB_UNLIKELY(!macro_handle.is_valid() || !logic_id.is_valid() || nullptr == buf || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_handle), K(logic_id), KP(buf), K(row_count));
  } else if (OB_FAIL(macro_handle.wait())) {
    STORAGE_LOG(WARN, "macro block writer fail to wait io finish", K(ret));
  } else {
    macro_block_id_ = macro_handle.get_macro_id();
    redo_info_.table_key_ = table_key_;
    redo_info_.data_buffer_.assign(buf, buf_len);
    redo_info_.block_type_ = block_type_;
    redo_info_.logic_id_ = logic_id;
    redo_info_.start_scn_ = start_scn_;
    redo_info_.data_format_version_ = data_format_version_;
    redo_info_.type_ = direct_load_type_;
    redo_info_.trans_id_ = trans_id_;
    if (OB_FAIL(ddl_inc_writer_.write_inc_redo_log_with_retry(redo_info_, macro_block_id_, task_id_, tx_desc_))) {
      LOG_WARN("write ddl inc redo log fail", K(ret));
    }
  }
  return ret;
}

int ObDDLIncRedoLogWriterCallback::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIncRedoLogWriterCallback is not inited", K(ret));
  } else if (OB_FAIL(ddl_inc_writer_.wait_inc_redo_log_finish())) {
    LOG_WARN("fail to wait inc redo log finish", K(ret), K(table_key_), K(macro_block_id_));
  }
  return ret;
}
