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

#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/ob_i_store.h"
#include "storage/ob_relative_table.h"

#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_tx_replay_executor.h"
#include "storage/tx/ob_timestamp_service.h"
#include "storage/tx/ob_trans_id_service.h"
#include "storage/tablelock/ob_lock_memtable.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace share;
using namespace memtable;
namespace transaction
{

int ObTxReplayExecutor::execute(storage::ObLS *ls,
                                ObLSTxService *ls_tx_srv,
                                const char *buf,
                                const int64_t size,
                                const int skip_pos,
                                const palf::LSN &lsn,
                                const SCN &log_timestamp,
                                const int64_t &replay_hint,
                                const ObLSID &ls_id,
                                const int64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  ObTxReplayExecutor replay_executor(ls, ls_tx_srv, lsn, log_timestamp);
  if (OB_ISNULL(ls) || OB_ISNULL(ls_tx_srv) || OB_ISNULL(buf) || size <= 0
      || !log_timestamp.is_valid() || !lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invaild arguments", K(replay_executor), K(buf), K(size));
  } else if (OB_FAIL(replay_executor.do_replay_(buf,
                                                size,
                                                skip_pos,
                                                replay_hint,
                                                ls_id,
                                                tenant_id))) {
    TRANS_LOG(WARN, "replay_executor.do_replay failed",
        K(replay_executor), K(buf), K(size), K(skip_pos), K(replay_hint), K(ls_id), K(tenant_id));
  } else {
    if (log_timestamp <= ls->get_ls_wrs_handler()->get_ls_weak_read_ts()) {
      SCN min_log_service_scn;
      // check max decided scn
      ls->get_max_decided_scn(min_log_service_scn);
      int tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected log timestamp and weak read ts", "ret", tmp_ret,
                                                                    K(replay_executor),
                                                                    K(buf),
                                                                    K(size),
                                                                    K(skip_pos),
                                                                    K(replay_hint),
                                                                    K(min_log_service_scn),
                                                                    K(ls_id),
                                                                    K(tenant_id));
    }
  }
  return ret;
}

int ObTxReplayExecutor::do_replay_(const char *buf,
                                   const int64_t size,
                                   const int skip_pos,
                                   const int64_t &replay_hint,
                                   const share::ObLSID &ls_id,
                                   const int64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invaild arguments", KPC(this), K(buf), K(size));
  } else if (OB_SUCC(prepare_replay_(buf, size, skip_pos))) {
    ObTxLogHeader header;
    first_created_ctx_ = false;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(try_get_tx_ctx_(
                                  log_block_header_.get_tx_id(),
                                  tenant_id,
                                  ls_id))) {
        TRANS_LOG(WARN, "try get tx ctx failed", K(ret), K(replay_hint), K(log_block_header_));
      } else if (OB_ISNULL(ctx_)) {
        if (OB_FAIL(log_block_.get_next_log(header))) {

          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            TRANS_LOG(WARN, "[Replay Tx] get_next_log error in replay_buf", KPC(this));
          }
        }
      } else {
        if (OB_FAIL(ctx_->iter_next_log_for_replay(log_block_, header, log_ts_ns_))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            TRANS_LOG(WARN, "[Replay Tx] get_next_log error in replay_buf", KPC(this));
          }
        }
      }

      if(OB_FAIL(ret)) {
          //do nothing
      } else if (ctx_ != nullptr
               && OB_FAIL(ctx_->validate_replay_log_entry_no(first_created_ctx_,
                                                             log_block_header_.get_log_entry_no(),
                                                             log_ts_ns_))) {
        TRANS_LOG(WARN, "validate log entry no failed", K(ret), K(header), K(first_created_ctx_),
                  K(log_block_header_), K(log_ts_ns_), K(ls_id), K(replay_hint));
      } else {
        tx_part_log_no_ += 1; // mark the replaying log_no in same log_ts

        switch (header.get_tx_log_type()) {
        case ObTxLogType::TX_REDO_LOG: {
          if (OB_FAIL(before_replay_redo_())) {
            TRANS_LOG(WARN, "[Replay Tx] start replay redo log failed", K(ret));
          } else if (!can_replay()) {
            ret = OB_STATE_NOT_MATCH;
            TRANS_LOG(ERROR, "can not replay tx log", K(ret), K(header), K(replay_hint), K(tenant_id), K(ls_id));
          } else if (OB_FAIL(replay_redo_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay redo log error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_ROLLBACK_TO_LOG: {
          if (OB_FAIL(replay_rollback_to_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay rollbackTo log error", KR(ret));
          }
          break;
        }
        case ObTxLogType::TX_ACTIVE_INFO_LOG: {
          if (OB_FAIL(replay_active_info_())) {
            TRANS_LOG(WARN, "replay active_state error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_COMMIT_INFO_LOG: {
          if (OB_FAIL(replay_commit_info_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay commit info log error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_PREPARE_LOG: {
          if (OB_FAIL(replay_prepare_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay prepare log error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_COMMIT_LOG: {
          if (OB_FAIL(replay_commit_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay commit log error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_ABORT_LOG: {
          if (OB_FAIL(replay_abort_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay abort log error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_CLEAR_LOG: {
          if (OB_FAIL(replay_clear_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay clear log error", K(ret));
          }
          break;
        }
        case ObTxLogType::TX_START_WORKING_LOG: {
          if (OB_FAIL(replay_start_working_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay clear log error", KR(ret));
          }
          break;
        }
        case ObTxLogType::TX_MULTI_DATA_SOURCE_LOG: {
          if (OB_FAIL(replay_multi_source_data_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay multi source data log error", KR(ret));
          }
          break;
        }
        case ObTxLogType::TX_RECORD_LOG: {
          if (OB_FAIL(replay_record_())) {
            TRANS_LOG(WARN, "[Replay Tx] replay record log error", KR(ret));
          }
          break;
        }
        case ObTxLogType::TX_BIG_SEGMENT_LOG: {
          if (OB_FAIL(ctx_->replay_one_part_of_big_segment(lsn_, log_ts_ns_, tx_part_log_no_))) {
            TRANS_LOG(WARN, "[Replay Tx] replay big segment log error", KR(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "[Replay Tx] Unknown Log Type in replay buf",
                    K(header.get_tx_log_type()), KPC(this));
        }
        }
      }
      TRANS_LOG(DEBUG, "[Replay Tx] Replay One Tx Log", K(header.get_tx_log_type()), K(ret));
    }
    finish_replay_(ret);
    rewrite_replay_retry_code_(ret);
  }
  return ret;
}

int ObTxReplayExecutor::prepare_replay_(const char *buf, const int64_t &size, const int skip_pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_block_.init(buf, size, skip_pos, log_block_header_))) {
    TRANS_LOG(ERROR, "TxLogBlock init error", K(log_block_), K(log_block_header_));
  }
  return ret;
}

int ObTxReplayExecutor::try_get_tx_ctx_(int64_t tx_id, int64_t tenant_id, const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;

  ObTransID trans_id(tx_id);
  // replay ls log without part_ctx
  if (ctx_ != nullptr) {
    first_created_ctx_ = false;
  } else if (trans_id.is_valid() && nullptr == ctx_) {

    if (OB_FAIL(ls_tx_srv_->get_tx_ctx(tx_id, true, ctx_)) && OB_TRANS_CTX_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "[Replay Tx] get tx ctx from ctx_mgr failed", K(ret), K(tx_id), KP(ctx_));
    } else if (OB_TRANS_CTX_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      bool tx_ctx_existed = false;
      common::ObAddr scheduler = log_block_header_.get_scheduler();
      ObTxCreateArg arg(true, /* for_replay */
                        false, /* for_special_tx */
                        tenant_id,
                        tx_id,
                        ls_id,
                        log_block_header_.get_org_cluster_id(),
                        GET_MIN_CLUSTER_VERSION(),
                        0, /*session_id*/
                        scheduler,
                        INT64_MAX,         /*trans_expired_time_*/
                        ls_tx_srv_->get_trans_service());
      if (OB_FAIL(ls_tx_srv_->create_tx_ctx(arg, tx_ctx_existed, ctx_))) {
        TRANS_LOG(WARN, "get_tx_ctx error", K(ret), K(tx_id), KP(ctx_));
      } else {
        first_created_ctx_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      ctx_->push_repalying_log_ts(log_ts_ns_);
    }
  }

  return ret;
}

int ObTxReplayExecutor::before_replay_redo_()
{
  int ret = OB_SUCCESS;
  if (!has_redo_) {
    if (OB_ISNULL(ctx_) || OB_ISNULL(mt_ctx_ = ctx_->get_memtable_ctx())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (mt_ctx_->replay_begin(log_ts_ns_)) {
      TRANS_LOG(ERROR, "[Replay Tx] replay_begin fail or mt_ctx_ is NULL", K(ret), K(mt_ctx_));
    } else {
      has_redo_ = true;
    }
  }
  return ret;
}

void ObTxReplayExecutor::finish_replay_(const int retcode)
{
  if (has_redo_) {
    if (OB_SUCCESS != retcode) {
      mt_ctx_->replay_end(false, /*is_replay_succ*/
                          log_ts_ns_);
      TRANS_LOG_RET(WARN, OB_EAGAIN, "[Replay Tx]Tx Redo replay error, rollback to start", K(*this));
    } else {
      mt_ctx_->replay_end(true, /*is_replay_succ*/
                          log_ts_ns_);
      // TRANS_LOG(INFO, "[Replay Tx] Tx Redo replay success, commit sub_trans", K(*this));
    }
  }

  if (nullptr != ctx_) {
    if (OB_SUCCESS == retcode) {
      ctx_->push_replayed_log_ts(log_ts_ns_, lsn_);
    }
    if (OB_SUCCESS != retcode) {
      ctx_->print_trace_log();
    }
    ls_tx_srv_->revert_tx_ctx(ctx_);
  }
}

bool ObTxReplayExecutor::can_replay() const
{
  return NULL != mt_ctx_ && mt_ctx_->is_for_replay();
}

int ObTxReplayExecutor::replay_redo_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxRedoLogTempRef temp_ref;
  ObTxRedoLog redo_log(temp_ref);

  if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Replay Tx] ls should not be null", K(ret), K(ls_));
  } else if (OB_FAIL(log_block_.deserialize_log_body(redo_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(redo_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(replay_redo_in_memtable_(redo_log))) {
    TRANS_LOG(WARN, "[Replay Tx] replay redo in memtable error", K(ret), K(lsn_), K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_redo_in_ctx(redo_log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay redo in tx_ctx error", K(ret), K(lsn_), K(log_ts_ns_));
  // } else if (first_created_ctx_ && redo_log.get_log_no() > 0) {
  //   // replay a commited tx in recovery process
  //   ctx_->force_no_need_replay_checksum();
  //   TRANS_LOG(
  //       WARN,
  //       "[Replay Tx] Don't replay from first redo log and Part_ctx is not existed in tx ctx table",
  //       K(first_created_ctx_), K(redo_log.get_log_no()));
    // ctx_->supplement_undo_actions_if_exist();
  }

  if (OB_SUCC(ret) && OB_TMP_FAIL(mt_ctx_->remove_callbacks_for_fast_commit())) {
    TRANS_LOG(WARN, "[Replay Tx] remove callbacks for fast commit", K(ret), K(tmp_ret),
              K(lsn_), K(log_ts_ns_), K(*mt_ctx_));
  }

  return ret;
}

int ObTxReplayExecutor::replay_rollback_to_()
{
  int ret = OB_SUCCESS;
  ObTxRollbackToLog log;
  if (OB_FAIL(log_block_.deserialize_log_body(log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", KR(ret), "log_type", "RollbackTo",
              K(lsn_), K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_rollback_to(log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay rollback_to in tx_ctx error", KR(ret), K(lsn_),
              K(log_ts_ns_));
  }
  return ret;
}

int ObTxReplayExecutor::replay_active_info_()
{
  int ret = OB_SUCCESS;
  ObTxActiveInfoLogTempRef temp_ref;
  ObTxActiveInfoLog active_info_log(temp_ref);
  if (OB_FAIL(log_block_.deserialize_log_body(active_info_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(active_info_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(
                 ctx_->replay_active_info(active_info_log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay active_info in tx_ctx error", K(ret), K(lsn_),
              K(log_ts_ns_));
  }
  return ret;
}

int ObTxReplayExecutor::replay_start_working_()
{
  int ret = OB_SUCCESS;
  ObTxStartWorkingLogTempRef temp_ref;
  ObTxStartWorkingLog start_working_log(temp_ref);
  if (OB_FAIL(log_block_.deserialize_log_body(start_working_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", KR(ret), K(start_working_log),
              K(lsn_), K(log_ts_ns_));
  } else if (OB_FAIL(ls_tx_srv_->replay_start_working_log(start_working_log, log_ts_ns_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay active_info in tx_ctx error", KR(ret), K(lsn_),
              K(log_ts_ns_));
  }
  return ret;
}

int ObTxReplayExecutor::replay_multi_source_data_()
{
  int ret = OB_SUCCESS;
  ObTxMultiDataSourceLog log;
  if (OB_FAIL(log_block_.deserialize_log_body(log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", KR(ret), K(lsn_), K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_multi_data_source(log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay multi source data in tx_ctx error", KR(ret), K(lsn_),
              K(log_ts_ns_));
  }
  return ret;
}

int ObTxReplayExecutor::replay_record_()
{
  int ret = OB_SUCCESS;
  ObTxRecordLogTempRef temp_ref;
  ObTxRecordLog log(temp_ref);
  if (OB_FAIL(log_block_.deserialize_log_body(log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", KR(ret), K(log), K(lsn_), K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_record(log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay record log in tx_ctx error", KR(ret), K(lsn_),
              K(log_ts_ns_));
  }
  return ret;
}

int ObTxReplayExecutor::replay_commit_info_()
{
  int ret = OB_SUCCESS;
  ObTxCommitInfoLogTempRef temp_ref;
  ObTxCommitInfoLog commit_info_log(temp_ref);

  if (OB_FAIL(log_block_.deserialize_log_body(commit_info_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(commit_info_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(
                 ctx_->replay_commit_info(commit_info_log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay commit_info in tx_ctx error", K(ret), K(lsn_),
              K(log_ts_ns_));
  }

  return ret;
}

int ObTxReplayExecutor::replay_prepare_()
{
  int ret = OB_SUCCESS;
  ObTxPrepareLogTempRef temp_ref;
  ObTxPrepareLog prepare_log(temp_ref);

  if (OB_FAIL(log_block_.deserialize_log_body(prepare_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(prepare_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_prepare(prepare_log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay prepare in tx_ctx error", K(ret), K(lsn_), K(log_ts_ns_));
  }

  return ret;
}

int ObTxReplayExecutor::replay_commit_()
{
  int ret = OB_SUCCESS;
  ObTxCommitLogTempRef temp_ref;
  ObTxCommitLog commit_log(temp_ref);
  SCN replay_compact_version = ls_tx_srv_->get_ls_weak_read_ts();
  if (OB_FAIL(log_block_.deserialize_log_body(commit_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(commit_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_commit(commit_log,
                                         lsn_,
                                         log_ts_ns_,
                                         tx_part_log_no_,
                                         replay_compact_version))) {
    TRANS_LOG(WARN, "[Replay Tx] replay commit in tx_ctx error", K(ret), K(lsn_), K(log_ts_ns_));
  }

  return ret;
}

int ObTxReplayExecutor::replay_abort_()
{
  int ret = OB_SUCCESS;
  ObTxAbortLogTempRef temp_ref;
  ObTxAbortLog abort_log(temp_ref);

  if (OB_FAIL(log_block_.deserialize_log_body(abort_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(abort_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_abort(abort_log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay abort in tx_ctx error", K(ret), K(lsn_), K(log_ts_ns_));
  }

  return ret;
}

int ObTxReplayExecutor::replay_clear_()
{
  int ret = OB_SUCCESS;
  ObTxClearLogTempRef temp_ref;
  ObTxClearLog clear_log(temp_ref);

  if (OB_FAIL(log_block_.deserialize_log_body(clear_log))) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize log body error", K(ret), K(clear_log), K(lsn_),
              K(log_ts_ns_));
  } else if (OB_FAIL(ctx_->replay_clear(clear_log, lsn_, log_ts_ns_, tx_part_log_no_))) {
    TRANS_LOG(WARN, "[Replay Tx] replay clear in tx_ctx error", K(ret), K(lsn_), K(log_ts_ns_));
  }

  return ret;
}

int ObTxReplayExecutor::replay_redo_in_memtable_(ObTxRedoLog &redo)
{
  int ret = OB_SUCCESS;
  // ObMemtable *cur_mem = nullptr;
  common::ObTimeGuard timeguard("replay_redo_in_memtable", 10 * 1000);
  int64_t pos = 0;

  const int64_t start_us = ObTimeUtility::current_time();
  const bool for_replay = true;

  ObMutatorRowHeader row_head;
  uint8_t meta_flag = 0;

  ObCLogEncryptInfo encrypt_info;
  encrypt_info.init();

  if (OB_ISNULL(mmi_ptr_)) {
    if (nullptr
        == (mmi_ptr_ = static_cast<ObMemtableMutatorIterator *>(
                ob_malloc(sizeof(ObMemtableMutatorIterator), "TxReplay")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "[Replay Tx] alloc memtable mutator iterator failed", K(ret));
    } else {
      new (mmi_ptr_) ObMemtableMutatorIterator();
    }
  } else {
    mmi_ptr_->reset();
  }

  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(mmi_ptr_->deserialize(redo.get_replay_mutator_buf(), redo.get_mutator_size(),
                                           pos, encrypt_info))
             || redo.get_mutator_size() != pos) {
    TRANS_LOG(WARN, "[Replay Tx] deserialize fail or pos does not match data_len", K(ret));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(encrypt_info.decrypt_table_key())) {
    TRANS_LOG(WARN, "[Replay Tx] failed to decrypt table key", K(ret));
#endif
  } else {
    meta_flag = mmi_ptr_->get_meta().get_flags();
    ObEncryptRowBuf row_buf;
    while (OB_SUCC(ret)) {
      row_head.reset();
      if (OB_FAIL(mmi_ptr_->iterate_next_row(row_buf, encrypt_info))) {
        if (OB_ITER_END != ret) {
          TRANS_LOG(WARN, "[Replay Tx]  iterate_next_row failed", K(ret));
        }
      } else if (FALSE_IT(row_head = mmi_ptr_->get_row_head())) {
        // do nothing
      } else if (OB_FAIL(replay_one_row_in_memtable_(row_head, mmi_ptr_))) {
        if (OB_MINOR_FREEZE_NOT_ALLOW == ret) {
          if (TC_REACH_TIME_INTERVAL(1000 * 1000)) {
            TRANS_LOG(WARN, "[Replay Tx] cannot create more memtable", K(ret),
                      K(row_head.tablet_id_), KP(ls_), K(log_ts_ns_), K(tx_part_log_no_),
                      KPC(ctx_));
          }
        } else {
          TRANS_LOG(WARN, "[Replay Tx] replay_one_row_in_memtable_ failed", K(ret),
                    K(row_head.tablet_id_), KP(ls_), K(log_ts_ns_), K(tx_part_log_no_),
                    KPC(ctx_));
        }
      } else {
        // do nothing
      }
    }
  }

  ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
  // free ObRowKey's objs's memory
  THIS_WORKER.get_sql_arena_allocator().reset();

  if(timeguard.get_diff()> 10*1000)
  {
    TRANS_LOG(INFO,
              "[Replay Tx] Replay redo in MemTable cost too much time",
              K(ret),
              K(timeguard.get_diff()),
              K(log_ts_ns_),
              K(ctx_->get_trans_id()),
              K(ctx_->get_ls_id()),
              K(mvcc_row_count_),
              K(table_lock_row_count_));
  }
  return ret;
}

int ObTxReplayExecutor::replay_one_row_in_memtable_(ObMutatorRowHeader &row_head,
                                                    memtable::ObMemtableMutatorIterator *mmi_ptr)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode mode;
  ObTabletHandle tablet_handle;
  const bool is_update_mds_table = false;

  if (OB_FAIL(ls_->replay_get_tablet(row_head.tablet_id_, log_ts_ns_, is_update_mds_table, tablet_handle))) {
    if (OB_OBSOLETE_CLOG_NEED_SKIP == ret) {
      ctx_->force_no_need_replay_checksum();
      ret = OB_SUCCESS;
      TRANS_LOG(WARN, "[Replay Tx] tablet gc, skip this log entry", K(ret), K(row_head.tablet_id_),
                KP(ls_), K(log_ts_ns_), K(tx_part_log_no_), K(ctx_));
    } else if (OB_EAGAIN == ret) {
      TRANS_LOG(INFO, "[Replay Tx] tablet not ready, retry this log entry", K(ret), K(row_head.tablet_id_),
                KP(ls_), K(log_ts_ns_), K(tx_part_log_no_), K(ctx_));
    } else {
      TRANS_LOG(INFO, "[Replay Tx] get tablet failed, retry this log entry", K(ret), K(row_head.tablet_id_),
                KP(ls_), K(log_ts_ns_), K(tx_part_log_no_), K(ctx_));
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(logservice::ObTabletReplayExecutor::replay_check_restore_status(tablet_handle, false/*update_tx_data*/))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ctx_->check_no_need_replay_checksum(log_ts_ns_);
      ret = OB_SUCCESS;
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(INFO, "[Replay Tx] Not need replay, skip this log entry", K(row_head.tablet_id_),
                  K(log_ts_ns_), K(tx_part_log_no_));
      }
    } else if (OB_EAGAIN == ret) {
      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        TRANS_LOG(INFO, "[Replay Tx] tablet not ready, retry this log entry", K(ret), K(row_head.tablet_id_),
                  K(log_ts_ns_), K(tx_part_log_no_));
      }
    } else {
      TRANS_LOG(WARN, "[Replay Tx] replay check restore status error", K(ret), K(row_head.tablet_id_),
                K(log_ts_ns_), K(tx_part_log_no_));
    }
  } else if (OB_FAIL(get_compat_mode_(row_head.tablet_id_, mode))) {
    TRANS_LOG(WARN, "[Replay Tx] get compat mode error", K(ret), K(mode));
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    storage::ObStoreCtx storeCtx;
    storeCtx.ls_id_ = ctx_->get_ls_id();
    storeCtx.mvcc_acc_ctx_.init_replay(
      *ctx_,
      *mt_ctx_,
      ctx_->get_trans_id()
    );
    storeCtx.replay_log_scn_ = log_ts_ns_;
    storeCtx.tablet_id_ = row_head.tablet_id_;
    storeCtx.ls_ = ls_;

    ObRelativeTable relative_table;
    lib::CompatModeGuard compat_guard(mode);
    switch (row_head.mutator_type_) {
    case MutatorType::MUTATOR_ROW: {
      if (OB_FAIL(replay_row_(storeCtx, tablet, mmi_ptr_)) && OB_ITER_END != ret) {
        if (OB_NO_NEED_UPDATE != ret && OB_MINOR_FREEZE_NOT_ALLOW != ret) {
          TRANS_LOG(WARN, "[Replay Tx] replay row failed.", K(ret), K(mt_ctx_),
                    K(row_head.tablet_id_));
        } else if (OB_NO_NEED_UPDATE == ret) {
          ctx_->check_no_need_replay_checksum(log_ts_ns_);
          ret = OB_SUCCESS;
          TRANS_LOG(DEBUG, "[Replay Tx] Not need replay row becase of no_need_update", K(log_ts_ns_),
                    K(tx_part_log_no_), K(row_head.tablet_id_));
        }
      }
      if (OB_SUCC(ret)) {
        mvcc_row_count_++;
      }
      break;
    }
    case MutatorType::MUTATOR_TABLE_LOCK: {
      if (OB_FAIL(replay_lock_(storeCtx, tablet, mmi_ptr_)) && OB_ITER_END != ret) {
        TRANS_LOG(WARN, "[Replay Tx] replay lock failed.", K(ret), K(mt_ctx_),
                  K(row_head.tablet_id_));
      } else {
        table_lock_row_count_++;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "[Replay Tx] Unknown mutator_type", K(row_head.mutator_type_));
    } // default
    }
  }
  return ret;
}

int ObTxReplayExecutor::prepare_memtable_replay_(ObStorageTableGuard &w_guard,
                                                 ObIMemtable *&mem_ptr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(w_guard.refresh_and_protect_memtable())) {
    TRANS_LOG(WARN, "[Replay Tx] refresh and protect memtable error", K(ret));
  } else if (OB_FAIL(w_guard.get_memtable_for_replay(mem_ptr))) {
    // OB_NO_NEED_UPDATE => don't need to replay
    if (OB_NO_NEED_UPDATE != ret) {
      TRANS_LOG(WARN, "[Replay Tx] get active_memtable error", K(ret), KP(mem_ptr));
    }
  }

  return ret;
}

int ObTxReplayExecutor::replay_row_(storage::ObStoreCtx &store_ctx,
                                    ObTablet *tablet,
                                    memtable::ObMemtableMutatorIterator *mmi_ptr)
{
  int ret = OB_SUCCESS;
  common::ObTimeGuard timeguard("replay_row_in_memtable", 10 * 1000);
  ObIMemtable *mem_ptr = nullptr;
  ObMemtable *data_mem_ptr = nullptr;
  ObStorageTableGuard w_guard(tablet, store_ctx, true, true, log_ts_ns_);
  if (OB_ISNULL(mmi_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Replay Tx] invaild arguments", K(ret), KP(mmi_ptr));
  } else if (FALSE_IT(timeguard.click("start"))) {
  } else if (OB_FAIL(prepare_memtable_replay_(w_guard, mem_ptr))) {
    if (OB_NO_NEED_UPDATE != ret) {
      TRANS_LOG(WARN, "[Replay Tx] prepare for replay failed", K(ret), KP(mem_ptr), KP(mmi_ptr));
    } else {
      TRANS_LOG(DEBUG, "[Replay Tx] Not need replay row for tablet", K(log_ts_ns_),
                K(tx_part_log_no_), K(mmi_ptr->get_row_head()),
                K(tablet->get_tablet_meta().tablet_id_));
    }
    // dynamic_cast will check whether this is really a ObMemtable.
  } else if (OB_ISNULL(data_mem_ptr = static_cast<ObMemtable *>(mem_ptr))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Replay Tx] this is not a ObMemtable", K(ret), KP(mem_ptr), KPC(mem_ptr),
              KP(mmi_ptr));
  } else if (FALSE_IT(timeguard.click("get_memtable"))) {
  } else if (OB_FAIL(data_mem_ptr->replay_row(store_ctx, mmi_ptr))) {
    TRANS_LOG(WARN, "[Replay Tx] replay row error", K(ret));
  } else if (OB_FAIL(data_mem_ptr->set_max_end_scn(log_ts_ns_))) { // for freeze log_ts , may be
    TRANS_LOG(WARN, "[Replay Tx] set memtable max end log ts failed", K(ret), KP(data_mem_ptr));
  } else if (OB_FAIL(data_mem_ptr->set_rec_scn(log_ts_ns_))) {
    TRANS_LOG(WARN, "[Replay Tx] set rec_log_ts error", K(ret), KPC(data_mem_ptr));
  }

  timeguard.click("replay_finish");
  if (OB_FAIL(ret) && ret != OB_NO_NEED_UPDATE) {
    // We need rollback all callbacks of this log to avoid replay a row
    // in a freeze memtable which has a smaller end ts than this log.
    //
    // The rollback operation must hold write_ref to make memtable stay in memory.
    mt_ctx_->rollback_redo_callbacks(log_ts_ns_);
  }
  return ret;
}

int ObTxReplayExecutor::replay_lock_(storage::ObStoreCtx &store_ctx,
                                     ObTablet *tablet,
                                     memtable::ObMemtableMutatorIterator *mmi_ptr)
{
  // TODO: yanyuan.cxf lock is not encrypted.
  common::ObTimeGuard timeguard("replay_row_in_lock_memtable", 10 * 1000);
  int ret = OB_SUCCESS;
  ObTableHandleV2 handle;
  ObLockMemtable *memtable = nullptr;
  timeguard.click("start");
  if (OB_FAIL(tablet->get_active_memtable(handle))) {
    TRANS_LOG(WARN, "[Replay Tx] get active memtable failed", K(ret), K(*tablet));
  } else if (OB_FAIL(handle.get_lock_memtable(memtable))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "[Replay Tx] get lock memtable failed", K(ret), K(handle));
  } else if (FALSE_IT(timeguard.click("get_memtable"))) {
  } else if (OB_FAIL(memtable->replay_row(store_ctx, mmi_ptr))) {
    TRANS_LOG(WARN, "[Replay Tx] replay lock row error", K(ret));
  } else {
    TRANS_LOG(DEBUG, "[Replay Tx] replay row in lock memtable success", KP(memtable));
  }
  return ret;
}

int ObTxReplayExecutor::get_compat_mode_(const ObTabletID &tablet_id, lib::Worker::CompatMode &mode)
{
  int ret = OB_SUCCESS;

  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (tablet_id.is_sys_tablet()) {
    mode = lib::Worker::CompatMode::MYSQL;
  } else {
    mode = THIS_WORKER.get_compatibility_mode();
  }

  return ret;
}

void ObTxReplayExecutor::rewrite_replay_retry_code_(int &ret_code)
{
  if (ret_code == OB_MINOR_FREEZE_NOT_ALLOW || ret_code == OB_SCN_OUT_OF_BOUND ||
      ret_code == OB_ALLOCATE_MEMORY_FAILED) {
    TRANS_LOG(INFO, "rewrite replay error_code as OB_EAGAIN for retry", K(ret_code),
              K(ls_->get_ls_id()), K(log_ts_ns_));
    ret_code = OB_EAGAIN;
  }
}

}
} // namespace oceanbase

