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
 *
 * TransLogResolver: identify TransLog type and handle based on the type
 */

#define USING_LOG_PREFIX OBLOG_FETCHER

#include "ob_cdc_part_trans_resolver.h"
#include "ob_log_cluster_id_filter.h"     // ClusterIdFilter
#include "logservice/logfetcher/ob_log_part_serve_info.h"       // PartServeInfo

namespace oceanbase
{
namespace libobcdc
{

bool IObCDCPartTransResolver::test_mode_on = false;
bool IObCDCPartTransResolver::test_checkpoint_mode_on = false;
int64_t IObCDCPartTransResolver::test_mode_ignore_redo_count = 0;
IObCDCPartTransResolver::IgnoreLogType IObCDCPartTransResolver::test_mode_ignore_log_type = IObCDCPartTransResolver::IgnoreLogType::INVALID_TYPE;

// ***************  MissingLogInfo ***************** //

IObCDCPartTransResolver::MissingLogInfo::MissingLogInfo()
{
  reset();
}

IObCDCPartTransResolver::MissingLogInfo::~MissingLogInfo()
{
  reset();
}

IObCDCPartTransResolver::MissingLogInfo
&IObCDCPartTransResolver::MissingLogInfo::operator=(const IObCDCPartTransResolver::MissingLogInfo &miss_log_info)
{
  this->miss_redo_lsn_arr_ = miss_log_info.miss_redo_lsn_arr_;
  this->miss_record_or_state_log_lsn_ = miss_log_info.miss_record_or_state_log_lsn_;
  this->need_reconsume_commit_log_entry_ = miss_log_info.need_reconsume_commit_log_entry_;
  this->is_resolving_miss_log_ = miss_log_info.is_resolving_miss_log_;
  this->is_reconsuming_ = miss_log_info.is_reconsuming_;
  return *this;
}

int IObCDCPartTransResolver::MissingLogInfo::set_miss_record_or_state_log_lsn(const palf::LSN &record_log_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!record_log_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("set_miss_record_or_state_log_lsn invalid record_log_lsn", KR(ret), K(record_log_lsn));
  } else if (OB_UNLIKELY(miss_record_or_state_log_lsn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("miss_record_or_state_log_lsn already set, should not set again!", KR(ret), K(record_log_lsn), KPC(this));
  } else {
    miss_record_or_state_log_lsn_ = record_log_lsn;
  }

  return ret;
}

int IObCDCPartTransResolver::MissingLogInfo::get_miss_record_or_state_log_lsn(palf::LSN &miss_record_lsn) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!miss_record_or_state_log_lsn_.is_valid())) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    miss_record_lsn = miss_record_or_state_log_lsn_;
  }

  return ret;
}

int IObCDCPartTransResolver::MissingLogInfo::push_back_single_miss_log_lsn(const palf::LSN &misslog_lsn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!misslog_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("misslog lsn invalid", KR(ret), K(misslog_lsn));
  } else if (OB_FAIL(miss_redo_lsn_arr_.push_back(misslog_lsn))) {
    LOG_ERROR("push_back misslog lsn to missinglog_lsn_array fail", KR(ret),
        K(misslog_lsn), K_(miss_redo_lsn_arr));
  }

  return ret;
}

template <typename LSN_ARRAY>
int IObCDCPartTransResolver::MissingLogInfo::push_back_missing_log_lsn_arr(const LSN_ARRAY &misslog_lsn_arr)
{
  int ret = OB_SUCCESS;

  for(int64_t idx = 0; OB_SUCC(ret) && idx < misslog_lsn_arr.count(); idx++) {
    const palf::LSN &lsn = misslog_lsn_arr.at(idx);

    if (OB_FAIL(miss_redo_lsn_arr_.push_back(misslog_lsn_arr.at(idx)))) {
      LOG_ERROR("push_back_missing_log_lsn_arr failed", KR(ret),
          K(misslog_lsn_arr), K(idx), K(miss_redo_lsn_arr_), K(lsn));
    }
  }

  return ret;
}

int64_t IObCDCPartTransResolver::MissingLogInfo::get_total_misslog_cnt() const
{
  int64_t cnt_ret = miss_redo_lsn_arr_.count();

  if (miss_record_or_state_log_lsn_.is_valid()) {
    cnt_ret +=1;
  }

  return cnt_ret;
}

int IObCDCPartTransResolver::MissingLogInfo::sort_and_unique_missing_log_lsn()
{
  auto fn = [](palf::LSN &lsn1, palf::LSN &lsn2) { return lsn1 < lsn2; };
  return sort_and_unique_array(miss_redo_lsn_arr_, fn);
}

// ***************  ObCDCPartTransResolver public functions ***************** //

ObCDCPartTransResolver::ObCDCPartTransResolver(
    const char *tls_id_str,
    TaskPool &task_pool,
    PartTransTaskMap &task_map,
    IObLogFetcherDispatcher &dispatcher,
    IObLogClusterIDFilter &cluster_id_filter) :
    offlined_(false),
    tls_id_(),
    part_trans_dispatcher_(tls_id_str, task_pool, task_map, dispatcher),
    cluster_id_filter_(cluster_id_filter)
{}

ObCDCPartTransResolver::~ObCDCPartTransResolver()
{}


int ObCDCPartTransResolver::init(
    const logservice::TenantLSID &tls_id,
    const int64_t start_commit_version)
{
  tls_id_ = tls_id;
  return part_trans_dispatcher_.init(tls_id, start_commit_version);
}

// read log_entry content
// iterate TransLog in LogEntry and handle by TransLogType
int ObCDCPartTransResolver::read(
    const char *buf,
    const int64_t buf_len,
    const int64_t pos_after_log_header,
    const palf::LSN &lsn,
    const int64_t submit_ts,
    const logfetcher::PartServeInfo &serve_info,
    MissingLogInfo &missing_info,
    logfetcher::TransStatInfo &tsi)
{
  int ret = OB_SUCCESS;
  int pos = pos_after_log_header;
  bool is_cluster_id_served = false;
  transaction::ObTxLogBlock tx_log_block;
  transaction::ObTxLogBlockHeader tx_log_block_header;

  if (OB_FAIL(tx_log_block.init(buf, buf_len, pos, tx_log_block_header))) {
    LOG_ERROR("failed to init tx_log_block with header",
        KR(ret), K(buf_len), K_(tls_id), K(tx_log_block), K(tx_log_block_header));
  } else if (OB_UNLIKELY(!tx_log_block_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ObTxLogBlockHeader found in LogEntry", KR(ret), K_(tls_id), K(lsn), K(tx_log_block_header));
  } else if (cluster_id_filter_.check_is_served(tx_log_block_header.get_org_cluster_id(), is_cluster_id_served)) {
    LOG_ERROR("check_cluster_id_served failed", KR(ret), K_(tls_id), K(lsn), K(tx_log_block_header));
  } else if (OB_UNLIKELY(!is_cluster_id_served)) {
    LOG_DEBUG("[STAT] [FETCHER] [TRANS_NOT_SERVE]", K_(tls_id), K(is_cluster_id_served), K(lsn));
  } else {
    // has redo-like tx_log in log_entry, including
    // ObTxRedoLog/ObTxMultiDataSourceLog/ObTxRollbackToLog
    bool has_redo_in_cur_entry = false;
    int64_t tx_log_idx_in_entry = -1;

    while (OB_SUCC(ret)) {
      transaction::ObTxLogHeader tx_header;
      bool stop_resolve_cur_log_entry = false;

      if (OB_FAIL(read_trans_header_(
          lsn,
          tx_log_block_header.get_tx_id(),
          missing_info.is_resolving_miss_log(),
          tx_log_block,
          tx_header,
          tx_log_idx_in_entry,
          has_redo_in_cur_entry))) {
        if (OB_ITER_END != ret) {
          LOG_ERROR("read_trans_header_ from tx_log_block failed", KR(ret), K_(tls_id), K(lsn),
            K(tx_log_block_header), K(tx_log_block), K(tx_header), K(has_redo_in_cur_entry), K(tx_log_idx_in_entry));
        }
      } else if (need_ignore_trans_log_(
          lsn,
          tx_log_block_header.get_tx_id(),
          tx_header,
          missing_info,
          tx_log_idx_in_entry,
          stop_resolve_cur_log_entry)) {
        if (stop_resolve_cur_log_entry) {
          ret = OB_ITER_END;
        }
      } else if (OB_FAIL(read_trans_log_(
          tx_log_block_header,
          tx_log_block,
          tx_header,
          lsn,
          submit_ts,
          serve_info,
          missing_info,
          has_redo_in_cur_entry))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("read_trans_log_ fail", KR(ret), K_(tls_id), K(tx_log_block_header),
              K(tx_header), K(has_redo_in_cur_entry));
        }
      }
    }

    if (OB_ITER_END == ret) {
      if (OB_UNLIKELY(! missing_info.is_empty())) {
        // miss_log can only find while resolving record/commit_info/prepare/commit
        ret = OB_ITEM_NOT_SETTED;
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObCDCPartTransResolver::dispatch(volatile bool &stop_flag, int64_t &pending_task_count)
{
  int ret = OB_SUCCESS;

  if (offlined_) {
    LOG_INFO("log_stream has been offlined, need not flush", K_(offlined), K_(tls_id));
  } else if (OB_FAIL(part_trans_dispatcher_.dispatch_part_trans(stop_flag, pending_task_count))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("part trans dispatch fail", KR(ret), K_(tls_id), K_(part_trans_dispatcher));
    }
  } else { /* success */ }

  return ret;
}

int ObCDCPartTransResolver::offline(volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  LOG_INFO("[PART_TRANS_RESOLVER] offline", KPC(this));

  if (offlined_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("log_stream has been offlined", KR(ret), K_(offlined), K_(tls_id));
  }
  // First clear all ready and unready tasks to ensure memory reclamation
  // This operation is mutually exclusive with the dispatch operation
  else if (OB_FAIL(part_trans_dispatcher_.clean_task())) {
    LOG_ERROR("clean task fail", KR(ret), K_(tls_id));
  }
  // dispatch offline partition task
  else if (OB_FAIL(part_trans_dispatcher_.dispatch_offline_ls_task(stop_flag))) {
    if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("dispatch offline task fail", KR(ret), K_(tls_id));
    }
  } else {
    offlined_ = true;
  }

  return ret;
}

// ***************  ObCDCPartTransResolver private functions ***************** //


int ObCDCPartTransResolver::read_trans_header_(
    const palf::LSN &lsn,
    const transaction::ObTransID &tx_id,
    const bool is_resolving_miss_log,
    transaction::ObTxLogBlock &tx_log_block,
    transaction::ObTxLogHeader &tx_header,
    int64_t &tx_log_idx_in_entry,
    bool &has_redo_in_cur_entry)
{
  int ret = OB_SUCCESS;
  tx_log_idx_in_entry ++;

  if (OB_FAIL(tx_log_block.get_next_log(tx_header))) {
    if (OB_LOG_ALREADY_SPLIT == ret) {
      // need use big_segment_buf
      PartTransTask *part_trans_task = NULL;

      if (OB_UNLIKELY(transaction::ObTxLogType::TX_BIG_SEGMENT_LOG != tx_header.get_tx_log_type())) {
        ret = OB_STATE_NOT_MATCH;
        LOG_ERROR("expected TX_BIG_SEGMENT_LOG but not", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(tx_header), K(is_resolving_miss_log));
      } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
        LOG_ERROR("obtain_task_ failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(is_resolving_miss_log));
      } else if (OB_FAIL(tx_log_block.get_next_log(tx_header, part_trans_task->get_segment_buf()))) {
        if (OB_LOG_TOO_LARGE == ret) {
          // note: will change ret to OB_SUCCESS if push_fetched_log_entry success.
          if (OB_FAIL(part_trans_task->push_fetched_log_entry(lsn))) {
            LOG_ERROR("push_fetched_log_entry of BigSegmentBuf Log failed", KR(ret));
          } else {
            LOG_DEBUG("handle_big_segment_buf part done", K_(tls_id), K(tx_id), K(lsn), K(tx_header));
          }
        } else if (OB_NO_NEED_UPDATE == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("consume tx_log_block while segment_buf is collected done and not reseted", KR(ret),
              K_(tls_id), K(lsn), K(tx_id), K(tx_header));
        } else if (OB_START_LOG_CURSOR_INVALID == ret) {
          // consume tx_big_segment_log in middle of parts log.
          // 1. reset segment buf and reset ret to OB_SUCCESS
          // 2. not mark fetched_logentry_list.
          // 3. wait misslog to find all log_entry of the TX_BIG_SEGMENT_LOG
          part_trans_task->get_segment_buf()->reset();
          ret = OB_SUCCESS;
          LOG_DEBUG("found half_part of big_segment_buf tx_log, should ignore and fetch by misslog later",
              K_(tls_id), K(tx_id), K(lsn), K(tx_header));
        }
      }
    } else if (OB_ITER_END != ret) {
      LOG_ERROR("get_next_log from tx_log_block failed", KR(ret), K_(tls_id), K(lsn), K(tx_id),
          K(is_resolving_miss_log), K(tx_header), K(tx_log_idx_in_entry));
    }
  }

  if (OB_SUCC(ret)) {
    const transaction::ObTxLogType log_type = tx_header.get_tx_log_type();
    // RollbackToLog is treated as a special REDO
    // normally RollbackToLog should occupy a log_entry alone.
    const bool is_redo_like_log =
        (transaction::ObTxLogType::TX_REDO_LOG == log_type)
        || (transaction::ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type)
        || (transaction::ObTxLogType::TX_ROLLBACK_TO_LOG == log_type);
    if (is_redo_like_log) {
      if (OB_UNLIKELY(has_redo_in_cur_entry)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected only one redo_log/multi_data_source_log/rollback_to_log in one log_entry",
            KR(ret), K_(tls_id), K(tx_id), K(lsn), K(tx_header), K(tx_log_idx_in_entry));
      } else {
        has_redo_in_cur_entry = true;
      }
    }
  }

  return ret;
}

bool ObCDCPartTransResolver::need_ignore_trans_log_(
    const palf::LSN &lsn,
    const transaction::ObTransID &tx_id,
    const transaction::ObTxLogHeader &tx_header,
    const MissingLogInfo &missing_info,
    const int64_t tx_log_idx_in_entry,
    bool &stop_resolve_cur_log_entry)
{
  bool need_ignore_cur_tx_log = false;
  const char *reason = "NONE";

  if (OB_UNLIKELY(transaction::ObTxLogType::TX_BIG_SEGMENT_LOG == tx_header.get_tx_log_type())) {
    need_ignore_cur_tx_log = true;
    stop_resolve_cur_log_entry = true;
    reason = "TX_BIG_SEGMENT_LOG NOT COLLECT COMPLETE";
  } else if (missing_info.is_reconsuming()
      && ! (transaction::ObTxLogType::TX_COMMIT_LOG == tx_header.get_tx_log_type())) {
    need_ignore_cur_tx_log = true;
    reason = "NON_COMMIT_LOG WHILE RECONSUME LOG_ENTRY CONTAINS COMMIT_LOG";
  }

  if (OB_UNLIKELY(need_ignore_cur_tx_log)) {
    LOG_INFO("[IGNORE] [TX_LOG]", K(lsn), K(tx_id), K(tx_header), KCSTRING(reason),
        K(stop_resolve_cur_log_entry), K(tx_log_idx_in_entry), K(missing_info));
  }

  return need_ignore_cur_tx_log;
}

int ObCDCPartTransResolver::read_trans_log_(
    const transaction::ObTxLogBlockHeader &tx_log_block_header,
    transaction::ObTxLogBlock &tx_log_block,
    const transaction::ObTxLogHeader &tx_log_header,
    const palf::LSN &lsn,
    const int64_t submit_ts,
    const logfetcher::PartServeInfo &serve_info,
    MissingLogInfo &missing_info,
    bool &has_redo_in_cur_entry)
{
  int ret = OB_SUCCESS;
  // tx_log_block_header is valid(check by ObCDCPartTransResolver::read)
  const int64_t cluster_id = tx_log_block_header.get_org_cluster_id();
  const transaction::ObTransID &tx_id = tx_log_block_header.get_tx_id();
  const bool handling_miss_log = missing_info.is_resolving_miss_log();
  const transaction::ObTxLogType log_type = tx_log_header.get_tx_log_type();

  switch (log_type) {
    case transaction::ObTxLogType::TX_REDO_LOG:
    {
      if (OB_FAIL(handle_redo_(tx_id, lsn, submit_ts, handling_miss_log, tx_log_block))) {
        if (OB_IN_STOP_STATE != ret) {
          LOG_ERROR("handle_redo_ fail", KR(ret), K_(tls_id), K(tx_id), K(tx_id), K(lsn), K(tx_log_header),
              K(missing_info));
        }
      }
      break;
    }
    case transaction::ObTxLogType::TX_MULTI_DATA_SOURCE_LOG:
    {
      if (OB_FAIL(handle_multi_data_source_log_(tx_id, lsn, handling_miss_log, tx_log_block))) {
        LOG_ERROR("handle_multi_data_source_log_ failed", KR(ret), K_(tls_id), K(tx_id), K(lsn),
            K(handling_miss_log));
      }
      break;
    }
    case transaction::ObTxLogType::TX_RECORD_LOG:
    {
      if (OB_FAIL(handle_record_(tx_id, lsn, missing_info, tx_log_block))) {
        LOG_ERROR("handle_record_ fail", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(tx_log_header),
            K(missing_info));
      }
      break;
    }
    case transaction::ObTxLogType::TX_ROLLBACK_TO_LOG:
    {
      if (OB_FAIL(handle_rollback_to_(tx_id, lsn, handling_miss_log, tx_log_block))) {
        LOG_ERROR("handle_rollback_to_ failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(tx_log_header),
            K(handling_miss_log));
      }
      break;
    }
    case transaction::ObTxLogType::TX_COMMIT_INFO_LOG:
    {
      if (OB_FAIL(handle_commit_info_(tx_id, lsn, submit_ts, has_redo_in_cur_entry, missing_info, tx_log_block))) {
        LOG_ERROR("handle_commit_info_ fail", KR(ret), K_(tls_id), K(tx_id), K(lsn),  K(tx_log_header),
            K(submit_ts), K(has_redo_in_cur_entry), K(missing_info));
      }
      break;
    }
    case transaction::ObTxLogType::TX_PREPARE_LOG:
    {
      if (OB_FAIL(handle_prepare_(tx_id, lsn, submit_ts, missing_info, tx_log_block))) {
        LOG_ERROR("handle_prepare_ fail", KR(ret), K_(tls_id), K(tx_id), K(tx_log_header), K(missing_info));
      }
      break;
    }
    case transaction::ObTxLogType::TX_COMMIT_LOG:
    {
      bool is_trans_served = true;
      if (OB_FAIL(handle_commit_(
          cluster_id,
          tx_id,
          lsn,
          submit_ts,
          serve_info,
          missing_info,
          tx_log_block,
          is_trans_served))) {
        if (OB_NEED_RETRY == ret) {
          LOG_INFO("handle_commit_ need retry after missing_info is handled", KR(ret),
              K_(tls_id), K(tx_id), K(missing_info));
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("handle_commit_ fail", KR(ret), K_(tls_id), K(tx_id), K(tx_log_header),
              K(lsn), K(submit_ts), K(is_trans_served), K(missing_info));
        }
      }
      break;
    }
    case transaction::ObTxLogType::TX_ABORT_LOG:
    {
      if (OB_FAIL(handle_abort_(tx_id, handling_miss_log, tx_log_block))) {
        LOG_ERROR("handle_abort_ fail", KR(ret), K_(tls_id), K(tx_id), K(tx_log_header));
      } else{
        missing_info.reset();
      }
      break;
    }
    default:
    {
      LOG_DEBUG("ignore_tx_log", K_(tls_id), K(tx_id), K(lsn), K(tx_log_header), K(submit_ts));
      break;
    }
  }

  LOG_DEBUG("resolver_read_tx_log", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(submit_ts), K(tx_log_header), K(handling_miss_log), "lbt", lbt());

  return ret;
}

// TODO:
// 1. modify sorted_redo_list:
//   (1) modify order rule: LSN(LogEntryNode)
//        otherwise sort will cost too much in big_trans case, which contains too much redo node
int ObCDCPartTransResolver::handle_redo_(
    const transaction::ObTransID &tx_id,
    const palf::LSN &lsn,
    const int64_t submit_ts,
    const bool handling_miss_log,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  transaction::ObTxRedoLogTempRef tmp_ref;
  transaction::ObTxRedoLog redo_log(tmp_ref);
  PartTransTask *task = NULL;

  if (OB_FAIL(tx_log_block.deserialize_log_body(redo_log))) {
    LOG_ERROR("deserialize_redo_log_body failed", KR(ret), K_(tls_id), K(tx_id), K(lsn));
  } else if (OB_UNLIKELY(redo_log.get_mutator_size() <= 0)) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid mutator size", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(submit_ts), K(redo_log));
  } else if (OB_FAIL(obtain_task_(tx_id, task, handling_miss_log))) {
    LOG_ERROR("obtain_task_ fail", KR(ret), K_(tls_id), K(tx_id), K(lsn),
        K(handling_miss_log));
  } else if (OB_FAIL(push_fetched_log_entry_(lsn, *task))) {
    if (OB_ENTRY_EXIST == ret) {
      LOG_WARN("redo already fetched, ignore", KR(ret), K_(tls_id), K(tx_id), K(lsn),
          "task_sorted_log_entry_info", task->get_sorted_log_entry_info());
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("push_fetched_log_entry failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), KPC(task));
    }
  } else if (OB_FAIL(task->push_redo_log(
      tx_id,
      lsn,
      submit_ts,
      redo_log.get_replay_mutator_buf(),
      redo_log.get_mutator_size()))) {
    if (OB_ENTRY_EXIST == ret) {
      LOG_DEBUG("redo_log duplication", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(submit_ts),
          K(redo_log), K(handling_miss_log), K(task));
      ret = OB_SUCCESS;
    } else if (OB_IN_STOP_STATE != ret) {
      LOG_ERROR("push_redo_log into PartTransTask fail", KR(ret), K_(tls_id), K(tx_id), K(lsn),
          K(handling_miss_log), K(task), K(redo_log));
    }
  } else {
    LOG_DEBUG("handle_trans_redo", K_(tls_id), K(tx_id), K(lsn), K(submit_ts), K(redo_log),
        K(handling_miss_log), KPC(task));
  }

  return ret;
}

int ObCDCPartTransResolver::handle_multi_data_source_log_(
    const transaction::ObTransID &tx_id,
    const palf::LSN &lsn,
    const bool handling_miss_log,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  transaction::ObTxMultiDataSourceLog multi_data_source_log;
  PartTransTask *task = NULL;

  if (OB_FAIL(tx_log_block.deserialize_log_body(multi_data_source_log))) {
    LOG_ERROR("deserialize multi_data_source_log failed", KR(ret),
        K_(tls_id), K(tx_id), K(lsn), K(multi_data_source_log));
  } else if (OB_FAIL(obtain_task_(tx_id, task, handling_miss_log))) {
    LOG_ERROR("obtain_task_ fail", KR(ret), K_(tls_id), K(tx_id), K(multi_data_source_log), K(handling_miss_log));
  } else if (OB_FAIL(push_fetched_log_entry_(lsn, *task))) {
    LOG_ERROR("push_fetched_log_entry into part_trans_task failed", KR(ret), K_(tls_id), K(tx_id), K(lsn),
        K(multi_data_source_log), KPC(task));
  } else if (OB_FAIL(task->push_multi_data_source_data(
      lsn,
      multi_data_source_log.get_data(),
      false/*is_commit_log*/))) {
    LOG_ERROR("push_multi_data_source_data failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(multi_data_source_log));
  } else {
    LOG_DEBUG("handle_multi_data_source_log_ succ", K_(tls_id), K(tx_id), K(lsn), K(multi_data_source_log), KPC(task));
  }

  return ret;
}

int ObCDCPartTransResolver::handle_record_(
    const transaction::ObTransID &tx_id,
    const palf::LSN &lsn,
    MissingLogInfo &missing_info,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  const bool is_resolving_miss_log = missing_info.is_resolving_miss_log();
  transaction::ObTxRecordLogTempRef tmp_ref;
  transaction::ObTxRecordLog record_log(tmp_ref);
  bool is_first_record = false;

  if (OB_FAIL(tx_log_block.deserialize_log_body(record_log))) {
    LOG_ERROR("deserialize_record_log_body failed", KR(ret), K_(tls_id), K(tx_id), K(lsn));
  } else {
    PartTransTask *part_trans_task = NULL;
    const transaction::ObRedoLSNArray &prev_redo_lsns = record_log.get_redo_lsns();
    const palf::LSN &prev_record_lsn = record_log.get_prev_record_lsn();
    is_first_record = ! prev_record_lsn.is_valid();

    if (OB_UNLIKELY(prev_redo_lsns.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("prev_redo_lsn_arr should not be empty in record_log", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log), K(prev_redo_lsns));
    } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
      LOG_ERROR("obtain PartTransTask failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log), K(missing_info));
    } else if (OB_FAIL(part_trans_task->push_back_recored_redo_lsn_arr(prev_redo_lsns, lsn, false/*has_redo_in_cur_entry*/))) {
      LOG_ERROR("push_back_recored_redo_lsn_arr failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log), K(prev_redo_lsns), KPC(part_trans_task));
    } else if (OB_UNLIKELY(missing_info.has_miss_record_or_state_log())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expect prev miss_record_or_state_log handled while resolving record_log", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log),
          K(missing_info), KPC(part_trans_task));
    } else if (is_resolving_miss_log) {
      // push back all prev_log_lsns into missing_info
      if (OB_FAIL(missing_info.push_back_missing_log_lsn_arr(prev_redo_lsns))) {
        LOG_ERROR("push_back_missing_log_lsn_arr failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log), K(is_resolving_miss_log),
            K(missing_info), KPC(part_trans_task));
      } else if (is_first_record) {
        part_trans_task->mark_read_first_record();
      } else if (OB_FAIL(missing_info.set_miss_record_or_state_log_lsn(prev_record_lsn))) {
        LOG_ERROR("push prev_record_lsn into missing_info failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log),
            K(is_first_record), K(missing_info), KPC(part_trans_task));
      } else {
        LOG_INFO("push_back missing_log_info found in record_log", KR(ret), K_(tls_id), K(tx_id), K(lsn),
            K(record_log), K(is_first_record), K(missing_info), KPC(part_trans_task));
      }
    } else if (! part_trans_task->has_find_first_record()) {
      if (OB_FAIL(check_redo_log_list_(prev_redo_lsns, *part_trans_task, missing_info))) {
        LOG_ERROR("check_redo_log_list_ failed while handling record_log", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log), KPC(part_trans_task));
      } else if (is_first_record) {
        part_trans_task->mark_read_first_record();
        LOG_DEBUG("mark_read_first_record", K_(tls_id), K(tx_id), K(lsn), K(record_log));
      } else if (OB_FAIL(missing_info.set_miss_record_or_state_log_lsn(prev_record_lsn))) {
        LOG_ERROR("push prev_record_lsn into missing_info failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log),
            K(is_first_record), K(missing_info), KPC(part_trans_task));
      }
    } else {
      // already handle the first record_log in trans, expected all redo is complete, don't need any more operation.
    }
  }

  LOG_DEBUG("handle record_log", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(record_log), K(is_first_record), K(missing_info));
  return ret;
}

int ObCDCPartTransResolver::handle_rollback_to_(
    const transaction::ObTransID &tx_id,
    const palf::LSN &lsn,
    const bool is_resolving_miss_log,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  transaction::ObTxRollbackToLog rollback_to_log;
  PartTransTask *part_trans_task = NULL;

  if (OB_FAIL(tx_log_block.deserialize_log_body(rollback_to_log))) {
    LOG_ERROR("deserialize_rollback_to_log_body failed", KR(ret), K_(tls_id), K(tx_id), K(lsn));
  } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
    LOG_ERROR("obtain PartTransTask failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(rollback_to_log));
  } else if (OB_FAIL(push_fetched_log_entry_(lsn, *part_trans_task))) {
    LOG_ERROR("push_fetched_log_entry into part_trans_task failed", KR(ret), K_(tls_id), K(tx_id), K(lsn),
        K(rollback_to_log), KPC(part_trans_task));
  } else if (OB_FAIL(part_trans_task->push_rollback_to_info(lsn, rollback_to_log.get_from(), rollback_to_log.get_to()))) {
    LOG_ERROR("push_rollback_to_info failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(rollback_to_log));
  } else {
    LOG_INFO("handle rollback_to_log success", K_(tls_id), K(tx_id), K(lsn), K(rollback_to_log));
    // success
  }

  return ret;
}

// handle_commit_info_: check redo_lsn is complete, fill missing_info and return OB_ITEM_NOT_SETTED
// if has miss_log.
// commit_info should not consume twice!(if commit_info_log is misslog, it doesn't have chance to
// reconsume).
// detail:
// 0. commit_info handle logic:
// 0.1 mark read commit_info
// 0.2. handle trace_info and other info recorded in commit_info_log
// 0.3. push prev_redo_lsn_arr and lsn of current_logentry(if redo exist in current log_entry) into
//        SortedLogEntryInfo::recorded_logentry_list
// 0.4. check_missing_info:
// 0.4.1. push CommitInfoLog::prev_redo_lsn_arr and CommitInfoLog::prev_record_lsn(if valid) into MissingInfo directly if is resolving miss log.
// 0.4.2. check lsn diff between SortedLogEntryInfo::fetched_logentry_list and CommitInfoLog::prev_redo_lsn_arr,
//       push miss_log_lsn and prev_record_lsn(if valid) into MissingInfo
//
// all cases of handing commit_info_log:
// 1. for log_seq like: redo->redo->commit_info->prepare/commit:
//    |redo|redo-commit_info| or |redo|commit_info-xxx| or |commit_info-xxx|(xxx means prepare or commit or no-log)
// 1.1. start before commit_info: commit_info check redo_list and find miss_info
// 1.2. start after commit_info: commit_info is missed and push all prev_redo_lsn_arr into miss_info
// 2. for log_seq like: redo->record->redo->commit_info->prepare/commit:
// 2.1. start before commit_info:
// 2.1.1. has read record_log: should have read all log while handling record_log and PartTransTask::has_read_first_record should be true.
// 2.1.2. not read record_log: which means start between the last record_log and commit_info_log, should push miss_log and prev_record_log_lsn into miss_info
// 2.2. start after commit_info: commit_info is missed and push all prev_redo_lsn_arr and prev_record_lsn into miss_info
// 3. for log_seq like: commit_info->prepare/commit:
// 3.1. start before commit_info: prev_redo_lsn_arr and prev_record_lsn should be empty or invalid. no miss_log will be found.
// 3.2. start after commit_info: same to above.
int ObCDCPartTransResolver::handle_commit_info_(
    const transaction::ObTransID &tx_id,
    const palf::LSN &lsn,
    const int64_t submit_ts,
    const bool has_redo_in_cur_entry,
    MissingLogInfo &missing_info,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  const bool is_resolving_miss_log = missing_info.is_resolving_miss_log();
  transaction::ObTxCommitInfoLogTempRef tmp_ref;
  transaction::ObTxCommitInfoLog commit_info_log(tmp_ref);
  PartTransTask *part_trans_task = NULL;

  if (OB_FAIL(tx_log_block.deserialize_log_body(commit_info_log))) {
    LOG_ERROR("deserialize_commit_info_log_body failed", KR(ret), K_(tls_id), K(tx_id), K(lsn));
  } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
    LOG_ERROR("obtain PartTransTask failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(submit_ts), K(missing_info));
  } else {
    const common::ObString &trace_id = commit_info_log.get_app_trace_id();
    const common::ObString &trace_info = commit_info_log.get_app_trace_info();
    const bool is_dup_tx = commit_info_log.is_dup_tx();
    const transaction::ObXATransID &xid = commit_info_log.get_xid();
    const transaction::ObRedoLSNArray &prev_redo_lsns = commit_info_log.get_redo_lsns();
    const palf::LSN &prev_record_lsn = commit_info_log.get_prev_record_lsn();
    const bool has_record_log = prev_record_lsn.is_valid();

    if (OB_FAIL(part_trans_task->set_commit_info(trace_id, trace_info, is_dup_tx, xid))) {
      LOG_ERROR("set_commit_info failed", KR(ret), K_(tls_id), K(lsn), K(commit_info_log), KPC(part_trans_task));
    } else if (OB_UNLIKELY(!missing_info.is_empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expect empty missing_info while reading commit_info_log", KR(ret), KPC(part_trans_task));
    // push_back_recored_redo_lsn_arr in any case.
    } else if (OB_FAIL(part_trans_task->push_back_recored_redo_lsn_arr(prev_redo_lsns, lsn, has_redo_in_cur_entry))) {
      LOG_ERROR("push_back_recored_redo_lsn_arr failed", KR(ret), K(prev_redo_lsns), KPC(part_trans_task));
    } else if (is_resolving_miss_log) {
      if (OB_FAIL(missing_info.push_back_missing_log_lsn_arr(prev_redo_lsns))) {
        LOG_ERROR("push_back_missing_log_lsn_arr fail", KR(ret), K(commit_info_log), K(missing_info), KPC(part_trans_task));
      } else if (prev_record_lsn.is_valid() && OB_FAIL(missing_info.set_miss_record_or_state_log_lsn(prev_record_lsn))) {
        LOG_ERROR("set_miss_record_or_state_log_lsn failed", KR(ret), K(commit_info_log), K(missing_info), KPC(part_trans_task));
      }
    } else if (! part_trans_task->has_find_first_record()) {
      // check if (1) trans doesn't have record_log; (2) trans has record_log but found log_miss
      if (OB_UNLIKELY(has_record_log && missing_info.need_reconsume_commit_log_entry())) {
        ret = OB_STATE_NOT_MATCH;
        LOG_ERROR("all record_log should already fetched while reconsume log_entry", KR(ret), K_(tls_id), K(commit_info_log));
      } else if (OB_FAIL(check_redo_log_list_(prev_redo_lsns, *part_trans_task, missing_info))) {
        LOG_ERROR("check_redo_log_list_ failed", KR(ret), K(commit_info_log), K(missing_info), KPC(part_trans_task));
        // To handle log seq like: record redo redo redo commit_info, obcdc start after last record.
        // prev_record_log_lsn is valid && not find first record
      } else if (has_record_log && OB_FAIL(missing_info.set_miss_record_or_state_log_lsn(prev_record_lsn))) {
        LOG_ERROR("push prev_record_lsn failed", KR(ret), K(prev_record_lsn), K(missing_info), KPC(part_trans_task));
      }
    } else {
      /* has read first record, assume all redo log are fetched before commit_info*/
    }
  }

  LOG_DEBUG("handle commit_info_log", KR(ret), K_(tls_id), K(tx_id), K(lsn),
      K(commit_info_log), K(missing_info), KPC(part_trans_task));

  return ret;
}

// handle_prepare_log_
// if already read commit_info log, assume all redo_log_entry are fetched
int ObCDCPartTransResolver::handle_prepare_(
    const transaction::ObTransID &tx_id,
    const palf::LSN &prepare_lsn,
    const int64_t prepare_ts,
    MissingLogInfo &missing_info,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  const bool is_resolving_miss_log = missing_info.is_resolving_miss_log();
  transaction::ObTxPrepareLogTempRef tmp_ref;
  transaction::ObTxPrepareLog prepare_log(tmp_ref);
  PartTransTask *part_trans_task = NULL;

  if (OB_FAIL(tx_log_block.deserialize_log_body(prepare_log))) {
    LOG_ERROR("deserialize_prepare_log_body failed", KR(ret), K_(tls_id), K(tx_id), K(prepare_ts));
  } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
    LOG_ERROR("obtain PartTransTask failed", KR(ret), K_(tls_id), K(tx_id));
  } else if (OB_FAIL(part_trans_task->prepare(prepare_lsn, prepare_ts, part_trans_dispatcher_))) {
    LOG_ERROR("prepare part_trans_task failed", KR(ret), K_(tls_id), K(tx_id),
        K(prepare_lsn), K(prepare_ts), K(prepare_log), KPC(part_trans_task));
  } else if (!part_trans_task->has_read_commit_info()) {
    const palf::LSN &commit_info_lsn = prepare_log.get_prev_lsn();

    if (commit_info_lsn.is_valid()) {
      if (OB_FAIL(missing_info.set_miss_record_or_state_log_lsn(commit_info_lsn))) {
        LOG_ERROR("push_back_missing_log_lsn fail", KR(ret), K_(tls_id), K(tx_id),
            K(prepare_log), K(commit_info_lsn), K(missing_info), KPC(part_trans_task));
      } else {
        LOG_DEBUG("push_back_commit_info_log_lsn_to_miss_log", K_(tls_id), K(tx_id), K(prepare_log), K(missing_info));
      }
    } else {
      // prev_record_lsn is invalid, may transfer after trans execute prepare
      // usually, commit_info_log and prepare_log is in the same log_entry. and won't handle
      // prepare_log in this case
    }
  }

  LOG_DEBUG("handle prepare_log", KR(ret), K_(tls_id), K(tx_id), K(prepare_log), K(missing_info), KPC(part_trans_task));

  return ret;
}

int ObCDCPartTransResolver::handle_commit_(
    const int64_t cluster_id,
    const transaction::ObTransID &tx_id,
    const palf::LSN &lsn,
    const int64_t submit_ts,
    const logfetcher::PartServeInfo &serve_info,
    MissingLogInfo &missing_info,
    transaction::ObTxLogBlock &tx_log_block,
    bool &is_served)
{
  int ret = OB_SUCCESS;
  const bool is_resolving_miss_log = missing_info.is_resolving_miss_log();
  transaction::ObTxCommitLogTempRef tmp_ref;
  transaction::ObTxCommitLog commit_log(tmp_ref);
  PartTransTask *part_trans_task = NULL;
  int64_t trans_commit_version = OB_INVALID_VERSION;
  bool is_redo_complete = false;
  is_served = false;

  if (OB_UNLIKELY(! missing_info.is_empty())) {
    ret = OB_NEED_RETRY;
    missing_info.set_need_reconsume_commit_log_entry();
    LOG_WARN("found missing_info not empty, may have commit_info_log before the commit_log in current log_entry, retry later",
        KR(ret), K_(tls_id), K(tx_id), K(lsn), K(missing_info));
  } else if (OB_FAIL(tx_log_block.deserialize_log_body(commit_log))) {
    LOG_ERROR("deserialize_log_body failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(commit_log));
  } else if (OB_UNLIKELY(!is_valid_trans_type_(commit_log.get_trans_type()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid trans_type", KR(ret), K_(tls_id), K(tx_id), K(commit_log), K(lsn));
  } else if (OB_UNLIKELY(OB_INVALID_VERSION ==
      (trans_commit_version = get_trans_commit_version_(submit_ts, commit_log.get_commit_version().get_val_for_logservice())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid trans_commit_version", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(submit_ts), K(commit_log));
  } else if (!serve_info.is_served(trans_commit_version)) {
    LOG_WARN("found trans not served", K_(tls_id), K(tx_id), K(lsn),
        K(commit_log), K(serve_info));
    if (OB_FAIL(part_trans_dispatcher_.remove_task(tls_id_.is_sys_log_stream(), tx_id))) {
      LOG_ERROR("handle unserverd PartTransTask failed", KR(ret), K_(tls_id), K(tx_id));
    }
  } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
    LOG_ERROR("obtain_part_trans_task fail while reading commit log", KR(ret), K_(tls_id), K(tx_id), K(lsn),
        K(commit_log), K(missing_info));
  } else if (OB_FAIL(part_trans_task->push_multi_data_source_data(lsn, commit_log.get_multi_source_data(), true/*is_commit_log*/))) {
    LOG_ERROR("push_multi_data_source_data failed", KR(ret), K_(tls_id), K(tx_id), K(lsn), K(commit_log), KPC(part_trans_task));
  } else if (!part_trans_task->has_read_commit_info()) {
    if (is_resolving_miss_log) {
      // commit info is miss log and handled done, reconsumeing commit_log
      // should have read commit_info log
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected hasn't read commit_info log after resolving misslog", KR(ret),
          K_(tls_id), K(tx_id), K(commit_log), K(lsn), KPC(part_trans_task), K(missing_info));
    } else {
      const palf::LSN &prev_log_lsn = commit_log.get_prev_lsn();
      // 1. if dist-trans, prepare_log should be not the same log_entry with commit log, the prev_log_lsn in commit_log should be prepare log,
      //     should fetch prev prepare log and misslog found in prepare log.
      // 2. if single_ls_trans, which doesn't have prepare_log, has_read_commit_info == false means
      //     commit_info_log is not fetched yet, should try fetch prev_log_lsn(if valid).
      if (OB_UNLIKELY(!prev_log_lsn.is_valid())) {
        // 1. if the prev_log is in the same log_entry with commit_log, it should already handled before commit_log
        // 2. if the prev_log is not same log_entry with commit_log, the prev_log_lsn should be valid
        // 3. in transfer case, CommitLog may be alone at a LS, should remove the task
        ret = OB_ERR_UNEXPECTED;
        // expect prev_log_lsn of commit_log is valid: prepare_log for dist_trans and
        // commit_info_log for single_ls_trans.
        LOG_ERROR("expect valid prev_log_lsn in commit_log", KR(ret), K_(tls_id), K(tx_id), K(commit_log), KPC(part_trans_task));
        // remove part_trans_task if dist_trans and invalid prev_log_lsn
        // if (OB_FAIL(part_trans_dispatcher_.remove_task(tls_id_.is_sys_log_stream(), tx_id))) {
        //   LOG_ERROR("handle unserverd single CommitLog(commit_log with invalid prev_log_lsn in dist_trans) failed",
        //       KR(ret), K_(tls_id), K(tx_id), K(commit_log), K(lsn));
        // }
      } else if (OB_FAIL(missing_info.set_miss_record_or_state_log_lsn(prev_log_lsn))) {
        LOG_ERROR("push_back_single_miss_log_lsn failed", KR(ret), K_(tls_id), K(tx_id), K(commit_log), K(missing_info));
      } else {
        missing_info.set_need_reconsume_commit_log_entry();
      }
    }
  } else if (OB_FAIL(part_trans_task->is_all_redo_log_entry_fetched(is_redo_complete))) {
    LOG_ERROR("check is_all_log_entry_fetched failed", KR(ret), KPC(part_trans_task));
  } else if (!is_redo_complete) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected found redo log entry not complete if have read commit_info log", KR(ret),
        K_(tls_id), K(tx_id), K(commit_log), K(lsn), KPC(part_trans_task));
  } else if (OB_UNLIKELY(part_trans_task->is_sys_ls_not_serve_trans())
      && transaction::TransType::SP_TRANS == commit_log.get_trans_type()) {
    // remove part_trans_task if is single_sys_logstream trans but not has valid MultiDataSourceInfo(means not DDL/LS_TABLE_CHANGE/TABLET_CHANGE)
    LOG_DEBUG("[FILTER_PART_TRANS] sys_ls_trans without valid multi_data_source_info(not ddl/ls_table or tablet_change)",
        K_(tls_id), K(tx_id), K(commit_log), KPC(part_trans_task));
   if (OB_FAIL(part_trans_dispatcher_.remove_task(tls_id_.is_sys_log_stream(), tx_id))) {
      LOG_ERROR("handle unserverd single CommitLog(commit_log with invalid prev_log_lsn in dist_trans) failed",
          KR(ret), K_(tls_id), K(tx_id), K(commit_log), K(lsn));
    }
  } else if (transaction::TransType::SP_TRANS == commit_log.get_trans_type()
      && OB_FAIL(part_trans_task->prepare(lsn, submit_ts, part_trans_dispatcher_))) {
    // prepare single_ls_trans while resolving its commit_log.
    LOG_ERROR("prepare part_trans_task for single_ls_trans failed", KR(ret), K_(tls_id), K(tx_id),
        K(lsn), K(submit_ts), K(commit_log), KPC(part_trans_task));
  } else if (OB_FAIL(part_trans_task->commit(
      cluster_id,
      tx_id,
      trans_commit_version,
      (transaction::TransType)commit_log.get_trans_type(),
      commit_log.get_ls_log_info_arr(),
      lsn,
      submit_ts,
      part_trans_dispatcher_.is_data_dict_dispatcher()))) {
    LOG_ERROR("commit PartTransTask failed", KR(ret), K_(tls_id), K(tx_id), K(trans_commit_version),
        K(lsn), K(submit_ts), K(commit_log), KPC(part_trans_task));
  }

  LOG_DEBUG("handle commit_log", KR(ret), K_(tls_id), K(tx_id), K(trans_commit_version),
      K(lsn), K(submit_ts), K(commit_log));

  return ret;
}

int ObCDCPartTransResolver::handle_abort_(
    const transaction::ObTransID &tx_id,
    const bool is_resolving_miss_log,
    transaction::ObTxLogBlock &tx_log_block)
{
  int ret = OB_SUCCESS;
  const bool is_sys_tablet = tls_id_.is_sys_log_stream();
  transaction::ObTxAbortLogTempRef tmp_ref;
  transaction::ObTxAbortLog abort_log(tmp_ref);
  PartTransTask *part_trans_task = NULL;

  if (OB_FAIL(tx_log_block.deserialize_log_body(abort_log))) {
    LOG_ERROR("deserialize_abort_log_body failed", KR(ret), K_(tls_id), K(tx_id));
  } else if (OB_FAIL(obtain_task_(tx_id, part_trans_task, is_resolving_miss_log))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("obtain PartTransTask failed", KR(ret), K_(tls_id), K(tx_id));
    }
  } else if (part_trans_task->is_trans_committed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected trans state while abort trans", KR(ret), K_(tls_id), K(tx_id), KPC(part_trans_task));
  } else if (OB_FAIL(part_trans_dispatcher_.remove_task(is_sys_tablet, tx_id))) {
    LOG_ERROR("remove task from part_trans_dispatcher_ fail for abort log", KR(ret),
        K_(tls_id), K(tx_id), K(abort_log));
  }

  LOG_DEBUG("handle abort_log", KR(ret), K_(tls_id), K(tx_id), K(abort_log));

  return ret;
}

int ObCDCPartTransResolver::obtain_task_(
    const transaction::ObTransID &tx_id,
    PartTransTask *&part_trans_task,
    const bool is_resolving_miss_log)
{
  int ret = OB_SUCCESS;
  PartTransID part_trans_id(tls_id_, tx_id);

  if (OB_FAIL(part_trans_dispatcher_.get_task(part_trans_id, part_trans_task))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (is_resolving_miss_log) {
        LOG_ERROR("get part_trans_task fail while resolving missing log",
            KR(ret), K(is_resolving_miss_log), K_(tls_id), K(tx_id));
      } else if (OB_FAIL(part_trans_dispatcher_.alloc_task(part_trans_id, part_trans_task))) {
        LOG_ERROR("alloc part_trans_task fail", KR(ret), K_(tls_id), K(tx_id), K(is_resolving_miss_log));
      } else {
        LOG_DEBUG("alloc part_trans_task succ", K_(tls_id), K(tx_id), K(is_resolving_miss_log));
      }
    } else {
      LOG_ERROR("get part_trans_task fail", KR(ret), K_(tls_id), K(tx_id), K(is_resolving_miss_log));
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(part_trans_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("obtain part_trans_task fail", KR(ret), K_(tls_id), K(tx_id), K(is_resolving_miss_log));
  }

  return ret;
}

int ObCDCPartTransResolver::push_fetched_log_entry_(
    const palf::LSN &lsn,
    PartTransTask &part_trans_task)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(lsn));
  } else if (OB_FAIL(part_trans_task.push_fetched_log_entry(lsn))) {
    LOG_ERROR("push_fetched_log_entry into part_trans_task failed", KR(ret),
        K_(tls_id), K(lsn), K(part_trans_task));
  }

  return ret;
}

int ObCDCPartTransResolver::check_redo_log_list_(
    const transaction::ObRedoLSNArray &prev_redo_lsn_arr,
    PartTransTask &part_trans_task,
    MissingLogInfo &missing_info)
{
  int ret = OB_SUCCESS;
  ObLogLSNArray sorted_redo_lsn_arr_in_trans_log;
  const SortedLogEntryArray &fetched_lsn_arr =
      part_trans_task.get_sorted_log_entry_info().get_fetched_log_entry_node_arr();

  for (int64_t idx = 0; OB_SUCC(ret) && idx < prev_redo_lsn_arr.count(); ++idx) {
    const palf::LSN &lsn = prev_redo_lsn_arr.at(idx);
    if (OB_FAIL(sorted_redo_lsn_arr_in_trans_log.push_back(lsn))) {
      LOG_ERROR("push_back lsn to sorted_redo_lsn_arr_in_trans_log failed", KR(ret),
          K(prev_redo_lsn_arr), K(part_trans_task));
    }
  }

  if (OB_SUCC(ret)) {
    std::sort(
        sorted_redo_lsn_arr_in_trans_log.begin(),
        sorted_redo_lsn_arr_in_trans_log.end(),
        CDCLSNComparator());
    LogEntryNode *first_fetched_log_entry_node = fetched_lsn_arr.get_first_node();
    if (OB_ISNULL(first_fetched_log_entry_node)) {
      // doesn't fetch any log.
      if (OB_FAIL(missing_info.push_back_missing_log_lsn_arr(prev_redo_lsn_arr))) {
        LOG_ERROR("push_back_missing_log_lsn_arr failed", KR(ret), K(prev_redo_lsn_arr), K(part_trans_task));
      }
    } else {
      const palf::LSN &min_fetched_lsn = first_fetched_log_entry_node->get_lsn();

      for (int64_t idx = 0; OB_SUCC(ret) && idx < sorted_redo_lsn_arr_in_trans_log.count(); ++idx) {
        palf::LSN &log_lsn = sorted_redo_lsn_arr_in_trans_log.at(idx);

        if (log_lsn < min_fetched_lsn) {
          if (OB_FAIL(missing_info.push_back_single_miss_log_lsn(log_lsn))) {
            LOG_ERROR("push_back_single_miss_log_lsn failed", KR(ret), K_(tls_id), K(log_lsn),
                K(part_trans_task), K(sorted_redo_lsn_arr_in_trans_log));
          } else {
            LOG_DEBUG("found miss_log_lsn and push into msising_info succ", KR(ret), K(log_lsn), K(fetched_lsn_arr));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !missing_info.is_empty()) {
    LOG_INFO("find miss log", K(missing_info), K(part_trans_task), K(prev_redo_lsn_arr),
        K(sorted_redo_lsn_arr_in_trans_log));
  }

  return ret;
}

} // end namespace cdc
} // end namespace oceanbase
