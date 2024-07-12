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

#include "storage/tx/ob_tx_log.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "storage/memtable/ob_memtable_mutator.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/tx/ob_multi_data_source_printer.h"
#include "common/cell/ob_cell_reader.h"

namespace oceanbase
{

using namespace common;
using namespace share;
namespace transaction
{

logservice::ObReplayBarrierType
ObTxLogTypeChecker::need_replay_barrier(const ObTxLogType log_type,
                                        const ObTxDataSourceType data_source_type)
{

  logservice::ObReplayBarrierType barrier_flag = logservice::ObReplayBarrierType::NO_NEED_BARRIER;

  // multi data source trans's redo log
  if (ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type) {
    if (data_source_type == ObTxDataSourceType::CREATE_TABLET_NEW_MDS
        || data_source_type == ObTxDataSourceType::DELETE_TABLET_NEW_MDS
        || data_source_type == ObTxDataSourceType::UNBIND_TABLET_NEW_MDS
        || data_source_type == ObTxDataSourceType::START_TRANSFER_OUT
        || data_source_type == ObTxDataSourceType::START_TRANSFER_OUT_PREPARE
        || data_source_type == ObTxDataSourceType::FINISH_TRANSFER_OUT
        || data_source_type == ObTxDataSourceType::START_TRANSFER_IN
        || data_source_type == ObTxDataSourceType::TABLET_BINDING) {

      barrier_flag = logservice::ObReplayBarrierType::PRE_BARRIER;

    } else if (data_source_type == ObTxDataSourceType::FINISH_TRANSFER_IN
               || data_source_type == ObTxDataSourceType::START_TRANSFER_OUT_V2
               || data_source_type == ObTxDataSourceType::TRANSFER_MOVE_TX_CTX) {

      barrier_flag = logservice::ObReplayBarrierType::STRICT_BARRIER;
    }
  } else if (ObTxLogType::TX_COMMIT_INFO_LOG == log_type) {
    if (data_source_type == ObTxDataSourceType::START_TRANSFER_IN) {
      barrier_flag = logservice::ObReplayBarrierType::STRICT_BARRIER;
    }
  } else if (ObTxLogType::TX_COMMIT_LOG == log_type) {
    if (data_source_type == ObTxDataSourceType::START_TRANSFER_IN
        || data_source_type == ObTxDataSourceType::START_TRANSFER_OUT_V2
        || data_source_type == ObTxDataSourceType::TRANSFER_MOVE_TX_CTX) {
      barrier_flag = logservice::ObReplayBarrierType::STRICT_BARRIER;
    }
  } else if (ObTxLogType::TX_ABORT_LOG == log_type) {
    if (data_source_type == ObTxDataSourceType::START_TRANSFER_IN
        || data_source_type == ObTxDataSourceType::START_TRANSFER_OUT_V2
        || data_source_type == ObTxDataSourceType::TRANSFER_MOVE_TX_CTX) {
      barrier_flag = logservice::ObReplayBarrierType::STRICT_BARRIER;
    }
  }

  return barrier_flag;
}
int ObTxLogTypeChecker::decide_final_barrier_type(
    const logservice::ObReplayBarrierType tmp_log_barrier_type,
    logservice::ObReplayBarrierType &final_barrier_type)
{

  int ret = OB_SUCCESS;
  if (logservice::ObReplayBarrierType::NO_NEED_BARRIER == final_barrier_type
      || logservice::ObReplayBarrierType::INVALID_BARRIER == final_barrier_type) {
    final_barrier_type = tmp_log_barrier_type;

  } else if (logservice::ObReplayBarrierType::NO_NEED_BARRIER == tmp_log_barrier_type) {
    // do nothing
  } else if (logservice::ObReplayBarrierType::PRE_BARRIER == tmp_log_barrier_type) {
    if (logservice::ObReplayBarrierType::PRE_BARRIER == final_barrier_type
        || logservice::ObReplayBarrierType::STRICT_BARRIER == final_barrier_type) {
      // do nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Unkown final barrier type", K(ret), K(final_barrier_type),
                K(tmp_log_barrier_type));
    }

  } else if (logservice::ObReplayBarrierType::STRICT_BARRIER == tmp_log_barrier_type) {

    if (logservice::ObReplayBarrierType::PRE_BARRIER == final_barrier_type) {
      final_barrier_type = tmp_log_barrier_type;
    } else if (logservice::ObReplayBarrierType::STRICT_BARRIER == final_barrier_type) {
      // do nothing
    } else {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "Unkown final barrier type", K(ret), K(final_barrier_type),
                K(tmp_log_barrier_type));
    }
  }
  return ret;
}

ObTxLogType ObTxPrevLogType::convert_to_tx_log_type()
{
  ObTxLogType tx_log_type = ObTxLogType::UNKNOWN;
  if (TypeEnum::COMMIT_INFO == prev_log_type_) {
    tx_log_type = ObTxLogType::TX_COMMIT_INFO_LOG;
  } else if (TypeEnum::PREPARE == prev_log_type_) {
    tx_log_type = ObTxLogType::TX_PREPARE_LOG;
  }
  return tx_log_type;
}

int ObTxPrevLogType::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int8_t prev_val;
  memcpy(&prev_val, &prev_log_type_, 1);
  return serialization::encode_i8(buf, buf_len, pos, prev_val);
}

int ObTxPrevLogType::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int8_t prev_val = 0;
  int64_t tmp_pos = pos;

  if (OB_SUCC(serialization::decode_i8(buf, data_len, pos, &prev_val))) {
    memcpy(&prev_log_type_, &prev_val, 1);
    pos = tmp_pos;
  }
  return ret;
}

int64_t ObTxPrevLogType::get_serialize_size(void) const
{
  int8_t prev_val;
  memcpy(&prev_val, &prev_log_type_, 1);
  return serialization::encoded_length_i8(prev_val);
}


// ============================== Tx Log Header =============================

DEFINE_SERIALIZE(ObTxLogHeader)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, tmp_pos, static_cast<int64_t>(tx_log_type_)))) {
  } else {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObTxLogHeader)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;

  int64_t log_type = 0;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &log_type))) {
  } else {
    tx_log_type_ = static_cast<ObTxLogType>(log_type);
    pos = tmp_pos;
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObTxLogHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i64(static_cast<int64_t>(tx_log_type_));
  return size;
}

// ============================== Tx Log serialization =============================

int ObCtxRedoInfo::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    } else {
      // skip serialize cluster_version, since 4.2.4, cluster_version put in LogBlockHeader
      TX_NO_NEED_SER(true, 1, compat_bytes_);
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(1))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  return ret;
}

OB_TX_SERIALIZE_MEMBER(ObCtxRedoInfo, compat_bytes_, cluster_version_);

// RedoLogBody serialize mutator_buf in log block
OB_DEF_SERIALIZE(ObTxRedoLog)
{
  int ret = OB_SUCCESS;
  uint32_t tmp_size = 0;
  int64_t tmp_pos = pos;
  if (mutator_size_ < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "INVALID mutator_buf_");
  } else if (OB_FAIL(ctx_redo_info_.serialize(buf, buf_len, tmp_pos))) {
    TRANS_LOG(WARN, "ctx_redo_info_ serialize failed", K(ret));
    // } else if (OB_FAIL(clog_encrypt_info_.serialize(buf, buf_len, tmp_pos))) {
    //   TRANS_LOG(WARN, "clog_encrypt_info_ serialize error", K(ret));
    // } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, tmp_pos,
    // static_cast<int64_t>(cluster_version_)))) {
    //   TRANS_LOG(WARN, "cluster_version_ serialize error", K(ret));
  } else if ((tmp_size = static_cast<uint32_t>(mutator_size_))
             && OB_FAIL(serialization::encode_i32(buf, buf_len, tmp_pos, tmp_size))) {
    TRANS_LOG(WARN, "encode mutator_size_ error", K(ret));
  } else {
    pos = tmp_pos + mutator_size_;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTxRedoLog)
{
  int ret = OB_SUCCESS;
  int64_t org_pos = pos;
  int32_t tmp_size = 0;

  if(OB_FAIL(ctx_redo_info_.deserialize(buf,data_len,pos)))
  {
    TRANS_LOG(WARN, "ctx_redo_info_ deserialize failed",K(ret));
  // if (OB_FAIL(clog_encrypt_info_.deserialize(buf, data_len, pos))) {
  //   TRANS_LOG(WARN, "deserialize clog_encrypt_info_ error", K(ret));
  // } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_cluster_version))) {
  //   TRANS_LOG(WARN, "decode cluster_version_ error", K(ret));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &tmp_size))) {
    TRANS_LOG(WARN, "decode mutator_size_ error", K(ret));
  } else {
    // cluster_version_ = static_cast<uint64_t>(tmp_cluster_version);
    mutator_size_ = static_cast<int64_t>(tmp_size);
    replay_mutator_buf_ = buf + pos;
    pos = pos + mutator_size_;
  }
  if (OB_FAIL(ret)) {
    pos = org_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTxRedoLog)
{
  int64_t len = 0;
  if (mutator_size_ < 0) {
    len = mutator_size_;
    TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "mutator_buf_ has not set");
  } else {
    len = len + ctx_redo_info_.get_serialize_size();
    len = len + MUTATOR_SIZE_NEED_BYTES;
    len = len + mutator_size_;
  }
  return len;
}

// Other LogBody
OB_TX_SERIALIZE_MEMBER(ObTxActiveInfoLog,
                       compat_bytes_,
                       /* 1 */ scheduler_,
                       /* 2 */ trans_type_,
                       /* 3 */ session_id_,
                       /* 4 */ app_trace_id_str_,
                       /* 5 */ schema_version_,
                       /* 6 */ can_elr_,
                       /* 7 */ proposal_leader_,
                       /* 8 */ cur_query_start_time_,
                       /* 9 */ is_sub2pc_,
                       /* 10 */ is_dup_tx_,
                       /* 11 */ tx_expired_time_,
                       /* 12 */ epoch_,
                       /* 13 */ last_op_sn_,
                       /* 14 */ first_seq_no_,
                       /* 15 */ last_seq_no_,
                       /* 16 */ cluster_version_,
                       /* 17 */ max_submitted_seq_no_,
                       /* 18 */ xid_,
                       /* 19 */ serial_final_seq_no_,
                       /* 20 */ associated_session_id_);

OB_TX_SERIALIZE_MEMBER(ObTxCommitInfoLog,
                       compat_bytes_,
                       /* 1 */ scheduler_,
                       /* 2 */ participants_,
                       /* 3 */ upstream_,
                       /* 4 */ is_sub2pc_,
                       /* 5 */ is_dup_tx_,
                       /* 6 */ can_elr_,
                       /* 7 */ incremental_participants_,
                       /* 8 */ cluster_version_,
                       /* 9 */ app_trace_id_str_,
                       /* 10 */ app_trace_info_,
                       /* 11 */ prev_record_lsn_,
                       /* 12 */ redo_lsns_,
                       /* 13 */ xid_,
                       /* 14 */ commit_parts_,
                       /* 15 */ epoch_);

OB_TX_SERIALIZE_MEMBER(ObTxPrepareLog,
                       compat_bytes_,
                       /* 1 */ incremental_participants_,
                       /* 2 */ prev_lsn_,
                       /* 3 */ prev_log_type_);

OB_TX_SERIALIZE_MEMBER(ObTxCommitLog,
                       compat_bytes_,
                       /* 1 */ commit_version_,
                       /* 2 */ checksum_,
                       /* 3 */ incremental_participants_,
                       /* 4 */ multi_source_data_,
                       /* 5 */ trans_type_,
                       /* 6 */ tx_data_backup_,
                       /* 7 */ prev_lsn_,
                       /* 8 */ ls_log_info_arr_,
                       /* 9 */ checksum_sig_serde_,
                       /* 10 */ prev_log_type_);

OB_TX_SERIALIZE_MEMBER(ObTxClearLog, compat_bytes_, /* 1 */ incremental_participants_);

OB_TX_SERIALIZE_MEMBER(ObTxAbortLog,
                    compat_bytes_,
                    /* 1 */ multi_source_data_,
                    /* 2 */ tx_data_backup_);

OB_TX_SERIALIZE_MEMBER(ObTxRecordLog, compat_bytes_, /* 1 */ prev_record_lsn_, /* 2 */ redo_lsns_);

OB_TX_SERIALIZE_MEMBER(ObTxStartWorkingLog, compat_bytes_, /* 1 */ leader_epoch_);

OB_TX_SERIALIZE_MEMBER(ObTxRollbackToLog, compat_bytes_, /* 1 */ from_, /* 2 */ to_);

OB_TX_SERIALIZE_MEMBER(ObTxMultiDataSourceLog, compat_bytes_, /* 1 */ data_);

OB_TX_SERIALIZE_MEMBER(ObTxDirectLoadIncLog, compat_bytes_, /* 1 */ ddl_log_type_, /* 2 */ log_buf_);

int ObTxActiveInfoLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(19))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(scheduler_.is_valid() == false, 1, compat_bytes_);
    TX_NO_NEED_SER(trans_type_ == TransType::UNKNOWN_TRANS, 2, compat_bytes_); /*trans_type_*/
    TX_NO_NEED_SER(session_id_ == 0, 3, compat_bytes_);
    TX_NO_NEED_SER(app_trace_id_str_.empty(), 4, compat_bytes_);
    TX_NO_NEED_SER(schema_version_ == 0, 5, compat_bytes_);
    TX_NO_NEED_SER(can_elr_ == false, 6, compat_bytes_);
    TX_NO_NEED_SER(proposal_leader_.is_valid() == false, 7, compat_bytes_);
    TX_NO_NEED_SER(cur_query_start_time_ == 0, 8, compat_bytes_);
    TX_NO_NEED_SER(is_sub2pc_ == false, 9, compat_bytes_);
    TX_NO_NEED_SER(is_dup_tx_ == false, 10, compat_bytes_);
    TX_NO_NEED_SER(tx_expired_time_ == 0, 11, compat_bytes_);
    TX_NO_NEED_SER(epoch_ == 0, 12, compat_bytes_);
    TX_NO_NEED_SER(last_op_sn_ == 0, 13, compat_bytes_);
    TX_NO_NEED_SER(!first_seq_no_.is_valid(), 14, compat_bytes_);
    TX_NO_NEED_SER(!last_seq_no_.is_valid(), 15, compat_bytes_);
    TX_NO_NEED_SER(true, 16, compat_bytes_);
    TX_NO_NEED_SER(!max_submitted_seq_no_.is_valid(), 17, compat_bytes_);
    TX_NO_NEED_SER(xid_.empty(), 18, compat_bytes_);
    TX_NO_NEED_SER(!serial_final_seq_no_.is_valid(), 19, compat_bytes_);
  }

  return ret;
}

int ObTxCommitInfoLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(15))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(scheduler_.is_valid() == false, 1, compat_bytes_);
    TX_NO_NEED_SER(participants_.empty(), 2, compat_bytes_);
    TX_NO_NEED_SER(upstream_.is_valid() == false, 3, compat_bytes_);
    TX_NO_NEED_SER(is_sub2pc_ == false, 4, compat_bytes_);
    TX_NO_NEED_SER(is_dup_tx_ == false, 5, compat_bytes_);
    TX_NO_NEED_SER(can_elr_ == false, 6, compat_bytes_);
    TX_NO_NEED_SER(incremental_participants_.empty(), 7, compat_bytes_);
    TX_NO_NEED_SER(true, 8, compat_bytes_);
    TX_NO_NEED_SER(app_trace_id_str_.empty(), 9, compat_bytes_);
    TX_NO_NEED_SER(app_trace_info_.empty(), 10, compat_bytes_);
    TX_NO_NEED_SER(prev_record_lsn_.is_valid() == false, 11, compat_bytes_);
    TX_NO_NEED_SER(redo_lsns_.empty(), 12, compat_bytes_);
    TX_NO_NEED_SER(xid_.empty(), 13, compat_bytes_);
    TX_NO_NEED_SER(commit_parts_.empty(), 14, compat_bytes_);
    TX_NO_NEED_SER(epoch_ == 0, 15, compat_bytes_);
  }

  return ret;
}

int ObTxPrepareLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(3))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(incremental_participants_.empty(), 1, compat_bytes_);
    TX_NO_NEED_SER(prev_lsn_.is_valid() == false, 2, compat_bytes_);
    TX_NO_NEED_SER(prev_log_type_.is_valid() == false, 3, compat_bytes_);
  }
  return ret;
}

int ObTxCommitLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(10))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(!commit_version_.is_valid(), 1, compat_bytes_);
    TX_NO_NEED_SER(checksum_ == 0, 2, compat_bytes_);
    TX_NO_NEED_SER(incremental_participants_.empty(), 3, compat_bytes_);
    TX_NO_NEED_SER(multi_source_data_.empty(), 4, compat_bytes_);
    TX_NO_NEED_SER(trans_type_ == TransType::UNKNOWN_TRANS, 5, compat_bytes_); /*trans_type_*/
    TX_NO_NEED_SER(false, 6, compat_bytes_);                                   // tx_data_backup_
    TX_NO_NEED_SER(prev_lsn_.is_valid() == false, 7, compat_bytes_);
    TX_NO_NEED_SER(ls_log_info_arr_.empty(), 8, compat_bytes_);
    TX_NO_NEED_SER(checksum_sig_.count() == 0, 9, compat_bytes_);
    TX_NO_NEED_SER(prev_log_type_.is_valid() == false, 10, compat_bytes_);
  }
  return ret;
}

int ObTxClearLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(1))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(incremental_participants_.empty(), 1, compat_bytes_);
  }

  return ret;
}

int ObTxAbortLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(multi_source_data_.empty(), 1, compat_bytes_);
    TX_NO_NEED_SER(false, 2, compat_bytes_); // tx_data_backup_
  }

  return ret;
}

int ObTxRecordLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(prev_record_lsn_.is_valid() == false, 1, compat_bytes_);
    TX_NO_NEED_SER(redo_lsns_.empty(), 2, compat_bytes_);
  }
  return ret;
}

int ObTxStartWorkingLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(leader_epoch_ == 0, 1, compat_bytes_);
  }
  return ret;
}

int ObTxRollbackToLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(false, 1, compat_bytes_);
    TX_NO_NEED_SER(false, 2, compat_bytes_);
  }

  return ret;
}

int ObTxMultiDataSourceLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(1))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(false, 1, compat_bytes_);
  }

  return ret;
}

int ObTxDirectLoadIncLog::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(2))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    TX_NO_NEED_SER(false, 1, compat_bytes_);
    TX_NO_NEED_SER(false, 2, compat_bytes_);
  }

  return ret;
}


// ============================== Tx Log Body ===========================

const ObTxLogType ObTxRedoLog::LOG_TYPE = ObTxLogType::TX_REDO_LOG;
const ObTxLogType ObTxActiveInfoLog::LOG_TYPE = ObTxLogType::TX_ACTIVE_INFO_LOG;
const ObTxLogType ObTxCommitInfoLog::LOG_TYPE = ObTxLogType::TX_COMMIT_INFO_LOG;
const ObTxLogType ObTxPrepareLog::LOG_TYPE = ObTxLogType::TX_PREPARE_LOG;
const ObTxLogType ObTxCommitLog::LOG_TYPE = ObTxLogType::TX_COMMIT_LOG;
const ObTxLogType ObTxClearLog::LOG_TYPE = ObTxLogType::TX_CLEAR_LOG;
const ObTxLogType ObTxAbortLog::LOG_TYPE = ObTxLogType::TX_ABORT_LOG;
const ObTxLogType ObTxRecordLog::LOG_TYPE = ObTxLogType::TX_RECORD_LOG;
// const ObTxLogType ObTxKeepAliveLog::LOG_TYPE = ObTxLogType::TX_KEEP_ALIVE_LOG;
const ObTxLogType ObTxStartWorkingLog::LOG_TYPE = ObTxLogType::TX_START_WORKING_LOG;
const ObTxLogType ObTxRollbackToLog::LOG_TYPE = ObTxLogType::TX_ROLLBACK_TO_LOG;
const ObTxLogType ObTxMultiDataSourceLog::LOG_TYPE = ObTxLogType::TX_MULTI_DATA_SOURCE_LOG;
const ObTxLogType ObTxDirectLoadIncLog::LOG_TYPE = ObTxLogType::TX_DIRECT_LOAD_INC_LOG;

int ObTxRedoLog::set_mutator_buf(char *buf)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || mutator_size_ >= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid mutator buf", K(buf), K(mutator_size_));
  } else {
    mutator_buf_ = buf;
  }
  return ret;
}

int ObTxRedoLog::set_mutator_size(const int64_t size, const bool after_fill)
{
  int ret = OB_SUCCESS;
  if (size < 0 || OB_ISNULL(mutator_buf_) || (!after_fill && mutator_size_ >= 0)
      || (after_fill && mutator_size_ < size)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument when set mutator size", K(after_fill), K(size),
               K(mutator_size_), K(mutator_buf_));
  } else if (!after_fill) {
    int len = 0;
    SERIALIZE_SIZE_HEADER(UNIS_VERSION);
    ret = ctx_redo_info_.before_serialize();
    len = len + MUTATOR_SIZE_NEED_BYTES + ctx_redo_info_.get_serialize_size();
    if (size <= len) {
      ret = OB_SIZE_OVERFLOW;
      TRANS_LOG(WARN, "mutator buf is not enough", K(len), K(size));
    } else {
      mutator_size_ = size - len;
      mutator_buf_ = mutator_buf_ + len;
    }
  } else {
    mutator_size_ = size;
  }
  return ret;
}

void ObTxRedoLog::reset_mutator_buf()
{
  mutator_buf_ = nullptr;
  mutator_size_ = -1;
}

//TODO: if ob_admin_dump is called by others and clog is encrypted,
//      unused_encrypt_info can work. This may be perfected in the future.
int ObTxRedoLog::ob_admin_dump(memtable::ObMemtableMutatorIterator *iter_ptr,
                               ObAdminMutatorStringArg &arg,
                               const char *block_name,
                               palf::LSN lsn,
                               int64_t tx_id,
                               SCN scn,
                               bool &has_dumped_tx_id)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  transaction::ObCLogEncryptInfo unused_encrypt_info;
  unused_encrypt_info.init();

    arg.log_stat_->tx_redo_log_size_ += get_serialize_size();
  if (OB_ISNULL(iter_ptr) || OB_ISNULL(arg.writer_ptr_) || OB_ISNULL(arg.buf_)
      || OB_NOT_NULL(mutator_buf_) || OB_ISNULL(replay_mutator_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(iter_ptr), KP(arg.writer_ptr_), KP(arg.buf_),
              KP(mutator_buf_), KP(replay_mutator_buf_));
  } else if (OB_FAIL(iter_ptr->deserialize(replay_mutator_buf_, mutator_size_, pos,
                                           unused_encrypt_info))) {
    TRANS_LOG(WARN, "deserialize replay_mutator_buf_ failed", K(ret));
  } else {
    bool has_output = false;
    arg.log_stat_->mutator_size_ += get_mutator_size();
    if (!arg.filter_.is_tablet_id_valid()) {
      arg.writer_ptr_->dump_key("###<TxRedoLog>");
      arg.writer_ptr_->start_object();
      arg.writer_ptr_->dump_key("txctxinfo");
      arg.writer_ptr_->dump_string(to_cstring(*this));
      arg.writer_ptr_->dump_key("MutatorMeta");
      arg.writer_ptr_->dump_string(to_cstring(iter_ptr->get_meta()));

      arg.writer_ptr_->dump_key("MutatorRows");
      arg.writer_ptr_->start_object();
      has_output = true;
    } else {
      if (!has_dumped_tx_id) {
        databuff_printf(arg.buf_, arg.buf_len_, arg.pos_, "{BlockID: %s; LSN:%ld, TxID:%ld; SCN:%s",
                        block_name, lsn.val_, tx_id, to_cstring(scn));
      }
      databuff_printf(arg.buf_, arg.buf_len_, arg.pos_,
                      "<TxRedoLog>: {TxCtxInfo: {%s}; MutatorMeta: {%s}; MutatorRows: {",
                      to_cstring(*this), to_cstring(iter_ptr->get_meta()));
      //fill info in buf
    }
    bool has_dumped_meta_info = false;
    memtable::ObEncryptRowBuf unused_row_buf;
    while (OB_SUCC(iter_ptr->iterate_next_row(unused_row_buf, unused_encrypt_info))) {
      // arg.writer_ptr_->start_object();
      if (arg.filter_.is_tablet_id_valid()) {
        if (arg.filter_.get_tablet_id() != iter_ptr->get_row_head().tablet_id_) {
          TRANS_LOG(INFO, "just skip according to tablet_id", K(arg), K(iter_ptr->get_row_head()));
          continue;
        } else if (!has_dumped_meta_info) {
          arg.writer_ptr_->dump_string(arg.buf_);
          has_dumped_meta_info = true;
          has_dumped_tx_id = true;
          //print tx_id and RedoLog related info in arg
        }
      }
      has_output = true;
      arg.writer_ptr_->dump_key("RowHeader");
      arg.writer_ptr_->dump_string(to_cstring(iter_ptr->get_row_head()));

      switch (iter_ptr->get_row_head().mutator_type_) {
        case memtable::MutatorType::MUTATOR_ROW: {
          arg.writer_ptr_->dump_key("NORMAL_ROW");
          arg.writer_ptr_->start_object();
          arg.log_stat_->normal_row_count_++;
          if (OB_FAIL(format_mutator_row_(iter_ptr->get_mutator_row(), arg))) {
            TRANS_LOG(WARN, "format json mutator row failed", K(ret));
          }
          arg.writer_ptr_->end_object();
          break;
        }
        case memtable::MutatorType::MUTATOR_TABLE_LOCK: {
          arg.log_stat_->table_lock_count_++;
          arg.writer_ptr_->dump_key("TableLock");
          arg.writer_ptr_->dump_string(to_cstring(iter_ptr->get_table_lock_row()));
          break;
        }
        case memtable::MutatorType::MUTATOR_ROW_EXT_INFO: {
          arg.writer_ptr_->dump_key("ExtInfo");
          arg.writer_ptr_->start_object();
          arg.log_stat_->ext_info_log_count_++;
          if (OB_FAIL(format_mutator_row_(iter_ptr->get_mutator_row(), arg))) {
            TRANS_LOG(WARN, "format ext info mutator row failed", K(ret));
          }
          arg.writer_ptr_->end_object();
          break;
        }
        default: {
          arg.writer_ptr_->dump_key("ERROR:unknown mutator type");
          const int64_t mutator_type = static_cast<int64_t>(iter_ptr->get_row_head().mutator_type_);
          arg.writer_ptr_->dump_int64(mutator_type);
          ret = OB_NOT_SUPPORTED;
          TRANS_LOG(WARN, "ERROR:unknown mutator type", K(ret));
          break;
        }
      }
    }
    if (has_output) {
      //mutator row
      arg.writer_ptr_->end_object();
      //TxRedoLog
      arg.writer_ptr_->end_object();
    }
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate_next_row failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObTxRedoLog::format_mutator_row_(const memtable::ObMemtableMutatorRow &row,
                                          ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;

  uint64_t table_id = OB_INVALID_ID;
  int64_t table_version = 0;
  uint32_t modify_count = 0;
  uint32_t acc_checksum = 0;
  int64_t version = 0;
  int32_t flag = 0;
  transaction::ObTxSEQ seq_no;
  int64_t column_cnt = 0;
  ObStoreRowkey rowkey;
  memtable::ObRowData new_row;
  memtable::ObRowData old_row;
  blocksstable::ObDmlFlag dml_flag = blocksstable::ObDmlFlag::DF_NOT_EXIST;

  if (OB_FAIL(row.copy(table_id, rowkey, table_version, new_row, old_row, dml_flag,
                       modify_count, acc_checksum, version, flag, seq_no, column_cnt))) {
    TRANS_LOG(WARN, "row_.copy fail", K(ret), K(table_id), K(rowkey), K(table_version), K(new_row),
              K(old_row), K(dml_flag), K(modify_count), K(acc_checksum), K(version), K(column_cnt));
  } else {
    arg.log_stat_->new_row_size_ += new_row.size_;
    arg.log_stat_->old_row_size_ += old_row.size_;
    arg.writer_ptr_->dump_key("RowKey");
    (void)smart_dump_rowkey_(rowkey, arg);
    arg.writer_ptr_->dump_key("TableVersion");
    arg.writer_ptr_->dump_int64(table_version);

    // new row
    arg.writer_ptr_->dump_key("NewRow Cols");
    arg.writer_ptr_->start_object();
    if (OB_FAIL(format_row_data_(new_row, arg))) {
      TRANS_LOG(WARN, "format new_row failed", K(ret));
    }
    arg.writer_ptr_->end_object();

    // old row
    arg.writer_ptr_->dump_key("OldRow Cols");
    arg.writer_ptr_->start_object();
    if (OB_SUCC(ret) && OB_FAIL(format_row_data_(old_row, arg))) {
      TRANS_LOG(WARN, "format old_row failed", K(ret));
    }
    arg.writer_ptr_->end_object();

    arg.writer_ptr_->dump_key("DmlFlag");
    arg.writer_ptr_->dump_string(get_dml_str(dml_flag));
    arg.writer_ptr_->dump_key("ModifyCount");
    arg.writer_ptr_->dump_uint64(modify_count);
    arg.writer_ptr_->dump_key("AccChecksum");
    arg.writer_ptr_->dump_uint64(acc_checksum);
    arg.writer_ptr_->dump_key("Version");
    arg.writer_ptr_->dump_int64(version);
    arg.writer_ptr_->dump_key("Flag");
    arg.writer_ptr_->dump_int64(flag);
    arg.writer_ptr_->dump_key("SeqNo");
    arg.writer_ptr_->dump_string(to_cstring(seq_no));
    arg.writer_ptr_->dump_key("NewRowSize");
    arg.writer_ptr_->dump_int64(new_row.size_);
    arg.writer_ptr_->dump_key("OldRowSize");
    arg.writer_ptr_->dump_int64(old_row.size_);
    arg.writer_ptr_->dump_key("ColumnCnt");
    arg.writer_ptr_->dump_int64(column_cnt);
  }
  return ret;
}

int ObTxRedoLog::smart_dump_rowkey_(const ObStoreRowkey &rowkey, ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t pos = rowkey.to_smart_string(arg.buf_ + arg.pos_, arg.buf_len_ - arg.pos_);
  if (pos > 0) {
    arg.writer_ptr_->dump_string(arg.buf_ + arg.pos_);
  }
  return ret;
}

int ObTxRedoLog::format_row_data_(const memtable::ObRowData &row_data, ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;

  blocksstable::ObDatumRow datum_row;
  blocksstable::ObRowReader row_reader;
  const blocksstable::ObRowHeader *row_header = nullptr;
  if (row_data.size_ > 0) {
    if (OB_FAIL(row_reader.read_row(row_data.data_, row_data.size_, nullptr, datum_row))) {
      CLOG_LOG(WARN, "Failed to read datum row", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_row.get_column_count(); i++) {
      int64_t pos = 0;
      if (nullptr != arg.writer_ptr_) {
        sprintf(arg.buf_ + arg.pos_, "%lu", i);
        arg.writer_ptr_->dump_key(arg.buf_ + arg.pos_);
        pos = datum_row.storage_datums_[i].storage_to_string(arg.buf_ + arg.pos_, arg.buf_len_ - arg.pos_);
        arg.writer_ptr_->dump_string(arg.buf_ + arg.pos_);
      }
    }
  } else if (NULL == row_data.data_ && 0 == row_data.size_) {
  }
  return ret;
}

void ObTxMultiDataSourceLog::reset()
{
  compat_bytes_.reset();
  data_.reset();
  before_serialize();
}

int ObTxMultiDataSourceLog::fill_MDS_data(const ObTxBufferNode &node)
{
  int ret = OB_SUCCESS;
#ifndef OB_TX_MDS_LOG_USE_BIT_SEGMENT_BUF
  if (node.get_serialize_size() + data_.get_serialize_size() >= MAX_MDS_LOG_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    TRANS_LOG(WARN, "MDS log is overflow", K(*this), K(node));
  } else {
#endif

    data_.push_back(node);

#ifndef OB_TX_MDS_LOG_USE_BIT_SEGMENT_BUF
  }
#endif
  return ret;
}

int64_t ObTxDLIncLogBuf::get_serialize_size() const
{
  return serialization::encoded_length(dli_buf_size_) + dli_buf_size_;
}

int ObTxDLIncLogBuf::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_ISNULL(submit_buf_) || dli_buf_size_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KP(submit_buf_), K(dli_buf_size_));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, tmp_pos, dli_buf_size_))) {
    TRANS_LOG(WARN, "encode buf size failed", K(ret), KP(buf), K(buf_len), K(tmp_pos), KPC(this));
  } else if (tmp_pos + dli_buf_size_ > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    TRANS_LOG(WARN, "the log buf is not enough", K(ret), KP(buf), K(buf_len), K(tmp_pos),
              KPC(this));
  } else {
    memcpy(buf + tmp_pos, submit_buf_, dli_buf_size_);
#ifdef  ENABLE_DEBUG_LOG
    // TRANS_LOG(INFO, "<ObTxDirectLoadIncLog>after serialize buf_size", K(ret), KP(buf),
    //           KP(submit_buf_), K(tmp_pos), K(pos), K(dli_buf_size_),KPHEX(submit_buf_,dli_buf_size_),KPHEX(buf+tmp_pos,dli_buf_size_ ));
#endif
    tmp_pos = tmp_pos + dli_buf_size_;
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }

  return ret;
}

int ObTxDLIncLogBuf::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, tmp_pos, &dli_buf_size_))) {
    TRANS_LOG(WARN, "deserialize direct_load_inc buf_size failed", K(ret), KP(buf), K(data_len),
              K(tmp_pos), KPC(this));
  } else if (tmp_pos + dli_buf_size_ > data_len) {
    ret = OB_SIZE_OVERFLOW;
    TRANS_LOG(WARN, "the log buf is not enough", K(ret), KP(buf), K(data_len), K(tmp_pos),
              KPC(this));
  } else {
    replay_buf_ = buf + tmp_pos;
#ifdef  ENABLE_DEBUG_LOG
    // TRANS_LOG(INFO, "<ObTxDirectLoadIncLog>after deserialize buf_size", K(ret), KP(buf),
    //           KP(replay_buf_), K(tmp_pos), K(pos), K(dli_buf_size_), KPHEX(replay_buf_,dli_buf_size_));
#endif
    tmp_pos = tmp_pos + dli_buf_size_;
  }

  if (OB_SUCC(ret)) {
    pos = tmp_pos;
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTxDataBackup, start_log_ts_);

int ObTxActiveInfoLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxActiveInfoLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxCommitInfoLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxCommitInfoLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Size");
    arg.writer_ptr_->dump_int64(get_serialize_size());
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxPrepareLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxPrepareLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxCommitLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxCommitLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Size");
    arg.writer_ptr_->dump_int64(get_serialize_size());
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxClearLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxClearLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxAbortLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxAbortLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxRecordLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxRecordLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxStartWorkingLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("###<TxStartWorkingLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxRollbackToLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("<TxRollbackToLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("Members");
    arg.writer_ptr_->dump_string(to_cstring(*this));
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int  ObTxDirectLoadIncLog::ob_admin_dump(share::ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("<TxDirectLoadIncLog>");
    arg.writer_ptr_->start_object();
    //TODO direct_load_inc
    //dump direct_load_inc log_buf as a string in ob_admin log_tool
    arg.writer_ptr_->end_object();
  }
  return ret;
}

int ObTxMultiDataSourceLog::ob_admin_dump(ObAdminMutatorStringArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg.writer_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arg writer is NULL", K(arg), K(ret));
  } else {
    arg.writer_ptr_->dump_key("<TxMultiDataSourceLog>");
    arg.writer_ptr_->start_object();
    arg.writer_ptr_->dump_key("mds_count");
    arg.writer_ptr_->dump_string(to_cstring(data_.count()));

    arg.writer_ptr_->dump_key("mds_array");
    arg.writer_ptr_->start_object();
    for (int64_t i = 0; i < data_.count(); i++) {
      arg.writer_ptr_->dump_key("type");
        arg.writer_ptr_->dump_string(ObMultiDataSourcePrinter::to_str_mds_type(data_[i].get_data_source_type()));
        arg.writer_ptr_->dump_key("buf_len");
        arg.writer_ptr_->dump_string(to_cstring(data_[i].get_data_size()));
        arg.writer_ptr_->dump_key("content");
        arg.writer_ptr_->dump_string(ObMulSourceTxDataDump::dump_buf(data_[i].get_data_source_type(),static_cast<char *>(data_[i].get_ptr()),data_[i].get_data_size()));
    }
    arg.writer_ptr_->end_object();

    arg.writer_ptr_->end_object();
  }
  return ret;
}

ObTxDataBackup::ObTxDataBackup() { reset(); }

int ObTxDataBackup::init(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  start_log_ts_ = start_scn;
  return ret;
}

void ObTxDataBackup::reset() { start_log_ts_.reset(); }

int ObTxCommitLog::init_tx_data_backup(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_data_backup_.init(start_scn))) {
    TRANS_LOG(WARN, "init tx_data_backup_ failed", K(ret));
  }

  // TRANS_LOG(INFO, "init tx_data_backup_", K(ret), K(tx_data_backup_));
  return ret;
}

int ObTxAbortLog::init_tx_data_backup(const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_data_backup_.init(start_scn))) {
    TRANS_LOG(WARN, "init tx_data_backup_ failed", K(ret));
  }

  // TRANS_LOG(INFO, "init tx_data_backup_", K(ret), K(tx_data_backup_));
  return ret;
}

// ============================== Tx Log Blcok =============================
#define TX_LOG_BLOCK_SERIALIZE_MEMBERS          \
  org_cluster_id_,                              \
  log_entry_no_,                                \
  tx_id_,                                       \
  scheduler_,                                   \
  __log_entry_no_,                              \
  cluster_version_,                             \
  flags_

OB_DEF_SERIALIZE(ObTxLogBlockHeader)
{
  int64_t pos_bk = pos;
  int ret = OB_SUCCESS;
  TX_SER_COMPAT_BYTES(compat_bytes_);
  TX_LST_DO_CODE(OB_TX_UNIS_ENCODE, compat_bytes_, TX_LOG_BLOCK_SERIALIZE_MEMBERS);
  // if the real serialized data size less than reserved
  // must do pading
  const int64_t pading_size = serialize_size_ - (pos - pos_bk);
  MEMSET(buf + pos, 0 , pading_size);
  pos += pading_size;
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTxLogBlockHeader)
{
  if (serialize_size_ == 0) {
    ob_abort();
  }
  return serialize_size_;
}

void ObTxLogBlockHeader::calc_serialize_size_()
{
  int64_t len = 0;
  int64_t log_entry_no = log_entry_no_;
  int64_t log_entry_no_ = INT64_MAX;
  uint8_t flags = flags_;
  uint8_t flags_ = UINT8_MAX;
  TX_SER_SIZE_COMPAT_BYTES(compat_bytes_);
  TX_LST_DO_CODE(OB_TX_UNIS_ADD_LEN, compat_bytes_, TX_LOG_BLOCK_SERIALIZE_MEMBERS);
  flags_ = flags;
  log_entry_no_ = log_entry_no;
  serialize_size_ = len;
}

OB_DEF_DESERIALIZE(ObTxLogBlockHeader)
{
  int ret = OB_SUCCESS;
  UNF_UNUSED_DES;
  TX_DSER_COMPAT_BYTES(compat_bytes_);
  TX_LST_DO_CODE(OB_TX_UNIS_DECODE, compat_bytes_, TX_LOG_BLOCK_SERIALIZE_MEMBERS);
  serialize_size_ = data_len;
  return ret;
}

int ObTxLogBlockHeader::before_serialize()
{
  int ret = OB_SUCCESS;

  if (compat_bytes_.is_inited()) {
    if (OB_FAIL(compat_bytes_.set_all_member_need_ser())) {
      TRANS_LOG(WARN, "reset all compat_bytes_ valid failed", K(ret));
    } else {
      if (cluster_version_ == 0) {
        ob_abort();
      }
      TX_NO_NEED_SER(true, 2, compat_bytes_);
      if (serialize_size_ == 0) {
        calc_serialize_size_();
      }
    }
  } else {
    if (OB_FAIL(compat_bytes_.init(7))) {
      TRANS_LOG(WARN, "init compat_bytes_ failed", K(ret));
    }
  }
  return ret;
}

const logservice::ObLogBaseType ObTxLogBlock::DEFAULT_LOG_BLOCK_TYPE =
    logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE; // TRANS_LOG
const int32_t ObTxLogBlock::DEFAULT_BIG_ROW_BLOCK_SIZE =
    62 * 1024 * 1024; // 62M redo log buf for big row

const int64_t ObTxLogBlock::BIG_SEGMENT_SPILT_SIZE = common::OB_MAX_LOG_ALLOWED_SIZE; //256 * 1024

void ObTxLogBlock::reset()
{
  inited_ = false;
  log_base_header_.reset();
  header_.reset();
  fill_buf_.reset();
  replay_buf_ = nullptr;
  len_ = pos_ = 0;
  cur_log_type_ = ObTxLogType::UNKNOWN;
  cb_arg_array_.reset();
  big_segment_buf_ = nullptr;
}

int ObTxLogBlock::reuse_for_fill()
{
  int ret = OB_SUCCESS;
  if (!header_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "must init header before reuse", K(ret));
  } else {
    log_base_header_.reset();
    cur_log_type_ = ObTxLogType::UNKNOWN;
    cb_arg_array_.reset();
    big_segment_buf_ = nullptr;
    pos_ = 0;
    // reserve place for headers, header will be filled back
    pos_ += log_base_header_.get_serialize_size(); // assume FIXED size
    header_.before_serialize();
    pos_ += header_.get_serialize_size();
  }
  return ret;
}

ObTxLogBlock::ObTxLogBlock()
    : inited_(false), log_base_header_(), header_(),
      replay_buf_(nullptr), len_(0), pos_(0), cur_log_type_(ObTxLogType::UNKNOWN), cb_arg_array_(),
      big_segment_buf_(nullptr)
{
  // do nothing
}

int ObTxLogBlock::init_for_fill(const int64_t suggested_buf_size)
{
  int ret = OB_SUCCESS;
  // accept the suggested buffer size
  const int64_t buf_size = suggested_buf_size;
  if (OB_NOT_NULL(replay_buf_) || !header_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this), K_(header));
  } else if (OB_FAIL(fill_buf_.init(buf_size))) {
    TRANS_LOG(WARN, "fill log buffer init error", K(ret), K(buf_size));
  } else {
    len_ = fill_buf_.get_length();
    pos_ = 0;
    // reserve place for headers, header will be filled back
    pos_ += log_base_header_.get_serialize_size(); // assume FIXED size
    header_.before_serialize();
    pos_ += header_.get_serialize_size();
    inited_ = true;
  }
  return ret;
}

int ObTxLogBlock::init_for_replay(const char *buf, const int64_t &size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_NOT_NULL(replay_buf_)
      || OB_ISNULL(buf)
      || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(buf), K(size), K(*this));
  } else {
    replay_buf_ = buf;
    len_ = size;
    pos_ = 0;
    if (OB_FAIL(deserialize_log_block_header_())) {
      ret = OB_DESERIALIZE_ERROR;
      TRANS_LOG(WARN, "deserialize log block header error", K(ret), K(*this));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObTxLogBlock::init_for_replay(const char *buf, const int64_t &size, int skip_pos)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(buf), K(size), K(*this));
  } else {
    replay_buf_ = buf;
    len_ = size;
    pos_ = skip_pos;

    if (OB_FAIL(header_.deserialize(replay_buf_, len_, pos_))) {
      TRANS_LOG(WARN, "deserialize block header", K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObTxLogBlock::seal(const int64_t replay_hint, const ObReplayBarrierType barrier_type)
{
  int ret = OB_SUCCESS;
  log_base_header_ = logservice::ObLogBaseHeader(logservice::ObLogBaseType::TRANS_SERVICE_LOG_BASE_TYPE,
                                                 barrier_type, replay_hint);
  int64_t pos_bk = pos_;
  pos_ = 0;
  if (OB_FAIL(serialize_log_block_header_())) {
    TRANS_LOG(WARN, "serialize log block header error", K(ret));
  } else {
    pos_ = pos_bk;
  }
  return ret;
}

int ObTxLogBlock::set_prev_big_segment_scn(const share::SCN prev_scn)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(big_segment_buf_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "invalid big segment buf", K(ret), KPC(this));
  } else if (OB_FAIL(
                 big_segment_buf_->set_prev_part_id(prev_scn.get_val_for_inner_table_field()))) {
    TRANS_LOG(WARN, "set prev part scn", K(ret), KPC(this));
  }
  return ret;
}

int ObTxLogBlock::acquire_segment_log_buf(const ObTxLogType big_segment_log_type, ObTxBigSegmentBuf *big_segment_buf)
{
  int ret = OB_SUCCESS;
  bool need_fill_part_scn = false;
  ObTxBigSegmentBuf *tmp_segment_buf = nullptr;
  ObTxLogHeader log_type_header(ObTxLogType::TX_BIG_SEGMENT_LOG);
  if (OB_ISNULL(big_segment_buf_) && OB_NOT_NULL(big_segment_buf) && big_segment_buf->is_active()) {
    big_segment_buf_ = big_segment_buf;
  }

  if (OB_ISNULL(big_segment_buf_) || OB_ISNULL(fill_buf_.get_buf())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KPC(big_segment_buf), KPC(this));
  } else if (OB_ISNULL(big_segment_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, " big segment_buf", K(ret), KPC(this));
  } else if (OB_FALSE_IT(tmp_segment_buf = big_segment_buf_)) {
  } else if (OB_FAIL(reuse_for_fill())) {
    TRANS_LOG(WARN, "reuse log block failed", K(ret), KPC(this));
  } else if (OB_FAIL(log_type_header.serialize(fill_buf_.get_buf(), len_, pos_))) {
    TRANS_LOG(WARN, "serialize log type header failed", K(ret), KPC(this));
  } else if (OB_FAIL(cb_arg_array_.push_back(ObTxCbArg(ObTxLogType::TX_BIG_SEGMENT_LOG, NULL)))) {
    TRANS_LOG(WARN, "push the first log type arg failed", K(ret), K(*this));
  } else if (OB_FAIL(cb_arg_array_.push_back(ObTxCbArg(big_segment_log_type, NULL)))) {
    TRANS_LOG(WARN, "push the second log type arg failed", K(ret), K(*this));
  } else if (OB_FAIL(tmp_segment_buf->split_one_part(fill_buf_.get_buf(), BIG_SEGMENT_SPILT_SIZE, pos_,
                                                     need_fill_part_scn))) {
    TRANS_LOG(WARN, "acquire a part of big segment failed", K(ret), KPC(this));
  } else if (tmp_segment_buf->is_completed()) {
    // tmp_segment_buf->reset();
    // reset big_segment buf after set prev scn
    ret = OB_ITER_END;
  } else {
    big_segment_buf_ = tmp_segment_buf;
    cb_arg_array_.pop_back();
    ret = OB_EAGAIN;
  }

  return ret;
}

int ObTxLogBlock::serialize_log_block_header_()
{
  int ret = OB_SUCCESS;
  char *serialize_buf = nullptr;
  if (OB_ISNULL(fill_buf_.get_buf()) || pos_ != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    serialize_buf = fill_buf_.get_buf();
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(serialize_buf)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected empty serialize_buf", K(*this));
  } else if (OB_FAIL(log_base_header_.serialize(serialize_buf, len_, pos_))) {
    TRANS_LOG(WARN, "serialize log base header error", K(ret));
  } else if (OB_FAIL(header_.before_serialize())) {
    TRANS_LOG(WARN, "before serialize failed", K(ret), K(*this));
  } else if (OB_FAIL(header_.serialize(serialize_buf, len_, pos_))) {
    TRANS_LOG(WARN, "serialize block header error", K(ret));
  }

  return ret;
}

int ObTxLogBlock::deserialize_log_block_header_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(replay_buf_) || pos_ != 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_base_header_.deserialize(replay_buf_, len_, pos_))) {
    TRANS_LOG(WARN, "deserialize log base header error", K(ret),K(len_),K(pos_));
  } else if (OB_FAIL(header_.deserialize(replay_buf_, len_, pos_))) {
    TRANS_LOG(WARN, "deserialize block header", K(ret), K(len_), K(pos_));
  }
  return ret;
}

int ObTxLogBlock::get_next_log(ObTxLogHeader &header,
                               ObTxBigSegmentBuf *big_segment_buf,
                               bool *contain_big_segment)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  if (OB_NOT_NULL(contain_big_segment)) {
    *contain_big_segment = false;
  }

  if (OB_ISNULL(replay_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (OB_SUCC(update_next_log_pos_())) {
    if (OB_FAIL(header.deserialize(replay_buf_, len_, pos_))) {
      TRANS_LOG(WARN, "deserialize log header error", K(*this));
    } else {
      cur_log_type_ = header.get_tx_log_type();

      if (ObTxLogType::TX_BIG_SEGMENT_LOG == cur_log_type_) {
        if (OB_NOT_NULL(contain_big_segment)) {
          *contain_big_segment = true;
        }

        if (OB_ISNULL(big_segment_buf)) {
          ret = OB_LOG_ALREADY_SPLIT;
          TRANS_LOG(WARN, "the tx log entry has been split, need big_segment_buf", K(ret),
                    KPC(big_segment_buf), KPC(this));
        } else if (OB_NOT_NULL(big_segment_buf_)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "A completed big segment need be serialized", K(ret), KPC(this),
                    K(big_segment_buf));
        } else if (big_segment_buf->is_completed()) {
          ret = OB_NO_NEED_UPDATE;
          TRANS_LOG(WARN, "collect all part of big segment", K(ret));
        } else if (OB_FAIL(big_segment_buf->collect_one_part(replay_buf_, len_, pos_))) {
          TRANS_LOG(WARN, "merge one part of big segment failed", K(ret), KPC(this));
          // rollback to the start position
          pos_ = tmp_pos;
        } else {
          if (big_segment_buf->is_completed()) {
            big_segment_buf_ = big_segment_buf;
            // deserialize log_header
            if (OB_FAIL(big_segment_buf_->deserialize_object(header))) {
              TRANS_LOG(WARN, "deserialize log header from  big segment buf", K(ret), K(header),
                        KPC(this));
            } else {
              cur_log_type_ = header.get_tx_log_type();
            }
          } else {
            TRANS_LOG(INFO, "collect one part of big segment buf, need continue", K(ret),
                      KPC(big_segment_buf), KPC(this));
            ret = OB_LOG_TOO_LARGE;
          }
        }

        if (OB_FAIL(ret)) {
          if (OB_LOG_TOO_LARGE != ret) {
            pos_ = tmp_pos;
          }
          cur_log_type_ = ObTxLogType::UNKNOWN;
        }
      }
    }
    TRANS_LOG(DEBUG, "[TxLogBlock] get_next_log in replay",K(cur_log_type_), K(len_), K(pos_));
  }
  return ret;
}

int ObTxLogBlock::prepare_mutator_buf(ObTxRedoLog &redo)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = get_buf();
  if (OB_ISNULL(tmp_buf)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (ObTxLogType::UNKNOWN != cur_log_type_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "MutatorBuf is using", K(ret), KPC(this));
  } else if (OB_FAIL(redo.set_mutator_buf(tmp_buf + pos_ + ObTxLogHeader::TX_LOG_HEADER_SIZE))) {
    TRANS_LOG(WARN, "set mutator buf error", K(ret));
  } else if (OB_FAIL(
                 redo.set_mutator_size(len_ - pos_ - ObTxLogHeader::TX_LOG_HEADER_SIZE, false))) {
    TRANS_LOG(WARN, "set mutator buf size error", K(ret));
  } else {
    cur_log_type_ = ObTxLogType::TX_REDO_LOG;
  }
  return ret;
}

int ObTxLogBlock::finish_mutator_buf(ObTxRedoLog &redo, const int64_t &mutator_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  char * tmp_buf = get_buf();
  ObTxLogHeader header(ObTxLogType::TX_REDO_LOG);
  if (OB_ISNULL(tmp_buf)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (ObTxLogType::TX_REDO_LOG != cur_log_type_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "MutatorBuf not prepare");
  } else if (0 == mutator_size) {
    cur_log_type_ = ObTxLogType::UNKNOWN;
    redo.reset_mutator_buf();
  } else if (OB_FAIL(redo.set_mutator_size(mutator_size, true))) {
    TRANS_LOG(WARN, "set mutator buf size error after fill", K(ret));
  } else if (OB_FAIL(header.serialize(tmp_buf, len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize log header error", K(ret), K(header), K(*this));
  } else if (OB_FAIL(redo.before_serialize())) {
    TRANS_LOG(WARN, "before serialize for redo failed", K(ret), K(redo));
  } else if (OB_FAIL(redo.serialize(tmp_buf, len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize redo log body error", K(ret));
  } else {
    pos_ = tmp_pos;
    cur_log_type_ = ObTxLogType::UNKNOWN;
  }
  return ret;
}

int ObTxLogBlock::extend_log_buf()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_buf_.extend_and_copy(pos_))) {
    TRANS_LOG(WARN, "extend clog buffer failed", K(ret));
  } else {
    len_ = fill_buf_.get_length();
  }
  return ret;
}

int ObTxLogBlock::update_next_log_pos_()
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t body_size = 0;
  // use in DESERIALIZE_HEADER
  int64_t tmp_pos = pos_;

  if (ObTxLogType::UNKNOWN != cur_log_type_) {
    if (OB_FAIL(serialization::decode(replay_buf_, len_, tmp_pos, version))) {
      TRANS_LOG(WARN, "deserialize UNIS_VERSION error", K(ret), K(*this), K(tmp_pos), K(version));
    } else if (OB_FAIL(serialization::decode(replay_buf_, len_, tmp_pos, body_size))) {
      TRANS_LOG(WARN, "deserialize body_size error", K(ret), K(*this), K(tmp_pos), K(body_size));
    } else if (tmp_pos + body_size > len_) {
      ret = OB_SIZE_OVERFLOW;
      TRANS_LOG(WARN, "has not enough space for deserializing tx_log_body", K(body_size),
                K(tmp_pos), K(*this));
    } else {
      // skip log_body if cur_log_type_ isn't UNKNOWN
      // if deserialize_log_body success, cur_log_type_ will be UNKNOWN
      pos_ = tmp_pos + body_size;
    }
  }

  if (pos_ >= len_) {
    cur_log_type_ = ObTxLogType::UNKNOWN;
    ret = OB_ITER_END;
  }
  return ret;
}

} // namespace transaction
} // namespace oceanbase
