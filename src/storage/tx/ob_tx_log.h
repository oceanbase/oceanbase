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

#ifndef OCEANBASE_TRANSACTION_OB_TX_LOG
#define OCEANBASE_TRANSACTION_OB_TX_LOG

#include <type_traits>
#include "share/ob_ls_id.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_tx_data_define.h"
#include "storage/tx/ob_tx_serialization.h"
#include "storage/tx/ob_tx_big_segment_buf.h"
#include "storage/tx/ob_trans_factory.h"
#include "storage/tx/ob_clog_encrypter.h"
#include "share/ob_admin_dump_helper.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/palf/lsn.h"
#include "share/scn.h"
//#include <cstdint>

namespace oceanbase
{

namespace memtable
{
class ObMemtableMutatorIterator;
class ObMemtableMutatorRow;
class ObRowData;
}

namespace share
{
class ObLSID;
struct ObAdminMutatorStringArg;

}

namespace palf
{
class LSN;
}

namespace common
{
class ObDataBuffer;
} // namespace common

namespace transaction
{

// class ObTxLogCb;
class ObPartTransCtx;

typedef int64_t TxID;
typedef share::ObLSID LSKey;
typedef palf::LSN LogOffSet;

// Trans Log
enum class ObTxLogType : int64_t
{
  UNKNOWN = 0,
  TX_REDO_LOG = 0x1,
  TX_ROLLBACK_TO_LOG = 0x2,
  TX_MULTI_DATA_SOURCE_LOG = 0x4,
  // reserve for other data log
  TX_RESERVE_FOR_DATA_LOG = 0x8,
  TX_ACTIVE_INFO_LOG = 0x10,
  TX_RECORD_LOG = 0x20,
  TX_COMMIT_INFO_LOG = 0x40,
  TX_PREPARE_LOG = 0x80,
  TX_COMMIT_LOG = 0x100,
  TX_ABORT_LOG = 0x200,
  TX_CLEAR_LOG = 0x400,

  // logstream-level log
  TX_START_WORKING_LOG = 0x100000,
  // BigSegment log
  TX_BIG_SEGMENT_LOG = 0x200000,
  TX_LOG_TYPE_LIMIT
};

class ObTxCbArg
{
public:
  ObTxCbArg() : log_type_(ObTxLogType::UNKNOWN), arg_(NULL) {}
  ObTxCbArg(const ObTxLogType log_type, const void *arg) : log_type_(log_type), arg_(arg) {}
  ObTxLogType get_log_type() const { return log_type_; }
  const void *get_arg() const { return arg_; }
  TO_STRING_KV(KP_(log_type), KP_(arg));
private:
  ObTxLogType log_type_;
  const void *arg_;
};

typedef common::ObSEArray<ObTxCbArg, 3> ObTxCbArgArray;

inline bool is_contain(const ObTxCbArgArray &array, const ObTxLogType log_type)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < array.count(); i++) {
    if ((array.at(i).get_log_type() == log_type)) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

static inline int64_t operator&(const ObTxLogType left, const ObTxLogType right)
{
  return (
      static_cast<int64_t>(left) &
      static_cast<int64_t>(right) );
}

static inline ObTxLogType operator|(const ObTxLogType left, const ObTxLogType right)
{
  return static_cast<ObTxLogType>(
      static_cast<int64_t>(left) |
      static_cast<int64_t>(right) );
}

static const ObTxLogType TX_LOG_TYPE_MASK = (ObTxLogType::TX_REDO_LOG |
                                             ObTxLogType::TX_ROLLBACK_TO_LOG |
                                             ObTxLogType::TX_MULTI_DATA_SOURCE_LOG |
                                             ObTxLogType::TX_RESERVE_FOR_DATA_LOG |
                                             ObTxLogType::TX_ACTIVE_INFO_LOG |
                                             ObTxLogType::TX_RECORD_LOG |
                                             ObTxLogType::TX_COMMIT_INFO_LOG |
                                             ObTxLogType::TX_PREPARE_LOG |
                                             ObTxLogType::TX_COMMIT_LOG |
                                             ObTxLogType::TX_ABORT_LOG |
                                             ObTxLogType::TX_CLEAR_LOG |
                                             ObTxLogType::TX_START_WORKING_LOG);

class ObTxLogTypeChecker {
public:
  static bool is_state_log(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_PREPARE_LOG == log_type ||
           ObTxLogType::TX_COMMIT_LOG  == log_type ||
           ObTxLogType::TX_ABORT_LOG   == log_type ||
           ObTxLogType::TX_CLEAR_LOG   == log_type;
  }
  static bool is_valid_log(const ObTxLogType log_type)
  {
    return (TX_LOG_TYPE_MASK & log_type) != 0;
  }
  static bool is_ls_log(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_START_WORKING_LOG == log_type; 
  }
  static bool can_be_spilt(const ObTxLogType log_type)
  {
    return ObTxLogType::TX_MULTI_DATA_SOURCE_LOG == log_type
  #ifdef OB_TX_LOG_TEST
           || ObTxLogType::TX_COMMIT_INFO_LOG == log_type
  #endif
        ;
  }
  static bool need_pre_replay_barrier(const ObTxLogType log_type, const ObTxDataSourceType data_source_type);
};

// ============================== Tx Log Header ==============================
class ObTxLogHeader
{
public:
  static const int64_t TX_LOG_HEADER_SIZE = sizeof(ObTxLogType);

  NEED_SERIALIZE_AND_DESERIALIZE;
  ObTxLogHeader() : tx_log_type_(ObTxLogType::UNKNOWN) {}
  ObTxLogHeader(const ObTxLogType type) : tx_log_type_(type) {}
  ObTxLogType get_tx_log_type() const { return tx_log_type_; };
  TO_STRING_KV(K(tx_log_type_));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTxLogHeader);
  ObTxLogType tx_log_type_;
};

// ============================== Tx Log Body ==============================
class ObTxRedoLogTempRef
{
public:
  ObTxRedoLogTempRef() : encrypt_info_() { encrypt_info_.init(); }
public:
  ObCLogEncryptInfo encrypt_info_;
};

struct ObCtxRedoInfo
{
  ObCtxRedoInfo(ObCLogEncryptInfo &encrypt_info, const int64_t cluster_version)
      : clog_encrypt_info_(encrypt_info), cluster_version_(cluster_version)
  {
  };
  int before_serialize();

  ObTxSerCompatByte compat_bytes_;
  // serialize before mutator_buf by fixed length
  ObCLogEncryptInfo &clog_encrypt_info_;
  uint64_t cluster_version_;

  OB_UNIS_VERSION(1);
};

class ObTxRedoLog
{
  OB_UNIS_VERSION(1);
// public:
//   NEED_SERIALIZE_AND_DESERIALIZE;

public:
  ObTxRedoLog(ObTxRedoLogTempRef &temp_ref)
      : mutator_buf_(nullptr), replay_mutator_buf_(nullptr), mutator_size_(-1),
        ctx_redo_info_(temp_ref.encrypt_info_, 0)
        // (ctx_redo_info_.clog_encrypt_info_)(temp_ref.encrypt_info_), (ctx_redo_info_.cluster_version_)(0)
  {
    before_serialize();
  }
  ObTxRedoLog(ObCLogEncryptInfo &encrypt_info, const int64_t &log_no, const uint64_t &cluster_version)
      : mutator_buf_(nullptr), replay_mutator_buf_(nullptr), mutator_size_(-1),
        ctx_redo_info_(encrypt_info, cluster_version)
  // (ctx_redo_info_.clog_encrypt_info_)(encrypt_info),(ctx_redo_info_.cluster_version_)(cluster_version)
  {
    before_serialize();
  }

  char *get_mutator_buf() { return mutator_buf_; }
  const char *get_replay_mutator_buf() const { return replay_mutator_buf_; }
  const int64_t &get_mutator_size() const { return mutator_size_; }
  const ObCLogEncryptInfo &get_clog_encrypt_info() const { return ctx_redo_info_.clog_encrypt_info_; }
  const uint64_t &get_cluster_version() const { return ctx_redo_info_.cluster_version_; }

  //------------ Only invoke in ObTxLogBlock
  int set_mutator_buf(char *buf);
  int set_mutator_size(const int64_t size, const bool after_fill);
  void reset_mutator_buf();
  //------------

  //for ob_admin dump self
  int ob_admin_dump(memtable::ObMemtableMutatorIterator *iter_ptr,
                    share::ObAdminMutatorStringArg &arg,
                    const char *block_name,
                    palf::LSN lsn,
                    int64_t tx_id,
                    share::SCN scn,
                    bool &has_output);
  //------------

  static const int8_t MUTATOR_SIZE_NEED_BYTES = 4;
  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(
      K(LOG_TYPE),
      // KP(mutator_buf_),
      K(mutator_size_),
      // KP(replay_mutator_buf_),
      // K(clog_encrypt_info_),
      K(ctx_redo_info_.cluster_version_));

public:
  int before_serialize() { return ctx_redo_info_.before_serialize(); }
private:
  //------------ For ob_admin
  int format_mutator_row_(const memtable::ObMemtableMutatorRow &row, share::ObAdminMutatorStringArg &arg);
  int format_row_data_(const memtable::ObRowData &row_data, share::ObAdminMutatorStringArg &arg);
  //------------
private:
  char *mutator_buf_;              // for fill
  const char *replay_mutator_buf_; // for replay, only set by deserialize
  int64_t mutator_size_;

  ObCtxRedoInfo ctx_redo_info_;
  //--------- for liboblog -----------
  // int64_t log_no_;
};

// for dist trans write it's multi source data, the same as redo,
// for simplicity, add a new log type
class ObTxMultiDataSourceLog
{
  OB_UNIS_VERSION(1);

public:
  const static int64_t MAX_MDS_LOG_SIZE = 512 * 3 * 1024;
  const static int64_t MAX_PENDING_BUF_SIZE = 512 * 1024;

public:
  ObTxMultiDataSourceLog()
  {
    reset();
  }
  ~ObTxMultiDataSourceLog() {}
  void reset();
  const ObTxBufferNodeArray &get_data() const { return data_; }

  int fill_MDS_data(const ObTxBufferNode &node);
  int64_t count() const { return data_.count(); }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K_(data));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  ObTxBufferNodeArray data_;
};

class ObTxActiveInfoLogTempRef {
public:
  ObTxActiveInfoLogTempRef() : scheduler_(), app_trace_id_str_(), proposal_leader_(), xid_() {}

public:
  common::ObAddr scheduler_;
  common::ObString app_trace_id_str_;
  common::ObAddr proposal_leader_;
  ObXATransID xid_;
};

class ObTxActiveInfoLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxActiveInfoLog(ObTxActiveInfoLogTempRef &temp_ref)
      : scheduler_(temp_ref.scheduler_), trans_type_(TransType::SP_TRANS), session_id_(0),
        app_trace_id_str_(temp_ref.app_trace_id_str_), schema_version_(0), can_elr_(false),
        proposal_leader_(temp_ref.proposal_leader_), cur_query_start_time_(0), is_sub2pc_(false),
        is_dup_tx_(false), tx_expired_time_(0), epoch_(0), last_op_sn_(0), first_sn(0),
        last_sn(0), max_submitted_seq_no_(0), cluster_version_(0), xid_(temp_ref.xid_)
  {
    before_serialize();
  }
  ObTxActiveInfoLog(common::ObAddr &scheduler,
                    int trans_type,
                    int session_id,
                    common::ObString &app_trace_id_str,
                    int64_t schema_version,
                    bool elr,
                    common::ObAddr &proposal_leader,
                    int64_t cur_query_start_time,
                    bool is_sub2pc,
                    bool is_dup_tx,
                    int64_t tx_expired_time,
                    int64_t epoch,
                    int64_t last_op_sn,
                    int64_t first_scn,
                    int64_t last_scn,
                    int64_t max_submitted_seq_no,
                    uint64_t cluster_version,
                    const ObXATransID &xid)
      : scheduler_(scheduler), trans_type_(trans_type), session_id_(session_id),
        app_trace_id_str_(app_trace_id_str), schema_version_(schema_version), can_elr_(elr),
        proposal_leader_(proposal_leader), cur_query_start_time_(cur_query_start_time),
        is_sub2pc_(is_sub2pc), is_dup_tx_(is_dup_tx), tx_expired_time_(tx_expired_time),
        epoch_(epoch), last_op_sn_(last_op_sn), first_sn(first_scn), last_sn(last_scn),
        max_submitted_seq_no_(max_submitted_seq_no), cluster_version_(cluster_version),
        xid_(xid)
  {
    before_serialize();
  };

  const common::ObAddr &get_scheduler() const { return scheduler_; }
  int get_trans_type() const { return trans_type_; }
  int get_session_id() const { return session_id_; }
  const common::ObString &get_app_trace_id() const { return app_trace_id_str_; }
  const int64_t &get_schema_version() { return schema_version_; }
  bool is_elr() const { return can_elr_; }
  const common::ObAddr &get_proposal_leader() { return proposal_leader_; }
  const int64_t &get_cur_query_start_time() { return cur_query_start_time_; }
  bool is_sub2pc() const { return is_sub2pc_; }
  bool is_dup_tx() const { return is_dup_tx_; }
  int64_t get_tx_expired_time() const { return tx_expired_time_; }
  int64_t get_last_op_sn() const { return last_op_sn_; }
  int64_t get_epoch() const { return epoch_; }
  int64_t get_first_scn() const { return first_sn; }
  int64_t get_last_scn() const { return last_sn; }
  int64_t get_max_submitted_seq_no() const { return max_submitted_seq_no_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  const ObXATransID &get_xid() const { return xid_; }
  // for ob_admin
  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE),
               K(scheduler_),
               K(trans_type_),
               K(session_id_),
               K(app_trace_id_str_),
               K(schema_version_),
               K(can_elr_),
               K(proposal_leader_),
               K(cur_query_start_time_),
               K(is_sub2pc_),
               K(is_dup_tx_),
               K(tx_expired_time_),
               K(epoch_),
               K(last_op_sn_),
               K(first_sn),
               K(last_sn),
               K(max_submitted_seq_no_),
               K(cluster_version_),
               K(xid_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  common::ObAddr &scheduler_;
  int trans_type_;
  int session_id_;
  common::ObString &app_trace_id_str_;
  int64_t schema_version_;
  bool can_elr_;
  common::ObAddr &proposal_leader_;
  int64_t cur_query_start_time_;
  bool is_sub2pc_;
  bool is_dup_tx_;
  int64_t tx_expired_time_;

  // sql execution relative
  int64_t epoch_;
  int64_t last_op_sn_;
  int64_t first_sn;
  int64_t last_sn;

  // ctrl savepoint written log
  int64_t max_submitted_seq_no_;

  uint64_t cluster_version_;
  ObXATransID xid_;
};

class ObTxCommitInfoLogTempRef
{
public:
  ObTxCommitInfoLogTempRef()
      : scheduler_(), participants_(), app_trace_id_str_(), app_trace_info_(),
        incremental_participants_(), prev_record_lsn_(), redo_lsns_(), xid_()
  {}

public:
  common::ObAddr scheduler_;
  share::ObLSArray participants_;
  common::ObString app_trace_id_str_;
  common::ObString app_trace_info_;
  share::ObLSArray incremental_participants_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray redo_lsns_;
  ObXATransID xid_;
};

class ObTxCommitInfoLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxCommitInfoLog(ObTxCommitInfoLogTempRef &temp_ref)
      : scheduler_(temp_ref.scheduler_), participants_(temp_ref.participants_), upstream_(),
        is_sub2pc_(false), is_dup_tx_(false), can_elr_(false),
        incremental_participants_(temp_ref.incremental_participants_), cluster_version_(0),
        app_trace_id_str_(temp_ref.app_trace_id_str_), app_trace_info_(temp_ref.app_trace_info_),
        prev_record_lsn_(temp_ref.prev_record_lsn_), redo_lsns_(temp_ref.redo_lsns_),
        xid_(temp_ref.xid_)
  {
    before_serialize();
  }
  ObTxCommitInfoLog(common::ObAddr &scheduler,
                    share::ObLSArray &participants,
                    LSKey &upstream,
                    bool is_sub2pc,
                    bool is_dup_tx,
                    bool is_elr,
                    common::ObString &app_trace_id,
                    const common::ObString &app_trace_info,
                    const LogOffSet &prev_record_lsn,
                    ObRedoLSNArray &redo_lsns,
                    share::ObLSArray &incremental_participants,
                    uint64_t cluster_version,
                    const ObXATransID &xid)
      : scheduler_(scheduler), participants_(participants), upstream_(upstream),
        is_sub2pc_(is_sub2pc), is_dup_tx_(is_dup_tx), can_elr_(is_elr),
        incremental_participants_(incremental_participants), cluster_version_(cluster_version),
        app_trace_id_str_(app_trace_id), app_trace_info_(app_trace_info),
        prev_record_lsn_(prev_record_lsn), redo_lsns_(redo_lsns), xid_(xid)
  {
    before_serialize();
  };

  const common::ObAddr &get_scheduler() const { return scheduler_; }
  const share::ObLSArray &get_participants() const { return participants_; }
  const LSKey &get_upstream() const { return upstream_; }
  bool is_sub2pc() const { return is_sub2pc_; }
  bool is_dup_tx() const { return is_dup_tx_; }
  bool is_elr() const { return can_elr_; }
  const common::ObString &get_app_trace_id() const { return app_trace_id_str_; }
  const common::ObString &get_app_trace_info() const { return app_trace_info_; }
  const ObRedoLSNArray &get_redo_lsns() const { return redo_lsns_; }
  const LogOffSet &get_prev_record_lsn() const { return prev_record_lsn_; }
  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }
  uint64_t get_cluster_version() const { return cluster_version_; }
  const ObXATransID &get_xid() const { return xid_; }
  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE),
               K(scheduler_),
               K(participants_),
               K(upstream_),
               K(is_sub2pc_),
               K(is_dup_tx_),
               K(can_elr_),
               K(incremental_participants_),
               K(cluster_version_),
               K(app_trace_id_str_),
               K(app_trace_info_),
               K(prev_record_lsn_),
               K(redo_lsns_),
               K(xid_))
public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  common::ObAddr &scheduler_;
  share::ObLSArray &participants_;
  LSKey upstream_;
  bool is_sub2pc_;
  bool is_dup_tx_;
  bool can_elr_;
  share::ObLSArray &incremental_participants_;
  uint64_t cluster_version_;

  //--------- for liboblog -----------
  common::ObString &app_trace_id_str_;
  common::ObString app_trace_info_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray &redo_lsns_;
  // for xa
  ObXATransID xid_;
};

class ObTxPrepareLogTempRef
{
public:
  ObTxPrepareLogTempRef() : incremental_participants_() {}
public:
  share::ObLSArray incremental_participants_;
};

class ObTxPrepareLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxPrepareLog(ObTxPrepareLogTempRef &temp_ref)
      : incremental_participants_(temp_ref.incremental_participants_), prev_lsn_()
  {
    before_serialize();
  };
  ObTxPrepareLog(share::ObLSArray &incremental_participants, LogOffSet &commit_info_lsn)
      : incremental_participants_(incremental_participants), prev_lsn_(commit_info_lsn)
  {
    before_serialize();
  };

  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }
  const LogOffSet &get_prev_lsn() { return prev_lsn_; }
  void set_prev_lsn(const LogOffSet &lsn) { prev_lsn_ = lsn; }
  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(incremental_participants_), K(prev_lsn_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  share::ObLSArray &incremental_participants_;

  //--------- for liboblog -----------
  LogOffSet prev_lsn_;
};

class ObTxDataBackup
{
  OB_UNIS_VERSION(1);

public:
  ObTxDataBackup();
  int init(const share::SCN &start_scn);
  void reset();

  share::SCN get_start_log_ts() const { return start_log_ts_; }

  TO_STRING_KV(K(start_log_ts_));

private:
  share::SCN start_log_ts_;
};

class ObTxCommitLogTempRef
{
public:
  ObTxCommitLogTempRef() : incremental_participants_(), multi_source_data_(), ls_log_info_arr_() {}

public:
  share::ObLSArray incremental_participants_;
  ObTxBufferNodeArray multi_source_data_;
  ObLSLogInfoArray ls_log_info_arr_;
};

class ObTxCommitLog
{
  OB_UNIS_VERSION(1);

public:
  const static int64_t INVALID_COMMIT_VERSION = ObTransVersion::INVALID_TRANS_VERSION;

public:
  ObTxCommitLog(ObTxCommitLogTempRef &temp_ref)
      : commit_version_(), checksum_(0),
        incremental_participants_(temp_ref.incremental_participants_),
        multi_source_data_(temp_ref.multi_source_data_), trans_type_(TransType::SP_TRANS),
        tx_data_backup_(), prev_lsn_(), ls_log_info_arr_(temp_ref.ls_log_info_arr_)
  {
    before_serialize();
  }
  ObTxCommitLog(share::SCN commit_version,
                uint64_t checksum,
                share::ObLSArray &incremental_participants,
                ObTxBufferNodeArray &multi_source_data,
                int32_t trans_type,
                LogOffSet prev_lsn,
                ObLSLogInfoArray &ls_log_info_arr)
      : checksum_(checksum),
        incremental_participants_(incremental_participants), multi_source_data_(multi_source_data),
        trans_type_(trans_type), prev_lsn_(prev_lsn), ls_log_info_arr_(ls_log_info_arr)
  {
    commit_version_ = commit_version;
    before_serialize();
  }
  int init_tx_data_backup(const share::SCN &start_scn);
  const ObTxDataBackup &get_tx_data_backup() const { return tx_data_backup_; }
  share::SCN get_commit_version() const { return commit_version_; }
  uint64_t get_checksum() const { return checksum_; }
  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }
  const ObTxBufferNodeArray &get_multi_source_data() const { return multi_source_data_; }
  const int32_t &get_trans_type() const { return trans_type_; }
  const ObLSLogInfoArray &get_ls_log_info_arr() const { return ls_log_info_arr_; }
  const LogOffSet &get_prev_lsn() const { return prev_lsn_; }
  void set_prev_lsn(const LogOffSet &lsn) { prev_lsn_ = lsn; }

  const share::SCN get_backup_start_scn() { return tx_data_backup_.get_start_log_ts(); }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE),
               K(commit_version_),
               K(checksum_),
               K(incremental_participants_),
               K(multi_source_data_),
               K(trans_type_),
               K(tx_data_backup_),
               K(prev_lsn_),
               K(ls_log_info_arr_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  share::SCN commit_version_; // equal to INVALID_COMMIT_VERSION in Single LS Transaction
  uint64_t checksum_;
  share::ObLSArray &incremental_participants_;
  ObTxBufferNodeArray &multi_source_data_;

  // need by a committed transaction which replay from commit log
  int32_t trans_type_;

  ObTxDataBackup tx_data_backup_;

  //--------- fo r liboblog -----------
  LogOffSet prev_lsn_;
  ObLSLogInfoArray &ls_log_info_arr_;
};

class ObTxClearLogTempRef
{
public:
  ObTxClearLogTempRef() : incremental_participants_() {}

public:
  share::ObLSArray incremental_participants_;
};

class ObTxClearLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxClearLog(ObTxClearLogTempRef &temp_ref)
      : incremental_participants_(temp_ref.incremental_participants_)
  {
    before_serialize();
  }
  ObTxClearLog(share::ObLSArray &incremental_participants)
      : incremental_participants_(incremental_participants)
  {
    before_serialize();
  }

  const share::ObLSArray &get_incremental_participants() const { return incremental_participants_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(incremental_participants_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  share::ObLSArray &incremental_participants_;
};

class ObTxAbortLogTempRef
{
public:
  ObTxAbortLogTempRef() : multi_source_data_() {};
public:
  ObTxBufferNodeArray multi_source_data_;
};

class ObTxAbortLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxAbortLog(ObTxAbortLogTempRef &temp_ref)
      : multi_source_data_(temp_ref.multi_source_data_), tx_data_backup_()
  {
    before_serialize();
  }
  ObTxAbortLog(ObTxBufferNodeArray &multi_source_data)
      : multi_source_data_(multi_source_data), tx_data_backup_()
  {
    before_serialize();
  }
  const ObTxBufferNodeArray &get_multi_source_data() const { return multi_source_data_; }

  int init_tx_data_backup(const share::SCN &start_scn);

  const ObTxDataBackup &get_tx_data_backup() const { return tx_data_backup_; }

  const share::SCN get_backup_start_scn() { return tx_data_backup_.get_start_log_ts(); }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(multi_source_data_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  ObTxBufferNodeArray &multi_source_data_;
  ObTxDataBackup tx_data_backup_;
};

class ObTxRecordLogTempRef
{
public:
  ObTxRecordLogTempRef() : prev_record_lsn_(), redo_lsns_() {}

public:
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray redo_lsns_;
};

class ObTxRecordLog
{
  OB_UNIS_VERSION(1);

public:
  ObTxRecordLog(ObTxRecordLogTempRef &temp_ref)
      : prev_record_lsn_(temp_ref.prev_record_lsn_), redo_lsns_(temp_ref.redo_lsns_)
  {
    before_serialize();
  }
  ObTxRecordLog(const LogOffSet &prev_record_lsn, ObRedoLSNArray &redo_lsns)
      : prev_record_lsn_(prev_record_lsn), redo_lsns_(redo_lsns)
  {
    before_serialize();
  }

  const ObRedoLSNArray &get_redo_lsns() { return redo_lsns_; }
  const LogOffSet &get_prev_record_lsn() { return prev_record_lsn_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(prev_record_lsn_), K(redo_lsns_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  LogOffSet prev_record_lsn_;
  ObRedoLSNArray &redo_lsns_;
};


class ObTxStartWorkingLogTempRef
{
public:
  ObTxStartWorkingLogTempRef() {}
};

class ObTxStartWorkingLog
{
public:
  OB_UNIS_VERSION(1);

public:
  ObTxStartWorkingLog(ObTxStartWorkingLogTempRef &temp_ref) : leader_epoch_(0)
  {
    UNUSED(temp_ref);
    before_serialize();
  }
  ObTxStartWorkingLog(int64_t leader_epoch) : leader_epoch_(leader_epoch) { before_serialize(); }

  const int64_t &get_leader_epoch() { return leader_epoch_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(K(LOG_TYPE), K(leader_epoch_));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  int64_t leader_epoch_;
};

class ObTxRollbackToLog
{
public:
  OB_UNIS_VERSION(1);
public:
  ObTxRollbackToLog() = default;
  ObTxRollbackToLog(const int64_t from, const int64_t to)
    : from_(from), to_(to) {before_serialize();}


  int64_t get_from() const { return from_; }
  int64_t get_to() const { return to_; }

  int ob_admin_dump(share::ObAdminMutatorStringArg &arg);

  static const ObTxLogType LOG_TYPE;
  TO_STRING_KV(KP(this), K(LOG_TYPE), K_(from), K_(to));

public:
  int before_serialize();
private:
  ObTxSerCompatByte compat_bytes_;
  int64_t from_;
  int64_t to_;
};

// ============================== Tx Log Blcok ==============================

class ObTxLogBlockHeader
{
  OB_UNIS_VERSION(1);

public:
  void reset()
  {
    org_cluster_id_ = 0;
    log_entry_no_ = 0;
    tx_id_ = 0;
    scheduler_.reset();
  }
  ObTxLogBlockHeader()
  {
    reset();
    before_serialize();
  };
  ObTxLogBlockHeader(const uint64_t org_cluster_id,
                     const int64_t log_entry_no,
                     const ObTransID &tx_id,
                     const common::ObAddr &scheduler)
      : org_cluster_id_(org_cluster_id), log_entry_no_(log_entry_no), tx_id_(tx_id),
        scheduler_(scheduler)
  {
    before_serialize();
  }

  uint64_t get_org_cluster_id() const { return org_cluster_id_; }
  int64_t get_log_entry_no() const { return log_entry_no_; }
  const ObTransID &get_tx_id() const { return tx_id_; }
  const common::ObAddr &get_scheduler() const { return scheduler_; }

  bool is_valid() const { return org_cluster_id_ >= 0; }

  TO_STRING_KV(K_(org_cluster_id), K_(log_entry_no), K_(tx_id), K_(scheduler));

public:
  int before_serialize();

private:
  ObTxSerCompatByte compat_bytes_;
  uint64_t org_cluster_id_;
  int64_t log_entry_no_;
  ObTransID tx_id_;
  common::ObAddr scheduler_;
};

class ObTxAdaptiveLogBuf
{
public:
  ObTxAdaptiveLogBuf() : buf_(NULL), use_default_buf_(false)
  {
    default_buf_[0]='\0';
  }
  ~ObTxAdaptiveLogBuf() { reset(); }
  int init(const bool use_local_buf)
  {
    int ret = OB_SUCCESS;

    if (use_local_buf) {
      // for performance optimization
      use_default_buf_ = true;
    } else {
      if (OB_ISNULL(buf_ = ClogBufFactory::alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TRANS_LOG(ERROR, "alloc clog buffer error", KR(ret));
      } else {
        use_default_buf_ = false;
      }
    }

    return ret;
  }
  void reset()
  {
    if (NULL != buf_) {
      ClogBufFactory::release(buf_);
      buf_ = NULL;
    }
    default_buf_[0]='\0';
    use_default_buf_ = false;
  }
  char *get_buf()
  {
    char *buf = NULL;

    if (use_default_buf_) {
      buf = default_buf_;
    } else if (NULL != buf_) {
      buf = buf_->get_buf();
    }
    return buf;
  }
  int64_t get_length() const
  {
    return use_default_buf_ ? DEFAULT_MIN_LOG_BUF_SIZE : common::OB_MAX_LOG_ALLOWED_SIZE;
  }
  bool is_use_local_buf() const { return use_default_buf_; }
  TO_STRING_KV(KP_(buf), K_(use_default_buf));
public:
  static const int64_t DEFAULT_MIN_LOG_BUF_SIZE = 2048;
private:
  ClogBuf *buf_;
  char default_buf_[DEFAULT_MIN_LOG_BUF_SIZE];
  bool use_default_buf_;
};

class ObTxLogBlock
{
public:
  // static const int MIN_LOG_BLOCK_HEADER_SIZE;
  static const logservice::ObLogBaseType DEFAULT_LOG_BLOCK_TYPE; // TRANS_LOG
  static const int32_t DEFAULT_BIG_ROW_BLOCK_SIZE;

  NEED_SERIALIZE_AND_DESERIALIZE;
  ObTxLogBlock();
  void reset();
  int reuse(const int64_t replay_hint, const ObTxLogBlockHeader &block_header);
  int init(const int64_t replay_hint,
           const ObTxLogBlockHeader &block_header,
           const bool use_local_buf = false);
  int init_with_header(const char *buf,
                       const int64_t &size,
                       int64_t &replay_hint,
                       ObTxLogBlockHeader &block_header);
  int init(const char *buf,
           const int64_t &size,
           int skip_pos,
           ObTxLogBlockHeader &block_header); // init before replay
  ~ObTxLogBlock() { reset(); }
  int get_next_log(ObTxLogHeader &header, ObTxBigSegmentBuf * big_segment_buf = nullptr);
  const ObTxCbArgArray &get_cb_arg_array() const { return cb_arg_array_; }
  template <typename T>
  int deserialize_log_body(T &tx_log_body); // make cur_log_type_ is UNKNOWN
  // fill log except RedoLog
  template <typename T>
  int add_new_log(T &tx_log_body, ObTxBigSegmentBuf * big_segment_buf = nullptr);
  // fill RedoLog and mutator_buf
  int prepare_mutator_buf(ObTxRedoLog &redo);
  int finish_mutator_buf(ObTxRedoLog &redo, const int64_t &mutator_size);

  //rewrite base log header at the head of log block.
  //It is a temporary interface for create/remove tablet which can not be used for any other target
  int rewrite_barrier_log_block(int64_t replay_hint,
                                const enum logservice::ObReplayBarrierType barrier_type);

  bool is_use_local_buf() const { return fill_buf_.is_use_local_buf(); }
  TO_STRING_KV(K(fill_buf_),
               KP(replay_buf_),
               K(len_),
               K(pos_),
               K(cur_log_type_),
               K(cb_arg_array_),
               KPC(big_segment_buf_));

public:
  // get fill buf for submit log
  char *get_buf() { return fill_buf_.get_buf(); }
  const int64_t &get_size() { return pos_; }

  int set_prev_big_segment_scn(const share::SCN prev_scn);
  int acquire_segment_log_buf(const char *&submit_buf,
                              int64_t &submit_buf_len,
                              const ObTxLogBlockHeader &block_header,
                              const ObTxLogType big_segment_log_type,
                              ObTxBigSegmentBuf *big_segment_buf = nullptr);
private:
  int serialize_log_block_header_(const int64_t replay_hint,
                                  const ObTxLogBlockHeader &block_header,
                                  const logservice::ObReplayBarrierType barrier_type =
                                      logservice::ObReplayBarrierType::NO_NEED_BARRIER);
  int deserialize_log_block_header_(int64_t &replay_hint, ObTxLogBlockHeader &block_header);
  int update_next_log_pos_(); // skip log body if  cur_log_type_ is UNKNOWN (depend on
                              // DESERIALIZE_HEADER in ob_unify_serialize.h)
  DISALLOW_COPY_AND_ASSIGN(ObTxLogBlock);

  ObTxAdaptiveLogBuf fill_buf_;
  const char *replay_buf_;
  int64_t len_;
  int64_t pos_;
  ObTxLogType cur_log_type_;
  ObTxCbArgArray cb_arg_array_;

  ObTxBigSegmentBuf *big_segment_buf_;
};

template <typename T>
int ObTxLogBlock::deserialize_log_body(T &tx_log_body)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  if (OB_ISNULL(replay_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (T::LOG_TYPE != cur_log_type_) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid log_body type", K(T::LOG_TYPE), K(cur_log_type_));
  } else {
    if (OB_NOT_NULL(big_segment_buf_)) {
      if (OB_FAIL(big_segment_buf_->deserialize_object(tx_log_body))) {
        TRANS_LOG(WARN, "deserialize log body from big segment buf failed", K(ret), K(tx_log_body),
                  KPC(this));
      } else {
        big_segment_buf_->reset();
        big_segment_buf_ = nullptr;
      }
    } else if (OB_FAIL(tx_log_body.deserialize(replay_buf_, len_, tmp_pos))) {
      TRANS_LOG(WARN, "desrialize tx_log_body error", K(ret), K(*this));
    }
    if (OB_SUCC(ret)) {
      pos_ = tmp_pos;
      cur_log_type_ = ObTxLogType::UNKNOWN;
    }
  }
  return ret;
}

template <typename T>
int ObTxLogBlock::add_new_log(T &tx_log_body, ObTxBigSegmentBuf *big_segment_buf)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos_;
  ObTxLogHeader header(T::LOG_TYPE);
  if ((OB_ISNULL(fill_buf_.get_buf()))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(ERROR, "invalid argument", K(*this));
  } else if (ObTxLogType::TX_REDO_LOG == cur_log_type_) {
    ret = OB_EAGAIN;
    TRANS_LOG(WARN, "MutatorBuf is using");
  } else if (OB_NOT_NULL(big_segment_buf_) && big_segment_buf_->is_active()) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "big segment is not empty, can not add new log", K(ret), KPC(this));
  } else if (T::LOG_TYPE == ObTxLogType::TX_REDO_LOG) {
    // ret = OB_EAGAIN;
    TRANS_LOG(DEBUG, "insert redo_log type into cb_arg_array_", K(tx_log_body), KPC(this));
  } else if (OB_FAIL(tx_log_body.before_serialize())) {
    TRANS_LOG(WARN, "before serialize failed", K(ret), K(tx_log_body), K(*this));
  } else if (len_ < header.get_serialize_size() + tx_log_body.get_serialize_size() + tmp_pos) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(header.serialize(fill_buf_.get_buf(), len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize log header error", K(ret), K(header), K(*this));
  } else if (OB_FAIL(tx_log_body.serialize(fill_buf_.get_buf(), len_, tmp_pos))) {
    TRANS_LOG(WARN, "serialize tx_log_body error", K(ret), K(tx_log_body), K(*this));
  } else {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    // it should be not failed
    if (OB_FAIL(cb_arg_array_.push_back(ObTxCbArg(T::LOG_TYPE, NULL)))) {
      TRANS_LOG(ERROR, "push log cb arg failed", K(ret), K(*this));
    } else {
      pos_ = tmp_pos;
    }
  } else if (OB_BUF_NOT_ENOUGH == ret && !is_use_local_buf() && cb_arg_array_.empty()
             && ObTxLogTypeChecker::can_be_spilt(T::LOG_TYPE)) {
    // use big segment
    if (OB_ISNULL(big_segment_buf)) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), KPC(big_segment_buf));
    } else if (OB_FALSE_IT(big_segment_buf_ = big_segment_buf)) {
    } else if (OB_FAIL(big_segment_buf_->init_for_serialize(header.get_serialize_size()
                                                            + tx_log_body.get_serialize_size()))) {
      TRANS_LOG(WARN, "init big segment buf failed", K(ret), KPC(big_segment_buf_));
    } else if (OB_FAIL(big_segment_buf_->serialize_object(header))) {
      TRANS_LOG(WARN, "serialize log header error", K(ret), K(header), K(*this));
    } else if (OB_FAIL(big_segment_buf_->serialize_object(tx_log_body))) {
      TRANS_LOG(WARN, "serialize tx_log_body error", K(ret), K(tx_log_body), K(*this));
    } else {
      ret = OB_LOG_TOO_LARGE;
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "[TxLogBlock] add new log", K(ret), K(tx_log_body), K(*this), K(tmp_pos));
  }

  return ret;
}

} // namespace transaction
} // namespace oceanbase
#endif


